"""
Redis Streams Event Processing Integration

Implements Redis Streams for real-time event processing, cache invalidation,
and distributed system coordination.
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Set, Tuple, Union

import redis.asyncio as aioredis

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager

logger = get_logger(__name__)


class EventType(Enum):
    """Event types for stream processing."""
    CACHE_INVALIDATION = "cache.invalidation"
    CACHE_WARMING = "cache.warming"
    MODEL_UPDATE = "model.update"
    FEATURE_UPDATE = "feature.update"
    SCHEMA_CHANGE = "schema.change"
    USER_ACTION = "user.action"
    SYSTEM_ALERT = "system.alert"
    DEPLOYMENT = "deployment"
    METRICS = "metrics"
    CUSTOM = "custom"


@dataclass
class StreamEvent:
    """Event structure for Redis Streams."""
    event_id: str
    event_type: EventType
    source: str
    timestamp: datetime
    data: Dict[str, Any]
    correlation_id: Optional[str] = None
    priority: int = 0  # 0=low, 1=normal, 2=high, 3=critical
    tags: Optional[List[str]] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Redis storage."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "data": json.dumps(self.data, default=str),
            "correlation_id": self.correlation_id or "",
            "priority": str(self.priority),
            "tags": json.dumps(self.tags or []),
            "retry_count": str(self.retry_count)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StreamEvent':
        """Create from dictionary."""
        return cls(
            event_id=data["event_id"],
            event_type=EventType(data["event_type"]),
            source=data["source"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data=json.loads(data["data"]) if data["data"] else {},
            correlation_id=data.get("correlation_id") or None,
            priority=int(data.get("priority", 0)),
            tags=json.loads(data.get("tags", "[]")),
            retry_count=int(data.get("retry_count", 0))
        )


class StreamProcessor:
    """Base class for stream event processors."""
    
    def __init__(self, processor_name: str, 
                 supported_events: Optional[List[EventType]] = None):
        self.processor_name = processor_name
        self.supported_events = supported_events or []
        self.processed_count = 0
        self.error_count = 0
        
    async def can_process(self, event: StreamEvent) -> bool:
        """Check if processor can handle this event."""
        if not self.supported_events:
            return True
        return event.event_type in self.supported_events
    
    async def process(self, event: StreamEvent) -> bool:
        """Process the event. Return True if successful."""
        try:
            success = await self._process_event(event)
            if success:
                self.processed_count += 1
            else:
                self.error_count += 1
            return success
        except Exception as e:
            logger.error(f"Error processing event {event.event_id}: {e}")
            self.error_count += 1
            return False
    
    async def _process_event(self, event: StreamEvent) -> bool:
        """Override this method in subclasses."""
        raise NotImplementedError


class CacheInvalidationProcessor(StreamProcessor):
    """Processor for cache invalidation events."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        super().__init__(
            "cache_invalidation",
            [EventType.CACHE_INVALIDATION]
        )
        self.cache_manager = cache_manager
        
    async def _process_event(self, event: StreamEvent) -> bool:
        """Process cache invalidation event."""
        cache_manager = self.cache_manager or await get_cache_manager()
        
        data = event.data
        invalidation_type = data.get("type", "key")
        
        if invalidation_type == "key":
            key = data.get("key")
            namespace = data.get("namespace", "")
            if key:
                success = await cache_manager.delete(key, namespace)
                logger.info(f"Invalidated cache key: {key} (success: {success})")
                return success
                
        elif invalidation_type == "pattern":
            pattern = data.get("pattern")
            namespace = data.get("namespace", "")
            if pattern:
                count = await cache_manager.invalidate_pattern(pattern, namespace)
                logger.info(f"Invalidated {count} keys matching pattern: {pattern}")
                return count > 0
                
        elif invalidation_type == "namespace":
            namespace = data.get("namespace")
            if namespace:
                count = await cache_manager.invalidate_pattern("*", namespace)
                logger.info(f"Invalidated {count} keys in namespace: {namespace}")
                return count > 0
        
        return False


class CacheWarmingProcessor(StreamProcessor):
    """Processor for cache warming events."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        super().__init__(
            "cache_warming",
            [EventType.CACHE_WARMING]
        )
        self.cache_manager = cache_manager
        
    async def _process_event(self, event: StreamEvent) -> bool:
        """Process cache warming event."""
        cache_manager = self.cache_manager or await get_cache_manager()
        
        data = event.data
        key = data.get("key")
        value = data.get("value")
        ttl = data.get("ttl", 3600)
        namespace = data.get("namespace", "")
        
        if key and value is not None:
            success = await cache_manager.set(key, value, ttl, namespace)
            logger.info(f"Warmed cache key: {key} (success: {success})")
            return success
        
        return False


class RedisStreamsManager:
    """Manager for Redis Streams operations."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 stream_prefix: str = "events"):
        self.cache_manager = cache_manager
        self.stream_prefix = stream_prefix
        self.processors: Dict[str, StreamProcessor] = {}
        self.consumer_groups: Dict[str, str] = {}  # stream -> consumer_group
        self.running = False
        self.consumer_tasks: List[asyncio.Task] = []
        
        # Default processors
        self._register_default_processors()
    
    def _register_default_processors(self):
        """Register default event processors."""
        self.register_processor(CacheInvalidationProcessor(self.cache_manager))
        self.register_processor(CacheWarmingProcessor(self.cache_manager))
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def register_processor(self, processor: StreamProcessor):
        """Register an event processor."""
        self.processors[processor.processor_name] = processor
        logger.info(f"Registered stream processor: {processor.processor_name}")
    
    def unregister_processor(self, processor_name: str):
        """Unregister an event processor."""
        if processor_name in self.processors:
            del self.processors[processor_name]
            logger.info(f"Unregistered stream processor: {processor_name}")
    
    def get_stream_name(self, event_type: EventType) -> str:
        """Get stream name for event type."""
        return f"{self.stream_prefix}:{event_type.value}"
    
    async def publish_event(self, event: StreamEvent, stream_name: Optional[str] = None) -> str:
        """
        Publish event to Redis stream.
        
        Returns:
            Stream entry ID
        """
        cache_manager = await self._get_cache_manager()
        
        if stream_name is None:
            stream_name = self.get_stream_name(event.event_type)
        
        try:
            # Add event to stream
            entry_id = await cache_manager.async_redis_client.xadd(
                stream_name,
                event.to_dict(),
                maxlen=10000,  # Keep only latest 10K events
                approximate=True
            )
            
            logger.debug(f"Published event {event.event_id} to stream {stream_name}")
            return entry_id
            
        except Exception as e:
            logger.error(f"Failed to publish event to stream {stream_name}: {e}")
            raise
    
    async def create_consumer_group(self, stream_name: str, group_name: str, 
                                  start_id: str = "0") -> bool:
        """Create consumer group for stream."""
        cache_manager = await self._get_cache_manager()
        
        try:
            await cache_manager.async_redis_client.xgroup_create(
                stream_name, group_name, start_id, mkstream=True
            )
            self.consumer_groups[stream_name] = group_name
            logger.info(f"Created consumer group {group_name} for stream {stream_name}")
            return True
            
        except aioredis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                # Group already exists
                self.consumer_groups[stream_name] = group_name
                logger.debug(f"Consumer group {group_name} already exists for stream {stream_name}")
                return True
            else:
                logger.error(f"Failed to create consumer group: {e}")
                return False
        except Exception as e:
            logger.error(f"Failed to create consumer group: {e}")
            return False
    
    async def start_consumers(self, consumer_name: str = "cache-consumer", 
                            batch_size: int = 10):
        """Start consumer processes for all registered event types."""
        if self.running:
            logger.warning("Consumers already running")
            return
        
        self.running = True
        cache_manager = await self._get_cache_manager()
        
        # Create consumer groups for all event types
        for event_type in EventType:
            stream_name = self.get_stream_name(event_type)
            group_name = f"{consumer_name}-group"
            await self.create_consumer_group(stream_name, group_name)
        
        # Start consumer tasks
        for event_type in EventType:
            stream_name = self.get_stream_name(event_type)
            group_name = self.consumer_groups.get(stream_name)
            
            if group_name:
                task = asyncio.create_task(
                    self._consume_stream(
                        stream_name, group_name, consumer_name, batch_size
                    )
                )
                self.consumer_tasks.append(task)
        
        logger.info(f"Started {len(self.consumer_tasks)} stream consumers")
    
    async def stop_consumers(self):
        """Stop all consumer processes."""
        self.running = False
        
        # Cancel all consumer tasks
        for task in self.consumer_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self.consumer_tasks:
            await asyncio.gather(*self.consumer_tasks, return_exceptions=True)
        
        self.consumer_tasks.clear()
        logger.info("Stopped all stream consumers")
    
    async def _consume_stream(self, stream_name: str, group_name: str, 
                            consumer_name: str, batch_size: int):
        """Consume events from a specific stream."""
        cache_manager = await self._get_cache_manager()
        
        logger.info(f"Starting consumer {consumer_name} for stream {stream_name}")
        
        while self.running:
            try:
                # Read new messages
                messages = await cache_manager.async_redis_client.xreadgroup(
                    group_name,
                    consumer_name,
                    {stream_name: ">"},
                    count=batch_size,
                    block=1000  # 1 second timeout
                )
                
                if not messages:
                    continue
                
                # Process messages
                for stream, stream_messages in messages:
                    for message_id, fields in stream_messages:
                        try:
                            # Parse event
                            event = StreamEvent.from_dict(fields)
                            
                            # Find suitable processors
                            processed = False
                            for processor in self.processors.values():
                                if await processor.can_process(event):
                                    success = await processor.process(event)
                                    if success:
                                        processed = True
                                        # Acknowledge message
                                        await cache_manager.async_redis_client.xack(
                                            stream_name, group_name, message_id
                                        )
                                        break
                            
                            if not processed:
                                logger.warning(f"No processor found for event {event.event_id}")
                                # Still acknowledge to avoid reprocessing
                                await cache_manager.async_redis_client.xack(
                                    stream_name, group_name, message_id
                                )
                        
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {e}")
                            # Don't acknowledge on error - message will be retried
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consumer error for stream {stream_name}: {e}")
                await asyncio.sleep(1)  # Brief pause before retry
        
        logger.info(f"Consumer {consumer_name} for stream {stream_name} stopped")
    
    async def get_stream_info(self, stream_name: str) -> Dict[str, Any]:
        """Get information about a stream."""
        cache_manager = await self._get_cache_manager()
        
        try:
            info = await cache_manager.async_redis_client.xinfo_stream(stream_name)
            return {
                "length": info.get("length", 0),
                "radix_tree_keys": info.get("radix-tree-keys", 0),
                "radix_tree_nodes": info.get("radix-tree-nodes", 0),
                "groups": info.get("groups", 0),
                "last_generated_id": info.get("last-generated-id", ""),
                "first_entry": info.get("first-entry"),
                "last_entry": info.get("last-entry")
            }
        except Exception as e:
            logger.error(f"Failed to get stream info for {stream_name}: {e}")
            return {}
    
    async def get_consumer_group_info(self, stream_name: str) -> List[Dict[str, Any]]:
        """Get consumer group information for a stream."""
        cache_manager = await self._get_cache_manager()
        
        try:
            groups = await cache_manager.async_redis_client.xinfo_groups(stream_name)
            return [
                {
                    "name": group.get("name", ""),
                    "consumers": group.get("consumers", 0),
                    "pending": group.get("pending", 0),
                    "last_delivered_id": group.get("last-delivered-id", "")
                }
                for group in groups
            ]
        except Exception as e:
            logger.error(f"Failed to get consumer group info for {stream_name}: {e}")
            return []
    
    async def get_pending_messages(self, stream_name: str, 
                                 group_name: str) -> List[Dict[str, Any]]:
        """Get pending messages for a consumer group."""
        cache_manager = await self._get_cache_manager()
        
        try:
            pending = await cache_manager.async_redis_client.xpending(
                stream_name, group_name
            )
            
            if not pending or pending[0] == 0:
                return []
            
            # Get detailed pending info
            detailed = await cache_manager.async_redis_client.xpending_range(
                stream_name, group_name, "-", "+", count=100
            )
            
            return [
                {
                    "message_id": msg[0],
                    "consumer": msg[1],
                    "idle_time": msg[2],
                    "delivery_count": msg[3]
                }
                for msg in detailed
            ]
            
        except Exception as e:
            logger.error(f"Failed to get pending messages for {stream_name}: {e}")
            return []
    
    async def trim_stream(self, stream_name: str, max_length: int = 10000) -> int:
        """Trim stream to maximum length."""
        cache_manager = await self._get_cache_manager()
        
        try:
            result = await cache_manager.async_redis_client.xtrim(
                stream_name, maxlen=max_length, approximate=True
            )
            logger.info(f"Trimmed stream {stream_name} to {max_length} entries")
            return result
        except Exception as e:
            logger.error(f"Failed to trim stream {stream_name}: {e}")
            return 0
    
    async def delete_stream(self, stream_name: str) -> bool:
        """Delete a stream."""
        cache_manager = await self._get_cache_manager()
        
        try:
            result = await cache_manager.async_redis_client.delete(stream_name)
            logger.info(f"Deleted stream {stream_name}")
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to delete stream {stream_name}: {e}")
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get streams and processor statistics."""
        stats = {
            "processors": {},
            "streams": {},
            "consumer_groups": len(self.consumer_groups),
            "running": self.running,
            "active_consumers": len(self.consumer_tasks)
        }
        
        # Processor stats
        for name, processor in self.processors.items():
            stats["processors"][name] = {
                "processed_count": processor.processed_count,
                "error_count": processor.error_count,
                "supported_events": [e.value for e in processor.supported_events]
            }
        
        # Stream stats
        for event_type in EventType:
            stream_name = self.get_stream_name(event_type)
            stream_info = await self.get_stream_info(stream_name)
            if stream_info:
                stats["streams"][event_type.value] = stream_info
        
        return stats


# Convenience functions for common event publishing

async def publish_cache_invalidation(key: str, namespace: str = "", 
                                   pattern: bool = False,
                                   source: str = "system") -> str:
    """Publish cache invalidation event."""
    streams_manager = RedisStreamsManager()
    
    event = StreamEvent(
        event_id=str(uuid.uuid4()),
        event_type=EventType.CACHE_INVALIDATION,
        source=source,
        timestamp=datetime.utcnow(),
        data={
            "type": "pattern" if pattern else "key",
            "key" if not pattern else "pattern": key,
            "namespace": namespace
        },
        priority=2  # High priority
    )
    
    return await streams_manager.publish_event(event)


async def publish_cache_warming(key: str, value: Any, ttl: int = 3600,
                              namespace: str = "", source: str = "system") -> str:
    """Publish cache warming event."""
    streams_manager = RedisStreamsManager()
    
    event = StreamEvent(
        event_id=str(uuid.uuid4()),
        event_type=EventType.CACHE_WARMING,
        source=source,
        timestamp=datetime.utcnow(),
        data={
            "key": key,
            "value": value,
            "ttl": ttl,
            "namespace": namespace
        },
        priority=1  # Normal priority
    )
    
    return await streams_manager.publish_event(event)


async def publish_model_update(model_id: str, version: str, 
                             metadata: Dict[str, Any],
                             source: str = "ml_system") -> str:
    """Publish model update event."""
    streams_manager = RedisStreamsManager()
    
    event = StreamEvent(
        event_id=str(uuid.uuid4()),
        event_type=EventType.MODEL_UPDATE,
        source=source,
        timestamp=datetime.utcnow(),
        data={
            "model_id": model_id,
            "version": version,
            "metadata": metadata
        },
        priority=2  # High priority
    )
    
    return await streams_manager.publish_event(event)


# Global streams manager instance
_streams_manager: Optional[RedisStreamsManager] = None


async def get_streams_manager() -> RedisStreamsManager:
    """Get or create global streams manager instance."""
    global _streams_manager
    if _streams_manager is None:
        _streams_manager = RedisStreamsManager()
    return _streams_manager