"""
Event Sourcing with Redis Caching Integration
Implements cache-aside pattern with event replay capabilities
"""
from __future__ import annotations

import json
import uuid
import asyncio
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union, Tuple
import hashlib
import gzip
import pickle

import redis
import redis.sentinel
from confluent_kafka import Producer, Consumer

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import EnhancedStreamingMessage
from src.streaming.enhanced_kafka_streams import EventSourcingState


class CacheStrategy(Enum):
    """Cache strategies for event sourcing"""
    CACHE_ASIDE = "cache_aside"
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    REFRESH_AHEAD = "refresh_ahead"


class ConsistencyLevel(Enum):
    """Consistency levels for cache operations"""
    EVENTUAL = "eventual"
    STRONG = "strong"
    WEAK = "weak"


@dataclass 
class CacheConfig:
    """Configuration for Redis caching"""
    redis_url: str = "redis://localhost:6379"
    sentinel_hosts: Optional[List[Tuple[str, int]]] = None
    sentinel_service_name: Optional[str] = None
    max_connections: int = 100
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    retry_on_timeout: bool = True
    health_check_interval: int = 30
    
    # Cache behavior
    default_ttl: int = 3600  # 1 hour
    max_ttl: int = 86400  # 24 hours
    cache_strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE
    consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    
    # Serialization
    compression_enabled: bool = True
    compression_threshold: int = 1024  # bytes
    
    # Cache warming
    enable_cache_warming: bool = True
    warming_batch_size: int = 100


@dataclass
class EventReplayConfig:
    """Configuration for event replay functionality"""
    enable_replay: bool = True
    replay_topic: str = "event-replay"
    max_replay_events: int = 10000
    replay_batch_size: int = 100
    replay_timeout_ms: int = 30000
    
    # Checkpoint management
    checkpoint_frequency: int = 100
    checkpoint_topic: str = "event-replay-checkpoints"


class CacheMetrics:
    """Metrics tracking for cache operations"""
    
    def __init__(self, metrics_collector):
        self.metrics_collector = metrics_collector
        
        # Cache statistics
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_writes = 0
        self.cache_deletes = 0
        self.cache_errors = 0
        
        # Performance metrics
        self.avg_cache_response_time = 0.0
        self.max_cache_response_time = 0.0
        self.cache_throughput = 0.0
        
    def record_cache_hit(self, response_time: float):
        """Record cache hit"""
        self.cache_hits += 1
        self._update_response_time(response_time)
        if self.metrics_collector:
            self.metrics_collector.increment_counter("cache_hits")
            self.metrics_collector.histogram("cache_response_time", response_time)
    
    def record_cache_miss(self, response_time: float):
        """Record cache miss"""
        self.cache_misses += 1
        self._update_response_time(response_time)
        if self.metrics_collector:
            self.metrics_collector.increment_counter("cache_misses")
            self.metrics_collector.histogram("cache_response_time", response_time)
    
    def record_cache_write(self, response_time: float):
        """Record cache write"""
        self.cache_writes += 1
        self._update_response_time(response_time)
        if self.metrics_collector:
            self.metrics_collector.increment_counter("cache_writes")
    
    def record_cache_error(self, error_type: str):
        """Record cache error"""
        self.cache_errors += 1
        if self.metrics_collector:
            self.metrics_collector.increment_counter("cache_errors", {"error_type": error_type})
    
    def _update_response_time(self, response_time: float):
        """Update response time metrics"""
        if response_time > self.max_cache_response_time:
            self.max_cache_response_time = response_time
        
        # Simple moving average for demo
        self.avg_cache_response_time = (self.avg_cache_response_time + response_time) / 2
    
    def get_cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio"""
        total_requests = self.cache_hits + self.cache_misses
        return self.cache_hits / total_requests if total_requests > 0 else 0.0
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_writes": self.cache_writes,
            "cache_deletes": self.cache_deletes,
            "cache_errors": self.cache_errors,
            "hit_ratio": self.get_cache_hit_ratio(),
            "avg_response_time": self.avg_cache_response_time,
            "max_response_time": self.max_cache_response_time
        }


class EventCache:
    """Advanced Redis cache for event sourcing with high availability"""
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self.logger = get_logger(__name__)
        self.metrics = CacheMetrics(get_metrics_collector())
        
        # Redis clients
        self.redis_client: Optional[redis.Redis] = None
        self.sentinel: Optional[redis.sentinel.Sentinel] = None
        
        # Connection management
        self._connection_pool = None
        self._health_check_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Initialize connection
        self._initialize_redis_connection()
        self._start_health_monitoring()
        
    def _initialize_redis_connection(self):
        """Initialize Redis connection with sentinel support"""
        try:
            if self.config.sentinel_hosts:
                # Use Redis Sentinel for high availability
                self.sentinel = redis.sentinel.Sentinel(
                    self.config.sentinel_hosts,
                    socket_timeout=self.config.socket_timeout,
                    socket_connect_timeout=self.config.socket_connect_timeout
                )
                self.redis_client = self.sentinel.master_for(
                    self.config.sentinel_service_name,
                    socket_timeout=self.config.socket_timeout,
                    socket_connect_timeout=self.config.socket_connect_timeout,
                    retry_on_timeout=self.config.retry_on_timeout
                )
                self.logger.info("Connected to Redis via Sentinel")
            else:
                # Direct Redis connection
                self.redis_client = redis.from_url(
                    self.config.redis_url,
                    max_connections=self.config.max_connections,
                    socket_timeout=self.config.socket_timeout,
                    socket_connect_timeout=self.config.socket_connect_timeout,
                    retry_on_timeout=self.config.retry_on_timeout
                )
                self.logger.info("Connected to Redis directly")
                
            # Test connection
            self.redis_client.ping()
            self.logger.info("Redis connection established successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis connection: {e}")
            raise
    
    def _start_health_monitoring(self):
        """Start background health monitoring"""
        self._running = True
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            name="redis-health-monitor",
            daemon=True
        )
        self._health_check_thread.start()
    
    def _health_check_loop(self):
        """Background health check loop"""
        while self._running:
            try:
                start_time = time.time()
                self.redis_client.ping()
                response_time = time.time() - start_time
                
                # Record health check metrics
                if get_metrics_collector():
                    get_metrics_collector().gauge("redis_health_check_time", response_time)
                    
            except Exception as e:
                self.logger.error(f"Redis health check failed: {e}")
                self.metrics.record_cache_error("health_check_failed")
                
                # Attempt reconnection
                try:
                    self._initialize_redis_connection()
                except Exception as reconnect_error:
                    self.logger.error(f"Redis reconnection failed: {reconnect_error}")
            
            time.sleep(self.config.health_check_interval)
    
    def _serialize_data(self, data: Any) -> bytes:
        """Serialize data for caching"""
        try:
            # Convert to JSON first
            json_data = json.dumps(data, default=str).encode('utf-8')
            
            # Apply compression if enabled and data size exceeds threshold
            if (self.config.compression_enabled and 
                len(json_data) > self.config.compression_threshold):
                return gzip.compress(json_data)
            
            return json_data
            
        except Exception as e:
            self.logger.error(f"Data serialization failed: {e}")
            raise
    
    def _deserialize_data(self, data: bytes) -> Any:
        """Deserialize cached data"""
        try:
            # Try to decompress first
            try:
                decompressed = gzip.decompress(data)
                return json.loads(decompressed.decode('utf-8'))
            except gzip.BadGzipFile:
                # Not compressed, try direct JSON decode
                return json.loads(data.decode('utf-8'))
                
        except Exception as e:
            self.logger.error(f"Data deserialization failed: {e}")
            raise
    
    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        """Get value from cache"""
        start_time = time.time()
        
        try:
            cached_data = self.redis_client.get(key)
            response_time = time.time() - start_time
            
            if cached_data:
                value = self._deserialize_data(cached_data)
                self.metrics.record_cache_hit(response_time)
                return value, True
            else:
                self.metrics.record_cache_miss(response_time)
                return None, False
                
        except Exception as e:
            response_time = time.time() - start_time
            self.logger.error(f"Cache get failed for key {key}: {e}")
            self.metrics.record_cache_error("get_failed")
            self.metrics.record_cache_miss(response_time)
            return None, False
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache"""
        start_time = time.time()
        
        try:
            serialized_data = self._serialize_data(value)
            actual_ttl = ttl or self.config.default_ttl
            
            # Ensure TTL doesn't exceed maximum
            if actual_ttl > self.config.max_ttl:
                actual_ttl = self.config.max_ttl
            
            self.redis_client.setex(key, actual_ttl, serialized_data)
            response_time = time.time() - start_time
            
            self.metrics.record_cache_write(response_time)
            return True
            
        except Exception as e:
            response_time = time.time() - start_time
            self.logger.error(f"Cache set failed for key {key}: {e}")
            self.metrics.record_cache_error("set_failed")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        try:
            result = self.redis_client.delete(key)
            self.metrics.cache_deletes += 1
            return result > 0
            
        except Exception as e:
            self.logger.error(f"Cache delete failed for key {key}: {e}")
            self.metrics.record_cache_error("delete_failed")
            return False
    
    def mget(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache"""
        start_time = time.time()
        
        try:
            cached_values = self.redis_client.mget(keys)
            response_time = time.time() - start_time
            
            results = {}
            for key, cached_data in zip(keys, cached_values):
                if cached_data:
                    try:
                        results[key] = self._deserialize_data(cached_data)
                        self.metrics.record_cache_hit(response_time / len(keys))
                    except Exception as e:
                        self.logger.error(f"Failed to deserialize cached data for key {key}: {e}")
                        self.metrics.record_cache_miss(response_time / len(keys))
                else:
                    self.metrics.record_cache_miss(response_time / len(keys))
            
            return results
            
        except Exception as e:
            response_time = time.time() - start_time
            self.logger.error(f"Cache mget failed: {e}")
            self.metrics.record_cache_error("mget_failed")
            # Record misses for all keys
            for _ in keys:
                self.metrics.record_cache_miss(response_time / len(keys))
            return {}
    
    def mset(self, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Set multiple values in cache"""
        start_time = time.time()
        
        try:
            serialized_data = {}
            for key, value in data.items():
                serialized_data[key] = self._serialize_data(value)
            
            # Use pipeline for atomic operation
            pipe = self.redis_client.pipeline()
            pipe.mset(serialized_data)
            
            # Set TTL for all keys if specified
            actual_ttl = ttl or self.config.default_ttl
            if actual_ttl > self.config.max_ttl:
                actual_ttl = self.config.max_ttl
                
            for key in data.keys():
                pipe.expire(key, actual_ttl)
            
            pipe.execute()
            
            response_time = time.time() - start_time
            for _ in data:
                self.metrics.record_cache_write(response_time / len(data))
            
            return True
            
        except Exception as e:
            response_time = time.time() - start_time
            self.logger.error(f"Cache mset failed: {e}")
            self.metrics.record_cache_error("mset_failed")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        try:
            return self.redis_client.exists(key) > 0
        except Exception as e:
            self.logger.error(f"Cache exists check failed for key {key}: {e}")
            return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate keys matching pattern"""
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                count = self.redis_client.delete(*keys)
                self.metrics.cache_deletes += count
                return count
            return 0
            
        except Exception as e:
            self.logger.error(f"Cache pattern invalidation failed for pattern {pattern}: {e}")
            self.metrics.record_cache_error("invalidate_pattern_failed")
            return 0
    
    def flush_all(self) -> bool:
        """Flush all cached data"""
        try:
            self.redis_client.flushdb()
            self.logger.warning("Cache flushed - all data cleared")
            return True
            
        except Exception as e:
            self.logger.error(f"Cache flush failed: {e}")
            self.metrics.record_cache_error("flush_failed")
            return False
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get cache information and statistics"""
        try:
            info = self.redis_client.info()
            return {
                "redis_version": info.get("redis_version"),
                "used_memory": info.get("used_memory"),
                "used_memory_human": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "total_commands_processed": info.get("total_commands_processed"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "cache_metrics": self.metrics.get_metrics_summary()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get cache info: {e}")
            return {"error": str(e)}
    
    def close(self):
        """Close cache connections"""
        self._running = False
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5)
        
        if self.redis_client:
            self.redis_client.connection_pool.disconnect()
        
        self.logger.info("Cache connections closed")


class EventSourcingCacheManager:
    """Manager for event sourcing with cache-aside pattern"""
    
    def __init__(self, 
                 cache_config: CacheConfig,
                 replay_config: EventReplayConfig,
                 kafka_bootstrap_servers: List[str]):
        self.cache_config = cache_config
        self.replay_config = replay_config
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.logger = get_logger(__name__)
        
        # Components
        self.cache = EventCache(cache_config)
        self.kafka_producer: Optional[Producer] = None
        self.kafka_consumer: Optional[Consumer] = None
        
        # Event store (in-memory for demo - use persistent storage in production)
        self.event_store: Dict[str, List[EnhancedStreamingMessage]] = {}
        self.aggregate_cache: Dict[str, EventSourcingState] = {}
        
        # Initialize Kafka clients
        self._initialize_kafka_clients()
        
    def _initialize_kafka_clients(self):
        """Initialize Kafka producer and consumer for event replay"""
        try:
            # Producer for event replay
            producer_config = {
                'bootstrap.servers': ','.join(self.kafka_bootstrap_servers),
                'enable.idempotence': True,
                'acks': 'all',
                'compression.type': 'zstd'
            }
            self.kafka_producer = Producer(producer_config)
            
            # Consumer for event replay checkpoints
            consumer_config = {
                'bootstrap.servers': ','.join(self.kafka_bootstrap_servers),
                'group.id': 'event-replay-checkpoints',
                'auto.offset.reset': 'earliest'
            }
            self.kafka_consumer = Consumer(consumer_config)
            
            self.logger.info("Kafka clients for event replay initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka clients: {e}")
            raise
    
    def get_aggregate(self, aggregate_id: str, use_cache: bool = True) -> Optional[EventSourcingState]:
        """Get aggregate state using cache-aside pattern"""
        # Try cache first if enabled
        if use_cache:
            cache_key = f"aggregate:{aggregate_id}"
            cached_state, hit = self.cache.get(cache_key)
            
            if hit and cached_state:
                # Convert cached data back to EventSourcingState
                state = EventSourcingState(
                    aggregate_id=cached_state["aggregate_id"],
                    aggregate_type=cached_state["aggregate_type"],
                    version=cached_state["version"],
                    snapshot_data=cached_state["snapshot_data"],
                    last_snapshot_version=cached_state.get("last_snapshot_version", 0)
                )
                
                self.logger.debug(f"Cache hit for aggregate {aggregate_id}")
                return state
        
        # Cache miss - rebuild from event store
        self.logger.debug(f"Cache miss for aggregate {aggregate_id}, rebuilding from events")
        return self._rebuild_aggregate_from_events(aggregate_id, use_cache)
    
    def _rebuild_aggregate_from_events(self, aggregate_id: str, cache_result: bool = True) -> Optional[EventSourcingState]:
        """Rebuild aggregate state from event store"""
        if aggregate_id not in self.event_store:
            return None
        
        events = self.event_store[aggregate_id]
        if not events:
            return None
        
        # Create initial state
        state = EventSourcingState(
            aggregate_id=aggregate_id,
            aggregate_type="default",
            version=0
        )
        
        # Apply events to rebuild state
        for event in events:
            self._apply_event_to_state(state, event)
        
        # Cache the rebuilt state
        if cache_result:
            self._cache_aggregate_state(state)
        
        return state
    
    def _apply_event_to_state(self, state: EventSourcingState, event: EnhancedStreamingMessage):
        """Apply event to aggregate state"""
        state.version += 1
        state.events.append(event)
        
        # Apply business logic based on event type
        event_type = event.event_type
        payload = event.payload
        
        if event_type == "customer_created":
            state.snapshot_data = {
                "customer_id": payload.get("customer_id"),
                "name": payload.get("name"),
                "email": payload.get("email"),
                "created_at": event.timestamp.isoformat(),
                "status": "active",
                "version": state.version
            }
        elif event_type == "customer_updated":
            if state.snapshot_data:
                state.snapshot_data.update(payload)
                state.snapshot_data["version"] = state.version
                state.snapshot_data["updated_at"] = event.timestamp.isoformat()
        elif event_type == "order_placed":
            if not state.snapshot_data:
                state.snapshot_data = {"orders": []}
            
            if "orders" not in state.snapshot_data:
                state.snapshot_data["orders"] = []
                
            state.snapshot_data["orders"].append({
                "order_id": payload.get("order_id"),
                "amount": payload.get("amount"),
                "placed_at": event.timestamp.isoformat()
            })
            state.snapshot_data["version"] = state.version
    
    def _cache_aggregate_state(self, state: EventSourcingState):
        """Cache aggregate state"""
        cache_key = f"aggregate:{state.aggregate_id}"
        cache_data = {
            "aggregate_id": state.aggregate_id,
            "aggregate_type": state.aggregate_type,
            "version": state.version,
            "snapshot_data": state.snapshot_data,
            "last_snapshot_version": state.last_snapshot_version,
            "cached_at": datetime.now().isoformat()
        }
        
        ttl = self.cache_config.default_ttl
        success = self.cache.set(cache_key, cache_data, ttl)
        
        if success:
            self.logger.debug(f"Cached aggregate {state.aggregate_id} version {state.version}")
        else:
            self.logger.warning(f"Failed to cache aggregate {state.aggregate_id}")
    
    def store_event(self, event: EnhancedStreamingMessage, update_cache: bool = True) -> bool:
        """Store event and optionally update cache"""
        try:
            aggregate_id = event.aggregate_id
            if not aggregate_id:
                self.logger.warning(f"Event {event.message_id} missing aggregate_id")
                return False
            
            # Add to event store
            if aggregate_id not in self.event_store:
                self.event_store[aggregate_id] = []
            
            self.event_store[aggregate_id].append(event)
            
            # Update cache if enabled
            if update_cache:
                # Get current state from cache or rebuild
                current_state = self.get_aggregate(aggregate_id, use_cache=True)
                
                if not current_state:
                    # Create new state
                    current_state = EventSourcingState(
                        aggregate_id=aggregate_id,
                        aggregate_type="default",
                        version=0
                    )
                
                # Apply new event
                self._apply_event_to_state(current_state, event)
                
                # Update cache
                self._cache_aggregate_state(current_state)
                
                # Invalidate related cache entries if needed
                self._invalidate_related_cache_entries(event)
            
            self.logger.debug(f"Stored event {event.message_id} for aggregate {aggregate_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store event: {e}")
            return False
    
    def _invalidate_related_cache_entries(self, event: EnhancedStreamingMessage):
        """Invalidate related cache entries based on event"""
        # Example: invalidate customer-related queries when customer events occur
        if event.event_type in ["customer_created", "customer_updated", "customer_deleted"]:
            customer_id = event.payload.get("customer_id")
            if customer_id:
                # Invalidate customer queries
                self.cache.invalidate_pattern(f"query:customer:{customer_id}:*")
                self.cache.invalidate_pattern(f"query:customer_orders:{customer_id}")
    
    def replay_events(self, aggregate_id: str, from_version: int = 0, to_version: Optional[int] = None) -> List[EnhancedStreamingMessage]:
        """Replay events for an aggregate"""
        if aggregate_id not in self.event_store:
            return []
        
        events = self.event_store[aggregate_id]
        
        # Filter events by version range
        filtered_events = []
        for event in events:
            event_version = event.aggregate_version
            if event_version >= from_version:
                if to_version is None or event_version <= to_version:
                    filtered_events.append(event)
        
        # Publish replay events to Kafka topic
        if self.replay_config.enable_replay and filtered_events:
            self._publish_replay_events(aggregate_id, filtered_events)
        
        return filtered_events
    
    def _publish_replay_events(self, aggregate_id: str, events: List[EnhancedStreamingMessage]):
        """Publish replay events to Kafka"""
        try:
            for event in events:
                replay_event = EnhancedStreamingMessage(
                    message_id=str(uuid.uuid4()),
                    topic=self.replay_config.replay_topic,
                    key=aggregate_id,
                    payload={
                        "replay_type": "event_replay",
                        "original_event": event.to_dict(),
                        "replay_timestamp": datetime.now().isoformat()
                    },
                    timestamp=datetime.now(),
                    headers={"replay": "true"},
                    event_type="event_replay",
                    aggregate_id=aggregate_id,
                    correlation_id=event.correlation_id
                )
                
                self.kafka_producer.produce(
                    topic=replay_event.topic,
                    key=replay_event.key,
                    value=json.dumps(replay_event.to_dict(), default=str)
                )
            
            self.kafka_producer.flush()
            self.logger.info(f"Published {len(events)} replay events for aggregate {aggregate_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to publish replay events: {e}")
    
    def create_checkpoint(self, aggregate_id: str, version: int):
        """Create checkpoint for event replay"""
        try:
            checkpoint = {
                "aggregate_id": aggregate_id,
                "version": version,
                "timestamp": datetime.now().isoformat(),
                "checkpoint_id": str(uuid.uuid4())
            }
            
            self.kafka_producer.produce(
                topic=self.replay_config.checkpoint_topic,
                key=aggregate_id,
                value=json.dumps(checkpoint)
            )
            self.kafka_producer.flush()
            
            # Cache checkpoint
            checkpoint_key = f"checkpoint:{aggregate_id}"
            self.cache.set(checkpoint_key, checkpoint, ttl=self.cache_config.max_ttl)
            
            self.logger.debug(f"Created checkpoint for aggregate {aggregate_id} at version {version}")
            
        except Exception as e:
            self.logger.error(f"Failed to create checkpoint: {e}")
    
    def get_latest_checkpoint(self, aggregate_id: str) -> Optional[Dict[str, Any]]:
        """Get latest checkpoint for aggregate"""
        checkpoint_key = f"checkpoint:{aggregate_id}"
        checkpoint, hit = self.cache.get(checkpoint_key)
        
        if hit:
            return checkpoint
        
        # If not in cache, this would typically query persistent checkpoint storage
        return None
    
    def warm_cache(self, aggregate_ids: List[str]):
        """Warm cache with frequently accessed aggregates"""
        if not self.cache_config.enable_cache_warming:
            return
        
        self.logger.info(f"Warming cache for {len(aggregate_ids)} aggregates")
        
        batch_size = self.cache_config.warming_batch_size
        for i in range(0, len(aggregate_ids), batch_size):
            batch = aggregate_ids[i:i + batch_size]
            
            # Load aggregates in parallel (simplified)
            for aggregate_id in batch:
                try:
                    # This will cache the aggregate if not already cached
                    self.get_aggregate(aggregate_id, use_cache=True)
                except Exception as e:
                    self.logger.error(f"Failed to warm cache for aggregate {aggregate_id}: {e}")
        
        self.logger.info("Cache warming completed")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get event sourcing and cache metrics"""
        cache_info = self.cache.get_cache_info()
        
        return {
            "event_store_stats": {
                "total_aggregates": len(self.event_store),
                "total_events": sum(len(events) for events in self.event_store.values())
            },
            "cache_stats": cache_info,
            "config": {
                "cache_strategy": self.cache_config.cache_strategy.value,
                "consistency_level": self.cache_config.consistency_level.value,
                "default_ttl": self.cache_config.default_ttl,
                "replay_enabled": self.replay_config.enable_replay
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def close(self):
        """Close connections and cleanup resources"""
        if self.kafka_producer:
            self.kafka_producer.flush()
        
        if self.kafka_consumer:
            self.kafka_consumer.close()
        
        self.cache.close()
        
        self.logger.info("Event sourcing cache manager closed")


# Factory functions
def create_cache_config(**kwargs) -> CacheConfig:
    """Create cache configuration"""
    return CacheConfig(**kwargs)


def create_replay_config(**kwargs) -> EventReplayConfig:
    """Create event replay configuration"""
    return EventReplayConfig(**kwargs)


def create_event_sourcing_cache_manager(
    cache_config: CacheConfig,
    replay_config: EventReplayConfig,
    kafka_bootstrap_servers: List[str]
) -> EventSourcingCacheManager:
    """Create event sourcing cache manager"""
    return EventSourcingCacheManager(
        cache_config=cache_config,
        replay_config=replay_config,
        kafka_bootstrap_servers=kafka_bootstrap_servers
    )


# Example usage
if __name__ == "__main__":
    import time
    
    try:
        print("Testing Event Sourcing Cache Integration...")
        
        # Create configurations
        cache_config = create_cache_config(
            redis_url="redis://localhost:6379",
            default_ttl=3600,
            cache_strategy=CacheStrategy.CACHE_ASIDE,
            enable_cache_warming=True
        )
        
        replay_config = create_replay_config(
            enable_replay=True,
            replay_topic="event-replay",
            max_replay_events=10000
        )
        
        # Create manager
        manager = create_event_sourcing_cache_manager(
            cache_config=cache_config,
            replay_config=replay_config,
            kafka_bootstrap_servers=["localhost:9092"]
        )
        
        # Create test events
        customer_created = EnhancedStreamingMessage(
            message_id=str(uuid.uuid4()),
            topic="events",
            key="customer_123",
            payload={
                "customer_id": "customer_123",
                "name": "John Doe",
                "email": "john@example.com"
            },
            timestamp=datetime.now(),
            headers={},
            event_type="customer_created",
            aggregate_id="customer_123",
            aggregate_version=1
        )
        
        # Store event
        success = manager.store_event(customer_created)
        print(f"✅ Event stored: {success}")
        
        # Get aggregate (should hit cache)
        aggregate = manager.get_aggregate("customer_123")
        if aggregate:
            print(f"✅ Retrieved aggregate version {aggregate.version}")
            print(f"   Snapshot data: {aggregate.snapshot_data}")
        
        # Test cache performance
        start_time = time.time()
        for i in range(100):
            manager.get_aggregate("customer_123", use_cache=True)
        cached_time = time.time() - start_time
        
        start_time = time.time()  
        for i in range(100):
            manager.get_aggregate("customer_123", use_cache=False)
        uncached_time = time.time() - start_time
        
        print(f"✅ Cache performance - Cached: {cached_time:.3f}s, Uncached: {uncached_time:.3f}s")
        print(f"   Cache speedup: {uncached_time/cached_time:.1f}x")
        
        # Get metrics
        metrics = manager.get_metrics()
        print(f"✅ Cache hit ratio: {metrics['cache_stats']['cache_metrics']['hit_ratio']:.2%}")
        
        # Cleanup
        manager.close()
        
        print("✅ Event Sourcing Cache Integration testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()