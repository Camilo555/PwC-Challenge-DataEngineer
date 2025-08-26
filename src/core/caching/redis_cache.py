"""
Distributed Messaging Cache Implementation
Replaces Redis with RabbitMQ for task queuing and Kafka for event streaming.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import wraps
from typing import Any

from messaging.rabbitmq_manager import RabbitMQManager, QueueType, MessagePriority, TaskMessage, ResultMessage
from streaming.kafka_manager import KafkaManager, StreamingTopic, StreamingMessage
from core.logging import get_logger

logger = get_logger(__name__)


class ICache:
    """Cache interface for dependency injection."""

    async def get(self, key: str) -> Any | None:
        """Get value from cache."""
        raise NotImplementedError

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set value in cache."""
        raise NotImplementedError

    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        raise NotImplementedError

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        raise NotImplementedError

    async def invalidate(self, pattern: str) -> int:
        """Invalidate keys matching pattern."""
        raise NotImplementedError

    async def clear(self) -> bool:
        """Clear all cache."""
        raise NotImplementedError


class DistributedMessagingCache(ICache):
    """Distributed messaging cache implementation using RabbitMQ and Kafka."""

    def __init__(self, key_prefix: str = "pwc_challenge:",
                 default_ttl: int = 300,
                 cache_queue_name: str = "cache_operations"):

        self.key_prefix = key_prefix
        self.default_ttl = default_ttl
        self.cache_queue_name = cache_queue_name

        # Initialize messaging systems
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
        
        # In-memory cache for frequently accessed items
        self.local_cache: dict[str, dict[str, Any]] = {}
        self.cache_timestamps: dict[str, datetime] = {}
        
        # Metrics
        self.hit_count = 0
        self.miss_count = 0
        self.error_count = 0
        
        logger.info("DistributedMessagingCache initialized")

    def _build_key(self, key: str) -> str:
        """Build full cache key with prefix."""
        return f"{self.key_prefix}{key}"

    def _serialize(self, value: Any) -> str:
        """Serialize value for storage."""
        try:
            return json.dumps(value, default=str)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise

    def _deserialize(self, data: str) -> Any:
        """Deserialize value from storage."""
        try:
            return json.loads(data)
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise
            
    def _is_expired(self, timestamp: datetime, ttl: int) -> bool:
        """Check if cache entry is expired."""
        return datetime.now() > timestamp + timedelta(seconds=ttl)
        
    async def _publish_cache_operation(self, operation: str, key: str, value: Any = None, ttl: int = None) -> str:
        """Publish cache operation to RabbitMQ for distributed processing."""
        task_data = {
            "operation": operation,
            "key": key,
            "value": self._serialize(value) if value is not None else None,
            "ttl": ttl or self.default_ttl,
            "timestamp": datetime.now().isoformat()
        }
        
        task_id = self.rabbitmq_manager.publish_task(
            task_name="cache_operation",
            payload=task_data,
            queue=QueueType.TASK_QUEUE,
            priority=MessagePriority.HIGH
        )
        
        return task_id

    async def get(self, key: str) -> Any | None:
        """Get value from distributed cache system."""
        try:
            full_key = self._build_key(key)
            
            # Check local cache first for performance
            if full_key in self.local_cache:
                if not self._is_expired(
                    self.cache_timestamps[full_key], 
                    self.local_cache[full_key].get('ttl', self.default_ttl)
                ):
                    self.hit_count += 1
                    return self.local_cache[full_key]['value']
                else:
                    # Remove expired entry
                    del self.local_cache[full_key]
                    del self.cache_timestamps[full_key]
            
            # Publish cache get operation to RabbitMQ
            await self._publish_cache_operation("get", full_key)
            
            # For now, return None for cache miss
            # In a full implementation, this would wait for a response from the cache worker
            self.miss_count += 1
            return None

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache get error for key {key}: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set value in distributed cache system."""
        try:
            full_key = self._build_key(key)
            cache_ttl = ttl or self.default_ttl
            
            # Store in local cache immediately
            self.local_cache[full_key] = {
                'value': value,
                'ttl': cache_ttl
            }
            self.cache_timestamps[full_key] = datetime.now()
            
            # Publish cache set operation to RabbitMQ for distribution
            await self._publish_cache_operation("set", full_key, value, cache_ttl)
            
            # Publish cache event to Kafka for analytics/monitoring
            self.kafka_manager.produce_message(
                topic=StreamingTopic.SYSTEM_EVENTS,
                message={
                    "event_type": "cache_set",
                    "key": full_key,
                    "ttl": cache_ttl,
                    "timestamp": datetime.now().isoformat()
                },
                key=full_key
            )
            
            return True

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache set error for key {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete key from distributed cache system."""
        try:
            full_key = self._build_key(key)
            
            # Remove from local cache
            if full_key in self.local_cache:
                del self.local_cache[full_key]
                del self.cache_timestamps[full_key]
            
            # Publish delete operation to RabbitMQ
            await self._publish_cache_operation("delete", full_key)
            
            # Publish delete event to Kafka
            self.kafka_manager.produce_message(
                topic=StreamingTopic.SYSTEM_EVENTS,
                message={
                    "event_type": "cache_delete",
                    "key": full_key,
                    "timestamp": datetime.now().isoformat()
                },
                key=full_key
            )
            
            return True

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache delete error for key {key}: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists in distributed cache system."""
        try:
            full_key = self._build_key(key)
            
            # Check local cache first
            if full_key in self.local_cache:
                if not self._is_expired(
                    self.cache_timestamps[full_key], 
                    self.local_cache[full_key].get('ttl', self.default_ttl)
                ):
                    return True
                else:
                    # Remove expired entry
                    del self.local_cache[full_key]
                    del self.cache_timestamps[full_key]
            
            # For distributed check, publish existence check operation
            await self._publish_cache_operation("exists", full_key)
            
            return False

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache exists error for key {key}: {e}")
            return False

    async def invalidate(self, pattern: str) -> int:
        """Invalidate keys matching pattern in distributed cache system."""
        try:
            import fnmatch
            
            full_pattern = self._build_key(pattern)
            deleted_count = 0
            
            # Clear matching keys from local cache
            keys_to_delete = []
            for key in self.local_cache.keys():
                if fnmatch.fnmatch(key, full_pattern):
                    keys_to_delete.append(key)
            
            for key in keys_to_delete:
                del self.local_cache[key]
                del self.cache_timestamps[key]
                deleted_count += 1
            
            # Publish invalidation operation to RabbitMQ for distributed clearing
            await self._publish_cache_operation("invalidate", full_pattern)
            
            # Publish invalidation event to Kafka
            self.kafka_manager.produce_message(
                topic=StreamingTopic.SYSTEM_EVENTS,
                message={
                    "event_type": "cache_invalidate",
                    "pattern": full_pattern,
                    "local_deleted_count": deleted_count,
                    "timestamp": datetime.now().isoformat()
                },
                key=f"invalidate_{uuid.uuid4()}"
            )
            
            logger.info(f"Invalidated {deleted_count} local keys matching pattern: {pattern}")
            return deleted_count

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache invalidation error for pattern {pattern}: {e}")
            return 0

    async def clear(self) -> bool:
        """Clear all cache entries with prefix."""
        try:
            deleted_count = await self.invalidate("*")
            logger.info(f"Cleared {deleted_count} cache entries")
            return True

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache clear error: {e}")
            return False

    async def increment(self, key: str, amount: int = 1) -> int:
        """Increment numeric value in distributed cache system."""
        try:
            full_key = self._build_key(key)
            
            # Get current value from local cache
            current_value = 0
            if full_key in self.local_cache:
                if not self._is_expired(
                    self.cache_timestamps[full_key], 
                    self.local_cache[full_key].get('ttl', self.default_ttl)
                ):
                    current_value = self.local_cache[full_key]['value']
            
            # Increment and store
            new_value = current_value + amount
            await self.set(key, new_value)
            
            return new_value

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache increment error for key {key}: {e}")
            raise

    async def expire(self, key: str, ttl: int) -> bool:
        """Set expiration for existing key."""
        try:
            full_key = self._build_key(key)
            
            if full_key in self.local_cache:
                self.local_cache[full_key]['ttl'] = ttl
                self.cache_timestamps[full_key] = datetime.now()
            
            # Publish expire operation to RabbitMQ
            await self._publish_cache_operation("expire", full_key, ttl=ttl)
            
            return True

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache expire error for key {key}: {e}")
            return False

    async def get_ttl(self, key: str) -> int:
        """Get remaining TTL for key."""
        try:
            full_key = self._build_key(key)
            
            if full_key in self.local_cache and full_key in self.cache_timestamps:
                ttl = self.local_cache[full_key].get('ttl', self.default_ttl)
                elapsed = (datetime.now() - self.cache_timestamps[full_key]).total_seconds()
                remaining = max(0, int(ttl - elapsed))
                return remaining
            
            return -1

        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache TTL error for key {key}: {e}")
            return -1

    async def cache_aside(self, key: str, func: Callable, ttl: int = 300, *args, **kwargs) -> Any:
        """
        Cache-aside pattern implementation with distributed messaging.
        Check cache -> Miss -> Call function -> Store result -> Return
        """
        try:
            # Try to get from cache first
            cached_value = await self.get(key)
            if cached_value is not None:
                return cached_value

            # Cache miss - call function
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            # Store in cache
            await self.set(key, result, ttl)
            
            # Publish cache miss event to Kafka for analytics
            self.kafka_manager.produce_message(
                topic=StreamingTopic.SYSTEM_EVENTS,
                message={
                    "event_type": "cache_miss",
                    "key": key,
                    "function": func.__name__ if hasattr(func, '__name__') else str(func),
                    "timestamp": datetime.now().isoformat()
                },
                key=key
            )

            return result

        except Exception as e:
            logger.error(f"Cache-aside error for key {key}: {e}")
            # Fallback to direct function call
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

    async def get_stats(self) -> dict[str, Any]:
        """Get distributed cache statistics."""
        total_requests = self.hit_count + self.miss_count
        hit_ratio = self.hit_count / total_requests if total_requests > 0 else 0
        
        # Get local cache info
        local_cache_size = len(self.local_cache)
        
        # Get messaging system stats
        try:
            rabbitmq_stats = self.rabbitmq_manager.get_queue_stats(QueueType.TASK_QUEUE)
            kafka_stats = self.kafka_manager.get_metrics()
        except Exception:
            rabbitmq_stats = {}
            kafka_stats = {}

        return {
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'error_count': self.error_count,
            'hit_ratio': hit_ratio,
            'total_requests': total_requests,
            'local_cache_size': local_cache_size,
            'messaging_stats': {
                'rabbitmq': rabbitmq_stats,
                'kafka': kafka_stats
            }
        }

    async def health_check(self) -> dict[str, Any]:
        """Perform health check on distributed messaging systems."""
        try:
            start_time = datetime.now()
            
            # Check RabbitMQ connection
            rabbitmq_healthy = self.rabbitmq_manager.connect()
            
            # Check Kafka connection
            kafka_healthy = bool(self.kafka_manager.producer)
            
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            overall_status = 'healthy' if rabbitmq_healthy and kafka_healthy else 'degraded'

            return {
                'status': overall_status,
                'response_time_ms': response_time,
                'components': {
                    'rabbitmq': 'healthy' if rabbitmq_healthy else 'unhealthy',
                    'kafka': 'healthy' if kafka_healthy else 'unhealthy',
                    'local_cache': 'healthy'
                },
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


class MemoryCache(ICache):
    """In-memory cache implementation for testing/development."""

    def __init__(self, default_ttl: int = 300, max_size: int = 1000):
        self.cache: dict[str, dict[str, Any]] = {}
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.hit_count = 0
        self.miss_count = 0

    def _is_expired(self, entry: dict[str, Any]) -> bool:
        """Check if cache entry is expired."""
        if 'expires_at' not in entry:
            return False
        return datetime.now() > entry['expires_at']

    async def get(self, key: str) -> Any | None:
        """Get value from memory cache."""
        if key in self.cache:
            entry = self.cache[key]
            if not self._is_expired(entry):
                self.hit_count += 1
                return entry['value']
            else:
                del self.cache[key]

        self.miss_count += 1
        return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set value in memory cache."""
        # Evict oldest entries if at max size
        if len(self.cache) >= self.max_size and key not in self.cache:
            # Remove oldest entry
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]

        cache_ttl = ttl or self.default_ttl
        expires_at = datetime.now() + timedelta(seconds=cache_ttl)

        self.cache[key] = {
            'value': value,
            'expires_at': expires_at,
            'created_at': datetime.now()
        }
        return True

    async def delete(self, key: str) -> bool:
        """Delete key from memory cache."""
        if key in self.cache:
            del self.cache[key]
            return True
        return False

    async def exists(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        if key in self.cache:
            if not self._is_expired(self.cache[key]):
                return True
            else:
                del self.cache[key]
        return False

    async def invalidate(self, pattern: str) -> int:
        """Invalidate keys matching pattern."""
        import fnmatch

        keys_to_delete = []
        for key in self.cache.keys():
            if fnmatch.fnmatch(key, pattern):
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self.cache[key]

        return len(keys_to_delete)

    async def clear(self) -> bool:
        """Clear all cache."""
        self.cache.clear()
        return True


# Cache decorators

def cached(ttl: int = 300, key_builder: Callable | None = None, cache: ICache | None = None):
    """
    Decorator for caching function results.
    
    Args:
        ttl: Time to live in seconds
        key_builder: Function to build cache key from arguments
        cache: Cache instance to use
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get cache instance
            cache_instance = cache or get_default_cache()

            # Build cache key
            if key_builder:
                cache_key = key_builder(*args, **kwargs)
            else:
                # Default key building
                key_parts = [func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}:{v}" for k, v in sorted(kwargs.items()))
                cache_key = hashlib.md5(":".join(key_parts).encode()).hexdigest()

            # Try cache first
            cached_result = await cache_instance.get(cache_key)
            if cached_result is not None:
                return cached_result

            # Call function
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            # Cache result
            await cache_instance.set(cache_key, result, ttl)

            return result

        return wrapper
    return decorator


def cache_invalidate(pattern: str, cache: ICache | None = None):
    """
    Decorator to invalidate cache after function execution.
    
    Args:
        pattern: Cache key pattern to invalidate
        cache: Cache instance to use
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Call function first
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            # Invalidate cache
            cache_instance = cache or get_default_cache()
            await cache_instance.invalidate(pattern)

            return result

        return wrapper
    return decorator


# Cache factory

class CacheFactory:
    """Factory for creating cache instances."""

    @staticmethod
    def create_distributed_cache(**kwargs) -> DistributedMessagingCache:
        """Create distributed messaging cache instance."""
        return DistributedMessagingCache(**kwargs)

    @staticmethod
    def create_memory_cache(**kwargs) -> MemoryCache:
        """Create memory cache instance."""
        return MemoryCache(**kwargs)

    @staticmethod
    def create_cache(cache_type: str = "distributed", **kwargs) -> ICache:
        """Create cache instance by type."""
        if cache_type == "distributed":
            return CacheFactory.create_distributed_cache(**kwargs)
        elif cache_type == "memory":
            return CacheFactory.create_memory_cache(**kwargs)
        else:
            raise ValueError(f"Unknown cache type: {cache_type}")


# Global cache instance
_default_cache: ICache | None = None


def get_default_cache() -> ICache:
    """Get default cache instance."""
    global _default_cache
    if _default_cache is None:
        try:
            _default_cache = CacheFactory.create_distributed_cache()
        except Exception:
            logger.warning("Distributed messaging not available, using memory cache")
            _default_cache = CacheFactory.create_memory_cache()

    return _default_cache


def set_default_cache(cache: ICache):
    """Set default cache instance."""
    global _default_cache
    _default_cache = cache
