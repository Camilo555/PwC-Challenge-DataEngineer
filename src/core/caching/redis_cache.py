"""
Redis Cache Implementation
Provides high-performance caching with Redis backend.
"""
import json
import pickle
import hashlib
from typing import Any, Optional, Dict, List, Callable, Union
from datetime import datetime, timedelta
from functools import wraps
import asyncio

try:
    import redis.asyncio as redis
    import redis as sync_redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False

from core.logging import get_logger

logger = get_logger(__name__)


class ICache:
    """Cache interface for dependency injection."""
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        raise NotImplementedError
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
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


class RedisCache(ICache):
    """Redis cache implementation with advanced features."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", 
                 key_prefix: str = "pwc_challenge:",
                 default_ttl: int = 300,
                 max_connections: int = 50,
                 serialization: str = "json"):
        
        if not REDIS_AVAILABLE:
            raise ImportError("Redis is not available. Install with: pip install redis")
        
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.default_ttl = default_ttl
        self.serialization = serialization
        
        # Connection pool configuration
        self.connection_pool = redis.ConnectionPool.from_url(
            redis_url,
            max_connections=max_connections,
            decode_responses=False  # We handle encoding/decoding manually
        )
        
        self.redis_client = redis.Redis(connection_pool=self.connection_pool)
        
        # Metrics
        self.hit_count = 0
        self.miss_count = 0
        self.error_count = 0
    
    def _build_key(self, key: str) -> str:
        """Build full cache key with prefix."""
        return f"{self.key_prefix}{key}"
    
    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage."""
        try:
            if self.serialization == "msgpack" and MSGPACK_AVAILABLE:
                return msgpack.packb(value, use_bin_type=True)
            elif self.serialization == "pickle":
                return pickle.dumps(value)
            else:  # Default to JSON
                return json.dumps(value, default=str).encode('utf-8')
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
    
    def _deserialize(self, data: bytes) -> Any:
        """Deserialize value from storage."""
        try:
            if self.serialization == "msgpack" and MSGPACK_AVAILABLE:
                return msgpack.unpackb(data, raw=False)
            elif self.serialization == "pickle":
                return pickle.loads(data)
            else:  # Default to JSON
                return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache with automatic deserialization."""
        try:
            full_key = self._build_key(key)
            data = await self.redis_client.get(full_key)
            
            if data is None:
                self.miss_count += 1
                return None
            
            self.hit_count += 1
            return self._deserialize(data)
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with automatic serialization."""
        try:
            full_key = self._build_key(key)
            serialized_value = self._serialize(value)
            
            cache_ttl = ttl or self.default_ttl
            
            await self.redis_client.setex(full_key, cache_ttl, serialized_value)
            return True
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        try:
            full_key = self._build_key(key)
            result = await self.redis_client.delete(full_key)
            return result > 0
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        try:
            full_key = self._build_key(key)
            result = await self.redis_client.exists(full_key)
            return result > 0
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache exists error for key {key}: {e}")
            return False
    
    async def invalidate(self, pattern: str) -> int:
        """Invalidate keys matching pattern using SCAN for safety."""
        try:
            full_pattern = self._build_key(pattern)
            deleted_count = 0
            
            # Use SCAN to safely iterate over keys
            async for key in self.redis_client.scan_iter(match=full_pattern, count=100):
                await self.redis_client.delete(key)
                deleted_count += 1
            
            logger.info(f"Invalidated {deleted_count} keys matching pattern: {pattern}")
            return deleted_count
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache invalidation error for pattern {pattern}: {e}")
            return 0
    
    async def clear(self) -> bool:
        """Clear all cache entries with prefix."""
        try:
            pattern = f"{self.key_prefix}*"
            deleted_count = await self.invalidate("*")
            logger.info(f"Cleared {deleted_count} cache entries")
            return True
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache clear error: {e}")
            return False
    
    async def increment(self, key: str, amount: int = 1) -> int:
        """Increment numeric value in cache."""
        try:
            full_key = self._build_key(key)
            return await self.redis_client.incrby(full_key, amount)
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache increment error for key {key}: {e}")
            raise
    
    async def expire(self, key: str, ttl: int) -> bool:
        """Set expiration for existing key."""
        try:
            full_key = self._build_key(key)
            return await self.redis_client.expire(full_key, ttl)
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache expire error for key {key}: {e}")
            return False
    
    async def get_ttl(self, key: str) -> int:
        """Get remaining TTL for key."""
        try:
            full_key = self._build_key(key)
            return await self.redis_client.ttl(full_key)
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Cache TTL error for key {key}: {e}")
            return -1
    
    async def cache_aside(self, key: str, func: Callable, ttl: int = 300, *args, **kwargs) -> Any:
        """
        Cache-aside pattern implementation.
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
            
            return result
            
        except Exception as e:
            logger.error(f"Cache-aside error for key {key}: {e}")
            # Fallback to direct function call
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.hit_count + self.miss_count
        hit_ratio = self.hit_count / total_requests if total_requests > 0 else 0
        
        # Get Redis info
        try:
            redis_info = await self.redis_client.info()
            memory_info = {
                'used_memory': redis_info.get('used_memory', 0),
                'used_memory_human': redis_info.get('used_memory_human', '0B'),
                'total_connections': redis_info.get('total_connections_received', 0),
                'connected_clients': redis_info.get('connected_clients', 0),
            }
        except Exception:
            memory_info = {}
        
        return {
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'error_count': self.error_count,
            'hit_ratio': hit_ratio,
            'total_requests': total_requests,
            'redis_info': memory_info
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on Redis connection."""
        try:
            start_time = datetime.now()
            await self.redis_client.ping()
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                'status': 'healthy',
                'response_time_ms': response_time,
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
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.hit_count = 0
        self.miss_count = 0
    
    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        """Check if cache entry is expired."""
        if 'expires_at' not in entry:
            return False
        return datetime.now() > entry['expires_at']
    
    async def get(self, key: str) -> Optional[Any]:
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
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
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

def cached(ttl: int = 300, key_builder: Optional[Callable] = None, cache: Optional[ICache] = None):
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


def cache_invalidate(pattern: str, cache: Optional[ICache] = None):
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
    def create_redis_cache(redis_url: str = "redis://localhost:6379", **kwargs) -> RedisCache:
        """Create Redis cache instance."""
        return RedisCache(redis_url, **kwargs)
    
    @staticmethod
    def create_memory_cache(**kwargs) -> MemoryCache:
        """Create memory cache instance."""
        return MemoryCache(**kwargs)
    
    @staticmethod
    def create_cache(cache_type: str = "redis", **kwargs) -> ICache:
        """Create cache instance by type."""
        if cache_type == "redis":
            return CacheFactory.create_redis_cache(**kwargs)
        elif cache_type == "memory":
            return CacheFactory.create_memory_cache(**kwargs)
        else:
            raise ValueError(f"Unknown cache type: {cache_type}")


# Global cache instance
_default_cache: Optional[ICache] = None


def get_default_cache() -> ICache:
    """Get default cache instance."""
    global _default_cache
    if _default_cache is None:
        try:
            _default_cache = CacheFactory.create_redis_cache()
        except ImportError:
            logger.warning("Redis not available, using memory cache")
            _default_cache = CacheFactory.create_memory_cache()
    
    return _default_cache


def set_default_cache(cache: ICache):
    """Set default cache instance."""
    global _default_cache
    _default_cache = cache