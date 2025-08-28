"""
Enterprise Redis Cache Manager

Comprehensive Redis caching implementation with clustering, high availability,
multiple caching patterns, and advanced monitoring capabilities.
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Union

import redis
import redis.asyncio as aioredis
from redis.sentinel import Sentinel
from redis.cluster import RedisCluster
from redis.connection import ConnectionPool
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

from core.logging import get_logger
from core.config import get_settings

logger = get_logger(__name__)
settings = get_settings()


class CacheStats:
    """Cache statistics tracking."""
    
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self.operations = 0
        self.start_time = time.time()
    
    def record_hit(self):
        self.hits += 1
        self.operations += 1
    
    def record_miss(self):
        self.misses += 1
        self.operations += 1
    
    def record_error(self):
        self.errors += 1
        self.operations += 1
    
    @property
    def hit_rate(self) -> float:
        return self.hits / self.operations if self.operations > 0 else 0.0
    
    @property
    def miss_rate(self) -> float:
        return self.misses / self.operations if self.operations > 0 else 0.0
    
    @property
    def error_rate(self) -> float:
        return self.errors / self.operations if self.operations > 0 else 0.0
    
    @property
    def uptime(self) -> float:
        return time.time() - self.start_time


class RedisConfig:
    """Redis configuration management."""
    
    def __init__(self):
        self.host = settings.REDIS_HOST or "localhost"
        self.port = settings.REDIS_PORT or 6379
        self.password = settings.REDIS_PASSWORD
        self.db = settings.REDIS_DB or 0
        self.cluster_enabled = getattr(settings, 'REDIS_CLUSTER_ENABLED', False)
        self.sentinel_enabled = getattr(settings, 'REDIS_SENTINEL_ENABLED', False)
        self.cluster_nodes = getattr(settings, 'REDIS_CLUSTER_NODES', [])
        self.sentinel_hosts = getattr(settings, 'REDIS_SENTINEL_HOSTS', [])
        self.master_name = getattr(settings, 'REDIS_MASTER_NAME', 'mymaster')
        self.max_connections = getattr(settings, 'REDIS_MAX_CONNECTIONS', 100)
        self.connection_timeout = getattr(settings, 'REDIS_CONNECTION_TIMEOUT', 5)
        self.socket_timeout = getattr(settings, 'REDIS_SOCKET_TIMEOUT', 5)
        self.retry_on_timeout = True
        self.health_check_interval = 30


class RedisCacheManager:
    """
    Enterprise Redis cache manager with clustering, patterns, and monitoring.
    """
    
    def __init__(self, config: Optional[RedisConfig] = None, key_prefix: str = "pwc:"):
        self.config = config or RedisConfig()
        self.key_prefix = key_prefix
        self.stats = CacheStats()
        self.redis_client = None
        self.async_redis_client = None
        self.is_cluster = False
        self.is_sentinel = False
        self.connection_pool = None
        self._initialized = False
        
    async def initialize(self) -> bool:
        """Initialize Redis connections based on configuration."""
        try:
            if self._initialized:
                return True
                
            # Configure retry strategy
            retry_strategy = Retry(
                ExponentialBackoff(),
                retries=3
            )
            
            # Initialize based on deployment type
            if self.config.cluster_enabled:
                await self._init_cluster()
            elif self.config.sentinel_enabled:
                await self._init_sentinel()
            else:
                await self._init_standalone()
                
            # Test connection
            await self.ping()
            self._initialized = True
            logger.info(f"Redis cache manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Redis cache manager: {e}")
            return False
    
    async def _init_cluster(self):
        """Initialize Redis cluster connection."""
        try:
            startup_nodes = [
                {"host": node.split(":")[0], "port": int(node.split(":")[1])}
                for node in self.config.cluster_nodes
            ]
            
            # Sync client for cluster
            self.redis_client = RedisCluster(
                startup_nodes=startup_nodes,
                password=self.config.password,
                decode_responses=True,
                skip_full_coverage_check=True,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.connection_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
                health_check_interval=self.config.health_check_interval
            )
            
            # Async client
            self.async_redis_client = aioredis.RedisCluster(
                startup_nodes=startup_nodes,
                password=self.config.password,
                decode_responses=True,
                skip_full_coverage_check=True,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.connection_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
                health_check_interval=self.config.health_check_interval
            )
            
            self.is_cluster = True
            logger.info("Redis cluster connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Redis cluster: {e}")
            raise
    
    async def _init_sentinel(self):
        """Initialize Redis with Sentinel for HA."""
        try:
            sentinel_hosts = [
                (host.split(":")[0], int(host.split(":")[1]))
                for host in self.config.sentinel_hosts
            ]
            
            sentinel = Sentinel(
                sentinel_hosts,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.connection_timeout,
                password=self.config.password
            )
            
            # Get master connection
            self.redis_client = sentinel.master_for(
                self.config.master_name,
                password=self.config.password,
                db=self.config.db,
                decode_responses=True,
                retry_on_timeout=self.config.retry_on_timeout
            )
            
            # Async sentinel setup
            async_sentinel = aioredis.Sentinel(
                sentinel_hosts,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.connection_timeout,
                password=self.config.password
            )
            
            self.async_redis_client = async_sentinel.master_for(
                self.config.master_name,
                password=self.config.password,
                db=self.config.db,
                decode_responses=True,
                retry_on_timeout=self.config.retry_on_timeout
            )
            
            self.is_sentinel = True
            logger.info("Redis Sentinel connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Redis Sentinel: {e}")
            raise
    
    async def _init_standalone(self):
        """Initialize standalone Redis connection."""
        try:
            # Connection pool for better performance
            self.connection_pool = ConnectionPool(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=self.config.db,
                max_connections=self.config.max_connections,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.connection_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
                health_check_interval=self.config.health_check_interval,
                decode_responses=True
            )
            
            # Sync client
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)
            
            # Async client
            self.async_redis_client = aioredis.Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=self.config.db,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.connection_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
                health_check_interval=self.config.health_check_interval,
                decode_responses=True
            )
            
            logger.info("Redis standalone connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize standalone Redis: {e}")
            raise
    
    def _build_key(self, key: str, namespace: str = "") -> str:
        """Build prefixed cache key."""
        if namespace:
            return f"{self.key_prefix}{namespace}:{key}"
        return f"{self.key_prefix}{key}"
    
    def _serialize_value(self, value: Any) -> str:
        """Serialize value for Redis storage."""
        try:
            if isinstance(value, (str, int, float, bool)):
                return json.dumps({"v": value, "t": type(value).__name__})
            return json.dumps({"v": value, "t": "object"}, default=str)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
    
    def _deserialize_value(self, data: str) -> Any:
        """Deserialize value from Redis."""
        try:
            if not data:
                return None
            parsed = json.loads(data)
            return parsed.get("v")
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise
    
    async def ping(self) -> bool:
        """Test Redis connection."""
        try:
            if self.async_redis_client:
                result = await self.async_redis_client.ping()
                return result
            return False
        except Exception as e:
            logger.error(f"Redis ping failed: {e}")
            return False
    
    # Basic Cache Operations
    
    async def get(self, key: str, namespace: str = "") -> Any:
        """Get value from cache."""
        try:
            cache_key = self._build_key(key, namespace)
            data = await self.async_redis_client.get(cache_key)
            
            if data is not None:
                self.stats.record_hit()
                return self._deserialize_value(data)
            
            self.stats.record_miss()
            return None
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, 
                 namespace: str = "", nx: bool = False, xx: bool = False) -> bool:
        """Set value in cache with optional TTL."""
        try:
            cache_key = self._build_key(key, namespace)
            serialized_value = self._serialize_value(value)
            
            result = await self.async_redis_client.set(
                cache_key, 
                serialized_value, 
                ex=ttl,
                nx=nx,
                xx=xx
            )
            
            if result:
                self.stats.operations += 1
                logger.debug(f"Cache set successful for key {key}")
            return bool(result)
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str, namespace: str = "") -> bool:
        """Delete key from cache."""
        try:
            cache_key = self._build_key(key, namespace)
            result = await self.async_redis_client.delete(cache_key)
            
            if result > 0:
                self.stats.operations += 1
            return result > 0
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def exists(self, key: str, namespace: str = "") -> bool:
        """Check if key exists in cache."""
        try:
            cache_key = self._build_key(key, namespace)
            result = await self.async_redis_client.exists(cache_key)
            return bool(result)
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache exists error for key {key}: {e}")
            return False
    
    async def expire(self, key: str, ttl: int, namespace: str = "") -> bool:
        """Set expiration for existing key."""
        try:
            cache_key = self._build_key(key, namespace)
            result = await self.async_redis_client.expire(cache_key, ttl)
            return bool(result)
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache expire error for key {key}: {e}")
            return False
    
    async def ttl(self, key: str, namespace: str = "") -> int:
        """Get remaining TTL for key."""
        try:
            cache_key = self._build_key(key, namespace)
            return await self.async_redis_client.ttl(cache_key)
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache TTL error for key {key}: {e}")
            return -1
    
    # Advanced Operations
    
    async def increment(self, key: str, amount: int = 1, namespace: str = "") -> int:
        """Increment numeric value."""
        try:
            cache_key = self._build_key(key, namespace)
            result = await self.async_redis_client.incrby(cache_key, amount)
            self.stats.operations += 1
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache increment error for key {key}: {e}")
            raise
    
    async def decrement(self, key: str, amount: int = 1, namespace: str = "") -> int:
        """Decrement numeric value."""
        try:
            cache_key = self._build_key(key, namespace)
            result = await self.async_redis_client.decrby(cache_key, amount)
            self.stats.operations += 1
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache decrement error for key {key}: {e}")
            raise
    
    async def get_multi(self, keys: List[str], namespace: str = "") -> Dict[str, Any]:
        """Get multiple keys at once."""
        try:
            cache_keys = [self._build_key(key, namespace) for key in keys]
            values = await self.async_redis_client.mget(cache_keys)
            
            result = {}
            for i, key in enumerate(keys):
                if values[i] is not None:
                    result[key] = self._deserialize_value(values[i])
                    self.stats.record_hit()
                else:
                    self.stats.record_miss()
            
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache mget error: {e}")
            return {}
    
    async def set_multi(self, data: Dict[str, Any], ttl: Optional[int] = None, 
                       namespace: str = "") -> bool:
        """Set multiple key-value pairs."""
        try:
            pipe = self.async_redis_client.pipeline()
            
            for key, value in data.items():
                cache_key = self._build_key(key, namespace)
                serialized_value = self._serialize_value(value)
                
                if ttl:
                    pipe.setex(cache_key, ttl, serialized_value)
                else:
                    pipe.set(cache_key, serialized_value)
            
            results = await pipe.execute()
            self.stats.operations += len(data)
            return all(results)
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache mset error: {e}")
            return False
    
    async def invalidate_pattern(self, pattern: str, namespace: str = "") -> int:
        """Invalidate keys matching pattern."""
        try:
            search_pattern = self._build_key(pattern, namespace)
            
            if self.is_cluster:
                # For cluster, need to scan each node
                deleted_count = 0
                for node in self.async_redis_client.get_nodes():
                    keys = await node.keys(search_pattern)
                    if keys:
                        deleted_count += await node.delete(*keys)
            else:
                # For standalone/sentinel
                keys = await self.async_redis_client.keys(search_pattern)
                if keys:
                    deleted_count = await self.async_redis_client.delete(*keys)
                else:
                    deleted_count = 0
            
            logger.info(f"Invalidated {deleted_count} keys matching pattern: {pattern}")
            return deleted_count
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache pattern invalidation error: {e}")
            return 0
    
    # Hash Operations for Complex Data
    
    async def hget(self, name: str, key: str, namespace: str = "") -> Any:
        """Get field from hash."""
        try:
            hash_name = self._build_key(name, namespace)
            value = await self.async_redis_client.hget(hash_name, key)
            
            if value is not None:
                self.stats.record_hit()
                return self._deserialize_value(value)
            
            self.stats.record_miss()
            return None
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache hget error for {name}:{key}: {e}")
            return None
    
    async def hset(self, name: str, key: str, value: Any, namespace: str = "") -> bool:
        """Set field in hash."""
        try:
            hash_name = self._build_key(name, namespace)
            serialized_value = self._serialize_value(value)
            result = await self.async_redis_client.hset(hash_name, key, serialized_value)
            self.stats.operations += 1
            return bool(result)
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache hset error for {name}:{key}: {e}")
            return False
    
    async def hgetall(self, name: str, namespace: str = "") -> Dict[str, Any]:
        """Get all fields from hash."""
        try:
            hash_name = self._build_key(name, namespace)
            data = await self.async_redis_client.hgetall(hash_name)
            
            result = {}
            for key, value in data.items():
                result[key] = self._deserialize_value(value)
                self.stats.record_hit()
            
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache hgetall error for {name}: {e}")
            return {}
    
    # List Operations
    
    async def lpush(self, name: str, *values: Any, namespace: str = "") -> int:
        """Push values to left of list."""
        try:
            list_name = self._build_key(name, namespace)
            serialized_values = [self._serialize_value(v) for v in values]
            result = await self.async_redis_client.lpush(list_name, *serialized_values)
            self.stats.operations += 1
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache lpush error for {name}: {e}")
            return 0
    
    async def rpush(self, name: str, *values: Any, namespace: str = "") -> int:
        """Push values to right of list."""
        try:
            list_name = self._build_key(name, namespace)
            serialized_values = [self._serialize_value(v) for v in values]
            result = await self.async_redis_client.rpush(list_name, *serialized_values)
            self.stats.operations += 1
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache rpush error for {name}: {e}")
            return 0
    
    async def lpop(self, name: str, namespace: str = "") -> Any:
        """Pop value from left of list."""
        try:
            list_name = self._build_key(name, namespace)
            value = await self.async_redis_client.lpop(list_name)
            
            if value is not None:
                self.stats.record_hit()
                return self._deserialize_value(value)
            
            self.stats.record_miss()
            return None
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache lpop error for {name}: {e}")
            return None
    
    async def lrange(self, name: str, start: int, end: int, namespace: str = "") -> List[Any]:
        """Get range of values from list."""
        try:
            list_name = self._build_key(name, namespace)
            values = await self.async_redis_client.lrange(list_name, start, end)
            
            result = []
            for value in values:
                result.append(self._deserialize_value(value))
                self.stats.record_hit()
            
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache lrange error for {name}: {e}")
            return []
    
    # Set Operations
    
    async def sadd(self, name: str, *values: Any, namespace: str = "") -> int:
        """Add values to set."""
        try:
            set_name = self._build_key(name, namespace)
            serialized_values = [self._serialize_value(v) for v in values]
            result = await self.async_redis_client.sadd(set_name, *serialized_values)
            self.stats.operations += 1
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache sadd error for {name}: {e}")
            return 0
    
    async def smembers(self, name: str, namespace: str = "") -> Set[Any]:
        """Get all members of set."""
        try:
            set_name = self._build_key(name, namespace)
            values = await self.async_redis_client.smembers(set_name)
            
            result = set()
            for value in values:
                result.add(self._deserialize_value(value))
                self.stats.record_hit()
            
            return result
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache smembers error for {name}: {e}")
            return set()
    
    async def sismember(self, name: str, value: Any, namespace: str = "") -> bool:
        """Check if value is member of set."""
        try:
            set_name = self._build_key(name, namespace)
            serialized_value = self._serialize_value(value)
            result = await self.async_redis_client.sismember(set_name, serialized_value)
            
            if result:
                self.stats.record_hit()
            else:
                self.stats.record_miss()
            
            return bool(result)
            
        except Exception as e:
            self.stats.record_error()
            logger.error(f"Cache sismember error for {name}: {e}")
            return False
    
    # Monitoring and Health
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            info = await self.async_redis_client.info()
            
            return {
                "cache_stats": {
                    "hits": self.stats.hits,
                    "misses": self.stats.misses,
                    "errors": self.stats.errors,
                    "operations": self.stats.operations,
                    "hit_rate": self.stats.hit_rate,
                    "miss_rate": self.stats.miss_rate,
                    "error_rate": self.stats.error_rate,
                    "uptime": self.stats.uptime
                },
                "redis_stats": {
                    "connected_clients": info.get("connected_clients", 0),
                    "used_memory": info.get("used_memory", 0),
                    "used_memory_human": info.get("used_memory_human", "0B"),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0),
                    "expired_keys": info.get("expired_keys", 0),
                    "evicted_keys": info.get("evicted_keys", 0),
                    "total_commands_processed": info.get("total_commands_processed", 0),
                    "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0)
                },
                "connection_info": {
                    "is_cluster": self.is_cluster,
                    "is_sentinel": self.is_sentinel,
                    "redis_version": info.get("redis_version", "unknown")
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {
                "cache_stats": {
                    "hits": self.stats.hits,
                    "misses": self.stats.misses,
                    "errors": self.stats.errors,
                    "operations": self.stats.operations,
                    "hit_rate": self.stats.hit_rate,
                    "miss_rate": self.stats.miss_rate,
                    "error_rate": self.stats.error_rate,
                    "uptime": self.stats.uptime
                },
                "redis_stats": {},
                "connection_info": {
                    "is_cluster": self.is_cluster,
                    "is_sentinel": self.is_sentinel,
                    "error": str(e)
                }
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check."""
        try:
            start_time = time.time()
            
            # Test basic operations
            test_key = f"health_check_{uuid.uuid4().hex[:8]}"
            test_value = {"test": True, "timestamp": datetime.utcnow().isoformat()}
            
            # Set operation
            set_success = await self.set(test_key, test_value, ttl=60)
            
            # Get operation
            get_result = await self.get(test_key)
            get_success = get_result is not None
            
            # Delete operation
            delete_success = await self.delete(test_key)
            
            response_time = (time.time() - start_time) * 1000  # ms
            
            # Connection test
            ping_success = await self.ping()
            
            overall_health = all([set_success, get_success, delete_success, ping_success])
            
            return {
                "status": "healthy" if overall_health else "unhealthy",
                "response_time_ms": round(response_time, 2),
                "checks": {
                    "ping": ping_success,
                    "set_operation": set_success,
                    "get_operation": get_success,
                    "delete_operation": delete_success
                },
                "configuration": {
                    "cluster_mode": self.is_cluster,
                    "sentinel_mode": self.is_sentinel,
                    "key_prefix": self.key_prefix
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def flush_db(self, async_mode: bool = False) -> bool:
        """Flush database (use with caution)."""
        try:
            if async_mode:
                result = await self.async_redis_client.flushdb(asynchronous=True)
            else:
                result = await self.async_redis_client.flushdb()
            return bool(result)
            
        except Exception as e:
            logger.error(f"Cache flush error: {e}")
            return False
    
    async def close(self):
        """Close Redis connections."""
        try:
            if self.async_redis_client:
                await self.async_redis_client.close()
            if self.connection_pool:
                self.connection_pool.disconnect()
            logger.info("Redis connections closed")
            
        except Exception as e:
            logger.error(f"Error closing Redis connections: {e}")


# Global cache manager instance
_cache_manager: Optional[RedisCacheManager] = None


async def get_cache_manager() -> RedisCacheManager:
    """Get or create global cache manager instance."""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = RedisCacheManager()
        await _cache_manager.initialize()
    return _cache_manager


async def set_cache_manager(cache_manager: RedisCacheManager):
    """Set global cache manager instance."""
    global _cache_manager
    _cache_manager = cache_manager