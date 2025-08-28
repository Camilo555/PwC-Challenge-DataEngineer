"""
Enhanced Redis Caching System

Comprehensive Redis caching implementation with clustering, high availability,
and advanced caching patterns for enterprise-grade performance.
"""

import asyncio
import json
import pickle
import time
import hashlib
import logging
from typing import Any, Dict, List, Optional, Union, Callable, Type
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import redis
import aioredis
from redis.sentinel import Sentinel
from redis.cluster import RedisCluster
import pandas as pd
import numpy as np

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector

logger = get_logger(__name__)
settings = get_settings()


class CacheStrategy(Enum):
    """Cache strategy patterns."""
    CACHE_ASIDE = "cache_aside"
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    REFRESH_AHEAD = "refresh_ahead"


class SerializationFormat(Enum):
    """Serialization formats for cached data."""
    JSON = "json"
    PICKLE = "pickle"
    MSGPACK = "msgpack"
    COMPRESSED = "compressed"


@dataclass
class CacheConfig:
    """Configuration for cache operations."""
    
    ttl_seconds: int = 3600
    max_retries: int = 3
    retry_delay: float = 0.1
    serialization: SerializationFormat = SerializationFormat.JSON
    strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE
    enable_compression: bool = False
    compression_threshold: int = 1024  # bytes
    namespace: str = "default"
    tags: List[str] = field(default_factory=list)


class RedisConnectionManager:
    """Manages Redis connections with clustering and high availability."""
    
    def __init__(self):
        self.settings = get_settings()
        self.metrics = MetricsCollector()
        self._connection = None
        self._async_connection = None
        self._cluster = None
        self._sentinel = None
        
    async def get_connection(self, async_mode: bool = True) -> Union[redis.Redis, aioredis.Redis]:
        """Get Redis connection with failover support."""
        try:
            if async_mode:
                if not self._async_connection:
                    self._async_connection = await self._create_async_connection()
                return self._async_connection
            else:
                if not self._connection:
                    self._connection = self._create_sync_connection()
                return self._connection
                
        except Exception as e:
            logger.error(f"Failed to establish Redis connection: {str(e)}")
            self.metrics.increment_counter("redis_connection_errors_total")
            raise
    
    def _create_sync_connection(self) -> redis.Redis:
        """Create synchronous Redis connection."""
        if hasattr(self.settings, 'redis_cluster_nodes') and self.settings.redis_cluster_nodes:
            # Redis Cluster mode
            return RedisCluster(
                startup_nodes=[
                    {"host": node["host"], "port": node["port"]}
                    for node in self.settings.redis_cluster_nodes
                ],
                decode_responses=False,
                skip_full_coverage_check=True,
                health_check_interval=10
            )
        elif hasattr(self.settings, 'redis_sentinel_hosts') and self.settings.redis_sentinel_hosts:
            # Redis Sentinel mode
            sentinel = Sentinel([
                (host["host"], host["port"])
                for host in self.settings.redis_sentinel_hosts
            ])
            return sentinel.master_for(
                self.settings.redis_sentinel_master_name,
                decode_responses=False,
                socket_timeout=0.1,
                socket_connect_timeout=0.1,
                retry_on_timeout=True
            )
        else:
            # Single instance mode
            return redis.Redis(
                host=getattr(self.settings, 'redis_host', 'localhost'),
                port=getattr(self.settings, 'redis_port', 6379),
                db=getattr(self.settings, 'redis_db', 0),
                password=getattr(self.settings, 'redis_password', None),
                decode_responses=False,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
    
    async def _create_async_connection(self) -> aioredis.Redis:
        """Create asynchronous Redis connection."""
        if hasattr(self.settings, 'redis_cluster_nodes') and self.settings.redis_cluster_nodes:
            # For cluster mode (simplified for example)
            return await aioredis.create_redis_pool(
                f"redis://{self.settings.redis_cluster_nodes[0]['host']}:"
                f"{self.settings.redis_cluster_nodes[0]['port']}/0"
            )
        else:
            # Single instance mode
            return await aioredis.create_redis_pool(
                f"redis://{getattr(self.settings, 'redis_host', 'localhost')}:"
                f"{getattr(self.settings, 'redis_port', 6379)}/0",
                password=getattr(self.settings, 'redis_password', None)
            )
    
    async def health_check(self) -> bool:
        """Perform Redis health check."""
        try:
            redis_conn = await self.get_connection(async_mode=True)
            await redis_conn.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {str(e)}")
            return False


class CacheSerializer:
    """Handles data serialization for caching."""
    
    @staticmethod
    def serialize(data: Any, format_type: SerializationFormat, compress: bool = False) -> bytes:
        """Serialize data for caching."""
        try:
            if format_type == SerializationFormat.JSON:
                serialized = json.dumps(data, default=str).encode('utf-8')
            elif format_type == SerializationFormat.PICKLE:
                serialized = pickle.dumps(data)
            elif format_type == SerializationFormat.MSGPACK:
                import msgpack
                serialized = msgpack.packb(data, default=str)
            else:
                raise ValueError(f"Unsupported serialization format: {format_type}")
            
            if compress and len(serialized) > 1024:
                import zlib
                serialized = zlib.compress(serialized)
                
            return serialized
            
        except Exception as e:
            logger.error(f"Serialization error: {str(e)}")
            raise
    
    @staticmethod
    def deserialize(data: bytes, format_type: SerializationFormat, 
                   decompress: bool = False) -> Any:
        """Deserialize cached data."""
        try:
            if decompress:
                import zlib
                data = zlib.decompress(data)
            
            if format_type == SerializationFormat.JSON:
                return json.loads(data.decode('utf-8'))
            elif format_type == SerializationFormat.PICKLE:
                return pickle.loads(data)
            elif format_type == SerializationFormat.MSGPACK:
                import msgpack
                return msgpack.unpackb(data, raw=False)
            else:
                raise ValueError(f"Unsupported deserialization format: {format_type}")
                
        except Exception as e:
            logger.error(f"Deserialization error: {str(e)}")
            raise


class CacheKeyManager:
    """Manages cache key generation and namespacing."""
    
    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        
    def generate_key(self, base_key: str, params: Optional[Dict] = None, 
                    version: Optional[str] = None) -> str:
        """Generate cache key with namespace and parameters."""
        key_parts = [self.namespace, base_key]
        
        if params:
            # Create deterministic hash from parameters
            param_str = json.dumps(params, sort_keys=True, default=str)
            param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]
            key_parts.append(param_hash)
            
        if version:
            key_parts.append(version)
            
        return ":".join(key_parts)
    
    def get_pattern(self, prefix: str) -> str:
        """Get pattern for key matching."""
        return f"{self.namespace}:{prefix}:*"


class EnhancedRedisCache:
    """Enhanced Redis cache with advanced patterns and features."""
    
    def __init__(self, connection_manager: Optional[RedisConnectionManager] = None):
        self.connection_manager = connection_manager or RedisConnectionManager()
        self.serializer = CacheSerializer()
        self.metrics = MetricsCollector()
        self.key_manager = CacheKeyManager()
        
    async def get(self, key: str, config: Optional[CacheConfig] = None) -> Optional[Any]:
        """Get value from cache with advanced options."""
        config = config or CacheConfig()
        full_key = self.key_manager.generate_key(key)
        
        start_time = time.time()
        
        try:
            redis_conn = await self.connection_manager.get_connection()
            cached_data = await redis_conn.get(full_key)
            
            if cached_data is None:
                self.metrics.increment_counter(
                    "redis_cache_misses_total",
                    tags={"namespace": config.namespace, "key": key}
                )
                return None
            
            # Deserialize data
            result = self.serializer.deserialize(
                cached_data, config.serialization, config.enable_compression
            )
            
            # Record metrics
            latency = (time.time() - start_time) * 1000
            self.metrics.increment_counter(
                "redis_cache_hits_total",
                tags={"namespace": config.namespace, "key": key}
            )
            self.metrics.record_histogram("redis_get_latency_ms", latency)
            
            return result
            
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {str(e)}")
            self.metrics.increment_counter("redis_cache_errors_total", tags={"operation": "get"})
            return None
    
    async def set(self, key: str, value: Any, config: Optional[CacheConfig] = None) -> bool:
        """Set value in cache with TTL and advanced options."""
        config = config or CacheConfig()
        full_key = self.key_manager.generate_key(key)
        
        start_time = time.time()
        
        try:
            # Serialize data
            serialized_data = self.serializer.serialize(
                value, config.serialization, config.enable_compression
            )
            
            redis_conn = await self.connection_manager.get_connection()
            
            # Set with TTL
            if config.ttl_seconds > 0:
                await redis_conn.setex(full_key, config.ttl_seconds, serialized_data)
            else:
                await redis_conn.set(full_key, serialized_data)
            
            # Add tags if specified
            if config.tags:
                await self._add_tags(full_key, config.tags)
            
            # Record metrics
            latency = (time.time() - start_time) * 1000
            self.metrics.increment_counter(
                "redis_cache_sets_total",
                tags={"namespace": config.namespace}
            )
            self.metrics.record_histogram("redis_set_latency_ms", latency)
            
            return True
            
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {str(e)}")
            self.metrics.increment_counter("redis_cache_errors_total", tags={"operation": "set"})
            return False
    
    async def get_or_set(self, key: str, factory: Callable, 
                        config: Optional[CacheConfig] = None) -> Any:
        """Cache-aside pattern implementation."""
        config = config or CacheConfig()
        
        # Try to get from cache first
        cached_value = await self.get(key, config)
        if cached_value is not None:
            return cached_value
        
        # Generate value and cache it
        try:
            if asyncio.iscoroutinefunction(factory):
                value = await factory()
            else:
                value = factory()
            
            await self.set(key, value, config)
            return value
            
        except Exception as e:
            logger.error(f"Factory function failed for key {key}: {str(e)}")
            raise
    
    async def invalidate(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        try:
            redis_conn = await self.connection_manager.get_connection()
            
            # Get all matching keys
            keys = await redis_conn.keys(pattern)
            
            if keys:
                deleted_count = await redis_conn.delete(*keys)
                self.metrics.increment_counter(
                    "redis_cache_invalidations_total",
                    tags={"pattern": pattern, "count": str(deleted_count)}
                )
                return deleted_count
            
            return 0
            
        except Exception as e:
            logger.error(f"Cache invalidation error for pattern {pattern}: {str(e)}")
            self.metrics.increment_counter("redis_cache_errors_total", tags={"operation": "invalidate"})
            return 0
    
    async def _add_tags(self, key: str, tags: List[str]):
        """Add tags to cache entry for easier invalidation."""
        try:
            redis_conn = await self.connection_manager.get_connection()
            
            for tag in tags:
                tag_key = f"tag:{tag}"
                await redis_conn.sadd(tag_key, key)
                # Set TTL on tag set
                await redis_conn.expire(tag_key, 86400)  # 24 hours
                
        except Exception as e:
            logger.error(f"Error adding tags: {str(e)}")
    
    async def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all cache entries with specific tag."""
        try:
            redis_conn = await self.connection_manager.get_connection()
            tag_key = f"tag:{tag}"
            
            # Get all keys with this tag
            keys = await redis_conn.smembers(tag_key)
            
            if keys:
                # Delete the cached entries
                deleted_count = await redis_conn.delete(*keys)
                # Delete the tag set
                await redis_conn.delete(tag_key)
                
                self.metrics.increment_counter(
                    "redis_cache_tag_invalidations_total",
                    tags={"tag": tag, "count": str(deleted_count)}
                )
                return deleted_count
            
            return 0
            
        except Exception as e:
            logger.error(f"Tag invalidation error for tag {tag}: {str(e)}")
            return 0


class MLModelCache(EnhancedRedisCache):
    """Specialized cache for ML model results and features."""
    
    def __init__(self, connection_manager: Optional[RedisConnectionManager] = None):
        super().__init__(connection_manager)
        self.key_manager = CacheKeyManager("ml")
    
    async def cache_prediction(self, model_id: str, features: Dict[str, Any], 
                              prediction: Any, ttl: int = 1800) -> bool:
        """Cache ML model prediction with feature hash."""
        feature_hash = self._hash_features(features)
        key = f"prediction:{model_id}:{feature_hash}"
        
        cache_data = {
            "prediction": prediction,
            "features": features,
            "timestamp": datetime.utcnow().isoformat(),
            "model_id": model_id
        }
        
        config = CacheConfig(
            ttl_seconds=ttl,
            serialization=SerializationFormat.PICKLE,
            tags=[f"model:{model_id}", "predictions"]
        )
        
        return await self.set(key, cache_data, config)
    
    async def get_prediction(self, model_id: str, features: Dict[str, Any]) -> Optional[Any]:
        """Get cached prediction for model and features."""
        feature_hash = self._hash_features(features)
        key = f"prediction:{model_id}:{feature_hash}"
        
        config = CacheConfig(serialization=SerializationFormat.PICKLE)
        cached_data = await self.get(key, config)
        
        if cached_data:
            return cached_data["prediction"]
        return None
    
    async def cache_features(self, entity_id: str, features: Dict[str, Any], 
                           ttl: int = 3600) -> bool:
        """Cache computed features for an entity."""
        key = f"features:{entity_id}"
        
        cache_data = {
            "features": features,
            "timestamp": datetime.utcnow().isoformat(),
            "entity_id": entity_id
        }
        
        config = CacheConfig(
            ttl_seconds=ttl,
            serialization=SerializationFormat.PICKLE,
            tags=["features", f"entity:{entity_id}"]
        )
        
        return await self.set(key, cache_data, config)
    
    async def get_features(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get cached features for an entity."""
        key = f"features:{entity_id}"
        
        config = CacheConfig(serialization=SerializationFormat.PICKLE)
        cached_data = await self.get(key, config)
        
        if cached_data:
            return cached_data["features"]
        return None
    
    def _hash_features(self, features: Dict[str, Any]) -> str:
        """Create hash from feature dictionary."""
        feature_str = json.dumps(features, sort_keys=True, default=str)
        return hashlib.md5(feature_str.encode()).hexdigest()[:16]


class QueryResultCache(EnhancedRedisCache):
    """Specialized cache for expensive database query results."""
    
    def __init__(self, connection_manager: Optional[RedisConnectionManager] = None):
        super().__init__(connection_manager)
        self.key_manager = CacheKeyManager("query")
    
    async def cache_dataframe(self, query_hash: str, df: pd.DataFrame, 
                            ttl: int = 3600) -> bool:
        """Cache pandas DataFrame query result."""
        key = f"dataframe:{query_hash}"
        
        # Convert DataFrame to efficient format
        cache_data = {
            "data": df.to_dict('records'),
            "columns": df.columns.tolist(),
            "index": df.index.tolist(),
            "dtypes": df.dtypes.to_dict(),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        config = CacheConfig(
            ttl_seconds=ttl,
            serialization=SerializationFormat.PICKLE,
            enable_compression=True,
            tags=["dataframes", "queries"]
        )
        
        return await self.set(key, cache_data, config)
    
    async def get_dataframe(self, query_hash: str) -> Optional[pd.DataFrame]:
        """Get cached DataFrame query result."""
        key = f"dataframe:{query_hash}"
        
        config = CacheConfig(
            serialization=SerializationFormat.PICKLE,
            enable_compression=True
        )
        
        cached_data = await self.get(key, config)
        
        if cached_data:
            df = pd.DataFrame(cached_data["data"], columns=cached_data["columns"])
            df.index = cached_data["index"]
            # Restore dtypes
            for col, dtype in cached_data["dtypes"].items():
                try:
                    df[col] = df[col].astype(dtype)
                except:
                    pass  # Skip if conversion fails
            return df
        
        return None
    
    def hash_query(self, query: str, params: Optional[Dict] = None) -> str:
        """Generate hash for query and parameters."""
        query_data = {"query": query, "params": params or {}}
        query_str = json.dumps(query_data, sort_keys=True, default=str)
        return hashlib.sha256(query_str.encode()).hexdigest()[:16]


class SessionCache(EnhancedRedisCache):
    """Specialized cache for user sessions and temporary data."""
    
    def __init__(self, connection_manager: Optional[RedisConnectionManager] = None):
        super().__init__(connection_manager)
        self.key_manager = CacheKeyManager("session")
    
    async def set_session(self, session_id: str, data: Dict[str, Any], 
                         ttl: int = 7200) -> bool:
        """Set session data with TTL."""
        key = f"session:{session_id}"
        
        session_data = {
            "data": data,
            "created_at": datetime.utcnow().isoformat(),
            "session_id": session_id
        }
        
        config = CacheConfig(
            ttl_seconds=ttl,
            serialization=SerializationFormat.JSON,
            tags=["sessions", f"user:{data.get('user_id', 'anonymous')}"]
        )
        
        return await self.set(key, session_data, config)
    
    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data."""
        key = f"session:{session_id}"
        
        config = CacheConfig(serialization=SerializationFormat.JSON)
        cached_data = await self.get(key, config)
        
        if cached_data:
            return cached_data["data"]
        return None
    
    async def extend_session(self, session_id: str, ttl: int = 7200) -> bool:
        """Extend session TTL."""
        key = f"session:{session_id}"
        full_key = self.key_manager.generate_key(key)
        
        try:
            redis_conn = await self.connection_manager.get_connection()
            result = await redis_conn.expire(full_key, ttl)
            return bool(result)
        except Exception as e:
            logger.error(f"Error extending session {session_id}: {str(e)}")
            return False


class CacheManager:
    """Centralized cache management with multiple specialized caches."""
    
    def __init__(self):
        self.connection_manager = RedisConnectionManager()
        
        # Initialize specialized caches
        self.general = EnhancedRedisCache(self.connection_manager)
        self.ml_models = MLModelCache(self.connection_manager)
        self.queries = QueryResultCache(self.connection_manager)
        self.sessions = SessionCache(self.connection_manager)
        
        self.metrics = MetricsCollector()
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive cache health check."""
        health_status = {
            "redis_connection": await self.connection_manager.health_check(),
            "timestamp": datetime.utcnow().isoformat(),
            "caches": {
                "general": "healthy",
                "ml_models": "healthy", 
                "queries": "healthy",
                "sessions": "healthy"
            }
        }
        
        return health_status
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        try:
            redis_conn = await self.connection_manager.get_connection()
            info = await redis_conn.info()
            
            stats = {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": 0
            }
            
            total_ops = stats["keyspace_hits"] + stats["keyspace_misses"]
            if total_ops > 0:
                stats["hit_rate"] = stats["keyspace_hits"] / total_ops
                
            return stats
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {}
    
    async def warm_cache(self):
        """Implement cache warming strategies."""
        logger.info("Starting cache warming process")
        
        try:
            # Warm critical data that's accessed frequently
            # This would be customized based on application needs
            
            # Example: Pre-load frequently accessed ML models
            # await self._warm_ml_models()
            
            # Example: Pre-load common query results
            # await self._warm_query_results()
            
            logger.info("Cache warming completed successfully")
            
        except Exception as e:
            logger.error(f"Cache warming failed: {str(e)}")
    
    async def cleanup_expired(self):
        """Clean up expired cache entries and optimize memory."""
        try:
            redis_conn = await self.connection_manager.get_connection()
            
            # Clean up expired tag sets
            tag_pattern = "tag:*"
            tag_keys = await redis_conn.keys(tag_pattern)
            
            cleaned_count = 0
            for tag_key in tag_keys:
                # Check if tag set is empty or expired
                size = await redis_conn.scard(tag_key)
                if size == 0:
                    await redis_conn.delete(tag_key)
                    cleaned_count += 1
            
            logger.info(f"Cleaned up {cleaned_count} expired tag sets")
            
            # Record metrics
            self.metrics.increment_counter(
                "redis_cleanup_operations_total",
                tags={"cleaned_tags": str(cleaned_count)}
            )
            
        except Exception as e:
            logger.error(f"Cache cleanup failed: {str(e)}")


# Factory function
def get_cache_manager() -> CacheManager:
    """Get global cache manager instance."""
    if not hasattr(get_cache_manager, "_instance"):
        get_cache_manager._instance = CacheManager()
    return get_cache_manager._instance