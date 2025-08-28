"""
API Response Caching with Distributed Cache Invalidation

Advanced caching system for API responses with intelligent invalidation,
conditional caching, and performance optimizations.
"""

import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from urllib.parse import urlencode

from fastapi import Request, Response
from fastapi.responses import JSONResponse
import gzip

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.cache_patterns import CacheAsidePattern, WriteThroughPattern
from core.caching.redis_streams import publish_cache_invalidation, EventType

logger = get_logger(__name__)


class CacheStrategy(Enum):
    """API caching strategies."""
    CACHE_ALL = "cache_all"
    CACHE_SUCCESS_ONLY = "cache_success_only"
    CACHE_GET_ONLY = "cache_get_only"
    CACHE_EXPENSIVE_ONLY = "cache_expensive_only"
    NO_CACHE = "no_cache"


class InvalidationTrigger(Enum):
    """Cache invalidation triggers."""
    TIME_BASED = "time_based"
    DATA_CHANGE = "data_change"
    USER_ACTION = "user_action"
    MODEL_UPDATE = "model_update"
    MANUAL = "manual"


@dataclass
class APICacheEntry:
    """API cache entry metadata."""
    cache_key: str
    method: str
    path: str
    query_params: Dict[str, Any]
    headers: Dict[str, str]
    response_status: int
    response_size: int
    created_at: datetime
    expires_at: datetime
    hit_count: int = 0
    last_accessed: datetime = None
    user_id: Optional[str] = None
    tags: List[str] = None
    etag: Optional[str] = None
    
    def __post_init__(self):
        if self.last_accessed is None:
            self.last_accessed = self.created_at
        if self.tags is None:
            self.tags = []
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return datetime.utcnow() > self.expires_at
    
    def increment_hit_count(self):
        """Increment hit count and update access time."""
        self.hit_count += 1
        self.last_accessed = datetime.utcnow()


class APICacheManager:
    """Advanced API response caching manager."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 default_ttl: int = 300,  # 5 minutes
                 max_response_size: int = 1024 * 1024,  # 1MB
                 enable_compression: bool = True):
        self.cache_manager = cache_manager
        self.default_ttl = default_ttl
        self.max_response_size = max_response_size
        self.enable_compression = enable_compression
        self.namespace = "api_cache"
        self.metadata_namespace = "api_cache_meta"
        
        # Cache strategies per endpoint
        self.endpoint_strategies: Dict[str, CacheStrategy] = {}
        self.endpoint_ttls: Dict[str, int] = {}
        
        # Invalidation rules
        self.invalidation_rules: Dict[str, List[str]] = {}  # trigger -> patterns
        
        # Statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "invalidations": 0,
            "cache_size": 0,
            "compression_ratio": 0.0
        }
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def configure_endpoint(self, path_pattern: str, strategy: CacheStrategy,
                          ttl: Optional[int] = None, tags: Optional[List[str]] = None):
        """Configure caching strategy for an endpoint."""
        self.endpoint_strategies[path_pattern] = strategy
        if ttl:
            self.endpoint_ttls[path_pattern] = ttl
        
        logger.info(f"Configured caching for {path_pattern}: {strategy.value}, TTL: {ttl or self.default_ttl}")
    
    def add_invalidation_rule(self, trigger: str, patterns: List[str]):
        """Add cache invalidation rule."""
        if trigger not in self.invalidation_rules:
            self.invalidation_rules[trigger] = []
        self.invalidation_rules[trigger].extend(patterns)
        
        logger.info(f"Added invalidation rule for trigger {trigger}: {patterns}")
    
    def _build_cache_key(self, method: str, path: str, 
                        query_params: Dict[str, Any] = None,
                        headers: Dict[str, str] = None,
                        user_id: Optional[str] = None) -> str:
        """Build cache key for API request."""
        key_parts = [method.upper(), path]
        
        # Add query parameters (sorted for consistency)
        if query_params:
            query_string = urlencode(sorted(query_params.items()))
            if query_string:
                key_parts.append(query_string)
        
        # Add relevant headers
        if headers:
            relevant_headers = {}
            for header_name in ["accept", "content-type", "authorization"]:
                if header_name in headers:
                    relevant_headers[header_name] = headers[header_name]
            
            if relevant_headers:
                header_string = urlencode(sorted(relevant_headers.items()))
                key_parts.append(header_string)
        
        # Add user context for personalized responses
        if user_id:
            key_parts.append(f"user:{user_id}")
        
        # Create hash for consistency and size
        key_string = ":".join(key_parts)
        cache_key = hashlib.md5(key_string.encode()).hexdigest()
        
        return f"api:{cache_key}"
    
    def _should_cache_request(self, method: str, path: str, 
                            response_status: int, response_size: int) -> bool:
        """Determine if request should be cached based on strategy."""
        # Find matching strategy
        strategy = CacheStrategy.CACHE_SUCCESS_ONLY  # Default
        
        for pattern, endpoint_strategy in self.endpoint_strategies.items():
            if pattern in path or path.startswith(pattern):
                strategy = endpoint_strategy
                break
        
        # Apply strategy logic
        if strategy == CacheStrategy.NO_CACHE:
            return False
        elif strategy == CacheStrategy.CACHE_GET_ONLY and method.upper() != "GET":
            return False
        elif strategy == CacheStrategy.CACHE_SUCCESS_ONLY and response_status >= 400:
            return False
        elif strategy == CacheStrategy.CACHE_EXPENSIVE_ONLY and response_size < 1024:
            # Only cache responses larger than 1KB (expensive to compute)
            return False
        
        # Size limits
        if response_size > self.max_response_size:
            logger.warning(f"Response too large to cache: {response_size} bytes")
            return False
        
        return True
    
    def _get_ttl_for_path(self, path: str) -> int:
        """Get TTL for specific path."""
        for pattern, ttl in self.endpoint_ttls.items():
            if pattern in path or path.startswith(pattern):
                return ttl
        return self.default_ttl
    
    async def _compress_response(self, data: bytes) -> bytes:
        """Compress response data if enabled."""
        if not self.enable_compression or len(data) < 1024:
            return data
        
        try:
            compressed = gzip.compress(data)
            compression_ratio = len(compressed) / len(data)
            
            # Only use compression if it saves significant space
            if compression_ratio < 0.8:
                self.stats["compression_ratio"] = compression_ratio
                return compressed
            
            return data
        except Exception as e:
            logger.error(f"Compression error: {e}")
            return data
    
    async def _decompress_response(self, data: bytes, is_compressed: bool = False) -> bytes:
        """Decompress response data if needed."""
        if not is_compressed:
            return data
        
        try:
            return gzip.decompress(data)
        except Exception as e:
            logger.error(f"Decompression error: {e}")
            return data
    
    async def cache_response(self, request: Request, response: Response,
                           response_body: bytes, user_id: Optional[str] = None,
                           tags: Optional[List[str]] = None) -> bool:
        """Cache API response."""
        try:
            method = request.method
            path = str(request.url.path)
            query_params = dict(request.query_params)
            headers = dict(request.headers)
            
            # Check if should cache
            response_size = len(response_body)
            if not self._should_cache_request(method, path, response.status_code, response_size):
                return False
            
            cache_manager = await self._get_cache_manager()
            
            # Build cache key
            cache_key = self._build_cache_key(method, path, query_params, headers, user_id)
            
            # Prepare cache data
            ttl = self._get_ttl_for_path(path)
            
            # Compress response if enabled
            compressed_body = await self._compress_response(response_body)
            is_compressed = len(compressed_body) < len(response_body)
            
            cache_data = {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body": compressed_body,
                "is_compressed": is_compressed,
                "content_type": response.headers.get("content-type", "application/json"),
                "cached_at": datetime.utcnow().isoformat(),
                "original_size": response_size,
                "compressed_size": len(compressed_body)
            }
            
            # Store in cache
            success = await cache_manager.set(
                cache_key, cache_data, ttl, self.namespace
            )
            
            if success:
                # Store metadata
                cache_entry = APICacheEntry(
                    cache_key=cache_key,
                    method=method,
                    path=path,
                    query_params=query_params,
                    headers={k: v for k, v in headers.items() if k.lower() in ["accept", "content-type"]},
                    response_status=response.status_code,
                    response_size=response_size,
                    created_at=datetime.utcnow(),
                    expires_at=datetime.utcnow() + timedelta(seconds=ttl),
                    user_id=user_id,
                    tags=tags or [],
                    etag=response.headers.get("etag")
                )
                
                await self._store_cache_metadata(cache_entry)
                
                self.stats["cache_size"] += 1
                logger.debug(f"Cached API response: {method} {path} (TTL: {ttl}s)")
            
            return success
            
        except Exception as e:
            logger.error(f"Error caching API response: {e}")
            return False
    
    async def get_cached_response(self, request: Request, 
                                user_id: Optional[str] = None) -> Optional[Response]:
        """Get cached API response."""
        try:
            method = request.method
            path = str(request.url.path)
            query_params = dict(request.query_params)
            headers = dict(request.headers)
            
            cache_manager = await self._get_cache_manager()
            
            # Build cache key
            cache_key = self._build_cache_key(method, path, query_params, headers, user_id)
            
            # Get from cache
            cached_data = await cache_manager.get(cache_key, self.namespace)
            
            if cached_data is None:
                self.stats["misses"] += 1
                return None
            
            # Check ETag if present
            client_etag = headers.get("if-none-match")
            if client_etag and cached_data.get("etag") == client_etag:
                # Return 304 Not Modified
                response = Response(status_code=304)
                response.headers["etag"] = client_etag
                return response
            
            # Decompress body if needed
            body = await self._decompress_response(
                cached_data["body"], cached_data.get("is_compressed", False)
            )
            
            # Create response
            response = Response(
                content=body,
                status_code=cached_data["status_code"],
                headers=cached_data["headers"]
            )
            
            # Add cache headers
            response.headers["X-Cache"] = "HIT"
            response.headers["X-Cache-Key"] = cache_key
            response.headers["X-Cached-At"] = cached_data["cached_at"]
            
            # Update hit count
            await self._update_hit_count(cache_key)
            
            self.stats["hits"] += 1
            logger.debug(f"Cache hit: {method} {path}")
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting cached response: {e}")
            self.stats["misses"] += 1
            return None
    
    async def invalidate_by_pattern(self, pattern: str, 
                                  trigger: InvalidationTrigger = InvalidationTrigger.MANUAL) -> int:
        """Invalidate cache entries matching pattern."""
        cache_manager = await self._get_cache_manager()
        
        # Invalidate data
        count = await cache_manager.invalidate_pattern(pattern, self.namespace)
        
        # Invalidate metadata
        meta_pattern = f"meta:{pattern}"
        await cache_manager.invalidate_pattern(meta_pattern, self.metadata_namespace)
        
        # Publish invalidation event
        await publish_cache_invalidation(
            pattern, self.namespace, pattern=True,
            source="api_cache_manager"
        )
        
        self.stats["invalidations"] += count
        logger.info(f"Invalidated {count} API cache entries with pattern: {pattern}")
        
        return count
    
    async def invalidate_by_tags(self, tags: List[str]) -> int:
        """Invalidate cache entries by tags."""
        cache_manager = await self._get_cache_manager()
        
        total_invalidated = 0
        
        try:
            # Get all metadata keys
            meta_pattern = cache_manager._build_key("meta:api:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)
            
            # Check each entry for matching tags
            for key in all_keys:
                try:
                    metadata = await cache_manager.get(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace
                    )
                    
                    if metadata and metadata.get("tags"):
                        entry_tags = metadata["tags"]
                        if any(tag in entry_tags for tag in tags):
                            # Invalidate this entry
                            cache_key = metadata["cache_key"]
                            await cache_manager.delete(cache_key, self.namespace)
                            await cache_manager.delete(f"meta:{cache_key}", self.metadata_namespace)
                            total_invalidated += 1
                
                except Exception as e:
                    logger.error(f"Error processing metadata key {key}: {e}")
        
        except Exception as e:
            logger.error(f"Error invalidating by tags: {e}")
        
        self.stats["invalidations"] += total_invalidated
        logger.info(f"Invalidated {total_invalidated} API cache entries with tags: {tags}")
        
        return total_invalidated
    
    async def invalidate_by_user(self, user_id: str) -> int:
        """Invalidate all cache entries for a specific user."""
        pattern = f"api:*:user:{user_id}*"
        return await self.invalidate_by_pattern(pattern, InvalidationTrigger.USER_ACTION)
    
    async def apply_invalidation_rules(self, trigger: str, context: Dict[str, Any] = None):
        """Apply invalidation rules for a specific trigger."""
        if trigger not in self.invalidation_rules:
            return
        
        patterns = self.invalidation_rules[trigger]
        total_invalidated = 0
        
        for pattern in patterns:
            # Apply context substitutions if provided
            if context:
                for key, value in context.items():
                    pattern = pattern.replace(f"{{{key}}}", str(value))
            
            count = await self.invalidate_by_pattern(
                pattern, InvalidationTrigger(trigger)
            )
            total_invalidated += count
        
        logger.info(f"Applied invalidation rules for trigger {trigger}: {total_invalidated} entries")
    
    async def _store_cache_metadata(self, cache_entry: APICacheEntry):
        """Store cache entry metadata."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{cache_entry.cache_key}"
        
        # Convert to dict for storage
        entry_dict = asdict(cache_entry)
        entry_dict["created_at"] = entry_dict["created_at"].isoformat()
        entry_dict["expires_at"] = entry_dict["expires_at"].isoformat()
        entry_dict["last_accessed"] = entry_dict["last_accessed"].isoformat()
        
        ttl = int((cache_entry.expires_at - datetime.utcnow()).total_seconds()) + 300
        
        await cache_manager.set(
            metadata_key, entry_dict, ttl, self.metadata_namespace
        )
    
    async def _update_hit_count(self, cache_key: str):
        """Update hit count for cache entry."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{cache_key}"
        current_metadata = await cache_manager.get(metadata_key, self.metadata_namespace)
        
        if current_metadata:
            current_metadata["hit_count"] = current_metadata.get("hit_count", 0) + 1
            current_metadata["last_accessed"] = datetime.utcnow().isoformat()
            
            await cache_manager.set(
                metadata_key, current_metadata,
                namespace=self.metadata_namespace
            )
    
    async def get_cache_statistics(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        cache_manager = await self._get_cache_manager()
        
        stats = {
            "basic_stats": self.stats.copy(),
            "endpoint_stats": {},
            "cache_health": {
                "hit_rate": 0.0,
                "avg_response_size": 0,
                "compression_savings": 0
            },
            "top_endpoints": []
        }
        
        try:
            # Calculate hit rate
            total_requests = self.stats["hits"] + self.stats["misses"]
            if total_requests > 0:
                stats["cache_health"]["hit_rate"] = self.stats["hits"] / total_requests
            
            # Get detailed stats from metadata
            meta_pattern = cache_manager._build_key("meta:api:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)
            
            endpoint_stats = defaultdict(lambda: {"hits": 0, "entries": 0, "avg_size": 0})
            total_original_size = 0
            total_compressed_size = 0
            
            for key in all_keys[:200]:  # Limit for performance
                try:
                    metadata = await cache_manager.get(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace
                    )
                    
                    if metadata:
                        path = metadata.get("path", "unknown")
                        endpoint_stats[path]["hits"] += metadata.get("hit_count", 0)
                        endpoint_stats[path]["entries"] += 1
                        endpoint_stats[path]["avg_size"] += metadata.get("response_size", 0)
                        
                        # Calculate compression savings
                        original_size = metadata.get("response_size", 0)
                        compressed_size = metadata.get("response_size", 0)  # Fallback
                        
                        total_original_size += original_size
                        total_compressed_size += compressed_size
                
                except Exception as e:
                    logger.error(f"Error processing metadata key {key}: {e}")
            
            # Calculate averages
            for path, path_stats in endpoint_stats.items():
                if path_stats["entries"] > 0:
                    path_stats["avg_size"] = path_stats["avg_size"] / path_stats["entries"]
            
            # Top endpoints by hits
            sorted_endpoints = sorted(
                endpoint_stats.items(), 
                key=lambda x: x[1]["hits"], 
                reverse=True
            )
            stats["top_endpoints"] = [
                {"path": path, **path_stats} for path, path_stats in sorted_endpoints[:10]
            ]
            
            # Calculate compression savings
            if total_original_size > 0:
                compression_savings = 1 - (total_compressed_size / total_original_size)
                stats["cache_health"]["compression_savings"] = compression_savings
            
            if len(all_keys) > 0:
                stats["cache_health"]["avg_response_size"] = total_original_size / len(all_keys)
            
            stats["endpoint_stats"] = dict(endpoint_stats)
        
        except Exception as e:
            logger.error(f"Error getting cache statistics: {e}")
            stats["error"] = str(e)
        
        return stats
    
    async def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries."""
        cache_manager = await self._get_cache_manager()
        
        try:
            meta_pattern = cache_manager._build_key("meta:api:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)
            
            cleaned_count = 0
            
            for key in all_keys:
                try:
                    metadata = await cache_manager.get(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace
                    )
                    
                    if metadata:
                        expires_at = datetime.fromisoformat(metadata["expires_at"])
                        
                        if datetime.utcnow() > expires_at:
                            cache_key = metadata["cache_key"]
                            await cache_manager.delete(cache_key, self.namespace)
                            await cache_manager.delete(f"meta:{cache_key}", self.metadata_namespace)
                            cleaned_count += 1
                
                except Exception as e:
                    logger.error(f"Error cleaning cache entry {key}: {e}")
            
            logger.info(f"Cleaned up {cleaned_count} expired API cache entries")
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Error during API cache cleanup: {e}")
            return 0


# Global API cache manager instance
_api_cache_manager: Optional[APICacheManager] = None


async def get_api_cache_manager() -> APICacheManager:
    """Get or create global API cache manager instance."""
    global _api_cache_manager
    if _api_cache_manager is None:
        _api_cache_manager = APICacheManager()
    return _api_cache_manager


# FastAPI middleware for automatic caching
class APICacheMiddleware:
    """FastAPI middleware for automatic API response caching."""
    
    def __init__(self, cache_manager: Optional[APICacheManager] = None):
        self.cache_manager = cache_manager
    
    async def __call__(self, request: Request, call_next):
        """Process request with caching."""
        if self.cache_manager is None:
            self.cache_manager = await get_api_cache_manager()
        
        # Try to get cached response
        cached_response = await self.cache_manager.get_cached_response(request)
        if cached_response:
            return cached_response
        
        # Call the actual endpoint
        response = await call_next(request)
        
        # Cache the response if appropriate
        if hasattr(response, 'body'):
            response_body = response.body
        else:
            # For streaming responses, we might not be able to cache
            response_body = b""
        
        if response_body:
            await self.cache_manager.cache_response(request, response, response_body)
        
        # Add cache miss header
        response.headers["X-Cache"] = "MISS"
        
        return response


# Decorator for method-level caching control
def cache_response(ttl: Optional[int] = None, 
                  strategy: CacheStrategy = CacheStrategy.CACHE_SUCCESS_ONLY,
                  tags: Optional[List[str]] = None):
    """Decorator for controlling API response caching."""
    def decorator(func):
        # Store caching configuration in function metadata
        func._cache_ttl = ttl
        func._cache_strategy = strategy
        func._cache_tags = tags or []
        return func
    return decorator