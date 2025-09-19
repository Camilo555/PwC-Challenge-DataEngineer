"""
Advanced Redis-based Response Caching Middleware for FastAPI
Implements intelligent caching strategies with <50ms response time optimization
"""

import json
import hashlib
import asyncio
from typing import Optional, Dict, Any, Set, List, Callable
from datetime import datetime, timedelta
import gzip
import pickle
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import Request, Response
from fastapi.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
import logging
from pydantic import BaseModel

from core.config import settings
from core.monitoring.metrics import MetricsCollector


class CacheConfig(BaseModel):
    """Configuration for Redis cache behavior"""
    default_ttl: int = 300  # 5 minutes
    max_cache_size: int = 1000000  # 1MB max cached response
    compress_threshold: int = 1024  # Compress responses > 1KB
    cache_control_header: bool = True
    etag_support: bool = True
    user_specific_cache: bool = True
    public_cache_endpoints: Set[str] = {"/health", "/metrics", "/api/v1/public"}


class CacheEntry(BaseModel):
    """Redis cache entry structure"""
    data: bytes
    content_type: str
    status_code: int
    headers: Dict[str, str]
    created_at: datetime
    expires_at: datetime
    is_compressed: bool = False
    cache_key: str
    hit_count: int = 0


class RedisCacheManager:
    """Advanced Redis cache manager with intelligent caching strategies"""

    def __init__(self, redis_url: str = None, config: CacheConfig = None):
        self.redis_url = redis_url or settings.REDIS_URL
        self.config = config or CacheConfig()
        self.redis_pool = None
        self.metrics = MetricsCollector()
        self.logger = logging.getLogger(__name__)

        # Cache strategy configuration
        self.endpoint_ttl_map = {
            "/api/v1/sales": 600,      # 10 minutes for sales data
            "/api/v1/analytics": 1800,  # 30 minutes for analytics
            "/api/v1/health": 60,       # 1 minute for health checks
            "/api/v1/users": 300,       # 5 minutes for user data
        }

        # Cache warming strategies
        self.warm_cache_endpoints = [
            "/api/v1/health",
            "/api/v1/analytics/dashboard",
            "/api/v1/sales/summary"
        ]

    async def initialize(self):
        """Initialize Redis connection pool"""
        try:
            self.redis_pool = redis.ConnectionPool.from_url(
                self.redis_url,
                max_connections=20,
                retry_on_timeout=True,
                health_check_interval=30
            )

            # Test connection
            async with redis.Redis(connection_pool=self.redis_pool) as r:
                await r.ping()

            self.logger.info("Redis cache manager initialized successfully")

            # Warm cache on startup
            await self._warm_cache()

        except Exception as e:
            self.logger.error(f"Failed to initialize Redis cache: {e}")
            raise

    async def _warm_cache(self):
        """Warm cache with frequently accessed endpoints"""
        try:
            for endpoint in self.warm_cache_endpoints:
                # This would typically make internal requests to warm the cache
                # For now, we'll just log the warming strategy
                self.logger.info(f"Cache warming strategy configured for {endpoint}")
        except Exception as e:
            self.logger.warning(f"Cache warming failed: {e}")

    def _generate_cache_key(self, request: Request, user_id: Optional[str] = None) -> str:
        """Generate intelligent cache key based on request context"""
        key_components = [
            request.method,
            str(request.url.path),
            str(request.url.query),
        ]

        # Add user context for user-specific caching
        if user_id and self.config.user_specific_cache:
            if request.url.path not in self.config.public_cache_endpoints:
                key_components.append(f"user:{user_id}")

        # Add headers that affect response content
        cache_affecting_headers = ["accept", "accept-language", "authorization"]
        for header in cache_affecting_headers:
            if header in request.headers:
                if header == "authorization":
                    # Hash authorization for privacy
                    auth_hash = hashlib.md5(request.headers[header].encode()).hexdigest()[:8]
                    key_components.append(f"{header}:{auth_hash}")
                else:
                    key_components.append(f"{header}:{request.headers[header]}")

        # Create hash of all components
        key_string = "|".join(key_components)
        cache_key = hashlib.sha256(key_string.encode()).hexdigest()[:32]

        return f"api_cache:{cache_key}"

    def _get_ttl_for_endpoint(self, path: str) -> int:
        """Get TTL for specific endpoint or default"""
        for pattern, ttl in self.endpoint_ttl_map.items():
            if path.startswith(pattern):
                return ttl
        return self.config.default_ttl

    def _should_cache_response(self, request: Request, response: Response) -> bool:
        """Determine if response should be cached"""
        # Only cache GET requests
        if request.method != "GET":
            return False

        # Don't cache error responses
        if response.status_code >= 400:
            return False

        # Don't cache responses that are too large
        content_length = response.headers.get("content-length")
        if content_length and int(content_length) > self.config.max_cache_size:
            return False

        # Don't cache if explicitly disabled
        cache_control = response.headers.get("cache-control", "")
        if "no-cache" in cache_control or "no-store" in cache_control:
            return False

        return True

    def _compress_data(self, data: bytes) -> tuple[bytes, bool]:
        """Compress data if it exceeds threshold"""
        if len(data) > self.config.compress_threshold:
            try:
                compressed = gzip.compress(data)
                if len(compressed) < len(data):
                    return compressed, True
            except Exception as e:
                self.logger.warning(f"Compression failed: {e}")

        return data, False

    def _decompress_data(self, data: bytes, is_compressed: bool) -> bytes:
        """Decompress data if needed"""
        if is_compressed:
            try:
                return gzip.decompress(data)
            except Exception as e:
                self.logger.error(f"Decompression failed: {e}")
                raise
        return data

    async def get_cached_response(self, cache_key: str) -> Optional[CacheEntry]:
        """Retrieve cached response from Redis"""
        try:
            async with redis.Redis(connection_pool=self.redis_pool) as r:
                cached_data = await r.get(cache_key)
                if not cached_data:
                    self.metrics.increment_counter("cache_miss")
                    return None

                # Deserialize cache entry
                cache_entry_dict = pickle.loads(cached_data)
                cache_entry = CacheEntry(**cache_entry_dict)

                # Check if expired (additional safety check)
                if datetime.now() > cache_entry.expires_at:
                    await r.delete(cache_key)
                    self.metrics.increment_counter("cache_expired")
                    return None

                # Update hit count
                cache_entry.hit_count += 1
                self.metrics.increment_counter("cache_hit")
                self.metrics.histogram("cache_hit_count", cache_entry.hit_count)

                return cache_entry

        except Exception as e:
            self.logger.error(f"Cache retrieval failed: {e}")
            self.metrics.increment_counter("cache_error")
            return None

    async def set_cached_response(
        self,
        cache_key: str,
        response_data: bytes,
        content_type: str,
        status_code: int,
        headers: Dict[str, str],
        ttl: int
    ) -> bool:
        """Store response in Redis cache"""
        try:
            # Compress data if needed
            compressed_data, is_compressed = self._compress_data(response_data)

            # Create cache entry
            cache_entry = CacheEntry(
                data=compressed_data,
                content_type=content_type,
                status_code=status_code,
                headers=headers,
                created_at=datetime.now(),
                expires_at=datetime.now() + timedelta(seconds=ttl),
                is_compressed=is_compressed,
                cache_key=cache_key,
                hit_count=0
            )

            # Serialize and store
            serialized_entry = pickle.dumps(cache_entry.dict())

            async with redis.Redis(connection_pool=self.redis_pool) as r:
                await r.setex(cache_key, ttl, serialized_entry)

            self.metrics.increment_counter("cache_set")
            self.metrics.histogram("cached_response_size", len(compressed_data))

            return True

        except Exception as e:
            self.logger.error(f"Cache storage failed: {e}")
            self.metrics.increment_counter("cache_set_error")
            return False

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern"""
        try:
            async with redis.Redis(connection_pool=self.redis_pool) as r:
                keys = await r.keys(f"api_cache:*{pattern}*")
                if keys:
                    deleted_count = await r.delete(*keys)
                    self.metrics.increment_counter("cache_invalidated", deleted_count)
                    return deleted_count
                return 0
        except Exception as e:
            self.logger.error(f"Cache invalidation failed: {e}")
            return 0

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        try:
            async with redis.Redis(connection_pool=self.redis_pool) as r:
                info = await r.info()
                keys_count = await r.dbsize()

                return {
                    "redis_info": {
                        "used_memory": info.get("used_memory_human"),
                        "connected_clients": info.get("connected_clients"),
                        "total_commands_processed": info.get("total_commands_processed"),
                        "keyspace_hits": info.get("keyspace_hits"),
                        "keyspace_misses": info.get("keyspace_misses"),
                    },
                    "cache_keys_count": keys_count,
                    "cache_hit_ratio": self._calculate_hit_ratio(info),
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Failed to get cache stats: {e}")
            return {"error": str(e)}

    def _calculate_hit_ratio(self, redis_info: Dict) -> float:
        """Calculate cache hit ratio from Redis info"""
        hits = redis_info.get("keyspace_hits", 0)
        misses = redis_info.get("keyspace_misses", 0)
        total = hits + misses
        return (hits / total * 100) if total > 0 else 0.0


class RedisCacheMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for Redis-based response caching"""

    def __init__(self, app, cache_manager: RedisCacheManager = None):
        super().__init__(app)
        self.cache_manager = cache_manager or RedisCacheManager()
        self.logger = logging.getLogger(__name__)

        # Initialize cache manager
        asyncio.create_task(self.cache_manager.initialize())

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with intelligent caching"""
        start_time = datetime.now()

        # Skip caching for non-GET requests
        if request.method != "GET":
            return await call_next(request)

        # Generate cache key
        user_id = self._extract_user_id(request)
        cache_key = self.cache_manager._generate_cache_key(request, user_id)

        # Try to get cached response
        cached_entry = await self.cache_manager.get_cached_response(cache_key)
        if cached_entry:
            # Return cached response
            response_data = self.cache_manager._decompress_data(
                cached_entry.data, cached_entry.is_compressed
            )

            response = Response(
                content=response_data,
                status_code=cached_entry.status_code,
                headers=cached_entry.headers,
                media_type=cached_entry.content_type
            )

            # Add cache headers
            response.headers["X-Cache-Status"] = "HIT"
            response.headers["X-Cache-Key"] = cache_key[:16]  # Partial key for debugging

            if self.cache_manager.config.etag_support:
                etag = hashlib.md5(response_data).hexdigest()[:16]
                response.headers["ETag"] = f'"{etag}"'

            # Record cache hit metrics
            cache_time = (datetime.now() - start_time).total_seconds() * 1000
            self.cache_manager.metrics.histogram("cache_response_time_ms", cache_time)

            return response

        # Cache miss - process request normally
        response = await call_next(request)

        # Determine if we should cache this response
        if self.cache_manager._should_cache_response(request, response):
            # Read response body
            response_body = b""
            if hasattr(response, 'body'):
                response_body = response.body
            elif isinstance(response, StreamingResponse):
                # Handle streaming responses
                chunks = []
                async for chunk in response.body_iterator:
                    chunks.append(chunk)
                response_body = b"".join(chunks)

                # Recreate response with consumed body
                response = Response(
                    content=response_body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type
                )

            # Cache the response
            if response_body:
                ttl = self.cache_manager._get_ttl_for_endpoint(request.url.path)
                headers_dict = dict(response.headers)

                await self.cache_manager.set_cached_response(
                    cache_key=cache_key,
                    response_data=response_body,
                    content_type=response.media_type or "application/json",
                    status_code=response.status_code,
                    headers=headers_dict,
                    ttl=ttl
                )

        # Add cache headers to response
        response.headers["X-Cache-Status"] = "MISS"
        response.headers["X-Cache-Key"] = cache_key[:16]

        # Record response time metrics
        response_time = (datetime.now() - start_time).total_seconds() * 1000
        self.cache_manager.metrics.histogram("api_response_time_ms", response_time)

        return response

    def _extract_user_id(self, request: Request) -> Optional[str]:
        """Extract user ID from request for user-specific caching"""
        try:
            # Try to get user ID from JWT token
            auth_header = request.headers.get("authorization", "")
            if auth_header.startswith("Bearer "):
                # This would typically decode the JWT token
                # For now, we'll use a hash of the token as user ID
                token = auth_header.split(" ")[1]
                return hashlib.md5(token.encode()).hexdigest()[:16]

            # Try to get user ID from session or other means
            # Implementation depends on your auth system
            return None

        except Exception:
            return None


# Cache invalidation helpers
class CacheInvalidator:
    """Helper class for cache invalidation strategies"""

    def __init__(self, cache_manager: RedisCacheManager):
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__)

    async def invalidate_user_cache(self, user_id: str):
        """Invalidate all cache entries for a specific user"""
        pattern = f"user:{user_id}"
        deleted = await self.cache_manager.invalidate_pattern(pattern)
        self.logger.info(f"Invalidated {deleted} cache entries for user {user_id}")

    async def invalidate_endpoint_cache(self, endpoint_pattern: str):
        """Invalidate cache entries for specific endpoint pattern"""
        deleted = await self.cache_manager.invalidate_pattern(endpoint_pattern)
        self.logger.info(f"Invalidated {deleted} cache entries for endpoint {endpoint_pattern}")

    async def invalidate_all_cache(self):
        """Invalidate all cache entries (use with caution)"""
        pattern = "*"
        deleted = await self.cache_manager.invalidate_pattern(pattern)
        self.logger.warning(f"Invalidated ALL {deleted} cache entries")


# Cache warming service
class CacheWarmingService:
    """Service for intelligent cache warming"""

    def __init__(self, cache_manager: RedisCacheManager):
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__)

    async def warm_frequently_accessed_endpoints(self):
        """Warm cache for frequently accessed endpoints"""
        # This would typically make internal HTTP requests
        # to populate the cache with frequently accessed data
        endpoints_to_warm = [
            "/api/v1/health",
            "/api/v1/sales/summary",
            "/api/v1/analytics/dashboard"
        ]

        for endpoint in endpoints_to_warm:
            try:
                # Implementation would make internal request here
                self.logger.info(f"Warming cache for {endpoint}")
            except Exception as e:
                self.logger.error(f"Failed to warm cache for {endpoint}: {e}")