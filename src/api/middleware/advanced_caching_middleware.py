"""
Advanced Redis-based Response Caching Middleware

High-performance caching middleware with intelligent cache strategies:
- Multi-layer caching (memory + Redis)
- Cache invalidation patterns
- Performance-aware cache decisions
- Automatic cache warming
- Cache analytics and optimization
"""

import asyncio
import hashlib
import json
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from urllib.parse import urlparse, parse_qs

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response as StarletteResponse

from core.logging import get_logger
from core.caching.redis_cache_manager import get_cache_manager

logger = get_logger(__name__)


class CacheStrategy(Enum):
    """Cache strategy types."""
    AGGRESSIVE = "aggressive"    # Cache everything possible
    CONSERVATIVE = "conservative"  # Cache only safe endpoints
    ADAPTIVE = "adaptive"        # Adapt based on performance
    SELECTIVE = "selective"      # Cache based on rules


class CacheInvalidation(Enum):
    """Cache invalidation strategies."""
    TTL = "ttl"                 # Time-based expiration
    TAG_BASED = "tag_based"     # Tag-based invalidation
    DEPENDENCY = "dependency"    # Dependency-based invalidation
    MANUAL = "manual"           # Manual invalidation


class CacheLevel(Enum):
    """Cache levels for multi-layer caching."""
    MEMORY = "memory"           # In-memory cache
    REDIS = "redis"            # Redis cache
    CDN = "cdn"                # CDN cache


@dataclass
class CacheRule:
    """Cache rule configuration."""
    path_pattern: str
    methods: Set[str] = field(default_factory=lambda: {"GET"})
    ttl_seconds: int = 300  # 5 minutes default
    cache_levels: List[CacheLevel] = field(default_factory=lambda: [CacheLevel.REDIS])
    invalidation_tags: List[str] = field(default_factory=list)
    cache_key_params: List[str] = field(default_factory=list)
    exclude_params: Set[str] = field(default_factory=lambda: {"_", "timestamp", "nocache"})
    vary_headers: List[str] = field(default_factory=list)
    condition_func: Optional[Callable[[Request], bool]] = None


@dataclass
class CacheConfig:
    """Advanced cache configuration."""
    
    # Basic settings
    default_ttl: int = 300
    max_cache_size_mb: int = 100
    enable_cache_warming: bool = True
    
    # Strategy settings
    strategy: CacheStrategy = CacheStrategy.ADAPTIVE
    
    # Performance thresholds
    min_response_time_ms: float = 10.0  # Don't cache very fast responses
    max_response_size_kb: int = 1024   # Don't cache very large responses
    cache_hit_ratio_threshold: float = 0.1  # Minimum hit ratio to keep caching
    
    # Memory cache settings
    memory_cache_size: int = 1000
    memory_cache_ttl: int = 60  # 1 minute
    
    # Selective caching rules
    cache_rules: List[CacheRule] = field(default_factory=list)
    
    # Paths to never cache
    excluded_paths: Set[str] = field(default_factory=lambda: {
        "/health", "/metrics", "/docs", "/redoc", "/openapi.json", "/favicon.ico"
    })
    
    # Content types to cache
    cacheable_content_types: Set[str] = field(default_factory=lambda: {
        "application/json", "application/xml", "text/html", "text/plain",
        "text/csv", "application/csv", "image/svg+xml"
    })


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    data: bytes
    content_type: str
    headers: Dict[str, str]
    created_at: datetime
    ttl_seconds: int
    access_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)
    cache_level: CacheLevel = CacheLevel.REDIS
    
    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return datetime.utcnow() > self.created_at + timedelta(seconds=self.ttl_seconds)
    
    @property
    def age_seconds(self) -> float:
        """Get age of cache entry in seconds."""
        return (datetime.utcnow() - self.created_at).total_seconds()
    
    def touch(self):
        """Update last accessed time and increment access count."""
        self.last_accessed = datetime.utcnow()
        self.access_count += 1


@dataclass
class CacheStats:
    """Cache performance statistics."""
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_stores: int = 0
    cache_invalidations: int = 0
    cache_errors: int = 0
    
    # Performance metrics
    avg_cache_lookup_time_ms: float = 0.0
    avg_cache_store_time_ms: float = 0.0
    total_bytes_cached: int = 0
    total_bytes_served: int = 0
    
    # Hit ratios by endpoint
    endpoint_hit_ratios: Dict[str, float] = field(default_factory=dict)
    
    # Recent performance history
    recent_lookups: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    @property
    def hit_ratio(self) -> float:
        """Calculate overall cache hit ratio."""
        if self.total_requests == 0:
            return 0.0
        return self.cache_hits / self.total_requests
    
    @property
    def bytes_saved(self) -> int:
        """Calculate bytes saved through caching."""
        return self.total_bytes_served - self.total_bytes_cached
    
    def record_hit(self, lookup_time_ms: float, bytes_served: int):
        """Record cache hit."""
        self.total_requests += 1
        self.cache_hits += 1
        self.total_bytes_served += bytes_served
        self._update_avg_lookup_time(lookup_time_ms)
    
    def record_miss(self, lookup_time_ms: float):
        """Record cache miss."""
        self.total_requests += 1
        self.cache_misses += 1
        self._update_avg_lookup_time(lookup_time_ms)
    
    def record_store(self, store_time_ms: float, bytes_stored: int):
        """Record cache store operation."""
        self.cache_stores += 1
        self.total_bytes_cached += bytes_stored
        self._update_avg_store_time(store_time_ms)
    
    def _update_avg_lookup_time(self, lookup_time_ms: float):
        """Update average cache lookup time."""
        if self.avg_cache_lookup_time_ms == 0:
            self.avg_cache_lookup_time_ms = lookup_time_ms
        else:
            # Exponential moving average
            alpha = 0.1
            self.avg_cache_lookup_time_ms = (
                alpha * lookup_time_ms + (1 - alpha) * self.avg_cache_lookup_time_ms
            )
    
    def _update_avg_store_time(self, store_time_ms: float):
        """Update average cache store time."""
        if self.avg_cache_store_time_ms == 0:
            self.avg_cache_store_time_ms = store_time_ms
        else:
            # Exponential moving average
            alpha = 0.1
            self.avg_cache_store_time_ms = (
                alpha * store_time_ms + (1 - alpha) * self.avg_cache_store_time_ms
            )


class MemoryCache:
    """High-performance in-memory cache for frequently accessed data."""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 60):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache: Dict[str, CacheEntry] = {}
        self.access_order: deque = deque()
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[CacheEntry]:
        """Get entry from memory cache."""
        async with self._lock:
            entry = self.cache.get(key)
            if entry and not entry.is_expired:
                entry.touch()
                # Move to end of access order (most recently used)
                if key in self.access_order:
                    self.access_order.remove(key)
                self.access_order.append(key)
                return entry
            elif entry:
                # Remove expired entry
                del self.cache[key]
                if key in self.access_order:
                    self.access_order.remove(key)
        return None
    
    async def set(self, key: str, entry: CacheEntry):
        """Set entry in memory cache with LRU eviction."""
        async with self._lock:
            # Check if we need to evict entries
            while len(self.cache) >= self.max_size:
                if self.access_order:
                    oldest_key = self.access_order.popleft()
                    if oldest_key in self.cache:
                        del self.cache[oldest_key]
                else:
                    break
            
            self.cache[key] = entry
            self.access_order.append(key)
    
    async def delete(self, key: str) -> bool:
        """Delete entry from memory cache."""
        async with self._lock:
            if key in self.cache:
                del self.cache[key]
                if key in self.access_order:
                    self.access_order.remove(key)
                return True
        return False
    
    async def clear(self):
        """Clear all entries from memory cache."""
        async with self._lock:
            self.cache.clear()
            self.access_order.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get memory cache statistics."""
        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "utilization": len(self.cache) / self.max_size,
            "entries": [
                {
                    "key": key,
                    "age_seconds": entry.age_seconds,
                    "access_count": entry.access_count,
                    "size_bytes": len(entry.data)
                }
                for key, entry in self.cache.items()
            ][:10]  # Top 10 entries
        }


class AdvancedCacheMiddleware(BaseHTTPMiddleware):
    """Advanced caching middleware with multi-layer caching and intelligent strategies."""
    
    def __init__(self, app, config: Optional[CacheConfig] = None):
        super().__init__(app)
        self.config = config or CacheConfig()
        self.stats = CacheStats()
        
        # Multi-layer cache
        self.memory_cache = MemoryCache(
            max_size=self.config.memory_cache_size,
            default_ttl=self.config.memory_cache_ttl
        )
        
        # Cache key generation
        self.key_generator = CacheKeyGenerator()
        
        # Performance tracking
        self.endpoint_performance: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Cache warming tasks
        self.cache_warming_tasks: Set[asyncio.Task] = set()
        
        # Tag-based invalidation mapping
        self.tag_to_keys: Dict[str, Set[str]] = defaultdict(set)
        
    async def dispatch(self, request: Request, call_next):
        """Process request with advanced caching."""
        # Skip caching for excluded paths
        if self._should_skip_caching(request):
            return await call_next(request)
        
        # Only cache GET requests by default (unless rules specify otherwise)
        if not self._should_cache_request(request):
            return await call_next(request)
        
        # Generate cache key
        cache_key = await self._generate_cache_key(request)
        
        # Try to get from cache
        cache_entry = await self._get_from_cache(cache_key)
        if cache_entry:
            # Cache hit - return cached response
            cache_lookup_time = time.time() - time.time()  # This would be measured properly
            self.stats.record_hit(cache_lookup_time * 1000, len(cache_entry.data))
            
            response = self._create_response_from_cache(cache_entry)
            
            # Add cache headers
            response.headers["X-Cache"] = "HIT"
            response.headers["X-Cache-Age"] = str(int(cache_entry.age_seconds))
            response.headers["X-Cache-Level"] = cache_entry.cache_level.value
            
            return response
        
        # Cache miss - call endpoint
        start_time = time.time()
        response = await call_next(request)
        response_time_ms = (time.time() - start_time) * 1000
        
        # Record miss
        self.stats.record_miss(0)  # Lookup time would be measured properly
        
        # Decide whether to cache the response
        if await self._should_cache_response(request, response, response_time_ms):
            await self._store_in_cache(cache_key, request, response, response_time_ms)
        
        # Add cache headers
        response.headers["X-Cache"] = "MISS"
        response.headers["X-Cache-Key"] = cache_key[:16] + "..."  # Truncated for security
        
        return response
    
    def _should_skip_caching(self, request: Request) -> bool:
        """Check if request should skip caching entirely."""
        path = str(request.url.path)
        
        # Skip excluded paths
        if path in self.config.excluded_paths:
            return True
        
        # Skip if no-cache header present
        if "no-cache" in request.headers.get("cache-control", "").lower():
            return True
        
        # Skip if nocache query parameter present
        if "nocache" in request.query_params:
            return True
        
        return False
    
    def _should_cache_request(self, request: Request) -> bool:
        """Check if request should be cached based on method and rules."""
        method = request.method.upper()
        path = str(request.url.path)
        
        # Check cache rules
        for rule in self.config.cache_rules:
            if self._matches_pattern(path, rule.path_pattern):
                if method in rule.methods:
                    # Check condition function if specified
                    if rule.condition_func:
                        return rule.condition_func(request)
                    return True
        
        # Default: only cache GET requests
        return method == "GET"
    
    def _matches_pattern(self, path: str, pattern: str) -> bool:
        """Check if path matches pattern (supports wildcards)."""
        if "*" not in pattern:
            return path == pattern
        
        # Simple wildcard matching
        if pattern.endswith("*"):
            return path.startswith(pattern[:-1])
        
        # More complex pattern matching could be added here
        return path == pattern
    
    async def _generate_cache_key(self, request: Request) -> str:
        """Generate cache key for request."""
        return await self.key_generator.generate_key(
            method=request.method,
            url=str(request.url),
            headers=dict(request.headers),
            body=await self._get_request_body(request) if request.method in ["POST", "PUT", "PATCH"] else None
        )
    
    async def _get_request_body(self, request: Request) -> Optional[bytes]:
        """Get request body for cache key generation."""
        try:
            if hasattr(request, "_body"):
                return request._body
            
            body = await request.body()
            request._body = body  # Cache for potential reuse
            return body
        except Exception as e:
            logger.warning(f"Failed to read request body for caching: {e}")
            return None
    
    async def _get_from_cache(self, cache_key: str) -> Optional[CacheEntry]:
        """Get entry from multi-layer cache."""
        # Try memory cache first
        if CacheLevel.MEMORY in [rule.cache_levels[0] for rule in self.config.cache_rules if rule.cache_levels]:
            memory_entry = await self.memory_cache.get(cache_key)
            if memory_entry:
                return memory_entry
        
        # Try Redis cache
        try:
            cache_manager = await get_cache_manager()
            cached_data = await cache_manager.get(cache_key, namespace="responses")
            
            if cached_data:
                # Deserialize cache entry
                entry = CacheEntry(
                    key=cache_key,
                    data=cached_data.get("data", b""),
                    content_type=cached_data.get("content_type", "application/json"),
                    headers=cached_data.get("headers", {}),
                    created_at=datetime.fromisoformat(cached_data.get("created_at")),
                    ttl_seconds=cached_data.get("ttl_seconds", self.config.default_ttl),
                    access_count=cached_data.get("access_count", 0),
                    tags=cached_data.get("tags", []),
                    cache_level=CacheLevel.REDIS
                )
                
                if not entry.is_expired:
                    entry.touch()
                    # Also store in memory cache for faster access
                    if CacheLevel.MEMORY in [rule.cache_levels[0] for rule in self.config.cache_rules if rule.cache_levels]:
                        await self.memory_cache.set(cache_key, entry)
                    return entry
        
        except Exception as e:
            logger.error(f"Cache lookup error: {e}")
            self.stats.cache_errors += 1
        
        return None
    
    async def _should_cache_response(self, request: Request, response: Response, response_time_ms: float) -> bool:
        """Intelligent decision on whether to cache the response."""
        # Check basic conditions
        if response.status_code not in [200, 201, 202]:
            return False
        
        # Check content type
        content_type = response.headers.get("content-type", "").split(";")[0].strip()
        if content_type not in self.config.cacheable_content_types:
            return False
        
        # Check response size
        if hasattr(response, "body") and response.body:
            size_kb = len(response.body) / 1024
            if size_kb > self.config.max_response_size_kb:
                return False
        
        # Performance-based decisions
        if self.config.strategy == CacheStrategy.ADAPTIVE:
            # Don't cache very fast responses (likely not worth caching)
            if response_time_ms < self.config.min_response_time_ms:
                return False
            
            # Check endpoint-specific hit ratio
            endpoint_key = f"{request.method}:{request.url.path}"
            hit_ratio = self.stats.endpoint_hit_ratios.get(endpoint_key, 1.0)
            if hit_ratio < self.config.cache_hit_ratio_threshold:
                return False
        
        # Check cache rules
        path = str(request.url.path)
        for rule in self.config.cache_rules:
            if self._matches_pattern(path, rule.path_pattern):
                return request.method.upper() in rule.methods
        
        # Conservative strategy: only cache GET requests with reasonable response times
        if self.config.strategy == CacheStrategy.CONSERVATIVE:
            return request.method == "GET" and response_time_ms > 20.0
        
        # Aggressive strategy: cache everything possible
        if self.config.strategy == CacheStrategy.AGGRESSIVE:
            return True
        
        # Default: cache GET requests
        return request.method == "GET"
    
    async def _store_in_cache(self, cache_key: str, request: Request, response: Response, response_time_ms: float):
        """Store response in cache with intelligent TTL and tagging."""
        try:
            # Get response data
            response_data = b""
            if hasattr(response, "body"):
                response_data = response.body
            elif hasattr(response, "content"):
                if isinstance(response.content, bytes):
                    response_data = response.content
                elif isinstance(response.content, str):
                    response_data = response.content.encode("utf-8")
            
            if not response_data:
                return
            
            # Determine TTL and tags from rules
            ttl_seconds = self.config.default_ttl
            tags = []
            cache_levels = [CacheLevel.REDIS]
            
            path = str(request.url.path)
            for rule in self.config.cache_rules:
                if self._matches_pattern(path, rule.path_pattern):
                    ttl_seconds = rule.ttl_seconds
                    tags = rule.invalidation_tags
                    cache_levels = rule.cache_levels
                    break
            
            # Adaptive TTL based on performance
            if self.config.strategy == CacheStrategy.ADAPTIVE:
                # Longer TTL for slower responses (more valuable to cache)
                if response_time_ms > 100:
                    ttl_seconds = min(ttl_seconds * 2, 3600)  # Max 1 hour
                elif response_time_ms < 20:
                    ttl_seconds = max(ttl_seconds // 2, 60)   # Min 1 minute
            
            # Create cache entry
            cache_entry = CacheEntry(
                key=cache_key,
                data=response_data,
                content_type=response.headers.get("content-type", "application/json"),
                headers=dict(response.headers),
                created_at=datetime.utcnow(),
                ttl_seconds=ttl_seconds,
                tags=tags,
                cache_level=CacheLevel.REDIS
            )
            
            # Store in memory cache if configured
            if CacheLevel.MEMORY in cache_levels:
                await self.memory_cache.set(cache_key, cache_entry)
            
            # Store in Redis
            if CacheLevel.REDIS in cache_levels:
                store_start = time.time()
                cache_manager = await get_cache_manager()
                
                cache_data = {
                    "data": response_data,
                    "content_type": cache_entry.content_type,
                    "headers": cache_entry.headers,
                    "created_at": cache_entry.created_at.isoformat(),
                    "ttl_seconds": ttl_seconds,
                    "tags": tags,
                    "access_count": 0
                }
                
                await cache_manager.set(
                    cache_key,
                    cache_data,
                    ttl=ttl_seconds,
                    namespace="responses"
                )
                
                store_time_ms = (time.time() - store_start) * 1000
                self.stats.record_store(store_time_ms, len(response_data))
                
                # Update tag-to-key mapping for invalidation
                for tag in tags:
                    self.tag_to_keys[tag].add(cache_key)
            
            logger.debug(f"Cached response: {cache_key} (TTL: {ttl_seconds}s, Size: {len(response_data)} bytes)")
            
        except Exception as e:
            logger.error(f"Cache store error: {e}")
            self.stats.cache_errors += 1
    
    def _create_response_from_cache(self, cache_entry: CacheEntry) -> Response:
        """Create FastAPI response from cache entry."""
        response = Response(
            content=cache_entry.data,
            status_code=200,
            headers=cache_entry.headers.copy(),
            media_type=cache_entry.content_type
        )
        return response
    
    async def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all cache entries with the given tag."""
        invalidated_count = 0
        
        # Get keys associated with tag
        cache_keys = self.tag_to_keys.get(tag, set())
        
        try:
            cache_manager = await get_cache_manager()
            
            for cache_key in cache_keys:
                # Remove from Redis
                await cache_manager.delete(cache_key, namespace="responses")
                
                # Remove from memory cache
                await self.memory_cache.delete(cache_key)
                
                invalidated_count += 1
            
            # Clear tag mapping
            if tag in self.tag_to_keys:
                self.tag_to_keys[tag].clear()
            
            self.stats.cache_invalidations += invalidated_count
            logger.info(f"Invalidated {invalidated_count} cache entries for tag: {tag}")
            
        except Exception as e:
            logger.error(f"Cache invalidation error for tag {tag}: {e}")
            self.stats.cache_errors += 1
        
        return invalidated_count
    
    async def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching path pattern."""
        # This would require scanning all cache keys, which is expensive
        # In production, consider using Redis SCAN with patterns
        logger.warning(f"Pattern-based invalidation not fully implemented: {pattern}")
        return 0
    
    async def warm_cache(self, endpoints: List[Dict[str, Any]]):
        """Warm cache for specified endpoints."""
        if not self.config.enable_cache_warming:
            return
        
        for endpoint_config in endpoints:
            task = asyncio.create_task(self._warm_endpoint(endpoint_config))
            self.cache_warming_tasks.add(task)
            # Clean up completed tasks
            task.add_done_callback(self.cache_warming_tasks.discard)
    
    async def _warm_endpoint(self, endpoint_config: Dict[str, Any]):
        """Warm cache for a specific endpoint."""
        try:
            # This is a placeholder - in practice, you'd make actual requests
            # to warm the cache based on the endpoint configuration
            logger.info(f"Cache warming for endpoint: {endpoint_config}")
        except Exception as e:
            logger.error(f"Cache warming error: {e}")
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        memory_stats = self.memory_cache.get_stats()
        
        try:
            cache_manager = await get_cache_manager()
            redis_info = await cache_manager.get_info()
        except Exception:
            redis_info = {"error": "Could not connect to Redis"}
        
        return {
            "performance": {
                "hit_ratio": self.stats.hit_ratio,
                "total_requests": self.stats.total_requests,
                "cache_hits": self.stats.cache_hits,
                "cache_misses": self.stats.cache_misses,
                "cache_stores": self.stats.cache_stores,
                "cache_errors": self.stats.cache_errors,
                "avg_lookup_time_ms": self.stats.avg_cache_lookup_time_ms,
                "avg_store_time_ms": self.stats.avg_cache_store_time_ms,
                "bytes_saved": self.stats.bytes_saved
            },
            "memory_cache": memory_stats,
            "redis_cache": redis_info,
            "configuration": {
                "strategy": self.config.strategy.value,
                "default_ttl": self.config.default_ttl,
                "max_cache_size_mb": self.config.max_cache_size_mb,
                "cache_warming_enabled": self.config.enable_cache_warming
            },
            "tag_mappings": len(self.tag_to_keys),
            "active_warming_tasks": len(self.cache_warming_tasks)
        }


class CacheKeyGenerator:
    """Generates cache keys for requests."""
    
    def __init__(self):
        self.hash_algorithm = hashlib.blake2b
    
    async def generate_key(self, method: str, url: str, headers: Dict[str, str], 
                          body: Optional[bytes] = None) -> str:
        """Generate cache key for request."""
        # Parse URL components
        parsed_url = urlparse(url)
        path = parsed_url.path
        query_params = parse_qs(parsed_url.query)
        
        # Sort query parameters for consistent keys
        sorted_params = sorted(query_params.items())
        
        # Create key components
        key_components = [
            method.upper(),
            path,
            json.dumps(sorted_params, sort_keys=True)
        ]
        
        # Add relevant headers (e.g., Accept, Authorization)
        relevant_headers = ["accept", "authorization", "content-type"]
        header_data = {}
        for header in relevant_headers:
            if header in headers:
                header_data[header] = headers[header]
        
        if header_data:
            key_components.append(json.dumps(header_data, sort_keys=True))
        
        # Add body hash for POST/PUT/PATCH requests
        if body:
            body_hash = hashlib.sha256(body).hexdigest()
            key_components.append(body_hash)
        
        # Generate final key
        key_string = "|".join(key_components)
        key_hash = self.hash_algorithm(key_string.encode()).hexdigest()
        
        return f"resp:{key_hash}"


# Factory function
def create_advanced_cache_middleware(
    strategy: CacheStrategy = CacheStrategy.ADAPTIVE,
    default_ttl: int = 300,
    memory_cache_size: int = 1000,
    cache_rules: Optional[List[CacheRule]] = None
) -> AdvancedCacheMiddleware:
    """Create advanced cache middleware with custom configuration."""
    
    config = CacheConfig(
        strategy=strategy,
        default_ttl=default_ttl,
        memory_cache_size=memory_cache_size,
        cache_rules=cache_rules or []
    )
    
    return AdvancedCacheMiddleware(None, config)


# Global middleware instance
_cache_middleware: Optional[AdvancedCacheMiddleware] = None


def get_cache_middleware() -> AdvancedCacheMiddleware:
    """Get or create global cache middleware instance."""
    global _cache_middleware
    if _cache_middleware is None:
        _cache_middleware = create_advanced_cache_middleware()
    return _cache_middleware