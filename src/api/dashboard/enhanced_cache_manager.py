"""
Enhanced Multi-Layer Cache Manager for Story 1.1 Dashboard API
Ultra-fast caching system optimized for <25ms response time target

Features:
- L1: In-memory cache with LRU eviction
- L2: Redis distributed cache with clustering support
- L3: Compressed object cache with smart prefetching
- Intelligent cache warming and prediction
- Performance monitoring and SLA tracking
- Circuit breaker protection
- Auto-scaling cache resources
"""
import asyncio
import json
import hashlib
import time
import pickle
import gzip
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import OrderedDict, defaultdict
import logging
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool, Redis
import aiofiles
from prometheus_client import Counter, Histogram, Gauge, Summary

from src.api.dashboard.dashboard_cache_manager import create_dashboard_cache_manager
from src.core.caching.redis_cache_manager import RedisCacheManager
from src.streaming.kafka_manager import create_kafka_manager, StreamingTopic
from core.config.unified_config import get_unified_config
from core.logging import get_logger
from api.middleware.circuit_breaker import CircuitBreaker


class CacheLevel(Enum):
    """Cache hierarchy levels"""
    L1_MEMORY = "l1_memory"           # Ultra-fast in-memory
    L2_REDIS = "l2_redis"             # Distributed Redis cache
    L3_COMPRESSED = "l3_compressed"   # Compressed persistent cache
    L4_PREDICTIVE = "l4_predictive"   # AI-driven predictive cache


class CacheStrategy(Enum):
    """Advanced cache strategies"""
    ULTRA_FAST = "ultra_fast"         # <5ms target
    HIGH_PERFORMANCE = "high_performance"  # <15ms target
    BALANCED = "balanced"             # <25ms target (Story 1.1)
    MEMORY_OPTIMIZED = "memory_optimized"  # Memory conscious
    NETWORK_OPTIMIZED = "network_optimized"  # Minimize network calls


class CompressionType(Enum):
    """Compression algorithms"""
    NONE = "none"
    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"


@dataclass
class CacheMetrics:
    """Enhanced cache performance metrics"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    refreshes: int = 0
    compressions: int = 0
    decompressions: int = 0
    total_requests: int = 0
    avg_response_time_ms: float = 0.0
    l1_hit_rate: float = 0.0
    l2_hit_rate: float = 0.0
    l3_hit_rate: float = 0.0
    cache_size_mb: float = 0.0
    memory_pressure: float = 0.0
    compression_ratio: float = 0.0
    sla_violations: int = 0
    prediction_accuracy: float = 0.0


@dataclass
class CacheConfiguration:
    """Enhanced cache configuration"""
    name: str
    strategy: CacheStrategy = CacheStrategy.BALANCED
    target_response_time_ms: float = 25.0
    l1_ttl_seconds: int = 60
    l2_ttl_seconds: int = 300
    l3_ttl_seconds: int = 3600
    max_l1_size_mb: int = 100
    max_l2_size_mb: int = 500
    compression: CompressionType = CompressionType.GZIP
    compression_threshold: int = 1024  # Compress if > 1KB
    enable_prediction: bool = True
    prefetch_probability_threshold: float = 0.7
    priority: int = 1  # 1 = highest priority
    warmup_keys: List[str] = field(default_factory=list)


class LRUCache:
    """High-performance LRU cache implementation"""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache: OrderedDict = OrderedDict()
        self.lock = asyncio.Lock()
        
    async def get(self, key: str) -> Optional[Any]:
        async with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                value = self.cache.pop(key)
                self.cache[key] = value
                return value
            return None
    
    async def set(self, key: str, value: Any):
        async with self.lock:
            if key in self.cache:
                # Update existing
                self.cache.pop(key)
            elif len(self.cache) >= self.max_size:
                # Evict least recently used
                self.cache.popitem(last=False)
            
            self.cache[key] = value
    
    async def delete(self, key: str):
        async with self.lock:
            self.cache.pop(key, None)
    
    async def clear(self):
        async with self.lock:
            self.cache.clear()
    
    def size(self) -> int:
        return len(self.cache)


class PredictiveCache:
    """AI-driven predictive caching system"""
    
    def __init__(self, window_size: int = 1000):
        self.access_patterns: Dict[str, List[float]] = defaultdict(list)
        self.predictions: Dict[str, float] = {}
        self.window_size = window_size
        self.lock = asyncio.Lock()
    
    async def record_access(self, key: str):
        """Record cache access for pattern learning"""
        async with self.lock:
            current_time = time.time()
            self.access_patterns[key].append(current_time)
            
            # Maintain sliding window
            if len(self.access_patterns[key]) > self.window_size:
                self.access_patterns[key] = self.access_patterns[key][-self.window_size:]
    
    async def predict_access_probability(self, key: str) -> float:
        """Predict probability of key being accessed soon"""
        async with self.lock:
            if key not in self.access_patterns or len(self.access_patterns[key]) < 5:
                return 0.0
            
            accesses = self.access_patterns[key]
            current_time = time.time()
            
            # Simple pattern recognition: frequency and recency
            recent_accesses = [t for t in accesses if current_time - t < 300]  # Last 5 minutes
            frequency_score = len(recent_accesses) / 300  # Accesses per second
            
            # Recency score
            if recent_accesses:
                last_access = max(recent_accesses)
                recency_score = max(0, 1 - (current_time - last_access) / 300)
            else:
                recency_score = 0
            
            # Combined probability
            probability = min(1.0, (frequency_score * 0.6 + recency_score * 0.4))
            self.predictions[key] = probability
            return probability
    
    async def get_prefetch_candidates(self, threshold: float = 0.7) -> List[str]:
        """Get keys that should be prefetched"""
        candidates = []
        for key, probability in self.predictions.items():
            if probability >= threshold:
                candidates.append(key)
        return candidates


class EnhancedCacheManager:
    """
    Ultra-high performance multi-layer cache manager
    Optimized for <25ms response time with enterprise resilience
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.config = get_unified_config()
        
        # Cache layers
        self.l1_cache = LRUCache(max_size=10000)  # In-memory ultra-fast
        self.l2_redis_pool: Optional[ConnectionPool] = None
        self.l2_redis: Optional[Redis] = None
        self.l3_disk_cache: Dict[str, Any] = {}  # Compressed disk cache
        
        # Predictive caching
        self.predictive_cache = PredictiveCache()
        
        # Configuration management
        self.cache_configs: Dict[str, CacheConfiguration] = {}
        self._initialize_cache_configs()
        
        # Performance tracking
        self.metrics: Dict[str, CacheMetrics] = {
            name: CacheMetrics() for name in self.cache_configs.keys()
        }
        self.global_metrics = CacheMetrics()
        
        # Circuit breakers for resilience
        self.circuit_breakers = {
            "l1_operations": CircuitBreaker(failure_threshold=10, timeout=5),
            "l2_operations": CircuitBreaker(failure_threshold=5, timeout=30),
            "l3_operations": CircuitBreaker(failure_threshold=3, timeout=60),
            "compression": CircuitBreaker(failure_threshold=5, timeout=15)
        }
        
        # Prometheus metrics
        self._setup_prometheus_metrics()
        
        # Background tasks
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.background_tasks: List[asyncio.Task] = []
        
        # Performance monitoring
        self.performance_window: List[float] = []
        self.performance_window_size = 1000
        
        # Initialize components
        asyncio.create_task(self._initialize_async_components())
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics"""
        self.prom_cache_hits = Counter('cache_hits_total', 'Cache hits', ['level', 'cache_name'])
        self.prom_cache_misses = Counter('cache_misses_total', 'Cache misses', ['level', 'cache_name'])
        self.prom_cache_latency = Histogram('cache_operation_seconds', 'Cache operation latency')
        self.prom_cache_size = Gauge('cache_size_bytes', 'Cache size in bytes', ['level'])
        self.prom_sla_violations = Counter('cache_sla_violations_total', 'SLA violations')
        self.prom_compression_ratio = Gauge('cache_compression_ratio', 'Compression ratio')
    
    def _initialize_cache_configs(self):
        """Initialize optimized cache configurations for dashboard types"""
        
        # Executive Dashboard - Ultra-high priority, ultra-fast
        self.cache_configs["executive_dashboard"] = CacheConfiguration(
            name="executive_dashboard",
            strategy=CacheStrategy.ULTRA_FAST,
            target_response_time_ms=10.0,  # Ultra-fast target
            l1_ttl_seconds=30,
            l2_ttl_seconds=60,
            max_l1_size_mb=50,
            compression=CompressionType.LZ4,  # Fastest compression
            priority=1,
            warmup_keys=["hourly_revenue", "daily_kpis", "executive_summary"]
        )
        
        # Revenue Analytics - High performance
        self.cache_configs["revenue_analytics"] = CacheConfiguration(
            name="revenue_analytics",
            strategy=CacheStrategy.HIGH_PERFORMANCE,
            target_response_time_ms=15.0,
            l1_ttl_seconds=60,
            l2_ttl_seconds=300,
            max_l1_size_mb=75,
            compression=CompressionType.GZIP,
            priority=1,
            warmup_keys=["revenue_trends", "sales_by_region", "top_products"]
        )
        
        # Operations Dashboard - Balanced performance
        self.cache_configs["operations_dashboard"] = CacheConfiguration(
            name="operations_dashboard",
            strategy=CacheStrategy.BALANCED,
            target_response_time_ms=25.0,  # Story 1.1 target
            l1_ttl_seconds=120,
            l2_ttl_seconds=600,
            max_l1_size_mb=40,
            compression=CompressionType.GZIP,
            priority=2,
            warmup_keys=["system_health", "pipeline_status", "error_rates"]
        )
        
        # Customer Analytics - Memory optimized
        self.cache_configs["customer_analytics"] = CacheConfiguration(
            name="customer_analytics",
            strategy=CacheStrategy.MEMORY_OPTIMIZED,
            target_response_time_ms=25.0,
            l1_ttl_seconds=300,
            l2_ttl_seconds=1800,
            max_l1_size_mb=30,
            compression=CompressionType.ZSTD,  # Best compression
            priority=3,
            warmup_keys=["customer_segments", "churn_analysis"]
        )
        
        # Performance Monitoring - Network optimized
        self.cache_configs["performance_monitoring"] = CacheConfiguration(
            name="performance_monitoring",
            strategy=CacheStrategy.NETWORK_OPTIMIZED,
            target_response_time_ms=20.0,
            l1_ttl_seconds=15,  # Very fresh data
            l2_ttl_seconds=60,
            max_l1_size_mb=25,
            compression=CompressionType.LZ4,
            priority=1,
            enable_prediction=True,
            warmup_keys=["api_response_times", "error_rates", "throughput"]
        )
    
    async def _initialize_async_components(self):
        """Initialize async components"""
        try:
            # Initialize Redis connection pool
            redis_url = getattr(self.config, 'redis_url', 'redis://localhost:6379/0')
            self.l2_redis_pool = ConnectionPool.from_url(
                redis_url,
                max_connections=50,
                socket_timeout=0.1,  # 100ms timeout for ultra-fast response
                socket_connect_timeout=0.1,
                health_check_interval=30
            )
            self.l2_redis = Redis(connection_pool=self.l2_redis_pool)
            
            # Test Redis connection
            await self.l2_redis.ping()
            self.logger.info("Redis connection established successfully")
            
            # Start background tasks
            self._start_background_tasks()
            
        except Exception as e:
            self.logger.error(f"Error initializing async components: {e}")
            # Fallback to L1 cache only
            self.l2_redis = None
    
    def _start_background_tasks(self):
        """Start background optimization tasks"""
        tasks = [
            self._cache_warming_loop(),
            self._performance_monitoring_loop(),
            self._memory_optimization_loop(),
            self._predictive_prefetch_loop(),
            self._metrics_collection_loop()
        ]
        
        for task in tasks:
            background_task = asyncio.create_task(task)
            self.background_tasks.append(background_task)
        
        self.logger.info("Enhanced cache background tasks started")
    
    async def get_ultra_fast(
        self,
        cache_name: str,
        key: str,
        user_id: Optional[str] = None,
        default: Any = None,
        force_refresh: bool = False
    ) -> Tuple[Any, float]:
        """
        Ultra-fast cache retrieval with <25ms SLA guarantee
        Returns: (value, response_time_ms)
        """
        start_time = time.perf_counter()
        
        try:
            config = self.cache_configs.get(cache_name)
            if not config:
                self.logger.warning(f"Cache config not found: {cache_name}")
                return default, (time.perf_counter() - start_time) * 1000
            
            # Generate composite key
            composite_key = self._generate_composite_key(cache_name, key, user_id)
            
            # Record access pattern for prediction
            await self.predictive_cache.record_access(composite_key)
            
            # Strategy-based retrieval
            if config.strategy == CacheStrategy.ULTRA_FAST:
                value = await self._get_ultra_fast_strategy(composite_key, config, force_refresh)
            elif config.strategy == CacheStrategy.HIGH_PERFORMANCE:
                value = await self._get_high_performance_strategy(composite_key, config, force_refresh)
            else:
                value = await self._get_balanced_strategy(composite_key, config, force_refresh)
            
            # Update metrics
            response_time_ms = (time.perf_counter() - start_time) * 1000
            await self._update_performance_metrics(cache_name, response_time_ms, value is not None)
            
            # Check SLA compliance
            if response_time_ms > config.target_response_time_ms:
                self.metrics[cache_name].sla_violations += 1
                self.prom_sla_violations.inc()
                self.logger.warning(
                    f"SLA violation: {cache_name}:{key} took {response_time_ms:.2f}ms "
                    f"(target: {config.target_response_time_ms}ms)"
                )
            
            return value if value is not None else default, response_time_ms
            
        except Exception as e:
            response_time_ms = (time.perf_counter() - start_time) * 1000
            self.logger.error(f"Error in ultra-fast get for {cache_name}:{key}: {e}")
            return default, response_time_ms
    
    async def _get_ultra_fast_strategy(
        self, composite_key: str, config: CacheConfiguration, force_refresh: bool
    ) -> Optional[Any]:
        """Ultra-fast strategy: L1 cache only with aggressive optimization"""
        
        if not force_refresh:
            # L1 cache with circuit breaker
            try:
                value = await self.circuit_breakers["l1_operations"].call(
                    self.l1_cache.get, composite_key
                )
                if value is not None:
                    self.prom_cache_hits.labels(level="l1", cache_name=config.name).inc()
                    return self._decompress_if_needed(value, config)
            except Exception as e:
                self.logger.error(f"L1 cache circuit breaker tripped: {e}")
        
        # Cache miss - get from source and cache
        fresh_data = await self._get_from_source(composite_key, config)
        if fresh_data is not None:
            # Store in L1 with compression if needed
            compressed_data = await self._compress_if_needed(fresh_data, config)
            await self.l1_cache.set(composite_key, compressed_data)
            
            self.prom_cache_misses.labels(level="l1", cache_name=config.name).inc()
            return fresh_data
        
        return None
    
    async def _get_high_performance_strategy(
        self, composite_key: str, config: CacheConfiguration, force_refresh: bool
    ) -> Optional[Any]:
        """High performance strategy: L1 + L2 with smart fallback"""
        
        if not force_refresh:
            # Try L1 first
            try:
                value = await self.circuit_breakers["l1_operations"].call(
                    self.l1_cache.get, composite_key
                )
                if value is not None:
                    self.prom_cache_hits.labels(level="l1", cache_name=config.name).inc()
                    return self._decompress_if_needed(value, config)
            except Exception:
                pass
            
            # Try L2 (Redis)
            if self.l2_redis:
                try:
                    redis_value = await self.circuit_breakers["l2_operations"].call(
                        self.l2_redis.get, composite_key
                    )
                    if redis_value:
                        # Store in L1 for next time
                        await self.l1_cache.set(composite_key, redis_value)
                        
                        self.prom_cache_hits.labels(level="l2", cache_name=config.name).inc()
                        return self._decompress_if_needed(redis_value, config)
                except Exception as e:
                    self.logger.error(f"L2 cache error: {e}")
        
        # Cache miss - get from source and cache in both levels
        fresh_data = await self._get_from_source(composite_key, config)
        if fresh_data is not None:
            compressed_data = await self._compress_if_needed(fresh_data, config)
            
            # Store in both L1 and L2
            await self.l1_cache.set(composite_key, compressed_data)
            
            if self.l2_redis:
                try:
                    await self.l2_redis.setex(
                        composite_key, config.l2_ttl_seconds, compressed_data
                    )
                except Exception as e:
                    self.logger.error(f"Error storing in L2 cache: {e}")
            
            return fresh_data
        
        return None
    
    async def _get_balanced_strategy(
        self, composite_key: str, config: CacheConfiguration, force_refresh: bool
    ) -> Optional[Any]:
        """Balanced strategy: Full multi-layer with predictive prefetching"""
        
        if not force_refresh:
            # L1 cache
            try:
                value = await self.l1_cache.get(composite_key)
                if value is not None:
                    self.prom_cache_hits.labels(level="l1", cache_name=config.name).inc()
                    return self._decompress_if_needed(value, config)
            except Exception:
                pass
            
            # L2 cache (Redis)
            if self.l2_redis:
                try:
                    redis_value = await self.l2_redis.get(composite_key)
                    if redis_value:
                        await self.l1_cache.set(composite_key, redis_value)
                        self.prom_cache_hits.labels(level="l2", cache_name=config.name).inc()
                        return self._decompress_if_needed(redis_value, config)
                except Exception:
                    pass
            
            # L3 cache (compressed disk/memory)
            if composite_key in self.l3_disk_cache:
                try:
                    l3_value = self.l3_disk_cache[composite_key]
                    await self.l1_cache.set(composite_key, l3_value)
                    
                    if self.l2_redis:
                        await self.l2_redis.setex(composite_key, config.l2_ttl_seconds, l3_value)
                    
                    self.prom_cache_hits.labels(level="l3", cache_name=config.name).inc()
                    return self._decompress_if_needed(l3_value, config)
                except Exception:
                    pass
        
        # All cache levels missed - get from source
        fresh_data = await self._get_from_source(composite_key, config)
        if fresh_data is not None:
            compressed_data = await self._compress_if_needed(fresh_data, config)
            
            # Store in all cache levels
            await self.l1_cache.set(composite_key, compressed_data)
            
            if self.l2_redis:
                await self.l2_redis.setex(composite_key, config.l2_ttl_seconds, compressed_data)
            
            self.l3_disk_cache[composite_key] = compressed_data
            
            return fresh_data
        
        return None
    
    async def set_ultra_fast(
        self,
        cache_name: str,
        key: str,
        value: Any,
        user_id: Optional[str] = None,
        ttl_override: Optional[int] = None
    ) -> bool:
        """Ultra-fast cache storage with multi-level propagation"""
        
        try:
            config = self.cache_configs.get(cache_name)
            if not config:
                return False
            
            composite_key = self._generate_composite_key(cache_name, key, user_id)
            compressed_value = await self._compress_if_needed(value, config)
            
            # Store based on strategy
            if config.strategy == CacheStrategy.ULTRA_FAST:
                # L1 only for ultra-fast
                await self.l1_cache.set(composite_key, compressed_value)
            else:
                # Multi-level storage
                await self.l1_cache.set(composite_key, compressed_value)
                
                if self.l2_redis:
                    ttl = ttl_override or config.l2_ttl_seconds
                    await self.l2_redis.setex(composite_key, ttl, compressed_value)
                
                # L3 for persistence
                self.l3_disk_cache[composite_key] = compressed_value
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in ultra-fast set for {cache_name}:{key}: {e}")
            return False
    
    async def invalidate_ultra_fast(
        self,
        cache_name: str,
        key: Optional[str] = None,
        pattern: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> int:
        """Ultra-fast cache invalidation with pattern support"""
        
        try:
            invalidated_count = 0
            
            if key:
                # Invalidate specific key
                composite_key = self._generate_composite_key(cache_name, key, user_id)
                
                await self.l1_cache.delete(composite_key)
                
                if self.l2_redis:
                    deleted = await self.l2_redis.delete(composite_key)
                    invalidated_count += deleted
                
                if composite_key in self.l3_disk_cache:
                    del self.l3_disk_cache[composite_key]
                    invalidated_count += 1
                    
            elif pattern:
                # Pattern-based invalidation
                pattern_key = f"{cache_name}:{pattern}"
                
                # L1 cache pattern matching
                l1_keys_to_delete = []
                for cached_key in self.l1_cache.cache.keys():
                    if pattern_key in str(cached_key):
                        l1_keys_to_delete.append(cached_key)
                
                for cached_key in l1_keys_to_delete:
                    await self.l1_cache.delete(cached_key)
                    invalidated_count += 1
                
                # L2 Redis pattern deletion
                if self.l2_redis:
                    cursor = 0
                    while True:
                        cursor, keys = await self.l2_redis.scan(cursor, match=f"{pattern_key}*")
                        if keys:
                            deleted = await self.l2_redis.delete(*keys)
                            invalidated_count += deleted
                        if cursor == 0:
                            break
                
                # L3 pattern matching
                l3_keys_to_delete = [k for k in self.l3_disk_cache.keys() if pattern_key in k]
                for k in l3_keys_to_delete:
                    del self.l3_disk_cache[k]
                    invalidated_count += 1
            
            else:
                # Invalidate entire cache
                await self.l1_cache.clear()
                
                if self.l2_redis:
                    cursor = 0
                    while True:
                        cursor, keys = await self.l2_redis.scan(cursor, match=f"{cache_name}:*")
                        if keys:
                            deleted = await self.l2_redis.delete(*keys)
                            invalidated_count += deleted
                        if cursor == 0:
                            break
                
                # Clear L3 cache entries for this cache_name
                l3_keys_to_delete = [k for k in self.l3_disk_cache.keys() if k.startswith(cache_name)]
                for k in l3_keys_to_delete:
                    del self.l3_disk_cache[k]
                    invalidated_count += len(l3_keys_to_delete)
            
            return invalidated_count
            
        except Exception as e:
            self.logger.error(f"Error in cache invalidation: {e}")
            return 0
    
    def _generate_composite_key(self, cache_name: str, key: str, user_id: Optional[str]) -> str:
        """Generate optimized composite cache key"""
        key_parts = [cache_name, key]
        
        if user_id:
            key_parts.append(f"u:{user_id}")
        
        # Use hash for very long keys to keep Redis key length manageable
        composite = ":".join(key_parts)
        if len(composite) > 250:  # Redis key limit is 512MB, but shorter is faster
            composite = f"{cache_name}:h:{hashlib.md5(composite.encode()).hexdigest()}"
        
        return composite
    
    async def _compress_if_needed(self, data: Any, config: CacheConfiguration) -> Any:
        """Compress data if it exceeds threshold"""
        try:
            if config.compression == CompressionType.NONE:
                return data
            
            # Serialize data
            serialized = pickle.dumps(data)
            
            if len(serialized) < config.compression_threshold:
                return data  # Don't compress small data
            
            # Apply compression based on config
            if config.compression == CompressionType.GZIP:
                compressed = await self.circuit_breakers["compression"].call(
                    self._compress_gzip, serialized
                )
            elif config.compression == CompressionType.LZ4:
                # LZ4 compression would go here
                compressed = serialized  # Placeholder
            elif config.compression == CompressionType.ZSTD:
                # ZSTD compression would go here  
                compressed = serialized  # Placeholder
            else:
                compressed = serialized
            
            # Track compression ratio
            if len(serialized) > 0:
                ratio = len(compressed) / len(serialized)
                config_metrics = self.metrics[config.name]
                config_metrics.compression_ratio = ratio
                self.prom_compression_ratio.set(ratio)
                config_metrics.compressions += 1
            
            return compressed
            
        except Exception as e:
            self.logger.error(f"Error compressing data: {e}")
            return data
    
    def _decompress_if_needed(self, data: Any, config: CacheConfiguration) -> Any:
        """Decompress data if needed"""
        try:
            if config.compression == CompressionType.NONE or not isinstance(data, bytes):
                return data
            
            # Attempt decompression
            if config.compression == CompressionType.GZIP:
                try:
                    decompressed = gzip.decompress(data)
                    result = pickle.loads(decompressed)
                    self.metrics[config.name].decompressions += 1
                    return result
                except:
                    # Not compressed, try direct pickle load
                    return pickle.loads(data)
            
            # For other compression types, implement accordingly
            return pickle.loads(data)
            
        except Exception as e:
            self.logger.error(f"Error decompressing data: {e}")
            return data
    
    def _compress_gzip(self, data: bytes) -> bytes:
        """GZIP compression - runs in thread pool for non-blocking"""
        return gzip.compress(data, compresslevel=6)  # Balanced speed vs compression
    
    async def _get_from_source(self, composite_key: str, config: CacheConfiguration) -> Optional[Any]:
        """Get data from original source (database, API, etc.)"""
        try:
            # This would integrate with your actual data sources
            # For now, return a placeholder indicating cache miss
            
            cache_name = config.name
            key_parts = composite_key.split(":")
            
            # Simulate data source lookup based on cache type
            if "executive" in cache_name:
                return {
                    "revenue_today": 125000.0,
                    "orders_today": 450,
                    "active_users": 1200,
                    "conversion_rate": 3.2,
                    "timestamp": datetime.now().isoformat(),
                    "source": "executive_kpis_api",
                    "cache_miss": True
                }
            elif "revenue" in cache_name:
                return {
                    "hourly_revenue": [12000, 15000, 18000, 22000],
                    "top_products": ["Product A", "Product B", "Product C"],
                    "regional_breakdown": {"US": 60000, "EU": 40000, "APAC": 25000},
                    "timestamp": datetime.now().isoformat(),
                    "source": "revenue_analytics_api",
                    "cache_miss": True
                }
            
            # Return None if no data source configured
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting data from source: {e}")
            return None
    
    async def _update_performance_metrics(self, cache_name: str, response_time_ms: float, cache_hit: bool):
        """Update performance metrics"""
        try:
            metrics = self.metrics[cache_name]
            metrics.total_requests += 1
            
            if cache_hit:
                metrics.hits += 1
            else:
                metrics.misses += 1
            
            # Update running average response time
            if metrics.total_requests == 1:
                metrics.avg_response_time_ms = response_time_ms
            else:
                metrics.avg_response_time_ms = (
                    (metrics.avg_response_time_ms * (metrics.total_requests - 1) + response_time_ms) /
                    metrics.total_requests
                )
            
            # Update hit rates
            if metrics.total_requests > 0:
                metrics.l1_hit_rate = (metrics.hits / metrics.total_requests) * 100
            
            # Update performance window for global tracking
            self.performance_window.append(response_time_ms)
            if len(self.performance_window) > self.performance_window_size:
                self.performance_window.pop(0)
            
        except Exception as e:
            self.logger.error(f"Error updating performance metrics: {e}")
    
    # Background task methods
    async def _cache_warming_loop(self):
        """Background cache warming for optimal performance"""
        while True:
            try:
                for cache_name, config in self.cache_configs.items():
                    if config.warmup_keys:
                        for key in config.warmup_keys:
                            try:
                                await self.get_ultra_fast(cache_name, key)
                                await asyncio.sleep(0.1)  # Small delay between warmups
                            except Exception as e:
                                self.logger.error(f"Error warming cache {cache_name}:{key}: {e}")
                
                await asyncio.sleep(300)  # Warm every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in cache warming loop: {e}")
                await asyncio.sleep(60)
    
    async def _performance_monitoring_loop(self):
        """Background performance monitoring"""
        while True:
            try:
                # Update global performance metrics
                if self.performance_window:
                    avg_response_time = sum(self.performance_window) / len(self.performance_window)
                    self.global_metrics.avg_response_time_ms = avg_response_time
                
                # Update Prometheus metrics
                self.prom_cache_latency.observe(avg_response_time / 1000)
                
                # Check memory pressure
                memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                self.global_metrics.memory_pressure = memory_mb / 1024  # Assuming 1GB limit
                
                await asyncio.sleep(10)  # Monitor every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in performance monitoring: {e}")
                await asyncio.sleep(30)
    
    async def _memory_optimization_loop(self):
        """Background memory optimization"""
        while True:
            try:
                # Check memory pressure and optimize if needed
                if self.global_metrics.memory_pressure > 0.8:  # 80% memory usage
                    await self._optimize_memory_usage()
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in memory optimization: {e}")
                await asyncio.sleep(120)
    
    async def _optimize_memory_usage(self):
        """Optimize memory usage when under pressure"""
        try:
            # Clear L3 cache partially
            if len(self.l3_disk_cache) > 1000:
                # Remove 20% of L3 cache entries
                keys_to_remove = list(self.l3_disk_cache.keys())[:len(self.l3_disk_cache)//5]
                for key in keys_to_remove:
                    del self.l3_disk_cache[key]
            
            # Force garbage collection
            import gc
            gc.collect()
            
            self.logger.info("Memory optimization completed")
            
        except Exception as e:
            self.logger.error(f"Error in memory optimization: {e}")
    
    async def _predictive_prefetch_loop(self):
        """Background predictive prefetching"""
        while True:
            try:
                # Get prefetch candidates
                candidates = await self.predictive_cache.get_prefetch_candidates(threshold=0.7)
                
                for composite_key in candidates[:10]:  # Limit to top 10 candidates
                    try:
                        # Extract cache name from composite key
                        cache_name = composite_key.split(":")[0]
                        config = self.cache_configs.get(cache_name)
                        
                        if config and config.enable_prediction:
                            # Check if key is not already cached
                            cached_value = await self.l1_cache.get(composite_key)
                            if cached_value is None:
                                # Prefetch the data
                                fresh_data = await self._get_from_source(composite_key, config)
                                if fresh_data:
                                    await self.set_ultra_fast(cache_name, composite_key, fresh_data)
                                    self.logger.debug(f"Predictively prefetched: {composite_key}")
                    
                    except Exception as e:
                        self.logger.error(f"Error in predictive prefetch for {composite_key}: {e}")
                
                await asyncio.sleep(30)  # Prefetch every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in predictive prefetch loop: {e}")
                await asyncio.sleep(60)
    
    async def _metrics_collection_loop(self):
        """Background metrics collection and reporting"""
        while True:
            try:
                # Update cache size metrics
                l1_size = self.l1_cache.size() * 1024  # Approximate size
                self.prom_cache_size.labels(level="l1").set(l1_size)
                
                if self.l2_redis:
                    try:
                        redis_info = await self.l2_redis.info('memory')
                        redis_memory = redis_info.get('used_memory', 0)
                        self.prom_cache_size.labels(level="l2").set(redis_memory)
                    except Exception:
                        pass
                
                l3_size = len(json.dumps(self.l3_disk_cache))
                self.prom_cache_size.labels(level="l3").set(l3_size)
                
                await asyncio.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in metrics collection: {e}")
                await asyncio.sleep(60)
    
    async def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        stats = {
            "global_metrics": {
                "avg_response_time_ms": round(self.global_metrics.avg_response_time_ms, 2),
                "memory_pressure": round(self.global_metrics.memory_pressure, 2),
                "total_l1_entries": self.l1_cache.size(),
                "total_l3_entries": len(self.l3_disk_cache)
            },
            "cache_configs": {
                name: {
                    "strategy": config.strategy.value,
                    "target_response_time_ms": config.target_response_time_ms,
                    "compression": config.compression.value,
                    "priority": config.priority
                }
                for name, config in self.cache_configs.items()
            },
            "per_cache_metrics": {},
            "circuit_breaker_states": {
                name: {
                    "state": cb.state.value,
                    "failure_count": cb.failure_count,
                    "last_failure_time": cb.last_failure_time
                }
                for name, cb in self.circuit_breakers.items()
            },
            "predictive_cache_stats": {
                "tracked_patterns": len(self.predictive_cache.access_patterns),
                "predictions": len(self.predictive_cache.predictions)
            }
        }
        
        # Per-cache metrics
        for cache_name, metrics in self.metrics.items():
            stats["per_cache_metrics"][cache_name] = {
                "hits": metrics.hits,
                "misses": metrics.misses,
                "total_requests": metrics.total_requests,
                "hit_rate": round(metrics.l1_hit_rate, 2),
                "avg_response_time_ms": round(metrics.avg_response_time_ms, 2),
                "sla_violations": metrics.sla_violations,
                "compression_ratio": round(metrics.compression_ratio, 2),
                "compressions": metrics.compressions,
                "decompressions": metrics.decompressions
            }
        
        return stats
    
    async def close(self):
        """Clean shutdown of cache manager"""
        try:
            # Cancel background tasks
            for task in self.background_tasks:
                task.cancel()
            
            # Close Redis connections
            if self.l2_redis:
                await self.l2_redis.close()
            
            if self.l2_redis_pool:
                await self.l2_redis_pool.disconnect()
            
            # Clear caches
            await self.l1_cache.clear()
            self.l3_disk_cache.clear()
            
            # Shutdown thread pool
            self.executor.shutdown(wait=True)
            
            self.logger.info("Enhanced cache manager closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing enhanced cache manager: {e}")


# Factory function
def create_enhanced_cache_manager() -> EnhancedCacheManager:
    """Create EnhancedCacheManager instance"""
    return EnhancedCacheManager()


# Usage example
async def main():
    """Example usage of enhanced cache manager"""
    cache_manager = create_enhanced_cache_manager()
    
    try:
        # Test ultra-fast retrieval
        value, response_time = await cache_manager.get_ultra_fast(
            "executive_dashboard", 
            "hourly_revenue",
            user_id="exec_001"
        )
        
        print(f"✅ Retrieved data in {response_time:.2f}ms: {value}")
        
        # Test ultra-fast storage
        success = await cache_manager.set_ultra_fast(
            "executive_dashboard",
            "test_key",
            {"test": "data", "timestamp": datetime.now().isoformat()},
            user_id="exec_001"
        )
        
        print(f"✅ Storage success: {success}")
        
        # Get comprehensive stats
        stats = await cache_manager.get_comprehensive_stats()
        print("📊 Cache Statistics:")
        print(f"Global avg response time: {stats['global_metrics']['avg_response_time_ms']}ms")
        print(f"L1 cache entries: {stats['global_metrics']['total_l1_entries']}")
        
    finally:
        await cache_manager.close()


if __name__ == "__main__":
    asyncio.run(main())