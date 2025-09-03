"""Advanced FastAPI Performance Optimizer
Enterprise-grade performance optimizations for high-throughput scenarios
"""

from __future__ import annotations

import asyncio
import gc
import os
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import lru_cache, wraps
from threading import RLock
from typing import Any

import orjson
import psutil
import redis.asyncio as redis
import uvloop
from fastapi import FastAPI, Request, Response
from fastapi.responses import ORJSONResponse
from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool, QueuePool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from core.config import get_settings
from core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Performance metrics
REQUEST_COUNT = Counter(
    "fastapi_requests_total", "Total requests", ["method", "endpoint", "status"]
)
REQUEST_DURATION = Histogram(
    "fastapi_request_duration_seconds", "Request duration", ["method", "endpoint"]
)
ACTIVE_CONNECTIONS = Gauge("fastapi_active_connections", "Active connections")
MEMORY_USAGE = Gauge("fastapi_memory_usage_bytes", "Memory usage")
CACHE_HITS = Counter("fastapi_cache_hits_total", "Cache hits")
CACHE_MISSES = Counter("fastapi_cache_misses_total", "Cache misses")
DB_POOL_SIZE = Gauge("fastapi_db_pool_size", "Database pool size")
DB_CHECKED_OUT = Gauge("fastapi_db_checked_out", "Checked out database connections")


@dataclass
class PerformanceMetrics:
    """Performance metrics tracking"""

    total_requests: int = 0
    avg_response_time: float = 0.0
    p95_response_time: float = 0.0
    p99_response_time: float = 0.0
    active_connections: int = 0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    gc_collections: dict[str, int] = field(
        default_factory=lambda: {"gen0": 0, "gen1": 0, "gen2": 0}
    )
    cache_hit_rate: float = 0.0
    concurrent_requests: int = 0
    last_updated: float = field(default_factory=time.time)


class AsyncConnectionPool:
    """Advanced connection pool with adaptive sizing"""

    def __init__(self, min_connections: int = 10, max_connections: int = 100):
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connections: list[Any] = []
        self.in_use: set = set()
        self.lock = RLock()
        self.total_created = 0
        self.total_reused = 0

    async def get_connection(self) -> Any:
        """Get connection from pool with adaptive scaling"""
        with self.lock:
            if self.connections:
                conn = self.connections.pop()
                self.in_use.add(id(conn))
                self.total_reused += 1
                return conn

            if len(self.in_use) < self.max_connections:
                conn = await self._create_connection()
                self.in_use.add(id(conn))
                self.total_created += 1
                return conn

            raise RuntimeError("Connection pool exhausted")

    async def return_connection(self, conn: Any):
        """Return connection to pool"""
        with self.lock:
            conn_id = id(conn)
            if conn_id in self.in_use:
                self.in_use.remove(conn_id)

                # Only keep up to max_connections in the pool
                if len(self.connections) < self.max_connections:
                    self.connections.append(conn)
                else:
                    await self._close_connection(conn)

    async def _create_connection(self) -> Any:
        """Create new connection - override in subclasses"""
        return object()  # Placeholder

    async def _close_connection(self, conn: Any):
        """Close connection - override in subclasses"""
        pass

    @property
    def pool_stats(self) -> dict[str, Any]:
        """Get connection pool statistics"""
        return {
            "available_connections": len(self.connections),
            "in_use_connections": len(self.in_use),
            "total_created": self.total_created,
            "total_reused": self.total_reused,
            "reuse_rate": self.total_reused / max(1, self.total_created + self.total_reused),
        }


class MemoryOptimizer:
    """Advanced memory optimization and garbage collection tuning"""

    def __init__(self):
        self.gc_stats = {"gen0": 0, "gen1": 0, "gen2": 0}
        self.last_gc_time = time.time()
        self.gc_threshold = (700, 10, 10)  # Optimized GC thresholds

        # Configure garbage collection
        self._configure_gc()

    def _configure_gc(self):
        """Configure garbage collection for optimal performance"""
        # Set custom GC thresholds for high-throughput applications
        gc.set_threshold(*self.gc_threshold)

        # Enable GC debugging in development
        if settings.environment.value == "development":
            gc.set_debug(gc.DEBUG_STATS)

    def force_gc(self) -> dict[str, Any]:
        """Force garbage collection and return stats"""
        start_time = time.time()

        # Collect each generation
        collected = {"gen0": gc.collect(0), "gen1": gc.collect(1), "gen2": gc.collect(2)}

        duration = time.time() - start_time
        self.last_gc_time = time.time()

        # Update stats
        for gen, count in collected.items():
            self.gc_stats[gen] += count

        return {
            "collected": collected,
            "duration_ms": duration * 1000,
            "total_stats": self.gc_stats,
            "memory_usage_mb": psutil.Process().memory_info().rss / 1024 / 1024,
        }

    async def monitor_memory(self) -> dict[str, Any]:
        """Monitor memory usage and trigger GC if needed"""
        process = psutil.Process()
        memory_info = process.memory_info()

        memory_usage_mb = memory_info.rss / 1024 / 1024
        memory_percent = process.memory_percent()

        # Trigger GC if memory usage is high
        if memory_percent > 80 or (time.time() - self.last_gc_time) > 300:  # 5 minutes
            gc_stats = self.force_gc()
            logger.info(f"Automatic GC triggered - freed {gc_stats['collected']} objects")

        return {
            "memory_usage_mb": memory_usage_mb,
            "memory_percent": memory_percent,
            "gc_stats": self.gc_stats,
        }


class ResponseTimeTracker:
    """Track response times with percentile calculations"""

    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.response_times: list[float] = []
        self.lock = RLock()

    def add_response_time(self, response_time: float):
        """Add response time to tracking window"""
        with self.lock:
            self.response_times.append(response_time)

            # Keep only the last window_size entries
            if len(self.response_times) > self.window_size:
                self.response_times = self.response_times[-self.window_size :]

    def get_percentiles(self) -> dict[str, float]:
        """Calculate response time percentiles"""
        with self.lock:
            if not self.response_times:
                return {"avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}

            sorted_times = sorted(self.response_times)
            length = len(sorted_times)

            return {
                "avg": sum(sorted_times) / length,
                "p50": sorted_times[int(length * 0.5)],
                "p95": sorted_times[int(length * 0.95)],
                "p99": sorted_times[int(length * 0.99)],
                "min": min(sorted_times),
                "max": max(sorted_times),
                "samples": length,
            }


class AdvancedPerformanceMiddleware(BaseHTTPMiddleware):
    """Advanced performance monitoring and optimization middleware"""

    def __init__(self, app):
        super().__init__(app)
        self.memory_optimizer = MemoryOptimizer()
        self.response_tracker = ResponseTimeTracker()
        self.metrics = PerformanceMetrics()
        self.concurrent_requests = 0
        self.request_lock = RLock()

        # Performance thresholds
        self.slow_request_threshold = 1.0  # seconds
        self.memory_warning_threshold = 1000  # MB

        # Start background monitoring
        asyncio.create_task(self._background_monitoring())

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request with performance monitoring"""
        start_time = time.time()

        with self.request_lock:
            self.concurrent_requests += 1
            self.metrics.concurrent_requests = self.concurrent_requests

        try:
            # Add performance headers to request context
            request.state.start_time = start_time
            request.state.request_id = f"req-{int(start_time * 1000000)}"

            response = await call_next(request)

            # Calculate response time
            response_time = time.time() - start_time

            # Update metrics
            self._update_metrics(response_time, response.status_code)

            # Add performance headers
            response.headers["x-response-time"] = f"{response_time:.4f}"
            response.headers["x-request-id"] = request.state.request_id
            response.headers["x-concurrent-requests"] = str(self.concurrent_requests)

            # Log slow requests
            if response_time > self.slow_request_threshold:
                logger.warning(
                    f"Slow request detected: {request.method} {request.url.path} - {response_time:.4f}s",
                    extra={
                        "request_id": request.state.request_id,
                        "response_time": response_time,
                        "status_code": response.status_code,
                        "path": str(request.url.path),
                    },
                )

            return response

        except Exception as e:
            response_time = time.time() - start_time
            self._update_metrics(response_time, 500)

            logger.error(
                f"Request failed: {request.method} {request.url.path} - {str(e)}",
                extra={
                    "request_id": getattr(request.state, "request_id", "unknown"),
                    "response_time": response_time,
                    "error": str(e),
                },
            )
            raise e

        finally:
            with self.request_lock:
                self.concurrent_requests -= 1

    def _update_metrics(self, response_time: float, status_code: int):
        """Update performance metrics"""
        self.metrics.total_requests += 1
        self.response_tracker.add_response_time(response_time)

        # Update average response time (exponential moving average)
        alpha = 0.1
        self.metrics.avg_response_time = (
            alpha * response_time + (1 - alpha) * self.metrics.avg_response_time
        )

        # Update percentiles periodically
        if self.metrics.total_requests % 100 == 0:
            percentiles = self.response_tracker.get_percentiles()
            self.metrics.p95_response_time = percentiles["p95"]
            self.metrics.p99_response_time = percentiles["p99"]

    async def _background_monitoring(self):
        """Background task for system monitoring"""
        while True:
            try:
                # Monitor memory
                memory_stats = await self.memory_optimizer.monitor_memory()
                self.metrics.memory_usage_mb = memory_stats["memory_usage_mb"]
                self.metrics.gc_collections = self.memory_optimizer.gc_stats.copy()

                # Monitor CPU
                self.metrics.cpu_usage_percent = psutil.cpu_percent(interval=None)

                # Update timestamp
                self.metrics.last_updated = time.time()

                # Warn about high memory usage
                if self.metrics.memory_usage_mb > self.memory_warning_threshold:
                    logger.warning(
                        f"High memory usage detected: {self.metrics.memory_usage_mb:.2f}MB",
                        extra={"memory_mb": self.metrics.memory_usage_mb},
                    )

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                logger.error(f"Background monitoring error: {e}")
                await asyncio.sleep(60)  # Wait longer on error

    async def get_performance_metrics(self) -> dict[str, Any]:
        """Get comprehensive performance metrics"""
        percentiles = self.response_tracker.get_percentiles()

        return {
            "requests": {
                "total": self.metrics.total_requests,
                "concurrent": self.metrics.concurrent_requests,
                "response_times": {
                    "avg": percentiles["avg"],
                    "p50": percentiles["p50"],
                    "p95": percentiles["p95"],
                    "p99": percentiles["p99"],
                    "min": percentiles["min"],
                    "max": percentiles["max"],
                },
            },
            "system": {
                "memory_usage_mb": self.metrics.memory_usage_mb,
                "cpu_usage_percent": self.metrics.cpu_usage_percent,
                "gc_collections": self.metrics.gc_collections,
            },
            "timestamp": self.metrics.last_updated,
        }


class AsyncTaskOptimizer:
    """Optimize async task execution and concurrency"""

    def __init__(self, max_concurrent_tasks: int = 100):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.task_queue = asyncio.Queue(maxsize=1000)
        self.active_tasks = 0

    async def execute_with_concurrency_limit(self, coro):
        """Execute coroutine with concurrency limiting"""
        async with self.semaphore:
            self.active_tasks += 1
            try:
                return await coro
            finally:
                self.active_tasks -= 1

    def create_optimized_task(self, coro, name: str = None) -> asyncio.Task:
        """Create optimized task with monitoring"""
        task = asyncio.create_task(self.execute_with_concurrency_limit(coro), name=name)
        return task

    @property
    def task_stats(self) -> dict[str, Any]:
        """Get task execution statistics"""
        return {
            "active_tasks": self.active_tasks,
            "max_concurrent": self.max_concurrent_tasks,
            "queue_size": self.task_queue.qsize(),
        }


class FastAPIPerformanceOptimizer:
    """Main FastAPI performance optimization class"""

    def __init__(self, app: FastAPI):
        self.app = app
        self.performance_middleware = AdvancedPerformanceMiddleware(app)
        self.task_optimizer = AsyncTaskOptimizer()
        self.connection_pool = AsyncConnectionPool()

        # Configure event loop for optimal performance
        self._configure_event_loop()

        # Add performance middleware
        app.add_middleware(AdvancedPerformanceMiddleware)

    def _configure_event_loop(self):
        """Configure event loop for optimal performance"""
        # Use uvloop if available (Unix-like systems)
        if hasattr(uvloop, "install"):
            try:
                uvloop.install()
                logger.info("Uvloop event loop installed for better performance")
            except Exception as e:
                logger.warning(f"Failed to install uvloop: {e}")

    def optimize_json_serialization(self):
        """Configure optimal JSON serialization"""
        # Already using ORJSON in main.py, ensure it's properly configured
        logger.info("JSON serialization optimized with ORJSON")

    def configure_worker_processes(self) -> dict[str, Any]:
        """Configure optimal worker process settings"""
        cpu_count = os.cpu_count() or 4

        # Optimal worker count for I/O bound applications
        optimal_workers = min(cpu_count * 2 + 1, 16)

        config = {
            "workers": optimal_workers,
            "worker_class": "uvicorn.workers.UvicornWorker",
            "worker_connections": 1000,
            "max_requests": 10000,  # Restart workers after handling this many requests
            "max_requests_jitter": 1000,
            "preload_app": True,
            "timeout": 120,
            "keepalive": 5,
        }

        logger.info(f"Optimal worker configuration: {config}")
        return config

    async def get_comprehensive_metrics(self) -> dict[str, Any]:
        """Get comprehensive performance metrics"""
        performance_metrics = await self.performance_middleware.get_performance_metrics()

        return {
            "performance": performance_metrics,
            "tasks": self.task_optimizer.task_stats,
            "connections": self.connection_pool.pool_stats,
            "system": {
                "cpu_count": os.cpu_count(),
                "memory_total_mb": psutil.virtual_memory().total / 1024 / 1024,
                "memory_available_mb": psutil.virtual_memory().available / 1024 / 1024,
                "disk_usage": psutil.disk_usage("/").percent
                if os.name != "nt"
                else psutil.disk_usage("C:\\").percent,
            },
        }

    @asynccontextmanager
    async def performance_monitoring_context(self):
        """Context manager for performance monitoring"""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss

        try:
            yield
        finally:
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss

            duration = end_time - start_time
            memory_diff = (end_memory - start_memory) / 1024 / 1024

            logger.info(
                f"Performance context completed in {duration:.4f}s, memory change: {memory_diff:+.2f}MB"
            )


# Performance decorator for async functions
def performance_monitor(func: Callable) -> Callable:
    """Decorator to monitor function performance"""

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start_time

            logger.debug(
                f"Function {func.__name__} completed in {duration:.4f}s",
                extra={"function": func.__name__, "duration": duration},
            )
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"Function {func.__name__} failed after {duration:.4f}s: {e}",
                extra={"function": func.__name__, "duration": duration, "error": str(e)},
            )
            raise

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time

            logger.debug(
                f"Function {func.__name__} completed in {duration:.4f}s",
                extra={"function": func.__name__, "duration": duration},
            )
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"Function {func.__name__} failed after {duration:.4f}s: {e}",
                extra={"function": func.__name__, "duration": duration, "error": str(e)},
            )
            raise

    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper


# Cache decorator with performance optimization
def performance_cache(maxsize: int = 1000, typed: bool = False):
    """High-performance caching decorator"""

    def decorator(func: Callable) -> Callable:
        cached_func = lru_cache(maxsize=maxsize, typed=typed)(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Add cache hit/miss tracking
            start_time = time.time()
            result = cached_func(*args, **kwargs)
            time.time() - start_time

            # Log cache statistics periodically
            if hasattr(cached_func, "cache_info"):
                cache_info = cached_func.cache_info()
                if cache_info.misses % 100 == 0:  # Log every 100 misses
                    hit_rate = cache_info.hits / (cache_info.hits + cache_info.misses) * 100
                    logger.debug(
                        f"Cache stats for {func.__name__}: {hit_rate:.1f}% hit rate, {cache_info.currsize}/{cache_info.maxsize} entries"
                    )

            return result

        return wrapper

    return decorator


# Global performance optimizer instance
_performance_optimizer: FastAPIPerformanceOptimizer | None = None


def get_performance_optimizer(app: FastAPI) -> FastAPIPerformanceOptimizer:
    """Get or create global performance optimizer instance"""
    global _performance_optimizer
    if _performance_optimizer is None:
        _performance_optimizer = FastAPIPerformanceOptimizer(app)
    return _performance_optimizer


# Performance utilities
class PerformanceUtils:
    """Utility functions for performance optimization"""

    @staticmethod
    async def warmup_application(app: FastAPI, warmup_requests: int = 10):
        """Warm up application with dummy requests"""
        import httpx

        logger.info(f"Starting application warmup with {warmup_requests} requests")

        async with httpx.AsyncClient() as client:
            warmup_start = time.time()

            for i in range(warmup_requests):
                try:
                    # Make warmup requests to common endpoints
                    await client.get("http://localhost:8000/health")
                    if i % 10 == 0:
                        logger.info(f"Warmup progress: {i}/{warmup_requests}")
                except Exception as e:
                    logger.warning(f"Warmup request {i} failed: {e}")

            warmup_duration = time.time() - warmup_start
            logger.info(f"Application warmup completed in {warmup_duration:.2f}s")

    @staticmethod
    def get_system_info() -> dict[str, Any]:
        """Get comprehensive system information"""
        return {
            "python_version": os.sys.version,
            "platform": os.name,
            "cpu_count": os.cpu_count(),
            "memory_total_gb": psutil.virtual_memory().total / (1024**3),
            "disk_total_gb": psutil.disk_usage("/").total / (1024**3)
            if os.name != "nt"
            else psutil.disk_usage("C:\\").total / (1024**3),
            "uvloop_available": hasattr(uvloop, "install"),
            "gc_thresholds": gc.get_threshold(),
            "gc_counts": gc.get_counts(),
        }


class EnterpriseRedisPool:
    """Enterprise Redis connection pool with advanced features"""

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.pool: redis.ConnectionPool | None = None
        self.client: redis.Redis | None = None
        self._lock = threading.RLock()

    async def initialize(self):
        """Initialize Redis connection pool"""
        try:
            self.pool = redis.ConnectionPool.from_url(
                self.redis_url,
                max_connections=100,  # Increased pool size for enterprise
                retry_on_timeout=True,
                socket_keepalive=True,
                socket_keepalive_options={
                    "TCP_KEEPIDLE": 1,
                    "TCP_KEEPINTVL": 3,
                    "TCP_KEEPCNT": 5,
                },
                health_check_interval=30,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_error=[redis.BusyLoadingError, redis.ConnectionError, redis.TimeoutError],
            )

            self.client = redis.Redis(
                connection_pool=self.pool,
                decode_responses=False,  # Handle binary data
                retry_on_error=[redis.BusyLoadingError, redis.ConnectionError],
                health_check_interval=30,
            )

            # Test connection
            await self.client.ping()
            logger.info("Enterprise Redis pool initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Redis pool: {e}")
            raise

    async def get_client(self) -> redis.Redis:
        """Get Redis client instance"""
        if not self.client:
            await self.initialize()
        return self.client

    async def cached_get(self, key: str, deserializer=orjson.loads) -> Any:
        """Get cached value with deserialization"""
        client = await self.get_client()
        try:
            value = await client.get(key)
            if value:
                CACHE_HITS.inc()
                return deserializer(value) if deserializer else value
            else:
                CACHE_MISSES.inc()
                return None
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            CACHE_MISSES.inc()
            return None

    async def cached_set(self, key: str, value: Any, ttl: int = 300, serializer=orjson.dumps):
        """Set cached value with serialization"""
        client = await self.get_client()
        try:
            serialized_value = serializer(value) if serializer else value
            await client.setex(key, ttl, serialized_value)
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")

    async def get_stats(self) -> dict[str, Any]:
        """Get Redis connection pool statistics"""
        if not self.pool:
            return {"status": "not_initialized"}

        try:
            client = await self.get_client()
            info = await client.info()

            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "pool_created_connections": len(self.pool._created_connections)
                if hasattr(self.pool, "_created_connections")
                else 0,
                "pool_available_connections": len(self.pool._available_connections)
                if hasattr(self.pool, "_available_connections")
                else 0,
            }
        except Exception as e:
            logger.error(f"Failed to get Redis stats: {e}")
            return {"status": "error", "error": str(e)}

    async def cleanup(self):
        """Cleanup Redis connections"""
        if self.client:
            await self.client.close()
        if self.pool:
            await self.pool.disconnect()


class EnterpriseDatabasePool:
    """Enterprise database connection pool with advanced features"""

    def __init__(self):
        self.engine: AsyncEngine | None = None
        self.sessionmaker: sessionmaker | None = None
        self._lock = threading.RLock()

    async def initialize(self):
        """Initialize database engine with optimal settings"""
        try:
            database_url = settings.get_database_url(async_mode=True)

            if "sqlite" in database_url:
                # SQLite optimization for development
                self.engine = create_async_engine(
                    database_url,
                    poolclass=NullPool,  # SQLite doesn't benefit from pooling
                    connect_args={
                        "check_same_thread": False,
                        "timeout": 20,
                        "isolation_level": None,
                    },
                    echo=False,
                    future=True,
                    query_cache_size=1200,
                )
            else:
                # PostgreSQL optimization for production
                self.engine = create_async_engine(
                    database_url,
                    poolclass=QueuePool,
                    pool_size=25,  # Increased base pool size
                    max_overflow=50,  # Increased overflow
                    pool_recycle=3600,  # Recycle every hour
                    pool_pre_ping=True,  # Health check connections
                    pool_reset_on_return="rollback",
                    connect_args={
                        "server_settings": {
                            "application_name": "fastapi_enterprise",
                            "jit": "off",  # Disable JIT for faster connections
                            "shared_preload_libraries": "pg_stat_statements",
                            "track_activity_query_size": "2048",
                        },
                        "command_timeout": 60,
                        "prepared_statement_cache_size": 100,
                    },
                    echo=False,
                    future=True,
                    query_cache_size=1200,
                )

            # Create async session factory
            self.sessionmaker = sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=True,
                autocommit=False,
            )

            # Test connection
            async with self.engine.begin() as conn:
                await conn.execute("SELECT 1")

            logger.info("Enterprise database pool initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    async def get_session(self) -> AsyncSession:
        """Get database session from pool"""
        if not self.sessionmaker:
            await self.initialize()
        return self.sessionmaker()

    async def get_engine(self) -> AsyncEngine:
        """Get database engine"""
        if not self.engine:
            await self.initialize()
        return self.engine

    async def get_stats(self) -> dict[str, Any]:
        """Get database pool statistics"""
        if not self.engine:
            return {"status": "not_initialized"}

        try:
            pool = self.engine.pool
            return {
                "pool_size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "invalid": pool.invalid(),
                "connection_records": len(pool._pool.queue) if hasattr(pool._pool, "queue") else 0,
            }
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {"status": "error", "error": str(e)}

    async def cleanup(self):
        """Cleanup database connections"""
        if self.engine:
            await self.engine.dispose()


class OptimizedORJSONResponse(ORJSONResponse):
    """Highly optimized JSON response class"""

    def render(self, content: Any) -> bytes:
        return orjson.dumps(
            content,
            option=(
                orjson.OPT_NON_STR_KEYS
                | orjson.OPT_SERIALIZE_NUMPY
                | orjson.OPT_UTC_Z
                | orjson.OPT_OMIT_MICROSECONDS
                | orjson.OPT_SERIALIZE_DATACLASS
            ),
        )


class EnterpriseCompressionMiddleware(BaseHTTPMiddleware):
    """Advanced compression middleware with multiple algorithms"""

    def __init__(self, app: ASGIApp, minimum_size: int = 1000, compression_level: int = 6):
        super().__init__(app)
        self.minimum_size = minimum_size
        self.compression_level = compression_level

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Skip compression for certain content types
        content_type = response.headers.get("content-type", "")
        if any(
            skip_type in content_type
            for skip_type in ["image/", "video/", "application/zip", "application/gzip"]
        ):
            return response

        # Check if compression should be applied
        accept_encoding = request.headers.get("accept-encoding", "")
        if (
            hasattr(response, "body")
            and len(response.body) > self.minimum_size
            and not response.headers.get("content-encoding")
        ):
            if "br" in accept_encoding and self._has_brotli():
                # Use Brotli compression (best compression ratio)
                import brotli

                compressed_body = brotli.compress(
                    response.body, quality=self.compression_level, mode=brotli.MODE_TEXT
                )
                if len(compressed_body) < len(response.body) * 0.8:
                    response.headers["content-encoding"] = "br"
                    response.headers["content-length"] = str(len(compressed_body))
                    response.body = compressed_body

            elif "gzip" in accept_encoding:
                # Use Gzip compression (widely supported)
                import gzip

                compressed_body = gzip.compress(response.body, compresslevel=self.compression_level)
                if len(compressed_body) < len(response.body) * 0.85:
                    response.headers["content-encoding"] = "gzip"
                    response.headers["content-length"] = str(len(compressed_body))
                    response.body = compressed_body

        return response

    def _has_brotli(self) -> bool:
        """Check if Brotli compression is available"""
        try:
            import brotli

            return True
        except ImportError:
            return False


def create_enterprise_fastapi_optimizer() -> tuple[FastAPI, dict[str, Any]]:
    """Create enterprise-optimized FastAPI application"""

    # Initialize resource pools
    redis_pool = EnterpriseRedisPool(getattr(settings, "redis_url", "redis://localhost:6379/0"))
    db_pool = EnterpriseDatabasePool()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        try:
            # Set uvloop for better performance
            if hasattr(uvloop, "install") and os.name != "nt":
                uvloop.install()
                logger.info("uvloop installed for optimal async performance")

            # Initialize pools
            await redis_pool.initialize()
            await db_pool.initialize()

            # Store in app state for access in endpoints
            app.state.redis_pool = redis_pool
            app.state.db_pool = db_pool

            logger.info("Enterprise FastAPI optimization initialized")
            yield

        except Exception as e:
            logger.error(f"Failed to initialize enterprise optimizer: {e}")
            raise
        finally:
            # Shutdown
            await redis_pool.cleanup()
            await db_pool.cleanup()
            logger.info("Enterprise FastAPI optimization cleanup completed")

    # Create optimized FastAPI app
    app = FastAPI(
        title="Enterprise Data Engineering API",
        version="1.0.0",
        description="High-performance enterprise API with advanced optimization",
        default_response_class=OptimizedORJSONResponse,
        lifespan=lifespan,
        generate_unique_id_function=lambda route: f"{route.tags[0]}-{route.name}"
        if route.tags
        else route.name,
    )

    # Add enterprise middleware
    app.add_middleware(EnterpriseCompressionMiddleware)

    # Performance monitoring endpoint
    @app.get("/performance/metrics")
    async def get_performance_metrics():
        """Get comprehensive performance metrics"""
        redis_stats = await redis_pool.get_stats()
        db_stats = await db_pool.get_stats()

        return {
            "timestamp": time.time(),
            "redis": redis_stats,
            "database": db_stats,
            "system": PerformanceUtils.get_system_info(),
        }

    optimizer_config = {
        "redis_pool": redis_pool,
        "db_pool": db_pool,
        "optimization_level": "enterprise",
        "features": [
            "connection_pooling",
            "advanced_compression",
            "performance_monitoring",
            "resource_optimization",
            "enterprise_caching",
        ],
    }

    return app, optimizer_config
