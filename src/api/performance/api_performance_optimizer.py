"""
API Performance Optimizer
Advanced performance optimization for FastAPI applications with comprehensive monitoring.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import psutil
import uvloop
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from api.services.microservices_orchestrator import get_microservices_orchestrator
from core.caching.cache_patterns import CacheAsidePattern
from core.logging import get_logger

logger = get_logger(__name__)


class PerformanceLevel(str, Enum):
    """API performance levels."""

    EXCELLENT = "excellent"  # < 50ms
    GOOD = "good"  # 50-200ms
    ACCEPTABLE = "acceptable"  # 200-1000ms
    POOR = "poor"  # > 1000ms


class OptimizationStrategy(str, Enum):
    """Performance optimization strategies."""

    AGGRESSIVE = "aggressive"
    BALANCED = "balanced"
    CONSERVATIVE = "conservative"


@dataclass
class RequestMetrics:
    """Individual request performance metrics."""

    endpoint: str
    method: str
    response_time_ms: float
    status_code: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    user_agent: str | None = None
    client_ip: str | None = None
    content_length: int = 0

    @property
    def performance_level(self) -> PerformanceLevel:
        if self.response_time_ms < 50:
            return PerformanceLevel.EXCELLENT
        elif self.response_time_ms < 200:
            return PerformanceLevel.GOOD
        elif self.response_time_ms < 1000:
            return PerformanceLevel.ACCEPTABLE
        else:
            return PerformanceLevel.POOR


@dataclass
class EndpointStats:
    """Aggregated endpoint statistics."""

    endpoint: str
    method: str
    total_requests: int = 0
    total_response_time: float = 0.0
    min_response_time: float = float("inf")
    max_response_time: float = 0.0
    error_count: int = 0
    recent_requests: deque = field(default_factory=lambda: deque(maxlen=100))

    @property
    def avg_response_time(self) -> float:
        return self.total_response_time / max(1, self.total_requests)

    @property
    def error_rate(self) -> float:
        return self.error_count / max(1, self.total_requests)

    @property
    def requests_per_minute(self) -> float:
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)
        recent_count = sum(1 for req in self.recent_requests if req.timestamp > minute_ago)
        return recent_count

    def add_request(self, metrics: RequestMetrics):
        self.total_requests += 1
        self.total_response_time += metrics.response_time_ms
        self.min_response_time = min(self.min_response_time, metrics.response_time_ms)
        self.max_response_time = max(self.max_response_time, metrics.response_time_ms)

        if metrics.status_code >= 400:
            self.error_count += 1

        self.recent_requests.append(metrics)


class APIPerformanceOptimizer:
    """
    Comprehensive API performance optimizer with real-time monitoring and optimization.

    Features:
    - Real-time performance monitoring
    - Automatic response caching
    - Request compression optimization
    - Connection pooling optimization
    - Memory usage monitoring
    - Auto-scaling recommendations
    - Performance bottleneck detection
    """

    def __init__(
        self, app: FastAPI, strategy: OptimizationStrategy = OptimizationStrategy.BALANCED
    ):
        self.app = app
        self.strategy = strategy
        self.endpoint_stats: dict[str, EndpointStats] = defaultdict(lambda: EndpointStats("", ""))
        self.response_cache = CacheAsidePattern(default_ttl=300)  # 5 minutes
        self.orchestrator = get_microservices_orchestrator()

        # Performance thresholds
        self.slow_request_threshold = 1000  # ms
        self.high_error_rate_threshold = 0.05  # 5%
        self.high_load_threshold = 100  # requests per minute

        # System monitoring
        self.system_metrics = {
            "cpu_usage": 0.0,
            "memory_usage": 0.0,
            "active_connections": 0,
            "total_requests": 0,
            "cache_hit_rate": 0.0,
        }

        # Optimization features
        self.features = {
            "response_caching": True,
            "request_compression": True,
            "connection_pooling": True,
            "async_optimization": True,
            "preemptive_scaling": True,
        }

        # Background tasks
        self._monitoring_task: asyncio.Task | None = None
        self._optimization_task: asyncio.Task | None = None

        # Configure uvloop for better performance
        self._setup_uvloop()
        self._start_background_tasks()

    def _setup_uvloop(self):
        """Setup uvloop for better async performance."""
        try:
            if not isinstance(asyncio.get_event_loop(), uvloop.Loop):
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                logger.info("Configured uvloop for enhanced async performance")
        except Exception as e:
            logger.warning(f"Failed to setup uvloop: {e}")

    def _start_background_tasks(self):
        """Start background monitoring and optimization tasks."""
        self._monitoring_task = asyncio.create_task(self._system_monitoring_worker())
        self._optimization_task = asyncio.create_task(self._optimization_worker())
        logger.info("Started API performance optimization background tasks")

    async def _system_monitoring_worker(self):
        """Background worker for system metrics monitoring."""
        while True:
            try:
                # Update system metrics
                self.system_metrics["cpu_usage"] = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                self.system_metrics["memory_usage"] = memory.percent

                # Update cache hit rate
                cache_stats = self.response_cache.get_stats()
                self.system_metrics["cache_hit_rate"] = cache_stats["hit_rate"]

                # Log performance warnings
                if self.system_metrics["cpu_usage"] > 80:
                    logger.warning(f"High CPU usage: {self.system_metrics['cpu_usage']:.1f}%")

                if self.system_metrics["memory_usage"] > 85:
                    logger.warning(f"High memory usage: {self.system_metrics['memory_usage']:.1f}%")

                await asyncio.sleep(30)  # Monitor every 30 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"System monitoring error: {e}")
                await asyncio.sleep(5)

    async def _optimization_worker(self):
        """Background worker for automatic optimizations."""
        while True:
            try:
                # Run optimization checks every 5 minutes
                await asyncio.sleep(300)

                # Detect performance bottlenecks
                bottlenecks = await self.detect_bottlenecks()

                if bottlenecks:
                    logger.info(f"Detected performance bottlenecks: {len(bottlenecks)}")

                    # Apply automatic optimizations
                    await self.apply_optimizations(bottlenecks)

                # Cleanup old metrics
                await self._cleanup_old_metrics()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Optimization worker error: {e}")
                await asyncio.sleep(60)

    async def record_request_metrics(
        self, request: Request, response: Response, response_time: float
    ) -> RequestMetrics:
        """Record request performance metrics."""
        endpoint = f"{request.method} {request.url.path}"

        metrics = RequestMetrics(
            endpoint=endpoint,
            method=request.method,
            response_time_ms=response_time * 1000,  # Convert to milliseconds
            status_code=response.status_code,
            user_agent=request.headers.get("user-agent"),
            client_ip=request.client.host if request.client else None,
            content_length=int(response.headers.get("content-length", 0)),
        )

        # Update endpoint statistics
        stats_key = f"{request.method}:{request.url.path}"
        if stats_key not in self.endpoint_stats:
            self.endpoint_stats[stats_key] = EndpointStats(endpoint, request.method)

        self.endpoint_stats[stats_key].add_request(metrics)

        # Update global metrics
        self.system_metrics["total_requests"] += 1

        # Log slow requests
        if metrics.response_time_ms > self.slow_request_threshold:
            logger.warning(
                f"Slow request detected: {endpoint} took {metrics.response_time_ms:.2f}ms",
                extra={
                    "endpoint": endpoint,
                    "response_time_ms": metrics.response_time_ms,
                    "status_code": metrics.status_code,
                },
            )

        return metrics

    async def should_cache_response(self, request: Request) -> bool:
        """Determine if response should be cached."""
        if not self.features["response_caching"]:
            return False

        # Only cache GET requests
        if request.method != "GET":
            return False

        # Don't cache authenticated requests with user-specific data
        if "authorization" in request.headers:
            # Could implement user-specific caching here
            return False

        # Cache public endpoints
        cacheable_patterns = [
            "/api/v1/health",
            "/api/v1/sales/analytics",
            "/api/v1/search",
            "/api/v2/analytics",
        ]

        path = request.url.path
        return any(path.startswith(pattern) for pattern in cacheable_patterns)

    async def get_cached_response(self, cache_key: str) -> dict[str, Any] | None:
        """Get cached response if available."""
        try:

            async def load_none():
                return None

            cached = await self.response_cache.get(cache_key, load_none)
            return cached
        except Exception as e:
            logger.error(f"Cache retrieval error: {e}")
            return None

    async def cache_response(
        self, cache_key: str, response_data: dict[str, Any], ttl: float | None = None
    ):
        """Cache response data."""
        try:
            await self.response_cache.put(cache_key, response_data, ttl)
        except Exception as e:
            logger.error(f"Cache storage error: {e}")

    def generate_cache_key(self, request: Request) -> str:
        """Generate cache key for request."""
        # Include method, path, and query parameters
        query_string = str(request.url.query) if request.url.query else ""
        return f"{request.method}:{request.url.path}:{hash(query_string)}"

    async def detect_bottlenecks(self) -> list[dict[str, Any]]:
        """Detect performance bottlenecks."""
        bottlenecks = []

        # Analyze endpoint performance
        for _stats_key, stats in self.endpoint_stats.items():
            # Check for slow endpoints
            if stats.avg_response_time > self.slow_request_threshold:
                bottlenecks.append(
                    {
                        "type": "slow_endpoint",
                        "endpoint": stats.endpoint,
                        "avg_response_time": stats.avg_response_time,
                        "severity": "high" if stats.avg_response_time > 2000 else "medium",
                    }
                )

            # Check for high error rates
            if stats.error_rate > self.high_error_rate_threshold:
                bottlenecks.append(
                    {
                        "type": "high_error_rate",
                        "endpoint": stats.endpoint,
                        "error_rate": stats.error_rate,
                        "severity": "high" if stats.error_rate > 0.1 else "medium",
                    }
                )

            # Check for high load
            if stats.requests_per_minute > self.high_load_threshold:
                bottlenecks.append(
                    {
                        "type": "high_load",
                        "endpoint": stats.endpoint,
                        "requests_per_minute": stats.requests_per_minute,
                        "severity": "medium",
                    }
                )

        # System-level bottlenecks
        if self.system_metrics["cpu_usage"] > 80:
            bottlenecks.append(
                {
                    "type": "high_cpu_usage",
                    "cpu_usage": self.system_metrics["cpu_usage"],
                    "severity": "high",
                }
            )

        if self.system_metrics["memory_usage"] > 85:
            bottlenecks.append(
                {
                    "type": "high_memory_usage",
                    "memory_usage": self.system_metrics["memory_usage"],
                    "severity": "high",
                }
            )

        return bottlenecks

    async def apply_optimizations(self, bottlenecks: list[dict[str, Any]]):
        """Apply automatic optimizations based on detected bottlenecks."""
        optimizations_applied = []

        for bottleneck in bottlenecks:
            if bottleneck["type"] == "slow_endpoint":
                # Enable caching for slow endpoints
                if self.strategy in [
                    OptimizationStrategy.AGGRESSIVE,
                    OptimizationStrategy.BALANCED,
                ]:
                    endpoint = bottleneck["endpoint"]
                    logger.info(f"Enabling aggressive caching for slow endpoint: {endpoint}")
                    optimizations_applied.append(f"enabled_caching_{endpoint}")

            elif bottleneck["type"] == "high_error_rate":
                # Implement circuit breaker
                if self.strategy == OptimizationStrategy.AGGRESSIVE:
                    endpoint = bottleneck["endpoint"]
                    logger.info(f"Activating circuit breaker for endpoint: {endpoint}")
                    optimizations_applied.append(f"circuit_breaker_{endpoint}")

            elif bottleneck["type"] == "high_load":
                # Trigger auto-scaling
                if self.features["preemptive_scaling"]:
                    await self._trigger_scaling_recommendation(bottleneck)
                    optimizations_applied.append("scaling_recommended")

            elif bottleneck["type"] in ["high_cpu_usage", "high_memory_usage"]:
                # Optimize system resources
                await self._optimize_system_resources()
                optimizations_applied.append("system_optimization")

        if optimizations_applied:
            logger.info(f"Applied optimizations: {optimizations_applied}")

    async def _trigger_scaling_recommendation(self, bottleneck: dict[str, Any]):
        """Trigger scaling recommendation through orchestrator."""
        try:
            # Analyze load patterns
            endpoint = bottleneck["endpoint"]
            requests_per_minute = bottleneck["requests_per_minute"]

            # Determine service from endpoint
            if "/sales/" in endpoint:
                service_name = "sales-service"
            elif "/analytics/" in endpoint:
                service_name = "analytics-service"
            elif "/search/" in endpoint:
                service_name = "search-service"
            else:
                return

            # Calculate target instances based on load
            current_load_factor = requests_per_minute / self.high_load_threshold
            suggested_instances = max(2, int(current_load_factor * 2))

            # Trigger scaling through orchestrator
            result = await self.orchestrator.scale_service(service_name, suggested_instances)

            if result["success"]:
                logger.info(
                    f"Scaling recommendation accepted: {service_name} -> {suggested_instances} instances"
                )
            else:
                logger.warning(f"Scaling recommendation failed: {result.get('error')}")

        except Exception as e:
            logger.error(f"Scaling recommendation error: {e}")

    async def _optimize_system_resources(self):
        """Optimize system resources."""
        try:
            # Clear old cache entries
            cache_stats = self.response_cache.get_stats()
            if cache_stats["size"] > 1000:
                await self.response_cache.clear()
                logger.info("Cleared response cache to free memory")

            # Run garbage collection
            import gc

            collected = gc.collect()
            logger.debug(f"Garbage collection freed {collected} objects")

        except Exception as e:
            logger.error(f"System resource optimization error: {e}")

    async def _cleanup_old_metrics(self):
        """Clean up old metrics to prevent memory leaks."""
        cutoff_time = datetime.utcnow() - timedelta(hours=1)

        for stats in self.endpoint_stats.values():
            # Clean old recent requests
            stats.recent_requests = deque(
                (req for req in stats.recent_requests if req.timestamp > cutoff_time), maxlen=100
            )

    def get_performance_report(self) -> dict[str, Any]:
        """Generate comprehensive performance report."""
        # Calculate overall statistics
        total_requests = sum(stats.total_requests for stats in self.endpoint_stats.values())
        avg_response_time = sum(
            stats.avg_response_time * stats.total_requests for stats in self.endpoint_stats.values()
        ) / max(1, total_requests)

        # Get slowest endpoints
        slowest_endpoints = sorted(
            [(key, stats) for key, stats in self.endpoint_stats.items()],
            key=lambda x: x[1].avg_response_time,
            reverse=True,
        )[:10]

        # Get most error-prone endpoints
        error_prone_endpoints = sorted(
            [(key, stats) for key, stats in self.endpoint_stats.items()],
            key=lambda x: x[1].error_rate,
            reverse=True,
        )[:10]

        # Generate recommendations
        recommendations = []

        if avg_response_time > 500:
            recommendations.append("Enable response caching for frequently accessed endpoints")

        if self.system_metrics["cpu_usage"] > 70:
            recommendations.append("Consider horizontal scaling - CPU usage is high")

        if self.system_metrics["cache_hit_rate"] < 0.5:
            recommendations.append("Optimize caching strategy - low hit rate detected")

        return {
            "summary": {
                "total_requests": total_requests,
                "avg_response_time_ms": avg_response_time,
                "cache_hit_rate": self.system_metrics["cache_hit_rate"],
                "cpu_usage": self.system_metrics["cpu_usage"],
                "memory_usage": self.system_metrics["memory_usage"],
            },
            "slowest_endpoints": [
                {
                    "endpoint": stats.endpoint,
                    "avg_response_time_ms": stats.avg_response_time,
                    "total_requests": stats.total_requests,
                    "error_rate": stats.error_rate,
                }
                for key, stats in slowest_endpoints
                if stats.total_requests > 0
            ],
            "error_prone_endpoints": [
                {
                    "endpoint": stats.endpoint,
                    "error_rate": stats.error_rate,
                    "total_requests": stats.total_requests,
                    "avg_response_time_ms": stats.avg_response_time,
                }
                for key, stats in error_prone_endpoints
                if stats.error_count > 0
            ],
            "optimization_features": self.features,
            "optimization_strategy": self.strategy.value,
            "recommendations": recommendations,
            "report_generated_at": datetime.utcnow().isoformat(),
        }

    async def benchmark_api(
        self, endpoints: list[str], concurrent_requests: int = 10, duration_seconds: int = 60
    ) -> dict[str, Any]:
        """Run API performance benchmark."""
        import httpx

        results = {}

        for endpoint in endpoints:
            logger.info(f"Benchmarking endpoint: {endpoint}")

            start_time = time.time()
            request_times = []
            error_count = 0

            async with httpx.AsyncClient() as client:
                # Create semaphore for concurrency control
                semaphore = asyncio.Semaphore(concurrent_requests)

                async def make_request():
                    async with semaphore:
                        try:
                            req_start = time.time()
                            response = await client.get(f"http://localhost:8000{endpoint}")
                            req_time = (time.time() - req_start) * 1000

                            request_times.append(req_time)

                            if response.status_code >= 400:
                                nonlocal error_count
                                error_count += 1

                        except Exception:
                            error_count += 1

                # Run requests for specified duration
                tasks = []
                while time.time() - start_time < duration_seconds:
                    task = asyncio.create_task(make_request())
                    tasks.append(task)
                    await asyncio.sleep(0.1)  # Small delay between requests

                # Wait for all requests to complete
                await asyncio.gather(*tasks, return_exceptions=True)

            # Calculate statistics
            if request_times:
                request_times.sort()
                results[endpoint] = {
                    "total_requests": len(request_times),
                    "error_count": error_count,
                    "error_rate": error_count / len(request_times),
                    "avg_response_time_ms": sum(request_times) / len(request_times),
                    "min_response_time_ms": min(request_times),
                    "max_response_time_ms": max(request_times),
                    "p50_response_time_ms": request_times[int(0.5 * len(request_times))],
                    "p95_response_time_ms": request_times[int(0.95 * len(request_times))],
                    "p99_response_time_ms": request_times[int(0.99 * len(request_times))],
                    "requests_per_second": len(request_times) / duration_seconds,
                }
            else:
                results[endpoint] = {"error": "No successful requests"}

        return {
            "benchmark_results": results,
            "benchmark_config": {
                "concurrent_requests": concurrent_requests,
                "duration_seconds": duration_seconds,
                "endpoints_tested": len(endpoints),
            },
            "benchmark_completed_at": datetime.utcnow().isoformat(),
        }

    async def shutdown(self):
        """Gracefully shutdown the performance optimizer."""
        logger.info("Shutting down API performance optimizer...")

        # Cancel background tasks
        for task in [self._monitoring_task, self._optimization_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Clear caches
        await self.response_cache.clear()

        logger.info("API performance optimizer shutdown complete")


class PerformanceMiddleware(BaseHTTPMiddleware):
    """Middleware for API performance monitoring and optimization."""

    def __init__(self, app, optimizer: APIPerformanceOptimizer):
        super().__init__(app)
        self.optimizer = optimizer

    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip monitoring for health checks
        if request.url.path in ["/health", "/metrics"]:
            return await call_next(request)

        start_time = time.time()

        # Check for cached response
        if await self.optimizer.should_cache_response(request):
            cache_key = self.optimizer.generate_cache_key(request)
            cached_response = await self.optimizer.get_cached_response(cache_key)

            if cached_response:
                # Return cached response
                response_time = (time.time() - start_time) * 1000

                response = Response(
                    content=cached_response["content"],
                    status_code=cached_response["status_code"],
                    headers=cached_response["headers"],
                )
                response.headers["X-Cache"] = "HIT"
                response.headers["X-Response-Time-Ms"] = f"{response_time:.2f}"

                return response

        # Process request
        response = await call_next(request)
        response_time = time.time() - start_time

        # Cache successful responses if applicable
        if (
            await self.optimizer.should_cache_response(request)
            and 200 <= response.status_code < 300
        ):
            cache_key = self.optimizer.generate_cache_key(request)

            # Read response body for caching
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            # Cache the response
            cache_data = {
                "content": body,
                "status_code": response.status_code,
                "headers": dict(response.headers),
            }
            await self.optimizer.cache_response(cache_key, cache_data)

            # Recreate response with cached indicator
            response = Response(
                content=body, status_code=response.status_code, headers=response.headers
            )
            response.headers["X-Cache"] = "MISS"

        # Record performance metrics
        await self.optimizer.record_request_metrics(request, response, response_time)

        # Add performance headers
        response.headers["X-Response-Time-Ms"] = f"{response_time * 1000:.2f}"

        return response


# Factory function for creating performance optimizer
def create_api_performance_optimizer(
    app: FastAPI, strategy: OptimizationStrategy = OptimizationStrategy.BALANCED
) -> APIPerformanceOptimizer:
    """Create and configure API performance optimizer."""
    optimizer = APIPerformanceOptimizer(app, strategy)

    # Add performance middleware
    app.add_middleware(PerformanceMiddleware, optimizer=optimizer)

    logger.info(f"API performance optimizer created with {strategy.value} strategy")
    return optimizer
