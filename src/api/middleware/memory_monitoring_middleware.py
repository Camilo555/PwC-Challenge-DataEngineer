"""
Memory Monitoring Middleware for FastAPI
========================================

FastAPI middleware for real-time memory monitoring, leak detection, and automatic optimization.
Integrates with the enterprise memory monitoring system to provide request-level tracking.

Features:
- Per-request memory tracking
- Automatic memory optimization on high usage
- Memory leak detection for specific endpoints
- Integration with Prometheus metrics
- Memory usage headers in responses
- Circuit breaker for memory-intensive operations

Author: Enterprise Data Engineering Team
Created: 2025-09-18
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Callable, Dict, List, Optional, Set

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from src.core.config.memory_config import get_memory_monitoring_config
from src.monitoring.memory_usage_monitor import (
    MemoryAlert,
    MemoryMonitor,
    MemoryPressureLevel,
    get_memory_monitor,
    get_memory_usage,
    setup_memory_monitoring
)


class MemoryCircuitBreaker:
    """Circuit breaker for memory-intensive operations."""

    def __init__(
        self,
        memory_threshold_mb: float = 1000.0,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0
    ):
        self.memory_threshold_mb = memory_threshold_mb
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"  # closed, open, half-open

        self.logger = logging.getLogger(__name__)

    def is_memory_available(self) -> bool:
        """Check if memory is available for processing."""

        current_usage = get_memory_usage()
        return current_usage["process_rss_mb"] < self.memory_threshold_mb

    def can_proceed(self) -> bool:
        """Check if operation can proceed based on circuit breaker state."""

        current_time = time.time()

        if self.state == "closed":
            return True
        elif self.state == "open":
            if current_time - self.last_failure_time > self.recovery_timeout:
                self.state = "half-open"
                self.logger.info("Memory circuit breaker: transitioning to half-open")
                return True
            return False
        else:  # half-open
            return True

    def record_success(self):
        """Record successful operation."""

        if self.state == "half-open":
            self.state = "closed"
            self.failure_count = 0
            self.logger.info("Memory circuit breaker: closed after successful recovery")

    def record_failure(self):
        """Record failed operation due to memory constraints."""

        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            self.logger.warning(
                f"Memory circuit breaker: opened after {self.failure_count} failures"
            )


class MemoryMonitoringMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware for memory monitoring and optimization.
    """

    def __init__(
        self,
        app: ASGIApp,
        monitor: Optional[MemoryMonitor] = None,
        enable_request_tracking: bool = True,
        enable_response_headers: bool = True,
        enable_circuit_breaker: bool = True,
        memory_threshold_mb: float = 1500.0,
        high_memory_endpoints: Optional[Set[str]] = None
    ):
        super().__init__(app)

        self.monitor = monitor or get_memory_monitor()
        self.enable_request_tracking = enable_request_tracking
        self.enable_response_headers = enable_response_headers
        self.enable_circuit_breaker = enable_circuit_breaker

        # Circuit breaker for memory protection
        self.circuit_breaker = MemoryCircuitBreaker(memory_threshold_mb) if enable_circuit_breaker else None

        # High memory endpoints that need special attention
        self.high_memory_endpoints = high_memory_endpoints or {
            "/api/v1/etl/process",
            "/api/v1/data/export",
            "/api/v1/analytics/complex-query",
            "/graphql"
        }

        # Request tracking
        self.request_memory_usage: Dict[str, Dict] = {}
        self.endpoint_memory_stats: Dict[str, List[float]] = {}

        self.logger = logging.getLogger(__name__)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with memory monitoring."""

        request_id = id(request)
        endpoint = f"{request.method} {request.url.path}"

        # Pre-request memory check
        if self.enable_circuit_breaker and self.circuit_breaker:
            if not self.circuit_breaker.can_proceed():
                self.logger.warning(f"Request blocked by memory circuit breaker: {endpoint}")
                return Response(
                    content="Service temporarily unavailable due to memory constraints",
                    status_code=503,
                    headers={"Retry-After": "60"}
                )

        # Record initial memory state
        start_time = time.time()
        start_memory = None

        if self.enable_request_tracking:
            start_memory = get_memory_usage()
            self.request_memory_usage[request_id] = {
                "endpoint": endpoint,
                "start_time": start_time,
                "start_memory": start_memory
            }

        # Check if this is a high-memory endpoint
        is_high_memory_endpoint = any(
            pattern in request.url.path for pattern in self.high_memory_endpoints
        )

        # Use memory tracking context for high-memory endpoints
        if is_high_memory_endpoint and self.monitor:
            try:
                with self.monitor.memory_tracking(f"request_{endpoint}"):
                    response = await call_next(request)
            except Exception as e:
                self.logger.error(f"Error in memory tracking for {endpoint}: {e}")
                response = await call_next(request)
        else:
            response = await call_next(request)

        # Post-request processing
        await self._process_post_request(request_id, response, endpoint, start_time, start_memory)

        return response

    async def _process_post_request(
        self,
        request_id: int,
        response: Response,
        endpoint: str,
        start_time: float,
        start_memory: Optional[Dict]
    ):
        """Process request completion and update metrics."""

        duration = time.time() - start_time

        if self.enable_request_tracking and start_memory:
            end_memory = get_memory_usage()
            memory_delta = end_memory["process_rss_mb"] - start_memory["process_rss_mb"]

            # Track endpoint memory statistics
            if endpoint not in self.endpoint_memory_stats:
                self.endpoint_memory_stats[endpoint] = []

            self.endpoint_memory_stats[endpoint].append(memory_delta)

            # Keep only recent statistics
            if len(self.endpoint_memory_stats[endpoint]) > 100:
                self.endpoint_memory_stats[endpoint] = self.endpoint_memory_stats[endpoint][-50:]

            # Log significant memory usage
            if memory_delta > 50.0:  # 50MB threshold
                self.logger.warning(
                    f"High memory usage for {endpoint}: {memory_delta:+.2f}MB in {duration:.2f}s"
                )

            # Circuit breaker feedback
            if self.circuit_breaker:
                current_memory = end_memory["process_rss_mb"]
                if current_memory > self.circuit_breaker.memory_threshold_mb:
                    self.circuit_breaker.record_failure()
                else:
                    self.circuit_breaker.record_success()

            # Clean up tracking data
            if request_id in self.request_memory_usage:
                del self.request_memory_usage[request_id]

        # Add memory headers to response
        if self.enable_response_headers:
            current_usage = get_memory_usage()
            response.headers["X-Memory-Usage-MB"] = f"{current_usage['process_rss_mb']:.1f}"
            response.headers["X-Memory-Usage-Percent"] = f"{current_usage['process_percent']:.1f}"

            if self.circuit_breaker:
                response.headers["X-Memory-Circuit-Breaker"] = self.circuit_breaker.state

    def get_endpoint_memory_stats(self) -> Dict[str, Dict]:
        """Get memory statistics for all endpoints."""

        stats = {}
        for endpoint, memory_deltas in self.endpoint_memory_stats.items():
            if memory_deltas:
                stats[endpoint] = {
                    "count": len(memory_deltas),
                    "avg_memory_delta_mb": sum(memory_deltas) / len(memory_deltas),
                    "max_memory_delta_mb": max(memory_deltas),
                    "min_memory_delta_mb": min(memory_deltas),
                    "total_memory_delta_mb": sum(memory_deltas)
                }

        return stats

    def reset_endpoint_stats(self):
        """Reset endpoint memory statistics."""
        self.endpoint_memory_stats.clear()


class MemoryMonitoringService:
    """Service for managing memory monitoring in FastAPI applications."""

    def __init__(self):
        self.monitor: Optional[MemoryMonitor] = None
        self.middleware: Optional[MemoryMonitoringMiddleware] = None
        self.alert_handlers: List[Callable[[MemoryAlert], None]] = []
        self.is_initialized = False

        self.logger = logging.getLogger(__name__)

    async def initialize(
        self,
        app: Optional[FastAPI] = None,
        config: Optional[Dict] = None
    ):
        """Initialize memory monitoring service."""

        if self.is_initialized:
            return

        try:
            # Load configuration
            monitoring_config = get_memory_monitoring_config()
            if config:
                # Override with provided config
                for key, value in config.items():
                    setattr(monitoring_config, key, value)

            # Setup memory monitor
            self.monitor = setup_memory_monitoring(
                thresholds=monitoring_config.get_memory_thresholds(),
                optimization_strategy=getattr(monitoring_config, 'optimization_strategy', 'balanced'),
                monitoring_interval=monitoring_config.monitoring_interval,
                enable_prometheus=monitoring_config.prometheus_enabled
            )

            # Register default alert handlers
            self.monitor.add_alert_callback(self._default_alert_handler)

            # Add middleware to FastAPI app if provided
            if app:
                self.middleware = MemoryMonitoringMiddleware(
                    app=app,
                    monitor=self.monitor,
                    enable_circuit_breaker=True
                )
                app.add_middleware(MemoryMonitoringMiddleware, monitor=self.monitor)

            self.is_initialized = True
            self.logger.info("Memory monitoring service initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize memory monitoring service: {e}")
            raise

    async def shutdown(self):
        """Shutdown memory monitoring service."""

        if self.monitor:
            self.monitor.stop_monitoring()

        self.is_initialized = False
        self.logger.info("Memory monitoring service shutdown")

    def _default_alert_handler(self, alert: MemoryAlert):
        """Default alert handler."""

        self.logger.warning(f"Memory Alert: {alert.level.value} - {alert.message}")

        # Additional alert processing
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                self.logger.error(f"Error in custom alert handler: {e}")

    def add_alert_handler(self, handler: Callable[[MemoryAlert], None]):
        """Add custom alert handler."""
        self.alert_handlers.append(handler)

    def get_memory_summary(self) -> Dict:
        """Get comprehensive memory summary."""

        if not self.monitor:
            return {"error": "Memory monitoring not initialized"}

        summary = self.monitor.get_memory_summary()

        # Add middleware statistics if available
        if self.middleware:
            summary["endpoint_stats"] = self.middleware.get_endpoint_memory_stats()
            if self.middleware.circuit_breaker:
                summary["circuit_breaker"] = {
                    "state": self.middleware.circuit_breaker.state,
                    "failure_count": self.middleware.circuit_breaker.failure_count
                }

        return summary

    @asynccontextmanager
    async def memory_tracking(self, operation_name: str):
        """Context manager for tracking memory usage."""

        if self.monitor:
            with self.monitor.memory_tracking(operation_name):
                yield
        else:
            yield


# Global service instance
_memory_service: Optional[MemoryMonitoringService] = None


def get_memory_service() -> MemoryMonitoringService:
    """Get global memory monitoring service instance."""
    global _memory_service

    if _memory_service is None:
        _memory_service = MemoryMonitoringService()

    return _memory_service


async def setup_memory_monitoring_for_app(
    app: FastAPI,
    config: Optional[Dict] = None
) -> MemoryMonitoringService:
    """Setup memory monitoring for FastAPI application."""

    service = get_memory_service()
    await service.initialize(app=app, config=config)

    return service


# FastAPI lifespan management
@asynccontextmanager
async def memory_monitoring_lifespan(app: FastAPI):
    """FastAPI lifespan context manager for memory monitoring."""

    # Startup
    service = await setup_memory_monitoring_for_app(app)

    try:
        yield
    finally:
        # Shutdown
        await service.shutdown()


if __name__ == "__main__":
    # Example usage with FastAPI
    import uvicorn
    from fastapi import FastAPI

    # Create FastAPI app with memory monitoring
    app = FastAPI(
        title="Memory Monitoring Demo",
        lifespan=memory_monitoring_lifespan
    )

    @app.get("/")
    async def root():
        return {"message": "Memory monitoring active"}

    @app.get("/memory")
    async def memory_status():
        service = get_memory_service()
        return service.get_memory_summary()

    @app.get("/memory/allocate/{size_mb}")
    async def allocate_memory(size_mb: int):
        """Endpoint to test memory allocation."""

        # Allocate memory for testing
        data = [0] * (size_mb * 1024 * 1024 // 8)  # Approximate MB in integers

        return {
            "allocated_mb": size_mb,
            "current_memory": get_memory_usage()
        }

    if __name__ == "__main__":
        uvicorn.run(app, host="0.0.0.0", port=8000)