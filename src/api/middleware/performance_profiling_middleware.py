"""
Performance Profiling Middleware

Advanced middleware for automatic API endpoint performance profiling
and bottleneck identification in FastAPI applications.
"""

import asyncio
import psutil
import time
import tracemalloc
from typing import Callable, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from api.performance.endpoint_profiler import get_profiler, profile_request
from core.logging import get_logger
from core.config.base_config import BaseConfig

logger = get_logger(__name__)
config = BaseConfig()


class PerformanceProfilingMiddleware(BaseHTTPMiddleware):
    """
    Advanced performance profiling middleware that automatically
    profiles all API endpoints and identifies performance bottlenecks.
    """

    def __init__(self,
                 app,
                 enable_profiling: bool = True,
                 enable_memory_tracking: bool = True,
                 enable_cpu_tracking: bool = True,
                 exclude_paths: Optional[list] = None,
                 max_body_size_kb: int = 1024):
        super().__init__(app)
        self.enable_profiling = enable_profiling
        self.enable_memory_tracking = enable_memory_tracking
        self.enable_cpu_tracking = enable_cpu_tracking
        self.exclude_paths = exclude_paths or ["/health", "/metrics", "/docs", "/redoc"]
        self.max_body_size_kb = max_body_size_kb
        self.profiler = get_profiler()

        # Initialize memory tracking if enabled
        if self.enable_memory_tracking:
            tracemalloc.start()

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Main middleware dispatch method."""

        # Skip profiling for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)

        # Skip profiling if disabled
        if not self.enable_profiling:
            return await call_next(request)

        # Profile the request
        return await self._profile_request_with_context(request, call_next)

    async def _profile_request_with_context(self, request: Request, call_next: Callable) -> Response:
        """Profile request with comprehensive context tracking."""

        start_time = time.time()
        request_id = f"req_{int(start_time)}_{hash(str(request.url))}"

        # Initialize request state for context tracking
        request.state.request_id = request_id
        request.state.start_time = start_time
        request.state.db_time_ms = 0
        request.state.api_time_ms = 0
        request.state.cache_time_ms = 0
        request.state.serialization_time_ms = 0

        # Track initial resource usage
        if self.enable_cpu_tracking:
            initial_cpu = psutil.cpu_percent(interval=None)
            request.state.initial_cpu = initial_cpu

        if self.enable_memory_tracking and tracemalloc.is_tracing():
            initial_memory = tracemalloc.take_snapshot()
            request.state.initial_memory = initial_memory

        try:
            # Create context manager for tracking
            async with self._create_performance_context(request):
                # Execute the request
                response = await call_next(request)

            # Calculate final metrics and profile
            await self._finalize_profiling(request, response)

            return response

        except Exception as e:
            logger.error(f"Error in performance profiling middleware: {e}")

            # Still execute the request even if profiling fails
            response = await call_next(request)

            # Add error indication to response
            response.headers["X-Profiling-Error"] = "Profiling failed"
            return response

    @asynccontextmanager
    async def _create_performance_context(self, request: Request):
        """Create performance tracking context manager."""

        # Database query tracking
        original_execute = None
        db_start_times = []

        # External API tracking
        api_start_times = []

        # Cache operation tracking
        cache_start_times = []

        try:
            # Set up database query tracking
            try:
                from sqlalchemy import event
                from sqlalchemy.engine import Engine

                @event.listens_for(Engine, "before_cursor_execute")
                def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                    db_start_times.append(time.time())

                @event.listens_for(Engine, "after_cursor_execute")
                def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                    if db_start_times:
                        db_time = (time.time() - db_start_times.pop()) * 1000
                        request.state.db_time_ms += db_time

            except ImportError:
                logger.debug("SQLAlchemy not available for database tracking")

            # Set up external API call tracking
            try:
                import aiohttp
                original_request = aiohttp.ClientSession.request

                async def tracked_request(self, method, url, **kwargs):
                    api_start = time.time()
                    try:
                        response = await original_request(self, method, url, **kwargs)
                        api_time = (time.time() - api_start) * 1000
                        if hasattr(request.state, 'api_time_ms'):
                            request.state.api_time_ms += api_time
                        return response
                    except Exception as e:
                        api_time = (time.time() - api_start) * 1000
                        if hasattr(request.state, 'api_time_ms'):
                            request.state.api_time_ms += api_time
                        raise e

                aiohttp.ClientSession.request = tracked_request

            except ImportError:
                logger.debug("aiohttp not available for API call tracking")

            yield

        finally:
            # Restore original methods
            try:
                if original_execute:
                    # Restore original database execute method if we patched it
                    pass
            except:
                pass

    async def _finalize_profiling(self, request: Request, response: Response) -> None:
        """Finalize profiling and generate performance profile."""

        total_time = time.time() - request.state.start_time
        total_time_ms = total_time * 1000

        # Calculate CPU usage if enabled
        cpu_usage = 0
        if self.enable_cpu_tracking and hasattr(request.state, 'initial_cpu'):
            current_cpu = psutil.cpu_percent(interval=None)
            cpu_usage = max(0, current_cpu - request.state.initial_cpu)

        # Calculate memory usage if enabled
        memory_usage_mb = 0
        if (self.enable_memory_tracking and
            hasattr(request.state, 'initial_memory') and
            tracemalloc.is_tracing()):

            try:
                current_memory = tracemalloc.take_snapshot()
                top_stats = current_memory.compare_to(request.state.initial_memory, 'lineno')
                if top_stats:
                    memory_usage_mb = sum(stat.size_diff for stat in top_stats[:10]) / (1024 * 1024)
                    memory_usage_mb = max(0, memory_usage_mb)
            except Exception as e:
                logger.debug(f"Memory tracking error: {e}")

        # Calculate response serialization time (rough estimate)
        response_body_size = 0
        if hasattr(response, 'body') and response.body:
            response_body_size = len(response.body)
        elif hasattr(response, 'content') and response.content:
            response_body_size = len(response.content)

        # Estimate serialization time based on response size (rough heuristic)
        serialization_time_ms = max(1, response_body_size / 1024 / 100)  # ~100KB/ms baseline

        # Create execution context
        execution_context = {
            "request_id": request.state.request_id,
            "total_time_ms": total_time_ms,
            "database_time_ms": getattr(request.state, 'db_time_ms', 0),
            "external_api_time_ms": getattr(request.state, 'api_time_ms', 0),
            "cache_time_ms": getattr(request.state, 'cache_time_ms', 0),
            "serialization_time_ms": serialization_time_ms,
            "memory_usage_mb": memory_usage_mb,
            "cpu_usage_percent": cpu_usage,
            "user_id": getattr(request.state, 'user_id', None),
            "response_size_bytes": response_body_size,
            "request_size_bytes": await self._get_request_size(request)
        }

        # Profile the endpoint
        try:
            profile = await self.profiler.profile_endpoint(request, response, execution_context)

            # Add performance headers
            response.headers["X-Response-Time"] = f"{total_time_ms:.2f}ms"
            response.headers["X-Request-ID"] = request.state.request_id

            if profile.bottlenecks:
                response.headers["X-Performance-Bottlenecks"] = str(len(profile.bottlenecks))

            # Add detailed timing breakdown in debug mode
            if config.environment == "development":
                response.headers["X-DB-Time"] = f"{execution_context['database_time_ms']:.2f}ms"
                response.headers["X-API-Time"] = f"{execution_context['external_api_time_ms']:.2f}ms"
                response.headers["X-Memory-Usage"] = f"{memory_usage_mb:.2f}MB"
                response.headers["X-CPU-Usage"] = f"{cpu_usage:.1f}%"

        except Exception as e:
            logger.error(f"Error finalizing profiling: {e}")
            response.headers["X-Profiling-Error"] = "Profile generation failed"

    async def _get_request_size(self, request: Request) -> int:
        """Get request size in bytes."""
        try:
            # Get query parameters size
            query_size = len(str(request.query_params))

            # Get headers size
            headers_size = sum(len(k) + len(v) for k, v in request.headers.items())

            # Get body size if available (for POST/PUT requests)
            body_size = 0
            if hasattr(request, '_body') and request._body:
                body_size = len(request._body)

            return query_size + headers_size + body_size

        except Exception as e:
            logger.debug(f"Error calculating request size: {e}")
            return 0


class PerformanceReportingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that adds performance reporting endpoints.
    """

    def __init__(self, app, report_endpoint: str = "/api/performance/report"):
        super().__init__(app)
        self.report_endpoint = report_endpoint
        self.profiler = get_profiler()

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Handle performance reporting requests."""

        if request.url.path == self.report_endpoint and request.method == "GET":
            return await self._generate_performance_report(request)

        return await call_next(request)

    async def _generate_performance_report(self, request: Request) -> JSONResponse:
        """Generate comprehensive performance report."""

        try:
            # Get query parameters for filtering
            endpoint_filter = request.query_params.get("endpoint")
            limit = int(request.query_params.get("limit", "10"))

            if endpoint_filter:
                # Get specific endpoint analysis
                stats = self.profiler.get_endpoint_analysis(endpoint_filter)
                if stats:
                    return JSONResponse({
                        "endpoint_analysis": {
                            "endpoint": stats.endpoint,
                            "method": stats.method,
                            "performance_stats": {
                                "total_requests": stats.total_requests,
                                "avg_response_time_ms": stats.avg_response_time_ms,
                                "p95_response_time_ms": stats.p95_response_time_ms,
                                "p99_response_time_ms": stats.p99_response_time_ms,
                                "error_rate_percent": stats.error_rate_percent,
                                "throughput_rps": stats.throughput_rps
                            },
                            "bottlenecks": stats.common_bottlenecks,
                            "optimization_suggestions": stats.optimization_suggestions,
                            "last_updated": stats.last_updated.isoformat()
                        }
                    })
                else:
                    return JSONResponse({"error": "Endpoint not found in performance data"}, status_code=404)
            else:
                # Get comprehensive performance report
                report = self.profiler.get_performance_report()
                return JSONResponse(report)

        except Exception as e:
            logger.error(f"Error generating performance report: {e}")
            return JSONResponse({"error": "Failed to generate performance report"}, status_code=500)


# Factory functions for easy middleware creation
def create_profiling_middleware(app, **kwargs):
    """Create performance profiling middleware."""
    return PerformanceProfilingMiddleware(app, **kwargs)


def create_reporting_middleware(app, **kwargs):
    """Create performance reporting middleware."""
    return PerformanceReportingMiddleware(app, **kwargs)