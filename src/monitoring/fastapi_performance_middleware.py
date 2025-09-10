"""
FastAPI Performance Monitoring Middleware for <15ms SLA Validation
Integrates with PerformanceSLAMonitor to track production API performance with microsecond precision
"""

import asyncio
import time
import traceback
from typing import Callable, Dict, Any, Optional
from contextlib import asynccontextmanager

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from core.logging import get_logger
from monitoring.performance_sla_monitor import get_performance_monitor, PerformanceSLAMonitor

logger = get_logger(__name__)


class FastAPIPerformanceMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware for comprehensive performance monitoring with <15ms SLA validation.
    
    Features:
    - Microsecond-precision response time measurement
    - Real-time SLA compliance tracking
    - Cache performance monitoring (L0-L3)
    - Business story correlation
    - Production load validation
    - Automated performance regression detection
    """
    
    def __init__(self, 
                 app: ASGIApp, 
                 monitor: Optional[PerformanceSLAMonitor] = None,
                 excluded_paths: Optional[list] = None):
        super().__init__(app)
        self.monitor = monitor or get_performance_monitor()
        self.excluded_paths = excluded_paths or [
            '/health', '/metrics', '/docs', '/redoc', '/openapi.json', 
            '/favicon.ico', '/static'
        ]
        
        # Performance tracking
        self.request_counter = 0
        self.cache_layer_detection = {
            'memory': 'L0',
            'redis': 'L1', 
            'database': 'L2',
            'external': 'L3'
        }
        
        logger.info("FastAPI Performance Middleware initialized for <15ms SLA monitoring")
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with comprehensive performance monitoring."""
        
        # Skip monitoring for excluded paths
        if any(excluded in str(request.url.path) for excluded in self.excluded_paths):
            return await call_next(request)
        
        # Increment request counter
        self.request_counter += 1
        
        # Start high-precision timing
        start_time = time.perf_counter()
        start_timestamp = time.time()
        
        # Extract request metadata
        request_metadata = await self._extract_request_metadata(request)
        
        # Initialize response variables
        response = None
        error_occurred = False
        cache_info = {'hit': False, 'level': None}
        
        try:
            # Process request and capture response
            response = await call_next(request)
            
            # Extract cache information from response headers
            cache_info = self._extract_cache_info(response)
            
            # Extract additional response metadata
            response_metadata = self._extract_response_metadata(response)
            
        except Exception as e:
            error_occurred = True
            logger.error(f"Request processing error: {e}", extra={
                'endpoint': request.url.path,
                'method': request.method,
                'error': str(e),
                'traceback': traceback.format_exc()
            })
            
            # Create error response
            from fastapi import HTTPException
            from fastapi.responses import JSONResponse
            
            response = JSONResponse(
                status_code=500,
                content={"error": "Internal server error", "request_id": request_metadata.get('request_id')}
            )
        
        # Calculate precise response time
        end_time = time.perf_counter()
        response_time_seconds = end_time - start_time
        
        # Record performance measurement
        try:
            measurement = self.monitor.record_measurement(
                endpoint=request.url.path,
                method=request.method,
                response_time_seconds=response_time_seconds,
                status_code=response.status_code,
                user_id=request_metadata.get('user_id'),
                session_id=request_metadata.get('session_id'),
                cache_hit=cache_info['hit'],
                cache_level=cache_info['level'],
                request_size_bytes=request_metadata.get('content_length', 0),
                response_size_bytes=self._get_response_size(response),
                cpu_usage_percent=await self._get_cpu_usage(),
                memory_usage_mb=await self._get_memory_usage()
            )
            
            # Log performance details for sub-15ms tracking
            if measurement.response_time_ms < 15.0:
                logger.debug(f"SLA COMPLIANT: {request.method} {request.url.path} - "
                           f"{measurement.response_time_ms:.3f}ms "
                           f"({measurement.response_time_us}μs) - "
                           f"Grade: {measurement.performance_grade}")
            else:
                logger.warning(f"SLA VIOLATION: {request.method} {request.url.path} - "
                             f"{measurement.response_time_ms:.3f}ms "
                             f"(Target: <15ms, Exceeded by: {measurement.response_time_ms - 15.0:.3f}ms)")
            
            # Add performance headers to response
            self._add_performance_headers(response, measurement)
            
        except Exception as e:
            logger.error(f"Failed to record performance measurement: {e}")
        
        return response
    
    async def _extract_request_metadata(self, request: Request) -> Dict[str, Any]:
        """Extract comprehensive request metadata."""
        metadata = {}
        
        try:
            # Extract user information from headers or JWT
            auth_header = request.headers.get("authorization", "")
            if auth_header.startswith("Bearer "):
                # Extract user info from JWT token (simplified)
                metadata['user_id'] = request.headers.get("X-User-ID", "anonymous")
                metadata['session_id'] = request.headers.get("X-Session-ID")
            
            # Request size
            content_length = request.headers.get("content-length")
            if content_length:
                metadata['content_length'] = int(content_length)
            
            # Request ID for tracing
            metadata['request_id'] = request.headers.get("X-Request-ID", f"req_{int(time.time() * 1000)}")
            
            # Client information
            metadata['client_ip'] = self._get_client_ip(request)
            metadata['user_agent'] = request.headers.get("user-agent", "unknown")
            
        except Exception as e:
            logger.warning(f"Failed to extract request metadata: {e}")
        
        return metadata
    
    def _extract_cache_info(self, response: Response) -> Dict[str, Any]:
        """Extract cache information from response headers."""
        cache_info = {'hit': False, 'level': None}
        
        try:
            # Check standard cache headers
            cache_control = response.headers.get("cache-control", "")
            if "hit" in cache_control.lower():
                cache_info['hit'] = True
            
            # Check custom cache headers
            x_cache = response.headers.get("X-Cache", "").lower()
            if "hit" in x_cache:
                cache_info['hit'] = True
                
                # Determine cache level from custom headers
                if "memory" in x_cache or "l0" in x_cache:
                    cache_info['level'] = 'L0'
                elif "redis" in x_cache or "l1" in x_cache:
                    cache_info['level'] = 'L1'
                elif "database" in x_cache or "l2" in x_cache:
                    cache_info['level'] = 'L2'
                elif "external" in x_cache or "l3" in x_cache:
                    cache_info['level'] = 'L3'
            
            # Check CloudFlare cache headers
            cf_cache_status = response.headers.get("CF-Cache-Status", "").lower()
            if cf_cache_status in ["hit", "expired"]:
                cache_info['hit'] = True
                cache_info['level'] = 'L3'  # CDN level
            
        except Exception as e:
            logger.warning(f"Failed to extract cache info: {e}")
        
        return cache_info
    
    def _extract_response_metadata(self, response: Response) -> Dict[str, Any]:
        """Extract response metadata for performance analysis."""
        metadata = {}
        
        try:
            # Response size
            content_length = response.headers.get("content-length")
            if content_length:
                metadata['content_length'] = int(content_length)
            
            # Response type
            content_type = response.headers.get("content-type", "")
            metadata['content_type'] = content_type.split(';')[0].strip()
            
        except Exception as e:
            logger.warning(f"Failed to extract response metadata: {e}")
        
        return metadata
    
    def _get_client_ip(self, request: Request) -> str:
        """Get client IP from request headers."""
        # Check forwarded headers first
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
        
        # Fallback to client host
        if hasattr(request, 'client') and request.client:
            return request.client.host
        
        return "unknown"
    
    def _get_response_size(self, response: Response) -> int:
        """Get response size in bytes."""
        try:
            content_length = response.headers.get("content-length")
            if content_length:
                return int(content_length)
            
            # Estimate from response body if available
            if hasattr(response, 'body'):
                return len(response.body)
                
        except Exception as e:
            logger.warning(f"Failed to get response size: {e}")
        
        return 0
    
    async def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        try:
            import psutil
            return psutil.cpu_percent(interval=None)
        except ImportError:
            return 0.0
        except Exception as e:
            logger.debug(f"Failed to get CPU usage: {e}")
            return 0.0
    
    async def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            memory = psutil.virtual_memory()
            return (memory.total - memory.available) / 1024 / 1024  # Convert to MB
        except ImportError:
            return 0.0
        except Exception as e:
            logger.debug(f"Failed to get memory usage: {e}")
            return 0.0
    
    def _add_performance_headers(self, response: Response, measurement):
        """Add performance-related headers to response."""
        try:
            # Core performance headers
            response.headers["X-Response-Time-Ms"] = f"{measurement.response_time_ms:.3f}"
            response.headers["X-Response-Time-Us"] = str(measurement.response_time_us)
            response.headers["X-Performance-Grade"] = measurement.performance_grade
            response.headers["X-SLA-Compliant"] = "true" if measurement.is_sla_compliant else "false"
            
            # Cache performance headers
            if measurement.cache_hit:
                response.headers["X-Cache-Performance"] = f"HIT-{measurement.cache_level or 'UNKNOWN'}"
            else:
                response.headers["X-Cache-Performance"] = "MISS"
            
            # Business context headers
            if measurement.business_story:
                story_info = self.monitor.story_values.get(measurement.business_story, {})
                response.headers["X-Business-Story"] = f"{measurement.business_story}:{story_info.get('name', 'Unknown')}"
                response.headers["X-Business-Value"] = str(int(story_info.get('value', 0)))
            
            # Monitoring headers
            response.headers["X-Request-Count"] = str(self.request_counter)
            
        except Exception as e:
            logger.warning(f"Failed to add performance headers: {e}")
    
    def get_middleware_stats(self) -> Dict[str, Any]:
        """Get middleware performance statistics."""
        performance_summary = self.monitor.get_performance_summary()
        
        return {
            "middleware_info": {
                "total_requests_processed": self.request_counter,
                "sla_threshold_ms": self.monitor.sla_threshold_ms,
                "sla_target_percentage": self.monitor.sla_target_percentage,
                "excluded_paths": self.excluded_paths
            },
            "performance_summary": performance_summary,
            "real_time_status": {
                "monitoring_active": self.monitor.monitoring_active,
                "current_window_requests": len(self.monitor.current_window_measurements),
                "total_measurements": len(self.monitor.measurements)
            }
        }


# Middleware configuration helper
def setup_performance_monitoring_middleware(app, **kwargs):
    """
    Setup performance monitoring middleware for FastAPI application.
    
    Args:
        app: FastAPI application instance
        **kwargs: Additional configuration options
    
    Returns:
        FastAPIPerformanceMiddleware instance
    """
    
    # Create custom monitor if needed
    monitor = kwargs.get('monitor')
    if not monitor:
        from monitoring.performance_sla_monitor import PerformanceSLAMonitor
        monitor = PerformanceSLAMonitor(
            sla_threshold_ms=kwargs.get('sla_threshold_ms', 15.0),
            sla_target_percentage=kwargs.get('sla_target_percentage', 95.0),
            measurement_window_minutes=kwargs.get('measurement_window_minutes', 5),
            enable_datadog=kwargs.get('enable_datadog', True)
        )
        monitor.start_monitoring()
    
    # Create and add middleware
    middleware = FastAPIPerformanceMiddleware(
        app=app,
        monitor=monitor,
        excluded_paths=kwargs.get('excluded_paths')
    )
    
    app.add_middleware(
        FastAPIPerformanceMiddleware,
        monitor=monitor,
        excluded_paths=kwargs.get('excluded_paths')
    )
    
    logger.info("FastAPI Performance Monitoring Middleware configured for <15ms SLA validation")
    return middleware


# Context manager for manual performance tracking
@asynccontextmanager
async def track_performance(endpoint: str, method: str = "GET", **kwargs):
    """
    Context manager for manual performance tracking.
    
    Usage:
        async with track_performance("/api/v1/analytics", "POST", user_id="user123"):
            # Your code here
            result = await some_operation()
    """
    
    monitor = get_performance_monitor()
    start_time = time.perf_counter()
    
    try:
        yield
    except Exception as e:
        # Still record the measurement even if there was an error
        end_time = time.perf_counter()
        response_time = end_time - start_time
        
        monitor.record_measurement(
            endpoint=endpoint,
            method=method,
            response_time_seconds=response_time,
            status_code=500,  # Error status
            **kwargs
        )
        raise
    else:
        # Record successful measurement
        end_time = time.perf_counter()
        response_time = end_time - start_time
        
        monitor.record_measurement(
            endpoint=endpoint,
            method=method,
            response_time_seconds=response_time,
            status_code=kwargs.get('status_code', 200),
            **kwargs
        )


# Decorator for function-level performance tracking
def monitor_performance(endpoint: Optional[str] = None, method: str = "GET"):
    """
    Decorator for monitoring function performance.
    
    Usage:
        @monitor_performance("/api/v1/sales/analytics", "GET")
        async def get_sales_analytics():
            # Your function code
            return result
    """
    
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            func_endpoint = endpoint or f"{func.__module__}.{func.__name__}"
            
            async with track_performance(func_endpoint, method):
                return await func(*args, **kwargs)
        
        def sync_wrapper(*args, **kwargs):
            func_endpoint = endpoint or f"{func.__module__}.{func.__name__}"
            monitor = get_performance_monitor()
            
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                end_time = time.perf_counter()
                response_time = end_time - start_time
                
                monitor.record_measurement(
                    endpoint=func_endpoint,
                    method=method,
                    response_time_seconds=response_time,
                    status_code=200
                )
                return result
            except Exception as e:
                end_time = time.perf_counter()
                response_time = end_time - start_time
                
                monitor.record_measurement(
                    endpoint=func_endpoint,
                    method=method,
                    response_time_seconds=response_time,
                    status_code=500
                )
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator