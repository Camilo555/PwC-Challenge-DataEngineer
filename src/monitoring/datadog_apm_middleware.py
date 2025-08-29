"""
Comprehensive DataDog APM Middleware for FastAPI
Provides deep APM integration with automatic instrumentation, custom spans, and business metrics
"""

import asyncio
import json
import time
import traceback
import uuid
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

# DataDog APM imports
from ddtrace import patch_all, tracer, config
from ddtrace.ext import http, errors
from ddtrace.filters import FilterRequestsOnUrl
from ddtrace.propagation.http import HTTPPropagator

from core.config import get_settings
from core.logging import get_logger
from monitoring.datadog_integration import create_datadog_monitoring, DatadogMonitoring

logger = get_logger(__name__)
settings = get_settings()

# Context variables for request tracking
request_id_var: ContextVar[Optional[str]] = ContextVar('request_id', default=None)
trace_id_var: ContextVar[Optional[str]] = ContextVar('trace_id', default=None)
span_id_var: ContextVar[Optional[str]] = ContextVar('span_id', default=None)
user_id_var: ContextVar[Optional[str]] = ContextVar('user_id', default=None)


class DataDogAPMConfig:
    """Configuration for DataDog APM integration."""
    
    def __init__(self):
        # Core APM settings
        self.service_name = getattr(settings, 'DD_SERVICE', 'pwc-data-engineering')
        self.environment = getattr(settings, 'DD_ENV', 'development')
        self.version = getattr(settings, 'DD_VERSION', '1.0.0')
        
        # Agent settings
        self.agent_host = getattr(settings, 'DD_AGENT_HOST', 'localhost')
        self.agent_port = int(getattr(settings, 'DD_TRACE_AGENT_PORT', 8126))
        
        # Feature flags
        self.enable_profiling = getattr(settings, 'DD_PROFILING_ENABLED', 'true').lower() == 'true'
        self.enable_runtime_metrics = getattr(settings, 'DD_RUNTIME_METRICS_ENABLED', 'true').lower() == 'true'
        self.enable_logs_injection = getattr(settings, 'DD_LOGS_INJECTION', 'true').lower() == 'true'
        
        # Sampling settings
        self.sample_rate = float(getattr(settings, 'DD_TRACE_SAMPLE_RATE', 1.0))
        self.analytics_enabled = getattr(settings, 'DD_TRACE_ANALYTICS_ENABLED', 'true').lower() == 'true'
        
        # Custom settings
        self.enable_custom_metrics = True
        self.enable_business_metrics = True
        self.enable_error_tracking = True
        self.enable_performance_monitoring = True
        
        # Ignored routes
        self.ignored_routes = [
            '/health',
            '/metrics',
            '/docs',
            '/redoc', 
            '/openapi.json',
            '/favicon.ico'
        ]


class DataDogAPMMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive DataDog APM Middleware for FastAPI
    
    Features:
    - Automatic request tracing with custom spans
    - Business metrics and KPI tracking
    - Error tracking and analysis
    - Performance monitoring
    - User activity tracking
    - Custom tags and metadata
    """
    
    def __init__(self, app: ASGIApp, config: Optional[DataDogAPMConfig] = None):
        super().__init__(app)
        self.config = config or DataDogAPMConfig()
        self.datadog_monitoring: Optional[DatadogMonitoring] = None
        
        # Initialize DataDog APM
        self._initialize_datadog_apm()
        
        # Performance tracking
        self.request_count = 0
        self.error_count = 0
        self.total_response_time = 0.0
        
        logger.info(f"DataDog APM Middleware initialized for service: {self.config.service_name}")
    
    def _initialize_datadog_apm(self):
        """Initialize DataDog APM configuration."""
        try:
            # Configure DataDog tracer
            config.service = self.config.service_name
            config.env = self.config.environment
            config.version = self.config.version
            
            # Configure agent connection
            config.trace.hostname = self.config.agent_host
            config.trace.port = self.config.agent_port
            
            # Configure sampling
            config.trace.sample_rate = self.config.sample_rate
            
            # Enable profiling if configured
            if self.config.enable_profiling:
                config.profiling.enabled = True
                
            # Enable runtime metrics if configured
            if self.config.enable_runtime_metrics:
                config.runtime_metrics.enabled = True
            
            # Enable logs injection
            if self.config.enable_logs_injection:
                config.logs_injection = True
            
            # Automatic instrumentation for common libraries
            patch_all()
            
            # Configure request filtering
            tracer.configure(
                settings={
                    'FILTERS': [
                        FilterRequestsOnUrl(r'http.*/health'),
                        FilterRequestsOnUrl(r'http.*/metrics'),
                        FilterRequestsOnUrl(r'http.*/docs'),
                        FilterRequestsOnUrl(r'http.*/redoc'),
                        FilterRequestsOnUrl(r'http.*/openapi.json'),
                        FilterRequestsOnUrl(r'http.*/favicon.ico'),
                    ]
                }
            )
            
            # Initialize DataDog monitoring
            if self.config.enable_custom_metrics:
                self.datadog_monitoring = create_datadog_monitoring()
            
            logger.info("DataDog APM initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize DataDog APM: {str(e)}")
            raise
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process HTTP request with comprehensive APM monitoring."""
        
        # Skip monitoring for ignored routes
        if any(ignored in str(request.url) for ignored in self.config.ignored_routes):
            return await call_next(request)
        
        # Generate request ID
        request_id = str(uuid.uuid4())
        request_id_var.set(request_id)
        
        # Start timing
        start_time = time.time()
        
        # Extract trace context from headers
        span_context = HTTPPropagator.extract(request.headers)
        
        # Create main request span
        with tracer.trace(
            name="fastapi.request",
            service=self.config.service_name,
            resource=f"{request.method} {request.url.path}",
            span_type="web"
        ) as span:
            
            # Set context variables
            trace_id_var.set(str(span.trace_id))
            span_id_var.set(str(span.span_id))
            
            # Set standard tags
            span.set_tags({
                "http.method": request.method,
                "http.url": str(request.url),
                "http.route": request.url.path,
                "http.scheme": request.url.scheme,
                "http.host": request.headers.get("host", "unknown"),
                "http.user_agent": request.headers.get("user-agent", "unknown"),
                "request.id": request_id,
                "service.name": self.config.service_name,
                "service.version": self.config.version,
                "environment": self.config.environment
            })
            
            # Extract user information if available
            user_info = await self._extract_user_info(request)
            if user_info:
                user_id_var.set(user_info.get("user_id"))
                span.set_tags({
                    "user.id": user_info.get("user_id"),
                    "user.roles": ",".join(user_info.get("roles", [])),
                    "user.session_id": user_info.get("session_id"),
                    "auth.method": user_info.get("auth_method", "unknown")
                })
            
            # Set client information
            client_ip = self._get_client_ip(request)
            if client_ip:
                span.set_tag("http.client_ip", client_ip)
            
            response = None
            error_occurred = False
            
            try:
                # Add request body information for non-GET requests
                if request.method in ["POST", "PUT", "PATCH"]:
                    content_type = request.headers.get("content-type", "")
                    span.set_tag("http.request.content_type", content_type)
                    
                    # Track request size
                    content_length = request.headers.get("content-length")
                    if content_length:
                        span.set_tag("http.request.content_length", int(content_length))
                
                # Process request with business context
                with tracer.trace("business.process_request", service=self.config.service_name) as business_span:
                    business_span.set_tag("business.component", self._identify_business_component(request.url.path))
                    business_span.set_tag("business.operation", self._identify_business_operation(request.method, request.url.path))
                    
                    response = await call_next(request)
                
                # Set response tags
                span.set_tags({
                    "http.status_code": response.status_code,
                    "http.status_class": f"{response.status_code // 100}xx"
                })
                
                # Track response size
                if hasattr(response, 'headers') and 'content-length' in response.headers:
                    span.set_tag("http.response.content_length", response.headers['content-length'])
                
                # Mark errors
                if response.status_code >= 400:
                    error_occurred = True
                    span.set_tag("error", True)
                    span.set_tag("error.type", "http_error")
                    span.set_tag("error.message", f"HTTP {response.status_code}")
                    
                    if response.status_code >= 500:
                        span.set_tag("error.severity", "critical")
                    else:
                        span.set_tag("error.severity", "warning")
            
            except Exception as e:
                error_occurred = True
                error_type = type(e).__name__
                error_message = str(e)
                error_traceback = traceback.format_exc()
                
                # Set error information
                span.set_error(e)
                span.set_tags({
                    "error": True,
                    "error.type": error_type,
                    "error.message": error_message,
                    "error.severity": "critical",
                    "error.stack": error_traceback
                })
                
                logger.error(f"Request processing error: {error_message}", extra={
                    "request_id": request_id,
                    "trace_id": span.trace_id,
                    "error_type": error_type,
                    "path": request.url.path,
                    "method": request.method
                })
                
                # Create error response
                response = JSONResponse(
                    status_code=500,
                    content={"error": "Internal server error", "request_id": request_id}
                )
            
            # Calculate response time
            response_time = time.time() - start_time
            response_time_ms = response_time * 1000
            
            span.set_tag("http.response_time_ms", response_time_ms)
            
            # Update performance counters
            self.request_count += 1
            if error_occurred:
                self.error_count += 1
            self.total_response_time += response_time
            
            # Send custom metrics
            if self.datadog_monitoring and self.config.enable_custom_metrics:
                await self._send_request_metrics(request, response, response_time_ms, user_info)
            
            # Add APM headers to response
            if hasattr(response, 'headers'):
                response.headers["X-Trace-ID"] = str(span.trace_id)
                response.headers["X-Span-ID"] = str(span.span_id)
                response.headers["X-Request-ID"] = request_id
            
            return response
    
    async def _extract_user_info(self, request: Request) -> Optional[Dict[str, Any]]:
        """Extract user information from request."""
        try:
            # Check for JWT token
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                # This would integrate with your auth service
                # For now, return basic info if available
                return {
                    "user_id": request.headers.get("X-User-ID", "anonymous"),
                    "session_id": request.headers.get("X-Session-ID"),
                    "auth_method": "jwt"
                }
            
            # Check for API key
            api_key = request.headers.get("X-API-Key")
            if api_key:
                return {
                    "user_id": f"api_key_{api_key[:8]}",
                    "auth_method": "api_key"
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to extract user info: {str(e)}")
            return None
    
    def _get_client_ip(self, request: Request) -> Optional[str]:
        """Get client IP address from request."""
        # Check common headers for real client IP
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
        
        # Fallback to request client
        if hasattr(request, 'client') and request.client:
            return request.client.host
        
        return None
    
    def _identify_business_component(self, path: str) -> str:
        """Identify business component from request path."""
        if "/sales" in path:
            return "sales"
        elif "/analytics" in path:
            return "analytics"
        elif "/ml" in path:
            return "ml"
        elif "/etl" in path:
            return "etl"
        elif "/auth" in path:
            return "authentication"
        elif "/search" in path:
            return "search"
        elif "/datamart" in path:
            return "datamart"
        else:
            return "general"
    
    def _identify_business_operation(self, method: str, path: str) -> str:
        """Identify business operation from method and path."""
        if method == "GET":
            if "/analytics" in path:
                return "data_analysis"
            elif "/search" in path:
                return "data_search"
            else:
                return "data_retrieval"
        elif method == "POST":
            if "/auth" in path:
                return "authentication"
            elif "/ml" in path:
                return "ml_inference"
            else:
                return "data_creation"
        elif method == "PUT" or method == "PATCH":
            return "data_update"
        elif method == "DELETE":
            return "data_deletion"
        else:
            return "data_operation"
    
    async def _send_request_metrics(self, request: Request, response: Response, 
                                  response_time_ms: float, user_info: Optional[Dict]):
        """Send custom metrics to DataDog."""
        try:
            if not self.datadog_monitoring:
                return
            
            # Prepare tags
            tags = [
                f"service:{self.config.service_name}",
                f"environment:{self.config.environment}",
                f"method:{request.method}",
                f"route:{request.url.path}",
                f"status_code:{response.status_code}",
                f"status_class:{response.status_code // 100}xx",
                f"component:{self._identify_business_component(request.url.path)}",
                f"operation:{self._identify_business_operation(request.method, request.url.path)}"
            ]
            
            if user_info:
                tags.append(f"auth_method:{user_info.get('auth_method', 'none')}")
                if user_info.get('user_id'):
                    tags.append(f"user_type:{'api' if user_info['auth_method'] == 'api_key' else 'user'}")
            
            # Track API request metrics
            self.datadog_monitoring.track_api_request(
                endpoint=request.url.path,
                method=request.method,
                status_code=response.status_code,
                response_time_ms=response_time_ms,
                user_id=user_info.get('user_id') if user_info else None
            )
            
            # Custom business metrics
            if self.config.enable_business_metrics:
                component = self._identify_business_component(request.url.path)
                
                # Track business component usage
                self.datadog_monitoring.counter(
                    "business.component.requests",
                    tags=tags + [f"business_component:{component}"]
                )
                
                # Track response time by component
                self.datadog_monitoring.histogram(
                    "business.component.response_time",
                    response_time_ms,
                    tags=tags + [f"business_component:{component}"]
                )
                
                # Track error rates by component
                if response.status_code >= 400:
                    self.datadog_monitoring.counter(
                        "business.component.errors",
                        tags=tags + [f"business_component:{component}"]
                    )
                
                # Track user activity
                if user_info and user_info.get('user_id'):
                    self.datadog_monitoring.counter(
                        "business.user.activity",
                        tags=tags + [f"user_id:{user_info['user_id']}"]
                    )
            
        except Exception as e:
            logger.warning(f"Failed to send request metrics: {str(e)}")
    
    def get_middleware_metrics(self) -> Dict[str, Any]:
        """Get middleware performance metrics."""
        avg_response_time = (
            self.total_response_time / self.request_count 
            if self.request_count > 0 else 0
        )
        
        error_rate = (
            self.error_count / self.request_count 
            if self.request_count > 0 else 0
        )
        
        return {
            "total_requests": self.request_count,
            "total_errors": self.error_count,
            "error_rate": error_rate,
            "avg_response_time_seconds": avg_response_time,
            "service_name": self.config.service_name,
            "environment": self.config.environment
        }


def setup_datadog_apm_middleware(app: FastAPI, config: Optional[DataDogAPMConfig] = None):
    """Setup DataDog APM middleware for FastAPI application."""
    
    # Create configuration
    apm_config = config or DataDogAPMConfig()
    
    # Add middleware
    middleware = DataDogAPMMiddleware(app, apm_config)
    app.add_middleware(DataDogAPMMiddleware, config=apm_config)
    
    logger.info(f"DataDog APM middleware added to FastAPI application: {apm_config.service_name}")
    
    return middleware


# Context managers for custom tracing
class DataDogTraceContext:
    """Context manager for custom DataDog tracing."""
    
    def __init__(self, operation_name: str, service_name: Optional[str] = None, 
                 resource_name: Optional[str] = None, span_type: Optional[str] = None,
                 tags: Optional[Dict[str, str]] = None):
        self.operation_name = operation_name
        self.service_name = service_name
        self.resource_name = resource_name
        self.span_type = span_type
        self.tags = tags or {}
        self.span = None
    
    def __enter__(self):
        self.span = tracer.trace(
            name=self.operation_name,
            service=self.service_name,
            resource=self.resource_name,
            span_type=self.span_type
        )
        
        # Set tags
        for key, value in self.tags.items():
            self.span.set_tag(key, value)
        
        return self.span
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.span.set_error(exc_val)
        self.span.finish()


# Decorators for function tracing
def trace_function(operation_name: Optional[str] = None, service_name: Optional[str] = None,
                  resource_name: Optional[str] = None, tags: Optional[Dict[str, str]] = None):
    """Decorator for tracing functions with DataDog APM."""
    
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            res_name = resource_name or func.__name__
            
            with DataDogTraceContext(
                operation_name=op_name,
                service_name=service_name,
                resource_name=res_name,
                tags=tags
            ) as span:
                try:
                    result = await func(*args, **kwargs)
                    span.set_tag("function.success", True)
                    return result
                except Exception as e:
                    span.set_tag("function.success", False)
                    span.set_error(e)
                    raise
        
        def sync_wrapper(*args, **kwargs):
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            res_name = resource_name or func.__name__
            
            with DataDogTraceContext(
                operation_name=op_name,
                service_name=service_name,
                resource_name=res_name,
                tags=tags
            ) as span:
                try:
                    result = func(*args, **kwargs)
                    span.set_tag("function.success", True)
                    return result
                except Exception as e:
                    span.set_tag("function.success", False)
                    span.set_error(e)
                    raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Helper functions for getting trace context
def get_current_trace_id() -> Optional[str]:
    """Get current trace ID from context."""
    return trace_id_var.get()


def get_current_span_id() -> Optional[str]:
    """Get current span ID from context."""
    return span_id_var.get()


def get_current_request_id() -> Optional[str]:
    """Get current request ID from context."""
    return request_id_var.get()


def get_current_user_id() -> Optional[str]:
    """Get current user ID from context."""
    return user_id_var.get()


def get_trace_context() -> Dict[str, Optional[str]]:
    """Get full trace context."""
    return {
        "trace_id": get_current_trace_id(),
        "span_id": get_current_span_id(),
        "request_id": get_current_request_id(),
        "user_id": get_current_user_id()
    }


# Utility for adding custom tags to current span
def add_custom_tags(tags: Dict[str, Any]):
    """Add custom tags to the current active span."""
    current_span = tracer.current_span()
    if current_span:
        for key, value in tags.items():
            current_span.set_tag(key, str(value))


def add_custom_metric(name: str, value: float, tags: Optional[Dict[str, str]] = None):
    """Add custom metric to current trace."""
    current_span = tracer.current_span()
    if current_span:
        current_span.set_metric(name, value)
        if tags:
            for key, tag_value in tags.items():
                current_span.set_tag(f"metric.{key}", tag_value)