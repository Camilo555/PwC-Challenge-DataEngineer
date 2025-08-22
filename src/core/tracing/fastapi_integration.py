"""
FastAPI Integration for Distributed Tracing
Enhances the existing correlation middleware with OpenTelemetry tracing.
"""

from typing import Callable, Optional, Dict, Any, List
import time

try:
    from fastapi import FastAPI, Request, Response
    from fastapi.middleware.base import BaseHTTPMiddleware
    from starlette.responses import Response as StarletteResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    FastAPI = Any
    Request = Any
    Response = Any
    BaseHTTPMiddleware = object

from .tracer import get_tracer, trace_api_request
from .correlation import CorrelationContext, extract_context_from_headers
from .otel_config import is_tracing_enabled
from core.logging import get_logger

logger = get_logger(__name__)


class TracingMiddleware(BaseHTTPMiddleware):
    """
    Enhanced middleware that combines correlation IDs with OpenTelemetry tracing.
    Builds upon the existing correlation middleware.
    """
    
    def __init__(
        self,
        app: FastAPI,
        service_name: str = "retail-etl-api",
        exclude_paths: Optional[List[str]] = None,
        include_request_body: bool = False,
        include_response_body: bool = False,
        max_body_size: int = 1024 * 10
    ):
        """
        Initialize tracing middleware.
        
        Args:
            app: FastAPI application
            service_name: Service name for tracing
            exclude_paths: Paths to exclude from tracing
            include_request_body: Whether to include request body in traces
            include_response_body: Whether to include response body in traces
            max_body_size: Maximum body size to include (bytes)
        """
        super().__init__(app)
        self.service_name = service_name
        self.exclude_paths = exclude_paths or [
            "/health", "/metrics", "/favicon.ico", "/docs", "/redoc", "/openapi.json"
        ]
        self.include_request_body = include_request_body
        self.include_response_body = include_response_body
        self.max_body_size = max_body_size
        
        self.tracer = get_tracer(service_name)
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with enhanced tracing and correlation."""
        
        # Skip excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)
        
        # Extract correlation context from headers
        correlation_ctx = extract_context_from_headers(dict(request.headers))
        
        # Start timing
        start_time = time.time()
        
        # Create span for the request
        span_name = f"{request.method} {request.url.path}"
        
        with correlation_ctx:
            async with self.tracer.async_span(
                span_name, 
                kind="server",
                attributes=self._get_request_attributes(request)
            ) as span:
                try:
                    # Add correlation IDs to span
                    if span:
                        span.set_attribute("correlation.id", correlation_ctx.correlation_id)
                        span.set_attribute("request.id", correlation_ctx.request_id)
                        if correlation_ctx.user_id:
                            span.set_attribute("user.id", correlation_ctx.user_id)
                    
                    # Add request body if enabled
                    if self.include_request_body and span:
                        await self._add_request_body(request, span)
                    
                    # Process request
                    response = await call_next(request)
                    
                    # Calculate duration
                    duration = time.time() - start_time
                    
                    # Add response attributes
                    if span:
                        span.set_attribute("http.status_code", response.status_code)
                        span.set_attribute("http.duration_ms", duration * 1000)
                        
                        # Set span status based on response
                        if response.status_code >= 400:
                            self.tracer.set_span_status("error", f"HTTP {response.status_code}")
                        else:
                            self.tracer.set_span_status("ok")
                    
                    # Add response body if enabled
                    if self.include_response_body and span:
                        await self._add_response_body(response, span)
                    
                    # Add correlation headers to response
                    self._add_correlation_headers(response, correlation_ctx)
                    
                    return response
                    
                except Exception as e:
                    # Record exception in span
                    if span:
                        span.record_exception(e)
                        self.tracer.set_span_status("error", str(e))
                    
                    logger.error(
                        f"Request failed: {request.method} {request.url.path}",
                        exc_info=e,
                        extra={
                            "correlation_id": correlation_ctx.correlation_id,
                            "request_id": correlation_ctx.request_id,
                            "duration_ms": (time.time() - start_time) * 1000
                        }
                    )
                    raise
    
    def _get_request_attributes(self, request: Request) -> Dict[str, Any]:
        """Get OpenTelemetry attributes for the request."""
        attributes = {
            "http.method": request.method,
            "http.url": str(request.url),
            "http.scheme": request.url.scheme,
            "http.host": request.url.hostname or "unknown",
            "http.target": request.url.path,
            "http.user_agent": request.headers.get("user-agent", "unknown"),
            "http.flavor": "1.1",  # Assume HTTP/1.1
            "net.peer.ip": self._get_client_ip(request),
            "service.name": self.service_name
        }
        
        # Add query parameters (sanitized)
        if request.url.query:
            attributes["http.query_string"] = self._sanitize_query_string(request.url.query)
        
        # Add route if available
        if hasattr(request, "route") and request.route:
            attributes["http.route"] = request.route.path
        
        return attributes
    
    def _get_client_ip(self, request: Request) -> str:
        """Get client IP address from request."""
        # Check for forwarded headers
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip
        
        # Fall back to client host
        if request.client:
            return request.client.host
        
        return "unknown"
    
    def _sanitize_query_string(self, query_string: str) -> str:
        """Sanitize query string by removing sensitive parameters."""
        sensitive_params = {"password", "token", "key", "secret", "auth"}
        
        params = []
        for param in query_string.split("&"):
            if "=" in param:
                key, value = param.split("=", 1)
                if key.lower() in sensitive_params:
                    params.append(f"{key}=***")
                else:
                    params.append(param)
            else:
                params.append(param)
        
        return "&".join(params)
    
    async def _add_request_body(self, request: Request, span) -> None:
        """Add request body to span if enabled and within size limit."""
        try:
            body = await request.body()
            if len(body) <= self.max_body_size:
                # Try to decode as text
                try:
                    body_text = body.decode('utf-8')
                    span.set_attribute("http.request.body", body_text)
                except UnicodeDecodeError:
                    span.set_attribute("http.request.body.size", len(body))
                    span.set_attribute("http.request.body.type", "binary")
            else:
                span.set_attribute("http.request.body.size", len(body))
                span.set_attribute("http.request.body.truncated", True)
        except Exception as e:
            logger.debug(f"Failed to add request body to span: {e}")
    
    async def _add_response_body(self, response: Response, span) -> None:
        """Add response body to span if enabled and within size limit."""
        try:
            # This is complex to implement without consuming the response
            # For now, just add content type and length
            content_type = response.headers.get("content-type", "unknown")
            span.set_attribute("http.response.content_type", content_type)
            
            content_length = response.headers.get("content-length")
            if content_length:
                span.set_attribute("http.response.content_length", int(content_length))
        except Exception as e:
            logger.debug(f"Failed to add response body to span: {e}")
    
    def _add_correlation_headers(self, response: Response, correlation_ctx: CorrelationContext) -> None:
        """Add correlation headers to response."""
        correlation_headers = correlation_ctx.to_headers()
        
        for header, value in correlation_headers.items():
            response.headers[header] = value


def setup_tracing_middleware(
    app: FastAPI,
    service_name: str = "retail-etl-api",
    **kwargs
) -> bool:
    """
    Set up tracing middleware for FastAPI application.
    
    Args:
        app: FastAPI application
        service_name: Service name for tracing
        **kwargs: Additional middleware configuration
        
    Returns:
        True if middleware was added successfully
    """
    if not FASTAPI_AVAILABLE:
        logger.warning("FastAPI not available, skipping tracing middleware")
        return False
    
    try:
        # Add tracing middleware
        app.add_middleware(TracingMiddleware, service_name=service_name, **kwargs)
        
        # Auto-instrument FastAPI if OpenTelemetry is available
        if is_tracing_enabled():
            from .instrumentation import instrument_fastapi
            instrument_fastapi(app)
        
        logger.info(f"Tracing middleware configured for {service_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to setup tracing middleware: {e}")
        return False


def create_traced_app(
    app_name: str = "retail-etl-api",
    service_name: Optional[str] = None,
    **middleware_kwargs
) -> Optional[FastAPI]:
    """
    Create a FastAPI app with tracing pre-configured.
    
    Args:
        app_name: Application name
        service_name: Service name for tracing
        **middleware_kwargs: Additional middleware configuration
        
    Returns:
        FastAPI app with tracing configured
    """
    if not FASTAPI_AVAILABLE:
        logger.error("FastAPI not available")
        return None
    
    app = FastAPI(title=app_name)
    
    service_name = service_name or app_name
    success = setup_tracing_middleware(app, service_name, **middleware_kwargs)
    
    if success:
        logger.info(f"Created traced FastAPI app: {app_name}")
    else:
        logger.warning(f"Created FastAPI app without tracing: {app_name}")
    
    return app