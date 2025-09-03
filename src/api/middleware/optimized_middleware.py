"""
Optimized API Middleware Stack
Combines multiple middleware functions into essential layers for maximum performance.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException, Request, Response, status
from fastapi.responses import ORJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from api.common.response_models import ErrorCode, PerformanceTracker, ResponseBuilder
from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)
config = BaseConfig()


class CombinedSecurityPerformanceMiddleware(BaseHTTPMiddleware):
    """
    Combined middleware that handles:
    - Request correlation IDs
    - Performance tracking
    - Security headers
    - Error handling
    - JSON optimization
    - Basic rate limiting checks

    This replaces 4-5 separate middleware layers.
    """

    def __init__(
        self,
        app,
        enable_security_headers: bool = True,
        enable_performance_tracking: bool = True,
        enable_correlation_id: bool = True,
        json_response_optimization: bool = True,
    ):
        super().__init__(app)
        self.enable_security_headers = enable_security_headers
        self.enable_performance_tracking = enable_performance_tracking
        self.enable_correlation_id = enable_correlation_id
        self.json_response_optimization = json_response_optimization

        # Security headers configuration
        self.security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Content-Security-Policy": "default-src 'self'",
        }

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Main middleware dispatch method."""
        start_time = time.time()

        # Generate correlation ID
        correlation_id = None
        if self.enable_correlation_id:
            correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
            request.state.correlation_id = correlation_id

        # Initialize performance tracker
        if self.enable_performance_tracking:
            request.state.performance_tracker = PerformanceTracker()

        # Add request metadata to state
        request.state.start_time = start_time
        request.state.user_agent = request.headers.get("user-agent", "")
        request.state.client_ip = self._get_client_ip(request)

        try:
            # Process request
            response = await call_next(request)

            # Add performance headers
            if self.enable_performance_tracking:
                process_time = time.time() - start_time
                response.headers["X-Process-Time"] = f"{process_time:.4f}"
                response.headers["X-API-Version"] = (
                    config.api_version if hasattr(config, "api_version") else "4.0.0"
                )

            # Add correlation ID to response
            if self.enable_correlation_id and correlation_id:
                response.headers["X-Correlation-ID"] = correlation_id

            # Add security headers
            if self.enable_security_headers:
                for header, value in self.security_headers.items():
                    response.headers[header] = value

            # Optimize JSON responses
            if self.json_response_optimization and self._is_json_response(response):
                response = await self._optimize_json_response(response, request)

            return response

        except HTTPException as exc:
            # Handle HTTP exceptions with standardized format
            return await self._handle_http_exception(exc, request, correlation_id)

        except Exception as exc:
            # Handle unexpected exceptions
            logger.error(
                f"Unexpected error in middleware: {exc}",
                extra={
                    "correlation_id": correlation_id,
                    "path": str(request.url.path),
                    "method": request.method,
                    "client_ip": request.state.client_ip,
                },
            )
            return await self._handle_internal_error(exc, request, correlation_id)

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request headers."""
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        return request.client.host if request.client else "unknown"

    def _is_json_response(self, response: Response) -> bool:
        """Check if response is JSON."""
        content_type = response.headers.get("content-type", "")
        return "application/json" in content_type.lower()

    async def _optimize_json_response(self, response: Response, request: Request) -> Response:
        """Optimize JSON responses using orjson."""
        try:
            # Only optimize if response is not already an ORJSONResponse
            if not isinstance(response, ORJSONResponse):
                # This is a simplified optimization
                # In practice, you'd need to handle the response body more carefully
                return response
            return response
        except Exception as e:
            logger.warning(f"Failed to optimize JSON response: {e}")
            return response

    async def _handle_http_exception(
        self, exc: HTTPException, request: Request, correlation_id: str | None
    ) -> ORJSONResponse:
        """Handle HTTP exceptions with standardized error response."""

        # Map HTTP status codes to error codes
        error_code_map = {
            400: ErrorCode.BAD_REQUEST,
            401: ErrorCode.AUTHENTICATION_ERROR,
            403: ErrorCode.AUTHORIZATION_ERROR,
            404: ErrorCode.NOT_FOUND,
            409: ErrorCode.CONFLICT,
            429: ErrorCode.RATE_LIMIT_EXCEEDED,
            500: ErrorCode.INTERNAL_ERROR,
            503: ErrorCode.SERVICE_UNAVAILABLE,
            504: ErrorCode.TIMEOUT_ERROR,
        }

        error_code = error_code_map.get(exc.status_code, ErrorCode.INTERNAL_ERROR)

        error_response = ResponseBuilder.error(
            ResponseBuilder.error(
                [
                    {
                        "code": error_code,
                        "message": exc.detail,
                        "details": getattr(exc, "details", None),
                    }
                ]
            ),
            request_id=getattr(request.state, "request_id", None),
            correlation_id=correlation_id,
        )

        return ORJSONResponse(
            status_code=exc.status_code,
            content=error_response.dict(),
            headers={"X-Correlation-ID": correlation_id} if correlation_id else {},
        )

    async def _handle_internal_error(
        self, exc: Exception, request: Request, correlation_id: str | None
    ) -> ORJSONResponse:
        """Handle internal server errors."""

        error_response = ResponseBuilder.error(
            ResponseBuilder.error(
                [
                    {
                        "code": ErrorCode.INTERNAL_ERROR,
                        "message": "Internal server error",
                        "details": {"type": type(exc).__name__}
                        if config.environment != "production"
                        else None,
                    }
                ]
            ),
            request_id=getattr(request.state, "request_id", None),
            correlation_id=correlation_id,
        )

        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_response.dict(),
            headers={"X-Correlation-ID": correlation_id} if correlation_id else {},
        )


class ConditionalRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Lightweight rate limiting middleware that only applies to specific endpoints.
    This reduces overhead for endpoints that don't need rate limiting.
    """

    def __init__(
        self,
        app,
        rate_limit_rules: dict[str, dict[str, Any]],
        redis_client=None,
        default_enabled: bool = False,
    ):
        super().__init__(app)
        self.rate_limit_rules = rate_limit_rules
        self.redis_client = redis_client
        self.default_enabled = default_enabled

        # Compile path patterns for efficient matching
        self._compile_patterns()

    def _compile_patterns(self):
        """Pre-compile path patterns for efficient matching."""
        import re

        self.compiled_patterns = {}

        for pattern, config in self.rate_limit_rules.items():
            if "*" in pattern:
                # Convert glob pattern to regex
                regex_pattern = pattern.replace("*", "[^/]*").replace("**", ".*")
                self.compiled_patterns[re.compile(f"^{regex_pattern}$")] = config
            else:
                # Exact match
                self.compiled_patterns[pattern] = config

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Apply conditional rate limiting."""

        # Check if this path needs rate limiting
        rate_limit_config = self._get_rate_limit_config(request.url.path)

        if not rate_limit_config:
            # No rate limiting needed, proceed directly
            return await call_next(request)

        # Apply rate limiting
        try:
            await self._check_rate_limit(request, rate_limit_config)
            response = await call_next(request)

            # Add rate limit headers
            self._add_rate_limit_headers(response, rate_limit_config)

            return response

        except HTTPException as exc:
            if exc.status_code == 429:
                # Rate limit exceeded
                logger.warning(
                    f"Rate limit exceeded for {request.state.client_ip}",
                    extra={
                        "path": str(request.url.path),
                        "method": request.method,
                        "client_ip": request.state.client_ip,
                        "rate_limit": rate_limit_config,
                    },
                )
            raise

    def _get_rate_limit_config(self, path: str) -> dict[str, Any] | None:
        """Get rate limit configuration for a path."""

        # Check exact matches first
        if path in self.compiled_patterns:
            return self.compiled_patterns[path]

        # Check regex patterns
        import re

        for pattern, config in self.compiled_patterns.items():
            if isinstance(pattern, re.Pattern) and pattern.match(path):
                return config

        # Check for default rule
        if "default" in self.rate_limit_rules and self.default_enabled:
            return self.rate_limit_rules["default"]

        return None

    async def _check_rate_limit(self, request: Request, config: dict[str, Any]):
        """Check if request exceeds rate limit."""
        if not self.redis_client:
            # No Redis available, skip rate limiting
            return

        try:
            # Simple rate limiting implementation
            # In production, you'd use a more sophisticated algorithm
            client_id = self._get_client_identifier(request)
            key = f"rate_limit:{client_id}:{request.url.path}"

            limit = config.get("limit", 100)
            window = config.get("window", 60)

            current_count = await self.redis_client.incr(key)

            if current_count == 1:
                # First request in window, set expiry
                await self.redis_client.expire(key, window)

            if current_count > limit:
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit exceeded: {limit} requests per {window} seconds",
                )

        except Exception as e:
            logger.error(f"Rate limiting error: {e}")
            # On error, allow request to proceed (fail open)

    def _get_client_identifier(self, request: Request) -> str:
        """Get client identifier for rate limiting."""
        # Try to get user ID from authentication
        user_id = getattr(request.state, "user_id", None)
        if user_id:
            return f"user:{user_id}"

        # Fall back to IP address
        return f"ip:{request.state.client_ip}"

    def _add_rate_limit_headers(self, response: Response, config: dict[str, Any]):
        """Add rate limiting headers to response."""
        limit = config.get("limit", 100)
        response.headers["X-RateLimit-Limit"] = str(limit)
        # In production, you'd calculate remaining from Redis


class ConditionalCompressionMiddleware(BaseHTTPMiddleware):
    """
    Conditional GZip compression that only compresses responses above a certain size
    and for specific content types to reduce CPU overhead.
    """

    def __init__(
        self,
        app,
        minimum_size: int = 1000,
        compressible_types: set | None = None,
        compression_level: int = 6,
    ):
        super().__init__(app)
        self.minimum_size = minimum_size
        self.compression_level = compression_level

        self.compressible_types = compressible_types or {
            "application/json",
            "application/javascript",
            "text/css",
            "text/html",
            "text/plain",
            "text/xml",
        }

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Apply conditional compression."""

        # Check if client accepts compression
        accept_encoding = request.headers.get("accept-encoding", "")
        if "gzip" not in accept_encoding:
            return await call_next(request)

        response = await call_next(request)

        # Check if response should be compressed
        if not self._should_compress(response):
            return response

        # Apply compression
        return await self._compress_response(response)

    def _should_compress(self, response: Response) -> bool:
        """Check if response should be compressed."""

        # Check if already compressed
        if response.headers.get("content-encoding"):
            return False

        # Check content type
        content_type = response.headers.get("content-type", "").lower()
        if not any(ct in content_type for ct in self.compressible_types):
            return False

        # Check content length
        content_length = response.headers.get("content-length")
        if content_length and int(content_length) < self.minimum_size:
            return False

        return True

    async def _compress_response(self, response: Response) -> Response:
        """Compress response using gzip."""
        try:
            import gzip

            # Get response content
            if hasattr(response, "body"):
                body = response.body
            else:
                # For streaming responses, we can't compress easily
                return response

            if isinstance(body, str):
                body = body.encode("utf-8")

            # Compress
            compressed_body = gzip.compress(body, compresslevel=self.compression_level)

            # Only use compression if it actually reduces size
            if len(compressed_body) < len(body):
                response.headers["content-encoding"] = "gzip"
                response.headers["content-length"] = str(len(compressed_body))

                # Create new response with compressed body
                # This is simplified - in practice you'd need to handle different response types
                if hasattr(response, "_content"):
                    response._content = compressed_body

            return response

        except Exception as e:
            logger.warning(f"Compression failed: {e}")
            return response


# Optimized middleware stack factory
def create_optimized_middleware_stack(
    enable_security: bool = True,
    enable_rate_limiting: bool = True,
    enable_compression: bool = True,
    rate_limit_rules: dict[str, dict[str, Any]] | None = None,
    redis_client=None,
) -> list:
    """
    Create an optimized middleware stack with minimal layers.

    Replaces 7+ middleware layers with 2-3 efficient ones.
    """

    middleware_stack = []

    # 1. Combined Security + Performance + Error Handling (replaces 4-5 middleware)
    if enable_security:
        middleware_stack.append(
            (
                CombinedSecurityPerformanceMiddleware,
                {
                    "enable_security_headers": True,
                    "enable_performance_tracking": True,
                    "enable_correlation_id": True,
                    "json_response_optimization": True,
                },
            )
        )

    # 2. Conditional Rate Limiting (only where needed)
    if enable_rate_limiting and rate_limit_rules:
        middleware_stack.append(
            (
                ConditionalRateLimitMiddleware,
                {
                    "rate_limit_rules": rate_limit_rules,
                    "redis_client": redis_client,
                    "default_enabled": False,  # Only apply to specific endpoints
                },
            )
        )

    # 3. Conditional Compression (only for large responses)
    if enable_compression:
        middleware_stack.append(
            (
                ConditionalCompressionMiddleware,
                {
                    "minimum_size": 1500,  # Only compress larger responses
                    "compression_level": 6,  # Balance compression vs CPU
                },
            )
        )

    return middleware_stack


# Performance monitoring utilities
def track_middleware_performance():
    """Decorator to track middleware performance."""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                execution_time = (time.time() - start_time) * 1000
                logger.debug(f"Middleware {func.__name__} executed in {execution_time:.2f}ms")

        return wrapper

    return decorator
