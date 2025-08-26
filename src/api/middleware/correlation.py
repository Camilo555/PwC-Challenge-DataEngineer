"""
Correlation ID Middleware
Provides automatic correlation ID propagation for FastAPI applications with logging integration.
Implements distributed tracing patterns and request context management.
"""
from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

try:
    from fastapi import FastAPI, Request, Response
    from fastapi.middleware.base import BaseHTTPMiddleware
    from starlette.responses import Response as StarletteResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    BaseHTTPMiddleware = object
    Request = Any
    Response = Any

from monitoring.logging import (
    CorrelationIdManager,
    correlation_id_context,
    get_structured_logger,
    trace_id_context,
    user_id_context,
)


class CorrelationMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware for automatic correlation ID management.
    
    Features:
    - Automatic correlation ID generation and propagation
    - Support for distributed tracing headers
    - Request/response timing and metrics
    - Integration with structured logging
    - User context extraction from JWT tokens
    """

    # Standard correlation headers
    CORRELATION_ID_HEADER = "x-correlation-id"
    TRACE_ID_HEADER = "x-trace-id"
    USER_ID_HEADER = "x-user-id"
    REQUEST_ID_HEADER = "x-request-id"

    # Distributed tracing headers (OpenTelemetry compatible)
    OTEL_TRACE_ID = "traceparent"
    OTEL_SPAN_ID = "tracestate"

    # Additional headers to propagate
    PROPAGATION_HEADERS = [
        "x-forwarded-for",
        "x-real-ip",
        "user-agent",
        "x-api-key",
        "authorization"
    ]

    def __init__(
        self,
        app: Any | None = None,
        correlation_manager: CorrelationIdManager | None = None,
        include_request_body: bool = False,
        include_response_body: bool = False,
        sensitive_headers: list[str] | None = None,
        max_body_size: int = 1024 * 10,  # 10KB
        exclude_paths: list[str] | None = None
    ):
        """
        Initialize correlation middleware.
        
        Args:
            app: FastAPI application instance
            correlation_manager: Custom correlation manager
            include_request_body: Whether to log request bodies
            include_response_body: Whether to log response bodies
            sensitive_headers: Headers to exclude from logging
            max_body_size: Maximum body size to log (bytes)
            exclude_paths: Paths to exclude from correlation tracking
        """
        if app:
            super().__init__(app)

        self.correlation_manager = correlation_manager or CorrelationIdManager()
        self.include_request_body = include_request_body
        self.include_response_body = include_response_body
        self.max_body_size = max_body_size
        self.exclude_paths = exclude_paths or ["/health", "/metrics", "/favicon.ico"]

        # Headers to exclude from logging (PII/sensitive data)
        self.sensitive_headers = set((sensitive_headers or []) + [
            "authorization",
            "x-api-key",
            "cookie",
            "set-cookie",
            "x-auth-token",
            "x-access-token"
        ])

        # Setup logger
        self.logger = get_structured_logger("api.middleware.correlation")

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with correlation tracking."""

        # Skip excluded paths
        if request.url.path in self.exclude_paths:
            return await call_next(request)

        # Extract or generate correlation context
        correlation_context = self._extract_correlation_context(request)

        # Set context variables
        correlation_id_token = correlation_id_context.set(correlation_context["correlation_id"])
        trace_id_token = trace_id_context.set(correlation_context.get("trace_id"))
        user_id_token = user_id_context.set(correlation_context.get("user_id"))

        start_time = time.time()
        request_timestamp = datetime.utcnow()

        try:
            # Log request start
            await self._log_request_start(request, correlation_context, request_timestamp)

            # Process request
            response = await call_next(request)

            # Calculate timing
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)

            # Add correlation headers to response
            response = self._add_correlation_headers(response, correlation_context)

            # Log request completion
            await self._log_request_completion(
                request, response, correlation_context, duration_ms, request_timestamp
            )

            return response

        except Exception as e:
            # Calculate timing for error case
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)

            # Log request error
            await self._log_request_error(
                request, e, correlation_context, duration_ms, request_timestamp
            )

            raise

        finally:
            # Clean up context
            correlation_id_context.reset(correlation_id_token)
            if trace_id_token:
                trace_id_context.reset(trace_id_token)
            if user_id_token:
                user_id_context.reset(user_id_token)

    def _extract_correlation_context(self, request: Request) -> dict[str, str | None]:
        """Extract correlation context from request headers."""

        headers = request.headers

        # Extract correlation ID
        correlation_id = (
            headers.get(self.CORRELATION_ID_HEADER) or
            headers.get(self.REQUEST_ID_HEADER) or
            self.correlation_manager.generate_correlation_id()
        )

        # Extract trace ID
        trace_id = headers.get(self.TRACE_ID_HEADER)
        if not trace_id and headers.get(self.OTEL_TRACE_ID):
            # Extract from OpenTelemetry traceparent header
            traceparent = headers.get(self.OTEL_TRACE_ID)
            if traceparent and len(traceparent.split('-')) >= 2:
                trace_id = traceparent.split('-')[1][:16]  # Use first 16 chars of trace ID

        if not trace_id:
            trace_id = self.correlation_manager.generate_trace_id()

        # Extract user ID (from header or JWT token)
        user_id = headers.get(self.USER_ID_HEADER)
        if not user_id:
            user_id = self._extract_user_from_jwt(headers.get("authorization"))

        return {
            "correlation_id": correlation_id,
            "trace_id": trace_id,
            "user_id": user_id
        }

    def _extract_user_from_jwt(self, auth_header: str | None) -> str | None:
        """Extract user ID from JWT token (simplified implementation)."""
        if not auth_header or not auth_header.startswith("Bearer "):
            return None

        try:
            import base64
            import json

            # Extract token
            token = auth_header.replace("Bearer ", "")

            # Decode payload (without verification - just for user ID extraction)
            # In production, use proper JWT library with signature verification
            parts = token.split('.')
            if len(parts) >= 2:
                # Decode payload
                payload = parts[1]
                # Add padding if needed
                payload += '=' * (4 - len(payload) % 4)
                decoded = base64.urlsafe_b64decode(payload)
                claims = json.loads(decoded)

                # Extract user identifier (common claim names)
                return (
                    claims.get('sub') or
                    claims.get('user_id') or
                    claims.get('uid') or
                    claims.get('email')
                )

        except Exception:
            # JWT parsing failed, return None
            pass

        return None

    def _add_correlation_headers(self, response: Response, context: dict[str, str | None]) -> Response:
        """Add correlation headers to response."""

        if hasattr(response, 'headers'):
            response.headers[self.CORRELATION_ID_HEADER] = context["correlation_id"]

            if context.get("trace_id"):
                response.headers[self.TRACE_ID_HEADER] = context["trace_id"]

            # Add timing header
            response.headers["x-response-time"] = str(int(time.time() * 1000))

        return response

    async def _log_request_start(
        self,
        request: Request,
        context: dict[str, str | None],
        timestamp: datetime
    ) -> None:
        """Log request start with context."""

        # Extract safe request information
        request_info = {
            "http_method": request.method,
            "http_url": str(request.url),
            "http_path": request.url.path,
            "http_query": str(request.url.query) if request.url.query else None,
            "http_headers": self._safe_headers(dict(request.headers)),
            "client_ip": self._get_client_ip(request),
            "user_agent": request.headers.get("user-agent"),
            "request_timestamp": timestamp.isoformat(),
            "event": "request_start",
            **{k: v for k, v in context.items() if v is not None}
        }

        # Add request body if configured
        if self.include_request_body and request.method in ["POST", "PUT", "PATCH"]:
            try:
                body = await self._get_request_body(request)
                if body:
                    request_info["request_body"] = body
            except Exception:
                request_info["request_body"] = "[BODY_READ_ERROR]"

        self.logger.info("HTTP request started", extra=request_info)

    async def _log_request_completion(
        self,
        request: Request,
        response: Response,
        context: dict[str, str | None],
        duration_ms: int,
        request_timestamp: datetime
    ) -> None:
        """Log request completion with metrics."""

        response_info = {
            "http_method": request.method,
            "http_url": str(request.url),
            "http_path": request.url.path,
            "http_status_code": response.status_code,
            "http_status_text": getattr(response, 'status_text', ''),
            "duration_ms": duration_ms,
            "request_timestamp": request_timestamp.isoformat(),
            "response_timestamp": datetime.utcnow().isoformat(),
            "event": "request_completed",
            **{k: v for k, v in context.items() if v is not None}
        }

        # Add response body if configured and status indicates success
        if (self.include_response_body and
            hasattr(response, 'body') and
            200 <= response.status_code < 300):
            try:
                body = await self._get_response_body(response)
                if body:
                    response_info["response_body"] = body
            except Exception:
                response_info["response_body"] = "[BODY_READ_ERROR]"

        # Determine log level based on status code
        if response.status_code >= 500:
            log_level = "error"
        elif response.status_code >= 400:
            log_level = "warning"
        else:
            log_level = "info"

        getattr(self.logger, log_level)("HTTP request completed", extra=response_info)

    async def _log_request_error(
        self,
        request: Request,
        error: Exception,
        context: dict[str, str | None],
        duration_ms: int,
        request_timestamp: datetime
    ) -> None:
        """Log request error."""

        error_info = {
            "http_method": request.method,
            "http_url": str(request.url),
            "http_path": request.url.path,
            "duration_ms": duration_ms,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "request_timestamp": request_timestamp.isoformat(),
            "error_timestamp": datetime.utcnow().isoformat(),
            "event": "request_error",
            **{k: v for k, v in context.items() if v is not None}
        }

        self.logger.error("HTTP request failed", extra=error_info, exc_info=True)

    def _safe_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Filter out sensitive headers for logging."""
        return {
            k: v if k.lower() not in self.sensitive_headers else "[REDACTED]"
            for k, v in headers.items()
        }

    def _get_client_ip(self, request: Request) -> str | None:
        """Extract client IP from request headers."""

        # Check common proxy headers
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            # Take the first IP in the chain
            return forwarded_for.split(',')[0].strip()

        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        # Fallback to client address
        if hasattr(request, 'client') and request.client:
            return request.client.host

        return None

    async def _get_request_body(self, request: Request) -> str | None:
        """Safely extract request body for logging."""

        try:
            # Check content length
            content_length = int(request.headers.get("content-length", 0))
            if content_length > self.max_body_size:
                return f"[BODY_TOO_LARGE_{content_length}_BYTES]"

            # Read body
            body = await request.body()

            if not body:
                return None

            # Decode based on content type
            content_type = request.headers.get("content-type", "")

            if "application/json" in content_type:
                return body.decode('utf-8')
            elif "application/x-www-form-urlencoded" in content_type:
                return body.decode('utf-8')
            elif "text/" in content_type:
                return body.decode('utf-8')
            else:
                return f"[BINARY_CONTENT_{len(body)}_BYTES]"

        except Exception:
            return "[BODY_READ_ERROR]"

    async def _get_response_body(self, response: Response) -> str | None:
        """Safely extract response body for logging."""

        try:
            if not hasattr(response, 'body'):
                return None

            body = response.body
            if isinstance(body, bytes):
                if len(body) > self.max_body_size:
                    return f"[RESPONSE_TOO_LARGE_{len(body)}_BYTES]"
                return body.decode('utf-8')
            elif isinstance(body, str):
                if len(body) > self.max_body_size:
                    return f"[RESPONSE_TOO_LARGE_{len(body)}_CHARS]"
                return body

            return str(body)[:self.max_body_size]

        except Exception:
            return "[RESPONSE_READ_ERROR]"


# Utility functions for manual correlation management

def get_correlation_context() -> dict[str, str | None]:
    """Get current correlation context from context variables."""
    return {
        "correlation_id": correlation_id_context.get(),
        "trace_id": trace_id_context.get(),
        "user_id": user_id_context.get()
    }


def extract_correlation_headers(headers: dict[str, str]) -> dict[str, str | None]:
    """Extract correlation headers from header dictionary."""

    correlation_id = (
        headers.get("x-correlation-id") or
        headers.get("x-request-id") or
        str(uuid.uuid4())
    )

    trace_id = headers.get("x-trace-id")
    user_id = headers.get("x-user-id")

    return {
        "correlation_id": correlation_id,
        "trace_id": trace_id,
        "user_id": user_id
    }


def add_correlation_headers(
    headers: dict[str, str],
    context: dict[str, str | None] | None = None
) -> dict[str, str]:
    """Add correlation headers to header dictionary."""

    if context is None:
        context = get_correlation_context()

    if context.get("correlation_id"):
        headers["x-correlation-id"] = context["correlation_id"]

    if context.get("trace_id"):
        headers["x-trace-id"] = context["trace_id"]

    if context.get("user_id"):
        headers["x-user-id"] = context["user_id"]

    return headers


# FastAPI integration helper

def setup_correlation_middleware(
    app: Any,  # FastAPI app
    **middleware_kwargs
) -> CorrelationMiddleware:
    """
    Setup correlation middleware on FastAPI application.
    
    Args:
        app: FastAPI application instance
        **middleware_kwargs: Arguments to pass to CorrelationMiddleware
    
    Returns:
        Configured middleware instance
    """

    if not FASTAPI_AVAILABLE:
        raise ImportError("FastAPI is required for correlation middleware")

    middleware = CorrelationMiddleware(**middleware_kwargs)
    app.add_middleware(BaseHTTPMiddleware, dispatch=middleware.dispatch)

    return middleware
