"""
OpenTelemetry Tracer Wrapper
Provides simplified interface for creating and managing traces and spans.
"""
from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from typing import Any, TypeVar

try:
    from opentelemetry import trace
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.trace import Span, SpanKind, Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None
    Span = Any
    SpanKind = Any
    Status = Any
    StatusCode = Any

from core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])


class TracerWrapper:
    """Wrapper around OpenTelemetry tracer with enhanced functionality."""

    def __init__(self, service_name: str = "retail-etl-pipeline"):
        self.service_name = service_name
        self._tracer = None

        if OTEL_AVAILABLE:
            self._tracer = trace.get_tracer(service_name)

    @property
    def tracer(self):
        """Get the underlying OpenTelemetry tracer."""
        return self._tracer

    def is_available(self) -> bool:
        """Check if OpenTelemetry is available and configured."""
        return OTEL_AVAILABLE and self._tracer is not None

    @contextmanager
    def span(
        self,
        name: str,
        kind: str | None = None,
        attributes: dict[str, Any] | None = None
    ):
        """
        Create a span context manager.
        
        Args:
            name: Span name
            kind: Span kind (client, server, internal, producer, consumer)
            attributes: Initial span attributes
        """
        if not self.is_available():
            yield None
            return

        span_kind = self._get_span_kind(kind)

        with self._tracer.start_as_current_span(name, kind=span_kind) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            yield span

    @asynccontextmanager
    async def async_span(
        self,
        name: str,
        kind: str | None = None,
        attributes: dict[str, Any] | None = None
    ):
        """
        Create an async span context manager.
        
        Args:
            name: Span name
            kind: Span kind
            attributes: Initial span attributes
        """
        if not self.is_available():
            yield None
            return

        span_kind = self._get_span_kind(kind)

        with self._tracer.start_as_current_span(name, kind=span_kind) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            yield span

    def _get_span_kind(self, kind: str | None):
        """Convert string span kind to OpenTelemetry SpanKind."""
        if not OTEL_AVAILABLE or not kind:
            return None

        kind_mapping = {
            'client': SpanKind.CLIENT,
            'server': SpanKind.SERVER,
            'internal': SpanKind.INTERNAL,
            'producer': SpanKind.PRODUCER,
            'consumer': SpanKind.CONSUMER
        }

        return kind_mapping.get(kind.lower(), SpanKind.INTERNAL)

    def get_current_span(self) -> Span | None:
        """Get the current active span."""
        if not self.is_available():
            return None
        return trace.get_current_span()

    def get_current_trace_id(self) -> str | None:
        """Get the current trace ID as a string."""
        span = self.get_current_span()
        if span and span.get_span_context().is_valid:
            return format(span.get_span_context().trace_id, '032x')
        return None

    def get_current_span_id(self) -> str | None:
        """Get the current span ID as a string."""
        span = self.get_current_span()
        if span and span.get_span_context().is_valid:
            return format(span.get_span_context().span_id, '016x')
        return None

    def set_span_attribute(self, key: str, value: Any) -> None:
        """Set an attribute on the current span."""
        span = self.get_current_span()
        if span:
            span.set_attribute(key, value)

    def set_span_status(self, status: str, description: str | None = None) -> None:
        """Set the status of the current span."""
        span = self.get_current_span()
        if span and OTEL_AVAILABLE:
            status_code = StatusCode.OK if status.lower() == 'ok' else StatusCode.ERROR
            span.set_status(Status(status_code, description))

    def add_span_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        """Add an event to the current span."""
        span = self.get_current_span()
        if span:
            span.add_event(name, attributes or {})

    def record_exception(self, exception: Exception) -> None:
        """Record an exception in the current span."""
        span = self.get_current_span()
        if span:
            span.record_exception(exception)
            self.set_span_status('error', str(exception))


# Global tracer instance
_tracer: TracerWrapper | None = None


def get_tracer(service_name: str | None = None) -> TracerWrapper:
    """
    Get or create global tracer instance.
    
    Args:
        service_name: Service name for the tracer
        
    Returns:
        TracerWrapper instance
    """
    global _tracer

    if _tracer is None:
        _tracer = TracerWrapper(service_name or "retail-etl-pipeline")

    return _tracer


def get_trace_context() -> dict[str, str]:
    """
    Get current trace context as propagation headers.
    
    Returns:
        Dictionary of trace context headers
    """
    if not OTEL_AVAILABLE:
        return {}

    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)
    W3CBaggagePropagator().inject(carrier)

    return carrier


def create_span(
    name: str,
    kind: str | None = None,
    attributes: dict[str, Any] | None = None
):
    """
    Create a span context manager.
    
    Args:
        name: Span name
        kind: Span kind
        attributes: Initial attributes
    """
    tracer = get_tracer()
    return tracer.span(name, kind, attributes)


def set_span_attribute(key: str, value: Any) -> None:
    """Set an attribute on the current span."""
    tracer = get_tracer()
    tracer.set_span_attribute(key, value)


def set_span_status(status: str, description: str | None = None) -> None:
    """Set the status of the current span."""
    tracer = get_tracer()
    tracer.set_span_status(status, description)


def get_current_span_context() -> dict[str, str | None]:
    """
    Get current span context information.
    
    Returns:
        Dictionary with trace_id and span_id
    """
    tracer = get_tracer()
    return {
        'trace_id': tracer.get_current_trace_id(),
        'span_id': tracer.get_current_span_id()
    }


def trace_function(
    name: str | None = None,
    kind: str = "internal",
    attributes: dict[str, Any] | None = None
):
    """
    Decorator to trace function execution.
    
    Args:
        name: Span name (defaults to function name)
        kind: Span kind
        attributes: Additional attributes
    """
    def decorator(func: F) -> F:
        span_name = name or f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()

            with tracer.span(span_name, kind, attributes) as span:
                try:
                    # Add function metadata
                    if span:
                        span.set_attribute("function.name", func.__name__)
                        span.set_attribute("function.module", func.__module__)
                        if args:
                            span.set_attribute("function.args.count", len(args))
                        if kwargs:
                            span.set_attribute("function.kwargs.count", len(kwargs))

                    result = func(*args, **kwargs)

                    if span:
                        span.set_attribute("function.result.type", type(result).__name__)

                    return result

                except Exception as e:
                    if span:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper
    return decorator


def trace_async_function(
    name: str | None = None,
    kind: str = "internal",
    attributes: dict[str, Any] | None = None
):
    """
    Decorator to trace async function execution.
    
    Args:
        name: Span name (defaults to function name)
        kind: Span kind
        attributes: Additional attributes
    """
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        span_name = name or f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            tracer = get_tracer()

            async with tracer.async_span(span_name, kind, attributes) as span:
                try:
                    # Add function metadata
                    if span:
                        span.set_attribute("function.name", func.__name__)
                        span.set_attribute("function.module", func.__module__)
                        span.set_attribute("function.async", True)
                        if args:
                            span.set_attribute("function.args.count", len(args))
                        if kwargs:
                            span.set_attribute("function.kwargs.count", len(kwargs))

                    result = await func(*args, **kwargs)

                    if span:
                        span.set_attribute("function.result.type", type(result).__name__)

                    return result

                except Exception as e:
                    if span:
                        span.record_exception(e)
                        tracer.set_span_status('error', str(e))
                    raise

        return wrapper
    return decorator


# Convenience functions for common tracing patterns
def trace_etl_operation(operation_name: str, stage: str = "unknown"):
    """Decorator for tracing ETL operations."""
    return trace_function(
        name=f"etl.{stage}.{operation_name}",
        kind="internal",
        attributes={
            "etl.operation": operation_name,
            "etl.stage": stage,
            "etl.type": "batch"
        }
    )


def trace_api_request(endpoint: str, method: str = "GET"):
    """Decorator for tracing API requests."""
    return trace_function(
        name=f"api.{method.lower()}.{endpoint}",
        kind="server",
        attributes={
            "http.method": method,
            "http.endpoint": endpoint,
            "component": "api"
        }
    )


def trace_database_operation(operation: str, table: str | None = None):
    """Decorator for tracing database operations."""
    attributes = {
        "db.operation": operation,
        "component": "database"
    }
    if table:
        attributes["db.table"] = table

    return trace_function(
        name=f"db.{operation}",
        kind="client",
        attributes=attributes
    )
