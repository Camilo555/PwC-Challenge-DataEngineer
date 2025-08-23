"""
Correlation ID and Context Management
Provides correlation ID generation, propagation, and context management for distributed tracing.
"""

import contextvars
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)

# Context variables for correlation data
correlation_id_context: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'correlation_id', default=None
)

trace_id_context: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'trace_id', default=None
)

user_id_context: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'user_id', default=None
)

request_id_context: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'request_id', default=None
)

operation_context: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'operation', default=None
)


class CorrelationContext:
    """
    Context manager for correlation data that integrates with OpenTelemetry.
    Maintains correlation IDs and propagates them through the call stack.
    """

    def __init__(
        self,
        correlation_id: str | None = None,
        trace_id: str | None = None,
        user_id: str | None = None,
        request_id: str | None = None,
        operation: str | None = None,
        **additional_context
    ):
        self.correlation_id = correlation_id or generate_correlation_id()
        self.trace_id = trace_id
        self.user_id = user_id
        self.request_id = request_id or generate_request_id()
        self.operation = operation
        self.additional_context = additional_context

        # Store original context values for restoration
        self._original_values = {}

    def __enter__(self):
        """Enter correlation context."""
        # Store original values
        self._original_values = {
            'correlation_id': correlation_id_context.get(None),
            'trace_id': trace_id_context.get(None),
            'user_id': user_id_context.get(None),
            'request_id': request_id_context.get(None),
            'operation': operation_context.get(None)
        }

        # Set new values
        correlation_id_context.set(self.correlation_id)
        if self.trace_id:
            trace_id_context.set(self.trace_id)
        if self.user_id:
            user_id_context.set(self.user_id)
        if self.request_id:
            request_id_context.set(self.request_id)
        if self.operation:
            operation_context.set(self.operation)

        # Add to OpenTelemetry span if available
        self._add_to_current_span()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit correlation context and restore original values."""
        # Restore original values
        correlation_id_context.set(self._original_values['correlation_id'])
        trace_id_context.set(self._original_values['trace_id'])
        user_id_context.set(self._original_values['user_id'])
        request_id_context.set(self._original_values['request_id'])
        operation_context.set(self._original_values['operation'])

    def _add_to_current_span(self):
        """Add correlation data to current OpenTelemetry span."""
        try:
            from .tracer import get_tracer
            tracer = get_tracer()

            if tracer.is_available():
                tracer.set_span_attribute("correlation.id", self.correlation_id)
                if self.user_id:
                    tracer.set_span_attribute("user.id", self.user_id)
                if self.request_id:
                    tracer.set_span_attribute("request.id", self.request_id)
                if self.operation:
                    tracer.set_span_attribute("operation.name", self.operation)

                # Add additional context
                for key, value in self.additional_context.items():
                    tracer.set_span_attribute(f"context.{key}", value)

        except ImportError:
            # OpenTelemetry not available, continue without tracing
            pass
        except Exception as e:
            logger.debug(f"Failed to add correlation data to span: {e}")

    def to_dict(self) -> dict[str, Any]:
        """Convert correlation context to dictionary."""
        data = {
            'correlation_id': self.correlation_id,
            'request_id': self.request_id
        }

        if self.trace_id:
            data['trace_id'] = self.trace_id
        if self.user_id:
            data['user_id'] = self.user_id
        if self.operation:
            data['operation'] = self.operation

        data.update(self.additional_context)
        return data

    def to_headers(self) -> dict[str, str]:
        """Convert correlation context to HTTP headers."""
        headers = {
            'X-Correlation-ID': self.correlation_id,
            'X-Request-ID': self.request_id
        }

        if self.trace_id:
            headers['X-Trace-ID'] = self.trace_id
        if self.user_id:
            headers['X-User-ID'] = self.user_id
        if self.operation:
            headers['X-Operation'] = self.operation

        return headers

    @classmethod
    def from_headers(cls, headers: dict[str, str]) -> 'CorrelationContext':
        """Create correlation context from HTTP headers."""
        return cls(
            correlation_id=headers.get('X-Correlation-ID') or headers.get('x-correlation-id'),
            trace_id=headers.get('X-Trace-ID') or headers.get('x-trace-id'),
            user_id=headers.get('X-User-ID') or headers.get('x-user-id'),
            request_id=headers.get('X-Request-ID') or headers.get('x-request-id'),
            operation=headers.get('X-Operation') or headers.get('x-operation')
        )


# Utility functions for correlation management
def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())


def generate_request_id() -> str:
    """Generate a new request ID."""
    return str(uuid.uuid4())


def get_correlation_id() -> str | None:
    """Get the current correlation ID from context."""
    return correlation_id_context.get(None)


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID in current context."""
    correlation_id_context.set(correlation_id)


def get_trace_id() -> str | None:
    """Get the current trace ID from context."""
    trace_id = trace_id_context.get(None)
    if not trace_id:
        # Try to get from OpenTelemetry
        try:
            from .tracer import get_tracer
            tracer = get_tracer()
            trace_id = tracer.get_current_trace_id()
            if trace_id:
                set_trace_id(trace_id)
        except ImportError:
            pass
    return trace_id


def set_trace_id(trace_id: str) -> None:
    """Set the trace ID in current context."""
    trace_id_context.set(trace_id)


def get_user_id() -> str | None:
    """Get the current user ID from context."""
    return user_id_context.get(None)


def set_user_id(user_id: str) -> None:
    """Set the user ID in current context."""
    user_id_context.set(user_id)


def get_request_id() -> str | None:
    """Get the current request ID from context."""
    return request_id_context.get(None)


def set_request_id(request_id: str) -> None:
    """Set the request ID in current context."""
    request_id_context.set(request_id)


def get_operation() -> str | None:
    """Get the current operation from context."""
    return operation_context.get(None)


def set_operation(operation: str) -> None:
    """Set the operation in current context."""
    operation_context.set(operation)


def get_correlation_context() -> dict[str, str | None]:
    """
    Get all correlation context data.
    
    Returns:
        Dictionary with correlation data
    """
    return {
        'correlation_id': get_correlation_id(),
        'trace_id': get_trace_id(),
        'user_id': get_user_id(),
        'request_id': get_request_id(),
        'operation': get_operation()
    }


@contextmanager
def correlation_context(
    correlation_id: str | None = None,
    trace_id: str | None = None,
    user_id: str | None = None,
    request_id: str | None = None,
    operation: str | None = None,
    **kwargs
) -> Generator[CorrelationContext, None, None]:
    """
    Context manager for correlation data.
    
    Args:
        correlation_id: Correlation ID
        trace_id: Trace ID
        user_id: User ID
        request_id: Request ID
        operation: Operation name
        **kwargs: Additional context data
    """
    context = CorrelationContext(
        correlation_id=correlation_id,
        trace_id=trace_id,
        user_id=user_id,
        request_id=request_id,
        operation=operation,
        **kwargs
    )

    with context:
        yield context


def propagate_context() -> dict[str, str]:
    """
    Get correlation context as propagation headers.
    
    Returns:
        Dictionary of headers for context propagation
    """
    context = get_correlation_context()
    headers = {}

    if context['correlation_id']:
        headers['X-Correlation-ID'] = context['correlation_id']
    if context['trace_id']:
        headers['X-Trace-ID'] = context['trace_id']
    if context['user_id']:
        headers['X-User-ID'] = context['user_id']
    if context['request_id']:
        headers['X-Request-ID'] = context['request_id']
    if context['operation']:
        headers['X-Operation'] = context['operation']

    # Add OpenTelemetry trace context if available
    try:
        from .tracer import get_trace_context
        otel_headers = get_trace_context()
        headers.update(otel_headers)
    except ImportError:
        pass

    return headers


def extract_context_from_headers(headers: dict[str, str]) -> CorrelationContext:
    """
    Extract correlation context from HTTP headers.
    
    Args:
        headers: HTTP headers dictionary
        
    Returns:
        CorrelationContext instance
    """
    return CorrelationContext.from_headers(headers)


# Integration with logging
def get_correlation_fields() -> dict[str, Any]:
    """
    Get correlation fields for structured logging.
    
    Returns:
        Dictionary of correlation fields
    """
    fields = {}

    correlation_id = get_correlation_id()
    if correlation_id:
        fields['correlation_id'] = correlation_id

    trace_id = get_trace_id()
    if trace_id:
        fields['trace_id'] = trace_id

    user_id = get_user_id()
    if user_id:
        fields['user_id'] = user_id

    request_id = get_request_id()
    if request_id:
        fields['request_id'] = request_id

    operation = get_operation()
    if operation:
        fields['operation'] = operation

    return fields
