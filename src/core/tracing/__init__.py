"""
Distributed Tracing Module
Provides OpenTelemetry-based distributed tracing with correlation ID support.
"""

from .correlation import (
    CorrelationContext,
    correlation_context,
    get_correlation_id,
    get_trace_id,
    propagate_context,
    set_correlation_id,
    set_trace_id,
)
from .instrumentation import (
    auto_instrument_all,
    instrument_fastapi,
    instrument_redis,
    instrument_requests,
    instrument_sqlalchemy,
)
from .otel_config import TracingConfig, configure_opentelemetry, get_otel_config, shutdown_tracing
from .tracer import (
    create_span,
    get_current_span_context,
    get_trace_context,
    get_tracer,
    set_span_attribute,
    set_span_status,
    trace_async_function,
    trace_function,
)

__all__ = [
    # Tracer functions
    'get_tracer',
    'get_trace_context',
    'create_span',
    'set_span_attribute',
    'set_span_status',
    'get_current_span_context',
    'trace_function',
    'trace_async_function',

    # Configuration
    'configure_opentelemetry',
    'get_otel_config',
    'TracingConfig',
    'shutdown_tracing',

    # Correlation
    'CorrelationContext',
    'get_correlation_id',
    'set_correlation_id',
    'get_trace_id',
    'set_trace_id',
    'propagate_context',
    'correlation_context',

    # Instrumentation
    'instrument_fastapi',
    'instrument_sqlalchemy',
    'instrument_requests',
    'instrument_redis',
    'auto_instrument_all'
]
