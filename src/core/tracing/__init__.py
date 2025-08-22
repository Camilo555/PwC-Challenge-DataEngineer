"""
Distributed Tracing Module
Provides OpenTelemetry-based distributed tracing with correlation ID support.
"""

from .tracer import (
    get_tracer,
    get_trace_context,
    create_span,
    set_span_attribute,
    set_span_status,
    get_current_span_context,
    trace_function,
    trace_async_function
)

from .otel_config import (
    configure_opentelemetry,
    get_otel_config,
    TracingConfig,
    shutdown_tracing
)

from .correlation import (
    CorrelationContext,
    get_correlation_id,
    set_correlation_id,
    get_trace_id,
    set_trace_id,
    propagate_context,
    correlation_context
)

from .instrumentation import (
    instrument_fastapi,
    instrument_sqlalchemy,
    instrument_requests,
    instrument_redis,
    auto_instrument_all
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