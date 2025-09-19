"""
Distributed Tracing Implementation with Jaeger
==============================================

Comprehensive distributed tracing platform providing:
- End-to-end request tracing across microservices
- Performance monitoring and bottleneck identification
- Service dependency mapping and visualization
- Error tracking and debugging capabilities
- Custom span and tag instrumentation
- Integration with OpenTelemetry standards
- Real-time trace analytics and alerting

Key Features:
- Automatic instrumentation for FastAPI, SQLAlchemy, Redis, and HTTP requests
- Custom business logic tracing with context propagation
- Performance metrics and SLA monitoring
- Trace sampling and optimization for high-volume environments
- Integration with Prometheus metrics and Grafana dashboards
- Advanced search and filtering capabilities
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import traceback
from contextlib import asynccontextmanager
import threading
from collections import defaultdict

import jaeger_client
from jaeger_client import Config as JaegerConfig
from jaeger_client.constants import TRACE_ID_HEADER, SPAN_ID_HEADER
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat

import requests
from fastapi import Request, Response
from fastapi.middleware.base import BaseHTTPMiddleware
import sqlalchemy
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SpanKind(Enum):
    """Span types for different operations"""
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    INTERNAL = "internal"


class TraceStatus(Enum):
    """Trace completion status"""
    OK = "ok"
    ERROR = "error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class SpanContext:
    """Span context information"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    baggage: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TraceSpan:
    """Distributed trace span"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    service_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    status: TraceStatus = TraceStatus.OK
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    kind: SpanKind = SpanKind.INTERNAL


@dataclass
class DistributedTrace:
    """Complete distributed trace"""
    trace_id: str
    spans: List[TraceSpan]
    services: List[str]
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration_ms: Optional[float] = None
    status: TraceStatus = TraceStatus.OK
    error_count: int = 0
    span_count: int = 0


class TracingConfig(BaseModel):
    """Configuration for distributed tracing"""
    service_name: str = "pwc-data-engineering"
    jaeger_host: str = "localhost"
    jaeger_port: int = 14268
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 6831

    # Sampling configuration
    sampling_rate: float = 1.0  # 100% sampling for development
    max_traces_per_second: int = 100
    adaptive_sampling: bool = True

    # Performance configuration
    max_span_count: int = 1000
    max_tag_value_length: int = 1024
    flush_interval_seconds: int = 5

    # Integration configuration
    enable_fastapi_instrumentation: bool = True
    enable_sqlalchemy_instrumentation: bool = True
    enable_redis_instrumentation: bool = True
    enable_requests_instrumentation: bool = True

    # Custom instrumentation
    enable_business_tracing: bool = True
    enable_performance_monitoring: bool = True
    enable_error_tracking: bool = True


class JaegerTracingProvider:
    """Jaeger-based distributed tracing provider"""

    def __init__(self, config: TracingConfig):
        self.config = config
        self.tracer = None
        self.jaeger_tracer = None
        self.active_spans = {}
        self.trace_cache = {}
        self.metrics_collector = TracingMetricsCollector()
        self.logger = logging.getLogger(__name__)

        # Initialize tracing
        self._initialize_jaeger()
        self._initialize_opentelemetry()

        # Auto-instrument if enabled
        if config.enable_fastapi_instrumentation:
            FastAPIInstrumentor().instrument()

        if config.enable_sqlalchemy_instrumentation:
            SQLAlchemyInstrumentor().instrument()

        if config.enable_redis_instrumentation:
            RedisInstrumentor().instrument()

        if config.enable_requests_instrumentation:
            RequestsInstrumentor().instrument()

    def _initialize_jaeger(self):
        """Initialize Jaeger tracer"""
        try:
            jaeger_config = JaegerConfig(
                config={
                    'sampler': {
                        'type': 'probabilistic',
                        'param': self.config.sampling_rate,
                    },
                    'local_agent': {
                        'reporting_host': self.config.jaeger_agent_host,
                        'reporting_port': self.config.jaeger_agent_port,
                    },
                    'logging': True,
                },
                service_name=self.config.service_name,
                validate=True,
            )

            self.jaeger_tracer = jaeger_config.initialize_tracer()
            self.logger.info("Jaeger tracer initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize Jaeger tracer: {e}")
            raise

    def _initialize_opentelemetry(self):
        """Initialize OpenTelemetry provider"""
        try:
            # Set up tracer provider
            trace.set_tracer_provider(TracerProvider())

            # Create Jaeger exporter
            jaeger_exporter = JaegerExporter(
                agent_host_name=self.config.jaeger_agent_host,
                agent_port=self.config.jaeger_agent_port,
            )

            # Create span processor
            span_processor = BatchSpanProcessor(jaeger_exporter)
            trace.get_tracer_provider().add_span_processor(span_processor)

            # Set up propagator
            set_global_textmap(B3MultiFormat())

            # Get tracer
            self.tracer = trace.get_tracer(self.config.service_name)

            self.logger.info("OpenTelemetry tracer initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize OpenTelemetry: {e}")
            raise

    def start_span(self,
                   operation_name: str,
                   parent_context: Optional[SpanContext] = None,
                   tags: Optional[Dict[str, Any]] = None,
                   kind: SpanKind = SpanKind.INTERNAL) -> TraceSpan:
        """Start a new distributed trace span"""
        try:
            # Generate span IDs
            trace_id = parent_context.trace_id if parent_context else self._generate_trace_id()
            span_id = self._generate_span_id()
            parent_span_id = parent_context.span_id if parent_context else None

            # Create span
            span = TraceSpan(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
                operation_name=operation_name,
                service_name=self.config.service_name,
                start_time=datetime.now(),
                kind=kind,
                tags=tags or {}
            )

            # Add to active spans
            self.active_spans[span_id] = span

            # Start OpenTelemetry span
            if self.tracer:
                otel_span = self.tracer.start_span(operation_name)
                if tags:
                    for key, value in tags.items():
                        otel_span.set_attribute(key, str(value))

            # Collect metrics
            self.metrics_collector.span_started(operation_name, self.config.service_name)

            self.logger.debug(f"Started span: {operation_name} (ID: {span_id})")
            return span

        except Exception as e:
            self.logger.error(f"Failed to start span {operation_name}: {e}")
            raise

    def finish_span(self,
                    span: TraceSpan,
                    status: TraceStatus = TraceStatus.OK,
                    error: Optional[Exception] = None):
        """Finish a distributed trace span"""
        try:
            # Update span
            span.end_time = datetime.now()
            span.duration_ms = (span.end_time - span.start_time).total_seconds() * 1000
            span.status = status

            # Add error information if present
            if error:
                span.tags['error'] = True
                span.tags['error.message'] = str(error)
                span.tags['error.type'] = type(error).__name__
                span.logs.append({
                    'timestamp': datetime.now(),
                    'level': 'error',
                    'message': str(error),
                    'traceback': traceback.format_exc()
                })

            # Remove from active spans
            if span.span_id in self.active_spans:
                del self.active_spans[span.span_id]

            # Cache completed span
            if span.trace_id not in self.trace_cache:
                self.trace_cache[span.trace_id] = []
            self.trace_cache[span.trace_id].append(span)

            # Collect metrics
            self.metrics_collector.span_finished(
                span.operation_name,
                span.service_name,
                span.duration_ms,
                status == TraceStatus.ERROR
            )

            self.logger.debug(f"Finished span: {span.operation_name} (Duration: {span.duration_ms:.2f}ms)")

        except Exception as e:
            self.logger.error(f"Failed to finish span {span.span_id}: {e}")

    @asynccontextmanager
    async def trace_operation(self,
                            operation_name: str,
                            parent_context: Optional[SpanContext] = None,
                            tags: Optional[Dict[str, Any]] = None,
                            kind: SpanKind = SpanKind.INTERNAL):
        """Context manager for tracing operations"""
        span = self.start_span(operation_name, parent_context, tags, kind)
        try:
            yield span
            self.finish_span(span, TraceStatus.OK)
        except Exception as e:
            self.finish_span(span, TraceStatus.ERROR, e)
            raise

    def trace_function(self,
                      operation_name: Optional[str] = None,
                      tags: Optional[Dict[str, Any]] = None,
                      kind: SpanKind = SpanKind.INTERNAL):
        """Decorator for tracing functions"""
        def decorator(func):
            async def async_wrapper(*args, **kwargs):
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                async with self.trace_operation(op_name, tags=tags, kind=kind):
                    return await func(*args, **kwargs)

            def sync_wrapper(*args, **kwargs):
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                span = self.start_span(op_name, tags=tags, kind=kind)
                try:
                    result = func(*args, **kwargs)
                    self.finish_span(span, TraceStatus.OK)
                    return result
                except Exception as e:
                    self.finish_span(span, TraceStatus.ERROR, e)
                    raise

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        return decorator

    def add_span_tag(self, span: TraceSpan, key: str, value: Any):
        """Add tag to span"""
        span.tags[key] = value

    def add_span_log(self, span: TraceSpan, level: str, message: str, **kwargs):
        """Add log entry to span"""
        span.logs.append({
            'timestamp': datetime.now(),
            'level': level,
            'message': message,
            **kwargs
        })

    def get_trace(self, trace_id: str) -> Optional[DistributedTrace]:
        """Get complete distributed trace"""
        if trace_id not in self.trace_cache:
            return None

        spans = self.trace_cache[trace_id]
        services = list(set(span.service_name for span in spans))

        # Calculate trace metrics
        start_time = min(span.start_time for span in spans)
        end_time = max(span.end_time for span in spans if span.end_time)
        total_duration = (end_time - start_time).total_seconds() * 1000 if end_time else None

        error_count = sum(1 for span in spans if span.status == TraceStatus.ERROR)
        overall_status = TraceStatus.ERROR if error_count > 0 else TraceStatus.OK

        return DistributedTrace(
            trace_id=trace_id,
            spans=spans,
            services=services,
            start_time=start_time,
            end_time=end_time,
            total_duration_ms=total_duration,
            status=overall_status,
            error_count=error_count,
            span_count=len(spans)
        )

    def search_traces(self,
                     service_name: Optional[str] = None,
                     operation_name: Optional[str] = None,
                     start_time: Optional[datetime] = None,
                     end_time: Optional[datetime] = None,
                     min_duration_ms: Optional[float] = None,
                     max_duration_ms: Optional[float] = None,
                     has_errors: Optional[bool] = None,
                     tags: Optional[Dict[str, Any]] = None,
                     limit: int = 100) -> List[DistributedTrace]:
        """Search for traces with filters"""
        traces = []

        for trace_id, spans in self.trace_cache.items():
            trace = self.get_trace(trace_id)
            if not trace:
                continue

            # Apply filters
            if service_name and service_name not in trace.services:
                continue

            if operation_name:
                if not any(operation_name in span.operation_name for span in spans):
                    continue

            if start_time and trace.start_time < start_time:
                continue

            if end_time and trace.end_time and trace.end_time > end_time:
                continue

            if min_duration_ms and (trace.total_duration_ms is None or trace.total_duration_ms < min_duration_ms):
                continue

            if max_duration_ms and (trace.total_duration_ms is None or trace.total_duration_ms > max_duration_ms):
                continue

            if has_errors is not None:
                if has_errors and trace.error_count == 0:
                    continue
                if not has_errors and trace.error_count > 0:
                    continue

            if tags:
                tag_match = False
                for span in spans:
                    if all(span.tags.get(k) == v for k, v in tags.items()):
                        tag_match = True
                        break
                if not tag_match:
                    continue

            traces.append(trace)

            if len(traces) >= limit:
                break

        # Sort by start time (newest first)
        traces.sort(key=lambda t: t.start_time, reverse=True)
        return traces

    def get_service_map(self) -> Dict[str, Any]:
        """Generate service dependency map"""
        service_dependencies = defaultdict(set)
        service_stats = defaultdict(lambda: {
            'request_count': 0,
            'error_count': 0,
            'avg_duration_ms': 0,
            'total_duration_ms': 0
        })

        for spans in self.trace_cache.values():
            # Build dependency graph
            for span in spans:
                if span.parent_span_id:
                    parent_span = next((s for s in spans if s.span_id == span.parent_span_id), None)
                    if parent_span and parent_span.service_name != span.service_name:
                        service_dependencies[parent_span.service_name].add(span.service_name)

                # Collect service statistics
                stats = service_stats[span.service_name]
                stats['request_count'] += 1
                if span.status == TraceStatus.ERROR:
                    stats['error_count'] += 1
                if span.duration_ms:
                    stats['total_duration_ms'] += span.duration_ms

        # Calculate averages
        for service, stats in service_stats.items():
            if stats['request_count'] > 0:
                stats['avg_duration_ms'] = stats['total_duration_ms'] / stats['request_count']
                stats['error_rate'] = stats['error_count'] / stats['request_count']

        return {
            'services': list(service_stats.keys()),
            'dependencies': {k: list(v) for k, v in service_dependencies.items()},
            'service_stats': dict(service_stats)
        }

    def _generate_trace_id(self) -> str:
        """Generate unique trace ID"""
        return uuid.uuid4().hex

    def _generate_span_id(self) -> str:
        """Generate unique span ID"""
        return uuid.uuid4().hex[:16]


class TracingMetricsCollector:
    """Collect metrics about tracing operations"""

    def __init__(self):
        self.metrics = {
            'spans_started': defaultdict(int),
            'spans_finished': defaultdict(int),
            'spans_errored': defaultdict(int),
            'operation_durations': defaultdict(list),
            'service_request_counts': defaultdict(int),
            'service_error_counts': defaultdict(int)
        }

    def span_started(self, operation_name: str, service_name: str):
        """Record span start"""
        self.metrics['spans_started'][operation_name] += 1
        self.metrics['service_request_counts'][service_name] += 1

    def span_finished(self, operation_name: str, service_name: str, duration_ms: float, has_error: bool):
        """Record span completion"""
        self.metrics['spans_finished'][operation_name] += 1
        self.metrics['operation_durations'][operation_name].append(duration_ms)

        if has_error:
            self.metrics['spans_errored'][operation_name] += 1
            self.metrics['service_error_counts'][service_name] += 1

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        # Calculate operation statistics
        operation_stats = {}
        for operation, durations in self.metrics['operation_durations'].items():
            if durations:
                operation_stats[operation] = {
                    'count': len(durations),
                    'avg_duration_ms': sum(durations) / len(durations),
                    'min_duration_ms': min(durations),
                    'max_duration_ms': max(durations),
                    'p95_duration_ms': sorted(durations)[int(len(durations) * 0.95)] if durations else 0,
                    'error_count': self.metrics['spans_errored'][operation],
                    'error_rate': self.metrics['spans_errored'][operation] / len(durations)
                }

        # Calculate service statistics
        service_stats = {}
        for service in self.metrics['service_request_counts']:
            request_count = self.metrics['service_request_counts'][service]
            error_count = self.metrics['service_error_counts'][service]

            service_stats[service] = {
                'request_count': request_count,
                'error_count': error_count,
                'error_rate': error_count / request_count if request_count > 0 else 0
            }

        return {
            'operation_stats': operation_stats,
            'service_stats': service_stats,
            'total_spans_started': sum(self.metrics['spans_started'].values()),
            'total_spans_finished': sum(self.metrics['spans_finished'].values()),
            'total_spans_errored': sum(self.metrics['spans_errored'].values())
        }


class DistributedTracingMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for automatic request tracing"""

    def __init__(self, app, tracing_provider: JaegerTracingProvider):
        super().__init__(app)
        self.tracing_provider = tracing_provider
        self.logger = logging.getLogger(__name__)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Trace HTTP requests automatically"""
        # Extract trace context from headers
        parent_context = self._extract_trace_context(request)

        # Create operation name
        operation_name = f"{request.method} {request.url.path}"

        # Start span
        span = self.tracing_provider.start_span(
            operation_name=operation_name,
            parent_context=parent_context,
            tags={
                'http.method': request.method,
                'http.url': str(request.url),
                'http.scheme': request.url.scheme,
                'http.host': request.url.hostname,
                'http.target': request.url.path,
                'user_agent': request.headers.get('user-agent', ''),
                'component': 'fastapi'
            },
            kind=SpanKind.SERVER
        )

        try:
            # Process request
            response = await call_next(request)

            # Add response tags
            self.tracing_provider.add_span_tag(span, 'http.status_code', response.status_code)

            # Determine status
            status = TraceStatus.ERROR if response.status_code >= 400 else TraceStatus.OK

            # Add trace headers to response
            response.headers['X-Trace-ID'] = span.trace_id
            response.headers['X-Span-ID'] = span.span_id

            self.tracing_provider.finish_span(span, status)
            return response

        except Exception as e:
            self.tracing_provider.add_span_tag(span, 'error', True)
            self.tracing_provider.add_span_tag(span, 'error.message', str(e))
            self.tracing_provider.finish_span(span, TraceStatus.ERROR, e)
            raise

    def _extract_trace_context(self, request: Request) -> Optional[SpanContext]:
        """Extract trace context from HTTP headers"""
        trace_id = request.headers.get('X-Trace-ID') or request.headers.get(TRACE_ID_HEADER)
        span_id = request.headers.get('X-Parent-Span-ID') or request.headers.get(SPAN_ID_HEADER)

        if trace_id:
            return SpanContext(
                trace_id=trace_id,
                span_id=span_id or self.tracing_provider._generate_span_id(),
                baggage=self._extract_baggage(request)
            )

        return None

    def _extract_baggage(self, request: Request) -> Dict[str, str]:
        """Extract baggage from headers"""
        baggage = {}
        for header_name, header_value in request.headers.items():
            if header_name.lower().startswith('baggage-'):
                key = header_name[8:]  # Remove 'baggage-' prefix
                baggage[key] = header_value
        return baggage


class BusinessLogicTracer:
    """Specialized tracer for business logic operations"""

    def __init__(self, tracing_provider: JaegerTracingProvider):
        self.provider = tracing_provider
        self.logger = logging.getLogger(__name__)

    async def trace_data_processing(self,
                                  operation: str,
                                  input_size: int,
                                  processor_func: Callable,
                                  *args, **kwargs) -> Any:
        """Trace data processing operations"""
        async with self.provider.trace_operation(
            f"data_processing.{operation}",
            tags={
                'operation.type': 'data_processing',
                'input.size': input_size,
                'component': 'data_pipeline'
            }
        ) as span:
            self.provider.add_span_log(span, 'info', f"Starting data processing: {operation}")

            try:
                result = await processor_func(*args, **kwargs) if asyncio.iscoroutinefunction(processor_func) else processor_func(*args, **kwargs)

                # Add result metadata
                if hasattr(result, '__len__'):
                    self.provider.add_span_tag(span, 'output.size', len(result))

                self.provider.add_span_log(span, 'info', f"Data processing completed: {operation}")
                return result

            except Exception as e:
                self.provider.add_span_log(span, 'error', f"Data processing failed: {str(e)}")
                raise

    async def trace_database_operation(self,
                                     query_type: str,
                                     table_name: str,
                                     query_func: Callable,
                                     *args, **kwargs) -> Any:
        """Trace database operations"""
        async with self.provider.trace_operation(
            f"db.{query_type}.{table_name}",
            tags={
                'db.type': 'postgresql',
                'db.table': table_name,
                'db.operation': query_type,
                'component': 'database'
            }
        ) as span:
            try:
                result = await query_func(*args, **kwargs) if asyncio.iscoroutinefunction(query_func) else query_func(*args, **kwargs)

                # Add query statistics
                if hasattr(result, 'rowcount'):
                    self.provider.add_span_tag(span, 'db.rows_affected', result.rowcount)

                return result

            except Exception as e:
                self.provider.add_span_log(span, 'error', f"Database operation failed: {str(e)}")
                raise

    async def trace_api_call(self,
                           service_name: str,
                           endpoint: str,
                           method: str,
                           request_func: Callable,
                           *args, **kwargs) -> Any:
        """Trace external API calls"""
        async with self.provider.trace_operation(
            f"api.{service_name}.{endpoint}",
            tags={
                'http.method': method,
                'http.url': endpoint,
                'service.name': service_name,
                'component': 'http_client'
            },
            kind=SpanKind.CLIENT
        ) as span:
            try:
                response = await request_func(*args, **kwargs) if asyncio.iscoroutinefunction(request_func) else request_func(*args, **kwargs)

                # Add response metadata
                if hasattr(response, 'status_code'):
                    self.provider.add_span_tag(span, 'http.status_code', response.status_code)

                if hasattr(response, 'headers'):
                    self.provider.add_span_tag(span, 'http.response_size', len(str(response.content)))

                return response

            except Exception as e:
                self.provider.add_span_log(span, 'error', f"API call failed: {str(e)}")
                raise


class PerformanceAnalyzer:
    """Analyze performance from distributed traces"""

    def __init__(self, tracing_provider: JaegerTracingProvider):
        self.provider = tracing_provider
        self.logger = logging.getLogger(__name__)

    def analyze_trace_performance(self, trace: DistributedTrace) -> Dict[str, Any]:
        """Analyze performance characteristics of a trace"""
        analysis = {
            'trace_id': trace.trace_id,
            'total_duration_ms': trace.total_duration_ms,
            'span_count': trace.span_count,
            'service_count': len(trace.services),
            'error_count': trace.error_count,
            'bottlenecks': [],
            'service_breakdown': {},
            'critical_path': [],
            'recommendations': []
        }

        # Analyze service breakdown
        for service in trace.services:
            service_spans = [span for span in trace.spans if span.service_name == service]
            total_service_time = sum(span.duration_ms or 0 for span in service_spans)

            analysis['service_breakdown'][service] = {
                'span_count': len(service_spans),
                'total_duration_ms': total_service_time,
                'percentage': (total_service_time / trace.total_duration_ms) * 100 if trace.total_duration_ms else 0,
                'avg_span_duration_ms': total_service_time / len(service_spans) if service_spans else 0
            }

        # Identify bottlenecks (spans taking >20% of total time)
        if trace.total_duration_ms:
            threshold = trace.total_duration_ms * 0.2
            bottlenecks = [
                {
                    'span_id': span.span_id,
                    'operation': span.operation_name,
                    'service': span.service_name,
                    'duration_ms': span.duration_ms,
                    'percentage': (span.duration_ms / trace.total_duration_ms) * 100
                }
                for span in trace.spans
                if span.duration_ms and span.duration_ms > threshold
            ]
            analysis['bottlenecks'] = sorted(bottlenecks, key=lambda x: x['duration_ms'], reverse=True)

        # Find critical path (longest path through the trace)
        analysis['critical_path'] = self._find_critical_path(trace.spans)

        # Generate recommendations
        analysis['recommendations'] = self._generate_performance_recommendations(analysis)

        return analysis

    def _find_critical_path(self, spans: List[TraceSpan]) -> List[Dict[str, Any]]:
        """Find the critical path through the trace"""
        # Build span hierarchy
        span_map = {span.span_id: span for span in spans}
        children = defaultdict(list)

        for span in spans:
            if span.parent_span_id:
                children[span.parent_span_id].append(span.span_id)

        # Find root spans
        root_spans = [span for span in spans if not span.parent_span_id]

        # Calculate critical path for each root
        longest_path = []
        max_duration = 0

        for root in root_spans:
            path, duration = self._calculate_longest_path(root.span_id, span_map, children)
            if duration > max_duration:
                max_duration = duration
                longest_path = path

        return [
            {
                'span_id': span_id,
                'operation': span_map[span_id].operation_name,
                'service': span_map[span_id].service_name,
                'duration_ms': span_map[span_id].duration_ms
            }
            for span_id in longest_path
        ]

    def _calculate_longest_path(self,
                               span_id: str,
                               span_map: Dict[str, TraceSpan],
                               children: Dict[str, List[str]]) -> Tuple[List[str], float]:
        """Calculate longest path from a given span"""
        span = span_map[span_id]
        current_duration = span.duration_ms or 0

        if span_id not in children or not children[span_id]:
            return [span_id], current_duration

        # Find longest child path
        longest_child_path = []
        max_child_duration = 0

        for child_id in children[span_id]:
            child_path, child_duration = self._calculate_longest_path(child_id, span_map, children)
            if child_duration > max_child_duration:
                max_child_duration = child_duration
                longest_child_path = child_path

        return [span_id] + longest_child_path, current_duration + max_child_duration

    def _generate_performance_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate performance improvement recommendations"""
        recommendations = []

        # Check for slow services
        for service, stats in analysis['service_breakdown'].items():
            if stats['percentage'] > 50:
                recommendations.append(f"Service '{service}' consumes {stats['percentage']:.1f}% of total time. Consider optimization.")

        # Check for bottlenecks
        if analysis['bottlenecks']:
            top_bottleneck = analysis['bottlenecks'][0]
            recommendations.append(f"Operation '{top_bottleneck['operation']}' is a major bottleneck ({top_bottleneck['percentage']:.1f}% of total time).")

        # Check error rate
        if analysis['error_count'] > 0:
            error_rate = (analysis['error_count'] / analysis['span_count']) * 100
            recommendations.append(f"Error rate is {error_rate:.1f}%. Investigate failing operations.")

        # Check span count
        if analysis['span_count'] > 100:
            recommendations.append("High span count detected. Consider reducing instrumentation granularity.")

        if not recommendations:
            recommendations.append("Performance looks good. Continue monitoring for trends.")

        return recommendations


# Factory function for easy setup
def create_distributed_tracing(config: TracingConfig = None) -> JaegerTracingProvider:
    """Create configured distributed tracing provider"""
    if config is None:
        config = TracingConfig()

    return JaegerTracingProvider(config)


# Integration helper for FastAPI
def setup_fastapi_tracing(app, config: TracingConfig = None):
    """Set up distributed tracing for FastAPI application"""
    tracing_provider = create_distributed_tracing(config)

    # Add tracing middleware
    app.add_middleware(DistributedTracingMiddleware, tracing_provider=tracing_provider)

    # Add tracing endpoints
    @app.get("/traces/{trace_id}")
    async def get_trace(trace_id: str):
        """Get trace by ID"""
        trace = tracing_provider.get_trace(trace_id)
        if not trace:
            return {"error": "Trace not found"}

        return {
            'trace_id': trace.trace_id,
            'services': trace.services,
            'span_count': trace.span_count,
            'duration_ms': trace.total_duration_ms,
            'status': trace.status.value,
            'spans': [
                {
                    'span_id': span.span_id,
                    'operation_name': span.operation_name,
                    'service_name': span.service_name,
                    'start_time': span.start_time.isoformat(),
                    'duration_ms': span.duration_ms,
                    'status': span.status.value,
                    'tags': span.tags
                }
                for span in trace.spans
            ]
        }

    @app.get("/traces/search")
    async def search_traces(
        service: Optional[str] = None,
        operation: Optional[str] = None,
        limit: int = 100
    ):
        """Search traces"""
        traces = tracing_provider.search_traces(
            service_name=service,
            operation_name=operation,
            limit=limit
        )

        return {
            'traces': [
                {
                    'trace_id': trace.trace_id,
                    'services': trace.services,
                    'duration_ms': trace.total_duration_ms,
                    'span_count': trace.span_count,
                    'error_count': trace.error_count,
                    'start_time': trace.start_time.isoformat()
                }
                for trace in traces
            ],
            'total_found': len(traces)
        }

    @app.get("/traces/service-map")
    async def get_service_map():
        """Get service dependency map"""
        return tracing_provider.get_service_map()

    return tracing_provider