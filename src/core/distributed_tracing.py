"""
OpenTelemetry Distributed Tracing System
Provides comprehensive distributed tracing for ETL operations, API requests,
database operations, and external service calls with correlation tracking.
"""

from __future__ import annotations

import functools
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

# OpenTelemetry imports
try:
    from opentelemetry import context, propagate, trace
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.propagators.b3 import B3MultiFormat
    from opentelemetry.propagators.jaeger import JaegerPropagator
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.trace.sampling import ParentBased, StaticSampler, TraceIdRatioBased
    from opentelemetry.semconv.resource import ResourceAttributes
    from opentelemetry.semconv.trace import SpanAttributes
    from opentelemetry.trace import SpanKind as OTelSpanKind
    from opentelemetry.trace import Status, StatusCode

    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    # Fallback implementations if OpenTelemetry is not available
    OPENTELEMETRY_AVAILABLE = False

    class MockTracer:
        def start_span(self, *args, **kwargs):
            return MockSpan()

    class MockSpan:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def set_attribute(self, *args):
            pass

        def set_status(self, *args):
            pass

        def record_exception(self, *args):
            pass

        def add_event(self, *args):
            pass


from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class DataDogTraceExporter:
    """DataDog trace exporter for OpenTelemetry spans."""

    def __init__(self, custom_metrics):
        self.custom_metrics = custom_metrics
        self.logger = get_logger(f"{__name__}.datadog_exporter")

    def export(self, spans):
        """Export spans to DataDog."""
        try:
            for span in spans:
                # Convert OpenTelemetry span to DataDog format
                trace_id = (
                    format(span.context.trace_id, "032x")
                    if hasattr(span.context, "trace_id")
                    else "unknown"
                )
                span_id = (
                    format(span.context.span_id, "016x")
                    if hasattr(span.context, "span_id")
                    else "unknown"
                )

                # Extract span information
                operation_name = span.name
                service_name = (
                    span.resource.attributes.get("service.name", "unknown")
                    if hasattr(span, "resource")
                    else "unknown"
                )
                duration_ns = (
                    span.end_time - span.start_time
                    if hasattr(span, "end_time") and hasattr(span, "start_time")
                    else 0
                )
                duration_ms = duration_ns / 1_000_000  # Convert to milliseconds

                # Submit trace metrics to custom metrics system
                import asyncio

                asyncio.create_task(
                    self.custom_metrics.submit_metric(
                        "distributed_trace_span_duration_ms",
                        duration_ms,
                        dimensions={
                            "service": service_name,
                            "operation": operation_name,
                            "trace_id": trace_id[:16],  # Truncate for cardinality
                        },
                        metadata={"span_id": span_id, "trace_id": trace_id},
                    )
                )

                # Submit span count metric
                asyncio.create_task(
                    self.custom_metrics.submit_metric(
                        "distributed_trace_spans_count",
                        1,
                        dimensions={"service": service_name, "operation": operation_name},
                    )
                )

        except Exception as e:
            self.logger.error(f"Failed to export spans to DataDog: {str(e)}")

    def shutdown(self):
        """Shutdown the exporter."""
        pass


class SpanKind(Enum):
    """Span kinds for different operation types."""

    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class TracingComponent(Enum):
    """Components that can be traced."""

    ETL = "etl"
    API = "api"
    DATABASE = "database"
    CACHE = "cache"
    EXTERNAL_API = "external_api"
    MESSAGING = "messaging"
    BUSINESS_LOGIC = "business_logic"
    SECURITY = "security"
    ML_PIPELINE = "ml_pipeline"
    DATA_QUALITY = "data_quality"
    MONITORING = "monitoring"
    AUTHENTICATION = "authentication"
    NOTIFICATION = "notification"
    ANALYTICS = "analytics"


@dataclass
class TracingConfig:
    """Advanced configuration for distributed tracing."""

    service_name: str = "pwc-data-engineering"
    service_version: str = "1.0.0"
    environment: str = field(
        default_factory=lambda: getattr(settings, "environment", "development")
    )

    # Exporters
    enable_jaeger: bool = True
    enable_otlp: bool = False
    enable_console: bool = False
    enable_datadog: bool = True

    # Jaeger configuration
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 14268
    jaeger_collector_endpoint: str = "http://localhost:14268/api/traces"

    # OTLP configuration
    otlp_endpoint: str = "http://localhost:4317"

    # DataDog configuration
    datadog_agent_url: str = "http://localhost:8126"

    # Advanced Sampling Configuration
    sample_rate: float = 1.0  # Base sampling rate
    high_volume_sample_rate: float = 0.1  # For high-volume operations
    error_sampling_rate: float = 1.0  # Always sample errors
    critical_path_sample_rate: float = 1.0  # Always sample critical paths

    # Sampling rules by component
    component_sample_rates: dict[str, float] = field(
        default_factory=lambda: {
            "etl": 0.5,
            "api": 1.0,
            "database": 0.3,
            "cache": 0.1,
            "external_api": 1.0,
            "business_logic": 1.0,
            "security": 1.0,
            "ml_pipeline": 0.8,
            "data_quality": 1.0,
            "monitoring": 0.2,
            "analytics": 0.7,
        }
    )

    # Performance settings
    max_spans_per_trace: int = 1000
    max_attributes_per_span: int = 128
    max_events_per_span: int = 128
    batch_timeout: int = 5  # seconds
    max_export_batch_size: int = 512

    # Resource attributes
    resource_attributes: dict[str, str] = field(default_factory=dict)

    # Custom span attributes
    default_span_attributes: dict[str, str] = field(
        default_factory=lambda: {
            "deployment.environment": getattr(settings, "environment", "development"),
            "team": "data-engineering",
            "project": "pwc-retail-analytics",
        }
    )

    # Business context attributes
    business_attributes: dict[str, str] = field(
        default_factory=lambda: {
            "business.domain": "retail-analytics",
            "business.criticality": "high",
            "compliance.required": "true",
        }
    )


@dataclass
class SpanMetrics:
    """Metrics collected from spans."""

    span_id: str
    trace_id: str
    operation_name: str
    component: str
    duration_ms: float
    status: str
    error_count: int = 0
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class DistributedTracingManager:
    """Manages distributed tracing configuration and operations."""

    def __init__(self, config: TracingConfig = None):
        self.config = config or TracingConfig()
        self.tracer_provider = None
        self.tracer = None
        self.span_metrics: list[SpanMetrics] = []

        if OPENTELEMETRY_AVAILABLE:
            self._setup_tracing()
        else:
            logger.warning("OpenTelemetry not available. Tracing will use mock implementations.")
            self.tracer = MockTracer()

    def _setup_tracing(self):
        """Set up advanced OpenTelemetry tracing with intelligent sampling."""
        try:
            # Create comprehensive resource attributes
            resource_attrs = {
                ResourceAttributes.SERVICE_NAME: self.config.service_name,
                ResourceAttributes.SERVICE_VERSION: self.config.service_version,
                ResourceAttributes.DEPLOYMENT_ENVIRONMENT: self.config.environment,
                ResourceAttributes.SERVICE_NAMESPACE: "pwc-data-platform",
                ResourceAttributes.SERVICE_INSTANCE_ID: f"{self.config.service_name}-{id(self)}",
                "telemetry.sdk.name": "opentelemetry",
                "telemetry.sdk.language": "python",
                **self.config.resource_attributes,
                **self.config.business_attributes,
            }
            resource = Resource.create(resource_attrs)

            # Create intelligent sampler
            sampler = self._create_intelligent_sampler()

            # Create tracer provider with advanced configuration
            self.tracer_provider = TracerProvider(
                resource=resource,
                sampler=sampler,
                span_limits=trace.SpanLimits(
                    max_attributes=self.config.max_attributes_per_span,
                    max_events=self.config.max_events_per_span,
                    max_links=32,
                ),
            )
            trace.set_tracer_provider(self.tracer_provider)

            # Configure exporters
            if self.config.enable_jaeger:
                jaeger_exporter = JaegerExporter(
                    agent_host_name=self.config.jaeger_agent_host,
                    agent_port=self.config.jaeger_agent_port,
                    collector_endpoint=self.config.jaeger_collector_endpoint,
                )
                self.tracer_provider.add_span_processor(
                    BatchSpanProcessor(
                        jaeger_exporter,
                        max_export_batch_size=self.config.max_export_batch_size,
                        schedule_delay_millis=self.config.batch_timeout * 1000,
                    )
                )
                logger.info("Enhanced Jaeger exporter configured")

            if self.config.enable_otlp:
                otlp_exporter = OTLPSpanExporter(endpoint=self.config.otlp_endpoint)
                self.tracer_provider.add_span_processor(
                    BatchSpanProcessor(
                        otlp_exporter,
                        max_export_batch_size=self.config.max_export_batch_size,
                        schedule_delay_millis=self.config.batch_timeout * 1000,
                    )
                )
                logger.info("Enhanced OTLP exporter configured")

            if self.config.enable_console:
                console_exporter = ConsoleSpanExporter()
                self.tracer_provider.add_span_processor(
                    BatchSpanProcessor(
                        console_exporter,
                        max_export_batch_size=100,  # Lower batch size for console
                    )
                )
                logger.info("Enhanced console exporter configured")

            # Add DataDog APM integration
            if self.config.enable_datadog:
                try:
                    # Create custom DataDog span exporter
                    from monitoring.datadog_custom_metrics_advanced import (
                        get_custom_metrics_advanced,
                    )

                    custom_metrics = get_custom_metrics_advanced()

                    # Create DataDog tracing exporter
                    datadog_exporter = DataDogTraceExporter(custom_metrics)
                    self.tracer_provider.add_span_processor(
                        BatchSpanProcessor(
                            datadog_exporter,
                            max_export_batch_size=self.config.max_export_batch_size,
                            schedule_delay_millis=self.config.batch_timeout * 1000,
                        )
                    )
                    logger.info("DataDog APM integration configured")
                except Exception as e:
                    logger.warning(f"DataDog APM integration failed: {str(e)}")

            # Configure propagators for distributed tracing
            from opentelemetry.propagators.composite import CompositePropagator
            from opentelemetry.propagators.tracecontext import TraceContextTextMapPropagator

            composite_propagator = CompositePropagator(
                [TraceContextTextMapPropagator(), JaegerPropagator(), B3MultiFormat()]
            )
            propagate.set_global_textmap(composite_propagator)

            # Get tracer
            self.tracer = trace.get_tracer(__name__, version=self.config.service_version)

            # Auto-instrument common libraries
            self._setup_auto_instrumentation()

            logger.info(
                f"Enhanced distributed tracing initialized for {self.config.service_name} with intelligent sampling"
            )

        except Exception as e:
            logger.error(f"Failed to setup tracing: {str(e)}")
            self.tracer = MockTracer()

    def _create_intelligent_sampler(self):
        """Create intelligent sampling strategy based on configuration."""
        try:
            # Create component-based sampling rules
            component_samplers = {}

            for component, rate in self.config.component_sample_rates.items():
                if rate >= 1.0:
                    component_samplers[component] = StaticSampler(True)
                elif rate <= 0.0:
                    component_samplers[component] = StaticSampler(False)
                else:
                    component_samplers[component] = TraceIdRatioBased(rate)

            # Use parent-based sampling as the base strategy with intelligent rules
            base_sampler = ParentBased(
                root=TraceIdRatioBased(self.config.sample_rate),
                remote_parent_sampled=StaticSampler(True),  # Always sample if parent was sampled
                remote_parent_not_sampled=TraceIdRatioBased(
                    0.1
                ),  # Still sample some from unsampled parents
                local_parent_sampled=StaticSampler(
                    True
                ),  # Always sample if local parent was sampled
                local_parent_not_sampled=StaticSampler(
                    False
                ),  # Don't sample if local parent was not sampled
            )

            return base_sampler

        except Exception as e:
            logger.warning(f"Failed to create intelligent sampler, using default: {str(e)}")
            return ParentBased(TraceIdRatioBased(self.config.sample_rate))

    def _setup_auto_instrumentation(self):
        """Set up automatic instrumentation for common libraries."""
        try:
            # Instrument HTTP requests
            RequestsInstrumentor().instrument()

            # Instrument SQLAlchemy (if available)
            try:
                SQLAlchemyInstrumentor().instrument()
            except Exception:
                pass

            # Instrument Redis (if available)
            try:
                RedisInstrumentor().instrument()
            except Exception:
                pass

            logger.info("Auto-instrumentation configured")

        except Exception as e:
            logger.warning(f"Some auto-instrumentation failed: {str(e)}")

    def instrument_fastapi_app(self, app):
        """Instrument FastAPI application."""
        if OPENTELEMETRY_AVAILABLE:
            try:
                FastAPIInstrumentor.instrument_app(app)
                logger.info("FastAPI instrumented")
            except Exception as e:
                logger.warning(f"FastAPI instrumentation failed: {str(e)}")

    @contextmanager
    def start_span(
        self,
        name: str,
        component: TracingComponent,
        span_kind: SpanKind = SpanKind.INTERNAL,
        attributes: dict[str, Any] = None,
        parent_context: Any = None,
    ):
        """Start a new span with context management."""
        span = self._create_span(name, component, span_kind, attributes, parent_context)
        start_time = time.time()

        try:
            yield span

            # Set success status
            if hasattr(span, "set_status"):
                span.set_status(Status(StatusCode.OK))

        except Exception as e:
            # Record exception
            if hasattr(span, "record_exception"):
                span.record_exception(e)
            if hasattr(span, "set_status"):
                span.set_status(Status(StatusCode.ERROR, str(e)))

            # Collect error metrics
            duration_ms = (time.time() - start_time) * 1000
            self._collect_span_metrics(span, name, component.value, duration_ms, "error", 1)

            raise

        finally:
            duration_ms = (time.time() - start_time) * 1000
            self._collect_span_metrics(span, name, component.value, duration_ms, "ok", 0)

            if hasattr(span, "__exit__"):
                span.__exit__(None, None, None)

    def _create_span(
        self,
        name: str,
        component: TracingComponent,
        span_kind: SpanKind,
        attributes: dict[str, Any] = None,
        parent_context: Any = None,
    ):
        """Create a new span with enhanced attributes and sampling logic."""
        if not OPENTELEMETRY_AVAILABLE:
            return MockSpan()

        # Set comprehensive span attributes
        span_attributes = {
            "component": component.value,
            "span.kind": span_kind.value,
            "service.name": self.config.service_name,
            "service.version": self.config.service_version,
            **self.config.default_span_attributes,
        }

        # Add component-specific attributes
        if component == TracingComponent.ETL:
            span_attributes.update({"etl.framework": "pyspark", "data.processing.type": "batch"})
        elif component == TracingComponent.API:
            span_attributes.update({"api.framework": "fastapi", "api.version": "v1"})
        elif component == TracingComponent.DATABASE:
            span_attributes.update(
                {
                    "db.system": "postgresql",
                    "db.connection_string.sanitized": "postgresql://user@host:5432/db",
                }
            )
        elif component == TracingComponent.ML_PIPELINE:
            span_attributes.update({"ml.framework": "scikit-learn", "ml.pipeline.type": "training"})

        # Merge with custom attributes
        if attributes:
            span_attributes.update(attributes)

        # Map custom span kind to OpenTelemetry span kind
        otel_span_kind = OTelSpanKind.INTERNAL
        if span_kind == SpanKind.SERVER:
            otel_span_kind = OTelSpanKind.SERVER
        elif span_kind == SpanKind.CLIENT:
            otel_span_kind = OTelSpanKind.CLIENT
        elif span_kind == SpanKind.PRODUCER:
            otel_span_kind = OTelSpanKind.PRODUCER
        elif span_kind == SpanKind.CONSUMER:
            otel_span_kind = OTelSpanKind.CONSUMER

        # Create span with enhanced configuration
        span = self.tracer.start_span(
            name, context=parent_context, kind=otel_span_kind, attributes=span_attributes
        )

        # Add business context events for critical operations
        if component in [
            TracingComponent.SECURITY,
            TracingComponent.BUSINESS_LOGIC,
            TracingComponent.DATA_QUALITY,
        ]:
            span.add_event(
                "business_critical_operation_started",
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "component": component.value,
                    "operation": name,
                },
            )

        return span

    def _collect_span_metrics(
        self, span, name: str, component: str, duration_ms: float, status: str, error_count: int
    ):
        """Collect metrics from completed span."""
        try:
            if hasattr(span, "context") and hasattr(span.context, "span_id"):
                span_metrics = SpanMetrics(
                    span_id=format(span.context.span_id, "016x"),
                    trace_id=format(span.context.trace_id, "032x"),
                    operation_name=name,
                    component=component,
                    duration_ms=duration_ms,
                    status=status,
                    error_count=error_count,
                )

                self.span_metrics.append(span_metrics)

                # Keep only last 1000 metrics
                if len(self.span_metrics) > 1000:
                    self.span_metrics = self.span_metrics[-1000:]
        except Exception as e:
            logger.debug(f"Failed to collect span metrics: {str(e)}")

    def trace_etl_operation(self, operation: str, layer: str, dataset: str = None):
        """Create span for ETL operations."""
        attributes = {"etl.operation": operation, "etl.layer": layer}

        if dataset:
            attributes["etl.dataset"] = dataset

        return self.start_span(
            f"etl.{operation}", TracingComponent.ETL, SpanKind.INTERNAL, attributes
        )

    def trace_api_request(self, method: str, endpoint: str, user_id: str = None):
        """Create span for API requests."""
        attributes = {SpanAttributes.HTTP_METHOD: method, SpanAttributes.HTTP_URL: endpoint}

        if user_id:
            attributes["user.id"] = user_id

        return self.start_span(
            f"api.{method.lower()} {endpoint}", TracingComponent.API, SpanKind.SERVER, attributes
        )

    def trace_database_operation(self, operation: str, table: str, database: str = None):
        """Create span for database operations."""
        attributes = {SpanAttributes.DB_OPERATION: operation, SpanAttributes.DB_SQL_TABLE: table}

        if database:
            attributes[SpanAttributes.DB_NAME] = database

        return self.start_span(
            f"db.{operation}", TracingComponent.DATABASE, SpanKind.CLIENT, attributes
        )

    def trace_external_api_call(self, service: str, endpoint: str, method: str = "GET"):
        """Create span for external API calls."""
        attributes = {
            SpanAttributes.HTTP_METHOD: method,
            SpanAttributes.HTTP_URL: endpoint,
            "external.service": service,
        }

        return self.start_span(
            f"external.{service}", TracingComponent.EXTERNAL_API, SpanKind.CLIENT, attributes
        )

    def trace_cache_operation(self, operation: str, key: str, cache_type: str = "redis"):
        """Create span for cache operations."""
        attributes = {"cache.operation": operation, "cache.key": key, "cache.type": cache_type}

        return self.start_span(
            f"cache.{operation}", TracingComponent.CACHE, SpanKind.CLIENT, attributes
        )

    def trace_business_operation(self, operation: str, entity_type: str, entity_id: str = None):
        """Create span for business operations."""
        attributes = {"business.operation": operation, "business.entity_type": entity_type}

        if entity_id:
            attributes["business.entity_id"] = entity_id

        return self.start_span(
            f"business.{operation}", TracingComponent.BUSINESS_LOGIC, SpanKind.INTERNAL, attributes
        )

    def trace_ml_operation(self, operation: str, model_name: str, model_version: str = None):
        """Create span for ML operations."""
        attributes = {"ml.operation": operation, "ml.model.name": model_name}

        if model_version:
            attributes["ml.model.version"] = model_version

        return self.start_span(
            f"ml.{operation}", TracingComponent.ML_PIPELINE, SpanKind.INTERNAL, attributes
        )

    def get_current_trace_context(self) -> dict[str, str]:
        """Get current trace context for correlation."""
        if not OPENTELEMETRY_AVAILABLE:
            return {}

        try:
            current_span = trace.get_current_span()
            if current_span and hasattr(current_span, "context"):
                return {
                    "trace_id": format(current_span.context.trace_id, "032x"),
                    "span_id": format(current_span.context.span_id, "016x"),
                }
        except Exception:
            pass

        return {}

    def inject_trace_context(self, carrier: dict[str, str]) -> dict[str, str]:
        """Inject trace context into carrier for propagation."""
        if OPENTELEMETRY_AVAILABLE:
            propagate.inject(carrier)
        return carrier

    def extract_trace_context(self, carrier: dict[str, str]):
        """Extract trace context from carrier."""
        if OPENTELEMETRY_AVAILABLE:
            return propagate.extract(carrier)
        return None

    def get_span_metrics(self, component: str = None) -> list[SpanMetrics]:
        """Get collected span metrics."""
        if component:
            return [m for m in self.span_metrics if m.component == component]
        return self.span_metrics.copy()

    def get_tracing_stats(self) -> dict[str, Any]:
        """Get tracing statistics."""
        if not self.span_metrics:
            return {}

        components = {m.component for m in self.span_metrics}
        stats = {
            "total_spans": len(self.span_metrics),
            "components": list(components),
            "error_rate": len([m for m in self.span_metrics if m.error_count > 0])
            / len(self.span_metrics),
            "avg_duration_ms": sum(m.duration_ms for m in self.span_metrics)
            / len(self.span_metrics),
        }

        # Component-specific stats
        for component in components:
            component_metrics = [m for m in self.span_metrics if m.component == component]
            stats[f"{component}_spans"] = len(component_metrics)
            stats[f"{component}_avg_duration_ms"] = sum(
                m.duration_ms for m in component_metrics
            ) / len(component_metrics)
            stats[f"{component}_error_rate"] = len(
                [m for m in component_metrics if m.error_count > 0]
            ) / len(component_metrics)

        return stats

    def shutdown(self):
        """Shutdown tracing and flush spans."""
        if OPENTELEMETRY_AVAILABLE and self.tracer_provider:
            try:
                self.tracer_provider.shutdown()
                logger.info("Tracing shutdown completed")
            except Exception as e:
                logger.warning(f"Error during tracing shutdown: {str(e)}")


# Global tracing manager instance
_tracing_manager: DistributedTracingManager | None = None


def get_tracing_manager() -> DistributedTracingManager:
    """Get global tracing manager instance."""
    global _tracing_manager
    if _tracing_manager is None:
        _tracing_manager = DistributedTracingManager()
    return _tracing_manager


def setup_tracing(config: TracingConfig = None) -> DistributedTracingManager:
    """Set up distributed tracing with configuration."""
    global _tracing_manager
    _tracing_manager = DistributedTracingManager(config)
    return _tracing_manager


# Decorators for easy tracing
def trace_etl_stage(stage: str, dataset: str = None):
    """Decorator to trace ETL stages."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracing_manager()
            with tracer.trace_etl_operation(func.__name__, stage, dataset) as span:
                if hasattr(span, "set_attribute"):
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)
                return func(*args, **kwargs)

        return wrapper

    return decorator


def trace_api_endpoint(endpoint: str = None):
    """Decorator to trace API endpoints."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracing_manager()
            endpoint_name = endpoint or func.__name__
            with tracer.trace_api_request("POST", f"/api/{endpoint_name}") as span:
                if hasattr(span, "set_attribute"):
                    span.set_attribute("function.name", func.__name__)
                return func(*args, **kwargs)

        return wrapper

    return decorator


def trace_database_query(table: str = None):
    """Decorator to trace database queries."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracing_manager()
            table_name = table or "unknown"
            with tracer.trace_database_operation("query", table_name) as span:
                if hasattr(span, "set_attribute"):
                    span.set_attribute("function.name", func.__name__)
                return func(*args, **kwargs)

        return wrapper

    return decorator


def trace_business_logic(operation: str, entity_type: str = "unknown"):
    """Decorator to trace business logic."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracing_manager()
            with tracer.trace_business_operation(operation, entity_type) as span:
                if hasattr(span, "set_attribute"):
                    span.set_attribute("function.name", func.__name__)
                return func(*args, **kwargs)

        return wrapper

    return decorator


def trace_external_call(service: str):
    """Decorator to trace external service calls."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracing_manager()
            with tracer.trace_external_api_call(service, f"/{func.__name__}") as span:
                if hasattr(span, "set_attribute"):
                    span.set_attribute("function.name", func.__name__)
                return func(*args, **kwargs)

        return wrapper

    return decorator


# Context utilities
@contextmanager
def trace_context(trace_id: str = None, span_id: str = None):
    """Context manager for setting trace context."""
    tracer = get_tracing_manager()

    # Create a carrier with trace context
    carrier = {}
    if trace_id and span_id:
        carrier = {"uber-trace-id": f"{trace_id}:{span_id}:0:1"}

    context = tracer.extract_trace_context(carrier) if carrier else None

    if OPENTELEMETRY_AVAILABLE and context:
        token = trace.set_current_span(trace.NonRecordingSpan(context))
        try:
            yield
        finally:
            trace.set_current_span(token)
    else:
        yield


# Integration utilities
def add_trace_headers_to_request(headers: dict[str, str]) -> dict[str, str]:
    """Add tracing headers to outgoing requests."""
    tracer = get_tracing_manager()
    return tracer.inject_trace_context(headers)


def get_trace_context_for_logging() -> dict[str, str]:
    """Get trace context for structured logging."""
    tracer = get_tracing_manager()
    return tracer.get_current_trace_context()


if __name__ == "__main__":
    # Test distributed tracing
    config = TracingConfig(
        service_name="test-service",
        enable_console=True,
        enable_jaeger=False,  # Disable for testing
    )

    tracer = setup_tracing(config)

    # Test ETL tracing
    @trace_etl_stage("bronze", "test_dataset")
    def process_bronze_data():
        time.sleep(0.1)
        return "processed"

    # Test API tracing
    @trace_api_endpoint("test")
    def api_endpoint():
        time.sleep(0.05)
        return {"result": "success"}

    # Test database tracing
    @trace_database_query("customers")
    def query_customers():
        time.sleep(0.02)
        return [{"id": 1, "name": "test"}]

    # Execute test functions
    with tracer.start_span("test_operations", TracingComponent.BUSINESS_LOGIC):
        result1 = process_bronze_data()
        result2 = api_endpoint()
        result3 = query_customers()

    # Test context
    context = get_trace_context_for_logging()
    print(f"Trace context: {context}")

    # Get stats
    stats = tracer.get_tracing_stats()
    print(f"Tracing stats: {stats}")

    # Shutdown
    tracer.shutdown()

    print("Distributed tracing test completed!")
