"""
Comprehensive Distributed Tracing System
Enterprise-grade distributed tracing with DataDog APM, Jaeger, and OpenTelemetry integration.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import httpx
from ddtrace import tracer as dd_tracer
from ddtrace.ext import http, sql, errors
from ddtrace.propagation.http import HTTPPropagator
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from sqlalchemy.engine import Engine

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class TraceLevel(Enum):
    """Trace severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class SpanType(Enum):
    """Types of spans for categorization."""
    HTTP_REQUEST = "http.request"
    DATABASE_QUERY = "db.query"
    CACHE_OPERATION = "cache.operation"
    MESSAGE_QUEUE = "message.queue"
    ML_INFERENCE = "ml.inference"
    ETL_OPERATION = "etl.operation"
    BUSINESS_LOGIC = "business.logic"
    EXTERNAL_SERVICE = "external.service"
    AUTHENTICATION = "auth.operation"
    SECURITY_CHECK = "security.check"


@dataclass
class TraceContext:
    """Context information for distributed tracing."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    baggage: Dict[str, str] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    business_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SpanMetrics:
    """Metrics collected for each span."""
    duration_ms: float
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    io_operations: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    errors_count: int = 0
    warnings_count: int = 0


@dataclass
class TraceAnalytics:
    """Analytics data for trace analysis."""
    total_spans: int
    error_rate: float
    p95_duration: float
    p99_duration: float
    throughput_per_second: float
    bottleneck_spans: List[str]
    critical_path_duration: float
    business_impact_score: float


class ComprehensiveDistributedTracer:
    """Enterprise distributed tracing system with multi-backend support."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self._active_traces: Dict[str, TraceContext] = {}
        self._span_metrics: Dict[str, SpanMetrics] = {}
        self._trace_analytics: Dict[str, TraceAnalytics] = {}
        
        # Initialize tracing backends
        self._initialize_datadog_tracing()
        self._initialize_opentelemetry()
        self._initialize_jaeger()
        
        # Custom instrumentation
        self._setup_custom_instrumentation()
        
    def _initialize_datadog_tracing(self):
        """Initialize DataDog APM tracing."""
        try:
            # Enhanced DataDog configuration
            dd_tracer.configure(
                hostname=settings.DATADOG_AGENT_HOST or "localhost",
                port=settings.DATADOG_AGENT_PORT or 8126,
                service="pwc-data-engineering-platform",
                env=settings.ENVIRONMENT or "production",
                version=settings.VERSION or "1.0.0",
                debug=settings.DEBUG_MODE or False
            )
            
            # Set global tags for business context
            dd_tracer.set_tags({
                "platform.value": "27.8M",
                "team": "data-engineering",
                "domain": "analytics",
                "compliance": "gdpr,soc2",
                "criticality": "high"
            })
            
            # Configure sampling rules for different service tiers
            dd_tracer.configure(
                priority_sampling=True,
                sample_rate=self._get_adaptive_sampling_rate()
            )
            
            self.logger.info("DataDog distributed tracing initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize DataDog tracing: {e}")
    
    def _initialize_opentelemetry(self):
        """Initialize OpenTelemetry tracing."""
        try:
            # Set up OpenTelemetry tracer provider
            trace.set_tracer_provider(TracerProvider())
            self.otel_tracer = trace.get_tracer(__name__)
            
            # Configure Jaeger exporter for OpenTelemetry
            jaeger_exporter = JaegerExporter(
                agent_host_name=settings.JAEGER_AGENT_HOST or "localhost",
                agent_port=settings.JAEGER_AGENT_PORT or 6831,
            )
            
            span_processor = BatchSpanProcessor(jaeger_exporter)
            trace.get_tracer_provider().add_span_processor(span_processor)
            
            # Auto-instrument common libraries
            FastAPIInstrumentor().instrument()
            SQLAlchemyInstrumentor().instrument()
            RequestsInstrumentor().instrument()
            
            self.logger.info("OpenTelemetry tracing initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenTelemetry: {e}")
            self.otel_tracer = None
    
    def _initialize_jaeger(self):
        """Initialize Jaeger tracing client."""
        try:
            from jaeger_client import Config
            
            jaeger_config = Config(
                config={
                    'sampler': {
                        'type': 'adaptive',
                        'param': 1.0,
                    },
                    'logging': True,
                    'reporter_batch_size': 100,
                    'reporter_flush_interval': 1,
                    'local_agent': {
                        'reporting_host': settings.JAEGER_AGENT_HOST or "localhost",
                        'reporting_port': settings.JAEGER_AGENT_PORT or 6831,
                    }
                },
                service_name='pwc-data-platform',
                validate=True,
            )
            
            self.jaeger_tracer = jaeger_config.initialize_tracer()
            self.logger.info("Jaeger tracing initialized")
            
        except ImportError:
            self.logger.warning("Jaeger client not available, skipping Jaeger initialization")
            self.jaeger_tracer = None
        except Exception as e:
            self.logger.error(f"Failed to initialize Jaeger tracing: {e}")
            self.jaeger_tracer = None
    
    def _setup_custom_instrumentation(self):
        """Set up custom instrumentation for business-specific operations."""
        # Custom instrumentation will be added as decorators and context managers
        pass
    
    def _get_adaptive_sampling_rate(self) -> float:
        """Calculate adaptive sampling rate based on system load and criticality."""
        # In production, use lower sampling for high-volume endpoints
        if settings.ENVIRONMENT == "production":
            return 0.1  # 10% sampling
        elif settings.ENVIRONMENT == "staging":
            return 0.5  # 50% sampling
        else:
            return 1.0  # 100% sampling for development
    
    @contextmanager
    def trace_operation(
        self,
        operation_name: str,
        span_type: SpanType = SpanType.BUSINESS_LOGIC,
        tags: Optional[Dict[str, Any]] = None,
        business_context: Optional[Dict[str, Any]] = None
    ):
        """Context manager for tracing operations across all backends."""
        
        # Generate trace context
        trace_id = str(uuid.uuid4())
        span_id = str(uuid.uuid4())
        
        trace_context = TraceContext(
            trace_id=trace_id,
            span_id=span_id,
            business_context=business_context or {}
        )
        
        self._active_traces[trace_id] = trace_context
        
        # Start timing
        start_time = time.time()
        
        # DataDog span
        dd_span = dd_tracer.trace(operation_name)
        if tags:
            dd_span.set_tags(tags)
        dd_span.set_tag("span.type", span_type.value)
        dd_span.set_tag("trace.id", trace_id)
        
        # Business context tags
        if business_context:
            for key, value in business_context.items():
                dd_span.set_tag(f"business.{key}", value)
        
        # OpenTelemetry span
        otel_span = None
        if self.otel_tracer:
            otel_span = self.otel_tracer.start_span(operation_name)
            if tags:
                for key, value in tags.items():
                    otel_span.set_attribute(key, value)
        
        # Jaeger span
        jaeger_span = None
        if self.jaeger_tracer:
            jaeger_span = self.jaeger_tracer.start_span(operation_name)
            if tags:
                for key, value in tags.items():
                    jaeger_span.set_tag(key, value)
        
        try:
            yield trace_context
            
            # Mark as successful
            dd_span.set_tag("operation.status", "success")
            if otel_span:
                otel_span.set_status(trace.Status(trace.StatusCode.OK))
            
        except Exception as e:
            # Mark as error and add error details
            error_message = str(e)
            error_type = type(e).__name__
            
            dd_span.set_error(e)
            dd_span.set_tag("error.message", error_message)
            dd_span.set_tag("error.type", error_type)
            
            if otel_span:
                otel_span.record_exception(e)
                otel_span.set_status(trace.Status(trace.StatusCode.ERROR, error_message))
            
            if jaeger_span:
                jaeger_span.set_tag("error", True)
                jaeger_span.set_tag("error.message", error_message)
                jaeger_span.set_tag("error.type", error_type)
            
            raise
            
        finally:
            # Calculate metrics
            duration_ms = (time.time() - start_time) * 1000
            
            self._span_metrics[span_id] = SpanMetrics(
                duration_ms=duration_ms
            )
            
            # Finish spans
            dd_span.finish()
            if otel_span:
                otel_span.end()
            if jaeger_span:
                jaeger_span.finish()
            
            # Clean up active traces
            if trace_id in self._active_traces:
                del self._active_traces[trace_id]
            
            self.logger.debug(f"Traced operation '{operation_name}' completed in {duration_ms:.2f}ms")
    
    @asynccontextmanager
    async def trace_async_operation(
        self,
        operation_name: str,
        span_type: SpanType = SpanType.BUSINESS_LOGIC,
        tags: Optional[Dict[str, Any]] = None,
        business_context: Optional[Dict[str, Any]] = None
    ):
        """Async context manager for tracing async operations."""
        
        with self.trace_operation(operation_name, span_type, tags, business_context) as trace_context:
            yield trace_context
    
    def trace_http_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        response_status: Optional[int] = None,
        response_size: Optional[int] = None,
        duration_ms: Optional[float] = None
    ):
        """Trace HTTP request with detailed metadata."""
        
        parsed_url = urlparse(url)
        operation_name = f"{method.upper()} {parsed_url.path}"
        
        tags = {
            "http.method": method.upper(),
            "http.url": url,
            "http.host": parsed_url.hostname,
            "http.scheme": parsed_url.scheme,
            "span.kind": "client"
        }
        
        if response_status:
            tags["http.status_code"] = response_status
            tags["http.status_class"] = f"{response_status // 100}xx"
        
        if response_size:
            tags["http.response_size"] = response_size
        
        if duration_ms:
            tags["http.duration_ms"] = duration_ms
        
        with self.trace_operation(operation_name, SpanType.HTTP_REQUEST, tags):
            pass
    
    def trace_database_operation(
        self,
        operation: str,
        table: str,
        query: Optional[str] = None,
        rows_affected: Optional[int] = None,
        duration_ms: Optional[float] = None,
        database: str = "postgresql"
    ):
        """Trace database operation with query details."""
        
        operation_name = f"db.{operation.lower()}"
        
        tags = {
            "db.system": database,
            "db.operation": operation.lower(),
            "db.table": table,
            "span.kind": "client"
        }
        
        if rows_affected is not None:
            tags["db.rows_affected"] = rows_affected
        
        if duration_ms:
            tags["db.duration_ms"] = duration_ms
        
        # Obfuscate sensitive data in queries
        if query and not settings.DEBUG_MODE:
            # Remove or obfuscate sensitive parts of the query
            query = self._obfuscate_sql_query(query)
            tags["db.statement"] = query[:1000]  # Limit query length
        
        with self.trace_operation(operation_name, SpanType.DATABASE_QUERY, tags):
            pass
    
    def trace_ml_operation(
        self,
        model_name: str,
        operation: str,
        input_shape: Optional[tuple] = None,
        output_shape: Optional[tuple] = None,
        accuracy: Optional[float] = None,
        confidence: Optional[float] = None,
        duration_ms: Optional[float] = None
    ):
        """Trace ML model operations."""
        
        operation_name = f"ml.{operation.lower()}"
        
        tags = {
            "ml.model_name": model_name,
            "ml.operation": operation.lower(),
            "span.kind": "internal"
        }
        
        if input_shape:
            tags["ml.input_shape"] = str(input_shape)
        
        if output_shape:
            tags["ml.output_shape"] = str(output_shape)
        
        if accuracy is not None:
            tags["ml.accuracy"] = accuracy
        
        if confidence is not None:
            tags["ml.confidence"] = confidence
        
        if duration_ms:
            tags["ml.duration_ms"] = duration_ms
        
        business_context = {
            "model_name": model_name,
            "operation": operation,
            "business_value": "3.5M"  # AI/LLM Analytics value
        }
        
        with self.trace_operation(operation_name, SpanType.ML_INFERENCE, tags, business_context):
            pass
    
    def trace_etl_operation(
        self,
        job_name: str,
        stage: str,
        records_processed: Optional[int] = None,
        records_failed: Optional[int] = None,
        data_size_mb: Optional[float] = None,
        duration_ms: Optional[float] = None
    ):
        """Trace ETL pipeline operations."""
        
        operation_name = f"etl.{stage.lower()}"
        
        tags = {
            "etl.job_name": job_name,
            "etl.stage": stage.lower(),
            "span.kind": "internal"
        }
        
        if records_processed is not None:
            tags["etl.records_processed"] = records_processed
        
        if records_failed is not None:
            tags["etl.records_failed"] = records_failed
        
        if data_size_mb is not None:
            tags["etl.data_size_mb"] = data_size_mb
        
        if duration_ms:
            tags["etl.duration_ms"] = duration_ms
        
        # Calculate success rate
        if records_processed is not None and records_failed is not None:
            total_records = records_processed + records_failed
            success_rate = (records_processed / total_records) if total_records > 0 else 1.0
            tags["etl.success_rate"] = success_rate
        
        business_context = {
            "job_name": job_name,
            "stage": stage,
            "business_impact": "data_quality"
        }
        
        with self.trace_operation(operation_name, SpanType.ETL_OPERATION, tags, business_context):
            pass
    
    def trace_security_operation(
        self,
        operation: str,
        user_id: Optional[str] = None,
        resource: Optional[str] = None,
        permission: Optional[str] = None,
        result: Optional[str] = None,
        risk_score: Optional[float] = None
    ):
        """Trace security operations."""
        
        operation_name = f"security.{operation.lower()}"
        
        tags = {
            "security.operation": operation.lower(),
            "span.kind": "internal"
        }
        
        if user_id:
            tags["security.user_id"] = user_id
        
        if resource:
            tags["security.resource"] = resource
        
        if permission:
            tags["security.permission"] = permission
        
        if result:
            tags["security.result"] = result
        
        if risk_score is not None:
            tags["security.risk_score"] = risk_score
        
        business_context = {
            "operation": operation,
            "business_value": "4.2M",  # Advanced Security value
            "compliance_impact": "high"
        }
        
        with self.trace_operation(operation_name, SpanType.SECURITY_CHECK, tags, business_context):
            pass
    
    def add_business_metrics(self, trace_id: str, metrics: Dict[str, Any]):
        """Add business metrics to an active trace."""
        
        if trace_id in self._active_traces:
            trace_context = self._active_traces[trace_id]
            trace_context.business_context.update(metrics)
            
            # Add to DataDog span if available
            current_span = dd_tracer.current_span()
            if current_span:
                for key, value in metrics.items():
                    current_span.set_tag(f"business.{key}", value)
    
    def inject_trace_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Inject trace headers for distributed tracing propagation."""
        
        # DataDog propagation
        propagator = HTTPPropagator()
        span_context = dd_tracer.current_span_context()
        if span_context:
            propagator.inject(span_context, headers)
        
        # Add custom correlation headers
        if hasattr(self, '_current_correlation_id'):
            headers['X-Correlation-ID'] = self._current_correlation_id
        
        return headers
    
    def extract_trace_headers(self, headers: Dict[str, str]) -> Optional[TraceContext]:
        """Extract trace context from headers."""
        
        try:
            # DataDog extraction
            propagator = HTTPPropagator()
            span_context = propagator.extract(headers)
            
            if span_context:
                return TraceContext(
                    trace_id=str(span_context.trace_id),
                    span_id=str(span_context.span_id),
                    correlation_id=headers.get('X-Correlation-ID')
                )
        
        except Exception as e:
            self.logger.warning(f"Failed to extract trace context: {e}")
        
        return None
    
    def _obfuscate_sql_query(self, query: str) -> str:
        """Obfuscate sensitive data in SQL queries."""
        import re
        
        # Replace potential sensitive values with placeholders
        patterns = [
            (r"'[^']*'", "'***'"),  # String literals
            (r"\b\d{4}-\d{2}-\d{2}\b", "****-**-**"),  # Dates
            (r"\b\d+\.\d+\b", "***.**"),  # Decimal numbers
        ]
        
        obfuscated = query
        for pattern, replacement in patterns:
            obfuscated = re.sub(pattern, replacement, obfuscated)
        
        return obfuscated
    
    def get_trace_analytics(self, trace_id: str) -> Optional[TraceAnalytics]:
        """Get analytics for a specific trace."""
        return self._trace_analytics.get(trace_id)
    
    def get_active_traces(self) -> Dict[str, TraceContext]:
        """Get all currently active traces."""
        return self._active_traces.copy()
    
    def get_span_metrics(self, span_id: str) -> Optional[SpanMetrics]:
        """Get metrics for a specific span."""
        return self._span_metrics.get(span_id)
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of tracing backends."""
        
        health = {
            "datadog": {"status": "unknown", "latency_ms": None},
            "jaeger": {"status": "unknown", "latency_ms": None},
            "opentelemetry": {"status": "unknown", "latency_ms": None}
        }
        
        # Check DataDog agent
        try:
            start_time = time.time()
            async with httpx.AsyncClient() as client:
                agent_url = f"http://{settings.DATADOG_AGENT_HOST or 'localhost'}:{settings.DATADOG_AGENT_PORT or 8126}/info"
                response = await client.get(agent_url, timeout=5.0)
                if response.status_code == 200:
                    health["datadog"]["status"] = "healthy"
                else:
                    health["datadog"]["status"] = "unhealthy"
                health["datadog"]["latency_ms"] = (time.time() - start_time) * 1000
        except Exception:
            health["datadog"]["status"] = "unhealthy"
        
        # Check Jaeger (simplified check)
        if self.jaeger_tracer:
            health["jaeger"]["status"] = "healthy"
        else:
            health["jaeger"]["status"] = "unavailable"
        
        # Check OpenTelemetry
        if self.otel_tracer:
            health["opentelemetry"]["status"] = "healthy"
        else:
            health["opentelemetry"]["status"] = "unavailable"
        
        return health


# Global tracer instance
distributed_tracer = ComprehensiveDistributedTracer()


# Decorators for automatic tracing
def trace_method(
    operation_name: Optional[str] = None,
    span_type: SpanType = SpanType.BUSINESS_LOGIC,
    tags: Optional[Dict[str, Any]] = None,
    business_context: Optional[Dict[str, Any]] = None
):
    """Decorator for tracing methods."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            name = operation_name or f"{func.__module__}.{func.__name__}"
            with distributed_tracer.trace_operation(name, span_type, tags, business_context):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def trace_async_method(
    operation_name: Optional[str] = None,
    span_type: SpanType = SpanType.BUSINESS_LOGIC,
    tags: Optional[Dict[str, Any]] = None,
    business_context: Optional[Dict[str, Any]] = None
):
    """Decorator for tracing async methods."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            name = operation_name or f"{func.__module__}.{func.__name__}"
            async with distributed_tracer.trace_async_operation(name, span_type, tags, business_context):
                return await func(*args, **kwargs)
        return wrapper
    return decorator


# Middleware integration functions
async def fastapi_tracing_middleware(request, call_next):
    """FastAPI middleware for automatic request tracing."""
    
    # Extract trace context from headers
    trace_context = distributed_tracer.extract_trace_headers(dict(request.headers))
    
    # Start tracing the request
    operation_name = f"{request.method} {request.url.path}"
    
    tags = {
        "http.method": request.method,
        "http.path": request.url.path,
        "http.scheme": request.url.scheme,
        "http.host": request.url.hostname,
        "span.kind": "server"
    }
    
    business_context = {
        "endpoint": request.url.path,
        "user_agent": request.headers.get("user-agent", "unknown")
    }
    
    start_time = time.time()
    
    try:
        async with distributed_tracer.trace_async_operation(
            operation_name, 
            SpanType.HTTP_REQUEST, 
            tags, 
            business_context
        ) as trace_ctx:
            
            # Process request
            response = await call_next(request)
            
            # Add response metadata
            duration_ms = (time.time() - start_time) * 1000
            distributed_tracer.add_business_metrics(trace_ctx.trace_id, {
                "response_status": response.status_code,
                "response_time_ms": duration_ms,
                "sla_compliant": duration_ms <= 15  # <15ms SLA
            })
            
            # Inject trace headers in response
            trace_headers = distributed_tracer.inject_trace_headers({})
            for key, value in trace_headers.items():
                response.headers[key] = value
            
            return response
    
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(f"Request failed after {duration_ms:.2f}ms: {e}")
        raise


# Database tracing integration
def trace_database_engine(engine: Engine):
    """Add tracing to SQLAlchemy engine."""
    
    from sqlalchemy import event
    
    @event.listens_for(engine, "before_cursor_execute")
    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        context._query_start_time = time.time()
    
    @event.listens_for(engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        duration_ms = (time.time() - context._query_start_time) * 1000
        
        # Extract operation and table from statement
        operation = statement.split()[0].upper()
        table = "unknown"
        
        # Simple table extraction
        if "FROM" in statement.upper():
            parts = statement.upper().split("FROM")[1].strip().split()
            if parts:
                table = parts[0].strip("(),")
        elif "INTO" in statement.upper():
            parts = statement.upper().split("INTO")[1].strip().split()
            if parts:
                table = parts[0].strip("(),")
        
        distributed_tracer.trace_database_operation(
            operation=operation,
            table=table,
            query=statement if settings.DEBUG_MODE else None,
            duration_ms=duration_ms
        )


# Export main functions
__all__ = [
    'ComprehensiveDistributedTracer',
    'distributed_tracer',
    'trace_method',
    'trace_async_method',
    'fastapi_tracing_middleware',
    'trace_database_engine',
    'TraceLevel',
    'SpanType',
    'TraceContext',
    'SpanMetrics'
]