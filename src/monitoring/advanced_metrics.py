"""
Advanced Metrics Collection and Monitoring Architecture
Provides comprehensive metrics, tracing, and observability capabilities.
"""
from __future__ import annotations

import json
import statistics
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any

from core.logging import get_logger


class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    TIMER = "timer"


class MetricUnit(Enum):
    """Units for metrics"""
    NONE = ""
    BYTES = "bytes"
    SECONDS = "seconds"
    MILLISECONDS = "milliseconds"
    PERCENT = "percent"
    COUNT = "count"
    RATE = "rate"


@dataclass
class MetricValue:
    """A single metric value with timestamp"""
    value: int | float
    timestamp: datetime
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class MetricDefinition:
    """Definition of a metric"""
    name: str
    metric_type: MetricType
    description: str
    unit: MetricUnit = MetricUnit.NONE
    labels: list[str] = field(default_factory=list)


class MetricRegistry:
    """Registry for metric definitions"""

    def __init__(self):
        self._metrics: dict[str, MetricDefinition] = {}
        self.logger = get_logger(self.__class__.__name__)

    def register(self, definition: MetricDefinition) -> None:
        """Register a metric definition"""
        if definition.name in self._metrics:
            raise ValueError(f"Metric '{definition.name}' already registered")

        self._metrics[definition.name] = definition
        self.logger.debug(f"Registered metric: {definition.name}")

    def get_definition(self, name: str) -> MetricDefinition | None:
        """Get metric definition by name"""
        return self._metrics.get(name)

    def list_metrics(self) -> list[MetricDefinition]:
        """List all registered metrics"""
        return list(self._metrics.values())


class MetricStorage(ABC):
    """Abstract storage backend for metrics"""

    @abstractmethod
    def store_metric(self, name: str, value: MetricValue) -> None:
        """Store a metric value"""
        pass

    @abstractmethod
    def query_metrics(
        self,
        name: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        labels: dict[str, str] | None = None
    ) -> list[MetricValue]:
        """Query metric values"""
        pass

    @abstractmethod
    def get_latest_value(self, name: str, labels: dict[str, str] | None = None) -> MetricValue | None:
        """Get the latest value for a metric"""
        pass


class InMemoryMetricStorage(MetricStorage):
    """In-memory storage for metrics (for development/testing)"""

    def __init__(self, max_values_per_metric: int = 10000):
        self.max_values_per_metric = max_values_per_metric
        self._data: dict[str, deque] = defaultdict(lambda: deque(maxlen=self.max_values_per_metric))
        self._lock = threading.Lock()

    def store_metric(self, name: str, value: MetricValue) -> None:
        """Store a metric value"""
        with self._lock:
            self._data[name].append(value)

    def query_metrics(
        self,
        name: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        labels: dict[str, str] | None = None
    ) -> list[MetricValue]:
        """Query metric values"""
        with self._lock:
            values = list(self._data.get(name, []))

        # Filter by time range
        if start_time:
            values = [v for v in values if v.timestamp >= start_time]
        if end_time:
            values = [v for v in values if v.timestamp <= end_time]

        # Filter by labels
        if labels:
            values = [v for v in values if all(v.labels.get(k) == v for k, v in labels.items())]

        return values

    def get_latest_value(self, name: str, labels: dict[str, str] | None = None) -> MetricValue | None:
        """Get the latest value for a metric"""
        with self._lock:
            values = list(self._data.get(name, []))

        if not values:
            return None

        # Filter by labels if provided
        if labels:
            values = [v for v in values if all(v.labels.get(k) == v for k, v in labels.items())]

        return values[-1] if values else None


@dataclass
class TraceSpan:
    """A span in distributed tracing"""
    trace_id: str
    span_id: str
    parent_span_id: str | None
    operation_name: str
    start_time: datetime
    end_time: datetime | None = None
    duration_ms: float | None = None
    tags: dict[str, Any] = field(default_factory=dict)
    logs: list[dict[str, Any]] = field(default_factory=list)
    status: str = "started"


class DistributedTracer:
    """Distributed tracing implementation"""

    def __init__(self):
        self._active_spans: dict[str, TraceSpan] = {}
        self._completed_spans: deque = deque(maxlen=10000)
        self._lock = threading.Lock()
        self.logger = get_logger(self.__class__.__name__)

    def start_span(
        self,
        operation_name: str,
        parent_span_id: str | None = None,
        tags: dict[str, Any] | None = None
    ) -> TraceSpan:
        """Start a new span"""
        span = TraceSpan(
            trace_id=str(uuid.uuid4()),
            span_id=str(uuid.uuid4()),
            parent_span_id=parent_span_id,
            operation_name=operation_name,
            start_time=datetime.now(),
            tags=tags or {}
        )

        with self._lock:
            self._active_spans[span.span_id] = span

        return span

    def finish_span(self, span: TraceSpan, status: str = "success") -> None:
        """Finish a span"""
        span.end_time = datetime.now()
        span.duration_ms = (span.end_time - span.start_time).total_seconds() * 1000
        span.status = status

        with self._lock:
            self._active_spans.pop(span.span_id, None)
            self._completed_spans.append(span)

    def add_log(self, span: TraceSpan, message: str, level: str = "info", **kwargs) -> None:
        """Add a log entry to a span"""
        log_entry = {
            "timestamp": datetime.now(),
            "level": level,
            "message": message,
            **kwargs
        }
        span.logs.append(log_entry)

    def get_trace(self, trace_id: str) -> list[TraceSpan]:
        """Get all spans for a trace"""
        spans = []

        # Check active spans
        with self._lock:
            spans.extend([span for span in self._active_spans.values() if span.trace_id == trace_id])
            spans.extend([span for span in self._completed_spans if span.trace_id == trace_id])

        return sorted(spans, key=lambda s: s.start_time)


class MetricCollector:
    """Advanced metric collector with multiple backends"""

    def __init__(self, storage: MetricStorage | None = None):
        self.registry = MetricRegistry()
        self.storage = storage or InMemoryMetricStorage()
        self.tracer = DistributedTracer()
        self._lock = threading.Lock()
        self.logger = get_logger(self.__class__.__name__)

        # Register default metrics
        self._register_default_metrics()

    def _register_default_metrics(self) -> None:
        """Register common ETL metrics"""
        default_metrics = [
            MetricDefinition("etl_records_processed", MetricType.COUNTER, "Total records processed", MetricUnit.COUNT),
            MetricDefinition("etl_records_failed", MetricType.COUNTER, "Total records failed", MetricUnit.COUNT),
            MetricDefinition("etl_processing_time", MetricType.HISTOGRAM, "Processing time", MetricUnit.SECONDS),
            MetricDefinition("etl_data_quality_score", MetricType.GAUGE, "Data quality score", MetricUnit.PERCENT),
            MetricDefinition("etl_error_rate", MetricType.GAUGE, "Error rate", MetricUnit.PERCENT),
            MetricDefinition("etl_throughput", MetricType.GAUGE, "Records per second", MetricUnit.RATE),
            MetricDefinition("system_memory_usage", MetricType.GAUGE, "Memory usage", MetricUnit.BYTES),
            MetricDefinition("system_cpu_usage", MetricType.GAUGE, "CPU usage", MetricUnit.PERCENT),
            # Messaging system metrics
            MetricDefinition("rabbitmq_queue_depth", MetricType.GAUGE, "RabbitMQ queue message count", MetricUnit.COUNT),
            MetricDefinition("rabbitmq_consumer_count", MetricType.GAUGE, "RabbitMQ consumer count", MetricUnit.COUNT),
            MetricDefinition("rabbitmq_message_rate", MetricType.GAUGE, "RabbitMQ message rate", MetricUnit.RATE),
            MetricDefinition("rabbitmq_connection_count", MetricType.GAUGE, "RabbitMQ connection count", MetricUnit.COUNT),
            MetricDefinition("kafka_consumer_lag", MetricType.GAUGE, "Kafka consumer lag", MetricUnit.COUNT),
            MetricDefinition("kafka_partition_count", MetricType.GAUGE, "Kafka partition count", MetricUnit.COUNT),
            MetricDefinition("kafka_broker_count", MetricType.GAUGE, "Kafka broker count", MetricUnit.COUNT),
            MetricDefinition("kafka_messages_per_sec", MetricType.GAUGE, "Kafka messages per second", MetricUnit.RATE),
            MetricDefinition("api_request_duration", MetricType.HISTOGRAM, "API request duration", MetricUnit.MILLISECONDS),
            MetricDefinition("api_request_count", MetricType.COUNTER, "API request count", MetricUnit.COUNT),
            MetricDefinition("messaging_errors", MetricType.COUNTER, "Messaging system errors", MetricUnit.COUNT),
            MetricDefinition("message_processing_time", MetricType.HISTOGRAM, "Message processing time", MetricUnit.MILLISECONDS),
        ]

        for metric in default_metrics:
            try:
                self.registry.register(metric)
            except ValueError:
                pass  # Already registered

    def increment_counter(self, name: str, value: int | float = 1, labels: dict[str, str] | None = None) -> None:
        """Increment a counter metric"""
        self._record_metric(name, MetricType.COUNTER, value, labels)

    def set_gauge(self, name: str, value: int | float, labels: dict[str, str] | None = None) -> None:
        """Set a gauge metric value"""
        self._record_metric(name, MetricType.GAUGE, value, labels)

    def record_histogram(self, name: str, value: int | float, labels: dict[str, str] | None = None) -> None:
        """Record a histogram metric value"""
        self._record_metric(name, MetricType.HISTOGRAM, value, labels)

    def record_timer(self, name: str, duration_seconds: float, labels: dict[str, str] | None = None) -> None:
        """Record a timer metric"""
        self._record_metric(name, MetricType.TIMER, duration_seconds, labels)

    def _record_metric(self, name: str, expected_type: MetricType, value: int | float, labels: dict[str, str] | None = None) -> None:
        """Internal method to record a metric"""
        definition = self.registry.get_definition(name)
        if definition and definition.metric_type != expected_type:
            raise ValueError(f"Metric '{name}' is of type {definition.metric_type.value}, not {expected_type.value}")

        metric_value = MetricValue(
            value=value,
            timestamp=datetime.now(),
            labels=labels or {}
        )

        self.storage.store_metric(name, metric_value)
        self.logger.debug(f"Recorded metric: {name} = {value}")

    @contextmanager
    def time_operation(self, operation_name: str, labels: dict[str, str] | None = None):
        """Context manager for timing operations"""
        start_time = time.time()
        span = self.tracer.start_span(operation_name, tags=labels or {})

        try:
            yield span
            self.tracer.finish_span(span, "success")
        except Exception as e:
            self.tracer.add_log(span, f"Operation failed: {str(e)}", "error")
            self.tracer.finish_span(span, "error")
            raise
        finally:
            duration = time.time() - start_time
            self.record_timer(f"{operation_name}_duration", duration, labels)

    def record_etl_metrics(
        self,
        pipeline_name: str,
        stage: str,
        records_processed: int,
        records_failed: int,
        processing_time: float,
        data_quality_score: float,
        error_count: int = 0,
        warnings_count: int = 0
    ) -> None:
        """Record comprehensive ETL metrics"""
        labels = {"pipeline": pipeline_name, "stage": stage}

        self.increment_counter("etl_records_processed", records_processed, labels)
        self.increment_counter("etl_records_failed", records_failed, labels)
        self.record_histogram("etl_processing_time", processing_time, labels)
        self.set_gauge("etl_data_quality_score", data_quality_score * 100, labels)

        if records_processed > 0:
            error_rate = (records_failed / records_processed) * 100
            throughput = records_processed / processing_time if processing_time > 0 else 0

            self.set_gauge("etl_error_rate", error_rate, labels)
            self.set_gauge("etl_throughput", throughput, labels)

        if error_count > 0:
            self.increment_counter("etl_errors", error_count, labels)

        if warnings_count > 0:
            self.increment_counter("etl_warnings", warnings_count, labels)

    def get_metric_summary(self, name: str, time_window_minutes: int = 60) -> dict[str, Any]:
        """Get summary statistics for a metric"""
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=time_window_minutes)

        values = self.storage.query_metrics(name, start_time, end_time)

        if not values:
            return {"count": 0, "message": "No data available"}

        numeric_values = [v.value for v in values]

        return {
            "count": len(numeric_values),
            "min": min(numeric_values),
            "max": max(numeric_values),
            "mean": statistics.mean(numeric_values),
            "median": statistics.median(numeric_values),
            "std_dev": statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0,
            "latest": numeric_values[-1],
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            }
        }

    def export_metrics(self, format: str = "prometheus") -> str:
        """Export metrics in various formats"""
        if format == "prometheus":
            return self._export_prometheus()
        elif format == "json":
            return self._export_json()
        else:
            raise ValueError(f"Unsupported export format: {format}")

    def _export_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        output = []

        for definition in self.registry.list_metrics():
            # Get latest value for each metric
            latest_value = self.storage.get_latest_value(definition.name)
            if latest_value:
                output.append(f"# HELP {definition.name} {definition.description}")
                output.append(f"# TYPE {definition.name} {definition.metric_type.value}")

                labels_str = ""
                if latest_value.labels:
                    labels_items = [f'{k}="{v}"' for k, v in latest_value.labels.items()]
                    labels_str = "{" + ",".join(labels_items) + "}"

                output.append(f"{definition.name}{labels_str} {latest_value.value}")

        return "\n".join(output)

    def _export_json(self) -> str:
        """Export metrics in JSON format"""
        metrics_data = {}

        for definition in self.registry.list_metrics():
            latest_value = self.storage.get_latest_value(definition.name)
            if latest_value:
                metrics_data[definition.name] = {
                    "value": latest_value.value,
                    "timestamp": latest_value.timestamp.isoformat(),
                    "labels": latest_value.labels,
                    "type": definition.metric_type.value,
                    "description": definition.description,
                    "unit": definition.unit.value
                }

        return json.dumps(metrics_data, indent=2)


def timed(metric_name: str | None = None, labels: dict[str, str] | None = None):
    """Decorator to time function execution"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            name = metric_name or f"{func.__module__}.{func.__name__}_duration"

            collector = get_metrics_collector()
            with collector.time_operation(name, labels):
                return func(*args, **kwargs)

        return wrapper
    return decorator


def counted(metric_name: str | None = None, labels: dict[str, str] | None = None):
    """Decorator to count function calls"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            name = metric_name or f"{func.__module__}.{func.__name__}_calls"

            collector = get_metrics_collector()
            collector.increment_counter(name, 1, labels)

            try:
                return func(*args, **kwargs)
            except Exception:
                error_name = f"{name}_errors"
                collector.increment_counter(error_name, 1, labels)
                raise

        return wrapper
    return decorator


# Global metrics collector instance
_global_metrics_collector: MetricCollector | None = None


def get_metrics_collector(storage: MetricStorage | None = None) -> MetricCollector:
    """Get the global metrics collector instance"""
    global _global_metrics_collector

    if _global_metrics_collector is None:
        _global_metrics_collector = MetricCollector(storage)

    return _global_metrics_collector


def setup_metrics_collection(storage: MetricStorage | None = None) -> MetricCollector:
    """Setup metrics collection with optional custom storage"""
    global _global_metrics_collector
    _global_metrics_collector = MetricCollector(storage)
    return _global_metrics_collector
