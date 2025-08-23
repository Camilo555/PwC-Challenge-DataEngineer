"""
Metrics Collection and Reporting
Provides comprehensive metrics collection for ETL pipeline monitoring.
"""
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        Summary,
        start_http_server,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

from core.logging import get_logger

logger = get_logger(__name__)


class MetricType(Enum):
    """Metric types for classification."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricPoint:
    """Single metric data point."""
    name: str
    value: float
    timestamp: datetime
    labels: dict[str, str] = field(default_factory=dict)
    metric_type: MetricType = MetricType.GAUGE


@dataclass
class ETLJobMetrics:
    """Metrics for ETL job execution."""
    job_name: str
    start_time: datetime
    end_time: datetime | None = None
    status: str = "running"
    records_processed: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.records_processed + self.records_failed
        return (self.records_processed / total) if total > 0 else 1.0

    @property
    def throughput_per_second(self) -> float:
        """Calculate processing throughput."""
        if self.duration_seconds > 0:
            return self.records_processed / self.duration_seconds
        return 0.0


class MetricsCollector:
    """Thread-safe metrics collection and aggregation."""

    def __init__(self, max_history: int = 10000):
        self.max_history = max_history
        self._metrics: dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = defaultdict(float)
        self._lock = threading.RLock()

        # Job-specific metrics
        self._job_metrics: dict[str, ETLJobMetrics] = {}

        # Performance tracking
        self._performance_history: deque = deque(maxlen=1000)

    def increment_counter(self, name: str, value: float = 1.0, labels: dict[str, str] | None = None):
        """Increment a counter metric."""
        with self._lock:
            key = self._build_metric_key(name, labels)
            self._counters[key] += value

            metric_point = MetricPoint(
                name=name,
                value=self._counters[key],
                timestamp=datetime.utcnow(),
                labels=labels or {},
                metric_type=MetricType.COUNTER
            )
            self._metrics[key].append(metric_point)

    def set_gauge(self, name: str, value: float, labels: dict[str, str] | None = None):
        """Set a gauge metric value."""
        with self._lock:
            key = self._build_metric_key(name, labels)
            self._gauges[key] = value

            metric_point = MetricPoint(
                name=name,
                value=value,
                timestamp=datetime.utcnow(),
                labels=labels or {},
                metric_type=MetricType.GAUGE
            )
            self._metrics[key].append(metric_point)

    def record_histogram(self, name: str, value: float, labels: dict[str, str] | None = None):
        """Record a histogram observation."""
        with self._lock:
            key = self._build_metric_key(name, labels)

            metric_point = MetricPoint(
                name=name,
                value=value,
                timestamp=datetime.utcnow(),
                labels=labels or {},
                metric_type=MetricType.HISTOGRAM
            )
            self._metrics[key].append(metric_point)

    def start_job_metrics(self, job_name: str) -> ETLJobMetrics:
        """Start collecting metrics for an ETL job."""
        with self._lock:
            job_metrics = ETLJobMetrics(
                job_name=job_name,
                start_time=datetime.utcnow()
            )
            self._job_metrics[job_name] = job_metrics
            return job_metrics

    def finish_job_metrics(self, job_name: str, status: str = "completed"):
        """Finish collecting metrics for an ETL job."""
        with self._lock:
            if job_name in self._job_metrics:
                job_metrics = self._job_metrics[job_name]
                job_metrics.end_time = datetime.utcnow()
                job_metrics.status = status
                job_metrics.duration_seconds = (
                    job_metrics.end_time - job_metrics.start_time
                ).total_seconds()

                # Record job completion metrics
                self.increment_counter("etl_jobs_total", labels={"job": job_name, "status": status})
                self.record_histogram("etl_job_duration_seconds", job_metrics.duration_seconds,
                                    labels={"job": job_name})
                self.set_gauge("etl_job_records_processed", job_metrics.records_processed,
                              labels={"job": job_name})
                self.set_gauge("etl_job_success_rate", job_metrics.success_rate,
                              labels={"job": job_name})

    def update_job_progress(self, job_name: str, records_processed: int = None,
                           records_failed: int = None, error: str = None):
        """Update job progress metrics."""
        with self._lock:
            if job_name in self._job_metrics:
                job_metrics = self._job_metrics[job_name]

                if records_processed is not None:
                    job_metrics.records_processed = records_processed

                if records_failed is not None:
                    job_metrics.records_failed = records_failed

                if error:
                    job_metrics.errors.append(error)

    def get_job_metrics(self, job_name: str) -> ETLJobMetrics | None:
        """Get metrics for specific job."""
        with self._lock:
            return self._job_metrics.get(job_name)

    def get_all_job_metrics(self) -> dict[str, ETLJobMetrics]:
        """Get all job metrics."""
        with self._lock:
            return self._job_metrics.copy()

    def get_metric_history(self, name: str, labels: dict[str, str] | None = None,
                          limit: int = 100) -> list[MetricPoint]:
        """Get metric history."""
        with self._lock:
            key = self._build_metric_key(name, labels)
            history = self._metrics.get(key, deque())
            return list(history)[-limit:]

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of all metrics."""
        with self._lock:
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "active_jobs": len([j for j in self._job_metrics.values() if j.status == "running"]),
                "total_jobs": len(self._job_metrics),
                "metrics_count": sum(len(m) for m in self._metrics.values())
            }

    def cleanup_old_metrics(self, max_age: timedelta = timedelta(hours=24)):
        """Clean up old metric data."""
        cutoff_time = datetime.utcnow() - max_age

        with self._lock:
            for key, metrics in self._metrics.items():
                while metrics and metrics[0].timestamp < cutoff_time:
                    metrics.popleft()

            # Clean up completed jobs older than max_age
            completed_jobs_to_remove = [
                job_name for job_name, job_metrics in self._job_metrics.items()
                if job_metrics.status != "running" and
                (job_metrics.end_time and job_metrics.end_time < cutoff_time)
            ]

            for job_name in completed_jobs_to_remove:
                del self._job_metrics[job_name]

    def _build_metric_key(self, name: str, labels: dict[str, str] | None) -> str:
        """Build unique key for metric with labels."""
        if not labels:
            return name

        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


class PrometheusExporter:
    """Export metrics to Prometheus format."""

    def __init__(self, collector: MetricsCollector, port: int = 8000):
        self.collector = collector
        self.port = port
        self.registry = CollectorRegistry() if PROMETHEUS_AVAILABLE else None
        self._prometheus_metrics: dict[str, Any] = {}

        if not PROMETHEUS_AVAILABLE:
            logger.warning("Prometheus client not available. Install with: pip install prometheus_client")

    def start_server(self):
        """Start Prometheus metrics server."""
        if not PROMETHEUS_AVAILABLE:
            logger.error("Cannot start Prometheus server - prometheus_client not installed")
            return

        try:
            start_http_server(self.port, registry=self.registry)
            logger.info(f"Prometheus metrics server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus server: {e}")

    def register_metric(self, name: str, metric_type: MetricType, description: str = ""):
        """Register a metric with Prometheus."""
        if not PROMETHEUS_AVAILABLE:
            return

        try:
            if metric_type == MetricType.COUNTER:
                metric = Counter(name, description, registry=self.registry)
            elif metric_type == MetricType.GAUGE:
                metric = Gauge(name, description, registry=self.registry)
            elif metric_type == MetricType.HISTOGRAM:
                metric = Histogram(name, description, registry=self.registry)
            elif metric_type == MetricType.SUMMARY:
                metric = Summary(name, description, registry=self.registry)
            else:
                logger.warning(f"Unknown metric type: {metric_type}")
                return

            self._prometheus_metrics[name] = metric
            logger.debug(f"Registered Prometheus metric: {name}")

        except Exception as e:
            logger.error(f"Failed to register Prometheus metric {name}: {e}")

    def export_metrics(self):
        """Export current metrics to Prometheus."""
        if not PROMETHEUS_AVAILABLE:
            return

        try:
            # Export job metrics
            for job_name, job_metrics in self.collector.get_all_job_metrics().items():
                labels = {"job": job_name}

                # Duration
                if "etl_job_duration_seconds" in self._prometheus_metrics:
                    if job_metrics.end_time:
                        self._prometheus_metrics["etl_job_duration_seconds"].observe(
                            job_metrics.duration_seconds
                        )

                # Records processed
                if "etl_records_processed_total" in self._prometheus_metrics:
                    self._prometheus_metrics["etl_records_processed_total"].labels(**labels).set(
                        job_metrics.records_processed
                    )

                # Success rate
                if "etl_job_success_rate" in self._prometheus_metrics:
                    self._prometheus_metrics["etl_job_success_rate"].labels(**labels).set(
                        job_metrics.success_rate
                    )

        except Exception as e:
            logger.error(f"Failed to export metrics to Prometheus: {e}")


class MetricsReporter:
    """Generate metrics reports and alerts."""

    def __init__(self, collector: MetricsCollector):
        self.collector = collector

    def generate_job_report(self, job_name: str) -> dict[str, Any]:
        """Generate detailed report for specific job."""
        job_metrics = self.collector.get_job_metrics(job_name)
        if not job_metrics:
            return {"error": f"No metrics found for job: {job_name}"}

        return {
            "job_name": job_name,
            "status": job_metrics.status,
            "start_time": job_metrics.start_time.isoformat(),
            "end_time": job_metrics.end_time.isoformat() if job_metrics.end_time else None,
            "duration_seconds": job_metrics.duration_seconds,
            "records_processed": job_metrics.records_processed,
            "records_failed": job_metrics.records_failed,
            "success_rate": job_metrics.success_rate,
            "throughput_per_second": job_metrics.throughput_per_second,
            "memory_usage_mb": job_metrics.memory_usage_mb,
            "cpu_usage_percent": job_metrics.cpu_usage_percent,
            "error_count": len(job_metrics.errors),
            "errors": job_metrics.errors[-10:]  # Last 10 errors
        }

    def generate_summary_report(self) -> dict[str, Any]:
        """Generate summary report of all metrics."""
        all_jobs = self.collector.get_all_job_metrics()

        running_jobs = [j for j in all_jobs.values() if j.status == "running"]
        completed_jobs = [j for j in all_jobs.values() if j.status == "completed"]
        failed_jobs = [j for j in all_jobs.values() if j.status == "failed"]

        total_records = sum(j.records_processed for j in all_jobs.values())
        total_failures = sum(j.records_failed for j in all_jobs.values())

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_jobs": len(all_jobs),
                "running_jobs": len(running_jobs),
                "completed_jobs": len(completed_jobs),
                "failed_jobs": len(failed_jobs),
                "total_records_processed": total_records,
                "total_record_failures": total_failures,
                "overall_success_rate": (total_records / (total_records + total_failures)) if (total_records + total_failures) > 0 else 1.0
            },
            "active_jobs": [
                {
                    "name": job.job_name,
                    "duration": (datetime.utcnow() - job.start_time).total_seconds(),
                    "records_processed": job.records_processed
                }
                for job in running_jobs
            ],
            "recent_completions": [
                {
                    "name": job.job_name,
                    "status": job.status,
                    "duration": job.duration_seconds,
                    "success_rate": job.success_rate
                }
                for job in sorted(
                    [j for j in all_jobs.values() if j.end_time],
                    key=lambda x: x.end_time,
                    reverse=True
                )[:10]
            ]
        }

    def check_alerts(self) -> list[dict[str, Any]]:
        """Check for alert conditions."""
        alerts = []
        all_jobs = self.collector.get_all_job_metrics()

        # Check for long-running jobs
        long_running_threshold = timedelta(hours=2)
        for job in all_jobs.values():
            if job.status == "running":
                duration = datetime.utcnow() - job.start_time
                if duration > long_running_threshold:
                    alerts.append({
                        "type": "long_running_job",
                        "severity": "warning",
                        "job_name": job.job_name,
                        "duration_hours": duration.total_seconds() / 3600,
                        "message": f"Job {job.job_name} has been running for {duration}"
                    })

        # Check for high failure rates
        for job in all_jobs.values():
            if job.records_processed + job.records_failed > 100:  # Only check jobs with significant volume
                if job.success_rate < 0.9:  # Less than 90% success
                    alerts.append({
                        "type": "high_failure_rate",
                        "severity": "error" if job.success_rate < 0.5 else "warning",
                        "job_name": job.job_name,
                        "success_rate": job.success_rate,
                        "message": f"Job {job.job_name} has low success rate: {job.success_rate:.2%}"
                    })

        return alerts


# Decorators for automatic metrics collection

def track_execution_time(metric_name: str = None, labels: dict[str, str] | None = None):
    """Decorator to track function execution time."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            name = metric_name or f"{func.__module__}.{func.__name__}_duration_seconds"
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time

                # Record success metric
                default_collector.record_histogram(name, duration, labels)
                default_collector.increment_counter(
                    f"{func.__module__}.{func.__name__}_calls_total",
                    labels={**(labels or {}), "status": "success"}
                )

                return result

            except Exception:
                duration = time.time() - start_time

                # Record failure metric
                default_collector.record_histogram(name, duration,
                                                 labels={**(labels or {}), "status": "error"})
                default_collector.increment_counter(
                    f"{func.__module__}.{func.__name__}_calls_total",
                    labels={**(labels or {}), "status": "error"}
                )

                raise

        return wrapper
    return decorator


def track_async_execution_time(metric_name: str = None, labels: dict[str, str] | None = None):
    """Decorator to track async function execution time."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            name = metric_name or f"{func.__module__}.{func.__name__}_duration_seconds"
            start_time = time.time()

            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time

                # Record success metric
                default_collector.record_histogram(name, duration, labels)
                default_collector.increment_counter(
                    f"{func.__module__}.{func.__name__}_calls_total",
                    labels={**(labels or {}), "status": "success"}
                )

                return result

            except Exception:
                duration = time.time() - start_time

                # Record failure metric
                default_collector.record_histogram(name, duration,
                                                 labels={**(labels or {}), "status": "error"})
                default_collector.increment_counter(
                    f"{func.__module__}.{func.__name__}_calls_total",
                    labels={**(labels or {}), "status": "error"}
                )

                raise

        return wrapper
    return decorator


# Global metrics collector
default_collector = MetricsCollector()
