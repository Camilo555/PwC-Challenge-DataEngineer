"""
Comprehensive Prometheus Metrics Exporter
Exports data engineering metrics for Prometheus scraping including
pipeline health, data quality, performance, and business KPIs.
"""

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)
from prometheus_client.core import REGISTRY

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class MetricType(Enum):
    """Types of metrics we collect."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    INFO = "info"
    ENUM = "enum"


@dataclass
class MetricDefinition:
    """Definition of a Prometheus metric."""

    name: str
    description: str
    metric_type: MetricType
    labels: list[str]
    buckets: list[float] = None  # For histograms
    quantiles: list[float] = None  # For summaries


class PrometheusMetricsExporter:
    """
    Comprehensive Prometheus metrics exporter for data engineering platform.

    Exports metrics for:
    - ETL pipeline health and performance
    - Data quality and freshness
    - System resource utilization
    - Business KPIs and data impact
    - Security and compliance metrics
    """

    def __init__(self, port: int = 9090, registry: CollectorRegistry = None):
        self.port = port
        self.registry = registry or REGISTRY
        self.logger = get_logger(__name__)

        # Metric storage
        self.metrics: dict[str, Any] = {}

        # Data collection state
        self.collection_active = False
        self.collection_tasks: dict[str, asyncio.Task] = {}
        self.last_collection_time: dict[str, datetime] = {}

        # Initialize metrics
        self._initialize_etl_metrics()
        self._initialize_data_quality_metrics()
        self._initialize_performance_metrics()
        self._initialize_business_metrics()
        self._initialize_security_metrics()
        self._initialize_system_metrics()

        # HTTP server for metrics endpoint
        self.http_server = None

        self.logger.info(f"Prometheus metrics exporter initialized on port {port}")

    def _initialize_etl_metrics(self):
        """Initialize ETL pipeline metrics."""

        # ETL Pipeline Health Score
        self.metrics["etl_pipeline_health_score"] = Gauge(
            "etl_pipeline_health_score",
            "Health score of ETL pipelines (0-100)",
            ["pipeline_name", "stage", "environment"],
            registry=self.registry,
        )

        # ETL Processing Duration
        self.metrics["etl_processing_duration_seconds"] = Histogram(
            "etl_processing_duration_seconds",
            "Time spent processing ETL stages",
            ["pipeline_name", "stage", "layer"],
            buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0],
            registry=self.registry,
        )

        # Records Processed
        self.metrics["etl_records_processed_total"] = Counter(
            "etl_records_processed_total",
            "Total number of records processed by ETL pipelines",
            ["pipeline_name", "stage", "layer", "table"],
            registry=self.registry,
        )

        # Bytes Processed
        self.metrics["etl_bytes_processed_total"] = Counter(
            "etl_bytes_processed_total",
            "Total bytes processed by ETL pipelines",
            ["pipeline_name", "stage", "layer"],
            registry=self.registry,
        )

        # Pipeline Executions
        self.metrics["etl_pipeline_executions_total"] = Counter(
            "etl_pipeline_executions_total",
            "Total number of pipeline executions",
            ["pipeline_name", "status", "trigger_type"],
            registry=self.registry,
        )

        # Pipeline Errors
        self.metrics["etl_pipeline_errors_total"] = Counter(
            "etl_pipeline_errors_total",
            "Total number of pipeline errors",
            ["pipeline_name", "stage", "error_type"],
            registry=self.registry,
        )

        # Resource Utilization
        self.metrics["etl_memory_usage_bytes"] = Gauge(
            "etl_memory_usage_bytes",
            "Memory usage of ETL processes",
            ["pipeline", "stage", "node"],
            registry=self.registry,
        )

        self.metrics["etl_cpu_usage_percent"] = Gauge(
            "etl_cpu_usage_percent",
            "CPU usage percentage of ETL processes",
            ["pipeline", "stage", "node"],
            registry=self.registry,
        )

    def _initialize_data_quality_metrics(self):
        """Initialize data quality metrics."""

        # Data Quality Scores
        self.metrics["data_quality_completeness_score"] = Gauge(
            "data_quality_completeness_score",
            "Data completeness score (0-1)",
            ["dataset", "table", "column"],
            registry=self.registry,
        )

        self.metrics["data_quality_accuracy_score"] = Gauge(
            "data_quality_accuracy_score",
            "Data accuracy score (0-1)",
            ["dataset", "table", "rule_type"],
            registry=self.registry,
        )

        self.metrics["data_quality_uniqueness_score"] = Gauge(
            "data_quality_uniqueness_score",
            "Data uniqueness score (0-1)",
            ["dataset", "table", "column"],
            registry=self.registry,
        )

        self.metrics["data_quality_consistency_score"] = Gauge(
            "data_quality_consistency_score",
            "Data consistency score (0-1)",
            ["dataset", "table", "rule_name"],
            registry=self.registry,
        )

        # Data Freshness
        self.metrics["data_freshness_hours"] = Gauge(
            "data_freshness_hours",
            "Hours since last data update",
            ["dataset", "table"],
            registry=self.registry,
        )

        # Data Volume Metrics
        self.metrics["data_volume_records"] = Gauge(
            "data_volume_records",
            "Number of records in dataset",
            ["dataset", "table", "layer"],
            registry=self.registry,
        )

        self.metrics["data_volume_bytes"] = Gauge(
            "data_volume_bytes",
            "Size of dataset in bytes",
            ["dataset", "table", "layer"],
            registry=self.registry,
        )

        # Data Quality Checks
        self.metrics["data_quality_checks_total"] = Counter(
            "data_quality_checks_total",
            "Total number of data quality checks performed",
            ["dataset", "check_type", "status"],
            registry=self.registry,
        )

        self.metrics["data_quality_violations_total"] = Counter(
            "data_quality_violations_total",
            "Total number of data quality violations",
            ["dataset", "table", "violation_type", "severity"],
            registry=self.registry,
        )

    def _initialize_performance_metrics(self):
        """Initialize performance metrics."""

        # API Performance
        self.metrics["http_request_duration_seconds"] = Histogram(
            "http_request_duration_seconds",
            "HTTP request duration in seconds",
            ["method", "endpoint", "status_code"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry,
        )

        self.metrics["http_requests_total"] = Counter(
            "http_requests_total",
            "Total number of HTTP requests",
            ["method", "endpoint", "status"],
            registry=self.registry,
        )

        # Database Performance
        self.metrics["postgres_query_duration_seconds"] = Histogram(
            "postgres_query_duration_seconds",
            "PostgreSQL query duration in seconds",
            ["database", "query_type", "table"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
            registry=self.registry,
        )

        self.metrics["postgres_connections_active"] = Gauge(
            "postgres_connections_active",
            "Number of active PostgreSQL connections",
            ["database"],
            registry=self.registry,
        )

        self.metrics["postgres_deadlocks_total"] = Counter(
            "postgres_deadlocks_total",
            "Total number of deadlocks",
            ["database"],
            registry=self.registry,
        )

        # Cache Performance
        self.metrics["redis_keyspace_hits_total"] = Counter(
            "redis_keyspace_hits_total",
            "Total number of cache hits",
            ["instance"],
            registry=self.registry,
        )

        self.metrics["redis_keyspace_misses_total"] = Counter(
            "redis_keyspace_misses_total",
            "Total number of cache misses",
            ["instance"],
            registry=self.registry,
        )

        self.metrics["redis_memory_used_bytes"] = Gauge(
            "redis_memory_used_bytes",
            "Memory used by Redis instance",
            ["instance"],
            registry=self.registry,
        )

        self.metrics["redis_connected_clients"] = Gauge(
            "redis_connected_clients",
            "Number of connected Redis clients",
            ["instance"],
            registry=self.registry,
        )

    def _initialize_business_metrics(self):
        """Initialize business KPI metrics."""

        # Custom Business Metrics
        self.metrics["custom_revenue_per_customer"] = Gauge(
            "custom_revenue_per_customer",
            "Average revenue per customer",
            ["customer_segment", "region"],
            registry=self.registry,
        )

        self.metrics["custom_customer_acquisition_cost"] = Gauge(
            "custom_customer_acquisition_cost",
            "Customer acquisition cost",
            ["channel", "campaign"],
            registry=self.registry,
        )

        self.metrics["custom_customer_lifetime_value"] = Gauge(
            "custom_customer_lifetime_value",
            "Average customer lifetime value",
            ["customer_segment"],
            registry=self.registry,
        )

        self.metrics["custom_data_quality_index"] = Gauge(
            "custom_data_quality_index",
            "Overall data quality index (0-1)",
            ["business_unit"],
            registry=self.registry,
        )

        self.metrics["custom_user_engagement_score"] = Gauge(
            "custom_user_engagement_score",
            "User engagement score based on data quality",
            ["feature", "user_segment"],
            registry=self.registry,
        )

        self.metrics["custom_cost_per_transaction"] = Gauge(
            "custom_cost_per_transaction",
            "Cost per business transaction",
            ["transaction_type", "channel"],
            registry=self.registry,
        )

        self.metrics["custom_resource_utilization_efficiency"] = Gauge(
            "custom_resource_utilization_efficiency",
            "Resource utilization efficiency percentage",
            ["resource_type", "service"],
            registry=self.registry,
        )

        # KPI Composites
        self.metrics["custom_kpi_customer_roi"] = Gauge(
            "custom_kpi_customer_roi",
            "Customer return on investment",
            ["segment"],
            registry=self.registry,
        )

        self.metrics["custom_kpi_data_quality_index"] = Gauge(
            "custom_kpi_data_quality_index",
            "Composite data quality index",
            ["domain"],
            registry=self.registry,
        )

        self.metrics["custom_kpi_operational_efficiency_ratio"] = Gauge(
            "custom_kpi_operational_efficiency_ratio",
            "Operational efficiency ratio",
            ["department"],
            registry=self.registry,
        )

        # Business Impact Metrics
        self.metrics["business_revenue_at_risk"] = Gauge(
            "business_revenue_at_risk",
            "Revenue potentially at risk due to system issues",
            ["risk_factor", "impact_level"],
            registry=self.registry,
        )

    def _initialize_security_metrics(self):
        """Initialize security and compliance metrics."""

        # Authentication Metrics
        self.metrics["auth_failures_total"] = Counter(
            "auth_failures_total",
            "Total number of authentication failures",
            ["method", "reason"],
            registry=self.registry,
        )

        self.metrics["auth_successful_total"] = Counter(
            "auth_successful_total",
            "Total number of successful authentications",
            ["method", "user_type"],
            registry=self.registry,
        )

        # Security Events
        self.metrics["security_events_total"] = Counter(
            "security_events_total",
            "Total number of security events",
            ["event_type", "severity", "source"],
            registry=self.registry,
        )

        # Compliance Metrics
        self.metrics["compliance_gdpr_score"] = Gauge(
            "compliance_gdpr_score",
            "GDPR compliance score (0-1)",
            ["component"],
            registry=self.registry,
        )

        self.metrics["compliance_sox_score"] = Gauge(
            "compliance_sox_score",
            "SOX compliance score (0-1)",
            ["process"],
            registry=self.registry,
        )

        # Data Access Auditing
        self.metrics["data_access_total"] = Counter(
            "data_access_total",
            "Total data access events",
            ["user_type", "data_classification", "access_type"],
            registry=self.registry,
        )

    def _initialize_system_metrics(self):
        """Initialize system and infrastructure metrics."""

        # Monitoring System Health
        self.metrics["monitoring_system_health_score"] = Gauge(
            "monitoring_system_health_score",
            "Overall monitoring system health score (0-100)",
            ["component"],
            registry=self.registry,
        )

        # SLA Compliance
        self.metrics["sla_compliance_rate"] = Gauge(
            "sla_compliance_rate",
            "SLA compliance rate (0-1)",
            ["service", "sla_type"],
            registry=self.registry,
        )

        # Alerting Metrics
        self.metrics["alerting_active_alerts"] = Gauge(
            "alerting_active_alerts",
            "Number of active alerts",
            ["severity", "component"],
            registry=self.registry,
        )

        # Service Discovery
        self.metrics["service_up"] = Gauge(
            "service_up",
            "Service availability (1 = up, 0 = down)",
            ["service_name", "instance"],
            registry=self.registry,
        )

    async def start_collection(self):
        """Start metrics collection tasks."""

        self.collection_active = True
        self.logger.info("Starting metrics collection...")

        # Start collection tasks
        collection_tasks = [
            ("etl_metrics", self._collect_etl_metrics, 30),
            ("data_quality_metrics", self._collect_data_quality_metrics, 60),
            ("performance_metrics", self._collect_performance_metrics, 15),
            ("business_metrics", self._collect_business_metrics, 300),
            ("security_metrics", self._collect_security_metrics, 60),
            ("system_metrics", self._collect_system_metrics, 30),
        ]

        for task_name, task_func, interval in collection_tasks:
            task = asyncio.create_task(self._collection_loop(task_name, task_func, interval))
            self.collection_tasks[task_name] = task
            self.logger.info(f"Started {task_name} collection task (interval: {interval}s)")

        self.logger.info("All metrics collection tasks started")

    async def stop_collection(self):
        """Stop metrics collection tasks."""

        self.collection_active = False

        for task_name, task in self.collection_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.logger.info(f"Stopped {task_name} collection task")

        self.collection_tasks.clear()
        self.logger.info("All metrics collection tasks stopped")

    async def _collection_loop(self, task_name: str, collect_func: callable, interval: int):
        """Generic collection loop for metrics."""

        while self.collection_active:
            try:
                start_time = time.time()

                # Collect metrics
                await collect_func()
                self.last_collection_time[task_name] = datetime.utcnow()

                # Calculate sleep time
                collection_time = time.time() - start_time
                sleep_time = max(0, interval - collection_time)

                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in {task_name} collection: {str(e)}")
                await asyncio.sleep(interval)

    async def _collect_etl_metrics(self):
        """Collect ETL pipeline metrics."""
        try:
            # Mock ETL metrics collection
            # In real implementation, this would query actual ETL systems

            # Pipeline health scores
            pipelines = ["customer_data_pipeline", "sales_analytics_pipeline", "inventory_pipeline"]
            for pipeline in pipelines:
                # Simulate different health scores
                health_score = 85.0 + (hash(pipeline) % 20) - 10
                self.metrics["etl_pipeline_health_score"].labels(
                    pipeline_name=pipeline, stage="overall", environment="production"
                ).set(health_score)

                # Processing duration (simulate)
                duration = 30.0 + (hash(pipeline + "duration") % 60)
                self.metrics["etl_processing_duration_seconds"].labels(
                    pipeline_name=pipeline, stage="transform", layer="silver"
                ).observe(duration)

                # Records processed
                records = 1000 + (hash(pipeline + "records") % 5000)
                self.metrics["etl_records_processed_total"].labels(
                    pipeline_name=pipeline, stage="load", layer="gold", table="main"
                ).inc(records)

        except Exception as e:
            self.logger.error(f"Failed to collect ETL metrics: {str(e)}")

    async def _collect_data_quality_metrics(self):
        """Collect data quality metrics."""
        try:
            # Mock data quality metrics
            datasets = ["customers", "sales", "products", "inventory"]

            for dataset in datasets:
                # Data quality scores
                completeness = 0.95 + (hash(dataset + "completeness") % 5) / 100
                self.metrics["data_quality_completeness_score"].labels(
                    dataset=dataset, table="main", column="all"
                ).set(completeness)

                accuracy = 0.90 + (hash(dataset + "accuracy") % 8) / 100
                self.metrics["data_quality_accuracy_score"].labels(
                    dataset=dataset, table="main", rule_type="format_validation"
                ).set(accuracy)

                uniqueness = 0.98 + (hash(dataset + "uniqueness") % 3) / 100
                self.metrics["data_quality_uniqueness_score"].labels(
                    dataset=dataset, table="main", column="id"
                ).set(uniqueness)

                # Data freshness
                freshness_hours = 0.5 + (hash(dataset + "fresh") % 4)
                self.metrics["data_freshness_hours"].labels(dataset=dataset, table="main").set(
                    freshness_hours
                )

                # Data volume
                volume = 100000 + (hash(dataset + "volume") % 500000)
                self.metrics["data_volume_records"].labels(
                    dataset=dataset, table="main", layer="gold"
                ).set(volume)

        except Exception as e:
            self.logger.error(f"Failed to collect data quality metrics: {str(e)}")

    async def _collect_performance_metrics(self):
        """Collect performance metrics."""
        try:
            # API performance simulation
            endpoints = ["/api/customers", "/api/sales", "/api/analytics"]
            methods = ["GET", "POST"]

            for endpoint in endpoints:
                for method in methods:
                    # Request duration
                    duration = 0.05 + (hash(endpoint + method) % 200) / 1000
                    self.metrics["http_request_duration_seconds"].labels(
                        method=method, endpoint=endpoint, status_code="200"
                    ).observe(duration)

                    # Request count
                    self.metrics["http_requests_total"].labels(
                        method=method, endpoint=endpoint, status="success"
                    ).inc(10 + (hash(endpoint + method + "count") % 50))

            # Database performance
            query_duration = 0.01 + (hash("db_query") % 50) / 1000
            self.metrics["postgres_query_duration_seconds"].labels(
                database="retail_db", query_type="select", table="customers"
            ).observe(query_duration)

            self.metrics["postgres_connections_active"].labels(database="retail_db").set(
                25 + (hash("connections") % 15)
            )

            # Cache performance
            self.metrics["redis_keyspace_hits_total"].labels(instance="redis-1").inc(
                1000 + (hash("hits") % 500)
            )

            self.metrics["redis_keyspace_misses_total"].labels(instance="redis-1").inc(
                50 + (hash("misses") % 100)
            )

        except Exception as e:
            self.logger.error(f"Failed to collect performance metrics: {str(e)}")

    async def _collect_business_metrics(self):
        """Collect business KPI metrics."""
        try:
            # Customer metrics
            segments = ["premium", "standard", "basic"]
            for segment in segments:
                revenue = 150.0 + (hash(segment + "revenue") % 200)
                self.metrics["custom_revenue_per_customer"].labels(
                    customer_segment=segment, region="north_america"
                ).set(revenue)

                clv = 2000.0 + (hash(segment + "clv") % 1000)
                self.metrics["custom_customer_lifetime_value"].labels(customer_segment=segment).set(
                    clv
                )

            # Data quality impact
            dq_index = 0.85 + (hash("dq_index") % 10) / 100
            self.metrics["custom_data_quality_index"].labels(business_unit="retail").set(dq_index)

            # Operational efficiency
            efficiency = 75.0 + (hash("efficiency") % 20)
            self.metrics["custom_resource_utilization_efficiency"].labels(
                resource_type="compute", service="etl"
            ).set(efficiency)

            # Revenue at risk
            risk_value = (1.0 - dq_index) * 50000  # Risk based on data quality
            self.metrics["business_revenue_at_risk"].labels(
                risk_factor="data_quality", impact_level="medium"
            ).set(risk_value)

        except Exception as e:
            self.logger.error(f"Failed to collect business metrics: {str(e)}")

    async def _collect_security_metrics(self):
        """Collect security and compliance metrics."""
        try:
            # Authentication metrics
            auth_failures = hash("auth_fail") % 10
            self.metrics["auth_failures_total"].labels(
                method="password", reason="invalid_credentials"
            ).inc(auth_failures)

            auth_success = 500 + (hash("auth_success") % 200)
            self.metrics["auth_successful_total"].labels(
                method="password", user_type="employee"
            ).inc(auth_success)

            # Compliance scores
            gdpr_score = 0.95 + (hash("gdpr") % 5) / 100
            self.metrics["compliance_gdpr_score"].labels(component="data_processing").set(
                gdpr_score
            )

            sox_score = 0.92 + (hash("sox") % 7) / 100
            self.metrics["compliance_sox_score"].labels(process="financial_reporting").set(
                sox_score
            )

        except Exception as e:
            self.logger.error(f"Failed to collect security metrics: {str(e)}")

    async def _collect_system_metrics(self):
        """Collect system and monitoring metrics."""
        try:
            # System health score
            health_score = 87.5 + (hash("system_health") % 25) / 2
            self.metrics["monitoring_system_health_score"].labels(component="overall").set(
                health_score
            )

            # SLA compliance
            sla_compliance = 0.995 + (hash("sla") % 5) / 1000
            self.metrics["sla_compliance_rate"].labels(service="api", sla_type="availability").set(
                sla_compliance
            )

            # Active alerts
            critical_alerts = hash("critical_alerts") % 3
            self.metrics["alerting_active_alerts"].labels(
                severity="p0_critical", component="database"
            ).set(critical_alerts)

            # Service availability
            services = ["api", "database", "cache", "etl"]
            for service in services:
                up_status = 1 if hash(service + "up") % 20 > 0 else 0
                self.metrics["service_up"].labels(
                    service_name=service, instance=f"{service}-1"
                ).set(up_status)

        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {str(e)}")

    def start_http_server(self):
        """Start HTTP server for metrics endpoint."""
        try:
            self.http_server = start_http_server(self.port, registry=self.registry)
            self.logger.info(f"Metrics HTTP server started on port {self.port}")
            self.logger.info(f"Metrics available at: http://localhost:{self.port}/metrics")
        except Exception as e:
            self.logger.error(f"Failed to start HTTP server: {str(e)}")
            raise

    def get_metrics_text(self) -> str:
        """Get metrics in Prometheus text format."""
        return generate_latest(self.registry).decode("utf-8")

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of metrics collection status."""
        return {
            "metrics_count": len(self.metrics),
            "collection_active": self.collection_active,
            "collection_tasks": list(self.collection_tasks.keys()),
            "last_collection_times": {
                task: time.isoformat() if time else None
                for task, time in self.last_collection_time.items()
            },
            "http_server_port": self.port,
            "registry_collectors": len(self.registry._collector_to_names),
        }


# Global exporter instance
_metrics_exporter: PrometheusMetricsExporter | None = None


def get_metrics_exporter() -> PrometheusMetricsExporter:
    """Get global metrics exporter instance."""
    global _metrics_exporter
    if _metrics_exporter is None:
        port = getattr(settings, "prometheus_port", 9090)
        _metrics_exporter = PrometheusMetricsExporter(port=port)
    return _metrics_exporter


async def start_metrics_collection():
    """Start metrics collection and HTTP server."""
    exporter = get_metrics_exporter()
    exporter.start_http_server()
    await exporter.start_collection()
    return exporter


async def stop_metrics_collection():
    """Stop metrics collection."""
    exporter = get_metrics_exporter()
    await exporter.stop_collection()


if __name__ == "__main__":
    # Test metrics exporter
    async def test_exporter():
        exporter = PrometheusMetricsExporter(port=9091)

        try:
            # Start collection
            exporter.start_http_server()
            await exporter.start_collection()

            # Let it collect for a few seconds
            await asyncio.sleep(10)

            # Get summary
            summary = exporter.get_metrics_summary()
            print(f"Metrics summary: {summary}")

            # Print sample metrics
            print("\nSample metrics:")
            metrics_text = exporter.get_metrics_text()
            print(metrics_text[:1000] + "...")

        finally:
            await exporter.stop_collection()

    # Run test
    asyncio.run(test_exporter())
