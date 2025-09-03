"""
Prometheus Configuration and Setup
Provides comprehensive Prometheus configuration for metrics collection,
alerting rules, and recording rules for the data platform.
"""

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PrometheusTarget:
    """Prometheus scrape target configuration."""

    targets: list[str]
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class ScrapeConfig:
    """Prometheus scrape configuration."""

    job_name: str
    metrics_path: str = "/metrics"
    scrape_interval: str = "15s"
    scrape_timeout: str = "10s"
    scheme: str = "http"
    static_configs: list[PrometheusTarget] = field(default_factory=list)
    relabel_configs: list[dict[str, Any]] = field(default_factory=list)
    metric_relabel_configs: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AlertingRule:
    """Prometheus alerting rule."""

    alert: str
    expr: str
    for_duration: str
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)


@dataclass
class RecordingRule:
    """Prometheus recording rule."""

    record: str
    expr: str
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class RuleGroup:
    """Prometheus rule group."""

    name: str
    interval: str = "30s"
    rules: list[AlertingRule | RecordingRule] = field(default_factory=list)


@dataclass
class PrometheusConfig:
    """Complete Prometheus configuration."""

    global_config: dict[str, Any] = field(default_factory=dict)
    scrape_configs: list[ScrapeConfig] = field(default_factory=list)
    alerting: dict[str, Any] = field(default_factory=dict)
    rule_files: list[str] = field(default_factory=list)
    storage: dict[str, Any] = field(default_factory=dict)
    remote_write: list[dict[str, Any]] = field(default_factory=list)
    remote_read: list[dict[str, Any]] = field(default_factory=list)


class PrometheusConfigGenerator:
    """Generates Prometheus configuration for the data platform."""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.config = PrometheusConfig()
        self._initialize_global_config()
        self._initialize_alerting_config()
        self._initialize_storage_config()

    def _initialize_global_config(self):
        """Initialize global Prometheus configuration."""
        self.config.global_config = {
            "scrape_interval": "15s",
            "evaluation_interval": "15s",
            "scrape_timeout": "10s",
            "external_labels": {
                "cluster": "pwc-data-platform",
                "environment": getattr(settings, "environment", "production"),
                "region": "us-east-1",
            },
        }

    def _initialize_alerting_config(self):
        """Initialize alerting configuration."""
        self.config.alerting = {
            "alertmanagers": [{"static_configs": [{"targets": ["alertmanager:9093"]}]}]
        }

    def _initialize_storage_config(self):
        """Initialize storage configuration."""
        self.config.storage = {
            "tsdb": {"retention.time": "30d", "retention.size": "100GB", "wal_compression": True}
        }

    def add_application_scrape_configs(self):
        """Add scrape configurations for application metrics."""

        # API service metrics
        api_config = ScrapeConfig(
            job_name="api-service",
            metrics_path="/metrics",
            scrape_interval="10s",
            static_configs=[
                PrometheusTarget(
                    targets=["api-service:8000"],
                    labels={"service": "api", "tier": "application", "team": "platform"},
                )
            ],
            relabel_configs=[
                {"source_labels": ["__address__"], "target_label": "__param_target"},
                {"source_labels": ["__param_target"], "target_label": "instance"},
                {"target_label": "__address__", "replacement": "api-service:8000"},
            ],
        )

        # ETL pipeline metrics
        etl_config = ScrapeConfig(
            job_name="etl-pipelines",
            metrics_path="/metrics",
            scrape_interval="30s",
            static_configs=[
                PrometheusTarget(
                    targets=["etl-worker-1:9090", "etl-worker-2:9090"],
                    labels={"service": "etl", "tier": "data-processing", "team": "data"},
                )
            ],
            metric_relabel_configs=[
                {
                    "source_labels": ["__name__"],
                    "regex": "etl_.*",
                    "target_label": "component",
                    "replacement": "etl-pipeline",
                }
            ],
        )

        # Database metrics
        db_config = ScrapeConfig(
            job_name="postgresql",
            metrics_path="/metrics",
            scrape_interval="30s",
            static_configs=[
                PrometheusTarget(
                    targets=["postgres-exporter:9187"],
                    labels={"service": "database", "tier": "data-store", "team": "platform"},
                )
            ],
        )

        # Redis metrics
        redis_config = ScrapeConfig(
            job_name="redis",
            metrics_path="/metrics",
            scrape_interval="30s",
            static_configs=[
                PrometheusTarget(
                    targets=["redis-exporter:9121"],
                    labels={"service": "cache", "tier": "data-store", "team": "platform"},
                )
            ],
        )

        # Monitoring system itself
        monitoring_config = ScrapeConfig(
            job_name="monitoring-system",
            metrics_path="/metrics",
            scrape_interval="15s",
            static_configs=[
                PrometheusTarget(
                    targets=["monitoring-service:8080"],
                    labels={"service": "monitoring", "tier": "observability", "team": "platform"},
                )
            ],
        )

        # Node exporter for system metrics
        node_config = ScrapeConfig(
            job_name="node-exporter",
            metrics_path="/metrics",
            scrape_interval="15s",
            static_configs=[
                PrometheusTarget(
                    targets=["node-exporter:9100"],
                    labels={"service": "system", "tier": "infrastructure", "team": "platform"},
                )
            ],
        )

        # cAdvisor for container metrics
        cadvisor_config = ScrapeConfig(
            job_name="cadvisor",
            metrics_path="/metrics",
            scrape_interval="30s",
            static_configs=[
                PrometheusTarget(
                    targets=["cadvisor:8080"],
                    labels={"service": "containers", "tier": "infrastructure", "team": "platform"},
                )
            ],
        )

        self.config.scrape_configs.extend(
            [
                api_config,
                etl_config,
                db_config,
                redis_config,
                monitoring_config,
                node_config,
                cadvisor_config,
            ]
        )

    def generate_etl_alerting_rules(self) -> RuleGroup:
        """Generate ETL-specific alerting rules."""

        rules = [
            AlertingRule(
                alert="ETLPipelineDown",
                expr='up{job="etl-pipelines"} == 0',
                for_duration="1m",
                labels={"severity": "critical", "team": "data", "component": "etl"},
                annotations={
                    "summary": "ETL pipeline is down",
                    "description": "ETL pipeline {{ $labels.instance }} has been down for more than 1 minute.",
                    "runbook_url": "https://wiki.company.com/runbooks/etl-pipeline-down",
                },
            ),
            AlertingRule(
                alert="ETLProcessingLatencyHigh",
                expr="histogram_quantile(0.95, rate(etl_processing_duration_seconds_bucket[5m])) > 300",
                for_duration="5m",
                labels={"severity": "warning", "team": "data", "component": "etl"},
                annotations={
                    "summary": "ETL processing latency is high",
                    "description": "95th percentile latency is {{ $value }}s for pipeline {{ $labels.pipeline }}",
                    "runbook_url": "https://wiki.company.com/runbooks/etl-high-latency",
                },
            ),
            AlertingRule(
                alert="ETLErrorRateHigh",
                expr="rate(etl_errors_total[5m]) / rate(etl_processed_total[5m]) > 0.05",
                for_duration="3m",
                labels={"severity": "warning", "team": "data", "component": "etl"},
                annotations={
                    "summary": "ETL error rate is high",
                    "description": "Error rate is {{ $value | humanizePercentage }} for pipeline {{ $labels.pipeline }}",
                    "runbook_url": "https://wiki.company.com/runbooks/etl-high-error-rate",
                },
            ),
            AlertingRule(
                alert="DataQualityScoreLow",
                expr="avg_over_time(data_quality_score[10m]) < 0.9",
                for_duration="5m",
                labels={"severity": "warning", "team": "data", "component": "data-quality"},
                annotations={
                    "summary": "Data quality score is below threshold",
                    "description": "Data quality score is {{ $value }} for dataset {{ $labels.dataset }}",
                    "runbook_url": "https://wiki.company.com/runbooks/data-quality-low",
                },
            ),
            AlertingRule(
                alert="ETLMemoryUsageHigh",
                expr='process_resident_memory_bytes{job="etl-pipelines"} > 4e9',
                for_duration="10m",
                labels={"severity": "warning", "team": "data", "component": "etl"},
                annotations={
                    "summary": "ETL process memory usage is high",
                    "description": "Memory usage is {{ $value | humanizeBytes }} for instance {{ $labels.instance }}",
                    "runbook_url": "https://wiki.company.com/runbooks/etl-memory-high",
                },
            ),
        ]

        return RuleGroup(name="etl_alerts", interval="30s", rules=rules)

    def generate_api_alerting_rules(self) -> RuleGroup:
        """Generate API-specific alerting rules."""

        rules = [
            AlertingRule(
                alert="APIHighErrorRate",
                expr='sum(rate(http_requests_total{status=~"[45].."}[5m])) / sum(rate(http_requests_total[5m])) > 0.05',
                for_duration="5m",
                labels={"severity": "critical", "team": "platform", "component": "api"},
                annotations={
                    "summary": "High API error rate detected",
                    "description": "API error rate is {{ $value | humanizePercentage }} which is above the threshold of 5%",
                    "runbook_url": "https://wiki.company.com/runbooks/api-high-error-rate",
                },
            ),
            AlertingRule(
                alert="APIHighLatency",
                expr="histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le)) > 0.5",
                for_duration="10m",
                labels={"severity": "warning", "team": "platform", "component": "api"},
                annotations={
                    "summary": "API latency is high",
                    "description": "95th percentile latency is {{ $value }}s which is above the threshold of 500ms",
                    "runbook_url": "https://wiki.company.com/runbooks/api-high-latency",
                },
            ),
            AlertingRule(
                alert="APILowThroughput",
                expr="sum(rate(http_requests_total[5m])) < 10",
                for_duration="15m",
                labels={"severity": "warning", "team": "platform", "component": "api"},
                annotations={
                    "summary": "API throughput is unusually low",
                    "description": "Request rate is {{ $value }} req/s which is below expected minimum",
                    "runbook_url": "https://wiki.company.com/runbooks/api-low-throughput",
                },
            ),
        ]

        return RuleGroup(name="api_alerts", interval="30s", rules=rules)

    def generate_system_alerting_rules(self) -> RuleGroup:
        """Generate system-level alerting rules."""

        rules = [
            AlertingRule(
                alert="HighCPUUsage",
                expr='100 - (avg by(instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100) > 80',
                for_duration="5m",
                labels={"severity": "warning", "team": "platform", "component": "system"},
                annotations={
                    "summary": "High CPU usage detected",
                    "description": "CPU usage is {{ $value }}% on {{ $labels.instance }}",
                    "runbook_url": "https://wiki.company.com/runbooks/high-cpu",
                },
            ),
            AlertingRule(
                alert="HighMemoryUsage",
                expr="(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes > 0.9",
                for_duration="5m",
                labels={"severity": "warning", "team": "platform", "component": "system"},
                annotations={
                    "summary": "High memory usage detected",
                    "description": "Memory usage is {{ $value | humanizePercentage }} on {{ $labels.instance }}",
                    "runbook_url": "https://wiki.company.com/runbooks/high-memory",
                },
            ),
            AlertingRule(
                alert="DiskSpaceRunningOut",
                expr="(node_filesystem_avail_bytes / node_filesystem_size_bytes) < 0.1",
                for_duration="5m",
                labels={"severity": "critical", "team": "platform", "component": "system"},
                annotations={
                    "summary": "Disk space is running out",
                    "description": "Only {{ $value | humanizePercentage }} disk space remaining on {{ $labels.instance }}",
                    "runbook_url": "https://wiki.company.com/runbooks/disk-space-low",
                },
            ),
            AlertingRule(
                alert="ServiceDown",
                expr="up == 0",
                for_duration="1m",
                labels={"severity": "critical", "team": "platform", "component": "service"},
                annotations={
                    "summary": "Service is down",
                    "description": "{{ $labels.job }} service is down on {{ $labels.instance }}",
                    "runbook_url": "https://wiki.company.com/runbooks/service-down",
                },
            ),
        ]

        return RuleGroup(name="system_alerts", interval="30s", rules=rules)

    def generate_recording_rules(self) -> RuleGroup:
        """Generate recording rules for common queries."""

        rules = [
            # API metrics
            RecordingRule(
                record="api:request_rate_5m",
                expr="sum(rate(http_requests_total[5m])) by (job, method, status)",
            ),
            RecordingRule(
                record="api:error_rate_5m",
                expr='sum(rate(http_requests_total{status=~"[45].."}[5m])) by (job) / sum(rate(http_requests_total[5m])) by (job)',
            ),
            RecordingRule(
                record="api:latency_p95_5m",
                expr="histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (job, le))",
            ),
            # ETL metrics
            RecordingRule(
                record="etl:processing_rate_5m",
                expr="sum(rate(etl_processed_total[5m])) by (pipeline)",
            ),
            RecordingRule(
                record="etl:error_rate_5m",
                expr="sum(rate(etl_errors_total[5m])) by (pipeline) / sum(rate(etl_processed_total[5m])) by (pipeline)",
            ),
            RecordingRule(
                record="etl:processing_latency_p95_5m",
                expr="histogram_quantile(0.95, sum(rate(etl_processing_duration_seconds_bucket[5m])) by (pipeline, le))",
            ),
            # System metrics
            RecordingRule(
                record="instance:cpu_usage:rate5m",
                expr='100 - (avg by(instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
            ),
            RecordingRule(
                record="instance:memory_usage:ratio",
                expr="(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes",
            ),
            RecordingRule(
                record="instance:disk_usage:ratio",
                expr="1 - (node_filesystem_avail_bytes / node_filesystem_size_bytes)",
            ),
        ]

        return RuleGroup(name="recording_rules", interval="30s", rules=rules)

    def add_remote_write_configs(self):
        """Add remote write configurations for long-term storage."""

        # DataDog integration
        self.config.remote_write.append(
            {
                "url": "https://app.datadoghq.com/api/v1/series",
                "basic_auth": {"username": "datadog", "password": "${DD_API_KEY}"},
                "write_relabel_configs": [
                    {
                        "source_labels": ["__name__"],
                        "regex": "(api|etl|data_quality)_.*",
                        "action": "keep",
                    }
                ],
                "queue_config": {
                    "max_samples_per_send": 1000,
                    "batch_send_deadline": "5s",
                    "max_retries": 3,
                },
            }
        )

        # Long-term storage (e.g., Cortex or Thanos)
        self.config.remote_write.append(
            {
                "url": "http://cortex:9009/api/v1/push",
                "queue_config": {
                    "max_samples_per_send": 2000,
                    "batch_send_deadline": "10s",
                    "max_retries": 5,
                },
            }
        )

    def generate_complete_config(self) -> dict[str, Any]:
        """Generate complete Prometheus configuration."""

        # Add scrape configurations
        self.add_application_scrape_configs()

        # Add remote write configurations
        self.add_remote_write_configs()

        # Add rule files
        self.config.rule_files = ["/etc/prometheus/rules/*.yml"]

        # Convert to dictionary
        config_dict = {
            "global": self.config.global_config,
            "scrape_configs": [asdict(sc) for sc in self.config.scrape_configs],
            "alerting": self.config.alerting,
            "rule_files": self.config.rule_files,
            "storage": self.config.storage,
            "remote_write": self.config.remote_write,
            "remote_read": self.config.remote_read,
        }

        return config_dict

    def generate_all_rule_groups(self) -> list[RuleGroup]:
        """Generate all alerting and recording rule groups."""

        return [
            self.generate_etl_alerting_rules(),
            self.generate_api_alerting_rules(),
            self.generate_system_alerting_rules(),
            self.generate_recording_rules(),
        ]

    def export_config(self, output_dir: str = "prometheus") -> None:
        """Export Prometheus configuration and rules to YAML files."""

        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        # Export main configuration
        config = self.generate_complete_config()
        with open(output_path / "prometheus.yml", "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

        # Export rule groups
        rules_dir = output_path / "rules"
        rules_dir.mkdir(exist_ok=True)

        rule_groups = self.generate_all_rule_groups()
        for group in rule_groups:
            group_dict = {"groups": [{"name": group.name, "interval": group.interval, "rules": []}]}

            for rule in group.rules:
                if isinstance(rule, AlertingRule):
                    rule_dict = {
                        "alert": rule.alert,
                        "expr": rule.expr,
                        "for": rule.for_duration,
                        "labels": rule.labels,
                        "annotations": rule.annotations,
                    }
                else:  # RecordingRule
                    rule_dict = {"record": rule.record, "expr": rule.expr, "labels": rule.labels}

                group_dict["groups"][0]["rules"].append(rule_dict)

            with open(rules_dir / f"{group.name}.yml", "w") as f:
                yaml.dump(group_dict, f, default_flow_style=False, sort_keys=False)

        self.logger.info(f"Exported Prometheus configuration to {output_path}")


# Global configuration generator
_prometheus_config = None


def get_prometheus_config() -> PrometheusConfigGenerator:
    """Get global Prometheus configuration generator."""
    global _prometheus_config
    if _prometheus_config is None:
        _prometheus_config = PrometheusConfigGenerator()
    return _prometheus_config


if __name__ == "__main__":
    # Generate and export Prometheus configuration
    generator = PrometheusConfigGenerator()

    # Generate complete configuration
    config = generator.generate_complete_config()
    print("Generated Prometheus configuration")

    # Generate rule groups
    rule_groups = generator.generate_all_rule_groups()
    print(f"Generated {len(rule_groups)} rule groups")

    # Export everything
    generator.export_config()
    print("Exported Prometheus configuration and rules")
