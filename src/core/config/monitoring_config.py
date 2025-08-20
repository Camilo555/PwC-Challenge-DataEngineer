"""
Monitoring and Observability Configuration
Provides comprehensive monitoring settings for production deployment
"""
from __future__ import annotations

from typing import Dict, List, Optional, Any
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .base_config import Environment


class MonitoringConfig(BaseSettings):
    """Configuration for monitoring and observability."""
    
    # Logging configuration
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")  # json, text
    log_file_enabled: bool = Field(default=True)
    log_file_path: str = Field(default="logs/application.log")
    log_rotation_size: str = Field(default="100MB")
    log_retention_days: int = Field(default=30)
    
    # Metrics configuration
    prometheus_enabled: bool = Field(default=False)
    prometheus_host: str = Field(default="localhost")
    prometheus_port: int = Field(default=9090)
    prometheus_metrics_path: str = Field(default="/metrics")
    
    # Grafana configuration
    grafana_enabled: bool = Field(default=False)
    grafana_host: str = Field(default="localhost")
    grafana_port: int = Field(default=3001)
    grafana_admin_password: str = Field(default="admin")
    
    # Elasticsearch/ELK configuration
    elasticsearch_enabled: bool = Field(default=False)
    elasticsearch_host: str = Field(default="localhost")
    elasticsearch_port: int = Field(default=9200)
    
    kibana_enabled: bool = Field(default=False)
    kibana_host: str = Field(default="localhost")
    kibana_port: int = Field(default=5601)
    
    logstash_enabled: bool = Field(default=False)
    logstash_host: str = Field(default="localhost")
    logstash_port: int = Field(default=5044)
    
    # Health check configuration
    health_check_enabled: bool = Field(default=True)
    health_check_interval_seconds: int = Field(default=30)
    health_check_timeout_seconds: int = Field(default=10)
    
    # Performance monitoring
    performance_monitoring_enabled: bool = Field(default=True)
    slow_query_threshold_seconds: float = Field(default=5.0)
    memory_usage_alert_threshold: float = Field(default=0.85)
    cpu_usage_alert_threshold: float = Field(default=0.80)
    
    # Alerting configuration
    alerting_enabled: bool = Field(default=False)
    alert_channels: List[str] = Field(default_factory=lambda: ["email"])
    
    # Email alerts
    smtp_host: Optional[str] = Field(default=None)
    smtp_port: int = Field(default=587)
    smtp_user: Optional[str] = Field(default=None)
    smtp_password: Optional[str] = Field(default=None)
    alert_email_from: str = Field(default="alerts@example.com")
    alert_email_to: List[str] = Field(default_factory=list)
    
    # Slack alerts
    slack_webhook_url: Optional[str] = Field(default=None)
    slack_channel: str = Field(default="#alerts")
    
    # PagerDuty alerts
    pagerduty_service_key: Optional[str] = Field(default=None)
    
    # Tracing configuration
    tracing_enabled: bool = Field(default=False)
    jaeger_enabled: bool = Field(default=False)
    jaeger_host: str = Field(default="localhost")
    jaeger_port: int = Field(default=14268)
    
    # Custom metrics
    custom_metrics_enabled: bool = Field(default=True)
    business_metrics_enabled: bool = Field(default=True)
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow"
    )
    
    def get_environment_overrides(self, environment: Environment) -> Dict[str, Any]:
        """Get environment-specific monitoring overrides."""
        overrides = {
            Environment.DEVELOPMENT: {
                "log_level": "DEBUG",
                "prometheus_enabled": False,
                "grafana_enabled": False,
                "elasticsearch_enabled": False,
                "alerting_enabled": False,
            },
            Environment.TESTING: {
                "log_level": "WARNING", 
                "prometheus_enabled": False,
                "grafana_enabled": False,
                "elasticsearch_enabled": False,
                "alerting_enabled": False,
                "health_check_enabled": False,
            },
            Environment.STAGING: {
                "log_level": "INFO",
                "prometheus_enabled": True,
                "grafana_enabled": True,
                "elasticsearch_enabled": False,
                "alerting_enabled": True,
            },
            Environment.PRODUCTION: {
                "log_level": "WARNING",
                "prometheus_enabled": True,
                "grafana_enabled": True,
                "elasticsearch_enabled": True,
                "kibana_enabled": True,
                "logstash_enabled": True,
                "alerting_enabled": True,
                "tracing_enabled": True,
            }
        }
        
        return overrides.get(environment, {})
    
    def get_prometheus_config(self) -> str:
        """Generate Prometheus configuration."""
        return f"""
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "alert_rules.yml"

scrape_configs:
  - job_name: 'retail-etl-api'
    static_configs:
      - targets: ['api:8000']
    metrics_path: {self.prometheus_metrics_path}
    scrape_interval: 15s

  - job_name: 'spark-master'
    static_configs:
      - targets: ['spark-master:8080']
    metrics_path: '/metrics/json'
    scrape_interval: 30s

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres:5432']
    scrape_interval: 30s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
"""
    
    def get_grafana_dashboards(self) -> Dict[str, Dict]:
        """Get Grafana dashboard configurations."""
        return {
            "retail_etl_overview": {
                "title": "Retail ETL Overview",
                "panels": [
                    {"title": "API Response Time", "type": "graph"},
                    {"title": "ETL Processing Time", "type": "graph"},
                    {"title": "Data Quality Score", "type": "singlestat"},
                    {"title": "Active Connections", "type": "singlestat"},
                ]
            },
            "spark_cluster": {
                "title": "Spark Cluster Metrics", 
                "panels": [
                    {"title": "Cluster CPU Usage", "type": "graph"},
                    {"title": "Cluster Memory Usage", "type": "graph"},
                    {"title": "Running Jobs", "type": "singlestat"},
                    {"title": "Failed Tasks", "type": "singlestat"},
                ]
            },
            "data_pipeline": {
                "title": "Data Pipeline Health",
                "panels": [
                    {"title": "Records Processed", "type": "graph"},
                    {"title": "Error Rate", "type": "graph"},
                    {"title": "Pipeline Status", "type": "table"},
                    {"title": "Data Freshness", "type": "singlestat"},
                ]
            }
        }
    
    def get_alert_rules(self) -> List[Dict]:
        """Get Prometheus alert rules."""
        return [
            {
                "alert": "HighErrorRate",
                "expr": "rate(http_requests_total{status=~'5..'}[5m]) > 0.1",
                "for": "5m",
                "labels": {"severity": "warning"},
                "annotations": {
                    "summary": "High error rate detected",
                    "description": "Error rate is {{ $value }} errors per second"
                }
            },
            {
                "alert": "APIResponseTimeTooHigh",
                "expr": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 2",
                "for": "2m",
                "labels": {"severity": "warning"},
                "annotations": {
                    "summary": "API response time too high",
                    "description": "95th percentile response time is {{ $value }}s"
                }
            },
            {
                "alert": "DataQualityScoreLow", 
                "expr": "data_quality_score < 0.8",
                "for": "1m",
                "labels": {"severity": "critical"},
                "annotations": {
                    "summary": "Data quality score below threshold",
                    "description": "Data quality score is {{ $value }}"
                }
            },
            {
                "alert": "ETLProcessingFailed",
                "expr": "etl_job_status != 1",
                "for": "0m",
                "labels": {"severity": "critical"},
                "annotations": {
                    "summary": "ETL processing failed",
                    "description": "ETL job {{ $labels.job_name }} has failed"
                }
            },
            {
                "alert": "HighMemoryUsage",
                "expr": "process_resident_memory_bytes / (1024*1024*1024) > 2",
                "for": "5m",
                "labels": {"severity": "warning"},
                "annotations": {
                    "summary": "High memory usage detected",
                    "description": "Memory usage is {{ $value }}GB"
                }
            }
        ]