"""
Monitoring and Observability Configuration
Provides comprehensive monitoring settings for production deployment
"""
from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings

from .base_config import Environment


class MonitoringConfig(BaseSettings):
    """Configuration for monitoring and observability."""
    
    # Logging configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(default="json", env="LOG_FORMAT")  # json, text
    log_file_enabled: bool = Field(default=True, env="LOG_FILE_ENABLED")
    log_file_path: str = Field(default="logs/application.log", env="LOG_FILE_PATH")
    log_rotation_size: str = Field(default="100MB", env="LOG_ROTATION_SIZE")
    log_retention_days: int = Field(default=30, env="LOG_RETENTION_DAYS")
    
    # Metrics configuration
    prometheus_enabled: bool = Field(default=False, env="PROMETHEUS_ENABLED")
    prometheus_host: str = Field(default="localhost", env="PROMETHEUS_HOST")
    prometheus_port: int = Field(default=9090, env="PROMETHEUS_PORT")
    prometheus_metrics_path: str = Field(default="/metrics", env="PROMETHEUS_METRICS_PATH")
    
    # Grafana configuration
    grafana_enabled: bool = Field(default=False, env="GRAFANA_ENABLED")
    grafana_host: str = Field(default="localhost", env="GRAFANA_HOST")
    grafana_port: int = Field(default=3001, env="GRAFANA_PORT")
    grafana_admin_password: str = Field(default="admin", env="GRAFANA_ADMIN_PASSWORD")
    
    # Elasticsearch/ELK configuration
    elasticsearch_enabled: bool = Field(default=False, env="ELASTICSEARCH_ENABLED")
    elasticsearch_host: str = Field(default="localhost", env="ELASTICSEARCH_HOST")
    elasticsearch_port: int = Field(default=9200, env="ELASTICSEARCH_PORT")
    
    kibana_enabled: bool = Field(default=False, env="KIBANA_ENABLED")
    kibana_host: str = Field(default="localhost", env="KIBANA_HOST")
    kibana_port: int = Field(default=5601, env="KIBANA_PORT")
    
    logstash_enabled: bool = Field(default=False, env="LOGSTASH_ENABLED")
    logstash_host: str = Field(default="localhost", env="LOGSTASH_HOST")
    logstash_port: int = Field(default=5044, env="LOGSTASH_PORT")
    
    # Health check configuration
    health_check_enabled: bool = Field(default=True, env="HEALTH_CHECK_ENABLED")
    health_check_interval_seconds: int = Field(default=30, env="HEALTH_CHECK_INTERVAL_SECONDS")
    health_check_timeout_seconds: int = Field(default=10, env="HEALTH_CHECK_TIMEOUT_SECONDS")
    
    # Performance monitoring
    performance_monitoring_enabled: bool = Field(default=True, env="PERFORMANCE_MONITORING_ENABLED")
    slow_query_threshold_seconds: float = Field(default=5.0, env="SLOW_QUERY_THRESHOLD_SECONDS")
    memory_usage_alert_threshold: float = Field(default=0.85, env="MEMORY_USAGE_ALERT_THRESHOLD")
    cpu_usage_alert_threshold: float = Field(default=0.80, env="CPU_USAGE_ALERT_THRESHOLD")
    
    # Alerting configuration
    alerting_enabled: bool = Field(default=False, env="ALERTING_ENABLED")
    alert_channels: List[str] = Field(default_factory=lambda: ["email"], env="ALERT_CHANNELS")
    
    # Email alerts
    smtp_host: Optional[str] = Field(default=None, env="SMTP_HOST")
    smtp_port: int = Field(default=587, env="SMTP_PORT")
    smtp_user: Optional[str] = Field(default=None, env="SMTP_USER")
    smtp_password: Optional[str] = Field(default=None, env="SMTP_PASSWORD")
    alert_email_from: str = Field(default="alerts@example.com", env="ALERT_EMAIL_FROM")
    alert_email_to: List[str] = Field(default_factory=list, env="ALERT_EMAIL_TO")
    
    # Slack alerts
    slack_webhook_url: Optional[str] = Field(default=None, env="SLACK_WEBHOOK_URL")
    slack_channel: str = Field(default="#alerts", env="SLACK_CHANNEL")
    
    # PagerDuty alerts
    pagerduty_service_key: Optional[str] = Field(default=None, env="PAGERDUTY_SERVICE_KEY")
    
    # Tracing configuration
    tracing_enabled: bool = Field(default=False, env="TRACING_ENABLED")
    jaeger_enabled: bool = Field(default=False, env="JAEGER_ENABLED")
    jaeger_host: str = Field(default="localhost", env="JAEGER_HOST")
    jaeger_port: int = Field(default=14268, env="JAEGER_PORT")
    
    # Custom metrics
    custom_metrics_enabled: bool = Field(default=True, env="CUSTOM_METRICS_ENABLED")
    business_metrics_enabled: bool = Field(default=True, env="BUSINESS_METRICS_ENABLED")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"
    
    def get_environment_overrides(self, environment: Environment) -> Dict[str, any]:
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