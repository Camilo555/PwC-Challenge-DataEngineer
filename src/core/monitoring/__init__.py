"""
Core Monitoring Module
Provides comprehensive monitoring, alerting, and observability capabilities.
"""
from core.monitoring.metrics import (
    MetricsCollector,
    MetricType,
    MetricPoint,
    ETLJobMetrics,
    PrometheusExporter,
    MetricsReporter,
    track_execution_time,
    track_async_execution_time,
    default_collector
)

from core.monitoring.health_checks import (
    HealthStatus,
    HealthCheckResult,
    BaseHealthCheck,
    DatabaseHealthCheck,
    RedisHealthCheck,
    SystemResourcesHealthCheck,
    FileSystemHealthCheck,
    CustomHealthCheck,
    HealthCheckManager,
    health_manager,
    setup_basic_health_checks,
    run_health_check_loop
)

from core.monitoring.alerting import (
    AlertSeverity,
    AlertStatus,
    Alert,
    AlertRule,
    BaseAlertChannel,
    EmailAlertChannel,
    WebhookAlertChannel,
    SlackAlertChannel,
    LogAlertChannel,
    AlertManager,
    create_health_check_rule,
    create_etl_failure_rule,
    create_system_resource_rule,
    alert_manager
)

from core.monitoring.dashboard import (
    MonitoringDashboard,
    create_monitoring_dashboard,
    setup_monitoring_stack
)

# Version info
__version__ = "1.0.0"

# Export main components
__all__ = [
    # Metrics
    "MetricsCollector",
    "MetricType", 
    "MetricPoint",
    "ETLJobMetrics",
    "PrometheusExporter",
    "MetricsReporter",
    "track_execution_time",
    "track_async_execution_time",
    "default_collector",
    
    # Health Checks
    "HealthStatus",
    "HealthCheckResult", 
    "BaseHealthCheck",
    "DatabaseHealthCheck",
    "RedisHealthCheck",
    "SystemResourcesHealthCheck",
    "FileSystemHealthCheck",
    "CustomHealthCheck",
    "HealthCheckManager",
    "health_manager",
    "setup_basic_health_checks",
    "run_health_check_loop",
    
    # Alerting
    "AlertSeverity",
    "AlertStatus",
    "Alert",
    "AlertRule", 
    "BaseAlertChannel",
    "EmailAlertChannel",
    "WebhookAlertChannel",
    "SlackAlertChannel",
    "LogAlertChannel",
    "AlertManager",
    "create_health_check_rule",
    "create_etl_failure_rule", 
    "create_system_resource_rule",
    "alert_manager",
    
    # Dashboard
    "MonitoringDashboard",
    "create_monitoring_dashboard",
    "setup_monitoring_stack"
]