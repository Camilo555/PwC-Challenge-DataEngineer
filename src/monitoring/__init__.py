"""
Enterprise Monitoring System Initialization
Complete monitoring and observability system for the PwC Data Engineering Platform.

This module provides a comprehensive monitoring solution with:
- Real-time performance monitoring and anomaly detection
- Advanced alerting with intelligent escalation
- Distributed tracing across all components
- Comprehensive data quality monitoring
- SLA tracking and compliance reporting
- Automated incident response and remediation
- Enterprise-grade dashboards and reporting
- Capacity planning and optimization recommendations

Usage:
    from monitoring import initialize_monitoring, get_monitoring_status

    # Initialize complete monitoring system
    await initialize_monitoring()

    # Get monitoring status
    status = get_monitoring_status()

    # Generate operational report
    report = generate_monitoring_report()
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.distributed_tracing import get_tracing_manager, setup_tracing
from core.logging import get_logger

# Structured logging and tracing
from core.structured_logging import get_logging_metrics, setup_structured_logging

from .advanced_alerting_sla import get_alerting_sla
from .advanced_health_checks import get_health_check_manager

# Import existing monitoring components for backward compatibility
from .alerting import AlertManager, AlertSeverity, get_alert_manager, trigger_custom_alert
from .dashboard import DashboardDataProvider, MonitoringDashboard
from .datadog_custom_metrics_advanced import get_custom_metrics_advanced
from .etl_comprehensive_observability import get_etl_observability_manager
from .grafana_enterprise_dashboards import export_grafana_dashboards, get_grafana_configurator
from .health_checks import HealthChecker, HealthStatus, check_system_health, get_health_checker
from .metrics_collector import MetricsCollector, get_metrics_collector, record_etl_run
from .security_metrics import SecurityMetricType, SecurityEventSeverity

# Import all monitoring components
from .monitoring_orchestrator_enterprise import (
    get_monitoring_orchestrator,
    start_enterprise_monitoring,
    stop_enterprise_monitoring,
)
from .performance_monitoring_realtime import get_performance_monitor
from .synthetic_monitoring_enterprise import get_synthetic_monitoring

logger = get_logger(__name__)


class MonitoringInitializationError(Exception):
    """Exception raised when monitoring initialization fails"""

    pass


class MonitoringSystemStatus:
    """Monitoring system status and health information"""

    def __init__(self):
        self.initialized = False
        self.orchestrator_running = False
        self.components_status = {}
        self.initialization_time = None
        self.last_health_check = None
        self.error_count = 0
        self.warnings = []

    def to_dict(self) -> dict[str, Any]:
        """Convert status to dictionary"""
        return {
            "initialized": self.initialized,
            "orchestrator_running": self.orchestrator_running,
            "components_status": self.components_status,
            "initialization_time": self.initialization_time.isoformat()
            if self.initialization_time
            else None,
            "last_health_check": self.last_health_check.isoformat()
            if self.last_health_check
            else None,
            "error_count": self.error_count,
            "warnings": self.warnings,
        }


# Global monitoring system status
_monitoring_status = MonitoringSystemStatus()


async def initialize_monitoring(
    enable_distributed_tracing: bool = True,
    enable_structured_logging: bool = True,
    enable_custom_metrics: bool = True,
    enable_alerting: bool = True,
    enable_performance_monitoring: bool = True,
    enable_etl_observability: bool = True,
    enable_health_checks: bool = True,
    enable_synthetic_monitoring: bool = True,
    export_dashboards: bool = True,
    dashboard_output_dir: str = "grafana_dashboards",
) -> bool:
    """
    Initialize the complete enterprise monitoring system.

    Args:
        enable_distributed_tracing: Enable OpenTelemetry distributed tracing
        enable_structured_logging: Enable structured JSON logging
        enable_custom_metrics: Enable custom metrics collection
        enable_alerting: Enable advanced alerting and SLA monitoring
        enable_performance_monitoring: Enable real-time performance monitoring
        enable_etl_observability: Enable ETL pipeline observability
        enable_health_checks: Enable health check monitoring
        enable_synthetic_monitoring: Enable synthetic monitoring
        export_dashboards: Export Grafana dashboard configurations
        dashboard_output_dir: Directory to export Grafana dashboards

    Returns:
        bool: True if initialization successful, False otherwise

    Raises:
        MonitoringInitializationError: If critical initialization fails
    """
    try:
        logger.info("Starting enterprise monitoring system initialization...")
        start_time = datetime.utcnow()

        # Initialize distributed tracing
        if enable_distributed_tracing:
            logger.info("Initializing distributed tracing...")
            setup_tracing()
            _monitoring_status.components_status["distributed_tracing"] = "initialized"
            logger.info("Distributed tracing initialized successfully")

        # Initialize structured logging
        if enable_structured_logging:
            logger.info("Initializing structured logging...")
            setup_structured_logging(__name__)
            _monitoring_status.components_status["structured_logging"] = "initialized"
            logger.info("Structured logging initialized successfully")

        # Initialize custom metrics
        if enable_custom_metrics:
            logger.info("Initializing custom metrics system...")
            custom_metrics = get_custom_metrics_advanced()
            await custom_metrics.start_metrics_collection()
            _monitoring_status.components_status["custom_metrics"] = "running"
            logger.info("Custom metrics system initialized successfully")

        # Initialize health checks
        if enable_health_checks:
            logger.info("Initializing health check system...")
            health_manager = get_health_check_manager()
            await health_manager.start_monitoring()
            _monitoring_status.components_status["health_checks"] = "running"
            logger.info("Health check system initialized successfully")

        # Initialize performance monitoring
        if enable_performance_monitoring:
            logger.info("Initializing performance monitoring...")
            performance_monitor = get_performance_monitor()
            await performance_monitor.start_monitoring()
            _monitoring_status.components_status["performance_monitoring"] = "running"
            logger.info("Performance monitoring initialized successfully")

        # Initialize ETL observability
        if enable_etl_observability:
            logger.info("Initializing ETL observability...")
            get_etl_observability_manager()
            _monitoring_status.components_status["etl_observability"] = "running"
            logger.info("ETL observability initialized successfully")

        # Initialize synthetic monitoring
        if enable_synthetic_monitoring:
            logger.info("Initializing synthetic monitoring...")
            synthetic_monitor = get_synthetic_monitoring()
            await synthetic_monitor.start_monitoring()
            _monitoring_status.components_status["synthetic_monitoring"] = "running"
            logger.info("Synthetic monitoring initialized successfully")

        # Initialize alerting and SLA monitoring
        if enable_alerting:
            logger.info("Initializing alerting and SLA monitoring...")
            alerting_sla = get_alerting_sla()
            await alerting_sla.start_monitoring()
            _monitoring_status.components_status["alerting_sla"] = "running"
            logger.info("Alerting and SLA monitoring initialized successfully")

        # Export Grafana dashboards
        if export_dashboards:
            logger.info("Exporting Grafana dashboard configurations...")
            try:
                exported_dashboards = export_grafana_dashboards(dashboard_output_dir)
                _monitoring_status.components_status["grafana_dashboards"] = (
                    f"exported_{len(exported_dashboards)}_dashboards"
                )
                logger.info(
                    f"Exported {len(exported_dashboards)} Grafana dashboards to {dashboard_output_dir}"
                )
            except Exception as e:
                logger.warning(f"Failed to export Grafana dashboards: {str(e)}")
                _monitoring_status.warnings.append(f"Dashboard export failed: {str(e)}")

        # Initialize monitoring orchestrator (this coordinates all components)
        logger.info("Starting monitoring orchestrator...")
        await start_enterprise_monitoring()
        _monitoring_status.orchestrator_running = True
        _monitoring_status.components_status["orchestrator"] = "running"
        logger.info("Monitoring orchestrator started successfully")

        # Mark system as initialized
        _monitoring_status.initialized = True
        _monitoring_status.initialization_time = start_time
        _monitoring_status.last_health_check = datetime.utcnow()

        # Log initialization summary
        initialization_duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            f"Enterprise monitoring system initialized successfully in {initialization_duration:.2f} seconds"
        )
        logger.info(f"Active components: {list(_monitoring_status.components_status.keys())}")

        # Run initial health check
        await _run_initial_health_check()

        return True

    except Exception as e:
        logger.error(f"Failed to initialize monitoring system: {str(e)}", exc_info=True)
        _monitoring_status.error_count += 1
        raise MonitoringInitializationError(f"Monitoring initialization failed: {str(e)}")


async def shutdown_monitoring() -> bool:
    """
    Shutdown the enterprise monitoring system.

    Returns:
        bool: True if shutdown successful, False otherwise
    """
    try:
        logger.info("Shutting down enterprise monitoring system...")

        # Stop monitoring orchestrator
        if _monitoring_status.orchestrator_running:
            logger.info("Stopping monitoring orchestrator...")
            await stop_enterprise_monitoring()
            _monitoring_status.orchestrator_running = False

        # Stop individual components
        try:
            # Stop alerting
            if "alerting_sla" in _monitoring_status.components_status:
                alerting_sla = get_alerting_sla()
                await alerting_sla.stop_monitoring()

            # Stop performance monitoring
            if "performance_monitoring" in _monitoring_status.components_status:
                performance_monitor = get_performance_monitor()
                await performance_monitor.stop_monitoring()

            # Stop synthetic monitoring
            if "synthetic_monitoring" in _monitoring_status.components_status:
                synthetic_monitor = get_synthetic_monitoring()
                await synthetic_monitor.stop_monitoring()

            # Stop health checks
            if "health_checks" in _monitoring_status.components_status:
                health_manager = get_health_check_manager()
                await health_manager.stop_monitoring()

            # Stop custom metrics
            if "custom_metrics" in _monitoring_status.components_status:
                custom_metrics = get_custom_metrics_advanced()
                await custom_metrics.stop_metrics_collection()

            # Shutdown tracing
            if "distributed_tracing" in _monitoring_status.components_status:
                tracing_manager = get_tracing_manager()
                tracing_manager.shutdown()

        except Exception as e:
            logger.warning(f"Some components failed to shutdown cleanly: {str(e)}")

        # Reset status
        _monitoring_status.initialized = False
        _monitoring_status.components_status.clear()

        logger.info("Enterprise monitoring system shutdown completed")
        return True

    except Exception as e:
        logger.error(f"Error during monitoring system shutdown: {str(e)}")
        return False


def get_monitoring_status() -> dict[str, Any]:
    """Get current monitoring system status"""
    status = _monitoring_status.to_dict()

    # Add runtime information
    if _monitoring_status.initialized and _monitoring_status.initialization_time:
        uptime = datetime.utcnow() - _monitoring_status.initialization_time
        status["uptime_seconds"] = uptime.total_seconds()
        status["uptime_human"] = str(uptime)

    # Add component count
    status["total_components"] = len(_monitoring_status.components_status)
    status["active_components"] = len(
        [c for c in _monitoring_status.components_status.values() if "running" in c]
    )

    return status


async def generate_monitoring_report(hours: int = 24) -> dict[str, Any]:
    """
    Generate comprehensive monitoring report.

    Args:
        hours: Time window for the report in hours

    Returns:
        Dict containing comprehensive monitoring report
    """
    try:
        if not _monitoring_status.initialized:
            return {"error": "Monitoring system not initialized"}

        logger.info(f"Generating monitoring report for last {hours} hours...")

        # Get orchestrator report
        orchestrator = get_monitoring_orchestrator()
        operational_report = await orchestrator.generate_operational_report(hours)

        # Get system overview
        system_overview = await orchestrator.get_system_overview()

        # Get logging metrics
        logging_metrics = get_logging_metrics()

        # Get tracing stats
        tracing_manager = get_tracing_manager()
        tracing_stats = tracing_manager.get_tracing_stats()

        # Get component-specific reports
        component_reports = {}

        # Performance monitoring report
        if "performance_monitoring" in _monitoring_status.components_status:
            try:
                performance_monitor = get_performance_monitor()
                component_reports["performance"] = performance_monitor.get_performance_summary(
                    hours
                )
            except Exception as e:
                logger.warning(f"Failed to get performance report: {str(e)}")

        # ETL observability report
        if "etl_observability" in _monitoring_status.components_status:
            try:
                etl_observability = get_etl_observability_manager()
                component_reports["etl"] = {
                    "active_pipelines": etl_observability.get_active_pipelines_status(),
                    "recent_alerts": etl_observability.get_recent_alerts(hours),
                }
            except Exception as e:
                logger.warning(f"Failed to get ETL report: {str(e)}")

        # Health checks report
        if "health_checks" in _monitoring_status.components_status:
            try:
                health_manager = get_health_check_manager()
                component_reports["health_checks"] = await health_manager.run_all_health_checks()
            except Exception as e:
                logger.warning(f"Failed to get health check report: {str(e)}")

        # Synthetic monitoring report
        if "synthetic_monitoring" in _monitoring_status.components_status:
            try:
                synthetic_monitor = get_synthetic_monitoring()
                component_reports["synthetic"] = synthetic_monitor.get_monitoring_summary()
            except Exception as e:
                logger.warning(f"Failed to get synthetic monitoring report: {str(e)}")

        # Compile comprehensive report
        comprehensive_report = {
            "report_metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "time_window_hours": hours,
                "monitoring_system_version": "1.0.0",
                "report_type": "comprehensive_monitoring",
            },
            "monitoring_system_status": get_monitoring_status(),
            "system_overview": system_overview.__dict__
            if hasattr(system_overview, "__dict__")
            else system_overview,
            "operational_report": operational_report,
            "component_reports": component_reports,
            "logging_metrics": logging_metrics,
            "tracing_statistics": tracing_stats,
            "recommendations": await _generate_monitoring_recommendations(system_overview),
            "summary": {
                "overall_health": system_overview.overall_health_score
                if hasattr(system_overview, "overall_health_score")
                else 0,
                "active_alerts": system_overview.active_alerts
                if hasattr(system_overview, "active_alerts")
                else 0,
                "sla_compliance": system_overview.sla_compliance_average
                if hasattr(system_overview, "sla_compliance_average")
                else 0,
                "system_uptime": _monitoring_status.components_status.get(
                    "orchestrator", "unknown"
                ),
                "monitoring_coverage": "comprehensive",
            },
        }

        logger.info("Monitoring report generated successfully")
        return comprehensive_report

    except Exception as e:
        logger.error(f"Failed to generate monitoring report: {str(e)}")
        return {"error": str(e), "timestamp": datetime.utcnow().isoformat(), "partial_data": True}


async def _run_initial_health_check():
    """Run initial health check after system initialization"""
    try:
        logger.info("Running initial health check...")

        # Check all initialized components
        health_issues = []

        # Test custom metrics
        if "custom_metrics" in _monitoring_status.components_status:
            try:
                custom_metrics = get_custom_metrics_advanced()
                await custom_metrics.submit_metric("system.initialization", 1.0)
            except Exception as e:
                health_issues.append(f"Custom metrics: {str(e)}")

        # Test performance monitoring
        if "performance_monitoring" in _monitoring_status.components_status:
            try:
                performance_monitor = get_performance_monitor()
                await performance_monitor.submit_metric("system.health_check", 1.0)
            except Exception as e:
                health_issues.append(f"Performance monitoring: {str(e)}")

        # Log health check results
        if health_issues:
            logger.warning(f"Initial health check found {len(health_issues)} issues:")
            for issue in health_issues:
                logger.warning(f"  - {issue}")
            _monitoring_status.warnings.extend(health_issues)
        else:
            logger.info("Initial health check completed successfully - all components healthy")

        _monitoring_status.last_health_check = datetime.utcnow()

    except Exception as e:
        logger.error(f"Initial health check failed: {str(e)}")
        _monitoring_status.error_count += 1


async def _generate_monitoring_recommendations(system_overview) -> list[str]:
    """Generate monitoring system recommendations"""
    recommendations = []

    if not hasattr(system_overview, "overall_health_score"):
        return ["Unable to generate recommendations - system overview unavailable"]

    # Health-based recommendations
    if system_overview.overall_health_score < 80:
        recommendations.append(
            "CRITICAL: System health below 80% - immediate investigation required"
        )
    elif system_overview.overall_health_score < 90:
        recommendations.append("Review system performance - health score below optimal threshold")

    # SLA-based recommendations
    if hasattr(system_overview, "sla_compliance_average"):
        if system_overview.sla_compliance_average < 95:
            recommendations.append(
                "SLA compliance below target - review service performance and capacity"
            )

    # Alert-based recommendations
    if hasattr(system_overview, "active_alerts") and system_overview.active_alerts > 10:
        recommendations.append(
            "High alert volume detected - consider alert optimization and noise reduction"
        )

    # Component-specific recommendations
    component_count = len(_monitoring_status.components_status)
    if component_count < 6:
        recommendations.append(
            "Consider enabling additional monitoring components for better coverage"
        )

    # Proactive recommendations
    recommendations.extend(
        [
            "Schedule regular monitoring system health reviews",
            "Update alerting thresholds based on historical performance data",
            "Consider implementing predictive analytics for capacity planning",
            "Review and update monitoring documentation and runbooks",
        ]
    )

    return recommendations[:8]  # Return top 8 recommendations


# Convenience functions for common operations
async def quick_start_monitoring():
    """Quick start monitoring with default configuration"""
    return await initialize_monitoring()


def is_monitoring_healthy() -> bool:
    """Check if monitoring system is healthy"""
    return (
        _monitoring_status.initialized
        and _monitoring_status.orchestrator_running
        and _monitoring_status.error_count == 0
    )


def get_component_status(component_name: str) -> str | None:
    """Get status of specific monitoring component"""
    return _monitoring_status.components_status.get(component_name)


class SecurityMonitoringFramework:
    """
    Main entry point for the enterprise security monitoring framework.
    Provides unified access to all security monitoring capabilities.
    """

    def __init__(self):
        self.logger = get_logger(__name__)

        # Initialize core components
        self.metrics_collector = get_security_metrics_collector()
        self.observability = get_security_observability()
        self.compliance_dashboard = get_compliance_dashboard()
        self.alerting_system = get_security_alerting_system()
        self.analytics = get_security_analytics()
        self.stack_orchestrator = get_security_monitoring_orchestrator()

        self._initialized = False
        self.logger.info("Security monitoring framework initialized")

    async def initialize(self, setup_integrations: bool = True):
        """Initialize the complete security monitoring framework"""

        if self._initialized:
            self.logger.warning("Security monitoring framework already initialized")
            return

        self.logger.info("Initializing enterprise security monitoring framework...")

        try:
            # Start background tasks for core components
            await self._start_core_components()

            # Setup default alert rules
            await self._setup_default_alert_rules()

            # Initialize monitoring stack integrations
            if setup_integrations:
                await self.stack_orchestrator.setup_complete_monitoring_stack()

            # Start compliance monitoring
            await self.compliance_dashboard.start_monitoring()

            # Start security alerting background tasks
            await self.alerting_system.start_background_tasks()

            # Start security metrics background processing
            await self.metrics_collector.start_background_processing()

            self._initialized = True
            self.logger.info("Security monitoring framework initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize security monitoring framework: {e}")
            raise

    async def _start_core_components(self):
        """Start core monitoring components"""

        # Start observability cleanup tasks
        asyncio.create_task(self.observability.cleanup_old_traces(retention_hours=168))

        self.logger.info("Core security monitoring components started")

    async def _setup_default_alert_rules(self):
        """Setup default security alert rules"""

        # Threat detection rules
        threat_rule = create_threat_detection_rule(
            rule_id="default_threat_detection",
            name="High Threat Detection Rate",
            severity_threshold=SecurityEventSeverity.MEDIUM,
            frequency_threshold=10,
            time_window_minutes=15,
            channels=[AlertChannel.EMAIL, AlertChannel.CONSOLE],
        )
        self.alerting_system.add_alert_rule(threat_rule)

        # DLP violation rules
        dlp_rule = create_dlp_violation_rule(
            rule_id="default_dlp_violations",
            name="Data Loss Prevention Violations",
            severity_threshold=SecurityEventSeverity.MEDIUM,
            frequency_threshold=5,
            channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
        )
        self.alerting_system.add_alert_rule(dlp_rule)

        # Compliance violation rules
        compliance_rule = create_compliance_violation_rule(
            rule_id="default_compliance_violations",
            name="Compliance Framework Violations",
            channels=[AlertChannel.EMAIL, AlertChannel.TEAMS],
        )
        self.alerting_system.add_alert_rule(compliance_rule)

        self.logger.info("Default security alert rules configured")

    async def get_security_status_overview(self) -> dict:
        """Get comprehensive security status overview"""

        try:
            # Get posture scores from analytics
            security_analytics = self.analytics.posture_analyzer
            posture_trends = security_analytics.analyze_security_posture_trends(
                AnalyticsTimeframe.DAILY, 7
            )

            # Get threat metrics
            threat_metrics = self.metrics_collector.get_threat_detection_metrics(24)

            # Get compliance status
            compliance_data = await self.compliance_dashboard.get_dashboard_data()

            # Get active alerts
            active_alerts = self.alerting_system.get_active_alerts()

            # Get monitoring stack status
            stack_status = await self.stack_orchestrator.get_monitoring_status()

            overview = {
                "timestamp": self._get_current_timestamp(),
                "framework_status": "operational" if self._initialized else "initializing",
                "security_posture": {
                    "overall_score": self._calculate_overall_security_score(posture_trends),
                    "trend_analysis": {
                        name: {
                            "direction": trend.trend_direction,
                            "strength": trend.trend_strength,
                            "current_value": trend.current_value,
                        }
                        for name, trend in posture_trends.items()
                    },
                },
                "threat_detection": {
                    "threats_detected_24h": threat_metrics.get("total_threats_detected", 0),
                    "detection_rate_per_hour": threat_metrics.get("detection_rate_per_hour", 0),
                    "block_rate": threat_metrics.get("block_rate", 0),
                    "top_threat_types": threat_metrics.get("top_threat_types", []),
                },
                "compliance_status": {
                    "overall_score": compliance_data["overview"]["overall_compliance_score"],
                    "compliant_frameworks": compliance_data["overview"]["compliant_frameworks"],
                    "total_violations": compliance_data["overview"]["total_violations"],
                    "overdue_violations": compliance_data["overview"]["overdue_violations"],
                },
                "alerting_status": {
                    "active_alerts": len(active_alerts),
                    "critical_alerts": len(
                        [a for a in active_alerts if a.severity == SecurityEventSeverity.CRITICAL]
                    ),
                    "alert_distribution": self._get_alert_severity_distribution(active_alerts),
                },
                "monitoring_integrations": stack_status["integrations"],
                "component_health": {
                    "metrics_collector": "healthy",
                    "observability": "healthy",
                    "compliance_dashboard": "healthy",
                    "alerting_system": "healthy",
                    "analytics_engine": "healthy",
                },
            }

            return overview

        except Exception as e:
            self.logger.error(f"Failed to get security status overview: {e}")
            return {
                "timestamp": self._get_current_timestamp(),
                "framework_status": "error",
                "error": str(e),
            }

    def _calculate_overall_security_score(self, posture_trends: dict) -> float:
        """Calculate overall security score from posture trends"""

        if not posture_trends:
            return 50.0  # Neutral score if no data

        scores = []
        for trend in posture_trends.values():
            if trend.current_value:
                # Normalize different metric types to 0-100 scale
                if "rate" in trend.metric_name or "score" in trend.metric_name:
                    scores.append(min(100, max(0, trend.current_value * 100)))
                else:
                    scores.append(min(100, max(0, trend.current_value)))

        return sum(scores) / len(scores) if scores else 50.0

    def _get_alert_severity_distribution(self, active_alerts: list) -> dict:
        """Get distribution of alerts by severity"""

        distribution = {severity.value: 0 for severity in SecurityEventSeverity}

        for alert in active_alerts:
            distribution[alert.severity.value] += 1

        return distribution

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime

        return datetime.now().isoformat()

    async def record_security_event(
        self,
        event_type: SecurityMetricType,
        severity: SecurityEventSeverity,
        user_id: str | None = None,
        source_ip: str | None = None,
        resource_id: str = "unknown",
        outcome: str = "processed",
        details: dict | None = None,
    ):
        """Record a security event through the monitoring framework"""

        # Create security metric event
        event = SecurityMetricEvent(
            event_id=f"event_{self._get_current_timestamp()}",
            metric_type=event_type,
            severity=severity,
            user_id=user_id,
            source_ip=source_ip,
            resource_id=resource_id,
            action="security_event",
            outcome=outcome,
            details=details or {},
        )

        # Record in metrics collector
        self.metrics_collector.record_security_event(event)

        # Process through alerting system
        await self.alerting_system.process_security_event(event)

        self.logger.debug(f"Recorded security event: {event.event_id}")

    async def generate_security_report(
        self,
        report_type: str = "executive_summary",
        period_days: int = 7,
        format_type: ReportFormat = ReportFormat.JSON,
    ) -> str:
        """Generate security report"""

        if report_type == "executive_summary":
            return await self.analytics.generate_executive_summary_report(period_days, format_type)
        elif report_type == "detailed_analysis":
            return await self.analytics.generate_detailed_security_report(
                period_days, True, True, format_type
            )
        else:
            raise ValueError(f"Unknown report type: {report_type}")

    async def shutdown(self):
        """Gracefully shutdown the security monitoring framework"""

        self.logger.info("Shutting down security monitoring framework...")

        try:
            # Stop background tasks
            await self.alerting_system.stop_background_tasks()
            await self.compliance_dashboard.stop_monitoring()
            await self.metrics_collector.stop_background_processing()
            await self.stack_orchestrator.stop_monitoring()

            self._initialized = False
            self.logger.info("Security monitoring framework shutdown complete")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")


# Global framework instance
_security_monitoring_framework: SecurityMonitoringFramework | None = None


def get_security_monitoring_framework() -> SecurityMonitoringFramework:
    """Get global security monitoring framework instance"""
    global _security_monitoring_framework
    if _security_monitoring_framework is None:
        _security_monitoring_framework = SecurityMonitoringFramework()
    return _security_monitoring_framework


# Convenience functions for easy integration
async def initialize_security_monitoring(
    setup_integrations: bool = True,
) -> SecurityMonitoringFramework:
    """Initialize and return the security monitoring framework"""
    framework = get_security_monitoring_framework()
    await framework.initialize(setup_integrations)
    return framework


async def record_security_event_simple(
    event_type: str,
    severity: str,
    user_id: str | None = None,
    resource_id: str = "unknown",
    details: dict | None = None,
):
    """Simple function to record security events"""

    framework = get_security_monitoring_framework()

    # Convert string types to enums
    try:
        metric_type = SecurityMetricType(event_type)
    except ValueError:
        metric_type = SecurityMetricType.THREAT_DETECTION

    try:
        event_severity = SecurityEventSeverity(severity)
    except ValueError:
        event_severity = SecurityEventSeverity.MEDIUM

    await framework.record_security_event(
        metric_type, event_severity, user_id, None, resource_id, "processed", details
    )


# Enhanced __all__ with both existing and new security monitoring components
__all__ = [
    # Existing monitoring components
    "MetricsCollector",
    "get_metrics_collector",
    "record_etl_run",
    "AlertManager",
    "get_alert_manager",
    "trigger_custom_alert",
    "AlertSeverity",
    "MonitoringDashboard",
    "DashboardDataProvider",
    "HealthChecker",
    "get_health_checker",
    "check_system_health",
    "HealthStatus",
    # Security monitoring framework
    "SecurityMonitoringFramework",
    "get_security_monitoring_framework",
    "initialize_security_monitoring",
    "record_security_event_simple",
    # Core security components
    "SecurityMetricsCollector",
    "get_security_metrics_collector",
    "get_security_observability",
    "get_compliance_dashboard",
    "get_security_alerting_system",
    "get_security_analytics",
    "get_security_monitoring_orchestrator",
    # Enums and types
    "SecurityEventSeverity",
    "SecurityMetricType",
    "SecurityTraceType",
    "AlertChannel",
    "AlertStatus",
    "ComplianceStatus",
    "AnalyticsTimeframe",
    "ReportFormat",
    # Event recording functions
    "record_threat_detection",
    "record_dlp_violation",
    "record_compliance_check",
    "record_access_control_decision",
    # Alert rule creation
    "create_threat_detection_rule",
    "create_dlp_violation_rule",
    "create_compliance_violation_rule",
    # Decorators and context managers
    "trace_security_operation",
    "security_workflow_context",
    # Integrations
    "PrometheusIntegration",
    "GrafanaIntegration",
    "DataDogSecurityIntegration",
    "initialize_security_monitoring_stack",
]
