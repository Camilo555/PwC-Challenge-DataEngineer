"""
Enterprise Security Monitoring Framework
Comprehensive enterprise security monitoring and observability system
that integrates with existing infrastructure for complete security visibility.
"""
import asyncio
from typing import Optional

from core.logging import get_logger

# Import existing monitoring components
from .alerting import AlertManager, AlertSeverity, get_alert_manager, trigger_custom_alert
from .dashboard import DashboardDataProvider, MonitoringDashboard
from .health_checks import HealthChecker, HealthStatus, check_system_health, get_health_checker
from .metrics_collector import MetricsCollector, get_metrics_collector, record_etl_run

# Import all security monitoring components
from .security_metrics import (
    SecurityMetricsCollector, get_security_metrics_collector,
    SecurityEventSeverity, SecurityMetricType, SecurityMetricEvent,
    record_threat_detection, record_dlp_violation, 
    record_compliance_check, record_access_control_decision
)

from .security_observability import (
    get_security_observability, SecurityTraceType, SecurityContextField,
    trace_security_operation, security_workflow_context
)

from .compliance_dashboard import (
    get_compliance_dashboard, ComplianceStatus, ComplianceViolation,
    ComplianceFrameworkStatus
)

from .security_alerting import (
    get_security_alerting_system, AlertChannel, AlertStatus, AlertRule,
    create_threat_detection_rule, create_dlp_violation_rule, create_compliance_violation_rule
)

from .security_analytics import (
    get_security_analytics, AnalyticsTimeframe, ReportFormat,
    SecurityTrend, AttackPattern, UserBehaviorProfile
)

from .security_stack_integration import (
    get_security_monitoring_orchestrator, initialize_security_monitoring_stack,
    PrometheusIntegration, GrafanaIntegration, DataDogSecurityIntegration
)

logger = get_logger(__name__)


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
            channels=[AlertChannel.EMAIL, AlertChannel.CONSOLE]
        )
        self.alerting_system.add_alert_rule(threat_rule)
        
        # DLP violation rules
        dlp_rule = create_dlp_violation_rule(
            rule_id="default_dlp_violations",
            name="Data Loss Prevention Violations",
            severity_threshold=SecurityEventSeverity.MEDIUM,
            frequency_threshold=5,
            channels=[AlertChannel.EMAIL, AlertChannel.SLACK]
        )
        self.alerting_system.add_alert_rule(dlp_rule)
        
        # Compliance violation rules
        compliance_rule = create_compliance_violation_rule(
            rule_id="default_compliance_violations",
            name="Compliance Framework Violations",
            channels=[AlertChannel.EMAIL, AlertChannel.TEAMS]
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
                'timestamp': self._get_current_timestamp(),
                'framework_status': 'operational' if self._initialized else 'initializing',
                'security_posture': {
                    'overall_score': self._calculate_overall_security_score(posture_trends),
                    'trend_analysis': {
                        name: {
                            'direction': trend.trend_direction,
                            'strength': trend.trend_strength,
                            'current_value': trend.current_value
                        }
                        for name, trend in posture_trends.items()
                    }
                },
                'threat_detection': {
                    'threats_detected_24h': threat_metrics.get('total_threats_detected', 0),
                    'detection_rate_per_hour': threat_metrics.get('detection_rate_per_hour', 0),
                    'block_rate': threat_metrics.get('block_rate', 0),
                    'top_threat_types': threat_metrics.get('top_threat_types', [])
                },
                'compliance_status': {
                    'overall_score': compliance_data['overview']['overall_compliance_score'],
                    'compliant_frameworks': compliance_data['overview']['compliant_frameworks'],
                    'total_violations': compliance_data['overview']['total_violations'],
                    'overdue_violations': compliance_data['overview']['overdue_violations']
                },
                'alerting_status': {
                    'active_alerts': len(active_alerts),
                    'critical_alerts': len([a for a in active_alerts if a.severity == SecurityEventSeverity.CRITICAL]),
                    'alert_distribution': self._get_alert_severity_distribution(active_alerts)
                },
                'monitoring_integrations': stack_status['integrations'],
                'component_health': {
                    'metrics_collector': 'healthy',
                    'observability': 'healthy', 
                    'compliance_dashboard': 'healthy',
                    'alerting_system': 'healthy',
                    'analytics_engine': 'healthy'
                }
            }
            
            return overview
            
        except Exception as e:
            self.logger.error(f"Failed to get security status overview: {e}")
            return {
                'timestamp': self._get_current_timestamp(),
                'framework_status': 'error',
                'error': str(e)
            }
    
    def _calculate_overall_security_score(self, posture_trends: dict) -> float:
        """Calculate overall security score from posture trends"""
        
        if not posture_trends:
            return 50.0  # Neutral score if no data
        
        scores = []
        for trend in posture_trends.values():
            if trend.current_value:
                # Normalize different metric types to 0-100 scale
                if 'rate' in trend.metric_name or 'score' in trend.metric_name:
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
        user_id: Optional[str] = None,
        source_ip: Optional[str] = None,
        resource_id: str = "unknown",
        outcome: str = "processed",
        details: Optional[dict] = None
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
            details=details or {}
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
        format_type: ReportFormat = ReportFormat.JSON
    ) -> str:
        """Generate security report"""
        
        if report_type == "executive_summary":
            return await self.analytics.generate_executive_summary_report(
                period_days, format_type
            )
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
_security_monitoring_framework: Optional[SecurityMonitoringFramework] = None

def get_security_monitoring_framework() -> SecurityMonitoringFramework:
    """Get global security monitoring framework instance"""
    global _security_monitoring_framework
    if _security_monitoring_framework is None:
        _security_monitoring_framework = SecurityMonitoringFramework()
    return _security_monitoring_framework


# Convenience functions for easy integration
async def initialize_security_monitoring(setup_integrations: bool = True) -> SecurityMonitoringFramework:
    """Initialize and return the security monitoring framework"""
    framework = get_security_monitoring_framework()
    await framework.initialize(setup_integrations)
    return framework


async def record_security_event_simple(
    event_type: str,
    severity: str,
    user_id: Optional[str] = None,
    resource_id: str = "unknown",
    details: Optional[dict] = None
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
    'SecurityMonitoringFramework',
    'get_security_monitoring_framework', 
    'initialize_security_monitoring',
    'record_security_event_simple',
    
    # Core security components
    'SecurityMetricsCollector',
    'get_security_metrics_collector',
    'get_security_observability',
    'get_compliance_dashboard',
    'get_security_alerting_system',
    'get_security_analytics',
    'get_security_monitoring_orchestrator',
    
    # Enums and types
    'SecurityEventSeverity',
    'SecurityMetricType',
    'SecurityTraceType',
    'AlertChannel',
    'AlertStatus',
    'ComplianceStatus',
    'AnalyticsTimeframe',
    'ReportFormat',
    
    # Event recording functions
    'record_threat_detection',
    'record_dlp_violation',
    'record_compliance_check',
    'record_access_control_decision',
    
    # Alert rule creation
    'create_threat_detection_rule',
    'create_dlp_violation_rule', 
    'create_compliance_violation_rule',
    
    # Decorators and context managers
    'trace_security_operation',
    'security_workflow_context',
    
    # Integrations
    'PrometheusIntegration',
    'GrafanaIntegration',
    'DataDogSecurityIntegration',
    'initialize_security_monitoring_stack'
]
