"""
Advanced Alerting and SLA Monitoring System
Provides comprehensive alerting with multi-level escalation, SLA tracking,
automated incident management, and intelligent noise reduction.
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from core.config import settings
from core.distributed_tracing import TracingComponent, get_tracing_manager
from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring

logger = get_logger(__name__)


class AlertChannel(Enum):
    """Alert notification channels."""

    EMAIL = "email"
    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    WEBHOOK = "webhook"
    SMS = "sms"
    TEAMS = "teams"
    DATADOG = "datadog"


class SLAStatus(Enum):
    """SLA compliance status."""

    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    VIOLATED = "violated"
    CRITICAL = "critical"


class EscalationLevel(Enum):
    """Alert escalation levels."""

    L1_SUPPORT = "l1_support"
    L2_ENGINEERING = "l2_engineering"
    L3_SENIOR_ENG = "l3_senior_engineering"
    L4_MANAGEMENT = "l4_management"
    L5_EXECUTIVE = "l5_executive"


class IncidentSeverity(Enum):
    """Incident severity levels."""

    P0_CRITICAL = "p0_critical"  # Service down, data loss
    P1_HIGH = "p1_high"  # Major functionality impacted
    P2_MEDIUM = "p2_medium"  # Minor functionality impacted
    P3_LOW = "p3_low"  # Cosmetic issues, future concerns
    P4_INFO = "p4_info"  # Informational


@dataclass
class SLADefinition:
    """Service Level Agreement definition."""

    sla_id: str
    name: str
    description: str
    service: str

    # Availability SLA
    availability_target: float = 99.9  # Percentage
    availability_window_hours: int = 24

    # Performance SLA
    response_time_target_ms: float = 500.0
    response_time_percentile: float = 95.0

    # Error rate SLA
    error_rate_target: float = 0.1  # Percentage
    error_rate_window_hours: int = 1

    # Throughput SLA
    throughput_target: float | None = None  # Requests per second

    # Business hours vs 24/7
    business_hours_only: bool = False
    business_hours_start: int = 9  # 9 AM
    business_hours_end: int = 17  # 5 PM
    business_days: list[int] = field(default_factory=lambda: [0, 1, 2, 3, 4])  # Mon-Fri

    # Escalation settings
    violation_escalation_minutes: int = 15
    at_risk_escalation_minutes: int = 60

    # Stakeholders
    service_owner: str = ""
    business_owner: str = ""
    escalation_contacts: list[str] = field(default_factory=list)


@dataclass
class AlertRule:
    """Advanced alert rule definition."""

    rule_id: str
    name: str
    description: str
    service: str
    metric_name: str

    # Thresholds
    warning_threshold: float
    critical_threshold: float
    emergency_threshold: float | None = None

    # Evaluation settings
    evaluation_window_minutes: int = 5
    consecutive_violations: int = 3
    comparison_operator: str = "gt"  # gt, lt, eq, ne, contains

    # Channels and escalation
    notification_channels: list[AlertChannel] = field(default_factory=list)
    escalation_policy: list[EscalationLevel] = field(
        default_factory=lambda: [EscalationLevel.L1_SUPPORT]
    )
    escalation_delay_minutes: int = 15

    # Smart alerting features
    enable_anomaly_detection: bool = False
    enable_predictive_alerting: bool = False
    suppress_similar_alerts: bool = True
    alert_correlation: bool = True

    # Auto-resolution
    auto_resolve: bool = True
    auto_resolve_threshold: float | None = None
    auto_resolve_duration_minutes: int = 10

    # Dependencies and conditions
    dependency_services: list[str] = field(default_factory=list)
    conditional_rules: list[str] = field(default_factory=list)

    # Tags and metadata
    tags: dict[str, str] = field(default_factory=dict)
    runbook_url: str | None = None
    dashboard_url: str | None = None


@dataclass
class AlertInstance:
    """Active alert instance."""

    alert_id: str
    rule_id: str
    timestamp: datetime
    severity: IncidentSeverity
    service: str
    metric_name: str
    current_value: float
    threshold_value: float
    message: str

    # Status tracking
    status: str = "active"  # active, acknowledged, resolved, suppressed
    acknowledged_by: str | None = None
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None

    # Escalation tracking
    current_escalation_level: EscalationLevel = EscalationLevel.L1_SUPPORT
    escalation_history: list[dict[str, Any]] = field(default_factory=list)
    next_escalation_at: datetime | None = None

    # Correlation and grouping
    correlation_id: str | None = None
    parent_incident_id: str | None = None
    related_alerts: list[str] = field(default_factory=list)

    # Notification tracking
    notifications_sent: list[dict[str, Any]] = field(default_factory=list)
    last_notification_at: datetime | None = None

    # Context and metadata
    affected_users: int = 0
    business_impact: str = "unknown"
    root_cause: str | None = None
    remediation_actions: list[str] = field(default_factory=list)


@dataclass
class SLAViolation:
    """SLA violation record."""

    violation_id: str
    sla_id: str
    timestamp: datetime
    service: str
    violation_type: str  # availability, performance, error_rate
    current_value: float
    target_value: float
    severity: SLAStatus
    duration_minutes: int
    impact_description: str
    estimated_affected_users: int = 0
    business_cost_estimate: float = 0.0


class AdvancedAlertingSLA:
    """
    Advanced alerting and SLA monitoring system.

    Features:
    - Multi-level alert escalation with intelligent routing
    - Comprehensive SLA monitoring and violation tracking
    - Smart alert correlation and noise reduction
    - Automated incident management lifecycle
    - Predictive alerting based on trends
    - Business impact assessment
    - Integration with multiple notification channels
    - Runbook automation and suggested remediation
    """

    def __init__(
        self,
        service_name: str = "advanced-alerting",
        datadog_monitoring: DatadogMonitoring | None = None,
    ):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        self.tracing_manager = get_tracing_manager()

        # Configuration
        self.sla_definitions: dict[str, SLADefinition] = {}
        self.alert_rules: dict[str, AlertRule] = {}

        # Active monitoring
        self.active_alerts: dict[str, AlertInstance] = {}
        self.sla_violations: list[SLAViolation] = []
        self.incident_timeline: list[dict[str, Any]] = []

        # SLA tracking
        self.sla_metrics_history: dict[str, list[dict[str, Any]]] = {}
        self.sla_compliance_scores: dict[str, float] = {}

        # Smart alerting features
        self.alert_correlation_groups: dict[str, list[str]] = {}
        self.suppressed_alerts: set[str] = set()
        self.predictive_models: dict[str, Any] = {}

        # Notification system
        self.notification_channels: dict[AlertChannel, dict[str, Any]] = {}
        self.escalation_timers: dict[str, asyncio.Task] = {}

        # Background tasks
        self.monitoring_active = False
        self.monitoring_tasks: dict[str, asyncio.Task] = {}

        # Initialize default configurations
        self._initialize_default_slas()
        self._initialize_default_alert_rules()
        self._initialize_notification_channels()

        self.logger.info(f"Advanced alerting and SLA monitoring initialized for {service_name}")

    async def start_monitoring(self):
        """Start all alerting and SLA monitoring tasks."""
        try:
            self.monitoring_active = True

            # Start SLA monitoring tasks
            sla_task = asyncio.create_task(self._sla_monitoring_loop())
            self.monitoring_tasks["sla_monitoring"] = sla_task

            # Start alert evaluation task
            alert_task = asyncio.create_task(self._alert_evaluation_loop())
            self.monitoring_tasks["alert_evaluation"] = alert_task

            # Start escalation management task
            escalation_task = asyncio.create_task(self._escalation_management_loop())
            self.monitoring_tasks["escalation_management"] = escalation_task

            # Start alert correlation task
            correlation_task = asyncio.create_task(self._alert_correlation_loop())
            self.monitoring_tasks["alert_correlation"] = correlation_task

            # Start predictive alerting task
            predictive_task = asyncio.create_task(self._predictive_alerting_loop())
            self.monitoring_tasks["predictive_alerting"] = predictive_task

            # Start incident lifecycle management
            incident_task = asyncio.create_task(self._incident_lifecycle_loop())
            self.monitoring_tasks["incident_management"] = incident_task

            self.logger.info("All alerting and SLA monitoring tasks started")

        except Exception as e:
            self.logger.error(f"Failed to start alerting system: {str(e)}")
            raise

    async def stop_monitoring(self):
        """Stop all monitoring tasks."""
        try:
            self.monitoring_active = False

            # Cancel all running tasks
            for task_name, task in self.monitoring_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self.logger.info(f"Stopped monitoring task: {task_name}")

            # Cancel escalation timers
            for _timer_id, timer_task in self.escalation_timers.items():
                if not timer_task.done():
                    timer_task.cancel()
                    try:
                        await timer_task
                    except asyncio.CancelledError:
                        pass

            self.monitoring_tasks.clear()
            self.escalation_timers.clear()

            self.logger.info("Advanced alerting system stopped")

        except Exception as e:
            self.logger.error(f"Error stopping alerting system: {str(e)}")

    def _initialize_default_slas(self):
        """Initialize default SLA definitions."""

        default_slas = [
            # API Service SLA
            SLADefinition(
                sla_id="api_service_sla",
                name="Core API Service SLA",
                description="Service level agreement for the core REST API",
                service="api",
                availability_target=99.9,
                availability_window_hours=24,
                response_time_target_ms=200.0,
                response_time_percentile=95.0,
                error_rate_target=0.1,
                error_rate_window_hours=1,
                throughput_target=100.0,
                service_owner="platform_team",
                business_owner="product_manager",
                escalation_contacts=["oncall@company.com", "manager@company.com"],
            ),
            # ETL Pipeline SLA
            SLADefinition(
                sla_id="etl_pipeline_sla",
                name="Data Processing Pipeline SLA",
                description="SLA for ETL pipeline processing times and data quality",
                service="etl",
                availability_target=99.5,
                availability_window_hours=24,
                response_time_target_ms=30000.0,  # 30 seconds for processing
                error_rate_target=0.5,
                business_hours_only=True,
                service_owner="data_team",
                business_owner="data_product_manager",
            ),
            # Database SLA
            SLADefinition(
                sla_id="database_sla",
                name="Primary Database SLA",
                description="Database availability and performance SLA",
                service="database",
                availability_target=99.95,
                availability_window_hours=24,
                response_time_target_ms=50.0,
                response_time_percentile=95.0,
                error_rate_target=0.05,
                service_owner="platform_team",
                business_owner="cto",
            ),
            # Data Quality SLA
            SLADefinition(
                sla_id="data_quality_sla",
                name="Data Quality SLA",
                description="Data completeness and accuracy SLA",
                service="data_quality",
                availability_target=98.0,  # Data quality score as availability
                response_time_target_ms=5000.0,  # Quality check response time
                error_rate_target=2.0,
                business_hours_only=True,
                service_owner="data_team",
                business_owner="chief_data_officer",
            ),
        ]

        for sla in default_slas:
            self.sla_definitions[sla.sla_id] = sla

        self.logger.info(f"Initialized {len(default_slas)} default SLA definitions")

    def _initialize_default_alert_rules(self):
        """Initialize default alert rules."""

        default_rules = [
            # API Response Time Alert
            AlertRule(
                rule_id="api_response_time_alert",
                name="API Response Time Alert",
                description="Alert when API response time exceeds thresholds",
                service="api",
                metric_name="api.p95_response_time",
                warning_threshold=200.0,
                critical_threshold=500.0,
                emergency_threshold=1000.0,
                evaluation_window_minutes=2,
                consecutive_violations=2,
                notification_channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
                escalation_policy=[EscalationLevel.L1_SUPPORT, EscalationLevel.L2_ENGINEERING],
                enable_predictive_alerting=True,
                runbook_url="https://wiki.company.com/api-performance-runbook",
            ),
            # API Error Rate Alert
            AlertRule(
                rule_id="api_error_rate_alert",
                name="API Error Rate Alert",
                description="Alert when API error rate is too high",
                service="api",
                metric_name="api.error_rate_percent",
                warning_threshold=1.0,
                critical_threshold=5.0,
                emergency_threshold=10.0,
                evaluation_window_minutes=5,
                consecutive_violations=3,
                notification_channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
                escalation_policy=[
                    EscalationLevel.L1_SUPPORT,
                    EscalationLevel.L2_ENGINEERING,
                    EscalationLevel.L3_SENIOR_ENG,
                ],
                escalation_delay_minutes=10,
                enable_anomaly_detection=True,
            ),
            # System Resource Alerts
            AlertRule(
                rule_id="system_cpu_alert",
                name="High CPU Usage Alert",
                description="Alert when system CPU usage is high",
                service="system",
                metric_name="system.cpu_percent",
                warning_threshold=70.0,
                critical_threshold=85.0,
                emergency_threshold=95.0,
                evaluation_window_minutes=3,
                consecutive_violations=2,
                notification_channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
                escalation_policy=[EscalationLevel.L1_SUPPORT, EscalationLevel.L2_ENGINEERING],
                enable_predictive_alerting=True,
            ),
            AlertRule(
                rule_id="system_memory_alert",
                name="High Memory Usage Alert",
                description="Alert when system memory usage is high",
                service="system",
                metric_name="system.memory_percent",
                warning_threshold=75.0,
                critical_threshold=90.0,
                emergency_threshold=95.0,
                evaluation_window_minutes=2,
                consecutive_violations=3,
                notification_channels=[
                    AlertChannel.SLACK,
                    AlertChannel.EMAIL,
                    AlertChannel.PAGERDUTY,
                ],
                escalation_policy=[EscalationLevel.L1_SUPPORT, EscalationLevel.L2_ENGINEERING],
            ),
            # ETL Pipeline Alerts
            AlertRule(
                rule_id="etl_processing_time_alert",
                name="ETL Processing Time Alert",
                description="Alert when ETL processing takes too long",
                service="etl",
                metric_name="etl.processing_time_minutes",
                warning_threshold=30.0,
                critical_threshold=60.0,
                emergency_threshold=120.0,
                evaluation_window_minutes=1,
                consecutive_violations=1,
                notification_channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
                escalation_policy=[EscalationLevel.L1_SUPPORT, EscalationLevel.L2_ENGINEERING],
            ),
            AlertRule(
                rule_id="data_quality_alert",
                name="Data Quality Score Alert",
                description="Alert when data quality score is below threshold",
                service="data_quality",
                metric_name="data_quality.score",
                warning_threshold=95.0,
                critical_threshold=90.0,
                emergency_threshold=85.0,
                comparison_operator="lt",  # Lower is worse
                evaluation_window_minutes=10,
                consecutive_violations=2,
                notification_channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
                escalation_policy=[
                    EscalationLevel.L1_SUPPORT,
                    EscalationLevel.L2_ENGINEERING,
                    EscalationLevel.L3_SENIOR_ENG,
                ],
            ),
            # Database Alerts
            AlertRule(
                rule_id="database_connection_alert",
                name="Database Connection Pool Alert",
                description="Alert when database connection pool usage is high",
                service="database",
                metric_name="database.connection_pool_usage_percent",
                warning_threshold=70.0,
                critical_threshold=85.0,
                emergency_threshold=95.0,
                evaluation_window_minutes=2,
                consecutive_violations=2,
                notification_channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
                escalation_policy=[EscalationLevel.L1_SUPPORT, EscalationLevel.L2_ENGINEERING],
            ),
        ]

        for rule in default_rules:
            self.alert_rules[rule.rule_id] = rule

        self.logger.info(f"Initialized {len(default_rules)} default alert rules")

    def _initialize_notification_channels(self):
        """Initialize notification channel configurations."""

        # Email configuration
        self.notification_channels[AlertChannel.EMAIL] = {
            "enabled": True,
            "smtp_server": getattr(settings, "smtp_server", "localhost"),
            "smtp_port": getattr(settings, "smtp_port", 587),
            "smtp_username": getattr(settings, "smtp_username", ""),
            "smtp_password": getattr(settings, "smtp_password", ""),
            "from_address": getattr(settings, "alert_email_from", "alerts@company.com"),
            "default_recipients": getattr(
                settings, "default_alert_recipients", ["oncall@company.com"]
            ),
        }

        # Slack configuration
        self.notification_channels[AlertChannel.SLACK] = {
            "enabled": True,
            "webhook_url": getattr(settings, "slack_webhook_url", ""),
            "default_channel": getattr(settings, "slack_alert_channel", "#alerts"),
            "username": "AlertBot",
            "icon_emoji": ":rotating_light:",
        }

        # PagerDuty configuration
        self.notification_channels[AlertChannel.PAGERDUTY] = {
            "enabled": False,
            "integration_key": getattr(settings, "pagerduty_integration_key", ""),
            "api_url": "https://events.pagerduty.com/v2/enqueue",
        }

        # Webhook configuration
        self.notification_channels[AlertChannel.WEBHOOK] = {
            "enabled": True,
            "default_url": getattr(settings, "alert_webhook_url", ""),
            "timeout": 10,
            "retry_count": 3,
        }

        self.logger.info("Notification channels initialized")

    async def _sla_monitoring_loop(self):
        """Monitor SLA compliance continuously."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(60)  # Check every minute

                current_time = datetime.utcnow()

                for sla_id, sla in self.sla_definitions.items():
                    # Check if we're in business hours (if required)
                    if sla.business_hours_only and not self._is_business_hours(current_time, sla):
                        continue

                    # Evaluate SLA compliance
                    with self.tracing_manager.start_span(
                        f"evaluate_sla_{sla_id}", TracingComponent.MONITORING
                    ) as span:
                        if hasattr(span, "set_attribute"):
                            span.set_attribute("sla.id", sla_id)
                            span.set_attribute("sla.service", sla.service)

                        compliance_score = await self._evaluate_sla_compliance(sla)
                        self.sla_compliance_scores[sla_id] = compliance_score

                        # Check for violations
                        await self._check_sla_violations(sla, compliance_score)

                        # Send SLA metrics to DataDog
                        if self.datadog_monitoring:
                            await self._send_sla_metrics_to_datadog(sla, compliance_score)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in SLA monitoring loop: {str(e)}")
                await asyncio.sleep(60)

    async def _evaluate_sla_compliance(self, sla: SLADefinition) -> float:
        """Evaluate SLA compliance and return compliance score (0-100)."""
        try:
            compliance_factors = []

            # Get metrics from DataDog or other monitoring systems
            service_metrics = await self._get_service_metrics(
                sla.service, sla.availability_window_hours
            )

            if not service_metrics:
                return 100.0  # Default to compliant if no data

            # Availability compliance
            uptime_percentage = service_metrics.get("uptime_percentage", 100.0)
            availability_compliance = min(
                100.0, (uptime_percentage / sla.availability_target) * 100
            )
            compliance_factors.append((availability_compliance, 0.4))  # 40% weight

            # Performance compliance (response time)
            avg_response_time = service_metrics.get("avg_response_time_ms", 0.0)
            if avg_response_time > 0:
                performance_compliance = max(
                    0.0,
                    100.0
                    - (
                        (avg_response_time - sla.response_time_target_ms)
                        / sla.response_time_target_ms
                        * 100
                    ),
                )
                compliance_factors.append((performance_compliance, 0.3))  # 30% weight

            # Error rate compliance
            error_rate = service_metrics.get("error_rate_percent", 0.0)
            if sla.error_rate_target > 0:
                error_compliance = max(0.0, 100.0 - (error_rate / sla.error_rate_target * 100))
                compliance_factors.append((error_compliance, 0.3))  # 30% weight

            # Calculate weighted average
            if compliance_factors:
                weighted_sum = sum(score * weight for score, weight in compliance_factors)
                total_weight = sum(weight for _, weight in compliance_factors)
                return weighted_sum / total_weight

            return 100.0

        except Exception as e:
            self.logger.error(f"Failed to evaluate SLA compliance: {str(e)}")
            return 50.0  # Default to neutral score on error

    async def _get_service_metrics(self, service: str, window_hours: int) -> dict[str, Any]:
        """Get service metrics for SLA evaluation."""
        try:
            # This would typically query DataDog, Prometheus, or other monitoring systems
            # For now, return mock data structure

            current_time = datetime.utcnow()

            # Mock metrics - in production, this would query real monitoring data
            mock_metrics = {
                "uptime_percentage": 99.95,
                "avg_response_time_ms": 150.0,
                "p95_response_time_ms": 250.0,
                "error_rate_percent": 0.05,
                "throughput_rps": 120.0,
                "timestamp": current_time.isoformat(),
            }

            return mock_metrics

        except Exception as e:
            self.logger.error(f"Failed to get service metrics for {service}: {str(e)}")
            return {}

    async def _alert_evaluation_loop(self):
        """Continuously evaluate alert rules against current metrics."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(30)  # Evaluate every 30 seconds

                for rule_id, rule in self.alert_rules.items():
                    try:
                        # Get current metric value
                        current_value = await self._get_current_metric_value(
                            rule.service, rule.metric_name
                        )

                        if current_value is not None:
                            # Evaluate rule
                            should_alert, severity = self._evaluate_alert_rule(rule, current_value)

                            if should_alert:
                                await self._trigger_alert(rule, current_value, severity)
                            else:
                                # Check for auto-resolution
                                await self._check_alert_auto_resolution(rule, current_value)

                    except Exception as e:
                        self.logger.error(f"Failed to evaluate alert rule {rule_id}: {str(e)}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in alert evaluation loop: {str(e)}")
                await asyncio.sleep(30)

    def _evaluate_alert_rule(
        self, rule: AlertRule, current_value: float
    ) -> tuple[bool, IncidentSeverity]:
        """Evaluate if an alert rule should trigger and at what severity."""
        try:
            should_alert = False
            severity = IncidentSeverity.P4_INFO

            if rule.comparison_operator == "gt":
                if rule.emergency_threshold and current_value >= rule.emergency_threshold:
                    should_alert = True
                    severity = IncidentSeverity.P0_CRITICAL
                elif current_value >= rule.critical_threshold:
                    should_alert = True
                    severity = IncidentSeverity.P1_HIGH
                elif current_value >= rule.warning_threshold:
                    should_alert = True
                    severity = IncidentSeverity.P2_MEDIUM

            elif rule.comparison_operator == "lt":
                if rule.emergency_threshold and current_value <= rule.emergency_threshold:
                    should_alert = True
                    severity = IncidentSeverity.P0_CRITICAL
                elif current_value <= rule.critical_threshold:
                    should_alert = True
                    severity = IncidentSeverity.P1_HIGH
                elif current_value <= rule.warning_threshold:
                    should_alert = True
                    severity = IncidentSeverity.P2_MEDIUM

            return should_alert, severity

        except Exception as e:
            self.logger.error(f"Failed to evaluate alert rule: {str(e)}")
            return False, IncidentSeverity.P4_INFO

    async def _trigger_alert(
        self, rule: AlertRule, current_value: float, severity: IncidentSeverity
    ):
        """Trigger an alert based on rule evaluation."""
        try:
            alert_key = f"{rule.service}_{rule.rule_id}"

            # Check if alert already exists
            if alert_key in self.active_alerts:
                existing_alert = self.active_alerts[alert_key]
                # Update severity if changed
                if existing_alert.severity != severity:
                    existing_alert.severity = severity
                    existing_alert.current_value = current_value
                    existing_alert.timestamp = datetime.utcnow()
                    await self._send_alert_update(existing_alert)
                return

            # Create new alert
            alert = AlertInstance(
                alert_id=f"{alert_key}_{int(time.time())}",
                rule_id=rule.rule_id,
                timestamp=datetime.utcnow(),
                severity=severity,
                service=rule.service,
                metric_name=rule.metric_name,
                current_value=current_value,
                threshold_value=rule.critical_threshold,
                message=f"{rule.name}: {rule.metric_name} = {current_value} (threshold: {rule.critical_threshold})",
            )

            # Add business impact assessment
            alert.business_impact = self._assess_business_impact(rule, severity)
            alert.affected_users = self._estimate_affected_users(rule, severity)

            # Add suggested remediation actions
            alert.remediation_actions = self._get_remediation_actions(rule, current_value)

            # Store alert
            self.active_alerts[alert_key] = alert

            # Send notifications
            await self._send_alert_notifications(alert, rule)

            # Schedule escalation if configured
            if len(rule.escalation_policy) > 1:
                await self._schedule_escalation(alert, rule)

            # Log the alert
            self.logger.error(f"ALERT TRIGGERED: {severity.value} - {alert.message}")

            # Add to incident timeline
            self.incident_timeline.append(
                {
                    "timestamp": alert.timestamp.isoformat(),
                    "type": "alert_triggered",
                    "alert_id": alert.alert_id,
                    "rule_id": rule.rule_id,
                    "severity": severity.value,
                    "service": rule.service,
                    "metric": rule.metric_name,
                    "value": current_value,
                }
            )

        except Exception as e:
            self.logger.error(f"Failed to trigger alert: {str(e)}")

    async def _escalation_management_loop(self):
        """Manage alert escalations."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(60)  # Check every minute

                current_time = datetime.utcnow()

                for _alert_key, alert in list(self.active_alerts.items()):
                    try:
                        if alert.next_escalation_at and current_time >= alert.next_escalation_at:
                            await self._escalate_alert(alert)

                    except Exception as e:
                        self.logger.error(f"Failed to escalate alert {alert.alert_id}: {str(e)}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in escalation management loop: {str(e)}")
                await asyncio.sleep(60)

    async def _alert_correlation_loop(self):
        """Correlate related alerts."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(120)  # Check every 2 minutes

                await self._correlate_alerts()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in alert correlation loop: {str(e)}")
                await asyncio.sleep(120)

    async def _predictive_alerting_loop(self):
        """Predictive alerting based on trends."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes

                await self._check_predictive_alerts()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in predictive alerting loop: {str(e)}")
                await asyncio.sleep(300)

    async def _incident_lifecycle_loop(self):
        """Manage incident lifecycle."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(180)  # Check every 3 minutes

                await self._manage_incident_lifecycle()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in incident lifecycle loop: {str(e)}")
                await asyncio.sleep(180)

    async def _escalate_alert(self, alert: AlertInstance):
        """Escalate alert to next level."""
        try:
            # Implementation for alert escalation
            self.logger.info(f"Escalating alert {alert.alert_id}")

        except Exception as e:
            self.logger.error(f"Failed to escalate alert: {str(e)}")

    async def _correlate_alerts(self):
        """Correlate related alerts."""
        try:
            # Implementation for alert correlation
            pass

        except Exception as e:
            self.logger.error(f"Failed to correlate alerts: {str(e)}")

    async def _check_predictive_alerts(self):
        """Check for predictive alert conditions."""
        try:
            # Implementation for predictive alerting
            pass

        except Exception as e:
            self.logger.error(f"Failed to check predictive alerts: {str(e)}")

    async def _manage_incident_lifecycle(self):
        """Manage incident lifecycle."""
        try:
            # Implementation for incident lifecycle management
            pass

        except Exception as e:
            self.logger.error(f"Failed to manage incident lifecycle: {str(e)}")

    def get_alerting_summary(self) -> dict[str, Any]:
        """Get comprehensive alerting and SLA summary."""
        try:
            current_time = datetime.utcnow()

            # Active alerts summary
            active_alerts_by_severity = {}
            for severity in IncidentSeverity:
                active_alerts_by_severity[severity.value] = len(
                    [
                        a
                        for a in self.active_alerts.values()
                        if a.severity == severity and a.status == "active"
                    ]
                )

            # SLA compliance summary
            sla_summary = {}
            for sla_id, score in self.sla_compliance_scores.items():
                sla = self.sla_definitions.get(sla_id)
                if sla:
                    status = SLAStatus.HEALTHY
                    if score < 90.0:
                        status = SLAStatus.CRITICAL
                    elif score < 95.0:
                        status = SLAStatus.VIOLATED
                    elif score < 98.0:
                        status = SLAStatus.AT_RISK

                    sla_summary[sla_id] = {
                        "name": sla.name,
                        "service": sla.service,
                        "compliance_score": score,
                        "status": status.value,
                        "target_availability": sla.availability_target,
                    }

            # Recent violations
            recent_violations = [
                v
                for v in self.sla_violations
                if (current_time - v.timestamp).total_seconds() <= 86400  # Last 24 hours
            ]

            # Escalation status
            escalated_alerts = len(
                [
                    a
                    for a in self.active_alerts.values()
                    if a.current_escalation_level != EscalationLevel.L1_SUPPORT
                ]
            )

            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "monitoring_active": self.monitoring_active,
                "alerts": {
                    "total_active": len(self.active_alerts),
                    "by_severity": active_alerts_by_severity,
                    "escalated": escalated_alerts,
                    "suppressed": len(self.suppressed_alerts),
                },
                "sla_compliance": {
                    "total_slas": len(self.sla_definitions),
                    "services": sla_summary,
                    "overall_health": self._calculate_overall_sla_health(),
                    "recent_violations": len(recent_violations),
                },
                "escalation": {
                    "active_escalations": len(self.escalation_timers),
                    "notification_channels": len(
                        [
                            c
                            for c, config in self.notification_channels.items()
                            if config.get("enabled", False)
                        ]
                    ),
                },
                "incident_management": {
                    "recent_incidents": len(
                        [
                            i
                            for i in self.incident_timeline
                            if (
                                current_time - datetime.fromisoformat(i["timestamp"])
                            ).total_seconds()
                            <= 3600
                        ]
                    ),
                    "correlation_groups": len(self.alert_correlation_groups),
                },
            }

        except Exception as e:
            self.logger.error(f"Failed to generate alerting summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


# Global instance
_alerting_sla: AdvancedAlertingSLA | None = None


def get_alerting_sla(
    service_name: str = "advanced-alerting", datadog_monitoring: DatadogMonitoring | None = None
) -> AdvancedAlertingSLA:
    """Get or create alerting and SLA monitoring instance."""
    global _alerting_sla

    if _alerting_sla is None:
        _alerting_sla = AdvancedAlertingSLA(service_name, datadog_monitoring)

    return _alerting_sla


# Convenience functions
async def start_alerting_monitoring():
    """Start alerting and SLA monitoring."""
    alerting = get_alerting_sla()
    await alerting.start_monitoring()


async def stop_alerting_monitoring():
    """Stop alerting and SLA monitoring."""
    alerting = get_alerting_sla()
    await alerting.stop_monitoring()


def get_alerting_summary() -> dict[str, Any]:
    """Get alerting and SLA summary."""
    alerting = get_alerting_sla()
    return alerting.get_alerting_summary()
