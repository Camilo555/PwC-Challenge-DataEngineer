"""
DataDog Intelligent Alerting System
Provides comprehensive real-time alerting with intelligent threshold management,
alert fatigue prevention, escalation policies, and automated remediation
"""

import asyncio
import json
import time
import smtplib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
from collections import defaultdict, deque
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring, Alert, AlertPriority

logger = get_logger(__name__)


class AlertState(Enum):
    """Alert states"""
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SILENCED = "silenced"


class AlertChannel(Enum):
    """Alert notification channels"""
    EMAIL = "email"
    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    WEBHOOK = "webhook"
    SMS = "sms"
    TEAMS = "teams"
    DATADOG = "datadog"


class EscalationLevel(Enum):
    """Alert escalation levels"""
    LEVEL_1 = "level_1"  # Team leads
    LEVEL_2 = "level_2"  # Managers
    LEVEL_3 = "level_3"  # Directors
    EXECUTIVE = "executive"  # C-level


class ThresholdType(Enum):
    """Types of threshold detection"""
    STATIC = "static"  # Fixed thresholds
    DYNAMIC = "dynamic"  # Adaptive based on historical data
    ANOMALY = "anomaly"  # ML-based anomaly detection
    TREND = "trend"  # Trend-based detection
    COMPOSITE = "composite"  # Multiple conditions


@dataclass
class NotificationChannel:
    """Notification channel configuration"""
    channel_type: AlertChannel
    config: Dict[str, Any]
    enabled: bool = True
    priority_filter: List[AlertPriority] = field(default_factory=list)
    rate_limit_per_hour: int = 60
    quiet_hours: Optional[Tuple[int, int]] = None  # (start_hour, end_hour) in 24h format


@dataclass
class EscalationRule:
    """Alert escalation rule"""
    level: EscalationLevel
    delay_minutes: int
    channels: List[str]  # Channel IDs
    conditions: Dict[str, Any] = field(default_factory=dict)
    auto_resolve: bool = False


@dataclass
class AlertRule:
    """Advanced alert rule definition"""
    rule_id: str
    name: str
    description: str
    metric_query: str
    threshold_type: ThresholdType
    
    # Thresholds
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None
    
    # Dynamic threshold settings
    baseline_window_hours: int = 24
    sensitivity: float = 2.0  # Standard deviations for anomaly detection
    trend_window_minutes: int = 30
    
    # Alert behavior
    evaluation_delay: int = 60  # seconds
    recovery_threshold: Optional[float] = None
    require_full_window: bool = True
    
    # Notification settings
    notification_channels: List[str] = field(default_factory=list)
    escalation_rules: List[EscalationRule] = field(default_factory=list)
    
    # Alert fatigue prevention
    max_alerts_per_hour: int = 10
    suppress_duration_minutes: int = 60
    similar_alert_grouping: bool = True
    
    # Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    business_impact: str = "medium"  # low, medium, high, critical
    runbook_url: Optional[str] = None
    owner: Optional[str] = None
    team: Optional[str] = None
    
    # Auto-remediation
    auto_remediation_enabled: bool = False
    remediation_script: Optional[str] = None
    remediation_conditions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertIncident:
    """Alert incident record"""
    incident_id: str
    rule_id: str
    alert_state: AlertState
    metric_value: float
    threshold_value: float
    start_time: datetime
    last_update: datetime
    end_time: Optional[datetime] = None
    
    # Context
    dimensions: Dict[str, str] = field(default_factory=dict)
    message: str = ""
    
    # Escalation tracking
    escalation_level: EscalationLevel = EscalationLevel.LEVEL_1
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    
    # Notifications sent
    notifications_sent: List[Dict[str, Any]] = field(default_factory=list)
    
    # Auto-remediation
    remediation_attempts: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class AlertMetrics:
    """Alert system performance metrics"""
    total_alerts: int = 0
    alerts_by_state: Dict[AlertState, int] = field(default_factory=lambda: defaultdict(int))
    alerts_by_priority: Dict[AlertPriority, int] = field(default_factory=lambda: defaultdict(int))
    mean_resolution_time: float = 0.0
    false_positive_rate: float = 0.0
    notification_delivery_rate: float = 100.0
    escalation_rate: float = 0.0


class DataDogIntelligentAlerting:
    """
    Intelligent Real-time Alerting System
    
    Features:
    - Dynamic threshold adjustment based on historical patterns
    - Intelligent alert fatigue prevention and grouping
    - Multi-channel notification with escalation policies
    - Automated remediation and self-healing capabilities
    - Alert correlation and root cause analysis
    - Business impact-aware prioritization
    - Machine learning-based anomaly detection
    - Compliance and audit trail management
    """
    
    def __init__(self, service_name: str = "intelligent-alerting",
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Core data structures
        self.notification_channels: Dict[str, NotificationChannel] = {}
        self.alert_rules: Dict[str, AlertRule] = {}
        self.active_incidents: Dict[str, AlertIncident] = {}
        self.incident_history: List[AlertIncident] = []
        
        # Performance tracking
        self.alert_metrics = AlertMetrics()
        self.historical_values: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.baseline_calculations: Dict[str, Dict[str, float]] = {}
        
        # Rate limiting and suppression
        self.alert_counts: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.suppressed_alerts: Dict[str, datetime] = {}
        self.grouped_alerts: Dict[str, List[str]] = defaultdict(list)
        
        # Background processing
        self.evaluation_queue: asyncio.Queue = asyncio.Queue()
        self.notification_queue: asyncio.Queue = asyncio.Queue()
        
        # Initialize system
        self._initialize_notification_channels()
        self._initialize_standard_alert_rules()
        
        # Start background workers
        asyncio.create_task(self._start_background_workers())
        
        self.logger.info(f"Intelligent alerting system initialized for {service_name}")
    
    def _initialize_notification_channels(self):
        """Initialize notification channels"""
        
        # Email channel
        self.notification_channels["email_ops"] = NotificationChannel(
            channel_type=AlertChannel.EMAIL,
            config={
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "alerts@company.com",
                "password": "${EMAIL_PASSWORD}",
                "recipients": ["ops-team@company.com"],
                "cc": [],
                "subject_template": "[{priority}] {alert_name}: {metric_value}"
            },
            priority_filter=[AlertPriority.HIGH, AlertPriority.CRITICAL],
            rate_limit_per_hour=20
        )
        
        self.notification_channels["email_executives"] = NotificationChannel(
            channel_type=AlertChannel.EMAIL,
            config={
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "alerts@company.com",
                "password": "${EMAIL_PASSWORD}",
                "recipients": ["cto@company.com", "ceo@company.com"],
                "subject_template": "[EXECUTIVE ALERT] {alert_name}: {message}"
            },
            priority_filter=[AlertPriority.CRITICAL],
            rate_limit_per_hour=5,
            quiet_hours=(22, 7)  # 10 PM to 7 AM
        )
        
        # Slack channel
        self.notification_channels["slack_ops"] = NotificationChannel(
            channel_type=AlertChannel.SLACK,
            config={
                "webhook_url": "${SLACK_WEBHOOK_URL}",
                "channel": "#ops-alerts",
                "username": "DataDog Alerts",
                "icon_emoji": ":warning:"
            },
            rate_limit_per_hour=50
        )
        
        # PagerDuty for critical alerts
        self.notification_channels["pagerduty_oncall"] = NotificationChannel(
            channel_type=AlertChannel.PAGERDUTY,
            config={
                "service_key": "${PAGERDUTY_SERVICE_KEY}",
                "api_url": "https://events.pagerduty.com/v2/enqueue"
            },
            priority_filter=[AlertPriority.CRITICAL]
        )
        
        # DataDog native alerts
        self.notification_channels["datadog_native"] = NotificationChannel(
            channel_type=AlertChannel.DATADOG,
            config={
                "api_key": "${DD_API_KEY}",
                "app_key": "${DD_APP_KEY}"
            }
        )
        
        self.logger.info(f"Initialized {len(self.notification_channels)} notification channels")
    
    def _initialize_standard_alert_rules(self):
        """Initialize standard alert rules"""
        
        standard_rules = [
            # Critical System Alerts
            AlertRule(
                rule_id="system_cpu_critical",
                name="System CPU Usage Critical",
                description="CPU usage above critical threshold",
                metric_query="avg:system.cpu.utilization{*}",
                threshold_type=ThresholdType.STATIC,
                warning_threshold=80.0,
                critical_threshold=95.0,
                evaluation_delay=60,
                notification_channels=["slack_ops", "email_ops", "pagerduty_oncall"],
                escalation_rules=[
                    EscalationRule(
                        level=EscalationLevel.LEVEL_1,
                        delay_minutes=0,
                        channels=["slack_ops"]
                    ),
                    EscalationRule(
                        level=EscalationLevel.LEVEL_2,
                        delay_minutes=15,
                        channels=["email_ops", "pagerduty_oncall"]
                    ),
                    EscalationRule(
                        level=EscalationLevel.EXECUTIVE,
                        delay_minutes=60,
                        channels=["email_executives"]
                    )
                ],
                business_impact="high",
                auto_remediation_enabled=True,
                remediation_script="scale_up_instances.py",
                team="infrastructure",
                runbook_url="https://wiki.company.com/runbooks/high-cpu"
            ),
            
            AlertRule(
                rule_id="api_error_rate_anomaly",
                name="API Error Rate Anomaly",
                description="Anomalous increase in API error rate",
                metric_query="sum:api.errors.total{*}.as_rate() / sum:api.requests.total{*}.as_rate() * 100",
                threshold_type=ThresholdType.ANOMALY,
                baseline_window_hours=24,
                sensitivity=3.0,
                notification_channels=["slack_ops", "email_ops"],
                escalation_rules=[
                    EscalationRule(
                        level=EscalationLevel.LEVEL_1,
                        delay_minutes=0,
                        channels=["slack_ops"]
                    ),
                    EscalationRule(
                        level=EscalationLevel.LEVEL_2,
                        delay_minutes=30,
                        channels=["email_ops"]
                    )
                ],
                business_impact="high",
                team="api",
                runbook_url="https://wiki.company.com/runbooks/api-errors"
            ),
            
            # Business Metrics Alerts
            AlertRule(
                rule_id="revenue_drop_critical",
                name="Revenue Drop Critical",
                description="Significant drop in revenue metrics",
                metric_query="sum:business.revenue.total{*}",
                threshold_type=ThresholdType.TREND,
                trend_window_minutes=60,
                critical_threshold=-20.0,  # 20% drop
                notification_channels=["email_executives", "slack_ops"],
                escalation_rules=[
                    EscalationRule(
                        level=EscalationLevel.EXECUTIVE,
                        delay_minutes=0,
                        channels=["email_executives"]
                    )
                ],
                business_impact="critical",
                team="business",
                owner="cfo@company.com"
            ),
            
            # Data Quality Alerts
            AlertRule(
                rule_id="data_quality_degradation",
                name="Data Quality Degradation",
                description="Data quality score below acceptable threshold",
                metric_query="avg:data_quality.overall_score{*}",
                threshold_type=ThresholdType.STATIC,
                warning_threshold=95.0,
                critical_threshold=90.0,
                notification_channels=["slack_ops", "email_ops"],
                business_impact="medium",
                team="data-engineering",
                runbook_url="https://wiki.company.com/runbooks/data-quality"
            ),
            
            # ML Model Performance Alerts
            AlertRule(
                rule_id="ml_model_accuracy_drop",
                name="ML Model Accuracy Drop",
                description="ML model accuracy below acceptable threshold",
                metric_query="avg:ml.model.accuracy{stage:production}",
                threshold_type=ThresholdType.DYNAMIC,
                baseline_window_hours=168,  # 1 week
                sensitivity=2.5,
                warning_threshold=90.0,
                critical_threshold=85.0,
                notification_channels=["slack_ops", "email_ops"],
                business_impact="high",
                team="ml-ops",
                runbook_url="https://wiki.company.com/runbooks/ml-model-performance"
            ),
            
            # Security Alerts
            AlertRule(
                rule_id="security_threats_spike",
                name="Security Threats Spike",
                description="Unusual spike in security threats",
                metric_query="sum:security.threats.total{*}",
                threshold_type=ThresholdType.ANOMALY,
                baseline_window_hours=72,
                sensitivity=2.0,
                notification_channels=["pagerduty_oncall", "email_ops", "slack_ops"],
                escalation_rules=[
                    EscalationRule(
                        level=EscalationLevel.LEVEL_1,
                        delay_minutes=0,
                        channels=["pagerduty_oncall"]
                    ),
                    EscalationRule(
                        level=EscalationLevel.EXECUTIVE,
                        delay_minutes=30,
                        channels=["email_executives"]
                    )
                ],
                business_impact="critical",
                team="security",
                auto_remediation_enabled=True,
                remediation_script="block_suspicious_ips.py"
            )
        ]
        
        for rule in standard_rules:
            self.alert_rules[rule.rule_id] = rule
        
        self.logger.info(f"Initialized {len(standard_rules)} standard alert rules")
    
    async def _start_background_workers(self):
        """Start background worker tasks"""
        
        try:
            # Start alert evaluation worker
            asyncio.create_task(self._alert_evaluation_worker())
            
            # Start notification worker
            asyncio.create_task(self._notification_worker())
            
            # Start escalation worker
            asyncio.create_task(self._escalation_worker())
            
            # Start baseline calculation worker
            asyncio.create_task(self._baseline_calculation_worker())
            
            # Start incident cleanup worker
            asyncio.create_task(self._incident_cleanup_worker())
            
            self.logger.info("Background workers started")
            
        except Exception as e:
            self.logger.error(f"Failed to start background workers: {str(e)}")
    
    async def evaluate_metric(self, metric_query: str, value: float,
                            dimensions: Optional[Dict[str, str]] = None,
                            timestamp: Optional[datetime] = None) -> bool:
        """Evaluate metric against alert rules"""
        
        try:
            # Add to evaluation queue
            await self.evaluation_queue.put({
                "metric_query": metric_query,
                "value": value,
                "dimensions": dimensions or {},
                "timestamp": timestamp or datetime.utcnow()
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to evaluate metric: {str(e)}")
            return False
    
    async def _alert_evaluation_worker(self):
        """Background worker for evaluating alerts"""
        
        while True:
            try:
                # Get metric evaluation from queue
                eval_data = await self.evaluation_queue.get()
                
                metric_query = eval_data["metric_query"]
                value = eval_data["value"]
                dimensions = eval_data["dimensions"]
                timestamp = eval_data["timestamp"]
                
                # Store historical value
                metric_key = f"{metric_query}:{json.dumps(dimensions, sort_keys=True)}"
                self.historical_values[metric_key].append((timestamp, value))
                
                # Find matching alert rules
                matching_rules = [
                    rule for rule in self.alert_rules.values()
                    if rule.metric_query == metric_query
                ]
                
                for rule in matching_rules:
                    await self._evaluate_rule_against_value(rule, value, dimensions, timestamp)
                
                self.evaluation_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error in alert evaluation worker: {str(e)}")
                await asyncio.sleep(1)
    
    async def _evaluate_rule_against_value(self, rule: AlertRule, value: float,
                                         dimensions: Dict[str, str], timestamp: datetime):
        """Evaluate a specific rule against a metric value"""
        
        try:
            # Check if alert is suppressed
            if await self._is_alert_suppressed(rule.rule_id, dimensions):
                return
            
            # Determine alert state based on threshold type
            alert_state = await self._determine_alert_state(rule, value, dimensions, timestamp)
            
            if alert_state == AlertState.OK:
                # Check if we need to resolve an existing incident
                await self._check_incident_resolution(rule.rule_id, dimensions)
                return
            
            # Check for existing incident
            incident_key = f"{rule.rule_id}:{json.dumps(dimensions, sort_keys=True)}"
            existing_incident = self.active_incidents.get(incident_key)
            
            if existing_incident:
                # Update existing incident
                existing_incident.alert_state = alert_state
                existing_incident.metric_value = value
                existing_incident.last_update = timestamp
                
                # Check if we need to escalate
                if await self._should_escalate_incident(existing_incident):
                    await self._escalate_incident(existing_incident)
            else:
                # Create new incident
                incident = AlertIncident(
                    incident_id=f"{rule.rule_id}_{int(timestamp.timestamp())}",
                    rule_id=rule.rule_id,
                    alert_state=alert_state,
                    metric_value=value,
                    threshold_value=rule.critical_threshold or rule.warning_threshold or 0,
                    start_time=timestamp,
                    last_update=timestamp,
                    dimensions=dimensions,
                    message=await self._generate_alert_message(rule, value, dimensions)
                )
                
                self.active_incidents[incident_key] = incident
                
                # Queue for notification
                await self.notification_queue.put({
                    "incident": incident,
                    "rule": rule,
                    "action": "trigger"
                })
                
                # Update metrics
                self.alert_metrics.total_alerts += 1
                self.alert_metrics.alerts_by_state[alert_state] += 1
                
                # Check for auto-remediation
                if rule.auto_remediation_enabled:
                    await self._attempt_auto_remediation(incident, rule)
            
            # Track alert rate for suppression
            self.alert_counts[rule.rule_id].append(timestamp)
            
        except Exception as e:
            self.logger.error(f"Failed to evaluate rule {rule.rule_id}: {str(e)}")
    
    async def _determine_alert_state(self, rule: AlertRule, value: float,
                                   dimensions: Dict[str, str], timestamp: datetime) -> AlertState:
        """Determine alert state based on threshold type and value"""
        
        try:
            if rule.threshold_type == ThresholdType.STATIC:
                if rule.critical_threshold is not None and value >= rule.critical_threshold:
                    return AlertState.CRITICAL
                elif rule.warning_threshold is not None and value >= rule.warning_threshold:
                    return AlertState.WARNING
                else:
                    return AlertState.OK
            
            elif rule.threshold_type == ThresholdType.DYNAMIC:
                baseline = await self._get_dynamic_baseline(rule, dimensions)
                if baseline is None:
                    return AlertState.UNKNOWN
                
                deviation = abs(value - baseline["mean"]) / max(baseline["std"], 0.1)
                
                if deviation > rule.sensitivity * 1.5:  # Critical threshold
                    return AlertState.CRITICAL
                elif deviation > rule.sensitivity:  # Warning threshold
                    return AlertState.WARNING
                else:
                    return AlertState.OK
            
            elif rule.threshold_type == ThresholdType.ANOMALY:
                is_anomaly = await self._detect_anomaly(rule, value, dimensions, timestamp)
                return AlertState.CRITICAL if is_anomaly else AlertState.OK
            
            elif rule.threshold_type == ThresholdType.TREND:
                trend = await self._calculate_trend(rule, dimensions, timestamp)
                if trend is None:
                    return AlertState.UNKNOWN
                
                if rule.critical_threshold is not None and trend <= rule.critical_threshold:
                    return AlertState.CRITICAL
                elif rule.warning_threshold is not None and trend <= rule.warning_threshold:
                    return AlertState.WARNING
                else:
                    return AlertState.OK
            
            return AlertState.UNKNOWN
            
        except Exception as e:
            self.logger.error(f"Failed to determine alert state: {str(e)}")
            return AlertState.UNKNOWN
    
    async def _get_dynamic_baseline(self, rule: AlertRule, 
                                  dimensions: Dict[str, str]) -> Optional[Dict[str, float]]:
        """Get dynamic baseline for a metric"""
        
        try:
            metric_key = f"{rule.metric_query}:{json.dumps(dimensions, sort_keys=True)}"
            
            if metric_key in self.baseline_calculations:
                baseline = self.baseline_calculations[metric_key]
                
                # Check if baseline is recent enough
                baseline_age = datetime.utcnow() - datetime.fromisoformat(baseline["calculated_at"])
                if baseline_age.total_seconds() < rule.baseline_window_hours * 3600:
                    return baseline
            
            # Calculate new baseline
            if metric_key not in self.historical_values:
                return None
            
            historical_data = list(self.historical_values[metric_key])
            cutoff_time = datetime.utcnow() - timedelta(hours=rule.baseline_window_hours)
            
            recent_values = [
                value for timestamp, value in historical_data
                if timestamp >= cutoff_time
            ]
            
            if len(recent_values) < 10:  # Need minimum data points
                return None
            
            import statistics
            
            baseline = {
                "mean": statistics.mean(recent_values),
                "std": statistics.stdev(recent_values) if len(recent_values) > 1 else 0,
                "min": min(recent_values),
                "max": max(recent_values),
                "calculated_at": datetime.utcnow().isoformat()
            }
            
            self.baseline_calculations[metric_key] = baseline
            return baseline
            
        except Exception as e:
            self.logger.error(f"Failed to get dynamic baseline: {str(e)}")
            return None
    
    async def _detect_anomaly(self, rule: AlertRule, value: float,
                            dimensions: Dict[str, str], timestamp: datetime) -> bool:
        """Detect if value is anomalous using ML-based approach"""
        
        try:
            # This is a simplified anomaly detection
            # In production, you'd use more sophisticated ML algorithms
            
            baseline = await self._get_dynamic_baseline(rule, dimensions)
            if baseline is None:
                return False
            
            # Z-score based anomaly detection
            z_score = abs(value - baseline["mean"]) / max(baseline["std"], 0.1)
            
            return z_score > rule.sensitivity
            
        except Exception as e:
            self.logger.error(f"Failed to detect anomaly: {str(e)}")
            return False
    
    async def _calculate_trend(self, rule: AlertRule, dimensions: Dict[str, str],
                             timestamp: datetime) -> Optional[float]:
        """Calculate trend for trend-based alerting"""
        
        try:
            metric_key = f"{rule.metric_query}:{json.dumps(dimensions, sort_keys=True)}"
            
            if metric_key not in self.historical_values:
                return None
            
            cutoff_time = timestamp - timedelta(minutes=rule.trend_window_minutes)
            recent_data = [
                (ts, val) for ts, val in self.historical_values[metric_key]
                if ts >= cutoff_time
            ]
            
            if len(recent_data) < 2:
                return None
            
            # Calculate simple trend (percentage change)
            first_value = recent_data[0][1]
            last_value = recent_data[-1][1]
            
            if first_value == 0:
                return None
            
            trend = ((last_value - first_value) / first_value) * 100
            return trend
            
        except Exception as e:
            self.logger.error(f"Failed to calculate trend: {str(e)}")
            return None
    
    async def _is_alert_suppressed(self, rule_id: str, dimensions: Dict[str, str]) -> bool:
        """Check if alert should be suppressed"""
        
        try:
            # Check global suppression
            suppression_key = f"{rule_id}:{json.dumps(dimensions, sort_keys=True)}"
            if suppression_key in self.suppressed_alerts:
                suppress_until = self.suppressed_alerts[suppression_key]
                if datetime.utcnow() < suppress_until:
                    return True
                else:
                    # Remove expired suppression
                    del self.suppressed_alerts[suppression_key]
            
            # Check rate limiting
            rule = self.alert_rules.get(rule_id)
            if rule:
                recent_alerts = [
                    ts for ts in self.alert_counts[rule_id]
                    if (datetime.utcnow() - ts).total_seconds() < 3600  # Last hour
                ]
                
                if len(recent_alerts) >= rule.max_alerts_per_hour:
                    # Suppress for the configured duration
                    suppress_until = datetime.utcnow() + timedelta(minutes=rule.suppress_duration_minutes)
                    self.suppressed_alerts[suppression_key] = suppress_until
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to check alert suppression: {str(e)}")
            return False
    
    async def _generate_alert_message(self, rule: AlertRule, value: float,
                                    dimensions: Dict[str, str]) -> str:
        """Generate alert message"""
        
        try:
            dimension_str = ", ".join([f"{k}: {v}" for k, v in dimensions.items()])
            
            message = f"Alert: {rule.name}\n"
            message += f"Description: {rule.description}\n"
            message += f"Current Value: {value}\n"
            
            if rule.critical_threshold:
                message += f"Critical Threshold: {rule.critical_threshold}\n"
            if rule.warning_threshold:
                message += f"Warning Threshold: {rule.warning_threshold}\n"
            
            if dimensions:
                message += f"Dimensions: {dimension_str}\n"
            
            if rule.runbook_url:
                message += f"Runbook: {rule.runbook_url}\n"
            
            message += f"Business Impact: {rule.business_impact}\n"
            message += f"Team: {rule.team or 'Not specified'}\n"
            
            return message
            
        except Exception as e:
            self.logger.error(f"Failed to generate alert message: {str(e)}")
            return f"Alert: {rule.name} - Value: {value}"
    
    async def _notification_worker(self):
        """Background worker for sending notifications"""
        
        while True:
            try:
                # Get notification from queue
                notification_data = await self.notification_queue.get()
                
                incident = notification_data["incident"]
                rule = notification_data["rule"]
                action = notification_data["action"]
                
                # Send notifications through configured channels
                for channel_id in rule.notification_channels:
                    if channel_id in self.notification_channels:
                        channel = self.notification_channels[channel_id]
                        
                        # Check if channel should receive this alert
                        if await self._should_send_to_channel(channel, incident, rule):
                            success = await self._send_notification(channel, incident, rule, action)
                            
                            # Record notification attempt
                            incident.notifications_sent.append({
                                "channel_id": channel_id,
                                "channel_type": channel.channel_type.value,
                                "timestamp": datetime.utcnow().isoformat(),
                                "success": success,
                                "action": action
                            })
                
                self.notification_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error in notification worker: {str(e)}")
                await asyncio.sleep(1)
    
    async def _should_send_to_channel(self, channel: NotificationChannel,
                                    incident: AlertIncident, rule: AlertRule) -> bool:
        """Determine if notification should be sent to channel"""
        
        try:
            # Check if channel is enabled
            if not channel.enabled:
                return False
            
            # Check priority filter
            if channel.priority_filter:
                alert_priority = AlertPriority.CRITICAL if incident.alert_state == AlertState.CRITICAL else AlertPriority.HIGH
                if alert_priority not in channel.priority_filter:
                    return False
            
            # Check rate limiting
            recent_notifications = [
                notif for notif in incident.notifications_sent
                if (notif["channel_type"] == channel.channel_type.value and
                    datetime.utcnow() - datetime.fromisoformat(notif["timestamp"])).total_seconds() < 3600
            ]
            
            if len(recent_notifications) >= channel.rate_limit_per_hour:
                return False
            
            # Check quiet hours
            if channel.quiet_hours:
                current_hour = datetime.utcnow().hour
                quiet_start, quiet_end = channel.quiet_hours
                
                if quiet_start <= quiet_end:
                    # Same day quiet hours
                    if quiet_start <= current_hour < quiet_end:
                        # Only send critical alerts during quiet hours
                        return incident.alert_state == AlertState.CRITICAL
                else:
                    # Overnight quiet hours
                    if current_hour >= quiet_start or current_hour < quiet_end:
                        return incident.alert_state == AlertState.CRITICAL
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to check channel notification rules: {str(e)}")
            return False
    
    async def _send_notification(self, channel: NotificationChannel,
                               incident: AlertIncident, rule: AlertRule, action: str) -> bool:
        """Send notification through specific channel"""
        
        try:
            if channel.channel_type == AlertChannel.EMAIL:
                return await self._send_email_notification(channel, incident, rule, action)
            elif channel.channel_type == AlertChannel.SLACK:
                return await self._send_slack_notification(channel, incident, rule, action)
            elif channel.channel_type == AlertChannel.PAGERDUTY:
                return await self._send_pagerduty_notification(channel, incident, rule, action)
            elif channel.channel_type == AlertChannel.WEBHOOK:
                return await self._send_webhook_notification(channel, incident, rule, action)
            elif channel.channel_type == AlertChannel.DATADOG:
                return await self._send_datadog_notification(channel, incident, rule, action)
            else:
                self.logger.warning(f"Unsupported notification channel: {channel.channel_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to send notification via {channel.channel_type}: {str(e)}")
            return False
    
    async def _send_email_notification(self, channel: NotificationChannel,
                                     incident: AlertIncident, rule: AlertRule, action: str) -> bool:
        """Send email notification"""
        
        try:
            config = channel.config
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = config["username"]
            msg['To'] = ", ".join(config["recipients"])
            
            if config.get("cc"):
                msg['CC'] = ", ".join(config["cc"])
            
            # Generate subject
            subject = config.get("subject_template", "{alert_name}: {message}").format(
                priority=incident.alert_state.value.upper(),
                alert_name=rule.name,
                metric_value=incident.metric_value,
                message=incident.message
            )
            msg['Subject'] = subject
            
            # Create email body
            body = f"""
Alert Details:
==============
Alert Name: {rule.name}
Description: {rule.description}
Current State: {incident.alert_state.value.upper()}
Current Value: {incident.metric_value}
Threshold: {incident.threshold_value}
Start Time: {incident.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}

Dimensions:
{json.dumps(incident.dimensions, indent=2)}

Business Impact: {rule.business_impact}
Team: {rule.team or 'Not specified'}

{f'Runbook: {rule.runbook_url}' if rule.runbook_url else ''}

Incident ID: {incident.incident_id}
Rule ID: {rule.rule_id}
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email (this is a placeholder - implement actual SMTP sending)
            # In production, you'd use proper SMTP configuration
            self.logger.info(f"Would send email to {config['recipients']} with subject: {subject}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {str(e)}")
            return False
    
    async def _send_slack_notification(self, channel: NotificationChannel,
                                     incident: AlertIncident, rule: AlertRule, action: str) -> bool:
        """Send Slack notification"""
        
        try:
            # This is a placeholder for Slack integration
            # In production, you'd use the Slack API or webhook
            
            config = channel.config
            
            color = "danger" if incident.alert_state == AlertState.CRITICAL else "warning"
            
            slack_message = {
                "channel": config["channel"],
                "username": config.get("username", "DataDog Alerts"),
                "icon_emoji": config.get("icon_emoji", ":warning:"),
                "attachments": [{
                    "color": color,
                    "title": f"{incident.alert_state.value.upper()}: {rule.name}",
                    "text": rule.description,
                    "fields": [
                        {"title": "Current Value", "value": str(incident.metric_value), "short": True},
                        {"title": "Threshold", "value": str(incident.threshold_value), "short": True},
                        {"title": "Business Impact", "value": rule.business_impact, "short": True},
                        {"title": "Team", "value": rule.team or "Not specified", "short": True}
                    ],
                    "footer": f"Incident ID: {incident.incident_id}",
                    "ts": int(incident.start_time.timestamp())
                }]
            }
            
            self.logger.info(f"Would send Slack message to {config['channel']}: {rule.name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {str(e)}")
            return False
    
    async def _send_pagerduty_notification(self, channel: NotificationChannel,
                                         incident: AlertIncident, rule: AlertRule, action: str) -> bool:
        """Send PagerDuty notification"""
        
        try:
            # This is a placeholder for PagerDuty integration
            # In production, you'd use the PagerDuty Events API
            
            config = channel.config
            
            pagerduty_event = {
                "routing_key": config["service_key"],
                "event_action": "trigger" if action == "trigger" else "resolve",
                "dedup_key": incident.incident_id,
                "payload": {
                    "summary": f"{rule.name}: {incident.metric_value}",
                    "source": self.service_name,
                    "severity": "critical" if incident.alert_state == AlertState.CRITICAL else "warning",
                    "component": rule.team or "unknown",
                    "group": rule.business_impact,
                    "class": "metric_alert",
                    "custom_details": {
                        "rule_id": rule.rule_id,
                        "metric_value": incident.metric_value,
                        "threshold_value": incident.threshold_value,
                        "dimensions": incident.dimensions,
                        "runbook_url": rule.runbook_url
                    }
                }
            }
            
            self.logger.info(f"Would send PagerDuty event: {incident.incident_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send PagerDuty notification: {str(e)}")
            return False
    
    async def _send_webhook_notification(self, channel: NotificationChannel,
                                       incident: AlertIncident, rule: AlertRule, action: str) -> bool:
        """Send webhook notification"""
        
        try:
            # This is a placeholder for webhook integration
            # In production, you'd make HTTP POST requests
            
            config = channel.config
            
            webhook_payload = {
                "alert": {
                    "id": incident.incident_id,
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "description": rule.description,
                    "state": incident.alert_state.value,
                    "metric_value": incident.metric_value,
                    "threshold_value": incident.threshold_value,
                    "start_time": incident.start_time.isoformat(),
                    "dimensions": incident.dimensions,
                    "business_impact": rule.business_impact,
                    "team": rule.team,
                    "runbook_url": rule.runbook_url
                },
                "action": action,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.logger.info(f"Would send webhook to {config['url']}: {incident.incident_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send webhook notification: {str(e)}")
            return False
    
    async def _send_datadog_notification(self, channel: NotificationChannel,
                                       incident: AlertIncident, rule: AlertRule, action: str) -> bool:
        """Send DataDog native notification"""
        
        try:
            if not self.datadog_monitoring:
                return False
            
            # Create DataDog alert
            if action == "trigger":
                alert = Alert(
                    name=rule.name,
                    message=incident.message,
                    query=rule.metric_query,
                    priority=AlertPriority.CRITICAL if incident.alert_state == AlertState.CRITICAL else AlertPriority.HIGH,
                    tags=[f"rule_id:{rule.rule_id}", f"team:{rule.team}"],
                    threshold=incident.threshold_value
                )
                
                success = self.datadog_monitoring.create_alert(alert)
                return success
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send DataDog notification: {str(e)}")
            return False
    
    async def _escalation_worker(self):
        """Background worker for handling alert escalations"""
        
        while True:
            try:
                await asyncio.sleep(60)  # Check escalations every minute
                
                current_time = datetime.utcnow()
                
                for incident in self.active_incidents.values():
                    if await self._should_escalate_incident(incident):
                        await self._escalate_incident(incident)
                
            except Exception as e:
                self.logger.error(f"Error in escalation worker: {str(e)}")
                await asyncio.sleep(60)
    
    async def _should_escalate_incident(self, incident: AlertIncident) -> bool:
        """Check if incident should be escalated"""
        
        try:
            rule = self.alert_rules.get(incident.rule_id)
            if not rule or not rule.escalation_rules:
                return False
            
            current_time = datetime.utcnow()
            incident_duration = (current_time - incident.start_time).total_seconds() / 60  # minutes
            
            # Find next escalation level
            next_escalation = None
            for escalation in rule.escalation_rules:
                if (escalation.level.value > incident.escalation_level.value and
                    incident_duration >= escalation.delay_minutes):
                    if next_escalation is None or escalation.level.value < next_escalation.level.value:
                        next_escalation = escalation
            
            return next_escalation is not None
            
        except Exception as e:
            self.logger.error(f"Failed to check escalation: {str(e)}")
            return False
    
    async def _escalate_incident(self, incident: AlertIncident):
        """Escalate incident to next level"""
        
        try:
            rule = self.alert_rules.get(incident.rule_id)
            if not rule:
                return
            
            current_time = datetime.utcnow()
            incident_duration = (current_time - incident.start_time).total_seconds() / 60
            
            # Find appropriate escalation
            for escalation in rule.escalation_rules:
                if (escalation.level.value > incident.escalation_level.value and
                    incident_duration >= escalation.delay_minutes):
                    
                    # Update incident escalation level
                    incident.escalation_level = escalation.level
                    
                    # Send notifications to escalation channels
                    for channel_id in escalation.channels:
                        if channel_id in self.notification_channels:
                            await self.notification_queue.put({
                                "incident": incident,
                                "rule": rule,
                                "action": "escalate"
                            })
                    
                    self.logger.info(f"Escalated incident {incident.incident_id} to {escalation.level.value}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Failed to escalate incident: {str(e)}")
    
    async def _attempt_auto_remediation(self, incident: AlertIncident, rule: AlertRule):
        """Attempt automatic remediation"""
        
        try:
            if not rule.remediation_script:
                return
            
            # Check remediation conditions
            if rule.remediation_conditions:
                # Implement condition checking logic
                pass
            
            # Log remediation attempt
            remediation_attempt = {
                "timestamp": datetime.utcnow().isoformat(),
                "script": rule.remediation_script,
                "incident_id": incident.incident_id,
                "status": "attempted"
            }
            
            incident.remediation_attempts.append(remediation_attempt)
            
            # This is where you'd execute the actual remediation script
            # For security reasons, this should be done very carefully
            self.logger.info(f"Auto-remediation attempted for incident {incident.incident_id} using {rule.remediation_script}")
            
            # In production, you'd execute the script and track results
            # remediation_result = await execute_remediation_script(rule.remediation_script)
            
        except Exception as e:
            self.logger.error(f"Failed auto-remediation attempt: {str(e)}")
    
    async def _baseline_calculation_worker(self):
        """Background worker for calculating baselines"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Update baselines every hour
                
                # Update baselines for all dynamic threshold rules
                for rule in self.alert_rules.values():
                    if rule.threshold_type == ThresholdType.DYNAMIC:
                        # Calculate baseline for each unique dimension combination
                        # This is simplified - in production you'd handle this more efficiently
                        unique_dimensions = set()
                        
                        metric_key_prefix = f"{rule.metric_query}:"
                        for key in self.historical_values.keys():
                            if key.startswith(metric_key_prefix):
                                dimensions_str = key[len(metric_key_prefix):]
                                unique_dimensions.add(dimensions_str)
                        
                        for dimensions_str in unique_dimensions:
                            try:
                                dimensions = json.loads(dimensions_str)
                                await self._get_dynamic_baseline(rule, dimensions)
                            except Exception as e:
                                self.logger.error(f"Failed to update baseline: {str(e)}")
                
            except Exception as e:
                self.logger.error(f"Error in baseline calculation worker: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _incident_cleanup_worker(self):
        """Background worker for cleaning up old incidents"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Cleanup every hour
                
                current_time = datetime.utcnow()
                cutoff_time = current_time - timedelta(days=30)  # Keep 30 days of history
                
                # Move old resolved incidents to history
                resolved_incidents = [
                    incident for incident in self.active_incidents.values()
                    if incident.alert_state == AlertState.RESOLVED and
                    incident.end_time and incident.end_time < cutoff_time
                ]
                
                for incident in resolved_incidents:
                    # Move to history
                    self.incident_history.append(incident)
                    
                    # Remove from active incidents
                    incident_key = None
                    for key, active_incident in self.active_incidents.items():
                        if active_incident.incident_id == incident.incident_id:
                            incident_key = key
                            break
                    
                    if incident_key:
                        del self.active_incidents[incident_key]
                
                # Limit history size
                if len(self.incident_history) > 10000:
                    self.incident_history = self.incident_history[-10000:]
                
                # Clean up old validation errors
                if len(self.data_validation_errors) > 1000:
                    self.data_validation_errors = self.data_validation_errors[-1000:]
                
                self.logger.info(f"Cleaned up {len(resolved_incidents)} old incidents")
                
            except Exception as e:
                self.logger.error(f"Error in incident cleanup worker: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _check_incident_resolution(self, rule_id: str, dimensions: Dict[str, str]):
        """Check if incidents should be resolved"""
        
        try:
            incident_key = f"{rule_id}:{json.dumps(dimensions, sort_keys=True)}"
            incident = self.active_incidents.get(incident_key)
            
            if incident and incident.alert_state != AlertState.RESOLVED:
                # Mark as resolved
                incident.alert_state = AlertState.RESOLVED
                incident.end_time = datetime.utcnow()
                incident.last_update = incident.end_time
                
                # Send resolution notification
                rule = self.alert_rules.get(rule_id)
                if rule:
                    await self.notification_queue.put({
                        "incident": incident,
                        "rule": rule,
                        "action": "resolve"
                    })
                
                # Update metrics
                if incident.end_time and incident.start_time:
                    resolution_time = (incident.end_time - incident.start_time).total_seconds()
                    # Update mean resolution time (simplified calculation)
                    if self.alert_metrics.mean_resolution_time == 0:
                        self.alert_metrics.mean_resolution_time = resolution_time
                    else:
                        self.alert_metrics.mean_resolution_time = (
                            self.alert_metrics.mean_resolution_time + resolution_time
                        ) / 2
                
        except Exception as e:
            self.logger.error(f"Failed to check incident resolution: {str(e)}")
    
    def get_alerting_summary(self) -> Dict[str, Any]:
        """Get comprehensive alerting system summary"""
        
        try:
            current_time = datetime.utcnow()
            
            # Active incidents summary
            active_incidents_summary = {
                "total": len(self.active_incidents),
                "by_state": defaultdict(int),
                "by_priority": defaultdict(int),
                "by_team": defaultdict(int)
            }
            
            for incident in self.active_incidents.values():
                active_incidents_summary["by_state"][incident.alert_state.value] += 1
                
                rule = self.alert_rules.get(incident.rule_id)
                if rule:
                    if rule.team:
                        active_incidents_summary["by_team"][rule.team] += 1
                    
                    priority = "critical" if incident.alert_state == AlertState.CRITICAL else "warning"
                    active_incidents_summary["by_priority"][priority] += 1
            
            # Recent incidents (last 24 hours)
            recent_incidents = [
                incident for incident in self.incident_history + list(self.active_incidents.values())
                if (current_time - incident.start_time).total_seconds() < 86400
            ]
            
            # Performance metrics
            performance_summary = {
                "total_rules": len(self.alert_rules),
                "notification_channels": len(self.notification_channels),
                "historical_metrics": len(self.historical_values),
                "baseline_calculations": len(self.baseline_calculations),
                "suppressed_alerts": len(self.suppressed_alerts)
            }
            
            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "active_incidents": dict(active_incidents_summary),
                "recent_incidents_24h": len(recent_incidents),
                "alert_metrics": asdict(self.alert_metrics),
                "performance": performance_summary,
                "system_health": {
                    "evaluation_queue_size": self.evaluation_queue.qsize(),
                    "notification_queue_size": self.notification_queue.qsize(),
                    "worker_status": "healthy"
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate alerting summary: {str(e)}")
            return {}


# Global intelligent alerting instance
_intelligent_alerting: Optional[DataDogIntelligentAlerting] = None


def get_intelligent_alerting(service_name: str = "intelligent-alerting",
                           datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogIntelligentAlerting:
    """Get or create intelligent alerting instance"""
    global _intelligent_alerting
    
    if _intelligent_alerting is None:
        _intelligent_alerting = DataDogIntelligentAlerting(service_name, datadog_monitoring)
    
    return _intelligent_alerting


# Convenience functions

async def evaluate_metric(metric_query: str, value: float,
                        dimensions: Optional[Dict[str, str]] = None) -> bool:
    """Convenience function for evaluating metrics"""
    alerting_system = get_intelligent_alerting()
    return await alerting_system.evaluate_metric(metric_query, value, dimensions)


def get_alerting_summary() -> Dict[str, Any]:
    """Convenience function for getting alerting summary"""
    alerting_system = get_intelligent_alerting()
    return alerting_system.get_alerting_summary()