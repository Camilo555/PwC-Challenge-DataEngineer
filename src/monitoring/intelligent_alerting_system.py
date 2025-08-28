"""
Intelligent Alerting System for Infrastructure Monitoring
Provides smart alert generation, escalation, notification routing,
and adaptive thresholds based on machine learning patterns.
"""
import asyncio
import json
import smtplib
import time
import statistics
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from typing import Any, Dict, List, Optional, Callable, Union, Set
from collections import defaultdict, deque
import threading
import logging
from pathlib import Path

try:
    import requests
    import slack_sdk
    NOTIFICATION_PROVIDERS_AVAILABLE = True
except ImportError:
    NOTIFICATION_PROVIDERS_AVAILABLE = False

from core.config import settings
from core.logging import get_logger
from .infrastructure_health_monitor import (
    Alert, AlertSeverity, HealthStatus, ComponentType, HealthMetrics
)

logger = get_logger(__name__)


class AlertState(Enum):
    """Alert lifecycle states"""
    TRIGGERED = "triggered"
    ACKNOWLEDGED = "acknowledged"
    ESCALATED = "escalated"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class NotificationChannel(Enum):
    """Notification delivery channels"""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    SMS = "sms"
    PAGERDUTY = "pagerduty"
    DISCORD = "discord"


@dataclass
class AlertRule:
    """Alert rule definition"""
    id: str
    name: str
    component_type: ComponentType
    metric_name: str
    condition: str  # "gt", "lt", "eq", "ne"
    threshold: float
    severity: AlertSeverity
    enabled: bool = True
    cooldown_seconds: int = 300  # 5 minutes
    escalation_seconds: int = 900  # 15 minutes
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EscalationLevel:
    """Alert escalation level"""
    level: int
    delay_seconds: int
    channels: List[NotificationChannel]
    recipients: List[str]
    message_template: str
    repeat_interval_seconds: Optional[int] = None


@dataclass
class NotificationTemplate:
    """Notification message template"""
    channel: NotificationChannel
    severity: AlertSeverity
    subject_template: str
    body_template: str
    format: str = "text"  # text, html, markdown


@dataclass
class AlertContext:
    """Enhanced alert with context and state"""
    alert: Alert
    rule: AlertRule
    state: AlertState
    created_at: datetime
    acknowledged_at: Optional[datetime] = None
    escalated_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    escalation_level: int = 0
    notification_count: int = 0
    last_notification: Optional[datetime] = None
    context_data: Dict[str, Any] = field(default_factory=dict)


class AlertProcessor(ABC):
    """Base class for alert processors"""
    
    @abstractmethod
    async def should_trigger(self, metrics: HealthMetrics, rule: AlertRule) -> bool:
        """Determine if alert should trigger"""
        pass
    
    @abstractmethod
    async def enrich_alert(self, alert: Alert, metrics: HealthMetrics) -> Dict[str, Any]:
        """Enrich alert with additional context"""
        pass


class ThresholdProcessor(AlertProcessor):
    """Standard threshold-based alert processor"""
    
    async def should_trigger(self, metrics: HealthMetrics, rule: AlertRule) -> bool:
        """Check if metric violates threshold"""
        try:
            if rule.metric_name in metrics.custom_metrics:
                value = metrics.custom_metrics[rule.metric_name]
            else:
                value = getattr(metrics, rule.metric_name, None)
            
            if value is None:
                return False
            
            if rule.condition == "gt":
                return value > rule.threshold
            elif rule.condition == "lt":
                return value < rule.threshold
            elif rule.condition == "eq":
                return value == rule.threshold
            elif rule.condition == "ne":
                return value != rule.threshold
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error evaluating rule {rule.id}: {e}")
            return False
    
    async def enrich_alert(self, alert: Alert, metrics: HealthMetrics) -> Dict[str, Any]:
        """Add metric context to alert"""
        return {
            'current_value': getattr(metrics, alert.metadata.get('metric_name'), 'unknown'),
            'threshold': alert.metadata.get('threshold'),
            'component_status': metrics.status.value,
            'response_time_ms': metrics.response_time_ms,
            'availability_percent': metrics.availability_percent
        }


class AnomalyProcessor(AlertProcessor):
    """Machine learning-based anomaly detection processor"""
    
    def __init__(self):
        self.baselines: Dict[str, List[float]] = defaultdict(list)
        self.window_size = 100
        self.anomaly_threshold = 2.5  # Standard deviations
    
    async def should_trigger(self, metrics: HealthMetrics, rule: AlertRule) -> bool:
        """Detect anomalies using statistical methods"""
        try:
            metric_key = f"{metrics.component_id}_{rule.metric_name}"
            
            if rule.metric_name in metrics.custom_metrics:
                value = metrics.custom_metrics[rule.metric_name]
            else:
                value = getattr(metrics, rule.metric_name, None)
            
            if value is None:
                return False
            
            # Add to baseline
            self.baselines[metric_key].append(value)
            
            # Keep only recent values
            if len(self.baselines[metric_key]) > self.window_size:
                self.baselines[metric_key] = self.baselines[metric_key][-self.window_size:]
            
            # Need sufficient data for anomaly detection
            if len(self.baselines[metric_key]) < 30:
                return False
            
            # Calculate z-score
            baseline_values = self.baselines[metric_key][:-1]  # Exclude current value
            mean_val = statistics.mean(baseline_values)
            stdev_val = statistics.stdev(baseline_values) if len(baseline_values) > 1 else 0
            
            if stdev_val == 0:
                return False
            
            z_score = abs(value - mean_val) / stdev_val
            return z_score > self.anomaly_threshold
            
        except Exception as e:
            logger.error(f"Error in anomaly detection for rule {rule.id}: {e}")
            return False
    
    async def enrich_alert(self, alert: Alert, metrics: HealthMetrics) -> Dict[str, Any]:
        """Add anomaly context to alert"""
        metric_key = f"{metrics.component_id}_{alert.metadata.get('metric_name')}"
        baseline_values = self.baselines.get(metric_key, [])
        
        context = {
            'anomaly_type': 'statistical',
            'baseline_count': len(baseline_values),
            'detection_method': 'z_score'
        }
        
        if baseline_values:
            context.update({
                'baseline_mean': statistics.mean(baseline_values),
                'baseline_stdev': statistics.stdev(baseline_values) if len(baseline_values) > 1 else 0
            })
        
        return context


class NotificationProvider(ABC):
    """Base class for notification providers"""
    
    @abstractmethod
    async def send_notification(self, recipient: str, subject: str, 
                               message: str, severity: AlertSeverity) -> bool:
        """Send notification"""
        pass


class EmailNotificationProvider(NotificationProvider):
    """Email notification provider"""
    
    def __init__(self, smtp_server: str, smtp_port: int, username: str, 
                 password: str, use_tls: bool = True):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
    
    async def send_notification(self, recipient: str, subject: str, 
                               message: str, severity: AlertSeverity) -> bool:
        """Send email notification"""
        try:
            msg = MimeMultipart()
            msg['From'] = self.username
            msg['To'] = recipient
            msg['Subject'] = f"[{severity.value.upper()}] {subject}"
            
            msg.attach(MimeText(message, 'plain'))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"Email sent to {recipient} for alert: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {recipient}: {e}")
            return False


class SlackNotificationProvider(NotificationProvider):
    """Slack notification provider"""
    
    def __init__(self, bot_token: str, default_channel: str = "#alerts"):
        if not NOTIFICATION_PROVIDERS_AVAILABLE:
            raise ImportError("slack_sdk not available")
        
        self.client = slack_sdk.WebClient(token=bot_token)
        self.default_channel = default_channel
    
    async def send_notification(self, recipient: str, subject: str, 
                               message: str, severity: AlertSeverity) -> bool:
        """Send Slack notification"""
        try:
            # Use recipient as channel if provided, otherwise default
            channel = recipient if recipient.startswith('#') or recipient.startswith('@') else self.default_channel
            
            # Color coding by severity
            color_map = {
                AlertSeverity.INFO: "good",
                AlertSeverity.WARNING: "warning", 
                AlertSeverity.CRITICAL: "danger",
                AlertSeverity.EMERGENCY: "#ff0000"
            }
            
            attachments = [{
                "color": color_map.get(severity, "warning"),
                "title": subject,
                "text": message,
                "footer": f"Infrastructure Monitoring • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "mrkdwn_in": ["text"]
            }]
            
            response = self.client.chat_postMessage(
                channel=channel,
                text=f"*{severity.value.upper()} Alert*",
                attachments=attachments
            )
            
            logger.info(f"Slack message sent to {channel} for alert: {subject}")
            return response.get("ok", False)
            
        except Exception as e:
            logger.error(f"Failed to send Slack message to {recipient}: {e}")
            return False


class WebhookNotificationProvider(NotificationProvider):
    """Webhook notification provider"""
    
    def __init__(self, webhook_url: str, headers: Dict[str, str] = None):
        self.webhook_url = webhook_url
        self.headers = headers or {}
    
    async def send_notification(self, recipient: str, subject: str, 
                               message: str, severity: AlertSeverity) -> bool:
        """Send webhook notification"""
        try:
            if not NOTIFICATION_PROVIDERS_AVAILABLE:
                logger.error("requests library not available for webhook notifications")
                return False
            
            payload = {
                "alert": {
                    "subject": subject,
                    "message": message,
                    "severity": severity.value,
                    "timestamp": datetime.now().isoformat(),
                    "recipient": recipient
                }
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"Webhook notification sent for alert: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send webhook notification: {e}")
            return False


class IntelligentAlertingSystem:
    """Central intelligent alerting system"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Alert rules and processors
        self.rules: Dict[str, AlertRule] = {}
        self.processors: Dict[str, AlertProcessor] = {
            'threshold': ThresholdProcessor(),
            'anomaly': AnomalyProcessor()
        }
        
        # Alert state management
        self.active_alerts: Dict[str, AlertContext] = {}
        self.alert_history: deque = deque(maxlen=10000)
        
        # Notification system
        self.notification_providers: Dict[NotificationChannel, NotificationProvider] = {}
        self.notification_templates: Dict[str, NotificationTemplate] = {}
        self.escalation_levels: List[EscalationLevel] = []
        
        # Alert suppression and correlation
        self.suppression_rules: Dict[str, Dict[str, Any]] = {}
        self.correlation_window_seconds = 300  # 5 minutes
        
        # Background processing
        self.processing_active = False
        self.processing_task = None
        
        # Setup default configuration
        self._setup_default_rules()
        self._setup_default_templates()
        self._setup_default_escalation()
    
    def _setup_default_rules(self):
        """Setup default alert rules"""
        default_rules = [
            # Redis rules
            AlertRule(
                id="redis_memory_critical",
                name="Redis Memory Usage Critical",
                component_type=ComponentType.REDIS,
                metric_name="memory_usage_percent",
                condition="gt",
                threshold=95.0,
                severity=AlertSeverity.CRITICAL,
                tags={"redis", "memory", "critical"}
            ),
            AlertRule(
                id="redis_cache_hit_low",
                name="Redis Cache Hit Ratio Low",
                component_type=ComponentType.REDIS,
                metric_name="cache_hit_ratio",
                condition="lt",
                threshold=85.0,
                severity=AlertSeverity.WARNING,
                tags={"redis", "performance", "cache"}
            ),
            # RabbitMQ rules
            AlertRule(
                id="rabbitmq_queue_depth_critical",
                name="RabbitMQ Queue Depth Critical",
                component_type=ComponentType.RABBITMQ,
                metric_name="queue_depth_total",
                condition="gt",
                threshold=5000,
                severity=AlertSeverity.CRITICAL,
                tags={"rabbitmq", "queue", "critical"}
            ),
            AlertRule(
                id="rabbitmq_no_consumers",
                name="RabbitMQ No Consumers",
                component_type=ComponentType.RABBITMQ,
                metric_name="consumer_count",
                condition="eq",
                threshold=0,
                severity=AlertSeverity.WARNING,
                tags={"rabbitmq", "consumer", "warning"}
            ),
            # Kafka rules
            AlertRule(
                id="kafka_consumer_lag_critical",
                name="Kafka Consumer Lag Critical",
                component_type=ComponentType.KAFKA,
                metric_name="consumer_lag_total",
                condition="gt",
                threshold=10000,
                severity=AlertSeverity.CRITICAL,
                tags={"kafka", "consumer", "lag", "critical"}
            ),
            AlertRule(
                id="kafka_offline_partitions",
                name="Kafka Offline Partitions",
                component_type=ComponentType.KAFKA,
                metric_name="offline_partitions",
                condition="gt",
                threshold=0,
                severity=AlertSeverity.CRITICAL,
                tags={"kafka", "partition", "offline", "critical"}
            )
        ]
        
        for rule in default_rules:
            self.rules[rule.id] = rule
    
    def _setup_default_templates(self):
        """Setup default notification templates"""
        templates = [
            NotificationTemplate(
                channel=NotificationChannel.EMAIL,
                severity=AlertSeverity.CRITICAL,
                subject_template="CRITICAL: {alert_title}",
                body_template="""
CRITICAL ALERT

Component: {component_id}
Alert: {alert_title}
Message: {alert_message}
Timestamp: {timestamp}
Severity: {severity}

Current Value: {current_value}
Threshold: {threshold}
Response Time: {response_time_ms}ms
Availability: {availability_percent}%

This is a critical alert requiring immediate attention.

Infrastructure Monitoring System
                """.strip()
            ),
            NotificationTemplate(
                channel=NotificationChannel.SLACK,
                severity=AlertSeverity.WARNING,
                subject_template="⚠️ {alert_title}",
                body_template="""
:warning: *WARNING ALERT*

*Component:* {component_id}
*Alert:* {alert_title}
*Message:* {alert_message}
*Timestamp:* {timestamp}

*Metrics:*
• Current Value: `{current_value}`
• Threshold: `{threshold}`  
• Response Time: `{response_time_ms}ms`
• Availability: `{availability_percent}%`

Please investigate when possible.
                """.strip(),
                format="markdown"
            )
        ]
        
        for template in templates:
            key = f"{template.channel.value}_{template.severity.value}"
            self.notification_templates[key] = template
    
    def _setup_default_escalation(self):
        """Setup default escalation levels"""
        self.escalation_levels = [
            EscalationLevel(
                level=1,
                delay_seconds=0,  # Immediate
                channels=[NotificationChannel.SLACK],
                recipients=["#alerts"],
                message_template="initial"
            ),
            EscalationLevel(
                level=2,
                delay_seconds=900,  # 15 minutes
                channels=[NotificationChannel.EMAIL, NotificationChannel.SLACK],
                recipients=["ops-team@company.com", "#critical-alerts"],
                message_template="escalated"
            ),
            EscalationLevel(
                level=3,
                delay_seconds=1800,  # 30 minutes
                channels=[NotificationChannel.EMAIL],
                recipients=["manager@company.com"],
                message_template="executive",
                repeat_interval_seconds=3600  # Repeat every hour
            )
        ]
    
    def add_notification_provider(self, channel: NotificationChannel, 
                                 provider: NotificationProvider):
        """Add notification provider"""
        self.notification_providers[channel] = provider
        self.logger.info(f"Added {channel.value} notification provider")
    
    def add_alert_rule(self, rule: AlertRule):
        """Add alert rule"""
        self.rules[rule.id] = rule
        self.logger.info(f"Added alert rule: {rule.name}")
    
    def remove_alert_rule(self, rule_id: str):
        """Remove alert rule"""
        if rule_id in self.rules:
            del self.rules[rule_id]
            self.logger.info(f"Removed alert rule: {rule_id}")
    
    async def process_metrics(self, metrics: HealthMetrics):
        """Process metrics against alert rules"""
        try:
            for rule in self.rules.values():
                if not rule.enabled:
                    continue
                
                if rule.component_type != metrics.component_type:
                    continue
                
                # Check if alert is in cooldown
                if await self._is_in_cooldown(rule, metrics.component_id):
                    continue
                
                # Process with appropriate processor
                processor = self.processors.get('threshold', self.processors['threshold'])
                if 'anomaly' in rule.tags:
                    processor = self.processors.get('anomaly', self.processors['threshold'])
                
                should_trigger = await processor.should_trigger(metrics, rule)
                
                if should_trigger:
                    alert = Alert(
                        id=f"{rule.id}_{metrics.component_id}_{int(time.time())}",
                        component_type=metrics.component_type,
                        component_id=metrics.component_id,
                        severity=rule.severity,
                        title=rule.name,
                        message=f"Alert triggered for {metrics.component_id}",
                        timestamp=datetime.now(),
                        metadata={
                            'rule_id': rule.id,
                            'metric_name': rule.metric_name,
                            'threshold': rule.threshold,
                            'condition': rule.condition
                        }
                    )
                    
                    # Enrich alert with context
                    context = await processor.enrich_alert(alert, metrics)
                    alert.metadata.update(context)
                    
                    await self._handle_new_alert(alert, rule)
        
        except Exception as e:
            self.logger.error(f"Error processing metrics: {e}")
    
    async def _is_in_cooldown(self, rule: AlertRule, component_id: str) -> bool:
        """Check if alert rule is in cooldown period"""
        cooldown_key = f"{rule.id}_{component_id}"
        
        for alert_context in self.active_alerts.values():
            if (alert_context.rule.id == rule.id and 
                alert_context.alert.component_id == component_id and
                alert_context.state not in [AlertState.RESOLVED]):
                
                time_since_created = (datetime.now() - alert_context.created_at).total_seconds()
                if time_since_created < rule.cooldown_seconds:
                    return True
        
        return False
    
    async def _handle_new_alert(self, alert: Alert, rule: AlertRule):
        """Handle new alert"""
        try:
            # Check for suppression
            if await self._should_suppress_alert(alert):
                self.logger.info(f"Suppressed alert: {alert.title}")
                return
            
            # Create alert context
            alert_context = AlertContext(
                alert=alert,
                rule=rule,
                state=AlertState.TRIGGERED,
                created_at=datetime.now(),
                context_data=alert.metadata
            )
            
            self.active_alerts[alert.id] = alert_context
            self.alert_history.append(alert_context)
            
            # Send initial notifications
            await self._send_notifications(alert_context, escalation_level=1)
            
            self.logger.warning(f"New alert: {alert.title} for {alert.component_id}")
        
        except Exception as e:
            self.logger.error(f"Error handling new alert: {e}")
    
    async def _should_suppress_alert(self, alert: Alert) -> bool:
        """Check if alert should be suppressed"""
        try:
            # Check for similar recent alerts (correlation)
            cutoff_time = datetime.now() - timedelta(seconds=self.correlation_window_seconds)
            
            similar_alerts = 0
            for alert_context in self.active_alerts.values():
                if (alert_context.alert.component_type == alert.component_type and
                    alert_context.alert.severity == alert.severity and
                    alert_context.created_at > cutoff_time):
                    similar_alerts += 1
            
            # Suppress if too many similar alerts
            if similar_alerts > 5:
                return True
            
            # Check suppression rules
            for rule_id, rule_config in self.suppression_rules.items():
                if self._matches_suppression_rule(alert, rule_config):
                    return True
            
            return False
        
        except Exception as e:
            self.logger.error(f"Error checking alert suppression: {e}")
            return False
    
    def _matches_suppression_rule(self, alert: Alert, rule_config: Dict[str, Any]) -> bool:
        """Check if alert matches suppression rule"""
        # Simplified suppression logic
        if 'component_type' in rule_config:
            if alert.component_type.value != rule_config['component_type']:
                return False
        
        if 'severity' in rule_config:
            if alert.severity.value not in rule_config['severity']:
                return False
        
        return True
    
    async def _send_notifications(self, alert_context: AlertContext, escalation_level: int = 1):
        """Send notifications for alert"""
        try:
            # Find escalation level
            escalation = None
            for level in self.escalation_levels:
                if level.level == escalation_level:
                    escalation = level
                    break
            
            if not escalation:
                self.logger.warning(f"No escalation level {escalation_level} found")
                return
            
            # Format message
            template_key = f"{escalation.channels[0].value}_{alert_context.alert.severity.value}"
            template = self.notification_templates.get(template_key)
            
            if not template:
                # Use generic template
                subject = alert_context.alert.title
                message = f"{alert_context.alert.message}\n\nComponent: {alert_context.alert.component_id}\nSeverity: {alert_context.alert.severity.value}\nTimestamp: {alert_context.alert.timestamp}"
            else:
                subject = template.subject_template.format(
                    alert_title=alert_context.alert.title,
                    component_id=alert_context.alert.component_id,
                    severity=alert_context.alert.severity.value
                )
                message = template.body_template.format(
                    alert_title=alert_context.alert.title,
                    alert_message=alert_context.alert.message,
                    component_id=alert_context.alert.component_id,
                    severity=alert_context.alert.severity.value,
                    timestamp=alert_context.alert.timestamp.isoformat(),
                    current_value=alert_context.context_data.get('current_value', 'N/A'),
                    threshold=alert_context.context_data.get('threshold', 'N/A'),
                    response_time_ms=alert_context.context_data.get('response_time_ms', 'N/A'),
                    availability_percent=alert_context.context_data.get('availability_percent', 'N/A')
                )
            
            # Send notifications
            success_count = 0
            for channel in escalation.channels:
                provider = self.notification_providers.get(channel)
                if not provider:
                    self.logger.warning(f"No provider for channel {channel.value}")
                    continue
                
                for recipient in escalation.recipients:
                    try:
                        success = await provider.send_notification(
                            recipient, subject, message, alert_context.alert.severity
                        )
                        if success:
                            success_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to send notification to {recipient} via {channel.value}: {e}")
            
            # Update alert context
            alert_context.notification_count += success_count
            alert_context.last_notification = datetime.now()
            
            if escalation_level > alert_context.escalation_level:
                alert_context.escalation_level = escalation_level
                alert_context.escalated_at = datetime.now()
                alert_context.state = AlertState.ESCALATED
            
            self.logger.info(f"Sent {success_count} notifications for alert: {alert_context.alert.title}")
        
        except Exception as e:
            self.logger.error(f"Error sending notifications: {e}")
    
    async def start_processing(self):
        """Start background alert processing"""
        if self.processing_active:
            return
        
        self.processing_active = True
        self.processing_task = asyncio.create_task(self._processing_loop())
        self.logger.info("Started intelligent alerting system")
    
    async def stop_processing(self):
        """Stop background processing"""
        self.processing_active = False
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped intelligent alerting system")
    
    async def _processing_loop(self):
        """Background processing loop"""
        while self.processing_active:
            try:
                await self._process_escalations()
                await self._process_auto_resolution()
                await self._cleanup_old_alerts()
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in processing loop: {e}")
                await asyncio.sleep(60)
    
    async def _process_escalations(self):
        """Process alert escalations"""
        current_time = datetime.now()
        
        for alert_context in self.active_alerts.values():
            if alert_context.state == AlertState.RESOLVED:
                continue
            
            # Check if alert should be escalated
            time_since_created = (current_time - alert_context.created_at).total_seconds()
            next_escalation_level = alert_context.escalation_level + 1
            
            # Find next escalation level
            next_escalation = None
            for level in self.escalation_levels:
                if level.level == next_escalation_level:
                    next_escalation = level
                    break
            
            if next_escalation and time_since_created >= next_escalation.delay_seconds:
                await self._send_notifications(alert_context, next_escalation_level)
    
    async def _process_auto_resolution(self):
        """Process automatic alert resolution"""
        # This would typically check if the underlying condition is resolved
        # For now, we'll implement a simple time-based auto-resolution for non-critical alerts
        
        current_time = datetime.now()
        auto_resolve_threshold = 3600  # 1 hour
        
        for alert_id, alert_context in list(self.active_alerts.items()):
            if (alert_context.alert.severity != AlertSeverity.CRITICAL and
                alert_context.state not in [AlertState.RESOLVED] and
                (current_time - alert_context.created_at).total_seconds() > auto_resolve_threshold):
                
                await self.resolve_alert(alert_id, auto_resolved=True)
    
    async def _cleanup_old_alerts(self):
        """Cleanup old resolved alerts"""
        current_time = datetime.now()
        cleanup_threshold = 86400  # 24 hours
        
        to_remove = []
        for alert_id, alert_context in self.active_alerts.items():
            if (alert_context.state == AlertState.RESOLVED and
                alert_context.resolved_at and
                (current_time - alert_context.resolved_at).total_seconds() > cleanup_threshold):
                to_remove.append(alert_id)
        
        for alert_id in to_remove:
            del self.active_alerts[alert_id]
    
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str = "system") -> bool:
        """Acknowledge an alert"""
        if alert_id not in self.active_alerts:
            return False
        
        alert_context = self.active_alerts[alert_id]
        alert_context.state = AlertState.ACKNOWLEDGED
        alert_context.acknowledged_at = datetime.now()
        alert_context.context_data['acknowledged_by'] = acknowledged_by
        
        self.logger.info(f"Alert acknowledged: {alert_context.alert.title} by {acknowledged_by}")
        return True
    
    async def resolve_alert(self, alert_id: str, resolved_by: str = "system", 
                           auto_resolved: bool = False) -> bool:
        """Resolve an alert"""
        if alert_id not in self.active_alerts:
            return False
        
        alert_context = self.active_alerts[alert_id]
        alert_context.state = AlertState.RESOLVED
        alert_context.resolved_at = datetime.now()
        alert_context.context_data['resolved_by'] = resolved_by
        alert_context.context_data['auto_resolved'] = auto_resolved
        
        # Send resolution notification if it was escalated
        if alert_context.escalation_level > 1:
            await self._send_resolution_notification(alert_context)
        
        self.logger.info(f"Alert resolved: {alert_context.alert.title} by {resolved_by}")
        return True
    
    async def _send_resolution_notification(self, alert_context: AlertContext):
        """Send alert resolution notification"""
        try:
            subject = f"RESOLVED: {alert_context.alert.title}"
            message = f"Alert has been resolved.\n\nComponent: {alert_context.alert.component_id}\nResolved at: {alert_context.resolved_at}\nResolved by: {alert_context.context_data.get('resolved_by', 'system')}"
            
            # Send to escalated channels only
            for channel in self.notification_providers.keys():
                provider = self.notification_providers[channel]
                # Use first escalation level recipients for resolution notifications
                if self.escalation_levels:
                    for recipient in self.escalation_levels[0].recipients:
                        await provider.send_notification(
                            recipient, subject, message, AlertSeverity.INFO
                        )
        
        except Exception as e:
            self.logger.error(f"Error sending resolution notification: {e}")
    
    def get_active_alerts(self, component_id: str = None, 
                         severity: AlertSeverity = None) -> List[Dict[str, Any]]:
        """Get active alerts with optional filtering"""
        alerts = []
        
        for alert_context in self.active_alerts.values():
            if alert_context.state == AlertState.RESOLVED:
                continue
            
            if component_id and alert_context.alert.component_id != component_id:
                continue
            
            if severity and alert_context.alert.severity != severity:
                continue
            
            alert_data = asdict(alert_context)
            alert_data['alert'] = asdict(alert_context.alert)
            alerts.append(alert_data)
        
        return sorted(alerts, key=lambda x: x['created_at'], reverse=True)
    
    def get_alert_statistics(self) -> Dict[str, Any]:
        """Get alert system statistics"""
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)
        
        # Count alerts by severity and timeframe
        stats = {
            'total_active': len([a for a in self.active_alerts.values() if a.state != AlertState.RESOLVED]),
            'total_rules': len(self.rules),
            'total_providers': len(self.notification_providers),
            'last_24h': {
                'triggered': 0,
                'resolved': 0,
                'by_severity': {s.value: 0 for s in AlertSeverity}
            },
            'last_7d': {
                'triggered': 0,
                'resolved': 0,
                'by_severity': {s.value: 0 for s in AlertSeverity}
            }
        }
        
        for alert_context in self.alert_history:
            # 24 hour stats
            if alert_context.created_at >= last_24h:
                stats['last_24h']['triggered'] += 1
                stats['last_24h']['by_severity'][alert_context.alert.severity.value] += 1
                
                if alert_context.resolved_at and alert_context.resolved_at >= last_24h:
                    stats['last_24h']['resolved'] += 1
            
            # 7 day stats
            if alert_context.created_at >= last_7d:
                stats['last_7d']['triggered'] += 1
                stats['last_7d']['by_severity'][alert_context.alert.severity.value] += 1
                
                if alert_context.resolved_at and alert_context.resolved_at >= last_7d:
                    stats['last_7d']['resolved'] += 1
        
        return stats


# Factory function
def create_intelligent_alerting_system() -> IntelligentAlertingSystem:
    """Create intelligent alerting system with default configuration"""
    system = IntelligentAlertingSystem()
    
    # Setup notification providers based on settings
    try:
        # Email provider
        if hasattr(settings, 'smtp_server'):
            email_provider = EmailNotificationProvider(
                smtp_server=settings.smtp_server,
                smtp_port=getattr(settings, 'smtp_port', 587),
                username=getattr(settings, 'smtp_username', ''),
                password=getattr(settings, 'smtp_password', ''),
                use_tls=getattr(settings, 'smtp_use_tls', True)
            )
            system.add_notification_provider(NotificationChannel.EMAIL, email_provider)
        
        # Slack provider
        if hasattr(settings, 'slack_bot_token'):
            slack_provider = SlackNotificationProvider(
                bot_token=settings.slack_bot_token,
                default_channel=getattr(settings, 'slack_default_channel', '#alerts')
            )
            system.add_notification_provider(NotificationChannel.SLACK, slack_provider)
        
        # Webhook provider
        if hasattr(settings, 'webhook_url'):
            webhook_provider = WebhookNotificationProvider(
                webhook_url=settings.webhook_url,
                headers=getattr(settings, 'webhook_headers', {})
            )
            system.add_notification_provider(NotificationChannel.WEBHOOK, webhook_provider)
    
    except Exception as e:
        logger.warning(f"Could not setup all notification providers: {e}")
    
    return system


# Global instance
_alerting_system = None


def get_alerting_system() -> IntelligentAlertingSystem:
    """Get global alerting system"""
    global _alerting_system
    if _alerting_system is None:
        _alerting_system = create_intelligent_alerting_system()
    return _alerting_system


if __name__ == "__main__":
    print("Intelligent Alerting System")
    print("Features:")
    print("- Threshold and anomaly-based alerting")
    print("- Multi-channel notifications (Email, Slack, Webhook)")
    print("- Smart escalation and correlation")
    print("- Alert suppression and auto-resolution")
    print("- Performance baseline tracking")
    print("- Comprehensive alert lifecycle management")