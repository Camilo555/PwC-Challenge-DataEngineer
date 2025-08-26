"""
Security Alerting System
Advanced multi-channel security alerting with severity-based escalation,
correlation, deduplication, and automated response capabilities.
"""
import asyncio
import json
import smtplib
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import threading
import time
import aiohttp
import hashlib

from core.logging import get_logger
from monitoring.security_metrics import SecurityEventSeverity, SecurityMetricType, SecurityMetricEvent
from monitoring.security_observability import SecurityTraceType


logger = get_logger(__name__)


class AlertChannel(Enum):
    """Available alerting channels"""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    SMS = "sms"
    TEAMS = "teams"
    PAGERDUTY = "pagerduty"
    DISCORD = "discord"
    CONSOLE = "console"
    DATABASE = "database"


class AlertStatus(Enum):
    """Alert status"""
    TRIGGERED = "triggered"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"
    ESCALATED = "escalated"
    EXPIRED = "expired"


class EscalationLevel(Enum):
    """Escalation levels"""
    LEVEL_1 = "level_1"  # Initial notification
    LEVEL_2 = "level_2"  # Supervisor notification
    LEVEL_3 = "level_3"  # Management notification
    LEVEL_4 = "level_4"  # Executive notification


@dataclass
class AlertRule:
    """Alert rule configuration"""
    rule_id: str
    name: str
    description: str
    enabled: bool
    
    # Trigger conditions
    metric_type: SecurityMetricType
    severity_threshold: SecurityEventSeverity
    frequency_threshold: int  # Events per time window
    time_window_minutes: int
    
    # Alert configuration
    channels: List[AlertChannel]
    escalation_enabled: bool = False
    escalation_delay_minutes: int = 30
    max_escalation_level: EscalationLevel = EscalationLevel.LEVEL_2
    
    # Filtering and correlation
    user_filter: Optional[List[str]] = None
    resource_filter: Optional[List[str]] = None
    correlation_fields: List[str] = field(default_factory=lambda: ["user_id", "source_ip"])
    correlation_window_minutes: int = 15
    
    # Auto-response
    auto_response_enabled: bool = False
    auto_response_actions: List[str] = field(default_factory=list)
    
    # Suppression
    suppression_enabled: bool = False
    suppression_duration_minutes: int = 60
    
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class AlertInstance:
    """Individual alert instance"""
    alert_id: str
    rule_id: str
    correlation_id: str
    
    # Alert details
    title: str
    description: str
    severity: SecurityEventSeverity
    metric_type: SecurityMetricType
    
    # Triggering event details
    triggering_events: List[SecurityMetricEvent]
    user_id: Optional[str]
    source_ip: Optional[str]
    resource_id: Optional[str]
    
    # Status and timing
    status: AlertStatus
    triggered_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    resolved_by: Optional[str] = None
    
    # Escalation
    current_escalation_level: EscalationLevel = EscalationLevel.LEVEL_1
    escalated_at: Optional[datetime] = None
    escalation_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # Notifications
    notifications_sent: List[Dict[str, Any]] = field(default_factory=list)
    
    # Additional context
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class ChannelConfig:
    """Configuration for a specific alerting channel"""
    channel: AlertChannel
    enabled: bool
    
    # Channel-specific settings
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Filtering
    severity_filter: List[SecurityEventSeverity] = field(default_factory=list)
    escalation_levels: List[EscalationLevel] = field(default_factory=list)
    
    # Rate limiting
    rate_limit_enabled: bool = False
    max_alerts_per_hour: int = 10
    
    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: int = 30


class AlertCorrelationEngine:
    """Engine for correlating and deduplicating alerts"""
    
    def __init__(self):
        self.correlation_groups: Dict[str, List[SecurityMetricEvent]] = defaultdict(list)
        self.correlation_timers: Dict[str, asyncio.Task] = {}
        self.logger = get_logger(__name__)
    
    def correlate_event(
        self,
        event: SecurityMetricEvent,
        correlation_fields: List[str],
        correlation_window_minutes: int
    ) -> Tuple[str, bool]:
        """
        Correlate event and return correlation ID and whether it's a new correlation group
        """
        
        # Generate correlation key based on specified fields
        correlation_values = []
        for field in correlation_fields:
            if field == "user_id":
                correlation_values.append(event.user_id or "anonymous")
            elif field == "source_ip":
                correlation_values.append(event.source_ip or "unknown")
            elif field == "resource_id":
                correlation_values.append(event.resource_id)
            elif field == "metric_type":
                correlation_values.append(event.metric_type.value)
            elif field in event.details:
                correlation_values.append(str(event.details[field]))
            else:
                correlation_values.append("unknown")
        
        # Create correlation ID
        correlation_key = ":".join(correlation_values)
        correlation_id = hashlib.sha256(
            f"{correlation_key}:{event.metric_type.value}".encode()
        ).hexdigest()[:16]
        
        # Check if this is a new correlation group
        is_new_group = correlation_id not in self.correlation_groups
        
        # Add event to correlation group
        self.correlation_groups[correlation_id].append(event)
        
        # Set up timer to process correlation group
        if correlation_id in self.correlation_timers:
            self.correlation_timers[correlation_id].cancel()
        
        self.correlation_timers[correlation_id] = asyncio.create_task(
            self._process_correlation_group_delayed(correlation_id, correlation_window_minutes)
        )
        
        return correlation_id, is_new_group
    
    async def _process_correlation_group_delayed(self, correlation_id: str, delay_minutes: int):
        """Process correlation group after delay"""
        try:
            await asyncio.sleep(delay_minutes * 60)
            
            # Process the correlation group
            events = self.correlation_groups.get(correlation_id, [])
            
            if events:
                self.logger.debug(f"Processing correlation group {correlation_id} with {len(events)} events")
                # The correlation group will be processed by the alerting system
                
        except asyncio.CancelledError:
            # Timer was cancelled (new event arrived), this is normal
            pass
        except Exception as e:
            self.logger.error(f"Error processing correlation group {correlation_id}: {e}")
    
    def get_correlation_group(self, correlation_id: str) -> List[SecurityMetricEvent]:
        """Get all events in a correlation group"""
        return self.correlation_groups.get(correlation_id, [])
    
    def cleanup_old_correlations(self, max_age_hours: int = 24):
        """Clean up old correlation groups"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        # Remove old correlation groups
        old_groups = []
        for correlation_id, events in self.correlation_groups.items():
            if events and events[0].timestamp < cutoff_time:
                old_groups.append(correlation_id)
        
        for correlation_id in old_groups:
            del self.correlation_groups[correlation_id]
            if correlation_id in self.correlation_timers:
                self.correlation_timers[correlation_id].cancel()
                del self.correlation_timers[correlation_id]
        
        if old_groups:
            self.logger.debug(f"Cleaned up {len(old_groups)} old correlation groups")


class SecurityAlertingSystem:
    """Main security alerting system"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Alert rules and instances
        self.alert_rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, AlertInstance] = {}
        self.alert_history: deque = deque(maxlen=10000)
        
        # Channel configurations
        self.channel_configs: Dict[AlertChannel, ChannelConfig] = {}
        
        # Correlation engine
        self.correlation_engine = AlertCorrelationEngine()
        
        # Event buffers for rule evaluation
        self.event_buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Background tasks
        self.escalation_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        
        # Rate limiting
        self.channel_rate_limits: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Notification handlers
        self.notification_handlers: Dict[AlertChannel, Callable] = {}
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Initialize default configurations
        self._initialize_default_configs()
        self._register_notification_handlers()
        
        self.logger.info("Security alerting system initialized")
    
    def _initialize_default_configs(self):
        """Initialize default channel configurations"""
        
        # Email configuration
        self.channel_configs[AlertChannel.EMAIL] = ChannelConfig(
            channel=AlertChannel.EMAIL,
            enabled=True,
            config={
                'smtp_server': 'localhost',
                'smtp_port': 587,
                'use_tls': True,
                'username': '',
                'password': '',
                'from_address': 'security-alerts@company.com',
                'to_addresses': ['security-team@company.com']
            },
            severity_filter=[SecurityEventSeverity.HIGH, SecurityEventSeverity.CRITICAL],
            rate_limit_enabled=True,
            max_alerts_per_hour=20
        )
        
        # Slack configuration
        self.channel_configs[AlertChannel.SLACK] = ChannelConfig(
            channel=AlertChannel.SLACK,
            enabled=False,
            config={
                'webhook_url': '',
                'channel': '#security-alerts',
                'username': 'SecurityBot',
                'icon_emoji': ':warning:'
            },
            severity_filter=[SecurityEventSeverity.MEDIUM, SecurityEventSeverity.HIGH, SecurityEventSeverity.CRITICAL]
        )
        
        # Webhook configuration
        self.channel_configs[AlertChannel.WEBHOOK] = ChannelConfig(
            channel=AlertChannel.WEBHOOK,
            enabled=False,
            config={
                'url': '',
                'method': 'POST',
                'headers': {'Content-Type': 'application/json'},
                'timeout_seconds': 30
            }
        )
        
        # Console configuration (for debugging)
        self.channel_configs[AlertChannel.CONSOLE] = ChannelConfig(
            channel=AlertChannel.CONSOLE,
            enabled=True,
            config={},
            severity_filter=list(SecurityEventSeverity)
        )
    
    def _register_notification_handlers(self):
        """Register notification handlers for each channel"""
        
        self.notification_handlers[AlertChannel.EMAIL] = self._send_email_notification
        self.notification_handlers[AlertChannel.SLACK] = self._send_slack_notification
        self.notification_handlers[AlertChannel.WEBHOOK] = self._send_webhook_notification
        self.notification_handlers[AlertChannel.CONSOLE] = self._send_console_notification
        self.notification_handlers[AlertChannel.TEAMS] = self._send_teams_notification
        self.notification_handlers[AlertChannel.PAGERDUTY] = self._send_pagerduty_notification
    
    def add_alert_rule(self, rule: AlertRule):
        """Add an alert rule"""
        with self._lock:
            self.alert_rules[rule.rule_id] = rule
        
        self.logger.info(f"Added alert rule: {rule.name} ({rule.rule_id})")
    
    def remove_alert_rule(self, rule_id: str) -> bool:
        """Remove an alert rule"""
        with self._lock:
            if rule_id in self.alert_rules:
                rule_name = self.alert_rules[rule_id].name
                del self.alert_rules[rule_id]
                self.logger.info(f"Removed alert rule: {rule_name} ({rule_id})")
                return True
        return False
    
    def update_channel_config(self, channel: AlertChannel, config: ChannelConfig):
        """Update channel configuration"""
        with self._lock:
            self.channel_configs[channel] = config
        
        self.logger.info(f"Updated configuration for channel: {channel.value}")
    
    async def process_security_event(self, event: SecurityMetricEvent):
        """Process a security event for alerting"""
        
        # Evaluate against all alert rules
        triggered_rules = []
        
        with self._lock:
            for rule_id, rule in self.alert_rules.items():
                if not rule.enabled:
                    continue
                
                # Check if event matches rule criteria
                if self._evaluate_rule(rule, event):
                    triggered_rules.append(rule)
        
        # Process triggered rules
        for rule in triggered_rules:
            await self._handle_triggered_rule(rule, event)
    
    def _evaluate_rule(self, rule: AlertRule, event: SecurityMetricEvent) -> bool:
        """Evaluate if an event triggers an alert rule"""
        
        # Check metric type
        if rule.metric_type != event.metric_type:
            return False
        
        # Check severity threshold
        severity_levels = {
            SecurityEventSeverity.INFO: 1,
            SecurityEventSeverity.LOW: 2,
            SecurityEventSeverity.MEDIUM: 3,
            SecurityEventSeverity.HIGH: 4,
            SecurityEventSeverity.CRITICAL: 5
        }
        
        if severity_levels.get(event.severity, 0) < severity_levels.get(rule.severity_threshold, 0):
            return False
        
        # Check user filter
        if rule.user_filter and event.user_id not in rule.user_filter:
            return False
        
        # Check resource filter
        if rule.resource_filter and event.resource_id not in rule.resource_filter:
            return False
        
        # Check frequency threshold
        rule_buffer_key = f"{rule.rule_id}:{event.metric_type.value}"
        self.event_buffers[rule_buffer_key].append(event)
        
        # Count events in time window
        cutoff_time = datetime.now() - timedelta(minutes=rule.time_window_minutes)
        recent_events = [
            e for e in self.event_buffers[rule_buffer_key]
            if e.timestamp >= cutoff_time
        ]
        
        return len(recent_events) >= rule.frequency_threshold
    
    async def _handle_triggered_rule(self, rule: AlertRule, triggering_event: SecurityMetricEvent):
        """Handle a triggered alert rule"""
        
        # Get correlation ID
        correlation_id, is_new_group = self.correlation_engine.correlate_event(
            triggering_event, rule.correlation_fields, rule.correlation_window_minutes
        )
        
        # Check if alert already exists for this correlation
        existing_alert = None
        with self._lock:
            for alert in self.active_alerts.values():
                if (alert.rule_id == rule.rule_id and 
                    alert.correlation_id == correlation_id and 
                    alert.status in [AlertStatus.TRIGGERED, AlertStatus.ACKNOWLEDGED]):
                    existing_alert = alert
                    break
        
        if existing_alert:
            # Update existing alert with new event
            existing_alert.triggering_events.append(triggering_event)
            existing_alert.metadata['event_count'] = len(existing_alert.triggering_events)
            self.logger.debug(f"Updated existing alert {existing_alert.alert_id} with new event")
        else:
            # Create new alert
            await self._create_new_alert(rule, triggering_event, correlation_id)
    
    async def _create_new_alert(
        self,
        rule: AlertRule,
        triggering_event: SecurityMetricEvent,
        correlation_id: str
    ):
        """Create a new alert instance"""
        
        alert_id = f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        # Get all correlated events
        correlated_events = self.correlation_engine.get_correlation_group(correlation_id)
        
        # Create alert title and description
        title = f"Security Alert: {rule.name}"
        description = self._generate_alert_description(rule, correlated_events)
        
        alert = AlertInstance(
            alert_id=alert_id,
            rule_id=rule.rule_id,
            correlation_id=correlation_id,
            title=title,
            description=description,
            severity=triggering_event.severity,
            metric_type=triggering_event.metric_type,
            triggering_events=correlated_events,
            user_id=triggering_event.user_id,
            source_ip=triggering_event.source_ip,
            resource_id=triggering_event.resource_id,
            status=AlertStatus.TRIGGERED,
            triggered_at=datetime.now(),
            metadata={
                'rule_name': rule.name,
                'event_count': len(correlated_events),
                'correlation_fields': rule.correlation_fields
            }
        )
        
        with self._lock:
            self.active_alerts[alert_id] = alert
        
        # Send notifications
        await self._send_alert_notifications(alert, rule)
        
        self.logger.info(f"Created new alert: {alert_id} for rule {rule.name}")
    
    def _generate_alert_description(
        self,
        rule: AlertRule,
        events: List[SecurityMetricEvent]
    ) -> str:
        """Generate alert description based on rule and events"""
        
        if not events:
            return rule.description
        
        event_count = len(events)
        latest_event = max(events, key=lambda e: e.timestamp)
        
        description_parts = [
            rule.description,
            f"\nTriggered by {event_count} event(s) in the last {rule.time_window_minutes} minutes.",
            f"Latest event: {latest_event.outcome} on {latest_event.resource_id}"
        ]
        
        if latest_event.user_id:
            description_parts.append(f"User: {latest_event.user_id}")
        
        if latest_event.source_ip:
            description_parts.append(f"Source IP: {latest_event.source_ip}")
        
        return "\n".join(description_parts)
    
    async def _send_alert_notifications(self, alert: AlertInstance, rule: AlertRule):
        """Send alert notifications through configured channels"""
        
        notification_tasks = []
        
        for channel in rule.channels:
            channel_config = self.channel_configs.get(channel)
            if not channel_config or not channel_config.enabled:
                continue
            
            # Check severity filter
            if (channel_config.severity_filter and 
                alert.severity not in channel_config.severity_filter):
                continue
            
            # Check rate limiting
            if not self._check_rate_limit(channel_config, alert):
                self.logger.warning(f"Rate limit exceeded for channel {channel.value}")
                continue
            
            # Send notification
            notification_task = asyncio.create_task(
                self._send_notification(channel, channel_config, alert)
            )
            notification_tasks.append(notification_task)
        
        # Wait for all notifications to complete
        if notification_tasks:
            await asyncio.gather(*notification_tasks, return_exceptions=True)
    
    def _check_rate_limit(self, channel_config: ChannelConfig, alert: AlertInstance) -> bool:
        """Check if channel rate limit allows sending notification"""
        
        if not channel_config.rate_limit_enabled:
            return True
        
        channel_key = f"{channel_config.channel.value}"
        now = datetime.now()
        
        # Clean old entries
        cutoff_time = now - timedelta(hours=1)
        self.channel_rate_limits[channel_key] = deque(
            [timestamp for timestamp in self.channel_rate_limits[channel_key] 
             if timestamp >= cutoff_time],
            maxlen=100
        )
        
        # Check limit
        if len(self.channel_rate_limits[channel_key]) >= channel_config.max_alerts_per_hour:
            return False
        
        # Record this notification
        self.channel_rate_limits[channel_key].append(now)
        return True
    
    async def _send_notification(
        self,
        channel: AlertChannel,
        channel_config: ChannelConfig,
        alert: AlertInstance
    ):
        """Send notification to a specific channel"""
        
        handler = self.notification_handlers.get(channel)
        if not handler:
            self.logger.error(f"No handler registered for channel {channel.value}")
            return
        
        max_retries = channel_config.max_retries
        retry_delay = channel_config.retry_delay_seconds
        
        for attempt in range(max_retries + 1):
            try:
                success = await handler(channel_config, alert)
                if success:
                    # Record successful notification
                    alert.notifications_sent.append({
                        'channel': channel.value,
                        'sent_at': datetime.now().isoformat(),
                        'attempt': attempt + 1
                    })
                    return
                
            except Exception as e:
                self.logger.error(f"Notification failed for {channel.value}: {e}")
                
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
                else:
                    # Record failed notification
                    alert.notifications_sent.append({
                        'channel': channel.value,
                        'sent_at': datetime.now().isoformat(),
                        'attempt': attempt + 1,
                        'error': str(e),
                        'success': False
                    })
    
    # Notification handlers
    
    async def _send_email_notification(
        self,
        channel_config: ChannelConfig,
        alert: AlertInstance
    ) -> bool:
        """Send email notification"""
        
        try:
            config = channel_config.config
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = config['from_address']
            msg['To'] = ', '.join(config['to_addresses'])
            msg['Subject'] = f"🚨 {alert.title} - {alert.severity.value.upper()}"
            
            # Email body
            body = f"""
Security Alert Details:

Alert ID: {alert.alert_id}
Severity: {alert.severity.value.upper()}
Status: {alert.status.value}
Triggered At: {alert.triggered_at}

Description:
{alert.description}

Event Details:
- Metric Type: {alert.metric_type.value}
- User: {alert.user_id or 'Unknown'}
- Source IP: {alert.source_ip or 'Unknown'}
- Resource: {alert.resource_id}
- Event Count: {len(alert.triggering_events)}

Correlation ID: {alert.correlation_id}

Please investigate and take appropriate action.

This is an automated security alert from the monitoring system.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            if config.get('use_tls'):
                server.starttls()
            if config.get('username') and config.get('password'):
                server.login(config['username'], config['password'])
            
            server.send_message(msg)
            server.quit()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")
            return False
    
    async def _send_slack_notification(
        self,
        channel_config: ChannelConfig,
        alert: AlertInstance
    ) -> bool:
        """Send Slack notification"""
        
        try:
            config = channel_config.config
            webhook_url = config['webhook_url']
            
            if not webhook_url:
                self.logger.error("Slack webhook URL not configured")
                return False
            
            # Severity colors
            color_map = {
                SecurityEventSeverity.INFO: "#36a64f",
                SecurityEventSeverity.LOW: "#ffeb3b", 
                SecurityEventSeverity.MEDIUM: "#ff9800",
                SecurityEventSeverity.HIGH: "#f44336",
                SecurityEventSeverity.CRITICAL: "#9c27b0"
            }
            
            # Create Slack message
            slack_message = {
                "channel": config.get('channel', '#security-alerts'),
                "username": config.get('username', 'SecurityBot'),
                "icon_emoji": config.get('icon_emoji', ':warning:'),
                "attachments": [
                    {
                        "fallback": f"Security Alert: {alert.title}",
                        "color": color_map.get(alert.severity, "#ff0000"),
                        "title": alert.title,
                        "title_link": f"#alert-{alert.alert_id}",
                        "text": alert.description[:500] + "..." if len(alert.description) > 500 else alert.description,
                        "fields": [
                            {
                                "title": "Severity",
                                "value": alert.severity.value.upper(),
                                "short": True
                            },
                            {
                                "title": "Status",
                                "value": alert.status.value,
                                "short": True
                            },
                            {
                                "title": "User",
                                "value": alert.user_id or "Unknown",
                                "short": True
                            },
                            {
                                "title": "Source IP",
                                "value": alert.source_ip or "Unknown",
                                "short": True
                            },
                            {
                                "title": "Resource",
                                "value": alert.resource_id,
                                "short": False
                            }
                        ],
                        "footer": "Security Monitoring System",
                        "ts": int(alert.triggered_at.timestamp())
                    }
                ]
            }
            
            # Send to Slack
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=slack_message,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        return True
                    else:
                        self.logger.error(f"Slack API returned status {response.status}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    async def _send_webhook_notification(
        self,
        channel_config: ChannelConfig,
        alert: AlertInstance
    ) -> bool:
        """Send webhook notification"""
        
        try:
            config = channel_config.config
            url = config['url']
            method = config.get('method', 'POST').upper()
            headers = config.get('headers', {})
            timeout = config.get('timeout_seconds', 30)
            
            # Prepare payload
            payload = {
                'alert_id': alert.alert_id,
                'rule_id': alert.rule_id,
                'correlation_id': alert.correlation_id,
                'title': alert.title,
                'description': alert.description,
                'severity': alert.severity.value,
                'status': alert.status.value,
                'metric_type': alert.metric_type.value,
                'user_id': alert.user_id,
                'source_ip': alert.source_ip,
                'resource_id': alert.resource_id,
                'triggered_at': alert.triggered_at.isoformat(),
                'event_count': len(alert.triggering_events),
                'metadata': alert.metadata
            }
            
            # Send webhook
            async with aiohttp.ClientSession() as session:
                if method == 'POST':
                    async with session.post(
                        url,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=timeout)
                    ) as response:
                        return response.status < 400
                elif method == 'PUT':
                    async with session.put(
                        url,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=timeout)
                    ) as response:
                        return response.status < 400
                        
        except Exception as e:
            self.logger.error(f"Failed to send webhook notification: {e}")
            return False
    
    async def _send_console_notification(
        self,
        channel_config: ChannelConfig,
        alert: AlertInstance
    ) -> bool:
        """Send console notification (for debugging)"""
        
        try:
            console_message = f"""
{'='*60}
🚨 SECURITY ALERT 🚨
{'='*60}
Alert ID: {alert.alert_id}
Title: {alert.title}
Severity: {alert.severity.value.upper()}
Status: {alert.status.value}
Triggered: {alert.triggered_at}

{alert.description}

User: {alert.user_id or 'Unknown'}
Source IP: {alert.source_ip or 'Unknown'}
Resource: {alert.resource_id}
Events: {len(alert.triggering_events)}
{'='*60}
            """
            
            self.logger.info(console_message)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send console notification: {e}")
            return False
    
    async def _send_teams_notification(
        self,
        channel_config: ChannelConfig,
        alert: AlertInstance
    ) -> bool:
        """Send Microsoft Teams notification"""
        
        try:
            config = channel_config.config
            webhook_url = config['webhook_url']
            
            # Create Teams message card
            teams_message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "FF0000" if alert.severity == SecurityEventSeverity.CRITICAL else "FFA500",
                "summary": alert.title,
                "sections": [
                    {
                        "activityTitle": alert.title,
                        "activitySubtitle": f"Severity: {alert.severity.value.upper()}",
                        "activityImage": "https://example.com/security-icon.png",
                        "facts": [
                            {"name": "Alert ID", "value": alert.alert_id},
                            {"name": "User", "value": alert.user_id or "Unknown"},
                            {"name": "Source IP", "value": alert.source_ip or "Unknown"},
                            {"name": "Resource", "value": alert.resource_id},
                            {"name": "Triggered", "value": alert.triggered_at.strftime('%Y-%m-%d %H:%M:%S')},
                            {"name": "Event Count", "value": str(len(alert.triggering_events))}
                        ],
                        "markdown": True
                    }
                ],
                "potentialAction": [
                    {
                        "@type": "ActionCard",
                        "name": "Acknowledge Alert",
                        "inputs": [
                            {
                                "@type": "TextInput",
                                "id": "comment",
                                "title": "Comment",
                                "isMultiline": False
                            }
                        ],
                        "actions": [
                            {
                                "@type": "HttpPOST",
                                "name": "Acknowledge",
                                "target": f"https://security-system.com/api/alerts/{alert.alert_id}/acknowledge"
                            }
                        ]
                    }
                ]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=teams_message,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    return response.status == 200
                    
        except Exception as e:
            self.logger.error(f"Failed to send Teams notification: {e}")
            return False
    
    async def _send_pagerduty_notification(
        self,
        channel_config: ChannelConfig,
        alert: AlertInstance
    ) -> bool:
        """Send PagerDuty notification"""
        
        try:
            config = channel_config.config
            integration_key = config['integration_key']
            
            # Create PagerDuty event
            pagerduty_event = {
                "routing_key": integration_key,
                "event_action": "trigger",
                "dedup_key": alert.correlation_id,
                "payload": {
                    "summary": alert.title,
                    "source": "Security Monitoring System",
                    "severity": "critical" if alert.severity == SecurityEventSeverity.CRITICAL else "error",
                    "component": "security",
                    "group": "alerts",
                    "class": alert.metric_type.value,
                    "custom_details": {
                        "alert_id": alert.alert_id,
                        "user_id": alert.user_id,
                        "source_ip": alert.source_ip,
                        "resource_id": alert.resource_id,
                        "event_count": len(alert.triggering_events),
                        "description": alert.description
                    }
                }
            }
            
            # Send to PagerDuty Events API
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://events.pagerduty.com/v2/enqueue",
                    json=pagerduty_event,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    return response.status == 202
                    
        except Exception as e:
            self.logger.error(f"Failed to send PagerDuty notification: {e}")
            return False
    
    # Alert management methods
    
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str, notes: str = "") -> bool:
        """Acknowledge an active alert"""
        
        with self._lock:
            alert = self.active_alerts.get(alert_id)
            if not alert or alert.status != AlertStatus.TRIGGERED:
                return False
            
            alert.status = AlertStatus.ACKNOWLEDGED
            alert.acknowledged_at = datetime.now()
            alert.acknowledged_by = acknowledged_by
            alert.metadata['acknowledgment_notes'] = notes
        
        self.logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
        return True
    
    async def resolve_alert(self, alert_id: str, resolved_by: str, resolution_notes: str = "") -> bool:
        """Resolve an active alert"""
        
        with self._lock:
            alert = self.active_alerts.get(alert_id)
            if not alert:
                return False
            
            alert.status = AlertStatus.RESOLVED
            alert.resolved_at = datetime.now()
            alert.resolved_by = resolved_by
            alert.metadata['resolution_notes'] = resolution_notes
            
            # Move to history
            self.alert_history.append(alert)
            del self.active_alerts[alert_id]
        
        self.logger.info(f"Alert {alert_id} resolved by {resolved_by}")
        return True
    
    def get_active_alerts(
        self,
        severity_filter: Optional[List[SecurityEventSeverity]] = None,
        status_filter: Optional[List[AlertStatus]] = None
    ) -> List[AlertInstance]:
        """Get active alerts with optional filtering"""
        
        with self._lock:
            alerts = list(self.active_alerts.values())
        
        if severity_filter:
            alerts = [a for a in alerts if a.severity in severity_filter]
        
        if status_filter:
            alerts = [a for a in alerts if a.status in status_filter]
        
        return sorted(alerts, key=lambda x: x.triggered_at, reverse=True)
    
    def get_alert_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get alerting system statistics"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self._lock:
            # Active alerts
            active_count = len(self.active_alerts)
            
            # Historical alerts in time window
            recent_alerts = [
                alert for alert in self.alert_history
                if alert.triggered_at >= cutoff_time
            ]
        
        # Statistics
        total_recent = len(recent_alerts)
        severity_breakdown = defaultdict(int)
        status_breakdown = defaultdict(int)
        
        for alert in recent_alerts + list(self.active_alerts.values()):
            if alert.triggered_at >= cutoff_time:
                severity_breakdown[alert.severity.value] += 1
                status_breakdown[alert.status.value] += 1
        
        return {
            'time_window_hours': hours,
            'active_alerts': active_count,
            'recent_alerts': total_recent,
            'severity_breakdown': dict(severity_breakdown),
            'status_breakdown': dict(status_breakdown),
            'avg_resolution_time_minutes': self._calculate_avg_resolution_time(recent_alerts),
            'escalation_rate': len([a for a in recent_alerts if a.escalation_history]) / max(total_recent, 1),
            'notification_success_rate': self._calculate_notification_success_rate(recent_alerts)
        }
    
    def _calculate_avg_resolution_time(self, alerts: List[AlertInstance]) -> float:
        """Calculate average resolution time"""
        resolved_alerts = [a for a in alerts if a.resolved_at]
        
        if not resolved_alerts:
            return 0.0
        
        total_time = sum(
            (alert.resolved_at - alert.triggered_at).total_seconds() / 60
            for alert in resolved_alerts
        )
        
        return total_time / len(resolved_alerts)
    
    def _calculate_notification_success_rate(self, alerts: List[AlertInstance]) -> float:
        """Calculate notification success rate"""
        total_notifications = 0
        successful_notifications = 0
        
        for alert in alerts:
            for notification in alert.notifications_sent:
                total_notifications += 1
                if notification.get('success', True):
                    successful_notifications += 1
        
        return successful_notifications / max(total_notifications, 1)
    
    async def start_background_tasks(self):
        """Start background tasks"""
        self.escalation_task = asyncio.create_task(self._escalation_loop())
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        self.logger.info("Security alerting background tasks started")
    
    async def stop_background_tasks(self):
        """Stop background tasks"""
        if self.escalation_task:
            self.escalation_task.cancel()
        if self.cleanup_task:
            self.cleanup_task.cancel()
        self.logger.info("Security alerting background tasks stopped")
    
    async def _escalation_loop(self):
        """Background loop for alert escalation"""
        while True:
            try:
                current_time = datetime.now()
                
                with self._lock:
                    alerts_to_escalate = []
                    
                    for alert in self.active_alerts.values():
                        rule = self.alert_rules.get(alert.rule_id)
                        if not rule or not rule.escalation_enabled:
                            continue
                        
                        if alert.status != AlertStatus.TRIGGERED:
                            continue
                        
                        # Check if escalation is due
                        time_since_trigger = (current_time - alert.triggered_at).total_seconds() / 60
                        if time_since_trigger >= rule.escalation_delay_minutes:
                            alerts_to_escalate.append((alert, rule))
                
                # Process escalations
                for alert, rule in alerts_to_escalate:
                    await self._escalate_alert(alert, rule)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Escalation loop error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _escalate_alert(self, alert: AlertInstance, rule: AlertRule):
        """Escalate an alert to the next level"""
        
        # Determine next escalation level
        current_level = alert.current_escalation_level
        escalation_levels = list(EscalationLevel)
        current_index = escalation_levels.index(current_level)
        
        if current_index < len(escalation_levels) - 1:
            next_level = escalation_levels[current_index + 1]
            
            # Check if we've reached max escalation level
            max_level_index = escalation_levels.index(rule.max_escalation_level)
            if escalation_levels.index(next_level) > max_level_index:
                return
            
            # Update alert
            alert.current_escalation_level = next_level
            alert.escalated_at = datetime.now()
            alert.status = AlertStatus.ESCALATED
            
            # Record escalation history
            alert.escalation_history.append({
                'from_level': current_level.value,
                'to_level': next_level.value,
                'escalated_at': datetime.now().isoformat(),
                'reason': 'timeout'
            })
            
            # Send escalation notifications
            await self._send_escalation_notifications(alert, rule, next_level)
            
            self.logger.warning(f"Alert {alert.alert_id} escalated to {next_level.value}")
    
    async def _send_escalation_notifications(
        self,
        alert: AlertInstance,
        rule: AlertRule,
        escalation_level: EscalationLevel
    ):
        """Send notifications for alert escalation"""
        
        # Update alert title to indicate escalation
        original_title = alert.title
        alert.title = f"[ESCALATED - {escalation_level.value.upper()}] {original_title}"
        
        # Send notifications through configured channels
        await self._send_alert_notifications(alert, rule)
        
        # Restore original title
        alert.title = original_title
    
    async def _cleanup_loop(self):
        """Background loop for cleanup tasks"""
        while True:
            try:
                # Clean up old correlation groups
                self.correlation_engine.cleanup_old_correlations()
                
                # Clean up old event buffers
                cutoff_time = datetime.now() - timedelta(hours=24)
                for buffer_key in list(self.event_buffers.keys()):
                    self.event_buffers[buffer_key] = deque(
                        [event for event in self.event_buffers[buffer_key] 
                         if event.timestamp >= cutoff_time],
                        maxlen=1000
                    )
                
                # Clean up old rate limit data
                for channel_key in list(self.channel_rate_limits.keys()):
                    cutoff_time_rate = datetime.now() - timedelta(hours=1)
                    self.channel_rate_limits[channel_key] = deque(
                        [timestamp for timestamp in self.channel_rate_limits[channel_key]
                         if timestamp >= cutoff_time_rate],
                        maxlen=100
                    )
                
                await asyncio.sleep(3600)  # Run cleanup every hour
                
            except Exception as e:
                self.logger.error(f"Cleanup loop error: {e}")
                await asyncio.sleep(1800)  # Wait 30 minutes on error


# Global security alerting system instance
_security_alerting_system: Optional[SecurityAlertingSystem] = None

def get_security_alerting_system() -> SecurityAlertingSystem:
    """Get global security alerting system instance"""
    global _security_alerting_system
    if _security_alerting_system is None:
        _security_alerting_system = SecurityAlertingSystem()
    return _security_alerting_system


# Convenience functions for common alert rules
def create_threat_detection_rule(
    rule_id: str,
    name: str,
    severity_threshold: SecurityEventSeverity = SecurityEventSeverity.HIGH,
    frequency_threshold: int = 5,
    time_window_minutes: int = 15,
    channels: List[AlertChannel] = None
) -> AlertRule:
    """Create a threat detection alert rule"""
    
    return AlertRule(
        rule_id=rule_id,
        name=name,
        description=f"Alert triggered when {frequency_threshold} or more threat detection events occur within {time_window_minutes} minutes",
        enabled=True,
        metric_type=SecurityMetricType.THREAT_DETECTION,
        severity_threshold=severity_threshold,
        frequency_threshold=frequency_threshold,
        time_window_minutes=time_window_minutes,
        channels=channels or [AlertChannel.EMAIL, AlertChannel.CONSOLE],
        escalation_enabled=True,
        escalation_delay_minutes=30,
        max_escalation_level=EscalationLevel.LEVEL_3,
        correlation_fields=["user_id", "source_ip", "resource_id"],
        auto_response_enabled=False
    )


def create_dlp_violation_rule(
    rule_id: str,
    name: str,
    severity_threshold: SecurityEventSeverity = SecurityEventSeverity.MEDIUM,
    frequency_threshold: int = 3,
    channels: List[AlertChannel] = None
) -> AlertRule:
    """Create a DLP violation alert rule"""
    
    return AlertRule(
        rule_id=rule_id,
        name=name,
        description=f"Alert triggered when {frequency_threshold} or more DLP violations occur",
        enabled=True,
        metric_type=SecurityMetricType.DLP_VIOLATION,
        severity_threshold=severity_threshold,
        frequency_threshold=frequency_threshold,
        time_window_minutes=30,
        channels=channels or [AlertChannel.EMAIL, AlertChannel.SLACK],
        escalation_enabled=True,
        correlation_fields=["user_id", "resource_id"]
    )


def create_compliance_violation_rule(
    rule_id: str,
    name: str,
    channels: List[AlertChannel] = None
) -> AlertRule:
    """Create a compliance violation alert rule"""
    
    return AlertRule(
        rule_id=rule_id,
        name=name,
        description="Alert triggered when compliance violations are detected",
        enabled=True,
        metric_type=SecurityMetricType.COMPLIANCE_CHECK,
        severity_threshold=SecurityEventSeverity.MEDIUM,
        frequency_threshold=1,  # Alert on any compliance violation
        time_window_minutes=60,
        channels=channels or [AlertChannel.EMAIL, AlertChannel.TEAMS],
        escalation_enabled=True,
        escalation_delay_minutes=120,  # 2 hours for compliance issues
        correlation_fields=["resource_id"]
    )