"""
Alerting System
Provides comprehensive alerting capabilities for monitoring events.
"""
from __future__ import annotations

import asyncio
import smtplib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any

import aiohttp

from core.logging import get_logger
from core.monitoring.health_checks import HealthStatus

logger = get_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(Enum):
    """Alert lifecycle status."""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


@dataclass
class Alert:
    """Alert data structure."""
    id: str
    title: str
    description: str
    severity: AlertSeverity
    source: str
    timestamp: datetime
    status: AlertStatus = AlertStatus.ACTIVE
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    resolved_at: datetime | None = None
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "severity": self.severity.value,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status.value,
            "labels": self.labels,
            "annotations": self.annotations,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "acknowledged_by": self.acknowledged_by
        }


@dataclass
class AlertRule:
    """Alert rule configuration."""
    id: str
    name: str
    condition: Callable[[dict[str, Any]], bool]
    severity: AlertSeverity
    description: str
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    cooldown: timedelta = timedelta(minutes=15)
    auto_resolve: bool = True
    resolve_condition: Callable[[dict[str, Any]], bool] | None = None

    # State tracking
    last_fired: datetime | None = None
    last_resolved: datetime | None = None


class BaseAlertChannel:
    """Base class for alert notification channels."""

    def __init__(self, name: str, enabled: bool = True):
        self.name = name
        self.enabled = enabled

    async def send_alert(self, alert: Alert) -> bool:
        """Send alert notification. Override in subclasses."""
        raise NotImplementedError

    def format_alert_message(self, alert: Alert) -> str:
        """Format alert message for this channel."""
        severity_emoji = {
            AlertSeverity.INFO: "ℹ️",
            AlertSeverity.WARNING: "⚠️",
            AlertSeverity.ERROR: "❌",
            AlertSeverity.CRITICAL: "🚨"
        }

        emoji = severity_emoji.get(alert.severity, "📢")

        message = f"{emoji} **{alert.severity.value.upper()}** - {alert.title}\n"
        message += f"📝 {alert.description}\n"
        message += f"🕒 {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        message += f"📍 Source: {alert.source}\n"

        if alert.labels:
            message += f"🏷️ Labels: {', '.join(f'{k}={v}' for k, v in alert.labels.items())}\n"

        return message


class EmailAlertChannel(BaseAlertChannel):
    """Email notification channel."""

    def __init__(self, name: str, smtp_host: str, smtp_port: int,
                 username: str, password: str, from_email: str,
                 to_emails: list[str], use_tls: bool = True):
        super().__init__(name)
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_email = from_email
        self.to_emails = to_emails
        self.use_tls = use_tls

    async def send_alert(self, alert: Alert) -> bool:
        """Send alert via email."""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.title}"

            # Create HTML and text versions
            text_body = self.format_alert_message(alert)
            html_body = self._format_html_alert(alert)

            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))

            # Send email
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            if self.use_tls:
                server.starttls()
            server.login(self.username, self.password)

            text = msg.as_string()
            server.sendmail(self.from_email, self.to_emails, text)
            server.quit()

            logger.info(f"Alert sent via email: {alert.id}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert {alert.id}: {e}")
            return False

    def _format_html_alert(self, alert: Alert) -> str:
        """Format alert as HTML."""
        severity_colors = {
            AlertSeverity.INFO: "#2196F3",
            AlertSeverity.WARNING: "#FF9800",
            AlertSeverity.ERROR: "#F44336",
            AlertSeverity.CRITICAL: "#9C27B0"
        }

        color = severity_colors.get(alert.severity, "#757575")

        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .alert-header {{ background-color: {color}; color: white; padding: 15px; border-radius: 5px; }}
                .alert-body {{ padding: 15px; background-color: #f5f5f5; border-radius: 5px; margin-top: 10px; }}
                .alert-meta {{ margin-top: 10px; font-size: 0.9em; color: #666; }}
                .labels {{ margin-top: 10px; }}
                .label {{ display: inline-block; background-color: #e0e0e0; padding: 2px 6px; border-radius: 3px; margin-right: 5px; }}
            </style>
        </head>
        <body>
            <div class="alert-header">
                <h2>{alert.severity.value.upper()} - {alert.title}</h2>
            </div>
            <div class="alert-body">
                <p><strong>Description:</strong> {alert.description}</p>
                <div class="alert-meta">
                    <p><strong>Source:</strong> {alert.source}</p>
                    <p><strong>Timestamp:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
                    <p><strong>Alert ID:</strong> {alert.id}</p>
                </div>
        """

        if alert.labels:
            html += '<div class="labels"><strong>Labels:</strong><br>'
            for key, value in alert.labels.items():
                html += f'<span class="label">{key}={value}</span>'
            html += '</div>'

        html += """
            </div>
        </body>
        </html>
        """

        return html


class WebhookAlertChannel(BaseAlertChannel):
    """Webhook notification channel."""

    def __init__(self, name: str, webhook_url: str, headers: dict[str, str] | None = None,
                 timeout: float = 10.0):
        super().__init__(name)
        self.webhook_url = webhook_url
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout = timeout

    async def send_alert(self, alert: Alert) -> bool:
        """Send alert via webhook."""
        try:
            payload = {
                "alert": alert.to_dict(),
                "message": self.format_alert_message(alert),
                "timestamp": datetime.utcnow().isoformat()
            }

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.post(self.webhook_url,
                                      json=payload,
                                      headers=self.headers) as response:
                    if response.status < 300:
                        logger.info(f"Alert sent via webhook: {alert.id}")
                        return True
                    else:
                        logger.error(f"Webhook returned status {response.status} for alert {alert.id}")
                        return False

        except Exception as e:
            logger.error(f"Failed to send webhook alert {alert.id}: {e}")
            return False


class SlackAlertChannel(WebhookAlertChannel):
    """Slack notification channel using webhooks."""

    def __init__(self, name: str, webhook_url: str):
        super().__init__(name, webhook_url)

    def format_alert_message(self, alert: Alert) -> dict[str, Any]:
        """Format alert for Slack."""
        severity_colors = {
            AlertSeverity.INFO: "#36a64f",
            AlertSeverity.WARNING: "#ff9900",
            AlertSeverity.ERROR: "#ff0000",
            AlertSeverity.CRITICAL: "#8b0000"
        }

        color = severity_colors.get(alert.severity, "#000000")

        fields = [
            {"title": "Source", "value": alert.source, "short": True},
            {"title": "Timestamp", "value": alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC'), "short": True}
        ]

        if alert.labels:
            labels_text = ", ".join(f"{k}={v}" for k, v in alert.labels.items())
            fields.append({"title": "Labels", "value": labels_text, "short": False})

        return {
            "attachments": [
                {
                    "color": color,
                    "title": f"{alert.severity.value.upper()} - {alert.title}",
                    "text": alert.description,
                    "fields": fields,
                    "footer": f"Alert ID: {alert.id}",
                    "ts": int(alert.timestamp.timestamp())
                }
            ]
        }

    async def send_alert(self, alert: Alert) -> bool:
        """Send alert to Slack."""
        try:
            payload = self.format_alert_message(alert)

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.post(self.webhook_url, json=payload) as response:
                    if response.status < 300:
                        logger.info(f"Alert sent to Slack: {alert.id}")
                        return True
                    else:
                        logger.error(f"Slack webhook returned status {response.status} for alert {alert.id}")
                        return False

        except Exception as e:
            logger.error(f"Failed to send Slack alert {alert.id}: {e}")
            return False


class LogAlertChannel(BaseAlertChannel):
    """Log file notification channel."""

    def __init__(self, name: str, log_file_path: str | None = None):
        super().__init__(name)
        self.log_file_path = log_file_path
        self.logger = get_logger(f"alerts.{name}")

    async def send_alert(self, alert: Alert) -> bool:
        """Log alert message."""
        try:
            message = self.format_alert_message(alert)

            if alert.severity == AlertSeverity.CRITICAL:
                self.logger.critical(message)
            elif alert.severity == AlertSeverity.ERROR:
                self.logger.error(message)
            elif alert.severity == AlertSeverity.WARNING:
                self.logger.warning(message)
            else:
                self.logger.info(message)

            return True

        except Exception as e:
            logger.error(f"Failed to log alert {alert.id}: {e}")
            return False


class AlertManager:
    """Central alert management system."""

    def __init__(self):
        self.rules: dict[str, AlertRule] = {}
        self.channels: dict[str, BaseAlertChannel] = {}
        self.active_alerts: dict[str, Alert] = {}
        self.alert_history: list[Alert] = []
        self.channel_routing: dict[AlertSeverity, list[str]] = {
            AlertSeverity.INFO: [],
            AlertSeverity.WARNING: [],
            AlertSeverity.ERROR: [],
            AlertSeverity.CRITICAL: []
        }
        self.global_suppression: bool = False
        self.suppressed_rules: set[str] = set()

    def add_rule(self, rule: AlertRule):
        """Add alert rule."""
        self.rules[rule.id] = rule
        logger.info(f"Added alert rule: {rule.name}")

    def remove_rule(self, rule_id: str):
        """Remove alert rule."""
        if rule_id in self.rules:
            del self.rules[rule_id]
            logger.info(f"Removed alert rule: {rule_id}")

    def add_channel(self, channel: BaseAlertChannel):
        """Add notification channel."""
        self.channels[channel.name] = channel
        logger.info(f"Added alert channel: {channel.name}")

    def remove_channel(self, channel_name: str):
        """Remove notification channel."""
        if channel_name in self.channels:
            del self.channels[channel_name]
            logger.info(f"Removed alert channel: {channel_name}")

    def set_channel_routing(self, severity: AlertSeverity, channel_names: list[str]):
        """Set which channels receive alerts of specific severity."""
        self.channel_routing[severity] = channel_names
        logger.info(f"Set routing for {severity.value}: {channel_names}")

    def suppress_global(self, suppress: bool = True):
        """Enable/disable global alert suppression."""
        self.global_suppression = suppress
        logger.info(f"Global suppression: {'enabled' if suppress else 'disabled'}")

    def suppress_rule(self, rule_id: str, suppress: bool = True):
        """Suppress specific rule."""
        if suppress:
            self.suppressed_rules.add(rule_id)
            logger.info(f"Suppressed rule: {rule_id}")
        else:
            self.suppressed_rules.discard(rule_id)
            logger.info(f"Unsuppressed rule: {rule_id}")

    async def evaluate_rules(self, context: dict[str, Any]):
        """Evaluate all alert rules against current context."""
        current_time = datetime.utcnow()

        for rule_id, rule in self.rules.items():
            # Skip suppressed rules
            if self.global_suppression or rule_id in self.suppressed_rules:
                continue

            try:
                # Check cooldown period
                if (rule.last_fired and
                    current_time - rule.last_fired < rule.cooldown):
                    continue

                # Evaluate condition
                if rule.condition(context):
                    # Rule fired - create alert
                    alert = Alert(
                        id=f"{rule_id}_{int(current_time.timestamp())}",
                        title=rule.name,
                        description=rule.description,
                        severity=rule.severity,
                        source=rule_id,
                        timestamp=current_time,
                        labels=rule.labels.copy(),
                        annotations=rule.annotations.copy()
                    )

                    rule.last_fired = current_time
                    await self._fire_alert(alert)

                elif rule.auto_resolve and rule.resolve_condition:
                    # Check if we should resolve existing alert
                    if rule.resolve_condition(context):
                        await self._resolve_alerts_for_rule(rule_id)
                        rule.last_resolved = current_time

            except Exception as e:
                logger.error(f"Error evaluating rule {rule_id}: {e}")

    async def _fire_alert(self, alert: Alert):
        """Fire an alert and send notifications."""
        # Store alert
        self.active_alerts[alert.id] = alert
        self.alert_history.append(alert)

        # Keep history manageable
        if len(self.alert_history) > 10000:
            self.alert_history = self.alert_history[-5000:]

        logger.info(f"Alert fired: {alert.id} - {alert.title}")

        # Send notifications
        await self._send_notifications(alert)

    async def _send_notifications(self, alert: Alert):
        """Send alert to appropriate channels."""
        channel_names = self.channel_routing.get(alert.severity, [])

        if not channel_names:
            logger.warning(f"No channels configured for severity {alert.severity.value}")
            return

        # Send to each channel
        tasks = []
        for channel_name in channel_names:
            if channel_name in self.channels:
                channel = self.channels[channel_name]
                if channel.enabled:
                    tasks.append(channel.send_alert(alert))
                else:
                    logger.debug(f"Channel {channel_name} is disabled")
            else:
                logger.warning(f"Channel {channel_name} not found")

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)
            logger.info(f"Alert {alert.id} sent to {success_count}/{len(tasks)} channels")

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an active alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.status = AlertStatus.ACKNOWLEDGED
            alert.acknowledged_at = datetime.utcnow()
            alert.acknowledged_by = acknowledged_by
            logger.info(f"Alert acknowledged: {alert_id} by {acknowledged_by}")
            return True
        return False

    async def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.status = AlertStatus.RESOLVED
            alert.resolved_at = datetime.utcnow()
            del self.active_alerts[alert_id]
            logger.info(f"Alert resolved: {alert_id}")
            return True
        return False

    async def _resolve_alerts_for_rule(self, rule_id: str):
        """Resolve all active alerts for a specific rule."""
        alerts_to_resolve = [
            alert for alert in self.active_alerts.values()
            if alert.source == rule_id
        ]

        for alert in alerts_to_resolve:
            await self.resolve_alert(alert.id)

    def get_active_alerts(self) -> list[Alert]:
        """Get all active alerts."""
        return list(self.active_alerts.values())

    def get_alert_history(self, limit: int = 100) -> list[Alert]:
        """Get alert history."""
        return self.alert_history[-limit:]

    def get_alerts_by_severity(self, severity: AlertSeverity) -> list[Alert]:
        """Get active alerts by severity."""
        return [alert for alert in self.active_alerts.values()
                if alert.severity == severity]

    def get_alert_summary(self) -> dict[str, Any]:
        """Get summary of alert system status."""
        active_by_severity = {}
        for severity in AlertSeverity:
            count = len(self.get_alerts_by_severity(severity))
            active_by_severity[severity.value] = count

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_active_alerts": len(self.active_alerts),
            "active_by_severity": active_by_severity,
            "total_rules": len(self.rules),
            "suppressed_rules": len(self.suppressed_rules),
            "total_channels": len(self.channels),
            "enabled_channels": len([c for c in self.channels.values() if c.enabled]),
            "global_suppression": self.global_suppression,
            "total_history": len(self.alert_history)
        }


# Pre-defined alert rules for common scenarios

def create_health_check_rule(component_name: str, severity: AlertSeverity = AlertSeverity.ERROR) -> AlertRule:
    """Create alert rule for health check failures."""
    return AlertRule(
        id=f"health_check_{component_name}",
        name=f"{component_name} Health Check Failed",
        condition=lambda ctx: (
            ctx.get("health_checks", {}).get(component_name, {}).get("status") == HealthStatus.UNHEALTHY.value
        ),
        severity=severity,
        description=f"Health check for {component_name} is failing",
        labels={"component": component_name, "type": "health_check"},
        cooldown=timedelta(minutes=5)
    )


def create_etl_failure_rule(job_name: str, failure_threshold: float = 0.1) -> AlertRule:
    """Create alert rule for ETL job failures."""
    return AlertRule(
        id=f"etl_failure_{job_name}",
        name=f"ETL Job {job_name} High Failure Rate",
        condition=lambda ctx: (
            ctx.get("etl_jobs", {}).get(job_name, {}).get("success_rate", 1.0) < (1.0 - failure_threshold)
        ),
        severity=AlertSeverity.WARNING,
        description=f"ETL job {job_name} has high failure rate (>{failure_threshold*100}%)",
        labels={"job": job_name, "type": "etl_failure"},
        cooldown=timedelta(minutes=10)
    )


def create_system_resource_rule(resource_type: str, threshold: float,
                              severity: AlertSeverity = AlertSeverity.WARNING) -> AlertRule:
    """Create alert rule for system resource usage."""
    return AlertRule(
        id=f"system_{resource_type}_high",
        name=f"High {resource_type.title()} Usage",
        condition=lambda ctx: (
            ctx.get("system_resources", {}).get(f"{resource_type}_percent", 0) > threshold
        ),
        severity=severity,
        description=f"System {resource_type} usage is above {threshold}%",
        labels={"resource": resource_type, "type": "system_resource"},
        cooldown=timedelta(minutes=5)
    )


# Global alert manager instance
alert_manager = AlertManager()
