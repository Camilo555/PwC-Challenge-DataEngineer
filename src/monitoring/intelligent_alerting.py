"""
Intelligent Alerting System for Messaging Infrastructure
ML-powered alert system with anomaly detection and smart routing for RabbitMQ and Kafka
"""
import json
import statistics
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import httpx
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from core.logging import get_logger

logger = get_logger(__name__)


class AlertSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertStatus(Enum):
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


@dataclass
class Alert:
    id: str
    title: str
    description: str
    severity: AlertSeverity
    status: AlertStatus
    source: str
    metric_name: str
    metric_value: float
    threshold: float | None
    timestamp: datetime
    tags: dict[str, str]
    resolution_time: datetime | None = None
    acknowledged_by: str | None = None
    suppressed_until: datetime | None = None


@dataclass
class AlertRule:
    name: str
    metric_name: str
    condition: str  # 'gt', 'lt', 'eq', 'anomaly'
    threshold: float | None
    severity: AlertSeverity
    enabled: bool = True
    tags: dict[str, str] = None
    cooldown_minutes: int = 15
    evaluation_window_minutes: int = 5
    min_samples: int = 5
    custom_evaluator: Callable | None = None


@dataclass
class NotificationChannel:
    name: str
    type: str  # 'slack', 'email', 'webhook', 'pagerduty'
    config: dict[str, Any]
    enabled: bool = True
    severity_filter: list[AlertSeverity] = None


class AnomalyDetector:
    def __init__(self, contamination: float = 0.1, n_estimators: int = 100):
        self.model = IsolationForest(contamination=contamination, n_estimators=n_estimators, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.feature_history = []
        self.min_samples_for_training = 50

    def add_sample(self, features: list[float]):
        """Add a new sample to the training data"""
        self.feature_history.append(features)

        # Keep only recent samples to adapt to changing patterns
        if len(self.feature_history) > 1000:
            self.feature_history = self.feature_history[-1000:]

        # Retrain if we have enough samples
        if len(self.feature_history) >= self.min_samples_for_training and len(self.feature_history) % 10 == 0:
            self._retrain()

    def _retrain(self):
        """Retrain the anomaly detection model"""
        try:
            X = np.array(self.feature_history)
            X_scaled = self.scaler.fit_transform(X)
            self.model.fit(X_scaled)
            self.is_trained = True
            logger.info(f"Retrained anomaly detector with {len(self.feature_history)} samples")
        except Exception as e:
            logger.error(f"Failed to retrain anomaly detector: {e}")

    def is_anomaly(self, features: list[float]) -> tuple[bool, float]:
        """Check if the given features represent an anomaly"""
        if not self.is_trained:
            return False, 0.0

        try:
            X = np.array([features])
            X_scaled = self.scaler.transform(X)

            # Get anomaly score (-1 for anomaly, 1 for normal)
            anomaly_score = self.model.decision_function(X_scaled)[0]
            is_outlier = self.model.predict(X_scaled)[0] == -1

            # Convert to probability-like score (0-1, higher = more anomalous)
            anomaly_probability = max(0, (0.5 - anomaly_score) * 2)

            return is_outlier, anomaly_probability

        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            return False, 0.0


class IntelligentAlerting:
    def __init__(self):
        self.alert_rules: dict[str, AlertRule] = {}
        self.active_alerts: dict[str, Alert] = {}
        self.alert_history: list[Alert] = []
        self.notification_channels: dict[str, NotificationChannel] = {}
        self.anomaly_detectors: dict[str, AnomalyDetector] = {}

        # Metric storage for trend analysis
        self.metric_history: dict[str, list[tuple[datetime, float]]] = {}
        self.max_history_size = 10000

        # Alert suppression and grouping
        self.alert_cooldowns: dict[str, datetime] = {}
        self.grouped_alerts: dict[str, list[Alert]] = {}

    def add_alert_rule(self, rule: AlertRule):
        """Add a new alert rule"""
        self.alert_rules[rule.name] = rule

        # Initialize anomaly detector for anomaly-based rules
        if rule.condition == 'anomaly':
            self.anomaly_detectors[rule.metric_name] = AnomalyDetector()

        logger.info(f"Added alert rule: {rule.name}")

    def add_notification_channel(self, channel: NotificationChannel):
        """Add a notification channel"""
        self.notification_channels[channel.name] = channel
        logger.info(f"Added notification channel: {channel.name} ({channel.type})")

    async def process_metric(self, metric_name: str, value: float, tags: dict[str, str] = None):
        """Process a metric value and check for alerts"""
        timestamp = datetime.utcnow()
        tags = tags or {}

        # Store metric history
        if metric_name not in self.metric_history:
            self.metric_history[metric_name] = []

        self.metric_history[metric_name].append((timestamp, value))

        # Limit history size
        if len(self.metric_history[metric_name]) > self.max_history_size:
            self.metric_history[metric_name] = self.metric_history[metric_name][-self.max_history_size:]

        # Update anomaly detector
        if metric_name in self.anomaly_detectors:
            # Create features from recent history
            features = self._extract_features(metric_name, timestamp)
            if features:
                self.anomaly_detectors[metric_name].add_sample(features)

        # Evaluate alert rules
        await self._evaluate_alert_rules(metric_name, value, timestamp, tags)

    def _extract_features(self, metric_name: str, timestamp: datetime) -> list[float] | None:
        """Extract features for anomaly detection"""
        history = self.metric_history.get(metric_name, [])

        # Need at least 10 data points for meaningful features
        if len(history) < 10:
            return None

        # Get recent values (last 10 points)
        recent_values = [v for t, v in history[-10:]]

        features = [
            recent_values[-1],  # Current value
            statistics.mean(recent_values),  # Mean
            statistics.stdev(recent_values) if len(recent_values) > 1 else 0,  # Std dev
            max(recent_values),  # Max
            min(recent_values),  # Min
            recent_values[-1] - recent_values[-2] if len(recent_values) >= 2 else 0,  # Change from previous
        ]

        # Add time-based features
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        features.extend([hour, day_of_week])

        return features

    async def _evaluate_alert_rules(self, metric_name: str, value: float, timestamp: datetime, tags: dict[str, str]):
        """Evaluate all alert rules for a metric"""
        for rule_name, rule in self.alert_rules.items():
            if rule.metric_name != metric_name or not rule.enabled:
                continue

            # Check cooldown
            cooldown_key = f"{rule_name}:{json.dumps(tags, sort_keys=True)}"
            if cooldown_key in self.alert_cooldowns:
                if timestamp < self.alert_cooldowns[cooldown_key]:
                    continue

            # Evaluate rule
            should_alert, alert_context = await self._evaluate_rule(rule, metric_name, value, timestamp, tags)

            if should_alert:
                alert = Alert(
                    id=f"{rule_name}-{timestamp.timestamp()}",
                    title=f"{rule_name}: {metric_name}",
                    description=self._generate_alert_description(rule, value, alert_context),
                    severity=rule.severity,
                    status=AlertStatus.ACTIVE,
                    source=rule_name,
                    metric_name=metric_name,
                    metric_value=value,
                    threshold=rule.threshold,
                    timestamp=timestamp,
                    tags=tags
                )

                await self._fire_alert(alert, rule)

                # Set cooldown
                cooldown_end = timestamp + timedelta(minutes=rule.cooldown_minutes)
                self.alert_cooldowns[cooldown_key] = cooldown_end

    async def _evaluate_rule(self, rule: AlertRule, metric_name: str, value: float, timestamp: datetime, tags: dict[str, str]) -> tuple[bool, dict[str, Any]]:
        """Evaluate a specific rule"""
        context = {}

        # Custom evaluator
        if rule.custom_evaluator:
            try:
                return rule.custom_evaluator(metric_name, value, timestamp, tags), context
            except Exception as e:
                logger.error(f"Custom evaluator failed for rule {rule.name}: {e}")
                return False, context

        # Standard conditions
        if rule.condition == 'gt':
            return value > rule.threshold, context
        elif rule.condition == 'lt':
            return value < rule.threshold, context
        elif rule.condition == 'eq':
            return abs(value - rule.threshold) < 0.001, context
        elif rule.condition == 'anomaly':
            if metric_name in self.anomaly_detectors:
                features = self._extract_features(metric_name, timestamp)
                if features:
                    is_anomaly, anomaly_score = self.anomaly_detectors[metric_name].is_anomaly(features)
                    context['anomaly_score'] = anomaly_score
                    return is_anomaly, context

        return False, context

    def _generate_alert_description(self, rule: AlertRule, value: float, context: dict[str, Any]) -> str:
        """Generate human-readable alert description"""
        if rule.condition == 'anomaly':
            anomaly_score = context.get('anomaly_score', 0)
            return f"Anomaly detected in {rule.metric_name}. Current value: {value:.2f}, Anomaly score: {anomaly_score:.2f}"
        else:
            operator_text = {
                'gt': 'above',
                'lt': 'below',
                'eq': 'equal to'
            }.get(rule.condition, rule.condition)
            return f"{rule.metric_name} is {operator_text} threshold. Current: {value:.2f}, Threshold: {rule.threshold:.2f}"

    async def _fire_alert(self, alert: Alert, rule: AlertRule):
        """Fire an alert and send notifications"""
        # Store alert
        self.active_alerts[alert.id] = alert
        self.alert_history.append(alert)

        # Group similar alerts
        group_key = f"{alert.source}:{alert.metric_name}"
        if group_key not in self.grouped_alerts:
            self.grouped_alerts[group_key] = []
        self.grouped_alerts[group_key].append(alert)

        logger.warning(
            f"Alert fired: {alert.title}",
            extra={
                "alert_id": alert.id,
                "severity": alert.severity.value,
                "metric_value": alert.metric_value,
                "threshold": alert.threshold
            }
        )

        # Send notifications
        await self._send_notifications(alert, rule)

    async def _send_notifications(self, alert: Alert, rule: AlertRule):
        """Send notifications through configured channels"""
        for channel_name, channel in self.notification_channels.items():
            if not channel.enabled:
                continue

            # Check severity filter
            if channel.severity_filter and alert.severity not in channel.severity_filter:
                continue

            try:
                await self._send_notification(channel, alert)
            except Exception as e:
                logger.error(f"Failed to send notification via {channel_name}: {e}")

    async def _send_notification(self, channel: NotificationChannel, alert: Alert):
        """Send notification via specific channel"""
        if channel.type == 'slack':
            await self._send_slack_notification(channel, alert)
        elif channel.type == 'webhook':
            await self._send_webhook_notification(channel, alert)
        elif channel.type == 'email':
            await self._send_email_notification(channel, alert)
        # Add more notification types as needed

    async def _send_slack_notification(self, channel: NotificationChannel, alert: Alert):
        """Send Slack notification"""
        webhook_url = channel.config.get('webhook_url')
        if not webhook_url:
            return

        color_map = {
            AlertSeverity.LOW: '#36a64f',
            AlertSeverity.MEDIUM: '#ffcc00',
            AlertSeverity.HIGH: '#ff9900',
            AlertSeverity.CRITICAL: '#ff0000'
        }

        payload = {
            "attachments": [{
                "color": color_map.get(alert.severity, '#808080'),
                "title": f"🚨 {alert.title}",
                "text": alert.description,
                "fields": [
                    {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
                    {"title": "Metric", "value": f"{alert.metric_value:.2f}", "short": True},
                    {"title": "Threshold", "value": f"{alert.threshold:.2f}" if alert.threshold else "N/A", "short": True},
                    {"title": "Time", "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"), "short": True}
                ],
                "footer": f"Source: {alert.source}",
                "ts": int(alert.timestamp.timestamp())
            }]
        }

        async with httpx.AsyncClient() as client:
            await client.post(webhook_url, json=payload, timeout=10)

    async def _send_webhook_notification(self, channel: NotificationChannel, alert: Alert):
        """Send webhook notification"""
        webhook_url = channel.config.get('url')
        if not webhook_url:
            return

        payload = {
            "alert_id": alert.id,
            "title": alert.title,
            "description": alert.description,
            "severity": alert.severity.value,
            "metric_name": alert.metric_name,
            "metric_value": alert.metric_value,
            "threshold": alert.threshold,
            "timestamp": alert.timestamp.isoformat(),
            "tags": alert.tags,
            "source": alert.source
        }

        headers = channel.config.get('headers', {})

        async with httpx.AsyncClient() as client:
            await client.post(webhook_url, json=payload, headers=headers, timeout=10)

    async def _send_email_notification(self, channel: NotificationChannel, alert: Alert):
        """Send email notification (placeholder implementation)"""
        # In a real implementation, integrate with email service
        logger.info(f"Would send email notification for alert {alert.id}")

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str):
        """Acknowledge an active alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].status = AlertStatus.ACKNOWLEDGED
            self.active_alerts[alert_id].acknowledged_by = acknowledged_by
            logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")

    async def resolve_alert(self, alert_id: str):
        """Resolve an active alert"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.status = AlertStatus.RESOLVED
            alert.resolution_time = datetime.utcnow()

            # Remove from active alerts
            del self.active_alerts[alert_id]

            logger.info(f"Alert {alert_id} resolved")

    def get_active_alerts(self, severity_filter: AlertSeverity | None = None) -> list[Alert]:
        """Get all active alerts, optionally filtered by severity"""
        alerts = list(self.active_alerts.values())

        if severity_filter:
            alerts = [a for a in alerts if a.severity == severity_filter]

        return sorted(alerts, key=lambda x: x.timestamp, reverse=True)

    def get_alert_statistics(self) -> dict[str, Any]:
        """Get alert statistics"""
        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        last_week = now - timedelta(days=7)

        recent_alerts = [a for a in self.alert_history if a.timestamp >= last_24h]
        weekly_alerts = [a for a in self.alert_history if a.timestamp >= last_week]

        return {
            "active_alerts": len(self.active_alerts),
            "alerts_last_24h": len(recent_alerts),
            "alerts_last_week": len(weekly_alerts),
            "alerts_by_severity": {
                severity.value: len([a for a in recent_alerts if a.severity == severity])
                for severity in AlertSeverity
            },
            "top_alert_sources": self._get_top_alert_sources(recent_alerts),
            "avg_resolution_time_minutes": self._calculate_avg_resolution_time(weekly_alerts)
        }

    def _get_top_alert_sources(self, alerts: list[Alert], limit: int = 5) -> list[dict[str, Any]]:
        """Get top alert sources by frequency"""
        source_counts = {}
        for alert in alerts:
            source_counts[alert.source] = source_counts.get(alert.source, 0) + 1

        sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
        return [{"source": source, "count": count} for source, count in sorted_sources[:limit]]

    def _calculate_avg_resolution_time(self, alerts: list[Alert]) -> float | None:
        """Calculate average resolution time in minutes"""
        resolved_alerts = [a for a in alerts if a.resolution_time]

        if not resolved_alerts:
            return None

        total_minutes = sum([
            (a.resolution_time - a.timestamp).total_seconds() / 60
            for a in resolved_alerts
        ])

        return total_minutes / len(resolved_alerts)


# Messaging-specific alert factory functions

def create_rabbitmq_alert_rules() -> list[AlertRule]:
    """Create RabbitMQ-specific alert rules"""
    return [
        AlertRule(
            id="rabbitmq_queue_depth_high",
            name="RabbitMQ High Queue Depth",
            description="Queue depth exceeds warning threshold",
            metric_name="rabbitmq_queue_depth",
            threshold=1000,
            severity=AlertSeverity.WARNING,
            condition=lambda x: x > 1000,
            source="rabbitmq",
            tags={"system": "messaging", "component": "rabbitmq"}
        ),
        AlertRule(
            id="rabbitmq_queue_depth_critical",
            name="RabbitMQ Critical Queue Depth", 
            description="Queue depth critically high - immediate attention required",
            metric_name="rabbitmq_queue_depth",
            threshold=5000,
            severity=AlertSeverity.CRITICAL,
            condition=lambda x: x > 5000,
            source="rabbitmq",
            tags={"system": "messaging", "component": "rabbitmq"}
        ),
        AlertRule(
            id="rabbitmq_no_consumers",
            name="RabbitMQ No Active Consumers",
            description="Queue has messages but no active consumers",
            metric_name="rabbitmq_consumer_count",
            threshold=0,
            severity=AlertSeverity.ERROR,
            condition=lambda x: x == 0,
            source="rabbitmq",
            tags={"system": "messaging", "component": "rabbitmq"}
        ),
        AlertRule(
            id="rabbitmq_connection_failures",
            name="RabbitMQ Connection Failures",
            description="High number of connection failures detected",
            metric_name="rabbitmq_connection_errors",
            threshold=10,
            severity=AlertSeverity.ERROR,
            condition=lambda x: x > 10,
            source="rabbitmq",
            tags={"system": "messaging", "component": "rabbitmq"}
        ),
        AlertRule(
            id="rabbitmq_memory_usage_high",
            name="RabbitMQ High Memory Usage",
            description="RabbitMQ memory usage exceeds safe threshold",
            metric_name="rabbitmq_memory_usage_percent",
            threshold=85.0,
            severity=AlertSeverity.WARNING,
            condition=lambda x: x > 85.0,
            source="rabbitmq",
            tags={"system": "messaging", "component": "rabbitmq"}
        )
    ]


def create_kafka_alert_rules() -> list[AlertRule]:
    """Create Kafka-specific alert rules"""
    return [
        AlertRule(
            id="kafka_consumer_lag_high",
            name="Kafka High Consumer Lag",
            description="Consumer lag exceeds warning threshold",
            metric_name="kafka_consumer_lag",
            threshold=1000,
            severity=AlertSeverity.WARNING,
            condition=lambda x: x > 1000,
            source="kafka",
            tags={"system": "messaging", "component": "kafka"}
        ),
        AlertRule(
            id="kafka_consumer_lag_critical",
            name="Kafka Critical Consumer Lag",
            description="Consumer lag critically high - data processing delayed",
            metric_name="kafka_consumer_lag",
            threshold=10000,
            severity=AlertSeverity.CRITICAL,
            condition=lambda x: x > 10000,
            source="kafka",
            tags={"system": "messaging", "component": "kafka"}
        ),
        AlertRule(
            id="kafka_broker_down",
            name="Kafka Broker Unavailable",
            description="One or more Kafka brokers are unavailable",
            metric_name="kafka_broker_count",
            threshold=1,  # Assuming minimum of 1 broker required
            severity=AlertSeverity.CRITICAL,
            condition=lambda x: x < 1,
            source="kafka",
            tags={"system": "messaging", "component": "kafka"}
        ),
        AlertRule(
            id="kafka_under_replicated_partitions",
            name="Kafka Under-replicated Partitions",
            description="Some partitions are under-replicated",
            metric_name="kafka_under_replicated_partitions",
            threshold=0,
            severity=AlertSeverity.ERROR,
            condition=lambda x: x > 0,
            source="kafka",
            tags={"system": "messaging", "component": "kafka"}
        ),
        AlertRule(
            id="kafka_offline_partitions",
            name="Kafka Offline Partitions",
            description="Some partitions are offline",
            metric_name="kafka_offline_partitions",
            threshold=0,
            severity=AlertSeverity.CRITICAL,
            condition=lambda x: x > 0,
            source="kafka",
            tags={"system": "messaging", "component": "kafka"}
        ),
        AlertRule(
            id="kafka_low_throughput",
            name="Kafka Low Message Throughput",
            description="Message throughput unusually low",
            metric_name="kafka_messages_per_sec",
            threshold=10.0,
            severity=AlertSeverity.WARNING,
            condition=lambda x: x < 10.0,
            source="kafka",
            tags={"system": "messaging", "component": "kafka"}
        )
    ]


def create_messaging_system_alert_rules() -> list[AlertRule]:
    """Create general messaging system alert rules"""
    return [
        AlertRule(
            id="messaging_error_rate_high",
            name="High Messaging Error Rate",
            description="Error rate in messaging operations is above threshold",
            metric_name="messaging_errors_per_minute",
            threshold=5.0,
            severity=AlertSeverity.ERROR,
            condition=lambda x: x > 5.0,
            source="messaging",
            tags={"system": "messaging", "component": "general"}
        ),
        AlertRule(
            id="message_processing_latency_high",
            name="High Message Processing Latency",
            description="Message processing taking longer than expected",
            metric_name="message_processing_time_ms",
            threshold=5000.0,  # 5 seconds
            severity=AlertSeverity.WARNING,
            condition=lambda x: x > 5000.0,
            source="messaging",
            tags={"system": "messaging", "component": "general"}
        ),
        AlertRule(
            id="dead_letter_queue_growing",
            name="Dead Letter Queue Growing",
            description="Dead letter queue size is increasing",
            metric_name="dead_letter_queue_size",
            threshold=50,
            severity=AlertSeverity.WARNING,
            condition=lambda x: x > 50,
            source="messaging",
            tags={"system": "messaging", "component": "general"}
        ),
        AlertRule(
            id="messaging_system_unavailable",
            name="Messaging System Unavailable",
            description="Messaging system health check failing",
            metric_name="messaging_system_health",
            threshold=1,
            severity=AlertSeverity.CRITICAL,
            condition=lambda x: x == 0,  # 0 = unhealthy, 1 = healthy
            source="messaging",
            tags={"system": "messaging", "component": "general"}
        )
    ]


class MessagingAlertManager:
    """Specialized alert manager for messaging systems"""
    
    def __init__(self, alert_manager: IntelligentAlertManager):
        self.alert_manager = alert_manager
        self.logger = get_logger(__name__)
        
        # Setup messaging-specific alert rules
        self._setup_messaging_alerts()
    
    def _setup_messaging_alerts(self):
        """Setup all messaging-related alert rules"""
        # Add RabbitMQ alerts
        for rule in create_rabbitmq_alert_rules():
            self.alert_manager.add_alert_rule(rule)
        
        # Add Kafka alerts  
        for rule in create_kafka_alert_rules():
            self.alert_manager.add_alert_rule(rule)
        
        # Add general messaging alerts
        for rule in create_messaging_system_alert_rules():
            self.alert_manager.add_alert_rule(rule)
        
        self.logger.info("Setup messaging-specific alert rules")
    
    def create_custom_messaging_alert(
        self,
        alert_id: str,
        name: str,
        description: str,
        metric_name: str,
        threshold: float,
        severity: AlertSeverity,
        component: str,
        condition: Callable[[float], bool] = None
    ) -> AlertRule:
        """Create custom messaging alert rule"""
        
        if condition is None:
            condition = lambda x: x > threshold
        
        rule = AlertRule(
            id=alert_id,
            name=name,
            description=description,
            metric_name=metric_name,
            threshold=threshold,
            severity=severity,
            condition=condition,
            source=component,
            tags={"system": "messaging", "component": component, "custom": "true"}
        )
        
        self.alert_manager.add_alert_rule(rule)
        self.logger.info(f"Created custom messaging alert rule: {name}")
        
        return rule
    
    async def check_messaging_health(self) -> dict[str, Any]:
        """Check overall messaging system health"""
        rabbitmq_alerts = [
            alert for alert in self.alert_manager.get_active_alerts()
            if alert.source == "rabbitmq"
        ]
        
        kafka_alerts = [
            alert for alert in self.alert_manager.get_active_alerts()
            if alert.source == "kafka"
        ]
        
        general_alerts = [
            alert for alert in self.alert_manager.get_active_alerts()
            if alert.source == "messaging"
        ]
        
        # Determine overall health status
        critical_alerts = [
            alert for alert in (rabbitmq_alerts + kafka_alerts + general_alerts)
            if alert.severity == AlertSeverity.CRITICAL
        ]
        
        error_alerts = [
            alert for alert in (rabbitmq_alerts + kafka_alerts + general_alerts)
            if alert.severity == AlertSeverity.ERROR
        ]
        
        if critical_alerts:
            health_status = "critical"
        elif error_alerts:
            health_status = "degraded"
        elif rabbitmq_alerts or kafka_alerts or general_alerts:
            health_status = "warning"
        else:
            health_status = "healthy"
        
        return {
            "overall_health": health_status,
            "rabbitmq": {
                "active_alerts": len(rabbitmq_alerts),
                "critical_alerts": len([a for a in rabbitmq_alerts if a.severity == AlertSeverity.CRITICAL]),
                "error_alerts": len([a for a in rabbitmq_alerts if a.severity == AlertSeverity.ERROR])
            },
            "kafka": {
                "active_alerts": len(kafka_alerts),
                "critical_alerts": len([a for a in kafka_alerts if a.severity == AlertSeverity.CRITICAL]),
                "error_alerts": len([a for a in kafka_alerts if a.severity == AlertSeverity.ERROR])
            },
            "general_messaging": {
                "active_alerts": len(general_alerts),
                "critical_alerts": len([a for a in general_alerts if a.severity == AlertSeverity.CRITICAL]),
                "error_alerts": len([a for a in general_alerts if a.severity == AlertSeverity.ERROR])
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def get_messaging_alert_summary(self) -> dict[str, Any]:
        """Get summary of messaging-specific alerts"""
        all_alerts = self.alert_manager.get_active_alerts()
        
        messaging_alerts = [
            alert for alert in all_alerts
            if alert.source in ["rabbitmq", "kafka", "messaging"]
        ]
        
        return {
            "total_messaging_alerts": len(messaging_alerts),
            "by_component": {
                "rabbitmq": len([a for a in messaging_alerts if a.source == "rabbitmq"]),
                "kafka": len([a for a in messaging_alerts if a.source == "kafka"]),
                "general": len([a for a in messaging_alerts if a.source == "messaging"])
            },
            "by_severity": {
                severity.value: len([a for a in messaging_alerts if a.severity == severity])
                for severity in AlertSeverity
            },
            "most_recent_alert": messaging_alerts[0].timestamp.isoformat() if messaging_alerts else None,
            "timestamp": datetime.utcnow().isoformat()
        }


# Factory function for creating messaging alert manager
def create_messaging_alert_manager(alert_manager: IntelligentAlertManager) -> MessagingAlertManager:
    """Create messaging-specific alert manager"""
    return MessagingAlertManager(alert_manager)
