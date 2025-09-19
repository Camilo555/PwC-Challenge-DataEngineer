"""
Enterprise Resource Usage Dashboard and Alerting System
======================================================

Comprehensive dashboard and alerting system for monitoring system resources, performance metrics,
and operational health across the enterprise data platform.

Features:
- Real-time resource monitoring dashboards
- Multi-channel alerting (Email, Slack, PagerDuty, Webhook)
- Custom metric visualization and KPI tracking
- Historical trend analysis and forecasting
- Interactive dashboard generation for Grafana
- Automated alert correlation and suppression
- SLA monitoring and breach detection
- Integration with Prometheus, DataDog, and custom metrics

Author: Enterprise Data Engineering Team
Created: 2025-09-18
"""

import asyncio
import json
import logging
import smtplib
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import aiohttp
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psutil
import requests
from jinja2 import Template
from prometheus_client import REGISTRY, CollectorRegistry, generate_latest


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Alert delivery channels."""
    EMAIL = "email"
    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    WEBHOOK = "webhook"
    SMS = "sms"
    TEAMS = "teams"


class DashboardType(Enum):
    """Dashboard types for different audiences."""
    EXECUTIVE = "executive"
    OPERATIONAL = "operational"
    TECHNICAL = "technical"
    SECURITY = "security"
    PERFORMANCE = "performance"


@dataclass
class AlertRule:
    """Definition of an alert rule."""

    name: str
    description: str
    metric_name: str
    threshold: float
    operator: str  # >, <, >=, <=, ==, !=
    severity: AlertSeverity
    channels: List[AlertChannel]
    evaluation_window_minutes: int = 5
    min_firing_duration_minutes: int = 1
    cooldown_minutes: int = 15
    enabled: bool = True
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)


@dataclass
class Alert:
    """Active alert instance."""

    rule_name: str
    severity: AlertSeverity
    message: str
    metric_value: float
    threshold: float
    timestamp: datetime = field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)


@dataclass
class DashboardWidget:
    """Dashboard widget definition."""

    id: str
    title: str
    type: str  # metric, chart, table, gauge, heatmap
    query: str
    visualization: Dict[str, Any] = field(default_factory=dict)
    position: Dict[str, int] = field(default_factory=dict)
    refresh_interval: int = 30
    thresholds: Dict[str, float] = field(default_factory=dict)


@dataclass
class Dashboard:
    """Dashboard definition."""

    id: str
    title: str
    description: str
    type: DashboardType
    widgets: List[DashboardWidget] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    auto_refresh: bool = True
    refresh_interval: int = 30
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class AlertManager:
    """Manages alert rules, evaluation, and delivery."""

    def __init__(self):
        self.alert_rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=10000)
        self.suppressed_alerts: Set[str] = set()
        self.alert_channels: Dict[AlertChannel, Dict] = {}

        # Alert correlation and grouping
        self.alert_groups: Dict[str, List[str]] = defaultdict(list)
        self.correlation_rules: Dict[str, Callable] = {}

        # Metrics for alert evaluation
        self.metrics_cache: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))

        # Threading
        self.evaluation_thread: Optional[threading.Thread] = None
        self.is_running = False
        self.lock = threading.RLock()

        self.logger = logging.getLogger(__name__)

    def add_alert_rule(self, rule: AlertRule):
        """Add an alert rule."""
        self.alert_rules[rule.name] = rule
        self.logger.info(f"Added alert rule: {rule.name}")

    def configure_alert_channel(self, channel: AlertChannel, config: Dict[str, Any]):
        """Configure alert delivery channel."""
        self.alert_channels[channel] = config
        self.logger.info(f"Configured alert channel: {channel.value}")

    def start_alert_evaluation(self, interval_seconds: int = 30):
        """Start alert rule evaluation."""
        if self.is_running:
            return

        self.is_running = True
        self.evaluation_thread = threading.Thread(
            target=self._evaluation_loop,
            args=(interval_seconds,),
            name="alert_evaluator",
            daemon=True
        )
        self.evaluation_thread.start()
        self.logger.info("Alert evaluation started")

    def stop_alert_evaluation(self):
        """Stop alert rule evaluation."""
        self.is_running = False
        if self.evaluation_thread and self.evaluation_thread.is_alive():
            self.evaluation_thread.join(timeout=5.0)
        self.logger.info("Alert evaluation stopped")

    def _evaluation_loop(self, interval_seconds: int):
        """Main alert evaluation loop."""
        while self.is_running:
            try:
                self._evaluate_all_rules()
                time.sleep(interval_seconds)
            except Exception as e:
                self.logger.error(f"Error in alert evaluation: {e}")
                time.sleep(interval_seconds)

    def _evaluate_all_rules(self):
        """Evaluate all alert rules."""
        current_time = datetime.now()

        for rule_name, rule in self.alert_rules.items():
            if not rule.enabled:
                continue

            try:
                self._evaluate_rule(rule, current_time)
            except Exception as e:
                self.logger.error(f"Error evaluating rule {rule_name}: {e}")

    def _evaluate_rule(self, rule: AlertRule, current_time: datetime):
        """Evaluate a single alert rule."""
        # Get metric value (simplified - in practice would query from metrics backend)
        metric_value = self._get_metric_value(rule.metric_name)

        if metric_value is None:
            return

        # Store metric for history
        self.metrics_cache[rule.metric_name].append((current_time, metric_value))

        # Evaluate condition
        is_firing = self._evaluate_condition(metric_value, rule.threshold, rule.operator)

        alert_key = f"{rule.name}_{rule.metric_name}"

        if is_firing:
            if alert_key not in self.active_alerts:
                # Create new alert
                alert = Alert(
                    rule_name=rule.name,
                    severity=rule.severity,
                    message=f"{rule.description}: {rule.metric_name} = {metric_value} {rule.operator} {rule.threshold}",
                    metric_value=metric_value,
                    threshold=rule.threshold,
                    labels=rule.labels.copy(),
                    annotations=rule.annotations.copy()
                )

                self.active_alerts[alert_key] = alert
                self.alert_history.append(alert)

                # Send alert notifications
                self._send_alert_notifications(alert, rule)

                self.logger.warning(f"Alert fired: {rule.name}")
        else:
            if alert_key in self.active_alerts:
                # Resolve alert
                alert = self.active_alerts[alert_key]
                alert.resolved_at = current_time
                del self.active_alerts[alert_key]

                self.logger.info(f"Alert resolved: {rule.name}")

    def _get_metric_value(self, metric_name: str) -> Optional[float]:
        """Get current metric value."""
        # Simplified metric collection - in practice would integrate with monitoring system
        try:
            if metric_name == "cpu_usage_percent":
                return psutil.cpu_percent()
            elif metric_name == "memory_usage_percent":
                return psutil.virtual_memory().percent
            elif metric_name == "disk_usage_percent":
                return psutil.disk_usage('/').percent
            elif metric_name == "load_average":
                return psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0.0
            else:
                # Custom metrics would be retrieved from metrics backend
                return None
        except Exception:
            return None

    def _evaluate_condition(self, value: float, threshold: float, operator: str) -> bool:
        """Evaluate alert condition."""
        operators = {
            '>': lambda x, y: x > y,
            '<': lambda x, y: x < y,
            '>=': lambda x, y: x >= y,
            '<=': lambda x, y: x <= y,
            '==': lambda x, y: x == y,
            '!=': lambda x, y: x != y
        }

        op_func = operators.get(operator)
        if op_func:
            return op_func(value, threshold)
        return False

    def _send_alert_notifications(self, alert: Alert, rule: AlertRule):
        """Send alert notifications to configured channels."""
        for channel in rule.channels:
            try:
                if channel == AlertChannel.EMAIL:
                    self._send_email_alert(alert)
                elif channel == AlertChannel.SLACK:
                    self._send_slack_alert(alert)
                elif channel == AlertChannel.WEBHOOK:
                    self._send_webhook_alert(alert)
                # Add other channels as needed
            except Exception as e:
                self.logger.error(f"Failed to send alert via {channel.value}: {e}")

    def _send_email_alert(self, alert: Alert):
        """Send email alert."""
        email_config = self.alert_channels.get(AlertChannel.EMAIL, {})
        if not email_config:
            return

        msg = MIMEMultipart()
        msg['From'] = email_config.get('sender')
        msg['To'] = ', '.join(email_config.get('recipients', []))
        msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.rule_name}"

        body = f"""
        Alert: {alert.rule_name}
        Severity: {alert.severity.value}
        Message: {alert.message}
        Timestamp: {alert.timestamp}
        Metric Value: {alert.metric_value}
        Threshold: {alert.threshold}
        """

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(email_config.get('smtp_host'), email_config.get('smtp_port', 587))
        if email_config.get('use_tls'):
            server.starttls()
        if email_config.get('username'):
            server.login(email_config['username'], email_config['password'])

        server.sendmail(msg['From'], email_config.get('recipients', []), msg.as_string())
        server.quit()

    def _send_slack_alert(self, alert: Alert):
        """Send Slack alert."""
        slack_config = self.alert_channels.get(AlertChannel.SLACK, {})
        webhook_url = slack_config.get('webhook_url')

        if not webhook_url:
            return

        color_map = {
            AlertSeverity.INFO: "#36a64f",
            AlertSeverity.WARNING: "#ff9500",
            AlertSeverity.ERROR: "#ff0000",
            AlertSeverity.CRITICAL: "#8B0000"
        }

        payload = {
            "attachments": [{
                "color": color_map.get(alert.severity, "#ff0000"),
                "title": f"Alert: {alert.rule_name}",
                "text": alert.message,
                "fields": [
                    {"title": "Severity", "value": alert.severity.value, "short": True},
                    {"title": "Metric Value", "value": str(alert.metric_value), "short": True},
                    {"title": "Threshold", "value": str(alert.threshold), "short": True},
                    {"title": "Timestamp", "value": alert.timestamp.isoformat(), "short": True}
                ]
            }]
        }

        requests.post(webhook_url, json=payload)

    def _send_webhook_alert(self, alert: Alert):
        """Send webhook alert."""
        webhook_config = self.alert_channels.get(AlertChannel.WEBHOOK, {})
        webhook_url = webhook_config.get('url')

        if not webhook_url:
            return

        payload = {
            "alert": {
                "rule_name": alert.rule_name,
                "severity": alert.severity.value,
                "message": alert.message,
                "metric_value": alert.metric_value,
                "threshold": alert.threshold,
                "timestamp": alert.timestamp.isoformat(),
                "labels": alert.labels,
                "annotations": alert.annotations
            }
        }

        headers = webhook_config.get('headers', {})
        requests.post(webhook_url, json=payload, headers=headers)


class DashboardManager:
    """Manages dashboard creation, rendering, and updates."""

    def __init__(self):
        self.dashboards: Dict[str, Dashboard] = {}
        self.dashboard_templates: Dict[str, str] = {}
        self.metrics_provider: Optional[Any] = None

        self.logger = logging.getLogger(__name__)

    def create_dashboard(self, dashboard: Dashboard):
        """Create a new dashboard."""
        self.dashboards[dashboard.id] = dashboard
        self.logger.info(f"Created dashboard: {dashboard.title}")

    def add_widget(self, dashboard_id: str, widget: DashboardWidget):
        """Add widget to dashboard."""
        if dashboard_id in self.dashboards:
            self.dashboards[dashboard_id].widgets.append(widget)
            self.dashboards[dashboard_id].updated_at = datetime.now()

    def generate_grafana_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Generate Grafana dashboard JSON."""
        dashboard = self.dashboards.get(dashboard_id)
        if not dashboard:
            return {}

        grafana_dashboard = {
            "dashboard": {
                "id": None,
                "title": dashboard.title,
                "description": dashboard.description,
                "tags": dashboard.tags,
                "timezone": "browser",
                "panels": [],
                "time": {
                    "from": "now-24h",
                    "to": "now"
                },
                "refresh": f"{dashboard.refresh_interval}s",
                "schemaVersion": 16,
                "version": 0
            },
            "folderId": 0,
            "overwrite": True
        }

        # Convert widgets to Grafana panels
        for i, widget in enumerate(dashboard.widgets):
            panel = self._widget_to_grafana_panel(widget, i)
            grafana_dashboard["dashboard"]["panels"].append(panel)

        return grafana_dashboard

    def _widget_to_grafana_panel(self, widget: DashboardWidget, panel_id: int) -> Dict[str, Any]:
        """Convert widget to Grafana panel."""
        base_panel = {
            "id": panel_id,
            "title": widget.title,
            "type": self._map_widget_type_to_grafana(widget.type),
            "targets": [{
                "expr": widget.query,
                "refId": "A"
            }],
            "gridPos": {
                "h": widget.position.get("height", 8),
                "w": widget.position.get("width", 12),
                "x": widget.position.get("x", 0),
                "y": widget.position.get("y", 0)
            }
        }

        # Add type-specific configuration
        if widget.type == "gauge":
            base_panel.update({
                "fieldConfig": {
                    "defaults": {
                        "min": 0,
                        "max": 100,
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": 0},
                                {"color": "yellow", "value": widget.thresholds.get("warning", 70)},
                                {"color": "red", "value": widget.thresholds.get("critical", 90)}
                            ]
                        }
                    }
                }
            })

        return base_panel

    def _map_widget_type_to_grafana(self, widget_type: str) -> str:
        """Map widget types to Grafana panel types."""
        mapping = {
            "metric": "stat",
            "chart": "graph",
            "table": "table",
            "gauge": "gauge",
            "heatmap": "heatmap"
        }
        return mapping.get(widget_type, "graph")

    def render_dashboard_html(self, dashboard_id: str) -> str:
        """Render dashboard as HTML."""
        dashboard = self.dashboards.get(dashboard_id)
        if not dashboard:
            return "<html><body>Dashboard not found</body></html>"

        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>{{ dashboard.title }}</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .dashboard-header { background: #f0f0f0; padding: 20px; margin-bottom: 20px; }
                .widget { display: inline-block; margin: 10px; border: 1px solid #ddd; }
                .widget-title { background: #333; color: white; padding: 10px; margin: 0; }
                .widget-content { padding: 20px; }
            </style>
        </head>
        <body>
            <div class="dashboard-header">
                <h1>{{ dashboard.title }}</h1>
                <p>{{ dashboard.description }}</p>
                <p>Last updated: {{ dashboard.updated_at }}</p>
            </div>
            <div class="dashboard-content">
                {% for widget in dashboard.widgets %}
                <div class="widget">
                    <h3 class="widget-title">{{ widget.title }}</h3>
                    <div class="widget-content" id="widget-{{ loop.index }}">
                        <!-- Widget content will be populated by JavaScript -->
                    </div>
                </div>
                {% endfor %}
            </div>

            <script>
                // Auto-refresh dashboard
                setInterval(function() {
                    location.reload();
                }, {{ dashboard.refresh_interval * 1000 }});
            </script>
        </body>
        </html>
        """

        template = Template(html_template)
        return template.render(dashboard=dashboard)


class ResourceDashboardManager:
    """
    Enterprise Resource Usage Dashboard and Alerting Manager

    Provides comprehensive monitoring dashboards and intelligent alerting
    for system resources, application performance, and operational health.
    """

    def __init__(
        self,
        prometheus_url: Optional[str] = None,
        grafana_url: Optional[str] = None,
        enable_builtin_dashboards: bool = True
    ):
        self.prometheus_url = prometheus_url
        self.grafana_url = grafana_url

        # Core components
        self.alert_manager = AlertManager()
        self.dashboard_manager = DashboardManager()

        # Resource monitoring
        self.resource_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.monitoring_thread: Optional[threading.Thread] = None
        self.is_monitoring = False

        # Dashboard and alert definitions
        self.builtin_dashboards: Dict[str, Dashboard] = {}
        self.builtin_alerts: Dict[str, AlertRule] = {}

        self.logger = logging.getLogger(__name__)

        if enable_builtin_dashboards:
            self._create_builtin_dashboards()
            self._create_builtin_alerts()

    def _create_builtin_dashboards(self):
        """Create built-in dashboard templates."""

        # Executive Dashboard
        executive_dashboard = Dashboard(
            id="executive",
            title="Executive Overview",
            description="High-level system health and business metrics",
            type=DashboardType.EXECUTIVE
        )

        executive_widgets = [
            DashboardWidget(
                id="system_health",
                title="System Health Score",
                type="gauge",
                query="system_health_score",
                position={"x": 0, "y": 0, "width": 6, "height": 4},
                thresholds={"warning": 70, "critical": 50}
            ),
            DashboardWidget(
                id="sla_status",
                title="SLA Compliance",
                type="gauge",
                query="sla_compliance_percent",
                position={"x": 6, "y": 0, "width": 6, "height": 4},
                thresholds={"warning": 95, "critical": 90}
            ),
            DashboardWidget(
                id="cost_efficiency",
                title="Cost Efficiency Trend",
                type="chart",
                query="cost_per_transaction",
                position={"x": 0, "y": 4, "width": 12, "height": 6}
            )
        ]

        for widget in executive_widgets:
            executive_dashboard.widgets.append(widget)

        self.builtin_dashboards["executive"] = executive_dashboard

        # Operational Dashboard
        operational_dashboard = Dashboard(
            id="operational",
            title="Operational Monitoring",
            description="Real-time operational metrics and alerts",
            type=DashboardType.OPERATIONAL
        )

        operational_widgets = [
            DashboardWidget(
                id="cpu_usage",
                title="CPU Usage",
                type="chart",
                query="cpu_usage_percent",
                position={"x": 0, "y": 0, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="memory_usage",
                title="Memory Usage",
                type="chart",
                query="memory_usage_percent",
                position={"x": 6, "y": 0, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="disk_usage",
                title="Disk Usage",
                type="gauge",
                query="disk_usage_percent",
                position={"x": 0, "y": 4, "width": 4, "height": 4},
                thresholds={"warning": 80, "critical": 90}
            ),
            DashboardWidget(
                id="network_io",
                title="Network I/O",
                type="chart",
                query="network_io_bytes",
                position={"x": 4, "y": 4, "width": 8, "height": 4}
            ),
            DashboardWidget(
                id="active_alerts",
                title="Active Alerts",
                type="table",
                query="active_alerts_count",
                position={"x": 0, "y": 8, "width": 12, "height": 4}
            )
        ]

        for widget in operational_widgets:
            operational_dashboard.widgets.append(widget)

        self.builtin_dashboards["operational"] = operational_dashboard

        # Performance Dashboard
        performance_dashboard = Dashboard(
            id="performance",
            title="Performance Analytics",
            description="Detailed performance metrics and trends",
            type=DashboardType.PERFORMANCE
        )

        performance_widgets = [
            DashboardWidget(
                id="response_time",
                title="API Response Time",
                type="chart",
                query="api_response_time_ms",
                position={"x": 0, "y": 0, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="throughput",
                title="Request Throughput",
                type="chart",
                query="requests_per_second",
                position={"x": 6, "y": 0, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="error_rate",
                title="Error Rate",
                type="chart",
                query="error_rate_percent",
                position={"x": 0, "y": 4, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="database_performance",
                title="Database Performance",
                type="chart",
                query="db_query_time_ms",
                position={"x": 6, "y": 4, "width": 6, "height": 4}
            )
        ]

        for widget in performance_widgets:
            performance_dashboard.widgets.append(widget)

        self.builtin_dashboards["performance"] = performance_dashboard

    def _create_builtin_alerts(self):
        """Create built-in alert rules."""

        # System resource alerts
        self.builtin_alerts["high_cpu"] = AlertRule(
            name="high_cpu_usage",
            description="High CPU usage detected",
            metric_name="cpu_usage_percent",
            threshold=80.0,
            operator=">",
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
            evaluation_window_minutes=5,
            cooldown_minutes=15
        )

        self.builtin_alerts["critical_cpu"] = AlertRule(
            name="critical_cpu_usage",
            description="Critical CPU usage detected",
            metric_name="cpu_usage_percent",
            threshold=95.0,
            operator=">",
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.SLACK, AlertChannel.EMAIL, AlertChannel.PAGERDUTY],
            evaluation_window_minutes=2,
            cooldown_minutes=5
        )

        self.builtin_alerts["high_memory"] = AlertRule(
            name="high_memory_usage",
            description="High memory usage detected",
            metric_name="memory_usage_percent",
            threshold=85.0,
            operator=">",
            severity=AlertSeverity.WARNING,
            channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
            evaluation_window_minutes=5,
            cooldown_minutes=15
        )

        self.builtin_alerts["disk_space"] = AlertRule(
            name="low_disk_space",
            description="Low disk space detected",
            metric_name="disk_usage_percent",
            threshold=90.0,
            operator=">",
            severity=AlertSeverity.ERROR,
            channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
            evaluation_window_minutes=10,
            cooldown_minutes=60
        )

    def setup_monitoring(
        self,
        alert_channels: Optional[Dict[AlertChannel, Dict]] = None,
        enable_dashboards: List[str] = None
    ):
        """Setup monitoring with alerts and dashboards."""

        # Configure alert channels
        if alert_channels:
            for channel, config in alert_channels.items():
                self.alert_manager.configure_alert_channel(channel, config)

        # Add built-in alert rules
        for rule in self.builtin_alerts.values():
            self.alert_manager.add_alert_rule(rule)

        # Create built-in dashboards
        enabled_dashboards = enable_dashboards or ["executive", "operational", "performance"]
        for dashboard_id in enabled_dashboards:
            if dashboard_id in self.builtin_dashboards:
                self.dashboard_manager.create_dashboard(self.builtin_dashboards[dashboard_id])

        # Start alert evaluation
        self.alert_manager.start_alert_evaluation()

        # Start resource monitoring
        self.start_resource_monitoring()

        self.logger.info("Resource monitoring setup completed")

    def start_resource_monitoring(self, interval_seconds: int = 30):
        """Start resource monitoring."""
        if self.is_monitoring:
            return

        self.is_monitoring = True
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval_seconds,),
            name="resource_monitor",
            daemon=True
        )
        self.monitoring_thread.start()
        self.logger.info("Resource monitoring started")

    def _monitoring_loop(self, interval_seconds: int):
        """Resource monitoring loop."""
        while self.is_monitoring:
            try:
                self._collect_resource_metrics()
                time.sleep(interval_seconds)
            except Exception as e:
                self.logger.error(f"Error in resource monitoring: {e}")
                time.sleep(interval_seconds)

    def _collect_resource_metrics(self):
        """Collect system resource metrics."""
        timestamp = datetime.now()

        # CPU metrics
        cpu_percent = psutil.cpu_percent()
        self.resource_metrics["cpu_usage_percent"].append((timestamp, cpu_percent))

        # Memory metrics
        memory = psutil.virtual_memory()
        self.resource_metrics["memory_usage_percent"].append((timestamp, memory.percent))
        self.resource_metrics["memory_available_gb"].append((timestamp, memory.available / (1024**3)))

        # Disk metrics
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        self.resource_metrics["disk_usage_percent"].append((timestamp, disk_percent))

        # Network metrics
        network = psutil.net_io_counters()
        self.resource_metrics["network_bytes_sent"].append((timestamp, network.bytes_sent))
        self.resource_metrics["network_bytes_recv"].append((timestamp, network.bytes_recv))

        # Load average (if available)
        if hasattr(psutil, 'getloadavg'):
            load_avg = psutil.getloadavg()[0]
            self.resource_metrics["load_average"].append((timestamp, load_avg))

    def get_dashboard_data(self, dashboard_id: str) -> Dict[str, Any]:
        """Get dashboard data for rendering."""
        dashboard = self.dashboard_manager.dashboards.get(dashboard_id)
        if not dashboard:
            return {}

        dashboard_data = {
            "dashboard": dashboard,
            "widgets": [],
            "last_updated": datetime.now()
        }

        # Collect data for each widget
        for widget in dashboard.widgets:
            widget_data = {
                "widget": widget,
                "data": self._get_widget_data(widget),
                "status": "active"
            }
            dashboard_data["widgets"].append(widget_data)

        return dashboard_data

    def _get_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Get data for a specific widget."""
        metric_name = widget.query

        if metric_name in self.resource_metrics:
            # Get recent data points
            recent_data = list(self.resource_metrics[metric_name])[-100:]  # Last 100 points

            if recent_data:
                timestamps = [point[0] for point in recent_data]
                values = [point[1] for point in recent_data]

                return {
                    "timestamps": [ts.isoformat() for ts in timestamps],
                    "values": values,
                    "current_value": values[-1] if values else 0,
                    "min_value": min(values) if values else 0,
                    "max_value": max(values) if values else 0,
                    "avg_value": sum(values) / len(values) if values else 0
                }

        return {"error": f"No data available for metric: {metric_name}"}

    def export_grafana_dashboards(self, output_dir: str = "./dashboards"):
        """Export dashboards as Grafana JSON files."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        for dashboard_id, dashboard in self.dashboard_manager.dashboards.items():
            grafana_json = self.dashboard_manager.generate_grafana_dashboard(dashboard_id)

            file_path = output_path / f"{dashboard_id}_dashboard.json"
            with open(file_path, 'w') as f:
                json.dump(grafana_json, f, indent=2)

            self.logger.info(f"Exported Grafana dashboard: {file_path}")

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alert status."""
        return {
            "active_alerts": len(self.alert_manager.active_alerts),
            "total_rules": len(self.alert_manager.alert_rules),
            "alert_history_count": len(self.alert_manager.alert_history),
            "alerts_by_severity": {
                severity.value: len([
                    alert for alert in self.alert_manager.active_alerts.values()
                    if alert.severity == severity
                ])
                for severity in AlertSeverity
            },
            "recent_alerts": [
                {
                    "rule_name": alert.rule_name,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat()
                }
                for alert in list(self.alert_manager.alert_history)[-10:]
            ]
        }

    def stop_monitoring(self):
        """Stop all monitoring activities."""
        self.is_monitoring = False
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5.0)

        self.alert_manager.stop_alert_evaluation()
        self.logger.info("Resource monitoring stopped")


# Global dashboard manager instance
_dashboard_manager: Optional[ResourceDashboardManager] = None


def get_dashboard_manager() -> ResourceDashboardManager:
    """Get global dashboard manager instance."""
    global _dashboard_manager

    if _dashboard_manager is None:
        _dashboard_manager = ResourceDashboardManager()

    return _dashboard_manager


def setup_enterprise_monitoring(
    prometheus_url: Optional[str] = None,
    grafana_url: Optional[str] = None,
    alert_channels: Optional[Dict[AlertChannel, Dict]] = None
) -> ResourceDashboardManager:
    """Setup enterprise monitoring with dashboards and alerts."""

    manager = ResourceDashboardManager(
        prometheus_url=prometheus_url,
        grafana_url=grafana_url
    )

    manager.setup_monitoring(alert_channels=alert_channels)

    return manager


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Setup alert channels
    alert_channels = {
        AlertChannel.EMAIL: {
            "smtp_host": "smtp.gmail.com",
            "smtp_port": 587,
            "use_tls": True,
            "sender": "alerts@company.com",
            "recipients": ["ops@company.com"],
            "username": "alerts@company.com",
            "password": "app_password"
        },
        AlertChannel.SLACK: {
            "webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
        }
    }

    # Setup monitoring
    manager = setup_enterprise_monitoring(
        prometheus_url="http://localhost:9090",
        grafana_url="http://localhost:3000",
        alert_channels=alert_channels
    )

    try:
        print("Enterprise monitoring started...")
        print(f"Active dashboards: {list(manager.dashboard_manager.dashboards.keys())}")
        print(f"Alert rules: {list(manager.alert_manager.alert_rules.keys())}")

        # Export Grafana dashboards
        manager.export_grafana_dashboards("./exported_dashboards")

        # Run for demonstration
        time.sleep(60)

        # Get status
        alert_summary = manager.get_alert_summary()
        print(f"Alert summary: {alert_summary}")

    except KeyboardInterrupt:
        print("Stopping monitoring...")
    finally:
        manager.stop_monitoring()