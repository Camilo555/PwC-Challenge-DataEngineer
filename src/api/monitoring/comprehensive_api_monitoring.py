"""Comprehensive API Monitoring & Performance Tracking
Enterprise-grade monitoring with real-time metrics, alerting, and performance analytics
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import psutil
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from core.config import get_settings
from core.logging import get_logger, get_structured_logger
from messaging.rabbitmq_manager import RabbitMQManager
from streaming.kafka_manager import KafkaManager

logger = get_logger(__name__)
structured_logger = get_structured_logger(__name__)
settings = get_settings()


class MetricType(Enum):
    """Types of metrics collected"""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    SUMMARY = "summary"


class AlertSeverity(Enum):
    """Alert severity levels"""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class APIMetric:
    """API metric data point"""

    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    labels: dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class AlertRule:
    """Alert rule configuration"""

    name: str
    metric_name: str
    condition: str  # "gt", "lt", "eq", "gte", "lte"
    threshold: float
    severity: AlertSeverity
    duration: int = 60  # seconds
    description: str = ""
    actions: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class Alert:
    """Alert instance"""

    alert_id: str
    rule_name: str
    metric_name: str
    current_value: float
    threshold: float
    severity: AlertSeverity
    message: str
    fired_at: datetime
    resolved_at: datetime | None = None
    labels: dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Collect and aggregate API metrics"""

    def __init__(self):
        self.metrics: dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.counters: dict[str, float] = defaultdict(float)
        self.gauges: dict[str, float] = defaultdict(float)
        self.histograms: dict[str, list[float]] = defaultdict(list)

        # Performance metrics
        self.request_durations = deque(maxlen=10000)
        self.request_counts = defaultdict(int)
        self.error_counts = defaultdict(int)

        # System metrics
        self.system_metrics = {}

        # Start background collection
        asyncio.create_task(self._collect_system_metrics())

    def record_metric(self, metric: APIMetric):
        """Record a metric data point"""
        self.metrics[metric.name].append(metric)

        if metric.metric_type == MetricType.COUNTER:
            self.counters[metric.name] += metric.value
        elif metric.metric_type == MetricType.GAUGE:
            self.gauges[metric.name] = metric.value
        elif metric.metric_type == MetricType.HISTOGRAM:
            self.histograms[metric.name].append(metric.value)
            # Keep only last 1000 values for histograms
            if len(self.histograms[metric.name]) > 1000:
                self.histograms[metric.name] = self.histograms[metric.name][-1000:]

    def record_request(
        self, method: str, path: str, status_code: int, duration: float, size: int = 0
    ):
        """Record API request metrics"""
        labels = {
            "method": method,
            "path": path,
            "status": str(status_code),
            "status_class": f"{status_code // 100}xx",
        }

        # Request count
        self.record_metric(
            APIMetric(
                name="http_requests_total",
                value=1,
                metric_type=MetricType.COUNTER,
                labels=labels,
                description="Total HTTP requests",
            )
        )

        # Request duration
        self.record_metric(
            APIMetric(
                name="http_request_duration_seconds",
                value=duration,
                metric_type=MetricType.HISTOGRAM,
                labels=labels,
                description="HTTP request duration in seconds",
            )
        )

        # Response size
        if size > 0:
            self.record_metric(
                APIMetric(
                    name="http_response_size_bytes",
                    value=size,
                    metric_type=MetricType.HISTOGRAM,
                    labels=labels,
                    description="HTTP response size in bytes",
                )
            )

        # Error tracking
        if status_code >= 400:
            self.record_metric(
                APIMetric(
                    name="http_errors_total",
                    value=1,
                    metric_type=MetricType.COUNTER,
                    labels=labels,
                    description="Total HTTP errors",
                )
            )

    async def _collect_system_metrics(self):
        """Collect system performance metrics"""
        while True:
            try:
                # CPU metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                self.record_metric(
                    APIMetric(
                        name="system_cpu_usage_percent",
                        value=cpu_percent,
                        metric_type=MetricType.GAUGE,
                        description="System CPU usage percentage",
                    )
                )

                # Memory metrics
                memory = psutil.virtual_memory()
                self.record_metric(
                    APIMetric(
                        name="system_memory_usage_percent",
                        value=memory.percent,
                        metric_type=MetricType.GAUGE,
                        description="System memory usage percentage",
                    )
                )

                self.record_metric(
                    APIMetric(
                        name="system_memory_available_bytes",
                        value=memory.available,
                        metric_type=MetricType.GAUGE,
                        description="System available memory in bytes",
                    )
                )

                # Disk metrics
                disk = psutil.disk_usage("/")
                self.record_metric(
                    APIMetric(
                        name="system_disk_usage_percent",
                        value=disk.percent,
                        metric_type=MetricType.GAUGE,
                        description="System disk usage percentage",
                    )
                )

                # Network metrics
                network = psutil.net_io_counters()
                self.record_metric(
                    APIMetric(
                        name="system_network_bytes_sent",
                        value=network.bytes_sent,
                        metric_type=MetricType.COUNTER,
                        description="System network bytes sent",
                    )
                )

                self.record_metric(
                    APIMetric(
                        name="system_network_bytes_recv",
                        value=network.bytes_recv,
                        metric_type=MetricType.COUNTER,
                        description="System network bytes received",
                    )
                )

                await asyncio.sleep(30)  # Collect every 30 seconds

            except Exception as e:
                logger.error(f"System metrics collection error: {e}")
                await asyncio.sleep(60)

    def get_metric_summary(self, metric_name: str, duration_minutes: int = 5) -> dict[str, Any]:
        """Get metric summary for specified duration"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=duration_minutes)

        recent_metrics = [m for m in self.metrics[metric_name] if m.timestamp > cutoff_time]

        if not recent_metrics:
            return {}

        values = [m.value for m in recent_metrics]

        return {
            "name": metric_name,
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "p50": self._percentile(values, 50),
            "p95": self._percentile(values, 95),
            "p99": self._percentile(values, 99),
            "total": sum(values),
            "rate_per_minute": len(values) / duration_minutes,
        }

    def _percentile(self, values: list[float], percentile: int) -> float:
        """Calculate percentile from list of values"""
        if not values:
            return 0.0

        sorted_values = sorted(values)
        index = int((percentile / 100) * len(sorted_values))
        return sorted_values[min(index, len(sorted_values) - 1)]

    def get_all_metrics_summary(self) -> dict[str, Any]:
        """Get summary of all collected metrics"""
        summary = {"timestamp": datetime.utcnow().isoformat(), "metrics": {}, "system": {}}

        # HTTP request metrics
        for duration in [1, 5, 15]:
            summary["metrics"][f"requests_last_{duration}m"] = self.get_metric_summary(
                "http_requests_total", duration
            )
            summary["metrics"][f"duration_last_{duration}m"] = self.get_metric_summary(
                "http_request_duration_seconds", duration
            )
            summary["metrics"][f"errors_last_{duration}m"] = self.get_metric_summary(
                "http_errors_total", duration
            )

        # System metrics (current values)
        for metric_name in [
            "system_cpu_usage_percent",
            "system_memory_usage_percent",
            "system_disk_usage_percent",
        ]:
            if metric_name in self.metrics and self.metrics[metric_name]:
                latest = self.metrics[metric_name][-1]
                summary["system"][metric_name] = latest.value

        return summary


class AlertManager:
    """Manage alerts based on metric thresholds"""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.alert_rules: list[AlertRule] = []
        self.active_alerts: dict[str, Alert] = {}
        self.alert_history: list[Alert] = []

        # Messaging for alert notifications
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()

        # Start alert evaluation loop
        asyncio.create_task(self._evaluate_alerts_loop())

    def add_alert_rule(self, rule: AlertRule):
        """Add alert rule"""
        self.alert_rules.append(rule)
        logger.info(f"Added alert rule: {rule.name}")

    async def _evaluate_alerts_loop(self):
        """Continuously evaluate alert conditions"""
        while True:
            try:
                for rule in self.alert_rules:
                    if rule.enabled:
                        await self._evaluate_rule(rule)

                await asyncio.sleep(30)  # Evaluate every 30 seconds

            except Exception as e:
                logger.error(f"Alert evaluation error: {e}")
                await asyncio.sleep(60)

    async def _evaluate_rule(self, rule: AlertRule):
        """Evaluate single alert rule"""
        try:
            # Get recent metric values
            cutoff_time = datetime.utcnow() - timedelta(seconds=rule.duration)

            if rule.metric_name not in self.metrics_collector.metrics:
                return

            recent_metrics = [
                m
                for m in self.metrics_collector.metrics[rule.metric_name]
                if m.timestamp > cutoff_time
            ]

            if not recent_metrics:
                return

            # Calculate current value (average for the duration)
            current_value = sum(m.value for m in recent_metrics) / len(recent_metrics)

            # Evaluate condition
            is_triggered = self._evaluate_condition(rule.condition, current_value, rule.threshold)

            alert_key = f"{rule.name}:{rule.metric_name}"

            if is_triggered and alert_key not in self.active_alerts:
                # Fire new alert
                alert = Alert(
                    alert_id=f"{alert_key}:{int(time.time())}",
                    rule_name=rule.name,
                    metric_name=rule.metric_name,
                    current_value=current_value,
                    threshold=rule.threshold,
                    severity=rule.severity,
                    message=f"{rule.description or rule.name}: {current_value:.2f} {rule.condition} {rule.threshold}",
                    fired_at=datetime.utcnow(),
                )

                self.active_alerts[alert_key] = alert
                self.alert_history.append(alert)

                await self._send_alert_notification(alert, "fired")

                logger.warning(
                    f"Alert fired: {alert.message}",
                    extra={
                        "alert_id": alert.alert_id,
                        "rule_name": rule.name,
                        "metric_name": rule.metric_name,
                        "current_value": current_value,
                        "threshold": rule.threshold,
                        "severity": rule.severity.value,
                    },
                )

            elif not is_triggered and alert_key in self.active_alerts:
                # Resolve alert
                alert = self.active_alerts[alert_key]
                alert.resolved_at = datetime.utcnow()

                del self.active_alerts[alert_key]

                await self._send_alert_notification(alert, "resolved")

                logger.info(
                    f"Alert resolved: {alert.message}",
                    extra={
                        "alert_id": alert.alert_id,
                        "resolved_at": alert.resolved_at.isoformat(),
                    },
                )

        except Exception as e:
            logger.error(f"Rule evaluation error for {rule.name}: {e}")

    def _evaluate_condition(self, condition: str, current_value: float, threshold: float) -> bool:
        """Evaluate alert condition"""
        if condition == "gt":
            return current_value > threshold
        elif condition == "lt":
            return current_value < threshold
        elif condition == "eq":
            return abs(current_value - threshold) < 0.001  # Float comparison
        elif condition == "gte":
            return current_value >= threshold
        elif condition == "lte":
            return current_value <= threshold
        else:
            return False

    async def _send_alert_notification(self, alert: Alert, action: str):
        """Send alert notification"""
        try:
            # Publish to RabbitMQ for immediate processing
            self.rabbitmq_manager.publish_event(
                event_type="alert",
                data={
                    "alert_id": alert.alert_id,
                    "rule_name": alert.rule_name,
                    "metric_name": alert.metric_name,
                    "current_value": alert.current_value,
                    "threshold": alert.threshold,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "action": action,
                    "fired_at": alert.fired_at.isoformat(),
                    "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
                },
            )

            # Publish to Kafka for long-term storage and analysis
            self.kafka_manager.produce_alert_event(
                alert_id=alert.alert_id,
                rule_name=alert.rule_name,
                severity=alert.severity.value,
                action=action,
                metadata={
                    "metric_name": alert.metric_name,
                    "current_value": alert.current_value,
                    "threshold": alert.threshold,
                    "message": alert.message,
                },
            )

        except Exception as e:
            logger.error(f"Failed to send alert notification: {e}")

    def get_alert_status(self) -> dict[str, Any]:
        """Get current alert status"""
        return {
            "active_alerts": len(self.active_alerts),
            "total_rules": len(self.alert_rules),
            "enabled_rules": len([r for r in self.alert_rules if r.enabled]),
            "alerts_by_severity": {
                severity.value: len(
                    [a for a in self.active_alerts.values() if a.severity == severity]
                )
                for severity in AlertSeverity
            },
            "recent_alerts": [
                {
                    "alert_id": alert.alert_id,
                    "rule_name": alert.rule_name,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "fired_at": alert.fired_at.isoformat(),
                    "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
                }
                for alert in sorted(
                    self.alert_history[-10:], key=lambda a: a.fired_at, reverse=True
                )
            ],
        }


class PerformanceAnalyzer:
    """Analyze API performance patterns and trends"""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.analysis_cache = {}
        self.cache_ttl = 300  # 5 minutes

    def analyze_endpoint_performance(self, endpoint_pattern: str = None) -> dict[str, Any]:
        """Analyze performance for specific endpoint or all endpoints"""
        cache_key = f"endpoint_performance:{endpoint_pattern or 'all'}"

        # Check cache
        if cache_key in self.analysis_cache:
            cached_result, timestamp = self.analysis_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return cached_result

        analysis = {
            "timestamp": datetime.utcnow().isoformat(),
            "endpoint_pattern": endpoint_pattern,
            "endpoints": {},
            "overall": {},
        }

        # Analyze request duration metrics
        duration_metrics = [
            m
            for m in self.metrics_collector.metrics.get("http_request_duration_seconds", [])
            if not endpoint_pattern or endpoint_pattern in m.labels.get("path", "")
        ]

        if duration_metrics:
            # Group by endpoint
            endpoint_groups = defaultdict(list)
            for metric in duration_metrics:
                endpoint_key = (
                    f"{metric.labels.get('method', 'GET')} {metric.labels.get('path', '/')}"
                )
                endpoint_groups[endpoint_key].append(metric.value)

            # Analyze each endpoint
            for endpoint, durations in endpoint_groups.items():
                analysis["endpoints"][endpoint] = {
                    "request_count": len(durations),
                    "avg_duration": sum(durations) / len(durations),
                    "min_duration": min(durations),
                    "max_duration": max(durations),
                    "p50_duration": self.metrics_collector._percentile(durations, 50),
                    "p95_duration": self.metrics_collector._percentile(durations, 95),
                    "p99_duration": self.metrics_collector._percentile(durations, 99),
                }

            # Overall statistics
            all_durations = [d for durations in endpoint_groups.values() for d in durations]
            analysis["overall"] = {
                "total_requests": len(all_durations),
                "avg_duration": sum(all_durations) / len(all_durations),
                "p95_duration": self.metrics_collector._percentile(all_durations, 95),
                "p99_duration": self.metrics_collector._percentile(all_durations, 99),
            }

        # Cache result
        self.analysis_cache[cache_key] = (analysis, time.time())

        return analysis

    def analyze_error_patterns(self) -> dict[str, Any]:
        """Analyze error patterns and trends"""
        cache_key = "error_patterns"

        # Check cache
        if cache_key in self.analysis_cache:
            cached_result, timestamp = self.analysis_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return cached_result

        error_metrics = self.metrics_collector.metrics.get("http_errors_total", [])

        analysis = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_errors": len(error_metrics),
            "errors_by_status": defaultdict(int),
            "errors_by_endpoint": defaultdict(int),
            "error_rate": 0.0,
        }

        if error_metrics:
            # Group errors by status code and endpoint
            for metric in error_metrics:
                status = metric.labels.get("status", "unknown")
                endpoint = f"{metric.labels.get('method', 'GET')} {metric.labels.get('path', '/')}"

                analysis["errors_by_status"][status] += 1
                analysis["errors_by_endpoint"][endpoint] += 1

            # Calculate error rate
            total_requests = len(self.metrics_collector.metrics.get("http_requests_total", []))
            if total_requests > 0:
                analysis["error_rate"] = len(error_metrics) / total_requests * 100

        # Convert defaultdicts to regular dicts for JSON serialization
        analysis["errors_by_status"] = dict(analysis["errors_by_status"])
        analysis["errors_by_endpoint"] = dict(analysis["errors_by_endpoint"])

        # Cache result
        self.analysis_cache[cache_key] = (analysis, time.time())

        return analysis

    def get_performance_trends(self, hours: int = 24) -> dict[str, Any]:
        """Get performance trends over specified time period"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        # Get metrics for the time period
        duration_metrics = [
            m
            for m in self.metrics_collector.metrics.get("http_request_duration_seconds", [])
            if m.timestamp > cutoff_time
        ]

        request_metrics = [
            m
            for m in self.metrics_collector.metrics.get("http_requests_total", [])
            if m.timestamp > cutoff_time
        ]

        # Group by hour
        hourly_data = defaultdict(lambda: {"durations": [], "requests": 0})

        for metric in duration_metrics:
            hour_key = metric.timestamp.replace(minute=0, second=0, microsecond=0)
            hourly_data[hour_key]["durations"].append(metric.value)

        for metric in request_metrics:
            hour_key = metric.timestamp.replace(minute=0, second=0, microsecond=0)
            hourly_data[hour_key]["requests"] += 1

        # Calculate hourly statistics
        trends = []
        for hour, data in sorted(hourly_data.items()):
            hour_stats = {
                "hour": hour.isoformat(),
                "request_count": data["requests"],
                "avg_duration": sum(data["durations"]) / len(data["durations"])
                if data["durations"]
                else 0,
                "p95_duration": self.metrics_collector._percentile(data["durations"], 95)
                if data["durations"]
                else 0,
            }
            trends.append(hour_stats)

        return {
            "period_hours": hours,
            "trends": trends,
            "summary": {
                "total_hours": len(trends),
                "avg_requests_per_hour": sum(t["request_count"] for t in trends) / len(trends)
                if trends
                else 0,
                "avg_response_time": sum(t["avg_duration"] for t in trends) / len(trends)
                if trends
                else 0,
            },
        }


class MonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware to collect API monitoring metrics"""

    def __init__(self, app, metrics_collector: MetricsCollector):
        super().__init__(app)
        self.metrics_collector = metrics_collector

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.time()

        # Extract request information
        method = request.method
        path = str(request.url.path)

        # Normalize path for metrics (remove IDs and parameters)
        normalized_path = self._normalize_path(path)

        try:
            response = await call_next(request)

            # Calculate metrics
            duration = time.time() - start_time
            status_code = response.status_code
            response_size = len(response.body) if hasattr(response, "body") else 0

            # Record metrics
            self.metrics_collector.record_request(
                method=method,
                path=normalized_path,
                status_code=status_code,
                duration=duration,
                size=response_size,
            )

            # Add performance headers
            response.headers["X-Response-Time"] = f"{duration:.3f}"
            response.headers["X-API-Monitor"] = "enabled"

            return response

        except Exception as e:
            # Record error metrics
            duration = time.time() - start_time
            self.metrics_collector.record_request(
                method=method, path=normalized_path, status_code=500, duration=duration
            )

            logger.error(
                f"Request processing error: {e}",
                extra={"method": method, "path": path, "duration": duration, "error": str(e)},
            )

            raise

    def _normalize_path(self, path: str) -> str:
        """Normalize path for consistent metrics collection"""
        import re

        # Replace UUIDs with placeholder
        path = re.sub(
            r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "/{uuid}", path
        )

        # Replace numeric IDs with placeholder
        path = re.sub(r"/\d+", "/{id}", path)

        # Remove query parameters
        path = path.split("?")[0]

        return path


class APIMonitoringDashboard:
    """API monitoring dashboard with real-time metrics"""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        alert_manager: AlertManager,
        performance_analyzer: PerformanceAnalyzer,
    ):
        self.metrics_collector = metrics_collector
        self.alert_manager = alert_manager
        self.performance_analyzer = performance_analyzer

    def get_dashboard_data(self) -> dict[str, Any]:
        """Get comprehensive dashboard data"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics_summary": self.metrics_collector.get_all_metrics_summary(),
            "alert_status": self.alert_manager.get_alert_status(),
            "performance_analysis": self.performance_analyzer.analyze_endpoint_performance(),
            "error_analysis": self.performance_analyzer.analyze_error_patterns(),
            "performance_trends": self.performance_analyzer.get_performance_trends(hours=4),
        }

    def get_health_score(self) -> dict[str, Any]:
        """Calculate overall API health score"""
        metrics_summary = self.metrics_collector.get_all_metrics_summary()
        alert_status = self.alert_manager.get_alert_status()
        error_analysis = self.performance_analyzer.analyze_error_patterns()

        # Calculate health components (0-100 scale)
        availability_score = 100  # Start with perfect score
        performance_score = 100
        error_score = 100

        # Availability: Based on active alerts
        critical_alerts = alert_status["alerts_by_severity"].get("critical", 0)
        high_alerts = alert_status["alerts_by_severity"].get("high", 0)

        availability_score -= critical_alerts * 30  # -30 points per critical alert
        availability_score -= high_alerts * 15  # -15 points per high alert
        availability_score = max(0, availability_score)

        # Performance: Based on response times
        recent_duration = metrics_summary["metrics"].get("duration_last_5m", {})
        if recent_duration.get("p95"):
            p95_duration = recent_duration["p95"]
            if p95_duration > 2.0:  # > 2 seconds
                performance_score -= 40
            elif p95_duration > 1.0:  # > 1 second
                performance_score -= 20
            elif p95_duration > 0.5:  # > 500ms
                performance_score -= 10

        # Error rate
        error_rate = error_analysis.get("error_rate", 0)
        if error_rate > 5:  # > 5% error rate
            error_score -= 50
        elif error_rate > 2:  # > 2% error rate
            error_score -= 25
        elif error_rate > 1:  # > 1% error rate
            error_score -= 10

        error_score = max(0, error_score)

        # Overall health score (weighted average)
        overall_score = (
            availability_score * 0.4  # 40% weight
            + performance_score * 0.3  # 30% weight
            + error_score * 0.3  # 30% weight
        )

        # Determine health status
        if overall_score >= 90:
            health_status = "excellent"
        elif overall_score >= 75:
            health_status = "good"
        elif overall_score >= 50:
            health_status = "fair"
        elif overall_score >= 25:
            health_status = "poor"
        else:
            health_status = "critical"

        return {
            "overall_score": round(overall_score, 1),
            "health_status": health_status,
            "component_scores": {
                "availability": round(availability_score, 1),
                "performance": round(performance_score, 1),
                "error_rate": round(error_score, 1),
            },
            "recommendations": self._generate_health_recommendations(
                availability_score, performance_score, error_score, alert_status, error_analysis
            ),
        }

    def _generate_health_recommendations(
        self,
        availability_score: float,
        performance_score: float,
        error_score: float,
        alert_status: dict,
        error_analysis: dict,
    ) -> list[str]:
        """Generate health improvement recommendations"""
        recommendations = []

        if availability_score < 80:
            recommendations.append("Investigate and resolve active critical/high severity alerts")

        if performance_score < 70:
            recommendations.append(
                "Optimize slow endpoints - consider caching, database indexing, or code optimization"
            )

        if error_score < 70:
            error_rate = error_analysis.get("error_rate", 0)
            if error_rate > 5:
                recommendations.append(
                    f"High error rate ({error_rate:.1f}%) - review error logs and fix common issues"
                )

        if not recommendations:
            recommendations.append("API health is good - continue monitoring")

        return recommendations


# Factory function to create monitoring system
def create_comprehensive_monitoring() -> tuple[
    MetricsCollector, AlertManager, PerformanceAnalyzer, APIMonitoringDashboard
]:
    """Create comprehensive monitoring system"""

    # Create core components
    metrics_collector = MetricsCollector()
    alert_manager = AlertManager(metrics_collector)
    performance_analyzer = PerformanceAnalyzer(metrics_collector)
    dashboard = APIMonitoringDashboard(metrics_collector, alert_manager, performance_analyzer)

    # Add default alert rules
    default_alert_rules = [
        AlertRule(
            name="High Response Time",
            metric_name="http_request_duration_seconds",
            condition="gt",
            threshold=2.0,
            severity=AlertSeverity.HIGH,
            description="API response time is too high",
        ),
        AlertRule(
            name="High Error Rate",
            metric_name="http_errors_total",
            condition="gt",
            threshold=10,
            severity=AlertSeverity.CRITICAL,
            description="API error rate is too high",
        ),
        AlertRule(
            name="High CPU Usage",
            metric_name="system_cpu_usage_percent",
            condition="gt",
            threshold=80,
            severity=AlertSeverity.HIGH,
            description="System CPU usage is too high",
        ),
        AlertRule(
            name="High Memory Usage",
            metric_name="system_memory_usage_percent",
            condition="gt",
            threshold=85,
            severity=AlertSeverity.HIGH,
            description="System memory usage is too high",
        ),
        AlertRule(
            name="Low Disk Space",
            metric_name="system_disk_usage_percent",
            condition="gt",
            threshold=90,
            severity=AlertSeverity.CRITICAL,
            description="System disk space is running low",
        ),
    ]

    for rule in default_alert_rules:
        alert_manager.add_alert_rule(rule)

    return metrics_collector, alert_manager, performance_analyzer, dashboard


if __name__ == "__main__":
    # Example usage
    async def main():
        # Create monitoring system
        metrics, alerts, analyzer, dashboard = create_comprehensive_monitoring()

        # Simulate some metrics
        for i in range(10):
            metrics.record_request("GET", "/api/v1/sales", 200, 0.1 + i * 0.05, 1024)
            await asyncio.sleep(1)

        # Get dashboard data
        data = dashboard.get_dashboard_data()
        health = dashboard.get_health_score()

        print("Dashboard Data:", json.dumps(data, indent=2, default=str))
        print("Health Score:", json.dumps(health, indent=2))

    asyncio.run(main())
