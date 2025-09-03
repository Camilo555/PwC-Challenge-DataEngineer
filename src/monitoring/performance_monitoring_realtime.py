"""
Real-Time Performance Monitoring with Automated Response
Provides comprehensive real-time performance monitoring, anomaly detection,
automated response workflows, and intelligent capacity management.
"""

import asyncio
import json
import statistics
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import psutil

from core.distributed_tracing import get_tracing_manager
from core.logging import get_logger
from core.structured_logging import LogContext, StructuredLogger, log_context
from monitoring.advanced_alerting_sla import get_alerting_sla
from monitoring.datadog_custom_metrics_advanced import get_custom_metrics_advanced

logger = get_logger(__name__)
structured_logger = StructuredLogger.get_logger(__name__)


class PerformanceMetricType(Enum):
    """Types of performance metrics."""

    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    RESOURCE_USAGE = "resource_usage"
    QUEUE_DEPTH = "queue_depth"
    CONNECTION_COUNT = "connection_count"
    CACHE_HIT_RATE = "cache_hit_rate"
    CUSTOM = "custom"


class ResponseAction(Enum):
    """Automated response actions."""

    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    RESTART_SERVICE = "restart_service"
    CIRCUIT_BREAKER = "circuit_breaker"
    THROTTLE = "throttle"
    ALERT_ONLY = "alert_only"
    CUSTOM_WEBHOOK = "custom_webhook"
    FAILOVER = "failover"
    CACHE_FLUSH = "cache_flush"
    CONNECTION_POOL_RESET = "connection_pool_reset"


class AnomalyType(Enum):
    """Types of performance anomalies."""

    SPIKE = "spike"
    DIP = "dip"
    TREND_CHANGE = "trend_change"
    OSCILLATION = "oscillation"
    FLATLINE = "flatline"
    OUTLIER = "outlier"


@dataclass
class PerformanceThreshold:
    """Performance threshold configuration."""

    metric_name: str
    metric_type: PerformanceMetricType
    warning_threshold: float
    critical_threshold: float
    evaluation_window_seconds: int = 300
    consecutive_violations: int = 3
    recovery_threshold: float | None = None
    recovery_window_seconds: int = 600
    enabled: bool = True
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class AutomatedResponse:
    """Automated response configuration."""

    response_id: str
    trigger_metric: str
    trigger_threshold: float
    action: ResponseAction
    parameters: dict[str, Any] = field(default_factory=dict)
    cooldown_minutes: int = 15
    max_executions_per_hour: int = 3
    requires_confirmation: bool = False
    enabled: bool = True
    execution_timeout_seconds: int = 300


@dataclass
class PerformanceDataPoint:
    """Performance data point."""

    metric_name: str
    value: float
    timestamp: datetime
    dimensions: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceAnomaly:
    """Detected performance anomaly."""

    anomaly_id: str
    metric_name: str
    anomaly_type: AnomalyType
    severity: str
    detected_at: datetime
    value: float
    expected_range: tuple[float, float]
    confidence: float
    description: str
    suggested_actions: list[str] = field(default_factory=list)
    resolved_at: datetime | None = None


@dataclass
class ResponseExecution:
    """Automated response execution record."""

    execution_id: str
    response_id: str
    triggered_by: str
    executed_at: datetime
    action: ResponseAction
    parameters: dict[str, Any]
    status: str  # pending, running, completed, failed, cancelled
    result: dict[str, Any] | None = None
    error: str | None = None
    duration_ms: float | None = None


class AnomalyDetector:
    """Advanced anomaly detection for performance metrics."""

    def __init__(self):
        self.logger = get_logger(f"{__name__}.anomaly_detector")
        self.metric_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.baselines: dict[str, dict[str, float]] = {}
        self.learning_window_hours = 24

    def add_data_point(self, metric_name: str, value: float, timestamp: datetime = None):
        """Add data point for anomaly detection."""
        if timestamp is None:
            timestamp = datetime.utcnow()

        data_point = PerformanceDataPoint(metric_name=metric_name, value=value, timestamp=timestamp)

        self.metric_history[metric_name].append(data_point)
        self._update_baseline(metric_name)

    def _update_baseline(self, metric_name: str):
        """Update baseline statistics for a metric."""
        history = list(self.metric_history[metric_name])
        if len(history) < 10:
            return

        # Use recent data for baseline (last 24 hours or 100 points)
        cutoff_time = datetime.utcnow() - timedelta(hours=self.learning_window_hours)
        recent_values = [dp.value for dp in history if dp.timestamp >= cutoff_time][
            -100:
        ]  # Last 100 points within time window

        if len(recent_values) < 10:
            recent_values = [dp.value for dp in history[-50:]]  # Fallback to last 50

        if recent_values:
            self.baselines[metric_name] = {
                "mean": statistics.mean(recent_values),
                "std": statistics.stdev(recent_values) if len(recent_values) > 1 else 0,
                "min": min(recent_values),
                "max": max(recent_values),
                "p95": sorted(recent_values)[int(len(recent_values) * 0.95)],
                "p05": sorted(recent_values)[int(len(recent_values) * 0.05)],
                "updated_at": datetime.utcnow(),
            }

    def detect_anomalies(self, metric_name: str, current_value: float) -> list[PerformanceAnomaly]:
        """Detect anomalies in current value."""
        anomalies = []

        if metric_name not in self.baselines:
            return anomalies

        baseline = self.baselines[metric_name]
        mean = baseline["mean"]
        std = baseline["std"]

        if std == 0:  # No variance in data
            return anomalies

        # Z-score based detection
        abs(current_value - mean) / std

        # Multiple detection methods
        anomalies.extend(self._detect_statistical_anomalies(metric_name, current_value, baseline))
        anomalies.extend(self._detect_trend_anomalies(metric_name, current_value))
        anomalies.extend(self._detect_pattern_anomalies(metric_name, current_value))

        return anomalies

    def _detect_statistical_anomalies(
        self, metric_name: str, value: float, baseline: dict[str, float]
    ) -> list[PerformanceAnomaly]:
        """Detect statistical anomalies using z-score and IQR methods."""
        anomalies = []
        mean = baseline["mean"]
        std = baseline["std"]

        # Z-score based detection (3 sigma rule)
        z_score = abs(value - mean) / std if std > 0 else 0

        if z_score > 3:
            anomaly_type = AnomalyType.SPIKE if value > mean else AnomalyType.DIP
            severity = "critical" if z_score > 4 else "warning"

            anomaly = PerformanceAnomaly(
                anomaly_id=f"{metric_name}_{int(time.time())}",
                metric_name=metric_name,
                anomaly_type=anomaly_type,
                severity=severity,
                detected_at=datetime.utcnow(),
                value=value,
                expected_range=(baseline["p05"], baseline["p95"]),
                confidence=min(1.0, (z_score - 3) / 2),
                description=f"{anomaly_type.value.title()} detected: value {value:.2f} is {z_score:.2f} standard deviations from mean {mean:.2f}",
                suggested_actions=self._get_suggested_actions(metric_name, anomaly_type, severity),
            )
            anomalies.append(anomaly)

        return anomalies

    def _detect_trend_anomalies(
        self, metric_name: str, current_value: float
    ) -> list[PerformanceAnomaly]:
        """Detect trend-based anomalies."""
        anomalies = []
        history = list(self.metric_history[metric_name])

        if len(history) < 20:
            return anomalies

        # Check for sudden trend changes
        recent_values = [dp.value for dp in history[-10:]]
        older_values = [dp.value for dp in history[-20:-10]]

        if len(recent_values) >= 5 and len(older_values) >= 5:
            recent_mean = statistics.mean(recent_values)
            older_mean = statistics.mean(older_values)

            # Significant trend change (>50% change)
            if abs(recent_mean - older_mean) / older_mean > 0.5:
                anomaly_type = AnomalyType.TREND_CHANGE
                severity = "warning"

                anomaly = PerformanceAnomaly(
                    anomaly_id=f"{metric_name}_trend_{int(time.time())}",
                    metric_name=metric_name,
                    anomaly_type=anomaly_type,
                    severity=severity,
                    detected_at=datetime.utcnow(),
                    value=current_value,
                    expected_range=(min(older_values), max(older_values)),
                    confidence=0.8,
                    description=f"Trend change detected: mean changed from {older_mean:.2f} to {recent_mean:.2f}",
                    suggested_actions=self._get_suggested_actions(
                        metric_name, anomaly_type, severity
                    ),
                )
                anomalies.append(anomaly)

        return anomalies

    def _detect_pattern_anomalies(
        self, metric_name: str, current_value: float
    ) -> list[PerformanceAnomaly]:
        """Detect pattern-based anomalies like flatlines and oscillations."""
        anomalies = []
        history = list(self.metric_history[metric_name])

        if len(history) < 15:
            return anomalies

        recent_values = [dp.value for dp in history[-15:]]

        # Flatline detection (very low variance)
        if len(recent_values) >= 10:
            variance = statistics.variance(recent_values) if len(recent_values) > 1 else 0
            mean_val = statistics.mean(recent_values)

            # Coefficient of variation < 1% indicates potential flatline
            if mean_val != 0 and (variance**0.5) / abs(mean_val) < 0.01:
                anomaly = PerformanceAnomaly(
                    anomaly_id=f"{metric_name}_flatline_{int(time.time())}",
                    metric_name=metric_name,
                    anomaly_type=AnomalyType.FLATLINE,
                    severity="warning",
                    detected_at=datetime.utcnow(),
                    value=current_value,
                    expected_range=(mean_val * 0.9, mean_val * 1.1),
                    confidence=0.7,
                    description=f"Flatline detected: metric has very low variance ({variance:.4f})",
                    suggested_actions=self._get_suggested_actions(
                        metric_name, AnomalyType.FLATLINE, "warning"
                    ),
                )
                anomalies.append(anomaly)

        return anomalies

    def _get_suggested_actions(
        self, metric_name: str, anomaly_type: AnomalyType, severity: str
    ) -> list[str]:
        """Get suggested actions for detected anomaly."""
        actions = []

        if "latency" in metric_name.lower() or "response_time" in metric_name.lower():
            if anomaly_type == AnomalyType.SPIKE:
                actions.extend(
                    [
                        "Check system resource utilization",
                        "Review database query performance",
                        "Consider scaling up resources",
                        "Investigate external dependencies",
                    ]
                )
        elif "throughput" in metric_name.lower() or "requests" in metric_name.lower():
            if anomaly_type == AnomalyType.DIP:
                actions.extend(
                    [
                        "Check upstream traffic sources",
                        "Verify service availability",
                        "Review load balancer configuration",
                        "Check for circuit breaker activation",
                    ]
                )
        elif "error" in metric_name.lower():
            if anomaly_type == AnomalyType.SPIKE:
                actions.extend(
                    [
                        "Review application logs for error details",
                        "Check recent deployments",
                        "Verify external service dependencies",
                        "Consider implementing circuit breakers",
                    ]
                )
        elif "memory" in metric_name.lower() or "cpu" in metric_name.lower():
            if anomaly_type == AnomalyType.SPIKE:
                actions.extend(
                    [
                        "Investigate memory leaks",
                        "Review resource-intensive processes",
                        "Consider vertical scaling",
                        "Optimize algorithms and queries",
                    ]
                )

        # General actions based on anomaly type
        if anomaly_type == AnomalyType.FLATLINE:
            actions.append("Check if metric collection is working properly")
            actions.append("Verify service is processing requests")
        elif anomaly_type == AnomalyType.TREND_CHANGE:
            actions.append("Review recent configuration changes")
            actions.append("Check for capacity planning adjustments")

        return actions


class AutomatedResponseExecutor:
    """Executes automated responses to performance issues."""

    def __init__(self):
        self.logger = get_logger(f"{__name__}.response_executor")
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.execution_history: deque = deque(maxlen=1000)
        self.cooldown_tracking: dict[str, datetime] = {}
        self.execution_counts: dict[str, list[datetime]] = defaultdict(list)

    async def execute_response(
        self, response: AutomatedResponse, trigger_context: dict[str, Any]
    ) -> ResponseExecution:
        """Execute an automated response."""
        execution_id = f"{response.response_id}_{int(time.time())}"

        execution = ResponseExecution(
            execution_id=execution_id,
            response_id=response.response_id,
            triggered_by=trigger_context.get("metric_name", "unknown"),
            executed_at=datetime.utcnow(),
            action=response.action,
            parameters=response.parameters,
            status="pending",
        )

        try:
            # Check cooldown period
            if not self._check_cooldown(response.response_id, response.cooldown_minutes):
                execution.status = "cancelled"
                execution.error = "Response in cooldown period"
                self.execution_history.append(execution)
                return execution

            # Check execution limits
            if not self._check_execution_limits(
                response.response_id, response.max_executions_per_hour
            ):
                execution.status = "cancelled"
                execution.error = "Execution limit reached"
                self.execution_history.append(execution)
                return execution

            execution.status = "running"
            start_time = time.time()

            # Execute the response action
            if response.action == ResponseAction.SCALE_UP:
                result = await self._execute_scale_up(response.parameters, trigger_context)
            elif response.action == ResponseAction.SCALE_DOWN:
                result = await self._execute_scale_down(response.parameters, trigger_context)
            elif response.action == ResponseAction.RESTART_SERVICE:
                result = await self._execute_restart_service(response.parameters, trigger_context)
            elif response.action == ResponseAction.CIRCUIT_BREAKER:
                result = await self._execute_circuit_breaker(response.parameters, trigger_context)
            elif response.action == ResponseAction.THROTTLE:
                result = await self._execute_throttle(response.parameters, trigger_context)
            elif response.action == ResponseAction.CUSTOM_WEBHOOK:
                result = await self._execute_webhook(response.parameters, trigger_context)
            elif response.action == ResponseAction.FAILOVER:
                result = await self._execute_failover(response.parameters, trigger_context)
            elif response.action == ResponseAction.CACHE_FLUSH:
                result = await self._execute_cache_flush(response.parameters, trigger_context)
            elif response.action == ResponseAction.CONNECTION_POOL_RESET:
                result = await self._execute_connection_pool_reset(
                    response.parameters, trigger_context
                )
            else:
                result = {"message": f"Action {response.action.value} not implemented"}

            execution.duration_ms = (time.time() - start_time) * 1000
            execution.result = result
            execution.status = "completed"

            # Update tracking
            self.cooldown_tracking[response.response_id] = datetime.utcnow()
            self.execution_counts[response.response_id].append(datetime.utcnow())

            self.logger.info(
                f"Automated response executed: {response.action.value} for {trigger_context.get('metric_name')}"
            )

        except Exception as e:
            execution.duration_ms = (
                (time.time() - start_time) * 1000 if "start_time" in locals() else 0
            )
            execution.status = "failed"
            execution.error = str(e)
            self.logger.error(f"Automated response failed: {str(e)}")

        self.execution_history.append(execution)
        return execution

    def _check_cooldown(self, response_id: str, cooldown_minutes: int) -> bool:
        """Check if response is in cooldown period."""
        if response_id not in self.cooldown_tracking:
            return True

        last_execution = self.cooldown_tracking[response_id]
        time_since = datetime.utcnow() - last_execution
        return time_since.total_seconds() >= cooldown_minutes * 60

    def _check_execution_limits(self, response_id: str, max_executions: int) -> bool:
        """Check execution limits for the past hour."""
        cutoff_time = datetime.utcnow() - timedelta(hours=1)

        # Clean old executions
        self.execution_counts[response_id] = [
            exec_time
            for exec_time in self.execution_counts[response_id]
            if exec_time >= cutoff_time
        ]

        return len(self.execution_counts[response_id]) < max_executions

    async def _execute_scale_up(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute scale up action."""
        service_name = parameters.get("service_name", "unknown")
        scale_factor = parameters.get("scale_factor", 1.5)

        # This would integrate with container orchestration (Kubernetes, Docker Swarm, etc.)
        # For now, we'll simulate the action
        self.logger.info(f"Scaling up {service_name} by factor {scale_factor}")

        return {
            "action": "scale_up",
            "service": service_name,
            "scale_factor": scale_factor,
            "status": "simulated",
        }

    async def _execute_scale_down(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute scale down action."""
        service_name = parameters.get("service_name", "unknown")
        scale_factor = parameters.get("scale_factor", 0.7)

        self.logger.info(f"Scaling down {service_name} by factor {scale_factor}")

        return {
            "action": "scale_down",
            "service": service_name,
            "scale_factor": scale_factor,
            "status": "simulated",
        }

    async def _execute_restart_service(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute service restart action."""
        service_name = parameters.get("service_name", "unknown")
        graceful = parameters.get("graceful", True)

        self.logger.warning(f"Restarting service {service_name} (graceful: {graceful})")

        return {
            "action": "restart_service",
            "service": service_name,
            "graceful": graceful,
            "status": "simulated",
        }

    async def _execute_circuit_breaker(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute circuit breaker activation."""
        service_name = parameters.get("service_name", "unknown")
        duration_minutes = parameters.get("duration_minutes", 5)

        self.logger.warning(
            f"Activating circuit breaker for {service_name} for {duration_minutes} minutes"
        )

        return {
            "action": "circuit_breaker",
            "service": service_name,
            "duration_minutes": duration_minutes,
            "status": "simulated",
        }

    async def _execute_throttle(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute request throttling."""
        service_name = parameters.get("service_name", "unknown")
        rate_limit = parameters.get("rate_limit", 100)

        self.logger.info(f"Applying throttling to {service_name} (rate: {rate_limit} req/min)")

        return {
            "action": "throttle",
            "service": service_name,
            "rate_limit": rate_limit,
            "status": "simulated",
        }

    async def _execute_webhook(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute custom webhook."""
        webhook_url = parameters.get("webhook_url")
        payload = {
            "trigger_metric": context.get("metric_name"),
            "trigger_value": context.get("value"),
            "timestamp": datetime.utcnow().isoformat(),
            "parameters": parameters,
        }

        # In production, this would make an HTTP request to the webhook
        self.logger.info(f"Calling webhook {webhook_url} with payload")

        return {"action": "webhook", "url": webhook_url, "payload": payload, "status": "simulated"}

    async def _execute_failover(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute failover to backup system."""
        primary_service = parameters.get("primary_service", "unknown")
        backup_service = parameters.get("backup_service", "unknown")

        self.logger.critical(f"Executing failover from {primary_service} to {backup_service}")

        return {
            "action": "failover",
            "primary_service": primary_service,
            "backup_service": backup_service,
            "status": "simulated",
        }

    async def _execute_cache_flush(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute cache flush."""
        cache_name = parameters.get("cache_name", "default")
        cache_keys = parameters.get("cache_keys", [])

        self.logger.info(f"Flushing cache {cache_name}")

        return {
            "action": "cache_flush",
            "cache_name": cache_name,
            "keys_flushed": len(cache_keys) if cache_keys else "all",
            "status": "simulated",
        }

    async def _execute_connection_pool_reset(
        self, parameters: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute connection pool reset."""
        pool_name = parameters.get("pool_name", "default")

        self.logger.info(f"Resetting connection pool {pool_name}")

        return {"action": "connection_pool_reset", "pool_name": pool_name, "status": "simulated"}


class RealTimePerformanceMonitor:
    """Real-time performance monitoring with automated response system."""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.structured_logger = StructuredLogger.get_logger(__name__)
        self.tracer = get_tracing_manager()
        self.custom_metrics = get_custom_metrics_advanced()
        self.alerting = get_alerting_sla()

        # Components
        self.anomaly_detector = AnomalyDetector()
        self.response_executor = AutomatedResponseExecutor()

        # Configuration
        self.thresholds: dict[str, PerformanceThreshold] = {}
        self.automated_responses: dict[str, AutomatedResponse] = {}

        # Real-time data
        self.current_metrics: dict[str, PerformanceDataPoint] = {}
        self.metric_streams: dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))

        # System monitoring
        self.system_monitor_active = False
        self.system_monitor_thread: threading.Thread | None = None

        # Initialize default configuration
        self._initialize_default_thresholds()
        self._initialize_default_responses()

        self.logger.info("Real-time performance monitor initialized")

    def _initialize_default_thresholds(self):
        """Initialize default performance thresholds."""

        default_thresholds = [
            # API Performance Thresholds
            PerformanceThreshold(
                metric_name="api_response_time_ms",
                metric_type=PerformanceMetricType.LATENCY,
                warning_threshold=200.0,
                critical_threshold=500.0,
                evaluation_window_seconds=300,
                consecutive_violations=3,
                recovery_threshold=150.0,
                tags={"component": "api", "criticality": "high"},
            ),
            PerformanceThreshold(
                metric_name="api_requests_per_second",
                metric_type=PerformanceMetricType.THROUGHPUT,
                warning_threshold=1000.0,
                critical_threshold=1500.0,
                evaluation_window_seconds=300,
                consecutive_violations=2,
                tags={"component": "api", "type": "throughput"},
            ),
            PerformanceThreshold(
                metric_name="api_error_rate_percent",
                metric_type=PerformanceMetricType.ERROR_RATE,
                warning_threshold=1.0,
                critical_threshold=5.0,
                evaluation_window_seconds=300,
                consecutive_violations=3,
                recovery_threshold=0.5,
                tags={"component": "api", "criticality": "critical"},
            ),
            # System Resource Thresholds
            PerformanceThreshold(
                metric_name="system_cpu_percent",
                metric_type=PerformanceMetricType.RESOURCE_USAGE,
                warning_threshold=70.0,
                critical_threshold=85.0,
                evaluation_window_seconds=600,
                consecutive_violations=3,
                recovery_threshold=60.0,
                tags={"component": "system", "resource": "cpu"},
            ),
            PerformanceThreshold(
                metric_name="system_memory_percent",
                metric_type=PerformanceMetricType.RESOURCE_USAGE,
                warning_threshold=80.0,
                critical_threshold=90.0,
                evaluation_window_seconds=300,
                consecutive_violations=2,
                recovery_threshold=70.0,
                tags={"component": "system", "resource": "memory"},
            ),
            # Database Performance Thresholds
            PerformanceThreshold(
                metric_name="db_connection_pool_usage_percent",
                metric_type=PerformanceMetricType.RESOURCE_USAGE,
                warning_threshold=70.0,
                critical_threshold=85.0,
                evaluation_window_seconds=300,
                consecutive_violations=2,
                recovery_threshold=60.0,
                tags={"component": "database", "resource": "connections"},
            ),
            PerformanceThreshold(
                metric_name="db_query_response_time_ms",
                metric_type=PerformanceMetricType.LATENCY,
                warning_threshold=100.0,
                critical_threshold=500.0,
                evaluation_window_seconds=300,
                consecutive_violations=3,
                recovery_threshold=75.0,
                tags={"component": "database", "criticality": "high"},
            ),
            # Cache Performance Thresholds
            PerformanceThreshold(
                metric_name="cache_hit_rate_percent",
                metric_type=PerformanceMetricType.CACHE_HIT_RATE,
                warning_threshold=80.0,  # Warning if hit rate drops below 80%
                critical_threshold=60.0,  # Critical if below 60%
                evaluation_window_seconds=600,
                consecutive_violations=3,
                recovery_threshold=85.0,
                tags={"component": "cache", "criticality": "medium"},
            ),
            # ETL Performance Thresholds
            PerformanceThreshold(
                metric_name="etl_processing_time_minutes",
                metric_type=PerformanceMetricType.LATENCY,
                warning_threshold=30.0,
                critical_threshold=60.0,
                evaluation_window_seconds=300,
                consecutive_violations=1,  # ETL jobs should not take too long
                recovery_threshold=25.0,
                tags={"component": "etl", "criticality": "high"},
            ),
            # Queue Depth Thresholds
            PerformanceThreshold(
                metric_name="message_queue_depth",
                metric_type=PerformanceMetricType.QUEUE_DEPTH,
                warning_threshold=1000.0,
                critical_threshold=5000.0,
                evaluation_window_seconds=300,
                consecutive_violations=2,
                recovery_threshold=500.0,
                tags={"component": "messaging", "criticality": "medium"},
            ),
        ]

        for threshold in default_thresholds:
            self.thresholds[threshold.metric_name] = threshold

        self.logger.info(f"Initialized {len(default_thresholds)} default performance thresholds")

    def _initialize_default_responses(self):
        """Initialize default automated responses."""

        default_responses = [
            # API Response Time - Scale Up
            AutomatedResponse(
                response_id="api_latency_scale_up",
                trigger_metric="api_response_time_ms",
                trigger_threshold=500.0,
                action=ResponseAction.SCALE_UP,
                parameters={
                    "service_name": "api_service",
                    "scale_factor": 1.5,
                    "min_instances": 2,
                    "max_instances": 10,
                },
                cooldown_minutes=10,
                max_executions_per_hour=3,
                requires_confirmation=False,
            ),
            # High Error Rate - Circuit Breaker
            AutomatedResponse(
                response_id="api_error_circuit_breaker",
                trigger_metric="api_error_rate_percent",
                trigger_threshold=5.0,
                action=ResponseAction.CIRCUIT_BREAKER,
                parameters={
                    "service_name": "api_service",
                    "duration_minutes": 5,
                    "failure_threshold": 10,
                },
                cooldown_minutes=15,
                max_executions_per_hour=2,
                requires_confirmation=False,
            ),
            # High CPU - Scale Up
            AutomatedResponse(
                response_id="system_cpu_scale_up",
                trigger_metric="system_cpu_percent",
                trigger_threshold=85.0,
                action=ResponseAction.SCALE_UP,
                parameters={
                    "service_name": "application",
                    "scale_factor": 1.3,
                    "resource_type": "cpu",
                },
                cooldown_minutes=15,
                max_executions_per_hour=2,
                requires_confirmation=True,
            ),
            # High Memory - Restart Service
            AutomatedResponse(
                response_id="memory_leak_restart",
                trigger_metric="system_memory_percent",
                trigger_threshold=90.0,
                action=ResponseAction.RESTART_SERVICE,
                parameters={"service_name": "application", "graceful": True, "timeout_seconds": 30},
                cooldown_minutes=30,
                max_executions_per_hour=1,
                requires_confirmation=True,
            ),
            # Low Cache Hit Rate - Cache Flush
            AutomatedResponse(
                response_id="cache_performance_flush",
                trigger_metric="cache_hit_rate_percent",
                trigger_threshold=60.0,
                action=ResponseAction.CACHE_FLUSH,
                parameters={
                    "cache_name": "application_cache",
                    "selective_flush": True,
                    "warm_up_enabled": True,
                },
                cooldown_minutes=20,
                max_executions_per_hour=2,
                requires_confirmation=False,
            ),
            # Database Connection Pool Issues
            AutomatedResponse(
                response_id="db_pool_reset",
                trigger_metric="db_connection_pool_usage_percent",
                trigger_threshold=85.0,
                action=ResponseAction.CONNECTION_POOL_RESET,
                parameters={
                    "pool_name": "primary_db_pool",
                    "min_connections": 5,
                    "max_connections": 20,
                },
                cooldown_minutes=10,
                max_executions_per_hour=3,
                requires_confirmation=False,
            ),
            # High Queue Depth - Throttling
            AutomatedResponse(
                response_id="queue_depth_throttle",
                trigger_metric="message_queue_depth",
                trigger_threshold=5000.0,
                action=ResponseAction.THROTTLE,
                parameters={
                    "service_name": "message_processor",
                    "rate_limit": 100,
                    "duration_minutes": 10,
                },
                cooldown_minutes=15,
                max_executions_per_hour=3,
                requires_confirmation=False,
            ),
        ]

        for response in default_responses:
            self.automated_responses[response.response_id] = response

        self.logger.info(f"Initialized {len(default_responses)} default automated responses")

    async def start_monitoring(self):
        """Start real-time monitoring."""
        self.system_monitor_active = True

        # Start system resource monitoring
        self.system_monitor_thread = threading.Thread(target=self._system_monitor_loop, daemon=True)
        self.system_monitor_thread.start()

        # Start metric processing
        asyncio.create_task(self._metric_processing_loop())

        self.logger.info("Real-time performance monitoring started")

    async def stop_monitoring(self):
        """Stop real-time monitoring."""
        self.system_monitor_active = False

        if self.system_monitor_thread:
            self.system_monitor_thread.join(timeout=5)

        self.logger.info("Real-time performance monitoring stopped")

    def _system_monitor_loop(self):
        """System resource monitoring loop."""
        while self.system_monitor_active:
            try:
                # Collect system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage("/")

                # Submit metrics
                asyncio.create_task(self.submit_metric("system_cpu_percent", cpu_percent))
                asyncio.create_task(self.submit_metric("system_memory_percent", memory.percent))
                asyncio.create_task(self.submit_metric("system_disk_percent", disk.percent))

                # Network metrics
                try:
                    network = psutil.net_io_counters()
                    asyncio.create_task(
                        self.submit_metric("system_network_bytes_sent", network.bytes_sent)
                    )
                    asyncio.create_task(
                        self.submit_metric("system_network_bytes_recv", network.bytes_recv)
                    )
                except:
                    pass  # Network metrics may not be available

                time.sleep(30)  # Collect every 30 seconds

            except Exception as e:
                self.logger.error(f"Error in system monitor loop: {str(e)}")
                time.sleep(30)

    async def _metric_processing_loop(self):
        """Process metrics and detect anomalies."""
        while self.system_monitor_active:
            try:
                await asyncio.sleep(10)  # Process every 10 seconds

                datetime.utcnow()

                # Process each metric
                for metric_name, data_point in self.current_metrics.items():
                    # Check thresholds
                    if metric_name in self.thresholds:
                        await self._check_threshold_violations(metric_name, data_point)

                    # Detect anomalies
                    anomalies = self.anomaly_detector.detect_anomalies(
                        metric_name, data_point.value
                    )

                    for anomaly in anomalies:
                        await self._handle_anomaly(anomaly)

            except Exception as e:
                self.logger.error(f"Error in metric processing loop: {str(e)}")
                await asyncio.sleep(10)

    async def submit_metric(
        self,
        metric_name: str,
        value: float,
        dimensions: dict[str, str] = None,
        metadata: dict[str, Any] = None,
    ) -> bool:
        """Submit a performance metric for real-time monitoring."""
        try:
            data_point = PerformanceDataPoint(
                metric_name=metric_name,
                value=value,
                timestamp=datetime.utcnow(),
                dimensions=dimensions or {},
                metadata=metadata or {},
            )

            # Store current value
            self.current_metrics[metric_name] = data_point

            # Add to stream
            self.metric_streams[metric_name].append(data_point)

            # Add to anomaly detector
            self.anomaly_detector.add_data_point(metric_name, value, data_point.timestamp)

            # Submit to custom metrics system
            await self.custom_metrics.submit_metric(metric_name, value, dimensions, metadata)

            # Enhanced real-time processing
            await self._process_realtime_alerts(metric_name, data_point)

            return True

        except Exception as e:
            self.logger.error(f"Failed to submit metric {metric_name}: {str(e)}")
            return False

    async def _process_realtime_alerts(self, metric_name: str, data_point: PerformanceDataPoint):
        """Process real-time alerts for immediate response."""
        try:
            if metric_name in self.thresholds:
                threshold = self.thresholds[metric_name]

                # Immediate critical threshold check
                if data_point.value >= threshold.critical_threshold:
                    await self._trigger_immediate_alert(
                        metric_name, data_point, "critical", threshold
                    )

                # Real-time trend analysis for predictive alerting
                if len(self.metric_streams[metric_name]) >= 5:
                    recent_values = [dp.value for dp in list(self.metric_streams[metric_name])[-5:]]
                    trend_rate = (recent_values[-1] - recent_values[0]) / len(recent_values)

                    # Predict if we'll breach threshold in next 2 minutes at current rate
                    if trend_rate > 0:
                        time_to_breach = (
                            threshold.warning_threshold - data_point.value
                        ) / trend_rate
                        if 0 < time_to_breach <= 2:  # 2 data points ahead
                            await self._trigger_predictive_alert(
                                metric_name, data_point, time_to_breach
                            )

        except Exception as e:
            self.logger.error(f"Failed to process real-time alerts for {metric_name}: {str(e)}")

    async def _trigger_immediate_alert(
        self,
        metric_name: str,
        data_point: PerformanceDataPoint,
        severity: str,
        threshold: PerformanceThreshold,
    ):
        """Trigger immediate alert for critical thresholds."""
        try:
            alert_message = (
                f"IMMEDIATE {severity.upper()} ALERT: {metric_name} = {data_point.value} "
                f"exceeds {severity} threshold {threshold.critical_threshold}"
            )

            # Log with high priority
            self.logger.critical(alert_message)

            # Send to structured logging for immediate processing
            self.structured_logger.critical(
                alert_message,
                extra={
                    "category": "performance",
                    "event_type": "immediate_alert",
                    "metric_name": metric_name,
                    "value": data_point.value,
                    "threshold": threshold.critical_threshold,
                    "severity": severity,
                    "requires_immediate_action": True,
                },
            )

            # If we have alerting system, send immediate notification
            if self.alerting:
                await self.alerting.trigger_immediate_alert(
                    metric_name, data_point.value, severity, threshold.critical_threshold
                )

        except Exception as e:
            self.logger.error(f"Failed to trigger immediate alert: {str(e)}")

    async def _trigger_predictive_alert(
        self, metric_name: str, data_point: PerformanceDataPoint, time_to_breach: float
    ):
        """Trigger predictive alert before threshold is breached."""
        try:
            alert_message = (
                f"PREDICTIVE ALERT: {metric_name} trending toward threshold breach "
                f"in ~{time_to_breach:.1f} minutes (current: {data_point.value})"
            )

            self.logger.warning(alert_message)

            self.structured_logger.warning(
                alert_message,
                extra={
                    "category": "performance",
                    "event_type": "predictive_alert",
                    "metric_name": metric_name,
                    "current_value": data_point.value,
                    "time_to_breach_minutes": time_to_breach,
                    "alert_type": "predictive",
                },
            )

        except Exception as e:
            self.logger.error(f"Failed to trigger predictive alert: {str(e)}")

    async def _check_threshold_violations(self, metric_name: str, data_point: PerformanceDataPoint):
        """Check for threshold violations."""
        threshold = self.thresholds[metric_name]

        if not threshold.enabled:
            return

        # Check critical threshold
        if data_point.value >= threshold.critical_threshold:
            await self._handle_threshold_violation(metric_name, data_point, "critical", threshold)
        # Check warning threshold
        elif data_point.value >= threshold.warning_threshold:
            await self._handle_threshold_violation(metric_name, data_point, "warning", threshold)

    async def _handle_threshold_violation(
        self,
        metric_name: str,
        data_point: PerformanceDataPoint,
        severity: str,
        threshold: PerformanceThreshold,
    ):
        """Handle threshold violation."""
        context = LogContext(operation="threshold_violation", component="performance_monitor")

        with log_context(context):
            self.structured_logger.warning(
                f"Performance threshold violation: {metric_name} = {data_point.value} exceeds {severity} threshold {threshold.critical_threshold if severity == 'critical' else threshold.warning_threshold}",
                extra={
                    "category": "performance",
                    "event_type": "threshold_violation",
                    "metric_name": metric_name,
                    "value": data_point.value,
                    "threshold": threshold.critical_threshold
                    if severity == "critical"
                    else threshold.warning_threshold,
                    "severity": severity,
                },
            )

        # Check for automated responses
        matching_responses = [
            response
            for response in self.automated_responses.values()
            if response.trigger_metric == metric_name
            and data_point.value >= response.trigger_threshold
            and response.enabled
        ]

        for response in matching_responses:
            trigger_context = {
                "metric_name": metric_name,
                "value": data_point.value,
                "severity": severity,
                "threshold": threshold,
                "timestamp": data_point.timestamp.isoformat(),
            }

            execution = await self.response_executor.execute_response(response, trigger_context)

            self.structured_logger.info(
                f"Automated response executed: {response.action.value}",
                extra={
                    "category": "performance",
                    "event_type": "automated_response",
                    "response_id": response.response_id,
                    "execution_id": execution.execution_id,
                    "status": execution.status,
                },
            )

    async def _handle_anomaly(self, anomaly: PerformanceAnomaly):
        """Handle detected anomaly."""
        context = LogContext(operation="anomaly_detection", component="performance_monitor")

        with log_context(context):
            self.structured_logger.warning(
                f"Performance anomaly detected: {anomaly.description}",
                extra={
                    "category": "performance",
                    "event_type": "anomaly_detected",
                    "anomaly_id": anomaly.anomaly_id,
                    "metric_name": anomaly.metric_name,
                    "anomaly_type": anomaly.anomaly_type.value,
                    "severity": anomaly.severity,
                    "confidence": anomaly.confidence,
                    "suggested_actions": anomaly.suggested_actions,
                },
            )

    def get_performance_summary(self, hours: int = 1) -> dict[str, Any]:
        """Get performance monitoring summary."""
        try:
            current_time = datetime.utcnow()
            cutoff_time = current_time - timedelta(hours=hours)

            # Collect recent metrics
            recent_metrics = {}
            for metric_name, stream in self.metric_streams.items():
                recent_values = [dp.value for dp in stream if dp.timestamp >= cutoff_time]

                if recent_values:
                    recent_metrics[metric_name] = {
                        "current": recent_values[-1] if recent_values else 0,
                        "avg": statistics.mean(recent_values),
                        "min": min(recent_values),
                        "max": max(recent_values),
                        "count": len(recent_values),
                    }

            # Threshold violations
            threshold_violations = {}
            for metric_name, threshold in self.thresholds.items():
                if metric_name in recent_metrics:
                    current_value = recent_metrics[metric_name]["current"]
                    violations = []

                    if current_value >= threshold.critical_threshold:
                        violations.append("critical")
                    elif current_value >= threshold.warning_threshold:
                        violations.append("warning")

                    if violations:
                        threshold_violations[metric_name] = violations

            # Recent executions
            recent_executions = [
                {
                    "execution_id": exec.execution_id,
                    "response_id": exec.response_id,
                    "action": exec.action.value,
                    "status": exec.status,
                    "executed_at": exec.executed_at.isoformat(),
                }
                for exec in self.response_executor.execution_history
                if exec.executed_at >= cutoff_time
            ]

            return {
                "timestamp": current_time.isoformat(),
                "monitoring_period_hours": hours,
                "metrics_summary": recent_metrics,
                "threshold_violations": threshold_violations,
                "recent_executions": recent_executions,
                "total_thresholds": len(self.thresholds),
                "total_responses": len(self.automated_responses),
                "active_monitoring": self.system_monitor_active,
            }

        except Exception as e:
            self.logger.error(f"Failed to generate performance summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


# Global performance monitor
_performance_monitor = None


def get_performance_monitor() -> RealTimePerformanceMonitor:
    """Get global performance monitor instance."""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = RealTimePerformanceMonitor()
    return _performance_monitor


if __name__ == "__main__":
    # Example usage
    import asyncio

    async def test_performance_monitoring():
        monitor = get_performance_monitor()

        # Start monitoring
        await monitor.start_monitoring()

        # Submit some test metrics
        await monitor.submit_metric("api_response_time_ms", 150.0)
        await monitor.submit_metric("api_response_time_ms", 250.0)
        await monitor.submit_metric("api_response_time_ms", 600.0)  # Should trigger alert

        await monitor.submit_metric("system_cpu_percent", 45.0)
        await monitor.submit_metric("system_cpu_percent", 78.0)
        await monitor.submit_metric("system_cpu_percent", 88.0)  # Should trigger alert

        # Wait for processing
        await asyncio.sleep(5)

        # Get summary
        summary = monitor.get_performance_summary()
        print("Performance Summary:")
        print(json.dumps(summary, indent=2))

        await monitor.stop_monitoring()

    # Run test
    asyncio.run(test_performance_monitoring())
