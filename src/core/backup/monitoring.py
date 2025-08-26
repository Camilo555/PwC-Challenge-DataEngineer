"""
Backup and Recovery Monitoring System

Real-time monitoring, metrics collection, and alerting for backup and recovery operations.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class MetricType(str, Enum):
    """Types of backup metrics."""
    BACKUP_SUCCESS_RATE = "backup_success_rate"
    BACKUP_DURATION = "backup_duration"
    BACKUP_SIZE = "backup_size"
    RECOVERY_TIME = "recovery_time"
    STORAGE_UTILIZATION = "storage_utilization"
    VALIDATION_ERRORS = "validation_errors"
    RTO_COMPLIANCE = "rto_compliance"
    RPO_COMPLIANCE = "rpo_compliance"


@dataclass
class BackupMetric:
    """Individual backup metric."""
    metric_type: MetricType
    value: float
    unit: str
    timestamp: datetime
    tags: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            'metric_type': self.metric_type.value,
            'value': self.value,
            'unit': self.unit,
            'timestamp': self.timestamp.isoformat(),
            'tags': self.tags,
            'metadata': self.metadata
        }


@dataclass
class BackupAlert:
    """Backup system alert."""
    alert_id: str
    severity: AlertSeverity
    title: str
    message: str
    component: str
    timestamp: datetime
    resolved: bool = False
    resolved_at: datetime | None = None
    tags: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            'alert_id': self.alert_id,
            'severity': self.severity.value,
            'title': self.title,
            'message': self.message,
            'component': self.component,
            'timestamp': self.timestamp.isoformat(),
            'resolved': self.resolved,
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None,
            'tags': self.tags,
            'metadata': self.metadata
        }


class BackupMonitor:
    """
    Comprehensive backup and recovery monitoring system.
    
    Features:
    - Real-time metrics collection
    - Threshold-based alerting
    - Performance monitoring
    - SLA compliance tracking
    - Custom notification channels
    """

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}

        # Storage for metrics and alerts
        self.metrics: list[BackupMetric] = []
        self.alerts: dict[str, BackupAlert] = {}
        self.alert_handlers: list[Callable] = []

        # Monitoring thresholds
        self.thresholds = {
            MetricType.BACKUP_SUCCESS_RATE: {"warning": 95.0, "critical": 90.0},
            MetricType.BACKUP_DURATION: {"warning": 3600, "critical": 7200},  # seconds
            MetricType.RECOVERY_TIME: {"warning": 1800, "critical": 3600},   # seconds
            MetricType.STORAGE_UTILIZATION: {"warning": 80.0, "critical": 90.0},  # percentage
            MetricType.VALIDATION_ERRORS: {"warning": 5, "critical": 10},
            MetricType.RTO_COMPLIANCE: {"warning": 95.0, "critical": 90.0},  # percentage
            MetricType.RPO_COMPLIANCE: {"warning": 95.0, "critical": 90.0}   # percentage
        }

        # Update with custom thresholds
        if "thresholds" in self.config:
            self.thresholds.update(self.config["thresholds"])

        # Metrics retention (days)
        self.metrics_retention_days = self.config.get("metrics_retention_days", 90)

        logger.info("Backup monitor initialized")

    async def record_backup_start(self, backup_id: str, component: str, backup_type: str) -> None:
        """Record backup operation start."""
        await self._record_metric(
            MetricType.BACKUP_DURATION,
            0.0,  # Will be updated on completion
            "seconds",
            tags={
                "backup_id": backup_id,
                "component": component,
                "backup_type": backup_type,
                "phase": "started"
            }
        )

        logger.debug(f"Recorded backup start: {backup_id}")

    async def record_backup_completion(
        self,
        backup_id: str,
        component: str,
        backup_type: str,
        duration_seconds: float,
        size_bytes: int,
        success: bool
    ) -> None:
        """Record backup operation completion."""
        # Record duration metric
        await self._record_metric(
            MetricType.BACKUP_DURATION,
            duration_seconds,
            "seconds",
            tags={
                "backup_id": backup_id,
                "component": component,
                "backup_type": backup_type,
                "success": str(success).lower()
            }
        )

        # Record size metric
        await self._record_metric(
            MetricType.BACKUP_SIZE,
            size_bytes,
            "bytes",
            tags={
                "backup_id": backup_id,
                "component": component,
                "backup_type": backup_type
            }
        )

        # Update success rate
        await self._update_success_rate(component, success)

        # Check thresholds and generate alerts if needed
        await self._check_thresholds_and_alert(
            MetricType.BACKUP_DURATION,
            duration_seconds,
            component,
            backup_id
        )

        logger.info(f"Recorded backup completion: {backup_id} "
                   f"(duration: {duration_seconds:.1f}s, size: {size_bytes} bytes, success: {success})")

    async def record_recovery_operation(
        self,
        recovery_id: str,
        component: str,
        recovery_type: str,
        duration_seconds: float,
        success: bool
    ) -> None:
        """Record recovery operation metrics."""
        await self._record_metric(
            MetricType.RECOVERY_TIME,
            duration_seconds,
            "seconds",
            tags={
                "recovery_id": recovery_id,
                "component": component,
                "recovery_type": recovery_type,
                "success": str(success).lower()
            }
        )

        # Check RTO compliance
        await self._check_rto_compliance(component, duration_seconds)

        logger.info(f"Recorded recovery operation: {recovery_id} "
                   f"(duration: {duration_seconds:.1f}s, success: {success})")

    async def record_validation_result(
        self,
        backup_id: str,
        component: str,
        validation_errors: int,
        validation_duration: float
    ) -> None:
        """Record backup validation results."""
        await self._record_metric(
            MetricType.VALIDATION_ERRORS,
            validation_errors,
            "count",
            tags={
                "backup_id": backup_id,
                "component": component
            }
        )

        # Alert on validation errors
        if validation_errors > 0:
            await self._check_thresholds_and_alert(
                MetricType.VALIDATION_ERRORS,
                validation_errors,
                component,
                backup_id
            )

        logger.debug(f"Recorded validation result: {backup_id} "
                    f"({validation_errors} errors, {validation_duration:.1f}s)")

    async def record_storage_utilization(
        self,
        storage_backend: str,
        utilization_percentage: float,
        total_bytes: int,
        used_bytes: int
    ) -> None:
        """Record storage utilization metrics."""
        await self._record_metric(
            MetricType.STORAGE_UTILIZATION,
            utilization_percentage,
            "percentage",
            tags={
                "storage_backend": storage_backend
            },
            metadata={
                "total_bytes": total_bytes,
                "used_bytes": used_bytes
            }
        )

        # Check storage utilization thresholds
        await self._check_thresholds_and_alert(
            MetricType.STORAGE_UTILIZATION,
            utilization_percentage,
            f"storage_{storage_backend}",
            storage_backend
        )

        logger.debug(f"Recorded storage utilization: {storage_backend} "
                    f"({utilization_percentage:.1f}%)")

    async def generate_alert(
        self,
        severity: AlertSeverity,
        title: str,
        message: str,
        component: str,
        tags: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None
    ) -> BackupAlert:
        """Generate a new alert."""
        alert_id = f"alert_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"

        alert = BackupAlert(
            alert_id=alert_id,
            severity=severity,
            title=title,
            message=message,
            component=component,
            timestamp=datetime.utcnow(),
            tags=tags or {},
            metadata=metadata or {}
        )

        # Store alert
        self.alerts[alert_id] = alert

        # Send to alert handlers
        await self._send_alert(alert)

        logger.warning(f"Generated {severity.value} alert: {title} ({alert_id})")
        return alert

    async def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert."""
        if alert_id in self.alerts:
            alert = self.alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.utcnow()

            logger.info(f"Resolved alert: {alert_id}")
            return True

        return False

    async def get_metrics_summary(
        self,
        metric_type: MetricType | None = None,
        component: str | None = None,
        hours: int = 24
    ) -> dict[str, Any]:
        """Get metrics summary for specified period."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        # Filter metrics
        filtered_metrics = []
        for metric in self.metrics:
            if metric.timestamp < cutoff_time:
                continue

            if metric_type and metric.metric_type != metric_type:
                continue

            if component and metric.tags.get("component") != component:
                continue

            filtered_metrics.append(metric)

        if not filtered_metrics:
            return {"count": 0, "summary": "No metrics found"}

        # Calculate summary statistics
        values = [m.value for m in filtered_metrics]

        summary = {
            "count": len(filtered_metrics),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "latest": filtered_metrics[-1].value if filtered_metrics else 0,
            "period_hours": hours,
            "metric_type": metric_type.value if metric_type else "all",
            "component": component or "all"
        }

        return summary

    async def get_sla_compliance_report(self) -> dict[str, Any]:
        """Generate SLA compliance report."""
        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)
        last_30d = now - timedelta(days=30)

        report = {
            "timestamp": now.isoformat(),
            "periods": {
                "24_hours": await self._calculate_sla_compliance(last_24h, now),
                "7_days": await self._calculate_sla_compliance(last_7d, now),
                "30_days": await self._calculate_sla_compliance(last_30d, now)
            },
            "current_alerts": len([a for a in self.alerts.values() if not a.resolved]),
            "total_alerts": len(self.alerts)
        }

        return report

    async def get_active_alerts(self, severity: AlertSeverity | None = None) -> list[dict[str, Any]]:
        """Get active alerts, optionally filtered by severity."""
        active_alerts = [
            alert.to_dict()
            for alert in self.alerts.values()
            if not alert.resolved
        ]

        if severity:
            active_alerts = [
                alert for alert in active_alerts
                if alert["severity"] == severity.value
            ]

        # Sort by timestamp (newest first)
        active_alerts.sort(key=lambda x: x["timestamp"], reverse=True)

        return active_alerts

    async def cleanup_old_metrics(self) -> dict[str, Any]:
        """Clean up old metrics based on retention policy."""
        cutoff_date = datetime.utcnow() - timedelta(days=self.metrics_retention_days)

        initial_count = len(self.metrics)
        self.metrics = [m for m in self.metrics if m.timestamp >= cutoff_date]
        final_count = len(self.metrics)

        cleaned_count = initial_count - final_count

        logger.info(f"Cleaned up {cleaned_count} old metrics")

        return {
            "initial_count": initial_count,
            "final_count": final_count,
            "cleaned_count": cleaned_count,
            "retention_days": self.metrics_retention_days
        }

    def add_alert_handler(self, handler: Callable) -> None:
        """Add alert notification handler."""
        self.alert_handlers.append(handler)
        logger.info("Added alert handler")

    async def _record_metric(
        self,
        metric_type: MetricType,
        value: float,
        unit: str,
        tags: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None
    ) -> None:
        """Record a metric."""
        metric = BackupMetric(
            metric_type=metric_type,
            value=value,
            unit=unit,
            timestamp=datetime.utcnow(),
            tags=tags or {},
            metadata=metadata or {}
        )

        self.metrics.append(metric)

    async def _update_success_rate(self, component: str, success: bool) -> None:
        """Update backup success rate for component."""
        # Calculate success rate from recent backups
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)

        recent_backups = [
            m for m in self.metrics
            if (m.metric_type == MetricType.BACKUP_DURATION and
                m.timestamp >= recent_cutoff and
                m.tags.get("component") == component)
        ]

        if recent_backups:
            successful_backups = sum(
                1 for m in recent_backups
                if m.tags.get("success") == "true"
            )
            success_rate = (successful_backups / len(recent_backups)) * 100

            await self._record_metric(
                MetricType.BACKUP_SUCCESS_RATE,
                success_rate,
                "percentage",
                tags={"component": component}
            )

            # Check success rate thresholds
            await self._check_thresholds_and_alert(
                MetricType.BACKUP_SUCCESS_RATE,
                success_rate,
                component,
                f"success_rate_{component}"
            )

    async def _check_rto_compliance(self, component: str, recovery_duration: float) -> None:
        """Check RTO compliance for recovery operation."""
        # Default RTO targets (in seconds)
        rto_targets = {
            "database": 3600,      # 1 hour
            "configuration": 1800,  # 30 minutes
            "data_lake": 7200      # 2 hours
        }

        target_rto = rto_targets.get(component, 3600)  # Default 1 hour
        compliance = 100.0 if recovery_duration <= target_rto else 0.0

        await self._record_metric(
            MetricType.RTO_COMPLIANCE,
            compliance,
            "percentage",
            tags={"component": component},
            metadata={"target_rto": target_rto, "actual_duration": recovery_duration}
        )

        if compliance < 100.0:
            await self.generate_alert(
                AlertSeverity.HIGH,
                f"RTO Violation - {component}",
                f"Recovery took {recovery_duration:.0f}s, exceeding RTO of {target_rto:.0f}s",
                component,
                tags={"alert_type": "rto_violation"}
            )

    async def _check_thresholds_and_alert(
        self,
        metric_type: MetricType,
        value: float,
        component: str,
        context_id: str
    ) -> None:
        """Check metric against thresholds and generate alerts."""
        if metric_type not in self.thresholds:
            return

        thresholds = self.thresholds[metric_type]
        severity = None

        # Determine severity based on thresholds
        if metric_type in [MetricType.BACKUP_SUCCESS_RATE, MetricType.RTO_COMPLIANCE, MetricType.RPO_COMPLIANCE]:
            # Higher is better - alert if below threshold
            if value <= thresholds.get("critical", 0):
                severity = AlertSeverity.CRITICAL
            elif value <= thresholds.get("warning", 0):
                severity = AlertSeverity.HIGH
        else:
            # Lower is better - alert if above threshold
            if value >= thresholds.get("critical", float('inf')):
                severity = AlertSeverity.CRITICAL
            elif value >= thresholds.get("warning", float('inf')):
                severity = AlertSeverity.HIGH

        if severity:
            title = f"{metric_type.value.replace('_', ' ').title()} Alert - {component}"
            message = f"{metric_type.value} is {value:.2f}, threshold: {thresholds}"

            await self.generate_alert(
                severity,
                title,
                message,
                component,
                tags={
                    "metric_type": metric_type.value,
                    "context_id": context_id,
                    "threshold_type": "automatic"
                },
                metadata={"value": value, "thresholds": thresholds}
            )

    async def _send_alert(self, alert: BackupAlert) -> None:
        """Send alert to all registered handlers."""
        for handler in self.alert_handlers:
            try:
                await handler(alert.to_dict())
            except Exception as e:
                logger.error(f"Alert handler failed: {e}")

    async def _calculate_sla_compliance(self, start_time: datetime, end_time: datetime) -> dict[str, Any]:
        """Calculate SLA compliance for a time period."""
        period_metrics = [
            m for m in self.metrics
            if start_time <= m.timestamp <= end_time
        ]

        compliance = {
            "backup_success_rate": 100.0,
            "rto_compliance": 100.0,
            "rpo_compliance": 100.0,
            "total_backups": 0,
            "failed_backups": 0,
            "total_recoveries": 0,
            "rto_violations": 0
        }

        # Calculate backup success rate
        backup_metrics = [
            m for m in period_metrics
            if m.metric_type == MetricType.BACKUP_DURATION
        ]

        if backup_metrics:
            compliance["total_backups"] = len(backup_metrics)
            compliance["failed_backups"] = sum(
                1 for m in backup_metrics
                if m.tags.get("success") == "false"
            )

            if compliance["total_backups"] > 0:
                compliance["backup_success_rate"] = (
                    (compliance["total_backups"] - compliance["failed_backups"]) /
                    compliance["total_backups"]
                ) * 100

        # Calculate RTO compliance
        rto_metrics = [
            m for m in period_metrics
            if m.metric_type == MetricType.RTO_COMPLIANCE
        ]

        if rto_metrics:
            compliance["total_recoveries"] = len(rto_metrics)
            compliance["rto_violations"] = sum(
                1 for m in rto_metrics
                if m.value < 100.0
            )

            if compliance["total_recoveries"] > 0:
                compliance["rto_compliance"] = (
                    (compliance["total_recoveries"] - compliance["rto_violations"]) /
                    compliance["total_recoveries"]
                ) * 100

        return compliance


# Default alert handlers
async def console_alert_handler(alert_data: dict[str, Any]) -> None:
    """Simple console alert handler."""
    severity = alert_data["severity"].upper()
    title = alert_data["title"]
    message = alert_data["message"]

    print(f"[{severity}] {title}: {message}")


async def log_alert_handler(alert_data: dict[str, Any]) -> None:
    """Log file alert handler."""
    severity = alert_data["severity"]
    title = alert_data["title"]
    message = alert_data["message"]

    if severity in ["critical", "high"]:
        logger.error(f"ALERT: {title} - {message}")
    elif severity == "medium":
        logger.warning(f"ALERT: {title} - {message}")
    else:
        logger.info(f"ALERT: {title} - {message}")
