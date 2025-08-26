"""
Data Quality Monitoring System
Real-time monitoring and alerting for data quality metrics and issues.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from core.config import get_settings
from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    LOW = "low" 
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MonitoringStatus(Enum):
    """Monitoring status"""
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"
    ERROR = "error"


@dataclass
class QualityMetric:
    """Single quality metric measurement"""
    metric_id: str
    dataset_id: str
    metric_name: str
    metric_value: float
    threshold_min: float | None = None
    threshold_max: float | None = None
    timestamp: datetime = field(default_factory=datetime.now)
    tags: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityAlert:
    """Data quality alert"""
    alert_id: str
    dataset_id: str
    metric_name: str
    severity: AlertSeverity
    title: str
    description: str
    triggered_value: float
    threshold_value: float
    threshold_type: str  # min, max, range
    
    # Alert lifecycle
    triggered_at: datetime = field(default_factory=datetime.now)
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    acknowledged_by: str | None = None
    resolved_by: str | None = None
    
    # Context
    affected_records: int = 0
    business_impact: str | None = None
    remediation_steps: list[str] = field(default_factory=list)
    
    # Metadata
    alert_rule_id: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertRule:
    """Data quality alert rule configuration"""
    rule_id: str
    name: str
    description: str
    dataset_pattern: str  # Regex pattern for dataset matching
    metric_name: str
    
    # Threshold configuration
    min_threshold: float | None = None
    max_threshold: float | None = None
    percentage_change_threshold: float | None = None
    
    # Alert configuration
    severity: AlertSeverity = AlertSeverity.MEDIUM
    cooldown_minutes: int = 60
    escalation_minutes: int = 240
    
    # Conditions
    consecutive_failures: int = 1
    evaluation_window_minutes: int = 5
    
    # Actions
    notification_channels: list[str] = field(default_factory=list)
    auto_remediation: bool = False
    
    # Lifecycle
    active: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    tags: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MonitoringDashboard:
    """Real-time monitoring dashboard data"""
    dashboard_id: str
    name: str
    description: str | None = None
    
    # Current status
    total_datasets: int = 0
    healthy_datasets: int = 0
    warning_datasets: int = 0
    critical_datasets: int = 0
    
    # Alerts
    active_alerts: int = 0
    critical_alerts: int = 0
    alerts_last_24h: int = 0
    
    # Quality metrics
    overall_quality_score: float = 0.0
    quality_trend: str = "stable"  # improving, stable, declining
    
    # Recent activity
    recent_alerts: list[QualityAlert] = field(default_factory=list)
    recent_metrics: list[QualityMetric] = field(default_factory=list)
    
    # Time series data for charts
    quality_trend_data: list[dict[str, Any]] = field(default_factory=list)
    alert_trend_data: list[dict[str, Any]] = field(default_factory=list)
    
    last_updated: datetime = field(default_factory=datetime.now)


class QualityMonitor:
    """
    Real-time Data Quality Monitoring System
    
    Provides:
    - Continuous quality metric monitoring
    - Real-time alerting and notifications
    - Interactive dashboards and visualizations
    - Historical trend analysis
    - Automated remediation workflows
    - SLA and compliance tracking
    """

    def __init__(self, storage_path: Path | None = None):
        self.settings = get_settings()
        self.logger = get_logger(self.__class__.__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Storage
        self.storage_path = storage_path or Path("./quality_monitoring")
        self.metrics_path = self.storage_path / "metrics"
        self.alerts_path = self.storage_path / "alerts"
        self.rules_path = self.storage_path / "rules"
        self.dashboards_path = self.storage_path / "dashboards"
        
        self._ensure_directories()
        
        # In-memory storage for real-time performance
        self.metrics_buffer: deque[QualityMetric] = deque(maxlen=10000)
        self.active_alerts: dict[str, QualityAlert] = {}
        self.alert_rules: dict[str, AlertRule] = {}
        self.alert_history: deque[QualityAlert] = deque(maxlen=5000)
        
        # Monitoring state
        self.monitoring_status = MonitoringStatus.ACTIVE
        self.monitoring_threads: dict[str, threading.Thread] = {}
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Configuration
        self.metric_retention_days = 30
        self.alert_retention_days = 90
        self.dashboard_refresh_seconds = 30
        
        # Trend analysis
        self.trend_analyzers: dict[str, Any] = {}
        
        # Load existing data
        self._load_monitoring_data()
        
        # Start background monitoring
        self._start_monitoring_threads()

    def _ensure_directories(self):
        """Ensure monitoring directories exist"""
        for path in [self.storage_path, self.metrics_path, self.alerts_path, 
                    self.rules_path, self.dashboards_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _load_monitoring_data(self):
        """Load existing monitoring data"""
        try:
            # Load alert rules
            for rule_file in self.rules_path.glob("*.json"):
                try:
                    with open(rule_file) as f:
                        rule_data = json.load(f)
                        rule = self._dict_to_alert_rule(rule_data)
                        self.alert_rules[rule.rule_id] = rule
                except Exception as e:
                    self.logger.error(f"Failed to load alert rule {rule_file}: {e}")
            
            # Load recent metrics (last 24 hours)
            cutoff_time = datetime.now() - timedelta(hours=24)
            for metric_file in self.metrics_path.glob("*.json"):
                try:
                    file_time = datetime.fromtimestamp(metric_file.stat().st_mtime)
                    if file_time < cutoff_time:
                        continue
                    
                    with open(metric_file) as f:
                        metrics_data = json.load(f)
                        for metric_data in metrics_data.get('metrics', []):
                            metric = self._dict_to_quality_metric(metric_data)
                            if metric.timestamp >= cutoff_time:
                                self.metrics_buffer.append(metric)
                except Exception as e:
                    self.logger.error(f"Failed to load metrics {metric_file}: {e}")
            
            # Load active alerts
            for alert_file in self.alerts_path.glob("active_*.json"):
                try:
                    with open(alert_file) as f:
                        alert_data = json.load(f)
                        alert = self._dict_to_quality_alert(alert_data)
                        if alert.resolved_at is None:
                            self.active_alerts[alert.alert_id] = alert
                except Exception as e:
                    self.logger.error(f"Failed to load active alert {alert_file}: {e}")
            
            self.logger.info(
                f"Loaded monitoring data: {len(self.alert_rules)} rules, "
                f"{len(self.metrics_buffer)} recent metrics, "
                f"{len(self.active_alerts)} active alerts"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load monitoring data: {e}")

    def _start_monitoring_threads(self):
        """Start background monitoring threads"""
        try:
            # Metric evaluation thread
            metric_thread = threading.Thread(
                target=self._metric_evaluation_loop,
                name="MetricEvaluator",
                daemon=True
            )
            metric_thread.start()
            self.monitoring_threads["metric_evaluator"] = metric_thread
            
            # Alert management thread
            alert_thread = threading.Thread(
                target=self._alert_management_loop,
                name="AlertManager",
                daemon=True
            )
            alert_thread.start()
            self.monitoring_threads["alert_manager"] = alert_thread
            
            # Dashboard update thread
            dashboard_thread = threading.Thread(
                target=self._dashboard_update_loop,
                name="DashboardUpdater",
                daemon=True
            )
            dashboard_thread.start()
            self.monitoring_threads["dashboard_updater"] = dashboard_thread
            
            # Cleanup thread
            cleanup_thread = threading.Thread(
                target=self._cleanup_loop,
                name="DataCleanup",
                daemon=True
            )
            cleanup_thread.start()
            self.monitoring_threads["data_cleanup"] = cleanup_thread
            
            self.logger.info("Started monitoring background threads")
            
        except Exception as e:
            self.logger.error(f"Failed to start monitoring threads: {e}")

    def record_quality_metric(
        self,
        dataset_id: str,
        metric_name: str,
        metric_value: float,
        tags: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None
    ) -> str:
        """
        Record a quality metric measurement
        
        Args:
            dataset_id: Dataset identifier
            metric_name: Name of the quality metric
            metric_value: Measured value
            tags: Additional tags
            metadata: Additional metadata
        
        Returns:
            Metric ID
        """
        try:
            with self._lock:
                metric_id = f"{dataset_id}_{metric_name}_{int(datetime.now().timestamp())}"
                
                metric = QualityMetric(
                    metric_id=metric_id,
                    dataset_id=dataset_id,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    tags=tags or {},
                    metadata=metadata or {}
                )
                
                # Add to buffer
                self.metrics_buffer.append(metric)
                
                # Record in metrics collector
                self.metrics_collector.set_gauge(
                    f"data_quality_{metric_name}",
                    metric_value,
                    {"dataset_id": dataset_id, **metric.tags}
                )
                
                # Persist metric
                self._persist_metric(metric)
                
                return metric_id
                
        except Exception as e:
            self.logger.error(f"Failed to record quality metric: {e}")
            raise

    def create_alert_rule(
        self,
        name: str,
        dataset_pattern: str,
        metric_name: str,
        **kwargs
    ) -> str:
        """Create a new alert rule"""
        try:
            with self._lock:
                rule_id = f"rule_{int(datetime.now().timestamp())}"
                
                rule = AlertRule(
                    rule_id=rule_id,
                    name=name,
                    dataset_pattern=dataset_pattern,
                    metric_name=metric_name,
                    description=kwargs.get('description', ''),
                    **{k: v for k, v in kwargs.items() if hasattr(AlertRule, k)}
                )
                
                self.alert_rules[rule_id] = rule
                
                # Persist rule
                self._persist_alert_rule(rule)
                
                self.logger.info(f"Created alert rule: {name} ({rule_id})")
                return rule_id
                
        except Exception as e:
            self.logger.error(f"Failed to create alert rule: {e}")
            raise

    def update_alert_rule(
        self,
        rule_id: str,
        updates: dict[str, Any]
    ) -> bool:
        """Update an existing alert rule"""
        try:
            with self._lock:
                if rule_id not in self.alert_rules:
                    raise ValueError(f"Alert rule {rule_id} not found")
                
                rule = self.alert_rules[rule_id]
                
                # Apply updates
                for field, value in updates.items():
                    if hasattr(rule, field):
                        setattr(rule, field, value)
                
                rule.updated_at = datetime.now()
                
                # Persist changes
                self._persist_alert_rule(rule)
                
                self.logger.info(f"Updated alert rule: {rule_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update alert rule {rule_id}: {e}")
            return False

    def get_dashboard_data(self, dashboard_id: str = "main") -> MonitoringDashboard:
        """Get real-time dashboard data"""
        try:
            with self._lock:
                dashboard = MonitoringDashboard(
                    dashboard_id=dashboard_id,
                    name="Data Quality Dashboard",
                    description="Real-time data quality monitoring"
                )
                
                # Calculate current status
                dataset_stats = self._calculate_dataset_statistics()
                dashboard.total_datasets = dataset_stats['total']
                dashboard.healthy_datasets = dataset_stats['healthy']
                dashboard.warning_datasets = dataset_stats['warning']
                dashboard.critical_datasets = dataset_stats['critical']
                
                # Alert statistics
                dashboard.active_alerts = len(self.active_alerts)
                dashboard.critical_alerts = len([
                    a for a in self.active_alerts.values()
                    if a.severity == AlertSeverity.CRITICAL
                ])
                
                # Alerts in last 24 hours
                last_24h = datetime.now() - timedelta(hours=24)
                dashboard.alerts_last_24h = len([
                    a for a in self.alert_history
                    if a.triggered_at >= last_24h
                ])
                
                # Quality metrics
                dashboard.overall_quality_score = self._calculate_overall_quality_score()
                dashboard.quality_trend = self._analyze_quality_trend()
                
                # Recent data
                dashboard.recent_alerts = list(self.active_alerts.values())[-10:]
                dashboard.recent_metrics = list(self.metrics_buffer)[-20:]
                
                # Trend data for charts
                dashboard.quality_trend_data = self._get_quality_trend_data()
                dashboard.alert_trend_data = self._get_alert_trend_data()
                
                dashboard.last_updated = datetime.now()
                
                return dashboard
                
        except Exception as e:
            self.logger.error(f"Failed to get dashboard data: {e}")
            return MonitoringDashboard(dashboard_id=dashboard_id, name="Error")

    def acknowledge_alert(
        self,
        alert_id: str,
        acknowledged_by: str,
        notes: str | None = None
    ) -> bool:
        """Acknowledge an active alert"""
        try:
            with self._lock:
                if alert_id not in self.active_alerts:
                    raise ValueError(f"Active alert {alert_id} not found")
                
                alert = self.active_alerts[alert_id]
                alert.acknowledged_at = datetime.now()
                alert.acknowledged_by = acknowledged_by
                
                if notes:
                    alert.metadata['acknowledgment_notes'] = notes
                
                # Persist updated alert
                self._persist_active_alert(alert)
                
                self.logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to acknowledge alert {alert_id}: {e}")
            return False

    def resolve_alert(
        self,
        alert_id: str,
        resolved_by: str,
        resolution_notes: str | None = None
    ) -> bool:
        """Resolve an active alert"""
        try:
            with self._lock:
                if alert_id not in self.active_alerts:
                    raise ValueError(f"Active alert {alert_id} not found")
                
                alert = self.active_alerts[alert_id]
                alert.resolved_at = datetime.now()
                alert.resolved_by = resolved_by
                
                if resolution_notes:
                    alert.metadata['resolution_notes'] = resolution_notes
                
                # Move to history
                self.alert_history.append(alert)
                del self.active_alerts[alert_id]
                
                # Remove active alert file and archive
                active_file = self.alerts_path / f"active_{alert_id}.json"
                if active_file.exists():
                    active_file.unlink()
                
                self._persist_alert_to_history(alert)
                
                self.logger.info(f"Alert {alert_id} resolved by {resolved_by}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to resolve alert {alert_id}: {e}")
            return False

    def get_quality_trends(
        self,
        dataset_id: str | None = None,
        metric_name: str | None = None,
        time_range_hours: int = 24
    ) -> dict[str, Any]:
        """Get quality trends and analysis"""
        try:
            with self._lock:
                cutoff_time = datetime.now() - timedelta(hours=time_range_hours)
                
                # Filter metrics
                relevant_metrics = [
                    m for m in self.metrics_buffer
                    if m.timestamp >= cutoff_time
                    and (not dataset_id or m.dataset_id == dataset_id)
                    and (not metric_name or m.metric_name == metric_name)
                ]
                
                if not relevant_metrics:
                    return {"message": "No metrics found for specified criteria"}
                
                # Group by metric name
                metrics_by_name = defaultdict(list)
                for metric in relevant_metrics:
                    metrics_by_name[metric.metric_name].append(metric)
                
                # Calculate trends
                trends = {}
                for name, metrics in metrics_by_name.items():
                    # Sort by timestamp
                    sorted_metrics = sorted(metrics, key=lambda m: m.timestamp)
                    values = [m.metric_value for m in sorted_metrics]
                    
                    if len(values) >= 2:
                        # Simple trend calculation
                        first_half = values[:len(values)//2]
                        second_half = values[len(values)//2:]
                        
                        first_avg = sum(first_half) / len(first_half)
                        second_avg = sum(second_half) / len(second_half)
                        
                        trend_direction = "improving" if second_avg > first_avg else "declining"
                        if abs(second_avg - first_avg) < 0.01:
                            trend_direction = "stable"
                        
                        trends[name] = {
                            'direction': trend_direction,
                            'change_percentage': ((second_avg - first_avg) / first_avg * 100) if first_avg != 0 else 0,
                            'current_value': values[-1],
                            'min_value': min(values),
                            'max_value': max(values),
                            'avg_value': sum(values) / len(values),
                            'data_points': len(values),
                            'time_series': [
                                {
                                    'timestamp': m.timestamp.isoformat(),
                                    'value': m.metric_value
                                } for m in sorted_metrics[-50:]  # Last 50 points
                            ]
                        }
                
                return {
                    'time_range_hours': time_range_hours,
                    'total_metrics': len(relevant_metrics),
                    'metric_trends': trends
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get quality trends: {e}")
            return {"error": str(e)}

    def get_alert_summary(
        self,
        time_range_hours: int = 24
    ) -> dict[str, Any]:
        """Get alert summary and statistics"""
        try:
            with self._lock:
                cutoff_time = datetime.now() - timedelta(hours=time_range_hours)
                
                # Recent alerts from history
                recent_alerts = [
                    a for a in self.alert_history
                    if a.triggered_at >= cutoff_time
                ]
                
                # Add active alerts
                all_alerts = list(self.active_alerts.values()) + recent_alerts
                
                if not all_alerts:
                    return {"message": "No alerts in specified time range"}
                
                # Statistics
                total_alerts = len(all_alerts)
                active_count = len(self.active_alerts)
                resolved_count = len([a for a in all_alerts if a.resolved_at])
                
                # By severity
                by_severity = defaultdict(int)
                for alert in all_alerts:
                    by_severity[alert.severity.value] += 1
                
                # By dataset
                by_dataset = defaultdict(int)
                for alert in all_alerts:
                    by_dataset[alert.dataset_id] += 1
                
                # By metric
                by_metric = defaultdict(int)
                for alert in all_alerts:
                    by_metric[alert.metric_name] += 1
                
                # Resolution times for resolved alerts
                resolution_times = []
                for alert in recent_alerts:
                    if alert.resolved_at:
                        resolution_time = (alert.resolved_at - alert.triggered_at).total_seconds() / 3600
                        resolution_times.append(resolution_time)
                
                avg_resolution_time = (
                    sum(resolution_times) / len(resolution_times) 
                    if resolution_times else 0
                )
                
                return {
                    'time_range_hours': time_range_hours,
                    'summary': {
                        'total_alerts': total_alerts,
                        'active_alerts': active_count,
                        'resolved_alerts': resolved_count,
                        'avg_resolution_time_hours': round(avg_resolution_time, 2)
                    },
                    'by_severity': dict(by_severity),
                    'by_dataset': dict(sorted(by_dataset.items(), key=lambda x: x[1], reverse=True)[:10]),
                    'by_metric': dict(sorted(by_metric.items(), key=lambda x: x[1], reverse=True)[:10]),
                    'recent_critical_alerts': [
                        {
                            'alert_id': a.alert_id,
                            'dataset_id': a.dataset_id,
                            'metric_name': a.metric_name,
                            'title': a.title,
                            'triggered_at': a.triggered_at.isoformat(),
                            'resolved': a.resolved_at is not None
                        }
                        for a in all_alerts
                        if a.severity == AlertSeverity.CRITICAL
                    ][-10:]
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get alert summary: {e}")
            return {"error": str(e)}

    def _metric_evaluation_loop(self):
        """Background thread for evaluating metrics against alert rules"""
        while self.monitoring_status == MonitoringStatus.ACTIVE:
            try:
                time.sleep(30)  # Evaluate every 30 seconds
                
                # Get recent metrics (last 5 minutes)
                recent_time = datetime.now() - timedelta(minutes=5)
                recent_metrics = [
                    m for m in self.metrics_buffer
                    if m.timestamp >= recent_time
                ]
                
                # Evaluate each metric against rules
                for metric in recent_metrics:
                    self._evaluate_metric_against_rules(metric)
                
            except Exception as e:
                self.logger.error(f"Metric evaluation loop error: {e}")
                time.sleep(60)  # Wait longer on error

    def _evaluate_metric_against_rules(self, metric: QualityMetric):
        """Evaluate a single metric against all applicable alert rules"""
        try:
            for rule in self.alert_rules.values():
                if not rule.active:
                    continue
                
                # Check if rule applies to this metric
                if not self._rule_applies_to_metric(rule, metric):
                    continue
                
                # Check thresholds
                violation = self._check_threshold_violation(rule, metric)
                if violation:
                    self._trigger_alert(rule, metric, violation)
                    
        except Exception as e:
            self.logger.error(f"Metric rule evaluation failed: {e}")

    def _rule_applies_to_metric(self, rule: AlertRule, metric: QualityMetric) -> bool:
        """Check if alert rule applies to the given metric"""
        import re
        
        try:
            # Check metric name match
            if rule.metric_name != metric.metric_name:
                return False
            
            # Check dataset pattern match
            if not re.match(rule.dataset_pattern, metric.dataset_id):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Rule applicability check failed: {e}")
            return False

    def _check_threshold_violation(
        self, 
        rule: AlertRule, 
        metric: QualityMetric
    ) -> dict[str, Any] | None:
        """Check if metric violates rule thresholds"""
        try:
            violations = []
            
            # Min threshold check
            if rule.min_threshold is not None and metric.metric_value < rule.min_threshold:
                violations.append({
                    'type': 'min_threshold',
                    'threshold_value': rule.min_threshold,
                    'actual_value': metric.metric_value,
                    'severity': rule.severity
                })
            
            # Max threshold check
            if rule.max_threshold is not None and metric.metric_value > rule.max_threshold:
                violations.append({
                    'type': 'max_threshold',
                    'threshold_value': rule.max_threshold,
                    'actual_value': metric.metric_value,
                    'severity': rule.severity
                })
            
            # Percentage change check (requires historical data)
            if rule.percentage_change_threshold is not None:
                historical_avg = self._get_historical_average(
                    metric.dataset_id, 
                    metric.metric_name,
                    hours=24
                )
                
                if historical_avg is not None and historical_avg != 0:
                    change_percent = abs((metric.metric_value - historical_avg) / historical_avg * 100)
                    if change_percent > rule.percentage_change_threshold:
                        violations.append({
                            'type': 'percentage_change',
                            'threshold_value': rule.percentage_change_threshold,
                            'actual_change': change_percent,
                            'historical_average': historical_avg,
                            'severity': rule.severity
                        })
            
            return violations[0] if violations else None
            
        except Exception as e:
            self.logger.error(f"Threshold violation check failed: {e}")
            return None

    def _trigger_alert(
        self,
        rule: AlertRule,
        metric: QualityMetric,
        violation: dict[str, Any]
    ):
        """Trigger a new alert"""
        try:
            with self._lock:
                # Check cooldown period
                if self._is_in_cooldown(rule, metric):
                    return
                
                alert_id = f"alert_{int(datetime.now().timestamp())}"
                
                # Create alert
                alert = QualityAlert(
                    alert_id=alert_id,
                    dataset_id=metric.dataset_id,
                    metric_name=metric.metric_name,
                    severity=rule.severity,
                    title=f"{rule.name}: {violation['type']} violation",
                    description=f"Metric {metric.metric_name} value {violation['actual_value']} "
                              f"violates {violation['type']} threshold {violation['threshold_value']}",
                    triggered_value=metric.metric_value,
                    threshold_value=violation['threshold_value'],
                    threshold_type=violation['type'],
                    alert_rule_id=rule.rule_id,
                    remediation_steps=self._get_remediation_steps(rule, violation)
                )
                
                # Store alert
                self.active_alerts[alert_id] = alert
                
                # Persist alert
                self._persist_active_alert(alert)
                
                # Send notifications
                self._send_alert_notifications(alert, rule)
                
                self.logger.warning(
                    f"Alert triggered: {alert.title} for dataset {alert.dataset_id}"
                )
                
        except Exception as e:
            self.logger.error(f"Alert triggering failed: {e}")

    def _is_in_cooldown(self, rule: AlertRule, metric: QualityMetric) -> bool:
        """Check if rule is in cooldown period for this metric"""
        try:
            cooldown_time = datetime.now() - timedelta(minutes=rule.cooldown_minutes)
            
            # Check recent alerts for this rule + metric combination
            recent_alerts = [
                a for a in self.alert_history
                if (a.alert_rule_id == rule.rule_id and
                    a.dataset_id == metric.dataset_id and
                    a.metric_name == metric.metric_name and
                    a.triggered_at >= cooldown_time)
            ]
            
            return len(recent_alerts) > 0
            
        except Exception as e:
            self.logger.error(f"Cooldown check failed: {e}")
            return False

    def _get_historical_average(
        self,
        dataset_id: str,
        metric_name: str,
        hours: int = 24
    ) -> float | None:
        """Get historical average for a metric"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            historical_values = [
                m.metric_value for m in self.metrics_buffer
                if (m.dataset_id == dataset_id and
                    m.metric_name == metric_name and
                    m.timestamp >= cutoff_time)
            ]
            
            return sum(historical_values) / len(historical_values) if historical_values else None
            
        except Exception as e:
            self.logger.error(f"Historical average calculation failed: {e}")
            return None

    def _get_remediation_steps(
        self,
        rule: AlertRule,
        violation: dict[str, Any]
    ) -> list[str]:
        """Get recommended remediation steps for an alert"""
        steps = []
        
        violation_type = violation['type']
        metric_name = rule.metric_name
        
        if violation_type == 'min_threshold':
            if 'completeness' in metric_name.lower():
                steps.extend([
                    "Check data source availability",
                    "Verify ETL pipeline execution",
                    "Review data validation rules"
                ])
            elif 'accuracy' in metric_name.lower():
                steps.extend([
                    "Review data transformation logic",
                    "Check source data quality",
                    "Validate business rules"
                ])
        
        elif violation_type == 'max_threshold':
            if 'error' in metric_name.lower():
                steps.extend([
                    "Investigate error logs",
                    "Check system resources",
                    "Review recent code changes"
                ])
        
        # Default steps
        if not steps:
            steps = [
                "Investigate the root cause",
                "Check recent data changes",
                "Review system logs",
                "Contact data owner if needed"
            ]
        
        return steps

    def _send_alert_notifications(self, alert: QualityAlert, rule: AlertRule):
        """Send alert notifications through configured channels"""
        try:
            # This is a placeholder for notification integration
            # In practice, integrate with email, Slack, PagerDuty, etc.
            
            notification_payload = {
                'alert_id': alert.alert_id,
                'title': alert.title,
                'description': alert.description,
                'severity': alert.severity.value,
                'dataset_id': alert.dataset_id,
                'metric_name': alert.metric_name,
                'triggered_at': alert.triggered_at.isoformat()
            }
            
            for channel in rule.notification_channels:
                self.logger.info(f"Sending alert {alert.alert_id} to channel {channel}")
                # Implement channel-specific notification logic
                
        except Exception as e:
            self.logger.error(f"Alert notification sending failed: {e}")

    def _alert_management_loop(self):
        """Background thread for alert management and escalation"""
        while self.monitoring_status == MonitoringStatus.ACTIVE:
            try:
                time.sleep(300)  # Check every 5 minutes
                
                # Check for escalation
                self._check_alert_escalation()
                
                # Auto-resolve stale alerts
                self._auto_resolve_stale_alerts()
                
            except Exception as e:
                self.logger.error(f"Alert management loop error: {e}")

    def _check_alert_escalation(self):
        """Check alerts for escalation based on rules"""
        try:
            with self._lock:
                now = datetime.now()
                
                for alert in self.active_alerts.values():
                    if alert.acknowledged_at:
                        continue  # Don't escalate acknowledged alerts
                    
                    # Get associated rule
                    rule = self.alert_rules.get(alert.alert_rule_id)
                    if not rule:
                        continue
                    
                    # Check escalation time
                    escalation_time = alert.triggered_at + timedelta(minutes=rule.escalation_minutes)
                    
                    if now >= escalation_time and alert.severity != AlertSeverity.CRITICAL:
                        # Escalate alert
                        old_severity = alert.severity
                        alert.severity = AlertSeverity.CRITICAL
                        
                        # Update metadata
                        alert.metadata['escalated_from'] = old_severity.value
                        alert.metadata['escalated_at'] = now.isoformat()
                        
                        # Persist update
                        self._persist_active_alert(alert)
                        
                        self.logger.warning(f"Escalated alert {alert.alert_id} to CRITICAL")
                        
        except Exception as e:
            self.logger.error(f"Alert escalation check failed: {e}")

    def _auto_resolve_stale_alerts(self):
        """Auto-resolve stale alerts based on current metrics"""
        try:
            with self._lock:
                current_time = datetime.now()
                stale_threshold = timedelta(hours=24)
                
                alerts_to_resolve = []
                
                for alert_id, alert in self.active_alerts.items():
                    # Check if alert is stale
                    if current_time - alert.triggered_at > stale_threshold:
                        # Check if conditions still exist
                        recent_metrics = [
                            m for m in self.metrics_buffer
                            if (m.dataset_id == alert.dataset_id and
                                m.metric_name == alert.metric_name and
                                m.timestamp >= current_time - timedelta(hours=1))
                        ]
                        
                        if recent_metrics:
                            # Check if any recent metric still violates threshold
                            still_violating = False
                            for metric in recent_metrics:
                                if alert.threshold_type == 'min_threshold':
                                    if metric.metric_value < alert.threshold_value:
                                        still_violating = True
                                        break
                                elif alert.threshold_type == 'max_threshold':
                                    if metric.metric_value > alert.threshold_value:
                                        still_violating = True
                                        break
                            
                            if not still_violating:
                                alerts_to_resolve.append(alert_id)
                
                # Auto-resolve alerts
                for alert_id in alerts_to_resolve:
                    self.resolve_alert(
                        alert_id=alert_id,
                        resolved_by="system_auto_resolve",
                        resolution_notes="Auto-resolved: conditions no longer met"
                    )
                    
        except Exception as e:
            self.logger.error(f"Auto-resolve stale alerts failed: {e}")

    def _dashboard_update_loop(self):
        """Background thread for updating dashboard data"""
        while self.monitoring_status == MonitoringStatus.ACTIVE:
            try:
                time.sleep(self.dashboard_refresh_seconds)
                
                # Update dashboard cache
                dashboard_data = self.get_dashboard_data("main")
                
                # Persist dashboard snapshot
                self._persist_dashboard_snapshot(dashboard_data)
                
            except Exception as e:
                self.logger.error(f"Dashboard update loop error: {e}")

    def _cleanup_loop(self):
        """Background thread for data cleanup and maintenance"""
        while self.monitoring_status == MonitoringStatus.ACTIVE:
            try:
                time.sleep(3600)  # Run hourly
                
                # Clean old metrics
                self._cleanup_old_metrics()
                
                # Clean old alerts
                self._cleanup_old_alerts()
                
                # Compress old data
                self._compress_old_data()
                
            except Exception as e:
                self.logger.error(f"Cleanup loop error: {e}")

    def _cleanup_old_metrics(self):
        """Clean up old metric files"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.metric_retention_days)
            
            for metric_file in self.metrics_path.glob("*.json"):
                file_time = datetime.fromtimestamp(metric_file.stat().st_mtime)
                if file_time < cutoff_date:
                    metric_file.unlink()
                    
        except Exception as e:
            self.logger.error(f"Metric cleanup failed: {e}")

    def _cleanup_old_alerts(self):
        """Clean up old alert files"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.alert_retention_days)
            
            for alert_file in self.alerts_path.glob("history_*.json"):
                file_time = datetime.fromtimestamp(alert_file.stat().st_mtime)
                if file_time < cutoff_date:
                    alert_file.unlink()
                    
        except Exception as e:
            self.logger.error(f"Alert cleanup failed: {e}")

    def _compress_old_data(self):
        """Compress old data files for storage efficiency"""
        # Placeholder for data compression logic
        pass

    # Statistics and analysis methods
    def _calculate_dataset_statistics(self) -> dict[str, int]:
        """Calculate dataset health statistics"""
        try:
            # Get recent metrics per dataset
            recent_time = datetime.now() - timedelta(hours=1)
            recent_metrics = [
                m for m in self.metrics_buffer
                if m.timestamp >= recent_time
            ]
            
            # Group by dataset
            datasets = defaultdict(list)
            for metric in recent_metrics:
                datasets[metric.dataset_id].append(metric)
            
            total = len(datasets)
            healthy = 0
            warning = 0
            critical = 0
            
            for dataset_id, metrics in datasets.items():
                # Calculate average quality score for dataset
                quality_scores = [
                    m.metric_value for m in metrics
                    if 'quality_score' in m.metric_name.lower()
                ]
                
                if quality_scores:
                    avg_quality = sum(quality_scores) / len(quality_scores)
                    if avg_quality >= 0.9:
                        healthy += 1
                    elif avg_quality >= 0.7:
                        warning += 1
                    else:
                        critical += 1
                else:
                    # If no quality scores, check for active alerts
                    dataset_alerts = [
                        a for a in self.active_alerts.values()
                        if a.dataset_id == dataset_id
                    ]
                    
                    if any(a.severity == AlertSeverity.CRITICAL for a in dataset_alerts):
                        critical += 1
                    elif dataset_alerts:
                        warning += 1
                    else:
                        healthy += 1
            
            return {
                'total': total,
                'healthy': healthy,
                'warning': warning,
                'critical': critical
            }
            
        except Exception as e:
            self.logger.error(f"Dataset statistics calculation failed: {e}")
            return {'total': 0, 'healthy': 0, 'warning': 0, 'critical': 0}

    def _calculate_overall_quality_score(self) -> float:
        """Calculate overall quality score across all datasets"""
        try:
            recent_time = datetime.now() - timedelta(hours=1)
            quality_metrics = [
                m.metric_value for m in self.metrics_buffer
                if (m.timestamp >= recent_time and
                    'quality_score' in m.metric_name.lower())
            ]
            
            return sum(quality_metrics) / len(quality_metrics) if quality_metrics else 0.0
            
        except Exception as e:
            self.logger.error(f"Overall quality score calculation failed: {e}")
            return 0.0

    def _analyze_quality_trend(self) -> str:
        """Analyze overall quality trend"""
        try:
            # Get quality scores from last 24 hours
            last_24h = datetime.now() - timedelta(hours=24)
            quality_metrics = [
                m for m in self.metrics_buffer
                if (m.timestamp >= last_24h and
                    'quality_score' in m.metric_name.lower())
            ]
            
            if len(quality_metrics) < 10:
                return "insufficient_data"
            
            # Split into two halves and compare
            sorted_metrics = sorted(quality_metrics, key=lambda m: m.timestamp)
            mid = len(sorted_metrics) // 2
            
            first_half_avg = sum(m.metric_value for m in sorted_metrics[:mid]) / mid
            second_half_avg = sum(m.metric_value for m in sorted_metrics[mid:]) / (len(sorted_metrics) - mid)
            
            if second_half_avg > first_half_avg + 0.05:
                return "improving"
            elif second_half_avg < first_half_avg - 0.05:
                return "declining"
            else:
                return "stable"
                
        except Exception as e:
            self.logger.error(f"Quality trend analysis failed: {e}")
            return "unknown"

    def _get_quality_trend_data(self) -> list[dict[str, Any]]:
        """Get time series data for quality trends"""
        try:
            last_24h = datetime.now() - timedelta(hours=24)
            quality_metrics = [
                m for m in self.metrics_buffer
                if (m.timestamp >= last_24h and
                    'quality_score' in m.metric_name.lower())
            ]
            
            # Group by hour and calculate average
            hourly_data = defaultdict(list)
            for metric in quality_metrics:
                hour_key = metric.timestamp.replace(minute=0, second=0, microsecond=0)
                hourly_data[hour_key].append(metric.metric_value)
            
            trend_data = []
            for hour, values in sorted(hourly_data.items()):
                trend_data.append({
                    'timestamp': hour.isoformat(),
                    'value': sum(values) / len(values)
                })
            
            return trend_data[-24:]  # Last 24 hours
            
        except Exception as e:
            self.logger.error(f"Quality trend data generation failed: {e}")
            return []

    def _get_alert_trend_data(self) -> list[dict[str, Any]]:
        """Get time series data for alert trends"""
        try:
            last_24h = datetime.now() - timedelta(hours=24)
            
            # Get all alerts from last 24 hours
            recent_alerts = [
                a for a in self.alert_history
                if a.triggered_at >= last_24h
            ] + list(self.active_alerts.values())
            
            # Group by hour
            hourly_alerts = defaultdict(int)
            for alert in recent_alerts:
                hour_key = alert.triggered_at.replace(minute=0, second=0, microsecond=0)
                hourly_alerts[hour_key] += 1
            
            # Generate data points for all hours (including zeros)
            trend_data = []
            current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            
            for i in range(24):
                hour = current_time - timedelta(hours=i)
                alert_count = hourly_alerts.get(hour, 0)
                trend_data.append({
                    'timestamp': hour.isoformat(),
                    'value': alert_count
                })
            
            return list(reversed(trend_data))  # Chronological order
            
        except Exception as e:
            self.logger.error(f"Alert trend data generation failed: {e}")
            return []

    # Persistence methods
    def _persist_metric(self, metric: QualityMetric):
        """Persist metric to storage"""
        try:
            # Create daily metric files
            date_str = metric.timestamp.strftime("%Y%m%d")
            metric_file = self.metrics_path / f"metrics_{date_str}.json"
            
            # Load existing or create new
            if metric_file.exists():
                with open(metric_file) as f:
                    data = json.load(f)
            else:
                data = {'date': date_str, 'metrics': []}
            
            # Add metric
            data['metrics'].append(self._quality_metric_to_dict(metric))
            
            # Save
            with open(metric_file, 'w') as f:
                json.dump(data, f, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to persist metric: {e}")

    def _persist_active_alert(self, alert: QualityAlert):
        """Persist active alert to storage"""
        try:
            alert_file = self.alerts_path / f"active_{alert.alert_id}.json"
            with open(alert_file, 'w') as f:
                json.dump(self._quality_alert_to_dict(alert), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist active alert: {e}")

    def _persist_alert_to_history(self, alert: QualityAlert):
        """Persist resolved alert to history"""
        try:
            date_str = alert.triggered_at.strftime("%Y%m%d")
            history_file = self.alerts_path / f"history_{date_str}.json"
            
            # Load existing or create new
            if history_file.exists():
                with open(history_file) as f:
                    data = json.load(f)
            else:
                data = {'date': date_str, 'alerts': []}
            
            # Add alert
            data['alerts'].append(self._quality_alert_to_dict(alert))
            
            # Save
            with open(history_file, 'w') as f:
                json.dump(data, f, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to persist alert to history: {e}")

    def _persist_alert_rule(self, rule: AlertRule):
        """Persist alert rule to storage"""
        try:
            rule_file = self.rules_path / f"{rule.rule_id}.json"
            with open(rule_file, 'w') as f:
                json.dump(self._alert_rule_to_dict(rule), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist alert rule: {e}")

    def _persist_dashboard_snapshot(self, dashboard: MonitoringDashboard):
        """Persist dashboard snapshot"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            snapshot_file = self.dashboards_path / f"dashboard_{timestamp}.json"
            
            with open(snapshot_file, 'w') as f:
                json.dump(self._dashboard_to_dict(dashboard), f, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to persist dashboard snapshot: {e}")

    # Serialization helpers
    def _quality_metric_to_dict(self, metric: QualityMetric) -> dict[str, Any]:
        """Convert quality metric to dictionary"""
        return {
            'metric_id': metric.metric_id,
            'dataset_id': metric.dataset_id,
            'metric_name': metric.metric_name,
            'metric_value': metric.metric_value,
            'threshold_min': metric.threshold_min,
            'threshold_max': metric.threshold_max,
            'timestamp': metric.timestamp.isoformat(),
            'tags': metric.tags,
            'metadata': metric.metadata
        }

    def _dict_to_quality_metric(self, data: dict[str, Any]) -> QualityMetric:
        """Convert dictionary to quality metric"""
        return QualityMetric(
            metric_id=data['metric_id'],
            dataset_id=data['dataset_id'],
            metric_name=data['metric_name'],
            metric_value=data['metric_value'],
            threshold_min=data.get('threshold_min'),
            threshold_max=data.get('threshold_max'),
            timestamp=datetime.fromisoformat(data['timestamp']),
            tags=data.get('tags', {}),
            metadata=data.get('metadata', {})
        )

    def _quality_alert_to_dict(self, alert: QualityAlert) -> dict[str, Any]:
        """Convert quality alert to dictionary"""
        return {
            'alert_id': alert.alert_id,
            'dataset_id': alert.dataset_id,
            'metric_name': alert.metric_name,
            'severity': alert.severity.value,
            'title': alert.title,
            'description': alert.description,
            'triggered_value': alert.triggered_value,
            'threshold_value': alert.threshold_value,
            'threshold_type': alert.threshold_type,
            'triggered_at': alert.triggered_at.isoformat(),
            'acknowledged_at': alert.acknowledged_at.isoformat() if alert.acknowledged_at else None,
            'resolved_at': alert.resolved_at.isoformat() if alert.resolved_at else None,
            'acknowledged_by': alert.acknowledged_by,
            'resolved_by': alert.resolved_by,
            'affected_records': alert.affected_records,
            'business_impact': alert.business_impact,
            'remediation_steps': alert.remediation_steps,
            'alert_rule_id': alert.alert_rule_id,
            'tags': alert.tags,
            'metadata': alert.metadata
        }

    def _dict_to_quality_alert(self, data: dict[str, Any]) -> QualityAlert:
        """Convert dictionary to quality alert"""
        return QualityAlert(
            alert_id=data['alert_id'],
            dataset_id=data['dataset_id'],
            metric_name=data['metric_name'],
            severity=AlertSeverity(data['severity']),
            title=data['title'],
            description=data['description'],
            triggered_value=data['triggered_value'],
            threshold_value=data['threshold_value'],
            threshold_type=data['threshold_type'],
            triggered_at=datetime.fromisoformat(data['triggered_at']),
            acknowledged_at=datetime.fromisoformat(data['acknowledged_at']) if data.get('acknowledged_at') else None,
            resolved_at=datetime.fromisoformat(data['resolved_at']) if data.get('resolved_at') else None,
            acknowledged_by=data.get('acknowledged_by'),
            resolved_by=data.get('resolved_by'),
            affected_records=data.get('affected_records', 0),
            business_impact=data.get('business_impact'),
            remediation_steps=data.get('remediation_steps', []),
            alert_rule_id=data.get('alert_rule_id'),
            tags=data.get('tags', {}),
            metadata=data.get('metadata', {})
        )

    def _alert_rule_to_dict(self, rule: AlertRule) -> dict[str, Any]:
        """Convert alert rule to dictionary"""
        return {
            'rule_id': rule.rule_id,
            'name': rule.name,
            'description': rule.description,
            'dataset_pattern': rule.dataset_pattern,
            'metric_name': rule.metric_name,
            'min_threshold': rule.min_threshold,
            'max_threshold': rule.max_threshold,
            'percentage_change_threshold': rule.percentage_change_threshold,
            'severity': rule.severity.value,
            'cooldown_minutes': rule.cooldown_minutes,
            'escalation_minutes': rule.escalation_minutes,
            'consecutive_failures': rule.consecutive_failures,
            'evaluation_window_minutes': rule.evaluation_window_minutes,
            'notification_channels': rule.notification_channels,
            'auto_remediation': rule.auto_remediation,
            'active': rule.active,
            'created_at': rule.created_at.isoformat(),
            'updated_at': rule.updated_at.isoformat(),
            'tags': rule.tags,
            'metadata': rule.metadata
        }

    def _dict_to_alert_rule(self, data: dict[str, Any]) -> AlertRule:
        """Convert dictionary to alert rule"""
        return AlertRule(
            rule_id=data['rule_id'],
            name=data['name'],
            description=data['description'],
            dataset_pattern=data['dataset_pattern'],
            metric_name=data['metric_name'],
            min_threshold=data.get('min_threshold'),
            max_threshold=data.get('max_threshold'),
            percentage_change_threshold=data.get('percentage_change_threshold'),
            severity=AlertSeverity(data.get('severity', 'medium')),
            cooldown_minutes=data.get('cooldown_minutes', 60),
            escalation_minutes=data.get('escalation_minutes', 240),
            consecutive_failures=data.get('consecutive_failures', 1),
            evaluation_window_minutes=data.get('evaluation_window_minutes', 5),
            notification_channels=data.get('notification_channels', []),
            auto_remediation=data.get('auto_remediation', False),
            active=data.get('active', True),
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at']),
            tags=data.get('tags', {}),
            metadata=data.get('metadata', {})
        )

    def _dashboard_to_dict(self, dashboard: MonitoringDashboard) -> dict[str, Any]:
        """Convert dashboard to dictionary"""
        return {
            'dashboard_id': dashboard.dashboard_id,
            'name': dashboard.name,
            'description': dashboard.description,
            'total_datasets': dashboard.total_datasets,
            'healthy_datasets': dashboard.healthy_datasets,
            'warning_datasets': dashboard.warning_datasets,
            'critical_datasets': dashboard.critical_datasets,
            'active_alerts': dashboard.active_alerts,
            'critical_alerts': dashboard.critical_alerts,
            'alerts_last_24h': dashboard.alerts_last_24h,
            'overall_quality_score': dashboard.overall_quality_score,
            'quality_trend': dashboard.quality_trend,
            'recent_alerts': [self._quality_alert_to_dict(a) for a in dashboard.recent_alerts],
            'recent_metrics': [self._quality_metric_to_dict(m) for m in dashboard.recent_metrics],
            'quality_trend_data': dashboard.quality_trend_data,
            'alert_trend_data': dashboard.alert_trend_data,
            'last_updated': dashboard.last_updated.isoformat()
        }