"""
Enterprise Observability Platform
Provides unified observability with metrics, logs, and traces correlation.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

try:
    import numpy as np
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

try:
    from prometheus_client import (
        CollectorRegistry, Counter, Gauge, Histogram, Summary,
        start_http_server, generate_latest
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

from core.logging import get_logger

logger = get_logger(__name__)


class ObservabilityLevel(Enum):
    """Observability monitoring levels."""
    BASIC = "basic"
    STANDARD = "standard"
    ADVANCED = "advanced"
    ENTERPRISE = "enterprise"


class MetricCategory(Enum):
    """Metric categories for organization."""
    INFRASTRUCTURE = "infrastructure"
    APPLICATION = "application"
    BUSINESS = "business"
    SECURITY = "security"
    DATA_QUALITY = "data_quality"
    USER_EXPERIENCE = "user_experience"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    MINOR = "minor"
    MAJOR = "major"
    CRITICAL = "critical"


@dataclass
class CorrelatedEvent:
    """Event with correlation across metrics, logs, and traces."""
    event_id: str
    timestamp: datetime
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    correlation_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    service_name: str = ""
    operation: str = ""
    level: str = "info"
    message: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SLIMetric:
    """Service Level Indicator metric definition."""
    name: str
    description: str
    query: str
    unit: str
    target_value: float
    threshold_warning: float
    threshold_critical: float
    category: MetricCategory
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SLO:
    """Service Level Objective definition."""
    name: str
    description: str
    sli_metrics: List[SLIMetric]
    target_percentage: float
    time_window: timedelta
    error_budget_percentage: float
    alert_rules: List[str] = field(default_factory=list)


@dataclass
class BusinessKPI:
    """Business Key Performance Indicator."""
    name: str
    description: str
    formula: str
    target_value: float
    current_value: float = 0.0
    trend: str = "stable"  # up, down, stable
    category: str = "revenue"
    unit: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)


class AnomalyDetector:
    """Advanced anomaly detection using statistical methods."""

    def __init__(self):
        self.baselines: Dict[str, Dict[str, Any]] = {}
        self.seasonal_patterns: Dict[str, Any] = {}
        self.lock = threading.RLock()
        self.logger = get_logger(self.__class__.__name__)

    def update_baseline(self, metric_name: str, values: List[float], window_hours: int = 24):
        """Update statistical baseline for a metric."""
        if not values or not SCIPY_AVAILABLE:
            return

        with self.lock:
            # Calculate statistical measures
            mean = np.mean(values)
            std = np.std(values)
            median = np.median(values)
            q25, q75 = np.percentile(values, [25, 75])
            iqr = q75 - q25

            # Detect seasonality if enough data points
            seasonal_period = None
            if len(values) > 144:  # At least 6 hours of 2.5min intervals
                seasonal_period = self._detect_seasonality(values)

            self.baselines[metric_name] = {
                'mean': mean,
                'std': std,
                'median': median,
                'q25': q25,
                'q75': q75,
                'iqr': iqr,
                'min': np.min(values),
                'max': np.max(values),
                'seasonal_period': seasonal_period,
                'updated_at': datetime.utcnow(),
                'sample_count': len(values)
            }

    def _detect_seasonality(self, values: List[float]) -> Optional[int]:
        """Detect seasonal patterns in time series data."""
        if not SCIPY_AVAILABLE or len(values) < 144:
            return None

        try:
            # Test for common seasonal periods (hourly, daily)
            periods_to_test = [24, 144, 288]  # 1h, 6h, 12h in 2.5min intervals
            best_period = None
            best_autocorr = 0

            for period in periods_to_test:
                if len(values) >= period * 2:
                    autocorr = self._calculate_autocorrelation(values, period)
                    if autocorr > best_autocorr and autocorr > 0.3:
                        best_autocorr = autocorr
                        best_period = period

            return best_period
        except Exception as e:
            self.logger.warning(f"Seasonality detection failed: {e}")
            return None

    def _calculate_autocorrelation(self, values: List[float], lag: int) -> float:
        """Calculate autocorrelation at given lag."""
        if len(values) <= lag:
            return 0

        try:
            series = np.array(values)
            n = len(series)
            series = series - np.mean(series)
            autocorr = np.correlate(series, series, mode='full')
            autocorr = autocorr[n-1:]
            autocorr = autocorr / autocorr[0]
            return float(autocorr[lag]) if lag < len(autocorr) else 0
        except Exception:
            return 0

    def detect_anomalies(self, metric_name: str, current_value: float,
                        sensitivity: float = 2.5) -> Dict[str, Any]:
        """Detect anomalies using multiple statistical methods."""
        with self.lock:
            if metric_name not in self.baselines:
                return {'is_anomaly': False, 'confidence': 0, 'methods': []}

            baseline = self.baselines[metric_name]
            anomaly_results = {'is_anomaly': False, 'confidence': 0, 'methods': []}
            detection_methods = []

            # Z-Score based detection
            if baseline['std'] > 0:
                z_score = abs(current_value - baseline['mean']) / baseline['std']
                is_zscore_anomaly = z_score > sensitivity
                detection_methods.append({
                    'method': 'z_score',
                    'is_anomaly': is_zscore_anomaly,
                    'score': z_score,
                    'threshold': sensitivity
                })

            # IQR-based detection (for robustness against outliers)
            iqr_threshold = baseline['iqr'] * 1.5
            is_iqr_anomaly = (current_value < baseline['q25'] - iqr_threshold or
                            current_value > baseline['q75'] + iqr_threshold)
            detection_methods.append({
                'method': 'iqr',
                'is_anomaly': is_iqr_anomaly,
                'lower_bound': baseline['q25'] - iqr_threshold,
                'upper_bound': baseline['q75'] + iqr_threshold
            })

            # Statistical significance test
            if baseline['sample_count'] > 30:
                # Using modified z-test for single observation
                modified_std = baseline['std'] * (1 + 1/baseline['sample_count'])**0.5
                if modified_std > 0:
                    t_score = abs(current_value - baseline['mean']) / modified_std
                    is_statistical_anomaly = t_score > stats.norm.ppf(1 - 0.05/2)  # 95% confidence
                    detection_methods.append({
                        'method': 'statistical_significance',
                        'is_anomaly': is_statistical_anomaly,
                        'score': t_score
                    })

            # Aggregate results
            anomaly_votes = sum(1 for method in detection_methods if method['is_anomaly'])
            anomaly_results['is_anomaly'] = anomaly_votes >= 2  # Majority vote
            anomaly_results['confidence'] = anomaly_votes / len(detection_methods)
            anomaly_results['methods'] = detection_methods
            anomaly_results['deviation_percentage'] = (
                abs(current_value - baseline['mean']) / baseline['mean'] * 100
                if baseline['mean'] != 0 else 0
            )

            return anomaly_results


class CorrelationEngine:
    """Engine for correlating events across metrics, logs, and traces."""

    def __init__(self):
        self.events: deque = deque(maxlen=100000)
        self.correlation_index: Dict[str, List[str]] = defaultdict(list)
        self.lock = threading.RLock()
        self.logger = get_logger(self.__class__.__name__)

    def add_event(self, event: CorrelatedEvent):
        """Add event with automatic correlation indexing."""
        with self.lock:
            self.events.append(event)

            # Index by various correlation keys
            if event.trace_id:
                self.correlation_index[f"trace:{event.trace_id}"].append(event.event_id)
            if event.correlation_id:
                self.correlation_index[f"correlation:{event.correlation_id}"].append(event.event_id)
            if event.user_id:
                self.correlation_index[f"user:{event.user_id}"].append(event.event_id)
            if event.session_id:
                self.correlation_index[f"session:{event.session_id}"].append(event.event_id)
            if event.service_name:
                self.correlation_index[f"service:{event.service_name}"].append(event.event_id)

            # Time-based indexing for temporal correlation
            time_bucket = event.timestamp.replace(second=0, microsecond=0)
            self.correlation_index[f"time:{time_bucket.isoformat()}"].append(event.event_id)

    def find_correlated_events(self, event: CorrelatedEvent,
                             time_window: timedelta = timedelta(minutes=5)) -> List[CorrelatedEvent]:
        """Find events correlated with the given event."""
        correlated_events = []
        correlation_keys = []

        # Build correlation keys
        if event.trace_id:
            correlation_keys.append(f"trace:{event.trace_id}")
        if event.correlation_id:
            correlation_keys.append(f"correlation:{event.correlation_id}")
        if event.user_id:
            correlation_keys.append(f"user:{event.user_id}")
        if event.session_id:
            correlation_keys.append(f"session:{event.session_id}")
        if event.service_name:
            correlation_keys.append(f"service:{event.service_name}")

        with self.lock:
            correlated_event_ids = set()

            # Find directly correlated events
            for key in correlation_keys:
                if key in self.correlation_index:
                    correlated_event_ids.update(self.correlation_index[key])

            # Find temporally correlated events
            time_start = event.timestamp - time_window
            time_end = event.timestamp + time_window
            current_time = time_start.replace(second=0, microsecond=0)

            while current_time <= time_end:
                time_key = f"time:{current_time.isoformat()}"
                if time_key in self.correlation_index:
                    correlated_event_ids.update(self.correlation_index[time_key])
                current_time += timedelta(minutes=1)

            # Retrieve actual events
            event_map = {event.event_id: event for event in self.events}
            for event_id in correlated_event_ids:
                if event_id in event_map and event_id != event.event_id:
                    correlated_event = event_map[event_id]
                    if time_start <= correlated_event.timestamp <= time_end:
                        correlated_events.append(correlated_event)

        return sorted(correlated_events, key=lambda x: x.timestamp)

    def analyze_correlation_patterns(self) -> Dict[str, Any]:
        """Analyze patterns in event correlations."""
        with self.lock:
            if not self.events:
                return {}

            # Analyze correlation key frequency
            key_frequency = defaultdict(int)
            service_interactions = defaultdict(lambda: defaultdict(int))
            error_correlations = defaultdict(int)

            for event in list(self.events):
                # Count correlation key usage
                if event.trace_id:
                    key_frequency['trace_id'] += 1
                if event.correlation_id:
                    key_frequency['correlation_id'] += 1
                if event.user_id:
                    key_frequency['user_id'] += 1

                # Analyze service interactions
                if event.service_name:
                    for other_event in list(self.events):
                        if (other_event.trace_id == event.trace_id and
                            other_event.service_name != event.service_name and
                            abs((other_event.timestamp - event.timestamp).total_seconds()) < 60):
                            service_interactions[event.service_name][other_event.service_name] += 1

                # Error correlation analysis
                if event.level in ['error', 'critical']:
                    error_correlations[event.service_name] += 1

            return {
                'total_events': len(self.events),
                'correlation_key_frequency': dict(key_frequency),
                'service_interactions': dict(service_interactions),
                'error_correlations': dict(error_correlations),
                'analysis_timestamp': datetime.utcnow().isoformat()
            }


class SLOManager:
    """Service Level Objective management and tracking."""

    def __init__(self):
        self.slos: Dict[str, SLO] = {}
        self.sli_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.error_budgets: Dict[str, float] = {}
        self.lock = threading.RLock()
        self.logger = get_logger(self.__class__.__name__)

    def register_slo(self, slo: SLO):
        """Register a new SLO for monitoring."""
        with self.lock:
            self.slos[slo.name] = slo
            self.error_budgets[slo.name] = slo.error_budget_percentage
            self.logger.info(f"Registered SLO: {slo.name} with target {slo.target_percentage}%")

    def record_sli_measurement(self, slo_name: str, sli_name: str, value: float, timestamp: Optional[datetime] = None):
        """Record an SLI measurement."""
        if timestamp is None:
            timestamp = datetime.utcnow()

        key = f"{slo_name}:{sli_name}"
        measurement = {
            'value': value,
            'timestamp': timestamp,
            'slo_name': slo_name,
            'sli_name': sli_name
        }

        with self.lock:
            self.sli_history[key].append(measurement)

    def calculate_slo_compliance(self, slo_name: str, time_window: Optional[timedelta] = None) -> Dict[str, Any]:
        """Calculate SLO compliance for a given time window."""
        with self.lock:
            if slo_name not in self.slos:
                return {'error': f'SLO {slo_name} not found'}

            slo = self.slos[slo_name]
            if time_window is None:
                time_window = slo.time_window

            cutoff_time = datetime.utcnow() - time_window
            compliance_results = {}

            for sli_metric in slo.sli_metrics:
                key = f"{slo_name}:{sli_metric.name}"
                if key not in self.sli_history:
                    continue

                # Filter measurements within time window
                recent_measurements = [
                    m for m in self.sli_history[key]
                    if m['timestamp'] >= cutoff_time
                ]

                if not recent_measurements:
                    continue

                # Calculate compliance based on SLI target
                compliant_measurements = [
                    m for m in recent_measurements
                    if m['value'] <= sli_metric.target_value
                ]

                compliance_rate = len(compliant_measurements) / len(recent_measurements)
                compliance_results[sli_metric.name] = {
                    'compliance_rate': compliance_rate,
                    'target_rate': slo.target_percentage / 100,
                    'is_compliant': compliance_rate >= (slo.target_percentage / 100),
                    'total_measurements': len(recent_measurements),
                    'compliant_measurements': len(compliant_measurements),
                    'current_value': recent_measurements[-1]['value'] if recent_measurements else 0,
                    'target_value': sli_metric.target_value
                }

            # Calculate overall SLO compliance
            if compliance_results:
                overall_compliance = sum(r['compliance_rate'] for r in compliance_results.values()) / len(compliance_results)
                is_overall_compliant = all(r['is_compliant'] for r in compliance_results.values())

                # Calculate error budget consumption
                error_budget_consumed = max(0, (1 - overall_compliance) / (slo.error_budget_percentage / 100))

                return {
                    'slo_name': slo_name,
                    'overall_compliance_rate': overall_compliance,
                    'target_compliance_rate': slo.target_percentage / 100,
                    'is_compliant': is_overall_compliant,
                    'error_budget_consumed_percentage': min(100, error_budget_consumed * 100),
                    'error_budget_remaining_percentage': max(0, 100 - error_budget_consumed * 100),
                    'time_window_hours': time_window.total_seconds() / 3600,
                    'sli_results': compliance_results,
                    'calculated_at': datetime.utcnow().isoformat()
                }

            return {'error': 'No SLI data available for compliance calculation'}

    def get_error_budget_status(self, slo_name: str) -> Dict[str, Any]:
        """Get current error budget status."""
        compliance_result = self.calculate_slo_compliance(slo_name)
        if 'error' in compliance_result:
            return compliance_result

        error_budget_remaining = compliance_result['error_budget_remaining_percentage']
        status = 'healthy'

        if error_budget_remaining <= 10:
            status = 'critical'
        elif error_budget_remaining <= 25:
            status = 'warning'
        elif error_budget_remaining <= 50:
            status = 'caution'

        return {
            'slo_name': slo_name,
            'error_budget_remaining_percentage': error_budget_remaining,
            'error_budget_consumed_percentage': compliance_result['error_budget_consumed_percentage'],
            'status': status,
            'recommendation': self._get_error_budget_recommendation(status, error_budget_remaining)
        }

    def _get_error_budget_recommendation(self, status: str, remaining_percentage: float) -> str:
        """Get recommendation based on error budget status."""
        if status == 'critical':
            return "Error budget critically low. Immediate action required to improve reliability."
        elif status == 'warning':
            return "Error budget running low. Consider reducing deployment frequency and focusing on reliability."
        elif status == 'caution':
            return "Error budget usage is elevated. Monitor closely and consider reliability improvements."
        else:
            return "Error budget is healthy. Continue normal operations."


class BusinessMetricsCollector:
    """Collector for business-level KPIs and metrics."""

    def __init__(self):
        self.kpis: Dict[str, BusinessKPI] = {}
        self.kpi_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.lock = threading.RLock()
        self.logger = get_logger(self.__class__.__name__)

    def register_kpi(self, kpi: BusinessKPI):
        """Register a business KPI for tracking."""
        with self.lock:
            self.kpis[kpi.name] = kpi
            self.logger.info(f"Registered business KPI: {kpi.name}")

    def update_kpi_value(self, kpi_name: str, value: float, timestamp: Optional[datetime] = None):
        """Update KPI value and calculate trend."""
        if timestamp is None:
            timestamp = datetime.utcnow()

        with self.lock:
            if kpi_name not in self.kpis:
                self.logger.warning(f"KPI {kpi_name} not registered")
                return

            kpi = self.kpis[kpi_name]
            previous_value = kpi.current_value
            kpi.current_value = value
            kpi.timestamp = timestamp

            # Calculate trend
            if previous_value > 0:
                change_percentage = ((value - previous_value) / previous_value) * 100
                if abs(change_percentage) < 5:
                    kpi.trend = "stable"
                elif change_percentage > 0:
                    kpi.trend = "up"
                else:
                    kpi.trend = "down"

            # Record in history
            self.kpi_history[kpi_name].append({
                'value': value,
                'timestamp': timestamp,
                'trend': kpi.trend
            })

    def get_kpi_dashboard_data(self) -> Dict[str, Any]:
        """Get dashboard data for all business KPIs."""
        with self.lock:
            dashboard_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'kpis': {},
                'summary': {
                    'total_kpis': len(self.kpis),
                    'trending_up': 0,
                    'trending_down': 0,
                    'stable': 0
                }
            }

            for kpi_name, kpi in self.kpis.items():
                # Calculate target achievement
                target_achievement = (kpi.current_value / kpi.target_value * 100) if kpi.target_value > 0 else 0

                # Get recent trend data
                recent_history = list(self.kpi_history[kpi_name])[-20:]  # Last 20 measurements

                dashboard_data['kpis'][kpi_name] = {
                    'current_value': kpi.current_value,
                    'target_value': kpi.target_value,
                    'target_achievement_percentage': target_achievement,
                    'trend': kpi.trend,
                    'category': kpi.category,
                    'unit': kpi.unit,
                    'description': kpi.description,
                    'last_updated': kpi.timestamp.isoformat(),
                    'recent_history': recent_history
                }

                # Update summary counts
                if kpi.trend == "up":
                    dashboard_data['summary']['trending_up'] += 1
                elif kpi.trend == "down":
                    dashboard_data['summary']['trending_down'] += 1
                else:
                    dashboard_data['summary']['stable'] += 1

            return dashboard_data


class EnterpriseObservabilityPlatform:
    """Unified enterprise observability platform."""

    def __init__(self, level: ObservabilityLevel = ObservabilityLevel.ENTERPRISE):
        self.level = level
        self.anomaly_detector = AnomalyDetector()
        self.correlation_engine = CorrelationEngine()
        self.slo_manager = SLOManager()
        self.business_metrics = BusinessMetricsCollector()
        self.prometheus_metrics: Dict[str, Any] = {}
        self.lock = threading.RLock()
        self.logger = get_logger(self.__class__.__name__)

        # Initialize Prometheus metrics if available
        if PROMETHEUS_AVAILABLE:
            self._setup_prometheus_metrics()

        # Setup default SLOs
        self._setup_default_slos()

        # Setup default business KPIs
        self._setup_default_kpis()

    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics for enterprise monitoring."""
        if not PROMETHEUS_AVAILABLE:
            return

        self.prometheus_metrics = {
            # Infrastructure metrics
            'system_cpu_usage': Gauge('system_cpu_usage_percent', 'System CPU usage percentage'),
            'system_memory_usage': Gauge('system_memory_usage_percent', 'System memory usage percentage'),
            'system_disk_usage': Gauge('system_disk_usage_percent', 'System disk usage percentage'),
            
            # Application metrics
            'app_request_duration': Histogram('app_request_duration_seconds', 'Application request duration'),
            'app_request_rate': Counter('app_requests_total', 'Total application requests'),
            'app_error_rate': Counter('app_errors_total', 'Total application errors'),
            
            # Business metrics
            'business_revenue_daily': Gauge('business_revenue_daily_dollars', 'Daily revenue in dollars'),
            'business_active_users': Gauge('business_active_users_total', 'Total active users'),
            'business_conversion_rate': Gauge('business_conversion_rate_percent', 'Conversion rate percentage'),
            
            # Data quality metrics
            'data_quality_score': Gauge('data_quality_score_percent', 'Data quality score percentage'),
            'data_freshness_minutes': Gauge('data_freshness_minutes', 'Data freshness in minutes'),
            'etl_success_rate': Gauge('etl_success_rate_percent', 'ETL success rate percentage'),
            
            # Security metrics
            'security_threats_detected': Counter('security_threats_detected_total', 'Security threats detected'),
            'security_login_failures': Counter('security_login_failures_total', 'Login failures'),
            'security_compliance_score': Gauge('security_compliance_score_percent', 'Security compliance score')
        }

    def _setup_default_slos(self):
        """Setup default SLOs for enterprise monitoring."""
        # API Availability SLO
        api_slo = SLO(
            name="api_availability",
            description="API availability and response time SLO",
            sli_metrics=[
                SLIMetric(
                    name="response_time",
                    description="API response time",
                    query="avg(app_request_duration_seconds)",
                    unit="seconds",
                    target_value=0.5,  # 500ms
                    threshold_warning=0.8,
                    threshold_critical=1.0,
                    category=MetricCategory.APPLICATION
                ),
                SLIMetric(
                    name="error_rate",
                    description="API error rate",
                    query="rate(app_errors_total) / rate(app_requests_total)",
                    unit="percentage",
                    target_value=0.01,  # 1%
                    threshold_warning=0.05,
                    threshold_critical=0.10,
                    category=MetricCategory.APPLICATION
                )
            ],
            target_percentage=99.9,
            time_window=timedelta(hours=24),
            error_budget_percentage=0.1
        )
        self.slo_manager.register_slo(api_slo)

        # Data Pipeline SLO
        data_pipeline_slo = SLO(
            name="data_pipeline_reliability",
            description="Data pipeline reliability and freshness SLO",
            sli_metrics=[
                SLIMetric(
                    name="etl_success_rate",
                    description="ETL job success rate",
                    query="etl_success_rate_percent",
                    unit="percentage",
                    target_value=99.0,
                    threshold_warning=95.0,
                    threshold_critical=90.0,
                    category=MetricCategory.DATA_QUALITY
                ),
                SLIMetric(
                    name="data_freshness",
                    description="Data freshness",
                    query="data_freshness_minutes",
                    unit="minutes",
                    target_value=15.0,
                    threshold_warning=30.0,
                    threshold_critical=60.0,
                    category=MetricCategory.DATA_QUALITY
                )
            ],
            target_percentage=99.5,
            time_window=timedelta(hours=24),
            error_budget_percentage=0.5
        )
        self.slo_manager.register_slo(data_pipeline_slo)

    def _setup_default_kpis(self):
        """Setup default business KPIs."""
        # Revenue KPI
        revenue_kpi = BusinessKPI(
            name="daily_revenue",
            description="Daily revenue from all channels",
            formula="sum(order_value) where date = today",
            target_value=100000.0,  # $100K daily target
            category="revenue",
            unit="USD"
        )
        self.business_metrics.register_kpi(revenue_kpi)

        # User Engagement KPI
        engagement_kpi = BusinessKPI(
            name="daily_active_users",
            description="Daily active users",
            formula="count(distinct user_id) where last_activity_date = today",
            target_value=10000.0,  # 10K daily active users target
            category="engagement",
            unit="users"
        )
        self.business_metrics.register_kpi(engagement_kpi)

        # Data Quality KPI
        data_quality_kpi = BusinessKPI(
            name="data_quality_score",
            description="Overall data quality score",
            formula="weighted_avg(quality_dimensions)",
            target_value=95.0,  # 95% data quality target
            category="data_quality",
            unit="percentage"
        )
        self.business_metrics.register_kpi(data_quality_kpi)

    def record_metric(self, name: str, value: float, category: MetricCategory,
                     tags: Dict[str, str] = None, timestamp: Optional[datetime] = None):
        """Record a metric with automatic anomaly detection and correlation."""
        if timestamp is None:
            timestamp = datetime.utcnow()

        # Create correlated event
        event = CorrelatedEvent(
            event_id=str(uuid.uuid4()),
            timestamp=timestamp,
            service_name=tags.get('service', 'unknown') if tags else 'unknown',
            operation=f"metric:{name}",
            level="info",
            message=f"Metric {name} recorded with value {value}",
            metrics={name: value},
            tags=tags or {},
            attributes={
                'category': category.value,
                'metric_name': name,
                'metric_value': value
            }
        )

        # Add to correlation engine
        self.correlation_engine.add_event(event)

        # Update Prometheus metrics if available
        if PROMETHEUS_AVAILABLE and name in self.prometheus_metrics:
            if hasattr(self.prometheus_metrics[name], 'set'):
                self.prometheus_metrics[name].set(value)
            elif hasattr(self.prometheus_metrics[name], 'inc'):
                self.prometheus_metrics[name].inc(value)
            elif hasattr(self.prometheus_metrics[name], 'observe'):
                self.prometheus_metrics[name].observe(value)

        # Perform anomaly detection
        anomaly_result = self.anomaly_detector.detect_anomalies(name, value)
        if anomaly_result['is_anomaly']:
            self._handle_anomaly_detection(name, value, anomaly_result, event)

    def _handle_anomaly_detection(self, metric_name: str, value: float,
                                anomaly_result: Dict[str, Any], event: CorrelatedEvent):
        """Handle detected anomaly."""
        anomaly_event = CorrelatedEvent(
            event_id=str(uuid.uuid4()),
            timestamp=event.timestamp,
            trace_id=event.trace_id,
            correlation_id=event.correlation_id,
            service_name=event.service_name,
            operation=f"anomaly_detected:{metric_name}",
            level="warning" if anomaly_result['confidence'] < 0.8 else "error",
            message=f"Anomaly detected in {metric_name}: {value} (confidence: {anomaly_result['confidence']:.2%})",
            attributes={
                'anomaly_methods': anomaly_result['methods'],
                'confidence': anomaly_result['confidence'],
                'deviation_percentage': anomaly_result['deviation_percentage'],
                'original_metric': metric_name,
                'original_value': value
            }
        )

        self.correlation_engine.add_event(anomaly_event)
        self.logger.warning(f"Anomaly detected: {anomaly_event.message}")

    @asynccontextmanager
    async def trace_operation(self, operation_name: str, service_name: str = "",
                            user_id: Optional[str] = None, correlation_id: Optional[str] = None):
        """Context manager for tracing operations with correlation."""
        trace_id = str(uuid.uuid4())
        span_id = str(uuid.uuid4())
        start_time = datetime.utcnow()

        start_event = CorrelatedEvent(
            event_id=str(uuid.uuid4()),
            timestamp=start_time,
            trace_id=trace_id,
            span_id=span_id,
            correlation_id=correlation_id,
            user_id=user_id,
            service_name=service_name,
            operation=operation_name,
            level="info",
            message=f"Operation started: {operation_name}",
            attributes={'operation_phase': 'start'}
        )
        self.correlation_engine.add_event(start_event)

        try:
            yield {
                'trace_id': trace_id,
                'span_id': span_id,
                'correlation_id': correlation_id,
                'start_time': start_time
            }

            # Record successful completion
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            success_event = CorrelatedEvent(
                event_id=str(uuid.uuid4()),
                timestamp=end_time,
                trace_id=trace_id,
                span_id=span_id,
                correlation_id=correlation_id,
                user_id=user_id,
                service_name=service_name,
                operation=operation_name,
                level="info",
                message=f"Operation completed: {operation_name}",
                metrics={'duration_seconds': duration},
                attributes={
                    'operation_phase': 'success',
                    'duration_seconds': duration
                }
            )
            self.correlation_engine.add_event(success_event)

            # Record performance metrics
            self.record_metric(
                f"operation_duration_seconds",
                duration,
                MetricCategory.APPLICATION,
                {'operation': operation_name, 'service': service_name, 'status': 'success'}
            )

        except Exception as e:
            # Record error
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            error_event = CorrelatedEvent(
                event_id=str(uuid.uuid4()),
                timestamp=end_time,
                trace_id=trace_id,
                span_id=span_id,
                correlation_id=correlation_id,
                user_id=user_id,
                service_name=service_name,
                operation=operation_name,
                level="error",
                message=f"Operation failed: {operation_name} - {str(e)}",
                metrics={'duration_seconds': duration},
                attributes={
                    'operation_phase': 'error',
                    'error_message': str(e),
                    'error_type': type(e).__name__,
                    'duration_seconds': duration
                }
            )
            self.correlation_engine.add_event(error_event)

            # Record error metrics
            self.record_metric(
                f"operation_duration_seconds",
                duration,
                MetricCategory.APPLICATION,
                {'operation': operation_name, 'service': service_name, 'status': 'error'}
            )

            raise

    def get_unified_dashboard_data(self) -> Dict[str, Any]:
        """Get unified dashboard data across all observability dimensions."""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'observability_level': self.level.value,
            'correlation_analytics': self.correlation_engine.analyze_correlation_patterns(),
            'business_kpis': self.business_metrics.get_kpi_dashboard_data(),
            'slo_status': {
                slo_name: self.slo_manager.calculate_slo_compliance(slo_name)
                for slo_name in self.slo_manager.slos.keys()
            },
            'error_budget_status': {
                slo_name: self.slo_manager.get_error_budget_status(slo_name)
                for slo_name in self.slo_manager.slos.keys()
            },
            'platform_health': {
                'total_events': len(self.correlation_engine.events),
                'active_slos': len(self.slo_manager.slos),
                'business_kpis': len(self.business_metrics.kpis),
                'anomaly_baselines': len(self.anomaly_detector.baselines)
            }
        }

    def export_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format."""
        if not PROMETHEUS_AVAILABLE:
            return "# Prometheus not available"

        from prometheus_client import generate_latest
        return generate_latest().decode('utf-8')

    def shutdown(self):
        """Shutdown observability platform."""
        self.logger.info("Shutting down Enterprise Observability Platform")


# Global platform instance
_platform: Optional[EnterpriseObservabilityPlatform] = None


def get_observability_platform() -> EnterpriseObservabilityPlatform:
    """Get global observability platform instance."""
    global _platform
    if _platform is None:
        _platform = EnterpriseObservabilityPlatform()
    return _platform


# Convenience decorators
def monitor_business_operation(operation_name: str, service_name: str = "",
                             user_id_param: Optional[str] = None):
    """Decorator for monitoring business operations."""
    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                platform = get_observability_platform()
                
                # Extract user_id from parameters if specified
                user_id = kwargs.get(user_id_param) if user_id_param else None
                correlation_id = kwargs.get('correlation_id')
                
                async with platform.trace_operation(
                    operation_name, service_name, user_id, correlation_id
                ) as trace_context:
                    return await func(*args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                # For sync functions, we'll create a simple event without async context
                platform = get_observability_platform()
                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    platform.record_metric(
                        f"{operation_name}_duration_seconds",
                        duration,
                        MetricCategory.BUSINESS,
                        {'operation': operation_name, 'service': service_name, 'status': 'success'}
                    )
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    
                    platform.record_metric(
                        f"{operation_name}_duration_seconds",
                        duration,
                        MetricCategory.BUSINESS,
                        {'operation': operation_name, 'service': service_name, 'status': 'error'}
                    )
                    
                    raise
            return sync_wrapper
    return decorator
