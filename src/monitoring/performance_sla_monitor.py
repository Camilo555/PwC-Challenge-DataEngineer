"""
Enterprise Performance SLA Monitor for <15ms API Response Time Validation
Provides microsecond-precision monitoring for production SLA compliance with comprehensive business impact tracking
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import json
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import statistics

from core.logging import get_logger
from core.config import get_settings

try:
    from datadog import initialize, statsd
    DATADOG_AVAILABLE = True
except ImportError:
    DATADOG_AVAILABLE = False

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class PerformanceMeasurement:
    """Individual performance measurement with microsecond precision."""
    timestamp: float  # Unix timestamp with microseconds
    endpoint: str
    method: str
    response_time_ms: float  # Precise to microseconds
    response_time_us: int  # Microseconds for ultra-precision
    status_code: int
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    cache_hit: bool = False
    cache_level: Optional[str] = None  # L0, L1, L2, L3
    business_story: Optional[str] = None
    request_size_bytes: int = 0
    response_size_bytes: int = 0
    cpu_usage_percent: float = 0.0
    memory_usage_mb: float = 0.0
    
    @property
    def is_sla_compliant(self) -> bool:
        """Check if this measurement meets <15ms SLA."""
        return self.response_time_ms < 15.0
    
    @property
    def performance_grade(self) -> str:
        """Grade performance: Excellent, Good, Warning, Critical."""
        if self.response_time_ms < 5.0:
            return "Excellent"
        elif self.response_time_ms < 10.0:
            return "Good"
        elif self.response_time_ms < 15.0:
            return "Warning"
        else:
            return "Critical"


@dataclass
class SLAMetrics:
    """SLA compliance metrics for monitoring periods."""
    total_requests: int
    compliant_requests: int
    sla_compliance_percentage: float
    avg_response_time_ms: float
    p50_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    fastest_response_ms: float
    slowest_response_ms: float
    cache_hit_rate: float
    business_impact_score: float  # 0-100 based on story values
    estimated_revenue_impact: float  # USD impact
    
    @property
    def meets_sla_target(self) -> bool:
        """Check if SLA compliance meets 95% target."""
        return self.sla_compliance_percentage >= 95.0


class CachePerformanceMonitor:
    """Monitor L0-L3 cache layer performance."""
    
    def __init__(self):
        self.cache_metrics = {
            'L0': {'hits': 0, 'misses': 0, 'avg_response_ms': 0.0},
            'L1': {'hits': 0, 'misses': 0, 'avg_response_ms': 0.0},
            'L2': {'hits': 0, 'misses': 0, 'avg_response_ms': 0.0},
            'L3': {'hits': 0, 'misses': 0, 'avg_response_ms': 0.0}
        }
        self.cache_response_times = defaultdict(list)
    
    def record_cache_access(self, cache_level: str, hit: bool, response_time_ms: float):
        """Record cache access performance."""
        if cache_level in self.cache_metrics:
            if hit:
                self.cache_metrics[cache_level]['hits'] += 1
            else:
                self.cache_metrics[cache_level]['misses'] += 1
            
            self.cache_response_times[cache_level].append(response_time_ms)
            
            # Update rolling average (last 1000 measurements)
            times = self.cache_response_times[cache_level][-1000:]
            self.cache_metrics[cache_level]['avg_response_ms'] = sum(times) / len(times)
    
    def get_cache_hit_rate(self, cache_level: str) -> float:
        """Get hit rate for specific cache level."""
        metrics = self.cache_metrics.get(cache_level, {})
        hits = metrics.get('hits', 0)
        misses = metrics.get('misses', 0)
        total = hits + misses
        return (hits / total * 100) if total > 0 else 0.0
    
    def get_overall_cache_hit_rate(self) -> float:
        """Get overall cache hit rate across all levels."""
        total_hits = sum(m['hits'] for m in self.cache_metrics.values())
        total_misses = sum(m['misses'] for m in self.cache_metrics.values())
        total = total_hits + total_misses
        return (total_hits / total * 100) if total > 0 else 0.0


class PerformanceSLAMonitor:
    """
    Enterprise Performance SLA Monitor
    
    Features:
    - Microsecond-precision response time tracking
    - Real-time SLA compliance monitoring
    - Cache performance analysis (L0-L3 levels)
    - Business impact correlation
    - Automated alerting on SLA breaches
    - Performance regression detection
    - Production traffic validation
    """
    
    def __init__(self, 
                 sla_threshold_ms: float = 15.0,
                 sla_target_percentage: float = 95.0,
                 measurement_window_minutes: int = 5,
                 enable_datadog: bool = True):
        
        self.sla_threshold_ms = sla_threshold_ms
        self.sla_target_percentage = sla_target_percentage
        self.measurement_window_minutes = measurement_window_minutes
        self.enable_datadog = enable_datadog and DATADOG_AVAILABLE
        
        # Performance data storage
        self.measurements: deque[PerformanceMeasurement] = deque(maxlen=10000)
        self.current_window_measurements: List[PerformanceMeasurement] = []
        
        # Cache monitoring
        self.cache_monitor = CachePerformanceMonitor()
        
        # Business story mappings and values
        self.story_values = {
            "1": {"name": "BI Dashboards", "value": 2.5e6},
            "2": {"name": "Data Quality", "value": 1.8e6},
            "3": {"name": "Security & Governance", "value": 3.2e6},
            "4": {"name": "API Performance", "value": 2.1e6},
            "4.1": {"name": "Mobile Analytics", "value": 2.8e6},
            "4.2": {"name": "AI/LLM Analytics", "value": 3.5e6},
            "6": {"name": "Advanced Security", "value": 4.2e6},
            "7": {"name": "Real-time Streaming", "value": 3.4e6},
            "8": {"name": "ML Operations", "value": 6.4e6}
        }
        
        # Alerting and regression detection
        self.alert_queue = queue.Queue()
        self.baseline_percentiles = {}
        self.regression_threshold = 0.20  # 20% degradation triggers alert
        
        # Threading for background processing
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.monitoring_active = False
        self.background_tasks = []
        
        # DataDog integration
        if self.enable_datadog:
            self._initialize_datadog()
        
        logger.info(f"Performance SLA Monitor initialized with {sla_threshold_ms}ms threshold, {sla_target_percentage}% target")
    
    def _initialize_datadog(self):
        """Initialize DataDog integration."""
        try:
            if not DATADOG_AVAILABLE:
                logger.warning("DataDog not available for SLA monitoring")
                return
            
            initialize(
                api_key=getattr(settings, 'DATADOG_API_KEY', ''),
                app_key=getattr(settings, 'DATADOG_APP_KEY', ''),
                host_name=getattr(settings, 'DD_AGENT_HOST', 'localhost'),
                statsd_host=getattr(settings, 'DD_AGENT_HOST', 'localhost'),
                statsd_port=int(getattr(settings, 'DD_DOGSTATSD_PORT', 8125))
            )
            logger.info("DataDog integration initialized for SLA monitoring")
        except Exception as e:
            logger.warning(f"Failed to initialize DataDog for SLA monitoring: {e}")
            self.enable_datadog = False
    
    def start_monitoring(self):
        """Start background monitoring tasks."""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        
        # Start background tasks
        self.background_tasks = [
            self.executor.submit(self._monitoring_loop),
            self.executor.submit(self._alert_processor)
        ]
        
        logger.info("Performance SLA monitoring started")
    
    def stop_monitoring(self):
        """Stop background monitoring tasks."""
        self.monitoring_active = False
        
        # Wait for background tasks to complete
        for task in self.background_tasks:
            try:
                task.result(timeout=5)
            except Exception as e:
                logger.warning(f"Background task cleanup error: {e}")
        
        self.executor.shutdown(wait=True)
        logger.info("Performance SLA monitoring stopped")
    
    def record_measurement(self, 
                          endpoint: str,
                          method: str,
                          response_time_seconds: float,
                          status_code: int,
                          **kwargs) -> PerformanceMeasurement:
        """
        Record a performance measurement with microsecond precision.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            response_time_seconds: Response time in seconds (high precision)
            status_code: HTTP status code
            **kwargs: Additional measurement data (user_id, cache_hit, etc.)
        
        Returns:
            PerformanceMeasurement object
        """
        # Convert to milliseconds and microseconds with high precision
        response_time_ms = response_time_seconds * 1000
        response_time_us = int(response_time_seconds * 1_000_000)
        
        # Determine business story from endpoint
        business_story = self._identify_business_story(endpoint)
        
        # Create measurement
        measurement = PerformanceMeasurement(
            timestamp=time.time(),
            endpoint=endpoint,
            method=method,
            response_time_ms=response_time_ms,
            response_time_us=response_time_us,
            status_code=status_code,
            user_id=kwargs.get('user_id'),
            session_id=kwargs.get('session_id'),
            cache_hit=kwargs.get('cache_hit', False),
            cache_level=kwargs.get('cache_level'),
            business_story=business_story,
            request_size_bytes=kwargs.get('request_size_bytes', 0),
            response_size_bytes=kwargs.get('response_size_bytes', 0),
            cpu_usage_percent=kwargs.get('cpu_usage_percent', 0.0),
            memory_usage_mb=kwargs.get('memory_usage_mb', 0.0)
        )
        
        # Store measurement
        self.measurements.append(measurement)
        self.current_window_measurements.append(measurement)
        
        # Record cache performance if applicable
        if measurement.cache_level:
            self.cache_monitor.record_cache_access(
                measurement.cache_level,
                measurement.cache_hit,
                measurement.response_time_ms
            )
        
        # Send to DataDog
        if self.enable_datadog:
            self._send_measurement_to_datadog(measurement)
        
        # Check for immediate SLA violations
        if not measurement.is_sla_compliant:
            self._queue_sla_violation_alert(measurement)
        
        return measurement
    
    def _identify_business_story(self, endpoint: str) -> Optional[str]:
        """Identify which BMAD business story an endpoint belongs to."""
        endpoint_story_mapping = {
            '/api/v1/sales': '1',  # BI Dashboards
            '/api/v1/analytics': '1',
            '/api/v1/datamart': '2',  # Data Quality
            '/api/v1/security': '3',  # Security & Governance
            '/api/v1/auth': '3',
            '/api/v2/analytics': '4',  # API Performance
            '/api/v1/mobile': '4.1',  # Mobile Analytics
            '/api/v1/ai': '4.2',  # AI/LLM Analytics
            '/api/v1/llm': '4.2',
            '/api/v1/advanced-security': '6',  # Advanced Security
            '/api/v1/streaming': '7',  # Real-time Streaming
            '/api/v1/ml': '8'  # ML Operations
        }
        
        for path_prefix, story_id in endpoint_story_mapping.items():
            if endpoint.startswith(path_prefix):
                return story_id
        return None
    
    def _send_measurement_to_datadog(self, measurement: PerformanceMeasurement):
        """Send performance measurement to DataDog."""
        try:
            tags = [
                f"endpoint:{measurement.endpoint}",
                f"method:{measurement.method}",
                f"status_code:{measurement.status_code}",
                f"sla_compliant:{measurement.is_sla_compliant}",
                f"performance_grade:{measurement.performance_grade}",
                f"cache_hit:{measurement.cache_hit}",
                f"environment:{getattr(settings, 'environment', 'development')}"
            ]
            
            if measurement.cache_level:
                tags.append(f"cache_level:{measurement.cache_level}")
            
            if measurement.business_story:
                story_info = self.story_values.get(measurement.business_story, {})
                tags.extend([
                    f"business_story:{measurement.business_story}",
                    f"story_name:{story_info.get('name', 'unknown')}",
                    f"story_value:{int(story_info.get('value', 0))}"
                ])
            
            # Send core metrics
            statsd.histogram('api.response_time_ms', measurement.response_time_ms, tags=tags)
            statsd.histogram('api.response_time_us', measurement.response_time_us, tags=tags)
            statsd.increment('api.requests_total', tags=tags)
            
            if measurement.is_sla_compliant:
                statsd.increment('api.sla_compliant', tags=tags)
            else:
                statsd.increment('api.sla_violations', tags=tags)
            
            # Business impact metrics
            if measurement.business_story:
                story_value = self.story_values.get(measurement.business_story, {}).get('value', 0)
                impact_score = 100 if measurement.is_sla_compliant else max(0, 100 - (measurement.response_time_ms / self.sla_threshold_ms * 100))
                
                statsd.gauge('business.story_performance_score', impact_score, tags=tags)
                statsd.gauge('business.story_value_at_risk', 
                           story_value * (1 - impact_score/100), tags=tags)
            
        except Exception as e:
            logger.warning(f"Failed to send measurement to DataDog: {e}")
    
    def _queue_sla_violation_alert(self, measurement: PerformanceMeasurement):
        """Queue an alert for SLA violation."""
        alert = {
            'type': 'sla_violation',
            'timestamp': measurement.timestamp,
            'endpoint': measurement.endpoint,
            'response_time_ms': measurement.response_time_ms,
            'business_story': measurement.business_story,
            'severity': 'critical' if measurement.response_time_ms > 25.0 else 'warning'
        }
        
        try:
            self.alert_queue.put_nowait(alert)
        except queue.Full:
            logger.warning("Alert queue full, dropping SLA violation alert")
    
    def calculate_current_sla_metrics(self) -> SLAMetrics:
        """Calculate SLA metrics for current monitoring window."""
        if not self.current_window_measurements:
            return SLAMetrics(
                total_requests=0,
                compliant_requests=0,
                sla_compliance_percentage=100.0,
                avg_response_time_ms=0.0,
                p50_response_time_ms=0.0,
                p95_response_time_ms=0.0,
                p99_response_time_ms=0.0,
                fastest_response_ms=0.0,
                slowest_response_ms=0.0,
                cache_hit_rate=0.0,
                business_impact_score=100.0,
                estimated_revenue_impact=0.0
            )
        
        measurements = self.current_window_measurements
        response_times = [m.response_time_ms for m in measurements]
        
        # Basic metrics
        total_requests = len(measurements)
        compliant_requests = sum(1 for m in measurements if m.is_sla_compliant)
        sla_compliance_percentage = (compliant_requests / total_requests) * 100
        
        # Response time statistics
        response_times.sort()
        avg_response_time_ms = statistics.mean(response_times)
        p50_response_time_ms = statistics.median(response_times)
        p95_response_time_ms = response_times[int(len(response_times) * 0.95)]
        p99_response_time_ms = response_times[int(len(response_times) * 0.99)]
        fastest_response_ms = min(response_times)
        slowest_response_ms = max(response_times)
        
        # Cache performance
        cache_hit_requests = sum(1 for m in measurements if m.cache_hit)
        cache_hit_rate = (cache_hit_requests / total_requests) * 100
        
        # Business impact calculation
        business_impact_score = self._calculate_business_impact_score(measurements)
        estimated_revenue_impact = self._calculate_revenue_impact(measurements)
        
        return SLAMetrics(
            total_requests=total_requests,
            compliant_requests=compliant_requests,
            sla_compliance_percentage=sla_compliance_percentage,
            avg_response_time_ms=avg_response_time_ms,
            p50_response_time_ms=p50_response_time_ms,
            p95_response_time_ms=p95_response_time_ms,
            p99_response_time_ms=p99_response_time_ms,
            fastest_response_ms=fastest_response_ms,
            slowest_response_ms=slowest_response_ms,
            cache_hit_rate=cache_hit_rate,
            business_impact_score=business_impact_score,
            estimated_revenue_impact=estimated_revenue_impact
        )
    
    def _calculate_business_impact_score(self, measurements: List[PerformanceMeasurement]) -> float:
        """Calculate weighted business impact score based on story values."""
        if not measurements:
            return 100.0
        
        total_weighted_score = 0.0
        total_weight = 0.0
        
        for measurement in measurements:
            story_value = 1.0  # Default weight
            if measurement.business_story:
                story_info = self.story_values.get(measurement.business_story, {})
                story_value = story_info.get('value', 1.0) / 1e6  # Normalize to millions
            
            # Calculate performance score (0-100)
            if measurement.is_sla_compliant:
                perf_score = 100.0
            else:
                # Degraded score based on how much over SLA
                perf_score = max(0, 100 - (measurement.response_time_ms / self.sla_threshold_ms * 50))
            
            total_weighted_score += perf_score * story_value
            total_weight += story_value
        
        return (total_weighted_score / total_weight) if total_weight > 0 else 100.0
    
    def _calculate_revenue_impact(self, measurements: List[PerformanceMeasurement]) -> float:
        """Calculate estimated revenue impact of performance degradation."""
        total_impact = 0.0
        
        for measurement in measurements:
            if not measurement.business_story:
                continue
                
            story_info = self.story_values.get(measurement.business_story, {})
            story_value = story_info.get('value', 0)
            
            if not measurement.is_sla_compliant:
                # Estimate impact as percentage of story value
                degradation_factor = min(0.1, (measurement.response_time_ms - self.sla_threshold_ms) / self.sla_threshold_ms * 0.05)
                total_impact += story_value * degradation_factor
        
        return total_impact
    
    def detect_performance_regression(self) -> List[Dict[str, Any]]:
        """Detect performance regressions compared to baseline."""
        if not self.baseline_percentiles:
            # Establish baseline from recent measurements
            self._establish_baseline()
            return []
        
        current_metrics = self.calculate_current_sla_metrics()
        regressions = []
        
        # Check P95 regression
        baseline_p95 = self.baseline_percentiles.get('p95', 0)
        if baseline_p95 > 0:
            p95_increase = (current_metrics.p95_response_time_ms - baseline_p95) / baseline_p95
            if p95_increase > self.regression_threshold:
                regressions.append({
                    'type': 'p95_regression',
                    'current_value': current_metrics.p95_response_time_ms,
                    'baseline_value': baseline_p95,
                    'increase_percentage': p95_increase * 100,
                    'severity': 'critical' if p95_increase > 0.5 else 'warning'
                })
        
        # Check average response time regression
        baseline_avg = self.baseline_percentiles.get('avg', 0)
        if baseline_avg > 0:
            avg_increase = (current_metrics.avg_response_time_ms - baseline_avg) / baseline_avg
            if avg_increase > self.regression_threshold:
                regressions.append({
                    'type': 'avg_regression',
                    'current_value': current_metrics.avg_response_time_ms,
                    'baseline_value': baseline_avg,
                    'increase_percentage': avg_increase * 100,
                    'severity': 'warning'
                })
        
        # Check SLA compliance regression
        baseline_sla = self.baseline_percentiles.get('sla_compliance', 100)
        sla_decrease = baseline_sla - current_metrics.sla_compliance_percentage
        if sla_decrease > 5.0:  # 5% decrease in SLA compliance
            regressions.append({
                'type': 'sla_compliance_regression',
                'current_value': current_metrics.sla_compliance_percentage,
                'baseline_value': baseline_sla,
                'decrease_percentage': sla_decrease,
                'severity': 'critical' if sla_decrease > 10 else 'warning'
            })
        
        return regressions
    
    def _establish_baseline(self):
        """Establish performance baseline from recent measurements."""
        if len(self.measurements) < 100:
            return
        
        # Use last 1000 measurements as baseline
        baseline_measurements = list(self.measurements)[-1000:]
        response_times = [m.response_time_ms for m in baseline_measurements]
        
        if response_times:
            response_times.sort()
            self.baseline_percentiles = {
                'avg': statistics.mean(response_times),
                'p50': statistics.median(response_times),
                'p95': response_times[int(len(response_times) * 0.95)],
                'p99': response_times[int(len(response_times) * 0.99)],
                'sla_compliance': (sum(1 for m in baseline_measurements if m.is_sla_compliant) / len(baseline_measurements)) * 100
            }
            
            logger.info(f"Performance baseline established: P95={self.baseline_percentiles['p95']:.2f}ms, "
                       f"SLA={self.baseline_percentiles['sla_compliance']:.1f}%")
    
    def _monitoring_loop(self):
        """Background monitoring loop for window-based analysis."""
        while self.monitoring_active:
            try:
                time.sleep(self.measurement_window_minutes * 60)
                
                # Calculate current window metrics
                current_metrics = self.calculate_current_sla_metrics()
                
                # Check for regressions
                regressions = self.detect_performance_regression()
                
                # Send window metrics to DataDog
                if self.enable_datadog and current_metrics.total_requests > 0:
                    self._send_window_metrics_to_datadog(current_metrics)
                
                # Queue alerts for regressions
                for regression in regressions:
                    self.alert_queue.put_nowait({
                        'type': 'performance_regression',
                        'timestamp': time.time(),
                        'regression_data': regression
                    })
                
                # Clear current window and prepare for next
                self.current_window_measurements = []
                
                logger.debug(f"Monitoring window completed: {current_metrics.total_requests} requests, "
                           f"{current_metrics.sla_compliance_percentage:.1f}% SLA compliance")
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                time.sleep(10)
    
    def _send_window_metrics_to_datadog(self, metrics: SLAMetrics):
        """Send window-level metrics to DataDog."""
        try:
            tags = [f"environment:{getattr(settings, 'environment', 'development')}"]
            
            # SLA compliance metrics
            statsd.gauge('api.sla_compliance_percentage', metrics.sla_compliance_percentage, tags=tags)
            statsd.gauge('api.avg_response_time_ms', metrics.avg_response_time_ms, tags=tags)
            statsd.gauge('api.p95_response_time_ms', metrics.p95_response_time_ms, tags=tags)
            statsd.gauge('api.p99_response_time_ms', metrics.p99_response_time_ms, tags=tags)
            statsd.gauge('api.cache_hit_rate', metrics.cache_hit_rate, tags=tags)
            
            # Business impact metrics
            statsd.gauge('business.impact_score', metrics.business_impact_score, tags=tags)
            statsd.gauge('business.estimated_revenue_impact', metrics.estimated_revenue_impact, tags=tags)
            
            # SLA target compliance
            statsd.gauge('sla.target_met', 1 if metrics.meets_sla_target else 0, tags=tags)
            
        except Exception as e:
            logger.warning(f"Failed to send window metrics to DataDog: {e}")
    
    def _alert_processor(self):
        """Background alert processing."""
        while self.monitoring_active:
            try:
                alert = self.alert_queue.get(timeout=1)
                self._process_alert(alert)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Alert processing error: {e}")
    
    def _process_alert(self, alert: Dict[str, Any]):
        """Process and send alerts."""
        alert_type = alert.get('type')
        
        if alert_type == 'sla_violation':
            logger.warning(f"SLA violation detected: {alert['endpoint']} took {alert['response_time_ms']:.2f}ms")
            
        elif alert_type == 'performance_regression':
            regression_data = alert['regression_data']
            logger.warning(f"Performance regression detected: {regression_data['type']}, "
                         f"{regression_data['increase_percentage']:.1f}% increase")
        
        # Send alert to DataDog if available
        if self.enable_datadog:
            try:
                tags = [
                    f"alert_type:{alert_type}",
                    f"severity:{alert.get('severity', 'warning')}",
                    f"environment:{getattr(settings, 'environment', 'development')}"
                ]
                statsd.increment('alerts.sla_performance', tags=tags)
            except Exception as e:
                logger.warning(f"Failed to send alert to DataDog: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        current_metrics = self.calculate_current_sla_metrics()
        regressions = self.detect_performance_regression()
        cache_hit_rate = self.cache_monitor.get_overall_cache_hit_rate()
        
        # Cache level breakdown
        cache_breakdown = {}
        for level in ['L0', 'L1', 'L2', 'L3']:
            cache_breakdown[level] = {
                'hit_rate': self.cache_monitor.get_cache_hit_rate(level),
                'avg_response_ms': self.cache_monitor.cache_metrics[level]['avg_response_ms']
            }
        
        # Story performance breakdown
        story_performance = {}
        if self.current_window_measurements:
            story_measurements = defaultdict(list)
            for m in self.current_window_measurements:
                if m.business_story:
                    story_measurements[m.business_story].append(m)
            
            for story_id, measurements in story_measurements.items():
                story_info = self.story_values.get(story_id, {})
                compliant = sum(1 for m in measurements if m.is_sla_compliant)
                
                story_performance[story_id] = {
                    'name': story_info.get('name', f'Story {story_id}'),
                    'value': story_info.get('value', 0),
                    'requests': len(measurements),
                    'sla_compliance': (compliant / len(measurements)) * 100,
                    'avg_response_ms': statistics.mean([m.response_time_ms for m in measurements])
                }
        
        return {
            'current_metrics': asdict(current_metrics),
            'sla_status': {
                'threshold_ms': self.sla_threshold_ms,
                'target_percentage': self.sla_target_percentage,
                'meets_target': current_metrics.meets_sla_target,
                'compliance_percentage': current_metrics.sla_compliance_percentage
            },
            'cache_performance': {
                'overall_hit_rate': cache_hit_rate,
                'level_breakdown': cache_breakdown,
                'target_hit_rate': 95.0,
                'meets_target': cache_hit_rate >= 95.0
            },
            'regressions_detected': regressions,
            'business_stories': story_performance,
            'total_business_value': sum(story['value'] for story in self.story_values.values()),
            'monitoring_status': {
                'active': self.monitoring_active,
                'total_measurements': len(self.measurements),
                'window_measurements': len(self.current_window_measurements),
                'datadog_enabled': self.enable_datadog
            }
        }


@asynccontextmanager
async def performance_monitoring_context(monitor: PerformanceSLAMonitor):
    """Context manager for performance monitoring."""
    start_time = time.perf_counter()
    try:
        yield
    finally:
        end_time = time.perf_counter()
        response_time = end_time - start_time
        # This would be called by the middleware with proper context
        # monitor.record_measurement(endpoint, method, response_time, status_code, **kwargs)


# Global monitor instance
_global_monitor: Optional[PerformanceSLAMonitor] = None


def get_performance_monitor() -> PerformanceSLAMonitor:
    """Get or create global performance monitor instance."""
    global _global_monitor
    
    if _global_monitor is None:
        _global_monitor = PerformanceSLAMonitor(
            sla_threshold_ms=15.0,
            sla_target_percentage=95.0,
            measurement_window_minutes=5,
            enable_datadog=True
        )
        _global_monitor.start_monitoring()
    
    return _global_monitor


def shutdown_performance_monitor():
    """Shutdown global performance monitor."""
    global _global_monitor
    
    if _global_monitor:
        _global_monitor.stop_monitoring()
        _global_monitor = None