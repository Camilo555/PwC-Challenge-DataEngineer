"""
Advanced Monitoring and Observability System
Provides comprehensive distributed tracing, metrics collection, and intelligent alerting.
"""
import asyncio
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import threading
import json
from pathlib import Path

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

from core.logging import get_logger


class MetricType(Enum):
    """Types of metrics collected"""
    COUNTER = "counter"
    GAUGE = "gauge" 
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class MetricPoint:
    """Individual metric measurement"""
    name: str
    value: float
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: MetricType = MetricType.GAUGE


@dataclass
class TraceSpan:
    """Distributed trace span information"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_ms: Optional[float]
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "OK"


@dataclass
class Alert:
    """System alert definition"""
    id: str
    name: str
    description: str
    severity: AlertSeverity
    condition: str
    threshold: float
    current_value: float
    triggered_at: datetime
    acknowledged: bool = False
    resolved: bool = False


class PerformanceProfiler:
    """Advanced performance profiling and analysis"""
    
    def __init__(self):
        self.profiles: Dict[str, List[Dict[str, Any]]] = {}
        self.lock = threading.Lock()
        self.logger = get_logger(__name__)
    
    @contextmanager
    def profile_operation(self, operation_name: str, tags: Dict[str, str] = None):
        """Context manager for profiling operations"""
        tags = tags or {}
        start_time = time.perf_counter()
        start_memory = self._get_memory_usage()
        
        try:
            yield
        finally:
            end_time = time.perf_counter()
            end_memory = self._get_memory_usage()
            duration = end_time - start_time
            memory_delta = end_memory - start_memory
            
            profile_data = {
                'operation': operation_name,
                'duration_ms': duration * 1000,
                'memory_delta_mb': memory_delta,
                'start_memory_mb': start_memory,
                'end_memory_mb': end_memory,
                'timestamp': datetime.now(),
                'tags': tags
            }
            
            with self.lock:
                if operation_name not in self.profiles:
                    self.profiles[operation_name] = []
                self.profiles[operation_name].append(profile_data)
                
                # Keep only last 1000 profiles per operation
                if len(self.profiles[operation_name]) > 1000:
                    self.profiles[operation_name] = self.profiles[operation_name][-1000:]
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0
    
    def get_operation_stats(self, operation_name: str) -> Dict[str, Any]:
        """Get statistical analysis for an operation"""
        with self.lock:
            if operation_name not in self.profiles:
                return {}
            
            profiles = self.profiles[operation_name]
            if not profiles:
                return {}
            
            durations = [p['duration_ms'] for p in profiles]
            memory_deltas = [p['memory_delta_mb'] for p in profiles]
            
            return {
                'count': len(profiles),
                'avg_duration_ms': sum(durations) / len(durations),
                'min_duration_ms': min(durations),
                'max_duration_ms': max(durations),
                'p95_duration_ms': self._percentile(durations, 95),
                'p99_duration_ms': self._percentile(durations, 99),
                'avg_memory_delta_mb': sum(memory_deltas) / len(memory_deltas),
                'total_memory_allocated_mb': sum(max(0, delta) for delta in memory_deltas),
                'last_execution': max(p['timestamp'] for p in profiles)
            }
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile value"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]


class DistributedTracer:
    """Advanced distributed tracing system"""
    
    def __init__(self, service_name: str = "retail-etl-pipeline"):
        self.service_name = service_name
        self.tracer_provider = TracerProvider()
        trace.set_tracer_provider(self.tracer_provider)
        
        # Configure exporters
        self._setup_exporters()
        
        self.tracer = trace.get_tracer(service_name)
        self.active_spans: Dict[str, TraceSpan] = {}
        self.completed_traces: List[TraceSpan] = []
        self.lock = threading.Lock()
        self.logger = get_logger(__name__)
    
    def _setup_exporters(self):
        """Setup trace exporters"""
        # Console exporter for development
        console_exporter = ConsoleSpanExporter()
        console_processor = BatchSpanProcessor(console_exporter)
        self.tracer_provider.add_span_processor(console_processor)
        
        # OTLP exporter for production (Jaeger/Zipkin)
        try:
            otlp_exporter = OTLPSpanExporter(
                endpoint="http://localhost:4317",
                insecure=True
            )
            otlp_processor = BatchSpanProcessor(otlp_exporter)
            self.tracer_provider.add_span_processor(otlp_processor)
        except Exception as e:
            self.logger.warning(f"Failed to setup OTLP exporter: {e}")
    
    @contextmanager
    def trace_operation(self, operation_name: str, tags: Dict[str, Any] = None):
        """Context manager for tracing operations"""
        tags = tags or {}
        
        with self.tracer.start_as_current_span(operation_name) as span:
            # Set tags
            for key, value in tags.items():
                span.set_attribute(key, str(value))
            
            trace_id = format(span.get_span_context().trace_id, '032x')
            span_id = format(span.get_span_context().span_id, '016x')
            
            trace_span = TraceSpan(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=None,  # Will be set by OpenTelemetry
                operation_name=operation_name,
                start_time=datetime.now(),
                end_time=None,
                duration_ms=None,
                tags=tags
            )
            
            try:
                with self.lock:
                    self.active_spans[span_id] = trace_span
                
                yield trace_span
                
                # Mark as successful
                span.set_status(trace.Status(trace.StatusCode.OK))
                trace_span.status = "OK"
                
            except Exception as e:
                # Mark as error
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                trace_span.status = "ERROR"
                trace_span.logs.append({
                    'timestamp': datetime.now(),
                    'level': 'error',
                    'message': str(e)
                })
                raise
                
            finally:
                end_time = datetime.now()
                trace_span.end_time = end_time
                trace_span.duration_ms = (end_time - trace_span.start_time).total_seconds() * 1000
                
                with self.lock:
                    if span_id in self.active_spans:
                        del self.active_spans[span_id]
                    self.completed_traces.append(trace_span)
                    
                    # Keep only last 10000 traces
                    if len(self.completed_traces) > 10000:
                        self.completed_traces = self.completed_traces[-10000:]
    
    def get_trace_analytics(self) -> Dict[str, Any]:
        """Get trace analytics and insights"""
        with self.lock:
            if not self.completed_traces:
                return {}
            
            # Group by operation
            operation_stats = {}
            for trace in self.completed_traces:
                op_name = trace.operation_name
                if op_name not in operation_stats:
                    operation_stats[op_name] = {
                        'count': 0,
                        'total_duration_ms': 0,
                        'errors': 0,
                        'avg_duration_ms': 0
                    }
                
                stats = operation_stats[op_name]
                stats['count'] += 1
                stats['total_duration_ms'] += trace.duration_ms or 0
                if trace.status == "ERROR":
                    stats['errors'] += 1
            
            # Calculate averages
            for stats in operation_stats.values():
                if stats['count'] > 0:
                    stats['avg_duration_ms'] = stats['total_duration_ms'] / stats['count']
                    stats['error_rate'] = stats['errors'] / stats['count']
            
            return {
                'total_traces': len(self.completed_traces),
                'active_spans': len(self.active_spans),
                'operation_stats': operation_stats,
                'overall_error_rate': sum(1 for t in self.completed_traces if t.status == "ERROR") / len(self.completed_traces)
            }


class IntelligentAlerting:
    """AI-driven alerting system with anomaly detection"""
    
    def __init__(self):
        self.metrics_history: Dict[str, List[MetricPoint]] = {}
        self.active_alerts: List[Alert] = []
        self.alert_rules: List[Dict[str, Any]] = []
        self.baselines: Dict[str, Dict[str, float]] = {}
        self.lock = threading.Lock()
        self.logger = get_logger(__name__)
        
        # Initialize default alert rules
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """Setup default alerting rules"""
        self.alert_rules = [
            {
                'name': 'High Error Rate',
                'metric': 'error_rate',
                'condition': 'greater_than',
                'threshold': 0.05,  # 5% error rate
                'severity': AlertSeverity.ERROR,
                'window_minutes': 5
            },
            {
                'name': 'High Response Time',
                'metric': 'avg_response_time_ms',
                'condition': 'greater_than',
                'threshold': 5000,  # 5 seconds
                'severity': AlertSeverity.WARNING,
                'window_minutes': 10
            },
            {
                'name': 'Low Throughput',
                'metric': 'requests_per_minute',
                'condition': 'less_than',
                'threshold': 10,
                'severity': AlertSeverity.WARNING,
                'window_minutes': 15
            },
            {
                'name': 'Memory Usage High',
                'metric': 'memory_usage_percent',
                'condition': 'greater_than',
                'threshold': 85,
                'severity': AlertSeverity.ERROR,
                'window_minutes': 5
            },
            {
                'name': 'Disk Usage Critical',
                'metric': 'disk_usage_percent',
                'condition': 'greater_than',
                'threshold': 95,
                'severity': AlertSeverity.CRITICAL,
                'window_minutes': 1
            }
        ]
    
    def record_metric(self, metric: MetricPoint):
        """Record a metric point"""
        with self.lock:
            if metric.name not in self.metrics_history:
                self.metrics_history[metric.name] = []
            
            self.metrics_history[metric.name].append(metric)
            
            # Keep only last 24 hours of data
            cutoff_time = datetime.now() - timedelta(hours=24)
            self.metrics_history[metric.name] = [
                m for m in self.metrics_history[metric.name] 
                if m.timestamp >= cutoff_time
            ]
        
        # Check for alerts
        self._evaluate_alerts(metric)
    
    def _evaluate_alerts(self, metric: MetricPoint):
        """Evaluate alerting rules against new metric"""
        for rule in self.alert_rules:
            if rule['metric'] != metric.name:
                continue
            
            # Get recent metrics for window
            window_start = datetime.now() - timedelta(minutes=rule['window_minutes'])
            recent_metrics = [
                m for m in self.metrics_history[metric.name]
                if m.timestamp >= window_start
            ]
            
            if not recent_metrics:
                continue
            
            # Calculate aggregate value
            if rule['condition'] in ['greater_than', 'less_than']:
                aggregate_value = sum(m.value for m in recent_metrics) / len(recent_metrics)
            else:
                aggregate_value = recent_metrics[-1].value
            
            # Check condition
            should_alert = False
            if rule['condition'] == 'greater_than' and aggregate_value > rule['threshold']:
                should_alert = True
            elif rule['condition'] == 'less_than' and aggregate_value < rule['threshold']:
                should_alert = True
            
            if should_alert:
                self._trigger_alert(rule, aggregate_value, metric)
    
    def _trigger_alert(self, rule: Dict[str, Any], current_value: float, metric: MetricPoint):
        """Trigger an alert"""
        # Check if alert already active
        existing_alert = next(
            (a for a in self.active_alerts 
             if a.name == rule['name'] and not a.resolved), 
            None
        )
        
        if existing_alert:
            existing_alert.current_value = current_value
            return
        
        alert = Alert(
            id=str(uuid.uuid4()),
            name=rule['name'],
            description=f"{rule['metric']} is {current_value:.2f}, threshold is {rule['threshold']}",
            severity=rule['severity'],
            condition=rule['condition'],
            threshold=rule['threshold'],
            current_value=current_value,
            triggered_at=datetime.now()
        )
        
        with self.lock:
            self.active_alerts.append(alert)
        
        self.logger.warning(f"Alert triggered: {alert.name} - {alert.description}")
    
    def detect_anomalies(self, metric_name: str, sensitivity: float = 2.0) -> List[Dict[str, Any]]:
        """Detect anomalies using statistical analysis"""
        with self.lock:
            if metric_name not in self.metrics_history:
                return []
            
            metrics = self.metrics_history[metric_name]
            if len(metrics) < 20:  # Need minimum data points
                return []
            
            # Calculate baseline statistics
            values = [m.value for m in metrics[-100:]]  # Last 100 points
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            std_dev = variance ** 0.5
            
            # Find anomalies (values beyond sensitivity * std_dev)
            anomalies = []
            threshold_upper = mean + (sensitivity * std_dev)
            threshold_lower = mean - (sensitivity * std_dev)
            
            for metric in metrics[-20:]:  # Check last 20 points
                if metric.value > threshold_upper or metric.value < threshold_lower:
                    anomalies.append({
                        'timestamp': metric.timestamp,
                        'value': metric.value,
                        'expected_range': [threshold_lower, threshold_upper],
                        'severity': 'high' if abs(metric.value - mean) > 3 * std_dev else 'medium',
                        'deviation_score': abs(metric.value - mean) / std_dev
                    })
            
            return anomalies


class SystemHealthMonitor:
    """Comprehensive system health monitoring"""
    
    def __init__(self):
        self.profiler = PerformanceProfiler()
        self.tracer = DistributedTracer()
        self.alerting = IntelligentAlerting()
        self.health_checks: Dict[str, Callable] = {}
        self.logger = get_logger(__name__)
        
        # Setup instrumentation
        self._setup_instrumentation()
    
    def _setup_instrumentation(self):
        """Setup automatic instrumentation"""
        try:
            # Instrument FastAPI
            FastAPIInstrumentor.instrument()
            
            # Instrument SQLAlchemy
            SQLAlchemyInstrumentor().instrument()
            
            # Instrument HTTP requests
            RequestsInstrumentor().instrument()
            
            self.logger.info("Automatic instrumentation setup completed")
        except Exception as e:
            self.logger.error(f"Failed to setup instrumentation: {e}")
    
    def register_health_check(self, name: str, check_func: Callable[[], bool]):
        """Register a health check function"""
        self.health_checks[name] = check_func
    
    async def run_health_checks(self) -> Dict[str, Any]:
        """Run all registered health checks"""
        results = {}
        overall_healthy = True
        
        for name, check_func in self.health_checks.items():
            try:
                with self.profiler.profile_operation(f"health_check_{name}"):
                    if asyncio.iscoroutinefunction(check_func):
                        result = await check_func()
                    else:
                        result = check_func()
                
                results[name] = {
                    'healthy': result,
                    'timestamp': datetime.now(),
                    'error': None
                }
                
                if not result:
                    overall_healthy = False
                    
            except Exception as e:
                results[name] = {
                    'healthy': False,
                    'timestamp': datetime.now(),
                    'error': str(e)
                }
                overall_healthy = False
                self.logger.error(f"Health check {name} failed: {e}")
        
        return {
            'overall_healthy': overall_healthy,
            'checks': results,
            'timestamp': datetime.now()
        }
    
    def collect_system_metrics(self) -> List[MetricPoint]:
        """Collect comprehensive system metrics"""
        metrics = []
        timestamp = datetime.now()
        
        try:
            import psutil
            
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            metrics.append(MetricPoint(
                name="cpu_usage_percent",
                value=cpu_percent,
                timestamp=timestamp,
                labels={"system": "cpu"}
            ))
            
            # Memory metrics
            memory = psutil.virtual_memory()
            metrics.append(MetricPoint(
                name="memory_usage_percent",
                value=memory.percent,
                timestamp=timestamp,
                labels={"system": "memory"}
            ))
            
            metrics.append(MetricPoint(
                name="memory_available_mb",
                value=memory.available / 1024 / 1024,
                timestamp=timestamp,
                labels={"system": "memory"}
            ))
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            metrics.append(MetricPoint(
                name="disk_usage_percent",
                value=disk.percent,
                timestamp=timestamp,
                labels={"system": "disk"}
            ))
            
            # Process metrics
            process = psutil.Process()
            metrics.append(MetricPoint(
                name="process_memory_mb",
                value=process.memory_info().rss / 1024 / 1024,
                timestamp=timestamp,
                labels={"system": "process"}
            ))
            
            metrics.append(MetricPoint(
                name="process_cpu_percent",
                value=process.cpu_percent(),
                timestamp=timestamp,
                labels={"system": "process"}
            ))
            
        except ImportError:
            self.logger.warning("psutil not available, system metrics collection limited")
        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {e}")
        
        # Record metrics in alerting system
        for metric in metrics:
            self.alerting.record_metric(metric)
        
        return metrics
    
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        return {
            'timestamp': datetime.now(),
            'profiler': {
                'operations_tracked': len(self.profiler.profiles),
                'total_profiles': sum(len(profiles) for profiles in self.profiler.profiles.values())
            },
            'tracing': self.tracer.get_trace_analytics(),
            'alerting': {
                'active_alerts': len([a for a in self.alerting.active_alerts if not a.resolved]),
                'total_metrics_tracked': len(self.alerting.metrics_history)
            },
            'health_checks_registered': len(self.health_checks)
        }
    
    def export_metrics_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        lines = []
        timestamp = int(time.time() * 1000)
        
        for metric_name, points in self.alerting.metrics_history.items():
            if not points:
                continue
            
            latest_point = points[-1]
            labels = ','.join(f'{k}="{v}"' for k, v in latest_point.labels.items())
            label_str = f"{{{labels}}}" if labels else ""
            
            lines.append(f"# HELP {metric_name} System metric")
            lines.append(f"# TYPE {metric_name} gauge")
            lines.append(f"{metric_name}{label_str} {latest_point.value} {timestamp}")
        
        return '\n'.join(lines)


# Global monitoring instance
_system_monitor: Optional[SystemHealthMonitor] = None


def get_system_monitor() -> SystemHealthMonitor:
    """Get global system monitor instance"""
    global _system_monitor
    if _system_monitor is None:
        _system_monitor = SystemHealthMonitor()
    return _system_monitor


# Decorators for easy monitoring
def monitor_performance(operation_name: str = None, tags: Dict[str, str] = None):
    """Decorator for performance monitoring"""
    def decorator(func):
        nonlocal operation_name
        if operation_name is None:
            operation_name = f"{func.__module__}.{func.__name__}"
        
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                monitor = get_system_monitor()
                with monitor.profiler.profile_operation(operation_name, tags):
                    with monitor.tracer.trace_operation(operation_name, tags):
                        return await func(*args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                monitor = get_system_monitor()
                with monitor.profiler.profile_operation(operation_name, tags):
                    with monitor.tracer.trace_operation(operation_name, tags):
                        return func(*args, **kwargs)
            return sync_wrapper
    
    return decorator


@asynccontextmanager
async def monitoring_context(operation_name: str, tags: Dict[str, str] = None):
    """Async context manager for monitoring"""
    monitor = get_system_monitor()
    with monitor.profiler.profile_operation(operation_name, tags):
        with monitor.tracer.trace_operation(operation_name, tags):
            yield monitor