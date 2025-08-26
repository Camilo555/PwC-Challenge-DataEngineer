"""
Advanced Prometheus Metrics Collection
Provides comprehensive metrics collection for enterprise observability.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary, Info, Enum as PrometheusEnum,
        CollectorRegistry, generate_latest, start_http_server, CONTENT_TYPE_LATEST
    )
    from prometheus_client.core import REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = Summary = Info = PrometheusEnum = None
    CollectorRegistry = REGISTRY = None
    generate_latest = start_http_server = lambda *args, **kwargs: None

from core.logging import get_logger

logger = get_logger(__name__)


class MetricType(Enum):
    """Types of Prometheus metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    INFO = "info"
    ENUM = "enum"


class BusinessDomain(Enum):
    """Business domains for metric categorization."""
    SALES = "sales"
    INVENTORY = "inventory"
    CUSTOMER = "customer"
    FINANCE = "finance"
    OPERATIONS = "operations"
    MARKETING = "marketing"


@dataclass
class MetricDefinition:
    """Definition of a Prometheus metric."""
    name: str
    description: str
    metric_type: MetricType
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None
    objectives: Optional[Dict[float, float]] = None
    business_domain: Optional[BusinessDomain] = None
    unit: str = ""
    help_text: str = ""


@dataclass
class BusinessMetric:
    """Business-specific metric with KPI information."""
    name: str
    value: float
    labels: Dict[str, str]
    timestamp: datetime
    business_domain: BusinessDomain
    target_value: Optional[float] = None
    threshold_warning: Optional[float] = None
    threshold_critical: Optional[float] = None


class EnterprisePrometheusCollector:
    """Enterprise-grade Prometheus metrics collector."""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or (REGISTRY if PROMETHEUS_AVAILABLE else None)
        self.metrics: Dict[str, Any] = {}
        self.business_metrics: Dict[str, BusinessMetric] = {}
        self.metric_definitions: Dict[str, MetricDefinition] = {}
        self.lock = threading.RLock()
        self.logger = get_logger(self.__class__.__name__)
        
        if PROMETHEUS_AVAILABLE:
            self._initialize_core_metrics()
            self._initialize_business_metrics()
            self._initialize_infrastructure_metrics()
            
    def _initialize_core_metrics(self):
        """Initialize core application metrics."""
        core_metrics = [
            MetricDefinition(
                name="app_requests_total",
                description="Total number of application requests",
                metric_type=MetricType.COUNTER,
                labels=["method", "endpoint", "status", "service"],
                help_text="Tracks all incoming requests to the application"
            ),
            MetricDefinition(
                name="app_request_duration_seconds",
                description="Application request duration in seconds",
                metric_type=MetricType.HISTOGRAM,
                labels=["method", "endpoint", "service"],
                buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
                help_text="Measures request processing time distribution"
            ),
            MetricDefinition(
                name="app_active_connections",
                description="Number of active connections",
                metric_type=MetricType.GAUGE,
                labels=["service", "connection_type"],
                help_text="Current number of active connections"
            ),
            MetricDefinition(
                name="app_errors_total",
                description="Total application errors",
                metric_type=MetricType.COUNTER,
                labels=["error_type", "service", "severity"],
                help_text="Count of application errors by type"
            ),
        ]
        
        for metric_def in core_metrics:
            self._create_metric(metric_def)
            
    def _initialize_business_metrics(self):
        """Initialize business-specific metrics."""
        business_metrics = [
            MetricDefinition(
                name="business_sales_revenue_total",
                description="Total sales revenue",
                metric_type=MetricType.COUNTER,
                labels=["currency", "product_category", "region", "channel"],
                business_domain=BusinessDomain.SALES,
                unit="currency",
                help_text="Total revenue from sales transactions"
            ),
            MetricDefinition(
                name="business_active_customers",
                description="Number of active customers",
                metric_type=MetricType.GAUGE,
                labels=["segment", "region", "tier"],
                business_domain=BusinessDomain.CUSTOMER,
                help_text="Current count of active customers"
            ),
            MetricDefinition(
                name="business_order_value",
                description="Order value distribution",
                metric_type=MetricType.HISTOGRAM,
                labels=["currency", "product_category", "customer_tier"],
                buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
                business_domain=BusinessDomain.SALES,
                unit="currency",
                help_text="Distribution of individual order values"
            ),
            MetricDefinition(
                name="business_inventory_levels",
                description="Current inventory levels",
                metric_type=MetricType.GAUGE,
                labels=["product_id", "warehouse", "category"],
                business_domain=BusinessDomain.INVENTORY,
                unit="units",
                help_text="Current stock levels for products"
            ),
            MetricDefinition(
                name="business_conversion_rate",
                description="Conversion rate percentage",
                metric_type=MetricType.GAUGE,
                labels=["channel", "campaign", "product_category"],
                business_domain=BusinessDomain.MARKETING,
                unit="percentage",
                help_text="Customer conversion rates"
            ),
        ]
        
        for metric_def in business_metrics:
            self._create_metric(metric_def)
            
    def _initialize_infrastructure_metrics(self):
        """Initialize infrastructure and system metrics."""
        infrastructure_metrics = [
            MetricDefinition(
                name="system_cpu_usage_percent",
                description="System CPU usage percentage",
                metric_type=MetricType.GAUGE,
                labels=["cpu_core", "host"],
                unit="percentage",
                help_text="CPU utilization per core"
            ),
            MetricDefinition(
                name="system_memory_usage_bytes",
                description="System memory usage in bytes",
                metric_type=MetricType.GAUGE,
                labels=["memory_type", "host"],
                unit="bytes",
                help_text="Memory usage breakdown"
            ),
            MetricDefinition(
                name="database_query_duration_seconds",
                description="Database query execution time",
                metric_type=MetricType.HISTOGRAM,
                labels=["database", "operation", "table"],
                buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
                help_text="Database query performance metrics"
            ),
            MetricDefinition(
                name="etl_processing_duration_seconds",
                description="ETL processing duration",
                metric_type=MetricType.HISTOGRAM,
                labels=["stage", "dataset", "partition"],
                buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1200, 3600],
                help_text="ETL pipeline processing times"
            ),
            MetricDefinition(
                name="etl_records_processed_total",
                description="Total ETL records processed",
                metric_type=MetricType.COUNTER,
                labels=["stage", "dataset", "status"],
                help_text="Count of records processed through ETL"
            ),
            MetricDefinition(
                name="cache_hit_ratio",
                description="Cache hit ratio",
                metric_type=MetricType.GAUGE,
                labels=["cache_name", "cache_type"],
                unit="ratio",
                help_text="Cache effectiveness ratio"
            ),
        ]
        
        for metric_def in infrastructure_metrics:
            self._create_metric(metric_def)
            
    def _create_metric(self, metric_def: MetricDefinition):
        """Create a Prometheus metric from definition."""
        if not PROMETHEUS_AVAILABLE:
            return
            
        try:
            with self.lock:
                if metric_def.name in self.metrics:
                    self.logger.debug(f"Metric {metric_def.name} already exists")
                    return
                    
                kwargs = {
                    'name': metric_def.name,
                    'documentation': metric_def.description,
                    'labelnames': metric_def.labels,
                    'registry': self.registry
                }
                
                if metric_def.metric_type == MetricType.COUNTER:
                    metric = Counter(**kwargs)
                elif metric_def.metric_type == MetricType.GAUGE:
                    metric = Gauge(**kwargs)
                elif metric_def.metric_type == MetricType.HISTOGRAM:
                    if metric_def.buckets:
                        kwargs['buckets'] = metric_def.buckets
                    metric = Histogram(**kwargs)
                elif metric_def.metric_type == MetricType.SUMMARY:
                    if metric_def.objectives:
                        kwargs['objectives'] = metric_def.objectives
                    metric = Summary(**kwargs)
                elif metric_def.metric_type == MetricType.INFO:
                    metric = Info(**kwargs)
                elif metric_def.metric_type == MetricType.ENUM:
                    # Enum metrics need states parameter
                    kwargs['states'] = ['active', 'inactive', 'error']
                    metric = PrometheusEnum(**kwargs)
                else:
                    self.logger.error(f"Unsupported metric type: {metric_def.metric_type}")
                    return
                    
                self.metrics[metric_def.name] = metric
                self.metric_definitions[metric_def.name] = metric_def
                self.logger.info(f"Created metric: {metric_def.name}")
                
        except Exception as e:
            self.logger.error(f"Failed to create metric {metric_def.name}: {e}")
            
    def increment_counter(self, name: str, value: float = 1, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        if not PROMETHEUS_AVAILABLE or name not in self.metrics:
            return
            
        try:
            metric = self.metrics[name]
            if hasattr(metric, 'inc'):
                if labels:
                    metric.labels(**labels).inc(value)
                else:
                    metric.inc(value)
        except Exception as e:
            self.logger.error(f"Failed to increment counter {name}: {e}")
            
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric value."""
        if not PROMETHEUS_AVAILABLE or name not in self.metrics:
            return
            
        try:
            metric = self.metrics[name]
            if hasattr(metric, 'set'):
                if labels:
                    metric.labels(**labels).set(value)
                else:
                    metric.set(value)
        except Exception as e:
            self.logger.error(f"Failed to set gauge {name}: {e}")
            
    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe a value in histogram metric."""
        if not PROMETHEUS_AVAILABLE or name not in self.metrics:
            return
            
        try:
            metric = self.metrics[name]
            if hasattr(metric, 'observe'):
                if labels:
                    metric.labels(**labels).observe(value)
                else:
                    metric.observe(value)
        except Exception as e:
            self.logger.error(f"Failed to observe histogram {name}: {e}")
            
    def record_business_metric(self, business_metric: BusinessMetric):
        """Record a business-specific metric."""
        with self.lock:
            self.business_metrics[business_metric.name] = business_metric
            
        # Update corresponding Prometheus metric if it exists
        if business_metric.name in self.metrics:
            metric_def = self.metric_definitions.get(business_metric.name)
            if metric_def:
                if metric_def.metric_type == MetricType.COUNTER:
                    self.increment_counter(business_metric.name, business_metric.value, business_metric.labels)
                elif metric_def.metric_type == MetricType.GAUGE:
                    self.set_gauge(business_metric.name, business_metric.value, business_metric.labels)
                elif metric_def.metric_type == MetricType.HISTOGRAM:
                    self.observe_histogram(business_metric.name, business_metric.value, business_metric.labels)
                    
    @contextmanager
    def time_operation(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager to time operations."""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.observe_histogram(name, duration, labels)
            
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all registered metrics."""
        with self.lock:
            return {
                'total_metrics': len(self.metrics),
                'business_metrics': len(self.business_metrics),
                'metric_types': {
                    metric_type.value: sum(1 for def_ in self.metric_definitions.values() 
                                         if def_.metric_type == metric_type)
                    for metric_type in MetricType
                },
                'business_domains': {
                    domain.value: sum(1 for def_ in self.metric_definitions.values() 
                                    if def_.business_domain == domain)
                    for domain in BusinessDomain
                },
                'last_updated': datetime.utcnow().isoformat()
            }
            
    def export_metrics(self) -> str:
        """Export metrics in Prometheus format."""
        if not PROMETHEUS_AVAILABLE:
            return "# Prometheus not available"
            
        try:
            return generate_latest(self.registry).decode('utf-8')
        except Exception as e:
            self.logger.error(f"Failed to export metrics: {e}")
            return f"# Error exporting metrics: {e}"
            
    def start_metrics_server(self, port: int = 8000, addr: str = '0.0.0.0'):
        """Start HTTP server for metrics scraping."""
        if not PROMETHEUS_AVAILABLE:
            self.logger.warning("Cannot start metrics server: Prometheus client not available")
            return False
            
        try:
            start_http_server(port, addr=addr, registry=self.registry)
            self.logger.info(f"Metrics server started on {addr}:{port}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to start metrics server: {e}")
            return False


class CustomMetricsCollector:
    """Collector for custom application-specific metrics."""
    
    def __init__(self, prometheus_collector: EnterprisePrometheusCollector):
        self.prometheus = prometheus_collector
        self.logger = get_logger(self.__class__.__name__)
        
    def collect_api_metrics(self, method: str, endpoint: str, status_code: int, 
                           duration: float, service: str = "api"):
        """Collect API-specific metrics."""
        labels = {
            'method': method,
            'endpoint': endpoint,
            'status': str(status_code),
            'service': service
        }
        
        # Increment request counter
        self.prometheus.increment_counter('app_requests_total', 1, labels)
        
        # Record request duration
        duration_labels = {k: v for k, v in labels.items() if k != 'status'}
        self.prometheus.observe_histogram('app_request_duration_seconds', duration, duration_labels)
        
        # Increment error counter if status indicates error
        if status_code >= 400:
            error_labels = {
                'error_type': 'http_error',
                'service': service,
                'severity': 'high' if status_code >= 500 else 'medium'
            }
            self.prometheus.increment_counter('app_errors_total', 1, error_labels)
            
    def collect_database_metrics(self, database: str, operation: str, 
                               table: str, duration: float, success: bool = True):
        """Collect database operation metrics."""
        labels = {
            'database': database,
            'operation': operation,
            'table': table
        }
        
        # Record query duration
        self.prometheus.observe_histogram('database_query_duration_seconds', duration, labels)
        
        # Record error if operation failed
        if not success:
            error_labels = {
                'error_type': 'database_error',
                'service': 'database',
                'severity': 'high'
            }
            self.prometheus.increment_counter('app_errors_total', 1, error_labels)
            
    def collect_etl_metrics(self, stage: str, dataset: str, partition: str,
                           duration: float, records_processed: int, success: bool = True):
        """Collect ETL pipeline metrics."""
        duration_labels = {
            'stage': stage,
            'dataset': dataset,
            'partition': partition
        }
        
        records_labels = {
            'stage': stage,
            'dataset': dataset,
            'status': 'success' if success else 'failed'
        }
        
        # Record processing duration
        self.prometheus.observe_histogram('etl_processing_duration_seconds', duration, duration_labels)
        
        # Record processed records count
        self.prometheus.increment_counter('etl_records_processed_total', records_processed, records_labels)
        
        # Record error if ETL failed
        if not success:
            error_labels = {
                'error_type': 'etl_error',
                'service': 'etl',
                'severity': 'high'
            }
            self.prometheus.increment_counter('app_errors_total', 1, error_labels)
            
    def collect_business_sales_metrics(self, revenue: float, currency: str, 
                                     product_category: str, region: str, 
                                     channel: str, order_value: float = None):
        """Collect business sales metrics."""
        revenue_labels = {
            'currency': currency,
            'product_category': product_category,
            'region': region,
            'channel': channel
        }
        
        # Record revenue
        self.prometheus.increment_counter('business_sales_revenue_total', revenue, revenue_labels)
        
        # Record order value distribution if provided
        if order_value is not None:
            order_labels = {
                'currency': currency,
                'product_category': product_category,
                'customer_tier': 'standard'  # Could be determined dynamically
            }
            self.prometheus.observe_histogram('business_order_value', order_value, order_labels)


# Global collector instance
_prometheus_collector: Optional[EnterprisePrometheusCollector] = None
_custom_collector: Optional[CustomMetricsCollector] = None


def get_prometheus_collector() -> EnterprisePrometheusCollector:
    """Get global Prometheus collector instance."""
    global _prometheus_collector
    if _prometheus_collector is None:
        _prometheus_collector = EnterprisePrometheusCollector()
    return _prometheus_collector


def get_custom_collector() -> CustomMetricsCollector:
    """Get global custom metrics collector instance."""
    global _custom_collector
    if _custom_collector is None:
        prometheus_collector = get_prometheus_collector()
        _custom_collector = CustomMetricsCollector(prometheus_collector)
    return _custom_collector


def initialize_prometheus_monitoring(port: int = 8000) -> bool:
    """Initialize Prometheus monitoring with metrics server."""
    try:
        collector = get_prometheus_collector()
        success = collector.start_metrics_server(port)
        
        if success:
            logger.info("Prometheus monitoring initialized successfully")
        else:
            logger.warning("Prometheus monitoring initialization failed")
            
        return success
        
    except Exception as e:
        logger.error(f"Failed to initialize Prometheus monitoring: {e}")
        return False


# Decorator for automatic metric collection
def monitor_performance(metric_name: str, labels: Optional[Dict[str, str]] = None):
    """Decorator to automatically monitor function performance."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            collector = get_prometheus_collector()
            
            with collector.time_operation(metric_name, labels):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    # Record error metric
                    error_labels = {
                        'error_type': type(e).__name__.lower(),
                        'service': 'application',
                        'severity': 'medium'
                    }
                    collector.increment_counter('app_errors_total', 1, error_labels)
                    raise
                    
        return wrapper
    return decorator