"""
Datadog Integration for Comprehensive Monitoring
Provides APM, metrics, logging, and alerting integration with Datadog
"""
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
import threading
import json
import traceback

# Datadog SDK imports
from datadog import initialize, api
import datadog
from datadog import DogStatsdClient
from ddtrace import tracer, patch_all
from ddtrace.ext import http, sql, redis as redis_ext

from core.config.unified_config import get_unified_config
from core.logging import get_logger


class MetricType(Enum):
    """Datadog metric types"""
    GAUGE = "gauge"
    COUNT = "count"
    RATE = "rate"
    HISTOGRAM = "histogram"
    DISTRIBUTION = "distribution"
    SET = "set"


class AlertPriority(Enum):
    """Alert priority levels"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class DatadogConfig:
    """Datadog configuration"""
    api_key: str
    app_key: str
    site: str = "datadoghq.com"
    service_name: str = "pwc-data-engineering"
    environment: str = "production"
    version: str = "1.0.0"
    statsd_host: str = "localhost"
    statsd_port: int = 8125
    enable_apm: bool = True
    enable_profiling: bool = True
    enable_runtime_metrics: bool = True


@dataclass
class CustomMetric:
    """Custom metric definition"""
    name: str
    value: float
    metric_type: MetricType
    tags: List[str]
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class Alert:
    """Alert definition"""
    name: str
    message: str
    query: str
    priority: AlertPriority
    tags: List[str]
    threshold: float
    comparison: str = "above"  # above, below, equal
    timeframe: str = "5m"


class DatadogMonitoring:
    """
    Comprehensive Datadog monitoring integration
    Provides metrics, APM, logging, and alerting capabilities
    """
    
    def __init__(self, config: Optional[DatadogConfig] = None):
        self.logger = get_logger(__name__)
        
        # Initialize configuration
        self.config = config or self._get_default_config()
        
        # Initialize Datadog
        self._initialize_datadog()
        
        # Initialize StatsD client
        self.statsd = DogStatsdClient(
            host=self.config.statsd_host,
            port=self.config.statsd_port,
            namespace='pwc.data_engineering.',
            constant_tags=[
                f'service:{self.config.service_name}',
                f'env:{self.config.environment}',
                f'version:{self.config.version}'
            ]
        )
        
        # Metrics buffer for batch sending
        self.metrics_buffer: List[CustomMetric] = []
        self.buffer_lock = threading.Lock()
        
        # Background thread for metrics submission
        self.metrics_thread = threading.Thread(target=self._metrics_worker, daemon=True)
        self.metrics_thread_active = True
        self.metrics_thread.start()
        
        # Performance tracking
        self.performance_metrics = {
            "requests_total": 0,
            "errors_total": 0,
            "processing_time_sum": 0.0,
            "last_reset": datetime.now()
        }
        
        self.logger.info("Datadog monitoring initialized")
    
    def _get_default_config(self) -> DatadogConfig:
        """Get default Datadog configuration from environment"""
        
        return DatadogConfig(
            api_key=os.getenv("DD_API_KEY", ""),
            app_key=os.getenv("DD_APP_KEY", ""),
            site=os.getenv("DD_SITE", "datadoghq.com"),
            service_name=os.getenv("DD_SERVICE", "pwc-data-engineering"),
            environment=os.getenv("DD_ENV", "development"),
            version=os.getenv("DD_VERSION", "1.0.0"),
            statsd_host=os.getenv("DD_STATSD_HOST", "localhost"),
            statsd_port=int(os.getenv("DD_STATSD_PORT", "8125")),
            enable_apm=os.getenv("DD_TRACE_ENABLED", "true").lower() == "true",
            enable_profiling=os.getenv("DD_PROFILING_ENABLED", "true").lower() == "true",
            enable_runtime_metrics=os.getenv("DD_RUNTIME_METRICS_ENABLED", "true").lower() == "true"
        )
    
    def _initialize_datadog(self):
        """Initialize Datadog SDK"""
        try:
            # Initialize Datadog API
            initialize(
                api_key=self.config.api_key,
                app_key=self.config.app_key,
                api_host=f'api.{self.config.site}'
            )
            
            # Configure APM if enabled
            if self.config.enable_apm:
                os.environ.update({
                    'DD_SERVICE': self.config.service_name,
                    'DD_ENV': self.config.environment,
                    'DD_VERSION': self.config.version,
                    'DD_TRACE_ENABLED': 'true',
                    'DD_PROFILING_ENABLED': str(self.config.enable_profiling).lower(),
                    'DD_RUNTIME_METRICS_ENABLED': str(self.config.enable_runtime_metrics).lower(),
                    'DD_LOGS_INJECTION': 'true'
                })
                
                # Patch common libraries for automatic instrumentation
                patch_all()
                
                # Configure tracer
                tracer.configure(
                    hostname=self.config.statsd_host,
                    port=self.config.statsd_port,
                    settings={
                        'FILTERS': [
                            {
                                'match_all': {
                                    'service': self.config.service_name
                                }
                            }
                        ]
                    }
                )
            
            self.logger.info("Datadog SDK initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Datadog: {str(e)}")
            raise
    
    def _metrics_worker(self):
        """Background worker for sending metrics to Datadog"""
        while self.metrics_thread_active:
            try:
                with self.buffer_lock:
                    if self.metrics_buffer:
                        metrics_to_send = self.metrics_buffer.copy()
                        self.metrics_buffer.clear()
                    else:
                        metrics_to_send = []
                
                if metrics_to_send:
                    self._send_metrics_batch(metrics_to_send)
                
                time.sleep(10)  # Send metrics every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Metrics worker error: {str(e)}")
                time.sleep(5)
    
    def _send_metrics_batch(self, metrics: List[CustomMetric]):
        """Send metrics batch to Datadog"""
        try:
            for metric in metrics:
                if metric.metric_type == MetricType.GAUGE:
                    self.statsd.gauge(
                        metric.name,
                        metric.value,
                        tags=metric.tags,
                        timestamp=metric.timestamp
                    )
                elif metric.metric_type == MetricType.COUNT:
                    self.statsd.count(
                        metric.name,
                        metric.value,
                        tags=metric.tags
                    )
                elif metric.metric_type == MetricType.HISTOGRAM:
                    self.statsd.histogram(
                        metric.name,
                        metric.value,
                        tags=metric.tags
                    )
                elif metric.metric_type == MetricType.DISTRIBUTION:
                    self.statsd.distribution(
                        metric.name,
                        metric.value,
                        tags=metric.tags
                    )
                elif metric.metric_type == MetricType.SET:
                    self.statsd.set(
                        metric.name,
                        metric.value,
                        tags=metric.tags
                    )
            
            self.logger.debug(f"Sent {len(metrics)} metrics to Datadog")
            
        except Exception as e:
            self.logger.error(f"Failed to send metrics batch: {str(e)}")
    
    # Metric Collection Methods
    
    def gauge(self, name: str, value: float, tags: Optional[List[str]] = None):
        """Send gauge metric"""
        self._add_metric(name, value, MetricType.GAUGE, tags or [])
    
    def counter(self, name: str, value: float = 1, tags: Optional[List[str]] = None):
        """Send counter metric"""
        self._add_metric(name, value, MetricType.COUNT, tags or [])
    
    def histogram(self, name: str, value: float, tags: Optional[List[str]] = None):
        """Send histogram metric"""
        self._add_metric(name, value, MetricType.HISTOGRAM, tags or [])
    
    def distribution(self, name: str, value: float, tags: Optional[List[str]] = None):
        """Send distribution metric"""
        self._add_metric(name, value, MetricType.DISTRIBUTION, tags or [])
    
    def _add_metric(self, name: str, value: float, metric_type: MetricType, tags: List[str]):
        """Add metric to buffer"""
        with self.buffer_lock:
            self.metrics_buffer.append(CustomMetric(
                name=name,
                value=value,
                metric_type=metric_type,
                tags=tags
            ))
    
    # ETL-specific metrics
    
    def track_etl_stage(self, stage: str, records_processed: int, processing_time_seconds: float, 
                       quality_score: Optional[float] = None):
        """Track ETL stage metrics"""
        
        tags = [f'stage:{stage}']
        
        # Record count
        self.gauge(f'etl.records_processed', records_processed, tags)
        
        # Processing time
        self.histogram(f'etl.processing_time', processing_time_seconds, tags)
        
        # Records per second
        if processing_time_seconds > 0:
            rps = records_processed / processing_time_seconds
            self.gauge(f'etl.records_per_second', rps, tags)
        
        # Quality score if available
        if quality_score is not None:
            self.gauge(f'etl.quality_score', quality_score, tags)
        
        # Stage completion
        self.counter(f'etl.stage_completed', tags=tags)
        
        self.logger.debug(f"Tracked ETL stage {stage}: {records_processed} records in {processing_time_seconds}s")
    
    def track_data_quality(self, layer: str, component: str, metrics: Dict[str, float]):
        """Track data quality metrics"""
        
        tags = [f'layer:{layer}', f'component:{component}']
        
        for metric_name, value in metrics.items():
            self.gauge(f'data_quality.{metric_name}', value, tags)
        
        # Overall quality score
        if 'overall_quality_score' in metrics:
            self.gauge('data_quality.overall_score', metrics['overall_quality_score'], tags)
        
        self.logger.debug(f"Tracked data quality for {layer}.{component}")
    
    def track_api_request(self, endpoint: str, method: str, status_code: int, 
                         response_time_ms: float, user_id: Optional[str] = None):
        """Track API request metrics"""
        
        tags = [
            f'endpoint:{endpoint}',
            f'method:{method}',
            f'status_code:{status_code}',
            f'status_class:{status_code // 100}xx'
        ]
        
        if user_id:
            tags.append(f'user_id:{user_id}')
        
        # Request count
        self.counter('api.requests_total', tags=tags)
        
        # Response time
        self.histogram('api.response_time', response_time_ms, tags)
        
        # Error tracking
        if status_code >= 400:
            self.counter('api.errors_total', tags=tags)
        
        # Update performance metrics
        self.performance_metrics["requests_total"] += 1
        if status_code >= 400:
            self.performance_metrics["errors_total"] += 1
        self.performance_metrics["processing_time_sum"] += response_time_ms
    
    def track_database_operation(self, operation: str, table: str, duration_ms: float, 
                                row_count: Optional[int] = None, success: bool = True):
        """Track database operation metrics"""
        
        tags = [
            f'operation:{operation}',
            f'table:{table}',
            f'success:{success}'
        ]
        
        # Operation count
        self.counter('db.operations_total', tags=tags)
        
        # Duration
        self.histogram('db.operation_duration', duration_ms, tags)
        
        # Row count if available
        if row_count is not None:
            self.gauge('db.rows_affected', row_count, tags)
        
        # Error tracking
        if not success:
            self.counter('db.errors_total', tags=tags)
    
    def track_cache_operation(self, operation: str, hit: bool, duration_ms: float):
        """Track cache operation metrics"""
        
        tags = [
            f'operation:{operation}',
            f'hit:{hit}'
        ]
        
        # Cache operations
        self.counter('cache.operations_total', tags=tags)
        
        # Cache hits/misses
        if hit:
            self.counter('cache.hits_total', tags=tags)
        else:
            self.counter('cache.misses_total', tags=tags)
        
        # Duration
        self.histogram('cache.operation_duration', duration_ms, tags)
    
    def track_message_queue(self, queue_name: str, operation: str, message_count: int = 1, 
                           processing_time_ms: Optional[float] = None):
        """Track message queue metrics"""
        
        tags = [
            f'queue:{queue_name}',
            f'operation:{operation}'
        ]
        
        # Message count
        self.counter('mq.messages_total', message_count, tags)
        
        # Processing time if available
        if processing_time_ms is not None:
            self.histogram('mq.processing_time', processing_time_ms, tags)
    
    def track_kafka_metrics(self, topic: str, partition: int, operation: str, 
                           lag: Optional[int] = None, throughput_mps: Optional[float] = None):
        """Track Kafka-specific metrics"""
        
        tags = [
            f'topic:{topic}',
            f'partition:{partition}',
            f'operation:{operation}'
        ]
        
        # Operations
        self.counter('kafka.operations_total', tags=tags)
        
        # Consumer lag
        if lag is not None:
            self.gauge('kafka.consumer_lag', lag, tags)
        
        # Throughput
        if throughput_mps is not None:
            self.gauge('kafka.throughput_mps', throughput_mps, tags)
    
    def track_system_metrics(self, cpu_usage: float, memory_usage: float, 
                           disk_usage: float, network_io: Optional[Dict[str, float]] = None):
        """Track system resource metrics"""
        
        tags = [f'host:{os.getenv("HOSTNAME", "unknown")}']
        
        # System resources
        self.gauge('system.cpu_usage_percent', cpu_usage, tags)
        self.gauge('system.memory_usage_percent', memory_usage, tags)
        self.gauge('system.disk_usage_percent', disk_usage, tags)
        
        # Network I/O if available
        if network_io:
            for metric, value in network_io.items():
                self.gauge(f'system.network.{metric}', value, tags)
    
    # APM and Tracing
    
    def trace_function(self, service: str = None, resource: str = None):
        """Decorator for tracing functions"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                with tracer.trace(
                    name=func.__name__,
                    service=service or self.config.service_name,
                    resource=resource or f"{func.__module__}.{func.__name__}"
                ) as span:
                    try:
                        result = func(*args, **kwargs)
                        span.set_tag("success", True)
                        return result
                    except Exception as e:
                        span.set_error(e)
                        span.set_tag("success", False)
                        raise
            return wrapper
        return decorator
    
    def trace_etl_operation(self, operation: str, layer: str):
        """Context manager for tracing ETL operations"""
        return tracer.trace(
            name=f"etl.{operation}",
            service=self.config.service_name,
            resource=f"etl.{layer}.{operation}",
            span_type="etl"
        )
    
    # Log Correlation
    
    def get_trace_context(self) -> Dict[str, str]:
        """Get current trace context for log correlation"""
        span = tracer.current_span()
        if span:
            return {
                'dd.trace_id': str(span.trace_id),
                'dd.span_id': str(span.span_id),
                'dd.service': self.config.service_name,
                'dd.env': self.config.environment,
                'dd.version': self.config.version
            }
        return {}
    
    # Alert Management
    
    def create_alert(self, alert: Alert) -> bool:
        """Create Datadog alert/monitor"""
        try:
            monitor_config = {
                'type': 'metric alert',
                'query': alert.query,
                'name': alert.name,
                'message': alert.message,
                'tags': alert.tags,
                'options': {
                    'thresholds': {
                        'critical': alert.threshold
                    },
                    'notify_audit': True,
                    'notify_no_data': True,
                    'no_data_timeframe': 20,
                    'timeout_h': 0,
                    'silenced': {}
                }
            }
            
            response = api.Monitor.create(**monitor_config)
            
            if 'id' in response:
                self.logger.info(f"Created alert {alert.name} with ID {response['id']}")
                return True
            else:
                self.logger.error(f"Failed to create alert {alert.name}: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to create alert {alert.name}: {str(e)}")
            return False
    
    def create_standard_alerts(self):
        """Create standard monitoring alerts"""
        
        alerts = [
            Alert(
                name="High Error Rate",
                message="API error rate is above 5% for 5 minutes",
                query="avg(last_5m):sum:pwc.data_engineering.api.errors_total{*}.as_rate() / sum:pwc.data_engineering.api.requests_total{*}.as_rate() > 0.05",
                priority=AlertPriority.HIGH,
                tags=["alert_type:error_rate", "component:api"],
                threshold=0.05
            ),
            Alert(
                name="ETL Processing Delay",
                message="ETL processing time is above 30 minutes",
                query="avg(last_10m):avg:pwc.data_engineering.etl.processing_time{*} > 1800",
                priority=AlertPriority.CRITICAL,
                tags=["alert_type:performance", "component:etl"],
                threshold=1800
            ),
            Alert(
                name="Low Data Quality Score",
                message="Data quality score is below 80%",
                query="avg(last_15m):min:pwc.data_engineering.data_quality.overall_score{*} < 0.8",
                priority=AlertPriority.HIGH,
                tags=["alert_type:data_quality", "component:etl"],
                threshold=0.8,
                comparison="below"
            ),
            Alert(
                name="High System CPU Usage",
                message="System CPU usage is above 85%",
                query="avg(last_10m):avg:pwc.data_engineering.system.cpu_usage_percent{*} > 85",
                priority=AlertPriority.NORMAL,
                tags=["alert_type:system", "component:infrastructure"],
                threshold=85
            ),
            Alert(
                name="Database Connection Issues",
                message="Database error rate is above 2%",
                query="avg(last_5m):sum:pwc.data_engineering.db.errors_total{*}.as_rate() / sum:pwc.data_engineering.db.operations_total{*}.as_rate() > 0.02",
                priority=AlertPriority.HIGH,
                tags=["alert_type:database", "component:database"],
                threshold=0.02
            ),
            Alert(
                name="Kafka Consumer Lag",
                message="Kafka consumer lag is above 10000 messages",
                query="avg(last_10m):max:pwc.data_engineering.kafka.consumer_lag{*} > 10000",
                priority=AlertPriority.NORMAL,
                tags=["alert_type:kafka", "component:streaming"],
                threshold=10000
            )
        ]
        
        created_count = 0
        for alert in alerts:
            if self.create_alert(alert):
                created_count += 1
        
        self.logger.info(f"Created {created_count}/{len(alerts)} standard alerts")
    
    # Dashboard Creation
    
    def create_etl_dashboard(self) -> bool:
        """Create ETL monitoring dashboard"""
        try:
            dashboard_config = {
                'title': 'PwC Data Engineering - ETL Pipeline',
                'description': 'Comprehensive ETL pipeline monitoring dashboard',
                'layout_type': 'ordered',
                'is_read_only': False,
                'widgets': [
                    {
                        'definition': {
                            'type': 'timeseries',
                            'title': 'ETL Processing Time',
                            'requests': [{
                                'q': 'avg:pwc.data_engineering.etl.processing_time{*} by {stage}',
                                'display_type': 'line'
                            }]
                        }
                    },
                    {
                        'definition': {
                            'type': 'timeseries',
                            'title': 'Records Processed',
                            'requests': [{
                                'q': 'sum:pwc.data_engineering.etl.records_processed{*} by {stage}',
                                'display_type': 'bars'
                            }]
                        }
                    },
                    {
                        'definition': {
                            'type': 'timeseries',
                            'title': 'Data Quality Score',
                            'requests': [{
                                'q': 'avg:pwc.data_engineering.data_quality.overall_score{*} by {layer}',
                                'display_type': 'line'
                            }]
                        }
                    },
                    {
                        'definition': {
                            'type': 'timeseries',
                            'title': 'API Performance',
                            'requests': [{
                                'q': 'avg:pwc.data_engineering.api.response_time{*} by {endpoint}',
                                'display_type': 'line'
                            }]
                        }
                    }
                ]
            }
            
            response = api.Dashboard.create(**dashboard_config)
            
            if 'id' in response:
                self.logger.info(f"Created ETL dashboard with ID {response['id']}")
                return True
            else:
                self.logger.error(f"Failed to create dashboard: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to create dashboard: {str(e)}")
            return False
    
    # Health Check
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check of Datadog integration"""
        
        health_status = {
            "datadog_connected": False,
            "statsd_connected": False,
            "apm_enabled": self.config.enable_apm,
            "metrics_buffer_size": len(self.metrics_buffer),
            "performance_metrics": self.performance_metrics.copy(),
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # Test API connection
            response = api.Validate.validate()
            health_status["datadog_connected"] = response.get('valid', False)
            
            # Test StatsD connection
            self.statsd.gauge('health_check.test', 1.0, tags=['test:true'])
            health_status["statsd_connected"] = True
            
        except Exception as e:
            health_status["error"] = str(e)
            self.logger.warning(f"Datadog health check failed: {str(e)}")
        
        return health_status
    
    def close(self):
        """Close Datadog monitoring"""
        self.metrics_thread_active = False
        
        # Flush remaining metrics
        with self.buffer_lock:
            if self.metrics_buffer:
                self._send_metrics_batch(self.metrics_buffer)
                self.metrics_buffer.clear()
        
        # Close StatsD client
        if hasattr(self.statsd, 'close'):
            self.statsd.close()
        
        self.logger.info("Datadog monitoring closed")


# Factory function
def create_datadog_monitoring(config: Optional[DatadogConfig] = None) -> DatadogMonitoring:
    """Create DatadogMonitoring instance"""
    return DatadogMonitoring(config)


# Example usage with context managers
class DatadogETLMonitor:
    """Example ETL monitor using Datadog"""
    
    def __init__(self):
        self.datadog = create_datadog_monitoring()
        self.logger = get_logger(__name__)
    
    def monitor_etl_pipeline(self, pipeline_id: str):
        """Monitor complete ETL pipeline"""
        
        with self.datadog.trace_etl_operation("complete_pipeline", "all") as span:
            span.set_tag("pipeline_id", pipeline_id)
            
            # Monitor each stage
            stages = ["bronze", "silver", "gold"]
            
            for stage in stages:
                with self.datadog.trace_etl_operation(f"process_{stage}", stage):
                    # Simulate ETL processing
                    start_time = time.time()
                    records_processed = (len(stages) - stages.index(stage)) * 10000
                    
                    # Simulate processing time
                    time.sleep(0.1)
                    
                    processing_time = time.time() - start_time
                    quality_score = 0.95 - (stages.index(stage) * 0.05)
                    
                    # Track metrics
                    self.datadog.track_etl_stage(
                        stage=stage,
                        records_processed=records_processed,
                        processing_time_seconds=processing_time,
                        quality_score=quality_score
                    )
                    
                    self.logger.info(f"Monitored {stage} stage: {records_processed} records")


# Testing and example usage
if __name__ == "__main__":
    import os
    
    # Set test environment variables
    os.environ.setdefault("DD_API_KEY", "test_api_key")
    os.environ.setdefault("DD_APP_KEY", "test_app_key")
    os.environ.setdefault("DD_ENV", "test")
    
    print("Testing Datadog Integration...")
    
    try:
        # Test basic functionality
        config = DatadogConfig(
            api_key="test_key",
            app_key="test_app_key",
            environment="test",
            enable_apm=False  # Disable for testing
        )
        
        monitor = create_datadog_monitoring(config)
        
        # Test metric collection
        monitor.gauge("test.metric", 42.0, tags=["test:true"])
        monitor.counter("test.counter", 1, tags=["test:true"])
        monitor.histogram("test.histogram", 1.5, tags=["test:true"])
        print("✅ Basic metrics sent")
        
        # Test ETL tracking
        monitor.track_etl_stage("bronze", 10000, 5.2, 0.95)
        monitor.track_etl_stage("silver", 9500, 3.8, 0.92)
        monitor.track_etl_stage("gold", 9500, 2.1, 0.98)
        print("✅ ETL metrics tracked")
        
        # Test API tracking
        monitor.track_api_request("/api/v1/sales", "GET", 200, 150.5)
        monitor.track_api_request("/api/v1/customers", "POST", 400, 250.0)
        print("✅ API metrics tracked")
        
        # Test database tracking
        monitor.track_database_operation("SELECT", "sales", 45.2, 1000, True)
        monitor.track_database_operation("INSERT", "customers", 120.8, 100, True)
        print("✅ Database metrics tracked")
        
        # Test system tracking
        monitor.track_system_metrics(45.2, 67.8, 23.4)
        print("✅ System metrics tracked")
        
        # Test health check
        health = monitor.health_check()
        print(f"✅ Health check: {health}")
        
        # Wait for metrics to be sent
        time.sleep(2)
        
        monitor.close()
        print("✅ Datadog integration testing completed successfully!")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()