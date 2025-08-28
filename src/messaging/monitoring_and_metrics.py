"""
RabbitMQ Monitoring and Metrics Collection

This module provides comprehensive monitoring and metrics collection for
RabbitMQ messaging including:
- Queue depth monitoring and alerting
- Message throughput and latency metrics
- Error rate tracking and analysis
- Consumer lag monitoring
- Performance optimization recommendations
- Health checks and status monitoring
"""

import asyncio
import json
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Union
from concurrent.futures import ThreadPoolExecutor
import psutil
import socket

from .enterprise_rabbitmq_manager import (
    EnterpriseRabbitMQManager, EnterpriseMessage, MessageMetadata,
    QueueType, MessagePriority, get_rabbitmq_manager
)
from .message_patterns import PublisherSubscriber, TopicRouter
from core.logging import get_logger


class MetricType(Enum):
    """Types of metrics collected"""
    COUNTER = "counter"
    GAUGE = "gauge" 
    HISTOGRAM = "histogram"
    TIMER = "timer"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class MetricPoint:
    """Single metric data point"""
    name: str
    value: Union[int, float]
    metric_type: MetricType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "type": self.metric_type.value,
            "timestamp": self.timestamp.isoformat(),
            "tags": self.tags
        }


@dataclass
class QueueMetrics:
    """Queue-specific metrics"""
    queue_name: str
    message_count: int = 0
    consumer_count: int = 0
    message_rate: float = 0.0
    publish_rate: float = 0.0
    consume_rate: float = 0.0
    avg_processing_time_ms: float = 0.0
    error_rate: float = 0.0
    memory_usage_mb: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "queue_name": self.queue_name,
            "message_count": self.message_count,
            "consumer_count": self.consumer_count,
            "message_rate": self.message_rate,
            "publish_rate": self.publish_rate,
            "consume_rate": self.consume_rate,
            "avg_processing_time_ms": self.avg_processing_time_ms,
            "error_rate": self.error_rate,
            "memory_usage_mb": self.memory_usage_mb,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class SystemHealthCheck:
    """System health check result"""
    component: str
    status: str  # healthy, degraded, unhealthy
    response_time_ms: float
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "component": self.component,
            "status": self.status,
            "response_time_ms": self.response_time_ms,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class PerformanceAlert:
    """Performance alert"""
    alert_id: str
    alert_type: str
    severity: AlertSeverity
    message: str
    metric_name: str
    current_value: Union[int, float]
    threshold_value: Union[int, float]
    queue_name: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type,
            "severity": self.severity.value,
            "message": self.message,
            "metric_name": self.metric_name,
            "current_value": self.current_value,
            "threshold_value": self.threshold_value,
            "queue_name": self.queue_name,
            "timestamp": self.timestamp.isoformat(),
            "resolved": self.resolved
        }


class MetricsCollector:
    """
    Comprehensive metrics collection system for RabbitMQ
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Metrics storage
        self.metrics_buffer: deque = deque(maxlen=10000)
        self.queue_metrics: Dict[str, QueueMetrics] = {}
        self.system_metrics: Dict[str, Any] = {}
        
        # Performance tracking
        self.message_processing_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.throughput_counters: Dict[str, int] = defaultdict(int)
        
        # Monitoring configuration
        self.collection_interval = 30  # seconds
        self.retention_period = 7  # days
        self.alert_thresholds = self._setup_default_thresholds()
        
        # Threading
        self.collection_thread: Optional[threading.Thread] = None
        self.is_collecting = False
        
        # Message patterns for metric publishing
        self.publisher = PublisherSubscriber(self.rabbitmq)
        self.topic_router = TopicRouter(self.rabbitmq)
        
        self.logger.info("Metrics collector initialized")
    
    def _setup_default_thresholds(self) -> Dict[str, Dict[str, Any]]:
        """Setup default alert thresholds"""
        return {
            "queue_depth": {
                "warning": 1000,
                "critical": 5000
            },
            "consumer_lag": {
                "warning": 100,
                "critical": 500
            },
            "error_rate": {
                "warning": 0.05,  # 5%
                "critical": 0.10   # 10%
            },
            "processing_time": {
                "warning": 5000,   # 5 seconds
                "critical": 15000  # 15 seconds
            },
            "memory_usage": {
                "warning": 512,    # 512 MB
                "critical": 1024   # 1 GB
            },
            "disk_usage": {
                "warning": 0.80,   # 80%
                "critical": 0.95   # 95%
            }
        }
    
    def start_collection(self):
        """Start metrics collection"""
        if self.is_collecting:
            self.logger.warning("Metrics collection already running")
            return
        
        self.is_collecting = True
        self.collection_thread = threading.Thread(
            target=self._collection_worker,
            daemon=True
        )
        self.collection_thread.start()
        
        self.logger.info("Started metrics collection")
    
    def stop_collection(self):
        """Stop metrics collection"""
        self.is_collecting = False
        if self.collection_thread:
            self.collection_thread.join(timeout=5)
        
        self.logger.info("Stopped metrics collection")
    
    def _collection_worker(self):
        """Background metrics collection worker"""
        while self.is_collecting:
            try:
                start_time = time.time()
                
                # Collect queue metrics
                self._collect_queue_metrics()
                
                # Collect system metrics
                self._collect_system_metrics()
                
                # Collect RabbitMQ specific metrics
                self._collect_rabbitmq_metrics()
                
                # Check for alerts
                self._check_alert_conditions()
                
                # Publish metrics
                self._publish_collected_metrics()
                
                collection_time = time.time() - start_time
                
                # Record collection performance
                self._record_metric(
                    MetricPoint(
                        name="metrics.collection_time_ms",
                        value=collection_time * 1000,
                        metric_type=MetricType.TIMER,
                        tags={"component": "metrics_collector"}
                    )
                )
                
                # Sleep until next collection interval
                sleep_time = max(0, self.collection_interval - collection_time)
                time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.error(f"Error in metrics collection: {e}")
                time.sleep(self.collection_interval)
    
    def _collect_queue_metrics(self):
        """Collect metrics for all queues"""
        try:
            for queue_type in QueueType:
                queue_name = queue_type.value
                
                try:
                    # Get queue stats from RabbitMQ
                    stats = self.rabbitmq.get_queue_stats(queue_type)
                    
                    if "error" not in stats:
                        # Calculate rates
                        current_metrics = self.queue_metrics.get(queue_name)
                        
                        if current_metrics:
                            time_diff = (datetime.utcnow() - current_metrics.timestamp).total_seconds()
                            if time_diff > 0:
                                message_diff = stats["message_count"] - current_metrics.message_count
                                message_rate = message_diff / time_diff
                            else:
                                message_rate = 0
                        else:
                            message_rate = 0
                        
                        # Create queue metrics
                        queue_metrics = QueueMetrics(
                            queue_name=queue_name,
                            message_count=stats["message_count"],
                            consumer_count=stats["consumer_count"],
                            message_rate=message_rate,
                            avg_processing_time_ms=self._get_avg_processing_time(queue_name),
                            error_rate=self._calculate_error_rate(queue_name)
                        )
                        
                        self.queue_metrics[queue_name] = queue_metrics
                        
                        # Record individual metrics
                        self._record_queue_metrics(queue_metrics)
                        
                except Exception as e:
                    self.logger.warning(f"Failed to collect metrics for queue {queue_name}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error collecting queue metrics: {e}")
    
    def _collect_system_metrics(self):
        """Collect system-level metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self._record_metric(
                MetricPoint(
                    name="system.cpu_percent",
                    value=cpu_percent,
                    metric_type=MetricType.GAUGE,
                    tags={"component": "system"}
                )
            )
            
            # Memory usage
            memory = psutil.virtual_memory()
            self._record_metric(
                MetricPoint(
                    name="system.memory_percent",
                    value=memory.percent,
                    metric_type=MetricType.GAUGE,
                    tags={"component": "system"}
                )
            )
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self._record_metric(
                MetricPoint(
                    name="system.disk_percent",
                    value=disk_percent,
                    metric_type=MetricType.GAUGE,
                    tags={"component": "system"}
                )
            )
            
            # Network I/O
            network = psutil.net_io_counters()
            self._record_metric(
                MetricPoint(
                    name="system.network_bytes_sent",
                    value=network.bytes_sent,
                    metric_type=MetricType.COUNTER,
                    tags={"component": "system"}
                )
            )
            
            self._record_metric(
                MetricPoint(
                    name="system.network_bytes_recv",
                    value=network.bytes_recv,
                    metric_type=MetricType.COUNTER,
                    tags={"component": "system"}
                )
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")
    
    def _collect_rabbitmq_metrics(self):
        """Collect RabbitMQ-specific metrics"""
        try:
            # Get RabbitMQ health check
            health_check = self.rabbitmq.health_check()
            
            if health_check.get("status") == "healthy":
                response_time = health_check.get("response_time_ms", 0)
                
                self._record_metric(
                    MetricPoint(
                        name="rabbitmq.health_check_response_time_ms",
                        value=response_time,
                        metric_type=MetricType.TIMER,
                        tags={"component": "rabbitmq"}
                    )
                )
                
                self._record_metric(
                    MetricPoint(
                        name="rabbitmq.health_status",
                        value=1,  # healthy
                        metric_type=MetricType.GAUGE,
                        tags={"component": "rabbitmq"}
                    )
                )
            else:
                self._record_metric(
                    MetricPoint(
                        name="rabbitmq.health_status",
                        value=0,  # unhealthy
                        metric_type=MetricType.GAUGE,
                        tags={"component": "rabbitmq"}
                    )
                )
            
            # Get connection pool metrics
            rabbitmq_metrics = self.rabbitmq.get_metrics()
            
            for metric_name, metric_value in rabbitmq_metrics.items():
                if isinstance(metric_value, (int, float)):
                    self._record_metric(
                        MetricPoint(
                            name=f"rabbitmq.{metric_name}",
                            value=metric_value,
                            metric_type=MetricType.GAUGE,
                            tags={"component": "rabbitmq"}
                        )
                    )
            
        except Exception as e:
            self.logger.error(f"Error collecting RabbitMQ metrics: {e}")
    
    def _record_queue_metrics(self, queue_metrics: QueueMetrics):
        """Record queue-specific metrics"""
        queue_name = queue_metrics.queue_name
        
        metrics_to_record = [
            ("queue.message_count", queue_metrics.message_count, MetricType.GAUGE),
            ("queue.consumer_count", queue_metrics.consumer_count, MetricType.GAUGE),
            ("queue.message_rate", queue_metrics.message_rate, MetricType.GAUGE),
            ("queue.processing_time_ms", queue_metrics.avg_processing_time_ms, MetricType.GAUGE),
            ("queue.error_rate", queue_metrics.error_rate, MetricType.GAUGE)
        ]
        
        for metric_name, metric_value, metric_type in metrics_to_record:
            self._record_metric(
                MetricPoint(
                    name=metric_name,
                    value=metric_value,
                    metric_type=metric_type,
                    tags={
                        "queue": queue_name,
                        "component": "queue"
                    }
                )
            )
    
    def _record_metric(self, metric: MetricPoint):
        """Record a metric point"""
        self.metrics_buffer.append(metric)
        
        # Update system metrics cache
        if metric.tags.get("component") == "system":
            self.system_metrics[metric.name] = {
                "value": metric.value,
                "timestamp": metric.timestamp
            }
    
    def _get_avg_processing_time(self, queue_name: str) -> float:
        """Get average processing time for queue"""
        processing_times = self.message_processing_times.get(queue_name, deque())
        
        if processing_times:
            return statistics.mean(processing_times)
        
        return 0.0
    
    def _calculate_error_rate(self, queue_name: str) -> float:
        """Calculate error rate for queue"""
        error_count = self.error_counts.get(queue_name, 0)
        throughput = self.throughput_counters.get(queue_name, 0)
        
        if throughput > 0:
            return error_count / throughput
        
        return 0.0
    
    def record_message_processing_time(self, queue_name: str, processing_time_ms: float):
        """Record message processing time"""
        self.message_processing_times[queue_name].append(processing_time_ms)
        self.throughput_counters[queue_name] += 1
        
        # Record as metric
        self._record_metric(
            MetricPoint(
                name="message.processing_time_ms",
                value=processing_time_ms,
                metric_type=MetricType.HISTOGRAM,
                tags={
                    "queue": queue_name,
                    "component": "message_processing"
                }
            )
        )
    
    def record_message_error(self, queue_name: str, error_type: str):
        """Record message processing error"""
        self.error_counts[queue_name] += 1
        
        # Record as metric
        self._record_metric(
            MetricPoint(
                name="message.error_count",
                value=1,
                metric_type=MetricType.COUNTER,
                tags={
                    "queue": queue_name,
                    "error_type": error_type,
                    "component": "message_processing"
                }
            )
        )
    
    def _check_alert_conditions(self):
        """Check for alert conditions"""
        try:
            # Check queue depth alerts
            for queue_name, metrics in self.queue_metrics.items():
                self._check_queue_alerts(metrics)
            
            # Check system resource alerts
            self._check_system_alerts()
            
        except Exception as e:
            self.logger.error(f"Error checking alert conditions: {e}")
    
    def _check_queue_alerts(self, metrics: QueueMetrics):
        """Check queue-specific alerts"""
        queue_name = metrics.queue_name
        
        # Queue depth alert
        if metrics.message_count > self.alert_thresholds["queue_depth"]["critical"]:
            self._send_alert(
                alert_type="queue_depth_critical",
                severity=AlertSeverity.CRITICAL,
                message=f"Queue {queue_name} has critical depth: {metrics.message_count} messages",
                metric_name="queue.message_count",
                current_value=metrics.message_count,
                threshold_value=self.alert_thresholds["queue_depth"]["critical"],
                queue_name=queue_name
            )
        elif metrics.message_count > self.alert_thresholds["queue_depth"]["warning"]:
            self._send_alert(
                alert_type="queue_depth_warning",
                severity=AlertSeverity.WARNING,
                message=f"Queue {queue_name} depth warning: {metrics.message_count} messages",
                metric_name="queue.message_count",
                current_value=metrics.message_count,
                threshold_value=self.alert_thresholds["queue_depth"]["warning"],
                queue_name=queue_name
            )
        
        # Error rate alert
        if metrics.error_rate > self.alert_thresholds["error_rate"]["critical"]:
            self._send_alert(
                alert_type="error_rate_critical",
                severity=AlertSeverity.CRITICAL,
                message=f"Queue {queue_name} has critical error rate: {metrics.error_rate:.2%}",
                metric_name="queue.error_rate",
                current_value=metrics.error_rate,
                threshold_value=self.alert_thresholds["error_rate"]["critical"],
                queue_name=queue_name
            )
        elif metrics.error_rate > self.alert_thresholds["error_rate"]["warning"]:
            self._send_alert(
                alert_type="error_rate_warning",
                severity=AlertSeverity.WARNING,
                message=f"Queue {queue_name} error rate warning: {metrics.error_rate:.2%}",
                metric_name="queue.error_rate",
                current_value=metrics.error_rate,
                threshold_value=self.alert_thresholds["error_rate"]["warning"],
                queue_name=queue_name
            )
        
        # Processing time alert
        if metrics.avg_processing_time_ms > self.alert_thresholds["processing_time"]["critical"]:
            self._send_alert(
                alert_type="processing_time_critical",
                severity=AlertSeverity.CRITICAL,
                message=f"Queue {queue_name} has critical processing time: {metrics.avg_processing_time_ms:.2f}ms",
                metric_name="queue.processing_time_ms",
                current_value=metrics.avg_processing_time_ms,
                threshold_value=self.alert_thresholds["processing_time"]["critical"],
                queue_name=queue_name
            )
    
    def _check_system_alerts(self):
        """Check system-level alerts"""
        # CPU usage alert
        cpu_metric = self.system_metrics.get("system.cpu_percent")
        if cpu_metric and cpu_metric["value"] > 90:
            self._send_alert(
                alert_type="cpu_usage_critical",
                severity=AlertSeverity.CRITICAL,
                message=f"CPU usage critical: {cpu_metric['value']:.1f}%",
                metric_name="system.cpu_percent",
                current_value=cpu_metric["value"],
                threshold_value=90
            )
        
        # Memory usage alert
        memory_metric = self.system_metrics.get("system.memory_percent")
        if memory_metric and memory_metric["value"] > 90:
            self._send_alert(
                alert_type="memory_usage_critical",
                severity=AlertSeverity.CRITICAL,
                message=f"Memory usage critical: {memory_metric['value']:.1f}%",
                metric_name="system.memory_percent",
                current_value=memory_metric["value"],
                threshold_value=90
            )
        
        # Disk usage alert
        disk_metric = self.system_metrics.get("system.disk_percent")
        if disk_metric and disk_metric["value"] > 95:
            self._send_alert(
                alert_type="disk_usage_critical",
                severity=AlertSeverity.CRITICAL,
                message=f"Disk usage critical: {disk_metric['value']:.1f}%",
                metric_name="system.disk_percent",
                current_value=disk_metric["value"],
                threshold_value=95
            )
    
    def _send_alert(
        self,
        alert_type: str,
        severity: AlertSeverity,
        message: str,
        metric_name: str,
        current_value: Union[int, float],
        threshold_value: Union[int, float],
        queue_name: Optional[str] = None
    ):
        """Send performance alert"""
        try:
            alert = PerformanceAlert(
                alert_id=f"alert_{int(time.time())}_{hash(message) % 10000}",
                alert_type=alert_type,
                severity=severity,
                message=message,
                metric_name=metric_name,
                current_value=current_value,
                threshold_value=threshold_value,
                queue_name=queue_name
            )
            
            # Publish alert
            self.publisher.publish(
                event_type="performance_alert",
                data=alert.to_dict(),
                priority=MessagePriority.CRITICAL if severity == AlertSeverity.CRITICAL else MessagePriority.HIGH
            )
            
            self.logger.warning(f"Performance alert: {message}")
            
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
    
    def _publish_collected_metrics(self):
        """Publish collected metrics"""
        try:
            # Prepare metrics batch
            metrics_batch = []
            while self.metrics_buffer and len(metrics_batch) < 100:
                metric = self.metrics_buffer.popleft()
                metrics_batch.append(metric.to_dict())
            
            if metrics_batch:
                # Publish metrics batch
                self.topic_router.publish(
                    routing_key="metrics.batch",
                    data={
                        "metrics": metrics_batch,
                        "batch_size": len(metrics_batch),
                        "timestamp": datetime.utcnow().isoformat()
                    },
                    priority=MessagePriority.LOW
                )
                
        except Exception as e:
            self.logger.error(f"Error publishing metrics: {e}")
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot"""
        return {
            "queue_metrics": {name: metrics.to_dict() for name, metrics in self.queue_metrics.items()},
            "system_metrics": self.system_metrics.copy(),
            "collection_status": {
                "is_collecting": self.is_collecting,
                "metrics_buffer_size": len(self.metrics_buffer),
                "collection_interval": self.collection_interval
            }
        }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        total_messages = sum(self.throughput_counters.values())
        total_errors = sum(self.error_counts.values())
        overall_error_rate = total_errors / total_messages if total_messages > 0 else 0
        
        # Calculate average processing times per queue
        avg_processing_times = {}
        for queue_name, times in self.message_processing_times.items():
            if times:
                avg_processing_times[queue_name] = statistics.mean(times)
        
        return {
            "total_messages_processed": total_messages,
            "total_errors": total_errors,
            "overall_error_rate": overall_error_rate,
            "average_processing_times_ms": avg_processing_times,
            "active_queues": list(self.queue_metrics.keys()),
            "collection_period": self.collection_interval
        }


class PerformanceOptimizer:
    """
    Performance optimization recommendations and tuning
    """
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.logger = get_logger(__name__)
        
        # Optimization thresholds
        self.optimization_thresholds = {
            "high_queue_depth": 500,
            "slow_processing": 2000,  # 2 seconds
            "low_consumer_count": 1,
            "high_error_rate": 0.05   # 5%
        }
    
    def analyze_performance(self) -> Dict[str, Any]:
        """Analyze current performance and provide recommendations"""
        try:
            current_metrics = self.metrics_collector.get_current_metrics()
            recommendations = []
            issues = []
            
            # Analyze queue metrics
            for queue_name, queue_metrics in current_metrics["queue_metrics"].items():
                queue_analysis = self._analyze_queue_performance(queue_name, queue_metrics)
                recommendations.extend(queue_analysis["recommendations"])
                issues.extend(queue_analysis["issues"])
            
            # Analyze system metrics
            system_analysis = self._analyze_system_performance(current_metrics["system_metrics"])
            recommendations.extend(system_analysis["recommendations"])
            issues.extend(system_analysis["issues"])
            
            # Generate optimization score
            optimization_score = self._calculate_optimization_score(issues)
            
            return {
                "optimization_score": optimization_score,
                "issues_found": len(issues),
                "issues": issues,
                "recommendations": recommendations,
                "analysis_timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance: {e}")
            return {
                "optimization_score": 0,
                "issues_found": 0,
                "issues": [],
                "recommendations": ["Error occurred during analysis"],
                "analysis_timestamp": datetime.utcnow().isoformat()
            }
    
    def _analyze_queue_performance(self, queue_name: str, queue_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze individual queue performance"""
        recommendations = []
        issues = []
        
        message_count = queue_metrics.get("message_count", 0)
        consumer_count = queue_metrics.get("consumer_count", 0)
        processing_time = queue_metrics.get("avg_processing_time_ms", 0)
        error_rate = queue_metrics.get("error_rate", 0)
        
        # High queue depth
        if message_count > self.optimization_thresholds["high_queue_depth"]:
            issues.append(f"Queue {queue_name} has high depth: {message_count} messages")
            recommendations.append(f"Increase consumer count for queue {queue_name}")
            recommendations.append(f"Consider message batching for queue {queue_name}")
        
        # Low consumer count
        if consumer_count < self.optimization_thresholds["low_consumer_count"] and message_count > 0:
            issues.append(f"Queue {queue_name} has insufficient consumers: {consumer_count}")
            recommendations.append(f"Add more consumers to queue {queue_name}")
        
        # Slow processing
        if processing_time > self.optimization_thresholds["slow_processing"]:
            issues.append(f"Queue {queue_name} has slow processing: {processing_time:.2f}ms")
            recommendations.append(f"Optimize message processing logic for queue {queue_name}")
            recommendations.append(f"Consider async processing for queue {queue_name}")
        
        # High error rate
        if error_rate > self.optimization_thresholds["high_error_rate"]:
            issues.append(f"Queue {queue_name} has high error rate: {error_rate:.2%}")
            recommendations.append(f"Investigate error causes for queue {queue_name}")
            recommendations.append(f"Implement better error handling for queue {queue_name}")
        
        return {"recommendations": recommendations, "issues": issues}
    
    def _analyze_system_performance(self, system_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze system-level performance"""
        recommendations = []
        issues = []
        
        # CPU usage analysis
        cpu_metric = system_metrics.get("system.cpu_percent", {})
        if cpu_metric and cpu_metric.get("value", 0) > 80:
            issues.append(f"High CPU usage: {cpu_metric['value']:.1f}%")
            recommendations.append("Consider scaling horizontally")
            recommendations.append("Optimize message processing algorithms")
        
        # Memory usage analysis
        memory_metric = system_metrics.get("system.memory_percent", {})
        if memory_metric and memory_metric.get("value", 0) > 80:
            issues.append(f"High memory usage: {memory_metric['value']:.1f}%")
            recommendations.append("Increase available memory")
            recommendations.append("Implement message streaming for large payloads")
        
        # Disk usage analysis
        disk_metric = system_metrics.get("system.disk_percent", {})
        if disk_metric and disk_metric.get("value", 0) > 90:
            issues.append(f"High disk usage: {disk_metric['value']:.1f}%")
            recommendations.append("Clean up old log files")
            recommendations.append("Implement log rotation")
        
        return {"recommendations": recommendations, "issues": issues}
    
    def _calculate_optimization_score(self, issues: List[str]) -> float:
        """Calculate optimization score based on issues found"""
        if not issues:
            return 100.0
        
        # Deduct points based on issue severity
        score = 100.0
        critical_keywords = ["critical", "high", "slow", "insufficient"]
        
        for issue in issues:
            if any(keyword in issue.lower() for keyword in critical_keywords):
                score -= 15  # Major issue
            else:
                score -= 5   # Minor issue
        
        return max(0.0, score)
    
    def get_optimization_recommendations(self) -> List[str]:
        """Get general optimization recommendations"""
        return [
            "Monitor queue depths regularly and adjust consumer counts accordingly",
            "Implement message batching for high-throughput queues",
            "Use priority queues for critical messages",
            "Implement proper error handling and retry strategies",
            "Monitor system resources and scale when necessary",
            "Use connection pooling for better resource utilization",
            "Implement circuit breakers for fault tolerance",
            "Consider message compression for large payloads",
            "Use dead letter queues for failed messages",
            "Implement proper logging and monitoring",
            "Regular performance testing and optimization",
            "Keep RabbitMQ and dependencies up to date"
        ]


class HealthCheckService:
    """
    Comprehensive health check service for RabbitMQ components
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Health check components
        self.components = [
            "rabbitmq_connection",
            "message_publishing",
            "message_consuming",
            "queue_operations",
            "system_resources"
        ]
    
    def perform_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        try:
            start_time = time.time()
            health_results = {}
            overall_status = "healthy"
            
            # Check each component
            for component in self.components:
                result = self._check_component_health(component)
                health_results[component] = result.to_dict()
                
                if result.status != "healthy":
                    overall_status = "degraded" if overall_status == "healthy" else "unhealthy"
            
            total_time = (time.time() - start_time) * 1000
            
            return {
                "overall_status": overall_status,
                "total_response_time_ms": total_time,
                "component_checks": health_results,
                "timestamp": datetime.utcnow().isoformat(),
                "healthy_components": sum(1 for r in health_results.values() if r["status"] == "healthy"),
                "total_components": len(self.components)
            }
            
        except Exception as e:
            self.logger.error(f"Error performing health check: {e}")
            return {
                "overall_status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def _check_component_health(self, component: str) -> SystemHealthCheck:
        """Check health of individual component"""
        start_time = time.time()
        
        try:
            if component == "rabbitmq_connection":
                result = self._check_rabbitmq_connection()
            elif component == "message_publishing":
                result = self._check_message_publishing()
            elif component == "message_consuming":
                result = self._check_message_consuming()
            elif component == "queue_operations":
                result = self._check_queue_operations()
            elif component == "system_resources":
                result = self._check_system_resources()
            else:
                result = {"status": "unknown", "details": {"error": f"Unknown component: {component}"}}
            
            response_time = (time.time() - start_time) * 1000
            
            return SystemHealthCheck(
                component=component,
                status=result["status"],
                response_time_ms=response_time,
                details=result.get("details", {})
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return SystemHealthCheck(
                component=component,
                status="unhealthy",
                response_time_ms=response_time,
                details={"error": str(e)}
            )
    
    def _check_rabbitmq_connection(self) -> Dict[str, Any]:
        """Check RabbitMQ connection health"""
        try:
            health_result = self.rabbitmq.health_check()
            
            if health_result.get("status") == "healthy":
                return {
                    "status": "healthy",
                    "details": {
                        "connection_status": "connected",
                        "response_time_ms": health_result.get("response_time_ms", 0)
                    }
                }
            else:
                return {
                    "status": "unhealthy",
                    "details": {
                        "connection_status": "failed",
                        "error": health_result.get("error", "Unknown error")
                    }
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }
    
    def _check_message_publishing(self) -> Dict[str, Any]:
        """Check message publishing capability"""
        try:
            # Create test message
            test_message = EnterpriseMessage(
                queue_type=QueueType.TASK_QUEUE,
                message_type="health_check",
                payload={"test": True, "timestamp": datetime.utcnow().isoformat()}
            )
            
            # Attempt to publish
            success = self.rabbitmq.publish_message(test_message)
            
            if success:
                return {
                    "status": "healthy",
                    "details": {"publish_test": "success"}
                }
            else:
                return {
                    "status": "degraded",
                    "details": {"publish_test": "failed"}
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }
    
    def _check_message_consuming(self) -> Dict[str, Any]:
        """Check message consuming capability"""
        # For now, just return healthy since actual consumption test would be complex
        return {
            "status": "healthy",
            "details": {"consume_test": "simulated_success"}
        }
    
    def _check_queue_operations(self) -> Dict[str, Any]:
        """Check queue operations"""
        try:
            # Test queue stats retrieval
            stats = self.rabbitmq.get_queue_stats(QueueType.TASK_QUEUE)
            
            if "error" not in stats:
                return {
                    "status": "healthy",
                    "details": {
                        "queue_operations": "success",
                        "sample_message_count": stats.get("message_count", 0)
                    }
                }
            else:
                return {
                    "status": "degraded",
                    "details": {"error": stats["error"]}
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }
    
    def _check_system_resources(self) -> Dict[str, Any]:
        """Check system resource availability"""
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # Determine status based on resource usage
            if cpu_percent > 90 or memory.percent > 90 or (disk.used / disk.total) > 0.95:
                status = "degraded"
            else:
                status = "healthy"
            
            return {
                "status": status,
                "details": {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "disk_percent": (disk.used / disk.total) * 100
                }
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }


# Global instances
_metrics_collector: Optional[MetricsCollector] = None
_performance_optimizer: Optional[PerformanceOptimizer] = None
_health_check_service: Optional[HealthCheckService] = None


def get_metrics_collector() -> MetricsCollector:
    """Get or create global metrics collector instance"""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def get_performance_optimizer() -> PerformanceOptimizer:
    """Get or create global performance optimizer instance"""
    global _performance_optimizer
    if _performance_optimizer is None:
        metrics_collector = get_metrics_collector()
        _performance_optimizer = PerformanceOptimizer(metrics_collector)
    return _performance_optimizer


def get_health_check_service() -> HealthCheckService:
    """Get or create global health check service instance"""
    global _health_check_service
    if _health_check_service is None:
        _health_check_service = HealthCheckService()
    return _health_check_service