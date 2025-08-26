"""
Messaging Systems Monitoring for RabbitMQ and Kafka
Provides comprehensive monitoring, metrics collection, and health checks
"""
import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import aio_pika
import pika
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager, QueueType
from monitoring.advanced_metrics import MetricCollector
from streaming.kafka_manager import KafkaManager, StreamingTopic


class HealthStatus(Enum):
    """Health status for messaging components"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class RabbitMQMetrics:
    """RabbitMQ metrics snapshot"""
    timestamp: datetime
    queue_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    connection_count: int = 0
    channel_count: int = 0
    total_messages: int = 0
    message_rate: float = 0.0
    consumer_count: int = 0
    ready_messages: int = 0
    unacknowledged_messages: int = 0
    memory_usage: int = 0
    disk_usage: int = 0
    health_status: HealthStatus = HealthStatus.UNKNOWN


@dataclass 
class KafkaMetrics:
    """Kafka metrics snapshot"""
    timestamp: datetime
    topic_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    broker_count: int = 0
    partition_count: int = 0
    consumer_lag: dict[str, int] = field(default_factory=dict)
    messages_per_sec: float = 0.0
    bytes_per_sec: float = 0.0
    active_controllers: int = 0
    under_replicated_partitions: int = 0
    offline_partitions: int = 0
    health_status: HealthStatus = HealthStatus.UNKNOWN


class RabbitMQMonitor:
    """Comprehensive RabbitMQ monitoring and health checking"""
    
    def __init__(self, rabbitmq_manager: RabbitMQManager, metric_collector: MetricCollector):
        self.rabbitmq = rabbitmq_manager
        self.metrics = metric_collector
        self.logger = get_logger(__name__)
        
        # Metrics history for rate calculations
        self.message_count_history: list[tuple[datetime, int]] = []
        self.last_metrics: RabbitMQMetrics | None = None
        
        # Health thresholds
        self.queue_depth_warning = 1000
        self.queue_depth_critical = 5000
        self.consumer_lag_warning = 100
        self.consumer_lag_critical = 500
        
    async def collect_metrics(self) -> RabbitMQMetrics:
        """Collect comprehensive RabbitMQ metrics"""
        try:
            current_time = datetime.now()
            queue_metrics = {}
            total_messages = 0
            total_consumers = 0
            total_ready = 0
            total_unacked = 0
            
            # Collect metrics for each standard queue
            for queue_type in QueueType:
                try:
                    stats = self.rabbitmq.get_queue_stats(queue_type)
                    if "error" not in stats:
                        queue_name = queue_type.value
                        message_count = stats.get("message_count", 0)
                        consumer_count = stats.get("consumer_count", 0)
                        
                        queue_metrics[queue_name] = {
                            "message_count": message_count,
                            "consumer_count": consumer_count,
                            "message_rate": self._calculate_message_rate(queue_name, message_count),
                            "health_status": self._assess_queue_health(message_count, consumer_count),
                            "timestamp": current_time.isoformat()
                        }
                        
                        total_messages += message_count
                        total_consumers += consumer_count
                        total_ready += message_count  # Simplified
                        
                        # Record individual queue metrics
                        self.metrics.set_gauge(
                            "rabbitmq_queue_depth", 
                            message_count,
                            labels={"queue": queue_name}
                        )
                        self.metrics.set_gauge(
                            "rabbitmq_consumer_count", 
                            consumer_count,
                            labels={"queue": queue_name}
                        )
                        
                except Exception as e:
                    self.logger.warning(f"Failed to collect metrics for queue {queue_type.value}: {e}")
            
            # Calculate overall message rate
            self.message_count_history.append((current_time, total_messages))
            self._cleanup_history()
            message_rate = self._calculate_overall_message_rate()
            
            # Create metrics snapshot
            metrics_snapshot = RabbitMQMetrics(
                timestamp=current_time,
                queue_metrics=queue_metrics,
                total_messages=total_messages,
                consumer_count=total_consumers,
                ready_messages=total_ready,
                unacknowledged_messages=total_unacked,
                message_rate=message_rate,
                health_status=self._assess_overall_health(queue_metrics)
            )
            
            # Record global metrics
            self.metrics.set_gauge("rabbitmq_message_rate", message_rate)
            self.metrics.set_gauge("rabbitmq_connection_count", 1)  # Simplified
            
            self.last_metrics = metrics_snapshot
            self.logger.debug("RabbitMQ metrics collected successfully")
            
            return metrics_snapshot
            
        except Exception as e:
            self.logger.error(f"Failed to collect RabbitMQ metrics: {e}")
            return RabbitMQMetrics(
                timestamp=datetime.now(),
                health_status=HealthStatus.UNKNOWN
            )
    
    def _calculate_message_rate(self, queue_name: str, current_count: int) -> float:
        """Calculate message processing rate for a queue"""
        # Simplified rate calculation
        if self.last_metrics and queue_name in self.last_metrics.queue_metrics:
            last_count = self.last_metrics.queue_metrics[queue_name]["message_count"]
            time_diff = (datetime.now() - self.last_metrics.timestamp).total_seconds()
            if time_diff > 0:
                return abs(current_count - last_count) / time_diff
        return 0.0
    
    def _calculate_overall_message_rate(self) -> float:
        """Calculate overall message processing rate"""
        if len(self.message_count_history) < 2:
            return 0.0
            
        recent_points = self.message_count_history[-2:]
        time_diff = (recent_points[1][0] - recent_points[0][0]).total_seconds()
        
        if time_diff > 0:
            count_diff = abs(recent_points[1][1] - recent_points[0][1])
            return count_diff / time_diff
        
        return 0.0
    
    def _cleanup_history(self):
        """Remove old history points"""
        cutoff_time = datetime.now() - timedelta(minutes=5)
        self.message_count_history = [
            point for point in self.message_count_history 
            if point[0] > cutoff_time
        ]
    
    def _assess_queue_health(self, message_count: int, consumer_count: int) -> HealthStatus:
        """Assess health of individual queue"""
        if message_count >= self.queue_depth_critical:
            return HealthStatus.UNHEALTHY
        elif message_count >= self.queue_depth_warning:
            return HealthStatus.DEGRADED
        elif consumer_count == 0 and message_count > 0:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    def _assess_overall_health(self, queue_metrics: dict) -> HealthStatus:
        """Assess overall RabbitMQ health"""
        if not queue_metrics:
            return HealthStatus.UNKNOWN
        
        unhealthy_queues = sum(
            1 for metrics in queue_metrics.values() 
            if metrics["health_status"] == HealthStatus.UNHEALTHY
        )
        degraded_queues = sum(
            1 for metrics in queue_metrics.values() 
            if metrics["health_status"] == HealthStatus.DEGRADED
        )
        
        if unhealthy_queues > 0:
            return HealthStatus.UNHEALTHY
        elif degraded_queues > 0:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    async def health_check(self) -> dict[str, Any]:
        """Perform RabbitMQ health check"""
        try:
            # Test connection
            connection_healthy = self.rabbitmq.connect()
            
            if not connection_healthy:
                return {
                    "component": "rabbitmq",
                    "status": HealthStatus.UNHEALTHY.value,
                    "message": "Cannot connect to RabbitMQ",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Collect current metrics
            metrics = await self.collect_metrics()
            
            # Check queue health
            unhealthy_queues = []
            for queue_name, queue_metrics in metrics.queue_metrics.items():
                if queue_metrics["health_status"] == HealthStatus.UNHEALTHY:
                    unhealthy_queues.append(queue_name)
            
            if unhealthy_queues:
                return {
                    "component": "rabbitmq",
                    "status": HealthStatus.UNHEALTHY.value,
                    "message": f"Unhealthy queues: {', '.join(unhealthy_queues)}",
                    "details": {
                        "unhealthy_queues": unhealthy_queues,
                        "total_messages": metrics.total_messages,
                        "consumer_count": metrics.consumer_count
                    },
                    "timestamp": datetime.now().isoformat()
                }
            
            return {
                "component": "rabbitmq",
                "status": metrics.health_status.value,
                "message": "RabbitMQ is operating normally",
                "details": {
                    "total_messages": metrics.total_messages,
                    "consumer_count": metrics.consumer_count,
                    "message_rate": metrics.message_rate,
                    "queue_count": len(metrics.queue_metrics)
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "component": "rabbitmq",
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Health check failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            }


class KafkaMonitor:
    """Comprehensive Kafka monitoring and health checking"""
    
    def __init__(self, kafka_manager: KafkaManager, metric_collector: MetricCollector):
        self.kafka = kafka_manager
        self.metrics = metric_collector
        self.logger = get_logger(__name__)
        
        # Monitoring state
        self.last_metrics: KafkaMetrics | None = None
        self.consumer_groups_monitored: set[str] = set()
        
        # Health thresholds
        self.consumer_lag_warning = 1000
        self.consumer_lag_critical = 5000
        self.partition_warning_threshold = 100
        
    async def collect_metrics(self) -> KafkaMetrics:
        """Collect comprehensive Kafka metrics"""
        try:
            current_time = datetime.now()
            topic_metrics = {}
            total_partitions = 0
            total_consumer_lag = 0
            
            # Get all topics
            topics = self.kafka.list_topics()
            broker_count = len(self.kafka.bootstrap_servers)
            
            # Collect metrics for each topic
            for topic_name in topics:
                try:
                    topic_info = self.kafka.get_topic_info(topic_name)
                    if "error" not in topic_info:
                        partition_count = topic_info.get("partitions", 0)
                        
                        # Get consumer lag if we have consumers
                        consumer_lag = 0
                        for group_id in self.consumer_groups_monitored:
                            lag_info = self.kafka.get_consumer_lag(group_id)
                            if "error" not in lag_info:
                                consumer_lag += lag_info.get("total_lag", 0)
                        
                        topic_metrics[topic_name] = {
                            "partition_count": partition_count,
                            "consumer_lag": consumer_lag,
                            "replication_factor": topic_info.get("replication_factor", 0),
                            "health_status": self._assess_topic_health(partition_count, consumer_lag),
                            "timestamp": current_time.isoformat()
                        }
                        
                        total_partitions += partition_count
                        total_consumer_lag += consumer_lag
                        
                        # Record individual topic metrics
                        self.metrics.set_gauge(
                            "kafka_partition_count",
                            partition_count,
                            labels={"topic": topic_name}
                        )
                        self.metrics.set_gauge(
                            "kafka_consumer_lag",
                            consumer_lag,
                            labels={"topic": topic_name}
                        )
                        
                except Exception as e:
                    self.logger.warning(f"Failed to collect metrics for topic {topic_name}: {e}")
            
            # Calculate messages per second (simplified)
            messages_per_sec = self._calculate_message_rate()
            
            # Create metrics snapshot
            metrics_snapshot = KafkaMetrics(
                timestamp=current_time,
                topic_metrics=topic_metrics,
                broker_count=broker_count,
                partition_count=total_partitions,
                consumer_lag={"total": total_consumer_lag},
                messages_per_sec=messages_per_sec,
                health_status=self._assess_overall_kafka_health(topic_metrics, broker_count)
            )
            
            # Record global metrics
            self.metrics.set_gauge("kafka_broker_count", broker_count)
            self.metrics.set_gauge("kafka_messages_per_sec", messages_per_sec)
            
            self.last_metrics = metrics_snapshot
            self.logger.debug("Kafka metrics collected successfully")
            
            return metrics_snapshot
            
        except Exception as e:
            self.logger.error(f"Failed to collect Kafka metrics: {e}")
            return KafkaMetrics(
                timestamp=datetime.now(),
                health_status=HealthStatus.UNKNOWN
            )
    
    def _calculate_message_rate(self) -> float:
        """Calculate approximate message processing rate"""
        # This is simplified - in production you'd use JMX metrics
        if self.kafka.producer:
            kafka_metrics = self.kafka.get_metrics()
            return kafka_metrics.get("kafka_metrics", {}).get("messages_produced", 0) / 60.0  # Per minute to per second
        return 0.0
    
    def _assess_topic_health(self, partition_count: int, consumer_lag: int) -> HealthStatus:
        """Assess health of individual topic"""
        if consumer_lag >= self.consumer_lag_critical:
            return HealthStatus.UNHEALTHY
        elif consumer_lag >= self.consumer_lag_warning:
            return HealthStatus.DEGRADED
        elif partition_count == 0:
            return HealthStatus.UNHEALTHY
        else:
            return HealthStatus.HEALTHY
    
    def _assess_overall_kafka_health(self, topic_metrics: dict, broker_count: int) -> HealthStatus:
        """Assess overall Kafka health"""
        if broker_count == 0:
            return HealthStatus.UNHEALTHY
        
        if not topic_metrics:
            return HealthStatus.DEGRADED
        
        unhealthy_topics = sum(
            1 for metrics in topic_metrics.values()
            if metrics["health_status"] == HealthStatus.UNHEALTHY
        )
        degraded_topics = sum(
            1 for metrics in topic_metrics.values()
            if metrics["health_status"] == HealthStatus.DEGRADED
        )
        
        if unhealthy_topics > 0:
            return HealthStatus.UNHEALTHY
        elif degraded_topics > 0:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    def register_consumer_group(self, group_id: str):
        """Register consumer group for monitoring"""
        self.consumer_groups_monitored.add(group_id)
        self.logger.info(f"Registered consumer group for monitoring: {group_id}")
    
    async def health_check(self) -> dict[str, Any]:
        """Perform Kafka health check"""
        try:
            # Test basic connectivity by listing topics
            topics = self.kafka.list_topics()
            
            if not topics:
                return {
                    "component": "kafka",
                    "status": HealthStatus.DEGRADED.value,
                    "message": "Connected but no topics found",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Collect current metrics
            metrics = await self.collect_metrics()
            
            # Check for critical issues
            high_lag_topics = []
            for topic_name, topic_metrics in metrics.topic_metrics.items():
                if topic_metrics["consumer_lag"] >= self.consumer_lag_critical:
                    high_lag_topics.append(topic_name)
            
            if high_lag_topics:
                return {
                    "component": "kafka",
                    "status": HealthStatus.UNHEALTHY.value,
                    "message": f"High consumer lag on topics: {', '.join(high_lag_topics)}",
                    "details": {
                        "high_lag_topics": high_lag_topics,
                        "broker_count": metrics.broker_count,
                        "total_partitions": metrics.partition_count
                    },
                    "timestamp": datetime.now().isoformat()
                }
            
            return {
                "component": "kafka",
                "status": metrics.health_status.value,
                "message": "Kafka is operating normally",
                "details": {
                    "broker_count": metrics.broker_count,
                    "topic_count": len(topics),
                    "total_partitions": metrics.partition_count,
                    "total_consumer_lag": metrics.consumer_lag.get("total", 0),
                    "messages_per_sec": metrics.messages_per_sec
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "component": "kafka",
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Health check failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            }


class MessagingMonitoringManager:
    """Central manager for all messaging system monitoring"""
    
    def __init__(self, rabbitmq_manager: RabbitMQManager, kafka_manager: KafkaManager, metric_collector: MetricCollector):
        self.rabbitmq_monitor = RabbitMQMonitor(rabbitmq_manager, metric_collector)
        self.kafka_monitor = KafkaMonitor(kafka_manager, metric_collector)
        self.metrics = metric_collector
        self.logger = get_logger(__name__)
        
        # Monitoring configuration
        self.monitoring_interval = 30  # seconds
        self.monitoring_active = False
        self.monitoring_task = None
        
    async def start_monitoring(self):
        """Start continuous monitoring of messaging systems"""
        if self.monitoring_active:
            self.logger.warning("Monitoring already active")
            return
        
        self.monitoring_active = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("Started messaging systems monitoring")
    
    async def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring_active = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped messaging systems monitoring")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.monitoring_active:
            try:
                # Collect metrics from both systems
                await self.collect_all_metrics()
                
                # Wait for next collection
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.monitoring_interval)
    
    async def collect_all_metrics(self):
        """Collect metrics from all messaging systems"""
        try:
            # Collect RabbitMQ metrics
            rabbitmq_metrics = await self.rabbitmq_monitor.collect_metrics()
            
            # Collect Kafka metrics  
            kafka_metrics = await self.kafka_monitor.collect_metrics()
            
            # Record combined metrics
            self.metrics.set_gauge(
                "messaging_systems_healthy",
                1 if (rabbitmq_metrics.health_status == HealthStatus.HEALTHY and 
                      kafka_metrics.health_status == HealthStatus.HEALTHY) else 0
            )
            
            self.logger.debug("Collected metrics from all messaging systems")
            
        except Exception as e:
            self.logger.error(f"Failed to collect messaging metrics: {e}")
            self.metrics.increment_counter("messaging_errors")
    
    async def get_health_status(self) -> dict[str, Any]:
        """Get comprehensive health status of all messaging systems"""
        try:
            # Get individual health checks
            rabbitmq_health = await self.rabbitmq_monitor.health_check()
            kafka_health = await self.kafka_monitor.health_check()
            
            # Determine overall status
            overall_status = HealthStatus.HEALTHY
            if (rabbitmq_health["status"] == HealthStatus.UNHEALTHY.value or 
                kafka_health["status"] == HealthStatus.UNHEALTHY.value):
                overall_status = HealthStatus.UNHEALTHY
            elif (rabbitmq_health["status"] == HealthStatus.DEGRADED.value or 
                  kafka_health["status"] == HealthStatus.DEGRADED.value):
                overall_status = HealthStatus.DEGRADED
            
            return {
                "overall_status": overall_status.value,
                "components": {
                    "rabbitmq": rabbitmq_health,
                    "kafka": kafka_health
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "overall_status": HealthStatus.UNKNOWN.value,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of all messaging metrics"""
        return {
            "rabbitmq": {
                "last_collection": self.rabbitmq_monitor.last_metrics.timestamp.isoformat() if self.rabbitmq_monitor.last_metrics else None,
                "queue_count": len(self.rabbitmq_monitor.last_metrics.queue_metrics) if self.rabbitmq_monitor.last_metrics else 0,
                "total_messages": self.rabbitmq_monitor.last_metrics.total_messages if self.rabbitmq_monitor.last_metrics else 0,
                "health_status": self.rabbitmq_monitor.last_metrics.health_status.value if self.rabbitmq_monitor.last_metrics else "unknown"
            },
            "kafka": {
                "last_collection": self.kafka_monitor.last_metrics.timestamp.isoformat() if self.kafka_monitor.last_metrics else None,
                "topic_count": len(self.kafka_monitor.last_metrics.topic_metrics) if self.kafka_monitor.last_metrics else 0,
                "broker_count": self.kafka_monitor.last_metrics.broker_count if self.kafka_monitor.last_metrics else 0,
                "health_status": self.kafka_monitor.last_metrics.health_status.value if self.kafka_monitor.last_metrics else "unknown"
            },
            "monitoring": {
                "active": self.monitoring_active,
                "interval_seconds": self.monitoring_interval
            },
            "timestamp": datetime.now().isoformat()
        }


# Factory function
def create_messaging_monitoring_manager(
    rabbitmq_manager: RabbitMQManager, 
    kafka_manager: KafkaManager, 
    metric_collector: MetricCollector
) -> MessagingMonitoringManager:
    """Create messaging monitoring manager"""
    return MessagingMonitoringManager(rabbitmq_manager, kafka_manager, metric_collector)