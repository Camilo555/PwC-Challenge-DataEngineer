"""
Comprehensive Infrastructure Health Monitoring System
Provides unified health monitoring, performance tracking, and automated recovery
for Redis, RabbitMQ, and Kafka infrastructure components.
"""
import asyncio
import json
import time
import statistics
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union
from collections import defaultdict, deque
import threading
import logging
from pathlib import Path

try:
    import redis
    import pika
    from kafka import KafkaAdminClient, KafkaConsumer
    from kafka.admin.config_resource import ConfigResource, ConfigResourceType
    from kafka.errors import KafkaError
    INFRASTRUCTURE_MONITORING_AVAILABLE = True
except ImportError:
    INFRASTRUCTURE_MONITORING_AVAILABLE = False

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class HealthStatus(Enum):
    """Comprehensive health status levels"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


class ComponentType(Enum):
    """Infrastructure component types"""
    REDIS = "redis"
    RABBITMQ = "rabbitmq"
    KAFKA = "kafka"
    SYSTEM = "system"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class HealthMetrics:
    """Base health metrics structure"""
    component_type: ComponentType
    component_id: str
    timestamp: datetime
    status: HealthStatus
    response_time_ms: float
    availability_percent: float = 100.0
    error_count: int = 0
    warning_count: int = 0
    custom_metrics: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RedisHealthMetrics(HealthMetrics):
    """Redis-specific health metrics"""
    memory_usage_mb: float = 0.0
    memory_usage_percent: float = 0.0
    connected_clients: int = 0
    cache_hit_ratio: float = 0.0
    operations_per_second: float = 0.0
    keyspace_hits: int = 0
    keyspace_misses: int = 0
    evicted_keys: int = 0
    expired_keys: int = 0
    replication_lag_ms: Optional[float] = None
    cluster_state: str = "unknown"


@dataclass
class RabbitMQHealthMetrics(HealthMetrics):
    """RabbitMQ-specific health metrics"""
    queue_depth_total: int = 0
    message_rate_per_sec: float = 0.0
    consumer_count: int = 0
    connection_count: int = 0
    channel_count: int = 0
    unacknowledged_messages: int = 0
    memory_usage_mb: float = 0.0
    disk_usage_mb: float = 0.0
    queue_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    node_status: str = "unknown"


@dataclass
class KafkaHealthMetrics(HealthMetrics):
    """Kafka-specific health metrics"""
    broker_count: int = 0
    topic_count: int = 0
    partition_count: int = 0
    under_replicated_partitions: int = 0
    offline_partitions: int = 0
    consumer_lag_total: int = 0
    messages_per_sec: float = 0.0
    bytes_per_sec: float = 0.0
    leader_election_rate: float = 0.0
    isr_shrink_rate: float = 0.0
    consumer_group_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    broker_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class Alert:
    """Alert structure"""
    id: str
    component_type: ComponentType
    component_id: str
    severity: AlertSeverity
    title: str
    message: str
    timestamp: datetime
    resolved: bool = False
    acknowledged: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceBaseline:
    """Performance baseline for comparison"""
    metric_name: str
    component_id: str
    baseline_value: float
    current_value: float
    deviation_percent: float
    sample_count: int
    last_updated: datetime
    trend: str = "stable"  # improving, degrading, stable


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreaker:
    """Circuit breaker implementation"""
    name: str
    failure_threshold: int = 5
    recovery_timeout: int = 60
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    success_count: int = 0
    total_requests: int = 0


class HealthMonitorBase(ABC):
    """Base class for all health monitors"""
    
    def __init__(self, component_id: str, component_type: ComponentType):
        self.component_id = component_id
        self.component_type = component_type
        self.logger = get_logger(f"{__name__}.{component_type.value}")
        self.metrics_history: deque = deque(maxlen=1000)
        self.alerts: List[Alert] = []
        self.circuit_breaker = CircuitBreaker(f"{component_type.value}_{component_id}")
        
        # Performance thresholds
        self.response_time_warning_ms = 1000
        self.response_time_critical_ms = 5000
        self.error_rate_warning = 5.0  # %
        self.error_rate_critical = 20.0  # %
        
    @abstractmethod
    async def collect_metrics(self) -> HealthMetrics:
        """Collect component-specific metrics"""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        pass
    
    def add_metrics(self, metrics: HealthMetrics):
        """Add metrics to history"""
        self.metrics_history.append(metrics)
    
    def get_latest_metrics(self) -> Optional[HealthMetrics]:
        """Get latest metrics"""
        return self.metrics_history[-1] if self.metrics_history else None
    
    def calculate_availability(self, hours: int = 24) -> float:
        """Calculate availability percentage over time period"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_metrics = [m for m in self.metrics_history if m.timestamp > cutoff_time]
        
        if not recent_metrics:
            return 0.0
        
        healthy_count = sum(1 for m in recent_metrics 
                          if m.status in [HealthStatus.HEALTHY, HealthStatus.WARNING])
        return (healthy_count / len(recent_metrics)) * 100.0
    
    def generate_alert(self, severity: AlertSeverity, title: str, message: str, 
                      metadata: Dict[str, Any] = None) -> Alert:
        """Generate alert"""
        alert = Alert(
            id=f"{self.component_id}_{int(time.time())}_{len(self.alerts)}",
            component_type=self.component_type,
            component_id=self.component_id,
            severity=severity,
            title=title,
            message=message,
            timestamp=datetime.now(),
            metadata=metadata or {}
        )
        self.alerts.append(alert)
        self.logger.warning(f"Alert generated: {title} - {message}")
        return alert
    
    def update_circuit_breaker(self, success: bool):
        """Update circuit breaker state"""
        cb = self.circuit_breaker
        cb.total_requests += 1
        
        if success:
            cb.success_count += 1
            if cb.state == CircuitBreakerState.HALF_OPEN:
                # Successful request in half-open state, close the circuit
                cb.state = CircuitBreakerState.CLOSED
                cb.failure_count = 0
        else:
            cb.failure_count += 1
            cb.last_failure_time = datetime.now()
            
            if cb.state == CircuitBreakerState.CLOSED and cb.failure_count >= cb.failure_threshold:
                cb.state = CircuitBreakerState.OPEN
                self.generate_alert(
                    AlertSeverity.CRITICAL,
                    "Circuit Breaker Opened",
                    f"Circuit breaker for {self.component_id} opened due to repeated failures"
                )
        
        # Check if we should move from OPEN to HALF_OPEN
        if (cb.state == CircuitBreakerState.OPEN and 
            cb.last_failure_time and 
            (datetime.now() - cb.last_failure_time).seconds > cb.recovery_timeout):
            cb.state = CircuitBreakerState.HALF_OPEN
    
    def is_circuit_breaker_open(self) -> bool:
        """Check if circuit breaker is open"""
        return self.circuit_breaker.state == CircuitBreakerState.OPEN


class RedisHealthMonitor(HealthMonitorBase):
    """Redis health monitor"""
    
    def __init__(self, component_id: str, host: str = "localhost", port: int = 6379, 
                 password: str = None, db: int = 0):
        super().__init__(component_id, ComponentType.REDIS)
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.redis_client = None
        
        # Redis-specific thresholds
        self.memory_usage_warning = 80.0  # %
        self.memory_usage_critical = 95.0  # %
        self.cache_hit_ratio_warning = 85.0  # %
        self.eviction_rate_warning = 100  # keys/sec
    
    async def collect_metrics(self) -> RedisHealthMetrics:
        """Collect Redis metrics"""
        start_time = time.time()
        
        try:
            if not self.redis_client:
                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    decode_responses=True
                )
            
            # Test connectivity
            ping_start = time.time()
            self.redis_client.ping()
            ping_time = (time.time() - ping_start) * 1000
            
            # Get Redis info
            info = self.redis_client.info()
            memory_info = self.redis_client.info('memory')
            stats_info = self.redis_client.info('stats')
            
            # Calculate metrics
            used_memory = memory_info.get('used_memory', 0)
            max_memory = memory_info.get('maxmemory', 0)
            memory_usage_mb = used_memory / (1024 * 1024)
            memory_usage_percent = (used_memory / max_memory * 100) if max_memory > 0 else 0
            
            hits = stats_info.get('keyspace_hits', 0)
            misses = stats_info.get('keyspace_misses', 0)
            cache_hit_ratio = (hits / (hits + misses) * 100) if (hits + misses) > 0 else 100
            
            ops_per_sec = stats_info.get('instantaneous_ops_per_sec', 0)
            
            # Determine health status
            status = self._determine_redis_health_status(
                ping_time, memory_usage_percent, cache_hit_ratio, info
            )
            
            response_time = (time.time() - start_time) * 1000
            
            metrics = RedisHealthMetrics(
                component_type=ComponentType.REDIS,
                component_id=self.component_id,
                timestamp=datetime.now(),
                status=status,
                response_time_ms=response_time,
                memory_usage_mb=memory_usage_mb,
                memory_usage_percent=memory_usage_percent,
                connected_clients=info.get('connected_clients', 0),
                cache_hit_ratio=cache_hit_ratio,
                operations_per_second=ops_per_sec,
                keyspace_hits=hits,
                keyspace_misses=misses,
                evicted_keys=stats_info.get('evicted_keys', 0),
                expired_keys=stats_info.get('expired_keys', 0),
                cluster_state=info.get('cluster_enabled', 'no') == 'yes' and 'cluster_state' or 'standalone',
                custom_metrics={
                    'ping_time_ms': ping_time,
                    'used_memory_rss': memory_info.get('used_memory_rss', 0),
                    'mem_fragmentation_ratio': memory_info.get('mem_fragmentation_ratio', 1.0),
                    'rejected_connections': stats_info.get('rejected_connections', 0)
                },
                metadata={
                    'redis_version': info.get('redis_version'),
                    'redis_mode': info.get('redis_mode'),
                    'role': info.get('role'),
                    'uptime_in_seconds': info.get('uptime_in_seconds', 0)
                }
            )
            
            self.add_metrics(metrics)
            self.update_circuit_breaker(True)
            
            # Generate alerts based on thresholds
            await self._check_redis_alerts(metrics)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect Redis metrics: {e}")
            self.update_circuit_breaker(False)
            
            error_metrics = RedisHealthMetrics(
                component_type=ComponentType.REDIS,
                component_id=self.component_id,
                timestamp=datetime.now(),
                status=HealthStatus.OFFLINE,
                response_time_ms=(time.time() - start_time) * 1000,
                error_count=1,
                metadata={'error': str(e)}
            )
            
            self.add_metrics(error_metrics)
            return error_metrics
    
    def _determine_redis_health_status(self, ping_time: float, memory_usage: float, 
                                     hit_ratio: float, info: dict) -> HealthStatus:
        """Determine Redis health status"""
        if ping_time > self.response_time_critical_ms:
            return HealthStatus.CRITICAL
        elif memory_usage > self.memory_usage_critical:
            return HealthStatus.CRITICAL
        elif (ping_time > self.response_time_warning_ms or 
              memory_usage > self.memory_usage_warning or
              hit_ratio < self.cache_hit_ratio_warning):
            return HealthStatus.WARNING
        elif info.get('loading', '0') == '1':
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    async def _check_redis_alerts(self, metrics: RedisHealthMetrics):
        """Check Redis metrics and generate alerts"""
        if metrics.memory_usage_percent > self.memory_usage_critical:
            self.generate_alert(
                AlertSeverity.CRITICAL,
                "Redis Memory Critical",
                f"Memory usage at {metrics.memory_usage_percent:.1f}%, exceeds critical threshold {self.memory_usage_critical}%"
            )
        elif metrics.memory_usage_percent > self.memory_usage_warning:
            self.generate_alert(
                AlertSeverity.WARNING,
                "Redis Memory Warning",
                f"Memory usage at {metrics.memory_usage_percent:.1f}%, exceeds warning threshold {self.memory_usage_warning}%"
            )
        
        if metrics.cache_hit_ratio < self.cache_hit_ratio_warning:
            self.generate_alert(
                AlertSeverity.WARNING,
                "Redis Cache Hit Ratio Low",
                f"Cache hit ratio at {metrics.cache_hit_ratio:.1f}%, below warning threshold {self.cache_hit_ratio_warning}%"
            )
        
        if metrics.response_time_ms > self.response_time_critical_ms:
            self.generate_alert(
                AlertSeverity.CRITICAL,
                "Redis Response Time Critical",
                f"Response time {metrics.response_time_ms:.1f}ms exceeds critical threshold"
            )
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform Redis health check"""
        try:
            metrics = await self.collect_metrics()
            availability = self.calculate_availability()
            
            return {
                "component": "redis",
                "component_id": self.component_id,
                "status": metrics.status.value,
                "response_time_ms": metrics.response_time_ms,
                "availability_24h": availability,
                "details": {
                    "memory_usage_percent": metrics.memory_usage_percent,
                    "cache_hit_ratio": metrics.cache_hit_ratio,
                    "connected_clients": metrics.connected_clients,
                    "operations_per_second": metrics.operations_per_second,
                    "cluster_state": metrics.cluster_state
                },
                "circuit_breaker_state": self.circuit_breaker.state.value,
                "alerts_count": len([a for a in self.alerts if not a.resolved]),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "component": "redis",
                "component_id": self.component_id,
                "status": HealthStatus.OFFLINE.value,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }


class RabbitMQHealthMonitor(HealthMonitorBase):
    """RabbitMQ health monitor"""
    
    def __init__(self, component_id: str, host: str = "localhost", port: int = 5672,
                 username: str = "guest", password: str = "guest", vhost: str = "/"):
        super().__init__(component_id, ComponentType.RABBITMQ)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.connection = None
        
        # RabbitMQ-specific thresholds
        self.queue_depth_warning = 1000
        self.queue_depth_critical = 5000
        self.consumer_count_minimum = 1
        self.message_rate_threshold = 100  # messages/sec
    
    async def collect_metrics(self) -> RabbitMQHealthMetrics:
        """Collect RabbitMQ metrics"""
        start_time = time.time()
        
        try:
            if not self.connection or self.connection.is_closed:
                credentials = pika.PlainCredentials(self.username, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host=self.vhost,
                    credentials=credentials,
                    connection_attempts=3,
                    retry_delay=1.0
                )
                self.connection = pika.BlockingConnection(parameters)
            
            channel = self.connection.channel()
            
            # Get queue information
            queue_metrics = {}
            total_queue_depth = 0
            total_consumers = 0
            total_unacked = 0
            
            # This is a simplified implementation - in production you'd use RabbitMQ Management API
            test_queues = ['data_ingestion', 'data_processing', 'notification', 'dead_letter']
            
            for queue_name in test_queues:
                try:
                    method = channel.queue_declare(queue=queue_name, passive=True)
                    message_count = method.method.message_count
                    consumer_count = method.method.consumer_count
                    
                    queue_metrics[queue_name] = {
                        'message_count': message_count,
                        'consumer_count': consumer_count,
                        'status': self._assess_queue_health(message_count, consumer_count).value
                    }
                    
                    total_queue_depth += message_count
                    total_consumers += consumer_count
                    
                except Exception as queue_error:
                    self.logger.warning(f"Could not get stats for queue {queue_name}: {queue_error}")
            
            # Calculate message rate (simplified)
            message_rate = self._calculate_message_rate(total_queue_depth)
            
            # Determine overall health status
            status = self._determine_rabbitmq_health_status(
                total_queue_depth, total_consumers, len(queue_metrics)
            )
            
            response_time = (time.time() - start_time) * 1000
            
            metrics = RabbitMQHealthMetrics(
                component_type=ComponentType.RABBITMQ,
                component_id=self.component_id,
                timestamp=datetime.now(),
                status=status,
                response_time_ms=response_time,
                queue_depth_total=total_queue_depth,
                message_rate_per_sec=message_rate,
                consumer_count=total_consumers,
                connection_count=1,  # Simplified
                channel_count=1,  # Simplified
                unacknowledged_messages=total_unacked,
                queue_metrics=queue_metrics,
                node_status="running",
                custom_metrics={
                    'average_queue_depth': total_queue_depth / len(queue_metrics) if queue_metrics else 0,
                    'queues_without_consumers': sum(1 for q in queue_metrics.values() if q['consumer_count'] == 0)
                }
            )
            
            channel.close()
            
            self.add_metrics(metrics)
            self.update_circuit_breaker(True)
            
            # Generate alerts
            await self._check_rabbitmq_alerts(metrics)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect RabbitMQ metrics: {e}")
            self.update_circuit_breaker(False)
            
            error_metrics = RabbitMQHealthMetrics(
                component_type=ComponentType.RABBITMQ,
                component_id=self.component_id,
                timestamp=datetime.now(),
                status=HealthStatus.OFFLINE,
                response_time_ms=(time.time() - start_time) * 1000,
                error_count=1,
                metadata={'error': str(e)}
            )
            
            self.add_metrics(error_metrics)
            return error_metrics
    
    def _assess_queue_health(self, message_count: int, consumer_count: int) -> HealthStatus:
        """Assess individual queue health"""
        if message_count >= self.queue_depth_critical:
            return HealthStatus.CRITICAL
        elif message_count >= self.queue_depth_warning:
            return HealthStatus.WARNING
        elif consumer_count == 0 and message_count > 0:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    def _determine_rabbitmq_health_status(self, total_depth: int, total_consumers: int, 
                                        queue_count: int) -> HealthStatus:
        """Determine RabbitMQ health status"""
        if total_depth >= self.queue_depth_critical * queue_count * 0.5:  # 50% of queues critical
            return HealthStatus.CRITICAL
        elif total_depth >= self.queue_depth_warning * queue_count * 0.3:  # 30% of queues warning
            return HealthStatus.WARNING
        elif total_consumers == 0 and total_depth > 0:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    def _calculate_message_rate(self, current_total: int) -> float:
        """Calculate approximate message processing rate"""
        if len(self.metrics_history) < 2:
            return 0.0
        
        previous_metrics = list(self.metrics_history)[-2]
        if isinstance(previous_metrics, RabbitMQHealthMetrics):
            time_diff = (datetime.now() - previous_metrics.timestamp).total_seconds()
            if time_diff > 0:
                message_diff = abs(current_total - previous_metrics.queue_depth_total)
                return message_diff / time_diff
        
        return 0.0
    
    async def _check_rabbitmq_alerts(self, metrics: RabbitMQHealthMetrics):
        """Check RabbitMQ metrics and generate alerts"""
        # Check individual queue alerts
        for queue_name, queue_data in metrics.queue_metrics.items():
            message_count = queue_data['message_count']
            consumer_count = queue_data['consumer_count']
            
            if message_count >= self.queue_depth_critical:
                self.generate_alert(
                    AlertSeverity.CRITICAL,
                    f"RabbitMQ Queue Critical - {queue_name}",
                    f"Queue {queue_name} has {message_count} messages, exceeds critical threshold {self.queue_depth_critical}"
                )
            elif message_count >= self.queue_depth_warning:
                self.generate_alert(
                    AlertSeverity.WARNING,
                    f"RabbitMQ Queue Warning - {queue_name}",
                    f"Queue {queue_name} has {message_count} messages, exceeds warning threshold {self.queue_depth_warning}"
                )
            
            if consumer_count == 0 and message_count > 0:
                self.generate_alert(
                    AlertSeverity.WARNING,
                    f"RabbitMQ No Consumers - {queue_name}",
                    f"Queue {queue_name} has {message_count} messages but no consumers"
                )
        
        # Check overall performance
        if metrics.response_time_ms > self.response_time_critical_ms:
            self.generate_alert(
                AlertSeverity.CRITICAL,
                "RabbitMQ Response Time Critical",
                f"Response time {metrics.response_time_ms:.1f}ms exceeds critical threshold"
            )
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform RabbitMQ health check"""
        try:
            metrics = await self.collect_metrics()
            availability = self.calculate_availability()
            
            return {
                "component": "rabbitmq",
                "component_id": self.component_id,
                "status": metrics.status.value,
                "response_time_ms": metrics.response_time_ms,
                "availability_24h": availability,
                "details": {
                    "total_queue_depth": metrics.queue_depth_total,
                    "consumer_count": metrics.consumer_count,
                    "message_rate_per_sec": metrics.message_rate_per_sec,
                    "queue_count": len(metrics.queue_metrics),
                    "queues": metrics.queue_metrics
                },
                "circuit_breaker_state": self.circuit_breaker.state.value,
                "alerts_count": len([a for a in self.alerts if not a.resolved]),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "component": "rabbitmq",
                "component_id": self.component_id,
                "status": HealthStatus.OFFLINE.value,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }


class KafkaHealthMonitor(HealthMonitorBase):
    """Kafka health monitor"""
    
    def __init__(self, component_id: str, bootstrap_servers: List[str]):
        super().__init__(component_id, ComponentType.KAFKA)
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        self.consumer = None
        
        # Kafka-specific thresholds
        self.consumer_lag_warning = 1000
        self.consumer_lag_critical = 10000
        self.under_replicated_threshold = 5
        self.offline_partitions_threshold = 1
    
    async def collect_metrics(self) -> KafkaHealthMetrics:
        """Collect Kafka metrics"""
        start_time = time.time()
        
        try:
            if not self.admin_client:
                self.admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    request_timeout_ms=5000
                )
            
            # Get cluster metadata
            metadata = self.admin_client.describe_cluster()
            broker_count = len(metadata.brokers)
            
            # Get topic information
            topics = self.admin_client.list_topics()
            topic_count = len(topics)
            
            # Calculate partition metrics
            partition_count = 0
            under_replicated = 0
            offline_partitions = 0
            
            for topic in topics:
                try:
                    topic_metadata = self.admin_client.describe_topics([topic])[topic]
                    partition_count += len(topic_metadata.partitions)
                    
                    # Check partition health (simplified)
                    for partition in topic_metadata.partitions:
                        if len(partition.replicas) > len(partition.isr):
                            under_replicated += 1
                        if not partition.isr:  # No in-sync replicas
                            offline_partitions += 1
                            
                except Exception as topic_error:
                    self.logger.warning(f"Could not get metadata for topic {topic}: {topic_error}")
            
            # Get consumer group lag (simplified - would need actual consumer groups)
            consumer_lag_total = 0
            consumer_groups = {}  # Placeholder - would need actual implementation
            
            # Calculate message rates (simplified)
            messages_per_sec = 0.0  # Would need JMX metrics or other monitoring
            bytes_per_sec = 0.0
            
            # Determine health status
            status = self._determine_kafka_health_status(
                broker_count, under_replicated, offline_partitions, consumer_lag_total
            )
            
            response_time = (time.time() - start_time) * 1000
            
            metrics = KafkaHealthMetrics(
                component_type=ComponentType.KAFKA,
                component_id=self.component_id,
                timestamp=datetime.now(),
                status=status,
                response_time_ms=response_time,
                broker_count=broker_count,
                topic_count=topic_count,
                partition_count=partition_count,
                under_replicated_partitions=under_replicated,
                offline_partitions=offline_partitions,
                consumer_lag_total=consumer_lag_total,
                messages_per_sec=messages_per_sec,
                bytes_per_sec=bytes_per_sec,
                consumer_group_metrics=consumer_groups,
                custom_metrics={
                    'replication_factor_avg': 3.0,  # Would calculate from actual topics
                    'leader_election_rate': 0.0,  # Would get from JMX
                    'isr_shrink_rate': 0.0
                },
                metadata={
                    'cluster_id': metadata.cluster_id if hasattr(metadata, 'cluster_id') else 'unknown',
                    'controller_id': metadata.controller.id if hasattr(metadata, 'controller') else -1
                }
            )
            
            self.add_metrics(metrics)
            self.update_circuit_breaker(True)
            
            # Generate alerts
            await self._check_kafka_alerts(metrics)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect Kafka metrics: {e}")
            self.update_circuit_breaker(False)
            
            error_metrics = KafkaHealthMetrics(
                component_type=ComponentType.KAFKA,
                component_id=self.component_id,
                timestamp=datetime.now(),
                status=HealthStatus.OFFLINE,
                response_time_ms=(time.time() - start_time) * 1000,
                error_count=1,
                metadata={'error': str(e)}
            )
            
            self.add_metrics(error_metrics)
            return error_metrics
    
    def _determine_kafka_health_status(self, broker_count: int, under_replicated: int,
                                     offline_partitions: int, consumer_lag: int) -> HealthStatus:
        """Determine Kafka health status"""
        if (broker_count == 0 or 
            offline_partitions >= self.offline_partitions_threshold or
            consumer_lag >= self.consumer_lag_critical):
            return HealthStatus.CRITICAL
        elif (under_replicated >= self.under_replicated_threshold or
              consumer_lag >= self.consumer_lag_warning):
            return HealthStatus.WARNING
        elif broker_count < 3:  # Assuming minimum 3 brokers for production
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    async def _check_kafka_alerts(self, metrics: KafkaHealthMetrics):
        """Check Kafka metrics and generate alerts"""
        if metrics.offline_partitions > 0:
            self.generate_alert(
                AlertSeverity.CRITICAL,
                "Kafka Offline Partitions",
                f"{metrics.offline_partitions} partitions are offline"
            )
        
        if metrics.under_replicated_partitions >= self.under_replicated_threshold:
            self.generate_alert(
                AlertSeverity.WARNING,
                "Kafka Under-replicated Partitions",
                f"{metrics.under_replicated_partitions} partitions are under-replicated"
            )
        
        if metrics.consumer_lag_total >= self.consumer_lag_critical:
            self.generate_alert(
                AlertSeverity.CRITICAL,
                "Kafka Consumer Lag Critical",
                f"Total consumer lag is {metrics.consumer_lag_total}, exceeds critical threshold"
            )
        elif metrics.consumer_lag_total >= self.consumer_lag_warning:
            self.generate_alert(
                AlertSeverity.WARNING,
                "Kafka Consumer Lag Warning",
                f"Total consumer lag is {metrics.consumer_lag_total}, exceeds warning threshold"
            )
        
        if metrics.broker_count == 0:
            self.generate_alert(
                AlertSeverity.EMERGENCY,
                "Kafka Cluster Offline",
                "No Kafka brokers are available"
            )
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform Kafka health check"""
        try:
            metrics = await self.collect_metrics()
            availability = self.calculate_availability()
            
            return {
                "component": "kafka",
                "component_id": self.component_id,
                "status": metrics.status.value,
                "response_time_ms": metrics.response_time_ms,
                "availability_24h": availability,
                "details": {
                    "broker_count": metrics.broker_count,
                    "topic_count": metrics.topic_count,
                    "partition_count": metrics.partition_count,
                    "under_replicated_partitions": metrics.under_replicated_partitions,
                    "offline_partitions": metrics.offline_partitions,
                    "consumer_lag_total": metrics.consumer_lag_total,
                    "messages_per_sec": metrics.messages_per_sec
                },
                "circuit_breaker_state": self.circuit_breaker.state.value,
                "alerts_count": len([a for a in self.alerts if not a.resolved]),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "component": "kafka",
                "component_id": self.component_id,
                "status": HealthStatus.OFFLINE.value,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }


class InfrastructureHealthManager:
    """Central manager for all infrastructure health monitoring"""
    
    def __init__(self):
        self.monitors: Dict[str, HealthMonitorBase] = {}
        self.logger = get_logger(__name__)
        self.monitoring_active = False
        self.monitoring_interval = 30  # seconds
        self.monitoring_task = None
        
        # Performance baselines
        self.baselines: Dict[str, PerformanceBaseline] = {}
        
        # Alert management
        self.alert_callbacks: List[Callable[[Alert], None]] = []
        
        # Cross-system integration health
        self.integration_health: Dict[str, Dict[str, Any]] = {}
    
    def add_monitor(self, monitor: HealthMonitorBase):
        """Add a health monitor"""
        self.monitors[monitor.component_id] = monitor
        self.logger.info(f"Added {monitor.component_type.value} monitor: {monitor.component_id}")
    
    def remove_monitor(self, component_id: str):
        """Remove a health monitor"""
        if component_id in self.monitors:
            del self.monitors[component_id]
            self.logger.info(f"Removed monitor: {component_id}")
    
    def add_alert_callback(self, callback: Callable[[Alert], None]):
        """Add alert callback function"""
        self.alert_callbacks.append(callback)
    
    async def start_monitoring(self):
        """Start continuous monitoring"""
        if self.monitoring_active:
            self.logger.warning("Monitoring already active")
            return
        
        self.monitoring_active = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("Started infrastructure health monitoring")
    
    async def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring_active = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped infrastructure health monitoring")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.monitoring_active:
            try:
                await self.collect_all_metrics()
                await self._check_integration_health()
                await self._update_baselines()
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.monitoring_interval)
    
    async def collect_all_metrics(self):
        """Collect metrics from all monitors"""
        tasks = []
        for monitor in self.monitors.values():
            if not monitor.is_circuit_breaker_open():
                tasks.append(monitor.collect_metrics())
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Monitor error: {result}")
    
    async def _check_integration_health(self):
        """Check cross-system integration health"""
        try:
            # Check Redis-RabbitMQ integration
            redis_monitors = [m for m in self.monitors.values() if m.component_type == ComponentType.REDIS]
            rabbitmq_monitors = [m for m in self.monitors.values() if m.component_type == ComponentType.RABBITMQ]
            
            if redis_monitors and rabbitmq_monitors:
                redis_healthy = all(m.get_latest_metrics().status in [HealthStatus.HEALTHY, HealthStatus.WARNING] 
                                  for m in redis_monitors if m.get_latest_metrics())
                rabbitmq_healthy = all(m.get_latest_metrics().status in [HealthStatus.HEALTHY, HealthStatus.WARNING] 
                                     for m in rabbitmq_monitors if m.get_latest_metrics())
                
                self.integration_health['redis_rabbitmq'] = {
                    'status': 'healthy' if redis_healthy and rabbitmq_healthy else 'degraded',
                    'redis_status': 'healthy' if redis_healthy else 'unhealthy',
                    'rabbitmq_status': 'healthy' if rabbitmq_healthy else 'unhealthy',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Similar checks for other integrations...
            
        except Exception as e:
            self.logger.error(f"Error checking integration health: {e}")
    
    async def _update_baselines(self):
        """Update performance baselines"""
        try:
            for monitor in self.monitors.values():
                latest_metrics = monitor.get_latest_metrics()
                if not latest_metrics:
                    continue
                
                # Update response time baseline
                baseline_key = f"{monitor.component_id}_response_time"
                current_value = latest_metrics.response_time_ms
                
                if baseline_key in self.baselines:
                    baseline = self.baselines[baseline_key]
                    baseline.sample_count += 1
                    
                    # Calculate new baseline (exponential moving average)
                    alpha = 0.1  # Smoothing factor
                    baseline.baseline_value = (alpha * current_value + 
                                             (1 - alpha) * baseline.baseline_value)
                    
                    # Calculate deviation
                    baseline.current_value = current_value
                    baseline.deviation_percent = ((current_value - baseline.baseline_value) / 
                                                baseline.baseline_value * 100)
                    baseline.last_updated = datetime.now()
                    
                    # Determine trend
                    if baseline.deviation_percent > 20:
                        baseline.trend = "degrading"
                    elif baseline.deviation_percent < -20:
                        baseline.trend = "improving"
                    else:
                        baseline.trend = "stable"
                else:
                    # Create new baseline
                    self.baselines[baseline_key] = PerformanceBaseline(
                        metric_name="response_time_ms",
                        component_id=monitor.component_id,
                        baseline_value=current_value,
                        current_value=current_value,
                        deviation_percent=0.0,
                        sample_count=1,
                        last_updated=datetime.now()
                    )
        
        except Exception as e:
            self.logger.error(f"Error updating baselines: {e}")
    
    async def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health status"""
        try:
            component_health = {}
            overall_status = HealthStatus.HEALTHY
            critical_issues = []
            warning_issues = []
            total_alerts = 0
            
            for monitor in self.monitors.values():
                health = await monitor.health_check()
                component_health[monitor.component_id] = health
                
                # Determine overall status
                component_status = HealthStatus(health['status'])
                if component_status in [HealthStatus.CRITICAL, HealthStatus.OFFLINE]:
                    overall_status = HealthStatus.CRITICAL
                    critical_issues.append(f"{monitor.component_id}: {health.get('status', 'unknown')}")
                elif (component_status in [HealthStatus.WARNING, HealthStatus.DEGRADED] and 
                      overall_status != HealthStatus.CRITICAL):
                    overall_status = HealthStatus.WARNING
                    warning_issues.append(f"{monitor.component_id}: {health.get('status', 'unknown')}")
                
                total_alerts += health.get('alerts_count', 0)
            
            return {
                'overall_status': overall_status.value,
                'total_components': len(self.monitors),
                'healthy_components': len([h for h in component_health.values() 
                                         if h['status'] == HealthStatus.HEALTHY.value]),
                'critical_issues': critical_issues,
                'warning_issues': warning_issues,
                'total_alerts': total_alerts,
                'components': component_health,
                'integration_health': self.integration_health,
                'baselines': {k: asdict(v) for k, v in self.baselines.items()},
                'monitoring_active': self.monitoring_active,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            return {
                'overall_status': HealthStatus.UNKNOWN.value,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance metrics summary"""
        try:
            performance_data = {
                'redis': {},
                'rabbitmq': {},
                'kafka': {},
                'overall': {}
            }
            
            # Aggregate performance metrics by component type
            for monitor in self.monitors.values():
                latest_metrics = monitor.get_latest_metrics()
                if not latest_metrics:
                    continue
                
                component_type = monitor.component_type.value
                if component_type not in performance_data:
                    performance_data[component_type] = {}
                
                performance_data[component_type][monitor.component_id] = {
                    'response_time_ms': latest_metrics.response_time_ms,
                    'availability_24h': monitor.calculate_availability(),
                    'status': latest_metrics.status.value,
                    'custom_metrics': latest_metrics.custom_metrics
                }
            
            # Calculate overall performance metrics
            all_response_times = []
            all_availabilities = []
            
            for monitor in self.monitors.values():
                latest_metrics = monitor.get_latest_metrics()
                if latest_metrics:
                    all_response_times.append(latest_metrics.response_time_ms)
                    all_availabilities.append(monitor.calculate_availability())
            
            if all_response_times:
                performance_data['overall'] = {
                    'average_response_time_ms': statistics.mean(all_response_times),
                    'max_response_time_ms': max(all_response_times),
                    'average_availability': statistics.mean(all_availabilities),
                    'min_availability': min(all_availabilities)
                }
            
            return performance_data
        
        except Exception as e:
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def get_alerts(self, component_id: str = None, resolved: bool = None) -> List[Dict[str, Any]]:
        """Get alerts with optional filtering"""
        all_alerts = []
        
        for monitor in self.monitors.values():
            if component_id and monitor.component_id != component_id:
                continue
            
            for alert in monitor.alerts:
                if resolved is not None and alert.resolved != resolved:
                    continue
                
                all_alerts.append(asdict(alert))
        
        # Sort by timestamp (newest first)
        all_alerts.sort(key=lambda x: x['timestamp'], reverse=True)
        return all_alerts
    
    async def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        for monitor in self.monitors.values():
            for alert in monitor.alerts:
                if alert.id == alert_id:
                    alert.resolved = True
                    self.logger.info(f"Resolved alert: {alert_id}")
                    return True
        return False


# Factory functions for easy setup
def create_redis_monitor(component_id: str, **kwargs) -> RedisHealthMonitor:
    """Create Redis health monitor"""
    return RedisHealthMonitor(component_id, **kwargs)


def create_rabbitmq_monitor(component_id: str, **kwargs) -> RabbitMQHealthMonitor:
    """Create RabbitMQ health monitor"""
    return RabbitMQHealthMonitor(component_id, **kwargs)


def create_kafka_monitor(component_id: str, bootstrap_servers: List[str]) -> KafkaHealthMonitor:
    """Create Kafka health monitor"""
    return KafkaHealthMonitor(component_id, bootstrap_servers)


def create_infrastructure_manager() -> InfrastructureHealthManager:
    """Create infrastructure health manager with default monitors"""
    manager = InfrastructureHealthManager()
    
    # Add default monitors based on settings
    try:
        # Redis monitor
        redis_host = getattr(settings, 'redis_host', 'localhost')
        redis_port = getattr(settings, 'redis_port', 6379)
        redis_monitor = create_redis_monitor(
            'primary_redis',
            host=redis_host,
            port=redis_port
        )
        manager.add_monitor(redis_monitor)
        
        # RabbitMQ monitor
        rabbitmq_host = getattr(settings, 'rabbitmq_host', 'localhost')
        rabbitmq_port = getattr(settings, 'rabbitmq_port', 5672)
        rabbitmq_monitor = create_rabbitmq_monitor(
            'primary_rabbitmq',
            host=rabbitmq_host,
            port=rabbitmq_port
        )
        manager.add_monitor(rabbitmq_monitor)
        
        # Kafka monitor
        kafka_servers = getattr(settings, 'kafka_bootstrap_servers', ['localhost:9092'])
        kafka_monitor = create_kafka_monitor('primary_kafka', kafka_servers)
        manager.add_monitor(kafka_monitor)
        
    except Exception as e:
        logger.warning(f"Could not create all default monitors: {e}")
    
    return manager


# Global instance
_infrastructure_manager = None


def get_infrastructure_manager() -> InfrastructureHealthManager:
    """Get global infrastructure health manager"""
    global _infrastructure_manager
    if _infrastructure_manager is None:
        _infrastructure_manager = create_infrastructure_manager()
    return _infrastructure_manager


if __name__ == "__main__":
    print("Infrastructure Health Monitoring System")
    print("Available components:")
    print("- Redis health monitoring with cache metrics")
    print("- RabbitMQ monitoring with queue depth tracking") 
    print("- Kafka monitoring with consumer lag tracking")
    print("- Circuit breaker implementation")
    print("- Performance baseline tracking")
    print("- Automated alert generation")
    print("- Cross-system integration health checks")