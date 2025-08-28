"""
Enterprise RabbitMQ Manager with High Availability

This module provides comprehensive RabbitMQ integration for the enterprise data platform
with connection pooling, high availability, automatic failover, and advanced messaging patterns.
"""

import asyncio
import json
import os
import ssl
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union
from contextlib import asynccontextmanager, contextmanager

import aio_pika
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from pika.adapters import BlockingConnection
from aio_pika.connection import Connection as AsyncConnection
from aio_pika.channel import Channel as AsyncChannel
from aio_pika.queue import Queue as AsyncQueue
from aio_pika.exchange import Exchange as AsyncExchange

from core.config.unified_config import get_unified_config
from core.logging import get_logger


class MessagePriority(Enum):
    """Message priority levels for queue prioritization"""
    LOWEST = 1
    LOW = 3
    NORMAL = 5
    HIGH = 7
    HIGHEST = 9
    CRITICAL = 10


class ExchangeType(Enum):
    """Exchange types for different messaging patterns"""
    DIRECT = "direct"
    TOPIC = "topic"
    FANOUT = "fanout"
    HEADERS = "headers"


class QueueType(Enum):
    """Comprehensive queue types for enterprise messaging"""
    # Core ETL and Processing
    ETL_BRONZE = "etl.bronze"
    ETL_SILVER = "etl.silver"
    ETL_GOLD = "etl.gold"
    
    # ML and Analytics
    ML_TRAINING = "ml.training"
    ML_INFERENCE = "ml.inference"
    ML_MODEL_DEPLOYMENT = "ml.deployment"
    ML_FEATURE_PIPELINE = "ml.features"
    
    # Data Quality and Governance
    DATA_QUALITY_VALIDATION = "dq.validation"
    DATA_QUALITY_MONITORING = "dq.monitoring"
    DATA_LINEAGE = "governance.lineage"
    DATA_CATALOG = "governance.catalog"
    
    # Real-time Analytics
    STREAMING_EVENTS = "streaming.events"
    REALTIME_ANALYTICS = "analytics.realtime"
    DASHBOARD_UPDATES = "dashboard.updates"
    
    # User and Session Management
    USER_EVENTS = "user.events"
    SESSION_MANAGEMENT = "session.management"
    USER_ACTIVITY = "user.activity"
    
    # Notifications and Alerts
    SYSTEM_ALERTS = "alerts.system"
    DATA_ALERTS = "alerts.data"
    USER_NOTIFICATIONS = "notifications.user"
    EMAIL_NOTIFICATIONS = "notifications.email"
    
    # Audit and Logging
    AUDIT_LOGS = "audit.logs"
    SECURITY_EVENTS = "security.events"
    COMPLIANCE_LOGS = "compliance.logs"
    
    # Task Management
    TASK_QUEUE = "tasks.general"
    PRIORITY_TASKS = "tasks.priority"
    BACKGROUND_JOBS = "jobs.background"
    
    # Dead Letter and Error Handling
    DEAD_LETTER = "dlx.messages"
    ERROR_QUEUE = "errors.processing"
    RETRY_QUEUE = "retry.messages"


class DeliveryMode(Enum):
    """Message delivery modes"""
    TRANSIENT = 1  # Non-persistent
    PERSISTENT = 2  # Persistent


@dataclass
class ConnectionConfig:
    """RabbitMQ connection configuration"""
    hosts: List[str] = field(default_factory=lambda: ["localhost"])
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"
    
    # SSL Configuration
    ssl_enabled: bool = False
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None
    ssl_ca_cert_path: Optional[str] = None
    ssl_verify_hostname: bool = True
    
    # Connection Pool Settings
    max_connections: int = 20
    heartbeat: int = 600
    blocked_connection_timeout: int = 300
    connection_attempts: int = 5
    retry_delay: float = 2.0
    
    # High Availability
    cluster_enabled: bool = False
    load_balancing: bool = True
    failover_timeout: int = 30


@dataclass
class MessageMetadata:
    """Enhanced message metadata"""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    expiration: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    content_type: str = "application/json"
    content_encoding: str = "utf-8"
    delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT
    priority: MessagePriority = MessagePriority.NORMAL
    headers: Dict[str, Any] = field(default_factory=dict)
    
    # Custom metadata
    source_service: Optional[str] = None
    trace_id: Optional[str] = None
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None


@dataclass
class EnterpriseMessage:
    """Comprehensive enterprise message structure"""
    queue_type: QueueType
    message_type: str
    payload: Dict[str, Any]
    metadata: MessageMetadata = field(default_factory=MessageMetadata)
    
    # Retry and Error Handling
    retry_count: int = 0
    max_retries: int = 3
    retry_delay: int = 5
    error_details: Optional[str] = None
    
    # Routing
    routing_key: Optional[str] = None
    exchange_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "queue_type": self.queue_type.value,
            "message_type": self.message_type,
            "payload": self.payload,
            "metadata": asdict(self.metadata),
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "error_details": self.error_details,
            "routing_key": self.routing_key,
            "exchange_name": self.exchange_name
        }


class CircuitBreaker:
    """Circuit breaker pattern for fault tolerance"""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 60, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self.lock:
            if self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise Exception("Circuit breaker is OPEN")
            
            try:
                result = func(*args, **kwargs)
                if self.state == 'HALF_OPEN':
                    self.state = 'CLOSED'
                    self.failure_count = 0
                return result
                
            except self.expected_exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = 'OPEN'
                
                raise e


class ConnectionPool:
    """Connection pool for RabbitMQ connections"""
    
    def __init__(self, config: ConnectionConfig, logger):
        self.config = config
        self.logger = logger
        self.connections = []
        self.available_connections = []
        self.lock = threading.Lock()
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        for _ in range(self.config.max_connections):
            try:
                conn = self._create_connection()
                self.connections.append(conn)
                self.available_connections.append(conn)
            except Exception as e:
                self.logger.error(f"Failed to create pooled connection: {e}")
    
    def _create_connection(self) -> BlockingConnection:
        """Create a new RabbitMQ connection"""
        parameters = []
        
        for host in self.config.hosts:
            params = pika.ConnectionParameters(
                host=host,
                port=self.config.port,
                virtual_host=self.config.virtual_host,
                credentials=pika.PlainCredentials(
                    self.config.username, 
                    self.config.password
                ),
                heartbeat=self.config.heartbeat,
                blocked_connection_timeout=self.config.blocked_connection_timeout,
                connection_attempts=self.config.connection_attempts,
                retry_delay=self.config.retry_delay,
                ssl_options=self._get_ssl_options() if self.config.ssl_enabled else None
            )
            parameters.append(params)
        
        if self.config.cluster_enabled and len(parameters) > 1:
            return pika.BlockingConnection(parameters)
        else:
            return pika.BlockingConnection(parameters[0])
    
    def _get_ssl_options(self) -> Dict[str, Any]:
        """Get SSL options for secure connections"""
        ssl_options = {}
        
        if self.config.ssl_cert_path and self.config.ssl_key_path:
            ssl_options['certfile'] = self.config.ssl_cert_path
            ssl_options['keyfile'] = self.config.ssl_key_path
        
        if self.config.ssl_ca_cert_path:
            ssl_options['ca_certs'] = self.config.ssl_ca_cert_path
            ssl_options['cert_reqs'] = ssl.CERT_REQUIRED
        
        ssl_options['ssl_version'] = ssl.PROTOCOL_TLS
        
        if not self.config.ssl_verify_hostname:
            ssl_options['check_hostname'] = False
        
        return ssl_options
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool (context manager)"""
        connection = None
        try:
            with self.lock:
                if self.available_connections:
                    connection = self.available_connections.pop()
                else:
                    # Create new connection if pool is exhausted
                    connection = self._create_connection()
            
            # Test connection
            if connection.is_closed:
                connection = self._create_connection()
            
            yield connection
            
        except Exception as e:
            self.logger.error(f"Connection pool error: {e}")
            if connection:
                try:
                    connection.close()
                except:
                    pass
            raise
        finally:
            if connection and not connection.is_closed:
                with self.lock:
                    self.available_connections.append(connection)
    
    def close_all(self):
        """Close all connections in pool"""
        with self.lock:
            for conn in self.connections:
                try:
                    if not conn.is_closed:
                        conn.close()
                except:
                    pass
            self.connections.clear()
            self.available_connections.clear()


class EnterpriseRabbitMQManager:
    """
    Enterprise-grade RabbitMQ manager with high availability, 
    advanced messaging patterns, and comprehensive monitoring
    """
    
    def __init__(self, config: Optional[ConnectionConfig] = None):
        self.config = config or self._load_config()
        self.logger = get_logger(__name__)
        
        # Connection management
        self.connection_pool = ConnectionPool(self.config, self.logger)
        self.async_connection: Optional[AsyncConnection] = None
        
        # Circuit breakers for fault tolerance
        self.publish_breaker = CircuitBreaker(failure_threshold=5, reset_timeout=30)
        self.consume_breaker = CircuitBreaker(failure_threshold=3, reset_timeout=60)
        
        # Message tracking and deduplication
        self.message_cache: Set[str] = set()
        self.message_cache_ttl = 3600  # 1 hour
        self.last_cache_cleanup = time.time()
        
        # Monitoring and metrics
        self.metrics = {
            'messages_published': 0,
            'messages_consumed': 0,
            'messages_failed': 0,
            'connection_errors': 0,
            'circuit_breaker_trips': 0
        }
        
        # Declare topology on startup
        self._declare_topology()
        
        self.logger.info("Enterprise RabbitMQ Manager initialized")
    
    def _load_config(self) -> ConnectionConfig:
        """Load configuration from environment variables"""
        unified_config = get_unified_config()
        
        return ConnectionConfig(
            hosts=os.getenv("RABBITMQ_HOSTS", "localhost").split(","),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            username=os.getenv("RABBITMQ_USERNAME", "guest"),
            password=os.getenv("RABBITMQ_PASSWORD", "guest"),
            virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
            ssl_enabled=os.getenv("RABBITMQ_SSL_ENABLED", "false").lower() == "true",
            ssl_cert_path=os.getenv("RABBITMQ_SSL_CERT_PATH"),
            ssl_key_path=os.getenv("RABBITMQ_SSL_KEY_PATH"),
            ssl_ca_cert_path=os.getenv("RABBITMQ_SSL_CA_CERT_PATH"),
            cluster_enabled=os.getenv("RABBITMQ_CLUSTER_ENABLED", "false").lower() == "true",
            max_connections=int(os.getenv("RABBITMQ_MAX_CONNECTIONS", "20")),
            heartbeat=int(os.getenv("RABBITMQ_HEARTBEAT", "600")),
            connection_attempts=int(os.getenv("RABBITMQ_CONNECTION_ATTEMPTS", "5"))
        )
    
    def _declare_topology(self):
        """Declare exchanges, queues, and bindings"""
        try:
            with self.connection_pool.get_connection() as connection:
                channel = connection.channel()
                
                # Declare exchanges
                self._declare_exchanges(channel)
                
                # Declare queues
                self._declare_queues(channel)
                
                # Create bindings
                self._create_bindings(channel)
                
                self.logger.info("RabbitMQ topology declared successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to declare topology: {e}")
            raise
    
    def _declare_exchanges(self, channel):
        """Declare all required exchanges"""
        exchanges = {
            # Core ETL exchanges
            "etl.direct": ExchangeType.DIRECT,
            "etl.topic": ExchangeType.TOPIC,
            
            # ML and Analytics exchanges  
            "ml.direct": ExchangeType.DIRECT,
            "ml.events": ExchangeType.TOPIC,
            
            # Data Quality exchanges
            "data.quality": ExchangeType.TOPIC,
            "governance": ExchangeType.TOPIC,
            
            # Real-time exchanges
            "streaming": ExchangeType.TOPIC,
            "analytics": ExchangeType.FANOUT,
            
            # User and notifications
            "user.events": ExchangeType.TOPIC,
            "notifications": ExchangeType.TOPIC,
            
            # System exchanges
            "system.alerts": ExchangeType.FANOUT,
            "audit": ExchangeType.TOPIC,
            
            # Dead letter exchange
            "dlx": ExchangeType.DIRECT,
            
            # RPC exchange
            "rpc": ExchangeType.DIRECT
        }
        
        for exchange_name, exchange_type in exchanges.items():
            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type.value,
                durable=True,
                auto_delete=False
            )
    
    def _declare_queues(self, channel):
        """Declare all required queues with appropriate settings"""
        
        # Dead letter queue (must be declared first)
        channel.queue_declare(
            queue=QueueType.DEAD_LETTER.value,
            durable=True,
            arguments={
                'x-message-ttl': 86400000,  # 24 hours
                'x-max-length': 10000
            }
        )
        
        # Error and retry queues
        channel.queue_declare(
            queue=QueueType.ERROR_QUEUE.value,
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': QueueType.DEAD_LETTER.value,
                'x-message-ttl': 3600000  # 1 hour
            }
        )
        
        channel.queue_declare(
            queue=QueueType.RETRY_QUEUE.value,
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': QueueType.DEAD_LETTER.value,
                'x-message-ttl': 60000  # 1 minute retry delay
            }
        )
        
        # Standard queue arguments template
        standard_args = {
            'x-dead-letter-exchange': 'dlx',
            'x-dead-letter-routing-key': QueueType.DEAD_LETTER.value,
            'x-max-priority': 10
        }
        
        # High-priority queues
        priority_args = {**standard_args, 'x-max-priority': 10}
        
        # ETL queues
        for queue_type in [QueueType.ETL_BRONZE, QueueType.ETL_SILVER, QueueType.ETL_GOLD]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=priority_args
            )
        
        # ML queues  
        for queue_type in [QueueType.ML_TRAINING, QueueType.ML_INFERENCE, 
                          QueueType.ML_MODEL_DEPLOYMENT, QueueType.ML_FEATURE_PIPELINE]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=priority_args
            )
        
        # Data quality queues
        for queue_type in [QueueType.DATA_QUALITY_VALIDATION, QueueType.DATA_QUALITY_MONITORING,
                          QueueType.DATA_LINEAGE, QueueType.DATA_CATALOG]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=standard_args
            )
        
        # Real-time analytics queues
        realtime_args = {**standard_args, 'x-message-ttl': 300000}  # 5 minute TTL
        for queue_type in [QueueType.STREAMING_EVENTS, QueueType.REALTIME_ANALYTICS, 
                          QueueType.DASHBOARD_UPDATES]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=realtime_args
            )
        
        # User and session queues
        for queue_type in [QueueType.USER_EVENTS, QueueType.SESSION_MANAGEMENT, 
                          QueueType.USER_ACTIVITY]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=standard_args
            )
        
        # Notification queues
        for queue_type in [QueueType.SYSTEM_ALERTS, QueueType.DATA_ALERTS, 
                          QueueType.USER_NOTIFICATIONS, QueueType.EMAIL_NOTIFICATIONS]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=priority_args
            )
        
        # Audit and logging queues
        audit_args = {**standard_args, 'x-max-length': 50000}  # Large capacity
        for queue_type in [QueueType.AUDIT_LOGS, QueueType.SECURITY_EVENTS, 
                          QueueType.COMPLIANCE_LOGS]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=audit_args
            )
        
        # Task queues
        for queue_type in [QueueType.TASK_QUEUE, QueueType.PRIORITY_TASKS, 
                          QueueType.BACKGROUND_JOBS]:
            channel.queue_declare(
                queue=queue_type.value,
                durable=True,
                arguments=priority_args
            )
    
    def _create_bindings(self, channel):
        """Create exchange-queue bindings"""
        bindings = [
            # ETL bindings
            ("etl.direct", QueueType.ETL_BRONZE.value, "bronze"),
            ("etl.direct", QueueType.ETL_SILVER.value, "silver"),
            ("etl.direct", QueueType.ETL_GOLD.value, "gold"),
            ("etl.topic", QueueType.ETL_BRONZE.value, "etl.bronze.*"),
            ("etl.topic", QueueType.ETL_SILVER.value, "etl.silver.*"),
            ("etl.topic", QueueType.ETL_GOLD.value, "etl.gold.*"),
            
            # ML bindings
            ("ml.direct", QueueType.ML_TRAINING.value, "training"),
            ("ml.direct", QueueType.ML_INFERENCE.value, "inference"),
            ("ml.direct", QueueType.ML_MODEL_DEPLOYMENT.value, "deployment"),
            ("ml.events", QueueType.ML_FEATURE_PIPELINE.value, "ml.features.*"),
            
            # Data quality bindings
            ("data.quality", QueueType.DATA_QUALITY_VALIDATION.value, "dq.validation.*"),
            ("data.quality", QueueType.DATA_QUALITY_MONITORING.value, "dq.monitoring.*"),
            ("governance", QueueType.DATA_LINEAGE.value, "governance.lineage.*"),
            ("governance", QueueType.DATA_CATALOG.value, "governance.catalog.*"),
            
            # Streaming bindings
            ("streaming", QueueType.STREAMING_EVENTS.value, "streaming.*"),
            ("analytics", QueueType.REALTIME_ANALYTICS.value, ""),
            ("analytics", QueueType.DASHBOARD_UPDATES.value, ""),
            
            # User bindings
            ("user.events", QueueType.USER_EVENTS.value, "user.*"),
            ("user.events", QueueType.SESSION_MANAGEMENT.value, "session.*"),
            ("user.events", QueueType.USER_ACTIVITY.value, "activity.*"),
            
            # Notification bindings
            ("notifications", QueueType.SYSTEM_ALERTS.value, "alert.system.*"),
            ("notifications", QueueType.DATA_ALERTS.value, "alert.data.*"),
            ("notifications", QueueType.USER_NOTIFICATIONS.value, "notify.user.*"),
            ("notifications", QueueType.EMAIL_NOTIFICATIONS.value, "notify.email.*"),
            
            # System bindings
            ("system.alerts", QueueType.SYSTEM_ALERTS.value, ""),
            ("audit", QueueType.AUDIT_LOGS.value, "audit.*"),
            ("audit", QueueType.SECURITY_EVENTS.value, "security.*"),
            ("audit", QueueType.COMPLIANCE_LOGS.value, "compliance.*"),
            
            # Task bindings
            ("rpc", QueueType.TASK_QUEUE.value, "task"),
            ("rpc", QueueType.PRIORITY_TASKS.value, "priority"),
            ("rpc", QueueType.BACKGROUND_JOBS.value, "background"),
            
            # Dead letter binding
            ("dlx", QueueType.DEAD_LETTER.value, QueueType.DEAD_LETTER.value),
            ("dlx", QueueType.ERROR_QUEUE.value, QueueType.ERROR_QUEUE.value),
            ("dlx", QueueType.RETRY_QUEUE.value, QueueType.RETRY_QUEUE.value)
        ]
        
        for exchange, queue, routing_key in bindings:
            channel.queue_bind(
                exchange=exchange,
                queue=queue,
                routing_key=routing_key
            )
    
    async def initialize_async(self) -> bool:
        """Initialize async connection for high-performance operations"""
        try:
            if self.async_connection and not self.async_connection.is_closed:
                return True
            
            # Build connection URL
            host = self.config.hosts[0]  # Use first host for async
            connection_url = f"amqp://{self.config.username}:{self.config.password}@{host}:{self.config.port}{self.config.virtual_host}"
            
            # SSL options
            ssl_options = None
            if self.config.ssl_enabled:
                ssl_context = ssl.create_default_context()
                if self.config.ssl_cert_path and self.config.ssl_key_path:
                    ssl_context.load_cert_chain(self.config.ssl_cert_path, self.config.ssl_key_path)
                if self.config.ssl_ca_cert_path:
                    ssl_context.load_verify_locations(self.config.ssl_ca_cert_path)
                ssl_options = ssl_context
            
            # Create robust connection
            self.async_connection = await aio_pika.connect_robust(
                connection_url,
                ssl_options=ssl_options,
                heartbeat=self.config.heartbeat,
                blocked_connection_timeout=self.config.blocked_connection_timeout
            )
            
            self.logger.info("Async RabbitMQ connection initialized")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize async connection: {e}")
            return False
    
    def _is_duplicate_message(self, message_id: str) -> bool:
        """Check for duplicate messages using in-memory cache"""
        current_time = time.time()
        
        # Cleanup old entries periodically
        if current_time - self.last_cache_cleanup > 300:  # 5 minutes
            self._cleanup_message_cache()
            self.last_cache_cleanup = current_time
        
        if message_id in self.message_cache:
            return True
        
        self.message_cache.add(message_id)
        return False
    
    def _cleanup_message_cache(self):
        """Clean up old message cache entries"""
        # Simple cleanup - in production, use Redis with TTL
        if len(self.message_cache) > 10000:
            # Keep only recent 5000 entries
            self.message_cache = set(list(self.message_cache)[-5000:])
    
    def publish_message(
        self,
        message: EnterpriseMessage,
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None
    ) -> bool:
        """
        Publish enterprise message with circuit breaker protection
        """
        try:
            return self.publish_breaker.call(
                self._do_publish_message,
                message,
                exchange_name,
                routing_key
            )
        except Exception as e:
            self.metrics['circuit_breaker_trips'] += 1
            self.logger.error(f"Circuit breaker prevented message publishing: {e}")
            return False
    
    def _do_publish_message(
        self,
        message: EnterpriseMessage,
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None
    ) -> bool:
        """Internal message publishing implementation"""
        try:
            # Check for duplicate messages
            if self._is_duplicate_message(message.metadata.message_id):
                self.logger.debug(f"Duplicate message detected: {message.metadata.message_id}")
                return True
            
            with self.connection_pool.get_connection() as connection:
                channel = connection.channel()
                
                # Determine exchange and routing key
                if not exchange_name:
                    exchange_name = self._get_default_exchange(message.queue_type)
                
                if not routing_key:
                    routing_key = self._get_default_routing_key(message.queue_type)
                
                # Prepare message properties
                properties = pika.BasicProperties(
                    message_id=message.metadata.message_id,
                    correlation_id=message.metadata.correlation_id,
                    reply_to=message.metadata.reply_to,
                    expiration=str(message.metadata.expiration * 1000) if message.metadata.expiration else None,
                    timestamp=int(message.metadata.timestamp.timestamp()),
                    content_type=message.metadata.content_type,
                    content_encoding=message.metadata.content_encoding,
                    delivery_mode=message.metadata.delivery_mode.value,
                    priority=message.metadata.priority.value,
                    headers={
                        **message.metadata.headers,
                        'source_service': message.metadata.source_service,
                        'trace_id': message.metadata.trace_id,
                        'user_id': message.metadata.user_id,
                        'tenant_id': message.metadata.tenant_id,
                        'retry_count': message.retry_count,
                        'max_retries': message.max_retries
                    },
                    user_id=message.metadata.user_id
                )
                
                # Serialize message
                message_body = json.dumps(message.to_dict(), default=str)
                
                # Publish message
                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=message_body.encode(message.metadata.content_encoding),
                    properties=properties,
                    mandatory=True
                )
                
                self.metrics['messages_published'] += 1
                self.logger.debug(
                    f"Published message {message.metadata.message_id} to {exchange_name}/{routing_key}"
                )
                
                return True
                
        except Exception as e:
            self.metrics['messages_failed'] += 1
            self.logger.error(f"Failed to publish message: {e}")
            raise
    
    def _get_default_exchange(self, queue_type: QueueType) -> str:
        """Get default exchange for queue type"""
        exchange_mapping = {
            # ETL exchanges
            QueueType.ETL_BRONZE: "etl.direct",
            QueueType.ETL_SILVER: "etl.direct", 
            QueueType.ETL_GOLD: "etl.direct",
            
            # ML exchanges
            QueueType.ML_TRAINING: "ml.direct",
            QueueType.ML_INFERENCE: "ml.direct",
            QueueType.ML_MODEL_DEPLOYMENT: "ml.direct",
            QueueType.ML_FEATURE_PIPELINE: "ml.events",
            
            # Data quality exchanges
            QueueType.DATA_QUALITY_VALIDATION: "data.quality",
            QueueType.DATA_QUALITY_MONITORING: "data.quality",
            QueueType.DATA_LINEAGE: "governance",
            QueueType.DATA_CATALOG: "governance",
            
            # Streaming exchanges
            QueueType.STREAMING_EVENTS: "streaming",
            QueueType.REALTIME_ANALYTICS: "analytics",
            QueueType.DASHBOARD_UPDATES: "analytics",
            
            # User exchanges
            QueueType.USER_EVENTS: "user.events",
            QueueType.SESSION_MANAGEMENT: "user.events",
            QueueType.USER_ACTIVITY: "user.events",
            
            # Notification exchanges
            QueueType.SYSTEM_ALERTS: "notifications",
            QueueType.DATA_ALERTS: "notifications",
            QueueType.USER_NOTIFICATIONS: "notifications",
            QueueType.EMAIL_NOTIFICATIONS: "notifications",
            
            # Audit exchanges
            QueueType.AUDIT_LOGS: "audit",
            QueueType.SECURITY_EVENTS: "audit",
            QueueType.COMPLIANCE_LOGS: "audit",
            
            # Task exchanges
            QueueType.TASK_QUEUE: "rpc",
            QueueType.PRIORITY_TASKS: "rpc",
            QueueType.BACKGROUND_JOBS: "rpc"
        }
        
        return exchange_mapping.get(queue_type, "rpc")
    
    def _get_default_routing_key(self, queue_type: QueueType) -> str:
        """Get default routing key for queue type"""
        routing_mapping = {
            QueueType.ETL_BRONZE: "bronze",
            QueueType.ETL_SILVER: "silver",
            QueueType.ETL_GOLD: "gold",
            QueueType.ML_TRAINING: "training",
            QueueType.ML_INFERENCE: "inference",
            QueueType.ML_MODEL_DEPLOYMENT: "deployment",
            QueueType.TASK_QUEUE: "task",
            QueueType.PRIORITY_TASKS: "priority",
            QueueType.BACKGROUND_JOBS: "background"
        }
        
        return routing_mapping.get(queue_type, queue_type.value)
    
    async def publish_message_async(
        self,
        message: EnterpriseMessage,
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None
    ) -> bool:
        """Async message publishing for high performance"""
        try:
            if not await self.initialize_async():
                return False
            
            # Check for duplicate messages
            if self._is_duplicate_message(message.metadata.message_id):
                self.logger.debug(f"Duplicate message detected: {message.metadata.message_id}")
                return True
            
            channel = await self.async_connection.channel()
            
            # Determine exchange and routing key
            if not exchange_name:
                exchange_name = self._get_default_exchange(message.queue_type)
            
            if not routing_key:
                routing_key = self._get_default_routing_key(message.queue_type)
            
            exchange = await channel.get_exchange(exchange_name)
            
            # Create async message
            async_message = aio_pika.Message(
                json.dumps(message.to_dict(), default=str).encode(message.metadata.content_encoding),
                message_id=message.metadata.message_id,
                correlation_id=message.metadata.correlation_id,
                reply_to=message.metadata.reply_to,
                expiration=timedelta(seconds=message.metadata.expiration) if message.metadata.expiration else None,
                timestamp=message.metadata.timestamp,
                content_type=message.metadata.content_type,
                content_encoding=message.metadata.content_encoding,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT if message.metadata.delivery_mode == DeliveryMode.PERSISTENT else aio_pika.DeliveryMode.NOT_PERSISTENT,
                priority=message.metadata.priority.value,
                headers={
                    **message.metadata.headers,
                    'source_service': message.metadata.source_service,
                    'trace_id': message.metadata.trace_id,
                    'user_id': message.metadata.user_id,
                    'tenant_id': message.metadata.tenant_id,
                    'retry_count': message.retry_count,
                    'max_retries': message.max_retries
                },
                user_id=message.metadata.user_id
            )
            
            # Publish message
            await exchange.publish(async_message, routing_key=routing_key, mandatory=True)
            
            self.metrics['messages_published'] += 1
            self.logger.debug(
                f"Published async message {message.metadata.message_id} to {exchange_name}/{routing_key}"
            )
            
            await channel.close()
            return True
            
        except Exception as e:
            self.metrics['messages_failed'] += 1
            self.logger.error(f"Failed to publish async message: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get messaging metrics"""
        return {
            **self.metrics,
            'connection_pool_size': len(self.connection_pool.connections),
            'available_connections': len(self.connection_pool.available_connections),
            'message_cache_size': len(self.message_cache),
            'circuit_breaker_state': {
                'publish_state': self.publish_breaker.state,
                'consume_state': self.consume_breaker.state
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        try:
            start_time = time.time()
            
            # Test connection
            with self.connection_pool.get_connection() as connection:
                channel = connection.channel()
                
                # Test queue declaration
                test_queue = f"health_check_{uuid.uuid4().hex[:8]}"
                channel.queue_declare(queue=test_queue, auto_delete=True)
                
                # Test message publish/consume
                test_message = EnterpriseMessage(
                    queue_type=QueueType.TASK_QUEUE,
                    message_type="health_check",
                    payload={"test": True, "timestamp": datetime.utcnow().isoformat()}
                )
                
                publish_success = self.publish_message(test_message)
                
                # Clean up test queue
                channel.queue_delete(queue=test_queue)
                
                response_time = (time.time() - start_time) * 1000
                
                return {
                    "status": "healthy" if publish_success else "degraded",
                    "response_time_ms": round(response_time, 2),
                    "checks": {
                        "connection": True,
                        "publish": publish_success,
                        "queue_operations": True
                    },
                    "metrics": self.get_metrics(),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
        except Exception as e:
            self.metrics['connection_errors'] += 1
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def close(self):
        """Close all connections and clean up resources"""
        try:
            self.connection_pool.close_all()
            if self.async_connection:
                asyncio.create_task(self.async_connection.close())
            self.logger.info("Enterprise RabbitMQ Manager closed")
        except Exception as e:
            self.logger.error(f"Error closing RabbitMQ manager: {e}")


# Global manager instance
_enterprise_rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None


def get_rabbitmq_manager() -> EnterpriseRabbitMQManager:
    """Get or create global RabbitMQ manager instance"""
    global _enterprise_rabbitmq_manager
    if _enterprise_rabbitmq_manager is None:
        _enterprise_rabbitmq_manager = EnterpriseRabbitMQManager()
    return _enterprise_rabbitmq_manager


def set_rabbitmq_manager(manager: EnterpriseRabbitMQManager):
    """Set global RabbitMQ manager instance"""
    global _enterprise_rabbitmq_manager
    _enterprise_rabbitmq_manager = manager