"""
Apache Kafka Manager for Real-time Data Streaming
Provides comprehensive Kafka integration for real-time data processing
"""
import json
import uuid
import asyncio
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union
import hashlib
import gzip
import avro.schema
import avro.io
import redis
import pika

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import ConfigResource, ConfigResourceType, KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.structs import TopicPartition
from confluent_kafka import Producer as ConfluentProducer, Consumer as ConfluentConsumer
from confluent_kafka.avro import AvroProducer, AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.admin import AdminClient as ConfluentAdminClient

from core.config.unified_config import get_unified_config
from core.logging import get_logger


class MessageFormat(Enum):
    """Message format types"""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    PLAINTEXT = "plaintext"


class PartitionStrategy(Enum):
    """Partition assignment strategies"""
    ROUND_ROBIN = "round_robin"
    HASH_KEY = "hash_key"
    RANDOM = "random"
    CUSTOM = "custom"


class CompressionType(Enum):
    """Compression types for Kafka messages"""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class DeliveryGuarantee(Enum):
    """Message delivery guarantees"""
    AT_MOST_ONCE = "at_most_once"      # acks=0
    AT_LEAST_ONCE = "at_least_once"    # acks=1
    EXACTLY_ONCE = "exactly_once"      # acks=all, idempotent=true


class ConsumerStrategy(Enum):
    """Consumer processing strategies"""
    LATEST = "latest"
    EARLIEST = "earliest"
    SPECIFIC_OFFSET = "specific_offset"
    TIMESTAMP = "timestamp"


class StreamingTopic(Enum):
    """Predefined Kafka topics"""
    RETAIL_TRANSACTIONS = "retail-transactions"
    CUSTOMER_EVENTS = "customer-events"
    PRODUCT_UPDATES = "product-updates"
    SYSTEM_EVENTS = "system-events"
    DATA_QUALITY_ALERTS = "data-quality-alerts"
    ETL_PROGRESS = "etl-progress"
    NOTIFICATIONS = "notifications"
    METRICS = "metrics"
    CACHE_EVENTS = "cache-events"
    STATE_CHANGES = "state-changes"
    RATE_LIMIT_EVENTS = "rate-limit-events"
    TASK_EVENTS = "task-events"


@dataclass
class StreamingMessage:
    """Streaming message structure"""
    message_id: str
    topic: str
    key: str | None
    payload: dict[str, Any]
    timestamp: datetime
    headers: dict[str, str]
    schema_version: str = "1.0"
    source: str = "kafka_manager"

    def __post_init__(self):
        if not self.message_id:
            self.message_id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class AdvancedProducerConfig:
    """Advanced Kafka producer configuration for enterprise use"""
    bootstrap_servers: List[str]
    
    # Reliability settings
    acks: str = "all"  # all, 1, 0
    retries: int = int(1e9)  # Retry essentially forever
    retry_backoff_ms: int = 100
    delivery_timeout_ms: int = 300000  # 5 minutes
    request_timeout_ms: int = 30000
    
    # Performance settings
    batch_size: int = 65536  # 64KB for better throughput
    linger_ms: int = 10  # Wait up to 10ms to batch
    buffer_memory: int = 134217728  # 128MB
    max_in_flight_requests_per_connection: int = 1  # For ordering guarantee
    
    # Compression settings
    compression_type: CompressionType = CompressionType.ZSTD
    
    # Message settings
    max_request_size: int = 10485760  # 10MB
    send_buffer_bytes: int = 131072  # 128KB
    receive_buffer_bytes: int = 65536  # 64KB
    
    # Idempotent producer settings
    enable_idempotence: bool = True
    transactional_id: Optional[str] = None
    
    # Security settings
    security_protocol: str = "PLAINTEXT"
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    
    # Schema registry settings
    schema_registry_url: Optional[str] = None
    schema_registry_auth: Optional[tuple] = None
    
    # Custom partitioner
    partitioner: Optional[Callable] = None
    partition_strategy: PartitionStrategy = PartitionStrategy.HASH_KEY
    
    # Monitoring
    enable_metrics: bool = True
    metrics_reporters: List[str] = field(default_factory=list)
    
    def to_kafka_config(self) -> Dict[str, Any]:
        """Convert to Kafka client configuration"""
        config = {
            'bootstrap.servers': ','.join(self.bootstrap_servers),
            'acks': self.acks,
            'retries': self.retries,
            'retry.backoff.ms': self.retry_backoff_ms,
            'delivery.timeout.ms': self.delivery_timeout_ms,
            'request.timeout.ms': self.request_timeout_ms,
            'batch.size': self.batch_size,
            'linger.ms': self.linger_ms,
            'buffer.memory': self.buffer_memory,
            'max.in.flight.requests.per.connection': self.max_in_flight_requests_per_connection,
            'compression.type': self.compression_type.value,
            'max.request.size': self.max_request_size,
            'send.buffer.bytes': self.send_buffer_bytes,
            'receive.buffer.bytes': self.receive_buffer_bytes,
            'enable.idempotence': self.enable_idempotence,
            'security.protocol': self.security_protocol
        }
        
        if self.transactional_id:
            config['transactional.id'] = self.transactional_id
            
        if self.ssl_cafile:
            config.update({
                'ssl.ca.location': self.ssl_cafile,
                'ssl.certificate.location': self.ssl_certfile,
                'ssl.key.location': self.ssl_keyfile
            })
            
        if self.sasl_mechanism:
            config.update({
                'sasl.mechanism': self.sasl_mechanism,
                'sasl.username': self.sasl_username,
                'sasl.password': self.sasl_password
            })
            
        return config


@dataclass
class AdvancedConsumerConfig:
    """Advanced Kafka consumer configuration for enterprise use"""
    bootstrap_servers: List[str]
    group_id: str
    
    # Offset management
    auto_offset_reset: ConsumerStrategy = ConsumerStrategy.EARLIEST
    enable_auto_commit: bool = False  # Manual commit for better control
    auto_commit_interval_ms: int = 5000
    
    # Performance settings  
    max_poll_records: int = 1000
    max_poll_interval_ms: int = 300000  # 5 minutes
    fetch_min_bytes: int = 1024  # 1KB
    fetch_max_wait_ms: int = 500
    max_partition_fetch_bytes: int = 10485760  # 10MB
    
    # Session management
    session_timeout_ms: int = 30000  # 30 seconds
    heartbeat_interval_ms: int = 10000  # 10 seconds
    
    # Network settings
    request_timeout_ms: int = 30000
    connections_max_idle_ms: int = 300000  # 5 minutes
    
    # Security settings  
    security_protocol: str = "PLAINTEXT"
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    
    # Consumer group settings
    partition_assignment_strategy: str = "range"  # range, roundrobin, sticky, cooperative-sticky
    
    # Schema registry settings
    schema_registry_url: Optional[str] = None
    schema_registry_auth: Optional[tuple] = None
    
    # Processing settings
    enable_exactly_once: bool = False
    isolation_level: str = "read_uncommitted"  # read_committed, read_uncommitted
    
    # Monitoring
    enable_metrics: bool = True
    metrics_reporters: List[str] = field(default_factory=list)
    
    def to_kafka_config(self) -> Dict[str, Any]:
        """Convert to Kafka client configuration"""
        config = {
            'bootstrap.servers': ','.join(self.bootstrap_servers),
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset.value,
            'enable.auto.commit': self.enable_auto_commit,
            'auto.commit.interval.ms': self.auto_commit_interval_ms,
            'max.poll.records': self.max_poll_records,
            'max.poll.interval.ms': self.max_poll_interval_ms,
            'fetch.min.bytes': self.fetch_min_bytes,
            'fetch.max.wait.ms': self.fetch_max_wait_ms,
            'max.partition.fetch.bytes': self.max_partition_fetch_bytes,
            'session.timeout.ms': self.session_timeout_ms,
            'heartbeat.interval.ms': self.heartbeat_interval_ms,
            'request.timeout.ms': self.request_timeout_ms,
            'connections.max.idle.ms': self.connections_max_idle_ms,
            'security.protocol': self.security_protocol,
            'partition.assignment.strategy': [self.partition_assignment_strategy],
            'isolation.level': self.isolation_level
        }
        
        if self.ssl_cafile:
            config.update({
                'ssl.ca.location': self.ssl_cafile,
                'ssl.certificate.location': self.ssl_certfile,
                'ssl.key.location': self.ssl_keyfile
            })
            
        if self.sasl_mechanism:
            config.update({
                'sasl.mechanism': self.sasl_mechanism,
                'sasl.username': self.sasl_username,
                'sasl.password': self.sasl_password
            })
            
        return config


class KafkaManager:
    """
    Comprehensive Kafka manager for real-time data streaming
    Supports producers, consumers, schema management, and monitoring
    """

    def __init__(self):
        self.config = get_unified_config()
        self.logger = get_logger(__name__)

        # Kafka connection settings
        self.bootstrap_servers = self._get_bootstrap_servers()

        # Clients
        self.producer: KafkaProducer | None = None
        self.admin_client: KafkaAdminClient | None = None
        self.consumers: dict[str, KafkaConsumer] = {}

        # Schema registry (simplified)
        self.schemas: dict[str, dict[str, Any]] = {}

        # Metrics tracking
        self.metrics = {
            "messages_produced": 0,
            "messages_consumed": 0,
            "errors": 0,
            "topics_created": 0
        }

        # Initialize admin client
        self._initialize_admin_client()

        # Create standard topics
        self._create_standard_topics()

        self.logger.info("KafkaManager initialized")

    def _get_bootstrap_servers(self) -> list[str]:
        """Get Kafka bootstrap servers from environment"""
        import os

        servers_str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        return [s.strip() for s in servers_str.split(",")]

    def _initialize_admin_client(self):
        """Initialize Kafka admin client"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=30000,
                api_version=(2, 0, 0)
            )
            self.logger.info("Kafka admin client initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka admin client: {str(e)}")

    def create_producer(self, config: ProducerConfig | None = None) -> KafkaProducer:
        """
        Create Kafka producer with optimized configuration
        
        Args:
            config: Optional producer configuration
            
        Returns:
            KafkaProducer instance
        """
        try:
            if not config:
                config = ProducerConfig(bootstrap_servers=self.bootstrap_servers)

            producer_config = {
                'bootstrap_servers': config.bootstrap_servers,
                'acks': config.acks,
                'retries': config.retries,
                'batch_size': config.batch_size,
                'linger_ms': config.linger_ms,
                'buffer_memory': config.buffer_memory,
                'compression_type': config.compression_type,
                'max_request_size': config.max_request_size,
                'security_protocol': config.security_protocol,
                'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None
            }

            self.producer = KafkaProducer(**producer_config)

            self.logger.info("Kafka producer created successfully")
            return self.producer

        except Exception as e:
            self.logger.error(f"Failed to create Kafka producer: {str(e)}")
            raise

    def create_consumer(
        self,
        topics: list[str],
        group_id: str,
        config: ConsumerConfig | None = None
    ) -> KafkaConsumer:
        """
        Create Kafka consumer for specified topics
        
        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            config: Optional consumer configuration
            
        Returns:
            KafkaConsumer instance
        """
        try:
            if not config:
                config = ConsumerConfig(
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=group_id
                )

            consumer_config = {
                'bootstrap_servers': config.bootstrap_servers,
                'group_id': config.group_id,
                'auto_offset_reset': config.auto_offset_reset,
                'enable_auto_commit': config.enable_auto_commit,
                'auto_commit_interval_ms': config.auto_commit_interval_ms,
                'max_poll_records': config.max_poll_records,
                'max_poll_interval_ms': config.max_poll_interval_ms,
                'session_timeout_ms': config.session_timeout_ms,
                'heartbeat_interval_ms': config.heartbeat_interval_ms,
                'security_protocol': config.security_protocol,
                'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
                'key_deserializer': lambda k: k.decode('utf-8') if k else None
            }

            consumer = KafkaConsumer(*topics, **consumer_config)
            self.consumers[group_id] = consumer

            self.logger.info(f"Kafka consumer created for topics {topics} with group {group_id}")
            return consumer

        except Exception as e:
            self.logger.error(f"Failed to create Kafka consumer: {str(e)}")
            raise

    def produce_message(
        self,
        topic: str | StreamingTopic,
        message: dict[str, Any] | StreamingMessage,
        key: str | None = None,
        partition: int | None = None,
        headers: dict[str, str] | None = None,
        callback: Callable | None = None
    ) -> bool:
        """
        Produce message to Kafka topic
        
        Args:
            topic: Topic name or StreamingTopic enum
            message: Message payload or StreamingMessage object
            key: Optional message key
            partition: Optional specific partition
            headers: Optional message headers
            callback: Optional callback function
            
        Returns:
            True if message sent successfully
        """
        try:
            if not self.producer:
                self.create_producer()

            # Handle topic type
            topic_name = topic.value if isinstance(topic, StreamingTopic) else topic

            # Handle message type
            if isinstance(message, StreamingMessage):
                payload = asdict(message)
                key = message.key or key
                headers = message.headers or headers or {}
            else:
                payload = message
                headers = headers or {}

            # Add default headers
            headers.update({
                'timestamp': str(int(datetime.now().timestamp() * 1000)),
                'source': 'kafka_manager',
                'version': '1.0'
            })

            # Convert headers to bytes
            headers_bytes = {k: v.encode('utf-8') for k, v in headers.items()}

            # Send message
            future = self.producer.send(
                topic_name,
                value=payload,
                key=key,
                partition=partition,
                headers=list(headers_bytes.items())
            )

            # Add callback if provided
            if callback:
                future.add_callback(callback)
                future.add_errback(lambda e: self.logger.error(f"Message send failed: {e}"))

            # Update metrics
            self.metrics["messages_produced"] += 1

            self.logger.debug(f"Message produced to topic {topic_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to produce message to {topic}: {str(e)}")
            self.metrics["errors"] += 1
            return False

    def consume_messages(
        self,
        topics: list[str | StreamingTopic],
        group_id: str,
        message_handler: Callable[[dict[str, Any]], None],
        max_messages: int | None = None,
        timeout_ms: int = 1000
    ):
        """
        Consume messages from Kafka topics
        
        Args:
            topics: List of topics to consume from
            group_id: Consumer group ID
            message_handler: Function to handle received messages
            max_messages: Maximum number of messages to consume
            timeout_ms: Consumer timeout in milliseconds
        """
        try:
            # Convert topics to strings
            topic_names = [
                topic.value if isinstance(topic, StreamingTopic) else topic
                for topic in topics
            ]

            # Create consumer
            consumer = self.create_consumer(topic_names, group_id)

            messages_consumed = 0

            self.logger.info(f"Starting message consumption from {topic_names}")

            try:
                while True:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=timeout_ms)

                    if not message_batch:
                        continue

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                # Process message
                                message_data = {
                                    'topic': message.topic,
                                    'partition': message.partition,
                                    'offset': message.offset,
                                    'key': message.key,
                                    'value': message.value,
                                    'headers': dict(message.headers or {}),
                                    'timestamp': message.timestamp
                                }

                                message_handler(message_data)

                                messages_consumed += 1
                                self.metrics["messages_consumed"] += 1

                                # Check max messages limit
                                if max_messages and messages_consumed >= max_messages:
                                    return

                            except Exception as e:
                                self.logger.error(f"Message processing failed: {str(e)}")
                                self.metrics["errors"] += 1

                    # Commit offsets
                    consumer.commit()

            except KeyboardInterrupt:
                self.logger.info("Consumer stopped by user")
            finally:
                consumer.close()

        except Exception as e:
            self.logger.error(f"Message consumption failed: {str(e)}")
            raise

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: dict[str, str] | None = None
    ) -> bool:
        """
        Create Kafka topic
        
        Args:
            topic_name: Name of the topic to create
            num_partitions: Number of partitions
            replication_factor: Replication factor
            config: Optional topic configuration
            
        Returns:
            True if topic created successfully
        """
        try:
            if not self.admin_client:
                self._initialize_admin_client()

            # Default topic configuration
            default_config = {
                'cleanup.policy': 'delete',
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                'segment.ms': str(24 * 60 * 60 * 1000),  # 1 day
                'compression.type': 'gzip'
            }

            if config:
                default_config.update(config)

            # Create topic
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=default_config
            )

            result = self.admin_client.create_topics([topic])

            # Wait for creation
            for topic_name, future in result.items():
                try:
                    future.result()
                    self.logger.info(f"Topic {topic_name} created successfully")
                    self.metrics["topics_created"] += 1
                    return True
                except TopicAlreadyExistsError:
                    self.logger.info(f"Topic {topic_name} already exists")
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to create topic {topic_name}: {str(e)}")
                    return False

        except Exception as e:
            self.logger.error(f"Topic creation failed: {str(e)}")
            return False

    def _create_standard_topics(self):
        """Create standard topics for the platform"""

        standard_topics = [
            {
                'name': StreamingTopic.RETAIL_TRANSACTIONS.value,
                'partitions': 3,
                'replication': 1,
                'config': {'retention.ms': str(30 * 24 * 60 * 60 * 1000)}  # 30 days
            },
            {
                'name': StreamingTopic.CUSTOMER_EVENTS.value,
                'partitions': 2,
                'replication': 1,
                'config': {'cleanup.policy': 'compact'}
            },
            {
                'name': StreamingTopic.PRODUCT_UPDATES.value,
                'partitions': 2,
                'replication': 1,
                'config': {'cleanup.policy': 'compact'}
            },
            {
                'name': StreamingTopic.SYSTEM_EVENTS.value,
                'partitions': 1,
                'replication': 1,
                'config': {'retention.ms': str(7 * 24 * 60 * 60 * 1000)}  # 7 days
            },
            {
                'name': StreamingTopic.DATA_QUALITY_ALERTS.value,
                'partitions': 1,
                'replication': 1,
                'config': {'retention.ms': str(14 * 24 * 60 * 60 * 1000)}  # 14 days
            },
            {
                'name': StreamingTopic.ETL_PROGRESS.value,
                'partitions': 1,
                'replication': 1,
                'config': {'retention.ms': str(3 * 24 * 60 * 60 * 1000)}  # 3 days
            },
            {
                'name': StreamingTopic.NOTIFICATIONS.value,
                'partitions': 1,
                'replication': 1,
                'config': {'retention.ms': str(24 * 60 * 60 * 1000)}  # 1 day
            },
            {
                'name': StreamingTopic.METRICS.value,
                'partitions': 2,
                'replication': 1,
                'config': {'retention.ms': str(7 * 24 * 60 * 60 * 1000)}  # 7 days
            },
            {
                'name': StreamingTopic.CACHE_EVENTS.value,
                'partitions': 2,
                'replication': 1,
                'config': {'retention.ms': str(24 * 60 * 60 * 1000)}  # 1 day
            },
            {
                'name': StreamingTopic.STATE_CHANGES.value,
                'partitions': 2,
                'replication': 1,
                'config': {'retention.ms': str(3 * 24 * 60 * 60 * 1000)}  # 3 days
            },
            {
                'name': StreamingTopic.RATE_LIMIT_EVENTS.value,
                'partitions': 1,
                'replication': 1,
                'config': {'retention.ms': str(6 * 60 * 60 * 1000)}  # 6 hours
            },
            {
                'name': StreamingTopic.TASK_EVENTS.value,
                'partitions': 2,
                'replication': 1,
                'config': {'retention.ms': str(24 * 60 * 60 * 1000)}  # 1 day
            }
        ]

        created_count = 0
        for topic_config in standard_topics:
            if self.create_topic(
                topic_name=topic_config['name'],
                num_partitions=topic_config['partitions'],
                replication_factor=topic_config['replication'],
                config=topic_config['config']
            ):
                created_count += 1

        self.logger.info(f"Created {created_count} standard topics")

    def list_topics(self) -> list[str]:
        """List all Kafka topics"""
        try:
            if not self.admin_client:
                self._initialize_admin_client()

            metadata = self.admin_client.describe_topics()
            return list(metadata.keys())

        except Exception as e:
            self.logger.error(f"Failed to list topics: {str(e)}")
            return []

    def get_topic_info(self, topic_name: str) -> dict[str, Any]:
        """Get topic information including partitions and configuration"""
        try:
            if not self.admin_client:
                self._initialize_admin_client()

            # Get topic metadata
            metadata = self.admin_client.describe_topics([topic_name])
            topic_metadata = metadata.get(topic_name)

            if not topic_metadata:
                return {"error": "Topic not found"}

            # Get topic configuration
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = self.admin_client.describe_configs([config_resource])
            topic_config = configs.get(config_resource, {})

            return {
                "name": topic_name,
                "partitions": len(topic_metadata.partitions),
                "replication_factor": len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
                "configuration": {k: v.value for k, v in topic_config.items()},
                "partition_info": [
                    {
                        "partition": p.partition,
                        "leader": p.leader,
                        "replicas": p.replicas,
                        "isr": p.isr
                    }
                    for p in topic_metadata.partitions
                ]
            }

        except Exception as e:
            self.logger.error(f"Failed to get topic info for {topic_name}: {str(e)}")
            return {"error": str(e)}

    def produce_retail_transaction(self, transaction_data: dict[str, Any]) -> bool:
        """Produce retail transaction event"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.RETAIL_TRANSACTIONS.value,
            key=transaction_data.get("invoice_no"),
            payload=transaction_data,
            timestamp=datetime.now(),
            headers={"event_type": "transaction", "version": "1.0"},
            source="retail_system"
        )

        return self.produce_message(
            topic=StreamingTopic.RETAIL_TRANSACTIONS,
            message=message
        )

    def produce_customer_event(self, customer_id: str, event_type: str, event_data: dict[str, Any]) -> bool:
        """Produce customer event"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.CUSTOMER_EVENTS.value,
            key=customer_id,
            payload={
                "customer_id": customer_id,
                "event_type": event_type,
                "event_data": event_data,
                "timestamp": datetime.now().isoformat()
            },
            timestamp=datetime.now(),
            headers={"event_type": event_type, "customer_id": customer_id},
            source="customer_system"
        )

        return self.produce_message(
            topic=StreamingTopic.CUSTOMER_EVENTS,
            message=message
        )

    def produce_data_quality_alert(self, alert_data: dict[str, Any]) -> bool:
        """Produce data quality alert"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.DATA_QUALITY_ALERTS.value,
            key=alert_data.get("component", "unknown"),
            payload=alert_data,
            timestamp=datetime.now(),
            headers={
                "alert_type": "data_quality",
                "severity": alert_data.get("severity", "medium")
            },
            source="data_quality_system"
        )

        return self.produce_message(
            topic=StreamingTopic.DATA_QUALITY_ALERTS,
            message=message
        )

    def produce_etl_progress(self, pipeline_id: str, stage: str, progress_data: dict[str, Any]) -> bool:
        """Produce ETL progress event"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.ETL_PROGRESS.value,
            key=pipeline_id,
            payload={
                "pipeline_id": pipeline_id,
                "stage": stage,
                "progress_data": progress_data,
                "timestamp": datetime.now().isoformat()
            },
            timestamp=datetime.now(),
            headers={"pipeline_id": pipeline_id, "stage": stage},
            source="etl_system"
        )

        return self.produce_message(
            topic=StreamingTopic.ETL_PROGRESS,
            message=message
        )

    def produce_cache_event(self, operation: str, key: str, hit: bool = False, metadata: dict[str, Any] | None = None) -> bool:
        """Produce cache operation event for analytics and monitoring"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.CACHE_EVENTS.value,
            key=key,
            payload={
                "operation": operation,
                "key": key,
                "hit": hit,
                "metadata": metadata or {},
                "timestamp": datetime.now().isoformat()
            },
            timestamp=datetime.now(),
            headers={"operation": operation, "hit": str(hit).lower()},
            source="cache_system"
        )

        return self.produce_message(
            topic=StreamingTopic.CACHE_EVENTS,
            message=message
        )

    def produce_state_change_event(self, component: str, key: str, operation: str, metadata: dict[str, Any] | None = None) -> bool:
        """Produce state change event for distributed state management"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.STATE_CHANGES.value,
            key=f"{component}:{key}",
            payload={
                "component": component,
                "key": key,
                "operation": operation,
                "metadata": metadata or {},
                "timestamp": datetime.now().isoformat()
            },
            timestamp=datetime.now(),
            headers={"component": component, "operation": operation},
            source="state_management_system"
        )

        return self.produce_message(
            topic=StreamingTopic.STATE_CHANGES,
            message=message
        )

    def produce_rate_limit_event(self, client_id: str, endpoint: str, allowed: bool, current_count: int, limit: int) -> bool:
        """Produce rate limiting event for monitoring and analytics"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.RATE_LIMIT_EVENTS.value,
            key=client_id,
            payload={
                "client_id": client_id,
                "endpoint": endpoint,
                "allowed": allowed,
                "current_count": current_count,
                "limit": limit,
                "timestamp": datetime.now().isoformat()
            },
            timestamp=datetime.now(),
            headers={
                "client_id": client_id,
                "endpoint": endpoint,
                "allowed": str(allowed).lower()
            },
            source="rate_limiter"
        )

        return self.produce_message(
            topic=StreamingTopic.RATE_LIMIT_EVENTS,
            message=message
        )

    def produce_task_event(self, task_id: str, task_name: str, status: str, metadata: dict[str, Any] | None = None) -> bool:
        """Produce task execution event for monitoring and analytics"""

        message = StreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=StreamingTopic.TASK_EVENTS.value,
            key=task_id,
            payload={
                "task_id": task_id,
                "task_name": task_name,
                "status": status,
                "metadata": metadata or {},
                "timestamp": datetime.now().isoformat()
            },
            timestamp=datetime.now(),
            headers={"task_id": task_id, "task_name": task_name, "status": status},
            source="task_system"
        )

        return self.produce_message(
            topic=StreamingTopic.TASK_EVENTS,
            message=message
        )

    def get_consumer_lag(self, group_id: str) -> dict[str, Any]:
        """Get consumer lag information"""
        try:
            if group_id not in self.consumers:
                return {"error": "Consumer group not found"}

            consumer = self.consumers[group_id]

            # Get current offsets
            committed_offsets = {}
            assigned_partitions = consumer.assignment()

            for partition in assigned_partitions:
                committed = consumer.committed(partition)
                if committed:
                    committed_offsets[str(partition)] = committed.offset

            # Get latest offsets
            latest_offsets = consumer.end_offsets(assigned_partitions)

            # Calculate lag
            lag_info = {}
            total_lag = 0

            for partition, latest_offset in latest_offsets.items():
                committed_offset = committed_offsets.get(str(partition), 0)
                lag = latest_offset - committed_offset
                total_lag += lag

                lag_info[str(partition)] = {
                    "committed_offset": committed_offset,
                    "latest_offset": latest_offset,
                    "lag": lag
                }

            return {
                "group_id": group_id,
                "total_lag": total_lag,
                "partition_lag": lag_info,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get consumer lag for {group_id}: {str(e)}")
            return {"error": str(e)}

    def get_metrics(self) -> dict[str, Any]:
        """Get Kafka manager metrics"""
        return {
            "kafka_metrics": self.metrics,
            "producer_active": self.producer is not None,
            "active_consumers": len(self.consumers),
            "bootstrap_servers": self.bootstrap_servers,
            "timestamp": datetime.now().isoformat()
        }

    def flush_producer(self):
        """Flush producer to ensure all messages are sent"""
        if self.producer:
            self.producer.flush()
            self.logger.info("Producer flushed")

    def close(self):
        """Close all Kafka connections"""
        try:
            # Close producer
            if self.producer:
                self.producer.flush()
                self.producer.close()
                self.logger.info("Kafka producer closed")

            # Close consumers
            for group_id, consumer in self.consumers.items():
                consumer.close()
                self.logger.info(f"Kafka consumer {group_id} closed")

            self.consumers.clear()

            # Close admin client
            if self.admin_client:
                self.admin_client.close()
                self.logger.info("Kafka admin client closed")

            self.logger.info("All Kafka connections closed")

        except Exception as e:
            self.logger.warning(f"Error closing Kafka connections: {str(e)}")


# Factory function
def create_kafka_manager() -> KafkaManager:
    """Create KafkaManager instance"""
    return KafkaManager()


# Example usage classes
class KafkaETLStreamer:
    """Example ETL streaming processor using Kafka"""

    def __init__(self):
        self.kafka = create_kafka_manager()
        self.logger = get_logger(__name__)

    def stream_etl_progress(self, pipeline_id: str):
        """Stream ETL progress updates"""
        stages = ["bronze", "silver", "gold"]

        for i, stage in enumerate(stages):
            progress_data = {
                "stage": stage,
                "progress_percentage": ((i + 1) / len(stages)) * 100,
                "records_processed": (i + 1) * 10000,
                "status": "completed" if i < len(stages) - 1 else "in_progress"
            }

            self.kafka.produce_etl_progress(pipeline_id, stage, progress_data)
            self.logger.info(f"Streamed progress for {stage} stage")

    def consume_transaction_stream(self):
        """Consume real-time transaction stream"""

        def handle_transaction(message_data):
            transaction = message_data['value']['payload']
            self.logger.info(f"Processing transaction: {transaction.get('invoice_no')}")

            # Process transaction in real-time
            # Could trigger immediate data quality checks, alerts, etc.

        self.kafka.consume_messages(
            topics=[StreamingTopic.RETAIL_TRANSACTIONS],
            group_id="transaction_processor",
            message_handler=handle_transaction
        )


# Testing and example usage
if __name__ == "__main__":
    import os

    # Set environment variables for testing
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    print("Testing Kafka Manager...")

    try:
        # Test basic functionality
        manager = create_kafka_manager()

        # Test producer creation
        producer = manager.create_producer()
        if producer:
            print("✅ Producer created successfully")

        # Test message production
        success = manager.produce_message(
            topic=StreamingTopic.SYSTEM_EVENTS,
            message={
                "event_type": "test",
                "data": {"test": True, "timestamp": datetime.now().isoformat()}
            },
            key="test_key"
        )

        if success:
            print("✅ Message produced successfully")

        # Test retail transaction
        transaction_data = {
            "invoice_no": "TEST001",
            "customer_id": "12345",
            "amount": 99.99,
            "timestamp": datetime.now().isoformat()
        }

        if manager.produce_retail_transaction(transaction_data):
            print("✅ Retail transaction streamed")

        # Test customer event
        if manager.produce_customer_event(
            "12345", "purchase", {"amount": 99.99, "items": 3}
        ):
            print("✅ Customer event streamed")

        # Test data quality alert
        alert_data = {
            "component": "bronze_layer",
            "severity": "medium",
            "message": "Data quality score below threshold",
            "quality_score": 0.75
        }

        if manager.produce_data_quality_alert(alert_data):
            print("✅ Data quality alert streamed")

        # Test topic listing
        topics = manager.list_topics()
        print(f"✅ Topics available: {len(topics)}")

        # Test metrics
        metrics = manager.get_metrics()
        print(f"✅ Metrics: {metrics['kafka_metrics']}")

        # Flush and close
        manager.flush_producer()
        manager.close()

        print("✅ Kafka Manager testing completed successfully!")

    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
