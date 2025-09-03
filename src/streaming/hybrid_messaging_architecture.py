"""
Hybrid Messaging Architecture Implementation
Combines RabbitMQ for commands and Kafka for events
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import pika

# Note: Kafka imports would be added here for full implementation
# from confluent_kafka import Consumer, Producer
from src.core.logging import get_logger

# Simplified metrics collector to avoid complex dependencies
def get_metrics_collector():
    """Simplified metrics collector"""
    return None


class MessageType(Enum):
    """Types of messages in hybrid architecture"""
    COMMAND = "command"
    EVENT = "event"
    QUERY = "query"
    REPLY = "reply"


class MessagePriority(Enum):
    """Message priority levels"""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


@dataclass
class RabbitMQConfig:
    """RabbitMQ configuration"""
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"

    # Connection settings
    connection_attempts: int = 3
    retry_delay: int = 5
    socket_timeout: int = 10
    heartbeat: int = 600
    blocked_connection_timeout: int = 300

    # Channel settings
    max_channels: int = 100
    confirm_delivery: bool = True

    # Queue settings
    queue_durable: bool = True
    queue_auto_delete: bool = False
    queue_exclusive: bool = False

    # Message settings
    message_ttl: int | None = None  # milliseconds
    max_priority: int = 10
    enable_dead_letter: bool = True
    dead_letter_exchange: str = "dlx"

    # High availability
    enable_ha: bool = False
    ha_policy: str = "all"


@dataclass
class HybridMessage:
    """Unified message format for hybrid messaging"""
    message_id: str
    message_type: MessageType
    routing_key: str
    payload: dict[str, Any]
    timestamp: datetime
    headers: dict[str, Any] = field(default_factory=dict)

    # Message metadata
    priority: MessagePriority = MessagePriority.NORMAL
    correlation_id: str | None = None
    reply_to: str | None = None
    expiration: int | None = None  # TTL in milliseconds

    # Routing information
    exchange: str = ""
    queue: str | None = None

    # Tracing
    trace_id: str | None = None
    span_id: str | None = None

    def __post_init__(self):
        if not self.message_id:
            self.message_id = str(uuid.uuid4())

        if not self.correlation_id:
            self.correlation_id = str(uuid.uuid4())

        if not self.span_id:
            self.span_id = str(uuid.uuid4())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary"""
        return {
            "message_id": self.message_id,
            "message_type": self.message_type.value,
            "routing_key": self.routing_key,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "headers": self.headers,
            "priority": self.priority.value,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "expiration": self.expiration,
            "exchange": self.exchange,
            "queue": self.queue,
            "trace_id": self.trace_id,
            "span_id": self.span_id
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HybridMessage:
        """Create from dictionary"""
        timestamp_str = data.get('timestamp')
        timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.now()

        return cls(
            message_id=data.get("message_id", str(uuid.uuid4())),
            message_type=MessageType(data.get("message_type", "command")),
            routing_key=data.get("routing_key", ""),
            payload=data.get("payload", {}),
            timestamp=timestamp,
            headers=data.get("headers", {}),
            priority=MessagePriority(data.get("priority", 3)),
            correlation_id=data.get("correlation_id"),
            reply_to=data.get("reply_to"),
            expiration=data.get("expiration"),
            exchange=data.get("exchange", ""),
            queue=data.get("queue"),
            trace_id=data.get("trace_id"),
            span_id=data.get("span_id")
        )


class RabbitMQManager:
    """Enhanced RabbitMQ manager for command processing"""

    def __init__(self, config: RabbitMQConfig):
        self.config = config
        self.logger = get_logger(__name__)
        self.metrics = get_metrics_collector()

        # Connections and channels
        self.connection: pika.BlockingConnection | None = None
        self.channel: pika.channel.Channel | None = None
        self.consumer_channels: dict[str, pika.channel.Channel] = {}
        self.consumer_threads: dict[str, threading.Thread] = {}

        # State management
        self.is_connected = False
        self.shutdown_event = threading.Event()

        # Metrics
        self.messages_published = 0
        self.messages_consumed = 0
        self.connection_errors = 0

    def _connect(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(
                self.config.username,
                self.config.password
            )

            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                credentials=credentials,
                connection_attempts=self.config.connection_attempts,
                retry_delay=self.config.retry_delay,
                socket_timeout=self.config.socket_timeout,
                heartbeat=self.config.heartbeat,
                blocked_connection_timeout=self.config.blocked_connection_timeout
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Setup exchanges and queues
            self._setup_exchanges_and_queues()

            # Enable publisher confirms if configured
            if self.config.confirm_delivery:
                self.channel.confirm_delivery()

            self.is_connected = True
            self.logger.info(f"Connected to RabbitMQ at {self.config.host}:{self.config.port}")

        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {e}")
            self.connection_errors += 1
            raise

    def _setup_exchanges_and_queues(self):
        """Setup exchanges and queues"""
        try:
            # Dead letter exchange
            if self.config.enable_dead_letter:
                self.channel.exchange_declare(
                    exchange=self.config.dead_letter_exchange,
                    exchange_type='direct',
                    durable=True
                )

                # Dead letter queue
                self.channel.queue_declare(
                    queue='dead_letters',
                    durable=True
                )

                self.channel.queue_bind(
                    exchange=self.config.dead_letter_exchange,
                    queue='dead_letters'
                )

            # Command exchanges
            exchanges = [
                ('commands', 'direct'),
                ('commands_topic', 'topic'),
                ('commands_fanout', 'fanout'),
                ('replies', 'direct')
            ]

            for exchange_name, exchange_type in exchanges:
                self.channel.exchange_declare(
                    exchange=exchange_name,
                    exchange_type=exchange_type,
                    durable=True
                )

            # Standard queues
            standard_queues = [
                'commands.high_priority',
                'commands.normal_priority',
                'commands.low_priority',
                'replies'
            ]

            for queue_name in standard_queues:
                queue_args = {
                    'x-max-priority': self.config.max_priority
                }

                if self.config.enable_dead_letter:
                    queue_args['x-dead-letter-exchange'] = self.config.dead_letter_exchange

                if self.config.message_ttl:
                    queue_args['x-message-ttl'] = self.config.message_ttl

                if self.config.enable_ha:
                    queue_args['x-ha-policy'] = self.config.ha_policy

                self.channel.queue_declare(
                    queue=queue_name,
                    durable=self.config.queue_durable,
                    auto_delete=self.config.queue_auto_delete,
                    arguments=queue_args
                )

            # Bind queues to exchanges
            bindings = [
                ('commands', 'commands.high_priority', 'high'),
                ('commands', 'commands.normal_priority', 'normal'),
                ('commands', 'commands.low_priority', 'low'),
                ('replies', 'replies', 'reply')
            ]

            for exchange, queue, routing_key in bindings:
                self.channel.queue_bind(
                    exchange=exchange,
                    queue=queue,
                    routing_key=routing_key
                )

            self.logger.info("RabbitMQ exchanges and queues setup complete")

        except Exception as e:
            self.logger.error(f"Failed to setup exchanges and queues: {e}")
            raise


# Factory functions
def create_rabbitmq_config(**kwargs) -> RabbitMQConfig:
    """Create RabbitMQ configuration"""
    return RabbitMQConfig(**kwargs)


# Example usage
if __name__ == "__main__":
    print("Hybrid Messaging Architecture - Clean Implementation")

    # Create configurations
    rabbitmq_config = create_rabbitmq_config(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        confirm_delivery=True,
        enable_dead_letter=True
    )

    print("✅ Configuration created successfully")
    print("Note: Full implementation would include Kafka integration")
