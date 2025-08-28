"""
Hybrid Messaging Architecture with Kafka and RabbitMQ Integration
Implements command-event segregation using RabbitMQ for commands and Kafka for events
"""
from __future__ import annotations

import json
import uuid
import asyncio
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union, Tuple
import concurrent.futures
import hashlib

import pika
import pika.exceptions
from confluent_kafka import Producer, Consumer, TopicPartition
import redis

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import EnhancedStreamingMessage
from src.streaming.event_sourcing_cache_integration import EventCache


class MessageType(Enum):
    """Types of messages in hybrid architecture"""
    COMMAND = "command"
    EVENT = "event" 
    QUERY = "query"
    REPLY = "reply"


class MessagePriority(Enum):
    """Message priority levels"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class RoutingStrategy(Enum):
    """Message routing strategies"""
    DIRECT = "direct"
    TOPIC = "topic"
    FANOUT = "fanout"
    HEADERS = "headers"


@dataclass
class RabbitMQConfig:
    """Configuration for RabbitMQ"""
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    username: str = "guest"
    password: str = "guest"
    
    # Connection settings
    connection_attempts: int = 3
    retry_delay: int = 5
    socket_timeout: int = 10
    connection_timeout: int = 30
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
    message_ttl: Optional[int] = None  # milliseconds
    max_priority: int = 10
    enable_dead_letter: bool = True
    dead_letter_exchange: str = "dlx"
    
    # High availability
    enable_ha: bool = False
    ha_policy: str = "all"  # all, exactly, nodes


@dataclass
class HybridMessage:
    """Unified message structure for hybrid architecture"""
    message_id: str
    message_type: MessageType
    routing_key: str
    payload: Dict[str, Any]
    timestamp: datetime
    headers: Dict[str, Any] = field(default_factory=dict)
    
    # Message metadata
    priority: MessagePriority = MessagePriority.NORMAL
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    expiration: Optional[int] = None  # TTL in milliseconds
    
    # Routing information
    exchange: str = ""
    queue: Optional[str] = None
    
    # Tracing
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    
    def __post_init__(self):
        if not self.message_id:
            self.message_id = str(uuid.uuid4())
        if not self.trace_id:
            self.trace_id = str(uuid.uuid4())
        if not self.span_id:
            self.span_id = str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
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
    def from_dict(cls, data: Dict[str, Any]) -> 'HybridMessage':
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
            priority=MessagePriority(data.get("priority", 2)),
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
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.consumer_channels: Dict[str, pika.channel.Channel] = {}
        
        # Message handlers
        self.message_handlers: Dict[str, Callable] = {}
        self.consumer_threads: Dict[str, threading.Thread] = {}
        
        # State management
        self.is_connected = False
        self.shutdown_event = threading.Event()
        
        # Performance tracking
        self.messages_published = 0
        self.messages_consumed = 0
        self.connection_errors = 0
        
        # Initialize connection
        self._connect()
        self._setup_exchanges_and_queues()
    
    def _connect(self):
        """Establish connection to RabbitMQ"""
        try:
            # Connection parameters
            credentials = pika.PlainCredentials(
                self.config.username,
                self.config.password
            )
            
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                virtual_host=self.config.virtual_host,
                credentials=credentials,
                connection_attempts=self.config.connection_attempts,
                retry_delay=self.config.retry_delay,
                socket_timeout=self.config.socket_timeout,
                heartbeat=self.config.heartbeat,
                blocked_connection_timeout=self.config.blocked_connection_timeout
            )
            
            # Create connection
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Enable delivery confirmations if configured
            if self.config.confirm_delivery:
                self.channel.confirm_delivery()
            
            self.is_connected = True
            self.logger.info(f\"Connected to RabbitMQ at {self.config.host}:{self.config.port}\")\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to connect to RabbitMQ: {e}\")\n            self.connection_errors += 1\n            raise\n    \n    def _setup_exchanges_and_queues(self):\n        \"\"\"Setup exchanges and queues\"\"\"\n        try:\n            # Dead letter exchange\n            if self.config.enable_dead_letter:\n                self.channel.exchange_declare(\n                    exchange=self.config.dead_letter_exchange,\n                    exchange_type='direct',\n                    durable=True\n                )\n                \n                # Dead letter queue\n                self.channel.queue_declare(\n                    queue='dead_letters',\n                    durable=True\n                )\n                \n                self.channel.queue_bind(\n                    exchange=self.config.dead_letter_exchange,\n                    queue='dead_letters'\n                )\n            \n            # Command exchanges\n            exchanges = [\n                ('commands', 'direct'),\n                ('commands_topic', 'topic'),\n                ('commands_fanout', 'fanout'),\n                ('replies', 'direct')\n            ]\n            \n            for exchange_name, exchange_type in exchanges:\n                self.channel.exchange_declare(\n                    exchange=exchange_name,\n                    exchange_type=exchange_type,\n                    durable=True\n                )\n            \n            # Standard queues\n            standard_queues = [\n                'commands.high_priority',\n                'commands.normal_priority', \n                'commands.low_priority',\n                'replies'\n            ]\n            \n            for queue_name in standard_queues:\n                queue_args = {\n                    'x-max-priority': self.config.max_priority\n                }\n                \n                if self.config.enable_dead_letter:\n                    queue_args['x-dead-letter-exchange'] = self.config.dead_letter_exchange\n                    \n                if self.config.message_ttl:\n                    queue_args['x-message-ttl'] = self.config.message_ttl\n                    \n                if self.config.enable_ha:\n                    queue_args['x-ha-policy'] = self.config.ha_policy\n                \n                self.channel.queue_declare(\n                    queue=queue_name,\n                    durable=self.config.queue_durable,\n                    auto_delete=self.config.queue_auto_delete,\n                    arguments=queue_args\n                )\n            \n            # Bind queues to exchanges\n            bindings = [\n                ('commands', 'commands.high_priority', 'high'),\n                ('commands', 'commands.normal_priority', 'normal'),\n                ('commands', 'commands.low_priority', 'low'),\n                ('replies', 'replies', 'reply')\n            ]\n            \n            for exchange, queue, routing_key in bindings:\n                self.channel.queue_bind(\n                    exchange=exchange,\n                    queue=queue,\n                    routing_key=routing_key\n                )\n            \n            self.logger.info(\"RabbitMQ exchanges and queues setup complete\")\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to setup exchanges and queues: {e}\")\n            raise\n    \n    def publish_command(self, message: HybridMessage) -> bool:\n        \"\"\"Publish command message to RabbitMQ\"\"\"\n        try:\n            if not self.is_connected:\n                self._connect()\n            \n            # Determine routing based on priority\n            if message.priority == MessagePriority.CRITICAL:\n                routing_key = 'high'\n            elif message.priority == MessagePriority.HIGH:\n                routing_key = 'high'\n            elif message.priority == MessagePriority.NORMAL:\n                routing_key = 'normal'\n            else:\n                routing_key = 'low'\n            \n            # Message properties\n            properties = pika.BasicProperties(\n                message_id=message.message_id,\n                correlation_id=message.correlation_id,\n                reply_to=message.reply_to,\n                timestamp=int(message.timestamp.timestamp()),\n                priority=message.priority.value,\n                expiration=str(message.expiration) if message.expiration else None,\n                headers={\n                    **message.headers,\n                    'trace_id': message.trace_id,\n                    'span_id': message.span_id,\n                    'message_type': message.message_type.value\n                },\n                delivery_mode=2  # Persistent\n            )\n            \n            # Publish message\n            success = self.channel.basic_publish(\n                exchange='commands',\n                routing_key=routing_key,\n                body=json.dumps(message.payload, default=str),\n                properties=properties,\n                mandatory=True\n            )\n            \n            if success:\n                self.messages_published += 1\n                \n                if self.metrics:\n                    self.metrics.increment_counter(\n                        \"rabbitmq_messages_published\",\n                        {\"routing_key\": routing_key, \"priority\": message.priority.value}\n                    )\n                \n                self.logger.debug(f\"Published command {message.message_id} with routing key {routing_key}\")\n                return True\n            else:\n                self.logger.error(f\"Failed to publish command {message.message_id}\")\n                return False\n                \n        except Exception as e:\n            self.logger.error(f\"Command publishing failed: {e}\")\n            return False\n    \n    def consume_commands(self, \n                       queue: str, \n                       handler: Callable[[HybridMessage], Optional[HybridMessage]],\n                       auto_ack: bool = False) -> str:\n        \"\"\"Start consuming commands from queue\"\"\"\n        try:\n            # Create dedicated channel for this consumer\n            consumer_channel = self.connection.channel()\n            consumer_channel.basic_qos(prefetch_count=10)  # Limit unacked messages\n            \n            self.consumer_channels[queue] = consumer_channel\n            \n            def message_callback(ch, method, properties, body):\n                try:\n                    # Parse message\n                    payload = json.loads(body.decode('utf-8'))\n                    \n                    # Create HybridMessage\n                    message = HybridMessage(\n                        message_id=properties.message_id or str(uuid.uuid4()),\n                        message_type=MessageType.COMMAND,\n                        routing_key=method.routing_key,\n                        payload=payload,\n                        timestamp=datetime.fromtimestamp(properties.timestamp) if properties.timestamp else datetime.now(),\n                        headers=properties.headers or {},\n                        priority=MessagePriority(properties.priority) if properties.priority else MessagePriority.NORMAL,\n                        correlation_id=properties.correlation_id,\n                        reply_to=properties.reply_to,\n                        trace_id=properties.headers.get('trace_id') if properties.headers else None,\n                        span_id=properties.headers.get('span_id') if properties.headers else None\n                    )\n                    \n                    # Process message\n                    start_time = time.time()\n                    result = handler(message)\n                    processing_time = time.time() - start_time\n                    \n                    # Send reply if requested and result provided\n                    if message.reply_to and result:\n                        self._send_reply(message, result)\n                    \n                    # Acknowledge message\n                    if not auto_ack:\n                        ch.basic_ack(delivery_tag=method.delivery_tag)\n                    \n                    self.messages_consumed += 1\n                    \n                    if self.metrics:\n                        self.metrics.increment_counter(\"rabbitmq_messages_consumed\")\n                        self.metrics.histogram(\"rabbitmq_message_processing_time\", processing_time)\n                    \n                except Exception as e:\n                    self.logger.error(f\"Message processing failed: {e}\")\n                    \n                    # Reject message and send to dead letter queue\n                    if not auto_ack:\n                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)\n            \n            # Start consuming\n            consumer_tag = consumer_channel.basic_consume(\n                queue=queue,\n                on_message_callback=message_callback,\n                auto_ack=auto_ack\n            )\n            \n            # Start consumer thread\n            consumer_thread = threading.Thread(\n                target=self._consume_loop,\n                args=(consumer_channel,),\n                name=f\"rabbitmq-consumer-{queue}\",\n                daemon=True\n            )\n            consumer_thread.start()\n            self.consumer_threads[queue] = consumer_thread\n            \n            self.logger.info(f\"Started consuming from queue {queue}\")\n            return consumer_tag\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to start consuming from {queue}: {e}\")\n            raise\n    \n    def _consume_loop(self, channel):\n        \"\"\"Consumer loop for processing messages\"\"\"\n        try:\n            while not self.shutdown_event.is_set():\n                channel.connection.process_data_events(time_limit=1)\n        except Exception as e:\n            self.logger.error(f\"Consumer loop error: {e}\")\n    \n    def _send_reply(self, original_message: HybridMessage, reply_message: HybridMessage):\n        \"\"\"Send reply message\"\"\"\n        try:\n            reply_message.correlation_id = original_message.correlation_id\n            reply_message.message_type = MessageType.REPLY\n            \n            properties = pika.BasicProperties(\n                message_id=reply_message.message_id,\n                correlation_id=reply_message.correlation_id,\n                timestamp=int(reply_message.timestamp.timestamp()),\n                headers={\n                    **reply_message.headers,\n                    'trace_id': original_message.trace_id,\n                    'original_message_id': original_message.message_id\n                }\n            )\n            \n            self.channel.basic_publish(\n                exchange='replies',\n                routing_key='reply',\n                body=json.dumps(reply_message.payload, default=str),\n                properties=properties\n            )\n            \n            self.logger.debug(f\"Sent reply for message {original_message.message_id}\")\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to send reply: {e}\")\n    \n    def stop_consuming(self, queue: str):\n        \"\"\"Stop consuming from specific queue\"\"\"\n        if queue in self.consumer_channels:\n            try:\n                channel = self.consumer_channels[queue]\n                channel.stop_consuming()\n                channel.close()\n                del self.consumer_channels[queue]\n                \n                if queue in self.consumer_threads:\n                    self.consumer_threads[queue].join(timeout=5)\n                    del self.consumer_threads[queue]\n                \n                self.logger.info(f\"Stopped consuming from queue {queue}\")\n                \n            except Exception as e:\n                self.logger.error(f\"Error stopping consumer for {queue}: {e}\")\n    \n    def get_queue_info(self, queue: str) -> Dict[str, Any]:\n        \"\"\"Get queue information\"\"\"\n        try:\n            method = self.channel.queue_declare(queue, passive=True)\n            return {\n                \"queue\": queue,\n                \"messages\": method.method.message_count,\n                \"consumers\": method.method.consumer_count\n            }\n        except Exception as e:\n            self.logger.error(f\"Failed to get queue info for {queue}: {e}\")\n            return {\"error\": str(e)}\n    \n    def close(self):\n        \"\"\"Close RabbitMQ connection\"\"\"\n        self.logger.info(\"Closing RabbitMQ connection\")\n        \n        # Signal shutdown\n        self.shutdown_event.set()\n        \n        # Stop all consumers\n        for queue in list(self.consumer_channels.keys()):\n            self.stop_consuming(queue)\n        \n        # Close channels\n        if self.channel and not self.channel.is_closed:\n            self.channel.close()\n        \n        for channel in self.consumer_channels.values():\n            if not channel.is_closed:\n                channel.close()\n        \n        # Close connection\n        if self.connection and not self.connection.is_closed:\n            self.connection.close()\n        \n        self.is_connected = False\n        self.logger.info(\"RabbitMQ connection closed\")\n    \n    def get_metrics(self) -> Dict[str, Any]:\n        \"\"\"Get RabbitMQ metrics\"\"\"\n        return {\n            \"messages_published\": self.messages_published,\n            \"messages_consumed\": self.messages_consumed,\n            \"connection_errors\": self.connection_errors,\n            \"active_consumers\": len(self.consumer_threads),\n            \"is_connected\": self.is_connected\n        }\n\n\nclass HybridMessagingOrchestrator:\n    \"\"\"Orchestrator for hybrid Kafka-RabbitMQ messaging\"\"\"\n    \n    def __init__(self,\n                 kafka_bootstrap_servers: List[str],\n                 rabbitmq_config: RabbitMQConfig,\n                 redis_client: Optional[redis.Redis] = None):\n        self.kafka_bootstrap_servers = kafka_bootstrap_servers\n        self.rabbitmq_config = rabbitmq_config\n        self.redis_client = redis_client\n        self.logger = get_logger(__name__)\n        self.metrics = get_metrics_collector()\n        \n        # Messaging components\n        self.kafka_producer: Optional[Producer] = None\n        self.kafka_consumer: Optional[Consumer] = None\n        self.rabbitmq_manager: Optional[RabbitMQManager] = None\n        self.event_cache: Optional[EventCache] = None\n        \n        # Message routing\n        self.command_handlers: Dict[str, Callable] = {}\n        self.event_handlers: Dict[str, Callable] = {}\n        self.query_handlers: Dict[str, Callable] = {}\n        \n        # Processing state\n        self.running = False\n        self.processor_threads: Dict[str, threading.Thread] = {}\n        \n        # Initialize components\n        self._initialize_components()\n        \n    def _initialize_components(self):\n        \"\"\"Initialize messaging components\"\"\"\n        try:\n            # Initialize Kafka producer for events\n            kafka_config = {\n                'bootstrap.servers': ','.join(self.kafka_bootstrap_servers),\n                'enable.idempotence': True,\n                'acks': 'all',\n                'compression.type': 'zstd',\n                'batch.size': 65536,\n                'linger.ms': 10\n            }\n            self.kafka_producer = Producer(kafka_config)\n            \n            # Initialize Kafka consumer for events\n            consumer_config = {\n                'bootstrap.servers': ','.join(self.kafka_bootstrap_servers),\n                'group.id': 'hybrid-messaging-orchestrator',\n                'auto.offset.reset': 'earliest',\n                'enable.auto.commit': False\n            }\n            self.kafka_consumer = Consumer(consumer_config)\n            \n            # Initialize RabbitMQ for commands\n            self.rabbitmq_manager = RabbitMQManager(self.rabbitmq_config)\n            \n            # Initialize event cache if Redis available\n            if self.redis_client:\n                from src.streaming.event_sourcing_cache_integration import CacheConfig, EventCache\n                cache_config = CacheConfig()\n                self.event_cache = EventCache(cache_config)\n            \n            # Register default handlers\n            self._register_default_handlers()\n            \n            self.logger.info(\"Hybrid messaging components initialized\")\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to initialize messaging components: {e}\")\n            raise\n    \n    def _register_default_handlers(self):\n        \"\"\"Register default message handlers\"\"\"\n        # Command handlers\n        self.command_handlers[\"create_customer\"] = self._handle_create_customer_command\n        self.command_handlers[\"place_order\"] = self._handle_place_order_command\n        self.command_handlers[\"update_inventory\"] = self._handle_update_inventory_command\n        \n        # Event handlers  \n        self.event_handlers[\"customer_created\"] = self._handle_customer_created_event\n        self.event_handlers[\"order_placed\"] = self._handle_order_placed_event\n        self.event_handlers[\"inventory_updated\"] = self._handle_inventory_updated_event\n        \n        # Query handlers\n        self.query_handlers[\"get_customer\"] = self._handle_get_customer_query\n        self.query_handlers[\"get_order_history\"] = self._handle_get_order_history_query\n    \n    def start_processing(self):\n        \"\"\"Start hybrid message processing\"\"\"\n        try:\n            self.running = True\n            \n            # Start RabbitMQ command consumers\n            self.rabbitmq_manager.consume_commands(\n                \"commands.high_priority\",\n                self._process_command\n            )\n            \n            self.rabbitmq_manager.consume_commands(\n                \"commands.normal_priority\", \n                self._process_command\n            )\n            \n            self.rabbitmq_manager.consume_commands(\n                \"commands.low_priority\",\n                self._process_command\n            )\n            \n            # Start Kafka event consumer\n            event_thread = threading.Thread(\n                target=self._consume_events,\n                name=\"kafka-event-consumer\",\n                daemon=True\n            )\n            event_thread.start()\n            self.processor_threads[\"events\"] = event_thread\n            \n            self.logger.info(\"Hybrid messaging processing started\")\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to start processing: {e}\")\n            self.stop_processing()\n            raise\n    \n    def _process_command(self, command: HybridMessage) -> Optional[HybridMessage]:\n        \"\"\"Process command message from RabbitMQ\"\"\"\n        try:\n            command_type = command.payload.get(\"command_type\")\n            \n            if command_type in self.command_handlers:\n                handler = self.command_handlers[command_type]\n                \n                # Execute command\n                result = handler(command)\n                \n                # Generate event based on command result\n                if result:\n                    event = self._convert_command_result_to_event(command, result)\n                    self._publish_event(event)\n                \n                # Return reply if requested\n                if command.reply_to:\n                    return HybridMessage(\n                        message_id=str(uuid.uuid4()),\n                        message_type=MessageType.REPLY,\n                        routing_key=\"reply\",\n                        payload={\n                            \"status\": \"success\",\n                            \"result\": result.payload if result else {},\n                            \"original_command_id\": command.message_id\n                        },\n                        timestamp=datetime.now(),\n                        correlation_id=command.correlation_id\n                    )\n            else:\n                self.logger.warning(f\"No handler for command type: {command_type}\")\n                \n                if command.reply_to:\n                    return HybridMessage(\n                        message_id=str(uuid.uuid4()),\n                        message_type=MessageType.REPLY,\n                        routing_key=\"reply\",\n                        payload={\n                            \"status\": \"error\",\n                            \"error\": f\"Unknown command type: {command_type}\",\n                            \"original_command_id\": command.message_id\n                        },\n                        timestamp=datetime.now(),\n                        correlation_id=command.correlation_id\n                    )\n            \n            return None\n            \n        except Exception as e:\n            self.logger.error(f\"Command processing failed: {e}\")\n            \n            if command.reply_to:\n                return HybridMessage(\n                    message_id=str(uuid.uuid4()),\n                    message_type=MessageType.REPLY,\n                    routing_key=\"reply\",\n                    payload={\n                        \"status\": \"error\",\n                        \"error\": str(e),\n                        \"original_command_id\": command.message_id\n                    },\n                    timestamp=datetime.now(),\n                    correlation_id=command.correlation_id\n                )\n            \n            return None\n    \n    def _consume_events(self):\n        \"\"\"Consume events from Kafka\"\"\"\n        try:\n            self.kafka_consumer.subscribe([\"events\", \"customer_events\", \"order_events\"])\n            \n            while self.running:\n                msg = self.kafka_consumer.poll(timeout=1.0)\n                \n                if msg is None:\n                    continue\n                    \n                if msg.error():\n                    self.logger.error(f\"Kafka consumer error: {msg.error()}\")\n                    continue\n                \n                try:\n                    # Parse event message\n                    event_data = json.loads(msg.value().decode('utf-8'))\n                    \n                    # Convert to EnhancedStreamingMessage\n                    event = EnhancedStreamingMessage.from_dict(event_data)\n                    \n                    # Process event\n                    self._process_event(event)\n                    \n                    # Commit offset\n                    self.kafka_consumer.commit(asynchronous=False)\n                    \n                except Exception as e:\n                    self.logger.error(f\"Event processing failed: {e}\")\n                    \n        except Exception as e:\n            self.logger.error(f\"Event consumption failed: {e}\")\n    \n    def _process_event(self, event: EnhancedStreamingMessage):\n        \"\"\"Process event message\"\"\"\n        try:\n            event_type = event.event_type\n            \n            # Cache event if caching enabled\n            if self.event_cache:\n                cache_key = f\"event:{event.message_id}\"\n                self.event_cache.set(cache_key, event.to_dict(), ttl=3600)\n            \n            # Handle event\n            if event_type in self.event_handlers:\n                handler = self.event_handlers[event_type]\n                handler(event)\n            \n            # Update metrics\n            if self.metrics:\n                self.metrics.increment_counter(\n                    \"hybrid_events_processed\",\n                    {\"event_type\": event_type}\n                )\n            \n        except Exception as e:\n            self.logger.error(f\"Event processing failed: {e}\")\n    \n    def _publish_event(self, event: EnhancedStreamingMessage):\n        \"\"\"Publish event to Kafka\"\"\"\n        try:\n            self.kafka_producer.produce(\n                topic=\"events\",\n                key=event.key,\n                value=json.dumps(event.to_dict(), default=str),\n                headers=event.headers\n            )\n            self.kafka_producer.flush()\n            \n            self.logger.debug(f\"Published event {event.event_type} with ID {event.message_id}\")\n            \n        except Exception as e:\n            self.logger.error(f\"Event publishing failed: {e}\")\n    \n    def _convert_command_result_to_event(self, \n                                       command: HybridMessage, \n                                       result: HybridMessage) -> EnhancedStreamingMessage:\n        \"\"\"Convert command processing result to event\"\"\"\n        command_type = command.payload.get(\"command_type\")\n        \n        # Map command types to event types\n        event_type_mapping = {\n            \"create_customer\": \"customer_created\",\n            \"place_order\": \"order_placed\",\n            \"update_inventory\": \"inventory_updated\"\n        }\n        \n        event_type = event_type_mapping.get(command_type, f\"{command_type}_completed\")\n        \n        return EnhancedStreamingMessage(\n            message_id=str(uuid.uuid4()),\n            topic=\"events\",\n            key=result.payload.get(\"entity_id\", command.correlation_id),\n            payload=result.payload,\n            timestamp=datetime.now(),\n            headers=command.headers,\n            event_type=event_type,\n            correlation_id=command.correlation_id,\n            causation_id=command.message_id\n        )\n    \n    # Default command handlers\n    def _handle_create_customer_command(self, command: HybridMessage) -> HybridMessage:\n        \"\"\"Handle create customer command\"\"\"\n        customer_data = command.payload\n        customer_id = str(uuid.uuid4())\n        \n        # Simulate customer creation\n        result = {\n            \"entity_id\": customer_id,\n            \"customer_id\": customer_id,\n            \"name\": customer_data.get(\"name\"),\n            \"email\": customer_data.get(\"email\"),\n            \"created_at\": datetime.now().isoformat(),\n            \"status\": \"active\"\n        }\n        \n        return HybridMessage(\n            message_id=str(uuid.uuid4()),\n            message_type=MessageType.EVENT,\n            routing_key=\"customer.created\",\n            payload=result,\n            timestamp=datetime.now(),\n            correlation_id=command.correlation_id\n        )\n    \n    def _handle_place_order_command(self, command: HybridMessage) -> HybridMessage:\n        \"\"\"Handle place order command\"\"\"\n        order_data = command.payload\n        order_id = str(uuid.uuid4())\n        \n        # Simulate order placement\n        result = {\n            \"entity_id\": order_id,\n            \"order_id\": order_id,\n            \"customer_id\": order_data.get(\"customer_id\"),\n            \"items\": order_data.get(\"items\", []),\n            \"total_amount\": order_data.get(\"total_amount\", 0.0),\n            \"placed_at\": datetime.now().isoformat(),\n            \"status\": \"placed\"\n        }\n        \n        return HybridMessage(\n            message_id=str(uuid.uuid4()),\n            message_type=MessageType.EVENT,\n            routing_key=\"order.placed\",\n            payload=result,\n            timestamp=datetime.now(),\n            correlation_id=command.correlation_id\n        )\n    \n    def _handle_update_inventory_command(self, command: HybridMessage) -> HybridMessage:\n        \"\"\"Handle update inventory command\"\"\"\n        inventory_data = command.payload\n        \n        # Simulate inventory update\n        result = {\n            \"entity_id\": inventory_data.get(\"product_id\"),\n            \"product_id\": inventory_data.get(\"product_id\"),\n            \"previous_quantity\": inventory_data.get(\"previous_quantity\", 0),\n            \"new_quantity\": inventory_data.get(\"new_quantity\", 0),\n            \"updated_at\": datetime.now().isoformat()\n        }\n        \n        return HybridMessage(\n            message_id=str(uuid.uuid4()),\n            message_type=MessageType.EVENT,\n            routing_key=\"inventory.updated\",\n            payload=result,\n            timestamp=datetime.now(),\n            correlation_id=command.correlation_id\n        )\n    \n    # Default event handlers\n    def _handle_customer_created_event(self, event: EnhancedStreamingMessage):\n        \"\"\"Handle customer created event\"\"\"\n        customer_id = event.payload.get(\"customer_id\")\n        self.logger.info(f\"Customer created: {customer_id}\")\n        \n        # Update read models, send notifications, etc.\n        \n    def _handle_order_placed_event(self, event: EnhancedStreamingMessage):\n        \"\"\"Handle order placed event\"\"\"\n        order_id = event.payload.get(\"order_id\")\n        customer_id = event.payload.get(\"customer_id\")\n        self.logger.info(f\"Order {order_id} placed by customer {customer_id}\")\n        \n        # Update read models, trigger fulfillment, etc.\n        \n    def _handle_inventory_updated_event(self, event: EnhancedStreamingMessage):\n        \"\"\"Handle inventory updated event\"\"\"\n        product_id = event.payload.get(\"product_id\")\n        new_quantity = event.payload.get(\"new_quantity\")\n        self.logger.info(f\"Inventory updated for product {product_id}: {new_quantity}\")\n        \n        # Update read models, check stock levels, etc.\n    \n    # Default query handlers\n    def _handle_get_customer_query(self, query: HybridMessage) -> Dict[str, Any]:\n        \"\"\"Handle get customer query\"\"\"\n        customer_id = query.payload.get(\"customer_id\")\n        \n        # Check cache first\n        if self.event_cache:\n            cache_key = f\"customer:{customer_id}\"\n            customer_data, hit = self.event_cache.get(cache_key)\n            if hit:\n                return customer_data\n        \n        # Simulate database lookup\n        return {\n            \"customer_id\": customer_id,\n            \"name\": \"John Doe\",\n            \"email\": \"john@example.com\",\n            \"status\": \"active\"\n        }\n    \n    def _handle_get_order_history_query(self, query: HybridMessage) -> Dict[str, Any]:\n        \"\"\"Handle get order history query\"\"\"\n        customer_id = query.payload.get(\"customer_id\")\n        \n        # Check cache first\n        if self.event_cache:\n            cache_key = f\"orders:{customer_id}\"\n            orders_data, hit = self.event_cache.get(cache_key)\n            if hit:\n                return {\"customer_id\": customer_id, \"orders\": orders_data}\n        \n        # Simulate database lookup\n        return {\n            \"customer_id\": customer_id,\n            \"orders\": [\n                {\"order_id\": \"order1\", \"amount\": 99.99, \"status\": \"delivered\"},\n                {\"order_id\": \"order2\", \"amount\": 149.50, \"status\": \"processing\"}\n            ]\n        }\n    \n    def send_command(self, command: HybridMessage) -> bool:\n        \"\"\"Send command via RabbitMQ\"\"\"\n        return self.rabbitmq_manager.publish_command(command)\n    \n    def publish_event(self, event: EnhancedStreamingMessage):\n        \"\"\"Publish event via Kafka\"\"\"\n        self._publish_event(event)\n    \n    def stop_processing(self):\n        \"\"\"Stop hybrid message processing\"\"\"\n        self.logger.info(\"Stopping hybrid messaging processing\")\n        \n        self.running = False\n        \n        # Stop RabbitMQ consumers\n        if self.rabbitmq_manager:\n            self.rabbitmq_manager.close()\n        \n        # Stop Kafka consumer\n        if self.kafka_consumer:\n            self.kafka_consumer.close()\n        \n        # Wait for processor threads\n        for thread in self.processor_threads.values():\n            thread.join(timeout=30)\n        \n        # Flush Kafka producer\n        if self.kafka_producer:\n            self.kafka_producer.flush()\n        \n        self.logger.info(\"Hybrid messaging processing stopped\")\n    \n    def get_metrics(self) -> Dict[str, Any]:\n        \"\"\"Get hybrid messaging metrics\"\"\"\n        rabbitmq_metrics = self.rabbitmq_manager.get_metrics() if self.rabbitmq_manager else {}\n        \n        return {\n            \"running\": self.running,\n            \"processor_threads\": len(self.processor_threads),\n            \"command_handlers\": len(self.command_handlers),\n            \"event_handlers\": len(self.event_handlers),\n            \"query_handlers\": len(self.query_handlers),\n            \"rabbitmq_metrics\": rabbitmq_metrics,\n            \"timestamp\": datetime.now().isoformat()\n        }\n\n\n# Factory functions\ndef create_rabbitmq_config(**kwargs) -> RabbitMQConfig:\n    \"\"\"Create RabbitMQ configuration\"\"\"\n    return RabbitMQConfig(**kwargs)\n\n\ndef create_hybrid_messaging_orchestrator(\n    kafka_bootstrap_servers: List[str],\n    rabbitmq_config: RabbitMQConfig,\n    redis_client: Optional[redis.Redis] = None\n) -> HybridMessagingOrchestrator:\n    \"\"\"Create hybrid messaging orchestrator\"\"\"\n    return HybridMessagingOrchestrator(\n        kafka_bootstrap_servers=kafka_bootstrap_servers,\n        rabbitmq_config=rabbitmq_config,\n        redis_client=redis_client\n    )\n\n\n# Example usage\nif __name__ == \"__main__\":\n    import time\n    \n    try:\n        print(\"Testing Hybrid Messaging Architecture...\")\n        \n        # Create configurations\n        rabbitmq_config = create_rabbitmq_config(\n            host=\"localhost\",\n            port=5672,\n            username=\"guest\",\n            password=\"guest\",\n            confirm_delivery=True,\n            enable_dead_letter=True\n        )\n        \n        # Create orchestrator\n        orchestrator = create_hybrid_messaging_orchestrator(\n            kafka_bootstrap_servers=[\"localhost:9092\"],\n            rabbitmq_config=rabbitmq_config\n        )\n        \n        # Start processing\n        orchestrator.start_processing()\n        \n        print(\"✅ Hybrid messaging orchestrator started\")\n        \n        # Send test command\n        test_command = HybridMessage(\n            message_id=str(uuid.uuid4()),\n            message_type=MessageType.COMMAND,\n            routing_key=\"high\",\n            payload={\n                \"command_type\": \"create_customer\",\n                \"name\": \"John Doe\",\n                \"email\": \"john@example.com\"\n            },\n            timestamp=datetime.now(),\n            priority=MessagePriority.HIGH\n        )\n        \n        success = orchestrator.send_command(test_command)\n        print(f\"✅ Test command sent: {success}\")\n        \n        # Let it process for a moment\n        time.sleep(5)\n        \n        # Get metrics\n        metrics = orchestrator.get_metrics()\n        print(f\"✅ Hybrid messaging metrics: {metrics}\")\n        \n        # Stop processing\n        orchestrator.stop_processing()\n        \n        print(\"✅ Hybrid Messaging Architecture testing completed\")\n        \n    except Exception as e:\n        print(f\"❌ Testing failed: {str(e)}\")\n        import traceback\n        traceback.print_exc()