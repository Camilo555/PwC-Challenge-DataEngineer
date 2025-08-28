"""
Advanced Kafka Streams Processing with Event Sourcing and CQRS Patterns
Provides comprehensive stream processing capabilities with enterprise patterns
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
from concurrent.futures import ThreadPoolExecutor
import hashlib

import redis
import pika
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.avro import AvroProducer, AvroConsumer
from confluent_kafka.admin import AdminClient, NewTopic

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import (
    EnhancedStreamingMessage, MessageFormat, CompressionType,
    AdvancedProducerConfig, AdvancedConsumerConfig
)


class StreamProcessorType(Enum):
    """Types of stream processors"""
    STATELESS = "stateless"
    STATEFUL = "stateful"
    WINDOWED = "windowed"
    JOIN = "join"
    AGGREGATION = "aggregation"


class WindowType(Enum):
    """Window types for stream processing"""
    TUMBLING = "tumbling"
    HOPPING = "hopping"
    SESSION = "session"
    SLIDING = "sliding"


class JoinType(Enum):
    """Join types for stream processing"""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"


@dataclass
class WindowConfig:
    """Configuration for windowed operations"""
    window_type: WindowType
    window_size_ms: int
    advance_ms: Optional[int] = None  # For hopping windows
    grace_period_ms: int = 300000  # 5 minutes
    retention_ms: int = 3600000  # 1 hour
    

@dataclass
class ProcessorState:
    """State management for stateful processors"""
    state_store_name: str
    changelog_topic: str
    enable_logging: bool = True
    cache_max_bytes: int = 10485760  # 10MB
    commit_interval_ms: int = 30000  # 30 seconds


@dataclass
class EventSourcingState:
    """Event sourcing state management"""
    aggregate_id: str
    aggregate_type: str
    version: int
    events: List[EnhancedStreamingMessage] = field(default_factory=list)
    snapshot_data: Optional[Dict[str, Any]] = None
    last_snapshot_version: int = 0


class StreamProcessor(ABC):
    """Abstract base class for stream processors"""
    
    def __init__(self, processor_name: str):
        self.processor_name = processor_name
        self.logger = get_logger(f"{__name__}.{processor_name}")
        
    @abstractmethod
    def process(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Process a single message"""
        pass
    
    @abstractmethod
    def setup(self):
        """Setup processor state"""
        pass
    
    @abstractmethod
    def cleanup(self):
        """Cleanup processor resources"""
        pass


class EventSourcingProcessor(StreamProcessor):
    """Event sourcing pattern implementation"""
    
    def __init__(self, 
                 processor_name: str,
                 event_store_topic: str,
                 snapshot_frequency: int = 100,
                 redis_client: Optional[redis.Redis] = None):
        super().__init__(processor_name)
        self.event_store_topic = event_store_topic
        self.snapshot_frequency = snapshot_frequency
        self.redis_client = redis_client
        
        # In-memory event store (in production, use persistent storage)
        self.event_store: Dict[str, EventSourcingState] = {}
        self.aggregate_cache: Dict[str, Dict[str, Any]] = {}
        
    def setup(self):
        """Setup event sourcing processor"""
        self.logger.info(f"Setting up event sourcing processor: {self.processor_name}")
        
    def cleanup(self):
        """Cleanup event sourcing resources"""
        self.logger.info(f"Cleaning up event sourcing processor: {self.processor_name}")
        
    def process(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Process event sourcing message"""
        try:
            # Extract aggregate information
            aggregate_id = message.aggregate_id
            if not aggregate_id:
                self.logger.warning(f"Message {message.message_id} missing aggregate_id")
                return None
            
            # Get or create aggregate state
            aggregate_state = self._get_aggregate_state(aggregate_id)
            
            # Apply event to aggregate
            self._apply_event_to_aggregate(aggregate_state, message)
            
            # Check if snapshot is needed
            if self._should_create_snapshot(aggregate_state):
                self._create_snapshot(aggregate_state)
                
            # Cache aggregate state in Redis if available
            if self.redis_client:
                self._cache_aggregate_state(aggregate_state)
                
            # Create result event
            result_event = self._create_result_event(message, aggregate_state)
            
            self.logger.debug(f"Processed event for aggregate {aggregate_id}, version {aggregate_state.version}")
            return result_event
            
        except Exception as e:
            self.logger.error(f"Event sourcing processing failed: {e}")
            return None
    
    def _get_aggregate_state(self, aggregate_id: str) -> EventSourcingState:
        """Get aggregate state from store or cache"""
        # Try cache first
        if self.redis_client:
            cached_state = self._get_cached_aggregate_state(aggregate_id)
            if cached_state:
                return cached_state
        
        # Get from event store
        if aggregate_id in self.event_store:
            return self.event_store[aggregate_id]
        
        # Create new aggregate state
        aggregate_state = EventSourcingState(
            aggregate_id=aggregate_id,
            aggregate_type="default",
            version=0
        )
        self.event_store[aggregate_id] = aggregate_state
        return aggregate_state
    
    def _apply_event_to_aggregate(self, 
                                state: EventSourcingState, 
                                event: EnhancedStreamingMessage):
        """Apply event to aggregate state"""
        # Increment version
        state.version += 1
        
        # Add event to event log
        state.events.append(event)
        
        # Apply business logic based on event type
        self._apply_business_logic(state, event)
        
    def _apply_business_logic(self, 
                            state: EventSourcingState, 
                            event: EnhancedStreamingMessage):
        """Apply business logic based on event type"""
        # This would be customized based on domain logic
        event_type = event.event_type
        payload = event.payload
        
        # Simple example: customer events
        if event_type == "customer_created":
            state.snapshot_data = {
                "customer_id": payload.get("customer_id"),
                "name": payload.get("name"),
                "created_at": event.timestamp.isoformat(),
                "status": "active"
            }
        elif event_type == "customer_updated":
            if state.snapshot_data:
                state.snapshot_data.update(payload)
        elif event_type == "customer_deleted":
            if state.snapshot_data:
                state.snapshot_data["status"] = "deleted"
                state.snapshot_data["deleted_at"] = event.timestamp.isoformat()
                
    def _should_create_snapshot(self, state: EventSourcingState) -> bool:
        """Check if snapshot should be created"""
        events_since_snapshot = state.version - state.last_snapshot_version
        return events_since_snapshot >= self.snapshot_frequency
    
    def _create_snapshot(self, state: EventSourcingState):
        """Create aggregate snapshot"""
        state.last_snapshot_version = state.version
        self.logger.info(f"Created snapshot for aggregate {state.aggregate_id} at version {state.version}")
        
    def _cache_aggregate_state(self, state: EventSourcingState):
        """Cache aggregate state in Redis"""
        try:
            cache_key = f"aggregate:{state.aggregate_id}"
            cache_data = {
                "aggregate_type": state.aggregate_type,
                "version": state.version,
                "snapshot_data": state.snapshot_data,
                "last_snapshot_version": state.last_snapshot_version
            }
            self.redis_client.setex(
                cache_key, 
                3600,  # 1 hour TTL
                json.dumps(cache_data, default=str)
            )
        except Exception as e:
            self.logger.error(f"Failed to cache aggregate state: {e}")
            
    def _get_cached_aggregate_state(self, aggregate_id: str) -> Optional[EventSourcingState]:
        """Get cached aggregate state"""
        try:
            cache_key = f"aggregate:{aggregate_id}"
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return EventSourcingState(
                    aggregate_id=aggregate_id,
                    aggregate_type=data["aggregate_type"],
                    version=data["version"],
                    snapshot_data=data["snapshot_data"],
                    last_snapshot_version=data["last_snapshot_version"]
                )
        except Exception as e:
            self.logger.error(f"Failed to get cached aggregate state: {e}")
        return None
        
    def _create_result_event(self, 
                           original_event: EnhancedStreamingMessage,
                           state: EventSourcingState) -> EnhancedStreamingMessage:
        """Create result event after processing"""
        return EnhancedStreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=f"{original_event.topic}_processed",
            key=original_event.key,
            payload={
                "aggregate_id": state.aggregate_id,
                "aggregate_version": state.version,
                "snapshot_data": state.snapshot_data,
                "original_event_id": original_event.message_id
            },
            timestamp=datetime.now(),
            headers=original_event.headers.copy(),
            event_type="aggregate_updated",
            aggregate_id=state.aggregate_id,
            aggregate_version=state.version,
            correlation_id=original_event.correlation_id,
            causation_id=original_event.message_id
        )


class CQRSProcessor(StreamProcessor):
    """CQRS pattern implementation separating commands and queries"""
    
    def __init__(self, 
                 processor_name: str,
                 command_topics: List[str],
                 event_topics: List[str],
                 query_topics: List[str],
                 rabbitmq_channel: Optional[pika.channel.Channel] = None):
        super().__init__(processor_name)
        self.command_topics = command_topics
        self.event_topics = event_topics
        self.query_topics = query_topics
        self.rabbitmq_channel = rabbitmq_channel
        
        # Command handlers registry
        self.command_handlers: Dict[str, Callable] = {}
        self.query_handlers: Dict[str, Callable] = {}
        
        # Read models (simplified in-memory for demo)
        self.read_models: Dict[str, Dict[str, Any]] = {}
        
    def setup(self):
        """Setup CQRS processor"""
        self._register_default_handlers()
        self.logger.info(f"CQRS processor setup complete: {self.processor_name}")
        
    def cleanup(self):
        """Cleanup CQRS resources"""
        self.logger.info(f"CQRS processor cleanup: {self.processor_name}")
        
    def process(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Process CQRS message based on type"""
        try:
            if message.topic in self.command_topics:
                return self._handle_command(message)
            elif message.topic in self.event_topics:
                return self._handle_event(message)
            elif message.topic in self.query_topics:
                return self._handle_query(message)
            else:
                self.logger.warning(f"Unknown topic for CQRS processing: {message.topic}")
                return None
                
        except Exception as e:
            self.logger.error(f"CQRS processing failed: {e}")
            return None
    
    def _handle_command(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Handle command message"""
        command_type = message.event_type
        
        # Send command to RabbitMQ for reliable processing
        if self.rabbitmq_channel:
            self._send_command_to_rabbitmq(message)
        
        # Process command
        handler = self.command_handlers.get(command_type)
        if handler:
            result = handler(message)
            
            # Convert command result to event
            if result:
                return self._create_event_from_command(message, result)
        else:
            self.logger.warning(f"No handler for command type: {command_type}")
            
        return None
    
    def _handle_event(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Handle event message - update read models"""
        try:
            # Update read models based on event
            self._update_read_models(message)
            
            # Create acknowledgment event
            return EnhancedStreamingMessage(
                message_id=str(uuid.uuid4()),
                topic="event_processed",
                key=message.key,
                payload={
                    "original_event_id": message.message_id,
                    "processed_at": datetime.now().isoformat(),
                    "read_models_updated": True
                },
                timestamp=datetime.now(),
                headers=message.headers,
                event_type="event_processed",
                correlation_id=message.correlation_id,
                causation_id=message.message_id
            )
            
        except Exception as e:
            self.logger.error(f"Event handling failed: {e}")
            return None
    
    def _handle_query(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Handle query message"""
        query_type = message.event_type
        
        handler = self.query_handlers.get(query_type)
        if handler:
            result = handler(message)
            
            # Create query response
            return EnhancedStreamingMessage(
                message_id=str(uuid.uuid4()),
                topic="query_response",
                key=message.key,
                payload={
                    "query_id": message.message_id,
                    "result": result,
                    "timestamp": datetime.now().isoformat()
                },
                timestamp=datetime.now(),
                headers=message.headers,
                event_type="query_response",
                correlation_id=message.correlation_id,
                causation_id=message.message_id
            )
        else:
            self.logger.warning(f"No handler for query type: {query_type}")
            
        return None
    
    def _send_command_to_rabbitmq(self, message: EnhancedStreamingMessage):
        """Send command to RabbitMQ for reliable processing"""
        try:
            queue_name = f"command_{message.topic}"
            self.rabbitmq_channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message.to_dict(), default=str),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    message_id=message.message_id,
                    correlation_id=message.correlation_id
                )
            )
            self.logger.debug(f"Sent command to RabbitMQ queue: {queue_name}")
        except Exception as e:
            self.logger.error(f"Failed to send command to RabbitMQ: {e}")
    
    def _create_event_from_command(self, 
                                 command: EnhancedStreamingMessage,
                                 command_result: Dict[str, Any]) -> EnhancedStreamingMessage:
        """Create event from command processing result"""
        return EnhancedStreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=f"event_{command.topic}",
            key=command.key,
            payload=command_result,
            timestamp=datetime.now(),
            headers=command.headers,
            event_type=f"{command.event_type}_completed",
            aggregate_id=command.aggregate_id,
            aggregate_version=command.aggregate_version + 1,
            correlation_id=command.correlation_id,
            causation_id=command.message_id
        )
    
    def _update_read_models(self, event: EnhancedStreamingMessage):
        """Update read models based on events"""
        event_type = event.event_type
        payload = event.payload
        
        # Simple read model updates (in production, use proper database)
        if event_type == "customer_created":
            customer_id = payload.get("customer_id")
            if customer_id:
                self.read_models[f"customer_{customer_id}"] = {
                    "customer_id": customer_id,
                    "name": payload.get("name"),
                    "email": payload.get("email"),
                    "created_at": event.timestamp.isoformat(),
                    "status": "active"
                }
        elif event_type == "order_placed":
            order_id = payload.get("order_id")
            if order_id:
                self.read_models[f"order_{order_id}"] = {
                    "order_id": order_id,
                    "customer_id": payload.get("customer_id"),
                    "total_amount": payload.get("total_amount"),
                    "placed_at": event.timestamp.isoformat(),
                    "status": "placed"
                }
    
    def _register_default_handlers(self):
        """Register default command and query handlers"""
        # Command handlers
        self.command_handlers["create_customer"] = self._handle_create_customer_command
        self.command_handlers["place_order"] = self._handle_place_order_command
        
        # Query handlers  
        self.query_handlers["get_customer"] = self._handle_get_customer_query
        self.query_handlers["get_customer_orders"] = self._handle_get_customer_orders_query
    
    def _handle_create_customer_command(self, message: EnhancedStreamingMessage) -> Dict[str, Any]:
        """Handle create customer command"""
        payload = message.payload
        customer_id = str(uuid.uuid4())
        
        return {
            "customer_id": customer_id,
            "name": payload.get("name"),
            "email": payload.get("email"),
            "created_at": datetime.now().isoformat()
        }
    
    def _handle_place_order_command(self, message: EnhancedStreamingMessage) -> Dict[str, Any]:
        """Handle place order command"""
        payload = message.payload
        order_id = str(uuid.uuid4())
        
        return {
            "order_id": order_id,
            "customer_id": payload.get("customer_id"),
            "items": payload.get("items", []),
            "total_amount": payload.get("total_amount", 0.0),
            "placed_at": datetime.now().isoformat()
        }
    
    def _handle_get_customer_query(self, message: EnhancedStreamingMessage) -> Dict[str, Any]:
        """Handle get customer query"""
        customer_id = message.payload.get("customer_id")
        customer_key = f"customer_{customer_id}"
        
        if customer_key in self.read_models:
            return self.read_models[customer_key]
        else:
            return {"error": "Customer not found"}
    
    def _handle_get_customer_orders_query(self, message: EnhancedStreamingMessage) -> Dict[str, Any]:
        """Handle get customer orders query"""
        customer_id = message.payload.get("customer_id")
        
        # Find all orders for customer
        orders = []
        for key, model in self.read_models.items():
            if key.startswith("order_") and model.get("customer_id") == customer_id:
                orders.append(model)
        
        return {"customer_id": customer_id, "orders": orders}


class SagaOrchestrator(StreamProcessor):
    """Saga pattern implementation for distributed transactions"""
    
    def __init__(self, 
                 processor_name: str,
                 orchestration_topic: str,
                 compensation_topic: str):
        super().__init__(processor_name)
        self.orchestration_topic = orchestration_topic
        self.compensation_topic = compensation_topic
        
        # Saga instances tracking
        self.active_sagas: Dict[str, Dict[str, Any]] = {}
        
        # Saga definitions
        self.saga_definitions: Dict[str, List[Dict[str, Any]]] = {}
        
    def setup(self):
        """Setup saga orchestrator"""
        self._register_default_sagas()
        self.logger.info(f"Saga orchestrator setup complete: {self.processor_name}")
        
    def cleanup(self):
        """Cleanup saga resources"""
        self.logger.info(f"Saga orchestrator cleanup: {self.processor_name}")
        
    def process(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Process saga orchestration message"""
        try:
            if message.event_type == "start_saga":
                return self._start_saga(message)
            elif message.event_type == "saga_step_completed":
                return self._handle_saga_step_completion(message)
            elif message.event_type == "saga_step_failed":
                return self._handle_saga_step_failure(message)
            else:
                self.logger.warning(f"Unknown saga event type: {message.event_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"Saga orchestration failed: {e}")
            return None
    
    def _start_saga(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Start a new saga instance"""
        saga_type = message.payload.get("saga_type")
        saga_id = str(uuid.uuid4())
        
        if saga_type not in self.saga_definitions:
            self.logger.error(f"Unknown saga type: {saga_type}")
            return None
        
        # Create saga instance
        saga_instance = {
            "saga_id": saga_id,
            "saga_type": saga_type,
            "status": "started",
            "current_step": 0,
            "steps_completed": [],
            "steps_to_compensate": [],
            "saga_data": message.payload.get("saga_data", {}),
            "started_at": datetime.now().isoformat(),
            "correlation_id": message.correlation_id
        }
        
        self.active_sagas[saga_id] = saga_instance
        
        # Execute first step
        return self._execute_saga_step(saga_instance, 0)
    
    def _execute_saga_step(self, saga_instance: Dict[str, Any], step_index: int) -> Optional[EnhancedStreamingMessage]:
        """Execute a saga step"""
        saga_type = saga_instance["saga_type"]
        steps = self.saga_definitions[saga_type]
        
        if step_index >= len(steps):
            # Saga completed successfully
            return self._complete_saga(saga_instance)
        
        step = steps[step_index]
        saga_instance["current_step"] = step_index
        
        # Create step execution message
        return EnhancedStreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=step["topic"],
            key=saga_instance["saga_id"],
            payload={
                "saga_id": saga_instance["saga_id"],
                "step_name": step["name"],
                "step_data": step.get("data", {}),
                "saga_data": saga_instance["saga_data"]
            },
            timestamp=datetime.now(),
            headers={"saga_step": str(step_index)},
            event_type=step["command"],
            correlation_id=saga_instance["correlation_id"],
            causation_id=saga_instance["saga_id"]
        )
    
    def _handle_saga_step_completion(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Handle saga step completion"""
        saga_id = message.payload.get("saga_id")
        
        if saga_id not in self.active_sagas:
            self.logger.error(f"Unknown saga instance: {saga_id}")
            return None
        
        saga_instance = self.active_sagas[saga_id]
        current_step = saga_instance["current_step"]
        
        # Mark step as completed
        saga_instance["steps_completed"].append(current_step)
        saga_instance["steps_to_compensate"].insert(0, current_step)  # LIFO for compensation
        
        # Update saga data with step result
        step_result = message.payload.get("step_result", {})
        saga_instance["saga_data"].update(step_result)
        
        # Execute next step
        return self._execute_saga_step(saga_instance, current_step + 1)
    
    def _handle_saga_step_failure(self, message: EnhancedStreamingMessage) -> Optional[EnhancedStreamingMessage]:
        """Handle saga step failure - start compensation"""
        saga_id = message.payload.get("saga_id")
        
        if saga_id not in self.active_sagas:
            self.logger.error(f"Unknown saga instance: {saga_id}")
            return None
        
        saga_instance = self.active_sagas[saga_id]
        saga_instance["status"] = "compensating"
        saga_instance["failure_reason"] = message.payload.get("error", "Unknown error")
        
        # Start compensation from last completed step
        return self._start_compensation(saga_instance)
    
    def _start_compensation(self, saga_instance: Dict[str, Any]) -> Optional[EnhancedStreamingMessage]:
        """Start saga compensation"""
        if not saga_instance["steps_to_compensate"]:
            # No steps to compensate, mark saga as failed
            return self._fail_saga(saga_instance)
        
        step_index = saga_instance["steps_to_compensate"].pop(0)
        saga_type = saga_instance["saga_type"]
        steps = self.saga_definitions[saga_type]
        step = steps[step_index]
        
        # Create compensation message
        return EnhancedStreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=self.compensation_topic,
            key=saga_instance["saga_id"],
            payload={
                "saga_id": saga_instance["saga_id"],
                "compensate_step": step["name"],
                "compensation_data": step.get("compensation_data", {}),
                "saga_data": saga_instance["saga_data"]
            },
            timestamp=datetime.now(),
            headers={"compensation_step": str(step_index)},
            event_type="compensate_step",
            correlation_id=saga_instance["correlation_id"],
            causation_id=saga_instance["saga_id"]
        )
    
    def _complete_saga(self, saga_instance: Dict[str, Any]) -> EnhancedStreamingMessage:
        """Complete saga successfully"""
        saga_instance["status"] = "completed"
        saga_instance["completed_at"] = datetime.now().isoformat()
        
        return EnhancedStreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=self.orchestration_topic,
            key=saga_instance["saga_id"],
            payload={
                "saga_id": saga_instance["saga_id"],
                "saga_type": saga_instance["saga_type"],
                "status": "completed",
                "result": saga_instance["saga_data"],
                "completed_at": saga_instance["completed_at"]
            },
            timestamp=datetime.now(),
            headers={},
            event_type="saga_completed",
            correlation_id=saga_instance["correlation_id"]
        )
    
    def _fail_saga(self, saga_instance: Dict[str, Any]) -> EnhancedStreamingMessage:
        """Fail saga after compensation"""
        saga_instance["status"] = "failed"
        saga_instance["failed_at"] = datetime.now().isoformat()
        
        return EnhancedStreamingMessage(
            message_id=str(uuid.uuid4()),
            topic=self.orchestration_topic,
            key=saga_instance["saga_id"],
            payload={
                "saga_id": saga_instance["saga_id"],
                "saga_type": saga_instance["saga_type"],
                "status": "failed",
                "failure_reason": saga_instance.get("failure_reason", "Unknown"),
                "failed_at": saga_instance["failed_at"]
            },
            timestamp=datetime.now(),
            headers={},
            event_type="saga_failed",
            correlation_id=saga_instance["correlation_id"]
        )
    
    def _register_default_sagas(self):
        """Register default saga definitions"""
        # Order fulfillment saga
        self.saga_definitions["order_fulfillment"] = [
            {
                "name": "validate_payment",
                "topic": "payment_service",
                "command": "validate_payment",
                "data": {},
                "compensation_data": {"action": "release_payment_hold"}
            },
            {
                "name": "reserve_inventory",
                "topic": "inventory_service", 
                "command": "reserve_inventory",
                "data": {},
                "compensation_data": {"action": "release_inventory"}
            },
            {
                "name": "create_shipment",
                "topic": "shipping_service",
                "command": "create_shipment", 
                "data": {},
                "compensation_data": {"action": "cancel_shipment"}
            },
            {
                "name": "process_payment",
                "topic": "payment_service",
                "command": "process_payment",
                "data": {},
                "compensation_data": {"action": "refund_payment"}
            }
        ]


class EnhancedKafkaStreamsProcessor:
    """Main Kafka Streams processor with advanced patterns"""
    
    def __init__(self,
                 bootstrap_servers: List[str],
                 application_id: str,
                 redis_client: Optional[redis.Redis] = None,
                 rabbitmq_channel: Optional[pika.channel.Channel] = None):
        self.bootstrap_servers = bootstrap_servers
        self.application_id = application_id
        self.redis_client = redis_client
        self.rabbitmq_channel = rabbitmq_channel
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Stream processors
        self.processors: Dict[str, StreamProcessor] = {}
        self.consumer_threads: Dict[str, threading.Thread] = {}
        self.producer: Optional[Producer] = None
        
        # Processing state
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Initialize components
        self._initialize_components()
        
    def _initialize_components(self):
        """Initialize Kafka Streams components"""
        try:
            # Initialize producer
            producer_config = {
                'bootstrap.servers': ','.join(self.bootstrap_servers),
                'enable.idempotence': True,
                'acks': 'all',
                'retries': 1000000,
                'compression.type': 'zstd'
            }
            self.producer = Producer(producer_config)
            
            # Initialize processors
            self._setup_processors()
            
            self.logger.info("Enhanced Kafka Streams processor initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
    
    def _setup_processors(self):
        """Setup stream processors"""
        # Event sourcing processor
        event_sourcing = EventSourcingProcessor(
            "event_sourcing",
            "event-store",
            snapshot_frequency=100,
            redis_client=self.redis_client
        )
        self.processors["event_sourcing"] = event_sourcing
        
        # CQRS processor
        cqrs = CQRSProcessor(
            "cqrs",
            ["commands"],
            ["events"],
            ["queries"],
            rabbitmq_channel=self.rabbitmq_channel
        )
        self.processors["cqrs"] = cqrs
        
        # Saga orchestrator
        saga = SagaOrchestrator(
            "saga_orchestrator",
            "saga-orchestration", 
            "saga-compensation"
        )
        self.processors["saga"] = saga
        
        # Setup all processors
        for processor in self.processors.values():
            processor.setup()
    
    def start_processing(self, topic_processor_mapping: Dict[str, str]):
        """Start stream processing"""
        try:
            self.running = True
            
            # Start consumer threads for each processor
            for topic, processor_name in topic_processor_mapping.items():
                if processor_name in self.processors:
                    thread = threading.Thread(
                        target=self._consume_topic,
                        args=(topic, processor_name),
                        name=f"consumer-{topic}",
                        daemon=True
                    )
                    thread.start()
                    self.consumer_threads[topic] = thread
            
            self.logger.info(f"Started {len(self.consumer_threads)} consumer threads")
            
        except Exception as e:
            self.logger.error(f"Failed to start processing: {e}")
            self.stop_processing()
            raise
    
    def _consume_topic(self, topic: str, processor_name: str):
        """Consume messages from topic and process with specified processor"""
        processor = self.processors[processor_name]
        
        # Create consumer
        consumer_config = {
            'bootstrap.servers': ','.join(self.bootstrap_servers),
            'group.id': f'{self.application_id}-{processor_name}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        
        try:
            while self.running and not self.shutdown_event.is_set():
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Parse message
                    message_data = json.loads(msg.value().decode('utf-8'))
                    message = EnhancedStreamingMessage.from_dict(message_data)
                    
                    # Process message
                    result = processor.process(message)
                    
                    # Publish result if available
                    if result:
                        self._publish_result(result)
                    
                    # Commit offset
                    consumer.commit(asynchronous=False)
                    
                    # Update metrics
                    if self.metrics_collector:
                        self.metrics_collector.increment_counter(
                            "messages_processed",
                            {"topic": topic, "processor": processor_name}
                        )
                    
                except Exception as e:
                    self.logger.error(f"Message processing failed: {e}")
                    # Send to dead letter queue
                    self._send_to_dead_letter_queue(msg, str(e))
                    
        except Exception as e:
            self.logger.error(f"Consumer thread error: {e}")
        finally:
            consumer.close()
    
    def _publish_result(self, message: EnhancedStreamingMessage):
        """Publish processing result"""
        try:
            message_json = json.dumps(message.to_dict(), default=str)
            self.producer.produce(
                topic=message.topic,
                key=message.key,
                value=message_json,
                headers=message.headers
            )
            self.producer.flush()
            
        except Exception as e:
            self.logger.error(f"Failed to publish result: {e}")
    
    def _send_to_dead_letter_queue(self, original_msg, error: str):
        """Send failed message to dead letter queue"""
        try:
            dead_letter_msg = {
                "original_topic": original_msg.topic(),
                "original_partition": original_msg.partition(),
                "original_offset": original_msg.offset(),
                "original_value": original_msg.value().decode('utf-8') if original_msg.value() else None,
                "error": error,
                "timestamp": datetime.now().isoformat()
            }
            
            self.producer.produce(
                topic="dead-letter-queue",
                value=json.dumps(dead_letter_msg)
            )
            self.producer.flush()
            
        except Exception as e:
            self.logger.error(f"Failed to send to dead letter queue: {e}")
    
    def stop_processing(self):
        """Stop stream processing"""
        self.logger.info("Stopping Kafka Streams processing")
        
        self.running = False
        self.shutdown_event.set()
        
        # Wait for consumer threads to finish
        for topic, thread in self.consumer_threads.items():
            thread.join(timeout=30)
            if thread.is_alive():
                self.logger.warning(f"Consumer thread for {topic} did not shutdown gracefully")
        
        # Cleanup processors
        for processor in self.processors.values():
            processor.cleanup()
            
        if self.producer:
            self.producer.flush()
            
        self.logger.info("Kafka Streams processing stopped")
    
    def get_processing_metrics(self) -> Dict[str, Any]:
        """Get processing metrics"""
        return {
            "running": self.running,
            "active_threads": len([t for t in self.consumer_threads.values() if t.is_alive()]),
            "processors": list(self.processors.keys()),
            "timestamp": datetime.now().isoformat()
        }


# Factory functions
def create_enhanced_kafka_streams_processor(
    bootstrap_servers: List[str],
    application_id: str,
    redis_client: Optional[redis.Redis] = None,
    rabbitmq_channel: Optional[pika.channel.Channel] = None
) -> EnhancedKafkaStreamsProcessor:
    """Create enhanced Kafka Streams processor"""
    return EnhancedKafkaStreamsProcessor(
        bootstrap_servers=bootstrap_servers,
        application_id=application_id,
        redis_client=redis_client,
        rabbitmq_channel=rabbitmq_channel
    )


# Example usage
if __name__ == "__main__":
    import time
    
    try:
        print("Testing Enhanced Kafka Streams Processor...")
        
        # Create processor
        processor = create_enhanced_kafka_streams_processor(
            bootstrap_servers=["localhost:9092"],
            application_id="enhanced-streams-test"
        )
        
        # Define topic to processor mapping
        topic_mapping = {
            "event-store": "event_sourcing",
            "commands": "cqrs",
            "events": "cqrs", 
            "queries": "cqrs",
            "saga-orchestration": "saga"
        }
        
        # Start processing
        processor.start_processing(topic_mapping)
        
        print("✅ Enhanced Kafka Streams processor started")
        
        # Let it run for a moment
        time.sleep(10)
        
        # Get metrics
        metrics = processor.get_processing_metrics()
        print(f"✅ Processing metrics: {metrics}")
        
        # Stop processing
        processor.stop_processing()
        
        print("✅ Enhanced Kafka Streams testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()