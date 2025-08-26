"""
Distributed Tracing for Message Flows
Provides comprehensive tracing for RabbitMQ and Kafka message processing
"""
import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import Status, StatusCode

from core.logging import get_logger


class MessageOperationType(Enum):
    """Types of message operations"""
    PUBLISH = "publish"
    CONSUME = "consume" 
    PROCESS = "process"
    ACK = "acknowledge"
    NACK = "negative_acknowledge"
    DEAD_LETTER = "dead_letter"
    RETRY = "retry"


@dataclass
class MessageTraceContext:
    """Message tracing context"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    correlation_id: Optional[str] = None
    message_id: Optional[str] = None
    operation_type: Optional[MessageOperationType] = None
    component: str = "messaging"
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class MessageTracer:
    """Distributed tracing for messaging systems"""
    
    def __init__(self, service_name: str = "messaging-system"):
        self.service_name = service_name
        self.tracer = trace.get_tracer(__name__)
        self.logger = get_logger(__name__)
        
        # Active traces
        self.active_traces: Dict[str, MessageTraceContext] = {}
        
    @contextmanager
    def trace_message_operation(
        self,
        operation_type: MessageOperationType,
        component: str,
        message_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        queue_or_topic: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None
    ):
        """Context manager for tracing message operations"""
        
        # Create span name
        span_name = f"{component}.{operation_type.value}"
        if queue_or_topic:
            span_name += f".{queue_or_topic}"
        
        # Extract context from headers if available
        context = Context()
        if headers:
            context = extract(headers)
        
        with self.tracer.start_as_current_span(
            span_name,
            context=context,
            kind=trace.SpanKind.PRODUCER if operation_type == MessageOperationType.PUBLISH else trace.SpanKind.CONSUMER
        ) as span:
            
            # Set span attributes
            span.set_attribute("messaging.system", component)
            span.set_attribute("messaging.operation", operation_type.value)
            span.set_attribute("service.name", self.service_name)
            
            if message_id:
                span.set_attribute("messaging.message_id", message_id)
            if correlation_id:
                span.set_attribute("messaging.correlation_id", correlation_id)
            if queue_or_topic:
                if component == "rabbitmq":
                    span.set_attribute("messaging.rabbitmq.queue", queue_or_topic)
                elif component == "kafka":
                    span.set_attribute("messaging.kafka.topic", queue_or_topic)
            
            # Create trace context
            trace_context = MessageTraceContext(
                trace_id=format(span.get_span_context().trace_id, '032x'),
                span_id=format(span.get_span_context().span_id, '016x'),
                correlation_id=correlation_id,
                message_id=message_id,
                operation_type=operation_type,
                component=component
            )
            
            # Store in active traces
            self.active_traces[trace_context.span_id] = trace_context
            
            try:
                yield trace_context
                
                # Mark as successful
                span.set_status(Status(StatusCode.OK))
                trace_context.metadata["status"] = "success"
                
            except Exception as e:
                # Record exception
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                trace_context.metadata["status"] = "error"
                trace_context.metadata["error"] = str(e)
                
                self.logger.error(f"Message operation failed: {operation_type.value} - {str(e)}")
                raise
                
            finally:
                # Remove from active traces
                self.active_traces.pop(trace_context.span_id, None)
    
    @asynccontextmanager
    async def trace_async_message_operation(
        self,
        operation_type: MessageOperationType,
        component: str,
        message_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        queue_or_topic: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None
    ):
        """Async context manager for tracing message operations"""
        
        # Create span name
        span_name = f"{component}.{operation_type.value}"
        if queue_or_topic:
            span_name += f".{queue_or_topic}"
        
        # Extract context from headers if available
        context = Context()
        if headers:
            context = extract(headers)
        
        with self.tracer.start_as_current_span(
            span_name,
            context=context,
            kind=trace.SpanKind.PRODUCER if operation_type == MessageOperationType.PUBLISH else trace.SpanKind.CONSUMER
        ) as span:
            
            # Set span attributes
            span.set_attribute("messaging.system", component)
            span.set_attribute("messaging.operation", operation_type.value)
            span.set_attribute("service.name", self.service_name)
            
            if message_id:
                span.set_attribute("messaging.message_id", message_id)
            if correlation_id:
                span.set_attribute("messaging.correlation_id", correlation_id)
            if queue_or_topic:
                if component == "rabbitmq":
                    span.set_attribute("messaging.rabbitmq.queue", queue_or_topic)
                elif component == "kafka":
                    span.set_attribute("messaging.kafka.topic", queue_or_topic)
            
            # Create trace context
            trace_context = MessageTraceContext(
                trace_id=format(span.get_span_context().trace_id, '032x'),
                span_id=format(span.get_span_context().span_id, '016x'),
                correlation_id=correlation_id,
                message_id=message_id,
                operation_type=operation_type,
                component=component
            )
            
            # Store in active traces
            self.active_traces[trace_context.span_id] = trace_context
            
            try:
                yield trace_context
                
                # Mark as successful
                span.set_status(Status(StatusCode.OK))
                trace_context.metadata["status"] = "success"
                
            except Exception as e:
                # Record exception
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                trace_context.metadata["status"] = "error"
                trace_context.metadata["error"] = str(e)
                
                self.logger.error(f"Async message operation failed: {operation_type.value} - {str(e)}")
                raise
                
            finally:
                # Remove from active traces
                self.active_traces.pop(trace_context.span_id, None)
    
    def inject_trace_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Inject tracing headers into message headers"""
        if headers is None:
            headers = {}
        
        # Inject OpenTelemetry context
        inject(headers)
        
        return headers
    
    def extract_trace_context(self, headers: Dict[str, str]) -> Optional[Context]:
        """Extract tracing context from message headers"""
        if not headers:
            return None
        
        return extract(headers)
    
    def add_span_event(self, span_id: str, event_name: str, attributes: Dict[str, Any] = None):
        """Add an event to an active span"""
        if span_id in self.active_traces:
            # This would need to be integrated with the actual span
            # For now, just log and store in metadata
            trace_context = self.active_traces[span_id]
            if "events" not in trace_context.metadata:
                trace_context.metadata["events"] = []
            
            trace_context.metadata["events"].append({
                "name": event_name,
                "timestamp": datetime.now().isoformat(),
                "attributes": attributes or {}
            })
            
            self.logger.debug(f"Added span event: {event_name} for span {span_id}")
    
    def get_active_traces(self) -> Dict[str, MessageTraceContext]:
        """Get all active traces"""
        return self.active_traces.copy()
    
    def get_trace_summary(self) -> Dict[str, Any]:
        """Get summary of tracing activity"""
        return {
            "active_traces": len(self.active_traces),
            "service_name": self.service_name,
            "timestamp": datetime.now().isoformat()
        }


class RabbitMQTracer(MessageTracer):
    """Specialized tracer for RabbitMQ operations"""
    
    def __init__(self, service_name: str = "rabbitmq-messaging"):
        super().__init__(service_name)
    
    @contextmanager
    def trace_publish(
        self,
        queue_name: str,
        message_id: str,
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None
    ):
        """Trace message publishing to RabbitMQ"""
        with self.trace_message_operation(
            MessageOperationType.PUBLISH,
            "rabbitmq",
            message_id=message_id,
            correlation_id=correlation_id,
            queue_or_topic=queue_name,
            headers=headers
        ) as trace_context:
            yield trace_context
    
    @contextmanager  
    def trace_consume(
        self,
        queue_name: str,
        message_id: str,
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None
    ):
        """Trace message consumption from RabbitMQ"""
        with self.trace_message_operation(
            MessageOperationType.CONSUME,
            "rabbitmq",
            message_id=message_id,
            correlation_id=correlation_id,
            queue_or_topic=queue_name,
            headers=headers
        ) as trace_context:
            yield trace_context
    
    @contextmanager
    def trace_process(
        self,
        queue_name: str,
        message_id: str,
        processor_name: str,
        correlation_id: Optional[str] = None
    ):
        """Trace message processing"""
        with self.trace_message_operation(
            MessageOperationType.PROCESS,
            "rabbitmq",
            message_id=message_id,
            correlation_id=correlation_id,
            queue_or_topic=queue_name
        ) as trace_context:
            trace_context.metadata["processor"] = processor_name
            yield trace_context


class KafkaTracer(MessageTracer):
    """Specialized tracer for Kafka operations"""
    
    def __init__(self, service_name: str = "kafka-messaging"):
        super().__init__(service_name)
    
    @contextmanager
    def trace_produce(
        self,
        topic_name: str,
        message_id: str,
        partition: Optional[int] = None,
        key: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None
    ):
        """Trace message production to Kafka"""
        with self.trace_message_operation(
            MessageOperationType.PUBLISH,
            "kafka", 
            message_id=message_id,
            queue_or_topic=topic_name,
            headers=headers
        ) as trace_context:
            if partition is not None:
                trace_context.metadata["partition"] = partition
            if key:
                trace_context.metadata["key"] = key
            yield trace_context
    
    @contextmanager
    def trace_consume(
        self,
        topic_name: str,
        message_id: str,
        partition: int,
        offset: int,
        consumer_group: str,
        headers: Optional[Dict[str, Any]] = None
    ):
        """Trace message consumption from Kafka"""
        with self.trace_message_operation(
            MessageOperationType.CONSUME,
            "kafka",
            message_id=message_id,
            queue_or_topic=topic_name,
            headers=headers
        ) as trace_context:
            trace_context.metadata.update({
                "partition": partition,
                "offset": offset,
                "consumer_group": consumer_group
            })
            yield trace_context
    
    @asynccontextmanager
    async def trace_async_consume(
        self,
        topic_name: str, 
        message_id: str,
        partition: int,
        offset: int,
        consumer_group: str,
        headers: Optional[Dict[str, Any]] = None
    ):
        """Trace async message consumption from Kafka"""
        async with self.trace_async_message_operation(
            MessageOperationType.CONSUME,
            "kafka",
            message_id=message_id,
            queue_or_topic=topic_name,
            headers=headers
        ) as trace_context:
            trace_context.metadata.update({
                "partition": partition,
                "offset": offset,
                "consumer_group": consumer_group
            })
            yield trace_context


class MessageFlowTracer:
    """Traces complete message flows across systems"""
    
    def __init__(self):
        self.rabbitmq_tracer = RabbitMQTracer()
        self.kafka_tracer = KafkaTracer()
        self.logger = get_logger(__name__)
        
        # Flow tracking
        self.active_flows: Dict[str, Dict[str, Any]] = {}
    
    def start_flow(self, flow_id: str, flow_name: str, metadata: Dict[str, Any] = None) -> str:
        """Start tracking a message flow"""
        self.active_flows[flow_id] = {
            "flow_name": flow_name,
            "start_time": datetime.now(),
            "steps": [],
            "metadata": metadata or {},
            "status": "active"
        }
        
        self.logger.info(f"Started message flow: {flow_name} ({flow_id})")
        return flow_id
    
    def add_flow_step(
        self,
        flow_id: str,
        step_name: str,
        component: str,
        operation_type: MessageOperationType,
        message_id: Optional[str] = None,
        metadata: Dict[str, Any] = None
    ):
        """Add a step to a message flow"""
        if flow_id in self.active_flows:
            step = {
                "step_name": step_name,
                "component": component,
                "operation_type": operation_type.value,
                "message_id": message_id,
                "timestamp": datetime.now(),
                "metadata": metadata or {}
            }
            
            self.active_flows[flow_id]["steps"].append(step)
            self.logger.debug(f"Added step to flow {flow_id}: {step_name}")
    
    def complete_flow(self, flow_id: str, status: str = "completed"):
        """Complete a message flow"""
        if flow_id in self.active_flows:
            flow = self.active_flows[flow_id]
            flow["status"] = status
            flow["end_time"] = datetime.now()
            flow["duration_ms"] = (flow["end_time"] - flow["start_time"]).total_seconds() * 1000
            
            self.logger.info(f"Completed message flow: {flow['flow_name']} ({flow_id}) - {status}")
    
    def get_flow_details(self, flow_id: str) -> Optional[Dict[str, Any]]:
        """Get details of a specific flow"""
        return self.active_flows.get(flow_id)
    
    def get_active_flows(self) -> Dict[str, Dict[str, Any]]:
        """Get all active flows"""
        return {
            flow_id: flow for flow_id, flow in self.active_flows.items()
            if flow["status"] == "active"
        }
    
    def get_flow_metrics(self) -> Dict[str, Any]:
        """Get metrics about message flows"""
        total_flows = len(self.active_flows)
        active_flows = len([f for f in self.active_flows.values() if f["status"] == "active"])
        completed_flows = len([f for f in self.active_flows.values() if f["status"] == "completed"])
        failed_flows = len([f for f in self.active_flows.values() if f["status"] == "failed"])
        
        # Calculate average duration for completed flows
        completed_durations = [
            f["duration_ms"] for f in self.active_flows.values()
            if f["status"] == "completed" and "duration_ms" in f
        ]
        
        avg_duration = sum(completed_durations) / len(completed_durations) if completed_durations else 0
        
        return {
            "total_flows": total_flows,
            "active_flows": active_flows,
            "completed_flows": completed_flows,
            "failed_flows": failed_flows,
            "average_duration_ms": avg_duration,
            "timestamp": datetime.now().isoformat()
        }


# Global message tracer instances
_rabbitmq_tracer: Optional[RabbitMQTracer] = None
_kafka_tracer: Optional[KafkaTracer] = None
_message_flow_tracer: Optional[MessageFlowTracer] = None


def get_rabbitmq_tracer() -> RabbitMQTracer:
    """Get global RabbitMQ tracer"""
    global _rabbitmq_tracer
    if _rabbitmq_tracer is None:
        _rabbitmq_tracer = RabbitMQTracer()
    return _rabbitmq_tracer


def get_kafka_tracer() -> KafkaTracer:
    """Get global Kafka tracer"""
    global _kafka_tracer
    if _kafka_tracer is None:
        _kafka_tracer = KafkaTracer()
    return _kafka_tracer


def get_message_flow_tracer() -> MessageFlowTracer:
    """Get global message flow tracer"""
    global _message_flow_tracer
    if _message_flow_tracer is None:
        _message_flow_tracer = MessageFlowTracer()
    return _message_flow_tracer


# Decorator functions for automatic tracing
def trace_rabbitmq_publish(queue_name: str):
    """Decorator for tracing RabbitMQ message publishing"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            message_id = kwargs.get('message_id') or str(uuid.uuid4())
            correlation_id = kwargs.get('correlation_id')
            headers = kwargs.get('headers', {})
            
            tracer = get_rabbitmq_tracer()
            with tracer.trace_publish(queue_name, message_id, correlation_id, headers):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def trace_kafka_produce(topic_name: str):
    """Decorator for tracing Kafka message production"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            message_id = kwargs.get('message_id') or str(uuid.uuid4())
            headers = kwargs.get('headers', {})
            partition = kwargs.get('partition')
            key = kwargs.get('key')
            
            tracer = get_kafka_tracer()
            with tracer.trace_produce(topic_name, message_id, partition, key, headers):
                return func(*args, **kwargs)
        return wrapper
    return decorator