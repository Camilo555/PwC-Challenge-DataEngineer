"""
Advanced RabbitMQ Message Patterns

This module implements comprehensive messaging patterns including:
- Publisher/Subscriber for event broadcasting
- Request/Response for RPC calls
- Work queues for task distribution  
- Topic exchanges for routing
- Priority queues for critical messages
"""

import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, AsyncCallable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, Future
import threading

import aio_pika
import pika
from pika.exceptions import AMQPConnectionError

from .enterprise_rabbitmq_manager import (
    EnterpriseRabbitMQManager, EnterpriseMessage, MessageMetadata, 
    QueueType, MessagePriority, get_rabbitmq_manager
)
from core.logging import get_logger


class PatternType(Enum):
    """Message pattern types"""
    PUBLISH_SUBSCRIBE = "pub_sub"
    REQUEST_RESPONSE = "req_resp" 
    WORK_QUEUE = "work_queue"
    TOPIC_ROUTING = "topic"
    PRIORITY_QUEUE = "priority"
    DELAYED_MESSAGE = "delayed"


@dataclass
class RPCRequest:
    """RPC request structure"""
    method: str
    params: Dict[str, Any]
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timeout: int = 30
    reply_to: Optional[str] = None
    correlation_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "params": self.params,
            "request_id": self.request_id,
            "timeout": self.timeout,
            "timestamp": datetime.utcnow().isoformat()
        }


@dataclass 
class RPCResponse:
    """RPC response structure"""
    request_id: str
    result: Any = None
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "result": self.result,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat()
        }


class MessagePattern(ABC):
    """Base class for message patterns"""
    
    def __init__(self, rabbitmq_manager: EnterpriseRabbitMQManager):
        self.rabbitmq = rabbitmq_manager
        self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")
        self.is_running = False
        
    @abstractmethod
    def start(self):
        """Start the message pattern"""
        pass
        
    @abstractmethod  
    def stop(self):
        """Stop the message pattern"""
        pass


class PublisherSubscriber(MessagePattern):
    """
    Publisher/Subscriber pattern for event broadcasting
    Supports multiple subscribers receiving the same message
    """
    
    def __init__(self, rabbitmq_manager: EnterpriseRabbitMQManager):
        super().__init__(rabbitmq_manager)
        self.subscribers: Dict[str, Callable] = {}
        self.async_subscribers: Dict[str, AsyncCallable] = {}
        self.subscription_threads: List[threading.Thread] = []
        self.exchange_name = "pub_sub_events"
        
    def publish(
        self,
        event_type: str, 
        data: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> bool:
        """Publish event to all subscribers"""
        try:
            message = EnterpriseMessage(
                queue_type=QueueType.STREAMING_EVENTS,
                message_type=event_type,
                payload=data,
                metadata=MessageMetadata(
                    priority=priority,
                    headers={"pattern": "pub_sub", "event_type": event_type}
                )
            )
            
            # Use fanout exchange for broadcasting
            return self.rabbitmq.publish_message(
                message=message,
                exchange_name=self.exchange_name,
                routing_key=""  # Fanout ignores routing key
            )
            
        except Exception as e:
            self.logger.error(f"Failed to publish event {event_type}: {e}")
            return False
    
    async def publish_async(
        self,
        event_type: str,
        data: Dict[str, Any], 
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> bool:
        """Async publish event to all subscribers"""
        try:
            message = EnterpriseMessage(
                queue_type=QueueType.STREAMING_EVENTS,
                message_type=event_type,
                payload=data,
                metadata=MessageMetadata(
                    priority=priority,
                    headers={"pattern": "pub_sub", "event_type": event_type}
                )
            )
            
            return await self.rabbitmq.publish_message_async(
                message=message,
                exchange_name=self.exchange_name,
                routing_key=""
            )
            
        except Exception as e:
            self.logger.error(f"Failed to async publish event {event_type}: {e}")
            return False
    
    def subscribe(
        self, 
        event_types: List[str],
        callback: Callable[[str, Dict[str, Any]], None],
        subscriber_id: Optional[str] = None
    ) -> str:
        """Subscribe to specific event types"""
        if not subscriber_id:
            subscriber_id = f"subscriber_{uuid.uuid4().hex[:8]}"
            
        self.subscribers[subscriber_id] = callback
        
        # Create exclusive queue for this subscriber
        queue_name = f"pub_sub_{subscriber_id}"
        
        def consume_messages():
            try:
                with self.rabbitmq.connection_pool.get_connection() as connection:
                    channel = connection.channel()
                    
                    # Declare fanout exchange
                    channel.exchange_declare(
                        exchange=self.exchange_name,
                        exchange_type="fanout",
                        durable=True
                    )
                    
                    # Declare exclusive queue
                    channel.queue_declare(queue=queue_name, durable=False, auto_delete=True)
                    
                    # Bind queue to exchange
                    channel.queue_bind(exchange=self.exchange_name, queue=queue_name)
                    
                    def process_message(ch, method, properties, body):
                        try:
                            data = json.loads(body.decode())
                            message = EnterpriseMessage(**data)
                            
                            # Filter by event types if specified
                            if not event_types or message.message_type in event_types:
                                callback(message.message_type, message.payload)
                                
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing pub/sub message: {e}")
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    
                    channel.basic_consume(queue=queue_name, on_message_callback=process_message)
                    
                    self.logger.info(f"Started subscriber {subscriber_id} for events: {event_types}")
                    channel.start_consuming()
                    
            except Exception as e:
                self.logger.error(f"Error in subscriber {subscriber_id}: {e}")
        
        # Start consumer thread
        thread = threading.Thread(target=consume_messages, daemon=True)
        thread.start()
        self.subscription_threads.append(thread)
        
        return subscriber_id
    
    async def subscribe_async(
        self,
        event_types: List[str],
        callback: AsyncCallable[[str, Dict[str, Any]], None],
        subscriber_id: Optional[str] = None
    ) -> str:
        """Async subscribe to specific event types"""
        if not subscriber_id:
            subscriber_id = f"async_subscriber_{uuid.uuid4().hex[:8]}"
            
        self.async_subscribers[subscriber_id] = callback
        
        if not await self.rabbitmq.initialize_async():
            raise Exception("Failed to initialize async connection")
        
        channel = await self.rabbitmq.async_connection.channel()
        
        # Declare fanout exchange
        exchange = await channel.declare_exchange(
            self.exchange_name, 
            aio_pika.ExchangeType.FANOUT,
            durable=True
        )
        
        # Create exclusive queue
        queue_name = f"async_pub_sub_{subscriber_id}"
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        
        # Bind queue to exchange
        await queue.bind(exchange)
        
        async def process_message(message):
            async with message.process():
                try:
                    data = json.loads(message.body.decode())
                    msg = EnterpriseMessage(**data)
                    
                    # Filter by event types if specified
                    if not event_types or msg.message_type in event_types:
                        await callback(msg.message_type, msg.payload)
                        
                except Exception as e:
                    self.logger.error(f"Error processing async pub/sub message: {e}")
                    raise
        
        await queue.consume(process_message)
        self.logger.info(f"Started async subscriber {subscriber_id} for events: {event_types}")
        
        return subscriber_id
    
    def unsubscribe(self, subscriber_id: str):
        """Unsubscribe from events"""
        if subscriber_id in self.subscribers:
            del self.subscribers[subscriber_id]
        if subscriber_id in self.async_subscribers:
            del self.async_subscribers[subscriber_id]
            
    def start(self):
        """Start publisher/subscriber pattern"""
        self.is_running = True
        self.logger.info("Publisher/Subscriber pattern started")
        
    def stop(self):
        """Stop publisher/subscriber pattern"""
        self.is_running = False
        self.subscribers.clear()
        self.async_subscribers.clear()
        self.logger.info("Publisher/Subscriber pattern stopped")


class RequestResponse(MessagePattern):
    """
    Request/Response pattern for RPC-style communication
    Supports timeouts and correlation tracking
    """
    
    def __init__(self, rabbitmq_manager: EnterpriseRabbitMQManager):
        super().__init__(rabbitmq_manager)
        self.pending_requests: Dict[str, Future] = {}
        self.rpc_handlers: Dict[str, Callable] = {}
        self.response_consumers = {}
        self.request_queue = QueueType.TASK_QUEUE.value
        self.response_queue_prefix = "rpc_response"
        
    def call(
        self,
        method: str,
        params: Dict[str, Any],
        timeout: int = 30,
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> Any:
        """Make synchronous RPC call"""
        request = RPCRequest(method=method, params=params, timeout=timeout)
        
        # Create response queue
        response_queue = f"{self.response_queue_prefix}_{request.request_id}"
        request.reply_to = response_queue
        request.correlation_id = request.request_id
        
        # Create future for response
        response_future = Future()
        self.pending_requests[request.request_id] = response_future
        
        try:
            # Set up response consumer
            self._setup_response_consumer(response_queue, request.request_id)
            
            # Send request
            message = EnterpriseMessage(
                queue_type=QueueType.TASK_QUEUE,
                message_type="rpc_request",
                payload=request.to_dict(),
                metadata=MessageMetadata(
                    correlation_id=request.correlation_id,
                    reply_to=response_queue,
                    priority=priority,
                    expiration=timeout,
                    headers={"pattern": "rpc", "method": method}
                )
            )
            
            if not self.rabbitmq.publish_message(message, routing_key="task"):
                raise Exception("Failed to send RPC request")
            
            # Wait for response
            try:
                response_data = response_future.result(timeout=timeout)
                response = RPCResponse(**response_data)
                
                if response.error:
                    raise Exception(f"RPC Error: {response.error}")
                    
                return response.result
                
            except asyncio.TimeoutError:
                raise TimeoutError(f"RPC call timed out after {timeout} seconds")
                
        finally:
            # Cleanup
            if request.request_id in self.pending_requests:
                del self.pending_requests[request.request_id]
                
            self._cleanup_response_consumer(response_queue)
    
    async def call_async(
        self,
        method: str,
        params: Dict[str, Any],
        timeout: int = 30,
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> Any:
        """Make asynchronous RPC call"""
        request = RPCRequest(method=method, params=params, timeout=timeout)
        
        response_queue = f"{self.response_queue_prefix}_{request.request_id}"
        request.reply_to = response_queue
        request.correlation_id = request.request_id
        
        try:
            if not await self.rabbitmq.initialize_async():
                raise Exception("Failed to initialize async connection")
            
            channel = await self.rabbitmq.async_connection.channel()
            
            # Declare response queue
            response_q = await channel.declare_queue(
                response_queue,
                auto_delete=True,
                arguments={'x-message-ttl': timeout * 1000}
            )
            
            # Create response future
            response_future = asyncio.Future()
            
            async def handle_response(message):
                async with message.process():
                    try:
                        data = json.loads(message.body.decode())
                        if not response_future.done():
                            response_future.set_result(data)
                    except Exception as e:
                        if not response_future.done():
                            response_future.set_exception(e)
            
            await response_q.consume(handle_response, no_ack=False)
            
            # Send request
            message = EnterpriseMessage(
                queue_type=QueueType.TASK_QUEUE,
                message_type="rpc_request",
                payload=request.to_dict(),
                metadata=MessageMetadata(
                    correlation_id=request.correlation_id,
                    reply_to=response_queue,
                    priority=priority,
                    expiration=timeout,
                    headers={"pattern": "rpc", "method": method}
                )
            )
            
            if not await self.rabbitmq.publish_message_async(message, routing_key="task"):
                raise Exception("Failed to send async RPC request")
            
            # Wait for response with timeout
            try:
                response_data = await asyncio.wait_for(response_future, timeout=timeout)
                response = RPCResponse(**response_data)
                
                if response.error:
                    raise Exception(f"Async RPC Error: {response.error}")
                    
                return response.result
                
            except asyncio.TimeoutError:
                raise TimeoutError(f"Async RPC call timed out after {timeout} seconds")
                
        finally:
            await channel.close()
    
    def _setup_response_consumer(self, queue_name: str, request_id: str):
        """Set up response consumer for RPC call"""
        def consume_response():
            try:
                with self.rabbitmq.connection_pool.get_connection() as connection:
                    channel = connection.channel()
                    
                    # Declare temporary response queue
                    channel.queue_declare(
                        queue=queue_name,
                        auto_delete=True,
                        arguments={'x-message-ttl': 60000}  # 1 minute TTL
                    )
                    
                    def handle_response(ch, method, properties, body):
                        try:
                            data = json.loads(body.decode())
                            
                            if request_id in self.pending_requests:
                                future = self.pending_requests[request_id]
                                if not future.done():
                                    future.set_result(data)
                                    
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            ch.stop_consuming()
                            
                        except Exception as e:
                            self.logger.error(f"Error handling RPC response: {e}")
                            if request_id in self.pending_requests:
                                future = self.pending_requests[request_id]
                                if not future.done():
                                    future.set_exception(e)
                    
                    channel.basic_consume(queue=queue_name, on_message_callback=handle_response)
                    channel.start_consuming()
                    
            except Exception as e:
                self.logger.error(f"Error in response consumer: {e}")
                if request_id in self.pending_requests:
                    future = self.pending_requests[request_id]
                    if not future.done():
                        future.set_exception(e)
        
        # Start consumer thread
        thread = threading.Thread(target=consume_response, daemon=True)
        thread.start()
        self.response_consumers[queue_name] = thread
    
    def _cleanup_response_consumer(self, queue_name: str):
        """Clean up response consumer"""
        if queue_name in self.response_consumers:
            del self.response_consumers[queue_name]
    
    def register_handler(
        self,
        method: str,
        handler: Callable[[Dict[str, Any]], Any]
    ):
        """Register RPC method handler"""
        self.rpc_handlers[method] = handler
        self.logger.info(f"Registered RPC handler for method: {method}")
    
    def start_server(self, max_workers: int = 5):
        """Start RPC server to handle requests"""
        def process_requests():
            try:
                with self.rabbitmq.connection_pool.get_connection() as connection:
                    channel = connection.channel()
                    channel.basic_qos(prefetch_count=max_workers)
                    
                    def handle_request(ch, method, properties, body):
                        try:
                            data = json.loads(body.decode())
                            message = EnterpriseMessage(**data)
                            
                            if message.message_type == "rpc_request":
                                request_data = message.payload
                                method_name = request_data.get("method")
                                params = request_data.get("params", {})
                                request_id = request_data.get("request_id")
                                
                                start_time = time.time()
                                
                                if method_name in self.rpc_handlers:
                                    try:
                                        result = self.rpc_handlers[method_name](params)
                                        execution_time = (time.time() - start_time) * 1000
                                        
                                        response = RPCResponse(
                                            request_id=request_id,
                                            result=result,
                                            execution_time_ms=execution_time
                                        )
                                        
                                    except Exception as handler_error:
                                        execution_time = (time.time() - start_time) * 1000
                                        response = RPCResponse(
                                            request_id=request_id,
                                            error=str(handler_error),
                                            execution_time_ms=execution_time
                                        )
                                else:
                                    response = RPCResponse(
                                        request_id=request_id,
                                        error=f"Unknown method: {method_name}"
                                    )
                                
                                # Send response
                                if properties.reply_to:
                                    ch.basic_publish(
                                        exchange='',
                                        routing_key=properties.reply_to,
                                        body=json.dumps(response.to_dict(), default=str),
                                        properties=pika.BasicProperties(
                                            correlation_id=properties.correlation_id
                                        )
                                    )
                            
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing RPC request: {e}")
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    
                    channel.basic_consume(
                        queue=self.request_queue,
                        on_message_callback=handle_request
                    )
                    
                    self.logger.info(f"RPC server started with {max_workers} workers")
                    channel.start_consuming()
                    
            except Exception as e:
                self.logger.error(f"RPC server error: {e}")
        
        # Start server thread
        server_thread = threading.Thread(target=process_requests, daemon=True)
        server_thread.start()
        
        return server_thread
    
    def start(self):
        """Start request/response pattern"""
        self.is_running = True
        self.logger.info("Request/Response pattern started")
        
    def stop(self):
        """Stop request/response pattern"""
        self.is_running = False
        
        # Cancel pending requests
        for request_id, future in self.pending_requests.items():
            if not future.done():
                future.cancel()
                
        self.pending_requests.clear()
        self.rpc_handlers.clear()
        self.logger.info("Request/Response pattern stopped")


class WorkQueue(MessagePattern):
    """
    Work Queue pattern for load-balanced task distribution
    Multiple workers compete for tasks from the same queue
    """
    
    def __init__(self, rabbitmq_manager: EnterpriseRabbitMQManager):
        super().__init__(rabbitmq_manager)
        self.workers: List[threading.Thread] = []
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.task_handlers: Dict[str, Callable] = {}
        
    def add_task(
        self,
        task_type: str,
        task_data: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL,
        delay_seconds: Optional[int] = None
    ) -> str:
        """Add task to work queue"""
        try:
            task_id = str(uuid.uuid4())
            
            message = EnterpriseMessage(
                queue_type=QueueType.TASK_QUEUE,
                message_type=task_type,
                payload={
                    "task_id": task_id,
                    "task_type": task_type,
                    "data": task_data,
                    "created_at": datetime.utcnow().isoformat()
                },
                metadata=MessageMetadata(
                    message_id=task_id,
                    priority=priority,
                    expiration=delay_seconds,
                    headers={"pattern": "work_queue", "task_type": task_type}
                )
            )
            
            exchange_name = "rpc" if not delay_seconds else "delayed"
            routing_key = "task"
            
            success = self.rabbitmq.publish_message(
                message=message,
                exchange_name=exchange_name,
                routing_key=routing_key
            )
            
            if success:
                self.logger.debug(f"Added task {task_id} of type {task_type} to work queue")
                return task_id
            else:
                raise Exception("Failed to publish task")
                
        except Exception as e:
            self.logger.error(f"Failed to add task to work queue: {e}")
            raise
    
    async def add_task_async(
        self,
        task_type: str,
        task_data: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL,
        delay_seconds: Optional[int] = None
    ) -> str:
        """Add task to work queue asynchronously"""
        try:
            task_id = str(uuid.uuid4())
            
            message = EnterpriseMessage(
                queue_type=QueueType.TASK_QUEUE,
                message_type=task_type,
                payload={
                    "task_id": task_id,
                    "task_type": task_type, 
                    "data": task_data,
                    "created_at": datetime.utcnow().isoformat()
                },
                metadata=MessageMetadata(
                    message_id=task_id,
                    priority=priority,
                    expiration=delay_seconds,
                    headers={"pattern": "work_queue", "task_type": task_type}
                )
            )
            
            exchange_name = "rpc" if not delay_seconds else "delayed"
            routing_key = "task"
            
            success = await self.rabbitmq.publish_message_async(
                message=message,
                exchange_name=exchange_name,
                routing_key=routing_key
            )
            
            if success:
                self.logger.debug(f"Added async task {task_id} of type {task_type} to work queue")
                return task_id
            else:
                raise Exception("Failed to publish async task")
                
        except Exception as e:
            self.logger.error(f"Failed to add async task to work queue: {e}")
            raise
    
    def register_handler(
        self,
        task_type: str,
        handler: Callable[[Dict[str, Any]], Any]
    ):
        """Register task handler"""
        self.task_handlers[task_type] = handler
        self.logger.info(f"Registered handler for task type: {task_type}")
    
    def start_workers(self, num_workers: int = 3, queue_name: Optional[str] = None):
        """Start worker threads"""
        if not queue_name:
            queue_name = QueueType.TASK_QUEUE.value
            
        for i in range(num_workers):
            worker = threading.Thread(
                target=self._worker_process,
                args=(f"worker_{i+1}", queue_name),
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
        
        self.logger.info(f"Started {num_workers} workers for queue {queue_name}")
    
    def _worker_process(self, worker_id: str, queue_name: str):
        """Worker process to handle tasks"""
        try:
            with self.rabbitmq.connection_pool.get_connection() as connection:
                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)  # Fair dispatch
                
                def process_task(ch, method, properties, body):
                    try:
                        data = json.loads(body.decode())
                        message = EnterpriseMessage(**data)
                        
                        task_data = message.payload
                        task_type = task_data.get("task_type")
                        task_id = task_data.get("task_id")
                        
                        self.logger.debug(f"Worker {worker_id} processing task {task_id}")
                        
                        if task_type in self.task_handlers:
                            start_time = time.time()
                            try:
                                result = self.task_handlers[task_type](task_data.get("data", {}))
                                execution_time = (time.time() - start_time) * 1000
                                
                                self.logger.info(
                                    f"Worker {worker_id} completed task {task_id} "
                                    f"in {execution_time:.2f}ms"
                                )
                                
                                # Acknowledge successful processing
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                                
                            except Exception as handler_error:
                                execution_time = (time.time() - start_time) * 1000
                                
                                self.logger.error(
                                    f"Worker {worker_id} failed task {task_id}: {handler_error}"
                                )
                                
                                # Check retry count
                                retry_count = message.retry_count
                                if retry_count < message.max_retries:
                                    # Reject and requeue for retry
                                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                                else:
                                    # Send to dead letter queue
                                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        else:
                            self.logger.warning(
                                f"No handler registered for task type: {task_type}"
                            )
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                            
                    except Exception as e:
                        self.logger.error(f"Worker {worker_id} error processing task: {e}")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                
                channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=process_task
                )
                
                self.logger.info(f"Worker {worker_id} started consuming from {queue_name}")
                channel.start_consuming()
                
        except Exception as e:
            self.logger.error(f"Worker {worker_id} error: {e}")
    
    def start(self):
        """Start work queue pattern"""
        self.is_running = True
        self.logger.info("Work Queue pattern started")
        
    def stop(self):
        """Stop work queue pattern"""
        self.is_running = False
        self.executor.shutdown(wait=True)
        self.task_handlers.clear()
        self.logger.info("Work Queue pattern stopped")


class TopicRouter(MessagePattern):
    """
    Topic-based routing for complex message routing scenarios
    Uses wildcards and patterns for flexible routing
    """
    
    def __init__(self, rabbitmq_manager: EnterpriseRabbitMQManager):
        super().__init__(rabbitmq_manager)
        self.exchange_name = "topic_router"
        self.subscribers: Dict[str, Dict[str, Any]] = {}
        
    def publish(
        self,
        routing_key: str,
        data: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> bool:
        """Publish message with topic routing"""
        try:
            message = EnterpriseMessage(
                queue_type=QueueType.STREAMING_EVENTS,
                message_type="topic_message",
                payload=data,
                metadata=MessageMetadata(
                    priority=priority,
                    headers={"pattern": "topic", "routing_key": routing_key}
                )
            )
            
            return self.rabbitmq.publish_message(
                message=message,
                exchange_name=self.exchange_name,
                routing_key=routing_key
            )
            
        except Exception as e:
            self.logger.error(f"Failed to publish topic message: {e}")
            return False
    
    def subscribe(
        self,
        routing_patterns: List[str],
        callback: Callable[[str, Dict[str, Any]], None],
        subscriber_id: Optional[str] = None
    ) -> str:
        """Subscribe to topics using routing patterns"""
        if not subscriber_id:
            subscriber_id = f"topic_sub_{uuid.uuid4().hex[:8]}"
        
        queue_name = f"topic_{subscriber_id}"
        
        def consume_messages():
            try:
                with self.rabbitmq.connection_pool.get_connection() as connection:
                    channel = connection.channel()
                    
                    # Declare topic exchange
                    channel.exchange_declare(
                        exchange=self.exchange_name,
                        exchange_type="topic",
                        durable=True
                    )
                    
                    # Declare queue
                    channel.queue_declare(queue=queue_name, durable=False, auto_delete=True)
                    
                    # Bind queue with routing patterns
                    for pattern in routing_patterns:
                        channel.queue_bind(
                            exchange=self.exchange_name,
                            queue=queue_name,
                            routing_key=pattern
                        )
                    
                    def process_message(ch, method, properties, body):
                        try:
                            data = json.loads(body.decode())
                            message = EnterpriseMessage(**data)
                            
                            routing_key = method.routing_key
                            callback(routing_key, message.payload)
                            
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing topic message: {e}")
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    
                    channel.basic_consume(queue=queue_name, on_message_callback=process_message)
                    
                    self.logger.info(
                        f"Topic subscriber {subscriber_id} started for patterns: {routing_patterns}"
                    )
                    channel.start_consuming()
                    
            except Exception as e:
                self.logger.error(f"Error in topic subscriber {subscriber_id}: {e}")
        
        # Store subscriber info
        self.subscribers[subscriber_id] = {
            "patterns": routing_patterns,
            "callback": callback,
            "queue": queue_name
        }
        
        # Start consumer thread
        thread = threading.Thread(target=consume_messages, daemon=True)
        thread.start()
        
        return subscriber_id
    
    def unsubscribe(self, subscriber_id: str):
        """Unsubscribe from topics"""
        if subscriber_id in self.subscribers:
            del self.subscribers[subscriber_id]
    
    def start(self):
        """Start topic router pattern"""
        self.is_running = True
        self.logger.info("Topic Router pattern started")
        
    def stop(self):
        """Stop topic router pattern"""
        self.is_running = False
        self.subscribers.clear()
        self.logger.info("Topic Router pattern stopped")


class MessagePatternFactory:
    """Factory for creating message pattern instances"""
    
    @staticmethod
    def create_publisher_subscriber(rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None) -> PublisherSubscriber:
        """Create Publisher/Subscriber pattern"""
        if not rabbitmq_manager:
            rabbitmq_manager = get_rabbitmq_manager()
        return PublisherSubscriber(rabbitmq_manager)
    
    @staticmethod
    def create_request_response(rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None) -> RequestResponse:
        """Create Request/Response pattern"""
        if not rabbitmq_manager:
            rabbitmq_manager = get_rabbitmq_manager()
        return RequestResponse(rabbitmq_manager)
    
    @staticmethod
    def create_work_queue(rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None) -> WorkQueue:
        """Create Work Queue pattern"""
        if not rabbitmq_manager:
            rabbitmq_manager = get_rabbitmq_manager()
        return WorkQueue(rabbitmq_manager)
    
    @staticmethod
    def create_topic_router(rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None) -> TopicRouter:
        """Create Topic Router pattern"""
        if not rabbitmq_manager:
            rabbitmq_manager = get_rabbitmq_manager()
        return TopicRouter(rabbitmq_manager)


# Convenience functions
def get_publisher_subscriber() -> PublisherSubscriber:
    """Get Publisher/Subscriber instance"""
    return MessagePatternFactory.create_publisher_subscriber()


def get_request_response() -> RequestResponse:
    """Get Request/Response instance"""
    return MessagePatternFactory.create_request_response()


def get_work_queue() -> WorkQueue:
    """Get Work Queue instance"""
    return MessagePatternFactory.create_work_queue()


def get_topic_router() -> TopicRouter:
    """Get Topic Router instance"""
    return MessagePatternFactory.create_topic_router()