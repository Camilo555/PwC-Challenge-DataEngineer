"""
RabbitMQ Message Queue Manager
Replaces Redis with RabbitMQ for task queuing and message passing
"""
import json
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, asdict
from enum import Enum
import threading
import time

import pika
import aio_pika
from pika.adapters.asyncio_connection import AsyncioConnection

from core.config.unified_config import get_unified_config
from core.logging import get_logger


class MessagePriority(Enum):
    """Message priority levels"""
    LOW = 1
    NORMAL = 5
    HIGH = 8
    CRITICAL = 10


class QueueType(Enum):
    """Queue type definitions"""
    TASK_QUEUE = "task_queue"
    RESULT_QUEUE = "result_queue"
    EVENT_QUEUE = "event_queue"
    ETL_QUEUE = "etl_queue"
    NOTIFICATION_QUEUE = "notification_queue"
    DEAD_LETTER_QUEUE = "dead_letter_queue"


@dataclass
class TaskMessage:
    """Task message structure"""
    task_id: str
    task_name: str
    payload: Dict[str, Any]
    priority: MessagePriority = MessagePriority.NORMAL
    created_at: datetime = None
    retry_count: int = 0
    max_retries: int = 3
    expires_at: Optional[datetime] = None
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())


@dataclass
class ResultMessage:
    """Result message structure"""
    task_id: str
    correlation_id: str
    status: str  # success, error, timeout
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    processing_time_ms: Optional[float] = None
    completed_at: datetime = None
    
    def __post_init__(self):
        if self.completed_at is None:
            self.completed_at = datetime.now()


class RabbitMQManager:
    """
    Comprehensive RabbitMQ manager for message queuing and task processing
    Replaces Redis with robust message queuing capabilities
    """
    
    def __init__(self):
        self.config = get_unified_config()
        self.logger = get_logger(__name__)
        
        # Connection settings
        self.host = os.getenv("RABBITMQ_HOST", "localhost")
        self.port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.username = os.getenv("RABBITMQ_USERNAME", "guest")
        self.password = os.getenv("RABBITMQ_PASSWORD", "guest")
        self.virtual_host = os.getenv("RABBITMQ_VHOST", "/")
        
        # Connection management
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.async_connection: Optional[aio_pika.Connection] = None
        
        # Queue configuration
        self.queues_declared = set()
        self.consumers = {}
        
        # Task tracking
        self.active_tasks: Dict[str, TaskMessage] = {}
        self.task_results: Dict[str, ResultMessage] = {}
        
        self._setup_connection_params()
        self.logger.info("RabbitMQManager initialized")
    
    def _setup_connection_params(self):
        """Setup connection parameters"""
        self.connection_params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=pika.PlainCredentials(self.username, self.password),
            heartbeat=600,
            blocked_connection_timeout=300,
            connection_attempts=3,
            retry_delay=2.0
        )
    
    def connect(self) -> bool:
        """Establish connection to RabbitMQ"""
        try:
            if self.connection and not self.connection.is_closed:
                return True
            
            self.connection = pika.BlockingConnection(self.connection_params)
            self.channel = self.connection.channel()
            
            # Declare standard queues
            self._declare_standard_queues()
            
            self.logger.info("Connected to RabbitMQ successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            return False
    
    async def connect_async(self) -> bool:
        """Establish async connection to RabbitMQ"""
        try:
            if self.async_connection and not self.async_connection.is_closed:
                return True
            
            self.async_connection = await aio_pika.connect_robust(
                f"amqp://{self.username}:{self.password}@{self.host}:{self.port}{self.virtual_host}",
                heartbeat=600
            )
            
            self.logger.info("Connected to RabbitMQ (async) successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ (async): {str(e)}")
            return False
    
    def _declare_standard_queues(self):
        """Declare standard queues with appropriate settings"""
        
        # Dead letter exchange and queue
        self.channel.exchange_declare(
            exchange='dead_letter_exchange',
            exchange_type='direct',
            durable=True
        )
        
        self.channel.queue_declare(
            queue='dead_letter_queue',
            durable=True,
            arguments={'x-message-ttl': 86400000}  # 24 hours
        )
        
        self.channel.queue_bind(
            exchange='dead_letter_exchange',
            queue='dead_letter_queue',
            routing_key='dead_letter'
        )
        
        # Standard queues with dead letter routing
        queue_configs = {
            QueueType.TASK_QUEUE.value: {
                'durable': True,
                'arguments': {
                    'x-max-priority': 10,
                    'x-dead-letter-exchange': 'dead_letter_exchange',
                    'x-dead-letter-routing-key': 'dead_letter'
                }
            },
            QueueType.RESULT_QUEUE.value: {
                'durable': True,
                'arguments': {'x-message-ttl': 3600000}  # 1 hour
            },
            QueueType.EVENT_QUEUE.value: {
                'durable': True,
                'arguments': {'x-max-priority': 10}
            },
            QueueType.ETL_QUEUE.value: {
                'durable': True,
                'arguments': {
                    'x-max-priority': 10,
                    'x-dead-letter-exchange': 'dead_letter_exchange',
                    'x-dead-letter-routing-key': 'dead_letter'
                }
            },
            QueueType.NOTIFICATION_QUEUE.value: {
                'durable': True,
                'arguments': {'x-max-priority': 10}
            }
        }
        
        for queue_name, config in queue_configs.items():
            self.channel.queue_declare(queue=queue_name, **config)
            self.queues_declared.add(queue_name)
        
        self.logger.info(f"Declared {len(queue_configs)} standard queues")
    
    def publish_task(
        self,
        task_name: str,
        payload: Dict[str, Any],
        queue: QueueType = QueueType.TASK_QUEUE,
        priority: MessagePriority = MessagePriority.NORMAL,
        expires_in_seconds: Optional[int] = None,
        correlation_id: Optional[str] = None
    ) -> str:
        """
        Publish a task message to the specified queue
        
        Args:
            task_name: Name of the task
            payload: Task payload data
            queue: Target queue type
            priority: Message priority
            expires_in_seconds: Message expiration time
            correlation_id: Optional correlation ID
            
        Returns:
            Task ID for tracking
        """
        try:
            if not self.connect():
                raise ConnectionError("Failed to connect to RabbitMQ")
            
            # Create task message
            task_id = str(uuid.uuid4())
            expires_at = None
            if expires_in_seconds:
                expires_at = datetime.now() + timedelta(seconds=expires_in_seconds)
            
            task_message = TaskMessage(
                task_id=task_id,
                task_name=task_name,
                payload=payload,
                priority=priority,
                expires_at=expires_at,
                correlation_id=correlation_id
            )
            
            # Convert to JSON
            message_body = json.dumps(asdict(task_message), default=str)
            
            # Publish properties
            properties = pika.BasicProperties(
                priority=priority.value,
                message_id=task_id,
                correlation_id=task_message.correlation_id,
                timestamp=int(task_message.created_at.timestamp()),
                delivery_mode=2,  # Persistent
                expiration=str(expires_in_seconds * 1000) if expires_in_seconds else None
            )
            
            # Publish message
            self.channel.basic_publish(
                exchange='',
                routing_key=queue.value,
                body=message_body,
                properties=properties
            )
            
            # Track task
            self.active_tasks[task_id] = task_message
            
            self.logger.info(f"Published task {task_name} with ID {task_id}")
            return task_id
            
        except Exception as e:
            self.logger.error(f"Failed to publish task {task_name}: {str(e)}")
            raise
    
    def consume_tasks(
        self,
        queue: QueueType,
        callback: Callable[[TaskMessage], ResultMessage],
        auto_ack: bool = False,
        max_workers: int = 1
    ):
        """
        Consume tasks from specified queue with callback processing
        
        Args:
            queue: Queue to consume from
            callback: Task processing callback function
            auto_ack: Whether to auto-acknowledge messages
            max_workers: Maximum concurrent workers
        """
        try:
            if not self.connect():
                raise ConnectionError("Failed to connect to RabbitMQ")
            
            def process_message(ch, method, properties, body):
                try:
                    # Parse task message
                    task_data = json.loads(body.decode())
                    task_message = TaskMessage(**task_data)
                    
                    # Process task
                    start_time = time.time()
                    result = callback(task_message)
                    processing_time = (time.time() - start_time) * 1000
                    
                    # Create result message
                    if not isinstance(result, ResultMessage):
                        result = ResultMessage(
                            task_id=task_message.task_id,
                            correlation_id=task_message.correlation_id,
                            status="success",
                            result=result if isinstance(result, dict) else {"data": result},
                            processing_time_ms=processing_time
                        )
                    
                    # Publish result if reply_to is specified
                    if task_message.reply_to:
                        self.publish_result(result, task_message.reply_to)
                    
                    # Store result
                    self.task_results[task_message.task_id] = result
                    
                    # Acknowledge message
                    if not auto_ack:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                    self.logger.info(f"Processed task {task_message.task_name} successfully")
                    
                except Exception as e:
                    self.logger.error(f"Task processing failed: {str(e)}")
                    
                    # Create error result
                    if 'task_message' in locals():
                        error_result = ResultMessage(
                            task_id=task_message.task_id,
                            correlation_id=task_message.correlation_id,
                            status="error",
                            error=str(e)
                        )
                        self.task_results[task_message.task_id] = error_result
                    
                    # Negative acknowledge for retry
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # Setup consumer
            self.channel.basic_qos(prefetch_count=max_workers)
            self.channel.basic_consume(
                queue=queue.value,
                on_message_callback=process_message,
                auto_ack=auto_ack
            )
            
            self.logger.info(f"Started consuming from {queue.value} with {max_workers} workers")
            
            # Start consuming
            self.channel.start_consuming()
            
        except Exception as e:
            self.logger.error(f"Consumer setup failed: {str(e)}")
            raise
    
    def publish_result(self, result: ResultMessage, queue_name: str):
        """Publish task result to result queue"""
        try:
            message_body = json.dumps(asdict(result), default=str)
            
            properties = pika.BasicProperties(
                correlation_id=result.correlation_id,
                timestamp=int(result.completed_at.timestamp()),
                delivery_mode=2
            )
            
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=properties
            )
            
            self.logger.debug(f"Published result for task {result.task_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to publish result: {str(e)}")
    
    def get_task_result(self, task_id: str, timeout: int = 30) -> Optional[ResultMessage]:
        """
        Get task result with timeout
        
        Args:
            task_id: Task ID to get result for
            timeout: Timeout in seconds
            
        Returns:
            ResultMessage if found, None if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if task_id in self.task_results:
                return self.task_results[task_id]
            
            # Poll result queue
            try:
                method_frame, header_frame, body = self.channel.basic_get(
                    queue=QueueType.RESULT_QUEUE.value,
                    auto_ack=True
                )
                
                if body:
                    result_data = json.loads(body.decode())
                    result = ResultMessage(**result_data)
                    self.task_results[result.task_id] = result
                    
                    if result.task_id == task_id:
                        return result
            except:
                pass
            
            time.sleep(0.1)
        
        return None
    
    def publish_event(
        self,
        event_type: str,
        event_data: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL
    ):
        """Publish system event"""
        event_message = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_data": event_data,
            "timestamp": datetime.now().isoformat(),
            "source": "rabbitmq_manager"
        }
        
        self.publish_task(
            task_name="system_event",
            payload=event_message,
            queue=QueueType.EVENT_QUEUE,
            priority=priority
        )
    
    def publish_notification(
        self,
        recipient: str,
        subject: str,
        message: str,
        notification_type: str = "info",
        priority: MessagePriority = MessagePriority.NORMAL
    ):
        """Publish notification message"""
        notification = {
            "notification_id": str(uuid.uuid4()),
            "recipient": recipient,
            "subject": subject,
            "message": message,
            "type": notification_type,
            "timestamp": datetime.now().isoformat()
        }
        
        self.publish_task(
            task_name="send_notification",
            payload=notification,
            queue=QueueType.NOTIFICATION_QUEUE,
            priority=priority
        )
    
    def get_queue_stats(self, queue: QueueType) -> Dict[str, Any]:
        """Get queue statistics"""
        try:
            if not self.connect():
                return {"error": "Connection failed"}
            
            method = self.channel.queue_declare(queue=queue.value, passive=True)
            message_count = method.method.message_count
            consumer_count = method.method.consumer_count
            
            return {
                "queue_name": queue.value,
                "message_count": message_count,
                "consumer_count": consumer_count,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def purge_queue(self, queue: QueueType) -> bool:
        """Purge all messages from queue"""
        try:
            if not self.connect():
                return False
            
            self.channel.queue_purge(queue=queue.value)
            self.logger.info(f"Purged queue {queue.value}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to purge queue {queue.value}: {str(e)}")
            return False
    
    def close(self):
        """Close connections"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            
            self.logger.info("RabbitMQ connections closed")
            
        except Exception as e:
            self.logger.warning(f"Error closing RabbitMQ connections: {str(e)}")


class RabbitMQAsyncManager:
    """Async version of RabbitMQ manager for high-performance applications"""
    
    def __init__(self):
        self.config = get_unified_config()
        self.logger = get_logger(__name__)
        
        # Connection settings
        self.host = os.getenv("RABBITMQ_HOST", "localhost")
        self.port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.username = os.getenv("RABBITMQ_USERNAME", "guest")
        self.password = os.getenv("RABBITMQ_PASSWORD", "guest")
        self.virtual_host = os.getenv("RABBITMQ_VHOST", "/")
        
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
    
    async def connect(self) -> bool:
        """Establish async connection"""
        try:
            if self.connection and not self.connection.is_closed:
                return True
            
            connection_url = f"amqp://{self.username}:{self.password}@{self.host}:{self.port}{self.virtual_host}"
            self.connection = await aio_pika.connect_robust(connection_url)
            self.channel = await self.connection.channel()
            
            await self.channel.set_qos(prefetch_count=100)
            
            self.logger.info("Connected to RabbitMQ (async) successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ (async): {str(e)}")
            return False
    
    async def publish_task_async(
        self,
        task_name: str,
        payload: Dict[str, Any],
        queue_name: str = "task_queue",
        priority: int = 5
    ) -> str:
        """Async task publishing"""
        if not await self.connect():
            raise ConnectionError("Failed to connect to RabbitMQ")
        
        task_id = str(uuid.uuid4())
        task_message = {
            "task_id": task_id,
            "task_name": task_name,
            "payload": payload,
            "created_at": datetime.now().isoformat()
        }
        
        queue = await self.channel.declare_queue(queue_name, durable=True)
        
        message = aio_pika.Message(
            json.dumps(task_message, default=str).encode(),
            priority=priority,
            message_id=task_id,
            timestamp=datetime.now()
        )
        
        await queue.publish(message)
        
        self.logger.info(f"Published async task {task_name} with ID {task_id}")
        return task_id
    
    async def consume_tasks_async(
        self,
        queue_name: str,
        callback: Callable,
        max_workers: int = 10
    ):
        """Async task consumption"""
        if not await self.connect():
            raise ConnectionError("Failed to connect to RabbitMQ")
        
        queue = await self.channel.declare_queue(queue_name, durable=True)
        
        async def process_message(message: aio_pika.IncomingMessage):
            async with message.process():
                try:
                    task_data = json.loads(message.body.decode())
                    result = await callback(task_data)
                    self.logger.info(f"Processed async task successfully")
                except Exception as e:
                    self.logger.error(f"Async task processing failed: {str(e)}")
                    raise
        
        await queue.consume(process_message, no_ack=False)
    
    async def close(self):
        """Close async connections"""
        if self.connection:
            await self.connection.close()


# Factory functions
def create_rabbitmq_manager() -> RabbitMQManager:
    """Create RabbitMQ manager instance"""
    return RabbitMQManager()


def create_rabbitmq_async_manager() -> RabbitMQAsyncManager:
    """Create async RabbitMQ manager instance"""
    return RabbitMQAsyncManager()


# Example ETL task processor
class ETLTaskProcessor:
    """Example ETL task processor using RabbitMQ"""
    
    def __init__(self):
        self.rabbitmq = create_rabbitmq_manager()
        self.logger = get_logger(__name__)
    
    def process_etl_task(self, task_message: TaskMessage) -> ResultMessage:
        """Process ETL task"""
        try:
            task_name = task_message.task_name
            payload = task_message.payload
            
            self.logger.info(f"Processing ETL task: {task_name}")
            
            # Simulate ETL processing
            if task_name == "bronze_processing":
                result = {"records_processed": 10000, "status": "completed"}
            elif task_name == "silver_processing":
                result = {"records_processed": 9500, "quality_score": 0.95}
            elif task_name == "gold_processing":
                result = {"tables_created": 5, "aggregations": 12}
            else:
                result = {"message": "Task processed successfully"}
            
            return ResultMessage(
                task_id=task_message.task_id,
                correlation_id=task_message.correlation_id,
                status="success",
                result=result
            )
            
        except Exception as e:
            return ResultMessage(
                task_id=task_message.task_id,
                correlation_id=task_message.correlation_id,
                status="error",
                error=str(e)
            )
    
    def start_consumer(self):
        """Start ETL task consumer"""
        self.rabbitmq.consume_tasks(
            queue=QueueType.ETL_QUEUE,
            callback=self.process_etl_task,
            max_workers=3
        )


# Testing and example usage
if __name__ == "__main__":
    import os
    
    # Set environment variables for testing
    os.environ.setdefault("RABBITMQ_HOST", "localhost")
    
    print("Testing RabbitMQ Manager...")
    
    try:
        # Test basic functionality
        manager = create_rabbitmq_manager()
        
        # Test connection
        if manager.connect():
            print("✅ Connection established")
        else:
            print("❌ Connection failed")
            exit(1)
        
        # Test task publishing
        task_id = manager.publish_task(
            task_name="test_task",
            payload={"data": "test_data", "number": 42},
            priority=MessagePriority.HIGH
        )
        print(f"✅ Task published with ID: {task_id}")
        
        # Test queue stats
        stats = manager.get_queue_stats(QueueType.TASK_QUEUE)
        print(f"✅ Queue stats: {stats}")
        
        # Test event publishing
        manager.publish_event(
            event_type="system_test",
            event_data={"test": True, "timestamp": datetime.now().isoformat()}
        )
        print("✅ Event published")
        
        # Test notification
        manager.publish_notification(
            recipient="admin@example.com",
            subject="Test Notification",
            message="RabbitMQ is working correctly"
        )
        print("✅ Notification published")
        
        manager.close()
        print("✅ RabbitMQ Manager testing completed successfully!")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()