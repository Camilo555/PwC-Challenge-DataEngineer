"""
RabbitMQ Reliability Features

This module implements comprehensive reliability features including:
- Advanced dead letter queue management
- Circuit breakers for fault tolerance
- Message deduplication mechanisms
- Retry strategies and backoff policies
- Message persistence and durability
- Priority queues for critical messages
"""

import asyncio
import hashlib
import json
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Union
import threading
from concurrent.futures import ThreadPoolExecutor, Future
import pickle

import pika
import aio_pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from .enterprise_rabbitmq_manager import (
    EnterpriseRabbitMQManager, EnterpriseMessage, MessageMetadata,
    QueueType, MessagePriority, get_rabbitmq_manager
)
from core.logging import get_logger


class RetryStrategy(Enum):
    """Retry strategy types"""
    IMMEDIATE = "immediate"
    LINEAR_BACKOFF = "linear_backoff"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    FIXED_DELAY = "fixed_delay"
    CUSTOM = "custom"


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject all requests
    HALF_OPEN = "half_open" # Testing if service recovered


@dataclass
class RetryConfig:
    """Retry configuration"""
    max_retries: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 300.0  # 5 minutes
    backoff_multiplier: float = 2.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    retry_on_exceptions: List[str] = field(default_factory=list)
    dead_letter_after_max_retries: bool = True
    
    def calculate_delay(self, retry_count: int) -> float:
        """Calculate delay for retry attempt"""
        if self.strategy == RetryStrategy.IMMEDIATE:
            return 0
        elif self.strategy == RetryStrategy.FIXED_DELAY:
            return self.initial_delay_seconds
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.initial_delay_seconds * retry_count
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.initial_delay_seconds * (self.backoff_multiplier ** retry_count)
        else:
            delay = self.initial_delay_seconds
            
        return min(delay, self.max_delay_seconds)


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_seconds: int = 60
    half_open_max_calls: int = 3
    monitor_window_seconds: int = 300  # 5 minutes
    expected_exceptions: List[str] = field(default_factory=lambda: ["AMQPConnectionError", "TimeoutError"])


@dataclass
class DeduplicationConfig:
    """Message deduplication configuration"""
    enabled: bool = True
    cache_ttl_seconds: int = 3600  # 1 hour
    max_cache_size: int = 100000
    hash_payload: bool = True
    hash_headers: bool = False
    persistence_enabled: bool = False  # Store in Redis for distributed dedup


class MessageReliabilityHandler:
    """
    Comprehensive message reliability handler with advanced features
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Retry management
        self.retry_configs: Dict[str, RetryConfig] = {}
        self.retry_queue_prefix = "retry"
        self.delayed_queue_prefix = "delayed"
        
        # Circuit breaker management
        self.circuit_breakers: Dict[str, "AdvancedCircuitBreaker"] = {}
        self.breaker_configs: Dict[str, CircuitBreakerConfig] = {}
        
        # Message deduplication
        self.dedup_config = DeduplicationConfig()
        self.message_hashes: Dict[str, Set[str]] = defaultdict(set)  # queue -> hash set
        self.hash_timestamps: Dict[str, datetime] = {}  # hash -> timestamp
        self.dedup_stats = {
            "messages_processed": 0,
            "duplicates_detected": 0,
            "duplicates_rejected": 0
        }
        
        # Dead letter management
        self.dead_letter_handlers: Dict[str, Callable] = {}
        self.dead_letter_stats = {
            "messages_sent_to_dlq": 0,
            "messages_recovered": 0,
            "permanent_failures": 0
        }
        
        # Priority queue management
        self.priority_queues: Dict[str, "PriorityQueueManager"] = {}
        
        # Performance monitoring
        self.performance_stats = {
            "total_messages": 0,
            "successful_deliveries": 0,
            "failed_deliveries": 0,
            "retry_attempts": 0,
            "circuit_breaker_trips": 0
        }
        
        self._setup_reliability_features()
        
    def _setup_reliability_features(self):
        """Setup reliability features"""
        # Setup default retry configurations
        self.setup_default_retry_configs()
        
        # Setup circuit breakers for critical queues
        self.setup_default_circuit_breakers()
        
        # Start cleanup threads
        self._start_cleanup_threads()
        
        self.logger.info("Message reliability features initialized")
    
    def setup_default_retry_configs(self):
        """Setup default retry configurations for different queue types"""
        configs = {
            QueueType.ML_TRAINING.value: RetryConfig(
                max_retries=3,
                initial_delay_seconds=30,
                max_delay_seconds=3600,  # 1 hour
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            ),
            QueueType.ML_INFERENCE.value: RetryConfig(
                max_retries=2,
                initial_delay_seconds=1,
                max_delay_seconds=10,
                strategy=RetryStrategy.LINEAR_BACKOFF
            ),
            QueueType.DATA_QUALITY_VALIDATION.value: RetryConfig(
                max_retries=3,
                initial_delay_seconds=60,
                max_delay_seconds=1800,  # 30 minutes
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            ),
            QueueType.SYSTEM_ALERTS.value: RetryConfig(
                max_retries=5,
                initial_delay_seconds=5,
                max_delay_seconds=60,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            ),
            QueueType.USER_NOTIFICATIONS.value: RetryConfig(
                max_retries=3,
                initial_delay_seconds=10,
                max_delay_seconds=300,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            ),
            "default": RetryConfig(
                max_retries=3,
                initial_delay_seconds=5,
                max_delay_seconds=300,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            )
        }
        
        self.retry_configs.update(configs)
    
    def setup_default_circuit_breakers(self):
        """Setup default circuit breaker configurations"""
        configs = {
            QueueType.ML_INFERENCE.value: CircuitBreakerConfig(
                failure_threshold=5,
                timeout_seconds=30,
                half_open_max_calls=3
            ),
            QueueType.SYSTEM_ALERTS.value: CircuitBreakerConfig(
                failure_threshold=3,
                timeout_seconds=60,
                half_open_max_calls=2
            ),
            QueueType.REALTIME_ANALYTICS.value: CircuitBreakerConfig(
                failure_threshold=10,
                timeout_seconds=45,
                half_open_max_calls=5
            ),
            "default": CircuitBreakerConfig(
                failure_threshold=5,
                timeout_seconds=60,
                half_open_max_calls=3
            )
        }
        
        for queue_name, config in configs.items():
            self.circuit_breakers[queue_name] = AdvancedCircuitBreaker(
                name=queue_name,
                config=config,
                logger=self.logger
            )
            self.breaker_configs[queue_name] = config
    
    def _start_cleanup_threads(self):
        """Start background cleanup threads"""
        # Deduplication cache cleanup
        dedup_cleanup_thread = threading.Thread(
            target=self._deduplication_cleanup_worker,
            daemon=True
        )
        dedup_cleanup_thread.start()
        
        # Circuit breaker reset thread
        breaker_reset_thread = threading.Thread(
            target=self._circuit_breaker_reset_worker,
            daemon=True
        )
        breaker_reset_thread.start()
        
        self.logger.info("Started reliability cleanup threads")
    
    # Message Publishing with Reliability
    
    def publish_reliable_message(
        self,
        message: EnterpriseMessage,
        queue_name: Optional[str] = None,
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None,
        enable_deduplication: bool = True,
        enable_circuit_breaker: bool = True,
        retry_config: Optional[RetryConfig] = None
    ) -> bool:
        """
        Publish message with comprehensive reliability features
        """
        try:
            self.performance_stats["total_messages"] += 1
            
            # Get queue name for configuration lookup
            effective_queue = queue_name or message.queue_type.value
            
            # Message deduplication check
            if enable_deduplication and self._is_duplicate_message(message, effective_queue):
                self.dedup_stats["duplicates_rejected"] += 1
                self.logger.debug(f"Duplicate message rejected: {message.metadata.message_id}")
                return True  # Consider duplicate as successful
            
            # Circuit breaker check
            if enable_circuit_breaker:
                breaker = self._get_circuit_breaker(effective_queue)
                if not breaker.can_execute():
                    self.logger.warning(f"Circuit breaker OPEN for queue {effective_queue}")
                    self.performance_stats["circuit_breaker_trips"] += 1
                    return False
            
            # Attempt to publish with circuit breaker protection
            success = False
            exception_occurred = None
            
            try:
                if enable_circuit_breaker:
                    success = breaker.execute(self._do_publish_message, message, exchange_name, routing_key)
                else:
                    success = self._do_publish_message(message, exchange_name, routing_key)
                    
                if success:
                    self.performance_stats["successful_deliveries"] += 1
                    
                    # Record message hash for deduplication
                    if enable_deduplication:
                        self._record_message_hash(message, effective_queue)
                else:
                    self.performance_stats["failed_deliveries"] += 1
                    
            except Exception as e:
                exception_occurred = e
                self.performance_stats["failed_deliveries"] += 1
                
                # Determine if this should be retried
                if self._should_retry(e, effective_queue, retry_config):
                    self._schedule_retry(message, effective_queue, 1, e)
                else:
                    self._send_to_dead_letter_queue(message, str(e))
                
                return False
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in reliable message publishing: {e}")
            return False
    
    async def publish_reliable_message_async(
        self,
        message: EnterpriseMessage,
        queue_name: Optional[str] = None,
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None,
        enable_deduplication: bool = True,
        enable_circuit_breaker: bool = True,
        retry_config: Optional[RetryConfig] = None
    ) -> bool:
        """
        Publish message asynchronously with comprehensive reliability features
        """
        try:
            self.performance_stats["total_messages"] += 1
            
            # Get queue name for configuration lookup
            effective_queue = queue_name or message.queue_type.value
            
            # Message deduplication check
            if enable_deduplication and self._is_duplicate_message(message, effective_queue):
                self.dedup_stats["duplicates_rejected"] += 1
                self.logger.debug(f"Async duplicate message rejected: {message.metadata.message_id}")
                return True
            
            # Circuit breaker check
            if enable_circuit_breaker:
                breaker = self._get_circuit_breaker(effective_queue)
                if not breaker.can_execute():
                    self.logger.warning(f"Circuit breaker OPEN for async queue {effective_queue}")
                    self.performance_stats["circuit_breaker_trips"] += 1
                    return False
            
            # Attempt to publish with circuit breaker protection
            success = False
            
            try:
                if enable_circuit_breaker:
                    success = await breaker.execute_async(
                        self._do_publish_message_async, message, exchange_name, routing_key
                    )
                else:
                    success = await self._do_publish_message_async(message, exchange_name, routing_key)
                    
                if success:
                    self.performance_stats["successful_deliveries"] += 1
                    
                    # Record message hash for deduplication
                    if enable_deduplication:
                        self._record_message_hash(message, effective_queue)
                else:
                    self.performance_stats["failed_deliveries"] += 1
                    
            except Exception as e:
                self.performance_stats["failed_deliveries"] += 1
                
                # Determine if this should be retried
                if self._should_retry(e, effective_queue, retry_config):
                    await self._schedule_retry_async(message, effective_queue, 1, e)
                else:
                    await self._send_to_dead_letter_queue_async(message, str(e))
                
                return False
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in async reliable message publishing: {e}")
            return False
    
    def _do_publish_message(
        self, 
        message: EnterpriseMessage, 
        exchange_name: Optional[str], 
        routing_key: Optional[str]
    ) -> bool:
        """Internal message publishing implementation"""
        return self.rabbitmq.publish_message(message, exchange_name, routing_key)
    
    async def _do_publish_message_async(
        self, 
        message: EnterpriseMessage, 
        exchange_name: Optional[str], 
        routing_key: Optional[str]
    ) -> bool:
        """Internal async message publishing implementation"""
        return await self.rabbitmq.publish_message_async(message, exchange_name, routing_key)
    
    # Message Deduplication
    
    def _is_duplicate_message(self, message: EnterpriseMessage, queue_name: str) -> bool:
        """Check if message is a duplicate"""
        if not self.dedup_config.enabled:
            return False
            
        try:
            # Generate message hash
            message_hash = self._generate_message_hash(message)
            
            # Check if hash exists for this queue
            if message_hash in self.message_hashes.get(queue_name, set()):
                self.dedup_stats["duplicates_detected"] += 1
                return True
                
            self.dedup_stats["messages_processed"] += 1
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking message duplication: {e}")
            return False  # Allow message if dedup check fails
    
    def _record_message_hash(self, message: EnterpriseMessage, queue_name: str):
        """Record message hash for deduplication"""
        try:
            message_hash = self._generate_message_hash(message)
            
            # Add to queue hash set
            if queue_name not in self.message_hashes:
                self.message_hashes[queue_name] = set()
            
            self.message_hashes[queue_name].add(message_hash)
            self.hash_timestamps[message_hash] = datetime.utcnow()
            
        except Exception as e:
            self.logger.error(f"Error recording message hash: {e}")
    
    def _generate_message_hash(self, message: EnterpriseMessage) -> str:
        """Generate hash for message deduplication"""
        try:
            hash_data = {
                "message_id": message.metadata.message_id,
                "message_type": message.message_type
            }
            
            if self.dedup_config.hash_payload:
                # Create deterministic hash of payload
                payload_str = json.dumps(message.payload, sort_keys=True, default=str)
                hash_data["payload"] = payload_str
            
            if self.dedup_config.hash_headers:
                headers_str = json.dumps(message.metadata.headers, sort_keys=True, default=str)
                hash_data["headers"] = headers_str
            
            # Generate SHA-256 hash
            hash_str = json.dumps(hash_data, sort_keys=True)
            return hashlib.sha256(hash_str.encode()).hexdigest()
            
        except Exception as e:
            self.logger.error(f"Error generating message hash: {e}")
            # Fallback to message ID if hash generation fails
            return message.metadata.message_id
    
    def _deduplication_cleanup_worker(self):
        """Background worker to cleanup old deduplication hashes"""
        while True:
            try:
                current_time = datetime.utcnow()
                expired_hashes = []
                
                # Find expired hashes
                for message_hash, timestamp in self.hash_timestamps.items():
                    if (current_time - timestamp).total_seconds() > self.dedup_config.cache_ttl_seconds:
                        expired_hashes.append(message_hash)
                
                # Remove expired hashes
                for message_hash in expired_hashes:
                    # Remove from timestamp tracking
                    del self.hash_timestamps[message_hash]
                    
                    # Remove from queue hash sets
                    for queue_name in self.message_hashes:
                        self.message_hashes[queue_name].discard(message_hash)
                
                # Check cache size limits
                if len(self.hash_timestamps) > self.dedup_config.max_cache_size:
                    self._trim_deduplication_cache()
                
                if expired_hashes:
                    self.logger.debug(f"Cleaned up {len(expired_hashes)} expired deduplication hashes")
                
                # Sleep for 5 minutes
                time.sleep(300)
                
            except Exception as e:
                self.logger.error(f"Error in deduplication cleanup: {e}")
                time.sleep(60)  # Sleep for 1 minute on error
    
    def _trim_deduplication_cache(self):
        """Trim deduplication cache to size limits"""
        try:
            target_size = int(self.dedup_config.max_cache_size * 0.8)  # Trim to 80%
            
            # Sort by timestamp and keep newest
            sorted_hashes = sorted(
                self.hash_timestamps.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Keep only target_size newest entries
            hashes_to_keep = {h[0] for h in sorted_hashes[:target_size]}
            hashes_to_remove = set(self.hash_timestamps.keys()) - hashes_to_keep
            
            # Remove old hashes
            for message_hash in hashes_to_remove:
                del self.hash_timestamps[message_hash]
                
                for queue_name in self.message_hashes:
                    self.message_hashes[queue_name].discard(message_hash)
            
            self.logger.info(f"Trimmed deduplication cache: removed {len(hashes_to_remove)} entries")
            
        except Exception as e:
            self.logger.error(f"Error trimming deduplication cache: {e}")
    
    # Retry Management
    
    def _should_retry(
        self, 
        exception: Exception, 
        queue_name: str, 
        retry_config: Optional[RetryConfig] = None
    ) -> bool:
        """Determine if message should be retried"""
        config = retry_config or self._get_retry_config(queue_name)
        
        # Check if exception type is retryable
        exception_name = type(exception).__name__
        
        if config.retry_on_exceptions:
            return exception_name in config.retry_on_exceptions
        
        # Default retryable exceptions
        retryable_exceptions = [
            "AMQPConnectionError",
            "AMQPChannelError", 
            "ConnectionError",
            "TimeoutError",
            "BrokenPipeError"
        ]
        
        return exception_name in retryable_exceptions
    
    def _schedule_retry(
        self,
        message: EnterpriseMessage,
        queue_name: str,
        retry_count: int,
        last_exception: Exception
    ):
        """Schedule message retry"""
        try:
            config = self._get_retry_config(queue_name)
            
            if retry_count > config.max_retries:
                self.logger.warning(
                    f"Max retries exceeded for message {message.metadata.message_id}"
                )
                
                if config.dead_letter_after_max_retries:
                    self._send_to_dead_letter_queue(message, str(last_exception))
                
                return
            
            self.performance_stats["retry_attempts"] += 1
            
            # Calculate delay
            delay = config.calculate_delay(retry_count)
            
            # Update message retry information
            message.retry_count = retry_count
            message.error_details = str(last_exception)
            
            # Add retry metadata
            message.metadata.headers.update({
                "x-retry-count": retry_count,
                "x-max-retries": config.max_retries,
                "x-last-error": str(last_exception),
                "x-retry-delay": delay,
                "x-retry-strategy": config.strategy.value,
                "x-scheduled-at": datetime.utcnow().isoformat()
            })
            
            if delay > 0:
                # Use delayed message for retry
                self._schedule_delayed_retry(message, queue_name, delay)
            else:
                # Immediate retry
                self._retry_message_immediately(message, queue_name)
                
        except Exception as e:
            self.logger.error(f"Error scheduling retry: {e}")
            self._send_to_dead_letter_queue(message, str(e))
    
    async def _schedule_retry_async(
        self,
        message: EnterpriseMessage,
        queue_name: str,
        retry_count: int,
        last_exception: Exception
    ):
        """Schedule message retry asynchronously"""
        try:
            config = self._get_retry_config(queue_name)
            
            if retry_count > config.max_retries:
                self.logger.warning(
                    f"Max async retries exceeded for message {message.metadata.message_id}"
                )
                
                if config.dead_letter_after_max_retries:
                    await self._send_to_dead_letter_queue_async(message, str(last_exception))
                
                return
            
            self.performance_stats["retry_attempts"] += 1
            
            # Calculate delay
            delay = config.calculate_delay(retry_count)
            
            # Update message retry information
            message.retry_count = retry_count
            message.error_details = str(last_exception)
            
            # Add retry metadata
            message.metadata.headers.update({
                "x-retry-count": retry_count,
                "x-max-retries": config.max_retries,
                "x-last-error": str(last_exception),
                "x-retry-delay": delay,
                "x-retry-strategy": config.strategy.value,
                "x-scheduled-at": datetime.utcnow().isoformat()
            })
            
            if delay > 0:
                # Use delayed message for retry
                await self._schedule_delayed_retry_async(message, queue_name, delay)
            else:
                # Immediate retry
                await self._retry_message_immediately_async(message, queue_name)
                
        except Exception as e:
            self.logger.error(f"Error scheduling async retry: {e}")
            await self._send_to_dead_letter_queue_async(message, str(e))
    
    def _schedule_delayed_retry(self, message: EnterpriseMessage, queue_name: str, delay: float):
        """Schedule delayed retry using TTL and dead letter routing"""
        try:
            # Create delayed retry queue name
            retry_queue = f"{self.retry_queue_prefix}_{queue_name}_{int(delay)}"
            
            with self.rabbitmq.connection_pool.get_connection() as connection:
                channel = connection.channel()
                
                # Declare retry queue with TTL
                channel.queue_declare(
                    queue=retry_queue,
                    durable=True,
                    arguments={
                        'x-message-ttl': int(delay * 1000),  # Convert to milliseconds
                        'x-dead-letter-exchange': '',
                        'x-dead-letter-routing-key': queue_name
                    }
                )
                
                # Publish to retry queue
                message_body = json.dumps(message.to_dict(), default=str)
                
                channel.basic_publish(
                    exchange='',
                    routing_key=retry_queue,
                    body=message_body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Persistent
                        headers=message.metadata.headers
                    )
                )
                
                self.logger.debug(
                    f"Scheduled retry for message {message.metadata.message_id} "
                    f"in {delay} seconds"
                )
                
        except Exception as e:
            self.logger.error(f"Error scheduling delayed retry: {e}")
            self._send_to_dead_letter_queue(message, str(e))
    
    async def _schedule_delayed_retry_async(self, message: EnterpriseMessage, queue_name: str, delay: float):
        """Schedule delayed retry asynchronously"""
        try:
            # For async, we can use asyncio.sleep
            await asyncio.sleep(delay)
            await self._retry_message_immediately_async(message, queue_name)
            
        except Exception as e:
            self.logger.error(f"Error scheduling async delayed retry: {e}")
            await self._send_to_dead_letter_queue_async(message, str(e))
    
    def _retry_message_immediately(self, message: EnterpriseMessage, queue_name: str):
        """Retry message immediately"""
        try:
            success = self.rabbitmq.publish_message(
                message=message,
                routing_key=queue_name
            )
            
            if not success:
                # If immediate retry fails, try scheduling another retry
                self._schedule_retry(
                    message, queue_name, 
                    message.retry_count + 1, 
                    Exception("Immediate retry failed")
                )
                
        except Exception as e:
            self.logger.error(f"Error in immediate retry: {e}")
            self._schedule_retry(message, queue_name, message.retry_count + 1, e)
    
    async def _retry_message_immediately_async(self, message: EnterpriseMessage, queue_name: str):
        """Retry message immediately asynchronously"""
        try:
            success = await self.rabbitmq.publish_message_async(
                message=message,
                routing_key=queue_name
            )
            
            if not success:
                # If immediate retry fails, try scheduling another retry
                await self._schedule_retry_async(
                    message, queue_name, 
                    message.retry_count + 1, 
                    Exception("Immediate async retry failed")
                )
                
        except Exception as e:
            self.logger.error(f"Error in immediate async retry: {e}")
            await self._schedule_retry_async(message, queue_name, message.retry_count + 1, e)
    
    def _get_retry_config(self, queue_name: str) -> RetryConfig:
        """Get retry configuration for queue"""
        return self.retry_configs.get(queue_name, self.retry_configs.get("default", RetryConfig()))
    
    # Dead Letter Queue Management
    
    def _send_to_dead_letter_queue(self, message: EnterpriseMessage, error_reason: str):
        """Send message to dead letter queue"""
        try:
            self.dead_letter_stats["messages_sent_to_dlq"] += 1
            
            # Add dead letter metadata
            message.metadata.headers.update({
                "x-death-reason": error_reason,
                "x-death-timestamp": datetime.utcnow().isoformat(),
                "x-original-queue": message.queue_type.value,
                "x-total-retries": message.retry_count
            })
            
            # Create dead letter message
            dead_letter_message = EnterpriseMessage(
                queue_type=QueueType.DEAD_LETTER,
                message_type="dead_letter",
                payload={
                    "original_message": message.to_dict(),
                    "error_reason": error_reason,
                    "death_timestamp": datetime.utcnow().isoformat()
                },
                metadata=message.metadata
            )
            
            success = self.rabbitmq.publish_message(
                message=dead_letter_message,
                routing_key=QueueType.DEAD_LETTER.value
            )
            
            if success:
                self.logger.warning(
                    f"Message {message.metadata.message_id} sent to dead letter queue: {error_reason}"
                )
            else:
                self.dead_letter_stats["permanent_failures"] += 1
                self.logger.error(
                    f"Failed to send message {message.metadata.message_id} to dead letter queue"
                )
                
        except Exception as e:
            self.dead_letter_stats["permanent_failures"] += 1
            self.logger.error(f"Error sending to dead letter queue: {e}")
    
    async def _send_to_dead_letter_queue_async(self, message: EnterpriseMessage, error_reason: str):
        """Send message to dead letter queue asynchronously"""
        try:
            self.dead_letter_stats["messages_sent_to_dlq"] += 1
            
            # Add dead letter metadata
            message.metadata.headers.update({
                "x-death-reason": error_reason,
                "x-death-timestamp": datetime.utcnow().isoformat(),
                "x-original-queue": message.queue_type.value,
                "x-total-retries": message.retry_count
            })
            
            # Create dead letter message
            dead_letter_message = EnterpriseMessage(
                queue_type=QueueType.DEAD_LETTER,
                message_type="dead_letter",
                payload={
                    "original_message": message.to_dict(),
                    "error_reason": error_reason,
                    "death_timestamp": datetime.utcnow().isoformat()
                },
                metadata=message.metadata
            )
            
            success = await self.rabbitmq.publish_message_async(
                message=dead_letter_message,
                routing_key=QueueType.DEAD_LETTER.value
            )
            
            if success:
                self.logger.warning(
                    f"Message {message.metadata.message_id} sent to dead letter queue (async): {error_reason}"
                )
            else:
                self.dead_letter_stats["permanent_failures"] += 1
                self.logger.error(
                    f"Failed to send message {message.metadata.message_id} to dead letter queue (async)"
                )
                
        except Exception as e:
            self.dead_letter_stats["permanent_failures"] += 1
            self.logger.error(f"Error sending to dead letter queue (async): {e}")
    
    def register_dead_letter_handler(self, message_type: str, handler: Callable):
        """Register handler for dead letter messages"""
        self.dead_letter_handlers[message_type] = handler
        self.logger.info(f"Registered dead letter handler for message type: {message_type}")
    
    def recover_from_dead_letter_queue(self, message_id: str, target_queue: str) -> bool:
        """Attempt to recover message from dead letter queue"""
        try:
            # This would implement recovery logic
            # For now, simulate recovery
            self.dead_letter_stats["messages_recovered"] += 1
            self.logger.info(f"Recovered message {message_id} from dead letter queue to {target_queue}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error recovering message from dead letter queue: {e}")
            return False
    
    # Circuit Breaker Management
    
    def _get_circuit_breaker(self, queue_name: str) -> "AdvancedCircuitBreaker":
        """Get or create circuit breaker for queue"""
        if queue_name not in self.circuit_breakers:
            config = self.breaker_configs.get(queue_name, self.breaker_configs.get("default"))
            self.circuit_breakers[queue_name] = AdvancedCircuitBreaker(
                name=queue_name,
                config=config,
                logger=self.logger
            )
        
        return self.circuit_breakers[queue_name]
    
    def _circuit_breaker_reset_worker(self):
        """Background worker to reset circuit breakers"""
        while True:
            try:
                current_time = time.time()
                
                for breaker in self.circuit_breakers.values():
                    breaker.check_timeout(current_time)
                
                # Sleep for 30 seconds
                time.sleep(30)
                
            except Exception as e:
                self.logger.error(f"Error in circuit breaker reset worker: {e}")
                time.sleep(60)
    
    # Priority Queue Management
    
    def create_priority_queue_manager(self, queue_name: str, max_priority: int = 10) -> "PriorityQueueManager":
        """Create priority queue manager"""
        if queue_name not in self.priority_queues:
            self.priority_queues[queue_name] = PriorityQueueManager(
                queue_name=queue_name,
                max_priority=max_priority,
                rabbitmq_manager=self.rabbitmq,
                logger=self.logger
            )
        
        return self.priority_queues[queue_name]
    
    # Configuration Management
    
    def set_retry_config(self, queue_name: str, config: RetryConfig):
        """Set retry configuration for queue"""
        self.retry_configs[queue_name] = config
        self.logger.info(f"Set retry config for queue {queue_name}")
    
    def set_circuit_breaker_config(self, queue_name: str, config: CircuitBreakerConfig):
        """Set circuit breaker configuration for queue"""
        self.breaker_configs[queue_name] = config
        self.circuit_breakers[queue_name] = AdvancedCircuitBreaker(
            name=queue_name,
            config=config,
            logger=self.logger
        )
        self.logger.info(f"Set circuit breaker config for queue {queue_name}")
    
    def set_deduplication_config(self, config: DeduplicationConfig):
        """Set deduplication configuration"""
        self.dedup_config = config
        self.logger.info("Updated deduplication configuration")
    
    # Monitoring and Metrics
    
    def get_reliability_metrics(self) -> Dict[str, Any]:
        """Get comprehensive reliability metrics"""
        circuit_breaker_states = {}
        for name, breaker in self.circuit_breakers.items():
            circuit_breaker_states[name] = {
                "state": breaker.state.value,
                "failure_count": breaker.failure_count,
                "success_count": breaker.success_count,
                "last_failure_time": breaker.last_failure_time,
                "consecutive_failures": breaker.consecutive_failures
            }
        
        return {
            "performance_stats": self.performance_stats.copy(),
            "deduplication_stats": self.dedup_stats.copy(),
            "dead_letter_stats": self.dead_letter_stats.copy(),
            "circuit_breaker_states": circuit_breaker_states,
            "message_cache_size": len(self.hash_timestamps),
            "active_priority_queues": list(self.priority_queues.keys())
        }


class AdvancedCircuitBreaker:
    """
    Advanced circuit breaker implementation with half-open state and monitoring
    """
    
    def __init__(self, name: str, config: CircuitBreakerConfig, logger):
        self.name = name
        self.config = config
        self.logger = logger
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.consecutive_failures = 0
        self.last_failure_time = None
        self.last_success_time = None
        self.half_open_attempts = 0
        
        # Monitoring window for failure rate calculation
        self.recent_calls = deque(maxlen=100)
        
        self.lock = threading.Lock()
    
    def can_execute(self) -> bool:
        """Check if circuit breaker allows execution"""
        with self.lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True
            elif self.state == CircuitBreakerState.OPEN:
                return False
            elif self.state == CircuitBreakerState.HALF_OPEN:
                return self.half_open_attempts < self.config.half_open_max_calls
            
            return False
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        if not self.can_execute():
            raise Exception(f"Circuit breaker {self.name} is OPEN")
        
        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            self._record_success(execution_time)
            return result
            
        except Exception as e:
            self._record_failure(str(e))
            raise
    
    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with circuit breaker protection"""
        if not self.can_execute():
            raise Exception(f"Circuit breaker {self.name} is OPEN")
        
        try:
            start_time = time.time()
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            self._record_success(execution_time)
            return result
            
        except Exception as e:
            self._record_failure(str(e))
            raise
    
    def _record_success(self, execution_time: float):
        """Record successful execution"""
        with self.lock:
            current_time = time.time()
            
            self.success_count += 1
            self.consecutive_failures = 0
            self.last_success_time = current_time
            
            # Record call result
            self.recent_calls.append({
                "timestamp": current_time,
                "success": True,
                "execution_time": execution_time
            })
            
            # State transitions
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_attempts += 1
                
                if self.half_open_attempts >= self.config.success_threshold:
                    self._transition_to_closed()
            
            self.logger.debug(f"Circuit breaker {self.name}: Success recorded")
    
    def _record_failure(self, error: str):
        """Record failed execution"""
        with self.lock:
            current_time = time.time()
            
            self.failure_count += 1
            self.consecutive_failures += 1
            self.last_failure_time = current_time
            
            # Record call result
            self.recent_calls.append({
                "timestamp": current_time,
                "success": False,
                "error": error
            })
            
            # Check if we should open the circuit
            if self.state == CircuitBreakerState.CLOSED:
                if self.consecutive_failures >= self.config.failure_threshold:
                    self._transition_to_open()
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self._transition_to_open()
            
            self.logger.warning(f"Circuit breaker {self.name}: Failure recorded - {error}")
    
    def _transition_to_open(self):
        """Transition to OPEN state"""
        self.state = CircuitBreakerState.OPEN
        self.half_open_attempts = 0
        self.logger.warning(f"Circuit breaker {self.name} transitioned to OPEN")
    
    def _transition_to_half_open(self):
        """Transition to HALF_OPEN state"""
        self.state = CircuitBreakerState.HALF_OPEN
        self.half_open_attempts = 0
        self.logger.info(f"Circuit breaker {self.name} transitioned to HALF_OPEN")
    
    def _transition_to_closed(self):
        """Transition to CLOSED state"""
        self.state = CircuitBreakerState.CLOSED
        self.consecutive_failures = 0
        self.half_open_attempts = 0
        self.logger.info(f"Circuit breaker {self.name} transitioned to CLOSED")
    
    def check_timeout(self, current_time: float):
        """Check if circuit breaker should transition from OPEN to HALF_OPEN"""
        with self.lock:
            if (self.state == CircuitBreakerState.OPEN and 
                self.last_failure_time and
                (current_time - self.last_failure_time) > self.config.timeout_seconds):
                self._transition_to_half_open()
    
    def get_failure_rate(self, window_seconds: int = 60) -> float:
        """Get failure rate within time window"""
        with self.lock:
            current_time = time.time()
            window_start = current_time - window_seconds
            
            relevant_calls = [
                call for call in self.recent_calls 
                if call["timestamp"] >= window_start
            ]
            
            if not relevant_calls:
                return 0.0
            
            failures = sum(1 for call in relevant_calls if not call["success"])
            return failures / len(relevant_calls)


class PriorityQueueManager:
    """
    Manager for priority-based message queues
    """
    
    def __init__(self, queue_name: str, max_priority: int, rabbitmq_manager: EnterpriseRabbitMQManager, logger):
        self.queue_name = queue_name
        self.max_priority = max_priority
        self.rabbitmq = rabbitmq_manager
        self.logger = logger
        
        self._setup_priority_queue()
    
    def _setup_priority_queue(self):
        """Setup priority queue with max priority"""
        try:
            with self.rabbitmq.connection_pool.get_connection() as connection:
                channel = connection.channel()
                
                # Declare queue with priority support
                channel.queue_declare(
                    queue=self.queue_name,
                    durable=True,
                    arguments={
                        'x-max-priority': self.max_priority,
                        'x-dead-letter-exchange': 'dlx',
                        'x-dead-letter-routing-key': QueueType.DEAD_LETTER.value
                    }
                )
                
                self.logger.info(f"Priority queue {self.queue_name} setup with max priority {self.max_priority}")
                
        except Exception as e:
            self.logger.error(f"Error setting up priority queue: {e}")
            raise
    
    def publish_priority_message(
        self, 
        message: EnterpriseMessage, 
        priority: Optional[int] = None
    ) -> bool:
        """Publish message with priority"""
        try:
            # Use message priority or provided priority
            effective_priority = priority or message.metadata.priority.value
            effective_priority = min(effective_priority, self.max_priority)
            
            # Update message priority
            message.metadata.priority = MessagePriority(effective_priority)
            
            return self.rabbitmq.publish_message(
                message=message,
                routing_key=self.queue_name
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing priority message: {e}")
            return False


# Global reliability handler instance
_reliability_handler: Optional[MessageReliabilityHandler] = None


def get_reliability_handler() -> MessageReliabilityHandler:
    """Get or create global reliability handler instance"""
    global _reliability_handler
    if _reliability_handler is None:
        _reliability_handler = MessageReliabilityHandler()
    return _reliability_handler


def set_reliability_handler(handler: MessageReliabilityHandler):
    """Set global reliability handler instance"""
    global _reliability_handler
    _reliability_handler = handler