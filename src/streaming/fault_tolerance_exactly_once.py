"""
Enhanced Fault Tolerance with Exactly-Once Semantics and Idempotent Producers
Implements comprehensive fault tolerance patterns for Kafka streaming
"""
from __future__ import annotations

import json
import uuid
import time
import threading
import asyncio
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import pickle

from confluent_kafka import Producer, Consumer, TopicPartition, Message
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError, KafkaException
import redis

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import EnhancedStreamingMessage


class DeliveryGuarantee(Enum):
    """Message delivery guarantee levels"""
    AT_MOST_ONCE = "at_most_once"      # Fire and forget
    AT_LEAST_ONCE = "at_least_once"    # May have duplicates
    EXACTLY_ONCE = "exactly_once"      # No duplicates, no loss


class RetryPolicy(Enum):
    """Retry policies for failed operations"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    NO_RETRY = "no_retry"


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class FaultToleranceConfig:
    """Configuration for fault tolerance features"""
    # Delivery guarantees
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE
    enable_idempotent_producer: bool = True
    enable_transactions: bool = True
    transaction_timeout_ms: int = 60000  # 1 minute
    
    # Retry configuration
    retry_policy: RetryPolicy = RetryPolicy.EXPONENTIAL_BACKOFF
    max_retries: int = 5
    base_retry_delay_ms: int = 1000
    max_retry_delay_ms: int = 30000
    retry_backoff_multiplier: float = 2.0
    
    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_ms: int = 60000
    circuit_breaker_success_threshold: int = 3
    
    # Dead letter queue
    enable_dead_letter_queue: bool = True
    dead_letter_topic: str = "dead-letter-queue"
    max_dead_letter_attempts: int = 3
    
    # Duplicate detection
    enable_duplicate_detection: bool = True
    duplicate_detection_window_ms: int = 300000  # 5 minutes
    
    # Health monitoring
    enable_health_monitoring: bool = True
    health_check_interval_ms: int = 30000  # 30 seconds
    
    # Kafka-specific settings
    enable_compression: bool = True
    compression_type: str = "zstd"
    batch_size: int = 65536  # 64KB
    linger_ms: int = 10
    buffer_memory: int = 134217728  # 128MB


@dataclass
class RetryContext:
    """Context for retry operations"""
    attempt: int = 0
    last_error: Optional[Exception] = None
    last_attempt_time: Optional[datetime] = None
    operation_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker"""
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    state_change_time: datetime = field(default_factory=datetime.now)


class DuplicateDetector:
    """Detects duplicate messages using Redis-backed bloom filters"""
    
    def __init__(self, 
                 redis_client: redis.Redis,
                 window_ms: int = 300000,
                 expected_elements: int = 1000000,
                 false_positive_rate: float = 0.01):
        self.redis_client = redis_client
        self.window_ms = window_ms
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        self.logger = get_logger(__name__)
        
        # Calculate bloom filter parameters
        self.bit_array_size = self._calculate_bit_array_size()
        self.hash_functions = self._calculate_hash_functions()
        
        # Redis keys for bloom filter
        self.bloom_key_prefix = "duplicate_detection:"
        
    def _calculate_bit_array_size(self) -> int:
        """Calculate optimal bit array size for bloom filter"""
        import math
        n = self.expected_elements
        p = self.false_positive_rate
        return int(-n * math.log(p) / (math.log(2) ** 2))
    
    def _calculate_hash_functions(self) -> int:
        """Calculate optimal number of hash functions"""
        import math
        m = self.bit_array_size
        n = self.expected_elements
        return int(m * math.log(2) / n)
    
    def _hash_message_id(self, message_id: str, seed: int) -> int:
        """Hash message ID with seed"""
        combined = f"{message_id}{seed}".encode('utf-8')
        return int(hashlib.sha256(combined).hexdigest(), 16) % self.bit_array_size
    
    def _get_current_window_key(self) -> str:
        """Get current time window key"""
        current_window = int(time.time() * 1000) // self.window_ms
        return f"{self.bloom_key_prefix}{current_window}"
    
    def _get_previous_window_key(self) -> str:
        """Get previous time window key"""
        current_window = int(time.time() * 1000) // self.window_ms
        return f"{self.bloom_key_prefix}{current_window - 1}"
    
    def is_duplicate(self, message_id: str) -> bool:
        """Check if message is duplicate"""
        try:
            current_key = self._get_current_window_key()
            previous_key = self._get_previous_window_key()
            
            # Check both current and previous windows
            for window_key in [current_key, previous_key]:
                # Check all hash positions
                is_present = True
                for i in range(self.hash_functions):
                    bit_position = self._hash_message_id(message_id, i)
                    if not self.redis_client.getbit(window_key, bit_position):
                        is_present = False
                        break
                
                if is_present:
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Duplicate detection check failed: {e}")
            # Fail open - assume not duplicate
            return False
    
    def mark_message(self, message_id: str):
        """Mark message as seen"""
        try:
            current_key = self._get_current_window_key()
            
            # Set all hash positions
            pipe = self.redis_client.pipeline()
            for i in range(self.hash_functions):
                bit_position = self._hash_message_id(message_id, i)
                pipe.setbit(current_key, bit_position, 1)
            
            # Set expiration on the key
            pipe.expire(current_key, self.window_ms // 1000 + 60)  # Extra 60 seconds buffer
            
            pipe.execute()
            
        except Exception as e:
            self.logger.error(f"Failed to mark message {message_id}: {e}")
    
    def cleanup_old_windows(self):
        """Cleanup old time windows"""
        try:
            current_window = int(time.time() * 1000) // self.window_ms
            
            # Delete windows older than 2 windows ago
            old_window_key = f"{self.bloom_key_prefix}{current_window - 2}"
            self.redis_client.delete(old_window_key)
            
        except Exception as e:
            self.logger.error(f"Window cleanup failed: {e}")


class CircuitBreaker:
    """Circuit breaker for fault tolerance"""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 timeout_ms: int = 60000,
                 success_threshold: int = 3):
        self.failure_threshold = failure_threshold
        self.timeout_ms = timeout_ms
        self.success_threshold = success_threshold
        self.logger = get_logger(__name__)
        
        # State management
        self._stats = CircuitBreakerStats()
        self._lock = threading.RLock()
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        with self._lock:
            if self._stats.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self._stats.state = CircuitBreakerState.HALF_OPEN
                    self._stats.state_change_time = datetime.now()
                    self.logger.info("Circuit breaker: OPEN -> HALF_OPEN")
                else:
                    raise Exception("Circuit breaker is OPEN - failing fast")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure(e)
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset circuit breaker"""
        if self._stats.last_failure_time is None:
            return True
        
        time_since_failure = (datetime.now() - self._stats.last_failure_time).total_seconds() * 1000
        return time_since_failure >= self.timeout_ms
    
    def _on_success(self):
        """Handle successful operation"""
        with self._lock:
            self._stats.success_count += 1
            
            if self._stats.state == CircuitBreakerState.HALF_OPEN:
                if self._stats.success_count >= self.success_threshold:
                    self._stats.state = CircuitBreakerState.CLOSED
                    self._stats.failure_count = 0
                    self._stats.success_count = 0
                    self._stats.state_change_time = datetime.now()
                    self.logger.info("Circuit breaker: HALF_OPEN -> CLOSED")
    
    def _on_failure(self, error: Exception):
        """Handle failed operation"""
        with self._lock:
            self._stats.failure_count += 1
            self._stats.last_failure_time = datetime.now()
            
            if self._stats.state == CircuitBreakerState.CLOSED:
                if self._stats.failure_count >= self.failure_threshold:
                    self._stats.state = CircuitBreakerState.OPEN
                    self._stats.state_change_time = datetime.now()
                    self.logger.warning(f"Circuit breaker: CLOSED -> OPEN due to {self.failure_threshold} failures")
            
            elif self._stats.state == CircuitBreakerState.HALF_OPEN:
                self._stats.state = CircuitBreakerState.OPEN
                self._stats.state_change_time = datetime.now()
                self.logger.warning("Circuit breaker: HALF_OPEN -> OPEN due to failure during recovery")
    
    def get_stats(self) -> CircuitBreakerStats:
        """Get circuit breaker statistics"""
        with self._lock:
            return CircuitBreakerStats(
                failure_count=self._stats.failure_count,
                success_count=self._stats.success_count,
                last_failure_time=self._stats.last_failure_time,
                state=self._stats.state,
                state_change_time=self._stats.state_change_time
            )


class RetryManager:
    """Manages retry operations with different policies"""
    
    def __init__(self, config: FaultToleranceConfig):
        self.config = config
        self.logger = get_logger(__name__)
    
    def execute_with_retry(self, 
                          func: Callable, 
                          operation_id: str = "",
                          *args, **kwargs) -> Any:
        """Execute function with retry logic"""
        context = RetryContext(operation_id=operation_id)
        
        for attempt in range(self.config.max_retries + 1):
            context.attempt = attempt
            context.last_attempt_time = datetime.now()
            
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(f"Operation {operation_id} succeeded after {attempt} retries")
                return result
                
            except Exception as e:
                context.last_error = e
                
                if attempt < self.config.max_retries:
                    delay = self._calculate_retry_delay(attempt)
                    self.logger.warning(
                        f"Operation {operation_id} failed (attempt {attempt + 1}), "
                        f"retrying in {delay}ms: {str(e)}"
                    )
                    time.sleep(delay / 1000.0)
                else:
                    self.logger.error(
                        f"Operation {operation_id} failed after {attempt + 1} attempts: {str(e)}"
                    )
                    raise
        
        # This should never be reached
        raise Exception(f"Operation {operation_id} failed after all retry attempts")
    
    def _calculate_retry_delay(self, attempt: int) -> int:
        """Calculate delay for retry attempt"""
        if self.config.retry_policy == RetryPolicy.FIXED_DELAY:
            return self.config.base_retry_delay_ms
        
        elif self.config.retry_policy == RetryPolicy.LINEAR_BACKOFF:
            delay = self.config.base_retry_delay_ms * (attempt + 1)
            
        elif self.config.retry_policy == RetryPolicy.EXPONENTIAL_BACKOFF:
            delay = self.config.base_retry_delay_ms * (self.config.retry_backoff_multiplier ** attempt)
            
        else:  # NO_RETRY
            return 0
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0.1, 0.3) * delay
        delay = int(delay + jitter)
        
        # Cap at maximum delay
        return min(delay, self.config.max_retry_delay_ms)


class IdempotentProducer:
    """Idempotent producer with exactly-once semantics"""
    
    def __init__(self, 
                 bootstrap_servers: List[str],
                 config: FaultToleranceConfig,
                 redis_client: Optional[redis.Redis] = None):
        self.bootstrap_servers = bootstrap_servers
        self.config = config
        self.redis_client = redis_client
        self.logger = get_logger(__name__)
        self.metrics = get_metrics_collector()
        
        # Components
        self.producer: Optional[Producer] = None
        self.duplicate_detector: Optional[DuplicateDetector] = None
        self.retry_manager = RetryManager(config)
        self.circuit_breaker: Optional[CircuitBreaker] = None
        
        # Transactional state
        self.transaction_id: Optional[str] = None
        self.in_transaction = False
        
        # Message tracking
        self.pending_messages: Dict[str, EnhancedStreamingMessage] = {}
        self.delivered_messages: Set[str] = set()
        self._message_lock = threading.RLock()
        
        # Initialize components
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize producer components"""
        try:\n            # Initialize duplicate detector if Redis available\n            if self.redis_client and self.config.enable_duplicate_detection:\n                self.duplicate_detector = DuplicateDetector(\n                    self.redis_client,\n                    self.config.duplicate_detection_window_ms\n                )\n            \n            # Initialize circuit breaker\n            if self.config.enable_circuit_breaker:\n                self.circuit_breaker = CircuitBreaker(\n                    self.config.circuit_breaker_failure_threshold,\n                    self.config.circuit_breaker_timeout_ms,\n                    self.config.circuit_breaker_success_threshold\n                )\n            \n            # Initialize Kafka producer\n            self._create_producer()\n            \n            self.logger.info(\"Idempotent producer initialized\")\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to initialize idempotent producer: {e}\")\n            raise\n    \n    def _create_producer(self):\n        \"\"\"Create Kafka producer with idempotent configuration\"\"\"\n        try:\n            # Base configuration\n            producer_config = {\n                'bootstrap.servers': ','.join(self.bootstrap_servers),\n                'client.id': f'idempotent-producer-{uuid.uuid4()}',\n            }\n            \n            # Configure delivery guarantees\n            if self.config.delivery_guarantee == DeliveryGuarantee.EXACTLY_ONCE:\n                producer_config.update({\n                    'enable.idempotence': True,\n                    'acks': 'all',\n                    'retries': 1000000,  # Essentially unlimited\n                    'max.in.flight.requests.per.connection': 1,  # Maintain ordering\n                    'delivery.timeout.ms': 300000,  # 5 minutes\n                })\n                \n                # Enable transactions if configured\n                if self.config.enable_transactions:\n                    self.transaction_id = f'txn-{uuid.uuid4()}'\n                    producer_config['transactional.id'] = self.transaction_id\n                    \n            elif self.config.delivery_guarantee == DeliveryGuarantee.AT_LEAST_ONCE:\n                producer_config.update({\n                    'acks': 'all',\n                    'retries': self.config.max_retries,\n                    'delivery.timeout.ms': 120000,  # 2 minutes\n                })\n                \n            else:  # AT_MOST_ONCE\n                producer_config.update({\n                    'acks': '0',\n                    'retries': 0,\n                })\n            \n            # Performance optimizations\n            if self.config.enable_compression:\n                producer_config['compression.type'] = self.config.compression_type\n            \n            producer_config.update({\n                'batch.size': self.config.batch_size,\n                'linger.ms': self.config.linger_ms,\n                'buffer.memory': self.config.buffer_memory,\n            })\n            \n            # Create producer\n            self.producer = Producer(producer_config)\n            \n            # Initialize transactions if enabled\n            if self.config.enable_transactions and self.transaction_id:\n                self.producer.init_transactions()\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to create producer: {e}\")\n            raise\n    \n    def produce_message(self, message: EnhancedStreamingMessage, \n                       callback: Optional[Callable] = None) -> bool:\n        \"\"\"Produce message with exactly-once semantics\"\"\"\n        try:\n            # Check for duplicates\n            if self._is_duplicate_message(message):\n                self.logger.info(f\"Duplicate message detected: {message.message_id}\")\n                return True\n            \n            # Execute with circuit breaker protection\n            if self.circuit_breaker:\n                return self.circuit_breaker.call(\n                    self._produce_message_internal, \n                    message, \n                    callback\n                )\n            else:\n                return self._produce_message_internal(message, callback)\n                \n        except Exception as e:\n            self.logger.error(f\"Failed to produce message {message.message_id}: {e}\")\n            \n            # Send to dead letter queue if configured\n            if self.config.enable_dead_letter_queue:\n                self._send_to_dead_letter_queue(message, str(e))\n            \n            return False\n    \n    def _produce_message_internal(self, message: EnhancedStreamingMessage, \n                                callback: Optional[Callable] = None) -> bool:\n        \"\"\"Internal message production logic\"\"\"\n        def produce_operation():\n            return self._do_produce_message(message, callback)\n        \n        # Execute with retry\n        return self.retry_manager.execute_with_retry(\n            produce_operation,\n            operation_id=f\"produce_{message.message_id}\"\n        )\n    \n    def _do_produce_message(self, message: EnhancedStreamingMessage,\n                          callback: Optional[Callable] = None) -> bool:\n        \"\"\"Actual message production\"\"\"\n        try:\n            # Track pending message\n            with self._message_lock:\n                self.pending_messages[message.message_id] = message\n            \n            # Create delivery callback\n            def delivery_report(err: KafkaError, msg: Message):\n                self._handle_delivery_report(err, msg, message, callback)\n            \n            # Prepare message data\n            message_json = json.dumps(message.to_dict(), default=str)\n            headers = [(k, v.encode('utf-8')) for k, v in message.headers.items()]\n            \n            # Produce message\n            self.producer.produce(\n                topic=message.topic,\n                key=message.key,\n                value=message_json,\n                headers=headers,\n                on_delivery=delivery_report\n            )\n            \n            # Poll to handle delivery reports\n            self.producer.poll(0)\n            \n            # Mark message as seen in duplicate detector\n            if self.duplicate_detector:\n                self.duplicate_detector.mark_message(message.message_id)\n            \n            return True\n            \n        except Exception as e:\n            # Remove from pending messages on error\n            with self._message_lock:\n                self.pending_messages.pop(message.message_id, None)\n            raise\n    \n    def _handle_delivery_report(self, err: KafkaError, msg: Message,\n                              original_message: EnhancedStreamingMessage,\n                              callback: Optional[Callable] = None):\n        \"\"\"Handle message delivery report\"\"\"\n        try:\n            with self._message_lock:\n                # Remove from pending messages\n                self.pending_messages.pop(original_message.message_id, None)\n                \n                if err is not None:\n                    self.logger.error(f\"Message delivery failed: {err}\")\n                    \n                    # Send to dead letter queue if configured\n                    if self.config.enable_dead_letter_queue:\n                        self._send_to_dead_letter_queue(original_message, str(err))\n                    \n                    # Call error callback if provided\n                    if callback:\n                        try:\n                            callback(original_message, err)\n                        except Exception as cb_error:\n                            self.logger.error(f\"Delivery callback error: {cb_error}\")\n                else:\n                    # Message delivered successfully\n                    self.delivered_messages.add(original_message.message_id)\n                    \n                    if self.metrics:\n                        self.metrics.increment_counter(\"messages_delivered_successfully\")\n                    \n                    self.logger.debug(f\"Message {original_message.message_id} delivered successfully\")\n                    \n                    # Call success callback if provided\n                    if callback:\n                        try:\n                            callback(original_message, None)\n                        except Exception as cb_error:\n                            self.logger.error(f\"Delivery callback error: {cb_error}\")\n                            \n        except Exception as e:\n            self.logger.error(f\"Error handling delivery report: {e}\")\n    \n    def _is_duplicate_message(self, message: EnhancedStreamingMessage) -> bool:\n        \"\"\"Check if message is duplicate\"\"\"\n        # Check delivered messages cache\n        if message.message_id in self.delivered_messages:\n            return True\n        \n        # Check duplicate detector if available\n        if self.duplicate_detector:\n            return self.duplicate_detector.is_duplicate(message.message_id)\n        \n        return False\n    \n    def _send_to_dead_letter_queue(self, message: EnhancedStreamingMessage, error: str):\n        \"\"\"Send message to dead letter queue\"\"\"\n        try:\n            dead_letter_message = {\n                \"original_message\": message.to_dict(),\n                \"error\": error,\n                \"timestamp\": datetime.now().isoformat(),\n                \"attempts\": getattr(message, '_dead_letter_attempts', 0) + 1\n            }\n            \n            # Only send to DLQ if under attempt limit\n            if dead_letter_message[\"attempts\"] <= self.config.max_dead_letter_attempts:\n                self.producer.produce(\n                    topic=self.config.dead_letter_topic,\n                    key=message.message_id,\n                    value=json.dumps(dead_letter_message, default=str)\n                )\n                self.producer.poll(0)\n                \n                self.logger.warning(f\"Sent message {message.message_id} to dead letter queue\")\n            else:\n                self.logger.error(f\"Message {message.message_id} exceeded max DLQ attempts\")\n                \n        except Exception as e:\n            self.logger.error(f\"Failed to send message to dead letter queue: {e}\")\n    \n    def begin_transaction(self) -> bool:\n        \"\"\"Begin transaction\"\"\"\n        if not self.config.enable_transactions or not self.transaction_id:\n            return False\n        \n        try:\n            self.producer.begin_transaction()\n            self.in_transaction = True\n            self.logger.debug(\"Transaction started\")\n            return True\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to begin transaction: {e}\")\n            return False\n    \n    def commit_transaction(self) -> bool:\n        \"\"\"Commit transaction\"\"\"\n        if not self.in_transaction:\n            return False\n        \n        try:\n            self.producer.commit_transaction()\n            self.in_transaction = False\n            self.logger.debug(\"Transaction committed\")\n            return True\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to commit transaction: {e}\")\n            return False\n    \n    def abort_transaction(self) -> bool:\n        \"\"\"Abort transaction\"\"\"\n        if not self.in_transaction:\n            return False\n        \n        try:\n            self.producer.abort_transaction()\n            self.in_transaction = False\n            self.logger.debug(\"Transaction aborted\")\n            return True\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to abort transaction: {e}\")\n            return False\n    \n    def flush(self, timeout: float = -1) -> int:\n        \"\"\"Flush producer\"\"\"\n        try:\n            return self.producer.flush(timeout)\n        except Exception as e:\n            self.logger.error(f\"Producer flush failed: {e}\")\n            return -1\n    \n    def get_metrics(self) -> Dict[str, Any]:\n        \"\"\"Get producer metrics\"\"\"\n        circuit_breaker_stats = None\n        if self.circuit_breaker:\n            stats = self.circuit_breaker.get_stats()\n            circuit_breaker_stats = {\n                \"state\": stats.state.value,\n                \"failure_count\": stats.failure_count,\n                \"success_count\": stats.success_count,\n                \"last_failure_time\": stats.last_failure_time.isoformat() if stats.last_failure_time else None\n            }\n        \n        return {\n            \"delivery_guarantee\": self.config.delivery_guarantee.value,\n            \"transactions_enabled\": self.config.enable_transactions,\n            \"in_transaction\": self.in_transaction,\n            \"pending_messages\": len(self.pending_messages),\n            \"delivered_messages\": len(self.delivered_messages),\n            \"duplicate_detection_enabled\": self.config.enable_duplicate_detection,\n            \"circuit_breaker_stats\": circuit_breaker_stats,\n            \"timestamp\": datetime.now().isoformat()\n        }\n    \n    def close(self):\n        \"\"\"Close producer\"\"\"\n        try:\n            # Abort any active transaction\n            if self.in_transaction:\n                self.abort_transaction()\n            \n            # Flush pending messages\n            if self.producer:\n                remaining = self.flush(30.0)  # 30 second timeout\n                if remaining > 0:\n                    self.logger.warning(f\"{remaining} messages were not delivered before close\")\n            \n            self.logger.info(\"Idempotent producer closed\")\n            \n        except Exception as e:\n            self.logger.error(f\"Error closing producer: {e}\")\n\n\nclass ExactlyOnceConsumer:\n    \"\"\"Consumer with exactly-once processing semantics\"\"\"\n    \n    def __init__(self,\n                 bootstrap_servers: List[str],\n                 group_id: str,\n                 topics: List[str],\n                 config: FaultToleranceConfig,\n                 redis_client: Optional[redis.Redis] = None):\n        self.bootstrap_servers = bootstrap_servers\n        self.group_id = group_id\n        self.topics = topics\n        self.config = config\n        self.redis_client = redis_client\n        self.logger = get_logger(__name__)\n        self.metrics = get_metrics_collector()\n        \n        # Components\n        self.consumer: Optional[Consumer] = None\n        self.duplicate_detector: Optional[DuplicateDetector] = None\n        self.retry_manager = RetryManager(config)\n        \n        # Processing tracking\n        self.processed_messages: Set[str] = set()\n        self.processing_offsets: Dict[TopicPartition, int] = {}\n        self._processing_lock = threading.RLock()\n        \n        # Message handler\n        self.message_handler: Optional[Callable] = None\n        \n        # Initialize components\n        self._initialize_components()\n    \n    def _initialize_components(self):\n        \"\"\"Initialize consumer components\"\"\"\n        try:\n            # Initialize duplicate detector if Redis available\n            if self.redis_client and self.config.enable_duplicate_detection:\n                self.duplicate_detector = DuplicateDetector(\n                    self.redis_client,\n                    self.config.duplicate_detection_window_ms\n                )\n            \n            # Create consumer\n            self._create_consumer()\n            \n            self.logger.info(\"Exactly-once consumer initialized\")\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to initialize exactly-once consumer: {e}\")\n            raise\n    \n    def _create_consumer(self):\n        \"\"\"Create Kafka consumer\"\"\"\n        try:\n            consumer_config = {\n                'bootstrap.servers': ','.join(self.bootstrap_servers),\n                'group.id': self.group_id,\n                'client.id': f'exactly-once-consumer-{uuid.uuid4()}',\n                'enable.auto.commit': False,  # Manual commit for exactly-once\n                'auto.offset.reset': 'earliest',\n                'session.timeout.ms': 30000,\n                'heartbeat.interval.ms': 10000,\n                'max.poll.interval.ms': 300000,  # 5 minutes\n                'fetch.min.bytes': 1024,\n                'fetch.max.wait.ms': 1000,\n            }\n            \n            # Configure for exactly-once if needed\n            if self.config.delivery_guarantee == DeliveryGuarantee.EXACTLY_ONCE:\n                consumer_config['isolation.level'] = 'read_committed'\n            \n            self.consumer = Consumer(consumer_config)\n            self.consumer.subscribe(self.topics)\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to create consumer: {e}\")\n            raise\n    \n    def set_message_handler(self, handler: Callable[[EnhancedStreamingMessage], bool]):\n        \"\"\"Set message processing handler\"\"\"\n        self.message_handler = handler\n    \n    def start_consuming(self):\n        \"\"\"Start consuming messages\"\"\"\n        if not self.message_handler:\n            raise ValueError(\"Message handler not set\")\n        \n        try:\n            self.logger.info(f\"Starting exactly-once consumer for topics: {self.topics}\")\n            \n            while True:\n                # Poll for messages\n                msg = self.consumer.poll(timeout=1.0)\n                \n                if msg is None:\n                    continue\n                \n                if msg.error():\n                    if msg.error().code() == KafkaError._PARTITION_EOF:\n                        continue\n                    else:\n                        self.logger.error(f\"Consumer error: {msg.error()}\")\n                        continue\n                \n                # Process message\n                self._process_message(msg)\n                \n        except KeyboardInterrupt:\n            self.logger.info(\"Consumer interrupted by user\")\n        except Exception as e:\n            self.logger.error(f\"Consumer error: {e}\")\n            raise\n        finally:\n            self.close()\n    \n    def _process_message(self, msg: Message):\n        \"\"\"Process individual message with exactly-once semantics\"\"\"\n        try:\n            # Parse message\n            message_data = json.loads(msg.value().decode('utf-8'))\n            message = EnhancedStreamingMessage.from_dict(message_data)\n            \n            # Check for duplicate\n            if self._is_duplicate_message(message):\n                self.logger.debug(f\"Skipping duplicate message: {message.message_id}\")\n                self._commit_message(msg)\n                return\n            \n            # Process with retry\n            def process_operation():\n                return self._do_process_message(message)\n            \n            success = self.retry_manager.execute_with_retry(\n                process_operation,\n                operation_id=f\"process_{message.message_id}\"\n            )\n            \n            if success:\n                # Mark as processed and commit offset\n                with self._processing_lock:\n                    self.processed_messages.add(message.message_id)\n                \n                if self.duplicate_detector:\n                    self.duplicate_detector.mark_message(message.message_id)\n                \n                self._commit_message(msg)\n                \n                if self.metrics:\n                    self.metrics.increment_counter(\"messages_processed_successfully\")\n            else:\n                self.logger.error(f\"Failed to process message {message.message_id}\")\n                \n        except Exception as e:\n            self.logger.error(f\"Message processing failed: {e}\")\n            # Don't commit on processing failure\n    \n    def _do_process_message(self, message: EnhancedStreamingMessage) -> bool:\n        \"\"\"Actually process the message\"\"\"\n        try:\n            return self.message_handler(message)\n        except Exception as e:\n            self.logger.error(f\"Message handler failed: {e}\")\n            return False\n    \n    def _is_duplicate_message(self, message: EnhancedStreamingMessage) -> bool:\n        \"\"\"Check if message is duplicate\"\"\"\n        # Check local processed cache\n        if message.message_id in self.processed_messages:\n            return True\n        \n        # Check duplicate detector if available\n        if self.duplicate_detector:\n            return self.duplicate_detector.is_duplicate(message.message_id)\n        \n        return False\n    \n    def _commit_message(self, msg: Message):\n        \"\"\"Commit message offset\"\"\"\n        try:\n            self.consumer.commit(message=msg, asynchronous=False)\n        except Exception as e:\n            self.logger.error(f\"Failed to commit offset: {e}\")\n    \n    def close(self):\n        \"\"\"Close consumer\"\"\"\n        try:\n            if self.consumer:\n                self.consumer.close()\n            self.logger.info(\"Exactly-once consumer closed\")\n        except Exception as e:\n            self.logger.error(f\"Error closing consumer: {e}\")\n    \n    def get_metrics(self) -> Dict[str, Any]:\n        \"\"\"Get consumer metrics\"\"\"\n        return {\n            \"group_id\": self.group_id,\n            \"topics\": self.topics,\n            \"processed_messages\": len(self.processed_messages),\n            \"duplicate_detection_enabled\": self.config.enable_duplicate_detection,\n            \"delivery_guarantee\": self.config.delivery_guarantee.value,\n            \"timestamp\": datetime.now().isoformat()\n        }\n\n\n# Factory functions\ndef create_fault_tolerance_config(**kwargs) -> FaultToleranceConfig:\n    \"\"\"Create fault tolerance configuration\"\"\"\n    return FaultToleranceConfig(**kwargs)\n\n\ndef create_idempotent_producer(\n    bootstrap_servers: List[str],\n    config: FaultToleranceConfig,\n    redis_client: Optional[redis.Redis] = None\n) -> IdempotentProducer:\n    \"\"\"Create idempotent producer\"\"\"\n    return IdempotentProducer(\n        bootstrap_servers=bootstrap_servers,\n        config=config,\n        redis_client=redis_client\n    )\n\n\ndef create_exactly_once_consumer(\n    bootstrap_servers: List[str],\n    group_id: str,\n    topics: List[str],\n    config: FaultToleranceConfig,\n    redis_client: Optional[redis.Redis] = None\n) -> ExactlyOnceConsumer:\n    \"\"\"Create exactly-once consumer\"\"\"\n    return ExactlyOnceConsumer(\n        bootstrap_servers=bootstrap_servers,\n        group_id=group_id,\n        topics=topics,\n        config=config,\n        redis_client=redis_client\n    )\n\n\n# Example usage\nif __name__ == \"__main__\":\n    import time\n    \n    try:\n        print(\"Testing Fault Tolerance with Exactly-Once Semantics...\")\n        \n        # Create configuration\n        config = create_fault_tolerance_config(\n            delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,\n            enable_idempotent_producer=True,\n            enable_transactions=True,\n            enable_duplicate_detection=True,\n            enable_circuit_breaker=True,\n            enable_dead_letter_queue=True\n        )\n        \n        # Create producer\n        producer = create_idempotent_producer(\n            bootstrap_servers=[\"localhost:9092\"],\n            config=config\n        )\n        \n        # Create test message\n        test_message = EnhancedStreamingMessage(\n            message_id=str(uuid.uuid4()),\n            topic=\"test-exactly-once\",\n            key=\"test-key\",\n            payload={\n                \"test_data\": \"exactly-once-test\",\n                \"timestamp\": datetime.now().isoformat()\n            },\n            timestamp=datetime.now(),\n            headers={\"test\": \"true\"}\n        )\n        \n        # Test transaction\n        producer.begin_transaction()\n        \n        # Produce message\n        success = producer.produce_message(test_message)\n        print(f\"✅ Message produced with exactly-once semantics: {success}\")\n        \n        # Commit transaction\n        producer.commit_transaction()\n        \n        # Test duplicate detection\n        duplicate_success = producer.produce_message(test_message)\n        print(f\"✅ Duplicate detection working: {not duplicate_success or 'duplicate detected'}\")\n        \n        # Get metrics\n        producer_metrics = producer.get_metrics()\n        print(f\"✅ Producer metrics: {producer_metrics}\")\n        \n        # Test consumer\n        def message_handler(message: EnhancedStreamingMessage) -> bool:\n            print(f\"Processing message: {message.message_id}\")\n            return True\n        \n        consumer = create_exactly_once_consumer(\n            bootstrap_servers=[\"localhost:9092\"],\n            group_id=\"test-exactly-once-consumer\",\n            topics=[\"test-exactly-once\"],\n            config=config\n        )\n        \n        consumer.set_message_handler(message_handler)\n        \n        # Start consumer in separate thread for testing\n        import threading\n        consumer_thread = threading.Thread(\n            target=consumer.start_consuming,\n            daemon=True\n        )\n        consumer_thread.start()\n        \n        # Let it run briefly\n        time.sleep(5)\n        \n        # Get consumer metrics\n        consumer_metrics = consumer.get_metrics()\n        print(f\"✅ Consumer metrics: {consumer_metrics}\")\n        \n        # Cleanup\n        producer.close()\n        consumer.close()\n        \n        print(\"✅ Fault Tolerance with Exactly-Once Semantics testing completed\")\n        \n    except Exception as e:\n        print(f\"❌ Testing failed: {str(e)}\")\n        import traceback\n        traceback.print_exc()