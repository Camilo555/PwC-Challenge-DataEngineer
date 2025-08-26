"""
Enhanced Logging and Error Tracking for Messaging Operations
Provides comprehensive logging, error tracking, and audit trails for RabbitMQ and Kafka
"""
import json
import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from core.logging import get_logger


class LogLevel(Enum):
    """Logging levels for messaging operations"""
    DEBUG = "debug"
    INFO = "info" 
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MessageEventType(Enum):
    """Types of message events to log"""
    PUBLISH_ATTEMPT = "publish_attempt"
    PUBLISH_SUCCESS = "publish_success"
    PUBLISH_FAILURE = "publish_failure"
    CONSUME_ATTEMPT = "consume_attempt"
    CONSUME_SUCCESS = "consume_success"
    CONSUME_FAILURE = "consume_failure"
    PROCESS_START = "process_start"
    PROCESS_SUCCESS = "process_success"
    PROCESS_FAILURE = "process_failure"
    ACK = "acknowledge"
    NACK = "negative_acknowledge"
    REJECT = "reject"
    DEAD_LETTER = "dead_letter"
    RETRY = "retry"
    CONNECTION_ESTABLISHED = "connection_established"
    CONNECTION_LOST = "connection_lost"
    CONNECTION_FAILED = "connection_failed"
    HEALTH_CHECK = "health_check"
    METRIC_COLLECTION = "metric_collection"


@dataclass
class MessageLogEntry:
    """Structured log entry for message operations"""
    timestamp: datetime
    event_type: MessageEventType
    component: str  # 'rabbitmq', 'kafka', 'messaging'
    level: LogLevel
    message: str
    message_id: Optional[str] = None
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    queue_or_topic: Optional[str] = None
    consumer_group: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    stack_trace: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "component": self.component,
            "level": self.level.value,
            "message": self.message,
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "queue_or_topic": self.queue_or_topic,
            "consumer_group": self.consumer_group,
            "partition": self.partition,
            "offset": self.offset,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "metadata": self.metadata
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str)


class MessageErrorTracker:
    """Tracks and analyzes messaging errors"""
    
    def __init__(self, max_errors: int = 1000):
        self.max_errors = max_errors
        self.errors: List[MessageLogEntry] = []
        self.error_stats: Dict[str, Dict[str, int]] = {
            "by_component": {},
            "by_error_type": {},
            "by_queue_topic": {}
        }
        self.logger = get_logger(__name__)
    
    def record_error(self, log_entry: MessageLogEntry):
        """Record an error for tracking and analysis"""
        if log_entry.level not in [LogLevel.ERROR, LogLevel.CRITICAL]:
            return
        
        # Store error
        self.errors.append(log_entry)
        
        # Maintain size limit
        if len(self.errors) > self.max_errors:
            self.errors = self.errors[-self.max_errors:]
        
        # Update statistics
        self._update_error_stats(log_entry)
        
        self.logger.debug(f"Recorded error for tracking: {log_entry.error_type}")
    
    def _update_error_stats(self, log_entry: MessageLogEntry):
        """Update error statistics"""
        # By component
        component = log_entry.component
        self.error_stats["by_component"][component] = self.error_stats["by_component"].get(component, 0) + 1
        
        # By error type
        if log_entry.error_type:
            error_type = log_entry.error_type
            self.error_stats["by_error_type"][error_type] = self.error_stats["by_error_type"].get(error_type, 0) + 1
        
        # By queue/topic
        if log_entry.queue_or_topic:
            queue_topic = log_entry.queue_or_topic
            self.error_stats["by_queue_topic"][queue_topic] = self.error_stats["by_queue_topic"].get(queue_topic, 0) + 1
    
    def get_error_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get error summary for specified time period"""
        cutoff_time = datetime.now().timestamp() - (hours * 3600)
        
        recent_errors = [
            error for error in self.errors
            if error.timestamp.timestamp() > cutoff_time
        ]
        
        if not recent_errors:
            return {
                "total_errors": 0,
                "time_period_hours": hours,
                "timestamp": datetime.now().isoformat()
            }
        
        # Calculate error rates
        component_errors = {}
        error_type_counts = {}
        
        for error in recent_errors:
            # Count by component
            component = error.component
            component_errors[component] = component_errors.get(component, 0) + 1
            
            # Count by error type
            if error.error_type:
                error_type_counts[error.error_type] = error_type_counts.get(error.error_type, 0) + 1
        
        return {
            "total_errors": len(recent_errors),
            "time_period_hours": hours,
            "errors_by_component": component_errors,
            "errors_by_type": error_type_counts,
            "error_rate_per_hour": len(recent_errors) / hours,
            "most_common_error": max(error_type_counts, key=error_type_counts.get) if error_type_counts else None,
            "timestamp": datetime.now().isoformat()
        }
    
    def get_top_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most common errors"""
        error_counts = {}
        
        for error in self.errors:
            key = f"{error.component}:{error.error_type or 'unknown'}"
            if key not in error_counts:
                error_counts[key] = {
                    "component": error.component,
                    "error_type": error.error_type,
                    "count": 0,
                    "latest_occurrence": error.timestamp,
                    "sample_message": error.error_message
                }
            
            error_counts[key]["count"] += 1
            if error.timestamp > error_counts[key]["latest_occurrence"]:
                error_counts[key]["latest_occurrence"] = error.timestamp
                error_counts[key]["sample_message"] = error.error_message
        
        # Sort by count
        sorted_errors = sorted(error_counts.values(), key=lambda x: x["count"], reverse=True)
        
        # Convert timestamps to ISO format
        for error in sorted_errors:
            error["latest_occurrence"] = error["latest_occurrence"].isoformat()
        
        return sorted_errors[:limit]


class MessagingLogger:
    """Centralized logger for messaging operations"""
    
    def __init__(self, component: str):
        self.component = component
        self.logger = get_logger(f"messaging.{component}")
        self.error_tracker = MessageErrorTracker()
        
        # Log history for analysis
        self.log_history: List[MessageLogEntry] = []
        self.max_history = 5000
    
    def _create_log_entry(
        self,
        event_type: MessageEventType,
        level: LogLevel,
        message: str,
        message_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        queue_or_topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        error: Optional[Exception] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> MessageLogEntry:
        """Create a structured log entry"""
        
        # Extract error information if provided
        error_type = None
        error_message = None
        stack_trace = None
        
        if error:
            error_type = error.__class__.__name__
            error_message = str(error)
            stack_trace = traceback.format_exc()
        
        return MessageLogEntry(
            timestamp=datetime.now(),
            event_type=event_type,
            component=self.component,
            level=level,
            message=message,
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            span_id=span_id,
            queue_or_topic=queue_or_topic,
            consumer_group=consumer_group,
            partition=partition,
            offset=offset,
            error_type=error_type,
            error_message=error_message,
            stack_trace=stack_trace,
            metadata=metadata or {}
        )
    
    def _log_entry(self, log_entry: MessageLogEntry):
        """Log an entry using appropriate log level"""
        
        # Create structured message
        log_data = {
            "event_type": log_entry.event_type.value,
            "message_id": log_entry.message_id,
            "correlation_id": log_entry.correlation_id,
            "queue_or_topic": log_entry.queue_or_topic
        }
        
        # Add tracing information if available
        if log_entry.trace_id:
            log_data["trace_id"] = log_entry.trace_id
        if log_entry.span_id:
            log_data["span_id"] = log_entry.span_id
        
        # Create formatted message
        formatted_message = f"{log_entry.message} | {json.dumps(log_data, default=str)}"
        
        # Log at appropriate level
        if log_entry.level == LogLevel.DEBUG:
            self.logger.debug(formatted_message)
        elif log_entry.level == LogLevel.INFO:
            self.logger.info(formatted_message)
        elif log_entry.level == LogLevel.WARNING:
            self.logger.warning(formatted_message)
        elif log_entry.level == LogLevel.ERROR:
            self.logger.error(formatted_message)
            if log_entry.stack_trace:
                self.logger.error(f"Stack trace: {log_entry.stack_trace}")
        elif log_entry.level == LogLevel.CRITICAL:
            self.logger.critical(formatted_message)
            if log_entry.stack_trace:
                self.logger.critical(f"Stack trace: {log_entry.stack_trace}")
        
        # Store in history
        self.log_history.append(log_entry)
        if len(self.log_history) > self.max_history:
            self.log_history = self.log_history[-self.max_history:]
        
        # Track errors
        if log_entry.level in [LogLevel.ERROR, LogLevel.CRITICAL]:
            self.error_tracker.record_error(log_entry)
    
    def log_publish_attempt(
        self,
        queue_or_topic: str,
        message_id: str,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log message publish attempt"""
        log_entry = self._create_log_entry(
            MessageEventType.PUBLISH_ATTEMPT,
            LogLevel.DEBUG,
            f"Attempting to publish message to {queue_or_topic}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            queue_or_topic=queue_or_topic,
            metadata=metadata
        )
        self._log_entry(log_entry)
    
    def log_publish_success(
        self,
        queue_or_topic: str,
        message_id: str,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log successful message publish"""
        log_entry = self._create_log_entry(
            MessageEventType.PUBLISH_SUCCESS,
            LogLevel.INFO,
            f"Successfully published message to {queue_or_topic}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            queue_or_topic=queue_or_topic,
            metadata=metadata
        )
        self._log_entry(log_entry)
    
    def log_publish_failure(
        self,
        queue_or_topic: str,
        message_id: str,
        error: Exception,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log message publish failure"""
        log_entry = self._create_log_entry(
            MessageEventType.PUBLISH_FAILURE,
            LogLevel.ERROR,
            f"Failed to publish message to {queue_or_topic}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            queue_or_topic=queue_or_topic,
            error=error,
            metadata=metadata
        )
        self._log_entry(log_entry)
    
    def log_consume_attempt(
        self,
        queue_or_topic: str,
        message_id: Optional[str] = None,
        consumer_group: Optional[str] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        trace_id: Optional[str] = None
    ):
        """Log message consume attempt"""
        log_entry = self._create_log_entry(
            MessageEventType.CONSUME_ATTEMPT,
            LogLevel.DEBUG,
            f"Attempting to consume message from {queue_or_topic}",
            message_id=message_id,
            trace_id=trace_id,
            queue_or_topic=queue_or_topic,
            consumer_group=consumer_group,
            partition=partition,
            offset=offset
        )
        self._log_entry(log_entry)
    
    def log_consume_success(
        self,
        queue_or_topic: str,
        message_id: str,
        correlation_id: Optional[str] = None,
        consumer_group: Optional[str] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        trace_id: Optional[str] = None,
        processing_time_ms: Optional[float] = None
    ):
        """Log successful message consumption"""
        metadata = {}
        if processing_time_ms is not None:
            metadata["processing_time_ms"] = processing_time_ms
            
        log_entry = self._create_log_entry(
            MessageEventType.CONSUME_SUCCESS,
            LogLevel.INFO,
            f"Successfully consumed message from {queue_or_topic}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            queue_or_topic=queue_or_topic,
            consumer_group=consumer_group,
            partition=partition,
            offset=offset,
            metadata=metadata
        )
        self._log_entry(log_entry)
    
    def log_consume_failure(
        self,
        queue_or_topic: str,
        error: Exception,
        message_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        consumer_group: Optional[str] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        trace_id: Optional[str] = None
    ):
        """Log message consume failure"""
        log_entry = self._create_log_entry(
            MessageEventType.CONSUME_FAILURE,
            LogLevel.ERROR,
            f"Failed to consume message from {queue_or_topic}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            queue_or_topic=queue_or_topic,
            consumer_group=consumer_group,
            partition=partition,
            offset=offset,
            error=error
        )
        self._log_entry(log_entry)
    
    def log_process_start(
        self,
        processor_name: str,
        message_id: str,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log start of message processing"""
        log_entry = self._create_log_entry(
            MessageEventType.PROCESS_START,
            LogLevel.INFO,
            f"Started processing message with {processor_name}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            metadata={**(metadata or {}), "processor": processor_name}
        )
        self._log_entry(log_entry)
    
    def log_process_success(
        self,
        processor_name: str,
        message_id: str,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        processing_time_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log successful message processing"""
        meta = {**(metadata or {}), "processor": processor_name}
        if processing_time_ms is not None:
            meta["processing_time_ms"] = processing_time_ms
            
        log_entry = self._create_log_entry(
            MessageEventType.PROCESS_SUCCESS,
            LogLevel.INFO,
            f"Successfully processed message with {processor_name}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            metadata=meta
        )
        self._log_entry(log_entry)
    
    def log_process_failure(
        self,
        processor_name: str,
        message_id: str,
        error: Exception,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        processing_time_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log message processing failure"""
        meta = {**(metadata or {}), "processor": processor_name}
        if processing_time_ms is not None:
            meta["processing_time_ms"] = processing_time_ms
            
        log_entry = self._create_log_entry(
            MessageEventType.PROCESS_FAILURE,
            LogLevel.ERROR,
            f"Failed to process message with {processor_name}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            error=error,
            metadata=meta
        )
        self._log_entry(log_entry)
    
    def log_dead_letter(
        self,
        queue_or_topic: str,
        message_id: str,
        reason: str,
        correlation_id: Optional[str] = None,
        retry_count: Optional[int] = None,
        trace_id: Optional[str] = None
    ):
        """Log message sent to dead letter queue"""
        metadata = {"reason": reason}
        if retry_count is not None:
            metadata["retry_count"] = retry_count
            
        log_entry = self._create_log_entry(
            MessageEventType.DEAD_LETTER,
            LogLevel.WARNING,
            f"Message sent to dead letter queue from {queue_or_topic}: {reason}",
            message_id=message_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            queue_or_topic=queue_or_topic,
            metadata=metadata
        )
        self._log_entry(log_entry)
    
    def log_connection_event(
        self,
        event_type: MessageEventType,
        connection_info: str,
        error: Optional[Exception] = None
    ):
        """Log connection events"""
        level = LogLevel.INFO
        if event_type == MessageEventType.CONNECTION_FAILED:
            level = LogLevel.ERROR
        elif event_type == MessageEventType.CONNECTION_LOST:
            level = LogLevel.WARNING
            
        log_entry = self._create_log_entry(
            event_type,
            level,
            f"Connection event: {connection_info}",
            error=error,
            metadata={"connection_info": connection_info}
        )
        self._log_entry(log_entry)
    
    @contextmanager
    def log_operation(
        self,
        operation_name: str,
        message_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Context manager for logging operation start/success/failure"""
        start_time = datetime.now()
        
        # Log operation start
        self.logger.info(f"Starting {operation_name}", extra={
            "message_id": message_id,
            "correlation_id": correlation_id,
            "trace_id": trace_id
        })
        
        try:
            yield
            
            # Log success
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            self.logger.info(f"Completed {operation_name} in {duration_ms:.2f}ms", extra={
                "message_id": message_id,
                "correlation_id": correlation_id,
                "trace_id": trace_id,
                "duration_ms": duration_ms
            })
            
        except Exception as e:
            # Log failure
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            log_entry = self._create_log_entry(
                MessageEventType.PROCESS_FAILURE,
                LogLevel.ERROR,
                f"Operation {operation_name} failed after {duration_ms:.2f}ms",
                message_id=message_id,
                correlation_id=correlation_id,
                trace_id=trace_id,
                error=e,
                metadata={**(metadata or {}), "duration_ms": duration_ms}
            )
            self._log_entry(log_entry)
            raise
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """Get logging statistics"""
        if not self.log_history:
            return {"total_logs": 0}
        
        # Count by event type
        event_counts = {}
        level_counts = {}
        
        for log_entry in self.log_history:
            event_type = log_entry.event_type.value
            level = log_entry.level.value
            
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
            level_counts[level] = level_counts.get(level, 0) + 1
        
        return {
            "total_logs": len(self.log_history),
            "component": self.component,
            "events_by_type": event_counts,
            "logs_by_level": level_counts,
            "error_summary": self.error_tracker.get_error_summary(24),
            "timestamp": datetime.now().isoformat()
        }


# Global logger instances
_rabbitmq_logger: Optional[MessagingLogger] = None
_kafka_logger: Optional[MessagingLogger] = None


def get_rabbitmq_logger() -> MessagingLogger:
    """Get global RabbitMQ logger"""
    global _rabbitmq_logger
    if _rabbitmq_logger is None:
        _rabbitmq_logger = MessagingLogger("rabbitmq")
    return _rabbitmq_logger


def get_kafka_logger() -> MessagingLogger:
    """Get global Kafka logger"""
    global _kafka_logger
    if _kafka_logger is None:
        _kafka_logger = MessagingLogger("kafka")
    return _kafka_logger


def get_messaging_logger(component: str) -> MessagingLogger:
    """Get messaging logger for specified component"""
    if component == "rabbitmq":
        return get_rabbitmq_logger()
    elif component == "kafka":
        return get_kafka_logger()
    else:
        return MessagingLogger(component)