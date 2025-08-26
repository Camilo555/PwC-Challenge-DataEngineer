"""
Advanced Kafka Streaming Processor with Complex Event Processing
Provides enterprise-grade stream processing with fault tolerance, windowing, and real-time analytics.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError

from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingMessage, StreamingTopic


class EventPattern(Enum):
    """Complex event patterns for CEP"""
    SEQUENCE = "sequence"
    CONJUNCTION = "conjunction" 
    DISJUNCTION = "disjunction"
    NEGATION = "negation"
    AGGREGATION = "aggregation"
    TEMPORAL = "temporal"


class WindowType(Enum):
    """Stream processing window types"""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT = "count"


@dataclass
class EventRule:
    """Complex event processing rule definition"""
    rule_id: str
    name: str
    pattern: EventPattern
    conditions: List[Dict[str, Any]]
    window_config: Dict[str, Any]
    action: str
    priority: int = 1
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class StreamWindow:
    """Stream processing window"""
    window_id: str
    window_type: WindowType
    size_ms: int
    slide_ms: Optional[int] = None
    session_timeout_ms: Optional[int] = None
    events: deque = field(default_factory=deque)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class ProcessedEvent:
    """Processed stream event with metadata"""
    event_id: str
    source_topic: str
    event_type: str
    payload: Dict[str, Any]
    timestamp: datetime
    processing_time: datetime
    window_id: Optional[str] = None
    rule_matches: List[str] = field(default_factory=list)
    aggregations: Dict[str, Any] = field(default_factory=dict)


class StreamProcessor(ABC):
    """Abstract base class for stream processors"""
    
    @abstractmethod
    def process_event(self, event: Dict[str, Any]) -> Optional[ProcessedEvent]:
        """Process a single event"""
        pass
    
    @abstractmethod
    def initialize(self):
        """Initialize the processor"""
        pass
    
    @abstractmethod
    def cleanup(self):
        """Cleanup resources"""
        pass


class ComplexEventProcessor(StreamProcessor):
    """
    Complex Event Processing (CEP) engine for real-time pattern matching
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.rules: Dict[str, EventRule] = {}
        self.event_store: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.pattern_state: Dict[str, Any] = {}
        self.metrics_collector = get_metrics_collector()
        
    def initialize(self):
        """Initialize CEP engine"""
        self.logger.info("Initializing Complex Event Processor")
        
    def cleanup(self):
        """Cleanup CEP resources"""
        self.event_store.clear()
        self.pattern_state.clear()
        
    def add_rule(self, rule: EventRule):
        """Add complex event rule"""
        self.rules[rule.rule_id] = rule
        self.logger.info(f"Added CEP rule: {rule.name}")
        
    def process_event(self, event: Dict[str, Any]) -> Optional[ProcessedEvent]:
        """Process event through CEP engine"""
        try:
            processed_event = ProcessedEvent(
                event_id=event.get('message_id', f"evt_{int(time.time() * 1000)}"),
                source_topic=event.get('topic', 'unknown'),
                event_type=event.get('event_type', 'unknown'),
                payload=event.get('value', {}),
                timestamp=datetime.fromtimestamp(event.get('timestamp', time.time()) / 1000),
                processing_time=datetime.now()
            )
            
            # Store event for pattern matching
            event_type = processed_event.event_type
            self.event_store[event_type].append(processed_event)
            
            # Check patterns
            matching_rules = self._check_patterns(processed_event)
            processed_event.rule_matches = [rule.rule_id for rule in matching_rules]
            
            # Execute actions for matched rules
            for rule in matching_rules:
                self._execute_rule_action(rule, processed_event)
                
            return processed_event
            
        except Exception as e:
            self.logger.error(f"CEP processing failed: {e}")
            return None
            
    def _check_patterns(self, event: ProcessedEvent) -> List[EventRule]:
        """Check event against all patterns"""
        matching_rules = []
        
        for rule in self.rules.values():
            if not rule.enabled:
                continue
                
            if self._matches_pattern(rule, event):
                matching_rules.append(rule)
                
        return matching_rules
        
    def _matches_pattern(self, rule: EventRule, event: ProcessedEvent) -> bool:
        """Check if event matches rule pattern"""
        try:
            if rule.pattern == EventPattern.SEQUENCE:
                return self._check_sequence_pattern(rule, event)
            elif rule.pattern == EventPattern.CONJUNCTION:
                return self._check_conjunction_pattern(rule, event)
            elif rule.pattern == EventPattern.AGGREGATION:
                return self._check_aggregation_pattern(rule, event)
            elif rule.pattern == EventPattern.TEMPORAL:
                return self._check_temporal_pattern(rule, event)
            else:
                return self._check_simple_condition(rule.conditions[0], event)
                
        except Exception as e:
            self.logger.error(f"Pattern matching failed for rule {rule.rule_id}: {e}")
            return False
            
    def _check_sequence_pattern(self, rule: EventRule, event: ProcessedEvent) -> bool:
        """Check sequence pattern (A followed by B within time window)"""
        conditions = rule.conditions
        window_ms = rule.window_config.get('window_ms', 30000)
        
        if len(conditions) < 2:
            return False
            
        # Check if current event matches last condition
        if not self._check_simple_condition(conditions[-1], event):
            return False
            
        # Look for preceding events in sequence
        cutoff_time = event.timestamp - timedelta(milliseconds=window_ms)
        
        for event_type, events in self.event_store.items():
            sequence_matched = 0
            
            for stored_event in reversed(events):
                if stored_event.timestamp < cutoff_time:
                    break
                    
                if sequence_matched < len(conditions) - 1:
                    if self._check_simple_condition(conditions[sequence_matched], stored_event):
                        sequence_matched += 1
                        
            if sequence_matched == len(conditions) - 1:
                return True
                
        return False
        
    def _check_conjunction_pattern(self, rule: EventRule, event: ProcessedEvent) -> bool:
        """Check conjunction pattern (A AND B within time window)"""
        conditions = rule.conditions
        window_ms = rule.window_config.get('window_ms', 30000)
        cutoff_time = event.timestamp - timedelta(milliseconds=window_ms)
        
        matched_conditions = set()
        
        # Check current event
        for i, condition in enumerate(conditions):
            if self._check_simple_condition(condition, event):
                matched_conditions.add(i)
                
        # Check stored events
        for events in self.event_store.values():
            for stored_event in reversed(events):
                if stored_event.timestamp < cutoff_time:
                    break
                    
                for i, condition in enumerate(conditions):
                    if i not in matched_conditions:
                        if self._check_simple_condition(condition, stored_event):
                            matched_conditions.add(i)
                            
        return len(matched_conditions) == len(conditions)
        
    def _check_aggregation_pattern(self, rule: EventRule, event: ProcessedEvent) -> bool:
        """Check aggregation pattern (COUNT, SUM, AVG, etc.)"""
        condition = rule.conditions[0]
        window_ms = rule.window_config.get('window_ms', 60000)
        cutoff_time = event.timestamp - timedelta(milliseconds=window_ms)
        
        # Collect matching events
        matching_events = []
        for events in self.event_store.values():
            for stored_event in reversed(events):
                if stored_event.timestamp < cutoff_time:
                    break
                if self._check_simple_condition(condition, stored_event):
                    matching_events.append(stored_event)
                    
        # Check aggregation condition
        agg_type = condition.get('aggregation', 'count')
        threshold = condition.get('threshold', 0)
        
        if agg_type == 'count':
            return len(matching_events) >= threshold
        elif agg_type == 'sum':
            field = condition.get('field', 'value')
            total = sum(self._get_field_value(e.payload, field) for e in matching_events)
            return total >= threshold
        elif agg_type == 'avg':
            if not matching_events:
                return False
            field = condition.get('field', 'value')
            avg = sum(self._get_field_value(e.payload, field) for e in matching_events) / len(matching_events)
            return avg >= threshold
            
        return False
        
    def _check_temporal_pattern(self, rule: EventRule, event: ProcessedEvent) -> bool:
        """Check temporal pattern (time-based conditions)"""
        condition = rule.conditions[0]
        temporal_type = condition.get('temporal_type', 'window')
        
        if temporal_type == 'window':
            start_time = condition.get('start_time')
            end_time = condition.get('end_time')
            if start_time and end_time:
                event_time = event.timestamp.time()
                return start_time <= event_time <= end_time
                
        elif temporal_type == 'frequency':
            # Check event frequency
            window_ms = condition.get('window_ms', 60000)
            min_frequency = condition.get('min_frequency', 1)
            cutoff_time = event.timestamp - timedelta(milliseconds=window_ms)
            
            count = 0
            event_type = event.event_type
            for stored_event in reversed(self.event_store[event_type]):
                if stored_event.timestamp < cutoff_time:
                    break
                count += 1
                
            return count >= min_frequency
            
        return False
        
    def _check_simple_condition(self, condition: Dict[str, Any], event: ProcessedEvent) -> bool:
        """Check simple field conditions"""
        field_path = condition.get('field', '')
        operator = condition.get('operator', 'eq')
        value = condition.get('value')
        
        event_value = self._get_field_value(event.payload, field_path)
        
        if operator == 'eq':
            return event_value == value
        elif operator == 'ne':
            return event_value != value
        elif operator == 'gt':
            return event_value > value
        elif operator == 'gte':
            return event_value >= value
        elif operator == 'lt':
            return event_value < value
        elif operator == 'lte':
            return event_value <= value
        elif operator == 'in':
            return event_value in value
        elif operator == 'contains':
            return value in str(event_value)
        elif operator == 'regex':
            import re
            return bool(re.match(value, str(event_value)))
            
        return False
        
    def _get_field_value(self, payload: Dict[str, Any], field_path: str) -> Any:
        """Get field value from payload using dot notation"""
        if not field_path:
            return payload
            
        keys = field_path.split('.')
        value = payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value
        
    def _execute_rule_action(self, rule: EventRule, event: ProcessedEvent):
        """Execute action for matched rule"""
        try:
            action = rule.action
            
            if action == 'alert':
                self._send_alert(rule, event)
            elif action == 'log':
                self._log_match(rule, event)
            elif action == 'forward':
                self._forward_event(rule, event)
            elif action == 'store':
                self._store_event(rule, event)
                
        except Exception as e:
            self.logger.error(f"Rule action execution failed: {e}")
            
    def _send_alert(self, rule: EventRule, event: ProcessedEvent):
        """Send alert for matched rule"""
        self.logger.warning(f"CEP Alert - Rule: {rule.name}, Event: {event.event_id}")
        
    def _log_match(self, rule: EventRule, event: ProcessedEvent):
        """Log pattern match"""
        self.logger.info(f"Pattern matched - Rule: {rule.name}, Event: {event.event_id}")
        
    def _forward_event(self, rule: EventRule, event: ProcessedEvent):
        """Forward event to another topic"""
        # Implementation would forward to specified topic
        pass
        
    def _store_event(self, rule: EventRule, event: ProcessedEvent):
        """Store matched event"""
        # Implementation would store to database/file
        pass


class WindowManager:
    """
    Manages streaming windows for time-based aggregations
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.windows: Dict[str, StreamWindow] = {}
        self.cleanup_thread = None
        self.running = False
        
    def start(self):
        """Start window manager"""
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_expired_windows)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()
        self.logger.info("Window manager started")
        
    def stop(self):
        """Stop window manager"""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join()
        self.logger.info("Window manager stopped")
        
    def create_window(self, window_id: str, window_type: WindowType, 
                     size_ms: int, slide_ms: Optional[int] = None) -> StreamWindow:
        """Create new stream window"""
        window = StreamWindow(
            window_id=window_id,
            window_type=window_type,
            size_ms=size_ms,
            slide_ms=slide_ms
        )
        
        self.windows[window_id] = window
        self.logger.debug(f"Created window: {window_id}")
        return window
        
    def add_event_to_windows(self, event: ProcessedEvent) -> List[str]:
        """Add event to relevant windows"""
        updated_windows = []
        
        for window_id, window in self.windows.items():
            if self._event_belongs_to_window(event, window):
                window.events.append(event)
                window.updated_at = datetime.now()
                updated_windows.append(window_id)
                
        return updated_windows
        
    def _event_belongs_to_window(self, event: ProcessedEvent, window: StreamWindow) -> bool:
        """Check if event belongs to window"""
        now = datetime.now()
        window_start = now - timedelta(milliseconds=window.size_ms)
        
        if window.window_type == WindowType.TUMBLING:
            return event.timestamp >= window_start
        elif window.window_type == WindowType.SLIDING:
            return event.timestamp >= window_start
        elif window.window_type == WindowType.SESSION:
            # Session window logic - events belong if within session timeout
            if not window.events:
                return True
            last_event_time = max(e.timestamp for e in window.events)
            session_timeout = timedelta(milliseconds=window.session_timeout_ms or 30000)
            return event.timestamp - last_event_time <= session_timeout
            
        return False
        
    def get_window_aggregations(self, window_id: str) -> Dict[str, Any]:
        """Get aggregations for window"""
        if window_id not in self.windows:
            return {}
            
        window = self.windows[window_id]
        events = list(window.events)
        
        if not events:
            return {"count": 0}
            
        aggregations = {
            "count": len(events),
            "first_event_time": min(e.timestamp for e in events),
            "last_event_time": max(e.timestamp for e in events),
            "unique_event_types": len(set(e.event_type for e in events))
        }
        
        # Calculate numeric aggregations if possible
        try:
            values = [e.payload.get('value', 0) for e in events if 'value' in e.payload]
            if values:
                aggregations.update({
                    "sum": sum(values),
                    "avg": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values)
                })
        except (TypeError, KeyError):
            pass
            
        return aggregations
        
    def _cleanup_expired_windows(self):
        """Cleanup expired windows"""
        while self.running:
            try:
                now = datetime.now()
                expired_windows = []
                
                for window_id, window in self.windows.items():
                    # Remove old events from window
                    window_age = now - timedelta(milliseconds=window.size_ms)
                    window.events = deque(
                        e for e in window.events if e.timestamp >= window_age
                    )
                    
                    # Mark empty windows for removal if old enough
                    if not window.events and (now - window.updated_at).total_seconds() > 300:
                        expired_windows.append(window_id)
                        
                # Remove expired windows
                for window_id in expired_windows:
                    del self.windows[window_id]
                    self.logger.debug(f"Cleaned up expired window: {window_id}")
                    
                time.sleep(60)  # Cleanup every minute
                
            except Exception as e:
                self.logger.error(f"Window cleanup failed: {e}")
                time.sleep(60)


class AdvancedStreamProcessor:
    """
    Advanced stream processor with CEP, windowing, and fault tolerance
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.kafka_manager = KafkaManager()
        self.cep_engine = ComplexEventProcessor()
        self.window_manager = WindowManager()
        self.metrics_collector = get_metrics_collector()
        
        # Processing state
        self.running = False
        self.consumer_threads: List[threading.Thread] = []
        self.processed_events = 0
        self.failed_events = 0
        self.start_time = None
        
        # Configure processors
        self.processors: Dict[str, StreamProcessor] = {
            'cep': self.cep_engine
        }
        
    def initialize(self):
        """Initialize stream processor"""
        self.logger.info("Initializing Advanced Stream Processor")
        
        # Initialize components
        self.cep_engine.initialize()
        self.window_manager.start()
        
        # Load default CEP rules
        self._load_default_rules()
        
        self.logger.info("Advanced Stream Processor initialized")
        
    def _load_default_rules(self):
        """Load default CEP rules"""
        
        # High transaction value alert
        high_value_rule = EventRule(
            rule_id="high_value_transaction",
            name="High Value Transaction Alert",
            pattern=EventPattern.AGGREGATION,
            conditions=[{
                "field": "amount",
                "operator": "gt",
                "value": 10000,
                "aggregation": "count",
                "threshold": 1
            }],
            window_config={"window_ms": 1000},
            action="alert",
            priority=1
        )
        self.cep_engine.add_rule(high_value_rule)
        
        # Suspicious activity pattern (multiple failed attempts)
        suspicious_pattern = EventRule(
            rule_id="suspicious_activity",
            name="Suspicious Activity Pattern",
            pattern=EventPattern.AGGREGATION,
            conditions=[{
                "field": "status",
                "operator": "eq",
                "value": "failed",
                "aggregation": "count",
                "threshold": 5
            }],
            window_config={"window_ms": 300000},  # 5 minutes
            action="alert",
            priority=2
        )
        self.cep_engine.add_rule(suspicious_pattern)
        
        # Fraud sequence detection
        fraud_sequence = EventRule(
            rule_id="fraud_sequence",
            name="Fraud Sequence Detection",
            pattern=EventPattern.SEQUENCE,
            conditions=[
                {"field": "event_type", "operator": "eq", "value": "login"},
                {"field": "event_type", "operator": "eq", "value": "high_value_transaction"}
            ],
            window_config={"window_ms": 60000},  # 1 minute
            action="alert",
            priority=3
        )
        self.cep_engine.add_rule(fraud_sequence)
        
    def start_processing(self, topics: List[str], consumer_group: str = "advanced_stream_processor"):
        """Start stream processing"""
        if self.running:
            self.logger.warning("Stream processor already running")
            return
            
        self.running = True
        self.start_time = datetime.now()
        self.logger.info(f"Starting stream processing for topics: {topics}")
        
        # Start consumer threads for each topic
        for topic in topics:
            thread = threading.Thread(
                target=self._process_topic_stream,
                args=(topic, consumer_group),
                name=f"StreamProcessor-{topic}"
            )
            thread.daemon = True
            thread.start()
            self.consumer_threads.append(thread)
            
        self.logger.info(f"Started {len(self.consumer_threads)} consumer threads")
        
    def stop_processing(self):
        """Stop stream processing"""
        if not self.running:
            return
            
        self.logger.info("Stopping stream processing")
        self.running = False
        
        # Wait for threads to complete
        for thread in self.consumer_threads:
            thread.join(timeout=30)
            
        self.consumer_threads.clear()
        self.window_manager.stop()
        
        self.logger.info("Stream processing stopped")
        
    def _process_topic_stream(self, topic: str, consumer_group: str):
        """Process stream from specific topic"""
        try:
            consumer = self.kafka_manager.create_consumer([topic], f"{consumer_group}_{topic}")
            
            self.logger.info(f"Started processing stream from topic: {topic}")
            
            while self.running:
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000, max_records=100)
                    
                    if not message_batch:
                        continue
                        
                    # Process messages
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                self._process_single_message(message)
                                self.processed_events += 1
                                
                            except Exception as e:
                                self.logger.error(f"Message processing failed: {e}")
                                self.failed_events += 1
                                
                    # Commit offsets
                    consumer.commit()
                    
                except Exception as e:
                    self.logger.error(f"Stream processing error for topic {topic}: {e}")
                    time.sleep(5)  # Brief pause before retry
                    
        except Exception as e:
            self.logger.error(f"Fatal error in topic stream {topic}: {e}")
        finally:
            try:
                consumer.close()
            except:
                pass
                
    def _process_single_message(self, message: ConsumerRecord):
        """Process single Kafka message"""
        try:
            # Convert Kafka message to event format
            event_data = {
                'message_id': f"{message.topic}_{message.partition}_{message.offset}",
                'topic': message.topic,
                'partition': message.partition,
                'offset': message.offset,
                'key': message.key,
                'value': message.value,
                'timestamp': message.timestamp,
                'headers': dict(message.headers or {})
            }
            
            # Process through CEP engine
            processed_event = self.cep_engine.process_event(event_data)
            
            if processed_event:
                # Add to windows
                updated_windows = self.window_manager.add_event_to_windows(processed_event)
                
                # Calculate window aggregations for updated windows
                for window_id in updated_windows:
                    aggregations = self.window_manager.get_window_aggregations(window_id)
                    processed_event.aggregations.update({f"window_{window_id}": aggregations})
                    
                # Send processed event downstream if needed
                self._send_processed_event(processed_event)
                
                # Record metrics
                if self.metrics_collector:
                    self.metrics_collector.increment_counter("stream_events_processed", {
                        "topic": message.topic,
                        "event_type": processed_event.event_type
                    })
                    
        except Exception as e:
            self.logger.error(f"Single message processing failed: {e}")
            raise
            
    def _send_processed_event(self, event: ProcessedEvent):
        """Send processed event to output topics"""
        try:
            # Send to processed events topic
            processed_topic = StreamingTopic.SYSTEM_EVENTS
            
            message_data = {
                "event_id": event.event_id,
                "source_topic": event.source_topic,
                "event_type": event.event_type,
                "payload": event.payload,
                "timestamp": event.timestamp.isoformat(),
                "processing_time": event.processing_time.isoformat(),
                "rule_matches": event.rule_matches,
                "aggregations": event.aggregations
            }
            
            self.kafka_manager.produce_message(
                topic=processed_topic,
                message=message_data,
                key=event.event_id
            )
            
            # Send alerts if rules matched
            if event.rule_matches:
                alert_data = {
                    "alert_type": "cep_rule_match",
                    "event_id": event.event_id,
                    "matched_rules": event.rule_matches,
                    "payload": event.payload,
                    "severity": "medium"
                }
                
                self.kafka_manager.produce_data_quality_alert(alert_data)
                
        except Exception as e:
            self.logger.error(f"Failed to send processed event: {e}")
            
    def add_custom_rule(self, rule: EventRule):
        """Add custom CEP rule"""
        self.cep_engine.add_rule(rule)
        
    def create_window(self, window_id: str, window_type: WindowType, 
                     size_ms: int, slide_ms: Optional[int] = None) -> StreamWindow:
        """Create streaming window"""
        return self.window_manager.create_window(window_id, window_type, size_ms, slide_ms)
        
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        uptime = 0
        if self.start_time:
            uptime = (datetime.now() - self.start_time).total_seconds()
            
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "processed_events": self.processed_events,
            "failed_events": self.failed_events,
            "events_per_second": self.processed_events / max(uptime, 1),
            "active_threads": len(self.consumer_threads),
            "active_windows": len(self.window_manager.windows),
            "cep_rules": len(self.cep_engine.rules),
            "timestamp": datetime.now().isoformat()
        }


# Factory function
def create_advanced_stream_processor(config: Optional[Dict[str, Any]] = None) -> AdvancedStreamProcessor:
    """Create advanced stream processor instance"""
    return AdvancedStreamProcessor(config)


# Example usage
if __name__ == "__main__":
    import signal
    import sys
    
    def signal_handler(sig, frame):
        print("\nShutting down stream processor...")
        processor.stop_processing()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create and configure processor
    processor = create_advanced_stream_processor({
        "enable_monitoring": True,
        "batch_size": 100
    })
    
    # Initialize
    processor.initialize()
    
    # Create custom windows
    processor.create_window("transaction_window", WindowType.SLIDING, 60000, 10000)
    processor.create_window("session_window", WindowType.SESSION, 300000)
    
    # Start processing
    topics = [
        StreamingTopic.RETAIL_TRANSACTIONS.value,
        StreamingTopic.CUSTOMER_EVENTS.value,
        StreamingTopic.SYSTEM_EVENTS.value
    ]
    
    processor.start_processing(topics)
    
    print("Advanced Stream Processor started. Press Ctrl+C to stop.")
    
    # Keep running and print stats
    try:
        while processor.running:
            time.sleep(30)
            stats = processor.get_processing_stats()
            print(f"Stats: {stats['processed_events']} processed, "
                  f"{stats['events_per_second']:.2f} EPS, "
                  f"{stats['active_windows']} windows")
    except KeyboardInterrupt:
        pass
    finally:
        processor.stop_processing()