"""
Stream Processing with Windowing and Aggregations
Provides real-time stream analytics with time-based windows and complex aggregations.
"""
from __future__ import annotations

import asyncio
import json
import math
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

import numpy as np
from kafka import KafkaConsumer

from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingMessage, StreamingTopic


class WindowType(Enum):
    """Stream window types"""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT = "count"
    GLOBAL = "global"


class AggregationType(Enum):
    """Aggregation function types"""
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    STDDEV = "stddev"
    VARIANCE = "variance"
    PERCENTILE = "percentile"
    DISTINCT_COUNT = "distinct_count"
    FIRST = "first"
    LAST = "last"
    RATE = "rate"
    TOP_K = "top_k"
    HISTOGRAM = "histogram"


@dataclass
class WindowDefinition:
    """Window configuration"""
    window_id: str
    window_type: WindowType
    size_ms: int
    slide_ms: Optional[int] = None
    session_timeout_ms: Optional[int] = None
    count_size: Optional[int] = None
    grace_period_ms: int = 5000
    allowed_lateness_ms: int = 30000
    trigger_conditions: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class AggregationDefinition:
    """Aggregation configuration"""
    aggregation_id: str
    name: str
    aggregation_type: AggregationType
    field_path: str
    group_by_fields: List[str] = field(default_factory=list)
    filter_conditions: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    output_field: Optional[str] = None
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class StreamEvent:
    """Stream event for processing"""
    event_id: str
    event_type: str
    timestamp: datetime
    payload: Dict[str, Any]
    partition_key: Optional[str] = None
    source_topic: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class WindowInstance:
    """Instance of a stream window"""
    window_id: str
    start_time: datetime
    end_time: datetime
    events: List[StreamEvent] = field(default_factory=list)
    aggregations: Dict[str, Any] = field(default_factory=dict)
    is_closed: bool = False
    last_updated: datetime = field(default_factory=datetime.now)
    event_count: int = 0


@dataclass
class AggregationResult:
    """Result of stream aggregation"""
    window_id: str
    aggregation_id: str
    window_start: datetime
    window_end: datetime
    group_keys: Dict[str, Any]
    aggregation_type: AggregationType
    field_path: str
    result_value: Any
    event_count: int
    calculation_time: datetime = field(default_factory=datetime.now)


class StreamAggregator(ABC):
    """Abstract base class for stream aggregators"""
    
    @abstractmethod
    def add_event(self, event: StreamEvent):
        """Add event to aggregator"""
        pass
        
    @abstractmethod
    def get_result(self) -> Any:
        """Get aggregation result"""
        pass
        
    @abstractmethod
    def reset(self):
        """Reset aggregator state"""
        pass


class CountAggregator(StreamAggregator):
    """Count aggregator"""
    
    def __init__(self):
        self.count = 0
        
    def add_event(self, event: StreamEvent):
        self.count += 1
        
    def get_result(self) -> int:
        return self.count
        
    def reset(self):
        self.count = 0


class SumAggregator(StreamAggregator):
    """Sum aggregator"""
    
    def __init__(self, field_path: str):
        self.field_path = field_path
        self.sum_value = 0.0
        
    def add_event(self, event: StreamEvent):
        value = self._extract_value(event, self.field_path)
        if isinstance(value, (int, float)):
            self.sum_value += value
            
    def get_result(self) -> float:
        return self.sum_value
        
    def reset(self):
        self.sum_value = 0.0
        
    def _extract_value(self, event: StreamEvent, field_path: str) -> Any:
        """Extract field value from event"""
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class AvgAggregator(StreamAggregator):
    """Average aggregator"""
    
    def __init__(self, field_path: str):
        self.field_path = field_path
        self.sum_value = 0.0
        self.count = 0
        
    def add_event(self, event: StreamEvent):
        value = self._extract_value(event, self.field_path)
        if isinstance(value, (int, float)):
            self.sum_value += value
            self.count += 1
            
    def get_result(self) -> Optional[float]:
        return self.sum_value / self.count if self.count > 0 else None
        
    def reset(self):
        self.sum_value = 0.0
        self.count = 0
        
    def _extract_value(self, event: StreamEvent, field_path: str) -> Any:
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class MinMaxAggregator(StreamAggregator):
    """Min/Max aggregator"""
    
    def __init__(self, field_path: str, aggregation_type: AggregationType):
        self.field_path = field_path
        self.aggregation_type = aggregation_type
        self.values: List[float] = []
        
    def add_event(self, event: StreamEvent):
        value = self._extract_value(event, self.field_path)
        if isinstance(value, (int, float)):
            self.values.append(float(value))
            
    def get_result(self) -> Optional[float]:
        if not self.values:
            return None
        return min(self.values) if self.aggregation_type == AggregationType.MIN else max(self.values)
        
    def reset(self):
        self.values.clear()
        
    def _extract_value(self, event: StreamEvent, field_path: str) -> Any:
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class StatisticalAggregator(StreamAggregator):
    """Statistical aggregator (median, stddev, percentiles)"""
    
    def __init__(self, field_path: str, aggregation_type: AggregationType, parameters: Dict[str, Any] = None):
        self.field_path = field_path
        self.aggregation_type = aggregation_type
        self.parameters = parameters or {}
        self.values: List[float] = []
        
    def add_event(self, event: StreamEvent):
        value = self._extract_value(event, self.field_path)
        if isinstance(value, (int, float)):
            self.values.append(float(value))
            
    def get_result(self) -> Optional[float]:
        if not self.values:
            return None
            
        sorted_values = sorted(self.values)
        
        if self.aggregation_type == AggregationType.MEDIAN:
            return self._calculate_median(sorted_values)
        elif self.aggregation_type == AggregationType.PERCENTILE:
            percentile = self.parameters.get('percentile', 50)
            return self._calculate_percentile(sorted_values, percentile)
        elif self.aggregation_type == AggregationType.STDDEV:
            return self._calculate_stddev(self.values)
        elif self.aggregation_type == AggregationType.VARIANCE:
            return self._calculate_variance(self.values)
            
        return None
        
    def reset(self):
        self.values.clear()
        
    def _calculate_median(self, sorted_values: List[float]) -> float:
        n = len(sorted_values)
        if n % 2 == 0:
            return (sorted_values[n//2 - 1] + sorted_values[n//2]) / 2
        else:
            return sorted_values[n//2]
            
    def _calculate_percentile(self, sorted_values: List[float], percentile: float) -> float:
        if not sorted_values:
            return 0.0
        k = (len(sorted_values) - 1) * percentile / 100
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_values[int(k)]
        d0 = sorted_values[int(f)] * (c - k)
        d1 = sorted_values[int(c)] * (k - f)
        return d0 + d1
        
    def _calculate_stddev(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return math.sqrt(variance)
        
    def _calculate_variance(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        return sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        
    def _extract_value(self, event: StreamEvent, field_path: str) -> Any:
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class DistinctCountAggregator(StreamAggregator):
    """Distinct count aggregator"""
    
    def __init__(self, field_path: str):
        self.field_path = field_path
        self.distinct_values: Set[Any] = set()
        
    def add_event(self, event: StreamEvent):
        value = self._extract_value(event, self.field_path)
        if value is not None:
            # Convert to string for hashability
            self.distinct_values.add(str(value))
            
    def get_result(self) -> int:
        return len(self.distinct_values)
        
    def reset(self):
        self.distinct_values.clear()
        
    def _extract_value(self, event: StreamEvent, field_path: str) -> Any:
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class TopKAggregator(StreamAggregator):
    """Top-K most frequent values aggregator"""
    
    def __init__(self, field_path: str, k: int = 10):
        self.field_path = field_path
        self.k = k
        self.value_counts: Dict[str, int] = defaultdict(int)
        
    def add_event(self, event: StreamEvent):
        value = self._extract_value(event, self.field_path)
        if value is not None:
            self.value_counts[str(value)] += 1
            
    def get_result(self) -> List[Dict[str, Any]]:
        # Return top K most frequent values
        sorted_items = sorted(
            self.value_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:self.k]
        
        return [{"value": value, "count": count} for value, count in sorted_items]
        
    def reset(self):
        self.value_counts.clear()
        
    def _extract_value(self, event: StreamEvent, field_path: str) -> Any:
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class HistogramAggregator(StreamAggregator):
    """Histogram aggregator"""
    
    def __init__(self, field_path: str, bins: int = 10, min_value: float = None, max_value: float = None):
        self.field_path = field_path
        self.bins = bins
        self.min_value = min_value
        self.max_value = max_value
        self.values: List[float] = []
        
    def add_event(self, event: StreamEvent):
        value = self._extract_value(event, self.field_path)
        if isinstance(value, (int, float)):
            self.values.append(float(value))
            
    def get_result(self) -> Dict[str, Any]:
        if not self.values:
            return {"bins": [], "counts": [], "edges": []}
            
        # Calculate histogram
        min_val = self.min_value if self.min_value is not None else min(self.values)
        max_val = self.max_value if self.max_value is not None else max(self.values)
        
        bin_width = (max_val - min_val) / self.bins
        edges = [min_val + i * bin_width for i in range(self.bins + 1)]
        counts = [0] * self.bins
        
        for value in self.values:
            if min_val <= value <= max_val:
                bin_index = min(int((value - min_val) / bin_width), self.bins - 1)
                counts[bin_index] += 1
                
        return {
            "bins": list(range(self.bins)),
            "counts": counts,
            "edges": edges,
            "total_count": len(self.values)
        }
        
    def reset(self):
        self.values.clear()
        
    def _extract_value(self, event: StreamEvent, field_path: str) -> Any:
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class WindowManager:
    """
    Manages stream windows and their lifecycle
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.windows: Dict[str, WindowDefinition] = {}
        self.window_instances: Dict[str, List[WindowInstance]] = defaultdict(list)
        self.aggregators: Dict[str, Dict[str, StreamAggregator]] = defaultdict(dict)
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
        
    def create_window(self, window_def: WindowDefinition):
        """Create new window definition"""
        self.windows[window_def.window_id] = window_def
        self.logger.info(f"Created window: {window_def.window_id}")
        
    def add_event(self, event: StreamEvent) -> List[str]:
        """Add event to relevant windows and return updated window IDs"""
        updated_windows = []
        
        for window_id, window_def in self.windows.items():
            if self._event_belongs_to_window(event, window_def):
                window_instances = self._get_or_create_window_instances(event, window_def)
                
                for window_instance in window_instances:
                    if not window_instance.is_closed:
                        window_instance.events.append(event)
                        window_instance.event_count += 1
                        window_instance.last_updated = datetime.now()
                        updated_windows.append(f"{window_id}_{id(window_instance)}")
                        
        return updated_windows
        
    def _event_belongs_to_window(self, event: StreamEvent, window_def: WindowDefinition) -> bool:
        """Check if event belongs to window based on time"""
        now = datetime.now()
        
        if window_def.window_type == WindowType.GLOBAL:
            return True
        elif window_def.window_type == WindowType.COUNT:
            return True  # Count windows accept all events
        else:
            # Time-based windows
            grace_period = timedelta(milliseconds=window_def.grace_period_ms)
            return event.timestamp >= (now - grace_period)
            
    def _get_or_create_window_instances(self, event: StreamEvent, window_def: WindowDefinition) -> List[WindowInstance]:
        """Get or create window instances for event"""
        window_instances = []
        
        if window_def.window_type == WindowType.TUMBLING:
            window_instances = self._get_tumbling_windows(event, window_def)
        elif window_def.window_type == WindowType.SLIDING:
            window_instances = self._get_sliding_windows(event, window_def)
        elif window_def.window_type == WindowType.SESSION:
            window_instances = self._get_session_windows(event, window_def)
        elif window_def.window_type == WindowType.COUNT:
            window_instances = self._get_count_windows(event, window_def)
        elif window_def.window_type == WindowType.GLOBAL:
            window_instances = self._get_global_window(event, window_def)
            
        return window_instances
        
    def _get_tumbling_windows(self, event: StreamEvent, window_def: WindowDefinition) -> List[WindowInstance]:
        """Get tumbling window instances"""
        window_size = timedelta(milliseconds=window_def.size_ms)
        
        # Calculate window start time (aligned to window boundaries)
        timestamp_ms = int(event.timestamp.timestamp() * 1000)
        window_start_ms = (timestamp_ms // window_def.size_ms) * window_def.size_ms
        window_start = datetime.fromtimestamp(window_start_ms / 1000)
        window_end = window_start + window_size
        
        # Find or create window instance
        window_instance = self._find_or_create_window_instance(
            window_def.window_id, window_start, window_end
        )
        
        return [window_instance]
        
    def _get_sliding_windows(self, event: StreamEvent, window_def: WindowDefinition) -> List[WindowInstance]:
        """Get sliding window instances"""
        window_size = timedelta(milliseconds=window_def.size_ms)
        slide_size = timedelta(milliseconds=window_def.slide_ms or window_def.size_ms)
        
        window_instances = []
        
        # Find all overlapping windows
        current_time = event.timestamp
        
        # Look back to find overlapping windows
        lookback_time = current_time - window_size
        
        # Create windows at slide intervals
        window_start = lookback_time
        while window_start <= current_time:
            window_end = window_start + window_size
            
            if window_start <= event.timestamp <= window_end:
                window_instance = self._find_or_create_window_instance(
                    window_def.window_id, window_start, window_end
                )
                window_instances.append(window_instance)
                
            window_start += slide_size
            
        return window_instances
        
    def _get_session_windows(self, event: StreamEvent, window_def: WindowDefinition) -> List[WindowInstance]:
        """Get session window instances"""
        session_timeout = timedelta(milliseconds=window_def.session_timeout_ms or 300000)
        
        # Find existing session window or create new one
        existing_windows = self.window_instances[window_def.window_id]
        
        for window in existing_windows:
            if not window.is_closed:
                # Check if event extends existing session
                if event.timestamp - window.end_time <= session_timeout:
                    # Extend window
                    window.end_time = event.timestamp + session_timeout
                    return [window]
                    
        # Create new session window
        window_start = event.timestamp
        window_end = event.timestamp + session_timeout
        
        window_instance = self._find_or_create_window_instance(
            window_def.window_id, window_start, window_end
        )
        
        return [window_instance]
        
    def _get_count_windows(self, event: StreamEvent, window_def: WindowDefinition) -> List[WindowInstance]:
        """Get count-based window instances"""
        count_size = window_def.count_size or 1000
        
        # Find current count window or create new one
        existing_windows = self.window_instances[window_def.window_id]
        
        for window in existing_windows:
            if not window.is_closed and window.event_count < count_size:
                return [window]
                
        # Create new count window
        window_start = event.timestamp
        window_end = event.timestamp  # Will be updated as events arrive
        
        window_instance = self._find_or_create_window_instance(
            window_def.window_id, window_start, window_end
        )
        
        return [window_instance]
        
    def _get_global_window(self, event: StreamEvent, window_def: WindowDefinition) -> List[WindowInstance]:
        """Get global window instance"""
        # Global window spans all time
        existing_windows = self.window_instances[window_def.window_id]
        
        if existing_windows:
            return [existing_windows[0]]
            
        # Create global window
        window_start = datetime.min
        window_end = datetime.max
        
        window_instance = self._find_or_create_window_instance(
            window_def.window_id, window_start, window_end
        )
        
        return [window_instance]
        
    def _find_or_create_window_instance(self, window_id: str, start_time: datetime, 
                                      end_time: datetime) -> WindowInstance:
        """Find existing window instance or create new one"""
        existing_windows = self.window_instances[window_id]
        
        for window in existing_windows:
            if window.start_time == start_time and window.end_time == end_time:
                return window
                
        # Create new window instance
        window_instance = WindowInstance(
            window_id=window_id,
            start_time=start_time,
            end_time=end_time
        )
        
        existing_windows.append(window_instance)
        return window_instance
        
    def close_window(self, window_id: str, window_instance_id: str):
        """Close window instance"""
        for window in self.window_instances[window_id]:
            if id(window) == int(window_instance_id.split('_')[-1]):
                window.is_closed = True
                break
                
    def get_window_instances(self, window_id: str) -> List[WindowInstance]:
        """Get all window instances for window ID"""
        return self.window_instances[window_id]
        
    def _cleanup_expired_windows(self):
        """Cleanup expired window instances"""
        while self.running:
            try:
                now = datetime.now()
                
                for window_id, window_def in self.windows.items():
                    expired_instances = []
                    
                    for window in self.window_instances[window_id]:
                        # Check if window is expired
                        allowed_lateness = timedelta(milliseconds=window_def.allowed_lateness_ms)
                        
                        if window.end_time + allowed_lateness < now:
                            expired_instances.append(window)
                            
                    # Remove expired instances
                    for expired in expired_instances:
                        self.window_instances[window_id].remove(expired)
                        
                    self.logger.debug(f"Cleaned up {len(expired_instances)} expired windows for {window_id}")
                    
                time.sleep(60)  # Cleanup every minute
                
            except Exception as e:
                self.logger.error(f"Window cleanup failed: {e}")
                time.sleep(60)


class StreamAggregationProcessor:
    """
    Main stream aggregation processor with windowing support
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.kafka_manager = KafkaManager()
        self.window_manager = WindowManager()
        self.aggregation_definitions: Dict[str, AggregationDefinition] = {}
        self.metrics_collector = get_metrics_collector()
        
        # Processing state
        self.running = False
        self.processing_threads: List[threading.Thread] = []
        self.processed_events = 0
        self.aggregation_calculations = 0
        self.start_time = None
        
    def initialize(self):
        """Initialize stream aggregation processor"""
        self.logger.info("Initializing Stream Aggregation Processor")
        
        self.window_manager.start()
        
        # Load default windows and aggregations
        self._create_default_windows()
        self._create_default_aggregations()
        
        self.logger.info("Stream Aggregation Processor initialized")
        
    def _create_default_windows(self):
        """Create default window definitions"""
        
        # 1-minute tumbling window
        minute_window = WindowDefinition(
            window_id="minute_tumbling",
            window_type=WindowType.TUMBLING,
            size_ms=60000  # 1 minute
        )
        self.window_manager.create_window(minute_window)
        
        # 5-minute sliding window with 1-minute slide
        sliding_window = WindowDefinition(
            window_id="sliding_5min",
            window_type=WindowType.SLIDING,
            size_ms=300000,  # 5 minutes
            slide_ms=60000   # 1 minute slide
        )
        self.window_manager.create_window(sliding_window)
        
        # Session window with 30-second timeout
        session_window = WindowDefinition(
            window_id="user_sessions",
            window_type=WindowType.SESSION,
            session_timeout_ms=30000  # 30 seconds
        )
        self.window_manager.create_window(session_window)
        
        # Count-based window (1000 events)
        count_window = WindowDefinition(
            window_id="count_1000",
            window_type=WindowType.COUNT,
            count_size=1000
        )
        self.window_manager.create_window(count_window)
        
        # Global window
        global_window = WindowDefinition(
            window_id="global",
            window_type=WindowType.GLOBAL,
            size_ms=0
        )
        self.window_manager.create_window(global_window)
        
    def _create_default_aggregations(self):
        """Create default aggregation definitions"""
        
        # Transaction count
        transaction_count = AggregationDefinition(
            aggregation_id="transaction_count",
            name="Transaction Count",
            aggregation_type=AggregationType.COUNT,
            field_path="",
            group_by_fields=["country"],
            output_field="transaction_count"
        )
        self.add_aggregation(transaction_count)
        
        # Revenue sum
        revenue_sum = AggregationDefinition(
            aggregation_id="revenue_sum",
            name="Total Revenue",
            aggregation_type=AggregationType.SUM,
            field_path="unit_price",
            group_by_fields=["country", "stock_code"],
            output_field="total_revenue"
        )
        self.add_aggregation(revenue_sum)
        
        # Average transaction value
        avg_transaction = AggregationDefinition(
            aggregation_id="avg_transaction_value",
            name="Average Transaction Value",
            aggregation_type=AggregationType.AVG,
            field_path="unit_price",
            group_by_fields=["country"],
            output_field="avg_transaction_value"
        )
        self.add_aggregation(avg_transaction)
        
        # Unique customers
        unique_customers = AggregationDefinition(
            aggregation_id="unique_customers",
            name="Unique Customer Count",
            aggregation_type=AggregationType.DISTINCT_COUNT,
            field_path="customer_id",
            group_by_fields=["country"],
            output_field="unique_customers"
        )
        self.add_aggregation(unique_customers)
        
        # Top products
        top_products = AggregationDefinition(
            aggregation_id="top_products",
            name="Top 10 Products",
            aggregation_type=AggregationType.TOP_K,
            field_path="stock_code",
            parameters={"k": 10},
            output_field="top_products"
        )
        self.add_aggregation(top_products)
        
        # Transaction value percentiles
        value_percentiles = AggregationDefinition(
            aggregation_id="transaction_percentiles",
            name="Transaction Value 95th Percentile",
            aggregation_type=AggregationType.PERCENTILE,
            field_path="unit_price",
            parameters={"percentile": 95},
            group_by_fields=["country"],
            output_field="value_p95"
        )
        self.add_aggregation(value_percentiles)
        
        # Transaction histogram
        value_histogram = AggregationDefinition(
            aggregation_id="transaction_histogram",
            name="Transaction Value Histogram",
            aggregation_type=AggregationType.HISTOGRAM,
            field_path="unit_price",
            parameters={"bins": 20, "min_value": 0, "max_value": 1000},
            output_field="value_histogram"
        )
        self.add_aggregation(value_histogram)
        
    def add_aggregation(self, aggregation_def: AggregationDefinition):
        """Add aggregation definition"""
        self.aggregation_definitions[aggregation_def.aggregation_id] = aggregation_def
        self.logger.info(f"Added aggregation: {aggregation_def.name}")
        
    def start_processing(self, topics: List[str], consumer_group: str = "stream_aggregation"):
        """Start stream processing"""
        if self.running:
            self.logger.warning("Stream aggregation processor already running")
            return
            
        self.running = True
        self.start_time = datetime.now()
        self.logger.info(f"Starting stream aggregation for topics: {topics}")
        
        # Start processing threads
        for topic in topics:
            thread = threading.Thread(
                target=self._process_topic_aggregations,
                args=(topic, consumer_group),
                name=f"StreamAggregator-{topic}"
            )
            thread.daemon = True
            thread.start()
            self.processing_threads.append(thread)
            
        # Start aggregation computation thread
        computation_thread = threading.Thread(
            target=self._compute_aggregations_periodically,
            name="AggregationComputer"
        )
        computation_thread.daemon = True
        computation_thread.start()
        self.processing_threads.append(computation_thread)
        
        self.logger.info(f"Started {len(self.processing_threads)} aggregation threads")
        
    def stop_processing(self):
        """Stop stream processing"""
        if not self.running:
            return
            
        self.logger.info("Stopping stream aggregation processor")
        self.running = False
        
        # Wait for threads
        for thread in self.processing_threads:
            thread.join(timeout=30)
            
        self.processing_threads.clear()
        self.window_manager.stop()
        
        self.logger.info("Stream aggregation processor stopped")
        
    def _process_topic_aggregations(self, topic: str, consumer_group: str):
        """Process aggregations for specific topic"""
        try:
            consumer = self.kafka_manager.create_consumer([topic], f"{consumer_group}_{topic}")
            
            self.logger.info(f"Started aggregation processing for topic: {topic}")
            
            while self.running:
                try:
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                        
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                event = self._convert_to_stream_event(message)
                                updated_windows = self.window_manager.add_event(event)
                                self.processed_events += 1
                                
                            except Exception as e:
                                self.logger.error(f"Event processing failed: {e}")
                                
                    consumer.commit()
                    
                except Exception as e:
                    self.logger.error(f"Batch processing error for topic {topic}: {e}")
                    time.sleep(5)
                    
        except Exception as e:
            self.logger.error(f"Fatal error in topic aggregation {topic}: {e}")
        finally:
            try:
                consumer.close()
            except:
                pass
                
    def _convert_to_stream_event(self, message) -> StreamEvent:
        """Convert Kafka message to stream event"""
        return StreamEvent(
            event_id=f"{message.topic}_{message.partition}_{message.offset}",
            event_type=message.value.get('event_type', 'transaction'),
            timestamp=datetime.fromtimestamp(message.timestamp / 1000),
            payload=message.value,
            partition_key=message.key,
            source_topic=message.topic,
            headers=dict(message.headers or {})
        )
        
    def _compute_aggregations_periodically(self):
        """Compute aggregations periodically"""
        while self.running:
            try:
                start_time = time.time()
                
                for window_id, window_def in self.window_manager.windows.items():
                    window_instances = self.window_manager.get_window_instances(window_id)
                    
                    for window_instance in window_instances:
                        if window_instance.events and not window_instance.is_closed:
                            # Check if window should be triggered
                            should_trigger = self._should_trigger_window(window_instance, window_def)
                            
                            if should_trigger:
                                results = self._compute_window_aggregations(window_instance)
                                self._publish_aggregation_results(results)
                                self.aggregation_calculations += len(results)
                                
                                # Close window if appropriate
                                if window_def.window_type in [WindowType.TUMBLING, WindowType.COUNT]:
                                    window_instance.is_closed = True
                                    
                computation_time = time.time() - start_time
                self.logger.debug(f"Aggregation computation took {computation_time:.3f}s")
                
                time.sleep(10)  # Compute every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Aggregation computation failed: {e}")
                time.sleep(10)
                
    def _should_trigger_window(self, window_instance: WindowInstance, window_def: WindowDefinition) -> bool:
        """Check if window should be triggered for computation"""
        now = datetime.now()
        
        if window_def.window_type == WindowType.TUMBLING:
            return now >= window_instance.end_time
        elif window_def.window_type == WindowType.SLIDING:
            # Trigger every slide interval
            return True
        elif window_def.window_type == WindowType.SESSION:
            # Trigger if session timeout exceeded
            return now >= window_instance.end_time
        elif window_def.window_type == WindowType.COUNT:
            # Trigger if count reached
            return window_instance.event_count >= window_def.count_size
        elif window_def.window_type == WindowType.GLOBAL:
            # Trigger periodically for global windows
            return (now - window_instance.last_updated).total_seconds() >= 60
            
        return False
        
    def _compute_window_aggregations(self, window_instance: WindowInstance) -> List[AggregationResult]:
        """Compute aggregations for window instance"""
        results = []
        
        # Group events by aggregation group keys
        grouped_events = self._group_events_by_keys(window_instance.events)
        
        for aggregation_def in self.aggregation_definitions.values():
            if not aggregation_def.enabled:
                continue
                
            try:
                for group_keys, events in grouped_events.items():
                    # Filter events based on aggregation conditions
                    filtered_events = self._filter_events(events, aggregation_def.filter_conditions)
                    
                    if not filtered_events:
                        continue
                        
                    # Create aggregator
                    aggregator = self._create_aggregator(aggregation_def)
                    
                    # Add events to aggregator
                    for event in filtered_events:
                        aggregator.add_event(event)
                        
                    # Get result
                    result_value = aggregator.get_result()
                    
                    if result_value is not None:
                        result = AggregationResult(
                            window_id=window_instance.window_id,
                            aggregation_id=aggregation_def.aggregation_id,
                            window_start=window_instance.start_time,
                            window_end=window_instance.end_time,
                            group_keys=group_keys,
                            aggregation_type=aggregation_def.aggregation_type,
                            field_path=aggregation_def.field_path,
                            result_value=result_value,
                            event_count=len(filtered_events)
                        )
                        results.append(result)
                        
            except Exception as e:
                self.logger.error(f"Aggregation computation failed for {aggregation_def.aggregation_id}: {e}")
                
        return results
        
    def _group_events_by_keys(self, events: List[StreamEvent]) -> Dict[Dict[str, Any], List[StreamEvent]]:
        """Group events by aggregation group keys"""
        grouped = defaultdict(list)
        
        for event in events:
            # Create group key from all possible group by fields
            all_group_keys = set()
            for agg_def in self.aggregation_definitions.values():
                all_group_keys.update(agg_def.group_by_fields)
                
            group_key = {}
            for field in all_group_keys:
                value = self._extract_field_value(event, field)
                group_key[field] = value
                
            # Convert to hashable key
            hashable_key = tuple(sorted(group_key.items()))
            grouped[group_key].append(event)
            
        return dict(grouped)
        
    def _filter_events(self, events: List[StreamEvent], filter_conditions: Dict[str, Any]) -> List[StreamEvent]:
        """Filter events based on conditions"""
        if not filter_conditions:
            return events
            
        filtered = []
        for event in events:
            if self._event_matches_conditions(event, filter_conditions):
                filtered.append(event)
                
        return filtered
        
    def _event_matches_conditions(self, event: StreamEvent, conditions: Dict[str, Any]) -> bool:
        """Check if event matches filter conditions"""
        for field, condition in conditions.items():
            event_value = self._extract_field_value(event, field)
            
            if not self._check_condition(event_value, condition):
                return False
                
        return True
        
    def _check_condition(self, value: Any, condition: Dict[str, Any]) -> bool:
        """Check single field condition"""
        operator = condition.get('operator', 'eq')
        expected = condition.get('value')
        
        if operator == 'eq':
            return value == expected
        elif operator == 'ne':
            return value != expected
        elif operator == 'gt':
            return value > expected
        elif operator == 'lt':
            return value < expected
        elif operator == 'gte':
            return value >= expected
        elif operator == 'lte':
            return value <= expected
        elif operator == 'in':
            return value in expected
        elif operator == 'not_in':
            return value not in expected
            
        return False
        
    def _extract_field_value(self, event: StreamEvent, field_path: str) -> Any:
        """Extract field value from event payload"""
        if not field_path:
            return None
            
        keys = field_path.split('.')
        value = event.payload
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value
        
    def _create_aggregator(self, aggregation_def: AggregationDefinition) -> StreamAggregator:
        """Create appropriate aggregator for aggregation type"""
        agg_type = aggregation_def.aggregation_type
        field_path = aggregation_def.field_path
        parameters = aggregation_def.parameters
        
        if agg_type == AggregationType.COUNT:
            return CountAggregator()
        elif agg_type == AggregationType.SUM:
            return SumAggregator(field_path)
        elif agg_type == AggregationType.AVG:
            return AvgAggregator(field_path)
        elif agg_type in [AggregationType.MIN, AggregationType.MAX]:
            return MinMaxAggregator(field_path, agg_type)
        elif agg_type in [AggregationType.MEDIAN, AggregationType.PERCENTILE, AggregationType.STDDEV, AggregationType.VARIANCE]:
            return StatisticalAggregator(field_path, agg_type, parameters)
        elif agg_type == AggregationType.DISTINCT_COUNT:
            return DistinctCountAggregator(field_path)
        elif agg_type == AggregationType.TOP_K:
            k = parameters.get('k', 10)
            return TopKAggregator(field_path, k)
        elif agg_type == AggregationType.HISTOGRAM:
            bins = parameters.get('bins', 10)
            min_val = parameters.get('min_value')
            max_val = parameters.get('max_value')
            return HistogramAggregator(field_path, bins, min_val, max_val)
        else:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")
            
    def _publish_aggregation_results(self, results: List[AggregationResult]):
        """Publish aggregation results to output topics"""
        try:
            for result in results:
                message_data = {
                    "window_id": result.window_id,
                    "aggregation_id": result.aggregation_id,
                    "window_start": result.window_start.isoformat(),
                    "window_end": result.window_end.isoformat(),
                    "group_keys": result.group_keys,
                    "aggregation_type": result.aggregation_type.value,
                    "field_path": result.field_path,
                    "result_value": result.result_value,
                    "event_count": result.event_count,
                    "calculation_time": result.calculation_time.isoformat()
                }
                
                # Send to aggregation results topic
                self.kafka_manager.produce_message(
                    topic="stream_aggregation_results",
                    message=message_data,
                    key=f"{result.window_id}_{result.aggregation_id}"
                )
                
        except Exception as e:
            self.logger.error(f"Failed to publish aggregation results: {e}")
            
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        uptime = 0
        if self.start_time:
            uptime = (datetime.now() - self.start_time).total_seconds()
            
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "processed_events": self.processed_events,
            "aggregation_calculations": self.aggregation_calculations,
            "events_per_second": self.processed_events / max(uptime, 1),
            "aggregations_per_second": self.aggregation_calculations / max(uptime, 1),
            "active_threads": len(self.processing_threads),
            "window_definitions": len(self.window_manager.windows),
            "aggregation_definitions": len(self.aggregation_definitions),
            "timestamp": datetime.now().isoformat()
        }


# Factory function
def create_stream_aggregation_processor(config: Optional[Dict[str, Any]] = None) -> StreamAggregationProcessor:
    """Create stream aggregation processor"""
    return StreamAggregationProcessor(config)


# Example usage
if __name__ == "__main__":
    processor = create_stream_aggregation_processor()
    processor.initialize()
    
    topics = [StreamingTopic.RETAIL_TRANSACTIONS.value]
    processor.start_processing(topics)
    
    print("Stream Aggregation Processor started. Press Ctrl+C to stop.")
    
    try:
        while processor.running:
            time.sleep(30)
            stats = processor.get_processing_stats()
            print(f"Stats: {stats['processed_events']} events, "
                  f"{stats['aggregation_calculations']} aggregations, "
                  f"{stats['events_per_second']:.2f} EPS")
    except KeyboardInterrupt:
        processor.stop_processing()