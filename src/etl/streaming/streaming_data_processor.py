"""
Streaming Data Processing Framework
Enterprise-grade streaming data processing for large datasets (>1GB) with
memory optimization, backpressure handling, and real-time analytics.
"""
from __future__ import annotations

import asyncio
import gc
import io
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Dict, Iterator, List, Optional, Union

import pandas as pd
import polars as pl
import psutil
from pydantic import BaseModel, Field

from core.logging import get_logger

logger = get_logger(__name__)


class StreamingMode(Enum):
    """Streaming processing modes."""
    MICRO_BATCH = "micro_batch"         # Process in small batches
    CONTINUOUS = "continuous"           # Continuous stream processing
    WINDOWED = "windowed"              # Time or count-based windows
    ADAPTIVE = "adaptive"              # Adaptive batch sizing


class BackpressureStrategy(Enum):
    """Backpressure handling strategies."""
    BUFFER = "buffer"                  # Buffer incoming data
    DROP = "drop"                      # Drop oldest data when full
    BLOCK = "block"                    # Block upstream when full
    SAMPLE = "sample"                  # Sample data when overloaded


class WindowType(Enum):
    """Windowing types for stream processing."""
    TUMBLING = "tumbling"              # Non-overlapping windows
    SLIDING = "sliding"                # Overlapping windows
    SESSION = "session"                # Session-based windows
    CUSTOM = "custom"                  # Custom windowing logic


@dataclass
class StreamingConfig:
    """Configuration for streaming data processing."""
    mode: StreamingMode = StreamingMode.MICRO_BATCH
    batch_size: int = 10000
    max_memory_mb: int = 1024
    buffer_size: int = 100000
    backpressure_strategy: BackpressureStrategy = BackpressureStrategy.BUFFER
    window_type: WindowType = WindowType.TUMBLING
    window_size_seconds: int = 60
    slide_interval_seconds: int = 30
    enable_checkpointing: bool = True
    checkpoint_interval_seconds: int = 300
    parallelism: int = 4
    enable_metrics: bool = True


@dataclass
class StreamingMetrics:
    """Metrics for streaming processing."""
    records_processed: int = 0
    records_per_second: float = 0.0
    bytes_processed: int = 0
    bytes_per_second: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    buffer_utilization_percent: float = 0.0
    processing_latency_ms: float = 0.0
    error_count: int = 0
    last_checkpoint: Optional[datetime] = None


class MemoryManager:
    """Memory management for streaming processing."""

    def __init__(self, max_memory_mb: int = 1024):
        """Initialize memory manager."""
        self.max_memory_mb = max_memory_mb
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.current_usage = 0
        self.gc_threshold_percent = 80

    def get_current_usage(self) -> int:
        """Get current memory usage in bytes."""
        process = psutil.Process()
        return process.memory_info().rss

    def is_memory_available(self, required_bytes: int) -> bool:
        """Check if required memory is available."""
        current = self.get_current_usage()
        return (current + required_bytes) < self.max_memory_bytes

    def trigger_gc_if_needed(self):
        """Trigger garbage collection if memory usage is high."""
        current = self.get_current_usage()
        usage_percent = (current / self.max_memory_bytes) * 100

        if usage_percent > self.gc_threshold_percent:
            logger.info(f"Memory usage {usage_percent:.1f}% - triggering garbage collection")
            gc.collect()

    def get_available_memory_mb(self) -> float:
        """Get available memory in MB."""
        current = self.get_current_usage()
        available = max(0, self.max_memory_bytes - current)
        return available / (1024 * 1024)


class StreamBuffer:
    """Thread-safe buffer for streaming data with backpressure handling."""

    def __init__(self, max_size: int, backpressure_strategy: BackpressureStrategy):
        """Initialize stream buffer."""
        self.max_size = max_size
        self.backpressure_strategy = backpressure_strategy
        self.buffer = deque()
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)
        self.not_full = threading.Condition(self.lock)
        self.total_added = 0
        self.total_removed = 0

    def put(self, item: Any, timeout: Optional[float] = None) -> bool:
        """Add item to buffer with backpressure handling."""
        with self.lock:
            if len(self.buffer) >= self.max_size:
                if self.backpressure_strategy == BackpressureStrategy.DROP:
                    # Drop oldest item
                    if self.buffer:
                        self.buffer.popleft()
                elif self.backpressure_strategy == BackpressureStrategy.BLOCK:
                    # Block until space available
                    if not self.not_full.wait(timeout):
                        return False
                elif self.backpressure_strategy == BackpressureStrategy.SAMPLE:
                    # Sample: only keep every nth item when full
                    if self.total_added % 10 != 0:  # Keep every 10th item
                        return True

            self.buffer.append(item)
            self.total_added += 1
            self.not_empty.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Get item from buffer."""
        with self.lock:
            while not self.buffer:
                if not self.not_empty.wait(timeout):
                    return None

            item = self.buffer.popleft()
            self.total_removed += 1
            self.not_full.notify()
            return item

    def get_batch(self, batch_size: int, timeout: Optional[float] = None) -> List[Any]:
        """Get batch of items from buffer."""
        batch = []
        end_time = time.time() + (timeout or 0)

        while len(batch) < batch_size:
            remaining_timeout = max(0, end_time - time.time()) if timeout else None
            item = self.get(remaining_timeout)

            if item is None:
                break

            batch.append(item)

        return batch

    def size(self) -> int:
        """Get current buffer size."""
        with self.lock:
            return len(self.buffer)

    def utilization(self) -> float:
        """Get buffer utilization percentage."""
        with self.lock:
            return (len(self.buffer) / self.max_size) * 100


class WindowManager:
    """Manages windowing for stream processing."""

    def __init__(self, config: StreamingConfig):
        """Initialize window manager."""
        self.config = config
        self.windows: Dict[str, List[Any]] = {}
        self.window_start_times: Dict[str, datetime] = {}

    def add_to_window(self, key: str, item: Any, timestamp: datetime):
        """Add item to appropriate window."""
        if self.config.window_type == WindowType.TUMBLING:
            window_key = self._get_tumbling_window_key(timestamp)
        elif self.config.window_type == WindowType.SLIDING:
            window_keys = self._get_sliding_window_keys(timestamp)
            for wk in window_keys:
                self._add_item_to_window(wk, item)
            return
        else:
            window_key = key  # Use provided key for custom windowing

        self._add_item_to_window(window_key, item)

    def _add_item_to_window(self, window_key: str, item: Any):
        """Add item to specific window."""
        if window_key not in self.windows:
            self.windows[window_key] = []
            self.window_start_times[window_key] = datetime.utcnow()

        self.windows[window_key].append(item)

    def _get_tumbling_window_key(self, timestamp: datetime) -> str:
        """Get tumbling window key for timestamp."""
        window_seconds = self.config.window_size_seconds
        window_start = timestamp.replace(
            second=(timestamp.second // window_seconds) * window_seconds,
            microsecond=0
        )
        return f"tumbling_{window_start.isoformat()}"

    def _get_sliding_window_keys(self, timestamp: datetime) -> List[str]:
        """Get sliding window keys for timestamp."""
        keys = []
        slide_interval = self.config.slide_interval_seconds
        window_size = self.config.window_size_seconds

        # Calculate how many windows this timestamp belongs to
        num_windows = window_size // slide_interval

        for i in range(num_windows):
            window_start = timestamp.replace(
                second=((timestamp.second // slide_interval) - i) * slide_interval,
                microsecond=0
            )
            keys.append(f"sliding_{window_start.isoformat()}")

        return keys

    def get_completed_windows(self) -> Dict[str, List[Any]]:
        """Get windows that are ready for processing."""
        current_time = datetime.utcnow()
        completed = {}

        for window_key, start_time in list(self.window_start_times.items()):
            if (current_time - start_time).total_seconds() >= self.config.window_size_seconds:
                completed[window_key] = self.windows.pop(window_key)
                del self.window_start_times[window_key]

        return completed


class StreamProcessor(ABC):
    """Abstract base class for stream processors."""

    @abstractmethod
    async def process_batch(self, batch: List[Any]) -> List[Any]:
        """Process a batch of data."""
        pass

    @abstractmethod
    async def process_window(self, window_key: str, window_data: List[Any]) -> Any:
        """Process a complete window of data."""
        pass


class DataFrameStreamProcessor(StreamProcessor):
    """Stream processor using pandas/polars DataFrames."""

    def __init__(self, use_polars: bool = False):
        """Initialize DataFrame processor."""
        self.use_polars = use_polars

    async def process_batch(self, batch: List[Any]) -> List[Any]:
        """Process batch using DataFrames."""
        if not batch:
            return []

        try:
            if self.use_polars:
                df = pl.DataFrame(batch)
                # Example processing: basic cleaning and enrichment
                processed_df = df.with_columns([
                    pl.when(pl.col("value").is_null()).then(0).otherwise(pl.col("value")).alias("value"),
                    pl.lit(datetime.utcnow()).alias("processed_at")
                ])
                return processed_df.to_dicts()
            else:
                df = pd.DataFrame(batch)
                # Example processing: basic cleaning and enrichment
                df['value'] = df['value'].fillna(0)
                df['processed_at'] = datetime.utcnow()
                return df.to_dict('records')

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return batch

    async def process_window(self, window_key: str, window_data: List[Any]) -> Any:
        """Process complete window with aggregations."""
        if not window_data:
            return None

        try:
            if self.use_polars:
                df = pl.DataFrame(window_data)
                summary = {
                    'window_key': window_key,
                    'record_count': len(df),
                    'avg_value': df.select(pl.col("value").mean()).item() if 'value' in df.columns else 0,
                    'sum_value': df.select(pl.col("value").sum()).item() if 'value' in df.columns else 0,
                    'window_processed_at': datetime.utcnow()
                }
            else:
                df = pd.DataFrame(window_data)
                summary = {
                    'window_key': window_key,
                    'record_count': len(df),
                    'avg_value': df['value'].mean() if 'value' in df.columns else 0,
                    'sum_value': df['value'].sum() if 'value' in df.columns else 0,
                    'window_processed_at': datetime.utcnow()
                }

            return summary

        except Exception as e:
            logger.error(f"Error processing window {window_key}: {e}")
            return {'window_key': window_key, 'error': str(e)}


class StreamingDataProcessor:
    """
    Enterprise-grade streaming data processor for large datasets with
    memory optimization, backpressure handling, and real-time processing.
    """

    def __init__(self, config: StreamingConfig, processor: StreamProcessor):
        """Initialize streaming data processor."""
        self.config = config
        self.processor = processor
        self.memory_manager = MemoryManager(config.max_memory_mb)
        self.buffer = StreamBuffer(config.buffer_size, config.backpressure_strategy)
        self.window_manager = WindowManager(config)

        # Processing state
        self.is_running = False
        self.metrics = StreamingMetrics()
        self.workers: List[asyncio.Task] = []
        self.checkpoint_data: Dict[str, Any] = {}

        # Performance tracking
        self.last_metrics_update = time.time()
        self.processed_since_last_update = 0
        self.bytes_processed_since_last_update = 0

    async def start(self):
        """Start streaming processing."""
        if self.is_running:
            logger.warning("Stream processor is already running")
            return

        self.is_running = True
        logger.info("Starting streaming data processor")

        # Start worker tasks
        for i in range(self.config.parallelism):
            worker = asyncio.create_task(self._worker_loop(f"worker_{i}"))
            self.workers.append(worker)

        # Start metrics collection
        if self.config.enable_metrics:
            metrics_task = asyncio.create_task(self._metrics_loop())
            self.workers.append(metrics_task)

        # Start checkpointing
        if self.config.enable_checkpointing:
            checkpoint_task = asyncio.create_task(self._checkpoint_loop())
            self.workers.append(checkpoint_task)

        logger.info(f"Started {len(self.workers)} worker tasks")

    async def stop(self):
        """Stop streaming processing."""
        if not self.is_running:
            return

        logger.info("Stopping streaming data processor")
        self.is_running = False

        # Cancel all workers
        for worker in self.workers:
            worker.cancel()

        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

        logger.info("Streaming data processor stopped")

    async def put_data(self, data: Any, timeout: Optional[float] = None) -> bool:
        """Add data to processing stream."""
        if not self.is_running:
            raise RuntimeError("Stream processor is not running")

        return self.buffer.put(data, timeout)

    async def put_data_batch(self, batch: List[Any], timeout: Optional[float] = None) -> int:
        """Add batch of data to processing stream."""
        added_count = 0
        for item in batch:
            if await self.put_data(item, timeout):
                added_count += 1
            else:
                break
        return added_count

    async def _worker_loop(self, worker_id: str):
        """Main worker loop for processing data."""
        logger.info(f"Worker {worker_id} started")

        while self.is_running:
            try:
                # Check memory availability
                self.memory_manager.trigger_gc_if_needed()

                if not self.memory_manager.is_memory_available(10 * 1024 * 1024):  # 10MB buffer
                    logger.warning(f"Worker {worker_id}: Low memory, waiting...")
                    await asyncio.sleep(1)
                    continue

                # Get batch from buffer
                batch = self.buffer.get_batch(self.config.batch_size, timeout=1.0)

                if not batch:
                    await asyncio.sleep(0.1)  # Short sleep if no data
                    continue

                # Process batch
                start_time = time.time()
                processed_batch = await self.processor.process_batch(batch)
                processing_time = time.time() - start_time

                # Update metrics
                self.metrics.records_processed += len(batch)
                self.processed_since_last_update += len(batch)

                # Estimate bytes processed
                batch_bytes = self._estimate_batch_size(batch)
                self.metrics.bytes_processed += batch_bytes
                self.bytes_processed_since_last_update += batch_bytes

                # Update processing latency
                self.metrics.processing_latency_ms = processing_time * 1000

                # Handle windowing if enabled
                if self.config.window_type != WindowType.CUSTOM:
                    await self._handle_windowing(processed_batch)

                logger.debug(f"Worker {worker_id}: Processed batch of {len(batch)} records in {processing_time:.3f}s")

            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                self.metrics.error_count += 1
                await asyncio.sleep(1)  # Brief pause on error

        logger.info(f"Worker {worker_id} stopped")

    async def _handle_windowing(self, batch: List[Any]):
        """Handle windowing for processed batch."""
        current_time = datetime.utcnow()

        for item in batch:
            # Add to appropriate windows
            self.window_manager.add_to_window("default", item, current_time)

        # Process completed windows
        completed_windows = self.window_manager.get_completed_windows()
        for window_key, window_data in completed_windows.items():
            try:
                window_result = await self.processor.process_window(window_key, window_data)
                logger.info(f"Processed window {window_key}: {window_result}")
            except Exception as e:
                logger.error(f"Error processing window {window_key}: {e}")

    async def _metrics_loop(self):
        """Background loop for updating metrics."""
        while self.is_running:
            try:
                await asyncio.sleep(10)  # Update every 10 seconds
                await self._update_metrics()
            except Exception as e:
                logger.error(f"Metrics loop error: {e}")

    async def _update_metrics(self):
        """Update streaming metrics."""
        current_time = time.time()
        time_delta = current_time - self.last_metrics_update

        if time_delta > 0:
            # Calculate rates
            self.metrics.records_per_second = self.processed_since_last_update / time_delta
            self.metrics.bytes_per_second = self.bytes_processed_since_last_update / time_delta

            # Reset counters
            self.processed_since_last_update = 0
            self.bytes_processed_since_last_update = 0
            self.last_metrics_update = current_time

        # Update system metrics
        self.metrics.memory_usage_mb = self.memory_manager.get_current_usage() / (1024 * 1024)

        try:
            process = psutil.Process()
            self.metrics.cpu_usage_percent = process.cpu_percent()
        except:
            pass

        # Update buffer utilization
        self.metrics.buffer_utilization_percent = self.buffer.utilization()

        logger.debug(f"Metrics: {self.metrics.records_per_second:.1f} records/sec, "
                    f"{self.metrics.memory_usage_mb:.1f}MB memory, "
                    f"{self.metrics.buffer_utilization_percent:.1f}% buffer")

    async def _checkpoint_loop(self):
        """Background loop for checkpointing."""
        while self.is_running:
            try:
                await asyncio.sleep(self.config.checkpoint_interval_seconds)
                await self._create_checkpoint()
            except Exception as e:
                logger.error(f"Checkpoint loop error: {e}")

    async def _create_checkpoint(self):
        """Create processing checkpoint."""
        checkpoint = {
            'timestamp': datetime.utcnow(),
            'metrics': self.metrics,
            'buffer_size': self.buffer.size(),
            'memory_usage_mb': self.metrics.memory_usage_mb
        }

        self.checkpoint_data = checkpoint
        self.metrics.last_checkpoint = checkpoint['timestamp']

        logger.info(f"Checkpoint created: {self.metrics.records_processed} records processed")

    def _estimate_batch_size(self, batch: List[Any]) -> int:
        """Estimate size of batch in bytes."""
        if not batch:
            return 0

        # Sample a few items to estimate average size
        sample_size = min(10, len(batch))
        sample_bytes = 0

        for i in range(sample_size):
            try:
                if isinstance(batch[i], (str, bytes)):
                    sample_bytes += len(batch[i].encode() if isinstance(batch[i], str) else batch[i])
                elif isinstance(batch[i], dict):
                    sample_bytes += len(str(batch[i]).encode())
                else:
                    sample_bytes += 100  # Rough estimate

            except:
                sample_bytes += 100  # Fallback estimate

        # Extrapolate to full batch
        avg_size = sample_bytes / sample_size if sample_size > 0 else 100
        return int(avg_size * len(batch))

    def get_metrics(self) -> StreamingMetrics:
        """Get current streaming metrics."""
        return self.metrics

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive processor status."""
        return {
            'is_running': self.is_running,
            'workers_active': len(self.workers),
            'buffer_size': self.buffer.size(),
            'buffer_utilization': self.buffer.utilization(),
            'memory_available_mb': self.memory_manager.get_available_memory_mb(),
            'metrics': self.metrics,
            'last_checkpoint': self.metrics.last_checkpoint.isoformat() if self.metrics.last_checkpoint else None,
            'config': {
                'mode': self.config.mode.value,
                'batch_size': self.config.batch_size,
                'parallelism': self.config.parallelism,
                'window_type': self.config.window_type.value
            }
        }


# Factory functions for common configurations
def create_micro_batch_processor(
    batch_size: int = 10000,
    parallelism: int = 4,
    max_memory_mb: int = 1024,
    use_polars: bool = False
) -> StreamingDataProcessor:
    """Create micro-batch streaming processor."""
    config = StreamingConfig(
        mode=StreamingMode.MICRO_BATCH,
        batch_size=batch_size,
        parallelism=parallelism,
        max_memory_mb=max_memory_mb
    )

    processor = DataFrameStreamProcessor(use_polars=use_polars)
    return StreamingDataProcessor(config, processor)


def create_windowed_processor(
    window_size_seconds: int = 60,
    slide_interval_seconds: int = 30,
    window_type: WindowType = WindowType.TUMBLING,
    max_memory_mb: int = 1024,
    use_polars: bool = False
) -> StreamingDataProcessor:
    """Create windowed streaming processor."""
    config = StreamingConfig(
        mode=StreamingMode.WINDOWED,
        window_type=window_type,
        window_size_seconds=window_size_seconds,
        slide_interval_seconds=slide_interval_seconds,
        max_memory_mb=max_memory_mb
    )

    processor = DataFrameStreamProcessor(use_polars=use_polars)
    return StreamingDataProcessor(config, processor)


# Export key classes and functions
__all__ = [
    'StreamingDataProcessor',
    'StreamingConfig',
    'StreamingMetrics',
    'StreamProcessor',
    'DataFrameStreamProcessor',
    'MemoryManager',
    'StreamBuffer',
    'WindowManager',
    'StreamingMode',
    'BackpressureStrategy',
    'WindowType',
    'create_micro_batch_processor',
    'create_windowed_processor'
]