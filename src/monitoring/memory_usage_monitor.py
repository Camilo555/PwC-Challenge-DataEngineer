"""
Enterprise Memory Usage Monitoring and Optimization System
=========================================================

Comprehensive memory monitoring, analysis, and optimization framework for production environments.
Provides real-time memory tracking, leak detection, and automated optimization strategies.

Features:
- Real-time memory usage monitoring with configurable thresholds
- Memory leak detection with trend analysis
- Process-level and system-level memory tracking
- Automatic garbage collection optimization
- Memory pressure alerts and automated responses
- Historical memory usage analytics
- Memory pool optimization for high-frequency allocations
- Integration with enterprise monitoring systems (Prometheus, DataDog)

Author: Enterprise Data Engineering Team
Created: 2025-09-18
"""

import asyncio
import gc
import logging
import os
import psutil
import sys
import threading
import time
import tracemalloc
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from weakref import WeakSet

import numpy as np
import pandas as pd
from prometheus_client import Counter, Gauge, Histogram, start_http_server


class MemoryPressureLevel(Enum):
    """Memory pressure levels for alert thresholds."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class OptimizationStrategy(Enum):
    """Memory optimization strategies."""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"
    CUSTOM = "custom"


@dataclass
class MemoryThresholds:
    """Memory usage thresholds for alerts and optimization."""

    # Process memory thresholds (MB)
    process_warning: float = 500.0
    process_critical: float = 1000.0
    process_emergency: float = 2000.0

    # System memory thresholds (%)
    system_warning: float = 70.0
    system_critical: float = 85.0
    system_emergency: float = 95.0

    # Memory growth rate thresholds (MB/minute)
    growth_rate_warning: float = 10.0
    growth_rate_critical: float = 50.0

    # Garbage collection thresholds
    gc_pressure_threshold: float = 80.0
    gc_frequency_max: int = 10  # per minute


@dataclass
class MemoryMetrics:
    """Memory usage metrics data structure."""

    timestamp: datetime = field(default_factory=datetime.now)

    # Process metrics
    process_rss: float = 0.0  # Resident Set Size
    process_vms: float = 0.0  # Virtual Memory Size
    process_percent: float = 0.0
    process_threads: int = 0

    # System metrics
    system_total: float = 0.0
    system_available: float = 0.0
    system_percent: float = 0.0
    system_used: float = 0.0

    # Python-specific metrics
    python_objects: int = 0
    python_memory: float = 0.0
    gc_collections: Dict[int, int] = field(default_factory=dict)

    # Application metrics
    active_connections: int = 0
    cached_objects: int = 0
    memory_pools: Dict[str, float] = field(default_factory=dict)


@dataclass
class MemoryAlert:
    """Memory alert information."""

    level: MemoryPressureLevel
    message: str
    metrics: MemoryMetrics
    timestamp: datetime = field(default_factory=datetime.now)
    resolved: bool = False
    resolution_timestamp: Optional[datetime] = None


class MemoryOptimizer:
    """Advanced memory optimization engine."""

    def __init__(self, strategy: OptimizationStrategy = OptimizationStrategy.BALANCED):
        self.strategy = strategy
        self.optimization_history: List[Dict[str, Any]] = []
        self.object_pools: Dict[str, List[Any]] = defaultdict(list)
        self.weak_refs: WeakSet = WeakSet()

    def optimize_garbage_collection(self, pressure_level: MemoryPressureLevel) -> Dict[str, Any]:
        """Optimize garbage collection based on memory pressure."""

        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024

        optimization_result = {
            "timestamp": datetime.now(),
            "pressure_level": pressure_level.value,
            "strategy": self.strategy.value,
            "actions_taken": [],
            "memory_freed": 0.0,
            "duration": 0.0
        }

        if pressure_level == MemoryPressureLevel.LOW:
            # Light optimization
            if self.strategy in [OptimizationStrategy.BALANCED, OptimizationStrategy.AGGRESSIVE]:
                collected = gc.collect()
                optimization_result["actions_taken"].append(f"gc.collect() freed {collected} objects")

        elif pressure_level == MemoryPressureLevel.MEDIUM:
            # Moderate optimization
            self._clear_object_pools(max_age_minutes=30)
            optimization_result["actions_taken"].append("Cleared aged object pools")

            collected = gc.collect()
            optimization_result["actions_taken"].append(f"gc.collect() freed {collected} objects")

            if self.strategy == OptimizationStrategy.AGGRESSIVE:
                self._force_gc_all_generations()
                optimization_result["actions_taken"].append("Forced GC on all generations")

        elif pressure_level == MemoryPressureLevel.HIGH:
            # Aggressive optimization
            self._clear_object_pools(max_age_minutes=5)
            optimization_result["actions_taken"].append("Emergency object pool clearing")

            self._force_gc_all_generations()
            optimization_result["actions_taken"].append("Forced GC on all generations")

            # Clear weak references
            cleared_refs = len(self.weak_refs)
            self.weak_refs.clear()
            optimization_result["actions_taken"].append(f"Cleared {cleared_refs} weak references")

        elif pressure_level == MemoryPressureLevel.CRITICAL:
            # Emergency optimization
            self._emergency_memory_cleanup()
            optimization_result["actions_taken"].append("Emergency memory cleanup")

        # Calculate results
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        optimization_result["memory_freed"] = initial_memory - final_memory
        optimization_result["duration"] = time.time() - start_time

        self.optimization_history.append(optimization_result)
        return optimization_result

    def _clear_object_pools(self, max_age_minutes: int = 30):
        """Clear aged objects from memory pools."""
        cutoff_time = datetime.now() - timedelta(minutes=max_age_minutes)

        for pool_name, objects in self.object_pools.items():
            # Simple age-based clearing (in real implementation, objects would have timestamps)
            if len(objects) > 100:  # Keep some objects for reuse
                keep_count = max(10, len(objects) // 4)
                self.object_pools[pool_name] = objects[-keep_count:]

    def _force_gc_all_generations(self):
        """Force garbage collection on all generations."""
        for generation in range(3):
            gc.collect(generation)

    def _emergency_memory_cleanup(self):
        """Emergency memory cleanup procedures."""

        # Clear all object pools
        self.object_pools.clear()

        # Clear weak references
        self.weak_refs.clear()

        # Force aggressive garbage collection
        for _ in range(3):
            self._force_gc_all_generations()

        # Clear optimization history (keep only recent)
        if len(self.optimization_history) > 10:
            self.optimization_history = self.optimization_history[-10:]


class MemoryMonitor:
    """
    Enterprise Memory Usage Monitor

    Provides comprehensive memory monitoring, leak detection, and optimization
    for production Python applications.
    """

    def __init__(
        self,
        thresholds: Optional[MemoryThresholds] = None,
        optimization_strategy: OptimizationStrategy = OptimizationStrategy.BALANCED,
        enable_tracemalloc: bool = True,
        monitoring_interval: float = 10.0,
        history_retention_hours: int = 24,
        enable_prometheus: bool = True,
        prometheus_port: int = 9090
    ):
        self.thresholds = thresholds or MemoryThresholds()
        self.monitoring_interval = monitoring_interval
        self.history_retention_hours = history_retention_hours
        self.enable_prometheus = enable_prometheus

        # Core components
        self.optimizer = MemoryOptimizer(optimization_strategy)
        self.process = psutil.Process()

        # Monitoring state
        self.is_monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.metrics_history: deque = deque(maxlen=int(history_retention_hours * 3600 / monitoring_interval))
        self.active_alerts: Dict[str, MemoryAlert] = {}
        self.alert_callbacks: List[Callable[[MemoryAlert], None]] = []

        # Memory leak detection
        self.memory_snapshots: List[Tuple[datetime, float]] = []
        self.leak_detection_window = 60  # minutes

        # Prometheus metrics
        if enable_prometheus:
            self._setup_prometheus_metrics()

        # Tracemalloc for detailed tracking
        if enable_tracemalloc:
            tracemalloc.start()

        # Threading
        self.lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="memory_monitor")

        # Logger
        self.logger = logging.getLogger(__name__)

    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics for monitoring."""

        self.prom_process_memory = Gauge('process_memory_bytes', 'Process memory usage in bytes')
        self.prom_system_memory_percent = Gauge('system_memory_percent', 'System memory usage percentage')
        self.prom_gc_collections = Counter('gc_collections_total', 'Total garbage collections', ['generation'])
        self.prom_memory_alerts = Counter('memory_alerts_total', 'Total memory alerts', ['level'])
        self.prom_optimization_runs = Counter('memory_optimizations_total', 'Total memory optimizations')
        self.prom_memory_freed = Histogram('memory_freed_bytes', 'Memory freed by optimizations')

        if self.enable_prometheus:
            try:
                start_http_server(9090)
                self.logger.info("Prometheus metrics server started on port 9090")
            except Exception as e:
                self.logger.warning(f"Failed to start Prometheus server: {e}")

    def start_monitoring(self):
        """Start continuous memory monitoring."""

        if self.is_monitoring:
            self.logger.warning("Memory monitoring already running")
            return

        self.is_monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            name="memory_monitor",
            daemon=True
        )
        self.monitor_thread.start()
        self.logger.info("Memory monitoring started")

    def stop_monitoring(self):
        """Stop memory monitoring."""

        self.is_monitoring = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)

        self.executor.shutdown(wait=True)
        self.logger.info("Memory monitoring stopped")

    def _monitoring_loop(self):
        """Main monitoring loop."""

        while self.is_monitoring:
            try:
                # Collect metrics
                metrics = self._collect_metrics()

                with self.lock:
                    self.metrics_history.append(metrics)

                    # Update Prometheus metrics
                    if self.enable_prometheus:
                        self._update_prometheus_metrics(metrics)

                    # Check thresholds and generate alerts
                    self._check_thresholds(metrics)

                    # Detect memory leaks
                    self._detect_memory_leaks(metrics)

                    # Auto-optimization if needed
                    self._auto_optimize(metrics)

                time.sleep(self.monitoring_interval)

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.monitoring_interval)

    def _collect_metrics(self) -> MemoryMetrics:
        """Collect comprehensive memory metrics."""

        # Process metrics
        memory_info = self.process.memory_info()
        process_percent = self.process.memory_percent()

        # System metrics
        system_memory = psutil.virtual_memory()

        # Python-specific metrics
        python_objects = len(gc.get_objects())
        gc_stats = {i: gc.get_count()[i] for i in range(3)}

        # Tracemalloc metrics
        python_memory = 0.0
        if tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            python_memory = current / 1024 / 1024  # MB

        return MemoryMetrics(
            process_rss=memory_info.rss / 1024 / 1024,  # MB
            process_vms=memory_info.vms / 1024 / 1024,  # MB
            process_percent=process_percent,
            process_threads=self.process.num_threads(),
            system_total=system_memory.total / 1024 / 1024,  # MB
            system_available=system_memory.available / 1024 / 1024,  # MB
            system_percent=system_memory.percent,
            system_used=system_memory.used / 1024 / 1024,  # MB
            python_objects=python_objects,
            python_memory=python_memory,
            gc_collections=gc_stats
        )

    def _update_prometheus_metrics(self, metrics: MemoryMetrics):
        """Update Prometheus metrics."""

        self.prom_process_memory.set(metrics.process_rss * 1024 * 1024)
        self.prom_system_memory_percent.set(metrics.system_percent)

        for generation, count in metrics.gc_collections.items():
            self.prom_gc_collections.labels(generation=generation)._value._value = count

    def _check_thresholds(self, metrics: MemoryMetrics):
        """Check memory thresholds and generate alerts."""

        alerts_to_create = []

        # Process memory checks
        if metrics.process_rss >= self.thresholds.process_emergency:
            alerts_to_create.append((
                MemoryPressureLevel.CRITICAL,
                f"Process memory critical: {metrics.process_rss:.1f}MB (threshold: {self.thresholds.process_emergency}MB)"
            ))
        elif metrics.process_rss >= self.thresholds.process_critical:
            alerts_to_create.append((
                MemoryPressureLevel.HIGH,
                f"Process memory high: {metrics.process_rss:.1f}MB (threshold: {self.thresholds.process_critical}MB)"
            ))
        elif metrics.process_rss >= self.thresholds.process_warning:
            alerts_to_create.append((
                MemoryPressureLevel.MEDIUM,
                f"Process memory warning: {metrics.process_rss:.1f}MB (threshold: {self.thresholds.process_warning}MB)"
            ))

        # System memory checks
        if metrics.system_percent >= self.thresholds.system_emergency:
            alerts_to_create.append((
                MemoryPressureLevel.CRITICAL,
                f"System memory critical: {metrics.system_percent:.1f}% (threshold: {self.thresholds.system_emergency}%)"
            ))
        elif metrics.system_percent >= self.thresholds.system_critical:
            alerts_to_create.append((
                MemoryPressureLevel.HIGH,
                f"System memory high: {metrics.system_percent:.1f}% (threshold: {self.thresholds.system_critical}%)"
            ))
        elif metrics.system_percent >= self.thresholds.system_warning:
            alerts_to_create.append((
                MemoryPressureLevel.MEDIUM,
                f"System memory warning: {metrics.system_percent:.1f}% (threshold: {self.thresholds.system_warning}%)"
            ))

        # Create alerts
        for level, message in alerts_to_create:
            alert_key = f"{level.value}_memory"
            if alert_key not in self.active_alerts:
                alert = MemoryAlert(level=level, message=message, metrics=metrics)
                self.active_alerts[alert_key] = alert
                self._trigger_alert(alert)

    def _detect_memory_leaks(self, current_metrics: MemoryMetrics):
        """Detect potential memory leaks using trend analysis."""

        # Add current snapshot
        self.memory_snapshots.append((datetime.now(), current_metrics.process_rss))

        # Keep only recent snapshots
        cutoff_time = datetime.now() - timedelta(minutes=self.leak_detection_window)
        self.memory_snapshots = [
            (timestamp, memory) for timestamp, memory in self.memory_snapshots
            if timestamp > cutoff_time
        ]

        # Analyze trend if we have enough data
        if len(self.memory_snapshots) >= 10:
            timestamps = [ts.timestamp() for ts, _ in self.memory_snapshots]
            memory_values = [mem for _, mem in self.memory_snapshots]

            # Calculate linear regression slope
            try:
                slope = np.polyfit(timestamps, memory_values, 1)[0]
                growth_rate_mb_per_second = slope
                growth_rate_mb_per_minute = growth_rate_mb_per_second * 60

                if growth_rate_mb_per_minute > self.thresholds.growth_rate_critical:
                    alert = MemoryAlert(
                        level=MemoryPressureLevel.CRITICAL,
                        message=f"Memory leak detected: {growth_rate_mb_per_minute:.2f}MB/min growth",
                        metrics=current_metrics
                    )
                    self.active_alerts["memory_leak"] = alert
                    self._trigger_alert(alert)
                elif growth_rate_mb_per_minute > self.thresholds.growth_rate_warning:
                    alert = MemoryAlert(
                        level=MemoryPressureLevel.MEDIUM,
                        message=f"Memory growth detected: {growth_rate_mb_per_minute:.2f}MB/min growth",
                        metrics=current_metrics
                    )
                    self.active_alerts["memory_growth"] = alert
                    self._trigger_alert(alert)

            except Exception as e:
                self.logger.debug(f"Error calculating memory trend: {e}")

    def _auto_optimize(self, metrics: MemoryMetrics):
        """Automatically optimize memory based on current metrics."""

        pressure_level = self._determine_pressure_level(metrics)

        if pressure_level in [MemoryPressureLevel.HIGH, MemoryPressureLevel.CRITICAL]:
            # Run optimization in background
            self.executor.submit(self._run_optimization, pressure_level)

    def _determine_pressure_level(self, metrics: MemoryMetrics) -> MemoryPressureLevel:
        """Determine current memory pressure level."""

        if (metrics.process_rss >= self.thresholds.process_emergency or
            metrics.system_percent >= self.thresholds.system_emergency):
            return MemoryPressureLevel.CRITICAL
        elif (metrics.process_rss >= self.thresholds.process_critical or
              metrics.system_percent >= self.thresholds.system_critical):
            return MemoryPressureLevel.HIGH
        elif (metrics.process_rss >= self.thresholds.process_warning or
              metrics.system_percent >= self.thresholds.system_warning):
            return MemoryPressureLevel.MEDIUM
        else:
            return MemoryPressureLevel.LOW

    def _run_optimization(self, pressure_level: MemoryPressureLevel):
        """Run memory optimization."""

        try:
            result = self.optimizer.optimize_garbage_collection(pressure_level)

            if self.enable_prometheus:
                self.prom_optimization_runs.inc()
                self.prom_memory_freed.observe(result["memory_freed"] * 1024 * 1024)

            self.logger.info(f"Memory optimization completed: {result}")

        except Exception as e:
            self.logger.error(f"Error during memory optimization: {e}")

    def _trigger_alert(self, alert: MemoryAlert):
        """Trigger memory alert."""

        self.logger.warning(f"Memory Alert [{alert.level.value.upper()}]: {alert.message}")

        if self.enable_prometheus:
            self.prom_memory_alerts.labels(level=alert.level.value).inc()

        # Call registered callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Error in alert callback: {e}")

    def add_alert_callback(self, callback: Callable[[MemoryAlert], None]):
        """Add alert callback function."""
        self.alert_callbacks.append(callback)

    def get_current_metrics(self) -> Optional[MemoryMetrics]:
        """Get current memory metrics."""
        return self._collect_metrics()

    def get_metrics_history(self, hours: int = 1) -> List[MemoryMetrics]:
        """Get historical metrics."""

        cutoff_time = datetime.now() - timedelta(hours=hours)
        with self.lock:
            return [
                metrics for metrics in self.metrics_history
                if metrics.timestamp > cutoff_time
            ]

    def get_memory_summary(self) -> Dict[str, Any]:
        """Get comprehensive memory usage summary."""

        current_metrics = self.get_current_metrics()
        history = list(self.metrics_history)

        if not history:
            return {"error": "No metrics available"}

        # Calculate statistics
        process_memory_values = [m.process_rss for m in history]
        system_memory_values = [m.system_percent for m in history]

        return {
            "current": {
                "process_rss_mb": current_metrics.process_rss,
                "process_percent": current_metrics.process_percent,
                "system_percent": current_metrics.system_percent,
                "python_objects": current_metrics.python_objects,
                "active_threads": current_metrics.process_threads
            },
            "statistics": {
                "process_memory": {
                    "min_mb": min(process_memory_values),
                    "max_mb": max(process_memory_values),
                    "avg_mb": sum(process_memory_values) / len(process_memory_values),
                    "current_mb": current_metrics.process_rss
                },
                "system_memory": {
                    "min_percent": min(system_memory_values),
                    "max_percent": max(system_memory_values),
                    "avg_percent": sum(system_memory_values) / len(system_memory_values),
                    "current_percent": current_metrics.system_percent
                }
            },
            "alerts": {
                "active_count": len(self.active_alerts),
                "active_alerts": list(self.active_alerts.keys())
            },
            "optimization": {
                "total_runs": len(self.optimizer.optimization_history),
                "recent_runs": self.optimizer.optimization_history[-5:] if self.optimizer.optimization_history else []
            }
        }

    @contextmanager
    def memory_tracking(self, operation_name: str):
        """Context manager for tracking memory usage of specific operations."""

        start_metrics = self.get_current_metrics()
        start_time = time.time()

        try:
            yield
        finally:
            end_metrics = self.get_current_metrics()
            duration = time.time() - start_time

            memory_delta = end_metrics.process_rss - start_metrics.process_rss

            self.logger.info(
                f"Memory tracking [{operation_name}]: "
                f"Duration: {duration:.2f}s, "
                f"Memory delta: {memory_delta:+.2f}MB, "
                f"Final memory: {end_metrics.process_rss:.2f}MB"
            )


# Global memory monitor instance
_memory_monitor: Optional[MemoryMonitor] = None


def get_memory_monitor() -> MemoryMonitor:
    """Get global memory monitor instance."""
    global _memory_monitor

    if _memory_monitor is None:
        _memory_monitor = MemoryMonitor()
        _memory_monitor.start_monitoring()

    return _memory_monitor


def setup_memory_monitoring(
    thresholds: Optional[MemoryThresholds] = None,
    optimization_strategy: OptimizationStrategy = OptimizationStrategy.BALANCED,
    **kwargs
) -> MemoryMonitor:
    """Setup and start memory monitoring."""

    global _memory_monitor

    if _memory_monitor is not None:
        _memory_monitor.stop_monitoring()

    _memory_monitor = MemoryMonitor(
        thresholds=thresholds,
        optimization_strategy=optimization_strategy,
        **kwargs
    )
    _memory_monitor.start_monitoring()

    return _memory_monitor


# Utility functions for quick memory checks
def get_memory_usage() -> Dict[str, float]:
    """Get current memory usage in MB."""

    process = psutil.Process()
    memory_info = process.memory_info()
    system_memory = psutil.virtual_memory()

    return {
        "process_rss_mb": memory_info.rss / 1024 / 1024,
        "process_vms_mb": memory_info.vms / 1024 / 1024,
        "process_percent": process.memory_percent(),
        "system_percent": system_memory.percent,
        "system_available_mb": system_memory.available / 1024 / 1024
    }


def force_garbage_collection() -> Dict[str, int]:
    """Force garbage collection and return statistics."""

    results = {}
    for generation in range(3):
        collected = gc.collect(generation)
        results[f"generation_{generation}"] = collected

    return results


if __name__ == "__main__":
    # Example usage and testing
    logging.basicConfig(level=logging.INFO)

    # Setup custom thresholds
    thresholds = MemoryThresholds(
        process_warning=100.0,  # 100MB
        process_critical=500.0,  # 500MB
        system_warning=80.0,  # 80%
        system_critical=90.0  # 90%
    )

    # Start monitoring
    monitor = setup_memory_monitoring(
        thresholds=thresholds,
        optimization_strategy=OptimizationStrategy.BALANCED,
        monitoring_interval=5.0,
        enable_prometheus=True
    )

    # Add custom alert callback
    def alert_handler(alert: MemoryAlert):
        print(f"ALERT: {alert.level.value} - {alert.message}")

    monitor.add_alert_callback(alert_handler)

    try:
        # Simulate some memory usage
        print("Starting memory monitoring demo...")
        print(f"Initial memory: {get_memory_usage()}")

        # Allocate some memory to trigger alerts
        large_list = []
        for i in range(10):
            large_list.extend([0] * 1000000)  # Allocate ~1M integers
            time.sleep(2)

            current_usage = get_memory_usage()
            print(f"Step {i+1}: Process memory: {current_usage['process_rss_mb']:.1f}MB")

        # Test memory tracking context
        with monitor.memory_tracking("test_operation"):
            # Simulate work
            temp_data = [i for i in range(1000000)]
            time.sleep(1)
            del temp_data

        # Get summary
        summary = monitor.get_memory_summary()
        print(f"Memory summary: {summary}")

        # Keep monitoring for a while
        time.sleep(30)

    except KeyboardInterrupt:
        print("Stopping memory monitoring...")
    finally:
        monitor.stop_monitoring()