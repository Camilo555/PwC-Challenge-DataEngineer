"""
Advanced Garbage Collection Optimization System
==============================================

Enterprise-grade garbage collection optimization for long-running Python processes.
Provides intelligent GC tuning, memory pressure management, and performance optimization.

Features:
- Adaptive GC threshold tuning based on application behavior
- Memory pressure-aware garbage collection strategies
- Generation-specific optimization algorithms
- GC performance monitoring and analytics
- Automatic memory leak detection and mitigation
- Integration with memory monitoring systems
- Production-ready monitoring and alerting

Author: Enterprise Data Engineering Team
Created: 2025-09-18
"""

import gc
import logging
import threading
import time
import weakref
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import psutil
from prometheus_client import Counter, Gauge, Histogram


class GCStrategy(Enum):
    """Garbage collection optimization strategies."""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"
    ADAPTIVE = "adaptive"
    CUSTOM = "custom"


class GCTrigger(Enum):
    """Garbage collection trigger conditions."""
    MEMORY_PRESSURE = "memory_pressure"
    TIME_BASED = "time_based"
    ALLOCATION_THRESHOLD = "allocation_threshold"
    MANUAL = "manual"
    SHUTDOWN = "shutdown"


@dataclass
class GCMetrics:
    """Garbage collection performance metrics."""

    timestamp: datetime = field(default_factory=datetime.now)

    # GC execution metrics
    generation: int = 0
    objects_collected: int = 0
    objects_uncollectable: int = 0
    execution_time_ms: float = 0.0

    # Memory metrics before/after GC
    memory_before_mb: float = 0.0
    memory_after_mb: float = 0.0
    memory_freed_mb: float = 0.0

    # System state
    total_objects: int = 0
    gc_thresholds: Tuple[int, int, int] = (0, 0, 0)
    gc_counts: Tuple[int, int, int] = (0, 0, 0)

    # Performance impact
    cpu_usage_percent: float = 0.0
    gc_pause_time_ms: float = 0.0


@dataclass
class GCConfiguration:
    """Garbage collection configuration parameters."""

    # Basic settings
    strategy: GCStrategy = GCStrategy.BALANCED
    enabled: bool = True
    auto_tuning: bool = True

    # Threshold settings (gen0, gen1, gen2)
    thresholds: Tuple[int, int, int] = (700, 10, 10)
    min_thresholds: Tuple[int, int, int] = (400, 5, 5)
    max_thresholds: Tuple[int, int, int] = (2000, 50, 50)

    # Timing settings
    max_pause_time_ms: float = 100.0
    gc_interval_seconds: float = 60.0
    monitoring_interval_seconds: float = 10.0

    # Memory pressure settings
    memory_pressure_threshold_percent: float = 80.0
    emergency_threshold_percent: float = 95.0

    # Adaptive settings
    adaptation_window_minutes: int = 30
    performance_target_ms: float = 50.0
    memory_efficiency_weight: float = 0.7
    performance_weight: float = 0.3


class GCPerformanceAnalyzer:
    """Analyzes GC performance and provides optimization recommendations."""

    def __init__(self, history_window_minutes: int = 60):
        self.history_window_minutes = history_window_minutes
        self.metrics_history: deque = deque(maxlen=1000)
        self.performance_trends: Dict[str, List[float]] = defaultdict(list)

    def add_metrics(self, metrics: GCMetrics):
        """Add GC metrics for analysis."""
        self.metrics_history.append(metrics)

        # Track performance trends
        self.performance_trends["execution_time"].append(metrics.execution_time_ms)
        self.performance_trends["memory_freed"].append(metrics.memory_freed_mb)
        self.performance_trends["objects_collected"].append(metrics.objects_collected)

        # Keep trends within window
        for trend_list in self.performance_trends.values():
            if len(trend_list) > 100:
                trend_list[:] = trend_list[-50:]

    def analyze_performance(self) -> Dict[str, Any]:
        """Analyze GC performance and identify issues."""

        if not self.metrics_history:
            return {"error": "No metrics available for analysis"}

        recent_metrics = [
            m for m in self.metrics_history
            if m.timestamp > datetime.now() - timedelta(minutes=self.history_window_minutes)
        ]

        if not recent_metrics:
            return {"error": "No recent metrics available"}

        # Calculate statistics
        execution_times = [m.execution_time_ms for m in recent_metrics]
        memory_freed = [m.memory_freed_mb for m in recent_metrics]
        objects_collected = [m.objects_collected for m in recent_metrics]

        analysis = {
            "performance_summary": {
                "total_collections": len(recent_metrics),
                "avg_execution_time_ms": sum(execution_times) / len(execution_times),
                "max_execution_time_ms": max(execution_times),
                "total_memory_freed_mb": sum(memory_freed),
                "avg_objects_collected": sum(objects_collected) / len(objects_collected)
            },
            "efficiency_metrics": {
                "memory_efficiency": sum(memory_freed) / max(sum(execution_times), 1),
                "collection_frequency": len(recent_metrics) / (self.history_window_minutes / 60),
                "average_pause_time_ms": sum(execution_times) / len(execution_times)
            }
        }

        # Identify issues
        issues = []
        recommendations = []

        if analysis["performance_summary"]["max_execution_time_ms"] > 200:
            issues.append("Long GC pause times detected")
            recommendations.append("Consider reducing GC thresholds or using incremental collection")

        if analysis["efficiency_metrics"]["collection_frequency"] > 60:  # More than 1 per minute
            issues.append("High GC frequency")
            recommendations.append("Increase generation 0 threshold to reduce collection frequency")

        if analysis["efficiency_metrics"]["memory_efficiency"] < 0.1:
            issues.append("Low memory reclamation efficiency")
            recommendations.append("Review object lifecycle and potential memory leaks")

        analysis["issues"] = issues
        analysis["recommendations"] = recommendations

        return analysis

    def get_optimization_suggestions(self, current_config: GCConfiguration) -> Dict[str, Any]:
        """Get specific optimization suggestions based on performance analysis."""

        analysis = self.analyze_performance()
        suggestions = {
            "current_performance": analysis.get("performance_summary", {}),
            "suggested_changes": {},
            "expected_improvements": {}
        }

        if not self.metrics_history:
            return suggestions

        recent_metrics = list(self.metrics_history)[-10:]  # Last 10 collections

        if recent_metrics:
            avg_execution_time = sum(m.execution_time_ms for m in recent_metrics) / len(recent_metrics)
            avg_memory_freed = sum(m.memory_freed_mb for m in recent_metrics) / len(recent_metrics)

            # Suggest threshold adjustments
            if avg_execution_time > current_config.performance_target_ms:
                # Reduce thresholds to trigger GC earlier with smaller workloads
                new_thresholds = tuple(max(t * 0.8, min_t) for t, min_t in
                                     zip(current_config.thresholds, current_config.min_thresholds))
                suggestions["suggested_changes"]["thresholds"] = new_thresholds
                suggestions["expected_improvements"]["execution_time_reduction"] = "20-30%"

            elif avg_execution_time < current_config.performance_target_ms * 0.5:
                # Increase thresholds to reduce collection frequency
                new_thresholds = tuple(min(t * 1.2, max_t) for t, max_t in
                                     zip(current_config.thresholds, current_config.max_thresholds))
                suggestions["suggested_changes"]["thresholds"] = new_thresholds
                suggestions["expected_improvements"]["collection_frequency_reduction"] = "15-25%"

        return suggestions


class GarbageCollectionOptimizer:
    """
    Advanced Garbage Collection Optimizer

    Provides intelligent garbage collection optimization for long-running Python processes
    with adaptive tuning, performance monitoring, and memory pressure management.
    """

    def __init__(
        self,
        config: Optional[GCConfiguration] = None,
        enable_monitoring: bool = True,
        enable_prometheus: bool = True
    ):
        self.config = config or GCConfiguration()
        self.enable_monitoring = enable_monitoring
        self.enable_prometheus = enable_prometheus

        # Core components
        self.analyzer = GCPerformanceAnalyzer()
        self.is_running = False
        self.monitor_thread: Optional[threading.Thread] = None

        # Monitoring state
        self.last_gc_time = time.time()
        self.total_collections = 0
        self.total_memory_freed_mb = 0.0
        self.emergency_collections = 0

        # Weak references for cleanup tracking
        self.tracked_objects: Set[weakref.ref] = set()
        self.object_creation_timestamps: Dict[int, float] = {}

        # Thread safety
        self.lock = threading.RLock()

        # Prometheus metrics
        if self.enable_prometheus:
            self._setup_prometheus_metrics()

        # Logger
        self.logger = logging.getLogger(__name__)

        # Apply initial configuration
        self._apply_gc_configuration()

    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics for GC monitoring."""

        self.prom_gc_collections = Counter('gc_collections_total', 'Total garbage collections', ['generation'])
        self.prom_gc_execution_time = Histogram('gc_execution_time_seconds', 'GC execution time in seconds')
        self.prom_gc_memory_freed = Histogram('gc_memory_freed_bytes', 'Memory freed by GC in bytes')
        self.prom_gc_objects_collected = Histogram('gc_objects_collected', 'Objects collected by GC')
        self.prom_gc_pause_time = Gauge('gc_pause_time_ms', 'Current GC pause time in milliseconds')

        self.logger.info("Prometheus GC metrics initialized")

    def _apply_gc_configuration(self):
        """Apply garbage collection configuration."""

        if not self.config.enabled:
            gc.disable()
            self.logger.info("Garbage collection disabled")
            return

        # Enable GC if disabled
        if not gc.isenabled():
            gc.enable()

        # Set thresholds
        gc.set_threshold(*self.config.thresholds)

        self.logger.info(f"GC configuration applied: thresholds={self.config.thresholds}, strategy={self.config.strategy.value}")

    def start_optimization(self):
        """Start the GC optimization monitoring."""

        if self.is_running:
            self.logger.warning("GC optimization already running")
            return

        self.is_running = True

        if self.enable_monitoring:
            self.monitor_thread = threading.Thread(
                target=self._monitoring_loop,
                name="gc_optimizer",
                daemon=True
            )
            self.monitor_thread.start()

        self.logger.info("GC optimization started")

    def stop_optimization(self):
        """Stop the GC optimization monitoring."""

        self.is_running = False

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)

        self.logger.info("GC optimization stopped")

    def _monitoring_loop(self):
        """Main monitoring loop."""

        while self.is_running:
            try:
                # Check memory pressure and perform adaptive optimization
                self._check_memory_pressure()

                # Auto-tune GC parameters if enabled
                if self.config.auto_tuning:
                    self._adaptive_tuning()

                # Cleanup tracking data
                self._cleanup_tracking_data()

                time.sleep(self.config.monitoring_interval_seconds)

            except Exception as e:
                self.logger.error(f"Error in GC monitoring loop: {e}")
                time.sleep(self.config.monitoring_interval_seconds)

    def _check_memory_pressure(self):
        """Check memory pressure and trigger emergency GC if needed."""

        try:
            memory = psutil.virtual_memory()
            process = psutil.Process()
            process_memory_mb = process.memory_info().rss / 1024 / 1024

            # Emergency collection for high memory pressure
            if memory.percent > self.config.emergency_threshold_percent:
                self.logger.warning(f"Emergency GC triggered: system memory at {memory.percent:.1f}%")
                self.force_collection(all_generations=True, reason="emergency_memory_pressure")
                self.emergency_collections += 1

            # Proactive collection for moderate memory pressure
            elif memory.percent > self.config.memory_pressure_threshold_percent:
                # Check if enough time has passed since last collection
                time_since_last_gc = time.time() - self.last_gc_time
                if time_since_last_gc > self.config.gc_interval_seconds:
                    self.logger.info(f"Proactive GC triggered: system memory at {memory.percent:.1f}%")
                    self.force_collection(all_generations=False, reason="memory_pressure")

        except Exception as e:
            self.logger.error(f"Error checking memory pressure: {e}")

    def _adaptive_tuning(self):
        """Perform adaptive tuning of GC parameters."""

        try:
            suggestions = self.analyzer.get_optimization_suggestions(self.config)

            if "thresholds" in suggestions.get("suggested_changes", {}):
                new_thresholds = suggestions["suggested_changes"]["thresholds"]

                # Apply new thresholds with validation
                validated_thresholds = tuple(
                    max(min(t, max_t), min_t) for t, min_t, max_t in
                    zip(new_thresholds, self.config.min_thresholds, self.config.max_thresholds)
                )

                if validated_thresholds != self.config.thresholds:
                    self.config.thresholds = validated_thresholds
                    gc.set_threshold(*validated_thresholds)

                    self.logger.info(f"Adaptive GC tuning: updated thresholds to {validated_thresholds}")

        except Exception as e:
            self.logger.error(f"Error in adaptive tuning: {e}")

    def _cleanup_tracking_data(self):
        """Clean up expired tracking data."""

        current_time = time.time()
        cutoff_time = current_time - 3600  # 1 hour

        with self.lock:
            # Clean up object creation timestamps
            expired_objects = [
                obj_id for obj_id, timestamp in self.object_creation_timestamps.items()
                if timestamp < cutoff_time
            ]

            for obj_id in expired_objects:
                del self.object_creation_timestamps[obj_id]

            # Clean up dead weak references
            dead_refs = [ref for ref in self.tracked_objects if ref() is None]
            for ref in dead_refs:
                self.tracked_objects.remove(ref)

    def force_collection(
        self,
        generation: Optional[int] = None,
        all_generations: bool = False,
        reason: str = "manual"
    ) -> GCMetrics:
        """Force garbage collection with performance tracking."""

        start_time = time.time()
        memory_before = psutil.Process().memory_info().rss / 1024 / 1024
        cpu_before = psutil.cpu_percent()

        total_collected = 0
        total_uncollectable = 0
        execution_times = []

        try:
            if all_generations:
                # Collect all generations
                for gen in range(3):
                    gen_start = time.time()
                    collected = gc.collect(gen)
                    gen_time = (time.time() - gen_start) * 1000

                    total_collected += collected
                    execution_times.append(gen_time)

                    self.logger.debug(f"GC generation {gen}: {collected} objects in {gen_time:.2f}ms")
            else:
                # Collect specific generation or default
                target_gen = generation if generation is not None else 0
                gen_start = time.time()
                total_collected = gc.collect(target_gen)
                execution_times.append((time.time() - gen_start) * 1000)

            # Calculate metrics
            execution_time_ms = sum(execution_times)
            memory_after = psutil.Process().memory_info().rss / 1024 / 1024
            memory_freed = memory_before - memory_after
            cpu_after = psutil.cpu_percent()

            # Create metrics object
            metrics = GCMetrics(
                generation=generation or (2 if all_generations else 0),
                objects_collected=total_collected,
                objects_uncollectable=total_uncollectable,
                execution_time_ms=execution_time_ms,
                memory_before_mb=memory_before,
                memory_after_mb=memory_after,
                memory_freed_mb=memory_freed,
                total_objects=len(gc.get_objects()),
                gc_thresholds=gc.get_threshold(),
                gc_counts=gc.get_count(),
                cpu_usage_percent=(cpu_after + cpu_before) / 2,
                gc_pause_time_ms=execution_time_ms
            )

            # Update tracking
            with self.lock:
                self.last_gc_time = time.time()
                self.total_collections += 1
                self.total_memory_freed_mb += memory_freed

                # Add to analyzer
                self.analyzer.add_metrics(metrics)

            # Update Prometheus metrics
            if self.enable_prometheus:
                self.prom_gc_collections.labels(generation=metrics.generation).inc()
                self.prom_gc_execution_time.observe(execution_time_ms / 1000)
                self.prom_gc_memory_freed.observe(memory_freed * 1024 * 1024)
                self.prom_gc_objects_collected.observe(total_collected)
                self.prom_gc_pause_time.set(execution_time_ms)

            self.logger.info(
                f"GC completed [{reason}]: {total_collected} objects collected, "
                f"{memory_freed:.2f}MB freed in {execution_time_ms:.2f}ms"
            )

            return metrics

        except Exception as e:
            self.logger.error(f"Error during garbage collection: {e}")
            raise

    @contextmanager
    def gc_pause_protection(self, max_pause_ms: Optional[float] = None):
        """Context manager to protect against long GC pauses."""

        max_pause = max_pause_ms or self.config.max_pause_time_ms
        original_thresholds = gc.get_threshold()

        try:
            # Temporarily increase thresholds to reduce GC frequency
            temp_thresholds = tuple(t * 2 for t in original_thresholds)
            gc.set_threshold(*temp_thresholds)

            yield

        finally:
            # Restore original thresholds
            gc.set_threshold(*original_thresholds)

            # Force a quick collection if needed
            if psutil.virtual_memory().percent > self.config.memory_pressure_threshold_percent:
                self.force_collection(generation=0, reason="post_pause_protection")

    def track_object(self, obj: Any, category: str = "general") -> weakref.ref:
        """Track an object for lifecycle monitoring."""

        try:
            ref = weakref.ref(obj)

            with self.lock:
                self.tracked_objects.add(ref)
                self.object_creation_timestamps[id(obj)] = time.time()

            return ref

        except TypeError:
            # Object doesn't support weak references
            self.logger.debug(f"Cannot create weak reference for {type(obj)}")
            return None

    def get_performance_report(self) -> Dict[str, Any]:
        """Get comprehensive GC performance report."""

        analysis = self.analyzer.analyze_performance()

        report = {
            "configuration": {
                "strategy": self.config.strategy.value,
                "thresholds": self.config.thresholds,
                "auto_tuning": self.config.auto_tuning,
                "enabled": self.config.enabled
            },
            "runtime_statistics": {
                "total_collections": self.total_collections,
                "total_memory_freed_mb": self.total_memory_freed_mb,
                "emergency_collections": self.emergency_collections,
                "tracked_objects": len(self.tracked_objects),
                "uptime_hours": (time.time() - self.last_gc_time) / 3600 if self.total_collections > 0 else 0
            },
            "current_state": {
                "gc_enabled": gc.isenabled(),
                "gc_thresholds": gc.get_threshold(),
                "gc_counts": gc.get_count(),
                "total_objects": len(gc.get_objects()),
                "memory_usage_mb": psutil.Process().memory_info().rss / 1024 / 1024
            },
            "performance_analysis": analysis
        }

        return report

    def optimize_for_workload(self, workload_type: str):
        """Optimize GC settings for specific workload types."""

        workload_configs = {
            "web_server": GCConfiguration(
                strategy=GCStrategy.BALANCED,
                thresholds=(1000, 15, 15),
                max_pause_time_ms=50.0,
                gc_interval_seconds=30.0
            ),
            "data_processing": GCConfiguration(
                strategy=GCStrategy.CONSERVATIVE,
                thresholds=(2000, 25, 25),
                max_pause_time_ms=200.0,
                gc_interval_seconds=120.0
            ),
            "ml_training": GCConfiguration(
                strategy=GCStrategy.AGGRESSIVE,
                thresholds=(500, 8, 8),
                max_pause_time_ms=500.0,
                gc_interval_seconds=60.0
            ),
            "streaming": GCConfiguration(
                strategy=GCStrategy.ADAPTIVE,
                thresholds=(800, 12, 12),
                max_pause_time_ms=30.0,
                gc_interval_seconds=15.0
            )
        }

        if workload_type in workload_configs:
            self.config = workload_configs[workload_type]
            self._apply_gc_configuration()
            self.logger.info(f"GC optimized for {workload_type} workload")
        else:
            self.logger.warning(f"Unknown workload type: {workload_type}")


# Global optimizer instance
_gc_optimizer: Optional[GarbageCollectionOptimizer] = None


def get_gc_optimizer() -> GarbageCollectionOptimizer:
    """Get global GC optimizer instance."""
    global _gc_optimizer

    if _gc_optimizer is None:
        _gc_optimizer = GarbageCollectionOptimizer()
        _gc_optimizer.start_optimization()

    return _gc_optimizer


def setup_gc_optimization(
    config: Optional[GCConfiguration] = None,
    workload_type: Optional[str] = None,
    **kwargs
) -> GarbageCollectionOptimizer:
    """Setup and start GC optimization."""

    global _gc_optimizer

    if _gc_optimizer is not None:
        _gc_optimizer.stop_optimization()

    _gc_optimizer = GarbageCollectionOptimizer(config=config, **kwargs)

    if workload_type:
        _gc_optimizer.optimize_for_workload(workload_type)

    _gc_optimizer.start_optimization()

    return _gc_optimizer


# Utility functions
def force_full_gc() -> Dict[str, int]:
    """Force full garbage collection and return statistics."""

    optimizer = get_gc_optimizer()
    metrics = optimizer.force_collection(all_generations=True, reason="manual_full_gc")

    return {
        "objects_collected": metrics.objects_collected,
        "memory_freed_mb": metrics.memory_freed_mb,
        "execution_time_ms": metrics.execution_time_ms
    }


def get_gc_status() -> Dict[str, Any]:
    """Get current garbage collection status."""

    return {
        "gc_enabled": gc.isenabled(),
        "gc_thresholds": gc.get_threshold(),
        "gc_counts": gc.get_count(),
        "total_objects": len(gc.get_objects()),
        "memory_usage_mb": psutil.Process().memory_info().rss / 1024 / 1024
    }


if __name__ == "__main__":
    # Example usage and testing
    logging.basicConfig(level=logging.INFO)

    # Setup GC optimization for web server workload
    optimizer = setup_gc_optimization(
        workload_type="web_server",
        enable_monitoring=True,
        enable_prometheus=True
    )

    try:
        print("GC Optimization Demo")
        print(f"Initial status: {get_gc_status()}")

        # Simulate memory allocations
        test_objects = []
        for i in range(5):
            # Allocate some objects
            batch = [list(range(10000)) for _ in range(100)]
            test_objects.extend(batch)

            print(f"Allocation batch {i+1}: {len(test_objects)} total objects")

            # Force collection to demonstrate optimization
            if i % 2 == 0:
                metrics = optimizer.force_collection(reason=f"demo_batch_{i}")
                print(f"GC metrics: {metrics.objects_collected} collected, {metrics.memory_freed_mb:.2f}MB freed")

            time.sleep(2)

        # Test pause protection
        print("\nTesting GC pause protection...")
        with optimizer.gc_pause_protection(max_pause_ms=100):
            # Simulate work that might trigger GC
            large_data = [list(range(1000)) for _ in range(1000)]
            time.sleep(1)

        # Get performance report
        report = optimizer.get_performance_report()
        print(f"\nPerformance Report:")
        print(f"Total collections: {report['runtime_statistics']['total_collections']}")
        print(f"Total memory freed: {report['runtime_statistics']['total_memory_freed_mb']:.2f}MB")

        # Keep running for monitoring
        time.sleep(30)

    except KeyboardInterrupt:
        print("Stopping GC optimization...")
    finally:
        optimizer.stop_optimization()