"""
DataDog Profiling Integration for Performance Optimization
Provides comprehensive performance profiling and code execution tracking with DataDog
"""

import asyncio
import functools
import gc
import os
import psutil
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union
from pathlib import Path

# DataDog profiling imports
try:
    import ddtrace.profiling.auto  # Auto-instrument profiling
    from ddtrace.profiling import Profiler
    from ddtrace.profiling.collector import StackCollector, MemoryCollector
    from ddtrace.profiling.exporter import ConsoleExporter, AgentExporter
    from ddtrace.profiling.recorder import Recorder
    PROFILING_AVAILABLE = True
except ImportError:
    PROFILING_AVAILABLE = False

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_apm_middleware import add_custom_tags, add_custom_metric

logger = get_logger(__name__)


@dataclass
class ProfilingMetrics:
    """Performance profiling metrics."""
    cpu_time: float = 0.0
    memory_usage_mb: float = 0.0
    memory_peak_mb: float = 0.0
    gc_collections: int = 0
    thread_count: int = 0
    execution_time: float = 0.0
    function_calls: int = 0
    exceptions_count: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PerformanceProfile:
    """Comprehensive performance profile."""
    operation_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    cpu_usage: Dict[str, float]
    memory_usage: Dict[str, float]
    io_operations: Dict[str, int]
    network_operations: Dict[str, int]
    database_operations: Dict[str, int]
    custom_metrics: Dict[str, Any]
    stack_traces: List[str] = field(default_factory=list)
    hotspots: List[Dict[str, Any]] = field(default_factory=list)


class DataDogProfiler:
    """
    Comprehensive DataDog profiling integration
    
    Features:
    - CPU profiling with hotspot identification
    - Memory profiling and leak detection
    - I/O operation profiling
    - Database query profiling
    - Custom function profiling
    - Performance bottleneck analysis
    - Real-time performance monitoring
    - Profile export and analysis
    """
    
    def __init__(self, service_name: str = "profiling-service", 
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Profiling state
        self.profiler: Optional[Profiler] = None
        self.is_profiling_enabled = PROFILING_AVAILABLE
        self.is_active = False
        
        # Performance tracking
        self.profiles: List[PerformanceProfile] = []
        self.active_operations: Dict[str, Dict[str, Any]] = {}
        self.system_metrics: Dict[str, List[float]] = {
            "cpu_percent": [],
            "memory_percent": [],
            "memory_mb": [],
            "threads": [],
            "file_handles": []
        }
        
        # Performance baselines
        self.baselines: Dict[str, Dict[str, float]] = {}
        
        # Profiling configuration
        self.config = {
            "sample_rate": 0.01,  # 1% sampling
            "max_time_usage_pct": 2,  # Max 2% CPU overhead
            "upload_period": 60,  # Upload every 60 seconds
            "max_frames": 64,  # Max stack frames
            "memory_events": True,
            "exceptions_events": True,
            "lock_events": True
        }
        
        if self.is_profiling_enabled:
            self._initialize_profiler()
        else:
            self.logger.warning("DataDog profiling not available - install ddtrace with profiling support")
    
    def _initialize_profiler(self):
        """Initialize DataDog profiler."""
        
        if not self.is_profiling_enabled:
            return
        
        try:
            # Create profiler with custom configuration
            self.profiler = Profiler(
                services=[self.service_name],
                env=os.getenv("DD_ENV", "development"),
                version=os.getenv("DD_VERSION", "1.0.0"),
                url=f"http://{os.getenv('DD_AGENT_HOST', 'localhost')}:{os.getenv('DD_TRACE_AGENT_PORT', 8126)}"
            )
            
            # Configure profiler settings
            if hasattr(self.profiler, '_recorder') and self.profiler._recorder:
                recorder = self.profiler._recorder
                
                # Configure stack collector
                for collector in recorder.collectors:
                    if isinstance(collector, StackCollector):
                        collector.max_nframes = self.config["max_frames"]
                    elif isinstance(collector, MemoryCollector):
                        collector.capture_pct = self.config["sample_rate"] * 100
            
            self.logger.info("DataDog profiler initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize DataDog profiler: {str(e)}")
            self.is_profiling_enabled = False
    
    def start_profiling(self) -> bool:
        """Start DataDog profiling."""
        
        if not self.is_profiling_enabled or not self.profiler:
            self.logger.warning("Profiling not available")
            return False
        
        if self.is_active:
            self.logger.info("Profiling already active")
            return True
        
        try:
            self.profiler.start()
            self.is_active = True
            
            # Start system metrics collection
            self._start_system_metrics_collection()
            
            self.logger.info("DataDog profiling started")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start profiling: {str(e)}")
            return False
    
    def stop_profiling(self) -> bool:
        """Stop DataDog profiling."""
        
        if not self.is_active or not self.profiler:
            return False
        
        try:
            self.profiler.stop(flush=True)
            self.is_active = False
            self.logger.info("DataDog profiling stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop profiling: {str(e)}")
            return False
    
    def _start_system_metrics_collection(self):
        """Start background system metrics collection."""
        
        def collect_metrics():
            while self.is_active:
                try:
                    process = psutil.Process()
                    
                    # CPU metrics
                    cpu_percent = process.cpu_percent()
                    self.system_metrics["cpu_percent"].append(cpu_percent)
                    
                    # Memory metrics
                    memory_info = process.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)
                    memory_percent = process.memory_percent()
                    
                    self.system_metrics["memory_mb"].append(memory_mb)
                    self.system_metrics["memory_percent"].append(memory_percent)
                    
                    # Thread metrics
                    thread_count = process.num_threads()
                    self.system_metrics["threads"].append(thread_count)
                    
                    # File handles
                    try:
                        file_handles = process.num_fds() if hasattr(process, 'num_fds') else 0
                        self.system_metrics["file_handles"].append(file_handles)
                    except:
                        pass
                    
                    # Keep only last 1000 measurements
                    for metric_list in self.system_metrics.values():
                        if len(metric_list) > 1000:
                            metric_list[:] = metric_list[-1000:]
                    
                    # Send to DataDog if monitoring is available
                    if self.datadog_monitoring:
                        tags = [f"service:{self.service_name}"]
                        self.datadog_monitoring.gauge("profiling.system.cpu_percent", cpu_percent, tags=tags)
                        self.datadog_monitoring.gauge("profiling.system.memory_mb", memory_mb, tags=tags)
                        self.datadog_monitoring.gauge("profiling.system.memory_percent", memory_percent, tags=tags)
                        self.datadog_monitoring.gauge("profiling.system.threads", thread_count, tags=tags)
                    
                except Exception as e:
                    self.logger.warning(f"Error collecting system metrics: {str(e)}")
                
                time.sleep(10)  # Collect every 10 seconds
        
        # Start metrics collection in background thread
        metrics_thread = threading.Thread(target=collect_metrics, daemon=True)
        metrics_thread.start()
    
    # Function Profiling Decorators
    
    def profile_function(self, operation_name: Optional[str] = None, 
                        track_memory: bool = True, track_io: bool = False):
        """Decorator for profiling functions."""
        
        def decorator(func: Callable) -> Callable:
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                async with self.profile_operation(op_name, track_memory, track_io):
                    return await func(*args, **kwargs)
            
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                with self.profile_operation(op_name, track_memory, track_io):
                    return func(*args, **kwargs)
            
            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        
        return decorator
    
    @contextmanager
    def profile_operation(self, operation_name: str, track_memory: bool = True, 
                         track_io: bool = False):
        """Context manager for profiling operations."""
        
        operation_id = f"{operation_name}_{int(time.time())}"
        start_time = datetime.utcnow()
        
        # Initial metrics
        process = psutil.Process()
        initial_metrics = {
            "cpu_times": process.cpu_times(),
            "memory_info": process.memory_info(),
            "io_counters": process.io_counters() if track_io else None,
            "num_threads": process.num_threads(),
            "gc_counts": gc.get_counts()
        }
        
        # Store active operation
        self.active_operations[operation_id] = {
            "operation_name": operation_name,
            "start_time": start_time,
            "initial_metrics": initial_metrics
        }
        
        # Add to current span if available
        add_custom_tags({
            "profiling.operation": operation_name,
            "profiling.start_time": start_time.isoformat(),
            "profiling.tracking_memory": track_memory,
            "profiling.tracking_io": track_io
        })
        
        try:
            yield
            
        except Exception as e:
            # Track exception in profiling
            add_custom_tags({
                "profiling.exception": type(e).__name__,
                "profiling.error": str(e)
            })
            raise
            
        finally:
            # Calculate final metrics
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            final_metrics = {
                "cpu_times": process.cpu_times(),
                "memory_info": process.memory_info(),
                "io_counters": process.io_counters() if track_io else None,
                "num_threads": process.num_threads(),
                "gc_counts": gc.get_counts()
            }
            
            # Create performance profile
            profile = self._create_performance_profile(
                operation_name, start_time, end_time, duration,
                initial_metrics, final_metrics
            )
            
            # Store profile
            self.profiles.append(profile)
            
            # Keep only last 1000 profiles
            if len(self.profiles) > 1000:
                self.profiles = self.profiles[-1000:]
            
            # Clean up active operation
            if operation_id in self.active_operations:
                del self.active_operations[operation_id]
            
            # Add profiling metrics to span
            add_custom_metric("profiling.operation.duration", duration * 1000)
            add_custom_metric("profiling.operation.memory_delta", 
                            profile.memory_usage.get("memory_delta_mb", 0))
            
            # Send to DataDog
            if self.datadog_monitoring:
                await self._send_profile_metrics(profile)
    
    def _create_performance_profile(self, operation_name: str, start_time: datetime,
                                  end_time: datetime, duration: float,
                                  initial_metrics: Dict, final_metrics: Dict) -> PerformanceProfile:
        """Create a comprehensive performance profile."""
        
        # Calculate CPU usage
        cpu_initial = initial_metrics["cpu_times"]
        cpu_final = final_metrics["cpu_times"]
        cpu_usage = {
            "user_time": cpu_final.user - cpu_initial.user,
            "system_time": cpu_final.system - cpu_initial.system,
            "total_time": (cpu_final.user + cpu_final.system) - (cpu_initial.user + cpu_initial.system)
        }
        
        # Calculate memory usage
        mem_initial = initial_metrics["memory_info"]
        mem_final = final_metrics["memory_info"]
        memory_usage = {
            "initial_mb": mem_initial.rss / (1024 * 1024),
            "final_mb": mem_final.rss / (1024 * 1024),
            "memory_delta_mb": (mem_final.rss - mem_initial.rss) / (1024 * 1024),
            "peak_mb": mem_final.rss / (1024 * 1024)  # Approximate
        }
        
        # Calculate I/O operations
        io_operations = {}
        if initial_metrics["io_counters"] and final_metrics["io_counters"]:
            io_initial = initial_metrics["io_counters"]
            io_final = final_metrics["io_counters"]
            io_operations = {
                "read_count": io_final.read_count - io_initial.read_count,
                "write_count": io_final.write_count - io_initial.write_count,
                "read_bytes": io_final.read_bytes - io_initial.read_bytes,
                "write_bytes": io_final.write_bytes - io_initial.write_bytes
            }
        
        # Calculate GC statistics
        gc_initial = initial_metrics["gc_counts"]
        gc_final = final_metrics["gc_counts"]
        gc_operations = {
            "gen0_collections": gc_final[0] - gc_initial[0],
            "gen1_collections": gc_final[1] - gc_initial[1],
            "gen2_collections": gc_final[2] - gc_initial[2]
        }
        
        # Thread information
        thread_info = {
            "initial_threads": initial_metrics["num_threads"],
            "final_threads": final_metrics["num_threads"],
            "thread_delta": final_metrics["num_threads"] - initial_metrics["num_threads"]
        }
        
        return PerformanceProfile(
            operation_name=operation_name,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            io_operations=io_operations,
            network_operations={},  # Would need additional tracking
            database_operations={},  # Would need additional tracking
            custom_metrics={
                "gc_operations": gc_operations,
                "thread_info": thread_info
            }
        )
    
    async def _send_profile_metrics(self, profile: PerformanceProfile):
        """Send profile metrics to DataDog."""
        
        try:
            tags = [
                f"service:{self.service_name}",
                f"operation:{profile.operation_name}"
            ]
            
            # Duration metrics
            self.datadog_monitoring.histogram(
                "profiling.operation.duration",
                profile.duration_seconds * 1000,  # Convert to milliseconds
                tags=tags
            )
            
            # CPU metrics
            if profile.cpu_usage:
                self.datadog_monitoring.histogram(
                    "profiling.operation.cpu_time",
                    profile.cpu_usage.get("total_time", 0) * 1000,
                    tags=tags
                )
            
            # Memory metrics
            if profile.memory_usage:
                self.datadog_monitoring.histogram(
                    "profiling.operation.memory_delta",
                    profile.memory_usage.get("memory_delta_mb", 0),
                    tags=tags
                )
                
                self.datadog_monitoring.gauge(
                    "profiling.operation.memory_peak",
                    profile.memory_usage.get("peak_mb", 0),
                    tags=tags
                )
            
            # I/O metrics
            if profile.io_operations:
                self.datadog_monitoring.histogram(
                    "profiling.operation.io_reads",
                    profile.io_operations.get("read_count", 0),
                    tags=tags
                )
                
                self.datadog_monitoring.histogram(
                    "profiling.operation.io_writes",
                    profile.io_operations.get("write_count", 0),
                    tags=tags
                )
            
            # GC metrics
            gc_ops = profile.custom_metrics.get("gc_operations", {})
            total_gc = sum(gc_ops.values()) if gc_ops else 0
            if total_gc > 0:
                self.datadog_monitoring.histogram(
                    "profiling.operation.gc_collections",
                    total_gc,
                    tags=tags
                )
            
        except Exception as e:
            self.logger.warning(f"Failed to send profile metrics: {str(e)}")
    
    # Performance Analysis
    
    def analyze_performance(self, operation_name: Optional[str] = None,
                          time_window_minutes: int = 60) -> Dict[str, Any]:
        """Analyze performance profiles for bottlenecks and trends."""
        
        cutoff_time = datetime.utcnow() - timedelta(minutes=time_window_minutes)
        
        # Filter profiles
        if operation_name:
            profiles = [p for p in self.profiles 
                       if p.operation_name == operation_name and p.start_time >= cutoff_time]
        else:
            profiles = [p for p in self.profiles if p.start_time >= cutoff_time]
        
        if not profiles:
            return {"error": "No profiles found for analysis"}
        
        # Calculate statistics
        durations = [p.duration_seconds for p in profiles]
        memory_deltas = [p.memory_usage.get("memory_delta_mb", 0) for p in profiles]
        cpu_times = [p.cpu_usage.get("total_time", 0) for p in profiles]
        
        analysis = {
            "time_window_minutes": time_window_minutes,
            "operation_name": operation_name,
            "total_operations": len(profiles),
            "duration_stats": {
                "min_seconds": min(durations),
                "max_seconds": max(durations),
                "avg_seconds": sum(durations) / len(durations),
                "p95_seconds": self._calculate_percentile(durations, 95),
                "p99_seconds": self._calculate_percentile(durations, 99)
            },
            "memory_stats": {
                "min_delta_mb": min(memory_deltas),
                "max_delta_mb": max(memory_deltas),
                "avg_delta_mb": sum(memory_deltas) / len(memory_deltas),
                "total_allocated_mb": sum([d for d in memory_deltas if d > 0])
            },
            "cpu_stats": {
                "min_cpu_time": min(cpu_times),
                "max_cpu_time": max(cpu_times),
                "avg_cpu_time": sum(cpu_times) / len(cpu_times),
                "total_cpu_time": sum(cpu_times)
            }
        }
        
        # Identify performance issues
        issues = []
        avg_duration = analysis["duration_stats"]["avg_seconds"]
        p95_duration = analysis["duration_stats"]["p95_seconds"]
        
        if p95_duration > avg_duration * 3:
            issues.append("High duration variance - investigate outliers")
        
        if analysis["memory_stats"]["avg_delta_mb"] > 100:
            issues.append("High memory usage - potential memory leaks")
        
        if analysis["cpu_stats"]["avg_cpu_time"] > avg_duration * 0.8:
            issues.append("High CPU usage - potential CPU-bound operations")
        
        analysis["performance_issues"] = issues
        
        # Performance trends
        recent_profiles = profiles[-10:] if len(profiles) >= 10 else profiles
        older_profiles = profiles[:-10] if len(profiles) >= 20 else []
        
        if older_profiles and recent_profiles:
            recent_avg_duration = sum(p.duration_seconds for p in recent_profiles) / len(recent_profiles)
            older_avg_duration = sum(p.duration_seconds for p in older_profiles) / len(older_profiles)
            
            if recent_avg_duration > older_avg_duration * 1.2:
                analysis["trend"] = "degrading"
            elif recent_avg_duration < older_avg_duration * 0.8:
                analysis["trend"] = "improving"
            else:
                analysis["trend"] = "stable"
        else:
            analysis["trend"] = "insufficient_data"
        
        return analysis
    
    def _calculate_percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile value."""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        index = int((percentile / 100) * len(sorted_values))
        index = min(index, len(sorted_values) - 1)
        return sorted_values[index]
    
    def identify_hotspots(self, operation_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Identify performance hotspots."""
        
        # Filter profiles
        if operation_name:
            profiles = [p for p in self.profiles if p.operation_name == operation_name]
        else:
            profiles = self.profiles
        
        # Group by operation
        operation_stats = {}
        for profile in profiles:
            if profile.operation_name not in operation_stats:
                operation_stats[profile.operation_name] = {
                    "total_time": 0.0,
                    "total_memory": 0.0,
                    "count": 0,
                    "max_duration": 0.0
                }
            
            stats = operation_stats[profile.operation_name]
            stats["total_time"] += profile.duration_seconds
            stats["total_memory"] += profile.memory_usage.get("memory_delta_mb", 0)
            stats["count"] += 1
            stats["max_duration"] = max(stats["max_duration"], profile.duration_seconds)
        
        # Calculate hotspots
        hotspots = []
        for op_name, stats in operation_stats.items():
            avg_duration = stats["total_time"] / stats["count"]
            total_impact = stats["total_time"]  # Total time spent in this operation
            
            hotspots.append({
                "operation_name": op_name,
                "avg_duration_seconds": avg_duration,
                "max_duration_seconds": stats["max_duration"],
                "total_time_seconds": total_impact,
                "call_count": stats["count"],
                "avg_memory_delta_mb": stats["total_memory"] / stats["count"],
                "impact_score": total_impact * stats["count"]  # Time * frequency
            })
        
        # Sort by impact score
        hotspots.sort(key=lambda x: x["impact_score"], reverse=True)
        
        return hotspots
    
    def get_system_health_summary(self) -> Dict[str, Any]:
        """Get system health summary based on profiling data."""
        
        current_time = datetime.utcnow()
        
        # System metrics summary
        system_summary = {}
        for metric_name, values in self.system_metrics.items():
            if values:
                recent_values = values[-60:]  # Last 60 measurements (10 minutes)
                system_summary[metric_name] = {
                    "current": values[-1],
                    "avg_10min": sum(recent_values) / len(recent_values),
                    "max_10min": max(recent_values),
                    "min_10min": min(recent_values)
                }
        
        # Recent performance analysis
        recent_profiles = [p for p in self.profiles 
                         if (current_time - p.start_time).total_seconds() < 3600]  # Last hour
        
        performance_summary = {}
        if recent_profiles:
            total_operations = len(recent_profiles)
            avg_duration = sum(p.duration_seconds for p in recent_profiles) / total_operations
            total_memory_allocated = sum(p.memory_usage.get("memory_delta_mb", 0) 
                                       for p in recent_profiles if p.memory_usage.get("memory_delta_mb", 0) > 0)
            
            performance_summary = {
                "total_operations_1h": total_operations,
                "avg_operation_duration": avg_duration,
                "total_memory_allocated_mb": total_memory_allocated,
                "operations_per_minute": total_operations / 60
            }
        
        # Health indicators
        health_indicators = {
            "profiling_active": self.is_active,
            "profiles_collected": len(self.profiles),
            "active_operations": len(self.active_operations),
            "system_metrics_available": bool(self.system_metrics["cpu_percent"])
        }
        
        return {
            "timestamp": current_time.isoformat(),
            "service_name": self.service_name,
            "system_metrics": system_summary,
            "performance_metrics": performance_summary,
            "health_indicators": health_indicators,
            "profiling_config": self.config
        }


# Global profiler instance
_datadog_profiler: Optional[DataDogProfiler] = None


def get_datadog_profiler(service_name: str = "profiling-service",
                        datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogProfiler:
    """Get or create DataDog profiler."""
    global _datadog_profiler
    
    if _datadog_profiler is None:
        _datadog_profiler = DataDogProfiler(service_name, datadog_monitoring)
    
    return _datadog_profiler


# Convenience functions and decorators

def profile_function(operation_name: Optional[str] = None, track_memory: bool = True, track_io: bool = False):
    """Convenience decorator for profiling functions."""
    profiler = get_datadog_profiler()
    return profiler.profile_function(operation_name, track_memory, track_io)


@contextmanager
def profile_operation(operation_name: str, track_memory: bool = True, track_io: bool = False):
    """Convenience context manager for profiling operations."""
    profiler = get_datadog_profiler()
    with profiler.profile_operation(operation_name, track_memory, track_io):
        yield


def start_profiling() -> bool:
    """Start DataDog profiling."""
    profiler = get_datadog_profiler()
    return profiler.start_profiling()


def stop_profiling() -> bool:
    """Stop DataDog profiling."""
    profiler = get_datadog_profiler()
    return profiler.stop_profiling()


def analyze_performance(operation_name: Optional[str] = None, time_window_minutes: int = 60) -> Dict[str, Any]:
    """Analyze performance for bottlenecks and trends."""
    profiler = get_datadog_profiler()
    return profiler.analyze_performance(operation_name, time_window_minutes)


def get_performance_hotspots(operation_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get performance hotspots."""
    profiler = get_datadog_profiler()
    return profiler.identify_hotspots(operation_name)


def get_system_health() -> Dict[str, Any]:
    """Get system health summary."""
    profiler = get_datadog_profiler()
    return profiler.get_system_health_summary()


# Performance optimization recommendations
class PerformanceOptimizer:
    """Performance optimization recommendations based on profiling data."""
    
    @staticmethod
    def generate_recommendations(analysis: Dict[str, Any]) -> List[str]:
        """Generate performance optimization recommendations."""
        
        recommendations = []
        
        if "duration_stats" in analysis:
            duration_stats = analysis["duration_stats"]
            
            # High duration variance
            if duration_stats["p99_seconds"] > duration_stats["avg_seconds"] * 5:
                recommendations.append(
                    "High duration variance detected. Consider implementing caching or "
                    "optimizing the slowest operations."
                )
            
            # Slow average performance
            if duration_stats["avg_seconds"] > 1.0:
                recommendations.append(
                    "Average operation duration is high. Consider parallel processing "
                    "or algorithm optimization."
                )
        
        if "memory_stats" in analysis:
            memory_stats = analysis["memory_stats"]
            
            # High memory usage
            if memory_stats["avg_delta_mb"] > 50:
                recommendations.append(
                    "High memory usage per operation. Consider memory pooling or "
                    "streaming processing for large datasets."
                )
            
            # Memory growth
            if memory_stats["total_allocated_mb"] > 1000:
                recommendations.append(
                    "High total memory allocation. Monitor for memory leaks and "
                    "implement garbage collection optimization."
                )
        
        if "cpu_stats" in analysis:
            cpu_stats = analysis["cpu_stats"]
            
            # High CPU usage
            if "duration_stats" in analysis:
                cpu_ratio = cpu_stats["avg_cpu_time"] / analysis["duration_stats"]["avg_seconds"]
                if cpu_ratio > 0.8:
                    recommendations.append(
                        "High CPU usage relative to wall time. Consider CPU optimization "
                        "or offloading to background processes."
                    )
        
        # Trend-based recommendations
        if analysis.get("trend") == "degrading":
            recommendations.append(
                "Performance is degrading over time. Investigate recent changes and "
                "consider performance regression testing."
            )
        
        if not recommendations:
            recommendations.append("Performance appears optimal. Continue monitoring.")
        
        return recommendations