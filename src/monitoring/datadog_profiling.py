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
from monitoring.enterprise_prometheus_metrics import enterprise_metrics

# Advanced APM imports for comprehensive observability
try:
    from ddtrace import tracer, patch_all, config
    from ddtrace.contrib.asyncio import patch as patch_asyncio
    from ddtrace.contrib.redis import patch as patch_redis
    from ddtrace.contrib.psycopg import patch as patch_postgres
    from ddtrace.contrib.sqlalchemy import patch as patch_sqlalchemy
    from ddtrace.contrib.kafka import patch as patch_kafka
    from ddtrace.contrib.elasticsearch import patch as patch_elasticsearch
    from ddtrace.contrib.requests import patch as patch_requests
    APM_TRACING_AVAILABLE = True
except ImportError:
    APM_TRACING_AVAILABLE = False

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
        
        # Business metrics tracking with BMAD story correlation
        self.business_metrics: Dict[str, Any] = {
            "api_sla_violations": 0,
            "etl_processing_delays": 0,
            "data_quality_score": 100.0,
            "business_value_at_risk": 0.0,
            "revenue_impact": 0.0,
            "bmad_story_health": {
                "story_1_bi_dashboards": 100.0,
                "story_2_data_quality": 100.0,
                "story_3_security_governance": 100.0,
                "story_4_api_performance": 100.0,
                "story_4_1_mobile_analytics": 100.0,
                "story_4_2_ai_analytics": 100.0,
                "story_6_advanced_security": 100.0,
                "story_7_streaming": 100.0,
                "story_8_ml_operations": 100.0
            },
            "platform_roi_percentage": 100.0,
            "critical_alerts_last_hour": 0,
            "sla_compliance_rate": 100.0,
            "infrastructure_efficiency": 100.0
        }
        
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
        
        # Initialize advanced APM tracing
        self._initialize_apm_tracing()
        
        if self.is_profiling_enabled:
            self._initialize_profiler()
        else:
            self.logger.warning("DataDog profiling not available - install ddtrace with profiling support")
            
        # Initialize comprehensive monitoring collectors
        self._initialize_monitoring_collectors()
    
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
            
    def _initialize_apm_tracing(self):
        """Initialize advanced APM tracing with comprehensive integration."""
        
        if not APM_TRACING_AVAILABLE:
            self.logger.warning("DataDog APM tracing not available")
            return
            
        try:
            # Configure tracer with enhanced settings
            config.trace_headers_compat = True
            config.health_metrics_enabled = True
            config.runtime_metrics_enabled = True
            
            # Set service information
            tracer.configure(
                hostname=os.getenv('DD_AGENT_HOST', 'localhost'),
                port=int(os.getenv('DD_TRACE_AGENT_PORT', 8126)),
                https=False,
                priority_sampling=True,
                analytics_enabled=True,
                analytics_sample_rate=0.1,  # 10% sampling for analytics
            )
            
            # Patch integrations for comprehensive tracing
            patch_asyncio()
            patch_redis()
            patch_postgres()
            patch_sqlalchemy()
            patch_kafka()
            patch_elasticsearch()
            patch_requests()
            
            # Enable automatic instrumentation
            patch_all()
            
            self.logger.info("DataDog APM tracing initialized with comprehensive integrations")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize APM tracing: {str(e)}")
            
    def _initialize_monitoring_collectors(self):
        """Initialize comprehensive monitoring collectors."""
        
        # Advanced system monitoring
        self.system_metrics.update({
            "database_connections": [],
            "cache_hit_rates": [],
            "api_error_rates": [],
            "etl_success_rates": [],
            "security_alerts": [],
            "business_kpi_scores": [],
            "infrastructure_costs": [],
            "user_experience_scores": [],
            "compliance_scores": [],
            "ml_model_accuracy": []
        })
        
        # BMAD story metrics initialization
        self._initialize_bmad_story_metrics()
        
    def _initialize_bmad_story_metrics(self):
        """Initialize BMAD story-specific metrics tracking."""
        
        self.bmad_story_metrics = {
            "story_1_bi_dashboards": {
                "dashboard_load_times": [],
                "user_engagement": [],
                "data_freshness": [],
                "business_value": 2.5  # $2.5M
            },
            "story_2_data_quality": {
                "quality_scores": [],
                "validation_failures": [],
                "data_completeness": [],
                "business_value": 1.8  # $1.8M
            },
            "story_3_security_governance": {
                "compliance_scores": [],
                "security_incidents": [],
                "audit_results": [],
                "business_value": 3.2  # $3.2M
            },
            "story_4_api_performance": {
                "response_times": [],
                "sla_violations": [],
                "throughput": [],
                "business_value": 2.1  # $2.1M
            },
            "story_4_1_mobile_analytics": {
                "app_launch_times": [],
                "crash_rates": [],
                "user_engagement": [],
                "business_value": 2.8  # $2.8M
            },
            "story_4_2_ai_analytics": {
                "model_accuracy": [],
                "inference_latency": [],
                "drift_scores": [],
                "business_value": 3.5  # $3.5M
            },
            "story_6_advanced_security": {
                "threat_detection_rate": [],
                "incident_response_time": [],
                "vulnerability_scores": [],
                "business_value": 4.2  # $4.2M
            },
            "story_7_streaming": {
                "streaming_latency": [],
                "throughput_rates": [],
                "data_loss_rate": [],
                "business_value": 3.4  # $3.4M
            },
            "story_8_ml_operations": {
                "model_deployment_time": [],
                "training_accuracy": [],
                "prediction_latency": [],
                "business_value": 6.4  # $6.4M
            }
        }
    
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
            
            # Start business metrics collection
            self._start_business_metrics_collection()
            
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
                    
                    # Update Enterprise Prometheus metrics
                    enterprise_metrics.system_cpu_usage.labels(
                        core="all",
                        instance=self.service_name
                    ).set(cpu_percent)
                    
                    enterprise_metrics.system_memory_usage.labels(
                        type="used",
                        instance=self.service_name
                    ).set(memory_mb * 1024 * 1024)  # Convert to bytes
                    
                    # Enhanced business metrics integration with BMAD correlation
                    if hasattr(enterprise_metrics, 'resource_utilization'):
                        enterprise_metrics.resource_utilization.labels(
                            resource_type="cpu",
                            instance=self.service_name,
                            recommended_action="optimize" if cpu_percent > 85 else "maintain",
                            business_impact="high" if cpu_percent > 90 else "low"
                        ).set(cpu_percent)
                        
                        memory_percent_usage = (memory_mb * 1024 * 1024) / psutil.virtual_memory().total * 100
                        enterprise_metrics.resource_utilization.labels(
                            resource_type="memory",
                            instance=self.service_name,
                            recommended_action="scale" if memory_percent_usage > 85 else "maintain",
                            business_impact="high" if memory_percent_usage > 90 else "low"
                        ).set(memory_percent_usage)
                        
                    # BMAD Platform Health Score
                    platform_health_score = self._calculate_platform_health_score()
                    if hasattr(enterprise_metrics, 'bmad_platform_health_score'):
                        enterprise_metrics.bmad_platform_health_score.set(platform_health_score)
                        
                    # Business Value at Risk Calculation
                    value_at_risk = self._calculate_business_value_at_risk()
                    if hasattr(enterprise_metrics, 'business_value_at_risk_dollars'):
                        enterprise_metrics.business_value_at_risk_dollars.set(value_at_risk)
                    
                except Exception as e:
                    self.logger.warning(f"Error collecting system metrics: {str(e)}")
                
                time.sleep(10)  # Collect every 10 seconds
        
        # Start metrics collection in background thread
        metrics_thread = threading.Thread(target=collect_metrics, daemon=True)
        metrics_thread.start()
    
    def _start_business_metrics_collection(self):
        """Start background business metrics collection."""
        
        def collect_business_metrics():
            while self.is_active:
                try:
                    asyncio.run(self.collect_business_performance_metrics())
                except Exception as e:
                    self.logger.warning(f"Error in business metrics collection: {str(e)}")
                
                time.sleep(60)  # Collect every 60 seconds
        
        # Start business metrics collection in background thread
        business_metrics_thread = threading.Thread(target=collect_business_metrics, daemon=True)
        business_metrics_thread.start()
    
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
    
    async def collect_business_performance_metrics(self):
        """Collect business-focused performance metrics."""
        try:
            # API Performance Business Metrics
            api_response_times = self.system_metrics.get("api_response_times", [])
            if api_response_times:
                recent_times = api_response_times[-10:]  # Last 10 measurements
                avg_response_time = sum(recent_times) / len(recent_times) if recent_times else 0
                
                # Check for SLA violations (<15ms target)
                sla_violations = sum(1 for t in recent_times if t > 15)
                if sla_violations > 0:
                    self.business_metrics["api_sla_violations"] += sla_violations
                    
                    # Calculate business impact
                    business_impact = sla_violations * 2100  # $2,100 per violation (Story 4 value)
                    self.business_metrics["revenue_impact"] += business_impact
                    
                    # Send business metric to DataDog
                    if self.datadog_monitoring:
                        self.datadog_monitoring.increment(
                            "business.api_sla_violations",
                            value=sla_violations,
                            tags=[f"service:{self.service_name}", "business_story:4", "impact:revenue"]
                        )
                        
                        self.datadog_monitoring.gauge(
                            "business.revenue_impact_dollars",
                            business_impact,
                            tags=[f"service:{self.service_name}", "category:api_performance"]
                        )
            
            # ETL Performance Business Metrics
            etl_job_durations = self.system_metrics.get("etl_job_durations", [])
            if etl_job_durations:
                recent_durations = etl_job_durations[-5:]  # Last 5 ETL jobs
                avg_duration = sum(recent_durations) / len(recent_durations) if recent_durations else 0
                
                # Check for processing delays (>60 minutes baseline)
                processing_delays = sum(1 for d in recent_durations if d > 3600)  # 1 hour in seconds
                if processing_delays > 0:
                    self.business_metrics["etl_processing_delays"] += processing_delays
                    
                    # Calculate business impact for data delays
                    delay_impact = processing_delays * 1800  # $1,800 per delay (Data Quality story value)
                    self.business_metrics["business_value_at_risk"] += delay_impact
                    
                    # Send business metric to DataDog
                    if self.datadog_monitoring:
                        self.datadog_monitoring.increment(
                            "business.etl_processing_delays",
                            value=processing_delays,
                            tags=[f"service:{self.service_name}", "business_story:2", "impact:data_quality"]
                        )
            
            # Data Quality Score Tracking
            data_quality_scores = self.system_metrics.get("data_quality_scores", [])
            if data_quality_scores:
                current_score = data_quality_scores[-1] if data_quality_scores else 100.0
                self.business_metrics["data_quality_score"] = current_score
                
                # Alert if quality drops below 95%
                if current_score < 95.0:
                    quality_degradation = 100.0 - current_score
                    value_at_risk = quality_degradation * 180  # $180 per percentage point
                    self.business_metrics["business_value_at_risk"] += value_at_risk
                    
                    if self.datadog_monitoring:
                        self.datadog_monitoring.gauge(
                            "business.data_quality_score",
                            current_score,
                            tags=[f"service:{self.service_name}", "business_story:2", "impact:quality"]
                        )
            
            # Security Performance Metrics
            security_incidents = self.system_metrics.get("security_incidents", [])
            if security_incidents:
                recent_incidents = len([i for i in security_incidents if i.get("timestamp", 0) > time.time() - 3600])
                if recent_incidents > 0:
                    # High business impact for security issues
                    security_impact = recent_incidents * 4200  # $4,200 per incident (Security story value)
                    self.business_metrics["business_value_at_risk"] += security_impact
                    
                    if self.datadog_monitoring:
                        self.datadog_monitoring.increment(
                            "business.security_incidents",
                            value=recent_incidents,
                            tags=[f"service:{self.service_name}", "business_story:6", "impact:security"]
                        )
            
            # Overall Platform ROI Calculation
            total_value_protected = 27_800_000  # Total BMAD platform value ($27.8M)
            value_at_risk_percentage = min(100.0, (self.business_metrics["business_value_at_risk"] / total_value_protected) * 100)
            platform_roi = max(0.0, 100.0 - value_at_risk_percentage)
            
            if self.datadog_monitoring:
                self.datadog_monitoring.gauge(
                    "business.platform_roi_percentage",
                    platform_roi,
                    tags=[f"service:{self.service_name}", "metric_type:roi"]
                )
                
                self.datadog_monitoring.gauge(
                    "business.total_value_at_risk_dollars",
                    self.business_metrics["business_value_at_risk"],
                    tags=[f"service:{self.service_name}", "metric_type:risk"]
                )
            
            # Enhanced BMAD Story Health Monitoring
            await self._update_bmad_story_health_metrics()
            
            # Executive KPI Dashboard Updates
            await self._update_executive_kpi_metrics()
            
            # Advanced alerting correlation
            await self._correlate_business_alerts()
            
            self.logger.info(
                f"Business metrics collected - "
                f"ROI: {platform_roi:.1f}%, "
                f"Value at risk: ${self.business_metrics['business_value_at_risk']:,.0f}, "
                f"Platform health: {self.business_metrics.get('platform_health_score', 100):.1f}%"
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting business performance metrics: {str(e)}")
    
    def track_api_performance(self, endpoint: str, response_time_ms: float, status_code: int):
        """Track API performance for business impact analysis."""
        if "api_response_times" not in self.system_metrics:
            self.system_metrics["api_response_times"] = []
        
        self.system_metrics["api_response_times"].append(response_time_ms)
        
        # Keep only recent measurements
        if len(self.system_metrics["api_response_times"]) > 100:
            self.system_metrics["api_response_times"] = self.system_metrics["api_response_times"][-100:]
        
        # Enhanced Prometheus metrics integration
        if hasattr(enterprise_metrics, 'api_response_time_histogram'):
            enterprise_metrics.api_response_time_histogram.labels(
                endpoint=endpoint,
                method="GET",
                status_code=str(status_code),
                sla_tier="critical" if response_time_ms <= 15 else "standard"
            ).observe(response_time_ms)
            
            enterprise_metrics.api_requests_total.labels(
                endpoint=endpoint,
                method="GET",
                status_code=str(status_code)
            ).inc()
            
            # SLA compliance calculation with BMAD Story 4 correlation
            compliance_rate = 100 if response_time_ms <= 15 else max(0, 100 - (response_time_ms - 15) * 2)
            enterprise_metrics.api_sla_compliance_rate.labels(
                endpoint=endpoint,
                time_window="real_time",
                bmad_story="4",
                business_value="2.1M"
            ).set(compliance_rate)
            
            # Update BMAD Story 4 health score
            current_health = self.business_metrics["bmad_story_health"]["story_4_api_performance"]
            if response_time_ms > 15:
                health_impact = min(10, (response_time_ms - 15) * 0.5)
                new_health = max(0, current_health - health_impact)
            else:
                new_health = min(100, current_health + 0.1)  # Gradual recovery
            
            self.business_metrics["bmad_story_health"]["story_4_api_performance"] = new_health
        
        # Track SLA violations in real-time
        if response_time_ms > 15:  # <15ms SLA target
            self.business_metrics["api_sla_violations"] += 1
            
            if self.datadog_monitoring:
                self.datadog_monitoring.increment(
                    "api.sla_violation",
                    tags=[
                        f"endpoint:{endpoint}",
                        f"response_time:{response_time_ms:.1f}ms",
                        f"status_code:{status_code}",
                        "business_impact:revenue",
                        "story_id:4",
                        "business_value:2.1M"
                    ]
                )
    
    def track_etl_performance(self, job_name: str, duration_seconds: float, records_processed: int, data_quality_score: float):
        """Track ETL performance for business impact analysis."""
        if "etl_job_durations" not in self.system_metrics:
            self.system_metrics["etl_job_durations"] = []
        if "data_quality_scores" not in self.system_metrics:
            self.system_metrics["data_quality_scores"] = []
        
        self.system_metrics["etl_job_durations"].append(duration_seconds)
        self.system_metrics["data_quality_scores"].append(data_quality_score)
        
        # Keep only recent measurements
        for metric_key in ["etl_job_durations", "data_quality_scores"]:
            if len(self.system_metrics[metric_key]) > 50:
                self.system_metrics[metric_key] = self.system_metrics[metric_key][-50:]
        
        # Track processing efficiency
        throughput = records_processed / duration_seconds if duration_seconds > 0 else 0
        
        # Enhanced Prometheus metrics integration
        if hasattr(enterprise_metrics, 'etl_job_duration'):
            enterprise_metrics.etl_job_duration.labels(
                job_name=job_name,
                stage="complete",
                status="success"
            ).observe(duration_seconds)
            
            enterprise_metrics.etl_records_processed.labels(
                job_name=job_name,
                table="processed",
                operation="transform"
            ).inc(records_processed)
            
            enterprise_metrics.etl_data_quality_score.labels(
                job_name=job_name,
                table="output",
                quality_rule="overall"
            ).set(data_quality_score)
            
            enterprise_metrics.etl_throughput.labels(
                job_name=job_name,
                stage="processing"
            ).set(throughput)
        
        if self.datadog_monitoring:
            self.datadog_monitoring.gauge(
                "etl.job_duration_seconds",
                duration_seconds,
                tags=[
                    f"job_name:{job_name}", 
                    "business_impact:data_quality",
                    "story_id:2",
                    "business_value:1.8M"
                ]
            )
            
            self.datadog_monitoring.gauge(
                "etl.throughput_records_per_second",
                throughput,
                tags=[
                    f"job_name:{job_name}", 
                    "business_impact:efficiency",
                    "story_id:2",
                    "optimization_target:high_throughput"
                ]
            )
            
            self.datadog_monitoring.gauge(
                "etl.data_quality_score",
                data_quality_score,
                tags=[
                    f"job_name:{job_name}", 
                    "business_impact:quality",
                    "story_id:2",
                    "sla_target:95_percent",
                    "bmad_story:2",
                    "business_value:1.8M"
                ]
            )
            
            # Update BMAD Story 2 health score based on data quality
            current_health = self.business_metrics["bmad_story_health"]["story_2_data_quality"]
            if data_quality_score < 95:
                health_impact = (95 - data_quality_score) * 2
                new_health = max(0, current_health - health_impact)
            else:
                new_health = min(100, current_health + 0.5)  # Gradual recovery
            
            self.business_metrics["bmad_story_health"]["story_2_data_quality"] = new_health
            
            # Send BMAD Story 2 health to DataDog
            self.datadog_monitoring.gauge(
                "bmad.story_2.data_quality_health_score",
                new_health,
                tags=["business_value:1.8M", "story_type:data_quality"]
            )
    
    def track_security_incident(self, incident_type: str, severity: str, resolved: bool = False):
        """Track security incidents for business impact analysis."""
        if "security_incidents" not in self.system_metrics:
            self.system_metrics["security_incidents"] = []
        
        incident = {
            "type": incident_type,
            "severity": severity,
            "resolved": resolved,
            "timestamp": time.time()
        }
        
        self.system_metrics["security_incidents"].append(incident)
        
        # Keep only recent incidents (last 24 hours)
        cutoff_time = time.time() - 86400  # 24 hours
        self.system_metrics["security_incidents"] = [
            i for i in self.system_metrics["security_incidents"] 
            if i["timestamp"] > cutoff_time
        ]
        
        # Enhanced security monitoring with BMAD Story 6 correlation
        if self.datadog_monitoring:
            self.datadog_monitoring.increment(
                "security.incident_detected",
                tags=[
                    f"type:{incident_type}",
                    f"severity:{severity}",
                    f"resolved:{resolved}",
                    "business_impact:security",
                    "bmad_story:6",
                    "business_value:4.2M"
                ]
            )
            
            # Update BMAD Story 6 health score
            severity_impact = {"low": 2, "medium": 5, "high": 10, "critical": 20}
            impact = severity_impact.get(severity, 5)
            
            current_health = self.business_metrics["bmad_story_health"]["story_6_advanced_security"]
            new_health = max(0, current_health - impact if not resolved else min(100, current_health + 1))
            self.business_metrics["bmad_story_health"]["story_6_advanced_security"] = new_health
            
            self.datadog_monitoring.gauge(
                "bmad.story_6.security_health_score",
                new_health,
                tags=["business_value:4.2M", "story_type:advanced_security"]
            )
    
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

def get_bmad_business_impact_analysis() -> Dict[str, Any]:
    """Get comprehensive BMAD business impact analysis."""
    profiler = get_datadog_profiler()
    return profiler.get_bmad_business_impact_analysis()


def get_system_health() -> Dict[str, Any]:
    """Get system health summary."""
    profiler = get_datadog_profiler()
    return profiler.get_system_health_summary()

def get_bmad_story_health() -> Dict[str, Any]:
    """Get BMAD story health summary."""
    profiler = get_datadog_profiler()
    return {
        "story_health": profiler.business_metrics.get("bmad_story_health", {}),
        "platform_health_score": profiler._calculate_platform_health_score(),
        "business_value_at_risk": profiler._calculate_business_value_at_risk(),
        "executive_kpis": profiler.business_metrics.get("executive_kpis", {})
    }

def get_business_metrics_summary() -> Dict[str, Any]:
    """Get comprehensive business metrics summary."""
    profiler = get_datadog_profiler()
    return {
        "bmad_platform_value": 27.8,  # $27.8M total value
        "story_health": profiler.business_metrics.get("bmad_story_health", {}),
        "platform_health_score": profiler._calculate_platform_health_score(),
        "value_at_risk_millions": profiler._calculate_business_value_at_risk() / 1_000_000,
        "roi_achievement_percentage": profiler.business_metrics.get("roi_achievement_percentage", 100),
        "critical_alerts_count": profiler.business_metrics.get("critical_alerts_last_hour", 0),
        "sla_compliance_rate": profiler.business_metrics.get("sla_compliance_rate", 100),
        "infrastructure_efficiency": profiler.business_metrics.get("infrastructure_efficiency", 100)
    }


def track_bmad_story_metric(story_id: str, metric_name: str, value: float, tags: List[str] = None):
    """Track BMAD story-specific metrics."""
    profiler = get_datadog_profiler()
    if profiler.datadog_monitoring:
        metric_tags = tags or []
        metric_tags.extend([f"bmad_story:{story_id}", f"metric:{metric_name}"])
        
        profiler.datadog_monitoring.gauge(
            f"bmad.story_{story_id}.{metric_name}",
            value,
            tags=metric_tags
        )

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
        
        # Add BMAD business context recommendations
        if "duration_stats" in analysis and analysis["duration_stats"]["avg_seconds"] > 0.015:
            recommendations.append(
                "API response time exceeds 15ms SLA target - impacts $2.1M API Performance story (BMAD Story 4)"
            )
            
        return recommendations
    
    def get_bmad_business_impact_analysis(self) -> Dict[str, Any]:
        """Generate BMAD business impact analysis."""
        try:
            platform_health = self._calculate_platform_health_score()
            value_at_risk = self._calculate_business_value_at_risk()
            
            # Identify most at-risk stories
            story_risks = []
            story_values = {
                "story_1_bi_dashboards": 2.5,
                "story_2_data_quality": 1.8,
                "story_3_security_governance": 3.2,
                "story_4_api_performance": 2.1,
                "story_4_1_mobile_analytics": 2.8,
                "story_4_2_ai_analytics": 3.5,
                "story_6_advanced_security": 4.2,
                "story_7_streaming": 3.4,
                "story_8_ml_operations": 6.4
            }
            
            for story, health in self.business_metrics["bmad_story_health"].items():
                if health < 95:  # Stories with health issues
                    story_value = story_values.get(story, 0)
                    risk_amount = story_value * ((100 - health) / 100)
                    story_risks.append({
                        "story": story.replace("_", " ").title(),
                        "health_percentage": health,
                        "business_value_millions": story_value,
                        "value_at_risk_millions": risk_amount,
                        "priority": "HIGH" if risk_amount > 1.0 else "MEDIUM" if risk_amount > 0.5 else "LOW"
                    })
            
            # Sort by risk amount descending
            story_risks.sort(key=lambda x: x["value_at_risk_millions"], reverse=True)
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "platform_health_score": platform_health,
                "total_business_value_millions": 27.8,
                "total_value_at_risk_millions": value_at_risk / 1_000_000,
                "roi_achievement_percentage": max(0, 100 - (value_at_risk / 27_800_000 * 100)),
                "stories_at_risk": story_risks,
                "critical_recommendations": self._generate_business_recommendations(story_risks),
                "executive_summary": self._generate_executive_summary(platform_health, value_at_risk / 1_000_000)
            }
            
        except Exception as e:
            self.logger.error(f"Error generating BMAD business impact analysis: {str(e)}")
            return {"error": str(e)}
    
    def _generate_business_recommendations(self, story_risks: List[Dict]) -> List[str]:
        """Generate business recommendations based on story risks."""
        recommendations = []
        
        high_risk_stories = [s for s in story_risks if s["priority"] == "HIGH"]
        if high_risk_stories:
            recommendations.append(
                f"URGENT: {len(high_risk_stories)} high-risk stories require immediate attention - "
                f"${sum(s['value_at_risk_millions'] for s in high_risk_stories):.1f}M at risk"
            )
        
        api_story = next((s for s in story_risks if "api performance" in s["story"].lower()), None)
        if api_story and api_story["health_percentage"] < 90:
            recommendations.append(
                "API Performance (Story 4) degraded - implement emergency performance optimization"
            )
            
        security_story = next((s for s in story_risks if "security" in s["story"].lower()), None)
        if security_story and security_story["health_percentage"] < 95:
            recommendations.append(
                "Security & Governance issues detected - immediate security review required"
            )
            
        return recommendations[:5]  # Top 5 recommendations
    
    def _generate_executive_summary(self, platform_health: float, value_at_risk: float) -> str:
        """Generate executive summary for C-suite reporting."""
        if platform_health >= 95:
            status = "EXCELLENT"
        elif platform_health >= 85:
            status = "GOOD"
        elif platform_health >= 75:
            status = "CONCERNING"
        else:
            status = "CRITICAL"
            
        return (
            f"BMAD Platform Status: {status} ({platform_health:.1f}% health). "
            f"Total investment: $27.8M. Current value at risk: ${value_at_risk:.1f}M. "
            f"ROI Achievement: {max(0, 100 - (value_at_risk / 27.8 * 100)):.1f}%. "
            f"{'Immediate executive attention required.' if status in ['CRITICAL', 'CONCERNING'] else 'Platform performing within acceptable parameters.'}"
        )
    
    def _calculate_platform_health_score(self) -> float:
        """Calculate overall BMAD platform health score."""
        try:
            story_weights = {
                "story_1_bi_dashboards": 0.09,     # $2.5M / $27.8M
                "story_2_data_quality": 0.065,    # $1.8M / $27.8M
                "story_3_security_governance": 0.115,  # $3.2M / $27.8M
                "story_4_api_performance": 0.076,  # $2.1M / $27.8M
                "story_4_1_mobile_analytics": 0.101, # $2.8M / $27.8M
                "story_4_2_ai_analytics": 0.126,   # $3.5M / $27.8M
                "story_6_advanced_security": 0.151, # $4.2M / $27.8M
                "story_7_streaming": 0.122,        # $3.4M / $27.8M
                "story_8_ml_operations": 0.230     # $6.4M / $27.8M
            }
            
            total_weighted_health = 0
            for story, health in self.business_metrics["bmad_story_health"].items():
                weight = story_weights.get(story, 0)
                total_weighted_health += health * weight
                
            return min(100.0, max(0.0, total_weighted_health))
            
        except Exception as e:
            self.logger.error(f"Error calculating platform health score: {str(e)}")
            return 100.0
    
    def _calculate_business_value_at_risk(self) -> float:
        """Calculate total business value at risk based on story health."""
        try:
            story_values = {
                "story_1_bi_dashboards": 2.5,
                "story_2_data_quality": 1.8,
                "story_3_security_governance": 3.2,
                "story_4_api_performance": 2.1,
                "story_4_1_mobile_analytics": 2.8,
                "story_4_2_ai_analytics": 3.5,
                "story_6_advanced_security": 4.2,
                "story_7_streaming": 3.4,
                "story_8_ml_operations": 6.4
            }
            
            total_value_at_risk = 0
            for story, health in self.business_metrics["bmad_story_health"].items():
                story_value = story_values.get(story, 0) * 1_000_000  # Convert to dollars
                health_percentage = health / 100.0
                risk_percentage = 1.0 - health_percentage
                value_at_risk = story_value * risk_percentage
                total_value_at_risk += value_at_risk
                
            return total_value_at_risk
            
        except Exception as e:
            self.logger.error(f"Error calculating business value at risk: {str(e)}")
            return 0.0
    
    async def _update_bmad_story_health_metrics(self):
        """Update BMAD story health metrics based on current performance."""
        try:
            for story_name, metrics in self.bmad_story_metrics.items():
                current_health = self.business_metrics["bmad_story_health"].get(story_name, 100.0)
                business_value = metrics["business_value"]
                
                # Send individual story health to DataDog
                if self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        f"bmad.{story_name}.health_score",
                        current_health,
                        tags=[
                            f"business_value:{business_value}M",
                            f"story_type:{story_name.split('_', 2)[-1]}",
                            "metric_type:health_score"
                        ]
                    )
                    
                    # Calculate and send value at risk for this story
                    value_at_risk = (business_value * 1_000_000) * ((100 - current_health) / 100)
                    self.datadog_monitoring.gauge(
                        f"bmad.{story_name}.value_at_risk_dollars",
                        value_at_risk,
                        tags=[
                            f"business_value:{business_value}M",
                            f"story_type:{story_name.split('_', 2)[-1]}",
                            "metric_type:value_at_risk"
                        ]
                    )
                    
        except Exception as e:
            self.logger.error(f"Error updating BMAD story health metrics: {str(e)}")
    
    async def _update_executive_kpi_metrics(self):
        """Update executive KPI dashboard metrics."""
        try:
            # Calculate executive KPIs
            platform_health = self._calculate_platform_health_score()
            value_at_risk = self._calculate_business_value_at_risk()
            roi_achievement = max(0, 100 - (value_at_risk / 27_800_000 * 100))
            
            executive_kpis = {
                "platform_health_percentage": platform_health,
                "roi_achievement_percentage": roi_achievement,
                "total_business_value_millions": 27.8,
                "value_at_risk_millions": value_at_risk / 1_000_000,
                "critical_alerts_last_hour": self.business_metrics.get("critical_alerts_last_hour", 0),
                "sla_compliance_rate": self.business_metrics.get("sla_compliance_rate", 100),
                "infrastructure_efficiency": self.business_metrics.get("infrastructure_efficiency", 100)
            }
            
            # Send executive KPIs to DataDog
            if self.datadog_monitoring:
                for kpi_name, kpi_value in executive_kpis.items():
                    self.datadog_monitoring.gauge(
                        f"executive.kpi.{kpi_name}",
                        kpi_value,
                        tags=[
                            "dashboard_type:executive",
                            "metric_category:kpi",
                            "business_level:c_suite"
                        ]
                    )
                    
            # Update business metrics for external access
            self.business_metrics.update({
                "platform_health_score": platform_health,
                "roi_achievement_percentage": roi_achievement,
                "executive_kpis": executive_kpis
            })
            
        except Exception as e:
            self.logger.error(f"Error updating executive KPI metrics: {str(e)}")
    
    async def _correlate_business_alerts(self):
        """Correlate alerts with business impact and create intelligent summaries."""
        try:
            # Count critical alerts in last hour
            current_time = time.time()
            recent_critical_alerts = 0
            
            # This would be enhanced with actual alert correlation logic
            # For now, simulate based on story health
            for story, health in self.business_metrics["bmad_story_health"].items():
                if health < 90:  # Story in degraded state
                    recent_critical_alerts += 1
                    
            self.business_metrics["critical_alerts_last_hour"] = recent_critical_alerts
            
            # Calculate SLA compliance rate
            api_health = self.business_metrics["bmad_story_health"].get("story_4_api_performance", 100)
            data_quality_health = self.business_metrics["bmad_story_health"].get("story_2_data_quality", 100)
            sla_compliance = min(api_health, data_quality_health)
            self.business_metrics["sla_compliance_rate"] = sla_compliance
            
            # Send correlated metrics to DataDog
            if self.datadog_monitoring:
                self.datadog_monitoring.gauge(
                    "business.alert_correlation.critical_alerts_count",
                    recent_critical_alerts,
                    tags=["time_window:1hour", "alert_severity:critical"]
                )
                
                self.datadog_monitoring.gauge(
                    "business.sla_compliance.overall_rate",
                    sla_compliance,
                    tags=["metric_type:sla_compliance", "business_impact:high"]
                )
                
        except Exception as e:
            self.logger.error(f"Error correlating business alerts: {str(e)}")