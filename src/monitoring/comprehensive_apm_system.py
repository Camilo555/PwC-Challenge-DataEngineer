"""
Comprehensive Application Performance Monitoring (APM) System
=============================================================

Enterprise-grade APM system providing 360° application visibility with:
- Real-time performance tracking and analysis
- Distributed tracing across microservices
- Database query performance monitoring
- Memory leak detection and resource optimization
- Business transaction monitoring
- User experience tracking
- Predictive performance analytics
"""

import asyncio
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import json
import threading
from contextlib import asynccontextmanager, contextmanager
import gc
import psutil
import sys
from functools import wraps

import aiohttp
from sqlalchemy import text, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession
from prometheus_client import Counter, Histogram, Gauge, Summary
import redis

from core.database import get_async_session
from core.logging import get_logger
from core.config import settings
from monitoring.enterprise_prometheus_metrics import EnterprisePrometheusMetrics

logger = get_logger(__name__)


class PerformanceLevel(Enum):
    """Performance impact levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


class TransactionType(Enum):
    """Business transaction types"""
    API_REQUEST = "api_request"
    DATABASE_QUERY = "database_query"
    EXTERNAL_SERVICE = "external_service"
    BACKGROUND_TASK = "background_task"
    ETL_PROCESS = "etl_process"
    USER_SESSION = "user_session"


@dataclass
class PerformanceTrace:
    """Individual performance trace record"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "in_progress"
    error: Optional[str] = None
    stack_trace: Optional[str] = None


@dataclass
class BusinessTransaction:
    """Business transaction tracking"""
    transaction_id: str
    transaction_type: TransactionType
    user_id: Optional[str]
    session_id: Optional[str]
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration_ms: Optional[float] = None
    spans: List[PerformanceTrace] = field(default_factory=list)
    business_context: Dict[str, Any] = field(default_factory=dict)
    performance_level: PerformanceLevel = PerformanceLevel.GOOD
    sla_target_ms: Optional[float] = None
    sla_met: Optional[bool] = None


@dataclass
class ResourceMetrics:
    """System resource metrics"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_mb: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_io_recv_mb: float
    network_io_sent_mb: float
    active_threads: int
    gc_collections: Dict[str, int] = field(default_factory=dict)


@dataclass
class DatabaseMetrics:
    """Database performance metrics"""
    query_hash: str
    query_text: str
    execution_count: int
    total_duration_ms: float
    avg_duration_ms: float
    max_duration_ms: float
    min_duration_ms: float
    last_execution: datetime
    tables_accessed: List[str] = field(default_factory=list)
    query_plan: Optional[str] = None
    optimization_suggestions: List[str] = field(default_factory=list)


class ComprehensiveAPMSystem:
    """
    Enterprise APM system providing comprehensive application performance monitoring
    with real-time analysis, distributed tracing, and predictive analytics.
    """

    def __init__(self, prometheus_metrics: Optional[EnterprisePrometheusMetrics] = None):
        self.prometheus_metrics = prometheus_metrics or EnterprisePrometheusMetrics()
        self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")

        # Performance data storage
        self.active_traces: Dict[str, PerformanceTrace] = {}
        self.completed_traces: deque = deque(maxlen=10000)  # Keep last 10k traces
        self.business_transactions: Dict[str, BusinessTransaction] = {}
        self.database_metrics: Dict[str, DatabaseMetrics] = {}

        # Resource monitoring
        self.resource_history: deque = deque(maxlen=1440)  # 24 hours at 1-minute intervals
        self.performance_baselines: Dict[str, float] = {}

        # Configuration
        self.sampling_rate = 1.0  # 100% sampling initially
        self.trace_timeout_seconds = 300  # 5 minutes
        self.resource_collection_interval = 60  # 1 minute
        self.database_slow_query_threshold_ms = 1000  # 1 second

        # Background monitoring
        self.monitoring_enabled = True
        self._monitoring_tasks: List[asyncio.Task] = []

        # Redis for distributed tracing
        self.redis_client: Optional[redis.Redis] = None

        # Thread-local storage for trace context
        self._thread_local = threading.local()

    async def initialize(self):
        """Initialize the APM system"""
        try:
            # Initialize Redis for distributed tracing
            try:
                redis_url = getattr(settings, 'redis_url', 'redis://localhost:6379/2')
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                await asyncio.get_event_loop().run_in_executor(None, self.redis_client.ping)
                self.logger.info("Redis connection established for distributed tracing")
            except Exception as e:
                self.logger.warning(f"Redis not available for distributed tracing: {e}")

            # Set up database monitoring
            self._setup_database_monitoring()

            # Start background monitoring tasks
            await self._start_background_monitoring()

            # Initialize performance baselines
            await self._initialize_performance_baselines()

            self.logger.info("Comprehensive APM system initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize APM system: {e}")
            raise

    def _setup_database_monitoring(self):
        """Set up database query monitoring"""
        try:
            # SQLAlchemy event listeners for query monitoring
            @event.listens_for(Engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                context._query_start_time = time.time()

            @event.listens_for(Engine, "after_cursor_execute")
            def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                if hasattr(context, '_query_start_time'):
                    duration_ms = (time.time() - context._query_start_time) * 1000
                    asyncio.create_task(self._record_database_query(statement, duration_ms, parameters))

            self.logger.info("Database monitoring event listeners configured")

        except Exception as e:
            self.logger.error(f"Failed to setup database monitoring: {e}")

    async def _start_background_monitoring(self):
        """Start background monitoring tasks"""
        try:
            # Resource monitoring task
            resource_task = asyncio.create_task(self._resource_monitoring_loop())
            self._monitoring_tasks.append(resource_task)

            # Trace cleanup task
            cleanup_task = asyncio.create_task(self._trace_cleanup_loop())
            self._monitoring_tasks.append(cleanup_task)

            # Performance analysis task
            analysis_task = asyncio.create_task(self._performance_analysis_loop())
            self._monitoring_tasks.append(analysis_task)

            self.logger.info("Background monitoring tasks started")

        except Exception as e:
            self.logger.error(f"Failed to start background monitoring: {e}")

    async def _initialize_performance_baselines(self):
        """Initialize performance baselines for comparison"""
        try:
            # Set initial baselines (would typically come from historical data)
            self.performance_baselines = {
                "api_response_time_p95": 150.0,  # 150ms
                "database_query_avg": 50.0,      # 50ms
                "memory_usage_percent": 70.0,    # 70%
                "cpu_usage_percent": 60.0,       # 60%
                "transaction_throughput": 100.0  # 100 tps
            }

            self.logger.info("Performance baselines initialized")

        except Exception as e:
            self.logger.error(f"Failed to initialize performance baselines: {e}")

    async def start_transaction(
        self,
        transaction_type: TransactionType,
        operation_name: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        business_context: Optional[Dict[str, Any]] = None,
        sla_target_ms: Optional[float] = None
    ) -> str:
        """Start a new business transaction"""
        try:
            transaction_id = f"txn-{int(time.time() * 1000000)}-{hash(operation_name) % 10000}"

            transaction = BusinessTransaction(
                transaction_id=transaction_id,
                transaction_type=transaction_type,
                user_id=user_id,
                session_id=session_id,
                start_time=datetime.utcnow(),
                business_context=business_context or {},
                sla_target_ms=sla_target_ms
            )

            self.business_transactions[transaction_id] = transaction

            # Start root span
            root_span_id = await self.start_trace(
                operation_name=operation_name,
                tags={
                    "transaction_id": transaction_id,
                    "transaction_type": transaction_type.value,
                    "user_id": user_id,
                    "session_id": session_id
                }
            )

            transaction.spans.append(self.active_traces[root_span_id])

            # Store in thread-local for context propagation
            if not hasattr(self._thread_local, 'current_transaction'):
                self._thread_local.current_transaction = transaction_id

            self.logger.debug(f"Started business transaction: {transaction_id}")
            return transaction_id

        except Exception as e:
            self.logger.error(f"Failed to start transaction: {e}")
            raise

    async def start_trace(
        self,
        operation_name: str,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None
    ) -> str:
        """Start a new performance trace"""
        try:
            span_id = f"span-{int(time.time() * 1000000)}-{hash(operation_name) % 10000}"
            trace_id = parent_span_id or f"trace-{int(time.time() * 1000000)}"

            trace = PerformanceTrace(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
                operation_name=operation_name,
                start_time=datetime.utcnow(),
                tags=tags or {}
            )

            self.active_traces[span_id] = trace

            # Store distributed tracing info in Redis if available
            if self.redis_client:
                try:
                    trace_data = {
                        "trace_id": trace_id,
                        "span_id": span_id,
                        "parent_span_id": parent_span_id,
                        "operation": operation_name,
                        "start_time": trace.start_time.isoformat(),
                        "tags": tags or {}
                    }
                    await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self.redis_client.setex(
                            f"apm:trace:{span_id}",
                            self.trace_timeout_seconds,
                            json.dumps(trace_data, default=str)
                        )
                    )
                except Exception as e:
                    self.logger.debug(f"Failed to store trace in Redis: {e}")

            return span_id

        except Exception as e:
            self.logger.error(f"Failed to start trace: {e}")
            raise

    async def finish_trace(
        self,
        span_id: str,
        status: str = "success",
        error: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None
    ):
        """Finish a performance trace"""
        try:
            if span_id not in self.active_traces:
                self.logger.warning(f"Trace {span_id} not found in active traces")
                return

            trace = self.active_traces[span_id]
            trace.end_time = datetime.utcnow()
            trace.duration_ms = (trace.end_time - trace.start_time).total_seconds() * 1000
            trace.status = status

            if error:
                trace.error = error
                trace.stack_trace = traceback.format_exc()

            if tags:
                trace.tags.update(tags)

            # Move to completed traces
            self.completed_traces.append(trace)
            del self.active_traces[span_id]

            # Update Prometheus metrics
            self._update_trace_metrics(trace)

            # Analyze performance
            await self._analyze_trace_performance(trace)

            self.logger.debug(f"Finished trace: {span_id} in {trace.duration_ms:.2f}ms")

        except Exception as e:
            self.logger.error(f"Failed to finish trace {span_id}: {e}")

    async def finish_transaction(self, transaction_id: str):
        """Finish a business transaction"""
        try:
            if transaction_id not in self.business_transactions:
                self.logger.warning(f"Transaction {transaction_id} not found")
                return

            transaction = self.business_transactions[transaction_id]
            transaction.end_time = datetime.utcnow()
            transaction.total_duration_ms = (
                transaction.end_time - transaction.start_time
            ).total_seconds() * 1000

            # Calculate performance level
            transaction.performance_level = self._calculate_performance_level(
                transaction.total_duration_ms,
                transaction.sla_target_ms
            )

            # Check SLA compliance
            if transaction.sla_target_ms:
                transaction.sla_met = transaction.total_duration_ms <= transaction.sla_target_ms

            # Update business metrics
            await self._update_business_transaction_metrics(transaction)

            # Clean up thread-local context
            if hasattr(self._thread_local, 'current_transaction'):
                delattr(self._thread_local, 'current_transaction')

            self.logger.info(
                f"Finished transaction: {transaction_id} in {transaction.total_duration_ms:.2f}ms "
                f"(Performance: {transaction.performance_level.value})"
            )

        except Exception as e:
            self.logger.error(f"Failed to finish transaction {transaction_id}: {e}")

    def _calculate_performance_level(
        self, duration_ms: float, sla_target_ms: Optional[float]
    ) -> PerformanceLevel:
        """Calculate performance level based on duration and SLA"""
        if sla_target_ms:
            ratio = duration_ms / sla_target_ms
            if ratio <= 0.5:
                return PerformanceLevel.EXCELLENT
            elif ratio <= 1.0:
                return PerformanceLevel.GOOD
            elif ratio <= 2.0:
                return PerformanceLevel.FAIR
            elif ratio <= 4.0:
                return PerformanceLevel.POOR
            else:
                return PerformanceLevel.CRITICAL
        else:
            # Use generic thresholds
            if duration_ms <= 50:
                return PerformanceLevel.EXCELLENT
            elif duration_ms <= 200:
                return PerformanceLevel.GOOD
            elif duration_ms <= 1000:
                return PerformanceLevel.FAIR
            elif duration_ms <= 5000:
                return PerformanceLevel.POOR
            else:
                return PerformanceLevel.CRITICAL

    def _update_trace_metrics(self, trace: PerformanceTrace):
        """Update Prometheus metrics for trace"""
        try:
            # Update APM histogram
            if hasattr(self.prometheus_metrics, 'api_response_time_histogram'):
                labels = {
                    'endpoint': trace.operation_name,
                    'method': trace.tags.get('http_method', 'unknown'),
                    'status_code': trace.tags.get('http_status', 'unknown'),
                    'sla_tier': 'premium' if trace.duration_ms <= 15 else 'standard'
                }
                self.prometheus_metrics.api_response_time_histogram.labels(**labels).observe(trace.duration_ms)

        except Exception as e:
            self.logger.error(f"Failed to update trace metrics: {e}")

    async def _update_business_transaction_metrics(self, transaction: BusinessTransaction):
        """Update business transaction metrics"""
        try:
            # Record business metric for transaction performance
            from monitoring.business_metrics_collector import record_business_metric, BusinessMetricType

            await record_business_metric(
                name=f"transaction_duration_{transaction.transaction_type.value}",
                value=transaction.total_duration_ms,
                metric_type=BusinessMetricType.OPERATIONAL,
                unit="milliseconds",
                description=f"Average duration for {transaction.transaction_type.value} transactions",
                tags={
                    "performance_level": transaction.performance_level.value,
                    "sla_met": str(transaction.sla_met) if transaction.sla_met is not None else "unknown"
                },
                business_impact="high" if transaction.sla_target_ms and transaction.sla_target_ms <= 100 else "medium"
            )

        except Exception as e:
            self.logger.error(f"Failed to update business transaction metrics: {e}")

    async def _record_database_query(
        self, query: str, duration_ms: float, parameters: Optional[Any] = None
    ):
        """Record database query performance"""
        try:
            # Create query hash for grouping
            query_hash = str(hash(query.strip()[:500]))  # Use first 500 chars

            if query_hash not in self.database_metrics:
                self.database_metrics[query_hash] = DatabaseMetrics(
                    query_hash=query_hash,
                    query_text=query.strip()[:1000],  # Limit query text length
                    execution_count=0,
                    total_duration_ms=0.0,
                    avg_duration_ms=0.0,
                    max_duration_ms=0.0,
                    min_duration_ms=float('inf'),
                    last_execution=datetime.utcnow()
                )

            metrics = self.database_metrics[query_hash]
            metrics.execution_count += 1
            metrics.total_duration_ms += duration_ms
            metrics.avg_duration_ms = metrics.total_duration_ms / metrics.execution_count
            metrics.max_duration_ms = max(metrics.max_duration_ms, duration_ms)
            metrics.min_duration_ms = min(metrics.min_duration_ms, duration_ms)
            metrics.last_execution = datetime.utcnow()

            # Check for slow queries
            if duration_ms > self.database_slow_query_threshold_ms:
                self.logger.warning(
                    f"Slow query detected: {duration_ms:.2f}ms - {query[:100]}..."
                )

                # Record slow query metric
                from monitoring.business_metrics_collector import record_business_metric, BusinessMetricType
                await record_business_metric(
                    name="database_slow_query_count",
                    value=1,
                    metric_type=BusinessMetricType.OPERATIONAL,
                    unit="count",
                    description="Number of slow database queries",
                    threshold_warning=10.0,
                    threshold_critical=25.0,
                    business_impact="high"
                )

        except Exception as e:
            self.logger.error(f"Failed to record database query: {e}")

    async def _analyze_trace_performance(self, trace: PerformanceTrace):
        """Analyze individual trace performance"""
        try:
            # Check against baselines
            operation_baseline = self.performance_baselines.get(
                f"operation_{trace.operation_name}",
                self.performance_baselines.get("api_response_time_p95", 200.0)
            )

            if trace.duration_ms > operation_baseline * 2:
                self.logger.warning(
                    f"Performance degradation detected: {trace.operation_name} "
                    f"took {trace.duration_ms:.2f}ms (baseline: {operation_baseline:.2f}ms)"
                )

                # Record performance anomaly
                from monitoring.business_metrics_collector import record_business_metric, BusinessMetricType
                await record_business_metric(
                    name="performance_anomaly_detected",
                    value=1,
                    metric_type=BusinessMetricType.OPERATIONAL,
                    unit="count",
                    description="Performance anomaly detected",
                    tags={
                        "operation": trace.operation_name,
                        "severity": "high" if trace.duration_ms > operation_baseline * 5 else "medium"
                    },
                    business_impact="high"
                )

        except Exception as e:
            self.logger.error(f"Failed to analyze trace performance: {e}")

    async def _resource_monitoring_loop(self):
        """Background loop for resource monitoring"""
        while self.monitoring_enabled:
            try:
                # Collect system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk_io = psutil.disk_io_counters()
                network_io = psutil.net_io_counters()

                # Get garbage collection stats
                gc_stats = {f"gen{i}": gc.get_stats()[i]['collections'] for i in range(3)}

                resource_metrics = ResourceMetrics(
                    timestamp=datetime.utcnow(),
                    cpu_percent=cpu_percent,
                    memory_percent=memory.percent,
                    memory_mb=memory.used / (1024 * 1024),
                    disk_io_read_mb=(disk_io.read_bytes if disk_io else 0) / (1024 * 1024),
                    disk_io_write_mb=(disk_io.write_bytes if disk_io else 0) / (1024 * 1024),
                    network_io_recv_mb=(network_io.bytes_recv if network_io else 0) / (1024 * 1024),
                    network_io_sent_mb=(network_io.bytes_sent if network_io else 0) / (1024 * 1024),
                    active_threads=threading.active_count(),
                    gc_collections=gc_stats
                )

                self.resource_history.append(resource_metrics)

                # Update business metrics
                await self._update_resource_business_metrics(resource_metrics)

                # Check for resource issues
                await self._check_resource_thresholds(resource_metrics)

                await asyncio.sleep(self.resource_collection_interval)

            except Exception as e:
                self.logger.error(f"Error in resource monitoring loop: {e}")
                await asyncio.sleep(self.resource_collection_interval)

    async def _update_resource_business_metrics(self, metrics: ResourceMetrics):
        """Update business metrics from resource data"""
        try:
            from monitoring.business_metrics_collector import record_business_metric, BusinessMetricType

            await record_business_metric(
                name="system_cpu_utilization",
                value=metrics.cpu_percent,
                metric_type=BusinessMetricType.OPERATIONAL,
                unit="percentage",
                description="System CPU utilization",
                target_value=70.0,
                threshold_warning=80.0,
                threshold_critical=90.0,
                business_impact="medium"
            )

            await record_business_metric(
                name="system_memory_utilization",
                value=metrics.memory_percent,
                metric_type=BusinessMetricType.OPERATIONAL,
                unit="percentage",
                description="System memory utilization",
                target_value=75.0,
                threshold_warning=85.0,
                threshold_critical=95.0,
                business_impact="high"
            )

        except Exception as e:
            self.logger.error(f"Failed to update resource business metrics: {e}")

    async def _check_resource_thresholds(self, metrics: ResourceMetrics):
        """Check resource usage against thresholds"""
        try:
            # CPU threshold check
            if metrics.cpu_percent > 90:
                self.logger.critical(f"Critical CPU usage: {metrics.cpu_percent:.1f}%")
            elif metrics.cpu_percent > 80:
                self.logger.warning(f"High CPU usage: {metrics.cpu_percent:.1f}%")

            # Memory threshold check
            if metrics.memory_percent > 95:
                self.logger.critical(f"Critical memory usage: {metrics.memory_percent:.1f}%")
            elif metrics.memory_percent > 85:
                self.logger.warning(f"High memory usage: {metrics.memory_percent:.1f}%")

            # Memory leak detection (simplified)
            if len(self.resource_history) >= 10:
                recent_memory = [m.memory_percent for m in list(self.resource_history)[-10:]]
                if all(recent_memory[i] <= recent_memory[i+1] for i in range(len(recent_memory)-1)):
                    # Consistently increasing memory
                    if recent_memory[-1] - recent_memory[0] > 10:  # 10% increase
                        self.logger.warning("Potential memory leak detected - consistent memory growth")

        except Exception as e:
            self.logger.error(f"Failed to check resource thresholds: {e}")

    async def _trace_cleanup_loop(self):
        """Background loop to clean up stale traces"""
        while self.monitoring_enabled:
            try:
                current_time = datetime.utcnow()
                stale_spans = []

                for span_id, trace in self.active_traces.items():
                    if (current_time - trace.start_time).total_seconds() > self.trace_timeout_seconds:
                        stale_spans.append(span_id)

                for span_id in stale_spans:
                    await self.finish_trace(span_id, status="timeout", error="Trace timeout")

                # Clean up old business transactions
                stale_transactions = []
                for txn_id, transaction in self.business_transactions.items():
                    if transaction.end_time is None and (current_time - transaction.start_time).total_seconds() > self.trace_timeout_seconds:
                        stale_transactions.append(txn_id)

                for txn_id in stale_transactions:
                    await self.finish_transaction(txn_id)

                await asyncio.sleep(60)  # Run cleanup every minute

            except Exception as e:
                self.logger.error(f"Error in trace cleanup loop: {e}")
                await asyncio.sleep(60)

    async def _performance_analysis_loop(self):
        """Background loop for performance analysis"""
        while self.monitoring_enabled:
            try:
                await self._analyze_overall_performance()
                await asyncio.sleep(300)  # Run analysis every 5 minutes

            except Exception as e:
                self.logger.error(f"Error in performance analysis loop: {e}")
                await asyncio.sleep(300)

    async def _analyze_overall_performance(self):
        """Analyze overall system performance"""
        try:
            # Analyze recent traces
            recent_traces = list(self.completed_traces)[-100:]  # Last 100 traces
            if not recent_traces:
                return

            # Calculate performance statistics
            durations = [t.duration_ms for t in recent_traces if t.duration_ms]
            if not durations:
                return

            avg_duration = sum(durations) / len(durations)
            p95_duration = sorted(durations)[int(len(durations) * 0.95)]
            error_rate = len([t for t in recent_traces if t.status == "error"]) / len(recent_traces)

            # Record overall performance metrics
            from monitoring.business_metrics_collector import record_business_metric, BusinessMetricType

            await record_business_metric(
                name="overall_api_performance_p95",
                value=p95_duration,
                metric_type=BusinessMetricType.OPERATIONAL,
                unit="milliseconds",
                description="95th percentile API response time",
                target_value=100.0,
                threshold_warning=200.0,
                threshold_critical=500.0,
                business_impact="critical"
            )

            await record_business_metric(
                name="overall_error_rate",
                value=error_rate * 100,
                metric_type=BusinessMetricType.OPERATIONAL,
                unit="percentage",
                description="Overall system error rate",
                target_value=1.0,
                threshold_warning=2.0,
                threshold_critical=5.0,
                business_impact="critical"
            )

        except Exception as e:
            self.logger.error(f"Failed to analyze overall performance: {e}")

    # Decorator methods for easy instrumentation

    def trace_method(self, operation_name: Optional[str] = None):
        """Decorator to automatically trace method execution"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                span_id = await self.start_trace(op_name)
                try:
                    result = await func(*args, **kwargs)
                    await self.finish_trace(span_id)
                    return result
                except Exception as e:
                    await self.finish_trace(span_id, status="error", error=str(e))
                    raise

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                # For sync functions, we'd need to handle differently
                # This is a simplified version
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    self.logger.error(f"Error in traced method {op_name}: {e}")
                    raise

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        return decorator

    @asynccontextmanager
    async def trace_context(self, operation_name: str, tags: Optional[Dict[str, Any]] = None):
        """Context manager for tracing code blocks"""
        span_id = await self.start_trace(operation_name, tags=tags)
        try:
            yield span_id
            await self.finish_trace(span_id)
        except Exception as e:
            await self.finish_trace(span_id, status="error", error=str(e))
            raise

    async def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            current_time = datetime.utcnow()

            # Recent traces analysis
            recent_traces = list(self.completed_traces)[-1000:]  # Last 1000 traces
            active_trace_count = len(self.active_traces)

            performance_summary = {
                "timestamp": current_time.isoformat(),
                "active_traces": active_trace_count,
                "completed_traces_analyzed": len(recent_traces),
                "resource_metrics": {},
                "database_performance": {},
                "business_transactions": {},
                "system_health": {}
            }

            # Resource metrics summary
            if self.resource_history:
                latest_resources = self.resource_history[-1]
                performance_summary["resource_metrics"] = {
                    "cpu_percent": latest_resources.cpu_percent,
                    "memory_percent": latest_resources.memory_percent,
                    "memory_mb": latest_resources.memory_mb,
                    "active_threads": latest_resources.active_threads,
                    "timestamp": latest_resources.timestamp.isoformat()
                }

            # Database performance summary
            if self.database_metrics:
                slow_queries = [
                    m for m in self.database_metrics.values()
                    if m.avg_duration_ms > self.database_slow_query_threshold_ms
                ]

                performance_summary["database_performance"] = {
                    "total_unique_queries": len(self.database_metrics),
                    "slow_queries_count": len(slow_queries),
                    "avg_query_duration_ms": sum(m.avg_duration_ms for m in self.database_metrics.values()) / len(self.database_metrics),
                    "max_query_duration_ms": max(m.max_duration_ms for m in self.database_metrics.values()) if self.database_metrics else 0
                }

            # Business transactions summary
            active_transactions = len(self.business_transactions)
            performance_summary["business_transactions"] = {
                "active_transactions": active_transactions,
                "transaction_types": list(set(t.transaction_type.value for t in self.business_transactions.values()))
            }

            # Trace performance analysis
            if recent_traces:
                durations = [t.duration_ms for t in recent_traces if t.duration_ms]
                if durations:
                    performance_summary["trace_performance"] = {
                        "avg_duration_ms": sum(durations) / len(durations),
                        "p95_duration_ms": sorted(durations)[int(len(durations) * 0.95)],
                        "p99_duration_ms": sorted(durations)[int(len(durations) * 0.99)],
                        "error_rate_percent": len([t for t in recent_traces if t.status == "error"]) / len(recent_traces) * 100
                    }

            # System health assessment
            health_score = self._calculate_system_health_score()
            performance_summary["system_health"] = {
                "overall_health_score": health_score,
                "health_level": self._get_health_level(health_score),
                "monitoring_active": self.monitoring_enabled
            }

            return performance_summary

        except Exception as e:
            self.logger.error(f"Failed to get performance summary: {e}")
            return {"error": str(e)}

    def _calculate_system_health_score(self) -> float:
        """Calculate overall system health score (0-100)"""
        try:
            score = 100.0

            # Resource health
            if self.resource_history:
                latest = self.resource_history[-1]
                if latest.cpu_percent > 90:
                    score -= 30
                elif latest.cpu_percent > 80:
                    score -= 15

                if latest.memory_percent > 95:
                    score -= 30
                elif latest.memory_percent > 85:
                    score -= 15

            # Trace performance health
            recent_traces = list(self.completed_traces)[-100:]
            if recent_traces:
                error_rate = len([t for t in recent_traces if t.status == "error"]) / len(recent_traces)
                if error_rate > 0.1:  # 10% error rate
                    score -= 40
                elif error_rate > 0.05:  # 5% error rate
                    score -= 20

            # Database health
            if self.database_metrics:
                slow_query_ratio = len([
                    m for m in self.database_metrics.values()
                    if m.avg_duration_ms > self.database_slow_query_threshold_ms
                ]) / len(self.database_metrics)

                if slow_query_ratio > 0.5:  # 50% slow queries
                    score -= 25
                elif slow_query_ratio > 0.2:  # 20% slow queries
                    score -= 10

            return max(0.0, score)

        except Exception as e:
            self.logger.error(f"Failed to calculate system health score: {e}")
            return 50.0  # Default to fair health

    def _get_health_level(self, score: float) -> str:
        """Get health level from score"""
        if score >= 90:
            return "excellent"
        elif score >= 75:
            return "good"
        elif score >= 50:
            return "fair"
        elif score >= 25:
            return "poor"
        else:
            return "critical"

    async def shutdown(self):
        """Shutdown the APM system"""
        try:
            self.monitoring_enabled = False

            # Cancel all background tasks
            for task in self._monitoring_tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Finish all active traces
            for span_id in list(self.active_traces.keys()):
                await self.finish_trace(span_id, status="shutdown")

            # Finish all active transactions
            for txn_id in list(self.business_transactions.keys()):
                await self.finish_transaction(txn_id)

            self.logger.info("APM system shutdown completed")

        except Exception as e:
            self.logger.error(f"Error during APM system shutdown: {e}")


# Global instance
_apm_system: Optional[ComprehensiveAPMSystem] = None


async def get_apm_system() -> ComprehensiveAPMSystem:
    """Get or create the APM system instance"""
    global _apm_system

    if _apm_system is None:
        _apm_system = ComprehensiveAPMSystem()
        await _apm_system.initialize()

    return _apm_system


# Convenience functions for easy usage
async def start_transaction(operation_name: str, **kwargs) -> str:
    """Start a business transaction"""
    apm = await get_apm_system()
    return await apm.start_transaction(TransactionType.API_REQUEST, operation_name, **kwargs)


async def finish_transaction(transaction_id: str):
    """Finish a business transaction"""
    apm = await get_apm_system()
    await apm.finish_transaction(transaction_id)


async def trace_operation(operation_name: str, **kwargs) -> str:
    """Start tracing an operation"""
    apm = await get_apm_system()
    return await apm.start_trace(operation_name, **kwargs)


async def finish_trace_operation(span_id: str, **kwargs):
    """Finish tracing an operation"""
    apm = await get_apm_system()
    await apm.finish_trace(span_id, **kwargs)