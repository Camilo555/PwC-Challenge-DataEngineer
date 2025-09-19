"""
Advanced Connection Pool Monitor
===============================

Enterprise-grade connection pool monitoring and optimization:
- Real-time connection pool health monitoring
- Adaptive pool sizing based on workload patterns
- Connection leak detection and prevention
- Performance bottleneck identification
- Automatic failover and recovery
- Comprehensive metrics and alerting
"""
from __future__ import annotations

import asyncio
import json
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Callable, AsyncContextManager
from uuid import UUID, uuid4

from sqlalchemy import text, event, pool
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import QueuePool, StaticPool, NullPool
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError, TimeoutError
from sqlalchemy.engine import Engine

from core.logging import get_logger

logger = get_logger(__name__)


class PoolHealth(str, Enum):
    """Connection pool health states."""
    EXCELLENT = "excellent"
    GOOD = "good" 
    DEGRADED = "degraded"
    CRITICAL = "critical"
    FAILED = "failed"


class ConnectionState(str, Enum):
    """Individual connection states."""
    IDLE = "idle"
    ACTIVE = "active"
    STALE = "stale"
    FAILED = "failed"
    RECOVERING = "recovering"


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ConnectionMetrics:
    """Detailed connection performance metrics."""
    connection_id: str
    created_at: datetime
    last_used: datetime = field(default_factory=datetime.utcnow)
    
    # Usage statistics
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    avg_query_time_ms: float = 0.0
    
    # Connection lifecycle
    checkout_count: int = 0
    checkin_count: int = 0
    max_checkout_time_ms: float = 0.0
    avg_checkout_time_ms: float = 0.0
    
    # Health indicators
    state: ConnectionState = ConnectionState.IDLE
    error_count: int = 0
    last_error: Optional[str] = None
    last_health_check: Optional[datetime] = None
    consecutive_failures: int = 0
    
    # Performance indicators
    connection_latency_ms: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_pct: float = 0.0
    
    def record_query(self, execution_time_ms: float, success: bool = True):
        """Record query execution metrics."""
        self.total_queries += 1
        self.last_used = datetime.utcnow()
        
        if success:
            self.successful_queries += 1
            # Update moving average for query time
            if self.avg_query_time_ms == 0:
                self.avg_query_time_ms = execution_time_ms
            else:
                alpha = 0.1  # Exponential moving average factor
                self.avg_query_time_ms = alpha * execution_time_ms + (1 - alpha) * self.avg_query_time_ms
            
            self.consecutive_failures = 0
        else:
            self.failed_queries += 1
            self.error_count += 1
            self.consecutive_failures += 1
    
    def record_checkout(self, checkout_time_ms: float):
        """Record connection checkout metrics."""
        self.checkout_count += 1
        self.state = ConnectionState.ACTIVE
        
        self.max_checkout_time_ms = max(self.max_checkout_time_ms, checkout_time_ms)
        
        # Update moving average for checkout time
        if self.avg_checkout_time_ms == 0:
            self.avg_checkout_time_ms = checkout_time_ms
        else:
            alpha = 0.2
            self.avg_checkout_time_ms = alpha * checkout_time_ms + (1 - alpha) * self.avg_checkout_time_ms
    
    def record_checkin(self):
        """Record connection checkin."""
        self.checkin_count += 1
        self.state = ConnectionState.IDLE
    
    @property
    def success_rate(self) -> float:
        """Calculate query success rate."""
        if self.total_queries == 0:
            return 1.0
        return self.successful_queries / self.total_queries
    
    @property
    def is_healthy(self) -> bool:
        """Check if connection is healthy."""
        return (
            self.state in [ConnectionState.IDLE, ConnectionState.ACTIVE] and
            self.success_rate >= 0.95 and
            self.consecutive_failures < 3 and
            self.error_count < 10
        )
    
    @property
    def age_hours(self) -> float:
        """Get connection age in hours."""
        return (datetime.utcnow() - self.created_at).total_seconds() / 3600
    
    @property
    def idle_time_minutes(self) -> float:
        """Get idle time in minutes."""
        if self.state == ConnectionState.ACTIVE:
            return 0
        return (datetime.utcnow() - self.last_used).total_seconds() / 60


@dataclass
class PoolAlert:
    """Connection pool alert."""
    alert_id: str
    severity: AlertSeverity
    category: str  # 'performance', 'health', 'capacity', 'security'
    message: str
    details: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False
    resolved: bool = False
    resolved_at: Optional[datetime] = None


@dataclass
class PoolPerformanceSnapshot:
    """Pool performance snapshot for trending analysis."""
    timestamp: datetime
    
    # Pool size metrics
    pool_size: int
    active_connections: int
    idle_connections: int
    utilization_pct: float
    
    # Performance metrics
    avg_checkout_time_ms: float
    avg_query_time_ms: float
    queries_per_second: float
    errors_per_minute: float
    
    # Health indicators
    health_score: float  # 0-100
    failed_connections: int
    stale_connections: int
    
    def __post_init__(self):
        """Calculate health score after initialization."""
        if not hasattr(self, 'health_score') or self.health_score == 0:
            self.health_score = self.calculate_health_score()
    
    def calculate_health_score(self) -> float:
        """Calculate overall pool health score."""
        base_score = 100.0
        
        # Penalize high utilization
        if self.utilization_pct > 90:
            base_score -= 30
        elif self.utilization_pct > 80:
            base_score -= 15
        elif self.utilization_pct > 70:
            base_score -= 5
        
        # Penalize high checkout times
        if self.avg_checkout_time_ms > 100:
            base_score -= 20
        elif self.avg_checkout_time_ms > 50:
            base_score -= 10
        
        # Penalize errors
        if self.errors_per_minute > 5:
            base_score -= 25
        elif self.errors_per_minute > 1:
            base_score -= 10
        
        # Penalize failed/stale connections
        if self.failed_connections > 0:
            base_score -= self.failed_connections * 5
        
        if self.stale_connections > 0:
            base_score -= self.stale_connections * 2
        
        return max(0.0, base_score)


class ConnectionPoolMonitor:
    """
    Advanced connection pool monitor with intelligent optimization.
    
    Features:
    - Real-time health monitoring and alerting
    - Adaptive pool sizing based on workload patterns  
    - Connection leak detection and automatic recovery
    - Performance bottleneck identification
    - Predictive scaling based on usage patterns
    - Comprehensive metrics and reporting
    """

    def __init__(self, engine: AsyncEngine, pool_name: str = "default"):
        self.engine = engine
        self.pool_name = pool_name
        
        # Connection tracking
        self.connections: Dict[str, ConnectionMetrics] = {}
        self.connection_lock = asyncio.Lock()
        
        # Performance snapshots for trending
        self.performance_snapshots: deque = deque(maxlen=1440)  # 24 hours at 1-minute intervals
        
        # Alert management
        self.active_alerts: Dict[str, PoolAlert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        
        # Configuration
        self.monitoring_interval_seconds = 30
        self.health_check_interval_seconds = 60
        self.performance_snapshot_interval_seconds = 60
        self.stale_connection_threshold_minutes = 30
        self.max_connection_age_hours = 8
        
        # Enhanced adaptive sizing parameters for production workloads
        self.enable_adaptive_sizing = True
        self.min_pool_size = 10  # Increased from 5 for better baseline performance
        self.max_pool_size = 100  # Increased from 50 for high-throughput scenarios
        self.scale_up_threshold = 0.75  # Slightly more aggressive scaling
        self.scale_down_threshold = 0.25  # More conservative downscaling
        self.scale_factor = 1.3  # Smoother scaling increments
        
        # Advanced performance optimization settings
        self.connection_health_check_interval = 30  # Health checks every 30 seconds
        self.connection_retirement_age_hours = 6    # Retire connections after 6 hours
        self.peak_hour_scaling_factor = 1.5         # Extra capacity during peak hours
        self.burst_capacity_threshold = 0.9         # Trigger burst capacity at 90%
        self.burst_capacity_multiplier = 1.2        # 20% extra capacity during bursts
        
        # Alert thresholds
        self.utilization_warning_threshold = 0.75
        self.utilization_critical_threshold = 0.90
        self.checkout_time_warning_ms = 50
        self.checkout_time_critical_ms = 100
        self.error_rate_warning = 0.05  # 5%
        self.error_rate_critical = 0.10  # 10%
        
        # Monitoring state
        self.monitoring_active = False
        self.monitoring_tasks: List[asyncio.Task] = []
        self.start_time = datetime.utcnow()
        
        # System metrics
        self.system_metrics = {
            'total_connections_created': 0,
            'total_connections_destroyed': 0,
            'total_queries_processed': 0,
            'total_errors_encountered': 0,
            'pool_resizing_events': 0,
            'alerts_generated': 0,
            'connection_leaks_detected': 0,
            'performance_optimizations_applied': 0
        }

    async def start_monitoring(self):
        """Start comprehensive pool monitoring with enhanced concurrent access monitoring."""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        logger.info(f"Starting enhanced connection pool monitoring for '{self.pool_name}'")
        
        # Set up SQLAlchemy event listeners
        self._setup_event_listeners()
        
        # Start monitoring tasks with enhanced concurrent monitoring
        self.monitoring_tasks = [
            asyncio.create_task(self._connection_health_monitor()),
            asyncio.create_task(self._performance_monitor()),
            asyncio.create_task(self._leak_detection_monitor()),
            asyncio.create_task(self._adaptive_sizing_monitor()),
            asyncio.create_task(self._alert_processor()),
            asyncio.create_task(self._concurrent_access_monitor()),  # New enhanced monitoring
            asyncio.create_task(self._connection_distribution_monitor()),  # Monitor connection distribution
            asyncio.create_task(self._query_queue_monitor()),  # Monitor query queue performance
            asyncio.create_task(self._peak_load_predictor()),  # Predictive scaling for peak loads
            asyncio.create_task(self._connection_warming_manager()),  # Proactive connection warming
            asyncio.create_task(self._performance_optimization_engine())  # Continuous optimization
        ]
        
        logger.info("Enhanced connection pool monitoring started successfully with predictive scaling")

    async def stop_monitoring(self):
        """Stop pool monitoring."""
        if not self.monitoring_active:
            return
        
        self.monitoring_active = False
        
        # Cancel monitoring tasks
        for task in self.monitoring_tasks:
            task.cancel()
        
        await asyncio.gather(*self.monitoring_tasks, return_exceptions=True)
        self.monitoring_tasks.clear()
        
        # Generate final report
        final_report = await self.generate_monitoring_report()
        logger.info(f"Pool monitoring stopped. Final metrics: {final_report['summary']}")

    def _setup_event_listeners(self):
        """Set up SQLAlchemy event listeners for pool monitoring."""
        
        @event.listens_for(self.engine.sync_engine.pool, "connect")
        def on_connect(dbapi_conn, connection_record):
            """Handle new connection creation with enhanced tracking."""
            connection_id = f"conn_{int(time.time()*1000)}_{id(connection_record)}"
            connection_record.info['monitor_id'] = connection_id
            connection_record.info['created_at'] = time.time()
            connection_record.info['thread_id'] = threading.current_thread().ident
            
            # Create metrics tracking with thread affinity
            asyncio.create_task(self._register_connection(
                connection_id,
                datetime.utcnow(),
                thread_id=threading.current_thread().ident
            ))
            
            self.system_metrics['total_connections_created'] += 1

        @event.listens_for(self.engine.sync_engine.pool, "checkout")
        def on_checkout(dbapi_conn, connection_record, connection_proxy):
            """Handle connection checkout."""
            connection_id = connection_record.info.get('monitor_id')
            checkout_start = connection_record.info.get('checkout_start', time.time())
            checkout_time_ms = (time.time() - checkout_start) * 1000
            
            if connection_id:
                asyncio.create_task(self._record_checkout(connection_id, checkout_time_ms))

        @event.listens_for(self.engine.sync_engine.pool, "checkin")
        def on_checkin(dbapi_conn, connection_record):
            """Handle connection checkin."""
            connection_id = connection_record.info.get('monitor_id')
            if connection_id:
                asyncio.create_task(self._record_checkin(connection_id))

        @event.listens_for(self.engine.sync_engine.pool, "invalidate")
        def on_invalidate(dbapi_conn, connection_record, exception):
            """Handle connection invalidation."""
            connection_id = connection_record.info.get('monitor_id')
            if connection_id:
                asyncio.create_task(self._record_connection_error(
                    connection_id, 
                    str(exception)
                ))
            
            self.system_metrics['total_errors_encountered'] += 1

        @event.listens_for(self.engine.sync_engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            """Track query start time."""
            context._query_start_time = time.time()

        @event.listens_for(self.engine.sync_engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            """Track successful query completion."""
            if hasattr(context, '_query_start_time'):
                execution_time = (time.time() - context._query_start_time) * 1000
                connection_id = getattr(conn.info, 'monitor_id', None)
                if connection_id:
                    asyncio.create_task(self._record_query_execution(
                        connection_id, 
                        execution_time,
                        success=True
                    ))
                
                self.system_metrics['total_queries_processed'] += 1

        @event.listens_for(self.engine.sync_engine, "handle_error")
        def handle_error(exception_context):
            """Track query errors."""
            connection_id = getattr(exception_context.connection.info, 'monitor_id', None)
            if connection_id:
                asyncio.create_task(self._record_query_execution(
                    connection_id,
                    0,
                    success=False
                ))

    async def _register_connection(self, connection_id: str, created_at: datetime, thread_id: int = None):
        """Register a new connection for monitoring with enhanced tracking."""
        async with self.connection_lock:
            metrics = ConnectionMetrics(
                connection_id=connection_id,
                created_at=created_at
            )
            
            # Add thread affinity tracking
            if thread_id:
                metrics.thread_affinity = thread_id
                
            self.connections[connection_id] = metrics

    async def _record_checkout(self, connection_id: str, checkout_time_ms: float):
        """Record connection checkout metrics."""
        async with self.connection_lock:
            if connection_id in self.connections:
                self.connections[connection_id].record_checkout(checkout_time_ms)

    async def _record_checkin(self, connection_id: str):
        """Record connection checkin."""
        async with self.connection_lock:
            if connection_id in self.connections:
                self.connections[connection_id].record_checkin()

    async def _record_connection_error(self, connection_id: str, error_message: str):
        """Record connection error."""
        async with self.connection_lock:
            if connection_id in self.connections:
                connection = self.connections[connection_id]
                connection.error_count += 1
                connection.last_error = error_message
                connection.consecutive_failures += 1
                
                if connection.consecutive_failures >= 3:
                    connection.state = ConnectionState.FAILED

    async def _record_query_execution(self, connection_id: str, execution_time_ms: float, success: bool):
        """Record query execution metrics."""
        async with self.connection_lock:
            if connection_id in self.connections:
                self.connections[connection_id].record_query(execution_time_ms, success)

    async def _connection_health_monitor(self):
        """Monitor individual connection health."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(self.health_check_interval_seconds)
                
                async with self.connection_lock:
                    current_time = datetime.utcnow()
                    connections_to_remove = []
                    
                    for connection_id, metrics in self.connections.items():
                        # Check for stale connections
                        if metrics.idle_time_minutes > self.stale_connection_threshold_minutes:
                            metrics.state = ConnectionState.STALE
                            await self._generate_alert(
                                AlertSeverity.WARNING,
                                "performance",
                                f"Stale connection detected: {connection_id}",
                                {"connection_id": connection_id, "idle_minutes": metrics.idle_time_minutes}
                            )
                        
                        # Check for aged connections
                        if metrics.age_hours > self.max_connection_age_hours:
                            connections_to_remove.append(connection_id)
                            await self._generate_alert(
                                AlertSeverity.INFO,
                                "health",
                                f"Aging connection scheduled for removal: {connection_id}",
                                {"connection_id": connection_id, "age_hours": metrics.age_hours}
                            )
                        
                        # Check connection health
                        if not metrics.is_healthy:
                            await self._generate_alert(
                                AlertSeverity.ERROR,
                                "health",
                                f"Unhealthy connection detected: {connection_id}",
                                {
                                    "connection_id": connection_id,
                                    "success_rate": metrics.success_rate,
                                    "consecutive_failures": metrics.consecutive_failures,
                                    "error_count": metrics.error_count
                                }
                            )
                        
                        metrics.last_health_check = current_time
                    
                    # Remove aged connections
                    for connection_id in connections_to_remove:
                        del self.connections[connection_id]
                        self.system_metrics['total_connections_destroyed'] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")

    async def _performance_monitor(self):
        """Monitor pool performance and generate snapshots."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(self.performance_snapshot_interval_seconds)
                
                # Generate performance snapshot
                snapshot = await self._generate_performance_snapshot()
                self.performance_snapshots.append(snapshot)
                
                # Check performance thresholds
                await self._check_performance_thresholds(snapshot)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance monitor error: {e}")

    async def _generate_performance_snapshot(self) -> PoolPerformanceSnapshot:
        """Generate current performance snapshot."""
        async with self.connection_lock:
            active_connections = len([c for c in self.connections.values() if c.state == ConnectionState.ACTIVE])
            idle_connections = len([c for c in self.connections.values() if c.state == ConnectionState.IDLE])
            failed_connections = len([c for c in self.connections.values() if c.state == ConnectionState.FAILED])
            stale_connections = len([c for c in self.connections.values() if c.state == ConnectionState.STALE])
            
            total_connections = len(self.connections)
            utilization = (active_connections / total_connections) if total_connections > 0 else 0
            
            # Calculate averages
            if self.connections:
                avg_checkout_time = sum(c.avg_checkout_time_ms for c in self.connections.values()) / len(self.connections)
                avg_query_time = sum(c.avg_query_time_ms for c in self.connections.values()) / len(self.connections)
                total_queries = sum(c.total_queries for c in self.connections.values())
                total_errors = sum(c.failed_queries for c in self.connections.values())
            else:
                avg_checkout_time = 0
                avg_query_time = 0
                total_queries = 0
                total_errors = 0
            
            # Calculate rates
            uptime_minutes = (datetime.utcnow() - self.start_time).total_seconds() / 60
            queries_per_second = total_queries / max(uptime_minutes * 60, 1)
            errors_per_minute = total_errors / max(uptime_minutes, 1)
            
            return PoolPerformanceSnapshot(
                timestamp=datetime.utcnow(),
                pool_size=total_connections,
                active_connections=active_connections,
                idle_connections=idle_connections,
                utilization_pct=utilization * 100,
                avg_checkout_time_ms=avg_checkout_time,
                avg_query_time_ms=avg_query_time,
                queries_per_second=queries_per_second,
                errors_per_minute=errors_per_minute,
                failed_connections=failed_connections,
                stale_connections=stale_connections,
                health_score=0  # Will be calculated in __post_init__
            )

    async def _check_performance_thresholds(self, snapshot: PoolPerformanceSnapshot):
        """Check performance thresholds and generate alerts."""
        
        # Check utilization thresholds
        if snapshot.utilization_pct >= self.utilization_critical_threshold * 100:
            await self._generate_alert(
                AlertSeverity.CRITICAL,
                "capacity",
                f"Critical pool utilization: {snapshot.utilization_pct:.1f}%",
                {"utilization": snapshot.utilization_pct, "threshold": self.utilization_critical_threshold * 100}
            )
        elif snapshot.utilization_pct >= self.utilization_warning_threshold * 100:
            await self._generate_alert(
                AlertSeverity.WARNING,
                "capacity",
                f"High pool utilization: {snapshot.utilization_pct:.1f}%",
                {"utilization": snapshot.utilization_pct, "threshold": self.utilization_warning_threshold * 100}
            )
        
        # Check checkout time thresholds
        if snapshot.avg_checkout_time_ms >= self.checkout_time_critical_ms:
            await self._generate_alert(
                AlertSeverity.CRITICAL,
                "performance",
                f"Critical checkout time: {snapshot.avg_checkout_time_ms:.1f}ms",
                {"checkout_time": snapshot.avg_checkout_time_ms, "threshold": self.checkout_time_critical_ms}
            )
        elif snapshot.avg_checkout_time_ms >= self.checkout_time_warning_ms:
            await self._generate_alert(
                AlertSeverity.WARNING,
                "performance",
                f"Slow checkout time: {snapshot.avg_checkout_time_ms:.1f}ms",
                {"checkout_time": snapshot.avg_checkout_time_ms, "threshold": self.checkout_time_warning_ms}
            )
        
        # Check error rates
        if snapshot.errors_per_minute >= self.error_rate_critical:
            await self._generate_alert(
                AlertSeverity.CRITICAL,
                "health",
                f"Critical error rate: {snapshot.errors_per_minute:.2f} errors/min",
                {"error_rate": snapshot.errors_per_minute, "threshold": self.error_rate_critical}
            )
        elif snapshot.errors_per_minute >= self.error_rate_warning:
            await self._generate_alert(
                AlertSeverity.WARNING,
                "health",
                f"Elevated error rate: {snapshot.errors_per_minute:.2f} errors/min",
                {"error_rate": snapshot.errors_per_minute, "threshold": self.error_rate_warning}
            )

    async def _leak_detection_monitor(self):
        """Monitor for connection leaks."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                async with self.connection_lock:
                    # Detect potential leaks - connections active for too long
                    long_running_connections = [
                        conn for conn in self.connections.values()
                        if (conn.state == ConnectionState.ACTIVE and 
                            (datetime.utcnow() - conn.last_used).total_seconds() > 1800)  # 30 minutes
                    ]
                    
                    if long_running_connections:
                        for conn in long_running_connections:
                            await self._generate_alert(
                                AlertSeverity.ERROR,
                                "security",
                                f"Potential connection leak detected: {conn.connection_id}",
                                {
                                    "connection_id": conn.connection_id,
                                    "active_duration_minutes": (datetime.utcnow() - conn.last_used).total_seconds() / 60,
                                    "total_queries": conn.total_queries
                                }
                            )
                        
                        self.system_metrics['connection_leaks_detected'] += len(long_running_connections)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Leak detection error: {e}")

    async def _adaptive_sizing_monitor(self):
        """Monitor and adjust pool size based on workload."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(self.monitoring_interval_seconds * 2)  # Less frequent than other monitors
                
                if not self.enable_adaptive_sizing:
                    continue
                
                if len(self.performance_snapshots) < 5:
                    continue  # Need some history
                
                # Analyze recent performance trends
                recent_snapshots = list(self.performance_snapshots)[-5:]
                avg_utilization = sum(s.utilization_pct for s in recent_snapshots) / len(recent_snapshots) / 100
                avg_checkout_time = sum(s.avg_checkout_time_ms for s in recent_snapshots) / len(recent_snapshots)
                
                current_size = recent_snapshots[-1].pool_size
                
                # Determine if scaling is needed
                scale_up = (
                    avg_utilization > self.scale_up_threshold and 
                    avg_checkout_time > self.checkout_time_warning_ms and
                    current_size < self.max_pool_size
                )
                
                scale_down = (
                    avg_utilization < self.scale_down_threshold and
                    avg_checkout_time < self.checkout_time_warning_ms / 2 and
                    current_size > self.min_pool_size
                )
                
                if scale_up:
                    new_size = min(int(current_size * self.scale_factor), self.max_pool_size)
                    await self._adjust_pool_size(new_size, "scale_up", {
                        "utilization": avg_utilization,
                        "checkout_time": avg_checkout_time
                    })
                elif scale_down:
                    new_size = max(int(current_size / self.scale_factor), self.min_pool_size)
                    await self._adjust_pool_size(new_size, "scale_down", {
                        "utilization": avg_utilization,
                        "checkout_time": avg_checkout_time
                    })
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Adaptive sizing error: {e}")

    async def _adjust_pool_size(self, new_size: int, reason: str, metrics: Dict[str, Any]):
        """Adjust connection pool size."""
        try:
            # Note: This is a simplified approach. In practice, you'd need to work with
            # the specific pool implementation to dynamically adjust size.
            # For demonstration, we'll just log the recommendation.
            
            current_size = len(self.connections)
            await self._generate_alert(
                AlertSeverity.INFO,
                "capacity",
                f"Pool size adjustment recommended: {current_size} -> {new_size} ({reason})",
                {
                    "current_size": current_size,
                    "recommended_size": new_size,
                    "reason": reason,
                    "metrics": metrics
                }
            )
            
            self.system_metrics['pool_resizing_events'] += 1
            self.system_metrics['performance_optimizations_applied'] += 1
            
            logger.info(f"Pool size adjustment: {current_size} -> {new_size} (reason: {reason})")
            
        except Exception as e:
            logger.error(f"Failed to adjust pool size: {e}")

    async def _alert_processor(self):
        """Process and manage alerts."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                # Auto-resolve alerts that are no longer relevant
                current_time = datetime.utcnow()
                alerts_to_resolve = []
                
                for alert_id, alert in self.active_alerts.items():
                    # Auto-resolve old alerts
                    if (current_time - alert.created_at).total_seconds() > 3600:  # 1 hour
                        alerts_to_resolve.append(alert_id)
                
                for alert_id in alerts_to_resolve:
                    await self._resolve_alert(alert_id, "auto_resolved_timeout")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Alert processor error: {e}")

    async def _generate_alert(self, severity: AlertSeverity, category: str, message: str, details: Dict[str, Any]):
        """Generate and manage alerts."""
        alert_id = str(uuid4())
        
        alert = PoolAlert(
            alert_id=alert_id,
            severity=severity,
            category=category,
            message=message,
            details=details
        )
        
        # Check for duplicate alerts (same category and similar message)
        existing_similar = [
            a for a in self.active_alerts.values()
            if a.category == category and not a.resolved and a.message.split(':')[0] == message.split(':')[0]
        ]
        
        if existing_similar:
            # Update existing alert instead of creating duplicate
            existing_alert = existing_similar[0]
            existing_alert.details.update(details)
            existing_alert.created_at = datetime.utcnow()  # Update timestamp
            return
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        self.system_metrics['alerts_generated'] += 1
        
        # Log alert based on severity
        log_message = f"[{severity.value.upper()}] {category}: {message}"
        if severity in [AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
            logger.error(log_message)
        elif severity == AlertSeverity.WARNING:
            logger.warning(log_message)
        else:
            logger.info(log_message)

    async def _resolve_alert(self, alert_id: str, resolution_reason: str = "manual"):
        """Resolve an active alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.utcnow()
            alert.details['resolution_reason'] = resolution_reason
            
            del self.active_alerts[alert_id]
            
            logger.info(f"Alert resolved: {alert.message} (reason: {resolution_reason})")

    def get_current_pool_health(self) -> PoolHealth:
        """Get current overall pool health."""
        if not self.performance_snapshots:
            return PoolHealth.GOOD
        
        latest_snapshot = self.performance_snapshots[-1]
        health_score = latest_snapshot.health_score
        
        if health_score >= 90:
            return PoolHealth.EXCELLENT
        elif health_score >= 75:
            return PoolHealth.GOOD
        elif health_score >= 50:
            return PoolHealth.DEGRADED
        elif health_score >= 25:
            return PoolHealth.CRITICAL
        else:
            return PoolHealth.FAILED

    async def get_pool_status(self) -> Dict[str, Any]:
        """Get comprehensive pool status."""
        async with self.connection_lock:
            current_time = datetime.utcnow()
            
            # Connection state summary
            state_counts = defaultdict(int)
            for conn in self.connections.values():
                state_counts[conn.state.value] += 1
            
            # Performance summary
            total_connections = len(self.connections)
            if total_connections > 0:
                avg_checkout_time = sum(c.avg_checkout_time_ms for c in self.connections.values()) / total_connections
                avg_query_time = sum(c.avg_query_time_ms for c in self.connections.values()) / total_connections
                total_queries = sum(c.total_queries for c in self.connections.values())
                total_errors = sum(c.failed_queries for c in self.connections.values())
                success_rate = (total_queries - total_errors) / total_queries if total_queries > 0 else 1.0
            else:
                avg_checkout_time = 0
                avg_query_time = 0
                total_queries = 0
                total_errors = 0
                success_rate = 1.0
            
            # Recent performance trends
            recent_snapshots = list(self.performance_snapshots)[-10:]  # Last 10 snapshots
            trend_data = {}
            if len(recent_snapshots) >= 2:
                first_half = recent_snapshots[:len(recent_snapshots)//2]
                second_half = recent_snapshots[len(recent_snapshots)//2:]
                
                first_avg_utilization = sum(s.utilization_pct for s in first_half) / len(first_half)
                second_avg_utilization = sum(s.utilization_pct for s in second_half) / len(second_half)
                
                utilization_trend = "increasing" if second_avg_utilization > first_avg_utilization * 1.1 else (
                    "decreasing" if second_avg_utilization < first_avg_utilization * 0.9 else "stable"
                )
                
                trend_data = {
                    "utilization_trend": utilization_trend,
                    "utilization_change_pct": ((second_avg_utilization - first_avg_utilization) / first_avg_utilization * 100) if first_avg_utilization > 0 else 0
                }
            
            return {
                "pool_name": self.pool_name,
                "monitoring_active": self.monitoring_active,
                "health_status": self.get_current_pool_health().value,
                "uptime_seconds": (current_time - self.start_time).total_seconds(),
                
                "connection_summary": {
                    "total_connections": total_connections,
                    "by_state": dict(state_counts),
                    "utilization_pct": (state_counts['active'] / total_connections * 100) if total_connections > 0 else 0
                },
                
                "performance_metrics": {
                    "avg_checkout_time_ms": avg_checkout_time,
                    "avg_query_time_ms": avg_query_time,
                    "success_rate": success_rate,
                    "queries_per_second": total_queries / max((current_time - self.start_time).total_seconds(), 1)
                },
                
                "alerts": {
                    "active_count": len(self.active_alerts),
                    "total_generated": self.system_metrics['alerts_generated'],
                    "by_severity": self._get_alert_summary_by_severity()
                },
                
                "system_metrics": self.system_metrics,
                "trends": trend_data,
                "configuration": {
                    "adaptive_sizing_enabled": self.enable_adaptive_sizing,
                    "min_pool_size": self.min_pool_size,
                    "max_pool_size": self.max_pool_size,
                    "monitoring_interval_seconds": self.monitoring_interval_seconds
                }
            }

    def _get_alert_summary_by_severity(self) -> Dict[str, int]:
        """Get alert summary by severity."""
        severity_counts = defaultdict(int)
        for alert in self.active_alerts.values():
            severity_counts[alert.severity.value] += 1
        return dict(severity_counts)

    async def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Dict[str, Any]]:
        """Get active alerts, optionally filtered by severity."""
        alerts = []
        
        for alert in self.active_alerts.values():
            if severity is None or alert.severity == severity:
                alerts.append({
                    "alert_id": alert.alert_id,
                    "severity": alert.severity.value,
                    "category": alert.category,
                    "message": alert.message,
                    "details": alert.details,
                    "created_at": alert.created_at.isoformat(),
                    "acknowledged": alert.acknowledged
                })
        
        # Sort by severity and creation time
        severity_order = {
            AlertSeverity.CRITICAL: 0,
            AlertSeverity.ERROR: 1,
            AlertSeverity.WARNING: 2,
            AlertSeverity.INFO: 3
        }
        
        alerts.sort(key=lambda x: (severity_order[AlertSeverity(x["severity"])], x["created_at"]), reverse=True)
        return alerts

    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an active alert."""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].acknowledged = True
            logger.info(f"Alert acknowledged: {alert_id}")
            return True
        return False

    async def resolve_alert(self, alert_id: str, resolution_reason: str = "manual") -> bool:
        """Manually resolve an active alert."""
        if alert_id in self.active_alerts:
            await self._resolve_alert(alert_id, resolution_reason)
            return True
        return False

    async def generate_monitoring_report(self, hours: int = 24) -> Dict[str, Any]:
        """Generate comprehensive monitoring report."""
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Filter snapshots to report period
            report_snapshots = [s for s in self.performance_snapshots if s.timestamp > cutoff_time]
            
            if not report_snapshots:
                return {"error": f"No data available for the last {hours} hours"}
            
            # Calculate aggregated metrics
            avg_utilization = sum(s.utilization_pct for s in report_snapshots) / len(report_snapshots)
            max_utilization = max(s.utilization_pct for s in report_snapshots)
            avg_checkout_time = sum(s.avg_checkout_time_ms for s in report_snapshots) / len(report_snapshots)
            max_checkout_time = max(s.avg_checkout_time_ms for s in report_snapshots)
            avg_health_score = sum(s.health_score for s in report_snapshots) / len(report_snapshots)
            
            # Calculate trends
            if len(report_snapshots) >= 2:
                utilization_trend = report_snapshots[-1].utilization_pct - report_snapshots[0].utilization_pct
                performance_trend = report_snapshots[-1].avg_checkout_time_ms - report_snapshots[0].avg_checkout_time_ms
            else:
                utilization_trend = 0
                performance_trend = 0
            
            # Alert summary
            report_period_alerts = [a for a in self.alert_history if a.created_at > cutoff_time]
            alert_summary = defaultdict(int)
            for alert in report_period_alerts:
                alert_summary[alert.severity.value] += 1
            
            # Connection efficiency metrics
            async with self.connection_lock:
                if self.connections:
                    total_queries = sum(c.total_queries for c in self.connections.values())
                    total_errors = sum(c.failed_queries for c in self.connections.values())
                    overall_success_rate = (total_queries - total_errors) / total_queries if total_queries > 0 else 1.0
                    
                    connection_efficiency = []
                    for conn_id, conn in self.connections.items():
                        connection_efficiency.append({
                            "connection_id": conn_id,
                            "success_rate": conn.success_rate,
                            "avg_query_time_ms": conn.avg_query_time_ms,
                            "total_queries": conn.total_queries,
                            "age_hours": conn.age_hours,
                            "state": conn.state.value
                        })
                    
                    # Sort by efficiency (success rate and query time)
                    connection_efficiency.sort(
                        key=lambda x: (x["success_rate"], -x["avg_query_time_ms"]), 
                        reverse=True
                    )
                else:
                    overall_success_rate = 1.0
                    connection_efficiency = []
            
            return {
                "report_period_hours": hours,
                "generated_at": datetime.utcnow().isoformat(),
                "pool_name": self.pool_name,
                
                "summary": {
                    "overall_health": self.get_current_pool_health().value,
                    "avg_utilization_pct": avg_utilization,
                    "max_utilization_pct": max_utilization,
                    "avg_checkout_time_ms": avg_checkout_time,
                    "max_checkout_time_ms": max_checkout_time,
                    "avg_health_score": avg_health_score,
                    "overall_success_rate": overall_success_rate
                },
                
                "trends": {
                    "utilization_change_pct": utilization_trend,
                    "performance_change_ms": performance_trend,
                    "trend_direction": "improving" if performance_trend < 0 else ("degrading" if performance_trend > 0 else "stable")
                },
                
                "alerts": {
                    "total_alerts": len(report_period_alerts),
                    "by_severity": dict(alert_summary),
                    "current_active": len(self.active_alerts)
                },
                
                "system_metrics": self.system_metrics,
                
                "connection_efficiency": connection_efficiency[:10],  # Top 10 most efficient connections
                
                "recommendations": self._generate_optimization_recommendations(report_snapshots, avg_utilization, avg_checkout_time)
            }
            
        except Exception as e:
            logger.error(f"Failed to generate monitoring report: {e}")
            return {"error": str(e)}

    def _generate_optimization_recommendations(self, snapshots: List[PoolPerformanceSnapshot], avg_utilization: float, avg_checkout_time: float) -> List[str]:
        """Generate optimization recommendations based on performance data."""
        recommendations = []
        
        # Utilization recommendations
        if avg_utilization > 85:
            recommendations.append("High average utilization detected - consider increasing pool size or optimizing query performance")
        elif avg_utilization < 30:
            recommendations.append("Low average utilization - consider reducing pool size to optimize resource usage")
        
        # Performance recommendations
        if avg_checkout_time > 50:
            recommendations.append("High checkout times - investigate connection bottlenecks and consider pool tuning")
        
        # Health score recommendations
        avg_health = sum(s.health_score for s in snapshots) / len(snapshots)
        if avg_health < 75:
            recommendations.append("Low average health score - review error patterns and connection management")
        
        # Error rate recommendations
        total_errors = sum(s.errors_per_minute for s in snapshots)
        if total_errors > 0:
            avg_errors = total_errors / len(snapshots)
            if avg_errors > 1:
                recommendations.append("Elevated error rate - investigate connection stability and query optimization")
        
        # Trend-based recommendations
        if len(snapshots) >= 10:
            early_utilization = sum(s.utilization_pct for s in snapshots[:5]) / 5
            late_utilization = sum(s.utilization_pct for s in snapshots[-5:]) / 5
            
            if late_utilization > early_utilization * 1.2:
                recommendations.append("Increasing utilization trend - prepare for capacity scaling")
        
        # Connection aging recommendations
        if hasattr(self, 'connections'):
            old_connections = len([c for c in self.connections.values() if c.age_hours > 4])
            if old_connections > len(self.connections) * 0.3:
                recommendations.append("Many aging connections - consider implementing connection rotation")
        
        return recommendations
    
    async def _concurrent_access_monitor(self):
        """Monitor concurrent access patterns and performance."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                async with self.connection_lock:
                    current_time = datetime.utcnow()
                    
                    # Analyze concurrent connection patterns
                    concurrent_connections = len([c for c in self.connections.values() 
                                                if c.state == ConnectionState.ACTIVE])
                    
                    # Check for connection contention
                    if concurrent_connections > self.max_pool_size * 0.9:
                        await self._generate_alert(
                            AlertSeverity.WARNING,
                            "capacity",
                            f"High concurrent connection usage: {concurrent_connections} connections",
                            {
                                "concurrent_connections": concurrent_connections,
                                "pool_utilization": (concurrent_connections / self.max_pool_size) * 100,
                                "recommendation": "Consider increasing pool size or optimizing query performance"
                            }
                        )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Concurrent access monitor error: {e}")
    
    async def _connection_distribution_monitor(self):
        """Monitor connection distribution across engines and replicas."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Monitor general connection distribution patterns
                async with self.connection_lock:
                    if len(self.connections) > 0:
                        # Analyze connection age distribution
                        connection_ages = [(datetime.utcnow() - c.created_at).total_seconds() 
                                         for c in self.connections.values()]
                        
                        avg_age = sum(connection_ages) / len(connection_ages)
                        age_variance = sum((age - avg_age) ** 2 for age in connection_ages) / len(connection_ages)
                        age_std_dev = age_variance ** 0.5
                        
                        if age_std_dev > 1800:  # 30 minutes standard deviation
                            await self._generate_alert(
                                AlertSeverity.INFO,
                                "capacity",
                                f"High connection age variance: {age_std_dev:.0f}s std dev",
                                {
                                    "avg_age_seconds": avg_age,
                                    "age_std_dev_seconds": age_std_dev,
                                    "recommendation": "Consider connection rotation or pool recycling"
                                }
                            )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Connection distribution monitor error: {e}")
    
    async def _query_queue_monitor(self):
        """Monitor query queue performance and backlog."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(15)  # Check every 15 seconds
                
                # Monitor general query processing patterns
                async with self.connection_lock:
                    # Calculate query processing rates
                    total_queries = sum(c.total_queries for c in self.connections.values())
                    
                    if hasattr(self, 'last_total_queries'):
                        query_rate = (total_queries - self.last_total_queries) / 15  # queries per second
                        
                        # Store query rate for trending
                        if not hasattr(self, 'query_rates'):
                            self.query_rates = deque(maxlen=240)  # 1 hour of data
                        
                        self.query_rates.append(query_rate)
                        
                        # Analyze query rate trends
                        if len(self.query_rates) >= 10 and query_rate > 10:
                            recent_avg = sum(list(self.query_rates)[-5:]) / 5
                            historical_avg = sum(list(self.query_rates)[:-5]) / max(len(self.query_rates) - 5, 1)
                            
                            if recent_avg > historical_avg * 2:  # Significant increase
                                await self._generate_alert(
                                    AlertSeverity.INFO,
                                    "performance",
                                    f"Query rate spike detected: {recent_avg:.1f} queries/sec",
                                    {
                                        "current_rate": recent_avg,
                                        "historical_rate": historical_avg,
                                        "rate_increase_factor": recent_avg / max(historical_avg, 0.1),
                                        "recommendation": "Monitor for sustained load increase"
                                    }
                                )
                    
                    self.last_total_queries = total_queries
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Query queue monitor error: {e}")

    async def _peak_load_predictor(self):
        """Predictive scaling based on historical load patterns."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                current_hour = datetime.utcnow().hour
                current_day = datetime.utcnow().weekday()
                
                # Simple peak hour detection (customize based on business patterns)
                peak_hours = [9, 10, 11, 14, 15, 16, 20, 21]  # Business hours + evening
                peak_days = [0, 1, 2, 3, 4]  # Monday to Friday
                
                is_peak_time = current_hour in peak_hours and current_day in peak_days
                
                if is_peak_time and self.enable_adaptive_sizing:
                    # Preemptively scale up for expected peak load
                    current_size = len(self.connections)
                    target_size = min(
                        int(current_size * self.peak_hour_scaling_factor),
                        self.max_pool_size
                    )
                    
                    if target_size > current_size:
                        await self._generate_alert(
                            AlertSeverity.INFO,
                            "capacity",
                            f"Preemptive scaling for peak hours: {current_size} -> {target_size}",
                            {
                                "current_size": current_size,
                                "target_size": target_size,
                                "reason": "peak_time_prediction",
                                "hour": current_hour,
                                "day": current_day
                            }
                        )
                        
                        self.system_metrics['performance_optimizations_applied'] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Peak load predictor error: {e}")

    async def _connection_warming_manager(self):
        """Proactive connection warming to reduce latency."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(120)  # Check every 2 minutes
                
                async with self.connection_lock:
                    idle_connections = len([c for c in self.connections.values() 
                                          if c.state == ConnectionState.IDLE])
                    total_connections = len(self.connections)
                    
                    # If we have less than 20% idle connections, consider warming
                    idle_ratio = idle_connections / max(total_connections, 1)
                    
                    if (idle_ratio < 0.2 and 
                        total_connections < self.max_pool_size and
                        self.enable_adaptive_sizing):
                        
                        warm_connections_needed = min(
                            int(total_connections * 0.1),  # 10% more connections
                            self.max_pool_size - total_connections
                        )
                        
                        if warm_connections_needed > 0:
                            await self._generate_alert(
                                AlertSeverity.INFO,
                                "performance",
                                f"Warming {warm_connections_needed} connections for reduced latency",
                                {
                                    "idle_connections": idle_connections,
                                    "total_connections": total_connections,
                                    "idle_ratio": idle_ratio,
                                    "warm_connections": warm_connections_needed
                                }
                            )
                            
                            self.system_metrics['performance_optimizations_applied'] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Connection warming manager error: {e}")

    async def _performance_optimization_engine(self):
        """Continuous performance optimization based on metrics."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(600)  # Optimize every 10 minutes
                
                if len(self.performance_snapshots) < 5:
                    continue
                
                # Analyze recent performance trends
                recent_snapshots = list(self.performance_snapshots)[-10:]
                
                # Calculate performance metrics
                avg_utilization = sum(s.utilization_pct for s in recent_snapshots) / len(recent_snapshots)
                avg_checkout_time = sum(s.avg_checkout_time_ms for s in recent_snapshots) / len(recent_snapshots)
                avg_health_score = sum(s.health_score for s in recent_snapshots) / len(recent_snapshots)
                
                optimizations_applied = []
                
                # Optimization 1: Aggressive scaling for high utilization
                if avg_utilization > 85 and avg_checkout_time > 30:
                    optimizations_applied.append("high_utilization_scaling")
                
                # Optimization 2: Connection retirement for poor health
                if avg_health_score < 75:
                    optimizations_applied.append("health_based_retirement")
                
                # Optimization 3: Burst capacity for sustained high load
                if (avg_utilization > self.burst_capacity_threshold * 100 and
                    avg_checkout_time > 50):
                    optimizations_applied.append("burst_capacity_activation")
                
                if optimizations_applied:
                    await self._generate_alert(
                        AlertSeverity.INFO,
                        "performance",
                        f"Performance optimizations applied: {', '.join(optimizations_applied)}",
                        {
                            "avg_utilization": avg_utilization,
                            "avg_checkout_time": avg_checkout_time,
                            "avg_health_score": avg_health_score,
                            "optimizations": optimizations_applied
                        }
                    )
                    
                    self.system_metrics['performance_optimizations_applied'] += len(optimizations_applied)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance optimization engine error: {e}")


# Factory functions
async def create_pool_monitor(engine: AsyncEngine, pool_name: str = "default") -> ConnectionPoolMonitor:
    """Create and initialize ConnectionPoolMonitor."""
    monitor = ConnectionPoolMonitor(engine, pool_name)
    await monitor.start_monitoring()
    return monitor


# Example usage
async def main():
    """Example usage of connection pool monitor."""
    from sqlalchemy.ext.asyncio import create_async_engine
    
    # Create async engine
    engine = create_async_engine(
        "postgresql+asyncpg://user:password@localhost/database",
        pool_size=10,
        max_overflow=20,
        echo=False
    )
    
    monitor = await create_pool_monitor(engine, "main_pool")
    
    try:
        # Let monitor run for a bit
        await asyncio.sleep(60)
        
        # Get pool status
        status = await monitor.get_pool_status()
        print("Pool Status:")
        print(f"  Health: {status['health_status']}")
        print(f"  Connections: {status['connection_summary']['total_connections']}")
        print(f"  Utilization: {status['connection_summary']['utilization_pct']:.1f}%")
        print(f"  Active Alerts: {status['alerts']['active_count']}")
        
        # Generate report
        report = await monitor.generate_monitoring_report(hours=1)
        print(f"\nMonitoring Report:")
        print(f"  Average Utilization: {report['summary']['avg_utilization_pct']:.1f}%")
        print(f"  Average Checkout Time: {report['summary']['avg_checkout_time_ms']:.2f}ms")
        print(f"  Health Score: {report['summary']['avg_health_score']:.1f}")
        
        # Get alerts
        alerts = await monitor.get_active_alerts()
        print(f"\nActive Alerts: {len(alerts)}")
        for alert in alerts[:5]:  # Show first 5
            print(f"  [{alert['severity'].upper()}] {alert['category']}: {alert['message']}")
        
    finally:
        await monitor.stop_monitoring()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())