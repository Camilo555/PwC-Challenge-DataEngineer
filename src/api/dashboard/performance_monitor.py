"""
Comprehensive Performance Monitoring for Business Intelligence Dashboard
Tracks dashboard performance, user interactions, and system metrics with real-time alerting
"""
import asyncio
import json
import time
import psutil
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging
from collections import defaultdict, deque

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import redis.asyncio as aioredis

from src.core.database.async_db_manager import get_async_db_session
from src.api.dashboard.dashboard_cache_manager import create_dashboard_cache_manager
from src.streaming.kafka_manager import create_kafka_manager, StreamingTopic
from core.config.unified_config import get_unified_config
from core.logging import get_logger


class MetricType(Enum):
    """Performance metric types"""
    RESPONSE_TIME = "response_time"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    CACHE_HIT_RATE = "cache_hit_rate"
    MEMORY_USAGE = "memory_usage"
    CPU_USAGE = "cpu_usage"
    DATABASE_CONNECTIONS = "database_connections"
    WEBSOCKET_CONNECTIONS = "websocket_connections"
    USER_INTERACTIONS = "user_interactions"
    DASHBOARD_LOADS = "dashboard_loads"


class AlertLevel(Enum):
    """Performance alert levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class PerformanceMetric:
    """Performance metric data structure"""
    name: str
    value: float
    timestamp: datetime
    metric_type: MetricType
    tags: Dict[str, str] = field(default_factory=dict)
    unit: str = "count"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "metric_type": self.metric_type.value,
            "tags": self.tags,
            "unit": self.unit
        }


@dataclass
class PerformanceAlert:
    """Performance alert data structure"""
    id: str
    name: str
    level: AlertLevel
    metric_name: str
    current_value: float
    threshold: float
    message: str
    timestamp: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DashboardSessionMetrics:
    """Dashboard user session metrics"""
    user_id: str
    session_id: str
    start_time: datetime
    last_activity: datetime
    dashboard_loads: int = 0
    interactions: int = 0
    total_response_time: float = 0.0
    errors: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    
    @property
    def avg_response_time(self) -> float:
        return self.total_response_time / max(self.dashboard_loads, 1)
    
    @property
    def session_duration(self) -> float:
        return (self.last_activity - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "session_duration": self.session_duration,
            "dashboard_loads": self.dashboard_loads,
            "interactions": self.interactions,
            "avg_response_time": self.avg_response_time,
            "errors": self.errors,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": self.cache_hits / max(self.cache_hits + self.cache_misses, 1) * 100
        }


class DashboardPerformanceMiddleware(BaseHTTPMiddleware):
    """Middleware to track dashboard API performance"""
    
    def __init__(self, app, performance_monitor):
        super().__init__(app)
        self.performance_monitor = performance_monitor
    
    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip monitoring for non-dashboard endpoints
        if not request.url.path.startswith("/api/dashboard"):
            return await call_next(request)
        
        start_time = time.time()
        
        try:
            response = await call_next(request)
            duration = time.time() - start_time
            
            # Track performance metrics
            await self.performance_monitor.record_api_call(
                endpoint=request.url.path,
                method=request.method,
                status_code=response.status_code,
                duration=duration,
                user_id=request.headers.get("X-User-ID"),
                session_id=request.headers.get("X-Session-ID")
            )
            
            return response
            
        except Exception as e:
            duration = time.time() - start_time
            
            await self.performance_monitor.record_api_call(
                endpoint=request.url.path,
                method=request.method,
                status_code=500,
                duration=duration,
                user_id=request.headers.get("X-User-ID"),
                session_id=request.headers.get("X-Session-ID"),
                error=str(e)
            )
            
            raise


class DashboardPerformanceMonitor:
    """
    Comprehensive performance monitoring for Business Intelligence Dashboard
    
    Tracks:
    - API response times and throughput
    - Cache performance
    - WebSocket connection metrics
    - User interaction patterns
    - System resource usage
    - Database performance
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.config = get_unified_config()
        
        # Metric storage (in-memory circular buffers)
        self.metrics_buffer_size = 10000
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.metrics_buffer_size))
        
        # Session tracking
        self.active_sessions: Dict[str, DashboardSessionMetrics] = {}
        
        # Real-time counters
        self.counters = {
            "total_requests": 0,
            "total_errors": 0,
            "total_cache_hits": 0,
            "total_cache_misses": 0,
            "active_websockets": 0,
            "total_dashboard_loads": 0
        }
        
        # Performance thresholds
        self.thresholds = {
            "response_time_ms": 2000,  # 2 seconds for Story 1.1 requirement
            "error_rate_percent": 1.0,
            "cache_hit_rate_percent": 85.0,
            "cpu_usage_percent": 80.0,
            "memory_usage_percent": 85.0,
            "websocket_connections": 1000
        }
        
        # Active alerts
        self.active_alerts: Dict[str, PerformanceAlert] = {}
        
        # Components
        self.dashboard_cache = create_dashboard_cache_manager()
        self.kafka_manager = create_kafka_manager()
        
        # Background tasks
        self._background_tasks = []
        
        # Start monitoring
        asyncio.create_task(self._start_monitoring())
    
    async def _start_monitoring(self):
        """Start background monitoring tasks"""
        tasks = [
            asyncio.create_task(self._collect_system_metrics()),
            asyncio.create_task(self._collect_database_metrics()),
            asyncio.create_task(self._check_performance_thresholds()),
            asyncio.create_task(self._publish_metrics()),
            asyncio.create_task(self._cleanup_old_sessions())
        ]
        self._background_tasks.extend(tasks)
        
        self.logger.info("Dashboard Performance Monitor started")
    
    async def record_api_call(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        duration: float,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        error: Optional[str] = None
    ):
        """Record API call performance metrics"""
        try:
            timestamp = datetime.now()
            
            # Record response time metric
            self.metrics["response_time"].append(PerformanceMetric(
                name="api_response_time",
                value=duration * 1000,  # Convert to milliseconds
                timestamp=timestamp,
                metric_type=MetricType.RESPONSE_TIME,
                tags={
                    "endpoint": endpoint,
                    "method": method,
                    "status_code": str(status_code)
                },
                unit="ms"
            ))
            
            # Update counters
            self.counters["total_requests"] += 1
            if status_code >= 400:
                self.counters["total_errors"] += 1
            
            # Track dashboard loads specifically
            if "dashboard" in endpoint.lower() and method == "GET":
                self.counters["total_dashboard_loads"] += 1
                
                # Update session metrics
                if session_id and user_id:
                    await self._update_session_metrics(
                        user_id, session_id, duration, status_code >= 400
                    )
            
            # Record throughput metric
            self.metrics["throughput"].append(PerformanceMetric(
                name="api_throughput",
                value=1,
                timestamp=timestamp,
                metric_type=MetricType.THROUGHPUT,
                tags={"endpoint": endpoint},
                unit="requests/min"
            ))
            
            # Log slow requests
            if duration > self.thresholds["response_time_ms"] / 1000:
                self.logger.warning(
                    f"Slow API call: {method} {endpoint} took {duration*1000:.1f}ms"
                )
            
        except Exception as e:
            self.logger.error(f"Error recording API call metrics: {e}")
    
    async def record_cache_hit(self, cache_name: str, key: str, hit: bool):
        """Record cache performance metrics"""
        try:
            timestamp = datetime.now()
            
            if hit:
                self.counters["total_cache_hits"] += 1
            else:
                self.counters["total_cache_misses"] += 1
            
            self.metrics["cache_performance"].append(PerformanceMetric(
                name="cache_operation",
                value=1 if hit else 0,
                timestamp=timestamp,
                metric_type=MetricType.CACHE_HIT_RATE,
                tags={
                    "cache_name": cache_name,
                    "hit": str(hit)
                },
                unit="boolean"
            ))
            
        except Exception as e:
            self.logger.error(f"Error recording cache metrics: {e}")
    
    async def record_websocket_connection(self, user_id: str, connected: bool):
        """Record WebSocket connection metrics"""
        try:
            if connected:
                self.counters["active_websockets"] += 1
            else:
                self.counters["active_websockets"] = max(0, self.counters["active_websockets"] - 1)
            
            self.metrics["websocket_connections"].append(PerformanceMetric(
                name="websocket_connections",
                value=self.counters["active_websockets"],
                timestamp=datetime.now(),
                metric_type=MetricType.WEBSOCKET_CONNECTIONS,
                tags={"user_id": user_id, "connected": str(connected)},
                unit="count"
            ))
            
        except Exception as e:
            self.logger.error(f"Error recording WebSocket metrics: {e}")
    
    async def record_user_interaction(self, user_id: str, session_id: str, interaction_type: str):
        """Record user interaction metrics"""
        try:
            timestamp = datetime.now()
            
            self.metrics["user_interactions"].append(PerformanceMetric(
                name="user_interaction",
                value=1,
                timestamp=timestamp,
                metric_type=MetricType.USER_INTERACTIONS,
                tags={
                    "user_id": user_id,
                    "session_id": session_id,
                    "type": interaction_type
                },
                unit="count"
            ))
            
            # Update session metrics
            if session_id in self.active_sessions:
                self.active_sessions[session_id].interactions += 1
                self.active_sessions[session_id].last_activity = timestamp
            
        except Exception as e:
            self.logger.error(f"Error recording user interaction: {e}")
    
    async def _update_session_metrics(
        self, 
        user_id: str, 
        session_id: str, 
        response_time: float, 
        is_error: bool
    ):
        """Update session-specific metrics"""
        try:
            if session_id not in self.active_sessions:
                self.active_sessions[session_id] = DashboardSessionMetrics(
                    user_id=user_id,
                    session_id=session_id,
                    start_time=datetime.now(),
                    last_activity=datetime.now()
                )
            
            session = self.active_sessions[session_id]
            session.last_activity = datetime.now()
            session.dashboard_loads += 1
            session.total_response_time += response_time
            
            if is_error:
                session.errors += 1
                
        except Exception as e:
            self.logger.error(f"Error updating session metrics: {e}")
    
    async def _collect_system_metrics(self):
        """Collect system resource metrics"""
        while True:
            try:
                timestamp = datetime.now()
                
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                self.metrics["cpu_usage"].append(PerformanceMetric(
                    name="cpu_usage",
                    value=cpu_percent,
                    timestamp=timestamp,
                    metric_type=MetricType.CPU_USAGE,
                    unit="percent"
                ))
                
                # Memory usage
                memory = psutil.virtual_memory()
                self.metrics["memory_usage"].append(PerformanceMetric(
                    name="memory_usage",
                    value=memory.percent,
                    timestamp=timestamp,
                    metric_type=MetricType.MEMORY_USAGE,
                    tags={
                        "total_gb": str(round(memory.total / 1024**3, 2)),
                        "available_gb": str(round(memory.available / 1024**3, 2))
                    },
                    unit="percent"
                ))
                
                await asyncio.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error collecting system metrics: {e}")
                await asyncio.sleep(60)
    
    async def _collect_database_metrics(self):
        """Collect database performance metrics"""
        while True:
            try:
                async with get_async_db_session() as session:
                    # Get active connections
                    connections_query = text("SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'")
                    result = await session.execute(connections_query)
                    active_connections = result.scalar()
                    
                    # Get slow queries count (queries taking >1 second)
                    slow_queries_query = text("""
                        SELECT COUNT(*) 
                        FROM pg_stat_activity 
                        WHERE state = 'active' 
                        AND query_start < NOW() - INTERVAL '1 second'
                        AND query NOT LIKE '%pg_stat_activity%'
                    """)
                    result = await session.execute(slow_queries_query)
                    slow_queries = result.scalar()
                    
                    timestamp = datetime.now()
                    
                    self.metrics["database_connections"].append(PerformanceMetric(
                        name="database_active_connections",
                        value=active_connections,
                        timestamp=timestamp,
                        metric_type=MetricType.DATABASE_CONNECTIONS,
                        unit="count"
                    ))
                    
                    self.metrics["database_slow_queries"].append(PerformanceMetric(
                        name="database_slow_queries",
                        value=slow_queries,
                        timestamp=timestamp,
                        metric_type=MetricType.RESPONSE_TIME,
                        tags={"type": "slow_queries"},
                        unit="count"
                    ))
                
                await asyncio.sleep(60)  # Collect every minute
                
            except Exception as e:
                self.logger.error(f"Error collecting database metrics: {e}")
                await asyncio.sleep(120)
    
    async def _check_performance_thresholds(self):
        """Check performance thresholds and trigger alerts"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Check response time threshold
                await self._check_response_time_threshold()
                
                # Check error rate threshold
                await self._check_error_rate_threshold()
                
                # Check cache hit rate threshold
                await self._check_cache_hit_rate_threshold()
                
                # Check system resource thresholds
                await self._check_system_thresholds()
                
            except Exception as e:
                self.logger.error(f"Error checking performance thresholds: {e}")
    
    async def _check_response_time_threshold(self):
        """Check API response time threshold"""
        try:
            if not self.metrics["response_time"]:
                return
            
            # Calculate average response time for last 5 minutes
            five_min_ago = datetime.now() - timedelta(minutes=5)
            recent_metrics = [
                m for m in self.metrics["response_time"] 
                if m.timestamp > five_min_ago
            ]
            
            if not recent_metrics:
                return
            
            avg_response_time = sum(m.value for m in recent_metrics) / len(recent_metrics)
            
            if avg_response_time > self.thresholds["response_time_ms"]:
                alert_id = "high_response_time"
                
                if alert_id not in self.active_alerts:
                    alert = PerformanceAlert(
                        id=alert_id,
                        name="High API Response Time",
                        level=AlertLevel.WARNING if avg_response_time < self.thresholds["response_time_ms"] * 1.5 else AlertLevel.ERROR,
                        metric_name="api_response_time",
                        current_value=avg_response_time,
                        threshold=self.thresholds["response_time_ms"],
                        message=f"Average response time {avg_response_time:.1f}ms exceeds threshold {self.thresholds['response_time_ms']}ms",
                        timestamp=datetime.now()
                    )
                    
                    self.active_alerts[alert_id] = alert
                    await self._send_alert(alert)
            else:
                # Resolve alert if it exists
                if "high_response_time" in self.active_alerts:
                    await self._resolve_alert("high_response_time")
                    
        except Exception as e:
            self.logger.error(f"Error checking response time threshold: {e}")
    
    async def _check_error_rate_threshold(self):
        """Check API error rate threshold"""
        try:
            if self.counters["total_requests"] == 0:
                return
            
            error_rate = (self.counters["total_errors"] / self.counters["total_requests"]) * 100
            
            if error_rate > self.thresholds["error_rate_percent"]:
                alert_id = "high_error_rate"
                
                if alert_id not in self.active_alerts:
                    alert = PerformanceAlert(
                        id=alert_id,
                        name="High Error Rate",
                        level=AlertLevel.ERROR,
                        metric_name="error_rate",
                        current_value=error_rate,
                        threshold=self.thresholds["error_rate_percent"],
                        message=f"Error rate {error_rate:.1f}% exceeds threshold {self.thresholds['error_rate_percent']}%",
                        timestamp=datetime.now()
                    )
                    
                    self.active_alerts[alert_id] = alert
                    await self._send_alert(alert)
            else:
                if "high_error_rate" in self.active_alerts:
                    await self._resolve_alert("high_error_rate")
                    
        except Exception as e:
            self.logger.error(f"Error checking error rate threshold: {e}")
    
    async def _check_cache_hit_rate_threshold(self):
        """Check cache hit rate threshold"""
        try:
            total_cache_ops = self.counters["total_cache_hits"] + self.counters["total_cache_misses"]
            if total_cache_ops == 0:
                return
            
            cache_hit_rate = (self.counters["total_cache_hits"] / total_cache_ops) * 100
            
            if cache_hit_rate < self.thresholds["cache_hit_rate_percent"]:
                alert_id = "low_cache_hit_rate"
                
                if alert_id not in self.active_alerts:
                    alert = PerformanceAlert(
                        id=alert_id,
                        name="Low Cache Hit Rate",
                        level=AlertLevel.WARNING,
                        metric_name="cache_hit_rate",
                        current_value=cache_hit_rate,
                        threshold=self.thresholds["cache_hit_rate_percent"],
                        message=f"Cache hit rate {cache_hit_rate:.1f}% below threshold {self.thresholds['cache_hit_rate_percent']}%",
                        timestamp=datetime.now()
                    )
                    
                    self.active_alerts[alert_id] = alert
                    await self._send_alert(alert)
            else:
                if "low_cache_hit_rate" in self.active_alerts:
                    await self._resolve_alert("low_cache_hit_rate")
                    
        except Exception as e:
            self.logger.error(f"Error checking cache hit rate threshold: {e}")
    
    async def _check_system_thresholds(self):
        """Check system resource thresholds"""
        try:
            # Check latest CPU and memory metrics
            if self.metrics["cpu_usage"]:
                latest_cpu = self.metrics["cpu_usage"][-1].value
                if latest_cpu > self.thresholds["cpu_usage_percent"]:
                    await self._create_or_update_alert(
                        "high_cpu_usage", "High CPU Usage", 
                        latest_cpu, self.thresholds["cpu_usage_percent"],
                        f"CPU usage {latest_cpu:.1f}% exceeds threshold"
                    )
                else:
                    if "high_cpu_usage" in self.active_alerts:
                        await self._resolve_alert("high_cpu_usage")
            
            if self.metrics["memory_usage"]:
                latest_memory = self.metrics["memory_usage"][-1].value
                if latest_memory > self.thresholds["memory_usage_percent"]:
                    await self._create_or_update_alert(
                        "high_memory_usage", "High Memory Usage",
                        latest_memory, self.thresholds["memory_usage_percent"],
                        f"Memory usage {latest_memory:.1f}% exceeds threshold"
                    )
                else:
                    if "high_memory_usage" in self.active_alerts:
                        await self._resolve_alert("high_memory_usage")
                        
        except Exception as e:
            self.logger.error(f"Error checking system thresholds: {e}")
    
    async def _create_or_update_alert(
        self, 
        alert_id: str, 
        name: str, 
        current_value: float, 
        threshold: float, 
        message: str
    ):
        """Create or update a performance alert"""
        if alert_id not in self.active_alerts:
            level = AlertLevel.CRITICAL if current_value > threshold * 1.5 else AlertLevel.WARNING
            
            alert = PerformanceAlert(
                id=alert_id,
                name=name,
                level=level,
                metric_name=alert_id,
                current_value=current_value,
                threshold=threshold,
                message=message,
                timestamp=datetime.now()
            )
            
            self.active_alerts[alert_id] = alert
            await self._send_alert(alert)
    
    async def _resolve_alert(self, alert_id: str):
        """Resolve an active alert"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.now()
            
            self.logger.info(f"Resolved alert: {alert.name}")
            
            # Remove from active alerts
            del self.active_alerts[alert_id]
    
    async def _send_alert(self, alert: PerformanceAlert):
        """Send performance alert"""
        try:
            # Log the alert
            self.logger.warning(f"PERFORMANCE ALERT [{alert.level.value.upper()}]: {alert.message}")
            
            # Send to Kafka for external processing
            await self.kafka_manager.produce_message(
                topic=StreamingTopic.NOTIFICATIONS,
                message=alert.to_dict(),
                key=f"performance_alert_{alert.id}"
            )
            
            # Send to dashboard (would integrate with WebSocket manager)
            # This would notify connected dashboard users
            
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
    
    async def _publish_metrics(self):
        """Publish metrics to external systems"""
        while True:
            try:
                await asyncio.sleep(60)  # Publish every minute
                
                # Aggregate recent metrics
                metrics_summary = self.get_performance_summary()
                
                # Send to Kafka
                await self.kafka_manager.produce_message(
                    topic=StreamingTopic.METRICS,
                    message=metrics_summary,
                    key="dashboard_performance"
                )
                
            except Exception as e:
                self.logger.error(f"Error publishing metrics: {e}")
    
    async def _cleanup_old_sessions(self):
        """Clean up old inactive sessions"""
        while True:
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes
                
                cutoff_time = datetime.now() - timedelta(hours=1)
                expired_sessions = [
                    session_id for session_id, session in self.active_sessions.items()
                    if session.last_activity < cutoff_time
                ]
                
                for session_id in expired_sessions:
                    del self.active_sessions[session_id]
                
                if expired_sessions:
                    self.logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
                    
            except Exception as e:
                self.logger.error(f"Error cleaning up sessions: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            # Calculate recent averages
            five_min_ago = datetime.now() - timedelta(minutes=5)
            
            # Response time metrics
            recent_response_times = [
                m.value for m in self.metrics["response_time"] 
                if m.timestamp > five_min_ago
            ]
            
            avg_response_time = sum(recent_response_times) / len(recent_response_times) if recent_response_times else 0
            p95_response_time = sorted(recent_response_times)[int(len(recent_response_times) * 0.95)] if recent_response_times else 0
            
            # Cache metrics
            total_cache_ops = self.counters["total_cache_hits"] + self.counters["total_cache_misses"]
            cache_hit_rate = (self.counters["total_cache_hits"] / total_cache_ops * 100) if total_cache_ops > 0 else 0
            
            # Error rate
            error_rate = (self.counters["total_errors"] / max(self.counters["total_requests"], 1)) * 100
            
            # System metrics
            latest_cpu = self.metrics["cpu_usage"][-1].value if self.metrics["cpu_usage"] else 0
            latest_memory = self.metrics["memory_usage"][-1].value if self.metrics["memory_usage"] else 0
            
            # Session metrics
            total_sessions = len(self.active_sessions)
            avg_session_duration = (
                sum(session.session_duration for session in self.active_sessions.values()) / total_sessions
                if total_sessions > 0 else 0
            )
            
            return {
                "timestamp": datetime.now().isoformat(),
                "response_time": {
                    "average_ms": round(avg_response_time, 2),
                    "p95_ms": round(p95_response_time, 2),
                    "threshold_ms": self.thresholds["response_time_ms"],
                    "within_sla": avg_response_time <= self.thresholds["response_time_ms"]
                },
                "throughput": {
                    "total_requests": self.counters["total_requests"],
                    "total_dashboard_loads": self.counters["total_dashboard_loads"],
                    "requests_per_minute": len([
                        m for m in self.metrics["throughput"] 
                        if m.timestamp > datetime.now() - timedelta(minutes=1)
                    ])
                },
                "errors": {
                    "total_errors": self.counters["total_errors"],
                    "error_rate_percent": round(error_rate, 2),
                    "threshold_percent": self.thresholds["error_rate_percent"],
                    "within_sla": error_rate <= self.thresholds["error_rate_percent"]
                },
                "cache": {
                    "hit_rate_percent": round(cache_hit_rate, 2),
                    "total_hits": self.counters["total_cache_hits"],
                    "total_misses": self.counters["total_cache_misses"],
                    "threshold_percent": self.thresholds["cache_hit_rate_percent"],
                    "within_sla": cache_hit_rate >= self.thresholds["cache_hit_rate_percent"]
                },
                "system": {
                    "cpu_usage_percent": round(latest_cpu, 2),
                    "memory_usage_percent": round(latest_memory, 2),
                    "active_websockets": self.counters["active_websockets"]
                },
                "sessions": {
                    "total_active": total_sessions,
                    "avg_duration_seconds": round(avg_session_duration, 2),
                    "total_interactions": sum(s.interactions for s in self.active_sessions.values())
                },
                "alerts": {
                    "active_count": len(self.active_alerts),
                    "active_alerts": [alert.to_dict() for alert in self.active_alerts.values()]
                },
                "sla_compliance": {
                    "response_time_sla": avg_response_time <= self.thresholds["response_time_ms"],
                    "error_rate_sla": error_rate <= self.thresholds["error_rate_percent"],
                    "cache_hit_rate_sla": cache_hit_rate >= self.thresholds["cache_hit_rate_percent"],
                    "overall_health_score": self._calculate_health_score(
                        avg_response_time, error_rate, cache_hit_rate, latest_cpu, latest_memory
                    )
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error generating performance summary: {e}")
            return {"error": "Failed to generate performance summary"}
    
    def _calculate_health_score(
        self, 
        avg_response_time: float, 
        error_rate: float, 
        cache_hit_rate: float, 
        cpu_usage: float, 
        memory_usage: float
    ) -> float:
        """Calculate overall system health score (0-100)"""
        try:
            scores = []
            
            # Response time score (0-25 points)
            response_score = max(0, 25 - (avg_response_time / self.thresholds["response_time_ms"] * 25))
            scores.append(response_score)
            
            # Error rate score (0-25 points)
            error_score = max(0, 25 - (error_rate / self.thresholds["error_rate_percent"] * 25))
            scores.append(error_score)
            
            # Cache hit rate score (0-25 points)
            cache_score = min(25, (cache_hit_rate / self.thresholds["cache_hit_rate_percent"] * 25))
            scores.append(cache_score)
            
            # System resource score (0-25 points)
            cpu_score = max(0, 12.5 - (cpu_usage / self.thresholds["cpu_usage_percent"] * 12.5))
            memory_score = max(0, 12.5 - (memory_usage / self.thresholds["memory_usage_percent"] * 12.5))
            system_score = cpu_score + memory_score
            scores.append(system_score)
            
            return round(sum(scores), 1)
            
        except Exception:
            return 0.0
    
    def get_session_metrics(self) -> Dict[str, Any]:
        """Get current session metrics"""
        return {
            "active_sessions": len(self.active_sessions),
            "sessions": [session.to_dict() for session in self.active_sessions.values()],
            "total_interactions": sum(s.interactions for s in self.active_sessions.values()),
            "avg_session_duration": (
                sum(s.session_duration for s in self.active_sessions.values()) / 
                max(len(self.active_sessions), 1)
            )
        }
    
    async def close(self):
        """Close the performance monitor"""
        try:
            # Cancel background tasks
            for task in self._background_tasks:
                task.cancel()
            
            # Close components
            await self.dashboard_cache.close()
            self.kafka_manager.close()
            
            self.logger.info("Dashboard Performance Monitor closed")
            
        except Exception as e:
            self.logger.error(f"Error closing performance monitor: {e}")


# Global performance monitor instance
performance_monitor = DashboardPerformanceMonitor()

# Factory function
def create_dashboard_performance_monitor() -> DashboardPerformanceMonitor:
    """Create DashboardPerformanceMonitor instance"""
    return performance_monitor

# Middleware factory
def get_dashboard_performance_middleware():
    """Get dashboard performance middleware"""
    return lambda app: DashboardPerformanceMiddleware(app, performance_monitor)


# Export components
__all__ = [
    "DashboardPerformanceMonitor",
    "DashboardPerformanceMiddleware", 
    "PerformanceMetric",
    "PerformanceAlert",
    "create_dashboard_performance_monitor",
    "get_dashboard_performance_middleware",
    "performance_monitor"
]