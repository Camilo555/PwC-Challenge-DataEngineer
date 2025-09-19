"""
Performance SLA Monitoring Middleware

Real-time performance monitoring middleware that tracks response times, validates SLA compliance,
and provides automated performance optimization suggestions.
"""

import asyncio
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from pathlib import Path

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from core.logging import get_logger
from core.caching.redis_cache_manager import get_cache_manager

logger = get_logger(__name__)


class SLAStatus(Enum):
    """SLA compliance status."""
    MEETING = "meeting"
    WARNING = "warning"
    VIOLATION = "violation"
    CRITICAL = "critical"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class PerformanceMetrics:
    """Performance metrics for a request."""
    request_id: str
    method: str
    path: str
    status_code: int
    response_time_ms: float
    timestamp: datetime
    user_agent: Optional[str] = None
    client_ip: Optional[str] = None
    query_params: Optional[Dict[str, Any]] = None
    response_size: Optional[int] = None
    cache_hit: bool = False
    db_query_time_ms: float = 0.0
    external_api_time_ms: float = 0.0
    cpu_usage_percent: float = 0.0
    memory_usage_mb: float = 0.0
    
    def is_slow(self, threshold_ms: float = 50.0) -> bool:
        """Check if request is slower than threshold."""
        return self.response_time_ms > threshold_ms


@dataclass
class SLAConfig:
    """SLA configuration."""
    target_p95_ms: float = 50.0  # 95th percentile target
    target_p99_ms: float = 100.0  # 99th percentile target
    warning_threshold_ms: float = 40.0
    critical_threshold_ms: float = 80.0
    max_error_rate: float = 0.01  # 1%
    measurement_window_minutes: int = 5
    min_requests_for_sla: int = 10


@dataclass
class EndpointSLA:
    """SLA configuration per endpoint."""
    path_pattern: str
    sla_config: SLAConfig
    custom_thresholds: Optional[Dict[str, float]] = None


@dataclass
class PerformanceAlert:
    """Performance alert."""
    alert_id: str
    severity: AlertSeverity
    message: str
    endpoint: str
    metrics: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False
    resolved: bool = False


class PerformanceAnalyzer:
    """Analyzes performance metrics and generates insights."""
    
    def __init__(self):
        self.response_times = defaultdict(lambda: deque(maxlen=1000))
        self.error_counts = defaultdict(int)
        self.request_counts = defaultdict(int)
        self.cache_hit_ratios = defaultdict(list)
        
    def analyze_metrics(self, metrics: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Analyze performance metrics and generate insights."""
        if not metrics:
            return {"status": "no_data"}
        
        # Calculate percentiles
        response_times = [m.response_time_ms for m in metrics]
        response_times.sort()
        
        n = len(response_times)
        if n == 0:
            return {"status": "no_data"}
        
        p50 = response_times[int(n * 0.5)]
        p95 = response_times[int(n * 0.95)]
        p99 = response_times[int(n * 0.99)]
        avg = sum(response_times) / n
        max_time = max(response_times)
        
        # Error analysis
        error_count = len([m for m in metrics if m.status_code >= 400])
        error_rate = error_count / n if n > 0 else 0
        
        # Cache analysis
        cache_hits = len([m for m in metrics if m.cache_hit])
        cache_hit_rate = cache_hits / n if n > 0 else 0
        
        # Slow requests analysis
        slow_requests = [m for m in metrics if m.is_slow()]
        
        return {
            "summary": {
                "total_requests": n,
                "avg_response_time_ms": avg,
                "p50_ms": p50,
                "p95_ms": p95,
                "p99_ms": p99,
                "max_response_time_ms": max_time,
                "error_rate": error_rate,
                "cache_hit_rate": cache_hit_rate,
                "slow_requests": len(slow_requests)
            },
            "slow_requests": [
                {
                    "path": m.path,
                    "method": m.method,
                    "response_time_ms": m.response_time_ms,
                    "status_code": m.status_code,
                    "timestamp": m.timestamp.isoformat()
                }
                for m in slow_requests[:10]  # Top 10 slowest
            ],
            "endpoint_breakdown": self._analyze_by_endpoint(metrics),
            "recommendations": self._generate_recommendations(metrics)
        }
    
    def _analyze_by_endpoint(self, metrics: List[PerformanceMetrics]) -> Dict[str, Dict[str, Any]]:
        """Analyze metrics by endpoint."""
        endpoint_metrics = defaultdict(list)
        
        for metric in metrics:
            endpoint_key = f"{metric.method}:{metric.path}"
            endpoint_metrics[endpoint_key].append(metric)
        
        analysis = {}
        for endpoint, endpoint_list in endpoint_metrics.items():
            times = [m.response_time_ms for m in endpoint_list]
            times.sort()
            n = len(times)
            
            if n > 0:
                analysis[endpoint] = {
                    "request_count": n,
                    "avg_response_time_ms": sum(times) / n,
                    "p95_ms": times[int(n * 0.95)] if n > 0 else 0,
                    "error_rate": len([m for m in endpoint_list if m.status_code >= 400]) / n,
                    "cache_hit_rate": len([m for m in endpoint_list if m.cache_hit]) / n
                }
        
        return analysis
    
    def _generate_recommendations(self, metrics: List[PerformanceMetrics]) -> List[str]:
        """Generate performance optimization recommendations."""
        recommendations = []
        
        # Analyze slow endpoints
        slow_metrics = [m for m in metrics if m.is_slow()]
        if len(slow_metrics) > len(metrics) * 0.1:  # More than 10% slow requests
            recommendations.append(
                "High percentage of slow requests detected. Consider implementing response caching "
                "or optimizing database queries."
            )
        
        # Cache hit rate analysis
        cache_hits = len([m for m in metrics if m.cache_hit])
        total_requests = len(metrics)
        cache_hit_rate = cache_hits / total_requests if total_requests > 0 else 0
        
        if cache_hit_rate < 0.3:  # Less than 30% cache hit rate
            recommendations.append(
                f"Low cache hit rate ({cache_hit_rate:.1%}). Consider implementing more "
                "aggressive caching strategies or longer TTL values."
            )
        
        # Database query time analysis
        db_heavy_requests = [m for m in metrics if m.db_query_time_ms > 20]
        if len(db_heavy_requests) > total_requests * 0.2:  # More than 20%
            recommendations.append(
                "High database query times detected. Consider adding database indexes, "
                "query optimization, or implementing query result caching."
            )
        
        # Error rate analysis
        errors = len([m for m in metrics if m.status_code >= 400])
        error_rate = errors / total_requests if total_requests > 0 else 0
        if error_rate > 0.05:  # More than 5% error rate
            recommendations.append(
                f"High error rate ({error_rate:.1%}) detected. Review error handling "
                "and implement proper retry mechanisms."
            )
        
        # External API dependency analysis
        external_api_requests = [m for m in metrics if m.external_api_time_ms > 0]
        if len(external_api_requests) > 0:
            avg_external_time = sum(m.external_api_time_ms for m in external_api_requests) / len(external_api_requests)
            if avg_external_time > 100:  # More than 100ms average
                recommendations.append(
                    "High external API response times detected. Consider implementing "
                    "circuit breakers, request timeouts, and response caching."
                )
        
        return recommendations


class PerformanceSLAMiddleware(BaseHTTPMiddleware):
    """Performance SLA monitoring middleware."""
    
    def __init__(self, app, sla_config: Optional[SLAConfig] = None,
                 endpoint_slas: Optional[List[EndpointSLA]] = None):
        super().__init__(app)
        self.sla_config = sla_config or SLAConfig()
        self.endpoint_slas = {sla.path_pattern: sla for sla in endpoint_slas or []}
        
        # Performance tracking
        self.metrics_buffer = deque(maxlen=10000)
        self.analyzer = PerformanceAnalyzer()
        
        # Alerts
        self.active_alerts = {}
        self.alert_history = deque(maxlen=1000)
        
        # SLA tracking per endpoint
        self.endpoint_metrics = defaultdict(lambda: deque(maxlen=1000))
        
        # Performance counters
        self.counters = {
            "total_requests": 0,
            "slow_requests": 0,
            "error_requests": 0,
            "cache_hits": 0,
            "sla_violations": 0
        }
        
    async def dispatch(self, request: Request, call_next):
        """Monitor request performance and SLA compliance."""
        request_id = str(uuid.uuid4())
        start_time = time.time()
        
        # Skip monitoring for health checks and static assets
        if self._should_skip_monitoring(request):
            return await call_next(request)
        
        # Add request ID to request state
        request.state.request_id = request_id
        request.state.start_time = start_time
        
        try:
            # Call the actual endpoint
            response = await call_next(request)
            
            # Calculate response time
            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000
            
            # Extract response size
            response_size = None
            if hasattr(response, 'body'):
                response_size = len(response.body) if response.body else 0
            
            # Check for cache hit
            cache_hit = response.headers.get("X-Cache") == "HIT"
            
            # Create performance metrics
            metrics = PerformanceMetrics(
                request_id=request_id,
                method=request.method,
                path=str(request.url.path),
                status_code=response.status_code,
                response_time_ms=response_time_ms,
                timestamp=datetime.utcnow(),
                user_agent=request.headers.get("user-agent"),
                client_ip=self._get_client_ip(request),
                query_params=dict(request.query_params) if request.query_params else None,
                response_size=response_size,
                cache_hit=cache_hit,
                db_query_time_ms=getattr(request.state, 'db_query_time_ms', 0),
                external_api_time_ms=getattr(request.state, 'external_api_time_ms', 0)
            )
            
            # Store metrics
            await self._store_metrics(metrics)
            
            # Update counters
            self._update_counters(metrics)
            
            # Check SLA compliance
            await self._check_sla_compliance(metrics)
            
            # Add performance headers
            response.headers["X-Response-Time"] = f"{response_time_ms:.2f}ms"
            response.headers["X-Request-ID"] = request_id
            
            if cache_hit:
                response.headers["X-Cache-Performance"] = "optimal"
            elif response_time_ms > self.sla_config.warning_threshold_ms:
                response.headers["X-Cache-Performance"] = "degraded"
            else:
                response.headers["X-Cache-Performance"] = "good"
            
            return response
            
        except Exception as e:
            # Handle errors
            end_time = time.time()
            response_time_ms = (end_time - start_time) * 1000
            
            error_metrics = PerformanceMetrics(
                request_id=request_id,
                method=request.method,
                path=str(request.url.path),
                status_code=500,
                response_time_ms=response_time_ms,
                timestamp=datetime.utcnow(),
                user_agent=request.headers.get("user-agent"),
                client_ip=self._get_client_ip(request)
            )
            
            await self._store_metrics(error_metrics)
            self._update_counters(error_metrics)
            
            # Generate error alert
            await self._generate_alert(
                AlertSeverity.ERROR,
                f"Request error on {request.method} {request.url.path}",
                error_metrics.path,
                {"error": str(e), "response_time_ms": response_time_ms}
            )
            
            raise
    
    def _should_skip_monitoring(self, request: Request) -> bool:
        """Check if request should be skipped from monitoring."""
        skip_paths = {
            "/health", "/metrics", "/favicon.ico", "/docs", "/redoc", "/openapi.json"
        }
        return str(request.url.path) in skip_paths
    
    def _get_client_ip(self, request: Request) -> str:
        """Get client IP address."""
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(',')[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
            
        return str(request.client.host) if request.client else "unknown"
    
    async def _store_metrics(self, metrics: PerformanceMetrics):
        """Store performance metrics."""
        # Add to in-memory buffer
        self.metrics_buffer.append(metrics)
        
        # Store per-endpoint metrics
        endpoint_key = f"{metrics.method}:{metrics.path}"
        self.endpoint_metrics[endpoint_key].append(metrics)
        
        # Persist to cache for analysis
        try:
            cache_manager = await get_cache_manager()
            cache_key = f"perf_metrics:{metrics.request_id}"
            
            await cache_manager.set(
                cache_key, 
                {
                    "request_id": metrics.request_id,
                    "method": metrics.method,
                    "path": metrics.path,
                    "status_code": metrics.status_code,
                    "response_time_ms": metrics.response_time_ms,
                    "timestamp": metrics.timestamp.isoformat(),
                    "cache_hit": metrics.cache_hit,
                    "response_size": metrics.response_size
                },
                ttl=3600,  # 1 hour
                namespace="performance"
            )
        except Exception as e:
            logger.error(f"Failed to store performance metrics: {e}")
    
    def _update_counters(self, metrics: PerformanceMetrics):
        """Update performance counters."""
        self.counters["total_requests"] += 1
        
        if metrics.is_slow():
            self.counters["slow_requests"] += 1
        
        if metrics.status_code >= 400:
            self.counters["error_requests"] += 1
        
        if metrics.cache_hit:
            self.counters["cache_hits"] += 1
    
    async def _check_sla_compliance(self, metrics: PerformanceMetrics):
        """Check SLA compliance and generate alerts if needed."""
        endpoint_key = f"{metrics.method}:{metrics.path}"
        
        # Get SLA config for this endpoint
        sla_config = self.sla_config
        for pattern, endpoint_sla in self.endpoint_slas.items():
            if pattern in metrics.path:
                sla_config = endpoint_sla.sla_config
                break
        
        # Check individual request thresholds
        if metrics.response_time_ms > sla_config.critical_threshold_ms:
            await self._generate_alert(
                AlertSeverity.CRITICAL,
                f"Critical response time: {metrics.response_time_ms:.2f}ms",
                metrics.path,
                {
                    "response_time_ms": metrics.response_time_ms,
                    "threshold_ms": sla_config.critical_threshold_ms,
                    "request_id": metrics.request_id
                }
            )
            self.counters["sla_violations"] += 1
            
        elif metrics.response_time_ms > sla_config.warning_threshold_ms:
            await self._generate_alert(
                AlertSeverity.WARNING,
                f"Warning response time: {metrics.response_time_ms:.2f}ms",
                metrics.path,
                {
                    "response_time_ms": metrics.response_time_ms,
                    "threshold_ms": sla_config.warning_threshold_ms,
                    "request_id": metrics.request_id
                }
            )
        
        # Check aggregated SLA compliance
        endpoint_metrics_list = list(self.endpoint_metrics[endpoint_key])
        if len(endpoint_metrics_list) >= sla_config.min_requests_for_sla:
            await self._check_aggregated_sla(endpoint_key, endpoint_metrics_list, sla_config)
    
    async def _check_aggregated_sla(self, endpoint_key: str, metrics_list: List[PerformanceMetrics], 
                                   sla_config: SLAConfig):
        """Check aggregated SLA compliance for an endpoint."""
        # Get recent metrics within measurement window
        cutoff_time = datetime.utcnow() - timedelta(minutes=sla_config.measurement_window_minutes)
        recent_metrics = [m for m in metrics_list if m.timestamp > cutoff_time]
        
        if len(recent_metrics) < sla_config.min_requests_for_sla:
            return
        
        # Calculate percentiles
        response_times = [m.response_time_ms for m in recent_metrics]
        response_times.sort()
        n = len(response_times)
        
        p95 = response_times[int(n * 0.95)]
        p99 = response_times[int(n * 0.99)]
        
        # Calculate error rate
        errors = len([m for m in recent_metrics if m.status_code >= 400])
        error_rate = errors / n
        
        # Check P95 SLA
        if p95 > sla_config.target_p95_ms:
            await self._generate_alert(
                AlertSeverity.ERROR,
                f"P95 SLA violation: {p95:.2f}ms > {sla_config.target_p95_ms}ms",
                endpoint_key,
                {
                    "p95_ms": p95,
                    "target_p95_ms": sla_config.target_p95_ms,
                    "sample_size": n,
                    "measurement_window_minutes": sla_config.measurement_window_minutes
                }
            )
        
        # Check P99 SLA
        if p99 > sla_config.target_p99_ms:
            await self._generate_alert(
                AlertSeverity.ERROR,
                f"P99 SLA violation: {p99:.2f}ms > {sla_config.target_p99_ms}ms",
                endpoint_key,
                {
                    "p99_ms": p99,
                    "target_p99_ms": sla_config.target_p99_ms,
                    "sample_size": n,
                    "measurement_window_minutes": sla_config.measurement_window_minutes
                }
            )
        
        # Check error rate SLA
        if error_rate > sla_config.max_error_rate:
            await self._generate_alert(
                AlertSeverity.ERROR,
                f"Error rate SLA violation: {error_rate:.1%} > {sla_config.max_error_rate:.1%}",
                endpoint_key,
                {
                    "error_rate": error_rate,
                    "max_error_rate": sla_config.max_error_rate,
                    "error_count": errors,
                    "sample_size": n
                }
            )
    
    async def _generate_alert(self, severity: AlertSeverity, message: str, 
                            endpoint: str, metrics: Dict[str, Any]):
        """Generate performance alert."""
        alert_id = str(uuid.uuid4())
        
        alert = PerformanceAlert(
            alert_id=alert_id,
            severity=severity,
            message=message,
            endpoint=endpoint,
            metrics=metrics
        )
        
        # Store alert
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # Log alert
        log_level = {
            AlertSeverity.INFO: logger.info,
            AlertSeverity.WARNING: logger.warning,
            AlertSeverity.ERROR: logger.error,
            AlertSeverity.CRITICAL: logger.critical
        }
        
        log_level[severity](f"Performance Alert [{severity.value.upper()}]: {message}")
        
        # Auto-resolve old alerts
        await self._cleanup_old_alerts()
    
    async def _cleanup_old_alerts(self):
        """Clean up old alerts."""
        cutoff_time = datetime.utcnow() - timedelta(hours=1)
        alerts_to_remove = []
        
        for alert_id, alert in self.active_alerts.items():
            if alert.timestamp < cutoff_time and not alert.acknowledged:
                alerts_to_remove.append(alert_id)
        
        for alert_id in alerts_to_remove:
            if alert_id in self.active_alerts:
                self.active_alerts[alert_id].resolved = True
                del self.active_alerts[alert_id]
    
    async def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary statistics."""
        if not self.metrics_buffer:
            return {"status": "no_data"}
        
        # Get recent metrics (last 5 minutes)
        cutoff_time = datetime.utcnow() - timedelta(minutes=5)
        recent_metrics = [m for m in self.metrics_buffer if m.timestamp > cutoff_time]
        
        if not recent_metrics:
            return {"status": "no_recent_data"}
        
        # Analyze metrics
        analysis = self.analyzer.analyze_metrics(recent_metrics)
        
        # Add SLA status
        p95 = analysis["summary"]["p95_ms"]
        sla_status = SLAStatus.MEETING
        
        if p95 > self.sla_config.critical_threshold_ms:
            sla_status = SLAStatus.CRITICAL
        elif p95 > self.sla_config.target_p95_ms:
            sla_status = SLAStatus.VIOLATION
        elif p95 > self.sla_config.warning_threshold_ms:
            sla_status = SLAStatus.WARNING
        
        return {
            "sla_status": sla_status.value,
            "sla_config": {
                "target_p95_ms": self.sla_config.target_p95_ms,
                "target_p99_ms": self.sla_config.target_p99_ms,
                "warning_threshold_ms": self.sla_config.warning_threshold_ms,
                "critical_threshold_ms": self.sla_config.critical_threshold_ms
            },
            "performance_analysis": analysis,
            "counters": self.counters.copy(),
            "active_alerts": len(self.active_alerts),
            "total_alerts": len(self.alert_history)
        }
    
    async def get_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Dict[str, Any]]:
        """Get performance alerts."""
        alerts = list(self.active_alerts.values())
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        return [
            {
                "alert_id": alert.alert_id,
                "severity": alert.severity.value,
                "message": alert.message,
                "endpoint": alert.endpoint,
                "metrics": alert.metrics,
                "timestamp": alert.timestamp.isoformat(),
                "acknowledged": alert.acknowledged
            }
            for alert in sorted(alerts, key=lambda x: x.timestamp, reverse=True)
        ]
    
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].acknowledged = True
            return True
        return False


# Factory function
def create_performance_sla_middleware(
    target_p95_ms: float = 50.0,
    warning_threshold_ms: float = 40.0,
    critical_threshold_ms: float = 80.0
) -> PerformanceSLAMiddleware:
    """Create performance SLA middleware with custom configuration."""
    
    sla_config = SLAConfig(
        target_p95_ms=target_p95_ms,
        warning_threshold_ms=warning_threshold_ms,
        critical_threshold_ms=critical_threshold_ms
    )
    
    return PerformanceSLAMiddleware(None, sla_config)


# Global middleware instance
_performance_middleware: Optional[PerformanceSLAMiddleware] = None


def get_performance_middleware() -> PerformanceSLAMiddleware:
    """Get or create global performance middleware instance."""
    global _performance_middleware
    if _performance_middleware is None:
        _performance_middleware = create_performance_sla_middleware()
    return _performance_middleware