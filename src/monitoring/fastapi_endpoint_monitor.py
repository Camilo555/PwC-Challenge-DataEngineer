"""
Comprehensive FastAPI Endpoint Monitoring
Advanced monitoring system for FastAPI endpoints with <15ms SLA tracking,
request/response analysis, and intelligent performance optimization.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.parse import urlparse
import uuid

import psutil
from fastapi import FastAPI, Request, Response
from fastapi.middleware.base import BaseHTTPMiddleware
from prometheus_client import Counter, Gauge, Histogram, Summary
from starlette.middleware.base import RequestResponseEndpoint
from starlette.types import ASGIApp

from core.logging import get_logger
from monitoring.enterprise_prometheus_metrics import enterprise_metrics
from monitoring.comprehensive_distributed_tracing import distributed_tracer, SpanType

logger = get_logger(__name__)


class SLATier(Enum):
    """SLA performance tiers."""
    ULTRA_FAST = "ultra_fast"      # <5ms
    CRITICAL = "critical"           # <15ms
    STANDARD = "standard"           # <100ms
    ACCEPTABLE = "acceptable"       # <500ms
    SLOW = "slow"                   # >=500ms


class RequestType(Enum):
    """Types of API requests."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    SEARCH = "search"
    ANALYTICS = "analytics"
    AUTHENTICATION = "authentication"
    HEALTH_CHECK = "health_check"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class RequestMetrics:
    """Metrics for individual API requests."""
    request_id: str
    endpoint: str
    method: str
    path_params: Dict[str, str]
    query_params: Dict[str, str]
    headers: Dict[str, str]
    
    # Timing metrics
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration_ms: float = 0.0
    processing_duration_ms: float = 0.0
    db_duration_ms: float = 0.0
    cache_duration_ms: float = 0.0
    
    # Response metrics
    status_code: Optional[int] = None
    response_size_bytes: int = 0
    request_size_bytes: int = 0
    
    # Performance classification
    sla_tier: Optional[SLATier] = None
    sla_compliant: bool = True
    
    # Business context
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    client_ip: str = ""
    user_agent: str = ""
    request_type: Optional[RequestType] = None
    
    # Resource usage
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Error tracking
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    stack_trace: Optional[str] = None


@dataclass
class EndpointMetrics:
    """Aggregated metrics for API endpoints."""
    endpoint_pattern: str
    method: str
    
    # Request counts
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    
    # SLA compliance tracking
    ultra_fast_requests: int = 0      # <5ms
    critical_sla_requests: int = 0    # <15ms
    standard_requests: int = 0        # <100ms
    slow_requests: int = 0            # >=100ms
    
    # Timing statistics
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0.0
    avg_duration_ms: float = 0.0
    p50_duration_ms: float = 0.0
    p95_duration_ms: float = 0.0
    p99_duration_ms: float = 0.0
    
    # Throughput metrics
    requests_per_second: float = 0.0
    peak_rps: float = 0.0
    
    # Error analysis
    error_rate: float = 0.0
    error_types: Dict[str, int] = field(default_factory=dict)
    
    # Resource efficiency
    avg_memory_usage_mb: float = 0.0
    avg_cpu_usage_percent: float = 0.0
    
    # Business metrics
    unique_users: Set[str] = field(default_factory=set)
    geographic_distribution: Dict[str, int] = field(default_factory=dict)
    
    # Cache performance
    cache_hit_rate: float = 0.0
    cache_miss_count: int = 0
    cache_hit_count: int = 0
    
    # Recent activity
    recent_requests: deque[RequestMetrics] = field(default_factory=lambda: deque(maxlen=100))
    last_activity: Optional[datetime] = None


@dataclass
class APIHealth:
    """Overall API health metrics."""
    # SLA compliance
    overall_sla_compliance: float = 100.0
    critical_sla_compliance: float = 100.0  # <15ms compliance
    ultra_fast_compliance: float = 100.0    # <5ms compliance
    
    # Performance metrics
    avg_response_time_ms: float = 0.0
    p95_response_time_ms: float = 0.0
    p99_response_time_ms: float = 0.0
    
    # Throughput
    total_rps: float = 0.0
    peak_rps: float = 0.0
    
    # Error tracking
    overall_error_rate: float = 0.0
    critical_error_rate: float = 0.0  # 5xx errors
    
    # Resource utilization
    avg_memory_usage_mb: float = 0.0
    peak_memory_usage_mb: float = 0.0
    avg_cpu_usage: float = 0.0
    
    # Business metrics
    active_users: int = 0
    total_endpoints: int = 0
    healthy_endpoints: int = 0
    
    # Availability
    uptime_percentage: float = 100.0
    last_downtime: Optional[datetime] = None


class ComprehensiveFastAPIMonitor:
    """Advanced FastAPI monitoring system with <15ms SLA focus."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Request tracking
        self.active_requests: Dict[str, RequestMetrics] = {}
        self.completed_requests: deque[RequestMetrics] = deque(maxlen=10000)
        self.request_history: deque = deque(maxlen=50000)
        
        # Endpoint metrics
        self.endpoint_metrics: Dict[str, EndpointMetrics] = {}
        
        # API health
        self.api_health = APIHealth()
        self.health_history: deque[APIHealth] = deque(maxlen=1440)  # 24 hours of minute-by-minute data
        
        # Performance thresholds
        self.sla_thresholds = {
            SLATier.ULTRA_FAST: 5.0,      # 5ms
            SLATier.CRITICAL: 15.0,       # 15ms
            SLATier.STANDARD: 100.0,      # 100ms
            SLATier.ACCEPTABLE: 500.0,    # 500ms
        }
        
        # Alert thresholds
        self.alert_thresholds = self._initialize_alert_thresholds()
        
        # Pattern matching for endpoint grouping
        self.endpoint_patterns = self._initialize_endpoint_patterns()
        
        # Performance optimization suggestions
        self.optimization_suggestions: Dict[str, List[str]] = defaultdict(list)
        
    def _initialize_alert_thresholds(self) -> Dict[str, Any]:
        """Initialize alert thresholds for API monitoring."""
        return {
            "critical_sla_compliance": 95.0,     # 95% <15ms SLA compliance
            "overall_error_rate": 1.0,           # 1% overall error rate
            "critical_error_rate": 0.1,          # 0.1% 5xx error rate
            "response_time_p95": 50.0,           # 95th percentile <50ms
            "response_time_p99": 100.0,          # 99th percentile <100ms
            "throughput_degradation": 30.0,      # 30% throughput degradation
            "memory_usage_threshold": 1024.0,    # 1GB memory usage
            "cpu_usage_threshold": 80.0,         # 80% CPU usage
            "endpoint_error_rate": 5.0,          # 5% endpoint error rate
            "slow_request_threshold": 1000.0,    # 1 second slow request threshold
            "queue_length_threshold": 100,       # Request queue length
            "cache_hit_rate_threshold": 90.0     # 90% cache hit rate
        }
    
    def _initialize_endpoint_patterns(self) -> Dict[str, str]:
        """Initialize endpoint patterns for grouping similar endpoints."""
        return {
            r'/api/v1/data/\d+': '/api/v1/data/{id}',
            r'/api/v1/users/[^/]+': '/api/v1/users/{user_id}',
            r'/api/v1/analytics/dashboard/[^/]+': '/api/v1/analytics/dashboard/{dashboard_id}',
            r'/api/v1/ml/models/[^/]+/predict': '/api/v1/ml/models/{model_id}/predict',
            r'/api/v1/etl/jobs/[^/]+': '/api/v1/etl/jobs/{job_id}',
            r'/api/v1/files/[^/]+': '/api/v1/files/{file_id}',
        }
    
    def _classify_sla_tier(self, duration_ms: float) -> SLATier:
        """Classify request into SLA tier based on duration."""
        if duration_ms < self.sla_thresholds[SLATier.ULTRA_FAST]:
            return SLATier.ULTRA_FAST
        elif duration_ms < self.sla_thresholds[SLATier.CRITICAL]:
            return SLATier.CRITICAL
        elif duration_ms < self.sla_thresholds[SLATier.STANDARD]:
            return SLATier.STANDARD
        elif duration_ms < self.sla_thresholds[SLATier.ACCEPTABLE]:
            return SLATier.ACCEPTABLE
        else:
            return SLATier.SLOW
    
    def _classify_request_type(self, method: str, path: str) -> RequestType:
        """Classify request type based on method and path."""
        method_upper = method.upper()
        path_lower = path.lower()
        
        # Health checks
        if 'health' in path_lower or 'ping' in path_lower:
            return RequestType.HEALTH_CHECK
        
        # Authentication
        if 'auth' in path_lower or 'login' in path_lower or 'token' in path_lower:
            return RequestType.AUTHENTICATION
        
        # Analytics
        if 'analytics' in path_lower or 'dashboard' in path_lower or 'report' in path_lower:
            return RequestType.ANALYTICS
        
        # Search
        if 'search' in path_lower or 'query' in path_lower:
            return RequestType.SEARCH
        
        # Method-based classification
        if method_upper == 'GET':
            return RequestType.READ
        elif method_upper in ['POST', 'PUT', 'PATCH']:
            return RequestType.WRITE
        elif method_upper == 'DELETE':
            return RequestType.DELETE
        
        return RequestType.READ  # Default
    
    def _normalize_endpoint(self, path: str) -> str:
        """Normalize endpoint path for grouping."""
        import re
        
        for pattern, normalized in self.endpoint_patterns.items():
            if re.match(pattern, path):
                return normalized
        
        return path
    
    def _extract_user_context(self, request: Request) -> Dict[str, Any]:
        """Extract user context from request."""
        context = {}
        
        # Extract user ID from headers or auth
        if hasattr(request.state, 'user_id'):
            context['user_id'] = str(request.state.user_id)
        elif 'x-user-id' in request.headers:
            context['user_id'] = request.headers['x-user-id']
        
        # Extract session ID
        if hasattr(request.state, 'session_id'):
            context['session_id'] = str(request.state.session_id)
        elif 'x-session-id' in request.headers:
            context['session_id'] = request.headers['x-session-id']
        
        # Client information
        context['client_ip'] = request.client.host if request.client else 'unknown'
        context['user_agent'] = request.headers.get('user-agent', 'unknown')
        
        return context
    
    async def start_request_tracking(self, request: Request) -> str:
        """Start tracking a new request."""
        
        request_id = str(uuid.uuid4())
        user_context = self._extract_user_context(request)
        request_type = self._classify_request_type(request.method, str(request.url.path))
        
        # Calculate request size
        request_size = 0
        if hasattr(request, '_body'):
            request_size = len(request._body)
        elif 'content-length' in request.headers:
            try:
                request_size = int(request.headers['content-length'])
            except ValueError:
                pass
        
        request_metrics = RequestMetrics(
            request_id=request_id,
            endpoint=str(request.url.path),
            method=request.method,
            path_params=dict(request.path_params),
            query_params=dict(request.query_params),
            headers=dict(request.headers),
            start_time=datetime.utcnow(),
            request_size_bytes=request_size,
            client_ip=user_context.get('client_ip', ''),
            user_agent=user_context.get('user_agent', ''),
            user_id=user_context.get('user_id'),
            session_id=user_context.get('session_id'),
            request_type=request_type
        )
        
        self.active_requests[request_id] = request_metrics
        
        # Set request ID in request state for later reference
        request.state.monitoring_request_id = request_id
        
        return request_id
    
    async def finish_request_tracking(
        self,
        request_id: str,
        response: Response,
        error: Optional[Exception] = None
    ):
        """Finish tracking a request."""
        
        if request_id not in self.active_requests:
            return
        
        request_metrics = self.active_requests[request_id]
        request_metrics.end_time = datetime.utcnow()
        request_metrics.total_duration_ms = (
            request_metrics.end_time - request_metrics.start_time
        ).total_seconds() * 1000
        
        # Response metrics
        request_metrics.status_code = response.status_code
        if hasattr(response, 'headers') and 'content-length' in response.headers:
            try:
                request_metrics.response_size_bytes = int(response.headers['content-length'])
            except ValueError:
                pass
        
        # Error tracking
        if error:
            request_metrics.error_message = str(error)
            request_metrics.error_type = type(error).__name__
            if hasattr(error, '__traceback__'):
                import traceback
                request_metrics.stack_trace = ''.join(traceback.format_tb(error.__traceback__))
        
        # SLA classification
        request_metrics.sla_tier = self._classify_sla_tier(request_metrics.total_duration_ms)
        request_metrics.sla_compliant = (
            request_metrics.sla_tier in [SLATier.ULTRA_FAST, SLATier.CRITICAL]
        )
        
        # Resource usage (simplified - would need proper monitoring in production)
        try:
            process = psutil.Process()
            request_metrics.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            request_metrics.cpu_usage_percent = process.cpu_percent()
        except Exception:
            pass  # Ignore resource monitoring errors
        
        # Update endpoint metrics
        await self._update_endpoint_metrics(request_metrics)
        
        # Update Prometheus metrics
        self._update_prometheus_metrics(request_metrics)
        
        # Add to completed requests and history
        self.completed_requests.append(request_metrics)
        self.request_history.append({
            'timestamp': request_metrics.end_time,
            'endpoint': request_metrics.endpoint,
            'method': request_metrics.method,
            'duration_ms': request_metrics.total_duration_ms,
            'status_code': request_metrics.status_code,
            'sla_tier': request_metrics.sla_tier.value,
            'sla_compliant': request_metrics.sla_compliant,
            'user_id': request_metrics.user_id,
            'request_type': request_metrics.request_type.value if request_metrics.request_type else None,
            'error': error is not None
        })
        
        # Remove from active requests
        del self.active_requests[request_id]
        
        # Start distributed tracing
        distributed_tracer.trace_http_request(
            method=request_metrics.method,
            url=request_metrics.endpoint,
            response_status=request_metrics.status_code,
            response_size=request_metrics.response_size_bytes,
            duration_ms=request_metrics.total_duration_ms
        )
        
        self.logger.debug(
            f"Completed request tracking: {request_metrics.method} {request_metrics.endpoint} "
            f"({request_metrics.total_duration_ms:.1f}ms, {request_metrics.sla_tier.value})"
        )
    
    async def _update_endpoint_metrics(self, request_metrics: RequestMetrics):
        """Update aggregated endpoint metrics."""
        
        endpoint_key = f"{request_metrics.method}:{self._normalize_endpoint(request_metrics.endpoint)}"
        
        if endpoint_key not in self.endpoint_metrics:
            self.endpoint_metrics[endpoint_key] = EndpointMetrics(
                endpoint_pattern=self._normalize_endpoint(request_metrics.endpoint),
                method=request_metrics.method
            )
        
        endpoint = self.endpoint_metrics[endpoint_key]
        
        # Update request counts
        endpoint.total_requests += 1
        if request_metrics.error_message is None and 200 <= request_metrics.status_code < 400:
            endpoint.successful_requests += 1
        else:
            endpoint.failed_requests += 1
        
        # Update SLA compliance counts
        if request_metrics.sla_tier == SLATier.ULTRA_FAST:
            endpoint.ultra_fast_requests += 1
        elif request_metrics.sla_tier == SLATier.CRITICAL:
            endpoint.critical_sla_requests += 1
        elif request_metrics.sla_tier == SLATier.STANDARD:
            endpoint.standard_requests += 1
        else:
            endpoint.slow_requests += 1
        
        # Update timing statistics
        duration = request_metrics.total_duration_ms
        endpoint.min_duration_ms = min(endpoint.min_duration_ms, duration)
        endpoint.max_duration_ms = max(endpoint.max_duration_ms, duration)
        
        # Calculate running average
        total_duration = endpoint.avg_duration_ms * (endpoint.total_requests - 1) + duration
        endpoint.avg_duration_ms = total_duration / endpoint.total_requests
        
        # Update percentiles (simplified calculation)
        recent_durations = [r.total_duration_ms for r in endpoint.recent_requests]
        recent_durations.append(duration)
        if len(recent_durations) >= 20:  # Need sufficient samples
            recent_durations.sort()
            endpoint.p50_duration_ms = recent_durations[len(recent_durations) // 2]
            endpoint.p95_duration_ms = recent_durations[int(len(recent_durations) * 0.95)]
            endpoint.p99_duration_ms = recent_durations[int(len(recent_durations) * 0.99)]
        
        # Update error tracking
        if request_metrics.error_type:
            endpoint.error_types[request_metrics.error_type] = (
                endpoint.error_types.get(request_metrics.error_type, 0) + 1
            )
        
        endpoint.error_rate = (endpoint.failed_requests / endpoint.total_requests) * 100
        
        # Update resource usage
        if request_metrics.memory_usage_mb > 0:
            endpoint.avg_memory_usage_mb = (
                (endpoint.avg_memory_usage_mb * (endpoint.total_requests - 1) + 
                 request_metrics.memory_usage_mb) / endpoint.total_requests
            )
        
        if request_metrics.cpu_usage_percent > 0:
            endpoint.avg_cpu_usage_percent = (
                (endpoint.avg_cpu_usage_percent * (endpoint.total_requests - 1) + 
                 request_metrics.cpu_usage_percent) / endpoint.total_requests
            )
        
        # Update business metrics
        if request_metrics.user_id:
            endpoint.unique_users.add(request_metrics.user_id)
        
        # Add to recent requests
        endpoint.recent_requests.append(request_metrics)
        endpoint.last_activity = request_metrics.end_time
        
        # Calculate throughput (requests per second over last minute)
        cutoff_time = datetime.utcnow() - timedelta(minutes=1)
        recent_count = sum(1 for r in endpoint.recent_requests if r.end_time > cutoff_time)
        endpoint.requests_per_second = recent_count / 60.0
        endpoint.peak_rps = max(endpoint.peak_rps, endpoint.requests_per_second)
    
    def _update_prometheus_metrics(self, request_metrics: RequestMetrics):
        """Update Prometheus metrics."""
        
        labels = {
            'endpoint': self._normalize_endpoint(request_metrics.endpoint),
            'method': request_metrics.method,
            'status_code': str(request_metrics.status_code),
            'sla_tier': request_metrics.sla_tier.value
        }
        
        # API response time histogram
        enterprise_metrics.api_response_time_histogram.labels(**labels).observe(
            request_metrics.total_duration_ms
        )
        
        # Request counter
        enterprise_metrics.api_requests_total.labels(
            endpoint=labels['endpoint'],
            method=labels['method'],
            status_code=labels['status_code']
        ).inc()
        
        # SLA compliance (for critical endpoints)
        if request_metrics.request_type not in [RequestType.HEALTH_CHECK]:
            compliance_rate = 100.0 if request_metrics.sla_compliant else 0.0
            enterprise_metrics.api_sla_compliance_rate.labels(
                endpoint=labels['endpoint'],
                time_window='1m'
            ).set(compliance_rate)
    
    async def calculate_api_health(self) -> APIHealth:
        """Calculate overall API health metrics."""
        
        health = APIHealth()
        
        if not self.request_history:
            return health
        
        # Analyze recent requests (last hour)
        cutoff_time = datetime.utcnow() - timedelta(hours=1)
        recent_requests = [
            r for r in self.request_history 
            if r['timestamp'] > cutoff_time
        ]
        
        if not recent_requests:
            return health
        
        # SLA compliance calculations
        total_requests = len(recent_requests)
        critical_compliant = sum(1 for r in recent_requests if r['sla_compliant'])
        ultra_fast = sum(1 for r in recent_requests if r['sla_tier'] == SLATier.ULTRA_FAST.value)
        
        health.overall_sla_compliance = (critical_compliant / total_requests) * 100
        health.critical_sla_compliance = (critical_compliant / total_requests) * 100
        health.ultra_fast_compliance = (ultra_fast / total_requests) * 100
        
        # Performance metrics
        durations = [r['duration_ms'] for r in recent_requests]
        durations.sort()
        
        health.avg_response_time_ms = sum(durations) / len(durations)
        if len(durations) >= 20:
            health.p95_response_time_ms = durations[int(len(durations) * 0.95)]
            health.p99_response_time_ms = durations[int(len(durations) * 0.99)]
        
        # Throughput calculation
        health.total_rps = total_requests / 3600.0  # Requests per second over the hour
        
        # Error rate calculation
        error_requests = sum(1 for r in recent_requests if r['error'] or r['status_code'] >= 400)
        critical_errors = sum(1 for r in recent_requests if r['status_code'] >= 500)
        
        health.overall_error_rate = (error_requests / total_requests) * 100
        health.critical_error_rate = (critical_errors / total_requests) * 100
        
        # Business metrics
        unique_users = set(r['user_id'] for r in recent_requests if r['user_id'])
        health.active_users = len(unique_users)
        health.total_endpoints = len(self.endpoint_metrics)
        
        # Endpoint health
        healthy_endpoints = 0
        for endpoint in self.endpoint_metrics.values():
            if (endpoint.error_rate < self.alert_thresholds["endpoint_error_rate"] and
                endpoint.avg_duration_ms < self.sla_thresholds[SLATier.STANDARD]):
                healthy_endpoints += 1
        
        health.healthy_endpoints = healthy_endpoints
        
        # Store health history
        self.health_history.append(health)
        self.api_health = health
        
        return health
    
    async def detect_performance_issues(self) -> List[Dict[str, Any]]:
        """Detect API performance issues and anomalies."""
        
        issues = []
        
        try:
            # Update API health
            health = await self.calculate_api_health()
            
            # Critical SLA compliance check
            if health.critical_sla_compliance < self.alert_thresholds["critical_sla_compliance"]:
                issues.append({
                    "type": "sla_compliance_breach",
                    "severity": AlertSeverity.CRITICAL.value,
                    "metric": "critical_sla_compliance",
                    "value": health.critical_sla_compliance,
                    "threshold": self.alert_thresholds["critical_sla_compliance"],
                    "message": f"Critical <15ms SLA compliance at {health.critical_sla_compliance:.1f}%",
                    "business_impact": "High - affects $2.1M API performance story",
                    "recommendation": "Investigate slow endpoints, optimize database queries, check cache performance"
                })
            
            # High error rate
            if health.overall_error_rate > self.alert_thresholds["overall_error_rate"]:
                issues.append({
                    "type": "high_error_rate",
                    "severity": AlertSeverity.ERROR.value,
                    "metric": "overall_error_rate",
                    "value": health.overall_error_rate,
                    "threshold": self.alert_thresholds["overall_error_rate"],
                    "message": f"Overall error rate at {health.overall_error_rate:.1f}%",
                    "recommendation": "Check application logs, validate input handling, review recent deployments"
                })
            
            # High P95 response time
            if health.p95_response_time_ms > self.alert_thresholds["response_time_p95"]:
                issues.append({
                    "type": "high_response_time_p95",
                    "severity": AlertSeverity.WARNING.value,
                    "metric": "p95_response_time",
                    "value": health.p95_response_time_ms,
                    "threshold": self.alert_thresholds["response_time_p95"],
                    "message": f"95th percentile response time at {health.p95_response_time_ms:.1f}ms",
                    "recommendation": "Profile slow endpoints, optimize database queries, consider caching"
                })
            
            # Check individual endpoint issues
            for endpoint_key, endpoint in self.endpoint_metrics.items():
                # High endpoint error rate
                if (endpoint.error_rate > self.alert_thresholds["endpoint_error_rate"] and 
                    endpoint.total_requests > 10):
                    issues.append({
                        "type": "endpoint_high_error_rate",
                        "severity": AlertSeverity.WARNING.value,
                        "endpoint": endpoint_key,
                        "metric": "error_rate",
                        "value": endpoint.error_rate,
                        "threshold": self.alert_thresholds["endpoint_error_rate"],
                        "message": f"High error rate for {endpoint_key}: {endpoint.error_rate:.1f}%",
                        "error_types": dict(endpoint.error_types),
                        "recommendation": "Review endpoint implementation, validate input parameters"
                    })
                
                # Slow endpoint performance
                if (endpoint.avg_duration_ms > self.alert_thresholds["slow_request_threshold"] and
                    endpoint.total_requests > 5):
                    issues.append({
                        "type": "endpoint_slow_performance",
                        "severity": AlertSeverity.WARNING.value,
                        "endpoint": endpoint_key,
                        "metric": "avg_duration_ms",
                        "value": endpoint.avg_duration_ms,
                        "threshold": self.alert_thresholds["slow_request_threshold"],
                        "message": f"Slow performance for {endpoint_key}: {endpoint.avg_duration_ms:.1f}ms average",
                        "recommendation": "Profile endpoint, optimize database queries, consider async processing"
                    })
                
                # Low throughput (compared to historical data)
                if len(self.health_history) > 60:  # Need historical data
                    historical_rps = [h.total_rps for h in list(self.health_history)[-60:]]
                    avg_historical_rps = sum(historical_rps) / len(historical_rps)
                    
                    if (avg_historical_rps > 0 and 
                        endpoint.requests_per_second < avg_historical_rps * 0.7):  # 30% degradation
                        issues.append({
                            "type": "throughput_degradation",
                            "severity": AlertSeverity.WARNING.value,
                            "endpoint": endpoint_key,
                            "metric": "requests_per_second",
                            "value": endpoint.requests_per_second,
                            "historical_average": avg_historical_rps,
                            "degradation_percent": ((avg_historical_rps - endpoint.requests_per_second) / avg_historical_rps) * 100,
                            "message": f"Throughput degradation for {endpoint_key}",
                            "recommendation": "Check for upstream dependencies, review resource constraints"
                        })
            
            # Check for anomalous user activity
            if len(self.request_history) > 1000:
                recent_hour = [r for r in self.request_history if 
                              (datetime.utcnow() - r['timestamp']).seconds < 3600]
                
                if recent_hour:
                    # Check for unusual error patterns
                    error_rate_by_user = defaultdict(list)
                    for r in recent_hour:
                        if r['user_id']:
                            error_rate_by_user[r['user_id']].append(r['error'] or r['status_code'] >= 400)
                    
                    for user_id, errors in error_rate_by_user.items():
                        if len(errors) > 20:  # Sufficient activity
                            error_rate = (sum(errors) / len(errors)) * 100
                            if error_rate > 50:  # High error rate for this user
                                issues.append({
                                    "type": "user_high_error_rate",
                                    "severity": AlertSeverity.WARNING.value,
                                    "user_id": user_id,
                                    "error_rate": error_rate,
                                    "total_requests": len(errors),
                                    "message": f"User {user_id} experiencing {error_rate:.1f}% error rate",
                                    "recommendation": "Check user permissions, validate data, review user behavior"
                                })
            
        except Exception as e:
            self.logger.error(f"Error detecting performance issues: {e}")
        
        return issues
    
    def get_performance_recommendations(self) -> List[Dict[str, Any]]:
        """Generate performance optimization recommendations."""
        
        recommendations = []
        
        try:
            # Analyze endpoint performance patterns
            for endpoint_key, endpoint in self.endpoint_metrics.items():
                if endpoint.total_requests < 10:  # Need sufficient data
                    continue
                
                rec = {
                    "endpoint": endpoint_key,
                    "current_performance": {
                        "avg_duration_ms": endpoint.avg_duration_ms,
                        "p95_duration_ms": endpoint.p95_duration_ms,
                        "error_rate": endpoint.error_rate,
                        "requests_per_second": endpoint.requests_per_second
                    },
                    "recommendations": []
                }
                
                # High response time recommendations
                if endpoint.avg_duration_ms > 100:
                    if endpoint.avg_duration_ms > 1000:
                        rec["recommendations"].extend([
                            "Consider implementing async processing for long-running operations",
                            "Add response caching for frequently requested data",
                            "Optimize database queries with proper indexing",
                            "Consider implementing pagination for large data sets"
                        ])
                    else:
                        rec["recommendations"].extend([
                            "Profile database queries for optimization opportunities",
                            "Implement response caching for static or semi-static data",
                            "Consider database query optimization"
                        ])
                
                # Low SLA compliance recommendations
                sla_compliant_rate = ((endpoint.ultra_fast_requests + endpoint.critical_sla_requests) 
                                     / endpoint.total_requests) * 100
                if sla_compliant_rate < 90:
                    rec["recommendations"].extend([
                        "Optimize critical path operations to meet <15ms SLA",
                        "Implement request prioritization for critical operations",
                        "Consider horizontal scaling for high-traffic endpoints",
                        "Add request-level caching for frequently accessed data"
                    ])
                
                # High error rate recommendations
                if endpoint.error_rate > 5:
                    rec["recommendations"].extend([
                        "Add comprehensive input validation and error handling",
                        "Implement circuit breakers for external dependencies",
                        "Add detailed logging for error investigation",
                        "Consider implementing retry mechanisms with exponential backoff"
                    ])
                
                # Resource usage recommendations
                if endpoint.avg_memory_usage_mb > 512:
                    rec["recommendations"].append(
                        "Optimize memory usage - consider streaming for large data processing"
                    )
                
                if endpoint.avg_cpu_usage_percent > 70:
                    rec["recommendations"].append(
                        "High CPU usage detected - profile and optimize computational operations"
                    )
                
                # Cache performance recommendations
                if endpoint.cache_hit_rate < 80 and endpoint.cache_hit_count + endpoint.cache_miss_count > 0:
                    rec["recommendations"].extend([
                        "Improve cache hit rate by optimizing cache keys and TTL",
                        "Consider implementing cache warming strategies",
                        "Review cache invalidation strategies"
                    ])
                
                if rec["recommendations"]:
                    # Calculate potential impact
                    if endpoint.avg_duration_ms > 100:
                        rec["potential_improvement"] = "Could improve response times by 30-70%"
                    elif endpoint.error_rate > 5:
                        rec["potential_improvement"] = "Could reduce error rates significantly"
                    else:
                        rec["potential_improvement"] = "Moderate performance improvements expected"
                    
                    recommendations.append(rec)
            
            # Global recommendations based on overall patterns
            if self.api_health.critical_sla_compliance < 95:
                recommendations.append({
                    "type": "global",
                    "priority": "high",
                    "current_performance": {
                        "sla_compliance": self.api_health.critical_sla_compliance
                    },
                    "recommendations": [
                        "Implement global request prioritization for SLA-critical operations",
                        "Add infrastructure auto-scaling based on response time metrics",
                        "Consider implementing a global cache layer (Redis/Memcached)",
                        "Optimize database connection pooling and query performance",
                        "Implement request queuing with priority handling"
                    ],
                    "potential_improvement": "Could improve overall SLA compliance to 98%+"
                })
            
        except Exception as e:
            self.logger.error(f"Error generating performance recommendations: {e}")
        
        return recommendations
    
    def get_sla_dashboard_data(self) -> Dict[str, Any]:
        """Get data for SLA compliance dashboard."""
        
        # Recent SLA performance (last 24 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        recent_requests = [r for r in self.request_history if r['timestamp'] > cutoff_time]
        
        # SLA breakdown
        sla_breakdown = {
            "ultra_fast": sum(1 for r in recent_requests if r['sla_tier'] == SLATier.ULTRA_FAST.value),
            "critical": sum(1 for r in recent_requests if r['sla_tier'] == SLATier.CRITICAL.value),
            "standard": sum(1 for r in recent_requests if r['sla_tier'] == SLATier.STANDARD.value),
            "acceptable": sum(1 for r in recent_requests if r['sla_tier'] == SLATier.ACCEPTABLE.value),
            "slow": sum(1 for r in recent_requests if r['sla_tier'] == SLATier.SLOW.value)
        }
        
        total_requests = sum(sla_breakdown.values())
        
        # SLA compliance percentages
        sla_compliance = {}
        if total_requests > 0:
            sla_compliance = {
                "ultra_fast_percentage": (sla_breakdown["ultra_fast"] / total_requests) * 100,
                "critical_percentage": ((sla_breakdown["ultra_fast"] + sla_breakdown["critical"]) / total_requests) * 100,
                "standard_percentage": ((sla_breakdown["ultra_fast"] + sla_breakdown["critical"] + sla_breakdown["standard"]) / total_requests) * 100
            }
        
        # Endpoint SLA performance
        endpoint_sla = {}
        for endpoint_key, endpoint in self.endpoint_metrics.items():
            if endpoint.total_requests > 0:
                compliance_rate = ((endpoint.ultra_fast_requests + endpoint.critical_sla_requests) / endpoint.total_requests) * 100
                endpoint_sla[endpoint_key] = {
                    "compliance_rate": compliance_rate,
                    "avg_response_time": endpoint.avg_duration_ms,
                    "p95_response_time": endpoint.p95_duration_ms,
                    "total_requests": endpoint.total_requests,
                    "ultra_fast_count": endpoint.ultra_fast_requests,
                    "critical_count": endpoint.critical_sla_requests
                }
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_health": {
                "critical_sla_compliance": self.api_health.critical_sla_compliance,
                "ultra_fast_compliance": self.api_health.ultra_fast_compliance,
                "avg_response_time_ms": self.api_health.avg_response_time_ms,
                "p95_response_time_ms": self.api_health.p95_response_time_ms,
                "total_rps": self.api_health.total_rps,
                "error_rate": self.api_health.overall_error_rate
            },
            "sla_breakdown": sla_breakdown,
            "sla_compliance": sla_compliance,
            "endpoint_performance": endpoint_sla,
            "sla_thresholds": {tier.name: threshold for tier, threshold in self.sla_thresholds.items()},
            "business_impact": {
                "target_sla": "95% of requests < 15ms",
                "business_value": "$2.1M API performance story",
                "current_status": "compliant" if self.api_health.critical_sla_compliance >= 95 else "at_risk"
            }
        }


# FastAPI Middleware for automatic monitoring
class FastAPIMonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware for automatic FastAPI endpoint monitoring."""
    
    def __init__(self, app: ASGIApp, monitor: ComprehensiveFastAPIMonitor):
        super().__init__(app)
        self.monitor = monitor
        self.logger = get_logger(__name__)
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Skip monitoring for certain endpoints
        if self._should_skip_monitoring(request):
            return await call_next(request)
        
        # Start request tracking
        request_id = await self.monitor.start_request_tracking(request)
        
        error = None
        response = None
        
        try:
            # Process request
            response = await call_next(request)
            return response
            
        except Exception as e:
            error = e
            # Create error response
            from fastapi.responses import JSONResponse
            response = JSONResponse(
                status_code=500,
                content={"error": "Internal server error", "request_id": request_id}
            )
            return response
            
        finally:
            # Finish request tracking
            if response:
                await self.monitor.finish_request_tracking(request_id, response, error)
    
    def _should_skip_monitoring(self, request: Request) -> bool:
        """Determine if request should be skipped from monitoring."""
        path = str(request.url.path).lower()
        
        # Skip common endpoints that don't need detailed monitoring
        skip_patterns = ['/health', '/ping', '/metrics', '/docs', '/redoc', '/openapi.json']
        
        return any(pattern in path for pattern in skip_patterns)


# Global monitor instance
fastapi_monitor = ComprehensiveFastAPIMonitor()


def setup_fastapi_monitoring(app: FastAPI) -> ComprehensiveFastAPIMonitor:
    """Set up comprehensive FastAPI monitoring."""
    
    # Add monitoring middleware
    app.add_middleware(FastAPIMonitoringMiddleware, monitor=fastapi_monitor)
    
    # Add monitoring endpoints
    @app.get("/monitoring/health")
    async def get_api_health():
        """Get API health metrics."""
        health = await fastapi_monitor.calculate_api_health()
        return {
            "status": "healthy" if health.critical_sla_compliance >= 95 else "degraded",
            "health": health.__dict__,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    @app.get("/monitoring/sla-dashboard")
    async def get_sla_dashboard():
        """Get SLA compliance dashboard data."""
        return fastapi_monitor.get_sla_dashboard_data()
    
    @app.get("/monitoring/performance-issues")
    async def get_performance_issues():
        """Get current performance issues."""
        issues = await fastapi_monitor.detect_performance_issues()
        return {
            "issues": issues,
            "total_issues": len(issues),
            "critical_issues": len([i for i in issues if i.get("severity") == "critical"]),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    @app.get("/monitoring/recommendations")
    async def get_performance_recommendations():
        """Get performance optimization recommendations."""
        recommendations = fastapi_monitor.get_performance_recommendations()
        return {
            "recommendations": recommendations,
            "total_recommendations": len(recommendations),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    logger.info("FastAPI comprehensive monitoring setup completed")
    return fastapi_monitor


__all__ = [
    'ComprehensiveFastAPIMonitor',
    'FastAPIMonitoringMiddleware',
    'fastapi_monitor',
    'setup_fastapi_monitoring',
    'SLATier',
    'RequestType',
    'RequestMetrics',
    'EndpointMetrics',
    'APIHealth'
]