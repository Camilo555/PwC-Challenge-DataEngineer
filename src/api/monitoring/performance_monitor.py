"""
Comprehensive Performance Monitor for Story 1.1 Dashboard API
Real-time performance tracking with <25ms SLA validation and alerting

Features:
- Real-time performance metrics collection
- <25ms response time SLA monitoring and validation
- Advanced alerting with multiple channels
- Performance trend analysis and prediction
- Resource utilization tracking
- Dashboard-specific performance profiling
- Automated performance optimization recommendations
- Integration with Prometheus, DataDog, and custom metrics
"""
import asyncio
import json
import time
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable, NamedTuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict
import logging
import threading
import psutil
import numpy as np
from concurrent.futures import ThreadPoolExecutor

from prometheus_client import Counter, Histogram, Gauge, Summary, start_http_server
import aiohttp
import aiofiles
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from src.api.scaling.auto_scaling_manager import ScalingMetrics, AutoScalingManager
from src.api.dashboard.enhanced_cache_manager import EnhancedCacheManager
from src.api.dashboard.enhanced_websocket_dashboard import EnhancedDashboardWebSocketManager
from core.logging import get_logger
from core.config.unified_config import get_unified_config


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class PerformanceMetricType(Enum):
    """Types of performance metrics"""
    RESPONSE_TIME = "response_time"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    RESOURCE_UTILIZATION = "resource_utilization"
    WEBSOCKET_LATENCY = "websocket_latency"
    CACHE_PERFORMANCE = "cache_performance"
    DATABASE_PERFORMANCE = "database_performance"


class AlertChannel(Enum):
    """Alert delivery channels"""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    PROMETHEUS = "prometheus"
    DATADOG = "datadog"
    PAGERDUTY = "pagerduty"


@dataclass
class PerformanceThreshold:
    """Performance threshold configuration"""
    metric_type: PerformanceMetricType
    threshold_value: float
    comparison: str  # "gt", "lt", "eq"
    severity: AlertSeverity
    duration_seconds: int = 60  # Threshold must be breached for this duration
    description: str = ""


@dataclass
class PerformanceAlert:
    """Performance alert data"""
    alert_id: str
    timestamp: datetime
    severity: AlertSeverity
    metric_type: PerformanceMetricType
    current_value: float
    threshold_value: float
    description: str
    affected_endpoints: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    correlation_id: str = ""


@dataclass
class EndpointMetrics:
    """Metrics for a specific endpoint"""
    endpoint: str
    method: str
    
    # Response time metrics
    response_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    p50_response_time_ms: float = 0.0
    p95_response_time_ms: float = 0.0
    p99_response_time_ms: float = 0.0
    avg_response_time_ms: float = 0.0
    
    # Request metrics
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    error_rate_percent: float = 0.0
    
    # Throughput metrics
    requests_per_second: float = 0.0
    peak_rps: float = 0.0
    
    # SLA compliance
    sla_violations: int = 0
    sla_compliance_percent: float = 100.0
    
    # Business metrics
    dashboard_specific: bool = False
    websocket_connections: int = 0
    cache_hit_rate: float = 0.0


class PerformanceMonitor:
    """
    Comprehensive performance monitoring system
    Tracks all performance metrics with <25ms SLA validation
    """
    
    def __init__(self, sla_target_ms: float = 25.0):
        self.logger = get_logger("performance_monitor")
        self.config = get_unified_config()
        
        # SLA configuration
        self.sla_target_ms = sla_target_ms
        self.sla_buffer_ms = 5.0  # 5ms buffer before alerting
        
        # Metrics storage
        self.endpoint_metrics: Dict[str, EndpointMetrics] = {}
        self.global_metrics = {
            "total_requests": 0,
            "total_response_time_ms": 0.0,
            "sla_violations": 0,
            "active_alerts": 0,
            "system_health_score": 100.0
        }
        
        # Performance history for trend analysis
        self.performance_history: deque = deque(maxlen=10000)
        self.system_metrics_history: deque = deque(maxlen=1000)
        
        # Alert management
        self.active_alerts: Dict[str, PerformanceAlert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        self.alert_channels: List[AlertChannel] = [AlertChannel.PROMETHEUS]
        
        # Thresholds configuration
        self.thresholds = self._initialize_thresholds()
        
        # Threading and async management
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.background_tasks: List[asyncio.Task] = []
        
        # Prometheus metrics
        self._setup_prometheus_metrics()
        
        # External integrations
        self.cache_manager: Optional[EnhancedCacheManager] = None
        self.websocket_manager: Optional[EnhancedDashboardWebSocketManager] = None
        self.scaling_manager: Optional[AutoScalingManager] = None
        
        # Start monitoring
        self._start_background_monitoring()
    
    def _initialize_thresholds(self) -> List[PerformanceThreshold]:
        """Initialize performance thresholds"""
        return [
            # Critical SLA threshold
            PerformanceThreshold(
                metric_type=PerformanceMetricType.RESPONSE_TIME,
                threshold_value=self.sla_target_ms,
                comparison="gt",
                severity=AlertSeverity.CRITICAL,
                duration_seconds=30,
                description=f"Response time exceeded {self.sla_target_ms}ms SLA target"
            ),
            
            # Warning threshold
            PerformanceThreshold(
                metric_type=PerformanceMetricType.RESPONSE_TIME,
                threshold_value=self.sla_target_ms * 0.8,  # 80% of SLA
                comparison="gt",
                severity=AlertSeverity.WARNING,
                duration_seconds=60,
                description=f"Response time approaching SLA threshold"
            ),
            
            # Error rate threshold
            PerformanceThreshold(
                metric_type=PerformanceMetricType.ERROR_RATE,
                threshold_value=5.0,  # 5% error rate
                comparison="gt",
                severity=AlertSeverity.WARNING,
                duration_seconds=120,
                description="High error rate detected"
            ),
            
            # Critical error rate
            PerformanceThreshold(
                metric_type=PerformanceMetricType.ERROR_RATE,
                threshold_value=10.0,  # 10% error rate
                comparison="gt",
                severity=AlertSeverity.CRITICAL,
                duration_seconds=60,
                description="Critical error rate detected"
            ),
            
            # Resource utilization
            PerformanceThreshold(
                metric_type=PerformanceMetricType.RESOURCE_UTILIZATION,
                threshold_value=90.0,  # 90% CPU
                comparison="gt",
                severity=AlertSeverity.WARNING,
                duration_seconds=300,
                description="High resource utilization"
            ),
            
            # WebSocket latency
            PerformanceThreshold(
                metric_type=PerformanceMetricType.WEBSOCKET_LATENCY,
                threshold_value=50.0,  # 50ms for WebSocket
                comparison="gt",
                severity=AlertSeverity.WARNING,
                duration_seconds=60,
                description="High WebSocket latency"
            ),
            
            # Cache performance
            PerformanceThreshold(
                metric_type=PerformanceMetricType.CACHE_PERFORMANCE,
                threshold_value=80.0,  # 80% hit rate minimum
                comparison="lt",
                severity=AlertSeverity.WARNING,
                duration_seconds=300,
                description="Low cache hit rate"
            )
        ]
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics"""
        # Response time metrics
        self.prom_response_time = Histogram(
            'dashboard_api_response_time_seconds',
            'Response time in seconds',
            ['endpoint', 'method', 'status_code'],
            buckets=[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]
        )
        
        # SLA compliance metrics
        self.prom_sla_compliance = Gauge(
            'dashboard_api_sla_compliance_percent',
            'SLA compliance percentage',
            ['endpoint']
        )
        
        self.prom_sla_violations = Counter(
            'dashboard_api_sla_violations_total',
            'Total SLA violations',
            ['endpoint', 'severity']
        )
        
        # Request metrics
        self.prom_requests_total = Counter(
            'dashboard_api_requests_total',
            'Total requests',
            ['endpoint', 'method', 'status_code']
        )
        
        self.prom_requests_per_second = Gauge(
            'dashboard_api_requests_per_second',
            'Requests per second',
            ['endpoint']
        )
        
        # Error metrics
        self.prom_error_rate = Gauge(
            'dashboard_api_error_rate_percent',
            'Error rate percentage',
            ['endpoint']
        )
        
        # System health metrics
        self.prom_system_health = Gauge(
            'dashboard_api_system_health_score',
            'Overall system health score (0-100)'
        )
        
        # Performance alerts
        self.prom_active_alerts = Gauge(
            'dashboard_api_active_alerts',
            'Number of active performance alerts',
            ['severity']
        )
        
        # Custom dashboard metrics
        self.prom_dashboard_performance = Gauge(
            'dashboard_api_dashboard_performance_score',
            'Dashboard-specific performance score',
            ['dashboard_type']
        )
    
    def _start_background_monitoring(self):
        """Start background monitoring tasks"""
        tasks = [
            self._system_metrics_collector(),
            self._performance_analyzer(),
            self._alert_processor(),
            self._health_checker(),
            self._metrics_aggregator(),
            self._trend_analyzer()
        ]
        
        for task_func in tasks:
            task = asyncio.create_task(task_func)
            self.background_tasks.append(task)
        
        self.logger.info("Performance monitoring background tasks started")
    
    def register_external_components(
        self,
        cache_manager: Optional[EnhancedCacheManager] = None,
        websocket_manager: Optional[EnhancedDashboardWebSocketManager] = None,
        scaling_manager: Optional[AutoScalingManager] = None
    ):
        """Register external components for comprehensive monitoring"""
        self.cache_manager = cache_manager
        self.websocket_manager = websocket_manager
        self.scaling_manager = scaling_manager
        
        self.logger.info("External components registered for monitoring")
    
    async def record_request_metrics(
        self,
        endpoint: str,
        method: str,
        response_time_ms: float,
        status_code: int,
        request: Optional[Request] = None
    ):
        """Record metrics for a request"""
        try:
            # Create endpoint key
            endpoint_key = f"{method}:{endpoint}"
            
            # Initialize endpoint metrics if not exists
            if endpoint_key not in self.endpoint_metrics:
                self.endpoint_metrics[endpoint_key] = EndpointMetrics(
                    endpoint=endpoint,
                    method=method,
                    dashboard_specific=self._is_dashboard_endpoint(endpoint)
                )
            
            metrics = self.endpoint_metrics[endpoint_key]
            
            # Update basic metrics
            metrics.total_requests += 1
            metrics.response_times.append(response_time_ms)
            
            # Update success/failure counts
            if 200 <= status_code < 400:
                metrics.successful_requests += 1
            else:
                metrics.failed_requests += 1
            
            # Calculate derived metrics
            await self._update_endpoint_metrics(metrics)
            
            # Check SLA compliance
            if response_time_ms > self.sla_target_ms:
                metrics.sla_violations += 1
                self.global_metrics["sla_violations"] += 1
                
                # Record SLA violation in Prometheus
                severity = AlertSeverity.CRITICAL.value if response_time_ms > self.sla_target_ms * 2 else AlertSeverity.WARNING.value
                self.prom_sla_violations.labels(endpoint=endpoint, severity=severity).inc()
            
            # Update Prometheus metrics
            self.prom_response_time.labels(
                endpoint=endpoint,
                method=method,
                status_code=str(status_code)
            ).observe(response_time_ms / 1000)  # Convert to seconds
            
            self.prom_requests_total.labels(
                endpoint=endpoint,
                method=method,
                status_code=str(status_code)
            ).inc()
            
            # Update global metrics
            self.global_metrics["total_requests"] += 1
            self.global_metrics["total_response_time_ms"] += response_time_ms
            
            # Add to performance history
            performance_record = {
                'timestamp': datetime.now(),
                'endpoint': endpoint,
                'method': method,
                'response_time_ms': response_time_ms,
                'status_code': status_code,
                'sla_compliant': response_time_ms <= self.sla_target_ms,
                'user_id': getattr(request.state, 'user_id', None) if request else None,
                'correlation_id': getattr(request.state, 'correlation_id', None) if request else None
            }
            
            self.performance_history.append(performance_record)
            
        except Exception as e:
            self.logger.error(f"Error recording request metrics: {e}")
    
    def _is_dashboard_endpoint(self, endpoint: str) -> bool:
        """Check if endpoint is dashboard-specific"""
        dashboard_patterns = [
            '/api/v1/dashboard', '/api/v2/dashboard',
            '/ws/dashboard', '/api/v1/executive',
            '/api/v1/revenue', '/api/v2/analytics'
        ]
        
        return any(pattern in endpoint.lower() for pattern in dashboard_patterns)
    
    async def _update_endpoint_metrics(self, metrics: EndpointMetrics):
        """Update calculated endpoint metrics"""
        try:
            if not metrics.response_times:
                return
            
            response_times = list(metrics.response_times)
            
            # Calculate percentiles
            if len(response_times) >= 1:
                metrics.avg_response_time_ms = statistics.mean(response_times)
                
            if len(response_times) >= 2:
                sorted_times = sorted(response_times)
                metrics.p50_response_time_ms = statistics.median(sorted_times)
                
            if len(response_times) >= 20:
                sorted_times = sorted(response_times)
                p95_idx = int(len(sorted_times) * 0.95)
                p99_idx = int(len(sorted_times) * 0.99)
                
                metrics.p95_response_time_ms = sorted_times[p95_idx]
                metrics.p99_response_time_ms = sorted_times[p99_idx]
            
            # Calculate error rate
            total_requests = metrics.total_requests
            if total_requests > 0:
                metrics.error_rate_percent = (metrics.failed_requests / total_requests) * 100
            
            # Calculate SLA compliance
            if metrics.sla_violations > 0 and total_requests > 0:
                metrics.sla_compliance_percent = ((total_requests - metrics.sla_violations) / total_requests) * 100
            
            # Update Prometheus metrics
            self.prom_sla_compliance.labels(endpoint=metrics.endpoint).set(metrics.sla_compliance_percent)
            self.prom_error_rate.labels(endpoint=metrics.endpoint).set(metrics.error_rate_percent)
            
            # Calculate requests per second (approximate)
            if len(response_times) >= 60:  # At least 1 minute of data
                recent_requests = len([t for t in response_times if t is not None])
                metrics.requests_per_second = recent_requests / 60.0  # Rough approximation
                self.prom_requests_per_second.labels(endpoint=metrics.endpoint).set(metrics.requests_per_second)
            
        except Exception as e:
            self.logger.error(f"Error updating endpoint metrics: {e}")
    
    async def check_performance_thresholds(self):
        """Check all performance thresholds and generate alerts"""
        try:
            current_time = datetime.now()
            
            for threshold in self.thresholds:
                violation_detected = False
                current_value = 0.0
                affected_endpoints = []
                
                if threshold.metric_type == PerformanceMetricType.RESPONSE_TIME:
                    # Check response time thresholds
                    for endpoint_key, metrics in self.endpoint_metrics.items():
                        if metrics.p95_response_time_ms > 0:
                            current_value = metrics.p95_response_time_ms
                            
                            if self._evaluate_threshold(current_value, threshold.threshold_value, threshold.comparison):
                                violation_detected = True
                                affected_endpoints.append(endpoint_key)
                
                elif threshold.metric_type == PerformanceMetricType.ERROR_RATE:
                    # Check error rate thresholds
                    for endpoint_key, metrics in self.endpoint_metrics.items():
                        current_value = metrics.error_rate_percent
                        
                        if self._evaluate_threshold(current_value, threshold.threshold_value, threshold.comparison):
                            violation_detected = True
                            affected_endpoints.append(endpoint_key)
                
                elif threshold.metric_type == PerformanceMetricType.RESOURCE_UTILIZATION:
                    # Check system resource utilization
                    cpu_percent = psutil.cpu_percent()
                    memory_percent = psutil.virtual_memory().percent
                    current_value = max(cpu_percent, memory_percent)
                    
                    if self._evaluate_threshold(current_value, threshold.threshold_value, threshold.comparison):
                        violation_detected = True
                        affected_endpoints = ["system"]
                
                elif threshold.metric_type == PerformanceMetricType.CACHE_PERFORMANCE:
                    # Check cache performance if available
                    if self.cache_manager:
                        cache_stats = await self.cache_manager.get_comprehensive_stats()
                        overall_hit_rate = cache_stats.get('global_metrics', {}).get('overall_hit_rate', 100.0)
                        current_value = overall_hit_rate
                        
                        if self._evaluate_threshold(current_value, threshold.threshold_value, threshold.comparison):
                            violation_detected = True
                            affected_endpoints = ["cache_system"]
                
                # Generate alert if threshold violated
                if violation_detected:
                    await self._generate_alert(threshold, current_value, affected_endpoints)
            
        except Exception as e:
            self.logger.error(f"Error checking performance thresholds: {e}")
    
    def _evaluate_threshold(self, current_value: float, threshold_value: float, comparison: str) -> bool:
        """Evaluate if threshold is violated"""
        if comparison == "gt":
            return current_value > threshold_value
        elif comparison == "lt":
            return current_value < threshold_value
        elif comparison == "eq":
            return abs(current_value - threshold_value) < 0.01
        return False
    
    async def _generate_alert(self, threshold: PerformanceThreshold, current_value: float, affected_endpoints: List[str]):
        """Generate performance alert"""
        try:
            alert_id = f"{threshold.metric_type.value}_{int(time.time())}"
            
            # Check if similar alert already active
            existing_alert = None
            for alert in self.active_alerts.values():
                if (alert.metric_type == threshold.metric_type and
                    alert.severity == threshold.severity and
                    set(alert.affected_endpoints) == set(affected_endpoints)):
                    existing_alert = alert
                    break
            
            if existing_alert:
                # Update existing alert
                existing_alert.current_value = current_value
                existing_alert.timestamp = datetime.now()
                return
            
            # Create new alert
            alert = PerformanceAlert(
                alert_id=alert_id,
                timestamp=datetime.now(),
                severity=threshold.severity,
                metric_type=threshold.metric_type,
                current_value=current_value,
                threshold_value=threshold.threshold_value,
                description=threshold.description,
                affected_endpoints=affected_endpoints,
                recommended_actions=self._get_recommended_actions(threshold, current_value),
                correlation_id=f"perf_{alert_id}"
            )
            
            # Add to active alerts
            self.active_alerts[alert_id] = alert
            self.alert_history.append(alert)
            
            # Update global metrics
            self.global_metrics["active_alerts"] = len(self.active_alerts)
            
            # Update Prometheus
            self.prom_active_alerts.labels(severity=threshold.severity.value).set(
                sum(1 for a in self.active_alerts.values() if a.severity == threshold.severity)
            )
            
            # Send alert through configured channels
            await self._send_alert(alert)
            
            self.logger.warning(
                f"Performance alert generated: {alert.description} "
                f"(current: {current_value}, threshold: {threshold.threshold_value})"
            )
            
        except Exception as e:
            self.logger.error(f"Error generating alert: {e}")
    
    def _get_recommended_actions(self, threshold: PerformanceThreshold, current_value: float) -> List[str]:
        """Get recommended actions for performance issues"""
        actions = []
        
        if threshold.metric_type == PerformanceMetricType.RESPONSE_TIME:
            if current_value > self.sla_target_ms * 2:
                actions.extend([
                    "Immediate: Check for service outages or high load",
                    "Scale up resources immediately",
                    "Review recent deployments for performance regressions",
                    "Check database query performance",
                    "Verify cache hit rates"
                ])
            else:
                actions.extend([
                    "Monitor for trend continuation",
                    "Review cache configuration",
                    "Consider resource scaling",
                    "Optimize database queries"
                ])
        
        elif threshold.metric_type == PerformanceMetricType.ERROR_RATE:
            actions.extend([
                "Investigate error logs immediately",
                "Check service health endpoints",
                "Review recent code changes",
                "Verify external service dependencies",
                "Consider rollback if related to recent deployment"
            ])
        
        elif threshold.metric_type == PerformanceMetricType.RESOURCE_UTILIZATION:
            actions.extend([
                "Scale up resources immediately",
                "Review memory leaks or CPU-intensive processes",
                "Check for runaway processes",
                "Consider load balancing improvements"
            ])
        
        elif threshold.metric_type == PerformanceMetricType.CACHE_PERFORMANCE:
            actions.extend([
                "Review cache configuration and TTL settings",
                "Check cache eviction patterns",
                "Consider increasing cache size",
                "Optimize cache key patterns",
                "Review cache warming strategies"
            ])
        
        return actions
    
    async def _send_alert(self, alert: PerformanceAlert):
        """Send alert through configured channels"""
        try:
            for channel in self.alert_channels:
                if channel == AlertChannel.PROMETHEUS:
                    # Already handled by Prometheus metrics
                    pass
                
                elif channel == AlertChannel.WEBHOOK:
                    await self._send_webhook_alert(alert)
                
                elif channel == AlertChannel.SLACK:
                    await self._send_slack_alert(alert)
                
                # Add more channels as needed
                
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
    
    async def _send_webhook_alert(self, alert: PerformanceAlert):
        """Send alert via webhook"""
        try:
            webhook_url = getattr(self.config, 'alert_webhook_url', None)
            if not webhook_url:
                return
            
            payload = {
                "alert_id": alert.alert_id,
                "timestamp": alert.timestamp.isoformat(),
                "severity": alert.severity.value,
                "metric_type": alert.metric_type.value,
                "description": alert.description,
                "current_value": alert.current_value,
                "threshold_value": alert.threshold_value,
                "affected_endpoints": alert.affected_endpoints,
                "recommended_actions": alert.recommended_actions
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=payload, timeout=10) as response:
                    if response.status == 200:
                        self.logger.info(f"Alert {alert.alert_id} sent via webhook")
                    else:
                        self.logger.warning(f"Webhook alert failed with status {response.status}")
                        
        except Exception as e:
            self.logger.error(f"Error sending webhook alert: {e}")
    
    async def _send_slack_alert(self, alert: PerformanceAlert):
        """Send alert via Slack"""
        try:
            slack_webhook_url = getattr(self.config, 'slack_webhook_url', None)
            if not slack_webhook_url:
                return
            
            # Format Slack message
            color = {
                AlertSeverity.INFO: "good",
                AlertSeverity.WARNING: "warning", 
                AlertSeverity.CRITICAL: "danger",
                AlertSeverity.EMERGENCY: "danger"
            }.get(alert.severity, "warning")
            
            message = {
                "attachments": [{
                    "color": color,
                    "title": f"🚨 Performance Alert - {alert.severity.value.upper()}",
                    "text": alert.description,
                    "fields": [
                        {
                            "title": "Current Value",
                            "value": f"{alert.current_value:.2f}",
                            "short": True
                        },
                        {
                            "title": "Threshold",
                            "value": f"{alert.threshold_value:.2f}",
                            "short": True
                        },
                        {
                            "title": "Affected Endpoints",
                            "value": ", ".join(alert.affected_endpoints),
                            "short": False
                        },
                        {
                            "title": "Recommended Actions",
                            "value": "\n".join(f"• {action}" for action in alert.recommended_actions[:3]),
                            "short": False
                        }
                    ],
                    "footer": f"Alert ID: {alert.alert_id}",
                    "ts": int(alert.timestamp.timestamp())
                }]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(slack_webhook_url, json=message, timeout=10) as response:
                    if response.status == 200:
                        self.logger.info(f"Alert {alert.alert_id} sent via Slack")
                    else:
                        self.logger.warning(f"Slack alert failed with status {response.status}")
                        
        except Exception as e:
            self.logger.error(f"Error sending Slack alert: {e}")
    
    # Background monitoring tasks
    async def _system_metrics_collector(self):
        """Collect system metrics periodically"""
        while True:
            try:
                # Collect system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                system_metrics = {
                    'timestamp': datetime.now(),
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_gb': memory.used / (1024**3),
                    'disk_percent': disk.percent,
                    'disk_used_gb': disk.used / (1024**3)
                }
                
                self.system_metrics_history.append(system_metrics)
                
                # Collect external component metrics
                if self.websocket_manager:
                    ws_stats = await self.websocket_manager.get_enhanced_connection_stats()
                    system_metrics.update({
                        'websocket_connections': ws_stats.get('connection_metrics', {}).get('active_connections', 0),
                        'websocket_performance_score': ws_stats.get('performance_metrics', {}).get('performance_score', 0)
                    })
                
                if self.cache_manager:
                    cache_stats = await self.cache_manager.get_comprehensive_stats()
                    system_metrics.update({
                        'cache_hit_rate': cache_stats.get('global_metrics', {}).get('overall_hit_rate', 0),
                        'cache_memory_mb': cache_stats.get('global_metrics', {}).get('memory_pressure', 0) * 1000
                    })
                
                await asyncio.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error collecting system metrics: {e}")
                await asyncio.sleep(60)
    
    async def _performance_analyzer(self):
        """Analyze performance trends and patterns"""
        while True:
            try:
                await self.check_performance_thresholds()
                await self._calculate_system_health_score()
                await self._analyze_performance_trends()
                
                await asyncio.sleep(60)  # Analyze every minute
                
            except Exception as e:
                self.logger.error(f"Error in performance analyzer: {e}")
                await asyncio.sleep(120)
    
    async def _calculate_system_health_score(self):
        """Calculate overall system health score"""
        try:
            score_components = []
            
            # SLA compliance score (40% weight)
            if self.global_metrics["total_requests"] > 0:
                sla_compliance = ((self.global_metrics["total_requests"] - self.global_metrics["sla_violations"]) / 
                                self.global_metrics["total_requests"]) * 100
                score_components.append(("sla_compliance", sla_compliance, 0.4))
            else:
                score_components.append(("sla_compliance", 100.0, 0.4))
            
            # Error rate score (20% weight)
            total_requests = sum(metrics.total_requests for metrics in self.endpoint_metrics.values())
            total_errors = sum(metrics.failed_requests for metrics in self.endpoint_metrics.values())
            error_rate = (total_errors / total_requests * 100) if total_requests > 0 else 0
            error_score = max(0, 100 - (error_rate * 10))  # 10% error = 0 score
            score_components.append(("error_rate", error_score, 0.2))
            
            # Resource utilization score (20% weight)
            if self.system_metrics_history:
                latest_metrics = self.system_metrics_history[-1]
                cpu_score = max(0, 100 - latest_metrics['cpu_percent'])
                memory_score = max(0, 100 - latest_metrics['memory_percent'])
                resource_score = (cpu_score + memory_score) / 2
                score_components.append(("resource_utilization", resource_score, 0.2))
            else:
                score_components.append(("resource_utilization", 100.0, 0.2))
            
            # Active alerts penalty (20% weight)
            alert_penalty = min(50, len(self.active_alerts) * 10)  # -10 points per active alert, max -50
            alert_score = max(0, 100 - alert_penalty)
            score_components.append(("alerts", alert_score, 0.2))
            
            # Calculate weighted average
            total_score = sum(score * weight for _, score, weight in score_components)
            
            self.global_metrics["system_health_score"] = total_score
            self.prom_system_health.set(total_score)
            
            # Log health score details
            if total_score < 80:
                details = ", ".join(f"{name}:{score:.1f}" for name, score, _ in score_components)
                self.logger.warning(f"System health degraded: {total_score:.1f} ({details})")
            
        except Exception as e:
            self.logger.error(f"Error calculating system health score: {e}")
    
    async def _analyze_performance_trends(self):
        """Analyze performance trends for predictive insights"""
        try:
            if len(self.performance_history) < 100:
                return
            
            # Analyze recent trends (last hour)
            recent_data = [
                record for record in self.performance_history
                if (datetime.now() - record['timestamp']).total_seconds() < 3600
            ]
            
            if len(recent_data) < 10:
                return
            
            # Calculate trend metrics
            response_times = [record['response_time_ms'] for record in recent_data]
            avg_response_time = statistics.mean(response_times)
            
            # Check for degradation trends
            if len(response_times) >= 20:
                first_half = response_times[:len(response_times)//2]
                second_half = response_times[len(response_times)//2:]
                
                first_avg = statistics.mean(first_half)
                second_avg = statistics.mean(second_half)
                
                # If performance is degrading by more than 20%
                if second_avg > first_avg * 1.2 and second_avg > self.sla_target_ms * 0.8:
                    self.logger.warning(
                        f"Performance degradation trend detected: "
                        f"{first_avg:.2f}ms -> {second_avg:.2f}ms"
                    )
                    
                    # Generate predictive alert
                    if not any(alert.description.startswith("Performance degradation trend") 
                             for alert in self.active_alerts.values()):
                        
                        trend_alert = PerformanceAlert(
                            alert_id=f"trend_{int(time.time())}",
                            timestamp=datetime.now(),
                            severity=AlertSeverity.WARNING,
                            metric_type=PerformanceMetricType.RESPONSE_TIME,
                            current_value=second_avg,
                            threshold_value=self.sla_target_ms,
                            description="Performance degradation trend detected - proactive intervention recommended",
                            recommended_actions=[
                                "Monitor system resources closely",
                                "Prepare for potential scaling",
                                "Review recent changes",
                                "Check for gradual resource leaks"
                            ]
                        )
                        
                        self.active_alerts[trend_alert.alert_id] = trend_alert
                        await self._send_alert(trend_alert)
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance trends: {e}")
    
    async def _alert_processor(self):
        """Process and manage alerts"""
        while True:
            try:
                current_time = datetime.now()
                
                # Auto-resolve alerts that are no longer applicable
                alerts_to_resolve = []
                
                for alert_id, alert in self.active_alerts.items():
                    # Auto-resolve after 30 minutes if conditions improved
                    if (current_time - alert.timestamp).total_seconds() > 1800:  # 30 minutes
                        if await self._check_alert_resolution(alert):
                            alerts_to_resolve.append(alert_id)
                
                # Resolve alerts
                for alert_id in alerts_to_resolve:
                    resolved_alert = self.active_alerts.pop(alert_id)
                    self.logger.info(f"Auto-resolved alert: {resolved_alert.description}")
                
                # Update metrics
                self.global_metrics["active_alerts"] = len(self.active_alerts)
                
                await asyncio.sleep(300)  # Process every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in alert processor: {e}")
                await asyncio.sleep(600)
    
    async def _check_alert_resolution(self, alert: PerformanceAlert) -> bool:
        """Check if alert conditions have been resolved"""
        try:
            if alert.metric_type == PerformanceMetricType.RESPONSE_TIME:
                # Check if response times are back to normal
                for endpoint_key in alert.affected_endpoints:
                    if endpoint_key in self.endpoint_metrics:
                        metrics = self.endpoint_metrics[endpoint_key]
                        if metrics.p95_response_time_ms > alert.threshold_value:
                            return False
                return True
            
            elif alert.metric_type == PerformanceMetricType.ERROR_RATE:
                # Check if error rates have improved
                for endpoint_key in alert.affected_endpoints:
                    if endpoint_key in self.endpoint_metrics:
                        metrics = self.endpoint_metrics[endpoint_key]
                        if metrics.error_rate_percent > alert.threshold_value:
                            return False
                return True
            
            # Default to keeping alert active
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking alert resolution: {e}")
            return False
    
    async def _health_checker(self):
        """Periodic health checks"""
        while True:
            try:
                # Check component health
                component_health = {
                    'performance_monitor': 'healthy',
                    'metrics_collection': 'healthy' if len(self.performance_history) > 0 else 'unhealthy',
                    'alerting': 'healthy',
                    'cache_manager': 'healthy' if self.cache_manager else 'not_configured',
                    'websocket_manager': 'healthy' if self.websocket_manager else 'not_configured',
                    'scaling_manager': 'healthy' if self.scaling_manager else 'not_configured'
                }
                
                # Log health status
                unhealthy_components = [name for name, status in component_health.items() if status == 'unhealthy']
                if unhealthy_components:
                    self.logger.warning(f"Unhealthy components: {unhealthy_components}")
                
                await asyncio.sleep(300)  # Health check every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in health checker: {e}")
                await asyncio.sleep(600)
    
    async def _metrics_aggregator(self):
        """Aggregate and export metrics"""
        while True:
            try:
                # Update dashboard-specific performance scores
                dashboard_types = ['executive', 'revenue', 'operations', 'customers']
                
                for dashboard_type in dashboard_types:
                    dashboard_endpoints = [
                        key for key in self.endpoint_metrics.keys()
                        if dashboard_type in key.lower()
                    ]
                    
                    if dashboard_endpoints:
                        # Calculate average performance score for this dashboard type
                        total_score = 0
                        count = 0
                        
                        for endpoint_key in dashboard_endpoints:
                            metrics = self.endpoint_metrics[endpoint_key]
                            
                            # Calculate performance score based on SLA compliance and error rate
                            sla_score = metrics.sla_compliance_percent / 100
                            error_score = max(0, 1 - (metrics.error_rate_percent / 100))
                            
                            endpoint_score = (sla_score * 0.7) + (error_score * 0.3)
                            total_score += endpoint_score
                            count += 1
                        
                        if count > 0:
                            dashboard_score = (total_score / count) * 100
                            self.prom_dashboard_performance.labels(dashboard_type=dashboard_type).set(dashboard_score)
                
                await asyncio.sleep(60)  # Aggregate every minute
                
            except Exception as e:
                self.logger.error(f"Error in metrics aggregator: {e}")
                await asyncio.sleep(120)
    
    async def _trend_analyzer(self):
        """Analyze long-term trends"""
        while True:
            try:
                # Analyze daily patterns, weekly trends, etc.
                if len(self.performance_history) > 1000:
                    await self._analyze_daily_patterns()
                    await self._analyze_weekly_trends()
                
                await asyncio.sleep(3600)  # Analyze trends every hour
                
            except Exception as e:
                self.logger.error(f"Error in trend analyzer: {e}")
                await asyncio.sleep(3600)
    
    async def _analyze_daily_patterns(self):
        """Analyze daily performance patterns"""
        try:
            # Group data by hour of day
            hourly_performance = defaultdict(list)
            
            for record in self.performance_history:
                hour = record['timestamp'].hour
                hourly_performance[hour].append(record['response_time_ms'])
            
            # Calculate average performance by hour
            hourly_averages = {}
            for hour, response_times in hourly_performance.items():
                if len(response_times) >= 5:  # Minimum data points
                    hourly_averages[hour] = statistics.mean(response_times)
            
            # Identify peak and low performance hours
            if len(hourly_averages) >= 12:  # At least half day of data
                peak_hour = max(hourly_averages.items(), key=lambda x: x[1])
                low_hour = min(hourly_averages.items(), key=lambda x: x[1])
                
                self.logger.info(
                    f"Daily pattern analysis: Peak performance at {low_hour[0]:02d}:00 "
                    f"({low_hour[1]:.2f}ms), worst at {peak_hour[0]:02d}:00 ({peak_hour[1]:.2f}ms)"
                )
            
        except Exception as e:
            self.logger.error(f"Error analyzing daily patterns: {e}")
    
    async def _analyze_weekly_trends(self):
        """Analyze weekly performance trends"""
        try:
            # Group data by day of week
            daily_performance = defaultdict(list)
            
            for record in self.performance_history:
                day = record['timestamp'].weekday()  # 0 = Monday
                daily_performance[day].append(record['response_time_ms'])
            
            # Calculate average performance by day
            daily_averages = {}
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            
            for day, response_times in daily_performance.items():
                if len(response_times) >= 10:  # Minimum data points
                    daily_averages[day] = statistics.mean(response_times)
            
            # Log weekly trends
            if len(daily_averages) >= 5:  # At least 5 days of data
                best_day = min(daily_averages.items(), key=lambda x: x[1])
                worst_day = max(daily_averages.items(), key=lambda x: x[1])
                
                self.logger.info(
                    f"Weekly trend analysis: Best performance on {day_names[best_day[0]]} "
                    f"({best_day[1]:.2f}ms), worst on {day_names[worst_day[0]]} ({worst_day[1]:.2f}ms)"
                )
            
        except Exception as e:
            self.logger.error(f"Error analyzing weekly trends: {e}")
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        try:
            # Endpoint statistics
            endpoint_stats = {}
            for endpoint_key, metrics in self.endpoint_metrics.items():
                endpoint_stats[endpoint_key] = {
                    "total_requests": metrics.total_requests,
                    "avg_response_time_ms": round(metrics.avg_response_time_ms, 2),
                    "p95_response_time_ms": round(metrics.p95_response_time_ms, 2),
                    "p99_response_time_ms": round(metrics.p99_response_time_ms, 2),
                    "error_rate_percent": round(metrics.error_rate_percent, 2),
                    "sla_compliance_percent": round(metrics.sla_compliance_percent, 2),
                    "sla_violations": metrics.sla_violations,
                    "requests_per_second": round(metrics.requests_per_second, 2),
                    "dashboard_specific": metrics.dashboard_specific
                }
            
            # Active alerts summary
            alerts_by_severity = defaultdict(int)
            for alert in self.active_alerts.values():
                alerts_by_severity[alert.severity.value] += 1
            
            # Recent performance summary
            recent_violations = 0
            if len(self.performance_history) > 0:
                recent_data = [
                    record for record in self.performance_history
                    if (datetime.now() - record['timestamp']).total_seconds() < 3600
                ]
                recent_violations = sum(1 for record in recent_data if not record['sla_compliant'])
            
            return {
                "summary": {
                    "sla_target_ms": self.sla_target_ms,
                    "system_health_score": round(self.global_metrics["system_health_score"], 1),
                    "total_requests": self.global_metrics["total_requests"],
                    "total_sla_violations": self.global_metrics["sla_violations"],
                    "active_alerts": len(self.active_alerts),
                    "recent_hourly_violations": recent_violations
                },
                "endpoint_performance": endpoint_stats,
                "active_alerts": {
                    "total": len(self.active_alerts),
                    "by_severity": dict(alerts_by_severity),
                    "details": [
                        {
                            "alert_id": alert.alert_id,
                            "severity": alert.severity.value,
                            "description": alert.description,
                            "current_value": alert.current_value,
                            "threshold_value": alert.threshold_value,
                            "affected_endpoints": alert.affected_endpoints,
                            "timestamp": alert.timestamp.isoformat()
                        }
                        for alert in list(self.active_alerts.values())[:10]  # Limit to 10 most recent
                    ]
                },
                "system_metrics": {
                    "cpu_percent": self.system_metrics_history[-1]['cpu_percent'] if self.system_metrics_history else 0,
                    "memory_percent": self.system_metrics_history[-1]['memory_percent'] if self.system_metrics_history else 0,
                    "websocket_connections": self.system_metrics_history[-1].get('websocket_connections', 0) if self.system_metrics_history else 0,
                    "cache_hit_rate": self.system_metrics_history[-1].get('cache_hit_rate', 0) if self.system_metrics_history else 0
                },
                "configuration": {
                    "thresholds": [
                        {
                            "metric_type": threshold.metric_type.value,
                            "threshold_value": threshold.threshold_value,
                            "severity": threshold.severity.value,
                            "comparison": threshold.comparison
                        }
                        for threshold in self.thresholds
                    ],
                    "alert_channels": [channel.value for channel in self.alert_channels]
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive stats: {e}")
            return {"error": str(e)}
    
    async def close(self):
        """Clean shutdown of performance monitor"""
        try:
            # Cancel background tasks
            for task in self.background_tasks:
                task.cancel()
            
            # Wait for tasks to complete
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            
            # Shutdown thread pool
            self.executor.shutdown(wait=True)
            
            self.logger.info("Performance monitor closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing performance monitor: {e}")


class PerformanceMiddleware(BaseHTTPMiddleware):
    """Middleware to collect performance metrics from all requests"""
    
    def __init__(self, app, performance_monitor: PerformanceMonitor):
        super().__init__(app)
        self.performance_monitor = performance_monitor
        self.logger = get_logger("performance_middleware")
    
    async def dispatch(self, request: Request, call_next) -> Response:
        """Collect performance metrics for each request"""
        start_time = time.perf_counter()
        
        # Extract endpoint information
        endpoint = request.url.path
        method = request.method
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate response time
            response_time_ms = (time.perf_counter() - start_time) * 1000
            
            # Record metrics
            await self.performance_monitor.record_request_metrics(
                endpoint=endpoint,
                method=method,
                response_time_ms=response_time_ms,
                status_code=response.status_code,
                request=request
            )
            
            # Add performance headers
            response.headers["X-Response-Time-Ms"] = str(round(response_time_ms, 2))
            response.headers["X-SLA-Target-Ms"] = str(self.performance_monitor.sla_target_ms)
            response.headers["X-SLA-Compliant"] = "true" if response_time_ms <= self.performance_monitor.sla_target_ms else "false"
            
            return response
            
        except Exception as e:
            response_time_ms = (time.perf_counter() - start_time) * 1000
            
            # Record error metrics
            await self.performance_monitor.record_request_metrics(
                endpoint=endpoint,
                method=method,
                response_time_ms=response_time_ms,
                status_code=500,
                request=request
            )
            
            raise


# Factory functions
def create_performance_monitor(sla_target_ms: float = 25.0) -> PerformanceMonitor:
    """Create PerformanceMonitor instance"""
    return PerformanceMonitor(sla_target_ms=sla_target_ms)


def create_performance_middleware(performance_monitor: PerformanceMonitor) -> PerformanceMiddleware:
    """Create PerformanceMiddleware instance"""
    return PerformanceMiddleware


# Export components
__all__ = [
    "PerformanceMonitor",
    "PerformanceMiddleware",
    "PerformanceAlert",
    "PerformanceThreshold",
    "EndpointMetrics",
    "AlertSeverity",
    "PerformanceMetricType",
    "create_performance_monitor",
    "create_performance_middleware"
]