"""
DataDog Technical Operations Dashboards
Provides comprehensive technical operations dashboards for infrastructure performance,
application monitoring, database optimization, and system operations
"""

import asyncio
import json
import time
import psutil
import platform
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_dashboard_integration import DataDogDashboardManager

logger = get_logger(__name__)


class TechnicalDashboardType(Enum):
    """Types of technical operations dashboards"""
    INFRASTRUCTURE_OVERVIEW = "infrastructure_overview"
    APPLICATION_PERFORMANCE = "application_performance"  
    DATABASE_OPERATIONS = "database_operations"
    MESSAGE_QUEUES = "message_queues"
    CACHING_PERFORMANCE = "caching_performance"
    NETWORK_MONITORING = "network_monitoring"
    ERROR_TRACKING = "error_tracking"
    SECURITY_OPERATIONS = "security_operations"
    CAPACITY_PLANNING = "capacity_planning"


class AlertSeverity(Enum):
    """Alert severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class TechnicalMetric:
    """Technical metric definition"""
    name: str
    description: str
    metric_type: str  # gauge, counter, histogram, distribution
    unit: str
    alert_thresholds: Optional[Dict[str, float]] = None
    collection_interval: int = 60  # seconds
    retention_days: int = 30
    tags: Optional[List[str]] = None


@dataclass
class ServiceHealthCheck:
    """Service health check definition"""
    service_name: str
    check_type: str  # http, tcp, process, custom
    endpoint: Optional[str] = None
    expected_status: str = "healthy"
    timeout_seconds: int = 30
    retry_count: int = 3
    critical_threshold: float = 95.0  # percentage uptime
    warning_threshold: float = 99.0


class DataDogTechnicalOperationsDashboards:
    """
    Technical Operations Dashboards for comprehensive system monitoring
    
    Features:
    - Infrastructure performance monitoring
    - Application performance monitoring (APM)
    - Database and caching performance tracking
    - Message queue and streaming metrics
    - Error rate and incident tracking
    - Network monitoring and security operations
    - Capacity planning and resource optimization
    - Automated health checks and alerting
    """
    
    def __init__(self, service_name: str = "technical-operations",
                 datadog_monitoring: Optional[DatadogMonitoring] = None,
                 dashboard_manager: Optional[DataDogDashboardManager] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.dashboard_manager = dashboard_manager
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Technical metrics registry
        self.technical_metrics: Dict[str, TechnicalMetric] = {}
        self.health_checks: Dict[str, ServiceHealthCheck] = {}
        self.dashboard_definitions: Dict[TechnicalDashboardType, Dict[str, Any]] = {}
        
        # Performance tracking
        self.system_stats = {
            "cpu_percent": 0.0,
            "memory_percent": 0.0,
            "disk_percent": 0.0,
            "network_io": {"bytes_sent": 0, "bytes_recv": 0},
            "last_updated": datetime.utcnow()
        }
        
        # Alert tracking
        self.alert_history = []
        self.service_status = {}
        
        # Initialize components
        self._initialize_technical_metrics()
        self._initialize_health_checks()
        self._initialize_dashboard_definitions()
        
        self.logger.info(f"Technical operations dashboards initialized for {service_name}")
    
    def _initialize_technical_metrics(self):
        """Initialize technical metrics definitions"""
        
        technical_metrics = [
            # Infrastructure Metrics
            TechnicalMetric(
                name="cpu_utilization",
                description="CPU utilization percentage",
                metric_type="gauge",
                unit="percentage",
                alert_thresholds={"warning": 70.0, "critical": 90.0},
                tags=["infrastructure", "performance"]
            ),
            TechnicalMetric(
                name="memory_utilization", 
                description="Memory utilization percentage",
                metric_type="gauge",
                unit="percentage",
                alert_thresholds={"warning": 75.0, "critical": 90.0},
                tags=["infrastructure", "performance"]
            ),
            TechnicalMetric(
                name="disk_utilization",
                description="Disk utilization percentage",
                metric_type="gauge", 
                unit="percentage",
                alert_thresholds={"warning": 80.0, "critical": 95.0},
                tags=["infrastructure", "storage"]
            ),
            TechnicalMetric(
                name="network_throughput",
                description="Network throughput in bytes per second",
                metric_type="gauge",
                unit="bytes_per_second",
                alert_thresholds={"warning": 1000000000.0, "critical": 1500000000.0},  # 1GB, 1.5GB
                tags=["infrastructure", "network"]
            ),
            
            # Application Performance Metrics
            TechnicalMetric(
                name="api_response_time_p95",
                description="95th percentile API response time",
                metric_type="histogram",
                unit="milliseconds",
                alert_thresholds={"warning": 500.0, "critical": 1000.0},
                tags=["application", "performance"]
            ),
            TechnicalMetric(
                name="api_error_rate",
                description="API error rate percentage",
                metric_type="gauge",
                unit="percentage",
                alert_thresholds={"warning": 1.0, "critical": 5.0},
                tags=["application", "errors"]
            ),
            TechnicalMetric(
                name="application_throughput",
                description="Application requests per second",
                metric_type="counter",
                unit="requests_per_second",
                alert_thresholds={"warning": 1000.0, "critical": 10000.0},
                tags=["application", "throughput"]
            ),
            
            # Database Metrics
            TechnicalMetric(
                name="database_connection_pool",
                description="Database connection pool utilization",
                metric_type="gauge",
                unit="percentage",
                alert_thresholds={"warning": 80.0, "critical": 95.0},
                tags=["database", "connections"]
            ),
            TechnicalMetric(
                name="database_query_time_p95",
                description="95th percentile database query time",
                metric_type="histogram", 
                unit="milliseconds",
                alert_thresholds={"warning": 100.0, "critical": 500.0},
                tags=["database", "performance"]
            ),
            TechnicalMetric(
                name="database_deadlocks",
                description="Number of database deadlocks per minute",
                metric_type="counter",
                unit="count_per_minute",
                alert_thresholds={"warning": 1.0, "critical": 5.0},
                tags=["database", "errors"]
            ),
            
            # Message Queue Metrics
            TechnicalMetric(
                name="message_queue_depth",
                description="Message queue depth",
                metric_type="gauge",
                unit="count",
                alert_thresholds={"warning": 1000.0, "critical": 10000.0},
                tags=["messaging", "queues"]
            ),
            TechnicalMetric(
                name="message_processing_time",
                description="Message processing time",
                metric_type="histogram",
                unit="milliseconds", 
                alert_thresholds={"warning": 1000.0, "critical": 5000.0},
                tags=["messaging", "performance"]
            ),
            
            # Caching Metrics
            TechnicalMetric(
                name="cache_hit_rate",
                description="Cache hit rate percentage",
                metric_type="gauge",
                unit="percentage",
                alert_thresholds={"warning": 70.0, "critical": 50.0},  # Lower is worse
                tags=["caching", "performance"]
            ),
            TechnicalMetric(
                name="cache_memory_usage",
                description="Cache memory usage percentage",
                metric_type="gauge",
                unit="percentage",
                alert_thresholds={"warning": 80.0, "critical": 95.0},
                tags=["caching", "memory"]
            )
        ]
        
        for metric in technical_metrics:
            self.technical_metrics[metric.name] = metric
        
        self.logger.info(f"Initialized {len(technical_metrics)} technical metrics")
    
    def _initialize_health_checks(self):
        """Initialize service health checks"""
        
        health_checks = [
            ServiceHealthCheck(
                service_name="api_gateway",
                check_type="http",
                endpoint="/health",
                expected_status="healthy",
                timeout_seconds=10,
                critical_threshold=99.5,
                warning_threshold=99.9
            ),
            ServiceHealthCheck(
                service_name="database_primary",
                check_type="tcp",
                endpoint="localhost:5432",
                expected_status="healthy",
                timeout_seconds=5,
                critical_threshold=99.9,
                warning_threshold=99.95
            ),
            ServiceHealthCheck(
                service_name="redis_cache",
                check_type="tcp",
                endpoint="localhost:6379",
                expected_status="healthy",
                timeout_seconds=5,
                critical_threshold=99.0,
                warning_threshold=99.5
            ),
            ServiceHealthCheck(
                service_name="rabbitmq",
                check_type="http",
                endpoint="/api/healthchecks/node",
                expected_status="healthy",
                timeout_seconds=10,
                critical_threshold=99.0,
                warning_threshold=99.5
            ),
            ServiceHealthCheck(
                service_name="elasticsearch",
                check_type="http",
                endpoint="/_cluster/health",
                expected_status="green",
                timeout_seconds=15,
                critical_threshold=99.0,
                warning_threshold=99.5
            )
        ]
        
        for check in health_checks:
            self.health_checks[check.service_name] = check
        
        self.logger.info(f"Initialized {len(health_checks)} health checks")
    
    def _initialize_dashboard_definitions(self):
        """Initialize technical dashboard definitions"""
        
        # Infrastructure Overview Dashboard
        self.dashboard_definitions[TechnicalDashboardType.INFRASTRUCTURE_OVERVIEW] = {
            "title": "Infrastructure Overview - System Performance",
            "description": "Comprehensive infrastructure performance and resource utilization monitoring",
            "refresh_interval": "30s",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "CPU Utilization",
                    "query": "avg:system.cpu.utilization{*}",
                    "precision": 1,
                    "format": "percentage",
                    "conditional_formatting": [
                        {"comparator": ">", "value": 90, "palette": "red_on_white"},
                        {"comparator": ">", "value": 70, "palette": "yellow_on_white"}
                    ]
                },
                {
                    "type": "query_value", 
                    "title": "Memory Utilization",
                    "query": "avg:system.memory.utilization{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Disk Utilization",
                    "query": "avg:system.disk.utilization{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Network Throughput",
                    "query": "sum:system.network.throughput{*}",
                    "precision": 0,
                    "format": "bytes"
                },
                {
                    "type": "timeseries",
                    "title": "CPU Usage Trend (24h)",
                    "query": "avg:system.cpu.utilization{*} by {host}",
                    "timeframe": "24h"
                },
                {
                    "type": "timeseries",
                    "title": "Memory Usage Trend (24h)",
                    "query": "avg:system.memory.utilization{*} by {host}",
                    "timeframe": "24h"
                },
                {
                    "type": "heatmap",
                    "title": "Host Performance Heatmap",
                    "query": "avg:system.cpu.utilization{*} by {host}"
                },
                {
                    "type": "toplist",
                    "title": "Top Hosts by Resource Usage",
                    "query": "avg:system.resource.combined{*} by {host}",
                    "limit": 20
                }
            ]
        }
        
        # Application Performance Dashboard
        self.dashboard_definitions[TechnicalDashboardType.APPLICATION_PERFORMANCE] = {
            "title": "Application Performance Monitoring (APM)",
            "description": "Application performance metrics, response times, and error tracking",
            "refresh_interval": "1m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Requests per Minute",
                    "query": "sum:api.requests.total{*}.as_rate()",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Average Response Time",
                    "query": "avg:api.response_time{*}",
                    "precision": 0,
                    "format": "duration"
                },
                {
                    "type": "query_value",
                    "title": "Error Rate",
                    "query": "sum:api.errors.total{*}.as_rate() / sum:api.requests.total{*}.as_rate() * 100",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Apdex Score",
                    "query": "avg:api.apdex{*}",
                    "precision": 3,
                    "format": "number"
                },
                {
                    "type": "timeseries",
                    "title": "Response Time Distribution",
                    "query": "percentile:api.response_time{*}:50, percentile:api.response_time{*}:95, percentile:api.response_time{*}:99",
                    "timeframe": "4h"
                },
                {
                    "type": "timeseries",
                    "title": "Request Volume by Service",
                    "query": "sum:api.requests.total{*}.as_rate() by {service}",
                    "timeframe": "4h"
                },
                {
                    "type": "service_map",
                    "title": "Service Dependencies",
                    "services": ["api-gateway", "user-service", "order-service", "payment-service"]
                },
                {
                    "type": "trace_search",
                    "title": "Recent Error Traces",
                    "query": "status:error",
                    "limit": 50
                }
            ]
        }
        
        # Database Operations Dashboard
        self.dashboard_definitions[TechnicalDashboardType.DATABASE_OPERATIONS] = {
            "title": "Database Operations - Performance & Health",
            "description": "Database performance, connection pools, and query optimization monitoring",
            "refresh_interval": "2m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Active Connections",
                    "query": "avg:database.connections.active{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Connection Pool Utilization",
                    "query": "avg:database.connection_pool.utilization{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Average Query Time",
                    "query": "avg:database.query_time{*}",
                    "precision": 1,
                    "format": "duration"
                },
                {
                    "type": "query_value",
                    "title": "Deadlocks per Hour",
                    "query": "sum:database.deadlocks{*}.as_rate() * 3600",
                    "precision": 1,
                    "format": "number"
                },
                {
                    "type": "timeseries",
                    "title": "Query Performance Trends",
                    "query": "percentile:database.query_time{*}:50, percentile:database.query_time{*}:95",
                    "timeframe": "12h"
                },
                {
                    "type": "timeseries",
                    "title": "Connection Pool Usage",
                    "query": "avg:database.connections.active{*}, avg:database.connections.max{*}",
                    "timeframe": "12h"
                },
                {
                    "type": "toplist",
                    "title": "Slowest Queries",
                    "query": "avg:database.query_time{*} by {query_hash}",
                    "limit": 15
                },
                {
                    "type": "table",
                    "title": "Database Health Summary",
                    "query": "avg:database.health.score{*} by {database}",
                    "columns": ["Database", "Health Score", "Status"]
                }
            ]
        }
        
        # Message Queues Dashboard
        self.dashboard_definitions[TechnicalDashboardType.MESSAGE_QUEUES] = {
            "title": "Message Queues - Streaming & Processing",
            "description": "Message queue performance, consumer lag, and throughput monitoring",
            "refresh_interval": "1m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Messages per Second",
                    "query": "sum:messaging.messages.processed{*}.as_rate()",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Average Queue Depth",
                    "query": "avg:messaging.queue.depth{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Consumer Lag (Max)",
                    "query": "max:messaging.consumer.lag{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Processing Time P95",
                    "query": "percentile:messaging.processing_time{*}:95",
                    "precision": 0,
                    "format": "duration"
                },
                {
                    "type": "timeseries",
                    "title": "Message Throughput by Topic",
                    "query": "sum:messaging.messages.processed{*}.as_rate() by {topic}",
                    "timeframe": "6h"
                },
                {
                    "type": "timeseries",
                    "title": "Consumer Lag Trends",
                    "query": "avg:messaging.consumer.lag{*} by {consumer_group}",
                    "timeframe": "6h"
                },
                {
                    "type": "distribution",
                    "title": "Message Size Distribution",
                    "query": "avg:messaging.message.size{*}"
                },
                {
                    "type": "heat_map",
                    "title": "Processing Time Heatmap",
                    "query": "avg:messaging.processing_time{*} by {queue}"
                }
            ]
        }
        
        # Error Tracking Dashboard
        self.dashboard_definitions[TechnicalDashboardType.ERROR_TRACKING] = {
            "title": "Error Tracking - Incidents & Resolution",
            "description": "Error monitoring, incident tracking, and system reliability metrics",
            "refresh_interval": "1m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Error Rate",
                    "query": "sum:errors.total{*}.as_rate()",
                    "precision": 2,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Critical Errors",
                    "query": "sum:errors.critical{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Mean Time to Resolution",
                    "query": "avg:incidents.mttr{*}",
                    "precision": 1,
                    "format": "duration"
                },
                {
                    "type": "query_value",
                    "title": "System Reliability",
                    "query": "avg:system.reliability.score{*}",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "timeseries",
                    "title": "Error Rate by Service",
                    "query": "sum:errors.total{*}.as_rate() by {service}",
                    "timeframe": "24h"
                },
                {
                    "type": "timeseries",
                    "title": "Error Types Over Time",
                    "query": "sum:errors.total{*}.as_rate() by {error_type}",
                    "timeframe": "24h"
                },
                {
                    "type": "alert_graph",
                    "title": "Active Alerts Timeline",
                    "alert_id": "*"
                },
                {
                    "type": "log_stream",
                    "title": "Recent Error Logs",
                    "query": "status:error",
                    "columns": ["timestamp", "service", "message"],
                    "sort": {"column": "timestamp", "order": "desc"}
                }
            ]
        }
        
        # Security Operations Dashboard  
        self.dashboard_definitions[TechnicalDashboardType.SECURITY_OPERATIONS] = {
            "title": "Security Operations - Threats & Compliance",
            "description": "Security monitoring, threat detection, and compliance tracking",
            "refresh_interval": "30s",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Security Events",
                    "query": "sum:security.events.total{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Failed Login Attempts",
                    "query": "sum:security.login.failed{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Threat Level",
                    "query": "max:security.threat.level{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Compliance Score",
                    "query": "avg:security.compliance.score{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "timeseries",
                    "title": "Security Events Timeline",
                    "query": "sum:security.events.total{*} by {event_type}",
                    "timeframe": "24h"
                },
                {
                    "type": "geomap",
                    "title": "Security Events by Location",
                    "query": "sum:security.events.total{*} by {country}"
                },
                {
                    "type": "toplist",
                    "title": "Top Security Threats",
                    "query": "sum:security.threats{*} by {threat_type}",
                    "limit": 10
                },
                {
                    "type": "slo_summary",
                    "title": "Security SLA Status",
                    "slo_ids": ["security_response_time", "incident_resolution"]
                }
            ]
        }
        
        # Capacity Planning Dashboard
        self.dashboard_definitions[TechnicalDashboardType.CAPACITY_PLANNING] = {
            "title": "Capacity Planning - Resource Optimization",
            "description": "Resource capacity planning and optimization recommendations",
            "refresh_interval": "5m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Peak CPU Usage",
                    "query": "max:system.cpu.utilization{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Memory Growth Rate",
                    "query": "rate(avg:system.memory.utilization{*})",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Storage Capacity Remaining",
                    "query": "100 - avg:system.disk.utilization{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Estimated Days to Capacity",
                    "query": "forecast:system.disk.utilization{*}:100",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "timeseries",
                    "title": "Resource Usage Forecast (30 days)",
                    "query": "forecast:system.resource.combined{*}",
                    "timeframe": "30d"
                },
                {
                    "type": "timeseries",
                    "title": "Cost Optimization Trends",
                    "query": "sum:infrastructure.costs{*} by {service}",
                    "timeframe": "90d"
                },
                {
                    "type": "scatter_plot",
                    "title": "Cost vs Performance Matrix",
                    "x_axis": "avg:infrastructure.cost{*}",
                    "y_axis": "avg:infrastructure.performance{*}"
                },
                {
                    "type": "note",
                    "title": "Capacity Recommendations",
                    "content": "Automated capacity planning recommendations based on current trends and forecasts"
                }
            ]
        }
        
        self.logger.info(f"Initialized {len(self.dashboard_definitions)} technical dashboard definitions")
    
    async def create_technical_dashboard(self, dashboard_type: TechnicalDashboardType,
                                       custom_title: Optional[str] = None,
                                       custom_widgets: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
        """Create a technical operations dashboard"""
        
        try:
            if dashboard_type not in self.dashboard_definitions:
                self.logger.error(f"Unknown dashboard type: {dashboard_type}")
                return None
            
            definition = self.dashboard_definitions[dashboard_type]
            
            # Use dashboard manager to create the dashboard
            if self.dashboard_manager:
                dashboard_id = await self.dashboard_manager.create_dashboard(
                    dashboard_type.value,
                    custom_title or definition["title"],
                    definition.get("description", ""),
                    custom_widgets or definition["widgets"]
                )
                
                if dashboard_id and self.datadog_monitoring:
                    # Track dashboard creation
                    self.datadog_monitoring.counter(
                        "technical.dashboard.created",
                        tags=[
                            f"dashboard_type:{dashboard_type.value}",
                            f"service:{self.service_name}"
                        ]
                    )
                
                return dashboard_id
            else:
                self.logger.error("Dashboard manager not available")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to create technical dashboard {dashboard_type}: {str(e)}")
            return None
    
    async def create_all_technical_dashboards(self) -> Dict[TechnicalDashboardType, Optional[str]]:
        """Create all technical operations dashboards"""
        
        results = {}
        
        for dashboard_type in TechnicalDashboardType:
            try:
                dashboard_id = await self.create_technical_dashboard(dashboard_type)
                results[dashboard_type] = dashboard_id
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(1.5)
                
            except Exception as e:
                self.logger.error(f"Failed to create dashboard {dashboard_type}: {str(e)}")
                results[dashboard_type] = None
        
        success_count = sum(1 for result in results.values() if result is not None)
        total_count = len(results)
        
        self.logger.info(f"Technical dashboard creation completed: {success_count}/{total_count} successful")
        
        # Track overall success
        if self.datadog_monitoring:
            self.datadog_monitoring.gauge(
                "technical.dashboards.created_total",
                success_count,
                tags=[f"service:{self.service_name}"]
            )
        
        return results
    
    async def collect_system_metrics(self) -> Dict[str, Any]:
        """Collect real-time system metrics"""
        
        try:
            # Get current system stats
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            network = psutil.net_io_counters()
            
            # Update system stats
            self.system_stats.update({
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": (disk.used / disk.total) * 100,
                "network_io": {
                    "bytes_sent": network.bytes_sent,
                    "bytes_recv": network.bytes_recv
                },
                "last_updated": datetime.utcnow()
            })
            
            # Send metrics to DataDog
            if self.datadog_monitoring:
                await self._send_system_metrics_to_datadog()
            
            # Additional system information
            system_info = {
                "platform": platform.system(),
                "platform_version": platform.release(),
                "architecture": platform.machine(),
                "hostname": platform.node(),
                "python_version": platform.python_version(),
                "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat()
            }
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "metrics": self.system_stats.copy(),
                "system_info": system_info
            }
            
        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {str(e)}")
            return {}
    
    async def _send_system_metrics_to_datadog(self):
        """Send system metrics to DataDog"""
        
        try:
            hostname = platform.node()
            tags = [f"hostname:{hostname}", f"service:{self.service_name}"]
            
            # Send system metrics
            self.datadog_monitoring.gauge("system.cpu.utilization", self.system_stats["cpu_percent"], tags=tags)
            self.datadog_monitoring.gauge("system.memory.utilization", self.system_stats["memory_percent"], tags=tags)
            self.datadog_monitoring.gauge("system.disk.utilization", self.system_stats["disk_percent"], tags=tags)
            
            # Send network metrics
            self.datadog_monitoring.gauge("system.network.bytes_sent", self.system_stats["network_io"]["bytes_sent"], tags=tags)
            self.datadog_monitoring.gauge("system.network.bytes_recv", self.system_stats["network_io"]["bytes_recv"], tags=tags)
            
            # Calculate and send combined resource usage
            combined_resource = (
                self.system_stats["cpu_percent"] + 
                self.system_stats["memory_percent"] + 
                self.system_stats["disk_percent"]
            ) / 3
            
            self.datadog_monitoring.gauge("system.resource.combined", combined_resource, tags=tags)
            
        except Exception as e:
            self.logger.error(f"Failed to send system metrics to DataDog: {str(e)}")
    
    async def track_technical_metric(self, metric_name: str, value: float,
                                   tags: Optional[Dict[str, str]] = None) -> bool:
        """Track a technical metric value"""
        
        try:
            if metric_name not in self.technical_metrics:
                self.logger.warning(f"Unknown technical metric: {metric_name}")
                return False
            
            metric = self.technical_metrics[metric_name]
            
            # Send to DataDog with technical tags
            if self.datadog_monitoring:
                dd_tags = [
                    f"metric:{metric_name}",
                    f"unit:{metric.unit}",
                    f"service:{self.service_name}"
                ]
                
                if metric.tags:
                    dd_tags.extend(metric.tags)
                
                if tags:
                    dd_tags.extend([f"{k}:{v}" for k, v in tags.items()])
                
                # Send metric based on type
                if metric.metric_type == "gauge":
                    self.datadog_monitoring.gauge(f"technical.{metric_name}", value, tags=dd_tags)
                elif metric.metric_type == "counter":
                    self.datadog_monitoring.counter(f"technical.{metric_name}", tags=dd_tags)
                elif metric.metric_type == "histogram":
                    self.datadog_monitoring.histogram(f"technical.{metric_name}", value, tags=dd_tags)
                
                # Check thresholds
                await self._check_technical_threshold(metric, value)
            
            self.logger.debug(f"Tracked technical metric {metric_name}: {value}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track technical metric {metric_name}: {str(e)}")
            return False
    
    async def _check_technical_threshold(self, metric: TechnicalMetric, value: float):
        """Check technical metric thresholds and generate alerts"""
        
        try:
            if not metric.alert_thresholds:
                return
            
            alert_level = None
            
            # Check thresholds based on metric characteristics
            if metric.name in ["cache_hit_rate"]:  # Lower is worse for these metrics
                if "critical" in metric.alert_thresholds and value <= metric.alert_thresholds["critical"]:
                    alert_level = "critical"
                elif "warning" in metric.alert_thresholds and value <= metric.alert_thresholds["warning"]:
                    alert_level = "warning"
            else:  # Higher is worse for most metrics
                if "critical" in metric.alert_thresholds and value >= metric.alert_thresholds["critical"]:
                    alert_level = "critical"
                elif "warning" in metric.alert_thresholds and value >= metric.alert_thresholds["warning"]:
                    alert_level = "warning"
            
            if alert_level and self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "technical.metric.threshold_breach",
                    tags=[
                        f"metric:{metric.name}",
                        f"alert_level:{alert_level}",
                        f"service:{self.service_name}"
                    ]
                )
                
                # Add to alert history
                alert_record = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "metric": metric.name,
                    "value": value,
                    "threshold": metric.alert_thresholds[alert_level],
                    "alert_level": alert_level
                }
                
                self.alert_history.append(alert_record)
                
                # Keep only last 1000 alerts
                if len(self.alert_history) > 1000:
                    self.alert_history = self.alert_history[-1000:]
                
                self.logger.warning(
                    f"Technical metric threshold breach: {metric.name} = {value} "
                    f"(threshold: {metric.alert_thresholds[alert_level]})"
                )
                
        except Exception as e:
            self.logger.error(f"Failed to check technical threshold: {str(e)}")
    
    async def perform_health_checks(self) -> Dict[str, Any]:
        """Perform health checks on all registered services"""
        
        health_results = {}
        overall_health = True
        
        for service_name, health_check in self.health_checks.items():
            try:
                # This is a simplified health check - in practice, you'd implement
                # actual HTTP/TCP checks based on the check_type
                health_status = await self._perform_single_health_check(health_check)
                health_results[service_name] = health_status
                
                if health_status["status"] != "healthy":
                    overall_health = False
                
                # Track health check results
                if self.datadog_monitoring:
                    health_value = 1.0 if health_status["status"] == "healthy" else 0.0
                    self.datadog_monitoring.gauge(
                        f"technical.health_check.{service_name}",
                        health_value,
                        tags=[
                            f"service:{service_name}",
                            f"status:{health_status['status']}",
                            f"check_type:{health_check.check_type}"
                        ]
                    )
                
            except Exception as e:
                health_results[service_name] = {
                    "status": "unhealthy",
                    "error": str(e),
                    "response_time": None,
                    "timestamp": datetime.utcnow().isoformat()
                }
                overall_health = False
        
        # Track overall health
        if self.datadog_monitoring:
            overall_health_value = 1.0 if overall_health else 0.0
            self.datadog_monitoring.gauge(
                "technical.overall_health",
                overall_health_value,
                tags=[f"service:{self.service_name}"]
            )
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_healthy": overall_health,
            "services": health_results,
            "healthy_count": sum(1 for r in health_results.values() if r["status"] == "healthy"),
            "total_count": len(health_results)
        }
    
    async def _perform_single_health_check(self, health_check: ServiceHealthCheck) -> Dict[str, Any]:
        """Perform a single health check"""
        
        # This is a placeholder implementation
        # In practice, you would implement actual HTTP/TCP/process checks
        import random
        
        # Simulate health check
        await asyncio.sleep(random.uniform(0.01, 0.1))  # Simulate network delay
        
        # Random health status for demo (normally based on actual checks)
        is_healthy = random.random() > 0.1  # 90% chance of being healthy
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "response_time": random.uniform(10, 100),  # milliseconds
            "timestamp": datetime.utcnow().isoformat(),
            "check_type": health_check.check_type,
            "endpoint": health_check.endpoint
        }
    
    def get_dashboard_status(self) -> Dict[str, Any]:
        """Get status of technical dashboards"""
        
        status = {
            "service_name": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "technical_metrics": {
                "total_defined": len(self.technical_metrics),
                "with_thresholds": len([m for m in self.technical_metrics.values() if m.alert_thresholds]),
                "by_category": {}
            },
            "health_checks": {
                "total_defined": len(self.health_checks),
                "by_type": {}
            },
            "dashboards": {
                "types_available": [dt.value for dt in TechnicalDashboardType],
                "total_types": len(TechnicalDashboardType)
            },
            "system_stats": self.system_stats.copy(),
            "alert_history_count": len(self.alert_history)
        }
        
        # Count metrics by tags
        for metric in self.technical_metrics.values():
            if metric.tags:
                for tag in metric.tags:
                    if tag not in status["technical_metrics"]["by_category"]:
                        status["technical_metrics"]["by_category"][tag] = 0
                    status["technical_metrics"]["by_category"][tag] += 1
        
        # Count health checks by type
        for health_check in self.health_checks.values():
            check_type = health_check.check_type
            if check_type not in status["health_checks"]["by_type"]:
                status["health_checks"]["by_type"][check_type] = 0
            status["health_checks"]["by_type"][check_type] += 1
        
        return status


# Global technical operations dashboards instance
_technical_operations_dashboards: Optional[DataDogTechnicalOperationsDashboards] = None


def get_technical_operations_dashboards(service_name: str = "technical-operations",
                                      datadog_monitoring: Optional[DatadogMonitoring] = None,
                                      dashboard_manager: Optional[DataDogDashboardManager] = None) -> DataDogTechnicalOperationsDashboards:
    """Get or create technical operations dashboards instance"""
    global _technical_operations_dashboards
    
    if _technical_operations_dashboards is None:
        _technical_operations_dashboards = DataDogTechnicalOperationsDashboards(
            service_name, datadog_monitoring, dashboard_manager
        )
    
    return _technical_operations_dashboards


# Convenience functions

async def track_technical_metric(metric_name: str, value: float, tags: Optional[Dict[str, str]] = None) -> bool:
    """Convenience function for tracking technical metrics"""
    technical_dashboards = get_technical_operations_dashboards()
    return await technical_dashboards.track_technical_metric(metric_name, value, tags)


async def collect_system_metrics() -> Dict[str, Any]:
    """Convenience function for collecting system metrics"""
    technical_dashboards = get_technical_operations_dashboards()
    return await technical_dashboards.collect_system_metrics()


async def perform_health_checks() -> Dict[str, Any]:
    """Convenience function for performing health checks"""
    technical_dashboards = get_technical_operations_dashboards()
    return await technical_dashboards.perform_health_checks()


async def create_all_technical_dashboards() -> Dict[TechnicalDashboardType, Optional[str]]:
    """Convenience function for creating all technical dashboards"""
    technical_dashboards = get_technical_operations_dashboards()
    return await technical_dashboards.create_all_technical_dashboards()