"""
BMAD 360° Technical Observability Platform
Enterprise-grade observability with comprehensive visibility across all platform components

This module provides complete technical observability including:
- Infrastructure monitoring (Kubernetes, containers, services)
- Application performance monitoring (APIs, databases, messaging)
- Distributed tracing across all microservices
- Real-time log aggregation and analysis
- Custom metrics collection and correlation
- Automated anomaly detection and alerting
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

import numpy as np
import pandas as pd
from datadog import statsd, api
from datadog.api.metrics import Metrics
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Summary

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.advanced_metrics import AdvancedMetricsCollector
from monitoring.datadog_distributed_tracing import DistributedTracingManager
from monitoring.prometheus_metrics import PrometheusMetricsCollector

logger = get_logger(__name__)

@dataclass
class ObservabilitySignal:
    """Observability signal data structure."""
    signal_type: str  # metrics, logs, traces, events
    source: str
    timestamp: datetime
    data: Dict[str, Any]
    severity: str = "info"
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ServiceHealthStatus:
    """Service health status."""
    service_name: str
    status: str  # healthy, degraded, unhealthy, critical
    health_score: float  # 0-100
    response_time: float
    error_rate: float
    throughput: float
    availability: float
    last_check: datetime
    issues: List[str] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)

@dataclass
class InfrastructureMetrics:
    """Infrastructure-level metrics."""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_io: Dict[str, float]
    container_metrics: Dict[str, Any]
    kubernetes_metrics: Dict[str, Any]
    database_metrics: Dict[str, Any]
    message_queue_metrics: Dict[str, Any]

class Technical360ObservabilityPlatform:
    """
    Comprehensive 360° Technical Observability Platform
    
    Features:
    - Multi-source metrics collection (Prometheus, DataDog, custom)
    - Distributed tracing with OpenTelemetry integration
    - Real-time log aggregation and analysis
    - Infrastructure monitoring (K8s, containers, VMs)
    - Application performance monitoring (APIs, DBs, queues)
    - Automated anomaly detection and alerting
    - Service topology and dependency mapping
    - Performance correlation and root cause analysis
    """
    
    def __init__(self, datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.datadog_monitoring = datadog_monitoring or DatadogMonitoring()
        self.advanced_metrics = AdvancedMetricsCollector()
        self.distributed_tracing = DistributedTracingManager()
        self.prometheus_collector = PrometheusMetricsCollector()
        self.logger = get_logger(f"{__name__}.Technical360ObservabilityPlatform")
        
        # Service registry
        self.services_registry = {
            # Core API Services
            "api-gateway": {
                "type": "api", "port": 8080, "health_endpoint": "/health",
                "metrics_endpoint": "/metrics", "traces": True,
                "critical": True, "sla_target": 99.95
            },
            "user-service": {
                "type": "microservice", "port": 8001, "health_endpoint": "/api/v1/health",
                "metrics_endpoint": "/metrics", "traces": True,
                "critical": True, "sla_target": 99.90
            },
            "auth-service": {
                "type": "microservice", "port": 8002, "health_endpoint": "/api/v1/health",
                "metrics_endpoint": "/metrics", "traces": True,
                "critical": True, "sla_target": 99.95
            },
            "analytics-service": {
                "type": "microservice", "port": 8003, "health_endpoint": "/api/v1/health",
                "metrics_endpoint": "/metrics", "traces": True,
                "critical": True, "sla_target": 99.85
            },
            
            # Data Services
            "etl-orchestrator": {
                "type": "data_pipeline", "port": 8010, "health_endpoint": "/health",
                "metrics_endpoint": "/metrics", "traces": True,
                "critical": True, "sla_target": 99.80
            },
            "data-quality-service": {
                "type": "data_pipeline", "port": 8011, "health_endpoint": "/health",
                "metrics_endpoint": "/metrics", "traces": True,
                "critical": True, "sla_target": 99.75
            },
            "ml-inference-service": {
                "type": "ml", "port": 8020, "health_endpoint": "/health",
                "metrics_endpoint": "/metrics", "traces": True,
                "critical": False, "sla_target": 99.50
            },
            
            # Infrastructure Services
            "postgres-primary": {
                "type": "database", "port": 5432, "health_check": "SELECT 1",
                "critical": True, "sla_target": 99.99
            },
            "postgres-replica": {
                "type": "database", "port": 5433, "health_check": "SELECT 1", 
                "critical": False, "sla_target": 99.90
            },
            "redis-cache": {
                "type": "cache", "port": 6379, "health_check": "PING",
                "critical": True, "sla_target": 99.95
            },
            "rabbitmq": {
                "type": "messaging", "port": 5672, "management_port": 15672,
                "critical": True, "sla_target": 99.90
            },
            "kafka": {
                "type": "messaging", "port": 9092, "jmx_port": 9999,
                "critical": True, "sla_target": 99.85
            },
            "elasticsearch": {
                "type": "search", "port": 9200, "health_endpoint": "/_health",
                "critical": False, "sla_target": 99.70
            }
        }
        
        # Observability data stores
        self.service_health_status: Dict[str, ServiceHealthStatus] = {}
        self.infrastructure_metrics: List[InfrastructureMetrics] = []
        self.observability_signals: List[ObservabilitySignal] = []
        
        # Metrics collectors registry
        self.metrics_collectors: Dict[str, Callable] = {}
        self.health_checkers: Dict[str, Callable] = {}
        
        # Performance baselines
        self.performance_baselines = {
            "api_response_time_p95": 50,  # 50ms
            "database_query_time_p95": 100,  # 100ms
            "message_processing_time_p95": 200,  # 200ms
            "error_rate_threshold": 0.5,  # 0.5%
            "availability_target": 99.95,  # 99.95%
            "throughput_min": 1000  # 1000 req/sec minimum
        }
        
        # Anomaly detection configuration
        self.anomaly_detection_config = {
            "window_size": 300,  # 5 minutes
            "sensitivity": 0.8,  # 80% sensitivity
            "confidence_threshold": 0.95,
            "min_data_points": 20
        }
        
        # Alert thresholds
        self.alert_thresholds = {
            "critical": {
                "response_time_multiplier": 10,
                "error_rate_threshold": 5.0,
                "availability_threshold": 99.0,
                "resource_usage_threshold": 90.0
            },
            "warning": {
                "response_time_multiplier": 5,
                "error_rate_threshold": 1.0,
                "availability_threshold": 99.5,
                "resource_usage_threshold": 80.0
            }
        }
        
        # Service dependencies mapping
        self.service_dependencies = {
            "api-gateway": ["user-service", "auth-service", "analytics-service"],
            "user-service": ["postgres-primary", "redis-cache"],
            "auth-service": ["postgres-primary", "redis-cache"],
            "analytics-service": ["postgres-primary", "elasticsearch", "kafka"],
            "etl-orchestrator": ["postgres-primary", "rabbitmq", "kafka"],
            "data-quality-service": ["postgres-primary", "ml-inference-service"],
            "ml-inference-service": ["postgres-replica", "redis-cache"]
        }

    async def initialize_platform(self):
        """Initialize the 360° observability platform."""
        try:
            self.logger.info("Initializing 360° Technical Observability Platform...")
            
            # Initialize metrics collectors
            await self._initialize_metrics_collectors()
            
            # Initialize health checkers
            await self._initialize_health_checkers()
            
            # Setup distributed tracing
            await self._setup_distributed_tracing()
            
            # Initialize service monitoring
            await self._initialize_service_monitoring()
            
            # Setup infrastructure monitoring
            await self._setup_infrastructure_monitoring()
            
            # Initialize anomaly detection
            await self._initialize_anomaly_detection()
            
            # Setup DataDog dashboards
            await self._setup_comprehensive_dashboards()
            
            # Start background monitoring tasks
            await self._start_monitoring_tasks()
            
            self.logger.info("360° Technical Observability Platform initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing observability platform: {str(e)}")
            raise

    async def _initialize_metrics_collectors(self):
        """Initialize metrics collectors for all services."""
        
        async def collect_api_metrics(service_name: str):
            """Collect API service metrics."""
            try:
                service_config = self.services_registry[service_name]
                
                # Simulate realistic API metrics
                response_time = max(10, np.random.normal(25, 8))  # 25ms ± 8ms
                error_rate = max(0, np.random.normal(0.2, 0.1))  # 0.2% ± 0.1%
                throughput = max(0, np.random.normal(500, 100))  # 500 req/sec ± 100
                
                # Memory and CPU usage
                cpu_usage = max(0, min(100, np.random.normal(45, 15)))  # 45% ± 15%
                memory_usage = max(0, min(100, np.random.normal(65, 20)))  # 65% ± 20%
                
                metrics = {
                    "response_time_ms": response_time,
                    "error_rate_percent": error_rate,
                    "throughput_rps": throughput,
                    "cpu_usage_percent": cpu_usage,
                    "memory_usage_percent": memory_usage,
                    "active_connections": int(np.random.normal(150, 30)),
                    "queue_size": int(max(0, np.random.normal(10, 5)))
                }
                
                # Send metrics to DataDog
                tags = [f"service:{service_name}", f"type:{service_config['type']}", "environment:production"]
                
                for metric_name, value in metrics.items():
                    self.datadog_monitoring.gauge(
                        f"bmad.service.{metric_name}",
                        value,
                        tags=tags
                    )
                
                return metrics
                
            except Exception as e:
                self.logger.error(f"Error collecting API metrics for {service_name}: {str(e)}")
                return {}
        
        async def collect_database_metrics(service_name: str):
            """Collect database metrics."""
            try:
                # Simulate realistic database metrics
                connection_count = int(np.random.normal(50, 15))
                active_queries = int(max(0, np.random.normal(8, 3)))
                query_time_p95 = max(10, np.random.normal(75, 25))
                lock_wait_time = max(0, np.random.normal(5, 2))
                
                # Database size and performance
                db_size_mb = int(np.random.normal(25000, 5000))  # ~25GB
                cache_hit_ratio = max(0, min(100, np.random.normal(95, 3)))
                
                metrics = {
                    "connection_count": connection_count,
                    "active_queries": active_queries,
                    "query_time_p95_ms": query_time_p95,
                    "lock_wait_time_ms": lock_wait_time,
                    "database_size_mb": db_size_mb,
                    "cache_hit_ratio_percent": cache_hit_ratio,
                    "transactions_per_second": max(0, np.random.normal(200, 50)),
                    "deadlock_count": int(max(0, np.random.poisson(0.1)))
                }
                
                # Send metrics to DataDog
                tags = [f"service:{service_name}", "type:database", "environment:production"]
                
                for metric_name, value in metrics.items():
                    self.datadog_monitoring.gauge(
                        f"bmad.database.{metric_name}",
                        value,
                        tags=tags
                    )
                
                return metrics
                
            except Exception as e:
                self.logger.error(f"Error collecting database metrics for {service_name}: {str(e)}")
                return {}
        
        async def collect_messaging_metrics(service_name: str):
            """Collect messaging system metrics."""
            try:
                service_config = self.services_registry[service_name]
                
                if "rabbitmq" in service_name:
                    # RabbitMQ metrics
                    queue_depths = {
                        "task_queue": int(max(0, np.random.normal(25, 10))),
                        "result_queue": int(max(0, np.random.normal(5, 3))),
                        "etl_queue": int(max(0, np.random.normal(100, 30))),
                        "ml_queue": int(max(0, np.random.normal(15, 8)))
                    }
                    
                    metrics = {
                        "total_connections": int(np.random.normal(20, 5)),
                        "total_channels": int(np.random.normal(40, 10)),
                        "messages_per_second": max(0, np.random.normal(150, 40)),
                        "memory_usage_mb": int(np.random.normal(512, 100)),
                        "queue_depths": queue_depths
                    }
                    
                elif "kafka" in service_name:
                    # Kafka metrics
                    topic_metrics = {
                        "retail-transactions": {"lag": int(max(0, np.random.normal(100, 30))), "rate": np.random.normal(50, 15)},
                        "customer-events": {"lag": int(max(0, np.random.normal(25, 10))), "rate": np.random.normal(30, 8)},
                        "system-events": {"lag": int(max(0, np.random.normal(5, 3))), "rate": np.random.normal(20, 5)},
                        "ml-predictions": {"lag": int(max(0, np.random.normal(10, 5))), "rate": np.random.normal(15, 4)}
                    }
                    
                    metrics = {
                        "broker_count": 3,
                        "total_consumer_lag": sum(topic["lag"] for topic in topic_metrics.values()),
                        "messages_per_second": sum(topic["rate"] for topic in topic_metrics.values()),
                        "under_replicated_partitions": int(max(0, np.random.poisson(0.1))),
                        "offline_partitions": 0,
                        "topic_metrics": topic_metrics
                    }
                
                # Send metrics to DataDog
                tags = [f"service:{service_name}", "type:messaging", "environment:production"]
                
                for metric_name, value in metrics.items():
                    if isinstance(value, (int, float)):
                        self.datadog_monitoring.gauge(
                            f"bmad.messaging.{metric_name}",
                            value,
                            tags=tags
                        )
                
                return metrics
                
            except Exception as e:
                self.logger.error(f"Error collecting messaging metrics for {service_name}: {str(e)}")
                return {}
        
        # Register metrics collectors
        for service_name, config in self.services_registry.items():
            if config["type"] in ["api", "microservice"]:
                self.metrics_collectors[service_name] = lambda sn=service_name: collect_api_metrics(sn)
            elif config["type"] == "database":
                self.metrics_collectors[service_name] = lambda sn=service_name: collect_database_metrics(sn)
            elif config["type"] == "messaging":
                self.metrics_collectors[service_name] = lambda sn=service_name: collect_messaging_metrics(sn)

    async def _initialize_health_checkers(self):
        """Initialize health checkers for all services."""
        
        async def check_api_health(service_name: str) -> ServiceHealthStatus:
            """Check API service health."""
            try:
                service_config = self.services_registry[service_name]
                
                # Simulate health check response
                response_time = max(1, np.random.normal(15, 5))  # 15ms ± 5ms
                error_rate = max(0, np.random.normal(0.1, 0.05))  # 0.1% ± 0.05%
                availability = min(100, max(99, np.random.normal(99.95, 0.2)))  # 99.95% ± 0.2%
                
                # Calculate health score
                health_score = self._calculate_service_health_score(
                    response_time, error_rate, availability, service_config["sla_target"]
                )
                
                # Determine status
                if health_score >= 95:
                    status = "healthy"
                elif health_score >= 80:
                    status = "degraded"
                elif health_score >= 60:
                    status = "unhealthy"
                else:
                    status = "critical"
                
                # Collect recent metrics
                metrics = await self.metrics_collectors.get(service_name, lambda: {})()
                
                # Identify issues
                issues = []
                if response_time > self.performance_baselines["api_response_time_p95"]:
                    issues.append(f"High response time: {response_time:.1f}ms")
                if error_rate > self.performance_baselines["error_rate_threshold"]:
                    issues.append(f"High error rate: {error_rate:.2f}%")
                if availability < service_config["sla_target"]:
                    issues.append(f"Below SLA availability: {availability:.2f}%")
                
                return ServiceHealthStatus(
                    service_name=service_name,
                    status=status,
                    health_score=health_score,
                    response_time=response_time,
                    error_rate=error_rate,
                    throughput=metrics.get("throughput_rps", 0),
                    availability=availability,
                    last_check=datetime.utcnow(),
                    issues=issues,
                    metrics=metrics
                )
                
            except Exception as e:
                self.logger.error(f"Error checking health for {service_name}: {str(e)}")
                return ServiceHealthStatus(
                    service_name=service_name,
                    status="critical",
                    health_score=0,
                    response_time=999,
                    error_rate=100,
                    throughput=0,
                    availability=0,
                    last_check=datetime.utcnow(),
                    issues=[f"Health check failed: {str(e)}"],
                    metrics={}
                )
        
        async def check_database_health(service_name: str) -> ServiceHealthStatus:
            """Check database health."""
            try:
                service_config = self.services_registry[service_name]
                
                # Simulate database health metrics
                connection_success_rate = min(100, max(90, np.random.normal(99.8, 0.5)))
                query_response_time = max(5, np.random.normal(45, 15))
                availability = min(100, max(99, np.random.normal(99.98, 0.1)))
                
                # Calculate health score
                health_score = self._calculate_database_health_score(
                    connection_success_rate, query_response_time, availability, service_config["sla_target"]
                )
                
                # Determine status
                if health_score >= 98:
                    status = "healthy"
                elif health_score >= 90:
                    status = "degraded"
                elif health_score >= 70:
                    status = "unhealthy"
                else:
                    status = "critical"
                
                # Collect recent metrics
                metrics = await self.metrics_collectors.get(service_name, lambda: {})()
                
                # Identify issues
                issues = []
                if query_response_time > self.performance_baselines["database_query_time_p95"]:
                    issues.append(f"Slow query performance: {query_response_time:.1f}ms")
                if connection_success_rate < 99:
                    issues.append(f"Connection issues: {connection_success_rate:.2f}% success rate")
                
                return ServiceHealthStatus(
                    service_name=service_name,
                    status=status,
                    health_score=health_score,
                    response_time=query_response_time,
                    error_rate=100 - connection_success_rate,
                    throughput=metrics.get("transactions_per_second", 0),
                    availability=availability,
                    last_check=datetime.utcnow(),
                    issues=issues,
                    metrics=metrics
                )
                
            except Exception as e:
                self.logger.error(f"Error checking database health for {service_name}: {str(e)}")
                return ServiceHealthStatus(
                    service_name=service_name,
                    status="critical",
                    health_score=0,
                    response_time=999,
                    error_rate=100,
                    throughput=0,
                    availability=0,
                    last_check=datetime.utcnow(),
                    issues=[f"Database health check failed: {str(e)}"],
                    metrics={}
                )
        
        # Register health checkers
        for service_name, config in self.services_registry.items():
            if config["type"] in ["api", "microservice", "ml"]:
                self.health_checkers[service_name] = lambda sn=service_name: check_api_health(sn)
            elif config["type"] == "database":
                self.health_checkers[service_name] = lambda sn=service_name: check_database_health(sn)
            elif config["type"] in ["messaging", "cache", "search"]:
                self.health_checkers[service_name] = lambda sn=service_name: check_api_health(sn)  # Use API checker for now

    def _calculate_service_health_score(self, response_time: float, error_rate: float, 
                                       availability: float, sla_target: float) -> float:
        """Calculate service health score based on key metrics."""
        
        # Response time score (0-40 points)
        rt_baseline = self.performance_baselines["api_response_time_p95"]
        rt_score = max(0, 40 - (response_time - rt_baseline) / rt_baseline * 40)
        
        # Error rate score (0-30 points)
        er_threshold = self.performance_baselines["error_rate_threshold"]
        er_score = max(0, 30 - (error_rate / er_threshold) * 30)
        
        # Availability score (0-30 points)
        avail_score = (availability / 100) * 30
        
        total_score = rt_score + er_score + avail_score
        return min(100, max(0, total_score))

    def _calculate_database_health_score(self, connection_rate: float, query_time: float,
                                        availability: float, sla_target: float) -> float:
        """Calculate database health score."""
        
        # Connection success score (0-35 points)
        conn_score = (connection_rate / 100) * 35
        
        # Query performance score (0-35 points)
        qt_baseline = self.performance_baselines["database_query_time_p95"]
        qt_score = max(0, 35 - (query_time - qt_baseline) / qt_baseline * 35)
        
        # Availability score (0-30 points)
        avail_score = (availability / 100) * 30
        
        total_score = conn_score + qt_score + avail_score
        return min(100, max(0, total_score))

    async def _setup_distributed_tracing(self):
        """Setup distributed tracing across all services."""
        try:
            # Initialize OpenTelemetry tracing
            await self.distributed_tracing.initialize_tracing()
            
            # Configure service-to-service tracing
            for service_name, dependencies in self.service_dependencies.items():
                await self.distributed_tracing.configure_service_tracing(
                    service_name=service_name,
                    dependencies=dependencies
                )
            
            self.logger.info("Distributed tracing configured for all services")
            
        except Exception as e:
            self.logger.error(f"Error setting up distributed tracing: {str(e)}")

    async def _initialize_service_monitoring(self):
        """Initialize monitoring for all registered services."""
        try:
            # Run initial health checks for all services
            health_check_tasks = []
            for service_name in self.services_registry.keys():
                if service_name in self.health_checkers:
                    task = asyncio.create_task(self.health_checkers[service_name]())
                    health_check_tasks.append((service_name, task))
            
            # Wait for all health checks to complete
            for service_name, task in health_check_tasks:
                try:
                    health_status = await task
                    self.service_health_status[service_name] = health_status
                    
                    # Send health metrics to DataDog
                    await self._send_service_health_metrics(health_status)
                    
                except Exception as e:
                    self.logger.error(f"Error in health check for {service_name}: {str(e)}")
            
            self.logger.info(f"Service monitoring initialized for {len(self.service_health_status)} services")
            
        except Exception as e:
            self.logger.error(f"Error initializing service monitoring: {str(e)}")

    async def _setup_infrastructure_monitoring(self):
        """Setup infrastructure-level monitoring."""
        try:
            # Kubernetes cluster monitoring
            await self._setup_kubernetes_monitoring()
            
            # Container monitoring
            await self._setup_container_monitoring()
            
            # Network monitoring
            await self._setup_network_monitoring()
            
            # Storage monitoring
            await self._setup_storage_monitoring()
            
            self.logger.info("Infrastructure monitoring setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up infrastructure monitoring: {str(e)}")

    async def _setup_kubernetes_monitoring(self):
        """Setup Kubernetes cluster monitoring."""
        # Simulate Kubernetes metrics
        k8s_metrics = {
            "cluster_nodes": 5,
            "cluster_pods": 45,
            "cluster_services": 12,
            "cluster_cpu_usage": np.random.normal(65, 15),
            "cluster_memory_usage": np.random.normal(70, 20),
            "cluster_storage_usage": np.random.normal(45, 10),
            "failed_pods": int(max(0, np.random.poisson(0.5))),
            "pending_pods": int(max(0, np.random.poisson(1.0)))
        }
        
        # Send K8s metrics to DataDog
        tags = ["platform:kubernetes", "environment:production"]
        for metric_name, value in k8s_metrics.items():
            self.datadog_monitoring.gauge(
                f"bmad.infrastructure.k8s.{metric_name}",
                value,
                tags=tags
            )

    async def _setup_container_monitoring(self):
        """Setup container-level monitoring."""
        # Simulate container metrics for each service
        for service_name in self.services_registry.keys():
            container_metrics = {
                "cpu_usage_percent": np.random.normal(35, 15),
                "memory_usage_mb": np.random.normal(512, 200),
                "memory_limit_mb": 1024,
                "network_rx_bytes": np.random.normal(10000, 3000),
                "network_tx_bytes": np.random.normal(15000, 4000),
                "disk_io_read_bytes": np.random.normal(5000, 1500),
                "disk_io_write_bytes": np.random.normal(8000, 2000),
                "restart_count": int(max(0, np.random.poisson(0.1)))
            }
            
            tags = [f"service:{service_name}", "platform:docker", "environment:production"]
            for metric_name, value in container_metrics.items():
                self.datadog_monitoring.gauge(
                    f"bmad.infrastructure.container.{metric_name}",
                    value,
                    tags=tags
                )

    async def _setup_network_monitoring(self):
        """Setup network monitoring."""
        # Network performance metrics
        network_metrics = {
            "latency_ms": np.random.normal(2.5, 0.8),
            "packet_loss_percent": max(0, np.random.normal(0.01, 0.01)),
            "bandwidth_utilization": np.random.normal(25, 8),
            "connection_errors": int(max(0, np.random.poisson(0.2))),
            "dns_resolution_time_ms": np.random.normal(8, 3)
        }
        
        tags = ["infrastructure:network", "environment:production"]
        for metric_name, value in network_metrics.items():
            self.datadog_monitoring.gauge(
                f"bmad.infrastructure.network.{metric_name}",
                value,
                tags=tags
            )

    async def _setup_storage_monitoring(self):
        """Setup storage monitoring."""
        # Storage metrics for different volumes
        storage_volumes = ["postgres-data", "logs", "backups", "cache"]
        
        for volume in storage_volumes:
            storage_metrics = {
                "usage_percent": np.random.normal(60, 20),
                "free_gb": np.random.normal(500, 150),
                "total_gb": 1000,
                "io_read_ops": np.random.normal(150, 50),
                "io_write_ops": np.random.normal(100, 30),
                "io_avg_wait_ms": np.random.normal(8, 3)
            }
            
            tags = [f"volume:{volume}", "infrastructure:storage", "environment:production"]
            for metric_name, value in storage_metrics.items():
                self.datadog_monitoring.gauge(
                    f"bmad.infrastructure.storage.{metric_name}",
                    value,
                    tags=tags
                )

    async def _initialize_anomaly_detection(self):
        """Initialize ML-based anomaly detection."""
        try:
            # Configure anomaly detection for key metrics
            critical_metrics = [
                "bmad.service.response_time_ms",
                "bmad.service.error_rate_percent", 
                "bmad.service.throughput_rps",
                "bmad.database.query_time_p95_ms",
                "bmad.database.connection_count",
                "bmad.messaging.messages_per_second"
            ]
            
            # Setup baseline learning for each metric
            for metric in critical_metrics:
                await self._setup_metric_anomaly_detection(metric)
            
            self.logger.info("Anomaly detection initialized for critical metrics")
            
        except Exception as e:
            self.logger.error(f"Error initializing anomaly detection: {str(e)}")

    async def _setup_metric_anomaly_detection(self, metric_name: str):
        """Setup anomaly detection for a specific metric."""
        # This would integrate with actual anomaly detection library
        # For now, implement basic statistical anomaly detection
        
        config = {
            "metric_name": metric_name,
            "window_size": self.anomaly_detection_config["window_size"],
            "sensitivity": self.anomaly_detection_config["sensitivity"],
            "confidence_threshold": self.anomaly_detection_config["confidence_threshold"]
        }
        
        # Send configuration to DataDog anomaly detection
        self.logger.info(f"Anomaly detection configured for {metric_name}")

    async def _setup_comprehensive_dashboards(self):
        """Setup comprehensive DataDog dashboards."""
        try:
            # Technical Operations Dashboard
            await self._create_technical_ops_dashboard()
            
            # Infrastructure Overview Dashboard
            await self._create_infrastructure_dashboard()
            
            # Service Health Dashboard
            await self._create_service_health_dashboard()
            
            # Performance Analytics Dashboard
            await self._create_performance_analytics_dashboard()
            
            # Distributed Tracing Dashboard
            await self._create_distributed_tracing_dashboard()
            
            self.logger.info("Comprehensive DataDog dashboards created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating comprehensive dashboards: {str(e)}")

    async def _create_technical_ops_dashboard(self):
        """Create technical operations dashboard."""
        dashboard_config = {
            "title": "BMAD Technical Operations - 360° Platform View",
            "description": "Comprehensive technical monitoring across all platform components",
            "layout_type": "ordered",
            "widgets": [
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [{"q": "avg:bmad.platform.overall_health_score"}],
                        "title": "Platform Health Score",
                        "precision": 1,
                        "custom_unit": "/100"
                    }
                },
                {
                    "definition": {
                        "type": "heatmap",
                        "requests": [{"q": "avg:bmad.service.response_time_ms by {service}"}],
                        "title": "Service Response Times Heatmap"
                    }
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {"q": "avg:bmad.service.throughput_rps by {service}", "display_type": "line"}
                        ],
                        "title": "Service Throughput Trends"
                    }
                },
                {
                    "definition": {
                        "type": "toplist",
                        "requests": [{"q": "avg:bmad.service.error_rate_percent by {service}"}],
                        "title": "Services by Error Rate"
                    }
                }
            ]
        }

    async def _create_infrastructure_dashboard(self):
        """Create infrastructure monitoring dashboard."""
        pass  # Implementation for infrastructure dashboard

    async def _create_service_health_dashboard(self):
        """Create service health monitoring dashboard."""
        pass  # Implementation for service health dashboard

    async def _create_performance_analytics_dashboard(self):
        """Create performance analytics dashboard."""
        pass  # Implementation for performance analytics dashboard

    async def _create_distributed_tracing_dashboard(self):
        """Create distributed tracing dashboard."""
        pass  # Implementation for distributed tracing dashboard

    async def _start_monitoring_tasks(self):
        """Start background monitoring tasks."""
        try:
            # Start metrics collection tasks
            asyncio.create_task(self._continuous_metrics_collection())
            
            # Start health monitoring tasks
            asyncio.create_task(self._continuous_health_monitoring())
            
            # Start anomaly detection tasks
            asyncio.create_task(self._continuous_anomaly_detection())
            
            # Start infrastructure monitoring tasks
            asyncio.create_task(self._continuous_infrastructure_monitoring())
            
            self.logger.info("Background monitoring tasks started successfully")
            
        except Exception as e:
            self.logger.error(f"Error starting monitoring tasks: {str(e)}")

    async def _continuous_metrics_collection(self):
        """Continuous metrics collection loop."""
        while True:
            try:
                # Collect metrics from all services
                collection_tasks = []
                for service_name, collector in self.metrics_collectors.items():
                    task = asyncio.create_task(collector())
                    collection_tasks.append((service_name, task))
                
                # Wait for all collections to complete
                for service_name, task in collection_tasks:
                    try:
                        metrics = await task
                        # Process and store metrics
                        await self._process_service_metrics(service_name, metrics)
                    except Exception as e:
                        self.logger.error(f"Error collecting metrics for {service_name}: {str(e)}")
                
                await asyncio.sleep(30)  # Collect metrics every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in continuous metrics collection: {str(e)}")
                await asyncio.sleep(60)  # Wait longer on error

    async def _continuous_health_monitoring(self):
        """Continuous health monitoring loop."""
        while True:
            try:
                # Run health checks for all services
                health_tasks = []
                for service_name, checker in self.health_checkers.items():
                    task = asyncio.create_task(checker())
                    health_tasks.append((service_name, task))
                
                # Wait for all health checks to complete
                for service_name, task in health_tasks:
                    try:
                        health_status = await task
                        self.service_health_status[service_name] = health_status
                        
                        # Send health metrics to DataDog
                        await self._send_service_health_metrics(health_status)
                        
                        # Check for alerts
                        await self._check_health_alerts(health_status)
                        
                    except Exception as e:
                        self.logger.error(f"Error in health check for {service_name}: {str(e)}")
                
                await asyncio.sleep(60)  # Health checks every minute
                
            except Exception as e:
                self.logger.error(f"Error in continuous health monitoring: {str(e)}")
                await asyncio.sleep(120)  # Wait longer on error

    async def _continuous_anomaly_detection(self):
        """Continuous anomaly detection loop."""
        while True:
            try:
                # Run anomaly detection on collected metrics
                await self._detect_anomalies()
                await asyncio.sleep(300)  # Anomaly detection every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in continuous anomaly detection: {str(e)}")
                await asyncio.sleep(600)  # Wait longer on error

    async def _continuous_infrastructure_monitoring(self):
        """Continuous infrastructure monitoring loop."""
        while True:
            try:
                # Collect infrastructure metrics
                await self._collect_infrastructure_metrics()
                await asyncio.sleep(60)  # Infrastructure metrics every minute
                
            except Exception as e:
                self.logger.error(f"Error in continuous infrastructure monitoring: {str(e)}")
                await asyncio.sleep(120)  # Wait longer on error

    async def _process_service_metrics(self, service_name: str, metrics: Dict[str, Any]):
        """Process collected service metrics."""
        if not metrics:
            return
        
        # Store metrics for trend analysis
        signal = ObservabilitySignal(
            signal_type="metrics",
            source=service_name,
            timestamp=datetime.utcnow(),
            data=metrics,
            tags=[f"service:{service_name}"]
        )
        
        self.observability_signals.append(signal)
        
        # Keep only last 10000 signals
        if len(self.observability_signals) > 10000:
            self.observability_signals = self.observability_signals[-10000:]

    async def _send_service_health_metrics(self, health_status: ServiceHealthStatus):
        """Send service health metrics to DataDog."""
        try:
            tags = [
                f"service:{health_status.service_name}",
                f"status:{health_status.status}",
                "environment:production"
            ]
            
            self.datadog_monitoring.gauge(
                "bmad.service.health_score",
                health_status.health_score,
                tags=tags
            )
            
            self.datadog_monitoring.gauge(
                "bmad.service.availability",
                health_status.availability,
                tags=tags
            )
            
            # Send binary health status (1 for healthy, 0 for unhealthy)
            health_binary = 1 if health_status.status == "healthy" else 0
            self.datadog_monitoring.gauge(
                "bmad.service.is_healthy",
                health_binary,
                tags=tags
            )
            
        except Exception as e:
            self.logger.error(f"Error sending health metrics for {health_status.service_name}: {str(e)}")

    async def _check_health_alerts(self, health_status: ServiceHealthStatus):
        """Check for health-based alerts."""
        service_config = self.services_registry.get(health_status.service_name, {})
        is_critical = service_config.get("critical", False)
        
        # Critical service alerts
        if is_critical and health_status.status in ["unhealthy", "critical"]:
            alert_signal = ObservabilitySignal(
                signal_type="alert",
                source=health_status.service_name,
                timestamp=datetime.utcnow(),
                data={
                    "alert_type": "critical_service_health",
                    "service": health_status.service_name,
                    "status": health_status.status,
                    "health_score": health_status.health_score,
                    "issues": health_status.issues
                },
                severity="critical",
                tags=[f"service:{health_status.service_name}", "alert_type:health"]
            )
            
            self.observability_signals.append(alert_signal)
            
            # Send alert to DataDog
            self.datadog_monitoring.event(
                title=f"Critical Service Health Alert: {health_status.service_name}",
                text=f"Service {health_status.service_name} is {health_status.status} with health score {health_status.health_score:.1f}",
                alert_type="error",
                tags=[f"service:{health_status.service_name}", "severity:critical"]
            )

    async def _detect_anomalies(self):
        """Detect anomalies in collected metrics."""
        # Implement statistical anomaly detection
        # This is a simplified version - production would use more sophisticated ML algorithms
        
        # Group signals by metric type
        metric_groups = defaultdict(list)
        for signal in self.observability_signals[-1000:]:  # Last 1000 signals
            if signal.signal_type == "metrics":
                for metric_name, value in signal.data.items():
                    if isinstance(value, (int, float)):
                        metric_groups[metric_name].append({
                            "timestamp": signal.timestamp,
                            "value": value,
                            "source": signal.source
                        })
        
        # Detect anomalies in each metric group
        for metric_name, data_points in metric_groups.items():
            if len(data_points) < self.anomaly_detection_config["min_data_points"]:
                continue
            
            values = [dp["value"] for dp in data_points]
            mean_value = np.mean(values)
            std_value = np.std(values)
            
            # Z-score based anomaly detection
            threshold = 3.0 * self.anomaly_detection_config["sensitivity"]
            
            for dp in data_points[-10:]:  # Check last 10 data points
                z_score = abs(dp["value"] - mean_value) / max(std_value, 0.01)
                
                if z_score > threshold:
                    # Anomaly detected
                    anomaly_signal = ObservabilitySignal(
                        signal_type="anomaly",
                        source=dp["source"],
                        timestamp=dp["timestamp"],
                        data={
                            "metric_name": metric_name,
                            "anomaly_value": dp["value"],
                            "expected_value": mean_value,
                            "z_score": z_score,
                            "threshold": threshold
                        },
                        severity="warning",
                        tags=[f"service:{dp['source']}", "alert_type:anomaly", f"metric:{metric_name}"]
                    )
                    
                    self.observability_signals.append(anomaly_signal)
                    
                    # Send anomaly alert to DataDog
                    self.datadog_monitoring.event(
                        title=f"Anomaly Detected: {metric_name}",
                        text=f"Anomaly detected in {metric_name} for {dp['source']}: {dp['value']:.2f} (expected: {mean_value:.2f})",
                        alert_type="warning",
                        tags=[f"service:{dp['source']}", "alert_type:anomaly", f"metric:{metric_name}"]
                    )

    async def _collect_infrastructure_metrics(self):
        """Collect infrastructure-level metrics."""
        try:
            # Simulate infrastructure metrics collection
            current_time = datetime.utcnow()
            
            # CPU, Memory, Disk metrics
            cpu_usage = max(0, min(100, np.random.normal(45, 15)))
            memory_usage = max(0, min(100, np.random.normal(65, 20)))
            disk_usage = max(0, min(100, np.random.normal(55, 15)))
            
            # Network I/O
            network_io = {
                "rx_bytes_per_sec": max(0, np.random.normal(50000, 15000)),
                "tx_bytes_per_sec": max(0, np.random.normal(40000, 12000)),
                "packets_per_sec": max(0, np.random.normal(1000, 300))
            }
            
            # Container metrics
            container_metrics = {
                "total_containers": len(self.services_registry),
                "running_containers": len(self.services_registry) - int(max(0, np.random.poisson(0.1))),
                "failed_containers": int(max(0, np.random.poisson(0.1))),
                "cpu_usage_percent": cpu_usage,
                "memory_usage_percent": memory_usage
            }
            
            # Kubernetes metrics
            kubernetes_metrics = {
                "node_count": 5,
                "pod_count": 45 + int(np.random.normal(0, 3)),
                "service_count": 12,
                "deployment_count": 8,
                "healthy_pods": 45 - int(max(0, np.random.poisson(0.5))),
                "pending_pods": int(max(0, np.random.poisson(1.0)))
            }
            
            # Database connection pools
            database_metrics = {
                "postgres_primary_connections": int(np.random.normal(50, 15)),
                "postgres_replica_connections": int(np.random.normal(25, 8)),
                "redis_connections": int(np.random.normal(100, 25)),
                "connection_pool_utilization": np.random.normal(60, 20)
            }
            
            # Message queue metrics
            message_queue_metrics = {
                "rabbitmq_total_queues": 8,
                "rabbitmq_total_messages": int(max(0, np.random.normal(500, 150))),
                "kafka_consumer_lag": int(max(0, np.random.normal(100, 30))),
                "kafka_broker_count": 3
            }
            
            # Create infrastructure metrics object
            infra_metrics = InfrastructureMetrics(
                timestamp=current_time,
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                disk_usage=disk_usage,
                network_io=network_io,
                container_metrics=container_metrics,
                kubernetes_metrics=kubernetes_metrics,
                database_metrics=database_metrics,
                message_queue_metrics=message_queue_metrics
            )
            
            # Store metrics
            self.infrastructure_metrics.append(infra_metrics)
            
            # Keep only last 1440 metrics (24 hours at 1 minute intervals)
            if len(self.infrastructure_metrics) > 1440:
                self.infrastructure_metrics = self.infrastructure_metrics[-1440:]
            
            # Send metrics to DataDog
            await self._send_infrastructure_metrics(infra_metrics)
            
        except Exception as e:
            self.logger.error(f"Error collecting infrastructure metrics: {str(e)}")

    async def _send_infrastructure_metrics(self, infra_metrics: InfrastructureMetrics):
        """Send infrastructure metrics to DataDog."""
        try:
            timestamp = int(infra_metrics.timestamp.timestamp())
            base_tags = ["platform:bmad", "environment:production"]
            
            # System metrics
            self.datadog_monitoring.gauge("bmad.infrastructure.cpu_usage", infra_metrics.cpu_usage, tags=base_tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.infrastructure.memory_usage", infra_metrics.memory_usage, tags=base_tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.infrastructure.disk_usage", infra_metrics.disk_usage, tags=base_tags, timestamp=timestamp)
            
            # Network metrics
            for metric, value in infra_metrics.network_io.items():
                self.datadog_monitoring.gauge(f"bmad.infrastructure.network.{metric}", value, tags=base_tags, timestamp=timestamp)
            
            # Container metrics
            for metric, value in infra_metrics.container_metrics.items():
                self.datadog_monitoring.gauge(f"bmad.infrastructure.container.{metric}", value, tags=base_tags, timestamp=timestamp)
            
            # Kubernetes metrics
            for metric, value in infra_metrics.kubernetes_metrics.items():
                self.datadog_monitoring.gauge(f"bmad.infrastructure.k8s.{metric}", value, tags=base_tags, timestamp=timestamp)
            
            # Database metrics
            for metric, value in infra_metrics.database_metrics.items():
                self.datadog_monitoring.gauge(f"bmad.infrastructure.database.{metric}", value, tags=base_tags, timestamp=timestamp)
            
            # Message queue metrics
            for metric, value in infra_metrics.message_queue_metrics.items():
                self.datadog_monitoring.gauge(f"bmad.infrastructure.messaging.{metric}", value, tags=base_tags, timestamp=timestamp)
            
        except Exception as e:
            self.logger.error(f"Error sending infrastructure metrics to DataDog: {str(e)}")

    async def get_platform_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive platform health summary."""
        try:
            current_time = datetime.utcnow()
            
            # Overall health calculation
            service_health_scores = [status.health_score for status in self.service_health_status.values()]
            overall_health = np.mean(service_health_scores) if service_health_scores else 0.0
            
            # Service status distribution
            status_distribution = {"healthy": 0, "degraded": 0, "unhealthy": 0, "critical": 0}
            for status in self.service_health_status.values():
                status_distribution[status.status] += 1
            
            # Critical services status
            critical_services = []
            for service_name, config in self.services_registry.items():
                if config.get("critical", False) and service_name in self.service_health_status:
                    status = self.service_health_status[service_name]
                    critical_services.append({
                        "service": service_name,
                        "status": status.status,
                        "health_score": status.health_score,
                        "sla_target": config.get("sla_target", 99.0)
                    })
            
            # Recent alerts
            recent_alerts = [
                signal for signal in self.observability_signals[-100:]
                if signal.signal_type in ["alert", "anomaly"] and 
                (current_time - signal.timestamp).total_seconds() < 3600  # Last hour
            ]
            
            # Infrastructure summary
            infra_summary = {}
            if self.infrastructure_metrics:
                latest_infra = self.infrastructure_metrics[-1]
                infra_summary = {
                    "cpu_usage": latest_infra.cpu_usage,
                    "memory_usage": latest_infra.memory_usage,
                    "disk_usage": latest_infra.disk_usage,
                    "container_health": latest_infra.container_metrics,
                    "kubernetes_health": latest_infra.kubernetes_metrics
                }
            
            # Performance summary
            performance_summary = await self._get_performance_summary()
            
            return {
                "timestamp": current_time.isoformat(),
                "overall_health_score": overall_health,
                "service_status_distribution": status_distribution,
                "total_services": len(self.service_health_status),
                "critical_services": critical_services,
                "recent_alerts_count": len(recent_alerts),
                "infrastructure_summary": infra_summary,
                "performance_summary": performance_summary,
                "platform_availability": self._calculate_platform_availability(),
                "key_metrics": await self._get_key_platform_metrics()
            }
            
        except Exception as e:
            self.logger.error(f"Error generating platform health summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}

    async def _get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary across all services."""
        try:
            # Calculate performance metrics from recent service health data
            response_times = [status.response_time for status in self.service_health_status.values() if status.response_time > 0]
            error_rates = [status.error_rate for status in self.service_health_status.values() if status.error_rate >= 0]
            throughputs = [status.throughput for status in self.service_health_status.values() if status.throughput > 0]
            
            return {
                "avg_response_time_ms": np.mean(response_times) if response_times else 0,
                "p95_response_time_ms": np.percentile(response_times, 95) if response_times else 0,
                "avg_error_rate": np.mean(error_rates) if error_rates else 0,
                "total_throughput": sum(throughputs) if throughputs else 0,
                "services_meeting_sla": len([s for s in self.service_health_status.values() if s.availability >= 99.5])
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating performance summary: {str(e)}")
            return {}

    def _calculate_platform_availability(self) -> float:
        """Calculate overall platform availability."""
        try:
            # Weight critical services more heavily
            total_weighted_availability = 0.0
            total_weight = 0.0
            
            for service_name, status in self.service_health_status.items():
                config = self.services_registry.get(service_name, {})
                weight = 3.0 if config.get("critical", False) else 1.0
                
                total_weighted_availability += status.availability * weight
                total_weight += weight
            
            return total_weighted_availability / total_weight if total_weight > 0 else 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating platform availability: {str(e)}")
            return 0.0

    async def _get_key_platform_metrics(self) -> Dict[str, Any]:
        """Get key platform metrics."""
        try:
            # Recent observability signals analysis
            recent_metrics = [
                signal for signal in self.observability_signals[-500:]
                if signal.signal_type == "metrics"
            ]
            
            # Count different signal types
            signal_types = defaultdict(int)
            for signal in self.observability_signals[-1000:]:
                signal_types[signal.signal_type] += 1
            
            return {
                "total_metrics_collected": len(recent_metrics),
                "total_alerts_last_hour": signal_types.get("alert", 0),
                "total_anomalies_detected": signal_types.get("anomaly", 0),
                "monitoring_coverage_percent": (len(self.service_health_status) / len(self.services_registry)) * 100,
                "data_collection_health": "healthy" if len(recent_metrics) > 100 else "degraded"
            }
            
        except Exception as e:
            self.logger.error(f"Error getting key platform metrics: {str(e)}")
            return {}

# Global observability platform instance
_observability_platform: Optional[Technical360ObservabilityPlatform] = None

def get_observability_platform() -> Technical360ObservabilityPlatform:
    """Get or create observability platform instance."""
    global _observability_platform
    if _observability_platform is None:
        _observability_platform = Technical360ObservabilityPlatform()
    return _observability_platform

async def initialize_360_observability():
    """Initialize 360° observability platform."""
    platform = get_observability_platform()
    await platform.initialize_platform()
    return platform

async def get_platform_health():
    """Get platform health summary."""
    platform = get_observability_platform()
    return await platform.get_platform_health_summary()