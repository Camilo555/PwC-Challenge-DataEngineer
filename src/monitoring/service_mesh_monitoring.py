"""
Service Mesh and Messaging Monitoring
Advanced monitoring for distributed microservices architecture with comprehensive messaging observability
Integrates RabbitMQ, Kafka, distributed tracing, and intelligent alerting
"""
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx
from opentelemetry import trace
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ServiceHealthCheck:
    service_name: str
    endpoint: str
    expected_status: int = 200
    timeout: int = 5
    interval: int = 30


@dataclass
class CircuitBreakerMetrics:
    service_name: str
    state: str  # OPEN, CLOSED, HALF_OPEN
    failure_count: int
    success_count: int
    last_failure_time: datetime | None
    next_retry_time: datetime | None


class ServiceMeshMonitor:
    def __init__(self, jaeger_endpoint: str = "http://jaeger:14268/api/traces"):
        self.tracer = trace.get_tracer(__name__)
        self.jaeger_endpoint = jaeger_endpoint

        # Prometheus metrics
        self.request_count = Counter(
            'service_mesh_requests_total',
            'Total requests processed by service mesh',
            ['service', 'method', 'status', 'endpoint']
        )

        self.request_duration = Histogram(
            'service_mesh_request_duration_seconds',
            'Request duration in seconds',
            ['service', 'method', 'endpoint'],
            buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        )

        self.active_connections = Gauge(
            'service_mesh_active_connections',
            'Active connections per service',
            ['service']
        )

        self.circuit_breaker_state = Gauge(
            'service_mesh_circuit_breaker_state',
            'Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)',
            ['service', 'endpoint']
        )

        self.error_rate = Gauge(
            'service_mesh_error_rate_percent',
            'Error rate percentage over last 5 minutes',
            ['service', 'endpoint']
        )

        # Service registry
        self.services: dict[str, ServiceHealthCheck] = {}
        self.health_status: dict[str, dict[str, Any]] = {}
        self.circuit_breakers: dict[str, CircuitBreakerMetrics] = {}

        # Performance tracking
        self.request_history: list[dict[str, Any]] = []
        self.max_history = 10000

    def register_service(self, health_check: ServiceHealthCheck):
        """Register a service for health monitoring"""
        self.services[health_check.service_name] = health_check
        logger.info(f"Registered service for monitoring: {health_check.service_name}")

    async def start_monitoring(self):
        """Start continuous monitoring of all registered services"""
        monitoring_tasks = []

        for service_name, health_check in self.services.items():
            task = asyncio.create_task(
                self._monitor_service_health(service_name, health_check)
            )
            monitoring_tasks.append(task)

        # Start metrics aggregation
        metrics_task = asyncio.create_task(self._aggregate_metrics())
        monitoring_tasks.append(metrics_task)

        # Start service discovery
        discovery_task = asyncio.create_task(self._service_discovery())
        monitoring_tasks.append(discovery_task)

        await asyncio.gather(*monitoring_tasks)

    async def _monitor_service_health(self, service_name: str, health_check: ServiceHealthCheck):
        """Monitor individual service health"""
        while True:
            start_time = time.time()

            try:
                async with httpx.AsyncClient(timeout=health_check.timeout) as client:
                    with self.tracer.start_as_current_span(f"health_check_{service_name}") as span:
                        span.set_attribute("service.name", service_name)
                        span.set_attribute("health_check.endpoint", health_check.endpoint)

                        response = await client.get(health_check.endpoint)
                        duration = time.time() - start_time

                        is_healthy = response.status_code == health_check.expected_status

                        # Update health status
                        self.health_status[service_name] = {
                            "healthy": is_healthy,
                            "status_code": response.status_code,
                            "response_time": duration,
                            "last_check": datetime.utcnow().isoformat(),
                            "endpoint": health_check.endpoint
                        }

                        # Update metrics
                        self.request_count.labels(
                            service=service_name,
                            method="GET",
                            status=response.status_code,
                            endpoint="health"
                        ).inc()

                        self.request_duration.labels(
                            service=service_name,
                            method="GET",
                            endpoint="health"
                        ).observe(duration)

                        # Update span attributes
                        span.set_attribute("http.status_code", response.status_code)
                        span.set_attribute("http.response_time", duration)
                        span.set_attribute("service.healthy", is_healthy)

                        if not is_healthy:
                            span.record_exception(Exception(f"Health check failed: {response.status_code}"))
                            logger.warning(
                                f"Service {service_name} health check failed",
                                extra={
                                    "service": service_name,
                                    "status_code": response.status_code,
                                    "response_time": duration
                                }
                            )

            except Exception as e:
                duration = time.time() - start_time

                self.health_status[service_name] = {
                    "healthy": False,
                    "error": str(e),
                    "response_time": duration,
                    "last_check": datetime.utcnow().isoformat(),
                    "endpoint": health_check.endpoint
                }

                logger.error(
                    f"Service {service_name} health check error: {e}",
                    extra={"service": service_name, "error": str(e)}
                )

            await asyncio.sleep(health_check.interval)

    async def _aggregate_metrics(self):
        """Aggregate and calculate derived metrics"""
        while True:
            try:
                # Calculate error rates
                await self._calculate_error_rates()

                # Update circuit breaker metrics
                await self._update_circuit_breaker_metrics()

                # Clean old request history
                self._cleanup_request_history()

                await asyncio.sleep(60)  # Run every minute

            except Exception as e:
                logger.error(f"Error in metrics aggregation: {e}")
                await asyncio.sleep(60)

    async def _calculate_error_rates(self):
        """Calculate error rates for each service"""
        five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)

        for service_name in self.services.keys():
            # Filter requests from last 5 minutes
            recent_requests = [
                req for req in self.request_history
                if req['service'] == service_name and
                   datetime.fromisoformat(req['timestamp']) > five_minutes_ago
            ]

            if recent_requests:
                error_requests = [
                    req for req in recent_requests
                    if req['status_code'] >= 400
                ]

                error_rate = (len(error_requests) / len(recent_requests)) * 100

                self.error_rate.labels(
                    service=service_name,
                    endpoint="all"
                ).set(error_rate)

    async def _update_circuit_breaker_metrics(self):
        """Update circuit breaker state metrics"""
        for cb_key, cb_metrics in self.circuit_breakers.items():
            state_value = {
                'CLOSED': 0,
                'OPEN': 1,
                'HALF_OPEN': 2
            }.get(cb_metrics.state, 0)

            self.circuit_breaker_state.labels(
                service=cb_metrics.service_name,
                endpoint=cb_key
            ).set(state_value)

    def record_request(
        self,
        service_name: str,
        method: str,
        endpoint: str,
        status_code: int,
        duration: float,
        request_size: int = 0,
        response_size: int = 0
    ):
        """Record a service request for monitoring"""

        # Update Prometheus metrics
        self.request_count.labels(
            service=service_name,
            method=method,
            status=status_code,
            endpoint=endpoint
        ).inc()

        self.request_duration.labels(
            service=service_name,
            method=method,
            endpoint=endpoint
        ).observe(duration)

        # Store in request history for trend analysis
        request_record = {
            'timestamp': datetime.utcnow().isoformat(),
            'service': service_name,
            'method': method,
            'endpoint': endpoint,
            'status_code': status_code,
            'duration': duration,
            'request_size': request_size,
            'response_size': response_size
        }

        self.request_history.append(request_record)

    def update_circuit_breaker(self, service_name: str, endpoint: str, metrics: CircuitBreakerMetrics):
        """Update circuit breaker metrics"""
        key = f"{service_name}:{endpoint}"
        self.circuit_breakers[key] = metrics

    async def _service_discovery(self):
        """Discover and register new services automatically"""
        while True:
            try:
                # In a real implementation, this would integrate with service discovery
                # mechanisms like Consul, etcd, or Kubernetes service discovery

                # For now, simulate discovery
                await self._discover_kubernetes_services()
                await asyncio.sleep(120)  # Check every 2 minutes

            except Exception as e:
                logger.error(f"Error in service discovery: {e}")
                await asyncio.sleep(120)

    async def _discover_kubernetes_services(self):
        """Discover services from Kubernetes API"""
        try:
            # Simulate Kubernetes service discovery
            # In production, use kubernetes.client to query the API
            discovered_services = [
                {"name": "api-service", "endpoint": "http://api-service:8000/health"},
                {"name": "etl-service", "endpoint": "http://etl-service:8080/health"},
                {"name": "dagster-service", "endpoint": "http://dagster:3000/server_info"}
            ]

            for service in discovered_services:
                if service["name"] not in self.services:
                    health_check = ServiceHealthCheck(
                        service_name=service["name"],
                        endpoint=service["endpoint"]
                    )
                    self.register_service(health_check)

        except Exception as e:
            logger.error(f"Kubernetes service discovery failed: {e}")

    def _cleanup_request_history(self):
        """Clean up old request history to prevent memory leaks"""
        if len(self.request_history) > self.max_history:
            # Keep only the most recent requests
            self.request_history = self.request_history[-self.max_history:]

    async def get_service_topology(self) -> dict[str, Any]:
        """Get current service topology and dependencies"""
        topology = {
            "services": {},
            "dependencies": [],
            "health_summary": {
                "healthy_services": 0,
                "unhealthy_services": 0,
                "total_services": len(self.services)
            }
        }

        for service_name, health_info in self.health_status.items():
            topology["services"][service_name] = {
                "status": "healthy" if health_info.get("healthy", False) else "unhealthy",
                "response_time": health_info.get("response_time", 0),
                "last_check": health_info.get("last_check"),
                "endpoint": health_info.get("endpoint")
            }

            if health_info.get("healthy", False):
                topology["health_summary"]["healthy_services"] += 1
            else:
                topology["health_summary"]["unhealthy_services"] += 1

        return topology

    async def get_performance_metrics(self) -> dict[str, Any]:
        """Get performance metrics summary"""
        five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)

        recent_requests = [
            req for req in self.request_history
            if datetime.fromisoformat(req['timestamp']) > five_minutes_ago
        ]

        if not recent_requests:
            return {"error": "No recent requests found"}

        # Calculate aggregate metrics
        total_requests = len(recent_requests)
        error_requests = [req for req in recent_requests if req['status_code'] >= 400]

        avg_response_time = sum(req['duration'] for req in recent_requests) / total_requests
        p95_response_time = sorted([req['duration'] for req in recent_requests])[int(total_requests * 0.95)]
        p99_response_time = sorted([req['duration'] for req in recent_requests])[int(total_requests * 0.99)]

        return {
            "time_window": "5 minutes",
            "total_requests": total_requests,
            "error_requests": len(error_requests),
            "error_rate_percent": (len(error_requests) / total_requests) * 100,
            "avg_response_time": avg_response_time,
            "p95_response_time": p95_response_time,
            "p99_response_time": p99_response_time,
            "requests_per_minute": total_requests / 5
        }

    def get_prometheus_metrics(self) -> str:
        """Get Prometheus formatted metrics"""
        return generate_latest().decode('utf-8')
    
    async def integrate_messaging_monitoring(self, messaging_monitor):
        """Integrate messaging system monitoring"""
        try:
            # Get messaging health status
            messaging_health = await messaging_monitor.get_health_status()
            
            # Add messaging components to service topology
            if "components" in messaging_health:
                for component, health_info in messaging_health["components"].items():
                    self.health_status[f"messaging_{component}"] = {
                        "healthy": health_info.get("status") == "healthy",
                        "endpoint": f"messaging://{component}",
                        "last_check": messaging_health.get("timestamp"),
                        "response_time": 0,  # Not applicable for messaging
                        "metadata": health_info.get("details", {})
                    }
            
            logger.info("Integrated messaging monitoring into service mesh monitoring")
            
        except Exception as e:
            logger.error(f"Failed to integrate messaging monitoring: {e}")


# Enhanced monitoring integration class
class ComprehensiveMonitoringIntegration:
    """Integrates all monitoring components for complete observability"""
    
    def __init__(self):
        self.service_mesh_monitor = ServiceMeshMonitor()
        self.logger = get_logger(__name__)
        
        # Will be set by initialization methods
        self.messaging_monitor = None
        self.intelligent_alerting = None
        self.message_tracer = None
        
    async def initialize_messaging_monitoring(self, rabbitmq_manager, kafka_manager, metric_collector):
        """Initialize messaging monitoring components"""
        try:
            # Import here to avoid circular dependencies
            from monitoring.messaging_monitoring import MessagingMonitoringManager
            from monitoring.intelligent_alerting import create_messaging_alert_manager
            from monitoring.message_tracing import get_message_flow_tracer
            
            # Initialize messaging monitoring
            self.messaging_monitor = MessagingMonitoringManager(
                rabbitmq_manager, kafka_manager, metric_collector
            )
            
            # Initialize intelligent alerting
            from monitoring.intelligent_alerting import IntelligentAlertManager
            base_alert_manager = IntelligentAlertManager()
            self.intelligent_alerting = create_messaging_alert_manager(base_alert_manager)
            
            # Initialize message tracing
            self.message_tracer = get_message_flow_tracer()
            
            # Start monitoring
            await self.messaging_monitor.start_monitoring()
            await self.intelligent_alerting.start_monitoring()
            
            # Integrate with service mesh monitoring
            await self.service_mesh_monitor.integrate_messaging_monitoring(self.messaging_monitor)
            
            self.logger.info("Comprehensive messaging monitoring initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize messaging monitoring: {e}")
            raise
    
    async def get_comprehensive_health_status(self) -> dict[str, Any]:
        """Get comprehensive health status across all systems"""
        try:
            # Get service mesh health
            service_topology = await self.service_mesh_monitor.get_service_topology()
            
            # Get messaging health if available
            messaging_health = {}
            if self.messaging_monitor:
                messaging_health = await self.messaging_monitor.get_health_status()
            
            # Get alert summary if available
            alert_summary = {}
            if self.intelligent_alerting:
                alert_summary = await self.intelligent_alerting.check_messaging_health()
            
            return {
                "overall_status": self._determine_overall_health(service_topology, messaging_health, alert_summary),
                "service_mesh": service_topology,
                "messaging": messaging_health,
                "alerts": alert_summary,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive health status: {e}")
            return {
                "overall_status": "unknown",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def _determine_overall_health(self, service_topology, messaging_health, alert_summary):
        """Determine overall system health status"""
        # Check service mesh health
        service_health = service_topology.get("health_summary", {})
        unhealthy_services = service_health.get("unhealthy_services", 0)
        
        # Check messaging health
        messaging_status = messaging_health.get("overall_status", "unknown")
        
        # Check critical alerts
        critical_alerts = 0
        if alert_summary and "rabbitmq" in alert_summary:
            critical_alerts += alert_summary["rabbitmq"].get("critical_alerts", 0)
        if alert_summary and "kafka" in alert_summary:
            critical_alerts += alert_summary["kafka"].get("critical_alerts", 0)
        
        # Determine overall status
        if critical_alerts > 0 or messaging_status == "critical":
            return "critical"
        elif unhealthy_services > 0 or messaging_status == "degraded":
            return "degraded"  
        elif messaging_status == "warning":
            return "warning"
        elif messaging_status == "healthy" and unhealthy_services == 0:
            return "healthy"
        else:
            return "unknown"
    
    async def get_comprehensive_metrics(self) -> dict[str, Any]:
        """Get comprehensive metrics from all monitoring systems"""
        try:
            metrics = {
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Service mesh metrics
            try:
                metrics["service_mesh"] = await self.service_mesh_monitor.get_performance_metrics()
            except Exception as e:
                metrics["service_mesh"] = {"error": str(e)}
            
            # Messaging metrics
            if self.messaging_monitor:
                try:
                    metrics["messaging"] = self.messaging_monitor.get_metrics_summary()
                except Exception as e:
                    metrics["messaging"] = {"error": str(e)}
            
            # Alert metrics
            if self.intelligent_alerting:
                try:
                    metrics["alerts"] = self.intelligent_alerting.get_messaging_alert_summary()
                except Exception as e:
                    metrics["alerts"] = {"error": str(e)}
            
            # Message flow metrics
            if self.message_tracer:
                try:
                    metrics["message_flows"] = self.message_tracer.get_flow_metrics()
                except Exception as e:
                    metrics["message_flows"] = {"error": str(e)}
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive metrics: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def shutdown(self):
        """Shutdown all monitoring components"""
        try:
            if self.messaging_monitor:
                await self.messaging_monitor.stop_monitoring()
            
            if self.intelligent_alerting:
                await self.intelligent_alerting.alert_manager.stop_monitoring()
            
            self.logger.info("Comprehensive monitoring shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during monitoring shutdown: {e}")


# Factory function for easy setup
async def create_comprehensive_monitoring(
    rabbitmq_manager, 
    kafka_manager, 
    metric_collector
) -> ComprehensiveMonitoringIntegration:
    """Create and initialize comprehensive monitoring system"""
    monitoring = ComprehensiveMonitoringIntegration()
    await monitoring.initialize_messaging_monitoring(rabbitmq_manager, kafka_manager, metric_collector)
    return monitoring
