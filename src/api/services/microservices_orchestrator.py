"""
Microservices Orchestrator
Centralized service orchestration with advanced patterns including CQRS, Saga, and Event Sourcing.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import uuid4

from api.patterns.cqrs_framework import Command, CQRSFramework, Query
from api.patterns.saga_orchestrator import SagaOrchestrator, SagaTransaction
from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager
from streaming.kafka_manager import KafkaManager

logger = get_logger(__name__)


class ServiceStatus(str, Enum):
    """Service health status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ServiceType(str, Enum):
    """Service type classification."""

    CORE = "core"
    BUSINESS = "business"
    INFRASTRUCTURE = "infrastructure"
    EXTERNAL = "external"


@dataclass
class ServiceDefinition:
    """Microservice definition with capabilities."""

    name: str
    type: ServiceType
    version: str
    base_url: str
    health_endpoint: str = "/health"
    metrics_endpoint: str = "/metrics"
    capabilities: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    max_instances: int = 3
    min_instances: int = 1
    resource_requirements: dict[str, Any] = field(default_factory=dict)
    circuit_breaker_config: dict[str, Any] = field(default_factory=dict)
    retry_config: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ServiceInstance:
    """Service instance runtime information."""

    instance_id: str
    service_name: str
    host: str
    port: int
    status: ServiceStatus = ServiceStatus.UNKNOWN
    last_health_check: datetime | None = None
    response_time_ms: float = 0.0
    error_count: int = 0
    success_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.error_count
        return self.success_count / total if total > 0 else 0.0


class MicroservicesOrchestrator:
    """
    Advanced microservices orchestrator with comprehensive patterns.

    Features:
    - Service discovery and registry
    - Health monitoring and circuit breakers
    - Load balancing and routing
    - CQRS and Event Sourcing
    - Saga pattern for distributed transactions
    - Service mesh integration
    - Observability and metrics
    """

    def __init__(self):
        self.services: dict[str, ServiceDefinition] = {}
        self.instances: dict[str, list[ServiceInstance]] = {}
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
        self.cqrs_framework = CQRSFramework()
        self.saga_orchestrator = SagaOrchestrator()

        # Service mesh configuration
        self.service_mesh_enabled = True
        self.load_balancer_strategy = "round_robin"

        # Health check configuration
        self.health_check_interval = 30  # seconds
        self.health_check_timeout = 5  # seconds
        self._health_check_task: asyncio.Task | None = None

        # Metrics and monitoring
        self.metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "avg_response_time": 0.0,
            "circuit_breaker_trips": 0,
        }

        # Initialize core services
        self._register_core_services()

        # Start background tasks
        self._start_background_tasks()

    def _register_core_services(self):
        """Register core microservices."""
        core_services = [
            ServiceDefinition(
                name="auth-service",
                type=ServiceType.CORE,
                version="1.0.0",
                base_url="http://localhost:8001",
                capabilities=["authentication", "authorization", "jwt", "oauth2"],
                dependencies=[],
                circuit_breaker_config={"failure_threshold": 5, "timeout": 60},
            ),
            ServiceDefinition(
                name="sales-service",
                type=ServiceType.BUSINESS,
                version="1.0.0",
                base_url="http://localhost:8002",
                capabilities=["sales_data", "analytics", "reporting"],
                dependencies=["auth-service"],
                circuit_breaker_config={"failure_threshold": 3, "timeout": 30},
            ),
            ServiceDefinition(
                name="analytics-service",
                type=ServiceType.BUSINESS,
                version="2.0.0",
                base_url="http://localhost:8003",
                capabilities=["advanced_analytics", "ml_models", "predictions"],
                dependencies=["sales-service", "auth-service"],
                circuit_breaker_config={"failure_threshold": 3, "timeout": 45},
            ),
            ServiceDefinition(
                name="search-service",
                type=ServiceType.BUSINESS,
                version="1.0.0",
                base_url="http://localhost:8004",
                capabilities=["vector_search", "text_search", "semantic_search"],
                dependencies=["auth-service"],
                circuit_breaker_config={"failure_threshold": 5, "timeout": 30},
            ),
            ServiceDefinition(
                name="datamart-service",
                type=ServiceType.BUSINESS,
                version="1.0.0",
                base_url="http://localhost:8005",
                capabilities=["data_mart", "etl_status", "data_quality"],
                dependencies=["auth-service"],
                circuit_breaker_config={"failure_threshold": 3, "timeout": 60},
            ),
            ServiceDefinition(
                name="notification-service",
                type=ServiceType.INFRASTRUCTURE,
                version="1.0.0",
                base_url="http://localhost:8006",
                capabilities=["email", "sms", "push_notifications", "webhooks"],
                dependencies=["auth-service"],
                circuit_breaker_config={"failure_threshold": 10, "timeout": 30},
            ),
        ]

        for service in core_services:
            self.register_service(service)

    def register_service(self, service: ServiceDefinition) -> bool:
        """Register a microservice."""
        try:
            self.services[service.name] = service
            if service.name not in self.instances:
                self.instances[service.name] = []

            logger.info(f"Registered service: {service.name} v{service.version}")

            # Publish service registration event
            self.kafka_manager.produce_service_event(
                service_name=service.name,
                event_type="service_registered",
                metadata={
                    "version": service.version,
                    "type": service.type.value,
                    "capabilities": service.capabilities,
                },
            )

            return True

        except Exception as e:
            logger.error(f"Failed to register service {service.name}: {e}")
            return False

    def register_instance(
        self, service_name: str, host: str, port: int, metadata: dict[str, Any] | None = None
    ) -> str | None:
        """Register a service instance."""
        if service_name not in self.services:
            logger.error(f"Cannot register instance for unknown service: {service_name}")
            return None

        instance_id = f"{service_name}-{uuid4().hex[:8]}"
        instance = ServiceInstance(
            instance_id=instance_id,
            service_name=service_name,
            host=host,
            port=port,
            metadata=metadata or {},
        )

        self.instances[service_name].append(instance)
        logger.info(f"Registered instance: {instance_id} for {service_name}")

        # Publish instance registration event
        self.kafka_manager.produce_service_event(
            service_name=service_name,
            event_type="instance_registered",
            metadata={"instance_id": instance_id, "host": host, "port": port},
        )

        return instance_id

    def get_healthy_instances(self, service_name: str) -> list[ServiceInstance]:
        """Get healthy instances for a service."""
        if service_name not in self.instances:
            return []

        return [
            instance
            for instance in self.instances[service_name]
            if instance.status == ServiceStatus.HEALTHY
        ]

    def get_service_instance(
        self, service_name: str, strategy: str = "round_robin"
    ) -> ServiceInstance | None:
        """Get service instance using load balancing strategy."""
        healthy_instances = self.get_healthy_instances(service_name)

        if not healthy_instances:
            logger.warning(f"No healthy instances available for service: {service_name}")
            return None

        if strategy == "round_robin":
            # Simple round-robin selection
            import random

            return random.choice(healthy_instances)
        elif strategy == "least_connections":
            # Select instance with lowest error count (proxy for connections)
            return min(healthy_instances, key=lambda x: x.error_count)
        elif strategy == "response_time":
            # Select instance with best response time
            return min(healthy_instances, key=lambda x: x.response_time_ms)
        else:
            return healthy_instances[0]

    async def discover_services(self) -> dict[str, list[str]]:
        """Discover available services and their capabilities."""
        discovery_map = {}

        for service_name, service_def in self.services.items():
            healthy_instances = self.get_healthy_instances(service_name)
            discovery_map[service_name] = {
                "type": service_def.type.value,
                "version": service_def.version,
                "capabilities": service_def.capabilities,
                "dependencies": service_def.dependencies,
                "healthy_instances": len(healthy_instances),
                "total_instances": len(self.instances.get(service_name, [])),
                "endpoints": {
                    "base": service_def.base_url,
                    "health": f"{service_def.base_url}{service_def.health_endpoint}",
                    "metrics": f"{service_def.base_url}{service_def.metrics_endpoint}",
                },
            }

        return discovery_map

    async def check_service_health(self, instance: ServiceInstance) -> bool:
        """Check health of a service instance."""
        import time

        import httpx

        start_time = time.time()

        try:
            service_def = self.services[instance.service_name]
            health_url = f"{instance.base_url}{service_def.health_endpoint}"

            async with httpx.AsyncClient(timeout=self.health_check_timeout) as client:
                response = await client.get(health_url)

                response_time = (time.time() - start_time) * 1000
                instance.response_time_ms = response_time
                instance.last_health_check = datetime.utcnow()

                if response.status_code == 200:
                    instance.status = ServiceStatus.HEALTHY
                    instance.success_count += 1
                    return True
                else:
                    instance.status = ServiceStatus.UNHEALTHY
                    instance.error_count += 1
                    return False

        except Exception as e:
            logger.warning(f"Health check failed for {instance.instance_id}: {e}")
            instance.status = ServiceStatus.UNHEALTHY
            instance.error_count += 1
            instance.last_health_check = datetime.utcnow()
            return False

    async def health_check_worker(self):
        """Background worker for health checks."""
        while True:
            try:
                health_check_tasks = []

                for _service_name, instances in self.instances.items():
                    for instance in instances:
                        # Only check instances that haven't been checked recently
                        if (
                            not instance.last_health_check
                            or datetime.utcnow() - instance.last_health_check
                            > timedelta(seconds=self.health_check_interval)
                        ):
                            task = asyncio.create_task(self.check_service_health(instance))
                            health_check_tasks.append((task, instance))

                # Wait for all health checks to complete
                if health_check_tasks:
                    for task, instance in health_check_tasks:
                        try:
                            await task
                        except Exception as e:
                            logger.error(
                                f"Health check task failed for {instance.instance_id}: {e}"
                            )

                await asyncio.sleep(self.health_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check worker error: {e}")
                await asyncio.sleep(5)  # Brief pause before continuing

    def _start_background_tasks(self):
        """Start background monitoring tasks."""
        self._health_check_task = asyncio.create_task(self.health_check_worker())
        logger.info("Started microservices background tasks")

    async def execute_distributed_transaction(
        self, saga_definition: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute distributed transaction using Saga pattern."""
        try:
            saga_id = f"saga-{uuid4().hex[:8]}"

            # Create saga transaction
            saga_transaction = SagaTransaction(
                saga_id=saga_id,
                steps=saga_definition.get("steps", []),
                compensation_steps=saga_definition.get("compensation_steps", []),
                timeout=saga_definition.get("timeout", 300),
            )

            # Execute saga
            result = await self.saga_orchestrator.execute_saga(saga_transaction)

            # Publish saga completion event
            self.kafka_manager.produce_saga_event(
                saga_id=saga_id,
                event_type="saga_completed" if result["success"] else "saga_failed",
                metadata={
                    "steps_completed": result.get("steps_completed", 0),
                    "total_steps": len(saga_transaction.steps),
                    "execution_time_ms": result.get("execution_time_ms", 0),
                },
            )

            return result

        except Exception as e:
            logger.error(f"Distributed transaction failed: {e}")
            return {"success": False, "error": str(e), "saga_id": saga_id}

    async def handle_command(self, command: Command) -> dict[str, Any]:
        """Handle CQRS command."""
        try:
            result = await self.cqrs_framework.handle_command(command)

            # Update metrics
            self.metrics["total_requests"] += 1
            if result.get("success", False):
                self.metrics["successful_requests"] += 1
            else:
                self.metrics["failed_requests"] += 1

            return result

        except Exception as e:
            logger.error(f"Command handling failed: {e}")
            self.metrics["total_requests"] += 1
            self.metrics["failed_requests"] += 1
            return {"success": False, "error": str(e), "command_type": command.__class__.__name__}

    async def handle_query(self, query: Query) -> dict[str, Any]:
        """Handle CQRS query."""
        try:
            result = await self.cqrs_framework.handle_query(query)

            # Update metrics
            self.metrics["total_requests"] += 1
            self.metrics["successful_requests"] += 1

            return result

        except Exception as e:
            logger.error(f"Query handling failed: {e}")
            self.metrics["total_requests"] += 1
            self.metrics["failed_requests"] += 1
            return {"success": False, "error": str(e), "query_type": query.__class__.__name__}

    def get_service_topology(self) -> dict[str, Any]:
        """Get service dependency topology."""
        topology = {"services": {}, "dependencies": {}, "dependency_graph": []}

        for service_name, service_def in self.services.items():
            topology["services"][service_name] = {
                "type": service_def.type.value,
                "version": service_def.version,
                "capabilities": service_def.capabilities,
                "instances": len(self.instances.get(service_name, [])),
                "healthy_instances": len(self.get_healthy_instances(service_name)),
            }

            # Map dependencies
            topology["dependencies"][service_name] = service_def.dependencies

            # Create dependency graph edges
            for dependency in service_def.dependencies:
                topology["dependency_graph"].append(
                    {"from": service_name, "to": dependency, "type": "depends_on"}
                )

        return topology

    def get_orchestrator_metrics(self) -> dict[str, Any]:
        """Get comprehensive orchestrator metrics."""
        total_instances = sum(len(instances) for instances in self.instances.values())
        healthy_instances = sum(
            len(self.get_healthy_instances(name)) for name in self.instances.keys()
        )

        return {
            "services_registered": len(self.services),
            "total_instances": total_instances,
            "healthy_instances": healthy_instances,
            "instance_health_rate": healthy_instances / total_instances
            if total_instances > 0
            else 0,
            "request_metrics": dict(self.metrics),
            "services_by_type": {
                service_type.value: len(
                    [s for s in self.services.values() if s.type == service_type]
                )
                for service_type in ServiceType
            },
            "avg_response_time_ms": sum(
                instance.response_time_ms
                for instances in self.instances.values()
                for instance in instances
                if instance.response_time_ms > 0
            )
            / max(1, sum(len(instances) for instances in self.instances.values())),
            "health_check_interval": self.health_check_interval,
            "service_mesh_enabled": self.service_mesh_enabled,
        }

    async def scale_service(self, service_name: str, target_instances: int) -> dict[str, Any]:
        """Scale service instances (placeholder for auto-scaling logic)."""
        if service_name not in self.services:
            return {"success": False, "error": f"Service {service_name} not found"}

        current_instances = len(self.instances.get(service_name, []))
        service_def = self.services[service_name]

        # Validate scaling limits
        if target_instances > service_def.max_instances:
            target_instances = service_def.max_instances
        elif target_instances < service_def.min_instances:
            target_instances = service_def.min_instances

        logger.info(
            f"Scaling {service_name} from {current_instances} to {target_instances} instances"
        )

        # Publish scaling event
        self.kafka_manager.produce_service_event(
            service_name=service_name,
            event_type="service_scaling",
            metadata={
                "current_instances": current_instances,
                "target_instances": target_instances,
                "scaling_reason": "manual",
            },
        )

        return {
            "success": True,
            "service_name": service_name,
            "current_instances": current_instances,
            "target_instances": target_instances,
            "scaling_action": "scale_up" if target_instances > current_instances else "scale_down",
        }

    async def shutdown(self):
        """Gracefully shutdown the orchestrator."""
        logger.info("Shutting down microservices orchestrator...")

        # Cancel health check task
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Close messaging systems
        if hasattr(self.rabbitmq_manager, "close"):
            await self.rabbitmq_manager.close()

        if hasattr(self.kafka_manager, "close"):
            await self.kafka_manager.close()

        logger.info("Microservices orchestrator shutdown complete")


# Global orchestrator instance
_orchestrator: MicroservicesOrchestrator | None = None


def get_microservices_orchestrator() -> MicroservicesOrchestrator:
    """Get or create global microservices orchestrator."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = MicroservicesOrchestrator()
    return _orchestrator
