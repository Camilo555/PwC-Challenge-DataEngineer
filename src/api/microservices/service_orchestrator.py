"""Advanced Microservices Orchestrator
Enterprise-grade service decomposition and orchestration with event-driven patterns
"""

from __future__ import annotations

import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import httpx

from api.gateway.service_registry import ServiceInstance, ServiceRegistry
from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager
from streaming.kafka_manager import KafkaManager

logger = get_logger(__name__)


class ServiceStatus(Enum):
    """Service status enumeration"""

    INITIALIZING = "initializing"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    MAINTENANCE = "maintenance"
    TERMINATED = "terminated"


class TransactionState(Enum):
    """Distributed transaction state"""

    PENDING = "pending"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"


class CommunicationPattern(Enum):
    """Inter-service communication patterns"""

    SYNCHRONOUS = "synchronous"  # HTTP/gRPC
    ASYNCHRONOUS = "asynchronous"  # Message queues
    EVENT_STREAMING = "event_streaming"  # Kafka/Kinesis
    HYBRID = "hybrid"  # Mixed patterns


@dataclass
class ServiceDefinition:
    """Microservice definition with boundaries and dependencies"""

    name: str
    version: str
    domain: str  # Business domain (e.g., 'sales', 'auth', 'analytics')
    bounded_context: str  # DDD bounded context
    responsibilities: list[str]
    dependencies: list[str] = field(default_factory=list)
    communication_patterns: dict[str, CommunicationPattern] = field(default_factory=dict)
    data_ownership: list[str] = field(default_factory=list)
    sla_requirements: dict[str, Any] = field(default_factory=dict)
    scaling_policy: dict[str, Any] = field(default_factory=dict)
    health_check_endpoints: list[str] = field(default_factory=list)
    metrics_endpoints: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.sla_requirements:
            self.sla_requirements = {
                "availability": 99.9,
                "response_time_p99_ms": 1000,
                "throughput_rps": 1000,
            }

        if not self.scaling_policy:
            self.scaling_policy = {
                "min_instances": 2,
                "max_instances": 10,
                "cpu_threshold": 70,
                "memory_threshold": 80,
            }


@dataclass
class SagaStep:
    """Single step in a distributed saga transaction"""

    id: str
    service_name: str
    operation: str
    payload: dict[str, Any]
    compensation_operation: str | None = None
    compensation_payload: dict[str, Any] | None = None
    timeout_seconds: int = 30
    retry_attempts: int = 3
    depends_on: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributedTransaction:
    """Distributed transaction using Saga pattern"""

    id: str
    name: str
    steps: list[SagaStep]
    state: TransactionState = TransactionState.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    completed_steps: list[str] = field(default_factory=list)
    failed_steps: list[str] = field(default_factory=list)
    compensated_steps: list[str] = field(default_factory=list)
    error_details: dict[str, Any] | None = None
    timeout_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.timeout_at is None:
            # Default timeout of 5 minutes for distributed transactions
            self.timeout_at = self.created_at + timedelta(minutes=5)


class EventBus(ABC):
    """Abstract event bus for inter-service communication"""

    @abstractmethod
    async def publish_event(
        self, event_type: str, payload: dict[str, Any], routing_key: str = None
    ) -> bool:
        pass

    @abstractmethod
    async def subscribe_to_events(self, event_types: list[str], callback: Callable) -> str:
        pass

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        pass


class RabbitMQEventBus(EventBus):
    """RabbitMQ-based event bus implementation"""

    def __init__(self, rabbitmq_manager: RabbitMQManager):
        self.rabbitmq = rabbitmq_manager
        self.subscriptions: dict[str, dict[str, Any]] = {}

    async def publish_event(
        self, event_type: str, payload: dict[str, Any], routing_key: str = None
    ) -> bool:
        try:
            event_data = {
                "event_type": event_type,
                "payload": payload,
                "timestamp": datetime.utcnow().isoformat(),
                "event_id": str(uuid.uuid4()),
            }

            self.rabbitmq.publish_event(
                event_type=event_type, data=event_data, routing_key=routing_key or event_type
            )
            return True

        except Exception as e:
            logger.error(f"Failed to publish event {event_type}: {e}")
            return False

    async def subscribe_to_events(self, event_types: list[str], callback: Callable) -> str:
        subscription_id = str(uuid.uuid4())

        def event_handler(event_data):
            asyncio.create_task(callback(event_data))

        # Subscribe to each event type
        for event_type in event_types:
            self.rabbitmq.subscribe_to_events(event_types=[event_type], callback=event_handler)

        self.subscriptions[subscription_id] = {"event_types": event_types, "callback": callback}

        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        if subscription_id in self.subscriptions:
            del self.subscriptions[subscription_id]
            return True
        return False


class KafkaEventBus(EventBus):
    """Kafka-based event bus for high-throughput event streaming"""

    def __init__(self, kafka_manager: KafkaManager):
        self.kafka = kafka_manager
        self.subscriptions: dict[str, dict[str, Any]] = {}

    async def publish_event(
        self, event_type: str, payload: dict[str, Any], routing_key: str = None
    ) -> bool:
        try:
            topic = routing_key or f"microservices.{event_type}"

            event_data = {
                "event_type": event_type,
                "payload": payload,
                "timestamp": datetime.utcnow().isoformat(),
                "event_id": str(uuid.uuid4()),
            }

            self.kafka.produce_microservice_event(
                service_name=event_type, event_type=event_type, data=event_data, topic=topic
            )
            return True

        except Exception as e:
            logger.error(f"Failed to publish event {event_type} to Kafka: {e}")
            return False

    async def subscribe_to_events(self, event_types: list[str], callback: Callable) -> str:
        subscription_id = str(uuid.uuid4())

        # In a real implementation, set up Kafka consumer
        # For now, store subscription info
        self.subscriptions[subscription_id] = {"event_types": event_types, "callback": callback}

        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        if subscription_id in self.subscriptions:
            del self.subscriptions[subscription_id]
            return True
        return False


class SagaOrchestrator:
    """Saga pattern orchestrator for distributed transactions"""

    def __init__(self, event_bus: EventBus, service_registry: ServiceRegistry):
        self.event_bus = event_bus
        self.service_registry = service_registry
        self.active_transactions: dict[str, DistributedTransaction] = {}
        self.http_client = httpx.AsyncClient(timeout=30.0)

        # Subscribe to transaction events
        asyncio.create_task(self._setup_event_subscriptions())

    async def _setup_event_subscriptions(self):
        """Set up event subscriptions for saga coordination"""
        await self.event_bus.subscribe_to_events(
            ["step_completed", "step_failed", "compensation_completed"], self._handle_saga_event
        )

    async def _handle_saga_event(self, event_data: dict[str, Any]):
        """Handle saga-related events"""
        event_type = event_data.get("event_type")
        payload = event_data.get("payload", {})
        transaction_id = payload.get("transaction_id")

        if transaction_id not in self.active_transactions:
            return

        transaction = self.active_transactions[transaction_id]

        if event_type == "step_completed":
            await self._handle_step_completion(transaction, payload)
        elif event_type == "step_failed":
            await self._handle_step_failure(transaction, payload)
        elif event_type == "compensation_completed":
            await self._handle_compensation_completion(transaction, payload)

    async def execute_saga(self, transaction: DistributedTransaction) -> bool:
        """Execute distributed transaction using Saga pattern"""
        self.active_transactions[transaction.id] = transaction

        logger.info(f"Starting saga transaction: {transaction.name} ({transaction.id})")

        try:
            # Publish transaction started event
            await self.event_bus.publish_event(
                "saga_started",
                {
                    "transaction_id": transaction.id,
                    "transaction_name": transaction.name,
                    "steps_count": len(transaction.steps),
                },
            )

            transaction.state = TransactionState.COMMITTING

            # Execute steps in order, respecting dependencies
            await self._execute_saga_steps(transaction)

            if len(transaction.completed_steps) == len(transaction.steps):
                transaction.state = TransactionState.COMMITTED
                await self.event_bus.publish_event(
                    "saga_completed", {"transaction_id": transaction.id}
                )
                logger.info(f"Saga transaction completed: {transaction.id}")
                return True
            else:
                # Some steps failed, initiate compensation
                await self._compensate_saga(transaction)
                return False

        except Exception as e:
            logger.error(f"Saga execution failed: {e}")
            transaction.state = TransactionState.FAILED
            transaction.error_details = {"error": str(e)}
            await self._compensate_saga(transaction)
            return False

        finally:
            transaction.updated_at = datetime.utcnow()

    async def _execute_saga_steps(self, transaction: DistributedTransaction):
        """Execute saga steps with dependency resolution"""
        remaining_steps = transaction.steps.copy()

        while remaining_steps:
            # Find steps that can be executed (dependencies satisfied)
            executable_steps = [
                step
                for step in remaining_steps
                if all(dep in transaction.completed_steps for dep in step.depends_on)
            ]

            if not executable_steps:
                raise RuntimeError("Circular dependency detected in saga steps")

            # Execute steps in parallel where possible
            step_tasks = [self._execute_saga_step(transaction, step) for step in executable_steps]

            results = await asyncio.gather(*step_tasks, return_exceptions=True)

            # Process results
            for i, result in enumerate(results):
                step = executable_steps[i]

                if isinstance(result, Exception):
                    logger.error(f"Step {step.id} failed: {result}")
                    transaction.failed_steps.append(step.id)
                    transaction.error_details = {"failed_step": step.id, "error": str(result)}
                    break
                elif result:
                    transaction.completed_steps.append(step.id)
                    remaining_steps.remove(step)
                else:
                    transaction.failed_steps.append(step.id)
                    break

            # If any step failed, stop execution
            if transaction.failed_steps:
                break

    async def _execute_saga_step(self, transaction: DistributedTransaction, step: SagaStep) -> bool:
        """Execute a single saga step"""
        logger.info(f"Executing saga step: {step.id} on service {step.service_name}")

        # Get service instance
        service_instance = await self.service_registry.get_service_instance(step.service_name)

        if not service_instance:
            raise RuntimeError(f"Service {step.service_name} not available")

        # Prepare request
        url = f"{service_instance.base_url}/saga/{step.operation}"
        payload = {
            "transaction_id": transaction.id,
            "step_id": step.id,
            "payload": step.payload,
            "metadata": step.metadata,
        }

        # Execute with retries
        for attempt in range(step.retry_attempts + 1):
            try:
                response = await self.http_client.post(
                    url, json=payload, timeout=step.timeout_seconds
                )

                if response.status_code == 200:
                    # Publish step completed event
                    await self.event_bus.publish_event(
                        "step_completed",
                        {
                            "transaction_id": transaction.id,
                            "step_id": step.id,
                            "service_name": step.service_name,
                            "response": response.json(),
                        },
                    )
                    return True
                else:
                    logger.warning(
                        f"Step {step.id} returned {response.status_code}: {response.text}"
                    )

                    if attempt < step.retry_attempts:
                        await asyncio.sleep(2**attempt)  # Exponential backoff
                    else:
                        raise RuntimeError(f"Step failed after {step.retry_attempts} attempts")

            except Exception as e:
                if attempt < step.retry_attempts:
                    logger.warning(f"Step {step.id} attempt {attempt + 1} failed: {e}")
                    await asyncio.sleep(2**attempt)
                else:
                    raise e

        return False

    async def _compensate_saga(self, transaction: DistributedTransaction):
        """Compensate completed steps in reverse order"""
        logger.info(f"Starting compensation for transaction: {transaction.id}")

        transaction.state = TransactionState.COMPENSATING

        # Compensate completed steps in reverse order
        for step_id in reversed(transaction.completed_steps):
            step = next((s for s in transaction.steps if s.id == step_id), None)

            if step and step.compensation_operation:
                try:
                    await self._execute_compensation(transaction, step)
                    transaction.compensated_steps.append(step.id)
                except Exception as e:
                    logger.error(f"Compensation failed for step {step.id}: {e}")

        transaction.state = TransactionState.COMPENSATED
        await self.event_bus.publish_event(
            "saga_compensated",
            {
                "transaction_id": transaction.id,
                "compensated_steps": len(transaction.compensated_steps),
            },
        )

    async def _execute_compensation(self, transaction: DistributedTransaction, step: SagaStep):
        """Execute compensation for a saga step"""
        logger.info(f"Compensating step: {step.id} on service {step.service_name}")

        service_instance = await self.service_registry.get_service_instance(step.service_name)

        if not service_instance:
            raise RuntimeError(f"Service {step.service_name} not available for compensation")

        url = f"{service_instance.base_url}/saga/{step.compensation_operation}"
        payload = {
            "transaction_id": transaction.id,
            "step_id": step.id,
            "payload": step.compensation_payload or {},
            "original_payload": step.payload,
            "metadata": step.metadata,
        }

        response = await self.http_client.post(url, json=payload, timeout=step.timeout_seconds)

        if response.status_code != 200:
            raise RuntimeError(f"Compensation failed: {response.text}")

    async def _handle_step_completion(
        self, transaction: DistributedTransaction, payload: dict[str, Any]
    ):
        """Handle step completion event"""
        step_id = payload.get("step_id")
        if step_id and step_id not in transaction.completed_steps:
            transaction.completed_steps.append(step_id)
            logger.info(f"Step {step_id} completed in transaction {transaction.id}")

    async def _handle_step_failure(
        self, transaction: DistributedTransaction, payload: dict[str, Any]
    ):
        """Handle step failure event"""
        step_id = payload.get("step_id")
        if step_id and step_id not in transaction.failed_steps:
            transaction.failed_steps.append(step_id)
            transaction.error_details = payload.get("error_details", {})
            logger.error(f"Step {step_id} failed in transaction {transaction.id}")

    async def _handle_compensation_completion(
        self, transaction: DistributedTransaction, payload: dict[str, Any]
    ):
        """Handle compensation completion event"""
        step_id = payload.get("step_id")
        if step_id and step_id not in transaction.compensated_steps:
            transaction.compensated_steps.append(step_id)
            logger.info(
                f"Compensation for step {step_id} completed in transaction {transaction.id}"
            )

    def get_transaction_status(self, transaction_id: str) -> dict[str, Any] | None:
        """Get transaction status"""
        transaction = self.active_transactions.get(transaction_id)

        if not transaction:
            return None

        return {
            "transaction_id": transaction.id,
            "name": transaction.name,
            "state": transaction.state.value,
            "created_at": transaction.created_at.isoformat(),
            "updated_at": transaction.updated_at.isoformat(),
            "total_steps": len(transaction.steps),
            "completed_steps": len(transaction.completed_steps),
            "failed_steps": len(transaction.failed_steps),
            "compensated_steps": len(transaction.compensated_steps),
            "progress_percent": (len(transaction.completed_steps) / len(transaction.steps)) * 100,
            "error_details": transaction.error_details,
        }

    async def cleanup_completed_transactions(self, max_age_hours: int = 24):
        """Clean up old completed transactions"""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)

        completed_transaction_ids = [
            tx_id
            for tx_id, tx in self.active_transactions.items()
            if tx.state
            in [TransactionState.COMMITTED, TransactionState.ABORTED, TransactionState.COMPENSATED]
            and tx.updated_at < cutoff_time
        ]

        for tx_id in completed_transaction_ids:
            del self.active_transactions[tx_id]
            logger.info(f"Cleaned up completed transaction: {tx_id}")

        return len(completed_transaction_ids)


class MicroservicesOrchestrator:
    """Main orchestrator for microservices architecture"""

    def __init__(self):
        self.service_definitions: dict[str, ServiceDefinition] = {}
        self.service_registry = ServiceRegistry()

        # Initialize messaging systems
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()

        # Initialize event buses
        self.rabbitmq_event_bus = RabbitMQEventBus(self.rabbitmq_manager)
        self.kafka_event_bus = KafkaEventBus(self.kafka_manager)

        # Initialize saga orchestrator
        self.saga_orchestrator = SagaOrchestrator(self.rabbitmq_event_bus, self.service_registry)

        # Service metrics
        self.service_metrics: dict[str, dict[str, Any]] = defaultdict(dict)

        # Start background tasks
        asyncio.create_task(self._start_background_tasks())

    async def _start_background_tasks(self):
        """Start background monitoring and cleanup tasks"""
        await asyncio.gather(
            self._monitor_services(), self._cleanup_transactions(), self._collect_service_metrics()
        )

    def register_service(self, service_def: ServiceDefinition) -> bool:
        """Register a microservice definition"""
        self.service_definitions[service_def.name] = service_def

        logger.info(
            f"Registered service: {service_def.name} v{service_def.version} "
            f"in domain: {service_def.domain}"
        )

        return True

    async def deploy_service(self, service_name: str, instances: list[ServiceInstance]) -> bool:
        """Deploy service instances"""
        service_def = self.service_definitions.get(service_name)

        if not service_def:
            logger.error(f"Service definition not found: {service_name}")
            return False

        # Register service instances
        for instance in instances:
            await self.service_registry.register_service(
                service_name=service_name, instance=instance
            )

        # Publish service deployment event
        await self.rabbitmq_event_bus.publish_event(
            "service_deployed",
            {
                "service_name": service_name,
                "version": service_def.version,
                "instances_count": len(instances),
                "domain": service_def.domain,
            },
        )

        logger.info(f"Deployed {len(instances)} instances of service: {service_name}")
        return True

    async def execute_distributed_transaction(self, transaction: DistributedTransaction) -> bool:
        """Execute distributed transaction using Saga pattern"""
        return await self.saga_orchestrator.execute_saga(transaction)

    async def get_service_health(self, service_name: str) -> dict[str, Any]:
        """Get comprehensive service health status"""
        service_def = self.service_definitions.get(service_name)

        if not service_def:
            return {"status": "unknown", "error": "Service not found"}

        # Get service instances
        instances = await self.service_registry.get_all_instances(service_name)

        if not instances:
            return {"status": "down", "instances": 0}

        healthy_instances = 0
        instance_statuses = []

        # Check each instance health
        for instance in instances:
            instance_health = await self._check_instance_health(instance, service_def)
            instance_statuses.append(instance_health)

            if instance_health["status"] == "healthy":
                healthy_instances += 1

        # Determine overall service status
        total_instances = len(instances)
        health_ratio = healthy_instances / total_instances

        if health_ratio >= 0.8:
            overall_status = ServiceStatus.HEALTHY
        elif health_ratio >= 0.5:
            overall_status = ServiceStatus.DEGRADED
        else:
            overall_status = ServiceStatus.UNHEALTHY

        return {
            "service_name": service_name,
            "status": overall_status.value,
            "total_instances": total_instances,
            "healthy_instances": healthy_instances,
            "health_ratio": health_ratio,
            "instances": instance_statuses,
            "sla_compliance": self._check_sla_compliance(service_name, service_def),
            "metrics": self.service_metrics.get(service_name, {}),
        }

    async def _check_instance_health(
        self, instance: ServiceInstance, service_def: ServiceDefinition
    ) -> dict[str, Any]:
        """Check health of a single service instance"""
        health_status = {
            "instance_id": instance.instance_id,
            "base_url": instance.base_url,
            "status": "unknown",
            "response_time_ms": 0,
            "last_check": datetime.utcnow().isoformat(),
        }

        try:
            # Check health endpoints
            start_time = time.time()

            for health_endpoint in service_def.health_check_endpoints:
                url = f"{instance.base_url}{health_endpoint}"

                async with httpx.AsyncClient() as client:
                    response = await client.get(url, timeout=10.0)

                    if response.status_code == 200:
                        health_status["status"] = "healthy"
                        health_status["response_time_ms"] = (time.time() - start_time) * 1000
                        break
                    else:
                        health_status["status"] = "unhealthy"
                        health_status["error"] = f"HTTP {response.status_code}"

        except Exception as e:
            health_status["status"] = "unhealthy"
            health_status["error"] = str(e)

        return health_status

    def _check_sla_compliance(
        self, service_name: str, service_def: ServiceDefinition
    ) -> dict[str, Any]:
        """Check SLA compliance for service"""
        metrics = self.service_metrics.get(service_name, {})
        sla_requirements = service_def.sla_requirements

        compliance = {
            "availability": True,
            "response_time": True,
            "throughput": True,
            "overall_compliant": True,
        }

        # Check availability
        if "availability_percent" in metrics:
            availability = metrics["availability_percent"]
            required_availability = sla_requirements.get("availability", 99.9)
            compliance["availability"] = availability >= required_availability

        # Check response time
        if "response_time_p99_ms" in metrics:
            response_time = metrics["response_time_p99_ms"]
            required_response_time = sla_requirements.get("response_time_p99_ms", 1000)
            compliance["response_time"] = response_time <= required_response_time

        # Check throughput
        if "throughput_rps" in metrics:
            throughput = metrics["throughput_rps"]
            required_throughput = sla_requirements.get("throughput_rps", 1000)
            compliance["throughput"] = throughput >= required_throughput

        compliance["overall_compliant"] = all(
            [compliance["availability"], compliance["response_time"], compliance["throughput"]]
        )

        return compliance

    async def _monitor_services(self):
        """Background task to monitor all services"""
        while True:
            try:
                for service_name in self.service_definitions.keys():
                    health = await self.get_service_health(service_name)

                    # Publish service health events
                    await self.kafka_event_bus.publish_event(
                        "service_health_update",
                        {
                            "service_name": service_name,
                            "health": health,
                            "timestamp": datetime.utcnow().isoformat(),
                        },
                    )

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                logger.error(f"Service monitoring error: {e}")
                await asyncio.sleep(60)

    async def _cleanup_transactions(self):
        """Background task to clean up old transactions"""
        while True:
            try:
                cleaned_count = await self.saga_orchestrator.cleanup_completed_transactions()

                if cleaned_count > 0:
                    logger.info(f"Cleaned up {cleaned_count} completed transactions")

                await asyncio.sleep(3600)  # Run every hour

            except Exception as e:
                logger.error(f"Transaction cleanup error: {e}")
                await asyncio.sleep(3600)

    async def _collect_service_metrics(self):
        """Background task to collect service metrics"""
        while True:
            try:
                for service_name, service_def in self.service_definitions.items():
                    # Collect metrics from service endpoints
                    instances = await self.service_registry.get_all_instances(service_name)

                    if instances:
                        metrics = await self._collect_instance_metrics(instances[0], service_def)
                        self.service_metrics[service_name].update(metrics)

                await asyncio.sleep(60)  # Collect every minute

            except Exception as e:
                logger.error(f"Metrics collection error: {e}")
                await asyncio.sleep(300)

    async def _collect_instance_metrics(
        self, instance: ServiceInstance, service_def: ServiceDefinition
    ) -> dict[str, Any]:
        """Collect metrics from a service instance"""
        metrics = {}

        try:
            for metrics_endpoint in service_def.metrics_endpoints:
                url = f"{instance.base_url}{metrics_endpoint}"

                async with httpx.AsyncClient() as client:
                    response = await client.get(url, timeout=10.0)

                    if response.status_code == 200:
                        endpoint_metrics = response.json()
                        metrics.update(endpoint_metrics)

        except Exception as e:
            logger.warning(f"Failed to collect metrics from {instance.instance_id}: {e}")

        return metrics

    async def get_orchestrator_status(self) -> dict[str, Any]:
        """Get comprehensive orchestrator status"""
        return {
            "registered_services": len(self.service_definitions),
            "active_transactions": len(self.saga_orchestrator.active_transactions),
            "services": {
                name: {
                    "domain": service_def.domain,
                    "version": service_def.version,
                    "responsibilities_count": len(service_def.responsibilities),
                    "dependencies_count": len(service_def.dependencies),
                }
                for name, service_def in self.service_definitions.items()
            },
            "event_buses": {
                "rabbitmq": {"subscriptions": len(self.rabbitmq_event_bus.subscriptions)},
                "kafka": {"subscriptions": len(self.kafka_event_bus.subscriptions)},
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

    async def _initialize_enterprise_services(self):
        """Initialize enterprise service deployment patterns"""
        try:
            # Deploy standard microservices stack
            await deploy_standard_microservices_stack(self)

            # Initialize service mesh integration
            mesh_integration = EnhancedServiceMeshIntegration(self)

            # Configure circuit breakers for critical services
            await mesh_integration.configure_circuit_breaker("auth-service", 3, 60)
            await mesh_integration.configure_circuit_breaker("sales-service", 5, 30)
            await mesh_integration.configure_circuit_breaker("analytics-service", 10, 120)

            # Configure retry policies
            await mesh_integration.configure_retry_policy("auth-service", 2, "1s")
            await mesh_integration.configure_retry_policy("sales-service", 3, "2s")
            await mesh_integration.configure_retry_policy("analytics-service", 2, "5s")

            logger.info("Enterprise services initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize enterprise services: {e}")

    async def execute_order_processing_workflow(
        self, customer_id: str, order_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute complete order processing workflow with saga pattern"""

        # Create order processing saga
        saga = create_order_processing_saga(customer_id, order_data)

        # Execute the distributed transaction
        success = await self.execute_distributed_transaction(saga)

        if success:
            result = {
                "success": True,
                "transaction_id": saga.id,
                "order_status": "completed",
                "message": "Order processed successfully",
            }
        else:
            result = {
                "success": False,
                "transaction_id": saga.id,
                "order_status": "failed",
                "message": "Order processing failed - compensation executed",
                "error_details": saga.error_details,
            }

        return result

    async def execute_analytics_pipeline_workflow(
        self, dataset_id: str, pipeline_config: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute analytics pipeline workflow with saga pattern"""

        # Create analytics pipeline saga
        saga = create_analytics_pipeline_saga(dataset_id, pipeline_config)

        # Execute the distributed transaction
        success = await self.execute_distributed_transaction(saga)

        if success:
            result = {
                "success": True,
                "transaction_id": saga.id,
                "pipeline_status": "completed",
                "message": "Analytics pipeline completed successfully",
            }
        else:
            result = {
                "success": False,
                "transaction_id": saga.id,
                "pipeline_status": "failed",
                "message": "Analytics pipeline failed - cleanup executed",
                "error_details": saga.error_details,
            }

        return result


# Predefined service definitions for common microservices
def create_standard_service_definitions() -> list[ServiceDefinition]:
    """Create standard microservice definitions"""
    return [
        ServiceDefinition(
            name="auth-service",
            version="1.0.0",
            domain="authentication",
            bounded_context="user_identity",
            responsibilities=[
                "user_authentication",
                "token_management",
                "authorization",
                "user_session_management",
            ],
            dependencies=[],
            communication_patterns={
                "user_service": CommunicationPattern.SYNCHRONOUS,
                "audit_service": CommunicationPattern.ASYNCHRONOUS,
            },
            data_ownership=["users", "sessions", "tokens"],
            health_check_endpoints=["/health", "/auth/health"],
            metrics_endpoints=["/metrics", "/auth/metrics"],
            sla_requirements={
                "availability": 99.9,
                "response_time_p99_ms": 500,
                "throughput_rps": 2000,
            },
        ),
        ServiceDefinition(
            name="sales-service",
            version="2.0.0",
            domain="sales",
            bounded_context="sales_management",
            responsibilities=[
                "sales_data_management",
                "customer_analytics",
                "reporting",
                "data_aggregation",
            ],
            dependencies=["auth-service"],
            communication_patterns={
                "auth_service": CommunicationPattern.SYNCHRONOUS,
                "analytics_service": CommunicationPattern.EVENT_STREAMING,
                "notification_service": CommunicationPattern.ASYNCHRONOUS,
            },
            data_ownership=["sales_transactions", "customer_data", "products"],
            health_check_endpoints=["/health", "/api/v1/sales/health"],
            metrics_endpoints=["/metrics", "/api/v1/sales/metrics"],
        ),
        ServiceDefinition(
            name="analytics-service",
            version="1.5.0",
            domain="analytics",
            bounded_context="business_intelligence",
            responsibilities=[
                "advanced_analytics",
                "machine_learning",
                "predictive_modeling",
                "data_visualization",
            ],
            dependencies=["sales-service"],
            communication_patterns={
                "sales_service": CommunicationPattern.EVENT_STREAMING,
                "data_warehouse": CommunicationPattern.SYNCHRONOUS,
            },
            data_ownership=["analytics_models", "predictions", "insights"],
            health_check_endpoints=["/health", "/api/v2/analytics/health"],
            metrics_endpoints=["/metrics", "/api/v2/analytics/metrics"],
            sla_requirements={
                "availability": 99.5,
                "response_time_p99_ms": 2000,
                "throughput_rps": 500,
            },
            scaling_policy={
                "min_instances": 3,
                "max_instances": 15,
                "cpu_threshold": 75,
                "memory_threshold": 85,
            },
        ),
    ]


# Global orchestrator instance
_orchestrator: MicroservicesOrchestrator | None = None


def get_microservices_orchestrator() -> MicroservicesOrchestrator:
    """Get or create global microservices orchestrator instance"""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = MicroservicesOrchestrator()

        # Register standard service definitions
        for service_def in create_standard_service_definitions():
            _orchestrator.register_service(service_def)

        # Initialize enterprise deployment patterns
        asyncio.create_task(_orchestrator._initialize_enterprise_services())

    return _orchestrator


# Enhanced enterprise service patterns
async def deploy_standard_microservices_stack(orchestrator: MicroservicesOrchestrator):
    """Deploy standard microservices stack for enterprise deployment"""

    # Auth Service instances
    auth_instances = [
        ServiceInstance(
            instance_id="auth-001",
            host="auth-service-001.internal",
            port=8001,
            health_status="healthy",
            metadata={"version": "1.0.0", "region": "us-east-1"},
            weight=100,
        ),
        ServiceInstance(
            instance_id="auth-002",
            host="auth-service-002.internal",
            port=8001,
            health_status="healthy",
            metadata={"version": "1.0.0", "region": "us-west-2"},
            weight=100,
        ),
    ]

    # Sales Service instances
    sales_instances = [
        ServiceInstance(
            instance_id="sales-001",
            host="sales-service-001.internal",
            port=8002,
            health_status="healthy",
            metadata={"version": "2.0.0", "region": "us-east-1"},
            weight=150,
        ),
        ServiceInstance(
            instance_id="sales-002",
            host="sales-service-002.internal",
            port=8002,
            health_status="healthy",
            metadata={"version": "2.0.0", "region": "us-west-2"},
            weight=150,
        ),
        ServiceInstance(
            instance_id="sales-003",
            host="sales-service-003.internal",
            port=8002,
            health_status="healthy",
            metadata={"version": "2.0.0", "region": "eu-west-1"},
            weight=100,
        ),
    ]

    # Analytics Service instances
    analytics_instances = [
        ServiceInstance(
            instance_id="analytics-001",
            host="analytics-service-001.internal",
            port=8003,
            health_status="healthy",
            metadata={"version": "1.5.0", "region": "us-east-1"},
            weight=200,
        ),
        ServiceInstance(
            instance_id="analytics-002",
            host="analytics-service-002.internal",
            port=8003,
            health_status="healthy",
            metadata={"version": "1.5.0", "region": "us-west-2"},
            weight=200,
        ),
    ]

    # Deploy services
    await orchestrator.deploy_service("auth-service", auth_instances)
    await orchestrator.deploy_service("sales-service", sales_instances)
    await orchestrator.deploy_service("analytics-service", analytics_instances)

    logger.info("Standard microservices stack deployed successfully")


# Example distributed transaction patterns
def create_order_processing_saga(
    customer_id: str, order_data: dict[str, Any]
) -> DistributedTransaction:
    """Create saga for order processing across multiple services"""

    transaction_id = str(uuid.uuid4())

    steps = [
        # Step 1: Validate customer
        SagaStep(
            id="validate_customer",
            service_name="auth-service",
            operation="validate_customer",
            payload={"customer_id": customer_id},
            compensation_operation="release_customer_validation",
            timeout_seconds=30,
            retry_attempts=3,
        ),
        # Step 2: Reserve inventory
        SagaStep(
            id="reserve_inventory",
            service_name="inventory-service",
            operation="reserve_items",
            payload={"items": order_data.get("items", [])},
            compensation_operation="release_inventory",
            depends_on=["validate_customer"],
            timeout_seconds=45,
            retry_attempts=3,
        ),
        # Step 3: Process payment
        SagaStep(
            id="process_payment",
            service_name="payment-service",
            operation="charge_payment",
            payload={
                "customer_id": customer_id,
                "amount": order_data.get("total_amount"),
                "payment_method": order_data.get("payment_method"),
            },
            compensation_operation="refund_payment",
            depends_on=["validate_customer", "reserve_inventory"],
            timeout_seconds=60,
            retry_attempts=2,
        ),
        # Step 4: Create order record
        SagaStep(
            id="create_order",
            service_name="sales-service",
            operation="create_order",
            payload={"customer_id": customer_id, "order_data": order_data},
            compensation_operation="cancel_order",
            depends_on=["process_payment"],
            timeout_seconds=30,
            retry_attempts=3,
        ),
        # Step 5: Send confirmation
        SagaStep(
            id="send_confirmation",
            service_name="notification-service",
            operation="send_order_confirmation",
            payload={"customer_id": customer_id, "order_id": order_data.get("order_id")},
            compensation_operation="send_cancellation_notice",
            depends_on=["create_order"],
            timeout_seconds=30,
            retry_attempts=2,
        ),
    ]

    return DistributedTransaction(
        id=transaction_id,
        name="order_processing",
        steps=steps,
        metadata={"customer_id": customer_id, "order_type": "standard", "priority": "normal"},
    )


def create_analytics_pipeline_saga(
    dataset_id: str, pipeline_config: dict[str, Any]
) -> DistributedTransaction:
    """Create saga for analytics pipeline processing"""

    transaction_id = str(uuid.uuid4())

    steps = [
        # Step 1: Validate data source
        SagaStep(
            id="validate_data_source",
            service_name="data-service",
            operation="validate_dataset",
            payload={"dataset_id": dataset_id},
            compensation_operation="cleanup_validation",
            timeout_seconds=60,
            retry_attempts=2,
        ),
        # Step 2: Extract and transform data
        SagaStep(
            id="etl_processing",
            service_name="etl-service",
            operation="process_dataset",
            payload={"dataset_id": dataset_id, "config": pipeline_config},
            compensation_operation="cleanup_etl",
            depends_on=["validate_data_source"],
            timeout_seconds=300,
            retry_attempts=2,
        ),
        # Step 3: Run analytics
        SagaStep(
            id="run_analytics",
            service_name="analytics-service",
            operation="execute_analysis",
            payload={
                "dataset_id": dataset_id,
                "analysis_type": pipeline_config.get("analysis_type"),
            },
            compensation_operation="cleanup_analysis",
            depends_on=["etl_processing"],
            timeout_seconds=600,
            retry_attempts=1,
        ),
        # Step 4: Store results
        SagaStep(
            id="store_results",
            service_name="storage-service",
            operation="persist_results",
            payload={
                "dataset_id": dataset_id,
                "results_config": pipeline_config.get("output_config"),
            },
            compensation_operation="cleanup_results",
            depends_on=["run_analytics"],
            timeout_seconds=120,
            retry_attempts=2,
        ),
        # Step 5: Update metadata
        SagaStep(
            id="update_metadata",
            service_name="metadata-service",
            operation="update_pipeline_status",
            payload={
                "dataset_id": dataset_id,
                "status": "completed",
                "execution_time": None,  # Will be filled at runtime
            },
            depends_on=["store_results"],
            timeout_seconds=30,
            retry_attempts=3,
        ),
    ]

    return DistributedTransaction(
        id=transaction_id,
        name="analytics_pipeline",
        steps=steps,
        metadata={
            "dataset_id": dataset_id,
            "pipeline_type": pipeline_config.get("type", "batch"),
            "priority": pipeline_config.get("priority", "normal"),
        },
    )


# Enhanced service mesh integration patterns
class EnhancedServiceMeshIntegration:
    """Enhanced integration with service mesh for advanced traffic management"""

    def __init__(self, orchestrator: MicroservicesOrchestrator):
        self.orchestrator = orchestrator
        self.service_mesh_config = {
            "mesh_type": "istio",  # or "linkerd", "consul_connect"
            "mtls_enabled": True,
            "circuit_breaker_enabled": True,
            "retry_policy_enabled": True,
            "timeout_policy_enabled": True,
        }

    async def configure_canary_deployment(
        self,
        service_name: str,
        stable_version: str,
        canary_version: str,
        traffic_percentage: int = 10,
    ):
        """Configure canary deployment with gradual traffic shifting"""

        canary_config = {
            "destination_rule": {
                "host": service_name,
                "subsets": [
                    {"name": "stable", "labels": {"version": stable_version}},
                    {"name": "canary", "labels": {"version": canary_version}},
                ],
            },
            "virtual_service": {
                "hosts": [service_name],
                "http": [
                    {
                        "match": [{"headers": {"canary": {"exact": "true"}}}],
                        "route": [{"destination": {"host": service_name, "subset": "canary"}}],
                    },
                    {
                        "route": [
                            {
                                "destination": {"host": service_name, "subset": "stable"},
                                "weight": 100 - traffic_percentage,
                            },
                            {
                                "destination": {"host": service_name, "subset": "canary"},
                                "weight": traffic_percentage,
                            },
                        ]
                    },
                ],
            },
        }

        logger.info(
            f"Canary deployment configured for {service_name}: {traffic_percentage}% canary traffic"
        )
        return canary_config

    async def configure_circuit_breaker(
        self, service_name: str, failure_threshold: int = 5, timeout_seconds: int = 30
    ):
        """Configure circuit breaker for service resilience"""

        circuit_breaker_config = {
            "destination_rule": {
                "host": service_name,
                "traffic_policy": {
                    "outlier_detection": {
                        "consecutive_errors": failure_threshold,
                        "interval": f"{timeout_seconds}s",
                        "base_ejection_time": f"{timeout_seconds * 2}s",
                        "max_ejection_percent": 50,
                    },
                    "connection_pool": {
                        "tcp": {"max_connections": 100},
                        "http": {
                            "http1_max_pending_requests": 50,
                            "max_requests_per_connection": 10,
                        },
                    },
                },
            }
        }

        logger.info(f"Circuit breaker configured for {service_name}")
        return circuit_breaker_config

    async def configure_retry_policy(
        self, service_name: str, max_retries: int = 3, retry_timeout: str = "2s"
    ):
        """Configure intelligent retry policy"""

        retry_config = {
            "virtual_service": {
                "hosts": [service_name],
                "http": [
                    {
                        "route": [{"destination": {"host": service_name}}],
                        "retries": {
                            "attempts": max_retries,
                            "per_try_timeout": retry_timeout,
                            "retry_on": "gateway-error,connect-failure,refused-stream",
                        },
                    }
                ],
            }
        }

        logger.info(f"Retry policy configured for {service_name}")
        return retry_config
