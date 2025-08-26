"""
Advanced Service Registry for API Gateway
Implements service discovery, health checks, and load balancing
"""
import asyncio
import json
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
from fastapi import HTTPException

from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager
from streaming.kafka_manager import KafkaManager, StreamingTopic

logger = get_logger(__name__)


class ServiceStatus(Enum):
    """Service health status"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
    MAINTENANCE = "maintenance"


class LoadBalancingStrategy(Enum):
    """Load balancing strategies"""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    CONSISTENT_HASH = "consistent_hash"
    RANDOM = "random"


@dataclass
class ServiceInstance:
    """Service instance registration"""
    service_name: str
    instance_id: str
    host: str
    port: int
    protocol: str = "http"
    weight: int = 1
    health_check_url: str = "/health"
    metadata: Dict[str, Any] = None
    version: str = "1.0.0"
    tags: List[str] = None
    status: ServiceStatus = ServiceStatus.UNKNOWN
    last_health_check: Optional[float] = None
    response_time_ms: Optional[float] = None
    error_count: int = 0
    connection_count: int = 0

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.tags is None:
            self.tags = []
        if not self.instance_id:
            self.instance_id = f"{self.service_name}-{self.host}-{self.port}"

    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

    @property
    def health_url(self) -> str:
        return f"{self.base_url}{self.health_check_url}"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ServiceRegistry:
    """Advanced service registry with health monitoring and load balancing"""

    def __init__(self, health_check_interval: int = 30):
        self.services: Dict[str, List[ServiceInstance]] = defaultdict(list)
        self.service_weights: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.round_robin_counters: Dict[str, int] = defaultdict(int)
        self.health_check_interval = health_check_interval
        self.health_check_task: Optional[asyncio.Task] = None
        self.http_client = httpx.AsyncClient(timeout=5.0)
        
        # Messaging for distributed coordination
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
        
        # Start health checking
        self._start_health_checking()

    def _start_health_checking(self):
        """Start background health checking task"""
        if self.health_check_task is None or self.health_check_task.done():
            self.health_check_task = asyncio.create_task(self._health_check_loop())

    async def _health_check_loop(self):
        """Background health checking loop"""
        while True:
            try:
                await self._check_all_services_health()
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(5)

    async def register_service(self, service: ServiceInstance) -> bool:
        """Register a service instance"""
        try:
            # Add to local registry
            service_list = self.services[service.service_name]
            
            # Remove existing instance if it exists
            service_list[:] = [s for s in service_list if s.instance_id != service.instance_id]
            service_list.append(service)
            
            # Initialize weight
            self.service_weights[service.service_name][service.instance_id] = service.weight
            
            # Perform initial health check
            await self._check_service_health(service)
            
            # Publish service registration event
            await self._publish_service_event("registered", service)
            
            logger.info(f"Registered service: {service.service_name} ({service.instance_id})")
            return True
            
        except Exception as e:
            logger.error(f"Service registration failed: {e}")
            return False

    async def deregister_service(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance"""
        try:
            service_list = self.services[service_name]
            original_length = len(service_list)
            
            # Remove the service instance
            service_list[:] = [s for s in service_list if s.instance_id != instance_id]
            
            if len(service_list) < original_length:
                # Remove from weights
                self.service_weights[service_name].pop(instance_id, None)
                
                # Publish deregistration event
                await self._publish_service_event("deregistered", ServiceInstance(
                    service_name=service_name,
                    instance_id=instance_id,
                    host="unknown",
                    port=0
                ))
                
                logger.info(f"Deregistered service: {service_name} ({instance_id})")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Service deregistration failed: {e}")
            return False

    async def get_healthy_instances(self, service_name: str) -> List[ServiceInstance]:
        """Get all healthy instances for a service"""
        instances = self.services.get(service_name, [])
        return [instance for instance in instances if instance.status == ServiceStatus.HEALTHY]

    async def get_service_instance(
        self, 
        service_name: str, 
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
        client_id: Optional[str] = None
    ) -> Optional[ServiceInstance]:
        """Get a service instance using specified load balancing strategy"""
        
        healthy_instances = await self.get_healthy_instances(service_name)
        
        if not healthy_instances:
            logger.warning(f"No healthy instances available for service: {service_name}")
            return None

        if strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin_select(service_name, healthy_instances)
        elif strategy == LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin_select(service_name, healthy_instances)
        elif strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections_select(healthy_instances)
        elif strategy == LoadBalancingStrategy.CONSISTENT_HASH:
            return self._consistent_hash_select(healthy_instances, client_id or "default")
        elif strategy == LoadBalancingStrategy.RANDOM:
            return self._random_select(healthy_instances)
        else:
            return healthy_instances[0]

    def _round_robin_select(self, service_name: str, instances: List[ServiceInstance]) -> ServiceInstance:
        """Round robin load balancing"""
        counter = self.round_robin_counters[service_name]
        selected = instances[counter % len(instances)]
        self.round_robin_counters[service_name] = counter + 1
        return selected

    def _weighted_round_robin_select(self, service_name: str, instances: List[ServiceInstance]) -> ServiceInstance:
        """Weighted round robin load balancing"""
        weights = self.service_weights[service_name]
        total_weight = sum(weights.get(instance.instance_id, 1) for instance in instances)
        
        counter = self.round_robin_counters[service_name] % total_weight
        current_weight = 0
        
        for instance in instances:
            instance_weight = weights.get(instance.instance_id, 1)
            current_weight += instance_weight
            
            if counter < current_weight:
                self.round_robin_counters[service_name] += 1
                return instance
        
        return instances[0]

    def _least_connections_select(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Least connections load balancing"""
        return min(instances, key=lambda x: x.connection_count)

    def _consistent_hash_select(self, instances: List[ServiceInstance], client_id: str) -> ServiceInstance:
        """Consistent hash load balancing"""
        import hashlib
        hash_value = int(hashlib.md5(client_id.encode()).hexdigest(), 16)
        return instances[hash_value % len(instances)]

    def _random_select(self, instances: List[ServiceInstance]) -> ServiceInstance:
        """Random load balancing"""
        import random
        return random.choice(instances)

    async def _check_all_services_health(self):
        """Check health of all registered services"""
        tasks = []
        for service_list in self.services.values():
            for service in service_list:
                tasks.append(self._check_service_health(service))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_service_health(self, service: ServiceInstance) -> bool:
        """Check health of a specific service instance"""
        start_time = time.time()
        
        try:
            response = await self.http_client.get(service.health_url)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                old_status = service.status
                service.status = ServiceStatus.HEALTHY
                service.response_time_ms = response_time
                service.last_health_check = time.time()
                service.error_count = 0
                
                if old_status != ServiceStatus.HEALTHY:
                    await self._publish_service_event("health_recovered", service)
                
                return True
            else:
                await self._mark_service_unhealthy(service, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            await self._mark_service_unhealthy(service, str(e))
            return False

    async def _mark_service_unhealthy(self, service: ServiceInstance, reason: str):
        """Mark a service as unhealthy"""
        old_status = service.status
        service.status = ServiceStatus.UNHEALTHY
        service.error_count += 1
        service.last_health_check = time.time()
        
        if old_status != ServiceStatus.UNHEALTHY:
            await self._publish_service_event("health_failed", service, {"reason": reason})
            logger.warning(f"Service unhealthy: {service.service_name} ({service.instance_id}) - {reason}")

    async def _publish_service_event(self, event_type: str, service: ServiceInstance, extra_data: Dict = None):
        """Publish service events to Kafka"""
        try:
            event_data = {
                "event_type": event_type,
                "service": service.to_dict(),
                "timestamp": time.time()
            }
            
            if extra_data:
                event_data.update(extra_data)
            
            self.kafka_manager.produce_service_discovery_event(
                service_name=service.service_name,
                instance_id=service.instance_id,
                event_type=event_type,
                event_data=event_data
            )
            
        except Exception as e:
            logger.error(f"Failed to publish service event: {e}")

    async def get_service_metrics(self, service_name: Optional[str] = None) -> Dict[str, Any]:
        """Get service registry metrics"""
        if service_name:
            instances = self.services.get(service_name, [])
            healthy_count = sum(1 for i in instances if i.status == ServiceStatus.HEALTHY)
            
            return {
                "service_name": service_name,
                "total_instances": len(instances),
                "healthy_instances": healthy_count,
                "unhealthy_instances": len(instances) - healthy_count,
                "instances": [i.to_dict() for i in instances]
            }
        else:
            total_services = len(self.services)
            total_instances = sum(len(instances) for instances in self.services.values())
            healthy_instances = sum(
                1 for instances in self.services.values() 
                for instance in instances 
                if instance.status == ServiceStatus.HEALTHY
            )
            
            return {
                "total_services": total_services,
                "total_instances": total_instances,
                "healthy_instances": healthy_instances,
                "unhealthy_instances": total_instances - healthy_instances,
                "services": {
                    name: {
                        "total_instances": len(instances),
                        "healthy_instances": sum(1 for i in instances if i.status == ServiceStatus.HEALTHY)
                    }
                    for name, instances in self.services.items()
                }
            }

    async def update_connection_count(self, service_name: str, instance_id: str, delta: int):
        """Update connection count for load balancing"""
        for instance in self.services.get(service_name, []):
            if instance.instance_id == instance_id:
                instance.connection_count = max(0, instance.connection_count + delta)
                break

    async def close(self):
        """Clean up resources"""
        if self.health_check_task and not self.health_check_task.done():
            self.health_check_task.cancel()
        
        await self.http_client.aclose()


# Singleton instance for global use
_service_registry = None

def get_service_registry() -> ServiceRegistry:
    """Get global service registry instance"""
    global _service_registry
    if _service_registry is None:
        _service_registry = ServiceRegistry()
    return _service_registry


class ServiceDiscoveryMiddleware:
    """Middleware for automatic service discovery integration"""
    
    def __init__(self, service_registry: ServiceRegistry):
        self.service_registry = service_registry
        
    async def __call__(self, request, call_next):
        # Extract service name from request path
        path_parts = request.url.path.strip('/').split('/')
        if len(path_parts) >= 3 and path_parts[0] == 'api':
            service_name = f"{path_parts[1]}-service"  # e.g., "v1-service"
            
            # Get service instance
            instance = await self.service_registry.get_service_instance(
                service_name,
                strategy=LoadBalancingStrategy.ROUND_ROBIN,
                client_id=request.client.host
            )
            
            if instance:
                # Update connection count
                await self.service_registry.update_connection_count(
                    service_name, instance.instance_id, 1
                )
                
                # Add service info to request state
                request.state.service_instance = instance
                
                try:
                    response = await call_next(request)
                    return response
                finally:
                    # Decrease connection count
                    await self.service_registry.update_connection_count(
                        service_name, instance.instance_id, -1
                    )
            else:
                raise HTTPException(
                    status_code=503,
                    detail=f"No healthy instances available for service: {service_name}"
                )
        
        return await call_next(request)