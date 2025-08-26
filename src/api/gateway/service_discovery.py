"""
Service Discovery and Load Balancing Implementation
Enterprise-grade service discovery with health checking, load balancing, and circuit breaking
"""
from __future__ import annotations

import asyncio
import hashlib
import random
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import aiohttp

from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager, QueueType, MessagePriority
from streaming.kafka_manager import KafkaManager, StreamingTopic

logger = get_logger(__name__)


class ServiceStatus(Enum):
    """Service instance status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"
    UNKNOWN = "unknown"


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED_LEAST_CONNECTIONS = "weighted_least_connections"
    CONSISTENT_HASH = "consistent_hash"
    RANDOM = "random"
    WEIGHTED_RANDOM = "weighted_random"
    IP_HASH = "ip_hash"


@dataclass
class ServiceInstance:
    """Service instance representation."""
    id: str
    name: str
    host: str
    port: int
    protocol: str = "http"
    weight: int = 100
    status: ServiceStatus = ServiceStatus.UNKNOWN
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Health check info
    last_health_check: float = 0.0
    health_check_failures: int = 0
    health_check_successes: int = 0
    
    # Load balancing info
    current_connections: int = 0
    total_requests: int = 0
    avg_response_time: float = 0.0
    
    # Registration time
    registered_at: float = field(default_factory=time.time)
    
    @property
    def url(self) -> str:
        """Get the full URL for this service instance."""
        return f"{self.protocol}://{self.host}:{self.port}"
    
    @property
    def health_score(self) -> float:
        """Calculate health score (0.0 - 1.0) based on various metrics."""
        if self.status == ServiceStatus.UNHEALTHY:
            return 0.0
        if self.status == ServiceStatus.DRAINING:
            return 0.1
        
        total_checks = self.health_check_failures + self.health_check_successes
        if total_checks == 0:
            return 0.5  # Unknown health
        
        success_rate = self.health_check_successes / total_checks
        
        # Factor in response time (lower is better)
        response_time_score = max(0.0, 1.0 - (self.avg_response_time / 1000))  # Normalize to 1 second
        
        # Factor in connection count (lower relative load is better)
        connection_score = 1.0 if self.current_connections == 0 else max(0.1, 1.0 / self.current_connections)
        
        return (success_rate * 0.6 + response_time_score * 0.3 + connection_score * 0.1)


@dataclass
class HealthCheckConfig:
    """Health check configuration."""
    path: str = "/health"
    interval: int = 30  # seconds
    timeout: int = 5    # seconds
    failure_threshold: int = 3
    success_threshold: int = 2
    initial_delay: int = 10  # seconds


class ServiceRegistry:
    """Service registry for managing service instances."""
    
    def __init__(self):
        self.services: Dict[str, List[ServiceInstance]] = defaultdict(list)
        self.service_watchers: Dict[str, List[callable]] = defaultdict(list)
        self._lock = asyncio.Lock()
        
        # Messaging for distributed service registry
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
    
    async def register_service(self, instance: ServiceInstance) -> bool:
        """Register a service instance."""
        async with self._lock:
            service_instances = self.services[instance.name]
            
            # Check if instance already exists (update if so)
            existing_instance = None
            for i, existing in enumerate(service_instances):
                if existing.id == instance.id:
                    existing_instance = i
                    break
            
            if existing_instance is not None:
                service_instances[existing_instance] = instance
                action = "updated"
            else:
                service_instances.append(instance)
                action = "registered"
            
            logger.info(f"Service {instance.name} instance {instance.id} {action}: {instance.url}")
            
            # Publish service registration event
            await self._publish_service_event(action, instance)
            
            # Notify watchers
            await self._notify_watchers(instance.name)
            
            return True
    
    async def unregister_service(self, service_name: str, instance_id: str) -> bool:
        """Unregister a service instance."""
        async with self._lock:
            service_instances = self.services[service_name]
            
            for i, instance in enumerate(service_instances):
                if instance.id == instance_id:
                    removed_instance = service_instances.pop(i)
                    logger.info(f"Service {service_name} instance {instance_id} unregistered")
                    
                    # Publish service unregistration event
                    await self._publish_service_event("unregistered", removed_instance)
                    
                    # Notify watchers
                    await self._notify_watchers(service_name)
                    
                    return True
            
            return False
    
    async def get_healthy_instances(self, service_name: str) -> List[ServiceInstance]:
        """Get all healthy instances of a service."""
        async with self._lock:
            instances = self.services.get(service_name, [])
            return [instance for instance in instances if instance.status == ServiceStatus.HEALTHY]
    
    async def get_all_instances(self, service_name: str) -> List[ServiceInstance]:
        """Get all instances of a service regardless of health."""
        async with self._lock:
            return self.services.get(service_name, []).copy()
    
    async def update_instance_status(self, service_name: str, instance_id: str, status: ServiceStatus) -> bool:
        """Update the status of a service instance."""
        async with self._lock:
            service_instances = self.services.get(service_name, [])
            
            for instance in service_instances:
                if instance.id == instance_id:
                    old_status = instance.status
                    instance.status = status
                    
                    if old_status != status:
                        logger.info(f"Service {service_name} instance {instance_id} status changed: {old_status} -> {status}")
                        
                        # Publish status change event
                        await self._publish_service_event("status_changed", instance, 
                                                        metadata={"old_status": old_status.value, "new_status": status.value})
                        
                        # Notify watchers
                        await self._notify_watchers(service_name)
                    
                    return True
            
            return False
    
    async def watch_service(self, service_name: str, callback: callable):
        """Watch for changes to a service."""
        self.service_watchers[service_name].append(callback)
    
    async def _notify_watchers(self, service_name: str):
        """Notify watchers of service changes."""
        watchers = self.service_watchers.get(service_name, [])
        instances = await self.get_all_instances(service_name)
        
        for watcher in watchers:
            try:
                await watcher(service_name, instances)
            except Exception as e:
                logger.error(f"Error notifying service watcher: {e}")
    
    async def _publish_service_event(self, action: str, instance: ServiceInstance, metadata: Dict[str, Any] = None):
        """Publish service event to messaging systems."""
        try:
            event_data = {
                "action": action,
                "service_name": instance.name,
                "instance_id": instance.id,
                "instance_url": instance.url,
                "instance_status": instance.status.value,
                "timestamp": time.time(),
                "metadata": metadata or {}
            }
            
            # Publish to RabbitMQ for service coordination
            self.rabbitmq_manager.publish_task(
                task_name="service_registry_event",
                payload=event_data,
                queue=QueueType.EVENT_QUEUE,
                priority=MessagePriority.HIGH
            )
            
            # Publish to Kafka for monitoring and analytics
            self.kafka_manager.produce_message(
                topic=StreamingTopic.SYSTEM_EVENTS,
                message=event_data,
                key=f"{instance.name}:{instance.id}"
            )
            
        except Exception as e:
            logger.error(f"Error publishing service event: {e}")


class HealthChecker:
    """Health checker for service instances."""
    
    def __init__(self, service_registry: ServiceRegistry, config: HealthCheckConfig = None):
        self.service_registry = service_registry
        self.config = config or HealthCheckConfig()
        self.session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._health_check_tasks: Set[asyncio.Task] = set()
    
    async def start(self):
        """Start health checking."""
        if self._running:
            return
        
        self._running = True
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout)
        )
        
        # Start health check loop for each service
        for service_name in self.service_registry.services:
            task = asyncio.create_task(self._health_check_loop(service_name))
            self._health_check_tasks.add(task)
        
        logger.info("Health checker started")
    
    async def stop(self):
        """Stop health checking."""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel all health check tasks
        for task in self._health_check_tasks:
            task.cancel()
        
        await asyncio.gather(*self._health_check_tasks, return_exceptions=True)
        self._health_check_tasks.clear()
        
        if self.session:
            await self.session.close()
            self.session = None
        
        logger.info("Health checker stopped")
    
    async def _health_check_loop(self, service_name: str):
        """Health check loop for a specific service."""
        await asyncio.sleep(self.config.initial_delay)  # Initial delay
        
        while self._running:
            try:
                instances = await self.service_registry.get_all_instances(service_name)
                
                # Check health of each instance
                health_check_tasks = []
                for instance in instances:
                    task = asyncio.create_task(self._check_instance_health(instance))
                    health_check_tasks.append(task)
                
                if health_check_tasks:
                    await asyncio.gather(*health_check_tasks, return_exceptions=True)
                
                await asyncio.sleep(self.config.interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop for {service_name}: {e}")
                await asyncio.sleep(self.config.interval)
    
    async def _check_instance_health(self, instance: ServiceInstance):
        """Check health of a single instance."""
        if not self.session:
            return
        
        health_url = f"{instance.url}{self.config.path}"
        start_time = time.time()
        
        try:
            async with self.session.get(health_url) as response:
                response_time = (time.time() - start_time) * 1000  # Convert to ms
                
                if response.status == 200:
                    # Health check succeeded
                    instance.health_check_successes += 1
                    instance.last_health_check = time.time()
                    
                    # Update average response time
                    if instance.avg_response_time == 0:
                        instance.avg_response_time = response_time
                    else:
                        instance.avg_response_time = (instance.avg_response_time * 0.7 + response_time * 0.3)
                    
                    # Update status if needed
                    if instance.health_check_successes >= self.config.success_threshold:
                        if instance.status != ServiceStatus.HEALTHY:
                            await self.service_registry.update_instance_status(
                                instance.name, instance.id, ServiceStatus.HEALTHY
                            )
                        instance.health_check_failures = 0  # Reset failure count
                else:
                    # Health check failed
                    await self._handle_health_check_failure(instance)
                    
        except Exception as e:
            logger.warning(f"Health check failed for {instance.name}:{instance.id} - {e}")
            await self._handle_health_check_failure(instance)
    
    async def _handle_health_check_failure(self, instance: ServiceInstance):
        """Handle health check failure."""
        instance.health_check_failures += 1
        instance.last_health_check = time.time()
        
        if instance.health_check_failures >= self.config.failure_threshold:
            if instance.status == ServiceStatus.HEALTHY:
                await self.service_registry.update_instance_status(
                    instance.name, instance.id, ServiceStatus.UNHEALTHY
                )


class LoadBalancer(ABC):
    """Abstract load balancer."""
    
    @abstractmethod
    async def select_instance(self, instances: List[ServiceInstance], context: Dict[str, Any] = None) -> Optional[ServiceInstance]:
        """Select a service instance from available instances."""
        pass


class RoundRobinLoadBalancer(LoadBalancer):
    """Round robin load balancer."""
    
    def __init__(self):
        self.counters: Dict[str, int] = defaultdict(int)
    
    async def select_instance(self, instances: List[ServiceInstance], context: Dict[str, Any] = None) -> Optional[ServiceInstance]:
        if not instances:
            return None
        
        service_name = instances[0].name
        index = self.counters[service_name] % len(instances)
        self.counters[service_name] += 1
        
        return instances[index]


class WeightedRoundRobinLoadBalancer(LoadBalancer):
    """Weighted round robin load balancer."""
    
    def __init__(self):
        self.current_weights: Dict[str, Dict[str, int]] = defaultdict(dict)
    
    async def select_instance(self, instances: List[ServiceInstance], context: Dict[str, Any] = None) -> Optional[ServiceInstance]:
        if not instances:
            return None
        
        service_name = instances[0].name
        current_weights = self.current_weights[service_name]
        
        # Initialize current weights if not exists
        for instance in instances:
            if instance.id not in current_weights:
                current_weights[instance.id] = 0
        
        # Select instance with highest current weight
        total_weight = sum(instance.weight for instance in instances)
        best_instance = None
        best_weight = -1
        
        for instance in instances:
            current_weights[instance.id] += instance.weight
            
            if current_weights[instance.id] > best_weight:
                best_weight = current_weights[instance.id]
                best_instance = instance
        
        if best_instance:
            current_weights[best_instance.id] -= total_weight
        
        return best_instance


class LeastConnectionsLoadBalancer(LoadBalancer):
    """Least connections load balancer."""
    
    async def select_instance(self, instances: List[ServiceInstance], context: Dict[str, Any] = None) -> Optional[ServiceInstance]:
        if not instances:
            return None
        
        # Select instance with least connections
        return min(instances, key=lambda x: x.current_connections)


class ConsistentHashLoadBalancer(LoadBalancer):
    """Consistent hash load balancer."""
    
    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[str, Dict[int, ServiceInstance]] = defaultdict(dict)
    
    def _build_hash_ring(self, service_name: str, instances: List[ServiceInstance]):
        """Build hash ring for service instances."""
        ring = {}
        
        for instance in instances:
            for i in range(self.virtual_nodes):
                virtual_key = f"{instance.id}:{i}"
                hash_value = int(hashlib.md5(virtual_key.encode()).hexdigest(), 16)
                ring[hash_value] = instance
        
        self.hash_ring[service_name] = ring
    
    async def select_instance(self, instances: List[ServiceInstance], context: Dict[str, Any] = None) -> Optional[ServiceInstance]:
        if not instances:
            return None
        
        service_name = instances[0].name
        
        # Rebuild hash ring if instance list changed
        if service_name not in self.hash_ring or len(self.hash_ring[service_name]) != len(instances) * self.virtual_nodes:
            self._build_hash_ring(service_name, instances)
        
        # Get hash key from context (e.g., client IP, user ID)
        hash_key = context.get("hash_key", "default") if context else "default"
        hash_value = int(hashlib.md5(hash_key.encode()).hexdigest(), 16)
        
        ring = self.hash_ring[service_name]
        if not ring:
            return instances[0]  # Fallback
        
        # Find the first instance clockwise from hash value
        sorted_hashes = sorted(ring.keys())
        for ring_hash in sorted_hashes:
            if hash_value <= ring_hash:
                return ring[ring_hash]
        
        # Wrap around to the first instance
        return ring[sorted_hashes[0]]


class RandomLoadBalancer(LoadBalancer):
    """Random load balancer."""
    
    async def select_instance(self, instances: List[ServiceInstance], context: Dict[str, Any] = None) -> Optional[ServiceInstance]:
        if not instances:
            return None
        return random.choice(instances)


class ServiceDiscoveryClient:
    """Client for service discovery and load balancing."""
    
    def __init__(self, 
                 service_registry: ServiceRegistry,
                 load_balancer: LoadBalancer = None,
                 health_checker: HealthChecker = None):
        self.service_registry = service_registry
        self.load_balancer = load_balancer or RoundRobinLoadBalancer()
        self.health_checker = health_checker
        
        # Cache for service instances
        self.service_cache: Dict[str, List[ServiceInstance]] = {}
        self.cache_ttl = 30  # seconds
        self.last_cache_update: Dict[str, float] = {}
    
    async def start(self):
        """Start the service discovery client."""
        if self.health_checker:
            await self.health_checker.start()
        logger.info("Service discovery client started")
    
    async def stop(self):
        """Stop the service discovery client."""
        if self.health_checker:
            await self.health_checker.stop()
        logger.info("Service discovery client stopped")
    
    async def discover_service(self, service_name: str, use_cache: bool = True) -> List[ServiceInstance]:
        """Discover healthy instances of a service."""
        current_time = time.time()
        
        # Check cache first
        if (use_cache and 
            service_name in self.service_cache and 
            current_time - self.last_cache_update.get(service_name, 0) < self.cache_ttl):
            return self.service_cache[service_name]
        
        # Get healthy instances
        instances = await self.service_registry.get_healthy_instances(service_name)
        
        # Update cache
        self.service_cache[service_name] = instances
        self.last_cache_update[service_name] = current_time
        
        return instances
    
    async def get_service_instance(self, service_name: str, context: Dict[str, Any] = None) -> Optional[ServiceInstance]:
        """Get a service instance using load balancing."""
        instances = await self.discover_service(service_name)
        
        if not instances:
            logger.warning(f"No healthy instances found for service: {service_name}")
            return None
        
        # Use load balancer to select instance
        selected_instance = await self.load_balancer.select_instance(instances, context)
        
        if selected_instance:
            # Increment connection counter
            selected_instance.current_connections += 1
            selected_instance.total_requests += 1
        
        return selected_instance
    
    async def release_instance(self, instance: ServiceInstance):
        """Release a service instance after use."""
        if instance.current_connections > 0:
            instance.current_connections -= 1
    
    async def get_service_url(self, service_name: str, context: Dict[str, Any] = None) -> Optional[str]:
        """Get a service URL using load balancing."""
        instance = await self.get_service_instance(service_name, context)
        return instance.url if instance else None


# Factory functions

def create_load_balancer(strategy: LoadBalancingStrategy, **kwargs) -> LoadBalancer:
    """Create a load balancer instance based on strategy."""
    if strategy == LoadBalancingStrategy.ROUND_ROBIN:
        return RoundRobinLoadBalancer()
    elif strategy == LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN:
        return WeightedRoundRobinLoadBalancer()
    elif strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
        return LeastConnectionsLoadBalancer()
    elif strategy == LoadBalancingStrategy.CONSISTENT_HASH:
        virtual_nodes = kwargs.get('virtual_nodes', 150)
        return ConsistentHashLoadBalancer(virtual_nodes)
    elif strategy == LoadBalancingStrategy.RANDOM:
        return RandomLoadBalancer()
    else:
        raise ValueError(f"Unsupported load balancing strategy: {strategy}")


async def create_service_discovery_system(
    load_balancing_strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
    health_check_config: HealthCheckConfig = None,
    **kwargs
) -> ServiceDiscoveryClient:
    """Create a complete service discovery system."""
    service_registry = ServiceRegistry()
    load_balancer = create_load_balancer(load_balancing_strategy, **kwargs)
    health_checker = HealthChecker(service_registry, health_check_config)
    
    client = ServiceDiscoveryClient(service_registry, load_balancer, health_checker)
    await client.start()
    
    return client


# Global service discovery client
_default_service_discovery: Optional[ServiceDiscoveryClient] = None


async def get_service_discovery() -> ServiceDiscoveryClient:
    """Get default service discovery client."""
    global _default_service_discovery
    if _default_service_discovery is None:
        _default_service_discovery = await create_service_discovery_system()
    return _default_service_discovery


def set_service_discovery(client: ServiceDiscoveryClient):
    """Set default service discovery client."""
    global _default_service_discovery
    _default_service_discovery = client