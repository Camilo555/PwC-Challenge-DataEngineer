"""
Intelligent Load Balancer with Health-Aware Routing

Advanced load balancing implementation with:
- Health-aware routing based on real-time metrics
- Multiple load balancing algorithms (round-robin, least-connections, weighted)
- Circuit breakers for failed services
- Service discovery and registration
- Automatic failover and recovery
- Performance-based routing decisions
"""

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Callable, Union
from urllib.parse import urlparse
import hashlib
import random

import aiohttp
from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger

logger = get_logger(__name__)


class LoadBalancingAlgorithm(Enum):
    """Load balancing algorithms."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_RESPONSE_TIME = "least_response_time"
    HASH_BASED = "hash_based"
    GEOGRAPHIC = "geographic"
    PERFORMANCE_BASED = "performance_based"


class ServiceState(Enum):
    """Service health states."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECOVERING = "recovering"
    MAINTENANCE = "maintenance"


@dataclass
class ServiceMetrics:
    """Performance metrics for a service instance."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    active_connections: int = 0
    avg_response_time: float = 0.0
    p95_response_time: float = 0.0
    p99_response_time: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    error_rate: float = 0.0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    # Rolling windows for metrics
    response_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def record_request(self, response_time: float, success: bool = True):
        """Record a request with its metrics."""
        self.total_requests += 1
        self.response_times.append(response_time)
        self.last_updated = datetime.utcnow()
        
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            self.recent_errors.append(datetime.utcnow())
        
        # Update averages
        if self.response_times:
            self.avg_response_time = sum(self.response_times) / len(self.response_times)
            sorted_times = sorted(self.response_times)
            if len(sorted_times) >= 20:  # Need sufficient data for percentiles
                self.p95_response_time = sorted_times[int(len(sorted_times) * 0.95)]
                self.p99_response_time = sorted_times[int(len(sorted_times) * 0.99)]
        
        # Update error rate (last 100 requests)
        recent_total = min(self.total_requests, 100)
        recent_errors = len([e for e in self.recent_errors 
                           if datetime.utcnow() - e < timedelta(minutes=5)])
        self.error_rate = recent_errors / recent_total if recent_total > 0 else 0.0
    
    def get_health_score(self) -> float:
        """Calculate health score based on multiple metrics (0-1, 1 is best)."""
        score = 1.0
        
        # Error rate penalty
        score *= (1.0 - min(self.error_rate, 0.5))  # Cap at 50% penalty
        
        # Response time penalty (assume 100ms is baseline)
        if self.avg_response_time > 0:
            response_penalty = min(self.avg_response_time / 100.0, 2.0)  # Cap at 2x penalty
            score *= (1.0 / response_penalty)
        
        # CPU usage penalty
        if self.cpu_usage > 0:
            cpu_penalty = 1.0 - (self.cpu_usage / 100.0) * 0.3  # Max 30% penalty for high CPU
            score *= max(cpu_penalty, 0.7)
        
        # Memory usage penalty
        if self.memory_usage > 0:
            mem_penalty = 1.0 - (self.memory_usage / 100.0) * 0.2  # Max 20% penalty for high memory
            score *= max(mem_penalty, 0.8)
        
        return max(score, 0.1)  # Minimum score of 0.1


@dataclass
class ServiceInstance:
    """Represents a service instance in the load balancer."""
    id: str
    host: str
    port: int
    weight: float = 1.0
    state: ServiceState = ServiceState.HEALTHY
    metrics: ServiceMetrics = field(default_factory=ServiceMetrics)
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_health_check: Optional[datetime] = None
    consecutive_failures: int = 0
    circuit_breaker_until: Optional[datetime] = None
    
    @property
    def url(self) -> str:
        """Get full URL for this service instance."""
        return f"http://{self.host}:{self.port}"
    
    @property
    def is_available(self) -> bool:
        """Check if service is available for routing."""
        if self.state in [ServiceState.FAILED, ServiceState.MAINTENANCE]:
            return False
        
        if self.circuit_breaker_until and datetime.utcnow() < self.circuit_breaker_until:
            return False
        
        return True
    
    def record_success(self, response_time: float):
        """Record successful request."""
        self.metrics.record_request(response_time, success=True)
        self.consecutive_failures = 0
        
        # Update state based on metrics
        if self.state == ServiceState.RECOVERING and self.metrics.error_rate < 0.05:
            self.state = ServiceState.HEALTHY
        elif self.state == ServiceState.DEGRADED and self.metrics.error_rate < 0.02:
            self.state = ServiceState.HEALTHY
    
    def record_failure(self):
        """Record failed request."""
        self.metrics.record_request(0, success=False)
        self.consecutive_failures += 1
        
        # Update state based on failures
        if self.consecutive_failures >= 5:
            self.state = ServiceState.FAILED
            # Circuit breaker for 30 seconds
            self.circuit_breaker_until = datetime.utcnow() + timedelta(seconds=30)
        elif self.consecutive_failures >= 3:
            self.state = ServiceState.DEGRADED
        elif self.metrics.error_rate > 0.1:
            self.state = ServiceState.DEGRADED


class HealthChecker:
    """Health checker for service instances."""
    
    def __init__(self, check_interval: int = 30, timeout: float = 5.0):
        self.check_interval = check_interval
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self, services: Dict[str, ServiceInstance]):
        """Start health checking."""
        self._running = True
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        self._task = asyncio.create_task(self._health_check_loop(services))
    
    async def stop(self):
        """Stop health checking."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        if self.session:
            await self.session.close()
    
    async def _health_check_loop(self, services: Dict[str, ServiceInstance]):
        """Main health check loop."""
        while self._running:
            try:
                await asyncio.sleep(self.check_interval)
                await self._check_all_services(services)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
    
    async def _check_all_services(self, services: Dict[str, ServiceInstance]):
        """Check health of all services."""
        tasks = []
        for service in services.values():
            if service.state != ServiceState.MAINTENANCE:
                task = asyncio.create_task(self._check_service_health(service))
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_service_health(self, service: ServiceInstance):
        """Check health of a single service."""
        try:
            health_url = f"{service.url}/health"
            start_time = time.time()
            
            async with self.session.get(health_url) as response:
                response_time = time.time() - start_time
                
                if response.status == 200:
                    # Parse health response for additional metrics
                    try:
                        health_data = await response.json()
                        self._update_service_metrics(service, health_data)
                    except Exception:
                        pass  # Ignore JSON parsing errors
                    
                    service.record_success(response_time)
                    service.last_health_check = datetime.utcnow()
                    
                    # Check if service is recovering
                    if service.state == ServiceState.FAILED:
                        service.state = ServiceState.RECOVERING
                        service.circuit_breaker_until = None
                        logger.info(f"Service {service.id} is recovering")
                    
                else:
                    service.record_failure()
                    logger.warning(f"Service {service.id} health check failed: {response.status}")
        
        except Exception as e:
            service.record_failure()
            logger.warning(f"Service {service.id} health check error: {e}")
    
    def _update_service_metrics(self, service: ServiceInstance, health_data: Dict[str, Any]):
        """Update service metrics from health endpoint."""
        try:
            if "metrics" in health_data:
                metrics = health_data["metrics"]
                service.metrics.cpu_usage = metrics.get("cpu_usage", 0)
                service.metrics.memory_usage = metrics.get("memory_usage", 0)
                service.metrics.active_connections = metrics.get("active_connections", 0)
        except Exception as e:
            logger.debug(f"Error updating service metrics: {e}")


class IntelligentLoadBalancer:
    """Intelligent load balancer with health-aware routing."""
    
    def __init__(self, 
                 algorithm: LoadBalancingAlgorithm = LoadBalancingAlgorithm.PERFORMANCE_BASED,
                 health_check_interval: int = 30,
                 enable_circuit_breaker: bool = True,
                 enable_metrics_collection: bool = True):
        
        self.algorithm = algorithm
        self.enable_circuit_breaker = enable_circuit_breaker
        self.enable_metrics_collection = enable_metrics_collection
        
        # Service registry
        self.services: Dict[str, ServiceInstance] = {}
        self.service_groups: Dict[str, List[str]] = defaultdict(list)
        
        # Algorithm state
        self._round_robin_index = 0
        self._request_counts: Dict[str, int] = defaultdict(int)
        
        # Health checker
        self.health_checker = HealthChecker(health_check_interval)
        
        # Global metrics
        self.global_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "avg_response_time": 0.0,
            "requests_per_service": defaultdict(int)
        }
        
        # HTTP session for proxying
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def start(self):
        """Start the load balancer."""
        self.session = aiohttp.ClientSession()
        await self.health_checker.start(self.services)
        logger.info(f"Intelligent load balancer started with {self.algorithm.value} algorithm")
    
    async def stop(self):
        """Stop the load balancer."""
        await self.health_checker.stop()
        if self.session:
            await self.session.close()
        logger.info("Intelligent load balancer stopped")
    
    def register_service(self, 
                        service_id: str,
                        host: str,
                        port: int,
                        weight: float = 1.0,
                        tags: Optional[Dict[str, str]] = None,
                        group: Optional[str] = None) -> ServiceInstance:
        """Register a new service instance."""
        
        service = ServiceInstance(
            id=service_id,
            host=host,
            port=port,
            weight=weight,
            tags=tags or {},
            state=ServiceState.HEALTHY
        )
        
        self.services[service_id] = service
        
        if group:
            self.service_groups[group].append(service_id)
        
        logger.info(f"Registered service {service_id} at {host}:{port} (weight: {weight})")
        return service
    
    def unregister_service(self, service_id: str):
        """Unregister a service instance."""
        if service_id in self.services:
            service = self.services[service_id]
            del self.services[service_id]
            
            # Remove from groups
            for group_services in self.service_groups.values():
                if service_id in group_services:
                    group_services.remove(service_id)
            
            logger.info(f"Unregistered service {service_id}")
    
    def set_service_maintenance(self, service_id: str, maintenance: bool = True):
        """Set service maintenance mode."""
        if service_id in self.services:
            self.services[service_id].state = (
                ServiceState.MAINTENANCE if maintenance else ServiceState.HEALTHY
            )
            logger.info(f"Service {service_id} maintenance mode: {maintenance}")
    
    async def route_request(self, 
                          request: Request,
                          service_group: Optional[str] = None,
                          sticky_session: bool = False) -> Optional[ServiceInstance]:
        """Route request to optimal service instance."""
        
        # Get available services
        available_services = self._get_available_services(service_group)
        
        if not available_services:
            logger.error(f"No available services for group: {service_group}")
            return None
        
        # Select service based on algorithm
        if sticky_session:
            selected_service = self._select_sticky_service(request, available_services)
        else:
            selected_service = self._select_service_by_algorithm(available_services, request)
        
        return selected_service
    
    def _get_available_services(self, service_group: Optional[str] = None) -> List[ServiceInstance]:
        """Get list of available service instances."""
        if service_group and service_group in self.service_groups:
            service_ids = self.service_groups[service_group]
            services = [self.services[sid] for sid in service_ids if sid in self.services]
        else:
            services = list(self.services.values())
        
        return [s for s in services if s.is_available]
    
    def _select_service_by_algorithm(self, 
                                   available_services: List[ServiceInstance],
                                   request: Request) -> ServiceInstance:
        """Select service based on configured algorithm."""
        
        if self.algorithm == LoadBalancingAlgorithm.ROUND_ROBIN:
            return self._round_robin_selection(available_services)
        
        elif self.algorithm == LoadBalancingAlgorithm.LEAST_CONNECTIONS:
            return self._least_connections_selection(available_services)
        
        elif self.algorithm == LoadBalancingAlgorithm.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin_selection(available_services)
        
        elif self.algorithm == LoadBalancingAlgorithm.LEAST_RESPONSE_TIME:
            return self._least_response_time_selection(available_services)
        
        elif self.algorithm == LoadBalancingAlgorithm.HASH_BASED:
            return self._hash_based_selection(available_services, request)
        
        elif self.algorithm == LoadBalancingAlgorithm.PERFORMANCE_BASED:
            return self._performance_based_selection(available_services)
        
        else:
            # Default to round robin
            return self._round_robin_selection(available_services)
    
    def _round_robin_selection(self, services: List[ServiceInstance]) -> ServiceInstance:
        """Round-robin service selection."""
        service = services[self._round_robin_index % len(services)]
        self._round_robin_index += 1
        return service
    
    def _least_connections_selection(self, services: List[ServiceInstance]) -> ServiceInstance:
        """Select service with least active connections."""
        return min(services, key=lambda s: s.metrics.active_connections)
    
    def _weighted_round_robin_selection(self, services: List[ServiceInstance]) -> ServiceInstance:
        """Weighted round-robin selection."""
        # Build weighted list
        weighted_services = []
        for service in services:
            weight = max(int(service.weight * service.metrics.get_health_score()), 1)
            weighted_services.extend([service] * weight)
        
        if weighted_services:
            service = weighted_services[self._round_robin_index % len(weighted_services)]
            self._round_robin_index += 1
            return service
        
        return services[0]
    
    def _least_response_time_selection(self, services: List[ServiceInstance]) -> ServiceInstance:
        """Select service with lowest average response time."""
        return min(services, key=lambda s: s.metrics.avg_response_time or float('inf'))
    
    def _hash_based_selection(self, services: List[ServiceInstance], request: Request) -> ServiceInstance:
        """Hash-based selection for session affinity."""
        # Use client IP or session ID for consistent hashing
        client_ip = request.client.host if request.client else "unknown"
        session_id = request.headers.get("x-session-id", client_ip)
        
        hash_value = int(hashlib.md5(session_id.encode()).hexdigest(), 16)
        return services[hash_value % len(services)]
    
    def _performance_based_selection(self, services: List[ServiceInstance]) -> ServiceInstance:
        """Select service based on comprehensive performance metrics."""
        # Calculate scores for each service
        service_scores = []
        
        for service in services:
            health_score = service.metrics.get_health_score()
            
            # Adjust score based on current load
            load_factor = 1.0
            if service.metrics.active_connections > 0:
                # Penalize high connection count
                load_factor = 1.0 / (1.0 + service.metrics.active_connections * 0.1)
            
            # Weight by service weight
            final_score = health_score * load_factor * service.weight
            
            service_scores.append((service, final_score))
        
        # Select service with highest score
        return max(service_scores, key=lambda x: x[1])[0]
    
    def _select_sticky_service(self, request: Request, services: List[ServiceInstance]) -> ServiceInstance:
        """Select service for sticky sessions."""
        # Try to use existing session affinity
        session_cookie = request.cookies.get("lb_session")
        if session_cookie:
            for service in services:
                if service.id == session_cookie:
                    return service
        
        # Fall back to hash-based selection
        return self._hash_based_selection(services, request)
    
    async def proxy_request(self, 
                          request: Request,
                          target_service: ServiceInstance,
                          timeout: float = 30.0) -> Response:
        """Proxy request to target service."""
        
        start_time = time.time()
        
        try:
            # Build target URL
            target_url = f"{target_service.url}{request.url.path}"
            if request.url.query:
                target_url += f"?{request.url.query}"
            
            # Prepare headers (exclude hop-by-hop headers)
            headers = dict(request.headers)
            headers.pop("host", None)
            headers.pop("connection", None)
            headers["x-forwarded-for"] = request.client.host if request.client else "unknown"
            headers["x-forwarded-proto"] = request.url.scheme
            
            # Get request body
            body = None
            if request.method in ["POST", "PUT", "PATCH"]:
                body = await request.body()
            
            # Track active connection
            target_service.metrics.active_connections += 1
            
            try:
                # Make request to target service
                async with self.session.request(
                    method=request.method,
                    url=target_url,
                    headers=headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    
                    # Read response
                    response_body = await response.read()
                    response_time = time.time() - start_time
                    
                    # Record success
                    target_service.record_success(response_time)
                    self._record_global_metrics(True, response_time, target_service.id)
                    
                    # Build response
                    response_headers = dict(response.headers)
                    response_headers["x-served-by"] = target_service.id
                    response_headers["x-response-time"] = f"{response_time*1000:.2f}ms"
                    
                    return Response(
                        content=response_body,
                        status_code=response.status,
                        headers=response_headers
                    )
            
            finally:
                target_service.metrics.active_connections -= 1
        
        except Exception as e:
            response_time = time.time() - start_time
            target_service.record_failure()
            self._record_global_metrics(False, response_time, target_service.id)
            
            logger.error(f"Proxy request failed for service {target_service.id}: {e}")
            raise HTTPException(status_code=502, detail=f"Bad Gateway: {str(e)}")
    
    def _record_global_metrics(self, success: bool, response_time: float, service_id: str):
        """Record global load balancer metrics."""
        self.global_metrics["total_requests"] += 1
        self.global_metrics["requests_per_service"][service_id] += 1
        
        if success:
            self.global_metrics["successful_requests"] += 1
        else:
            self.global_metrics["failed_requests"] += 1
        
        # Update average response time
        total_requests = self.global_metrics["total_requests"]
        current_avg = self.global_metrics["avg_response_time"]
        self.global_metrics["avg_response_time"] = (
            (current_avg * (total_requests - 1) + response_time) / total_requests
        )
    
    def get_service_statistics(self) -> Dict[str, Any]:
        """Get comprehensive service statistics."""
        stats = {
            "algorithm": self.algorithm.value,
            "total_services": len(self.services),
            "available_services": len([s for s in self.services.values() if s.is_available]),
            "global_metrics": self.global_metrics,
            "services": {}
        }
        
        for service_id, service in self.services.items():
            stats["services"][service_id] = {
                "id": service.id,
                "url": service.url,
                "state": service.state.value,
                "weight": service.weight,
                "health_score": service.metrics.get_health_score(),
                "metrics": {
                    "total_requests": service.metrics.total_requests,
                    "successful_requests": service.metrics.successful_requests,
                    "failed_requests": service.metrics.failed_requests,
                    "error_rate": service.metrics.error_rate,
                    "avg_response_time": service.metrics.avg_response_time,
                    "active_connections": service.metrics.active_connections,
                    "cpu_usage": service.metrics.cpu_usage,
                    "memory_usage": service.metrics.memory_usage
                },
                "last_health_check": (
                    service.last_health_check.isoformat() 
                    if service.last_health_check else None
                ),
                "consecutive_failures": service.consecutive_failures
            }
        
        return stats


class LoadBalancerMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for intelligent load balancing."""
    
    def __init__(self, app, load_balancer: IntelligentLoadBalancer):
        super().__init__(app)
        self.load_balancer = load_balancer
    
    async def dispatch(self, request: Request, call_next):
        """Route requests through load balancer."""
        
        # Check if this is a load balancer management request
        if request.url.path.startswith("/lb/"):
            return await call_next(request)
        
        # Route through load balancer
        service = await self.load_balancer.route_request(request)
        
        if not service:
            raise HTTPException(status_code=503, detail="No available services")
        
        return await self.load_balancer.proxy_request(request, service)


# Factory function
def create_intelligent_load_balancer(
    algorithm: LoadBalancingAlgorithm = LoadBalancingAlgorithm.PERFORMANCE_BASED,
    health_check_interval: int = 30
) -> IntelligentLoadBalancer:
    """Create an intelligent load balancer instance."""
    return IntelligentLoadBalancer(
        algorithm=algorithm,
        health_check_interval=health_check_interval,
        enable_circuit_breaker=True,
        enable_metrics_collection=True
    )