"""
Bulkhead Isolation Middleware for Resource Segregation
Enterprise-grade bulkhead pattern implementation with resource pools and isolation
"""
import asyncio
import time
from collections.abc import Awaitable, Callable
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger

logger = get_logger(__name__)


class ResourceType(Enum):
    """Types of resources to isolate."""
    CPU_INTENSIVE = "cpu_intensive"
    IO_INTENSIVE = "io_intensive" 
    DATABASE = "database"
    EXTERNAL_API = "external_api"
    CACHE = "cache"
    ANALYTICS = "analytics"
    ADMIN = "admin"
    DEFAULT = "default"


@dataclass
class ResourcePool:
    """Resource pool configuration for bulkhead isolation."""
    name: str
    resource_type: ResourceType
    max_concurrent: int
    max_queue_size: int
    timeout: float = 30.0
    priority: int = 1
    current_active: int = field(default=0, init=False)
    queue: asyncio.Queue = field(default_factory=lambda: None, init=False)
    semaphore: asyncio.Semaphore = field(default_factory=lambda: None, init=False)
    
    def __post_init__(self):
        """Initialize async components after creation."""
        if self.queue is None:
            self.queue = asyncio.Queue(maxsize=self.max_queue_size)
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.max_concurrent)


@dataclass 
class BulkheadMetrics:
    """Metrics for bulkhead monitoring."""
    total_requests: int = 0
    successful_requests: int = 0
    rejected_requests: int = 0
    timeout_requests: int = 0
    current_active: int = 0
    queue_size: int = 0
    avg_response_time: float = 0.0
    last_request_time: float = 0.0
    
    def record_request(self, success: bool, response_time: float):
        """Record request metrics."""
        self.total_requests += 1
        self.last_request_time = time.time()
        
        if success:
            self.successful_requests += 1
        else:
            self.rejected_requests += 1
            
        # Update rolling average response time
        self.avg_response_time = (
            (self.avg_response_time * (self.total_requests - 1) + response_time) 
            / self.total_requests
        )


class BulkheadIsolation:
    """Bulkhead isolation manager for resource segregation."""
    
    def __init__(self):
        self.resource_pools: Dict[str, ResourcePool] = {}
        self.metrics: Dict[str, BulkheadMetrics] = defaultdict(BulkheadMetrics)
        self._setup_default_pools()
    
    def _setup_default_pools(self):
        """Setup default resource pools."""
        default_pools = [
            ResourcePool(
                name="database",
                resource_type=ResourceType.DATABASE,
                max_concurrent=10,
                max_queue_size=50,
                timeout=15.0,
                priority=2
            ),
            ResourcePool(
                name="external_api",
                resource_type=ResourceType.EXTERNAL_API,
                max_concurrent=5,
                max_queue_size=20,
                timeout=30.0,
                priority=1
            ),
            ResourcePool(
                name="analytics",
                resource_type=ResourceType.ANALYTICS,
                max_concurrent=3,
                max_queue_size=10,
                timeout=60.0,
                priority=1
            ),
            ResourcePool(
                name="cache", 
                resource_type=ResourceType.CACHE,
                max_concurrent=20,
                max_queue_size=100,
                timeout=5.0,
                priority=3
            ),
            ResourcePool(
                name="admin",
                resource_type=ResourceType.ADMIN,
                max_concurrent=2,
                max_queue_size=5,
                timeout=20.0,
                priority=3
            ),
            ResourcePool(
                name="default",
                resource_type=ResourceType.DEFAULT,
                max_concurrent=15,
                max_queue_size=30,
                timeout=20.0,
                priority=2
            )
        ]
        
        for pool in default_pools:
            self.resource_pools[pool.name] = pool
            logger.info(f"Initialized resource pool: {pool.name} "
                       f"(concurrent={pool.max_concurrent}, queue={pool.max_queue_size})")
    
    def add_resource_pool(self, pool: ResourcePool):
        """Add a custom resource pool."""
        self.resource_pools[pool.name] = pool
        logger.info(f"Added custom resource pool: {pool.name}")
    
    def get_pool_for_endpoint(self, method: str, path: str) -> ResourcePool:
        """Determine which resource pool to use for an endpoint."""
        # Define endpoint to pool mappings
        endpoint_mappings = {
            # Database operations
            "POST:/api/v1/sales": "database",
            "PUT:/api/v1/sales": "database", 
            "DELETE:/api/v1/sales": "database",
            "GET:/api/v1/datamart": "database",
            
            # External API calls
            "GET:/api/v1/search": "external_api",
            "POST:/api/v1/search": "external_api",
            
            # Analytics operations
            "GET:/api/v2/analytics": "analytics",
            "POST:/api/v2/analytics": "analytics",
            
            # Cache operations
            "GET:/api/v1/sales": "cache",
            "GET:/api/v1/health": "cache",
            
            # Admin operations
            "POST:/api/v1/batch": "admin",
            "GET:/api/v1/admin": "admin"
        }
        
        endpoint_key = f"{method}:{path}"
        
        # Check for exact match
        if endpoint_key in endpoint_mappings:
            pool_name = endpoint_mappings[endpoint_key]
            return self.resource_pools[pool_name]
        
        # Check for pattern matches
        for pattern, pool_name in endpoint_mappings.items():
            if self._matches_pattern(endpoint_key, pattern):
                return self.resource_pools[pool_name]
        
        # Return default pool
        return self.resource_pools["default"]
    
    def _matches_pattern(self, endpoint: str, pattern: str) -> bool:
        """Check if endpoint matches a pattern with wildcards."""
        import fnmatch
        return fnmatch.fnmatch(endpoint, pattern)
    
    async def execute_with_isolation(
        self, 
        pool: ResourcePool, 
        func: Callable[..., Awaitable[Any]], 
        request: Request,
        *args, 
        **kwargs
    ) -> Any:
        """Execute function with bulkhead isolation."""
        start_time = time.time()
        metrics = self.metrics[pool.name]
        
        try:
            # Check if queue is full
            if pool.queue.full():
                metrics.rejected_requests += 1
                raise HTTPException(
                    status_code=429,
                    detail=f"Resource pool '{pool.name}' queue is full. Try again later."
                )
            
            # Try to acquire semaphore with timeout
            try:
                await asyncio.wait_for(
                    pool.semaphore.acquire(), 
                    timeout=pool.timeout
                )
            except asyncio.TimeoutError:
                metrics.timeout_requests += 1
                raise HTTPException(
                    status_code=503,
                    detail=f"Resource pool '{pool.name}' timeout. Service temporarily unavailable."
                )
            
            try:
                pool.current_active += 1
                metrics.current_active = pool.current_active
                
                # Execute the function
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=pool.timeout
                )
                
                # Record successful execution
                response_time = time.time() - start_time
                metrics.record_request(True, response_time)
                
                return result
                
            finally:
                pool.current_active -= 1
                metrics.current_active = pool.current_active
                pool.semaphore.release()
                
        except HTTPException:
            response_time = time.time() - start_time
            metrics.record_request(False, response_time)
            raise
        except Exception as e:
            response_time = time.time() - start_time
            metrics.record_request(False, response_time)
            logger.error(f"Error in bulkhead execution for pool '{pool.name}': {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Internal error in resource pool '{pool.name}'"
            )
    
    async def get_pool_stats(self, pool_name: str) -> Dict[str, Any]:
        """Get statistics for a resource pool."""
        if pool_name not in self.resource_pools:
            return {}
        
        pool = self.resource_pools[pool_name]
        metrics = self.metrics[pool_name]
        
        return {
            "pool_name": pool_name,
            "resource_type": pool.resource_type.value,
            "max_concurrent": pool.max_concurrent,
            "max_queue_size": pool.max_queue_size,
            "timeout": pool.timeout,
            "priority": pool.priority,
            "current_active": pool.current_active,
            "queue_size": pool.queue.qsize(),
            "metrics": {
                "total_requests": metrics.total_requests,
                "successful_requests": metrics.successful_requests,
                "rejected_requests": metrics.rejected_requests,
                "timeout_requests": metrics.timeout_requests,
                "success_rate": (
                    metrics.successful_requests / metrics.total_requests 
                    if metrics.total_requests > 0 else 0
                ),
                "avg_response_time": metrics.avg_response_time,
                "last_request_time": metrics.last_request_time
            }
        }
    
    async def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all resource pools."""
        stats = {}
        for pool_name in self.resource_pools:
            stats[pool_name] = await self.get_pool_stats(pool_name)
        return stats
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all resource pools."""
        health_status = {}
        overall_healthy = True
        
        for pool_name, pool in self.resource_pools.items():
            metrics = self.metrics[pool_name]
            
            # Calculate health indicators
            utilization = pool.current_active / pool.max_concurrent
            queue_utilization = pool.queue.qsize() / pool.max_queue_size
            
            success_rate = (
                metrics.successful_requests / metrics.total_requests 
                if metrics.total_requests > 0 else 1.0
            )
            
            # Determine health status
            is_healthy = (
                utilization < 0.9 and 
                queue_utilization < 0.8 and 
                success_rate > 0.95
            )
            
            health_status[pool_name] = {
                "healthy": is_healthy,
                "utilization": utilization,
                "queue_utilization": queue_utilization,
                "success_rate": success_rate,
                "active_requests": pool.current_active,
                "queued_requests": pool.queue.qsize()
            }
            
            if not is_healthy:
                overall_healthy = False
        
        return {
            "overall_healthy": overall_healthy,
            "pools": health_status,
            "timestamp": time.time()
        }


class BulkheadMiddleware(BaseHTTPMiddleware):
    """Middleware for bulkhead isolation."""
    
    def __init__(
        self, 
        app, 
        bulkhead_manager: Optional[BulkheadIsolation] = None,
        custom_pools: Optional[Dict[str, ResourcePool]] = None
    ):
        super().__init__(app)
        self.bulkhead = bulkhead_manager or BulkheadIsolation()
        
        # Add custom pools if provided
        if custom_pools:
            for pool in custom_pools.values():
                self.bulkhead.add_resource_pool(pool)
    
    async def dispatch(self, request: Request, call_next) -> Response:
        """Apply bulkhead isolation to requests."""
        # Get appropriate resource pool for the endpoint
        pool = self.bulkhead.get_pool_for_endpoint(
            request.method, 
            request.url.path
        )
        
        # Execute request with bulkhead isolation
        try:
            response = await self.bulkhead.execute_with_isolation(
                pool, 
                call_next, 
                request
            )
            
            # Add bulkhead headers to response
            response.headers["X-Bulkhead-Pool"] = pool.name
            response.headers["X-Bulkhead-Active"] = str(pool.current_active)
            response.headers["X-Bulkhead-Max"] = str(pool.max_concurrent)
            
            return response
            
        except HTTPException as e:
            # Add bulkhead information to error response
            return Response(
                content=f'{{"detail": "{e.detail}", "bulkhead_pool": "{pool.name}"}}',
                status_code=e.status_code,
                media_type="application/json",
                headers={
                    "X-Bulkhead-Pool": pool.name,
                    "X-Bulkhead-Active": str(pool.current_active),
                    "X-Bulkhead-Max": str(pool.max_concurrent)
                }
            )


# Bulkhead decorator for specific functions
def bulkhead_isolated(
    pool_name: str = "default", 
    bulkhead_manager: Optional[BulkheadIsolation] = None
):
    """
    Decorator to apply bulkhead isolation to specific functions.
    
    Args:
        pool_name: Name of the resource pool to use
        bulkhead_manager: Bulkhead manager instance
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            manager = bulkhead_manager or BulkheadIsolation()
            pool = manager.resource_pools.get(pool_name, manager.resource_pools["default"])
            
            return await manager.execute_with_isolation(
                pool,
                func,
                None,  # No request object for direct function calls
                *args,
                **kwargs
            )
        return wrapper
    return decorator


# Global bulkhead manager instance
_default_bulkhead: Optional[BulkheadIsolation] = None


def get_default_bulkhead() -> BulkheadIsolation:
    """Get default bulkhead manager instance."""
    global _default_bulkhead
    if _default_bulkhead is None:
        _default_bulkhead = BulkheadIsolation()
    return _default_bulkhead


def set_default_bulkhead(bulkhead: BulkheadIsolation):
    """Set default bulkhead manager instance."""
    global _default_bulkhead
    _default_bulkhead = bulkhead