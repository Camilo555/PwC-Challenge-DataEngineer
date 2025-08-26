"""
Advanced API Gateway with Service Mesh Integration
Implements request routing, load balancing, and service mesh patterns
"""
import asyncio
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from api.gateway.service_registry import ServiceRegistry, LoadBalancingStrategy, get_service_registry
from api.middleware.circuit_breaker import CircuitBreaker
from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager
from streaming.kafka_manager import KafkaManager

logger = get_logger(__name__)


class RoutingStrategy(Enum):
    """Request routing strategies"""
    PATH_BASED = "path_based"
    HEADER_BASED = "header_based"
    WEIGHT_BASED = "weight_based"
    CANARY = "canary"
    BLUE_GREEN = "blue_green"


class RetryStrategy(Enum):
    """Retry strategies for failed requests"""
    NONE = "none"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_INTERVAL = "fixed_interval"


@dataclass
class RouteConfig:
    """Route configuration for API Gateway"""
    path_pattern: str
    service_name: str
    rewrite_path: Optional[str] = None
    strip_prefix: bool = False
    timeout_seconds: int = 30
    retry_attempts: int = 3
    retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    load_balancing: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    circuit_breaker: bool = True
    rate_limit: Optional[Dict[str, Any]] = None
    auth_required: bool = True
    headers_to_add: Optional[Dict[str, str]] = None
    headers_to_remove: Optional[List[str]] = None
    canary_weight: int = 0  # 0-100 for canary deployments
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.headers_to_add is None:
            self.headers_to_add = {}
        if self.headers_to_remove is None:
            self.headers_to_remove = []
        if self.metadata is None:
            self.metadata = {}


class ServiceMeshProxy:
    """Service mesh proxy for advanced traffic management"""

    def __init__(self):
        self.service_registry = get_service_registry()
        self.http_client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
        )
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.request_metrics: Dict[str, Dict[str, Any]] = {}
        
        # Messaging for telemetry
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()

    def get_circuit_breaker(self, service_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for service"""
        if service_name not in self.circuit_breakers:
            self.circuit_breakers[service_name] = CircuitBreaker(
                failure_threshold=5,
                timeout=60,
                expected_exception=Exception
            )
        return self.circuit_breakers[service_name]

    async def forward_request(
        self,
        request: Request,
        route_config: RouteConfig,
        service_instance: Optional[str] = None
    ) -> Response:
        """Forward request to target service with full service mesh capabilities"""
        
        start_time = time.time()
        
        try:
            # Get service instance
            if not service_instance:
                instance = await self.service_registry.get_service_instance(
                    route_config.service_name,
                    strategy=route_config.load_balancing,
                    client_id=request.client.host
                )
                
                if not instance:
                    raise HTTPException(
                        status_code=503,
                        detail=f"No healthy instances for service: {route_config.service_name}"
                    )
            else:
                instance = service_instance

            # Build target URL
            target_url = self._build_target_url(request, route_config, instance)
            
            # Prepare headers
            headers = await self._prepare_headers(request, route_config)
            
            # Get circuit breaker
            circuit_breaker = self.get_circuit_breaker(route_config.service_name)
            
            # Forward request with circuit breaker and retries
            response = await circuit_breaker.call(
                self._send_request_with_retry,
                request.method,
                target_url,
                headers=headers,
                content=await request.body(),
                route_config=route_config,
                request=request
            )
            
            # Process response
            response = await self._process_response(response, route_config)
            
            # Record metrics
            await self._record_request_metrics(
                route_config.service_name,
                request.method,
                target_url,
                time.time() - start_time,
                response.status_code
            )
            
            return response
            
        except Exception as e:
            # Record error metrics
            await self._record_request_metrics(
                route_config.service_name,
                request.method,
                "unknown",
                time.time() - start_time,
                500,
                error=str(e)
            )
            
            logger.error(f"Request forwarding failed: {e}")
            
            if isinstance(e, HTTPException):
                raise e
            else:
                raise HTTPException(status_code=502, detail="Bad Gateway")

    def _build_target_url(self, request: Request, route_config: RouteConfig, instance) -> str:
        """Build target URL for service instance"""
        base_url = instance.base_url
        path = str(request.url.path)
        
        # Apply path rewriting
        if route_config.rewrite_path:
            path = route_config.rewrite_path
        elif route_config.strip_prefix:
            # Remove the first path segment (e.g., /api/v1/service -> /service)
            path_parts = path.strip('/').split('/')
            if len(path_parts) > 2:
                path = '/' + '/'.join(path_parts[2:])
        
        # Add query parameters
        query_string = str(request.url.query)
        if query_string:
            path += f"?{query_string}"
        
        return urljoin(base_url, path)

    async def _prepare_headers(self, request: Request, route_config: RouteConfig) -> Dict[str, str]:
        """Prepare headers for forwarded request"""
        headers = dict(request.headers)
        
        # Remove hop-by-hop headers
        hop_by_hop_headers = [
            'connection', 'keep-alive', 'proxy-authenticate', 
            'proxy-authorization', 'te', 'trailers', 'transfer-encoding', 'upgrade'
        ]
        
        for header in hop_by_hop_headers:
            headers.pop(header, None)
        
        # Remove configured headers
        for header in route_config.headers_to_remove:
            headers.pop(header.lower(), None)
        
        # Add configured headers
        headers.update(route_config.headers_to_add)
        
        # Add service mesh headers
        headers['x-forwarded-for'] = request.client.host
        headers['x-forwarded-proto'] = request.url.scheme
        headers['x-forwarded-host'] = request.url.hostname
        headers['x-gateway-service'] = route_config.service_name
        headers['x-request-id'] = headers.get('x-request-id', f"req-{time.time()}")
        
        return headers

    async def _send_request_with_retry(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        content: bytes,
        route_config: RouteConfig,
        request: Request
    ) -> httpx.Response:
        """Send request with retry logic"""
        
        last_exception = None
        
        for attempt in range(route_config.retry_attempts + 1):
            try:
                response = await self.http_client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    content=content,
                    timeout=route_config.timeout_seconds
                )
                
                # Success if not a server error
                if response.status_code < 500:
                    return response
                
                # Server error - retry if attempts remaining
                if attempt < route_config.retry_attempts:
                    await self._wait_for_retry(attempt, route_config.retry_strategy)
                    continue
                
                return response
                
            except Exception as e:
                last_exception = e
                
                if attempt < route_config.retry_attempts:
                    await self._wait_for_retry(attempt, route_config.retry_strategy)
                    logger.warning(f"Request retry {attempt + 1}/{route_config.retry_attempts}: {e}")
                    continue
                
                raise e
        
        raise last_exception

    async def _wait_for_retry(self, attempt: int, strategy: RetryStrategy):
        """Wait before retry based on strategy"""
        if strategy == RetryStrategy.NONE:
            return
        elif strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            wait_time = min(2 ** attempt, 30)  # Max 30 seconds
        elif strategy == RetryStrategy.LINEAR_BACKOFF:
            wait_time = attempt * 1.0
        elif strategy == RetryStrategy.FIXED_INTERVAL:
            wait_time = 1.0
        else:
            wait_time = 1.0
        
        await asyncio.sleep(wait_time)

    async def _process_response(self, response: httpx.Response, route_config: RouteConfig) -> Response:
        """Process response from target service"""
        # Remove hop-by-hop headers
        headers = dict(response.headers)
        hop_by_hop_headers = [
            'connection', 'keep-alive', 'transfer-encoding'
        ]
        
        for header in hop_by_hop_headers:
            headers.pop(header, None)
        
        # Add gateway headers
        headers['x-gateway-processed'] = 'true'
        headers['x-service-name'] = route_config.service_name
        
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=headers,
            media_type=response.headers.get('content-type')
        )

    async def _record_request_metrics(
        self,
        service_name: str,
        method: str,
        url: str,
        duration: float,
        status_code: int,
        error: Optional[str] = None
    ):
        """Record request metrics for monitoring"""
        try:
            metrics_data = {
                "service_name": service_name,
                "method": method,
                "url": url,
                "duration_ms": duration * 1000,
                "status_code": status_code,
                "timestamp": time.time(),
                "error": error
            }
            
            # Publish to Kafka for real-time monitoring
            self.kafka_manager.produce_api_metrics(
                service_name=service_name,
                method=method,
                status_code=status_code,
                duration_ms=metrics_data["duration_ms"],
                error=error
            )
            
            # Update local metrics
            if service_name not in self.request_metrics:
                self.request_metrics[service_name] = {
                    "total_requests": 0,
                    "total_errors": 0,
                    "avg_response_time": 0,
                    "status_codes": {}
                }
            
            metrics = self.request_metrics[service_name]
            metrics["total_requests"] += 1
            
            if error or status_code >= 400:
                metrics["total_errors"] += 1
            
            # Update average response time (exponential moving average)
            current_avg = metrics["avg_response_time"]
            metrics["avg_response_time"] = (current_avg * 0.9) + (duration * 1000 * 0.1)
            
            # Track status codes
            status_str = str(status_code)
            metrics["status_codes"][status_str] = metrics["status_codes"].get(status_str, 0) + 1
            
        except Exception as e:
            logger.error(f"Failed to record metrics: {e}")

    async def get_metrics(self, service_name: Optional[str] = None) -> Dict[str, Any]:
        """Get service mesh metrics"""
        if service_name:
            return self.request_metrics.get(service_name, {})
        else:
            return {
                "services": self.request_metrics,
                "circuit_breakers": {
                    name: {
                        "state": cb.state.value,
                        "failure_count": cb.failure_count,
                        "last_failure_time": cb.last_failure_time
                    }
                    for name, cb in self.circuit_breakers.items()
                }
            }

    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose()


class APIGateway(BaseHTTPMiddleware):
    """Advanced API Gateway with service mesh capabilities"""

    def __init__(self, app, routes: List[RouteConfig], enable_service_mesh: bool = True):
        super().__init__(app)
        self.routes = {route.path_pattern: route for route in routes}
        self.service_mesh = ServiceMeshProxy() if enable_service_mesh else None
        self.fallback_routes = {}
        
        # Sort routes by specificity (more specific paths first)
        self.route_patterns = sorted(
            self.routes.keys(),
            key=lambda x: (-x.count('/'), -len(x), x)
        )

    def find_matching_route(self, path: str) -> Optional[RouteConfig]:
        """Find matching route configuration for request path"""
        for pattern in self.route_patterns:
            if self._path_matches_pattern(path, pattern):
                return self.routes[pattern]
        return None

    def _path_matches_pattern(self, path: str, pattern: str) -> bool:
        """Check if path matches route pattern"""
        if '*' in pattern:
            # Simple wildcard matching
            prefix = pattern.split('*')[0]
            return path.startswith(prefix)
        else:
            # Exact match
            return path == pattern

    async def dispatch(self, request: Request, call_next):
        """Main gateway request processing"""
        
        # Skip gateway processing for health checks and docs
        if request.url.path in ['/health', '/docs', '/redoc', '/']:
            return await call_next(request)
        
        # Find matching route
        route_config = self.find_matching_route(request.url.path)
        
        if not route_config:
            # No route found - pass through to local app
            return await call_next(request)
        
        # Use service mesh proxy if enabled
        if self.service_mesh:
            try:
                return await self.service_mesh.forward_request(request, route_config)
            except HTTPException as e:
                # Try fallback or return error
                if route_config.service_name in self.fallback_routes:
                    fallback_route = self.fallback_routes[route_config.service_name]
                    return await self.service_mesh.forward_request(request, fallback_route)
                raise e
        else:
            # Fallback to local processing
            return await call_next(request)

    def add_fallback_route(self, primary_service: str, fallback_config: RouteConfig):
        """Add fallback route for service"""
        self.fallback_routes[primary_service] = fallback_config

    async def get_gateway_metrics(self) -> Dict[str, Any]:
        """Get comprehensive gateway metrics"""
        metrics = {
            "total_routes": len(self.routes),
            "routes": list(self.routes.keys()),
        }
        
        if self.service_mesh:
            mesh_metrics = await self.service_mesh.get_metrics()
            metrics.update(mesh_metrics)
        
        return metrics


# Predefined route configurations for common patterns
def create_microservices_routes() -> List[RouteConfig]:
    """Create standard microservices route configurations"""
    return [
        # Authentication Service
        RouteConfig(
            path_pattern="/api/v1/auth/*",
            service_name="auth-service",
            strip_prefix=True,
            timeout_seconds=10,
            retry_attempts=2,
            circuit_breaker=True,
            auth_required=False
        ),
        
        # Sales Service
        RouteConfig(
            path_pattern="/api/v1/sales/*",
            service_name="sales-service",
            strip_prefix=True,
            timeout_seconds=30,
            retry_attempts=3,
            load_balancing=LoadBalancingStrategy.LEAST_CONNECTIONS
        ),
        
        # Analytics Service
        RouteConfig(
            path_pattern="/api/v2/analytics/*",
            service_name="analytics-service",
            strip_prefix=True,
            timeout_seconds=60,
            retry_attempts=1,
            load_balancing=LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN
        ),
        
        # Search Service
        RouteConfig(
            path_pattern="/api/v1/search/*",
            service_name="search-service",
            strip_prefix=True,
            timeout_seconds=15,
            retry_attempts=2,
            headers_to_add={"x-search-version": "2.0"}
        ),
        
        # Data Mart Service
        RouteConfig(
            path_pattern="/api/v1/datamart/*",
            service_name="datamart-service",
            strip_prefix=True,
            timeout_seconds=45,
            retry_attempts=2
        )
    ]


# Factory function for easy gateway setup
def create_api_gateway(app, custom_routes: Optional[List[RouteConfig]] = None) -> APIGateway:
    """Create API Gateway with default or custom routes"""
    routes = custom_routes or create_microservices_routes()
    return APIGateway(app, routes, enable_service_mesh=True)