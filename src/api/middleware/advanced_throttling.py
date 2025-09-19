"""
Enterprise-grade API throttling with circuit breaker patterns and intelligent request scheduling.
Complements rate limiting with more sophisticated traffic management for production systems.
"""
from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger
from monitoring.system_resource_monitor import SystemResourceMonitor

logger = get_logger(__name__)


class ThrottleStrategy(Enum):
    """Available throttling strategies."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"
    SLIDING_LOG = "sliding_log"
    ADAPTIVE = "adaptive"


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class Priority(Enum):
    """Request priority levels."""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5


@dataclass
class ThrottleConfig:
    """Throttling configuration for different strategies."""
    strategy: ThrottleStrategy = ThrottleStrategy.TOKEN_BUCKET
    capacity: int = 100
    refill_rate: float = 10.0  # tokens per second
    burst_capacity: int = 150
    window_size: int = 60  # seconds
    max_queue_size: int = 1000
    circuit_breaker_enabled: bool = True
    failure_threshold: int = 50
    recovery_timeout: int = 60
    half_open_max_calls: int = 10


@dataclass
class RequestContext:
    """Context information for incoming requests."""
    request_id: str
    client_id: str
    endpoint: str
    method: str
    priority: Priority = Priority.NORMAL
    metadata: Dict[str, Any] = field(default_factory=dict)
    queued_at: Optional[float] = None
    processing_started: Optional[float] = None


class TokenBucket:
    """Token bucket implementation for rate limiting."""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = float(capacity)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def consume(self, tokens: int = 1) -> bool:
        """Attempt to consume tokens from the bucket."""
        async with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def _refill(self):
        """Refill tokens based on time elapsed."""
        now = time.time()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now

    async def get_wait_time(self, tokens: int = 1) -> float:
        """Get estimated wait time for tokens to be available."""
        async with self._lock:
            self._refill()
            if self.tokens >= tokens:
                return 0.0

            needed_tokens = tokens - self.tokens
            return needed_tokens / self.refill_rate


class LeakyBucket:
    """Leaky bucket implementation for smoothing traffic."""

    def __init__(self, capacity: int, leak_rate: float):
        self.capacity = capacity
        self.leak_rate = leak_rate
        self.volume = 0.0
        self.last_leak = time.time()
        self._lock = asyncio.Lock()

    async def add_request(self, volume: float = 1.0) -> bool:
        """Try to add a request to the bucket."""
        async with self._lock:
            self._leak()
            if self.volume + volume <= self.capacity:
                self.volume += volume
                return True
            return False

    def _leak(self):
        """Leak requests from the bucket."""
        now = time.time()
        elapsed = now - self.last_leak
        leaked = elapsed * self.leak_rate
        self.volume = max(0, self.volume - leaked)
        self.last_leak = now


class CircuitBreaker:
    """Circuit breaker for protecting against cascading failures."""

    def __init__(self, config: ThrottleConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.half_open_calls = 0
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self.config.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_calls = 0
                else:
                    raise HTTPException(
                        status_code=503,
                        detail="Service temporarily unavailable (circuit breaker open)"
                    )

            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_calls >= self.config.half_open_max_calls:
                    raise HTTPException(
                        status_code=503,
                        detail="Service temporarily unavailable (circuit breaker half-open)"
                    )
                self.half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise

    async def _on_success(self):
        """Handle successful call."""
        async with self._lock:
            self.failure_count = 0
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED

    async def _on_failure(self):
        """Handle failed call."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN


class PriorityQueue:
    """Priority queue for request scheduling."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queues: Dict[Priority, List[RequestContext]] = {
            priority: [] for priority in Priority
        }
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)

    async def put(self, context: RequestContext) -> bool:
        """Add request to priority queue."""
        async with self._lock:
            total_size = sum(len(queue) for queue in self.queues.values())

            if total_size >= self.max_size:
                # Remove lowest priority items first
                for priority in reversed(Priority):
                    if self.queues[priority]:
                        self.queues[priority].pop()
                        break
                else:
                    return False  # Queue still full

            context.queued_at = time.time()
            self.queues[context.priority].append(context)
            self._not_empty.notify()
            return True

    async def get(self) -> RequestContext:
        """Get highest priority request from queue."""
        async with self._not_empty:
            while self._is_empty():
                await self._not_empty.wait()

            # Get from highest priority queue first
            for priority in Priority:
                if self.queues[priority]:
                    return self.queues[priority].pop(0)

            raise RuntimeError("Queue is empty")  # Should never happen

    def _is_empty(self) -> bool:
        """Check if queue is empty."""
        return all(not queue for queue in self.queues.values())

    async def size(self) -> int:
        """Get total queue size."""
        async with self._lock:
            return sum(len(queue) for queue in self.queues.values())


class AdaptiveThrottler:
    """Adaptive throttling based on system metrics and load patterns."""

    def __init__(self, config: ThrottleConfig):
        self.config = config
        self.base_capacity = config.capacity
        self.base_refill_rate = config.refill_rate
        self.load_factor = 1.0
        self.response_times: List[float] = []
        self.error_rates: List[float] = []
        self.resource_monitor = SystemResourceMonitor()

    async def update_metrics(self, response_time: float, error_occurred: bool):
        """Update performance metrics for adaptive adjustment."""
        # Keep rolling window of metrics
        self.response_times.append(response_time)
        if len(self.response_times) > 100:
            self.response_times.pop(0)

        error_rate = 1.0 if error_occurred else 0.0
        self.error_rates.append(error_rate)
        if len(self.error_rates) > 100:
            self.error_rates.pop(0)

        # Update load factor based on system metrics
        await self._calculate_load_factor()

    async def _calculate_load_factor(self):
        """Calculate adaptive load factor based on system performance."""
        # Get system metrics
        system_metrics = await self.resource_monitor.get_metrics()

        cpu_usage = system_metrics.get('cpu_usage_percent', 0)
        memory_usage = system_metrics.get('memory_usage_percent', 0)

        # Calculate performance indicators
        avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else 0
        avg_error_rate = sum(self.error_rates) / len(self.error_rates) if self.error_rates else 0

        # Adaptive load factor calculation
        cpu_factor = 1.0 - (cpu_usage / 100.0 * 0.5)  # Reduce capacity as CPU increases
        memory_factor = 1.0 - (memory_usage / 100.0 * 0.3)  # Reduce capacity as memory increases

        # Response time factor (assume 100ms is baseline)
        response_factor = max(0.5, 1.0 - ((avg_response_time - 0.1) * 2))

        # Error rate factor
        error_factor = max(0.3, 1.0 - (avg_error_rate * 0.7))

        # Combined load factor
        self.load_factor = min(cpu_factor, memory_factor, response_factor, error_factor)

        logger.debug(
            f"Adaptive throttling - Load factor: {self.load_factor:.2f}, "
            f"CPU: {cpu_usage:.1f}%, Memory: {memory_usage:.1f}%, "
            f"Avg response: {avg_response_time:.3f}s, Error rate: {avg_error_rate:.3f}"
        )

    def get_adjusted_capacity(self) -> int:
        """Get capacity adjusted for current load."""
        return max(1, int(self.base_capacity * self.load_factor))

    def get_adjusted_refill_rate(self) -> float:
        """Get refill rate adjusted for current load."""
        return max(0.1, self.base_refill_rate * self.load_factor)


class IntelligentThrottleManager:
    """
    Intelligent throttling manager that combines multiple strategies
    and provides enterprise-grade traffic management.
    """

    def __init__(self, config: ThrottleConfig):
        self.config = config
        self.token_bucket = TokenBucket(config.capacity, config.refill_rate)
        self.leaky_bucket = LeakyBucket(config.capacity, config.refill_rate)
        self.circuit_breaker = CircuitBreaker(config) if config.circuit_breaker_enabled else None
        self.priority_queue = PriorityQueue(config.max_queue_size)
        self.adaptive_throttler = AdaptiveThrottler(config)

        # Metrics
        self.metrics = {
            'total_requests': 0,
            'throttled_requests': 0,
            'queued_requests': 0,
            'circuit_breaker_trips': 0,
            'adaptive_adjustments': 0
        }

        # Request processing worker
        self._worker_task: Optional[asyncio.Task] = None
        self._processing_requests: Dict[str, RequestContext] = {}
        self._shutdown_event = asyncio.Event()

    async def start(self):
        """Start the throttle manager."""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._process_queue())
        logger.info("Intelligent throttle manager started")

    async def stop(self):
        """Stop the throttle manager."""
        self._shutdown_event.set()
        if self._worker_task:
            await self._worker_task
        logger.info("Intelligent throttle manager stopped")

    async def should_throttle(self, context: RequestContext) -> tuple[bool, Dict[str, Any]]:
        """
        Determine if request should be throttled based on configured strategy.

        Returns:
            Tuple of (should_throttle, metadata)
        """
        self.metrics['total_requests'] += 1

        # Check circuit breaker first
        if self.circuit_breaker and self.circuit_breaker.state == CircuitBreakerState.OPEN:
            self.metrics['circuit_breaker_trips'] += 1
            return True, {
                'reason': 'circuit_breaker_open',
                'retry_after': self.config.recovery_timeout,
                'state': self.circuit_breaker.state.value
            }

        # Apply throttling strategy
        if self.config.strategy == ThrottleStrategy.TOKEN_BUCKET:
            return await self._token_bucket_throttle(context)
        elif self.config.strategy == ThrottleStrategy.LEAKY_BUCKET:
            return await self._leaky_bucket_throttle(context)
        elif self.config.strategy == ThrottleStrategy.ADAPTIVE:
            return await self._adaptive_throttle(context)
        else:
            # Default to token bucket
            return await self._token_bucket_throttle(context)

    async def _token_bucket_throttle(self, context: RequestContext) -> tuple[bool, Dict[str, Any]]:
        """Token bucket throttling implementation."""
        if await self.token_bucket.consume():
            return False, {'strategy': 'token_bucket', 'tokens_consumed': 1}

        self.metrics['throttled_requests'] += 1
        wait_time = await self.token_bucket.get_wait_time()

        return True, {
            'reason': 'token_bucket_exhausted',
            'strategy': 'token_bucket',
            'retry_after': math.ceil(wait_time),
            'wait_time': wait_time
        }

    async def _leaky_bucket_throttle(self, context: RequestContext) -> tuple[bool, Dict[str, Any]]:
        """Leaky bucket throttling implementation."""
        if await self.leaky_bucket.add_request():
            return False, {'strategy': 'leaky_bucket', 'volume_added': 1.0}

        self.metrics['throttled_requests'] += 1

        return True, {
            'reason': 'leaky_bucket_full',
            'strategy': 'leaky_bucket',
            'retry_after': 60  # Fixed retry for leaky bucket
        }

    async def _adaptive_throttle(self, context: RequestContext) -> tuple[bool, Dict[str, Any]]:
        """Adaptive throttling based on system load."""
        # Update token bucket with adaptive parameters
        adjusted_capacity = self.adaptive_throttler.get_adjusted_capacity()
        adjusted_refill_rate = self.adaptive_throttler.get_adjusted_refill_rate()

        # Create temporary adaptive bucket
        adaptive_bucket = TokenBucket(adjusted_capacity, adjusted_refill_rate)
        adaptive_bucket.tokens = min(adjusted_capacity, self.token_bucket.tokens)

        if await adaptive_bucket.consume():
            # Update main bucket
            await self.token_bucket.consume()
            self.metrics['adaptive_adjustments'] += 1

            return False, {
                'strategy': 'adaptive',
                'load_factor': self.adaptive_throttler.load_factor,
                'adjusted_capacity': adjusted_capacity,
                'adjusted_refill_rate': adjusted_refill_rate
            }

        self.metrics['throttled_requests'] += 1
        wait_time = await adaptive_bucket.get_wait_time()

        return True, {
            'reason': 'adaptive_throttle',
            'strategy': 'adaptive',
            'load_factor': self.adaptive_throttler.load_factor,
            'retry_after': math.ceil(wait_time),
            'wait_time': wait_time
        }

    async def queue_request(self, context: RequestContext) -> bool:
        """Queue request for later processing."""
        if await self.priority_queue.put(context):
            self.metrics['queued_requests'] += 1
            return True
        return False

    async def _process_queue(self):
        """Background worker to process queued requests."""
        while not self._shutdown_event.is_set():
            try:
                # Get next request from queue
                context = await asyncio.wait_for(
                    self.priority_queue.get(),
                    timeout=1.0
                )

                context.processing_started = time.time()
                self._processing_requests[context.request_id] = context

                # Check if we can process the request now
                should_throttle, metadata = await self.should_throttle(context)

                if not should_throttle:
                    # Process request (this would trigger the actual request handling)
                    logger.debug(f"Processing queued request {context.request_id}")
                    # In a real implementation, this would trigger request processing

                    # Clean up
                    self._processing_requests.pop(context.request_id, None)
                else:
                    # Re-queue if still throttled (with lower priority)
                    if context.priority.value < Priority.BACKGROUND.value:
                        context.priority = Priority(context.priority.value + 1)
                        await self.priority_queue.put(context)

            except asyncio.TimeoutError:
                # No requests in queue, continue
                continue
            except Exception as e:
                logger.error(f"Error processing queue: {e}")
                await asyncio.sleep(1)

    async def get_metrics(self) -> Dict[str, Any]:
        """Get throttling metrics."""
        queue_size = await self.priority_queue.size()

        return {
            **self.metrics,
            'queue_size': queue_size,
            'processing_requests': len(self._processing_requests),
            'circuit_breaker_state': self.circuit_breaker.state.value if self.circuit_breaker else 'disabled',
            'load_factor': self.adaptive_throttler.load_factor,
            'token_bucket_tokens': self.token_bucket.tokens,
            'leaky_bucket_volume': self.leaky_bucket.volume
        }


class EnterpriseThrottleMiddleware(BaseHTTPMiddleware):
    """
    Enterprise throttling middleware with intelligent request management,
    priority queuing, and adaptive capacity management.
    """

    def __init__(
        self,
        app,
        config: ThrottleConfig | None = None,
        endpoint_configs: Dict[str, ThrottleConfig] | None = None
    ):
        super().__init__(app)
        self.default_config = config or ThrottleConfig()
        self.endpoint_configs = endpoint_configs or {}
        self.throttle_managers: Dict[str, IntelligentThrottleManager] = {}

    async def _get_throttle_manager(self, endpoint: str) -> IntelligentThrottleManager:
        """Get or create throttle manager for endpoint."""
        if endpoint not in self.throttle_managers:
            config = self.endpoint_configs.get(endpoint, self.default_config)
            manager = IntelligentThrottleManager(config)
            await manager.start()
            self.throttle_managers[endpoint] = manager

        return self.throttle_managers[endpoint]

    def _get_request_priority(self, request: Request) -> Priority:
        """Determine request priority based on headers and endpoint."""
        # Check for priority header
        priority_header = request.headers.get('x-request-priority', '').lower()
        if priority_header == 'critical':
            return Priority.CRITICAL
        elif priority_header == 'high':
            return Priority.HIGH
        elif priority_header == 'low':
            return Priority.LOW
        elif priority_header == 'background':
            return Priority.BACKGROUND

        # Default priority based on endpoint
        if request.url.path.startswith('/api/v1/health'):
            return Priority.CRITICAL
        elif request.url.path.startswith('/api/v1/auth'):
            return Priority.HIGH
        elif request.method == 'GET':
            return Priority.NORMAL
        else:
            return Priority.NORMAL

    def _create_request_context(self, request: Request) -> RequestContext:
        """Create request context for throttling."""
        client_id = request.headers.get('x-client-id', f"ip:{request.client.host}")
        request_id = request.headers.get('x-request-id', f"{int(time.time())}-{hash(str(request.url))}")
        endpoint = f"{request.method}:{request.url.path}"
        priority = self._get_request_priority(request)

        return RequestContext(
            request_id=request_id,
            client_id=client_id,
            endpoint=endpoint,
            method=request.method,
            priority=priority,
            metadata={
                'user_agent': request.headers.get('user-agent', ''),
                'content_length': request.headers.get('content-length', '0'),
                'accept': request.headers.get('accept', ''),
            }
        )

    async def dispatch(self, request: Request, call_next):
        """Process request with intelligent throttling."""
        start_time = time.time()
        context = self._create_request_context(request)

        # Skip throttling for health checks
        if request.url.path in ['/health', '/api/v1/health']:
            return await call_next(request)

        try:
            # Get throttle manager for this endpoint
            manager = await self._get_throttle_manager(context.endpoint)

            # Check if request should be throttled
            should_throttle, metadata = await manager.should_throttle(context)

            if should_throttle:
                # Try to queue the request if not critical
                if context.priority != Priority.CRITICAL:
                    if await manager.queue_request(context):
                        # Request queued successfully
                        return HTTPException(
                            status_code=202,
                            detail={
                                "message": "Request queued for processing",
                                "request_id": context.request_id,
                                "estimated_wait": metadata.get('retry_after', 30)
                            }
                        )

                # Return throttle response
                retry_after = metadata.get('retry_after', 60)
                response_data = {
                    "error": "Request throttled",
                    "reason": metadata.get('reason', 'unknown'),
                    "retry_after": retry_after,
                    "request_id": context.request_id,
                    "timestamp": datetime.now().isoformat()
                }

                response = HTTPException(status_code=429, detail=response_data)
                response.headers["Retry-After"] = str(retry_after)
                response.headers["X-RateLimit-Strategy"] = metadata.get('strategy', 'unknown')
                return response

            # Process request
            response = await call_next(request)

            # Update adaptive metrics
            processing_time = time.time() - start_time
            error_occurred = response.status_code >= 400

            await manager.adaptive_throttler.update_metrics(processing_time, error_occurred)

            # Add throttling headers
            response.headers["X-Throttle-Strategy"] = metadata.get('strategy', 'unknown')
            response.headers["X-Request-Priority"] = context.priority.name
            response.headers["X-Processing-Time"] = f"{processing_time * 1000:.2f}ms"

            if 'load_factor' in metadata:
                response.headers["X-System-Load-Factor"] = f"{metadata['load_factor']:.2f}"

            return response

        except Exception as e:
            logger.error(f"Throttling middleware error: {e}")
            # If throttling fails, allow request through
            return await call_next(request)


# Factory function for common throttling configurations
def create_enterprise_throttle_middleware(
    app,
    default_strategy: ThrottleStrategy = ThrottleStrategy.ADAPTIVE,
    default_capacity: int = 1000,
    endpoint_overrides: Dict[str, Dict[str, Any]] | None = None
) -> EnterpriseThrottleMiddleware:
    """Create enterprise throttle middleware with best practices."""

    default_config = ThrottleConfig(
        strategy=default_strategy,
        capacity=default_capacity,
        refill_rate=default_capacity / 60.0,  # Refill over 1 minute
        burst_capacity=int(default_capacity * 1.5),
        circuit_breaker_enabled=True,
        failure_threshold=100,
        recovery_timeout=300
    )

    endpoint_configs = {}

    # High-traffic endpoints
    endpoint_configs["GET:/api/v1/data"] = ThrottleConfig(
        strategy=ThrottleStrategy.ADAPTIVE,
        capacity=5000,
        refill_rate=100.0,
        burst_capacity=7500
    )

    # Authentication endpoints
    endpoint_configs["POST:/api/v1/auth/login"] = ThrottleConfig(
        strategy=ThrottleStrategy.TOKEN_BUCKET,
        capacity=50,
        refill_rate=1.0,
        burst_capacity=100,
        circuit_breaker_enabled=True,
        failure_threshold=20
    )

    # Heavy processing endpoints
    endpoint_configs["POST:/api/v1/analytics/process"] = ThrottleConfig(
        strategy=ThrottleStrategy.LEAKY_BUCKET,
        capacity=10,
        refill_rate=0.5,
        burst_capacity=20,
        circuit_breaker_enabled=True,
        failure_threshold=5
    )

    # Apply custom overrides
    if endpoint_overrides:
        for endpoint, overrides in endpoint_overrides.items():
            if endpoint in endpoint_configs:
                # Update existing config
                config = endpoint_configs[endpoint]
                for key, value in overrides.items():
                    if hasattr(config, key):
                        setattr(config, key, value)
            else:
                # Create new config with overrides
                config = ThrottleConfig(**overrides)
                endpoint_configs[endpoint] = config

    return EnterpriseThrottleMiddleware(
        app=app,
        config=default_config,
        endpoint_configs=endpoint_configs
    )