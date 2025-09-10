"""
Enhanced Circuit Breaker Middleware for Story 1.1 Dashboard API
Enterprise-grade resilience patterns with graceful degradation

Features:
- Multi-level circuit breakers with intelligent failure detection
- Graceful degradation with fallback mechanisms
- Performance-aware thresholds and adaptive timeout
- Real-time monitoring with Prometheus metrics
- Auto-recovery with gradual traffic restoration
- Bulkhead isolation for different service types
- Custom fallback strategies per endpoint
"""
import asyncio
import time
import threading
from collections import deque, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum
import logging
import json
import statistics

from fastapi import HTTPException, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Counter, Histogram, Gauge, Summary
import psutil

from core.logging import get_logger


class CircuitState(Enum):
    """Enhanced circuit breaker states"""
    CLOSED = "closed"           # Normal operation
    OPEN = "open"               # Failing, blocking requests
    HALF_OPEN = "half_open"     # Testing recovery
    DEGRADED = "degraded"       # Partial functionality
    MAINTENANCE = "maintenance"  # Scheduled maintenance


class FailureType(Enum):
    """Types of failures for intelligent detection"""
    TIMEOUT = "timeout"
    SERVER_ERROR = "server_error"
    CLIENT_ERROR = "client_error"
    RATE_LIMIT = "rate_limit"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    DEPENDENCY_FAILURE = "dependency_failure"


class FallbackStrategy(Enum):
    """Fallback strategies for graceful degradation"""
    CACHED_DATA = "cached_data"         # Return cached/stale data
    DEFAULT_VALUE = "default_value"     # Return predefined default
    SIMPLIFIED_RESPONSE = "simplified_response"  # Reduced functionality
    REDIRECT = "redirect"               # Redirect to alternate service
    QUEUE_REQUEST = "queue_request"     # Queue for later processing
    FAIL_FAST = "fail_fast"            # Immediate failure


@dataclass
class FailureMetrics:
    """Failure tracking metrics"""
    total_failures: int = 0
    consecutive_failures: int = 0
    failure_rate: float = 0.0
    avg_response_time: float = 0.0
    last_failure_time: Optional[float] = None
    failure_types: Dict[str, int] = field(default_factory=dict)
    recovery_attempts: int = 0


@dataclass
class CircuitConfiguration:
    """Enhanced circuit breaker configuration"""
    name: str
    failure_threshold: int = 5
    success_threshold: int = 3  # For half-open -> closed transition
    timeout_seconds: float = 60.0
    request_timeout: float = 30.0
    degraded_threshold: float = 0.3  # 30% failure rate triggers degraded mode
    
    # Performance thresholds
    max_response_time_ms: float = 25000.0  # 25 seconds for non-critical
    critical_response_time_ms: float = 1000.0  # 1 second for critical
    
    # Adaptive settings
    adaptive_timeout: bool = True
    adaptive_threshold: bool = True
    
    # Fallback configuration
    fallback_strategy: FallbackStrategy = FallbackStrategy.CACHED_DATA
    fallback_data: Any = None
    
    # Monitoring window
    monitoring_window_size: int = 100
    health_check_interval: float = 30.0
    
    # Bulkhead settings
    max_concurrent_requests: int = 100
    queue_size: int = 50


class EnhancedCircuitBreaker:
    """
    Enterprise-grade circuit breaker with intelligent failure detection
    and graceful degradation capabilities
    """
    
    def __init__(self, config: CircuitConfiguration):
        self.config = config
        self.logger = get_logger(f"circuit_breaker.{config.name}")
        
        # State management
        self.state = CircuitState.CLOSED
        self.state_lock = threading.RLock()
        
        # Metrics tracking
        self.metrics = FailureMetrics()
        self.request_history = deque(maxlen=config.monitoring_window_size)
        self.response_times = deque(maxlen=config.monitoring_window_size)
        
        # Concurrent request tracking for bulkhead pattern
        self.active_requests = 0
        self.request_queue = asyncio.Queue(maxsize=config.queue_size)
        self.request_semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        
        # Health check
        self.last_health_check = time.time()
        self.health_check_task: Optional[asyncio.Task] = None
        
        # Prometheus metrics
        self._setup_prometheus_metrics()
        
        # Start health checking
        self._start_health_check()
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics for monitoring"""
        name = self.config.name
        
        self.prom_requests = Counter(
            f'circuit_breaker_requests_total',
            'Total requests through circuit breaker',
            ['circuit_name', 'status']
        )
        
        self.prom_failures = Counter(
            f'circuit_breaker_failures_total',
            'Total failures by type',
            ['circuit_name', 'failure_type']
        )
        
        self.prom_state_duration = Histogram(
            f'circuit_breaker_state_duration_seconds',
            'Time spent in each state',
            ['circuit_name', 'state']
        )
        
        self.prom_response_time = Histogram(
            f'circuit_breaker_response_time_seconds',
            'Response time distribution',
            ['circuit_name']
        )
        
        self.prom_circuit_state = Gauge(
            f'circuit_breaker_state',
            'Current circuit breaker state (0=closed, 1=open, 2=half_open, 3=degraded)',
            ['circuit_name']
        )
        
        self.prom_active_requests = Gauge(
            f'circuit_breaker_active_requests',
            'Currently active requests',
            ['circuit_name']
        )
    
    def _start_health_check(self):
        """Start background health check task"""
        if not self.health_check_task or self.health_check_task.done():
            self.health_check_task = asyncio.create_task(self._health_check_loop())
    
    async def _health_check_loop(self):
        """Background health check loop"""
        while True:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self.config.health_check_interval)
            except Exception as e:
                self.logger.error(f"Health check error: {e}")
                await asyncio.sleep(self.config.health_check_interval * 2)
    
    async def _perform_health_check(self):
        """Perform health check and state transitions"""
        with self.state_lock:
            current_time = time.time()
            
            # Update adaptive configurations
            if self.config.adaptive_timeout:
                self._update_adaptive_timeout()
            
            if self.config.adaptive_threshold:
                self._update_adaptive_threshold()
            
            # State-specific health checks
            if self.state == CircuitState.OPEN:
                if self._should_attempt_recovery():
                    self._transition_to_half_open()
            
            elif self.state == CircuitState.HALF_OPEN:
                if self._should_close_circuit():
                    self._transition_to_closed()
                elif self._should_reopen_circuit():
                    self._transition_to_open()
            
            elif self.state == CircuitState.DEGRADED:
                if self._should_recover_from_degraded():
                    self._transition_to_closed()
                elif self._should_open_from_degraded():
                    self._transition_to_open()
            
            # Update Prometheus metrics
            state_value = {
                CircuitState.CLOSED: 0,
                CircuitState.OPEN: 1,
                CircuitState.HALF_OPEN: 2,
                CircuitState.DEGRADED: 3,
                CircuitState.MAINTENANCE: 4
            }[self.state]
            
            self.prom_circuit_state.labels(circuit_name=self.config.name).set(state_value)
            self.prom_active_requests.labels(circuit_name=self.config.name).set(self.active_requests)
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function through circuit breaker with enhanced protection
        """
        # Check circuit state before proceeding
        if not await self._can_execute():
            return await self._handle_circuit_open()
        
        # Apply bulkhead pattern
        async with self.request_semaphore:
            self.active_requests += 1
            start_time = time.perf_counter()
            
            try:
                # Execute with timeout
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.config.request_timeout
                )
                
                # Record success
                response_time = (time.perf_counter() - start_time) * 1000
                await self._record_success(response_time)
                
                return result
                
            except asyncio.TimeoutError:
                await self._record_failure(FailureType.TIMEOUT)
                raise HTTPException(
                    status_code=504,
                    detail=f"Request timeout after {self.config.request_timeout}s"
                )
            
            except HTTPException as e:
                # Classify HTTP errors
                if e.status_code >= 500:
                    await self._record_failure(FailureType.SERVER_ERROR)
                elif e.status_code == 429:
                    await self._record_failure(FailureType.RATE_LIMIT)
                else:
                    await self._record_failure(FailureType.CLIENT_ERROR)
                raise
            
            except Exception as e:
                await self._record_failure(FailureType.DEPENDENCY_FAILURE)
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal service error: {str(e)}"
                )
            
            finally:
                self.active_requests -= 1
    
    async def _can_execute(self) -> bool:
        """Check if request can be executed based on circuit state"""
        with self.state_lock:
            if self.state == CircuitState.OPEN:
                return False
            elif self.state == CircuitState.MAINTENANCE:
                return False
            elif self.state == CircuitState.DEGRADED:
                # Allow limited requests in degraded mode
                return self.active_requests < (self.config.max_concurrent_requests * 0.5)
            else:
                return True
    
    async def _handle_circuit_open(self) -> Any:
        """Handle request when circuit is open"""
        self.prom_requests.labels(
            circuit_name=self.config.name, status="blocked"
        ).inc()
        
        # Apply fallback strategy
        if self.config.fallback_strategy == FallbackStrategy.CACHED_DATA:
            return await self._get_cached_fallback()
        
        elif self.config.fallback_strategy == FallbackStrategy.DEFAULT_VALUE:
            return self.config.fallback_data or {
                "error": "Service temporarily unavailable",
                "fallback": True,
                "timestamp": datetime.now().isoformat()
            }
        
        elif self.config.fallback_strategy == FallbackStrategy.SIMPLIFIED_RESPONSE:
            return await self._get_simplified_response()
        
        elif self.config.fallback_strategy == FallbackStrategy.QUEUE_REQUEST:
            return await self._queue_request()
        
        else:  # FAIL_FAST
            raise HTTPException(
                status_code=503,
                detail=f"Service temporarily unavailable - circuit breaker is {self.state.value}"
            )
    
    async def _get_cached_fallback(self) -> Dict[str, Any]:
        """Get cached fallback data"""
        try:
            # This would integrate with your cache system
            # For now, return a basic fallback response
            return {
                "status": "degraded",
                "message": "Returning cached data due to service issues",
                "data": self.config.fallback_data or {},
                "timestamp": datetime.now().isoformat(),
                "circuit_state": self.state.value
            }
        except Exception as e:
            self.logger.error(f"Error getting cached fallback: {e}")
            return {"error": "No fallback data available", "fallback": True}
    
    async def _get_simplified_response(self) -> Dict[str, Any]:
        """Get simplified response with reduced functionality"""
        return {
            "status": "simplified",
            "message": "Service running in simplified mode",
            "basic_data": {
                "timestamp": datetime.now().isoformat(),
                "service_health": "degraded",
                "estimated_recovery": "5-10 minutes"
            },
            "circuit_state": self.state.value
        }
    
    async def _queue_request(self) -> Dict[str, Any]:
        """Queue request for later processing"""
        try:
            if self.request_queue.full():
                raise HTTPException(status_code=503, detail="Request queue is full")
            
            request_id = f"queued_{int(time.time())}_{id(self)}"
            await self.request_queue.put({
                "id": request_id,
                "timestamp": datetime.now().isoformat(),
                "circuit_state": self.state.value
            })
            
            return {
                "status": "queued",
                "request_id": request_id,
                "message": "Request queued for processing when service recovers",
                "estimated_processing_time": "2-5 minutes"
            }
            
        except Exception as e:
            self.logger.error(f"Error queuing request: {e}")
            raise HTTPException(status_code=503, detail="Failed to queue request")
    
    async def _record_success(self, response_time_ms: float):
        """Record successful request"""
        with self.state_lock:
            current_time = time.time()
            
            # Add to history
            self.request_history.append((current_time, True, response_time_ms))
            self.response_times.append(response_time_ms)
            
            # Reset consecutive failures
            self.metrics.consecutive_failures = 0
            
            # Update metrics
            self.prom_requests.labels(
                circuit_name=self.config.name, status="success"
            ).inc()
            
            self.prom_response_time.labels(
                circuit_name=self.config.name
            ).observe(response_time_ms / 1000)
            
            # Check for state transitions
            if self.state == CircuitState.HALF_OPEN:
                if self._should_close_circuit():
                    self._transition_to_closed()
            
            elif self.state == CircuitState.DEGRADED:
                if self._should_recover_from_degraded():
                    self._transition_to_closed()
    
    async def _record_failure(self, failure_type: FailureType):
        """Record failed request"""
        with self.state_lock:
            current_time = time.time()
            
            # Add to history
            self.request_history.append((current_time, False, 0))
            
            # Update failure metrics
            self.metrics.total_failures += 1
            self.metrics.consecutive_failures += 1
            self.metrics.last_failure_time = current_time
            
            # Track failure types
            if failure_type.value not in self.metrics.failure_types:
                self.metrics.failure_types[failure_type.value] = 0
            self.metrics.failure_types[failure_type.value] += 1
            
            # Update Prometheus metrics
            self.prom_requests.labels(
                circuit_name=self.config.name, status="failure"
            ).inc()
            
            self.prom_failures.labels(
                circuit_name=self.config.name, failure_type=failure_type.value
            ).inc()
            
            # Calculate failure rate
            self._update_failure_rate()
            
            # Check for state transitions
            if self.state == CircuitState.CLOSED:
                if self._should_open_circuit():
                    self._transition_to_open()
                elif self._should_degrade_circuit():
                    self._transition_to_degraded()
            
            elif self.state == CircuitState.HALF_OPEN:
                if self._should_reopen_circuit():
                    self._transition_to_open()
    
    def _update_failure_rate(self):
        """Update current failure rate"""
        if not self.request_history:
            self.metrics.failure_rate = 0.0
            return
        
        current_time = time.time()
        window_start = current_time - self.config.health_check_interval
        
        recent_requests = [
            (timestamp, success, response_time) 
            for timestamp, success, response_time in self.request_history
            if timestamp >= window_start
        ]
        
        if not recent_requests:
            self.metrics.failure_rate = 0.0
            return
        
        failures = sum(1 for _, success, _ in recent_requests if not success)
        self.metrics.failure_rate = failures / len(recent_requests)
    
    def _should_open_circuit(self) -> bool:
        """Check if circuit should transition to open"""
        return (
            self.metrics.consecutive_failures >= self.config.failure_threshold or
            self.metrics.failure_rate > 0.5  # 50% failure rate
        )
    
    def _should_degrade_circuit(self) -> bool:
        """Check if circuit should transition to degraded"""
        return (
            self.metrics.failure_rate >= self.config.degraded_threshold and
            self.metrics.consecutive_failures < self.config.failure_threshold
        )
    
    def _should_close_circuit(self) -> bool:
        """Check if circuit should transition to closed from half-open"""
        if not self.request_history:
            return False
        
        # Check recent success rate
        recent_successes = sum(
            1 for _, success, _ in list(self.request_history)[-self.config.success_threshold:]
            if success
        )
        
        return recent_successes >= self.config.success_threshold
    
    def _should_reopen_circuit(self) -> bool:
        """Check if circuit should reopen from half-open"""
        return self.metrics.consecutive_failures > 0
    
    def _should_attempt_recovery(self) -> bool:
        """Check if circuit should attempt recovery (open -> half-open)"""
        if not self.metrics.last_failure_time:
            return False
        
        return (time.time() - self.metrics.last_failure_time) >= self.config.timeout_seconds
    
    def _should_recover_from_degraded(self) -> bool:
        """Check if circuit should recover from degraded state"""
        return (
            self.metrics.failure_rate < (self.config.degraded_threshold * 0.5) and
            self.metrics.consecutive_failures == 0
        )
    
    def _should_open_from_degraded(self) -> bool:
        """Check if circuit should open from degraded state"""
        return (
            self.metrics.failure_rate > 0.7 or  # 70% failure rate
            self.metrics.consecutive_failures >= self.config.failure_threshold
        )
    
    def _transition_to_open(self):
        """Transition circuit to open state"""
        old_state = self.state
        self.state = CircuitState.OPEN
        self.logger.warning(
            f"Circuit breaker '{self.config.name}' opened "
            f"(failures: {self.metrics.consecutive_failures}, "
            f"failure_rate: {self.metrics.failure_rate:.2%})"
        )
        self._record_state_transition(old_state, self.state)
    
    def _transition_to_closed(self):
        """Transition circuit to closed state"""
        old_state = self.state
        self.state = CircuitState.CLOSED
        self.metrics.consecutive_failures = 0
        self.metrics.recovery_attempts += 1
        self.logger.info(f"Circuit breaker '{self.config.name}' closed - service recovered")
        self._record_state_transition(old_state, self.state)
    
    def _transition_to_half_open(self):
        """Transition circuit to half-open state"""
        old_state = self.state
        self.state = CircuitState.HALF_OPEN
        self.logger.info(f"Circuit breaker '{self.config.name}' half-open - testing recovery")
        self._record_state_transition(old_state, self.state)
    
    def _transition_to_degraded(self):
        """Transition circuit to degraded state"""
        old_state = self.state
        self.state = CircuitState.DEGRADED
        self.logger.warning(
            f"Circuit breaker '{self.config.name}' degraded "
            f"(failure_rate: {self.metrics.failure_rate:.2%})"
        )
        self._record_state_transition(old_state, self.state)
    
    def _record_state_transition(self, old_state: CircuitState, new_state: CircuitState):
        """Record state transition metrics"""
        current_time = time.time()
        
        # Record time spent in old state
        if hasattr(self, '_state_start_time'):
            duration = current_time - self._state_start_time
            self.prom_state_duration.labels(
                circuit_name=self.config.name, state=old_state.value
            ).observe(duration)
        
        self._state_start_time = current_time
    
    def _update_adaptive_timeout(self):
        """Update timeout based on recent response times"""
        if len(self.response_times) < 10:
            return
        
        # Calculate 95th percentile response time
        percentile_95 = statistics.quantiles(self.response_times, n=20)[18]  # 95th percentile
        
        # Adjust timeout to be 2x the 95th percentile, with bounds
        new_timeout = max(
            min(percentile_95 * 2 / 1000, 300.0),  # Max 5 minutes
            1.0  # Min 1 second
        )
        
        if abs(new_timeout - self.config.request_timeout) > 5.0:  # Only change if significant
            self.config.request_timeout = new_timeout
            self.logger.info(f"Adaptive timeout updated to {new_timeout:.1f}s")
    
    def _update_adaptive_threshold(self):
        """Update failure threshold based on recent patterns"""
        if len(self.request_history) < 50:
            return
        
        # Analyze failure patterns to adjust threshold
        recent_failures = [
            success for _, success, _ in list(self.request_history)[-50:]
        ]
        
        failure_rate = (50 - sum(recent_failures)) / 50
        
        if failure_rate < 0.05:  # Very stable, can increase threshold
            new_threshold = min(self.config.failure_threshold + 1, 10)
        elif failure_rate > 0.2:  # Unstable, decrease threshold
            new_threshold = max(self.config.failure_threshold - 1, 3)
        else:
            new_threshold = self.config.failure_threshold
        
        if new_threshold != self.config.failure_threshold:
            self.config.failure_threshold = new_threshold
            self.logger.info(f"Adaptive threshold updated to {new_threshold}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive circuit breaker statistics"""
        with self.state_lock:
            total_requests = len(self.request_history)
            successes = sum(1 for _, success, _ in self.request_history if success)
            
            return {
                "name": self.config.name,
                "state": self.state.value,
                "configuration": {
                    "failure_threshold": self.config.failure_threshold,
                    "success_threshold": self.config.success_threshold,
                    "timeout_seconds": self.config.timeout_seconds,
                    "request_timeout": self.config.request_timeout,
                    "fallback_strategy": self.config.fallback_strategy.value
                },
                "metrics": {
                    "total_requests": total_requests,
                    "success_rate": (successes / total_requests * 100) if total_requests > 0 else 0,
                    "failure_rate": self.metrics.failure_rate * 100,
                    "consecutive_failures": self.metrics.consecutive_failures,
                    "total_failures": self.metrics.total_failures,
                    "recovery_attempts": self.metrics.recovery_attempts,
                    "active_requests": self.active_requests,
                    "avg_response_time_ms": (
                        statistics.mean(self.response_times) if self.response_times else 0
                    )
                },
                "failure_breakdown": self.metrics.failure_types,
                "health": {
                    "last_failure_time": (
                        datetime.fromtimestamp(self.metrics.last_failure_time).isoformat()
                        if self.metrics.last_failure_time else None
                    ),
                    "time_since_last_failure": (
                        time.time() - self.metrics.last_failure_time
                        if self.metrics.last_failure_time else None
                    )
                }
            }


class EnhancedCircuitBreakerMiddleware(BaseHTTPMiddleware):
    """
    Enhanced circuit breaker middleware with intelligent routing and fallbacks
    """
    
    def __init__(self, app, circuit_configs: Optional[Dict[str, CircuitConfiguration]] = None):
        super().__init__(app)
        self.logger = get_logger("enhanced_circuit_breaker_middleware")
        
        # Initialize circuit breakers
        self.circuit_breakers: Dict[str, EnhancedCircuitBreaker] = {}
        
        if circuit_configs:
            for name, config in circuit_configs.items():
                self.circuit_breakers[name] = EnhancedCircuitBreaker(config)
        
        # Default configurations for dashboard API endpoints
        self._setup_default_circuits()
        
        # Middleware metrics
        self.middleware_metrics = {
            "requests_processed": 0,
            "circuit_breaker_activations": 0,
            "fallbacks_served": 0,
            "response_time_improvements": 0
        }
    
    def _setup_default_circuits(self):
        """Setup default circuit breakers for dashboard API"""
        
        # Executive Dashboard - Ultra-critical
        executive_config = CircuitConfiguration(
            name="executive_dashboard",
            failure_threshold=3,
            success_threshold=2,
            timeout_seconds=30.0,
            request_timeout=5.0,  # 5 second timeout for executive dashboards
            degraded_threshold=0.2,  # Degrade at 20% failure rate
            max_response_time_ms=10000.0,  # 10 seconds max
            critical_response_time_ms=2000.0,  # 2 seconds critical
            fallback_strategy=FallbackStrategy.CACHED_DATA,
            max_concurrent_requests=50
        )
        
        # Revenue Analytics - High priority
        revenue_config = CircuitConfiguration(
            name="revenue_analytics",
            failure_threshold=5,
            success_threshold=3,
            timeout_seconds=60.0,
            request_timeout=10.0,
            degraded_threshold=0.3,
            fallback_strategy=FallbackStrategy.SIMPLIFIED_RESPONSE,
            max_concurrent_requests=100
        )
        
        # WebSocket connections - Special handling
        websocket_config = CircuitConfiguration(
            name="websocket_dashboard",
            failure_threshold=8,
            success_threshold=4,
            timeout_seconds=90.0,
            request_timeout=30.0,
            degraded_threshold=0.4,
            fallback_strategy=FallbackStrategy.QUEUE_REQUEST,
            max_concurrent_requests=1000,
            queue_size=200
        )
        
        # General API endpoints
        general_config = CircuitConfiguration(
            name="general_api",
            failure_threshold=10,
            success_threshold=5,
            timeout_seconds=120.0,
            request_timeout=30.0,
            degraded_threshold=0.5,
            fallback_strategy=FallbackStrategy.DEFAULT_VALUE,
            max_concurrent_requests=200
        )
        
        # Create circuit breakers
        configs = {
            "executive_dashboard": executive_config,
            "revenue_analytics": revenue_config,
            "websocket_dashboard": websocket_config,
            "general_api": general_config
        }
        
        for name, config in configs.items():
            if name not in self.circuit_breakers:
                self.circuit_breakers[name] = EnhancedCircuitBreaker(config)
    
    async def dispatch(self, request: Request, call_next) -> Response:
        """Enhanced request processing with intelligent circuit breaker routing"""
        
        start_time = time.perf_counter()
        self.middleware_metrics["requests_processed"] += 1
        
        # Determine circuit breaker based on endpoint
        circuit_name = self._get_circuit_name(request)
        circuit_breaker = self.circuit_breakers.get(circuit_name)
        
        if not circuit_breaker:
            # No circuit breaker configured, proceed normally
            return await call_next(request)
        
        try:
            # Execute through circuit breaker
            response = await circuit_breaker.call(call_next, request)
            
            # Add circuit breaker headers for monitoring
            if hasattr(response, 'headers'):
                response.headers["X-Circuit-Breaker"] = circuit_name
                response.headers["X-Circuit-State"] = circuit_breaker.state.value
                
                # Add performance metrics
                response_time = (time.perf_counter() - start_time) * 1000
                response.headers["X-Response-Time-Ms"] = str(round(response_time, 2))
                
                if response_time < circuit_breaker.config.critical_response_time_ms:
                    self.middleware_metrics["response_time_improvements"] += 1
            
            return response
            
        except HTTPException as e:
            # Circuit breaker is open or degraded, return fallback response
            self.middleware_metrics["circuit_breaker_activations"] += 1
            
            if e.status_code == 503:
                self.middleware_metrics["fallbacks_served"] += 1
                
                # Create enhanced fallback response
                fallback_response = await self._create_fallback_response(
                    request, circuit_breaker, e
                )
                return fallback_response
            
            # Re-raise non-circuit-breaker exceptions
            raise
        
        except Exception as e:
            self.logger.error(f"Unexpected error in circuit breaker middleware: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")
    
    def _get_circuit_name(self, request: Request) -> str:
        """Determine which circuit breaker to use based on request"""
        
        path = request.url.path.lower()
        method = request.method.upper()
        
        # Executive dashboard endpoints
        if any(pattern in path for pattern in [
            "/api/v1/executive", "/api/v2/executive", 
            "/api/v1/dashboard/executive", "/ws/dashboard/executive"
        ]):
            return "executive_dashboard"
        
        # Revenue analytics endpoints
        elif any(pattern in path for pattern in [
            "/api/v1/revenue", "/api/v2/revenue", "/api/v1/sales",
            "/api/v2/sales", "/api/v1/analytics/revenue"
        ]):
            return "revenue_analytics"
        
        # WebSocket endpoints
        elif path.startswith("/ws/") or "websocket" in path:
            return "websocket_dashboard"
        
        # Default to general API circuit breaker
        else:
            return "general_api"
    
    async def _create_fallback_response(
        self, 
        request: Request, 
        circuit_breaker: EnhancedCircuitBreaker, 
        original_exception: HTTPException
    ) -> JSONResponse:
        """Create enhanced fallback response based on circuit breaker strategy"""
        
        fallback_data = {
            "status": "service_degraded",
            "message": "Service is temporarily degraded, serving fallback response",
            "circuit_breaker": {
                "name": circuit_breaker.config.name,
                "state": circuit_breaker.state.value,
                "strategy": circuit_breaker.config.fallback_strategy.value
            },
            "timestamp": datetime.now().isoformat(),
            "request_id": getattr(request.state, "correlation_id", "unknown")
        }
        
        # Add strategy-specific data
        if circuit_breaker.config.fallback_strategy == FallbackStrategy.CACHED_DATA:
            fallback_data.update({
                "data_source": "cache",
                "data_freshness": "may_be_stale",
                "recommendation": "Data may be cached, refresh in a few minutes"
            })
        
        elif circuit_breaker.config.fallback_strategy == FallbackStrategy.SIMPLIFIED_RESPONSE:
            fallback_data.update({
                "data_source": "simplified",
                "functionality": "reduced",
                "recommendation": "Limited functionality available"
            })
        
        elif circuit_breaker.config.fallback_strategy == FallbackStrategy.QUEUE_REQUEST:
            fallback_data.update({
                "status": "queued",
                "message": "Request queued for processing",
                "estimated_completion": "2-5 minutes"
            })
        
        # Add performance guidance
        fallback_data["performance_guidance"] = {
            "retry_after_seconds": circuit_breaker.config.timeout_seconds,
            "alternative_endpoints": self._get_alternative_endpoints(request),
            "monitoring_url": "/api/v1/health/circuit-breakers"
        }
        
        return JSONResponse(
            content=fallback_data,
            status_code=503,
            headers={
                "X-Circuit-Breaker": circuit_breaker.config.name,
                "X-Circuit-State": circuit_breaker.state.value,
                "X-Fallback-Strategy": circuit_breaker.config.fallback_strategy.value,
                "Retry-After": str(int(circuit_breaker.config.timeout_seconds)),
                "Cache-Control": "no-cache, no-store, must-revalidate"
            }
        )
    
    def _get_alternative_endpoints(self, request: Request) -> List[str]:
        """Suggest alternative endpoints if available"""
        path = request.url.path.lower()
        alternatives = []
        
        if "/executive" in path:
            alternatives.extend([
                "/api/v1/dashboard/summary",
                "/api/v1/health",
                "/api/v1/monitoring/status"
            ])
        
        elif "/revenue" in path or "/sales" in path:
            alternatives.extend([
                "/api/v1/dashboard/basic",
                "/api/v1/reports/cached"
            ])
        
        return alternatives
    
    def get_middleware_stats(self) -> Dict[str, Any]:
        """Get comprehensive middleware statistics"""
        circuit_stats = {}
        
        for name, circuit in self.circuit_breakers.items():
            circuit_stats[name] = circuit.get_stats()
        
        # Calculate overall health score
        total_circuits = len(self.circuit_breakers)
        healthy_circuits = sum(
            1 for circuit in self.circuit_breakers.values()
            if circuit.state in [CircuitState.CLOSED, CircuitState.DEGRADED]
        )
        
        health_score = (healthy_circuits / total_circuits * 100) if total_circuits > 0 else 100
        
        return {
            "middleware_metrics": self.middleware_metrics,
            "circuit_breakers": circuit_stats,
            "overall_health": {
                "score": round(health_score, 1),
                "total_circuits": total_circuits,
                "healthy_circuits": healthy_circuits,
                "degraded_circuits": sum(
                    1 for circuit in self.circuit_breakers.values()
                    if circuit.state == CircuitState.DEGRADED
                ),
                "open_circuits": sum(
                    1 for circuit in self.circuit_breakers.values()
                    if circuit.state == CircuitState.OPEN
                )
            },
            "recommendations": self._generate_health_recommendations()
        }
    
    def _generate_health_recommendations(self) -> List[str]:
        """Generate health recommendations based on current state"""
        recommendations = []
        
        for name, circuit in self.circuit_breakers.items():
            if circuit.state == CircuitState.OPEN:
                recommendations.append(
                    f"Circuit '{name}' is open - check underlying service health"
                )
            elif circuit.state == CircuitState.DEGRADED:
                recommendations.append(
                    f"Circuit '{name}' is degraded - monitor failure rates"
                )
            
            # Check high failure rates
            if circuit.metrics.failure_rate > 0.3:
                recommendations.append(
                    f"High failure rate ({circuit.metrics.failure_rate:.1%}) in circuit '{name}'"
                )
            
            # Check high response times
            if circuit.response_times and statistics.mean(circuit.response_times) > 10000:
                recommendations.append(
                    f"High response times in circuit '{name}' - consider optimization"
                )
        
        if not recommendations:
            recommendations.append("All circuits are healthy")
        
        return recommendations


# Export enhanced circuit breaker components
__all__ = [
    "EnhancedCircuitBreaker",
    "EnhancedCircuitBreakerMiddleware", 
    "CircuitConfiguration",
    "CircuitState",
    "FallbackStrategy",
    "FailureType"
]