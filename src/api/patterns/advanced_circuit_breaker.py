"""
Advanced Circuit Breaker Pattern for External Service Calls
==========================================================

Enterprise-grade circuit breaker implementation with intelligent failure detection,
adaptive thresholds, and comprehensive monitoring for external service resilience.

Key Features:
- Multi-level circuit breaker states with adaptive thresholds
- Service-specific configuration and monitoring
- Intelligent failure detection with error classification
- Fallback mechanisms with degraded service modes
- Real-time metrics and alerting integration
- Automatic recovery with health checking
- Load balancing integration and service discovery
"""

import asyncio
import json
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union, Awaitable
from functools import wraps
import aiohttp
import logging

from core.logging import get_logger
from core.config.base_config import BaseConfig

logger = get_logger(__name__)


class CircuitState(Enum):
    """Enhanced circuit breaker states"""
    CLOSED = "closed"              # Normal operation
    OPEN = "open"                  # Failing fast
    HALF_OPEN = "half_open"        # Testing recovery
    FORCED_OPEN = "forced_open"    # Manually disabled
    DEGRADED = "degraded"          # Partial functionality


class FailureType(Enum):
    """Classification of failure types"""
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    HTTP_ERROR = "http_error"
    SERVICE_UNAVAILABLE = "service_unavailable"
    RATE_LIMITED = "rate_limited"
    AUTHENTICATION_ERROR = "authentication_error"
    UNKNOWN = "unknown"


class ServiceTier(Enum):
    """Service criticality tiers"""
    CRITICAL = "critical"      # 99.99% uptime requirement
    HIGH = "high"             # 99.9% uptime requirement
    MEDIUM = "medium"         # 99% uptime requirement
    LOW = "low"              # Best effort


@dataclass
class CircuitBreakerConfig:
    """Advanced circuit breaker configuration"""
    name: str
    service_tier: ServiceTier = ServiceTier.MEDIUM

    # Failure thresholds
    failure_threshold: int = 10
    success_threshold: int = 3
    timeout_threshold_ms: float = 5000

    # Time windows
    failure_window_seconds: int = 60
    recovery_timeout_seconds: int = 30
    health_check_interval_seconds: int = 10

    # Advanced features
    enable_adaptive_thresholds: bool = True
    enable_health_checking: bool = True
    enable_fallback: bool = True
    enable_load_balancing: bool = False

    # Monitoring and alerting
    enable_metrics: bool = True
    enable_alerting: bool = True
    alert_threshold_failures: int = 5

    # Recovery settings
    gradual_recovery: bool = True
    recovery_test_requests: int = 1
    max_recovery_attempts: int = 3


@dataclass
class ServiceCall:
    """Service call metadata"""
    call_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    service_name: str = ""
    method: str = "GET"
    url: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_ms: Optional[float] = None
    status_code: Optional[int] = None
    failure_type: Optional[FailureType] = None
    error_message: Optional[str] = None
    circuit_state: Optional[CircuitState] = None
    success: bool = False


@dataclass
class CircuitMetrics:
    """Circuit breaker performance metrics"""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    circuit_opens: int = 0
    circuit_closes: int = 0
    fallback_calls: int = 0
    avg_response_time_ms: float = 0.0
    failure_rate: float = 0.0
    uptime_percentage: float = 100.0
    last_failure: Optional[datetime] = None
    last_success: Optional[datetime] = None


class ServiceHealthChecker:
    """Health checking for circuit breaker recovery"""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout_threshold_ms / 1000)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def check_health(self, service_url: str, health_endpoint: str = "/health") -> bool:
        """Check service health"""
        if not self.session:
            return False

        try:
            health_url = f"{service_url.rstrip('/')}{health_endpoint}"
            async with self.session.get(health_url) as response:
                return response.status == 200

        except Exception as e:
            logger.debug(f"Health check failed for {service_url}: {e}")
            return False


class AdvancedCircuitBreaker:
    """
    Advanced circuit breaker with intelligent failure detection and recovery.

    Features:
    - Adaptive failure thresholds based on service tier
    - Intelligent error classification and handling
    - Health checking and gradual recovery
    - Comprehensive metrics and monitoring
    - Fallback mechanisms with degraded service
    """

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_state_change: datetime = datetime.utcnow()

        # Call tracking
        self.recent_calls: deque = deque(maxlen=1000)
        self.failure_window: deque = deque(maxlen=100)

        # Metrics
        self.metrics = CircuitMetrics()

        # Health checking
        self.health_checker: Optional[ServiceHealthChecker] = None
        self.last_health_check: Optional[datetime] = None

        # Adaptive thresholds
        self.adaptive_failure_threshold = config.failure_threshold
        self.adaptive_timeout_threshold = config.timeout_threshold_ms

        # Service endpoints for health checking
        self.service_endpoints: List[str] = []

        # Lock for thread safety
        self._lock = asyncio.Lock()

        logger.info(f"Initialized circuit breaker '{config.name}' with {config.service_tier.value} tier")

    async def call(self,
                   func: Callable[..., Awaitable[Any]],
                   *args,
                   fallback_func: Optional[Callable[..., Awaitable[Any]]] = None,
                   service_url: Optional[str] = None,
                   **kwargs) -> Any:
        """
        Execute a service call with circuit breaker protection.

        Args:
            func: The service call function
            *args: Function arguments
            fallback_func: Optional fallback function
            service_url: Service URL for health checking
            **kwargs: Function keyword arguments

        Returns:
            Result of the service call or fallback

        Raises:
            CircuitBreakerOpenException: When circuit is open and no fallback
        """
        call = ServiceCall(
            service_name=self.config.name,
            timestamp=datetime.utcnow()
        )

        # Check if call should proceed
        async with self._lock:
            if not await self._should_allow_call(call):
                if fallback_func and self.config.enable_fallback:
                    logger.warning(f"Circuit breaker open for {self.config.name}, using fallback")
                    self.metrics.fallback_calls += 1
                    return await fallback_func(*args, **kwargs)
                else:
                    raise CircuitBreakerOpenException(
                        f"Circuit breaker is {self.state.value} for service {self.config.name}"
                    )

        # Execute the call
        start_time = time.time()
        try:
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.adaptive_timeout_threshold / 1000
            )

            # Record successful call
            call.duration_ms = (time.time() - start_time) * 1000
            call.success = True
            call.circuit_state = self.state

            await self._record_success(call)
            return result

        except asyncio.TimeoutError as e:
            call.duration_ms = (time.time() - start_time) * 1000
            call.failure_type = FailureType.TIMEOUT
            call.error_message = "Request timeout"
            await self._record_failure(call)
            raise ServiceTimeoutException(f"Service call timeout after {call.duration_ms}ms") from e

        except aiohttp.ClientConnectorError as e:
            call.duration_ms = (time.time() - start_time) * 1000
            call.failure_type = FailureType.CONNECTION_ERROR
            call.error_message = str(e)
            await self._record_failure(call)
            raise ServiceConnectionException(f"Connection error: {e}") from e

        except aiohttp.ClientResponseError as e:
            call.duration_ms = (time.time() - start_time) * 1000
            call.status_code = e.status

            if e.status == 429:
                call.failure_type = FailureType.RATE_LIMITED
            elif e.status in [401, 403]:
                call.failure_type = FailureType.AUTHENTICATION_ERROR
            elif e.status >= 500:
                call.failure_type = FailureType.SERVICE_UNAVAILABLE
            else:
                call.failure_type = FailureType.HTTP_ERROR

            call.error_message = str(e)
            await self._record_failure(call)
            raise ServiceHttpException(f"HTTP {e.status}: {e}") from e

        except Exception as e:
            call.duration_ms = (time.time() - start_time) * 1000
            call.failure_type = FailureType.UNKNOWN
            call.error_message = str(e)
            await self._record_failure(call)
            raise ServiceException(f"Unexpected error: {e}") from e

    async def _should_allow_call(self, call: ServiceCall) -> bool:
        """Determine if a call should be allowed based on circuit state"""
        current_time = datetime.utcnow()

        if self.state == CircuitState.CLOSED:
            return True

        elif self.state == CircuitState.OPEN:
            # Check if we should transition to half-open
            if (self.last_failure_time and
                current_time - self.last_failure_time >= timedelta(seconds=self.config.recovery_timeout_seconds)):

                if self.config.enable_health_checking:
                    # Perform health check before transitioning
                    if await self._perform_health_check():
                        await self._transition_to_half_open()
                        return True
                    else:
                        # Extend the timeout if health check fails
                        self.last_failure_time = current_time
                        return False
                else:
                    await self._transition_to_half_open()
                    return True
            return False

        elif self.state == CircuitState.HALF_OPEN:
            # Allow limited requests to test recovery
            return self.success_count < self.config.recovery_test_requests

        elif self.state == CircuitState.FORCED_OPEN:
            return False

        elif self.state == CircuitState.DEGRADED:
            # Allow calls but with reduced functionality
            return True

        return False

    async def _record_success(self, call: ServiceCall):
        """Record a successful call"""
        async with self._lock:
            self.success_count += 1
            self.metrics.total_calls += 1
            self.metrics.successful_calls += 1
            self.metrics.last_success = call.timestamp

            # Update average response time
            self._update_average_response_time(call.duration_ms)

            # Add to recent calls
            self.recent_calls.append(call)

            # Check for state transitions
            if self.state == CircuitState.HALF_OPEN:
                if self.success_count >= self.config.success_threshold:
                    await self._transition_to_closed()

            # Adaptive threshold adjustment
            if self.config.enable_adaptive_thresholds:
                await self._adjust_adaptive_thresholds()

    async def _record_failure(self, call: ServiceCall):
        """Record a failed call"""
        async with self._lock:
            self.failure_count += 1
            self.metrics.total_calls += 1
            self.metrics.failed_calls += 1
            self.metrics.last_failure = call.timestamp
            self.last_failure_time = call.timestamp

            # Update average response time
            if call.duration_ms:
                self._update_average_response_time(call.duration_ms)

            # Add to recent calls and failure window
            self.recent_calls.append(call)
            self.failure_window.append(call)

            # Clean old failures from window
            cutoff_time = call.timestamp - timedelta(seconds=self.config.failure_window_seconds)
            while self.failure_window and self.failure_window[0].timestamp < cutoff_time:
                self.failure_window.popleft()

            # Check for state transitions
            if self.state in [CircuitState.CLOSED, CircuitState.HALF_OPEN]:
                current_failure_rate = len(self.failure_window) / max(1, self.metrics.total_calls)

                if (len(self.failure_window) >= self.adaptive_failure_threshold or
                    current_failure_rate > 0.5):  # 50% failure rate
                    await self._transition_to_open()

            # Update failure rate
            self.metrics.failure_rate = self.metrics.failed_calls / max(1, self.metrics.total_calls)

            # Adaptive threshold adjustment
            if self.config.enable_adaptive_thresholds:
                await self._adjust_adaptive_thresholds()

    def _update_average_response_time(self, duration_ms: Optional[float]):
        """Update average response time"""
        if duration_ms is not None:
            if self.metrics.avg_response_time_ms == 0:
                self.metrics.avg_response_time_ms = duration_ms
            else:
                # Exponential moving average
                alpha = 0.1
                self.metrics.avg_response_time_ms = (
                    alpha * duration_ms + (1 - alpha) * self.metrics.avg_response_time_ms
                )

    async def _transition_to_open(self):
        """Transition circuit to OPEN state"""
        self.state = CircuitState.OPEN
        self.last_state_change = datetime.utcnow()
        self.success_count = 0
        self.metrics.circuit_opens += 1

        logger.warning(f"Circuit breaker '{self.config.name}' opened due to failures")

        if self.config.enable_alerting:
            await self._send_alert("Circuit breaker opened", "warning")

    async def _transition_to_half_open(self):
        """Transition circuit to HALF_OPEN state"""
        self.state = CircuitState.HALF_OPEN
        self.last_state_change = datetime.utcnow()
        self.success_count = 0

        logger.info(f"Circuit breaker '{self.config.name}' transitioning to half-open")

    async def _transition_to_closed(self):
        """Transition circuit to CLOSED state"""
        self.state = CircuitState.CLOSED
        self.last_state_change = datetime.utcnow()
        self.failure_count = 0
        self.success_count = 0
        self.metrics.circuit_closes += 1

        logger.info(f"Circuit breaker '{self.config.name}' closed - service recovered")

        if self.config.enable_alerting:
            await self._send_alert("Circuit breaker closed - service recovered", "info")

    async def _perform_health_check(self) -> bool:
        """Perform health check on service endpoints"""
        if not self.service_endpoints or not self.config.enable_health_checking:
            return True

        current_time = datetime.utcnow()
        if (self.last_health_check and
            current_time - self.last_health_check < timedelta(seconds=self.config.health_check_interval_seconds)):
            return True  # Skip if checked recently

        self.last_health_check = current_time

        try:
            async with ServiceHealthChecker(self.config) as health_checker:
                for endpoint in self.service_endpoints:
                    if await health_checker.check_health(endpoint):
                        return True
                return False
        except Exception as e:
            logger.error(f"Health check failed for {self.config.name}: {e}")
            return False

    async def _adjust_adaptive_thresholds(self):
        """Adjust thresholds based on service performance"""
        if not self.config.enable_adaptive_thresholds:
            return

        # Adjust failure threshold based on service tier and recent performance
        base_threshold = self.config.failure_threshold

        if self.config.service_tier == ServiceTier.CRITICAL:
            # More sensitive for critical services
            self.adaptive_failure_threshold = max(3, int(base_threshold * 0.7))
        elif self.config.service_tier == ServiceTier.HIGH:
            self.adaptive_failure_threshold = max(5, int(base_threshold * 0.8))
        elif self.config.service_tier == ServiceTier.MEDIUM:
            self.adaptive_failure_threshold = base_threshold
        else:  # LOW tier
            self.adaptive_failure_threshold = int(base_threshold * 1.2)

        # Adjust timeout threshold based on recent response times
        if self.metrics.avg_response_time_ms > 0:
            if self.metrics.avg_response_time_ms > self.config.timeout_threshold_ms * 0.8:
                # If average response time is high, be more aggressive with timeouts
                self.adaptive_timeout_threshold = self.config.timeout_threshold_ms * 0.8
            else:
                # Allow normal timeout
                self.adaptive_timeout_threshold = self.config.timeout_threshold_ms

    async def _send_alert(self, message: str, severity: str):
        """Send alert about circuit breaker state change"""
        # Implementation would integrate with alerting system
        logger.info(f"Circuit breaker alert [{severity}]: {message}")

    def add_service_endpoint(self, endpoint: str):
        """Add service endpoint for health checking"""
        if endpoint not in self.service_endpoints:
            self.service_endpoints.append(endpoint)

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive circuit breaker metrics"""
        uptime_percentage = 100.0
        if self.metrics.total_calls > 0:
            uptime_percentage = (self.metrics.successful_calls / self.metrics.total_calls) * 100

        return {
            "name": self.config.name,
            "state": self.state.value,
            "service_tier": self.config.service_tier.value,
            "metrics": {
                "total_calls": self.metrics.total_calls,
                "successful_calls": self.metrics.successful_calls,
                "failed_calls": self.metrics.failed_calls,
                "failure_rate": round(self.metrics.failure_rate * 100, 2),
                "avg_response_time_ms": round(self.metrics.avg_response_time_ms, 2),
                "uptime_percentage": round(uptime_percentage, 2),
                "circuit_opens": self.metrics.circuit_opens,
                "circuit_closes": self.metrics.circuit_closes,
                "fallback_calls": self.metrics.fallback_calls
            },
            "configuration": {
                "failure_threshold": self.config.failure_threshold,
                "adaptive_failure_threshold": self.adaptive_failure_threshold,
                "timeout_threshold_ms": self.config.timeout_threshold_ms,
                "adaptive_timeout_threshold_ms": self.adaptive_timeout_threshold,
                "recovery_timeout_seconds": self.config.recovery_timeout_seconds
            },
            "recent_failures": [
                {
                    "timestamp": call.timestamp.isoformat(),
                    "failure_type": call.failure_type.value if call.failure_type else None,
                    "duration_ms": call.duration_ms,
                    "error_message": call.error_message
                }
                for call in list(self.failure_window)[-5:]  # Last 5 failures
            ],
            "last_state_change": self.last_state_change.isoformat(),
            "health_status": {
                "endpoints_configured": len(self.service_endpoints),
                "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None
            }
        }

    async def force_open(self):
        """Manually force circuit breaker to OPEN state"""
        async with self._lock:
            self.state = CircuitState.FORCED_OPEN
            self.last_state_change = datetime.utcnow()
            logger.warning(f"Circuit breaker '{self.config.name}' manually forced open")

    async def force_close(self):
        """Manually force circuit breaker to CLOSED state"""
        async with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_state_change = datetime.utcnow()
            logger.info(f"Circuit breaker '{self.config.name}' manually forced closed")

    async def reset_metrics(self):
        """Reset circuit breaker metrics"""
        async with self._lock:
            self.metrics = CircuitMetrics()
            self.recent_calls.clear()
            self.failure_window.clear()
            logger.info(f"Metrics reset for circuit breaker '{self.config.name}'")


# Circuit Breaker Registry for managing multiple circuit breakers
class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers"""

    def __init__(self):
        self.circuit_breakers: Dict[str, AdvancedCircuitBreaker] = {}
        self._lock = asyncio.Lock()

    async def register(self, name: str, config: CircuitBreakerConfig) -> AdvancedCircuitBreaker:
        """Register a new circuit breaker"""
        async with self._lock:
            if name in self.circuit_breakers:
                raise ValueError(f"Circuit breaker '{name}' already exists")

            config.name = name
            circuit_breaker = AdvancedCircuitBreaker(config)
            self.circuit_breakers[name] = circuit_breaker

            logger.info(f"Registered circuit breaker '{name}'")
            return circuit_breaker

    def get(self, name: str) -> Optional[AdvancedCircuitBreaker]:
        """Get circuit breaker by name"""
        return self.circuit_breakers.get(name)

    async def unregister(self, name: str) -> bool:
        """Unregister a circuit breaker"""
        async with self._lock:
            if name in self.circuit_breakers:
                del self.circuit_breakers[name]
                logger.info(f"Unregistered circuit breaker '{name}'")
                return True
            return False

    def list_all(self) -> List[str]:
        """List all registered circuit breaker names"""
        return list(self.circuit_breakers.keys())

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get metrics for all circuit breakers"""
        return {
            name: cb.get_metrics()
            for name, cb in self.circuit_breakers.items()
        }


# Decorator for easy circuit breaker application
def circuit_breaker(name: str,
                   config: Optional[CircuitBreakerConfig] = None,
                   fallback_func: Optional[Callable] = None):
    """Decorator to apply circuit breaker to a function"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get or create circuit breaker
            cb = _global_registry.get(name)
            if not cb:
                cb_config = config or CircuitBreakerConfig(name=name)
                cb = await _global_registry.register(name, cb_config)

            return await cb.call(func, *args, fallback_func=fallback_func, **kwargs)

        return wrapper
    return decorator


# Custom Exceptions
class CircuitBreakerException(Exception):
    """Base exception for circuit breaker errors"""
    pass


class CircuitBreakerOpenException(CircuitBreakerException):
    """Raised when circuit breaker is open"""
    pass


class ServiceException(Exception):
    """Base exception for service call errors"""
    pass


class ServiceTimeoutException(ServiceException):
    """Service call timeout"""
    pass


class ServiceConnectionException(ServiceException):
    """Service connection error"""
    pass


class ServiceHttpException(ServiceException):
    """Service HTTP error"""
    pass


# Global circuit breaker registry
_global_registry = CircuitBreakerRegistry()


def get_circuit_breaker_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry"""
    return _global_registry


# Factory functions for common configurations
def create_critical_service_config(name: str) -> CircuitBreakerConfig:
    """Create configuration for critical services (99.99% uptime)"""
    return CircuitBreakerConfig(
        name=name,
        service_tier=ServiceTier.CRITICAL,
        failure_threshold=3,
        success_threshold=5,
        timeout_threshold_ms=2000,
        failure_window_seconds=30,
        recovery_timeout_seconds=60,
        enable_adaptive_thresholds=True,
        enable_health_checking=True,
        enable_fallback=True,
        enable_alerting=True
    )


def create_high_service_config(name: str) -> CircuitBreakerConfig:
    """Create configuration for high-priority services (99.9% uptime)"""
    return CircuitBreakerConfig(
        name=name,
        service_tier=ServiceTier.HIGH,
        failure_threshold=5,
        success_threshold=3,
        timeout_threshold_ms=3000,
        failure_window_seconds=45,
        recovery_timeout_seconds=45,
        enable_adaptive_thresholds=True,
        enable_health_checking=True,
        enable_fallback=True
    )


def create_standard_service_config(name: str) -> CircuitBreakerConfig:
    """Create configuration for standard services"""
    return CircuitBreakerConfig(
        name=name,
        service_tier=ServiceTier.MEDIUM,
        failure_threshold=10,
        success_threshold=3,
        timeout_threshold_ms=5000,
        failure_window_seconds=60,
        recovery_timeout_seconds=30
    )


def create_best_effort_service_config(name: str) -> CircuitBreakerConfig:
    """Create configuration for best-effort services"""
    return CircuitBreakerConfig(
        name=name,
        service_tier=ServiceTier.LOW,
        failure_threshold=15,
        success_threshold=2,
        timeout_threshold_ms=10000,
        failure_window_seconds=120,
        recovery_timeout_seconds=20,
        enable_adaptive_thresholds=False,
        enable_health_checking=False
    )