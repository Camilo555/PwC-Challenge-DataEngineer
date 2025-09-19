"""
Advanced Circuit Breaker and Retry System
=========================================

Enterprise-grade resilience patterns including circuit breakers, retry mechanisms,
bulkheads, and adaptive timeouts for fault tolerance and system stability.
"""
from __future__ import annotations

import asyncio
import time
import random
import statistics
from abc import ABC, abstractmethod
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union, Type
import uuid

from core.logging import get_logger

logger = get_logger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class RetryStrategy(Enum):
    """Retry strategies."""
    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    RANDOM_JITTER = "random_jitter"
    ADAPTIVE = "adaptive"


class BulkheadType(Enum):
    """Bulkhead isolation types."""
    THREAD_POOL = "thread_pool"
    SEMAPHORE = "semaphore"
    RESOURCE_POOL = "resource_pool"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    recovery_timeout: int = 60  # seconds
    success_threshold: int = 3  # for half-open state
    timeout: float = 30.0  # request timeout
    expected_exception_types: List[Type[Exception]] = field(default_factory=list)
    slow_call_threshold: float = 5.0  # seconds
    slow_call_rate_threshold: float = 0.5  # 50%
    minimum_throughput: int = 10  # minimum calls before evaluating


@dataclass
class RetryConfig:
    """Retry mechanism configuration."""
    max_attempts: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    backoff_multiplier: float = 2.0
    jitter_factor: float = 0.1
    retry_on_exceptions: List[Type[Exception]] = field(default_factory=list)
    stop_on_exceptions: List[Type[Exception]] = field(default_factory=list)


@dataclass
class BulkheadConfig:
    """Bulkhead configuration."""
    bulkhead_type: BulkheadType = BulkheadType.SEMAPHORE
    max_concurrent: int = 10
    max_wait_duration: float = 30.0  # seconds
    resource_pool_size: int = 50


@dataclass
class CallResult:
    """Result of a protected call."""
    success: bool
    duration: float
    exception: Optional[Exception] = None
    attempt_number: int = 1
    circuit_state: Optional[CircuitState] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class CircuitBreakerMetrics:
    """Circuit breaker metrics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    slow_calls: int = 0
    rejected_calls: int = 0
    avg_response_time: float = 0.0
    success_rate: float = 0.0
    slow_call_rate: float = 0.0
    state: CircuitState = CircuitState.CLOSED
    last_state_change: datetime = field(default_factory=datetime.utcnow)
    last_failure: Optional[datetime] = None


class CircuitBreaker:
    """Advanced circuit breaker with metrics and adaptive behavior."""

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.next_attempt_time: float = 0
        self.call_history: deque = deque(maxlen=100)
        self.metrics = CircuitBreakerMetrics()
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        async with self._lock:
            await self._update_state()

            if self.state == CircuitState.OPEN:
                self.metrics.rejected_calls += 1
                raise CircuitBreakerOpenException(
                    f"Circuit breaker '{self.name}' is OPEN. Next attempt allowed at {self.next_attempt_time}"
                )

        # Execute the call
        start_time = time.time()
        call_result = None

        try:
            # Apply timeout
            result = await asyncio.wait_for(
                self._execute_async(func, *args, **kwargs),
                timeout=self.config.timeout
            )

            duration = time.time() - start_time
            is_slow = duration > self.config.slow_call_threshold

            call_result = CallResult(
                success=True,
                duration=duration,
                circuit_state=self.state
            )

            await self._record_success(call_result, is_slow)
            return result

        except Exception as e:
            duration = time.time() - start_time
            call_result = CallResult(
                success=False,
                duration=duration,
                exception=e,
                circuit_state=self.state
            )

            await self._record_failure(call_result)
            raise

    async def _execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function asynchronously."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)

    async def _update_state(self):
        """Update circuit breaker state based on current conditions."""
        current_time = time.time()

        if self.state == CircuitState.OPEN:
            if current_time >= self.next_attempt_time:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                self.metrics.state = self.state
                self.metrics.last_state_change = datetime.utcnow()
                logger.info(f"Circuit breaker '{self.name}' transitioned to HALF_OPEN")

        elif self.state == CircuitState.HALF_OPEN:
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.metrics.state = self.state
                self.metrics.last_state_change = datetime.utcnow()
                logger.info(f"Circuit breaker '{self.name}' transitioned to CLOSED")

    async def _record_success(self, result: CallResult, is_slow: bool):
        """Record successful call."""
        async with self._lock:
            self.call_history.append(result)
            self.metrics.total_calls += 1
            self.metrics.successful_calls += 1

            if is_slow:
                self.metrics.slow_calls += 1

            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1

            self._update_metrics()

    async def _record_failure(self, result: CallResult):
        """Record failed call."""
        async with self._lock:
            self.call_history.append(result)
            self.metrics.total_calls += 1
            self.metrics.failed_calls += 1
            self.metrics.last_failure = datetime.utcnow()

            # Check if exception should trigger circuit breaker
            if self._should_trigger_on_exception(result.exception):
                self.failure_count += 1
                self.last_failure_time = time.time()

                # Check if we should open the circuit
                if (self.failure_count >= self.config.failure_threshold and
                    self.metrics.total_calls >= self.config.minimum_throughput):

                    self.state = CircuitState.OPEN
                    self.next_attempt_time = time.time() + self.config.recovery_timeout
                    self.metrics.state = self.state
                    self.metrics.last_state_change = datetime.utcnow()
                    logger.warning(f"Circuit breaker '{self.name}' OPENED after {self.failure_count} failures")

            self._update_metrics()

    def _should_trigger_on_exception(self, exception: Optional[Exception]) -> bool:
        """Check if exception should trigger circuit breaker."""
        if not exception:
            return False

        # If specific exception types are configured, only trigger on those
        if self.config.expected_exception_types:
            return any(isinstance(exception, exc_type) for exc_type in self.config.expected_exception_types)

        # Otherwise, trigger on most exceptions (but not timeouts handled separately)
        return not isinstance(exception, (asyncio.TimeoutError, asyncio.CancelledError))

    def _update_metrics(self):
        """Update circuit breaker metrics."""
        if self.metrics.total_calls > 0:
            self.metrics.success_rate = self.metrics.successful_calls / self.metrics.total_calls
            self.metrics.slow_call_rate = self.metrics.slow_calls / self.metrics.total_calls

            # Calculate average response time from recent calls
            recent_calls = [call for call in self.call_history if call.success]
            if recent_calls:
                self.metrics.avg_response_time = statistics.mean(call.duration for call in recent_calls)

    def get_metrics(self) -> CircuitBreakerMetrics:
        """Get current circuit breaker metrics."""
        return self.metrics

    def reset(self):
        """Reset circuit breaker to initial state."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.next_attempt_time = 0
        self.call_history.clear()
        self.metrics = CircuitBreakerMetrics()
        logger.info(f"Circuit breaker '{self.name}' reset")


class RetryMechanism:
    """Advanced retry mechanism with multiple strategies."""

    def __init__(self, name: str, config: RetryConfig):
        self.name = name
        self.config = config
        self.metrics = {
            "total_attempts": 0,
            "successful_retries": 0,
            "failed_retries": 0,
            "max_attempts_exceeded": 0
        }

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic."""
        last_exception = None
        attempt = 1

        while attempt <= self.config.max_attempts:
            try:
                self.metrics["total_attempts"] += 1

                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                if attempt > 1:
                    self.metrics["successful_retries"] += 1
                    logger.info(f"Retry mechanism '{self.name}' succeeded on attempt {attempt}")

                return result

            except Exception as e:
                last_exception = e

                # Check if we should stop retrying on this exception
                if self._should_stop_on_exception(e):
                    logger.info(f"Retry mechanism '{self.name}' stopping due to exception: {type(e).__name__}")
                    break

                # Check if we should retry on this exception
                if not self._should_retry_on_exception(e):
                    logger.info(f"Retry mechanism '{self.name}' not retrying on exception: {type(e).__name__}")
                    break

                if attempt < self.config.max_attempts:
                    delay = self._calculate_delay(attempt)
                    logger.info(f"Retry mechanism '{self.name}' attempt {attempt} failed, retrying in {delay:.2f}s")
                    await asyncio.sleep(delay)
                    attempt += 1
                else:
                    self.metrics["max_attempts_exceeded"] += 1
                    logger.warning(f"Retry mechanism '{self.name}' exhausted all {self.config.max_attempts} attempts")
                    break

        self.metrics["failed_retries"] += 1
        raise last_exception

    def _should_retry_on_exception(self, exception: Exception) -> bool:
        """Check if we should retry on this exception."""
        if self.config.retry_on_exceptions:
            return any(isinstance(exception, exc_type) for exc_type in self.config.retry_on_exceptions)

        # Default: retry on most exceptions except certain types
        non_retryable = (KeyboardInterrupt, SystemExit, asyncio.CancelledError)
        return not isinstance(exception, non_retryable)

    def _should_stop_on_exception(self, exception: Exception) -> bool:
        """Check if we should stop retrying on this exception."""
        if self.config.stop_on_exceptions:
            return any(isinstance(exception, exc_type) for exc_type in self.config.stop_on_exceptions)
        return False

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay based on retry strategy."""
        if self.config.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.config.initial_delay

        elif self.config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.config.initial_delay * (self.config.backoff_multiplier ** (attempt - 1))

        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.initial_delay * attempt

        elif self.config.strategy == RetryStrategy.RANDOM_JITTER:
            base_delay = self.config.initial_delay * (self.config.backoff_multiplier ** (attempt - 1))
            jitter = base_delay * self.config.jitter_factor * random.random()
            delay = base_delay + jitter

        elif self.config.strategy == RetryStrategy.ADAPTIVE:
            # Adaptive strategy based on historical success rates
            delay = self._calculate_adaptive_delay(attempt)

        else:
            delay = self.config.initial_delay

        return min(delay, self.config.max_delay)

    def _calculate_adaptive_delay(self, attempt: int) -> float:
        """Calculate adaptive delay based on historical data."""
        # Simple adaptive logic - can be enhanced with ML
        base_delay = self.config.initial_delay * (self.config.backoff_multiplier ** (attempt - 1))

        # Adjust based on recent success rate
        success_rate = self.metrics.get("successful_retries", 0) / max(self.metrics.get("total_attempts", 1), 1)

        if success_rate < 0.3:  # Low success rate
            multiplier = 1.5
        elif success_rate > 0.7:  # High success rate
            multiplier = 0.8
        else:
            multiplier = 1.0

        return base_delay * multiplier

    def get_metrics(self) -> Dict[str, Any]:
        """Get retry mechanism metrics."""
        return self.metrics.copy()


class Bulkhead:
    """Resource isolation bulkhead pattern."""

    def __init__(self, name: str, config: BulkheadConfig):
        self.name = name
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.active_calls = 0
        self.queued_calls = 0
        self.rejected_calls = 0
        self.total_calls = 0
        self._lock = asyncio.Lock()

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with bulkhead protection."""
        async with self._lock:
            self.total_calls += 1

        # Try to acquire semaphore with timeout
        try:
            await asyncio.wait_for(
                self.semaphore.acquire(),
                timeout=self.config.max_wait_duration
            )
        except asyncio.TimeoutError:
            async with self._lock:
                self.rejected_calls += 1
            raise BulkheadRejectedException(f"Bulkhead '{self.name}' rejected call - no resources available")

        try:
            async with self._lock:
                self.active_calls += 1

            # Execute the function
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            return result

        finally:
            async with self._lock:
                self.active_calls -= 1
            self.semaphore.release()

    def get_metrics(self) -> Dict[str, Any]:
        """Get bulkhead metrics."""
        return {
            "active_calls": self.active_calls,
            "queued_calls": self.queued_calls,
            "rejected_calls": self.rejected_calls,
            "total_calls": self.total_calls,
            "max_concurrent": self.config.max_concurrent,
            "utilization": self.active_calls / self.config.max_concurrent
        }


class ResilienceDecorator:
    """Comprehensive resilience decorator combining multiple patterns."""

    def __init__(
        self,
        name: str,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        bulkhead_config: Optional[BulkheadConfig] = None,
        timeout: Optional[float] = None
    ):
        self.name = name
        self.circuit_breaker = CircuitBreaker(name, circuit_breaker_config) if circuit_breaker_config else None
        self.retry_mechanism = RetryMechanism(name, retry_config) if retry_config else None
        self.bulkhead = Bulkhead(name, bulkhead_config) if bulkhead_config else None
        self.timeout = timeout

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with all enabled resilience patterns."""

        async def wrapped_func():
            # Apply circuit breaker if enabled
            if self.circuit_breaker:
                return await self.circuit_breaker.call(func, *args, **kwargs)
            else:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

        async def retry_wrapped_func():
            # Apply retry mechanism if enabled
            if self.retry_mechanism:
                return await self.retry_mechanism.execute(wrapped_func)
            else:
                return await wrapped_func()

        async def bulkhead_wrapped_func():
            # Apply bulkhead if enabled
            if self.bulkhead:
                return await self.bulkhead.execute(retry_wrapped_func)
            else:
                return await retry_wrapped_func()

        # Apply timeout if specified
        if self.timeout:
            try:
                return await asyncio.wait_for(bulkhead_wrapped_func(), timeout=self.timeout)
            except asyncio.TimeoutError:
                raise TimeoutException(f"Function call timed out after {self.timeout} seconds")
        else:
            return await bulkhead_wrapped_func()

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive metrics from all components."""
        metrics = {"name": self.name}

        if self.circuit_breaker:
            metrics["circuit_breaker"] = self.circuit_breaker.get_metrics()

        if self.retry_mechanism:
            metrics["retry"] = self.retry_mechanism.get_metrics()

        if self.bulkhead:
            metrics["bulkhead"] = self.bulkhead.get_metrics()

        return metrics


class ResilienceManager:
    """Manager for resilience patterns across the application."""

    def __init__(self):
        self.decorators: Dict[str, ResilienceDecorator] = {}
        self.global_metrics = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "circuit_breaker_openings": 0,
            "retry_exhaustions": 0,
            "bulkhead_rejections": 0
        }

    def register_resilience_pattern(
        self,
        name: str,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        bulkhead_config: Optional[BulkheadConfig] = None,
        timeout: Optional[float] = None
    ) -> ResilienceDecorator:
        """Register a resilience pattern."""

        decorator = ResilienceDecorator(
            name=name,
            circuit_breaker_config=circuit_breaker_config,
            retry_config=retry_config,
            bulkhead_config=bulkhead_config,
            timeout=timeout
        )

        self.decorators[name] = decorator
        logger.info(f"Registered resilience pattern: {name}")
        return decorator

    def get_pattern(self, name: str) -> Optional[ResilienceDecorator]:
        """Get resilience pattern by name."""
        return self.decorators.get(name)

    async def execute_with_resilience(self, pattern_name: str, func: Callable, *args, **kwargs) -> Any:
        """Execute function with specified resilience pattern."""
        decorator = self.decorators.get(pattern_name)
        if not decorator:
            raise ValueError(f"Resilience pattern '{pattern_name}' not found")

        try:
            self.global_metrics["total_calls"] += 1
            result = await decorator.execute(func, *args, **kwargs)
            self.global_metrics["successful_calls"] += 1
            return result

        except CircuitBreakerOpenException:
            self.global_metrics["circuit_breaker_openings"] += 1
            self.global_metrics["failed_calls"] += 1
            raise

        except BulkheadRejectedException:
            self.global_metrics["bulkhead_rejections"] += 1
            self.global_metrics["failed_calls"] += 1
            raise

        except Exception:
            self.global_metrics["failed_calls"] += 1
            raise

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get metrics from all registered patterns."""
        metrics = {
            "global": self.global_metrics.copy(),
            "patterns": {}
        }

        for name, decorator in self.decorators.items():
            metrics["patterns"][name] = decorator.get_metrics()

        return metrics

    def create_database_resilience_pattern(self) -> ResilienceDecorator:
        """Create resilience pattern optimized for database operations."""
        circuit_config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=30,
            timeout=10.0,
            expected_exception_types=[ConnectionError, TimeoutError]
        )

        retry_config = RetryConfig(
            max_attempts=3,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            initial_delay=1.0,
            max_delay=10.0,
            retry_on_exceptions=[ConnectionError, TimeoutError]
        )

        bulkhead_config = BulkheadConfig(
            max_concurrent=20,
            max_wait_duration=5.0
        )

        return self.register_resilience_pattern(
            name="database_operations",
            circuit_breaker_config=circuit_config,
            retry_config=retry_config,
            bulkhead_config=bulkhead_config,
            timeout=15.0
        )

    def create_external_api_resilience_pattern(self) -> ResilienceDecorator:
        """Create resilience pattern optimized for external API calls."""
        circuit_config = CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=60,
            timeout=30.0,
            slow_call_threshold=2.0
        )

        retry_config = RetryConfig(
            max_attempts=4,
            strategy=RetryStrategy.RANDOM_JITTER,
            initial_delay=2.0,
            max_delay=30.0,
            jitter_factor=0.2
        )

        bulkhead_config = BulkheadConfig(
            max_concurrent=10,
            max_wait_duration=10.0
        )

        return self.register_resilience_pattern(
            name="external_api_calls",
            circuit_breaker_config=circuit_config,
            retry_config=retry_config,
            bulkhead_config=bulkhead_config,
            timeout=45.0
        )


# Custom exceptions
class CircuitBreakerOpenException(Exception):
    """Raised when circuit breaker is open."""
    pass


class BulkheadRejectedException(Exception):
    """Raised when bulkhead rejects a call."""
    pass


class TimeoutException(Exception):
    """Raised when operation times out."""
    pass


# Decorator functions for easy use
def circuit_breaker(config: CircuitBreakerConfig):
    """Decorator for circuit breaker pattern."""
    def decorator(func):
        cb = CircuitBreaker(func.__name__, config)

        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                return await cb.call(func, *args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                return asyncio.run(cb.call(func, *args, **kwargs))
            return sync_wrapper

    return decorator


def retry(config: RetryConfig):
    """Decorator for retry pattern."""
    def decorator(func):
        retry_mechanism = RetryMechanism(func.__name__, config)

        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                return await retry_mechanism.execute(func, *args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                return asyncio.run(retry_mechanism.execute(func, *args, **kwargs))
            return sync_wrapper

    return decorator


def bulkhead(config: BulkheadConfig):
    """Decorator for bulkhead pattern."""
    def decorator(func):
        bh = Bulkhead(func.__name__, config)

        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                return await bh.execute(func, *args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                return asyncio.run(bh.execute(func, *args, **kwargs))
            return sync_wrapper

    return decorator


# Global resilience manager
resilience_manager = ResilienceManager()

# Create default patterns
resilience_manager.create_database_resilience_pattern()
resilience_manager.create_external_api_resilience_pattern()