"""
Enhanced Error Handling and Retry Mechanisms for ETL Processors
Provides comprehensive retry logic, circuit breakers, and fault tolerance.
"""

import random
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any, TypeVar

from core.logging import get_logger

T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])


class RetryStrategy(Enum):
    """Different retry strategies"""
    FIXED_INTERVAL = "fixed_interval"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    RANDOM_JITTER = "random_jitter"
    FIBONACCI = "fibonacci"


class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Categories of errors for different handling strategies"""
    TRANSIENT = "transient"           # Network timeouts, temporary resource unavailability
    RESOURCE = "resource"             # Out of memory, disk space
    DATA_QUALITY = "data_quality"     # Bad data format, validation failures
    CONFIGURATION = "configuration"   # Invalid config, missing dependencies
    EXTERNAL_SERVICE = "external_service"  # External API failures
    INFRASTRUCTURE = "infrastructure" # Database connection, file system
    BUSINESS_LOGIC = "business_logic" # Domain-specific validation errors
    UNKNOWN = "unknown"               # Unclassified errors


@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_attempts: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    jitter_max: float = 0.1
    retryable_exceptions: list[type[Exception]] = field(default_factory=lambda: [Exception])
    non_retryable_exceptions: list[type[Exception]] = field(default_factory=list)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    expected_exception: type[Exception] = Exception


@dataclass
class ErrorRecord:
    """Record of an error occurrence"""
    timestamp: datetime
    exception: Exception
    category: ErrorCategory
    severity: ErrorSeverity
    context: dict[str, Any]
    retry_attempt: int = 0


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, blocking calls
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker implementation for fault tolerance"""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: datetime | None = None
        self.lock = threading.Lock()
        self.logger = get_logger(f"CircuitBreaker-{id(self)}")

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Call a function through the circuit breaker"""

        with self.lock:
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise Exception("Circuit breaker is OPEN. Service unavailable.")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except self.config.expected_exception:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True

        return (datetime.now() - self.last_failure_time).total_seconds() >= self.config.recovery_timeout

    def _on_success(self):
        """Handle successful call"""
        with self.lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.logger.info("Circuit breaker reset to CLOSED")

    def _on_failure(self):
        """Handle failed call"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()

            if self.failure_count >= self.config.failure_threshold:
                if self.state != CircuitBreakerState.OPEN:
                    self.state = CircuitBreakerState.OPEN
                    self.logger.warning(f"Circuit breaker opened after {self.failure_count} failures")


class ErrorClassifier:
    """Classifies errors into categories and determines severity"""

    def __init__(self):
        self.classification_rules: dict[type[Exception], ErrorCategory] = {
            ConnectionError: ErrorCategory.INFRASTRUCTURE,
            TimeoutError: ErrorCategory.TRANSIENT,
            MemoryError: ErrorCategory.RESOURCE,
            FileNotFoundError: ErrorCategory.CONFIGURATION,
            ValueError: ErrorCategory.DATA_QUALITY,
            KeyError: ErrorCategory.DATA_QUALITY,
            ImportError: ErrorCategory.CONFIGURATION,
        }

        self.severity_rules: dict[ErrorCategory, ErrorSeverity] = {
            ErrorCategory.RESOURCE: ErrorSeverity.HIGH,
            ErrorCategory.INFRASTRUCTURE: ErrorSeverity.HIGH,
            ErrorCategory.CONFIGURATION: ErrorSeverity.MEDIUM,
            ErrorCategory.DATA_QUALITY: ErrorSeverity.MEDIUM,
            ErrorCategory.TRANSIENT: ErrorSeverity.LOW,
            ErrorCategory.EXTERNAL_SERVICE: ErrorSeverity.MEDIUM,
            ErrorCategory.BUSINESS_LOGIC: ErrorSeverity.LOW,
            ErrorCategory.UNKNOWN: ErrorSeverity.MEDIUM,
        }

    def classify_error(self, exception: Exception) -> ErrorCategory:
        """Classify an exception into a category"""

        exception_type = type(exception)

        # Direct match
        if exception_type in self.classification_rules:
            return self.classification_rules[exception_type]

        # Check inheritance
        for exc_type, category in self.classification_rules.items():
            if issubclass(exception_type, exc_type):
                return category

        # Check error message patterns
        error_msg = str(exception).lower()
        if any(keyword in error_msg for keyword in ['timeout', 'connection', 'network']):
            return ErrorCategory.TRANSIENT
        elif any(keyword in error_msg for keyword in ['memory', 'space', 'disk']):
            return ErrorCategory.RESOURCE
        elif any(keyword in error_msg for keyword in ['config', 'setting', 'parameter']):
            return ErrorCategory.CONFIGURATION

        return ErrorCategory.UNKNOWN

    def get_severity(self, category: ErrorCategory) -> ErrorSeverity:
        """Get severity level for an error category"""
        return self.severity_rules.get(category, ErrorSeverity.MEDIUM)


class RetryHandler:
    """Handles retry logic with various strategies"""

    def __init__(self, config: RetryConfig):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number"""

        if self.config.strategy == RetryStrategy.FIXED_INTERVAL:
            delay = self.config.base_delay

        elif self.config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.config.base_delay * (self.config.backoff_multiplier ** (attempt - 1))

        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.base_delay * attempt

        elif self.config.strategy == RetryStrategy.FIBONACCI:
            delay = self.config.base_delay * self._fibonacci(attempt)

        elif self.config.strategy == RetryStrategy.RANDOM_JITTER:
            delay = self.config.base_delay + random.uniform(0, self.config.base_delay)

        else:
            delay = self.config.base_delay

        # Apply max delay limit
        delay = min(delay, self.config.max_delay)

        # Add jitter if enabled
        if self.config.jitter:
            jitter = random.uniform(-self.config.jitter_max, self.config.jitter_max) * delay
            delay += jitter

        return max(0, delay)

    def _fibonacci(self, n: int) -> int:
        """Calculate fibonacci number for retry strategy"""
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        return b

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if an exception should trigger a retry"""

        if attempt >= self.config.max_attempts:
            return False

        # Check non-retryable exceptions first
        for exc_type in self.config.non_retryable_exceptions:
            if isinstance(exception, exc_type):
                return False

        # Check retryable exceptions
        for exc_type in self.config.retryable_exceptions:
            if isinstance(exception, exc_type):
                return True

        return False


class ErrorTracker:
    """Tracks and analyzes error patterns"""

    def __init__(self, max_history: int = 1000):
        self.max_history = max_history
        self.error_history: deque = deque(maxlen=max_history)
        self.error_counts: dict[ErrorCategory, int] = defaultdict(int)
        self.severity_counts: dict[ErrorSeverity, int] = defaultdict(int)
        self.hourly_counts: dict[int, int] = defaultdict(int)
        self.lock = threading.Lock()

    def record_error(self, error_record: ErrorRecord):
        """Record an error occurrence"""
        with self.lock:
            self.error_history.append(error_record)
            self.error_counts[error_record.category] += 1
            self.severity_counts[error_record.severity] += 1
            self.hourly_counts[error_record.timestamp.hour] += 1

    def get_error_rate(self, time_window_minutes: int = 60) -> float:
        """Calculate error rate for a time window"""
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)

        with self.lock:
            recent_errors = [
                record for record in self.error_history
                if record.timestamp >= cutoff_time
            ]

        return len(recent_errors) / max(time_window_minutes, 1)

    def get_error_summary(self) -> dict[str, Any]:
        """Get summary of error patterns"""
        with self.lock:
            return {
                "total_errors": len(self.error_history),
                "by_category": dict(self.error_counts),
                "by_severity": dict(self.severity_counts),
                "by_hour": dict(self.hourly_counts),
                "recent_rate": self.get_error_rate(60)
            }


class ResilientExecutor:
    """Main executor that combines all resilience mechanisms"""

    def __init__(
        self,
        retry_config: RetryConfig | None = None,
        circuit_breaker_config: CircuitBreakerConfig | None = None,
        error_tracker: ErrorTracker | None = None
    ):
        self.retry_config = retry_config or RetryConfig()
        self.retry_handler = RetryHandler(self.retry_config)

        self.circuit_breaker = None
        if circuit_breaker_config:
            self.circuit_breaker = CircuitBreaker(circuit_breaker_config)

        self.error_tracker = error_tracker or ErrorTracker()
        self.error_classifier = ErrorClassifier()
        self.logger = get_logger(self.__class__.__name__)

    def execute(
        self,
        func: Callable[..., T],
        *args,
        context: dict[str, Any] | None = None,
        **kwargs
    ) -> T:
        """Execute a function with full resilience mechanisms"""

        context = context or {}
        last_exception = None

        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                # Use circuit breaker if configured
                if self.circuit_breaker:
                    result = self.circuit_breaker.call(func, *args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                if attempt > 1:
                    self.logger.info(f"Function succeeded on attempt {attempt}")

                return result

            except Exception as e:
                last_exception = e

                # Classify and record error
                category = self.error_classifier.classify_error(e)
                severity = self.error_classifier.get_severity(category)

                error_record = ErrorRecord(
                    timestamp=datetime.now(),
                    exception=e,
                    category=category,
                    severity=severity,
                    context=context,
                    retry_attempt=attempt
                )

                self.error_tracker.record_error(error_record)

                # Check if we should retry
                if attempt < self.retry_config.max_attempts and self.retry_handler.should_retry(e, attempt):
                    delay = self.retry_handler.calculate_delay(attempt)

                    self.logger.warning(
                        f"Attempt {attempt} failed: {e}. "
                        f"Retrying in {delay:.2f}s (category: {category.value}, severity: {severity.value})"
                    )

                    time.sleep(delay)
                    continue
                else:
                    self.logger.error(
                        f"All retry attempts exhausted. Final error: {e} "
                        f"(category: {category.value}, severity: {severity.value})"
                    )
                    break

        # All retries exhausted
        raise last_exception


def resilient(
    max_attempts: int = 3,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: list[type[Exception]] | None = None,
    non_retryable_exceptions: list[type[Exception]] | None = None,
    use_circuit_breaker: bool = False,
    circuit_breaker_threshold: int = 5
) -> Callable[[F], F]:
    """Decorator for making functions resilient"""

    def decorator(func: F) -> F:
        retry_config = RetryConfig(
            max_attempts=max_attempts,
            strategy=strategy,
            base_delay=base_delay,
            max_delay=max_delay,
            retryable_exceptions=retryable_exceptions or [Exception],
            non_retryable_exceptions=non_retryable_exceptions or []
        )

        circuit_config = None
        if use_circuit_breaker:
            circuit_config = CircuitBreakerConfig(
                failure_threshold=circuit_breaker_threshold
            )

        executor = ResilientExecutor(
            retry_config=retry_config,
            circuit_breaker_config=circuit_config
        )

        @wraps(func)
        def wrapper(*args, **kwargs):
            return executor.execute(func, *args, **kwargs)

        # Attach executor for introspection
        wrapper._resilient_executor = executor

        return wrapper

    return decorator


class BulkResilientExecutor:
    """Executor for bulk operations with partial failure handling"""

    def __init__(self, resilient_executor: ResilientExecutor):
        self.executor = resilient_executor
        self.logger = get_logger(self.__class__.__name__)

    def execute_batch(
        self,
        func: Callable[[Any], Any],
        items: list[Any],
        max_failures: int | None = None,
        fail_fast: bool = False
    ) -> dict[str, Any]:
        """Execute function on batch of items with failure tolerance"""

        max_failures = max_failures or len(items) // 2  # Allow 50% failures by default

        results = []
        failures = []

        for i, item in enumerate(items):
            try:
                result = self.executor.execute(func, item, context={"batch_index": i})
                results.append({"index": i, "item": item, "result": result, "success": True})

            except Exception as e:
                failure_record = {
                    "index": i,
                    "item": item,
                    "error": str(e),
                    "exception_type": type(e).__name__,
                    "success": False
                }
                failures.append(failure_record)

                self.logger.error(f"Item {i} failed: {e}")

                # Check failure threshold
                if len(failures) > max_failures:
                    self.logger.error(f"Exceeded maximum failures ({max_failures})")
                    if fail_fast:
                        break

        success_rate = len(results) / len(items) if items else 0

        return {
            "total_items": len(items),
            "successful": len(results),
            "failed": len(failures),
            "success_rate": success_rate,
            "results": results,
            "failures": failures,
            "error_summary": self.executor.error_tracker.get_error_summary()
        }


# Convenience functions
def create_resilient_executor(
    max_attempts: int = 3,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
    use_circuit_breaker: bool = False
) -> ResilientExecutor:
    """Create a configured resilient executor"""

    retry_config = RetryConfig(
        max_attempts=max_attempts,
        strategy=strategy
    )

    circuit_config = None
    if use_circuit_breaker:
        circuit_config = CircuitBreakerConfig()

    return ResilientExecutor(retry_config, circuit_config)
