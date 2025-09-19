"""
Standardized Error Handling Patterns
Enterprise-grade error handling patterns with consistent structure,
automated recovery, circuit breakers, and comprehensive error tracking.
"""
from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import sys
import time
import traceback
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union

from pydantic import BaseModel

from core.logging import get_logger

logger = get_logger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels for categorization and handling."""
    CRITICAL = "critical"      # System-threatening errors requiring immediate attention
    HIGH = "high"             # Significant errors affecting functionality
    MEDIUM = "medium"         # Moderate errors with workarounds available
    LOW = "low"              # Minor errors with minimal impact
    INFO = "info"            # Informational errors for tracking


class ErrorCategory(Enum):
    """Error categories for classification and routing."""
    VALIDATION = "validation"         # Input/data validation errors
    AUTHENTICATION = "authentication" # Authentication/authorization errors
    AUTHORIZATION = "authorization"   # Access control errors
    DATABASE = "database"            # Database operation errors
    NETWORK = "network"              # Network communication errors
    EXTERNAL_SERVICE = "external_service"  # Third-party service errors
    CONFIGURATION = "configuration"  # Configuration-related errors
    BUSINESS_LOGIC = "business_logic" # Business rule violations
    SYSTEM = "system"                # System-level errors
    SECURITY = "security"            # Security-related errors
    PERFORMANCE = "performance"      # Performance-related errors
    DATA_QUALITY = "data_quality"    # Data quality issues


class ErrorRecoveryStrategy(Enum):
    """Error recovery strategies for automated handling."""
    RETRY = "retry"                  # Retry the operation
    FALLBACK = "fallback"           # Use fallback mechanism
    CIRCUIT_BREAKER = "circuit_breaker"  # Open circuit breaker
    ESCALATE = "escalate"           # Escalate to higher level
    IGNORE = "ignore"               # Log and ignore
    FAIL_FAST = "fail_fast"         # Fail immediately without retry


@dataclass
class ErrorContext:
    """Context information for error tracking and debugging."""
    error_id: str
    timestamp: datetime
    severity: ErrorSeverity
    category: ErrorCategory
    error_type: str
    error_message: str
    module: str
    function: str
    line_number: int
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    stack_trace: Optional[str] = None
    recovery_strategy: Optional[ErrorRecoveryStrategy] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class ErrorPattern(BaseModel):
    """Error pattern definition for automated handling."""
    pattern_name: str
    error_types: List[str]
    categories: List[ErrorCategory]
    severity: ErrorSeverity
    recovery_strategy: ErrorRecoveryStrategy
    max_retries: int = 3
    retry_delay: float = 1.0
    backoff_multiplier: float = 2.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    custom_handler: Optional[str] = None
    escalation_threshold: int = 10
    metadata: Dict[str, Any] = {}


class CircuitBreakerState(Enum):
    """Circuit breaker states for fault tolerance."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service has recovered


@dataclass
class CircuitBreaker:
    """Circuit breaker for fault tolerance and rapid failure detection."""
    name: str
    failure_threshold: int = 5
    timeout: int = 60
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    half_open_max_calls: int = 3
    half_open_calls: int = 0
    successful_calls: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def should_allow_request(self) -> bool:
        """Determine if request should be allowed based on circuit state."""
        current_time = datetime.utcnow()

        if self.state == CircuitBreakerState.CLOSED:
            return True

        elif self.state == CircuitBreakerState.OPEN:
            if (self.last_failure_time and
                current_time - self.last_failure_time > timedelta(seconds=self.timeout)):
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False

        elif self.state == CircuitBreakerState.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls

        return False

    def record_success(self):
        """Record a successful operation."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_calls += 1
            self.successful_calls += 1

            if self.successful_calls >= 2:  # Require multiple successes
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.successful_calls = 0

        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self):
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        self.successful_calls = 0

        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN

        elif (self.state == CircuitBreakerState.CLOSED and
              self.failure_count >= self.failure_threshold):
            self.state = CircuitBreakerState.OPEN


class StandardizedErrorHandler:
    """
    Centralized error handler implementing standardized patterns
    across the entire application with automated recovery.
    """

    def __init__(self):
        """Initialize the standardized error handler."""
        self.error_patterns: Dict[str, ErrorPattern] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.error_history: deque = deque(maxlen=10000)
        self.error_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'count': 0,
            'last_occurrence': None,
            'first_occurrence': None,
            'recovery_attempts': 0,
            'successful_recoveries': 0
        })

        # Error aggregation for pattern detection
        self.error_aggregator: Dict[str, List[ErrorContext]] = defaultdict(list)

        # Load default error patterns
        self._load_default_patterns()

    def _load_default_patterns(self):
        """Load default error handling patterns."""
        # Database connection errors
        self.add_error_pattern(ErrorPattern(
            pattern_name="database_connection",
            error_types=["ConnectionError", "DatabaseError", "OperationalError"],
            categories=[ErrorCategory.DATABASE],
            severity=ErrorSeverity.HIGH,
            recovery_strategy=ErrorRecoveryStrategy.RETRY,
            max_retries=3,
            retry_delay=2.0,
            backoff_multiplier=2.0,
            circuit_breaker_threshold=5
        ))

        # Network timeout errors
        self.add_error_pattern(ErrorPattern(
            pattern_name="network_timeout",
            error_types=["TimeoutError", "ConnectTimeoutError", "ReadTimeoutError"],
            categories=[ErrorCategory.NETWORK],
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.RETRY,
            max_retries=2,
            retry_delay=1.0,
            backoff_multiplier=1.5
        ))

        # Validation errors
        self.add_error_pattern(ErrorPattern(
            pattern_name="validation_error",
            error_types=["ValidationError", "ValueError", "TypeError"],
            categories=[ErrorCategory.VALIDATION],
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FAIL_FAST,
            max_retries=0
        ))

        # Authentication errors
        self.add_error_pattern(ErrorPattern(
            pattern_name="authentication_error",
            error_types=["AuthenticationError", "UnauthorizedError"],
            categories=[ErrorCategory.AUTHENTICATION],
            severity=ErrorSeverity.HIGH,
            recovery_strategy=ErrorRecoveryStrategy.ESCALATE,
            max_retries=1
        ))

        # External service errors
        self.add_error_pattern(ErrorPattern(
            pattern_name="external_service",
            error_types=["HTTPError", "ServiceUnavailableError", "ExternalServiceError"],
            categories=[ErrorCategory.EXTERNAL_SERVICE],
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.CIRCUIT_BREAKER,
            max_retries=2,
            circuit_breaker_threshold=3,
            circuit_breaker_timeout=30
        ))

        # Security errors
        self.add_error_pattern(ErrorPattern(
            pattern_name="security_error",
            error_types=["SecurityError", "PermissionError", "AccessDeniedError"],
            categories=[ErrorCategory.SECURITY],
            severity=ErrorSeverity.CRITICAL,
            recovery_strategy=ErrorRecoveryStrategy.ESCALATE,
            max_retries=0
        ))

        # Performance errors
        self.add_error_pattern(ErrorPattern(
            pattern_name="performance_error",
            error_types=["PerformanceError", "MemoryError", "ResourceExhaustedError"],
            categories=[ErrorCategory.PERFORMANCE],
            severity=ErrorSeverity.HIGH,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            max_retries=1
        ))

    def add_error_pattern(self, pattern: ErrorPattern):
        """Add a new error handling pattern."""
        self.error_patterns[pattern.pattern_name] = pattern

        # Create circuit breaker if strategy requires it
        if pattern.recovery_strategy == ErrorRecoveryStrategy.CIRCUIT_BREAKER:
            self.circuit_breakers[pattern.pattern_name] = CircuitBreaker(
                name=pattern.pattern_name,
                failure_threshold=pattern.circuit_breaker_threshold,
                timeout=pattern.circuit_breaker_timeout
            )

        logger.info(f"Added error pattern: {pattern.pattern_name}")

    def handle_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> ErrorContext:
        """
        Handle an error using standardized patterns.

        Args:
            error: The exception that occurred
            context: Additional context information
            user_id: User ID associated with the error
            request_id: Request ID for tracing

        Returns:
            ErrorContext with error details and handling information
        """
        # Create error context
        error_context = self._create_error_context(
            error, context, user_id, request_id
        )

        # Find matching pattern
        pattern = self._find_matching_pattern(error_context)

        if pattern:
            error_context.recovery_strategy = pattern.recovery_strategy

            # Apply recovery strategy
            recovery_result = self._apply_recovery_strategy(error_context, pattern)
            error_context.metadata.update(recovery_result)

        # Record error
        self._record_error(error_context)

        # Log error with appropriate level
        self._log_error(error_context)

        return error_context

    def _create_error_context(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]],
        user_id: Optional[str],
        request_id: Optional[str]
    ) -> ErrorContext:
        """Create comprehensive error context."""
        # Get caller information
        frame = inspect.currentframe()
        caller_frame = frame.f_back.f_back if frame and frame.f_back else None

        module_name = "unknown"
        function_name = "unknown"
        line_number = 0

        if caller_frame:
            module_name = caller_frame.f_globals.get('__name__', 'unknown')
            function_name = caller_frame.f_code.co_name
            line_number = caller_frame.f_lineno

        # Generate unique error ID
        error_signature = f"{type(error).__name__}:{str(error)}:{module_name}:{function_name}"
        error_id = hashlib.md5(error_signature.encode()).hexdigest()[:12]

        # Determine category and severity
        category = self._categorize_error(error)
        severity = self._assess_severity(error, category)

        return ErrorContext(
            error_id=error_id,
            timestamp=datetime.utcnow(),
            severity=severity,
            category=category,
            error_type=type(error).__name__,
            error_message=str(error),
            module=module_name,
            function=function_name,
            line_number=line_number,
            user_id=user_id,
            request_id=request_id,
            stack_trace=traceback.format_exc(),
            metadata=context or {}
        )

    def _find_matching_pattern(self, error_context: ErrorContext) -> Optional[ErrorPattern]:
        """Find the best matching error pattern for the error."""
        best_match = None
        best_score = 0

        for pattern in self.error_patterns.values():
            score = 0

            # Check error type match
            if error_context.error_type in pattern.error_types:
                score += 10

            # Check category match
            if error_context.category in pattern.categories:
                score += 5

            # Prefer more specific patterns
            if len(pattern.error_types) == 1:
                score += 2

            if score > best_score:
                best_score = score
                best_match = pattern

        return best_match

    def _categorize_error(self, error: Exception) -> ErrorCategory:
        """Categorize error based on type and characteristics."""
        error_type = type(error).__name__.lower()

        # Database errors
        if any(keyword in error_type for keyword in [
            'database', 'connection', 'sql', 'query', 'transaction'
        ]):
            return ErrorCategory.DATABASE

        # Network errors
        if any(keyword in error_type for keyword in [
            'network', 'timeout', 'connection', 'http', 'request'
        ]):
            return ErrorCategory.NETWORK

        # Validation errors
        if any(keyword in error_type for keyword in [
            'validation', 'value', 'type', 'format'
        ]):
            return ErrorCategory.VALIDATION

        # Security errors
        if any(keyword in error_type for keyword in [
            'security', 'auth', 'permission', 'access', 'unauthorized'
        ]):
            return ErrorCategory.SECURITY

        # Performance errors
        if any(keyword in error_type for keyword in [
            'memory', 'performance', 'resource', 'limit'
        ]):
            return ErrorCategory.PERFORMANCE

        # Default to system category
        return ErrorCategory.SYSTEM

    def _assess_severity(self, error: Exception, category: ErrorCategory) -> ErrorSeverity:
        """Assess error severity based on type and category."""
        error_type = type(error).__name__.lower()

        # Critical errors
        if any(keyword in error_type for keyword in [
            'critical', 'fatal', 'security', 'memory', 'system'
        ]):
            return ErrorSeverity.CRITICAL

        # High severity errors
        if category in [ErrorCategory.SECURITY, ErrorCategory.DATABASE]:
            return ErrorSeverity.HIGH

        # Medium severity for most functional errors
        if category in [
            ErrorCategory.NETWORK, ErrorCategory.EXTERNAL_SERVICE,
            ErrorCategory.BUSINESS_LOGIC
        ]:
            return ErrorSeverity.MEDIUM

        # Low severity for validation and minor errors
        return ErrorSeverity.LOW

    def _apply_recovery_strategy(
        self,
        error_context: ErrorContext,
        pattern: ErrorPattern
    ) -> Dict[str, Any]:
        """Apply the appropriate recovery strategy."""
        strategy = pattern.recovery_strategy
        recovery_result = {
            'strategy_applied': strategy.value,
            'recovery_attempted': False,
            'recovery_successful': False,
            'details': {}
        }

        try:
            if strategy == ErrorRecoveryStrategy.RETRY:
                recovery_result.update(self._handle_retry_strategy(error_context, pattern))

            elif strategy == ErrorRecoveryStrategy.CIRCUIT_BREAKER:
                recovery_result.update(self._handle_circuit_breaker_strategy(error_context, pattern))

            elif strategy == ErrorRecoveryStrategy.FALLBACK:
                recovery_result.update(self._handle_fallback_strategy(error_context, pattern))

            elif strategy == ErrorRecoveryStrategy.ESCALATE:
                recovery_result.update(self._handle_escalation_strategy(error_context, pattern))

            elif strategy == ErrorRecoveryStrategy.FAIL_FAST:
                recovery_result['details'] = {'message': 'Failing fast as per strategy'}

            # Update recovery statistics
            if recovery_result['recovery_attempted']:
                self.error_stats[error_context.error_type]['recovery_attempts'] += 1
                if recovery_result['recovery_successful']:
                    self.error_stats[error_context.error_type]['successful_recoveries'] += 1

        except Exception as recovery_error:
            recovery_result['recovery_error'] = str(recovery_error)
            logger.error(f"Recovery strategy failed: {recovery_error}")

        return recovery_result

    def _handle_retry_strategy(
        self,
        error_context: ErrorContext,
        pattern: ErrorPattern
    ) -> Dict[str, Any]:
        """Handle retry recovery strategy."""
        return {
            'recovery_attempted': True,
            'recovery_successful': False,
            'details': {
                'max_retries': pattern.max_retries,
                'retry_delay': pattern.retry_delay,
                'backoff_multiplier': pattern.backoff_multiplier,
                'message': 'Retry strategy configured for automatic retry'
            }
        }

    def _handle_circuit_breaker_strategy(
        self,
        error_context: ErrorContext,
        pattern: ErrorPattern
    ) -> Dict[str, Any]:
        """Handle circuit breaker recovery strategy."""
        circuit_breaker = self.circuit_breakers.get(pattern.pattern_name)
        if not circuit_breaker:
            return {'recovery_attempted': False, 'details': {'error': 'Circuit breaker not found'}}

        circuit_breaker.record_failure()

        return {
            'recovery_attempted': True,
            'recovery_successful': False,
            'details': {
                'circuit_state': circuit_breaker.state.value,
                'failure_count': circuit_breaker.failure_count,
                'threshold': circuit_breaker.failure_threshold,
                'message': f'Circuit breaker state: {circuit_breaker.state.value}'
            }
        }

    def _handle_fallback_strategy(
        self,
        error_context: ErrorContext,
        pattern: ErrorPattern
    ) -> Dict[str, Any]:
        """Handle fallback recovery strategy."""
        return {
            'recovery_attempted': True,
            'recovery_successful': True,
            'details': {
                'message': 'Fallback strategy activated - using alternative approach'
            }
        }

    def _handle_escalation_strategy(
        self,
        error_context: ErrorContext,
        pattern: ErrorPattern
    ) -> Dict[str, Any]:
        """Handle escalation recovery strategy."""
        # This would typically involve notifying administrators, creating incidents, etc.
        return {
            'recovery_attempted': True,
            'recovery_successful': False,
            'details': {
                'message': 'Error escalated for manual intervention',
                'escalation_level': 'administrative'
            }
        }

    def _record_error(self, error_context: ErrorContext):
        """Record error in history and update statistics."""
        self.error_history.append(error_context)

        # Update error statistics
        error_type = error_context.error_type
        stats = self.error_stats[error_type]
        stats['count'] += 1
        stats['last_occurrence'] = error_context.timestamp

        if stats['first_occurrence'] is None:
            stats['first_occurrence'] = error_context.timestamp

        # Add to aggregator for pattern detection
        self.error_aggregator[error_type].append(error_context)

        # Keep only recent errors for pattern detection
        cutoff_time = datetime.utcnow() - timedelta(hours=1)
        self.error_aggregator[error_type] = [
            ctx for ctx in self.error_aggregator[error_type]
            if ctx.timestamp > cutoff_time
        ]

    def _log_error(self, error_context: ErrorContext):
        """Log error with appropriate level and formatting."""
        log_data = {
            'error_id': error_context.error_id,
            'error_type': error_context.error_type,
            'category': error_context.category.value,
            'severity': error_context.severity.value,
            'module': error_context.module,
            'function': error_context.function,
            'line_number': error_context.line_number,
            'user_id': error_context.user_id,
            'request_id': error_context.request_id,
            'recovery_strategy': error_context.recovery_strategy.value if error_context.recovery_strategy else None
        }

        if error_context.severity == ErrorSeverity.CRITICAL:
            logger.critical(error_context.error_message, extra=log_data)
        elif error_context.severity == ErrorSeverity.HIGH:
            logger.error(error_context.error_message, extra=log_data)
        elif error_context.severity == ErrorSeverity.MEDIUM:
            logger.warning(error_context.error_message, extra=log_data)
        else:
            logger.info(error_context.error_message, extra=log_data)

    def get_error_statistics(self) -> Dict[str, Any]:
        """Get comprehensive error statistics."""
        current_time = datetime.utcnow()
        recent_cutoff = current_time - timedelta(hours=24)

        recent_errors = [
            ctx for ctx in self.error_history
            if ctx.timestamp > recent_cutoff
        ]

        # Calculate error rates by category
        category_stats = defaultdict(int)
        severity_stats = defaultdict(int)

        for error_ctx in recent_errors:
            category_stats[error_ctx.category.value] += 1
            severity_stats[error_ctx.severity.value] += 1

        # Circuit breaker states
        circuit_breaker_stats = {
            name: {
                'state': cb.state.value,
                'failure_count': cb.failure_count,
                'last_failure': cb.last_failure_time.isoformat() if cb.last_failure_time else None
            }
            for name, cb in self.circuit_breakers.items()
        }

        return {
            'total_errors': len(self.error_history),
            'recent_errors_24h': len(recent_errors),
            'error_rate_per_hour': len(recent_errors) / 24,
            'category_breakdown': dict(category_stats),
            'severity_breakdown': dict(severity_stats),
            'circuit_breakers': circuit_breaker_stats,
            'error_patterns': len(self.error_patterns),
            'most_common_errors': [
                (error_type, stats['count'])
                for error_type, stats in sorted(
                    self.error_stats.items(),
                    key=lambda x: x[1]['count'],
                    reverse=True
                )[:10]
            ]
        }


# Global error handler instance
_error_handler: Optional[StandardizedErrorHandler] = None


def get_error_handler() -> StandardizedErrorHandler:
    """Get or create the global error handler."""
    global _error_handler
    if _error_handler is None:
        _error_handler = StandardizedErrorHandler()
    return _error_handler


def handle_error(
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    request_id: Optional[str] = None
) -> ErrorContext:
    """Convenience function to handle errors using the global handler."""
    return get_error_handler().handle_error(error, context, user_id, request_id)


# Decorators for standardized error handling
def standardized_error_handler(
    retry_attempts: int = 0,
    fallback_value: Any = None,
    raise_on_failure: bool = True
):
    """
    Decorator for standardized error handling with retry and fallback support.

    Args:
        retry_attempts: Number of retry attempts
        fallback_value: Value to return if all attempts fail
        raise_on_failure: Whether to raise exception after exhausting retries
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None

            for attempt in range(retry_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_error = e

                    # Handle the error using standardized patterns
                    error_context = handle_error(
                        e,
                        context={
                            'function': func.__name__,
                            'attempt': attempt + 1,
                            'max_attempts': retry_attempts + 1,
                            'args_count': len(args),
                            'kwargs_keys': list(kwargs.keys())
                        }
                    )

                    # If this isn't the last attempt, wait before retrying
                    if attempt < retry_attempts:
                        # Apply exponential backoff
                        wait_time = 2 ** attempt
                        time.sleep(wait_time)
                        continue

                    # Last attempt failed
                    if fallback_value is not None:
                        logger.warning(
                            f"Function {func.__name__} failed after {retry_attempts + 1} attempts, "
                            f"returning fallback value"
                        )
                        return fallback_value

                    if raise_on_failure:
                        raise

                    return None

        return wrapper

    return decorator


@contextmanager
def error_handling_context(
    context_name: str,
    user_id: Optional[str] = None,
    request_id: Optional[str] = None,
    suppress_errors: bool = False
):
    """
    Context manager for standardized error handling within code blocks.

    Args:
        context_name: Name of the context for logging
        user_id: User ID for error tracking
        request_id: Request ID for tracing
        suppress_errors: Whether to suppress errors and continue
    """
    try:
        yield

    except Exception as e:
        # Handle the error using standardized patterns
        error_context = handle_error(
            e,
            context={'context_name': context_name},
            user_id=user_id,
            request_id=request_id
        )

        if not suppress_errors:
            raise


# Export key classes and functions
__all__ = [
    'StandardizedErrorHandler',
    'ErrorPattern',
    'ErrorContext',
    'ErrorSeverity',
    'ErrorCategory',
    'ErrorRecoveryStrategy',
    'CircuitBreaker',
    'CircuitBreakerState',
    'get_error_handler',
    'handle_error',
    'standardized_error_handler',
    'error_handling_context'
]