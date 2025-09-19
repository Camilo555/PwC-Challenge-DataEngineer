"""
Error Handling Decorators and Utilities
========================================

Provides decorators and utilities for standardizing error handling patterns
across all modules in the application.
"""
from __future__ import annotations

import functools
import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union
from datetime import datetime

from .base_exceptions import (
    BaseApplicationError,
    ValidationError,
    BusinessRuleError,
    DatabaseError,
    ExternalServiceError,
    DataProcessingError,
    ConfigurationError
)
from .error_codes import ErrorCode
from .error_handlers import handle_error

logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])
T = TypeVar('T')


def handle_exceptions(
    exceptions: Union[Type[Exception], List[Type[Exception]]] = None,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    user_message: Optional[str] = None,
    reraise: bool = False,
    log_level: str = "error",
    context_extractor: Optional[Callable[..., Dict[str, Any]]] = None
):
    """
    Decorator to standardize exception handling across functions and methods.

    Args:
        exceptions: Exception type(s) to catch. If None, catches all exceptions
        error_code: ErrorCode to use for caught exceptions
        user_message: User-friendly message for caught exceptions
        reraise: Whether to reraise the exception after handling
        log_level: Logging level for caught exceptions
        context_extractor: Function to extract context from function arguments
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Extract context if provided
                context = {}
                if context_extractor:
                    try:
                        context = context_extractor(*args, **kwargs)
                    except Exception:
                        logger.warning("Failed to extract context from function arguments")

                # Check if we should handle this exception
                if exceptions is None or isinstance(e, exceptions if isinstance(exceptions, tuple) else (exceptions,)):

                    # Convert to application error if not already
                    if not isinstance(e, BaseApplicationError):
                        app_error = BaseApplicationError(
                            message=str(e),
                            error_code=error_code,
                            user_message=user_message,
                            context=context,
                            inner_exception=e
                        )
                    else:
                        # Add context to existing application error
                        e.context.update(context)
                        app_error = e

                    # Log the error at specified level
                    log_method = getattr(logger, log_level, logger.error)
                    log_method(f"Error in {func.__name__}: {str(e)}")

                    if reraise:
                        raise app_error from e
                    return handle_error(app_error)
                else:
                    # Re-raise unhandled exceptions
                    raise

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Extract context if provided
                context = {}
                if context_extractor:
                    try:
                        context = context_extractor(*args, **kwargs)
                    except Exception:
                        logger.warning("Failed to extract context from function arguments")

                # Check if we should handle this exception
                if exceptions is None or isinstance(e, exceptions if isinstance(exceptions, tuple) else (exceptions,)):

                    # Convert to application error if not already
                    if not isinstance(e, BaseApplicationError):
                        app_error = BaseApplicationError(
                            message=str(e),
                            error_code=error_code,
                            user_message=user_message,
                            context=context,
                            inner_exception=e
                        )
                    else:
                        # Add context to existing application error
                        e.context.update(context)
                        app_error = e

                    # Log the error at specified level
                    log_method = getattr(logger, log_level, logger.error)
                    log_method(f"Error in {func.__name__}: {str(e)}")

                    if reraise:
                        raise app_error from e
                    return handle_error(app_error)
                else:
                    # Re-raise unhandled exceptions
                    raise

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator


def validate_input(
    validator: Callable[[Any], bool],
    error_message: str,
    field_name: Optional[str] = None
):
    """
    Decorator to validate input parameters and raise ValidationError on failure.

    Args:
        validator: Function that returns True if input is valid
        error_message: Error message for validation failure
        field_name: Name of the field being validated
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Run validation
            if not validator(*args, **kwargs):
                field_errors = {field_name: [error_message]} if field_name else {}
                raise ValidationError(
                    message=error_message,
                    field_errors=field_errors
                )
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Run validation
            if not validator(*args, **kwargs):
                field_errors = {field_name: [error_message]} if field_name else {}
                raise ValidationError(
                    message=error_message,
                    field_errors=field_errors
                )
            return await func(*args, **kwargs)

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator


def require_auth(
    permission: Optional[str] = None,
    role: Optional[str] = None
):
    """
    Decorator to require authentication and optionally authorization.

    Args:
        permission: Required permission
        role: Required role
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Check if user is authenticated (implementation specific)
            user = kwargs.get('current_user') or getattr(args[0] if args else None, 'current_user', None)

            if not user:
                raise BaseApplicationError(
                    message="Authentication required",
                    error_code=ErrorCode.AUTHENTICATION_ERROR,
                    user_message="You must be logged in to access this resource"
                )

            # Check permissions if specified
            if permission and not user.has_permission(permission):
                raise BaseApplicationError(
                    message=f"Permission '{permission}' required",
                    error_code=ErrorCode.AUTHORIZATION_ERROR,
                    user_message="You don't have permission to perform this action"
                )

            # Check role if specified
            if role and not user.has_role(role):
                raise BaseApplicationError(
                    message=f"Role '{role}' required",
                    error_code=ErrorCode.AUTHORIZATION_ERROR,
                    user_message="You don't have the required role to perform this action"
                )

            return func(*args, **kwargs)

        return wrapper
    return decorator


def retry_on_error(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Union[Type[Exception], List[Type[Exception]]] = None,
    retry_condition: Optional[Callable[[Exception], bool]] = None
):
    """
    Decorator to retry function execution on specific exceptions.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Backoff multiplier for delay
        exceptions: Exception types to retry on
        retry_condition: Custom function to determine if retry should occur
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if we should retry
                    should_retry = False

                    if retry_condition:
                        should_retry = retry_condition(e)
                    elif exceptions:
                        should_retry = isinstance(e, exceptions if isinstance(exceptions, tuple) else (exceptions,))
                    elif isinstance(e, BaseApplicationError):
                        should_retry = ErrorCode.requires_retry(e.error_code)

                    if attempt < max_retries and should_retry:
                        logger.warning(f"Retrying {func.__name__} after error: {str(e)} (attempt {attempt + 1}/{max_retries})")
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        # No more retries or shouldn't retry
                        break

            # All retries exhausted, raise the last exception
            if last_exception:
                raise last_exception

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if we should retry
                    should_retry = False

                    if retry_condition:
                        should_retry = retry_condition(e)
                    elif exceptions:
                        should_retry = isinstance(e, exceptions if isinstance(exceptions, tuple) else (exceptions,))
                    elif isinstance(e, BaseApplicationError):
                        should_retry = ErrorCode.requires_retry(e.error_code)

                    if attempt < max_retries and should_retry:
                        logger.warning(f"Retrying {func.__name__} after error: {str(e)} (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        # No more retries or shouldn't retry
                        break

            # All retries exhausted, raise the last exception
            if last_exception:
                raise last_exception

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator


def measure_performance(
    operation_name: Optional[str] = None,
    log_slow_operations: bool = True,
    slow_threshold_seconds: float = 1.0
):
    """
    Decorator to measure and log function performance.

    Args:
        operation_name: Name of the operation (defaults to function name)
        log_slow_operations: Whether to log slow operations
        slow_threshold_seconds: Threshold for considering an operation slow
    """
    def decorator(func: F) -> F:
        op_name = operation_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                if log_slow_operations and execution_time > slow_threshold_seconds:
                    logger.warning(f"Slow operation detected: {op_name} took {execution_time:.2f} seconds")

                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Error in {op_name} after {execution_time:.2f} seconds: {str(e)}")
                raise

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time

                if log_slow_operations and execution_time > slow_threshold_seconds:
                    logger.warning(f"Slow operation detected: {op_name} took {execution_time:.2f} seconds")

                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Error in {op_name} after {execution_time:.2f} seconds: {str(e)}")
                raise

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    expected_exception: Type[Exception] = Exception
):
    """
    Decorator implementing circuit breaker pattern for external services.

    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Time to wait before attempting to close circuit
        expected_exception: Exception type that triggers circuit breaker
    """
    def decorator(func: F) -> F:
        # Circuit breaker state
        state = {'failures': 0, 'last_failure_time': 0, 'is_open': False}

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()

            # Check if circuit is open and should remain open
            if state['is_open']:
                if now - state['last_failure_time'] < recovery_timeout:
                    raise ExternalServiceError(
                        message="Service circuit breaker is open",
                        error_code=ErrorCode.SERVICE_UNAVAILABLE,
                        user_message="Service is temporarily unavailable due to repeated failures"
                    )
                else:
                    # Try to close circuit (half-open state)
                    state['is_open'] = False
                    logger.info(f"Circuit breaker for {func.__name__} entering half-open state")

            try:
                result = func(*args, **kwargs)
                # Success - reset failure count
                if state['failures'] > 0:
                    logger.info(f"Circuit breaker for {func.__name__} reset after successful call")
                    state['failures'] = 0
                return result
            except expected_exception as e:
                state['failures'] += 1
                state['last_failure_time'] = now

                if state['failures'] >= failure_threshold:
                    state['is_open'] = True
                    logger.error(f"Circuit breaker for {func.__name__} opened after {failure_threshold} failures")
                    raise ExternalServiceError(
                        message="Service circuit breaker opened due to repeated failures",
                        error_code=ErrorCode.SERVICE_UNAVAILABLE,
                        user_message="Service is temporarily unavailable",
                        inner_exception=e
                    )
                else:
                    logger.warning(f"Circuit breaker for {func.__name__}: failure {state['failures']}/{failure_threshold}")
                    raise

        return wrapper
    return decorator


def database_transaction(
    rollback_on_error: bool = True,
    error_code: ErrorCode = ErrorCode.DATABASE_ERROR
):
    """
    Decorator to handle database transactions with automatic rollback on errors.

    Args:
        rollback_on_error: Whether to rollback transaction on error
        error_code: Error code to use for database errors
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # This would typically work with a database session
            # For now, it's a placeholder that catches database-related exceptions
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Convert to database error
                if not isinstance(e, DatabaseError):
                    raise DatabaseError(
                        message=f"Database operation failed: {str(e)}",
                        error_code=error_code,
                        inner_exception=e
                    )
                raise

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Convert to database error
                if not isinstance(e, DatabaseError):
                    raise DatabaseError(
                        message=f"Database operation failed: {str(e)}",
                        error_code=error_code,
                        inner_exception=e
                    )
                raise

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator


def standardize_errors(
    module_name: Optional[str] = None,
    default_error_code: ErrorCode = ErrorCode.INTERNAL_ERROR
):
    """
    Decorator to standardize all errors from a module into application errors.

    Args:
        module_name: Name of the module (for context)
        default_error_code: Default error code for unhandled exceptions
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except BaseApplicationError:
                # Already standardized, re-raise as-is
                raise
            except Exception as e:
                # Standardize the error
                context = {'module': module_name or func.__module__, 'function': func.__name__}

                raise BaseApplicationError(
                    message=f"Error in {context['function']}: {str(e)}",
                    error_code=default_error_code,
                    context=context,
                    inner_exception=e
                )

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except BaseApplicationError:
                # Already standardized, re-raise as-is
                raise
            except Exception as e:
                # Standardize the error
                context = {'module': module_name or func.__module__, 'function': func.__name__}

                raise BaseApplicationError(
                    message=f"Error in {context['function']}: {str(e)}",
                    error_code=default_error_code,
                    context=context,
                    inner_exception=e
                )

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator


# Common decorator combinations
def api_endpoint(
    require_auth: bool = True,
    validate_input: bool = True,
    handle_errors: bool = True
):
    """
    Composite decorator for API endpoints with common patterns.

    Args:
        require_auth: Whether to require authentication
        validate_input: Whether to validate input parameters
        handle_errors: Whether to handle exceptions
    """
    def decorator(func: F) -> F:
        # Apply decorators in reverse order
        decorated_func = func

        if handle_errors:
            decorated_func = handle_exceptions(
                error_code=ErrorCode.INTERNAL_ERROR,
                reraise=True
            )(decorated_func)

        if require_auth:
            decorated_func = require_auth()(decorated_func)

        decorated_func = measure_performance()(decorated_func)

        return decorated_func
    return decorator


def data_processing(
    retry_on_failure: bool = True,
    max_retries: int = 3,
    handle_errors: bool = True
):
    """
    Composite decorator for data processing functions.

    Args:
        retry_on_failure: Whether to retry on failure
        max_retries: Maximum number of retries
        handle_errors: Whether to handle exceptions
    """
    def decorator(func: F) -> F:
        decorated_func = func

        if handle_errors:
            decorated_func = handle_exceptions(
                error_code=ErrorCode.DATA_PROCESSING_ERROR,
                reraise=True
            )(decorated_func)

        if retry_on_failure:
            decorated_func = retry_on_error(
                max_retries=max_retries,
                exceptions=(DataProcessingError, ExternalServiceError)
            )(decorated_func)

        decorated_func = measure_performance(
            log_slow_operations=True,
            slow_threshold_seconds=5.0
        )(decorated_func)

        return decorated_func
    return decorator


def external_service(
    use_circuit_breaker: bool = True,
    retry_attempts: int = 3,
    timeout_seconds: float = 30.0
):
    """
    Composite decorator for external service calls.

    Args:
        use_circuit_breaker: Whether to use circuit breaker pattern
        retry_attempts: Number of retry attempts
        timeout_seconds: Request timeout in seconds
    """
    def decorator(func: F) -> F:
        decorated_func = func

        decorated_func = handle_exceptions(
            error_code=ErrorCode.EXTERNAL_SERVICE_ERROR,
            reraise=True
        )(decorated_func)

        if use_circuit_breaker:
            decorated_func = circuit_breaker(
                failure_threshold=5,
                recovery_timeout=60.0
            )(decorated_func)

        decorated_func = retry_on_error(
            max_retries=retry_attempts,
            delay=1.0,
            backoff=2.0,
            exceptions=ExternalServiceError
        )(decorated_func)

        decorated_func = measure_performance(
            log_slow_operations=True,
            slow_threshold_seconds=timeout_seconds / 2
        )(decorated_func)

        return decorated_func
    return decorator