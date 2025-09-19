"""
Error Handling Context Managers
===============================

Provides context managers for standardized error handling in different scenarios.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager, asynccontextmanager
from typing import Any, Dict, List, Optional, Type, Union, Generator, AsyncGenerator
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


@contextmanager
def handle_validation_errors(
    operation_name: str = "validation",
    reraise: bool = True
) -> Generator[List[str], None, None]:
    """
    Context manager for collecting and handling validation errors.

    Args:
        operation_name: Name of the validation operation
        reraise: Whether to reraise ValidationError at the end

    Yields:
        List[str]: List to collect validation error messages
    """
    errors = []

    try:
        yield errors
    finally:
        if errors:
            if reraise:
                raise ValidationError(
                    message=f"Validation failed for {operation_name}",
                    field_errors={operation_name: errors}
                )
            else:
                logger.warning(f"Validation errors in {operation_name}: {errors}")


@contextmanager
def database_operation(
    operation: str,
    table: Optional[str] = None,
    reraise: bool = True,
    log_queries: bool = False
) -> Generator[Dict[str, Any], None, None]:
    """
    Context manager for database operations with standardized error handling.

    Args:
        operation: Type of database operation (select, insert, update, delete)
        table: Table name being operated on
        reraise: Whether to reraise as DatabaseError
        log_queries: Whether to log SQL queries

    Yields:
        Dict[str, Any]: Context dictionary for operation metadata
    """
    start_time = time.time()
    context = {
        'operation': operation,
        'table': table,
        'start_time': start_time,
        'query_count': 0,
        'affected_rows': 0
    }

    try:
        yield context

        execution_time = time.time() - start_time
        logger.info(f"Database {operation} on {table or 'unknown'} completed in {execution_time:.3f}s")

    except Exception as e:
        execution_time = time.time() - start_time

        if reraise:
            raise DatabaseError(
                message=f"Database {operation} operation failed",
                operation=operation,
                table_name=table,
                context={
                    'execution_time_seconds': execution_time,
                    'query_count': context.get('query_count', 0)
                },
                inner_exception=e
            )
        else:
            logger.error(f"Database error in {operation} after {execution_time:.3f}s: {str(e)}")


@asynccontextmanager
async def async_database_operation(
    operation: str,
    table: Optional[str] = None,
    reraise: bool = True
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Async context manager for database operations.

    Args:
        operation: Type of database operation
        table: Table name being operated on
        reraise: Whether to reraise as DatabaseError

    Yields:
        Dict[str, Any]: Context dictionary for operation metadata
    """
    start_time = time.time()
    context = {
        'operation': operation,
        'table': table,
        'start_time': start_time,
        'query_count': 0,
        'affected_rows': 0
    }

    try:
        yield context

        execution_time = time.time() - start_time
        logger.info(f"Async database {operation} on {table or 'unknown'} completed in {execution_time:.3f}s")

    except Exception as e:
        execution_time = time.time() - start_time

        if reraise:
            raise DatabaseError(
                message=f"Async database {operation} operation failed",
                operation=operation,
                table_name=table,
                context={
                    'execution_time_seconds': execution_time,
                    'query_count': context.get('query_count', 0)
                },
                inner_exception=e
            )
        else:
            logger.error(f"Async database error in {operation} after {execution_time:.3f}s: {str(e)}")


@contextmanager
def external_service_call(
    service_name: str,
    operation: str,
    timeout_seconds: float = 30.0,
    reraise: bool = True
) -> Generator[Dict[str, Any], None, None]:
    """
    Context manager for external service calls with timeout and error handling.

    Args:
        service_name: Name of the external service
        operation: Operation being performed
        timeout_seconds: Timeout for the operation
        reraise: Whether to reraise as ExternalServiceError

    Yields:
        Dict[str, Any]: Context dictionary for operation metadata
    """
    start_time = time.time()
    context = {
        'service_name': service_name,
        'operation': operation,
        'timeout_seconds': timeout_seconds,
        'start_time': start_time,
        'response_status': None,
        'response_size': None
    }

    try:
        yield context

        execution_time = time.time() - start_time
        logger.info(f"External service call to {service_name}.{operation} completed in {execution_time:.3f}s")

        if execution_time > timeout_seconds:
            logger.warning(f"External service call to {service_name}.{operation} exceeded timeout ({timeout_seconds}s)")

    except Exception as e:
        execution_time = time.time() - start_time

        if reraise:
            # Determine if this was a timeout
            if execution_time >= timeout_seconds:
                error_code = ErrorCode.SERVICE_TIMEOUT
                message = f"External service call to {service_name}.{operation} timed out"
            else:
                error_code = ErrorCode.EXTERNAL_SERVICE_ERROR
                message = f"External service call to {service_name}.{operation} failed"

            raise ExternalServiceError(
                message=message,
                service_name=service_name,
                context={
                    'operation': operation,
                    'execution_time_seconds': execution_time,
                    'timeout_seconds': timeout_seconds,
                    'response_status': context.get('response_status'),
                    'response_size': context.get('response_size')
                },
                inner_exception=e
            )
        else:
            logger.error(f"External service error in {service_name}.{operation} after {execution_time:.3f}s: {str(e)}")


@contextmanager
def data_processing_batch(
    batch_name: str,
    total_records: Optional[int] = None,
    reraise: bool = True
) -> Generator[Dict[str, Any], None, None]:
    """
    Context manager for data processing batches with progress tracking.

    Args:
        batch_name: Name of the batch processing operation
        total_records: Total number of records to process
        reraise: Whether to reraise as DataProcessingError

    Yields:
        Dict[str, Any]: Context dictionary for batch metadata
    """
    start_time = time.time()
    context = {
        'batch_name': batch_name,
        'total_records': total_records,
        'processed_records': 0,
        'failed_records': 0,
        'errors': [],
        'start_time': start_time
    }

    try:
        yield context

        execution_time = time.time() - start_time
        success_rate = ((context['processed_records'] - context['failed_records']) /
                       context['processed_records'] * 100) if context['processed_records'] > 0 else 100

        logger.info(
            f"Data processing batch '{batch_name}' completed: "
            f"{context['processed_records']} processed, "
            f"{context['failed_records']} failed, "
            f"{success_rate:.1f}% success rate, "
            f"{execution_time:.3f}s total"
        )

        # Log warnings for failed records
        if context['failed_records'] > 0:
            logger.warning(f"Batch '{batch_name}' had {context['failed_records']} failed records")
            for error in context['errors'][-5:]:  # Log last 5 errors
                logger.warning(f"Batch error sample: {error}")

    except Exception as e:
        execution_time = time.time() - start_time

        if reraise:
            raise DataProcessingError(
                message=f"Data processing batch '{batch_name}' failed",
                processing_stage=batch_name,
                record_count=context.get('processed_records', 0),
                context={
                    'total_records': total_records,
                    'failed_records': context.get('failed_records', 0),
                    'execution_time_seconds': execution_time,
                    'error_samples': context.get('errors', [])[-3:]  # Last 3 errors
                },
                inner_exception=e
            )
        else:
            logger.error(f"Data processing error in batch '{batch_name}' after {execution_time:.3f}s: {str(e)}")


@contextmanager
def business_operation(
    operation_name: str,
    business_context: Optional[Dict[str, Any]] = None,
    reraise: bool = True
) -> Generator[Dict[str, Any], None, None]:
    """
    Context manager for business operations with rule validation tracking.

    Args:
        operation_name: Name of the business operation
        business_context: Business context information
        reraise: Whether to reraise as BusinessRuleError

    Yields:
        Dict[str, Any]: Context dictionary for operation metadata
    """
    start_time = time.time()
    context = {
        'operation_name': operation_name,
        'business_context': business_context or {},
        'rules_validated': [],
        'rule_violations': [],
        'start_time': start_time
    }

    try:
        yield context

        execution_time = time.time() - start_time
        logger.info(
            f"Business operation '{operation_name}' completed in {execution_time:.3f}s, "
            f"{len(context['rules_validated'])} rules validated"
        )

        if context['rule_violations']:
            logger.warning(f"Business rule violations in '{operation_name}': {context['rule_violations']}")

    except Exception as e:
        execution_time = time.time() - start_time

        if reraise:
            raise BusinessRuleError(
                message=f"Business operation '{operation_name}' failed",
                rule_name=operation_name,
                context={
                    'business_context': business_context or {},
                    'rules_validated': context.get('rules_validated', []),
                    'rule_violations': context.get('rule_violations', []),
                    'execution_time_seconds': execution_time
                },
                inner_exception=e
            )
        else:
            logger.error(f"Business operation error in '{operation_name}' after {execution_time:.3f}s: {str(e)}")


@contextmanager
def configuration_loading(
    config_source: str,
    reraise: bool = True
) -> Generator[Dict[str, Any], None, None]:
    """
    Context manager for configuration loading with validation.

    Args:
        config_source: Source of the configuration (file, env, etc.)
        reraise: Whether to reraise as ConfigurationError

    Yields:
        Dict[str, Any]: Context dictionary for configuration metadata
    """
    start_time = time.time()
    context = {
        'config_source': config_source,
        'keys_loaded': [],
        'validation_errors': [],
        'start_time': start_time
    }

    try:
        yield context

        execution_time = time.time() - start_time
        logger.info(
            f"Configuration loaded from '{config_source}' in {execution_time:.3f}s, "
            f"{len(context['keys_loaded'])} keys loaded"
        )

        if context['validation_errors']:
            logger.warning(f"Configuration validation errors: {context['validation_errors']}")

    except Exception as e:
        execution_time = time.time() - start_time

        if reraise:
            raise ConfigurationError(
                message=f"Configuration loading from '{config_source}' failed",
                config_key=config_source,
                context={
                    'keys_loaded': context.get('keys_loaded', []),
                    'validation_errors': context.get('validation_errors', []),
                    'execution_time_seconds': execution_time
                },
                inner_exception=e
            )
        else:
            logger.error(f"Configuration error loading from '{config_source}' after {execution_time:.3f}s: {str(e)}")


@contextmanager
def error_suppression(
    exceptions: Union[Type[Exception], List[Type[Exception]]],
    log_errors: bool = True,
    log_level: str = "error",
    default_return: Any = None
) -> Generator[Dict[str, Any], None, None]:
    """
    Context manager to suppress specific exceptions and optionally log them.

    Args:
        exceptions: Exception type(s) to suppress
        log_errors: Whether to log suppressed errors
        log_level: Log level for suppressed errors
        default_return: Default return value (not used in context manager)

    Yields:
        Dict[str, Any]: Context dictionary with error information
    """
    context = {
        'suppressed_errors': [],
        'error_count': 0
    }

    try:
        yield context
    except exceptions as e:
        context['suppressed_errors'].append(str(e))
        context['error_count'] += 1

        if log_errors:
            log_method = getattr(logger, log_level, logger.error)
            log_method(f"Suppressed exception: {str(e)}")


@contextmanager
def timed_operation(
    operation_name: str,
    slow_threshold_seconds: float = 5.0,
    log_completion: bool = True
) -> Generator[Dict[str, float], None, None]:
    """
    Context manager for timing operations and detecting slow operations.

    Args:
        operation_name: Name of the operation being timed
        slow_threshold_seconds: Threshold for considering operation slow
        log_completion: Whether to log operation completion

    Yields:
        Dict[str, float]: Dictionary with timing information
    """
    start_time = time.time()
    timing = {'start_time': start_time, 'duration': 0.0}

    try:
        yield timing
    finally:
        end_time = time.time()
        duration = end_time - start_time
        timing['duration'] = duration

        if log_completion:
            if duration > slow_threshold_seconds:
                logger.warning(f"Slow operation detected: {operation_name} took {duration:.3f}s")
            else:
                logger.info(f"Operation {operation_name} completed in {duration:.3f}s")


@contextmanager
def resource_management(
    resource_name: str,
    acquire_func: callable,
    release_func: callable,
    reraise: bool = True
) -> Generator[Any, None, None]:
    """
    Context manager for managing resources with automatic cleanup.

    Args:
        resource_name: Name of the resource being managed
        acquire_func: Function to acquire the resource
        release_func: Function to release the resource
        reraise: Whether to reraise exceptions after cleanup

    Yields:
        Any: The acquired resource
    """
    resource = None
    try:
        logger.debug(f"Acquiring resource: {resource_name}")
        resource = acquire_func()
        yield resource
    except Exception as e:
        logger.error(f"Error while using resource {resource_name}: {str(e)}")
        if reraise:
            raise
    finally:
        if resource:
            try:
                logger.debug(f"Releasing resource: {resource_name}")
                release_func(resource)
            except Exception as cleanup_error:
                logger.error(f"Error releasing resource {resource_name}: {str(cleanup_error)}")


# Utility functions for common patterns
def safe_call(func: callable, *args, default=None, log_errors: bool = True, **kwargs):
    """
    Safely call a function and return default value on error.

    Args:
        func: Function to call
        *args: Positional arguments
        default: Default value to return on error
        log_errors: Whether to log errors
        **kwargs: Keyword arguments

    Returns:
        Function result or default value on error
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Error in safe_call to {func.__name__}: {str(e)}")
        return default


async def async_safe_call(func: callable, *args, default=None, log_errors: bool = True, **kwargs):
    """
    Safely call an async function and return default value on error.

    Args:
        func: Async function to call
        *args: Positional arguments
        default: Default value to return on error
        log_errors: Whether to log errors
        **kwargs: Keyword arguments

    Returns:
        Function result or default value on error
    """
    try:
        return await func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Error in async_safe_call to {func.__name__}: {str(e)}")
        return default