"""
Standardized Error Handling Patterns
=====================================

Provides common error handling patterns and utilities for consistent
error management across all modules.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Type, Union, Callable, TypeVar
from functools import wraps

from .base_exceptions import (
    BaseApplicationError,
    ValidationError,
    BusinessRuleError,
    DatabaseError,
    ExternalServiceError,
    DataProcessingError,
    AuthenticationError,
    AuthorizationError,
    NotFoundError,
    ConfigurationError
)
from .error_codes import ErrorCode
from .error_handlers import ErrorHandler, ErrorResponse

logger = logging.getLogger(__name__)
T = TypeVar('T')


class ErrorPattern:
    """Base class for error handling patterns."""

    def __init__(self, name: str, error_handler: Optional[ErrorHandler] = None):
        self.name = name
        self.error_handler = error_handler or ErrorHandler()
        self.error_counts = {}
        self.last_errors = []
        self.max_last_errors = 10

    def record_error(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Record an error occurrence."""
        error_type = type(error).__name__
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

        # Keep track of recent errors
        self.last_errors.append({
            'error': str(error),
            'type': error_type,
            'context': context or {}
        })

        # Maintain max size
        if len(self.last_errors) > self.max_last_errors:
            self.last_errors = self.last_errors[-self.max_last_errors:]

    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of errors for this pattern."""
        return {
            'pattern_name': self.name,
            'total_errors': sum(self.error_counts.values()),
            'error_types': self.error_counts.copy(),
            'recent_errors': self.last_errors.copy()
        }


class ValidationPattern(ErrorPattern):
    """Pattern for handling validation errors consistently."""

    def __init__(self):
        super().__init__("validation")

    def validate_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> List[str]:
        """Validate that required fields are present and not empty."""
        errors = []

        for field in required_fields:
            if field not in data or data[field] is None or data[field] == "":
                errors.append(f"Field '{field}' is required")

        return errors

    def validate_field_types(self, data: Dict[str, Any], field_types: Dict[str, Type]) -> List[str]:
        """Validate that fields have correct types."""
        errors = []

        for field, expected_type in field_types.items():
            if field in data and data[field] is not None:
                if not isinstance(data[field], expected_type):
                    errors.append(f"Field '{field}' must be of type {expected_type.__name__}")

        return errors

    def validate_field_ranges(self, data: Dict[str, Any], field_ranges: Dict[str, Dict[str, Any]]) -> List[str]:
        """Validate that numeric fields are within specified ranges."""
        errors = []

        for field, range_config in field_ranges.items():
            if field in data and data[field] is not None:
                value = data[field]

                if 'min' in range_config and value < range_config['min']:
                    errors.append(f"Field '{field}' must be at least {range_config['min']}")

                if 'max' in range_config and value > range_config['max']:
                    errors.append(f"Field '{field}' must be at most {range_config['max']}")

        return errors

    def create_validation_error(self, errors: List[str], field_name: Optional[str] = None) -> ValidationError:
        """Create a standardized validation error."""
        field_errors = {}
        if field_name:
            field_errors[field_name] = errors
        else:
            field_errors['general'] = errors

        error = ValidationError(
            message="Validation failed",
            field_errors=field_errors
        )

        self.record_error(error, {'field_errors': field_errors})
        return error


class DatabasePattern(ErrorPattern):
    """Pattern for handling database errors consistently."""

    def __init__(self):
        super().__init__("database")
        self.connection_errors = 0
        self.query_errors = 0
        self.constraint_violations = 0

    def handle_connection_error(self, error: Exception, database_url: Optional[str] = None) -> DatabaseError:
        """Handle database connection errors."""
        self.connection_errors += 1

        db_error = DatabaseError(
            message="Database connection failed",
            operation="connect",
            context={
                'database_url': database_url[:50] + "..." if database_url and len(database_url) > 50 else database_url,
                'connection_attempt': self.connection_errors
            },
            inner_exception=error
        )

        # Add suggestions based on error type
        if "timeout" in str(error).lower():
            db_error.add_suggestion("Check database server availability and network connectivity")
            db_error.add_suggestion("Consider increasing connection timeout settings")
        elif "authentication" in str(error).lower() or "password" in str(error).lower():
            db_error.add_suggestion("Verify database credentials")
            db_error.add_suggestion("Check if user has necessary permissions")
        else:
            db_error.add_suggestion("Verify database server is running and accessible")

        self.record_error(db_error, {'operation': 'connect'})
        return db_error

    def handle_query_error(self, error: Exception, query: Optional[str] = None, params: Optional[Dict] = None) -> DatabaseError:
        """Handle database query errors."""
        self.query_errors += 1

        # Mask sensitive data in query for logging
        safe_query = query[:200] + "..." if query and len(query) > 200 else query

        db_error = DatabaseError(
            message="Database query failed",
            operation="query",
            context={
                'query_preview': safe_query,
                'parameter_count': len(params) if params else 0,
                'query_attempt': self.query_errors
            },
            inner_exception=error
        )

        # Add suggestions based on error type
        error_str = str(error).lower()
        if "syntax" in error_str:
            db_error.add_suggestion("Check SQL syntax")
            db_error.add_suggestion("Verify table and column names")
        elif "constraint" in error_str or "foreign key" in error_str:
            db_error.add_suggestion("Check data integrity constraints")
            db_error.add_suggestion("Verify referenced records exist")
        elif "permission" in error_str or "denied" in error_str:
            db_error.add_suggestion("Check user permissions for the operation")
        else:
            db_error.add_suggestion("Review query logic and data types")

        self.record_error(db_error, {'operation': 'query'})
        return db_error

    def handle_constraint_violation(self, error: Exception, table: Optional[str] = None, constraint: Optional[str] = None) -> DatabaseError:
        """Handle database constraint violations."""
        self.constraint_violations += 1

        db_error = DatabaseError(
            message="Database constraint violation",
            operation="constraint_check",
            table_name=table,
            constraint_name=constraint,
            context={
                'violation_count': self.constraint_violations
            },
            inner_exception=error
        )

        # Add specific suggestions for constraint types
        if constraint:
            if "primary" in constraint.lower() or "pk" in constraint.lower():
                db_error.add_suggestion("Ensure primary key values are unique")
            elif "foreign" in constraint.lower() or "fk" in constraint.lower():
                db_error.add_suggestion("Verify that referenced record exists")
            elif "unique" in constraint.lower():
                db_error.add_suggestion("Ensure field values are unique")
            elif "check" in constraint.lower():
                db_error.add_suggestion("Verify field values meet check constraint conditions")
        else:
            db_error.add_suggestion("Check data integrity requirements")

        self.record_error(db_error, {'operation': 'constraint_check', 'table': table})
        return db_error


class ExternalServicePattern(ErrorPattern):
    """Pattern for handling external service errors consistently."""

    def __init__(self):
        super().__init__("external_service")
        self.timeout_errors = 0
        self.authentication_errors = 0
        self.rate_limit_errors = 0

    def handle_timeout_error(self, error: Exception, service_name: str, timeout_seconds: float) -> ExternalServiceError:
        """Handle external service timeout errors."""
        self.timeout_errors += 1

        service_error = ExternalServiceError(
            message=f"External service '{service_name}' timed out",
            service_name=service_name,
            context={
                'timeout_seconds': timeout_seconds,
                'timeout_count': self.timeout_errors
            },
            inner_exception=error
        )

        # Add timeout-specific suggestions
        service_error.add_suggestion("Retry the request with exponential backoff")
        service_error.add_suggestion("Check service status and availability")
        if timeout_seconds < 30:
            service_error.add_suggestion("Consider increasing timeout duration")

        self.record_error(service_error, {'service': service_name, 'type': 'timeout'})
        return service_error

    def handle_authentication_error(self, error: Exception, service_name: str, auth_method: Optional[str] = None) -> ExternalServiceError:
        """Handle external service authentication errors."""
        self.authentication_errors += 1

        service_error = ExternalServiceError(
            message=f"Authentication failed for service '{service_name}'",
            service_name=service_name,
            context={
                'auth_method': auth_method,
                'auth_error_count': self.authentication_errors
            },
            inner_exception=error
        )

        # Add authentication-specific suggestions
        service_error.add_suggestion("Verify API credentials")
        service_error.add_suggestion("Check if API key or token has expired")
        service_error.add_suggestion("Ensure correct authentication method is used")

        self.record_error(service_error, {'service': service_name, 'type': 'authentication'})
        return service_error

    def handle_rate_limit_error(self, error: Exception, service_name: str, retry_after: Optional[int] = None) -> ExternalServiceError:
        """Handle external service rate limiting errors."""
        self.rate_limit_errors += 1

        service_error = ExternalServiceError(
            message=f"Rate limit exceeded for service '{service_name}'",
            service_name=service_name,
            context={
                'retry_after_seconds': retry_after,
                'rate_limit_count': self.rate_limit_errors
            },
            inner_exception=error
        )

        # Add rate limiting suggestions
        if retry_after:
            service_error.add_suggestion(f"Wait {retry_after} seconds before retrying")
        service_error.add_suggestion("Implement exponential backoff for retries")
        service_error.add_suggestion("Consider reducing request frequency")
        service_error.add_suggestion("Review API usage limits and quotas")

        self.record_error(service_error, {'service': service_name, 'type': 'rate_limit'})
        return service_error


class BusinessRulePattern(ErrorPattern):
    """Pattern for handling business rule violations consistently."""

    def __init__(self):
        super().__init__("business_rule")
        self.rule_violations = {}

    def check_business_rule(self, rule_name: str, condition: bool, error_message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Check a business rule and raise error if violated."""
        if not condition:
            self.rule_violations[rule_name] = self.rule_violations.get(rule_name, 0) + 1

            rule_error = BusinessRuleError(
                message=error_message,
                rule_name=rule_name,
                context=context or {}
            )

            self.record_error(rule_error, {'rule': rule_name, 'context': context})
            raise rule_error

    def validate_business_hours(self, operation_time: any, business_hours: Dict[str, Any]) -> None:
        """Validate that operation is within business hours."""
        # This would implement actual business hours logic
        # For now, it's a placeholder
        self.check_business_rule(
            "business_hours",
            True,  # Placeholder condition
            "Operation must be performed during business hours",
            {'operation_time': str(operation_time), 'business_hours': business_hours}
        )

    def validate_customer_limits(self, customer_id: str, current_amount: float, limit: float) -> None:
        """Validate customer spending limits."""
        self.check_business_rule(
            "customer_spending_limit",
            current_amount <= limit,
            f"Customer spending limit of {limit} exceeded",
            {'customer_id': customer_id, 'current_amount': current_amount, 'limit': limit}
        )

    def validate_inventory_availability(self, product_id: str, requested_quantity: int, available_quantity: int) -> None:
        """Validate product inventory availability."""
        self.check_business_rule(
            "inventory_availability",
            requested_quantity <= available_quantity,
            f"Insufficient inventory for product {product_id}",
            {
                'product_id': product_id,
                'requested_quantity': requested_quantity,
                'available_quantity': available_quantity
            }
        )


class DataProcessingPattern(ErrorPattern):
    """Pattern for handling data processing errors consistently."""

    def __init__(self):
        super().__init__("data_processing")
        self.processing_errors = {}
        self.quality_issues = 0

    def handle_data_quality_error(self, errors: List[str], record_count: int, failed_count: int) -> DataProcessingError:
        """Handle data quality validation errors."""
        self.quality_issues += 1
        failure_rate = (failed_count / record_count) * 100 if record_count > 0 else 0

        processing_error = DataProcessingError(
            message=f"Data quality validation failed for {failed_count}/{record_count} records ({failure_rate:.1f}% failure rate)",
            processing_stage="quality_validation",
            record_count=record_count,
            context={
                'failed_records': failed_count,
                'failure_rate': failure_rate,
                'quality_issues': errors,
                'quality_issue_count': self.quality_issues
            }
        )

        # Add data quality suggestions
        if failure_rate > 50:
            processing_error.add_suggestion("Review data source quality")
            processing_error.add_suggestion("Consider data cleansing before processing")
        elif failure_rate > 10:
            processing_error.add_suggestion("Implement data validation rules")
            processing_error.add_suggestion("Add data quality monitoring")
        else:
            processing_error.add_suggestion("Monitor data quality trends")

        self.record_error(processing_error, {'stage': 'quality_validation'})
        return processing_error

    def handle_transformation_error(self, error: Exception, stage: str, record_data: Optional[Dict] = None) -> DataProcessingError:
        """Handle data transformation errors."""
        self.processing_errors[stage] = self.processing_errors.get(stage, 0) + 1

        processing_error = DataProcessingError(
            message=f"Data transformation failed at stage '{stage}'",
            processing_stage=stage,
            context={
                'error_count': self.processing_errors[stage],
                'sample_record': record_data
            },
            inner_exception=error
        )

        # Add transformation-specific suggestions
        processing_error.add_suggestion("Review transformation logic")
        processing_error.add_suggestion("Validate input data format")
        processing_error.add_suggestion("Check for null or missing values")

        self.record_error(processing_error, {'stage': stage})
        return processing_error


class ErrorPatternManager:
    """Manages multiple error patterns and provides unified access."""

    def __init__(self):
        self.patterns = {
            'validation': ValidationPattern(),
            'database': DatabasePattern(),
            'external_service': ExternalServicePattern(),
            'business_rule': BusinessRulePattern(),
            'data_processing': DataProcessingPattern()
        }

    def get_pattern(self, pattern_name: str) -> Optional[ErrorPattern]:
        """Get a specific error pattern."""
        return self.patterns.get(pattern_name)

    def get_all_error_summaries(self) -> Dict[str, Any]:
        """Get error summaries for all patterns."""
        summaries = {}
        for name, pattern in self.patterns.items():
            summaries[name] = pattern.get_error_summary()
        return summaries

    def get_total_error_count(self) -> int:
        """Get total error count across all patterns."""
        total = 0
        for pattern in self.patterns.values():
            total += sum(pattern.error_counts.values())
        return total

    def reset_all_patterns(self):
        """Reset error counts for all patterns."""
        for pattern in self.patterns.values():
            pattern.error_counts.clear()
            pattern.last_errors.clear()


# Global pattern manager instance
_pattern_manager = ErrorPatternManager()


def get_pattern_manager() -> ErrorPatternManager:
    """Get the global error pattern manager."""
    return _pattern_manager


def get_validation_pattern() -> ValidationPattern:
    """Get the validation error pattern."""
    return _pattern_manager.get_pattern('validation')


def get_database_pattern() -> DatabasePattern:
    """Get the database error pattern."""
    return _pattern_manager.get_pattern('database')


def get_external_service_pattern() -> ExternalServicePattern:
    """Get the external service error pattern."""
    return _pattern_manager.get_pattern('external_service')


def get_business_rule_pattern() -> BusinessRulePattern:
    """Get the business rule error pattern."""
    return _pattern_manager.get_pattern('business_rule')


def get_data_processing_pattern() -> DataProcessingPattern:
    """Get the data processing error pattern."""
    return _pattern_manager.get_pattern('data_processing')


# Utility functions for common error scenarios
def standardize_exception(
    error: Exception,
    error_type: Type[BaseApplicationError],
    message: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> BaseApplicationError:
    """
    Standardize any exception into an application error.

    Args:
        error: Original exception
        error_type: Target application error type
        message: Optional custom message
        context: Additional context information

    Returns:
        BaseApplicationError: Standardized application error
    """
    if isinstance(error, BaseApplicationError):
        # Already an application error, just add context if provided
        if context:
            error.context.update(context)
        return error

    # Convert to application error
    return error_type(
        message=message or str(error),
        context=context or {},
        inner_exception=error
    )


def create_error_chain(errors: List[Exception], primary_message: str) -> BaseApplicationError:
    """
    Create an error chain from multiple exceptions.

    Args:
        errors: List of exceptions to chain
        primary_message: Primary error message

    Returns:
        BaseApplicationError: Error with chained exceptions in context
    """
    error_chain = []
    for i, error in enumerate(errors):
        error_chain.append({
            'index': i,
            'type': type(error).__name__,
            'message': str(error)
        })

    return BaseApplicationError(
        message=primary_message,
        error_code=ErrorCode.INTERNAL_ERROR,
        context={'error_chain': error_chain},
        inner_exception=errors[0] if errors else None
    )