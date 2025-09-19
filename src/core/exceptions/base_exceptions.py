"""
Base Exception Classes
======================

Defines the hierarchy of custom exceptions for the application.
All custom exceptions inherit from BaseApplicationError.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import traceback
import logging

from .error_codes import ErrorCode


logger = logging.getLogger(__name__)


class BaseApplicationError(Exception):
    """
    Base class for all application-specific exceptions.

    Provides structured error information including error codes,
    user-friendly messages, technical details, and context.
    """

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        user_message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        inner_exception: Optional[Exception] = None,
        suggestions: Optional[List[str]] = None
    ):
        super().__init__(message)

        self.message = message
        self.error_code = error_code
        self.user_message = user_message or self._get_default_user_message()
        self.details = details or {}
        self.context = context or {}
        self.inner_exception = inner_exception
        self.suggestions = suggestions or []
        self.timestamp = datetime.utcnow()
        self.traceback_info = traceback.format_exc() if inner_exception else None

        # Log the error
        self._log_error()

    def _get_default_user_message(self) -> str:
        """Get default user-friendly message."""
        return "An error occurred while processing your request. Please try again."

    def _log_error(self):
        """Log the error with appropriate level."""
        log_data = {
            'error_code': self.error_code.value,
            'message': self.message,
            'details': self.details,
            'context': self.context,
            'timestamp': self.timestamp.isoformat()
        }

        if self.inner_exception:
            log_data['inner_exception'] = str(self.inner_exception)
            log_data['traceback'] = self.traceback_info

        # Log at appropriate level based on error type
        if self.error_code.value.startswith('CRITICAL'):
            logger.critical(f"Critical error: {self.message}", extra=log_data)
        elif self.error_code.value.startswith('ERROR'):
            logger.error(f"Error: {self.message}", extra=log_data)
        elif self.error_code.value.startswith('WARNING'):
            logger.warning(f"Warning: {self.message}", extra=log_data)
        else:
            logger.info(f"Info: {self.message}", extra=log_data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API responses."""
        return {
            'error_code': self.error_code.value,
            'message': self.message,
            'user_message': self.user_message,
            'details': self.details,
            'context': self.context,
            'suggestions': self.suggestions,
            'timestamp': self.timestamp.isoformat(),
            'type': self.__class__.__name__
        }

    def add_context(self, key: str, value: Any) -> 'BaseApplicationError':
        """Add contextual information to the error."""
        self.context[key] = value
        return self

    def add_suggestion(self, suggestion: str) -> 'BaseApplicationError':
        """Add a suggestion for resolving the error."""
        self.suggestions.append(suggestion)
        return self


class ValidationError(BaseApplicationError):
    """Raised when input validation fails."""

    def __init__(
        self,
        message: str,
        field_errors: Optional[Dict[str, List[str]]] = None,
        **kwargs
    ):
        self.field_errors = field_errors or {}
        super().__init__(
            message,
            error_code=ErrorCode.VALIDATION_ERROR,
            details={'field_errors': self.field_errors},
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "The provided data is invalid. Please check your input and try again."


class BusinessRuleError(BaseApplicationError):
    """Raised when business rules are violated."""

    def __init__(
        self,
        message: str,
        rule_name: Optional[str] = None,
        **kwargs
    ):
        self.rule_name = rule_name
        super().__init__(
            message,
            error_code=ErrorCode.BUSINESS_RULE_VIOLATION,
            details={'rule_name': rule_name},
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "This action violates business rules and cannot be completed."


class ConfigurationError(BaseApplicationError):
    """Raised when there are configuration issues."""

    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        **kwargs
    ):
        self.config_key = config_key
        super().__init__(
            message,
            error_code=ErrorCode.CONFIGURATION_ERROR,
            details={'config_key': config_key},
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "The system is not properly configured. Please contact support."


class ExternalServiceError(BaseApplicationError):
    """Raised when external service calls fail."""

    def __init__(
        self,
        message: str,
        service_name: Optional[str] = None,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
        **kwargs
    ):
        self.service_name = service_name
        self.status_code = status_code
        self.response_body = response_body

        super().__init__(
            message,
            error_code=ErrorCode.EXTERNAL_SERVICE_ERROR,
            details={
                'service_name': service_name,
                'status_code': status_code,
                'response_body': response_body
            },
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "An external service is currently unavailable. Please try again later."


class SecurityError(BaseApplicationError):
    """Base class for security-related errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            error_code=ErrorCode.SECURITY_ERROR,
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "Access denied due to security policy."


class AuthenticationError(SecurityError):
    """Raised when authentication fails."""

    def __init__(
        self,
        message: str = "Authentication failed",
        auth_method: Optional[str] = None,
        **kwargs
    ):
        self.auth_method = auth_method
        super().__init__(
            message,
            error_code=ErrorCode.AUTHENTICATION_ERROR,
            details={'auth_method': auth_method},
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "Authentication failed. Please check your credentials and try again."


class AuthorizationError(SecurityError):
    """Raised when authorization fails."""

    def __init__(
        self,
        message: str = "Insufficient permissions",
        required_permission: Optional[str] = None,
        user_permissions: Optional[List[str]] = None,
        **kwargs
    ):
        self.required_permission = required_permission
        self.user_permissions = user_permissions or []

        super().__init__(
            message,
            error_code=ErrorCode.AUTHORIZATION_ERROR,
            details={
                'required_permission': required_permission,
                'user_permissions': user_permissions
            },
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "You don't have permission to perform this action."


class DataProcessingError(BaseApplicationError):
    """Raised when data processing operations fail."""

    def __init__(
        self,
        message: str,
        processing_stage: Optional[str] = None,
        data_source: Optional[str] = None,
        record_count: Optional[int] = None,
        **kwargs
    ):
        self.processing_stage = processing_stage
        self.data_source = data_source
        self.record_count = record_count

        super().__init__(
            message,
            error_code=ErrorCode.DATA_PROCESSING_ERROR,
            details={
                'processing_stage': processing_stage,
                'data_source': data_source,
                'record_count': record_count
            },
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "There was an error processing your data. Please try again."


class DatabaseError(BaseApplicationError):
    """Raised when database operations fail."""

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        table_name: Optional[str] = None,
        constraint_name: Optional[str] = None,
        **kwargs
    ):
        self.operation = operation
        self.table_name = table_name
        self.constraint_name = constraint_name

        super().__init__(
            message,
            error_code=ErrorCode.DATABASE_ERROR,
            details={
                'operation': operation,
                'table_name': table_name,
                'constraint_name': constraint_name
            },
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "A database error occurred. Please try again."


class NotFoundError(BaseApplicationError):
    """Raised when a requested resource is not found."""

    def __init__(
        self,
        message: str,
        resource_type: Optional[str] = None,
        resource_id: Optional[Union[str, int]] = None,
        **kwargs
    ):
        self.resource_type = resource_type
        self.resource_id = resource_id

        super().__init__(
            message,
            error_code=ErrorCode.NOT_FOUND,
            details={
                'resource_type': resource_type,
                'resource_id': str(resource_id) if resource_id else None
            },
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        resource_name = self.resource_type or "resource"
        return f"The requested {resource_name} was not found."


class ConflictError(BaseApplicationError):
    """Raised when there's a conflict with the current state."""

    def __init__(
        self,
        message: str,
        conflict_type: Optional[str] = None,
        **kwargs
    ):
        self.conflict_type = conflict_type
        super().__init__(
            message,
            error_code=ErrorCode.CONFLICT,
            details={'conflict_type': conflict_type},
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        return "This action conflicts with the current state. Please refresh and try again."


class RateLimitError(BaseApplicationError):
    """Raised when rate limits are exceeded."""

    def __init__(
        self,
        message: str,
        limit: Optional[int] = None,
        window_seconds: Optional[int] = None,
        retry_after: Optional[int] = None,
        **kwargs
    ):
        self.limit = limit
        self.window_seconds = window_seconds
        self.retry_after = retry_after

        super().__init__(
            message,
            error_code=ErrorCode.RATE_LIMIT_EXCEEDED,
            details={
                'limit': limit,
                'window_seconds': window_seconds,
                'retry_after': retry_after
            },
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        if self.retry_after:
            return f"Rate limit exceeded. Please try again in {self.retry_after} seconds."
        return "Rate limit exceeded. Please try again later."


class ServiceUnavailableError(BaseApplicationError):
    """Raised when a service is temporarily unavailable."""

    def __init__(
        self,
        message: str,
        service_name: Optional[str] = None,
        estimated_recovery_time: Optional[int] = None,
        **kwargs
    ):
        self.service_name = service_name
        self.estimated_recovery_time = estimated_recovery_time

        super().__init__(
            message,
            error_code=ErrorCode.SERVICE_UNAVAILABLE,
            details={
                'service_name': service_name,
                'estimated_recovery_time': estimated_recovery_time
            },
            **kwargs
        )

    def _get_default_user_message(self) -> str:
        if self.estimated_recovery_time:
            return f"Service is temporarily unavailable. Estimated recovery: {self.estimated_recovery_time} minutes."
        return "Service is temporarily unavailable. Please try again later."