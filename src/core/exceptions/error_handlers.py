"""
Error Handling Utilities
========================

Centralized error handling, logging, and response formatting utilities.
"""

import logging
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Type
from uuid import uuid4

from pydantic import BaseModel
from fastapi import Request
from fastapi.responses import JSONResponse

from .base_exceptions import BaseApplicationError
from .error_codes import ErrorCode


logger = logging.getLogger(__name__)


class ErrorResponse(BaseModel):
    """Standardized error response model."""

    error_id: str
    error_code: str
    message: str
    user_message: str
    timestamp: str
    details: Dict[str, Any] = {}
    suggestions: List[str] = []
    context: Dict[str, Any] = {}
    trace_id: Optional[str] = None

    @classmethod
    def from_exception(
        cls,
        error: BaseApplicationError,
        trace_id: Optional[str] = None,
        include_details: bool = True
    ) -> 'ErrorResponse':
        """Create ErrorResponse from BaseApplicationError."""
        return cls(
            error_id=str(uuid4()),
            error_code=error.error_code.value,
            message=error.message,
            user_message=error.user_message,
            timestamp=error.timestamp.isoformat(),
            details=error.details if include_details else {},
            suggestions=error.suggestions,
            context=error.context if include_details else {},
            trace_id=trace_id
        )

    @classmethod
    def from_generic_exception(
        cls,
        error: Exception,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        user_message: str = "An unexpected error occurred",
        trace_id: Optional[str] = None,
        include_details: bool = False
    ) -> 'ErrorResponse':
        """Create ErrorResponse from generic Exception."""
        details = {}
        if include_details:
            details = {
                'exception_type': type(error).__name__,
                'traceback': traceback.format_exc()
            }

        return cls(
            error_id=str(uuid4()),
            error_code=error_code.value,
            message=str(error),
            user_message=user_message,
            timestamp=datetime.utcnow().isoformat(),
            details=details,
            trace_id=trace_id
        )


class ErrorHandler:
    """Centralized error handling and logging."""

    def __init__(
        self,
        logger_name: Optional[str] = None,
        include_stack_trace: bool = False,
        mask_sensitive_data: bool = True
    ):
        self.logger = logging.getLogger(logger_name or __name__)
        self.include_stack_trace = include_stack_trace
        self.mask_sensitive_data = mask_sensitive_data
        self.sensitive_fields = {
            'password', 'token', 'secret', 'key', 'auth', 'credential',
            'ssn', 'social_security', 'credit_card', 'ccn', 'cvv'
        }

    def handle_error(
        self,
        error: Exception,
        request: Optional[Request] = None,
        user_id: Optional[str] = None,
        operation: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ) -> ErrorResponse:
        """
        Handle any exception and return standardized error response.

        Args:
            error: The exception to handle
            request: FastAPI request object (optional)
            user_id: ID of the user who triggered the error (optional)
            operation: The operation being performed when error occurred
            additional_context: Additional context information

        Returns:
            ErrorResponse: Standardized error response
        """
        trace_id = self._generate_trace_id(request)

        # Build context
        context = {
            'trace_id': trace_id,
            'timestamp': datetime.utcnow().isoformat()
        }

        if request:
            context.update({
                'method': request.method,
                'url': str(request.url),
                'user_agent': request.headers.get('user-agent'),
                'ip_address': self._get_client_ip(request)
            })

        if user_id:
            context['user_id'] = user_id

        if operation:
            context['operation'] = operation

        if additional_context:
            context.update(additional_context)

        # Handle different error types
        if isinstance(error, BaseApplicationError):
            # Add context to the error
            error.context.update(context)
            response = ErrorResponse.from_exception(
                error,
                trace_id=trace_id,
                include_details=not self.mask_sensitive_data
            )
            self._log_application_error(error, context)
        else:
            # Handle generic exceptions
            response = ErrorResponse.from_generic_exception(
                error,
                trace_id=trace_id,
                include_details=self.include_stack_trace and not self.mask_sensitive_data
            )
            self._log_generic_error(error, context)

        return response

    def create_json_response(
        self,
        error: Exception,
        request: Optional[Request] = None,
        **kwargs
    ) -> JSONResponse:
        """Create FastAPI JSONResponse from error."""
        error_response = self.handle_error(error, request, **kwargs)

        # Determine HTTP status code
        if isinstance(error, BaseApplicationError):
            status_code = ErrorCode.get_http_status_code(error.error_code)
        else:
            status_code = 500

        return JSONResponse(
            status_code=status_code,
            content=error_response.dict()
        )

    def _generate_trace_id(self, request: Optional[Request] = None) -> str:
        """Generate or extract trace ID."""
        if request and 'x-trace-id' in request.headers:
            return request.headers['x-trace-id']
        return str(uuid4())

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address."""
        # Check for forwarded headers first
        forwarded_for = request.headers.get('x-forwarded-for')
        if forwarded_for:
            return forwarded_for.split(',')[0].strip()

        real_ip = request.headers.get('x-real-ip')
        if real_ip:
            return real_ip

        return request.client.host if request.client else 'unknown'

    def _log_application_error(self, error: BaseApplicationError, context: Dict[str, Any]):
        """Log application-specific errors."""
        log_data = {
            'error_code': error.error_code.value,
            'error_type': type(error).__name__,
            'message': error.message,
            'user_message': error.user_message,
            'context': self._mask_sensitive_data(context),
            'details': self._mask_sensitive_data(error.details),
            'suggestions': error.suggestions
        }

        # Log at appropriate level
        severity = ErrorCode.get_severity(error.error_code)

        if severity == 'CRITICAL':
            self.logger.critical(f"Critical application error: {error.message}", extra=log_data)
        elif severity == 'ERROR':
            self.logger.error(f"Application error: {error.message}", extra=log_data)
        elif severity == 'WARNING':
            self.logger.warning(f"Application warning: {error.message}", extra=log_data)
        else:
            self.logger.info(f"Application info: {error.message}", extra=log_data)

    def _log_generic_error(self, error: Exception, context: Dict[str, Any]):
        """Log generic exceptions."""
        log_data = {
            'error_type': type(error).__name__,
            'message': str(error),
            'context': self._mask_sensitive_data(context)
        }

        if self.include_stack_trace:
            log_data['traceback'] = traceback.format_exc()

        self.logger.error(f"Unhandled exception: {str(error)}", extra=log_data)

    def _mask_sensitive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Mask sensitive data in logs if enabled."""
        if not self.mask_sensitive_data or not data:
            return data

        masked_data = {}
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in self.sensitive_fields):
                masked_data[key] = "***MASKED***"
            elif isinstance(value, dict):
                masked_data[key] = self._mask_sensitive_data(value)
            elif isinstance(value, list):
                masked_data[key] = [
                    self._mask_sensitive_data(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                masked_data[key] = value

        return masked_data


# Global error handler instance
_global_error_handler = ErrorHandler()


def handle_error(
    error: Exception,
    request: Optional[Request] = None,
    **kwargs
) -> ErrorResponse:
    """Global error handling function."""
    return _global_error_handler.handle_error(error, request, **kwargs)


def log_error(
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    operation: Optional[str] = None
):
    """Log error without creating response."""
    _global_error_handler._log_generic_error(error, context or {})


def create_error_response(
    error: Exception,
    request: Optional[Request] = None,
    **kwargs
) -> JSONResponse:
    """Create FastAPI JSON error response."""
    return _global_error_handler.create_json_response(error, request, **kwargs)


def configure_error_handler(
    include_stack_trace: bool = False,
    mask_sensitive_data: bool = True,
    logger_name: Optional[str] = None
):
    """Configure global error handler settings."""
    global _global_error_handler
    _global_error_handler = ErrorHandler(
        logger_name=logger_name,
        include_stack_trace=include_stack_trace,
        mask_sensitive_data=mask_sensitive_data
    )


# FastAPI exception handlers
async def validation_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle Pydantic validation exceptions."""
    from pydantic import ValidationError as PydanticValidationError

    if isinstance(exc, PydanticValidationError):
        # Convert Pydantic errors to our format
        field_errors = {}
        for error in exc.errors():
            field = ".".join(str(loc) for loc in error["loc"])
            if field not in field_errors:
                field_errors[field] = []
            field_errors[field].append(error["msg"])

        app_error = BaseApplicationError(
            message="Validation failed",
            error_code=ErrorCode.VALIDATION_ERROR,
            details={"field_errors": field_errors}
        )
        return create_error_response(app_error, request)

    return create_error_response(exc, request)


async def application_exception_handler(request: Request, exc: BaseApplicationError) -> JSONResponse:
    """Handle application-specific exceptions."""
    return create_error_response(exc, request)


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle all other exceptions."""
    return create_error_response(exc, request)