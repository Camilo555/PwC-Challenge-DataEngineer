"""
Standardized Error Handling Module
==================================

Comprehensive, standardized exception handling system for the PwC Challenge DataEngineer platform.
Provides consistent error types, error codes, structured error responses, decorators, context managers,
and standardized patterns for all error scenarios.

Features:
- Hierarchical exception classes with rich context
- Standardized error codes and HTTP status mapping
- Error handling decorators and context managers
- Common error patterns for validation, database, external services, etc.
- Comprehensive logging and monitoring
- Circuit breaker and retry patterns
- Performance measurement and slow operation detection
"""

# Core exceptions
from .base_exceptions import (
    BaseApplicationError,
    ValidationError,
    BusinessRuleError,
    ConfigurationError,
    ExternalServiceError,
    SecurityError,
    AuthenticationError,
    AuthorizationError,
    DataProcessingError,
    DatabaseError,
    NotFoundError,
    ConflictError,
    RateLimitError,
    ServiceUnavailableError
)

# Legacy aliases for backward compatibility
ConfigurationException = ConfigurationError
ValidationException = ValidationError
ETLException = DataProcessingError
DataQualityException = ValidationError

# Error handling utilities
from .error_handlers import (
    ErrorHandler,
    ErrorResponse,
    handle_error,
    log_error,
    create_error_response,
    configure_error_handler,
    validation_exception_handler,
    application_exception_handler,
    generic_exception_handler
)

# Error codes
from .error_codes import ErrorCode

# Error handling decorators
from .decorators import (
    handle_exceptions,
    validate_input,
    require_auth,
    retry_on_error,
    measure_performance,
    circuit_breaker,
    database_transaction,
    standardize_errors,
    api_endpoint,
    data_processing,
    external_service
)

# Context managers
from .context_managers import (
    handle_validation_errors,
    database_operation,
    async_database_operation,
    external_service_call,
    data_processing_batch,
    business_operation,
    configuration_loading,
    error_suppression,
    timed_operation,
    resource_management,
    safe_call,
    async_safe_call
)

# Standardized patterns
from .patterns import (
    ErrorPattern,
    ValidationPattern,
    DatabasePattern,
    ExternalServicePattern,
    BusinessRulePattern,
    DataProcessingPattern,
    ErrorPatternManager,
    get_pattern_manager,
    get_validation_pattern,
    get_database_pattern,
    get_external_service_pattern,
    get_business_rule_pattern,
    get_data_processing_pattern,
    standardize_exception,
    create_error_chain
)

__all__ = [
    # Base exceptions
    'BaseApplicationError',
    'ValidationError',
    'BusinessRuleError',
    'ConfigurationError',
    'ExternalServiceError',

    # Legacy aliases
    'ConfigurationException',
    'ValidationException',
    'ETLException',
    'DataQualityException',

    # Security exceptions
    'SecurityError',
    'AuthenticationError',
    'AuthorizationError',

    # Data exceptions
    'DataProcessingError',
    'DatabaseError',
    'NotFoundError',
    'ConflictError',

    # Service exceptions
    'RateLimitError',
    'ServiceUnavailableError',

    # Error handling utilities
    'ErrorHandler',
    'ErrorResponse',
    'handle_error',
    'log_error',
    'create_error_response',
    'configure_error_handler',
    'validation_exception_handler',
    'application_exception_handler',
    'generic_exception_handler',

    # Error codes
    'ErrorCode',

    # Decorators
    'handle_exceptions',
    'validate_input',
    'require_auth',
    'retry_on_error',
    'measure_performance',
    'circuit_breaker',
    'database_transaction',
    'standardize_errors',
    'api_endpoint',
    'data_processing',
    'external_service',

    # Context managers
    'handle_validation_errors',
    'database_operation',
    'async_database_operation',
    'external_service_call',
    'data_processing_batch',
    'business_operation',
    'configuration_loading',
    'error_suppression',
    'timed_operation',
    'resource_management',
    'safe_call',
    'async_safe_call',

    # Patterns
    'ErrorPattern',
    'ValidationPattern',
    'DatabasePattern',
    'ExternalServicePattern',
    'BusinessRulePattern',
    'DataProcessingPattern',
    'ErrorPatternManager',
    'get_pattern_manager',
    'get_validation_pattern',
    'get_database_pattern',
    'get_external_service_pattern',
    'get_business_rule_pattern',
    'get_data_processing_pattern',
    'standardize_exception',
    'create_error_chain',
]