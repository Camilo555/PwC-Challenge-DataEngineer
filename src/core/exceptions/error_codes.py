"""
Error Codes Enumeration
=======================

Standardized error codes for consistent error identification and handling.
Codes are organized by category and severity level.
"""

from enum import Enum


class ErrorCode(str, Enum):
    """
    Standardized error codes for the application.

    Format: [CATEGORY]_[SEVERITY]_[SPECIFIC_ERROR]
    Categories: VALIDATION, BUSINESS, AUTH, DATA, DATABASE, EXTERNAL, SYSTEM
    Severity: ERROR, WARNING, INFO, CRITICAL
    """

    # Validation Errors (4xx client errors)
    VALIDATION_ERROR = "VALIDATION_ERROR_001"
    INVALID_INPUT_FORMAT = "VALIDATION_ERROR_002"
    MISSING_REQUIRED_FIELD = "VALIDATION_ERROR_003"
    INVALID_EMAIL_FORMAT = "VALIDATION_ERROR_004"
    INVALID_PHONE_FORMAT = "VALIDATION_ERROR_005"
    INVALID_DATE_RANGE = "VALIDATION_ERROR_006"
    INVALID_FILE_TYPE = "VALIDATION_ERROR_007"
    FILE_SIZE_EXCEEDED = "VALIDATION_ERROR_008"
    INVALID_JSON_FORMAT = "VALIDATION_ERROR_009"
    SCHEMA_VALIDATION_FAILED = "VALIDATION_ERROR_010"

    # Business Rule Violations
    BUSINESS_RULE_VIOLATION = "BUSINESS_ERROR_001"
    INSUFFICIENT_FUNDS = "BUSINESS_ERROR_002"
    INVALID_BUSINESS_HOURS = "BUSINESS_ERROR_003"
    DUPLICATE_TRANSACTION = "BUSINESS_ERROR_004"
    INVALID_ORDER_STATUS = "BUSINESS_ERROR_005"
    PRODUCT_OUT_OF_STOCK = "BUSINESS_ERROR_006"
    CUSTOMER_LIMIT_EXCEEDED = "BUSINESS_ERROR_007"
    INVALID_PROMOTION_CODE = "BUSINESS_ERROR_008"
    EXPIRED_PROMOTION = "BUSINESS_ERROR_009"
    MINIMUM_ORDER_NOT_MET = "BUSINESS_ERROR_010"

    # Authentication & Authorization Errors
    AUTHENTICATION_ERROR = "AUTH_ERROR_001"
    INVALID_CREDENTIALS = "AUTH_ERROR_002"
    TOKEN_EXPIRED = "AUTH_ERROR_003"
    TOKEN_INVALID = "AUTH_ERROR_004"
    AUTHORIZATION_ERROR = "AUTH_ERROR_005"
    INSUFFICIENT_PERMISSIONS = "AUTH_ERROR_006"
    ACCOUNT_LOCKED = "AUTH_ERROR_007"
    ACCOUNT_SUSPENDED = "AUTH_ERROR_008"
    SESSION_EXPIRED = "AUTH_ERROR_009"
    MFA_REQUIRED = "AUTH_ERROR_010"
    INVALID_API_KEY = "AUTH_ERROR_011"
    API_KEY_EXPIRED = "AUTH_ERROR_012"

    # Data Processing Errors
    DATA_PROCESSING_ERROR = "DATA_ERROR_001"
    DATA_TRANSFORMATION_FAILED = "DATA_ERROR_002"
    DATA_VALIDATION_FAILED = "DATA_ERROR_003"
    DATA_QUALITY_CHECK_FAILED = "DATA_ERROR_004"
    DUPLICATE_DATA_DETECTED = "DATA_ERROR_005"
    DATA_INCONSISTENCY = "DATA_ERROR_006"
    MISSING_DATA_SOURCE = "DATA_ERROR_007"
    CORRUPTED_DATA = "DATA_ERROR_008"
    DATA_PIPELINE_FAILED = "DATA_ERROR_009"
    ETL_PROCESS_FAILED = "DATA_ERROR_010"

    # Database Errors
    DATABASE_ERROR = "DATABASE_ERROR_001"
    CONNECTION_FAILED = "DATABASE_ERROR_002"
    QUERY_EXECUTION_FAILED = "DATABASE_ERROR_003"
    TRANSACTION_FAILED = "DATABASE_ERROR_004"
    CONSTRAINT_VIOLATION = "DATABASE_ERROR_005"
    DEADLOCK_DETECTED = "DATABASE_ERROR_006"
    TIMEOUT_ERROR = "DATABASE_ERROR_007"
    MIGRATION_FAILED = "DATABASE_ERROR_008"
    BACKUP_FAILED = "DATABASE_ERROR_009"
    RECOVERY_FAILED = "DATABASE_ERROR_010"

    # External Service Errors
    EXTERNAL_SERVICE_ERROR = "EXTERNAL_ERROR_001"
    SERVICE_UNAVAILABLE = "EXTERNAL_ERROR_002"
    SERVICE_TIMEOUT = "EXTERNAL_ERROR_003"
    INVALID_SERVICE_RESPONSE = "EXTERNAL_ERROR_004"
    SERVICE_RATE_LIMITED = "EXTERNAL_ERROR_005"
    PAYMENT_GATEWAY_ERROR = "EXTERNAL_ERROR_006"
    SHIPPING_SERVICE_ERROR = "EXTERNAL_ERROR_007"
    EMAIL_SERVICE_ERROR = "EXTERNAL_ERROR_008"
    SMS_SERVICE_ERROR = "EXTERNAL_ERROR_009"
    THIRD_PARTY_API_ERROR = "EXTERNAL_ERROR_010"

    # System/Infrastructure Errors
    INTERNAL_ERROR = "SYSTEM_ERROR_001"
    CONFIGURATION_ERROR = "SYSTEM_ERROR_002"
    RESOURCE_EXHAUSTED = "SYSTEM_ERROR_003"
    MEMORY_ERROR = "SYSTEM_ERROR_004"
    DISK_SPACE_ERROR = "SYSTEM_ERROR_005"
    NETWORK_ERROR = "SYSTEM_ERROR_006"
    SECURITY_ERROR = "SYSTEM_ERROR_007"
    ENCRYPTION_ERROR = "SYSTEM_ERROR_008"
    CACHE_ERROR = "SYSTEM_ERROR_009"
    LOGGING_ERROR = "SYSTEM_ERROR_010"

    # HTTP Status Related Errors
    NOT_FOUND = "HTTP_ERROR_404"
    METHOD_NOT_ALLOWED = "HTTP_ERROR_405"
    CONFLICT = "HTTP_ERROR_409"
    UNPROCESSABLE_ENTITY = "HTTP_ERROR_422"
    TOO_MANY_REQUESTS = "HTTP_ERROR_429"
    RATE_LIMIT_EXCEEDED = "HTTP_ERROR_429"

    # Critical System Errors
    CRITICAL_SYSTEM_FAILURE = "CRITICAL_ERROR_001"
    DATA_CORRUPTION = "CRITICAL_ERROR_002"
    SECURITY_BREACH = "CRITICAL_ERROR_003"
    BACKUP_FAILURE = "CRITICAL_ERROR_004"
    DISASTER_RECOVERY_NEEDED = "CRITICAL_ERROR_005"

    # Warning Level Events
    PERFORMANCE_DEGRADATION = "WARNING_001"
    CAPACITY_THRESHOLD_REACHED = "WARNING_002"
    DEPRECATED_API_USAGE = "WARNING_003"
    MAINTENANCE_WINDOW_APPROACHING = "WARNING_004"
    LICENSE_EXPIRING = "WARNING_005"

    # Information Level Events
    OPERATION_COMPLETED = "INFO_001"
    USER_ACTION_LOGGED = "INFO_002"
    SCHEDULED_TASK_COMPLETED = "INFO_003"
    CACHE_REFRESHED = "INFO_004"
    BACKUP_COMPLETED = "INFO_005"

    @classmethod
    def get_severity(cls, error_code: 'ErrorCode') -> str:
        """Get the severity level of an error code."""
        code_value = error_code.value

        if code_value.startswith('CRITICAL'):
            return 'CRITICAL'
        elif code_value.startswith('WARNING'):
            return 'WARNING'
        elif code_value.startswith('INFO'):
            return 'INFO'
        else:
            return 'ERROR'

    @classmethod
    def get_category(cls, error_code: 'ErrorCode') -> str:
        """Get the category of an error code."""
        code_value = error_code.value

        if 'VALIDATION' in code_value:
            return 'VALIDATION'
        elif 'BUSINESS' in code_value:
            return 'BUSINESS'
        elif 'AUTH' in code_value:
            return 'AUTHENTICATION'
        elif 'DATA' in code_value:
            return 'DATA_PROCESSING'
        elif 'DATABASE' in code_value:
            return 'DATABASE'
        elif 'EXTERNAL' in code_value:
            return 'EXTERNAL_SERVICE'
        elif 'SYSTEM' in code_value:
            return 'SYSTEM'
        elif 'HTTP' in code_value:
            return 'HTTP'
        elif 'CRITICAL' in code_value:
            return 'CRITICAL'
        elif 'WARNING' in code_value:
            return 'WARNING'
        elif 'INFO' in code_value:
            return 'INFORMATION'
        else:
            return 'UNKNOWN'

    @classmethod
    def is_client_error(cls, error_code: 'ErrorCode') -> bool:
        """Check if error code represents a client error (4xx)."""
        category = cls.get_category(error_code)
        return category in ['VALIDATION', 'AUTHENTICATION', 'HTTP']

    @classmethod
    def is_server_error(cls, error_code: 'ErrorCode') -> bool:
        """Check if error code represents a server error (5xx)."""
        category = cls.get_category(error_code)
        return category in ['SYSTEM', 'DATABASE', 'EXTERNAL_SERVICE', 'CRITICAL']

    @classmethod
    def requires_retry(cls, error_code: 'ErrorCode') -> bool:
        """Check if this error type typically allows for retries."""
        retry_codes = [
            cls.SERVICE_TIMEOUT,
            cls.SERVICE_UNAVAILABLE,
            cls.NETWORK_ERROR,
            cls.DATABASE_ERROR,
            cls.EXTERNAL_SERVICE_ERROR,
            cls.RATE_LIMIT_EXCEEDED
        ]
        return error_code in retry_codes

    @classmethod
    def get_http_status_code(cls, error_code: 'ErrorCode') -> int:
        """Get appropriate HTTP status code for an error."""
        if error_code == cls.NOT_FOUND:
            return 404
        elif error_code in [cls.AUTHENTICATION_ERROR, cls.INVALID_CREDENTIALS, cls.TOKEN_EXPIRED]:
            return 401
        elif error_code in [cls.AUTHORIZATION_ERROR, cls.INSUFFICIENT_PERMISSIONS]:
            return 403
        elif error_code == cls.METHOD_NOT_ALLOWED:
            return 405
        elif error_code == cls.CONFLICT:
            return 409
        elif error_code in [cls.VALIDATION_ERROR, cls.UNPROCESSABLE_ENTITY]:
            return 422
        elif error_code == cls.RATE_LIMIT_EXCEEDED:
            return 429
        elif cls.is_client_error(error_code):
            return 400
        elif error_code in [cls.SERVICE_UNAVAILABLE, cls.EXTERNAL_SERVICE_ERROR]:
            return 503
        elif cls.is_server_error(error_code):
            return 500
        else:
            return 500  # Default to internal server error