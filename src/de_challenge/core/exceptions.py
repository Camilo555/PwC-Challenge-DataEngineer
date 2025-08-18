"""
Custom exception classes for the application.
Provides specific error types for better error handling and debugging.
"""

from typing import Any


class BaseApplicationException(Exception):
    """Base exception class for all application exceptions."""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize base exception.

        Args:
            message: Error message
            error_code: Optional error code for categorization
            details: Optional additional error details
        """
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for API responses."""
        return {
            "error": self.error_code,
            "message": self.message,
            "details": self.details,
        }


class ConfigurationException(BaseApplicationException):
    """Raised when configuration is invalid or missing."""

    pass


class ETLException(BaseApplicationException):
    """Base exception for ETL-related errors."""

    pass


class IngestionException(ETLException):
    """Raised when data ingestion fails."""

    pass


class TransformationException(ETLException):
    """Raised when data transformation fails."""

    pass


class ValidationException(BaseApplicationException):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        field: str | None = None,
        value: Any = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize validation exception.

        Args:
            message: Error message
            field: Field that failed validation
            value: Invalid value
            **kwargs: Additional arguments
        """
        details = kwargs.get("details", {})
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)
        kwargs["details"] = details
        super().__init__(message, **kwargs)


class DataQualityException(ValidationException):
    """Raised when data quality checks fail."""

    def __init__(
        self,
        message: str,
        failed_records: int | None = None,
        total_records: int | None = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize data quality exception.

        Args:
            message: Error message
            failed_records: Number of failed records
            total_records: Total number of records
            **kwargs: Additional arguments
        """
        details = kwargs.get("details", {})
        if failed_records is not None:
            details["failed_records"] = failed_records
        if total_records is not None:
            details["total_records"] = total_records
            if failed_records is not None:
                details["failure_rate"] = f"{(failed_records/total_records)*100:.2f}%"
        kwargs["details"] = details
        super().__init__(message, **kwargs)


class DatabaseException(BaseApplicationException):
    """Raised when database operations fail."""

    pass


class DatabaseConnectionError(DatabaseException):
    """Raised when database connection fails."""

    pass


class RepositoryException(DatabaseException):
    """Raised when repository operations fail."""

    pass


class APIException(BaseApplicationException):
    """Base exception for API-related errors."""

    def __init__(
        self,
        message: str,
        status_code: int = 500,
        **kwargs: Any
    ) -> None:
        """
        Initialize API exception.

        Args:
            message: Error message
            status_code: HTTP status code
            **kwargs: Additional arguments
        """
        self.status_code = status_code
        super().__init__(message, **kwargs)


class AuthenticationException(APIException):
    """Raised when authentication fails."""

    def __init__(self, message: str = "Authentication failed", **kwargs: Any) -> None:
        super().__init__(message, status_code=401, **kwargs)


class AuthorizationException(APIException):
    """Raised when authorization fails."""

    def __init__(self, message: str = "Insufficient permissions", **kwargs: Any) -> None:
        super().__init__(message, status_code=403, **kwargs)


class NotFoundException(APIException):
    """Raised when requested resource is not found."""

    def __init__(self, message: str = "Resource not found", **kwargs: Any) -> None:
        super().__init__(message, status_code=404, **kwargs)


class RateLimitException(APIException):
    """Raised when rate limit is exceeded."""

    def __init__(self, message: str = "Rate limit exceeded", **kwargs: Any) -> None:
        super().__init__(message, status_code=429, **kwargs)


class VectorSearchException(BaseApplicationException):
    """Raised when vector search operations fail."""

    pass


class EmbeddingException(VectorSearchException):
    """Raised when embedding generation fails."""

    pass


class IndexException(VectorSearchException):
    """Raised when index operations fail."""

    pass


class SparkException(ETLException):
    """Raised when Spark operations fail."""

    pass


class DeltaLakeException(ETLException):
    """Raised when Delta Lake operations fail."""

    pass


class FileProcessingException(BaseApplicationException):
    """Raised when file processing fails."""

    def __init__(
        self,
        message: str,
        file_path: str | None = None,
        file_type: str | None = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize file processing exception.

        Args:
            message: Error message
            file_path: Path to the problematic file
            file_type: Type of the file
            **kwargs: Additional arguments
        """
        details = kwargs.get("details", {})
        if file_path:
            details["file_path"] = file_path
        if file_type:
            details["file_type"] = file_type
        kwargs["details"] = details
        super().__init__(message, **kwargs)
