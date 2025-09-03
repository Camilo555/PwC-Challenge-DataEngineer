"""
Standardized API Response Models
Provides consistent response format across all API endpoints.
"""

from __future__ import annotations

import time
from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field, validator
from pydantic.generics import GenericModel

# Generic type for response data
T = TypeVar("T")


class ResponseStatus(str, Enum):
    """Standard response status codes."""

    SUCCESS = "success"
    ERROR = "error"
    WARNING = "warning"
    PARTIAL = "partial"


class ErrorCode(str, Enum):
    """Standard error codes for consistent error handling."""

    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    AUTHORIZATION_ERROR = "AUTHORIZATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    BAD_REQUEST = "BAD_REQUEST"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    DEPENDENCY_ERROR = "DEPENDENCY_ERROR"


class ErrorDetail(BaseModel):
    """Detailed error information."""

    code: ErrorCode
    message: str
    field: str | None = None
    details: dict[str, Any] | None = None


class PaginationMetadata(BaseModel):
    """Pagination metadata for paginated responses."""

    page: int = Field(..., ge=1, description="Current page number")
    page_size: int = Field(..., ge=1, le=1000, description="Number of items per page")
    total_count: int = Field(..., ge=0, description="Total number of items")
    total_pages: int = Field(..., ge=0, description="Total number of pages")
    has_next: bool = Field(..., description="Whether there are more pages")
    has_previous: bool = Field(..., description="Whether there are previous pages")

    @validator("total_pages", pre=False, always=True)
    def calculate_total_pages(cls, v, values):
        """Calculate total pages from total_count and page_size."""
        if "total_count" in values and "page_size" in values:
            import math

            return math.ceil(values["total_count"] / values["page_size"])
        return v


class PerformanceMetrics(BaseModel):
    """Performance metrics for API responses."""

    execution_time_ms: float = Field(..., description="Query execution time in milliseconds")
    cache_hit: bool = Field(False, description="Whether the result came from cache")
    database_queries: int = Field(0, description="Number of database queries executed")
    items_returned: int = Field(0, description="Number of items in the response")
    rate_limit_remaining: int | None = Field(None, description="Remaining rate limit count")


class APIMetadata(BaseModel):
    """Standard metadata for API responses."""

    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    request_id: str | None = Field(None, description="Unique request identifier")
    correlation_id: str | None = Field(None, description="Request correlation ID")
    api_version: str = Field("4.0.0", description="API version")
    environment: str | None = Field(None, description="Environment (dev/staging/prod)")
    performance: PerformanceMetrics | None = Field(None, description="Performance metrics")
    warnings: list[str] | None = Field(None, description="Non-critical warnings")


class APIResponse(GenericModel, Generic[T]):
    """
    Standardized API response wrapper.

    This provides a consistent response format across all API endpoints with:
    - Success/error status indication
    - Standardized error handling
    - Performance metrics
    - Request tracing information
    - Optional pagination metadata
    """

    status: ResponseStatus = Field(..., description="Response status")
    data: T | None = Field(None, description="Response data")
    errors: list[ErrorDetail] | None = Field(None, description="List of errors")
    metadata: APIMetadata = Field(default_factory=APIMetadata, description="Response metadata")
    pagination: PaginationMetadata | None = Field(
        None, description="Pagination info for paginated responses"
    )

    @validator("data")
    def validate_data_with_status(cls, v, values):
        """Ensure data is present for successful responses."""
        if values.get("status") == ResponseStatus.SUCCESS and v is None:
            # Allow None data for some operations like deletions
            pass
        return v

    @validator("errors")
    def validate_errors_with_status(cls, v, values):
        """Ensure errors are present for error responses."""
        if values.get("status") == ResponseStatus.ERROR and (not v or len(v) == 0):
            raise ValueError("Error responses must include error details")
        return v

    class Config:
        """Pydantic configuration."""

        use_enum_values = True
        validate_assignment = True


# Convenience classes for common response patterns


class SuccessResponse(APIResponse[T]):
    """Pre-configured success response."""

    status: ResponseStatus = ResponseStatus.SUCCESS


class ErrorResponse(APIResponse[None]):
    """Pre-configured error response."""

    status: ResponseStatus = ResponseStatus.ERROR
    data: None = None

    def __init__(self, errors: list[ErrorDetail], **kwargs):
        super().__init__(errors=errors, **kwargs)


class PaginatedResponse(APIResponse[list[T]]):
    """Pre-configured paginated response."""

    status: ResponseStatus = ResponseStatus.SUCCESS
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")


# Response builder utilities


class ResponseBuilder:
    """Utility class for building standardized API responses."""

    @staticmethod
    def success(
        data: T,
        request_id: str | None = None,
        correlation_id: str | None = None,
        performance_metrics: PerformanceMetrics | None = None,
        warnings: list[str] | None = None,
    ) -> APIResponse[T]:
        """Build a success response."""
        metadata = APIMetadata(
            request_id=request_id,
            correlation_id=correlation_id,
            performance=performance_metrics,
            warnings=warnings,
        )

        return APIResponse[T](status=ResponseStatus.SUCCESS, data=data, metadata=metadata)

    @staticmethod
    def error(
        errors: ErrorDetail | list[ErrorDetail],
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> ErrorResponse:
        """Build an error response."""
        if isinstance(errors, ErrorDetail):
            errors = [errors]

        metadata = APIMetadata(request_id=request_id, correlation_id=correlation_id)

        return ErrorResponse(errors=errors, metadata=metadata)

    @staticmethod
    def validation_error(
        message: str,
        field: str | None = None,
        details: dict[str, Any] | None = None,
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> ErrorResponse:
        """Build a validation error response."""
        error = ErrorDetail(
            code=ErrorCode.VALIDATION_ERROR, message=message, field=field, details=details
        )

        return ResponseBuilder.error([error], request_id, correlation_id)

    @staticmethod
    def not_found(
        resource: str,
        resource_id: str | None = None,
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> ErrorResponse:
        """Build a not found error response."""
        message = f"{resource} not found"
        if resource_id:
            message += f" with ID: {resource_id}"

        error = ErrorDetail(
            code=ErrorCode.NOT_FOUND,
            message=message,
            details={"resource": resource, "resource_id": resource_id},
        )

        return ResponseBuilder.error([error], request_id, correlation_id)

    @staticmethod
    def unauthorized(
        message: str = "Authentication required",
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> ErrorResponse:
        """Build an unauthorized error response."""
        error = ErrorDetail(code=ErrorCode.AUTHENTICATION_ERROR, message=message)

        return ResponseBuilder.error([error], request_id, correlation_id)

    @staticmethod
    def forbidden(
        message: str = "Insufficient permissions",
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> ErrorResponse:
        """Build a forbidden error response."""
        error = ErrorDetail(code=ErrorCode.AUTHORIZATION_ERROR, message=message)

        return ResponseBuilder.error([error], request_id, correlation_id)

    @staticmethod
    def rate_limit_exceeded(
        limit: int,
        reset_time: datetime | None = None,
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> ErrorResponse:
        """Build a rate limit exceeded error response."""
        details = {"limit": limit}
        if reset_time:
            details["reset_time"] = reset_time.isoformat()

        error = ErrorDetail(
            code=ErrorCode.RATE_LIMIT_EXCEEDED,
            message=f"Rate limit of {limit} requests exceeded",
            details=details,
        )

        return ResponseBuilder.error([error], request_id, correlation_id)

    @staticmethod
    def paginated(
        items: list[T],
        page: int,
        page_size: int,
        total_count: int,
        request_id: str | None = None,
        correlation_id: str | None = None,
        performance_metrics: PerformanceMetrics | None = None,
    ) -> PaginatedResponse[T]:
        """Build a paginated response."""
        import math

        total_pages = math.ceil(total_count / page_size) if page_size > 0 else 0
        has_next = page < total_pages
        has_previous = page > 1

        pagination = PaginationMetadata(
            page=page,
            page_size=page_size,
            total_count=total_count,
            total_pages=total_pages,
            has_next=has_next,
            has_previous=has_previous,
        )

        metadata = APIMetadata(
            request_id=request_id, correlation_id=correlation_id, performance=performance_metrics
        )

        return PaginatedResponse[T](data=items, pagination=pagination, metadata=metadata)


# Performance tracking utilities


class PerformanceTracker:
    """Utility for tracking API performance metrics."""

    def __init__(self):
        self.start_time = time.time()
        self.db_queries = 0
        self.cache_hit = False

    def add_db_query(self):
        """Increment database query count."""
        self.db_queries += 1

    def set_cache_hit(self, hit: bool = True):
        """Set cache hit status."""
        self.cache_hit = hit

    def get_metrics(
        self, items_count: int = 0, rate_limit_remaining: int | None = None
    ) -> PerformanceMetrics:
        """Get performance metrics."""
        execution_time = (time.time() - self.start_time) * 1000  # Convert to milliseconds

        return PerformanceMetrics(
            execution_time_ms=round(execution_time, 2),
            cache_hit=self.cache_hit,
            database_queries=self.db_queries,
            items_returned=items_count,
            rate_limit_remaining=rate_limit_remaining,
        )


# Common response types for reuse


# Health check response
class HealthCheckData(BaseModel):
    """Health check response data."""

    status: str
    version: str
    environment: str
    timestamp: datetime
    dependencies: dict[str, str]


# Task status response
class TaskStatusData(BaseModel):
    """Task status response data."""

    task_id: str
    status: str
    progress: int | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime


# Export common types
__all__ = [
    "APIResponse",
    "SuccessResponse",
    "ErrorResponse",
    "PaginatedResponse",
    "ResponseBuilder",
    "PerformanceTracker",
    "ResponseStatus",
    "ErrorCode",
    "ErrorDetail",
    "PaginationMetadata",
    "PerformanceMetrics",
    "APIMetadata",
    "HealthCheckData",
    "TaskStatusData",
]
