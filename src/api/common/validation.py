"""
Comprehensive Request/Response Validation
Advanced Pydantic validation with custom validators and error handling.
"""

from __future__ import annotations

import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from functools import wraps
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field, root_validator, validator
from pydantic.error_wrappers import ValidationError

from api.common.response_models import ErrorCode, ErrorDetail, ResponseBuilder
from core.logging import get_logger

logger = get_logger(__name__)


# Custom validators for common business rules


def validate_email(value: str) -> str:
    """Validate email format."""
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(pattern, value):
        raise ValueError("Invalid email format")
    return value.lower()


def validate_phone_number(value: str) -> str:
    """Validate phone number format."""
    # Remove all non-digit characters
    digits_only = re.sub(r"\D", "", value)

    # Check if it's a valid length (7-15 digits as per E.164)
    if not (7 <= len(digits_only) <= 15):
        raise ValueError("Phone number must be between 7 and 15 digits")

    return digits_only


def validate_currency_code(value: str) -> str:
    """Validate ISO 4217 currency code."""
    valid_codes = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "SEK", "NZD"}
    if value.upper() not in valid_codes:
        raise ValueError(f"Invalid currency code. Must be one of: {', '.join(valid_codes)}")
    return value.upper()


def validate_country_code(value: str) -> str:
    """Validate ISO 3166-1 alpha-2 country code."""
    if len(value) != 2 or not value.isalpha():
        raise ValueError("Country code must be a 2-letter ISO 3166-1 alpha-2 code")
    return value.upper()


def validate_positive_decimal(value: float | str | Decimal) -> Decimal:
    """Validate and convert to positive decimal."""
    try:
        decimal_value = Decimal(str(value))
        if decimal_value <= 0:
            raise ValueError("Value must be positive")
        return decimal_value
    except (InvalidOperation, TypeError) as e:
        raise ValueError("Invalid decimal value") from e


def validate_date_range(date_from: date | None, date_to: date | None) -> tuple:
    """Validate date range."""
    if date_from and date_to:
        if date_from > date_to:
            raise ValueError("date_from cannot be later than date_to")

        # Check for reasonable date ranges (not more than 5 years)
        if (date_to - date_from).days > 1825:  # 5 years
            raise ValueError("Date range cannot exceed 5 years")

    return date_from, date_to


# Base validation models


class PaginationParams(BaseModel):
    """Standard pagination parameters with validation."""

    page: int = Field(1, ge=1, le=10000, description="Page number (1-based)")
    page_size: int = Field(20, ge=1, le=1000, description="Number of items per page")

    @validator("page_size")
    def validate_reasonable_page_size(cls, v):
        """Ensure page size is reasonable for performance."""
        if v > 100:
            logger.warning(f"Large page size requested: {v}")
        return v


class DateRangeParams(BaseModel):
    """Date range parameters with validation."""

    date_from: date | None = Field(None, description="Start date (inclusive)")
    date_to: date | None = Field(None, description="End date (inclusive)")

    @root_validator
    def validate_date_range(cls, values):
        """Validate date range."""
        date_from = values.get("date_from")
        date_to = values.get("date_to")

        if date_from and date_to:
            if date_from > date_to:
                raise ValueError("date_from must be before or equal to date_to")

            # Limit to reasonable range
            days_diff = (date_to - date_from).days
            if days_diff > 1825:  # 5 years
                raise ValueError("Date range cannot exceed 5 years")

            # Warn for large ranges that might impact performance
            if days_diff > 365:  # 1 year
                logger.warning(f"Large date range requested: {days_diff} days")

        return values


class SalesFilterParams(BaseModel):
    """Sales filtering parameters with comprehensive validation."""

    country: str | None = Field(None, min_length=2, max_length=100, description="Country name")
    product_category: str | None = Field(
        None, min_length=1, max_length=100, description="Product category"
    )
    customer_segment: str | None = Field(
        None, min_length=1, max_length=50, description="Customer segment"
    )
    min_amount: Decimal | None = Field(None, ge=0, description="Minimum transaction amount")
    max_amount: Decimal | None = Field(None, ge=0, description="Maximum transaction amount")

    @validator("min_amount", "max_amount", pre=True)
    def validate_amounts(cls, v):
        """Validate monetary amounts."""
        if v is None:
            return v

        try:
            decimal_value = Decimal(str(v))
            if decimal_value < 0:
                raise ValueError("Amount cannot be negative")

            # Check for reasonable maximum (prevent potential abuse)
            if decimal_value > Decimal("1000000"):  # 1M limit
                raise ValueError("Amount exceeds maximum allowed value")

            return decimal_value
        except (InvalidOperation, TypeError) as e:
            raise ValueError("Invalid amount format") from e

    @root_validator
    def validate_amount_range(cls, values):
        """Validate amount range."""
        min_amount = values.get("min_amount")
        max_amount = values.get("max_amount")

        if min_amount and max_amount and min_amount > max_amount:
            raise ValueError("min_amount cannot be greater than max_amount")

        return values

    @validator("country", "product_category", "customer_segment")
    def validate_text_fields(cls, v):
        """Validate text fields for injection attacks."""
        if v is None:
            return v

        # Basic validation to prevent SQL injection and XSS
        dangerous_patterns = ["<script", "javascript:", "vbscript:", "--", ";", "union", "select"]
        v_lower = v.lower()

        for pattern in dangerous_patterns:
            if pattern in v_lower:
                raise ValueError("Invalid characters detected in field")

        return v.strip()


class TaskSubmissionParams(BaseModel):
    """Task submission parameters with validation."""

    task_name: str = Field(..., min_length=1, max_length=100, regex=r"^[a-zA-Z0-9_-]+$")
    parameters: dict[str, Any] = Field(..., description="Task parameters as JSON")
    priority: int = Field(5, ge=1, le=10, description="Task priority (1-10)")
    timeout: int | None = Field(None, ge=1, le=3600, description="Task timeout in seconds")

    @validator("task_name")
    def validate_task_name(cls, v):
        """Validate task name against allowed tasks."""
        allowed_tasks = {
            "data_sync",
            "analytics_refresh",
            "customer_segmentation",
            "sales_report",
            "data_quality_check",
            "backup_database",
        }

        if v not in allowed_tasks:
            raise ValueError(f"Invalid task name. Allowed tasks: {', '.join(allowed_tasks)}")

        return v

    @validator("parameters")
    def validate_parameters(cls, v):
        """Validate task parameters."""
        # Ensure parameters is a valid dict
        if not isinstance(v, dict):
            raise ValueError("Parameters must be a dictionary")

        # Check for reasonable size to prevent abuse
        if len(json.dumps(v)) > 10000:  # 10KB limit
            raise ValueError("Parameters exceed maximum size limit")

        # Validate no dangerous content in parameters
        param_str = json.dumps(v).lower()
        dangerous_patterns = ["<script", "javascript:", "import ", "exec(", "eval("]

        for pattern in dangerous_patterns:
            if pattern in param_str:
                raise ValueError("Invalid content in parameters")

        return v


# Custom validation decorators


def validate_request_size(max_size_mb: int = 10):
    """Decorator to validate request content size."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request from args (assuming it's the first parameter)
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break

            if request:
                content_length = request.headers.get("content-length")
                if content_length:
                    size_mb = int(content_length) / (1024 * 1024)
                    if size_mb > max_size_mb:
                        raise HTTPException(
                            status_code=413,
                            detail=f"Request too large. Maximum size: {max_size_mb}MB",
                        )

            return await func(*args, **kwargs)

        return wrapper

    return decorator


def validate_content_type(allowed_types: list[str]):
    """Decorator to validate request content type."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break

            if request:
                content_type = request.headers.get("content-type", "").lower()
                if not any(allowed_type in content_type for allowed_type in allowed_types):
                    raise HTTPException(
                        status_code=415,
                        detail=f"Unsupported content type. Allowed: {', '.join(allowed_types)}",
                    )

            return await func(*args, **kwargs)

        return wrapper

    return decorator


# Validation error handlers


class ValidationErrorHandler:
    """Handle and format validation errors consistently."""

    @staticmethod
    def format_validation_error(error: ValidationError) -> list[ErrorDetail]:
        """Format Pydantic validation error into standard error format."""
        errors = []

        for error_detail in error.errors():
            field_path = " -> ".join(str(loc) for loc in error_detail["loc"])

            error_obj = ErrorDetail(
                code=ErrorCode.VALIDATION_ERROR,
                message=error_detail["msg"],
                field=field_path,
                details={
                    "type": error_detail["type"],
                    "input": error_detail.get("input"),
                    "context": error_detail.get("ctx", {}),
                },
            )
            errors.append(error_obj)

        return errors

    @staticmethod
    async def validation_exception_handler(
        request: Request, exc: ValidationError
    ) -> ORJSONResponse:
        """FastAPI exception handler for validation errors."""
        correlation_id = getattr(request.state, "correlation_id", None)

        errors = ValidationErrorHandler.format_validation_error(exc)
        error_response = ResponseBuilder.error(errors, correlation_id=correlation_id)

        logger.warning(
            f"Validation error: {len(errors)} errors",
            extra={
                "correlation_id": correlation_id,
                "path": str(request.url.path),
                "method": request.method,
                "errors": [error.dict() for error in errors],
            },
        )

        return ORJSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content=error_response.dict()
        )


# Advanced validation utilities


class BusinessRuleValidator:
    """Advanced business rule validation."""

    @staticmethod
    def validate_customer_data(customer_data: dict[str, Any]) -> list[str]:
        """Validate customer data against business rules."""
        warnings = []

        # Check for potential duplicate indicators
        if customer_data.get("email") and customer_data.get("phone"):
            # This would typically involve database checks
            pass

        # Validate customer lifetime value consistency
        ltv = customer_data.get("lifetime_value", 0)
        total_spent = customer_data.get("total_spent", 0)

        if ltv > 0 and total_spent > 0 and ltv < total_spent * 0.1:
            warnings.append("Customer lifetime value seems unusually low compared to total spent")

        return warnings

    @staticmethod
    def validate_sales_data(sales_data: dict[str, Any]) -> list[str]:
        """Validate sales data against business rules."""
        warnings = []

        # Check for unusual profit margins
        total_amount = sales_data.get("total_amount", 0)
        profit_amount = sales_data.get("profit_amount", 0)

        if total_amount > 0 and profit_amount > 0:
            margin = profit_amount / total_amount
            if margin > 0.9:  # 90% margin
                warnings.append("Unusually high profit margin detected")
            elif margin < 0.01:  # 1% margin
                warnings.append("Unusually low profit margin detected")

        # Check for reasonable quantities
        quantity = sales_data.get("quantity", 0)
        if quantity > 1000:
            warnings.append("Large quantity order detected")

        return warnings


# Validation middleware


class RequestValidationMiddleware:
    """Middleware for request validation and sanitization."""

    def __init__(self, max_request_size_mb: int = 10):
        self.max_request_size_mb = max_request_size_mb

    async def __call__(self, request: Request, call_next):
        """Validate incoming requests."""

        # Check request size
        content_length = request.headers.get("content-length")
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > self.max_request_size_mb:
                error_response = ResponseBuilder.validation_error(
                    f"Request too large. Maximum size: {self.max_request_size_mb}MB",
                    correlation_id=getattr(request.state, "correlation_id", None),
                )
                return ORJSONResponse(status_code=413, content=error_response.dict())

        # Check for suspicious headers
        user_agent = request.headers.get("user-agent", "").lower()
        suspicious_agents = ["bot", "crawler", "scanner", "attack"]

        if any(agent in user_agent for agent in suspicious_agents):
            logger.warning(
                f"Suspicious user agent: {user_agent}",
                extra={
                    "client_ip": getattr(request.state, "client_ip", "unknown"),
                    "path": str(request.url.path),
                },
            )

        return await call_next(request)


# Export validation utilities
__all__ = [
    "PaginationParams",
    "DateRangeParams",
    "SalesFilterParams",
    "TaskSubmissionParams",
    "ValidationErrorHandler",
    "BusinessRuleValidator",
    "RequestValidationMiddleware",
    "validate_email",
    "validate_phone_number",
    "validate_currency_code",
    "validate_country_code",
    "validate_positive_decimal",
    "validate_request_size",
    "validate_content_type",
]
