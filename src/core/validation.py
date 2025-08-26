"""
Common validation utilities for consistent data validation across the application.
Provides reusable validation functions and decorators.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime
from typing import Any, TypeVar
from uuid import UUID

T = TypeVar('T')


class ValidationError(Exception):
    """Custom validation error for business rules."""

    def __init__(self, message: str, field: str | None = None, value: Any = None):
        self.message = message
        self.field = field
        self.value = value
        super().__init__(message)


class DataValidationUtils:
    """Common data validation utilities."""

    @staticmethod
    def validate_email(email: str | None) -> str | None:
        """Validate email format."""
        if not email:
            return None

        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            raise ValidationError(f"Invalid email format: {email}", field="email", value=email)

        return email.lower().strip()

    @staticmethod
    def validate_phone(phone: str | None) -> str | None:
        """Validate phone number format."""
        if not phone:
            return None

        # Remove all non-digit characters
        cleaned_phone = re.sub(r'\D', '', phone)

        # Check if it's a valid length (7-15 digits)
        if not 7 <= len(cleaned_phone) <= 15:
            raise ValidationError(f"Invalid phone number length: {phone}", field="phone", value=phone)

        return cleaned_phone

    @staticmethod
    def validate_uuid(value: str | UUID | None) -> UUID | None:
        """Validate UUID format."""
        if not value:
            return None

        if isinstance(value, UUID):
            return value

        try:
            return UUID(str(value))
        except ValueError as e:
            raise ValidationError(f"Invalid UUID format: {value}", field="uuid", value=value) from e

    @staticmethod
    def validate_positive_number(value: int | float | None, field_name: str = "value") -> int | float | None:
        """Validate positive number."""
        if value is None:
            return None

        if not isinstance(value, (int, float)) or value <= 0:
            raise ValidationError(f"{field_name} must be a positive number", field=field_name, value=value)

        return value

    @staticmethod
    def validate_date_range(start_date: datetime, end_date: datetime) -> tuple[datetime, datetime]:
        """Validate date range."""
        if start_date >= end_date:
            raise ValidationError(
                "Start date must be before end date",
                field="date_range",
                value={"start": start_date, "end": end_date}
            )

        return start_date, end_date

    @staticmethod
    def validate_string_length(value: str | None, min_length: int = 0, max_length: int = 255, field_name: str = "value") -> str | None:
        """Validate string length."""
        if not value:
            if min_length > 0:
                raise ValidationError(f"{field_name} cannot be empty", field=field_name, value=value)
            return None

        if len(value) < min_length:
            raise ValidationError(
                f"{field_name} must be at least {min_length} characters long",
                field=field_name,
                value=value
            )

        if len(value) > max_length:
            raise ValidationError(
                f"{field_name} cannot exceed {max_length} characters",
                field=field_name,
                value=value
            )

        return value.strip()

    @staticmethod
    def validate_country_code(country_code: str | None) -> str | None:
        """Validate ISO country code."""
        if not country_code:
            return None

        # Validate 2 or 3 letter country codes
        if not re.match(r'^[A-Z]{2,3}$', country_code.upper()):
            raise ValidationError(f"Invalid country code format: {country_code}", field="country_code", value=country_code)

        return country_code.upper()

    @staticmethod
    def validate_currency_code(currency_code: str | None) -> str | None:
        """Validate ISO currency code."""
        if not currency_code:
            return None

        # Validate 3 letter currency codes
        if not re.match(r'^[A-Z]{3}$', currency_code.upper()):
            raise ValidationError(f"Invalid currency code format: {currency_code}", field="currency_code", value=currency_code)

        return currency_code.upper()


class BusinessRuleValidators:
    """Business-specific validation rules."""

    @staticmethod
    def validate_invoice_number(invoice_no: str | None) -> str | None:
        """Validate invoice number format."""
        if not invoice_no:
            return None

        # Remove whitespace
        invoice_no = invoice_no.strip()

        # Check basic format (alphanumeric, minimum 3 characters)
        if not re.match(r'^[A-Za-z0-9-_.]{3,}$', invoice_no):
            raise ValidationError(f"Invalid invoice number format: {invoice_no}", field="invoice_no", value=invoice_no)

        # Check for cancelled invoices (starting with 'C')
        if invoice_no.upper().startswith('C'):
            return None  # Cancelled invoice, should be filtered out

        return invoice_no.upper()

    @staticmethod
    def validate_stock_code(stock_code: str | None) -> str | None:
        """Validate stock/product code."""
        if not stock_code:
            return None

        stock_code = stock_code.strip()

        # Basic alphanumeric validation
        if not re.match(r'^[A-Za-z0-9-_.]{1,50}$', stock_code):
            raise ValidationError(f"Invalid stock code format: {stock_code}", field="stock_code", value=stock_code)

        return stock_code.upper()

    @staticmethod
    def validate_customer_id(customer_id: str | None) -> str | None:
        """Validate customer ID."""
        if not customer_id:
            return "GUEST"  # Default for anonymous customers

        customer_id = customer_id.strip()

        # Check for numeric customer IDs
        if customer_id.isdigit():
            return customer_id

        # Check for alphanumeric customer IDs
        if re.match(r'^[A-Za-z0-9-_.]{1,20}$', customer_id):
            return customer_id.upper()

        raise ValidationError(f"Invalid customer ID format: {customer_id}", field="customer_id", value=customer_id)

    @staticmethod
    def validate_quantity(quantity: int | float | None) -> int | None:
        """Validate product quantity."""
        if quantity is None:
            return None

        try:
            qty = int(quantity)
            if qty <= 0:
                raise ValidationError("Quantity must be positive", field="quantity", value=quantity)
            if qty > 100000:  # Reasonable upper limit
                raise ValidationError("Quantity exceeds maximum allowed", field="quantity", value=quantity)
            return qty
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid quantity format: {quantity}", field="quantity", value=quantity) from e

    @staticmethod
    def validate_unit_price(price: int | float | None) -> float | None:
        """Validate unit price."""
        if price is None:
            return None

        try:
            price_float = float(price)
            if price_float <= 0:
                raise ValidationError("Unit price must be positive", field="unit_price", value=price)
            if price_float > 1000000:  # Reasonable upper limit
                raise ValidationError("Unit price exceeds maximum allowed", field="unit_price", value=price)
            return round(price_float, 2)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid unit price format: {price}", field="unit_price", value=price) from e


class DataQualityValidators:
    """Data quality validation utilities."""

    @staticmethod
    def check_completeness(data: dict[str, Any], required_fields: list[str]) -> dict[str, Any]:
        """Check data completeness."""
        missing_fields = [field for field in required_fields if not data.get(field)]

        return {
            "complete": len(missing_fields) == 0,
            "missing_fields": missing_fields,
            "completeness_score": (len(required_fields) - len(missing_fields)) / len(required_fields)
        }

    @staticmethod
    def check_data_types(data: dict[str, Any], expected_types: dict[str, type]) -> dict[str, Any]:
        """Check data type consistency."""
        type_errors = []

        for field, expected_type in expected_types.items():
            if field in data and data[field] is not None:
                if not isinstance(data[field], expected_type):
                    type_errors.append({
                        "field": field,
                        "expected_type": expected_type.__name__,
                        "actual_type": type(data[field]).__name__,
                        "value": data[field]
                    })

        return {
            "valid_types": len(type_errors) == 0,
            "type_errors": type_errors,
            "type_consistency_score": max(0, (len(expected_types) - len(type_errors)) / len(expected_types))
        }

    @staticmethod
    def check_value_ranges(data: dict[str, Any], ranges: dict[str, dict[str, int | float]]) -> dict[str, Any]:
        """Check if values are within expected ranges."""
        range_errors = []

        for field, range_def in ranges.items():
            if field in data and data[field] is not None:
                value = data[field]
                min_val = range_def.get("min")
                max_val = range_def.get("max")

                if min_val is not None and value < min_val:
                    range_errors.append({
                        "field": field,
                        "error": "below_minimum",
                        "value": value,
                        "minimum": min_val
                    })

                if max_val is not None and value > max_val:
                    range_errors.append({
                        "field": field,
                        "error": "above_maximum",
                        "value": value,
                        "maximum": max_val
                    })

        return {
            "within_ranges": len(range_errors) == 0,
            "range_errors": range_errors,
            "range_validity_score": max(0, (len(ranges) - len(range_errors)) / len(ranges)) if ranges else 1.0
        }


def validation_decorator(validator_func: Callable[[T], T]) -> Callable[[Callable], Callable]:
    """Decorator for applying validation functions."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            try:
                return validator_func(result)
            except ValidationError as e:
                # Re-raise with context about which function failed
                e.message = f"Validation failed in {func.__name__}: {e.message}"
                raise
        return wrapper
    return decorator


# Convenience validators that can be used as Pydantic field validators
def email_validator(cls, v):
    """Pydantic field validator for email."""
    return DataValidationUtils.validate_email(v)


def phone_validator(cls, v):
    """Pydantic field validator for phone."""
    return DataValidationUtils.validate_phone(v)


def uuid_validator(cls, v):
    """Pydantic field validator for UUID."""
    return DataValidationUtils.validate_uuid(v)


def positive_number_validator(field_name: str = "value"):
    """Pydantic field validator for positive numbers."""
    def validator(cls, v):
        return DataValidationUtils.validate_positive_number(v, field_name)
    return validator


def string_length_validator(min_length: int = 0, max_length: int = 255, field_name: str = "value"):
    """Pydantic field validator for string length."""
    def validator(cls, v):
        return DataValidationUtils.validate_string_length(v, min_length, max_length, field_name)
    return validator


# Business rule validators for Pydantic
def invoice_number_validator(cls, v):
    """Pydantic field validator for invoice numbers."""
    return BusinessRuleValidators.validate_invoice_number(v)


def stock_code_validator(cls, v):
    """Pydantic field validator for stock codes."""
    return BusinessRuleValidators.validate_stock_code(v)


def customer_id_validator(cls, v):
    """Pydantic field validator for customer IDs."""
    return BusinessRuleValidators.validate_customer_id(v)


def quantity_validator(cls, v):
    """Pydantic field validator for quantities."""
    return BusinessRuleValidators.validate_quantity(v)


def unit_price_validator(cls, v):
    """Pydantic field validator for unit prices."""
    return BusinessRuleValidators.validate_unit_price(v)
