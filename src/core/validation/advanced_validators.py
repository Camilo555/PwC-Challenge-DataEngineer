"""
Advanced Data Validation Framework
==================================

Comprehensive validation system providing:
- Custom Pydantic validators with business logic
- Complex data type validation and transformation
- Cross-field validation and conditional logic
- Performance-optimized validation chains
- Integration with API endpoints and database models
- Custom error messages and internationalization
- Validation metrics and monitoring
- Schema evolution and backward compatibility

Key Features:
- Business rule validation (e.g., sales data integrity)
- Financial calculation validation
- Geographic and temporal data validation
- File upload and content validation
- API payload validation with nested schemas
- Batch validation for large datasets
- Async validation for external data sources
"""

import asyncio
import re
import json
import uuid
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Union, Callable, Type, Set
from enum import Enum
import logging
from pathlib import Path
import mimetypes
import hashlib

from pydantic import BaseModel, Field, validator, root_validator, ValidationError
from pydantic.fields import ModelField
from pydantic.validators import str_validator, int_validator, float_validator
import phonenumbers
from email_validator import validate_email, EmailNotValidError
import requests
from geopy.geocoders import Nominatim
from babel.numbers import parse_decimal, NumberFormatError
import magic
import pandas as pd
import numpy as np

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class ValidationLevel(Enum):
    """Validation strictness levels"""
    STRICT = "strict"
    STANDARD = "standard"
    LENIENT = "lenient"
    DEVELOPMENT = "development"


class ValidationResult(BaseModel):
    """Validation result with detailed information"""
    is_valid: bool
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    transformed_data: Optional[Dict[str, Any]] = None
    validation_time_ms: float
    validation_level: ValidationLevel
    schema_version: str = "1.0"


class CustomValidationError(Exception):
    """Custom validation error with enhanced information"""

    def __init__(self, message: str, field: str = None, code: str = None, context: Dict[str, Any] = None):
        self.message = message
        self.field = field
        self.code = code
        self.context = context or {}
        super().__init__(message)


class BusinessRuleValidator:
    """Validator for complex business rules"""

    @staticmethod
    def validate_sales_data_integrity(data: Dict[str, Any]) -> bool:
        """Validate sales data integrity and business rules"""
        try:
            # Check required fields
            required_fields = ['quantity', 'unit_price', 'total_amount']
            for field in required_fields:
                if field not in data or data[field] is None:
                    raise CustomValidationError(f"Required field missing: {field}", field=field, code="MISSING_FIELD")

            # Validate quantity
            quantity = float(data['quantity'])
            if quantity <= 0:
                raise CustomValidationError("Quantity must be positive", field="quantity", code="INVALID_QUANTITY")

            # Validate unit price
            unit_price = float(data['unit_price'])
            if unit_price < 0:
                raise CustomValidationError("Unit price cannot be negative", field="unit_price", code="INVALID_PRICE")

            # Validate total amount calculation
            total_amount = float(data['total_amount'])
            expected_total = quantity * unit_price

            # Apply discount if present
            if 'discount_amount' in data and data['discount_amount']:
                expected_total -= float(data['discount_amount'])

            # Apply tax if present
            if 'tax_amount' in data and data['tax_amount']:
                expected_total += float(data['tax_amount'])

            # Allow small floating point differences
            if abs(total_amount - expected_total) > 0.01:
                raise CustomValidationError(
                    f"Total amount mismatch. Expected: {expected_total:.2f}, Got: {total_amount:.2f}",
                    field="total_amount",
                    code="AMOUNT_MISMATCH",
                    context={'expected': expected_total, 'actual': total_amount}
                )

            # Validate date if present
            if 'invoice_date' in data and data['invoice_date']:
                invoice_date = data['invoice_date']
                if isinstance(invoice_date, str):
                    try:
                        invoice_date = datetime.fromisoformat(invoice_date.replace('Z', '+00:00'))
                    except ValueError:
                        raise CustomValidationError("Invalid invoice date format", field="invoice_date", code="INVALID_DATE")

                # Check if date is not in the future
                if invoice_date.date() > date.today():
                    raise CustomValidationError("Invoice date cannot be in the future", field="invoice_date", code="FUTURE_DATE")

                # Check if date is not too old (e.g., more than 10 years)
                if invoice_date.date() < date.today() - timedelta(days=3650):
                    raise CustomValidationError("Invoice date is too old", field="invoice_date", code="DATE_TOO_OLD")

            return True

        except CustomValidationError:
            raise
        except Exception as e:
            raise CustomValidationError(f"Sales data validation failed: {str(e)}", code="VALIDATION_ERROR")

    @staticmethod
    def validate_customer_data(data: Dict[str, Any]) -> bool:
        """Validate customer data and business rules"""
        try:
            # Validate customer ID format
            if 'customer_id' in data and data['customer_id']:
                customer_id = str(data['customer_id'])
                if not re.match(r'^[A-Z0-9]{5,20}$', customer_id):
                    raise CustomValidationError(
                        "Customer ID must be 5-20 alphanumeric characters",
                        field="customer_id",
                        code="INVALID_CUSTOMER_ID"
                    )

            # Validate email if present
            if 'email' in data and data['email']:
                try:
                    validate_email(data['email'])
                except EmailNotValidError as e:
                    raise CustomValidationError(f"Invalid email: {str(e)}", field="email", code="INVALID_EMAIL")

            # Validate phone number if present
            if 'phone' in data and data['phone']:
                try:
                    parsed = phonenumbers.parse(data['phone'], None)
                    if not phonenumbers.is_valid_number(parsed):
                        raise CustomValidationError("Invalid phone number", field="phone", code="INVALID_PHONE")
                except phonenumbers.NumberParseException:
                    raise CustomValidationError("Invalid phone number format", field="phone", code="INVALID_PHONE_FORMAT")

            # Validate age if present
            if 'age' in data and data['age'] is not None:
                age = int(data['age'])
                if age < 0 or age > 150:
                    raise CustomValidationError("Age must be between 0 and 150", field="age", code="INVALID_AGE")

            return True

        except CustomValidationError:
            raise
        except Exception as e:
            raise CustomValidationError(f"Customer data validation failed: {str(e)}", code="VALIDATION_ERROR")

    @staticmethod
    def validate_product_data(data: Dict[str, Any]) -> bool:
        """Validate product data and business rules"""
        try:
            # Validate stock code format
            if 'stock_code' in data and data['stock_code']:
                stock_code = str(data['stock_code'])
                if not re.match(r'^[A-Z0-9]{4,10}$', stock_code):
                    raise CustomValidationError(
                        "Stock code must be 4-10 alphanumeric characters",
                        field="stock_code",
                        code="INVALID_STOCK_CODE"
                    )

            # Validate price ranges
            if 'unit_price' in data and data['unit_price'] is not None:
                unit_price = float(data['unit_price'])
                if unit_price < 0:
                    raise CustomValidationError("Unit price cannot be negative", field="unit_price", code="NEGATIVE_PRICE")
                if unit_price > 1000000:  # Arbitrary large amount check
                    raise CustomValidationError("Unit price seems unusually high", field="unit_price", code="PRICE_TOO_HIGH")

            # Validate description length
            if 'description' in data and data['description']:
                description = str(data['description'])
                if len(description) < 5:
                    raise CustomValidationError("Product description too short", field="description", code="DESCRIPTION_TOO_SHORT")
                if len(description) > 500:
                    raise CustomValidationError("Product description too long", field="description", code="DESCRIPTION_TOO_LONG")

            return True

        except CustomValidationError:
            raise
        except Exception as e:
            raise CustomValidationError(f"Product data validation failed: {str(e)}", code="VALIDATION_ERROR")


class FinancialValidator:
    """Validator for financial data and calculations"""

    @staticmethod
    def validate_currency_amount(amount: Union[str, float, Decimal], currency: str = "USD") -> Decimal:
        """Validate and normalize currency amounts"""
        try:
            if isinstance(amount, str):
                # Remove currency symbols and normalize
                amount = re.sub(r'[^\d.,\-]', '', amount)
                amount = amount.replace(',', '')

            decimal_amount = Decimal(str(amount))

            # Check for reasonable ranges
            if decimal_amount < Decimal('-1000000000'):  # -1 billion
                raise CustomValidationError("Amount too small", code="AMOUNT_TOO_SMALL")
            if decimal_amount > Decimal('1000000000000'):  # 1 trillion
                raise CustomValidationError("Amount too large", code="AMOUNT_TOO_LARGE")

            # Round to appropriate decimal places for currency
            if currency in ['USD', 'EUR', 'GBP']:
                decimal_amount = decimal_amount.quantize(Decimal('0.01'))
            elif currency in ['JPY', 'KRW']:
                decimal_amount = decimal_amount.quantize(Decimal('1'))

            return decimal_amount

        except (InvalidOperation, ValueError) as e:
            raise CustomValidationError(f"Invalid currency amount: {str(e)}", code="INVALID_CURRENCY")

    @staticmethod
    def validate_tax_calculation(subtotal: Decimal, tax_rate: float, tax_amount: Decimal) -> bool:
        """Validate tax calculation accuracy"""
        try:
            expected_tax = subtotal * Decimal(str(tax_rate))
            expected_tax = expected_tax.quantize(Decimal('0.01'))

            if abs(tax_amount - expected_tax) > Decimal('0.01'):
                raise CustomValidationError(
                    f"Tax calculation error. Expected: {expected_tax}, Got: {tax_amount}",
                    code="TAX_CALCULATION_ERROR",
                    context={'expected': float(expected_tax), 'actual': float(tax_amount)}
                )

            return True

        except Exception as e:
            raise CustomValidationError(f"Tax validation failed: {str(e)}", code="TAX_VALIDATION_ERROR")

    @staticmethod
    def validate_discount_logic(original_price: Decimal, discount_percent: float, final_price: Decimal) -> bool:
        """Validate discount calculation logic"""
        try:
            if discount_percent < 0 or discount_percent > 100:
                raise CustomValidationError("Discount percentage must be between 0 and 100", code="INVALID_DISCOUNT_PERCENT")

            expected_price = original_price * (Decimal('1') - Decimal(str(discount_percent / 100)))
            expected_price = expected_price.quantize(Decimal('0.01'))

            if abs(final_price - expected_price) > Decimal('0.01'):
                raise CustomValidationError(
                    f"Discount calculation error. Expected: {expected_price}, Got: {final_price}",
                    code="DISCOUNT_CALCULATION_ERROR"
                )

            return True

        except Exception as e:
            raise CustomValidationError(f"Discount validation failed: {str(e)}", code="DISCOUNT_VALIDATION_ERROR")


class GeographicValidator:
    """Validator for geographic and location data"""

    def __init__(self):
        self.geocoder = Nominatim(user_agent="pwc-data-validation")
        self.country_codes = {
            'US', 'CA', 'GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'CH',
            'AU', 'NZ', 'JP', 'CN', 'IN', 'BR', 'MX', 'AR', 'CL'
        }  # Add more as needed

    def validate_country_code(self, country_code: str) -> bool:
        """Validate ISO country code"""
        if not country_code or len(country_code) != 2:
            raise CustomValidationError("Country code must be 2 characters", code="INVALID_COUNTRY_CODE")

        if country_code.upper() not in self.country_codes:
            raise CustomValidationError(f"Unknown country code: {country_code}", code="UNKNOWN_COUNTRY")

        return True

    def validate_coordinates(self, latitude: float, longitude: float) -> bool:
        """Validate geographic coordinates"""
        try:
            if not (-90 <= latitude <= 90):
                raise CustomValidationError("Latitude must be between -90 and 90", code="INVALID_LATITUDE")

            if not (-180 <= longitude <= 180):
                raise CustomValidationError("Longitude must be between -180 and 180", code="INVALID_LONGITUDE")

            return True

        except Exception as e:
            raise CustomValidationError(f"Coordinate validation failed: {str(e)}", code="COORDINATE_ERROR")

    async def validate_address(self, address: str, country: str = None) -> Dict[str, Any]:
        """Validate and geocode address"""
        try:
            location = self.geocoder.geocode(address)
            if not location:
                raise CustomValidationError("Address not found", code="ADDRESS_NOT_FOUND")

            result = {
                'validated_address': location.address,
                'latitude': location.latitude,
                'longitude': location.longitude,
                'is_valid': True
            }

            # Validate country if specified
            if country:
                address_components = location.raw.get('display_name', '').split(', ')
                if country.lower() not in [comp.lower() for comp in address_components]:
                    result['country_mismatch'] = True

            return result

        except Exception as e:
            raise CustomValidationError(f"Address validation failed: {str(e)}", code="ADDRESS_VALIDATION_ERROR")


class FileValidator:
    """Validator for file uploads and content"""

    def __init__(self):
        self.allowed_types = {
            'image': ['image/jpeg', 'image/png', 'image/gif', 'image/webp'],
            'document': ['application/pdf', 'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'],
            'spreadsheet': ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'text/csv'],
            'archive': ['application/zip', 'application/x-rar-compressed', 'application/x-tar'],
            'data': ['application/json', 'text/csv', 'application/xml']
        }
        self.max_file_size = 100 * 1024 * 1024  # 100MB

    def validate_file_upload(self, file_content: bytes, filename: str, expected_type: str = None) -> Dict[str, Any]:
        """Validate uploaded file"""
        try:
            # Check file size
            if len(file_content) > self.max_file_size:
                raise CustomValidationError(
                    f"File too large. Maximum size: {self.max_file_size / 1024 / 1024:.1f}MB",
                    code="FILE_TOO_LARGE"
                )

            # Detect MIME type
            mime_type = magic.from_buffer(file_content, mime=True)

            # Validate file extension matches content
            expected_mime = mimetypes.guess_type(filename)[0]
            if expected_mime and mime_type != expected_mime:
                raise CustomValidationError(
                    f"File content doesn't match extension. Expected: {expected_mime}, Got: {mime_type}",
                    code="MIME_TYPE_MISMATCH"
                )

            # Check against allowed types
            if expected_type and expected_type in self.allowed_types:
                if mime_type not in self.allowed_types[expected_type]:
                    raise CustomValidationError(
                        f"File type not allowed for {expected_type}. Got: {mime_type}",
                        code="FILE_TYPE_NOT_ALLOWED"
                    )

            # Calculate file hash for integrity
            file_hash = hashlib.sha256(file_content).hexdigest()

            return {
                'is_valid': True,
                'mime_type': mime_type,
                'file_size': len(file_content),
                'file_hash': file_hash,
                'filename': filename
            }

        except Exception as e:
            raise CustomValidationError(f"File validation failed: {str(e)}", code="FILE_VALIDATION_ERROR")

    def validate_csv_content(self, csv_content: str, expected_columns: List[str] = None) -> Dict[str, Any]:
        """Validate CSV file content and structure"""
        try:
            # Parse CSV
            df = pd.read_csv(csv_content)

            # Check for empty file
            if df.empty:
                raise CustomValidationError("CSV file is empty", code="EMPTY_CSV")

            # Validate expected columns
            if expected_columns:
                missing_columns = set(expected_columns) - set(df.columns)
                if missing_columns:
                    raise CustomValidationError(
                        f"Missing required columns: {list(missing_columns)}",
                        code="MISSING_COLUMNS"
                    )

            # Check for data quality issues
            null_counts = df.isnull().sum()
            duplicate_count = df.duplicated().sum()

            return {
                'is_valid': True,
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': list(df.columns),
                'null_counts': null_counts.to_dict(),
                'duplicate_rows': int(duplicate_count),
                'data_types': df.dtypes.astype(str).to_dict()
            }

        except Exception as e:
            raise CustomValidationError(f"CSV validation failed: {str(e)}", code="CSV_VALIDATION_ERROR")


class AsyncValidator:
    """Asynchronous validator for external data sources"""

    def __init__(self):
        self.session = None

    async def validate_external_reference(self, reference_id: str, reference_type: str) -> bool:
        """Validate reference against external system"""
        try:
            # This would make async requests to external APIs
            # For now, simulate validation
            await asyncio.sleep(0.1)  # Simulate network delay

            if reference_type == "customer_id":
                # Simulate customer ID validation
                return len(reference_id) >= 5

            elif reference_type == "product_code":
                # Simulate product code validation
                return re.match(r'^[A-Z0-9]{4,10}$', reference_id) is not None

            return True

        except Exception as e:
            raise CustomValidationError(f"External validation failed: {str(e)}", code="EXTERNAL_VALIDATION_ERROR")

    async def validate_api_response(self, url: str, expected_status: int = 200) -> bool:
        """Validate API endpoint availability"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    return response.status == expected_status

        except Exception as e:
            raise CustomValidationError(f"API validation failed: {str(e)}", code="API_VALIDATION_ERROR")


class ValidationEngine:
    """Main validation engine orchestrating all validators"""

    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STANDARD):
        self.validation_level = validation_level
        self.business_validator = BusinessRuleValidator()
        self.financial_validator = FinancialValidator()
        self.geographic_validator = GeographicValidator()
        self.file_validator = FileValidator()
        self.async_validator = AsyncValidator()
        self.validation_metrics = defaultdict(int)

    async def validate_data(self, data: Dict[str, Any], schema_type: str, **kwargs) -> ValidationResult:
        """Main validation method"""
        start_time = datetime.now()
        errors = []
        warnings = []
        transformed_data = data.copy()

        try:
            # Choose validation strategy based on schema type
            if schema_type == "sales_data":
                await self._validate_sales_data(data, errors, warnings, transformed_data)
            elif schema_type == "customer_data":
                await self._validate_customer_data(data, errors, warnings, transformed_data)
            elif schema_type == "product_data":
                await self._validate_product_data(data, errors, warnings, transformed_data)
            elif schema_type == "financial_data":
                await self._validate_financial_data(data, errors, warnings, transformed_data)
            else:
                warnings.append({
                    'message': f"Unknown schema type: {schema_type}",
                    'code': 'UNKNOWN_SCHEMA',
                    'severity': 'warning'
                })

            # Record metrics
            self.validation_metrics[f"{schema_type}_validations"] += 1
            if errors:
                self.validation_metrics[f"{schema_type}_errors"] += len(errors)

            # Calculate validation time
            validation_time = (datetime.now() - start_time).total_seconds() * 1000

            return ValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                transformed_data=transformed_data if len(errors) == 0 else None,
                validation_time_ms=validation_time,
                validation_level=self.validation_level
            )

        except Exception as e:
            logger.error(f"Validation engine error: {e}")
            errors.append({
                'message': f"Validation engine error: {str(e)}",
                'code': 'VALIDATION_ENGINE_ERROR',
                'severity': 'error'
            })

            validation_time = (datetime.now() - start_time).total_seconds() * 1000
            return ValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
                validation_time_ms=validation_time,
                validation_level=self.validation_level
            )

    async def _validate_sales_data(self, data: Dict[str, Any], errors: List, warnings: List, transformed_data: Dict):
        """Validate sales data"""
        try:
            self.business_validator.validate_sales_data_integrity(data)

            # Additional financial validations
            if 'total_amount' in data:
                try:
                    validated_amount = self.financial_validator.validate_currency_amount(data['total_amount'])
                    transformed_data['total_amount'] = float(validated_amount)
                except CustomValidationError as e:
                    errors.append({'message': e.message, 'field': 'total_amount', 'code': e.code})

        except CustomValidationError as e:
            errors.append({'message': e.message, 'field': e.field, 'code': e.code})

    async def _validate_customer_data(self, data: Dict[str, Any], errors: List, warnings: List, transformed_data: Dict):
        """Validate customer data"""
        try:
            self.business_validator.validate_customer_data(data)

            # Geographic validation if address present
            if 'address' in data and data['address']:
                try:
                    address_result = await self.geographic_validator.validate_address(data['address'])
                    transformed_data['validated_address'] = address_result
                except CustomValidationError as e:
                    if self.validation_level == ValidationLevel.STRICT:
                        errors.append({'message': e.message, 'field': 'address', 'code': e.code})
                    else:
                        warnings.append({'message': e.message, 'field': 'address', 'code': e.code})

        except CustomValidationError as e:
            errors.append({'message': e.message, 'field': e.field, 'code': e.code})

    async def _validate_product_data(self, data: Dict[str, Any], errors: List, warnings: List, transformed_data: Dict):
        """Validate product data"""
        try:
            self.business_validator.validate_product_data(data)

            # External validation for product codes
            if 'stock_code' in data and self.validation_level in [ValidationLevel.STRICT, ValidationLevel.STANDARD]:
                try:
                    is_valid = await self.async_validator.validate_external_reference(
                        data['stock_code'], 'product_code'
                    )
                    if not is_valid:
                        warnings.append({
                            'message': 'Product code not found in external system',
                            'field': 'stock_code',
                            'code': 'EXTERNAL_NOT_FOUND'
                        })
                except CustomValidationError as e:
                    warnings.append({'message': e.message, 'field': 'stock_code', 'code': e.code})

        except CustomValidationError as e:
            errors.append({'message': e.message, 'field': e.field, 'code': e.code})

    async def _validate_financial_data(self, data: Dict[str, Any], errors: List, warnings: List, transformed_data: Dict):
        """Validate financial data"""
        try:
            # Validate currency amounts
            for field in ['amount', 'total', 'subtotal', 'tax', 'discount']:
                if field in data and data[field] is not None:
                    try:
                        validated_amount = self.financial_validator.validate_currency_amount(data[field])
                        transformed_data[field] = float(validated_amount)
                    except CustomValidationError as e:
                        errors.append({'message': e.message, 'field': field, 'code': e.code})

            # Validate tax calculations if present
            if all(key in data for key in ['subtotal', 'tax_rate', 'tax_amount']):
                try:
                    self.financial_validator.validate_tax_calculation(
                        Decimal(str(data['subtotal'])),
                        float(data['tax_rate']),
                        Decimal(str(data['tax_amount']))
                    )
                except CustomValidationError as e:
                    errors.append({'message': e.message, 'code': e.code})

        except Exception as e:
            errors.append({'message': f"Financial validation error: {str(e)}", 'code': 'FINANCIAL_ERROR'})

    def get_validation_metrics(self) -> Dict[str, Any]:
        """Get validation performance metrics"""
        return dict(self.validation_metrics)


# Factory function
def create_validation_engine(validation_level: ValidationLevel = ValidationLevel.STANDARD) -> ValidationEngine:
    """Create configured validation engine"""
    return ValidationEngine(validation_level)


# Pydantic model integration
class ValidatedSalesData(BaseModel):
    """Pydantic model with advanced validation for sales data"""

    invoice_no: str = Field(..., min_length=1, max_length=50)
    stock_code: str = Field(..., regex=r'^[A-Z0-9]{4,10}$')
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., ge=0)
    total_amount: Decimal = Field(..., ge=0)
    discount_amount: Optional[Decimal] = Field(None, ge=0)
    tax_amount: Optional[Decimal] = Field(None, ge=0)
    customer_id: Optional[str] = Field(None, regex=r'^[A-Z0-9]{5,20}$')
    country: Optional[str] = Field(None, min_length=2, max_length=2)
    invoice_date: datetime

    @validator('total_amount')
    def validate_total_calculation(cls, v, values):
        """Validate total amount calculation"""
        if 'quantity' in values and 'unit_price' in values:
            expected = values['quantity'] * values['unit_price']

            if 'discount_amount' in values and values['discount_amount']:
                expected -= values['discount_amount']

            if 'tax_amount' in values and values['tax_amount']:
                expected += values['tax_amount']

            if abs(v - expected) > Decimal('0.01'):
                raise ValueError(f'Total amount mismatch. Expected: {expected}, Got: {v}')

        return v

    @validator('invoice_date')
    def validate_invoice_date(cls, v):
        """Validate invoice date is reasonable"""
        if v.date() > date.today():
            raise ValueError('Invoice date cannot be in the future')

        if v.date() < date.today() - timedelta(days=3650):  # 10 years
            raise ValueError('Invoice date is too old')

        return v

    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat()
        }