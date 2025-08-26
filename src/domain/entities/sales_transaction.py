from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator
from sqlmodel import Field as SQLField
from sqlmodel import SQLModel


class TransactionStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class PaymentMethod(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    CASH = "cash"
    BANK_TRANSFER = "bank_transfer"
    DIGITAL_WALLET = "digital_wallet"


class SalesTransactionBase(SQLModel):
    """Base model for sales transactions with comprehensive validation."""

    invoice_no: str = SQLField(index=True, min_length=1, max_length=50)
    stock_code: str = SQLField(index=True, min_length=1, max_length=50)
    description: str | None = SQLField(default=None, max_length=500)
    quantity: int = SQLField(gt=0, description="Quantity must be positive")
    unit_price: Decimal = SQLField(
        gt=0,
        max_digits=10,
        decimal_places=2,
        description="Unit price must be positive"
    )
    invoice_date: datetime = SQLField(index=True)
    customer_id: str | None = SQLField(default=None, index=True, max_length=50)
    country: str = SQLField(index=True, min_length=2, max_length=100)

    @field_validator('invoice_no')
    @classmethod
    def validate_invoice_no(cls, v):
        if not v or v.isspace():
            raise ValueError('Invoice number cannot be empty')
        if v.startswith('C'):
            raise ValueError('Cancelled invoices (starting with C) are not allowed in active transactions')
        return v.upper().strip()

    @field_validator('stock_code')
    @classmethod
    def validate_stock_code(cls, v):
        if not v or v.isspace():
            raise ValueError('Stock code cannot be empty')
        return v.upper().strip()

    @field_validator('description')
    @classmethod
    def validate_description(cls, v):
        if v:
            v = v.strip()
            if len(v) == 0:
                return None
            # Clean common data quality issues
            if v.upper() in ['NULL', 'N/A', 'UNKNOWN', 'MISSING']:
                return None
        return v

    @field_validator('customer_id')
    @classmethod
    def validate_customer_id(cls, v):
        if v:
            v = v.strip()
            if len(v) == 0 or v.upper() in ['NULL', 'UNKNOWN']:
                return None
            try:
                # Validate if it's a number (as expected in retail data)
                float(v)
                return v
            except ValueError:
                raise ValueError('Customer ID must be numeric')
        return v

    @field_validator('country')
    @classmethod
    def validate_country(cls, v):
        if not v or v.isspace():
            raise ValueError('Country cannot be empty')
        v = v.strip()

        # Common country name standardization
        country_mappings = {
            'UK': 'United Kingdom',
            'USA': 'United States',
            'US': 'United States',
            'Deutschland': 'Germany',
            'España': 'Spain',
        }

        return country_mappings.get(v, v)

    @field_validator('invoice_date')
    @classmethod
    def validate_invoice_date(cls, v):
        if v > datetime.now():
            raise ValueError('Invoice date cannot be in the future')
        if v.year < 2000:
            raise ValueError('Invoice date seems too old to be valid')
        return v

    @model_validator(mode='after')
    def validate_transaction_integrity(self):
        """Business rule validation for transaction integrity."""
        quantity = self.quantity
        unit_price = self.unit_price

        if quantity and unit_price:
            total_amount = quantity * unit_price
            if total_amount > Decimal('100000'):  # £100k limit
                raise ValueError('Transaction amount exceeds maximum allowed limit')
            if total_amount < Decimal('0.01'):
                raise ValueError('Transaction amount too small to be valid')

        return self


class SalesTransaction(SalesTransactionBase, table=True):
    """Database table model for sales transactions."""
    __tablename__ = "sales_transactions"

    id: UUID | None = SQLField(default_factory=uuid4, primary_key=True)
    total_amount: Decimal | None = SQLField(
        default=None,
        max_digits=12,
        decimal_places=2,
        description="Auto-calculated total amount"
    )
    status: TransactionStatus = SQLField(default=TransactionStatus.COMPLETED)
    payment_method: PaymentMethod | None = SQLField(default=None)
    created_at: datetime = SQLField(default_factory=datetime.utcnow)
    updated_at: datetime = SQLField(default_factory=datetime.utcnow)

    def calculate_total_amount(self) -> Decimal:
        """Calculate total amount based on quantity and unit price."""
        return self.quantity * self.unit_price


class SalesTransactionCreate(SalesTransactionBase):
    """Model for creating new sales transactions via API."""

    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat()
        }


class SalesTransactionResponse(SalesTransactionBase):
    """Model for API responses with computed fields."""

    id: UUID
    total_amount: Decimal
    status: TransactionStatus
    created_at: datetime
    updated_at: datetime

    # Computed business intelligence fields
    revenue_category: str | None = None
    customer_segment: str | None = None
    product_category: str | None = None

    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }


class SalesTransactionUpdate(BaseModel):
    """Model for updating sales transactions."""

    description: str | None = Field(None, max_length=500)
    status: TransactionStatus | None = None
    payment_method: PaymentMethod | None = None

    class Config:
        use_enum_values = True


class SalesTransactionFilter(BaseModel):
    """Model for filtering sales transactions with advanced options."""

    invoice_no: str | None = None
    stock_code: str | None = None
    customer_id: str | None = None
    country: str | None = None
    status: TransactionStatus | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    min_amount: Decimal | None = Field(None, ge=0)
    max_amount: Decimal | None = Field(None, ge=0)
    min_quantity: int | None = Field(None, ge=1)
    max_quantity: int | None = Field(None, ge=1)

    @model_validator(mode='after')
    def validate_ranges(self):
        if self.max_amount and self.min_amount and self.max_amount < self.min_amount:
            raise ValueError('max_amount must be greater than min_amount')

        if self.date_to and self.date_from and self.date_to < self.date_from:
            raise ValueError('date_to must be after date_from')

        return self
