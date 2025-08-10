"""Transaction domain entity and related models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List
import re

from pydantic import Field, field_validator, model_validator
from pydantic.types import PositiveInt

from ..base import DomainEntity
from ...core.constants import (
    STOCK_CODE_PATTERN,
    CANCELLED_INVOICE_PREFIX,
    MIN_VALID_QUANTITY,
    MAX_VALID_QUANTITY,
    MIN_VALID_PRICE,
    MAX_VALID_PRICE,
)


class TransactionStatus(str, Enum):
    """Transaction status enumeration."""

    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PENDING = "pending"
    REFUNDED = "refunded"


class TransactionLine(DomainEntity):
    """
    Represents a single line item in a transaction.
    Maps to a row in the Online Retail dataset.
    """

    # Primary fields from dataset
    invoice_no: str = Field(
        ...,
        description="Invoice number",
        min_length=1,
        max_length=20,
    )
    stock_code: str = Field(
        ...,
        description="Product stock code",
        min_length=1,
        max_length=20,
    )
    description: Optional[str] = Field(
        None,
        description="Product description",
        max_length=500,
    )
    quantity: int = Field(
        ...,
        description="Quantity (negative for returns)",
        ge=MIN_VALID_QUANTITY,
        le=MAX_VALID_QUANTITY,
    )
    invoice_date: datetime = Field(
        ...,
        description="Date and time of invoice",
    )
    unit_price: Decimal = Field(
        ...,
        description="Unit price in GBP",
        ge=MIN_VALID_PRICE,
        le=MAX_VALID_PRICE,
        decimal_places=2,
    )
    customer_id: Optional[str] = Field(
        None,
        description="Customer identifier",
        max_length=20,
    )
    country: Optional[str] = Field(
        None,
        description="Country name",
        max_length=100,
    )

    # Derived fields
    line_total: Optional[Decimal] = Field(
        None,
        description="Total for this line (quantity * unit_price)",
    )
    is_return: bool = Field(
        default=False,
        description="Whether this is a return (negative quantity)",
    )
    is_cancelled: bool = Field(
        default=False,
        description="Whether this transaction was cancelled",
    )

    @field_validator("invoice_no")
    @classmethod
    def validate_invoice_no(cls, v: str) -> str:
        """Validate invoice number format."""
        v = v.strip().upper()
        if not v:
            raise ValueError("Invoice number cannot be empty")
        return v

    @field_validator("stock_code")
    @classmethod
    def validate_stock_code(cls, v: str) -> str:
        """Validate stock code format."""
        v = v.strip().upper()

        # Special codes that are valid
        special_codes = ["POST", "DOT", "M",
                         "BANK CHARGES", "PADS", "AMAZONFEE"]
        if v in special_codes:
            return v

        # Check against pattern for regular codes
        if not re.match(STOCK_CODE_PATTERN, v):
            # Don't fail, just mark for review
            pass  # Will be handled in business rules

        return v

    @field_validator("description")
    @classmethod
    def clean_description(cls, v: Optional[str]) -> Optional[str]:
        """Clean and validate description."""
        if v:
            v = v.strip()
            # Remove multiple spaces
            v = " ".join(v.split())
            # Handle special characters
            v = v.replace("?", "").strip()
            if v.upper() in ["", "?", "??", "???", "NONE", "NULL"]:
                return None
        return v

    @field_validator("customer_id")
    @classmethod
    def validate_customer_id(cls, v: Optional[str]) -> Optional[str]:
        """Validate and clean customer ID."""
        if v:
            v = str(v).strip()
            # Check if it's a valid customer ID (numeric for this dataset)
            try:
                float(v)  # Customer IDs are numeric in the dataset
                return v
            except ValueError:
                return None
        return v

    @field_validator("country")
    @classmethod
    def clean_country(cls, v: Optional[str]) -> Optional[str]:
        """Clean and standardize country name."""
        if v:
            v = v.strip().title()
            # Fix common issues in the dataset
            replacements = {
                "Uk": "United Kingdom",
                "Usa": "United States",
                "U.S.A.": "United States",
                "Uae": "United Arab Emirates",
                "Rsa": "South Africa",
                "Eire": "Ireland",
            }
            return replacements.get(v, v)
        return v

    @model_validator(mode="after")
    def calculate_derived_fields(self) -> "TransactionLine":
        """Calculate derived fields after validation."""
        # Calculate line total
        if self.quantity and self.unit_price:
            self.line_total = Decimal(
                str(abs(self.quantity))) * self.unit_price

        # Check if return
        self.is_return = self.quantity < 0

        # Check if cancelled
        self.is_cancelled = self.invoice_no.startswith(
            CANCELLED_INVOICE_PREFIX)

        return self

    def validate_business_rules(self) -> bool:
        """Validate transaction line against business rules."""
        self.clear_validation_errors()

        # Rule 1: Cancelled invoices should have negative quantities
        if self.is_cancelled and self.quantity > 0:
            self.add_validation_error(
                f"Cancelled invoice {self.invoice_no} has positive quantity"
            )

        # Rule 2: Stock code should match pattern (unless special)
        special_codes = ["POST", "DOT", "M",
                         "BANK CHARGES", "PADS", "AMAZONFEE"]
        if self.stock_code not in special_codes:
            if not re.match(STOCK_CODE_PATTERN, self.stock_code):
                self.add_validation_error(
                    f"Invalid stock code format: {self.stock_code}"
                )

        # Rule 3: Unit price should be positive for normal sales
        if not self.is_cancelled and not self.is_return and self.unit_price <= 0:
            self.add_validation_error(
                f"Non-return transaction has non-positive price: {self.unit_price}"
            )

        # Rule 4: Description required for regular products
        if self.stock_code not in special_codes and not self.description:
            self.add_validation_error(
                f"Missing description for product {self.stock_code}"
            )

        # Rule 5: Customer ID required for non-cancelled transactions
        if not self.is_cancelled and not self.customer_id:
            self.add_validation_error(
                "Customer ID required for non-cancelled transactions"
            )

        return self.is_valid


class Transaction(DomainEntity):
    """
    Represents a complete transaction (invoice) with multiple line items.
    Aggregates TransactionLine objects.
    """

    invoice_no: str = Field(
        ...,
        description="Invoice number",
    )
    invoice_date: datetime = Field(
        ...,
        description="Date and time of invoice",
    )
    customer_id: Optional[str] = Field(
        None,
        description="Customer identifier",
    )
    country: Optional[str] = Field(
        None,
        description="Country name",
    )

    # Line items
    lines: List[TransactionLine] = Field(
        default_factory=list,
        description="Transaction line items",
    )

    # Aggregated fields
    total_amount: Optional[Decimal] = Field(
        None,
        description="Total transaction amount",
    )
    total_items: Optional[int] = Field(
        None,
        description="Total number of items",
    )
    unique_products: Optional[int] = Field(
        None,
        description="Number of unique products",
    )
    status: TransactionStatus = Field(
        default=TransactionStatus.COMPLETED,
        description="Transaction status",
    )

    @model_validator(mode="after")
    def calculate_aggregates(self) -> "Transaction":
        """Calculate aggregate fields from line items."""
        if self.lines:
            # Calculate totals
            self.total_amount = sum(
                line.line_total for line in self.lines
                if line.line_total
            )
            self.total_items = sum(
                abs(line.quantity) for line in self.lines
            )
            self.unique_products = len(
                set(line.stock_code for line in self.lines)
            )

            # Determine status
            if self.invoice_no.startswith(CANCELLED_INVOICE_PREFIX):
                self.status = TransactionStatus.CANCELLED
            elif any(line.is_return for line in self.lines):
                self.status = TransactionStatus.REFUNDED
            else:
                self.status = TransactionStatus.COMPLETED

        return self

    def add_line(self, line: TransactionLine) -> None:
        """Add a line item to the transaction."""
        self.lines.append(line)
        # Recalculate aggregates
        self.calculate_aggregates()

    def validate_business_rules(self) -> bool:
        """Validate complete transaction against business rules."""
        self.clear_validation_errors()

        # Validate all line items
        for line in self.lines:
            if not line.validate_business_rules():
                self.add_validation_error(
                    f"Line validation failed: {line.validation_errors}"
                )

        # Rule 1: Transaction should have at least one line
        if not self.lines:
            self.add_validation_error("Transaction has no line items")

        # Rule 2: All lines should have same customer
        customer_ids = set(
            line.customer_id for line in self.lines
            if line.customer_id
        )
        if len(customer_ids) > 1:
            self.add_validation_error(
                f"Multiple customer IDs in same transaction: {customer_ids}"
            )

        # Rule 3: All lines should have same country
        countries = set(
            line.country for line in self.lines
            if line.country
        )
        if len(countries) > 1:
            self.add_validation_error(
                f"Multiple countries in same transaction: {countries}"
            )

        return self.is_valid


class CancelledTransaction(Transaction):
    """Special handling for cancelled transactions."""

    cancellation_reason: Optional[str] = Field(
        None,
        description="Reason for cancellation",
    )
    original_invoice_no: Optional[str] = Field(
        None,
        description="Original invoice number before cancellation",
    )

    @model_validator(mode="after")
    def extract_original_invoice(self) -> "CancelledTransaction":
        """Extract original invoice number from cancelled invoice."""
        if self.invoice_no.startswith(CANCELLED_INVOICE_PREFIX):
            self.original_invoice_no = self.invoice_no[len(
                CANCELLED_INVOICE_PREFIX):]
        return self
