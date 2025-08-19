"""Invoice domain entity and related models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import Field, field_validator, model_validator

from core.constants import CANCELLED_INVOICE_PREFIX
from domain.base import DomainEntity


class InvoiceStatus(str, Enum):
    """Invoice status enumeration."""

    DRAFT = "draft"
    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"
    PARTIAL_REFUND = "partial_refund"


class InvoiceType(str, Enum):
    """Invoice type enumeration."""

    SALE = "sale"
    RETURN = "return"
    CREDIT_NOTE = "credit_note"
    ADJUSTMENT = "adjustment"


class PaymentMethod(str, Enum):
    """Payment method enumeration."""

    CASH = "cash"
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    BANK_TRANSFER = "bank_transfer"
    PAYPAL = "paypal"
    OTHER = "other"
    UNKNOWN = "unknown"


class Invoice(DomainEntity):
    """
    Represents an invoice entity.
    Aggregates transaction information at invoice level.
    """

    invoice_no: str = Field(
        ...,
        description="Unique invoice number",
        min_length=1,
        max_length=20,
    )
    invoice_date: datetime = Field(
        ...,
        description="Invoice date and time",
    )
    customer_id: str | None = Field(
        None,
        description="Customer identifier",
        max_length=20,
    )

    # Invoice details
    invoice_type: InvoiceType = Field(
        default=InvoiceType.SALE,
        description="Type of invoice",
    )
    status: InvoiceStatus = Field(
        default=InvoiceStatus.COMPLETED,
        description="Invoice status",
    )
    payment_method: PaymentMethod = Field(
        default=PaymentMethod.UNKNOWN,
        description="Payment method used",
    )

    # Amounts
    subtotal: Decimal = Field(
        default=Decimal("0"),
        description="Subtotal before tax and discounts",
        ge=0,
    )
    tax_amount: Decimal | None = Field(
        None,
        description="Tax amount",
        ge=0,
    )
    discount_amount: Decimal | None = Field(
        None,
        description="Discount amount",
        ge=0,
    )
    shipping_amount: Decimal | None = Field(
        None,
        description="Shipping cost",
        ge=0,
    )
    total_amount: Decimal = Field(
        default=Decimal("0"),
        description="Total invoice amount",
    )

    # Items
    total_items: int = Field(
        default=0,
        description="Total number of items",
        ge=0,
    )
    unique_products: int = Field(
        default=0,
        description="Number of unique products",
        ge=0,
    )

    # Location
    country: str | None = Field(
        None,
        description="Billing country",
        max_length=100,
    )
    currency: str = Field(
        default="GBP",
        description="Invoice currency",
        max_length=3,
    )

    # Cancellation info
    is_cancelled: bool = Field(
        default=False,
        description="Whether invoice is cancelled",
    )
    cancellation_date: datetime | None = Field(
        None,
        description="Cancellation date",
    )
    cancellation_reason: str | None = Field(
        None,
        description="Reason for cancellation",
        max_length=500,
    )
    original_invoice_no: str | None = Field(
        None,
        description="Original invoice for cancellations",
        max_length=20,
    )

    # Additional metadata
    notes: str | None = Field(
        None,
        description="Invoice notes",
        max_length=1000,
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Invoice tags for categorization",
    )

    @field_validator("invoice_no")
    @classmethod
    def validate_invoice_no(cls, v: str) -> str:
        """Validate and normalize invoice number."""
        return v.strip().upper()

    @field_validator("country")
    @classmethod
    def standardize_country(cls, v: str | None) -> str | None:
        """Standardize country name."""
        if v:
            v = v.strip().title()
            replacements = {
                "Uk": "United Kingdom",
                "Usa": "United States",
                "Uae": "United Arab Emirates",
                "Rsa": "South Africa",
                "Eire": "Ireland",
            }
            return replacements.get(v, v)
        return v

    @model_validator(mode="after")
    def process_cancellation(self) -> "Invoice":
        """Process cancellation information."""
        # Check if cancelled based on invoice number
        if self.invoice_no.startswith(CANCELLED_INVOICE_PREFIX):
            self.is_cancelled = True
            self.status = InvoiceStatus.CANCELLED
            self.invoice_type = InvoiceType.CREDIT_NOTE

            # Extract original invoice number
            self.original_invoice_no = self.invoice_no[len(
                CANCELLED_INVOICE_PREFIX):]

            # Add tag
            if "cancelled" not in self.tags:
                self.tags.append("cancelled")

        # Determine invoice type based on amounts
        if self.total_amount < 0:
            self.invoice_type = InvoiceType.RETURN
            if "return" not in self.tags:
                self.tags.append("return")

        return self

    @model_validator(mode="after")
    def calculate_total(self) -> "Invoice":
        """Calculate total amount including tax, discounts, and shipping."""
        total = self.subtotal

        if self.tax_amount:
            total += self.tax_amount

        if self.discount_amount:
            total -= self.discount_amount

        if self.shipping_amount:
            total += self.shipping_amount

        self.total_amount = max(Decimal("0"), total)

        return self

    def validate_business_rules(self) -> bool:
        """Validate invoice against business rules."""
        self.clear_validation_errors()

        # Rule 1: Cancelled invoices should have original reference
        if self.is_cancelled and not self.original_invoice_no:
            # This is a warning, not always an error
            pass

        # Rule 2: Future dates not allowed
        if self.invoice_date > datetime.utcnow():
            self.add_validation_error(
                f"Invoice date is in the future: {self.invoice_date}"
            )

        # Rule 3: Total amount validation
        if self.total_amount < 0 and self.invoice_type == InvoiceType.SALE:
            self.add_validation_error(
                "Sale invoice cannot have negative total"
            )

        # Rule 4: Customer required for completed invoices
        if self.status == InvoiceStatus.COMPLETED and not self.customer_id:
            self.add_validation_error(
                "Completed invoice requires customer ID"
            )

        # Rule 5: Items required
        if self.total_items == 0 and self.status == InvoiceStatus.COMPLETED:
            self.add_validation_error(
                "Completed invoice must have items"
            )

        return self.is_valid

    def to_warehouse_dict(self) -> dict[str, Any]:
        """Convert invoice to warehouse-ready dictionary."""
        return {
            "invoice_no": self.invoice_no,
            "invoice_date": self.invoice_date,
            "customer_id": self.customer_id,
            "invoice_type": self.invoice_type.value,
            "status": self.status.value,
            "payment_method": self.payment_method.value,
            "subtotal": float(self.subtotal),
            "tax_amount": float(self.tax_amount) if self.tax_amount else None,
            "discount_amount": float(self.discount_amount) if self.discount_amount else None,
            "shipping_amount": float(self.shipping_amount) if self.shipping_amount else None,
            "total_amount": float(self.total_amount),
            "total_items": self.total_items,
            "unique_products": self.unique_products,
            "country": self.country,
            "currency": self.currency,
            "is_cancelled": self.is_cancelled,
            "cancellation_date": self.cancellation_date,
            "cancellation_reason": self.cancellation_reason,
            "original_invoice_no": self.original_invoice_no,
        }
