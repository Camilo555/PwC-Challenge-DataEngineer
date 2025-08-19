"""Customer domain entity and related models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import EmailStr, Field, field_validator, model_validator

from ..base import DomainEntity


class CustomerSegment(str, Enum):
    """Customer segmentation categories."""

    VIP = "vip"
    FREQUENT = "frequent"
    REGULAR = "regular"
    OCCASIONAL = "occasional"
    NEW = "new"
    DORMANT = "dormant"
    CHURNED = "churned"


class CustomerMetrics(DomainEntity):
    """Customer behavioral metrics."""

    customer_id: str = Field(
        ...,
        description="Customer identifier",
    )

    # Transaction metrics
    total_transactions: int = Field(
        default=0,
        description="Total number of transactions",
        ge=0,
    )
    total_spent: Decimal = Field(
        default=Decimal("0"),
        description="Total amount spent",
        ge=0,
    )
    total_items: int = Field(
        default=0,
        description="Total items purchased",
        ge=0,
    )
    total_returns: int = Field(
        default=0,
        description="Total items returned",
        ge=0,
    )

    # Date metrics
    first_purchase_date: datetime | None = Field(
        None,
        description="Date of first purchase",
    )
    last_purchase_date: datetime | None = Field(
        None,
        description="Date of last purchase",
    )
    days_since_last_purchase: int | None = Field(
        None,
        description="Days since last purchase",
        ge=0,
    )

    # Behavioral metrics
    avg_transaction_value: Decimal | None = Field(
        None,
        description="Average transaction value",
        ge=0,
    )
    avg_items_per_transaction: float | None = Field(
        None,
        description="Average items per transaction",
        ge=0,
    )
    purchase_frequency_days: float | None = Field(
        None,
        description="Average days between purchases",
        ge=0,
    )
    return_rate: float | None = Field(
        None,
        description="Return rate percentage",
        ge=0,
        le=100,
    )

    # RFM metrics
    recency_score: int | None = Field(
        None,
        description="Recency score (1-5)",
        ge=1,
        le=5,
    )
    frequency_score: int | None = Field(
        None,
        description="Frequency score (1-5)",
        ge=1,
        le=5,
    )
    monetary_score: int | None = Field(
        None,
        description="Monetary value score (1-5)",
        ge=1,
        le=5,
    )
    rfm_score: float | None = Field(
        None,
        description="Combined RFM score",
        ge=1,
        le=5,
    )

    @model_validator(mode="after")
    def calculate_metrics(self) -> "CustomerMetrics":
        """Calculate derived metrics."""
        # Average transaction value
        if self.total_transactions > 0:
            self.avg_transaction_value = self.total_spent / self.total_transactions
            self.avg_items_per_transaction = self.total_items / self.total_transactions

        # Return rate
        if self.total_items > 0:
            self.return_rate = (self.total_returns / self.total_items) * 100

        # Days since last purchase
        if self.last_purchase_date:
            days_diff = (datetime.utcnow() - self.last_purchase_date).days
            self.days_since_last_purchase = max(0, days_diff)

        # Purchase frequency
        if self.first_purchase_date and self.last_purchase_date and self.total_transactions > 1:
            total_days = (self.last_purchase_date -
                          self.first_purchase_date).days
            if total_days > 0:
                self.purchase_frequency_days = total_days / \
                    (self.total_transactions - 1)

        # RFM score
        if all([self.recency_score, self.frequency_score, self.monetary_score]):
            recency = self.recency_score or 0
            frequency = self.frequency_score or 0
            monetary = self.monetary_score or 0
            self.rfm_score = (recency + frequency + monetary) / 3

        return self

    def validate_business_rules(self) -> bool:
        """Validate metrics against business rules."""
        self.clear_validation_errors()

        # Rule 1: Date consistency
        if self.first_purchase_date and self.last_purchase_date:
            if self.first_purchase_date > self.last_purchase_date:
                self.add_validation_error(
                    "First purchase date is after last purchase date"
                )

        # Rule 2: Transaction consistency
        if self.total_transactions == 0 and self.total_spent > 0:
            self.add_validation_error(
                "Has spending but no transactions"
            )

        return self.is_valid


class Customer(DomainEntity):
    """
    Represents a customer entity with complete profile and metrics.
    """

    customer_id: str = Field(
        ...,
        description="Unique customer identifier",
        min_length=1,
        max_length=20,
    )

    # Profile information
    email: EmailStr | None = Field(
        None,
        description="Customer email address",
    )
    name: str | None = Field(
        None,
        description="Customer name",
        max_length=200,
    )
    company: str | None = Field(
        None,
        description="Company name",
        max_length=200,
    )

    # Location
    country: str = Field(
        ...,
        description="Customer country",
        min_length=1,
        max_length=100,
    )
    region: str | None = Field(
        None,
        description="Geographic region",
        max_length=100,
    )
    city: str | None = Field(
        None,
        description="City",
        max_length=100,
    )
    postal_code: str | None = Field(
        None,
        description="Postal code",
        max_length=20,
    )

    # Segmentation
    segment: CustomerSegment = Field(
        default=CustomerSegment.NEW,
        description="Customer segment",
    )
    lifetime_value: Decimal | None = Field(
        None,
        description="Customer lifetime value",
        ge=0,
    )
    acquisition_channel: str | None = Field(
        None,
        description="How customer was acquired",
        max_length=100,
    )

    # Preferences
    preferred_categories: list[str] = Field(
        default_factory=list,
        description="Preferred product categories",
    )
    preferred_payment_method: str | None = Field(
        None,
        description="Preferred payment method",
        max_length=50,
    )

    # Status
    is_active: bool = Field(
        default=True,
        description="Whether customer is active",
    )
    is_vip: bool = Field(
        default=False,
        description="Whether customer is VIP",
    )
    has_account: bool = Field(
        default=False,
        description="Whether customer has an account",
    )

    # Metrics
    metrics: CustomerMetrics | None = Field(
        None,
        description="Customer behavioral metrics",
    )

    # Dates
    registration_date: datetime | None = Field(
        None,
        description="Customer registration date",
    )
    last_active_date: datetime | None = Field(
        None,
        description="Last activity date",
    )

    @field_validator("customer_id")
    @classmethod
    def validate_customer_id(cls, v: str) -> str:
        """Validate customer ID format."""
        v = str(v).strip()
        if not v:
            raise ValueError("Customer ID cannot be empty")
        return v

    @field_validator("country")
    @classmethod
    def standardize_country(cls, v: str) -> str:
        """Standardize country name."""
        v = v.strip().title()
        # Fix common variations
        replacements = {
            "Uk": "United Kingdom",
            "Usa": "United States",
            "U.S.A.": "United States",
            "Uae": "United Arab Emirates",
            "Rsa": "South Africa",
            "Eire": "Ireland",
        }
        return replacements.get(v, v)

    @model_validator(mode="after")
    def determine_segment(self) -> "Customer":
        """Determine customer segment based on metrics."""
        if self.metrics:
            # VIP customers
            if (self.metrics.total_spent > 10000 or
                    self.metrics.total_transactions > 100):
                self.segment = CustomerSegment.VIP
                self.is_vip = True

            # Frequent buyers
            elif (self.metrics.total_transactions > 20 and
                  self.metrics.purchase_frequency_days and
                  self.metrics.purchase_frequency_days < 30):
                self.segment = CustomerSegment.FREQUENT

            # Regular customers
            elif self.metrics.total_transactions > 5:
                self.segment = CustomerSegment.REGULAR

            # Occasional customers
            elif self.metrics.total_transactions > 1:
                self.segment = CustomerSegment.OCCASIONAL

            # Dormant customers
            elif (self.metrics.days_since_last_purchase and
                  self.metrics.days_since_last_purchase > 365):
                self.segment = CustomerSegment.DORMANT
                self.is_active = False

            # Churned customers
            elif (self.metrics.days_since_last_purchase and
                  self.metrics.days_since_last_purchase > 730):
                self.segment = CustomerSegment.CHURNED
                self.is_active = False

            # Update lifetime value
            self.lifetime_value = self.metrics.total_spent

            # Update last active date
            self.last_active_date = self.metrics.last_purchase_date

        return self

    def validate_business_rules(self) -> bool:
        """Validate customer against business rules."""
        self.clear_validation_errors()

        # Validate metrics if present
        if self.metrics:
            if not self.metrics.validate_business_rules():
                self.add_validation_error(
                    f"Metrics validation failed: {self.metrics.validation_errors}"
                )

        # Rule 1: Customer ID format
        try:
            float(self.customer_id)  # Should be numeric in this dataset
        except ValueError:
            self.add_validation_error(
                f"Invalid customer ID format: {self.customer_id}"
            )

        # Rule 2: Country required
        if not self.country:
            self.add_validation_error("Country is required")

        # Rule 3: VIP consistency
        if self.is_vip and self.segment not in [CustomerSegment.VIP, CustomerSegment.FREQUENT]:
            self.add_validation_error(
                f"VIP flag inconsistent with segment: {self.segment}"
            )

        # Rule 4: Activity consistency
        if not self.is_active and self.segment in [CustomerSegment.VIP, CustomerSegment.FREQUENT]:
            self.add_validation_error(
                f"Inactive customer with active segment: {self.segment}"
            )

        return self.is_valid

    def to_analytics_dict(self) -> dict[str, Any]:
        """Convert customer to analytics-ready dictionary."""
        result = {
            "customer_id": self.customer_id,
            "country": self.country,
            "segment": self.segment.value,
            "is_active": self.is_active,
            "is_vip": self.is_vip,
        }

        if self.metrics:
            result.update({
                "total_transactions": str(self.metrics.total_transactions),
                "total_spent": str(float(self.metrics.total_spent)),
                "avg_transaction_value": str(float(self.metrics.avg_transaction_value))
                if self.metrics.avg_transaction_value else "None",
                "days_since_last_purchase": str(self.metrics.days_since_last_purchase)
                if self.metrics.days_since_last_purchase is not None else "None",
                "return_rate": str(self.metrics.return_rate)
                if self.metrics.return_rate is not None else "None",
                "rfm_score": str(self.metrics.rfm_score)
                if self.metrics.rfm_score is not None else "None",
            })

        return result
