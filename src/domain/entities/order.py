"""
Domain Entity for Order
Pure Pydantic model for order business logic and validation.
Represents individual orders containing multiple products and customer information.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, computed_field


class OrderStatus(str, Enum):
    """Order status enumeration."""
    PENDING = "Pending"
    CONFIRMED = "Confirmed"
    PROCESSING = "Processing"
    SHIPPED = "Shipped"
    DELIVERED = "Delivered"
    CANCELLED = "Cancelled"
    RETURNED = "Returned"
    REFUNDED = "Refunded"


class PaymentStatus(str, Enum):
    """Payment status enumeration."""
    PENDING = "Pending"
    AUTHORIZED = "Authorized"
    CAPTURED = "Captured"
    PARTIALLY_PAID = "Partially_Paid"
    PAID = "Paid"
    FAILED = "Failed"
    REFUNDED = "Refunded"
    PARTIALLY_REFUNDED = "Partially_Refunded"


class OrderPriority(str, Enum):
    """Order priority enumeration."""
    LOW = "Low"
    NORMAL = "Normal"
    HIGH = "High"
    URGENT = "Urgent"


class ShippingMethod(str, Enum):
    """Shipping method enumeration."""
    STANDARD = "Standard"
    EXPRESS = "Express"
    OVERNIGHT = "Overnight"
    PICKUP = "Pickup"
    DIGITAL = "Digital"


class OrderLineItem(BaseModel):
    """Individual line item within an order."""
    
    line_item_id: Optional[int] = None
    product_id: str = Field(min_length=1, max_length=50)
    product_name: Optional[str] = Field(max_length=200)
    sku: Optional[str] = Field(max_length=100)
    quantity: int = Field(gt=0, description="Quantity must be positive")
    unit_price: Decimal = Field(ge=0, max_digits=10, decimal_places=2)
    discount_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=8, decimal_places=2)
    tax_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=8, decimal_places=2)
    
    # Product categorization
    category: Optional[str] = Field(max_length=100)
    subcategory: Optional[str] = Field(max_length=100)
    
    # Fulfillment information
    is_digital: bool = False
    shipping_required: bool = True
    
    @computed_field
    @property
    def subtotal(self) -> Decimal:
        """Calculate subtotal before tax and discounts."""
        return self.unit_price * self.quantity
    
    @computed_field
    @property
    def discount_percentage(self) -> float:
        """Calculate discount percentage."""
        if self.subtotal > 0:
            return float((self.discount_amount / self.subtotal) * 100)
        return 0.0
    
    @computed_field
    @property
    def total_amount(self) -> Decimal:
        """Calculate total amount including tax and discounts."""
        return self.subtotal - self.discount_amount + self.tax_amount
    
    @field_validator('product_id')
    @classmethod
    def validate_product_id(cls, v: str) -> str:
        """Validate product ID format."""
        if not v or v.isspace():
            raise ValueError('Product ID cannot be empty')
        return v.strip()
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            Decimal: lambda v: float(v)
        }


class ShippingAddress(BaseModel):
    """Shipping address information."""
    
    recipient_name: str = Field(min_length=1, max_length=100)
    company: Optional[str] = Field(max_length=100)
    address_line1: str = Field(min_length=1, max_length=200)
    address_line2: Optional[str] = Field(max_length=200)
    city: str = Field(min_length=1, max_length=100)
    state_province: Optional[str] = Field(max_length=100)
    postal_code: str = Field(min_length=1, max_length=20)
    country: str = Field(min_length=2, max_length=3)  # ISO country code
    phone: Optional[str] = Field(max_length=20)
    
    @field_validator('country')
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        """Validate country code format."""
        v = v.strip().upper()
        if len(v) not in [2, 3]:
            raise ValueError('Country code must be 2 or 3 characters')
        return v
    
    @computed_field
    @property
    def full_address(self) -> str:
        """Get formatted full address."""
        address_parts = [self.address_line1]
        if self.address_line2:
            address_parts.append(self.address_line2)
        address_parts.append(f"{self.city}, {self.state_province or ''} {self.postal_code}".strip())
        address_parts.append(self.country)
        return ', '.join(filter(None, address_parts))


class PaymentInformation(BaseModel):
    """Payment information for the order."""
    
    payment_method: str = Field(min_length=1, max_length=50)
    payment_status: PaymentStatus = PaymentStatus.PENDING
    payment_reference: Optional[str] = Field(max_length=100)
    authorized_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=12, decimal_places=2)
    captured_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=12, decimal_places=2)
    refunded_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=12, decimal_places=2)
    
    # Payment dates
    authorized_at: Optional[datetime] = None
    captured_at: Optional[datetime] = None
    
    # Currency information
    currency_code: str = Field(default="USD", min_length=3, max_length=3)
    exchange_rate: Decimal = Field(default=Decimal('1.00'), gt=0, max_digits=10, decimal_places=6)
    
    @computed_field
    @property
    def remaining_amount(self) -> Decimal:
        """Calculate remaining amount to capture."""
        return self.authorized_amount - self.captured_amount
    
    @computed_field
    @property
    def refundable_amount(self) -> Decimal:
        """Calculate amount available for refund."""
        return self.captured_amount - self.refunded_amount
    
    @field_validator('currency_code')
    @classmethod
    def validate_currency_code(cls, v: str) -> str:
        """Validate currency code format."""
        return v.strip().upper()


class OrderEntity(BaseModel):
    """
    Pure domain entity for orders.
    Contains order business logic and validation rules.
    """
    
    # Core identifiers
    order_key: Optional[int] = None
    order_id: str = Field(min_length=1, max_length=50)
    order_number: Optional[str] = Field(max_length=50)
    
    # Customer information
    customer_id: str = Field(min_length=1, max_length=50)
    customer_email: Optional[str] = Field(max_length=200)
    
    # Order status and timing
    status: OrderStatus = OrderStatus.PENDING
    priority: OrderPriority = OrderPriority.NORMAL
    order_date: datetime
    required_date: Optional[date] = None
    shipped_date: Optional[date] = None
    delivered_date: Optional[date] = None
    
    # Line items
    line_items: List[OrderLineItem] = Field(min_items=1)
    
    # Financial information
    subtotal: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    tax_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=10, decimal_places=2)
    shipping_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=8, decimal_places=2)
    discount_amount: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=10, decimal_places=2)
    total_amount: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    
    # Payment information
    payment: Optional[PaymentInformation] = None
    
    # Shipping information
    shipping_method: ShippingMethod = ShippingMethod.STANDARD
    shipping_address: Optional[ShippingAddress] = None
    tracking_number: Optional[str] = Field(max_length=100)
    
    # Additional attributes
    notes: Optional[str] = Field(max_length=1000)
    source_channel: Optional[str] = Field(default="online", max_length=50)
    sales_representative: Optional[str] = Field(max_length=100)
    
    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = Field(max_length=100)
    updated_by: Optional[str] = Field(max_length=100)
    
    # Flags
    is_gift: bool = False
    is_recurring: bool = False
    requires_special_handling: bool = False
    
    @field_validator('order_id')
    @classmethod
    def validate_order_id(cls, v: str) -> str:
        """Validate order ID format."""
        if not v or v.isspace():
            raise ValueError('Order ID cannot be empty')
        return v.strip()
    
    @field_validator('customer_id')
    @classmethod
    def validate_customer_id(cls, v: str) -> str:
        """Validate customer ID format."""
        if not v or v.isspace():
            raise ValueError('Customer ID cannot be empty')
        return v.strip()
    
    @field_validator('line_items')
    @classmethod
    def validate_line_items(cls, v: List[OrderLineItem]) -> List[OrderLineItem]:
        """Validate line items."""
        if not v:
            raise ValueError('Order must have at least one line item')
        return v
    
    @computed_field
    @property
    def line_item_count(self) -> int:
        """Get total number of line items."""
        return len(self.line_items)
    
    @computed_field
    @property
    def total_quantity(self) -> int:
        """Get total quantity across all line items."""
        return sum(item.quantity for item in self.line_items)
    
    @computed_field
    @property
    def calculated_subtotal(self) -> Decimal:
        """Calculate subtotal from line items."""
        return sum(item.subtotal for item in self.line_items)
    
    @computed_field
    @property
    def calculated_tax_amount(self) -> Decimal:
        """Calculate total tax from line items."""
        return sum(item.tax_amount for item in self.line_items)
    
    @computed_field
    @property
    def calculated_total(self) -> Decimal:
        """Calculate total amount including shipping and discounts."""
        return (self.calculated_subtotal + 
                self.calculated_tax_amount + 
                self.shipping_amount - 
                self.discount_amount)
    
    @computed_field
    @property
    def is_paid(self) -> bool:
        """Check if order is fully paid."""
        if not self.payment:
            return False
        return (self.payment.payment_status in [PaymentStatus.PAID, PaymentStatus.CAPTURED] and
                self.payment.captured_amount >= self.total_amount)
    
    @computed_field
    @property
    def is_shipped(self) -> bool:
        """Check if order has been shipped."""
        return self.status in [OrderStatus.SHIPPED, OrderStatus.DELIVERED]
    
    @computed_field
    @property
    def is_delivered(self) -> bool:
        """Check if order has been delivered."""
        return self.status == OrderStatus.DELIVERED
    
    @computed_field
    @property
    def is_cancelled(self) -> bool:
        """Check if order is cancelled."""
        return self.status in [OrderStatus.CANCELLED, OrderStatus.RETURNED, OrderStatus.REFUNDED]
    
    @computed_field
    @property
    def days_since_order(self) -> int:
        """Calculate days since order was placed."""
        if self.order_date:
            return (datetime.utcnow() - self.order_date).days
        return 0
    
    @computed_field
    @property
    def is_overdue(self) -> bool:
        """Check if order is overdue based on required date."""
        if not self.required_date:
            return False
        return (date.today() > self.required_date and 
                self.status not in [OrderStatus.DELIVERED, OrderStatus.CANCELLED, 
                                  OrderStatus.RETURNED, OrderStatus.REFUNDED])
    
    def validate_financial_consistency(self) -> List[str]:
        """Validate financial calculations are consistent."""
        errors = []
        
        # Check subtotal consistency
        calculated_subtotal = self.calculated_subtotal
        if abs(self.subtotal - calculated_subtotal) > Decimal('0.01'):
            errors.append(f"Subtotal mismatch: stored={self.subtotal}, calculated={calculated_subtotal}")
        
        # Check total consistency
        calculated_total = self.calculated_total
        if abs(self.total_amount - calculated_total) > Decimal('0.01'):
            errors.append(f"Total amount mismatch: stored={self.total_amount}, calculated={calculated_total}")
        
        # Check payment consistency
        if self.payment and self.payment.authorized_amount > 0:
            if self.payment.authorized_amount < self.total_amount:
                errors.append("Authorized amount is less than order total")
        
        return errors
    
    def get_products_summary(self) -> Dict[str, Any]:
        """Get summary of products in the order."""
        categories = set()
        unique_products = set()
        
        for item in self.line_items:
            if item.category:
                categories.add(item.category)
            unique_products.add(item.product_id)
        
        return {
            "unique_products": len(unique_products),
            "total_quantity": self.total_quantity,
            "categories": list(categories),
            "has_digital_items": any(item.is_digital for item in self.line_items),
            "requires_shipping": any(item.shipping_required for item in self.line_items)
        }
    
    def can_be_cancelled(self) -> bool:
        """Check if order can be cancelled."""
        return self.status in [OrderStatus.PENDING, OrderStatus.CONFIRMED, OrderStatus.PROCESSING]
    
    def can_be_refunded(self) -> bool:
        """Check if order can be refunded."""
        return (self.status in [OrderStatus.DELIVERED] and
                self.payment and
                self.payment.payment_status in [PaymentStatus.PAID, PaymentStatus.CAPTURED])
    
    def get_fulfillment_status(self) -> Dict[str, Any]:
        """Get comprehensive fulfillment status."""
        return {
            "order_status": self.status.value,
            "payment_status": self.payment.payment_status.value if self.payment else "No Payment Info",
            "is_paid": self.is_paid,
            "is_shipped": self.is_shipped,
            "is_delivered": self.is_delivered,
            "days_since_order": self.days_since_order,
            "is_overdue": self.is_overdue,
            "can_cancel": self.can_be_cancelled(),
            "can_refund": self.can_be_refunded()
        }
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }
        validate_assignment = True