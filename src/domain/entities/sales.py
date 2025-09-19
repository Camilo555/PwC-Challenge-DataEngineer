"""
Sales Domain Entity
==================

Comprehensive sales domain model with advanced business rules,
analytics capabilities, and data quality validation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator, root_validator, model_validator
from pydantic.types import PositiveInt, PositiveFloat, NonNegativeFloat


class SalesChannelType(str, Enum):
    """Sales channel types."""
    ONLINE = "online"
    IN_STORE = "in_store"
    PHONE = "phone"
    MOBILE_APP = "mobile_app"
    SOCIAL_COMMERCE = "social_commerce"
    MARKETPLACE = "marketplace"
    B2B_PORTAL = "b2b_portal"


class PaymentMethodType(str, Enum):
    """Payment method types."""
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    CASH = "cash"
    BANK_TRANSFER = "bank_transfer"
    DIGITAL_WALLET = "digital_wallet"
    CRYPTOCURRENCY = "cryptocurrency"
    BUY_NOW_PAY_LATER = "buy_now_pay_later"
    STORE_CREDIT = "store_credit"


class SalesStatus(str, Enum):
    """Sales transaction status."""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"
    RETURNED = "returned"


class ReturnReason(str, Enum):
    """Product return reasons."""
    DEFECTIVE = "defective"
    WRONG_ITEM = "wrong_item"
    NOT_AS_DESCRIBED = "not_as_described"
    SIZE_ISSUE = "size_issue"
    CUSTOMER_CHANGED_MIND = "customer_changed_mind"
    DAMAGED_IN_SHIPPING = "damaged_in_shipping"
    LATE_DELIVERY = "late_delivery"
    DUPLICATE_ORDER = "duplicate_order"


class RiskLevel(str, Enum):
    """Transaction risk levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class SalesLineItem:
    """Individual line item in a sales transaction."""
    product_id: str
    product_name: str
    product_sku: str
    category: Optional[str] = None
    brand: Optional[str] = None
    quantity: int = 1
    unit_price: Decimal = Decimal('0.00')
    discount_amount: Decimal = Decimal('0.00')
    tax_amount: Decimal = Decimal('0.00')
    total_amount: Decimal = field(init=False)
    
    # Product attributes
    product_weight: Optional[Decimal] = None
    product_dimensions: Optional[str] = None
    product_color: Optional[str] = None
    product_size: Optional[str] = None
    
    # Pricing and margins
    cost_of_goods: Optional[Decimal] = None
    margin_amount: Optional[Decimal] = field(init=False)
    margin_percentage: Optional[Decimal] = field(init=False)
    
    # Inventory and fulfillment
    warehouse_location: Optional[str] = None
    fulfillment_center: Optional[str] = None
    inventory_reservation_id: Optional[UUID] = None
    
    def __post_init__(self):
        """Calculate derived fields."""
        self.total_amount = (self.quantity * self.unit_price) - self.discount_amount + self.tax_amount
        
        if self.cost_of_goods is not None:
            self.margin_amount = self.total_amount - (self.quantity * self.cost_of_goods)
            if self.total_amount > 0:
                self.margin_percentage = (self.margin_amount / self.total_amount) * 100

    @property
    def is_profitable(self) -> bool:
        """Check if line item is profitable."""
        return self.margin_amount is not None and self.margin_amount > 0

    @property
    def effective_unit_price(self) -> Decimal:
        """Get effective unit price after discounts."""
        return (self.unit_price - (self.discount_amount / self.quantity)) if self.quantity > 0 else Decimal('0.00')


class SalesAnalytics(BaseModel):
    """Advanced sales analytics and metrics."""
    
    # Revenue metrics
    gross_revenue: NonNegativeFloat = 0.0
    net_revenue: NonNegativeFloat = 0.0
    total_discount: NonNegativeFloat = 0.0
    total_tax: NonNegativeFloat = 0.0
    total_profit: Optional[float] = None
    profit_margin_pct: Optional[float] = None
    
    # Volume metrics
    total_items: PositiveInt = 1
    unique_products: PositiveInt = 1
    average_order_value: NonNegativeFloat = 0.0
    average_unit_price: NonNegativeFloat = 0.0
    
    # Customer metrics
    is_new_customer: bool = False
    customer_lifetime_orders: PositiveInt = 1
    customer_lifetime_value: NonNegativeFloat = 0.0
    days_since_last_order: Optional[int] = None
    
    # Behavioral metrics
    items_per_category: Dict[str, int] = Field(default_factory=dict)
    brands_purchased: Set[str] = Field(default_factory=set)
    price_sensitivity_score: Optional[float] = None  # 0-100 scale
    
    # Risk and fraud metrics
    fraud_score: float = Field(default=0.0, ge=0.0, le=1.0)
    risk_level: RiskLevel = RiskLevel.LOW
    risk_factors: List[str] = Field(default_factory=list)
    
    # Operational metrics
    processing_time_minutes: Optional[float] = None
    fulfillment_complexity_score: Optional[float] = None
    shipping_cost: NonNegativeFloat = 0.0
    handling_cost: NonNegativeFloat = 0.0


class SalesTransaction(BaseModel):
    """
    Comprehensive sales transaction entity with advanced analytics.
    
    This model represents a complete sales transaction with all business rules,
    analytics, and data quality validations for enterprise-grade retail systems.
    """
    
    # Core identifiers
    transaction_id: UUID = Field(default_factory=uuid4)
    order_number: str = Field(..., min_length=1, max_length=50)
    invoice_number: Optional[str] = Field(None, max_length=50)
    
    # Customer information
    customer_id: Optional[str] = None
    customer_email: Optional[str] = Field(None, pattern=r'^[^@]+@[^@]+\.[^@]+$')
    customer_phone: Optional[str] = None
    
    # Transaction details
    line_items: List[SalesLineItem] = Field(..., min_items=1)
    sales_channel: SalesChannelType = SalesChannelType.ONLINE
    payment_method: PaymentMethodType = PaymentMethodType.CREDIT_CARD
    status: SalesStatus = SalesStatus.PENDING
    
    # Temporal information
    transaction_date: datetime = Field(default_factory=datetime.utcnow)
    order_date: Optional[datetime] = None
    payment_date: Optional[datetime] = None
    shipment_date: Optional[datetime] = None
    delivery_date: Optional[datetime] = None
    
    # Financial details
    subtotal: NonNegativeFloat = 0.0
    discount_total: NonNegativeFloat = 0.0
    tax_total: NonNegativeFloat = 0.0
    shipping_fee: NonNegativeFloat = 0.0
    handling_fee: NonNegativeFloat = 0.0
    total_amount: NonNegativeFloat = Field(..., gt=0)
    
    # Currency and localization
    currency_code: str = Field(default="USD", min_length=3, max_length=3)
    exchange_rate: Optional[PositiveFloat] = None
    locale: str = Field(default="en-US", max_length=10)
    
    # Location and shipping
    billing_country: Optional[str] = None
    billing_region: Optional[str] = None
    billing_city: Optional[str] = None
    billing_postal_code: Optional[str] = None
    
    shipping_country: Optional[str] = None
    shipping_region: Optional[str] = None  
    shipping_city: Optional[str] = None
    shipping_postal_code: Optional[str] = None
    shipping_method: Optional[str] = None
    
    # Business context
    sales_rep_id: Optional[str] = None
    store_location: Optional[str] = None
    campaign_id: Optional[str] = None
    referral_source: Optional[str] = None
    promotion_codes: List[str] = Field(default_factory=list)
    
    # Returns and refunds
    return_reason: Optional[ReturnReason] = None
    return_date: Optional[datetime] = None
    refund_amount: NonNegativeFloat = 0.0
    restocking_fee: NonNegativeFloat = 0.0
    
    # Advanced analytics
    analytics: Optional[SalesAnalytics] = None
    
    # Data quality and audit
    data_quality_score: float = Field(default=1.0, ge=0.0, le=1.0)
    data_source: str = Field(default="manual_entry", max_length=50)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    # Metadata
    tags: List[str] = Field(default_factory=list)
    notes: Optional[str] = None
    external_references: Dict[str, str] = Field(default_factory=dict)
    
    @validator('order_number')
    def validate_order_number(cls, v):
        """Validate order number format."""
        if not v or len(v.strip()) == 0:
            raise ValueError('Order number cannot be empty')
        return v.strip().upper()
    
    @validator('line_items')
    def validate_line_items(cls, v):
        """Validate line items."""
        if not v or len(v) == 0:
            raise ValueError('At least one line item is required')
        
        # Check for duplicate product IDs
        product_ids = [item.product_id for item in v]
        if len(product_ids) != len(set(product_ids)):
            raise ValueError('Duplicate products found in line items')
        
        return v
    
    @model_validator(mode="before")
    @classmethod
    def validate_financial_totals(cls, values):
        """Validate that financial totals are consistent."""
        line_items = values.get('line_items', [])
        if not line_items:
            return values
        
        # Calculate totals from line items
        calculated_subtotal = sum(item.quantity * item.unit_price for item in line_items)
        calculated_discount = sum(item.discount_amount for item in line_items)
        calculated_tax = sum(item.tax_amount for item in line_items)
        
        subtotal = values.get('subtotal', 0)
        discount_total = values.get('discount_total', 0)
        tax_total = values.get('tax_total', 0)
        shipping_fee = values.get('shipping_fee', 0)
        handling_fee = values.get('handling_fee', 0)
        total_amount = values.get('total_amount', 0)
        
        # Allow small rounding differences (up to 0.01)
        tolerance = Decimal('0.01')
        
        if abs(calculated_subtotal - subtotal) > tolerance:
            raise ValueError(f'Subtotal mismatch: calculated {calculated_subtotal}, provided {subtotal}')
        
        if abs(calculated_discount - discount_total) > tolerance:
            raise ValueError(f'Discount total mismatch: calculated {calculated_discount}, provided {discount_total}')
        
        if abs(calculated_tax - tax_total) > tolerance:
            raise ValueError(f'Tax total mismatch: calculated {calculated_tax}, provided {tax_total}')
        
        expected_total = subtotal - discount_total + tax_total + shipping_fee + handling_fee
        if abs(expected_total - total_amount) > tolerance:
            raise ValueError(f'Total amount mismatch: expected {expected_total}, provided {total_amount}')
        
        return values
    
    @model_validator(mode="before")
    @classmethod
    def validate_dates(cls, values):
        """Validate date consistency."""
        transaction_date = values.get('transaction_date')
        order_date = values.get('order_date')
        payment_date = values.get('payment_date')
        shipment_date = values.get('shipment_date')
        delivery_date = values.get('delivery_date')
        return_date = values.get('return_date')
        
        dates = [
            ('order_date', order_date),
            ('transaction_date', transaction_date),
            ('payment_date', payment_date),
            ('shipment_date', shipment_date),
            ('delivery_date', delivery_date),
            ('return_date', return_date)
        ]
        
        # Filter out None dates and sort
        valid_dates = [(name, dt) for name, dt in dates if dt is not None]
        
        # Basic chronological validation
        if order_date and transaction_date and order_date > transaction_date:
            raise ValueError('Order date cannot be after transaction date')
        
        if shipment_date and delivery_date and shipment_date > delivery_date:
            raise ValueError('Shipment date cannot be after delivery date')
        
        return values
    
    def calculate_analytics(self) -> SalesAnalytics:
        """Calculate comprehensive sales analytics."""
        analytics = SalesAnalytics()
        
        # Revenue metrics
        analytics.gross_revenue = float(self.subtotal)
        analytics.net_revenue = float(self.total_amount)
        analytics.total_discount = float(self.discount_total)
        analytics.total_tax = float(self.tax_total)
        
        # Calculate profit if cost data is available
        total_cost = sum(
            float(item.cost_of_goods or 0) * item.quantity 
            for item in self.line_items 
            if item.cost_of_goods is not None
        )
        if total_cost > 0:
            analytics.total_profit = analytics.net_revenue - total_cost
            if analytics.net_revenue > 0:
                analytics.profit_margin_pct = (analytics.total_profit / analytics.net_revenue) * 100
        
        # Volume metrics
        analytics.total_items = sum(item.quantity for item in self.line_items)
        analytics.unique_products = len(self.line_items)
        analytics.average_order_value = analytics.net_revenue
        if analytics.total_items > 0:
            analytics.average_unit_price = analytics.gross_revenue / analytics.total_items
        
        # Product categorization
        for item in self.line_items:
            if item.category:
                analytics.items_per_category[item.category] = (
                    analytics.items_per_category.get(item.category, 0) + item.quantity
                )
            if item.brand:
                analytics.brands_purchased.add(item.brand)
        
        # Operational metrics
        if self.transaction_date and self.delivery_date:
            processing_time = self.delivery_date - self.transaction_date
            analytics.processing_time_minutes = processing_time.total_seconds() / 60
        
        analytics.shipping_cost = float(self.shipping_fee)
        analytics.handling_cost = float(self.handling_fee)
        
        # Risk assessment (simplified)
        risk_factors = []
        fraud_score = 0.0
        
        # High value transaction
        if analytics.net_revenue > 5000:
            risk_factors.append("high_value_transaction")
            fraud_score += 0.2
        
        # Many items
        if analytics.total_items > 10:
            risk_factors.append("high_volume_order")
            fraud_score += 0.1
        
        # International shipping
        if self.billing_country != self.shipping_country:
            risk_factors.append("international_shipping")
            fraud_score += 0.15
        
        # Rush processing
        if (self.shipment_date and self.transaction_date and 
            (self.shipment_date - self.transaction_date).days == 0):
            risk_factors.append("same_day_shipping")
            fraud_score += 0.1
        
        analytics.risk_factors = risk_factors
        analytics.fraud_score = min(fraud_score, 1.0)
        
        if fraud_score >= 0.7:
            analytics.risk_level = RiskLevel.CRITICAL
        elif fraud_score >= 0.5:
            analytics.risk_level = RiskLevel.HIGH
        elif fraud_score >= 0.3:
            analytics.risk_level = RiskLevel.MEDIUM
        else:
            analytics.risk_level = RiskLevel.LOW
        
        return analytics
    
    def update_analytics(self):
        """Update the analytics field with calculated values."""
        self.analytics = self.calculate_analytics()
        self.updated_at = datetime.utcnow()
    
    @property
    def is_returnable(self) -> bool:
        """Check if transaction is eligible for return."""
        if self.status in [SalesStatus.CANCELLED, SalesStatus.REFUNDED, SalesStatus.RETURNED]:
            return False
        
        if self.status != SalesStatus.DELIVERED:
            return False
        
        # Check if within return window (30 days)
        if self.delivery_date:
            days_since_delivery = (datetime.utcnow() - self.delivery_date).days
            return days_since_delivery <= 30
        
        return False
    
    @property
    def is_high_value(self) -> bool:
        """Check if this is a high-value transaction."""
        return self.total_amount >= 1000.0
    
    @property
    def is_international(self) -> bool:
        """Check if this is an international transaction."""
        return (self.billing_country and self.shipping_country and 
                self.billing_country != self.shipping_country)
    
    @property
    def days_since_transaction(self) -> int:
        """Get days since transaction date."""
        return (datetime.utcnow() - self.transaction_date).days
    
    @property
    def fulfillment_status(self) -> str:
        """Get human-readable fulfillment status."""
        status_map = {
            SalesStatus.PENDING: "Order Received",
            SalesStatus.CONFIRMED: "Order Confirmed", 
            SalesStatus.PROCESSING: "Preparing for Shipment",
            SalesStatus.SHIPPED: "In Transit",
            SalesStatus.DELIVERED: "Delivered",
            SalesStatus.CANCELLED: "Cancelled",
            SalesStatus.REFUNDED: "Refunded",
            SalesStatus.RETURNED: "Returned"
        }
        return status_map.get(self.status, "Unknown")
    
    def to_analytics_dict(self) -> Dict[str, Any]:
        """Convert to dictionary optimized for analytics."""
        if not self.analytics:
            self.update_analytics()
        
        return {
            'transaction_id': str(self.transaction_id),
            'order_number': self.order_number,
            'customer_id': self.customer_id,
            'transaction_date': self.transaction_date.isoformat(),
            'sales_channel': self.sales_channel.value,
            'payment_method': self.payment_method.value,
            'status': self.status.value,
            
            # Financial metrics
            'total_amount': float(self.total_amount),
            'subtotal': float(self.subtotal),
            'discount_total': float(self.discount_total),
            'tax_total': float(self.tax_total),
            'shipping_fee': float(self.shipping_fee),
            'currency_code': self.currency_code,
            
            # Analytics metrics
            'gross_revenue': self.analytics.gross_revenue if self.analytics else 0,
            'net_revenue': self.analytics.net_revenue if self.analytics else 0,
            'total_profit': self.analytics.total_profit if self.analytics else None,
            'profit_margin_pct': self.analytics.profit_margin_pct if self.analytics else None,
            'total_items': self.analytics.total_items if self.analytics else 0,
            'unique_products': self.analytics.unique_products if self.analytics else 0,
            'average_order_value': self.analytics.average_order_value if self.analytics else 0,
            
            # Geographic
            'billing_country': self.billing_country,
            'shipping_country': self.shipping_country,
            'is_international': self.is_international,
            
            # Risk and flags
            'is_high_value': self.is_high_value,
            'fraud_score': self.analytics.fraud_score if self.analytics else 0,
            'risk_level': self.analytics.risk_level.value if self.analytics else 'low',
            
            # Temporal
            'days_since_transaction': self.days_since_transaction,
            
            # Product details
            'product_categories': list(self.analytics.items_per_category.keys()) if self.analytics else [],
            'brands_purchased': list(self.analytics.brands_purchased) if self.analytics else [],
            
            # Data quality
            'data_quality_score': self.data_quality_score,
            'data_source': self.data_source
        }
    
    def validate_business_rules(self) -> List[str]:
        """Validate business rules and return list of violations."""
        violations = []
        
        # Minimum order value
        if self.total_amount < 0.01:
            violations.append("Total amount must be at least $0.01")
        
        # Maximum order value (fraud prevention)
        if self.total_amount > 50000:
            violations.append("Total amount exceeds maximum limit of $50,000")
        
        # Customer email required for online orders
        if self.sales_channel == SalesChannelType.ONLINE and not self.customer_email:
            violations.append("Customer email is required for online orders")
        
        # Shipping address required for shipped orders
        if (self.status in [SalesStatus.SHIPPED, SalesStatus.DELIVERED] and 
            not self.shipping_country):
            violations.append("Shipping address is required for shipped orders")
        
        # Return validation
        if self.return_reason and self.status != SalesStatus.RETURNED:
            violations.append("Return reason specified but status is not 'returned'")
        
        if self.status == SalesStatus.RETURNED and not self.return_reason:
            violations.append("Return reason is required for returned orders")
        
        # Refund validation
        if self.refund_amount > 0 and self.refund_amount > self.total_amount:
            violations.append("Refund amount cannot exceed total amount")
        
        # Date validation
        if self.delivery_date and self.transaction_date and self.delivery_date < self.transaction_date:
            violations.append("Delivery date cannot be before transaction date")
        
        return violations
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        validate_assignment = True
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: float(v),
            UUID: lambda v: str(v)
        }


# Factory functions and utilities

def create_sales_transaction(
    order_number: str,
    line_items: List[Dict[str, Any]],
    customer_id: Optional[str] = None,
    **kwargs
) -> SalesTransaction:
    """Factory function to create a sales transaction with validation."""
    
    # Convert line item dictionaries to SalesLineItem objects
    line_item_objects = []
    for item_data in line_items:
        line_item = SalesLineItem(**item_data)
        line_item_objects.append(line_item)
    
    # Calculate totals
    subtotal = sum(item.quantity * item.unit_price for item in line_item_objects)
    discount_total = sum(item.discount_amount for item in line_item_objects)
    tax_total = sum(item.tax_amount for item in line_item_objects)
    
    transaction_data = {
        'order_number': order_number,
        'line_items': line_item_objects,
        'customer_id': customer_id,
        'subtotal': float(subtotal),
        'discount_total': float(discount_total),
        'tax_total': float(tax_total),
        'total_amount': float(subtotal - discount_total + tax_total + kwargs.get('shipping_fee', 0) + kwargs.get('handling_fee', 0)),
        **kwargs
    }
    
    transaction = SalesTransaction(**transaction_data)
    transaction.update_analytics()
    
    return transaction


def create_sample_sales_transaction() -> SalesTransaction:
    """Create a sample sales transaction for testing."""
    line_items = [
        {
            'product_id': 'PROD-001',
            'product_name': 'Wireless Headphones',
            'product_sku': 'WH-BT-001',
            'category': 'Electronics',
            'brand': 'TechBrand',
            'quantity': 1,
            'unit_price': Decimal('199.99'),
            'discount_amount': Decimal('20.00'),
            'tax_amount': Decimal('14.40'),
            'cost_of_goods': Decimal('120.00')
        },
        {
            'product_id': 'PROD-002', 
            'product_name': 'Phone Case',
            'product_sku': 'PC-TPU-002',
            'category': 'Accessories',
            'brand': 'ProtectPlus',
            'quantity': 2,
            'unit_price': Decimal('24.99'),
            'discount_amount': Decimal('5.00'),
            'tax_amount': Decimal('3.20'),
            'cost_of_goods': Decimal('8.00')
        }
    ]
    
    return create_sales_transaction(
        order_number='ORD-2024-001',
        line_items=line_items,
        customer_id='CUST-12345',
        customer_email='customer@example.com',
        sales_channel=SalesChannelType.ONLINE,
        payment_method=PaymentMethodType.CREDIT_CARD,
        shipping_fee=12.99,
        handling_fee=2.50,
        billing_country='US',
        shipping_country='US',
        currency_code='USD'
    )