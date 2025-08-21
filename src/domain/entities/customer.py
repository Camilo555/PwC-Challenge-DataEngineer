"""
Domain Entity for Customer
Pure Pydantic model for customer business logic and validation.
"""
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class CustomerSegment(str, Enum):
    """Customer segment enumeration."""
    VIP = "VIP"
    PREMIUM = "Premium"
    REGULAR = "Regular"
    NEW = "New"
    INACTIVE = "Inactive"


class CustomerValueTier(str, Enum):
    """Customer value tier enumeration."""
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class CustomerStatus(str, Enum):
    """Customer status enumeration."""
    ACTIVE = "Active"
    INACTIVE = "Inactive"
    SUSPENDED = "Suspended"
    CHURNED = "Churned"


class CustomerEntity(BaseModel):
    """
    Pure domain entity for customers.
    Contains customer business logic and validation rules.
    """
    
    # Core identifiers
    customer_key: Optional[int] = None
    customer_id: str = Field(min_length=1, max_length=50)
    
    # Customer profile
    customer_segment: CustomerSegment = CustomerSegment.NEW
    customer_value_tier: Optional[CustomerValueTier] = None
    status: CustomerStatus = CustomerStatus.ACTIVE
    
    # Financial metrics
    lifetime_value: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=12, decimal_places=2)
    avg_order_value: Decimal = Field(default=Decimal('0.00'), ge=0, max_digits=10, decimal_places=2)
    order_count: int = Field(default=0, ge=0)
    
    # Temporal information
    first_order_date: Optional[date] = None
    last_order_date: Optional[date] = None
    
    # Metadata
    created_at: Optional[datetime] = None
    is_active: bool = True
    
    @field_validator('customer_id')
    @classmethod
    def validate_customer_id(cls, v: str) -> str:
        """Validate customer ID format."""
        if not v or v.isspace():
            raise ValueError('Customer ID cannot be empty')
        
        v = v.strip()
        
        # Business rule: Customer ID should be numeric in retail context
        try:
            float(v)
            return v
        except ValueError:
            raise ValueError('Customer ID must be numeric')
    
    def is_high_value_customer(self) -> bool:
        """Business rule: Check if this is a high-value customer."""
        return (self.customer_value_tier == CustomerValueTier.HIGH or 
                self.customer_segment in [CustomerSegment.VIP, CustomerSegment.PREMIUM])
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }
        validate_assignment = True