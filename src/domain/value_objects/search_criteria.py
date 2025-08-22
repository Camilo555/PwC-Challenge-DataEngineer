"""
Value Objects for Search and Filtering Operations
Encapsulates search parameters with validation and business rules.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass(frozen=True)
class SalesSearchCriteria:
    """
    Value object for sales search parameters.
    Immutable and self-validating.
    """
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    product: Optional[str] = None
    country: Optional[str] = None
    page: int = 1
    size: int = 20
    sort: str = "invoice_date:desc"
    
    def __post_init__(self):
        """Validate criteria after initialization."""
        if self.page < 1:
            raise ValueError("Page must be greater than 0")
        
        if self.size < 1 or self.size > 1000:
            raise ValueError("Size must be between 1 and 1000")
        
        if self.date_from:
            try:
                datetime.fromisoformat(self.date_from.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("Invalid date_from format. Use ISO format.")
        
        if self.date_to:
            try:
                datetime.fromisoformat(self.date_to.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("Invalid date_to format. Use ISO format.")
        
        # Validate sort format
        if ':' in self.sort:
            field, direction = self.sort.split(':', 1)
            if direction.lower() not in ['asc', 'desc']:
                raise ValueError("Sort direction must be 'asc' or 'desc'")
        
    @property
    def offset(self) -> int:
        """Calculate offset for pagination."""
        return (self.page - 1) * self.size


@dataclass(frozen=True)
class CustomerSearchCriteria:
    """Value object for customer search parameters."""
    customer_id: Optional[str] = None
    email: Optional[str] = None
    segment: Optional[str] = None
    country: Optional[str] = None
    page: int = 1
    size: int = 20
    
    def __post_init__(self):
        if self.page < 1:
            raise ValueError("Page must be greater than 0")
        
        if self.size < 1 or self.size > 1000:
            raise ValueError("Size must be between 1 and 1000")


@dataclass(frozen=True)
class ProductSearchCriteria:
    """Value object for product search parameters."""
    stock_code: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    page: int = 1
    size: int = 20
    
    def __post_init__(self):
        if self.page < 1:
            raise ValueError("Page must be greater than 0")
        
        if self.size < 1 or self.size > 1000:
            raise ValueError("Size must be between 1 and 1000")
        
        if self.price_min is not None and self.price_min < 0:
            raise ValueError("Minimum price cannot be negative")
        
        if self.price_max is not None and self.price_max < 0:
            raise ValueError("Maximum price cannot be negative")
        
        if (self.price_min is not None and self.price_max is not None 
            and self.price_min > self.price_max):
            raise ValueError("Minimum price cannot be greater than maximum price")