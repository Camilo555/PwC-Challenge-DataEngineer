"""
Domain Entity for Sales
Pure Pydantic model for business logic and validation.
"""
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


class TransactionStatus(str, Enum):
    """Transaction status enumeration."""
    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class PaymentMethod(str, Enum):
    """Payment method enumeration."""
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    CASH = "cash"
    BANK_TRANSFER = "bank_transfer"
    DIGITAL_WALLET = "digital_wallet"


class SaleEntity(BaseModel):
    """
    Pure domain entity for sales transactions.
    Contains business logic and validation rules.
    """
    
    # Core identifiers
    sale_id: Optional[UUID] = None
    invoice_no: str = Field(min_length=1, max_length=50)
    stock_code: str = Field(min_length=1, max_length=50)
    
    # Transaction details
    description: Optional[str] = Field(None, max_length=500)
    quantity: int = Field(gt=0, description="Quantity must be positive")
    unit_price: Decimal = Field(
        gt=0, 
        max_digits=10, 
        decimal_places=2,
        description="Unit price must be positive"
    )
    total_amount: Optional[Decimal] = Field(None, max_digits=12, decimal_places=2)
    discount_amount: Decimal = Field(default=Decimal('0.00'), ge=0)
    tax_amount: Decimal = Field(default=Decimal('0.00'), ge=0)
    
    # Financial metrics
    profit_amount: Optional[Decimal] = Field(None, max_digits=12, decimal_places=2)
    margin_percentage: Optional[Decimal] = Field(None, ge=0, le=100)
    
    # Customer information
    customer_id: Optional[str] = Field(None, max_length=50)
    customer_segment: Optional[str] = None
    customer_lifetime_value: Optional[Decimal] = None
    
    # Geographic information
    country: str = Field(min_length=2, max_length=100)
    region: Optional[str] = None
    continent: Optional[str] = None
    
    # Product information
    product_category: Optional[str] = None
    product_brand: Optional[str] = None
    
    # Temporal information
    invoice_date: datetime
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = Field(None, ge=1, le=4)
    is_weekend: bool = False
    is_holiday: bool = False
    
    # Transaction metadata
    status: TransactionStatus = TransactionStatus.COMPLETED
    payment_method: Optional[PaymentMethod] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Analytics fields
    revenue_per_unit: Optional[Decimal] = None
    profitability_score: Optional[float] = Field(None, ge=0, le=1)
    customer_value_tier: Optional[str] = Field(None, pattern="^(High|Medium|Low)$")
    
    @field_validator('invoice_no')
    @classmethod
    def validate_invoice_no(cls, v: str) -> str:
        """Validate invoice number format and business rules."""
        if not v or v.isspace():
            raise ValueError('Invoice number cannot be empty')
        
        v = v.upper().strip()
        
        # Business rule: Cancelled invoices start with 'C'
        if v.startswith('C'):
            raise ValueError('Cancelled invoices (starting with C) are not allowed in active transactions')
        
        return v
    
    @field_validator('stock_code')
    @classmethod
    def validate_stock_code(cls, v: str) -> str:
        """Validate stock code format."""
        if not v or v.isspace():
            raise ValueError('Stock code cannot be empty')
        return v.upper().strip()
    
    @field_validator('description')
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        """Clean and validate product description."""
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
    def validate_customer_id(cls, v: Optional[str]) -> Optional[str]:
        """Validate customer ID format."""
        if v:
            v = v.strip()
            if len(v) == 0 or v.upper() in ['NULL', 'UNKNOWN']:
                return None
            
            # Business rule: Customer ID should be numeric in retail context
            try:
                float(v)
                return v
            except ValueError:
                raise ValueError('Customer ID must be numeric')
        return v
    
    @field_validator('country')
    @classmethod
    def validate_country(cls, v: str) -> str:
        """Validate and standardize country names."""
        if not v or v.isspace():
            raise ValueError('Country cannot be empty')
        
        v = v.strip()
        
        # Business rule: Country name standardization
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
    def validate_invoice_date(cls, v: datetime) -> datetime:
        """Validate invoice date business rules."""
        if v > datetime.now():
            raise ValueError('Invoice date cannot be in the future')
        if v.year < 2000:
            raise ValueError('Invoice date seems too old to be valid')
        return v
    
    @field_validator('fiscal_quarter')
    @classmethod
    def validate_fiscal_quarter(cls, v: Optional[int]) -> Optional[int]:
        """Validate fiscal quarter."""
        if v is not None and (v < 1 or v > 4):
            raise ValueError('Fiscal quarter must be between 1 and 4')
        return v
    
    @model_validator(mode='after')
    def validate_business_rules(self) -> 'SaleEntity':
        """Comprehensive business rule validation."""
        
        # Calculate total amount if not provided
        if self.total_amount is None:
            self.total_amount = self.quantity * self.unit_price
        
        # Business rule: Transaction amount limits
        if self.total_amount > Decimal('100000'):  # £100k limit
            raise ValueError('Transaction amount exceeds maximum allowed limit (£100,000)')
        if self.total_amount < Decimal('0.01'):
            raise ValueError('Transaction amount too small to be valid (minimum £0.01)')
        
        # Business rule: Discount validation
        if self.discount_amount > self.total_amount:
            raise ValueError('Discount amount cannot exceed total amount')
        
        # Business rule: Profit margin validation
        if self.profit_amount and self.profit_amount > self.total_amount:
            raise ValueError('Profit amount cannot exceed total amount')
        
        # Calculate revenue per unit
        if self.revenue_per_unit is None and self.quantity > 0:
            self.revenue_per_unit = self.total_amount / self.quantity
        
        # Calculate profitability score
        if self.margin_percentage and self.profitability_score is None:
            self.profitability_score = min(float(self.margin_percentage) / 50.0, 1.0)
        
        # Determine fiscal year and quarter from invoice date
        if self.fiscal_year is None:
            # Assuming fiscal year starts in April
            self.fiscal_year = self.invoice_date.year if self.invoice_date.month >= 4 else self.invoice_date.year - 1
        
        if self.fiscal_quarter is None:
            # Calculate fiscal quarter
            fiscal_month = self.invoice_date.month - 3 if self.invoice_date.month >= 4 else self.invoice_date.month + 9
            self.fiscal_quarter = (fiscal_month - 1) // 3 + 1
        
        # Set weekend flag
        self.is_weekend = self.invoice_date.weekday() >= 5
        
        # Basic holiday detection (could be enhanced)
        self.is_holiday = (self.invoice_date.month == 12 and self.invoice_date.day == 25) or \
                         (self.invoice_date.month == 1 and self.invoice_date.day == 1)
        
        return self
    
    def calculate_derived_metrics(self) -> None:
        """Calculate additional business metrics."""
        
        # Customer value tier determination
        if self.customer_lifetime_value:
            ltv = float(self.customer_lifetime_value)
            if ltv >= 1000:
                self.customer_value_tier = "High"
            elif ltv >= 500:
                self.customer_value_tier = "Medium"
            else:
                self.customer_value_tier = "Low"
        
        # Revenue efficiency
        if self.revenue_per_unit and self.unit_price:
            efficiency = float(self.revenue_per_unit / self.unit_price)
            if efficiency < 1.0:
                raise ValueError("Revenue per unit cannot be less than unit price")
    
    def is_high_value_transaction(self) -> bool:
        """Business rule: Check if this is a high-value transaction."""
        return self.total_amount >= Decimal('1000')
    
    def is_bulk_purchase(self) -> bool:
        """Business rule: Check if this is a bulk purchase."""
        return self.quantity >= 50
    
    def get_transaction_risk_score(self) -> float:
        """Calculate transaction risk score for fraud detection."""
        risk_score = 0.0
        
        # High amount increases risk
        if self.total_amount > Decimal('5000'):
            risk_score += 0.3
        
        # Weekend transactions slightly more risky
        if self.is_weekend:
            risk_score += 0.1
        
        # International transactions
        if self.country != "United Kingdom":
            risk_score += 0.2
        
        # Large quantities
        if self.quantity > 100:
            risk_score += 0.2
        
        return min(risk_score, 1.0)
    
    def to_analytics_summary(self) -> dict:
        """Convert to analytics summary format."""
        return {
            "sale_id": str(self.sale_id) if self.sale_id else None,
            "invoice_no": self.invoice_no,
            "total_amount": float(self.total_amount) if self.total_amount else 0,
            "customer_segment": self.customer_segment,
            "product_category": self.product_category,
            "country": self.country,
            "fiscal_year": self.fiscal_year,
            "fiscal_quarter": self.fiscal_quarter,
            "is_high_value": self.is_high_value_transaction(),
            "is_bulk_purchase": self.is_bulk_purchase(),
            "risk_score": self.get_transaction_risk_score(),
            "profitability_score": self.profitability_score
        }
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }
        validate_assignment = True
        arbitrary_types_allowed = True