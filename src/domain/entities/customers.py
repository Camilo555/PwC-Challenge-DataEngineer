"""
Customer Domain Entity
=====================

Comprehensive customer domain model with advanced segmentation,
RFM analysis, lifetime value calculation, and behavioral analytics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator, root_validator, EmailStr, model_validator
from pydantic.types import PositiveInt, PositiveFloat, NonNegativeFloat, constr


class CustomerStatus(str, Enum):
    """Customer account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    CHURNED = "churned"
    PROSPECT = "prospect"
    VIP = "vip"


class CustomerSegment(str, Enum):
    """Customer segmentation categories."""
    CHAMPIONS = "champions"
    LOYAL_CUSTOMERS = "loyal_customers"
    POTENTIAL_LOYALISTS = "potential_loyalists"
    NEW_CUSTOMERS = "new_customers"
    PROMISING = "promising"
    NEED_ATTENTION = "need_attention"
    ABOUT_TO_SLEEP = "about_to_sleep"
    AT_RISK = "at_risk"
    CANNOT_LOSE_THEM = "cannot_lose_them"
    HIBERNATING = "hibernating"
    LOST = "lost"


class CustomerType(str, Enum):
    """Customer types."""
    INDIVIDUAL = "individual"
    BUSINESS = "business"
    ENTERPRISE = "enterprise"
    NON_PROFIT = "non_profit"
    GOVERNMENT = "government"


class CommunicationPreference(str, Enum):
    """Communication preferences."""
    EMAIL = "email"
    SMS = "sms"
    PHONE = "phone"
    MAIL = "mail"
    IN_APP = "in_app"
    PUSH_NOTIFICATION = "push_notification"


class RiskLevel(str, Enum):
    """Customer risk levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class CustomerAddress:
    """Customer address information."""
    address_type: str  # billing, shipping, primary, etc.
    street_address: str
    street_address_2: Optional[str] = None
    city: str
    state_province: str
    postal_code: str
    country: str
    is_primary: bool = False
    is_verified: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class CustomerContactInfo:
    """Customer contact information."""
    contact_type: str  # primary, secondary, emergency, etc.
    email: Optional[str] = None
    phone: Optional[str] = None
    mobile: Optional[str] = None
    is_primary: bool = False
    is_verified: bool = False
    opt_in_marketing: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)


class RFMScores(BaseModel):
    """RFM (Recency, Frequency, Monetary) analysis scores."""
    
    recency_score: int = Field(..., ge=1, le=5)
    frequency_score: int = Field(..., ge=1, le=5) 
    monetary_score: int = Field(..., ge=1, le=5)
    
    # Raw values
    days_since_last_purchase: int = Field(..., ge=0)
    total_purchases: int = Field(..., ge=0)
    total_spent: NonNegativeFloat = 0.0
    
    # Calculated metrics
    rfm_score: str = Field(..., pattern=r'^[1-5][1-5][1-5]$')
    rfm_segment: CustomerSegment = CustomerSegment.NEW_CUSTOMERS
    
    def __init__(self, **data):
        super().__init__(**data)
        if not hasattr(self, 'rfm_score') or not self.rfm_score:
            self.rfm_score = f"{self.recency_score}{self.frequency_score}{self.monetary_score}"
        
        # Determine RFM segment based on scores
        self.rfm_segment = self._calculate_rfm_segment()
    
    def _calculate_rfm_segment(self) -> CustomerSegment:
        """Calculate RFM segment based on scores."""
        r, f, m = self.recency_score, self.frequency_score, self.monetary_score
        
        # Champions: Bought recently, buy often and spend the most
        if r >= 4 and f >= 4 and m >= 4:
            return CustomerSegment.CHAMPIONS
        
        # Loyal Customers: Spend good money with us often. Responsive to promotions
        if r >= 3 and f >= 3 and m >= 3:
            return CustomerSegment.LOYAL_CUSTOMERS
        
        # Potential Loyalists: Recent customers, but spent a good amount and bought more than once
        if r >= 4 and f <= 2 and m >= 3:
            return CustomerSegment.POTENTIAL_LOYALISTS
        
        # New Customers: Bought most recently, but not often
        if r >= 4 and f <= 2 and m <= 2:
            return CustomerSegment.NEW_CUSTOMERS
        
        # Promising: Recent shoppers, but haven't spent much
        if r >= 3 and f <= 2 and m <= 2:
            return CustomerSegment.PROMISING
        
        # Need Attention: Above average recency, frequency and monetary values
        if r >= 3 and f >= 3 and m <= 2:
            return CustomerSegment.NEED_ATTENTION
        
        # About to Sleep: Below average recency, frequency and monetary values
        if r <= 2 and f <= 3 and m <= 3:
            return CustomerSegment.ABOUT_TO_SLEEP
        
        # At Risk: Spent big money and purchased often. But long time ago
        if r <= 2 and f >= 3 and m >= 3:
            return CustomerSegment.AT_RISK
        
        # Cannot Lose Them: Made biggest purchases, and often. But haven't returned for a long time
        if r <= 2 and f <= 2 and m >= 4:
            return CustomerSegment.CANNOT_LOSE_THEM
        
        # Hibernating: Last purchase was long back, low spenders and low number of orders
        if r <= 2 and f <= 2 and m <= 2:
            return CustomerSegment.HIBERNATING
        
        # Lost: Lowest recency, frequency and monetary scores
        if r == 1 and f == 1 and m == 1:
            return CustomerSegment.LOST
        
        return CustomerSegment.NEW_CUSTOMERS


class CustomerLifetimeValue(BaseModel):
    """Customer Lifetime Value (CLV) calculations."""
    
    # Core CLV metrics
    historical_clv: NonNegativeFloat = 0.0  # Total spent to date
    predicted_clv: Optional[float] = None     # ML-predicted future value
    clv_percentile: Optional[int] = Field(None, ge=1, le=100)
    
    # Component metrics
    average_order_value: NonNegativeFloat = 0.0
    purchase_frequency: NonNegativeFloat = 0.0  # purchases per period
    customer_lifespan_days: int = Field(default=0, ge=0)
    
    # Advanced metrics
    gross_margin_per_order: Optional[float] = None
    acquisition_cost: NonNegativeFloat = 0.0
    retention_cost: NonNegativeFloat = 0.0
    
    # Profitability
    net_present_value: Optional[float] = None
    profit_margin_percentage: Optional[float] = None
    is_profitable: bool = True
    
    # Predictions
    churn_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    next_purchase_prediction_days: Optional[int] = None
    upsell_potential_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    
    @validator('clv_percentile')
    def validate_percentile(cls, v):
        if v is not None and (v < 1 or v > 100):
            raise ValueError('CLV percentile must be between 1 and 100')
        return v


class CustomerBehavior(BaseModel):
    """Customer behavioral analytics."""
    
    # Purchase patterns
    preferred_categories: List[str] = Field(default_factory=list)
    preferred_brands: List[str] = Field(default_factory=list) 
    preferred_channels: List[str] = Field(default_factory=list)
    preferred_payment_methods: List[str] = Field(default_factory=list)
    
    # Timing patterns
    peak_shopping_hours: List[int] = Field(default_factory=list)  # 0-23
    peak_shopping_days: List[int] = Field(default_factory=list)   # 0-6 (Monday=0)
    seasonal_patterns: Dict[str, float] = Field(default_factory=dict)  # season -> preference score
    
    # Behavioral scores (0-1 scale)
    price_sensitivity_score: float = Field(default=0.5, ge=0.0, le=1.0)
    brand_loyalty_score: float = Field(default=0.5, ge=0.0, le=1.0)
    promotion_responsiveness: float = Field(default=0.5, ge=0.0, le=1.0)
    innovation_adoption_score: float = Field(default=0.5, ge=0.0, le=1.0)
    
    # Engagement metrics
    email_open_rate: Optional[float] = Field(None, ge=0.0, le=1.0)
    email_click_rate: Optional[float] = Field(None, ge=0.0, le=1.0)
    website_session_duration_avg: Optional[float] = None  # minutes
    pages_per_session_avg: Optional[float] = None
    
    # Social and referral
    referral_count: int = Field(default=0, ge=0)
    social_influence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    review_sentiment_avg: Optional[float] = Field(None, ge=-1.0, le=1.0)
    
    # Risk indicators
    return_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    complaint_count: int = Field(default=0, ge=0)
    fraud_incidents: int = Field(default=0, ge=0)


class CustomerPreferences(BaseModel):
    """Customer preferences and settings."""
    
    # Communication preferences
    communication_channels: List[CommunicationPreference] = Field(default_factory=list)
    marketing_opt_in: bool = True
    email_frequency: str = Field(default="weekly")  # daily, weekly, monthly, none
    
    # Product preferences
    favorite_categories: List[str] = Field(default_factory=list)
    favorite_brands: List[str] = Field(default_factory=list)
    price_range_preference: Optional[Tuple[float, float]] = None
    
    # Service preferences
    preferred_shipping_method: Optional[str] = None
    preferred_payment_method: Optional[str] = None
    preferred_store_location: Optional[str] = None
    
    # Privacy and data preferences
    data_sharing_consent: bool = False
    analytics_tracking_consent: bool = False
    personalization_consent: bool = True
    
    # Accessibility and special needs
    accessibility_requirements: List[str] = Field(default_factory=list)
    language_preference: str = Field(default="en-US")
    timezone: str = Field(default="UTC")


class CustomerProfile(BaseModel):
    """
    Comprehensive customer profile with advanced analytics and segmentation.
    
    This model represents a complete customer entity with all business intelligence,
    behavioral analytics, and predictive capabilities for enterprise CRM systems.
    """
    
    # Core identifiers
    customer_id: UUID = Field(default_factory=uuid4)
    external_id: Optional[str] = None  # External system ID
    customer_number: Optional[str] = None  # Human-readable customer number
    
    # Personal information
    title: Optional[str] = None
    first_name: str = Field(..., min_length=1, max_length=100)
    middle_name: Optional[str] = Field(None, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    display_name: Optional[str] = None
    preferred_name: Optional[str] = None
    
    # Contact information
    email: EmailStr
    phone: Optional[constr(min_length=10, max_length=20)] = None
    mobile: Optional[constr(min_length=10, max_length=20)] = None
    
    # Demographics
    date_of_birth: Optional[date] = None
    gender: Optional[str] = Field(None, pattern=r'^(M|F|O|N)$')  # Male, Female, Other, Not specified
    language: str = Field(default="en")
    country: Optional[str] = None
    timezone: str = Field(default="UTC")
    
    # Account information
    status: CustomerStatus = CustomerStatus.PROSPECT
    customer_type: CustomerType = CustomerType.INDIVIDUAL
    registration_date: datetime = Field(default_factory=datetime.utcnow)
    first_purchase_date: Optional[datetime] = None
    last_purchase_date: Optional[datetime] = None
    last_activity_date: Optional[datetime] = None
    
    # Addresses and contact details
    addresses: List[CustomerAddress] = Field(default_factory=list)
    contact_info: List[CustomerContactInfo] = Field(default_factory=list)
    
    # Business customer specific
    company_name: Optional[str] = None
    job_title: Optional[str] = None
    industry: Optional[str] = None
    company_size: Optional[str] = None
    annual_revenue: Optional[float] = None
    
    # Analytics and intelligence
    rfm_analysis: Optional[RFMScores] = None
    lifetime_value: Optional[CustomerLifetimeValue] = None
    behavior: Optional[CustomerBehavior] = None
    preferences: Optional[CustomerPreferences] = None
    
    # Transaction summary
    total_orders: int = Field(default=0, ge=0)
    total_spent: NonNegativeFloat = 0.0
    average_order_value: NonNegativeFloat = 0.0
    last_order_value: NonNegativeFloat = 0.0
    
    # Engagement metrics
    loyalty_points: int = Field(default=0, ge=0)
    tier_level: Optional[str] = None  # Bronze, Silver, Gold, Platinum, Diamond
    membership_status: Optional[str] = None
    
    # Risk and compliance
    risk_level: RiskLevel = RiskLevel.LOW
    risk_factors: List[str] = Field(default_factory=list)
    credit_limit: Optional[float] = None
    payment_terms: Optional[str] = None
    
    # Data quality and audit
    data_quality_score: float = Field(default=1.0, ge=0.0, le=1.0)
    data_source: str = Field(default="manual", max_length=50)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    # Metadata
    tags: List[str] = Field(default_factory=list)
    notes: Optional[str] = None
    custom_fields: Dict[str, Any] = Field(default_factory=dict)
    external_references: Dict[str, str] = Field(default_factory=dict)
    
    @validator('email')
    def validate_email(cls, v):
        """Validate email format."""
        if not v or '@' not in v:
            raise ValueError('Invalid email format')
        return v.lower().strip()
    
    @validator('display_name', pre=True, always=True)
    def set_display_name(cls, v, values):
        """Set display name if not provided."""
        if not v:
            first_name = values.get('first_name', '')
            last_name = values.get('last_name', '')
            return f"{first_name} {last_name}".strip()
        return v
    
    @validator('customer_number', pre=True, always=True)
    def generate_customer_number(cls, v, values):
        """Generate customer number if not provided."""
        if not v:
            # Simple customer number generation
            timestamp = int(datetime.utcnow().timestamp())
            return f"CUST-{timestamp}"
        return v
    
    @model_validator(mode="before")
    @classmethod
    def validate_business_fields(cls, values):
        """Validate business customer fields."""
        customer_type = values.get('customer_type')
        company_name = values.get('company_name')
        
        if customer_type in [CustomerType.BUSINESS, CustomerType.ENTERPRISE] and not company_name:
            raise ValueError('Company name is required for business customers')
        
        return values
    
    def calculate_age(self) -> Optional[int]:
        """Calculate customer age from date of birth."""
        if not self.date_of_birth:
            return None
        
        today = date.today()
        return today.year - self.date_of_birth.year - (
            (today.month, today.day) < (self.date_of_birth.month, self.date_of_birth.day)
        )
    
    def days_since_last_purchase(self) -> Optional[int]:
        """Calculate days since last purchase."""
        if not self.last_purchase_date:
            return None
        
        return (datetime.utcnow() - self.last_purchase_date).days
    
    def customer_lifetime_days(self) -> int:
        """Calculate customer lifetime in days."""
        if not self.first_purchase_date:
            return (datetime.utcnow() - self.registration_date).days
        
        return (datetime.utcnow() - self.first_purchase_date).days
    
    def is_active_customer(self, days_threshold: int = 90) -> bool:
        """Check if customer is active based on recent activity."""
        if self.status in [CustomerStatus.SUSPENDED, CustomerStatus.CHURNED]:
            return False
        
        days_since_last = self.days_since_last_purchase()
        if days_since_last is None:
            return False
        
        return days_since_last <= days_threshold
    
    def get_primary_address(self) -> Optional[CustomerAddress]:
        """Get primary address."""
        for address in self.addresses:
            if address.is_primary:
                return address
        return self.addresses[0] if self.addresses else None
    
    def get_primary_contact(self) -> Optional[CustomerContactInfo]:
        """Get primary contact information."""
        for contact in self.contact_info:
            if contact.is_primary:
                return contact
        return self.contact_info[0] if self.contact_info else None
    
    def update_transaction_summary(self, order_count: int, total_spent: float, 
                                  avg_order_value: float, last_order_value: float,
                                  last_purchase_date: datetime):
        """Update transaction summary metrics."""
        self.total_orders = order_count
        self.total_spent = total_spent
        self.average_order_value = avg_order_value
        self.last_order_value = last_order_value
        self.last_purchase_date = last_purchase_date
        
        # Update first purchase date if this is the first order
        if order_count == 1 and not self.first_purchase_date:
            self.first_purchase_date = last_purchase_date
        
        # Update status based on purchase activity
        if order_count > 0 and self.status == CustomerStatus.PROSPECT:
            self.status = CustomerStatus.ACTIVE
        
        self.updated_at = datetime.utcnow()
    
    def calculate_rfm_scores(self, all_customer_data: List[Dict[str, Any]]) -> RFMScores:
        """Calculate RFM scores relative to all customers."""
        days_since_last = self.days_since_last_purchase() or 999
        
        # Extract values for percentile calculations
        all_recency = [c.get('days_since_last_purchase', 999) for c in all_customer_data]
        all_frequency = [c.get('total_orders', 0) for c in all_customer_data]
        all_monetary = [c.get('total_spent', 0) for c in all_customer_data]
        
        # Calculate percentiles (inverse for recency - lower days = better)
        recency_percentile = sum(1 for x in all_recency if x >= days_since_last) / len(all_recency)
        frequency_percentile = sum(1 for x in all_frequency if x <= self.total_orders) / len(all_frequency)
        monetary_percentile = sum(1 for x in all_monetary if x <= self.total_spent) / len(all_monetary)
        
        # Convert to 1-5 scale
        recency_score = min(5, max(1, int(recency_percentile * 5) + 1))
        frequency_score = min(5, max(1, int(frequency_percentile * 5) + 1))
        monetary_score = min(5, max(1, int(monetary_percentile * 5) + 1))
        
        rfm_scores = RFMScores(
            recency_score=recency_score,
            frequency_score=frequency_score,
            monetary_score=monetary_score,
            days_since_last_purchase=days_since_last,
            total_purchases=self.total_orders,
            total_spent=float(self.total_spent)
        )
        
        self.rfm_analysis = rfm_scores
        return rfm_scores
    
    def calculate_lifetime_value(self, discount_rate: float = 0.1, 
                                predicted_lifespan_months: int = 36) -> CustomerLifetimeValue:
        """Calculate comprehensive customer lifetime value."""
        clv = CustomerLifetimeValue()
        
        # Historical CLV is simple - total spent to date
        clv.historical_clv = float(self.total_spent)
        
        # Calculate component metrics
        if self.total_orders > 0:
            clv.average_order_value = float(self.total_spent / self.total_orders)
            
            # Calculate purchase frequency (orders per month)
            lifetime_days = self.customer_lifetime_days()
            if lifetime_days > 0:
                clv.purchase_frequency = self.total_orders / (lifetime_days / 30.0)
                clv.customer_lifespan_days = lifetime_days
        
        # Simple predicted CLV calculation
        if clv.average_order_value > 0 and clv.purchase_frequency > 0:
            monthly_value = clv.average_order_value * clv.purchase_frequency
            
            # NPV calculation with discount rate
            predicted_value = 0
            for month in range(1, predicted_lifespan_months + 1):
                discounted_value = monthly_value / ((1 + discount_rate / 12) ** month)
                predicted_value += discounted_value
            
            clv.predicted_clv = predicted_value
        
        # Simple churn probability based on recency
        days_since_last = self.days_since_last_purchase()
        if days_since_last is not None:
            # Simple model - higher churn with more days
            clv.churn_probability = min(1.0, days_since_last / 365.0)
        
        # Profitability check
        clv.is_profitable = clv.historical_clv > clv.acquisition_cost
        
        self.lifetime_value = clv
        return clv
    
    def update_tier_level(self):
        """Update customer tier based on total spent."""
        if self.total_spent >= 10000:
            self.tier_level = "Diamond"
        elif self.total_spent >= 5000:
            self.tier_level = "Platinum"
        elif self.total_spent >= 2000:
            self.tier_level = "Gold"
        elif self.total_spent >= 500:
            self.tier_level = "Silver"
        else:
            self.tier_level = "Bronze"
    
    def assess_risk_level(self):
        """Assess customer risk level based on various factors."""
        risk_score = 0
        risk_factors = []
        
        # High value customer with payment issues
        if self.total_spent > 10000 and 'payment_issues' in self.risk_factors:
            risk_score += 30
            risk_factors.append("high_value_payment_risk")
        
        # Dormant high-value customer
        days_since_last = self.days_since_last_purchase()
        if days_since_last and days_since_last > 180 and self.total_spent > 1000:
            risk_score += 25
            risk_factors.append("dormant_valuable_customer")
        
        # New customer with high initial spend (fraud indicator)
        if self.total_orders <= 2 and self.total_spent > 5000:
            risk_score += 20
            risk_factors.append("high_initial_spend")
        
        # Multiple addresses or contact info (complexity risk)
        if len(self.addresses) > 3 or len(self.contact_info) > 2:
            risk_score += 10
            risk_factors.append("multiple_addresses_contacts")
        
        # Update risk level
        if risk_score >= 50:
            self.risk_level = RiskLevel.CRITICAL
        elif risk_score >= 30:
            self.risk_level = RiskLevel.HIGH
        elif risk_score >= 15:
            self.risk_level = RiskLevel.MEDIUM
        else:
            self.risk_level = RiskLevel.LOW
        
        self.risk_factors = risk_factors
    
    def to_analytics_dict(self) -> Dict[str, Any]:
        """Convert to dictionary optimized for analytics."""
        return {
            'customer_id': str(self.customer_id),
            'customer_number': self.customer_number,
            'display_name': self.display_name,
            'email': self.email,
            
            # Demographics
            'age': self.calculate_age(),
            'gender': self.gender,
            'country': self.country,
            'customer_type': self.customer_type.value,
            
            # Status and dates
            'status': self.status.value,
            'registration_date': self.registration_date.isoformat(),
            'first_purchase_date': self.first_purchase_date.isoformat() if self.first_purchase_date else None,
            'last_purchase_date': self.last_purchase_date.isoformat() if self.last_purchase_date else None,
            'days_since_last_purchase': self.days_since_last_purchase(),
            'customer_lifetime_days': self.customer_lifetime_days(),
            
            # Transaction metrics
            'total_orders': self.total_orders,
            'total_spent': float(self.total_spent),
            'average_order_value': float(self.average_order_value),
            'last_order_value': float(self.last_order_value),
            
            # Tier and loyalty
            'tier_level': self.tier_level,
            'loyalty_points': self.loyalty_points,
            
            # RFM Analysis
            'rfm_recency_score': self.rfm_analysis.recency_score if self.rfm_analysis else None,
            'rfm_frequency_score': self.rfm_analysis.frequency_score if self.rfm_analysis else None,
            'rfm_monetary_score': self.rfm_analysis.monetary_score if self.rfm_analysis else None,
            'rfm_segment': self.rfm_analysis.rfm_segment.value if self.rfm_analysis else None,
            
            # Lifetime Value
            'historical_clv': self.lifetime_value.historical_clv if self.lifetime_value else 0,
            'predicted_clv': self.lifetime_value.predicted_clv if self.lifetime_value else None,
            'churn_probability': self.lifetime_value.churn_probability if self.lifetime_value else 0,
            
            # Risk
            'risk_level': self.risk_level.value,
            'risk_factors': self.risk_factors,
            
            # Flags
            'is_active': self.is_active_customer(),
            'is_business_customer': self.customer_type in [CustomerType.BUSINESS, CustomerType.ENTERPRISE],
            'has_multiple_addresses': len(self.addresses) > 1,
            
            # Data quality
            'data_quality_score': self.data_quality_score,
            'data_source': self.data_source
        }
    
    def validate_business_rules(self) -> List[str]:
        """Validate business rules and return violations."""
        violations = []
        
        # Required fields
        if not self.first_name.strip():
            violations.append("First name is required")
        
        if not self.last_name.strip():
            violations.append("Last name is required")
        
        if not self.email:
            violations.append("Email is required")
        
        # Business customer validation
        if (self.customer_type in [CustomerType.BUSINESS, CustomerType.ENTERPRISE] 
            and not self.company_name):
            violations.append("Company name is required for business customers")
        
        # Age validation
        age = self.calculate_age()
        if age is not None and age < 13:
            violations.append("Customers must be at least 13 years old")
        
        # Contact information validation
        if not self.phone and not self.mobile:
            violations.append("At least one phone number is required")
        
        # Address validation
        if not self.addresses:
            violations.append("At least one address is required")
        
        # Data consistency
        if self.first_purchase_date and self.registration_date:
            if self.first_purchase_date < self.registration_date:
                violations.append("First purchase date cannot be before registration date")
        
        if self.last_purchase_date and self.first_purchase_date:
            if self.last_purchase_date < self.first_purchase_date:
                violations.append("Last purchase date cannot be before first purchase date")
        
        return violations
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        validate_assignment = True
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            Decimal: lambda v: float(v),
            UUID: lambda v: str(v)
        }


# Factory functions

def create_customer_profile(
    first_name: str,
    last_name: str,
    email: str,
    **kwargs
) -> CustomerProfile:
    """Factory function to create a customer profile."""
    
    customer_data = {
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        **kwargs
    }
    
    customer = CustomerProfile(**customer_data)
    customer.update_tier_level()
    customer.assess_risk_level()
    
    return customer


def create_sample_customer() -> CustomerProfile:
    """Create a sample customer for testing."""
    
    # Create addresses
    addresses = [
        CustomerAddress(
            address_type="billing",
            street_address="123 Main St",
            city="New York",
            state_province="NY",
            postal_code="10001",
            country="US",
            is_primary=True,
            is_verified=True
        ),
        CustomerAddress(
            address_type="shipping",
            street_address="456 Oak Ave",
            city="Brooklyn",
            state_province="NY", 
            postal_code="11201",
            country="US",
            is_verified=True
        )
    ]
    
    # Create contact info
    contacts = [
        CustomerContactInfo(
            contact_type="primary",
            email="john.doe@example.com",
            phone="+1-555-123-4567",
            mobile="+1-555-987-6543",
            is_primary=True,
            is_verified=True,
            opt_in_marketing=True
        )
    ]
    
    # Create preferences
    preferences = CustomerPreferences(
        communication_channels=[CommunicationPreference.EMAIL, CommunicationPreference.SMS],
        marketing_opt_in=True,
        email_frequency="weekly",
        favorite_categories=["Electronics", "Books"],
        preferred_payment_method="credit_card",
        data_sharing_consent=True,
        personalization_consent=True
    )
    
    return create_customer_profile(
        first_name="John",
        last_name="Doe",
        email="john.doe@example.com",
        phone="+1-555-123-4567",
        mobile="+1-555-987-6543",
        date_of_birth=date(1985, 6, 15),
        gender="M",
        country="US",
        status=CustomerStatus.ACTIVE,
        customer_type=CustomerType.INDIVIDUAL,
        addresses=addresses,
        contact_info=contacts,
        preferences=preferences,
        total_orders=15,
        total_spent=2500.75,
        average_order_value=166.72,
        last_order_value=89.99,
        tier_level="Gold",
        loyalty_points=1250
    )