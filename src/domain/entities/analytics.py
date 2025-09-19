"""
Analytics Domain Models
=====================

Core analytics entities for reporting and business intelligence.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4

from pydantic import Field, validator

from ..base import DomainEntity

# Import business intelligence entities
from .business_intelligence import (
    BusinessIntelligenceMetrics,
    CrossDomainInsights,
    PredictiveAnalytics,
    PerformanceBenchmark,
    CustomerJourneyStage
)


class MetricType(str, Enum):
    """Types of business metrics."""
    REVENUE = "revenue"
    SALES_COUNT = "sales_count"
    CUSTOMER_COUNT = "customer_count"
    PRODUCT_COUNT = "product_count"
    CONVERSION_RATE = "conversion_rate"
    AVERAGE_ORDER_VALUE = "average_order_value"
    CUSTOMER_LIFETIME_VALUE = "customer_lifetime_value"
    CHURN_RATE = "churn_rate"
    RETENTION_RATE = "retention_rate"


class TimeGranularity(str, Enum):
    """Time granularity for analytics."""
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class AnalyticsMetric(DomainEntity):
    """Core analytics metric entity."""
    
    id: UUID = Field(default_factory=uuid4)
    metric_type: MetricType
    value: Decimal
    dimensions: Dict[str, Any] = Field(default_factory=dict)
    time_period: date
    granularity: TimeGranularity
    calculation_timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Data quality indicators
    data_completeness: float = Field(ge=0.0, le=1.0)
    confidence_score: float = Field(ge=0.0, le=1.0)
    data_sources: List[str] = Field(default_factory=list)
    
    @validator('value')
    def validate_value(cls, v):
        if v < 0 and cls.metric_type not in [MetricType.CHURN_RATE]:
            raise ValueError('Value must be non-negative for this metric type')
        return v


class SalesAnalytics(DomainEntity):
    """Sales analytics aggregated entity."""
    
    id: UUID = Field(default_factory=uuid4)
    period_start: date
    period_end: date
    granularity: TimeGranularity
    
    # Core metrics
    total_revenue: Decimal
    total_orders: int
    total_units_sold: int
    average_order_value: Decimal
    
    # Customer metrics
    unique_customers: int
    new_customers: int
    returning_customers: int
    
    # Product metrics
    unique_products_sold: int
    top_selling_product_id: Optional[UUID]
    
    # Geographic dimensions
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    
    # Calculated fields
    conversion_rate: Optional[float] = Field(ge=0.0, le=1.0)
    customer_acquisition_cost: Optional[Decimal]
    
    @validator('period_end')
    def validate_period(cls, v, values):
        if 'period_start' in values and v <= values['period_start']:
            raise ValueError('period_end must be after period_start')
        return v


class CustomerAnalytics(DomainEntity):
    """Customer analytics entity."""
    
    id: UUID = Field(default_factory=uuid4)
    customer_id: UUID
    analysis_date: date
    
    # Lifetime metrics
    lifetime_value: Decimal
    total_orders: int
    total_spent: Decimal
    first_purchase_date: Optional[date]
    last_purchase_date: Optional[date]
    
    # Behavioral metrics
    average_order_value: Decimal
    purchase_frequency: float  # orders per month
    days_since_last_purchase: Optional[int]
    
    # Segmentation
    customer_segment: str
    value_tier: str
    risk_score: float = Field(ge=0.0, le=1.0)
    churn_probability: float = Field(ge=0.0, le=1.0)
    
    # Engagement metrics
    email_engagement_score: Optional[float] = Field(ge=0.0, le=1.0)
    website_engagement_score: Optional[float] = Field(ge=0.0, le=1.0)
    
    @validator('purchase_frequency')
    def validate_frequency(cls, v):
        if v < 0:
            raise ValueError('Purchase frequency must be non-negative')
        return v


class ProductAnalytics(DomainEntity):
    """Product performance analytics entity."""
    
    id: UUID = Field(default_factory=uuid4)
    product_id: UUID
    analysis_date: date
    
    # Sales metrics
    units_sold: int
    revenue: Decimal
    profit_margin: Optional[float]
    
    # Performance metrics
    conversion_rate: Optional[float] = Field(ge=0.0, le=1.0)
    view_to_purchase_rate: Optional[float] = Field(ge=0.0, le=1.0)
    return_rate: Optional[float] = Field(ge=0.0, le=1.0)
    
    # Inventory metrics
    stock_turnover: Optional[float]
    days_of_inventory: Optional[int]
    
    # Ranking and comparison
    sales_rank: Optional[int]
    category_rank: Optional[int]
    
    # Geographic performance
    top_selling_regions: List[str] = Field(default_factory=list)
    
    @validator('units_sold')
    def validate_units_sold(cls, v):
        if v < 0:
            raise ValueError('Units sold must be non-negative')
        return v


class KPIDashboard(DomainEntity):
    """KPI dashboard configuration entity."""
    
    id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = Field(max_length=500)
    
    # Dashboard configuration
    metrics: List[MetricType]
    time_range_days: int = Field(gt=0)
    refresh_interval_minutes: int = Field(gt=0, default=15)
    
    # Access control
    owner_id: UUID
    shared_with: List[UUID] = Field(default_factory=list)
    is_public: bool = False
    
    # Display settings
    chart_types: Dict[str, str] = Field(default_factory=dict)
    filters: Dict[str, Any] = Field(default_factory=dict)
    
    # Status
    is_active: bool = True
    last_accessed: Optional[datetime]
    
    @validator('name')
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError('Dashboard name cannot be empty')
        return v.strip()