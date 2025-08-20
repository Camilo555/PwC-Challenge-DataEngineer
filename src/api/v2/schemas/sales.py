"""
Enhanced Sales Schemas for API V2
Includes additional fields and improved validation.
"""
from datetime import datetime, date
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator


class EnhancedSaleItem(BaseModel):
    """Enhanced sale item with additional analytics fields."""
    sale_id: UUID
    invoice_no: str
    stock_code: str
    description: Optional[str] = None
    quantity: int
    unit_price: Decimal
    total_amount: Decimal
    discount_amount: Decimal = Field(default=Decimal('0.00'))
    tax_amount: Decimal = Field(default=Decimal('0.00'))
    profit_amount: Optional[Decimal] = None
    margin_percentage: Optional[Decimal] = None
    
    # Enhanced fields for v2
    customer_id: Optional[str] = None
    customer_segment: Optional[str] = None
    customer_lifetime_value: Optional[Decimal] = None
    country: str
    country_code: str
    region: Optional[str] = None
    continent: Optional[str] = None
    product_category: Optional[str] = None
    product_brand: Optional[str] = None
    
    # Temporal fields
    invoice_date: datetime
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    is_weekend: bool = False
    is_holiday: bool = False
    
    # Derived analytics
    revenue_per_unit: Optional[Decimal] = None
    profitability_score: Optional[float] = None
    customer_value_tier: Optional[str] = None
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            UUID: lambda v: str(v)
        }


class SalesAggregation(BaseModel):
    """Sales aggregation with multiple dimensions."""
    dimension: str  # e.g., 'country', 'category', 'month'
    dimension_value: str
    total_revenue: Decimal
    total_quantity: int
    transaction_count: int
    unique_customers: int
    avg_order_value: Decimal
    profit_margin: Optional[Decimal] = None
    growth_rate: Optional[float] = None  # Compared to previous period
    
    class Config:
        json_encoders = {Decimal: lambda v: float(v)}


class TimeSeriesPoint(BaseModel):
    """Time series data point."""
    period: str  # ISO date or period identifier
    revenue: Decimal
    quantity: int
    transactions: int
    unique_customers: int
    avg_order_value: Decimal
    cumulative_revenue: Optional[Decimal] = None
    period_growth: Optional[float] = None
    
    class Config:
        json_encoders = {Decimal: lambda v: float(v)}


class EnhancedSalesAnalytics(BaseModel):
    """Comprehensive sales analytics response."""
    summary: dict
    time_series: List[TimeSeriesPoint]
    top_products: List[dict]
    top_customers: List[dict]
    geographic_breakdown: List[SalesAggregation]
    category_performance: List[SalesAggregation]
    seasonal_insights: dict
    forecasting: Optional[dict] = None
    
    class Config:
        json_encoders = {Decimal: lambda v: float(v)}


class PaginatedEnhancedSales(BaseModel):
    """Paginated enhanced sales with metadata."""
    items: List[EnhancedSaleItem]
    total: int
    page: int
    size: int
    pages: int
    has_next: bool
    has_previous: bool
    
    # Enhanced metadata
    aggregations: dict
    filters_applied: dict
    response_time_ms: float
    data_freshness: datetime  # When data was last updated
    quality_score: Optional[float] = None  # Data quality indicator


class SalesFiltersV2(BaseModel):
    """Enhanced filters with more options."""
    # Date filters
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    
    # Geographic filters
    countries: Optional[List[str]] = None
    regions: Optional[List[str]] = None
    continents: Optional[List[str]] = None
    
    # Product filters
    categories: Optional[List[str]] = None
    brands: Optional[List[str]] = None
    stock_codes: Optional[List[str]] = None
    
    # Customer filters
    customer_segments: Optional[List[str]] = None
    customer_value_tiers: Optional[List[str]] = None
    min_customer_ltv: Optional[Decimal] = None
    
    # Financial filters
    min_amount: Optional[Decimal] = None
    max_amount: Optional[Decimal] = None
    min_margin: Optional[Decimal] = None
    has_profit_data: Optional[bool] = None
    
    # Business context filters
    exclude_cancelled: bool = True
    exclude_refunds: bool = True
    include_weekends: Optional[bool] = None
    include_holidays: Optional[bool] = None
    
    @validator('date_from', 'date_to')
    def validate_dates(cls, v, values):
        if 'date_from' in values and v and values['date_from']:
            if v < values['date_from']:
                raise ValueError('date_to must be after date_from')
        return v


class SalesExportRequest(BaseModel):
    """Request for exporting sales data."""
    format: str = Field(..., regex="^(csv|excel|parquet|json)$")
    filters: Optional[SalesFiltersV2] = None
    include_analytics: bool = False
    include_forecasting: bool = False
    compression: Optional[str] = Field(None, regex="^(gzip|zip|none)$")
    email_notification: bool = False
    notification_email: Optional[str] = None


class SalesExportResponse(BaseModel):
    """Response for sales export request."""
    export_id: UUID
    status: str
    estimated_completion: datetime
    download_url: Optional[str] = None
    file_size_mb: Optional[float] = None
    record_count: Optional[int] = None
    
    class Config:
        json_encoders = {UUID: lambda v: str(v)}