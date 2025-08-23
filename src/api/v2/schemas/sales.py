"""
Enhanced Sales Schemas for API V2
Includes additional fields and improved validation.
"""
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field, validator


class EnhancedSaleItem(BaseModel):
    """Enhanced sale item with additional analytics fields."""
    sale_id: UUID
    invoice_no: str
    stock_code: str
    description: str | None = None
    quantity: int
    unit_price: Decimal
    total_amount: Decimal
    discount_amount: Decimal = Field(default=Decimal('0.00'))
    tax_amount: Decimal = Field(default=Decimal('0.00'))
    profit_amount: Decimal | None = None
    margin_percentage: Decimal | None = None

    # Enhanced fields for v2
    customer_id: str | None = None
    customer_segment: str | None = None
    customer_lifetime_value: Decimal | None = None
    country: str
    country_code: str
    region: str | None = None
    continent: str | None = None
    product_category: str | None = None
    product_brand: str | None = None

    # Temporal fields
    invoice_date: datetime
    fiscal_year: int | None = None
    fiscal_quarter: int | None = None
    is_weekend: bool = False
    is_holiday: bool = False

    # Derived analytics
    revenue_per_unit: Decimal | None = None
    profitability_score: float | None = None
    customer_value_tier: str | None = None

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
    profit_margin: Decimal | None = None
    growth_rate: float | None = None  # Compared to previous period

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
    cumulative_revenue: Decimal | None = None
    period_growth: float | None = None

    class Config:
        json_encoders = {Decimal: lambda v: float(v)}


class EnhancedSalesAnalytics(BaseModel):
    """Comprehensive sales analytics response."""
    summary: dict
    time_series: list[TimeSeriesPoint]
    top_products: list[dict]
    top_customers: list[dict]
    geographic_breakdown: list[SalesAggregation]
    category_performance: list[SalesAggregation]
    seasonal_insights: dict
    forecasting: dict | None = None

    class Config:
        json_encoders = {Decimal: lambda v: float(v)}


class PaginatedEnhancedSales(BaseModel):
    """Paginated enhanced sales with metadata."""
    items: list[EnhancedSaleItem]
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
    quality_score: float | None = None  # Data quality indicator


class SalesFiltersV2(BaseModel):
    """Enhanced filters with more options."""
    # Date filters
    date_from: date | None = None
    date_to: date | None = None
    fiscal_year: int | None = None
    fiscal_quarter: int | None = None

    # Geographic filters
    countries: list[str] | None = None
    regions: list[str] | None = None
    continents: list[str] | None = None

    # Product filters
    categories: list[str] | None = None
    brands: list[str] | None = None
    stock_codes: list[str] | None = None

    # Customer filters
    customer_segments: list[str] | None = None
    customer_value_tiers: list[str] | None = None
    min_customer_ltv: Decimal | None = None

    # Financial filters
    min_amount: Decimal | None = None
    max_amount: Decimal | None = None
    min_margin: Decimal | None = None
    has_profit_data: bool | None = None

    # Business context filters
    exclude_cancelled: bool = True
    exclude_refunds: bool = True
    include_weekends: bool | None = None
    include_holidays: bool | None = None

    @validator('date_from', 'date_to')
    def validate_dates(cls, v, values):
        if 'date_from' in values and v and values['date_from']:
            if v < values['date_from']:
                raise ValueError('date_to must be after date_from')
        return v


class SalesExportRequest(BaseModel):
    """Request for exporting sales data."""
    format: str = Field(..., regex="^(csv|excel|parquet|json)$")
    filters: SalesFiltersV2 | None = None
    include_analytics: bool = False
    include_forecasting: bool = False
    compression: str | None = Field(None, regex="^(gzip|zip|none)$")
    email_notification: bool = False
    notification_email: str | None = None


class SalesExportResponse(BaseModel):
    """Response for sales export request."""
    export_id: UUID
    status: str
    estimated_completion: datetime
    download_url: str | None = None
    file_size_mb: float | None = None
    record_count: int | None = None

    class Config:
        json_encoders = {UUID: lambda v: str(v)}
