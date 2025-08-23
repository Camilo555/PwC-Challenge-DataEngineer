"""
GraphQL Schemas and Types
Defines GraphQL types and schemas for the retail data API.
"""
from datetime import date, datetime

import strawberry


@strawberry.type
class Customer:
    """Customer GraphQL type."""
    customer_key: int
    customer_id: str | None
    customer_segment: str | None
    lifetime_value: float | None
    total_orders: int | None
    total_spent: float | None
    avg_order_value: float | None
    recency_score: int | None
    frequency_score: int | None
    monetary_score: int | None
    rfm_segment: str | None


@strawberry.type
class Product:
    """Product GraphQL type."""
    product_key: int
    stock_code: str
    description: str | None
    category: str | None
    subcategory: str | None
    brand: str | None
    unit_cost: float | None
    recommended_price: float | None


@strawberry.type
class Country:
    """Country GraphQL type."""
    country_key: int
    country_code: str
    country_name: str
    region: str | None
    continent: str | None
    currency_code: str | None
    gdp_per_capita: float | None
    population: int | None


@strawberry.type
class Sale:
    """Sale fact GraphQL type."""
    sale_id: str
    quantity: int
    unit_price: float
    total_amount: float
    discount_amount: float
    tax_amount: float
    profit_amount: float | None
    margin_percentage: float | None
    created_at: datetime

    # Related entities (resolved via data loaders)
    customer: Customer | None
    product: Product
    country: Country


@strawberry.type
class SalesAnalytics:
    """Sales analytics aggregated data."""
    period: str
    total_revenue: float
    total_quantity: int
    transaction_count: int
    unique_customers: int
    avg_order_value: float


@strawberry.type
class CustomerSegment:
    """Customer segment analytics."""
    segment_name: str
    customer_count: int
    avg_lifetime_value: float
    avg_order_value: float
    avg_total_orders: float


@strawberry.type
class ProductPerformance:
    """Product performance metrics."""
    stock_code: str
    description: str | None
    category: str | None
    total_revenue: float
    total_quantity: int
    transaction_count: int


@strawberry.type
class BusinessMetrics:
    """High-level business metrics."""
    total_revenue: float
    total_transactions: int
    unique_customers: int
    avg_order_value: float
    total_products: int
    active_countries: int


@strawberry.type
class TaskStatus:
    """Async task status."""
    task_id: str
    task_name: str
    status: str
    submitted_at: datetime
    progress: int | None
    result: str | None  # JSON string
    error: str | None


# Input types for mutations and complex queries

@strawberry.input
class SalesFilters:
    """Filters for sales queries."""
    date_from: date | None = None
    date_to: date | None = None
    country: str | None = None
    product_category: str | None = None
    customer_segment: str | None = None
    min_amount: float | None = None
    max_amount: float | None = None


@strawberry.input
class PaginationInput:
    """Pagination parameters."""
    page: int = 1
    page_size: int = 20


@strawberry.input
class TaskSubmissionInput:
    """Input for submitting async tasks."""
    task_name: str
    parameters: str  # JSON string


@strawberry.type
class PaginatedSales:
    """Paginated sales results."""
    items: list[Sale]
    total_count: int
    page: int
    page_size: int
    has_next: bool
    has_previous: bool


# Enums

@strawberry.enum
class TimeGranularity:
    """Time granularity options for analytics."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


@strawberry.enum
class MetricType:
    """Metric types for product performance."""
    REVENUE = "revenue"
    QUANTITY = "quantity"
    PROFIT = "profit"
    MARGIN = "margin"


@strawberry.enum
class TaskStatusEnum:
    """Task status enumeration."""
    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    REVOKED = "revoked"
