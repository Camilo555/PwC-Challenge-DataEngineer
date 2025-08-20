"""
GraphQL Schemas and Types
Defines GraphQL types and schemas for the retail data API.
"""
from datetime import datetime, date
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

import strawberry
from strawberry.types import Info


@strawberry.type
class Customer:
    """Customer GraphQL type."""
    customer_key: int
    customer_id: Optional[str]
    customer_segment: Optional[str]
    lifetime_value: Optional[float]
    total_orders: Optional[int]
    total_spent: Optional[float]
    avg_order_value: Optional[float]
    recency_score: Optional[int]
    frequency_score: Optional[int]
    monetary_score: Optional[int]
    rfm_segment: Optional[str]


@strawberry.type
class Product:
    """Product GraphQL type."""
    product_key: int
    stock_code: str
    description: Optional[str]
    category: Optional[str]
    subcategory: Optional[str]
    brand: Optional[str]
    unit_cost: Optional[float]
    recommended_price: Optional[float]


@strawberry.type
class Country:
    """Country GraphQL type."""
    country_key: int
    country_code: str
    country_name: str
    region: Optional[str]
    continent: Optional[str]
    currency_code: Optional[str]
    gdp_per_capita: Optional[float]
    population: Optional[int]


@strawberry.type
class Sale:
    """Sale fact GraphQL type."""
    sale_id: str
    quantity: int
    unit_price: float
    total_amount: float
    discount_amount: float
    tax_amount: float
    profit_amount: Optional[float]
    margin_percentage: Optional[float]
    created_at: datetime
    
    # Related entities (resolved via data loaders)
    customer: Optional[Customer]
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
    description: Optional[str]
    category: Optional[str]
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
    progress: Optional[int]
    result: Optional[str]  # JSON string
    error: Optional[str]


# Input types for mutations and complex queries

@strawberry.input
class SalesFilters:
    """Filters for sales queries."""
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    country: Optional[str] = None
    product_category: Optional[str] = None
    customer_segment: Optional[str] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None


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
    items: List[Sale]
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