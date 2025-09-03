"""
GraphQL API Module
Provides GraphQL interface for the retail data platform.
"""

from .resolvers import schema
from .router import graphql_router
from .schemas import (
    BusinessMetrics,
    Country,
    Customer,
    CustomerSegment,
    PaginatedSales,
    PaginationInput,
    Product,
    ProductPerformance,
    Sale,
    SalesAnalytics,
    SalesFilters,
    TaskStatus,
    TaskSubmissionInput,
)

__all__ = ["graphql_router", "schema", "Customer", "Product", "Country", "Sale", "SalesAnalytics", "CustomerSegment", "ProductPerformance", "BusinessMetrics", "TaskStatus", "SalesFilters", "PaginationInput", "TaskSubmissionInput", "PaginatedSales"]
