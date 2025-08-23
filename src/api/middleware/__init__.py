"""
API Middleware Package
Provides request/response middleware for correlation tracking and observability.
"""

from .correlation import (
    CorrelationMiddleware,
    add_correlation_headers,
    extract_correlation_headers,
    get_correlation_context,
)

__all__ = [
    "CorrelationMiddleware",
    "get_correlation_context",
    "extract_correlation_headers",
    "add_correlation_headers"
]
