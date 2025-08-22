"""
API Middleware Package
Provides request/response middleware for correlation tracking and observability.
"""

from .correlation import (
    CorrelationMiddleware,
    get_correlation_context,
    extract_correlation_headers,
    add_correlation_headers
)

__all__ = [
    "CorrelationMiddleware",
    "get_correlation_context", 
    "extract_correlation_headers",
    "add_correlation_headers"
]