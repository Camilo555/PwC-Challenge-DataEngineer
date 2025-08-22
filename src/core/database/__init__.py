"""
Database Core Module

Comprehensive database management with connection pooling, indexing, and performance optimization.
"""

from .connection_manager import DatabaseManager, get_database_manager
from .indexes import IndexManager, create_performance_indexes
from .performance import DatabasePerformanceOptimizer
from .pool_config import ConnectionPoolConfig

__all__ = [
    "DatabaseManager",
    "get_database_manager", 
    "IndexManager",
    "create_performance_indexes",
    "DatabasePerformanceOptimizer",
    "ConnectionPoolConfig"
]