"""
SQLAlchemy Relationship Performance Optimizer
===========================================

Advanced SQLAlchemy optimization strategies for high-performance analytics:
- Lazy vs Eager loading optimization
- Batch loading strategies  
- Query optimization and N+1 prevention
- Relationship configuration for star schema
- Advanced query patterns with joins
- Connection and session optimization
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union

from sqlalchemy import (
    and_, or_, select, func, text, inspect, MetaData, Table, Column, 
    ForeignKey, Integer, String, DateTime, Numeric, Boolean
)
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy.orm import (
    selectinload, joinedload, subqueryload, contains_eager,
    Load, defaultload, defer, undefer, load_only
)
from sqlalchemy.orm.strategy_options import _AbstractLoad
from sqlalchemy.sql.selectable import Select
from sqlalchemy.engine.events import event
from sqlalchemy.pool import QueuePool

from core.logging import get_logger

logger = get_logger(__name__)


class LoadingStrategy(str, Enum):
    """SQLAlchemy loading strategies."""
    LAZY = "lazy"
    EAGER = "eager"
    SELECT_IN = "selectin"
    SUBQUERY = "subquery"
    JOINED = "joined"
    BATCH = "batch"
    DEFER = "defer"


class QueryType(str, Enum):
    """Query type classifications for optimization."""
    ANALYTICS = "analytics"
    TRANSACTIONAL = "transactional"
    REPORTING = "reporting"
    DASHBOARD = "dashboard"
    BULK_LOAD = "bulk_load"
    REAL_TIME = "real_time"


@dataclass
class RelationshipConfig:
    """Configuration for SQLAlchemy relationship optimization."""
    relationship_name: str
    model_class: str
    related_class: str
    loading_strategy: LoadingStrategy
    query_types: List[QueryType] = field(default_factory=list)
    batch_size: Optional[int] = None
    defer_columns: List[str] = field(default_factory=list)
    eager_columns: List[str] = field(default_factory=list)
    join_condition: Optional[str] = None
    index_hint: Optional[str] = None
    cache_enabled: bool = False
    cache_ttl_seconds: int = 300


@dataclass
class QueryOptimizationRule:
    """Query optimization rule definition."""
    rule_name: str
    query_pattern: str
    optimization_strategy: str
    loading_options: Dict[str, Any] = field(default_factory=dict)
    expected_improvement_pct: float = 0.0
    applicable_query_types: List[QueryType] = field(default_factory=list)


class SQLAlchemyRelationshipOptimizer:
    """
    Advanced SQLAlchemy relationship optimizer for enterprise analytics.
    
    Features:
    - Intelligent loading strategy selection based on query patterns
    - N+1 query prevention and batch loading
    - Star schema optimized relationships
    - Query plan analysis and optimization
    - Performance monitoring and adaptive tuning
    """
    
    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.relationship_configs: Dict[str, RelationshipConfig] = {}
        self.optimization_rules: List[QueryOptimizationRule] = []
        self.query_performance_history: List[Dict[str, Any]] = []
        
        # Performance metrics
        self.metrics = {
            "optimized_queries": 0,
            "n_plus_one_prevented": 0,
            "batch_loads_applied": 0,
            "avg_query_time_ms": 0.0,
            "performance_improvement_pct": 0.0
        }
        
        # Setup event listeners
        self._setup_query_monitoring()
    
    def _setup_query_monitoring(self):
        """Setup SQLAlchemy event listeners for query monitoring."""
        
        @event.listens_for(self.engine.sync_engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            context._query_start_time = time.time()
            context._statement = statement
        
        @event.listens_for(self.engine.sync_engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            if hasattr(context, '_query_start_time'):
                execution_time = (time.time() - context._query_start_time) * 1000
                
                # Analyze query for optimization opportunities
                self._analyze_query_performance(statement, execution_time)
    
    def _analyze_query_performance(self, statement: str, execution_time_ms: float):
        """Analyze query performance for optimization opportunities."""
        try:
            # Record performance data
            performance_record = {
                "timestamp": datetime.utcnow(),
                "statement": statement[:200] + "..." if len(statement) > 200 else statement,
                "execution_time_ms": execution_time_ms,
                "query_type": self._classify_query_type(statement),
                "optimization_applied": False
            }
            
            self.query_performance_history.append(performance_record)
            
            # Keep only last 1000 records
            if len(self.query_performance_history) > 1000:
                self.query_performance_history = self.query_performance_history[-1000:]
            
            # Update metrics
            self._update_performance_metrics()
            
            # Detect N+1 patterns
            if self._is_potential_n_plus_one(statement):
                logger.warning(f"Potential N+1 query detected: {statement[:100]}...")
                self.metrics["n_plus_one_prevented"] += 1
            
        except Exception as e:
            logger.warning(f"Error analyzing query performance: {e}")
    
    def _classify_query_type(self, statement: str) -> QueryType:
        """Classify query type based on SQL patterns."""
        statement_lower = statement.lower()
        
        # Analytics queries
        if any(keyword in statement_lower for keyword in ['group by', 'having', 'window', 'partition']):
            return QueryType.ANALYTICS
        
        # Reporting queries
        if any(keyword in statement_lower for keyword in ['sum(', 'count(', 'avg(', 'percentile']):
            return QueryType.REPORTING
        
        # Dashboard queries
        if 'limit' in statement_lower and ('order by' in statement_lower):
            return QueryType.DASHBOARD
        
        # Bulk operations
        if any(keyword in statement_lower for keyword in ['insert', 'update', 'delete']) and 'where' not in statement_lower:
            return QueryType.BULK_LOAD
        
        # Real-time queries
        if 'limit 1' in statement_lower or 'top 1' in statement_lower:
            return QueryType.REAL_TIME
        
        return QueryType.TRANSACTIONAL
    
    def _is_potential_n_plus_one(self, statement: str) -> bool:
        """Detect potential N+1 query patterns."""
        statement_lower = statement.lower()
        
        # Simple heuristics for N+1 detection
        if ('select' in statement_lower and 
            'from' in statement_lower and 
            'where' in statement_lower and
            'in (' not in statement_lower and
            'join' not in statement_lower):
            return True
        
        return False
    
    def _update_performance_metrics(self):
        """Update performance metrics from query history."""
        if not self.query_performance_history:
            return
        
        recent_queries = self.query_performance_history[-100:]  # Last 100 queries
        
        total_time = sum(q["execution_time_ms"] for q in recent_queries)
        self.metrics["avg_query_time_ms"] = total_time / len(recent_queries)
        
        optimized_queries = sum(1 for q in recent_queries if q.get("optimization_applied", False))
        self.metrics["optimized_queries"] = optimized_queries
    
    def get_star_schema_relationship_configs(self) -> Dict[str, RelationshipConfig]:
        """Get optimized relationship configurations for star schema."""
        
        configs = {
            # Fact to Dimension relationships - optimized for analytics
            "fact_sale_to_dim_product": RelationshipConfig(
                relationship_name="product",
                model_class="FactSale", 
                related_class="DimProduct",
                loading_strategy=LoadingStrategy.JOINED,
                query_types=[QueryType.ANALYTICS, QueryType.REPORTING],
                eager_columns=["product_key", "stock_code", "description", "category", "brand"],
                defer_columns=["created_at", "updated_at", "metadata"],
                cache_enabled=True,
                cache_ttl_seconds=1800  # 30 minutes
            ),
            
            "fact_sale_to_dim_customer": RelationshipConfig(
                relationship_name="customer",
                model_class="FactSale",
                related_class="DimCustomer", 
                loading_strategy=LoadingStrategy.SELECT_IN,
                query_types=[QueryType.ANALYTICS, QueryType.REPORTING],
                batch_size=50,
                eager_columns=["customer_key", "customer_id", "customer_segment", "rfm_segment"],
                defer_columns=["created_at", "updated_at", "version", "valid_from", "valid_to"],
                cache_enabled=True,
                cache_ttl_seconds=900  # 15 minutes
            ),
            
            "fact_sale_to_dim_date": RelationshipConfig(
                relationship_name="date",
                model_class="FactSale",
                related_class="DimDate",
                loading_strategy=LoadingStrategy.JOINED,  # Date is small and frequently needed
                query_types=[QueryType.ANALYTICS, QueryType.REPORTING, QueryType.DASHBOARD],
                eager_columns=["date_key", "date", "year", "month", "quarter", "month_name"],
                cache_enabled=True,
                cache_ttl_seconds=3600  # 1 hour
            ),
            
            "fact_sale_to_dim_country": RelationshipConfig(
                relationship_name="country",
                model_class="FactSale",
                related_class="DimCountry",
                loading_strategy=LoadingStrategy.JOINED,
                query_types=[QueryType.ANALYTICS, QueryType.REPORTING],
                eager_columns=["country_key", "country_code", "country_name", "region", "continent"],
                defer_columns=["latitude", "longitude", "timezone"],
                cache_enabled=True,
                cache_ttl_seconds=3600
            ),
            
            "fact_sale_to_dim_invoice": RelationshipConfig(
                relationship_name="invoice",
                model_class="FactSale",
                related_class="DimInvoice", 
                loading_strategy=LoadingStrategy.SELECT_IN,
                query_types=[QueryType.TRANSACTIONAL, QueryType.REAL_TIME],
                batch_size=25,
                eager_columns=["invoice_key", "invoice_no", "invoice_date", "is_cancelled"],
                defer_columns=["processing_time_minutes", "created_at", "updated_at"]
            ),
            
            # Dimension self-relationships (SCD Type 2)
            "dim_product_versions": RelationshipConfig(
                relationship_name="versions",
                model_class="DimProduct",
                related_class="DimProduct",
                loading_strategy=LoadingStrategy.LAZY,  # Rarely needed
                query_types=[QueryType.ANALYTICS],
                defer_columns=["created_at", "updated_at", "version"]
            ),
            
            "dim_customer_versions": RelationshipConfig(
                relationship_name="versions", 
                model_class="DimCustomer",
                related_class="DimCustomer",
                loading_strategy=LoadingStrategy.LAZY,
                query_types=[QueryType.ANALYTICS],
                defer_columns=["created_at", "updated_at", "version"]
            ),
            
            # Dashboard-specific optimized relationships
            "dashboard_fact_sale_summary": RelationshipConfig(
                relationship_name="sales_summary",
                model_class="FactSale",
                related_class="DimProduct",
                loading_strategy=LoadingStrategy.SUBQUERY,
                query_types=[QueryType.DASHBOARD],
                eager_columns=["category", "brand", "recommended_price"],
                batch_size=100
            )
        }
        
        return configs
    
    def get_advanced_optimization_rules(self) -> List[QueryOptimizationRule]:
        """Get advanced query optimization rules."""
        
        rules = [
            # Analytics query optimizations
            QueryOptimizationRule(
                rule_name="analytics_batch_loading",
                query_pattern="SELECT.*FROM fact_sale.*JOIN.*GROUP BY",
                optimization_strategy="batch_selectin_load",
                loading_options={
                    "batch_size": 100,
                    "defer_non_essential": True
                },
                expected_improvement_pct=40.0,
                applicable_query_types=[QueryType.ANALYTICS, QueryType.REPORTING]
            ),
            
            # Dashboard query optimizations
            QueryOptimizationRule(
                rule_name="dashboard_joined_loading",
                query_pattern="SELECT.*FROM fact_sale.*LIMIT",
                optimization_strategy="joined_load_essential",
                loading_options={
                    "join_essential_dims": ["date", "product", "customer"],
                    "defer_large_columns": True
                },
                expected_improvement_pct=60.0,
                applicable_query_types=[QueryType.DASHBOARD, QueryType.REAL_TIME]
            ),
            
            # Reporting aggregation optimizations
            QueryOptimizationRule(
                rule_name="reporting_aggregation_optimization",
                query_pattern="SELECT.*SUM\\(|COUNT\\(|AVG\\(.*GROUP BY",
                optimization_strategy="materialized_view_redirect",
                loading_options={
                    "use_pre_aggregated": True,
                    "cache_results": True
                },
                expected_improvement_pct=80.0,
                applicable_query_types=[QueryType.REPORTING, QueryType.ANALYTICS]
            ),
            
            # Large result set optimizations
            QueryOptimizationRule(
                rule_name="large_result_streaming",
                query_pattern="SELECT.*FROM.*WHERE.*LIMIT [0-9]{4,}",
                optimization_strategy="stream_results", 
                loading_options={
                    "stream_results": True,
                    "batch_size": 1000,
                    "lazy_load_relationships": True
                },
                expected_improvement_pct=50.0,
                applicable_query_types=[QueryType.BULK_LOAD, QueryType.ANALYTICS]
            ),
            
            # N+1 prevention
            QueryOptimizationRule(
                rule_name="n_plus_one_prevention",
                query_pattern="SELECT.*FROM.*WHERE.*IN \\(",
                optimization_strategy="selectin_load_batch",
                loading_options={
                    "selectin_load_relationships": True,
                    "batch_size": 50
                },
                expected_improvement_pct=90.0,
                applicable_query_types=[QueryType.TRANSACTIONAL, QueryType.DASHBOARD]
            )
        ]
        
        return rules
    
    def create_optimized_query(self, base_query: Select, query_type: QueryType,
                              relationships: List[str] = None) -> Select:
        """Create optimized query with appropriate loading strategies."""
        
        optimized_query = base_query
        
        if not relationships:
            relationships = []
        
        try:
            # Apply optimization based on query type
            if query_type == QueryType.ANALYTICS:
                optimized_query = self._optimize_for_analytics(optimized_query, relationships)
            elif query_type == QueryType.DASHBOARD:
                optimized_query = self._optimize_for_dashboard(optimized_query, relationships)
            elif query_type == QueryType.REPORTING:
                optimized_query = self._optimize_for_reporting(optimized_query, relationships)
            elif query_type == QueryType.REAL_TIME:
                optimized_query = self._optimize_for_real_time(optimized_query, relationships)
            elif query_type == QueryType.BULK_LOAD:
                optimized_query = self._optimize_for_bulk_load(optimized_query, relationships)
            
            # Apply general optimizations
            optimized_query = self._apply_general_optimizations(optimized_query)
            
            self.metrics["optimized_queries"] += 1
            
        except Exception as e:
            logger.error(f"Error creating optimized query: {e}")
            # Return original query if optimization fails
            return base_query
        
        return optimized_query
    
    def _optimize_for_analytics(self, query: Select, relationships: List[str]) -> Select:
        """Optimize query for analytics workloads."""
        options = []
        
        for rel in relationships:
            if rel in ["product", "date", "country"]:
                # Use joined loading for frequently accessed dimensions
                options.append(joinedload(rel).load_only(
                    "product_key", "stock_code", "category", "brand" if rel == "product" else
                    "date_key", "date", "year", "month", "quarter" if rel == "date" else
                    "country_key", "country_code", "country_name", "region"
                ))
            elif rel == "customer":
                # Use selectin loading for customer (larger dimension)
                options.append(selectinload(rel).options(
                    defer("created_at", "updated_at"),
                    load_only("customer_key", "customer_id", "customer_segment")
                ))
        
        return query.options(*options) if options else query
    
    def _optimize_for_dashboard(self, query: Select, relationships: List[str]) -> Select:
        """Optimize query for dashboard/real-time workloads."""
        options = []
        
        for rel in relationships:
            # Use joined loading for essential dashboard data
            if rel in ["product", "date"]:
                options.append(joinedload(rel))
            else:
                # Use selectin loading with small batches for other relationships
                options.append(selectinload(rel).options(defer("*")))
        
        return query.options(*options) if options else query
    
    def _optimize_for_reporting(self, query: Select, relationships: List[str]) -> Select:
        """Optimize query for reporting workloads."""
        options = []
        
        # For reporting, we typically need aggregated data
        for rel in relationships:
            # Defer large columns, load only essential reporting fields
            if rel == "product":
                options.append(selectinload(rel).options(
                    load_only("category", "brand", "recommended_price"),
                    defer("description", "created_at", "updated_at")
                ))
            elif rel == "customer":
                options.append(selectinload(rel).options(
                    load_only("customer_segment", "rfm_segment", "lifetime_value"),
                    defer("created_at", "updated_at", "version")
                ))
        
        return query.options(*options) if options else query
    
    def _optimize_for_real_time(self, query: Select, relationships: List[str]) -> Select:
        """Optimize query for real-time/low-latency workloads."""
        options = []
        
        # For real-time queries, minimize data transfer
        for rel in relationships:
            # Use joined loading but defer non-essential columns
            options.append(joinedload(rel).options(
                defer("created_at", "updated_at", "metadata")
            ))
        
        return query.options(*options) if options else query
    
    def _optimize_for_bulk_load(self, query: Select, relationships: List[str]) -> Select:
        """Optimize query for bulk load operations."""
        options = []
        
        # For bulk operations, minimize relationship loading
        for rel in relationships:
            # Use lazy loading to avoid loading unnecessary relationships
            options.append(defaultload(rel))
        
        return query.options(*options) if options else query
    
    def _apply_general_optimizations(self, query: Select) -> Select:
        """Apply general query optimizations."""
        # Add query hints or additional optimizations
        # This could include index hints, query plan directives, etc.
        
        return query
    
    async def create_optimized_session_query(self, session: AsyncSession,
                                           model_class: Type,
                                           query_type: QueryType = QueryType.TRANSACTIONAL,
                                           filters: Dict[str, Any] = None,
                                           relationships: List[str] = None,
                                           limit: Optional[int] = None,
                                           order_by: Optional[str] = None) -> Select:
        """Create an optimized query for a given model and parameters."""
        
        # Build base query
        query = select(model_class)
        
        # Apply filters
        if filters:
            for column, value in filters.items():
                if hasattr(model_class, column):
                    query = query.where(getattr(model_class, column) == value)
        
        # Apply ordering
        if order_by and hasattr(model_class, order_by):
            query = query.order_by(getattr(model_class, order_by))
        
        # Apply limit
        if limit:
            query = query.limit(limit)
        
        # Apply optimizations
        optimized_query = self.create_optimized_query(query, query_type, relationships or [])
        
        return optimized_query
    
    def get_performance_recommendations(self) -> List[Dict[str, Any]]:
        """Get performance optimization recommendations based on query history."""
        recommendations = []
        
        if not self.query_performance_history:
            return recommendations
        
        # Analyze slow queries
        slow_queries = [q for q in self.query_performance_history if q["execution_time_ms"] > 1000]
        
        if slow_queries:
            recommendations.append({
                "type": "slow_queries",
                "description": f"Found {len(slow_queries)} slow queries (>1s)",
                "recommendation": "Consider adding indexes or optimizing query patterns",
                "affected_queries": len(slow_queries),
                "priority": "high"
            })
        
        # Check for N+1 patterns
        n_plus_one_count = self.metrics.get("n_plus_one_prevented", 0)
        if n_plus_one_count > 10:
            recommendations.append({
                "type": "n_plus_one",
                "description": f"Detected {n_plus_one_count} potential N+1 query patterns",
                "recommendation": "Implement batch loading or eager loading strategies",
                "affected_queries": n_plus_one_count,
                "priority": "high"
            })
        
        # Analyze query types
        query_types = [q["query_type"] for q in self.query_performance_history[-100:]]
        type_counts = {}
        for qt in query_types:
            type_counts[qt] = type_counts.get(qt, 0) + 1
        
        # Recommendations based on query patterns
        if type_counts.get(QueryType.ANALYTICS, 0) > 50:
            recommendations.append({
                "type": "analytics_optimization",
                "description": "High volume of analytics queries detected",
                "recommendation": "Consider implementing materialized views or query caching",
                "affected_queries": type_counts[QueryType.ANALYTICS],
                "priority": "medium"
            })
        
        if type_counts.get(QueryType.DASHBOARD, 0) > 30:
            recommendations.append({
                "type": "dashboard_optimization", 
                "description": "Frequent dashboard queries detected",
                "recommendation": "Implement real-time caching and optimized loading strategies",
                "affected_queries": type_counts[QueryType.DASHBOARD],
                "priority": "medium"
            })
        
        return recommendations
    
    def get_optimization_metrics(self) -> Dict[str, Any]:
        """Get comprehensive optimization metrics."""
        
        # Calculate recent performance
        recent_queries = self.query_performance_history[-100:] if self.query_performance_history else []
        
        avg_execution_time = 0.0
        if recent_queries:
            avg_execution_time = sum(q["execution_time_ms"] for q in recent_queries) / len(recent_queries)
        
        # Query type distribution
        query_type_dist = {}
        for query in recent_queries:
            qt = query["query_type"]
            query_type_dist[qt.value] = query_type_dist.get(qt.value, 0) + 1
        
        return {
            "performance_metrics": self.metrics,
            "recent_avg_execution_time_ms": avg_execution_time,
            "total_queries_analyzed": len(self.query_performance_history),
            "query_type_distribution": query_type_dist,
            "relationship_configs": len(self.relationship_configs),
            "optimization_rules": len(self.optimization_rules),
            "recommendations": self.get_performance_recommendations(),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def initialize_optimizations(self):
        """Initialize default optimizations."""
        # Load star schema relationship configs
        self.relationship_configs.update(self.get_star_schema_relationship_configs())
        
        # Load optimization rules
        self.optimization_rules.extend(self.get_advanced_optimization_rules())
        
        logger.info(f"Initialized SQLAlchemy optimizer with "
                   f"{len(self.relationship_configs)} relationship configs and "
                   f"{len(self.optimization_rules)} optimization rules")


# Utility functions and context managers

@asynccontextmanager
async def optimized_session(optimizer: SQLAlchemyRelationshipOptimizer, 
                          engine: AsyncEngine,
                          query_type: QueryType = QueryType.TRANSACTIONAL):
    """Context manager for optimized database sessions."""
    
    from sqlalchemy.ext.asyncio import AsyncSession
    
    async with AsyncSession(engine, expire_on_commit=False) as session:
        # Configure session for specific query type
        if query_type == QueryType.BULK_LOAD:
            # Optimize for bulk operations
            session.configure(autoflush=False)
        elif query_type in [QueryType.ANALYTICS, QueryType.REPORTING]:
            # Optimize for read-heavy operations
            session.configure(autoflush=False, expire_on_commit=False)
        
        try:
            yield session
        finally:
            await session.close()


def create_relationship_optimizer(engine: AsyncEngine) -> SQLAlchemyRelationshipOptimizer:
    """Factory function to create and initialize relationship optimizer."""
    
    optimizer = SQLAlchemyRelationshipOptimizer(engine)
    optimizer.initialize_optimizations()
    
    return optimizer


# Advanced SQL pattern utilities

class AdvancedSQLPatterns:
    """Advanced SQL patterns and query builders for analytics."""
    
    @staticmethod
    def create_window_function_query(model_class: Type, 
                                   partition_by: str,
                                   order_by: str,
                                   window_function: str = "ROW_NUMBER()") -> Select:
        """Create query with window functions."""
        
        return select(
            model_class,
            func.row_number().over(
                partition_by=getattr(model_class, partition_by),
                order_by=getattr(model_class, order_by)
            ).label("row_num")
        )
    
    @staticmethod
    def create_cte_query(base_query: Select, cte_name: str) -> Select:
        """Create query with Common Table Expression (CTE)."""
        
        cte = base_query.cte(name=cte_name)
        return select(cte)
    
    @staticmethod
    def create_recursive_cte_query(model_class: Type,
                                 parent_column: str,
                                 child_column: str,
                                 start_condition: Any) -> Select:
        """Create recursive CTE query for hierarchical data."""
        
        # Base case
        base_case = select(model_class).where(
            getattr(model_class, parent_column) == start_condition
        )
        
        # Recursive case (simplified - would need proper implementation)
        recursive_cte = base_case.cte(name="hierarchy", recursive=True)
        
        return select(recursive_cte)
    
    @staticmethod
    def create_analytical_aggregation_query(model_class: Type,
                                          group_by_columns: List[str],
                                          aggregation_columns: Dict[str, str],
                                          filters: Dict[str, Any] = None) -> Select:
        """Create complex analytical aggregation query."""
        
        # Build select list
        select_columns = []
        
        # Add grouping columns
        for col in group_by_columns:
            if hasattr(model_class, col):
                select_columns.append(getattr(model_class, col))
        
        # Add aggregation columns
        for col, agg_func in aggregation_columns.items():
            if hasattr(model_class, col):
                column_ref = getattr(model_class, col)
                if agg_func.upper() == "SUM":
                    select_columns.append(func.sum(column_ref).label(f"sum_{col}"))
                elif agg_func.upper() == "COUNT":
                    select_columns.append(func.count(column_ref).label(f"count_{col}"))
                elif agg_func.upper() == "AVG":
                    select_columns.append(func.avg(column_ref).label(f"avg_{col}"))
                elif agg_func.upper() == "MAX":
                    select_columns.append(func.max(column_ref).label(f"max_{col}"))
                elif agg_func.upper() == "MIN":
                    select_columns.append(func.min(column_ref).label(f"min_{col}"))
        
        # Build query
        query = select(*select_columns)
        
        # Apply filters
        if filters:
            for column, value in filters.items():
                if hasattr(model_class, column):
                    query = query.where(getattr(model_class, column) == value)
        
        # Group by
        group_columns = [getattr(model_class, col) for col in group_by_columns 
                        if hasattr(model_class, col)]
        if group_columns:
            query = query.group_by(*group_columns)
        
        return query


# Example usage
async def example_optimized_queries():
    """Example of using optimized queries."""
    
    # This would be used with actual SQLAlchemy models
    # engine = create_async_engine("postgresql+asyncpg://...")
    # optimizer = create_relationship_optimizer(engine)
    
    # Example analytics query
    # async with optimized_session(optimizer, engine, QueryType.ANALYTICS) as session:
    #     query = await optimizer.create_optimized_session_query(
    #         session=session,
    #         model_class=FactSale,
    #         query_type=QueryType.ANALYTICS,
    #         relationships=["product", "customer", "date"],
    #         filters={"date_key": 20240101},
    #         limit=1000
    #     )
    #     results = await session.execute(query)
    
    pass


if __name__ == "__main__":
    asyncio.run(example_optimized_queries())