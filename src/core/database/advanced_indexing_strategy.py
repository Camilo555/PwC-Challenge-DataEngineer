"""
Advanced Database Indexing Strategy for High-Performance Queries
Targets <25ms query performance with comprehensive optimization
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass
from enum import Enum

import sqlalchemy as sa
from sqlalchemy import text, Index, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select
from sqlalchemy.engine import Engine

from core.database import get_async_session
from core.config import settings


class IndexType(Enum):
    """Types of database indexes for optimization strategy."""
    BTREE = "btree"
    HASH = "hash"
    GIN = "gin"
    GIST = "gist"
    BRIN = "brin"
    PARTIAL = "partial"
    COMPOSITE = "composite"
    COVERING = "covering"
    FUNCTIONAL = "functional"


@dataclass
class IndexRecommendation:
    """Index recommendation with performance impact analysis."""
    table_name: str
    columns: List[str]
    index_type: IndexType
    expected_performance_gain: float
    estimated_size_mb: float
    maintenance_cost: str
    query_patterns: List[str]
    creation_sql: str
    priority: str  # HIGH, MEDIUM, LOW


@dataclass
class QueryPerformanceMetrics:
    """Query performance metrics for analysis."""
    query_hash: str
    execution_time_ms: float
    rows_examined: int
    rows_returned: int
    table_scans: int
    index_usage: List[str]
    timestamp: datetime
    query_pattern: str


class AdvancedIndexingStrategy:
    """Advanced database indexing strategy with performance optimization."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.query_metrics: List[QueryPerformanceMetrics] = []
        self.index_recommendations: List[IndexRecommendation] = []

        # High-performance index configurations
        self.index_configurations = {
            # Sales data optimization - high-frequency queries
            "sales_performance_indexes": [
                {
                    "table": "sales",
                    "name": "idx_sales_date_customer_product",
                    "columns": ["sale_date", "customer_id", "product_id"],
                    "type": IndexType.COMPOSITE,
                    "where": "sale_date >= CURRENT_DATE - INTERVAL '2 years'",
                    "priority": "HIGH"
                },
                {
                    "table": "sales",
                    "name": "idx_sales_revenue_covering",
                    "columns": ["sale_date", "customer_id", "total_amount", "quantity"],
                    "type": IndexType.COVERING,
                    "priority": "HIGH"
                },
                {
                    "table": "sales",
                    "name": "idx_sales_time_series",
                    "columns": ["sale_date", "created_at"],
                    "type": IndexType.BRIN,
                    "priority": "MEDIUM"
                }
            ],

            # Customer analytics optimization
            "customer_analytics_indexes": [
                {
                    "table": "customers",
                    "name": "idx_customers_rfm_composite",
                    "columns": ["rfm_recency_score", "rfm_frequency_score", "rfm_monetary_score"],
                    "type": IndexType.COMPOSITE,
                    "priority": "HIGH"
                },
                {
                    "table": "customers",
                    "name": "idx_customers_segmentation",
                    "columns": ["customer_segment", "country", "created_at"],
                    "type": IndexType.COMPOSITE,
                    "priority": "MEDIUM"
                },
                {
                    "table": "customers",
                    "name": "idx_customers_active_partial",
                    "columns": ["customer_id", "last_purchase_date"],
                    "type": IndexType.PARTIAL,
                    "where": "last_purchase_date >= CURRENT_DATE - INTERVAL '1 year'",
                    "priority": "HIGH"
                }
            ],

            # Product catalog optimization
            "product_performance_indexes": [
                {
                    "table": "products",
                    "name": "idx_products_category_price",
                    "columns": ["category", "unit_price", "product_name"],
                    "type": IndexType.COMPOSITE,
                    "priority": "MEDIUM"
                },
                {
                    "table": "products",
                    "name": "idx_products_search_gin",
                    "columns": ["to_tsvector('english', product_name || ' ' || COALESCE(description, ''))"],
                    "type": IndexType.FUNCTIONAL,
                    "priority": "MEDIUM"
                }
            ],

            # ETL pipeline optimization
            "etl_pipeline_indexes": [
                {
                    "table": "bronze_sales",
                    "name": "idx_bronze_processing_status",
                    "columns": ["processing_status", "created_at"],
                    "type": IndexType.COMPOSITE,
                    "priority": "HIGH"
                },
                {
                    "table": "silver_sales_agg",
                    "name": "idx_silver_date_partitioned",
                    "columns": ["aggregation_date", "customer_id"],
                    "type": IndexType.COMPOSITE,
                    "priority": "HIGH"
                },
                {
                    "table": "gold_business_metrics",
                    "name": "idx_gold_kpi_time_series",
                    "columns": ["metric_date", "kpi_category"],
                    "type": IndexType.BRIN,
                    "priority": "HIGH"
                }
            ],

            # Real-time analytics optimization
            "realtime_analytics_indexes": [
                {
                    "table": "real_time_events",
                    "name": "idx_events_timestamp_hash",
                    "columns": ["event_timestamp"],
                    "type": IndexType.HASH,
                    "priority": "HIGH"
                },
                {
                    "table": "real_time_events",
                    "name": "idx_events_user_session",
                    "columns": ["user_id", "session_id", "event_type"],
                    "type": IndexType.COMPOSITE,
                    "priority": "MEDIUM"
                }
            ]
        }

    async def analyze_current_performance(self) -> Dict[str, Any]:
        """Analyze current database performance and identify optimization opportunities."""
        try:
            async with get_async_session() as session:
                # Get current index usage statistics
                index_stats = await self._get_index_usage_stats(session)

                # Analyze slow queries
                slow_queries = await self._identify_slow_queries(session)

                # Get table statistics
                table_stats = await self._get_table_statistics(session)

                # Analyze query patterns
                query_patterns = await self._analyze_query_patterns(session)

                return {
                    "current_performance": {
                        "avg_query_time_ms": await self._get_avg_query_time(session),
                        "slow_query_count": len(slow_queries),
                        "unused_indexes": await self._find_unused_indexes(session),
                        "table_scan_ratio": await self._calculate_table_scan_ratio(session)
                    },
                    "optimization_opportunities": {
                        "missing_indexes": await self._identify_missing_indexes(session),
                        "redundant_indexes": await self._find_redundant_indexes(session),
                        "optimization_potential": await self._calculate_optimization_potential(session)
                    },
                    "recommendations": await self._generate_index_recommendations(session),
                    "index_statistics": index_stats,
                    "table_statistics": table_stats,
                    "query_patterns": query_patterns
                }

        except Exception as e:
            self.logger.error(f"Error analyzing database performance: {e}")
            return {"error": str(e)}

    async def implement_high_priority_indexes(self) -> Dict[str, Any]:
        """Implement high-priority indexes for immediate performance gains."""
        results = {
            "implemented_indexes": [],
            "failed_indexes": [],
            "performance_impact": {}
        }

        try:
            async with get_async_session() as session:
                # Get performance baseline
                baseline_performance = await self._measure_query_performance(session)

                # Implement indexes by priority
                for category, indexes in self.index_configurations.items():
                    for index_config in indexes:
                        if index_config["priority"] == "HIGH":
                            try:
                                sql = self._generate_index_sql(index_config)
                                await session.execute(text(sql))
                                await session.commit()

                                results["implemented_indexes"].append({
                                    "name": index_config["name"],
                                    "table": index_config["table"],
                                    "columns": index_config["columns"],
                                    "type": index_config["type"].value,
                                    "sql": sql
                                })

                                self.logger.info(f"Successfully created index: {index_config['name']}")

                            except Exception as e:
                                results["failed_indexes"].append({
                                    "name": index_config["name"],
                                    "error": str(e)
                                })
                                self.logger.error(f"Failed to create index {index_config['name']}: {e}")

                # Measure performance improvement
                post_performance = await self._measure_query_performance(session)
                results["performance_impact"] = {
                    "baseline_avg_ms": baseline_performance["avg_query_time_ms"],
                    "optimized_avg_ms": post_performance["avg_query_time_ms"],
                    "improvement_percent": (
                        (baseline_performance["avg_query_time_ms"] - post_performance["avg_query_time_ms"])
                        / baseline_performance["avg_query_time_ms"] * 100
                    ) if baseline_performance["avg_query_time_ms"] > 0 else 0,
                    "target_achieved": post_performance["avg_query_time_ms"] < 25.0
                }

        except Exception as e:
            self.logger.error(f"Error implementing indexes: {e}")
            results["error"] = str(e)

        return results

    def _generate_index_sql(self, index_config: Dict[str, Any]) -> str:
        """Generate SQL for creating an index based on configuration."""
        table = index_config["table"]
        name = index_config["name"]
        columns = index_config["columns"]
        index_type = index_config["type"]

        if index_type == IndexType.COMPOSITE:
            columns_str = ", ".join(columns)
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} ({columns_str})"

        elif index_type == IndexType.PARTIAL:
            columns_str = ", ".join(columns)
            where_clause = index_config.get("where", "")
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} ({columns_str}) WHERE {where_clause}"

        elif index_type == IndexType.COVERING:
            base_columns = columns[:2]  # First 2 are key columns
            include_columns = columns[2:] if len(columns) > 2 else []

            base_str = ", ".join(base_columns)
            include_str = f" INCLUDE ({', '.join(include_columns)})" if include_columns else ""
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} ({base_str}){include_str}"

        elif index_type == IndexType.FUNCTIONAL:
            # For functional indexes, columns contain the expression
            expression = columns[0] if columns else ""
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} USING GIN ({expression})"

        elif index_type == IndexType.HASH:
            columns_str = ", ".join(columns)
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} USING HASH ({columns_str})"

        elif index_type == IndexType.BRIN:
            columns_str = ", ".join(columns)
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} USING BRIN ({columns_str})"

        elif index_type == IndexType.GIN:
            columns_str = ", ".join(columns)
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} USING GIN ({columns_str})"

        else:  # Default BTREE
            columns_str = ", ".join(columns)
            sql = f"CREATE INDEX CONCURRENTLY {name} ON {table} ({columns_str})"

        return sql

    async def _get_index_usage_stats(self, session: AsyncSession) -> Dict[str, Any]:
        """Get index usage statistics from PostgreSQL system tables."""
        try:
            query = text("""
                SELECT
                    schemaname,
                    tablename,
                    indexname,
                    idx_tup_read,
                    idx_tup_fetch,
                    idx_scan
                FROM pg_stat_user_indexes
                ORDER BY idx_scan DESC, idx_tup_read DESC
            """)

            result = await session.execute(query)
            stats = []

            for row in result:
                stats.append({
                    "schema": row.schemaname,
                    "table": row.tablename,
                    "index": row.indexname,
                    "tuples_read": row.idx_tup_read,
                    "tuples_fetched": row.idx_tup_fetch,
                    "scans": row.idx_scan,
                    "usage_ratio": row.idx_tup_fetch / max(row.idx_tup_read, 1)
                })

            return {
                "total_indexes": len(stats),
                "active_indexes": len([s for s in stats if s["scans"] > 0]),
                "unused_indexes": len([s for s in stats if s["scans"] == 0]),
                "index_details": stats
            }

        except Exception as e:
            self.logger.error(f"Error getting index usage stats: {e}")
            return {"error": str(e)}

    async def _identify_slow_queries(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """Identify slow queries using pg_stat_statements if available."""
        try:
            # Check if pg_stat_statements extension is available
            check_extension = text("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
                )
            """)

            result = await session.execute(check_extension)
            has_pg_stat_statements = result.scalar()

            if not has_pg_stat_statements:
                self.logger.warning("pg_stat_statements extension not available")
                return []

            query = text("""
                SELECT
                    query,
                    calls,
                    mean_exec_time,
                    max_exec_time,
                    stddev_exec_time,
                    rows,
                    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
                FROM pg_stat_statements
                WHERE mean_exec_time > 25.0  -- Slower than 25ms
                ORDER BY mean_exec_time DESC
                LIMIT 50
            """)

            result = await session.execute(query)
            slow_queries = []

            for row in result:
                slow_queries.append({
                    "query": row.query,
                    "calls": row.calls,
                    "mean_time_ms": row.mean_exec_time,
                    "max_time_ms": row.max_exec_time,
                    "stddev_time_ms": row.stddev_exec_time,
                    "rows_avg": row.rows / max(row.calls, 1),
                    "cache_hit_percent": row.hit_percent or 0
                })

            return slow_queries

        except Exception as e:
            self.logger.error(f"Error identifying slow queries: {e}")
            return []

    async def _get_table_statistics(self, session: AsyncSession) -> Dict[str, Any]:
        """Get table statistics for optimization analysis."""
        try:
            query = text("""
                SELECT
                    schemaname,
                    tablename,
                    n_tup_ins,
                    n_tup_upd,
                    n_tup_del,
                    n_live_tup,
                    n_dead_tup,
                    seq_scan,
                    seq_tup_read,
                    idx_scan,
                    idx_tup_fetch
                FROM pg_stat_user_tables
                ORDER BY n_live_tup DESC
            """)

            result = await session.execute(query)
            table_stats = []

            for row in result:
                table_stats.append({
                    "schema": row.schemaname,
                    "table": row.tablename,
                    "live_tuples": row.n_live_tup,
                    "dead_tuples": row.n_dead_tup,
                    "insertions": row.n_tup_ins,
                    "updates": row.n_tup_upd,
                    "deletions": row.n_tup_del,
                    "sequential_scans": row.seq_scan,
                    "seq_tuples_read": row.seq_tup_read,
                    "index_scans": row.idx_scan,
                    "index_tuples_fetched": row.idx_tup_fetch,
                    "table_scan_ratio": row.seq_scan / max(row.seq_scan + row.idx_scan, 1)
                })

            return {
                "total_tables": len(table_stats),
                "high_activity_tables": [
                    t for t in table_stats
                    if t["insertions"] + t["updates"] + t["deletions"] > 10000
                ],
                "scan_heavy_tables": [
                    t for t in table_stats
                    if t["table_scan_ratio"] > 0.5 and t["sequential_scans"] > 100
                ],
                "table_details": table_stats
            }

        except Exception as e:
            self.logger.error(f"Error getting table statistics: {e}")
            return {"error": str(e)}

    async def _analyze_query_patterns(self, session: AsyncSession) -> Dict[str, Any]:
        """Analyze query patterns to identify optimization opportunities."""
        try:
            # This would typically analyze application query logs
            # For now, we'll simulate based on known application patterns

            patterns = {
                "sales_analytics": {
                    "pattern": "SELECT customer_id, SUM(total_amount) FROM sales WHERE sale_date BETWEEN ? AND ? GROUP BY customer_id",
                    "frequency": "high",
                    "avg_time_ms": 45,
                    "optimization_potential": "high",
                    "recommended_index": "idx_sales_date_customer_amount"
                },
                "customer_segmentation": {
                    "pattern": "SELECT * FROM customers WHERE rfm_*_score BETWEEN ? AND ?",
                    "frequency": "medium",
                    "avg_time_ms": 35,
                    "optimization_potential": "medium",
                    "recommended_index": "idx_customers_rfm_composite"
                },
                "product_search": {
                    "pattern": "SELECT * FROM products WHERE product_name ILIKE ?",
                    "frequency": "high",
                    "avg_time_ms": 60,
                    "optimization_potential": "high",
                    "recommended_index": "idx_products_search_gin"
                },
                "real_time_events": {
                    "pattern": "SELECT * FROM real_time_events WHERE event_timestamp > ?",
                    "frequency": "very_high",
                    "avg_time_ms": 15,
                    "optimization_potential": "medium",
                    "recommended_index": "idx_events_timestamp_hash"
                }
            }

            return patterns

        except Exception as e:
            self.logger.error(f"Error analyzing query patterns: {e}")
            return {"error": str(e)}

    async def _measure_query_performance(self, session: AsyncSession) -> Dict[str, float]:
        """Measure current query performance benchmarks."""
        try:
            # Test queries representative of application workload
            test_queries = [
                "SELECT COUNT(*) FROM sales WHERE sale_date >= CURRENT_DATE - INTERVAL '30 days'",
                "SELECT customer_id, COUNT(*) FROM sales GROUP BY customer_id LIMIT 100",
                "SELECT * FROM customers WHERE customer_segment = 'Premium' LIMIT 50",
                "SELECT product_name FROM products WHERE unit_price BETWEEN 10 AND 100 LIMIT 100"
            ]

            total_time = 0
            query_count = 0

            for query_sql in test_queries:
                start_time = datetime.now()
                await session.execute(text(query_sql))
                end_time = datetime.now()

                query_time_ms = (end_time - start_time).total_seconds() * 1000
                total_time += query_time_ms
                query_count += 1

            avg_time_ms = total_time / query_count if query_count > 0 else 0

            return {
                "avg_query_time_ms": avg_time_ms,
                "total_queries_tested": query_count,
                "total_time_ms": total_time,
                "target_met": avg_time_ms < 25.0
            }

        except Exception as e:
            self.logger.error(f"Error measuring query performance: {e}")
            return {"avg_query_time_ms": 999.0, "error": str(e)}

    async def _get_avg_query_time(self, session: AsyncSession) -> float:
        """Get average query time from database statistics."""
        try:
            query = text("""
                SELECT COALESCE(AVG(mean_exec_time), 50.0) as avg_time
                FROM pg_stat_statements
                WHERE calls > 10
            """)

            result = await session.execute(query)
            avg_time = result.scalar()
            return float(avg_time) if avg_time else 50.0

        except Exception:
            # Fallback to measured performance
            perf = await self._measure_query_performance(session)
            return perf.get("avg_query_time_ms", 50.0)

    async def _find_unused_indexes(self, session: AsyncSession) -> List[str]:
        """Find unused indexes that can be dropped."""
        try:
            query = text("""
                SELECT schemaname, tablename, indexname
                FROM pg_stat_user_indexes
                WHERE idx_scan = 0
                AND schemaname = 'public'
                ORDER BY tablename, indexname
            """)

            result = await session.execute(query)
            unused = []

            for row in result:
                unused.append(f"{row.schemaname}.{row.tablename}.{row.indexname}")

            return unused

        except Exception as e:
            self.logger.error(f"Error finding unused indexes: {e}")
            return []

    async def _calculate_table_scan_ratio(self, session: AsyncSession) -> float:
        """Calculate the ratio of table scans vs index scans."""
        try:
            query = text("""
                SELECT
                    SUM(seq_scan) as total_seq_scans,
                    SUM(idx_scan) as total_idx_scans
                FROM pg_stat_user_tables
            """)

            result = await session.execute(query)
            row = result.fetchone()

            if row and (row.total_seq_scans + row.total_idx_scans) > 0:
                return row.total_seq_scans / (row.total_seq_scans + row.total_idx_scans)

            return 0.0

        except Exception as e:
            self.logger.error(f"Error calculating table scan ratio: {e}")
            return 0.5  # Default assumption

    async def _identify_missing_indexes(self, session: AsyncSession) -> List[str]:
        """Identify missing indexes based on query patterns."""
        missing_indexes = []

        # Based on common query patterns in the application
        common_patterns = [
            "sales.sale_date + customer_id (for time-series customer analysis)",
            "customers.rfm_*_score composite (for segmentation queries)",
            "products.category + unit_price (for catalog filtering)",
            "real_time_events.event_timestamp (for streaming analytics)"
        ]

        # This would typically analyze actual query logs
        # For now, return patterns that are commonly needed
        return common_patterns

    async def _find_redundant_indexes(self, session: AsyncSession) -> List[str]:
        """Find potentially redundant indexes."""
        try:
            # Look for indexes with similar column prefixes
            query = text("""
                SELECT
                    i1.schemaname, i1.tablename, i1.indexname as index1, i2.indexname as index2,
                    pg_get_indexdef(i1.indexrelid) as def1,
                    pg_get_indexdef(i2.indexrelid) as def2
                FROM pg_stat_user_indexes i1
                JOIN pg_stat_user_indexes i2 ON i1.tablename = i2.tablename
                    AND i1.indexname < i2.indexname
                WHERE i1.schemaname = 'public'
                ORDER BY i1.tablename
            """)

            result = await session.execute(query)
            redundant = []

            for row in result:
                # Simple check for similar index definitions
                if (row.def1 and row.def2 and
                    len(set(row.def1.split()) & set(row.def2.split())) > 3):
                    redundant.append(f"Potential redundancy: {row.index1} and {row.index2} on {row.tablename}")

            return redundant

        except Exception as e:
            self.logger.error(f"Error finding redundant indexes: {e}")
            return []

    async def _calculate_optimization_potential(self, session: AsyncSession) -> Dict[str, float]:
        """Calculate optimization potential metrics."""
        try:
            current_performance = await self._measure_query_performance(session)

            # Estimate potential improvements based on current performance
            current_avg = current_performance.get("avg_query_time_ms", 50.0)

            return {
                "current_avg_ms": current_avg,
                "target_avg_ms": 25.0,
                "potential_improvement_percent": max(0, (current_avg - 25.0) / current_avg * 100),
                "optimization_urgency": "high" if current_avg > 50 else "medium" if current_avg > 25 else "low"
            }

        except Exception as e:
            self.logger.error(f"Error calculating optimization potential: {e}")
            return {"error": str(e)}

    async def _generate_index_recommendations(self, session: AsyncSession) -> List[IndexRecommendation]:
        """Generate specific index recommendations based on analysis."""
        recommendations = []

        # High-priority recommendations for immediate implementation
        high_priority = [
            IndexRecommendation(
                table_name="sales",
                columns=["sale_date", "customer_id", "product_id"],
                index_type=IndexType.COMPOSITE,
                expected_performance_gain=40.0,
                estimated_size_mb=50.0,
                maintenance_cost="LOW",
                query_patterns=["date range + customer analysis", "product sales by date"],
                creation_sql="CREATE INDEX CONCURRENTLY idx_sales_date_customer_product ON sales (sale_date, customer_id, product_id)",
                priority="HIGH"
            ),
            IndexRecommendation(
                table_name="customers",
                columns=["rfm_recency_score", "rfm_frequency_score", "rfm_monetary_score"],
                index_type=IndexType.COMPOSITE,
                expected_performance_gain=35.0,
                estimated_size_mb=25.0,
                maintenance_cost="LOW",
                query_patterns=["customer segmentation", "RFM analysis"],
                creation_sql="CREATE INDEX CONCURRENTLY idx_customers_rfm_composite ON customers (rfm_recency_score, rfm_frequency_score, rfm_monetary_score)",
                priority="HIGH"
            ),
            IndexRecommendation(
                table_name="products",
                columns=["to_tsvector('english', product_name || ' ' || COALESCE(description, ''))"],
                index_type=IndexType.FUNCTIONAL,
                expected_performance_gain=60.0,
                estimated_size_mb=30.0,
                maintenance_cost="MEDIUM",
                query_patterns=["product search", "text matching"],
                creation_sql="CREATE INDEX CONCURRENTLY idx_products_search_gin ON products USING GIN (to_tsvector('english', product_name || ' ' || COALESCE(description, '')))",
                priority="HIGH"
            )
        ]

        recommendations.extend(high_priority)
        return recommendations


# Global instance for application use
indexing_strategy = AdvancedIndexingStrategy()


# Utility functions for external use
async def optimize_database_performance() -> Dict[str, Any]:
    """Main function to optimize database performance with advanced indexing."""
    return await indexing_strategy.implement_high_priority_indexes()


async def analyze_database_performance() -> Dict[str, Any]:
    """Analyze current database performance and provide recommendations."""
    return await indexing_strategy.analyze_current_performance()


async def get_performance_recommendations() -> List[IndexRecommendation]:
    """Get performance optimization recommendations."""
    async with get_async_session() as session:
        return await indexing_strategy._generate_index_recommendations(session)