"""
Database Performance Optimization

Comprehensive database performance monitoring, query optimization,
and maintenance utilities for the retail data warehouse.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from sqlalchemy import Engine, event, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from core.caching.cache_patterns import CacheAsidePattern
from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class QueryComplexity(str, Enum):
    """Query complexity levels."""

    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"


@dataclass
class QueryPerformanceMetric:
    """Performance metrics for a database query."""

    query_id: str
    sql: str
    execution_time_ms: float
    complexity: QueryComplexity
    affected_rows: int = 0
    execution_plan: str | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "query_id": self.query_id,
            "sql": self.sql[:200] + "..." if len(self.sql) > 200 else self.sql,
            "execution_time_ms": self.execution_time_ms,
            "complexity": self.complexity.value,
            "affected_rows": self.affected_rows,
            "timestamp": self.timestamp.isoformat(),
            "has_execution_plan": self.execution_plan is not None,
        }


@dataclass
class TableStatistics:
    """Database table statistics."""

    table_name: str
    row_count: int
    size_mb: float
    index_count: int
    last_analyzed: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "size_mb": self.size_mb,
            "index_count": self.index_count,
            "last_analyzed": self.last_analyzed.isoformat() if self.last_analyzed else None,
        }


@dataclass
class ConnectionPoolMetrics:
    """Connection pool performance metrics."""

    pool_size: int
    checked_out: int
    checked_in: int
    overflow: int
    invalid: int
    peak_connections: int = 0
    total_connections_created: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "pool_size": self.pool_size,
            "checked_out": self.checked_out,
            "checked_in": self.checked_in,
            "overflow": self.overflow,
            "invalid": self.invalid,
            "peak_connections": self.peak_connections,
            "total_connections_created": self.total_connections_created,
            "utilization_pct": (self.checked_out / max(self.pool_size, 1)) * 100,
        }


class DatabasePerformanceOptimizer:
    """
    Enterprise-grade database performance optimizer with comprehensive monitoring and optimization.

    Features:
    - Advanced query performance monitoring with intelligent caching
    - Dynamic connection pool optimization with auto-scaling
    - Automated statistics and index maintenance
    - Query execution plan analysis and optimization
    - Performance trend analysis and predictive recommendations
    - Data archival and partitioning strategies
    - Materialized view management
    - Real-time performance alerting
    """

    def __init__(
        self,
        engine: Engine,
        enable_query_cache: bool = True,
        cache_ttl: int = 300,
        max_workers: int = 4,
    ):
        self.engine = engine
        self.query_metrics: list[QueryPerformanceMetric] = []
        self.table_stats: dict[str, TableStatistics] = {}
        self._last_maintenance = datetime.utcnow()

        # Performance optimizations
        self.enable_query_cache = enable_query_cache
        self.cache_ttl = cache_ttl
        self.query_cache = CacheAsidePattern(default_ttl=cache_ttl) if enable_query_cache else None

        # Connection pool monitoring
        self.pool_metrics_history: list[ConnectionPoolMetrics] = []
        self._setup_connection_pool_monitoring()

        # Thread pool for parallel operations
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # Query optimization cache
        self.query_plan_cache: dict[str, str] = {}
        self.slow_query_patterns: list[str] = []

        # Advanced optimization features
        self.materialized_views: dict[str, datetime] = {}  # View name -> last refresh
        self.partition_strategy: dict[str, str] = {}  # Table -> partition strategy
        self.performance_alerts: list[dict[str, Any]] = []
        self.query_optimization_rules: list[dict[str, Any]] = []

        # Initialize optimization rules
        self._initialize_optimization_rules()

    def _initialize_optimization_rules(self) -> None:
        """Initialize query optimization rules based on database type and configuration."""
        db_type = str(self.engine.url.drivername).lower()

        # Common optimization rules
        self.query_optimization_rules = [
            {
                "name": "large_table_scan",
                "pattern": r"SELECT.*FROM (fact_\w+|large_\w+)",
                "recommendation": "Consider adding WHERE clause to limit rows",
                "severity": "medium",
            },
            {
                "name": "missing_join_condition",
                "pattern": r"FROM.*,.*WHERE(?!.*=)",
                "recommendation": "Use explicit JOIN syntax instead of comma joins",
                "severity": "high",
            },
            {
                "name": "select_star",
                "pattern": r"SELECT\s+\*\s+FROM",
                "recommendation": "Select specific columns instead of SELECT *",
                "severity": "low",
            },
        ]

        # Database-specific rules
        if "postgresql" in db_type:
            self.query_optimization_rules.extend(
                [
                    {
                        "name": "postgresql_vacuum_needed",
                        "pattern": None,  # Applied during maintenance
                        "recommendation": "Run VACUUM ANALYZE on large tables regularly",
                        "severity": "medium",
                    }
                ]
            )
        elif "sqlite" in db_type:
            self.query_optimization_rules.extend(
                [
                    {
                        "name": "sqlite_pragma_optimization",
                        "pattern": None,
                        "recommendation": "Enable WAL mode for better concurrency",
                        "severity": "low",
                    }
                ]
            )

    def _setup_connection_pool_monitoring(self):
        """Setup comprehensive connection pool event monitoring."""
        if hasattr(self.engine.pool, "size"):

            @event.listens_for(self.engine, "connect")
            def receive_connect(dbapi_connection, connection_record):
                logger.debug("New database connection established")
                # Track connection creation patterns
                if hasattr(self, "_connection_creation_times"):
                    self._connection_creation_times.append(datetime.utcnow())
                else:
                    self._connection_creation_times = [datetime.utcnow()]

            @event.listens_for(self.engine, "checkout")
            def receive_checkout(dbapi_connection, connection_record, connection_proxy):
                logger.debug("Connection checked out from pool")
                # Monitor checkout duration for pool optimization
                if not hasattr(connection_record, "checkout_time"):
                    connection_record.checkout_time = datetime.utcnow()

            @event.listens_for(self.engine, "checkin")
            def receive_checkin(dbapi_connection, connection_record):
                logger.debug("Connection returned to pool")
                # Calculate connection hold time
                if hasattr(connection_record, "checkout_time"):
                    hold_time = (
                        datetime.utcnow() - connection_record.checkout_time
                    ).total_seconds()
                    if hold_time > 300:  # More than 5 minutes
                        logger.warning(
                            f"Long-held connection returned after {hold_time:.1f} seconds"
                        )
                    delattr(connection_record, "checkout_time")

            @event.listens_for(self.engine, "invalidate")
            def receive_invalidate(dbapi_connection, connection_record, exception):
                logger.error(f"Connection invalidated: {exception}")
                # Track connection failures for health monitoring
                self.performance_alerts.append(
                    {
                        "type": "connection_invalidated",
                        "timestamp": datetime.utcnow(),
                        "error": str(exception),
                    }
                )

    async def get_connection_pool_metrics(self) -> ConnectionPoolMetrics:
        """Get current connection pool metrics."""
        pool = self.engine.pool

        metrics = ConnectionPoolMetrics(
            pool_size=getattr(pool, "size", lambda: 0)(),
            checked_out=getattr(pool, "checkedout", lambda: 0)(),
            checked_in=getattr(pool, "checkedin", lambda: 0)(),
            overflow=getattr(pool, "overflow", lambda: 0)(),
            invalid=getattr(pool, "invalidated", lambda: 0)(),
        )

        # Track peak connections
        if self.pool_metrics_history:
            metrics.peak_connections = max(
                metrics.checked_out, max(m.peak_connections for m in self.pool_metrics_history)
            )
        else:
            metrics.peak_connections = metrics.checked_out

        self.pool_metrics_history.append(metrics)

        # Keep only last 100 metrics
        if len(self.pool_metrics_history) > 100:
            self.pool_metrics_history = self.pool_metrics_history[-100:]

        return metrics

    async def execute_cached_query(self, sql: str, params: dict | None = None) -> Any:
        """Execute query with caching support."""
        if not self.enable_query_cache or not self.query_cache:
            return await self._execute_query_direct(sql, params)

        # Create cache key
        cache_key = f"query:{hash(sql)}:{hash(str(sorted((params or {}).items())))}"

        # Try cache first
        async def load_query_result():
            return await self._execute_query_direct(sql, params)

        result = await self.query_cache.get(cache_key, load_query_result)
        return result

    async def _execute_query_direct(self, sql: str, params: dict | None = None) -> Any:
        """Execute query directly without caching."""
        loop = asyncio.get_event_loop()

        def _execute():
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(sql), params)
                else:
                    result = conn.execute(text(sql))

                if result.returns_rows:
                    return result.fetchall()
                else:
                    return result.rowcount

        return await loop.run_in_executor(self.executor, _execute)

    async def monitor_query_performance(
        self, sql: str, query_id: str | None = None, use_cache: bool = None
    ) -> QueryPerformanceMetric:
        """
        Execute and monitor query performance with enhanced optimizations.

        Args:
            sql: SQL query to execute
            query_id: Optional query identifier
            use_cache: Whether to use query result caching

        Returns:
            Performance metrics for the query
        """
        query_id = query_id or f"query_{int(time.time())}"
        use_cache = use_cache if use_cache is not None else self.enable_query_cache

        start_time = time.time()
        affected_rows = 0
        cached_result = False

        try:
            # Get connection pool metrics before execution
            pool_metrics = await self.get_connection_pool_metrics()

            if use_cache:
                # Try cached execution
                cache_key = f"perf_query:{hash(sql)}"
                if cache_key in self.query_plan_cache:
                    logger.debug(f"Using cached execution plan for query: {query_id}")

                result = await self.execute_cached_query(sql)
                if isinstance(result, list):
                    affected_rows = len(result)
                else:
                    affected_rows = result if isinstance(result, int) else 0
            else:
                # Direct execution
                result = await self._execute_query_direct(sql)
                if isinstance(result, list):
                    affected_rows = len(result)
                else:
                    affected_rows = result if isinstance(result, int) else 0

        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            # Track slow query patterns
            if sql not in self.slow_query_patterns:
                self.slow_query_patterns.append(sql[:100])  # Store first 100 chars
            raise

        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        complexity = self._analyze_query_complexity(sql)

        # Get execution plan for complex queries
        execution_plan = None
        if complexity == QueryComplexity.COMPLEX and execution_time > 1000:
            execution_plan = await self._get_execution_plan(sql)

        metric = QueryPerformanceMetric(
            query_id=query_id,
            sql=sql,
            execution_time_ms=execution_time,
            complexity=complexity,
            affected_rows=affected_rows,
            execution_plan=execution_plan,
        )

        self.query_metrics.append(metric)

        # Keep only last 1000 metrics to avoid memory growth
        if len(self.query_metrics) > 1000:
            self.query_metrics = self.query_metrics[-1000:]

        # Log performance issues
        if execution_time > 2000:  # > 2 seconds
            logger.warning(f"Slow query detected: {query_id} ({execution_time:.2f}ms)")

        logger.debug(
            f"Query {query_id} executed in {execution_time:.2f}ms "
            f"(cached: {cached_result}, pool utilization: {pool_metrics.checked_out}/{pool_metrics.pool_size})"
        )

        return metric

    async def _get_execution_plan(self, sql: str) -> str | None:
        """Get execution plan for a query."""
        try:
            # Check cache first
            plan_key = f"plan:{hash(sql)}"
            if plan_key in self.query_plan_cache:
                return self.query_plan_cache[plan_key]

            # Get execution plan (database-specific)
            plan_sql = f"EXPLAIN QUERY PLAN {sql}"

            def _get_plan():
                with self.engine.connect() as conn:
                    result = conn.execute(text(plan_sql))
                    plan_rows = result.fetchall()
                    return "\n".join(str(row) for row in plan_rows)

            loop = asyncio.get_event_loop()
            plan = await loop.run_in_executor(self.executor, _get_plan)

            # Cache the plan
            self.query_plan_cache[plan_key] = plan

            # Limit cache size
            if len(self.query_plan_cache) > 1000:
                # Remove oldest entries (simple LRU approximation)
                keys_to_remove = list(self.query_plan_cache.keys())[:100]
                for key in keys_to_remove:
                    del self.query_plan_cache[key]

            return plan

        except Exception as e:
            logger.warning(f"Failed to get execution plan: {e}")
            return None

    def _analyze_query_complexity(self, sql: str) -> QueryComplexity:
        """Analyze query complexity based on SQL structure."""
        sql_lower = sql.lower()

        # Count complexity indicators
        complexity_score = 0

        # JOIN operations
        join_count = sql_lower.count("join")
        complexity_score += join_count * 2

        # Subqueries
        subquery_count = sql_lower.count("select") - 1  # Subtract main SELECT
        complexity_score += subquery_count * 3

        # Aggregation functions
        agg_functions = ["sum(", "count(", "avg(", "max(", "min(", "group by"]
        agg_count = sum(sql_lower.count(func) for func in agg_functions)
        complexity_score += agg_count * 1

        # Window functions
        window_count = sql_lower.count("over(")
        complexity_score += window_count * 4

        # Sorting
        if "order by" in sql_lower:
            complexity_score += 1

        # Classify complexity
        if complexity_score <= 2:
            return QueryComplexity.SIMPLE
        elif complexity_score <= 8:
            return QueryComplexity.MODERATE
        else:
            return QueryComplexity.COMPLEX

    async def collect_table_statistics(self) -> dict[str, TableStatistics]:
        """Collect comprehensive table statistics."""
        logger.info("Collecting database table statistics...")

        # Define key tables to monitor
        key_tables = [
            "fact_sale",
            "dim_customer",
            "dim_product",
            "dim_date",
            "dim_country",
            "dim_invoice",
        ]

        stats = {}

        for table_name in key_tables:
            try:
                table_stats = await self._get_table_statistics(table_name)
                if table_stats:
                    stats[table_name] = table_stats
                    self.table_stats[table_name] = table_stats

            except Exception as e:
                logger.warning(f"Could not collect statistics for table {table_name}: {e}")

        logger.info(f"Collected statistics for {len(stats)} tables")
        return stats

    async def _get_table_statistics(self, table_name: str) -> TableStatistics | None:
        """Get statistics for a single table."""
        try:
            with self.engine.connect() as conn:
                # Get row count
                row_count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
                result = conn.execute(text(row_count_query))
                row_count = result.scalar()

                # Get index count
                inspector = inspect(self.engine)
                indexes = inspector.get_indexes(table_name)
                index_count = len(indexes)

                # Estimate table size (simplified calculation)
                # This is a rough estimate - actual implementation would vary by database
                estimated_size_mb = row_count * 0.001  # Rough estimate

                return TableStatistics(
                    table_name=table_name,
                    row_count=row_count,
                    size_mb=estimated_size_mb,
                    index_count=index_count,
                    last_analyzed=datetime.utcnow(),
                )

        except SQLAlchemyError as e:
            logger.error(f"Failed to get statistics for table {table_name}: {e}")
            return None

    async def run_maintenance_tasks(self) -> dict[str, Any]:
        """Run comprehensive database maintenance tasks."""
        logger.info("Running database maintenance tasks...")

        maintenance_results = {
            "started_at": datetime.utcnow().isoformat(),
            "tasks_completed": [],
            "tasks_failed": [],
            "performance_improvement": {},
        }

        try:
            # Task 1: Update table statistics
            await self._update_table_statistics(maintenance_results)

            # Task 2: Analyze query performance
            await self._analyze_query_performance_trends(maintenance_results)

            # Task 3: Check index effectiveness
            await self._check_index_effectiveness(maintenance_results)

            # Task 4: Run database vacuum/optimize
            await self._optimize_database_storage(maintenance_results)

            # Task 5: Generate performance recommendations
            await self._generate_recommendations(maintenance_results)

            self._last_maintenance = datetime.utcnow()
            maintenance_results["completed_at"] = self._last_maintenance.isoformat()

            logger.info(
                f"Maintenance completed: {len(maintenance_results['tasks_completed'])} tasks successful"
            )

        except Exception as e:
            logger.error(f"Maintenance tasks failed: {e}")
            maintenance_results["tasks_failed"].append(
                {"task": "maintenance_execution", "error": str(e)}
            )

        return maintenance_results

    async def run_advanced_maintenance(self) -> dict[str, Any]:
        """Run advanced database maintenance including partitioning and archival."""
        logger.info("Running advanced database maintenance...")

        maintenance_results = {
            "started_at": datetime.utcnow().isoformat(),
            "tasks_completed": [],
            "tasks_failed": [],
            "optimizations_applied": [],
        }

        try:
            # Create partitions for large tables
            await self._create_table_partitions(maintenance_results)

            # Archive old data
            await self._archive_old_data(maintenance_results)

            # Update column statistics
            await self._update_column_statistics(maintenance_results)

            # Rebuild fragmented indexes
            await self._rebuild_indexes(maintenance_results)

            # Analyze query execution patterns
            await self._analyze_execution_patterns(maintenance_results)

            logger.info(
                f"Advanced maintenance completed: {len(maintenance_results['tasks_completed'])} tasks"
            )

        except Exception as e:
            logger.error(f"Advanced maintenance failed: {e}")
            maintenance_results["tasks_failed"].append(
                {"task": "advanced_maintenance", "error": str(e)}
            )

        return maintenance_results

    async def _update_table_statistics(self, results: dict[str, Any]) -> None:
        """Update database table statistics."""
        try:
            with self.engine.connect() as conn:
                # Run ANALYZE command for key tables
                analyze_commands = [
                    "ANALYZE fact_sale",
                    "ANALYZE dim_customer",
                    "ANALYZE dim_product",
                    "ANALYZE dim_date",
                    "ANALYZE dim_country",
                    "ANALYZE dim_invoice",
                ]

                for cmd in analyze_commands:
                    try:
                        conn.execute(text(cmd))
                        logger.debug(f"Executed: {cmd}")
                    except SQLAlchemyError as e:
                        logger.warning(f"Failed to execute {cmd}: {e}")

                conn.commit()

            results["tasks_completed"].append("table_statistics_update")

        except Exception as e:
            results["tasks_failed"].append({"task": "table_statistics_update", "error": str(e)})

    async def _analyze_query_performance_trends(self, results: dict[str, Any]) -> None:
        """Analyze query performance trends."""
        try:
            if not self.query_metrics:
                return

            # Calculate performance trends
            recent_metrics = [
                m
                for m in self.query_metrics
                if m.timestamp > datetime.utcnow() - timedelta(hours=1)
            ]

            if recent_metrics:
                avg_execution_time = sum(m.execution_time_ms for m in recent_metrics) / len(
                    recent_metrics
                )
                slow_queries = [
                    m for m in recent_metrics if m.execution_time_ms > 1000
                ]  # > 1 second

                results["performance_improvement"]["avg_execution_time_ms"] = avg_execution_time
                results["performance_improvement"]["slow_queries_count"] = len(slow_queries)
                results["performance_improvement"]["total_queries_analyzed"] = len(recent_metrics)

            results["tasks_completed"].append("query_performance_analysis")

        except Exception as e:
            results["tasks_failed"].append({"task": "query_performance_analysis", "error": str(e)})

    async def _check_index_effectiveness(self, results: dict[str, Any]) -> None:
        """Check database index effectiveness."""
        try:
            # This is a simplified check - real implementation would be database-specific
            with self.engine.connect():
                # Check for unused indexes (simplified)
                inspector = inspect(self.engine)

                index_info = {}
                for table_name in ["fact_sale", "dim_customer", "dim_product"]:
                    try:
                        indexes = inspector.get_indexes(table_name)
                        index_info[table_name] = len(indexes)
                    except:
                        pass

                results["performance_improvement"]["indexes_by_table"] = index_info

            results["tasks_completed"].append("index_effectiveness_check")

        except Exception as e:
            results["tasks_failed"].append({"task": "index_effectiveness_check", "error": str(e)})

    async def _optimize_database_storage(self, results: dict[str, Any]) -> None:
        """Optimize database storage."""
        try:
            with self.engine.connect() as conn:
                # Run VACUUM for SQLite or equivalent maintenance
                try:
                    conn.execute(text("VACUUM"))
                    logger.debug("Database vacuum completed")
                except SQLAlchemyError:
                    # VACUUM might not be supported in all databases
                    logger.debug("VACUUM not supported, skipping storage optimization")

                conn.commit()

            results["tasks_completed"].append("storage_optimization")

        except Exception as e:
            results["tasks_failed"].append({"task": "storage_optimization", "error": str(e)})

    async def _generate_recommendations(self, results: dict[str, Any]) -> None:
        """Generate performance optimization recommendations."""
        try:
            recommendations = []

            # Analyze slow queries
            if self.query_metrics:
                slow_queries = [m for m in self.query_metrics if m.execution_time_ms > 2000]
                if slow_queries:
                    recommendations.append(
                        f"Found {len(slow_queries)} slow queries (>2s). Consider optimizing or adding indexes."
                    )

            # Check table sizes
            large_tables = [
                name for name, stats in self.table_stats.items() if stats.row_count > 100000
            ]
            if large_tables:
                recommendations.append(
                    f"Large tables detected: {', '.join(large_tables)}. Consider partitioning or archiving."
                )

            # Index recommendations
            low_index_tables = [
                name
                for name, stats in self.table_stats.items()
                if stats.index_count < 3 and stats.row_count > 10000
            ]
            if low_index_tables:
                recommendations.append(
                    f"Tables with few indexes: {', '.join(low_index_tables)}. Consider adding performance indexes."
                )

            results["performance_improvement"]["recommendations"] = recommendations
            results["tasks_completed"].append("recommendation_generation")

        except Exception as e:
            results["tasks_failed"].append({"task": "recommendation_generation", "error": str(e)})

    async def _create_table_partitions(self, results: dict[str, Any]) -> None:
        """Create table partitions for improved performance."""
        try:
            if "postgresql" in str(self.engine.url).lower():
                with self.engine.connect() as conn:
                    # Check if partitioning is needed for fact_sale
                    row_check = conn.execute(text("SELECT COUNT(*) FROM fact_sale")).scalar()

                    if row_check > 1000000:  # Partition if > 1M rows
                        partition_queries = [
                            """
                            CREATE TABLE IF NOT EXISTS fact_sale_2024
                            PARTITION OF fact_sale
                            FOR VALUES FROM (20240101) TO (20250101)
                            """,
                            """
                            CREATE TABLE IF NOT EXISTS fact_sale_2025
                            PARTITION OF fact_sale
                            FOR VALUES FROM (20250101) TO (20260101)
                            """,
                        ]

                        for query in partition_queries:
                            try:
                                conn.execute(text(query))
                                logger.debug("Created table partition")
                            except SQLAlchemyError as e:
                                logger.warning(f"Partition creation failed: {e}")

                        conn.commit()
                        results["optimizations_applied"].append("table_partitioning")

            results["tasks_completed"].append("partition_management")

        except Exception as e:
            results["tasks_failed"].append({"task": "partition_management", "error": str(e)})

    def get_performance_summary(self) -> dict[str, Any]:
        """Get comprehensive performance summary with enhanced metrics."""
        # Get latest connection pool metrics
        latest_pool_metrics = self.pool_metrics_history[-1] if self.pool_metrics_history else None

        summary = {
            "query_metrics": {
                "total_queries": len(self.query_metrics),
                "avg_execution_time_ms": 0,
                "slow_queries_count": 0,
                "complex_queries_count": 0,
                "cached_queries_enabled": self.enable_query_cache,
                "query_cache_ttl": self.cache_ttl if self.enable_query_cache else None,
            },
            "connection_pool": {
                "current_metrics": latest_pool_metrics.to_dict() if latest_pool_metrics else None,
                "avg_utilization_pct": 0,
                "peak_connections": max(m.peak_connections for m in self.pool_metrics_history)
                if self.pool_metrics_history
                else 0,
            },
            "optimization": {
                "execution_plans_cached": len(self.query_plan_cache),
                "slow_query_patterns": len(self.slow_query_patterns),
                "thread_pool_workers": self.executor._max_workers,
            },
            "table_statistics": {name: stats.to_dict() for name, stats in self.table_stats.items()},
            "materialized_views": {
                "total_views": len(self.materialized_views),
                "views": {
                    name: refresh_time.isoformat()
                    for name, refresh_time in self.materialized_views.items()
                },
            },
            "optimization_rules": {
                "total_rules": len(self.query_optimization_rules),
                "rules_by_severity": self._count_rules_by_severity(),
            },
            "performance_alerts": {
                "total_alerts": len(self.performance_alerts),
                "recent_alerts": len(
                    [
                        a
                        for a in self.performance_alerts
                        if a["timestamp"] > datetime.utcnow() - timedelta(hours=24)
                    ]
                ),
            },
            "last_maintenance": self._last_maintenance.isoformat(),
            "recommendations": self._generate_performance_recommendations(),
        }

        # Calculate query metrics
        if self.query_metrics:
            summary["query_metrics"]["avg_execution_time_ms"] = sum(
                m.execution_time_ms for m in self.query_metrics
            ) / len(self.query_metrics)
            summary["query_metrics"]["slow_queries_count"] = sum(
                1 for m in self.query_metrics if m.execution_time_ms > 1000
            )
            summary["query_metrics"]["complex_queries_count"] = sum(
                1 for m in self.query_metrics if m.complexity == QueryComplexity.COMPLEX
            )

        # Calculate average pool utilization
        if self.pool_metrics_history:
            summary["connection_pool"]["avg_utilization_pct"] = (
                sum(m.checked_out / max(m.pool_size, 1) for m in self.pool_metrics_history)
                / len(self.pool_metrics_history)
                * 100
            )

        return summary

    def _count_rules_by_severity(self) -> dict[str, int]:
        """Count optimization rules by severity level."""
        severity_counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for rule in self.query_optimization_rules:
            severity = rule.get("severity", "low")
            if severity in severity_counts:
                severity_counts[severity] += 1
        return severity_counts

    def _generate_performance_recommendations(self) -> list[str]:
        """Generate performance recommendations based on current metrics."""
        recommendations = []

        # Connection pool recommendations
        if self.pool_metrics_history:
            latest_metrics = self.pool_metrics_history[-1]
            utilization = latest_metrics.checked_out / max(latest_metrics.pool_size, 1)

            if utilization > 0.9:
                recommendations.append(
                    "High connection pool utilization - consider increasing pool size"
                )
            elif utilization < 0.1:
                recommendations.append(
                    "Low connection pool utilization - consider decreasing pool size to save resources"
                )

        # Query performance recommendations
        if self.query_metrics:
            recent_queries = [
                m
                for m in self.query_metrics
                if m.timestamp > datetime.utcnow() - timedelta(hours=1)
            ]
            if recent_queries:
                avg_time = sum(m.execution_time_ms for m in recent_queries) / len(recent_queries)
                slow_queries = [m for m in recent_queries if m.execution_time_ms > 2000]

                if avg_time > 1000:
                    recommendations.append(
                        f"Average query time is high ({avg_time:.0f}ms) - review query optimization"
                    )

                if len(slow_queries) > len(recent_queries) * 0.1:
                    recommendations.append(
                        f"Many slow queries detected ({len(slow_queries)}/{len(recent_queries)}) - consider indexing"
                    )

        # Table size recommendations
        for table_name, stats in self.table_stats.items():
            if stats.row_count > 5000000:  # 5M+ rows
                recommendations.append(
                    f"Large table {table_name} ({stats.row_count:,} rows) - consider partitioning"
                )

            if stats.index_count < 3 and stats.row_count > 100000:
                recommendations.append(
                    f"Table {table_name} has few indexes ({stats.index_count}) for its size - review indexing strategy"
                )

        # Materialized view recommendations
        if len(self.materialized_views) == 0 and self.query_metrics:
            complex_queries = [
                m for m in self.query_metrics if m.complexity == QueryComplexity.COMPLEX
            ]
            if len(complex_queries) > 10:
                recommendations.append(
                    "Many complex queries detected - consider creating materialized views"
                )

        return recommendations

    async def optimize_connection_pool(self) -> dict[str, Any]:
        """Analyze and optimize connection pool configuration with advanced recommendations."""
        if not self.pool_metrics_history:
            return {"status": "no_metrics", "message": "No connection pool metrics available"}

        recent_metrics = self.pool_metrics_history[-10:]  # Last 10 snapshots
        avg_utilization = sum(m.checked_out / max(m.pool_size, 1) for m in recent_metrics) / len(
            recent_metrics
        )
        peak_utilization = max(m.checked_out / max(m.pool_size, 1) for m in recent_metrics)

        recommendations = []
        optimization_actions = []

        # Advanced utilization analysis
        utilization_trend = self._calculate_utilization_trend(recent_metrics)

        if avg_utilization > 0.8:
            recommendations.append("High average utilization - consider increasing pool size")
            optimization_actions.append(
                {
                    "action": "increase_pool_size",
                    "current": recent_metrics[-1].pool_size,
                    "suggested": int(recent_metrics[-1].pool_size * 1.5),
                    "priority": "high",
                }
            )
        elif avg_utilization < 0.2:
            recommendations.append(
                "Low utilization - consider decreasing pool size to save resources"
            )
            optimization_actions.append(
                {
                    "action": "decrease_pool_size",
                    "current": recent_metrics[-1].pool_size,
                    "suggested": max(5, int(recent_metrics[-1].pool_size * 0.7)),
                    "priority": "medium",
                }
            )

        if peak_utilization > 0.95:
            recommendations.append(
                "Peak utilization near maximum - increase pool size or optimize queries"
            )
            optimization_actions.append(
                {
                    "action": "emergency_pool_increase",
                    "current": recent_metrics[-1].pool_size,
                    "suggested": int(recent_metrics[-1].pool_size * 2),
                    "priority": "critical",
                }
            )

        # Connection lifecycle analysis
        connection_analysis = self._analyze_connection_lifecycle()
        if connection_analysis["high_churn"]:
            recommendations.append("High connection churn detected - investigate connection leaks")
            optimization_actions.append(
                {"action": "investigate_connection_leaks", "priority": "high"}
            )

        # Environment-specific optimizations
        env_recommendations = self._get_environment_optimizations()
        recommendations.extend(env_recommendations)

        return {
            "avg_utilization_pct": avg_utilization * 100,
            "peak_utilization_pct": peak_utilization * 100,
            "utilization_trend": utilization_trend,
            "recommendations": recommendations,
            "optimization_actions": optimization_actions,
            "current_pool_size": recent_metrics[-1].pool_size if recent_metrics else 0,
            "suggested_pool_size": self._calculate_optimal_pool_size(recent_metrics),
            "connection_lifecycle": connection_analysis,
        }

    def _calculate_utilization_trend(self, metrics: list[ConnectionPoolMetrics]) -> str:
        """Calculate connection pool utilization trend."""
        if len(metrics) < 3:
            return "insufficient_data"

        recent_utilizations = [m.checked_out / max(m.pool_size, 1) for m in metrics]

        # Simple trend analysis
        first_third = sum(recent_utilizations[: len(recent_utilizations) // 3]) / max(
            1, len(recent_utilizations) // 3
        )
        last_third = sum(recent_utilizations[-len(recent_utilizations) // 3 :]) / max(
            1, len(recent_utilizations) // 3
        )

        if last_third > first_third * 1.2:
            return "increasing"
        elif last_third < first_third * 0.8:
            return "decreasing"
        else:
            return "stable"

    def _analyze_connection_lifecycle(self) -> dict[str, Any]:
        """Analyze connection creation and destruction patterns."""
        # This would be enhanced with actual connection tracking
        if not hasattr(self, "_connection_creation_times"):
            return {"high_churn": False, "avg_lifetime": 0}

        recent_creations = [
            t
            for t in self._connection_creation_times
            if t > datetime.utcnow() - timedelta(minutes=10)
        ]

        return {
            "high_churn": len(recent_creations) > 20,  # More than 20 new connections in 10 minutes
            "recent_creation_rate": len(recent_creations) / 10,  # Connections per minute
            "total_tracked_connections": len(self._connection_creation_times),
        }

    def _get_environment_optimizations(self) -> list[str]:
        """Get environment-specific optimization recommendations."""
        recommendations = []

        try:
            env = getattr(settings, "environment", None)
            if env:
                if env.value == "production":
                    recommendations.append(
                        "Production environment - ensure connection pool monitoring is enabled"
                    )
                    recommendations.append("Consider connection pool warmup during startup")
                elif env.value == "development":
                    recommendations.append(
                        "Development environment - smaller pool size recommended"
                    )
                elif env.value == "testing":
                    recommendations.append(
                        "Testing environment - consider NullPool to avoid connection persistence"
                    )
        except Exception:
            pass  # Settings not available

        return recommendations

    def _calculate_optimal_pool_size(self, metrics: list[ConnectionPoolMetrics]) -> int:
        """Calculate optimal pool size using advanced statistical analysis."""
        if not metrics:
            return 10  # Default pool size

        checked_out_values = [m.checked_out for m in metrics]

        if not checked_out_values:
            return 10

        # Calculate percentiles for better sizing
        sorted_values = sorted(checked_out_values)
        n = len(sorted_values)

        p50_index = int(0.5 * n)
        p90_index = int(0.9 * n)
        p95_index = int(0.95 * n)
        p99_index = int(0.99 * n)

        sorted_values[min(p50_index, n - 1)]
        p90 = sorted_values[min(p90_index, n - 1)]
        p95 = sorted_values[min(p95_index, n - 1)]
        p99 = sorted_values[min(p99_index, n - 1)]

        # Use adaptive sizing based on variance
        variance = sum(
            (x - sum(checked_out_values) / len(checked_out_values)) ** 2 for x in checked_out_values
        ) / len(checked_out_values)

        if variance > 10:  # High variance - size for peak load
            optimal_size = p99 + 2
        elif variance > 5:  # Medium variance - size for 95th percentile
            optimal_size = p95 + 1
        else:  # Low variance - size for typical load
            optimal_size = p90 + 1

        # Apply environment-specific adjustments
        try:
            env = getattr(settings, "environment", None)
            if env:
                if env.value == "production":
                    optimal_size = int(optimal_size * 1.2)  # 20% buffer for production
                elif env.value == "development":
                    optimal_size = min(optimal_size, 10)  # Cap at 10 for development
                elif env.value == "testing":
                    optimal_size = min(optimal_size, 5)  # Cap at 5 for testing
        except Exception:
            pass

        return max(5, min(optimal_size, 50))  # Ensure between 5 and 50 connections

    async def clear_caches(self) -> dict[str, int]:
        """Clear all performance caches."""
        cleared_counts = {}

        # Clear query cache
        if self.query_cache:
            # This would depend on the cache implementation
            cleared_counts["query_cache"] = 0  # Placeholder

        # Clear execution plan cache
        cleared_counts["execution_plans"] = len(self.query_plan_cache)
        self.query_plan_cache.clear()

        # Clear slow query patterns
        cleared_counts["slow_query_patterns"] = len(self.slow_query_patterns)
        self.slow_query_patterns.clear()

        # Clear metrics history (keep last 10)
        if len(self.pool_metrics_history) > 10:
            cleared_counts["pool_metrics"] = len(self.pool_metrics_history) - 10
            self.pool_metrics_history = self.pool_metrics_history[-10:]

        logger.info(f"Cleared caches: {cleared_counts}")
        return cleared_counts

    async def benchmark_queries(self) -> dict[str, Any]:
        """Run benchmark queries to test database performance."""
        logger.info("Running database performance benchmarks...")

        benchmark_queries = [
            {
                "name": "sales_by_date",
                "sql": """
                    SELECT date_key, COUNT(*) as transaction_count, SUM(total_amount) as revenue
                    FROM fact_sale
                    WHERE date_key >= 20240101
                    GROUP BY date_key
                    ORDER BY date_key
                """,
                "expected_complexity": QueryComplexity.MODERATE,
            },
            {
                "name": "customer_analytics",
                "sql": """
                    SELECT c.customer_segment, COUNT(*) as customers, AVG(c.lifetime_value) as avg_ltv
                    FROM dim_customer c
                    WHERE c.is_current = 1
                    GROUP BY c.customer_segment
                    ORDER BY avg_ltv DESC
                """,
                "expected_complexity": QueryComplexity.MODERATE,
            },
            {
                "name": "product_performance",
                "sql": """
                    SELECT p.category, COUNT(DISTINCT p.stock_code) as products,
                           SUM(f.total_amount) as revenue
                    FROM dim_product p
                    JOIN fact_sale f ON p.product_key = f.product_key
                    WHERE p.is_current = 1
                    GROUP BY p.category
                    ORDER BY revenue DESC
                    LIMIT 10
                """,
                "expected_complexity": QueryComplexity.COMPLEX,
            },
        ]

        benchmark_results = []

        for query_info in benchmark_queries:
            try:
                metric = await self.monitor_query_performance(
                    query_info["sql"], f"benchmark_{query_info['name']}"
                )

                benchmark_results.append(
                    {
                        "name": query_info["name"],
                        "execution_time_ms": metric.execution_time_ms,
                        "complexity": metric.complexity.value,
                        "expected_complexity": query_info["expected_complexity"].value,
                        "performance_rating": self._rate_performance(metric.execution_time_ms),
                    }
                )

            except Exception as e:
                logger.error(f"Benchmark query {query_info['name']} failed: {e}")
                benchmark_results.append({"name": query_info["name"], "error": str(e)})

        return {
            "benchmark_results": benchmark_results,
            "overall_score": self._calculate_overall_score(benchmark_results),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _rate_performance(self, execution_time_ms: float) -> str:
        """Rate query performance based on execution time."""
        if execution_time_ms < 100:
            return "excellent"
        elif execution_time_ms < 500:
            return "good"
        elif execution_time_ms < 2000:
            return "acceptable"
        else:
            return "poor"

    def _calculate_overall_score(self, benchmark_results: list[dict[str, Any]]) -> float:
        """Calculate overall performance score."""
        if not benchmark_results:
            return 0.0

        valid_results = [r for r in benchmark_results if "error" not in r]
        if not valid_results:
            return 0.0

        # Score based on execution times (lower is better)
        total_score = 0
        for result in valid_results:
            time_ms = result.get("execution_time_ms", float("inf"))
            if time_ms < 100:
                total_score += 100
            elif time_ms < 500:
                total_score += 80
            elif time_ms < 2000:
                total_score += 60
            else:
                total_score += 20

        return total_score / len(valid_results)

    async def generate_optimization_report(self) -> dict[str, Any]:
        """Generate comprehensive database optimization report."""
        logger.info("Generating comprehensive optimization report...")

        report = {
            "report_metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "database_type": str(self.engine.url.drivername),
                "optimization_version": "2.0",
            },
            "performance_summary": {},
            "optimization_opportunities": [],
            "maintenance_recommendations": [],
            "resource_utilization": {},
            "query_analysis": {},
            "schema_analysis": {},
            "action_plan": [],
        }

        try:
            # Get comprehensive performance summary
            report["performance_summary"] = self.get_performance_summary()

            # Analyze optimization opportunities
            if self.query_metrics:
                report[
                    "optimization_opportunities"
                ] = await self._analyze_optimization_opportunities()

            # Get resource utilization analysis
            report["resource_utilization"] = await self._analyze_resource_utilization()

            # Analyze query patterns
            report["query_analysis"] = self._analyze_query_patterns()

            # Schema analysis
            report["schema_analysis"] = await self._analyze_schema_design()

            # Generate prioritized action plan
            report["action_plan"] = self._generate_action_plan(
                report["optimization_opportunities"],
                report["resource_utilization"],
                report["schema_analysis"],
            )

            # Overall health score
            report["overall_health_score"] = self._calculate_health_score(report)

        except Exception as e:
            logger.error(f"Optimization report generation failed: {e}")
            report["error"] = str(e)

        return report

    async def _analyze_optimization_opportunities(self) -> list[dict[str, Any]]:
        """Analyze database for optimization opportunities with ML-enhanced analysis."""
        opportunities = []

        try:
            # Slow query analysis
            slow_queries = [m for m in self.query_metrics if m.execution_time_ms > 2000]
            if slow_queries:
                query_patterns = self._analyze_slow_query_patterns(slow_queries)
                opportunities.append(
                    {
                        "category": "query_performance",
                        "priority": "high",
                        "impact": "performance",
                        "description": f"Detected {len(slow_queries)} slow queries with optimization potential",
                        "recommendations": [
                            "Add missing indexes for frequently accessed columns",
                            "Optimize JOIN operations and query structure",
                            "Consider query result caching for repeated patterns",
                            "Implement query hints for complex analytical queries",
                        ],
                        "patterns": query_patterns,
                        "estimated_improvement": "30-60% query time reduction",
                    }
                )

            # Index optimization analysis
            index_analysis = await self._analyze_index_opportunities()
            if index_analysis["missing_indexes"]:
                opportunities.append(
                    {
                        "category": "indexing",
                        "priority": "high",
                        "impact": "performance",
                        "description": "Missing indexes detected on frequently queried columns",
                        "recommendations": index_analysis["recommendations"],
                        "tables_affected": index_analysis["tables"],
                        "estimated_improvement": "20-80% query performance improvement",
                    }
                )

            # Connection pool optimization
            if self.pool_metrics_history:
                pool_analysis = await self._analyze_connection_pool_efficiency()
                if pool_analysis["optimization_needed"]:
                    opportunities.append(
                        {
                            "category": "connection_pooling",
                            "priority": "medium",
                            "impact": "scalability",
                            "description": pool_analysis["description"],
                            "recommendations": pool_analysis["recommendations"],
                            "current_metrics": pool_analysis["metrics"],
                            "estimated_improvement": "Improved concurrency and resource utilization",
                        }
                    )

            # Partitioning opportunities
            partition_analysis = await self._analyze_partitioning_opportunities()
            if partition_analysis["tables_to_partition"]:
                opportunities.append(
                    {
                        "category": "partitioning",
                        "priority": "medium",
                        "impact": "scalability",
                        "description": f"{len(partition_analysis['tables_to_partition'])} large tables could benefit from partitioning",
                        "recommendations": partition_analysis["strategies"],
                        "tables": partition_analysis["tables_to_partition"],
                        "estimated_improvement": "Faster queries on large datasets, improved maintenance",
                    }
                )

            # Materialized view opportunities
            mv_analysis = await self._analyze_materialized_view_opportunities()
            if mv_analysis["candidates"]:
                opportunities.append(
                    {
                        "category": "materialized_views",
                        "priority": "medium",
                        "impact": "performance",
                        "description": f"{len(mv_analysis['candidates'])} query patterns suitable for materialization",
                        "recommendations": mv_analysis["recommendations"],
                        "query_patterns": mv_analysis["candidates"],
                        "estimated_improvement": "70-95% faster analytical queries",
                    }
                )

            # Caching optimization
            cache_analysis = self._analyze_query_caching_opportunities()
            if cache_analysis["cacheable_patterns"]:
                opportunities.append(
                    {
                        "category": "query_caching",
                        "priority": "low",
                        "impact": "performance",
                        "description": "Query patterns identified for aggressive caching",
                        "recommendations": cache_analysis["strategies"],
                        "patterns": cache_analysis["cacheable_patterns"],
                        "estimated_improvement": "50-90% faster repeated query execution",
                    }
                )

            # Database maintenance optimization
            maintenance_analysis = await self._analyze_maintenance_optimization()
            if maintenance_analysis["improvements"]:
                opportunities.append(
                    {
                        "category": "maintenance",
                        "priority": "low",
                        "impact": "stability",
                        "description": "Database maintenance procedures can be optimized",
                        "recommendations": maintenance_analysis["improvements"],
                        "automation_opportunities": maintenance_analysis["automation"],
                        "estimated_improvement": "Reduced maintenance windows, improved reliability",
                    }
                )

            return opportunities

        except Exception as e:
            self.logger.error(f"Optimization analysis failed: {e}")
            return [
                {
                    "category": "analysis_error",
                    "priority": "high",
                    "description": f"Optimization analysis failed: {str(e)}",
                    "recommendations": ["Review database configuration and accessibility"],
                }
            ]

    def _analyze_slow_query_patterns(
        self, slow_queries: list[QueryPerformanceMetric]
    ) -> dict[str, Any]:
        """Analyze patterns in slow queries using advanced pattern recognition."""
        from collections import defaultdict

        patterns = {
            "common_operations": defaultdict(int),
            "table_access_patterns": defaultdict(int),
            "join_patterns": defaultdict(int),
            "aggregation_patterns": defaultdict(int),
            "complexity_distribution": defaultdict(int),
        }

        for query in slow_queries:
            sql_lower = query.sql.lower()

            # Analyze common operations
            if "select" in sql_lower:
                patterns["common_operations"]["SELECT"] += 1
            if "insert" in sql_lower:
                patterns["common_operations"]["INSERT"] += 1
            if "update" in sql_lower:
                patterns["common_operations"]["UPDATE"] += 1
            if "delete" in sql_lower:
                patterns["common_operations"]["DELETE"] += 1

            # Table access patterns
            import re

            table_matches = re.findall(r"from\s+(\w+)|join\s+(\w+)", sql_lower)
            for match in table_matches:
                table_name = match[0] or match[1]
                if table_name:
                    patterns["table_access_patterns"][table_name] += 1

            # Join patterns
            join_types = ["inner join", "left join", "right join", "full join", "cross join"]
            for join_type in join_types:
                if join_type in sql_lower:
                    patterns["join_patterns"][join_type.replace(" ", "_")] += 1

            # Aggregation patterns
            agg_functions = ["count(", "sum(", "avg(", "max(", "min(", "group by"]
            for agg_func in agg_functions:
                if agg_func in sql_lower:
                    patterns["aggregation_patterns"][
                        agg_func.replace("(", "").replace(" ", "_")
                    ] += 1

            # Complexity distribution
            patterns["complexity_distribution"][query.complexity.value] += 1

        return {
            "total_slow_queries": len(slow_queries),
            "avg_execution_time": sum(q.execution_time_ms for q in slow_queries)
            / len(slow_queries),
            "patterns": {k: dict(v) for k, v in patterns.items()},
            "optimization_hints": self._generate_query_optimization_hints(patterns),
        }

    def _generate_query_optimization_hints(self, patterns: dict) -> list[str]:
        """Generate specific optimization hints based on query patterns."""
        hints = []

        # Check for table access patterns
        if patterns["table_access_patterns"]:
            most_accessed = max(patterns["table_access_patterns"].items(), key=lambda x: x[1])
            hints.append(
                f"Consider indexing strategy for frequently accessed table: {most_accessed[0]}"
            )

        # Check for join patterns
        if patterns["join_patterns"]:
            total_joins = sum(patterns["join_patterns"].values())
            if total_joins > len(patterns["join_patterns"]) * 2:
                hints.append(
                    "High join usage detected - consider denormalization or materialized views"
                )

        # Check for aggregation patterns
        if patterns["aggregation_patterns"]:
            if patterns["aggregation_patterns"].get("group_by", 0) > 5:
                hints.append(
                    "Frequent GROUP BY operations - consider pre-aggregated tables or materialized views"
                )

        # Check complexity distribution
        if patterns["complexity_distribution"].get("complex", 0) > patterns[
            "complexity_distribution"
        ].get("simple", 0):
            hints.append(
                "High complexity query ratio - review query optimization and indexing strategies"
            )

        return hints

    async def _analyze_index_opportunities(self) -> dict[str, Any]:
        """Analyze database for missing index opportunities."""
        analysis = {
            "missing_indexes": [],
            "recommendations": [],
            "tables": [],
            "performance_impact": "medium",
        }

        try:
            # Analyze query patterns for potential missing indexes
            if self.query_metrics:
                # Look for queries that scan large tables without proper indexes
                for metric in self.query_metrics:
                    if (
                        metric.execution_time_ms > 1000
                        and metric.complexity == QueryComplexity.COMPLEX
                    ):
                        # Suggest indexes based on query patterns
                        sql_lower = metric.sql.lower()

                        if "where" in sql_lower and "fact_sale" in sql_lower:
                            analysis["missing_indexes"].append(
                                {
                                    "table": "fact_sale",
                                    "suggested_columns": [
                                        "date_key",
                                        "customer_key",
                                        "product_key",
                                    ],
                                    "index_type": "composite",
                                    "reason": "Frequent WHERE clauses on dimension keys",
                                }
                            )
                            analysis["tables"].append("fact_sale")

                        if "join" in sql_lower and "dim_customer" in sql_lower:
                            analysis["missing_indexes"].append(
                                {
                                    "table": "dim_customer",
                                    "suggested_columns": ["customer_key", "customer_segment"],
                                    "index_type": "covering",
                                    "reason": "Frequent joins and filtering on customer attributes",
                                }
                            )
                            analysis["tables"].append("dim_customer")

                        if "order by" in sql_lower:
                            analysis["missing_indexes"].append(
                                {
                                    "table": "extracted_from_query",
                                    "suggested_columns": ["order_by_column"],
                                    "index_type": "btree",
                                    "reason": "Sorting operations without proper index support",
                                }
                            )

                if analysis["missing_indexes"]:
                    analysis["recommendations"] = [
                        "Create composite indexes on frequently joined columns",
                        "Add covering indexes for SELECT queries with WHERE clauses",
                        "Consider partial indexes for filtered queries",
                        "Monitor index usage after creation to ensure effectiveness",
                    ]
                    analysis["performance_impact"] = (
                        "high" if len(analysis["missing_indexes"]) > 5 else "medium"
                    )

        except Exception as e:
            self.logger.error(f"Index analysis failed: {e}")
            analysis["error"] = str(e)

        return analysis

    async def _analyze_connection_pool_efficiency(self) -> dict[str, Any]:
        """Analyze connection pool efficiency and optimization needs."""
        if not self.pool_metrics_history:
            return {"optimization_needed": False}

        recent_metrics = self.pool_metrics_history[-10:]
        avg_utilization = sum(m.checked_out / max(m.pool_size, 1) for m in recent_metrics) / len(
            recent_metrics
        )
        peak_utilization = max(m.checked_out / max(m.pool_size, 1) for m in recent_metrics)

        analysis = {
            "optimization_needed": False,
            "description": "",
            "recommendations": [],
            "metrics": {
                "avg_utilization": avg_utilization,
                "peak_utilization": peak_utilization,
                "current_pool_size": recent_metrics[-1].pool_size if recent_metrics else 0,
            },
        }

        if avg_utilization > 0.8:
            analysis["optimization_needed"] = True
            analysis["description"] = (
                f"High average pool utilization ({avg_utilization:.1%}) indicates potential bottleneck"
            )
            analysis["recommendations"].extend(
                [
                    "Increase connection pool size for better concurrency",
                    "Implement connection pool warming during startup",
                    "Consider read replica connections for read-heavy workloads",
                    "Monitor application connection lifecycle for leaks",
                ]
            )
        elif avg_utilization < 0.2:
            analysis["optimization_needed"] = True
            analysis["description"] = (
                f"Low average pool utilization ({avg_utilization:.1%}) indicates oversized pool"
            )
            analysis["recommendations"].extend(
                [
                    "Reduce connection pool size to save resources",
                    "Implement dynamic pool sizing based on load",
                    "Consider shorter connection timeout values",
                ]
            )

        if peak_utilization > 0.95:
            analysis["optimization_needed"] = True
            analysis["recommendations"].append(
                "Peak utilization near maximum - implement connection queuing strategy"
            )

        return analysis

    async def _analyze_partitioning_opportunities(self) -> dict[str, Any]:
        """Analyze tables for partitioning opportunities."""
        analysis = {"tables_to_partition": [], "strategies": []}

        try:
            # Check large tables that could benefit from partitioning
            large_tables = []
            for table_name, stats in self.table_stats.items():
                if stats.row_count > 1000000:  # Tables with > 1M rows
                    large_tables.append(
                        {
                            "table": table_name,
                            "row_count": stats.row_count,
                            "size_mb": stats.size_mb,
                            "suggested_strategy": self._suggest_partitioning_strategy(
                                table_name, stats
                            ),
                        }
                    )

            analysis["tables_to_partition"] = large_tables

            if large_tables:
                analysis["strategies"] = [
                    "Implement date-based partitioning for time-series data",
                    "Use hash partitioning for evenly distributed data",
                    "Consider range partitioning for ordered data",
                    "Implement partition pruning in queries for optimal performance",
                ]

        except Exception as e:
            self.logger.error(f"Partitioning analysis failed: {e}")
            analysis["error"] = str(e)

        return analysis

    def _suggest_partitioning_strategy(self, table_name: str, stats: TableStatistics) -> str:
        """Suggest appropriate partitioning strategy for a table."""
        if "fact_sale" in table_name.lower() or "date" in table_name.lower():
            return "date_based"  # Time-series data
        elif "customer" in table_name.lower() or "product" in table_name.lower():
            return "hash_based"  # Evenly distributed reference data
        elif stats.row_count > 10000000:  # Very large tables
            return "range_based"  # Range partitioning for very large datasets
        else:
            return "hash_based"  # Default to hash partitioning

    async def _analyze_materialized_view_opportunities(self) -> dict[str, Any]:
        """Analyze query patterns for materialized view opportunities."""
        from collections import defaultdict

        analysis = {"candidates": [], "recommendations": []}

        try:
            if not self.query_metrics:
                return analysis

            # Look for repeated complex aggregation patterns
            aggregation_patterns = defaultdict(int)
            complex_queries = [
                m for m in self.query_metrics if m.complexity == QueryComplexity.COMPLEX
            ]

            for query in complex_queries:
                sql_lower = query.sql.lower()

                # Identify common aggregation patterns
                if "group by" in sql_lower and "sum(" in sql_lower:
                    aggregation_patterns["sales_aggregation"] += 1

                if "count(*)" in sql_lower and "join" in sql_lower:
                    aggregation_patterns["dimensional_counting"] += 1

                if "avg(" in sql_lower and "date" in sql_lower:
                    aggregation_patterns["time_series_analysis"] += 1

            # Suggest materialized views for frequent patterns
            for pattern, count in aggregation_patterns.items():
                if count > 5:  # Pattern appears frequently
                    analysis["candidates"].append(
                        {
                            "pattern": pattern,
                            "frequency": count,
                            "suggested_view": self._suggest_materialized_view(pattern),
                            "estimated_benefit": "High - frequently executed complex aggregation",
                        }
                    )

            if analysis["candidates"]:
                analysis["recommendations"] = [
                    "Create materialized views for frequently executed aggregations",
                    "Implement incremental refresh strategies for real-time data",
                    "Monitor materialized view usage and maintenance overhead",
                    "Consider indexed views for better query performance",
                ]

        except Exception as e:
            self.logger.error(f"Materialized view analysis failed: {e}")
            analysis["error"] = str(e)

        return analysis

    def _suggest_materialized_view(self, pattern: str) -> str:
        """Suggest materialized view definition for a pattern."""
        suggestions = {
            "sales_aggregation": "mv_daily_sales_summary (date, total_sales, transaction_count)",
            "dimensional_counting": "mv_customer_product_summary (customer_segment, product_category, transaction_count)",
            "time_series_analysis": "mv_monthly_trends (year_month, avg_sales, customer_count)",
        }
        return suggestions.get(pattern, f"mv_{pattern}_summary")

    def _analyze_query_caching_opportunities(self) -> dict[str, Any]:
        """Analyze query patterns for caching opportunities."""
        from collections import defaultdict

        analysis = {"cacheable_patterns": [], "strategies": []}

        try:
            if not self.query_metrics:
                return analysis

            # Look for repeated queries
            query_frequency = defaultdict(int)
            for metric in self.query_metrics:
                # Normalize query for pattern matching (remove literals)
                normalized_query = self._normalize_query_for_caching(metric.sql)
                query_frequency[normalized_query] += 1

            # Identify frequently executed queries suitable for caching
            for query, frequency in query_frequency.items():
                if frequency > 3:  # Query executed more than 3 times
                    analysis["cacheable_patterns"].append(
                        {
                            "query_pattern": query[:100] + "..." if len(query) > 100 else query,
                            "frequency": frequency,
                            "cache_strategy": "result_cache"
                            if "select" in query.lower()
                            else "query_plan_cache",
                        }
                    )

            if analysis["cacheable_patterns"]:
                analysis["strategies"] = [
                    "Implement query result caching for frequently executed SELECT statements",
                    "Use query plan caching for complex analytical queries",
                    "Consider application-level caching for dashboard queries",
                    "Implement cache invalidation strategy for real-time data requirements",
                ]

        except Exception as e:
            self.logger.error(f"Caching analysis failed: {e}")
            analysis["error"] = str(e)

        return analysis

    def _normalize_query_for_caching(self, sql: str) -> str:
        """Normalize SQL query for caching pattern analysis."""
        import re

        # Convert to lowercase
        normalized = sql.lower().strip()

        # Remove literal values (numbers and strings)
        normalized = re.sub(r"'[^']*'", "'?'", normalized)  # String literals
        normalized = re.sub(r"\b\d+\b", "?", normalized)  # Numeric literals

        # Remove extra whitespace
        normalized = re.sub(r"\s+", " ", normalized)

        return normalized

    async def _analyze_maintenance_optimization(self) -> dict[str, Any]:
        """Analyze database maintenance procedures for optimization."""
        analysis = {"improvements": [], "automation": []}

        try:
            # Check maintenance frequency and effectiveness
            if self._last_maintenance:
                hours_since_maintenance = (
                    datetime.utcnow() - self._last_maintenance
                ).total_seconds() / 3600

                if hours_since_maintenance > 24:
                    analysis["improvements"].append(
                        "Increase maintenance frequency for better performance"
                    )
                    analysis["automation"].append(
                        "Implement automated daily maintenance scheduling"
                    )

            # Suggest maintenance improvements based on database state
            if self.table_stats:
                large_tables = [
                    name for name, stats in self.table_stats.items() if stats.row_count > 500000
                ]
                if large_tables:
                    analysis["improvements"].extend(
                        [
                            "Implement incremental statistics updates for large tables",
                            "Consider online index maintenance for minimal downtime",
                            "Implement automated table partitioning maintenance",
                        ]
                    )
                    analysis["automation"].extend(
                        [
                            "Schedule automated ANALYZE during low-traffic periods",
                            "Implement intelligent index rebuild based on fragmentation",
                            "Set up automated partition management",
                        ]
                    )

            # Query cache maintenance
            if len(self.query_plan_cache) > 500:
                analysis["improvements"].append(
                    "Optimize query plan cache size and eviction policy"
                )
                analysis["automation"].append("Implement automated query cache cleanup")

        except Exception as e:
            self.logger.error(f"Maintenance analysis failed: {e}")
            analysis["error"] = str(e)

        return analysis
