"""
Database Schema Optimizer

Enterprise-grade database schema optimization with:
- Intelligent indexing strategies
- Automated partitioning recommendations
- Performance-based schema analysis
- Column store optimization
- Statistics-driven recommendations
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import Engine, MetaData, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from core.database.performance import DatabasePerformanceOptimizer
from core.logging import get_logger

logger = get_logger(__name__)


class IndexType(str, Enum):
    """Database index types."""

    BTREE = "btree"
    HASH = "hash"
    GIN = "gin"
    GIST = "gist"
    BITMAP = "bitmap"
    COLUMNSTORE = "columnstore"
    PARTIAL = "partial"
    COVERING = "covering"


class PartitionStrategy(str, Enum):
    """Partitioning strategies."""

    RANGE = "range"
    HASH = "hash"
    LIST = "list"
    TIME_BASED = "time_based"
    SIZE_BASED = "size_based"
    HYBRID = "hybrid"


@dataclass
class ColumnStatistics:
    """Comprehensive column statistics for optimization."""

    column_name: str
    table_name: str
    data_type: str
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    avg_length: float | None
    max_length: int | None
    min_value: str | None
    max_value: str | None
    most_frequent_values: list[tuple[Any, int]]
    histogram: dict[str, int] | None
    correlation_with_other_columns: dict[str, float]


@dataclass
class IndexRecommendation:
    """Index optimization recommendation."""

    table_name: str
    index_name: str
    columns: list[str]
    index_type: IndexType
    is_unique: bool
    is_partial: bool
    partial_condition: str | None
    estimated_benefit_score: float
    estimated_size_mb: float
    creation_sql: str
    rationale: str
    priority: str  # HIGH, MEDIUM, LOW
    expected_queries_improved: list[str]


@dataclass
class PartitionRecommendation:
    """Table partitioning recommendation."""

    table_name: str
    partition_strategy: PartitionStrategy
    partition_columns: list[str]
    partition_count: int
    partition_size_mb: float
    estimated_benefit_score: float
    creation_sql: str
    maintenance_sql: list[str]
    rationale: str
    priority: str


@dataclass
class SchemaOptimizationResult:
    """Results of schema optimization analysis."""

    table_name: str
    current_size_mb: float
    row_count: int
    index_recommendations: list[IndexRecommendation]
    partition_recommendations: list[PartitionRecommendation]
    column_statistics: dict[str, ColumnStatistics]
    query_patterns: list[dict[str, Any]]
    optimization_score: float
    estimated_performance_improvement: float
    timestamp: datetime


class DatabaseSchemaOptimizer:
    """
    Advanced database schema optimizer with intelligent recommendations.

    Features:
    - Automated index analysis and recommendations
    - Partitioning strategy optimization
    - Column statistics and correlation analysis
    - Query pattern-based optimization
    - Performance impact estimation
    - Cost-benefit analysis for optimizations
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.metadata = MetaData()
        self.performance_optimizer = DatabasePerformanceOptimizer(engine)
        self.inspector = inspect(engine)
        self.db_type = str(engine.url.drivername).lower()

        # Performance thresholds
        self.large_table_threshold_mb = 1000  # 1GB
        self.high_cardinality_threshold = 0.8  # 80% unique values
        self.low_selectivity_threshold = 0.1  # 10% selectivity

    async def analyze_table_schema(
        self, table_name: str, analyze_queries: bool = True
    ) -> SchemaOptimizationResult:
        """Analyze table schema and generate optimization recommendations."""
        logger.info(f"Starting schema analysis for table: {table_name}")

        try:
            # Gather table statistics
            table_stats = await self._get_table_statistics(table_name)

            # Analyze columns
            column_stats = await self._analyze_table_columns(table_name)

            # Analyze current indexes
            current_indexes = await self._get_current_indexes(table_name)

            # Generate index recommendations
            index_recommendations = await self._generate_index_recommendations(
                table_name, column_stats, current_indexes
            )

            # Generate partition recommendations
            partition_recommendations = await self._generate_partition_recommendations(
                table_name, table_stats, column_stats
            )

            # Analyze query patterns if requested
            query_patterns = []
            if analyze_queries:
                query_patterns = await self._analyze_query_patterns(table_name)

            # Calculate optimization scores
            optimization_score = self._calculate_optimization_score(
                table_stats, index_recommendations, partition_recommendations
            )

            # Estimate performance improvement
            performance_improvement = self._estimate_performance_improvement(
                index_recommendations, partition_recommendations
            )

            result = SchemaOptimizationResult(
                table_name=table_name,
                current_size_mb=table_stats.get("size_mb", 0),
                row_count=table_stats.get("row_count", 0),
                index_recommendations=index_recommendations,
                partition_recommendations=partition_recommendations,
                column_statistics=column_stats,
                query_patterns=query_patterns,
                optimization_score=optimization_score,
                estimated_performance_improvement=performance_improvement,
                timestamp=datetime.utcnow(),
            )

            logger.info(
                f"Schema analysis completed for {table_name}. "
                f"Score: {optimization_score:.2f}, "
                f"Est. improvement: {performance_improvement:.1f}%"
            )

            return result

        except Exception as e:
            logger.error(f"Schema analysis failed for {table_name}: {str(e)}")
            raise

    async def _get_table_statistics(self, table_name: str) -> dict[str, Any]:
        """Get comprehensive table statistics."""
        stats = {}

        try:
            with self.engine.connect() as conn:
                # Row count
                row_count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                stats["row_count"] = row_count_result.scalar()

                # Estimate table size
                if "postgresql" in self.db_type:
                    size_query = f"""
                    SELECT
                        pg_total_relation_size('{table_name}')::bigint / (1024*1024) as size_mb,
                        pg_relation_size('{table_name}')::bigint / (1024*1024) as data_size_mb,
                        (pg_total_relation_size('{table_name}') - pg_relation_size('{table_name}'))::bigint / (1024*1024) as index_size_mb
                    """
                    size_result = conn.execute(text(size_query))
                    size_row = size_result.fetchone()
                    if size_row:
                        stats["size_mb"] = size_row[0]
                        stats["data_size_mb"] = size_row[1]
                        stats["index_size_mb"] = size_row[2]

                elif "sqlite" in self.db_type:
                    # SQLite approximation
                    stats["size_mb"] = stats["row_count"] * 0.001  # Rough estimate

                else:
                    # Generic estimation
                    stats["size_mb"] = stats["row_count"] * 0.001

                # Table age and activity
                if "postgresql" in self.db_type:
                    activity_query = f"""
                    SELECT
                        n_tup_ins as inserts,
                        n_tup_upd as updates,
                        n_tup_del as deletes,
                        last_analyze,
                        last_vacuum
                    FROM pg_stat_user_tables
                    WHERE relname = '{table_name}'
                    """
                    activity_result = conn.execute(text(activity_query))
                    activity_row = activity_result.fetchone()
                    if activity_row:
                        stats.update(
                            {
                                "inserts": activity_row[0] or 0,
                                "updates": activity_row[1] or 0,
                                "deletes": activity_row[2] or 0,
                                "last_analyze": activity_row[3],
                                "last_vacuum": activity_row[4],
                            }
                        )

        except SQLAlchemyError as e:
            logger.warning(f"Failed to get some table statistics for {table_name}: {str(e)}")

        return stats

    async def _analyze_table_columns(self, table_name: str) -> dict[str, ColumnStatistics]:
        """Analyze all columns in the table."""
        column_stats = {}

        try:
            columns = self.inspector.get_columns(table_name)

            for column in columns:
                col_name = column["name"]
                col_type = str(column["type"])

                stats = await self._get_column_statistics(table_name, col_name, col_type)
                column_stats[col_name] = stats

        except Exception as e:
            logger.error(f"Failed to analyze columns for {table_name}: {str(e)}")

        return column_stats

    async def _get_column_statistics(
        self, table_name: str, column_name: str, data_type: str
    ) -> ColumnStatistics:
        """Get detailed statistics for a single column."""
        try:
            with self.engine.connect() as conn:
                # Basic statistics
                base_query = f"""
                SELECT
                    COUNT(*) as total_count,
                    COUNT({column_name}) as non_null_count,
                    COUNT(DISTINCT {column_name}) as distinct_count
                FROM {table_name}
                """

                base_result = conn.execute(text(base_query))
                base_row = base_result.fetchone()

                total_count = base_row[0]
                non_null_count = base_row[1]
                distinct_count = base_row[2]

                null_count = total_count - non_null_count
                null_percentage = null_count / total_count if total_count > 0 else 0
                distinct_percentage = distinct_count / total_count if total_count > 0 else 0

                # Type-specific statistics
                avg_length = None
                max_length = None
                min_value = None
                max_value = None

                if "char" in data_type.lower() or "text" in data_type.lower():
                    # String statistics
                    string_stats_query = f"""
                    SELECT
                        AVG(LENGTH({column_name})) as avg_length,
                        MAX(LENGTH({column_name})) as max_length,
                        MIN({column_name}) as min_value,
                        MAX({column_name}) as max_value
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    """

                    string_result = conn.execute(text(string_stats_query))
                    string_row = string_result.fetchone()
                    if string_row:
                        avg_length = float(string_row[0]) if string_row[0] else None
                        max_length = int(string_row[1]) if string_row[1] else None
                        min_value = str(string_row[2]) if string_row[2] else None
                        max_value = str(string_row[3]) if string_row[3] else None

                elif any(
                    num_type in data_type.lower()
                    for num_type in ["int", "float", "numeric", "decimal", "real"]
                ):
                    # Numeric statistics
                    numeric_stats_query = f"""
                    SELECT
                        MIN({column_name})::text as min_value,
                        MAX({column_name})::text as max_value
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    """

                    numeric_result = conn.execute(text(numeric_stats_query))
                    numeric_row = numeric_result.fetchone()
                    if numeric_row:
                        min_value = str(numeric_row[0]) if numeric_row[0] else None
                        max_value = str(numeric_row[1]) if numeric_row[1] else None

                # Most frequent values
                frequent_values = []
                try:
                    frequent_query = f"""
                    SELECT {column_name}, COUNT(*) as frequency
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    GROUP BY {column_name}
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                    """

                    frequent_result = conn.execute(text(frequent_query))
                    frequent_values = [(row[0], row[1]) for row in frequent_result.fetchall()]

                except Exception:
                    pass  # Skip if query fails

                return ColumnStatistics(
                    column_name=column_name,
                    table_name=table_name,
                    data_type=data_type,
                    null_count=null_count,
                    null_percentage=null_percentage,
                    distinct_count=distinct_count,
                    distinct_percentage=distinct_percentage,
                    avg_length=avg_length,
                    max_length=max_length,
                    min_value=min_value,
                    max_value=max_value,
                    most_frequent_values=frequent_values,
                    histogram=None,  # Could be implemented for specific databases
                    correlation_with_other_columns={},  # Could be computed if needed
                )

        except Exception as e:
            logger.warning(f"Failed to get statistics for column {column_name}: {str(e)}")
            return ColumnStatistics(
                column_name=column_name,
                table_name=table_name,
                data_type=data_type,
                null_count=0,
                null_percentage=0,
                distinct_count=0,
                distinct_percentage=0,
                avg_length=None,
                max_length=None,
                min_value=None,
                max_value=None,
                most_frequent_values=[],
                histogram=None,
                correlation_with_other_columns={},
            )

    async def _get_current_indexes(self, table_name: str) -> list[dict[str, Any]]:
        """Get information about current indexes on the table."""
        try:
            indexes = self.inspector.get_indexes(table_name)

            enhanced_indexes = []
            for index in indexes:
                enhanced_index = {
                    "name": index["name"],
                    "columns": index["column_names"],
                    "unique": index.get("unique", False),
                    "type": "btree",  # Default, could be enhanced per database
                    "size_mb": 0,  # Could be calculated if database supports it
                }
                enhanced_indexes.append(enhanced_index)

            return enhanced_indexes

        except Exception as e:
            logger.warning(f"Failed to get current indexes for {table_name}: {str(e)}")
            return []

    async def _generate_index_recommendations(
        self,
        table_name: str,
        column_stats: dict[str, ColumnStatistics],
        current_indexes: list[dict[str, Any]],
    ) -> list[IndexRecommendation]:
        """Generate intelligent index recommendations."""
        recommendations = []
        current_index_columns = set()

        # Track existing indexed columns
        for idx in current_indexes:
            for col in idx["columns"]:
                current_index_columns.add(col.lower())

        # Analyze each column for index potential
        for col_name, stats in column_stats.items():
            if col_name.lower() in current_index_columns:
                continue  # Skip already indexed columns

            index_recommendations = self._evaluate_column_for_indexing(table_name, col_name, stats)
            recommendations.extend(index_recommendations)

        # Generate composite index recommendations
        composite_recommendations = self._generate_composite_index_recommendations(
            table_name, column_stats, current_index_columns
        )
        recommendations.extend(composite_recommendations)

        # Sort by benefit score
        recommendations.sort(key=lambda x: x.estimated_benefit_score, reverse=True)

        return recommendations[:10]  # Return top 10 recommendations

    def _evaluate_column_for_indexing(
        self, table_name: str, column_name: str, stats: ColumnStatistics
    ) -> list[IndexRecommendation]:
        """Evaluate a single column for indexing potential."""
        recommendations = []

        # High selectivity columns (good for filtering)
        if stats.distinct_percentage >= self.high_cardinality_threshold:
            benefit_score = 85 + (stats.distinct_percentage - self.high_cardinality_threshold) * 15

            recommendations.append(
                IndexRecommendation(
                    table_name=table_name,
                    index_name=f"idx_{table_name}_{column_name}",
                    columns=[column_name],
                    index_type=IndexType.BTREE,
                    is_unique=stats.distinct_percentage >= 0.99,
                    is_partial=False,
                    partial_condition=None,
                    estimated_benefit_score=benefit_score,
                    estimated_size_mb=self._estimate_index_size(table_name, [column_name]),
                    creation_sql=f"CREATE INDEX idx_{table_name}_{column_name} ON {table_name} ({column_name})",
                    rationale=f"High selectivity column ({stats.distinct_percentage:.1%}) - excellent for filtering",
                    priority="HIGH",
                    expected_queries_improved=[
                        f"WHERE {column_name} = ?",
                        f"WHERE {column_name} IN (?)",
                        f"ORDER BY {column_name}",
                    ],
                )
            )

        # Columns with skewed distribution (partial indexes)
        elif (
            stats.most_frequent_values
            and len(stats.most_frequent_values) > 0
            and stats.most_frequent_values[0][1]
            / sum(freq for _, freq in stats.most_frequent_values)
            < 0.5
        ):
            # Recommend partial index excluding most common value
            most_common_value = stats.most_frequent_values[0][0]
            benefit_score = 65

            recommendations.append(
                IndexRecommendation(
                    table_name=table_name,
                    index_name=f"idx_{table_name}_{column_name}_partial",
                    columns=[column_name],
                    index_type=IndexType.PARTIAL,
                    is_unique=False,
                    is_partial=True,
                    partial_condition=f"{column_name} != '{most_common_value}'",
                    estimated_benefit_score=benefit_score,
                    estimated_size_mb=self._estimate_index_size(table_name, [column_name]) * 0.7,
                    creation_sql=f"CREATE INDEX idx_{table_name}_{column_name}_partial ON {table_name} ({column_name}) WHERE {column_name} != '{most_common_value}'",
                    rationale=f"Skewed distribution - partial index excludes common value '{most_common_value}'",
                    priority="MEDIUM",
                    expected_queries_improved=[
                        f"WHERE {column_name} = ? AND {column_name} != '{most_common_value}'"
                    ],
                )
            )

        # Date/timestamp columns for time-series queries
        if any(date_type in stats.data_type.lower() for date_type in ["date", "timestamp", "time"]):
            benefit_score = 70
            recommendations.append(
                IndexRecommendation(
                    table_name=table_name,
                    index_name=f"idx_{table_name}_{column_name}_time",
                    columns=[column_name],
                    index_type=IndexType.BTREE,
                    is_unique=False,
                    is_partial=False,
                    partial_condition=None,
                    estimated_benefit_score=benefit_score,
                    estimated_size_mb=self._estimate_index_size(table_name, [column_name]),
                    creation_sql=f"CREATE INDEX idx_{table_name}_{column_name}_time ON {table_name} ({column_name})",
                    rationale="Time-based column - excellent for range queries and time-series analysis",
                    priority="HIGH",
                    expected_queries_improved=[
                        f"WHERE {column_name} BETWEEN ? AND ?",
                        f"WHERE {column_name} >= ?",
                        f"ORDER BY {column_name}",
                    ],
                )
            )

        return recommendations

    def _generate_composite_index_recommendations(
        self,
        table_name: str,
        column_stats: dict[str, ColumnStatistics],
        existing_indexed_columns: set[str],
    ) -> list[IndexRecommendation]:
        """Generate composite index recommendations based on column relationships."""
        recommendations = []

        # Find foreign key-like columns (ending with _id, _key)
        fk_columns = [
            col
            for col in column_stats.keys()
            if col.lower().endswith(("_id", "_key")) and col.lower() not in existing_indexed_columns
        ]

        # Find date columns
        date_columns = [
            col
            for col, stats in column_stats.items()
            if any(date_type in stats.data_type.lower() for date_type in ["date", "timestamp"])
            and col.lower() not in existing_indexed_columns
        ]

        # Recommend FK + Date composite indexes
        for fk_col in fk_columns[:3]:  # Limit to top 3
            for date_col in date_columns[:2]:  # Limit to top 2
                benefit_score = 75

                recommendations.append(
                    IndexRecommendation(
                        table_name=table_name,
                        index_name=f"idx_{table_name}_{fk_col}_{date_col}",
                        columns=[fk_col, date_col],
                        index_type=IndexType.BTREE,
                        is_unique=False,
                        is_partial=False,
                        partial_condition=None,
                        estimated_benefit_score=benefit_score,
                        estimated_size_mb=self._estimate_index_size(table_name, [fk_col, date_col]),
                        creation_sql=f"CREATE INDEX idx_{table_name}_{fk_col}_{date_col} ON {table_name} ({fk_col}, {date_col})",
                        rationale="Composite index for common join + time filter pattern",
                        priority="MEDIUM",
                        expected_queries_improved=[
                            f"WHERE {fk_col} = ? AND {date_col} BETWEEN ? AND ?",
                            f"WHERE {fk_col} = ? ORDER BY {date_col}",
                        ],
                    )
                )

        return recommendations

    def _estimate_index_size(self, table_name: str, columns: list[str]) -> float:
        """Estimate index size in MB."""
        # Simple estimation based on column count and table size
        base_size = 0.1  # Base index overhead
        column_factor = len(columns) * 0.05  # Each column adds ~50KB

        # This is a simplified estimation - real implementation would be more sophisticated
        return base_size + column_factor

    async def _generate_partition_recommendations(
        self,
        table_name: str,
        table_stats: dict[str, Any],
        column_stats: dict[str, ColumnStatistics],
    ) -> list[PartitionRecommendation]:
        """Generate table partitioning recommendations."""
        recommendations = []

        table_size_mb = table_stats.get("size_mb", 0)
        table_stats.get("row_count", 0)

        # Only recommend partitioning for large tables
        if table_size_mb < self.large_table_threshold_mb:
            return recommendations

        # Find suitable partitioning columns
        date_columns = [
            (col, stats)
            for col, stats in column_stats.items()
            if any(date_type in stats.data_type.lower() for date_type in ["date", "timestamp"])
        ]

        # Time-based partitioning recommendations
        for col_name, stats in date_columns:
            if stats.min_value and stats.max_value:
                try:
                    # Estimate time range
                    benefit_score = 80 if table_size_mb > 5000 else 60

                    recommendations.append(
                        PartitionRecommendation(
                            table_name=table_name,
                            partition_strategy=PartitionStrategy.TIME_BASED,
                            partition_columns=[col_name],
                            partition_count=12,  # Monthly partitions
                            partition_size_mb=table_size_mb / 12,
                            estimated_benefit_score=benefit_score,
                            creation_sql=self._generate_partition_sql(
                                table_name, col_name, "monthly"
                            ),
                            maintenance_sql=[
                                f"-- Monthly partition maintenance for {table_name}",
                                f"-- Add new partition: CREATE TABLE {table_name}_YYYY_MM PARTITION OF {table_name} FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM+1-01')",
                                f"-- Drop old partition: DROP TABLE {table_name}_old_partition",
                            ],
                            rationale=f"Large table ({table_size_mb:.0f}MB) with time dimension - monthly partitioning recommended",
                            priority="HIGH" if table_size_mb > 5000 else "MEDIUM",
                        )
                    )

                except Exception:
                    pass

        # Hash partitioning for very large tables without time dimension
        if not date_columns and table_size_mb > 10000:  # 10GB+
            high_cardinality_cols = [
                (col, stats)
                for col, stats in column_stats.items()
                if stats.distinct_percentage > 0.5 and stats.distinct_count > 1000
            ]

            if high_cardinality_cols:
                col_name, stats = high_cardinality_cols[0]  # Take best candidate

                recommendations.append(
                    PartitionRecommendation(
                        table_name=table_name,
                        partition_strategy=PartitionStrategy.HASH,
                        partition_columns=[col_name],
                        partition_count=8,  # 8 hash partitions
                        partition_size_mb=table_size_mb / 8,
                        estimated_benefit_score=70,
                        creation_sql=self._generate_hash_partition_sql(table_name, col_name, 8),
                        maintenance_sql=[
                            f"-- Hash partition maintenance for {table_name}",
                            "-- Monitor partition balance regularly",
                            "-- Consider rebalancing if partitions become skewed",
                        ],
                        rationale=f"Very large table ({table_size_mb:.0f}MB) - hash partitioning for parallel processing",
                        priority="MEDIUM",
                    )
                )

        return recommendations

    def _generate_partition_sql(self, table_name: str, date_column: str, interval: str) -> str:
        """Generate partitioning SQL for time-based partitioning."""
        if "postgresql" in self.db_type:
            return f"""
            -- Convert {table_name} to partitioned table by {date_column}
            BEGIN;

            -- Rename original table
            ALTER TABLE {table_name} RENAME TO {table_name}_original;

            -- Create partitioned table
            CREATE TABLE {table_name} (LIKE {table_name}_original INCLUDING ALL)
            PARTITION BY RANGE ({date_column});

            -- Create initial partitions (example for monthly)
            CREATE TABLE {table_name}_2024_01 PARTITION OF {table_name}
                FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
            CREATE TABLE {table_name}_2024_02 PARTITION OF {table_name}
                FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
            -- Add more partitions as needed...

            -- Migrate data
            INSERT INTO {table_name} SELECT * FROM {table_name}_original;

            -- Drop original table after verification
            -- DROP TABLE {table_name}_original;

            COMMIT;
            """
        else:
            return f"-- Partitioning SQL generation not implemented for {self.db_type}"

    def _generate_hash_partition_sql(
        self, table_name: str, column: str, partition_count: int
    ) -> str:
        """Generate hash partitioning SQL."""
        if "postgresql" in self.db_type:
            partitions_sql = []
            for i in range(partition_count):
                partitions_sql.append(f"""
                CREATE TABLE {table_name}_part_{i} PARTITION OF {table_name}
                    FOR VALUES WITH (modulus {partition_count}, remainder {i});
                """)

            return f"""
            -- Convert {table_name} to hash partitioned table
            BEGIN;

            ALTER TABLE {table_name} RENAME TO {table_name}_original;

            CREATE TABLE {table_name} (LIKE {table_name}_original INCLUDING ALL)
            PARTITION BY HASH ({column});

            {"".join(partitions_sql)}

            INSERT INTO {table_name} SELECT * FROM {table_name}_original;

            COMMIT;
            """
        else:
            return f"-- Hash partitioning SQL generation not implemented for {self.db_type}"

    async def _analyze_query_patterns(self, table_name: str) -> list[dict[str, Any]]:
        """Analyze query patterns for the table (simplified implementation)."""
        # This would analyze slow query logs, pg_stat_statements, etc.
        # For now, return common patterns
        return [
            {
                "pattern_type": "point_lookup",
                "frequency": "high",
                "columns_used": ["id", "customer_id"],
                "example_query": f"SELECT * FROM {table_name} WHERE id = ?",
            },
            {
                "pattern_type": "range_scan",
                "frequency": "medium",
                "columns_used": ["created_at", "updated_at"],
                "example_query": f"SELECT * FROM {table_name} WHERE created_at BETWEEN ? AND ?",
            },
            {
                "pattern_type": "join",
                "frequency": "high",
                "columns_used": ["customer_id", "product_id"],
                "example_query": f"SELECT * FROM {table_name} t JOIN other_table o ON t.customer_id = o.id",
            },
        ]

    def _calculate_optimization_score(
        self,
        table_stats: dict[str, Any],
        index_recommendations: list[IndexRecommendation],
        partition_recommendations: list[PartitionRecommendation],
    ) -> float:
        """Calculate overall optimization score for the table."""
        base_score = 50  # Starting score

        # Bonus for high-value recommendations
        index_bonus = sum(
            min(rec.estimated_benefit_score / 10, 10) for rec in index_recommendations
        )
        partition_bonus = sum(
            min(rec.estimated_benefit_score / 10, 10) for rec in partition_recommendations
        )

        # Penalty for large unoptimized tables
        size_penalty = 0
        if table_stats.get("size_mb", 0) > self.large_table_threshold_mb:
            size_penalty = 10

        final_score = min(100, base_score + index_bonus + partition_bonus - size_penalty)
        return final_score

    def _estimate_performance_improvement(
        self,
        index_recommendations: list[IndexRecommendation],
        partition_recommendations: list[PartitionRecommendation],
    ) -> float:
        """Estimate percentage performance improvement from recommendations."""
        total_improvement = 0

        # Index improvements
        for rec in index_recommendations:
            if rec.priority == "HIGH":
                total_improvement += 20
            elif rec.priority == "MEDIUM":
                total_improvement += 10
            else:
                total_improvement += 5

        # Partitioning improvements
        for rec in partition_recommendations:
            if rec.priority == "HIGH":
                total_improvement += 30
            else:
                total_improvement += 15

        return min(total_improvement, 80)  # Cap at 80% improvement

    async def generate_optimization_report(self, tables: list[str] | None = None) -> dict[str, Any]:
        """Generate comprehensive optimization report for multiple tables."""
        if tables is None:
            tables = self.inspector.get_table_names()

        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "database_type": self.db_type,
            "tables_analyzed": len(tables),
            "table_results": {},
            "summary": {
                "total_recommendations": 0,
                "high_priority_recommendations": 0,
                "estimated_total_improvement": 0,
                "tables_needing_optimization": 0,
            },
        }

        total_recommendations = 0
        high_priority_count = 0
        total_improvement = 0
        tables_needing_optimization = 0

        for table_name in tables:
            try:
                logger.info(f"Analyzing table: {table_name}")
                result = await self.analyze_table_schema(table_name)

                table_recommendations = len(result.index_recommendations) + len(
                    result.partition_recommendations
                )
                total_recommendations += table_recommendations

                high_priority = sum(
                    1 for rec in result.index_recommendations if rec.priority == "HIGH"
                )
                high_priority += sum(
                    1 for rec in result.partition_recommendations if rec.priority == "HIGH"
                )
                high_priority_count += high_priority

                total_improvement += result.estimated_performance_improvement

                if result.optimization_score < 70:
                    tables_needing_optimization += 1

                report["table_results"][table_name] = {
                    "optimization_score": result.optimization_score,
                    "estimated_improvement": result.estimated_performance_improvement,
                    "index_recommendations": len(result.index_recommendations),
                    "partition_recommendations": len(result.partition_recommendations),
                    "current_size_mb": result.current_size_mb,
                    "row_count": result.row_count,
                }

            except Exception as e:
                logger.error(f"Failed to analyze table {table_name}: {str(e)}")
                report["table_results"][table_name] = {
                    "error": str(e),
                    "optimization_score": 0,
                    "estimated_improvement": 0,
                }

        # Update summary
        report["summary"].update(
            {
                "total_recommendations": total_recommendations,
                "high_priority_recommendations": high_priority_count,
                "estimated_total_improvement": total_improvement / len(tables) if tables else 0,
                "tables_needing_optimization": tables_needing_optimization,
            }
        )

        return report

    async def apply_recommendations(
        self,
        table_name: str,
        apply_indexes: bool = True,
        apply_partitions: bool = False,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Apply optimization recommendations to a table."""
        result = await self.analyze_table_schema(table_name)

        application_log = {
            "table_name": table_name,
            "started_at": datetime.utcnow().isoformat(),
            "dry_run": dry_run,
            "applied_indexes": [],
            "applied_partitions": [],
            "errors": [],
            "success": False,
        }

        try:
            with self.engine.connect() as conn:
                # Apply index recommendations
                if apply_indexes:
                    for rec in result.index_recommendations:
                        if rec.priority in ["HIGH", "MEDIUM"]:
                            try:
                                if not dry_run:
                                    conn.execute(text(rec.creation_sql))
                                    conn.commit()

                                application_log["applied_indexes"].append(
                                    {
                                        "name": rec.index_name,
                                        "columns": rec.columns,
                                        "sql": rec.creation_sql,
                                        "applied": not dry_run,
                                    }
                                )

                                logger.info(
                                    f"{'Would create' if dry_run else 'Created'} index: {rec.index_name}"
                                )

                            except Exception as e:
                                error_msg = f"Failed to create index {rec.index_name}: {str(e)}"
                                application_log["errors"].append(error_msg)
                                logger.error(error_msg)

                # Apply partition recommendations (more complex, typically requires downtime)
                if apply_partitions and not dry_run:
                    for rec in result.partition_recommendations:
                        if rec.priority == "HIGH":
                            logger.warning(
                                f"Partitioning {table_name} requires manual intervention and downtime"
                            )
                            application_log["applied_partitions"].append(
                                {
                                    "strategy": rec.partition_strategy.value,
                                    "columns": rec.partition_columns,
                                    "sql": rec.creation_sql,
                                    "applied": False,
                                    "note": "Requires manual intervention",
                                }
                            )

                application_log["success"] = len(application_log["errors"]) == 0
                application_log["completed_at"] = datetime.utcnow().isoformat()

        except Exception as e:
            application_log["errors"].append(f"General application error: {str(e)}")
            logger.error(f"Failed to apply recommendations to {table_name}: {str(e)}")

        return application_log


# Factory function for easy instantiation
def create_schema_optimizer(engine: Engine) -> DatabaseSchemaOptimizer:
    """Create and configure database schema optimizer."""
    optimizer = DatabaseSchemaOptimizer(engine)
    logger.info(f"Schema optimizer created for {engine.url.drivername}")
    return optimizer


# Convenience functions
async def quick_table_analysis(table_name: str, engine: Engine) -> SchemaOptimizationResult:
    """Quick analysis of a single table."""
    optimizer = create_schema_optimizer(engine)
    return await optimizer.analyze_table_schema(table_name)


async def generate_database_optimization_report(
    engine: Engine, tables: list[str] | None = None
) -> dict[str, Any]:
    """Generate optimization report for entire database."""
    optimizer = create_schema_optimizer(engine)
    return await optimizer.generate_optimization_report(tables)


if __name__ == "__main__":
    # Demo usage
    from data_access.db import get_engine

    async def main():
        engine = get_engine()
        optimizer = create_schema_optimizer(engine)

        # Analyze a table
        try:
            result = await optimizer.analyze_table_schema("fact_sale")
            print("Analysis completed for fact_sale")
            print(f"Optimization score: {result.optimization_score}")
            print(f"Index recommendations: {len(result.index_recommendations)}")
            print(f"Partition recommendations: {len(result.partition_recommendations)}")

        except Exception as e:
            print(f"Analysis failed: {e}")

    # asyncio.run(main())
