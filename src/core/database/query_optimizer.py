"""
Advanced SQL Query Optimizer and Transformation Engine

Provides comprehensive query optimization capabilities including:
- Intelligent query rewriting and optimization
- Cost-based optimization analysis
- Advanced SQL transformation patterns
- Query plan analysis and suggestions
- Performance-optimized query generation
"""

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import Engine, MetaData
from sqlparse import parse as sql_parse
from sqlparse.sql import Statement

from core.database.performance import DatabasePerformanceOptimizer
from core.logging import get_logger

logger = get_logger(__name__)


class OptimizationType(str, Enum):
    """Types of query optimizations"""

    INDEX_RECOMMENDATION = "index_recommendation"
    JOIN_OPTIMIZATION = "join_optimization"
    WHERE_CLAUSE_OPTIMIZATION = "where_clause_optimization"
    SUBQUERY_OPTIMIZATION = "subquery_optimization"
    AGGREGATION_OPTIMIZATION = "aggregation_optimization"
    PARTITION_PRUNING = "partition_pruning"
    MATERIALIZED_VIEW_SUGGESTION = "materialized_view_suggestion"
    QUERY_REWRITE = "query_rewrite"


class QueryComplexityLevel(str, Enum):
    """Query complexity classification"""

    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"


@dataclass
class QueryOptimizationSuggestion:
    """Query optimization suggestion"""

    optimization_type: OptimizationType
    priority: str  # high, medium, low
    description: str
    original_query: str
    optimized_query: str | None = None
    estimated_improvement: str | None = None
    implementation_notes: list[str] = field(default_factory=list)
    confidence_score: float = 0.0


@dataclass
class QueryAnalysisResult:
    """Complete query analysis result"""

    query_hash: str
    original_query: str
    complexity: QueryComplexityLevel
    estimated_cost: float
    table_accesses: list[str]
    join_patterns: list[str]
    where_conditions: list[str]
    aggregations: list[str]
    subqueries: int
    optimization_suggestions: list[QueryOptimizationSuggestion]
    analysis_timestamp: datetime = field(default_factory=datetime.utcnow)


class QueryPattern(ABC):
    """Abstract base class for query pattern detection and optimization"""

    @abstractmethod
    def detect(self, query: str, parsed_query: Statement) -> bool:
        """Detect if query matches this pattern"""
        pass

    @abstractmethod
    def optimize(
        self, query: str, parsed_query: Statement, metadata: MetaData | None = None
    ) -> list[QueryOptimizationSuggestion]:
        """Generate optimization suggestions for this pattern"""
        pass


class SelectStarPattern(QueryPattern):
    """Detect and optimize SELECT * queries"""

    def detect(self, query: str, parsed_query: Statement) -> bool:
        return "SELECT *" in query.upper() or "SELECT\n*" in query.upper().replace(" ", "\n")

    def optimize(
        self, query: str, parsed_query: Statement, metadata: MetaData | None = None
    ) -> list[QueryOptimizationSuggestion]:
        suggestions = []

        # Extract table names from query
        tables = self._extract_table_names(query)

        for table in tables:
            suggestion = QueryOptimizationSuggestion(
                optimization_type=OptimizationType.QUERY_REWRITE,
                priority="medium",
                description=f"Replace SELECT * with specific columns from table {table}",
                original_query=query,
                estimated_improvement="10-30% faster, reduced network I/O",
                implementation_notes=[
                    "Select only required columns to reduce data transfer",
                    "Improves query performance and reduces memory usage",
                    "Enables better index utilization",
                ],
                confidence_score=0.8,
            )
            suggestions.append(suggestion)

        return suggestions

    def _extract_table_names(self, query: str) -> list[str]:
        """Extract table names from SQL query"""
        import re

        pattern = r"FROM\s+(\w+)"
        matches = re.findall(pattern, query.upper())
        return matches


class MissingIndexPattern(QueryPattern):
    """Detect queries that could benefit from indexes"""

    def detect(self, query: str, parsed_query: Statement) -> bool:
        query_upper = query.upper()
        return "WHERE" in query_upper or "JOIN" in query_upper or "ORDER BY" in query_upper

    def optimize(
        self, query: str, parsed_query: Statement, metadata: MetaData | None = None
    ) -> list[QueryOptimizationSuggestion]:
        suggestions = []

        # Analyze WHERE clauses for potential indexes
        where_columns = self._extract_where_columns(query)
        for table, columns in where_columns.items():
            suggestion = QueryOptimizationSuggestion(
                optimization_type=OptimizationType.INDEX_RECOMMENDATION,
                priority="high",
                description=f"Create index on table {table} for columns: {', '.join(columns)}",
                original_query=query,
                optimized_query=f"CREATE INDEX idx_{table}_{'_'.join(columns).lower()} ON {table} ({', '.join(columns)});",
                estimated_improvement="50-90% faster WHERE clause evaluation",
                implementation_notes=[
                    "Index will significantly improve query performance",
                    "Consider composite index for multiple columns",
                    "Monitor index usage and maintenance overhead",
                ],
                confidence_score=0.9,
            )
            suggestions.append(suggestion)

        # Analyze JOIN conditions
        join_columns = self._extract_join_columns(query)
        for join_info in join_columns:
            suggestion = QueryOptimizationSuggestion(
                optimization_type=OptimizationType.JOIN_OPTIMIZATION,
                priority="high",
                description=f"Optimize JOIN performance between {join_info['left_table']} and {join_info['right_table']}",
                original_query=query,
                estimated_improvement="30-70% faster JOIN execution",
                implementation_notes=[
                    "Ensure proper indexes on JOIN columns",
                    "Consider JOIN order optimization",
                    "Verify foreign key constraints exist",
                ],
                confidence_score=0.85,
            )
            suggestions.append(suggestion)

        return suggestions

    def _extract_where_columns(self, query: str) -> dict[str, list[str]]:
        """Extract columns used in WHERE clauses"""
        where_columns = defaultdict(list)

        # Simple regex patterns for WHERE column extraction
        patterns = [
            r"WHERE\s+(\w+)\.(\w+)\s*[=<>!]",
            r"WHERE\s+(\w+)\s*[=<>!]",
            r"AND\s+(\w+)\.(\w+)\s*[=<>!]",
            r"AND\s+(\w+)\s*[=<>!]",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, query.upper())
            for match in matches:
                if isinstance(match, tuple) and len(match) == 2:
                    table, column = match
                    where_columns[table].append(column)
                else:
                    where_columns["unknown_table"].append(match)

        return dict(where_columns)

    def _extract_join_columns(self, query: str) -> list[dict[str, str]]:
        """Extract JOIN information from query"""
        join_info = []

        # Pattern to match JOIN conditions
        join_pattern = r"JOIN\s+(\w+)\s+.*?ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)"
        matches = re.findall(join_pattern, query.upper())

        for match in matches:
            right_table, left_table, left_column, right_table_2, right_column = match
            join_info.append(
                {
                    "left_table": left_table,
                    "right_table": right_table,
                    "left_column": left_column,
                    "right_column": right_column,
                }
            )

        return join_info


class SubqueryOptimizationPattern(QueryPattern):
    """Detect and optimize subqueries"""

    def detect(self, query: str, parsed_query: Statement) -> bool:
        # Count parentheses pairs as a simple subquery detection
        return query.count("(") > 1 and (
            "SELECT" in query[query.find("(") :] if "(" in query else False
        )

    def optimize(
        self, query: str, parsed_query: Statement, metadata: MetaData | None = None
    ) -> list[QueryOptimizationSuggestion]:
        suggestions = []

        subquery_count = self._count_subqueries(query)

        if subquery_count > 0:
            suggestion = QueryOptimizationSuggestion(
                optimization_type=OptimizationType.SUBQUERY_OPTIMIZATION,
                priority="medium",
                description=f"Optimize {subquery_count} subquer{'ies' if subquery_count > 1 else 'y'}",
                original_query=query,
                estimated_improvement="20-50% performance improvement",
                implementation_notes=[
                    "Consider converting correlated subqueries to JOINs",
                    "Use EXISTS instead of IN for better performance",
                    "Consider materializing complex subqueries as CTEs",
                    "Evaluate using window functions instead of subqueries",
                ],
                confidence_score=0.7,
            )
            suggestions.append(suggestion)

        # Detect specific IN subquery pattern
        if re.search(r"\bIN\s*\(\s*SELECT", query.upper()):
            suggestion = QueryOptimizationSuggestion(
                optimization_type=OptimizationType.SUBQUERY_OPTIMIZATION,
                priority="high",
                description="Replace IN subquery with EXISTS or JOIN",
                original_query=query,
                estimated_improvement="30-80% faster execution",
                implementation_notes=[
                    "EXISTS is generally faster than IN with subqueries",
                    "JOIN may be even faster with proper indexing",
                    "Consider semi-join optimization",
                ],
                confidence_score=0.85,
            )
            suggestions.append(suggestion)

        return suggestions

    def _count_subqueries(self, query: str) -> int:
        """Count approximate number of subqueries"""
        # Simple approximation based on SELECT statements within parentheses
        pattern = r"\([^()]*SELECT[^()]*\)"
        return len(re.findall(pattern, query.upper()))


class AggregationOptimizationPattern(QueryPattern):
    """Detect and optimize aggregation queries"""

    def detect(self, query: str, parsed_query: Statement) -> bool:
        query_upper = query.upper()
        agg_functions = ["COUNT", "SUM", "AVG", "MAX", "MIN", "GROUP BY"]
        return any(func in query_upper for func in agg_functions)

    def optimize(
        self, query: str, parsed_query: Statement, metadata: MetaData | None = None
    ) -> list[QueryOptimizationSuggestion]:
        suggestions = []

        # Check for missing GROUP BY optimizations
        if "GROUP BY" in query.upper():
            group_columns = self._extract_group_by_columns(query)
            suggestion = QueryOptimizationSuggestion(
                optimization_type=OptimizationType.AGGREGATION_OPTIMIZATION,
                priority="medium",
                description=f"Optimize GROUP BY performance for columns: {', '.join(group_columns)}",
                original_query=query,
                estimated_improvement="25-60% faster aggregation",
                implementation_notes=[
                    "Create composite index on GROUP BY columns",
                    "Order GROUP BY columns by cardinality (lowest first)",
                    "Consider pre-aggregated tables or materialized views",
                    "Use covering indexes when possible",
                ],
                confidence_score=0.75,
            )
            suggestions.append(suggestion)

        # Check for potential materialized view opportunities
        if self._is_materialized_view_candidate(query):
            suggestion = QueryOptimizationSuggestion(
                optimization_type=OptimizationType.MATERIALIZED_VIEW_SUGGESTION,
                priority="low",
                description="Consider creating materialized view for this aggregation query",
                original_query=query,
                estimated_improvement="90%+ faster for repeated queries",
                implementation_notes=[
                    "Materialized views are ideal for expensive aggregations",
                    "Set up refresh strategy based on data update frequency",
                    "Monitor storage requirements",
                    "Consider partial materialized views for large datasets",
                ],
                confidence_score=0.6,
            )
            suggestions.append(suggestion)

        return suggestions

    def _extract_group_by_columns(self, query: str) -> list[str]:
        """Extract GROUP BY columns"""
        pattern = r"GROUP\s+BY\s+([^ORDER\s][^HAVING\s][^LIMIT\s]+)"
        match = re.search(pattern, query.upper())
        if match:
            columns_str = match.group(1)
            return [col.strip() for col in columns_str.split(",")]
        return []

    def _is_materialized_view_candidate(self, query: str) -> bool:
        """Determine if query is a good candidate for materialized view"""
        query_upper = query.upper()
        # Complex aggregations with multiple tables are good candidates
        has_multiple_tables = query_upper.count("FROM") > 0 and ("JOIN" in query_upper)
        has_aggregations = any(func in query_upper for func in ["COUNT", "SUM", "AVG"])
        has_group_by = "GROUP BY" in query_upper

        return has_multiple_tables and has_aggregations and has_group_by


class AdvancedQueryOptimizer:
    """
    Advanced SQL query optimizer with comprehensive analysis and optimization capabilities.

    Features:
    - Query pattern recognition and optimization
    - Cost-based optimization analysis
    - Index recommendation engine
    - Query rewriting suggestions
    - Performance impact estimation
    - Batch query optimization
    """

    def __init__(self, engine: Engine, enable_caching: bool = True):
        self.engine = engine
        self.metadata = MetaData()
        self.enable_caching = enable_caching

        # Pattern detection engines
        self.patterns: list[QueryPattern] = [
            SelectStarPattern(),
            MissingIndexPattern(),
            SubqueryOptimizationPattern(),
            AggregationOptimizationPattern(),
        ]

        # Analysis cache
        self.analysis_cache: dict[str, QueryAnalysisResult] = {}
        self.optimization_history: list[QueryAnalysisResult] = []

        # Performance optimizer integration
        self.performance_optimizer = DatabasePerformanceOptimizer(engine)

        # Query statistics
        self.query_stats = {
            "total_analyzed": 0,
            "total_optimizations": 0,
            "optimization_types": defaultdict(int),
        }

        logger.info("Advanced Query Optimizer initialized")

    def analyze_query(self, query: str, use_cache: bool = None) -> QueryAnalysisResult:
        """Perform comprehensive query analysis"""
        use_cache = use_cache if use_cache is not None else self.enable_caching

        # Generate query hash for caching
        query_hash = hashlib.md5(query.encode()).hexdigest()

        if use_cache and query_hash in self.analysis_cache:
            logger.debug(f"Using cached analysis for query: {query_hash}")
            return self.analysis_cache[query_hash]

        logger.info(f"Analyzing query: {query_hash}")

        try:
            # Parse query
            parsed_query = sql_parse(query)[0]

            # Basic query analysis
            complexity = self._analyze_complexity(query, parsed_query)
            estimated_cost = self._estimate_cost(query, parsed_query)
            table_accesses = self._extract_table_accesses(query)
            join_patterns = self._analyze_join_patterns(query)
            where_conditions = self._extract_where_conditions(query)
            aggregations = self._extract_aggregations(query)
            subqueries = self._count_subqueries(query)

            # Generate optimization suggestions
            optimization_suggestions = self._generate_optimizations(query, parsed_query)

            # Create analysis result
            result = QueryAnalysisResult(
                query_hash=query_hash,
                original_query=query,
                complexity=complexity,
                estimated_cost=estimated_cost,
                table_accesses=table_accesses,
                join_patterns=join_patterns,
                where_conditions=where_conditions,
                aggregations=aggregations,
                subqueries=subqueries,
                optimization_suggestions=optimization_suggestions,
            )

            # Cache result
            if use_cache:
                self.analysis_cache[query_hash] = result

            # Update statistics
            self.query_stats["total_analyzed"] += 1
            self.query_stats["total_optimizations"] += len(optimization_suggestions)

            for suggestion in optimization_suggestions:
                self.query_stats["optimization_types"][suggestion.optimization_type.value] += 1

            # Store in history
            self.optimization_history.append(result)

            # Limit history size
            if len(self.optimization_history) > 1000:
                self.optimization_history = self.optimization_history[-1000:]

            logger.info(
                f"Query analysis completed: {len(optimization_suggestions)} suggestions generated"
            )
            return result

        except Exception as e:
            logger.error(f"Query analysis failed: {e}")
            # Return minimal result on failure
            return QueryAnalysisResult(
                query_hash=query_hash,
                original_query=query,
                complexity=QueryComplexityLevel.SIMPLE,
                estimated_cost=0.0,
                table_accesses=[],
                join_patterns=[],
                where_conditions=[],
                aggregations=[],
                subqueries=0,
                optimization_suggestions=[],
            )

    def _analyze_complexity(self, query: str, parsed_query: Statement) -> QueryComplexityLevel:
        """Analyze query complexity"""
        query_upper = query.upper()

        complexity_score = 0

        # Count different complexity factors
        complexity_score += query_upper.count("JOIN") * 2
        complexity_score += query_upper.count("UNION") * 3
        complexity_score += query_upper.count("SUBQUERY") * 4
        complexity_score += query_upper.count("GROUP BY") * 2
        complexity_score += query_upper.count("ORDER BY") * 1
        complexity_score += query_upper.count("HAVING") * 3
        complexity_score += query_upper.count("WINDOW") * 4

        # Classify based on score
        if complexity_score <= 3:
            return QueryComplexityLevel.SIMPLE
        elif complexity_score <= 8:
            return QueryComplexityLevel.MODERATE
        elif complexity_score <= 15:
            return QueryComplexityLevel.COMPLEX
        else:
            return QueryComplexityLevel.VERY_COMPLEX

    def _estimate_cost(self, query: str, parsed_query: Statement) -> float:
        """Estimate query execution cost"""
        # Simplified cost estimation based on query characteristics
        base_cost = 1.0

        query_upper = query.upper()

        # Table access cost
        table_count = len(self._extract_table_accesses(query))
        base_cost += table_count * 10

        # JOIN cost (exponential)
        join_count = query_upper.count("JOIN")
        base_cost += (join_count**2) * 50

        # Aggregation cost
        if "GROUP BY" in query_upper:
            base_cost += 100

        # Sorting cost
        if "ORDER BY" in query_upper:
            base_cost += 50

        # Subquery cost
        subquery_count = self._count_subqueries(query)
        base_cost += subquery_count * 75

        return base_cost

    def _extract_table_accesses(self, query: str) -> list[str]:
        """Extract all table accesses from query"""
        tables = set()

        # Pattern for FROM clauses
        from_pattern = r"FROM\s+(\w+)"
        tables.update(re.findall(from_pattern, query.upper()))

        # Pattern for JOIN clauses
        join_pattern = r"JOIN\s+(\w+)"
        tables.update(re.findall(join_pattern, query.upper()))

        return list(tables)

    def _analyze_join_patterns(self, query: str) -> list[str]:
        """Analyze JOIN patterns in query"""
        patterns = []

        query_upper = query.upper()

        if "INNER JOIN" in query_upper:
            patterns.append("inner_join")
        if "LEFT JOIN" in query_upper:
            patterns.append("left_join")
        if "RIGHT JOIN" in query_upper:
            patterns.append("right_join")
        if "FULL OUTER JOIN" in query_upper:
            patterns.append("full_outer_join")
        if "CROSS JOIN" in query_upper:
            patterns.append("cross_join")

        return patterns

    def _extract_where_conditions(self, query: str) -> list[str]:
        """Extract WHERE condition patterns"""
        conditions = []

        query_upper = query.upper()

        if "WHERE" in query_upper:
            # Extract basic condition patterns
            if re.search(r"WHERE.*=", query_upper):
                conditions.append("equality")
            if re.search(r"WHERE.*[<>]", query_upper):
                conditions.append("comparison")
            if re.search(r"WHERE.*LIKE", query_upper):
                conditions.append("pattern_matching")
            if re.search(r"WHERE.*IN", query_upper):
                conditions.append("set_membership")
            if re.search(r"WHERE.*BETWEEN", query_upper):
                conditions.append("range")

        return conditions

    def _extract_aggregations(self, query: str) -> list[str]:
        """Extract aggregation functions"""
        aggregations = []

        query_upper = query.upper()

        agg_functions = ["COUNT", "SUM", "AVG", "MAX", "MIN", "STDDEV", "VARIANCE"]

        for func in agg_functions:
            if func in query_upper:
                aggregations.append(func.lower())

        return aggregations

    def _count_subqueries(self, query: str) -> int:
        """Count subqueries in the query"""
        # Simple approximation
        pattern = r"\([^()]*SELECT[^()]*\)"
        return len(re.findall(pattern, query.upper()))

    def _generate_optimizations(
        self, query: str, parsed_query: Statement
    ) -> list[QueryOptimizationSuggestion]:
        """Generate optimization suggestions using all patterns"""
        all_suggestions = []

        for pattern in self.patterns:
            try:
                if pattern.detect(query, parsed_query):
                    suggestions = pattern.optimize(query, parsed_query, self.metadata)
                    all_suggestions.extend(suggestions)
            except Exception as e:
                logger.warning(f"Pattern optimization failed: {type(pattern).__name__}: {e}")

        # Sort suggestions by priority and confidence
        priority_order = {"high": 3, "medium": 2, "low": 1}
        all_suggestions.sort(
            key=lambda x: (priority_order.get(x.priority, 0), x.confidence_score), reverse=True
        )

        return all_suggestions

    def optimize_batch_queries(self, queries: list[str]) -> list[QueryAnalysisResult]:
        """Optimize multiple queries in batch"""
        logger.info(f"Starting batch optimization for {len(queries)} queries")

        results = []
        for i, query in enumerate(queries, 1):
            logger.debug(f"Processing query {i}/{len(queries)}")
            result = self.analyze_query(query)
            results.append(result)

        # Generate batch-level insights
        self._analyze_batch_patterns(results)

        logger.info(f"Batch optimization completed: {len(results)} queries analyzed")
        return results

    def _analyze_batch_patterns(self, results: list[QueryAnalysisResult]) -> None:
        """Analyze patterns across multiple queries"""
        # Common table access patterns
        table_frequency = defaultdict(int)
        for result in results:
            for table in result.table_accesses:
                table_frequency[table] += 1

        # Log frequently accessed tables
        frequent_tables = sorted(table_frequency.items(), key=lambda x: x[1], reverse=True)[:5]
        if frequent_tables:
            logger.info(f"Most accessed tables: {frequent_tables}")

    def get_optimization_statistics(self) -> dict[str, Any]:
        """Get comprehensive optimization statistics"""
        return {
            "total_queries_analyzed": self.query_stats["total_analyzed"],
            "total_optimizations_suggested": self.query_stats["total_optimizations"],
            "optimization_types": dict(self.query_stats["optimization_types"]),
            "cache_size": len(self.analysis_cache),
            "history_size": len(self.optimization_history),
            "patterns_registered": len(self.patterns),
        }

    def generate_optimization_report(self) -> dict[str, Any]:
        """Generate comprehensive optimization report"""
        if not self.optimization_history:
            return {"error": "No queries analyzed yet"}

        # Analyze optimization history
        complexity_distribution = defaultdict(int)
        optimization_type_distribution = defaultdict(int)

        total_suggestions = 0

        for result in self.optimization_history:
            complexity_distribution[result.complexity.value] += 1

            for suggestion in result.optimization_suggestions:
                optimization_type_distribution[suggestion.optimization_type.value] += 1
                total_suggestions += 1

        return {
            "report_timestamp": datetime.utcnow().isoformat(),
            "total_queries_analyzed": len(self.optimization_history),
            "total_optimization_suggestions": total_suggestions,
            "avg_suggestions_per_query": total_suggestions / len(self.optimization_history),
            "complexity_distribution": dict(complexity_distribution),
            "optimization_type_distribution": dict(optimization_type_distribution),
            "top_optimization_types": sorted(
                optimization_type_distribution.items(), key=lambda x: x[1], reverse=True
            )[:5],
            "query_statistics": self.get_optimization_statistics(),
        }

    def clear_cache(self) -> int:
        """Clear analysis cache"""
        cache_size = len(self.analysis_cache)
        self.analysis_cache.clear()
        logger.info(f"Cleared {cache_size} cached query analyses")
        return cache_size


# Convenience functions
def create_query_optimizer(engine: Engine) -> AdvancedQueryOptimizer:
    """Create query optimizer instance"""
    return AdvancedQueryOptimizer(engine)


def analyze_sql_query(query: str, engine: Engine) -> QueryAnalysisResult:
    """Quick function to analyze a single query"""
    optimizer = AdvancedQueryOptimizer(engine)
    return optimizer.analyze_query(query)
