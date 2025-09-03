"""
Advanced SQL Transformation and Query Optimization Engine

Comprehensive SQL optimization system with intelligent query rewriting,
execution plan analysis, and performance tuning capabilities.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import sqlparse
from sqlalchemy import Engine, inspect

from core.logging import get_logger

logger = get_logger(__name__)


class OptimizationRule(str, Enum):
    """SQL optimization rule types"""

    INDEX_RECOMMENDATION = "index_recommendation"
    QUERY_REWRITE = "query_rewrite"
    JOIN_OPTIMIZATION = "join_optimization"
    SUBQUERY_ELIMINATION = "subquery_elimination"
    PREDICATE_PUSHDOWN = "predicate_pushdown"
    COLUMN_PRUNING = "column_pruning"
    PARTITION_PRUNING = "partition_pruning"
    AGGREGATION_OPTIMIZATION = "aggregation_optimization"


class OptimizationImpact(str, Enum):
    """Impact level of optimization"""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class QueryAnalysis:
    """SQL query analysis results"""

    query_id: str
    original_query: str
    parsed_query: sqlparse.sql.Statement
    query_type: str  # SELECT, INSERT, UPDATE, DELETE
    tables_accessed: set[str]
    columns_accessed: dict[str, set[str]]  # table -> columns
    join_count: int
    subquery_count: int
    aggregate_functions: list[str]
    where_conditions: list[str]
    order_by_columns: list[str]
    group_by_columns: list[str]
    estimated_complexity: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class OptimizationRecommendation:
    """SQL optimization recommendation"""

    rule_type: OptimizationRule
    impact: OptimizationImpact
    description: str
    original_fragment: str
    optimized_fragment: str | None = None
    estimated_improvement: float | None = None  # Percentage improvement
    confidence_score: float = 0.8
    reasoning: str = ""
    implementation_cost: str = "low"  # low, medium, high


@dataclass
class QueryPerformanceProfile:
    """Query performance profiling data"""

    query_hash: str
    execution_count: int
    avg_execution_time_ms: float
    min_execution_time_ms: float
    max_execution_time_ms: float
    avg_rows_examined: int
    avg_rows_returned: int
    cache_hit_ratio: float
    execution_plan_hash: str
    last_executed: datetime
    optimization_applied: bool = False
    performance_trend: str = "stable"  # improving, degrading, stable


class SQLOptimizer:
    """
    Advanced SQL optimization engine with intelligent query analysis and rewriting.

    Features:
    - Query parsing and analysis
    - Index recommendations
    - Query rewriting for performance
    - Execution plan analysis
    - Performance monitoring
    - Cost-based optimization
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.query_profiles: dict[str, QueryPerformanceProfile] = {}
        self.optimization_history: list[dict[str, Any]] = []

        # Optimization rule weights
        self.rule_weights = {
            OptimizationRule.INDEX_RECOMMENDATION: 1.0,
            OptimizationRule.QUERY_REWRITE: 0.9,
            OptimizationRule.JOIN_OPTIMIZATION: 0.8,
            OptimizationRule.SUBQUERY_ELIMINATION: 0.7,
            OptimizationRule.PREDICATE_PUSHDOWN: 0.8,
            OptimizationRule.COLUMN_PRUNING: 0.6,
            OptimizationRule.PARTITION_PRUNING: 0.9,
            OptimizationRule.AGGREGATION_OPTIMIZATION: 0.7,
        }

        # Database-specific optimizations
        self.db_dialect = str(engine.url.drivername).lower()
        self._initialize_db_specific_rules()

        logger.info(f"SQL Optimizer initialized for {self.db_dialect}")

    def _initialize_db_specific_rules(self):
        """Initialize database-specific optimization rules"""
        if "postgresql" in self.db_dialect:
            self.db_features = {
                "supports_cte": True,
                "supports_window_functions": True,
                "supports_partial_indexes": True,
                "supports_expression_indexes": True,
                "supports_parallel_queries": True,
                "vacuum_analyze_available": True,
            }
        elif "sqlite" in self.db_dialect:
            self.db_features = {
                "supports_cte": True,
                "supports_window_functions": True,
                "supports_partial_indexes": True,
                "supports_expression_indexes": False,
                "supports_parallel_queries": False,
                "vacuum_analyze_available": True,
            }
        else:
            self.db_features = {
                "supports_cte": True,
                "supports_window_functions": True,
                "supports_partial_indexes": False,
                "supports_expression_indexes": False,
                "supports_parallel_queries": False,
                "vacuum_analyze_available": False,
            }

    def analyze_query(self, query: str, query_id: str | None = None) -> QueryAnalysis:
        """Comprehensive SQL query analysis"""
        if not query_id:
            query_id = f"query_{hash(query) % 10000000}"

        try:
            # Parse SQL
            parsed = sqlparse.parse(query)[0]

            # Extract query components
            analysis = QueryAnalysis(
                query_id=query_id,
                original_query=query,
                parsed_query=parsed,
                query_type=self._extract_query_type(parsed),
                tables_accessed=self._extract_tables(parsed),
                columns_accessed=self._extract_columns(parsed),
                join_count=self._count_joins(parsed),
                subquery_count=self._count_subqueries(parsed),
                aggregate_functions=self._extract_aggregates(parsed),
                where_conditions=self._extract_where_conditions(parsed),
                order_by_columns=self._extract_order_by(parsed),
                group_by_columns=self._extract_group_by(parsed),
                estimated_complexity=self._calculate_complexity(parsed),
            )

            logger.debug(
                f"Analyzed query {query_id}: complexity={analysis.estimated_complexity:.2f}"
            )
            return analysis

        except Exception as e:
            logger.error(f"Query analysis failed for {query_id}: {e}")
            # Return basic analysis
            return QueryAnalysis(
                query_id=query_id,
                original_query=query,
                parsed_query=None,
                query_type="UNKNOWN",
                tables_accessed=set(),
                columns_accessed={},
                join_count=0,
                subquery_count=0,
                aggregate_functions=[],
                where_conditions=[],
                order_by_columns=[],
                group_by_columns=[],
                estimated_complexity=0.0,
            )

    def generate_recommendations(self, analysis: QueryAnalysis) -> list[OptimizationRecommendation]:
        """Generate optimization recommendations for query"""
        recommendations = []

        # Index recommendations
        recommendations.extend(self._recommend_indexes(analysis))

        # Query rewrite recommendations
        recommendations.extend(self._recommend_query_rewrites(analysis))

        # Join optimization recommendations
        recommendations.extend(self._recommend_join_optimizations(analysis))

        # Subquery elimination recommendations
        recommendations.extend(self._recommend_subquery_elimination(analysis))

        # Predicate pushdown recommendations
        recommendations.extend(self._recommend_predicate_pushdown(analysis))

        # Column pruning recommendations
        recommendations.extend(self._recommend_column_pruning(analysis))

        # Aggregation optimization recommendations
        recommendations.extend(self._recommend_aggregation_optimizations(analysis))

        # Sort by impact and confidence
        recommendations.sort(key=lambda r: (r.impact.value, r.confidence_score), reverse=True)

        logger.info(
            f"Generated {len(recommendations)} optimization recommendations for {analysis.query_id}"
        )
        return recommendations

    def optimize_query(self, query: str, apply_rewrites: bool = True) -> dict[str, Any]:
        """Optimize SQL query with recommendations and optional rewrites"""
        start_time = time.time()

        # Analyze query
        analysis = self.analyze_query(query)

        # Generate recommendations
        recommendations = self.generate_recommendations(analysis)

        # Apply query rewrites if requested
        optimized_query = query
        applied_optimizations = []

        if apply_rewrites:
            for rec in recommendations:
                if (
                    rec.rule_type == OptimizationRule.QUERY_REWRITE
                    and rec.optimized_fragment
                    and rec.confidence_score > 0.7
                ):
                    try:
                        # Apply the optimization
                        old_fragment = rec.original_fragment
                        new_fragment = rec.optimized_fragment

                        if old_fragment in optimized_query:
                            optimized_query = optimized_query.replace(old_fragment, new_fragment)
                            applied_optimizations.append(rec)
                            logger.debug(f"Applied optimization: {rec.description}")
                    except Exception as e:
                        logger.warning(f"Failed to apply optimization: {e}")

        # Performance estimation
        optimization_time = (time.time() - start_time) * 1000

        result = {
            "original_query": query,
            "optimized_query": optimized_query,
            "analysis": analysis,
            "recommendations": recommendations,
            "applied_optimizations": applied_optimizations,
            "optimization_time_ms": optimization_time,
            "estimated_improvement": self._estimate_improvement(recommendations),
            "query_changed": optimized_query != query,
        }

        # Store optimization history
        self.optimization_history.append(
            {
                "query_id": analysis.query_id,
                "timestamp": datetime.utcnow(),
                "recommendations_count": len(recommendations),
                "optimizations_applied": len(applied_optimizations),
                "estimated_improvement": result["estimated_improvement"],
            }
        )

        return result

    def _recommend_indexes(self, analysis: QueryAnalysis) -> list[OptimizationRecommendation]:
        """Recommend database indexes for query optimization"""
        recommendations = []

        try:
            inspector = inspect(self.engine)

            for table in analysis.tables_accessed:
                if table not in inspector.get_table_names():
                    continue

                existing_indexes = {idx["name"]: idx for idx in inspector.get_indexes(table)}
                table_columns = analysis.columns_accessed.get(table, set())

                # Recommend indexes for WHERE conditions
                for condition in analysis.where_conditions:
                    columns = self._extract_column_references(condition, table)
                    if columns:
                        index_name = f"idx_{table}_{'_'.join(columns)}"

                        if not any(
                            set(columns).issubset(set(idx.get("column_names", [])))
                            for idx in existing_indexes.values()
                        ):
                            recommendations.append(
                                OptimizationRecommendation(
                                    rule_type=OptimizationRule.INDEX_RECOMMENDATION,
                                    impact=OptimizationImpact.HIGH,
                                    description=f"Create index on {table}({', '.join(columns)}) for WHERE clause optimization",
                                    original_fragment=condition,
                                    optimized_fragment=f"CREATE INDEX {index_name} ON {table} ({', '.join(columns)})",
                                    estimated_improvement=25.0,
                                    confidence_score=0.8,
                                    reasoning="Index will improve WHERE clause filtering performance",
                                    implementation_cost="medium",
                                )
                            )

                # Recommend indexes for JOIN conditions
                if analysis.join_count > 0:
                    for column in table_columns:
                        if column.lower().endswith("_id") or column.lower().endswith("_key"):
                            index_name = f"idx_{table}_{column}"

                            if not any(
                                column in idx.get("column_names", [])
                                for idx in existing_indexes.values()
                            ):
                                recommendations.append(
                                    OptimizationRecommendation(
                                        rule_type=OptimizationRule.INDEX_RECOMMENDATION,
                                        impact=OptimizationImpact.HIGH,
                                        description=f"Create index on {table}.{column} for JOIN optimization",
                                        original_fragment=f"{table}.{column}",
                                        optimized_fragment=f"CREATE INDEX {index_name} ON {table} ({column})",
                                        estimated_improvement=30.0,
                                        confidence_score=0.9,
                                        reasoning="Foreign key columns benefit from indexes for JOIN performance",
                                        implementation_cost="low",
                                    )
                                )

                # Recommend composite indexes for ORDER BY
                if analysis.order_by_columns:
                    order_columns = [
                        col for col in analysis.order_by_columns if col in table_columns
                    ]
                    if len(order_columns) > 1:
                        index_name = f"idx_{table}_order_{'_'.join(order_columns)}"

                        recommendations.append(
                            OptimizationRecommendation(
                                rule_type=OptimizationRule.INDEX_RECOMMENDATION,
                                impact=OptimizationImpact.MEDIUM,
                                description="Create composite index for ORDER BY optimization",
                                original_fragment=f"ORDER BY {', '.join(order_columns)}",
                                optimized_fragment=f"CREATE INDEX {index_name} ON {table} ({', '.join(order_columns)})",
                                estimated_improvement=20.0,
                                confidence_score=0.7,
                                reasoning="Composite index will eliminate sorting step",
                                implementation_cost="medium",
                            )
                        )

        except Exception as e:
            logger.warning(f"Index recommendation failed: {e}")

        return recommendations

    def _recommend_query_rewrites(
        self, analysis: QueryAnalysis
    ) -> list[OptimizationRecommendation]:
        """Recommend query rewrite optimizations"""
        recommendations = []

        query = analysis.original_query.upper()

        # Replace SELECT * with specific columns
        if "SELECT *" in query:
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.QUERY_REWRITE,
                    impact=OptimizationImpact.MEDIUM,
                    description="Replace SELECT * with specific column names",
                    original_fragment="SELECT *",
                    optimized_fragment="SELECT column1, column2, ...",
                    estimated_improvement=15.0,
                    confidence_score=0.9,
                    reasoning="Selecting only needed columns reduces I/O and network traffic",
                    implementation_cost="low",
                )
            )

        # LIMIT optimization
        if "LIMIT" not in query and analysis.query_type == "SELECT":
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.QUERY_REWRITE,
                    impact=OptimizationImpact.LOW,
                    description="Consider adding LIMIT clause for large result sets",
                    original_fragment=analysis.original_query,
                    optimized_fragment=None,
                    estimated_improvement=10.0,
                    confidence_score=0.6,
                    reasoning="LIMIT prevents excessive memory usage for large result sets",
                    implementation_cost="low",
                )
            )

        # EXISTS vs IN optimization
        if " IN (" in query and analysis.subquery_count > 0:
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.QUERY_REWRITE,
                    impact=OptimizationImpact.MEDIUM,
                    description="Consider using EXISTS instead of IN with subqueries",
                    original_fragment="column IN (SELECT ...)",
                    optimized_fragment="EXISTS (SELECT 1 FROM ... WHERE ...)",
                    estimated_improvement=20.0,
                    confidence_score=0.7,
                    reasoning="EXISTS can be more efficient than IN for subqueries",
                    implementation_cost="medium",
                )
            )

        return recommendations

    def _recommend_join_optimizations(
        self, analysis: QueryAnalysis
    ) -> list[OptimizationRecommendation]:
        """Recommend JOIN optimization strategies"""
        recommendations = []

        if analysis.join_count == 0:
            return recommendations

        # Multiple table joins optimization
        if analysis.join_count > 3:
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.JOIN_OPTIMIZATION,
                    impact=OptimizationImpact.HIGH,
                    description=f"Query has {analysis.join_count} JOINs - consider query decomposition",
                    original_fragment=analysis.original_query,
                    optimized_fragment=None,
                    estimated_improvement=25.0,
                    confidence_score=0.6,
                    reasoning="Large number of JOINs can be expensive; consider breaking into smaller queries",
                    implementation_cost="high",
                )
            )

        # JOIN order optimization
        recommendations.append(
            OptimizationRecommendation(
                rule_type=OptimizationRule.JOIN_OPTIMIZATION,
                impact=OptimizationImpact.MEDIUM,
                description="Verify JOIN order for optimal performance",
                original_fragment="JOIN operations",
                optimized_fragment=None,
                estimated_improvement=15.0,
                confidence_score=0.5,
                reasoning="JOIN order affects query execution plan and performance",
                implementation_cost="low",
            )
        )

        return recommendations

    def _recommend_subquery_elimination(
        self, analysis: QueryAnalysis
    ) -> list[OptimizationRecommendation]:
        """Recommend subquery elimination optimizations"""
        recommendations = []

        if analysis.subquery_count == 0:
            return recommendations

        recommendations.append(
            OptimizationRecommendation(
                rule_type=OptimizationRule.SUBQUERY_ELIMINATION,
                impact=OptimizationImpact.MEDIUM,
                description=f"Consider rewriting {analysis.subquery_count} subqueries as JOINs",
                original_fragment="subquery",
                optimized_fragment="JOIN",
                estimated_improvement=20.0,
                confidence_score=0.7,
                reasoning="JOINs are often more efficient than correlated subqueries",
                implementation_cost="medium",
            )
        )

        return recommendations

    def _recommend_predicate_pushdown(
        self, analysis: QueryAnalysis
    ) -> list[OptimizationRecommendation]:
        """Recommend predicate pushdown optimizations"""
        recommendations = []

        if analysis.join_count > 0 and analysis.where_conditions:
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.PREDICATE_PUSHDOWN,
                    impact=OptimizationImpact.MEDIUM,
                    description="Ensure WHERE conditions are pushed down to appropriate tables",
                    original_fragment="WHERE conditions",
                    optimized_fragment=None,
                    estimated_improvement=15.0,
                    confidence_score=0.6,
                    reasoning="Filtering early reduces intermediate result set sizes",
                    implementation_cost="low",
                )
            )

        return recommendations

    def _recommend_column_pruning(
        self, analysis: QueryAnalysis
    ) -> list[OptimizationRecommendation]:
        """Recommend column pruning optimizations"""
        recommendations = []

        total_columns = sum(len(cols) for cols in analysis.columns_accessed.values())

        if total_columns > 10:  # Arbitrary threshold
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.COLUMN_PRUNING,
                    impact=OptimizationImpact.LOW,
                    description=f"Query accesses {total_columns} columns - verify all are needed",
                    original_fragment="column selection",
                    optimized_fragment=None,
                    estimated_improvement=10.0,
                    confidence_score=0.5,
                    reasoning="Reducing column count decreases I/O and memory usage",
                    implementation_cost="low",
                )
            )

        return recommendations

    def _recommend_aggregation_optimizations(
        self, analysis: QueryAnalysis
    ) -> list[OptimizationRecommendation]:
        """Recommend aggregation optimization strategies"""
        recommendations = []

        if not analysis.aggregate_functions:
            return recommendations

        # GROUP BY optimization
        if analysis.group_by_columns and analysis.aggregate_functions:
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.AGGREGATION_OPTIMIZATION,
                    impact=OptimizationImpact.MEDIUM,
                    description="Consider materialized views for frequent aggregations",
                    original_fragment=f"GROUP BY {', '.join(analysis.group_by_columns)}",
                    optimized_fragment="materialized view",
                    estimated_improvement=40.0,
                    confidence_score=0.6,
                    reasoning="Materialized views can significantly speed up repeated aggregations",
                    implementation_cost="high",
                )
            )

        # HAVING vs WHERE optimization
        query = analysis.original_query.upper()
        if "HAVING" in query:
            recommendations.append(
                OptimizationRecommendation(
                    rule_type=OptimizationRule.AGGREGATION_OPTIMIZATION,
                    impact=OptimizationImpact.LOW,
                    description="Move non-aggregate conditions from HAVING to WHERE",
                    original_fragment="HAVING clause",
                    optimized_fragment="WHERE clause",
                    estimated_improvement=10.0,
                    confidence_score=0.7,
                    reasoning="WHERE filters before aggregation, HAVING filters after",
                    implementation_cost="low",
                )
            )

        return recommendations

    def profile_query_performance(
        self, query: str, execution_time_ms: float, rows_examined: int, rows_returned: int
    ) -> None:
        """Profile query performance for optimization tracking"""
        query_hash = str(hash(query) % 10000000)

        if query_hash in self.query_profiles:
            profile = self.query_profiles[query_hash]

            # Update statistics
            total_executions = profile.execution_count + 1
            profile.avg_execution_time_ms = (
                profile.avg_execution_time_ms * profile.execution_count + execution_time_ms
            ) / total_executions
            profile.execution_count = total_executions
            profile.min_execution_time_ms = min(profile.min_execution_time_ms, execution_time_ms)
            profile.max_execution_time_ms = max(profile.max_execution_time_ms, execution_time_ms)
            profile.avg_rows_examined = int(
                (profile.avg_rows_examined * (total_executions - 1) + rows_examined)
                / total_executions
            )
            profile.avg_rows_returned = int(
                (profile.avg_rows_returned * (total_executions - 1) + rows_returned)
                / total_executions
            )
            profile.last_executed = datetime.utcnow()

        else:
            # Create new profile
            self.query_profiles[query_hash] = QueryPerformanceProfile(
                query_hash=query_hash,
                execution_count=1,
                avg_execution_time_ms=execution_time_ms,
                min_execution_time_ms=execution_time_ms,
                max_execution_time_ms=execution_time_ms,
                avg_rows_examined=rows_examined,
                avg_rows_returned=rows_returned,
                cache_hit_ratio=0.0,
                execution_plan_hash="",
                last_executed=datetime.utcnow(),
            )

    def get_performance_insights(self, limit: int = 10) -> dict[str, Any]:
        """Get performance insights from query profiling"""
        if not self.query_profiles:
            return {"message": "No query performance data available"}

        profiles = list(self.query_profiles.values())

        # Sort by average execution time (descending)
        slow_queries = sorted(profiles, key=lambda p: p.avg_execution_time_ms, reverse=True)[:limit]

        # Sort by execution count (descending)
        frequent_queries = sorted(profiles, key=lambda p: p.execution_count, reverse=True)[:limit]

        # Calculate overall statistics
        total_executions = sum(p.execution_count for p in profiles)
        avg_execution_time = sum(p.avg_execution_time_ms for p in profiles) / len(profiles)

        insights = {
            "total_queries_profiled": len(profiles),
            "total_executions": total_executions,
            "avg_execution_time_ms": avg_execution_time,
            "slowest_queries": [
                {
                    "query_hash": p.query_hash,
                    "avg_execution_time_ms": p.avg_execution_time_ms,
                    "execution_count": p.execution_count,
                    "avg_rows_examined": p.avg_rows_examined,
                }
                for p in slow_queries
            ],
            "most_frequent_queries": [
                {
                    "query_hash": p.query_hash,
                    "execution_count": p.execution_count,
                    "avg_execution_time_ms": p.avg_execution_time_ms,
                    "last_executed": p.last_executed.isoformat(),
                }
                for p in frequent_queries
            ],
            "optimization_history": self.optimization_history[-20:],  # Last 20 optimizations
            "recommendations": {
                "high_impact_optimizations": len(
                    [h for h in self.optimization_history if h.get("estimated_improvement", 0) > 20]
                ),
                "total_optimizations_suggested": sum(
                    h.get("recommendations_count", 0) for h in self.optimization_history
                ),
            },
        }

        return insights

    def generate_optimization_report(self, query: str) -> dict[str, Any]:
        """Generate comprehensive optimization report for a query"""
        optimization_result = self.optimize_query(query, apply_rewrites=False)

        # Calculate priority scores for recommendations
        prioritized_recommendations = []
        for rec in optimization_result["recommendations"]:
            impact_score = {"low": 1, "medium": 2, "high": 3, "critical": 4}[rec.impact.value]
            priority_score = (
                impact_score * rec.confidence_score * self.rule_weights.get(rec.rule_type, 1.0)
            )

            prioritized_recommendations.append(
                {
                    "recommendation": rec,
                    "priority_score": priority_score,
                    "implementation_order": len(prioritized_recommendations) + 1,
                }
            )

        # Sort by priority score
        prioritized_recommendations.sort(key=lambda x: x["priority_score"], reverse=True)

        report = {
            "query_analysis": {
                "query_type": optimization_result["analysis"].query_type,
                "complexity_score": optimization_result["analysis"].estimated_complexity,
                "tables_count": len(optimization_result["analysis"].tables_accessed),
                "joins_count": optimization_result["analysis"].join_count,
                "subqueries_count": optimization_result["analysis"].subquery_count,
                "aggregations_count": len(optimization_result["analysis"].aggregate_functions),
            },
            "optimization_summary": {
                "total_recommendations": len(optimization_result["recommendations"]),
                "high_impact_recommendations": len(
                    [
                        r
                        for r in optimization_result["recommendations"]
                        if r.impact == OptimizationImpact.HIGH
                    ]
                ),
                "estimated_total_improvement": optimization_result["estimated_improvement"],
                "analysis_time_ms": optimization_result["optimization_time_ms"],
            },
            "prioritized_recommendations": prioritized_recommendations,
            "implementation_plan": self._generate_implementation_plan(prioritized_recommendations),
            "database_specific_notes": self._get_db_specific_notes(optimization_result["analysis"]),
            "generated_at": datetime.utcnow().isoformat(),
        }

        return report

    def _generate_implementation_plan(
        self, prioritized_recommendations: list[dict]
    ) -> dict[str, Any]:
        """Generate implementation plan for recommendations"""
        high_priority = [r for r in prioritized_recommendations if r["priority_score"] > 2.0]
        medium_priority = [
            r for r in prioritized_recommendations if 1.0 <= r["priority_score"] <= 2.0
        ]
        low_priority = [r for r in prioritized_recommendations if r["priority_score"] < 1.0]

        return {
            "immediate_actions": {
                "count": len(high_priority),
                "recommendations": [r["recommendation"].description for r in high_priority[:3]],
            },
            "short_term_actions": {
                "count": len(medium_priority),
                "recommendations": [r["recommendation"].description for r in medium_priority[:3]],
            },
            "long_term_considerations": {
                "count": len(low_priority),
                "recommendations": [r["recommendation"].description for r in low_priority[:3]],
            },
            "estimated_timeline": self._estimate_implementation_timeline(
                prioritized_recommendations
            ),
        }

    def _get_db_specific_notes(self, analysis: QueryAnalysis) -> list[str]:
        """Get database-specific optimization notes"""
        notes = []

        if "postgresql" in self.db_dialect:
            notes.extend(
                [
                    "Consider using EXPLAIN (ANALYZE, BUFFERS) for detailed execution analysis",
                    "PostgreSQL supports partial indexes for conditional filtering",
                    "Use pg_stat_statements extension for query performance monitoring",
                ]
            )
        elif "sqlite" in self.db_dialect:
            notes.extend(
                [
                    "SQLite benefits from ANALYZE command for query planner statistics",
                    "Consider WAL mode for better concurrency",
                    "Use EXPLAIN QUERY PLAN for execution plan analysis",
                ]
            )

        return notes

    def _estimate_implementation_timeline(self, recommendations: list[dict]) -> str:
        """Estimate implementation timeline for recommendations"""
        total_recommendations = len(recommendations)

        if total_recommendations <= 3:
            return "1-2 days"
        elif total_recommendations <= 7:
            return "3-5 days"
        elif total_recommendations <= 15:
            return "1-2 weeks"
        else:
            return "2-4 weeks"

    # Helper methods for query parsing
    def _extract_query_type(self, parsed_query) -> str:
        """Extract query type from parsed SQL"""
        if not parsed_query:
            return "UNKNOWN"

        first_token = str(parsed_query.tokens[0]).upper().strip()
        return first_token

    def _extract_tables(self, parsed_query) -> set[str]:
        """Extract table names from parsed SQL"""
        tables = set()
        if not parsed_query:
            return tables

        # Simple regex-based extraction for common patterns
        query_str = str(parsed_query).upper()
        patterns = [r"FROM\s+(\w+)", r"JOIN\s+(\w+)", r"UPDATE\s+(\w+)", r"INTO\s+(\w+)"]

        for pattern in patterns:
            matches = re.findall(pattern, query_str)
            tables.update(matches)

        return tables

    def _extract_columns(self, parsed_query) -> dict[str, set[str]]:
        """Extract column references from parsed SQL"""
        # Simplified implementation
        return {}

    def _count_joins(self, parsed_query) -> int:
        """Count JOIN operations in query"""
        if not parsed_query:
            return 0
        return str(parsed_query).upper().count("JOIN")

    def _count_subqueries(self, parsed_query) -> int:
        """Count subqueries in query"""
        if not parsed_query:
            return 0
        query_str = str(parsed_query).upper()
        return query_str.count("SELECT") - 1  # Subtract main SELECT

    def _extract_aggregates(self, parsed_query) -> list[str]:
        """Extract aggregate functions from query"""
        if not parsed_query:
            return []

        aggregates = []
        query_str = str(parsed_query).upper()

        for func in ["COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT"]:
            if f"{func}(" in query_str:
                aggregates.append(func)

        return aggregates

    def _extract_where_conditions(self, parsed_query) -> list[str]:
        """Extract WHERE conditions from query"""
        # Simplified implementation
        return []

    def _extract_order_by(self, parsed_query) -> list[str]:
        """Extract ORDER BY columns from query"""
        # Simplified implementation
        return []

    def _extract_group_by(self, parsed_query) -> list[str]:
        """Extract GROUP BY columns from query"""
        # Simplified implementation
        return []

    def _calculate_complexity(self, parsed_query) -> float:
        """Calculate query complexity score"""
        if not parsed_query:
            return 0.0

        query_str = str(parsed_query).upper()

        complexity_score = 0.0

        # Base complexity
        complexity_score += 1.0

        # Add complexity for operations
        complexity_score += query_str.count("JOIN") * 2.0
        complexity_score += (query_str.count("SELECT") - 1) * 3.0  # Subqueries
        complexity_score += query_str.count("GROUP BY") * 1.5
        complexity_score += query_str.count("ORDER BY") * 1.0
        complexity_score += query_str.count("HAVING") * 2.0

        # Add complexity for aggregate functions
        for func in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
            complexity_score += query_str.count(f"{func}(") * 0.5

        return complexity_score

    def _extract_column_references(self, condition: str, table: str) -> list[str]:
        """Extract column references from WHERE condition for specific table"""
        # Simplified implementation
        columns = []
        words = condition.split()

        for word in words:
            if f"{table}." in word:
                column = word.split(".")[-1].strip("(),=<>!")
                columns.append(column)

        return columns

    def _estimate_improvement(self, recommendations: list[OptimizationRecommendation]) -> float:
        """Estimate overall performance improvement from recommendations"""
        if not recommendations:
            return 0.0

        # Weighted average of improvements
        total_weight = sum(rec.confidence_score for rec in recommendations)
        if total_weight == 0:
            return 0.0

        weighted_improvement = sum(
            (rec.estimated_improvement or 0) * rec.confidence_score for rec in recommendations
        )

        return weighted_improvement / total_weight


# Factory function
def create_sql_optimizer(engine: Engine) -> SQLOptimizer:
    """Create SQL optimizer instance"""
    return SQLOptimizer(engine)
