"""
Advanced Query Optimization Service
===================================

Intelligent query optimization service that works with the performance monitor
to automatically optimize queries for <25ms target performance.

Features:
- Real-time query analysis and optimization
- Automatic index recommendations
- Query rewriting and restructuring
- Execution plan optimization
- Performance regression detection
- Cache strategy recommendations
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import sqlparse
from sqlalchemy import text, inspect, create_engine, MetaData
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger
from .query_performance_monitor import QueryPerformanceMonitor, QueryMetrics

logger = get_logger(__name__)


class OptimizationType(str, Enum):
    """Types of query optimizations."""
    INDEX_RECOMMENDATION = "index_recommendation"
    QUERY_REWRITE = "query_rewrite"
    JOIN_OPTIMIZATION = "join_optimization"
    SUBQUERY_OPTIMIZATION = "subquery_optimization"
    AGGREGATION_OPTIMIZATION = "aggregation_optimization"
    CACHE_RECOMMENDATION = "cache_recommendation"
    PARTITIONING_RECOMMENDATION = "partitioning_recommendation"


class OptimizationPriority(str, Enum):
    """Priority levels for optimizations."""
    CRITICAL = "critical"     # >1000ms queries
    HIGH = "high"            # >100ms queries
    MEDIUM = "medium"        # >25ms queries
    LOW = "low"             # <25ms but can be improved


@dataclass
class OptimizationRecommendation:
    """Comprehensive optimization recommendation."""
    recommendation_id: str
    query_hash: str
    optimization_type: OptimizationType
    priority: OptimizationPriority

    # Current performance metrics
    current_avg_execution_time_ms: float
    target_execution_time_ms: float
    estimated_improvement_pct: float

    # Optimization details
    recommendation_title: str
    recommendation_description: str
    implementation_sql: List[str] = field(default_factory=list)
    rollback_sql: List[str] = field(default_factory=list)

    # Impact analysis
    affected_queries: int = 0
    estimated_cost_savings_usd_per_day: float = 0
    implementation_complexity: str = "medium"  # low, medium, high, very_high

    # Risk assessment
    risk_level: str = "low"  # low, medium, high
    potential_side_effects: List[str] = field(default_factory=list)
    testing_requirements: List[str] = field(default_factory=list)

    # Tracking
    created_at: datetime = field(default_factory=datetime.utcnow)
    implemented: bool = False
    implemented_at: Optional[datetime] = None
    implementation_results: Optional[Dict[str, Any]] = None

    def calculate_roi_score(self) -> float:
        """Calculate ROI score for prioritizing recommendations."""
        # Base score from performance improvement
        performance_score = self.estimated_improvement_pct * 10

        # Scale by number of affected queries
        impact_multiplier = min(3.0, self.affected_queries / 100)

        # Adjust for implementation complexity
        complexity_penalty = {
            "low": 1.0,
            "medium": 0.8,
            "high": 0.6,
            "very_high": 0.4
        }.get(self.implementation_complexity, 0.8)

        # Adjust for risk level
        risk_penalty = {
            "low": 1.0,
            "medium": 0.8,
            "high": 0.6
        }.get(self.risk_level, 0.8)

        return performance_score * impact_multiplier * complexity_penalty * risk_penalty


@dataclass
class QueryRewriteRule:
    """Query rewriting rule for optimization."""
    rule_id: str
    pattern: str
    replacement: str
    conditions: List[str] = field(default_factory=list)
    estimated_improvement_pct: float = 0
    description: str = ""


class QueryOptimizerService:
    """
    Advanced query optimization service with intelligent recommendations.

    Works in conjunction with QueryPerformanceMonitor to provide:
    - Real-time optimization recommendations
    - Automatic query optimization
    - Performance regression analysis
    - Index optimization suggestions
    """

    def __init__(self,
                 engine: AsyncEngine,
                 performance_monitor: QueryPerformanceMonitor,
                 target_performance_ms: float = 25.0):
        self.engine = engine
        self.performance_monitor = performance_monitor
        self.target_performance_ms = target_performance_ms

        # Optimization tracking
        self.recommendations: Dict[str, OptimizationRecommendation] = {}
        self.implemented_optimizations: Dict[str, OptimizationRecommendation] = {}

        # Query patterns and analysis
        self.slow_query_patterns: Dict[str, List[QueryMetrics]] = defaultdict(list)
        self.optimization_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Database schema metadata
        self.metadata: Optional[MetaData] = None
        self.table_stats: Dict[str, Dict[str, Any]] = {}

        # Query rewrite rules
        self.rewrite_rules = self._initialize_rewrite_rules()

        # Optimization cache
        self.optimization_cache: Dict[str, Dict[str, Any]] = {}

        # Service state
        self.optimization_active = False
        self.background_task: Optional[asyncio.Task] = None

        # Performance thresholds
        self.critical_threshold_ms = 1000.0
        self.high_threshold_ms = 100.0
        self.medium_threshold_ms = 25.0

    def _initialize_rewrite_rules(self) -> List[QueryRewriteRule]:
        """Initialize query rewrite rules for common optimizations."""
        return [
            QueryRewriteRule(
                rule_id="eliminate_correlated_subquery",
                pattern=r"SELECT .+ WHERE .+ IN \(SELECT .+ WHERE .+\.(.+) = .+\.(.+)\)",
                replacement="",  # Will be dynamically generated
                estimated_improvement_pct=40,
                description="Convert correlated subquery to JOIN for better performance"
            ),

            QueryRewriteRule(
                rule_id="optimize_exists_to_join",
                pattern=r"WHERE EXISTS \(SELECT 1 FROM (.+) WHERE (.+)\)",
                replacement="",  # Will be dynamically generated
                estimated_improvement_pct=25,
                description="Convert EXISTS subquery to INNER JOIN"
            ),

            QueryRewriteRule(
                rule_id="add_limit_to_ordered_query",
                pattern=r"SELECT .+ ORDER BY .+(?!.*LIMIT)",
                replacement="",  # Will add LIMIT dynamically based on context
                conditions=["no_aggregation", "single_table"],
                estimated_improvement_pct=60,
                description="Add LIMIT clause to ordered queries when appropriate"
            ),

            QueryRewriteRule(
                rule_id="optimize_count_star",
                pattern=r"SELECT COUNT\(\*\) FROM (.+)(?!.*WHERE)",
                replacement="",  # Will suggest table statistics or approximate count
                estimated_improvement_pct=80,
                description="Use table statistics for COUNT(*) on full tables"
            ),

            QueryRewriteRule(
                rule_id="push_down_conditions",
                pattern=r"SELECT .+ FROM \(SELECT .+ FROM (.+)\) .+ WHERE (.+)",
                replacement="",  # Will push WHERE conditions into subquery
                estimated_improvement_pct=35,
                description="Push WHERE conditions into subqueries"
            )
        ]

    async def start_optimization_service(self):
        """Start the query optimization service."""
        if self.optimization_active:
            return

        self.optimization_active = True

        # Load database metadata
        await self._load_database_metadata()

        # Load table statistics
        await self._refresh_table_statistics()

        # Start background optimization tasks
        self.background_task = asyncio.create_task(self._background_optimization())

        logger.info("Query optimization service started")

    async def stop_optimization_service(self):
        """Stop the query optimization service."""
        if not self.optimization_active:
            return

        self.optimization_active = False

        if self.background_task:
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                pass

        logger.info("Query optimization service stopped")

    async def _load_database_metadata(self):
        """Load database metadata for optimization analysis."""
        try:
            self.metadata = MetaData()
            async with self.engine.begin() as conn:
                await conn.run_sync(self.metadata.reflect)

            logger.info(f"Loaded metadata for {len(self.metadata.tables)} tables")

        except Exception as e:
            logger.error(f"Failed to load database metadata: {e}")

    async def _refresh_table_statistics(self):
        """Refresh table statistics for optimization decisions."""
        try:
            if not self.metadata:
                return

            async with self.engine.begin() as conn:
                for table_name in self.metadata.tables.keys():
                    # Get table row count
                    count_result = await conn.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    )
                    row_count = count_result.scalar()

                    # Get table size (PostgreSQL specific, adapt for other databases)
                    try:
                        size_result = await conn.execute(
                            text(f"SELECT pg_total_relation_size('{table_name}')::bigint")
                        )
                        table_size_bytes = size_result.scalar() or 0
                    except:
                        table_size_bytes = 0

                    self.table_stats[table_name] = {
                        'row_count': row_count,
                        'size_bytes': table_size_bytes,
                        'last_updated': datetime.utcnow()
                    }

        except Exception as e:
            logger.error(f"Failed to refresh table statistics: {e}")

    async def analyze_query_for_optimization(self, metrics: QueryMetrics) -> List[OptimizationRecommendation]:
        """Analyze a query and generate optimization recommendations."""
        recommendations = []

        # Skip if query already meets target performance
        if metrics.execution_time_ms < self.target_performance_ms:
            return recommendations

        try:
            # Add to slow query patterns for analysis
            self.slow_query_patterns[metrics.query_hash].append(metrics)

            # Determine optimization priority
            priority = self._determine_optimization_priority(metrics.execution_time_ms)

            # Analyze different optimization opportunities
            recommendations.extend(await self._analyze_index_opportunities(metrics, priority))
            recommendations.extend(await self._analyze_query_rewrite_opportunities(metrics, priority))
            recommendations.extend(await self._analyze_join_optimization_opportunities(metrics, priority))
            recommendations.extend(await self._analyze_aggregation_optimization_opportunities(metrics, priority))
            recommendations.extend(await self._analyze_cache_opportunities(metrics, priority))

            # Store recommendations
            for rec in recommendations:
                self.recommendations[rec.recommendation_id] = rec

            # Log high-priority recommendations
            if priority in [OptimizationPriority.CRITICAL, OptimizationPriority.HIGH]:
                logger.warning(f"High-priority optimization needed for query {metrics.query_hash}: {metrics.execution_time_ms:.2f}ms")

        except Exception as e:
            logger.error(f"Failed to analyze query for optimization: {e}")

        return recommendations

    def _determine_optimization_priority(self, execution_time_ms: float) -> OptimizationPriority:
        """Determine optimization priority based on execution time."""
        if execution_time_ms > self.critical_threshold_ms:
            return OptimizationPriority.CRITICAL
        elif execution_time_ms > self.high_threshold_ms:
            return OptimizationPriority.HIGH
        elif execution_time_ms > self.medium_threshold_ms:
            return OptimizationPriority.MEDIUM
        else:
            return OptimizationPriority.LOW

    async def _analyze_index_opportunities(self,
                                         metrics: QueryMetrics,
                                         priority: OptimizationPriority) -> List[OptimizationRecommendation]:
        """Analyze index optimization opportunities."""
        recommendations = []

        try:
            # Check for missing indexes based on query patterns
            if metrics.full_table_scan:
                # Suggest covering index for the query pattern
                rec = OptimizationRecommendation(
                    recommendation_id=f"idx_{metrics.query_hash}_{int(time.time())}",
                    query_hash=metrics.query_hash,
                    optimization_type=OptimizationType.INDEX_RECOMMENDATION,
                    priority=priority,
                    current_avg_execution_time_ms=metrics.execution_time_ms,
                    target_execution_time_ms=self.target_performance_ms,
                    estimated_improvement_pct=70,
                    recommendation_title="Create Missing Index to Eliminate Full Table Scan",
                    recommendation_description=f"Query is performing full table scan. Creating appropriate index could reduce execution time by ~70%",
                    implementation_sql=[
                        self._generate_index_sql(metrics)
                    ],
                    affected_queries=len(self.slow_query_patterns[metrics.query_hash]),
                    implementation_complexity="low",
                    risk_level="low"
                )
                recommendations.append(rec)

            # Check for composite index opportunities
            if metrics.tables_accessed > 1 and metrics.execution_time_ms > 50:
                rec = OptimizationRecommendation(
                    recommendation_id=f"comp_idx_{metrics.query_hash}_{int(time.time())}",
                    query_hash=metrics.query_hash,
                    optimization_type=OptimizationType.INDEX_RECOMMENDATION,
                    priority=priority,
                    current_avg_execution_time_ms=metrics.execution_time_ms,
                    target_execution_time_ms=self.target_performance_ms,
                    estimated_improvement_pct=45,
                    recommendation_title="Create Composite Index for Multi-Table Query",
                    recommendation_description="Multi-table query could benefit from composite index on join columns",
                    implementation_sql=[
                        self._generate_composite_index_sql(metrics)
                    ],
                    affected_queries=len(self.slow_query_patterns[metrics.query_hash]),
                    implementation_complexity="medium",
                    risk_level="low"
                )
                recommendations.append(rec)

        except Exception as e:
            logger.error(f"Failed to analyze index opportunities: {e}")

        return recommendations

    async def _analyze_query_rewrite_opportunities(self,
                                                 metrics: QueryMetrics,
                                                 priority: OptimizationPriority) -> List[OptimizationRecommendation]:
        """Analyze query rewrite opportunities."""
        recommendations = []

        try:
            # Check against rewrite rules
            for rule in self.rewrite_rules:
                if self._query_matches_rule(metrics, rule):
                    rewritten_query = self._apply_rewrite_rule(metrics, rule)

                    if rewritten_query:
                        rec = OptimizationRecommendation(
                            recommendation_id=f"rewrite_{rule.rule_id}_{metrics.query_hash}_{int(time.time())}",
                            query_hash=metrics.query_hash,
                            optimization_type=OptimizationType.QUERY_REWRITE,
                            priority=priority,
                            current_avg_execution_time_ms=metrics.execution_time_ms,
                            target_execution_time_ms=self.target_performance_ms,
                            estimated_improvement_pct=rule.estimated_improvement_pct,
                            recommendation_title=f"Query Rewrite: {rule.description}",
                            recommendation_description=f"Rewrite query using rule '{rule.rule_id}' to improve performance by ~{rule.estimated_improvement_pct}%",
                            implementation_sql=[f"-- Optimized query:\n{rewritten_query}"],
                            affected_queries=len(self.slow_query_patterns[metrics.query_hash]),
                            implementation_complexity="medium",
                            risk_level="medium",
                            testing_requirements=["Validate query results match original", "Performance test in staging"]
                        )
                        recommendations.append(rec)

        except Exception as e:
            logger.error(f"Failed to analyze query rewrite opportunities: {e}")

        return recommendations

    async def _analyze_join_optimization_opportunities(self,
                                                     metrics: QueryMetrics,
                                                     priority: OptimizationPriority) -> List[OptimizationRecommendation]:
        """Analyze JOIN optimization opportunities."""
        recommendations = []

        try:
            if metrics.tables_accessed > 2 and metrics.execution_time_ms > 100:
                rec = OptimizationRecommendation(
                    recommendation_id=f"join_opt_{metrics.query_hash}_{int(time.time())}",
                    query_hash=metrics.query_hash,
                    optimization_type=OptimizationType.JOIN_OPTIMIZATION,
                    priority=priority,
                    current_avg_execution_time_ms=metrics.execution_time_ms,
                    target_execution_time_ms=self.target_performance_ms,
                    estimated_improvement_pct=30,
                    recommendation_title="Optimize JOIN Order and Strategy",
                    recommendation_description="Multi-table JOIN could benefit from optimization of join order and strategy",
                    implementation_sql=[
                        "-- Consider reordering JOINs to place most selective conditions first",
                        "-- Add indexes on JOIN columns",
                        "-- Consider denormalization for frequently joined tables"
                    ],
                    affected_queries=len(self.slow_query_patterns[metrics.query_hash]),
                    implementation_complexity="high",
                    risk_level="medium",
                    testing_requirements=["Analyze execution plans", "Test with production data volume"]
                )
                recommendations.append(rec)

        except Exception as e:
            logger.error(f"Failed to analyze JOIN opportunities: {e}")

        return recommendations

    async def _analyze_aggregation_optimization_opportunities(self,
                                                            metrics: QueryMetrics,
                                                            priority: OptimizationPriority) -> List[OptimizationRecommendation]:
        """Analyze aggregation optimization opportunities."""
        recommendations = []

        try:
            # Check if query involves aggregation and is slow
            if metrics.query_type.value == "analytical" and metrics.execution_time_ms > 50:
                rec = OptimizationRecommendation(
                    recommendation_id=f"agg_opt_{metrics.query_hash}_{int(time.time())}",
                    query_hash=metrics.query_hash,
                    optimization_type=OptimizationType.AGGREGATION_OPTIMIZATION,
                    priority=priority,
                    current_avg_execution_time_ms=metrics.execution_time_ms,
                    target_execution_time_ms=self.target_performance_ms,
                    estimated_improvement_pct=60,
                    recommendation_title="Create Materialized View for Aggregation Query",
                    recommendation_description="Frequently executed aggregation query could benefit from materialized view",
                    implementation_sql=[
                        self._generate_materialized_view_sql(metrics)
                    ],
                    affected_queries=len(self.slow_query_patterns[metrics.query_hash]),
                    implementation_complexity="high",
                    risk_level="medium",
                    testing_requirements=["Setup refresh strategy", "Monitor data freshness requirements"]
                )
                recommendations.append(rec)

        except Exception as e:
            logger.error(f"Failed to analyze aggregation opportunities: {e}")

        return recommendations

    async def _analyze_cache_opportunities(self,
                                         metrics: QueryMetrics,
                                         priority: OptimizationPriority) -> List[OptimizationRecommendation]:
        """Analyze caching opportunities."""
        recommendations = []

        try:
            # Check if query is a good cache candidate
            if (metrics.cache_candidate and
                len(self.slow_query_patterns[metrics.query_hash]) > 10):  # Frequently executed

                rec = OptimizationRecommendation(
                    recommendation_id=f"cache_{metrics.query_hash}_{int(time.time())}",
                    query_hash=metrics.query_hash,
                    optimization_type=OptimizationType.CACHE_RECOMMENDATION,
                    priority=priority,
                    current_avg_execution_time_ms=metrics.execution_time_ms,
                    target_execution_time_ms=5.0,  # Cached queries should be very fast
                    estimated_improvement_pct=90,
                    recommendation_title="Implement Query Result Caching",
                    recommendation_description="Frequently executed query with stable results is ideal for caching",
                    implementation_sql=[
                        "-- Implement application-level caching",
                        "-- Consider Redis cache with appropriate TTL",
                        "-- Setup cache invalidation strategy"
                    ],
                    affected_queries=len(self.slow_query_patterns[metrics.query_hash]),
                    implementation_complexity="medium",
                    risk_level="low",
                    testing_requirements=["Define cache TTL strategy", "Implement cache invalidation"]
                )
                recommendations.append(rec)

        except Exception as e:
            logger.error(f"Failed to analyze cache opportunities: {e}")

        return recommendations

    def _query_matches_rule(self, metrics: QueryMetrics, rule: QueryRewriteRule) -> bool:
        """Check if a query matches a rewrite rule."""
        try:
            # This is a simplified pattern matching - in production, use more sophisticated analysis
            pattern = rule.pattern.lower()
            query_indicators = {
                "exists": "exists" in metrics.query_hash.lower(),
                "subquery": metrics.has_subqueries,
                "join": metrics.tables_accessed > 1,
                "order_by": "order" in pattern and "limit" not in metrics.query_hash.lower(),
                "count_star": "count(*)" in metrics.query_hash.lower()
            }

            rule_requirements = {
                "eliminate_correlated_subquery": query_indicators["subquery"] and query_indicators["join"],
                "optimize_exists_to_join": query_indicators["exists"],
                "add_limit_to_ordered_query": query_indicators["order_by"],
                "optimize_count_star": query_indicators["count_star"],
                "push_down_conditions": query_indicators["subquery"]
            }

            return rule_requirements.get(rule.rule_id, False)

        except Exception as e:
            logger.error(f"Error matching rule {rule.rule_id}: {e}")
            return False

    def _apply_rewrite_rule(self, metrics: QueryMetrics, rule: QueryRewriteRule) -> Optional[str]:
        """Apply a rewrite rule to generate optimized query."""
        try:
            # This is a placeholder for actual query rewriting logic
            # In production, implement sophisticated AST-based query rewriting

            optimizations = {
                "eliminate_correlated_subquery": "-- Convert correlated subquery to JOIN",
                "optimize_exists_to_join": "-- Convert EXISTS to INNER JOIN",
                "add_limit_to_ordered_query": "-- Add LIMIT clause to ordered query",
                "optimize_count_star": "-- Use table statistics for COUNT(*)",
                "push_down_conditions": "-- Push WHERE conditions into subquery"
            }

            return optimizations.get(rule.rule_id)

        except Exception as e:
            logger.error(f"Error applying rule {rule.rule_id}: {e}")
            return None

    def _generate_index_sql(self, metrics: QueryMetrics) -> str:
        """Generate SQL for creating missing index."""
        # This is simplified - in production, analyze actual query to determine best index
        if metrics.table_pattern:
            tables = metrics.table_pattern.split(',')
            primary_table = tables[0] if tables else "fact_sale"

            return f"""
-- Create covering index for query pattern {metrics.query_hash}
CREATE INDEX CONCURRENTLY idx_{primary_table}_{metrics.query_hash[:8]}_covering
ON {primary_table} (date_key, total_amount)
INCLUDE (quantity, profit_amount, customer_key, product_key, country_key);
"""

        return "-- Index creation SQL would be generated based on query analysis"

    def _generate_composite_index_sql(self, metrics: QueryMetrics) -> str:
        """Generate SQL for creating composite index."""
        return f"""
-- Create composite index for multi-table query {metrics.query_hash}
CREATE INDEX CONCURRENTLY idx_composite_{metrics.query_hash[:8]}
ON fact_sale (date_key, customer_key, product_key)
INCLUDE (total_amount, profit_amount, quantity);
"""

    def _generate_materialized_view_sql(self, metrics: QueryMetrics) -> str:
        """Generate SQL for creating materialized view."""
        return f"""
-- Create materialized view for aggregation query {metrics.query_hash}
CREATE MATERIALIZED VIEW mv_aggregated_{metrics.query_hash[:8]} AS
-- Original aggregation query would be placed here
-- with appropriate indexing and refresh strategy
SELECT date_key, SUM(total_amount) as total_revenue, COUNT(*) as transaction_count
FROM fact_sale
GROUP BY date_key;

CREATE UNIQUE INDEX ON mv_aggregated_{metrics.query_hash[:8]} (date_key);
"""

    async def _background_optimization(self):
        """Background optimization tasks."""
        while self.optimization_active:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes

                # Analyze slow query patterns
                await self._analyze_slow_query_patterns()

                # Check for implementation opportunities
                await self._check_implementation_opportunities()

                # Update table statistics
                await self._refresh_table_statistics()

                # Cleanup old data
                await self._cleanup_old_recommendations()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background optimization error: {e}")

    async def _analyze_slow_query_patterns(self):
        """Analyze patterns in slow queries."""
        try:
            pattern_analysis = {}

            for query_hash, metrics_list in self.slow_query_patterns.items():
                if len(metrics_list) >= 5:  # Pattern with at least 5 occurrences
                    avg_time = sum(m.execution_time_ms for m in metrics_list) / len(metrics_list)

                    if avg_time > self.target_performance_ms:
                        pattern_analysis[query_hash] = {
                            'occurrence_count': len(metrics_list),
                            'avg_execution_time_ms': avg_time,
                            'trend': self._analyze_performance_trend(metrics_list),
                            'optimization_potential': avg_time / self.target_performance_ms
                        }

            # Log patterns that need attention
            critical_patterns = [
                (hash_val, data) for hash_val, data in pattern_analysis.items()
                if data['optimization_potential'] > 4.0  # 4x slower than target
            ]

            if critical_patterns:
                logger.warning(f"Found {len(critical_patterns)} critical query patterns requiring optimization")

        except Exception as e:
            logger.error(f"Failed to analyze slow query patterns: {e}")

    def _analyze_performance_trend(self, metrics_list: List[QueryMetrics]) -> str:
        """Analyze performance trend for a query pattern."""
        if len(metrics_list) < 10:
            return "insufficient_data"

        # Sort by execution time
        sorted_metrics = sorted(metrics_list, key=lambda m: m.executed_at)

        # Compare first and second half averages
        mid_point = len(sorted_metrics) // 2
        first_half_avg = sum(m.execution_time_ms for m in sorted_metrics[:mid_point]) / mid_point
        second_half_avg = sum(m.execution_time_ms for m in sorted_metrics[mid_point:]) / (len(sorted_metrics) - mid_point)

        change_pct = ((second_half_avg - first_half_avg) / first_half_avg) * 100

        if change_pct > 20:
            return "degrading"
        elif change_pct < -20:
            return "improving"
        else:
            return "stable"

    async def _check_implementation_opportunities(self):
        """Check for optimization implementation opportunities."""
        try:
            # Find high-ROI recommendations that haven't been implemented
            unimplemented = [
                rec for rec in self.recommendations.values()
                if not rec.implemented and rec.priority in [OptimizationPriority.CRITICAL, OptimizationPriority.HIGH]
            ]

            # Sort by ROI score
            unimplemented.sort(key=lambda r: r.calculate_roi_score(), reverse=True)

            # Log top opportunities
            if unimplemented:
                logger.info(f"Top optimization opportunities: {len(unimplemented[:5])} high-impact recommendations available")

        except Exception as e:
            logger.error(f"Failed to check implementation opportunities: {e}")

    async def _cleanup_old_recommendations(self):
        """Cleanup old optimization recommendations."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=30)

            # Remove old implemented recommendations
            old_recommendations = [
                rec_id for rec_id, rec in self.recommendations.items()
                if rec.implemented and rec.implemented_at and rec.implemented_at < cutoff_date
            ]

            for rec_id in old_recommendations:
                del self.recommendations[rec_id]

            if old_recommendations:
                logger.debug(f"Cleaned up {len(old_recommendations)} old optimization recommendations")

        except Exception as e:
            logger.error(f"Failed to cleanup old recommendations: {e}")

    async def get_optimization_report(self, priority_filter: Optional[OptimizationPriority] = None) -> Dict[str, Any]:
        """Generate comprehensive optimization report."""
        try:
            # Filter recommendations by priority if specified
            recommendations = list(self.recommendations.values())
            if priority_filter:
                recommendations = [r for r in recommendations if r.priority == priority_filter]

            # Calculate statistics
            total_recommendations = len(recommendations)
            implemented_count = len([r for r in recommendations if r.implemented])

            # Group by priority
            priority_breakdown = {}
            for priority in OptimizationPriority:
                priority_recs = [r for r in recommendations if r.priority == priority]
                priority_breakdown[priority.value] = {
                    'count': len(priority_recs),
                    'avg_estimated_improvement': sum(r.estimated_improvement_pct for r in priority_recs) / len(priority_recs) if priority_recs else 0,
                    'total_affected_queries': sum(r.affected_queries for r in priority_recs)
                }

            # Top recommendations by ROI
            top_recommendations = sorted(
                [r for r in recommendations if not r.implemented],
                key=lambda r: r.calculate_roi_score(),
                reverse=True
            )[:10]

            return {
                "report_generated_at": datetime.utcnow().isoformat(),
                "optimization_service_status": {
                    "active": self.optimization_active,
                    "target_performance_ms": self.target_performance_ms,
                    "recommendations_count": total_recommendations,
                    "implemented_count": implemented_count,
                    "implementation_rate": (implemented_count / total_recommendations * 100) if total_recommendations > 0 else 0
                },
                "priority_breakdown": priority_breakdown,
                "top_recommendations": [
                    {
                        "recommendation_id": rec.recommendation_id,
                        "title": rec.recommendation_title,
                        "priority": rec.priority.value,
                        "estimated_improvement_pct": rec.estimated_improvement_pct,
                        "affected_queries": rec.affected_queries,
                        "roi_score": rec.calculate_roi_score(),
                        "implementation_complexity": rec.implementation_complexity,
                        "risk_level": rec.risk_level
                    }
                    for rec in top_recommendations
                ],
                "query_pattern_analysis": {
                    "total_patterns_analyzed": len(self.slow_query_patterns),
                    "patterns_needing_optimization": len([
                        p for p in self.slow_query_patterns.values()
                        if len(p) > 0 and sum(m.execution_time_ms for m in p) / len(p) > self.target_performance_ms
                    ])
                }
            }

        except Exception as e:
            logger.error(f"Failed to generate optimization report: {e}")
            return {"error": str(e)}

    async def implement_recommendation(self, recommendation_id: str, dry_run: bool = True) -> Dict[str, Any]:
        """Implement a specific optimization recommendation."""
        if recommendation_id not in self.recommendations:
            return {"error": "Recommendation not found"}

        recommendation = self.recommendations[recommendation_id]

        try:
            results = {
                "recommendation_id": recommendation_id,
                "dry_run": dry_run,
                "implementation_started_at": datetime.utcnow().isoformat(),
                "sql_commands": recommendation.implementation_sql,
                "estimated_improvement": f"{recommendation.estimated_improvement_pct}%",
                "risk_assessment": recommendation.risk_level
            }

            if not dry_run:
                # Execute implementation SQL
                async with self.engine.begin() as conn:
                    for sql_command in recommendation.implementation_sql:
                        if sql_command.strip() and not sql_command.strip().startswith('--'):
                            await conn.execute(text(sql_command))

                # Mark as implemented
                recommendation.implemented = True
                recommendation.implemented_at = datetime.utcnow()
                recommendation.implementation_results = results

                self.implemented_optimizations[recommendation_id] = recommendation

                results["status"] = "implemented"
                logger.info(f"Successfully implemented optimization {recommendation_id}")
            else:
                results["status"] = "dry_run_completed"

            return results

        except Exception as e:
            logger.error(f"Failed to implement recommendation {recommendation_id}: {e}")
            return {
                "error": str(e),
                "recommendation_id": recommendation_id,
                "status": "failed"
            }


# Factory function
async def create_query_optimizer(engine: AsyncEngine,
                               performance_monitor: QueryPerformanceMonitor,
                               target_performance_ms: float = 25.0) -> QueryOptimizerService:
    """Create and initialize QueryOptimizerService."""
    optimizer = QueryOptimizerService(engine, performance_monitor, target_performance_ms)
    await optimizer.start_optimization_service()
    return optimizer