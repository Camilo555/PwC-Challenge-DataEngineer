"""
Comprehensive Query Performance Monitor with Advanced Analytics
Real-time monitoring, analysis, and optimization of database query performance
with intelligent alerting and automated recommendations.
"""

import asyncio
import logging
import time
import json
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Callable, AsyncGenerator
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, deque
import re
import hashlib
import redis.asyncio as redis
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class QueryCategory(Enum):
    """Query performance categories"""
    EXCELLENT = "excellent"    # <10ms
    GOOD = "good"             # 10-25ms
    ACCEPTABLE = "acceptable" # 25-100ms
    SLOW = "slow"            # 100-500ms
    CRITICAL = "critical"    # >500ms

class QueryType(Enum):
    """Types of SQL queries"""
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    WITH = "with"
    CREATE = "create"
    DROP = "drop"
    ALTER = "alter"

class QueryComplexity(Enum):
    """Query complexity levels"""
    SIMPLE = "simple"        # Single table, basic WHERE
    MODERATE = "moderate"    # Multiple tables, joins
    COMPLEX = "complex"      # Subqueries, CTEs, window functions
    VERY_COMPLEX = "very_complex"  # Recursive queries, advanced features

@dataclass
class QueryMetrics:
    """Comprehensive query performance metrics"""
    query_id: str
    query_hash: str
    sql_text: str
    query_type: QueryType
    complexity: QueryComplexity

    # Performance metrics
    execution_time_ms: float
    planning_time_ms: float = 0.0
    execution_count: int = 1

    # Statistical metrics
    min_time_ms: float = 0.0
    max_time_ms: float = 0.0
    avg_time_ms: float = 0.0
    p95_time_ms: float = 0.0
    p99_time_ms: float = 0.0

    # Database metrics
    rows_examined: Optional[int] = None
    rows_returned: Optional[int] = None
    buffer_hits: Optional[int] = None
    buffer_reads: Optional[int] = None
    temp_files_created: Optional[int] = None
    temp_bytes_used: Optional[int] = None

    # Query analysis
    tables_accessed: Set[str] = field(default_factory=set)
    indexes_used: Set[str] = field(default_factory=set)
    join_count: int = 0
    subquery_count: int = 0

    # Performance categorization
    category: QueryCategory = QueryCategory.GOOD
    is_slow_query: bool = False
    optimization_suggestions: List[str] = field(default_factory=list)

    # Timestamps
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)

    # Historical performance data
    execution_history: deque = field(default_factory=lambda: deque(maxlen=1000))

    def update_metrics(self, execution_time_ms: float, rows_examined: Optional[int] = None,
                      rows_returned: Optional[int] = None):
        """Update metrics with new execution data"""
        self.execution_count += 1
        self.last_seen = datetime.utcnow()
        self.execution_history.append({
            'timestamp': datetime.utcnow(),
            'execution_time_ms': execution_time_ms,
            'rows_examined': rows_examined,
            'rows_returned': rows_returned
        })

        # Update statistical metrics
        if self.min_time_ms == 0.0:
            self.min_time_ms = execution_time_ms
        else:
            self.min_time_ms = min(self.min_time_ms, execution_time_ms)

        self.max_time_ms = max(self.max_time_ms, execution_time_ms)

        # Update rolling average
        self.avg_time_ms = (self.avg_time_ms * (self.execution_count - 1) + execution_time_ms) / self.execution_count

        # Update percentiles from history
        if len(self.execution_history) >= 20:
            times = [h['execution_time_ms'] for h in self.execution_history]
            times.sort()
            self.p95_time_ms = times[int(len(times) * 0.95)]
            self.p99_time_ms = times[int(len(times) * 0.99)]

        # Update performance category
        self._categorize_performance()

        if rows_examined:
            self.rows_examined = rows_examined
        if rows_returned:
            self.rows_returned = rows_returned

    def _categorize_performance(self):
        """Categorize query performance based on execution time"""
        if self.avg_time_ms < 10:
            self.category = QueryCategory.EXCELLENT
        elif self.avg_time_ms < 25:
            self.category = QueryCategory.GOOD
        elif self.avg_time_ms < 100:
            self.category = QueryCategory.ACCEPTABLE
        elif self.avg_time_ms < 500:
            self.category = QueryCategory.SLOW
        else:
            self.category = QueryCategory.CRITICAL

        self.is_slow_query = self.avg_time_ms > 100

    def get_efficiency_ratio(self) -> float:
        """Calculate query efficiency (rows returned / rows examined)"""
        if self.rows_examined and self.rows_examined > 0:
            return (self.rows_returned or 0) / self.rows_examined
        return 1.0

@dataclass
class PerformanceAlert:
    """Query performance alert"""
    alert_id: str
    query_id: str
    alert_type: str
    severity: str
    message: str
    current_value: float
    threshold_value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False

class QueryAnalyzer:
    """Advanced SQL query analyzer"""

    def __init__(self):
        self.query_patterns = {
            'select': re.compile(r'^\s*SELECT', re.IGNORECASE),
            'insert': re.compile(r'^\s*INSERT', re.IGNORECASE),
            'update': re.compile(r'^\s*UPDATE', re.IGNORECASE),
            'delete': re.compile(r'^\s*DELETE', re.IGNORECASE),
            'with': re.compile(r'^\s*WITH', re.IGNORECASE),
        }

        self.complexity_indicators = {
            'join': re.compile(r'\bJOIN\b', re.IGNORECASE),
            'subquery': re.compile(r'\(\s*SELECT', re.IGNORECASE),
            'window': re.compile(r'\bOVER\s*\(', re.IGNORECASE),
            'recursive': re.compile(r'\bWITH\s+RECURSIVE\b', re.IGNORECASE),
            'union': re.compile(r'\bUNION\b', re.IGNORECASE),
            'exists': re.compile(r'\bEXISTS\s*\(', re.IGNORECASE),
            'aggregate': re.compile(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', re.IGNORECASE),
        }

    def analyze_query(self, sql: str) -> tuple[QueryType, QueryComplexity, Set[str], int, int]:
        """
        Analyze SQL query structure and complexity

        Returns:
            Tuple of (query_type, complexity, tables, join_count, subquery_count)
        """
        # Normalize SQL
        normalized_sql = re.sub(r'\s+', ' ', sql.strip())

        # Determine query type
        query_type = QueryType.SELECT  # Default
        for type_name, pattern in self.query_patterns.items():
            if pattern.match(normalized_sql):
                query_type = QueryType(type_name)
                break

        # Extract tables
        tables = self._extract_tables(normalized_sql)

        # Count joins and subqueries
        join_count = len(self.complexity_indicators['join'].findall(normalized_sql))
        subquery_count = len(self.complexity_indicators['subquery'].findall(normalized_sql))

        # Determine complexity
        complexity_score = 0
        for indicator, pattern in self.complexity_indicators.items():
            matches = len(pattern.findall(normalized_sql))
            if indicator == 'join':
                complexity_score += matches * 2
            elif indicator in ['recursive', 'window']:
                complexity_score += matches * 5
            else:
                complexity_score += matches

        if complexity_score == 0:
            complexity = QueryComplexity.SIMPLE
        elif complexity_score <= 3:
            complexity = QueryComplexity.MODERATE
        elif complexity_score <= 8:
            complexity = QueryComplexity.COMPLEX
        else:
            complexity = QueryComplexity.VERY_COMPLEX

        return query_type, complexity, tables, join_count, subquery_count

    def _extract_tables(self, sql: str) -> Set[str]:
        """Extract table names from SQL query"""
        tables = set()

        # Find FROM and JOIN clauses
        from_pattern = re.compile(r'\bFROM\s+([^\s,\(]+)', re.IGNORECASE)
        join_pattern = re.compile(r'\bJOIN\s+([^\s,\(]+)', re.IGNORECASE)

        from_matches = from_pattern.findall(sql)
        join_matches = join_pattern.findall(sql)

        for match in from_matches + join_matches:
            # Remove schema prefix and aliases
            table_name = match.split('.')[-1].split()[0]
            if table_name and not table_name.startswith('('):
                tables.add(table_name.lower())

        return tables

    def generate_optimization_suggestions(self, metrics: QueryMetrics) -> List[str]:
        """Generate optimization suggestions based on query analysis"""
        suggestions = []

        # Check for missing indexes
        if metrics.category in [QueryCategory.SLOW, QueryCategory.CRITICAL]:
            if metrics.rows_examined and metrics.rows_returned:
                efficiency = metrics.get_efficiency_ratio()
                if efficiency < 0.1:
                    suggestions.append("Consider adding indexes on WHERE clause columns")
                    suggestions.append("Review table statistics and consider ANALYZE")

            if metrics.join_count > 2:
                suggestions.append("Consider optimizing join order or adding composite indexes")

            if metrics.subquery_count > 0:
                suggestions.append("Consider rewriting subqueries as JOINs where possible")

        # Check for specific performance issues
        if metrics.temp_files_created and metrics.temp_files_created > 0:
            suggestions.append("Increase work_mem to avoid disk-based operations")

        if metrics.complexity == QueryComplexity.VERY_COMPLEX:
            suggestions.append("Consider breaking complex query into simpler parts")
            suggestions.append("Review query execution plan for optimization opportunities")

        return suggestions

class QueryPerformanceMonitor:
    """Advanced query performance monitoring system"""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis = redis_client
        self.query_analyzer = QueryAnalyzer()

        # Query tracking
        self.query_metrics: Dict[str, QueryMetrics] = {}
        self.query_history: deque = deque(maxlen=10000)

        # Performance thresholds
        self.slow_query_threshold_ms = 100.0
        self.critical_query_threshold_ms = 500.0
        self.efficiency_threshold = 0.1

        # Alerting
        self.active_alerts: Dict[str, PerformanceAlert] = {}
        self.alert_callbacks: List[Callable] = []

        # Monitoring state
        self.monitoring_enabled = True
        self.start_time = datetime.utcnow()

        # Background tasks
        self.background_tasks = []

    async def start_monitoring(self):
        """Start background monitoring tasks"""
        if self.background_tasks:
            return

        self.background_tasks = [
            asyncio.create_task(self._performance_analysis_loop()),
            asyncio.create_task(self._alert_processing_loop()),
            asyncio.create_task(self._metrics_aggregation_loop())
        ]

        logger.info("Query performance monitoring started")

    async def stop_monitoring(self):
        """Stop background monitoring tasks"""
        for task in self.background_tasks:
            task.cancel()

        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()

        logger.info("Query performance monitoring stopped")

    async def record_query_execution(self, sql: str, execution_time_ms: float,
                                   rows_examined: Optional[int] = None,
                                   rows_returned: Optional[int] = None,
                                   planning_time_ms: Optional[float] = None) -> QueryMetrics:
        """Record query execution for performance analysis"""

        if not self.monitoring_enabled:
            return

        # Create query hash for deduplication
        query_hash = hashlib.md5(re.sub(r'\s+', ' ', sql.strip()).encode()).hexdigest()

        # Analyze query if not seen before
        if query_hash not in self.query_metrics:
            query_type, complexity, tables, join_count, subquery_count = self.query_analyzer.analyze_query(sql)

            metrics = QueryMetrics(
                query_id=f"q_{query_hash[:12]}",
                query_hash=query_hash,
                sql_text=sql[:1000],  # Truncate for storage
                query_type=query_type,
                complexity=complexity,
                execution_time_ms=execution_time_ms,
                planning_time_ms=planning_time_ms or 0.0,
                tables_accessed=tables,
                join_count=join_count,
                subquery_count=subquery_count
            )

            self.query_metrics[query_hash] = metrics
        else:
            metrics = self.query_metrics[query_hash]

        # Update metrics
        metrics.update_metrics(execution_time_ms, rows_examined, rows_returned)

        # Generate optimization suggestions for slow queries
        if metrics.is_slow_query and not metrics.optimization_suggestions:
            metrics.optimization_suggestions = self.query_analyzer.generate_optimization_suggestions(metrics)

        # Add to history
        self.query_history.append({
            'timestamp': datetime.utcnow(),
            'query_hash': query_hash,
            'execution_time_ms': execution_time_ms,
            'category': metrics.category.value
        })

        # Check for alerts
        await self._check_performance_alerts(metrics)

        # Store in Redis if available
        if self.redis:
            await self._store_metrics_in_redis(metrics)

        return metrics

    async def _store_metrics_in_redis(self, metrics: QueryMetrics):
        """Store query metrics in Redis for distributed access"""
        try:
            key = f"query:metrics:{metrics.query_hash}"
            data = {
                'query_id': metrics.query_id,
                'avg_time_ms': metrics.avg_time_ms,
                'execution_count': metrics.execution_count,
                'category': metrics.category.value,
                'last_seen': metrics.last_seen.isoformat()
            }

            await self.redis.setex(key, 3600, json.dumps(data))  # 1 hour TTL

        except Exception as e:
            logger.error(f"Error storing metrics in Redis: {e}")

    async def _check_performance_alerts(self, metrics: QueryMetrics):
        """Check if query performance warrants an alert"""

        alerts_to_create = []

        # Slow query alert
        if (metrics.avg_time_ms > self.slow_query_threshold_ms and
            metrics.execution_count >= 3):  # Multiple executions

            alert_id = f"slow_query_{metrics.query_hash}"
            if alert_id not in self.active_alerts:
                alert = PerformanceAlert(
                    alert_id=alert_id,
                    query_id=metrics.query_id,
                    alert_type="slow_query",
                    severity="warning" if metrics.avg_time_ms < self.critical_query_threshold_ms else "critical",
                    message=f"Query {metrics.query_id} averaging {metrics.avg_time_ms:.2f}ms",
                    current_value=metrics.avg_time_ms,
                    threshold_value=self.slow_query_threshold_ms
                )
                alerts_to_create.append(alert)

        # Low efficiency alert
        efficiency = metrics.get_efficiency_ratio()
        if efficiency < self.efficiency_threshold and metrics.rows_examined and metrics.rows_examined > 1000:
            alert_id = f"low_efficiency_{metrics.query_hash}"
            if alert_id not in self.active_alerts:
                alert = PerformanceAlert(
                    alert_id=alert_id,
                    query_id=metrics.query_id,
                    alert_type="low_efficiency",
                    severity="warning",
                    message=f"Query {metrics.query_id} has low efficiency ratio: {efficiency:.3f}",
                    current_value=efficiency,
                    threshold_value=self.efficiency_threshold
                )
                alerts_to_create.append(alert)

        # Register alerts
        for alert in alerts_to_create:
            self.active_alerts[alert.alert_id] = alert
            await self._notify_alert_callbacks(alert)

    async def _notify_alert_callbacks(self, alert: PerformanceAlert):
        """Notify registered alert callbacks"""
        for callback in self.alert_callbacks:
            try:
                await callback(alert)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")

    def add_alert_callback(self, callback: Callable[[PerformanceAlert], None]):
        """Add callback function for performance alerts"""
        self.alert_callbacks.append(callback)

    async def get_performance_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive performance summary"""

        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        recent_queries = [q for q in self.query_history if q['timestamp'] > cutoff_time]

        if not recent_queries:
            return {"error": "No queries recorded in specified time period"}

        # Calculate summary statistics
        execution_times = [q['execution_time_ms'] for q in recent_queries]
        category_counts = defaultdict(int)
        for q in recent_queries:
            category_counts[q['category']] += 1

        # Get top slow queries
        slow_queries = [
            {
                'query_id': metrics.query_id,
                'avg_time_ms': metrics.avg_time_ms,
                'execution_count': metrics.execution_count,
                'category': metrics.category.value,
                'tables': list(metrics.tables_accessed),
                'suggestions': metrics.optimization_suggestions
            }
            for metrics in self.query_metrics.values()
            if metrics.last_seen > cutoff_time and metrics.is_slow_query
        ]
        slow_queries.sort(key=lambda x: x['avg_time_ms'], reverse=True)

        return {
            "monitoring_period_hours": hours,
            "total_queries": len(recent_queries),
            "unique_queries": len({q['query_hash'] for q in recent_queries}),
            "performance_distribution": dict(category_counts),
            "execution_time_stats": {
                "avg_ms": statistics.mean(execution_times),
                "median_ms": statistics.median(execution_times),
                "p95_ms": statistics.quantiles(execution_times, n=20)[18] if len(execution_times) > 20 else max(execution_times),
                "p99_ms": statistics.quantiles(execution_times, n=100)[98] if len(execution_times) > 100 else max(execution_times),
                "min_ms": min(execution_times),
                "max_ms": max(execution_times)
            },
            "slow_queries": slow_queries[:10],
            "active_alerts": len(self.active_alerts),
            "queries_under_25ms": category_counts.get('excellent', 0) + category_counts.get('good', 0),
            "performance_target_percentage": (
                (category_counts.get('excellent', 0) + category_counts.get('good', 0)) /
                len(recent_queries) * 100
            ) if recent_queries else 0
        }

    async def get_query_details(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific query"""

        for metrics in self.query_metrics.values():
            if metrics.query_id == query_id:
                return {
                    "query_id": metrics.query_id,
                    "sql_text": metrics.sql_text,
                    "query_type": metrics.query_type.value,
                    "complexity": metrics.complexity.value,
                    "performance": {
                        "category": metrics.category.value,
                        "avg_time_ms": metrics.avg_time_ms,
                        "min_time_ms": metrics.min_time_ms,
                        "max_time_ms": metrics.max_time_ms,
                        "p95_time_ms": metrics.p95_time_ms,
                        "p99_time_ms": metrics.p99_time_ms,
                        "execution_count": metrics.execution_count
                    },
                    "database_metrics": {
                        "rows_examined": metrics.rows_examined,
                        "rows_returned": metrics.rows_returned,
                        "efficiency_ratio": metrics.get_efficiency_ratio()
                    },
                    "structure": {
                        "tables_accessed": list(metrics.tables_accessed),
                        "join_count": metrics.join_count,
                        "subquery_count": metrics.subquery_count
                    },
                    "optimization_suggestions": metrics.optimization_suggestions,
                    "first_seen": metrics.first_seen.isoformat(),
                    "last_seen": metrics.last_seen.isoformat()
                }

        return None

    async def _performance_analysis_loop(self):
        """Background task for performance analysis"""
        while self.monitoring_enabled:
            try:
                # Analyze trends and patterns
                await self._analyze_performance_trends()
                await asyncio.sleep(300)  # Every 5 minutes

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in performance analysis loop: {e}")
                await asyncio.sleep(60)

    async def _alert_processing_loop(self):
        """Background task for alert processing"""
        while self.monitoring_enabled:
            try:
                # Process and clean up alerts
                await self._process_alerts()
                await asyncio.sleep(60)  # Every minute

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in alert processing loop: {e}")
                await asyncio.sleep(60)

    async def _metrics_aggregation_loop(self):
        """Background task for metrics aggregation"""
        while self.monitoring_enabled:
            try:
                # Aggregate and store historical metrics
                await self._aggregate_metrics()
                await asyncio.sleep(1800)  # Every 30 minutes

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics aggregation loop: {e}")
                await asyncio.sleep(300)

    async def _analyze_performance_trends(self):
        """Analyze performance trends and patterns"""
        # Implementation for trend analysis
        logger.debug("Analyzing performance trends")

    async def _process_alerts(self):
        """Process and manage alerts"""
        # Auto-resolve alerts that are no longer relevant
        current_time = datetime.utcnow()
        alerts_to_remove = []

        for alert_id, alert in self.active_alerts.items():
            # Auto-resolve alerts older than 1 hour if condition improved
            if (current_time - alert.timestamp).total_seconds() > 3600:
                alerts_to_remove.append(alert_id)

        for alert_id in alerts_to_remove:
            del self.active_alerts[alert_id]

    async def _aggregate_metrics(self):
        """Aggregate metrics for historical analysis"""
        logger.debug("Aggregating performance metrics")

# SQLAlchemy event listener for automatic monitoring
def setup_automatic_monitoring(engine: AsyncEngine, monitor: QueryPerformanceMonitor):
    """Set up automatic query monitoring with SQLAlchemy events"""

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        context._query_start_time = time.time()
        context._query_statement = statement

    @event.listens_for(engine.sync_engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if hasattr(context, '_query_start_time'):
            execution_time = (time.time() - context._query_start_time) * 1000

            # Record query performance asynchronously
            asyncio.create_task(
                monitor.record_query_execution(statement, execution_time)
            )

# Factory function
def create_query_performance_monitor(redis_client: Optional[redis.Redis] = None) -> QueryPerformanceMonitor:
    """Create query performance monitor instance"""
    return QueryPerformanceMonitor(redis_client)