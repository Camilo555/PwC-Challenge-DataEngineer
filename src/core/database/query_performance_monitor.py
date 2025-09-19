"""
Advanced Query Performance Monitor
=================================

Real-time query performance monitoring with intelligent alerting:
- Sub-25ms query performance tracking and optimization
- Slow query detection and analysis
- Query pattern recognition and caching recommendations
- Execution plan analysis and optimization suggestions
- Automatic performance regression detection
- ML-powered query optimization recommendations
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from uuid import UUID, uuid4

import sqlparse
from sqlalchemy import text, inspect, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger

logger = get_logger(__name__)


class QueryType(str, Enum):
    """Query classification types."""
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    DDL = "ddl"
    ANALYTICAL = "analytical"
    TRANSACTIONAL = "transactional"
    BULK_OPERATION = "bulk_operation"


class PerformanceThreshold(str, Enum):
    """Performance threshold classifications."""
    EXCELLENT = "excellent"    # < 10ms
    GOOD = "good"             # 10-25ms
    ACCEPTABLE = "acceptable"  # 25-100ms
    SLOW = "slow"             # 100-1000ms
    CRITICAL = "critical"      # > 1000ms


class QueryComplexity(str, Enum):
    """Query complexity levels."""
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"


@dataclass
class QueryMetrics:
    """Comprehensive query performance metrics."""
    query_id: str
    query_hash: str
    query_type: QueryType
    complexity: QueryComplexity
    
    # Execution metrics
    execution_time_ms: float
    cpu_time_ms: float
    io_time_ms: float
    memory_usage_mb: float
    
    # Database metrics
    rows_examined: int = 0
    rows_returned: int = 0
    tables_accessed: int = 0
    indexes_used: int = 0
    temp_tables_created: int = 0
    
    # Performance classification
    threshold_level: PerformanceThreshold = PerformanceThreshold.GOOD
    
    # Execution context
    database_name: str = ""
    session_id: Optional[str] = None
    client_id: Optional[str] = None
    
    # Timing details
    executed_at: datetime = field(default_factory=datetime.utcnow)
    parse_time_ms: float = 0
    plan_time_ms: float = 0
    execution_plan: Optional[Dict[str, Any]] = None
    
    # Query pattern information
    table_pattern: Optional[str] = None
    join_pattern: Optional[str] = None
    where_pattern: Optional[str] = None
    
    # Performance flags
    uses_index: bool = True
    full_table_scan: bool = False
    uses_temp_table: bool = False
    has_subqueries: bool = False
    
    # Optimization suggestions
    optimization_suggestions: List[str] = field(default_factory=list)
    cache_candidate: bool = False
    
    def calculate_efficiency_score(self) -> float:
        """Calculate query efficiency score (0-100)."""
        base_score = 100
        
        # Penalize for high execution time
        if self.execution_time_ms > 25:
            base_score -= min(50, (self.execution_time_ms - 25) / 10)
        
        # Penalize for full table scans
        if self.full_table_scan:
            base_score -= 30
        
        # Penalize for temporary tables
        if self.uses_temp_table:
            base_score -= 20
        
        # Penalize for poor row examination ratio
        if self.rows_examined > 0 and self.rows_returned > 0:
            examination_ratio = self.rows_returned / self.rows_examined
            if examination_ratio < 0.1:
                base_score -= 25
        
        # Reward for index usage
        if self.uses_index and not self.full_table_scan:
            base_score += 5
        
        return max(0, base_score)
    
    @property
    def is_target_performance(self) -> bool:
        """Check if query meets <25ms target."""
        return self.execution_time_ms < 25.0
    
    @property
    def needs_optimization(self) -> bool:
        """Check if query needs optimization."""
        return (
            self.execution_time_ms > 25.0 or
            self.full_table_scan or
            self.calculate_efficiency_score() < 70
        )


@dataclass
class QueryPatternStats:
    """Statistics for query patterns."""
    pattern_hash: str
    query_count: int = 0
    total_execution_time_ms: float = 0
    avg_execution_time_ms: float = 0
    min_execution_time_ms: float = float('inf')
    max_execution_time_ms: float = 0
    p95_execution_time_ms: float = 0
    p99_execution_time_ms: float = 0
    
    # Recent performance tracking
    recent_executions: deque = field(default_factory=lambda: deque(maxlen=100))
    trend_direction: str = "stable"  # improving, degrading, stable
    
    # Optimization tracking
    last_optimized: Optional[datetime] = None
    optimization_applied: bool = False
    performance_improvement_pct: float = 0
    
    def update_execution(self, execution_time_ms: float):
        """Update pattern statistics with new execution."""
        self.query_count += 1
        self.total_execution_time_ms += execution_time_ms
        self.avg_execution_time_ms = self.total_execution_time_ms / self.query_count
        
        self.min_execution_time_ms = min(self.min_execution_time_ms, execution_time_ms)
        self.max_execution_time_ms = max(self.max_execution_time_ms, execution_time_ms)
        
        # Update recent executions for trend analysis
        self.recent_executions.append({
            'execution_time_ms': execution_time_ms,
            'timestamp': datetime.utcnow()
        })
        
        # Calculate percentiles from recent executions
        if len(self.recent_executions) >= 10:
            recent_times = [e['execution_time_ms'] for e in self.recent_executions]
            self.p95_execution_time_ms = statistics.quantiles(recent_times, n=20)[18]  # 95th percentile
            self.p99_execution_time_ms = statistics.quantiles(recent_times, n=100)[98]  # 99th percentile
        
        # Analyze trend
        self._analyze_performance_trend()
    
    def _analyze_performance_trend(self):
        """Analyze performance trend from recent executions."""
        if len(self.recent_executions) < 20:
            return
        
        recent_times = [e['execution_time_ms'] for e in self.recent_executions]
        first_half = recent_times[:len(recent_times)//2]
        second_half = recent_times[len(recent_times)//2:]
        
        first_avg = sum(first_half) / len(first_half)
        second_avg = sum(second_half) / len(second_half)
        
        change_pct = ((second_avg - first_avg) / first_avg) * 100
        
        if change_pct > 10:
            self.trend_direction = "degrading"
        elif change_pct < -10:
            self.trend_direction = "improving"
        else:
            self.trend_direction = "stable"


class QueryPerformanceMonitor:
    """
    Advanced query performance monitoring system for enterprise databases.
    
    Features:
    - Real-time performance tracking with <25ms target
    - Intelligent slow query detection and analysis
    - Query pattern recognition and optimization
    - Execution plan analysis and caching recommendations
    - Performance regression detection
    - Automated optimization suggestions
    """

    def __init__(self, engine: AsyncEngine, target_performance_ms: float = 25.0):
        self.engine = engine
        self.target_performance_ms = target_performance_ms
        
        # Query tracking
        self.active_queries: Dict[str, Dict[str, Any]] = {}
        self.query_metrics: List[QueryMetrics] = []
        self.pattern_stats: Dict[str, QueryPatternStats] = {}
        
        # Performance analytics
        self.hourly_stats = defaultdict(lambda: {
            'query_count': 0,
            'avg_execution_time': 0,
            'slow_query_count': 0,
            'target_performance_rate': 0
        })
        
        # Alert thresholds
        self.slow_query_threshold_ms = 100.0
        self.critical_query_threshold_ms = 1000.0
        self.performance_degradation_threshold_pct = 20.0
        
        # Optimization cache
        self.optimization_cache: Dict[str, Dict[str, Any]] = {}
        self.query_plan_cache: Dict[str, Dict[str, Any]] = {}
        
        # Monitoring state
        self.monitoring_active = False
        self.background_task: Optional[asyncio.Task] = None
        
        # System metrics
        self.system_metrics = {
            'total_queries_monitored': 0,
            'target_performance_queries': 0,
            'slow_queries_detected': 0,
            'optimizations_suggested': 0,
            'performance_regressions_detected': 0,
            'cache_hit_rate': 0.0,
            'monitoring_start_time': None
        }

    async def start_monitoring(self):
        """Start continuous query performance monitoring."""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        self.system_metrics['monitoring_start_time'] = datetime.utcnow()
        
        # Create monitoring schema
        await self._create_monitoring_schema()
        
        # Load historical data
        await self._load_historical_patterns()
        
        # Start background monitoring tasks
        self.background_task = asyncio.create_task(self._background_monitoring())
        
        logger.info(f"Query performance monitoring started with {self.target_performance_ms}ms target")

    async def stop_monitoring(self):
        """Stop query performance monitoring."""
        if not self.monitoring_active:
            return
        
        self.monitoring_active = False
        
        if self.background_task:
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                pass
        
        # Save current metrics
        await self._persist_metrics()
        
        logger.info("Query performance monitoring stopped")

    async def _create_monitoring_schema(self):
        """Create database schema for query monitoring."""
        schema_sql = """
        -- Query performance metrics table
        CREATE TABLE IF NOT EXISTS query_performance_metrics (
            metric_id SERIAL PRIMARY KEY,
            query_id VARCHAR(255) NOT NULL,
            query_hash VARCHAR(64) NOT NULL,
            query_type VARCHAR(50) NOT NULL,
            execution_time_ms DECIMAL(10,3) NOT NULL,
            cpu_time_ms DECIMAL(10,3),
            io_time_ms DECIMAL(10,3),
            memory_usage_mb DECIMAL(10,3),
            rows_examined BIGINT,
            rows_returned BIGINT,
            tables_accessed INTEGER,
            indexes_used INTEGER,
            threshold_level VARCHAR(50),
            efficiency_score DECIMAL(5,2),
            uses_index BOOLEAN DEFAULT TRUE,
            full_table_scan BOOLEAN DEFAULT FALSE,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            session_id VARCHAR(255),
            database_name VARCHAR(255),
            execution_plan JSONB,
            optimization_suggestions JSONB,
            CONSTRAINT idx_query_perf_metrics_executed_at CHECK (executed_at IS NOT NULL)
        );

        -- Query pattern statistics
        CREATE TABLE IF NOT EXISTS query_pattern_stats (
            pattern_hash VARCHAR(64) PRIMARY KEY,
            query_count BIGINT DEFAULT 0,
            avg_execution_time_ms DECIMAL(10,3),
            min_execution_time_ms DECIMAL(10,3),
            max_execution_time_ms DECIMAL(10,3),
            p95_execution_time_ms DECIMAL(10,3),
            p99_execution_time_ms DECIMAL(10,3),
            trend_direction VARCHAR(20) DEFAULT 'stable',
            last_optimized TIMESTAMP,
            performance_improvement_pct DECIMAL(5,2),
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Slow query alerts
        CREATE TABLE IF NOT EXISTS slow_query_alerts (
            alert_id SERIAL PRIMARY KEY,
            query_hash VARCHAR(64) NOT NULL,
            execution_time_ms DECIMAL(10,3) NOT NULL,
            alert_level VARCHAR(20) NOT NULL, -- slow, critical
            alert_message TEXT,
            optimization_suggestions JSONB,
            acknowledged BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            acknowledged_at TIMESTAMP
        );

        -- Performance regression alerts
        CREATE TABLE IF NOT EXISTS performance_regression_alerts (
            regression_id SERIAL PRIMARY KEY,
            pattern_hash VARCHAR(64) NOT NULL,
            baseline_avg_time_ms DECIMAL(10,3),
            current_avg_time_ms DECIMAL(10,3),
            degradation_pct DECIMAL(5,2),
            regression_detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            investigation_notes TEXT,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_at TIMESTAMP
        );

        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_query_perf_metrics_hash_time 
            ON query_performance_metrics(query_hash, executed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_query_perf_metrics_execution_time 
            ON query_performance_metrics(execution_time_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_query_perf_metrics_threshold 
            ON query_performance_metrics(threshold_level, executed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_slow_query_alerts_created 
            ON slow_query_alerts(created_at DESC, acknowledged);
        CREATE INDEX IF NOT EXISTS idx_performance_regression_resolved 
            ON performance_regression_alerts(resolved, regression_detected_at DESC);
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(schema_sql))

    async def track_query_execution(self, 
                                   query: str, 
                                   execution_time_ms: float,
                                   session_id: Optional[str] = None,
                                   additional_metrics: Optional[Dict[str, Any]] = None) -> QueryMetrics:
        """Track and analyze query execution performance."""
        
        # Generate query identifiers
        normalized_query = self._normalize_query(query)
        query_hash = hashlib.sha256(normalized_query.encode()).hexdigest()[:16]
        query_id = str(uuid4())
        
        # Analyze query
        query_analysis = self._analyze_query_structure(normalized_query)
        
        # Create metrics object
        metrics = QueryMetrics(
            query_id=query_id,
            query_hash=query_hash,
            query_type=query_analysis['type'],
            complexity=query_analysis['complexity'],
            execution_time_ms=execution_time_ms,
            cpu_time_ms=additional_metrics.get('cpu_time_ms', 0) if additional_metrics else 0,
            io_time_ms=additional_metrics.get('io_time_ms', 0) if additional_metrics else 0,
            memory_usage_mb=additional_metrics.get('memory_usage_mb', 0) if additional_metrics else 0,
            rows_examined=additional_metrics.get('rows_examined', 0) if additional_metrics else 0,
            rows_returned=additional_metrics.get('rows_returned', 0) if additional_metrics else 0,
            tables_accessed=query_analysis['tables_count'],
            indexes_used=additional_metrics.get('indexes_used', 0) if additional_metrics else 0,
            session_id=session_id,
            uses_index=not query_analysis.get('full_table_scan', False),
            full_table_scan=query_analysis.get('full_table_scan', False),
            has_subqueries=query_analysis.get('has_subqueries', False),
            table_pattern=query_analysis.get('table_pattern'),
            join_pattern=query_analysis.get('join_pattern'),
            where_pattern=query_analysis.get('where_pattern')
        )
        
        # Classify performance threshold
        metrics.threshold_level = self._classify_performance_threshold(execution_time_ms)
        
        # Generate optimization suggestions
        metrics.optimization_suggestions = self._generate_optimization_suggestions(
            normalized_query, metrics, query_analysis
        )
        
        # Update pattern statistics
        await self._update_pattern_statistics(query_hash, metrics)
        
        # Store metrics
        self.query_metrics.append(metrics)
        await self._persist_query_metrics(metrics)
        
        # Update system metrics
        self.system_metrics['total_queries_monitored'] += 1
        if metrics.is_target_performance:
            self.system_metrics['target_performance_queries'] += 1
        if metrics.execution_time_ms > self.slow_query_threshold_ms:
            self.system_metrics['slow_queries_detected'] += 1
        if metrics.optimization_suggestions:
            self.system_metrics['optimizations_suggested'] += 1
        
        # Check for alerts
        await self._check_performance_alerts(metrics)
        
        # Log performance issues
        if not metrics.is_target_performance:
            logger.warning(f"Query exceeded target performance: {execution_time_ms:.2f}ms > {self.target_performance_ms}ms")
        
        return metrics

    def _normalize_query(self, query: str) -> str:
        """Normalize query for pattern analysis."""
        try:
            # Parse and format query
            parsed = sqlparse.parse(query)[0]
            
            # Remove comments and extra whitespace
            normalized = sqlparse.format(
                str(parsed),
                strip_comments=True,
                strip_whitespace=True,
                keyword_case='upper',
                identifier_case='lower'
            )
            
            # Replace literal values with placeholders for pattern matching
            normalized = self._replace_literals_with_placeholders(normalized)
            
            return normalized.strip()
        except:
            # Fallback to basic normalization
            return ' '.join(query.strip().split())

    def _replace_literals_with_placeholders(self, query: str) -> str:
        """Replace literal values with placeholders for pattern analysis."""
        import re
        
        # Replace string literals
        query = re.sub(r"'[^']*'", "?", query)
        query = re.sub(r'"[^"]*"', "?", query)
        
        # Replace numeric literals
        query = re.sub(r'\b\d+\b', "?", query)
        query = re.sub(r'\b\d*\.\d+\b', "?", query)
        
        # Replace date/time literals
        query = re.sub(r'\b\d{4}-\d{2}-\d{2}\b', "?", query)
        query = re.sub(r'\b\d{2}:\d{2}:\d{2}\b', "?", query)
        
        return query

    def _analyze_query_structure(self, query: str) -> Dict[str, Any]:
        """Analyze query structure for optimization insights."""
        analysis = {
            'type': QueryType.SELECT,
            'complexity': QueryComplexity.SIMPLE,
            'tables_count': 0,
            'joins_count': 0,
            'subqueries_count': 0,
            'aggregations_count': 0,
            'full_table_scan': False,
            'has_subqueries': False,
            'table_pattern': None,
            'join_pattern': None,
            'where_pattern': None
        }
        
        try:
            query_upper = query.upper()
            
            # Determine query type
            if query_upper.strip().startswith('SELECT'):
                analysis['type'] = QueryType.SELECT
                if any(keyword in query_upper for keyword in ['GROUP BY', 'HAVING', 'WINDOW', 'OVER']):
                    analysis['type'] = QueryType.ANALYTICAL
            elif query_upper.strip().startswith('INSERT'):
                analysis['type'] = QueryType.INSERT
            elif query_upper.strip().startswith('UPDATE'):
                analysis['type'] = QueryType.UPDATE
            elif query_upper.strip().startswith('DELETE'):
                analysis['type'] = QueryType.DELETE
            elif any(ddl in query_upper for ddl in ['CREATE', 'ALTER', 'DROP']):
                analysis['type'] = QueryType.DDL
            
            # Count table references
            import re
            table_references = re.findall(r'\bFROM\s+(\w+)', query_upper)
            table_references += re.findall(r'\bJOIN\s+(\w+)', query_upper)
            analysis['tables_count'] = len(set(table_references))
            
            # Count joins
            analysis['joins_count'] = len(re.findall(r'\b(INNER|LEFT|RIGHT|OUTER|CROSS)?\s*JOIN\b', query_upper))
            
            # Count subqueries
            analysis['subqueries_count'] = query_upper.count('(SELECT')
            analysis['has_subqueries'] = analysis['subqueries_count'] > 0
            
            # Count aggregations
            aggregation_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT']
            analysis['aggregations_count'] = sum(query_upper.count(func) for func in aggregation_functions)
            
            # Check for potential full table scans
            has_where = 'WHERE' in query_upper
            has_limit = 'LIMIT' in query_upper
            has_order = 'ORDER BY' in query_upper
            
            if not has_where and not has_limit and analysis['tables_count'] > 0:
                analysis['full_table_scan'] = True
            
            # Determine complexity
            complexity_score = (
                analysis['tables_count'] * 2 +
                analysis['joins_count'] * 3 +
                analysis['subqueries_count'] * 4 +
                analysis['aggregations_count'] * 2
            )
            
            if complexity_score >= 20:
                analysis['complexity'] = QueryComplexity.VERY_COMPLEX
            elif complexity_score >= 10:
                analysis['complexity'] = QueryComplexity.COMPLEX
            elif complexity_score >= 5:
                analysis['complexity'] = QueryComplexity.MODERATE
            else:
                analysis['complexity'] = QueryComplexity.SIMPLE
            
            # Generate patterns for caching
            analysis['table_pattern'] = ','.join(sorted(set(table_references)))
            if analysis['joins_count'] > 0:
                analysis['join_pattern'] = f"joins_{analysis['joins_count']}"
            if has_where:
                analysis['where_pattern'] = "filtered"
            
        except Exception as e:
            logger.warning(f"Query analysis failed: {e}")
        
        return analysis

    def _classify_performance_threshold(self, execution_time_ms: float) -> PerformanceThreshold:
        """Classify query performance into threshold levels."""
        if execution_time_ms < 10:
            return PerformanceThreshold.EXCELLENT
        elif execution_time_ms < 25:
            return PerformanceThreshold.GOOD
        elif execution_time_ms < 100:
            return PerformanceThreshold.ACCEPTABLE
        elif execution_time_ms < 1000:
            return PerformanceThreshold.SLOW
        else:
            return PerformanceThreshold.CRITICAL

    def _generate_optimization_suggestions(self, 
                                         query: str, 
                                         metrics: QueryMetrics,
                                         analysis: Dict[str, Any]) -> List[str]:
        """Generate intelligent optimization suggestions."""
        suggestions = []
        
        # Performance-based suggestions
        if metrics.execution_time_ms > self.target_performance_ms:
            if metrics.full_table_scan:
                suggestions.append("Consider adding indexes on columns used in WHERE clause to avoid full table scans")
            
            if analysis['joins_count'] > 3:
                suggestions.append("Review join order and consider denormalization for complex multi-table queries")
            
            if analysis['subqueries_count'] > 2:
                suggestions.append("Consider converting subqueries to JOINs for better performance")
            
            if metrics.rows_examined > 0 and metrics.rows_returned > 0:
                selectivity = metrics.rows_returned / metrics.rows_examined
                if selectivity < 0.1:
                    suggestions.append("Low selectivity detected - consider adding more selective WHERE conditions")
        
        # Query structure suggestions
        if analysis['aggregations_count'] > 0 and 'GROUP BY' in query.upper():
            suggestions.append("Consider creating covering indexes for GROUP BY operations")
        
        if 'ORDER BY' in query.upper() and not 'LIMIT' in query.upper():
            suggestions.append("Consider adding LIMIT clause to ORDER BY queries when possible")
        
        # Caching suggestions
        if (metrics.execution_time_ms > 50 and 
            analysis['type'] == QueryType.SELECT and 
            analysis['aggregations_count'] > 0):
            suggestions.append("Query is a candidate for result caching or materialized views")
            metrics.cache_candidate = True
        
        # Memory usage suggestions
        if metrics.memory_usage_mb > 100:
            suggestions.append("High memory usage - consider breaking query into smaller operations")
        
        return suggestions

    async def _update_pattern_statistics(self, query_hash: str, metrics: QueryMetrics):
        """Update pattern statistics with new execution data."""
        if query_hash not in self.pattern_stats:
            self.pattern_stats[query_hash] = QueryPatternStats(pattern_hash=query_hash)
        
        pattern_stats = self.pattern_stats[query_hash]
        pattern_stats.update_execution(metrics.execution_time_ms)
        
        # Persist pattern statistics
        await self._persist_pattern_stats(pattern_stats)

    async def _check_performance_alerts(self, metrics: QueryMetrics):
        """Check for performance alerts and create notifications."""
        
        # Slow query alert
        if metrics.execution_time_ms > self.slow_query_threshold_ms:
            alert_level = "critical" if metrics.execution_time_ms > self.critical_query_threshold_ms else "slow"
            
            await self._create_slow_query_alert(
                query_hash=metrics.query_hash,
                execution_time_ms=metrics.execution_time_ms,
                alert_level=alert_level,
                suggestions=metrics.optimization_suggestions
            )
        
        # Performance regression check
        if metrics.query_hash in self.pattern_stats:
            pattern_stats = self.pattern_stats[metrics.query_hash]
            
            if (pattern_stats.query_count > 10 and 
                pattern_stats.trend_direction == "degrading"):
                
                # Calculate degradation percentage
                if len(pattern_stats.recent_executions) >= 20:
                    recent_times = [e['execution_time_ms'] for e in pattern_stats.recent_executions]
                    recent_avg = sum(recent_times[-10:]) / 10
                    baseline_avg = sum(recent_times[:10]) / 10
                    
                    if baseline_avg > 0:
                        degradation_pct = ((recent_avg - baseline_avg) / baseline_avg) * 100
                        
                        if degradation_pct > self.performance_degradation_threshold_pct:
                            await self._create_performance_regression_alert(
                                pattern_hash=metrics.query_hash,
                                baseline_avg=baseline_avg,
                                current_avg=recent_avg,
                                degradation_pct=degradation_pct
                            )
                            
                            self.system_metrics['performance_regressions_detected'] += 1

    async def _create_slow_query_alert(self, 
                                     query_hash: str,
                                     execution_time_ms: float,
                                     alert_level: str,
                                     suggestions: List[str]):
        """Create slow query alert."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO slow_query_alerts 
                    (query_hash, execution_time_ms, alert_level, alert_message, optimization_suggestions)
                    VALUES ($1, $2, $3, $4, $5)
                """), 
                query_hash,
                execution_time_ms,
                alert_level,
                f"Query exceeded {alert_level} threshold: {execution_time_ms:.2f}ms",
                json.dumps(suggestions))
                
        except Exception as e:
            logger.error(f"Failed to create slow query alert: {e}")

    async def _create_performance_regression_alert(self,
                                                 pattern_hash: str,
                                                 baseline_avg: float,
                                                 current_avg: float,
                                                 degradation_pct: float):
        """Create performance regression alert."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO performance_regression_alerts 
                    (pattern_hash, baseline_avg_time_ms, current_avg_time_ms, degradation_pct)
                    VALUES ($1, $2, $3, $4)
                """), 
                pattern_hash,
                baseline_avg,
                current_avg,
                degradation_pct)
                
        except Exception as e:
            logger.error(f"Failed to create performance regression alert: {e}")

    async def _persist_query_metrics(self, metrics: QueryMetrics):
        """Persist query metrics to database."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO query_performance_metrics 
                    (query_id, query_hash, query_type, execution_time_ms, cpu_time_ms, io_time_ms,
                     memory_usage_mb, rows_examined, rows_returned, tables_accessed, indexes_used,
                     threshold_level, efficiency_score, uses_index, full_table_scan, executed_at,
                     session_id, database_name, execution_plan, optimization_suggestions)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                """),
                metrics.query_id,
                metrics.query_hash,
                metrics.query_type.value,
                metrics.execution_time_ms,
                metrics.cpu_time_ms,
                metrics.io_time_ms,
                metrics.memory_usage_mb,
                metrics.rows_examined,
                metrics.rows_returned,
                metrics.tables_accessed,
                metrics.indexes_used,
                metrics.threshold_level.value,
                metrics.calculate_efficiency_score(),
                metrics.uses_index,
                metrics.full_table_scan,
                metrics.executed_at,
                metrics.session_id,
                metrics.database_name,
                json.dumps(metrics.execution_plan) if metrics.execution_plan else None,
                json.dumps(metrics.optimization_suggestions))
                
        except Exception as e:
            logger.error(f"Failed to persist query metrics: {e}")

    async def _persist_pattern_stats(self, pattern_stats: QueryPatternStats):
        """Persist pattern statistics to database."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO query_pattern_stats 
                    (pattern_hash, query_count, avg_execution_time_ms, min_execution_time_ms,
                     max_execution_time_ms, p95_execution_time_ms, p99_execution_time_ms,
                     trend_direction, last_seen)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (pattern_hash) DO UPDATE SET
                        query_count = EXCLUDED.query_count,
                        avg_execution_time_ms = EXCLUDED.avg_execution_time_ms,
                        min_execution_time_ms = EXCLUDED.min_execution_time_ms,
                        max_execution_time_ms = EXCLUDED.max_execution_time_ms,
                        p95_execution_time_ms = EXCLUDED.p95_execution_time_ms,
                        p99_execution_time_ms = EXCLUDED.p99_execution_time_ms,
                        trend_direction = EXCLUDED.trend_direction,
                        last_seen = EXCLUDED.last_seen
                """),
                pattern_stats.pattern_hash,
                pattern_stats.query_count,
                pattern_stats.avg_execution_time_ms,
                pattern_stats.min_execution_time_ms,
                pattern_stats.max_execution_time_ms,
                pattern_stats.p95_execution_time_ms,
                pattern_stats.p99_execution_time_ms,
                pattern_stats.trend_direction,
                datetime.utcnow())
                
        except Exception as e:
            logger.error(f"Failed to persist pattern statistics: {e}")

    async def _background_monitoring(self):
        """Background monitoring tasks."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(60)  # Run every minute
                
                # Update hourly statistics
                await self._update_hourly_statistics()
                
                # Check for performance regressions
                await self._check_performance_regressions()
                
                # Update cache hit rates
                await self._update_cache_metrics()
                
                # Cleanup old data
                await self._cleanup_old_data()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background monitoring error: {e}")

    async def _update_hourly_statistics(self):
        """Update hourly performance statistics."""
        try:
            current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            
            # Get metrics for current hour
            hour_metrics = [
                m for m in self.query_metrics 
                if m.executed_at >= current_hour
            ]
            
            if hour_metrics:
                total_queries = len(hour_metrics)
                avg_execution_time = sum(m.execution_time_ms for m in hour_metrics) / total_queries
                slow_queries = len([m for m in hour_metrics if m.execution_time_ms > self.slow_query_threshold_ms])
                target_performance_queries = len([m for m in hour_metrics if m.is_target_performance])
                target_performance_rate = (target_performance_queries / total_queries) * 100
                
                self.hourly_stats[current_hour] = {
                    'query_count': total_queries,
                    'avg_execution_time': avg_execution_time,
                    'slow_query_count': slow_queries,
                    'target_performance_rate': target_performance_rate
                }
                
        except Exception as e:
            logger.error(f"Failed to update hourly statistics: {e}")

    async def _check_performance_regressions(self):
        """Check for performance regressions across all patterns."""
        try:
            regression_count = 0
            
            for pattern_hash, pattern_stats in self.pattern_stats.items():
                if (pattern_stats.query_count > 20 and 
                    pattern_stats.trend_direction == "degrading" and
                    len(pattern_stats.recent_executions) >= 20):
                    
                    regression_count += 1
            
            if regression_count > 0:
                logger.warning(f"Detected {regression_count} query patterns with performance regression")
                
        except Exception as e:
            logger.error(f"Failed to check performance regressions: {e}")

    async def _update_cache_metrics(self):
        """Update cache hit rate and related metrics."""
        try:
            if self.system_metrics['total_queries_monitored'] > 0:
                cache_candidates = len([m for m in self.query_metrics if m.cache_candidate])
                self.system_metrics['cache_hit_rate'] = (cache_candidates / self.system_metrics['total_queries_monitored']) * 100
                
        except Exception as e:
            logger.error(f"Failed to update cache metrics: {e}")

    async def _cleanup_old_data(self):
        """Cleanup old monitoring data."""
        try:
            # Keep only last 7 days of detailed metrics
            cutoff_date = datetime.utcnow() - timedelta(days=7)
            
            # Remove old metrics from memory
            self.query_metrics = [m for m in self.query_metrics if m.executed_at > cutoff_date]
            
            # Clean database tables
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    DELETE FROM query_performance_metrics 
                    WHERE executed_at < $1
                """), cutoff_date)
                
                await conn.execute(text("""
                    DELETE FROM slow_query_alerts 
                    WHERE created_at < $1 AND acknowledged = TRUE
                """), cutoff_date)
                
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")

    async def _load_historical_patterns(self):
        """Load historical query patterns from database."""
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(text("""
                    SELECT pattern_hash, query_count, avg_execution_time_ms, 
                           min_execution_time_ms, max_execution_time_ms,
                           p95_execution_time_ms, p99_execution_time_ms,
                           trend_direction
                    FROM query_pattern_stats
                """))
                
                for row in result:
                    pattern_stats = QueryPatternStats(
                        pattern_hash=row[0],
                        query_count=row[1],
                        avg_execution_time_ms=float(row[2]) if row[2] else 0,
                        min_execution_time_ms=float(row[3]) if row[3] else 0,
                        max_execution_time_ms=float(row[4]) if row[4] else 0,
                        p95_execution_time_ms=float(row[5]) if row[5] else 0,
                        p99_execution_time_ms=float(row[6]) if row[6] else 0,
                        trend_direction=row[7] or "stable"
                    )
                    self.pattern_stats[row[0]] = pattern_stats
                    
        except Exception as e:
            logger.warning(f"Failed to load historical patterns: {e}")

    async def _persist_metrics(self):
        """Persist current metrics before shutdown."""
        try:
            # Save pattern statistics
            for pattern_stats in self.pattern_stats.values():
                await self._persist_pattern_stats(pattern_stats)
                
        except Exception as e:
            logger.error(f"Failed to persist metrics: {e}")

    async def get_performance_report(self, hours: int = 24) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Filter recent metrics
            recent_metrics = [m for m in self.query_metrics if m.executed_at > cutoff_time]
            
            if not recent_metrics:
                return {"error": "No metrics available for the specified time period"}
            
            # Calculate basic statistics
            total_queries = len(recent_metrics)
            target_performance_queries = len([m for m in recent_metrics if m.is_target_performance])
            slow_queries = len([m for m in recent_metrics if m.execution_time_ms > self.slow_query_threshold_ms])
            critical_queries = len([m for m in recent_metrics if m.execution_time_ms > self.critical_query_threshold_ms])
            
            execution_times = [m.execution_time_ms for m in recent_metrics]
            avg_execution_time = sum(execution_times) / len(execution_times)
            p95_execution_time = statistics.quantiles(execution_times, n=20)[18] if len(execution_times) >= 20 else max(execution_times)
            p99_execution_time = statistics.quantiles(execution_times, n=100)[98] if len(execution_times) >= 100 else max(execution_times)
            
            # Query type breakdown
            query_types = {}
            for metric in recent_metrics:
                query_type = metric.query_type.value
                if query_type not in query_types:
                    query_types[query_type] = {'count': 0, 'avg_time': 0, 'total_time': 0}
                query_types[query_type]['count'] += 1
                query_types[query_type]['total_time'] += metric.execution_time_ms
            
            for qtype in query_types:
                query_types[qtype]['avg_time'] = query_types[qtype]['total_time'] / query_types[qtype]['count']
            
            # Performance threshold distribution
            threshold_distribution = {}
            for metric in recent_metrics:
                threshold = metric.threshold_level.value
                threshold_distribution[threshold] = threshold_distribution.get(threshold, 0) + 1
            
            # Top slow queries by pattern
            pattern_performance = {}
            for metric in recent_metrics:
                pattern = metric.query_hash
                if pattern not in pattern_performance:
                    pattern_performance[pattern] = {
                        'count': 0,
                        'total_time': 0,
                        'max_time': 0,
                        'optimization_suggestions': metric.optimization_suggestions
                    }
                pattern_performance[pattern]['count'] += 1
                pattern_performance[pattern]['total_time'] += metric.execution_time_ms
                pattern_performance[pattern]['max_time'] = max(pattern_performance[pattern]['max_time'], metric.execution_time_ms)
            
            # Calculate average time for each pattern
            for pattern in pattern_performance:
                pattern_performance[pattern]['avg_time'] = pattern_performance[pattern]['total_time'] / pattern_performance[pattern]['count']
            
            # Sort by average time and get top 10
            top_slow_patterns = sorted(
                pattern_performance.items(),
                key=lambda x: x[1]['avg_time'],
                reverse=True
            )[:10]
            
            return {
                "report_period_hours": hours,
                "generated_at": datetime.utcnow().isoformat(),
                
                "summary_statistics": {
                    "total_queries": total_queries,
                    "target_performance_queries": target_performance_queries,
                    "target_performance_rate_pct": (target_performance_queries / total_queries * 100) if total_queries > 0 else 0,
                    "slow_queries": slow_queries,
                    "critical_queries": critical_queries,
                    "avg_execution_time_ms": avg_execution_time,
                    "p95_execution_time_ms": p95_execution_time,
                    "p99_execution_time_ms": p99_execution_time
                },
                
                "performance_distribution": {
                    "by_threshold": threshold_distribution,
                    "by_query_type": query_types
                },
                
                "top_slow_patterns": [
                    {
                        "pattern_hash": pattern_hash,
                        "query_count": data['count'],
                        "avg_execution_time_ms": data['avg_time'],
                        "max_execution_time_ms": data['max_time'],
                        "optimization_suggestions": data['optimization_suggestions']
                    }
                    for pattern_hash, data in top_slow_patterns
                ],
                
                "system_metrics": self.system_metrics,
                
                "recommendations": self._generate_system_recommendations(recent_metrics)
            }
            
        except Exception as e:
            logger.error(f"Failed to generate performance report: {e}")
            return {"error": str(e)}

    def _generate_system_recommendations(self, metrics: List[QueryMetrics]) -> List[str]:
        """Generate system-level optimization recommendations."""
        recommendations = []
        
        if not metrics:
            return recommendations
        
        # Calculate rates
        total_queries = len(metrics)
        target_performance_rate = len([m for m in metrics if m.is_target_performance]) / total_queries
        slow_query_rate = len([m for m in metrics if m.execution_time_ms > self.slow_query_threshold_ms]) / total_queries
        full_scan_rate = len([m for m in metrics if m.full_table_scan]) / total_queries
        
        # Performance recommendations
        if target_performance_rate < 0.8:
            recommendations.append(f"Only {target_performance_rate*100:.1f}% of queries meet the <{self.target_performance_ms}ms target - consider index optimization and query tuning")
        
        if slow_query_rate > 0.1:
            recommendations.append(f"High slow query rate ({slow_query_rate*100:.1f}%) - implement query optimization program")
        
        if full_scan_rate > 0.05:
            recommendations.append(f"High full table scan rate ({full_scan_rate*100:.1f}%) - review indexing strategy")
        
        # Query pattern recommendations
        analytical_queries = len([m for m in metrics if m.query_type == QueryType.ANALYTICAL])
        if analytical_queries > total_queries * 0.3:
            recommendations.append("High analytical workload - consider implementing data warehouse optimizations and materialized views")
        
        # Memory usage recommendations
        high_memory_queries = len([m for m in metrics if m.memory_usage_mb > 100])
        if high_memory_queries > total_queries * 0.05:
            recommendations.append("Multiple high memory usage queries detected - consider query optimization and connection pooling")
        
        return recommendations

    async def get_slow_query_alerts(self, acknowledged: bool = False) -> List[Dict[str, Any]]:
        """Get slow query alerts."""
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(text("""
                    SELECT alert_id, query_hash, execution_time_ms, alert_level,
                           alert_message, optimization_suggestions, created_at
                    FROM slow_query_alerts
                    WHERE acknowledged = $1
                    ORDER BY created_at DESC
                    LIMIT 100
                """), acknowledged)
                
                alerts = []
                for row in result:
                    alerts.append({
                        "alert_id": row[0],
                        "query_hash": row[1],
                        "execution_time_ms": float(row[2]),
                        "alert_level": row[3],
                        "alert_message": row[4],
                        "optimization_suggestions": json.loads(row[5]) if row[5] else [],
                        "created_at": row[6].isoformat()
                    })
                
                return alerts
                
        except Exception as e:
            logger.error(f"Failed to get slow query alerts: {e}")
            return []

    async def acknowledge_slow_query_alert(self, alert_id: int):
        """Acknowledge a slow query alert."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    UPDATE slow_query_alerts 
                    SET acknowledged = TRUE, acknowledged_at = $1
                    WHERE alert_id = $2
                """), datetime.utcnow(), alert_id)
                
        except Exception as e:
            logger.error(f"Failed to acknowledge alert {alert_id}: {e}")

    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status and metrics."""
        uptime = None
        if self.system_metrics['monitoring_start_time']:
            uptime = (datetime.utcnow() - self.system_metrics['monitoring_start_time']).total_seconds()
        
        return {
            "monitoring_active": self.monitoring_active,
            "uptime_seconds": uptime,
            "target_performance_ms": self.target_performance_ms,
            "slow_query_threshold_ms": self.slow_query_threshold_ms,
            "critical_query_threshold_ms": self.critical_query_threshold_ms,
            "tracked_patterns": len(self.pattern_stats),
            "system_metrics": self.system_metrics,
            "current_memory_metrics_count": len(self.query_metrics),
            "recent_hourly_stats": dict(list(self.hourly_stats.items())[-24:])  # Last 24 hours
        }


# Factory function
async def create_query_monitor(engine: AsyncEngine, target_performance_ms: float = 25.0) -> QueryPerformanceMonitor:
    """Create and initialize QueryPerformanceMonitor."""
    monitor = QueryPerformanceMonitor(engine, target_performance_ms)
    await monitor.start_monitoring()
    return monitor


# Example usage and testing
async def main():
    """Example usage of query performance monitor."""
    from sqlalchemy.ext.asyncio import create_async_engine
    
    # Create async engine (example)
    engine = create_async_engine(
        "postgresql+asyncpg://user:password@localhost/database",
        echo=False
    )
    
    monitor = await create_query_monitor(engine, target_performance_ms=25.0)
    
    try:
        # Simulate query tracking
        await monitor.track_query_execution(
            query="SELECT * FROM customers WHERE customer_id = 12345",
            execution_time_ms=15.5,
            session_id="session_001"
        )
        
        await monitor.track_query_execution(
            query="SELECT COUNT(*) FROM orders WHERE order_date > '2024-01-01'",
            execution_time_ms=45.2,
            session_id="session_002",
            additional_metrics={
                'rows_examined': 10000,
                'rows_returned': 1,
                'memory_usage_mb': 25.5
            }
        )
        
        # Generate performance report
        report = await monitor.get_performance_report(hours=1)
        print("Performance Report:")
        print(f"  Total queries: {report['summary_statistics']['total_queries']}")
        print(f"  Target performance rate: {report['summary_statistics']['target_performance_rate_pct']:.1f}%")
        print(f"  Average execution time: {report['summary_statistics']['avg_execution_time_ms']:.2f}ms")
        
        # Get monitoring status
        status = monitor.get_monitoring_status()
        print(f"\nMonitoring Status:")
        print(f"  Active: {status['monitoring_active']}")
        print(f"  Tracked patterns: {status['tracked_patterns']}")
        print(f"  Total queries monitored: {status['system_metrics']['total_queries_monitored']}")
        
    finally:
        await monitor.stop_monitoring()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())