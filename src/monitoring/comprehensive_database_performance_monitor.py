"""
Comprehensive Database Performance Monitor
==========================================

Advanced query performance monitoring with slow query detection,
index optimization recommendations, and real-time performance analytics.
Enhanced with 25ms threshold monitoring and intelligent alerting.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import logging
import psutil
import hashlib

from sqlalchemy import text, create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import QueuePool
from prometheus_client import Counter, Gauge, Histogram, Summary

from core.database import get_async_session
from core.logging import get_logger
from core.config import settings

logger = get_logger(__name__)

# Enhanced Performance Metrics with 25ms threshold focus
query_execution_time_detailed = Histogram(
    'db_query_execution_time_detailed_seconds',
    'Detailed database query execution time targeting <25ms SLA',
    ['query_type', 'table', 'operation', 'performance_tier'],
    buckets=(0.001, 0.005, 0.01, 0.015, 0.020, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, float('inf'))
)

slow_queries_25ms_counter = Counter(
    'db_slow_queries_25ms_total',
    'Queries exceeding 25ms threshold',
    ['query_type', 'table', 'severity']
)

query_performance_sla_compliance = Gauge(
    'db_query_sla_compliance_percentage',
    'Percentage of queries meeting <25ms SLA',
    ['table', 'operation']
)

index_optimization_recommendations = Gauge(
    'db_index_optimization_recommendations_count',
    'Number of index optimization recommendations',
    ['table', 'recommendation_type', 'priority']
)

database_health_score = Gauge(
    'db_health_score_percentage',
    'Overall database health score (0-100)',
    ['component']
)


@dataclass
class QueryPerformanceMetric:
    """Query performance tracking."""
    query_hash: str
    query_template: str
    execution_time: float
    rows_affected: int
    table_names: List[str]
    operation_type: str  # SELECT, INSERT, UPDATE, DELETE
    timestamp: datetime
    connection_id: Optional[str] = None
    business_impact: str = "unknown"
    story_id: Optional[str] = None


@dataclass
class ConnectionMetric:
    """Connection pool metrics."""
    active_connections: int
    idle_connections: int
    waiting_connections: int
    total_connections: int
    max_connections: int
    connection_errors: int
    timestamp: datetime


@dataclass
class DatabaseHealthMetric:
    """Overall database health indicators."""
    cpu_usage: float
    memory_usage: float
    disk_io_read: float
    disk_io_write: float
    lock_waits: int
    deadlocks: int
    slow_queries: int
    connection_pool_utilization: float
    timestamp: datetime


class ComprehensiveDatabaseMonitor:
    """
    Advanced database performance monitoring system.
    
    Features:
    - Real-time query performance tracking
    - Connection pool monitoring with alerting
    - Business impact analysis for database operations
    - Automatic slow query detection and analysis
    - Database health scoring with recommendations
    - Integration with Prometheus and DataDog metrics
    """
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.DatabaseMonitor")
        
        # Performance tracking
        self.query_metrics: List[QueryPerformanceMetric] = []
        self.connection_metrics: List[ConnectionMetric] = []
        self.health_metrics: List[DatabaseHealthMetric] = []
        
        # Enhanced Configuration with 25ms SLA focus
        self.slow_query_threshold = 0.025  # 25ms - enterprise SLA requirement
        self.warning_query_threshold = 0.015  # 15ms - warning threshold
        self.critical_query_threshold = 0.100  # 100ms - critical threshold
        self.max_metrics_retention = 10000

        # Slow query tracking
        self.slow_queries_cache: Dict[str, QueryPerformanceMetric] = {}
        self.optimization_recommendations: List[Dict[str, Any]] = []
        self.monitoring_active = False
        
        # Business impact mapping
        self.business_impact_map = {
            "customers": {"story_id": "4", "impact": "high", "value": "2.1M"},
            "orders": {"story_id": "1", "impact": "high", "value": "2.5M"},
            "analytics": {"story_id": "1", "impact": "medium", "value": "2.5M"},
            "inventory": {"story_id": "2", "impact": "medium", "value": "1.8M"},
            "users": {"story_id": "6", "impact": "critical", "value": "4.2M"},
            "sessions": {"story_id": "6", "impact": "high", "value": "4.2M"},
            "ml_models": {"story_id": "4.2", "impact": "high", "value": "3.5M"},
            "streaming_data": {"story_id": "7", "impact": "critical", "value": "3.4M"}
        }
        
        # Initialize Prometheus metrics
        self._initialize_prometheus_metrics()
        
    def _initialize_prometheus_metrics(self):
        """Initialize database-specific Prometheus metrics."""
        self.db_query_execution_time = Histogram(
            'database_query_execution_seconds',
            'Database query execution time with 25ms SLA focus',
            ['query_type', 'table', 'operation', 'business_impact', 'story_id'],
            buckets=[0.001, 0.005, 0.010, 0.015, 0.020, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0]
        )
        
        self.db_connections_pool = Gauge(
            'database_connection_pool_status',
            'Database connection pool status',
            ['status', 'database']  # active, idle, waiting, total
        )
        
        self.db_slow_queries_counter = Counter(
            'database_slow_queries_total',
            'Total slow database queries',
            ['severity', 'table', 'operation', 'business_impact']
        )
        
        self.db_health_score = Gauge(
            'database_health_score',
            'Overall database health score (0-100)',
            ['database', 'metric_type']
        )
        
        self.db_business_impact_score = Gauge(
            'database_business_impact_risk',
            'Database performance risk to business value',
            ['story_id', 'business_value', 'risk_level']
        )

    async def track_query_performance(self, 
                                    query: str, 
                                    execution_time: float,
                                    rows_affected: int = 0,
                                    connection_id: Optional[str] = None) -> QueryPerformanceMetric:
        """Track individual query performance with business impact analysis."""
        
        try:
            # Analyze query to extract metadata
            query_metadata = self._analyze_query(query)
            
            # Determine business impact
            business_impact, story_id = self._determine_business_impact(query_metadata["tables"])
            
            # Create performance metric
            metric = QueryPerformanceMetric(
                query_hash=self._hash_query(query),
                query_template=query_metadata["template"],
                execution_time=execution_time,
                rows_affected=rows_affected,
                table_names=query_metadata["tables"],
                operation_type=query_metadata["operation"],
                timestamp=datetime.utcnow(),
                connection_id=connection_id,
                business_impact=business_impact,
                story_id=story_id
            )
            
            # Store metric
            self.query_metrics.append(metric)
            if len(self.query_metrics) > self.max_metrics_retention:
                self.query_metrics = self.query_metrics[-self.max_metrics_retention:]
            
            # Update Prometheus metrics
            self.db_query_execution_time.labels(
                query_type=metric.operation_type,
                table=",".join(metric.table_names[:2]) if metric.table_names else "unknown",
                operation=metric.operation_type.lower(),
                business_impact=business_impact,
                story_id=story_id or "unknown"
            ).observe(execution_time)
            
            # Track slow queries
            if execution_time > self.slow_query_threshold:
                severity = "critical" if execution_time > self.critical_query_threshold else "warning"
                
                self.db_slow_queries_counter.labels(
                    severity=severity,
                    table=",".join(metric.table_names[:2]) if metric.table_names else "unknown",
                    operation=metric.operation_type.lower(),
                    business_impact=business_impact
                ).inc()
                
                # Send to enterprise metrics if available
                if hasattr(enterprise_metrics, 'db_slow_queries_total'):
                    enterprise_metrics.db_slow_queries_total.labels(
                        database="pwc_data",
                        query_type=metric.operation_type
                    ).inc()
                
                self.logger.warning(
                    f"Slow query detected: {execution_time:.3f}s, "
                    f"Tables: {metric.table_names}, "
                    f"Business Impact: {business_impact}, "
                    f"Story ID: {story_id}"
                )
            
            return metric
            
        except Exception as e:
            self.logger.error(f"Error tracking query performance: {e}")
            return None

    async def monitor_connection_pool(self) -> ConnectionMetric:
        """Monitor database connection pool health."""
        
        try:
            async with get_async_session() as session:
                # Get connection pool stats
                result = await session.execute(
                    text("""
                        SELECT 
                            count(*) as total,
                            count(CASE WHEN state = 'active' THEN 1 END) as active,
                            count(CASE WHEN state = 'idle' THEN 1 END) as idle,
                            count(CASE WHEN state = 'idle in transaction' THEN 1 END) as waiting
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                    """)
                )
                
                stats = result.fetchone()
                
                # Get max connections
                max_conn_result = await session.execute(
                    text("SELECT setting::int FROM pg_settings WHERE name = 'max_connections'")
                )
                max_connections = max_conn_result.scalar()
                
                # Create metric
                metric = ConnectionMetric(
                    active_connections=stats.active,
                    idle_connections=stats.idle,
                    waiting_connections=stats.waiting,
                    total_connections=stats.total,
                    max_connections=max_connections,
                    connection_errors=0,  # Would need additional tracking
                    timestamp=datetime.utcnow()
                )
                
                # Store metric
                self.connection_metrics.append(metric)
                if len(self.connection_metrics) > self.max_metrics_retention:
                    self.connection_metrics = self.connection_metrics[-self.max_metrics_retention:]
                
                # Update Prometheus metrics
                self.db_connections_pool.labels(status="active", database="pwc_data").set(stats.active)
                self.db_connections_pool.labels(status="idle", database="pwc_data").set(stats.idle)
                self.db_connections_pool.labels(status="waiting", database="pwc_data").set(stats.waiting)
                self.db_connections_pool.labels(status="total", database="pwc_data").set(stats.total)
                
                # Send to enterprise metrics
                if hasattr(enterprise_metrics, 'db_connections_active'):
                    enterprise_metrics.db_connections_active.labels(
                        database="pwc_data",
                        user="app"
                    ).set(stats.active)
                    
                    enterprise_metrics.db_connections_waiting.labels(
                        database="pwc_data"
                    ).set(stats.waiting)
                
                # Calculate utilization and alert if high
                utilization = (stats.total / max_connections) * 100
                if utilization > 85:
                    self.logger.warning(
                        f"High connection pool utilization: {utilization:.1f}% "
                        f"({stats.total}/{max_connections})"
                    )
                
                return metric
                
        except Exception as e:
            self.logger.error(f"Error monitoring connection pool: {e}")
            return None

    async def collect_database_health_metrics(self) -> DatabaseHealthMetric:
        """Collect comprehensive database health metrics."""
        
        try:
            async with get_async_session() as session:
                # Database-specific metrics
                queries = {
                    "locks": "SELECT count(*) FROM pg_locks WHERE NOT granted",
                    "deadlocks": "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()",
                    "temp_files": "SELECT temp_files FROM pg_stat_database WHERE datname = current_database()",
                    "temp_bytes": "SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()",
                }
                
                db_stats = {}
                for metric_name, query in queries.items():
                    try:
                        result = await session.execute(text(query))
                        db_stats[metric_name] = result.scalar() or 0
                    except Exception as e:
                        self.logger.warning(f"Failed to collect {metric_name}: {e}")
                        db_stats[metric_name] = 0
                
                # System metrics
                cpu_usage = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk_io = psutil.disk_io_counters()
                
                # Create health metric
                metric = DatabaseHealthMetric(
                    cpu_usage=cpu_usage,
                    memory_usage=memory.percent,
                    disk_io_read=disk_io.read_bytes if disk_io else 0,
                    disk_io_write=disk_io.write_bytes if disk_io else 0,
                    lock_waits=db_stats["locks"],
                    deadlocks=db_stats["deadlocks"],
                    slow_queries=len([q for q in self.query_metrics[-100:] 
                                    if q.execution_time > self.slow_query_threshold]),
                    connection_pool_utilization=0,  # Will be calculated
                    timestamp=datetime.utcnow()
                )
                
                # Store metric
                self.health_metrics.append(metric)
                if len(self.health_metrics) > self.max_metrics_retention:
                    self.health_metrics = self.health_metrics[-self.max_metrics_retention:]
                
                # Calculate health score
                health_score = self._calculate_health_score(metric)
                
                # Update Prometheus metrics
                self.db_health_score.labels(
                    database="pwc_data",
                    metric_type="overall"
                ).set(health_score)
                
                self.db_health_score.labels(
                    database="pwc_data", 
                    metric_type="performance"
                ).set(max(0, 100 - metric.slow_queries * 10))
                
                self.db_health_score.labels(
                    database="pwc_data",
                    metric_type="resource_usage"
                ).set(max(0, 100 - max(metric.cpu_usage, metric.memory_usage)))
                
                # Business impact assessment
                await self._assess_business_impact(metric)
                
                return metric
                
        except Exception as e:
            self.logger.error(f"Error collecting database health metrics: {e}")
            return None

    def _analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze SQL query to extract metadata."""
        
        query_lower = query.lower().strip()
        
        # Determine operation type
        if query_lower.startswith('select'):
            operation = 'SELECT'
        elif query_lower.startswith('insert'):
            operation = 'INSERT'
        elif query_lower.startswith('update'):
            operation = 'UPDATE'
        elif query_lower.startswith('delete'):
            operation = 'DELETE'
        else:
            operation = 'OTHER'
        
        # Extract table names (simplified regex-based approach)
        import re
        
        # Look for table patterns
        table_patterns = [
            r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\binto\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\bupdate\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        tables = []
        for pattern in table_patterns:
            matches = re.findall(pattern, query_lower)
            tables.extend(matches)
        
        # Remove duplicates while preserving order
        tables = list(dict.fromkeys(tables))
        
        # Create query template (simplified)
        template = re.sub(r'\b\d+\b', '?', query)  # Replace numbers with ?
        template = re.sub(r"'[^']*'", '?', template)  # Replace strings with ?
        
        return {
            "operation": operation,
            "tables": tables,
            "template": template[:200] + "..." if len(template) > 200 else template
        }

    def _determine_business_impact(self, table_names: List[str]) -> Tuple[str, Optional[str]]:
        """Determine business impact based on table names."""
        
        if not table_names:
            return "low", None
        
        # Check for high-impact tables
        for table in table_names:
            if table in self.business_impact_map:
                mapping = self.business_impact_map[table]
                return mapping["impact"], mapping["story_id"]
        
        # Default classification based on common patterns
        for table in table_names:
            if any(keyword in table for keyword in ['user', 'customer', 'order', 'payment']):
                return "high", "4"
            elif any(keyword in table for keyword in ['analytics', 'report', 'dashboard']):
                return "medium", "1"
            elif any(keyword in table for keyword in ['log', 'audit', 'session']):
                return "medium", "6"
        
        return "low", None

    def _hash_query(self, query: str) -> str:
        """Create hash for query deduplication."""
        import hashlib
        # Normalize query for hashing
        normalized = re.sub(r'\s+', ' ', query.lower().strip())
        return hashlib.md5(normalized.encode()).hexdigest()[:12]

    async def start_slow_query_monitoring(self):
        """Start continuous slow query monitoring with 25ms threshold detection."""
        self.monitoring_active = True
        self.logger.info("Starting enhanced slow query monitoring with 25ms SLA threshold")

        # Start monitoring tasks
        monitoring_tasks = [
            asyncio.create_task(self._monitor_pg_stat_statements()),
            asyncio.create_task(self._monitor_active_queries()),
            asyncio.create_task(self._analyze_query_patterns()),
            asyncio.create_task(self._generate_optimization_recommendations())
        ]

        try:
            await asyncio.gather(*monitoring_tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error in slow query monitoring: {e}")

    async def _monitor_pg_stat_statements(self):
        """Monitor pg_stat_statements for slow query analysis."""
        while self.monitoring_active:
            try:
                async with get_async_session() as session:
                    # Query for slow queries from pg_stat_statements
                    slow_query_sql = text("""
                        SELECT
                            queryid,
                            query,
                            calls,
                            mean_exec_time / 1000.0 as mean_time_seconds,
                            max_exec_time / 1000.0 as max_time_seconds,
                            total_exec_time / 1000.0 as total_time_seconds,
                            rows,
                            100.0 * shared_blks_hit /
                                nullif(shared_blks_hit + shared_blks_read, 0) AS cache_hit_ratio
                        FROM pg_stat_statements
                        WHERE mean_exec_time > 25000  -- 25ms threshold in microseconds
                        ORDER BY mean_exec_time DESC
                        LIMIT 50
                    """)

                    result = await session.execute(slow_query_sql)
                    slow_queries_found = 0

                    for row in result:
                        slow_queries_found += 1

                        # Analyze the slow query
                        query_metadata = self._analyze_query(row.query)
                        business_impact, story_id = self._determine_business_impact(query_metadata["tables"])

                        # Determine severity
                        if row.mean_time_seconds > self.critical_query_threshold:
                            severity = "critical"
                        elif row.mean_time_seconds > self.slow_query_threshold * 2:  # 50ms
                            severity = "high"
                        else:
                            severity = "medium"

                        # Update metrics
                        for table in query_metadata["tables"]:
                            slow_queries_25ms_counter.labels(
                                query_type=query_metadata["operation"],
                                table=table,
                                severity=severity
                            ).inc()

                            query_execution_time_detailed.labels(
                                query_type=query_metadata["operation"],
                                table=table,
                                operation=query_metadata["operation"],
                                performance_tier=severity
                            ).observe(row.mean_time_seconds)

                        # Cache slow query for analysis
                        query_hash = self._hash_query(row.query)
                        self.slow_queries_cache[query_hash] = QueryPerformanceMetric(
                            query_hash=query_hash,
                            query_template=query_metadata["template"],
                            execution_time=row.mean_time_seconds,
                            rows_affected=row.rows or 0,
                            table_names=query_metadata["tables"],
                            operation_type=query_metadata["operation"],
                            timestamp=datetime.utcnow(),
                            business_impact=business_impact,
                            story_id=story_id
                        )

                        # Log critical slow queries
                        if severity == "critical":
                            self.logger.error(
                                f"CRITICAL slow query detected: {row.mean_time_seconds*1000:.1f}ms - "
                                f"Query: {row.query[:100]}... "
                                f"Business Impact: {business_impact} (Story {story_id})"
                            )
                        elif severity == "high":
                            self.logger.warning(
                                f"HIGH impact slow query: {row.mean_time_seconds*1000:.1f}ms - "
                                f"Tables: {','.join(query_metadata['tables'])} "
                                f"Business Impact: {business_impact}"
                            )

                    # Update SLA compliance metrics
                    await self._update_sla_compliance()

                    self.logger.debug(f"Slow query monitoring cycle completed: {slow_queries_found} slow queries found")

            except Exception as e:
                self.logger.error(f"Error in pg_stat_statements monitoring: {e}")

            await asyncio.sleep(60)  # Monitor every minute

    async def _monitor_active_queries(self):
        """Monitor currently active queries for real-time slow query detection."""
        while self.monitoring_active:
            try:
                async with get_async_session() as session:
                    # Monitor currently running queries
                    active_query_sql = text("""
                        SELECT
                            pid,
                            query,
                            state,
                            EXTRACT(EPOCH FROM NOW() - query_start) as duration_seconds,
                            datname,
                            usename
                        FROM pg_stat_activity
                        WHERE state = 'active'
                        AND query NOT LIKE '%pg_stat_activity%'
                        AND query_start IS NOT NULL
                        AND EXTRACT(EPOCH FROM NOW() - query_start) > 0.025  -- 25ms threshold
                        ORDER BY duration_seconds DESC
                    """)

                    result = await session.execute(active_query_sql)
                    long_running_found = 0

                    for row in result:
                        long_running_found += 1

                        # Analyze long-running query
                        query_metadata = self._analyze_query(row.query)
                        business_impact, story_id = self._determine_business_impact(query_metadata["tables"])

                        # Log long-running queries
                        if row.duration_seconds > 1.0:  # 1 second
                            self.logger.warning(
                                f"Long-running query detected: {row.duration_seconds:.2f}s - "
                                f"PID: {row.pid}, User: {row.usename}, "
                                f"Business Impact: {business_impact}"
                            )

                        # Update metrics
                        for table in query_metadata["tables"]:
                            query_execution_time_detailed.labels(
                                query_type=query_metadata["operation"],
                                table=table,
                                operation=query_metadata["operation"],
                                performance_tier="active_long_running"
                            ).observe(row.duration_seconds)

                    if long_running_found > 5:
                        self.logger.warning(f"High number of long-running queries: {long_running_found}")

            except Exception as e:
                self.logger.error(f"Error monitoring active queries: {e}")

            await asyncio.sleep(30)  # Monitor every 30 seconds

    async def _update_sla_compliance(self):
        """Update SLA compliance metrics based on recent performance."""
        try:
            # Calculate SLA compliance for each table/operation combination
            current_time = datetime.utcnow()
            recent_cutoff = current_time - timedelta(minutes=15)

            # Get recent queries from cache
            recent_queries = [
                q for q in self.query_metrics
                if q.timestamp >= recent_cutoff
            ]

            # Group by table and operation
            table_performance = {}
            for query in recent_queries:
                for table in query.table_names:
                    key = f"{table}_{query.operation_type}"
                    if key not in table_performance:
                        table_performance[key] = {
                            'total_queries': 0,
                            'fast_queries': 0,
                            'table': table,
                            'operation': query.operation_type
                        }

                    table_performance[key]['total_queries'] += 1
                    if query.execution_time <= self.slow_query_threshold:
                        table_performance[key]['fast_queries'] += 1

            # Update compliance metrics
            for key, perf in table_performance.items():
                if perf['total_queries'] > 0:
                    compliance_percentage = (perf['fast_queries'] / perf['total_queries']) * 100

                    query_performance_sla_compliance.labels(
                        table=perf['table'],
                        operation=perf['operation']
                    ).set(compliance_percentage)

                    # Log SLA violations
                    if compliance_percentage < 95:  # 95% SLA target
                        self.logger.warning(
                            f"SLA compliance below target: {perf['table']}.{perf['operation']} "
                            f"at {compliance_percentage:.1f}% (target: 95%)"
                        )

        except Exception as e:
            self.logger.error(f"Error updating SLA compliance: {e}")

    async def _analyze_query_patterns(self):
        """Analyze query patterns to identify optimization opportunities."""
        while self.monitoring_active:
            try:
                # Analyze patterns every 10 minutes
                await asyncio.sleep(600)

                if not self.slow_queries_cache:
                    continue

                # Pattern analysis
                patterns = {}
                for query_hash, query_metric in self.slow_queries_cache.items():
                    # Group by table and operation
                    for table in query_metric.table_names:
                        pattern_key = f"{table}_{query_metric.operation_type}"
                        if pattern_key not in patterns:
                            patterns[pattern_key] = {
                                'count': 0,
                                'avg_time': 0,
                                'total_time': 0,
                                'queries': [],
                                'table': table,
                                'operation': query_metric.operation_type
                            }

                        patterns[pattern_key]['count'] += 1
                        patterns[pattern_key]['total_time'] += query_metric.execution_time
                        patterns[pattern_key]['queries'].append(query_metric)

                # Calculate averages and identify hotspots
                for pattern_key, pattern_data in patterns.items():
                    pattern_data['avg_time'] = pattern_data['total_time'] / pattern_data['count']

                # Log top problematic patterns
                top_patterns = sorted(
                    patterns.items(),
                    key=lambda x: x[1]['total_time'],
                    reverse=True
                )[:5]

                for pattern_key, pattern_data in top_patterns:
                    self.logger.info(
                        f"Query pattern analysis: {pattern_key} - "
                        f"{pattern_data['count']} slow queries, "
                        f"avg time: {pattern_data['avg_time']*1000:.1f}ms, "
                        f"total impact: {pattern_data['total_time']:.2f}s"
                    )

            except Exception as e:
                self.logger.error(f"Error in query pattern analysis: {e}")

    async def _generate_optimization_recommendations(self):
        """Generate database optimization recommendations based on slow query analysis."""
        while self.monitoring_active:
            try:
                # Generate recommendations every 15 minutes
                await asyncio.sleep(900)

                if not self.slow_queries_cache:
                    continue

                recommendations = []

                # Analyze slow queries for index recommendations
                table_query_analysis = {}
                for query_metric in self.slow_queries_cache.values():
                    for table in query_metric.table_names:
                        if table not in table_query_analysis:
                            table_query_analysis[table] = {
                                'slow_selects': 0,
                                'slow_updates': 0,
                                'slow_inserts': 0,
                                'avg_time': 0,
                                'total_time': 0,
                                'count': 0
                            }

                        analysis = table_query_analysis[table]
                        analysis['count'] += 1
                        analysis['total_time'] += query_metric.execution_time

                        if query_metric.operation_type == 'SELECT':
                            analysis['slow_selects'] += 1
                        elif query_metric.operation_type == 'UPDATE':
                            analysis['slow_updates'] += 1
                        elif query_metric.operation_type == 'INSERT':
                            analysis['slow_inserts'] += 1

                # Generate recommendations
                for table, analysis in table_query_analysis.items():
                    analysis['avg_time'] = analysis['total_time'] / analysis['count']

                    # Recommend indexes for tables with many slow SELECTs
                    if analysis['slow_selects'] > 5:
                        priority = "high" if analysis['avg_time'] > 0.1 else "medium"
                        recommendations.append({
                            'table': table,
                            'type': 'add_index',
                            'priority': priority,
                            'reason': f"Table has {analysis['slow_selects']} slow SELECT queries",
                            'estimated_improvement': '30-60%'
                        })

                        # Update recommendation metrics
                        index_optimization_recommendations.labels(
                            table=table,
                            recommendation_type='add_index',
                            priority=priority
                        ).set(analysis['slow_selects'])

                    # Recommend query optimization for tables with slow UPDATEs
                    if analysis['slow_updates'] > 3:
                        recommendations.append({
                            'table': table,
                            'type': 'optimize_updates',
                            'priority': 'medium',
                            'reason': f"Table has {analysis['slow_updates']} slow UPDATE queries",
                            'estimated_improvement': '20-40%'
                        })

                # Store and log recommendations
                self.optimization_recommendations = recommendations[:10]  # Keep top 10

                if recommendations:
                    self.logger.info(f"Generated {len(recommendations)} optimization recommendations")
                    for rec in recommendations[:3]:  # Log top 3
                        self.logger.info(
                            f"Recommendation: {rec['type']} for {rec['table']} "
                            f"(Priority: {rec['priority']}) - {rec['reason']}"
                        )

            except Exception as e:
                self.logger.error(f"Error generating optimization recommendations: {e}")

    async def get_slow_query_report(self, hours: int = 1) -> Dict[str, Any]:
        """Generate comprehensive slow query report."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        # Filter slow queries from cache
        slow_queries = {
            hash_key: query for hash_key, query in self.slow_queries_cache.items()
            if query.timestamp >= cutoff_time and query.execution_time > self.slow_query_threshold
        }

        if not slow_queries:
            return {
                'status': 'no_slow_queries',
                'message': f'No slow queries detected in the last {hours} hours'
            }

        # Sort by execution time
        sorted_queries = sorted(
            slow_queries.values(),
            key=lambda q: q.execution_time,
            reverse=True
        )

        # Calculate statistics
        execution_times = [q.execution_time for q in sorted_queries]
        total_slow_time = sum(execution_times)

        # Group by business impact
        impact_groups = {}
        for query in sorted_queries:
            impact = query.business_impact
            if impact not in impact_groups:
                impact_groups[impact] = []
            impact_groups[impact].append(query)

        return {
            'report_period_hours': hours,
            'timestamp': datetime.utcnow().isoformat(),
            'summary': {
                'total_slow_queries': len(sorted_queries),
                'total_slow_time_seconds': total_slow_time,
                'average_slow_time_ms': (sum(execution_times) / len(execution_times)) * 1000,
                'worst_query_time_ms': max(execution_times) * 1000,
                'sla_violations': len([q for q in sorted_queries if q.execution_time > self.slow_query_threshold])
            },
            'top_slow_queries': [
                {
                    'query_hash': q.query_hash,
                    'execution_time_ms': q.execution_time * 1000,
                    'table_names': q.table_names,
                    'operation_type': q.operation_type,
                    'business_impact': q.business_impact,
                    'story_id': q.story_id,
                    'query_preview': q.query_template[:100] + '...' if len(q.query_template) > 100 else q.query_template
                }
                for q in sorted_queries[:10]
            ],
            'business_impact_analysis': {
                impact: {
                    'query_count': len(queries),
                    'total_time_seconds': sum(q.execution_time for q in queries),
                    'affected_stories': list(set(q.story_id for q in queries if q.story_id))
                }
                for impact, queries in impact_groups.items()
            },
            'optimization_recommendations': self.optimization_recommendations
        }

    def stop_monitoring(self):
        """Stop slow query monitoring."""
        self.monitoring_active = False
        self.logger.info("Slow query monitoring stopped")

    def _calculate_health_score(self, metric: DatabaseHealthMetric) -> float:
        """Calculate overall database health score (0-100)."""
        
        score = 100
        
        # CPU usage impact
        if metric.cpu_usage > 90:
            score -= 20
        elif metric.cpu_usage > 80:
            score -= 10
        elif metric.cpu_usage > 70:
            score -= 5
        
        # Memory usage impact
        if metric.memory_usage > 90:
            score -= 20
        elif metric.memory_usage > 80:
            score -= 10
        elif metric.memory_usage > 70:
            score -= 5
        
        # Lock waits impact
        if metric.lock_waits > 10:
            score -= 15
        elif metric.lock_waits > 5:
            score -= 8
        elif metric.lock_waits > 0:
            score -= 3
        
        # Deadlocks impact
        score -= min(20, metric.deadlocks * 5)
        
        # Slow queries impact
        score -= min(25, metric.slow_queries * 2)
        
        return max(0, score)

    async def _assess_business_impact(self, metric: DatabaseHealthMetric):
        """Assess business impact of database performance issues."""
        
        try:
            health_score = self._calculate_health_score(metric)
            
            # Calculate risk for each business story
            for story_id, story_info in {
                "1": {"value": "2.5M", "tables": ["orders", "analytics"]},
                "2": {"value": "1.8M", "tables": ["inventory", "quality"]},
                "4": {"value": "2.1M", "tables": ["customers", "api"]},
                "4.2": {"value": "3.5M", "tables": ["ml_models", "predictions"]},
                "6": {"value": "4.2M", "tables": ["users", "sessions"]},
                "7": {"value": "3.4M", "tables": ["streaming_data", "events"]}
            }.items():
                
                # Calculate risk level based on health score
                if health_score < 60:
                    risk_level = "high"
                elif health_score < 80:
                    risk_level = "medium"
                else:
                    risk_level = "low"
                
                # Risk score (inverse of health score)
                risk_score = max(0, 100 - health_score)
                
                self.db_business_impact_score.labels(
                    story_id=story_id,
                    business_value=story_info["value"],
                    risk_level=risk_level
                ).set(risk_score)
            
            if health_score < 70:
                self.logger.warning(
                    f"Database health degraded: {health_score:.1f}/100, "
                    f"Business impact risk increased across BMAD stories"
                )
                
        except Exception as e:
            self.logger.error(f"Error assessing business impact: {e}")

    async def get_performance_summary(self, minutes: int = 60) -> Dict[str, Any]:
        """Get performance summary for the last N minutes."""
        
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        
        # Filter recent metrics
        recent_queries = [q for q in self.query_metrics if q.timestamp >= cutoff_time]
        recent_connections = [c for c in self.connection_metrics if c.timestamp >= cutoff_time]
        recent_health = [h for h in self.health_metrics if h.timestamp >= cutoff_time]
        
        if not recent_queries and not recent_connections and not recent_health:
            return {"error": "No recent metrics available"}
        
        summary = {
            "time_window_minutes": minutes,
            "timestamp": datetime.utcnow().isoformat(),
            "query_performance": {},
            "connection_health": {},
            "database_health": {},
            "business_impact": {}
        }
        
        # Query performance summary
        if recent_queries:
            durations = [q.execution_time for q in recent_queries]
            summary["query_performance"] = {
                "total_queries": len(recent_queries),
                "avg_duration": sum(durations) / len(durations),
                "max_duration": max(durations),
                "slow_queries": len([q for q in recent_queries if q.execution_time > self.slow_query_threshold]),
                "critical_queries": len([q for q in recent_queries if q.execution_time > self.critical_query_threshold]),
                "most_impacted_tables": self._get_most_impacted_tables(recent_queries)
            }
        
        # Connection health summary
        if recent_connections:
            latest_conn = recent_connections[-1]
            utilization = (latest_conn.total_connections / latest_conn.max_connections) * 100
            
            summary["connection_health"] = {
                "current_utilization_percent": utilization,
                "active_connections": latest_conn.active_connections,
                "idle_connections": latest_conn.idle_connections,
                "waiting_connections": latest_conn.waiting_connections,
                "max_connections": latest_conn.max_connections
            }
        
        # Database health summary
        if recent_health:
            latest_health = recent_health[-1]
            health_score = self._calculate_health_score(latest_health)
            
            summary["database_health"] = {
                "overall_score": health_score,
                "cpu_usage_percent": latest_health.cpu_usage,
                "memory_usage_percent": latest_health.memory_usage,
                "active_locks": latest_health.lock_waits,
                "deadlocks": latest_health.deadlocks,
                "recent_slow_queries": latest_health.slow_queries
            }
        
        # Business impact summary
        summary["business_impact"] = self._calculate_business_impact_summary(recent_queries)
        
        return summary

    def _get_most_impacted_tables(self, queries: List[QueryPerformanceMetric]) -> List[Dict[str, Any]]:
        """Get tables most impacted by slow queries."""
        
        table_impact = {}
        
        for query in queries:
            if query.execution_time > self.slow_query_threshold:
                for table in query.table_names:
                    if table not in table_impact:
                        table_impact[table] = {
                            "slow_query_count": 0,
                            "total_slow_time": 0,
                            "business_impact": query.business_impact,
                            "story_id": query.story_id
                        }
                    
                    table_impact[table]["slow_query_count"] += 1
                    table_impact[table]["total_slow_time"] += query.execution_time
        
        # Sort by impact
        sorted_tables = sorted(
            table_impact.items(),
            key=lambda x: x[1]["total_slow_time"],
            reverse=True
        )
        
        return [
            {
                "table": table,
                "slow_queries": data["slow_query_count"],
                "total_slow_time": data["total_slow_time"],
                "business_impact": data["business_impact"],
                "story_id": data["story_id"]
            }
            for table, data in sorted_tables[:10]
        ]

    def _calculate_business_impact_summary(self, queries: List[QueryPerformanceMetric]) -> Dict[str, Any]:
        """Calculate business impact summary."""
        
        story_impact = {}
        
        for query in queries:
            if query.story_id and query.execution_time > self.slow_query_threshold:
                if query.story_id not in story_impact:
                    story_impact[query.story_id] = {
                        "slow_queries": 0,
                        "total_delay": 0,
                        "business_impact": query.business_impact,
                        "estimated_value_at_risk": 0
                    }
                
                story_impact[query.story_id]["slow_queries"] += 1
                story_impact[query.story_id]["total_delay"] += query.execution_time
                
                # Estimate value at risk (simplified calculation)
                if query.business_impact == "critical":
                    story_impact[query.story_id]["estimated_value_at_risk"] += 1000
                elif query.business_impact == "high":
                    story_impact[query.story_id]["estimated_value_at_risk"] += 500
                elif query.business_impact == "medium":
                    story_impact[query.story_id]["estimated_value_at_risk"] += 200
        
        return {
            "stories_impacted": len(story_impact),
            "total_estimated_risk_dollars": sum(s["estimated_value_at_risk"] for s in story_impact.values()),
            "story_details": story_impact
        }

    async def start_monitoring(self, interval: int = 30):
        """Start continuous database monitoring."""
        
        self.logger.info("Starting comprehensive database performance monitoring")
        
        while True:
            try:
                # Collect all metrics
                await asyncio.gather(
                    self.monitor_connection_pool(),
                    self.collect_database_health_metrics()
                )
                
                # Generate summary every 10 minutes
                if int(time.time()) % 600 == 0:
                    summary = await self.get_performance_summary(10)
                    self.logger.info(f"Database performance summary: {summary}")
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
            
            await asyncio.sleep(interval)


# Global database monitor instance
_db_monitor: Optional[ComprehensiveDatabaseMonitor] = None


def get_database_monitor() -> ComprehensiveDatabaseMonitor:
    """Get or create database monitor instance."""
    global _db_monitor
    
    if _db_monitor is None:
        _db_monitor = ComprehensiveDatabaseMonitor()
    
    return _db_monitor


# Context manager for automatic query tracking
class DatabaseQueryTracker:
    """Context manager for automatic database query performance tracking."""
    
    def __init__(self, query_description: str):
        self.query_description = query_description
        self.start_time = None
        self.monitor = get_database_monitor()
    
    async def __aenter__(self):
        self.start_time = time.time()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            execution_time = time.time() - self.start_time
            await self.monitor.track_query_performance(
                query=self.query_description,
                execution_time=execution_time
            )


# Decorator for automatic query tracking
def track_database_query(query_description: str):
    """Decorator for automatic database query performance tracking."""
    
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                monitor = get_database_monitor()
                await monitor.track_query_performance(
                    query=query_description,
                    execution_time=execution_time
                )
                
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                monitor = get_database_monitor()
                await monitor.track_query_performance(
                    query=f"{query_description} (FAILED: {str(e)[:50]})",
                    execution_time=execution_time
                )
                raise
        
        return wrapper
    return decorator


if __name__ == "__main__":
    # Start database monitoring
    monitor = ComprehensiveDatabaseMonitor()
    asyncio.run(monitor.start_monitoring())