"""
Advanced Database Performance Monitoring
Comprehensive PostgreSQL performance tracking with query analysis, connection monitoring,
and intelligent alerting for database optimization.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import asyncpg
import psutil
from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import QueuePool

from core.database import get_async_session
from core.logging import get_logger
from monitoring.enterprise_prometheus_metrics import enterprise_metrics

logger = get_logger(__name__)


class QueryType(Enum):
    """Types of database queries."""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    ANALYZE = "ANALYZE"
    VACUUM = "VACUUM"
    UNKNOWN = "UNKNOWN"


class QueryComplexity(Enum):
    """Query complexity levels."""
    SIMPLE = "simple"      # Single table, no joins
    MODERATE = "moderate"  # Joins, subqueries
    COMPLEX = "complex"    # Multiple joins, complex aggregations
    VERY_COMPLEX = "very_complex"  # Recursive, window functions


class LockType(Enum):
    """PostgreSQL lock types."""
    ACCESS_SHARE = "AccessShareLock"
    ROW_SHARE = "RowShareLock"
    ROW_EXCLUSIVE = "RowExclusiveLock"
    SHARE_UPDATE_EXCLUSIVE = "ShareUpdateExclusiveLock"
    SHARE = "ShareLock"
    SHARE_ROW_EXCLUSIVE = "ShareRowExclusiveLock"
    EXCLUSIVE = "ExclusiveLock"
    ACCESS_EXCLUSIVE = "AccessExclusiveLock"


@dataclass
class QueryMetrics:
    """Metrics for individual database queries."""
    query_id: str
    query_text: str
    query_type: QueryType
    complexity: QueryComplexity
    execution_count: int = 0
    total_time_ms: float = 0.0
    min_time_ms: float = float('inf')
    max_time_ms: float = 0.0
    avg_time_ms: float = 0.0
    rows_returned: int = 0
    rows_affected: int = 0
    buffer_hits: int = 0
    buffer_misses: int = 0
    temp_files: int = 0
    temp_bytes: int = 0
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)
    tables_accessed: Set[str] = field(default_factory=set)
    indexes_used: Set[str] = field(default_factory=set)
    plan_hash: Optional[str] = None
    error_count: int = 0


@dataclass
class ConnectionMetrics:
    """Metrics for database connections."""
    database_name: str
    username: str
    application_name: str
    client_addr: str
    connection_start: datetime
    state: str
    active_queries: int = 0
    total_queries: int = 0
    waiting: bool = False
    wait_event_type: Optional[str] = None
    wait_event: Optional[str] = None


@dataclass
class TableMetrics:
    """Performance metrics for database tables."""
    table_name: str
    schema_name: str
    seq_scans: int = 0
    seq_tup_read: int = 0
    idx_scans: int = 0
    idx_tup_fetch: int = 0
    n_tup_ins: int = 0
    n_tup_upd: int = 0
    n_tup_del: int = 0
    n_tup_hot_upd: int = 0
    n_live_tup: int = 0
    n_dead_tup: int = 0
    vacuum_count: int = 0
    autovacuum_count: int = 0
    analyze_count: int = 0
    autoanalyze_count: int = 0
    table_size_bytes: int = 0
    indexes_size_bytes: int = 0
    bloat_ratio: float = 0.0


@dataclass
class IndexMetrics:
    """Performance metrics for database indexes."""
    index_name: str
    table_name: str
    schema_name: str
    index_scans: int = 0
    tuples_read: int = 0
    tuples_fetched: int = 0
    size_bytes: int = 0
    bloat_ratio: float = 0.0
    is_unique: bool = False
    is_primary: bool = False
    usage_ratio: float = 0.0


@dataclass
class DatabaseHealth:
    """Overall database health metrics."""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    waiting_connections: int = 0
    max_connections: int = 0
    connection_utilization: float = 0.0
    
    total_queries_per_second: float = 0.0
    slow_queries_per_second: float = 0.0
    error_rate: float = 0.0
    
    cache_hit_ratio: float = 0.0
    buffer_efficiency: float = 0.0
    
    database_size_bytes: int = 0
    temp_files_count: int = 0
    temp_bytes: int = 0
    
    checkpoint_frequency: float = 0.0
    wal_size_bytes: int = 0
    
    bloat_ratio: float = 0.0
    fragmentation_ratio: float = 0.0


class DatabasePerformanceMonitor:
    """Advanced PostgreSQL performance monitoring system."""
    
    def __init__(self, connection_string: Optional[str] = None):
        self.logger = get_logger(__name__)
        self.connection_string = connection_string
        
        # Metrics storage
        self.query_metrics: Dict[str, QueryMetrics] = {}
        self.connection_metrics: Dict[str, ConnectionMetrics] = {}
        self.table_metrics: Dict[str, TableMetrics] = {}
        self.index_metrics: Dict[str, IndexMetrics] = {}
        self.health_history: deque[DatabaseHealth] = deque(maxlen=1000)
        
        # Performance tracking
        self.slow_query_threshold_ms = 1000.0  # 1 second
        self.query_history: deque = deque(maxlen=10000)
        self.alert_thresholds = self._initialize_alert_thresholds()
        
        # Monitoring state
        self.monitoring_active = False
        self.last_monitoring_run = datetime.utcnow()
        
    def _initialize_alert_thresholds(self) -> Dict[str, Any]:
        """Initialize alert thresholds for database monitoring."""
        return {
            "connection_utilization": 80.0,      # 80% of max connections
            "slow_query_rate": 5.0,              # 5 slow queries per second
            "cache_hit_ratio": 95.0,             # Below 95% cache hit ratio
            "error_rate": 1.0,                   # 1% error rate
            "lock_wait_time_ms": 5000.0,         # 5 seconds lock wait
            "temp_file_threshold_mb": 100.0,     # 100MB temp files
            "bloat_ratio": 20.0,                 # 20% table bloat
            "vacuum_frequency_hours": 24.0,      # Vacuum every 24 hours
            "checkpoint_frequency_minutes": 5.0   # Checkpoint every 5 minutes
        }
    
    def _generate_query_id(self, query_text: str) -> str:
        """Generate a stable ID for query text (normalized)."""
        import hashlib
        
        # Normalize query - remove extra whitespace, convert to lowercase
        normalized = re.sub(r'\s+', ' ', query_text.strip().lower())
        
        # Remove literal values for better grouping
        normalized = re.sub(r"'[^']*'", "'***'", normalized)  # String literals
        normalized = re.sub(r'\b\d+\b', '***', normalized)    # Numbers
        
        return hashlib.md5(normalized.encode()).hexdigest()[:16]
    
    def _classify_query_type(self, query_text: str) -> QueryType:
        """Classify query by its primary operation."""
        query_upper = query_text.strip().upper()
        
        for query_type in QueryType:
            if query_upper.startswith(query_type.value):
                return query_type
        
        return QueryType.UNKNOWN
    
    def _analyze_query_complexity(self, query_text: str) -> QueryComplexity:
        """Analyze query complexity based on structure."""
        query_upper = query_text.upper()
        
        complexity_indicators = {
            'joins': query_upper.count('JOIN'),
            'subqueries': query_upper.count('SELECT') - 1,  # Minus the main SELECT
            'aggregations': (
                query_upper.count('GROUP BY') +
                query_upper.count('HAVING') +
                query_upper.count('SUM(') +
                query_upper.count('COUNT(') +
                query_upper.count('AVG(') +
                query_upper.count('MAX(') +
                query_upper.count('MIN(')
            ),
            'window_functions': query_upper.count('OVER('),
            'recursive_cte': query_upper.count('WITH RECURSIVE'),
            'exists_clauses': query_upper.count('EXISTS')
        }
        
        total_complexity = sum(complexity_indicators.values())
        
        if total_complexity == 0:
            return QueryComplexity.SIMPLE
        elif total_complexity <= 3:
            return QueryComplexity.MODERATE
        elif total_complexity <= 8:
            return QueryComplexity.COMPLEX
        else:
            return QueryComplexity.VERY_COMPLEX
    
    def _extract_table_names(self, query_text: str) -> Set[str]:
        """Extract table names from SQL query."""
        tables = set()
        query_upper = query_text.upper()
        
        # Simple regex-based extraction (could be enhanced with SQL parser)
        patterns = [
            r'FROM\s+([^\s,\(\)]+)',
            r'JOIN\s+([^\s,\(\)]+)',
            r'INTO\s+([^\s,\(\)]+)',
            r'UPDATE\s+([^\s,\(\)]+)',
            r'DELETE\s+FROM\s+([^\s,\(\)]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, query_upper)
            for match in matches:
                table_name = match.strip().lower()
                if not table_name.startswith('('):
                    tables.add(table_name)
        
        return tables
    
    async def record_query_execution(
        self,
        query_text: str,
        execution_time_ms: float,
        rows_returned: int = 0,
        rows_affected: int = 0,
        error: Optional[str] = None
    ):
        """Record query execution metrics."""
        
        query_id = self._generate_query_id(query_text)
        query_type = self._classify_query_type(query_text)
        complexity = self._analyze_query_complexity(query_text)
        tables_accessed = self._extract_table_names(query_text)
        
        # Update or create query metrics
        if query_id not in self.query_metrics:
            self.query_metrics[query_id] = QueryMetrics(
                query_id=query_id,
                query_text=query_text[:1000],  # Truncate long queries
                query_type=query_type,
                complexity=complexity,
                tables_accessed=tables_accessed
            )
        
        metrics = self.query_metrics[query_id]
        metrics.execution_count += 1
        metrics.total_time_ms += execution_time_ms
        metrics.min_time_ms = min(metrics.min_time_ms, execution_time_ms)
        metrics.max_time_ms = max(metrics.max_time_ms, execution_time_ms)
        metrics.avg_time_ms = metrics.total_time_ms / metrics.execution_count
        metrics.rows_returned += rows_returned
        metrics.rows_affected += rows_affected
        metrics.last_seen = datetime.utcnow()
        
        if error:
            metrics.error_count += 1
        
        # Record in history
        self.query_history.append({
            'timestamp': datetime.utcnow(),
            'query_id': query_id,
            'execution_time_ms': execution_time_ms,
            'query_type': query_type.value,
            'complexity': complexity.value,
            'error': error is not None
        })
        
        # Update Prometheus metrics
        enterprise_metrics.db_query_duration.labels(
            query_type=query_type.value.lower(),
            table=','.join(sorted(tables_accessed)[:3]) if tables_accessed else 'unknown',
            operation='read' if query_type == QueryType.SELECT else 'write'
        ).observe(execution_time_ms / 1000)  # Convert to seconds
        
        if execution_time_ms > self.slow_query_threshold_ms:
            enterprise_metrics.db_slow_queries_total.labels(
                database='pwc_data',
                query_type=query_type.value.lower()
            ).inc()
    
    async def collect_connection_metrics(self):
        """Collect database connection metrics."""
        try:
            async with get_async_session() as session:
                # Get active connections
                result = await session.execute(text("""
                    SELECT 
                        datname,
                        usename,
                        application_name,
                        client_addr,
                        backend_start,
                        state,
                        query_start,
                        wait_event_type,
                        wait_event,
                        query
                    FROM pg_stat_activity 
                    WHERE state != 'idle'
                    ORDER BY backend_start
                """))
                
                active_connections = result.fetchall()
                
                # Update connection metrics
                for conn in active_connections:
                    conn_id = f"{conn.usename}@{conn.client_addr or 'local'}"
                    
                    if conn_id not in self.connection_metrics:
                        self.connection_metrics[conn_id] = ConnectionMetrics(
                            database_name=conn.datname,
                            username=conn.usename,
                            application_name=conn.application_name or 'unknown',
                            client_addr=conn.client_addr or 'localhost',
                            connection_start=conn.backend_start,
                            state=conn.state
                        )
                    
                    metrics = self.connection_metrics[conn_id]
                    metrics.state = conn.state
                    metrics.waiting = conn.wait_event_type is not None
                    metrics.wait_event_type = conn.wait_event_type
                    metrics.wait_event = conn.wait_event
                    
                    if conn.state == 'active':
                        metrics.active_queries += 1
                        metrics.total_queries += 1
                
                # Get connection stats
                conn_stats = await session.execute(text("""
                    SELECT 
                        count(*) as total_connections,
                        count(*) FILTER (WHERE state = 'active') as active_connections,
                        count(*) FILTER (WHERE state = 'idle') as idle_connections,
                        count(*) FILTER (WHERE wait_event_type IS NOT NULL) as waiting_connections
                    FROM pg_stat_activity
                """))
                
                stats = conn_stats.fetchone()
                
                # Update Prometheus metrics
                enterprise_metrics.db_connections_active.labels(
                    database='pwc_data',
                    user='all'
                ).set(stats.active_connections)
                
                enterprise_metrics.db_connections_waiting.labels(
                    database='pwc_data'
                ).set(stats.waiting_connections)
                
                self.logger.debug(f"Connection metrics: {stats.total_connections} total, "
                                f"{stats.active_connections} active, {stats.waiting_connections} waiting")
                
        except Exception as e:
            self.logger.error(f"Failed to collect connection metrics: {e}")
    
    async def collect_table_metrics(self):
        """Collect table performance metrics."""
        try:
            async with get_async_session() as session:
                # Get table statistics
                result = await session.execute(text("""
                    SELECT 
                        schemaname,
                        tablename,
                        seq_scan,
                        seq_tup_read,
                        idx_scan,
                        idx_tup_fetch,
                        n_tup_ins,
                        n_tup_upd,
                        n_tup_del,
                        n_tup_hot_upd,
                        n_live_tup,
                        n_dead_tup,
                        vacuum_count,
                        autovacuum_count,
                        analyze_count,
                        autoanalyze_count,
                        last_vacuum,
                        last_autovacuum,
                        last_analyze,
                        last_autoanalyze
                    FROM pg_stat_user_tables
                    ORDER BY (seq_scan + idx_scan) DESC
                    LIMIT 100
                """))
                
                for row in result.fetchall():
                    table_key = f"{row.schemaname}.{row.tablename}"
                    
                    if table_key not in self.table_metrics:
                        self.table_metrics[table_key] = TableMetrics(
                            table_name=row.tablename,
                            schema_name=row.schemaname
                        )
                    
                    metrics = self.table_metrics[table_key]
                    metrics.seq_scans = row.seq_scan or 0
                    metrics.seq_tup_read = row.seq_tup_read or 0
                    metrics.idx_scans = row.idx_scan or 0
                    metrics.idx_tup_fetch = row.idx_tup_fetch or 0
                    metrics.n_tup_ins = row.n_tup_ins or 0
                    metrics.n_tup_upd = row.n_tup_upd or 0
                    metrics.n_tup_del = row.n_tup_del or 0
                    metrics.n_tup_hot_upd = row.n_tup_hot_upd or 0
                    metrics.n_live_tup = row.n_live_tup or 0
                    metrics.n_dead_tup = row.n_dead_tup or 0
                    metrics.vacuum_count = row.vacuum_count or 0
                    metrics.autovacuum_count = row.autovacuum_count or 0
                    metrics.analyze_count = row.analyze_count or 0
                    metrics.autoanalyze_count = row.autoanalyze_count or 0
                
                # Get table sizes
                size_result = await session.execute(text("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_total_relation_size(schemaname||'.'||tablename) as table_size,
                        pg_indexes_size(schemaname||'.'||tablename) as indexes_size
                    FROM pg_tables 
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                """))
                
                for row in size_result.fetchall():
                    table_key = f"{row.schemaname}.{row.tablename}"
                    if table_key in self.table_metrics:
                        self.table_metrics[table_key].table_size_bytes = row.table_size or 0
                        self.table_metrics[table_key].indexes_size_bytes = row.indexes_size or 0
                
        except Exception as e:
            self.logger.error(f"Failed to collect table metrics: {e}")
    
    async def collect_index_metrics(self):
        """Collect index performance metrics."""
        try:
            async with get_async_session() as session:
                result = await session.execute(text("""
                    SELECT 
                        schemaname,
                        tablename,
                        indexname,
                        idx_scan,
                        idx_tup_read,
                        idx_tup_fetch,
                        pg_relation_size(indexname) as index_size
                    FROM pg_stat_user_indexes
                    ORDER BY idx_scan DESC
                    LIMIT 200
                """))
                
                for row in result.fetchall():
                    index_key = f"{row.schemaname}.{row.indexname}"
                    
                    if index_key not in self.index_metrics:
                        self.index_metrics[index_key] = IndexMetrics(
                            index_name=row.indexname,
                            table_name=row.tablename,
                            schema_name=row.schemaname
                        )
                    
                    metrics = self.index_metrics[index_key]
                    metrics.index_scans = row.idx_scan or 0
                    metrics.tuples_read = row.idx_tup_read or 0
                    metrics.tuples_fetched = row.idx_tup_fetch or 0
                    metrics.size_bytes = row.index_size or 0
                    
                    # Calculate usage ratio
                    total_table_scans = 0
                    table_key = f"{row.schemaname}.{row.tablename}"
                    if table_key in self.table_metrics:
                        table_metric = self.table_metrics[table_key]
                        total_table_scans = table_metric.seq_scans + table_metric.idx_scans
                    
                    if total_table_scans > 0:
                        metrics.usage_ratio = (metrics.index_scans / total_table_scans) * 100
                
        except Exception as e:
            self.logger.error(f"Failed to collect index metrics: {e}")
    
    async def collect_database_health(self) -> DatabaseHealth:
        """Collect overall database health metrics."""
        health = DatabaseHealth()
        
        try:
            async with get_async_session() as session:
                # Connection statistics
                conn_result = await session.execute(text("""
                    SELECT 
                        setting::int as max_connections
                    FROM pg_settings 
                    WHERE name = 'max_connections'
                """))
                max_conn_row = conn_result.fetchone()
                health.max_connections = max_conn_row.max_connections if max_conn_row else 0
                
                activity_result = await session.execute(text("""
                    SELECT 
                        count(*) as total,
                        count(*) FILTER (WHERE state = 'active') as active,
                        count(*) FILTER (WHERE state = 'idle') as idle,
                        count(*) FILTER (WHERE wait_event_type IS NOT NULL) as waiting
                    FROM pg_stat_activity
                """))
                
                activity = activity_result.fetchone()
                if activity:
                    health.total_connections = activity.total
                    health.active_connections = activity.active
                    health.idle_connections = activity.idle
                    health.waiting_connections = activity.waiting
                    
                    if health.max_connections > 0:
                        health.connection_utilization = (health.total_connections / health.max_connections) * 100
                
                # Cache hit ratio
                cache_result = await session.execute(text("""
                    SELECT 
                        sum(heap_blks_hit) as hits,
                        sum(heap_blks_read) as reads
                    FROM pg_statio_user_tables
                """))
                
                cache = cache_result.fetchone()
                if cache and (cache.hits + cache.reads) > 0:
                    health.cache_hit_ratio = (cache.hits / (cache.hits + cache.reads)) * 100
                
                # Database size
                size_result = await session.execute(text("""
                    SELECT pg_database_size(current_database()) as db_size
                """))
                size_row = size_result.fetchone()
                health.database_size_bytes = size_row.db_size if size_row else 0
                
                # Temporary files
                temp_result = await session.execute(text("""
                    SELECT 
                        sum(temp_files) as temp_files_count,
                        sum(temp_bytes) as temp_bytes
                    FROM pg_stat_database 
                    WHERE datname = current_database()
                """))
                
                temp = temp_result.fetchone()
                if temp:
                    health.temp_files_count = temp.temp_files_count or 0
                    health.temp_bytes = temp.temp_bytes or 0
                
                # Calculate query rates from recent history
                if len(self.query_history) > 0:
                    recent_queries = [q for q in self.query_history 
                                    if (datetime.utcnow() - q['timestamp']).seconds < 60]
                    
                    health.total_queries_per_second = len(recent_queries) / 60.0
                    slow_queries = [q for q in recent_queries 
                                  if q['execution_time_ms'] > self.slow_query_threshold_ms]
                    health.slow_queries_per_second = len(slow_queries) / 60.0
                    
                    error_queries = [q for q in recent_queries if q['error']]
                    if len(recent_queries) > 0:
                        health.error_rate = (len(error_queries) / len(recent_queries)) * 100
                
                # Store health history
                self.health_history.append(health)
                
                self.logger.debug(f"Database health - Connections: {health.total_connections}/{health.max_connections}, "
                                f"Cache hit ratio: {health.cache_hit_ratio:.1f}%, "
                                f"QPS: {health.total_queries_per_second:.1f}")
                
        except Exception as e:
            self.logger.error(f"Failed to collect database health metrics: {e}")
        
        return health
    
    async def detect_performance_issues(self) -> List[Dict[str, Any]]:
        """Detect database performance issues and generate alerts."""
        issues = []
        
        try:
            # Check connection utilization
            if len(self.health_history) > 0:
                latest_health = self.health_history[-1]
                
                if latest_health.connection_utilization > self.alert_thresholds["connection_utilization"]:
                    issues.append({
                        "type": "high_connection_utilization",
                        "severity": "warning",
                        "message": f"Connection utilization at {latest_health.connection_utilization:.1f}%",
                        "value": latest_health.connection_utilization,
                        "threshold": self.alert_thresholds["connection_utilization"],
                        "recommendation": "Consider connection pooling optimization or scaling database"
                    })
                
                if latest_health.cache_hit_ratio < self.alert_thresholds["cache_hit_ratio"]:
                    issues.append({
                        "type": "low_cache_hit_ratio",
                        "severity": "warning",
                        "message": f"Cache hit ratio at {latest_health.cache_hit_ratio:.1f}%",
                        "value": latest_health.cache_hit_ratio,
                        "threshold": self.alert_thresholds["cache_hit_ratio"],
                        "recommendation": "Increase shared_buffers or optimize queries"
                    })
                
                if latest_health.slow_queries_per_second > self.alert_thresholds["slow_query_rate"]:
                    issues.append({
                        "type": "high_slow_query_rate",
                        "severity": "critical",
                        "message": f"Slow query rate at {latest_health.slow_queries_per_second:.1f} queries/second",
                        "value": latest_health.slow_queries_per_second,
                        "threshold": self.alert_thresholds["slow_query_rate"],
                        "recommendation": "Analyze and optimize slow queries, check for missing indexes"
                    })
                
                if latest_health.error_rate > self.alert_thresholds["error_rate"]:
                    issues.append({
                        "type": "high_error_rate",
                        "severity": "critical",
                        "message": f"Database error rate at {latest_health.error_rate:.1f}%",
                        "value": latest_health.error_rate,
                        "threshold": self.alert_thresholds["error_rate"],
                        "recommendation": "Investigate application errors and query failures"
                    })
            
            # Check for unused indexes
            unused_indexes = []
            for index_key, metrics in self.index_metrics.items():
                if metrics.index_scans == 0 and not metrics.is_primary:
                    unused_indexes.append(index_key)
            
            if len(unused_indexes) > 5:
                issues.append({
                    "type": "unused_indexes",
                    "severity": "info",
                    "message": f"Found {len(unused_indexes)} unused indexes",
                    "count": len(unused_indexes),
                    "indexes": unused_indexes[:10],  # First 10
                    "recommendation": "Consider dropping unused indexes to improve write performance"
                })
            
            # Check for tables with high sequential scan ratio
            high_seq_scan_tables = []
            for table_key, metrics in self.table_metrics.items():
                total_scans = metrics.seq_scans + metrics.idx_scans
                if total_scans > 1000:  # Only check tables with significant activity
                    seq_ratio = (metrics.seq_scans / total_scans) * 100 if total_scans > 0 else 0
                    if seq_ratio > 80:  # More than 80% sequential scans
                        high_seq_scan_tables.append({
                            "table": table_key,
                            "seq_ratio": seq_ratio,
                            "seq_scans": metrics.seq_scans,
                            "total_scans": total_scans
                        })
            
            if high_seq_scan_tables:
                issues.append({
                    "type": "high_sequential_scan_ratio",
                    "severity": "warning",
                    "message": f"Found {len(high_seq_scan_tables)} tables with high sequential scan ratios",
                    "tables": high_seq_scan_tables[:5],  # First 5
                    "recommendation": "Consider adding indexes or optimizing WHERE clauses"
                })
            
            # Check for tables needing vacuum
            tables_needing_vacuum = []
            for table_key, metrics in self.table_metrics.items():
                if metrics.n_live_tup > 0:
                    dead_ratio = (metrics.n_dead_tup / (metrics.n_live_tup + metrics.n_dead_tup)) * 100
                    if dead_ratio > 20:  # More than 20% dead tuples
                        tables_needing_vacuum.append({
                            "table": table_key,
                            "dead_ratio": dead_ratio,
                            "dead_tuples": metrics.n_dead_tup,
                            "live_tuples": metrics.n_live_tup
                        })
            
            if tables_needing_vacuum:
                issues.append({
                    "type": "tables_need_vacuum",
                    "severity": "warning",
                    "message": f"Found {len(tables_needing_vacuum)} tables needing vacuum",
                    "tables": tables_needing_vacuum[:5],
                    "recommendation": "Run VACUUM or increase autovacuum frequency"
                })
            
        except Exception as e:
            self.logger.error(f"Failed to detect performance issues: {e}")
        
        return issues
    
    async def get_top_slow_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the top slow queries by average execution time."""
        slow_queries = [
            {
                "query_id": metrics.query_id,
                "query_text": metrics.query_text,
                "query_type": metrics.query_type.value,
                "complexity": metrics.complexity.value,
                "avg_time_ms": metrics.avg_time_ms,
                "max_time_ms": metrics.max_time_ms,
                "execution_count": metrics.execution_count,
                "total_time_ms": metrics.total_time_ms,
                "tables_accessed": list(metrics.tables_accessed)
            }
            for metrics in self.query_metrics.values()
            if metrics.avg_time_ms > self.slow_query_threshold_ms
        ]
        
        return sorted(slow_queries, key=lambda x: x["avg_time_ms"], reverse=True)[:limit]
    
    async def get_query_recommendations(self) -> List[Dict[str, Any]]:
        """Generate query optimization recommendations."""
        recommendations = []
        
        try:
            # Analyze query patterns
            for query_id, metrics in self.query_metrics.items():
                if metrics.avg_time_ms > self.slow_query_threshold_ms:
                    rec = {
                        "query_id": query_id,
                        "query_text": metrics.query_text,
                        "issue": "slow_execution",
                        "avg_time_ms": metrics.avg_time_ms,
                        "recommendations": []
                    }
                    
                    # Add specific recommendations based on query analysis
                    if metrics.complexity == QueryComplexity.VERY_COMPLEX:
                        rec["recommendations"].append(
                            "Consider breaking down complex query into simpler parts"
                        )
                    
                    if "ORDER BY" in metrics.query_text.upper() and "LIMIT" not in metrics.query_text.upper():
                        rec["recommendations"].append(
                            "Add LIMIT clause to ORDER BY queries to improve performance"
                        )
                    
                    if len(metrics.tables_accessed) > 3:
                        rec["recommendations"].append(
                            "Query accesses many tables - consider denormalization or materialized views"
                        )
                    
                    recommendations.append(rec)
            
            # Check for missing indexes based on table access patterns
            for table_key, table_metrics in self.table_metrics.items():
                if table_metrics.seq_scans > table_metrics.idx_scans * 2:  # Much more seq scans than index scans
                    recommendations.append({
                        "table": table_key,
                        "issue": "high_sequential_scans",
                        "seq_scans": table_metrics.seq_scans,
                        "idx_scans": table_metrics.idx_scans,
                        "recommendations": [
                            f"Consider adding indexes to {table_key} to reduce sequential scans",
                            "Analyze query patterns to identify frequently filtered columns"
                        ]
                    })
            
        except Exception as e:
            self.logger.error(f"Failed to generate query recommendations: {e}")
        
        return recommendations
    
    async def start_monitoring(self, interval_seconds: int = 60):
        """Start continuous database performance monitoring."""
        self.monitoring_active = True
        self.logger.info(f"Starting database performance monitoring with {interval_seconds}s interval")
        
        while self.monitoring_active:
            try:
                start_time = time.time()
                
                # Collect all metrics
                await asyncio.gather(
                    self.collect_connection_metrics(),
                    self.collect_table_metrics(),
                    self.collect_index_metrics(),
                    return_exceptions=True
                )
                
                # Collect overall health
                health = await self.collect_database_health()
                
                # Detect issues
                issues = await self.detect_performance_issues()
                if issues:
                    self.logger.warning(f"Detected {len(issues)} database performance issues")
                    for issue in issues:
                        self.logger.warning(f"Issue: {issue['message']}")
                
                self.last_monitoring_run = datetime.utcnow()
                collection_time = time.time() - start_time
                
                self.logger.debug(f"Database monitoring cycle completed in {collection_time:.2f}s")
                
                # Wait for next interval
                await asyncio.sleep(max(0, interval_seconds - collection_time))
                
            except Exception as e:
                self.logger.error(f"Error in database monitoring loop: {e}")
                await asyncio.sleep(interval_seconds)
    
    def stop_monitoring(self):
        """Stop database performance monitoring."""
        self.monitoring_active = False
        self.logger.info("Database performance monitoring stopped")
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get summary of database monitoring status and metrics."""
        return {
            "monitoring_active": self.monitoring_active,
            "last_run": self.last_monitoring_run.isoformat(),
            "tracked_queries": len(self.query_metrics),
            "tracked_connections": len(self.connection_metrics),
            "tracked_tables": len(self.table_metrics),
            "tracked_indexes": len(self.index_metrics),
            "health_history_size": len(self.health_history),
            "query_history_size": len(self.query_history),
            "slow_query_threshold_ms": self.slow_query_threshold_ms,
            "alert_thresholds": self.alert_thresholds
        }


# Global database monitor instance
db_monitor = DatabasePerformanceMonitor()


# Instrumentation decorator for automatic query tracking
def track_database_query(operation: str = None):
    """Decorator to automatically track database query performance."""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            error = None
            
            try:
                result = await func(*args, **kwargs)
                return result
                
            except Exception as e:
                error = str(e)
                raise
                
            finally:
                execution_time_ms = (time.time() - start_time) * 1000
                
                # Extract query info if available
                query_text = operation or func.__name__
                if hasattr(func, '__name__') and 'query' in kwargs:
                    query_text = str(kwargs['query'])[:1000]
                
                await db_monitor.record_query_execution(
                    query_text=query_text,
                    execution_time_ms=execution_time_ms,
                    error=error
                )
        
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            error = None
            
            try:
                result = func(*args, **kwargs)
                return result
                
            except Exception as e:
                error = str(e)
                raise
                
            finally:
                execution_time_ms = (time.time() - start_time) * 1000
                query_text = operation or func.__name__
                
                # Run async record in event loop
                try:
                    loop = asyncio.get_event_loop()
                    loop.create_task(db_monitor.record_query_execution(
                        query_text=query_text,
                        execution_time_ms=execution_time_ms,
                        error=error
                    ))
                except RuntimeError:
                    # No event loop running
                    pass
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


__all__ = [
    'DatabasePerformanceMonitor',
    'db_monitor',
    'track_database_query',
    'QueryType',
    'QueryComplexity',
    'QueryMetrics',
    'ConnectionMetrics',
    'TableMetrics',
    'IndexMetrics',
    'DatabaseHealth'
]