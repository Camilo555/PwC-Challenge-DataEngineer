"""
Comprehensive Database Optimization Framework for PwC Challenge DataEngineer Platform
=====================================================================================

Enterprise-grade database optimization framework targeting <25ms query performance:
- Advanced connection pooling with intelligent load balancing
- Query result caching with smart invalidation strategies
- Automated database maintenance and statistics updates
- Performance regression detection and alerting
- Real-time query optimization recommendations
- Database health monitoring and capacity planning

Features:
- Sub-25ms query performance targets
- Medallion architecture optimization (Bronze→Silver→Gold)
- High-throughput ETL processing optimization
- Real-time analytics performance tuning
- OLTP and OLAP workload balancing
- Automated index maintenance and optimization
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from uuid import UUID, uuid4
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as redis
from sqlalchemy import Engine, text, inspect, MetaData, event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import QueuePool, NullPool, StaticPool
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.events import PoolEvents
from sqlalchemy.sql import func

from core.logging import get_logger
from .query_performance_monitor import QueryPerformanceMonitor, QueryMetrics
from .query_optimizer_service import QueryOptimizerService
from .advanced_indexing_strategy import AdvancedIndexingStrategy

logger = get_logger(__name__)


class OptimizationLevel(str, Enum):
    """Database optimization levels."""
    CONSERVATIVE = "conservative"   # Minimal changes, high safety
    BALANCED = "balanced"          # Balanced performance and safety
    AGGRESSIVE = "aggressive"      # Maximum performance, higher risk
    ULTRA = "ultra"               # Experimental optimizations


class CacheStrategy(str, Enum):
    """Query result caching strategies."""
    LRU = "lru"                   # Least Recently Used
    LFU = "lfu"                   # Least Frequently Used
    TTL = "ttl"                   # Time To Live
    ADAPTIVE = "adaptive"         # Adaptive based on query patterns
    WRITE_THROUGH = "write_through"  # Write-through caching
    WRITE_BEHIND = "write_behind"    # Write-behind caching


class MaintenanceType(str, Enum):
    """Database maintenance operations."""
    ANALYZE_TABLES = "analyze_tables"
    REBUILD_INDEXES = "rebuild_indexes"
    UPDATE_STATISTICS = "update_statistics"
    VACUUM_TABLES = "vacuum_tables"
    REINDEX_FRAGMENTED = "reindex_fragmented"
    CLEANUP_LOGS = "cleanup_logs"


@dataclass
class ConnectionPoolMetrics:
    """Connection pool performance metrics."""
    pool_size: int
    checked_out: int
    overflow: int
    checked_in: int
    pool_hits: int = 0
    pool_misses: int = 0
    connection_failures: int = 0
    avg_checkout_time_ms: float = 0.0
    max_checkout_time_ms: float = 0.0
    pool_efficiency: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def calculate_efficiency(self) -> float:
        """Calculate pool efficiency percentage."""
        total_requests = self.pool_hits + self.pool_misses
        if total_requests == 0:
            return 100.0
        return (self.pool_hits / total_requests) * 100.0


@dataclass
class CacheMetrics:
    """Query cache performance metrics."""
    cache_hits: int = 0
    cache_misses: int = 0
    cache_evictions: int = 0
    cache_size_mb: float = 0.0
    hit_ratio: float = 0.0
    avg_hit_time_ms: float = 0.0
    avg_miss_time_ms: float = 0.0
    total_entries: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def calculate_hit_ratio(self) -> float:
        """Calculate cache hit ratio percentage."""
        total_requests = self.cache_hits + self.cache_misses
        if total_requests == 0:
            return 0.0
        return (self.cache_hits / total_requests) * 100.0


@dataclass
class DatabaseHealth:
    """Database health metrics and status."""
    cpu_usage_pct: float = 0.0
    memory_usage_pct: float = 0.0
    disk_usage_pct: float = 0.0
    connection_count: int = 0
    max_connections: int = 0
    active_queries: int = 0
    slow_queries_count: int = 0
    lock_waits: int = 0
    deadlocks: int = 0
    buffer_hit_ratio: float = 0.0
    checkpoint_frequency: float = 0.0
    wal_growth_rate_mb_per_min: float = 0.0
    table_bloat_pct: float = 0.0
    index_bloat_pct: float = 0.0
    health_score: float = 100.0
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def calculate_health_score(self) -> float:
        """Calculate overall database health score."""
        score = 100.0

        # CPU usage penalty
        if self.cpu_usage_pct > 80:
            score -= 20
        elif self.cpu_usage_pct > 60:
            score -= 10

        # Memory usage penalty
        if self.memory_usage_pct > 90:
            score -= 25
        elif self.memory_usage_pct > 75:
            score -= 10

        # Connection usage penalty
        connection_usage = (self.connection_count / max(self.max_connections, 1)) * 100
        if connection_usage > 90:
            score -= 20
        elif connection_usage > 75:
            score -= 10

        # Buffer hit ratio bonus/penalty
        if self.buffer_hit_ratio > 95:
            score += 5
        elif self.buffer_hit_ratio < 80:
            score -= 15

        # Bloat penalties
        if self.table_bloat_pct > 30:
            score -= 15
        if self.index_bloat_pct > 30:
            score -= 10

        self.health_score = max(0.0, score)
        return self.health_score


class AdvancedConnectionPool:
    """Advanced connection pool with intelligent optimization."""

    def __init__(self,
                 engine: AsyncEngine,
                 pool_size: int = 20,
                 max_overflow: int = 10,
                 pool_recycle: int = 3600,
                 pool_pre_ping: bool = True):
        self.engine = engine
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_recycle = pool_recycle
        self.pool_pre_ping = pool_pre_ping

        # Metrics tracking
        self.metrics = ConnectionPoolMetrics(pool_size=pool_size, checked_out=0, overflow=0, checked_in=0)
        self.checkout_times: deque = deque(maxlen=1000)
        self.connection_events: List[Dict[str, Any]] = []

        # Optimization parameters
        self.auto_scale_enabled = True
        self.min_pool_size = 5
        self.max_pool_size = 50
        self.scale_threshold = 0.8  # Scale when 80% utilized

    async def monitor_pool_health(self):
        """Monitor connection pool health and performance."""
        try:
            # Get pool statistics
            pool = self.engine.pool

            self.metrics.checked_out = pool.checkedout()
            self.metrics.overflow = pool.overflow()
            self.metrics.checked_in = pool.checkedin()
            self.metrics.pool_efficiency = self.metrics.calculate_efficiency()

            # Calculate average checkout time
            if self.checkout_times:
                self.metrics.avg_checkout_time_ms = statistics.mean(self.checkout_times)
                self.metrics.max_checkout_time_ms = max(self.checkout_times)

            # Auto-scaling logic
            if self.auto_scale_enabled:
                await self._auto_scale_pool()

            logger.debug(f"Pool metrics: {self.metrics.checked_out}/{self.pool_size} connections in use")

        except Exception as e:
            logger.error(f"Failed to monitor pool health: {e}")

    async def _auto_scale_pool(self):
        """Automatically scale connection pool based on usage patterns."""
        try:
            utilization = self.metrics.checked_out / self.pool_size

            # Scale up if utilization is high
            if (utilization > self.scale_threshold and
                self.pool_size < self.max_pool_size and
                self.metrics.avg_checkout_time_ms > 50):  # High checkout time

                new_size = min(self.pool_size + 5, self.max_pool_size)
                logger.info(f"Scaling up connection pool from {self.pool_size} to {new_size}")
                self.pool_size = new_size

            # Scale down if utilization is consistently low
            elif (utilization < 0.3 and
                  self.pool_size > self.min_pool_size and
                  self.metrics.avg_checkout_time_ms < 10):  # Low checkout time

                new_size = max(self.pool_size - 2, self.min_pool_size)
                logger.info(f"Scaling down connection pool from {self.pool_size} to {new_size}")
                self.pool_size = new_size

        except Exception as e:
            logger.error(f"Failed to auto-scale pool: {e}")

    def record_checkout_time(self, checkout_time_ms: float):
        """Record connection checkout time for metrics."""
        self.checkout_times.append(checkout_time_ms)

    def get_pool_metrics(self) -> ConnectionPoolMetrics:
        """Get current pool metrics."""
        return self.metrics


class IntelligentQueryCache:
    """Intelligent query result caching with adaptive strategies."""

    def __init__(self,
                 redis_client: Optional[redis.Redis] = None,
                 strategy: CacheStrategy = CacheStrategy.ADAPTIVE,
                 max_cache_size_mb: int = 512,
                 default_ttl_seconds: int = 300):
        self.redis_client = redis_client
        self.strategy = strategy
        self.max_cache_size_mb = max_cache_size_mb
        self.default_ttl_seconds = default_ttl_seconds

        # Local cache fallback
        self.local_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_access_times: Dict[str, datetime] = {}
        self.cache_hit_counts: Dict[str, int] = defaultdict(int)

        # Metrics
        self.metrics = CacheMetrics()

        # Cache invalidation patterns
        self.invalidation_patterns: Dict[str, List[str]] = {
            'sales_transactions': ['fact_sale*', 'dim_customer*', 'sales_*'],
            'dim_tables': ['dim_*', 'fact_*'],
            'analytics': ['dashboard_*', 'report_*', 'kpi_*']
        }

    async def get_cached_result(self, query_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached query result if available."""
        try:
            start_time = time.time()

            # Try Redis first if available
            if self.redis_client:
                cached_data = await self.redis_client.get(f"query_cache:{query_hash}")
                if cached_data:
                    result = json.loads(cached_data)
                    self.metrics.cache_hits += 1
                    self.metrics.avg_hit_time_ms = (time.time() - start_time) * 1000
                    self.cache_hit_counts[query_hash] += 1
                    return result

            # Fallback to local cache
            if query_hash in self.local_cache:
                cached_entry = self.local_cache[query_hash]

                # Check TTL
                if datetime.utcnow() < cached_entry['expires_at']:
                    self.metrics.cache_hits += 1
                    self.metrics.avg_hit_time_ms = (time.time() - start_time) * 1000
                    self.cache_hit_counts[query_hash] += 1
                    self.cache_access_times[query_hash] = datetime.utcnow()
                    return cached_entry['data']
                else:
                    # Expired, remove from cache
                    del self.local_cache[query_hash]
                    if query_hash in self.cache_access_times:
                        del self.cache_access_times[query_hash]

            self.metrics.cache_misses += 1
            self.metrics.avg_miss_time_ms = (time.time() - start_time) * 1000
            return None

        except Exception as e:
            logger.error(f"Cache retrieval error: {e}")
            self.metrics.cache_misses += 1
            return None

    async def cache_query_result(self,
                                query_hash: str,
                                result: Dict[str, Any],
                                ttl_seconds: Optional[int] = None) -> bool:
        """Cache query result with intelligent TTL."""
        try:
            ttl = ttl_seconds or self._calculate_adaptive_ttl(query_hash, result)

            # Cache in Redis if available
            if self.redis_client:
                cache_data = json.dumps(result, default=str)
                await self.redis_client.setex(
                    f"query_cache:{query_hash}",
                    ttl,
                    cache_data
                )

            # Cache locally as fallback
            self.local_cache[query_hash] = {
                'data': result,
                'cached_at': datetime.utcnow(),
                'expires_at': datetime.utcnow() + timedelta(seconds=ttl),
                'access_count': 1,
                'size_bytes': len(json.dumps(result, default=str))
            }
            self.cache_access_times[query_hash] = datetime.utcnow()

            # Manage cache size
            await self._manage_cache_size()

            return True

        except Exception as e:
            logger.error(f"Cache storage error: {e}")
            return False

    def _calculate_adaptive_ttl(self, query_hash: str, result: Dict[str, Any]) -> int:
        """Calculate adaptive TTL based on query patterns and result characteristics."""
        base_ttl = self.default_ttl_seconds

        # Adjust based on result size (larger results cached longer)
        result_size = len(json.dumps(result, default=str))
        if result_size > 100000:  # > 100KB
            base_ttl *= 2
        elif result_size < 1000:  # < 1KB
            base_ttl = max(60, base_ttl // 2)

        # Adjust based on access frequency
        access_count = self.cache_hit_counts.get(query_hash, 0)
        if access_count > 10:
            base_ttl *= 1.5
        elif access_count > 50:
            base_ttl *= 2

        # Adjust based on query type (analytical queries cached longer)
        if 'aggregate' in str(result) or 'group_by' in query_hash:
            base_ttl *= 1.5

        return int(min(base_ttl, 3600))  # Max 1 hour

    async def _manage_cache_size(self):
        """Manage local cache size using LRU eviction."""
        try:
            # Calculate current cache size
            total_size = sum(entry['size_bytes'] for entry in self.local_cache.values())
            max_size_bytes = self.max_cache_size_mb * 1024 * 1024

            if total_size > max_size_bytes:
                # Sort by access time (LRU)
                sorted_keys = sorted(
                    self.cache_access_times.keys(),
                    key=lambda k: self.cache_access_times[k]
                )

                # Remove oldest entries until under limit
                for key in sorted_keys:
                    if total_size <= max_size_bytes:
                        break

                    if key in self.local_cache:
                        total_size -= self.local_cache[key]['size_bytes']
                        del self.local_cache[key]
                        del self.cache_access_times[key]
                        self.metrics.cache_evictions += 1

        except Exception as e:
            logger.error(f"Cache size management error: {e}")

    async def invalidate_cache_pattern(self, pattern: str):
        """Invalidate cache entries matching a pattern."""
        try:
            # Invalidate Redis cache
            if self.redis_client:
                keys = await self.redis_client.keys(f"query_cache:*{pattern}*")
                if keys:
                    await self.redis_client.delete(*keys)

            # Invalidate local cache
            keys_to_remove = [key for key in self.local_cache.keys() if pattern in key]
            for key in keys_to_remove:
                del self.local_cache[key]
                if key in self.cache_access_times:
                    del self.cache_access_times[key]

            logger.info(f"Invalidated cache entries matching pattern: {pattern}")

        except Exception as e:
            logger.error(f"Cache invalidation error: {e}")

    def get_cache_metrics(self) -> CacheMetrics:
        """Get current cache metrics."""
        self.metrics.hit_ratio = self.metrics.calculate_hit_ratio()
        self.metrics.total_entries = len(self.local_cache)
        self.metrics.cache_size_mb = sum(
            entry['size_bytes'] for entry in self.local_cache.values()
        ) / (1024 * 1024)
        return self.metrics


class DatabaseMaintenanceManager:
    """Automated database maintenance and optimization."""

    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.maintenance_schedule: Dict[MaintenanceType, Dict[str, Any]] = {}
        self.last_maintenance: Dict[MaintenanceType, datetime] = {}
        self.maintenance_running = False

        # Default maintenance intervals (in hours)
        self.default_intervals = {
            MaintenanceType.ANALYZE_TABLES: 6,
            MaintenanceType.UPDATE_STATISTICS: 12,
            MaintenanceType.VACUUM_TABLES: 24,
            MaintenanceType.REBUILD_INDEXES: 168,  # Weekly
            MaintenanceType.REINDEX_FRAGMENTED: 72,
            MaintenanceType.CLEANUP_LOGS: 24
        }

    async def schedule_maintenance(self,
                                  maintenance_type: MaintenanceType,
                                  interval_hours: int,
                                  maintenance_window: Tuple[int, int] = (2, 6)):
        """Schedule automated maintenance task."""
        self.maintenance_schedule[maintenance_type] = {
            'interval_hours': interval_hours,
            'maintenance_window': maintenance_window,
            'enabled': True
        }

    async def run_maintenance_cycle(self):
        """Run automated maintenance cycle."""
        if self.maintenance_running:
            logger.warning("Maintenance cycle already running")
            return

        self.maintenance_running = True
        current_hour = datetime.utcnow().hour

        try:
            for maintenance_type, config in self.maintenance_schedule.items():
                if not config['enabled']:
                    continue

                # Check if within maintenance window
                window_start, window_end = config['maintenance_window']
                if not (window_start <= current_hour <= window_end):
                    continue

                # Check if maintenance is due
                last_run = self.last_maintenance.get(maintenance_type)
                if last_run:
                    hours_since = (datetime.utcnow() - last_run).total_seconds() / 3600
                    if hours_since < config['interval_hours']:
                        continue

                # Run maintenance
                logger.info(f"Running maintenance: {maintenance_type.value}")
                await self._execute_maintenance(maintenance_type)
                self.last_maintenance[maintenance_type] = datetime.utcnow()

        except Exception as e:
            logger.error(f"Maintenance cycle error: {e}")
        finally:
            self.maintenance_running = False

    async def _execute_maintenance(self, maintenance_type: MaintenanceType):
        """Execute specific maintenance operation."""
        try:
            async with self.engine.begin() as conn:
                if maintenance_type == MaintenanceType.ANALYZE_TABLES:
                    await self._analyze_tables(conn)
                elif maintenance_type == MaintenanceType.UPDATE_STATISTICS:
                    await self._update_statistics(conn)
                elif maintenance_type == MaintenanceType.VACUUM_TABLES:
                    await self._vacuum_tables(conn)
                elif maintenance_type == MaintenanceType.REBUILD_INDEXES:
                    await self._rebuild_indexes(conn)
                elif maintenance_type == MaintenanceType.REINDEX_FRAGMENTED:
                    await self._reindex_fragmented(conn)
                elif maintenance_type == MaintenanceType.CLEANUP_LOGS:
                    await self._cleanup_logs(conn)

        except Exception as e:
            logger.error(f"Maintenance execution error for {maintenance_type.value}: {e}")

    async def _analyze_tables(self, conn):
        """Analyze all tables to update optimizer statistics."""
        tables = ['fact_sale', 'dim_customer', 'dim_product', 'dim_date', 'dim_country', 'dim_invoice']
        for table in tables:
            await conn.execute(text(f"ANALYZE {table}"))
        logger.info(f"Analyzed {len(tables)} tables")

    async def _update_statistics(self, conn):
        """Update table and index statistics."""
        await conn.execute(text("ANALYZE"))
        logger.info("Updated database statistics")

    async def _vacuum_tables(self, conn):
        """Vacuum tables to reclaim space and update statistics."""
        tables = ['fact_sale', 'dim_customer', 'dim_product', 'sales_transactions']
        for table in tables:
            await conn.execute(text(f"VACUUM ANALYZE {table}"))
        logger.info(f"Vacuumed {len(tables)} tables")

    async def _rebuild_indexes(self, conn):
        """Rebuild fragmented indexes."""
        # Get fragmented indexes
        fragmented_query = text("""
            SELECT schemaname, tablename, indexname
            FROM pg_stat_user_indexes
            WHERE idx_scan < 10 AND pg_relation_size(indexrelid) > 1048576
        """)

        result = await conn.execute(fragmented_query)
        indexes = result.fetchall()

        for row in indexes:
            try:
                await conn.execute(text(f"REINDEX INDEX CONCURRENTLY {row.indexname}"))
                logger.info(f"Rebuilt index: {row.indexname}")
            except Exception as e:
                logger.warning(f"Failed to rebuild index {row.indexname}: {e}")

    async def _reindex_fragmented(self, conn):
        """Reindex highly fragmented indexes."""
        # This would implement logic to detect and rebuild fragmented indexes
        pass

    async def _cleanup_logs(self, conn):
        """Cleanup old log entries and temporary data."""
        # Cleanup query performance logs older than 30 days
        cleanup_query = text("""
            DELETE FROM query_performance_metrics
            WHERE executed_at < NOW() - INTERVAL '30 days'
        """)
        await conn.execute(cleanup_query)
        logger.info("Cleaned up old performance logs")


class DatabaseOptimizationFramework:
    """
    Comprehensive database optimization framework integrating all components.

    Provides enterprise-grade database performance optimization with:
    - Advanced connection pooling
    - Intelligent query caching
    - Automated maintenance
    - Performance monitoring
    - Query optimization
    - Index management
    """

    def __init__(self,
                 engine: AsyncEngine,
                 redis_client: Optional[redis.Redis] = None,
                 optimization_level: OptimizationLevel = OptimizationLevel.BALANCED,
                 target_performance_ms: float = 25.0):
        self.engine = engine
        self.optimization_level = optimization_level
        self.target_performance_ms = target_performance_ms

        # Core components
        self.connection_pool = AdvancedConnectionPool(engine)
        self.query_cache = IntelligentQueryCache(redis_client)
        self.maintenance_manager = DatabaseMaintenanceManager(engine)

        # Performance monitoring components
        self.performance_monitor: Optional[QueryPerformanceMonitor] = None
        self.query_optimizer: Optional[QueryOptimizerService] = None
        self.indexing_strategy: Optional[AdvancedIndexingStrategy] = None

        # Framework state
        self.optimization_active = False
        self.background_tasks: List[asyncio.Task] = []

        # Health monitoring
        self.database_health = DatabaseHealth()
        self.health_check_interval = 60  # seconds

        # Performance baselines
        self.performance_baselines: Dict[str, float] = {}
        self.regression_threshold_pct = 20.0

    async def initialize_framework(self):
        """Initialize the complete optimization framework."""
        try:
            logger.info("Initializing Database Optimization Framework...")

            # Initialize performance monitoring
            self.performance_monitor = QueryPerformanceMonitor(self.engine, self.target_performance_ms)
            await self.performance_monitor.start_monitoring()

            # Initialize query optimizer
            self.query_optimizer = QueryOptimizerService(self.engine, self.performance_monitor, self.target_performance_ms)
            await self.query_optimizer.start_optimization_service()

            # Initialize indexing strategy
            self.indexing_strategy = AdvancedIndexingStrategy(self.engine, self.target_performance_ms)
            await self.indexing_strategy.initialize_strategy()

            # Setup maintenance schedule based on optimization level
            await self._setup_maintenance_schedule()

            # Start background optimization tasks
            await self._start_background_tasks()

            self.optimization_active = True
            logger.info("Database Optimization Framework initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize optimization framework: {e}")
            raise

    async def _setup_maintenance_schedule(self):
        """Setup maintenance schedule based on optimization level."""
        if self.optimization_level == OptimizationLevel.CONSERVATIVE:
            intervals = {
                MaintenanceType.ANALYZE_TABLES: 12,
                MaintenanceType.UPDATE_STATISTICS: 24,
                MaintenanceType.VACUUM_TABLES: 48,
                MaintenanceType.REBUILD_INDEXES: 168
            }
        elif self.optimization_level == OptimizationLevel.BALANCED:
            intervals = {
                MaintenanceType.ANALYZE_TABLES: 6,
                MaintenanceType.UPDATE_STATISTICS: 12,
                MaintenanceType.VACUUM_TABLES: 24,
                MaintenanceType.REBUILD_INDEXES: 168,
                MaintenanceType.REINDEX_FRAGMENTED: 72
            }
        elif self.optimization_level == OptimizationLevel.AGGRESSIVE:
            intervals = {
                MaintenanceType.ANALYZE_TABLES: 3,
                MaintenanceType.UPDATE_STATISTICS: 6,
                MaintenanceType.VACUUM_TABLES: 12,
                MaintenanceType.REBUILD_INDEXES: 72,
                MaintenanceType.REINDEX_FRAGMENTED: 24
            }
        else:  # ULTRA
            intervals = {
                MaintenanceType.ANALYZE_TABLES: 1,
                MaintenanceType.UPDATE_STATISTICS: 3,
                MaintenanceType.VACUUM_TABLES: 6,
                MaintenanceType.REBUILD_INDEXES: 24,
                MaintenanceType.REINDEX_FRAGMENTED: 12
            }

        for maintenance_type, interval in intervals.items():
            await self.maintenance_manager.schedule_maintenance(maintenance_type, interval)

    async def _start_background_tasks(self):
        """Start background optimization tasks."""
        self.background_tasks = [
            asyncio.create_task(self._health_monitoring_loop()),
            asyncio.create_task(self._maintenance_loop()),
            asyncio.create_task(self._performance_optimization_loop()),
            asyncio.create_task(self._cache_management_loop())
        ]

    async def _health_monitoring_loop(self):
        """Background health monitoring loop."""
        while self.optimization_active:
            try:
                await self._collect_database_health_metrics()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitoring error: {e}")
                await asyncio.sleep(60)

    async def _maintenance_loop(self):
        """Background maintenance loop."""
        while self.optimization_active:
            try:
                await self.maintenance_manager.run_maintenance_cycle()
                await asyncio.sleep(3600)  # Check every hour
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Maintenance loop error: {e}")
                await asyncio.sleep(3600)

    async def _performance_optimization_loop(self):
        """Background performance optimization loop."""
        while self.optimization_active:
            try:
                # Check for performance regressions
                await self._check_performance_regressions()

                # Optimize based on recent query patterns
                if self.query_optimizer:
                    await self._optimize_recent_queries()

                await asyncio.sleep(1800)  # Every 30 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance optimization error: {e}")
                await asyncio.sleep(1800)

    async def _cache_management_loop(self):
        """Background cache management loop."""
        while self.optimization_active:
            try:
                # Update cache metrics
                cache_metrics = self.query_cache.get_cache_metrics()

                # Implement cache warming for frequently accessed queries
                await self._warm_query_cache()

                await asyncio.sleep(600)  # Every 10 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cache management error: {e}")
                await asyncio.sleep(600)

    async def _collect_database_health_metrics(self):
        """Collect comprehensive database health metrics."""
        try:
            async with self.engine.begin() as conn:
                # Get database statistics
                stats_query = text("""
                    SELECT
                        (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_queries,
                        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
                        (SELECT count(*) FROM pg_stat_activity) as connection_count,
                        (SELECT ROUND(100.0 * blks_hit / (blks_hit + blks_read), 2)
                         FROM pg_stat_database WHERE datname = current_database()) as buffer_hit_ratio
                """)

                result = await conn.execute(stats_query)
                row = result.fetchone()

                if row:
                    self.database_health.active_queries = row[0] or 0
                    self.database_health.max_connections = row[1] or 0
                    self.database_health.connection_count = row[2] or 0
                    self.database_health.buffer_hit_ratio = row[3] or 0.0

                # Calculate health score
                self.database_health.calculate_health_score()

        except Exception as e:
            logger.error(f"Failed to collect database health metrics: {e}")

    async def _check_performance_regressions(self):
        """Check for performance regressions."""
        if not self.performance_monitor:
            return

        try:
            # Get recent performance report
            report = await self.performance_monitor.get_performance_report(hours=1)

            current_avg = report.get('summary_statistics', {}).get('avg_execution_time_ms', 0)

            # Compare with baseline
            if 'overall_avg' in self.performance_baselines:
                baseline_avg = self.performance_baselines['overall_avg']
                regression_pct = ((current_avg - baseline_avg) / baseline_avg) * 100

                if regression_pct > self.regression_threshold_pct:
                    logger.warning(f"Performance regression detected: {regression_pct:.1f}% slower than baseline")
                    # Trigger optimization
                    await self._trigger_emergency_optimization()

            # Update baseline
            self.performance_baselines['overall_avg'] = current_avg

        except Exception as e:
            logger.error(f"Performance regression check failed: {e}")

    async def _optimize_recent_queries(self):
        """Optimize recent slow queries."""
        if not self.query_optimizer:
            return

        try:
            # Get optimization report
            report = await self.query_optimizer.get_optimization_report(priority_filter=None)

            # Implement top recommendations automatically for conservative optimizations
            if self.optimization_level in [OptimizationLevel.AGGRESSIVE, OptimizationLevel.ULTRA]:
                top_recommendations = report.get('top_recommendations', [])[:3]

                for rec in top_recommendations:
                    if rec.get('risk_level') == 'low' and rec.get('roi_score', 0) > 50:
                        await self.query_optimizer.implement_recommendation(
                            rec['recommendation_id'],
                            dry_run=False
                        )

        except Exception as e:
            logger.error(f"Query optimization failed: {e}")

    async def _warm_query_cache(self):
        """Warm cache with frequently accessed queries."""
        # This would implement cache warming logic based on query patterns
        pass

    async def _trigger_emergency_optimization(self):
        """Trigger emergency optimization procedures."""
        logger.warning("Triggering emergency optimization procedures")

        try:
            # Force maintenance cycle
            await self.maintenance_manager.run_maintenance_cycle()

            # Clear query cache to force fresh results
            if self.query_cache.redis_client:
                await self.query_cache.redis_client.flushdb()

            # Reset connection pool
            await self.connection_pool.monitor_pool_health()

        except Exception as e:
            logger.error(f"Emergency optimization failed: {e}")

    async def execute_query_with_optimization(self,
                                            query: str,
                                            params: Optional[Dict[str, Any]] = None,
                                            cache_ttl: Optional[int] = None) -> Dict[str, Any]:
        """Execute query with full optimization pipeline."""
        start_time = time.time()
        query_hash = hashlib.sha256(query.encode()).hexdigest()[:16]

        try:
            # Check cache first
            cached_result = await self.query_cache.get_cached_result(query_hash)
            if cached_result:
                logger.debug(f"Cache hit for query {query_hash}")
                return {
                    'data': cached_result,
                    'execution_time_ms': (time.time() - start_time) * 1000,
                    'cache_hit': True,
                    'query_hash': query_hash
                }

            # Execute query with monitoring
            async with self.engine.begin() as conn:
                if params:
                    result = await conn.execute(text(query), params)
                else:
                    result = await conn.execute(text(query))

                # Convert result to dict
                if result.returns_rows:
                    data = [dict(row._mapping) for row in result.fetchall()]
                else:
                    data = {'rowcount': result.rowcount}

            execution_time_ms = (time.time() - start_time) * 1000

            # Track performance
            if self.performance_monitor:
                await self.performance_monitor.track_query_execution(
                    query=query,
                    execution_time_ms=execution_time_ms,
                    additional_metrics={
                        'rows_returned': len(data) if isinstance(data, list) else 1
                    }
                )

            # Cache result if beneficial
            if execution_time_ms > 10:  # Cache queries taking more than 10ms
                await self.query_cache.cache_query_result(query_hash, data, cache_ttl)

            return {
                'data': data,
                'execution_time_ms': execution_time_ms,
                'cache_hit': False,
                'query_hash': query_hash
            }

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            logger.error(f"Query execution failed in {execution_time_ms:.2f}ms: {e}")
            raise

    async def get_optimization_status(self) -> Dict[str, Any]:
        """Get comprehensive optimization framework status."""
        try:
            status = {
                "framework_status": {
                    "active": self.optimization_active,
                    "optimization_level": self.optimization_level.value,
                    "target_performance_ms": self.target_performance_ms,
                    "background_tasks_running": len([t for t in self.background_tasks if not t.done()])
                },
                "connection_pool": self.connection_pool.get_pool_metrics().__dict__,
                "query_cache": self.query_cache.get_cache_metrics().__dict__,
                "database_health": self.database_health.__dict__,
                "performance_baselines": self.performance_baselines
            }

            # Add component-specific status
            if self.performance_monitor:
                status["performance_monitor"] = self.performance_monitor.get_monitoring_status()

            if self.query_optimizer:
                optimizer_report = await self.query_optimizer.get_optimization_report()
                status["query_optimizer"] = optimizer_report.get("optimization_service_status", {})

            if self.indexing_strategy:
                status["indexing_strategy"] = self.indexing_strategy.get_strategy_summary()

            return status

        except Exception as e:
            logger.error(f"Failed to get optimization status: {e}")
            return {"error": str(e)}

    async def shutdown_framework(self):
        """Gracefully shutdown the optimization framework."""
        try:
            logger.info("Shutting down Database Optimization Framework...")

            self.optimization_active = False

            # Cancel background tasks
            for task in self.background_tasks:
                task.cancel()

            # Wait for tasks to complete
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)

            # Shutdown components
            if self.performance_monitor:
                await self.performance_monitor.stop_monitoring()

            if self.query_optimizer:
                await self.query_optimizer.stop_optimization_service()

            logger.info("Database Optimization Framework shutdown complete")

        except Exception as e:
            logger.error(f"Error during framework shutdown: {e}")


# Factory function
async def create_database_optimization_framework(
    engine: AsyncEngine,
    redis_url: Optional[str] = None,
    optimization_level: OptimizationLevel = OptimizationLevel.BALANCED,
    target_performance_ms: float = 25.0
) -> DatabaseOptimizationFramework:
    """Create and initialize DatabaseOptimizationFramework."""

    # Initialize Redis client if URL provided
    redis_client = None
    if redis_url:
        try:
            redis_client = redis.from_url(redis_url)
            await redis_client.ping()  # Test connection
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            redis_client = None

    # Create and initialize framework
    framework = DatabaseOptimizationFramework(
        engine=engine,
        redis_client=redis_client,
        optimization_level=optimization_level,
        target_performance_ms=target_performance_ms
    )

    await framework.initialize_framework()
    return framework


# Example usage
async def main():
    """Example usage of the database optimization framework."""
    from sqlalchemy.ext.asyncio import create_async_engine

    # Create async engine
    engine = create_async_engine(
        "postgresql+asyncpg://user:password@localhost/database",
        echo=False,
        pool_size=20,
        max_overflow=10
    )

    try:
        # Create optimization framework
        framework = await create_database_optimization_framework(
            engine=engine,
            redis_url="redis://localhost:6379",
            optimization_level=OptimizationLevel.BALANCED,
            target_performance_ms=25.0
        )

        # Execute optimized query
        result = await framework.execute_query_with_optimization(
            "SELECT COUNT(*) FROM fact_sale WHERE date_key >= 20240101"
        )
        print(f"Query executed in {result['execution_time_ms']:.2f}ms")

        # Get optimization status
        status = await framework.get_optimization_status()
        print(f"Framework status: {status['framework_status']}")

        # Let it run for a while to see optimization in action
        await asyncio.sleep(60)

    finally:
        await framework.shutdown_framework()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())