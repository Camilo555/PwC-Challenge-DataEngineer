"""
Advanced Materialized Views Manager
===================================

Enterprise-grade materialized views management for query acceleration:
- Intelligent view creation and refresh strategies
- Query pattern-based view recommendations
- Automatic dependency tracking and cascade refreshes
- Cost-benefit analysis for view maintenance
- Performance impact monitoring
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4

from sqlalchemy import text, inspect
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger

logger = get_logger(__name__)


class RefreshStrategy(str, Enum):
    """Materialized view refresh strategies."""
    ON_DEMAND = "on_demand"
    SCHEDULED = "scheduled"
    INCREMENTAL = "incremental"
    REAL_TIME = "real_time"
    LAZY = "lazy"


class ViewType(str, Enum):
    """Types of materialized views."""
    AGGREGATE = "aggregate"
    JOIN = "join"
    FILTERED = "filtered"
    DENORMALIZED = "denormalized"
    REPORTING = "reporting"
    OLAP_CUBE = "olap_cube"


class ViewStatus(str, Enum):
    """Materialized view status."""
    ACTIVE = "active"
    REFRESHING = "refreshing"
    STALE = "stale"
    FAILED = "failed"
    SUSPENDED = "suspended"
    DEPRECATED = "deprecated"


@dataclass
class ViewPerformanceMetrics:
    """Performance metrics for materialized views."""
    view_name: str
    
    # Query performance
    avg_query_time_ms: float = 0.0
    query_count: int = 0
    total_query_time_ms: float = 0.0
    cache_hit_rate: float = 0.0
    
    # Refresh metrics
    last_refresh_duration_ms: float = 0.0
    avg_refresh_duration_ms: float = 0.0
    refresh_count: int = 0
    last_refresh_time: Optional[datetime] = None
    
    # Storage metrics
    storage_size_mb: float = 0.0
    row_count: int = 0
    index_count: int = 0
    
    # Cost-benefit analysis
    query_time_saved_ms: float = 0.0
    maintenance_cost_ms: float = 0.0
    cost_benefit_ratio: float = 0.0
    
    # Usage patterns
    peak_usage_hour: Optional[int] = None
    usage_frequency_score: float = 0.0
    
    def update_query_metrics(self, query_time_ms: float):
        """Update query performance metrics."""
        self.query_count += 1
        self.total_query_time_ms += query_time_ms
        self.avg_query_time_ms = self.total_query_time_ms / self.query_count
        
        # Update cost-benefit analysis
        self._calculate_cost_benefit_ratio()
    
    def update_refresh_metrics(self, refresh_duration_ms: float):
        """Update refresh performance metrics."""
        self.refresh_count += 1
        self.last_refresh_duration_ms = refresh_duration_ms
        self.last_refresh_time = datetime.utcnow()
        
        # Calculate average refresh time
        if self.avg_refresh_duration_ms == 0:
            self.avg_refresh_duration_ms = refresh_duration_ms
        else:
            alpha = 0.2  # Exponential moving average
            self.avg_refresh_duration_ms = (
                alpha * refresh_duration_ms + 
                (1 - alpha) * self.avg_refresh_duration_ms
            )
        
        self.maintenance_cost_ms += refresh_duration_ms
        self._calculate_cost_benefit_ratio()
    
    def _calculate_cost_benefit_ratio(self):
        """Calculate cost-benefit ratio for the view."""
        if self.maintenance_cost_ms > 0:
            self.cost_benefit_ratio = self.query_time_saved_ms / self.maintenance_cost_ms
        else:
            self.cost_benefit_ratio = float('inf')


@dataclass
class MaterializedViewDefinition:
    """Definition of a materialized view."""
    name: str
    sql_definition: str
    view_type: ViewType
    refresh_strategy: RefreshStrategy
    
    # Dependencies
    source_tables: List[str] = field(default_factory=list)
    dependent_views: List[str] = field(default_factory=list)
    
    # Configuration
    refresh_interval_minutes: Optional[int] = None
    incremental_column: Optional[str] = None
    partition_column: Optional[str] = None
    
    # Metadata
    description: str = ""
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    
    # Status
    status: ViewStatus = ViewStatus.ACTIVE
    last_refresh: Optional[datetime] = None
    next_refresh: Optional[datetime] = None
    
    # Performance tracking
    metrics: Optional[ViewPerformanceMetrics] = None
    
    def __post_init__(self):
        """Initialize metrics if not provided."""
        if self.metrics is None:
            self.metrics = ViewPerformanceMetrics(view_name=self.name)


class MaterializedViewsManager:
    """
    Advanced materialized views manager for enterprise query acceleration.
    
    Features:
    - Intelligent view creation based on query patterns
    - Multiple refresh strategies with cost optimization
    - Automatic dependency management and cascade refreshes
    - Performance monitoring and cost-benefit analysis
    - Query pattern analysis for view recommendations
    """

    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        
        # View registry
        self.views: Dict[str, MaterializedViewDefinition] = {}
        self.view_metrics: Dict[str, ViewPerformanceMetrics] = {}
        
        # Query pattern tracking for recommendations
        self.query_patterns: Dict[str, int] = defaultdict(int)
        self.recent_queries: deque = deque(maxlen=1000)
        
        # Refresh scheduling
        self.refresh_queue: List[str] = []
        self.refresh_locks: Dict[str, asyncio.Lock] = {}
        
        # Performance tracking
        self.performance_history: deque = deque(maxlen=24 * 60)  # 24 hours of minute-level data
        
        # Configuration
        self.auto_refresh_enabled = True
        self.max_concurrent_refreshes = 3
        self.stale_threshold_minutes = 60
        self.low_usage_threshold = 0.1  # Views used less than 10% of the time
        
        # Monitoring state
        self.monitoring_active = False
        self.background_tasks: List[asyncio.Task] = []
        
        # System metrics
        self.system_metrics = {
            'total_views_managed': 0,
            'active_views': 0,
            'total_refreshes_completed': 0,
            'total_query_acceleration_ms': 0,
            'views_recommended': 0,
            'views_auto_created': 0,
            'deprecated_views_removed': 0
        }

    async def start_monitoring(self):
        """Start materialized views monitoring and management."""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        
        # Create monitoring schema
        await self._create_monitoring_schema()
        
        # Load existing views
        await self._load_existing_views()
        
        # Start background tasks
        self.background_tasks = [
            asyncio.create_task(self._refresh_scheduler()),
            asyncio.create_task(self._performance_monitor()),
            asyncio.create_task(self._usage_analyzer()),
            asyncio.create_task(self._view_recommender()),
            asyncio.create_task(self._maintenance_optimizer())
        ]
        
        logger.info("Materialized views monitoring started")

    async def stop_monitoring(self):
        """Stop monitoring and management."""
        if not self.monitoring_active:
            return
        
        self.monitoring_active = False
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()
        
        logger.info("Materialized views monitoring stopped")

    async def create_view(self, view_definition: MaterializedViewDefinition) -> bool:
        """Create a new materialized view."""
        try:
            # Validate definition
            if not self._validate_view_definition(view_definition):
                return False
            
            # Generate CREATE MATERIALIZED VIEW statement
            create_sql = await self._generate_create_statement(view_definition)
            
            # Execute creation
            async with self.engine.begin() as conn:
                await conn.execute(text(create_sql))
            
            # Register view
            self.views[view_definition.name] = view_definition
            self.view_metrics[view_definition.name] = view_definition.metrics
            self.refresh_locks[view_definition.name] = asyncio.Lock()
            
            # Schedule initial refresh if needed
            if view_definition.refresh_strategy != RefreshStrategy.LAZY:
                await self._schedule_refresh(view_definition.name)
            
            # Persist metadata
            await self._persist_view_metadata(view_definition)
            
            self.system_metrics['total_views_managed'] += 1
            self.system_metrics['active_views'] += 1
            
            logger.info(f"Created materialized view: {view_definition.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create materialized view {view_definition.name}: {e}")
            return False

    async def refresh_view(self, view_name: str, force: bool = False) -> bool:
        """Refresh a specific materialized view."""
        if view_name not in self.views:
            logger.error(f"View {view_name} not found")
            return False
        
        view_def = self.views[view_name]
        
        # Check if refresh is needed
        if not force and not self._needs_refresh(view_def):
            logger.debug(f"View {view_name} does not need refresh")
            return True
        
        # Acquire refresh lock
        async with self.refresh_locks[view_name]:
            try:
                start_time = time.time()
                
                # Update status
                view_def.status = ViewStatus.REFRESHING
                
                # Execute refresh
                if view_def.refresh_strategy == RefreshStrategy.INCREMENTAL:
                    success = await self._incremental_refresh(view_def)
                else:
                    success = await self._full_refresh(view_def)
                
                # Calculate refresh time
                refresh_duration = (time.time() - start_time) * 1000
                
                if success:
                    # Update metrics and status
                    view_def.status = ViewStatus.ACTIVE
                    view_def.last_refresh = datetime.utcnow()
                    view_def.metrics.update_refresh_metrics(refresh_duration)
                    
                    # Schedule next refresh
                    await self._schedule_next_refresh(view_name)
                    
                    self.system_metrics['total_refreshes_completed'] += 1
                    logger.info(f"Refreshed view {view_name} in {refresh_duration:.2f}ms")
                else:
                    view_def.status = ViewStatus.FAILED
                    logger.error(f"Failed to refresh view {view_name}")
                
                return success
                
            except Exception as e:
                view_def.status = ViewStatus.FAILED
                logger.error(f"Error refreshing view {view_name}: {e}")
                return False

    async def drop_view(self, view_name: str) -> bool:
        """Drop a materialized view."""
        try:
            if view_name not in self.views:
                return False
            
            # Drop from database
            async with self.engine.begin() as conn:
                await conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}"))
            
            # Remove from registry
            del self.views[view_name]
            del self.view_metrics[view_name]
            del self.refresh_locks[view_name]
            
            # Remove from refresh queue
            if view_name in self.refresh_queue:
                self.refresh_queue.remove(view_name)
            
            # Update metrics
            self.system_metrics['active_views'] = len(self.views)
            
            logger.info(f"Dropped materialized view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop view {view_name}: {e}")
            return False

    async def track_query_usage(self, query: str, execution_time_ms: float):
        """Track query usage for view recommendations."""
        # Normalize query for pattern matching
        normalized_query = self._normalize_query(query)
        
        # Update pattern tracking
        self.query_patterns[normalized_query] += 1
        self.recent_queries.append({
            'query': normalized_query,
            'execution_time_ms': execution_time_ms,
            'timestamp': datetime.utcnow()
        })
        
        # Check if query uses existing materialized view
        view_used = self._identify_view_usage(query)
        if view_used and view_used in self.view_metrics:
            self.view_metrics[view_used].update_query_metrics(execution_time_ms)

    async def get_view_recommendations(self, min_frequency: int = 5) -> List[Dict[str, Any]]:
        """Get recommendations for new materialized views."""
        recommendations = []
        
        # Analyze query patterns
        for pattern, frequency in self.query_patterns.items():
            if frequency < min_frequency:
                continue
            
            # Estimate potential benefit
            recent_executions = [
                q for q in self.recent_queries 
                if q['query'] == pattern
            ]
            
            if not recent_executions:
                continue
            
            avg_execution_time = sum(q['execution_time_ms'] for q in recent_executions) / len(recent_executions)
            
            # Skip fast queries (less than 100ms)
            if avg_execution_time < 100:
                continue
            
            potential_savings = avg_execution_time * frequency * 0.8  # Assume 80% improvement
            
            recommendation = {
                'query_pattern': pattern,
                'frequency': frequency,
                'avg_execution_time_ms': avg_execution_time,
                'potential_savings_ms': potential_savings,
                'recommended_view_type': self._suggest_view_type(pattern),
                'recommended_refresh_strategy': self._suggest_refresh_strategy(frequency, avg_execution_time)
            }
            
            recommendations.append(recommendation)
        
        # Sort by potential savings
        recommendations.sort(key=lambda x: x['potential_savings_ms'], reverse=True)
        
        return recommendations[:10]  # Top 10 recommendations

    async def get_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        total_views = len(self.views)
        active_views = len([v for v in self.views.values() if v.status == ViewStatus.ACTIVE])
        
        # Calculate aggregate metrics
        total_query_acceleration = sum(
            m.query_time_saved_ms for m in self.view_metrics.values()
        )
        
        total_maintenance_cost = sum(
            m.maintenance_cost_ms for m in self.view_metrics.values()
        )
        
        overall_cost_benefit = (
            total_query_acceleration / total_maintenance_cost 
            if total_maintenance_cost > 0 else 0
        )
        
        # Top performing views
        top_views = sorted(
            [
                {
                    'name': name,
                    'query_count': metrics.query_count,
                    'avg_query_time_ms': metrics.avg_query_time_ms,
                    'cost_benefit_ratio': metrics.cost_benefit_ratio,
                    'storage_size_mb': metrics.storage_size_mb
                }
                for name, metrics in self.view_metrics.items()
            ],
            key=lambda x: x['cost_benefit_ratio'],
            reverse=True
        )[:5]
        
        # Low-performing views that might need attention
        underperforming_views = [
            {
                'name': name,
                'cost_benefit_ratio': metrics.cost_benefit_ratio,
                'usage_frequency': metrics.usage_frequency_score,
                'last_refresh': view.last_refresh.isoformat() if view.last_refresh else None
            }
            for name, view in self.views.items()
            for metrics in [self.view_metrics[name]]
            if metrics.cost_benefit_ratio < 2.0 or metrics.usage_frequency_score < 0.1
        ]
        
        return {
            'generated_at': datetime.utcnow().isoformat(),
            
            'summary': {
                'total_views': total_views,
                'active_views': active_views,
                'stale_views': len([v for v in self.views.values() if v.status == ViewStatus.STALE]),
                'failed_views': len([v for v in self.views.values() if v.status == ViewStatus.FAILED])
            },
            
            'performance': {
                'total_query_acceleration_ms': total_query_acceleration,
                'total_maintenance_cost_ms': total_maintenance_cost,
                'overall_cost_benefit_ratio': overall_cost_benefit,
                'avg_refresh_time_ms': sum(m.avg_refresh_duration_ms for m in self.view_metrics.values()) / max(len(self.view_metrics), 1)
            },
            
            'top_performing_views': top_views,
            'underperforming_views': underperforming_views,
            
            'system_metrics': self.system_metrics,
            
            'recommendations': await self.get_view_recommendations()
        }

    # Background monitoring tasks

    async def _refresh_scheduler(self):
        """Background task for scheduled view refreshes."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                if not self.auto_refresh_enabled:
                    continue
                
                current_time = datetime.utcnow()
                views_to_refresh = []
                
                # Find views that need refresh
                for view_name, view_def in self.views.items():
                    if self._needs_refresh(view_def, current_time):
                        views_to_refresh.append(view_name)
                
                # Refresh views (limit concurrent refreshes)
                if views_to_refresh:
                    semaphore = asyncio.Semaphore(self.max_concurrent_refreshes)
                    
                    async def refresh_with_semaphore(view_name):
                        async with semaphore:
                            await self.refresh_view(view_name)
                    
                    refresh_tasks = [
                        asyncio.create_task(refresh_with_semaphore(view_name))
                        for view_name in views_to_refresh
                    ]
                    
                    await asyncio.gather(*refresh_tasks, return_exceptions=True)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Refresh scheduler error: {e}")

    async def _performance_monitor(self):
        """Monitor materialized view performance."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                for view_name, metrics in self.view_metrics.items():
                    # Update storage metrics
                    await self._update_storage_metrics(view_name, metrics)
                    
                    # Check for performance issues
                    if metrics.cost_benefit_ratio < 1.0 and metrics.query_count > 10:
                        logger.warning(
                            f"View {view_name} has poor cost-benefit ratio: {metrics.cost_benefit_ratio:.2f}"
                        )
                    
                    # Check for stale views
                    view_def = self.views[view_name]
                    if (view_def.last_refresh and 
                        (datetime.utcnow() - view_def.last_refresh).total_seconds() > self.stale_threshold_minutes * 60):
                        view_def.status = ViewStatus.STALE
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance monitor error: {e}")

    async def _usage_analyzer(self):
        """Analyze view usage patterns."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(3600)  # Analyze every hour
                
                for view_name, metrics in self.view_metrics.items():
                    # Calculate usage frequency
                    recent_hour_queries = len([
                        q for q in self.recent_queries
                        if (datetime.utcnow() - q['timestamp']).total_seconds() < 3600
                        and self._query_uses_view(q['query'], view_name)
                    ])
                    
                    # Update usage frequency score
                    total_recent_queries = len([
                        q for q in self.recent_queries
                        if (datetime.utcnow() - q['timestamp']).total_seconds() < 3600
                    ])
                    
                    if total_recent_queries > 0:
                        metrics.usage_frequency_score = recent_hour_queries / total_recent_queries
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Usage analyzer error: {e}")

    async def _view_recommender(self):
        """Recommend new materialized views based on query patterns."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(1800)  # Check every 30 minutes
                
                recommendations = await self.get_view_recommendations(min_frequency=10)
                
                for rec in recommendations[:3]:  # Only process top 3 recommendations
                    if rec['potential_savings_ms'] > 10000:  # Only if significant savings
                        
                        # Auto-create view if conditions are met
                        view_name = f"auto_mv_{len(self.views) + 1}"
                        
                        view_def = MaterializedViewDefinition(
                            name=view_name,
                            sql_definition=rec['query_pattern'],
                            view_type=ViewType(rec['recommended_view_type']),
                            refresh_strategy=RefreshStrategy(rec['recommended_refresh_strategy']),
                            description=f"Auto-generated view based on query pattern analysis",
                            tags=['auto-generated', 'recommended'],
                            created_by='system_recommender'
                        )
                        
                        success = await self.create_view(view_def)
                        if success:
                            self.system_metrics['views_auto_created'] += 1
                            logger.info(
                                f"Auto-created materialized view {view_name} "
                                f"with potential savings of {rec['potential_savings_ms']:.2f}ms"
                            )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"View recommender error: {e}")

    async def _maintenance_optimizer(self):
        """Optimize view maintenance schedules and remove unused views."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(7200)  # Run every 2 hours
                
                views_to_deprecate = []
                
                for view_name, view_def in self.views.items():
                    metrics = self.view_metrics[view_name]
                    
                    # Check for unused views
                    if (metrics.usage_frequency_score < self.low_usage_threshold and
                        metrics.query_count > 0):  # Has some history
                        
                        # Mark for deprecation
                        if view_def.status != ViewStatus.DEPRECATED:
                            view_def.status = ViewStatus.DEPRECATED
                            logger.info(f"Marked view {view_name} as deprecated due to low usage")
                    
                    # Remove deprecated views after 24 hours
                    elif (view_def.status == ViewStatus.DEPRECATED and
                          view_def.last_refresh and
                          (datetime.utcnow() - view_def.last_refresh).total_seconds() > 86400):
                        
                        views_to_deprecate.append(view_name)
                
                # Remove deprecated views
                for view_name in views_to_deprecate:
                    if await self.drop_view(view_name):
                        self.system_metrics['deprecated_views_removed'] += 1
                        logger.info(f"Removed deprecated view {view_name}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Maintenance optimizer error: {e}")

    # Helper methods

    async def _create_monitoring_schema(self):
        """Create database schema for materialized views monitoring."""
        schema_sql = """
        -- Materialized views metadata
        CREATE TABLE IF NOT EXISTS mv_metadata (
            view_name VARCHAR(255) PRIMARY KEY,
            sql_definition TEXT NOT NULL,
            view_type VARCHAR(50) NOT NULL,
            refresh_strategy VARCHAR(50) NOT NULL,
            source_tables JSONB,
            refresh_interval_minutes INTEGER,
            description TEXT,
            tags JSONB,
            status VARCHAR(50) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(255),
            last_refresh TIMESTAMP,
            next_refresh TIMESTAMP
        );

        -- View performance metrics
        CREATE TABLE IF NOT EXISTS mv_performance_metrics (
            view_name VARCHAR(255) NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            query_count BIGINT DEFAULT 0,
            avg_query_time_ms DECIMAL(10,3),
            refresh_duration_ms DECIMAL(10,3),
            storage_size_mb DECIMAL(10,3),
            row_count BIGINT,
            cost_benefit_ratio DECIMAL(10,3),
            usage_frequency_score DECIMAL(5,4),
            PRIMARY KEY (view_name, recorded_at)
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_mv_metadata_status ON mv_metadata(status, next_refresh);
        CREATE INDEX IF NOT EXISTS idx_mv_metrics_recorded_at ON mv_performance_metrics(recorded_at DESC);
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(schema_sql))

    def _validate_view_definition(self, view_def: MaterializedViewDefinition) -> bool:
        """Validate materialized view definition."""
        if not view_def.name or not view_def.sql_definition:
            return False
        
        # Check for SQL injection (basic validation)
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE USER']
        sql_upper = view_def.sql_definition.upper()
        
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                logger.error(f"Dangerous keyword '{keyword}' found in view definition")
                return False
        
        return True

    async def _generate_create_statement(self, view_def: MaterializedViewDefinition) -> str:
        """Generate CREATE MATERIALIZED VIEW statement."""
        sql = f"CREATE MATERIALIZED VIEW {view_def.name} AS {view_def.sql_definition}"
        
        # Add partitioning if specified
        if view_def.partition_column:
            sql += f" PARTITION BY ({view_def.partition_column})"
        
        return sql

    def _normalize_query(self, query: str) -> str:
        """Normalize query for pattern matching."""
        # Simple normalization - replace parameters with placeholders
        import re
        
        # Remove extra whitespace
        normalized = ' '.join(query.split())
        
        # Replace string literals
        normalized = re.sub(r"'[^']*'", "?", normalized)
        
        # Replace numeric literals
        normalized = re.sub(r'\b\d+\b', "?", normalized)
        
        return normalized.lower()

    def _needs_refresh(self, view_def: MaterializedViewDefinition, current_time: datetime = None) -> bool:
        """Check if view needs refresh."""
        if current_time is None:
            current_time = datetime.utcnow()
        
        if view_def.refresh_strategy == RefreshStrategy.LAZY:
            return False
        
        if not view_def.last_refresh:
            return True
        
        if view_def.refresh_interval_minutes:
            time_since_refresh = (current_time - view_def.last_refresh).total_seconds() / 60
            return time_since_refresh >= view_def.refresh_interval_minutes
        
        return False

    async def _full_refresh(self, view_def: MaterializedViewDefinition) -> bool:
        """Perform full refresh of materialized view."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text(f"REFRESH MATERIALIZED VIEW {view_def.name}"))
            return True
        except Exception as e:
            logger.error(f"Full refresh failed for {view_def.name}: {e}")
            return False

    async def _incremental_refresh(self, view_def: MaterializedViewDefinition) -> bool:
        """Perform incremental refresh (simplified implementation)."""
        # This would implement incremental refresh logic based on incremental_column
        # For now, fall back to full refresh
        return await self._full_refresh(view_def)

    def _identify_view_usage(self, query: str) -> Optional[str]:
        """Identify which materialized view is being used by the query."""
        query_lower = query.lower()
        
        for view_name in self.views.keys():
            if view_name.lower() in query_lower:
                return view_name
        
        return None

    def _query_uses_view(self, query: str, view_name: str) -> bool:
        """Check if query uses specific view."""
        return view_name.lower() in query.lower()

    def _suggest_view_type(self, query_pattern: str) -> str:
        """Suggest appropriate view type based on query pattern."""
        pattern_lower = query_pattern.lower()
        
        if 'group by' in pattern_lower or 'sum(' in pattern_lower or 'count(' in pattern_lower:
            return ViewType.AGGREGATE.value
        elif 'join' in pattern_lower:
            return ViewType.JOIN.value
        elif 'where' in pattern_lower:
            return ViewType.FILTERED.value
        else:
            return ViewType.REPORTING.value

    def _suggest_refresh_strategy(self, frequency: int, avg_execution_time: float) -> str:
        """Suggest appropriate refresh strategy."""
        if frequency > 100 and avg_execution_time > 1000:
            return RefreshStrategy.INCREMENTAL.value
        elif frequency > 50:
            return RefreshStrategy.SCHEDULED.value
        else:
            return RefreshStrategy.ON_DEMAND.value

    async def _update_storage_metrics(self, view_name: str, metrics: ViewPerformanceMetrics):
        """Update storage-related metrics for a view."""
        try:
            async with self.engine.begin() as conn:
                # Get storage size (PostgreSQL specific)
                result = await conn.execute(text(f"""
                    SELECT 
                        pg_total_relation_size('{view_name}') / 1024.0 / 1024.0 as size_mb,
                        COUNT(*) as row_count
                    FROM {view_name}
                """))
                
                row = result.fetchone()
                if row:
                    metrics.storage_size_mb = float(row[0])
                    metrics.row_count = int(row[1])
                    
        except Exception as e:
            logger.debug(f"Could not update storage metrics for {view_name}: {e}")

    async def _schedule_refresh(self, view_name: str):
        """Schedule view refresh."""
        if view_name not in self.refresh_queue:
            self.refresh_queue.append(view_name)

    async def _schedule_next_refresh(self, view_name: str):
        """Schedule next refresh based on strategy."""
        view_def = self.views[view_name]
        
        if view_def.refresh_interval_minutes:
            view_def.next_refresh = datetime.utcnow() + timedelta(
                minutes=view_def.refresh_interval_minutes
            )

    async def _persist_view_metadata(self, view_def: MaterializedViewDefinition):
        """Persist view metadata to database."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO mv_metadata 
                    (view_name, sql_definition, view_type, refresh_strategy, 
                     source_tables, refresh_interval_minutes, description, tags, 
                     status, created_by, last_refresh, next_refresh)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (view_name) DO UPDATE SET
                        sql_definition = EXCLUDED.sql_definition,
                        view_type = EXCLUDED.view_type,
                        refresh_strategy = EXCLUDED.refresh_strategy,
                        source_tables = EXCLUDED.source_tables,
                        refresh_interval_minutes = EXCLUDED.refresh_interval_minutes,
                        description = EXCLUDED.description,
                        tags = EXCLUDED.tags,
                        status = EXCLUDED.status,
                        last_refresh = EXCLUDED.last_refresh,
                        next_refresh = EXCLUDED.next_refresh
                """),
                view_def.name,
                view_def.sql_definition,
                view_def.view_type.value,
                view_def.refresh_strategy.value,
                json.dumps(view_def.source_tables),
                view_def.refresh_interval_minutes,
                view_def.description,
                json.dumps(view_def.tags),
                view_def.status.value,
                view_def.created_by,
                view_def.last_refresh,
                view_def.next_refresh)
                
        except Exception as e:
            logger.error(f"Failed to persist metadata for view {view_def.name}: {e}")

    async def _load_existing_views(self):
        """Load existing materialized views from database."""
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(text("""
                    SELECT view_name, sql_definition, view_type, refresh_strategy,
                           source_tables, refresh_interval_minutes, description, 
                           tags, status, created_by, last_refresh, next_refresh
                    FROM mv_metadata
                """))
                
                for row in result:
                    view_def = MaterializedViewDefinition(
                        name=row[0],
                        sql_definition=row[1],
                        view_type=ViewType(row[2]),
                        refresh_strategy=RefreshStrategy(row[3]),
                        source_tables=json.loads(row[4]) if row[4] else [],
                        refresh_interval_minutes=row[5],
                        description=row[6] or "",
                        tags=json.loads(row[7]) if row[7] else [],
                        status=ViewStatus(row[8]),
                        created_by=row[9],
                        last_refresh=row[10],
                        next_refresh=row[11]
                    )
                    
                    self.views[view_def.name] = view_def
                    self.view_metrics[view_def.name] = view_def.metrics
                    self.refresh_locks[view_def.name] = asyncio.Lock()
                
                self.system_metrics['total_views_managed'] = len(self.views)
                self.system_metrics['active_views'] = len([
                    v for v in self.views.values() 
                    if v.status == ViewStatus.ACTIVE
                ])
                
        except Exception as e:
            logger.warning(f"Could not load existing views: {e}")


# Factory function
async def create_materialized_views_manager(engine: AsyncEngine) -> MaterializedViewsManager:
    """Create and initialize MaterializedViewsManager."""
    manager = MaterializedViewsManager(engine)
    await manager.start_monitoring()
    return manager


# Example usage
async def main():
    """Example usage of materialized views manager."""
    from sqlalchemy.ext.asyncio import create_async_engine
    
    engine = create_async_engine("postgresql+asyncpg://user:password@localhost/database")
    
    manager = await create_materialized_views_manager(engine)
    
    try:
        # Create a sample materialized view
        view_def = MaterializedViewDefinition(
            name="sales_summary_mv",
            sql_definition="""
                SELECT 
                    DATE_TRUNC('day', order_date) as order_day,
                    product_category,
                    COUNT(*) as order_count,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value
                FROM sales 
                WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY DATE_TRUNC('day', order_date), product_category
            """,
            view_type=ViewType.AGGREGATE,
            refresh_strategy=RefreshStrategy.SCHEDULED,
            refresh_interval_minutes=60,
            description="Daily sales summary by product category"
        )
        
        await manager.create_view(view_def)
        
        # Wait and generate report
        await asyncio.sleep(5)
        
        report = await manager.get_performance_report()
        print("Materialized Views Report:")
        print(f"  Total views: {report['summary']['total_views']}")
        print(f"  Active views: {report['summary']['active_views']}")
        print(f"  Overall cost-benefit ratio: {report['performance']['overall_cost_benefit_ratio']:.2f}")
        
    finally:
        await manager.stop_monitoring()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())