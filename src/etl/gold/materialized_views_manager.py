"""
Materialized Views Manager for Real-Time KPI Calculations
Provides comprehensive materialized view management for business intelligence dashboards
"""
import asyncio
import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncpg
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from core.config.unified_config import get_unified_config
from core.logging import get_logger


class RefreshStrategy(Enum):
    """Materialized view refresh strategies"""
    IMMEDIATE = "immediate"  # Refresh immediately when data changes
    SCHEDULED = "scheduled"  # Refresh on schedule
    ON_DEMAND = "on_demand"  # Refresh when requested
    HYBRID = "hybrid"       # Combination of immediate and scheduled


class ViewComplexity(Enum):
    """View complexity levels for optimization"""
    SIMPLE = "simple"       # Basic aggregations
    MEDIUM = "medium"       # Joins and window functions
    COMPLEX = "complex"     # Complex analytics and ML features


@dataclass
class MaterializedViewConfig:
    """Configuration for materialized view"""
    name: str
    query: str
    refresh_strategy: RefreshStrategy
    refresh_interval_minutes: int = 15
    complexity: ViewComplexity = ViewComplexity.MEDIUM
    dependencies: List[str] = field(default_factory=list)
    indexes: List[str] = field(default_factory=list)
    partition_by: Optional[str] = None
    retention_days: int = 90
    enable_incremental: bool = True
    priority: int = 1  # 1 = highest, 5 = lowest


class MaterializedViewsManager:
    """
    Comprehensive materialized views manager for real-time KPI calculations
    Supports intelligent refresh strategies and performance optimization
    """

    def __init__(self):
        self.config = get_unified_config()
        self.logger = get_logger(__name__)
        
        # Database connection
        self.engine = self._create_async_engine()
        
        # View configurations
        self.views_config = self._initialize_view_configs()
        
        # Refresh tracking
        self.refresh_history: Dict[str, List[datetime]] = {}
        self.view_dependencies: Dict[str, List[str]] = {}
        
        # Performance metrics
        self.metrics = {
            "views_created": 0,
            "views_refreshed": 0,
            "refresh_duration_total": 0,
            "refresh_errors": 0
        }

    def _create_async_engine(self):
        """Create async SQLAlchemy engine"""
        try:
            db_url = self.config.database_url.replace("postgresql://", "postgresql+asyncpg://")
            return create_async_engine(
                db_url,
                poolclass=NullPool,
                echo=False,
                pool_pre_ping=True
            )
        except Exception as e:
            self.logger.error(f"Failed to create async engine: {e}")
            raise

    def _initialize_view_configs(self) -> Dict[str, MaterializedViewConfig]:
        """Initialize predefined materialized view configurations"""
        return {
            # Executive KPI Views
            "executive_revenue_kpis": MaterializedViewConfig(
                name="executive_revenue_kpis",
                query=self._get_executive_revenue_kpis_query(),
                refresh_strategy=RefreshStrategy.IMMEDIATE,
                refresh_interval_minutes=5,
                complexity=ViewComplexity.MEDIUM,
                indexes=["time_bucket", "total_revenue"],
                partition_by="time_bucket",
                priority=1
            ),
            
            "executive_operational_kpis": MaterializedViewConfig(
                name="executive_operational_kpis",
                query=self._get_executive_operational_kpis_query(),
                refresh_strategy=RefreshStrategy.SCHEDULED,
                refresh_interval_minutes=10,
                complexity=ViewComplexity.COMPLEX,
                indexes=["time_bucket", "success_rate_percentage"],
                partition_by="time_bucket",
                priority=1
            ),
            
            # Real-time Business Intelligence
            "realtime_sales_metrics": MaterializedViewConfig(
                name="realtime_sales_metrics",
                query=self._get_realtime_sales_metrics_query(),
                refresh_strategy=RefreshStrategy.IMMEDIATE,
                refresh_interval_minutes=2,
                complexity=ViewComplexity.SIMPLE,
                indexes=["hour_bucket", "total_sales"],
                partition_by="hour_bucket",
                priority=1
            ),
            
            "customer_behavior_metrics": MaterializedViewConfig(
                name="customer_behavior_metrics",
                query=self._get_customer_behavior_metrics_query(),
                refresh_strategy=RefreshStrategy.SCHEDULED,
                refresh_interval_minutes=30,
                complexity=ViewComplexity.COMPLEX,
                indexes=["customer_segment", "behavior_score"],
                priority=2
            ),
            
            # Data Quality KPIs
            "data_quality_dashboard": MaterializedViewConfig(
                name="data_quality_dashboard",
                query=self._get_data_quality_dashboard_query(),
                refresh_strategy=RefreshStrategy.SCHEDULED,
                refresh_interval_minutes=15,
                complexity=ViewComplexity.MEDIUM,
                indexes=["check_timestamp", "quality_score"],
                partition_by="check_timestamp",
                priority=2
            ),
            
            # Performance Analytics
            "api_performance_metrics": MaterializedViewConfig(
                name="api_performance_metrics",
                query=self._get_api_performance_metrics_query(),
                refresh_strategy=RefreshStrategy.IMMEDIATE,
                refresh_interval_minutes=5,
                complexity=ViewComplexity.MEDIUM,
                indexes=["time_bucket", "endpoint"],
                partition_by="time_bucket",
                priority=1
            ),
            
            # ETL Pipeline Monitoring
            "etl_pipeline_health": MaterializedViewConfig(
                name="etl_pipeline_health",
                query=self._get_etl_pipeline_health_query(),
                refresh_strategy=RefreshStrategy.SCHEDULED,
                refresh_interval_minutes=10,
                complexity=ViewComplexity.MEDIUM,
                indexes=["pipeline_name", "execution_date"],
                partition_by="execution_date",
                priority=2
            ),
            
            # Financial Analytics
            "financial_kpis_hourly": MaterializedViewConfig(
                name="financial_kpis_hourly",
                query=self._get_financial_kpis_hourly_query(),
                refresh_strategy=RefreshStrategy.HYBRID,
                refresh_interval_minutes=60,
                complexity=ViewComplexity.COMPLEX,
                indexes=["hour_bucket", "revenue_total"],
                partition_by="hour_bucket",
                priority=2
            ),
            
            # Advanced Dashboard KPIs for Story 1.1
            "mv_daily_sales_kpi": MaterializedViewConfig(
                name="mv_daily_sales_kpi",
                query=self._get_daily_sales_kpi_query(),
                refresh_strategy=RefreshStrategy.IMMEDIATE,
                refresh_interval_minutes=1,
                complexity=ViewComplexity.COMPLEX,
                indexes=["date", "total_revenue", "volatility_flag"],
                partition_by="date",
                enable_incremental=True,
                priority=1
            ),
            
            "mv_customer_segments_realtime": MaterializedViewConfig(
                name="mv_customer_segments_realtime",
                query=self._get_customer_segments_realtime_query(),
                refresh_strategy=RefreshStrategy.SCHEDULED,
                refresh_interval_minutes=15,
                complexity=ViewComplexity.COMPLEX,
                indexes=["segment_name", "last_updated"],
                priority=1
            ),
            
            "mv_anomaly_detection_kpis": MaterializedViewConfig(
                name="mv_anomaly_detection_kpis",
                query=self._get_anomaly_detection_kpis_query(),
                refresh_strategy=RefreshStrategy.IMMEDIATE,
                refresh_interval_minutes=5,
                complexity=ViewComplexity.COMPLEX,
                indexes=["kpi_name", "timestamp", "is_anomaly"],
                partition_by="DATE(timestamp)",
                priority=1
            ),
            
            "mv_dashboard_performance_metrics": MaterializedViewConfig(
                name="mv_dashboard_performance_metrics",
                query=self._get_dashboard_performance_metrics_query(),
                refresh_strategy=RefreshStrategy.IMMEDIATE,
                refresh_interval_minutes=2,
                complexity=ViewComplexity.MEDIUM,
                indexes=["time_bucket", "endpoint", "avg_response_time"],
                partition_by="time_bucket",
                priority=1
            )
        }

    def _get_executive_revenue_kpis_query(self) -> str:
        """Get query for executive revenue KPIs materialized view"""
        return """
        SELECT 
            DATE_TRUNC('hour', created_at) as time_bucket,
            SUM(amount) as total_revenue,
            COUNT(DISTINCT customer_id) as unique_customers,
            AVG(amount) as average_transaction_value,
            SUM(amount) / NULLIF(COUNT(DISTINCT customer_id), 0) as revenue_per_customer,
            CASE 
                WHEN LAG(SUM(amount)) OVER (ORDER BY DATE_TRUNC('hour', created_at)) IS NULL THEN 0
                ELSE (SUM(amount) - LAG(SUM(amount)) OVER (ORDER BY DATE_TRUNC('hour', created_at))) 
                     / NULLIF(LAG(SUM(amount)) OVER (ORDER BY DATE_TRUNC('hour', created_at)), 0) * 100
            END as revenue_growth_rate,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_transaction_value,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) as p95_transaction_value,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN amount > 1000 THEN 1 ELSE 0 END) as high_value_transactions,
            AVG(quantity) as average_items_per_transaction,
            NOW() as last_updated
        FROM sales_transactions 
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY DATE_TRUNC('hour', created_at)
        ORDER BY time_bucket DESC
        """

    def _get_executive_operational_kpis_query(self) -> str:
        """Get query for executive operational KPIs materialized view"""
        return """
        WITH processing_metrics AS (
            SELECT 
                DATE_TRUNC('hour', processed_at) as time_bucket,
                COUNT(*) as total_records_processed,
                AVG(processing_time_ms) as avg_processing_time,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_time_ms) as p95_processing_time,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_time_ms) as p99_processing_time,
                COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
                MIN(processing_time_ms) as min_processing_time,
                MAX(processing_time_ms) as max_processing_time
            FROM etl_processing_logs
            WHERE processed_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE_TRUNC('hour', processed_at)
        ),
        system_health AS (
            SELECT 
                DATE_TRUNC('hour', timestamp) as time_bucket,
                AVG(cpu_usage_percent) as avg_cpu_usage,
                AVG(memory_usage_percent) as avg_memory_usage,
                AVG(disk_usage_percent) as avg_disk_usage,
                MAX(cpu_usage_percent) as max_cpu_usage,
                MAX(memory_usage_percent) as max_memory_usage
            FROM system_metrics
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY DATE_TRUNC('hour', timestamp)
        )
        SELECT 
            p.time_bucket,
            p.total_records_processed,
            p.avg_processing_time,
            p.p95_processing_time,
            p.p99_processing_time,
            ROUND((p.success_count::DECIMAL / NULLIF(p.total_records_processed, 0)) * 100, 2) as success_rate_percentage,
            ROUND((p.error_count::DECIMAL / NULLIF(p.total_records_processed, 0)) * 100, 2) as error_rate_percentage,
            CASE 
                WHEN p.p95_processing_time < 100 THEN 'Excellent'
                WHEN p.p95_processing_time < 500 THEN 'Good'
                WHEN p.p95_processing_time < 1000 THEN 'Fair'
                ELSE 'Needs Improvement'
            END as performance_grade,
            s.avg_cpu_usage,
            s.avg_memory_usage,
            s.avg_disk_usage,
            s.max_cpu_usage,
            s.max_memory_usage,
            CASE 
                WHEN s.avg_cpu_usage > 80 OR s.avg_memory_usage > 85 THEN 'High'
                WHEN s.avg_cpu_usage > 60 OR s.avg_memory_usage > 70 THEN 'Medium'
                ELSE 'Low'
            END as resource_utilization_level,
            NOW() as last_updated
        FROM processing_metrics p
        LEFT JOIN system_health s ON p.time_bucket = s.time_bucket
        ORDER BY p.time_bucket DESC
        """

    def _get_realtime_sales_metrics_query(self) -> str:
        """Get query for real-time sales metrics"""
        return """
        SELECT 
            DATE_TRUNC('hour', created_at) as hour_bucket,
            COUNT(*) as total_sales,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_sale_amount,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(quantity) as total_items_sold,
            MAX(amount) as max_sale_amount,
            MIN(amount) as min_sale_amount,
            STDDEV(amount) as revenue_std_dev,
            COUNT(CASE WHEN amount > (SELECT AVG(amount) * 2 FROM sales_transactions) THEN 1 END) as outlier_sales,
            NOW() as last_updated
        FROM sales_transactions 
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        GROUP BY DATE_TRUNC('hour', created_at)
        ORDER BY hour_bucket DESC
        """

    def _get_customer_behavior_metrics_query(self) -> str:
        """Get query for customer behavior metrics"""
        return """
        WITH customer_stats AS (
            SELECT 
                customer_id,
                COUNT(*) as total_purchases,
                SUM(amount) as total_spent,
                AVG(amount) as avg_purchase_amount,
                MAX(created_at) as last_purchase_date,
                MIN(created_at) as first_purchase_date,
                DATE_PART('days', MAX(created_at) - MIN(created_at)) as customer_lifetime_days,
                COUNT(DISTINCT DATE_TRUNC('month', created_at)) as active_months
            FROM sales_transactions 
            WHERE created_at >= NOW() - INTERVAL '12 months'
            GROUP BY customer_id
        ),
        customer_segments AS (
            SELECT 
                customer_id,
                CASE 
                    WHEN total_spent >= 10000 THEN 'VIP'
                    WHEN total_spent >= 5000 THEN 'Premium'
                    WHEN total_spent >= 1000 THEN 'Regular'
                    ELSE 'Basic'
                END as customer_segment,
                CASE 
                    WHEN last_purchase_date >= NOW() - INTERVAL '30 days' THEN 'Active'
                    WHEN last_purchase_date >= NOW() - INTERVAL '90 days' THEN 'At Risk'
                    ELSE 'Churned'
                END as customer_status,
                CASE 
                    WHEN customer_lifetime_days > 0 THEN total_spent / customer_lifetime_days
                    ELSE total_spent
                END as daily_value,
                total_purchases / GREATEST(active_months, 1) as purchases_per_month
            FROM customer_stats
        )
        SELECT 
            customer_segment,
            customer_status,
            COUNT(*) as customer_count,
            AVG(cs.total_spent) as avg_customer_value,
            AVG(cs.total_purchases) as avg_purchases,
            AVG(cs.avg_purchase_amount) as avg_transaction_size,
            AVG(seg.daily_value) as avg_daily_value,
            AVG(seg.purchases_per_month) as avg_monthly_frequency,
            SUM(cs.total_spent) as segment_revenue,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.total_spent) as median_customer_value,
            AVG(cs.customer_lifetime_days) as avg_customer_lifetime_days,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as segment_percentage,
            NOW() as last_updated
        FROM customer_stats cs
        JOIN customer_segments seg ON cs.customer_id = seg.customer_id
        GROUP BY customer_segment, customer_status
        ORDER BY 
            CASE customer_segment 
                WHEN 'VIP' THEN 1 
                WHEN 'Premium' THEN 2 
                WHEN 'Regular' THEN 3 
                ELSE 4 
            END,
            CASE customer_status 
                WHEN 'Active' THEN 1 
                WHEN 'At Risk' THEN 2 
                ELSE 3 
            END
        """

    def _get_data_quality_dashboard_query(self) -> str:
        """Get query for data quality dashboard"""
        return """
        SELECT 
            DATE_TRUNC('hour', check_timestamp) as check_timestamp,
            table_name,
            check_type,
            AVG(quality_score) as avg_quality_score,
            MIN(quality_score) as min_quality_score,
            MAX(quality_score) as max_quality_score,
            COUNT(*) as total_checks,
            COUNT(CASE WHEN quality_score >= 0.95 THEN 1 END) as excellent_checks,
            COUNT(CASE WHEN quality_score >= 0.80 AND quality_score < 0.95 THEN 1 END) as good_checks,
            COUNT(CASE WHEN quality_score < 0.80 THEN 1 END) as poor_checks,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_checks,
            STRING_AGG(DISTINCT issue_description, '; ') as common_issues,
            NOW() as last_updated
        FROM data_quality_checks 
        WHERE check_timestamp >= NOW() - INTERVAL '48 hours'
        GROUP BY DATE_TRUNC('hour', check_timestamp), table_name, check_type
        ORDER BY check_timestamp DESC, table_name, check_type
        """

    def _get_api_performance_metrics_query(self) -> str:
        """Get query for API performance metrics"""
        return """
        SELECT 
            DATE_TRUNC('minute', timestamp) as time_bucket,
            endpoint,
            method,
            COUNT(*) as request_count,
            AVG(response_time_ms) as avg_response_time,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY response_time_ms) as median_response_time,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) as p95_response_time,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time_ms) as p99_response_time,
            MIN(response_time_ms) as min_response_time,
            MAX(response_time_ms) as max_response_time,
            COUNT(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 END) as success_count,
            COUNT(CASE WHEN status_code >= 400 AND status_code < 500 THEN 1 END) as client_error_count,
            COUNT(CASE WHEN status_code >= 500 THEN 1 END) as server_error_count,
            ROUND((COUNT(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 END)::DECIMAL / COUNT(*)) * 100, 2) as success_rate,
            AVG(request_size_bytes) as avg_request_size,
            AVG(response_size_bytes) as avg_response_size,
            NOW() as last_updated
        FROM api_logs 
        WHERE timestamp >= NOW() - INTERVAL '6 hours'
        GROUP BY DATE_TRUNC('minute', timestamp), endpoint, method
        ORDER BY time_bucket DESC, request_count DESC
        """

    def _get_etl_pipeline_health_query(self) -> str:
        """Get query for ETL pipeline health monitoring"""
        return """
        SELECT 
            pipeline_name,
            DATE_TRUNC('day', execution_date) as execution_date,
            stage,
            COUNT(*) as total_executions,
            COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_executions,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_executions,
            COUNT(CASE WHEN status = 'running' THEN 1 END) as running_executions,
            AVG(duration_minutes) as avg_duration_minutes,
            MAX(duration_minutes) as max_duration_minutes,
            MIN(duration_minutes) as min_duration_minutes,
            AVG(records_processed) as avg_records_processed,
            SUM(records_processed) as total_records_processed,
            AVG(records_processed / NULLIF(duration_minutes, 0)) as avg_records_per_minute,
            ROUND((COUNT(CASE WHEN status = 'success' THEN 1 END)::DECIMAL / COUNT(*)) * 100, 2) as success_rate,
            MAX(execution_timestamp) as last_execution,
            STRING_AGG(DISTINCT error_message, '; ') FILTER (WHERE status = 'failed') as common_errors,
            NOW() as last_updated
        FROM etl_execution_logs 
        WHERE execution_date >= NOW() - INTERVAL '7 days'
        GROUP BY pipeline_name, DATE_TRUNC('day', execution_date), stage
        ORDER BY execution_date DESC, pipeline_name, stage
        """

    def _get_financial_kpis_hourly_query(self) -> str:
        """Get query for financial KPIs (hourly aggregation)"""
        return """
        WITH hourly_sales AS (
            SELECT 
                DATE_TRUNC('hour', created_at) as hour_bucket,
                SUM(amount) as revenue_total,
                COUNT(*) as transaction_count,
                COUNT(DISTINCT customer_id) as unique_customers,
                AVG(amount) as avg_transaction_value
            FROM sales_transactions 
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE_TRUNC('hour', created_at)
        ),
        hourly_costs AS (
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour_bucket,
                SUM(amount) as cost_total,
                COUNT(*) as cost_transactions
            FROM operational_costs 
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY DATE_TRUNC('hour', timestamp)
        )
        SELECT 
            COALESCE(s.hour_bucket, c.hour_bucket) as hour_bucket,
            COALESCE(s.revenue_total, 0) as revenue_total,
            COALESCE(c.cost_total, 0) as cost_total,
            COALESCE(s.revenue_total, 0) - COALESCE(c.cost_total, 0) as profit_total,
            CASE 
                WHEN COALESCE(c.cost_total, 0) > 0 THEN 
                    ROUND(((COALESCE(s.revenue_total, 0) - COALESCE(c.cost_total, 0)) / c.cost_total) * 100, 2)
                ELSE NULL 
            END as profit_margin_percent,
            COALESCE(s.transaction_count, 0) as transaction_count,
            COALESCE(s.unique_customers, 0) as unique_customers,
            COALESCE(s.avg_transaction_value, 0) as avg_transaction_value,
            CASE 
                WHEN COALESCE(s.unique_customers, 0) > 0 THEN 
                    COALESCE(s.revenue_total, 0) / s.unique_customers
                ELSE 0 
            END as revenue_per_customer,
            LAG(COALESCE(s.revenue_total, 0)) OVER (ORDER BY COALESCE(s.hour_bucket, c.hour_bucket)) as prev_hour_revenue,
            CASE 
                WHEN LAG(COALESCE(s.revenue_total, 0)) OVER (ORDER BY COALESCE(s.hour_bucket, c.hour_bucket)) > 0 THEN
                    ROUND(((COALESCE(s.revenue_total, 0) - LAG(COALESCE(s.revenue_total, 0)) OVER (ORDER BY COALESCE(s.hour_bucket, c.hour_bucket))) /
                           LAG(COALESCE(s.revenue_total, 0)) OVER (ORDER BY COALESCE(s.hour_bucket, c.hour_bucket))) * 100, 2)
                ELSE 0
            END as hour_over_hour_growth_percent,
            NOW() as last_updated
        FROM hourly_sales s
        FULL OUTER JOIN hourly_costs c ON s.hour_bucket = c.hour_bucket
        ORDER BY COALESCE(s.hour_bucket, c.hour_bucket) DESC
        """

    async def create_materialized_view(self, view_config: MaterializedViewConfig) -> bool:
        """Create materialized view with optimizations"""
        try:
            async with self.engine.begin() as conn:
                # Drop if exists
                await conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view_config.name} CASCADE"))
                
                # Create materialized view
                create_sql = f"""
                CREATE MATERIALIZED VIEW {view_config.name} AS
                {view_config.query}
                """
                
                if view_config.partition_by:
                    # Add partitioning if specified
                    create_sql += f" PARTITION BY RANGE ({view_config.partition_by})"
                
                await conn.execute(text(create_sql))
                
                # Create indexes
                for index_col in view_config.indexes:
                    index_name = f"idx_{view_config.name}_{index_col.replace(',', '_').replace(' ', '_')}"
                    index_sql = f"CREATE INDEX {index_name} ON {view_config.name} ({index_col})"
                    await conn.execute(text(index_sql))
                
                # Grant permissions
                await conn.execute(text(f"GRANT SELECT ON {view_config.name} TO PUBLIC"))
                
                self.metrics["views_created"] += 1
                self.logger.info(f"Created materialized view: {view_config.name}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to create materialized view {view_config.name}: {e}")
            return False

    async def refresh_materialized_view(self, view_name: str, concurrent: bool = True) -> bool:
        """Refresh materialized view"""
        try:
            start_time = datetime.now()
            
            async with self.engine.begin() as conn:
                refresh_sql = f"REFRESH MATERIALIZED VIEW"
                if concurrent:
                    refresh_sql += " CONCURRENTLY"
                refresh_sql += f" {view_name}"
                
                await conn.execute(text(refresh_sql))
                
            duration = (datetime.now() - start_time).total_seconds()
            
            # Track refresh history
            if view_name not in self.refresh_history:
                self.refresh_history[view_name] = []
            self.refresh_history[view_name].append(datetime.now())
            
            # Keep only last 100 refreshes
            self.refresh_history[view_name] = self.refresh_history[view_name][-100:]
            
            self.metrics["views_refreshed"] += 1
            self.metrics["refresh_duration_total"] += duration
            
            self.logger.info(f"Refreshed materialized view {view_name} in {duration:.2f}s")
            return True
            
        except Exception as e:
            self.metrics["refresh_errors"] += 1
            self.logger.error(f"Failed to refresh materialized view {view_name}: {e}")
            return False

    async def create_all_views(self) -> Dict[str, bool]:
        """Create all configured materialized views"""
        results = {}
        
        # Sort by priority (1 = highest priority)
        sorted_configs = sorted(
            self.views_config.items(),
            key=lambda x: x[1].priority
        )
        
        for view_name, config in sorted_configs:
            results[view_name] = await self.create_materialized_view(config)
            
        return results

    async def refresh_views_by_strategy(self, strategy: RefreshStrategy) -> Dict[str, bool]:
        """Refresh views based on their refresh strategy"""
        results = {}
        
        for view_name, config in self.views_config.items():
            if config.refresh_strategy == strategy or config.refresh_strategy == RefreshStrategy.HYBRID:
                results[view_name] = await self.refresh_materialized_view(view_name)
                
        return results

    async def get_view_statistics(self, view_name: str) -> Dict[str, Any]:
        """Get statistics for a materialized view"""
        try:
            async with self.engine.begin() as conn:
                # Get basic stats
                stats_sql = f"""
                SELECT 
                    schemaname,
                    matviewname,
                    matviewowner,
                    tablespace,
                    hasindexes,
                    ispopulated,
                    definition
                FROM pg_matviews 
                WHERE matviewname = '{view_name}'
                """
                
                result = await conn.execute(text(stats_sql))
                view_info = result.fetchone()
                
                if not view_info:
                    return {"error": "View not found"}
                
                # Get size information
                size_sql = f"""
                SELECT 
                    pg_size_pretty(pg_total_relation_size('{view_name}')) as total_size,
                    pg_size_pretty(pg_relation_size('{view_name}')) as table_size,
                    pg_size_pretty(pg_indexes_size('{view_name}')) as indexes_size
                """
                
                size_result = await conn.execute(text(size_sql))
                size_info = size_result.fetchone()
                
                # Get row count
                count_sql = f"SELECT COUNT(*) as row_count FROM {view_name}"
                count_result = await conn.execute(text(count_sql))
                row_count = count_result.fetchone()[0]
                
                return {
                    "view_name": view_name,
                    "schema": view_info[0],
                    "owner": view_info[2],
                    "has_indexes": view_info[4],
                    "is_populated": view_info[5],
                    "total_size": size_info[0],
                    "table_size": size_info[1],
                    "indexes_size": size_info[2],
                    "row_count": row_count,
                    "refresh_history": self.refresh_history.get(view_name, []),
                    "last_refresh": max(self.refresh_history.get(view_name, [datetime.min])),
                    "refresh_count": len(self.refresh_history.get(view_name, [])),
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get view statistics for {view_name}: {e}")
            return {"error": str(e)}

    async def optimize_view_performance(self, view_name: str) -> Dict[str, Any]:
        """Analyze and optimize materialized view performance"""
        try:
            recommendations = []
            
            stats = await self.get_view_statistics(view_name)
            if "error" in stats:
                return stats
                
            # Analyze refresh frequency
            refresh_times = self.refresh_history.get(view_name, [])
            if len(refresh_times) >= 2:
                avg_interval = sum(
                    (refresh_times[i] - refresh_times[i-1]).total_seconds()
                    for i in range(1, len(refresh_times))
                ) / (len(refresh_times) - 1)
                
                config = self.views_config.get(view_name)
                if config and avg_interval < config.refresh_interval_minutes * 60 * 0.8:
                    recommendations.append("Consider increasing refresh interval to reduce overhead")
                    
            # Analyze size
            if stats.get("row_count", 0) > 10000000:  # 10M rows
                recommendations.append("Consider partitioning for large tables")
                
            if not stats.get("has_indexes"):
                recommendations.append("Consider adding indexes for better query performance")
                
            return {
                "view_name": view_name,
                "performance_score": self._calculate_performance_score(stats),
                "recommendations": recommendations,
                "statistics": stats
            }
            
        except Exception as e:
            self.logger.error(f"Failed to optimize view {view_name}: {e}")
            return {"error": str(e)}

    def _calculate_performance_score(self, stats: Dict[str, Any]) -> int:
        """Calculate performance score (0-100) for a materialized view"""
        score = 100
        
        # Deduct points for large size without partitioning
        row_count = stats.get("row_count", 0)
        if row_count > 10000000:  # 10M rows
            score -= 20
        elif row_count > 1000000:  # 1M rows
            score -= 10
            
        # Deduct points for lack of indexes
        if not stats.get("has_indexes"):
            score -= 15
            
        # Deduct points for frequent refreshes with poor performance
        refresh_count = stats.get("refresh_count", 0)
        if refresh_count > 100:  # Many refreshes might indicate performance issues
            score -= 10
            
        return max(0, score)

    async def cleanup_old_data(self) -> Dict[str, int]:
        """Cleanup old data based on retention policies"""
        cleanup_results = {}
        
        for view_name, config in self.views_config.items():
            try:
                if config.retention_days > 0:
                    # This would depend on your specific schema
                    # For now, we'll just track the retention policy
                    cleanup_results[view_name] = config.retention_days
                    
            except Exception as e:
                self.logger.error(f"Failed to cleanup {view_name}: {e}")
                cleanup_results[view_name] = -1
                
        return cleanup_results

    def get_metrics(self) -> Dict[str, Any]:
        """Get materialized views manager metrics"""
        return {
            "materialized_views_metrics": self.metrics,
            "total_views_configured": len(self.views_config),
            "refresh_success_rate": (
                (self.metrics["views_refreshed"] / 
                 max(1, self.metrics["views_refreshed"] + self.metrics["refresh_errors"])) * 100
            ),
            "average_refresh_duration": (
                self.metrics["refresh_duration_total"] / 
                max(1, self.metrics["views_refreshed"])
            ),
            "timestamp": datetime.now().isoformat()
        }

    async def close(self):
        """Close database connections"""
        try:
            await self.engine.dispose()
            self.logger.info("Materialized views manager closed")
        except Exception as e:
            self.logger.warning(f"Error closing materialized views manager: {e}")


    def _get_daily_sales_kpi_query(self) -> str:
        """Get query for daily sales KPI with advanced analytics and anomaly detection"""
        return """
        WITH daily_aggregates AS (
            SELECT 
                d.date,
                d.fiscal_year,
                d.fiscal_quarter,
                COUNT(DISTINCT f.customer_id) as unique_customers,
                SUM(f.quantity_sold) as total_quantity,
                SUM(f.gross_revenue) as total_revenue,
                SUM(f.net_revenue) as net_revenue,
                SUM(f.cost_of_goods) as total_costs,
                AVG(f.average_order_value) as avg_order_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.gross_revenue) as median_order_value,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.gross_revenue) as p90_order_value
            FROM fact_sales_summary f
            JOIN dim_time d ON f.time_key = d.time_key
            WHERE d.date >= CURRENT_DATE - INTERVAL '2 years'
            GROUP BY d.date, d.fiscal_year, d.fiscal_quarter
        ),
        trend_analysis AS (
            SELECT 
                *,
                LAG(total_revenue, 1) OVER (ORDER BY date) as prev_day_revenue,
                LAG(total_revenue, 7) OVER (ORDER BY date) as same_day_last_week,
                AVG(total_revenue) OVER (
                    ORDER BY date 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as seven_day_avg_revenue,
                (total_revenue - LAG(total_revenue, 1) OVER (ORDER BY date)) / 
                NULLIF(LAG(total_revenue, 1) OVER (ORDER BY date), 0) * 100 as daily_growth_pct,
                (total_revenue - LAG(total_revenue, 7) OVER (ORDER BY date)) / 
                NULLIF(LAG(total_revenue, 7) OVER (ORDER BY date), 0) * 100 as wow_growth_pct,
                STDDEV(total_revenue) OVER (
                    ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as rolling_30d_stddev
            FROM daily_aggregates
        )
        SELECT 
            *,
            CASE 
                WHEN ABS(daily_growth_pct) > 25 THEN 'HIGH_VOLATILITY'
                WHEN ABS(daily_growth_pct) > 10 THEN 'MODERATE_VOLATILITY'
                ELSE 'STABLE'
            END as volatility_flag,
            CASE 
                WHEN total_revenue < seven_day_avg_revenue * 0.8 THEN 'UNDERPERFORMING'
                WHEN total_revenue > seven_day_avg_revenue * 1.2 THEN 'OVERPERFORMING'
                ELSE 'NORMAL'
            END as performance_flag,
            CASE 
                WHEN rolling_30d_stddev > 0 AND ABS(total_revenue - seven_day_avg_revenue) > 2 * rolling_30d_stddev
                THEN TRUE ELSE FALSE 
            END as is_anomaly,
            NOW() as last_updated
        FROM trend_analysis
        ORDER BY date
        """

    def _get_customer_segments_realtime_query(self) -> str:
        """Get query for real-time customer segmentation"""
        return """
        WITH customer_metrics AS (
            SELECT 
                c.customer_id,
                c.customer_name,
                c.customer_segment,
                COUNT(DISTINCT s.sale_id) as transaction_count_30d,
                SUM(s.total_amount) as total_spent_30d,
                AVG(s.total_amount) as avg_order_value,
                MAX(s.sale_date) as last_purchase_date,
                CURRENT_DATE - MAX(s.sale_date) as days_since_last_purchase,
                COUNT(DISTINCT s.product_id) as unique_products_purchased,
                SUM(s.quantity) as total_items_purchased
            FROM dim_customer c
            LEFT JOIN silver_sales_clean s ON c.customer_id = s.customer_id
                AND s.sale_date >= CURRENT_DATE - INTERVAL '30 days'
            WHERE c.is_current = TRUE
            GROUP BY c.customer_id, c.customer_name, c.customer_segment
        ),
        rfm_analysis AS (
            SELECT *,
                NTILE(5) OVER (ORDER BY days_since_last_purchase DESC) as recency_score,
                NTILE(5) OVER (ORDER BY transaction_count_30d) as frequency_score,
                NTILE(5) OVER (ORDER BY total_spent_30d) as monetary_score
            FROM customer_metrics
        ),
        segment_classification AS (
            SELECT *,
                CASE 
                    WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
                    WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
                    WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 4 THEN 'Big Spenders'
                    WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'New Customers'
                    WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Potential Loyalists'
                    WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
                    WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 4 THEN 'Cannot Lose Them'
                    WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Hibernating'
                    ELSE 'Others'
                END as rfm_segment,
                (recency_score + frequency_score + monetary_score) / 3.0 as customer_value_score
            FROM rfm_analysis
        )
        SELECT 
            rfm_segment as segment_name,
            COUNT(*) as customer_count,
            SUM(total_spent_30d) as segment_revenue_30d,
            AVG(total_spent_30d) as avg_customer_value,
            AVG(transaction_count_30d) as avg_transactions_per_customer,
            AVG(customer_value_score) as avg_value_score,
            COUNT(CASE WHEN days_since_last_purchase <= 7 THEN 1 END) as active_last_7d,
            COUNT(CASE WHEN days_since_last_purchase <= 30 THEN 1 END) as active_last_30d,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_spent_30d) as median_customer_value,
            NOW() as last_updated
        FROM segment_classification
        GROUP BY rfm_segment
        ORDER BY segment_revenue_30d DESC
        """

    def _get_anomaly_detection_kpis_query(self) -> str:
        """Get query for KPI anomaly detection with statistical analysis"""
        return """
        WITH kpi_history AS (
            SELECT 
                'hourly_revenue' as kpi_name,
                DATE_TRUNC('hour', created_at) as timestamp,
                SUM(total_amount) as value,
                'revenue' as category
            FROM silver_sales_clean 
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE_TRUNC('hour', created_at)
            
            UNION ALL
            
            SELECT 
                'active_customers' as kpi_name,
                DATE_TRUNC('hour', created_at) as timestamp,
                COUNT(DISTINCT customer_id) as value,
                'customer' as category
            FROM silver_sales_clean 
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE_TRUNC('hour', created_at)
            
            UNION ALL
            
            SELECT 
                'avg_order_value' as kpi_name,
                DATE_TRUNC('hour', created_at) as timestamp,
                AVG(total_amount) as value,
                'revenue' as category
            FROM silver_sales_clean 
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE_TRUNC('hour', created_at)
        ),
        statistical_analysis AS (
            SELECT 
                kpi_name,
                timestamp,
                value,
                category,
                AVG(value) OVER (
                    PARTITION BY kpi_name 
                    ORDER BY timestamp 
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) as rolling_24h_avg,
                STDDEV(value) OVER (
                    PARTITION BY kpi_name 
                    ORDER BY timestamp 
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) as rolling_24h_stddev,
                LAG(value, 1) OVER (PARTITION BY kpi_name ORDER BY timestamp) as prev_value,
                LAG(value, 24) OVER (PARTITION BY kpi_name ORDER BY timestamp) as same_hour_yesterday
            FROM kpi_history
        )
        SELECT 
            kpi_name,
            timestamp,
            value,
            category,
            rolling_24h_avg,
            rolling_24h_stddev,
            prev_value,
            same_hour_yesterday,
            CASE 
                WHEN rolling_24h_stddev > 0 
                THEN ABS(value - rolling_24h_avg) / rolling_24h_stddev 
                ELSE 0 
            END as z_score,
            CASE 
                WHEN rolling_24h_stddev > 0 AND ABS(value - rolling_24h_avg) / rolling_24h_stddev > 2.0
                THEN TRUE ELSE FALSE 
            END as is_anomaly,
            CASE 
                WHEN rolling_24h_stddev > 0 AND ABS(value - rolling_24h_avg) / rolling_24h_stddev > 3.0
                THEN 'HIGH'
                WHEN rolling_24h_stddev > 0 AND ABS(value - rolling_24h_avg) / rolling_24h_stddev > 2.0
                THEN 'MEDIUM'
                ELSE 'LOW'
            END as anomaly_severity,
            CASE 
                WHEN prev_value > 0 
                THEN (value - prev_value) / prev_value * 100 
                ELSE 0 
            END as hour_over_hour_pct,
            CASE 
                WHEN same_hour_yesterday > 0 
                THEN (value - same_hour_yesterday) / same_hour_yesterday * 100 
                ELSE 0 
            END as day_over_day_pct,
            NOW() as last_updated
        FROM statistical_analysis
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC, kpi_name
        """

    def _get_dashboard_performance_metrics_query(self) -> str:
        """Get query for dashboard performance metrics tracking with advanced SLA monitoring"""
        return """
        WITH api_performance AS (
            SELECT 
                DATE_TRUNC('minute', timestamp) as time_bucket,
                endpoint,
                method,
                COUNT(*) as request_count,
                AVG(response_time_ms) as avg_response_time,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY response_time_ms) as median_response_time,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) as p95_response_time,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time_ms) as p99_response_time,
                MIN(response_time_ms) as min_response_time,
                MAX(response_time_ms) as max_response_time,
                STDDEV(response_time_ms) as response_time_stddev,
                COUNT(CASE WHEN response_time_ms <= 25 THEN 1 END) as sub_25ms_requests,
                COUNT(CASE WHEN response_time_ms <= 500 THEN 1 END) as sub_500ms_requests,
                COUNT(CASE WHEN response_time_ms > 2000 THEN 1 END) as slow_requests,
                COUNT(CASE WHEN response_time_ms > 5000 THEN 1 END) as very_slow_requests,
                COUNT(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 END) as success_count,
                COUNT(CASE WHEN status_code = 429 THEN 1 END) as rate_limited_count,
                COUNT(CASE WHEN status_code >= 400 AND status_code < 500 THEN 1 END) as client_error_count,
                COUNT(CASE WHEN status_code >= 500 THEN 1 END) as server_error_count,
                SUM(request_size_bytes) as total_request_bytes,
                SUM(response_size_bytes) as total_response_bytes,
                AVG(cpu_usage_percent) as avg_cpu_usage,
                AVG(memory_usage_mb) as avg_memory_usage
            FROM api_logs 
            WHERE timestamp >= NOW() - INTERVAL '2 hours'
                AND endpoint LIKE '%dashboard%'
            GROUP BY DATE_TRUNC('minute', timestamp), endpoint, method
        ),
        cache_performance AS (
            SELECT 
                DATE_TRUNC('minute', timestamp) as time_bucket,
                'cache_performance' as endpoint,
                'GET' as method,
                COUNT(*) as request_count,
                AVG(CASE WHEN cache_hit THEN 5 ELSE response_time_ms END) as avg_response_time,
                PERCENTILE_CONT(0.50) WITHIN GROUP (
                    ORDER BY CASE WHEN cache_hit THEN 5 ELSE response_time_ms END
                ) as median_response_time,
                PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY CASE WHEN cache_hit THEN 5 ELSE response_time_ms END
                ) as p95_response_time,
                PERCENTILE_CONT(0.99) WITHIN GROUP (
                    ORDER BY CASE WHEN cache_hit THEN 5 ELSE response_time_ms END
                ) as p99_response_time,
                1 as min_response_time,
                MAX(CASE WHEN cache_hit THEN 5 ELSE response_time_ms END) as max_response_time,
                STDDEV(CASE WHEN cache_hit THEN 5 ELSE response_time_ms END) as response_time_stddev,
                COUNT(CASE WHEN cache_hit THEN 1 END) as sub_25ms_requests,
                COUNT(*) as sub_500ms_requests,
                0 as slow_requests,
                0 as very_slow_requests,
                COUNT(*) as success_count,
                0 as rate_limited_count,
                0 as client_error_count,
                0 as server_error_count,
                SUM(request_size_bytes) as total_request_bytes,
                SUM(response_size_bytes) as total_response_bytes,
                0 as avg_cpu_usage,
                0 as avg_memory_usage,
                COUNT(CASE WHEN cache_hit THEN 1 END) as cache_hits,
                COUNT(CASE WHEN NOT cache_hit THEN 1 END) as cache_misses,
                AVG(cache_ttl_seconds) as avg_cache_ttl,
                COUNT(CASE WHEN cache_hit AND cache_ttl_seconds < 60 THEN 1 END) as short_ttl_hits
            FROM cache_logs 
            WHERE timestamp >= NOW() - INTERVAL '2 hours'
            GROUP BY DATE_TRUNC('minute', timestamp)
        ),
        combined_metrics AS (
            SELECT 
                time_bucket, endpoint, method, request_count, 
                avg_response_time, median_response_time, p95_response_time, p99_response_time,
                min_response_time, max_response_time, response_time_stddev,
                sub_25ms_requests, sub_500ms_requests, slow_requests, very_slow_requests,
                success_count, rate_limited_count, client_error_count, server_error_count,
                total_request_bytes, total_response_bytes, avg_cpu_usage, avg_memory_usage,
                0 as cache_hits, 0 as cache_misses, 0 as avg_cache_ttl, 0 as short_ttl_hits
            FROM api_performance
            
            UNION ALL
            
            SELECT 
                time_bucket, endpoint, method, request_count, 
                avg_response_time, median_response_time, p95_response_time, p99_response_time,
                min_response_time, max_response_time, response_time_stddev,
                sub_25ms_requests, sub_500ms_requests, slow_requests, very_slow_requests,
                success_count, rate_limited_count, client_error_count, server_error_count,
                total_request_bytes, total_response_bytes, avg_cpu_usage, avg_memory_usage,
                cache_hits, cache_misses, avg_cache_ttl, short_ttl_hits
            FROM cache_performance
        )
        SELECT 
            time_bucket,
            endpoint,
            method,
            request_count,
            ROUND(avg_response_time::numeric, 2) as avg_response_time,
            ROUND(median_response_time::numeric, 2) as median_response_time,
            ROUND(p95_response_time::numeric, 2) as p95_response_time,
            ROUND(p99_response_time::numeric, 2) as p99_response_time,
            min_response_time,
            max_response_time,
            ROUND(response_time_stddev::numeric, 2) as response_time_stddev,
            sub_25ms_requests,
            sub_500ms_requests,
            slow_requests,
            very_slow_requests,
            success_count,
            rate_limited_count,
            client_error_count,
            server_error_count,
            -- SLA Calculations
            ROUND((sub_25ms_requests::DECIMAL / NULLIF(request_count, 0)) * 100, 2) as sub_25ms_percentage,
            ROUND((sub_500ms_requests::DECIMAL / NULLIF(request_count, 0)) * 100, 2) as sub_500ms_percentage,
            ROUND((success_count::DECIMAL / NULLIF(request_count, 0)) * 100, 2) as success_rate,
            ROUND(((request_count - slow_requests)::DECIMAL / NULLIF(request_count, 0)) * 100, 2) as performance_sla,
            -- Performance Grading
            CASE 
                WHEN p95_response_time <= 25 THEN 'EXCELLENT'
                WHEN p95_response_time <= 100 THEN 'VERY_GOOD'
                WHEN p95_response_time <= 500 THEN 'GOOD'
                WHEN p95_response_time <= 1000 THEN 'ACCEPTABLE'
                WHEN p95_response_time <= 2000 THEN 'POOR'
                ELSE 'CRITICAL'
            END as performance_grade,
            -- SLA Compliance
            CASE 
                WHEN (sub_25ms_requests::DECIMAL / NULLIF(request_count, 0)) >= 0.95 THEN 'EXCEEDS_SLA'
                WHEN (sub_500ms_requests::DECIMAL / NULLIF(request_count, 0)) >= 0.95 THEN 'MEETS_SLA'
                WHEN (sub_500ms_requests::DECIMAL / NULLIF(request_count, 0)) >= 0.90 THEN 'NEAR_SLA'
                ELSE 'BELOW_SLA'
            END as sla_status,
            -- Cache Metrics
            COALESCE(cache_hits, 0) as cache_hits,
            COALESCE(cache_misses, 0) as cache_misses,
            CASE 
                WHEN COALESCE(cache_hits, 0) + COALESCE(cache_misses, 0) > 0
                THEN ROUND((COALESCE(cache_hits, 0)::DECIMAL / 
                    (COALESCE(cache_hits, 0) + COALESCE(cache_misses, 0))) * 100, 2)
                ELSE 0
            END as cache_hit_rate,
            ROUND(avg_cache_ttl::numeric, 0) as avg_cache_ttl_seconds,
            short_ttl_hits,
            -- Resource Usage
            ROUND(total_request_bytes / 1024.0 / 1024.0, 2) as total_request_mb,
            ROUND(total_response_bytes / 1024.0 / 1024.0, 2) as total_response_mb,
            ROUND(avg_cpu_usage::numeric, 1) as avg_cpu_usage_percent,
            ROUND(avg_memory_usage::numeric, 1) as avg_memory_usage_mb,
            -- Alerting Thresholds
            CASE 
                WHEN p95_response_time > 2000 OR (success_count::DECIMAL / NULLIF(request_count, 0)) < 0.95
                THEN 'ALERT_REQUIRED'
                WHEN p95_response_time > 1000 OR (success_count::DECIMAL / NULLIF(request_count, 0)) < 0.98
                THEN 'WARNING'
                ELSE 'HEALTHY'
            END as health_status,
            -- Trend Analysis (compared to previous hour)
            LAG(avg_response_time) OVER (
                PARTITION BY endpoint, method 
                ORDER BY time_bucket
            ) as prev_avg_response_time,
            CASE 
                WHEN LAG(avg_response_time) OVER (
                    PARTITION BY endpoint, method ORDER BY time_bucket
                ) > 0 
                THEN ROUND(((avg_response_time - LAG(avg_response_time) OVER (
                    PARTITION BY endpoint, method ORDER BY time_bucket
                )) / LAG(avg_response_time) OVER (
                    PARTITION BY endpoint, method ORDER BY time_bucket
                )) * 100, 1)
                ELSE 0
            END as response_time_trend_pct,
            NOW() as last_updated
        FROM combined_metrics
        ORDER BY time_bucket DESC, request_count DESC
        """


# Factory function
def create_materialized_views_manager() -> MaterializedViewsManager:
    """Create MaterializedViewsManager instance"""
    return MaterializedViewsManager()


# Example usage and testing
async def main():
    """Example usage of materialized views manager"""
    manager = create_materialized_views_manager()
    
    try:
        print("Creating all materialized views...")
        results = await manager.create_all_views()
        
        for view_name, success in results.items():
            status = "✅" if success else "❌"
            print(f"{status} {view_name}")
            
        print("\nRefreshing immediate refresh views...")
        refresh_results = await manager.refresh_views_by_strategy(RefreshStrategy.IMMEDIATE)
        
        for view_name, success in refresh_results.items():
            status = "✅" if success else "❌"
            print(f"{status} Refreshed {view_name}")
            
        # Get statistics for a specific view
        print("\nGetting view statistics...")
        stats = await manager.get_view_statistics("executive_revenue_kpis")
        if "error" not in stats:
            print(f"View: {stats['view_name']}")
            print(f"Row count: {stats['row_count']:,}")
            print(f"Total size: {stats['total_size']}")
            print(f"Has indexes: {stats['has_indexes']}")
            
        # Get performance optimization recommendations
        print("\nAnalyzing performance...")
        perf_analysis = await manager.optimize_view_performance("executive_revenue_kpis")
        if "error" not in perf_analysis:
            print(f"Performance score: {perf_analysis['performance_score']}/100")
            if perf_analysis['recommendations']:
                print("Recommendations:")
                for rec in perf_analysis['recommendations']:
                    print(f"  - {rec}")
                    
        # Show metrics
        metrics = manager.get_metrics()
        print(f"\nMetrics:")
        print(f"Views created: {metrics['materialized_views_metrics']['views_created']}")
        print(f"Views refreshed: {metrics['materialized_views_metrics']['views_refreshed']}")
        print(f"Refresh success rate: {metrics['refresh_success_rate']:.1f}%")
        print(f"Average refresh duration: {metrics['average_refresh_duration']:.2f}s")
        
    finally:
        await manager.close()


if __name__ == "__main__":
    asyncio.run(main())