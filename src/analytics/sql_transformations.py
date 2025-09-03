"""
Advanced SQL Transformations and Analytical Functions

Enterprise-grade SQL transformation engine with:
- Complex analytical queries with window functions
- Advanced CTEs and recursive queries
- Performance-optimized data transformations
- Business intelligence calculations
- Data warehouse analytical patterns
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from core.database.performance import DatabasePerformanceOptimizer
from core.logging import get_logger
from data_access.db import get_engine

logger = get_logger(__name__)


class AnalyticalFunction(str, Enum):
    """Types of analytical functions."""

    RANKING = "ranking"
    WINDOWING = "windowing"
    AGGREGATION = "aggregation"
    TIME_SERIES = "time_series"
    STATISTICAL = "statistical"
    BUSINESS_METRICS = "business_metrics"


class TransformationType(str, Enum):
    """Types of SQL transformations."""

    BRONZE_TO_SILVER = "bronze_to_silver"
    SILVER_TO_GOLD = "silver_to_gold"
    ANALYTICAL_MART = "analytical_mart"
    KPI_CALCULATION = "kpi_calculation"
    DIMENSION_ENRICHMENT = "dimension_enrichment"


@dataclass
class SQLTransformation:
    """SQL transformation definition."""

    name: str
    sql: str
    transformation_type: TransformationType
    analytical_functions: list[AnalyticalFunction]
    depends_on: list[str]
    output_table: str
    is_incremental: bool = False
    partition_by: list[str] | None = None
    cluster_by: list[str] | None = None
    materialization: str = "table"  # table, view, incremental
    pre_hooks: list[str] | None = None
    post_hooks: list[str] | None = None
    tags: list[str] | None = None
    description: str = ""


@dataclass
class TransformationResult:
    """Result of SQL transformation execution."""

    name: str
    success: bool
    rows_affected: int
    execution_time_ms: float
    query_complexity: str
    error_message: str | None = None
    performance_metrics: dict[str, Any] | None = None
    timestamp: datetime = datetime.utcnow()


class SQLTransformationEngine:
    """
    Advanced SQL transformation engine with enterprise features.

    Features:
    - Complex analytical queries with window functions
    - Incremental processing with CDC
    - Performance monitoring and optimization
    - Automated testing and validation
    - Data lineage tracking
    """

    def __init__(self, engine: Engine | None = None):
        self.engine = engine or get_engine()
        self.performance_optimizer = DatabasePerformanceOptimizer(self.engine)
        self.transformations: dict[str, SQLTransformation] = {}
        self.execution_history: list[TransformationResult] = []

    def register_transformation(self, transformation: SQLTransformation) -> None:
        """Register a SQL transformation."""
        self.transformations[transformation.name] = transformation
        logger.info(f"Registered transformation: {transformation.name}")

    def get_retail_analytics_transformations(self) -> list[SQLTransformation]:
        """Get pre-built retail analytics transformations."""
        transformations = []

        # 1. Customer Lifetime Value (CLV) Analysis
        clv_sql = """
        WITH customer_metrics AS (
            SELECT
                c.customer_key,
                c.customer_id,
                c.customer_segment,
                c.country,
                COUNT(DISTINCT f.invoice_key) as total_orders,
                COUNT(DISTINCT f.date_key) as active_days,
                SUM(f.total_amount) as total_revenue,
                AVG(f.total_amount) as avg_order_value,
                MIN(d.date_actual) as first_purchase_date,
                MAX(d.date_actual) as last_purchase_date,
                DATEDIFF(day, MIN(d.date_actual), MAX(d.date_actual)) as customer_lifespan_days
            FROM fact_sale f
            INNER JOIN dim_customer c ON f.customer_key = c.customer_key
            INNER JOIN dim_date d ON f.date_key = d.date_key
            WHERE c.is_current = 1
            GROUP BY c.customer_key, c.customer_id, c.customer_segment, c.country
        ),
        clv_calculation AS (
            SELECT
                *,
                CASE
                    WHEN customer_lifespan_days > 0
                    THEN total_revenue * (365.0 / customer_lifespan_days)
                    ELSE total_revenue
                END as annualized_revenue,
                CASE
                    WHEN total_orders > 0
                    THEN customer_lifespan_days / total_orders
                    ELSE NULL
                END as avg_days_between_orders,
                -- Advanced CLV with probability of retention
                CASE
                    WHEN customer_lifespan_days > 365 THEN 0.8
                    WHEN customer_lifespan_days > 180 THEN 0.6
                    WHEN customer_lifespan_days > 90 THEN 0.4
                    ELSE 0.2
                END as retention_probability
            FROM customer_metrics
        ),
        clv_final AS (
            SELECT
                *,
                annualized_revenue * retention_probability * 2 as predicted_clv_2year,
                ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
                NTILE(10) OVER (ORDER BY total_revenue) as revenue_decile,
                CASE
                    WHEN total_revenue >= PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_revenue) OVER()
                    THEN 'VIP'
                    WHEN total_revenue >= PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY total_revenue) OVER()
                    THEN 'High Value'
                    WHEN total_revenue >= PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue) OVER()
                    THEN 'Medium Value'
                    ELSE 'Low Value'
                END as customer_value_segment
            FROM clv_calculation
        )
        SELECT * FROM clv_final
        """

        transformations.append(
            SQLTransformation(
                name="customer_lifetime_value_analysis",
                sql=clv_sql,
                transformation_type=TransformationType.ANALYTICAL_MART,
                analytical_functions=[
                    AnalyticalFunction.WINDOWING,
                    AnalyticalFunction.RANKING,
                    AnalyticalFunction.STATISTICAL,
                ],
                depends_on=["fact_sale", "dim_customer", "dim_date"],
                output_table="gold_customer_clv_analysis",
                materialization="table",
                description="Comprehensive customer lifetime value analysis with segmentation",
            )
        )

        # 2. Product Performance Analytics with Advanced Cohorts
        product_analytics_sql = """
        WITH product_sales_base AS (
            SELECT
                p.product_key,
                p.stock_code,
                p.description,
                p.category,
                p.unit_price,
                d.date_actual,
                d.year_number,
                d.quarter_number,
                d.month_number,
                d.week_number,
                f.quantity,
                f.total_amount,
                c.country,
                c.customer_segment,
                ROW_NUMBER() OVER (PARTITION BY p.product_key ORDER BY d.date_actual) as product_sale_sequence
            FROM fact_sale f
            INNER JOIN dim_product p ON f.product_key = p.product_key
            INNER JOIN dim_date d ON f.date_key = d.date_key
            INNER JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE p.is_current = 1 AND c.is_current = 1
        ),
        product_cohort_analysis AS (
            SELECT
                product_key,
                stock_code,
                description,
                category,
                -- Time-based analytics
                COUNT(*) as total_transactions,
                COUNT(DISTINCT customer_segment) as customer_segments_reached,
                COUNT(DISTINCT country) as countries_sold,
                SUM(quantity) as total_quantity_sold,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction_value,
                STDDEV(total_amount) as revenue_volatility,

                -- Performance by time periods
                SUM(CASE WHEN quarter_number = 1 THEN total_amount ELSE 0 END) as q1_revenue,
                SUM(CASE WHEN quarter_number = 2 THEN total_amount ELSE 0 END) as q2_revenue,
                SUM(CASE WHEN quarter_number = 3 THEN total_amount ELSE 0 END) as q3_revenue,
                SUM(CASE WHEN quarter_number = 4 THEN total_amount ELSE 0 END) as q4_revenue,

                -- Growth analytics
                FIRST_VALUE(total_amount) OVER (
                    PARTITION BY product_key
                    ORDER BY date_actual
                    ROWS UNBOUNDED PRECEDING
                ) as first_sale_amount,
                LAST_VALUE(total_amount) OVER (
                    PARTITION BY product_key
                    ORDER BY date_actual
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as last_sale_amount,

                -- Trend analysis
                LAG(SUM(total_amount), 1) OVER (
                    PARTITION BY product_key
                    ORDER BY month_number
                ) as prev_month_revenue,

                LEAD(SUM(total_amount), 1) OVER (
                    PARTITION BY product_key
                    ORDER BY month_number
                ) as next_month_revenue

            FROM product_sales_base
            GROUP BY product_key, stock_code, description, category, month_number
        ),
        product_performance_metrics AS (
            SELECT
                *,
                -- Performance rankings
                ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
                ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) as quantity_rank,
                ROW_NUMBER() OVER (ORDER BY avg_transaction_value DESC) as avg_value_rank,

                -- Percentile rankings
                PERCENT_RANK() OVER (ORDER BY total_revenue) as revenue_percentile,
                NTILE(5) OVER (ORDER BY total_revenue) as revenue_quintile,

                -- Growth calculations
                CASE
                    WHEN prev_month_revenue > 0
                    THEN ((total_revenue - prev_month_revenue) / prev_month_revenue) * 100
                    ELSE 0
                END as month_over_month_growth_pct,

                -- Seasonality indicators
                GREATEST(q1_revenue, q2_revenue, q3_revenue, q4_revenue) /
                NULLIF(LEAST(q1_revenue, q2_revenue, q3_revenue, q4_revenue), 0) as seasonality_ratio,

                -- Performance classification
                CASE
                    WHEN total_revenue >= PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_revenue) OVER()
                    THEN 'Star'
                    WHEN total_revenue >= PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY total_revenue) OVER()
                    AND month_over_month_growth_pct > 5
                    THEN 'Rising Star'
                    WHEN total_revenue >= PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY total_revenue) OVER()
                    THEN 'Core'
                    ELSE 'Tail'
                END as product_classification

            FROM product_cohort_analysis
        )
        SELECT * FROM product_performance_metrics
        """

        transformations.append(
            SQLTransformation(
                name="product_performance_analytics",
                sql=product_analytics_sql,
                transformation_type=TransformationType.ANALYTICAL_MART,
                analytical_functions=[
                    AnalyticalFunction.WINDOWING,
                    AnalyticalFunction.TIME_SERIES,
                    AnalyticalFunction.STATISTICAL,
                    AnalyticalFunction.RANKING,
                ],
                depends_on=["fact_sale", "dim_product", "dim_date", "dim_customer"],
                output_table="gold_product_performance_analytics",
                materialization="table",
                description="Advanced product performance analytics with cohort and trend analysis",
            )
        )

        # 3. Advanced Time Series Analysis with Forecasting
        time_series_sql = """
        WITH daily_sales_base AS (
            SELECT
                d.date_actual,
                d.year_number,
                d.quarter_number,
                d.month_number,
                d.week_number,
                d.day_of_week,
                d.is_weekend,
                d.is_holiday,
                COUNT(DISTINCT f.invoice_key) as daily_transactions,
                COUNT(DISTINCT f.customer_key) as daily_unique_customers,
                COUNT(DISTINCT f.product_key) as daily_unique_products,
                SUM(f.quantity) as daily_quantity,
                SUM(f.total_amount) as daily_revenue,
                AVG(f.total_amount) as daily_avg_order_value
            FROM fact_sale f
            INNER JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY
                d.date_actual, d.year_number, d.quarter_number,
                d.month_number, d.week_number, d.day_of_week,
                d.is_weekend, d.is_holiday
        ),
        time_series_analytics AS (
            SELECT
                *,
                -- Moving averages for trend analysis
                AVG(daily_revenue) OVER (
                    ORDER BY date_actual
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as revenue_7day_ma,

                AVG(daily_revenue) OVER (
                    ORDER BY date_actual
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as revenue_30day_ma,

                -- Year-over-year comparison
                LAG(daily_revenue, 365) OVER (ORDER BY date_actual) as revenue_same_day_last_year,

                -- Week-over-week comparison
                LAG(daily_revenue, 7) OVER (ORDER BY date_actual) as revenue_same_day_last_week,

                -- Month-over-month (approximate)
                LAG(daily_revenue, 30) OVER (ORDER BY date_actual) as revenue_same_day_last_month,

                -- Rolling volatility (standard deviation)
                STDDEV(daily_revenue) OVER (
                    ORDER BY date_actual
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as revenue_30day_volatility,

                -- Cumulative metrics
                SUM(daily_revenue) OVER (
                    PARTITION BY year_number
                    ORDER BY date_actual
                ) as ytd_revenue,

                SUM(daily_revenue) OVER (
                    PARTITION BY year_number, quarter_number
                    ORDER BY date_actual
                ) as qtd_revenue,

                -- Growth rates
                CASE
                    WHEN LAG(daily_revenue, 1) OVER (ORDER BY date_actual) > 0
                    THEN ((daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY date_actual)) /
                          LAG(daily_revenue, 1) OVER (ORDER BY date_actual)) * 100
                    ELSE 0
                END as daily_growth_rate_pct,

                -- Seasonal decomposition indicators
                AVG(daily_revenue) OVER (PARTITION BY day_of_week) as avg_revenue_by_dow,
                AVG(daily_revenue) OVER (PARTITION BY month_number) as avg_revenue_by_month,

                -- Anomaly detection using z-score
                (daily_revenue - AVG(daily_revenue) OVER()) /
                NULLIF(STDDEV(daily_revenue) OVER(), 0) as revenue_z_score

            FROM daily_sales_base
        ),
        forecasting_features AS (
            SELECT
                *,
                -- Growth trend indicators
                CASE
                    WHEN revenue_7day_ma > revenue_30day_ma THEN 'Accelerating'
                    WHEN revenue_7day_ma < revenue_30day_ma * 0.95 THEN 'Declining'
                    ELSE 'Stable'
                END as short_term_trend,

                -- Year-over-year growth
                CASE
                    WHEN revenue_same_day_last_year > 0
                    THEN ((daily_revenue - revenue_same_day_last_year) / revenue_same_day_last_year) * 100
                    ELSE NULL
                END as yoy_growth_pct,

                -- Seasonal strength
                ABS(daily_revenue - avg_revenue_by_dow) / NULLIF(avg_revenue_by_dow, 0) as dow_seasonality_strength,
                ABS(daily_revenue - avg_revenue_by_month) / NULLIF(avg_revenue_by_month, 0) as monthly_seasonality_strength,

                -- Anomaly classification
                CASE
                    WHEN ABS(revenue_z_score) > 3 THEN 'Extreme Outlier'
                    WHEN ABS(revenue_z_score) > 2 THEN 'Moderate Outlier'
                    WHEN ABS(revenue_z_score) > 1 THEN 'Mild Outlier'
                    ELSE 'Normal'
                END as anomaly_classification,

                -- Business day adjustments
                CASE
                    WHEN is_weekend = 1 THEN daily_revenue * 1.4  -- Weekend adjustment
                    WHEN is_holiday = 1 THEN daily_revenue * 0.8  -- Holiday adjustment
                    ELSE daily_revenue
                END as normalized_daily_revenue

            FROM time_series_analytics
        )
        SELECT * FROM forecasting_features
        ORDER BY date_actual
        """

        transformations.append(
            SQLTransformation(
                name="advanced_time_series_analysis",
                sql=time_series_sql,
                transformation_type=TransformationType.ANALYTICAL_MART,
                analytical_functions=[
                    AnalyticalFunction.TIME_SERIES,
                    AnalyticalFunction.WINDOWING,
                    AnalyticalFunction.STATISTICAL,
                ],
                depends_on=["fact_sale", "dim_date"],
                output_table="gold_time_series_analysis",
                materialization="table",
                partition_by=["year_number", "quarter_number"],
                description="Advanced time series analysis with forecasting features and anomaly detection",
            )
        )

        # 4. Customer Cohort and Retention Analysis
        cohort_analysis_sql = """
        WITH customer_first_purchase AS (
            SELECT
                c.customer_key,
                c.customer_id,
                c.customer_segment,
                c.country,
                MIN(d.date_actual) as first_purchase_date,
                DATE_TRUNC('month', MIN(d.date_actual)) as cohort_month
            FROM fact_sale f
            INNER JOIN dim_customer c ON f.customer_key = c.customer_key
            INNER JOIN dim_date d ON f.date_key = d.date_key
            WHERE c.is_current = 1
            GROUP BY c.customer_key, c.customer_id, c.customer_segment, c.country
        ),
        customer_monthly_activity AS (
            SELECT
                cfp.customer_key,
                cfp.cohort_month,
                cfp.customer_segment,
                cfp.country,
                DATE_TRUNC('month', d.date_actual) as activity_month,
                DATEDIFF(month, cfp.cohort_month, DATE_TRUNC('month', d.date_actual)) as period_number,
                COUNT(DISTINCT f.invoice_key) as monthly_orders,
                SUM(f.total_amount) as monthly_revenue,
                COUNT(DISTINCT f.product_key) as monthly_unique_products
            FROM customer_first_purchase cfp
            INNER JOIN fact_sale f ON cfp.customer_key = f.customer_key
            INNER JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY
                cfp.customer_key, cfp.cohort_month, cfp.customer_segment,
                cfp.country, DATE_TRUNC('month', d.date_actual)
        ),
        cohort_retention_metrics AS (
            SELECT
                cohort_month,
                customer_segment,
                country,
                period_number,
                COUNT(DISTINCT customer_key) as active_customers,
                SUM(monthly_revenue) as cohort_revenue,
                AVG(monthly_revenue) as avg_customer_revenue,
                SUM(monthly_orders) as total_orders,
                AVG(monthly_orders) as avg_customer_orders,

                -- Retention rate calculation
                COUNT(DISTINCT customer_key) * 100.0 /
                FIRST_VALUE(COUNT(DISTINCT customer_key)) OVER (
                    PARTITION BY cohort_month, customer_segment, country
                    ORDER BY period_number
                    ROWS UNBOUNDED PRECEDING
                ) as retention_rate_pct,

                -- Revenue retention
                SUM(monthly_revenue) * 100.0 /
                FIRST_VALUE(SUM(monthly_revenue)) OVER (
                    PARTITION BY cohort_month, customer_segment, country
                    ORDER BY period_number
                    ROWS UNBOUNDED PRECEDING
                ) as revenue_retention_rate_pct,

                -- Cohort size (always from period 0)
                FIRST_VALUE(COUNT(DISTINCT customer_key)) OVER (
                    PARTITION BY cohort_month, customer_segment, country
                    ORDER BY period_number
                    ROWS UNBOUNDED PRECEDING
                ) as cohort_size

            FROM customer_monthly_activity
            GROUP BY cohort_month, customer_segment, country, period_number
        ),
        cohort_analysis_enhanced AS (
            SELECT
                *,
                -- Churn indicators
                CASE
                    WHEN period_number > 0 THEN
                        LAG(retention_rate_pct, 1) OVER (
                            PARTITION BY cohort_month, customer_segment, country
                            ORDER BY period_number
                        ) - retention_rate_pct
                    ELSE 0
                END as churn_rate_pct,

                -- Lifetime value indicators
                SUM(cohort_revenue) OVER (
                    PARTITION BY cohort_month, customer_segment, country
                    ORDER BY period_number
                    ROWS UNBOUNDED PRECEDING
                ) as cumulative_cohort_ltv,

                -- Cohort performance classification
                CASE
                    WHEN period_number = 12 AND retention_rate_pct >= 50 THEN 'Excellent'
                    WHEN period_number = 12 AND retention_rate_pct >= 30 THEN 'Good'
                    WHEN period_number = 12 AND retention_rate_pct >= 15 THEN 'Average'
                    WHEN period_number = 12 AND retention_rate_pct < 15 THEN 'Poor'
                    ELSE 'In Progress'
                END as cohort_performance_rating,

                -- Time-based analytics
                CASE
                    WHEN period_number BETWEEN 0 AND 3 THEN 'Early Stage'
                    WHEN period_number BETWEEN 4 AND 12 THEN 'Growth Stage'
                    WHEN period_number > 12 THEN 'Mature Stage'
                END as cohort_stage

            FROM cohort_retention_metrics
        )
        SELECT * FROM cohort_analysis_enhanced
        ORDER BY cohort_month, customer_segment, country, period_number
        """

        transformations.append(
            SQLTransformation(
                name="customer_cohort_retention_analysis",
                sql=cohort_analysis_sql,
                transformation_type=TransformationType.ANALYTICAL_MART,
                analytical_functions=[
                    AnalyticalFunction.WINDOWING,
                    AnalyticalFunction.TIME_SERIES,
                    AnalyticalFunction.STATISTICAL,
                ],
                depends_on=["fact_sale", "dim_customer", "dim_date"],
                output_table="gold_customer_cohort_analysis",
                materialization="table",
                partition_by=["cohort_month"],
                description="Advanced customer cohort analysis with retention and lifetime value tracking",
            )
        )

        return transformations

    def get_statistical_functions(self) -> list[SQLTransformation]:
        """Get statistical analysis transformations."""
        transformations = []

        # Advanced Statistical Analysis
        stats_sql = """
        WITH sales_statistics AS (
            SELECT
                c.customer_segment,
                c.country,
                p.category,
                d.year_number,
                d.quarter_number,

                -- Basic statistics
                COUNT(*) as transaction_count,
                COUNT(DISTINCT c.customer_key) as unique_customers,
                SUM(f.total_amount) as total_revenue,
                AVG(f.total_amount) as mean_transaction_value,

                -- Advanced statistics
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_amount) as median_transaction_value,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.total_amount) as q1_transaction_value,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.total_amount) as q3_transaction_value,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.total_amount) as p90_transaction_value,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.total_amount) as p95_transaction_value,

                -- Variability measures
                STDDEV(f.total_amount) as stddev_transaction_value,
                VAR_POP(f.total_amount) as variance_transaction_value,

                -- Extreme values
                MIN(f.total_amount) as min_transaction_value,
                MAX(f.total_amount) as max_transaction_value,

                -- Statistical distribution analysis
                (AVG(f.total_amount) - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_amount)) /
                NULLIF(STDDEV(f.total_amount), 0) as skewness_indicator,

                -- Coefficient of variation
                STDDEV(f.total_amount) / NULLIF(AVG(f.total_amount), 0) as coefficient_of_variation

            FROM fact_sale f
            INNER JOIN dim_customer c ON f.customer_key = c.customer_key
            INNER JOIN dim_product p ON f.product_key = p.product_key
            INNER JOIN dim_date d ON f.date_key = d.date_key
            WHERE c.is_current = 1 AND p.is_current = 1
            GROUP BY c.customer_segment, c.country, p.category, d.year_number, d.quarter_number
        ),
        statistical_analysis AS (
            SELECT
                *,
                -- Interquartile range
                q3_transaction_value - q1_transaction_value as iqr_transaction_value,

                -- Outlier bounds using IQR method
                q1_transaction_value - 1.5 * (q3_transaction_value - q1_transaction_value) as lower_outlier_bound,
                q3_transaction_value + 1.5 * (q3_transaction_value - q1_transaction_value) as upper_outlier_bound,

                -- Distribution shape indicators
                CASE
                    WHEN ABS(skewness_indicator) < 0.5 THEN 'Symmetric'
                    WHEN skewness_indicator > 0.5 THEN 'Right Skewed'
                    WHEN skewness_indicator < -0.5 THEN 'Left Skewed'
                    ELSE 'Moderately Skewed'
                END as distribution_shape,

                -- Variability classification
                CASE
                    WHEN coefficient_of_variation < 0.3 THEN 'Low Variability'
                    WHEN coefficient_of_variation < 0.7 THEN 'Moderate Variability'
                    ELSE 'High Variability'
                END as variability_level,

                -- Performance percentile ranking
                PERCENT_RANK() OVER (ORDER BY total_revenue) as revenue_percentile_rank,
                NTILE(10) OVER (ORDER BY total_revenue) as revenue_decile,

                -- Statistical significance indicators
                CASE
                    WHEN transaction_count >= 30 THEN 'Statistically Significant'
                    WHEN transaction_count >= 10 THEN 'Moderately Significant'
                    ELSE 'Limited Sample Size'
                END as statistical_significance

            FROM sales_statistics
        )
        SELECT * FROM statistical_analysis
        """

        transformations.append(
            SQLTransformation(
                name="advanced_statistical_analysis",
                sql=stats_sql,
                transformation_type=TransformationType.ANALYTICAL_MART,
                analytical_functions=[AnalyticalFunction.STATISTICAL, AnalyticalFunction.RANKING],
                depends_on=["fact_sale", "dim_customer", "dim_product", "dim_date"],
                output_table="gold_statistical_analysis",
                materialization="table",
                description="Advanced statistical analysis with distribution analysis and significance testing",
            )
        )

        return transformations

    async def execute_transformation(
        self, name: str, validate_first: bool = True
    ) -> TransformationResult:
        """Execute a SQL transformation with performance monitoring."""
        if name not in self.transformations:
            raise ValueError(f"Transformation '{name}' not found")

        transformation = self.transformations[name]
        start_time = datetime.utcnow()

        try:
            # Validate dependencies if requested
            if validate_first:
                await self._validate_dependencies(transformation)

            # Execute pre-hooks
            if transformation.pre_hooks:
                await self._execute_hooks(transformation.pre_hooks, "pre")

            # Monitor query performance
            query_metric = await self.performance_optimizer.monitor_query_performance(
                transformation.sql, f"transformation_{name}"
            )

            # Execute main transformation
            rows_affected = await self._execute_transformation_sql(transformation)

            # Execute post-hooks
            if transformation.post_hooks:
                await self._execute_hooks(transformation.post_hooks, "post")

            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            result = TransformationResult(
                name=name,
                success=True,
                rows_affected=rows_affected,
                execution_time_ms=execution_time,
                query_complexity=query_metric.complexity.value,
                performance_metrics={
                    "execution_plan": query_metric.execution_plan,
                    "affected_rows": query_metric.affected_rows,
                    "query_id": query_metric.query_id,
                },
            )

            self.execution_history.append(result)
            logger.info(f"Transformation '{name}' completed successfully in {execution_time:.2f}ms")

            return result

        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            error_msg = str(e)

            result = TransformationResult(
                name=name,
                success=False,
                rows_affected=0,
                execution_time_ms=execution_time,
                query_complexity="unknown",
                error_message=error_msg,
            )

            self.execution_history.append(result)
            logger.error(f"Transformation '{name}' failed: {error_msg}")

            raise

    async def _validate_dependencies(self, transformation: SQLTransformation) -> None:
        """Validate that all dependencies exist."""
        for dep in transformation.depends_on:
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT 1 FROM {dep} LIMIT 1"))
                    result.fetchone()
            except SQLAlchemyError as e:
                raise ValueError(f"Dependency '{dep}' not available: {str(e)}") from e

    async def _execute_hooks(self, hooks: list[str], hook_type: str) -> None:
        """Execute pre/post hooks."""
        for hook in hooks:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(hook))
                    conn.commit()
                logger.debug(f"Executed {hook_type}-hook: {hook[:50]}...")
            except SQLAlchemyError as e:
                logger.error(f"Failed to execute {hook_type}-hook: {str(e)}")
                raise

    async def _execute_transformation_sql(self, transformation: SQLTransformation) -> int:
        """Execute the main transformation SQL."""
        if transformation.materialization == "table":
            # Drop and recreate table
            drop_sql = f"DROP TABLE IF EXISTS {transformation.output_table}"
            create_sql = f"CREATE TABLE {transformation.output_table} AS {transformation.sql}"

            with self.engine.connect() as conn:
                conn.execute(text(drop_sql))
                conn.execute(text(create_sql))
                conn.commit()

                # Get row count
                count_result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {transformation.output_table}")
                )
                row_count = count_result.scalar()

                return row_count

        elif transformation.materialization == "view":
            # Create or replace view
            view_sql = (
                f"CREATE OR REPLACE VIEW {transformation.output_table} AS {transformation.sql}"
            )

            with self.engine.connect() as conn:
                conn.execute(text(view_sql))
                conn.commit()

                return 0  # Views don't have row counts in the same way

        elif transformation.materialization == "incremental":
            # Handle incremental processing
            return await self._execute_incremental_transformation(transformation)

        else:
            raise ValueError(f"Unknown materialization type: {transformation.materialization}")

    async def _execute_incremental_transformation(self, transformation: SQLTransformation) -> int:
        """Execute incremental transformation with change data capture."""
        # This would implement incremental processing logic
        # For now, implement as a full refresh with timestamp tracking

        timestamp_col = "updated_at"

        # Get the last processed timestamp
        try:
            with self.engine.connect() as conn:
                last_update_result = conn.execute(
                    text(f"SELECT MAX({timestamp_col}) FROM {transformation.output_table}")
                )
                last_update = last_update_result.scalar()
        except SQLAlchemyError:
            # Table doesn't exist yet, process all data
            last_update = None

        # Build incremental SQL
        if last_update:
            incremental_sql = f"""
            INSERT INTO {transformation.output_table}
            {transformation.sql}
            WHERE {timestamp_col} > '{last_update}'
            """
        else:
            # First run, create table
            incremental_sql = f"CREATE TABLE {transformation.output_table} AS {transformation.sql}"

        with self.engine.connect() as conn:
            result = conn.execute(text(incremental_sql))
            conn.commit()
            return result.rowcount

    def get_transformation_lineage(self, name: str) -> dict[str, Any]:
        """Get data lineage information for a transformation."""
        if name not in self.transformations:
            return {}

        transformation = self.transformations[name]

        return {
            "transformation_name": name,
            "depends_on": transformation.depends_on,
            "output_table": transformation.output_table,
            "transformation_type": transformation.transformation_type.value,
            "analytical_functions": [af.value for af in transformation.analytical_functions],
            "materialization": transformation.materialization,
            "is_incremental": transformation.is_incremental,
            "tags": transformation.tags or [],
            "description": transformation.description,
        }

    def get_all_lineage(self) -> dict[str, dict[str, Any]]:
        """Get lineage information for all transformations."""
        return {name: self.get_transformation_lineage(name) for name in self.transformations.keys()}

    async def run_quality_tests(self, transformation_name: str) -> dict[str, Any]:
        """Run data quality tests on transformation output."""
        if transformation_name not in self.transformations:
            raise ValueError(f"Transformation '{transformation_name}' not found")

        transformation = self.transformations[transformation_name]
        output_table = transformation.output_table

        quality_tests = []

        # Test 1: Row count validation
        try:
            with self.engine.connect() as conn:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {output_table}"))
                row_count = count_result.scalar()

                quality_tests.append(
                    {
                        "test_name": "row_count_validation",
                        "status": "passed" if row_count > 0 else "failed",
                        "result": f"Found {row_count:,} rows",
                        "row_count": row_count,
                    }
                )
        except Exception as e:
            quality_tests.append(
                {
                    "test_name": "row_count_validation",
                    "status": "error",
                    "result": f"Error: {str(e)}",
                }
            )

        # Test 2: Null value analysis
        try:
            with self.engine.connect() as conn:
                # Get column information
                columns_result = conn.execute(
                    text(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{output_table.split(".")[-1]}'
                    LIMIT 10
                """)
                )
                columns = [row[0] for row in columns_result.fetchall()]

                null_analysis = {}
                for col in columns[:5]:  # Check first 5 columns
                    null_result = conn.execute(
                        text(f"""
                        SELECT
                            COUNT(*) as total_rows,
                            SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_count
                        FROM {output_table}
                    """)
                    )
                    row = null_result.fetchone()
                    total_rows, null_count = row[0], row[1]
                    null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

                    null_analysis[col] = {"null_count": null_count, "null_percentage": null_pct}

                quality_tests.append(
                    {
                        "test_name": "null_value_analysis",
                        "status": "passed",
                        "result": null_analysis,
                    }
                )

        except Exception as e:
            quality_tests.append(
                {
                    "test_name": "null_value_analysis",
                    "status": "error",
                    "result": f"Error: {str(e)}",
                }
            )

        # Test 3: Duplicate detection
        try:
            with self.engine.connect() as conn:
                # Try to detect duplicates using first few columns as key
                duplicate_result = conn.execute(
                    text(f"""
                    SELECT COUNT(*) - COUNT(DISTINCT *) as duplicate_count
                    FROM {output_table}
                """)
                )
                duplicate_count = duplicate_result.scalar()

                quality_tests.append(
                    {
                        "test_name": "duplicate_detection",
                        "status": "passed" if duplicate_count == 0 else "warning",
                        "result": f"Found {duplicate_count:,} potential duplicates",
                        "duplicate_count": duplicate_count,
                    }
                )

        except Exception as e:
            quality_tests.append(
                {
                    "test_name": "duplicate_detection",
                    "status": "error",
                    "result": f"Error: {str(e)}",
                }
            )

        # Summary
        passed_tests = sum(1 for test in quality_tests if test["status"] == "passed")
        total_tests = len(quality_tests)

        return {
            "transformation_name": transformation_name,
            "output_table": output_table,
            "tests_run": total_tests,
            "tests_passed": passed_tests,
            "tests_failed": total_tests - passed_tests,
            "overall_status": "passed" if passed_tests == total_tests else "failed",
            "test_results": quality_tests,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def generate_documentation(self) -> str:
        """Generate comprehensive documentation for all transformations."""
        doc_lines = [
            "# SQL Transformations Documentation",
            "",
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"Total Transformations: {len(self.transformations)}",
            "",
        ]

        # Group by transformation type
        by_type = {}
        for name, trans in self.transformations.items():
            trans_type = trans.transformation_type.value
            if trans_type not in by_type:
                by_type[trans_type] = []
            by_type[trans_type].append((name, trans))

        for trans_type, transformations in by_type.items():
            doc_lines.extend([f"## {trans_type.replace('_', ' ').title()} Transformations", ""])

            for name, trans in transformations:
                doc_lines.extend(
                    [
                        f"### {name}",
                        "",
                        f"**Description:** {trans.description}",
                        f"**Output Table:** `{trans.output_table}`",
                        f"**Materialization:** {trans.materialization}",
                        f"**Incremental:** {'Yes' if trans.is_incremental else 'No'}",
                        "",
                        "**Dependencies:**",
                        *[f"- {dep}" for dep in trans.depends_on],
                        "",
                        "**Analytical Functions:**",
                        *[f"- {af.value}" for af in trans.analytical_functions],
                        "",
                    ]
                )

                if trans.partition_by:
                    doc_lines.extend(
                        ["**Partitioned By:**", *[f"- {col}" for col in trans.partition_by], ""]
                    )

                if trans.tags:
                    doc_lines.extend(["**Tags:**", *[f"- {tag}" for tag in trans.tags], ""])

                doc_lines.append("---\n")

        return "\n".join(doc_lines)


# Factory function for easy instantiation
def create_sql_transformation_engine() -> SQLTransformationEngine:
    """Create and configure SQL transformation engine with retail analytics."""
    engine = SQLTransformationEngine()

    # Register retail analytics transformations
    retail_transformations = engine.get_retail_analytics_transformations()
    for transformation in retail_transformations:
        engine.register_transformation(transformation)

    # Register statistical transformations
    stats_transformations = engine.get_statistical_functions()
    for transformation in stats_transformations:
        engine.register_transformation(transformation)

    logger.info(
        f"SQL Transformation Engine created with {len(engine.transformations)} transformations"
    )

    return engine


# Convenience functions for common operations
async def execute_retail_analytics_pipeline() -> list[TransformationResult]:
    """Execute the complete retail analytics transformation pipeline."""
    engine = create_sql_transformation_engine()

    # Define execution order based on dependencies
    execution_order = [
        "advanced_statistical_analysis",
        "customer_lifetime_value_analysis",
        "product_performance_analytics",
        "advanced_time_series_analysis",
        "customer_cohort_retention_analysis",
    ]

    results = []
    for transformation_name in execution_order:
        try:
            result = await engine.execute_transformation(transformation_name)
            results.append(result)

            # Run quality tests
            quality_result = await engine.run_quality_tests(transformation_name)
            logger.info(
                f"Quality tests for {transformation_name}: {quality_result['overall_status']}"
            )

        except Exception as e:
            logger.error(f"Failed to execute {transformation_name}: {str(e)}")
            # Continue with other transformations

    return results


if __name__ == "__main__":
    # Demo usage
    import asyncio

    async def main():
        # Create transformation engine
        engine = create_sql_transformation_engine()

        # Generate documentation
        docs = engine.generate_documentation()
        print("Generated documentation:")
        print(docs[:1000] + "..." if len(docs) > 1000 else docs)

        # Show lineage
        lineage = engine.get_all_lineage()
        print(f"\nData lineage for {len(lineage)} transformations:")
        for name, info in lineage.items():
            print(f"- {name}: depends on {info['depends_on']}")

    asyncio.run(main())
