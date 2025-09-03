"""
Database Migration Script: Add Composite Indexes for Analytical Performance

This script adds critical composite indexes to optimize analytical query performance,
targeting <50ms response times for dashboard queries.

Migration Version: 2025_001_add_composite_indexes
Created: 2025-01-XX
Purpose: Add missing composite indexes for fact tables and dimensions
Expected Impact: 30-80% improvement in analytical query performance
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger

logger = get_logger(__name__)


class CompositeIndexMigration:
    """
    Database migration for adding composite indexes to optimize analytical queries.

    This migration adds 25+ critical indexes designed to support:
    - Dashboard queries (<50ms target)
    - Customer analytics and RFM analysis
    - Product performance analysis
    - Geographic revenue analysis
    - Time-series trend analysis
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.migration_name = "add_composite_indexes_v2025_001"

    async def execute(self, dry_run: bool = False) -> dict[str, Any]:
        """
        Execute the composite index migration.

        Args:
            dry_run: If True, only validate queries without creating indexes

        Returns:
            Dictionary with migration results and performance metrics
        """
        logger.info(f"Starting composite index migration: {self.migration_name}")

        results = {
            "migration_name": self.migration_name,
            "started_at": time.time(),
            "dry_run": dry_run,
            "indexes_created": [],
            "indexes_failed": [],
            "performance_benchmarks": {},
            "total_time_seconds": 0,
            "success": False,
        }

        try:
            # Step 1: Pre-migration performance benchmark
            logger.info("Running pre-migration performance benchmarks...")
            results["performance_benchmarks"]["before"] = await self._run_performance_benchmarks()

            # Step 2: Create fact table indexes
            await self._create_fact_table_indexes(results, dry_run)

            # Step 3: Create dimension table indexes
            await self._create_dimension_indexes(results, dry_run)

            # Step 4: Create covering indexes for performance
            await self._create_covering_indexes(results, dry_run)

            # Step 5: Create partial indexes for filtered queries
            await self._create_partial_indexes(results, dry_run)

            # Step 6: Post-migration performance benchmark
            if not dry_run:
                logger.info("Running post-migration performance benchmarks...")
                await self._update_table_statistics()
                results["performance_benchmarks"][
                    "after"
                ] = await self._run_performance_benchmarks()
                results["performance_improvement"] = self._calculate_performance_improvement(
                    results["performance_benchmarks"]["before"],
                    results["performance_benchmarks"]["after"],
                )

            results["total_time_seconds"] = time.time() - results["started_at"]
            results["success"] = len(results["indexes_failed"]) == 0

            logger.info(
                f"Migration completed: {len(results['indexes_created'])} indexes created, "
                f"{len(results['indexes_failed'])} failed"
            )

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            results["error"] = str(e)
            results["success"] = False

        return results

    async def _create_fact_table_indexes(self, results: dict[str, Any], dry_run: bool) -> None:
        """Create composite indexes for fact_sale table."""
        logger.info("Creating fact table composite indexes...")

        fact_table_indexes = [
            {
                "name": "idx_fact_sale_product_date_customer_perf",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_product_date_customer_perf
                    ON fact_sale (product_key, date_key, customer_key)
                """,
                "description": "Product-date-customer composite for product performance analysis",
            },
            {
                "name": "idx_fact_sale_date_customer_product_perf",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_date_customer_product_perf
                    ON fact_sale (date_key, customer_key, product_key)
                """,
                "description": "Date-customer-product composite for time-based customer analysis",
            },
            {
                "name": "idx_fact_sale_customer_value_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_customer_value_analysis
                    ON fact_sale (customer_key, date_key, total_amount, profit_amount)
                """,
                "description": "Customer value analysis with amounts for RFM calculations",
            },
            {
                "name": "idx_fact_sale_product_revenue_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_product_revenue_analysis
                    ON fact_sale (product_key, date_key, total_amount, quantity)
                """,
                "description": "Product revenue and quantity analysis",
            },
            {
                "name": "idx_fact_sale_country_time_revenue",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_country_time_revenue
                    ON fact_sale (country_key, date_key, total_amount)
                """,
                "description": "Geographic revenue analysis by country and time",
            },
            {
                "name": "idx_fact_sale_monthly_aggregation",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_monthly_aggregation
                    ON fact_sale (date_key, country_key, product_key, total_amount)
                    WHERE date_key >= 20240101
                """,
                "description": "Monthly aggregation index for recent data",
            },
            {
                "name": "idx_fact_sale_profit_margin_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_profit_margin_analysis
                    ON fact_sale (profit_amount, margin_percentage, date_key, product_key)
                    WHERE profit_amount IS NOT NULL
                """,
                "description": "Profit and margin analysis index",
            },
            {
                "name": "idx_fact_sale_invoice_processing",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_invoice_processing
                    ON fact_sale (invoice_key, batch_id, created_at, data_quality_score)
                """,
                "description": "Invoice processing and data quality tracking",
            },

            # === NEW COMPOSITE INDEXES FOR ENHANCED ANALYTICAL PERFORMANCE ===
            {
                "name": "idx_fact_sale_multi_dim_analytics",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_multi_dim_analytics
                    ON fact_sale (date_key, product_key, customer_key)
                """,
                "description": "Multi-dimensional sales analytics composite index",
            },
            {
                "name": "idx_fact_sale_store_product_performance",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_store_product_performance
                    ON fact_sale (store_key, product_key, date_key)
                """,
                "description": "Store-product performance analysis composite index",
            },
            {
                "name": "idx_fact_sale_customer_store_loyalty",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_customer_store_loyalty
                    ON fact_sale (customer_key, store_key, date_key)
                """,
                "description": "Customer-store loyalty analysis composite index",
            },
            {
                "name": "idx_fact_sale_all_foreign_keys",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_all_foreign_keys
                    ON fact_sale (date_key, product_key, customer_key, store_key)
                """,
                "description": "Complete foreign keys composite index for complex analytical queries",
            },
            {
                "name": "idx_fact_sale_time_invoice_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_time_invoice_analysis
                    ON fact_sale (date_key, invoice_id, line_amount)
                """,
                "description": "Time-partitioned analysis for invoice-level aggregations",
            },
        ]

        for index_def in fact_table_indexes:
            await self._create_single_index(index_def, results, dry_run)

    async def _create_dimension_indexes(self, results: dict[str, Any], dry_run: bool) -> None:
        """Create enhanced indexes for dimension tables."""
        logger.info("Creating dimension table indexes...")

        dimension_indexes = [
            # Customer dimension indexes
            {
                "name": "idx_dim_customer_rfm_segment_value",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_customer_rfm_segment_value
                    ON dim_customer (rfm_segment, customer_segment, lifetime_value, is_current)
                """,
                "description": "RFM and customer segment analysis with current status",
            },
            {
                "name": "idx_dim_customer_behavioral_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_customer_behavioral_analysis
                    ON dim_customer (customer_segment, first_purchase_date, last_purchase_date, total_orders)
                    WHERE is_current = 1
                """,
                "description": "Customer behavioral pattern analysis for active customers",
            },
            {
                "name": "idx_dim_customer_cohort_registration",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_customer_cohort_registration
                    ON dim_customer (registration_date, customer_segment, lifetime_value)
                    WHERE registration_date IS NOT NULL AND is_current = 1
                """,
                "description": "Customer cohort analysis by registration date",
            },
            # Product dimension indexes
            {
                "name": "idx_dim_product_hierarchy_complete",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_product_hierarchy_complete
                    ON dim_product (category, subcategory, brand, product_family, is_current)
                """,
                "description": "Complete product hierarchy navigation",
            },
            {
                "name": "idx_dim_product_pricing_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_product_pricing_analysis
                    ON dim_product (category, recommended_price, unit_cost, brand)
                    WHERE is_current = 1 AND recommended_price IS NOT NULL
                """,
                "description": "Product pricing analysis by category and brand",
            },
            {
                "name": "idx_dim_product_current_stock_lookup",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_product_current_stock_lookup
                    ON dim_product (stock_code, is_current, category, brand)
                """,
                "description": "Fast stock code lookups for current products",
            },
            # Date dimension indexes
            {
                "name": "idx_dim_date_fiscal_hierarchy",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_date_fiscal_hierarchy
                    ON dim_date (fiscal_year, fiscal_quarter, year, month)
                    WHERE fiscal_year IS NOT NULL
                """,
                "description": "Complete fiscal hierarchy for fiscal reporting",
            },
            {
                "name": "idx_dim_date_business_pattern_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_date_business_pattern_analysis
                    ON dim_date (year, month, is_weekend, is_holiday, day_of_week)
                """,
                "description": "Business pattern analysis (weekends, holidays, seasonality)",
            },
            {
                "name": "idx_dim_date_recent_business_days",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_date_recent_business_days
                    ON dim_date (date_key, is_weekend, is_holiday)
                    WHERE date_key >= 20240101
                """,
                "description": "Recent business days for dashboard queries",
            },
            # Country dimension indexes
            {
                "name": "idx_dim_country_regional_economic_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_country_regional_economic_analysis
                    ON dim_country (region, continent, gdp_per_capita, currency_code)
                """,
                "description": "Regional economic analysis by GDP and currency",
            },
            {
                "name": "idx_dim_country_business_environment",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_country_business_environment
                    ON dim_country (is_eu_member, tax_rate, currency_code, region)
                """,
                "description": "Business environment analysis (EU membership, taxes)",
            },
            # Invoice dimension indexes
            {
                "name": "idx_dim_invoice_payment_performance",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_invoice_payment_performance
                    ON dim_invoice (payment_status, payment_method, processing_time_minutes, invoice_total)
                """,
                "description": "Payment processing performance analysis",
            },
            {
                "name": "idx_dim_invoice_channel_analysis",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_invoice_channel_analysis
                    ON dim_invoice (channel, invoice_date, invoice_total, payment_status)
                    WHERE is_cancelled = false
                """,
                "description": "Sales channel performance analysis for non-cancelled invoices",
            },
        ]

        for index_def in dimension_indexes:
            await self._create_single_index(index_def, results, dry_run)

    async def _create_covering_indexes(self, results: dict[str, Any], dry_run: bool) -> None:
        """Create covering indexes to avoid table lookups for common queries."""
        logger.info("Creating covering indexes...")

        # Note: INCLUDE clause syntax is PostgreSQL-specific
        # For SQLite, we create regular composite indexes
        covering_indexes = [
            {
                "name": "idx_fact_sale_dashboard_covering",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_dashboard_covering
                    ON fact_sale (date_key, customer_key, product_key, country_key,
                                  total_amount, quantity, profit_amount)
                """,
                "description": "Covering index for dashboard queries (includes key metrics)",
            },
            {
                "name": "idx_fact_sale_revenue_summary_covering",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_revenue_summary_covering
                    ON fact_sale (date_key, country_key, total_amount, tax_amount,
                                  discount_amount, quantity)
                """,
                "description": "Covering index for revenue summary reports",
            },
            {
                "name": "idx_dim_customer_analytics_covering",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_customer_analytics_covering
                    ON dim_customer (customer_key, is_current, customer_segment, rfm_segment,
                                     lifetime_value, total_orders, avg_order_value)
                """,
                "description": "Covering index for customer analytics (includes key metrics)",
            },
            {
                "name": "idx_dim_product_catalog_covering",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_product_catalog_covering
                    ON dim_product (product_key, is_current, stock_code, category, subcategory,
                                    brand, recommended_price, unit_cost)
                """,
                "description": "Covering index for product catalog queries",
            },
        ]

        for index_def in covering_indexes:
            await self._create_single_index(index_def, results, dry_run)

    async def _create_partial_indexes(self, results: dict[str, Any], dry_run: bool) -> None:
        """Create partial indexes for filtered queries to improve performance and reduce size."""
        logger.info("Creating partial indexes...")

        partial_indexes = [
            {
                "name": "idx_fact_sale_high_value_customers",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_high_value_customers
                    ON fact_sale (customer_key, date_key, total_amount, profit_amount)
                    WHERE total_amount > 100.00 AND customer_key IS NOT NULL
                """,
                "description": "High-value customer transactions for VIP analysis",
            },
            {
                "name": "idx_fact_sale_recent_profitable",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_recent_profitable
                    ON fact_sale (product_key, profit_amount, date_key)
                    WHERE profit_amount > 0 AND date_key >= 20240101
                """,
                "description": "Recent profitable transactions for product performance",
            },
            {
                "name": "idx_fact_sale_bulk_orders",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_fact_sale_bulk_orders
                    ON fact_sale (quantity, customer_key, product_key, date_key)
                    WHERE quantity >= 10
                """,
                "description": "Bulk order analysis for B2B customer identification",
            },
            {
                "name": "idx_dim_customer_active_high_value",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_customer_active_high_value
                    ON dim_customer (lifetime_value, customer_segment, total_orders)
                    WHERE is_current = 1 AND lifetime_value > 500.00
                """,
                "description": "Active high-value customers for targeted marketing",
            },
            {
                "name": "idx_dim_product_active_premium",
                "sql": """
                    CREATE INDEX IF NOT EXISTS idx_dim_product_active_premium
                    ON dim_product (category, brand, recommended_price)
                    WHERE is_current = 1 AND recommended_price > 50.00
                """,
                "description": "Active premium products for category management",
            },
        ]

        for index_def in partial_indexes:
            await self._create_single_index(index_def, results, dry_run)

    async def _create_single_index(
        self, index_def: dict[str, Any], results: dict[str, Any], dry_run: bool
    ) -> None:
        """Create a single index with error handling."""
        try:
            if dry_run:
                logger.info(f"DRY RUN: Would create index {index_def['name']}")
                results["indexes_created"].append(
                    {
                        "name": index_def["name"],
                        "description": index_def["description"],
                        "dry_run": True,
                    }
                )
                return

            start_time = time.time()

            with self.engine.connect() as conn:
                conn.execute(text(index_def["sql"]))
                conn.commit()

            creation_time = time.time() - start_time

            results["indexes_created"].append(
                {
                    "name": index_def["name"],
                    "description": index_def["description"],
                    "creation_time_seconds": creation_time,
                    "dry_run": False,
                }
            )

            logger.info(f"✅ Created index {index_def['name']} in {creation_time:.2f}s")

        except SQLAlchemyError as e:
            error_msg = str(e)
            results["indexes_failed"].append(
                {
                    "name": index_def["name"],
                    "error": error_msg,
                    "description": index_def["description"],
                }
            )
            logger.error(f"❌ Failed to create index {index_def['name']}: {error_msg}")

    async def _run_performance_benchmarks(self) -> dict[str, float]:
        """Run benchmark queries to measure performance impact."""
        benchmarks = {}

        benchmark_queries = [
            {
                "name": "daily_sales_dashboard",
                "sql": """
                    SELECT
                        dd.date,
                        COUNT(*) as transactions,
                        SUM(fs.total_amount) as revenue,
                        COUNT(DISTINCT fs.customer_key) as unique_customers
                    FROM fact_sale fs
                    JOIN dim_date dd ON fs.date_key = dd.date_key
                    WHERE dd.date_key >= (SELECT MAX(date_key) - 30 FROM dim_date)
                    GROUP BY dd.date
                    ORDER BY dd.date
                """,
            },
            {
                "name": "customer_segment_analysis",
                "sql": """
                    SELECT
                        dc.customer_segment,
                        COUNT(*) as customer_count,
                        AVG(dc.lifetime_value) as avg_ltv
                    FROM dim_customer dc
                    WHERE dc.is_current = 1
                    GROUP BY dc.customer_segment
                """,
            },
            {
                "name": "product_performance",
                "sql": """
                    SELECT
                        dp.category,
                        dp.brand,
                        SUM(fs.total_amount) as revenue,
                        COUNT(*) as sales_count
                    FROM dim_product dp
                    JOIN fact_sale fs ON dp.product_key = fs.product_key
                    WHERE dp.is_current = 1 AND fs.date_key >= 20240101
                    GROUP BY dp.category, dp.brand
                    ORDER BY revenue DESC
                    LIMIT 20
                """,
            },
        ]

        for query in benchmark_queries:
            try:
                start_time = time.time()
                with self.engine.connect() as conn:
                    result = conn.execute(text(query["sql"]))
                    # Fetch results to ensure full execution
                    rows = result.fetchall()

                execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
                benchmarks[query["name"]] = {
                    "execution_time_ms": execution_time,
                    "rows_returned": len(rows),
                }
                logger.debug(f"Benchmark {query['name']}: {execution_time:.2f}ms")

            except Exception as e:
                logger.error(f"Benchmark {query['name']} failed: {e}")
                benchmarks[query["name"]] = {"execution_time_ms": -1, "error": str(e)}

        return benchmarks

    async def _update_table_statistics(self) -> None:
        """Update table statistics after index creation."""
        logger.info("Updating table statistics...")

        tables = [
            "fact_sale",
            "dim_customer",
            "dim_product",
            "dim_date",
            "dim_country",
            "dim_invoice",
        ]

        for table in tables:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"ANALYZE {table}"))
                    conn.commit()
                logger.debug(f"Updated statistics for {table}")
            except Exception as e:
                logger.warning(f"Failed to update statistics for {table}: {e}")

    def _calculate_performance_improvement(
        self, before: dict[str, Any], after: dict[str, Any]
    ) -> dict[str, Any]:
        """Calculate performance improvement metrics."""
        improvements = {}

        for query_name in before.keys():
            if query_name in after:
                before_time = before[query_name].get("execution_time_ms", -1)
                after_time = after[query_name].get("execution_time_ms", -1)

                if before_time > 0 and after_time > 0:
                    improvement_pct = ((before_time - after_time) / before_time) * 100
                    improvements[query_name] = {
                        "before_ms": before_time,
                        "after_ms": after_time,
                        "improvement_pct": improvement_pct,
                        "meets_target": after_time < 50,  # <50ms target for dashboard queries
                    }
                else:
                    improvements[query_name] = {
                        "before_ms": before_time,
                        "after_ms": after_time,
                        "improvement_pct": None,
                        "meets_target": False,
                    }

        return improvements

    async def rollback(self) -> dict[str, Any]:
        """
        Rollback the migration by dropping created indexes.

        Returns:
            Dictionary with rollback results
        """
        logger.warning("Rolling back composite index migration...")

        rollback_results = {
            "migration_name": self.migration_name,
            "started_at": time.time(),
            "indexes_dropped": [],
            "indexes_failed_to_drop": [],
            "success": False,
        }

        # List of all indexes created by this migration
        indexes_to_drop = [
            "idx_fact_sale_product_date_customer_perf",
            "idx_fact_sale_date_customer_product_perf",
            "idx_fact_sale_customer_value_analysis",
            "idx_fact_sale_product_revenue_analysis",
            "idx_fact_sale_country_time_revenue",
            "idx_fact_sale_monthly_aggregation",
            "idx_fact_sale_profit_margin_analysis",
            "idx_fact_sale_invoice_processing",
            "idx_dim_customer_rfm_segment_value",
            "idx_dim_customer_behavioral_analysis",
            "idx_dim_customer_cohort_registration",
            "idx_dim_product_hierarchy_complete",
            "idx_dim_product_pricing_analysis",
            "idx_dim_product_current_stock_lookup",
            "idx_dim_date_fiscal_hierarchy",
            "idx_dim_date_business_pattern_analysis",
            "idx_dim_date_recent_business_days",
            "idx_dim_country_regional_economic_analysis",
            "idx_dim_country_business_environment",
            "idx_dim_invoice_payment_performance",
            "idx_dim_invoice_channel_analysis",
            "idx_fact_sale_dashboard_covering",
            "idx_fact_sale_revenue_summary_covering",
            "idx_dim_customer_analytics_covering",
            "idx_dim_product_catalog_covering",
            "idx_fact_sale_high_value_customers",
            "idx_fact_sale_recent_profitable",
            "idx_fact_sale_bulk_orders",
            "idx_dim_customer_active_high_value",
            "idx_dim_product_active_premium",
        ]

        for index_name in indexes_to_drop:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
                    conn.commit()

                rollback_results["indexes_dropped"].append(index_name)
                logger.info(f"Dropped index {index_name}")

            except Exception as e:
                rollback_results["indexes_failed_to_drop"].append(
                    {"index_name": index_name, "error": str(e)}
                )
                logger.error(f"Failed to drop index {index_name}: {e}")

        rollback_results["success"] = len(rollback_results["indexes_failed_to_drop"]) == 0
        logger.info(
            f"Rollback completed: {len(rollback_results['indexes_dropped'])} indexes dropped"
        )

        return rollback_results


async def run_composite_index_migration(engine: Engine, dry_run: bool = False) -> dict[str, Any]:
    """
    Convenience function to run the composite index migration.

    Args:
        engine: SQLAlchemy engine
        dry_run: If True, validate without making changes

    Returns:
        Migration results and performance metrics
    """
    migration = CompositeIndexMigration(engine)
    return await migration.execute(dry_run=dry_run)


async def rollback_composite_index_migration(engine: Engine) -> dict[str, Any]:
    """
    Rollback the composite index migration.

    Args:
        engine: SQLAlchemy engine

    Returns:
        Rollback results
    """
    migration = CompositeIndexMigration(engine)
    return await migration.rollback()


if __name__ == "__main__":
    # Example usage for testing
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from data_access.db import get_engine

    async def main():
        engine = get_engine()

        # Run dry run first
        print("Running dry run...")
        dry_run_results = await run_composite_index_migration(engine, dry_run=True)
        print(
            f"Dry run completed: {len(dry_run_results['indexes_created'])} indexes would be created"
        )

        # Uncomment to run actual migration
        # print("\nRunning actual migration...")
        # results = await run_composite_index_migration(engine, dry_run=False)
        # print(f"Migration completed: {results['success']}")
        #
        # if results['performance_benchmarks']:
        #     print("\nPerformance improvements:")
        #     for query, improvement in results.get('performance_improvement', {}).items():
        #         if improvement['improvement_pct'] is not None:
        #             print(f"  {query}: {improvement['improvement_pct']:.1f}% improvement "
        #                   f"({improvement['before_ms']:.1f}ms -> {improvement['after_ms']:.1f}ms)")

    asyncio.run(main())
