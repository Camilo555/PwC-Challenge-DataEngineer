"""
Database Index Management

Comprehensive index creation and optimization for the retail data warehouse.
Includes performance-optimized indexes for all query patterns.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger

logger = get_logger(__name__)


class IndexType(str, Enum):
    """Types of database indexes."""
    BTREE = "btree"
    HASH = "hash"
    PARTIAL = "partial"
    COMPOSITE = "composite"
    COVERING = "covering"


@dataclass
class IndexDefinition:
    """Definition for a database index."""
    name: str
    table_name: str
    columns: list[str]
    index_type: IndexType = IndexType.BTREE
    unique: bool = False
    partial_condition: str | None = None
    include_columns: list[str] | None = None  # For covering indexes
    description: str = ""


class IndexManager:
    """
    Advanced index management for optimal query performance.
    
    This class manages creation, monitoring, and optimization of database indexes
    based on common query patterns in the retail analytics system.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self._existing_indexes: set[str] = set()

    def get_performance_indexes(self) -> list[IndexDefinition]:
        """
        Get comprehensive list of performance-optimized indexes for the retail system.
        
        These indexes are designed based on common query patterns:
        - Time-based analytics (sales by date, trends)
        - Customer analytics (RFM, segmentation, lifetime value)
        - Product analytics (performance, categories, inventory)
        - Geographic analytics (country-based analysis)
        - Transactional queries (invoice lookups, order processing)
        """
        return [
            # === FACT_SALE TABLE INDEXES - OPTIMIZED FOR <15MS QUERIES ===

            # Primary time-series index for sales analytics (ENHANCED)
            IndexDefinition(
                name="idx_fact_sale_date_performance_v2",
                table_name="fact_sale",
                columns=["date_key", "total_amount"],
                index_type=IndexType.BTREE,
                include_columns=["quantity", "profit_amount"],
                description="Enhanced primary index for time-based sales queries with covering data"
            ),

            # Customer analytics index (ENHANCED WITH RFM)
            IndexDefinition(
                name="idx_fact_sale_customer_analytics_v2",
                table_name="fact_sale",
                columns=["customer_key", "date_key", "total_amount"],
                index_type=IndexType.COMPOSITE,
                description="Enhanced customer behavior and RFM analysis with covering amounts"
            ),

            # Product performance index (ENHANCED)
            IndexDefinition(
                name="idx_fact_sale_product_performance_v2",
                table_name="fact_sale",
                columns=["product_key", "date_key", "total_amount"],
                include_columns=["quantity", "profit_amount", "unit_price"],
                description="Enhanced product sales performance with all key metrics"
            ),

            # Geographic analytics index (ENHANCED)
            IndexDefinition(
                name="idx_fact_sale_country_analytics_v2",
                table_name="fact_sale",
                columns=["country_key", "date_key", "total_amount"],
                include_columns=["quantity", "profit_amount"],
                description="Enhanced geographic sales analysis with revenue metrics"
            ),

            # Invoice processing index (ENHANCED)
            IndexDefinition(
                name="idx_fact_sale_invoice_lookup_v2",
                table_name="fact_sale",
                columns=["invoice_key", "date_key"],
                include_columns=["customer_key", "total_amount", "quantity"],
                description="Enhanced invoice detail lookups with key context"
            ),

            # High-value sales index (ENHANCED PARTIAL INDEX)
            IndexDefinition(
                name="idx_fact_sale_high_value_v2",
                table_name="fact_sale",
                columns=["total_amount", "date_key", "customer_key"],
                index_type=IndexType.PARTIAL,
                partial_condition="total_amount > 100.00",
                include_columns=["profit_amount", "product_key", "country_key"],
                description="Enhanced high-value transactions with complete context"
            ),

            # Batch processing index (ENHANCED)
            IndexDefinition(
                name="idx_fact_sale_batch_processing_v2",
                table_name="fact_sale",
                columns=["batch_id", "created_at", "source_system"],
                include_columns=["data_quality_score"],
                description="Enhanced ETL batch processing with quality metrics"
            ),

            # Revenue covering index (ENHANCED COVERING INDEX)
            IndexDefinition(
                name="idx_fact_sale_revenue_covering_v2",
                table_name="fact_sale",
                columns=["date_key", "country_key", "product_key"],
                index_type=IndexType.COVERING,
                include_columns=["total_amount", "quantity", "profit_amount", "customer_key", "unit_price"],
                description="Comprehensive covering index for revenue analytics - includes all key measures"
            ),

            # CRITICAL NEW INDEXES FOR <15MS PERFORMANCE

            # Multi-dimensional analytics index for dashboard queries
            IndexDefinition(
                name="idx_fact_sale_dashboard_analytics",
                table_name="fact_sale",
                columns=["date_key", "customer_key", "product_key", "country_key"],
                index_type=IndexType.COMPOSITE,
                include_columns=["total_amount", "quantity", "profit_amount"],
                description="CRITICAL: Multi-dimensional index for dashboard analytics - targets <15ms"
            ),

            # Time-series revenue aggregation index
            IndexDefinition(
                name="idx_fact_sale_timeseries_revenue",
                table_name="fact_sale",
                columns=["date_key", "total_amount", "profit_amount"],
                index_type=IndexType.COVERING,
                include_columns=["quantity", "customer_key", "product_key", "country_key"],
                description="CRITICAL: Time-series revenue aggregation - optimized for reporting queries"
            ),

            # Customer segment analysis index
            IndexDefinition(
                name="idx_fact_sale_customer_segment",
                table_name="fact_sale",
                columns=["customer_key", "total_amount", "date_key"],
                index_type=IndexType.COVERING,
                include_columns=["quantity", "profit_amount", "product_key", "invoice_key"],
                description="CRITICAL: Customer segmentation and lifetime value calculations"
            ),

            # Product category performance index
            IndexDefinition(
                name="idx_fact_sale_product_category",
                table_name="fact_sale",
                columns=["product_key", "total_amount", "date_key"],
                index_type=IndexType.COVERING,
                include_columns=["quantity", "profit_amount", "customer_key", "country_key"],
                description="CRITICAL: Product category performance analysis"
            ),

            # Geographic revenue analysis index
            IndexDefinition(
                name="idx_fact_sale_geographic_revenue",
                table_name="fact_sale",
                columns=["country_key", "total_amount", "date_key"],
                index_type=IndexType.COVERING,
                include_columns=["quantity", "profit_amount", "product_key", "customer_key"],
                description="CRITICAL: Geographic revenue analysis and regional reporting"
            ),

            # High-performance aggregation index for SUM operations
            IndexDefinition(
                name="idx_fact_sale_aggregation_optimized",
                table_name="fact_sale",
                columns=["date_key", "country_key", "product_key", "customer_key", "total_amount"],
                index_type=IndexType.COVERING,
                include_columns=["quantity", "profit_amount", "unit_price", "discount_amount"],
                description="CRITICAL: Optimized for GROUP BY and aggregation operations"
            ),

            # Profit margin analysis index
            IndexDefinition(
                name="idx_fact_sale_profit_margin",
                table_name="fact_sale",
                columns=["profit_amount", "total_amount", "date_key"],
                index_type=IndexType.COVERING,
                include_columns=["product_key", "customer_key", "country_key", "margin_percentage"],
                description="CRITICAL: Profit margin analysis and profitability reporting"
            ),

            # NEW ULTRA-PERFORMANCE INDEXES FOR <15MS TARGET

            # Lightning-fast dashboard queries index
            IndexDefinition(
                name="idx_fact_sale_lightning_dashboard",
                table_name="fact_sale",
                columns=["date_key", "total_amount", "profit_amount", "quantity"],
                index_type=IndexType.COVERING,
                include_columns=["customer_key", "product_key", "country_key", "invoice_key", "unit_price"],
                description="ULTRA-CRITICAL: Lightning-fast dashboard queries <15ms target"
            ),

            # Real-time analytics index for instant queries
            IndexDefinition(
                name="idx_fact_sale_realtime_analytics",
                table_name="fact_sale",
                columns=["date_key", "country_key", "total_amount"],
                index_type=IndexType.COVERING,
                include_columns=["profit_amount", "quantity", "product_key", "customer_key", "margin_percentage"],
                description="ULTRA-CRITICAL: Real-time analytics with sub-10ms response times"
            ),

            # Top-performing products instant lookup
            IndexDefinition(
                name="idx_fact_sale_top_products_instant",
                table_name="fact_sale",
                columns=["product_key", "total_amount", "quantity"],
                index_type=IndexType.COVERING,
                include_columns=["date_key", "profit_amount", "customer_key", "country_key"],
                description="ULTRA-CRITICAL: Instant top-performing products queries"
            ),

            # Customer lifetime value instant calculation
            IndexDefinition(
                name="idx_fact_sale_clv_instant",
                table_name="fact_sale",
                columns=["customer_key", "date_key", "total_amount"],
                index_type=IndexType.COVERING,
                include_columns=["profit_amount", "quantity", "product_key", "invoice_key"],
                description="ULTRA-CRITICAL: Instant customer lifetime value calculations"
            ),

            # Geographic revenue instant aggregation
            IndexDefinition(
                name="idx_fact_sale_geo_instant",
                table_name="fact_sale",
                columns=["country_key", "date_key", "total_amount", "profit_amount"],
                index_type=IndexType.COVERING,
                include_columns=["quantity", "product_key", "customer_key"],
                description="ULTRA-CRITICAL: Instant geographic revenue aggregations"
            ),

            # Time-series trend analysis ultra-fast
            IndexDefinition(
                name="idx_fact_sale_trends_ultrafast",
                table_name="fact_sale",
                columns=["date_key", "total_amount", "profit_amount", "quantity"],
                index_type=IndexType.COVERING,
                include_columns=["customer_key", "product_key", "country_key", "unit_price", "margin_percentage"],
                description="ULTRA-CRITICAL: Ultra-fast time-series trend analysis"
            ),

            # Hot data access pattern optimization
            IndexDefinition(
                name="idx_fact_sale_hot_data_access",
                table_name="fact_sale",
                columns=["date_key", "customer_key", "product_key", "country_key"],
                index_type=IndexType.COVERING,
                include_columns=["total_amount", "profit_amount", "quantity", "unit_price", "discount_amount"],
                description="ULTRA-CRITICAL: Hot data access pattern optimization for frequent queries"
            ),

            # === DIM_DATE TABLE INDEXES ===

            # Calendar navigation
            IndexDefinition(
                name="idx_dim_date_calendar",
                table_name="dim_date",
                columns=["year", "month", "day_of_month"],
                description="Calendar-based date lookups"
            ),

            # Fiscal period analytics
            IndexDefinition(
                name="idx_dim_date_fiscal",
                table_name="dim_date",
                columns=["fiscal_year", "fiscal_quarter"],
                description="Fiscal period reporting"
            ),

            # Weekend and holiday analysis
            IndexDefinition(
                name="idx_dim_date_business_patterns",
                table_name="dim_date",
                columns=["is_weekend", "is_holiday"],
                description="Business pattern analysis (weekends/holidays)"
            ),

            # === DIM_PRODUCT TABLE INDEXES ===

            # Product hierarchy navigation
            IndexDefinition(
                name="idx_dim_product_hierarchy",
                table_name="dim_product",
                columns=["category", "subcategory", "brand"],
                description="Product hierarchy and categorization"
            ),

            # SCD Type 2 current records
            IndexDefinition(
                name="idx_dim_product_scd2_current",
                table_name="dim_product",
                columns=["stock_code", "is_current"],
                unique=True,
                description="Unique current product versions for SCD2"
            ),

            # Product search and filtering
            IndexDefinition(
                name="idx_dim_product_search",
                table_name="dim_product",
                columns=["description"],  # Consider full-text index for PostgreSQL
                description="Product description search"
            ),

            # Price analysis
            IndexDefinition(
                name="idx_dim_product_pricing",
                table_name="dim_product",
                columns=["category", "recommended_price"],
                description="Price analysis by category"
            ),

            # SCD2 temporal queries
            IndexDefinition(
                name="idx_dim_product_temporal",
                table_name="dim_product",
                columns=["valid_from", "valid_to"],
                description="Temporal queries for product history"
            ),

            # === DIM_CUSTOMER TABLE INDEXES ===

            # Customer segmentation
            IndexDefinition(
                name="idx_dim_customer_segmentation",
                table_name="dim_customer",
                columns=["customer_segment", "rfm_segment"],
                description="Customer segmentation analytics"
            ),

            # RFM analysis
            IndexDefinition(
                name="idx_dim_customer_rfm",
                table_name="dim_customer",
                columns=["recency_score", "frequency_score", "monetary_score"],
                description="RFM scoring and analysis"
            ),

            # Customer lifetime value
            IndexDefinition(
                name="idx_dim_customer_ltv",
                table_name="dim_customer",
                columns=["lifetime_value"],
                description="Customer lifetime value ranking"
            ),

            # SCD2 customer current records
            IndexDefinition(
                name="idx_dim_customer_scd2_current",
                table_name="dim_customer",
                columns=["customer_id", "is_current"],
                description="Current customer versions for SCD2"
            ),

            # Customer acquisition cohorts
            IndexDefinition(
                name="idx_dim_customer_cohorts",
                table_name="dim_customer",
                columns=["registration_date", "customer_segment"],
                description="Customer acquisition cohort analysis"
            ),

            # High-value customers (partial index)
            IndexDefinition(
                name="idx_dim_customer_high_value",
                table_name="dim_customer",
                columns=["lifetime_value", "total_orders"],
                index_type=IndexType.PARTIAL,
                partial_condition="lifetime_value > 1000.00 AND total_orders > 10",
                description="High-value customer segment"
            ),

            # === DIM_COUNTRY TABLE INDEXES ===

            # Geographic hierarchy
            IndexDefinition(
                name="idx_dim_country_geography",
                table_name="dim_country",
                columns=["continent", "region"],
                description="Geographic hierarchy for regional reporting"
            ),

            # Economic analysis
            IndexDefinition(
                name="idx_dim_country_economic",
                table_name="dim_country",
                columns=["currency_code", "gdp_per_capita"],
                description="Economic indicators and currency analysis"
            ),

            # EU membership analysis
            IndexDefinition(
                name="idx_dim_country_eu_status",
                table_name="dim_country",
                columns=["is_eu_member", "tax_rate"],
                description="EU membership and tax analysis"
            ),

            # === DIM_INVOICE TABLE INDEXES ===

            # Invoice processing workflow
            IndexDefinition(
                name="idx_dim_invoice_processing",
                table_name="dim_invoice",
                columns=["payment_status", "invoice_date"],
                description="Invoice processing and payment tracking"
            ),

            # Cancelled invoice analysis
            IndexDefinition(
                name="idx_dim_invoice_cancelled",
                table_name="dim_invoice",
                columns=["is_cancelled", "invoice_date"],
                description="Cancelled invoice analysis and trends"
            ),

            # Payment method analysis
            IndexDefinition(
                name="idx_dim_invoice_payment_analysis",
                table_name="dim_invoice",
                columns=["payment_method", "channel"],
                description="Payment method and channel analysis"
            ),

            # Invoice value analysis
            IndexDefinition(
                name="idx_dim_invoice_value",
                table_name="dim_invoice",
                columns=["invoice_total", "tax_amount"],
                description="Invoice value and tax analysis"
            ),
        ]

    async def create_all_indexes(self, force_recreate: bool = False) -> dict[str, Any]:
        """
        Create all performance indexes with comprehensive error handling.
        
        Args:
            force_recreate: If True, drop and recreate existing indexes
            
        Returns:
            Dictionary with creation results and metrics
        """
        logger.info("Starting comprehensive index creation...")

        results = {
            'created': [],
            'skipped': [],
            'failed': [],
            'total_time_seconds': 0,
            'performance_impact': {}
        }

        start_time = time.time()

        try:
            # Get current indexes
            await self._refresh_existing_indexes()

            # Get index definitions
            index_definitions = self.get_performance_indexes()

            logger.info(f"Creating {len(index_definitions)} performance indexes...")

            for index_def in index_definitions:
                try:
                    index_start = time.time()

                    if await self._create_index(index_def, force_recreate):
                        index_time = time.time() - index_start
                        results['created'].append({
                            'name': index_def.name,
                            'table': index_def.table_name,
                            'columns': index_def.columns,
                            'creation_time_seconds': index_time,
                            'description': index_def.description
                        })
                        logger.info(f"✅ Created index {index_def.name} in {index_time:.2f}s")
                    else:
                        results['skipped'].append({
                            'name': index_def.name,
                            'reason': 'already_exists'
                        })
                        logger.debug(f"⏭️ Skipped existing index {index_def.name}")

                except Exception as e:
                    results['failed'].append({
                        'name': index_def.name,
                        'error': str(e),
                        'table': index_def.table_name
                    })
                    logger.error(f"❌ Failed to create index {index_def.name}: {e}")

            results['total_time_seconds'] = time.time() - start_time

            # Analyze performance impact
            results['performance_impact'] = await self._analyze_performance_impact()

            logger.info(f"Index creation completed in {results['total_time_seconds']:.2f}s")
            logger.info(f"✅ Created: {len(results['created'])}, ⏭️ Skipped: {len(results['skipped'])}, ❌ Failed: {len(results['failed'])}")

        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            raise

        return results

    async def _create_index(self, index_def: IndexDefinition, force_recreate: bool = False) -> bool:
        """Create a single index with proper error handling."""
        if index_def.name in self._existing_indexes and not force_recreate:
            return False

        if force_recreate and index_def.name in self._existing_indexes:
            await self._drop_index(index_def.name, index_def.table_name)

        try:
            # Create index SQL based on database type
            sql = self._generate_index_sql(index_def)

            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()

            self._existing_indexes.add(index_def.name)
            return True

        except SQLAlchemyError as e:
            logger.error(f"Failed to create index {index_def.name}: {e}")
            raise

    def _generate_index_sql(self, index_def: IndexDefinition) -> str:
        """Generate database-specific index creation SQL."""
        # Base index creation
        columns_str = ", ".join(index_def.columns)

        sql = "CREATE"

        if index_def.unique:
            sql += " UNIQUE"

        sql += f" INDEX {index_def.name} ON {index_def.table_name} ({columns_str})"

        # Add partial condition if specified
        if index_def.partial_condition:
            sql += f" WHERE {index_def.partial_condition}"

        # Add covering columns for PostgreSQL
        if index_def.include_columns:
            include_str = ", ".join(index_def.include_columns)
            sql += f" INCLUDE ({include_str})"

        return sql

    async def _refresh_existing_indexes(self) -> None:
        """Refresh the list of existing indexes."""
        self._existing_indexes.clear()

        try:
            # Query to get existing indexes (works for SQLite and PostgreSQL)
            sql = """
                SELECT name FROM sqlite_master 
                WHERE type='index' AND name NOT LIKE 'sqlite_%'
                UNION ALL
                SELECT indexname as name FROM pg_indexes 
                WHERE schemaname = 'public'
            """

            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                for row in result:
                    self._existing_indexes.add(row[0])

        except SQLAlchemyError:
            # Fallback for different database types
            logger.debug("Could not query existing indexes, continuing with creation")

    async def _drop_index(self, index_name: str, table_name: str) -> None:
        """Drop an existing index."""
        try:
            sql = f"DROP INDEX IF EXISTS {index_name}"

            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()

            self._existing_indexes.discard(index_name)
            logger.debug(f"Dropped index {index_name}")

        except SQLAlchemyError as e:
            logger.warning(f"Could not drop index {index_name}: {e}")

    async def _analyze_performance_impact(self) -> dict[str, Any]:
        """Analyze the performance impact of created indexes."""
        try:
            with self.engine.connect() as conn:
                # Test query performance on key tables
                test_queries = [
                    "SELECT COUNT(*) FROM fact_sale WHERE date_key >= 20240101",
                    "SELECT COUNT(*) FROM dim_customer WHERE customer_segment = 'VIP'",
                    "SELECT COUNT(*) FROM dim_product WHERE category = 'Electronics'",
                ]

                performance_data = {}

                for i, query in enumerate(test_queries):
                    start = time.time()
                    conn.execute(text(query))
                    duration = time.time() - start
                    performance_data[f'test_query_{i+1}_ms'] = duration * 1000

                return performance_data

        except Exception as e:
            logger.warning(f"Could not analyze performance impact: {e}")
            return {'error': str(e)}

    def get_index_maintenance_queries(self) -> list[str]:
        """Get maintenance queries for index optimization."""
        return [
            # Analyze table statistics
            "ANALYZE fact_sale;",
            "ANALYZE dim_customer;",
            "ANALYZE dim_product;",
            "ANALYZE dim_date;",
            "ANALYZE dim_country;",
            "ANALYZE dim_invoice;",

            # Vacuum for SQLite
            "VACUUM;",
        ]


import time


async def create_performance_indexes(engine: Engine, force_recreate: bool = False) -> dict[str, Any]:
    """
    Convenience function to create all performance indexes.
    
    Args:
        engine: SQLAlchemy engine
        force_recreate: Whether to recreate existing indexes
        
    Returns:
        Creation results and metrics
    """
    manager = IndexManager(engine)
    return await manager.create_all_indexes(force_recreate=force_recreate)
