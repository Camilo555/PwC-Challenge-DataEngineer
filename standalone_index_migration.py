#!/usr/bin/env python3
"""
Standalone script to create composite indexes for the retail data warehouse.
"""

import sqlite3
import time
from pathlib import Path


def create_composite_indexes():
    """Create composite indexes for optimal analytical query performance."""

    # Database path
    db_path = Path("data/warehouse/retail.db")

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return False

    print("=== Composite Index Creation ===")
    print(f"Database: {db_path}")

    # Index definitions
    indexes = [
        # === FACT_SALE TABLE INDEXES ===
        {
            'name': 'idx_fact_sale_product_date_customer_perf',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_product_date_customer_perf ON fact_sale (product_key, date_key, customer_key)',
            'description': 'Product-date-customer composite for product performance analysis'
        },
        {
            'name': 'idx_fact_sale_date_customer_product_perf',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_date_customer_product_perf ON fact_sale (date_key, customer_key, product_key)',
            'description': 'Date-customer-product composite for time-based customer analysis'
        },
        {
            'name': 'idx_fact_sale_customer_value_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_customer_value_analysis ON fact_sale (customer_key, date_key, total_amount, profit_amount)',
            'description': 'Customer value analysis with amounts for RFM calculations'
        },
        {
            'name': 'idx_fact_sale_product_revenue_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_product_revenue_analysis ON fact_sale (product_key, date_key, total_amount, quantity)',
            'description': 'Product revenue and quantity analysis'
        },
        {
            'name': 'idx_fact_sale_country_time_revenue',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_country_time_revenue ON fact_sale (country_key, date_key, total_amount)',
            'description': 'Geographic revenue analysis by country and time'
        },
        {
            'name': 'idx_fact_sale_monthly_aggregation',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_monthly_aggregation ON fact_sale (date_key, country_key, product_key, total_amount) WHERE date_key >= 20240101',
            'description': 'Monthly aggregation index for recent data'
        },
        {
            'name': 'idx_fact_sale_high_value_customers',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_high_value_customers ON fact_sale (customer_key, date_key, total_amount, profit_amount) WHERE total_amount > 100.00 AND customer_key IS NOT NULL',
            'description': 'High-value customer transactions for VIP analysis'
        },
        {
            'name': 'idx_fact_sale_recent_profitable',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_recent_profitable ON fact_sale (product_key, profit_amount, date_key) WHERE profit_amount > 0 AND date_key >= 20240101',
            'description': 'Recent profitable transactions for product performance'
        },
        {
            'name': 'idx_fact_sale_bulk_orders',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_bulk_orders ON fact_sale (quantity, customer_key, product_key, date_key) WHERE quantity >= 10',
            'description': 'Bulk order analysis for B2B customer identification'
        },

        # === DIMENSION TABLE INDEXES ===

        # Customer dimension
        {
            'name': 'idx_dim_customer_rfm_segment_value',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_customer_rfm_segment_value ON dim_customer (rfm_segment, customer_segment, lifetime_value, is_current)',
            'description': 'RFM and customer segment analysis with current status'
        },
        {
            'name': 'idx_dim_customer_behavioral_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_customer_behavioral_analysis ON dim_customer (customer_segment, first_purchase_date, last_purchase_date, total_orders) WHERE is_current = 1',
            'description': 'Customer behavioral pattern analysis for active customers'
        },
        {
            'name': 'idx_dim_customer_active_high_value',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_customer_active_high_value ON dim_customer (lifetime_value, customer_segment, total_orders) WHERE is_current = 1 AND lifetime_value > 500.00',
            'description': 'Active high-value customers for targeted marketing'
        },

        # Product dimension
        {
            'name': 'idx_dim_product_hierarchy_complete',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_product_hierarchy_complete ON dim_product (category, subcategory, brand, product_family, is_current)',
            'description': 'Complete product hierarchy navigation'
        },
        {
            'name': 'idx_dim_product_pricing_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_product_pricing_analysis ON dim_product (category, recommended_price, unit_cost, brand) WHERE is_current = 1 AND recommended_price IS NOT NULL',
            'description': 'Product pricing analysis by category and brand'
        },
        {
            'name': 'idx_dim_product_active_premium',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_product_active_premium ON dim_product (category, brand, recommended_price) WHERE is_current = 1 AND recommended_price > 50.00',
            'description': 'Active premium products for category management'
        },

        # Date dimension
        {
            'name': 'idx_dim_date_fiscal_hierarchy',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_date_fiscal_hierarchy ON dim_date (fiscal_year, fiscal_quarter, year, month) WHERE fiscal_year IS NOT NULL',
            'description': 'Complete fiscal hierarchy for fiscal reporting'
        },
        {
            'name': 'idx_dim_date_business_pattern_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_date_business_pattern_analysis ON dim_date (year, month, is_weekend, is_holiday, day_of_week)',
            'description': 'Business pattern analysis (weekends, holidays, seasonality)'
        },
        {
            'name': 'idx_dim_date_recent_business_days',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_date_recent_business_days ON dim_date (date_key, is_weekend, is_holiday) WHERE date_key >= 20240101',
            'description': 'Recent business days for dashboard queries'
        },

        # Country dimension
        {
            'name': 'idx_dim_country_regional_economic_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_country_regional_economic_analysis ON dim_country (region, continent, gdp_per_capita, currency_code)',
            'description': 'Regional economic analysis by GDP and currency'
        },
        {
            'name': 'idx_dim_country_business_environment',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_country_business_environment ON dim_country (is_eu_member, tax_rate, currency_code, region)',
            'description': 'Business environment analysis (EU membership, taxes)'
        },

        # Invoice dimension
        {
            'name': 'idx_dim_invoice_payment_performance',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_invoice_payment_performance ON dim_invoice (payment_status, payment_method, processing_time_minutes, invoice_total)',
            'description': 'Payment processing performance analysis'
        },
        {
            'name': 'idx_dim_invoice_channel_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_invoice_channel_analysis ON dim_invoice (channel, invoice_date, invoice_total, payment_status) WHERE is_cancelled = false',
            'description': 'Sales channel performance analysis for non-cancelled invoices'
        }
    ]

    # Connect to database and create indexes
    start_time = time.time()
    created_count = 0
    failed_count = 0
    failed_indexes = []

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if tables exist first
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0] for row in cursor.fetchall()}

        print(f"Found tables: {sorted(existing_tables)}")

        for index_def in indexes:
            try:
                index_start = time.time()
                cursor.execute(index_def['sql'])
                conn.commit()
                creation_time = time.time() - index_start

                created_count += 1
                print(f"SUCCESS: Created {index_def['name']} in {creation_time:.3f}s")
                print(f"  Description: {index_def['description']}")

            except sqlite3.Error as e:
                failed_count += 1
                failed_indexes.append({
                    'name': index_def['name'],
                    'error': str(e)
                })
                print(f"FAILED: {index_def['name']} - {e}")

        # Update table statistics
        cursor.execute("ANALYZE")
        conn.commit()

        conn.close()

        total_time = time.time() - start_time

        print("\n=== Migration Results ===")
        print(f"Total Time: {total_time:.2f}s")
        print(f"Indexes Created: {created_count}")
        print(f"Indexes Failed: {failed_count}")

        if failed_indexes:
            print("\nFailed Indexes:")
            for failed in failed_indexes:
                print(f"  - {failed['name']}: {failed['error']}")

        # Run performance test
        print("\n=== Performance Test ===")
        run_performance_test(db_path)

        return failed_count == 0

    except Exception as e:
        print(f"ERROR: Database connection failed: {e}")
        return False

def run_performance_test(db_path):
    """Run basic performance tests on the indexed database."""

    test_queries = [
        {
            'name': 'Daily Sales Summary',
            'sql': '''
                SELECT date_key, 
                       COUNT(*) as transactions,
                       SUM(total_amount) as revenue,
                       COUNT(DISTINCT customer_key) as unique_customers
                FROM fact_sale 
                WHERE date_key >= 20240101 
                GROUP BY date_key 
                ORDER BY date_key DESC 
                LIMIT 30
            '''
        },
        {
            'name': 'Customer Segment Analysis',
            'sql': '''
                SELECT customer_segment,
                       COUNT(*) as customer_count,
                       AVG(lifetime_value) as avg_ltv
                FROM dim_customer 
                WHERE is_current = 1 
                GROUP BY customer_segment
            '''
        },
        {
            'name': 'Product Performance by Category',
            'sql': '''
                SELECT p.category,
                       COUNT(DISTINCT p.product_key) as products,
                       SUM(f.total_amount) as revenue,
                       COUNT(*) as transactions
                FROM dim_product p
                JOIN fact_sale f ON p.product_key = f.product_key
                WHERE p.is_current = 1 AND f.date_key >= 20240101
                GROUP BY p.category
                ORDER BY revenue DESC
            '''
        }
    ]

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for query in test_queries:
            start_time = time.time()
            cursor.execute(query['sql'])
            results = cursor.fetchall()
            execution_time = (time.time() - start_time) * 1000  # Convert to ms

            print(f"  {query['name']}: {execution_time:.2f}ms ({len(results)} rows)")

            if execution_time < 50:
                print("    SUCCESS: Meets <50ms target")
            else:
                print("    WARNING: Above 50ms target")

        conn.close()

    except Exception as e:
        print(f"Performance test failed: {e}")

if __name__ == "__main__":
    success = create_composite_indexes()
    print(f"\n=== Final Result: {'SUCCESS' if success else 'PARTIAL SUCCESS'} ===")
