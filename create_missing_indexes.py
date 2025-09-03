#!/usr/bin/env python3
"""
Create composite indexes based on actual database schema.
"""

import sqlite3
import time
from pathlib import Path


def create_missing_indexes():
    """Create composite indexes based on existing columns."""

    db_path = Path("data/warehouse/retail.db")

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return False

    print("=== Missing Composite Index Creation ===")
    print(f"Database: {db_path}")

    # Index definitions based on actual schema
    indexes = [
        # === FACT_SALE TABLE INDEXES (existing columns only) ===
        {
            'name': 'idx_fact_sale_customer_analytics',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_customer_analytics ON fact_sale (customer_key, date_key, total_amount)',
            'description': 'Customer analytics with revenue data'
        },
        {
            'name': 'idx_fact_sale_invoice_lookup',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_invoice_lookup ON fact_sale (invoice_key, customer_key)',
            'description': 'Invoice detail lookups with customer'
        },
        {
            'name': 'idx_fact_sale_high_value_orders',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_high_value_orders ON fact_sale (total_amount, customer_key, date_key) WHERE total_amount > 10.00',
            'description': 'High-value transactions for analysis'
        },
        {
            'name': 'idx_fact_sale_discount_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_discount_analysis ON fact_sale (discount_amount, product_key, date_key) WHERE discount_amount > 0',
            'description': 'Discount analysis for promotional effectiveness'
        },
        {
            'name': 'idx_fact_sale_unit_price_analysis',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_unit_price_analysis ON fact_sale (unit_price, product_key, quantity)',
            'description': 'Unit price analysis for pricing strategies'
        },
        {
            'name': 'idx_fact_sale_covering_dashboard',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_fact_sale_covering_dashboard ON fact_sale (date_key, customer_key, product_key, total_amount, quantity)',
            'description': 'Covering index for dashboard queries'
        },

        # === DIMENSION TABLE INDEXES (existing columns only) ===

        # Customer dimension
        {
            'name': 'idx_dim_customer_segment_value',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_customer_segment_value ON dim_customer (customer_segment, lifetime_value)',
            'description': 'Customer segment and lifetime value analysis'
        },
        {
            'name': 'idx_dim_customer_high_value',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_customer_high_value ON dim_customer (lifetime_value, customer_segment) WHERE lifetime_value > 100.00',
            'description': 'High-value customers for targeted marketing'
        },
        {
            'name': 'idx_dim_customer_registration_cohort',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_customer_registration_cohort ON dim_customer (registration_date, customer_segment) WHERE registration_date IS NOT NULL',
            'description': 'Customer registration cohort analysis'
        },

        # Product dimension
        {
            'name': 'idx_dim_product_hierarchy',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_product_hierarchy ON dim_product (category, subcategory, brand)',
            'description': 'Product hierarchy navigation'
        },
        {
            'name': 'idx_dim_product_stock_lookup',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_product_stock_lookup ON dim_product (stock_code, category)',
            'description': 'Stock code lookups with category context'
        },
        {
            'name': 'idx_dim_product_category_brand',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_product_category_brand ON dim_product (category, brand) WHERE brand IS NOT NULL',
            'description': 'Category and brand analysis'
        },

        # Date dimension
        {
            'name': 'idx_dim_date_hierarchy',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_date_hierarchy ON dim_date (year, quarter, month)',
            'description': 'Date hierarchy for time-based analysis'
        },
        {
            'name': 'idx_dim_date_business_patterns',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_date_business_patterns ON dim_date (is_weekend, is_holiday, year, month)',
            'description': 'Business pattern analysis'
        },
        {
            'name': 'idx_dim_date_weekend_holidays',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_date_weekend_holidays ON dim_date (date_key, is_weekend, is_holiday)',
            'description': 'Weekend and holiday identification'
        },

        # Country dimension
        {
            'name': 'idx_dim_country_regional',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_country_regional ON dim_country (region, continent)',
            'description': 'Regional analysis by continent'
        },
        {
            'name': 'idx_dim_country_code_lookup',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_country_code_lookup ON dim_country (country_code, region)',
            'description': 'Country code lookups with regional context'
        },

        # Invoice dimension
        {
            'name': 'idx_dim_invoice_processing',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_invoice_processing ON dim_invoice (invoice_date, payment_method, is_cancelled)',
            'description': 'Invoice processing analysis'
        },
        {
            'name': 'idx_dim_invoice_payment_method',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_invoice_payment_method ON dim_invoice (payment_method, is_cancelled) WHERE payment_method IS NOT NULL',
            'description': 'Payment method effectiveness analysis'
        },
        {
            'name': 'idx_dim_invoice_active_only',
            'sql': 'CREATE INDEX IF NOT EXISTS idx_dim_invoice_active_only ON dim_invoice (invoice_no, invoice_date) WHERE is_cancelled = 0',
            'description': 'Active (non-cancelled) invoices'
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
        print("\nUpdating table statistics...")
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
    """Run comprehensive performance tests."""

    test_queries = [
        {
            'name': 'Daily Sales Summary',
            'sql': '''
                SELECT date_key, 
                       COUNT(*) as transactions,
                       SUM(total_amount) as revenue,
                       COUNT(DISTINCT customer_key) as unique_customers,
                       AVG(total_amount) as avg_order_value
                FROM fact_sale 
                WHERE date_key >= 20240101 
                GROUP BY date_key 
                ORDER BY date_key DESC
            '''
        },
        {
            'name': 'Customer Segment Performance',
            'sql': '''
                SELECT c.customer_segment,
                       COUNT(DISTINCT c.customer_key) as customer_count,
                       AVG(c.lifetime_value) as avg_ltv,
                       COUNT(f.sale_id) as total_orders,
                       SUM(f.total_amount) as total_revenue
                FROM dim_customer c
                LEFT JOIN fact_sale f ON c.customer_key = f.customer_key
                WHERE c.customer_segment IS NOT NULL
                GROUP BY c.customer_segment
            '''
        },
        {
            'name': 'Product Category Analysis',
            'sql': '''
                SELECT p.category,
                       p.subcategory,
                       COUNT(DISTINCT p.product_key) as products,
                       SUM(f.total_amount) as revenue,
                       COUNT(f.sale_id) as transactions,
                       AVG(f.unit_price) as avg_price
                FROM dim_product p
                JOIN fact_sale f ON p.product_key = f.product_key
                WHERE p.category IS NOT NULL
                GROUP BY p.category, p.subcategory
                ORDER BY revenue DESC
            '''
        },
        {
            'name': 'Geographic Revenue Analysis',
            'sql': '''
                SELECT co.country_name,
                       co.region,
                       SUM(f.total_amount) as revenue,
                       COUNT(f.sale_id) as transactions,
                       COUNT(DISTINCT f.customer_key) as unique_customers
                FROM dim_country co
                JOIN fact_sale f ON co.country_key = f.country_key
                GROUP BY co.country_name, co.region
                ORDER BY revenue DESC
            '''
        },
        {
            'name': 'High-Value Customer Analysis',
            'sql': '''
                SELECT c.customer_segment,
                       COUNT(*) as high_value_customers,
                       AVG(c.lifetime_value) as avg_ltv,
                       SUM(f.total_amount) as total_revenue
                FROM dim_customer c
                JOIN fact_sale f ON c.customer_key = f.customer_key
                WHERE c.lifetime_value > 100.00
                GROUP BY c.customer_segment
            '''
        },
        {
            'name': 'Weekend vs Weekday Sales',
            'sql': '''
                SELECT d.is_weekend,
                       COUNT(f.sale_id) as transactions,
                       SUM(f.total_amount) as revenue,
                       AVG(f.total_amount) as avg_order_value
                FROM dim_date d
                JOIN fact_sale f ON d.date_key = f.date_key
                GROUP BY d.is_weekend
            '''
        }
    ]

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        total_execution_time = 0
        target_met_count = 0

        for query in test_queries:
            start_time = time.time()
            cursor.execute(query['sql'])
            results = cursor.fetchall()
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            total_execution_time += execution_time

            print(f"  {query['name']}: {execution_time:.2f}ms ({len(results)} rows)")

            if execution_time < 50:
                print("    SUCCESS: Meets <50ms target")
                target_met_count += 1
            else:
                print("    WARNING: Above 50ms target")

        avg_execution_time = total_execution_time / len(test_queries)
        print(f"\n  Average Query Time: {avg_execution_time:.2f}ms")
        print(f"  Queries Meeting Target (<50ms): {target_met_count}/{len(test_queries)}")

        if target_met_count >= len(test_queries) * 0.8:  # 80% threshold
            print(f"  OVERALL: EXCELLENT - {target_met_count/len(test_queries)*100:.0f}% of queries meet target")
        elif target_met_count >= len(test_queries) * 0.6:  # 60% threshold
            print(f"  OVERALL: GOOD - {target_met_count/len(test_queries)*100:.0f}% of queries meet target")
        else:
            print(f"  OVERALL: NEEDS IMPROVEMENT - Only {target_met_count/len(test_queries)*100:.0f}% of queries meet target")

        conn.close()

    except Exception as e:
        print(f"Performance test failed: {e}")

if __name__ == "__main__":
    success = create_missing_indexes()
    print(f"\n=== Final Result: {'SUCCESS' if success else 'PARTIAL SUCCESS'} ===")
