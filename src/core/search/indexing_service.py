"""
PwC Retail Data Platform - Elasticsearch Indexing Service
Handles indexing of retail data into Elasticsearch for search and analytics
"""

from datetime import datetime
from typing import Any

from sqlalchemy import text

from data_access.db import session_scope

from ..logging import get_logger
from .elasticsearch_client import get_elasticsearch_client

logger = get_logger(__name__)


class ElasticsearchIndexingService:
    """Service for indexing retail data into Elasticsearch"""

    def __init__(self):
        self.es_client = get_elasticsearch_client()

        # Index configurations
        self.indexes = {
            "retail-transactions": {
                "mappings": {
                    "properties": {
                        "invoice_no": {"type": "keyword"},
                        "stock_code": {"type": "keyword"},
                        "product_name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "description": {"type": "text"},
                        "quantity": {"type": "integer"},
                        "unit_price": {"type": "float"},
                        "quantity_price": {"type": "float"},
                        "customer_id": {"type": "keyword"},
                        "customer_name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "country": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "order_date": {"type": "date"},
                        "category": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "brand": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "season": {"type": "keyword"},
                        "profit_margin": {"type": "float"},
                        "discount_applied": {"type": "boolean"},
                        "is_weekend": {"type": "boolean"},
                        "month_name": {"type": "keyword"},
                        "quarter": {"type": "keyword"},
                        "customer_segment": {"type": "keyword"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }
            },
            "retail-customers": {
                "mappings": {
                    "properties": {
                        "customer_id": {"type": "keyword"},
                        "customer_name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "country": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "first_order_date": {"type": "date"},
                        "last_order_date": {"type": "date"},
                        "total_orders": {"type": "integer"},
                        "total_spent": {"type": "float"},
                        "avg_order_value": {"type": "float"},
                        "customer_segment": {"type": "keyword"},
                        "rfm_score": {"type": "integer"},
                        "recency_score": {"type": "integer"},
                        "frequency_score": {"type": "integer"},
                        "monetary_score": {"type": "integer"},
                        "clv_prediction": {"type": "float"},
                        "is_active": {"type": "boolean"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }
            },
            "retail-products": {
                "mappings": {
                    "properties": {
                        "stock_code": {"type": "keyword"},
                        "product_name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "description": {"type": "text"},
                        "unit_price": {"type": "float"},
                        "category": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "brand": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}}
                        },
                        "season": {"type": "keyword"},
                        "total_sold": {"type": "integer"},
                        "total_revenue": {"type": "float"},
                        "avg_rating": {"type": "float"},
                        "is_active": {"type": "boolean"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }
            }
        }

    async def setup_indexes(self) -> dict[str, bool]:
        """Setup all Elasticsearch indexes"""
        results = {}

        for index_name, config in self.indexes.items():
            try:
                # Delete existing index if it exists
                await self.es_client.delete_index(index_name)

                # Create new index with mapping
                success = await self.es_client.create_index(index_name, config)
                results[index_name] = success

                if success:
                    logger.info(f"Successfully set up index: {index_name}")
                else:
                    logger.error(f"Failed to set up index: {index_name}")

            except Exception as e:
                logger.error(f"Error setting up index {index_name}: {e}")
                results[index_name] = False

        return results

    async def index_transactions(self, batch_size: int = 1000) -> dict[str, Any]:
        """Index retail transactions from the database"""
        try:
            with session_scope() as session:
                # Query transactions with all relevant fields
                query = """
                    SELECT 
                        invoice_no,
                        stock_code,
                        COALESCE(description, 'Unknown Product') as product_name,
                        description,
                        quantity,
                        unit_price,
                        (quantity * unit_price) as quantity_price,
                        customer_id,
                        CASE 
                            WHEN customer_id IS NOT NULL 
                            THEN ('Customer ' || customer_id)
                            ELSE 'Anonymous'
                        END as customer_name,
                        country,
                        invoice_date as order_date,
                        CASE 
                            WHEN UPPER(description) LIKE '%GIFT%' THEN 'Gifts'
                            WHEN UPPER(description) LIKE '%BAG%' THEN 'Bags'
                            WHEN UPPER(description) LIKE '%SET%' THEN 'Sets'
                            WHEN UPPER(description) LIKE '%LIGHT%' THEN 'Lighting'
                            WHEN UPPER(description) LIKE '%CANDLE%' THEN 'Candles'
                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'
                            ELSE 'General'
                        END as category,
                        CASE 
                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'
                            WHEN UPPER(description) LIKE '%REGENCY%' THEN 'Regency'
                            WHEN UPPER(description) LIKE '%FRENCH%' THEN 'French'
                            ELSE 'House Brand'
                        END as brand,
                        CASE 
                            WHEN CAST(strftime('%m', invoice_date) AS INTEGER) IN (12, 1, 2) THEN 'Winter'
                            WHEN CAST(strftime('%m', invoice_date) AS INTEGER) IN (3, 4, 5) THEN 'Spring'
                            WHEN CAST(strftime('%m', invoice_date) AS INTEGER) IN (6, 7, 8) THEN 'Summer'
                            ELSE 'Fall'
                        END as season,
                        CASE 
                            WHEN unit_price > 50 THEN 0.3
                            WHEN unit_price > 20 THEN 0.2
                            ELSE 0.1
                        END as profit_margin,
                        (unit_price < 5) as discount_applied,
                        CAST(strftime('%w', invoice_date) AS INTEGER) IN (0, 6) as is_weekend,
                        strftime('%B', invoice_date) as month_name,
                        ('Q' || CASE 
                            WHEN CAST(strftime('%m', invoice_date) AS INTEGER) BETWEEN 1 AND 3 THEN '1'
                            WHEN CAST(strftime('%m', invoice_date) AS INTEGER) BETWEEN 4 AND 6 THEN '2'
                            WHEN CAST(strftime('%m', invoice_date) AS INTEGER) BETWEEN 7 AND 9 THEN '3'
                            ELSE '4'
                        END) as quarter,
                        CASE 
                            WHEN (quantity * unit_price) > 100 THEN 'Premium'
                            WHEN (quantity * unit_price) > 50 THEN 'Standard'
                            ELSE 'Budget'
                        END as customer_segment,
                        datetime('now') as created_at,
                        datetime('now') as updated_at
                    FROM retail_invoices 
                    WHERE quantity > 0 
                      AND unit_price > 0
                      AND customer_id IS NOT NULL
                    ORDER BY invoice_date DESC
                    LIMIT 10000
                """

                result = session.execute(text(query))
                transactions = result.fetchall()

                if not transactions:
                    logger.warning("No transactions found to index")
                    return {
                        "status": "completed",
                        "total": 0,
                        "indexed": 0,
                        "errors": 0
                    }

                # Convert to dictionaries
                documents = []
                for row in transactions:
                    doc = dict(row._asdict())
                    # Convert datetime objects to ISO format
                    for key, value in doc.items():
                        if isinstance(value, datetime):
                            doc[key] = value.isoformat()
                    documents.append(doc)

                # Bulk index documents
                logger.info(f"Indexing {len(documents)} transactions")
                result = await self.es_client.bulk_index(
                    "retail-transactions",
                    documents,
                    id_field="invoice_no"
                )

                return {
                    "status": "completed",
                    "total": result["total"],
                    "indexed": result["successful"],
                    "errors": result["errors"],
                    "error_details": result.get("error_details", [])
                }

        except Exception as e:
            logger.error(f"Error indexing transactions: {e}")
            return {
                "status": "error",
                "error": str(e),
                "total": 0,
                "indexed": 0,
                "errors": 0
            }

    async def index_customers(self) -> dict[str, Any]:
        """Index customer data with analytics"""
        try:
            with session_scope() as session:
                # Query customer aggregates
                query = """
                    WITH customer_stats AS (
                        SELECT 
                            customer_id,
                            MIN(invoice_date) as first_order_date,
                            MAX(invoice_date) as last_order_date,
                            COUNT(DISTINCT invoice_no) as total_orders,
                            SUM(quantity * unit_price) as total_spent,
                            AVG(quantity * unit_price) as avg_order_value,
                            country
                        FROM retail_invoices 
                        WHERE customer_id IS NOT NULL
                          AND quantity > 0 
                          AND unit_price > 0
                        GROUP BY customer_id, country
                    )
                    SELECT 
                        customer_id,
                        ('Customer ' || customer_id) as customer_name,
                        country,
                        first_order_date,
                        last_order_date,
                        total_orders,
                        total_spent,
                        avg_order_value,
                        CASE 
                            WHEN total_spent > 1000 THEN 'Premium'
                            WHEN total_spent > 500 THEN 'Standard'
                            ELSE 'Budget'
                        END as customer_segment,
                        CASE 
                            WHEN total_orders >= 10 AND total_spent >= 500 THEN 5
                            WHEN total_orders >= 5 AND total_spent >= 200 THEN 4
                            WHEN total_orders >= 3 THEN 3
                            WHEN total_orders >= 2 THEN 2
                            ELSE 1
                        END as rfm_score,
                        CASE 
                            WHEN julianday('now') - julianday(last_order_date) <= 30 THEN 5
                            WHEN julianday('now') - julianday(last_order_date) <= 60 THEN 4
                            WHEN julianday('now') - julianday(last_order_date) <= 90 THEN 3
                            WHEN julianday('now') - julianday(last_order_date) <= 180 THEN 2
                            ELSE 1
                        END as recency_score,
                        CASE 
                            WHEN total_orders >= 20 THEN 5
                            WHEN total_orders >= 10 THEN 4
                            WHEN total_orders >= 5 THEN 3
                            WHEN total_orders >= 2 THEN 2
                            ELSE 1
                        END as frequency_score,
                        CASE 
                            WHEN total_spent >= 2000 THEN 5
                            WHEN total_spent >= 1000 THEN 4
                            WHEN total_spent >= 500 THEN 3
                            WHEN total_spent >= 100 THEN 2
                            ELSE 1
                        END as monetary_score,
                        total_spent * 1.2 as clv_prediction,
                        (julianday('now') - julianday(last_order_date) <= 90) as is_active,
                        datetime('now') as created_at,
                        datetime('now') as updated_at
                    FROM customer_stats
                    ORDER BY total_spent DESC
                    LIMIT 5000
                """

                result = session.execute(text(query))
                customers = result.fetchall()

                if not customers:
                    logger.warning("No customers found to index")
                    return {
                        "status": "completed",
                        "total": 0,
                        "indexed": 0,
                        "errors": 0
                    }

                # Convert to dictionaries
                documents = []
                for row in customers:
                    doc = dict(row._asdict())
                    # Convert datetime objects to ISO format
                    for key, value in doc.items():
                        if isinstance(value, datetime):
                            doc[key] = value.isoformat()
                    documents.append(doc)

                # Bulk index documents
                logger.info(f"Indexing {len(documents)} customers")
                result = await self.es_client.bulk_index(
                    "retail-customers",
                    documents,
                    id_field="customer_id"
                )

                return {
                    "status": "completed",
                    "total": result["total"],
                    "indexed": result["successful"],
                    "errors": result["errors"],
                    "error_details": result.get("error_details", [])
                }

        except Exception as e:
            logger.error(f"Error indexing customers: {e}")
            return {
                "status": "error",
                "error": str(e),
                "total": 0,
                "indexed": 0,
                "errors": 0
            }

    async def index_products(self) -> dict[str, Any]:
        """Index product data with analytics"""
        try:
            with session_scope() as session:
                # Query product aggregates
                query = """
                    WITH product_stats AS (
                        SELECT 
                            stock_code,
                            description,
                            AVG(unit_price) as unit_price,
                            SUM(quantity) as total_sold,
                            SUM(quantity * unit_price) as total_revenue,
                            COUNT(DISTINCT invoice_no) as order_count
                        FROM retail_invoices 
                        WHERE stock_code IS NOT NULL
                          AND description IS NOT NULL
                          AND quantity > 0 
                          AND unit_price > 0
                        GROUP BY stock_code, description
                    )
                    SELECT 
                        stock_code,
                        description as product_name,
                        description,
                        unit_price,
                        CASE 
                            WHEN UPPER(description) LIKE '%GIFT%' THEN 'Gifts'
                            WHEN UPPER(description) LIKE '%BAG%' THEN 'Bags'
                            WHEN UPPER(description) LIKE '%SET%' THEN 'Sets'
                            WHEN UPPER(description) LIKE '%LIGHT%' THEN 'Lighting'
                            WHEN UPPER(description) LIKE '%CANDLE%' THEN 'Candles'
                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'
                            ELSE 'General'
                        END as category,
                        CASE 
                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'
                            WHEN UPPER(description) LIKE '%REGENCY%' THEN 'Regency'
                            WHEN UPPER(description) LIKE '%FRENCH%' THEN 'French'
                            ELSE 'House Brand'
                        END as brand,
                        'All Season' as season,
                        total_sold,
                        total_revenue,
                        CASE 
                            WHEN order_count >= 100 THEN 4.5
                            WHEN order_count >= 50 THEN 4.0
                            WHEN order_count >= 20 THEN 3.5
                            WHEN order_count >= 10 THEN 3.0
                            ELSE 2.5
                        END as avg_rating,
                        (total_sold > 0) as is_active,
                        datetime('now') as created_at,
                        datetime('now') as updated_at
                    FROM product_stats
                    WHERE total_sold > 5
                    ORDER BY total_revenue DESC
                    LIMIT 5000
                """

                result = session.execute(text(query))
                products = result.fetchall()

                if not products:
                    logger.warning("No products found to index")
                    return {
                        "status": "completed",
                        "total": 0,
                        "indexed": 0,
                        "errors": 0
                    }

                # Convert to dictionaries
                documents = []
                for row in products:
                    doc = dict(row._asdict())
                    # Convert datetime objects to ISO format
                    for key, value in doc.items():
                        if isinstance(value, datetime):
                            doc[key] = value.isoformat()
                    documents.append(doc)

                # Bulk index documents
                logger.info(f"Indexing {len(documents)} products")
                result = await self.es_client.bulk_index(
                    "retail-products",
                    documents,
                    id_field="stock_code"
                )

                return {
                    "status": "completed",
                    "total": result["total"],
                    "indexed": result["successful"],
                    "errors": result["errors"],
                    "error_details": result.get("error_details", [])
                }

        except Exception as e:
            logger.error(f"Error indexing products: {e}")
            return {
                "status": "error",
                "error": str(e),
                "total": 0,
                "indexed": 0,
                "errors": 0
            }

    async def reindex_all(self) -> dict[str, Any]:
        """Reindex all data types"""
        logger.info("Starting complete reindexing process")

        results = {
            "started_at": datetime.utcnow().isoformat(),
            "setup_indexes": {},
            "transactions": {},
            "customers": {},
            "products": {},
            "completed_at": None
        }

        try:
            # Setup indexes
            results["setup_indexes"] = await self.setup_indexes()

            # Index data
            results["transactions"] = await self.index_transactions()
            results["customers"] = await self.index_customers()
            results["products"] = await self.index_products()

            results["completed_at"] = datetime.utcnow().isoformat()
            logger.info("Reindexing completed successfully")

        except Exception as e:
            logger.error(f"Error during reindexing: {e}")
            results["error"] = str(e)
            results["completed_at"] = datetime.utcnow().isoformat()

        return results


# Global service instance
_indexing_service = None


def get_indexing_service() -> ElasticsearchIndexingService:
    """Get global indexing service instance"""
    global _indexing_service
    if _indexing_service is None:
        _indexing_service = ElasticsearchIndexingService()
    return _indexing_service
