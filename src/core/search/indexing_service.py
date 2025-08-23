"""
PwC Retail Data Platform - Elasticsearch Indexing Service
Handles indexing of retail data into Elasticsearch for search and analytics
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
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
                            "analyzer": "english",
                            "fields": {
                                "keyword": {"type": "keyword"},
                                "search": {
                                    "type": "text",
                                    "analyzer": "standard"
                                }
                            }
                        },
                        "description": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "quantity": {"type": "integer"},
                        "unit_price": {"type": "float"},
                        "quantity_price": {"type": "float"},
                        "customer_id": {"type": "keyword"},
                        "customer_name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "country": {"type": "keyword"},
                        "order_date": {"type": "date"},
                        "category": {"type": "keyword"},
                        "brand": {"type": "keyword"},
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
                },
                "settings": {
                    "number_of_shards": 2,
                    "number_of_replicas": 1,
                    "analysis": {
                        "analyzer": {
                            "product_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "stop",
                                    "snowball",
                                    "word_delimiter"
                                ]
                            }
                        }
                    }
                }
            },
            "retail-customers": {
                "mappings": {
                    "properties": {
                        "customer_id": {"type": "keyword"},
                        "customer_name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "country": {"type": "keyword"},
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
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            },
            "retail-products": {
                "mappings": {
                    "properties": {
                        "stock_code": {"type": "keyword"},
                        "product_name": {
                            "type": "text",
                            "analyzer": "product_analyzer",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "description": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "unit_price": {"type": "float"},
                        "category": {"type": "keyword"},
                        "brand": {"type": "keyword"},
                        "season": {"type": "keyword"},
                        "total_sold": {"type": "integer"},
                        "total_revenue": {"type": "float"},
                        "avg_rating": {"type": "float"},
                        "is_active": {"type": "boolean"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "analysis": {
                        "analyzer": {
                            "product_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "stop",
                                    "snowball",
                                    "word_delimiter"
                                ]
                            }
                        }
                    }
                }
            }
        }
    
    async def setup_indexes(self) -> Dict[str, bool]:
        """Setup all Elasticsearch indexes"""
        results = {}
        
        for index_name, config in self.indexes.items():
            try:
                success = await self.es_client.create_index(index_name, config)
                results[index_name] = success
                logger.info(f"Index setup result for {index_name}: {success}")
            except Exception as e:
                logger.error(f"Error setting up index {index_name}: {e}")
                results[index_name] = False
        
        return results
    
    async def index_transactions(self, batch_size: int = 1000) -> Dict[str, Any]:
        """Index retail transactions from the database"""
        try:
            async with get_session() as session:
                # Query transactions with all relevant fields
                query = \"\"\"\n                    SELECT \n                        invoice_no,\n                        stock_code,\n                        COALESCE(description, 'Unknown Product') as product_name,\n                        description,\n                        quantity,\n                        unit_price,\n                        (quantity * unit_price) as quantity_price,\n                        customer_id,\n                        CASE \n                            WHEN customer_id IS NOT NULL \n                            THEN CONCAT('Customer ', customer_id)\n                            ELSE 'Anonymous'\n                        END as customer_name,\n                        country,\n                        invoice_date as order_date,\n                        CASE \n                            WHEN UPPER(description) LIKE '%GIFT%' THEN 'Gifts'\n                            WHEN UPPER(description) LIKE '%BAG%' THEN 'Bags'\n                            WHEN UPPER(description) LIKE '%SET%' THEN 'Sets'\n                            WHEN UPPER(description) LIKE '%LIGHT%' THEN 'Lighting'\n                            WHEN UPPER(description) LIKE '%CANDLE%' THEN 'Candles'\n                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'\n                            ELSE 'General'\n                        END as category,\n                        CASE \n                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'\n                            WHEN UPPER(description) LIKE '%REGENCY%' THEN 'Regency'\n                            WHEN UPPER(description) LIKE '%FRENCH%' THEN 'French'\n                            ELSE 'House Brand'\n                        END as brand,\n                        CASE \n                            WHEN EXTRACT(MONTH FROM invoice_date) IN (12, 1, 2) THEN 'Winter'\n                            WHEN EXTRACT(MONTH FROM invoice_date) IN (3, 4, 5) THEN 'Spring'\n                            WHEN EXTRACT(MONTH FROM invoice_date) IN (6, 7, 8) THEN 'Summer'\n                            ELSE 'Fall'\n                        END as season,\n                        CASE \n                            WHEN unit_price > 50 THEN 0.3\n                            WHEN unit_price > 20 THEN 0.2\n                            ELSE 0.1\n                        END as profit_margin,\n                        unit_price < 5 as discount_applied,\n                        EXTRACT(DOW FROM invoice_date) IN (0, 6) as is_weekend,\n                        TO_CHAR(invoice_date, 'Month') as month_name,\n                        CONCAT('Q', EXTRACT(QUARTER FROM invoice_date)) as quarter,\n                        CASE \n                            WHEN (quantity * unit_price) > 100 THEN 'Premium'\n                            WHEN (quantity * unit_price) > 50 THEN 'Standard'\n                            ELSE 'Budget'\n                        END as customer_segment,\n                        NOW() as created_at,\n                        NOW() as updated_at\n                    FROM retail_invoices \n                    WHERE quantity > 0 \n                      AND unit_price > 0\n                      AND customer_id IS NOT NULL\n                    ORDER BY invoice_date DESC\n                    LIMIT 10000\n                \"\"\"\n                \n                result = await session.execute(query)\n                transactions = result.fetchall()\n                \n                if not transactions:\n                    logger.warning(\"No transactions found to index\")\n                    return {\n                        \"status\": \"completed\",\n                        \"total\": 0,\n                        \"indexed\": 0,\n                        \"errors\": 0\n                    }\n                \n                # Convert to dictionaries\n                documents = []\n                for row in transactions:\n                    doc = dict(row._asdict())\n                    # Convert datetime objects to ISO format\n                    for key, value in doc.items():\n                        if isinstance(value, datetime):\n                            doc[key] = value.isoformat()\n                    documents.append(doc)\n                \n                # Bulk index documents\n                logger.info(f\"Indexing {len(documents)} transactions\")\n                result = await self.es_client.bulk_index(\n                    \"retail-transactions\",\n                    documents,\n                    id_field=\"invoice_no\"\n                )\n                \n                return {\n                    \"status\": \"completed\",\n                    \"total\": result[\"total\"],\n                    \"indexed\": result[\"successful\"],\n                    \"errors\": result[\"errors\"],\n                    \"error_details\": result.get(\"error_details\", [])\n                }\n                \n        except Exception as e:\n            logger.error(f\"Error indexing transactions: {e}\")\n            return {\n                \"status\": \"error\",\n                \"error\": str(e),\n                \"total\": 0,\n                \"indexed\": 0,\n                \"errors\": 0\n            }\n    \n    async def index_customers(self) -> Dict[str, Any]:\n        \"\"\"Index customer data with analytics\"\"\"\n        try:\n            async with get_session() as session:\n                # Query customer aggregates\n                query = \"\"\"\n                    WITH customer_stats AS (\n                        SELECT \n                            customer_id,\n                            MIN(invoice_date) as first_order_date,\n                            MAX(invoice_date) as last_order_date,\n                            COUNT(DISTINCT invoice_no) as total_orders,\n                            SUM(quantity * unit_price) as total_spent,\n                            AVG(quantity * unit_price) as avg_order_value,\n                            country\n                        FROM retail_invoices \n                        WHERE customer_id IS NOT NULL\n                          AND quantity > 0 \n                          AND unit_price > 0\n                        GROUP BY customer_id, country\n                    )\n                    SELECT \n                        customer_id,\n                        CONCAT('Customer ', customer_id) as customer_name,\n                        country,\n                        first_order_date,\n                        last_order_date,\n                        total_orders,\n                        total_spent,\n                        avg_order_value,\n                        CASE \n                            WHEN total_spent > 1000 THEN 'Premium'\n                            WHEN total_spent > 500 THEN 'Standard'\n                            ELSE 'Budget'\n                        END as customer_segment,\n                        CASE \n                            WHEN total_orders >= 10 AND total_spent >= 500 THEN 5\n                            WHEN total_orders >= 5 AND total_spent >= 200 THEN 4\n                            WHEN total_orders >= 3 THEN 3\n                            WHEN total_orders >= 2 THEN 2\n                            ELSE 1\n                        END as rfm_score,\n                        CASE \n                            WHEN CURRENT_DATE - last_order_date <= 30 THEN 5\n                            WHEN CURRENT_DATE - last_order_date <= 60 THEN 4\n                            WHEN CURRENT_DATE - last_order_date <= 90 THEN 3\n                            WHEN CURRENT_DATE - last_order_date <= 180 THEN 2\n                            ELSE 1\n                        END as recency_score,\n                        CASE \n                            WHEN total_orders >= 20 THEN 5\n                            WHEN total_orders >= 10 THEN 4\n                            WHEN total_orders >= 5 THEN 3\n                            WHEN total_orders >= 2 THEN 2\n                            ELSE 1\n                        END as frequency_score,\n                        CASE \n                            WHEN total_spent >= 2000 THEN 5\n                            WHEN total_spent >= 1000 THEN 4\n                            WHEN total_spent >= 500 THEN 3\n                            WHEN total_spent >= 100 THEN 2\n                            ELSE 1\n                        END as monetary_score,\n                        total_spent * 1.2 as clv_prediction,\n                        CURRENT_DATE - last_order_date <= 90 as is_active,\n                        NOW() as created_at,\n                        NOW() as updated_at\n                    FROM customer_stats\n                    ORDER BY total_spent DESC\n                    LIMIT 5000\n                \"\"\"\n                \n                result = await session.execute(query)\n                customers = result.fetchall()\n                \n                if not customers:\n                    logger.warning(\"No customers found to index\")\n                    return {\n                        \"status\": \"completed\",\n                        \"total\": 0,\n                        \"indexed\": 0,\n                        \"errors\": 0\n                    }\n                \n                # Convert to dictionaries\n                documents = []\n                for row in customers:\n                    doc = dict(row._asdict())\n                    # Convert datetime objects to ISO format\n                    for key, value in doc.items():\n                        if isinstance(value, datetime):\n                            doc[key] = value.isoformat()\n                    documents.append(doc)\n                \n                # Bulk index documents\n                logger.info(f\"Indexing {len(documents)} customers\")\n                result = await self.es_client.bulk_index(\n                    \"retail-customers\",\n                    documents,\n                    id_field=\"customer_id\"\n                )\n                \n                return {\n                    \"status\": \"completed\",\n                    \"total\": result[\"total\"],\n                    \"indexed\": result[\"successful\"],\n                    \"errors\": result[\"errors\"],\n                    \"error_details\": result.get(\"error_details\", [])\n                }\n                \n        except Exception as e:\n            logger.error(f\"Error indexing customers: {e}\")\n            return {\n                \"status\": \"error\",\n                \"error\": str(e),\n                \"total\": 0,\n                \"indexed\": 0,\n                \"errors\": 0\n            }\n    \n    async def index_products(self) -> Dict[str, Any]:\n        \"\"\"Index product data with analytics\"\"\"\n        try:\n            async with get_session() as session:\n                # Query product aggregates\n                query = \"\"\"\n                    WITH product_stats AS (\n                        SELECT \n                            stock_code,\n                            description,\n                            AVG(unit_price) as unit_price,\n                            SUM(quantity) as total_sold,\n                            SUM(quantity * unit_price) as total_revenue,\n                            COUNT(DISTINCT invoice_no) as order_count\n                        FROM retail_invoices \n                        WHERE stock_code IS NOT NULL\n                          AND description IS NOT NULL\n                          AND quantity > 0 \n                          AND unit_price > 0\n                        GROUP BY stock_code, description\n                    )\n                    SELECT \n                        stock_code,\n                        description as product_name,\n                        description,\n                        unit_price,\n                        CASE \n                            WHEN UPPER(description) LIKE '%GIFT%' THEN 'Gifts'\n                            WHEN UPPER(description) LIKE '%BAG%' THEN 'Bags'\n                            WHEN UPPER(description) LIKE '%SET%' THEN 'Sets'\n                            WHEN UPPER(description) LIKE '%LIGHT%' THEN 'Lighting'\n                            WHEN UPPER(description) LIKE '%CANDLE%' THEN 'Candles'\n                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'\n                            ELSE 'General'\n                        END as category,\n                        CASE \n                            WHEN UPPER(description) LIKE '%VINTAGE%' THEN 'Vintage'\n                            WHEN UPPER(description) LIKE '%REGENCY%' THEN 'Regency'\n                            WHEN UPPER(description) LIKE '%FRENCH%' THEN 'French'\n                            ELSE 'House Brand'\n                        END as brand,\n                        'All Season' as season,\n                        total_sold,\n                        total_revenue,\n                        CASE \n                            WHEN order_count >= 100 THEN 4.5\n                            WHEN order_count >= 50 THEN 4.0\n                            WHEN order_count >= 20 THEN 3.5\n                            WHEN order_count >= 10 THEN 3.0\n                            ELSE 2.5\n                        END as avg_rating,\n                        total_sold > 0 as is_active,\n                        NOW() as created_at,\n                        NOW() as updated_at\n                    FROM product_stats\n                    WHERE total_sold > 5\n                    ORDER BY total_revenue DESC\n                    LIMIT 5000\n                \"\"\"\n                \n                result = await session.execute(query)\n                products = result.fetchall()\n                \n                if not products:\n                    logger.warning(\"No products found to index\")\n                    return {\n                        \"status\": \"completed\",\n                        \"total\": 0,\n                        \"indexed\": 0,\n                        \"errors\": 0\n                    }\n                \n                # Convert to dictionaries\n                documents = []\n                for row in products:\n                    doc = dict(row._asdict())\n                    # Convert datetime objects to ISO format\n                    for key, value in doc.items():\n                        if isinstance(value, datetime):\n                            doc[key] = value.isoformat()\n                    documents.append(doc)\n                \n                # Bulk index documents\n                logger.info(f\"Indexing {len(documents)} products\")\n                result = await self.es_client.bulk_index(\n                    \"retail-products\",\n                    documents,\n                    id_field=\"stock_code\"\n                )\n                \n                return {\n                    \"status\": \"completed\",\n                    \"total\": result[\"total\"],\n                    \"indexed\": result[\"successful\"],\n                    \"errors\": result[\"errors\"],\n                    \"error_details\": result.get(\"error_details\", [])\n                }\n                \n        except Exception as e:\n            logger.error(f\"Error indexing products: {e}\")\n            return {\n                \"status\": \"error\",\n                \"error\": str(e),\n                \"total\": 0,\n                \"indexed\": 0,\n                \"errors\": 0\n            }\n    \n    async def reindex_all(self) -> Dict[str, Any]:\n        \"\"\"Reindex all data types\"\"\"\n        logger.info(\"Starting complete reindexing process\")\n        \n        results = {\n            \"started_at\": datetime.utcnow().isoformat(),\n            \"setup_indexes\": {},\n            \"transactions\": {},\n            \"customers\": {},\n            \"products\": {},\n            \"completed_at\": None\n        }\n        \n        try:\n            # Setup indexes\n            results[\"setup_indexes\"] = await self.setup_indexes()\n            \n            # Index data\n            results[\"transactions\"] = await self.index_transactions()\n            results[\"customers\"] = await self.index_customers()\n            results[\"products\"] = await self.index_products()\n            \n            results[\"completed_at\"] = datetime.utcnow().isoformat()\n            logger.info(\"Reindexing completed successfully\")\n            \n        except Exception as e:\n            logger.error(f\"Error during reindexing: {e}\")\n            results[\"error\"] = str(e)\n            results[\"completed_at\"] = datetime.utcnow().isoformat()\n        \n        return results\n\n\n# Global service instance\n_indexing_service = None\n\n\ndef get_indexing_service() -> ElasticsearchIndexingService:\n    \"\"\"Get global indexing service instance\"\"\"\n    global _indexing_service\n    if _indexing_service is None:\n        _indexing_service = ElasticsearchIndexingService()\n    return _indexing_service