"""
PwC Retail Data Platform - Elasticsearch Client
Advanced search and analytics capabilities for retail data
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from elasticsearch import AsyncElasticsearch, Elasticsearch
from elasticsearch.exceptions import ConnectionError, NotFoundError
from pydantic import BaseModel

from ..config.base_config import get_config
from ..logging import get_logger

logger = get_logger(__name__)
config = get_config()


class SearchQuery(BaseModel):
    """Search query model for Elasticsearch"""
    query: str
    size: int = 10
    from_: int = 0
    filters: Optional[Dict[str, Any]] = None
    sort: Optional[List[Dict[str, str]]] = None
    highlight: bool = True
    aggregations: Optional[Dict[str, Any]] = None


class SearchResult(BaseModel):
    """Search result model"""
    total: int
    hits: List[Dict[str, Any]]
    aggregations: Optional[Dict[str, Any]] = None
    took: int
    max_score: Optional[float] = None


class ElasticsearchClient:
    """Elasticsearch client for search and analytics operations"""
    
    def __init__(self):
        self.host = config.ELASTICSEARCH_HOST
        self.port = config.ELASTICSEARCH_PORT
        self.scheme = getattr(config, 'ELASTICSEARCH_SCHEME', 'http')
        self.username = getattr(config, 'ELASTICSEARCH_USERNAME', None)
        self.password = getattr(config, 'ELASTICSEARCH_PASSWORD', None)
        
        # Connection configuration
        self.connection_config = {
            'hosts': [f"{self.scheme}://{self.host}:{self.port}"],
            'timeout': 30,
            'max_retries': 3,
            'retry_on_timeout': True
        }
        
        if self.username and self.password:
            self.connection_config['http_auth'] = (self.username, self.password)
        
        self._client = None
        self._async_client = None
        
    @property
    def client(self) -> Elasticsearch:
        """Get synchronous Elasticsearch client"""
        if self._client is None:
            self._client = Elasticsearch(**self.connection_config)
        return self._client
    
    @property
    def async_client(self) -> AsyncElasticsearch:
        """Get asynchronous Elasticsearch client"""
        if self._async_client is None:
            self._async_client = AsyncElasticsearch(**self.connection_config)
        return self._async_client
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Elasticsearch cluster health"""
        try:
            health = await self.async_client.cluster.health()
            return {
                "status": "healthy",
                "cluster_status": health.get('status'),
                "number_of_nodes": health.get('number_of_nodes'),
                "active_shards": health.get('active_shards'),
                "timestamp": datetime.utcnow().isoformat()
            }
        except ConnectionError as e:
            logger.error(f"Elasticsearch connection error: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Elasticsearch health check error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def create_index(self, index_name: str, mapping: Dict[str, Any]) -> bool:
        """Create an index with mapping"""
        try:
            if await self.async_client.indices.exists(index=index_name):
                logger.info(f"Index {index_name} already exists")
                return True
            
            await self.async_client.indices.create(
                index=index_name,
                body=mapping
            )
            logger.info(f"Created index: {index_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating index {index_name}: {e}")
            return False
    
    async def delete_index(self, index_name: str) -> bool:
        """Delete an index"""
        try:
            await self.async_client.indices.delete(index=index_name)
            logger.info(f"Deleted index: {index_name}")
            return True
        except NotFoundError:
            logger.warning(f"Index {index_name} not found")
            return False
        except Exception as e:
            logger.error(f"Error deleting index {index_name}: {e}")
            return False
    
    async def index_document(
        self, 
        index_name: str, 
        document: Dict[str, Any], 
        doc_id: Optional[str] = None
    ) -> bool:
        """Index a single document"""
        try:
            kwargs = {'index': index_name, 'body': document}
            if doc_id:
                kwargs['id'] = doc_id
            
            response = await self.async_client.index(**kwargs)
            return response.get('result') in ['created', 'updated']
            
        except Exception as e:
            logger.error(f"Error indexing document: {e}")
            return False
    
    async def bulk_index(
        self, 
        index_name: str, 
        documents: List[Dict[str, Any]], 
        id_field: Optional[str] = None
    ) -> Dict[str, Any]:
        """Bulk index multiple documents"""
        try:
            actions = []
            for doc in documents:
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]
                actions.append(action)
            
            response = await self.async_client.bulk(body=actions)
            
            # Count successful operations
            successful = 0
            errors = []
            
            for item in response.get('items', []):
                if 'index' in item:
                    if item['index'].get('status') in [200, 201]:
                        successful += 1
                    else:
                        errors.append(item['index'])
            
            return {
                "total": len(documents),
                "successful": successful,
                "errors": len(errors),
                "error_details": errors[:10]  # First 10 errors
            }
            
        except Exception as e:
            logger.error(f"Error in bulk indexing: {e}")
            return {
                "total": len(documents),
                "successful": 0,
                "errors": len(documents),
                "error": str(e)
            }
    
    async def search(
        self, 
        index_name: str, 
        search_query: SearchQuery
    ) -> SearchResult:
        """Perform search with complex queries"""
        try:
            # Build Elasticsearch query
            query_body = {
                "query": self._build_query(search_query.query, search_query.filters),
                "size": search_query.size,
                "from": search_query.from_
            }
            
            # Add sorting
            if search_query.sort:
                query_body["sort"] = search_query.sort
            
            # Add highlighting
            if search_query.highlight:
                query_body["highlight"] = {
                    "fields": {
                        "*": {}
                    },
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"]
                }
            
            # Add aggregations
            if search_query.aggregations:
                query_body["aggs"] = search_query.aggregations
            
            response = await self.async_client.search(
                index=index_name,
                body=query_body
            )
            
            # Format response
            hits = []
            for hit in response.get('hits', {}).get('hits', []):
                hit_data = {
                    "id": hit.get('_id'),
                    "score": hit.get('_score'),
                    "source": hit.get('_source', {}),
                    "highlight": hit.get('highlight', {})
                }
                hits.append(hit_data)
            
            return SearchResult(
                total=response.get('hits', {}).get('total', {}).get('value', 0),
                hits=hits,
                aggregations=response.get('aggregations'),
                took=response.get('took', 0),
                max_score=response.get('hits', {}).get('max_score')
            )
            
        except Exception as e:
            logger.error(f"Error in search: {e}")
            return SearchResult(
                total=0,
                hits=[],
                took=0,
                aggregations={"error": str(e)}
            )
    
    def _build_query(self, query_string: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Build Elasticsearch query with filters"""
        if not query_string and not filters:
            return {"match_all": {}}
        
        must_clauses = []
        
        # Add text query if provided
        if query_string:
            must_clauses.append({
                "multi_match": {
                    "query": query_string,
                    "fields": [
                        "product_name^3",
                        "description^2", 
                        "category^2",
                        "brand",
                        "customer_name",
                        "*"
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            })
        
        # Add filters
        if filters:
            for field, value in filters.items():
                if isinstance(value, list):
                    must_clauses.append({
                        "terms": {field: value}
                    })
                elif isinstance(value, dict) and "range" in value:
                    must_clauses.append({
                        "range": {field: value["range"]}
                    })
                else:
                    must_clauses.append({
                        "term": {f"{field}.keyword": value}
                    })
        
        if not must_clauses:
            return {"match_all": {}}
        
        return {"bool": {"must": must_clauses}}
    
    async def get_analytics(self, index_name: str, date_range: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Get analytics and aggregations for the data"""
        try:
            # Build date filter if provided
            date_filter = []
            if date_range:
                date_filter.append({
                    "range": {
                        "order_date": {
                            "gte": date_range.get("start"),
                            "lte": date_range.get("end")
                        }
                    }
                })
            
            query_body = {
                "query": {
                    "bool": {
                        "must": date_filter if date_filter else [{"match_all": {}}]
                    }
                },
                "size": 0,
                "aggs": {
                    "total_revenue": {
                        "sum": {"field": "quantity_price"}
                    },
                    "total_orders": {
                        "cardinality": {"field": "invoice_no.keyword"}
                    },
                    "avg_order_value": {
                        "avg": {"field": "quantity_price"}
                    },
                    "top_products": {
                        "terms": {
                            "field": "product_name.keyword",
                            "size": 10
                        },
                        "aggs": {
                            "revenue": {
                                "sum": {"field": "quantity_price"}
                            }
                        }
                    },
                    "top_categories": {
                        "terms": {
                            "field": "category.keyword",
                            "size": 10
                        }
                    },
                    "revenue_by_country": {
                        "terms": {
                            "field": "country.keyword",
                            "size": 20
                        },
                        "aggs": {
                            "revenue": {
                                "sum": {"field": "quantity_price"}
                            }
                        }
                    },
                    "sales_over_time": {
                        "date_histogram": {
                            "field": "order_date",
                            "calendar_interval": "1d",
                            "min_doc_count": 0
                        },
                        "aggs": {
                            "daily_revenue": {
                                "sum": {"field": "quantity_price"}
                            }
                        }
                    }
                }
            }
            
            response = await self.async_client.search(
                index=index_name,
                body=query_body
            )
            
            return response.get('aggregations', {})
            
        except Exception as e:
            logger.error(f"Error getting analytics: {e}")
            return {"error": str(e)}
    
    async def close(self):
        """Close connections"""
        if self._async_client:
            await self._async_client.close()
        if self._client:
            self._client.close()


# Global client instance
_elasticsearch_client = None


def get_elasticsearch_client() -> ElasticsearchClient:
    """Get global Elasticsearch client instance"""
    global _elasticsearch_client
    if _elasticsearch_client is None:
        _elasticsearch_client = ElasticsearchClient()
    return _elasticsearch_client


async def test_elasticsearch_connection():
    """Test Elasticsearch connection"""
    client = get_elasticsearch_client()
    health = await client.health_check()
    logger.info(f"Elasticsearch health: {health}")
    return health