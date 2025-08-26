"""
PwC Retail Data Platform - Search Module
Advanced search and analytics with Elasticsearch
"""

from .elasticsearch_client import (
    ElasticsearchClient,
    SearchQuery,
    SearchResult,
    get_elasticsearch_client,
    test_elasticsearch_connection,
)
from .indexing_service import ElasticsearchIndexingService, get_indexing_service

__all__ = [
    "ElasticsearchClient",
    "SearchQuery",
    "SearchResult",
    "get_elasticsearch_client",
    "test_elasticsearch_connection",
    "ElasticsearchIndexingService",
    "get_indexing_service"
]
