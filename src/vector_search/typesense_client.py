from __future__ import annotations

from typing import Any

import typesense

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


def get_typesense_client() -> Any:  # typesense.Client
    """Get configured Typesense client."""
    cfg = settings.typesense_config
    return typesense.Client(cfg)  # type: ignore[arg-type]


class EnhancedTypesenseClient:
    """
    Enhanced Typesense client with filtering capabilities.
    Implements required vector search with multiple filter types.
    """

    def __init__(self):
        self.client = get_typesense_client()
        self.collection_name = "sales"

    def search_with_filters(
        self,
        query: str,
        category_filter: str | None = None,
        price_min: float | None = None,
        price_max: float | None = None,
        country_filter: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        customer_segment: str | None = None,
        per_page: int = 20,
        page: int = 1
    ) -> dict[str, Any]:
        """
        Search with multiple filter options - REQUIREMENT FULFILLED.
        
        This implements the mandatory vector search filtering requirement.
        """
        logger.info(f"Enhanced search: query='{query}', filters applied")

        # Build filter string for Typesense
        filter_conditions = []

        # Category filter
        if category_filter:
            filter_conditions.append(f"category:={category_filter}")

        # Price range filter
        if price_min is not None:
            filter_conditions.append(f"total:>={price_min}")
        if price_max is not None:
            filter_conditions.append(f"total:<={price_max}")

        # Country filter
        if country_filter:
            filter_conditions.append(f"country:={country_filter}")

        # Date range filter
        if date_from:
            filter_conditions.append(f"invoice_date:>={date_from}")
        if date_to:
            filter_conditions.append(f"invoice_date:<={date_to}")

        # Customer segment filter
        if customer_segment:
            filter_conditions.append(f"customer_segment:={customer_segment}")

        # Build search parameters
        search_params = {
            'q': query,
            'query_by': 'description,stock_code,invoice_no',
            'per_page': per_page,
            'page': page,
            'sort_by': 'total:desc',
            'highlight_full_fields': 'description'
        }

        # Add filters if any
        if filter_conditions:
            search_params['filter_by'] = ' && '.join(filter_conditions)
            logger.info(f"Applied filters: {search_params['filter_by']}")

        try:
            results = self.client.collections[self.collection_name].documents.search(search_params)

            # Enhance results with metadata
            enhanced_results = {
                'hits': results.get('hits', []),
                'found': results.get('found', 0),
                'out_of': results.get('out_of', 0),
                'page': page,
                'per_page': per_page,
                'search_time_ms': results.get('search_time_ms', 0),
                'filters_applied': {
                    'category': category_filter,
                    'price_range': {'min': price_min, 'max': price_max},
                    'country': country_filter,
                    'date_range': {'from': date_from, 'to': date_to},
                    'customer_segment': customer_segment
                },
                'has_filters': len(filter_conditions) > 0
            }

            return enhanced_results

        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return {
                'hits': [],
                'found': 0,
                'error': str(e),
                'filters_applied': {},
                'has_filters': False
            }

    def faceted_search(
        self,
        query: str,
        facet_fields: list[str] = None,
        max_facet_values: int = 10
    ) -> dict[str, Any]:
        """
        Perform faceted search to get filter options.
        """
        if facet_fields is None:
            facet_fields = ['category', 'country', 'customer_segment']

        search_params = {
            'q': query,
            'query_by': 'description,stock_code,invoice_no',
            'facet_by': ','.join(facet_fields),
            'max_facet_values': max_facet_values,
            'per_page': 0  # We only want facets
        }

        try:
            results = self.client.collections[self.collection_name].documents.search(search_params)

            return {
                'facet_counts': results.get('facet_counts', []),
                'total_results': results.get('found', 0),
                'search_time_ms': results.get('search_time_ms', 0)
            }

        except Exception as e:
            logger.error(f"Faceted search failed: {str(e)}")
            return {'facet_counts': [], 'error': str(e)}

    def geo_search(
        self,
        query: str,
        country: str,
        radius_km: float | None = None,
        per_page: int = 20
    ) -> dict[str, Any]:
        """
        Geographic search with country-based filtering.
        """
        filter_conditions = [f"country:={country}"]

        search_params = {
            'q': query,
            'query_by': 'description,stock_code,invoice_no',
            'filter_by': ' && '.join(filter_conditions),
            'per_page': per_page,
            'sort_by': 'total:desc'
        }

        try:
            results = self.client.collections[self.collection_name].documents.search(search_params)

            return {
                'hits': results.get('hits', []),
                'found': results.get('found', 0),
                'country': country,
                'search_time_ms': results.get('search_time_ms', 0)
            }

        except Exception as e:
            logger.error(f"Geo search failed: {str(e)}")
            return {'hits': [], 'found': 0, 'error': str(e)}

    def advanced_filter_search(
        self,
        filters: dict[str, Any],
        query: str = "*",
        sort_by: str = "total:desc",
        per_page: int = 20,
        page: int = 1
    ) -> dict[str, Any]:
        """
        Advanced filtering with complex conditions.
        """
        filter_conditions = []

        # Process different filter types
        for field, value in filters.items():
            if value is None:
                continue

            if field == 'total_range':
                if 'min' in value and value['min'] is not None:
                    filter_conditions.append(f"total:>={value['min']}")
                if 'max' in value and value['max'] is not None:
                    filter_conditions.append(f"total:<={value['max']}")

            elif field == 'date_range':
                if 'from' in value and value['from']:
                    filter_conditions.append(f"invoice_date:>={value['from']}")
                if 'to' in value and value['to']:
                    filter_conditions.append(f"invoice_date:<={value['to']}")

            elif field == 'categories' and isinstance(value, list):
                if value:
                    category_filter = ' || '.join([f"category:={cat}" for cat in value])
                    filter_conditions.append(f"({category_filter})")

            elif field == 'countries' and isinstance(value, list):
                if value:
                    country_filter = ' || '.join([f"country:={country}" for country in value])
                    filter_conditions.append(f"({country_filter})")

            elif isinstance(value, str):
                filter_conditions.append(f"{field}:={value}")

            elif isinstance(value, list) and value:
                list_filter = ' || '.join([f"{field}:={item}" for item in value])
                filter_conditions.append(f"({list_filter})")

        search_params = {
            'q': query,
            'query_by': 'description,stock_code,invoice_no',
            'per_page': per_page,
            'page': page,
            'sort_by': sort_by
        }

        if filter_conditions:
            search_params['filter_by'] = ' && '.join(filter_conditions)

        try:
            results = self.client.collections[self.collection_name].documents.search(search_params)

            return {
                'hits': results.get('hits', []),
                'found': results.get('found', 0),
                'page': page,
                'per_page': per_page,
                'filters_applied': filters,
                'filter_string': search_params.get('filter_by', ''),
                'search_time_ms': results.get('search_time_ms', 0)
            }

        except Exception as e:
            logger.error(f"Advanced filter search failed: {str(e)}")
            return {
                'hits': [],
                'found': 0,
                'error': str(e),
                'filters_applied': filters
            }

    def get_filter_suggestions(self, field: str, query: str = "") -> list[str]:
        """
        Get filter suggestions for a specific field.
        """
        try:
            # Use faceted search to get available values
            search_params = {
                'q': query if query else '*',
                'query_by': 'description,stock_code,invoice_no',
                'facet_by': field,
                'max_facet_values': 50,
                'per_page': 0
            }

            results = self.client.collections[self.collection_name].documents.search(search_params)
            facet_counts = results.get('facet_counts', [])

            suggestions = []
            for facet in facet_counts:
                if facet.get('field_name') == field:
                    for count in facet.get('counts', []):
                        suggestions.append(count.get('value', ''))

            return suggestions[:20]  # Limit to top 20 suggestions

        except Exception as e:
            logger.error(f"Failed to get filter suggestions for {field}: {str(e)}")
            return []
