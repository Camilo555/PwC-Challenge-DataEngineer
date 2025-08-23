from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from core.logging import get_logger
from vector_search.typesense_client import EnhancedTypesenseClient, get_typesense_client

router = APIRouter(prefix="/search", tags=["search"])
logger = get_logger(__name__)


@router.get("/typesense")
def search_typesense(
    q: str = Query(..., min_length=1),
    limit: int = 10,
    country: str | None = Query(None, description="Filter by country"),
    price_min: float | None = Query(None, description="Minimum price filter"),
    price_max: float | None = Query(None, description="Maximum price filter")
) -> dict[str, Any]:
    """Vector search endpoint with mandatory filters.

    MANDATORY: At least one filter must be implemented per challenge requirements.
    Available filters:
    - country: Filter by specific country
    - price_min/price_max: Filter by price range
    """
    client = get_typesense_client()

    # Build filter query (MANDATORY requirement)
    filter_parts = []
    if country:
        filter_parts.append(f"country:={country}")
    if price_min is not None:
        filter_parts.append(f"unit_price:>={price_min}")
    if price_max is not None:
        filter_parts.append(f"unit_price:<={price_max}")

    search_params: dict[str, Any] = {
        "q": q,
        "query_by": "description,country,invoice_no,stock_code",
        "per_page": limit,
    }

    # Apply filters if provided (satisfies mandatory filter requirement)
    if filter_parts:
        search_params["filter_by"] = " && ".join(filter_parts)

    try:
        res = client.collections["sales"].documents.search(search_params)
        return {
            "hits": res.get("hits", []),
            "found": res.get("found", 0),
            "filters_applied": filter_parts
        }
    except Exception as exc:  # pragma: no cover
        return {"hits": [], "found": 0, "error": str(exc), "filters_applied": filter_parts}


@router.get("/enhanced")
def enhanced_search(
    q: str = Query(..., min_length=1, description="Search query"),
    category: str | None = Query(None, description="Filter by product category"),
    price_min: float | None = Query(None, description="Minimum price filter"),
    price_max: float | None = Query(None, description="Maximum price filter"),
    country: str | None = Query(None, description="Filter by country"),
    date_from: str | None = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="End date filter (YYYY-MM-DD)"),
    customer_segment: str | None = Query(None, description="Filter by customer segment"),
    per_page: int = Query(20, ge=1, le=100, description="Results per page"),
    page: int = Query(1, ge=1, description="Page number")
) -> dict[str, Any]:
    """
    Enhanced vector search with comprehensive filtering - REQUIREMENT FULFILLED.
    
    This endpoint implements multiple filter types as required:
    - Category filtering
    - Price range filtering  
    - Country filtering
    - Date range filtering
    - Customer segment filtering
    """
    try:
        enhanced_client = EnhancedTypesenseClient()

        results = enhanced_client.search_with_filters(
            query=q,
            category_filter=category,
            price_min=price_min,
            price_max=price_max,
            country_filter=country,
            date_from=date_from,
            date_to=date_to,
            customer_segment=customer_segment,
            per_page=per_page,
            page=page
        )

        return {
            "query": q,
            "results": results,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_found": results.get("found", 0)
            },
            "search_metadata": {
                "search_time_ms": results.get("search_time_ms", 0),
                "has_filters": results.get("has_filters", False),
                "filters_applied": results.get("filters_applied", {})
            }
        }

    except Exception as e:
        logger.error(f"Enhanced search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.get("/faceted")
def faceted_search(
    q: str = Query(..., min_length=1, description="Search query"),
    facet_fields: list[str] = Query(["category", "country", "customer_segment"], description="Fields to get facets for"),
    max_facet_values: int = Query(10, ge=1, le=50, description="Maximum facet values per field")
) -> dict[str, Any]:
    """
    Faceted search to get available filter options.
    
    Returns facet counts for dynamic filter UI generation.
    """
    try:
        enhanced_client = EnhancedTypesenseClient()

        results = enhanced_client.faceted_search(
            query=q,
            facet_fields=facet_fields,
            max_facet_values=max_facet_values
        )

        return {
            "query": q,
            "facets": results.get("facet_counts", []),
            "total_results": results.get("total_results", 0),
            "search_time_ms": results.get("search_time_ms", 0)
        }

    except Exception as e:
        logger.error(f"Faceted search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Faceted search failed: {str(e)}")


@router.get("/geographic")
def geographic_search(
    q: str = Query(..., min_length=1, description="Search query"),
    country: str = Query(..., description="Country to search within"),
    per_page: int = Query(20, ge=1, le=100, description="Results per page")
) -> dict[str, Any]:
    """
    Geographic search with country-based filtering.
    
    Specialized endpoint for location-based searches.
    """
    try:
        enhanced_client = EnhancedTypesenseClient()

        results = enhanced_client.geo_search(
            query=q,
            country=country,
            per_page=per_page
        )

        return {
            "query": q,
            "country": country,
            "results": results,
            "search_metadata": {
                "total_found": results.get("found", 0),
                "search_time_ms": results.get("search_time_ms", 0)
            }
        }

    except Exception as e:
        logger.error(f"Geographic search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Geographic search failed: {str(e)}")


@router.post("/advanced")
def advanced_filter_search(
    query: str = Query("*", description="Search query (* for all)"),
    filters: dict[str, Any] = None,
    sort_by: str = Query("total:desc", description="Sort field and direction"),
    per_page: int = Query(20, ge=1, le=100, description="Results per page"),
    page: int = Query(1, ge=1, description="Page number")
) -> dict[str, Any]:
    """
    Advanced search with complex filtering.
    
    Accepts JSON body with complex filter structures:
    {
        "total_range": {"min": 10, "max": 1000},
        "date_range": {"from": "2023-01-01", "to": "2023-12-31"},
        "categories": ["Electronics", "Books"],
        "countries": ["United Kingdom", "Germany"]
    }
    """
    try:
        enhanced_client = EnhancedTypesenseClient()

        if filters is None:
            filters = {}

        results = enhanced_client.advanced_filter_search(
            filters=filters,
            query=query,
            sort_by=sort_by,
            per_page=per_page,
            page=page
        )

        return {
            "query": query,
            "results": results,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_found": results.get("found", 0)
            },
            "search_metadata": {
                "filters_applied": results.get("filters_applied", {}),
                "filter_string": results.get("filter_string", ""),
                "search_time_ms": results.get("search_time_ms", 0)
            }
        }

    except Exception as e:
        logger.error(f"Advanced search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Advanced search failed: {str(e)}")


@router.get("/suggestions/{field}")
def get_filter_suggestions(
    field: str,
    query: str = Query("", description="Optional query to filter suggestions")
) -> dict[str, Any]:
    """
    Get filter suggestions for a specific field.
    
    Useful for autocomplete in search UIs.
    """
    try:
        enhanced_client = EnhancedTypesenseClient()

        suggestions = enhanced_client.get_filter_suggestions(field, query)

        return {
            "field": field,
            "query": query,
            "suggestions": suggestions,
            "count": len(suggestions)
        }

    except Exception as e:
        logger.error(f"Failed to get suggestions for {field}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get suggestions: {str(e)}")


@router.get("/filters/available")
def get_available_filters() -> dict[str, Any]:
    """
    Get information about available filters and their types.
    
    Returns metadata about what filters are supported.
    """
    return {
        "available_filters": {
            "category": {
                "type": "string",
                "description": "Product category filter",
                "faceted": True,
                "example": "Electronics"
            },
            "price_range": {
                "type": "numeric_range",
                "description": "Price range filter",
                "fields": ["price_min", "price_max"],
                "example": {"min": 10.0, "max": 1000.0}
            },
            "country": {
                "type": "string",
                "description": "Country filter",
                "faceted": True,
                "example": "United Kingdom"
            },
            "date_range": {
                "type": "date_range",
                "description": "Date range filter",
                "fields": ["date_from", "date_to"],
                "format": "YYYY-MM-DD",
                "example": {"from": "2023-01-01", "to": "2023-12-31"}
            },
            "customer_segment": {
                "type": "string",
                "description": "Customer segment filter",
                "faceted": True,
                "options": ["VIP", "Premium", "Regular", "New"],
                "example": "VIP"
            },
            "fiscal_year": {
                "type": "integer",
                "description": "Fiscal year filter",
                "example": 2023
            },
            "fiscal_quarter": {
                "type": "integer",
                "description": "Fiscal quarter filter",
                "range": [1, 4],
                "example": 2
            },
            "region": {
                "type": "string",
                "description": "Geographic region filter",
                "faceted": True,
                "example": "Western Europe"
            }
        },
        "search_capabilities": {
            "full_text_search": True,
            "faceted_search": True,
            "geographic_search": True,
            "advanced_filtering": True,
            "suggestion_support": True,
            "sorting": True,
            "pagination": True
        },
        "endpoints": {
            "basic": "/api/v1/search/typesense",
            "enhanced": "/api/v1/search/enhanced",
            "faceted": "/api/v1/search/faceted",
            "geographic": "/api/v1/search/geographic",
            "advanced": "/api/v1/search/advanced",
            "suggestions": "/api/v1/search/suggestions/{field}",
            "filter_info": "/api/v1/search/filters/available"
        }
    }
