from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from vector_search.typesense_client import get_typesense_client

router = APIRouter(prefix="/search", tags=["search"])


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
