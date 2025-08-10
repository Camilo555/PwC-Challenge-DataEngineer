from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Query

from de_challenge.vector_search.typesense_client import get_typesense_client

router = APIRouter(prefix="/search", tags=["search"]) 


@router.get("/typesense")
def search_typesense(q: str = Query(..., min_length=1), limit: int = 10) -> Dict[str, Any]:
    """Minimal Typesense search endpoint.
    Expects a collection named 'sales' to exist and be indexed by the indexer.
    """
    client = get_typesense_client()
    try:
        res = client.collections["sales"].documents.search({  # type: ignore[index]
            "q": q,
            "query_by": "description,country,invoice_no,stock_code",
            "per_page": limit,
        })
        return {"hits": res.get("hits", []), "found": res.get("found", 0)}
    except Exception as exc:  # pragma: no cover
        return {"hits": [], "found": 0, "error": str(exc)}
