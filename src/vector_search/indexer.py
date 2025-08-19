from __future__ import annotations

from typing import Any

from data_access.repositories.sales_repository import SalesRepository
from vector_search.typesense_client import get_typesense_client

COLLECTION = "sales"


def ensure_collection() -> None:
    client = get_typesense_client()
    schema = {
        "name": COLLECTION,
        "fields": [
            {"name": "invoice_no", "type": "string"},
            {"name": "stock_code", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "customer_id", "type": "string", "optional": True},
            {"name": "country", "type": "string"},
            {"name": "invoice_date", "type": "string"},
            {"name": "total", "type": "float"},
        ],
        "default_sorting_field": "total",
    }
    try:
        client.collections[COLLECTION].retrieve()
    except Exception:
        # Create if missing
        client.collections.create(schema)


def index_sales(page_size: int = 1000, max_pages: int = 10) -> dict[str, Any]:
    """Index paginated sales into Typesense.
    Returns basic stats.
    """
    ensure_collection()
    client = get_typesense_client()
    repo = SalesRepository()

    total_indexed = 0
    total = 0
    for page in range(1, max_pages + 1):
        items, total = repo.query_sales(page=page, size=page_size)
        if not items:
            break
        docs: list[dict[str, Any]] = []
        for it in items:
            docs.append(
                {
                    "id": f"{it.invoice_no}-{it.stock_code}-{it.invoice_date}",
                    "invoice_no": it.invoice_no,
                    "stock_code": it.stock_code,
                    "description": it.description or "",
                    "customer_id": it.customer_id or "",
                    "country": it.country,
                    "invoice_date": it.invoice_date,
                    "total": float(it.total),
                }
            )
        client.collections[COLLECTION].documents.import_(
            docs, {
                "action": "upsert"
            }
        )
        total_indexed += len(docs)
        if page * page_size >= total:
            break

    return {"indexed": total_indexed, "total": total}
