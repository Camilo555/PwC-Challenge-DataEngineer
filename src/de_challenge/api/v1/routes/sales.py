from fastapi import APIRouter, Query
from typing import List, Optional

from de_challenge.api.v1.schemas.sales import SaleItem, PaginatedSales
from de_challenge.api.v1.services.sales_service import SalesService

router = APIRouter(prefix="/sales", tags=["sales"])
service = SalesService()


@router.get("", response_model=PaginatedSales)
async def list_sales(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    sort: str = Query("invoice_date:desc"),
) -> PaginatedSales:
    items: List[SaleItem]
    total: int
    items, total = service.query_sales(
        date_from=date_from,
        date_to=date_to,
        product=product,
        country=country,
        page=page,
        size=size,
        sort=sort,
    )
    return PaginatedSales(items=items, total=total, page=page, size=size)
