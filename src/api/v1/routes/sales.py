
from fastapi import APIRouter, Depends, Query

from api.v1.schemas.sales import PaginatedSales, SaleItem
from api.v1.services.sales_service import SalesService
from core.dependency_injection import get_service
from domain.interfaces.sales_service import ISalesService
from domain.mappers.model_mapper import ModelMapper

router = APIRouter(prefix="/sales", tags=["sales"])


def get_sales_service() -> SalesService:
    """Dependency injection for sales service."""
    domain_service = get_service(ISalesService)
    model_mapper = get_service(ModelMapper)
    return SalesService(domain_service, model_mapper)


@router.get("", response_model=PaginatedSales)
async def list_sales(
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    product: str | None = Query(None),
    country: str | None = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    sort: str = Query("invoice_date:desc"),
    service: SalesService = Depends(get_sales_service)
) -> PaginatedSales:
    """Get paginated sales data with proper dependency injection."""
    items: list[SaleItem]
    total: int
    items, total = await service.query_sales(
        date_from=date_from,
        date_to=date_to,
        product=product,
        country=country,
        page=page,
        size=size,
        sort=sort,
    )
    return PaginatedSales(items=items, total=total, page=page, size=size)
