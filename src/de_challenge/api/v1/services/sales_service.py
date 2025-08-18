
from de_challenge.api.v1.schemas.sales import SaleItem
from de_challenge.data_access.repositories.sales_repository import SalesRepository


class SalesService:
    """Stub service. Later will read from the gold warehouse (SQLite/Postgres)."""

    def query_sales(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        product: str | None = None,
        country: str | None = None,
        page: int = 1,
        size: int = 20,
        sort: str = "invoice_date:desc",
    ) -> tuple[list[SaleItem], int]:
        repo = SalesRepository()
        return repo.query_sales(
            date_from=date_from,
            date_to=date_to,
            product=product,
            country=country,
            page=page,
            size=size,
            sort=sort,
        )
