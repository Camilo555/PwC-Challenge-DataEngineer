
"""
API Sales Service
Application layer service that coordinates between API and domain layers.
"""


from api.v1.schemas.sales import SaleItem
from domain.interfaces.sales_service import ISalesService
from domain.mappers.model_mapper import ModelMapper
from domain.value_objects.search_criteria import SalesSearchCriteria


class SalesService:
    """
    Application service for sales operations.
    Orchestrates between API layer and domain layer.
    """

    def __init__(self, domain_sales_service: ISalesService, model_mapper: ModelMapper):
        self._domain_sales_service = domain_sales_service
        self._model_mapper = model_mapper

    async def query_sales(
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
        """
        Query sales through domain service and map to API DTOs.
        This maintains clean architecture by going through the domain layer.
        """
        # Create domain value object
        criteria = SalesSearchCriteria(
            date_from=date_from,
            date_to=date_to,
            product=product,
            country=country,
            page=page,
            size=size,
            sort=sort
        )

        # Get data from domain service
        sales, total_count = await self._domain_sales_service.get_sales(criteria)

        # Map domain entities to API DTOs
        sale_items = [
            self._model_mapper.domain_to_dto(sale, SaleItem)
            for sale in sales
        ]

        return sale_items, total_count
