"""
Sales Repository Interface
Domain layer interface for sales data access operations.
"""

from abc import ABC, abstractmethod

from domain.entities.sale import Sale


class ISalesRepository(ABC):
    """Interface for sales repository operations."""

    @abstractmethod
    def get_sales(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        product: str | None = None,
        country: str | None = None,
        page: int = 1,
        size: int = 20,
        sort: str = "invoice_date:desc"
    ) -> tuple[list[Sale], int]:
        """
        Get sales with filtering and pagination.
        
        Args:
            date_from: Start date filter
            date_to: End date filter
            product: Product filter
            country: Country filter
            page: Page number (1-based)
            size: Page size
            sort: Sort specification
            
        Returns:
            Tuple of (sales_list, total_count)
        """
        pass

    @abstractmethod
    def get_sale_by_id(self, sale_id: str) -> Sale | None:
        """Get single sale by ID."""
        pass

    @abstractmethod
    def create_sale(self, sale: Sale) -> Sale:
        """Create new sale."""
        pass

    @abstractmethod
    def update_sale(self, sale: Sale) -> Sale:
        """Update existing sale."""
        pass

    @abstractmethod
    def delete_sale(self, sale_id: str) -> bool:
        """Delete sale by ID."""
        pass
