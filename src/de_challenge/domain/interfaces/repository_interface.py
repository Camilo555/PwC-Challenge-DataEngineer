"""Repository interfaces for domain layer."""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Generic, TypeVar
from datetime import datetime

T = TypeVar("T")


class IRepository(ABC, Generic[T]):
    """Base repository interface."""

    @abstractmethod
    async def create(self, entity: T) -> T:
        """Create a new entity."""
        pass

    @abstractmethod
    async def get(self, id: Any) -> Optional[T]:
        """Get entity by ID."""
        pass

    @abstractmethod
    async def update(self, id: Any, entity: T) -> Optional[T]:
        """Update existing entity."""
        pass

    @abstractmethod
    async def delete(self, id: Any) -> bool:
        """Delete entity by ID."""
        pass

    @abstractmethod
    async def list(
        self,
        skip: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[T]:
        """List entities with pagination and filters."""
        pass

    @abstractmethod
    async def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Count entities with optional filters."""
        pass

    @abstractmethod
    async def exists(self, id: Any) -> bool:
        """Check if entity exists."""
        pass

    @abstractmethod
    async def bulk_create(self, entities: List[T]) -> List[T]:
        """Create multiple entities."""
        pass

    @abstractmethod
    async def bulk_update(self, entities: List[T]) -> List[T]:
        """Update multiple entities."""
        pass

    @abstractmethod
    async def bulk_delete(self, ids: List[Any]) -> int:
        """Delete multiple entities."""
        pass


class ITransactionRepository(IRepository):
    """Transaction-specific repository interface."""

    @abstractmethod
    async def get_by_invoice(self, invoice_no: str) -> Optional[Any]:
        """Get transaction by invoice number."""
        pass

    @abstractmethod
    async def get_by_customer(
        self,
        customer_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Any]:
        """Get transactions by customer."""
        pass

    @abstractmethod
    async def get_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Any]:
        """Get transactions within date range."""
        pass

    @abstractmethod
    async def get_cancelled(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Any]:
        """Get cancelled transactions."""
        pass


class IProductRepository(IRepository):
    """Product-specific repository interface."""

    @abstractmethod
    async def get_by_stock_code(self, stock_code: str) -> Optional[Any]:
        """Get product by stock code."""
        pass

    @abstractmethod
    async def get_by_category(self, category: str) -> List[Any]:
        """Get products by category."""
        pass

    @abstractmethod
    async def get_active(self) -> List[Any]:
        """Get active products."""
        pass

    @abstractmethod
    async def search(
        self,
        query: str,
        limit: int = 100
    ) -> List[Any]:
        """Search products by text."""
        pass

    @abstractmethod
    async def get_top_selling(
        self,
        limit: int = 10,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Any]:
        """Get top selling products."""
        pass


class ICustomerRepository(IRepository):
    """Customer-specific repository interface."""

    @abstractmethod
    async def get_by_country(self, country: str) -> List[Any]:
        """Get customers by country."""
        pass

    @abstractmethod
    async def get_by_segment(self, segment: str) -> List[Any]:
        """Get customers by segment."""
        pass

    @abstractmethod
    async def get_vip(self) -> List[Any]:
        """Get VIP customers."""
        pass

    @abstractmethod
    async def get_inactive(self, days: int = 365) -> List[Any]:
        """Get inactive customers."""
        pass

    @abstractmethod
    async def update_metrics(
        self,
        customer_id: str,
        metrics: Dict[str, Any]
    ) -> bool:
        """Update customer metrics."""
        pass


class IInvoiceRepository(IRepository):
    """Invoice-specific repository interface."""

    @abstractmethod
    async def get_by_status(self, status: str) -> List[Any]:
        """Get invoices by status."""
        pass

    @abstractmethod
    async def get_by_payment_method(self, method: str) -> List[Any]:
        """Get invoices by payment method."""
        pass

    @abstractmethod
    async def get_pending(self) -> List[Any]:
        """Get pending invoices."""
        pass

    @abstractmethod
    async def mark_as_paid(self, invoice_no: str) -> bool:
        """Mark invoice as paid."""
        pass

    @abstractmethod
    async def cancel(
        self,
        invoice_no: str,
        reason: Optional[str] = None
    ) -> bool:
        """Cancel an invoice."""
        pass
