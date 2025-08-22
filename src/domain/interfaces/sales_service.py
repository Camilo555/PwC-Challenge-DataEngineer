"""
Sales Service Interface
Domain layer interface for sales business operations.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from domain.entities.sale import Sale
from domain.value_objects.search_criteria import SalesSearchCriteria


class ISalesService(ABC):
    """Interface for sales business operations."""
    
    @abstractmethod
    async def get_sales(self, criteria: SalesSearchCriteria) -> Tuple[List[Sale], int]:
        """
        Get sales with business rules applied.
        
        Args:
            criteria: Search and filtering criteria
            
        Returns:
            Tuple of (sales_list, total_count)
        """
        pass
    
    @abstractmethod
    async def get_sale_by_id(self, sale_id: str) -> Optional[Sale]:
        """Get single sale with business rules validation."""
        pass
    
    @abstractmethod
    async def create_sale(self, sale: Sale) -> Sale:
        """Create new sale with validation."""
        pass
    
    @abstractmethod
    async def process_bulk_sales(self, sales: List[Sale]) -> List[Sale]:
        """Process multiple sales with business rules."""
        pass