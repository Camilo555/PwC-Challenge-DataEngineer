"""
Sales Domain Service
Business logic for sales operations following domain-driven design principles.
"""

from typing import List, Optional, Tuple
from domain.entities.sale import Sale
from domain.interfaces.sales_repository import ISalesRepository
from domain.interfaces.sales_service import ISalesService
from domain.value_objects.search_criteria import SalesSearchCriteria
from data_access.patterns.unit_of_work import IUnitOfWork
import logging

logger = logging.getLogger(__name__)


class SalesService(ISalesService):
    """
    Domain service for sales operations.
    Orchestrates business logic and repository operations.
    """
    
    def __init__(
        self,
        sales_repository: ISalesRepository,
        unit_of_work: IUnitOfWork
    ):
        self._sales_repository = sales_repository
        self._unit_of_work = unit_of_work
        
    async def get_sales(self, criteria: SalesSearchCriteria) -> Tuple[List[Sale], int]:
        """
        Get sales with business rules applied.
        
        Business Rules:
        - Apply default sorting if none provided
        - Validate date ranges
        - Apply security filters if needed
        """
        # Validate business rules
        if criteria.date_from and criteria.date_to:
            if criteria.date_from > criteria.date_to:
                raise ValueError("Start date must be before end date")
        
        # Apply default sort if none provided
        if not criteria.sort:
            criteria.sort = "invoice_date:desc"
        
        # Get data from repository
        sales, total_count = self._sales_repository.get_sales(
            date_from=criteria.date_from,
            date_to=criteria.date_to,
            product=criteria.product,
            country=criteria.country,
            page=criteria.page,
            size=criteria.size,
            sort=criteria.sort
        )
        
        # Apply business logic to results
        for sale in sales:
            # Example: Calculate derived values, apply business rules
            sale.apply_business_rules()
        
        logger.info(f"Retrieved {len(sales)} sales records (total: {total_count})")
        return sales, total_count
    
    async def get_sale_by_id(self, sale_id: str) -> Optional[Sale]:
        """Get single sale with business rules validation."""
        if not sale_id:
            raise ValueError("Sale ID cannot be empty")
        
        sale = self._sales_repository.get_sale_by_id(sale_id)
        
        if sale:
            sale.apply_business_rules()
            logger.info(f"Retrieved sale: {sale_id}")
        else:
            logger.warning(f"Sale not found: {sale_id}")
        
        return sale
    
    async def create_sale(self, sale: Sale) -> Sale:
        """Create new sale with validation."""
        async with self._unit_of_work:
            # Validate business rules
            if not sale.is_valid():
                raise ValueError("Sale data is invalid")
            
            # Apply business logic
            sale.apply_business_rules()
            sale.finalize_sale()
            
            # Persist through repository
            created_sale = self._sales_repository.create_sale(sale)
            
            # Commit transaction
            await self._unit_of_work.commit()
            
            logger.info(f"Created sale: {created_sale.sale_id}")
            return created_sale
    
    async def process_bulk_sales(self, sales: List[Sale]) -> List[Sale]:
        """Process multiple sales with business rules."""
        if not sales:
            return []
        
        async with self._unit_of_work:
            processed_sales = []
            
            for sale in sales:
                try:
                    # Validate and apply business rules
                    if sale.is_valid():
                        sale.apply_business_rules()
                        processed_sale = self._sales_repository.create_sale(sale)
                        processed_sales.append(processed_sale)
                    else:
                        logger.warning(f"Invalid sale skipped: {sale.sale_id}")
                except Exception as e:
                    logger.error(f"Error processing sale {sale.sale_id}: {e}")
                    raise
            
            # Commit all changes
            await self._unit_of_work.commit()
            
            logger.info(f"Processed {len(processed_sales)} sales successfully")
            return processed_sales