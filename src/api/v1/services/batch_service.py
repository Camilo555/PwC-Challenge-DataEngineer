"""
Batch Service for Bulk Operations
Handles batch CRUD operations with optimizations and error handling.
"""
import time
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4

from sqlmodel import Session, select
from pydantic import ValidationError

from domain.entities.sales_transaction import SalesTransactionCreate, SalesTransaction
from domain.validators.business_rules import BusinessRuleValidator
from core.logging import get_logger

logger = get_logger(__name__)


class BatchService:
    """
    Service for handling batch operations on sales transactions.
    Provides efficient bulk CRUD operations with proper error handling.
    """
    
    def __init__(self, session: Session):
        self.session = session
        self.validator = BusinessRuleValidator()
    
    async def batch_create_transactions(
        self,
        items: List[SalesTransactionCreate],
        validate_items: bool = True,
        fail_on_error: bool = False
    ) -> Dict[str, Any]:
        """
        Create multiple transactions in batch.
        
        Args:
            items: List of transactions to create
            validate_items: Whether to validate each item
            fail_on_error: Whether to fail entire batch on single error
            
        Returns:
            Dictionary with operation results
        """
        start_time = time.time()
        batch_id = str(uuid4())
        
        logger.info(f"Starting batch create operation {batch_id} for {len(items)} items")
        
        successful = 0
        failed = 0
        errors = []
        created_items = []
        
        try:
            for i, item in enumerate(items):
                try:
                    # Validate item if requested
                    if validate_items:
                        self._validate_transaction_item(item)
                    
                    # Convert to database model
                    db_item = SalesTransaction.from_orm(item)
                    
                    # Add to session
                    self.session.add(db_item)
                    created_items.append(db_item)
                    successful += 1
                    
                except ValidationError as e:
                    failed += 1
                    error_detail = {
                        "item_index": i,
                        "error_type": "validation_error",
                        "details": e.errors()
                    }
                    errors.append(error_detail)
                    logger.warning(f"Validation failed for item {i}: {e}")
                    
                    if fail_on_error:
                        raise ValueError(f"Validation failed for item {i}: {e}")
                
                except Exception as e:
                    failed += 1
                    error_detail = {
                        "item_index": i,
                        "error_type": "processing_error",
                        "details": str(e)
                    }
                    errors.append(error_detail)
                    logger.error(f"Failed to process item {i}: {e}")
                    
                    if fail_on_error:
                        raise
            
            # Commit all successful items
            if successful > 0:
                self.session.commit()
                logger.info(f"Batch create {batch_id}: committed {successful} items")
            
            processing_time = (time.time() - start_time) * 1000
            
            return {
                "batch_id": batch_id,
                "successful": successful,
                "failed": failed,
                "errors": errors,
                "processing_time_ms": processing_time,
                "created_items": [str(item.id) for item in created_items]
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch create {batch_id} failed: {str(e)}")
            raise
    
    async def batch_update_transactions(
        self,
        updates: List[Dict[str, Any]],
        upsert: bool = False
    ) -> Dict[str, Any]:
        """
        Update multiple transactions in batch.
        
        Args:
            updates: List of update dictionaries with 'id' and update fields
            upsert: Whether to create items if they don't exist
            
        Returns:
            Dictionary with operation results
        """
        start_time = time.time()
        batch_id = str(uuid4())
        
        logger.info(f"Starting batch update operation {batch_id} for {len(updates)} items")
        
        successful = 0
        failed = 0
        errors = []
        updated_items = []
        
        try:
            for i, update_data in enumerate(updates):
                try:
                    if 'id' not in update_data:
                        raise ValueError("Update data must include 'id' field")
                    
                    item_id = update_data['id']
                    
                    # Find existing item
                    stmt = select(SalesTransaction).where(SalesTransaction.id == item_id)
                    existing_item = self.session.exec(stmt).first()
                    
                    if not existing_item:
                        if upsert:
                            # Create new item
                            new_item = SalesTransaction(**update_data)
                            self.session.add(new_item)
                            updated_items.append(new_item)
                        else:
                            raise ValueError(f"Item with id {item_id} not found")
                    else:
                        # Update existing item
                        for field, value in update_data.items():
                            if field != 'id' and hasattr(existing_item, field):
                                setattr(existing_item, field, value)
                        
                        existing_item.updated_at = datetime.utcnow()
                        updated_items.append(existing_item)
                    
                    successful += 1
                    
                except Exception as e:
                    failed += 1
                    error_detail = {
                        "item_index": i,
                        "item_id": update_data.get('id'),
                        "error_type": "update_error",
                        "details": str(e)
                    }
                    errors.append(error_detail)
                    logger.error(f"Failed to update item {i}: {e}")
            
            # Commit all successful updates
            if successful > 0:
                self.session.commit()
                logger.info(f"Batch update {batch_id}: updated {successful} items")
            
            processing_time = (time.time() - start_time) * 1000
            
            return {
                "batch_id": batch_id,
                "successful": successful,
                "failed": failed,
                "errors": errors,
                "processing_time_ms": processing_time,
                "updated_items": [str(item.id) for item in updated_items]
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch update {batch_id} failed: {str(e)}")
            raise
    
    async def batch_delete_transactions(
        self,
        ids: List[UUID],
        soft_delete: bool = True
    ) -> Dict[str, Any]:
        """
        Delete multiple transactions in batch.
        
        Args:
            ids: List of transaction IDs to delete
            soft_delete: Whether to perform soft delete (mark as deleted)
            
        Returns:
            Dictionary with operation results
        """
        start_time = time.time()
        batch_id = str(uuid4())
        
        logger.info(f"Starting batch delete operation {batch_id} for {len(ids)} items")
        
        successful = 0
        failed = 0
        errors = []
        deleted_items = []
        
        try:
            for i, item_id in enumerate(ids):
                try:
                    # Find existing item
                    stmt = select(SalesTransaction).where(SalesTransaction.id == item_id)
                    existing_item = self.session.exec(stmt).first()
                    
                    if not existing_item:
                        raise ValueError(f"Item with id {item_id} not found")
                    
                    if soft_delete:
                        # Soft delete - mark as deleted
                        existing_item.status = "deleted"
                        existing_item.updated_at = datetime.utcnow()
                    else:
                        # Hard delete - remove from database
                        self.session.delete(existing_item)
                    
                    deleted_items.append(str(item_id))
                    successful += 1
                    
                except Exception as e:
                    failed += 1
                    error_detail = {
                        "item_index": i,
                        "item_id": str(item_id),
                        "error_type": "delete_error",
                        "details": str(e)
                    }
                    errors.append(error_detail)
                    logger.error(f"Failed to delete item {i}: {e}")
            
            # Commit all successful deletions
            if successful > 0:
                self.session.commit()
                logger.info(f"Batch delete {batch_id}: deleted {successful} items")
            
            processing_time = (time.time() - start_time) * 1000
            
            return {
                "batch_id": batch_id,
                "successful": successful,
                "failed": failed,
                "errors": errors,
                "processing_time_ms": processing_time,
                "deleted_items": deleted_items
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch delete {batch_id} failed: {str(e)}")
            raise
    
    async def batch_upsert_transactions(
        self,
        items: List[SalesTransactionCreate],
        validate_items: bool = True
    ) -> Dict[str, Any]:
        """
        Upsert (insert or update) multiple transactions in batch.
        
        Args:
            items: List of transactions to upsert
            validate_items: Whether to validate each item
            
        Returns:
            Dictionary with operation results
        """
        start_time = time.time()
        batch_id = str(uuid4())
        
        logger.info(f"Starting batch upsert operation {batch_id} for {len(items)} items")
        
        successful = 0
        failed = 0
        errors = []
        upserted_items = []
        
        try:
            for i, item in enumerate(items):
                try:
                    # Validate item if requested
                    if validate_items:
                        self._validate_transaction_item(item)
                    
                    # Check if item exists (based on invoice_no + stock_code)
                    stmt = select(SalesTransaction).where(
                        SalesTransaction.invoice_no == item.invoice_no,
                        SalesTransaction.stock_code == item.stock_code
                    )
                    existing_item = self.session.exec(stmt).first()
                    
                    if existing_item:
                        # Update existing item
                        for field, value in item.dict(exclude_unset=True).items():
                            if hasattr(existing_item, field):
                                setattr(existing_item, field, value)
                        existing_item.updated_at = datetime.utcnow()
                        upserted_items.append(existing_item)
                    else:
                        # Create new item
                        db_item = SalesTransaction.from_orm(item)
                        self.session.add(db_item)
                        upserted_items.append(db_item)
                    
                    successful += 1
                    
                except Exception as e:
                    failed += 1
                    error_detail = {
                        "item_index": i,
                        "error_type": "upsert_error",
                        "details": str(e)
                    }
                    errors.append(error_detail)
                    logger.error(f"Failed to upsert item {i}: {e}")
            
            # Commit all successful upserts
            if successful > 0:
                self.session.commit()
                logger.info(f"Batch upsert {batch_id}: processed {successful} items")
            
            processing_time = (time.time() - start_time) * 1000
            
            return {
                "batch_id": batch_id,
                "successful": successful,
                "failed": failed,
                "errors": errors,
                "processing_time_ms": processing_time,
                "upserted_items": [str(item.id) for item in upserted_items]
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch upsert {batch_id} failed: {str(e)}")
            raise
    
    async def validate_batch_data(
        self,
        items: List[SalesTransactionCreate]
    ) -> Dict[str, Any]:
        """
        Validate batch data without performing operations.
        
        Args:
            items: List of transactions to validate
            
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Validating batch data for {len(items)} items")
        
        valid_count = 0
        invalid_count = 0
        errors = []
        recommendations = []
        
        for i, item in enumerate(items):
            try:
                self._validate_transaction_item(item)
                valid_count += 1
            except ValidationError as e:
                invalid_count += 1
                errors.append({
                    "item_index": i,
                    "validation_errors": e.errors()
                })
            except Exception as e:
                invalid_count += 1
                errors.append({
                    "item_index": i,
                    "error": str(e)
                })
        
        # Generate recommendations
        if invalid_count > 0:
            error_rate = invalid_count / len(items)
            if error_rate > 0.1:  # More than 10% errors
                recommendations.append("High error rate detected. Consider reviewing data quality.")
            if error_rate > 0.5:  # More than 50% errors
                recommendations.append("Majority of items have errors. Consider data validation before batch processing.")
        
        return {
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "errors": errors,
            "is_valid": invalid_count == 0,
            "error_rate": invalid_count / len(items) if len(items) > 0 else 0,
            "recommendations": recommendations
        }
    
    async def get_batch_status(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a batch operation.
        
        Args:
            batch_id: ID of the batch operation
            
        Returns:
            Status information or None if not found
        """
        # In a real implementation, this would query a batch operations table
        # For now, return a mock response
        return {
            "batch_id": batch_id,
            "status": "completed",
            "created_at": datetime.utcnow().isoformat(),
            "completed_at": datetime.utcnow().isoformat(),
            "total_items": 0,
            "successful": 0,
            "failed": 0
        }
    
    async def get_active_operations(self) -> List[Dict[str, Any]]:
        """
        Get list of currently active batch operations.
        
        Returns:
            List of active operations
        """
        # In a real implementation, this would query active operations
        # For now, return an empty list
        return []
    
    def get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.utcnow().isoformat()
    
    # Background task methods
    
    async def optimize_post_batch_creation(self, batch_id: str) -> None:
        """Background optimization after batch creation."""
        logger.info(f"Running post-batch creation optimization for {batch_id}")
        # This could include:
        # - Index optimization
        # - Cache invalidation
        # - Data quality checks
        # - Analytics updates
        await asyncio.sleep(1)  # Simulate processing
        logger.info(f"Completed post-batch creation optimization for {batch_id}")
    
    async def optimize_post_batch_update(self, batch_id: str) -> None:
        """Background optimization after batch update."""
        logger.info(f"Running post-batch update optimization for {batch_id}")
        await asyncio.sleep(1)  # Simulate processing
        logger.info(f"Completed post-batch update optimization for {batch_id}")
    
    async def cleanup_post_batch_delete(self, batch_id: str) -> None:
        """Background cleanup after batch delete."""
        logger.info(f"Running post-batch delete cleanup for {batch_id}")
        await asyncio.sleep(1)  # Simulate processing
        logger.info(f"Completed post-batch delete cleanup for {batch_id}")
    
    # Private helper methods
    
    def _validate_transaction_item(self, item: SalesTransactionCreate) -> None:
        """
        Validate a single transaction item using business rules.
        
        Args:
            item: Transaction item to validate
            
        Raises:
            ValidationError: If validation fails
        """
        # Use business rule validator
        self.validator.validate_transaction_amount(item.total_amount)
        self.validator.validate_quantity(item.quantity)
        
        # Additional validations can be added here
        if hasattr(item, 'discount_amount') and item.discount_amount:
            self.validator.validate_discount_amount(item.discount_amount, item.total_amount)