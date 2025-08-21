"""
Batch Operations API Router
Provides batch CRUD operations for efficient bulk data processing.
"""
from typing import List, Dict, Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from pydantic import BaseModel, Field
from sqlmodel import Session

from api.v1.services.batch_service import BatchService
from data_access.db import get_session
from core.logging import get_logger
from domain.entities.sales_transaction import SalesTransactionCreate, SalesTransactionResponse

router = APIRouter(prefix="/batch", tags=["batch-operations"])
logger = get_logger(__name__)


class BatchCreateRequest(BaseModel):
    """Request model for batch create operations."""
    items: List[SalesTransactionCreate] = Field(..., min_items=1, max_items=1000)
    validate_items: bool = Field(default=True, description="Whether to validate each item")
    fail_on_error: bool = Field(default=False, description="Whether to fail entire batch on single item error")
    
    class Config:
        json_encoders = {
            UUID: lambda v: str(v)
        }


class BatchUpdateRequest(BaseModel):
    """Request model for batch update operations."""
    updates: List[Dict[str, Any]] = Field(..., min_items=1, max_items=1000)
    upsert: bool = Field(default=False, description="Whether to create items if they don't exist")
    
    class Config:
        json_encoders = {
            UUID: lambda v: str(v)
        }


class BatchDeleteRequest(BaseModel):
    """Request model for batch delete operations."""
    ids: List[UUID] = Field(..., min_items=1, max_items=1000)
    soft_delete: bool = Field(default=True, description="Whether to perform soft delete")


class BatchOperationResponse(BaseModel):
    """Response model for batch operations."""
    operation: str
    total_requested: int
    successful: int
    failed: int
    errors: List[Dict[str, Any]]
    processing_time_ms: float
    batch_id: Optional[str] = None
    
    class Config:
        json_encoders = {
            UUID: lambda v: str(v)
        }


@router.post("/create", response_model=BatchOperationResponse)
async def batch_create_transactions(
    request: BatchCreateRequest,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session)
) -> BatchOperationResponse:
    """
    Batch create multiple sales transactions.
    
    Features:
    - Validates up to 1000 items per batch
    - Optional individual item validation
    - Configurable error handling (fail-fast or continue)
    - Background processing for large batches
    """
    logger.info(f"Batch create request for {len(request.items)} items")
    
    try:
        batch_service = BatchService(session)
        
        # Process batch creation
        result = await batch_service.batch_create_transactions(
            items=request.items,
            validate_items=request.validate_items,
            fail_on_error=request.fail_on_error
        )
        
        # If large batch, process in background
        if len(request.items) > 100:
            background_tasks.add_task(
                batch_service.optimize_post_batch_creation,
                result.get("batch_id")
            )
        
        return BatchOperationResponse(
            operation="create",
            total_requested=len(request.items),
            successful=result.get("successful", 0),
            failed=result.get("failed", 0),
            errors=result.get("errors", []),
            processing_time_ms=result.get("processing_time_ms", 0),
            batch_id=result.get("batch_id")
        )
        
    except Exception as e:
        logger.error(f"Batch create failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch create operation failed: {str(e)}"
        )


@router.put("/update", response_model=BatchOperationResponse)
async def batch_update_transactions(
    request: BatchUpdateRequest,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session)
) -> BatchOperationResponse:
    """
    Batch update multiple sales transactions.
    
    Features:
    - Updates up to 1000 items per batch
    - Optional upsert functionality
    - Partial update support
    - Optimistic concurrency control
    """
    logger.info(f"Batch update request for {len(request.updates)} items")
    
    try:
        batch_service = BatchService(session)
        
        # Process batch updates
        result = await batch_service.batch_update_transactions(
            updates=request.updates,
            upsert=request.upsert
        )
        
        # Background optimization for large batches
        if len(request.updates) > 100:
            background_tasks.add_task(
                batch_service.optimize_post_batch_update,
                result.get("batch_id")
            )
        
        return BatchOperationResponse(
            operation="update",
            total_requested=len(request.updates),
            successful=result.get("successful", 0),
            failed=result.get("failed", 0),
            errors=result.get("errors", []),
            processing_time_ms=result.get("processing_time_ms", 0),
            batch_id=result.get("batch_id")
        )
        
    except Exception as e:
        logger.error(f"Batch update failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch update operation failed: {str(e)}"
        )


@router.delete("/delete", response_model=BatchOperationResponse)
async def batch_delete_transactions(
    request: BatchDeleteRequest,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session)
) -> BatchOperationResponse:
    """
    Batch delete multiple sales transactions.
    
    Features:
    - Deletes up to 1000 items per batch
    - Soft delete or hard delete options
    - Referential integrity checks
    - Audit trail for deletions
    """
    logger.info(f"Batch delete request for {len(request.ids)} items")
    
    try:
        batch_service = BatchService(session)
        
        # Process batch deletions
        result = await batch_service.batch_delete_transactions(
            ids=request.ids,
            soft_delete=request.soft_delete
        )
        
        # Background cleanup for large batches
        if len(request.ids) > 100:
            background_tasks.add_task(
                batch_service.cleanup_post_batch_delete,
                result.get("batch_id")
            )
        
        return BatchOperationResponse(
            operation="delete",
            total_requested=len(request.ids),
            successful=result.get("successful", 0),
            failed=result.get("failed", 0),
            errors=result.get("errors", []),
            processing_time_ms=result.get("processing_time_ms", 0),
            batch_id=result.get("batch_id")
        )
        
    except Exception as e:
        logger.error(f"Batch delete failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch delete operation failed: {str(e)}"
        )


@router.post("/upsert", response_model=BatchOperationResponse)
async def batch_upsert_transactions(
    request: BatchCreateRequest,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session)
) -> BatchOperationResponse:
    """
    Batch upsert (insert or update) sales transactions.
    
    Features:
    - Creates new items or updates existing ones
    - Conflict resolution strategies
    - Optimized for data synchronization
    """
    logger.info(f"Batch upsert request for {len(request.items)} items")
    
    try:
        batch_service = BatchService(session)
        
        # Process batch upserts
        result = await batch_service.batch_upsert_transactions(
            items=request.items,
            validate_items=request.validate_items
        )
        
        return BatchOperationResponse(
            operation="upsert",
            total_requested=len(request.items),
            successful=result.get("successful", 0),
            failed=result.get("failed", 0),
            errors=result.get("errors", []),
            processing_time_ms=result.get("processing_time_ms", 0),
            batch_id=result.get("batch_id")
        )
        
    except Exception as e:
        logger.error(f"Batch upsert failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch upsert operation failed: {str(e)}"
        )


@router.get("/status/{batch_id}")
async def get_batch_status(
    batch_id: str,
    session: Session = Depends(get_session)
) -> Dict[str, Any]:
    """
    Get status of a batch operation.
    
    Useful for tracking long-running batch operations.
    """
    try:
        batch_service = BatchService(session)
        status_info = await batch_service.get_batch_status(batch_id)
        
        if not status_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Batch operation {batch_id} not found"
            )
        
        return status_info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get batch status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get batch status: {str(e)}"
        )


@router.get("/operations/active")
async def get_active_batch_operations(
    session: Session = Depends(get_session)
) -> Dict[str, Any]:
    """
    Get list of currently active batch operations.
    
    Useful for monitoring and management.
    """
    try:
        batch_service = BatchService(session)
        active_operations = await batch_service.get_active_operations()
        
        return {
            "active_operations": active_operations,
            "count": len(active_operations),
            "timestamp": batch_service.get_current_timestamp()
        }
        
    except Exception as e:
        logger.error(f"Failed to get active operations: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get active operations: {str(e)}"
        )


@router.post("/validate")
async def validate_batch_data(
    request: BatchCreateRequest,
    session: Session = Depends(get_session)
) -> Dict[str, Any]:
    """
    Validate batch data without performing operations.
    
    Useful for pre-validation before actual batch processing.
    """
    try:
        batch_service = BatchService(session)
        validation_result = await batch_service.validate_batch_data(request.items)
        
        return {
            "total_items": len(request.items),
            "valid_items": validation_result.get("valid_count", 0),
            "invalid_items": validation_result.get("invalid_count", 0),
            "validation_errors": validation_result.get("errors", []),
            "is_valid": validation_result.get("is_valid", False),
            "recommendations": validation_result.get("recommendations", [])
        }
        
    except Exception as e:
        logger.error(f"Batch validation failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch validation failed: {str(e)}"
        )


@router.get("/limits")
def get_batch_limits() -> Dict[str, Any]:
    """
    Get information about batch operation limits and constraints.
    """
    return {
        "max_items_per_batch": 1000,
        "max_concurrent_batches": 10,
        "supported_operations": ["create", "update", "delete", "upsert"],
        "validation_options": {
            "validate_items": "Validate each item individually",
            "fail_on_error": "Stop batch on first error",
            "soft_delete": "Mark as deleted instead of removing"
        },
        "performance_recommendations": {
            "optimal_batch_size": "100-500 items",
            "background_processing_threshold": "100+ items",
            "concurrent_batch_limit": "Maximum 5 concurrent operations recommended"
        },
        "rate_limits": {
            "requests_per_minute": 60,
            "items_per_hour": 50000
        }
    }