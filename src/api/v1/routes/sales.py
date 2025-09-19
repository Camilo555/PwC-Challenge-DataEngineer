
from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException, status

from api.v1.schemas.sales import PaginatedSales, SaleItem
from api.v1.services.sales_service import SalesService
from api.pagination.cursor_pagination import (
    PaginationRequest, PaginatedResult, get_paginator, pagination_params
)
from core.dependency_injection import get_service
from domain.interfaces.sales_service import ISalesService
from domain.mappers.model_mapper import ModelMapper
from core.logging import get_logger

logger = get_logger(__name__)

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
    """Get paginated sales data with proper dependency injection (Legacy offset pagination)."""
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


@router.get("/cursor", response_model=dict)
async def list_sales_cursor(
    date_from: Optional[str] = Query(None, description="Filter sales from date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter sales to date (YYYY-MM-DD)"),
    product: Optional[str] = Query(None, description="Filter by product name"),
    country: Optional[str] = Query(None, description="Filter by country"),
    pagination: PaginationRequest = Depends(pagination_params),
    service: SalesService = Depends(get_sales_service)
) -> dict:
    """
    Get sales data using advanced cursor-based pagination for optimal performance.

    This endpoint provides:
    - Cursor-based pagination for consistent results even when data changes
    - High-performance navigation through large datasets
    - Forward and backward pagination support
    - Configurable sorting with multiple fields
    """

    logger.info(f"Cursor pagination request: {pagination}")

    try:
        # Get the paginator
        paginator = get_paginator()

        # Get query from service with filters
        query = await service.get_filtered_query(
            date_from=date_from,
            date_to=date_to,
            product=product,
            country=country
        )

        # Apply cursor-based pagination
        result = await paginator.paginate_with_cache(
            query=query,
            request=pagination,
            cursor_field=pagination.sort_by,
            cache_key=f"sales:{date_from}:{date_to}:{product}:{country}:{pagination.sort_by}",
            cache_ttl=300  # 5 minutes cache
        )

        # Add performance metrics to response
        performance_stats = paginator.get_performance_stats()

        response = result.to_dict()
        response["performance"] = {
            "pagination_time_ms": performance_stats["avg_query_time_ms"],
            "cache_hit_ratio": performance_stats["cache_hit_ratio"],
            "total_queries": performance_stats["queries_executed"]
        }

        return response

    except Exception as e:
        logger.error(f"Error in cursor pagination: {e}")
        raise


@router.get("/analytics/cursor", response_model=dict)
async def get_sales_analytics_cursor(
    date_from: Optional[str] = Query(None, description="Analytics from date"),
    date_to: Optional[str] = Query(None, description="Analytics to date"),
    group_by: str = Query("month", description="Group analytics by: day, week, month, quarter"),
    pagination: PaginationRequest = Depends(pagination_params),
    service: SalesService = Depends(get_sales_service)
) -> dict:
    """
    Get sales analytics with cursor-based pagination for large analytical datasets.

    Optimized for:
    - Large analytical result sets (millions of records)
    - Time-series data pagination
    - Consistent navigation through aggregated data
    """

    try:
        paginator = get_paginator()

        # Get analytics query
        analytics_query = await service.get_analytics_query(
            date_from=date_from,
            date_to=date_to,
            group_by=group_by
        )

        # Use timestamp-based cursor for time-series data
        result = await paginator.paginate_with_cache(
            query=analytics_query,
            request=pagination,
            cursor_field="period_date",  # Use date field for time-series pagination
            cache_key=f"analytics:{group_by}:{date_from}:{date_to}",
            cache_ttl=600  # 10 minutes cache for analytics
        )

        return result.to_dict()

    except Exception as e:
        logger.error(f"Error in analytics cursor pagination: {e}")
        raise


@router.get("/export/cursor", response_model=dict)
async def export_sales_cursor(
    date_from: Optional[str] = Query(None, description="Export from date"),
    date_to: Optional[str] = Query(None, description="Export to date"),
    format: str = Query("json", description="Export format: json, csv, excel"),
    pagination: PaginationRequest = Depends(pagination_params),
    service: SalesService = Depends(get_sales_service)
) -> dict:
    """
    Export large sales datasets using cursor-based pagination.

    Features:
    - Memory-efficient export of large datasets
    - Resumable exports using cursor navigation
    - Multiple format support with streaming
    - Progress tracking for long-running exports
    """

    try:
        # Use larger page size for export operations
        export_pagination = PaginationRequest(
            first=min(pagination.first or 1000, 5000),  # Max 5000 per page for exports
            after=pagination.after,
            sort_by=pagination.sort_by,
            sort_direction=pagination.sort_direction
        )

        paginator = get_paginator()

        # Get export query
        export_query = await service.get_export_query(
            date_from=date_from,
            date_to=date_to,
            format=format
        )

        # Execute pagination
        result = await paginator.paginate(
            query=export_query,
            request=export_pagination,
            cursor_field="id"
        )

        # Add export metadata
        response = result.to_dict()
        response["export_info"] = {
            "format": format,
            "total_pages_estimated": result.total_count // (export_pagination.first or 1000) if result.total_count else None,
            "records_per_page": export_pagination.first or 1000,
            "can_resume": bool(result.page_info.end_cursor),
            "next_cursor": result.page_info.end_cursor
        }

        return response

    except Exception as e:
        logger.error(f"Error in export cursor pagination: {e}")
        raise


@router.post("/batch/query", response_model=dict)
async def batch_query_sales(
    requests: list[dict] = [],
    service: SalesService = Depends(get_sales_service)
) -> dict:
    """
    Execute multiple sales queries in a single batch request for optimal performance.

    Features:
    - Process multiple query requests simultaneously
    - Parallel query execution for better throughput
    - Reduced network overhead compared to individual requests
    - Maintains query isolation and error handling per request

    Example request:
    [
        {
            "query_id": "q1",
            "date_from": "2023-01-01",
            "date_to": "2023-12-31",
            "product": "Widget A",
            "page": 1,
            "size": 100
        },
        {
            "query_id": "q2",
            "country": "United Kingdom",
            "page": 1,
            "size": 50
        }
    ]
    """

    if not requests or len(requests) > 10:  # Limit batch size
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Batch requests must contain 1-10 queries"
        )

    import asyncio
    from datetime import datetime

    async def execute_single_query(request_data: dict):
        """Execute a single sales query with error isolation."""
        query_id = request_data.get("query_id", f"query_{id(request_data)}")

        try:
            start_time = asyncio.get_event_loop().time()

            # Extract query parameters
            items, total = await service.query_sales(
                date_from=request_data.get("date_from"),
                date_to=request_data.get("date_to"),
                product=request_data.get("product"),
                country=request_data.get("country"),
                page=request_data.get("page", 1),
                size=request_data.get("size", 20),
                sort=request_data.get("sort", "invoice_date:desc")
            )

            execution_time = asyncio.get_event_loop().time() - start_time

            return {
                "query_id": query_id,
                "status": "success",
                "data": {
                    "items": [item.model_dump() for item in items],
                    "total": total,
                    "page": request_data.get("page", 1),
                    "size": request_data.get("size", 20)
                },
                "execution_time_ms": round(execution_time * 1000, 2),
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error in batch query {query_id}: {e}")
            return {
                "query_id": query_id,
                "status": "error",
                "error": str(e),
                "execution_time_ms": 0,
                "timestamp": datetime.now().isoformat()
            }

    try:
        # Execute all queries in parallel
        start_batch_time = asyncio.get_event_loop().time()
        results = await asyncio.gather(*[
            execute_single_query(req) for req in requests
        ], return_exceptions=True)

        total_batch_time = asyncio.get_event_loop().time() - start_batch_time

        # Count successful vs failed queries
        successful_queries = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
        failed_queries = len(results) - successful_queries

        return {
            "batch_id": f"batch_{int(datetime.now().timestamp())}",
            "total_requests": len(requests),
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "total_execution_time_ms": round(total_batch_time * 1000, 2),
            "results": [r if isinstance(r, dict) else {"status": "error", "error": str(r)} for r in results],
            "processed_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Batch query processing failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch processing failed: {str(e)}"
        )


@router.post("/batch/analytics", response_model=dict)
async def batch_analytics_query(
    requests: list[dict] = [],
    service: SalesService = Depends(get_sales_service)
) -> dict:
    """
    Execute multiple analytics queries in batch for dashboard and reporting needs.

    Features:
    - Parallel analytics query execution
    - Optimized for dashboard data aggregation
    - Supports different granularities and filters per query
    - Performance metrics for monitoring query efficiency
    """

    if not requests or len(requests) > 5:  # Smaller limit for analytics due to complexity
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Analytics batch requests must contain 1-5 queries"
        )

    import asyncio
    from datetime import datetime

    async def execute_analytics_query(request_data: dict):
        """Execute a single analytics query."""
        query_id = request_data.get("query_id", f"analytics_{id(request_data)}")

        try:
            start_time = asyncio.get_event_loop().time()

            # Get analytics query from service
            query = await service.get_analytics_query(
                date_from=request_data.get("date_from"),
                date_to=request_data.get("date_to"),
                group_by=request_data.get("group_by", "month")
            )

            # Execute analytics with pagination
            from api.pagination.cursor_pagination import get_paginator, PaginationRequest

            paginator = get_paginator()
            pagination_request = PaginationRequest(
                first=request_data.get("limit", 100),
                sort_by=request_data.get("sort_by", "period_date"),
                sort_direction=request_data.get("sort_direction", "DESC")
            )

            result = await paginator.paginate(
                query=query,
                request=pagination_request,
                cursor_field="period_date"
            )

            execution_time = asyncio.get_event_loop().time() - start_time

            return {
                "query_id": query_id,
                "status": "success",
                "data": result.to_dict(),
                "execution_time_ms": round(execution_time * 1000, 2),
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error in batch analytics query {query_id}: {e}")
            return {
                "query_id": query_id,
                "status": "error",
                "error": str(e),
                "execution_time_ms": 0,
                "timestamp": datetime.now().isoformat()
            }

    try:
        # Execute all analytics queries in parallel
        start_batch_time = asyncio.get_event_loop().time()
        results = await asyncio.gather(*[
            execute_analytics_query(req) for req in requests
        ], return_exceptions=True)

        total_batch_time = asyncio.get_event_loop().time() - start_batch_time

        # Count successful vs failed queries
        successful_queries = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
        failed_queries = len(results) - successful_queries

        return {
            "batch_id": f"analytics_batch_{int(datetime.now().timestamp())}",
            "total_requests": len(requests),
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "total_execution_time_ms": round(total_batch_time * 1000, 2),
            "results": [r if isinstance(r, dict) else {"status": "error", "error": str(r)} for r in results],
            "processed_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Batch analytics processing failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch analytics processing failed: {str(e)}"
        )


@router.get("/batch/status/{batch_id}")
async def get_batch_status(batch_id: str) -> dict:
    """
    Get status information for a batch operation.

    This endpoint provides tracking for long-running batch operations
    and can be extended to support async batch processing in the future.
    """

    # For now, return basic status info
    # In a production system, this would query a batch status store

    return {
        "batch_id": batch_id,
        "status": "completed",  # Since we're doing synchronous processing
        "message": "Batch operations in this API are executed synchronously",
        "recommendation": "Use the batch query endpoints for parallel processing of multiple requests",
        "available_batch_endpoints": [
            "/api/v1/sales/batch/query",
            "/api/v1/sales/batch/analytics"
        ]
    }
