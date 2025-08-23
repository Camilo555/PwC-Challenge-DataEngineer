"""
Enhanced Sales Router for API V2
Advanced sales endpoints with improved functionality and performance.
"""

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from sqlmodel import Session

from api.v2.schemas.sales import (
    EnhancedSalesAnalytics,
    PaginatedEnhancedSales,
    SalesExportRequest,
    SalesExportResponse,
    SalesFiltersV2,
)
from api.v2.services.sales_service import EnhancedSalesService
from core.logging import get_logger
from data_access.db import get_session

router = APIRouter(prefix="/sales", tags=["sales-v2"])
logger = get_logger(__name__)


@router.get("", response_model=PaginatedEnhancedSales)
async def list_enhanced_sales(
    # Pagination
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(20, ge=1, le=100, description="Items per page"),
    sort: str = Query("invoice_date:desc", description="Sort field and direction"),

    # Basic filters
    date_from: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="End date (YYYY-MM-DD)"),
    countries: str | None = Query(None, description="Comma-separated country names"),
    categories: str | None = Query(None, description="Comma-separated product categories"),

    # Financial filters
    min_amount: float | None = Query(None, ge=0, description="Minimum transaction amount"),
    max_amount: float | None = Query(None, ge=0, description="Maximum transaction amount"),
    min_margin: float | None = Query(None, description="Minimum profit margin percentage"),

    # Customer filters
    customer_segments: str | None = Query(None, description="Comma-separated customer segments"),
    min_customer_ltv: float | None = Query(None, ge=0, description="Minimum customer lifetime value"),

    # Business context
    exclude_cancelled: bool = Query(True, description="Exclude cancelled transactions"),
    exclude_refunds: bool = Query(True, description="Exclude refund transactions"),
    include_weekends: bool | None = Query(None, description="Include weekend transactions"),
    include_holidays: bool | None = Query(None, description="Include holiday transactions"),

    # Response options
    include_aggregations: bool = Query(True, description="Include aggregation data"),

    session: Session = Depends(get_session)
) -> PaginatedEnhancedSales:
    """
    Get enhanced sales data with advanced filtering and analytics.
    
    This v2 endpoint provides:
    - Enhanced sale items with derived analytics fields
    - Advanced filtering options
    - Real-time aggregations
    - Data quality indicators
    - Performance metrics
    """
    service = EnhancedSalesService(session)

    try:
        # Build filters object
        filters = SalesFiltersV2()

        # Parse date filters
        if date_from:
            from datetime import datetime
            filters.date_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        if date_to:
            from datetime import datetime
            filters.date_to = datetime.strptime(date_to, "%Y-%m-%d").date()

        # Parse list filters
        if countries:
            filters.countries = [c.strip() for c in countries.split(",")]
        if categories:
            filters.categories = [c.strip() for c in categories.split(",")]
        if customer_segments:
            filters.customer_segments = [s.strip() for s in customer_segments.split(",")]

        # Set numeric filters
        if min_amount is not None:
            from decimal import Decimal
            filters.min_amount = Decimal(str(min_amount))
        if max_amount is not None:
            from decimal import Decimal
            filters.max_amount = Decimal(str(max_amount))
        if min_margin is not None:
            from decimal import Decimal
            filters.min_margin = Decimal(str(min_margin))
        if min_customer_ltv is not None:
            from decimal import Decimal
            filters.min_customer_ltv = Decimal(str(min_customer_ltv))

        # Set boolean filters
        filters.exclude_cancelled = exclude_cancelled
        filters.exclude_refunds = exclude_refunds
        filters.include_weekends = include_weekends
        filters.include_holidays = include_holidays

        return await service.get_enhanced_sales(
            filters=filters,
            page=page,
            size=size,
            sort=sort,
            include_aggregations=include_aggregations
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid parameter: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error in list_enhanced_sales: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve sales data"
        )


@router.post("/analytics", response_model=EnhancedSalesAnalytics)
async def get_comprehensive_analytics(
    filters: SalesFiltersV2 | None = Body(None),
    include_forecasting: bool = Query(False, description="Include forecasting analysis"),
    session: Session = Depends(get_session)
) -> EnhancedSalesAnalytics:
    """
    Get comprehensive sales analytics with multiple dimensions.
    
    This advanced endpoint provides:
    - Time series analysis
    - Geographic breakdown
    - Product performance metrics
    - Customer insights
    - Seasonal patterns
    - Optional forecasting
    """
    service = EnhancedSalesService(session)

    try:
        return await service.get_comprehensive_analytics(
            filters=filters,
            include_forecasting=include_forecasting
        )

    except Exception as e:
        logger.error(f"Error in get_comprehensive_analytics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate analytics"
        )


@router.post("/export", response_model=SalesExportResponse)
async def export_sales_data(
    export_request: SalesExportRequest,
    user_id: str | None = Query(None, description="User ID for tracking"),
    session: Session = Depends(get_session)
) -> SalesExportResponse:
    """
    Export sales data in various formats with advanced options.
    
    Supported formats:
    - CSV: Comma-separated values
    - Excel: Microsoft Excel format
    - Parquet: Columnar format for analytics
    - JSON: JavaScript Object Notation
    
    Features:
    - Advanced filtering
    - Optional analytics inclusion
    - Compression options
    - Email notifications
    - Background processing for large datasets
    """
    service = EnhancedSalesService(session)

    try:
        return await service.export_sales_data(
            export_request=export_request,
            user_id=user_id
        )

    except Exception as e:
        logger.error(f"Error in export_sales_data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate export"
        )


@router.get("/schema", response_model=dict)
async def get_enhanced_schema() -> dict:
    """
    Get the enhanced schema definition for v2 sales data.
    
    Returns field definitions, types, and descriptions for the enhanced sale item.
    """
    try:
        from api.v2.schemas.sales import EnhancedSaleItem

        # Get Pydantic schema
        schema = EnhancedSaleItem.schema()

        # Add v2-specific enhancements info
        schema["version"] = "2.0"
        schema["enhancements"] = {
            "new_fields": [
                "customer_lifetime_value",
                "revenue_per_unit",
                "profitability_score",
                "customer_value_tier",
                "fiscal_year",
                "fiscal_quarter",
                "is_weekend",
                "is_holiday"
            ],
            "improved_filtering": [
                "geographic_filters",
                "customer_value_filters",
                "business_context_filters",
                "financial_filters"
            ],
            "performance_improvements": [
                "parallel_processing",
                "optimized_queries",
                "response_time_tracking",
                "data_quality_indicators"
            ]
        }

        return schema

    except Exception as e:
        logger.error(f"Error in get_enhanced_schema: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve schema"
        )


@router.get("/performance/benchmark")
async def benchmark_performance(
    sample_size: int = Query(1000, ge=100, le=10000, description="Sample size for benchmark"),
    include_analytics: bool = Query(True, description="Include analytics in benchmark"),
    session: Session = Depends(get_session)
) -> dict:
    """
    Benchmark v2 API performance with various operations.
    
    Returns performance metrics for different operations to help with optimization.
    """
    import time
    service = EnhancedSalesService(session)

    try:
        benchmark_results = {}

        # Benchmark basic query
        start_time = time.time()
        result = await service.get_enhanced_sales(
            filters=None,
            page=1,
            size=sample_size,
            include_aggregations=False
        )
        benchmark_results["basic_query_ms"] = (time.time() - start_time) * 1000
        benchmark_results["records_retrieved"] = len(result.items)

        # Benchmark with aggregations
        if include_analytics:
            start_time = time.time()
            result = await service.get_enhanced_sales(
                filters=None,
                page=1,
                size=min(sample_size, 100),  # Limit for analytics
                include_aggregations=True
            )
            benchmark_results["with_aggregations_ms"] = (time.time() - start_time) * 1000

        # Add system info
        benchmark_results["metadata"] = {
            "version": "2.0",
            "timestamp": time.time(),
            "sample_size": sample_size,
            "include_analytics": include_analytics
        }

        return benchmark_results

    except Exception as e:
        logger.error(f"Error in benchmark_performance: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to run performance benchmark"
        )
