"""
Data Mart API Router
Provides access to star schema data marts for analytics and business intelligence.
"""
from datetime import datetime, date
from typing import Any, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel import select, Session

from api.v1.services.datamart_service import DataMartService
from core.logging import get_logger
from data_access.db import get_session

router = APIRouter(prefix="/datamart", tags=["datamart"])
logger = get_logger(__name__)


@router.get("/dashboard/overview", response_model=dict[str, Any])
async def get_dashboard_overview(
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    session: Session = Depends(get_session)
) -> dict[str, Any]:
    """Get high-level dashboard overview metrics from the data mart."""
    service = DataMartService(session)
    
    try:
        return await service.get_dashboard_overview(
            date_from=date_from,
            date_to=date_to
        )
    except Exception as e:
        logger.error(f"Error getting dashboard overview: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve dashboard overview"
        )


@router.get("/sales/analytics", response_model=dict[str, Any])
async def get_sales_analytics(
    granularity: str = Query("monthly", regex="^(daily|weekly|monthly|quarterly)$"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    product_category: Optional[str] = Query(None),
    session: Session = Depends(get_session)
) -> dict[str, Any]:
    """Get detailed sales analytics with various time granularities."""
    service = DataMartService(session)
    
    try:
        return await service.get_sales_analytics(
            granularity=granularity,
            date_from=date_from,
            date_to=date_to,
            country=country,
            product_category=product_category
        )
    except Exception as e:
        logger.error(f"Error getting sales analytics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve sales analytics"
        )


@router.get("/customers/segments", response_model=List[dict[str, Any]])
async def get_customer_segments(
    session: Session = Depends(get_session)
) -> List[dict[str, Any]]:
    """Get customer segmentation analysis from RFM scoring."""
    service = DataMartService(session)
    
    try:
        return await service.get_customer_segments()
    except Exception as e:
        logger.error(f"Error getting customer segments: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve customer segments"
        )


@router.get("/customers/{customer_id}/analytics", response_model=dict[str, Any])
async def get_customer_analytics(
    customer_id: str,
    session: Session = Depends(get_session)
) -> dict[str, Any]:
    """Get detailed analytics for a specific customer."""
    service = DataMartService(session)
    
    try:
        result = await service.get_customer_analytics(customer_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Customer {customer_id} not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting customer analytics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve customer analytics"
        )


@router.get("/products/performance", response_model=List[dict[str, Any]])
async def get_product_performance(
    top_n: int = Query(20, ge=1, le=100, description="Number of top products to return"),
    metric: str = Query("revenue", regex="^(revenue|quantity|profit|margin)$"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    session: Session = Depends(get_session)
) -> List[dict[str, Any]]:
    """Get top-performing products by various metrics."""
    service = DataMartService(session)
    
    try:
        return await service.get_product_performance(
            top_n=top_n,
            metric=metric,
            date_from=date_from,
            date_to=date_to
        )
    except Exception as e:
        logger.error(f"Error getting product performance: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve product performance"
        )


@router.get("/countries/performance", response_model=List[dict[str, Any]])
async def get_country_performance(
    session: Session = Depends(get_session)
) -> List[dict[str, Any]]:
    """Get sales performance by country."""
    service = DataMartService(session)
    
    try:
        return await service.get_country_performance()
    except Exception as e:
        logger.error(f"Error getting country performance: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve country performance"
        )


@router.get("/trends/seasonal", response_model=dict[str, Any])
async def get_seasonal_trends(
    year: Optional[int] = Query(None, ge=2000, le=2030),
    session: Session = Depends(get_session)
) -> dict[str, Any]:
    """Get seasonal sales trends and patterns."""
    service = DataMartService(session)
    
    try:
        return await service.get_seasonal_trends(year=year)
    except Exception as e:
        logger.error(f"Error getting seasonal trends: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve seasonal trends"
        )


@router.get("/cohorts/analysis", response_model=dict[str, Any])
async def get_cohort_analysis(
    cohort_type: str = Query("monthly", regex="^(weekly|monthly|quarterly)$"),
    session: Session = Depends(get_session)
) -> dict[str, Any]:
    """Get customer cohort analysis for retention insights."""
    service = DataMartService(session)
    
    try:
        return await service.get_cohort_analysis(cohort_type=cohort_type)
    except Exception as e:
        logger.error(f"Error getting cohort analysis: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve cohort analysis"
        )


@router.get("/metrics/business", response_model=dict[str, Any])
async def get_business_metrics(
    session: Session = Depends(get_session)
) -> dict[str, Any]:
    """Get key business metrics and KPIs."""
    service = DataMartService(session)
    
    try:
        return await service.get_business_metrics()
    except Exception as e:
        logger.error(f"Error getting business metrics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve business metrics"
        )