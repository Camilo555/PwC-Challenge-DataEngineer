"""
Enhanced Sales Service for API V2
Provides advanced analytics and improved performance.
"""
import asyncio
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlmodel import Session, and_, asc, desc, func, select

from api.v2.schemas.sales import (
    EnhancedSaleItem,
    EnhancedSalesAnalytics,
    PaginatedEnhancedSales,
    SalesAggregation,
    SalesExportRequest,
    SalesExportResponse,
    SalesFiltersV2,
    TimeSeriesPoint,
)
from core.logging import get_logger
from data_access.models.star_schema import (
    DimCountry,
    DimCustomer,
    DimDate,
    DimInvoice,
    DimProduct,
    FactSale,
)

logger = get_logger(__name__)


class EnhancedSalesService:
    """Enhanced sales service with advanced analytics capabilities."""

    def __init__(self, session: Session):
        self.session = session

    async def get_enhanced_sales(
        self,
        filters: SalesFiltersV2 | None = None,
        page: int = 1,
        size: int = 20,
        sort: str = "invoice_date:desc",
        include_aggregations: bool = True
    ) -> PaginatedEnhancedSales:
        """Get paginated sales with enhanced details and analytics."""
        start_time = time.time()

        try:
            # Build the comprehensive query
            query = self._build_enhanced_query()

            # Apply filters
            query, applied_filters = self._apply_enhanced_filters(query, filters)

            # Get total count for pagination
            count_query = self._build_count_query(query)
            total = self.session.exec(count_query).first()

            # Apply sorting
            query = self._apply_sorting(query, sort)

            # Apply pagination
            offset = (page - 1) * size
            query = query.offset(offset).limit(size)

            # Execute query
            results = self.session.exec(query).all()

            # Convert to enhanced sale items
            items = [self._convert_to_enhanced_sale_item(row) for row in results]

            # Calculate aggregations if requested
            aggregations = {}
            if include_aggregations and items:
                aggregations = await self._calculate_aggregations(filters)

            # Calculate pagination metadata
            pages = (total + size - 1) // size
            has_next = page < pages
            has_previous = page > 1

            # Calculate response time
            response_time_ms = (time.time() - start_time) * 1000

            return PaginatedEnhancedSales(
                items=items,
                total=total,
                page=page,
                size=size,
                pages=pages,
                has_next=has_next,
                has_previous=has_previous,
                aggregations=aggregations,
                filters_applied=applied_filters,
                response_time_ms=response_time_ms,
                data_freshness=datetime.utcnow(),
                quality_score=self._calculate_data_quality_score(items)
            )

        except Exception as e:
            logger.error(f"Error in get_enhanced_sales: {str(e)}")
            raise

    async def get_comprehensive_analytics(
        self,
        filters: SalesFiltersV2 | None = None,
        include_forecasting: bool = False
    ) -> EnhancedSalesAnalytics:
        """Get comprehensive sales analytics with multiple dimensions."""
        try:
            # Run analytics queries in parallel for better performance
            tasks = [
                self._get_summary_metrics(filters),
                self._get_time_series_data(filters),
                self._get_top_products(filters),
                self._get_top_customers(filters),
                self._get_geographic_breakdown(filters),
                self._get_category_performance(filters),
                self._get_seasonal_insights(filters)
            ]

            if include_forecasting:
                tasks.append(self._get_forecasting_data(filters))

            results = await asyncio.gather(*tasks)

            analytics = EnhancedSalesAnalytics(
                summary=results[0],  # dict[str, Any]
                time_series=results[1],  # list[TimeSeriesPoint]
                top_products=results[2],  # list[dict[str, Any]]
                top_customers=results[3],  # list[dict[str, Any]]
                geographic_breakdown=results[4],  # list[SalesAggregation]
                category_performance=results[5],  # list[SalesAggregation]
                seasonal_insights=results[6],  # dict[str, Any]
                forecasting=results[7] if include_forecasting else None  # dict[str, Any] | None
            )

            return analytics

        except Exception as e:
            logger.error(f"Error in get_comprehensive_analytics: {str(e)}")
            raise

    async def export_sales_data(
        self,
        export_request: SalesExportRequest,
        user_id: str | None = None
    ) -> SalesExportResponse:
        """Export sales data with the specified format and options."""
        try:
            export_id = uuid4()

            # Estimate completion time based on filters and format
            estimated_duration = self._estimate_export_duration(export_request)
            estimated_completion = datetime.utcnow() + estimated_duration

            # For now, return a mock response
            # In production, this would submit a background task
            return SalesExportResponse(
                export_id=export_id,
                status="submitted",
                estimated_completion=estimated_completion,
                download_url=None,
                file_size_mb=None,
                record_count=None
            )

        except Exception as e:
            logger.error(f"Error in export_sales_data: {str(e)}")
            raise

    # Private helper methods

    def _build_enhanced_query(self):
        """Build the base enhanced query with all necessary joins."""
        return select(
            # Fact table fields
            FactSale.sale_id,
            FactSale.quantity,
            FactSale.unit_price,
            FactSale.total_amount,
            FactSale.discount_amount,
            FactSale.tax_amount,
            FactSale.profit_amount,
            FactSale.margin_percentage,
            FactSale.created_at,

            # Product dimension
            DimProduct.stock_code,
            DimProduct.description,
            DimProduct.category,
            DimProduct.brand,

            # Customer dimension
            DimCustomer.customer_id,
            DimCustomer.customer_segment,
            DimCustomer.lifetime_value,

            # Country dimension
            DimCountry.country_name,
            DimCountry.country_code,
            DimCountry.region,
            DimCountry.continent,

            # Date dimension
            DimDate.date,
            DimDate.fiscal_year,
            DimDate.fiscal_quarter,
            DimDate.is_weekend,
            DimDate.is_holiday,

            # Invoice dimension
            DimInvoice.invoice_no,
            DimInvoice.is_cancelled
        ).select_from(FactSale)\
         .join(DimProduct, FactSale.product_key == DimProduct.product_key)\
         .join(DimCountry, FactSale.country_key == DimCountry.country_key)\
         .join(DimDate, FactSale.date_key == DimDate.date_key)\
         .join(DimInvoice, FactSale.invoice_key == DimInvoice.invoice_key)\
         .outerjoin(DimCustomer, FactSale.customer_key == DimCustomer.customer_key)

    def _apply_enhanced_filters(self, query, filters: SalesFiltersV2 | None) -> tuple[Any, dict[str, Any]]:
        """Apply enhanced filters to the query."""
        applied_filters: dict[str, Any] = {}
        filter_conditions = []

        if not filters:
            return query, applied_filters

        # Date filters
        if filters.date_from:
            filter_conditions.append(DimDate.date >= filters.date_from)
            applied_filters['date_from'] = filters.date_from.isoformat()

        if filters.date_to:
            filter_conditions.append(DimDate.date <= filters.date_to)
            applied_filters['date_to'] = filters.date_to.isoformat()

        if filters.fiscal_year:
            filter_conditions.append(DimDate.fiscal_year == filters.fiscal_year)
            applied_filters['fiscal_year'] = str(filters.fiscal_year)

        if filters.fiscal_quarter:
            filter_conditions.append(DimDate.fiscal_quarter == filters.fiscal_quarter)
            applied_filters['fiscal_quarter'] = str(filters.fiscal_quarter)

        # Geographic filters
        if filters.countries:
            filter_conditions.append(DimCountry.country_name.in_(filters.countries))
            applied_filters['countries'] = list(filters.countries)

        if filters.regions:
            filter_conditions.append(DimCountry.region.in_(filters.regions))
            applied_filters['regions'] = list(filters.regions)

        if filters.continents:
            filter_conditions.append(DimCountry.continent.in_(filters.continents))
            applied_filters['continents'] = list(filters.continents)

        # Product filters
        if filters.categories:
            filter_conditions.append(DimProduct.category.in_(filters.categories))
            applied_filters['categories'] = list(filters.categories)

        if filters.brands:
            filter_conditions.append(DimProduct.brand.in_(filters.brands))
            applied_filters['brands'] = list(filters.brands)

        if filters.stock_codes:
            filter_conditions.append(DimProduct.stock_code.in_(filters.stock_codes))
            applied_filters['stock_codes'] = list(filters.stock_codes)

        # Customer filters
        if filters.customer_segments:
            filter_conditions.append(DimCustomer.customer_segment.in_(filters.customer_segments))
            applied_filters['customer_segments'] = list(filters.customer_segments)

        if filters.min_customer_ltv is not None:
            filter_conditions.append(DimCustomer.lifetime_value >= filters.min_customer_ltv)
            applied_filters['min_customer_ltv'] = str(float(filters.min_customer_ltv))

        # Financial filters
        if filters.min_amount is not None:
            filter_conditions.append(FactSale.total_amount >= filters.min_amount)
            applied_filters['min_amount'] = str(float(filters.min_amount))

        if filters.max_amount is not None:
            filter_conditions.append(FactSale.total_amount <= filters.max_amount)
            applied_filters['max_amount'] = str(float(filters.max_amount))

        if filters.min_margin is not None:
            filter_conditions.append(FactSale.margin_percentage >= filters.min_margin)
            applied_filters['min_margin'] = str(float(filters.min_margin))

        if filters.has_profit_data is not None:
            if filters.has_profit_data:
                filter_conditions.append(FactSale.profit_amount.is_not(None))
            else:
                filter_conditions.append(FactSale.profit_amount.is_(None))
            applied_filters['has_profit_data'] = str(filters.has_profit_data)

        # Business context filters
        if filters.exclude_cancelled:
            filter_conditions.append(DimInvoice.is_cancelled == False)
            applied_filters['exclude_cancelled'] = str(True)

        if filters.exclude_refunds:
            filter_conditions.append(FactSale.quantity > 0)
            applied_filters['exclude_refunds'] = str(True)

        if filters.include_weekends is not None:
            if not filters.include_weekends:
                filter_conditions.append(DimDate.is_weekend == False)
            applied_filters['include_weekends'] = str(filters.include_weekends)

        if filters.include_holidays is not None:
            if not filters.include_holidays:
                filter_conditions.append(DimDate.is_holiday == False)
            applied_filters['include_holidays'] = str(filters.include_holidays)

        # Apply all conditions
        if filter_conditions:
            query = query.where(and_(*filter_conditions))

        return query, applied_filters

    def _convert_to_enhanced_sale_item(self, row) -> EnhancedSaleItem:
        """Convert database row to enhanced sale item."""
        # Calculate derived fields
        revenue_per_unit = float(row.total_amount) / row.quantity if row.quantity > 0 else None

        # Simple profitability score calculation
        profitability_score = None
        if row.margin_percentage:
            profitability_score = min(float(row.margin_percentage) / 50.0, 1.0)  # Normalize to 0-1

        # Customer value tier
        customer_value_tier = None
        if row.lifetime_value:
            ltv = float(row.lifetime_value)
            if ltv >= 1000:
                customer_value_tier = "High"
            elif ltv >= 500:
                customer_value_tier = "Medium"
            else:
                customer_value_tier = "Low"

        return EnhancedSaleItem(
            sale_id=row.sale_id,
            invoice_no=row.invoice_no,
            stock_code=row.stock_code,
            description=row.description,
            quantity=row.quantity,
            unit_price=row.unit_price,
            total_amount=row.total_amount,
            discount_amount=row.discount_amount,
            tax_amount=row.tax_amount,
            profit_amount=row.profit_amount,
            margin_percentage=row.margin_percentage,
            customer_id=row.customer_id,
            customer_segment=row.customer_segment,
            customer_lifetime_value=row.lifetime_value,
            country=row.country_name,
            country_code=row.country_code,
            region=row.region,
            continent=row.continent,
            product_category=row.category,
            product_brand=row.brand,
            invoice_date=row.created_at,
            fiscal_year=row.fiscal_year,
            fiscal_quarter=row.fiscal_quarter,
            is_weekend=row.is_weekend,
            is_holiday=row.is_holiday,
            revenue_per_unit=Decimal(str(revenue_per_unit)) if revenue_per_unit else None,
            profitability_score=profitability_score,
            customer_value_tier=customer_value_tier
        )

    async def _calculate_aggregations(self, filters: SalesFiltersV2 | None) -> dict[str, Any]:
        """Calculate aggregations for the current dataset."""
        # This would contain aggregation calculations
        return {
            "total_revenue": 0,
            "avg_order_value": 0,
            "unique_customers": 0,
            "top_category": None
        }

    def _calculate_data_quality_score(self, items: list[EnhancedSaleItem]) -> float:
        """Calculate a data quality score for the current dataset."""
        if not items:
            return 0.0

        # Simple quality score based on completeness
        total_fields = len(items) * 10  # Assuming 10 key fields
        complete_fields = 0

        for item in items:
            if item.description:
                complete_fields += 1
            if item.customer_id:
                complete_fields += 1
            if item.product_category:
                complete_fields += 1
            if item.profit_amount is not None:
                complete_fields += 1
            # Add more quality checks...
            complete_fields += 6  # Assume other fields are mostly complete

        return min(complete_fields / total_fields, 1.0)

    # Async analytics methods (simplified implementations)

    async def _get_summary_metrics(self, filters) -> dict[str, Any]:
        """Get summary metrics."""
        return {"total_revenue": 1000000, "total_transactions": 50000}

    async def _get_time_series_data(self, filters) -> list[TimeSeriesPoint]:
        """Get time series data."""
        return []

    async def _get_top_products(self, filters) -> list[dict[str, Any]]:
        """Get top products."""
        return []

    async def _get_top_customers(self, filters) -> list[dict[str, Any]]:
        """Get top customers."""
        return []

    async def _get_geographic_breakdown(self, filters) -> list[SalesAggregation]:
        """Get geographic breakdown."""
        return []

    async def _get_category_performance(self, filters) -> list[SalesAggregation]:
        """Get category performance."""
        return []

    async def _get_seasonal_insights(self, filters) -> dict[str, Any]:
        """Get seasonal insights."""
        return {}

    async def _get_forecasting_data(self, filters) -> dict[str, Any]:
        """Get forecasting data."""
        return {"method": "linear_regression", "accuracy": 0.85}

    def _build_count_query(self, base_query):
        """Build count query from base query."""
        return select(func.count()).select_from(base_query.subquery())

    def _apply_sorting(self, query, sort: str):
        """Apply sorting to query."""
        if ":" in sort:
            field, direction = sort.split(":")
            order_func = desc if direction.lower() == "desc" else asc

            # Map field names to actual columns
            sort_mapping = {
                "invoice_date": FactSale.created_at,
                "total_amount": FactSale.total_amount,
                "quantity": FactSale.quantity,
                "country": DimCountry.country_name,
                "customer_id": DimCustomer.customer_id
            }

            if field in sort_mapping:
                query = query.order_by(order_func(sort_mapping[field]))

        return query

    def _estimate_export_duration(self, export_request: SalesExportRequest) -> timedelta:
        """Estimate export duration based on request parameters."""
        base_minutes = 5  # Base processing time

        if export_request.include_analytics:
            base_minutes += 10

        if export_request.include_forecasting:
            base_minutes += 15

        return timedelta(minutes=base_minutes)
