"""
GraphQL Resolvers
Business logic for GraphQL queries and mutations.
"""
from __future__ import annotations

import json
from datetime import datetime

import strawberry
from sqlmodel import Session, and_, desc, func, select
from strawberry.types import Info

from api.graphql.schemas import (
    BusinessMetrics,
    Country,
    Customer,
    CustomerSegment,
    MetricType,
    PaginatedSales,
    PaginationInput,
    Product,
    ProductPerformance,
    Sale,
    SalesAnalytics,
    SalesFilters,
    TaskStatus,
    TaskSubmissionInput,
    TimeGranularity,
)
from api.v1.services.async_tasks import AsyncTaskService
from api.v1.services.datamart_service import DataMartService
from api.graphql.dataloader import (
    DataLoaderRegistry, 
    DataLoaderContext,
    get_dataloaders,
    dataloader_monitor
)
from core.logging import get_logger
from data_access.db import get_session
from data_access.models.star_schema import (
    DimCountry,
    DimCustomer,
    DimDate,
    DimProduct,
    FactSale,
)

logger = get_logger(__name__)


def get_session_from_info(info: Info) -> Session:
    """Get database session from GraphQL info context."""
    return next(get_session())

def get_dataloaders_from_info(info: Info) -> dict:
    """Get DataLoaders from GraphQL info context."""
    if hasattr(info.context, 'dataloaders'):
        return info.context.dataloaders
    
    # Create new DataLoaders for this request
    session = get_session_from_info(info)
    dataloaders = get_dataloaders(session)
    
    # Store in context for request duration
    if not hasattr(info.context, 'dataloaders'):
        info.context.dataloaders = dataloaders
    
    return dataloaders


@strawberry.type
class Query:
    """GraphQL Query root."""

    @strawberry.field
    async def sales(
        self,
        info: Info,
        filters: SalesFilters | None = None,
        pagination: PaginationInput | None = None
    ) -> PaginatedSales:
        """Get paginated sales data with optional filters using optimized DataLoader pattern."""
        session = get_session_from_info(info)
        dataloaders = get_dataloaders_from_info(info)

        try:
            # Build optimized query - only fetch FactSale data and foreign keys
            query = select(
                FactSale.sale_id,
                FactSale.product_key,
                FactSale.customer_key,
                FactSale.country_key,
                FactSale.quantity,
                FactSale.unit_price,
                FactSale.total_amount,
                FactSale.discount_amount,
                FactSale.tax_amount,
                FactSale.profit_amount,
                FactSale.margin_percentage,
                FactSale.created_at
            ).select_from(FactSale)

            # Apply filters
            query_filters = []
            if filters:
                if filters.date_from or filters.date_to:
                    query = query.join(DimDate, FactSale.date_key == DimDate.date_key)
                    if filters.date_from:
                        query_filters.append(DimDate.date >= filters.date_from)
                    if filters.date_to:
                        query_filters.append(DimDate.date <= filters.date_to)

                if filters.country:
                    query_filters.append(DimCountry.country_name == filters.country)

                if filters.product_category:
                    query_filters.append(DimProduct.category == filters.product_category)

                if filters.customer_segment:
                    query_filters.append(DimCustomer.customer_segment == filters.customer_segment)

                if filters.min_amount:
                    query_filters.append(FactSale.total_amount >= filters.min_amount)

                if filters.max_amount:
                    query_filters.append(FactSale.total_amount <= filters.max_amount)

            if query_filters:
                query = query.where(and_(*query_filters))

            # Get total count
            count_query = select(func.count(FactSale.sale_id)).select_from(FactSale)
            if query_filters:
                # Apply same filters to count query
                count_query = count_query.join(DimProduct, FactSale.product_key == DimProduct.product_key)\
                                        .join(DimCountry, FactSale.country_key == DimCountry.country_key)\
                                        .outerjoin(DimCustomer, FactSale.customer_key == DimCustomer.customer_key)
                if filters and (filters.date_from or filters.date_to):
                    count_query = count_query.join(DimDate, FactSale.date_key == DimDate.date_key)
                count_query = count_query.where(and_(*query_filters))

            total_count = session.exec(count_query).first()

            # Apply pagination
            page = pagination.page if pagination else 1
            page_size = pagination.page_size if pagination else 20
            offset = (page - 1) * page_size

            query = query.offset(offset).limit(page_size).order_by(desc(FactSale.created_at))

            # Execute query
            results = session.exec(query).all()

            # Convert to GraphQL types using DataLoaders
            sales = []
            
            # Collect all foreign keys for batch loading
            product_keys = [row.product_key for row in results if row.product_key]
            customer_keys = [row.customer_key for row in results if row.customer_key]
            country_keys = [row.country_key for row in results if row.country_key]
            
            # Batch load all related data
            products_map = {}
            customers_map = {}
            countries_map = {}
            
            if product_keys:
                products = await dataloaders['products'].load_many(product_keys)
                products_map = {p.product_key: p for p in products if p}
            
            if customer_keys:
                customers = await dataloaders['customers'].load_many(customer_keys)
                customers_map = {c.customer_key: c for c in customers if c}
            
            if country_keys:
                countries = await dataloaders['countries'].load_many(country_keys)
                countries_map = {c.country_key: c for c in countries if c}
            
            # Build sale objects with loaded data
            for row in results:
                # Get related objects from loaded data
                product_data = products_map.get(row.product_key)
                customer_data = customers_map.get(row.customer_key)
                country_data = countries_map.get(row.country_key)
                
                # Create customer object if exists
                customer = None
                if customer_data:
                    customer = Customer(
                        customer_key=customer_data.customer_key,
                        customer_id=customer_data.customer_id,
                        customer_segment=customer_data.customer_segment,
                        lifetime_value=float(customer_data.lifetime_value) if customer_data.lifetime_value else None,
                        total_orders=customer_data.total_orders,
                        total_spent=float(customer_data.total_spent) if customer_data.total_spent else None,
                        avg_order_value=float(customer_data.avg_order_value) if customer_data.avg_order_value else None,
                        recency_score=customer_data.recency_score,
                        frequency_score=customer_data.frequency_score,
                        monetary_score=customer_data.monetary_score,
                        rfm_segment=customer_data.rfm_segment
                    )

                # Create product object
                product = None
                if product_data:
                    product = Product(
                        product_key=product_data.product_key,
                        stock_code=product_data.stock_code,
                        description=product_data.description,
                        category=product_data.category,
                        subcategory=product_data.subcategory,
                        brand=product_data.brand,
                        unit_cost=float(product_data.unit_cost) if product_data.unit_cost else None,
                        recommended_price=float(product_data.recommended_price) if product_data.recommended_price else None
                    )

                # Create country object
                country = None
                if country_data:
                    country = Country(
                        country_key=country_data.country_key,
                        country_code=country_data.country_code,
                        country_name=country_data.country_name,
                        region=country_data.region,
                        continent=country_data.continent,
                        currency_code=country_data.currency_code,
                        gdp_per_capita=float(country_data.gdp_per_capita) if country_data.gdp_per_capita else None,
                        population=country_data.population
                    )

                # Create sale object
                sale = Sale(
                    sale_id=str(row.sale_id),
                    quantity=row.quantity,
                    unit_price=float(row.unit_price),
                    total_amount=float(row.total_amount),
                    discount_amount=float(row.discount_amount),
                    tax_amount=float(row.tax_amount),
                    profit_amount=float(row.profit_amount) if row.profit_amount else None,
                    margin_percentage=float(row.margin_percentage) if row.margin_percentage else None,
                    created_at=row.created_at,
                    customer=customer,
                    product=product,
                    country=country
                )
                sales.append(sale)

            # Calculate pagination info
            has_next = (page * page_size) < total_count
            has_previous = page > 1

            return PaginatedSales(
                items=sales,
                total_count=total_count,
                page=page,
                page_size=page_size,
                has_next=has_next,
                has_previous=has_previous
            )

        except Exception as e:
            logger.error(f"Error in sales GraphQL query: {str(e)}")
            raise
        finally:
            session.close()
            
            # Log DataLoader performance stats for monitoring
            if hasattr(info.context, 'dataloaders'):
                stats = {}
                for name, loader in info.context.dataloaders.items():
                    stats[name] = loader.get_cache_stats()
                
                # Record stats with dataloader monitor
                dataloader_monitor.record_request(stats)
                logger.debug(f"DataLoader stats: {stats}")

    @strawberry.field
    async def sales_analytics(
        self,
        info: Info,
        granularity: TimeGranularity,
        filters: SalesFilters | None = None
    ) -> list[SalesAnalytics]:
        """Get sales analytics with time-based aggregation."""
        session = get_session_from_info(info)
        service = DataMartService(session)

        try:
            # Convert filters to service parameters
            date_from = filters.date_from.isoformat() if filters and filters.date_from else None
            date_to = filters.date_to.isoformat() if filters and filters.date_to else None
            country = filters.country if filters else None
            product_category = filters.product_category if filters else None

            result = await service.get_sales_analytics(
                granularity=granularity.value,
                date_from=date_from,
                date_to=date_to,
                country=country,
                product_category=product_category
            )

            # Convert to GraphQL types
            return [
                SalesAnalytics(
                    period=item["period"],
                    total_revenue=item["revenue"],
                    total_quantity=item["units_sold"],
                    transaction_count=item["transactions"],
                    unique_customers=item["unique_customers"],
                    avg_order_value=item["avg_order_value"]
                )
                for item in result["time_series"]
            ]

        except Exception as e:
            logger.error(f"Error in sales_analytics GraphQL query: {str(e)}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def customer_segments(self, info: Info) -> list[CustomerSegment]:
        """Get customer segmentation analysis."""
        session = get_session_from_info(info)
        service = DataMartService(session)

        try:
            segments = await service.get_customer_segments()

            return [
                CustomerSegment(
                    segment_name=segment["segment"],
                    customer_count=segment["customer_count"],
                    avg_lifetime_value=segment["avg_lifetime_value"],
                    avg_order_value=segment["avg_order_value"],
                    avg_total_orders=segment["avg_total_orders"]
                )
                for segment in segments
            ]

        except Exception as e:
            logger.error(f"Error in customer_segments GraphQL query: {str(e)}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def product_performance(
        self,
        info: Info,
        metric: MetricType,
        top_n: int = 20,
        filters: SalesFilters | None = None
    ) -> list[ProductPerformance]:
        """Get top-performing products by specified metric."""
        session = get_session_from_info(info)
        service = DataMartService(session)

        try:
            date_from = filters.date_from.isoformat() if filters and filters.date_from else None
            date_to = filters.date_to.isoformat() if filters and filters.date_to else None

            products = await service.get_product_performance(
                top_n=top_n,
                metric=metric.value,
                date_from=date_from,
                date_to=date_to
            )

            return [
                ProductPerformance(
                    stock_code=product["stock_code"],
                    description=product["description"],
                    category=product["category"],
                    total_revenue=product["total_revenue"],
                    total_quantity=product["total_quantity"],
                    transaction_count=product["transaction_count"]
                )
                for product in products
            ]

        except Exception as e:
            logger.error(f"Error in product_performance GraphQL query: {str(e)}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def business_metrics(self, info: Info) -> BusinessMetrics:
        """Get high-level business metrics."""
        session = get_session_from_info(info)
        service = DataMartService(session)

        try:
            metrics = await service.get_business_metrics()

            return BusinessMetrics(
                total_revenue=metrics["revenue_metrics"]["total_revenue"],
                total_transactions=metrics["sales_metrics"]["total_transactions"],
                unique_customers=metrics["customer_metrics"]["total_customers"],
                avg_order_value=metrics["sales_metrics"]["avg_order_value"],
                total_products=metrics["product_metrics"]["total_products"],
                active_countries=0  # Would need to calculate
            )

        except Exception as e:
            logger.error(f"Error in business_metrics GraphQL query: {str(e)}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def task_status(self, info: Info, task_id: str) -> TaskStatus | None:
        """Get the status of an async task."""
        service = AsyncTaskService()

        try:
            task_data = await service.get_task_status(task_id)

            if not task_data:
                return None

            return TaskStatus(
                task_id=task_data["task_id"],
                task_name=task_data["task_name"],
                status=task_data["status"],
                submitted_at=datetime.fromisoformat(task_data["submitted_at"]),
                progress=task_data.get("progress"),
                result=json.dumps(task_data["result"]) if task_data.get("result") else None,
                error=task_data.get("error")
            )

        except Exception as e:
            logger.error(f"Error in task_status GraphQL query: {str(e)}")
            raise


@strawberry.type
class Mutation:
    """GraphQL Mutation root."""

    @strawberry.mutation
    async def submit_task(
        self,
        info: Info,
        task_input: TaskSubmissionInput,
        user_id: str | None = None
    ) -> TaskStatus:
        """Submit an async task for processing."""
        service = AsyncTaskService()

        try:
            # Parse parameters
            parameters = json.loads(task_input.parameters)

            result = await service.submit_task(
                task_name=task_input.task_name,
                task_args=parameters,
                user_id=user_id
            )

            return TaskStatus(
                task_id=result["task_id"],
                task_name=task_input.task_name,
                status=result["status"],
                submitted_at=datetime.fromisoformat(result["submitted_at"]),
                progress=None,
                result=None,
                error=None
            )

        except Exception as e:
            logger.error(f"Error in submit_task GraphQL mutation: {str(e)}")
            raise

    @strawberry.mutation
    async def cancel_task(self, info: Info, task_id: str) -> bool:
        """Cancel a running task."""
        service = AsyncTaskService()

        try:
            return await service.cancel_task(task_id)

        except Exception as e:
            logger.error(f"Error in cancel_task GraphQL mutation: {str(e)}")
            raise


# Create the GraphQL schema
schema = strawberry.Schema(query=Query, mutation=Mutation)
