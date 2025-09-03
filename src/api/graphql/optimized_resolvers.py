"""
Optimized GraphQL Resolvers with DataLoader Pattern
Replaces N+1 queries with batch loading for superior performance.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import strawberry
from sqlmodel import Session, and_, desc, func, select
from strawberry.types import Info

from api.graphql.cache import get_graphql_cache
from api.graphql.dataloaders import DataLoaderFactory, get_dataloaders
from api.graphql.schemas import (
    BusinessMetrics,
    CustomerSegment,
    MetricType,
    PaginatedSales,
    PaginationInput,
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


def get_dataloaders_from_info(info: Info) -> DataLoaderFactory:
    """Get or create DataLoaders for this request context."""
    if not hasattr(info.context, "dataloaders"):
        session = get_session_from_info(info)
        info.context.dataloaders = get_dataloaders(session)
    return info.context.dataloaders


def get_user_context(info: Info) -> dict[str, Any] | None:
    """Extract user context from GraphQL info."""
    return getattr(info.context, "user", None)


@strawberry.type
class OptimizedQuery:
    """Optimized GraphQL Query root with DataLoader pattern."""

    @strawberry.field
    async def sales(
        self,
        info: Info,
        filters: SalesFilters | None = None,
        pagination: PaginationInput | None = None,
    ) -> PaginatedSales:
        """Get paginated sales data with optimal batch loading."""
        session = get_session_from_info(info)
        dataloaders = get_dataloaders_from_info(info)
        cache = get_graphql_cache()
        user_context = get_user_context(info)

        try:
            # Generate cache key for this query
            query_key = f"sales_{hash(str(filters) + str(pagination))}"
            variables = {
                "filters": filters.__dict__ if filters else None,
                "pagination": pagination.__dict__ if pagination else None,
            }

            # Try cache first
            cached_result = await cache.get(query_key, variables, user_context)
            if cached_result:
                logger.debug("Cache hit for sales query")
                return PaginatedSales(**cached_result)

            # Build optimized query - only fetch keys and essential fields
            base_query = select(
                FactSale.sale_id,
                FactSale.quantity,
                FactSale.unit_price,
                FactSale.total_amount,
                FactSale.discount_amount,
                FactSale.tax_amount,
                FactSale.profit_amount,
                FactSale.margin_percentage,
                FactSale.created_at,
                # Foreign keys for DataLoader batch loading
                FactSale.product_key,
                FactSale.customer_key,
                FactSale.country_key,
            ).select_from(FactSale)

            # Apply filters using efficient subqueries
            query_filters = []
            if filters:
                if filters.date_from or filters.date_to:
                    date_subquery = select(DimDate.date_key).select_from(DimDate)
                    if filters.date_from:
                        date_subquery = date_subquery.where(DimDate.date >= filters.date_from)
                    if filters.date_to:
                        date_subquery = date_subquery.where(DimDate.date <= filters.date_to)
                    query_filters.append(FactSale.date_key.in_(date_subquery))

                if filters.country:
                    country_subquery = select(DimCountry.country_key).where(
                        DimCountry.country_name == filters.country
                    )
                    query_filters.append(FactSale.country_key.in_(country_subquery))

                if filters.product_category:
                    product_subquery = select(DimProduct.product_key).where(
                        DimProduct.category == filters.product_category
                    )
                    query_filters.append(FactSale.product_key.in_(product_subquery))

                if filters.customer_segment:
                    customer_subquery = select(DimCustomer.customer_key).where(
                        DimCustomer.customer_segment == filters.customer_segment
                    )
                    query_filters.append(FactSale.customer_key.in_(customer_subquery))

                if filters.min_amount:
                    query_filters.append(FactSale.total_amount >= filters.min_amount)

                if filters.max_amount:
                    query_filters.append(FactSale.total_amount <= filters.max_amount)

            if query_filters:
                base_query = base_query.where(and_(*query_filters))

            # Get total count with same filters
            count_query = select(func.count(FactSale.sale_id)).select_from(FactSale)
            if query_filters:
                count_query = count_query.where(and_(*query_filters))

            total_count = session.exec(count_query).first()

            # Apply pagination
            page = pagination.page if pagination else 1
            page_size = pagination.page_size if pagination else 20
            offset = (page - 1) * page_size

            paginated_query = (
                base_query.offset(offset).limit(page_size).order_by(desc(FactSale.created_at))
            )

            # Execute main query
            results = session.exec(paginated_query).all()

            if not results:
                empty_result = PaginatedSales(
                    items=[],
                    total_count=0,
                    page=page,
                    page_size=page_size,
                    has_next=False,
                    has_previous=False,
                )
                await cache.set(query_key, empty_result.__dict__, variables, user_context, ttl=60)
                return empty_result

            # Extract foreign keys for batch loading
            customer_keys = [row.customer_key for row in results if row.customer_key]
            product_keys = [row.product_key for row in results]
            country_keys = [row.country_key for row in results]

            # Batch load all related entities using DataLoaders
            batch_tasks = []

            if customer_keys:
                batch_tasks.append(dataloaders.get_customer_loader().load_many(customer_keys))
            else:
                batch_tasks.append(None)

            batch_tasks.append(dataloaders.get_product_loader().load_many(product_keys))
            batch_tasks.append(dataloaders.get_country_loader().load_many(country_keys))

            # Wait for all batch loads
            customers_result, products_result, countries_result = await asyncio.gather(
                batch_tasks[0] if batch_tasks[0] else [],
                batch_tasks[1],
                batch_tasks[2],
                return_exceptions=True,
            )

            # Create lookup maps
            customers_map = {}
            if customers_result and customer_keys:
                customers_map = {
                    key: customer
                    for key, customer in zip(customer_keys, customers_result, strict=False)
                    if customer
                }

            products_map = {
                key: product
                for key, product in zip(product_keys, products_result, strict=False)
                if product
            }

            countries_map = {
                key: country
                for key, country in zip(country_keys, countries_result, strict=False)
                if country
            }

            # Build Sale objects with loaded relationships
            sales = []
            for row in results:
                customer = customers_map.get(row.customer_key)
                product = products_map.get(row.product_key)
                country = countries_map.get(row.country_key)

                sale = Sale(
                    sale_id=str(row.sale_id),
                    quantity=row.quantity,
                    unit_price=float(row.unit_price),
                    total_amount=float(row.total_amount),
                    discount_amount=float(row.discount_amount),
                    tax_amount=float(row.tax_amount),
                    profit_amount=float(row.profit_amount) if row.profit_amount else None,
                    margin_percentage=float(row.margin_percentage)
                    if row.margin_percentage
                    else None,
                    created_at=row.created_at,
                    customer=customer,
                    product=product,
                    country=country,
                )
                sales.append(sale)

            # Calculate pagination info
            has_next = (page * page_size) < total_count
            has_previous = page > 1

            result = PaginatedSales(
                items=sales,
                total_count=total_count,
                page=page,
                page_size=page_size,
                has_next=has_next,
                has_previous=has_previous,
            )

            # Cache the result
            await cache.set(query_key, result.__dict__, variables, user_context)
            logger.info(f"Optimized sales query completed: {len(sales)} items, {total_count} total")

            return result

        except Exception as e:
            logger.error(f"Error in optimized sales GraphQL query: {e}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def sales_analytics(
        self, info: Info, granularity: TimeGranularity, filters: SalesFilters | None = None
    ) -> list[SalesAnalytics]:
        """Get sales analytics with caching and optimization."""
        session = get_session_from_info(info)
        cache = get_graphql_cache()
        user_context = get_user_context(info)

        try:
            # Create cache key
            cache_key = f"analytics_{granularity.value}_{hash(str(filters))}"
            variables = {
                "granularity": granularity.value,
                "filters": filters.__dict__ if filters else None,
            }

            # Try cache first
            cached_result = await cache.get(cache_key, variables, user_context)
            if cached_result:
                logger.debug("Cache hit for sales analytics query")
                return [SalesAnalytics(**item) for item in cached_result]

            # Use service for analytics
            service = DataMartService(session)

            date_from = filters.date_from.isoformat() if filters and filters.date_from else None
            date_to = filters.date_to.isoformat() if filters and filters.date_to else None
            country = filters.country if filters else None
            product_category = filters.product_category if filters else None

            result = await service.get_sales_analytics(
                granularity=granularity.value,
                date_from=date_from,
                date_to=date_to,
                country=country,
                product_category=product_category,
            )

            analytics = [
                SalesAnalytics(
                    period=item["period"],
                    total_revenue=item["revenue"],
                    total_quantity=item["units_sold"],
                    transaction_count=item["transactions"],
                    unique_customers=item["unique_customers"],
                    avg_order_value=item["avg_order_value"],
                )
                for item in result["time_series"]
            ]

            # Cache result
            analytics_dict = [analytics_item.__dict__ for analytics_item in analytics]
            await cache.set(cache_key, analytics_dict, variables, user_context, ttl=900)

            return analytics

        except Exception as e:
            logger.error(f"Error in sales_analytics GraphQL query: {e}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def customer_segments(self, info: Info) -> list[CustomerSegment]:
        """Get customer segments with caching."""
        session = get_session_from_info(info)
        cache = get_graphql_cache()
        user_context = get_user_context(info)

        try:
            cache_key = "customer_segments"

            # Try cache first
            cached_result = await cache.get(cache_key, {}, user_context)
            if cached_result:
                logger.debug("Cache hit for customer segments query")
                return [CustomerSegment(**item) for item in cached_result]

            service = DataMartService(session)
            segments = await service.get_customer_segments()

            segments_result = [
                CustomerSegment(
                    segment_name=segment["segment"],
                    customer_count=segment["customer_count"],
                    avg_lifetime_value=segment["avg_lifetime_value"],
                    avg_order_value=segment["avg_order_value"],
                    avg_total_orders=segment["avg_total_orders"],
                )
                for segment in segments
            ]

            # Cache with longer TTL for segments
            segments_dict = [seg.__dict__ for seg in segments_result]
            await cache.set(cache_key, segments_dict, {}, user_context, ttl=3600)

            return segments_result

        except Exception as e:
            logger.error(f"Error in customer_segments GraphQL query: {e}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def product_performance(
        self, info: Info, metric: MetricType, top_n: int = 20, filters: SalesFilters | None = None
    ) -> list[ProductPerformance]:
        """Get product performance with caching."""
        session = get_session_from_info(info)
        cache = get_graphql_cache()
        user_context = get_user_context(info)

        try:
            cache_key = f"product_performance_{metric.value}_{top_n}_{hash(str(filters))}"
            variables = {
                "metric": metric.value,
                "top_n": top_n,
                "filters": filters.__dict__ if filters else None,
            }

            # Try cache first
            cached_result = await cache.get(cache_key, variables, user_context)
            if cached_result:
                logger.debug("Cache hit for product performance query")
                return [ProductPerformance(**item) for item in cached_result]

            service = DataMartService(session)

            date_from = filters.date_from.isoformat() if filters and filters.date_from else None
            date_to = filters.date_to.isoformat() if filters and filters.date_to else None

            products = await service.get_product_performance(
                top_n=top_n, metric=metric.value, date_from=date_from, date_to=date_to
            )

            performance_result = [
                ProductPerformance(
                    stock_code=product["stock_code"],
                    description=product["description"],
                    category=product["category"],
                    total_revenue=product["total_revenue"],
                    total_quantity=product["total_quantity"],
                    transaction_count=product["transaction_count"],
                )
                for product in products
            ]

            # Cache result
            performance_dict = [perf.__dict__ for perf in performance_result]
            await cache.set(cache_key, performance_dict, variables, user_context, ttl=1800)

            return performance_result

        except Exception as e:
            logger.error(f"Error in product_performance GraphQL query: {e}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def business_metrics(self, info: Info) -> BusinessMetrics:
        """Get business metrics with caching."""
        session = get_session_from_info(info)
        cache = get_graphql_cache()
        user_context = get_user_context(info)

        try:
            cache_key = "business_metrics"

            # Try cache first
            cached_result = await cache.get(cache_key, {}, user_context)
            if cached_result:
                logger.debug("Cache hit for business metrics query")
                return BusinessMetrics(**cached_result)

            service = DataMartService(session)
            metrics = await service.get_business_metrics()

            result = BusinessMetrics(
                total_revenue=metrics["revenue_metrics"]["total_revenue"],
                total_transactions=metrics["sales_metrics"]["total_transactions"],
                unique_customers=metrics["customer_metrics"]["total_customers"],
                avg_order_value=metrics["sales_metrics"]["avg_order_value"],
                total_products=metrics["product_metrics"]["total_products"],
                active_countries=metrics.get("country_metrics", {}).get("active_countries", 0),
            )

            # Cache with longer TTL for business metrics
            await cache.set(cache_key, result.__dict__, {}, user_context, ttl=600)

            return result

        except Exception as e:
            logger.error(f"Error in business_metrics GraphQL query: {e}")
            raise
        finally:
            session.close()

    @strawberry.field
    async def task_status(self, info: Info, task_id: str) -> TaskStatus | None:
        """Get async task status (no caching for real-time status)."""
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
                error=task_data.get("error"),
            )

        except Exception as e:
            logger.error(f"Error in task_status GraphQL query: {e}")
            raise


@strawberry.type
class OptimizedMutation:
    """Optimized GraphQL Mutation root."""

    @strawberry.mutation
    async def submit_task(
        self, info: Info, task_input: TaskSubmissionInput, user_id: str | None = None
    ) -> TaskStatus:
        """Submit async task with cache invalidation."""
        service = AsyncTaskService()
        cache = get_graphql_cache()

        try:
            parameters = json.loads(task_input.parameters)

            result = await service.submit_task(
                task_name=task_input.task_name, task_args=parameters, user_id=user_id
            )

            # Invalidate relevant caches based on task type
            if "sales" in task_input.task_name.lower():
                await cache.invalidate_by_tags(["sales_data"])
            elif "customer" in task_input.task_name.lower():
                await cache.invalidate_by_tags(["customer_data"])
            elif "product" in task_input.task_name.lower():
                await cache.invalidate_by_tags(["product_data"])

            return TaskStatus(
                task_id=result["task_id"],
                task_name=task_input.task_name,
                status=result["status"],
                submitted_at=datetime.fromisoformat(result["submitted_at"]),
                progress=None,
                result=None,
                error=None,
            )

        except Exception as e:
            logger.error(f"Error in submit_task GraphQL mutation: {e}")
            raise

    @strawberry.mutation
    async def cancel_task(self, info: Info, task_id: str) -> bool:
        """Cancel a running task."""
        service = AsyncTaskService()

        try:
            return await service.cancel_task(task_id)
        except Exception as e:
            logger.error(f"Error in cancel_task GraphQL mutation: {e}")
            raise


# Import asyncio for batch operations
import asyncio

# Create optimized schema
optimized_schema = strawberry.Schema(query=OptimizedQuery, mutation=OptimizedMutation)
