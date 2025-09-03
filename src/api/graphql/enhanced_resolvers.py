"""
Enhanced GraphQL Resolvers
Advanced GraphQL resolvers with DataLoader pattern, caching, and subscription support.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import date, datetime, timedelta
from typing import Any

import strawberry
from strawberry.dataloader import DataLoader
from strawberry.extensions import QueryDepthLimiter, ValidationCache
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
from api.services.microservices_orchestrator import get_microservices_orchestrator
from core.caching.cache_patterns import CacheAsidePattern
from core.logging import get_logger
from data_access.repositories.sales_repository import SalesRepository

logger = get_logger(__name__)


class GraphQLDataLoaders:
    """
    DataLoader implementations for efficient N+1 query resolution.
    Batches and caches database queries for optimal performance.
    """

    def __init__(self, sales_repository: SalesRepository):
        self.sales_repository = sales_repository
        self.cache = CacheAsidePattern(default_ttl=300)  # 5 minutes cache

    async def load_customers_batch(self, customer_keys: list[int]) -> list[Customer | None]:
        """Batch load customers by keys."""
        try:
            # Query database for all requested customer keys
            customers_data = await self.sales_repository.get_customers_by_keys(customer_keys)
            customers_map = {c["customer_key"]: c for c in customers_data}

            # Return customers in the same order as requested keys
            return [
                Customer(
                    customer_key=customers_map[key]["customer_key"],
                    customer_id=customers_map[key].get("customer_id"),
                    customer_segment=customers_map[key].get("customer_segment"),
                    lifetime_value=customers_map[key].get("lifetime_value"),
                    total_orders=customers_map[key].get("total_orders"),
                    total_spent=customers_map[key].get("total_spent"),
                    avg_order_value=customers_map[key].get("avg_order_value"),
                    recency_score=customers_map[key].get("recency_score"),
                    frequency_score=customers_map[key].get("frequency_score"),
                    monetary_score=customers_map[key].get("monetary_score"),
                    rfm_segment=customers_map[key].get("rfm_segment"),
                )
                if key in customers_map
                else None
                for key in customer_keys
            ]
        except Exception as e:
            logger.error(f"Error loading customers batch: {e}")
            return [None] * len(customer_keys)

    async def load_products_batch(self, product_keys: list[int]) -> list[Product | None]:
        """Batch load products by keys."""
        try:
            products_data = await self.sales_repository.get_products_by_keys(product_keys)
            products_map = {p["product_key"]: p for p in products_data}

            return [
                Product(
                    product_key=products_map[key]["product_key"],
                    stock_code=products_map[key]["stock_code"],
                    description=products_map[key].get("description"),
                    category=products_map[key].get("category"),
                    subcategory=products_map[key].get("subcategory"),
                    brand=products_map[key].get("brand"),
                    unit_cost=products_map[key].get("unit_cost"),
                    recommended_price=products_map[key].get("recommended_price"),
                )
                if key in products_map
                else None
                for key in product_keys
            ]
        except Exception as e:
            logger.error(f"Error loading products batch: {e}")
            return [None] * len(product_keys)

    async def load_countries_batch(self, country_keys: list[int]) -> list[Country | None]:
        """Batch load countries by keys."""
        try:
            countries_data = await self.sales_repository.get_countries_by_keys(country_keys)
            countries_map = {c["country_key"]: c for c in countries_data}

            return [
                Country(
                    country_key=countries_map[key]["country_key"],
                    country_code=countries_map[key]["country_code"],
                    country_name=countries_map[key]["country_name"],
                    region=countries_map[key].get("region"),
                    continent=countries_map[key].get("continent"),
                    currency_code=countries_map[key].get("currency_code"),
                    gdp_per_capita=countries_map[key].get("gdp_per_capita"),
                    population=countries_map[key].get("population"),
                )
                if key in countries_map
                else None
                for key in country_keys
            ]
        except Exception as e:
            logger.error(f"Error loading countries batch: {e}")
            return [None] * len(country_keys)

    async def load_sales_by_customer_batch(self, customer_keys: list[int]) -> list[list[Sale]]:
        """Batch load sales by customer keys."""
        try:
            sales_data = await self.sales_repository.get_sales_by_customer_keys(customer_keys)
            sales_by_customer = defaultdict(list)

            for sale_data in sales_data:
                customer_key = sale_data["customer_key"]
                sale = Sale(
                    sale_id=sale_data["sale_id"],
                    quantity=sale_data["quantity"],
                    unit_price=sale_data["unit_price"],
                    total_amount=sale_data["total_amount"],
                    discount_amount=sale_data.get("discount_amount", 0),
                    tax_amount=sale_data.get("tax_amount", 0),
                    profit_amount=sale_data.get("profit_amount"),
                    margin_percentage=sale_data.get("margin_percentage"),
                    created_at=sale_data["created_at"],
                    customer=None,  # Will be loaded separately
                    product=None,  # Will be loaded separately
                    country=None,  # Will be loaded separately
                )
                sales_by_customer[customer_key].append(sale)

            return [sales_by_customer[key] for key in customer_keys]

        except Exception as e:
            logger.error(f"Error loading sales by customer batch: {e}")
            return [[] for _ in customer_keys]


class EnhancedGraphQLContext:
    """Enhanced GraphQL context with data loaders and services."""

    def __init__(self, sales_repository: SalesRepository):
        self.sales_repository = sales_repository
        self.data_loaders = GraphQLDataLoaders(sales_repository)
        self.orchestrator = get_microservices_orchestrator()

        # Create DataLoader instances
        self.customer_loader = DataLoader(
            load_fn=self.data_loaders.load_customers_batch, max_batch_size=100
        )
        self.product_loader = DataLoader(
            load_fn=self.data_loaders.load_products_batch, max_batch_size=100
        )
        self.country_loader = DataLoader(
            load_fn=self.data_loaders.load_countries_batch, max_batch_size=100
        )
        self.sales_by_customer_loader = DataLoader(
            load_fn=self.data_loaders.load_sales_by_customer_batch, max_batch_size=50
        )

        # Performance monitoring
        self.query_start_time = datetime.utcnow()
        self.query_count = 0
        self.cache_hits = 0


# Subscription types for real-time data
@strawberry.type
class SalesUpdate:
    """Real-time sales update."""

    sale: Sale
    event_type: str  # 'new_sale', 'updated_sale'
    timestamp: datetime


@strawberry.type
class MetricsUpdate:
    """Real-time metrics update."""

    metrics: BusinessMetrics
    timestamp: datetime


class EnhancedResolver:
    """Enhanced GraphQL resolver with advanced features."""

    def __init__(self):
        self.cache = CacheAsidePattern(default_ttl=600)  # 10 minutes cache
        self.subscription_manager = SubscriptionManager()

    # Query resolvers with caching and optimization

    async def get_sales(
        self,
        info: Info,
        filters: SalesFilters | None = None,
        pagination: PaginationInput | None = None,
    ) -> PaginatedSales:
        """Get paginated sales with advanced filtering."""
        context: EnhancedGraphQLContext = info.context
        context.query_count += 1

        try:
            # Build cache key
            cache_key = f"sales:{hash(str(filters))}:{hash(str(pagination))}"

            async def load_sales_data():
                # Convert filters to repository format
                filter_dict = self._convert_filters_to_dict(filters) if filters else {}
                page = pagination.page if pagination else 1
                page_size = min(
                    pagination.page_size if pagination else 20, 100
                )  # Limit max page size

                # Get total count and paginated results
                total_count = await context.sales_repository.count_sales(filter_dict)
                sales_data = await context.sales_repository.get_sales_paginated(
                    filter_dict, page, page_size
                )

                # Convert to GraphQL types with data loaders
                sales = []
                for sale_data in sales_data:
                    # Use data loaders for efficient related data loading
                    customer = (
                        await context.customer_loader.load(sale_data["customer_key"])
                        if sale_data.get("customer_key")
                        else None
                    )
                    product = await context.product_loader.load(sale_data["product_key"])
                    country = await context.country_loader.load(sale_data["country_key"])

                    sale = Sale(
                        sale_id=sale_data["sale_id"],
                        quantity=sale_data["quantity"],
                        unit_price=sale_data["unit_price"],
                        total_amount=sale_data["total_amount"],
                        discount_amount=sale_data.get("discount_amount", 0),
                        tax_amount=sale_data.get("tax_amount", 0),
                        profit_amount=sale_data.get("profit_amount"),
                        margin_percentage=sale_data.get("margin_percentage"),
                        created_at=sale_data["created_at"],
                        customer=customer,
                        product=product,
                        country=country,
                    )
                    sales.append(sale)

                return PaginatedSales(
                    items=sales,
                    total_count=total_count,
                    page=page,
                    page_size=page_size,
                    has_next=page * page_size < total_count,
                    has_previous=page > 1,
                )

            # Use cache for expensive queries
            result = await self.cache.get(cache_key, load_sales_data)

            return result

        except Exception as e:
            logger.error(f"Error in get_sales resolver: {e}")
            raise

    async def get_sales_analytics(
        self,
        info: Info,
        granularity: TimeGranularity,
        date_from: date | None = None,
        date_to: date | None = None,
        filters: SalesFilters | None = None,
    ) -> list[SalesAnalytics]:
        """Get sales analytics with time-based aggregation."""
        context: EnhancedGraphQLContext = info.context
        context.query_count += 1

        try:
            # Build cache key
            cache_key = f"analytics:{granularity.value}:{date_from}:{date_to}:{hash(str(filters))}"

            async def load_analytics_data():
                # Set default date range if not provided
                if not date_from:
                    date_from_calc = date.today() - timedelta(days=90)
                else:
                    date_from_calc = date_from

                if not date_to:
                    date_to_calc = date.today()
                else:
                    date_to_calc = date_to

                # Convert filters
                filter_dict = self._convert_filters_to_dict(filters) if filters else {}
                filter_dict["date_from"] = date_from_calc
                filter_dict["date_to"] = date_to_calc

                # Get analytics data from repository
                analytics_data = await context.sales_repository.get_sales_analytics(
                    granularity.value, filter_dict
                )

                return [
                    SalesAnalytics(
                        period=item["period"],
                        total_revenue=item["total_revenue"],
                        total_quantity=item["total_quantity"],
                        transaction_count=item["transaction_count"],
                        unique_customers=item["unique_customers"],
                        avg_order_value=item["avg_order_value"],
                    )
                    for item in analytics_data
                ]

            result = await self.cache.get(cache_key, load_analytics_data)
            return result

        except Exception as e:
            logger.error(f"Error in get_sales_analytics resolver: {e}")
            raise

    async def get_customer_segments(
        self, info: Info, limit: int | None = None
    ) -> list[CustomerSegment]:
        """Get customer segment analytics."""
        context: EnhancedGraphQLContext = info.context
        context.query_count += 1

        try:
            cache_key = f"customer_segments:{limit}"

            async def load_segments_data():
                segments_data = await context.sales_repository.get_customer_segments(limit or 20)

                return [
                    CustomerSegment(
                        segment_name=item["segment_name"],
                        customer_count=item["customer_count"],
                        avg_lifetime_value=item["avg_lifetime_value"],
                        avg_order_value=item["avg_order_value"],
                        avg_total_orders=item["avg_total_orders"],
                    )
                    for item in segments_data
                ]

            result = await self.cache.get(cache_key, load_segments_data)
            return result

        except Exception as e:
            logger.error(f"Error in get_customer_segments resolver: {e}")
            raise

    async def get_product_performance(
        self,
        info: Info,
        metric: MetricType,
        limit: int | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[ProductPerformance]:
        """Get top performing products by metric."""
        context: EnhancedGraphQLContext = info.context
        context.query_count += 1

        try:
            cache_key = f"product_performance:{metric.value}:{limit}:{date_from}:{date_to}"

            async def load_performance_data():
                performance_data = await context.sales_repository.get_product_performance(
                    metric.value, limit or 20, date_from, date_to
                )

                return [
                    ProductPerformance(
                        stock_code=item["stock_code"],
                        description=item.get("description"),
                        category=item.get("category"),
                        total_revenue=item["total_revenue"],
                        total_quantity=item["total_quantity"],
                        transaction_count=item["transaction_count"],
                    )
                    for item in performance_data
                ]

            result = await self.cache.get(cache_key, load_performance_data)
            return result

        except Exception as e:
            logger.error(f"Error in get_product_performance resolver: {e}")
            raise

    async def get_business_metrics(
        self, info: Info, date_from: date | None = None, date_to: date | None = None
    ) -> BusinessMetrics:
        """Get high-level business metrics."""
        context: EnhancedGraphQLContext = info.context
        context.query_count += 1

        try:
            cache_key = f"business_metrics:{date_from}:{date_to}"

            async def load_metrics_data():
                metrics_data = await context.sales_repository.get_business_metrics(
                    date_from, date_to
                )

                return BusinessMetrics(
                    total_revenue=metrics_data["total_revenue"],
                    total_transactions=metrics_data["total_transactions"],
                    unique_customers=metrics_data["unique_customers"],
                    avg_order_value=metrics_data["avg_order_value"],
                    total_products=metrics_data["total_products"],
                    active_countries=metrics_data["active_countries"],
                )

            result = await self.cache.get(cache_key, load_metrics_data)
            return result

        except Exception as e:
            logger.error(f"Error in get_business_metrics resolver: {e}")
            raise

    # Advanced queries with microservices integration

    async def get_ml_predictions(
        self, info: Info, prediction_type: str, parameters: str | None = None
    ) -> dict[str, Any]:
        """Get ML predictions from analytics service."""
        context: EnhancedGraphQLContext = info.context

        try:
            # Get analytics service instance
            analytics_instance = context.orchestrator.get_service_instance("analytics-service")

            if not analytics_instance:
                raise Exception("Analytics service not available")

            # Make request to analytics service
            import httpx

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{analytics_instance.base_url}/ml/predict",
                    json={
                        "prediction_type": prediction_type,
                        "parameters": json.loads(parameters) if parameters else {},
                    },
                    timeout=30.0,
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    raise Exception(f"ML prediction failed: {response.text}")

        except Exception as e:
            logger.error(f"Error in get_ml_predictions resolver: {e}")
            raise

    # Mutation resolvers

    async def submit_analysis_task(self, info: Info, task_input: TaskSubmissionInput) -> TaskStatus:
        """Submit asynchronous analysis task."""
        context: EnhancedGraphQLContext = info.context

        try:
            # Submit task through orchestrator
            task_result = await context.orchestrator.handle_command(
                {
                    "type": "submit_task",
                    "task_name": task_input.task_name,
                    "parameters": json.loads(task_input.parameters),
                }
            )

            if task_result["success"]:
                return TaskStatus(
                    task_id=task_result["task_id"],
                    task_name=task_input.task_name,
                    status="pending",
                    submitted_at=datetime.utcnow(),
                    progress=None,
                    result=None,
                    error=None,
                )
            else:
                raise Exception(task_result.get("error", "Task submission failed"))

        except Exception as e:
            logger.error(f"Error in submit_analysis_task resolver: {e}")
            raise

    async def get_task_status(self, info: Info, task_id: str) -> TaskStatus | None:
        """Get status of asynchronous task."""
        context: EnhancedGraphQLContext = info.context

        try:
            # Query task status through orchestrator
            task_result = await context.orchestrator.handle_query(
                {"type": "get_task_status", "task_id": task_id}
            )

            if task_result["success"] and task_result.get("task"):
                task_data = task_result["task"]
                return TaskStatus(
                    task_id=task_data["task_id"],
                    task_name=task_data["task_name"],
                    status=task_data["status"],
                    submitted_at=datetime.fromisoformat(task_data["submitted_at"]),
                    progress=task_data.get("progress"),
                    result=json.dumps(task_data["result"]) if task_data.get("result") else None,
                    error=task_data.get("error"),
                )
            else:
                return None

        except Exception as e:
            logger.error(f"Error in get_task_status resolver: {e}")
            return None

    # Subscription resolvers for real-time data

    async def subscribe_to_sales_updates(self, info: Info) -> AsyncIterator[SalesUpdate]:
        """Subscribe to real-time sales updates."""
        try:
            async for update in self.subscription_manager.subscribe_sales_updates():
                yield update
        except Exception as e:
            logger.error(f"Error in sales subscription: {e}")

    async def subscribe_to_metrics_updates(self, info: Info) -> AsyncIterator[MetricsUpdate]:
        """Subscribe to real-time metrics updates."""
        try:
            async for update in self.subscription_manager.subscribe_metrics_updates():
                yield update
        except Exception as e:
            logger.error(f"Error in metrics subscription: {e}")

    # Helper methods

    def _convert_filters_to_dict(self, filters: SalesFilters) -> dict[str, Any]:
        """Convert GraphQL filters to repository format."""
        filter_dict = {}

        if filters.date_from:
            filter_dict["date_from"] = filters.date_from
        if filters.date_to:
            filter_dict["date_to"] = filters.date_to
        if filters.country:
            filter_dict["country"] = filters.country
        if filters.product_category:
            filter_dict["product_category"] = filters.product_category
        if filters.customer_segment:
            filter_dict["customer_segment"] = filters.customer_segment
        if filters.min_amount:
            filter_dict["min_amount"] = filters.min_amount
        if filters.max_amount:
            filter_dict["max_amount"] = filters.max_amount

        return filter_dict


class SubscriptionManager:
    """Manages GraphQL subscriptions for real-time data."""

    def __init__(self):
        self.sales_subscribers = set()
        self.metrics_subscribers = set()

    async def subscribe_sales_updates(self) -> AsyncIterator[SalesUpdate]:
        """Subscribe to sales updates."""
        queue = asyncio.Queue()
        self.sales_subscribers.add(queue)

        try:
            while True:
                update = await queue.get()
                yield update
        finally:
            self.sales_subscribers.discard(queue)

    async def subscribe_metrics_updates(self) -> AsyncIterator[MetricsUpdate]:
        """Subscribe to metrics updates."""
        queue = asyncio.Queue()
        self.metrics_subscribers.add(queue)

        try:
            while True:
                update = await queue.get()
                yield update
        finally:
            self.metrics_subscribers.discard(queue)

    async def publish_sales_update(self, update: SalesUpdate):
        """Publish sales update to all subscribers."""
        for queue in self.sales_subscribers:
            try:
                queue.put_nowait(update)
            except asyncio.QueueFull:
                pass  # Skip if queue is full

    async def publish_metrics_update(self, update: MetricsUpdate):
        """Publish metrics update to all subscribers."""
        for queue in self.metrics_subscribers:
            try:
                queue.put_nowait(update)
            except asyncio.QueueFull:
                pass  # Skip if queue is full


# Global resolver instance
_resolver = EnhancedResolver()


def get_enhanced_resolver() -> EnhancedResolver:
    """Get the enhanced GraphQL resolver."""
    return _resolver


# GraphQL query, mutation, and subscription definitions with the resolver


@strawberry.type
class Query:
    """GraphQL Query root with enhanced resolvers."""

    @strawberry.field
    async def sales(
        self,
        info: Info,
        filters: SalesFilters | None = None,
        pagination: PaginationInput | None = None,
    ) -> PaginatedSales:
        return await _resolver.get_sales(info, filters, pagination)

    @strawberry.field
    async def sales_analytics(
        self,
        info: Info,
        granularity: TimeGranularity,
        date_from: date | None = None,
        date_to: date | None = None,
        filters: SalesFilters | None = None,
    ) -> list[SalesAnalytics]:
        return await _resolver.get_sales_analytics(info, granularity, date_from, date_to, filters)

    @strawberry.field
    async def customer_segments(
        self, info: Info, limit: int | None = None
    ) -> list[CustomerSegment]:
        return await _resolver.get_customer_segments(info, limit)

    @strawberry.field
    async def product_performance(
        self,
        info: Info,
        metric: MetricType,
        limit: int | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[ProductPerformance]:
        return await _resolver.get_product_performance(info, metric, limit, date_from, date_to)

    @strawberry.field
    async def business_metrics(
        self, info: Info, date_from: date | None = None, date_to: date | None = None
    ) -> BusinessMetrics:
        return await _resolver.get_business_metrics(info, date_from, date_to)

    @strawberry.field
    async def ml_predictions(
        self, info: Info, prediction_type: str, parameters: str | None = None
    ) -> strawberry.scalars.JSON:
        return await _resolver.get_ml_predictions(info, prediction_type, parameters)

    @strawberry.field
    async def task_status(self, info: Info, task_id: str) -> TaskStatus | None:
        return await _resolver.get_task_status(info, task_id)


@strawberry.type
class Mutation:
    """GraphQL Mutation root with enhanced resolvers."""

    @strawberry.mutation
    async def submit_analysis_task(self, info: Info, task_input: TaskSubmissionInput) -> TaskStatus:
        return await _resolver.submit_analysis_task(info, task_input)


@strawberry.type
class Subscription:
    """GraphQL Subscription root for real-time data."""

    @strawberry.subscription
    async def sales_updates(self, info: Info) -> AsyncIterator[SalesUpdate]:
        async for update in _resolver.subscribe_to_sales_updates(info):
            yield update

    @strawberry.subscription
    async def metrics_updates(self, info: Info) -> AsyncIterator[MetricsUpdate]:
        async for update in _resolver.subscribe_to_metrics_updates(info):
            yield update


# Federation support for microservices
@strawberry.federation.type(keys=["customer_key"])
class FederatedCustomer:
    """Federated customer entity for microservices architecture"""

    customer_key: int = strawberry.federation.field(external=True)
    customer_id: str | None = None
    customer_segment: str | None = None
    lifetime_value: float | None = None

    @classmethod
    def resolve_reference(cls, info: Info, customer_key: int):
        """Resolve customer entity from reference"""
        context: EnhancedGraphQLContext = info.context
        return context.customer_loader.load(customer_key)


@strawberry.federation.type(keys=["product_key"])
class FederatedProduct:
    """Federated product entity for microservices architecture"""

    product_key: int = strawberry.federation.field(external=True)
    stock_code: str
    description: str | None = None
    category: str | None = None

    @classmethod
    def resolve_reference(cls, info: Info, product_key: int):
        """Resolve product entity from reference"""
        context: EnhancedGraphQLContext = info.context
        return context.product_loader.load(product_key)


# Federation query extensions
@strawberry.type
class FederationQuery:
    """Federation-specific query extensions"""

    @strawberry.field
    def _entities(self, info, representations: list[dict[str, Any]]) -> list[Any]:
        """Resolve federated entities"""
        entities = []
        for representation in representations:
            typename = representation.get("__typename")

            if typename == "FederatedCustomer":
                entities.append(
                    FederatedCustomer.resolve_reference(info, representation["customer_key"])
                )
            elif typename == "FederatedProduct":
                entities.append(
                    FederatedProduct.resolve_reference(info, representation["product_key"])
                )

        return entities

    @strawberry.field
    def _service(self) -> dict[str, str]:
        """Return federation schema SDL"""
        return {"sdl": enhanced_schema.as_str()}


# Performance monitoring extension
class PerformanceMonitoringExtension:
    """Extension for monitoring GraphQL query performance"""

    def __init__(self):
        self.query_metrics = {}

    async def on_request_start(self):
        self.start_time = datetime.utcnow()

    async def on_request_end(self):
        end_time = datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        # Log slow queries
        if duration > 2.0:  # Log queries taking more than 2 seconds
            logger.warning(
                f"Slow GraphQL query detected: {duration:.3f}s", extra={"execution_time": duration}
            )


# Create the enhanced GraphQL schema with federation support
enhanced_schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
    extensions=[
        QueryDepthLimiter(max_depth=15),  # Prevent overly complex queries
        ValidationCache(),  # Cache query validation
        PerformanceMonitoringExtension(),  # Performance monitoring
    ],
    federation=strawberry.federation.Schema(enable_federation_2=True),
)


# Federated schema for microservices deployment
federated_schema = strawberry.federation.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
    types=[FederatedCustomer, FederatedProduct],
    extensions=[
        QueryDepthLimiter(max_depth=15),
        ValidationCache(),
        PerformanceMonitoringExtension(),
    ],
    enable_federation_2=True,
)


# Production-ready GraphQL application configuration
class ProductionGraphQLConfig:
    """Production configuration for GraphQL deployment"""

    def __init__(self):
        self.config = {
            "introspection": False,  # Disable introspection in production
            "playground": False,  # Disable GraphQL playground
            "debug": False,  # Disable debug mode
            "max_query_depth": 15,  # Limit query complexity
            "max_query_complexity": 1000,  # Limit query complexity score
            "query_cache_size": 1000,  # Query validation cache size
            "enable_metrics": True,  # Enable performance metrics
            "enable_tracing": True,  # Enable query tracing
            "timeout_seconds": 30,  # Query timeout
            "max_aliases": 15,  # Prevent alias-based DoS attacks
            "max_directives": 50,  # Limit directive usage
        }

    def get_security_rules(self) -> list[Any]:
        """Get GraphQL security validation rules"""
        from graphql import ValidationRule

        class QueryComplexityRule(ValidationRule):
            """Custom validation rule for query complexity"""

            def __init__(self, max_complexity: int = 1000):
                self.max_complexity = max_complexity
                super().__init__()

            def enter_field(self, node, *_):
                # Simplified complexity calculation
                # In production, use a proper GraphQL complexity analysis library
                pass

        return [QueryComplexityRule(self.config["max_query_complexity"])]

    def get_schema_for_environment(self, environment: str = "production"):
        """Get schema configuration for specific environment"""
        if environment == "production":
            return strawberry.Schema(
                query=Query,
                mutation=Mutation,
                subscription=Subscription,
                extensions=[
                    QueryDepthLimiter(max_depth=self.config["max_query_depth"]),
                    ValidationCache(maxsize=self.config["query_cache_size"]),
                    PerformanceMonitoringExtension(),
                ],
            )
        elif environment == "federation":
            return federated_schema
        else:
            return enhanced_schema


# Global configuration instance
production_config = ProductionGraphQLConfig()
