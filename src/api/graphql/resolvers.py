"""
GraphQL Resolvers
Business logic for GraphQL queries and mutations.
"""
import json
from datetime import datetime, date
from typing import List, Optional

import strawberry
from strawberry.types import Info
from sqlmodel import Session, select, func, and_, desc

from api.graphql.schemas import (
    Sale, Customer, Product, Country, SalesAnalytics, CustomerSegment,
    ProductPerformance, BusinessMetrics, TaskStatus, PaginatedSales,
    SalesFilters, PaginationInput, TaskSubmissionInput, TimeGranularity, MetricType
)
from api.v1.services.datamart_service import DataMartService
from api.v1.services.async_tasks import AsyncTaskService
from data_access.db import get_session
from data_access.models.star_schema import (
    FactSale, DimProduct, DimCustomer, DimDate, DimCountry, DimInvoice
)
from core.logging import get_logger

logger = get_logger(__name__)


def get_session_from_info(info: Info) -> Session:
    """Get database session from GraphQL info context."""
    return next(get_session())


@strawberry.type
class Query:
    """GraphQL Query root."""
    
    @strawberry.field
    async def sales(
        self,
        info: Info,
        filters: Optional[SalesFilters] = None,
        pagination: Optional[PaginationInput] = None
    ) -> PaginatedSales:
        """Get paginated sales data with optional filters."""
        session = get_session_from_info(info)
        
        try:
            # Build base query
            query = select(
                FactSale.sale_id,
                FactSale.quantity,
                FactSale.unit_price,
                FactSale.total_amount,
                FactSale.discount_amount,
                FactSale.tax_amount,
                FactSale.profit_amount,
                FactSale.margin_percentage,
                FactSale.created_at,
                # Product fields
                DimProduct.stock_code,
                DimProduct.description,
                DimProduct.category,
                DimProduct.brand,
                # Customer fields
                DimCustomer.customer_id,
                DimCustomer.customer_segment,
                DimCustomer.lifetime_value,
                # Country fields
                DimCountry.country_name,
                DimCountry.country_code,
                DimCountry.region
            ).select_from(FactSale)\
             .join(DimProduct, FactSale.product_key == DimProduct.product_key)\
             .join(DimCountry, FactSale.country_key == DimCountry.country_key)\
             .outerjoin(DimCustomer, FactSale.customer_key == DimCustomer.customer_key)
            
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
            
            # Convert to GraphQL types
            sales = []
            for row in results:
                # Create customer object if exists
                customer = None
                if row.customer_id:
                    customer = Customer(
                        customer_key=0,  # Would need to get from join
                        customer_id=row.customer_id,
                        customer_segment=row.customer_segment,
                        lifetime_value=float(row.lifetime_value) if row.lifetime_value else None,
                        total_orders=None,
                        total_spent=None,
                        avg_order_value=None,
                        recency_score=None,
                        frequency_score=None,
                        monetary_score=None,
                        rfm_segment=None
                    )
                
                # Create product object
                product = Product(
                    product_key=0,  # Would need to get from join
                    stock_code=row.stock_code,
                    description=row.description,
                    category=row.category,
                    subcategory=None,
                    brand=row.brand,
                    unit_cost=None,
                    recommended_price=None
                )
                
                # Create country object
                country = Country(
                    country_key=0,  # Would need to get from join
                    country_code=row.country_code,
                    country_name=row.country_name,
                    region=row.region,
                    continent=None,
                    currency_code=None,
                    gdp_per_capita=None,
                    population=None
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
    
    @strawberry.field
    async def sales_analytics(
        self,
        info: Info,
        granularity: TimeGranularity,
        filters: Optional[SalesFilters] = None
    ) -> List[SalesAnalytics]:
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
    async def customer_segments(self, info: Info) -> List[CustomerSegment]:
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
        filters: Optional[SalesFilters] = None
    ) -> List[ProductPerformance]:
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
    async def task_status(self, info: Info, task_id: str) -> Optional[TaskStatus]:
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
        user_id: Optional[str] = None
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