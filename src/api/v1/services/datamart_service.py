"""
Data Mart Service
Business logic for accessing and analyzing star schema data.
"""
import asyncio
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from decimal import Decimal

from sqlmodel import Session, select, func, and_, or_, desc, asc
from sqlalchemy import text

from data_access.models.star_schema import (
    FactSale, DimProduct, DimCustomer, DimDate, DimCountry, DimInvoice
)
from core.logging import get_logger

logger = get_logger(__name__)


class DataMartService:
    """Service for accessing data mart analytics and business intelligence."""
    
    def __init__(self, session: Session):
        self.session = session
    
    async def get_dashboard_overview(
        self, 
        date_from: Optional[str] = None, 
        date_to: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get high-level dashboard KPIs."""
        try:
            # Build date filter
            date_filter = self._build_date_filter(date_from, date_to)
            
            # Get basic metrics
            total_sales_query = select(
                func.sum(FactSale.total_amount).label("total_revenue"),
                func.count(FactSale.sale_id).label("total_transactions"),
                func.count(func.distinct(FactSale.customer_key)).label("unique_customers"),
                func.avg(FactSale.total_amount).label("avg_order_value")
            ).select_from(FactSale)
            
            if date_filter is not None:
                total_sales_query = total_sales_query.join(DimDate).where(date_filter)
            
            result = self.session.exec(total_sales_query).first()
            
            # Get period comparison (previous period)
            prev_period_query = self._get_previous_period_metrics(date_from, date_to)
            prev_result = self.session.exec(prev_period_query).first() if prev_period_query else None
            
            # Calculate growth rates
            growth_metrics = self._calculate_growth_metrics(result, prev_result)
            
            # Get top selling products
            top_products = await self._get_top_products(limit=5, date_filter=date_filter)
            
            # Get top countries
            top_countries = await self._get_top_countries(limit=5, date_filter=date_filter)
            
            return {
                "period": {
                    "date_from": date_from,
                    "date_to": date_to or datetime.now().strftime("%Y-%m-%d")
                },
                "metrics": {
                    "total_revenue": float(result.total_revenue or 0),
                    "total_transactions": int(result.total_transactions or 0),
                    "unique_customers": int(result.unique_customers or 0),
                    "avg_order_value": float(result.avg_order_value or 0)
                },
                "growth": growth_metrics,
                "top_products": top_products,
                "top_countries": top_countries,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in get_dashboard_overview: {str(e)}")
            raise
    
    async def get_sales_analytics(
        self,
        granularity: str = "monthly",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        country: Optional[str] = None,
        product_category: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get detailed sales analytics with time series data."""
        try:
            # Build filters
            filters = []
            date_filter = self._build_date_filter(date_from, date_to)
            if date_filter is not None:
                filters.append(date_filter)
            
            if country:
                filters.append(DimCountry.country_name == country)
            
            if product_category:
                filters.append(DimProduct.category == product_category)
            
            # Build time series grouping
            time_group = self._get_time_grouping(granularity)
            
            # Main sales analytics query
            query = select(
                time_group.label("period"),
                func.sum(FactSale.total_amount).label("revenue"),
                func.sum(FactSale.quantity).label("units_sold"),
                func.count(FactSale.sale_id).label("transactions"),
                func.count(func.distinct(FactSale.customer_key)).label("unique_customers"),
                func.avg(FactSale.total_amount).label("avg_order_value"),
                func.sum(FactSale.profit_amount).label("profit")
            ).select_from(FactSale)\
             .join(DimDate, FactSale.date_key == DimDate.date_key)
            
            if country or product_category:
                if country:
                    query = query.join(DimCountry, FactSale.country_key == DimCountry.country_key)
                if product_category:
                    query = query.join(DimProduct, FactSale.product_key == DimProduct.product_key)
            
            if filters:
                query = query.where(and_(*filters))
            
            query = query.group_by(time_group).order_by(time_group)
            
            results = self.session.exec(query).all()
            
            # Format time series data
            time_series = [
                {
                    "period": str(row.period),
                    "revenue": float(row.revenue or 0),
                    "units_sold": int(row.units_sold or 0),
                    "transactions": int(row.transactions or 0),
                    "unique_customers": int(row.unique_customers or 0),
                    "avg_order_value": float(row.avg_order_value or 0),
                    "profit": float(row.profit or 0)
                }
                for row in results
            ]
            
            # Calculate summary statistics
            summary = self._calculate_time_series_summary(time_series)
            
            return {
                "granularity": granularity,
                "filters": {
                    "date_from": date_from,
                    "date_to": date_to,
                    "country": country,
                    "product_category": product_category
                },
                "time_series": time_series,
                "summary": summary,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in get_sales_analytics: {str(e)}")
            raise
    
    async def get_customer_segments(self) -> List[Dict[str, Any]]:
        """Get customer segmentation based on RFM analysis."""
        try:
            query = select(
                DimCustomer.rfm_segment,
                func.count(DimCustomer.customer_key).label("customer_count"),
                func.avg(DimCustomer.lifetime_value).label("avg_lifetime_value"),
                func.avg(DimCustomer.avg_order_value).label("avg_order_value"),
                func.avg(DimCustomer.total_orders).label("avg_total_orders"),
                func.avg(DimCustomer.recency_score).label("avg_recency"),
                func.avg(DimCustomer.frequency_score).label("avg_frequency"),
                func.avg(DimCustomer.monetary_score).label("avg_monetary")
            ).where(
                and_(
                    DimCustomer.is_current == True,
                    DimCustomer.rfm_segment.is_not(None)
                )
            ).group_by(DimCustomer.rfm_segment).order_by(desc("avg_lifetime_value"))
            
            results = self.session.exec(query).all()
            
            return [
                {
                    "segment": row.rfm_segment,
                    "customer_count": int(row.customer_count),
                    "avg_lifetime_value": float(row.avg_lifetime_value or 0),
                    "avg_order_value": float(row.avg_order_value or 0),
                    "avg_total_orders": float(row.avg_total_orders or 0),
                    "avg_recency_score": float(row.avg_recency or 0),
                    "avg_frequency_score": float(row.avg_frequency or 0),
                    "avg_monetary_score": float(row.avg_monetary or 0)
                }
                for row in results
            ]
            
        except Exception as e:
            logger.error(f"Error in get_customer_segments: {str(e)}")
            raise
    
    async def get_customer_analytics(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed analytics for a specific customer."""
        try:
            # Get customer details
            customer_query = select(DimCustomer).where(
                and_(
                    DimCustomer.customer_id == customer_id,
                    DimCustomer.is_current == True
                )
            )
            customer = self.session.exec(customer_query).first()
            
            if not customer:
                return None
            
            # Get customer's purchase history
            purchase_history_query = select(
                DimDate.date,
                FactSale.total_amount,
                FactSale.quantity,
                DimProduct.description,
                DimProduct.category
            ).select_from(FactSale)\
             .join(DimDate, FactSale.date_key == DimDate.date_key)\
             .join(DimProduct, FactSale.product_key == DimProduct.product_key)\
             .where(FactSale.customer_key == customer.customer_key)\
             .order_by(desc(DimDate.date))\
             .limit(20)
            
            purchases = self.session.exec(purchase_history_query).all()
            
            # Get favorite categories
            category_query = select(
                DimProduct.category,
                func.sum(FactSale.total_amount).label("total_spent"),
                func.count(FactSale.sale_id).label("purchases")
            ).select_from(FactSale)\
             .join(DimProduct, FactSale.product_key == DimProduct.product_key)\
             .where(FactSale.customer_key == customer.customer_key)\
             .group_by(DimProduct.category)\
             .order_by(desc("total_spent"))
            
            categories = self.session.exec(category_query).all()
            
            return {
                "customer_id": customer.customer_id,
                "customer_key": customer.customer_key,
                "segment": customer.customer_segment,
                "rfm_segment": customer.rfm_segment,
                "metrics": {
                    "lifetime_value": float(customer.lifetime_value or 0),
                    "total_orders": customer.total_orders,
                    "total_spent": float(customer.total_spent or 0),
                    "avg_order_value": float(customer.avg_order_value or 0),
                    "recency_score": customer.recency_score,
                    "frequency_score": customer.frequency_score,
                    "monetary_score": customer.monetary_score
                },
                "recent_purchases": [
                    {
                        "date": row.date.isoformat(),
                        "amount": float(row.total_amount),
                        "quantity": row.quantity,
                        "product": row.description,
                        "category": row.category
                    }
                    for row in purchases
                ],
                "favorite_categories": [
                    {
                        "category": row.category,
                        "total_spent": float(row.total_spent),
                        "purchase_count": int(row.purchases)
                    }
                    for row in categories
                ]
            }
            
        except Exception as e:
            logger.error(f"Error in get_customer_analytics: {str(e)}")
            raise
    
    async def get_product_performance(
        self,
        top_n: int = 20,
        metric: str = "revenue",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get top-performing products by specified metric."""
        try:
            # Define metric column
            metric_columns = {
                "revenue": func.sum(FactSale.total_amount),
                "quantity": func.sum(FactSale.quantity),
                "profit": func.sum(FactSale.profit_amount),
                "margin": func.avg(FactSale.margin_percentage)
            }
            
            metric_col = metric_columns.get(metric, metric_columns["revenue"])
            
            query = select(
                DimProduct.stock_code,
                DimProduct.description,
                DimProduct.category,
                DimProduct.brand,
                metric_col.label("metric_value"),
                func.sum(FactSale.quantity).label("total_quantity"),
                func.sum(FactSale.total_amount).label("total_revenue"),
                func.count(FactSale.sale_id).label("transaction_count")
            ).select_from(FactSale)\
             .join(DimProduct, FactSale.product_key == DimProduct.product_key)
            
            # Add date filter if specified
            date_filter = self._build_date_filter(date_from, date_to)
            if date_filter is not None:
                query = query.join(DimDate, FactSale.date_key == DimDate.date_key)\
                            .where(date_filter)
            
            query = query.group_by(
                DimProduct.stock_code,
                DimProduct.description,
                DimProduct.category,
                DimProduct.brand
            ).order_by(desc("metric_value")).limit(top_n)
            
            results = self.session.exec(query).all()
            
            return [
                {
                    "stock_code": row.stock_code,
                    "description": row.description,
                    "category": row.category,
                    "brand": row.brand,
                    f"{metric}_value": float(row.metric_value or 0),
                    "total_quantity": int(row.total_quantity),
                    "total_revenue": float(row.total_revenue),
                    "transaction_count": int(row.transaction_count)
                }
                for row in results
            ]
            
        except Exception as e:
            logger.error(f"Error in get_product_performance: {str(e)}")
            raise
    
    async def get_country_performance(self) -> List[Dict[str, Any]]:
        """Get sales performance by country."""
        try:
            query = select(
                DimCountry.country_name,
                DimCountry.country_code,
                DimCountry.region,
                DimCountry.continent,
                func.sum(FactSale.total_amount).label("total_revenue"),
                func.sum(FactSale.quantity).label("total_quantity"),
                func.count(FactSale.sale_id).label("transaction_count"),
                func.count(func.distinct(FactSale.customer_key)).label("unique_customers"),
                func.avg(FactSale.total_amount).label("avg_order_value")
            ).select_from(FactSale)\
             .join(DimCountry, FactSale.country_key == DimCountry.country_key)\
             .group_by(
                DimCountry.country_name,
                DimCountry.country_code,
                DimCountry.region,
                DimCountry.continent
            ).order_by(desc("total_revenue"))
            
            results = self.session.exec(query).all()
            
            return [
                {
                    "country_name": row.country_name,
                    "country_code": row.country_code,
                    "region": row.region,
                    "continent": row.continent,
                    "total_revenue": float(row.total_revenue),
                    "total_quantity": int(row.total_quantity),
                    "transaction_count": int(row.transaction_count),
                    "unique_customers": int(row.unique_customers),
                    "avg_order_value": float(row.avg_order_value or 0)
                }
                for row in results
            ]
            
        except Exception as e:
            logger.error(f"Error in get_country_performance: {str(e)}")
            raise
    
    async def get_seasonal_trends(self, year: Optional[int] = None) -> Dict[str, Any]:
        """Get seasonal sales trends and patterns."""
        try:
            query = select(
                DimDate.month,
                DimDate.month_name,
                DimDate.quarter,
                func.sum(FactSale.total_amount).label("revenue"),
                func.sum(FactSale.quantity).label("quantity"),
                func.count(FactSale.sale_id).label("transactions"),
                func.avg(FactSale.total_amount).label("avg_order_value")
            ).select_from(FactSale)\
             .join(DimDate, FactSale.date_key == DimDate.date_key)
            
            if year:
                query = query.where(DimDate.year == year)
            
            query = query.group_by(
                DimDate.month,
                DimDate.month_name,
                DimDate.quarter
            ).order_by(DimDate.month)
            
            results = self.session.exec(query).all()
            
            monthly_trends = [
                {
                    "month": row.month,
                    "month_name": row.month_name,
                    "quarter": row.quarter,
                    "revenue": float(row.revenue),
                    "quantity": int(row.quantity),
                    "transactions": int(row.transactions),
                    "avg_order_value": float(row.avg_order_value or 0)
                }
                for row in results
            ]
            
            # Calculate quarterly aggregates
            quarterly_data = {}
            for month_data in monthly_trends:
                quarter = month_data["quarter"]
                if quarter not in quarterly_data:
                    quarterly_data[quarter] = {
                        "quarter": quarter,
                        "revenue": 0,
                        "quantity": 0,
                        "transactions": 0
                    }
                quarterly_data[quarter]["revenue"] += month_data["revenue"]
                quarterly_data[quarter]["quantity"] += month_data["quantity"]
                quarterly_data[quarter]["transactions"] += month_data["transactions"]
            
            quarterly_trends = list(quarterly_data.values())
            for quarter in quarterly_trends:
                quarter["avg_order_value"] = (
                    quarter["revenue"] / quarter["transactions"] if quarter["transactions"] > 0 else 0
                )
            
            return {
                "year": year,
                "monthly_trends": monthly_trends,
                "quarterly_trends": quarterly_trends,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in get_seasonal_trends: {str(e)}")
            raise
    
    async def get_cohort_analysis(self, cohort_type: str = "monthly") -> Dict[str, Any]:
        """Get customer cohort analysis for retention insights."""
        try:
            # This would typically require a more complex query
            # For now, returning a simplified version
            return {
                "cohort_type": cohort_type,
                "analysis": "Cohort analysis requires complex temporal calculations",
                "note": "This feature requires advanced SQL window functions",
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in get_cohort_analysis: {str(e)}")
            raise
    
    async def get_business_metrics(self) -> Dict[str, Any]:
        """Get key business metrics and KPIs."""
        try:
            # Overall metrics
            overall_query = select(
                func.sum(FactSale.total_amount).label("total_revenue"),
                func.sum(FactSale.quantity).label("total_units"),
                func.count(FactSale.sale_id).label("total_transactions"),
                func.count(func.distinct(FactSale.customer_key)).label("total_customers"),
                func.avg(FactSale.total_amount).label("avg_order_value"),
                func.sum(FactSale.profit_amount).label("total_profit")
            ).select_from(FactSale)
            
            overall = self.session.exec(overall_query).first()
            
            # Customer metrics
            customer_query = select(
                func.avg(DimCustomer.lifetime_value).label("avg_clv"),
                func.avg(DimCustomer.total_orders).label("avg_orders_per_customer"),
                func.count(DimCustomer.customer_key).label("active_customers")
            ).where(DimCustomer.is_current == True)
            
            customer_metrics = self.session.exec(customer_query).first()
            
            # Product metrics
            product_query = select(
                func.count(func.distinct(DimProduct.product_key)).label("total_products"),
                func.count(func.distinct(DimProduct.category)).label("total_categories")
            ).where(DimProduct.is_current == True)
            
            product_metrics = self.session.exec(product_query).first()
            
            return {
                "revenue_metrics": {
                    "total_revenue": float(overall.total_revenue or 0),
                    "total_profit": float(overall.total_profit or 0),
                    "profit_margin": float(
                        (overall.total_profit / overall.total_revenue * 100) 
                        if overall.total_revenue and overall.total_profit else 0
                    )
                },
                "sales_metrics": {
                    "total_transactions": int(overall.total_transactions or 0),
                    "total_units_sold": int(overall.total_units or 0),
                    "avg_order_value": float(overall.avg_order_value or 0)
                },
                "customer_metrics": {
                    "total_customers": int(overall.total_customers or 0),
                    "active_customers": int(customer_metrics.active_customers or 0),
                    "avg_customer_lifetime_value": float(customer_metrics.avg_clv or 0),
                    "avg_orders_per_customer": float(customer_metrics.avg_orders_per_customer or 0)
                },
                "product_metrics": {
                    "total_products": int(product_metrics.total_products or 0),
                    "total_categories": int(product_metrics.total_categories or 0)
                },
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in get_business_metrics: {str(e)}")
            raise
    
    # Helper methods
    
    def _build_date_filter(self, date_from: Optional[str], date_to: Optional[str]):
        """Build date filter for queries."""
        if not date_from and not date_to:
            return None
        
        filters = []
        if date_from:
            filters.append(DimDate.date >= datetime.strptime(date_from, "%Y-%m-%d").date())
        if date_to:
            filters.append(DimDate.date <= datetime.strptime(date_to, "%Y-%m-%d").date())
        
        return and_(*filters) if len(filters) > 1 else filters[0]
    
    def _get_time_grouping(self, granularity: str):
        """Get appropriate time grouping for the specified granularity."""
        if granularity == "daily":
            return DimDate.date
        elif granularity == "weekly":
            return func.concat(DimDate.year, '-W', DimDate.week)
        elif granularity == "monthly":
            return func.concat(DimDate.year, '-', func.lpad(DimDate.month, 2, '0'))
        elif granularity == "quarterly":
            return func.concat(DimDate.year, '-Q', DimDate.quarter)
        else:
            return DimDate.date
    
    def _get_previous_period_metrics(self, date_from: Optional[str], date_to: Optional[str]):
        """Get metrics for the previous period for comparison."""
        # Simplified - would need more complex date logic for accurate comparison
        return None
    
    def _calculate_growth_metrics(self, current, previous):
        """Calculate growth metrics comparing current vs previous period."""
        if not previous:
            return {"note": "No previous period data available for comparison"}
        
        return {
            "revenue_growth": 0.0,  # Placeholder
            "transaction_growth": 0.0,  # Placeholder
            "customer_growth": 0.0  # Placeholder
        }
    
    def _calculate_time_series_summary(self, time_series: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics for time series data."""
        if not time_series:
            return {}
        
        revenues = [point["revenue"] for point in time_series]
        transactions = [point["transactions"] for point in time_series]
        
        return {
            "total_revenue": sum(revenues),
            "total_transactions": sum(transactions),
            "avg_revenue_per_period": sum(revenues) / len(revenues),
            "max_revenue_period": max(revenues),
            "min_revenue_period": min(revenues)
        }
    
    async def _get_top_products(self, limit: int = 5, date_filter=None) -> List[Dict[str, Any]]:
        """Get top products by revenue."""
        query = select(
            DimProduct.description,
            func.sum(FactSale.total_amount).label("revenue")
        ).select_from(FactSale)\
         .join(DimProduct, FactSale.product_key == DimProduct.product_key)
        
        if date_filter is not None:
            query = query.join(DimDate, FactSale.date_key == DimDate.date_key)\
                        .where(date_filter)
        
        query = query.group_by(DimProduct.description)\
                    .order_by(desc("revenue"))\
                    .limit(limit)
        
        results = self.session.exec(query).all()
        
        return [
            {
                "product": row.description,
                "revenue": float(row.revenue)
            }
            for row in results
        ]
    
    async def _get_top_countries(self, limit: int = 5, date_filter=None) -> List[Dict[str, Any]]:
        """Get top countries by revenue."""
        query = select(
            DimCountry.country_name,
            func.sum(FactSale.total_amount).label("revenue")
        ).select_from(FactSale)\
         .join(DimCountry, FactSale.country_key == DimCountry.country_key)
        
        if date_filter is not None:
            query = query.join(DimDate, FactSale.date_key == DimDate.date_key)\
                        .where(date_filter)
        
        query = query.group_by(DimCountry.country_name)\
                    .order_by(desc("revenue"))\
                    .limit(limit)
        
        results = self.session.exec(query).all()
        
        return [
            {
                "country": row.country_name,
                "revenue": float(row.revenue)
            }
            for row in results
        ]