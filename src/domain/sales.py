"""
Sales Domain Aggregate
======================

Comprehensive sales domain aggregate that consolidates sales entities,
business rules, and domain services for retail operations.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import UUID

from .entities.sales import (
    SalesTransaction,
    SalesLineItem,
    SalesAnalytics,
    SalesChannelType,
    PaymentMethodType,
    SalesStatus,
    ReturnReason,
    RiskLevel,
    create_sales_transaction,
    create_sample_sales_transaction
)
from .entities.sale import Sale
from .entities.sales_transaction import SalesTransactionEntity
from .base import DomainEntity
from .services.sales_service import SalesService


class SalesDomain:
    """
    Sales domain aggregate root that orchestrates sales operations,
    analytics, and business rule validation.
    """

    def __init__(self, sales_service: Optional[SalesService] = None):
        """Initialize sales domain with optional service dependency."""
        self.sales_service = sales_service or SalesService()
        self._transactions: Dict[UUID, SalesTransaction] = {}
        self._analytics_cache: Dict[str, Any] = {}

    def create_transaction(
        self,
        order_number: str,
        line_items: List[Dict[str, Any]],
        customer_id: Optional[str] = None,
        **kwargs
    ) -> SalesTransaction:
        """Create a new sales transaction with validation."""
        transaction = create_sales_transaction(
            order_number=order_number,
            line_items=line_items,
            customer_id=customer_id,
            **kwargs
        )

        # Validate business rules
        violations = transaction.validate_business_rules()
        if violations:
            raise ValueError(f"Business rule violations: {violations}")

        # Cache the transaction
        self._transactions[transaction.transaction_id] = transaction

        return transaction

    def process_return(
        self,
        transaction_id: UUID,
        return_reason: ReturnReason,
        return_items: Optional[List[str]] = None,
        return_date: Optional[datetime] = None
    ) -> SalesTransaction:
        """Process a return for a transaction."""
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            raise ValueError(f"Transaction {transaction_id} not found")

        if not transaction.is_returnable:
            raise ValueError("Transaction is not eligible for return")

        # Update transaction for return
        transaction.return_reason = return_reason
        transaction.return_date = return_date or datetime.utcnow()
        transaction.status = SalesStatus.RETURNED

        # Update analytics
        transaction.update_analytics()

        return transaction

    def calculate_sales_metrics(
        self,
        start_date: datetime,
        end_date: datetime,
        group_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculate comprehensive sales metrics for a period."""
        transactions = [
            t for t in self._transactions.values()
            if start_date <= t.transaction_date <= end_date
        ]

        if not transactions:
            return {
                'total_revenue': 0.0,
                'total_orders': 0,
                'average_order_value': 0.0,
                'total_items_sold': 0,
                'top_products': [],
                'channel_breakdown': {},
                'payment_method_breakdown': {}
            }

        # Calculate basic metrics
        total_revenue = sum(float(t.total_amount) for t in transactions)
        total_orders = len(transactions)
        average_order_value = total_revenue / total_orders if total_orders > 0 else 0.0
        total_items_sold = sum(sum(item.quantity for item in t.line_items) for t in transactions)

        # Channel breakdown
        channel_breakdown = {}
        for transaction in transactions:
            channel = transaction.sales_channel.value
            channel_breakdown[channel] = channel_breakdown.get(channel, 0) + float(transaction.total_amount)

        # Payment method breakdown
        payment_breakdown = {}
        for transaction in transactions:
            method = transaction.payment_method.value
            payment_breakdown[method] = payment_breakdown.get(method, 0) + 1

        # Top products analysis
        product_sales = {}
        for transaction in transactions:
            for item in transaction.line_items:
                if item.product_id not in product_sales:
                    product_sales[item.product_id] = {
                        'product_name': item.product_name,
                        'quantity_sold': 0,
                        'revenue': 0.0
                    }
                product_sales[item.product_id]['quantity_sold'] += item.quantity
                product_sales[item.product_id]['revenue'] += float(item.total_amount)

        # Sort by revenue and get top 10
        top_products = sorted(
            product_sales.items(),
            key=lambda x: x[1]['revenue'],
            reverse=True
        )[:10]

        return {
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            },
            'total_revenue': total_revenue,
            'total_orders': total_orders,
            'average_order_value': average_order_value,
            'total_items_sold': total_items_sold,
            'top_products': [
                {
                    'product_id': product_id,
                    'product_name': data['product_name'],
                    'quantity_sold': data['quantity_sold'],
                    'revenue': data['revenue']
                }
                for product_id, data in top_products
            ],
            'channel_breakdown': channel_breakdown,
            'payment_method_breakdown': payment_breakdown
        }

    def identify_high_risk_transactions(
        self,
        risk_threshold: float = 0.5
    ) -> List[SalesTransaction]:
        """Identify transactions with high fraud risk."""
        high_risk_transactions = []

        for transaction in self._transactions.values():
            if not transaction.analytics:
                transaction.update_analytics()

            if (transaction.analytics and
                transaction.analytics.fraud_score >= risk_threshold):
                high_risk_transactions.append(transaction)

        # Sort by fraud score descending
        high_risk_transactions.sort(
            key=lambda t: t.analytics.fraud_score if t.analytics else 0,
            reverse=True
        )

        return high_risk_transactions

    def calculate_customer_lifetime_value(
        self,
        customer_id: str
    ) -> Dict[str, Any]:
        """Calculate lifetime value for a specific customer."""
        customer_transactions = [
            t for t in self._transactions.values()
            if t.customer_id == customer_id
        ]

        if not customer_transactions:
            return {
                'customer_id': customer_id,
                'total_spent': 0.0,
                'total_orders': 0,
                'average_order_value': 0.0,
                'first_purchase_date': None,
                'last_purchase_date': None,
                'customer_lifetime_days': 0,
                'purchase_frequency_per_month': 0.0
            }

        # Sort by transaction date
        customer_transactions.sort(key=lambda t: t.transaction_date)

        total_spent = sum(float(t.total_amount) for t in customer_transactions)
        total_orders = len(customer_transactions)
        average_order_value = total_spent / total_orders

        first_purchase = customer_transactions[0].transaction_date
        last_purchase = customer_transactions[-1].transaction_date

        lifetime_days = (last_purchase - first_purchase).days + 1
        purchase_frequency = (total_orders / (lifetime_days / 30.0)) if lifetime_days > 0 else 0

        return {
            'customer_id': customer_id,
            'total_spent': total_spent,
            'total_orders': total_orders,
            'average_order_value': average_order_value,
            'first_purchase_date': first_purchase.isoformat(),
            'last_purchase_date': last_purchase.isoformat(),
            'customer_lifetime_days': lifetime_days,
            'purchase_frequency_per_month': purchase_frequency
        }

    def forecast_sales(
        self,
        forecast_days: int = 30,
        confidence_interval: float = 0.95
    ) -> Dict[str, Any]:
        """Simple sales forecasting based on historical data."""
        if not self._transactions:
            return {
                'forecast_period_days': forecast_days,
                'predicted_revenue': 0.0,
                'predicted_orders': 0,
                'confidence_interval': confidence_interval,
                'method': 'insufficient_data'
            }

        # Get recent 90 days of data for forecasting
        ninety_days_ago = datetime.utcnow() - timedelta(days=90)
        recent_transactions = [
            t for t in self._transactions.values()
            if t.transaction_date >= ninety_days_ago
        ]

        if len(recent_transactions) < 10:
            return {
                'forecast_period_days': forecast_days,
                'predicted_revenue': 0.0,
                'predicted_orders': 0,
                'confidence_interval': confidence_interval,
                'method': 'insufficient_recent_data'
            }

        # Simple moving average forecast
        daily_revenue = {}
        daily_orders = {}

        for transaction in recent_transactions:
            date_key = transaction.transaction_date.date()
            daily_revenue[date_key] = daily_revenue.get(date_key, 0.0) + float(transaction.total_amount)
            daily_orders[date_key] = daily_orders.get(date_key, 0) + 1

        # Calculate averages
        avg_daily_revenue = sum(daily_revenue.values()) / len(daily_revenue) if daily_revenue else 0
        avg_daily_orders = sum(daily_orders.values()) / len(daily_orders) if daily_orders else 0

        predicted_revenue = avg_daily_revenue * forecast_days
        predicted_orders = int(avg_daily_orders * forecast_days)

        return {
            'forecast_period_days': forecast_days,
            'predicted_revenue': predicted_revenue,
            'predicted_orders': predicted_orders,
            'confidence_interval': confidence_interval,
            'method': 'moving_average',
            'historical_data_points': len(recent_transactions),
            'avg_daily_revenue': avg_daily_revenue,
            'avg_daily_orders': avg_daily_orders
        }

    def get_transaction_by_id(self, transaction_id: UUID) -> Optional[SalesTransaction]:
        """Retrieve a transaction by ID."""
        return self._transactions.get(transaction_id)

    def get_transactions_by_customer(self, customer_id: str) -> List[SalesTransaction]:
        """Get all transactions for a customer."""
        return [
            t for t in self._transactions.values()
            if t.customer_id == customer_id
        ]

    def get_transactions_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[SalesTransaction]:
        """Get transactions within a date range."""
        return [
            t for t in self._transactions.values()
            if start_date <= t.transaction_date <= end_date
        ]

    def validate_transaction_integrity(self) -> Dict[str, Any]:
        """Validate integrity of all transactions."""
        validation_results = {
            'total_transactions': len(self._transactions),
            'valid_transactions': 0,
            'invalid_transactions': 0,
            'validation_errors': [],
            'data_quality_score': 1.0
        }

        for transaction_id, transaction in self._transactions.items():
            violations = transaction.validate_business_rules()
            if violations:
                validation_results['invalid_transactions'] += 1
                validation_results['validation_errors'].append({
                    'transaction_id': str(transaction_id),
                    'order_number': transaction.order_number,
                    'violations': violations
                })
            else:
                validation_results['valid_transactions'] += 1

        # Calculate overall data quality score
        if validation_results['total_transactions'] > 0:
            validation_results['data_quality_score'] = (
                validation_results['valid_transactions'] /
                validation_results['total_transactions']
            )

        return validation_results


# Export key classes and functions
__all__ = [
    # Domain aggregate
    'SalesDomain',

    # Entity classes
    'SalesTransaction',
    'SalesLineItem',
    'SalesAnalytics',

    # Enums
    'SalesChannelType',
    'PaymentMethodType',
    'SalesStatus',
    'ReturnReason',
    'RiskLevel',

    # Factory functions
    'create_sales_transaction',
    'create_sample_sales_transaction'
]