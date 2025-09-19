"""
Customers Domain Aggregate
==========================

Comprehensive customers domain aggregate that consolidates customer entities,
segmentation, analytics, and customer relationship management.
"""
from __future__ import annotations

from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import UUID

from .entities.customers import (
    CustomerProfile,
    CustomerAddress,
    CustomerContactInfo,
    RFMScores,
    CustomerLifetimeValue,
    CustomerBehavior,
    CustomerPreferences,
    CustomerStatus,
    CustomerSegment,
    CustomerType,
    CommunicationPreference,
    RiskLevel,
    create_customer_profile,
    create_sample_customer
)
from .entities.customer import CustomerEntity
from .base import DomainEntity


class CustomerDomain:
    """
    Customer domain aggregate root that orchestrates customer operations,
    analytics, segmentation, and relationship management.
    """

    def __init__(self):
        """Initialize customer domain."""
        self._customers: Dict[UUID, CustomerProfile] = {}
        self._customer_segments: Dict[str, List[UUID]] = {}
        self._analytics_cache: Dict[str, Any] = {}

    def create_customer(
        self,
        first_name: str,
        last_name: str,
        email: str,
        **kwargs
    ) -> CustomerProfile:
        """Create a new customer profile with validation."""
        customer = create_customer_profile(
            first_name=first_name,
            last_name=last_name,
            email=email,
            **kwargs
        )

        # Validate business rules
        violations = customer.validate_business_rules()
        if violations:
            raise ValueError(f"Customer validation failed: {violations}")

        # Cache the customer
        self._customers[customer.customer_id] = customer

        # Update segmentation
        self._update_customer_segments(customer)

        return customer

    def update_customer(
        self,
        customer_id: UUID,
        **updates
    ) -> CustomerProfile:
        """Update customer profile with validation."""
        customer = self._customers.get(customer_id)
        if not customer:
            raise ValueError(f"Customer {customer_id} not found")

        # Apply updates
        for field, value in updates.items():
            if hasattr(customer, field):
                setattr(customer, field, value)

        # Update timestamp
        customer.updated_at = datetime.utcnow()

        # Re-validate business rules
        violations = customer.validate_business_rules()
        if violations:
            raise ValueError(f"Updated customer validation failed: {violations}")

        # Update analytics if financial data changed
        financial_fields = {'total_orders', 'total_spent', 'average_order_value', 'last_order_value'}
        if any(field in updates for field in financial_fields):
            customer.update_tier_level()
            customer.assess_risk_level()

        # Update segmentation
        self._update_customer_segments(customer)

        return customer

    def calculate_customer_segments(
        self,
        recalculate: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """Calculate RFM segmentation for all customers."""
        if not recalculate and self._analytics_cache.get('segments'):
            return self._analytics_cache['segments']

        # Prepare data for RFM calculation
        all_customer_data = []
        for customer in self._customers.values():
            all_customer_data.append({
                'days_since_last_purchase': customer.days_since_last_purchase(),
                'total_orders': customer.total_orders,
                'total_spent': float(customer.total_spent)
            })

        # Calculate RFM scores for all customers
        segment_counts = {}
        segment_metrics = {}

        for customer in self._customers.values():
            rfm_scores = customer.calculate_rfm_scores(all_customer_data)
            segment = rfm_scores.rfm_segment.value

            # Update segment counts
            segment_counts[segment] = segment_counts.get(segment, 0) + 1

            # Update segment metrics
            if segment not in segment_metrics:
                segment_metrics[segment] = {
                    'total_customers': 0,
                    'total_revenue': 0.0,
                    'avg_order_value': 0.0,
                    'avg_orders_per_customer': 0.0,
                    'avg_clv': 0.0
                }

            metrics = segment_metrics[segment]
            metrics['total_customers'] += 1
            metrics['total_revenue'] += float(customer.total_spent)

        # Calculate averages
        for segment, metrics in segment_metrics.items():
            if metrics['total_customers'] > 0:
                metrics['avg_revenue_per_customer'] = metrics['total_revenue'] / metrics['total_customers']

                # Get customers in this segment for detailed calculations
                segment_customers = [
                    c for c in self._customers.values()
                    if c.rfm_analysis and c.rfm_analysis.rfm_segment.value == segment
                ]

                if segment_customers:
                    metrics['avg_order_value'] = sum(
                        float(c.average_order_value) for c in segment_customers
                    ) / len(segment_customers)

                    metrics['avg_orders_per_customer'] = sum(
                        c.total_orders for c in segment_customers
                    ) / len(segment_customers)

        results = {
            'segment_counts': segment_counts,
            'segment_metrics': segment_metrics,
            'total_customers': len(self._customers),
            'analysis_date': datetime.utcnow().isoformat()
        }

        # Cache results
        self._analytics_cache['segments'] = results
        return results

    def identify_at_risk_customers(
        self,
        days_threshold: int = 90,
        min_historical_value: float = 100.0
    ) -> List[CustomerProfile]:
        """Identify customers at risk of churning."""
        at_risk_customers = []
        current_date = datetime.utcnow()

        for customer in self._customers.values():
            # Skip if not enough historical value
            if customer.total_spent < min_historical_value:
                continue

            # Check if customer hasn't purchased recently
            days_since_last = customer.days_since_last_purchase()
            if days_since_last is None or days_since_last < days_threshold:
                continue

            # Additional risk factors
            risk_score = 0
            risk_factors = []

            # Long time since last purchase
            if days_since_last > days_threshold:
                risk_score += min(50, days_since_last / 10)
                risk_factors.append(f"No purchase in {days_since_last} days")

            # Declining order frequency
            if customer.total_orders > 5:
                lifetime_days = customer.customer_lifetime_days()
                if lifetime_days > 0:
                    monthly_frequency = customer.total_orders / (lifetime_days / 30.0)
                    if monthly_frequency < 0.5:  # Less than 0.5 orders per month
                        risk_score += 20
                        risk_factors.append("Low purchase frequency")

            # High-value customer with recent inactivity
            if customer.total_spent > 1000 and days_since_last > 60:
                risk_score += 30
                risk_factors.append("High-value customer inactive")

            if risk_score > 40:  # Threshold for at-risk classification
                customer.risk_factors = risk_factors
                at_risk_customers.append(customer)

        # Sort by risk score (approximated by days since last purchase and total spent)
        at_risk_customers.sort(
            key=lambda c: (c.days_since_last_purchase() or 0, -float(c.total_spent)),
            reverse=True
        )

        return at_risk_customers

    def calculate_customer_lifetime_values(
        self,
        discount_rate: float = 0.1,
        predicted_lifespan_months: int = 36
    ) -> Dict[str, Any]:
        """Calculate lifetime values for all customers."""
        clv_analysis = {
            'total_customers': len(self._customers),
            'total_historical_clv': 0.0,
            'total_predicted_clv': 0.0,
            'avg_historical_clv': 0.0,
            'avg_predicted_clv': 0.0,
            'clv_distribution': {
                'low': 0,     # < $100
                'medium': 0,  # $100 - $1000
                'high': 0,    # $1000 - $5000
                'premium': 0  # > $5000
            },
            'top_customers': []
        }

        customer_clvs = []

        for customer in self._customers.values():
            clv = customer.calculate_lifetime_value(
                discount_rate=discount_rate,
                predicted_lifespan_months=predicted_lifespan_months
            )

            clv_analysis['total_historical_clv'] += clv.historical_clv
            if clv.predicted_clv:
                clv_analysis['total_predicted_clv'] += clv.predicted_clv

            # Categorize CLV
            historical_clv = clv.historical_clv
            if historical_clv < 100:
                clv_analysis['clv_distribution']['low'] += 1
            elif historical_clv < 1000:
                clv_analysis['clv_distribution']['medium'] += 1
            elif historical_clv < 5000:
                clv_analysis['clv_distribution']['high'] += 1
            else:
                clv_analysis['clv_distribution']['premium'] += 1

            customer_clvs.append({
                'customer_id': str(customer.customer_id),
                'customer_name': customer.display_name,
                'email': customer.email,
                'historical_clv': historical_clv,
                'predicted_clv': clv.predicted_clv,
                'total_orders': customer.total_orders,
                'customer_since': customer.registration_date.isoformat()
            })

        # Calculate averages
        if clv_analysis['total_customers'] > 0:
            clv_analysis['avg_historical_clv'] = (
                clv_analysis['total_historical_clv'] / clv_analysis['total_customers']
            )
            if clv_analysis['total_predicted_clv'] > 0:
                clv_analysis['avg_predicted_clv'] = (
                    clv_analysis['total_predicted_clv'] / clv_analysis['total_customers']
                )

        # Sort by historical CLV and get top 10
        customer_clvs.sort(key=lambda x: x['historical_clv'], reverse=True)
        clv_analysis['top_customers'] = customer_clvs[:10]

        return clv_analysis

    def get_customer_insights(
        self,
        customer_id: UUID
    ) -> Dict[str, Any]:
        """Get comprehensive insights for a specific customer."""
        customer = self._customers.get(customer_id)
        if not customer:
            raise ValueError(f"Customer {customer_id} not found")

        insights = {
            'customer_profile': customer.to_analytics_dict(),
            'risk_assessment': {
                'risk_level': customer.risk_level.value,
                'risk_factors': customer.risk_factors,
                'is_at_risk': customer in self.identify_at_risk_customers()
            },
            'engagement_status': {
                'is_active': customer.is_active_customer(),
                'days_since_last_purchase': customer.days_since_last_purchase(),
                'customer_lifetime_days': customer.customer_lifetime_days(),
                'total_interactions': len(customer.addresses) + len(customer.contact_info)
            },
            'value_metrics': {},
            'predictions': {},
            'recommendations': []
        }

        # Value metrics
        if customer.lifetime_value:
            insights['value_metrics'] = {
                'historical_clv': customer.lifetime_value.historical_clv,
                'predicted_clv': customer.lifetime_value.predicted_clv,
                'average_order_value': customer.lifetime_value.average_order_value,
                'purchase_frequency': customer.lifetime_value.purchase_frequency,
                'churn_probability': customer.lifetime_value.churn_probability
            }

        # RFM Analysis
        if customer.rfm_analysis:
            insights['rfm_analysis'] = {
                'segment': customer.rfm_analysis.rfm_segment.value,
                'recency_score': customer.rfm_analysis.recency_score,
                'frequency_score': customer.rfm_analysis.frequency_score,
                'monetary_score': customer.rfm_analysis.monetary_score,
                'rfm_score': customer.rfm_analysis.rfm_score
            }

        # Generate recommendations based on customer data
        recommendations = self._generate_customer_recommendations(customer)
        insights['recommendations'] = recommendations

        return insights

    def _generate_customer_recommendations(
        self,
        customer: CustomerProfile
    ) -> List[Dict[str, str]]:
        """Generate personalized recommendations for a customer."""
        recommendations = []

        # High-value at-risk customer
        days_since_last = customer.days_since_last_purchase()
        if customer.total_spent > 1000 and days_since_last and days_since_last > 60:
            recommendations.append({
                'type': 'retention',
                'priority': 'high',
                'action': 'VIP re-engagement campaign',
                'description': f'High-value customer inactive for {days_since_last} days. Send personalized offer.'
            })

        # New customer welcome
        if customer.total_orders <= 2:
            recommendations.append({
                'type': 'onboarding',
                'priority': 'medium',
                'action': 'Welcome series',
                'description': 'New customer - send welcome email series and first purchase follow-up.'
            })

        # Tier upgrade opportunity
        if customer.tier_level == 'Silver' and customer.total_spent > 1800:
            recommendations.append({
                'type': 'upsell',
                'priority': 'medium',
                'action': 'Tier upgrade promotion',
                'description': 'Customer close to Gold tier - promote benefits and incentivize next purchase.'
            })

        # Frequent buyer rewards
        if customer.total_orders > 10 and customer.loyalty_points > 500:
            recommendations.append({
                'type': 'loyalty',
                'priority': 'low',
                'action': 'Loyalty reward reminder',
                'description': 'Frequent customer with high points - remind about redemption options.'
            })

        # Contact information update needed
        if not customer.phone and not customer.mobile:
            recommendations.append({
                'type': 'data_quality',
                'priority': 'low',
                'action': 'Contact info update',
                'description': 'Missing phone number - request update for better service.'
            })

        return recommendations

    def _update_customer_segments(self, customer: CustomerProfile):
        """Update customer segment cache."""
        if customer.rfm_analysis:
            segment = customer.rfm_analysis.rfm_segment.value
            if segment not in self._customer_segments:
                self._customer_segments[segment] = []

            if customer.customer_id not in self._customer_segments[segment]:
                self._customer_segments[segment].append(customer.customer_id)

    def get_customer_by_id(self, customer_id: UUID) -> Optional[CustomerProfile]:
        """Retrieve a customer by ID."""
        return self._customers.get(customer_id)

    def get_customer_by_email(self, email: str) -> Optional[CustomerProfile]:
        """Retrieve a customer by email address."""
        email = email.lower().strip()
        for customer in self._customers.values():
            if customer.email.lower() == email:
                return customer
        return None

    def get_customers_by_segment(self, segment: CustomerSegment) -> List[CustomerProfile]:
        """Get all customers in a specific segment."""
        segment_value = segment.value if isinstance(segment, CustomerSegment) else segment
        customer_ids = self._customer_segments.get(segment_value, [])
        return [self._customers[cid] for cid in customer_ids if cid in self._customers]

    def get_customers_by_tier(self, tier_level: str) -> List[CustomerProfile]:
        """Get all customers in a specific tier."""
        return [
            customer for customer in self._customers.values()
            if customer.tier_level == tier_level
        ]

    def search_customers(
        self,
        query: str,
        search_fields: Optional[List[str]] = None
    ) -> List[CustomerProfile]:
        """Search customers by various criteria."""
        if not query:
            return []

        search_fields = search_fields or ['first_name', 'last_name', 'email', 'customer_number']
        query_lower = query.lower()
        results = []

        for customer in self._customers.values():
            for field in search_fields:
                field_value = getattr(customer, field, '')
                if field_value and query_lower in str(field_value).lower():
                    results.append(customer)
                    break  # Avoid duplicates

        return results

    def get_customer_statistics(self) -> Dict[str, Any]:
        """Get comprehensive customer statistics."""
        total_customers = len(self._customers)
        if total_customers == 0:
            return {'total_customers': 0}

        # Basic statistics
        active_customers = sum(1 for c in self._customers.values() if c.is_active_customer())
        total_revenue = sum(float(c.total_spent) for c in self._customers.values())
        total_orders = sum(c.total_orders for c in self._customers.values())

        # Customer type breakdown
        type_breakdown = {}
        for customer in self._customers.values():
            customer_type = customer.customer_type.value
            type_breakdown[customer_type] = type_breakdown.get(customer_type, 0) + 1

        # Tier distribution
        tier_distribution = {}
        for customer in self._customers.values():
            tier = customer.tier_level or 'No Tier'
            tier_distribution[tier] = tier_distribution.get(tier, 0) + 1

        # Geographic distribution
        country_distribution = {}
        for customer in self._customers.values():
            country = customer.country or 'Unknown'
            country_distribution[country] = country_distribution.get(country, 0) + 1

        return {
            'total_customers': total_customers,
            'active_customers': active_customers,
            'active_rate': active_customers / total_customers if total_customers > 0 else 0,
            'total_revenue': total_revenue,
            'total_orders': total_orders,
            'avg_revenue_per_customer': total_revenue / total_customers if total_customers > 0 else 0,
            'avg_orders_per_customer': total_orders / total_customers if total_customers > 0 else 0,
            'customer_type_breakdown': type_breakdown,
            'tier_distribution': tier_distribution,
            'geographic_distribution': country_distribution,
            'analysis_date': datetime.utcnow().isoformat()
        }


# Export key classes and functions
__all__ = [
    # Domain aggregate
    'CustomerDomain',

    # Entity classes
    'CustomerProfile',
    'CustomerAddress',
    'CustomerContactInfo',
    'RFMScores',
    'CustomerLifetimeValue',
    'CustomerBehavior',
    'CustomerPreferences',

    # Enums
    'CustomerStatus',
    'CustomerSegment',
    'CustomerType',
    'CommunicationPreference',
    'RiskLevel',

    # Factory functions
    'create_customer_profile',
    'create_sample_customer'
]