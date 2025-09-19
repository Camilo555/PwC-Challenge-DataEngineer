"""
Customer Analytics Service
==========================

Advanced customer analytics service providing sophisticated customer
insights, segmentation, and predictive analytics capabilities.
"""

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from ..entities.customers import CustomerProfile, CustomerSegment, RiskLevel
from ..entities.business_intelligence import (
    BusinessIntelligenceMetrics,
    CrossDomainInsights,
    PredictiveAnalytics
)


class CustomerAnalyticsService:
    """
    Advanced customer analytics service with ML-driven insights
    and predictive capabilities.
    """

    def __init__(self):
        """Initialize customer analytics service."""
        self._analytics_cache: Dict[str, Any] = {}
        self._segmentation_models: Dict[str, Any] = {}
        self._prediction_models: Dict[str, Any] = {}

    async def calculate_advanced_rfm_analysis(
        self,
        customers: List[CustomerProfile],
        recency_weights: Tuple[float, float, float] = (0.3, 0.3, 0.4)
    ) -> Dict[str, Any]:
        """
        Calculate advanced RFM analysis with weighted scoring and
        sophisticated segmentation algorithms.
        """
        if not customers:
            return {
                'total_customers': 0,
                'rfm_distribution': {},
                'segment_characteristics': {},
                'model_performance': {}
            }

        # Prepare RFM data
        rfm_data = []
        for customer in customers:
            days_since_last = customer.days_since_last_purchase()
            if days_since_last is not None:
                rfm_data.append({
                    'customer_id': customer.customer_id,
                    'recency_days': days_since_last,
                    'frequency': customer.total_orders,
                    'monetary': float(customer.total_spent)
                })

        if not rfm_data:
            return {'error': 'No customer purchase data available'}

        # Calculate quintiles for scoring
        recency_values = sorted([c['recency_days'] for c in rfm_data])
        frequency_values = sorted([c['frequency'] for c in rfm_data], reverse=True)
        monetary_values = sorted([c['monetary'] for c in rfm_data], reverse=True)

        def get_quintile_score(value: float, values: List[float], reverse: bool = False) -> int:
            """Get quintile score (1-5) for a value."""
            quintile_size = len(values) // 5
            if quintile_size == 0:
                return 3  # Default to middle score

            for i in range(5):
                start_idx = i * quintile_size
                end_idx = (i + 1) * quintile_size if i < 4 else len(values)
                quintile_values = values[start_idx:end_idx]

                if value in quintile_values:
                    return 5 - i if reverse else i + 1

            return 3  # Default fallback

        # Calculate RFM scores with weights
        segment_distribution = defaultdict(int)
        customer_scores = {}

        for customer_data in rfm_data:
            recency_score = get_quintile_score(
                customer_data['recency_days'], recency_values, reverse=True
            )
            frequency_score = get_quintile_score(
                customer_data['frequency'], frequency_values
            )
            monetary_score = get_quintile_score(
                customer_data['monetary'], monetary_values
            )

            # Apply weights to calculate weighted RFM score
            weighted_score = (
                recency_score * recency_weights[0] +
                frequency_score * recency_weights[1] +
                monetary_score * recency_weights[2]
            )

            # Determine segment based on RFM combination
            segment = self._determine_rfm_segment(recency_score, frequency_score, monetary_score)

            customer_scores[customer_data['customer_id']] = {
                'recency_score': recency_score,
                'frequency_score': frequency_score,
                'monetary_score': monetary_score,
                'weighted_score': weighted_score,
                'segment': segment
            }

            segment_distribution[segment.value] += 1

        # Calculate segment characteristics
        segment_characteristics = {}
        for segment in CustomerSegment:
            segment_customers = [
                customer for customer in customers
                if customer_scores.get(customer.customer_id, {}).get('segment') == segment
            ]

            if segment_customers:
                total_revenue = sum(float(c.total_spent) for c in segment_customers)
                avg_orders = sum(c.total_orders for c in segment_customers) / len(segment_customers)
                avg_spending = total_revenue / len(segment_customers)

                segment_characteristics[segment.value] = {
                    'customer_count': len(segment_customers),
                    'percentage_of_base': len(segment_customers) / len(customers) * 100,
                    'total_revenue': total_revenue,
                    'avg_revenue_per_customer': avg_spending,
                    'avg_orders_per_customer': avg_orders,
                    'revenue_contribution': total_revenue / sum(float(c.total_spent) for c in customers) * 100
                }

        return {
            'total_customers': len(customers),
            'rfm_distribution': dict(segment_distribution),
            'segment_characteristics': segment_characteristics,
            'customer_scores': customer_scores,
            'model_performance': {
                'segmentation_quality': self._calculate_segmentation_quality(segment_characteristics),
                'revenue_concentration': self._calculate_revenue_concentration(segment_characteristics),
                'actionable_insights': self._generate_rfm_insights(segment_characteristics)
            }
        }

    def _determine_rfm_segment(self, recency: int, frequency: int, monetary: int) -> CustomerSegment:
        """Determine customer segment based on RFM scores."""
        # Advanced segmentation logic based on RFM combination
        if recency >= 4 and frequency >= 4 and monetary >= 4:
            return CustomerSegment.CHAMPIONS
        elif recency >= 3 and frequency >= 4 and monetary >= 3:
            return CustomerSegment.LOYAL_CUSTOMERS
        elif recency >= 4 and frequency <= 2 and monetary >= 3:
            return CustomerSegment.POTENTIAL_LOYALISTS
        elif recency >= 4 and frequency <= 2 and monetary <= 2:
            return CustomerSegment.NEW_CUSTOMERS
        elif recency >= 3 and frequency >= 2 and monetary >= 2:
            return CustomerSegment.PROMISING
        elif recency <= 3 and frequency >= 3 and monetary >= 3:
            return CustomerSegment.NEED_ATTENTION
        elif recency <= 2 and frequency >= 2 and monetary >= 2:
            return CustomerSegment.ABOUT_TO_SLEEP
        elif recency <= 2 and frequency >= 4 and monetary >= 4:
            return CustomerSegment.CANNOT_LOSE_THEM
        elif recency <= 2 and frequency <= 2 and monetary >= 4:
            return CustomerSegment.AT_RISK
        elif recency <= 2 and frequency <= 2 and monetary <= 2:
            return CustomerSegment.HIBERNATING
        else:
            return CustomerSegment.LOST

    def _calculate_segmentation_quality(self, segment_characteristics: Dict[str, Any]) -> float:
        """Calculate the quality of segmentation based on revenue distribution."""
        if not segment_characteristics:
            return 0.0

        # Good segmentation should have clear differentiation in revenue per customer
        revenue_per_customer_values = [
            char['avg_revenue_per_customer']
            for char in segment_characteristics.values()
            if char['customer_count'] > 0
        ]

        if len(revenue_per_customer_values) < 2:
            return 0.5

        # Calculate coefficient of variation
        mean_revenue = sum(revenue_per_customer_values) / len(revenue_per_customer_values)
        variance = sum((x - mean_revenue) ** 2 for x in revenue_per_customer_values) / len(revenue_per_customer_values)
        std_dev = variance ** 0.5

        cv = std_dev / mean_revenue if mean_revenue > 0 else 0
        return min(1.0, cv)  # Higher CV indicates better segmentation

    def _calculate_revenue_concentration(self, segment_characteristics: Dict[str, Any]) -> Dict[str, float]:
        """Calculate revenue concentration metrics."""
        if not segment_characteristics:
            return {'gini_coefficient': 0.0, 'top_segments_revenue_share': 0.0}

        # Sort segments by revenue contribution
        segments_by_revenue = sorted(
            segment_characteristics.items(),
            key=lambda x: x[1]['revenue_contribution'],
            reverse=True
        )

        # Calculate top 20% segments' revenue share (80/20 rule analysis)
        top_20_percent_count = max(1, len(segments_by_revenue) // 5)
        top_segments_revenue = sum(
            segment[1]['revenue_contribution']
            for segment in segments_by_revenue[:top_20_percent_count]
        )

        # Simple Gini coefficient approximation
        revenue_shares = [segment[1]['revenue_contribution'] for _, segment in segments_by_revenue]
        n = len(revenue_shares)
        gini = sum(
            (2 * (i + 1) - n - 1) * revenue_shares[i]
            for i in range(n)
        ) / (n * sum(revenue_shares)) if sum(revenue_shares) > 0 else 0

        return {
            'gini_coefficient': abs(gini),
            'top_segments_revenue_share': top_segments_revenue / 100
        }

    def _generate_rfm_insights(self, segment_characteristics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate actionable insights from RFM analysis."""
        insights = []

        for segment_name, characteristics in segment_characteristics.items():
            if characteristics['customer_count'] == 0:
                continue

            insight = {
                'segment': segment_name,
                'customer_count': characteristics['customer_count'],
                'recommendations': []
            }

            # Generate segment-specific recommendations
            if segment_name == 'champions':
                insight['recommendations'] = [
                    'Reward loyalty with exclusive offers',
                    'Ask for referrals and reviews',
                    'Upsell premium products'
                ]
            elif segment_name == 'loyal_customers':
                insight['recommendations'] = [
                    'Increase order frequency with subscription offers',
                    'Cross-sell complementary products',
                    'Provide early access to new products'
                ]
            elif segment_name == 'at_risk':
                insight['recommendations'] = [
                    'Send win-back campaigns',
                    'Offer limited-time discounts',
                    'Survey for feedback and improvement'
                ]
            elif segment_name == 'hibernating':
                insight['recommendations'] = [
                    'Re-engagement campaigns with significant discounts',
                    'Product education and value proposition',
                    'Consider removing from active marketing lists'
                ]

            # Add revenue impact assessment
            if characteristics['revenue_contribution'] > 20:
                insight['priority'] = 'high'
                insight['impact'] = 'High revenue impact segment'
            elif characteristics['revenue_contribution'] > 10:
                insight['priority'] = 'medium'
                insight['impact'] = 'Moderate revenue impact segment'
            else:
                insight['priority'] = 'low'
                insight['impact'] = 'Lower revenue impact segment'

            insights.append(insight)

        return insights

    async def predict_customer_lifetime_value(
        self,
        customers: List[CustomerProfile],
        prediction_horizon_months: int = 12
    ) -> Dict[str, Any]:
        """
        Predict customer lifetime value using advanced analytics and
        machine learning techniques.
        """
        predictions = {}
        aggregate_metrics = {
            'total_predicted_clv': 0.0,
            'avg_predicted_clv': 0.0,
            'high_value_customers': 0,
            'clv_distribution': {'low': 0, 'medium': 0, 'high': 0, 'premium': 0}
        }

        for customer in customers:
            # Calculate current CLV metrics
            current_clv = customer.calculate_lifetime_value()

            # Predict future CLV based on patterns
            predicted_clv = await self._calculate_predictive_clv(
                customer, prediction_horizon_months
            )

            # Risk assessment for CLV realization
            clv_risk_score = await self._assess_clv_risk(customer)

            predictions[str(customer.customer_id)] = {
                'current_clv': current_clv.historical_clv,
                'predicted_clv': predicted_clv,
                'clv_growth_potential': predicted_clv - current_clv.historical_clv,
                'risk_score': clv_risk_score,
                'confidence': self._calculate_prediction_confidence(customer),
                'value_category': self._categorize_clv(predicted_clv)
            }

            # Update aggregate metrics
            aggregate_metrics['total_predicted_clv'] += predicted_clv

            if predicted_clv > 1000:
                aggregate_metrics['high_value_customers'] += 1

            # CLV distribution
            category = self._categorize_clv(predicted_clv)
            aggregate_metrics['clv_distribution'][category] += 1

        # Calculate averages
        if customers:
            aggregate_metrics['avg_predicted_clv'] = (
                aggregate_metrics['total_predicted_clv'] / len(customers)
            )

        return {
            'prediction_horizon_months': prediction_horizon_months,
            'individual_predictions': predictions,
            'aggregate_metrics': aggregate_metrics,
            'model_performance': {
                'avg_confidence': sum(
                    pred['confidence'] for pred in predictions.values()
                ) / len(predictions) if predictions else 0,
                'high_confidence_predictions': len([
                    pred for pred in predictions.values()
                    if pred['confidence'] > 0.8
                ])
            }
        }

    async def _calculate_predictive_clv(
        self,
        customer: CustomerProfile,
        horizon_months: int
    ) -> float:
        """Calculate predictive CLV using customer behavior patterns."""
        # Simple predictive model based on historical patterns
        if customer.total_orders == 0:
            return 0.0

        # Calculate monthly purchase frequency
        lifetime_days = customer.customer_lifetime_days()
        if lifetime_days <= 0:
            monthly_frequency = 1.0  # Assume monthly for new customers
        else:
            monthly_frequency = customer.total_orders / (lifetime_days / 30.0)

        # Predict future purchases
        predicted_purchases = monthly_frequency * horizon_months

        # Apply growth/decay factor based on customer segment
        growth_factor = 1.0
        if hasattr(customer, 'rfm_analysis') and customer.rfm_analysis:
            segment = customer.rfm_analysis.rfm_segment
            if segment in [CustomerSegment.CHAMPIONS, CustomerSegment.LOYAL_CUSTOMERS]:
                growth_factor = 1.1  # 10% growth
            elif segment in [CustomerSegment.AT_RISK, CustomerSegment.HIBERNATING]:
                growth_factor = 0.7  # 30% decay
            elif segment == CustomerSegment.NEW_CUSTOMERS:
                growth_factor = 1.2  # 20% growth potential

        # Calculate predicted CLV
        avg_order_value = float(customer.average_order_value)
        predicted_clv = predicted_purchases * avg_order_value * growth_factor

        return max(0.0, predicted_clv)

    async def _assess_clv_risk(self, customer: CustomerProfile) -> float:
        """Assess risk factors that might impact CLV realization."""
        risk_score = 0.0

        # Recency risk
        days_since_last = customer.days_since_last_purchase()
        if days_since_last:
            if days_since_last > 90:
                risk_score += 0.3
            elif days_since_last > 30:
                risk_score += 0.1

        # Order frequency risk
        lifetime_days = customer.customer_lifetime_days()
        if lifetime_days > 0:
            monthly_frequency = customer.total_orders / (lifetime_days / 30.0)
            if monthly_frequency < 0.5:
                risk_score += 0.2

        # Spending pattern risk
        if customer.total_orders > 1:
            # Check for declining order values (simplified)
            if customer.last_order_value < customer.average_order_value * 0.8:
                risk_score += 0.2

        # Customer tier risk
        if customer.tier_level in ['Bronze', None]:
            risk_score += 0.1

        return min(1.0, risk_score)

    def _calculate_prediction_confidence(self, customer: CustomerProfile) -> float:
        """Calculate confidence level for predictions."""
        confidence = 0.5  # Base confidence

        # More orders = higher confidence
        if customer.total_orders >= 10:
            confidence += 0.3
        elif customer.total_orders >= 5:
            confidence += 0.2
        elif customer.total_orders >= 2:
            confidence += 0.1

        # Recent activity = higher confidence
        days_since_last = customer.days_since_last_purchase()
        if days_since_last and days_since_last < 30:
            confidence += 0.2
        elif days_since_last and days_since_last < 90:
            confidence += 0.1

        # Consistent spending = higher confidence
        if customer.total_orders > 3:
            # Simplified consistency check
            confidence += 0.1

        return min(1.0, confidence)

    def _categorize_clv(self, clv_value: float) -> str:
        """Categorize CLV into buckets."""
        if clv_value < 100:
            return 'low'
        elif clv_value < 500:
            return 'medium'
        elif clv_value < 2000:
            return 'high'
        else:
            return 'premium'

    async def generate_customer_insights_report(
        self,
        customers: List[CustomerProfile]
    ) -> Dict[str, Any]:
        """Generate comprehensive customer insights report."""
        # Run multiple analytics in parallel
        rfm_analysis, clv_predictions = await asyncio.gather(
            self.calculate_advanced_rfm_analysis(customers),
            self.predict_customer_lifetime_value(customers)
        )

        # Generate comprehensive insights
        insights_report = {
            'report_timestamp': datetime.utcnow().isoformat(),
            'customer_base_overview': {
                'total_customers': len(customers),
                'active_customers': len([c for c in customers if c.is_active_customer()]),
                'total_revenue': sum(float(c.total_spent) for c in customers),
                'avg_revenue_per_customer': sum(float(c.total_spent) for c in customers) / len(customers) if customers else 0
            },
            'segmentation_analysis': rfm_analysis,
            'lifetime_value_analysis': clv_predictions,
            'strategic_recommendations': self._generate_strategic_recommendations(
                rfm_analysis, clv_predictions
            ),
            'performance_metrics': {
                'segmentation_quality': rfm_analysis.get('model_performance', {}).get('segmentation_quality', 0),
                'prediction_confidence': clv_predictions.get('model_performance', {}).get('avg_confidence', 0),
                'actionable_insights_count': len(rfm_analysis.get('model_performance', {}).get('actionable_insights', []))
            }
        }

        return insights_report

    def _generate_strategic_recommendations(
        self,
        rfm_analysis: Dict[str, Any],
        clv_predictions: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate strategic recommendations based on analysis."""
        recommendations = []

        # RFM-based recommendations
        segment_characteristics = rfm_analysis.get('segment_characteristics', {})

        # High-value segment focus
        champions_data = segment_characteristics.get('champions', {})
        if champions_data.get('customer_count', 0) > 0:
            recommendations.append({
                'category': 'retention',
                'priority': 'high',
                'action': 'VIP Program Enhancement',
                'description': f"Focus on {champions_data['customer_count']} champion customers contributing "
                             f"{champions_data.get('revenue_contribution', 0):.1f}% of revenue",
                'expected_impact': 'Increase retention by 15-20%'
            })

        # At-risk customer intervention
        at_risk_data = segment_characteristics.get('at_risk', {})
        if at_risk_data.get('customer_count', 0) > 0:
            recommendations.append({
                'category': 'retention',
                'priority': 'high',
                'action': 'Win-back Campaign',
                'description': f"Immediate intervention needed for {at_risk_data['customer_count']} at-risk customers",
                'expected_impact': 'Prevent 30-50% churn through targeted campaigns'
            })

        # CLV-based recommendations
        clv_metrics = clv_predictions.get('aggregate_metrics', {})
        high_value_count = clv_metrics.get('high_value_customers', 0)

        if high_value_count > 0:
            recommendations.append({
                'category': 'growth',
                'priority': 'medium',
                'action': 'High-Value Customer Development',
                'description': f"Develop {high_value_count} high-value customers with >$1000 predicted CLV",
                'expected_impact': 'Increase average order value by 10-15%'
            })

        return recommendations


# Export the service
__all__ = ['CustomerAnalyticsService']