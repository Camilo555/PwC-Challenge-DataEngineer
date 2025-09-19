"""
Advanced Business Metrics Collector
====================================

Enhanced Prometheus metrics collection with sophisticated business intelligence,
real-time analytics, and predictive metrics for enterprise data platforms.

Features:
- Real-time business KPI tracking
- Customer journey metrics
- Predictive analytics metrics
- Cross-domain business insights
- Advanced segmentation metrics
- Performance SLA tracking
- Business health monitoring
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

from prometheus_client import (
    Counter, Histogram, Gauge, Summary, Info, Enum,
    CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
)

from core.config import get_settings
from core.database import get_async_session
from core.logging import get_logger

# Import domain services for metrics collection
from domain.integration.domain_orchestrator import domain_orchestrator
from domain.services.customer_analytics_service import CustomerAnalyticsService

logger = get_logger(__name__)
settings = get_settings()

# Advanced business metrics registry
advanced_business_registry = CollectorRegistry()

# =================== ADVANCED BUSINESS METRICS ===================

# Executive KPI Metrics
business_health_score = Gauge(
    'business_health_score',
    'Overall business health score (0-100)',
    ['category'],  # financial, customer, operational, strategic
    registry=advanced_business_registry
)

revenue_growth_rate = Gauge(
    'revenue_growth_rate_percentage',
    'Revenue growth rate percentage',
    ['period', 'segment'],  # monthly, quarterly, yearly
    registry=advanced_business_registry
)

customer_acquisition_cost = Histogram(
    'customer_acquisition_cost_usd',
    'Customer acquisition cost in USD',
    ['channel', 'campaign_type', 'customer_segment'],
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, float('inf')),
    registry=advanced_business_registry
)

# Advanced Customer Journey Metrics
customer_journey_stage_duration = Histogram(
    'customer_journey_stage_duration_days',
    'Duration in each customer journey stage',
    ['from_stage', 'to_stage', 'customer_segment'],
    buckets=(1, 7, 14, 30, 60, 90, 180, 365, float('inf')),
    registry=advanced_business_registry
)

customer_touchpoint_effectiveness = Gauge(
    'customer_touchpoint_effectiveness_score',
    'Effectiveness score of customer touchpoints (0-100)',
    ['touchpoint_type', 'channel', 'journey_stage'],
    registry=advanced_business_registry
)

customer_engagement_score = Histogram(
    'customer_engagement_score',
    'Customer engagement score distribution',
    ['segment', 'channel'],
    buckets=(10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
    registry=advanced_business_registry
)

# RFM Segmentation Metrics
rfm_segment_distribution = Gauge(
    'rfm_segment_customer_count',
    'Number of customers in each RFM segment',
    ['rfm_segment'],
    registry=advanced_business_registry
)

rfm_segment_revenue_contribution = Gauge(
    'rfm_segment_revenue_contribution_percentage',
    'Revenue contribution percentage by RFM segment',
    ['rfm_segment'],
    registry=advanced_business_registry
)

rfm_segment_clv = Histogram(
    'rfm_segment_customer_lifetime_value_usd',
    'Customer lifetime value distribution by RFM segment',
    ['rfm_segment'],
    buckets=(50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, float('inf')),
    registry=advanced_business_registry
)

# Predictive Analytics Metrics
churn_prediction_confidence = Histogram(
    'churn_prediction_confidence_score',
    'Churn prediction confidence score distribution',
    ['risk_level', 'customer_segment'],
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
    registry=advanced_business_registry
)

revenue_forecast_accuracy = Gauge(
    'revenue_forecast_accuracy_percentage',
    'Revenue forecasting accuracy percentage',
    ['forecast_horizon_days', 'model_type'],
    registry=advanced_business_registry
)

demand_prediction_variance = Gauge(
    'demand_prediction_variance',
    'Demand prediction variance from actual',
    ['product_category', 'forecast_period'],
    registry=advanced_business_registry
)

# Business Performance Metrics
profit_margin_trend = Gauge(
    'profit_margin_trend_percentage',
    'Profit margin trend percentage',
    ['product_category', 'channel', 'period'],
    registry=advanced_business_registry
)

market_share_percentage = Gauge(
    'market_share_percentage',
    'Market share percentage in target segments',
    ['market_segment', 'geography'],
    registry=advanced_business_registry
)

competitive_position_score = Gauge(
    'competitive_position_score',
    'Competitive position score (0-100)',
    ['market_category', 'competitor_tier'],
    registry=advanced_business_registry
)

# Operational Excellence Metrics
process_efficiency_score = Gauge(
    'process_efficiency_score_percentage',
    'Business process efficiency score',
    ['process_name', 'department'],
    registry=advanced_business_registry
)

sla_compliance_rate = Gauge(
    'sla_compliance_rate_percentage',
    'SLA compliance rate percentage',
    ['service_type', 'sla_tier'],
    registry=advanced_business_registry
)

automation_coverage = Gauge(
    'automation_coverage_percentage',
    'Process automation coverage percentage',
    ['process_category', 'automation_type'],
    registry=advanced_business_registry
)

# Quality and Risk Metrics
data_quality_score = Histogram(
    'data_quality_score',
    'Data quality score distribution',
    ['data_source', 'quality_dimension'],  # accuracy, completeness, consistency
    buckets=(50, 60, 70, 80, 85, 90, 95, 98, 99, 100),
    registry=advanced_business_registry
)

business_risk_exposure = Gauge(
    'business_risk_exposure_score',
    'Business risk exposure score',
    ['risk_category', 'risk_level'],  # operational, financial, strategic, compliance
    registry=advanced_business_registry
)

compliance_coverage_percentage = Gauge(
    'compliance_coverage_percentage',
    'Compliance requirement coverage percentage',
    ['regulation_type', 'compliance_area'],
    registry=advanced_business_registry
)

# Innovation and Growth Metrics
innovation_pipeline_value = Gauge(
    'innovation_pipeline_value_usd',
    'Innovation pipeline value in USD',
    ['innovation_stage', 'category'],  # ideation, development, testing, launch
    registry=advanced_business_registry
)

time_to_market_days = Histogram(
    'time_to_market_days',
    'Time to market in days for new initiatives',
    ['initiative_type', 'complexity'],
    buckets=(7, 14, 30, 60, 90, 180, 365, 730, float('inf')),
    registry=advanced_business_registry
)

feature_adoption_rate = Gauge(
    'feature_adoption_rate_percentage',
    'New feature adoption rate percentage',
    ['feature_category', 'user_segment'],
    registry=advanced_business_registry
)


@dataclass
class MetricsCollectionConfig:
    """Configuration for metrics collection."""
    collection_interval_seconds: int = 60
    batch_size: int = 1000
    enable_predictive_metrics: bool = True
    enable_real_time_metrics: bool = True
    cache_duration_seconds: int = 300
    max_metric_age_hours: int = 24


class AdvancedBusinessMetricsCollector:
    """
    Advanced business metrics collector with intelligent sampling,
    predictive analytics integration, and real-time business insights.
    """

    def __init__(self, config: Optional[MetricsCollectionConfig] = None):
        """Initialize advanced business metrics collector."""
        self.config = config or MetricsCollectionConfig()
        self.customer_analytics = CustomerAnalyticsService()

        # Metrics cache for performance optimization
        self._metrics_cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, datetime] = {}

        # Performance tracking
        self._collection_performance: deque = deque(maxlen=100)
        self._error_counts: Dict[str, int] = defaultdict(int)

        # Background tasks
        self._collection_tasks: List[asyncio.Task] = []
        self._is_collecting = False

    async def start_collection(self):
        """Start background metrics collection."""
        if self._is_collecting:
            logger.warning("Metrics collection already running")
            return

        self._is_collecting = True
        logger.info("Starting advanced business metrics collection")

        # Start collection tasks
        self._collection_tasks = [
            asyncio.create_task(self._collect_executive_kpis()),
            asyncio.create_task(self._collect_customer_journey_metrics()),
            asyncio.create_task(self._collect_rfm_segmentation_metrics()),
            asyncio.create_task(self._collect_predictive_analytics_metrics()),
            asyncio.create_task(self._collect_operational_metrics()),
            asyncio.create_task(self._collect_quality_and_risk_metrics()),
            asyncio.create_task(self._collect_innovation_metrics())
        ]

        # Health monitoring task
        self._collection_tasks.append(
            asyncio.create_task(self._monitor_collection_health())
        )

    async def stop_collection(self):
        """Stop background metrics collection."""
        self._is_collecting = False
        logger.info("Stopping advanced business metrics collection")

        # Cancel all tasks
        for task in self._collection_tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self._collection_tasks, return_exceptions=True)
        self._collection_tasks.clear()

    async def _collect_executive_kpis(self):
        """Collect executive-level KPI metrics."""
        while self._is_collecting:
            try:
                start_time = time.time()

                # Get comprehensive business metrics
                if not self._is_cache_valid('executive_dashboard'):
                    dashboard_data = await domain_orchestrator.generate_executive_dashboard_data()
                    self._set_cache('executive_dashboard', dashboard_data)
                else:
                    dashboard_data = self._get_cache('executive_dashboard')

                # Update business health scores
                health_metrics = dashboard_data.get('business_health', {})
                for category, score in health_metrics.items():
                    if isinstance(score, (int, float)):
                        business_health_score.labels(category=category).set(score * 100)

                # Update financial performance metrics
                financial_data = dashboard_data.get('financial_performance', {})
                if 'revenue_growth' in financial_data:
                    revenue_growth_rate.labels(
                        period='current_month',
                        segment='total'
                    ).set(financial_data['revenue_growth'] * 100)

                # Update KPIs
                kpis = dashboard_data.get('key_performance_indicators', {})
                await self._update_kpi_metrics(kpis)

                # Track collection performance
                collection_time = time.time() - start_time
                self._collection_performance.append(collection_time)

                await asyncio.sleep(self.config.collection_interval_seconds)

            except Exception as e:
                logger.error(f"Error collecting executive KPIs: {e}")
                self._error_counts['executive_kpis'] += 1
                await asyncio.sleep(self.config.collection_interval_seconds)

    async def _collect_customer_journey_metrics(self):
        """Collect customer journey and engagement metrics."""
        while self._is_collecting:
            try:
                start_time = time.time()

                # Get customer analytics
                async with get_async_session() as session:
                    # Simulate customer journey analysis
                    journey_data = await self._analyze_customer_journey_data(session)

                    # Update journey stage metrics
                    for transition, duration_data in journey_data.get('stage_transitions', {}).items():
                        from_stage, to_stage = transition.split('->')
                        avg_duration = duration_data.get('avg_duration_days', 0)

                        customer_journey_stage_duration.labels(
                            from_stage=from_stage,
                            to_stage=to_stage,
                            customer_segment='all'
                        ).observe(avg_duration)

                    # Update touchpoint effectiveness
                    for touchpoint, effectiveness in journey_data.get('touchpoint_effectiveness', {}).items():
                        customer_touchpoint_effectiveness.labels(
                            touchpoint_type=touchpoint,
                            channel='digital',
                            journey_stage='consideration'
                        ).set(effectiveness * 100)

                collection_time = time.time() - start_time
                self._collection_performance.append(collection_time)

                await asyncio.sleep(self.config.collection_interval_seconds * 2)  # Less frequent

            except Exception as e:
                logger.error(f"Error collecting customer journey metrics: {e}")
                self._error_counts['customer_journey'] += 1
                await asyncio.sleep(self.config.collection_interval_seconds)

    async def _collect_rfm_segmentation_metrics(self):
        """Collect RFM segmentation and customer analytics metrics."""
        while self._is_collecting:
            try:
                start_time = time.time()

                # Get customer segmentation data
                if not self._is_cache_valid('rfm_analysis'):
                    # Simulate getting customer data and performing RFM analysis
                    customers = await self._get_customer_data_sample()
                    rfm_analysis = await self.customer_analytics.calculate_advanced_rfm_analysis(customers)
                    self._set_cache('rfm_analysis', rfm_analysis)
                else:
                    rfm_analysis = self._get_cache('rfm_analysis')

                # Update RFM segment distribution
                segment_counts = rfm_analysis.get('rfm_distribution', {})
                for segment, count in segment_counts.items():
                    rfm_segment_distribution.labels(rfm_segment=segment).set(count)

                # Update segment revenue contribution
                segment_characteristics = rfm_analysis.get('segment_characteristics', {})
                for segment, characteristics in segment_characteristics.items():
                    revenue_contribution = characteristics.get('revenue_contribution', 0)
                    rfm_segment_revenue_contribution.labels(rfm_segment=segment).set(revenue_contribution)

                    # Update CLV distribution
                    avg_clv = characteristics.get('avg_revenue_per_customer', 0)
                    rfm_segment_clv.labels(rfm_segment=segment).observe(avg_clv)

                collection_time = time.time() - start_time
                self._collection_performance.append(collection_time)

                await asyncio.sleep(self.config.collection_interval_seconds * 3)  # Even less frequent

            except Exception as e:
                logger.error(f"Error collecting RFM segmentation metrics: {e}")
                self._error_counts['rfm_segmentation'] += 1
                await asyncio.sleep(self.config.collection_interval_seconds)

    async def _collect_predictive_analytics_metrics(self):
        """Collect predictive analytics and forecasting metrics."""
        if not self.config.enable_predictive_metrics:
            return

        while self._is_collecting:
            try:
                start_time = time.time()

                # Get predictive analytics data
                if not self._is_cache_valid('predictive_analytics'):
                    # Simulate getting predictive analytics
                    customers = await self._get_customer_data_sample()
                    clv_predictions = await self.customer_analytics.predict_customer_lifetime_value(customers)
                    self._set_cache('predictive_analytics', clv_predictions)
                else:
                    clv_predictions = self._get_cache('predictive_analytics')

                # Update churn prediction metrics
                model_performance = clv_predictions.get('model_performance', {})
                avg_confidence = model_performance.get('avg_confidence', 0)

                # Simulate churn predictions by risk level
                churn_predictions = {
                    'high': 0.8,
                    'medium': 0.6,
                    'low': 0.3
                }

                for risk_level, confidence in churn_predictions.items():
                    churn_prediction_confidence.labels(
                        risk_level=risk_level,
                        customer_segment='all'
                    ).observe(confidence)

                # Update forecast accuracy metrics
                revenue_forecast_accuracy.labels(
                    forecast_horizon_days='30',
                    model_type='linear_regression'
                ).set(85.5)  # Simulated accuracy

                collection_time = time.time() - start_time
                self._collection_performance.append(collection_time)

                await asyncio.sleep(self.config.collection_interval_seconds * 5)  # Least frequent

            except Exception as e:
                logger.error(f"Error collecting predictive analytics metrics: {e}")
                self._error_counts['predictive_analytics'] += 1
                await asyncio.sleep(self.config.collection_interval_seconds)

    async def _collect_operational_metrics(self):
        """Collect operational excellence metrics."""
        while self._is_collecting:
            try:
                start_time = time.time()

                # Simulate operational metrics collection
                operational_data = await self._get_operational_metrics()

                # Update process efficiency
                for process, efficiency in operational_data.get('process_efficiency', {}).items():
                    process_efficiency_score.labels(
                        process_name=process,
                        department='operations'
                    ).set(efficiency * 100)

                # Update SLA compliance
                for service, compliance in operational_data.get('sla_compliance', {}).items():
                    sla_compliance_rate.labels(
                        service_type=service,
                        sla_tier='standard'
                    ).set(compliance * 100)

                # Update automation coverage
                automation_coverage.labels(
                    process_category='data_processing',
                    automation_type='etl'
                ).set(92.5)  # Simulated coverage

                collection_time = time.time() - start_time
                self._collection_performance.append(collection_time)

                await asyncio.sleep(self.config.collection_interval_seconds * 2)

            except Exception as e:
                logger.error(f"Error collecting operational metrics: {e}")
                self._error_counts['operational'] += 1
                await asyncio.sleep(self.config.collection_interval_seconds)

    async def _collect_quality_and_risk_metrics(self):
        """Collect data quality and business risk metrics."""
        while self._is_collecting:
            try:
                start_time = time.time()

                # Simulate data quality metrics
                quality_data = await self._assess_data_quality()

                for source, quality_scores in quality_data.items():
                    for dimension, score in quality_scores.items():
                        data_quality_score.labels(
                            data_source=source,
                            quality_dimension=dimension
                        ).observe(score)

                # Update risk metrics
                risk_data = {
                    'operational': 0.15,
                    'financial': 0.08,
                    'strategic': 0.12,
                    'compliance': 0.05
                }

                for risk_category, exposure in risk_data.items():
                    business_risk_exposure.labels(
                        risk_category=risk_category,
                        risk_level='medium'
                    ).set(exposure * 100)

                # Update compliance coverage
                compliance_coverage_percentage.labels(
                    regulation_type='GDPR',
                    compliance_area='data_protection'
                ).set(98.5)

                collection_time = time.time() - start_time
                self._collection_performance.append(collection_time)

                await asyncio.sleep(self.config.collection_interval_seconds * 4)

            except Exception as e:
                logger.error(f"Error collecting quality and risk metrics: {e}")
                self._error_counts['quality_risk'] += 1
                await asyncio.sleep(self.config.collection_interval_seconds)

    async def _collect_innovation_metrics(self):
        """Collect innovation and growth metrics."""
        while self._is_collecting:
            try:
                start_time = time.time()

                # Simulate innovation pipeline metrics
                pipeline_value = 2500000  # $2.5M in pipeline
                innovation_pipeline_value.labels(
                    innovation_stage='development',
                    category='product_enhancement'
                ).set(pipeline_value)

                # Time to market metrics
                time_to_market_days.labels(
                    initiative_type='feature_enhancement',
                    complexity='medium'
                ).observe(45)  # 45 days average

                # Feature adoption metrics
                feature_adoption_rate.labels(
                    feature_category='analytics_dashboard',
                    user_segment='power_users'
                ).set(78.5)

                collection_time = time.time() - start_time
                self._collection_performance.append(collection_time)

                await asyncio.sleep(self.config.collection_interval_seconds * 6)

            except Exception as e:
                logger.error(f"Error collecting innovation metrics: {e}")
                self._error_counts['innovation'] += 1
                await asyncio.sleep(self.config.collection_interval_seconds)

    async def _monitor_collection_health(self):
        """Monitor the health of metrics collection."""
        while self._is_collecting:
            try:
                # Calculate average collection performance
                if self._collection_performance:
                    avg_collection_time = sum(self._collection_performance) / len(self._collection_performance)
                    max_collection_time = max(self._collection_performance)

                    logger.info(
                        f"Metrics collection performance - "
                        f"Avg: {avg_collection_time:.3f}s, Max: {max_collection_time:.3f}s"
                    )

                # Log error counts
                total_errors = sum(self._error_counts.values())
                if total_errors > 0:
                    logger.warning(f"Total collection errors: {total_errors}")
                    for error_type, count in self._error_counts.items():
                        if count > 0:
                            logger.warning(f"  {error_type}: {count} errors")

                await asyncio.sleep(300)  # Monitor every 5 minutes

            except Exception as e:
                logger.error(f"Error in collection health monitoring: {e}")
                await asyncio.sleep(300)

    # Helper methods for data collection and caching

    async def _update_kpi_metrics(self, kpis: Dict[str, Any]):
        """Update KPI metrics from dashboard data."""
        revenue_kpis = kpis.get('revenue_kpis', {})
        customer_kpis = kpis.get('customer_kpis', {})

        # Update revenue metrics
        if 'revenue_growth_rate' in revenue_kpis:
            revenue_growth_rate.labels(
                period='quarterly',
                segment='total'
            ).set(revenue_kpis['revenue_growth_rate'] * 100)

    async def _analyze_customer_journey_data(self, session) -> Dict[str, Any]:
        """Analyze customer journey data from database."""
        # Simplified simulation - would integrate with actual journey tracking
        return {
            'stage_transitions': {
                'awareness->consideration': {'avg_duration_days': 7.5},
                'consideration->purchase': {'avg_duration_days': 3.2},
                'purchase->retention': {'avg_duration_days': 30.0}
            },
            'touchpoint_effectiveness': {
                'email': 0.65,
                'social_media': 0.45,
                'website': 0.78,
                'mobile_app': 0.82
            }
        }

    async def _get_customer_data_sample(self) -> List[Any]:
        """Get customer data sample for analytics."""
        # Simplified - would get actual customer data
        return []  # Return empty list for now

    async def _get_operational_metrics(self) -> Dict[str, Any]:
        """Get operational metrics data."""
        return {
            'process_efficiency': {
                'order_processing': 0.925,
                'customer_onboarding': 0.885,
                'data_processing': 0.958
            },
            'sla_compliance': {
                'api_response_time': 0.995,
                'data_freshness': 0.988,
                'system_availability': 0.999
            }
        }

    async def _assess_data_quality(self) -> Dict[str, Dict[str, float]]:
        """Assess data quality across sources."""
        return {
            'sales_data': {
                'accuracy': 98.5,
                'completeness': 97.8,
                'consistency': 96.2
            },
            'customer_data': {
                'accuracy': 99.1,
                'completeness': 94.5,
                'consistency': 98.7
            }
        }

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in self._cache_timestamps:
            return False

        cache_age = datetime.utcnow() - self._cache_timestamps[cache_key]
        return cache_age.total_seconds() < self.config.cache_duration_seconds

    def _set_cache(self, cache_key: str, data: Any):
        """Set cache entry with timestamp."""
        self._metrics_cache[cache_key] = data
        self._cache_timestamps[cache_key] = datetime.utcnow()

    def _get_cache(self, cache_key: str) -> Any:
        """Get cached data."""
        return self._metrics_cache.get(cache_key)

    async def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics collection summary."""
        return {
            'collection_status': 'active' if self._is_collecting else 'stopped',
            'active_tasks': len(self._collection_tasks),
            'cache_entries': len(self._metrics_cache),
            'error_counts': dict(self._error_counts),
            'performance_metrics': {
                'avg_collection_time_seconds': sum(self._collection_performance) / len(self._collection_performance) if self._collection_performance else 0,
                'max_collection_time_seconds': max(self._collection_performance) if self._collection_performance else 0,
                'total_collections': len(self._collection_performance)
            },
            'configuration': {
                'collection_interval_seconds': self.config.collection_interval_seconds,
                'predictive_metrics_enabled': self.config.enable_predictive_metrics,
                'real_time_metrics_enabled': self.config.enable_real_time_metrics
            }
        }


# Global instance
advanced_metrics_collector = AdvancedBusinessMetricsCollector()


# FastAPI endpoint for metrics exposure
async def get_advanced_business_metrics() -> str:
    """Get advanced business metrics in Prometheus format."""
    return generate_latest(advanced_business_registry)


async def start_advanced_metrics_collection():
    """Start advanced business metrics collection."""
    await advanced_metrics_collector.start_collection()


async def stop_advanced_metrics_collection():
    """Stop advanced business metrics collection."""
    await advanced_metrics_collector.stop_collection()


async def get_metrics_collection_status() -> Dict[str, Any]:
    """Get metrics collection status and summary."""
    return await advanced_metrics_collector.get_metrics_summary()


# Export key components
__all__ = [
    'AdvancedBusinessMetricsCollector',
    'advanced_metrics_collector',
    'advanced_business_registry',
    'get_advanced_business_metrics',
    'start_advanced_metrics_collection',
    'stop_advanced_metrics_collection',
    'get_metrics_collection_status'
]