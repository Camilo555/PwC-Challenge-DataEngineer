"""
Business Intelligence Domain Entities
======================================

Advanced business intelligence entities for comprehensive analytics,
cross-domain insights, and predictive analytics.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator


class AnalyticsTimeframe(str, Enum):
    """Analytics timeframe options."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"
    CUSTOM = "custom"


class BusinessMetricType(str, Enum):
    """Types of business metrics."""
    REVENUE = "revenue"
    CUSTOMERS = "customers"
    ORDERS = "orders"
    CONVERSION = "conversion"
    RETENTION = "retention"
    PROFITABILITY = "profitability"
    EFFICIENCY = "efficiency"


class CustomerJourneyStage(str, Enum):
    """Customer journey stages."""
    AWARENESS = "awareness"
    CONSIDERATION = "consideration"
    PURCHASE = "purchase"
    RETENTION = "retention"
    ADVOCACY = "advocacy"
    CHURN = "churn"


class PredictionConfidence(str, Enum):
    """Prediction confidence levels."""
    LOW = "low"           # < 60%
    MEDIUM = "medium"     # 60-80%
    HIGH = "high"         # 80-95%
    VERY_HIGH = "very_high"  # > 95%


@dataclass
class BusinessIntelligenceMetrics:
    """Comprehensive business intelligence metrics."""
    metrics_id: UUID = field(default_factory=uuid4)

    # Time context
    timeframe: AnalyticsTimeframe = AnalyticsTimeframe.MONTHLY
    start_date: datetime = field(default_factory=datetime.utcnow)
    end_date: datetime = field(default_factory=datetime.utcnow)

    # Revenue metrics
    total_revenue: Decimal = Decimal('0.00')
    revenue_growth_rate: float = 0.0
    average_order_value: Decimal = Decimal('0.00')
    revenue_per_customer: Decimal = Decimal('0.00')

    # Customer metrics
    total_customers: int = 0
    new_customers: int = 0
    active_customers: int = 0
    churned_customers: int = 0
    customer_acquisition_rate: float = 0.0
    customer_retention_rate: float = 0.0

    # Operational metrics
    total_orders: int = 0
    items_sold: int = 0
    conversion_rate: float = 0.0
    order_fulfillment_rate: float = 0.0

    # Profitability metrics
    gross_profit: Decimal = Decimal('0.00')
    gross_margin: float = 0.0
    customer_acquisition_cost: Decimal = Decimal('0.00')
    customer_lifetime_value: Decimal = Decimal('0.00')

    # Quality metrics
    customer_satisfaction_score: float = 0.0
    net_promoter_score: float = 0.0
    return_rate: float = 0.0

    # Efficiency metrics
    operational_efficiency: float = 0.0
    cost_per_acquisition: Decimal = Decimal('0.00')
    inventory_turnover: float = 0.0

    # Metadata
    generated_at: datetime = field(default_factory=datetime.utcnow)
    data_quality_score: float = 1.0
    completeness_score: float = 1.0

    def calculate_overall_score(self) -> float:
        """Calculate overall business performance score."""
        scores = []

        # Revenue performance (0-100)
        revenue_score = min(100, (self.revenue_growth_rate * 100) + 50)
        scores.append(revenue_score)

        # Customer performance (0-100)
        customer_score = self.customer_retention_rate * 100
        scores.append(customer_score)

        # Operational performance (0-100)
        operational_score = self.operational_efficiency * 100
        scores.append(operational_score)

        # Profitability performance (0-100)
        profitability_score = min(100, self.gross_margin * 200)  # Assuming 50% is excellent
        scores.append(profitability_score)

        return sum(scores) / len(scores) if scores else 0.0

    def to_executive_summary(self) -> Dict[str, Any]:
        """Generate executive summary of key metrics."""
        return {
            'timeframe': f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}",
            'revenue': {
                'total': float(self.total_revenue),
                'growth_rate': f"{self.revenue_growth_rate:.1%}",
                'per_customer': float(self.revenue_per_customer)
            },
            'customers': {
                'total': self.total_customers,
                'new': self.new_customers,
                'retention_rate': f"{self.customer_retention_rate:.1%}"
            },
            'performance': {
                'overall_score': f"{self.calculate_overall_score():.1f}/100",
                'operational_efficiency': f"{self.operational_efficiency:.1%}",
                'gross_margin': f"{self.gross_margin:.1%}"
            }
        }


@dataclass
class CrossDomainInsights:
    """Cross-domain analytics insights."""
    insight_id: UUID = field(default_factory=uuid4)

    # Customer-Sales correlation
    customer_purchase_patterns: Dict[str, Any] = field(default_factory=dict)
    segmentation_performance: Dict[str, Any] = field(default_factory=dict)
    lifecycle_revenue_analysis: Dict[str, Any] = field(default_factory=dict)

    # Cross-selling insights
    product_affinity_analysis: Dict[str, Any] = field(default_factory=dict)
    upselling_opportunities: List[Dict[str, Any]] = field(default_factory=list)
    bundle_recommendations: List[Dict[str, Any]] = field(default_factory=list)

    # Customer journey insights
    journey_stage_analysis: Dict[str, Any] = field(default_factory=dict)
    touchpoint_effectiveness: Dict[str, Any] = field(default_factory=dict)
    conversion_funnel_analysis: Dict[str, Any] = field(default_factory=dict)

    # Behavioral insights
    purchase_frequency_patterns: Dict[str, Any] = field(default_factory=dict)
    seasonal_behavior_analysis: Dict[str, Any] = field(default_factory=dict)
    channel_preference_analysis: Dict[str, Any] = field(default_factory=dict)

    # Risk and opportunity analysis
    churn_risk_analysis: Dict[str, Any] = field(default_factory=dict)
    growth_opportunity_analysis: Dict[str, Any] = field(default_factory=dict)
    market_expansion_insights: Dict[str, Any] = field(default_factory=dict)

    # Quality and confidence metrics
    insight_confidence: PredictionConfidence = PredictionConfidence.MEDIUM
    data_freshness: datetime = field(default_factory=datetime.utcnow)
    analysis_depth: str = "comprehensive"

    generated_at: datetime = field(default_factory=datetime.utcnow)

    def get_actionable_insights(self) -> List[Dict[str, Any]]:
        """Extract actionable insights for business teams."""
        insights = []

        # High-value customer insights
        if self.segmentation_performance:
            for segment, performance in self.segmentation_performance.items():
                if performance.get('revenue_potential', 0) > 10000:
                    insights.append({
                        'type': 'revenue_opportunity',
                        'segment': segment,
                        'action': f'Focus marketing efforts on {segment} segment',
                        'potential_impact': performance.get('revenue_potential', 0),
                        'priority': 'high'
                    })

        # Churn risk insights
        if self.churn_risk_analysis.get('high_risk_customers', 0) > 0:
            insights.append({
                'type': 'retention_risk',
                'action': 'Implement retention campaigns for at-risk customers',
                'affected_customers': self.churn_risk_analysis['high_risk_customers'],
                'priority': 'high'
            })

        # Cross-selling opportunities
        for opportunity in self.upselling_opportunities:
            if opportunity.get('confidence', 0) > 0.7:
                insights.append({
                    'type': 'upselling_opportunity',
                    'action': f"Promote {opportunity.get('product')} to {opportunity.get('target_segment')}",
                    'confidence': opportunity.get('confidence'),
                    'priority': 'medium'
                })

        return insights


@dataclass
class PredictiveAnalytics:
    """Predictive analytics results and forecasts."""
    prediction_id: UUID = field(default_factory=uuid4)

    # Revenue predictions
    revenue_forecast: Dict[str, Any] = field(default_factory=dict)
    sales_volume_forecast: Dict[str, Any] = field(default_factory=dict)
    seasonal_revenue_prediction: Dict[str, Any] = field(default_factory=dict)

    # Customer predictions
    customer_growth_forecast: Dict[str, Any] = field(default_factory=dict)
    churn_predictions: Dict[str, Any] = field(default_factory=dict)
    lifetime_value_predictions: Dict[str, Any] = field(default_factory=dict)

    # Market predictions
    demand_forecasting: Dict[str, Any] = field(default_factory=dict)
    market_trend_analysis: Dict[str, Any] = field(default_factory=dict)
    competitive_position_forecast: Dict[str, Any] = field(default_factory=dict)

    # Operational predictions
    capacity_planning_forecast: Dict[str, Any] = field(default_factory=dict)
    inventory_optimization_predictions: Dict[str, Any] = field(default_factory=dict)
    resource_requirement_forecast: Dict[str, Any] = field(default_factory=dict)

    # Model performance metrics
    model_accuracy: float = 0.0
    confidence_intervals: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    prediction_horizon_days: int = 30

    # Validation metrics
    backtesting_results: Dict[str, Any] = field(default_factory=dict)
    model_drift_indicators: Dict[str, Any] = field(default_factory=dict)
    feature_importance: Dict[str, float] = field(default_factory=dict)

    generated_at: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)

    def get_high_confidence_predictions(self, min_confidence: float = 0.8) -> Dict[str, Any]:
        """Get predictions with high confidence scores."""
        high_confidence = {}

        # Revenue forecasts
        if self.revenue_forecast.get('confidence', 0) >= min_confidence:
            high_confidence['revenue'] = self.revenue_forecast

        # Customer predictions
        if self.customer_growth_forecast.get('confidence', 0) >= min_confidence:
            high_confidence['customer_growth'] = self.customer_growth_forecast

        # Demand forecasting
        if self.demand_forecasting.get('confidence', 0) >= min_confidence:
            high_confidence['demand'] = self.demand_forecasting

        return high_confidence

    def generate_prediction_summary(self) -> Dict[str, Any]:
        """Generate summary of key predictions."""
        return {
            'revenue_outlook': {
                'predicted_growth': self.revenue_forecast.get('growth_rate', 0),
                'confidence': self.revenue_forecast.get('confidence', 0)
            },
            'customer_outlook': {
                'predicted_new_customers': self.customer_growth_forecast.get('new_customers', 0),
                'churn_risk_level': self.churn_predictions.get('risk_level', 'unknown')
            },
            'business_outlook': {
                'market_trend': self.market_trend_analysis.get('trend', 'stable'),
                'opportunity_score': self.market_trend_analysis.get('opportunity_score', 0)
            },
            'model_confidence': {
                'overall_accuracy': self.model_accuracy,
                'prediction_reliability': 'high' if self.model_accuracy > 0.8 else 'medium'
            }
        }


@dataclass
class PerformanceBenchmark:
    """Performance benchmarking against industry standards."""
    benchmark_id: UUID = field(default_factory=uuid4)

    # Industry context
    industry_sector: str = "retail"
    company_size_category: str = "medium"  # small, medium, large, enterprise
    market_geography: str = "global"

    # Performance comparisons
    revenue_growth_benchmark: float = 0.0  # Industry average
    customer_retention_benchmark: float = 0.0
    operational_efficiency_benchmark: float = 0.0
    profitability_benchmark: float = 0.0

    # Company performance vs benchmark
    revenue_growth_percentile: float = 0.0  # 0-100
    customer_retention_percentile: float = 0.0
    operational_efficiency_percentile: float = 0.0
    profitability_percentile: float = 0.0

    # Competitive positioning
    market_position: str = "unknown"  # leader, challenger, follower, niche
    competitive_advantages: List[str] = field(default_factory=list)
    improvement_opportunities: List[str] = field(default_factory=list)

    # Benchmark data quality
    data_sources: List[str] = field(default_factory=list)
    benchmark_date: datetime = field(default_factory=datetime.utcnow)
    reliability_score: float = 1.0

    def calculate_overall_performance_score(self) -> float:
        """Calculate overall performance score vs industry."""
        percentiles = [
            self.revenue_growth_percentile,
            self.customer_retention_percentile,
            self.operational_efficiency_percentile,
            self.profitability_percentile
        ]

        valid_percentiles = [p for p in percentiles if p > 0]
        return sum(valid_percentiles) / len(valid_percentiles) if valid_percentiles else 0.0

    def get_performance_rating(self) -> str:
        """Get performance rating based on overall score."""
        score = self.calculate_overall_performance_score()

        if score >= 90:
            return "Excellent"
        elif score >= 75:
            return "Above Average"
        elif score >= 50:
            return "Average"
        elif score >= 25:
            return "Below Average"
        else:
            return "Poor"


class BusinessIntelligenceModel(BaseModel):
    """Pydantic model for business intelligence data validation."""

    metrics: BusinessIntelligenceMetrics
    cross_domain_insights: Optional[CrossDomainInsights] = None
    predictive_analytics: Optional[PredictiveAnalytics] = None
    performance_benchmark: Optional[PerformanceBenchmark] = None

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: float(v),
            UUID: lambda v: str(v)
        }

    @validator('metrics')
    def validate_metrics(cls, v):
        """Validate business intelligence metrics."""
        if v.total_revenue < 0:
            raise ValueError("Total revenue cannot be negative")

        if v.customer_retention_rate < 0 or v.customer_retention_rate > 1:
            raise ValueError("Customer retention rate must be between 0 and 1")

        if v.data_quality_score < 0 or v.data_quality_score > 1:
            raise ValueError("Data quality score must be between 0 and 1")

        return v

    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive business intelligence report."""
        report = {
            'executive_summary': self.metrics.to_executive_summary(),
            'detailed_metrics': {
                'revenue_metrics': {
                    'total_revenue': float(self.metrics.total_revenue),
                    'growth_rate': self.metrics.revenue_growth_rate,
                    'average_order_value': float(self.metrics.average_order_value),
                    'revenue_per_customer': float(self.metrics.revenue_per_customer)
                },
                'customer_metrics': {
                    'total_customers': self.metrics.total_customers,
                    'new_customers': self.metrics.new_customers,
                    'retention_rate': self.metrics.customer_retention_rate,
                    'acquisition_cost': float(self.metrics.customer_acquisition_cost)
                },
                'operational_metrics': {
                    'total_orders': self.metrics.total_orders,
                    'conversion_rate': self.metrics.conversion_rate,
                    'operational_efficiency': self.metrics.operational_efficiency
                }
            },
            'performance_score': self.metrics.calculate_overall_score(),
            'data_quality': {
                'quality_score': self.metrics.data_quality_score,
                'completeness_score': self.metrics.completeness_score,
                'generated_at': self.metrics.generated_at.isoformat()
            }
        }

        # Add cross-domain insights if available
        if self.cross_domain_insights:
            report['actionable_insights'] = self.cross_domain_insights.get_actionable_insights()

        # Add predictive analytics if available
        if self.predictive_analytics:
            report['predictions'] = self.predictive_analytics.generate_prediction_summary()

        # Add benchmark comparison if available
        if self.performance_benchmark:
            report['benchmark_comparison'] = {
                'performance_rating': self.performance_benchmark.get_performance_rating(),
                'overall_score': self.performance_benchmark.calculate_overall_performance_score(),
                'market_position': self.performance_benchmark.market_position
            }

        return report


# Export key classes
__all__ = [
    'BusinessIntelligenceMetrics',
    'CrossDomainInsights',
    'PredictiveAnalytics',
    'PerformanceBenchmark',
    'BusinessIntelligenceModel',
    'AnalyticsTimeframe',
    'BusinessMetricType',
    'CustomerJourneyStage',
    'PredictionConfidence'
]