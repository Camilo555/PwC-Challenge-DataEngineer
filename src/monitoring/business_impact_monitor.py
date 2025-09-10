"""
Business Impact Correlation Monitor for Performance SLA
Correlates API performance with business value impact across BMAD stories and provides real-time ROI analysis
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, NamedTuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import statistics
import json

from core.logging import get_logger
from core.config import get_settings

try:
    from datadog import initialize, statsd
    DATADOG_AVAILABLE = True
except ImportError:
    DATADOG_AVAILABLE = False

logger = get_logger(__name__)
settings = get_settings()


class BusinessStory(NamedTuple):
    """Business story definition with value and SLA requirements."""
    id: str
    name: str
    business_value_usd: float
    target_sla_ms: float
    critical_endpoints: List[str]
    daily_request_volume: int
    revenue_per_request: float
    compliance_requirements: List[str]


@dataclass
class PerformanceImpact:
    """Performance impact measurement for business correlation."""
    story_id: str
    endpoint: str
    timestamp: float
    response_time_ms: float
    request_volume: int
    sla_compliance: bool
    estimated_revenue_impact: float
    user_satisfaction_score: float
    business_continuity_score: float


@dataclass
class BusinessHealthMetrics:
    """Business health metrics derived from performance data."""
    story_id: str
    story_name: str
    business_value: float
    
    # Performance metrics
    current_sla_compliance: float
    avg_response_time_ms: float
    p95_response_time_ms: float
    request_volume_24h: int
    
    # Business impact metrics
    revenue_at_risk_usd: float
    estimated_daily_impact_usd: float
    user_satisfaction_score: float
    business_continuity_score: float
    
    # ROI metrics
    performance_roi_percentage: float
    cost_of_downtime_per_minute: float
    sla_breach_cost_estimate: float
    
    # Trend analysis
    performance_trend_7d: str  # "improving", "stable", "degrading"
    impact_trend_24h: str
    
    @property
    def health_status(self) -> str:
        """Overall health status based on multiple factors."""
        if self.current_sla_compliance >= 95.0 and self.performance_roi_percentage >= 90.0:
            return "healthy"
        elif self.current_sla_compliance >= 90.0 and self.performance_roi_percentage >= 80.0:
            return "warning"
        else:
            return "critical"
    
    @property
    def risk_level(self) -> str:
        """Business risk level assessment."""
        if self.revenue_at_risk_usd < self.business_value * 0.01:  # <1% at risk
            return "low"
        elif self.revenue_at_risk_usd < self.business_value * 0.05:  # <5% at risk
            return "medium"
        else:
            return "high"


class BusinessImpactMonitor:
    """
    Business Impact Correlation Monitor
    
    Features:
    - Real-time business value impact calculation
    - ROI analysis for performance improvements
    - Executive dashboard metrics
    - Stakeholder alerting based on business impact
    - Revenue correlation with performance metrics
    - Compliance monitoring with financial implications
    """
    
    def __init__(self, enable_datadog: bool = True):
        self.enable_datadog = enable_datadog and DATADOG_AVAILABLE
        
        # Business story definitions (BMAD stories with enhanced details)
        self.business_stories = {
            "1": BusinessStory(
                id="1",
                name="BI Dashboards & Executive Analytics",
                business_value_usd=2.5e6,
                target_sla_ms=12.0,  # Stricter for executive visibility
                critical_endpoints=[
                    "/api/v1/sales/analytics",
                    "/api/v1/analytics/executive-dashboard",
                    "/api/v1/sales/revenue-analytics"
                ],
                daily_request_volume=50000,
                revenue_per_request=50.0,  # High value per executive decision
                compliance_requirements=["SOX", "Financial Reporting"]
            ),
            "2": BusinessStory(
                id="2", 
                name="Data Quality & Governance",
                business_value_usd=1.8e6,
                target_sla_ms=15.0,
                critical_endpoints=[
                    "/api/v1/datamart/quality-metrics",
                    "/api/v1/datamart/data-lineage",
                    "/api/v1/datamart/validation-results"
                ],
                daily_request_volume=30000,
                revenue_per_request=60.0,  # High value for data quality
                compliance_requirements=["GDPR", "Data Governance", "Audit"]
            ),
            "3": BusinessStory(
                id="3",
                name="Security & Governance Platform", 
                business_value_usd=3.2e6,
                target_sla_ms=8.0,  # Critical for security
                critical_endpoints=[
                    "/api/v1/security/dashboard",
                    "/api/v1/auth/validate",
                    "/api/v1/security/compliance-status"
                ],
                daily_request_volume=100000,
                revenue_per_request=32.0,  # Security compliance value
                compliance_requirements=["SOX", "PCI-DSS", "GDPR", "HIPAA"]
            ),
            "4": BusinessStory(
                id="4",
                name="High-Performance API Platform",
                business_value_usd=2.1e6,
                target_sla_ms=15.0,
                critical_endpoints=[
                    "/api/v2/analytics/advanced-analytics",
                    "/api/v1/sales/real-time-metrics",
                    "/api/v2/analytics/performance-kpis"
                ],
                daily_request_volume=200000,
                revenue_per_request=10.5,  # Volume-based value
                compliance_requirements=["API Governance"]
            ),
            "4.1": BusinessStory(
                id="4.1",
                name="Mobile Analytics Platform",
                business_value_usd=2.8e6,
                target_sla_ms=10.0,  # Mobile needs faster response
                critical_endpoints=[
                    "/api/v1/mobile/analytics",
                    "/api/v1/mobile/dashboard", 
                    "/api/v1/sales/mobile-optimized"
                ],
                daily_request_volume=150000,
                revenue_per_request=18.7,  # Mobile user engagement value
                compliance_requirements=["Mobile Security", "Data Privacy"]
            ),
            "4.2": BusinessStory(
                id="4.2",
                name="AI/LLM Conversational Analytics",
                business_value_usd=3.5e6,
                target_sla_ms=15.0,
                critical_endpoints=[
                    "/api/v1/ai/conversation-analytics",
                    "/api/v1/llm/insights",
                    "/api/v1/ai/recommendations"
                ],
                daily_request_volume=75000,
                revenue_per_request=46.7,  # High-value AI insights
                compliance_requirements=["AI Ethics", "Data Privacy"]
            ),
            "6": BusinessStory(
                id="6",
                name="Advanced Security Operations",
                business_value_usd=4.2e6,
                target_sla_ms=5.0,  # Ultra-fast for security
                critical_endpoints=[
                    "/api/v1/security/threat-detection",
                    "/api/v1/security/incident-response",
                    "/api/v1/security/real-time-monitoring"
                ],
                daily_request_volume=120000,
                revenue_per_request=35.0,  # Security incident prevention
                compliance_requirements=["Zero Trust", "SOC2", "ISO27001"]
            ),
            "7": BusinessStory(
                id="7", 
                name="Real-time Streaming Analytics",
                business_value_usd=3.4e6,
                target_sla_ms=8.0,  # Real-time requires low latency
                critical_endpoints=[
                    "/api/v1/streaming/real-time-data",
                    "/api/v1/streaming/live-dashboard",
                    "/api/v1/streaming/event-processing"
                ],
                daily_request_volume=500000,  # High volume real-time
                revenue_per_request=6.8,  # Value from real-time decisions
                compliance_requirements=["Real-time Governance"]
            ),
            "8": BusinessStory(
                id="8",
                name="ML Operations & Model Serving", 
                business_value_usd=6.4e6,
                target_sla_ms=12.0,  # ML inference performance
                critical_endpoints=[
                    "/api/v1/ml/predictions",
                    "/api/v1/ml/model-serving",
                    "/api/v1/ml/feature-engineering"
                ],
                daily_request_volume=300000,
                revenue_per_request=21.3,  # ML-driven business value
                compliance_requirements=["Model Governance", "AI/ML Compliance"]
            )
        }
        
        # Impact tracking storage
        self.performance_impacts: deque = deque(maxlen=10000)
        self.business_health_cache: Dict[str, BusinessHealthMetrics] = {}
        self.impact_calculations_cache: Dict[str, Dict] = {}
        
        # Real-time monitoring
        self.monitoring_active = False
        self.alert_thresholds = {
            'revenue_at_risk_threshold': 100000,  # $100K
            'sla_compliance_threshold': 90.0,     # 90%
            'business_continuity_threshold': 85.0  # 85%
        }
        
        if self.enable_datadog:
            self._initialize_datadog()
        
        logger.info("Business Impact Monitor initialized for BMAD story correlation")
    
    def _initialize_datadog(self):
        """Initialize DataDog integration for business metrics."""
        try:
            if not DATADOG_AVAILABLE:
                logger.warning("DataDog not available for business impact monitoring")
                return
            
            # DataDog should already be initialized by other components
            logger.info("DataDog integration ready for business impact metrics")
        except Exception as e:
            logger.warning(f"Failed to initialize DataDog for business monitoring: {e}")
            self.enable_datadog = False
    
    def record_performance_impact(self, 
                                story_id: str,
                                endpoint: str,
                                response_time_ms: float,
                                request_volume: int = 1,
                                status_code: int = 200) -> PerformanceImpact:
        """Record performance measurement and calculate business impact."""
        
        story = self.business_stories.get(story_id)
        if not story:
            logger.warning(f"Unknown business story ID: {story_id}")
            return None
        
        # Calculate SLA compliance
        sla_compliance = response_time_ms <= story.target_sla_ms and 200 <= status_code < 300
        
        # Calculate revenue impact
        estimated_revenue_impact = self._calculate_revenue_impact(
            story, response_time_ms, request_volume, sla_compliance
        )
        
        # Calculate user satisfaction score
        user_satisfaction_score = self._calculate_user_satisfaction(
            story, response_time_ms, sla_compliance
        )
        
        # Calculate business continuity score
        business_continuity_score = self._calculate_business_continuity(
            story, response_time_ms, sla_compliance
        )
        
        # Create impact measurement
        impact = PerformanceImpact(
            story_id=story_id,
            endpoint=endpoint,
            timestamp=time.time(),
            response_time_ms=response_time_ms,
            request_volume=request_volume,
            sla_compliance=sla_compliance,
            estimated_revenue_impact=estimated_revenue_impact,
            user_satisfaction_score=user_satisfaction_score,
            business_continuity_score=business_continuity_score
        )
        
        # Store impact measurement
        self.performance_impacts.append(impact)
        
        # Send to DataDog if enabled
        if self.enable_datadog:
            self._send_business_metrics(impact, story)
        
        # Update business health cache
        self._update_business_health_cache(story_id)
        
        # Check for immediate alerts
        self._check_impact_alerts(impact, story)
        
        return impact
    
    def _calculate_revenue_impact(self, 
                                story: BusinessStory, 
                                response_time_ms: float,
                                request_volume: int,
                                sla_compliance: bool) -> float:
        """Calculate estimated revenue impact of performance."""
        
        base_revenue = story.revenue_per_request * request_volume
        
        if sla_compliance:
            # No negative impact for SLA-compliant requests
            return 0.0
        
        # Calculate impact based on how much SLA was exceeded
        sla_breach_factor = (response_time_ms - story.target_sla_ms) / story.target_sla_ms
        
        # Impact increases exponentially with breach severity
        impact_multiplier = min(0.5, sla_breach_factor * 0.1)  # Max 50% impact per request
        
        # Additional impact for critical stories
        if story.id in ["3", "6"]:  # Security stories
            impact_multiplier *= 1.5
        elif story.id in ["7"]:  # Real-time stories
            impact_multiplier *= 1.3
        
        return base_revenue * impact_multiplier
    
    def _calculate_user_satisfaction(self, 
                                   story: BusinessStory,
                                   response_time_ms: float,
                                   sla_compliance: bool) -> float:
        """Calculate user satisfaction score (0-100) based on performance."""
        
        if response_time_ms <= story.target_sla_ms / 2:
            # Excellent performance - users very satisfied
            return 100.0
        elif response_time_ms <= story.target_sla_ms:
            # Good performance - users satisfied
            return 90.0 - (response_time_ms / story.target_sla_ms * 10)
        else:
            # Poor performance - satisfaction drops rapidly
            breach_factor = (response_time_ms - story.target_sla_ms) / story.target_sla_ms
            satisfaction = max(30.0, 85.0 - (breach_factor * 30))
            return satisfaction
    
    def _calculate_business_continuity(self,
                                     story: BusinessStory,
                                     response_time_ms: float,
                                     sla_compliance: bool) -> float:
        """Calculate business continuity score (0-100)."""
        
        if sla_compliance:
            return 100.0
        
        # Business continuity degrades with performance
        if response_time_ms > story.target_sla_ms * 3:
            # Severe degradation
            return 40.0
        elif response_time_ms > story.target_sla_ms * 2:
            # Moderate degradation  
            return 70.0
        else:
            # Minor degradation
            return 85.0
    
    def _send_business_metrics(self, impact: PerformanceImpact, story: BusinessStory):
        """Send business impact metrics to DataDog."""
        try:
            tags = [
                f"story_id:{story.id}",
                f"story_name:{story.name}",
                f"endpoint:{impact.endpoint}",
                f"sla_compliant:{impact.sla_compliance}",
                f"environment:{getattr(settings, 'environment', 'development')}"
            ]
            
            # Performance correlation metrics
            statsd.histogram('business.response_time_ms', impact.response_time_ms, tags=tags)
            statsd.gauge('business.user_satisfaction_score', impact.user_satisfaction_score, tags=tags)
            statsd.gauge('business.continuity_score', impact.business_continuity_score, tags=tags)
            
            # Financial impact metrics
            statsd.gauge('business.revenue_impact_usd', impact.estimated_revenue_impact, tags=tags)
            statsd.gauge('business.story_value_usd', story.business_value_usd, tags=tags)
            
            # Business KPIs
            if impact.sla_compliance:
                statsd.increment('business.sla_compliant_requests', tags=tags)
            else:
                statsd.increment('business.sla_violations', tags=tags)
                statsd.gauge('business.sla_breach_severity', 
                           (impact.response_time_ms - story.target_sla_ms) / story.target_sla_ms, 
                           tags=tags)
            
        except Exception as e:
            logger.warning(f"Failed to send business metrics to DataDog: {e}")
    
    def _update_business_health_cache(self, story_id: str):
        """Update cached business health metrics for a story."""
        story = self.business_stories.get(story_id)
        if not story:
            return
        
        # Get recent impacts for this story (last hour)
        one_hour_ago = time.time() - 3600
        recent_impacts = [
            i for i in self.performance_impacts 
            if i.story_id == story_id and i.timestamp > one_hour_ago
        ]
        
        if not recent_impacts:
            return
        
        # Calculate current metrics
        response_times = [i.response_time_ms for i in recent_impacts]
        sla_compliant_count = sum(1 for i in recent_impacts if i.sla_compliance)
        
        current_sla_compliance = (sla_compliant_count / len(recent_impacts)) * 100
        avg_response_time_ms = statistics.mean(response_times)
        response_times.sort()
        p95_response_time_ms = response_times[int(len(response_times) * 0.95)]
        
        # Calculate business impact
        revenue_at_risk = sum(i.estimated_revenue_impact for i in recent_impacts)
        daily_impact_estimate = revenue_at_risk * 24  # Extrapolate to daily
        
        avg_user_satisfaction = statistics.mean([i.user_satisfaction_score for i in recent_impacts])
        avg_business_continuity = statistics.mean([i.business_continuity_score for i in recent_impacts])
        
        # Calculate ROI metrics
        performance_roi = self._calculate_performance_roi(story, current_sla_compliance)
        downtime_cost_per_minute = story.business_value_usd / (365 * 24 * 60) * 0.01  # 1% of annual value
        sla_breach_cost = revenue_at_risk
        
        # Trend analysis
        performance_trend = self._analyze_performance_trend(story_id)
        impact_trend = self._analyze_impact_trend(story_id)
        
        # Create health metrics
        health_metrics = BusinessHealthMetrics(
            story_id=story_id,
            story_name=story.name,
            business_value=story.business_value_usd,
            current_sla_compliance=current_sla_compliance,
            avg_response_time_ms=avg_response_time_ms,
            p95_response_time_ms=p95_response_time_ms,
            request_volume_24h=len(recent_impacts) * 24,  # Estimate daily volume
            revenue_at_risk_usd=revenue_at_risk,
            estimated_daily_impact_usd=daily_impact_estimate,
            user_satisfaction_score=avg_user_satisfaction,
            business_continuity_score=avg_business_continuity,
            performance_roi_percentage=performance_roi,
            cost_of_downtime_per_minute=downtime_cost_per_minute,
            sla_breach_cost_estimate=sla_breach_cost,
            performance_trend_7d=performance_trend,
            impact_trend_24h=impact_trend
        )
        
        self.business_health_cache[story_id] = health_metrics
        
        # Send aggregated metrics to DataDog
        if self.enable_datadog:
            self._send_health_metrics(health_metrics)
    
    def _calculate_performance_roi(self, story: BusinessStory, sla_compliance: float) -> float:
        """Calculate ROI percentage for performance investment."""
        # ROI based on SLA compliance and business value protection
        baseline_roi = 70.0  # Baseline ROI assumption
        compliance_bonus = (sla_compliance - 90.0) * 2  # 2% bonus per % above 90%
        
        # High-value stories get higher ROI
        if story.business_value_usd > 4e6:
            baseline_roi += 10.0
        elif story.business_value_usd > 2e6:
            baseline_roi += 5.0
        
        return min(100.0, baseline_roi + compliance_bonus)
    
    def _analyze_performance_trend(self, story_id: str) -> str:
        """Analyze 7-day performance trend."""
        seven_days_ago = time.time() - (7 * 24 * 3600)
        recent_impacts = [
            i for i in self.performance_impacts
            if i.story_id == story_id and i.timestamp > seven_days_ago
        ]
        
        if len(recent_impacts) < 10:
            return "insufficient_data"
        
        # Split into two periods for comparison
        mid_point = len(recent_impacts) // 2
        first_half = recent_impacts[:mid_point]
        second_half = recent_impacts[mid_point:]
        
        first_avg = statistics.mean([i.response_time_ms for i in first_half])
        second_avg = statistics.mean([i.response_time_ms for i in second_half])
        
        change_percent = ((second_avg - first_avg) / first_avg) * 100
        
        if change_percent < -5:
            return "improving"
        elif change_percent > 5:
            return "degrading"
        else:
            return "stable"
    
    def _analyze_impact_trend(self, story_id: str) -> str:
        """Analyze 24-hour business impact trend."""
        twenty_four_hours_ago = time.time() - (24 * 3600)
        recent_impacts = [
            i for i in self.performance_impacts
            if i.story_id == story_id and i.timestamp > twenty_four_hours_ago
        ]
        
        if len(recent_impacts) < 5:
            return "insufficient_data"
        
        # Compare first and last quarters
        quarter_size = len(recent_impacts) // 4
        first_quarter = recent_impacts[:quarter_size]
        last_quarter = recent_impacts[-quarter_size:]
        
        first_impact = sum(i.estimated_revenue_impact for i in first_quarter)
        last_impact = sum(i.estimated_revenue_impact for i in last_quarter)
        
        if last_impact < first_impact * 0.8:
            return "improving"
        elif last_impact > first_impact * 1.2:
            return "degrading" 
        else:
            return "stable"
    
    def _send_health_metrics(self, health: BusinessHealthMetrics):
        """Send business health metrics to DataDog."""
        try:
            tags = [
                f"story_id:{health.story_id}",
                f"story_name:{health.story_name}",
                f"health_status:{health.health_status}",
                f"risk_level:{health.risk_level}",
                f"environment:{getattr(settings, 'environment', 'development')}"
            ]
            
            # Business health KPIs
            statsd.gauge('business.health.sla_compliance', health.current_sla_compliance, tags=tags)
            statsd.gauge('business.health.user_satisfaction', health.user_satisfaction_score, tags=tags)
            statsd.gauge('business.health.continuity_score', health.business_continuity_score, tags=tags)
            statsd.gauge('business.health.performance_roi', health.performance_roi_percentage, tags=tags)
            
            # Financial impact KPIs
            statsd.gauge('business.finance.revenue_at_risk', health.revenue_at_risk_usd, tags=tags)
            statsd.gauge('business.finance.daily_impact', health.estimated_daily_impact_usd, tags=tags)
            statsd.gauge('business.finance.downtime_cost_per_minute', health.cost_of_downtime_per_minute, tags=tags)
            
            # Performance KPIs
            statsd.gauge('business.performance.avg_response_time', health.avg_response_time_ms, tags=tags)
            statsd.gauge('business.performance.p95_response_time', health.p95_response_time_ms, tags=tags)
            statsd.gauge('business.performance.request_volume_24h', health.request_volume_24h, tags=tags)
            
        except Exception as e:
            logger.warning(f"Failed to send health metrics to DataDog: {e}")
    
    def _check_impact_alerts(self, impact: PerformanceImpact, story: BusinessStory):
        """Check if impact warrants immediate alerting."""
        
        # High revenue impact alert
        if impact.estimated_revenue_impact > self.alert_thresholds['revenue_at_risk_threshold']:
            logger.warning(f"HIGH REVENUE IMPACT ALERT: ${impact.estimated_revenue_impact:,.2f} "
                         f"at risk for story {story.name} ({impact.endpoint})")
            
            if self.enable_datadog:
                statsd.increment('business.alerts.high_revenue_impact', 
                               tags=[f"story_id:{story.id}", f"endpoint:{impact.endpoint}"])
        
        # Business continuity alert
        if impact.business_continuity_score < self.alert_thresholds['business_continuity_threshold']:
            logger.warning(f"BUSINESS CONTINUITY ALERT: Score {impact.business_continuity_score:.1f} "
                         f"for story {story.name} ({impact.endpoint})")
            
            if self.enable_datadog:
                statsd.increment('business.alerts.continuity_risk',
                               tags=[f"story_id:{story.id}", f"endpoint:{impact.endpoint}"])
    
    def get_executive_dashboard_data(self) -> Dict[str, Any]:
        """Get executive-level business impact dashboard data."""
        
        # Overall platform metrics
        total_business_value = sum(story.business_value_usd for story in self.business_stories.values())
        total_revenue_at_risk = sum(
            health.revenue_at_risk_usd 
            for health in self.business_health_cache.values()
        )
        
        # Platform health overview
        healthy_stories = sum(
            1 for health in self.business_health_cache.values() 
            if health.health_status == "healthy"
        )
        
        critical_stories = [
            health for health in self.business_health_cache.values()
            if health.health_status == "critical"
        ]
        
        # ROI analysis
        overall_roi = statistics.mean([
            health.performance_roi_percentage 
            for health in self.business_health_cache.values()
        ]) if self.business_health_cache else 0.0
        
        # Performance summary
        overall_sla_compliance = statistics.mean([
            health.current_sla_compliance
            for health in self.business_health_cache.values()
        ]) if self.business_health_cache else 0.0
        
        return {
            "executive_summary": {
                "total_business_value_protected": total_business_value,
                "current_revenue_at_risk": total_revenue_at_risk,
                "overall_platform_health": "healthy" if len(critical_stories) == 0 else "at_risk",
                "overall_sla_compliance": overall_sla_compliance,
                "overall_roi_percentage": overall_roi,
                "stories_healthy": healthy_stories,
                "stories_total": len(self.business_stories),
                "critical_stories_count": len(critical_stories)
            },
            "story_performance": [asdict(health) for health in self.business_health_cache.values()],
            "critical_alerts": [
                {
                    "story_name": health.story_name,
                    "issue": f"Health status: {health.health_status}",
                    "impact": f"${health.revenue_at_risk_usd:,.2f} at risk",
                    "action_required": True
                }
                for health in critical_stories
            ],
            "kpi_targets": {
                "sla_compliance_target": 95.0,
                "roi_target": 85.0,
                "revenue_protection_target": 99.0,
                "user_satisfaction_target": 90.0
            },
            "recommendations": self._generate_executive_recommendations()
        }
    
    def _generate_executive_recommendations(self) -> List[str]:
        """Generate executive-level recommendations based on current performance."""
        recommendations = []
        
        if not self.business_health_cache:
            recommendations.append("📊 Insufficient performance data - recommend extended monitoring period")
            return recommendations
        
        overall_sla = statistics.mean([h.current_sla_compliance for h in self.business_health_cache.values()])
        total_at_risk = sum(h.revenue_at_risk_usd for h in self.business_health_cache.values())
        
        if overall_sla >= 95.0:
            recommendations.append("✅ Excellent platform performance - SLA targets consistently met")
            recommendations.append("💡 Consider expanding platform capabilities with current performance foundation")
        elif overall_sla >= 90.0:
            recommendations.append("⚠️  Platform approaching SLA targets - proactive optimization recommended")
            recommendations.append("🔧 Focus on performance tuning for critical business stories")
        else:
            recommendations.append("🚨 Platform SLA compliance below targets - immediate attention required")
            recommendations.append("🏗️  Consider infrastructure scaling and optimization investments")
        
        if total_at_risk > 500000:  # $500K at risk
            recommendations.append(f"💰 Significant business value at risk (${total_at_risk:,.2f}) - prioritize performance improvements")
        
        # Story-specific recommendations
        critical_stories = [h for h in self.business_health_cache.values() if h.health_status == "critical"]
        if critical_stories:
            for story_health in critical_stories[:3]:  # Top 3 critical
                recommendations.append(f"🎯 Priority: {story_health.story_name} requires immediate optimization")
        
        return recommendations
    
    def get_story_performance_report(self, story_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed performance report for a specific business story."""
        story = self.business_stories.get(story_id)
        health = self.business_health_cache.get(story_id)
        
        if not story or not health:
            return None
        
        # Get recent impact data
        one_hour_ago = time.time() - 3600
        recent_impacts = [
            i for i in self.performance_impacts
            if i.story_id == story_id and i.timestamp > one_hour_ago
        ]
        
        return {
            "story_info": {
                "id": story.id,
                "name": story.name,
                "business_value": story.business_value_usd,
                "target_sla_ms": story.target_sla_ms,
                "critical_endpoints": story.critical_endpoints,
                "compliance_requirements": story.compliance_requirements
            },
            "current_health": asdict(health),
            "performance_analysis": {
                "recent_request_count": len(recent_impacts),
                "sla_violations_count": sum(1 for i in recent_impacts if not i.sla_compliance),
                "worst_endpoint_performance": max(recent_impacts, key=lambda i: i.response_time_ms).endpoint if recent_impacts else None,
                "best_endpoint_performance": min(recent_impacts, key=lambda i: i.response_time_ms).endpoint if recent_impacts else None
            },
            "business_impact_details": {
                "hourly_revenue_at_risk": sum(i.estimated_revenue_impact for i in recent_impacts),
                "projected_daily_impact": sum(i.estimated_revenue_impact for i in recent_impacts) * 24,
                "user_satisfaction_trend": health.performance_trend_7d,
                "business_continuity_risk": "high" if health.business_continuity_score < 70 else "low"
            },
            "recommendations": self._generate_story_recommendations(story, health)
        }
    
    def _generate_story_recommendations(self, story: BusinessStory, health: BusinessHealthMetrics) -> List[str]:
        """Generate story-specific recommendations."""
        recommendations = []
        
        if health.current_sla_compliance >= 95.0:
            recommendations.append("✅ Story meeting SLA targets consistently")
        else:
            recommendations.append(f"🎯 SLA compliance at {health.current_sla_compliance:.1f}% - target: 95%+")
            recommendations.append("🔧 Recommend performance optimization for critical endpoints")
        
        if health.p95_response_time_ms > story.target_sla_ms:
            recommendations.append(f"⚡ P95 response time ({health.p95_response_time_ms:.1f}ms) exceeds target ({story.target_sla_ms}ms)")
        
        if health.revenue_at_risk_usd > 50000:  # $50K at risk
            recommendations.append(f"💰 Revenue at risk: ${health.revenue_at_risk_usd:,.2f} - prioritize optimization")
        
        if health.performance_trend_7d == "degrading":
            recommendations.append("📉 Performance trending downward - investigate recent changes")
        elif health.performance_trend_7d == "improving":
            recommendations.append("📈 Performance improving - continue current optimizations")
        
        return recommendations


# Global monitor instance
_global_business_monitor: Optional[BusinessImpactMonitor] = None


def get_business_impact_monitor() -> BusinessImpactMonitor:
    """Get or create global business impact monitor instance."""
    global _global_business_monitor
    
    if _global_business_monitor is None:
        _global_business_monitor = BusinessImpactMonitor(enable_datadog=True)
    
    return _global_business_monitor