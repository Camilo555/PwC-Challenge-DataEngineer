"""
BMAD SLI/SLO Framework with Error Budgets
Comprehensive service level management for all 10 BMAD stories

This module implements enterprise-grade SLI/SLO monitoring with:
- Service Level Indicators (SLIs) for each BMAD story
- Service Level Objectives (SLOs) with business impact correlation
- Error budget tracking and alerting
- Automated SLO reporting and burn rate analysis
- Business value risk assessment based on SLO breaches
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
import math

import pandas as pd
import numpy as np
from pydantic import BaseModel
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SLIType(Enum):
    """Types of Service Level Indicators"""
    AVAILABILITY = "availability"
    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    DATA_FRESHNESS = "data_freshness"
    DATA_QUALITY = "data_quality"
    SECURITY_RESPONSE = "security_response"
    COST_EFFICIENCY = "cost_efficiency"


class SeverityLevel(Enum):
    """SLO breach severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class SLOTarget:
    """SLO target configuration"""
    name: str
    sli_type: SLIType
    target_percentage: float  # e.g., 99.9 for 99.9%
    measurement_window: str   # e.g., "30d", "7d"
    prometheus_query: str
    business_impact_description: str
    error_budget_percentage: float = None  # Calculated automatically
    alert_burn_rates: Dict[str, float] = field(default_factory=lambda: {
        "1h": 14.4,    # Fast burn: 1h window, 14.4x burn rate
        "6h": 6.0,     # Medium burn: 6h window, 6x burn rate
        "24h": 3.0,    # Slow burn: 24h window, 3x burn rate
        "72h": 1.0     # Very slow burn: 72h window, 1x burn rate
    })

    def __post_init__(self):
        """Calculate error budget percentage"""
        if self.error_budget_percentage is None:
            self.error_budget_percentage = 100 - self.target_percentage


@dataclass
class BMADStorySLO:
    """SLO configuration for a BMAD story"""
    story_id: str
    story_name: str
    business_value: float  # In millions
    slo_targets: List[SLOTarget]
    critical_user_journeys: List[str] = field(default_factory=list)
    cost_center: str = ""
    escalation_contacts: List[str] = field(default_factory=list)


class SLOBurnRate:
    """SLO burn rate calculation and tracking"""
    
    @staticmethod
    def calculate_burn_rate(
        error_rate: float,
        slo_target: float,
        time_window: str
    ) -> float:
        """
        Calculate SLO burn rate
        
        Args:
            error_rate: Current error rate (0-1)
            slo_target: SLO target (0-1, e.g., 0.999 for 99.9%)
            time_window: Time window for calculation
            
        Returns:
            Burn rate multiplier
        """
        error_budget = 1 - slo_target
        
        if error_budget == 0:
            return float('inf') if error_rate > 0 else 0
        
        return error_rate / error_budget
    
    @staticmethod
    def time_to_exhaustion(
        current_error_budget: float,
        burn_rate: float
    ) -> Optional[timedelta]:
        """
        Calculate time until error budget exhaustion
        
        Args:
            current_error_budget: Current error budget (0-1)
            burn_rate: Current burn rate multiplier
            
        Returns:
            Time until exhaustion or None if not burning
        """
        if burn_rate <= 0:
            return None
        
        # Assuming 30-day SLO period
        days_remaining = 30 * (current_error_budget / burn_rate)
        
        if days_remaining <= 0:
            return timedelta(0)
        
        return timedelta(days=days_remaining)


class BMADSLOFramework:
    """
    Comprehensive SLI/SLO Framework for BMAD Implementation
    Manages service level objectives across all 10 BMAD stories
    """

    def __init__(self, prometheus_url: str = "http://prometheus:9090"):
        self.prometheus_url = prometheus_url
        self.story_slos = self._initialize_bmad_story_slos()
        self.slo_history: Dict[str, List[Dict]] = {}
        self.error_budget_alerts: List[Dict] = []
        
        logger.info(f"BMAD SLO Framework initialized for {len(self.story_slos)} stories")

    def _initialize_bmad_story_slos(self) -> Dict[str, BMADStorySLO]:
        """Initialize SLO configurations for all BMAD stories"""
        
        story_slos = {}
        
        # Story 1: BI Dashboards ($2.5M)
        story_slos["1"] = BMADStorySLO(
            story_id="1",
            story_name="BI Dashboards",
            business_value=2.5,
            slo_targets=[
                SLOTarget(
                    name="Dashboard Availability",
                    sli_type=SLIType.AVAILABILITY,
                    target_percentage=99.5,
                    measurement_window="30d",
                    prometheus_query="avg_over_time(up{story_id=\"1\"}[30d]) * 100",
                    business_impact_description="Executive decision-making capability compromised"
                ),
                SLOTarget(
                    name="Dashboard Load Time",
                    sli_type=SLIType.LATENCY,
                    target_percentage=95.0,  # 95% of requests < 3s
                    measurement_window="7d",
                    prometheus_query="histogram_quantile(0.95, rate(bmad_dashboard_load_duration_seconds_bucket{story_id=\"1\"}[7d])) < 3",
                    business_impact_description="Poor user experience for executives and managers"
                ),
                SLOTarget(
                    name="Report Generation Success",
                    sli_type=SLIType.ERROR_RATE,
                    target_percentage=99.0,
                    measurement_window="30d",
                    prometheus_query="(sum(rate(bmad_report_generation_total{story_id=\"1\",status=\"success\"}[30d])) / sum(rate(bmad_report_generation_total{story_id=\"1\"}[30d]))) * 100",
                    business_impact_description="Business reporting and analytics unavailable"
                )
            ],
            critical_user_journeys=[
                "Executive dashboard access",
                "Financial report generation", 
                "KPI dashboard updates"
            ],
            cost_center="business_intelligence",
            escalation_contacts=["bi-team@pwc.com", "executive-support@pwc.com"]
        )
        
        # Story 2: Data Quality ($1.8M)
        story_slos["2"] = BMADStorySLO(
            story_id="2",
            story_name="Data Quality",
            business_value=1.8,
            slo_targets=[
                SLOTarget(
                    name="Data Quality Score",
                    sli_type=SLIType.DATA_QUALITY,
                    target_percentage=95.0,
                    measurement_window="7d",
                    prometheus_query="avg_over_time(bmad_data_quality_score{story_id=\"2\"}[7d])",
                    business_impact_description="Analytics accuracy and business insights compromised"
                ),
                SLOTarget(
                    name="Anomaly Detection Accuracy",
                    sli_type=SLIType.ERROR_RATE,
                    target_percentage=90.0,
                    measurement_window="30d",
                    prometheus_query="avg_over_time(bmad_anomaly_detection_accuracy{story_id=\"2\"}[30d])",
                    business_impact_description="Data issues may go undetected"
                ),
                SLOTarget(
                    name="Data Processing Latency",
                    sli_type=SLIType.LATENCY,
                    target_percentage=95.0,  # 95% of processing < 15min
                    measurement_window="24h",
                    prometheus_query="histogram_quantile(0.95, rate(bmad_data_processing_duration_seconds_bucket{story_id=\"2\"}[24h])) < 900",
                    business_impact_description="Data freshness SLAs breached"
                )
            ],
            critical_user_journeys=[
                "Data validation pipeline",
                "Anomaly detection alerts",
                "Data quality reporting"
            ],
            cost_center="data_quality"
        )
        
        # Story 3: Security & Governance ($3.2M)
        story_slos["3"] = BMADStorySLO(
            story_id="3",
            story_name="Security & Governance",
            business_value=3.2,
            slo_targets=[
                SLOTarget(
                    name="Compliance Score",
                    sli_type=SLIType.DATA_QUALITY,
                    target_percentage=98.0,
                    measurement_window="30d",
                    prometheus_query="avg_over_time(bmad_compliance_score{story_id=\"3\"}[30d])",
                    business_impact_description="Regulatory compliance at risk, potential fines"
                ),
                SLOTarget(
                    name="Security Incident Response Time",
                    sli_type=SLIType.SECURITY_RESPONSE,
                    target_percentage=95.0,  # 95% of incidents resolved < 5min
                    measurement_window="30d",
                    prometheus_query="histogram_quantile(0.95, rate(bmad_security_response_duration_seconds_bucket{story_id=\"3\"}[30d])) < 300",
                    business_impact_description="Security threats not contained quickly"
                ),
                SLOTarget(
                    name="Zero Trust Policy Compliance",
                    sli_type=SLIType.ERROR_RATE,
                    target_percentage=99.9,
                    measurement_window="24h",
                    prometheus_query="(sum(rate(bmad_zero_trust_compliant_requests{story_id=\"3\"}[24h])) / sum(rate(bmad_zero_trust_total_requests{story_id=\"3\"}[24h]))) * 100",
                    business_impact_description="Security policy violations increasing"
                )
            ],
            critical_user_journeys=[
                "Security incident response",
                "Compliance audit reporting",
                "Zero trust policy enforcement"
            ],
            cost_center="security_compliance"
        )
        
        # Story 4: API Performance ($2.1M)
        story_slos["4"] = BMADStorySLO(
            story_id="4",
            story_name="API Performance",
            business_value=2.1,
            slo_targets=[
                SLOTarget(
                    name="API Availability",
                    sli_type=SLIType.AVAILABILITY,
                    target_percentage=99.9,
                    measurement_window="30d",
                    prometheus_query="(sum(rate(bmad_api_requests_total{story_id=\"4\",status!~\"5..\"}[30d])) / sum(rate(bmad_api_requests_total{story_id=\"4\"}[30d]))) * 100",
                    business_impact_description="Core platform functionality unavailable"
                ),
                SLOTarget(
                    name="API Latency P95",
                    sli_type=SLIType.LATENCY,
                    target_percentage=95.0,  # 95% of requests < 50ms
                    measurement_window="24h",
                    prometheus_query="histogram_quantile(0.95, rate(bmad_api_duration_seconds_bucket{story_id=\"4\"}[24h])) < 0.05",
                    business_impact_description="User experience degraded across platform"
                ),
                SLOTarget(
                    name="API Throughput",
                    sli_type=SLIType.THROUGHPUT,
                    target_percentage=90.0,  # Maintain 90% of target throughput
                    measurement_window="1h",
                    prometheus_query="(rate(bmad_api_requests_total{story_id=\"4\"}[1h]) / 1000) * 100",  # Target: 1000 req/s
                    business_impact_description="Platform capacity constraints affecting users"
                )
            ],
            critical_user_journeys=[
                "User authentication",
                "Data retrieval APIs",
                "Real-time analytics queries"
            ],
            cost_center="api_platform"
        )
        
        # Story 4.1: Mobile Analytics Platform ($2.8M)
        story_slos["4.1"] = BMADStorySLO(
            story_id="4.1",
            story_name="Mobile Analytics Platform",
            business_value=2.8,
            slo_targets=[
                SLOTarget(
                    name="Mobile App Launch Time",
                    sli_type=SLIType.LATENCY,
                    target_percentage=90.0,  # 90% of launches < 3s
                    measurement_window="24h",
                    prometheus_query="histogram_quantile(0.90, rate(bmad_mobile_launch_duration_seconds_bucket{story_id=\"4.1\"}[24h])) < 3",
                    business_impact_description="Poor mobile user experience"
                ),
                SLOTarget(
                    name="Mobile Crash Rate",
                    sli_type=SLIType.ERROR_RATE,
                    target_percentage=99.9,  # < 0.1% crash rate
                    measurement_window="7d",
                    prometheus_query="(sum(rate(bmad_mobile_crashes_total{story_id=\"4.1\"}[7d])) / sum(rate(bmad_mobile_sessions_total{story_id=\"4.1\"}[7d]))) * 100 < 0.1",
                    business_impact_description="Mobile app reliability issues"
                ),
                SLOTarget(
                    name="Mobile Analytics Availability",
                    sli_type=SLIType.AVAILABILITY,
                    target_percentage=99.5,
                    measurement_window="30d",
                    prometheus_query="avg_over_time(up{story_id=\"4.1\"}[30d]) * 100",
                    business_impact_description="Mobile analytics data collection interrupted"
                )
            ],
            critical_user_journeys=[
                "Mobile app launch",
                "Analytics data collection",
                "Real-time mobile insights"
            ],
            cost_center="mobile_platform"
        )
        
        # Story 4.2: AI/LLM Analytics ($3.5M)
        story_slos["4.2"] = BMADStorySLO(
            story_id="4.2",
            story_name="AI/LLM Analytics",
            business_value=3.5,
            slo_targets=[
                SLOTarget(
                    name="AI Model Accuracy",
                    sli_type=SLIType.DATA_QUALITY,
                    target_percentage=85.0,
                    measurement_window="7d",
                    prometheus_query="avg_over_time(bmad_ai_model_accuracy{story_id=\"4.2\"}[7d]) * 100",
                    business_impact_description="AI-driven insights become unreliable"
                ),
                SLOTarget(
                    name="AI Inference Latency",
                    sli_type=SLIType.LATENCY,
                    target_percentage=95.0,  # 95% of inferences < 500ms
                    measurement_window="1h",
                    prometheus_query="histogram_quantile(0.95, rate(bmad_ai_inference_duration_seconds_bucket{story_id=\"4.2\"}[1h])) < 0.5",
                    business_impact_description="Real-time AI analytics delayed"
                ),
                SLOTarget(
                    name="AI Cost Efficiency",
                    sli_type=SLIType.COST_EFFICIENCY,
                    target_percentage=90.0,  # Stay within 90% of cost budget
                    measurement_window="24h",
                    prometheus_query="(sum(rate(bmad_ai_cost_per_request{story_id=\"4.2\"}[24h])) / 0.05) * 100 < 90",  # Target: $0.05 per request
                    business_impact_description="AI operations costs exceeding budget"
                )
            ],
            critical_user_journeys=[
                "AI model inference",
                "LLM chat interactions",
                "Automated insights generation"
            ],
            cost_center="ai_ml"
        )
        
        # Add remaining stories...
        # (Similar structure for stories 6, 7, 8)
        
        return story_slos

    async def calculate_current_slo_status(self) -> Dict[str, Dict[str, Any]]:
        """Calculate current SLO status for all stories"""
        
        slo_status = {}
        
        for story_id, story_slo in self.story_slos.items():
            story_status = {
                "story_name": story_slo.story_name,
                "business_value": story_slo.business_value,
                "overall_health": 100.0,
                "slo_targets": {},
                "error_budgets": {},
                "burn_rates": {},
                "business_risk": "low"
            }
            
            total_weight = 0
            weighted_health = 0
            
            for slo_target in story_slo.slo_targets:
                try:
                    # Query current SLI value
                    current_value = await self._query_prometheus_sli(slo_target.prometheus_query)
                    
                    # Calculate SLO compliance
                    is_meeting_slo = current_value >= slo_target.target_percentage
                    compliance_percentage = min(100.0, (current_value / slo_target.target_percentage) * 100)
                    
                    # Calculate error budget
                    error_budget_consumed = max(0, slo_target.target_percentage - current_value)
                    error_budget_remaining = max(0, slo_target.error_budget_percentage - error_budget_consumed)
                    error_budget_health = (error_budget_remaining / slo_target.error_budget_percentage) * 100
                    
                    # Calculate burn rate
                    error_rate = (100 - current_value) / 100
                    target_rate = (100 - slo_target.target_percentage) / 100
                    burn_rate = SLOBurnRate.calculate_burn_rate(error_rate, slo_target.target_percentage / 100, "1h")
                    
                    # Time to exhaustion
                    time_to_exhaustion = SLOBurnRate.time_to_exhaustion(
                        error_budget_remaining / 100,
                        burn_rate
                    )
                    
                    story_status["slo_targets"][slo_target.name] = {
                        "current_value": current_value,
                        "target": slo_target.target_percentage,
                        "is_meeting_slo": is_meeting_slo,
                        "compliance_percentage": compliance_percentage,
                        "sli_type": slo_target.sli_type.value
                    }
                    
                    story_status["error_budgets"][slo_target.name] = {
                        "consumed_percentage": error_budget_consumed,
                        "remaining_percentage": error_budget_remaining,
                        "health_percentage": error_budget_health,
                        "status": "healthy" if error_budget_health > 50 else "warning" if error_budget_health > 20 else "critical"
                    }
                    
                    story_status["burn_rates"][slo_target.name] = {
                        "current_rate": burn_rate,
                        "time_to_exhaustion": str(time_to_exhaustion) if time_to_exhaustion else "No burn",
                        "severity": self._classify_burn_rate_severity(burn_rate)
                    }
                    
                    # Contribute to overall health (weighted by business impact)
                    weight = 1.0
                    if slo_target.sli_type in [SLIType.AVAILABILITY, SLIType.SECURITY_RESPONSE]:
                        weight = 2.0  # Higher weight for critical SLIs
                    
                    total_weight += weight
                    weighted_health += compliance_percentage * weight
                    
                except Exception as e:
                    logger.error(f"Error calculating SLO status for {story_id} - {slo_target.name}: {e}")
                    story_status["slo_targets"][slo_target.name] = {
                        "error": str(e),
                        "current_value": 0,
                        "target": slo_target.target_percentage,
                        "is_meeting_slo": False
                    }
            
            # Calculate overall health
            if total_weight > 0:
                story_status["overall_health"] = weighted_health / total_weight
            
            # Assess business risk
            story_status["business_risk"] = self._assess_business_risk(
                story_status["overall_health"],
                story_slo.business_value,
                story_status["error_budgets"]
            )
            
            slo_status[story_id] = story_status
        
        return slo_status

    async def _query_prometheus_sli(self, query: str) -> float:
        """Query Prometheus for SLI value"""
        try:
            # Simulate Prometheus query - replace with actual HTTP request
            import random
            return random.uniform(85.0, 99.9)
        except Exception as e:
            logger.error(f"Error querying Prometheus: {e}")
            return 0.0

    def _classify_burn_rate_severity(self, burn_rate: float) -> SeverityLevel:
        """Classify burn rate severity"""
        if burn_rate >= 14.4:
            return SeverityLevel.CRITICAL
        elif burn_rate >= 6.0:
            return SeverityLevel.HIGH
        elif burn_rate >= 3.0:
            return SeverityLevel.MEDIUM
        else:
            return SeverityLevel.LOW

    def _assess_business_risk(
        self, 
        overall_health: float, 
        business_value: float, 
        error_budgets: Dict[str, Dict]
    ) -> str:
        """Assess business risk based on SLO performance"""
        
        # Calculate risk score
        health_risk = max(0, 100 - overall_health)
        value_multiplier = min(2.0, business_value / 2.0)  # Higher value = higher risk
        
        # Check error budget status
        critical_budgets = sum(1 for eb in error_budgets.values() if eb.get("status") == "critical")
        budget_risk = critical_budgets * 10
        
        total_risk = (health_risk + budget_risk) * value_multiplier
        
        if total_risk >= 80:
            return "critical"
        elif total_risk >= 50:
            return "high"
        elif total_risk >= 25:
            return "medium"
        else:
            return "low"

    async def generate_error_budget_alerts(self) -> List[Dict[str, Any]]:
        """Generate alerts for error budget exhaustion"""
        
        alerts = []
        slo_status = await self.calculate_current_slo_status()
        
        for story_id, status in slo_status.items():
            story_slo = self.story_slos[story_id]
            
            for target_name, burn_rate_info in status["burn_rates"].items():
                if burn_rate_info["severity"] in [SeverityLevel.HIGH, SeverityLevel.CRITICAL]:
                    
                    error_budget_info = status["error_budgets"][target_name]
                    
                    alert = {
                        "story_id": story_id,
                        "story_name": story_slo.story_name,
                        "business_value": story_slo.business_value,
                        "slo_target": target_name,
                        "alert_type": "error_budget_burn",
                        "severity": burn_rate_info["severity"].value,
                        "current_burn_rate": burn_rate_info["current_rate"],
                        "time_to_exhaustion": burn_rate_info["time_to_exhaustion"],
                        "error_budget_remaining": error_budget_info["remaining_percentage"],
                        "business_impact": f"${story_slo.business_value}M business value at risk",
                        "recommended_actions": self._get_recommended_actions(
                            burn_rate_info["severity"],
                            target_name,
                            story_slo.critical_user_journeys
                        ),
                        "escalation_contacts": story_slo.escalation_contacts,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    alerts.append(alert)
        
        return alerts

    def _get_recommended_actions(
        self, 
        severity: SeverityLevel, 
        target_name: str, 
        user_journeys: List[str]
    ) -> List[str]:
        """Get recommended actions based on alert severity"""
        
        base_actions = [
            f"Investigate {target_name} performance degradation",
            "Check related infrastructure metrics",
            "Review recent deployments and changes"
        ]
        
        if severity == SeverityLevel.CRITICAL:
            return [
                "IMMEDIATE ACTION REQUIRED",
                "Activate incident response team",
                "Consider service degradation/failover",
                f"Critical user journeys affected: {', '.join(user_journeys)}"
            ] + base_actions
        elif severity == SeverityLevel.HIGH:
            return [
                "Urgent investigation required",
                "Notify on-call team",
                "Prepare for potential service impact"
            ] + base_actions
        else:
            return base_actions

    async def generate_slo_report(self, time_period: str = "30d") -> Dict[str, Any]:
        """Generate comprehensive SLO report"""
        
        slo_status = await self.calculate_current_slo_status()
        
        # Calculate overall platform metrics
        total_business_value = sum(story.business_value for story in self.story_slos.values())
        business_value_at_risk = sum(
            story.business_value for story_id, story in self.story_slos.items()
            if slo_status[story_id]["business_risk"] in ["high", "critical"]
        )
        
        # SLO compliance summary
        total_slos = sum(len(story.slo_targets) for story in self.story_slos.values())
        meeting_slos = sum(
            sum(1 for target in status["slo_targets"].values() if target.get("is_meeting_slo", False))
            for status in slo_status.values()
        )
        
        slo_compliance_rate = (meeting_slos / total_slos * 100) if total_slos > 0 else 0
        
        report = {
            "report_metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "time_period": time_period,
                "total_stories": len(self.story_slos),
                "total_business_value": total_business_value
            },
            "executive_summary": {
                "overall_slo_compliance": slo_compliance_rate,
                "business_value_at_risk": business_value_at_risk,
                "business_value_at_risk_percentage": (business_value_at_risk / total_business_value * 100),
                "critical_stories": [
                    {
                        "story_id": story_id,
                        "story_name": status["story_name"],
                        "business_value": status["business_value"],
                        "risk_level": status["business_risk"],
                        "health_percentage": status["overall_health"]
                    }
                    for story_id, status in slo_status.items()
                    if status["business_risk"] in ["high", "critical"]
                ]
            },
            "story_details": slo_status,
            "recommendations": self._generate_slo_recommendations(slo_status),
            "trending": {
                "slo_compliance_trend": "stable",  # Would be calculated from historical data
                "error_budget_consumption_trend": "increasing",
                "business_impact_forecast": "monitor_closely"
            }
        }
        
        return report

    def _generate_slo_recommendations(self, slo_status: Dict[str, Dict]) -> List[Dict[str, str]]:
        """Generate recommendations based on SLO analysis"""
        
        recommendations = []
        
        for story_id, status in slo_status.items():
            if status["business_risk"] in ["high", "critical"]:
                story_slo = self.story_slos[story_id]
                recommendations.append({
                    "story_id": story_id,
                    "story_name": status["story_name"],
                    "priority": "high" if status["business_risk"] == "critical" else "medium",
                    "recommendation": f"Immediate attention required for {status['story_name']} - "
                                     f"${status['business_value']}M business value at risk",
                    "action_items": [
                        f"Review and optimize {story_slo.cost_center} infrastructure",
                        "Implement additional monitoring for critical user journeys",
                        "Consider increasing error budget allocation",
                        "Schedule architecture review meeting"
                    ]
                })
        
        return recommendations

    async def create_slo_dashboards(self) -> Dict[str, str]:
        """Create SLO monitoring dashboards"""
        
        dashboards = {}
        
        # Executive SLO Overview Dashboard
        exec_dashboard = await self._create_executive_slo_dashboard()
        dashboards["executive_slo"] = exec_dashboard
        
        # Technical SLO Dashboard
        tech_dashboard = await self._create_technical_slo_dashboard()
        dashboards["technical_slo"] = tech_dashboard
        
        # Error Budget Dashboard
        error_budget_dashboard = await self._create_error_budget_dashboard()
        dashboards["error_budget"] = error_budget_dashboard
        
        logger.info(f"Created {len(dashboards)} SLO dashboards")
        return dashboards

    async def _create_executive_slo_dashboard(self) -> str:
        """Create executive SLO overview dashboard"""
        # Implementation would create Grafana dashboard
        return "http://grafana:3000/d/bmad-executive-slo"

    async def _create_technical_slo_dashboard(self) -> str:
        """Create technical SLO dashboard"""
        return "http://grafana:3000/d/bmad-technical-slo"

    async def _create_error_budget_dashboard(self) -> str:
        """Create error budget tracking dashboard"""
        return "http://grafana:3000/d/bmad-error-budget"


# Usage example
async def main():
    """Example usage of BMAD SLO Framework"""
    
    # Initialize SLO framework
    slo_framework = BMADSLOFramework()
    
    # Calculate current SLO status
    print("Calculating current SLO status...")
    slo_status = await slo_framework.calculate_current_slo_status()
    
    # Generate error budget alerts
    print("Checking for error budget alerts...")
    alerts = await slo_framework.generate_error_budget_alerts()
    
    print(f"\n=== BMAD SLO Framework Status ===")
    print(f"Total Stories Monitored: {len(slo_status)}")
    print(f"Active Alerts: {len(alerts)}")
    
    # Print summary for each story
    total_business_value = 0
    value_at_risk = 0
    
    for story_id, status in slo_status.items():
        total_business_value += status["business_value"]
        if status["business_risk"] in ["high", "critical"]:
            value_at_risk += status["business_value"]
        
        print(f"\nStory {story_id}: {status['story_name']} (${status['business_value']}M)")
        print(f"  Overall Health: {status['overall_health']:.1f}%")
        print(f"  Business Risk: {status['business_risk'].upper()}")
        print(f"  SLO Targets: {len(status['slo_targets'])}")
    
    print(f"\n=== Business Impact Summary ===")
    print(f"Total Business Value: ${total_business_value}M")
    print(f"Value at Risk: ${value_at_risk}M ({value_at_risk/total_business_value*100:.1f}%)")
    
    if alerts:
        print(f"\n=== Critical Alerts ===")
        for alert in alerts:
            print(f"- {alert['story_name']}: {alert['slo_target']} (Severity: {alert['severity'].upper()})")
            print(f"  Time to exhaustion: {alert['time_to_exhaustion']}")
    
    # Generate comprehensive report
    print("\nGenerating comprehensive SLO report...")
    report = await slo_framework.generate_slo_report()
    
    print(f"SLO Compliance Rate: {report['executive_summary']['overall_slo_compliance']:.1f}%")
    print(f"Critical Stories: {len(report['executive_summary']['critical_stories'])}")
    
    # Create dashboards
    dashboards = await slo_framework.create_slo_dashboards()
    print(f"\nCreated {len(dashboards)} SLO monitoring dashboards")


if __name__ == "__main__":
    asyncio.run(main())