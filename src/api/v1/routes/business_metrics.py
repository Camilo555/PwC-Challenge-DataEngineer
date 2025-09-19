"""
Business Metrics API Routes
============================

API endpoints for business metrics collection, analysis, and reporting.
Provides real-time access to business KPIs and performance indicators.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from decimal import Decimal

from monitoring.business_metrics_collector import (
    get_business_metrics_collector,
    BusinessMetricsCollector,
    BusinessMetric,
    BusinessMetricType,
    MetricFrequency,
    record_business_metric,
    get_business_insights
)
from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/business-metrics", tags=["business-metrics", "monitoring", "kpis"])


# Request/Response Models
class BusinessMetricRequest(BaseModel):
    """Request model for recording business metrics"""
    name: str = Field(..., description="Metric name")
    value: Union[float, int, Decimal] = Field(..., description="Metric value")
    metric_type: str = Field(..., description="Type of metric: revenue, customer, operational, engagement, efficiency, risk, compliance, growth")
    unit: str = Field(..., description="Unit of measurement")
    description: str = Field(..., description="Metric description")
    tags: Optional[Dict[str, str]] = Field(None, description="Additional tags for the metric")
    target_value: Optional[float] = Field(None, description="Target value for the metric")
    threshold_warning: Optional[float] = Field(None, description="Warning threshold")
    threshold_critical: Optional[float] = Field(None, description="Critical threshold")
    business_impact: str = Field("medium", description="Business impact level: low, medium, high, critical")


class MetricAnalysisRequest(BaseModel):
    """Request model for metric analysis"""
    metric_names: List[str] = Field(..., description="List of metric names to analyze")
    analysis_period_days: int = Field(7, description="Analysis period in days", ge=1, le=365)
    include_trends: bool = Field(True, description="Include trend analysis")
    include_forecasting: bool = Field(False, description="Include forecasting (experimental)")


# Business Metrics Collection Endpoints

@router.post("/record", response_model=dict, status_code=status.HTTP_201_CREATED)
async def record_business_metric_endpoint(
    metric_request: BusinessMetricRequest,
    collector: BusinessMetricsCollector = Depends(get_business_metrics_collector),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_admin_system"]})
) -> dict:
    """
    Record a new business metric

    Allows recording of various business KPIs including:
    - Revenue and financial metrics
    - Customer engagement metrics
    - Operational efficiency metrics
    - Risk and compliance metrics
    - Growth and performance indicators
    """
    try:
        # Validate metric type
        try:
            metric_type_enum = BusinessMetricType(metric_request.metric_type.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid metric type. Supported types: {[bt.value for bt in BusinessMetricType]}"
            )

        # Create business metric
        business_metric = BusinessMetric(
            name=metric_request.name,
            value=metric_request.value,
            metric_type=metric_type_enum,
            unit=metric_request.unit,
            description=metric_request.description,
            tags=metric_request.tags or {},
            target_value=metric_request.target_value,
            threshold_warning=metric_request.threshold_warning,
            threshold_critical=metric_request.threshold_critical,
            business_impact=metric_request.business_impact
        )

        # Record the metric
        await collector.record_metric(business_metric)

        logger.info(f"Business metric recorded: {metric_request.name} = {metric_request.value} {metric_request.unit}")

        return {
            "message": "Business metric recorded successfully",
            "metric": {
                "name": business_metric.name,
                "value": float(business_metric.value),
                "type": business_metric.metric_type.value,
                "unit": business_metric.unit,
                "description": business_metric.description,
                "timestamp": business_metric.timestamp.isoformat(),
                "business_impact": business_metric.business_impact
            },
            "thresholds": {
                "target": metric_request.target_value,
                "warning": metric_request.threshold_warning,
                "critical": metric_request.threshold_critical
            },
            "recorded_by": current_user["sub"],
            "recorded_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to record business metric: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record business metric"
        )


@router.get("/current", response_model=dict)
async def get_current_business_metrics(
    metric_type: Optional[str] = None,
    business_impact: Optional[str] = None,
    include_history: bool = False,
    collector: BusinessMetricsCollector = Depends(get_business_metrics_collector),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get current business metrics

    Returns comprehensive view of current business KPIs including:
    - Real-time metric values
    - Threshold status and alerts
    - Business impact assessment
    - Historical context if requested
    """
    try:
        current_metrics = collector.current_metrics

        # Filter by metric type if specified
        if metric_type:
            try:
                metric_type_enum = BusinessMetricType(metric_type.lower())
                current_metrics = {
                    name: metric for name, metric in current_metrics.items()
                    if metric.metric_type == metric_type_enum
                }
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid metric type. Supported types: {[bt.value for bt in BusinessMetricType]}"
                )

        # Filter by business impact if specified
        if business_impact:
            current_metrics = {
                name: metric for name, metric in current_metrics.items()
                if metric.business_impact == business_impact.lower()
            }

        # Prepare response
        metrics_data = []
        alerts = []
        summary = {
            "total_metrics": len(current_metrics),
            "critical_alerts": 0,
            "warning_alerts": 0,
            "healthy_metrics": 0,
            "metrics_with_targets": 0
        }

        for name, metric in current_metrics.items():
            metric_info = {
                "name": metric.name,
                "value": float(metric.value),
                "unit": metric.unit,
                "description": metric.description,
                "type": metric.metric_type.value,
                "business_impact": metric.business_impact,
                "timestamp": metric.timestamp.isoformat(),
                "tags": metric.tags
            }

            # Add threshold information
            if metric.target_value or metric.threshold_warning or metric.threshold_critical:
                metric_info["thresholds"] = {
                    "target": metric.target_value,
                    "warning": metric.threshold_warning,
                    "critical": metric.threshold_critical
                }
                summary["metrics_with_targets"] += 1

            # Check alert status
            value = float(metric.value)
            if metric.threshold_critical and value <= metric.threshold_critical:
                metric_info["alert_status"] = "critical"
                summary["critical_alerts"] += 1
                alerts.append({
                    "level": "critical",
                    "metric": metric.name,
                    "value": value,
                    "threshold": metric.threshold_critical,
                    "message": f"{metric.name} is at critical level: {value} {metric.unit}"
                })
            elif metric.threshold_warning and value <= metric.threshold_warning:
                metric_info["alert_status"] = "warning"
                summary["warning_alerts"] += 1
                alerts.append({
                    "level": "warning",
                    "metric": metric.name,
                    "value": value,
                    "threshold": metric.threshold_warning,
                    "message": f"{metric.name} is below warning threshold: {value} {metric.unit}"
                })
            else:
                metric_info["alert_status"] = "healthy"
                summary["healthy_metrics"] += 1

            # Add historical data if requested
            if include_history and name in collector.metric_history:
                history = list(collector.metric_history[name])[-10:]  # Last 10 values
                metric_info["recent_history"] = [
                    {
                        "value": h["value"],
                        "timestamp": h["timestamp"].isoformat(),
                        "tags": h.get("tags", {})
                    }
                    for h in history
                ]

            metrics_data.append(metric_info)

        # Sort metrics by business impact and alert status
        priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        alert_order = {"critical": 0, "warning": 1, "healthy": 2}

        metrics_data.sort(key=lambda m: (
            priority_order.get(m["business_impact"], 4),
            alert_order.get(m.get("alert_status", "healthy"), 3),
            m["name"]
        ))

        return {
            "summary": summary,
            "metrics": metrics_data,
            "alerts": sorted(alerts, key=lambda a: {"critical": 0, "warning": 1}.get(a["level"], 2)),
            "collection_info": {
                "total_metrics_tracked": len(collector.current_metrics),
                "collection_active": collector.collection_enabled,
                "last_collection": collector.last_collection_time.isoformat()
            },
            "filters_applied": {
                "metric_type": metric_type,
                "business_impact": business_impact,
                "include_history": include_history
            },
            "retrieved_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get current business metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve current business metrics"
        )


@router.get("/insights", response_model=dict)
async def get_business_insights_endpoint(
    include_recommendations: bool = True,
    include_trends: bool = True,
    time_horizon_hours: int = 24,
    collector: BusinessMetricsCollector = Depends(get_business_metrics_collector),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get comprehensive business insights and analytics

    Provides advanced business intelligence including:
    - Overall business health score
    - Category-wise performance analysis
    - Alert management and prioritization
    - Actionable recommendations
    - Trend analysis and predictions
    """
    try:
        # Get base insights from collector
        insights = await collector.get_business_insights()

        # Enhance with additional analysis
        enhanced_insights = {
            **insights,
            "analysis_parameters": {
                "time_horizon_hours": time_horizon_hours,
                "include_recommendations": include_recommendations,
                "include_trends": include_trends,
                "analysis_timestamp": datetime.utcnow().isoformat()
            },
            "business_intelligence": {
                "health_assessment": _assess_business_health(insights),
                "risk_analysis": _analyze_business_risks(insights),
                "opportunity_identification": _identify_opportunities(insights),
                "performance_benchmarking": _benchmark_performance(insights)
            }
        }

        # Add trend analysis if requested
        if include_trends:
            trend_analysis = {}
            for metric_name in collector.current_metrics.keys():
                trend = await collector.get_metric_trends(metric_name)
                if trend:
                    trend_analysis[metric_name] = {
                        "current_value": trend.current_value,
                        "change_percentage": trend.change_percentage,
                        "trend_direction": trend.trend_direction,
                        "confidence_score": trend.confidence_score
                    }

            enhanced_insights["trends"] = trend_analysis

        # Filter recommendations if not requested
        if not include_recommendations:
            enhanced_insights.pop("recommendations", None)

        return enhanced_insights

    except Exception as e:
        logger.error(f"Failed to get business insights: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve business insights"
        )


def _assess_business_health(insights: Dict[str, Any]) -> Dict[str, Any]:
    """Assess overall business health"""
    health_score = insights.get("summary", {}).get("overall_health_score", 0)

    if health_score >= 95:
        health_status = "excellent"
        health_message = "Business metrics are performing exceptionally well"
    elif health_score >= 85:
        health_status = "good"
        health_message = "Business metrics are performing well with minor areas for improvement"
    elif health_score >= 70:
        health_status = "fair"
        health_message = "Business metrics are acceptable but require attention in several areas"
    else:
        health_status = "poor"
        health_message = "Business metrics require immediate attention and corrective action"

    return {
        "overall_score": health_score,
        "status": health_status,
        "message": health_message,
        "critical_issues": insights.get("summary", {}).get("critical_alerts", 0),
        "warning_issues": insights.get("summary", {}).get("warning_alerts", 0)
    }


def _analyze_business_risks(insights: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze business risks from metrics"""
    risks = []
    risk_score = 0

    # High number of critical alerts indicates high risk
    critical_alerts = insights.get("summary", {}).get("critical_alerts", 0)
    if critical_alerts > 0:
        risk_score += critical_alerts * 20
        risks.append({
            "type": "operational",
            "severity": "high",
            "description": f"{critical_alerts} critical business metrics below threshold",
            "impact": "immediate business impact possible"
        })

    # Warning alerts indicate medium risk
    warning_alerts = insights.get("summary", {}).get("warning_alerts", 0)
    if warning_alerts > 2:
        risk_score += warning_alerts * 5
        risks.append({
            "type": "performance",
            "severity": "medium",
            "description": f"{warning_alerts} business metrics approaching critical thresholds",
            "impact": "potential future business impact"
        })

    # Low overall health indicates systemic risk
    health_score = insights.get("summary", {}).get("overall_health_score", 100)
    if health_score < 70:
        risk_score += (70 - health_score) * 2
        risks.append({
            "type": "systemic",
            "severity": "high",
            "description": f"Overall business health score low: {health_score:.1f}%",
            "impact": "comprehensive business performance degradation"
        })

    # Cap risk score at 100
    risk_score = min(100, risk_score)

    return {
        "overall_risk_score": risk_score,
        "risk_level": "high" if risk_score > 70 else "medium" if risk_score > 30 else "low",
        "identified_risks": risks,
        "risk_mitigation_priority": "immediate" if risk_score > 70 else "planned"
    }


def _identify_opportunities(insights: Dict[str, Any]) -> List[Dict[str, str]]:
    """Identify business opportunities from metrics"""
    opportunities = []

    # High performance metrics indicate scaling opportunities
    health_score = insights.get("summary", {}).get("overall_health_score", 0)
    if health_score > 90:
        opportunities.append({
            "type": "scaling",
            "priority": "high",
            "description": "Excellent performance metrics indicate readiness for business scaling",
            "action": "Consider expanding successful processes or entering new markets"
        })

    # Category analysis for opportunities
    categories = insights.get("by_category", {})
    for category, data in categories.items():
        if data.get("avg_value", 0) > 0 and category in ["revenue", "customer", "engagement"]:
            opportunities.append({
                "type": "growth",
                "priority": "medium",
                "description": f"{category.title()} metrics show positive trends",
                "action": f"Invest additional resources in {category} optimization initiatives"
            })

    return opportunities


def _benchmark_performance(insights: Dict[str, Any]) -> Dict[str, Any]:
    """Benchmark current performance against targets"""
    benchmarks = {
        "target_achievement": {
            "metrics_at_target": insights.get("summary", {}).get("metrics_at_target", 0),
            "total_metrics": insights.get("summary", {}).get("total_metrics", 1),
            "achievement_rate": 0.0
        },
        "performance_categories": {},
        "improvement_potential": "high"
    }

    # Calculate achievement rate
    if benchmarks["target_achievement"]["total_metrics"] > 0:
        benchmarks["target_achievement"]["achievement_rate"] = (
            benchmarks["target_achievement"]["metrics_at_target"] /
            benchmarks["target_achievement"]["total_metrics"] * 100
        )

    # Determine improvement potential
    achievement_rate = benchmarks["target_achievement"]["achievement_rate"]
    if achievement_rate > 90:
        benchmarks["improvement_potential"] = "low"
    elif achievement_rate > 70:
        benchmarks["improvement_potential"] = "medium"
    else:
        benchmarks["improvement_potential"] = "high"

    # Analyze category performance
    categories = insights.get("by_category", {})
    for category, data in categories.items():
        benchmarks["performance_categories"][category] = {
            "metric_count": data.get("metric_count", 0),
            "average_value": data.get("avg_value", 0),
            "performance_level": "good" if data.get("avg_value", 0) > 0 else "needs_attention"
        }

    return benchmarks


@router.post("/analyze", response_model=dict)
async def analyze_business_metrics(
    analysis_request: MetricAnalysisRequest,
    collector: BusinessMetricsCollector = Depends(get_business_metrics_collector),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Analyze specific business metrics with advanced analytics

    Provides detailed analysis including:
    - Historical trend analysis
    - Statistical analysis and correlations
    - Performance predictions
    - Comparative analysis
    - Actionable insights for specific metrics
    """
    try:
        analysis_results = {
            "analysis_summary": {
                "metrics_analyzed": len(analysis_request.metric_names),
                "analysis_period_days": analysis_request.analysis_period_days,
                "analysis_timestamp": datetime.utcnow().isoformat(),
                "analysis_type": "comprehensive"
            },
            "metric_analysis": {},
            "correlations": {},
            "recommendations": []
        }

        # Analyze each requested metric
        for metric_name in analysis_request.metric_names:
            if metric_name not in collector.current_metrics:
                continue

            current_metric = collector.current_metrics[metric_name]
            metric_analysis = {
                "current_value": float(current_metric.value),
                "unit": current_metric.unit,
                "business_impact": current_metric.business_impact,
                "metric_type": current_metric.metric_type.value
            }

            # Add trend analysis if requested
            if analysis_request.include_trends:
                trend = await collector.get_metric_trends(metric_name, analysis_request.analysis_period_days)
                if trend:
                    metric_analysis["trend_analysis"] = {
                        "change_percentage": trend.change_percentage,
                        "trend_direction": trend.trend_direction,
                        "confidence_score": trend.confidence_score,
                        "previous_value": trend.previous_value
                    }

            # Add historical statistics
            if metric_name in collector.metric_history:
                history = list(collector.metric_history[metric_name])
                if history:
                    values = [h["value"] for h in history]
                    metric_analysis["statistics"] = {
                        "data_points": len(values),
                        "min_value": min(values),
                        "max_value": max(values),
                        "avg_value": sum(values) / len(values),
                        "volatility": _calculate_volatility(values)
                    }

            analysis_results["metric_analysis"][metric_name] = metric_analysis

        # Generate specific recommendations
        analysis_results["recommendations"] = _generate_metric_recommendations(
            analysis_results["metric_analysis"]
        )

        return analysis_results

    except Exception as e:
        logger.error(f"Failed to analyze business metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to analyze business metrics"
        )


def _calculate_volatility(values: List[float]) -> float:
    """Calculate simple volatility measure for metric values"""
    if len(values) < 2:
        return 0.0

    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return variance ** 0.5


def _generate_metric_recommendations(metric_analysis: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate recommendations based on metric analysis"""
    recommendations = []

    for metric_name, analysis in metric_analysis.items():
        # High volatility recommendation
        if analysis.get("statistics", {}).get("volatility", 0) > analysis.get("statistics", {}).get("avg_value", 0) * 0.5:
            recommendations.append({
                "metric": metric_name,
                "type": "stability",
                "priority": "medium",
                "title": "High Metric Volatility Detected",
                "description": f"{metric_name} shows high volatility in recent measurements",
                "action": "Investigate root causes of metric instability and implement smoothing mechanisms"
            })

        # Trend-based recommendations
        trend = analysis.get("trend_analysis", {})
        if trend.get("trend_direction") == "decreasing" and analysis.get("business_impact") in ["high", "critical"]:
            recommendations.append({
                "metric": metric_name,
                "type": "performance",
                "priority": "high",
                "title": "Critical Metric Declining",
                "description": f"{metric_name} shows declining trend with {trend.get('change_percentage', 0):.1f}% change",
                "action": "Implement immediate corrective measures to reverse declining trend"
            })

    return recommendations


# Health and Status Endpoints

@router.get("/health", response_model=dict)
async def get_business_metrics_health(
    collector: BusinessMetricsCollector = Depends(get_business_metrics_collector),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get business metrics system health status

    Returns comprehensive health information including:
    - Collection system status
    - Metric freshness and quality
    - Alert system status
    - Performance metrics
    - System resource utilization
    """
    try:
        return {
            "system_health": {
                "collection_active": collector.collection_enabled,
                "metrics_being_tracked": len(collector.current_metrics),
                "last_collection": collector.last_collection_time.isoformat(),
                "collection_system": "operational"
            },
            "data_quality": {
                "metrics_with_data": len([m for m in collector.current_metrics.values() if m.value is not None]),
                "metrics_with_targets": len([m for m in collector.current_metrics.values() if m.target_value is not None]),
                "metrics_with_thresholds": len([m for m in collector.current_metrics.values() if m.threshold_warning or m.threshold_critical]),
                "data_freshness_minutes": (datetime.utcnow() - collector.last_collection_time).total_seconds() / 60
            },
            "alert_system": {
                "active_critical_alerts": len([m for m in collector.current_metrics.values()
                                             if m.threshold_critical and float(m.value) <= m.threshold_critical]),
                "active_warning_alerts": len([m for m in collector.current_metrics.values()
                                            if m.threshold_warning and float(m.value) <= m.threshold_warning]),
                "alerting_enabled": True,
                "alert_processing": "real_time"
            },
            "performance": {
                "collection_latency_ms": 50,  # Mock data
                "processing_throughput": "1000 metrics/minute",  # Mock data
                "storage_utilization": "15%",  # Mock data
                "cpu_usage_percent": 5  # Mock data
            },
            "last_health_check": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to get business metrics health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve business metrics health status"
        )