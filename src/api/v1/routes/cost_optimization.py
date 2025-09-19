"""
Cost Optimization API Routes
============================

FastAPI routes for enterprise cost optimization monitoring and management.

Key Features:
- Real-time cost tracking and budget monitoring
- Multi-cloud cost analysis and optimization recommendations
- Automated cost anomaly detection and alerting
- Cost forecasting and trend analysis
- Resource utilization optimization
- Department/project cost allocation and chargeback
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

from core.auth import get_current_user, User
from monitoring.cost_optimization_monitor import cost_monitor, CostSeverity, OptimizationStrategy, CloudProvider

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/cost-optimization", tags=["Cost Optimization"])


# Pydantic Models
class CostMonitoringRequest(BaseModel):
    """Request model for cost monitoring configuration."""
    providers: List[str] = Field(
        default=["aws", "azure", "gcp"],
        description="Cloud providers to monitor"
    )
    budget_alert_thresholds: Dict[str, float] = Field(
        default={"warning": 80.0, "critical": 95.0, "emergency": 100.0},
        description="Budget alert thresholds (percentage)"
    )
    optimization_enabled: bool = Field(True, description="Enable automated optimizations")
    alert_notifications: bool = Field(True, description="Enable alert notifications")

    @validator('providers')
    def validate_providers(cls, v):
        valid_providers = [provider.value for provider in CloudProvider if provider != CloudProvider.MULTI_CLOUD]
        for provider in v:
            if provider not in valid_providers:
                raise ValueError(f'Invalid provider: {provider}. Valid providers: {valid_providers}')
        return v

    @validator('budget_alert_thresholds')
    def validate_thresholds(cls, v):
        required_keys = ["warning", "critical", "emergency"]
        for key in required_keys:
            if key not in v:
                raise ValueError(f'Missing threshold: {key}')
            if not 0 <= v[key] <= 200:
                raise ValueError(f'Threshold {key} must be between 0 and 200')
        return v

    class Config:
        schema_extra = {
            "example": {
                "providers": ["aws", "azure"],
                "budget_alert_thresholds": {
                    "warning": 80.0,
                    "critical": 95.0,
                    "emergency": 100.0
                },
                "optimization_enabled": True,
                "alert_notifications": True
            }
        }


class OptimizationRequest(BaseModel):
    """Request model for cost optimization actions."""
    strategy: str = Field(description="Optimization strategy to apply")
    resource_ids: List[str] = Field(description="Resource IDs to optimize")
    auto_execute: bool = Field(False, description="Automatically execute optimizations")
    dry_run: bool = Field(True, description="Perform dry run analysis only")

    @validator('strategy')
    def validate_strategy(cls, v):
        valid_strategies = [strategy.value for strategy in OptimizationStrategy]
        if v not in valid_strategies:
            raise ValueError(f'Invalid strategy: {v}. Valid strategies: {valid_strategies}')
        return v

    class Config:
        schema_extra = {
            "example": {
                "strategy": "right_sizing",
                "resource_ids": ["aws-ec2-us-east-1", "azure-compute-eastus"],
                "auto_execute": False,
                "dry_run": True
            }
        }


# API Endpoints
@router.on_event("startup")
async def initialize_cost_monitor():
    """Initialize the cost optimization monitor on startup."""
    try:
        await cost_monitor.start_monitoring()
        logger.info("Cost optimization monitor initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize cost optimization monitor: {e}")


@router.post("/start-monitoring", response_model=Dict[str, Any])
async def start_cost_monitoring(
    request: CostMonitoringRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Start comprehensive cost optimization monitoring.

    Initiates real-time cost tracking, budget monitoring, and optimization
    recommendations across multiple cloud providers.
    """
    try:
        logger.info(f"Starting cost monitoring requested by {current_user.id}")

        # Update monitor configuration
        cost_monitor.config.update({
            "providers": request.providers,
            "budget_alert_thresholds": request.budget_alert_thresholds,
            "optimization_enabled": request.optimization_enabled,
            "alert_notifications": request.alert_notifications
        })

        # Update budget thresholds
        cost_monitor.budget_thresholds.update(request.budget_alert_thresholds)

        # Start monitoring if not already running
        if not cost_monitor._running:
            background_tasks.add_task(cost_monitor.start_monitoring)

        return {
            "message": "Cost optimization monitoring started successfully",
            "configuration": {
                "providers": request.providers,
                "budget_thresholds": request.budget_alert_thresholds,
                "optimization_enabled": request.optimization_enabled,
                "alert_notifications": request.alert_notifications
            },
            "initiated_by": current_user.id,
            "timestamp": datetime.utcnow().isoformat(),
            "monitoring_status": "active",
            "estimated_initialization_time": "30-60 seconds"
        }

    except Exception as e:
        logger.error(f"Failed to start cost monitoring: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start cost monitoring"
        )


@router.get("/summary", response_model=Dict[str, Any])
async def get_cost_summary(
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get comprehensive cost optimization summary.

    Returns current cost metrics, budget utilization, optimization opportunities,
    and cost trends across all monitored cloud providers.
    """
    try:
        summary = cost_monitor.get_cost_summary()

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "cost_summary": summary,
            "monitoring_status": "active" if cost_monitor._running else "inactive",
            "data_freshness": "real-time"
        }

    except Exception as e:
        logger.error(f"Failed to get cost summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve cost summary"
        )


@router.get("/recommendations", response_model=Dict[str, Any])
async def get_optimization_recommendations(
    limit: int = Query(20, ge=1, le=100, description="Maximum number of recommendations"),
    strategy: Optional[str] = Query(None, description="Filter by optimization strategy"),
    min_savings: Optional[float] = Query(None, ge=0, description="Minimum savings amount (USD)"),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get cost optimization recommendations.

    Returns prioritized recommendations for cost reduction including
    right-sizing, unused resources, and reserved instance opportunities.
    """
    try:
        recommendations = cost_monitor.get_optimization_recommendations(limit=limit)

        # Apply filters
        if strategy:
            recommendations = [r for r in recommendations if r["strategy"] == strategy]

        if min_savings is not None:
            recommendations = [r for r in recommendations if r["estimated_savings_usd"] >= min_savings]

        # Calculate summary metrics
        total_savings = sum(r["estimated_savings_usd"] for r in recommendations)
        avg_roi = sum(r["roi_percent"] for r in recommendations) / len(recommendations) if recommendations else 0

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "filters_applied": {
                "limit": limit,
                "strategy": strategy,
                "min_savings": min_savings
            },
            "summary": {
                "total_recommendations": len(recommendations),
                "total_potential_savings_usd": total_savings,
                "average_roi_percent": avg_roi
            },
            "recommendations": recommendations,
            "next_steps": [
                "Review high-impact recommendations",
                "Implement low-risk optimizations first",
                "Monitor cost impact after changes"
            ]
        }

    except Exception as e:
        logger.error(f"Failed to get optimization recommendations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve optimization recommendations"
        )


@router.get("/alerts", response_model=Dict[str, Any])
async def get_cost_alerts(
    severity: Optional[str] = Query(None, description="Filter by alert severity"),
    hours: int = Query(24, ge=1, le=168, description="Hours of alert history to retrieve"),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current cost alerts and budget violations.

    Returns active cost alerts, budget threshold violations, and
    cost anomaly notifications with recommended actions.
    """
    try:
        # Validate severity filter
        if severity:
            try:
                severity_enum = CostSeverity(severity)
            except ValueError:
                valid_severities = [s.value for s in CostSeverity]
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid severity: {severity}. Valid values: {valid_severities}"
                )
        else:
            severity_enum = None

        alerts = cost_monitor.get_cost_alerts(severity=severity_enum)

        # Filter by time window
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        filtered_alerts = [
            alert for alert in alerts
            if datetime.fromisoformat(alert["timestamp"]) >= cutoff_time
        ]

        # Calculate alert statistics
        alert_counts = {}
        for alert in filtered_alerts:
            alert_severity = alert["severity"]
            alert_counts[alert_severity] = alert_counts.get(alert_severity, 0) + 1

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "filters_applied": {
                "severity": severity,
                "time_window_hours": hours
            },
            "alert_summary": {
                "total_alerts": len(filtered_alerts),
                "alerts_by_severity": alert_counts,
                "most_common_type": "budget_utilization"  # Simplified
            },
            "alerts": filtered_alerts,
            "recommended_actions": [
                "Address critical alerts immediately",
                "Review budget allocations",
                "Implement cost optimization recommendations"
            ]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get cost alerts: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve cost alerts"
        )


@router.post("/optimize", response_model=Dict[str, Any])
async def apply_cost_optimization(
    request: OptimizationRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Apply cost optimization strategies to specified resources.

    Executes optimization actions such as right-sizing, resource scheduling,
    or unused resource cleanup based on the specified strategy.
    """
    try:
        logger.info(f"Cost optimization requested by {current_user.id}")

        # Validate resource IDs exist
        valid_resources = []
        invalid_resources = []

        for resource_id in request.resource_ids:
            if resource_id in cost_monitor.cost_metrics:
                valid_resources.append(resource_id)
            else:
                invalid_resources.append(resource_id)

        if invalid_resources:
            logger.warning(f"Invalid resource IDs: {invalid_resources}")

        if not valid_resources:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No valid resource IDs found. Invalid: {invalid_resources}"
            )

        # Calculate potential impact
        total_current_cost = sum(
            cost_monitor.cost_metrics[resource_id].current_cost_usd
            for resource_id in valid_resources
        )

        estimated_savings = total_current_cost * 0.2  # Simplified 20% savings estimate

        if request.dry_run:
            # Perform dry run analysis
            return {
                "message": "Dry run optimization analysis completed",
                "dry_run": True,
                "optimization_plan": {
                    "strategy": request.strategy,
                    "target_resources": valid_resources,
                    "invalid_resources": invalid_resources,
                    "estimated_impact": {
                        "current_monthly_cost_usd": total_current_cost,
                        "estimated_savings_usd": estimated_savings,
                        "estimated_new_cost_usd": total_current_cost - estimated_savings,
                        "savings_percentage": (estimated_savings / total_current_cost * 100) if total_current_cost > 0 else 0
                    }
                },
                "next_steps": [
                    "Review optimization plan",
                    "Execute with auto_execute=true when ready",
                    "Monitor cost impact after implementation"
                ],
                "analyzed_by": current_user.id,
                "timestamp": datetime.utcnow().isoformat()
            }

        else:
            # Execute optimization
            if request.auto_execute:
                # Start optimization in background
                background_tasks.add_task(
                    _execute_optimization_background,
                    request.strategy,
                    valid_resources,
                    current_user.id
                )

                return {
                    "message": "Cost optimization execution started",
                    "execution_started": True,
                    "optimization_details": {
                        "strategy": request.strategy,
                        "target_resources": valid_resources,
                        "estimated_savings_usd": estimated_savings,
                        "execution_mode": "background"
                    },
                    "initiated_by": current_user.id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "estimated_completion_time": "5-15 minutes"
                }

            else:
                return {
                    "message": "Optimization plan ready for execution",
                    "ready_for_execution": True,
                    "optimization_plan": {
                        "strategy": request.strategy,
                        "target_resources": valid_resources,
                        "estimated_savings_usd": estimated_savings
                    },
                    "next_steps": [
                        "Set auto_execute=true to proceed with optimization",
                        "Monitor resources during optimization",
                        "Validate cost savings after completion"
                    ],
                    "prepared_by": current_user.id,
                    "timestamp": datetime.utcnow().isoformat()
                }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Cost optimization failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cost optimization failed"
        )


@router.get("/metrics", response_model=Dict[str, Any])
async def get_cost_metrics(
    provider: Optional[str] = Query(None, description="Filter by cloud provider"),
    service: Optional[str] = Query(None, description="Filter by service name"),
    time_window_hours: int = Query(24, ge=1, le=168, description="Time window for metrics"),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get detailed cost metrics and performance data.

    Returns granular cost metrics, utilization data, and trends
    for all monitored cloud resources and services.
    """
    try:
        summary = cost_monitor.get_cost_summary()

        # Apply filters
        filtered_resources = summary["top_cost_resources"]

        if provider:
            filtered_resources = [r for r in filtered_resources if r["provider"] == provider]

        if service:
            filtered_resources = [r for r in filtered_resources if r["service"] == service]

        # Calculate aggregated metrics
        total_filtered_cost = sum(r["monthly_cost_usd"] for r in filtered_resources)
        avg_utilization = sum(r["utilization_percent"] for r in filtered_resources) / len(filtered_resources) if filtered_resources else 0

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "filters_applied": {
                "provider": provider,
                "service": service,
                "time_window_hours": time_window_hours
            },
            "metrics_summary": {
                "total_filtered_cost_usd": total_filtered_cost,
                "average_utilization_percent": avg_utilization,
                "resource_count": len(filtered_resources)
            },
            "cost_breakdown": summary["provider_breakdown"],
            "service_breakdown": summary["service_breakdown"],
            "resource_metrics": filtered_resources,
            "trends": {
                "cost_trend": "stable",  # Simplified
                "utilization_trend": "improving"  # Simplified
            }
        }

    except Exception as e:
        logger.error(f"Failed to get cost metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve cost metrics"
        )


@router.delete("/alerts/{alert_id}", response_model=Dict[str, str])
async def dismiss_cost_alert(
    alert_id: str,
    current_user: User = Depends(get_current_user)
) -> Dict[str, str]:
    """
    Dismiss a specific cost alert.

    Removes the alert from active alerts after user acknowledgment
    or resolution of the underlying cost issue.
    """
    try:
        if alert_id not in cost_monitor.cost_alerts:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Alert {alert_id} not found"
            )

        # Remove the alert
        del cost_monitor.cost_alerts[alert_id]

        logger.info(f"Cost alert {alert_id} dismissed by {current_user.id}")

        return {
            "message": "Cost alert dismissed successfully",
            "alert_id": alert_id,
            "dismissed_by": current_user.id,
            "timestamp": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to dismiss cost alert: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to dismiss cost alert"
        )


# Background task functions
async def _execute_optimization_background(
    strategy: str,
    resource_ids: List[str],
    user_id: str
) -> None:
    """Execute cost optimization in background."""
    try:
        logger.info(f"Starting background optimization: {strategy} for user {user_id}")

        # Simulate optimization execution
        for resource_id in resource_ids:
            logger.info(f"Optimizing resource: {resource_id} with strategy: {strategy}")

            # Simulate optimization work
            await asyncio.sleep(1)

        logger.info(f"Background optimization completed for user {user_id}")

    except Exception as e:
        logger.error(f"Background optimization failed for user {user_id}: {e}")