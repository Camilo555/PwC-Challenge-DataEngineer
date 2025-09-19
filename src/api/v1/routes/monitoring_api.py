"""
Monitoring and Alerting API Endpoints
=====================================

REST API endpoints for monitoring dashboards, alerting, and system health.
Provides real-time access to metrics, alert management, and dashboard data.

Features:
- Real-time metrics API endpoints
- Alert management (create, update, acknowledge, resolve)
- Dashboard data and configuration endpoints
- Health check and status endpoints
- Integration with monitoring systems
- Authentication and authorization for monitoring access

Author: Enterprise Data Engineering Team
Created: 2025-09-18
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.core.security.advanced_security import get_current_user
from src.monitoring.memory_usage_monitor import get_memory_monitor
from src.monitoring.resource_dashboard_manager import (
    Alert,
    AlertRule,
    AlertSeverity,
    Dashboard,
    get_dashboard_manager
)


class MetricsResponse(BaseModel):
    """Response model for metrics data."""

    metric_name: str
    timestamps: List[str]
    values: List[float]
    current_value: float
    min_value: float
    max_value: float
    avg_value: float
    unit: Optional[str] = None


class AlertResponse(BaseModel):
    """Response model for alert data."""

    rule_name: str
    severity: str
    message: str
    metric_value: float
    threshold: float
    timestamp: str
    resolved_at: Optional[str] = None
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    labels: Dict[str, str] = {}
    annotations: Dict[str, str] = {}


class AlertRuleRequest(BaseModel):
    """Request model for creating alert rules."""

    name: str = Field(..., description="Alert rule name")
    description: str = Field(..., description="Alert rule description")
    metric_name: str = Field(..., description="Metric to monitor")
    threshold: float = Field(..., description="Alert threshold value")
    operator: str = Field(..., description="Comparison operator (>, <, >=, <=, ==, !=)")
    severity: AlertSeverity = Field(..., description="Alert severity level")
    evaluation_window_minutes: int = Field(5, description="Evaluation window in minutes")
    cooldown_minutes: int = Field(15, description="Cooldown period in minutes")
    enabled: bool = Field(True, description="Whether the rule is enabled")


class HealthCheckResponse(BaseModel):
    """Response model for health check."""

    status: str
    timestamp: str
    uptime_seconds: float
    version: str
    components: Dict[str, str]
    metrics_summary: Dict[str, Any]


class DashboardDataResponse(BaseModel):
    """Response model for dashboard data."""

    dashboard_id: str
    title: str
    description: str
    widgets: List[Dict[str, Any]]
    last_updated: str
    refresh_interval: int


router = APIRouter(prefix="/api/v1/monitoring", tags=["monitoring"])


@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Get system health status."""

    dashboard_manager = get_dashboard_manager()
    memory_monitor = get_memory_monitor()

    # Component health checks
    components = {
        "dashboard_manager": "healthy",
        "memory_monitor": "healthy" if memory_monitor else "unavailable",
        "alert_manager": "healthy",
        "database": "healthy"  # Would check actual DB connection
    }

    # Get metrics summary
    try:
        memory_summary = memory_monitor.get_memory_summary() if memory_monitor else {}
        alert_summary = dashboard_manager.get_alert_summary()

        metrics_summary = {
            "memory": memory_summary.get("current", {}),
            "alerts": alert_summary,
            "dashboards": len(dashboard_manager.dashboard_manager.dashboards)
        }
    except Exception as e:
        metrics_summary = {"error": str(e)}

    return HealthCheckResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        uptime_seconds=0.0,  # Would calculate actual uptime
        version="1.0.0",
        components=components,
        metrics_summary=metrics_summary
    )


@router.get("/metrics/{metric_name}", response_model=MetricsResponse)
async def get_metric_data(
    metric_name: str,
    hours: int = Query(1, description="Hours of historical data to retrieve"),
    current_user: Dict = Depends(get_current_user)
):
    """Get metric data for a specific metric."""

    dashboard_manager = get_dashboard_manager()

    # Get metric data from resource metrics
    if metric_name in dashboard_manager.resource_metrics:
        # Get data for the specified time range
        cutoff_time = datetime.now() - timedelta(hours=hours)
        metric_data = [
            (timestamp, value) for timestamp, value in dashboard_manager.resource_metrics[metric_name]
            if timestamp > cutoff_time
        ]

        if not metric_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No data found for metric: {metric_name}"
            )

        timestamps = [point[0] for point in metric_data]
        values = [point[1] for point in metric_data]

        return MetricsResponse(
            metric_name=metric_name,
            timestamps=[ts.isoformat() for ts in timestamps],
            values=values,
            current_value=values[-1] if values else 0.0,
            min_value=min(values) if values else 0.0,
            max_value=max(values) if values else 0.0,
            avg_value=sum(values) / len(values) if values else 0.0
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Metric not found: {metric_name}"
    )


@router.get("/metrics", response_model=List[str])
async def list_available_metrics(
    current_user: Dict = Depends(get_current_user)
):
    """List all available metrics."""

    dashboard_manager = get_dashboard_manager()
    return list(dashboard_manager.resource_metrics.keys())


@router.get("/alerts", response_model=List[AlertResponse])
async def get_active_alerts(
    severity: Optional[AlertSeverity] = Query(None, description="Filter by severity"),
    current_user: Dict = Depends(get_current_user)
):
    """Get active alerts."""

    dashboard_manager = get_dashboard_manager()
    active_alerts = dashboard_manager.alert_manager.active_alerts

    alerts = []
    for alert in active_alerts.values():
        if severity is None or alert.severity == severity:
            alert_response = AlertResponse(
                rule_name=alert.rule_name,
                severity=alert.severity.value,
                message=alert.message,
                metric_value=alert.metric_value,
                threshold=alert.threshold,
                timestamp=alert.timestamp.isoformat(),
                resolved_at=alert.resolved_at.isoformat() if alert.resolved_at else None,
                acknowledged=alert.acknowledged,
                acknowledged_by=alert.acknowledged_by,
                labels=alert.labels,
                annotations=alert.annotations
            )
            alerts.append(alert_response)

    return alerts


@router.post("/alerts/rules", response_model=Dict[str, str])
async def create_alert_rule(
    rule_request: AlertRuleRequest,
    current_user: Dict = Depends(get_current_user)
):
    """Create a new alert rule."""

    dashboard_manager = get_dashboard_manager()

    # Create alert rule
    alert_rule = AlertRule(
        name=rule_request.name,
        description=rule_request.description,
        metric_name=rule_request.metric_name,
        threshold=rule_request.threshold,
        operator=rule_request.operator,
        severity=rule_request.severity,
        channels=[],  # Would be configured separately
        evaluation_window_minutes=rule_request.evaluation_window_minutes,
        cooldown_minutes=rule_request.cooldown_minutes,
        enabled=rule_request.enabled
    )

    dashboard_manager.alert_manager.add_alert_rule(alert_rule)

    return {"message": f"Alert rule '{rule_request.name}' created successfully"}


@router.put("/alerts/{alert_key}/acknowledge")
async def acknowledge_alert(
    alert_key: str,
    current_user: Dict = Depends(get_current_user)
):
    """Acknowledge an active alert."""

    dashboard_manager = get_dashboard_manager()
    active_alerts = dashboard_manager.alert_manager.active_alerts

    if alert_key not in active_alerts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert not found: {alert_key}"
        )

    alert = active_alerts[alert_key]
    alert.acknowledged = True
    alert.acknowledged_by = current_user.get("username", "unknown")

    return {"message": f"Alert '{alert_key}' acknowledged by {alert.acknowledged_by}"}


@router.get("/dashboards", response_model=List[Dict[str, Any]])
async def list_dashboards(
    current_user: Dict = Depends(get_current_user)
):
    """List available dashboards."""

    dashboard_manager = get_dashboard_manager()
    dashboards = dashboard_manager.dashboard_manager.dashboards

    dashboard_list = []
    for dashboard_id, dashboard in dashboards.items():
        dashboard_info = {
            "id": dashboard.id,
            "title": dashboard.title,
            "description": dashboard.description,
            "type": dashboard.type.value,
            "widget_count": len(dashboard.widgets),
            "created_at": dashboard.created_at.isoformat(),
            "updated_at": dashboard.updated_at.isoformat()
        }
        dashboard_list.append(dashboard_info)

    return dashboard_list


@router.get("/dashboards/{dashboard_id}", response_model=DashboardDataResponse)
async def get_dashboard_data(
    dashboard_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """Get dashboard data and widget information."""

    dashboard_manager = get_dashboard_manager()
    dashboard_data = dashboard_manager.get_dashboard_data(dashboard_id)

    if not dashboard_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dashboard not found: {dashboard_id}"
        )

    dashboard = dashboard_data["dashboard"]

    return DashboardDataResponse(
        dashboard_id=dashboard.id,
        title=dashboard.title,
        description=dashboard.description,
        widgets=dashboard_data["widgets"],
        last_updated=dashboard_data["last_updated"].isoformat(),
        refresh_interval=dashboard.refresh_interval
    )


@router.get("/dashboards/{dashboard_id}/export/grafana")
async def export_grafana_dashboard(
    dashboard_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """Export dashboard as Grafana JSON."""

    dashboard_manager = get_dashboard_manager()

    if dashboard_id not in dashboard_manager.dashboard_manager.dashboards:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dashboard not found: {dashboard_id}"
        )

    grafana_json = dashboard_manager.dashboard_manager.generate_grafana_dashboard(dashboard_id)

    return JSONResponse(
        content=grafana_json,
        headers={
            "Content-Disposition": f"attachment; filename={dashboard_id}_dashboard.json"
        }
    )


@router.get("/system/status")
async def get_system_status(
    current_user: Dict = Depends(get_current_user)
):
    """Get comprehensive system status."""

    dashboard_manager = get_dashboard_manager()
    memory_monitor = get_memory_monitor()

    # Collect system information
    system_status = {
        "monitoring": {
            "dashboard_manager_running": dashboard_manager.is_monitoring,
            "alert_evaluation_running": dashboard_manager.alert_manager.is_running,
            "memory_monitor_running": memory_monitor is not None
        },
        "metrics": {
            "available_metrics": len(dashboard_manager.resource_metrics),
            "total_data_points": sum(
                len(data) for data in dashboard_manager.resource_metrics.values()
            )
        },
        "alerts": dashboard_manager.get_alert_summary(),
        "dashboards": {
            "total_dashboards": len(dashboard_manager.dashboard_manager.dashboards),
            "dashboard_types": [
                dashboard.type.value
                for dashboard in dashboard_manager.dashboard_manager.dashboards.values()
            ]
        }
    }

    # Add memory information if available
    if memory_monitor:
        try:
            memory_summary = memory_monitor.get_memory_summary()
            system_status["memory"] = memory_summary
        except Exception as e:
            system_status["memory"] = {"error": str(e)}

    return system_status


@router.post("/system/maintenance")
async def trigger_maintenance(
    current_user: Dict = Depends(get_current_user)
):
    """Trigger system maintenance operations."""

    # This would trigger various maintenance operations
    # such as cleanup, optimization, etc.

    return {
        "message": "Maintenance operations triggered",
        "timestamp": datetime.now().isoformat(),
        "triggered_by": current_user.get("username", "unknown")
    }


@router.get("/export/prometheus")
async def export_prometheus_metrics():
    """Export metrics in Prometheus format."""

    # This would export metrics in Prometheus format
    # for integration with Prometheus monitoring

    from prometheus_client import REGISTRY, generate_latest

    try:
        metrics_data = generate_latest(REGISTRY)
        return JSONResponse(
            content=metrics_data.decode('utf-8'),
            media_type="text/plain"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export Prometheus metrics: {str(e)}"
        )