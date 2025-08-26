"""
Monitoring API endpoints for enterprise observability
Provides access to metrics, alerts, dashboards, and real-time monitoring
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from core.logging import get_logger
from monitoring.advanced_observability import (
    ObservabilityCollector, 
    MetricPoint, 
    MetricType, 
    Alert, 
    AlertSeverity
)
from monitoring.enterprise_dashboard import (
    DashboardManager,
    create_dashboard_router,
    SIMPLE_DASHBOARD_HTML
)

logger = get_logger(__name__)


# Initialize observability components
observability_collector = ObservabilityCollector()
dashboard_manager = DashboardManager(observability_collector)

# Create routers
router = APIRouter(prefix="/monitoring", tags=["monitoring"])
dashboard_router = create_dashboard_router(dashboard_manager)


# Pydantic models
class MetricRequest(BaseModel):
    name: str
    value: float
    metric_type: str = "gauge"
    labels: Dict[str, str] = {}


class AlertRequest(BaseModel):
    name: str
    description: str
    metric_name: str
    condition: str  # e.g., "> 100"
    threshold: float
    severity: str = "warning"
    duration_seconds: int = 60
    actions: List[str] = []


class MetricsQuery(BaseModel):
    metric_names: Optional[List[str]] = None
    time_range_hours: int = 24
    aggregation: str = "avg"  # avg, sum, max, min, count


# Metrics endpoints
@router.post("/metrics", status_code=status.HTTP_201_CREATED)
async def record_metric(request: MetricRequest):
    """Record a custom metric"""
    try:
        metric_type = MetricType(request.metric_type.lower())
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid metric type: {request.metric_type}"
        )
    
    metric = MetricPoint(
        name=request.name,
        value=request.value,
        timestamp=datetime.now(),
        labels=request.labels,
        metric_type=metric_type
    )
    
    await observability_collector.record_metric(metric)
    
    return {
        "message": "Metric recorded successfully",
        "metric_name": request.name,
        "value": request.value,
        "timestamp": metric.timestamp.isoformat()
    }


@router.get("/metrics")
async def get_metrics(
    metric_names: Optional[str] = Query(None, description="Comma-separated metric names"),
    hours: int = Query(24, description="Time range in hours"),
    aggregation: str = Query("avg", description="Aggregation method")
):
    """Get metrics data"""
    # Parse metric names
    target_metrics = metric_names.split(",") if metric_names else None
    
    # Get metrics from collector
    cutoff_time = datetime.now() - timedelta(hours=hours)
    metrics_data = {}
    
    for metric_key, metrics in observability_collector.metrics.items():
        # Filter by metric names if specified
        if target_metrics:
            if not any(name in metric_key for name in target_metrics):
                continue
        
        # Get metrics within time range
        filtered_metrics = [m for m in metrics if m.timestamp > cutoff_time]
        
        if filtered_metrics:
            values = [m.value for m in filtered_metrics]
            
            if aggregation == "avg":
                aggregated_value = sum(values) / len(values)
            elif aggregation == "sum":
                aggregated_value = sum(values)
            elif aggregation == "max":
                aggregated_value = max(values)
            elif aggregation == "min":
                aggregated_value = min(values)
            elif aggregation == "count":
                aggregated_value = len(values)
            else:
                aggregated_value = sum(values) / len(values)  # Default to avg
            
            metrics_data[metric_key] = {
                "value": aggregated_value,
                "count": len(filtered_metrics),
                "first_timestamp": filtered_metrics[0].timestamp.isoformat(),
                "last_timestamp": filtered_metrics[-1].timestamp.isoformat()
            }
    
    return {
        "metrics": metrics_data,
        "time_range_hours": hours,
        "aggregation": aggregation,
        "total_metrics": len(metrics_data)
    }


@router.get("/metrics/summary")
async def get_metrics_summary():
    """Get metrics summary"""
    return await observability_collector.get_metrics_summary()


@router.get("/metrics/timeseries/{metric_name}")
async def get_metric_timeseries(
    metric_name: str,
    hours: int = Query(24, description="Time range in hours"),
    interval_minutes: int = Query(5, description="Data point interval in minutes")
):
    """Get time series data for a specific metric"""
    cutoff_time = datetime.now() - timedelta(hours=hours)
    
    # Find matching metrics
    time_series = []
    for metric_key, metrics in observability_collector.metrics.items():
        if metric_name in metric_key:
            for metric in metrics:
                if metric.timestamp > cutoff_time:
                    time_series.append({
                        "timestamp": metric.timestamp.isoformat(),
                        "value": metric.value,
                        "labels": metric.labels
                    })
    
    # Sort by timestamp
    time_series.sort(key=lambda x: x["timestamp"])
    
    return {
        "metric_name": metric_name,
        "time_range_hours": hours,
        "data_points": len(time_series),
        "data": time_series
    }


# Alerts endpoints
@router.post("/alerts", status_code=status.HTTP_201_CREATED)
async def create_alert(request: AlertRequest):
    """Create a new alert rule"""
    try:
        severity = AlertSeverity(request.severity.lower())
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid severity: {request.severity}"
        )
    
    alert = Alert(
        alert_id=f"alert_{int(datetime.now().timestamp())}",
        name=request.name,
        description=request.description,
        severity=severity,
        metric_name=request.metric_name,
        condition=request.condition,
        threshold=request.threshold,
        duration_seconds=request.duration_seconds,
        actions=request.actions
    )
    
    observability_collector.add_alert_rule(alert)
    
    return {
        "alert_id": alert.alert_id,
        "name": alert.name,
        "message": "Alert rule created successfully"
    }


@router.get("/alerts")
async def get_alerts(active_only: bool = Query(True, description="Return only active alerts")):
    """Get alert rules and instances"""
    alert_rules = list(observability_collector.alerts.values())
    active_instances = await observability_collector.get_active_alerts()
    
    if active_only:
        alert_rules = [alert for alert in alert_rules if alert.is_active]
    
    return {
        "alert_rules": [
            {
                "alert_id": alert.alert_id,
                "name": alert.name,
                "description": alert.description,
                "severity": alert.severity.value,
                "metric_name": alert.metric_name,
                "condition": f"{alert.condition} {alert.threshold}",
                "is_active": alert.is_active,
                "created_at": alert.created_at.isoformat()
            }
            for alert in alert_rules
        ],
        "active_instances": [
            {
                "instance_id": instance.instance_id,
                "alert_id": instance.alert_id,
                "message": instance.message,
                "triggered_at": instance.triggered_at.isoformat(),
                "current_value": instance.current_value
            }
            for instance in active_instances
        ],
        "total_rules": len(alert_rules),
        "active_alerts": len(active_instances)
    }


@router.get("/alerts/{alert_id}")
async def get_alert_details(alert_id: str):
    """Get details for a specific alert"""
    alert = observability_collector.alerts.get(alert_id)
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found"
        )
    
    # Get instances for this alert
    instances = [
        instance for instance in observability_collector.alert_instances.values()
        if instance.alert_id == alert_id
    ]
    
    return {
        "alert": {
            "alert_id": alert.alert_id,
            "name": alert.name,
            "description": alert.description,
            "severity": alert.severity.value,
            "metric_name": alert.metric_name,
            "condition": f"{alert.condition} {alert.threshold}",
            "duration_seconds": alert.duration_seconds,
            "actions": alert.actions,
            "is_active": alert.is_active,
            "created_at": alert.created_at.isoformat()
        },
        "instances": [
            {
                "instance_id": instance.instance_id,
                "triggered_at": instance.triggered_at.isoformat(),
                "resolved_at": instance.resolved_at.isoformat() if instance.resolved_at else None,
                "current_value": instance.current_value,
                "message": instance.message,
                "context": instance.context
            }
            for instance in instances
        ]
    }


@router.delete("/alerts/{alert_id}")
async def delete_alert(alert_id: str):
    """Delete an alert rule"""
    if alert_id in observability_collector.alerts:
        del observability_collector.alerts[alert_id]
        return {"message": "Alert rule deleted successfully"}
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found"
        )


# Performance and health endpoints
@router.get("/health")
async def get_system_health():
    """Get overall system health status"""
    summary = await observability_collector.get_metrics_summary()
    active_alerts = await observability_collector.get_active_alerts()
    
    # Determine health status
    if len(active_alerts) > 5:
        health_status = "critical"
    elif len(active_alerts) > 2:
        health_status = "warning"
    elif summary.get("total_metrics", 0) > 0:
        health_status = "healthy"
    else:
        health_status = "unknown"
    
    return {
        "status": health_status,
        "timestamp": datetime.now().isoformat(),
        "metrics": {
            "total_metrics": summary.get("total_metrics", 0),
            "unique_metrics": summary.get("unique_metrics", 0),
            "active_alerts": len(active_alerts),
            "alert_rules": summary.get("active_alerts", 0),
            "performance_snapshots": summary.get("performance_snapshots", 0)
        },
        "uptime_seconds": 3600,  # Would be calculated from actual uptime
        "version": "2.0.0"
    }


@router.get("/performance")
async def get_performance_metrics(hours: int = Query(24, description="Time range in hours")):
    """Get performance trends"""
    trends = await observability_collector.get_performance_trends(hours=hours)
    
    if not trends:
        return {
            "message": "No performance data available",
            "time_range_hours": hours,
            "data_points": 0,
            "trends": []
        }
    
    return {
        "time_range_hours": hours,
        "data_points": len(trends),
        "latest_snapshot": {
            "timestamp": trends[-1].timestamp.isoformat(),
            "cpu_usage": trends[-1].cpu_usage,
            "memory_usage": trends[-1].memory_usage,
            "response_time_p95": trends[-1].response_time_p95,
            "error_rate": trends[-1].error_rate,
            "throughput_rps": trends[-1].throughput_rps
        },
        "trends": [
            {
                "timestamp": trend.timestamp.isoformat(),
                "cpu_usage": trend.cpu_usage,
                "memory_usage": trend.memory_usage,
                "response_time_p95": trend.response_time_p95,
                "error_rate": trend.error_rate,
                "throughput_rps": trend.throughput_rps
            }
            for trend in trends
        ]
    }


@router.get("/anomalies")
async def get_anomaly_detection_status():
    """Get anomaly detection status and baselines"""
    detector = observability_collector.anomaly_detector
    
    return {
        "total_metrics_monitored": len(detector.baselines),
        "baselines": {
            metric_name: {
                "mean": baseline["mean"],
                "standard_deviation": baseline["stdev"],
                "percentile_95": baseline["p95"],
                "percentile_99": baseline["p99"],
                "sample_count": len(detector.metric_windows[metric_name])
            }
            for metric_name, baseline in detector.baselines.items()
        },
        "window_size": detector.window_size
    }


# Real-time endpoints
@router.websocket("/realtime")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time monitoring updates"""
    await websocket.accept()
    
    try:
        # Send initial data
        initial_data = await observability_collector.get_metrics_summary()
        await websocket.send_json(initial_data)
        
        # Keep connection alive and send updates
        import asyncio
        while True:
            # Wait for any message or timeout
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send periodic updates
                update_data = await observability_collector.get_metrics_summary()
                await websocket.send_json(update_data)
            
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        logger.info("WebSocket connection closed")


# Dashboard endpoints (include the dashboard router)
router.include_router(dashboard_router)


@router.get("/dashboard/simple", response_class=HTMLResponse)
async def get_simple_dashboard():
    """Get simple HTML dashboard for development/testing"""
    return HTMLResponse(content=SIMPLE_DASHBOARD_HTML)


# Advanced monitoring endpoints
@router.get("/insights/top-metrics")
async def get_top_metrics(limit: int = Query(10, description="Number of top metrics to return")):
    """Get most active metrics"""
    metric_activity = {}
    
    for metric_key, metrics in observability_collector.metrics.items():
        metric_activity[metric_key] = len(metrics)
    
    # Sort by activity (number of data points)
    top_metrics = sorted(metric_activity.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    return {
        "top_metrics": [
            {"metric_name": metric, "data_points": count}
            for metric, count in top_metrics
        ],
        "total_unique_metrics": len(metric_activity)
    }


@router.get("/insights/error-analysis")
async def get_error_analysis(hours: int = Query(24, description="Time range in hours")):
    """Analyze error patterns and trends"""
    cutoff_time = datetime.now() - timedelta(hours=hours)
    error_metrics = {}
    
    for metric_key, metrics in observability_collector.metrics.items():
        if "error" in metric_key.lower():
            recent_errors = [m for m in metrics if m.timestamp > cutoff_time]
            if recent_errors:
                error_metrics[metric_key] = {
                    "total_errors": sum(m.value for m in recent_errors),
                    "error_rate": len(recent_errors),
                    "peak_error": max(m.value for m in recent_errors),
                    "last_error_time": max(m.timestamp for m in recent_errors).isoformat()
                }
    
    return {
        "time_range_hours": hours,
        "error_metrics": error_metrics,
        "total_error_types": len(error_metrics),
        "analysis_timestamp": datetime.now().isoformat()
    }


@router.post("/test/generate-metrics")
async def generate_test_metrics():
    """Generate test metrics for development/testing"""
    import random
    
    now = datetime.now()
    test_metrics = [
        ("cpu_usage", random.uniform(20, 80)),
        ("memory_usage", random.uniform(40, 90)),
        ("response_time", random.uniform(100, 500)),
        ("request_count", random.randint(10, 100)),
        ("error_count", random.randint(0, 10)),
        ("active_connections", random.randint(50, 200))
    ]
    
    for metric_name, value in test_metrics:
        metric = MetricPoint(
            name=metric_name,
            value=value,
            timestamp=now,
            labels={"source": "test", "environment": "development"},
            metric_type=MetricType.GAUGE
        )
        await observability_collector.record_metric(metric)
    
    return {
        "message": "Test metrics generated successfully",
        "metrics_generated": len(test_metrics),
        "timestamp": now.isoformat()
    }