"""
Infrastructure Health Check API Endpoints
Provides comprehensive REST API endpoints for health monitoring,
alerting, and dashboard data with proper authentication and rate limiting.
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Depends, Query, Path, Body
from fastapi.security import HTTPBearer
from pydantic import BaseModel, Field
import time

from core.logging import get_logger
from api.middleware.rate_limiter import RateLimiter
from api.middleware.enterprise_security import get_current_user
from monitoring.infrastructure_health_monitor import (
    get_infrastructure_manager, InfrastructureHealthManager,
    HealthStatus, ComponentType, HealthMetrics
)
from monitoring.intelligent_alerting_system import (
    get_alerting_system, IntelligentAlertingSystem,
    AlertSeverity, AlertState
)
from monitoring.infrastructure_dashboards import (
    get_dashboard_manager, DashboardManager, DashboardType
)

logger = get_logger(__name__)
router = APIRouter(prefix="/infrastructure", tags=["Infrastructure Health"])
security = HTTPBearer()
rate_limiter = RateLimiter(requests_per_minute=60)


# Pydantic models for API responses
class ComponentHealthResponse(BaseModel):
    """Component health check response"""
    component_id: str
    component_type: str
    status: str
    response_time_ms: float
    availability_24h: float
    last_updated: str
    details: Dict[str, Any] = Field(default_factory=dict)
    circuit_breaker_state: str
    alerts_count: int
    error: Optional[str] = None


class SystemHealthResponse(BaseModel):
    """System-wide health check response"""
    overall_status: str
    total_components: int
    healthy_components: int
    critical_issues: List[str]
    warning_issues: List[str]
    total_alerts: int
    monitoring_active: bool
    components: Dict[str, ComponentHealthResponse]
    integration_health: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    timestamp: str


class PerformanceMetricsResponse(BaseModel):
    """Performance metrics response"""
    redis: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    rabbitmq: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    kafka: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    overall: Dict[str, float] = Field(default_factory=dict)
    timestamp: str


class AlertResponse(BaseModel):
    """Alert response model"""
    id: str
    component_type: str
    component_id: str
    severity: str
    title: str
    message: str
    state: str
    created_at: str
    acknowledged_at: Optional[str] = None
    resolved_at: Optional[str] = None
    escalation_level: int
    notification_count: int
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AlertStatisticsResponse(BaseModel):
    """Alert statistics response"""
    total_active: int
    total_rules: int
    total_providers: int
    last_24h: Dict[str, Any]
    last_7d: Dict[str, Any]


class DashboardResponse(BaseModel):
    """Dashboard response model"""
    id: str
    name: str
    description: str
    type: str
    widget_count: int
    refresh_interval: int
    auto_refresh: bool


class DashboardDataResponse(BaseModel):
    """Dashboard data response"""
    dashboard: Dict[str, Any]
    widget_data: Dict[str, Dict[str, Any]]
    last_updated: str


class LoadTestRequest(BaseModel):
    """Load test request model"""
    target_component: str
    duration_seconds: int = Field(default=60, ge=10, le=300)
    concurrent_requests: int = Field(default=10, ge=1, le=100)
    test_type: str = Field(default="health_check")


class LoadTestResponse(BaseModel):
    """Load test response model"""
    test_id: str
    status: str
    duration_seconds: float
    total_requests: int
    successful_requests: int
    failed_requests: int
    average_response_time_ms: float
    min_response_time_ms: float
    max_response_time_ms: float
    requests_per_second: float
    error_rate_percent: float
    errors: List[str] = Field(default_factory=list)


# Dependency injection
async def get_health_manager() -> InfrastructureHealthManager:
    """Get infrastructure health manager"""
    return get_infrastructure_manager()


async def get_alert_system() -> IntelligentAlertingSystem:
    """Get alerting system"""
    return get_alerting_system()


async def get_dashboard_system() -> DashboardManager:
    """Get dashboard manager"""
    return get_dashboard_manager()


# Health check endpoints
@router.get("/health", response_model=SystemHealthResponse)
async def get_system_health(
    health_manager: InfrastructureHealthManager = Depends(get_health_manager),
    current_user: dict = Depends(get_current_user)
):
    """Get comprehensive system health status"""
    try:
        await rate_limiter.check_rate_limit(f"health_{current_user.get('id', 'anonymous')}")
        
        health_data = await health_manager.get_system_health()
        
        # Convert components to response format
        components = {}
        for component_id, component_data in health_data.get("components", {}).items():
            components[component_id] = ComponentHealthResponse(
                component_id=component_id,
                component_type=component_data.get("component", "unknown"),
                status=component_data.get("status", "unknown"),
                response_time_ms=component_data.get("response_time_ms", 0),
                availability_24h=component_data.get("availability_24h", 0),
                last_updated=component_data.get("timestamp", ""),
                details=component_data.get("details", {}),
                circuit_breaker_state=component_data.get("circuit_breaker_state", "unknown"),
                alerts_count=component_data.get("alerts_count", 0),
                error=component_data.get("error")
            )
        
        return SystemHealthResponse(
            overall_status=health_data.get("overall_status", "unknown"),
            total_components=health_data.get("total_components", 0),
            healthy_components=health_data.get("healthy_components", 0),
            critical_issues=health_data.get("critical_issues", []),
            warning_issues=health_data.get("warning_issues", []),
            total_alerts=health_data.get("total_alerts", 0),
            monitoring_active=health_data.get("monitoring_active", False),
            components=components,
            integration_health=health_data.get("integration_health", {}),
            timestamp=health_data.get("timestamp", datetime.now().isoformat())
        )
    
    except Exception as e:
        logger.error(f"Error getting system health: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get system health: {str(e)}")


@router.get("/health/{component_id}", response_model=ComponentHealthResponse)
async def get_component_health(
    component_id: str = Path(..., description="Component ID to check"),
    health_manager: InfrastructureHealthManager = Depends(get_health_manager),
    current_user: dict = Depends(get_current_user)
):
    """Get health status for a specific component"""
    try:
        await rate_limiter.check_rate_limit(f"component_health_{current_user.get('id', 'anonymous')}")
        
        if component_id not in health_manager.monitors:
            raise HTTPException(status_code=404, detail=f"Component {component_id} not found")
        
        monitor = health_manager.monitors[component_id]
        health_data = await monitor.health_check()
        
        return ComponentHealthResponse(
            component_id=component_id,
            component_type=health_data.get("component", "unknown"),
            status=health_data.get("status", "unknown"),
            response_time_ms=health_data.get("response_time_ms", 0),
            availability_24h=health_data.get("availability_24h", 0),
            last_updated=health_data.get("timestamp", ""),
            details=health_data.get("details", {}),
            circuit_breaker_state=health_data.get("circuit_breaker_state", "unknown"),
            alerts_count=health_data.get("alerts_count", 0),
            error=health_data.get("error")
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting component health for {component_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get component health: {str(e)}")


@router.get("/performance", response_model=PerformanceMetricsResponse)
async def get_performance_metrics(
    health_manager: InfrastructureHealthManager = Depends(get_health_manager),
    current_user: dict = Depends(get_current_user)
):
    """Get comprehensive performance metrics"""
    try:
        await rate_limiter.check_rate_limit(f"performance_{current_user.get('id', 'anonymous')}")
        
        performance_data = await health_manager.get_performance_summary()
        
        return PerformanceMetricsResponse(
            redis=performance_data.get("redis", {}),
            rabbitmq=performance_data.get("rabbitmq", {}),
            kafka=performance_data.get("kafka", {}),
            overall=performance_data.get("overall", {}),
            timestamp=performance_data.get("timestamp", datetime.now().isoformat())
        )
    
    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get performance metrics: {str(e)}")


# Alert management endpoints
@router.get("/alerts", response_model=List[AlertResponse])
async def get_alerts(
    component_id: Optional[str] = Query(None, description="Filter by component ID"),
    severity: Optional[AlertSeverity] = Query(None, description="Filter by severity"),
    state: Optional[AlertState] = Query(None, description="Filter by state"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of alerts to return"),
    offset: int = Query(0, ge=0, description="Number of alerts to skip"),
    alerting_system: IntelligentAlertingSystem = Depends(get_alert_system),
    current_user: dict = Depends(get_current_user)
):
    """Get alerts with optional filtering"""
    try:
        await rate_limiter.check_rate_limit(f"alerts_{current_user.get('id', 'anonymous')}")
        
        # Get active alerts first
        active_alerts = alerting_system.get_active_alerts(component_id, severity)
        
        # Convert to response format
        alert_responses = []
        for alert_data in active_alerts[offset:offset + limit]:
            alert_responses.append(AlertResponse(
                id=alert_data["alert"]["id"],
                component_type=alert_data["alert"]["component_type"],
                component_id=alert_data["alert"]["component_id"],
                severity=alert_data["alert"]["severity"],
                title=alert_data["alert"]["title"],
                message=alert_data["alert"]["message"],
                state=alert_data["state"],
                created_at=alert_data["created_at"],
                acknowledged_at=alert_data.get("acknowledged_at"),
                resolved_at=alert_data.get("resolved_at"),
                escalation_level=alert_data["escalation_level"],
                notification_count=alert_data["notification_count"],
                metadata=alert_data["context_data"]
            ))
        
        return alert_responses
    
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get alerts: {str(e)}")


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str = Path(..., description="Alert ID to acknowledge"),
    alerting_system: IntelligentAlertingSystem = Depends(get_alert_system),
    current_user: dict = Depends(get_current_user)
):
    """Acknowledge an alert"""
    try:
        await rate_limiter.check_rate_limit(f"ack_alert_{current_user.get('id', 'anonymous')}")
        
        acknowledged_by = current_user.get("username", "api_user")
        success = await alerting_system.acknowledge_alert(alert_id, acknowledged_by)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
        
        return {"message": f"Alert {alert_id} acknowledged by {acknowledged_by}", "success": True}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error acknowledging alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to acknowledge alert: {str(e)}")


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: str = Path(..., description="Alert ID to resolve"),
    alerting_system: IntelligentAlertingSystem = Depends(get_alert_system),
    current_user: dict = Depends(get_current_user)
):
    """Resolve an alert"""
    try:
        await rate_limiter.check_rate_limit(f"resolve_alert_{current_user.get('id', 'anonymous')}")
        
        resolved_by = current_user.get("username", "api_user")
        success = await alerting_system.resolve_alert(alert_id, resolved_by)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
        
        return {"message": f"Alert {alert_id} resolved by {resolved_by}", "success": True}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resolving alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to resolve alert: {str(e)}")


@router.get("/alerts/statistics", response_model=AlertStatisticsResponse)
async def get_alert_statistics(
    alerting_system: IntelligentAlertingSystem = Depends(get_alert_system),
    current_user: dict = Depends(get_current_user)
):
    """Get alert system statistics"""
    try:
        await rate_limiter.check_rate_limit(f"alert_stats_{current_user.get('id', 'anonymous')}")
        
        stats = alerting_system.get_alert_statistics()
        
        return AlertStatisticsResponse(
            total_active=stats.get("total_active", 0),
            total_rules=stats.get("total_rules", 0),
            total_providers=stats.get("total_providers", 0),
            last_24h=stats.get("last_24h", {}),
            last_7d=stats.get("last_7d", {})
        )
    
    except Exception as e:
        logger.error(f"Error getting alert statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get alert statistics: {str(e)}")


# Dashboard endpoints
@router.get("/dashboards", response_model=List[DashboardResponse])
async def get_dashboards(
    dashboard_manager: DashboardManager = Depends(get_dashboard_system),
    current_user: dict = Depends(get_current_user)
):
    """Get list of available dashboards"""
    try:
        await rate_limiter.check_rate_limit(f"dashboards_{current_user.get('id', 'anonymous')}")
        
        dashboards = dashboard_manager.get_dashboard_list()
        
        return [
            DashboardResponse(
                id=dashboard["id"],
                name=dashboard["name"],
                description=dashboard["description"],
                type=dashboard["type"],
                widget_count=dashboard["widget_count"],
                refresh_interval=dashboard["refresh_interval"],
                auto_refresh=dashboard["auto_refresh"]
            ) for dashboard in dashboards
        ]
    
    except Exception as e:
        logger.error(f"Error getting dashboards: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get dashboards: {str(e)}")


@router.get("/dashboards/{dashboard_id}", response_model=DashboardDataResponse)
async def get_dashboard_data(
    dashboard_id: str = Path(..., description="Dashboard ID"),
    hours: int = Query(24, ge=1, le=168, description="Time range in hours"),
    dashboard_manager: DashboardManager = Depends(get_dashboard_system),
    current_user: dict = Depends(get_current_user)
):
    """Get dashboard data with widgets"""
    try:
        await rate_limiter.check_rate_limit(f"dashboard_data_{current_user.get('id', 'anonymous')}")
        
        time_range = {
            "start": datetime.now() - timedelta(hours=hours),
            "end": datetime.now()
        }
        
        dashboard_data = await dashboard_manager.get_dashboard_data(dashboard_id, time_range)
        
        if "error" in dashboard_data:
            raise HTTPException(status_code=404, detail=dashboard_data["error"])
        
        return DashboardDataResponse(
            dashboard=dashboard_data["dashboard"],
            widget_data=dashboard_data["widget_data"],
            last_updated=dashboard_data["last_updated"]
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dashboard data for {dashboard_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")


@router.get("/dashboards/{dashboard_id}/widgets/{widget_id}")
async def get_widget_data(
    dashboard_id: str = Path(..., description="Dashboard ID"),
    widget_id: str = Path(..., description="Widget ID"),
    hours: int = Query(24, ge=1, le=168, description="Time range in hours"),
    dashboard_manager: DashboardManager = Depends(get_dashboard_system),
    current_user: dict = Depends(get_current_user)
):
    """Get data for a specific widget"""
    try:
        await rate_limiter.check_rate_limit(f"widget_data_{current_user.get('id', 'anonymous')}")
        
        time_range = {
            "start": datetime.now() - timedelta(hours=hours),
            "end": datetime.now()
        }
        
        widget_data = await dashboard_manager.get_widget_data(dashboard_id, widget_id, time_range)
        
        if "error" in widget_data:
            raise HTTPException(status_code=404, detail=widget_data["error"])
        
        return widget_data
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting widget data for {dashboard_id}/{widget_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get widget data: {str(e)}")


# Load testing endpoints
@router.post("/load-test", response_model=LoadTestResponse)
async def run_load_test(
    request: LoadTestRequest,
    health_manager: InfrastructureHealthManager = Depends(get_health_manager),
    current_user: dict = Depends(get_current_user)
):
    """Run load test against infrastructure components"""
    try:
        await rate_limiter.check_rate_limit(f"load_test_{current_user.get('id', 'anonymous')}")
        
        if request.target_component not in health_manager.monitors:
            raise HTTPException(
                status_code=404, 
                detail=f"Component {request.target_component} not found"
            )
        
        # Generate test ID
        test_id = f"load_test_{request.target_component}_{int(time.time())}"
        
        # Run load test
        monitor = health_manager.monitors[request.target_component]
        
        start_time = time.time()
        results = []
        errors = []
        
        # Simple load test implementation
        async def single_request():
            try:
                request_start = time.time()
                await monitor.health_check()
                request_time = (time.time() - request_start) * 1000
                return {"success": True, "response_time_ms": request_time}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        # Run concurrent requests
        tasks = []
        for _ in range(request.concurrent_requests):
            for _ in range(int(request.duration_seconds / 10)):  # Spread over duration
                tasks.append(single_request())
                if len(tasks) >= request.concurrent_requests:
                    batch_results = await asyncio.gather(*tasks[:request.concurrent_requests])
                    results.extend(batch_results)
                    tasks = tasks[request.concurrent_requests:]
                    await asyncio.sleep(1)  # Brief pause between batches
        
        # Process remaining tasks
        if tasks:
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
        
        test_duration = time.time() - start_time
        
        # Calculate statistics
        successful_requests = len([r for r in results if r.get("success")])
        failed_requests = len(results) - successful_requests
        
        response_times = [r.get("response_time_ms", 0) for r in results if r.get("success")]
        
        avg_response_time = statistics.mean(response_times) if response_times else 0
        min_response_time = min(response_times) if response_times else 0
        max_response_time = max(response_times) if response_times else 0
        
        requests_per_second = len(results) / test_duration if test_duration > 0 else 0
        error_rate = (failed_requests / len(results)) * 100 if results else 0
        
        # Collect errors
        for result in results:
            if not result.get("success") and result.get("error"):
                errors.append(result["error"])
        
        return LoadTestResponse(
            test_id=test_id,
            status="completed",
            duration_seconds=test_duration,
            total_requests=len(results),
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            average_response_time_ms=avg_response_time,
            min_response_time_ms=min_response_time,
            max_response_time_ms=max_response_time,
            requests_per_second=requests_per_second,
            error_rate_percent=error_rate,
            errors=list(set(errors))[:10]  # Unique errors, max 10
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running load test: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to run load test: {str(e)}")


# Control endpoints
@router.post("/monitoring/start")
async def start_monitoring(
    health_manager: InfrastructureHealthManager = Depends(get_health_manager),
    alerting_system: IntelligentAlertingSystem = Depends(get_alert_system),
    current_user: dict = Depends(get_current_user)
):
    """Start infrastructure monitoring"""
    try:
        # Check permissions (admin only)
        if not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="Admin access required")
        
        await health_manager.start_monitoring()
        await alerting_system.start_processing()
        
        return {"message": "Infrastructure monitoring started", "success": True}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting monitoring: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start monitoring: {str(e)}")


@router.post("/monitoring/stop")
async def stop_monitoring(
    health_manager: InfrastructureHealthManager = Depends(get_health_manager),
    alerting_system: IntelligentAlertingSystem = Depends(get_alert_system),
    current_user: dict = Depends(get_current_user)
):
    """Stop infrastructure monitoring"""
    try:
        # Check permissions (admin only)
        if not current_user.get("is_admin", False):
            raise HTTPException(status_code=403, detail="Admin access required")
        
        await health_manager.stop_monitoring()
        await alerting_system.stop_processing()
        
        return {"message": "Infrastructure monitoring stopped", "success": True}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping monitoring: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stop monitoring: {str(e)}")


@router.get("/monitoring/status")
async def get_monitoring_status(
    health_manager: InfrastructureHealthManager = Depends(get_health_manager),
    alerting_system: IntelligentAlertingSystem = Depends(get_alert_system),
    dashboard_manager: DashboardManager = Depends(get_dashboard_system),
    current_user: dict = Depends(get_current_user)
):
    """Get monitoring system status"""
    try:
        dashboard_health = await dashboard_manager.get_dashboard_health()
        
        return {
            "infrastructure_monitoring": {
                "active": health_manager.monitoring_active,
                "interval_seconds": health_manager.monitoring_interval,
                "total_monitors": len(health_manager.monitors),
                "baselines_count": len(health_manager.baselines)
            },
            "alerting_system": {
                "active": alerting_system.processing_active,
                "total_rules": len(alerting_system.rules),
                "total_providers": len(alerting_system.notification_providers),
                "active_alerts": len(alerting_system.active_alerts)
            },
            "dashboard_system": {
                "total_dashboards": dashboard_health.get("total_dashboards", 0),
                "total_widgets": dashboard_health.get("total_widgets", 0),
                "status": "healthy"
            },
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error getting monitoring status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get monitoring status: {str(e)}")


# Export router
__all__ = ["router"]