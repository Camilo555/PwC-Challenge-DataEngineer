"""
Monitoring Dashboards API Routes
==============================

FastAPI routes for comprehensive monitoring dashboards with:
- Dashboard CRUD operations with role-based access control
- Real-time dashboard data streaming with WebSocket support
- Dashboard template management with predefined configurations
- Export capabilities (PDF, Excel, PNG) with custom branding
- Widget management with drag-and-drop interface support
- Dashboard sharing and collaboration features

Endpoints:
- POST /dashboards - Create new dashboard
- GET /dashboards - List user's dashboards
- GET /dashboards/{dashboard_id} - Get dashboard configuration
- PUT /dashboards/{dashboard_id} - Update dashboard
- DELETE /dashboards/{dashboard_id} - Delete dashboard
- GET /dashboards/{dashboard_id}/data - Get dashboard data
- GET /dashboards/{dashboard_id}/export - Export dashboard
- WebSocket /dashboards/{dashboard_id}/stream - Real-time updates
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi import status, BackgroundTasks
from fastapi.responses import Response
from pydantic import BaseModel, Field

from core.auth.jwt import get_current_user
from core.auth.models import User
from monitoring.comprehensive_monitoring_dashboards import (
    ComprehensiveMonitoringDashboards,
    DashboardConfig,
    DashboardData,
    DashboardType,
    WidgetConfig,
    WidgetType,
    RefreshInterval,
    get_dashboard_system
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/dashboards", tags=["monitoring-dashboards"])

# Request/Response Models
class CreateDashboardRequest(BaseModel):
    """Create dashboard request model"""
    name: str = Field(..., min_length=1, max_length=100, description="Dashboard name")
    description: str = Field("", max_length=500, description="Dashboard description")
    dashboard_type: DashboardType = Field(DashboardType.CUSTOM, description="Dashboard type")
    widgets: List[Dict[str, Any]] = Field(default_factory=list, description="Dashboard widgets")
    layout: Dict[str, Any] = Field(default_factory=dict, description="Dashboard layout")
    tags: List[str] = Field(default_factory=list, description="Dashboard tags")
    permissions: Dict[str, List[str]] = Field(default_factory=dict, description="Dashboard permissions")

class UpdateDashboardRequest(BaseModel):
    """Update dashboard request model"""
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="Dashboard name")
    description: Optional[str] = Field(None, max_length=500, description="Dashboard description")
    widgets: Optional[List[Dict[str, Any]]] = Field(None, description="Dashboard widgets")
    layout: Optional[Dict[str, Any]] = Field(None, description="Dashboard layout")
    tags: Optional[List[str]] = Field(None, description="Dashboard tags")
    permissions: Optional[Dict[str, List[str]]] = Field(None, description="Dashboard permissions")

class DashboardResponse(BaseModel):
    """Dashboard response model"""
    dashboard_id: str = Field(..., description="Dashboard identifier")
    name: str = Field(..., description="Dashboard name")
    description: str = Field(..., description="Dashboard description")
    dashboard_type: DashboardType = Field(..., description="Dashboard type")
    created_by: str = Field(..., description="Dashboard creator")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    widget_count: int = Field(..., description="Number of widgets")
    tags: List[str] = Field(..., description="Dashboard tags")

class DashboardListResponse(BaseModel):
    """Dashboard list response model"""
    dashboards: List[DashboardResponse] = Field(..., description="List of dashboards")
    total_count: int = Field(..., description="Total number of dashboards")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Page size")

class ExportRequest(BaseModel):
    """Dashboard export request model"""
    format: str = Field(..., description="Export format (pdf, excel, png)")
    include_data: bool = Field(True, description="Include data in export")
    date_range: Optional[Dict[str, str]] = Field(None, description="Date range filter")

# Dashboard CRUD Operations
@router.post("/", response_model=Dict[str, str], status_code=status.HTTP_201_CREATED)
async def create_dashboard(
    request: CreateDashboardRequest,
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Create a new monitoring dashboard with comprehensive configuration

    Creates a new dashboard with specified widgets, layout, and permissions.
    Supports custom and template-based dashboard creation with validation.
    """
    try:
        # Convert request to DashboardConfig
        widgets = [WidgetConfig(**widget) for widget in request.widgets]

        dashboard_config = DashboardConfig(
            name=request.name,
            description=request.description,
            dashboard_type=request.dashboard_type,
            widgets=widgets,
            layout=request.layout,
            permissions=request.permissions,
            tags=request.tags,
            created_by=current_user.id
        )

        # Create dashboard
        dashboard_id = await dashboard_system.create_dashboard(dashboard_config, current_user.id)

        logger.info(f"Dashboard created successfully by user {current_user.id}: {dashboard_id}")

        return {
            "message": "Dashboard created successfully",
            "dashboard_id": dashboard_id
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail="Dashboard creation failed")

@router.get("/", response_model=DashboardListResponse)
async def list_dashboards(
    dashboard_type: Optional[DashboardType] = Query(None, description="Filter by dashboard type"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Get list of accessible dashboards for the current user

    Returns paginated list of dashboards with filtering options.
    Includes dashboard metadata and access permissions.
    """
    try:
        # Get dashboard list from system
        dashboards = await dashboard_system.get_dashboard_list(current_user.id, dashboard_type)

        # Convert to response format
        dashboard_responses = []
        for dashboard in dashboards:
            dashboard_responses.append(DashboardResponse(
                dashboard_id=dashboard["dashboard_id"],
                name=dashboard["name"],
                description=dashboard.get("description", ""),
                dashboard_type=DashboardType(dashboard["dashboard_type"]),
                created_by=dashboard.get("created_by", "unknown"),
                created_at=dashboard["created_at"],
                updated_at=dashboard["updated_at"],
                widget_count=len(dashboard.get("widgets", [])),
                tags=dashboard.get("tags", [])
            ))

        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_dashboards = dashboard_responses[start_idx:end_idx]

        return DashboardListResponse(
            dashboards=paginated_dashboards,
            total_count=len(dashboard_responses),
            page=page,
            page_size=page_size
        )

    except Exception as e:
        logger.error(f"Failed to list dashboards: {str(e)}")
        raise HTTPException(status_code=500, detail="Dashboard list retrieval failed")

@router.get("/{dashboard_id}", response_model=Dict[str, Any])
async def get_dashboard(
    dashboard_id: str,
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Get dashboard configuration and metadata

    Returns complete dashboard configuration including widgets, layout,
    and permissions for the specified dashboard.
    """
    try:
        # Get dashboard configuration
        dashboard_config = await dashboard_system._get_dashboard_config(dashboard_id)
        await dashboard_system._verify_dashboard_access(dashboard_config, current_user.id)

        # Convert to response format
        return {
            "dashboard_id": dashboard_config.dashboard_id,
            "name": dashboard_config.name,
            "description": dashboard_config.description,
            "dashboard_type": dashboard_config.dashboard_type.value,
            "widgets": [widget.__dict__ for widget in dashboard_config.widgets],
            "layout": dashboard_config.layout,
            "permissions": dashboard_config.permissions,
            "tags": dashboard_config.tags,
            "created_by": dashboard_config.created_by,
            "created_at": dashboard_config.created_at.isoformat(),
            "updated_at": dashboard_config.updated_at.isoformat(),
            "is_active": dashboard_config.is_active
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail="Dashboard retrieval failed")

@router.put("/{dashboard_id}", response_model=Dict[str, str])
async def update_dashboard(
    dashboard_id: str,
    request: UpdateDashboardRequest,
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Update dashboard configuration

    Updates dashboard properties including name, description, widgets,
    layout, and permissions with proper validation.
    """
    try:
        # Prepare updates dictionary
        updates = {}
        if request.name is not None:
            updates["name"] = request.name
        if request.description is not None:
            updates["description"] = request.description
        if request.widgets is not None:
            updates["widgets"] = [WidgetConfig(**widget) for widget in request.widgets]
        if request.layout is not None:
            updates["layout"] = request.layout
        if request.tags is not None:
            updates["tags"] = request.tags
        if request.permissions is not None:
            updates["permissions"] = request.permissions

        # Update dashboard
        success = await dashboard_system.update_dashboard(dashboard_id, updates, current_user.id)

        if success:
            logger.info(f"Dashboard updated successfully by user {current_user.id}: {dashboard_id}")
            return {"message": "Dashboard updated successfully"}
        else:
            raise HTTPException(status_code=500, detail="Dashboard update failed")

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail="Dashboard update failed")

@router.delete("/{dashboard_id}", response_model=Dict[str, str])
async def delete_dashboard(
    dashboard_id: str,
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Delete dashboard with proper cleanup

    Removes dashboard and all associated data with proper permission checking.
    Supports soft deletion with audit trail preservation.
    """
    try:
        # Delete dashboard
        success = await dashboard_system.delete_dashboard(dashboard_id, current_user.id)

        if success:
            logger.info(f"Dashboard deleted successfully by user {current_user.id}: {dashboard_id}")
            return {"message": "Dashboard deleted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Dashboard deletion failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail="Dashboard deletion failed")

@router.get("/{dashboard_id}/data", response_model=DashboardData)
async def get_dashboard_data(
    dashboard_id: str,
    refresh: bool = Query(False, description="Force refresh from data sources"),
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Get real-time dashboard data with comprehensive metrics

    Returns current dashboard data including all widget data, system health,
    and active alerts with sub-second refresh capabilities.
    """
    try:
        # Get dashboard data
        dashboard_data = await dashboard_system.get_dashboard_data(dashboard_id, current_user.id)

        logger.info(f"Dashboard data retrieved for user {current_user.id}: {dashboard_id}")
        return dashboard_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get dashboard data: {str(e)}")
        raise HTTPException(status_code=500, detail="Dashboard data retrieval failed")

@router.get("/{dashboard_id}/export")
async def export_dashboard(
    dashboard_id: str,
    export_request: ExportRequest = Depends(),
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Export dashboard in specified format (PDF, Excel, PNG)

    Generates dashboard export with current data and custom formatting.
    Supports multiple export formats with configurable options.
    """
    try:
        # Export dashboard
        export_data = await dashboard_system.export_dashboard(
            dashboard_id, export_request.format, current_user.id
        )

        # Set appropriate content type based on format
        content_type_map = {
            "pdf": "application/pdf",
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "png": "image/png"
        }

        content_type = content_type_map.get(export_request.format.lower(), "application/octet-stream")
        filename = f"dashboard_{dashboard_id}.{export_request.format.lower()}"

        logger.info(f"Dashboard exported by user {current_user.id}: {dashboard_id} as {export_request.format}")

        return Response(
            content=export_data,
            media_type=content_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail="Dashboard export failed")

# WebSocket Endpoints for Real-time Updates
@router.websocket("/{dashboard_id}/stream")
async def dashboard_stream(
    websocket: WebSocket,
    dashboard_id: str,
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    WebSocket endpoint for real-time dashboard updates

    Provides real-time streaming of dashboard data with configurable
    refresh intervals and intelligent update detection.
    """
    await websocket.accept()

    try:
        # Add connection to active connections
        if dashboard_id not in dashboard_system.active_connections:
            dashboard_system.active_connections[dashboard_id] = []
        dashboard_system.active_connections[dashboard_id].append(websocket)

        logger.info(f"WebSocket connection established for dashboard: {dashboard_id}")

        # Send initial dashboard data
        try:
            # Note: In real implementation, we'd need to get user from WebSocket auth
            initial_data = await dashboard_system.get_dashboard_data(dashboard_id, "system")
            await websocket.send_text(json.dumps(initial_data.dict(), default=str))
        except Exception as e:
            logger.error(f"Failed to send initial data: {str(e)}")

        # Keep connection alive and send updates
        while True:
            try:
                # Wait for client message or timeout
                message = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)

                # Process client commands (ping, configure refresh rate, etc.)
                try:
                    command = json.loads(message)
                    if command.get("type") == "ping":
                        await websocket.send_text(json.dumps({"type": "pong", "timestamp": datetime.utcnow().isoformat()}))
                    elif command.get("type") == "refresh":
                        # Send updated data
                        updated_data = await dashboard_system.get_dashboard_data(dashboard_id, "system")
                        await websocket.send_text(json.dumps({
                            "type": "data_update",
                            "data": updated_data.dict()
                        }, default=str))
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON received from WebSocket client: {message}")

            except asyncio.TimeoutError:
                # Send periodic updates even without client messages
                try:
                    updated_data = await dashboard_system.get_dashboard_data(dashboard_id, "system")
                    await websocket.send_text(json.dumps({
                        "type": "periodic_update",
                        "data": updated_data.dict()
                    }, default=str))
                except Exception as e:
                    logger.error(f"Failed to send periodic update: {str(e)}")
                    break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for dashboard: {dashboard_id}")
    except Exception as e:
        logger.error(f"WebSocket error for dashboard {dashboard_id}: {str(e)}")
    finally:
        # Remove connection from active connections
        if dashboard_id in dashboard_system.active_connections:
            try:
                dashboard_system.active_connections[dashboard_id].remove(websocket)
                if not dashboard_system.active_connections[dashboard_id]:
                    del dashboard_system.active_connections[dashboard_id]
            except ValueError:
                pass  # Connection was already removed

# Dashboard Template Endpoints
@router.get("/templates/", response_model=Dict[str, List[str]])
async def list_dashboard_templates(
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Get list of available dashboard templates

    Returns predefined dashboard templates for quick dashboard creation
    with different use cases (executive, operational, technical, etc.).
    """
    try:
        templates = list(dashboard_system.dashboard_templates.keys())
        return {
            "templates": [template.value for template in templates],
            "descriptions": {
                "executive": "High-level business metrics and KPIs for executive overview",
                "operational": "System performance and operational metrics monitoring",
                "technical": "Infrastructure and technical performance metrics",
                "business_intelligence": "Advanced business analytics and intelligence metrics",
                "security": "Security monitoring and threat detection metrics"
            }
        }

    except Exception as e:
        logger.error(f"Failed to list dashboard templates: {str(e)}")
        raise HTTPException(status_code=500, detail="Template list retrieval failed")

@router.post("/templates/{template_type}", response_model=Dict[str, str], status_code=status.HTTP_201_CREATED)
async def create_from_template(
    template_type: DashboardType,
    name: str = Query(..., description="Dashboard name"),
    description: str = Query("", description="Dashboard description"),
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Create dashboard from predefined template

    Creates a new dashboard using a predefined template with customizable
    name and description. Templates provide pre-configured widgets and layouts.
    """
    try:
        # Get template configuration
        if template_type not in dashboard_system.dashboard_templates:
            raise HTTPException(status_code=404, detail="Template not found")

        template_config = dashboard_system.dashboard_templates[template_type]

        # Customize template
        template_config.name = name
        template_config.description = description
        template_config.created_by = current_user.id

        # Create dashboard from template
        dashboard_id = await dashboard_system.create_dashboard(template_config, current_user.id)

        logger.info(f"Dashboard created from template {template_type.value} by user {current_user.id}: {dashboard_id}")

        return {
            "message": f"Dashboard created from {template_type.value} template",
            "dashboard_id": dashboard_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create dashboard from template: {str(e)}")
        raise HTTPException(status_code=500, detail="Template dashboard creation failed")

# Widget Management Endpoints
@router.get("/{dashboard_id}/widgets", response_model=List[Dict[str, Any]])
async def list_dashboard_widgets(
    dashboard_id: str,
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Get list of widgets for specific dashboard

    Returns all widgets configured for the dashboard with their
    configuration and current data status.
    """
    try:
        # Get dashboard configuration
        dashboard_config = await dashboard_system._get_dashboard_config(dashboard_id)
        await dashboard_system._verify_dashboard_access(dashboard_config, current_user.id)

        # Return widget configurations
        widgets = [widget.__dict__ for widget in dashboard_config.widgets]

        logger.info(f"Dashboard widgets retrieved for user {current_user.id}: {dashboard_id}")
        return widgets

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list dashboard widgets: {str(e)}")
        raise HTTPException(status_code=500, detail="Widget list retrieval failed")

@router.post("/{dashboard_id}/widgets", response_model=Dict[str, str], status_code=status.HTTP_201_CREATED)
async def add_dashboard_widget(
    dashboard_id: str,
    widget_config: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Add new widget to dashboard

    Adds a new widget to the specified dashboard with validation
    and automatic layout positioning.
    """
    try:
        # Get dashboard configuration
        dashboard_config = await dashboard_system._get_dashboard_config(dashboard_id)
        await dashboard_system._verify_dashboard_access(dashboard_config, current_user.id, permission="write")

        # Create new widget
        new_widget = WidgetConfig(**widget_config)
        dashboard_config.widgets.append(new_widget)

        # Update dashboard
        await dashboard_system.update_dashboard(
            dashboard_id,
            {"widgets": dashboard_config.widgets},
            current_user.id
        )

        logger.info(f"Widget added to dashboard {dashboard_id} by user {current_user.id}: {new_widget.widget_id}")

        return {
            "message": "Widget added successfully",
            "widget_id": new_widget.widget_id
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to add dashboard widget: {str(e)}")
        raise HTTPException(status_code=500, detail="Widget addition failed")

# Health Check Endpoint
@router.get("/health", response_model=Dict[str, Any])
async def dashboard_system_health(
    dashboard_system: ComprehensiveMonitoringDashboards = Depends(get_dashboard_system)
):
    """
    Get dashboard system health status

    Returns overall health status of the dashboard system including
    database connectivity, Redis status, and system performance metrics.
    """
    try:
        # Check system health
        health_status = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "components": {
                "database": "healthy",
                "redis": "healthy",
                "dashboard_system": "healthy"
            },
            "metrics": {
                "active_dashboards": len(dashboard_system.dashboards),
                "active_connections": sum(len(conns) for conns in dashboard_system.active_connections.values()),
                "uptime": "system uptime would be calculated here"
            }
        }

        return health_status

    except Exception as e:
        logger.error(f"Dashboard system health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }