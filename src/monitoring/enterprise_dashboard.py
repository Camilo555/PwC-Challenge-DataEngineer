"""
Enterprise Dashboard for Real-time Monitoring and Observability
Provides comprehensive dashboards, metrics visualization, and operational insights
"""
import asyncio
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, WebSocket
from fastapi.responses import HTMLResponse

from core.logging import get_logger
from monitoring.advanced_observability import ObservabilityCollector, MetricPoint, MetricType

logger = get_logger(__name__)


class DashboardType(Enum):
    """Dashboard types"""
    OVERVIEW = "overview"
    PERFORMANCE = "performance"
    ERRORS = "errors"
    BUSINESS = "business"
    INFRASTRUCTURE = "infrastructure"
    SECURITY = "security"


@dataclass
class ChartConfig:
    """Chart configuration for dashboard widgets"""
    chart_id: str
    title: str
    chart_type: str  # line, bar, pie, gauge, table
    metrics: List[str]
    time_range: int = 3600  # seconds
    refresh_interval: int = 30  # seconds
    aggregation: str = "avg"  # avg, sum, count, max, min
    filters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.filters is None:
            self.filters = {}


@dataclass
class DashboardWidget:
    """Dashboard widget definition"""
    widget_id: str
    title: str
    widget_type: str  # chart, table, metric, alert
    position: Dict[str, int]  # x, y, width, height
    config: Dict[str, Any]
    data_source: str = "metrics"


@dataclass
class Dashboard:
    """Dashboard definition"""
    dashboard_id: str
    title: str
    description: str
    dashboard_type: DashboardType
    widgets: List[DashboardWidget]
    auto_refresh: bool = True
    refresh_interval: int = 30
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class EnterpriseMetricsCollector:
    """Collects enterprise-specific metrics"""
    
    def __init__(self, observability_collector: ObservabilityCollector):
        self.collector = observability_collector
        self.logger = get_logger(__name__)
        self._collection_task: Optional[asyncio.Task] = None
        self._start_collection()
    
    def _start_collection(self):
        """Start metrics collection"""
        if not self._collection_task or self._collection_task.done():
            self._collection_task = asyncio.create_task(self._collect_system_metrics())
    
    async def _collect_system_metrics(self):
        """Collect system and application metrics"""
        while True:
            try:
                await self._collect_performance_metrics()
                await self._collect_business_metrics()
                await self._collect_security_metrics()
                
                await asyncio.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Metrics collection error: {e}")
                await asyncio.sleep(30)
    
    async def _collect_performance_metrics(self):
        """Collect performance-related metrics"""
        now = datetime.now()
        
        # Simulate system metrics (in production, use psutil or similar)
        await self.collector.record_metric(MetricPoint(
            name="cpu_usage",
            value=45.0,  # Simulated
            timestamp=now,
            metric_type=MetricType.GAUGE,
            labels={"host": "api-server-1"}
        ))
        
        await self.collector.record_metric(MetricPoint(
            name="memory_usage",
            value=78.5,  # Simulated
            timestamp=now,
            metric_type=MetricType.GAUGE,
            labels={"host": "api-server-1"}
        ))
        
        await self.collector.record_metric(MetricPoint(
            name="response_time",
            value=245.0,  # Simulated
            timestamp=now,
            metric_type=MetricType.HISTOGRAM,
            labels={"endpoint": "/api/v1/sales", "method": "GET"}
        ))
    
    async def _collect_business_metrics(self):
        """Collect business-related metrics"""
        now = datetime.now()
        
        # Simulate business metrics
        await self.collector.record_metric(MetricPoint(
            name="sales_revenue",
            value=12500.75,  # Simulated
            timestamp=now,
            metric_type=MetricType.COUNTER,
            labels={"currency": "USD", "region": "north_america"}
        ))
        
        await self.collector.record_metric(MetricPoint(
            name="active_users",
            value=1834,  # Simulated
            timestamp=now,
            metric_type=MetricType.GAUGE,
            labels={"platform": "web"}
        ))
    
    async def _collect_security_metrics(self):
        """Collect security-related metrics"""
        now = datetime.now()
        
        # Simulate security metrics
        await self.collector.record_metric(MetricPoint(
            name="failed_logins",
            value=3,  # Simulated
            timestamp=now,
            metric_type=MetricType.COUNTER,
            labels={"source": "api"}
        ))
        
        await self.collector.record_metric(MetricPoint(
            name="api_rate_limit_hits",
            value=15,  # Simulated
            timestamp=now,
            metric_type=MetricType.COUNTER,
            labels={"endpoint": "/api/v1/sales"}
        ))


class DashboardManager:
    """Manages enterprise dashboards"""
    
    def __init__(self, observability_collector: ObservabilityCollector):
        self.collector = observability_collector
        self.dashboards: Dict[str, Dashboard] = {}
        self.websocket_connections: List[WebSocket] = []
        self.metrics_collector = EnterpriseMetricsCollector(observability_collector)
        
        # Create default dashboards
        self._create_default_dashboards()
        
        # Start real-time updates
        self._update_task: Optional[asyncio.Task] = None
        self._start_updates()
    
    def _start_updates(self):
        """Start real-time dashboard updates"""
        if not self._update_task or self._update_task.done():
            self._update_task = asyncio.create_task(self._broadcast_updates())
    
    async def _broadcast_updates(self):
        """Broadcast real-time updates to connected clients"""
        while True:
            try:
                if self.websocket_connections:
                    # Get latest metrics summary
                    summary = await self.get_real_time_data()
                    
                    # Broadcast to all connected clients
                    disconnected = []
                    for websocket in self.websocket_connections:
                        try:
                            await websocket.send_json(summary)
                        except Exception:
                            disconnected.append(websocket)
                    
                    # Remove disconnected clients
                    for ws in disconnected:
                        self.websocket_connections.remove(ws)
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
            except Exception as e:
                logger.error(f"Dashboard broadcast error: {e}")
                await asyncio.sleep(5)
    
    def _create_default_dashboards(self):
        """Create default enterprise dashboards"""
        
        # Overview Dashboard
        overview_widgets = [
            DashboardWidget(
                widget_id="system_overview",
                title="System Overview",
                widget_type="metric",
                position={"x": 0, "y": 0, "width": 3, "height": 2},
                config={
                    "metrics": ["cpu_usage", "memory_usage", "response_time"],
                    "display_mode": "cards"
                }
            ),
            DashboardWidget(
                widget_id="performance_trend",
                title="Performance Trend",
                widget_type="chart",
                position={"x": 3, "y": 0, "width": 6, "height": 4},
                config={
                    "chart_type": "line",
                    "metrics": ["response_time", "cpu_usage"],
                    "time_range": 3600
                }
            ),
            DashboardWidget(
                widget_id="active_alerts",
                title="Active Alerts",
                widget_type="alert",
                position={"x": 9, "y": 0, "width": 3, "height": 4},
                config={
                    "max_items": 10,
                    "severity_filter": ["error", "critical"]
                }
            )
        ]
        
        overview_dashboard = Dashboard(
            dashboard_id="overview",
            title="System Overview",
            description="High-level system metrics and health",
            dashboard_type=DashboardType.OVERVIEW,
            widgets=overview_widgets
        )
        
        # Performance Dashboard
        performance_widgets = [
            DashboardWidget(
                widget_id="response_times",
                title="Response Times",
                widget_type="chart",
                position={"x": 0, "y": 0, "width": 6, "height": 3},
                config={
                    "chart_type": "line",
                    "metrics": ["response_time"],
                    "time_range": 3600,
                    "aggregation": "p95"
                }
            ),
            DashboardWidget(
                widget_id="throughput",
                title="Request Throughput",
                widget_type="chart",
                position={"x": 6, "y": 0, "width": 6, "height": 3},
                config={
                    "chart_type": "bar",
                    "metrics": ["request_count"],
                    "time_range": 3600,
                    "aggregation": "sum"
                }
            ),
            DashboardWidget(
                widget_id="error_rates",
                title="Error Rates",
                widget_type="chart",
                position={"x": 0, "y": 3, "width": 12, "height": 3},
                config={
                    "chart_type": "line",
                    "metrics": ["error_count"],
                    "time_range": 3600
                }
            )
        ]
        
        performance_dashboard = Dashboard(
            dashboard_id="performance",
            title="Performance Metrics",
            description="Detailed performance and latency metrics",
            dashboard_type=DashboardType.PERFORMANCE,
            widgets=performance_widgets
        )
        
        # Business Dashboard
        business_widgets = [
            DashboardWidget(
                widget_id="revenue_trend",
                title="Revenue Trend",
                widget_type="chart",
                position={"x": 0, "y": 0, "width": 8, "height": 4},
                config={
                    "chart_type": "line",
                    "metrics": ["sales_revenue"],
                    "time_range": 86400  # 24 hours
                }
            ),
            DashboardWidget(
                widget_id="active_users_gauge",
                title="Active Users",
                widget_type="chart",
                position={"x": 8, "y": 0, "width": 4, "height": 4},
                config={
                    "chart_type": "gauge",
                    "metrics": ["active_users"],
                    "max_value": 5000
                }
            )
        ]
        
        business_dashboard = Dashboard(
            dashboard_id="business",
            title="Business Metrics",
            description="Key business indicators and KPIs",
            dashboard_type=DashboardType.BUSINESS,
            widgets=business_widgets
        )
        
        # Store dashboards
        self.dashboards = {
            overview_dashboard.dashboard_id: overview_dashboard,
            performance_dashboard.dashboard_id: performance_dashboard,
            business_dashboard.dashboard_id: business_dashboard
        }
    
    async def get_dashboard(self, dashboard_id: str) -> Optional[Dashboard]:
        """Get dashboard by ID"""
        return self.dashboards.get(dashboard_id)
    
    async def list_dashboards(self) -> List[Dict[str, Any]]:
        """List all available dashboards"""
        return [
            {
                "dashboard_id": dashboard.dashboard_id,
                "title": dashboard.title,
                "description": dashboard.description,
                "type": dashboard.dashboard_type.value,
                "widget_count": len(dashboard.widgets)
            }
            for dashboard in self.dashboards.values()
        ]
    
    async def get_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Get data for a specific widget"""
        if widget.widget_type == "metric":
            return await self._get_metric_widget_data(widget)
        elif widget.widget_type == "chart":
            return await self._get_chart_widget_data(widget)
        elif widget.widget_type == "alert":
            return await self._get_alert_widget_data(widget)
        elif widget.widget_type == "table":
            return await self._get_table_widget_data(widget)
        else:
            return {"error": f"Unknown widget type: {widget.widget_type}"}
    
    async def _get_metric_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Get data for metric widgets"""
        metrics = widget.config.get("metrics", [])
        data = {}
        
        for metric_name in metrics:
            # Get recent metric values
            recent_values = []
            for metric_key, metric_deque in self.collector.metrics.items():
                if metric_name in metric_key:
                    cutoff_time = datetime.now() - timedelta(minutes=5)
                    recent_values.extend([
                        m.value for m in metric_deque 
                        if m.timestamp > cutoff_time
                    ])
            
            if recent_values:
                data[metric_name] = {
                    "current": recent_values[-1],
                    "average": statistics.mean(recent_values),
                    "trend": "up" if len(recent_values) > 1 and recent_values[-1] > recent_values[0] else "down"
                }
            else:
                data[metric_name] = {"current": 0, "average": 0, "trend": "flat"}
        
        return {"type": "metric", "data": data}
    
    async def _get_chart_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Get data for chart widgets"""
        metrics = widget.config.get("metrics", [])
        time_range = widget.config.get("time_range", 3600)
        chart_type = widget.config.get("chart_type", "line")
        
        cutoff_time = datetime.now() - timedelta(seconds=time_range)
        chart_data = {}
        
        for metric_name in metrics:
            time_series = []
            for metric_key, metric_deque in self.collector.metrics.items():
                if metric_name in metric_key:
                    for metric in metric_deque:
                        if metric.timestamp > cutoff_time:
                            time_series.append({
                                "timestamp": metric.timestamp.isoformat(),
                                "value": metric.value
                            })
            
            # Sort by timestamp
            time_series.sort(key=lambda x: x["timestamp"])
            chart_data[metric_name] = time_series
        
        return {
            "type": "chart",
            "chart_type": chart_type,
            "data": chart_data
        }
    
    async def _get_alert_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Get data for alert widgets"""
        active_alerts = await self.collector.get_active_alerts()
        max_items = widget.config.get("max_items", 10)
        severity_filter = widget.config.get("severity_filter", [])
        
        # Filter and sort alerts
        filtered_alerts = active_alerts[:max_items]
        
        alert_data = []
        for alert in filtered_alerts:
            alert_data.append({
                "id": alert.instance_id,
                "message": alert.message,
                "severity": "warning",  # Default severity
                "triggered_at": alert.triggered_at.isoformat(),
                "value": alert.current_value
            })
        
        return {
            "type": "alert",
            "data": alert_data,
            "total_count": len(active_alerts)
        }
    
    async def _get_table_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Get data for table widgets"""
        # Implement table data logic based on config
        return {"type": "table", "data": [], "columns": []}
    
    async def get_real_time_data(self) -> Dict[str, Any]:
        """Get real-time data for all dashboards"""
        summary = await self.collector.get_metrics_summary()
        performance_trends = await self.collector.get_performance_trends(hours=1)
        active_alerts = await self.collector.get_active_alerts()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "metrics_summary": summary,
            "performance_trends": [asdict(trend) for trend in performance_trends[-10:]],  # Last 10 snapshots
            "active_alerts": len(active_alerts),
            "system_health": self._calculate_system_health(summary, active_alerts)
        }
    
    def _calculate_system_health(self, summary: Dict[str, Any], alerts: List) -> str:
        """Calculate overall system health"""
        if len(alerts) > 5:
            return "critical"
        elif len(alerts) > 2:
            return "warning"
        elif summary.get("total_metrics", 0) > 0:
            return "healthy"
        else:
            return "unknown"
    
    async def add_websocket_connection(self, websocket: WebSocket):
        """Add WebSocket connection for real-time updates"""
        self.websocket_connections.append(websocket)
    
    async def remove_websocket_connection(self, websocket: WebSocket):
        """Remove WebSocket connection"""
        if websocket in self.websocket_connections:
            self.websocket_connections.remove(websocket)
    
    async def close(self):
        """Clean up resources"""
        if self._update_task and not self._update_task.done():
            self._update_task.cancel()


# API Router for dashboard endpoints
def create_dashboard_router(dashboard_manager: DashboardManager) -> APIRouter:
    """Create dashboard API router"""
    router = APIRouter(prefix="/dashboard", tags=["dashboard"])
    
    @router.get("/")
    async def list_dashboards():
        """List all available dashboards"""
        return await dashboard_manager.list_dashboards()
    
    @router.get("/{dashboard_id}")
    async def get_dashboard(dashboard_id: str):
        """Get dashboard configuration"""
        dashboard = await dashboard_manager.get_dashboard(dashboard_id)
        if not dashboard:
            raise HTTPException(status_code=404, detail="Dashboard not found")
        
        return asdict(dashboard)
    
    @router.get("/{dashboard_id}/data")
    async def get_dashboard_data(dashboard_id: str):
        """Get data for all widgets in a dashboard"""
        dashboard = await dashboard_manager.get_dashboard(dashboard_id)
        if not dashboard:
            raise HTTPException(status_code=404, detail="Dashboard not found")
        
        widget_data = {}
        for widget in dashboard.widgets:
            widget_data[widget.widget_id] = await dashboard_manager.get_widget_data(widget)
        
        return {
            "dashboard_id": dashboard_id,
            "timestamp": datetime.now().isoformat(),
            "widgets": widget_data
        }
    
    @router.get("/{dashboard_id}/widget/{widget_id}")
    async def get_widget_data(dashboard_id: str, widget_id: str):
        """Get data for a specific widget"""
        dashboard = await dashboard_manager.get_dashboard(dashboard_id)
        if not dashboard:
            raise HTTPException(status_code=404, detail="Dashboard not found")
        
        widget = next((w for w in dashboard.widgets if w.widget_id == widget_id), None)
        if not widget:
            raise HTTPException(status_code=404, detail="Widget not found")
        
        return await dashboard_manager.get_widget_data(widget)
    
    @router.websocket("/realtime")
    async def websocket_endpoint(websocket: WebSocket):
        """WebSocket endpoint for real-time dashboard updates"""
        await websocket.accept()
        await dashboard_manager.add_websocket_connection(websocket)
        
        try:
            while True:
                # Keep connection alive
                await websocket.receive_text()
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await dashboard_manager.remove_websocket_connection(websocket)
    
    @router.get("/health/status")
    async def get_health_status():
        """Get overall system health status"""
        real_time_data = await dashboard_manager.get_real_time_data()
        return {
            "status": real_time_data["system_health"],
            "timestamp": real_time_data["timestamp"],
            "metrics_count": real_time_data["metrics_summary"]["total_metrics"],
            "active_alerts": real_time_data["active_alerts"]
        }
    
    return router


# Simple HTML dashboard for development/demo
SIMPLE_DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Enterprise Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .dashboard { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .widget { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric-card { text-align: center; padding: 20px; }
        .metric-value { font-size: 2em; font-weight: bold; color: #2196F3; }
        .metric-label { color: #666; margin-top: 10px; }
        .alert { background: #ffebee; border-left: 4px solid #f44336; padding: 10px; margin: 5px 0; }
        .status-healthy { color: #4CAF50; }
        .status-warning { color: #FF9800; }
        .status-critical { color: #f44336; }
    </style>
</head>
<body>
    <h1>Enterprise Monitoring Dashboard</h1>
    <div id="status" class="widget">
        <h3>System Health</h3>
        <div id="health-status" class="status-healthy">Loading...</div>
    </div>
    
    <div class="dashboard">
        <div class="widget">
            <h3>CPU Usage</h3>
            <div class="metric-card">
                <div id="cpu-metric" class="metric-value">--</div>
                <div class="metric-label">Percent</div>
            </div>
        </div>
        
        <div class="widget">
            <h3>Memory Usage</h3>
            <div class="metric-card">
                <div id="memory-metric" class="metric-value">--</div>
                <div class="metric-label">Percent</div>
            </div>
        </div>
        
        <div class="widget">
            <h3>Response Time</h3>
            <div class="metric-card">
                <div id="response-time-metric" class="metric-value">--</div>
                <div class="metric-label">Milliseconds</div>
            </div>
        </div>
        
        <div class="widget">
            <h3>Active Alerts</h3>
            <div id="alerts-container">Loading...</div>
        </div>
    </div>

    <script>
        // WebSocket connection for real-time updates
        const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${location.host}/api/v1/monitoring/dashboard/realtime`;
        
        let ws;
        
        function connectWebSocket() {
            ws = new WebSocket(wsUrl);
            
            ws.onopen = function() {
                console.log('Connected to dashboard WebSocket');
            };
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };
            
            ws.onclose = function() {
                console.log('Dashboard WebSocket connection closed');
                setTimeout(connectWebSocket, 5000); // Reconnect after 5 seconds
            };
            
            ws.onerror = function(error) {
                console.error('Dashboard WebSocket error:', error);
            };
        }
        
        function updateDashboard(data) {
            // Update system health
            const healthElement = document.getElementById('health-status');
            healthElement.textContent = data.system_health.toUpperCase();
            healthElement.className = `status-${data.system_health}`;
            
            // Update metrics (mock data for demo)
            document.getElementById('cpu-metric').textContent = '45.2%';
            document.getElementById('memory-metric').textContent = '78.5%';
            document.getElementById('response-time-metric').textContent = '245ms';
            
            // Update alerts
            const alertsContainer = document.getElementById('alerts-container');
            if (data.active_alerts > 0) {
                alertsContainer.innerHTML = `<div class="alert">${data.active_alerts} active alerts</div>`;
            } else {
                alertsContainer.innerHTML = '<div style="color: #4CAF50;">No active alerts</div>';
            }
        }
        
        // Connect to WebSocket
        connectWebSocket();
        
        // Fallback: poll for updates if WebSocket fails
        setInterval(async () => {
            if (ws.readyState !== WebSocket.OPEN) {
                try {
                    const response = await fetch('/api/v1/monitoring/dashboard/health/status');
                    const data = await response.json();
                    updateDashboard({
                        system_health: data.status,
                        active_alerts: data.active_alerts
                    });
                } catch (error) {
                    console.error('Failed to fetch health status:', error);
                }
            }
        }, 30000);
    </script>
</body>
</html>
"""