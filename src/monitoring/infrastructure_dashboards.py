"""
Comprehensive Infrastructure Dashboards
Provides executive, technical, and troubleshooting dashboards
with real-time metrics, visualizations, and interactive components.
"""
import json
import time
import statistics
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union
from collections import defaultdict, deque

from core.config import settings
from core.logging import get_logger
from .infrastructure_health_monitor import (
    InfrastructureHealthManager, HealthStatus, ComponentType,
    RedisHealthMetrics, RabbitMQHealthMetrics, KafkaHealthMetrics
)
from .intelligent_alerting_system import IntelligentAlertingSystem, AlertSeverity

logger = get_logger(__name__)


class DashboardType(Enum):
    """Dashboard types"""
    EXECUTIVE = "executive"
    TECHNICAL = "technical"
    TROUBLESHOOTING = "troubleshooting"
    REAL_TIME = "real_time"


class VisualizationType(Enum):
    """Visualization types"""
    GAUGE = "gauge"
    LINE_CHART = "line_chart"
    BAR_CHART = "bar_chart"
    PIE_CHART = "pie_chart"
    HEATMAP = "heatmap"
    TABLE = "table"
    METRIC_CARD = "metric_card"
    STATUS_INDICATOR = "status_indicator"
    TIMELINE = "timeline"


@dataclass
class DashboardWidget:
    """Dashboard widget definition"""
    id: str
    title: str
    type: VisualizationType
    data_source: str
    refresh_interval: int = 30  # seconds
    size: str = "medium"  # small, medium, large
    position: Dict[str, int] = field(default_factory=dict)  # x, y, width, height
    config: Dict[str, Any] = field(default_factory=dict)
    filters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Dashboard:
    """Dashboard definition"""
    id: str
    name: str
    description: str
    type: DashboardType
    widgets: List[DashboardWidget]
    layout: Dict[str, Any] = field(default_factory=dict)
    permissions: Dict[str, Any] = field(default_factory=dict)
    auto_refresh: bool = True
    refresh_interval: int = 30


class DashboardDataProvider(ABC):
    """Base class for dashboard data providers"""
    
    @abstractmethod
    async def get_data(self, widget: DashboardWidget, 
                      time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Get data for widget"""
        pass


class InfrastructureDataProvider(DashboardDataProvider):
    """Infrastructure health data provider"""
    
    def __init__(self, health_manager: InfrastructureHealthManager,
                 alerting_system: IntelligentAlertingSystem):
        self.health_manager = health_manager
        self.alerting_system = alerting_system
    
    async def get_data(self, widget: DashboardWidget, 
                      time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Get infrastructure data"""
        try:
            if widget.data_source == "system_health":
                return await self._get_system_health_data()
            elif widget.data_source == "component_performance":
                return await self._get_component_performance_data(time_range)
            elif widget.data_source == "alert_summary":
                return await self._get_alert_summary_data()
            elif widget.data_source == "availability_metrics":
                return await self._get_availability_metrics_data(time_range)
            elif widget.data_source == "redis_metrics":
                return await self._get_redis_metrics_data(time_range)
            elif widget.data_source == "rabbitmq_metrics":
                return await self._get_rabbitmq_metrics_data(time_range)
            elif widget.data_source == "kafka_metrics":
                return await self._get_kafka_metrics_data(time_range)
            elif widget.data_source == "integration_health":
                return await self._get_integration_health_data()
            elif widget.data_source == "performance_baselines":
                return await self._get_performance_baseline_data()
            else:
                return {"error": f"Unknown data source: {widget.data_source}"}
        
        except Exception as e:
            logger.error(f"Error getting data for widget {widget.id}: {e}")
            return {"error": str(e)}
    
    async def _get_system_health_data(self) -> Dict[str, Any]:
        """Get system health overview data"""
        health_status = await self.health_manager.get_system_health()
        
        return {
            "overall_status": health_status.get("overall_status"),
            "total_components": health_status.get("total_components", 0),
            "healthy_components": health_status.get("healthy_components", 0),
            "critical_issues": health_status.get("critical_issues", []),
            "warning_issues": health_status.get("warning_issues", []),
            "components": health_status.get("components", {}),
            "timestamp": health_status.get("timestamp")
        }
    
    async def _get_component_performance_data(self, time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Get component performance metrics"""
        performance_summary = await self.health_manager.get_performance_summary()
        
        # Format for visualization
        components = []
        for component_type, component_data in performance_summary.items():
            if component_type == "overall":
                continue
            
            for component_id, metrics in component_data.items():
                components.append({
                    "component_id": component_id,
                    "component_type": component_type,
                    "response_time_ms": metrics.get("response_time_ms", 0),
                    "availability_24h": metrics.get("availability_24h", 0),
                    "status": metrics.get("status", "unknown")
                })
        
        return {
            "components": components,
            "overall": performance_summary.get("overall", {}),
            "timestamp": datetime.now().isoformat()
        }
    
    async def _get_alert_summary_data(self) -> Dict[str, Any]:
        """Get alert summary data"""
        alert_stats = self.alerting_system.get_alert_statistics()
        active_alerts = self.alerting_system.get_active_alerts()
        
        # Group alerts by severity and component
        alerts_by_severity = defaultdict(int)
        alerts_by_component = defaultdict(int)
        
        for alert in active_alerts:
            alerts_by_severity[alert['alert']['severity']] += 1
            alerts_by_component[alert['alert']['component_id']] += 1
        
        return {
            "active_count": alert_stats.get("total_active", 0),
            "by_severity": dict(alerts_by_severity),
            "by_component": dict(alerts_by_component),
            "last_24h": alert_stats.get("last_24h", {}),
            "recent_alerts": active_alerts[:10],  # Last 10 alerts
            "timestamp": datetime.now().isoformat()
        }
    
    async def _get_availability_metrics_data(self, time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Get availability metrics data"""
        availability_data = []
        
        for monitor in self.health_manager.monitors.values():
            hours = int((time_range.get('end', datetime.now()) - 
                        time_range.get('start', datetime.now() - timedelta(hours=24))).total_seconds() / 3600)
            
            availability = monitor.calculate_availability(hours=hours)
            
            availability_data.append({
                "component_id": monitor.component_id,
                "component_type": monitor.component_type.value,
                "availability_percent": availability,
                "uptime_hours": hours * (availability / 100),
                "downtime_hours": hours * ((100 - availability) / 100),
                "sla_target": 99.9,
                "sla_breach": availability < 99.9
            })
        
        return {
            "components": availability_data,
            "average_availability": statistics.mean([c["availability_percent"] for c in availability_data]) if availability_data else 0,
            "timestamp": datetime.now().isoformat()
        }
    
    async def _get_redis_metrics_data(self, time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Get Redis-specific metrics"""
        redis_data = []
        
        for monitor in self.health_manager.monitors.values():
            if monitor.component_type != ComponentType.REDIS:
                continue
            
            latest_metrics = monitor.get_latest_metrics()
            if isinstance(latest_metrics, RedisHealthMetrics):
                redis_data.append({
                    "component_id": monitor.component_id,
                    "memory_usage_percent": latest_metrics.memory_usage_percent,
                    "memory_usage_mb": latest_metrics.memory_usage_mb,
                    "cache_hit_ratio": latest_metrics.cache_hit_ratio,
                    "connected_clients": latest_metrics.connected_clients,
                    "operations_per_second": latest_metrics.operations_per_second,
                    "keyspace_hits": latest_metrics.keyspace_hits,
                    "keyspace_misses": latest_metrics.keyspace_misses,
                    "evicted_keys": latest_metrics.evicted_keys,
                    "cluster_state": latest_metrics.cluster_state,
                    "status": latest_metrics.status.value,
                    "response_time_ms": latest_metrics.response_time_ms,
                    "timestamp": latest_metrics.timestamp.isoformat()
                })
        
        return {
            "instances": redis_data,
            "total_instances": len(redis_data),
            "healthy_instances": len([r for r in redis_data if r["status"] == "healthy"]),
            "timestamp": datetime.now().isoformat()
        }
    
    async def _get_rabbitmq_metrics_data(self, time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Get RabbitMQ-specific metrics"""
        rabbitmq_data = []
        
        for monitor in self.health_manager.monitors.values():
            if monitor.component_type != ComponentType.RABBITMQ:
                continue
            
            latest_metrics = monitor.get_latest_metrics()
            if isinstance(latest_metrics, RabbitMQHealthMetrics):
                rabbitmq_data.append({
                    "component_id": monitor.component_id,
                    "queue_depth_total": latest_metrics.queue_depth_total,
                    "message_rate_per_sec": latest_metrics.message_rate_per_sec,
                    "consumer_count": latest_metrics.consumer_count,
                    "connection_count": latest_metrics.connection_count,
                    "unacknowledged_messages": latest_metrics.unacknowledged_messages,
                    "memory_usage_mb": latest_metrics.memory_usage_mb,
                    "queue_metrics": latest_metrics.queue_metrics,
                    "node_status": latest_metrics.node_status,
                    "status": latest_metrics.status.value,
                    "response_time_ms": latest_metrics.response_time_ms,
                    "timestamp": latest_metrics.timestamp.isoformat()
                })
        
        return {
            "instances": rabbitmq_data,
            "total_instances": len(rabbitmq_data),
            "total_queues": sum(len(r.get("queue_metrics", {})) for r in rabbitmq_data),
            "total_messages": sum(r.get("queue_depth_total", 0) for r in rabbitmq_data),
            "timestamp": datetime.now().isoformat()
        }
    
    async def _get_kafka_metrics_data(self, time_range: Dict[str, datetime]) -> Dict[str, Any]:
        """Get Kafka-specific metrics"""
        kafka_data = []
        
        for monitor in self.health_manager.monitors.values():
            if monitor.component_type != ComponentType.KAFKA:
                continue
            
            latest_metrics = monitor.get_latest_metrics()
            if isinstance(latest_metrics, KafkaHealthMetrics):
                kafka_data.append({
                    "component_id": monitor.component_id,
                    "broker_count": latest_metrics.broker_count,
                    "topic_count": latest_metrics.topic_count,
                    "partition_count": latest_metrics.partition_count,
                    "under_replicated_partitions": latest_metrics.under_replicated_partitions,
                    "offline_partitions": latest_metrics.offline_partitions,
                    "consumer_lag_total": latest_metrics.consumer_lag_total,
                    "messages_per_sec": latest_metrics.messages_per_sec,
                    "bytes_per_sec": latest_metrics.bytes_per_sec,
                    "consumer_group_metrics": latest_metrics.consumer_group_metrics,
                    "status": latest_metrics.status.value,
                    "response_time_ms": latest_metrics.response_time_ms,
                    "timestamp": latest_metrics.timestamp.isoformat()
                })
        
        return {
            "clusters": kafka_data,
            "total_clusters": len(kafka_data),
            "total_brokers": sum(k.get("broker_count", 0) for k in kafka_data),
            "total_topics": sum(k.get("topic_count", 0) for k in kafka_data),
            "total_partitions": sum(k.get("partition_count", 0) for k in kafka_data),
            "timestamp": datetime.now().isoformat()
        }
    
    async def _get_integration_health_data(self) -> Dict[str, Any]:
        """Get cross-system integration health"""
        health_status = await self.health_manager.get_system_health()
        integration_health = health_status.get("integration_health", {})
        
        return {
            "integrations": integration_health,
            "total_integrations": len(integration_health),
            "healthy_integrations": len([i for i in integration_health.values() if i.get("status") == "healthy"]),
            "timestamp": datetime.now().isoformat()
        }
    
    async def _get_performance_baseline_data(self) -> Dict[str, Any]:
        """Get performance baseline data"""
        health_status = await self.health_manager.get_system_health()
        baselines = health_status.get("baselines", {})
        
        baseline_data = []
        for baseline_key, baseline_info in baselines.items():
            baseline_data.append({
                "component_id": baseline_info.get("component_id"),
                "metric_name": baseline_info.get("metric_name"),
                "baseline_value": baseline_info.get("baseline_value"),
                "current_value": baseline_info.get("current_value"),
                "deviation_percent": baseline_info.get("deviation_percent"),
                "trend": baseline_info.get("trend"),
                "sample_count": baseline_info.get("sample_count"),
                "last_updated": baseline_info.get("last_updated")
            })
        
        return {
            "baselines": baseline_data,
            "total_baselines": len(baseline_data),
            "degrading_count": len([b for b in baseline_data if b.get("trend") == "degrading"]),
            "timestamp": datetime.now().isoformat()
        }


class DashboardManager:
    """Central dashboard management system"""
    
    def __init__(self, health_manager: InfrastructureHealthManager,
                 alerting_system: IntelligentAlertingSystem):
        self.health_manager = health_manager
        self.alerting_system = alerting_system
        self.logger = get_logger(__name__)
        
        # Data provider
        self.data_provider = InfrastructureDataProvider(health_manager, alerting_system)
        
        # Dashboard definitions
        self.dashboards: Dict[str, Dashboard] = {}
        
        # Create default dashboards
        self._create_default_dashboards()
    
    def _create_default_dashboards(self):
        """Create default dashboard definitions"""
        # Executive Dashboard
        executive_widgets = [
            DashboardWidget(
                id="system_overview",
                title="System Health Overview",
                type=VisualizationType.STATUS_INDICATOR,
                data_source="system_health",
                size="large",
                position={"x": 0, "y": 0, "width": 12, "height": 4}
            ),
            DashboardWidget(
                id="availability_summary",
                title="Service Availability (24h)",
                type=VisualizationType.GAUGE,
                data_source="availability_metrics",
                size="medium",
                position={"x": 0, "y": 4, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="alert_summary",
                title="Active Alerts",
                type=VisualizationType.PIE_CHART,
                data_source="alert_summary",
                size="medium",
                position={"x": 6, "y": 4, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="performance_trends",
                title="Performance Trends",
                type=VisualizationType.LINE_CHART,
                data_source="component_performance",
                size="large",
                position={"x": 0, "y": 8, "width": 12, "height": 4}
            ),
            DashboardWidget(
                id="critical_alerts",
                title="Critical Issues",
                type=VisualizationType.TABLE,
                data_source="alert_summary",
                size="large",
                position={"x": 0, "y": 12, "width": 12, "height": 4},
                filters={"severity": "critical"}
            )
        ]
        
        self.dashboards["executive"] = Dashboard(
            id="executive",
            name="Executive Dashboard",
            description="High-level overview of infrastructure health and performance",
            type=DashboardType.EXECUTIVE,
            widgets=executive_widgets,
            refresh_interval=60
        )
        
        # Technical Dashboard
        technical_widgets = [
            DashboardWidget(
                id="redis_performance",
                title="Redis Performance",
                type=VisualizationType.METRIC_CARD,
                data_source="redis_metrics",
                size="medium",
                position={"x": 0, "y": 0, "width": 4, "height": 3}
            ),
            DashboardWidget(
                id="rabbitmq_performance",
                title="RabbitMQ Performance",
                type=VisualizationType.METRIC_CARD,
                data_source="rabbitmq_metrics",
                size="medium",
                position={"x": 4, "y": 0, "width": 4, "height": 3}
            ),
            DashboardWidget(
                id="kafka_performance",
                title="Kafka Performance",
                type=VisualizationType.METRIC_CARD,
                data_source="kafka_metrics",
                size="medium",
                position={"x": 8, "y": 0, "width": 4, "height": 3}
            ),
            DashboardWidget(
                id="response_time_chart",
                title="Response Time Trends",
                type=VisualizationType.LINE_CHART,
                data_source="component_performance",
                size="large",
                position={"x": 0, "y": 3, "width": 12, "height": 4}
            ),
            DashboardWidget(
                id="memory_usage_heatmap",
                title="Memory Usage Heatmap",
                type=VisualizationType.HEATMAP,
                data_source="redis_metrics",
                size="medium",
                position={"x": 0, "y": 7, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="queue_depth_chart",
                title="Queue Depths",
                type=VisualizationType.BAR_CHART,
                data_source="rabbitmq_metrics",
                size="medium",
                position={"x": 6, "y": 7, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="baseline_deviations",
                title="Performance Baseline Deviations",
                type=VisualizationType.TABLE,
                data_source="performance_baselines",
                size="large",
                position={"x": 0, "y": 11, "width": 12, "height": 4}
            )
        ]
        
        self.dashboards["technical"] = Dashboard(
            id="technical",
            name="Technical Dashboard",
            description="Detailed technical metrics and performance data",
            type=DashboardType.TECHNICAL,
            widgets=technical_widgets,
            refresh_interval=30
        )
        
        # Troubleshooting Dashboard
        troubleshooting_widgets = [
            DashboardWidget(
                id="alert_timeline",
                title="Alert Timeline",
                type=VisualizationType.TIMELINE,
                data_source="alert_summary",
                size="large",
                position={"x": 0, "y": 0, "width": 12, "height": 4}
            ),
            DashboardWidget(
                id="component_errors",
                title="Component Error Rates",
                type=VisualizationType.BAR_CHART,
                data_source="component_performance",
                size="medium",
                position={"x": 0, "y": 4, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="integration_status",
                title="Integration Health",
                type=VisualizationType.STATUS_INDICATOR,
                data_source="integration_health",
                size="medium",
                position={"x": 6, "y": 4, "width": 6, "height": 4}
            ),
            DashboardWidget(
                id="detailed_metrics",
                title="Detailed Component Metrics",
                type=VisualizationType.TABLE,
                data_source="component_performance",
                size="large",
                position={"x": 0, "y": 8, "width": 12, "height": 6}
            )
        ]
        
        self.dashboards["troubleshooting"] = Dashboard(
            id="troubleshooting",
            name="Troubleshooting Dashboard",
            description="Detailed troubleshooting and diagnostic information",
            type=DashboardType.TROUBLESHOOTING,
            widgets=troubleshooting_widgets,
            refresh_interval=15
        )
        
        # Real-time Dashboard
        realtime_widgets = [
            DashboardWidget(
                id="live_status",
                title="Live System Status",
                type=VisualizationType.STATUS_INDICATOR,
                data_source="system_health",
                size="large",
                position={"x": 0, "y": 0, "width": 12, "height": 2},
                refresh_interval=5
            ),
            DashboardWidget(
                id="live_redis_metrics",
                title="Live Redis Metrics",
                type=VisualizationType.GAUGE,
                data_source="redis_metrics",
                size="medium",
                position={"x": 0, "y": 2, "width": 4, "height": 4},
                refresh_interval=5
            ),
            DashboardWidget(
                id="live_rabbitmq_metrics",
                title="Live RabbitMQ Metrics",
                type=VisualizationType.GAUGE,
                data_source="rabbitmq_metrics",
                size="medium",
                position={"x": 4, "y": 2, "width": 4, "height": 4},
                refresh_interval=5
            ),
            DashboardWidget(
                id="live_kafka_metrics",
                title="Live Kafka Metrics",
                type=VisualizationType.GAUGE,
                data_source="kafka_metrics",
                size="medium",
                position={"x": 8, "y": 2, "width": 4, "height": 4},
                refresh_interval=5
            ),
            DashboardWidget(
                id="recent_alerts_feed",
                title="Recent Alerts Feed",
                type=VisualizationType.TABLE,
                data_source="alert_summary",
                size="large",
                position={"x": 0, "y": 6, "width": 12, "height": 6},
                refresh_interval=10
            )
        ]
        
        self.dashboards["real_time"] = Dashboard(
            id="real_time",
            name="Real-time Monitoring",
            description="Real-time system monitoring with live updates",
            type=DashboardType.REAL_TIME,
            widgets=realtime_widgets,
            refresh_interval=5
        )
    
    async def get_dashboard_data(self, dashboard_id: str, 
                               time_range: Dict[str, datetime] = None) -> Dict[str, Any]:
        """Get complete dashboard data"""
        if dashboard_id not in self.dashboards:
            return {"error": f"Dashboard {dashboard_id} not found"}
        
        dashboard = self.dashboards[dashboard_id]
        
        if time_range is None:
            time_range = {
                "start": datetime.now() - timedelta(hours=24),
                "end": datetime.now()
            }
        
        dashboard_data = {
            "dashboard": asdict(dashboard),
            "widget_data": {},
            "last_updated": datetime.now().isoformat()
        }
        
        # Get data for each widget
        for widget in dashboard.widgets:
            try:
                widget_data = await self.data_provider.get_data(widget, time_range)
                dashboard_data["widget_data"][widget.id] = widget_data
            except Exception as e:
                self.logger.error(f"Error getting data for widget {widget.id}: {e}")
                dashboard_data["widget_data"][widget.id] = {"error": str(e)}
        
        return dashboard_data
    
    async def get_widget_data(self, dashboard_id: str, widget_id: str,
                             time_range: Dict[str, datetime] = None) -> Dict[str, Any]:
        """Get data for a specific widget"""
        if dashboard_id not in self.dashboards:
            return {"error": f"Dashboard {dashboard_id} not found"}
        
        dashboard = self.dashboards[dashboard_id]
        widget = None
        
        for w in dashboard.widgets:
            if w.id == widget_id:
                widget = w
                break
        
        if not widget:
            return {"error": f"Widget {widget_id} not found in dashboard {dashboard_id}"}
        
        if time_range is None:
            time_range = {
                "start": datetime.now() - timedelta(hours=24),
                "end": datetime.now()
            }
        
        try:
            return await self.data_provider.get_data(widget, time_range)
        except Exception as e:
            self.logger.error(f"Error getting data for widget {widget_id}: {e}")
            return {"error": str(e)}
    
    def get_dashboard_list(self) -> List[Dict[str, Any]]:
        """Get list of available dashboards"""
        dashboard_list = []
        
        for dashboard_id, dashboard in self.dashboards.items():
            dashboard_list.append({
                "id": dashboard.id,
                "name": dashboard.name,
                "description": dashboard.description,
                "type": dashboard.type.value,
                "widget_count": len(dashboard.widgets),
                "refresh_interval": dashboard.refresh_interval,
                "auto_refresh": dashboard.auto_refresh
            })
        
        return sorted(dashboard_list, key=lambda x: x["name"])
    
    def add_dashboard(self, dashboard: Dashboard):
        """Add custom dashboard"""
        self.dashboards[dashboard.id] = dashboard
        self.logger.info(f"Added dashboard: {dashboard.name}")
    
    def remove_dashboard(self, dashboard_id: str):
        """Remove dashboard"""
        if dashboard_id in self.dashboards:
            del self.dashboards[dashboard_id]
            self.logger.info(f"Removed dashboard: {dashboard_id}")
    
    def add_widget_to_dashboard(self, dashboard_id: str, widget: DashboardWidget):
        """Add widget to existing dashboard"""
        if dashboard_id in self.dashboards:
            self.dashboards[dashboard_id].widgets.append(widget)
            self.logger.info(f"Added widget {widget.title} to dashboard {dashboard_id}")
    
    def remove_widget_from_dashboard(self, dashboard_id: str, widget_id: str):
        """Remove widget from dashboard"""
        if dashboard_id in self.dashboards:
            dashboard = self.dashboards[dashboard_id]
            dashboard.widgets = [w for w in dashboard.widgets if w.id != widget_id]
            self.logger.info(f"Removed widget {widget_id} from dashboard {dashboard_id}")
    
    async def export_dashboard_config(self, dashboard_id: str) -> Dict[str, Any]:
        """Export dashboard configuration"""
        if dashboard_id not in self.dashboards:
            return {"error": f"Dashboard {dashboard_id} not found"}
        
        dashboard = self.dashboards[dashboard_id]
        return {
            "dashboard_config": asdict(dashboard),
            "exported_at": datetime.now().isoformat(),
            "version": "1.0"
        }
    
    async def import_dashboard_config(self, config: Dict[str, Any]) -> bool:
        """Import dashboard configuration"""
        try:
            dashboard_config = config.get("dashboard_config", {})
            
            # Create dashboard from config
            widgets = []
            for widget_config in dashboard_config.get("widgets", []):
                widget = DashboardWidget(
                    id=widget_config["id"],
                    title=widget_config["title"],
                    type=VisualizationType(widget_config["type"]),
                    data_source=widget_config["data_source"],
                    refresh_interval=widget_config.get("refresh_interval", 30),
                    size=widget_config.get("size", "medium"),
                    position=widget_config.get("position", {}),
                    config=widget_config.get("config", {}),
                    filters=widget_config.get("filters", {})
                )
                widgets.append(widget)
            
            dashboard = Dashboard(
                id=dashboard_config["id"],
                name=dashboard_config["name"],
                description=dashboard_config["description"],
                type=DashboardType(dashboard_config["type"]),
                widgets=widgets,
                layout=dashboard_config.get("layout", {}),
                permissions=dashboard_config.get("permissions", {}),
                auto_refresh=dashboard_config.get("auto_refresh", True),
                refresh_interval=dashboard_config.get("refresh_interval", 30)
            )
            
            self.add_dashboard(dashboard)
            return True
        
        except Exception as e:
            self.logger.error(f"Error importing dashboard config: {e}")
            return False
    
    async def get_dashboard_health(self) -> Dict[str, Any]:
        """Get dashboard system health"""
        return {
            "total_dashboards": len(self.dashboards),
            "dashboard_types": {
                dashboard_type.value: len([
                    d for d in self.dashboards.values() 
                    if d.type == dashboard_type
                ]) for dashboard_type in DashboardType
            },
            "total_widgets": sum(len(d.widgets) for d in self.dashboards.values()),
            "data_provider_status": "healthy",
            "last_updated": datetime.now().isoformat()
        }


# Factory function
def create_dashboard_manager(health_manager: InfrastructureHealthManager,
                           alerting_system: IntelligentAlertingSystem) -> DashboardManager:
    """Create dashboard manager with default configuration"""
    return DashboardManager(health_manager, alerting_system)


# Global instance
_dashboard_manager = None


def get_dashboard_manager(health_manager: InfrastructureHealthManager = None,
                         alerting_system: IntelligentAlertingSystem = None) -> DashboardManager:
    """Get global dashboard manager"""
    global _dashboard_manager
    if _dashboard_manager is None:
        if not health_manager or not alerting_system:
            from .infrastructure_health_monitor import get_infrastructure_manager
            from .intelligent_alerting_system import get_alerting_system
            
            health_manager = health_manager or get_infrastructure_manager()
            alerting_system = alerting_system or get_alerting_system()
        
        _dashboard_manager = create_dashboard_manager(health_manager, alerting_system)
    return _dashboard_manager


if __name__ == "__main__":
    print("Infrastructure Dashboards System")
    print("Available dashboards:")
    print("- Executive Dashboard: High-level KPIs and system health")
    print("- Technical Dashboard: Detailed metrics and performance data")
    print("- Troubleshooting Dashboard: Diagnostic and error analysis")
    print("- Real-time Dashboard: Live monitoring with frequent updates")
    print("\nVisualization types:")
    print("- Gauges, Line Charts, Bar Charts, Pie Charts")
    print("- Heatmaps, Tables, Metric Cards, Status Indicators")
    print("- Timelines and custom visualizations")