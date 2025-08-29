"""
DataDog Dashboard Integration and Custom Metrics Management
Provides comprehensive dashboard creation, custom metrics management, and visualization for enterprise data platform
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple

# DataDog API client
try:
    from datadog_api_client import ApiClient, Configuration
    from datadog_api_client.v1.api.dashboards_api import DashboardsApi
    from datadog_api_client.v1.api.metrics_api import MetricsApi
    from datadog_api_client.v1.model.dashboard import Dashboard
    from datadog_api_client.v1.model.dashboard_layout_type import DashboardLayoutType
    from datadog_api_client.v1.model.widget import Widget
    from datadog_api_client.v1.model.widget_definition import WidgetDefinition
    from datadog_api_client.v1.model.timeseries_widget_definition import TimeseriesWidgetDefinition
    from datadog_api_client.v1.model.query_value_widget_definition import QueryValueWidgetDefinition
    from datadog_api_client.v1.model.heatmap_widget_definition import HeatmapWidgetDefinition
    DATADOG_API_AVAILABLE = True
except ImportError:
    DATADOG_API_AVAILABLE = False

from core.config import get_settings
from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring

logger = get_logger(__name__)
settings = get_settings()


class DataDogDashboardManager:
    """
    Comprehensive DataDog dashboard and custom metrics management
    
    Features:
    - Automated dashboard creation for different components
    - Custom metrics visualization
    - Business KPI dashboards
    - Performance monitoring dashboards
    - Error tracking dashboards
    - ML pipeline monitoring dashboards
    - Real-time data quality dashboards
    - Alert management integration
    """
    
    def __init__(self, service_name: str = "dashboard-manager",
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Initialize DataDog API client if available
        self.api_client = None
        self.dashboards_api = None
        self.metrics_api = None
        
        if DATADOG_API_AVAILABLE:
            try:
                configuration = Configuration()
                configuration.api_key["apiKeyAuth"] = os.getenv("DD_API_KEY")
                configuration.api_key["appKeyAuth"] = os.getenv("DD_APP_KEY")
                configuration.server_variables["site"] = os.getenv("DD_SITE", "datadoghq.com")
                
                self.api_client = ApiClient(configuration)
                self.dashboards_api = DashboardsApi(self.api_client)
                self.metrics_api = MetricsApi(self.api_client)
                
                self.logger.info("DataDog API client initialized successfully")
                
            except Exception as e:
                self.logger.warning(f"Failed to initialize DataDog API client: {str(e)}")
                self.api_client = None
        else:
            self.logger.warning("DataDog API client not available - install datadog-api-client")
        
        # Dashboard definitions storage
        self.dashboard_definitions = {}
        self.created_dashboards = {}
        
        # Custom metrics registry
        self.custom_metrics = {}
        
        # Dashboard templates
        self.dashboard_templates = self._initialize_dashboard_templates()
    
    def _initialize_dashboard_templates(self) -> Dict[str, Dict[str, Any]]:
        """Initialize predefined dashboard templates."""
        
        return {
            "application_overview": {
                "title": "Enterprise Data Platform - Application Overview",
                "description": "High-level overview of application performance and health",
                "widgets": [
                    {
                        "type": "query_value",
                        "title": "Requests per Minute",
                        "query": "sum:fastapi.requests.total{*}.as_rate()"
                    },
                    {
                        "type": "query_value", 
                        "title": "Average Response Time",
                        "query": "avg:fastapi.response.duration{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Error Rate",
                        "query": "sum:fastapi.errors.total{*}.as_rate()"
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Volume Over Time",
                        "query": "sum:fastapi.requests.total{*}.as_rate()"
                    },
                    {
                        "type": "timeseries",
                        "title": "Response Time Distribution",
                        "query": "avg:fastapi.response.duration{*} by {endpoint}"
                    }
                ]
            },
            "business_kpis": {
                "title": "Enterprise Data Platform - Business KPIs",
                "description": "Business metrics and key performance indicators",
                "widgets": [
                    {
                        "type": "query_value",
                        "title": "Revenue Total",
                        "query": "sum:business.kpi.revenue_total{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Customer Acquisition Cost",
                        "query": "avg:business.kpi.customer_acquisition_cost{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Data Quality Score",
                        "query": "avg:business.kpi.data_quality_score{*}"
                    },
                    {
                        "type": "timeseries",
                        "title": "Revenue Trends",
                        "query": "sum:business.kpi.revenue_total{*}"
                    },
                    {
                        "type": "timeseries",
                        "title": "KPI Health Score",
                        "query": "avg:business.kpis.health_score{*}"
                    }
                ]
            },
            "data_pipelines": {
                "title": "Enterprise Data Platform - Data Pipelines",
                "description": "ETL pipeline performance and data quality monitoring",
                "widgets": [
                    {
                        "type": "query_value",
                        "title": "Pipeline Success Rate",
                        "query": "avg:etl.pipelines.success_rate{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Records Processed/Hour",
                        "query": "sum:etl.records.processed{*}.as_rate()"
                    },
                    {
                        "type": "query_value",
                        "title": "Data Quality Score",
                        "query": "avg:etl.data_quality.overall_score{*}"
                    },
                    {
                        "type": "timeseries",
                        "title": "Pipeline Execution Times",
                        "query": "avg:etl.operations.duration{*} by {layer}"
                    },
                    {
                        "type": "heatmap",
                        "title": "Pipeline Performance Heatmap",
                        "query": "avg:etl.operations.duration{*} by {pipeline_id}"
                    }
                ]
            },
            "ml_operations": {
                "title": "Enterprise Data Platform - ML Operations",
                "description": "Machine learning pipeline performance and model monitoring",
                "widgets": [
                    {
                        "type": "query_value",
                        "title": "Model Accuracy",
                        "query": "avg:ml.validation.accuracy{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Inference Latency P95",
                        "query": "percentile:ml.inference.latency{*}:95"
                    },
                    {
                        "type": "query_value",
                        "title": "Drift Detection Rate",
                        "query": "sum:ml.drift.detected{*}.as_rate()"
                    },
                    {
                        "type": "timeseries",
                        "title": "Model Performance Trends",
                        "query": "avg:ml.validation.accuracy{*} by {model}"
                    },
                    {
                        "type": "timeseries",
                        "title": "Inference Volume",
                        "query": "sum:ml.inference.requests{*}.as_rate()"
                    }
                ]
            },
            "streaming_data": {
                "title": "Enterprise Data Platform - Streaming Data",
                "description": "Real-time data processing and streaming pipeline monitoring",
                "widgets": [
                    {
                        "type": "query_value",
                        "title": "Messages/Second",
                        "query": "sum:streaming.messages.processed{*}.as_rate()"
                    },
                    {
                        "type": "query_value",
                        "title": "Consumer Lag",
                        "query": "max:streaming.consumer.lag{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Processing Latency P95",
                        "query": "percentile:streaming.processing.latency{*}:95"
                    },
                    {
                        "type": "timeseries",
                        "title": "Message Throughput",
                        "query": "sum:streaming.messages.processed{*}.as_rate() by {topic}"
                    },
                    {
                        "type": "timeseries",
                        "title": "Consumer Lag Trends",
                        "query": "avg:streaming.consumer.lag{*} by {consumer_group}"
                    }
                ]
            },
            "error_tracking": {
                "title": "Enterprise Data Platform - Error Tracking",
                "description": "Error monitoring and exception analysis",
                "widgets": [
                    {
                        "type": "query_value",
                        "title": "Error Rate",
                        "query": "sum:errors.occurrences.total{*}.as_rate()"
                    },
                    {
                        "type": "query_value",
                        "title": "Critical Errors",
                        "query": "sum:errors.critical.total{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Unique Error Types",
                        "query": "count:errors.groups.total{*}"
                    },
                    {
                        "type": "timeseries",
                        "title": "Error Trends by Severity",
                        "query": "sum:errors.occurrences.total{*}.as_rate() by {severity}"
                    },
                    {
                        "type": "timeseries",
                        "title": "Error Distribution by Service",
                        "query": "sum:errors.occurrences.total{*}.as_rate() by {service}"
                    }
                ]
            },
            "system_performance": {
                "title": "Enterprise Data Platform - System Performance",
                "description": "System resource utilization and performance metrics",
                "widgets": [
                    {
                        "type": "query_value",
                        "title": "CPU Usage",
                        "query": "avg:profiling.system.cpu_percent{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Memory Usage",
                        "query": "avg:profiling.system.memory_percent{*}"
                    },
                    {
                        "type": "query_value",
                        "title": "Active Threads",
                        "query": "avg:profiling.system.threads{*}"
                    },
                    {
                        "type": "timeseries",
                        "title": "Resource Usage Trends",
                        "query": "avg:profiling.system.cpu_percent{*}, avg:profiling.system.memory_percent{*}"
                    },
                    {
                        "type": "heatmap",
                        "title": "Operation Duration Heatmap",
                        "query": "avg:profiling.operation.duration{*} by {operation}"
                    }
                ]
            }
        }
    
    async def create_dashboard(self, dashboard_type: str, 
                             custom_title: Optional[str] = None,
                             custom_description: Optional[str] = None,
                             custom_widgets: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
        """Create a DataDog dashboard from template."""
        
        if not self.api_client:
            self.logger.error("DataDog API client not available")
            return None
        
        if dashboard_type not in self.dashboard_templates:
            self.logger.error(f"Unknown dashboard type: {dashboard_type}")
            return None
        
        template = self.dashboard_templates[dashboard_type]
        
        try:
            # Prepare dashboard definition
            dashboard_title = custom_title or template["title"]
            dashboard_description = custom_description or template["description"]
            widgets_config = custom_widgets or template["widgets"]
            
            # Convert widget configurations to DataDog widget objects
            widgets = []
            for i, widget_config in enumerate(widgets_config):
                widget = self._create_widget(widget_config, i)
                if widget:
                    widgets.append(widget)
            
            # Create dashboard object
            dashboard = Dashboard(
                title=dashboard_title,
                description=dashboard_description,
                layout_type=DashboardLayoutType.ORDERED,
                widgets=widgets,
                template_variables=[],
                is_read_only=False
            )
            
            # Create dashboard via API
            response = self.dashboards_api.create_dashboard(body=dashboard)
            dashboard_id = response.id
            
            # Store dashboard information
            self.created_dashboards[dashboard_type] = {
                "id": dashboard_id,
                "title": dashboard_title,
                "url": f"https://app.datadoghq.com/dashboard/{dashboard_id}",
                "created_at": datetime.utcnow().isoformat(),
                "type": dashboard_type
            }
            
            self.logger.info(f"Dashboard created successfully: {dashboard_title} (ID: {dashboard_id})")
            
            # Send success metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "dashboard.creation.success",
                    tags=[f"dashboard_type:{dashboard_type}", f"service:{self.service_name}"]
                )
            
            return dashboard_id
            
        except Exception as e:
            self.logger.error(f"Failed to create dashboard: {str(e)}")
            
            # Send failure metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "dashboard.creation.failure",
                    tags=[
                        f"dashboard_type:{dashboard_type}",
                        f"service:{self.service_name}",
                        f"error_type:{type(e).__name__}"
                    ]
                )
            
            return None
    
    def _create_widget(self, widget_config: Dict[str, Any], position: int) -> Optional[Widget]:
        """Create a DataDog widget from configuration."""
        
        try:
            widget_type = widget_config.get("type")
            title = widget_config.get("title", f"Widget {position}")
            query = widget_config.get("query", "")
            
            if widget_type == "query_value":
                widget_definition = QueryValueWidgetDefinition(
                    requests=[
                        {
                            "q": query,
                            "aggregator": "avg"
                        }
                    ],
                    title=title,
                    title_size="16",
                    title_align="left",
                    precision=2
                )
            elif widget_type == "timeseries":
                widget_definition = TimeseriesWidgetDefinition(
                    requests=[
                        {
                            "q": query,
                            "display_type": "line"
                        }
                    ],
                    title=title,
                    title_size="16",
                    title_align="left",
                    show_legend=True
                )
            elif widget_type == "heatmap":
                widget_definition = HeatmapWidgetDefinition(
                    requests=[
                        {
                            "q": query
                        }
                    ],
                    title=title,
                    title_size="16",
                    title_align="left"
                )
            else:
                self.logger.warning(f"Unknown widget type: {widget_type}")
                return None
            
            return Widget(
                definition=widget_definition,
                layout={
                    "x": 0,
                    "y": position * 4,
                    "width": 4,
                    "height": 3
                }
            )
            
        except Exception as e:
            self.logger.error(f"Failed to create widget: {str(e)}")
            return None
    
    async def create_all_dashboards(self) -> Dict[str, Optional[str]]:
        """Create all predefined dashboards."""
        
        results = {}
        
        for dashboard_type in self.dashboard_templates.keys():
            try:
                dashboard_id = await self.create_dashboard(dashboard_type)
                results[dashboard_type] = dashboard_id
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Failed to create dashboard {dashboard_type}: {str(e)}")
                results[dashboard_type] = None
        
        success_count = sum(1 for result in results.values() if result is not None)
        total_count = len(results)
        
        self.logger.info(f"Dashboard creation completed: {success_count}/{total_count} successful")
        
        return results
    
    async def update_dashboard(self, dashboard_id: str, 
                             new_title: Optional[str] = None,
                             new_description: Optional[str] = None,
                             additional_widgets: Optional[List[Dict[str, Any]]] = None) -> bool:
        """Update an existing dashboard."""
        
        if not self.api_client:
            self.logger.error("DataDog API client not available")
            return False
        
        try:
            # Get existing dashboard
            existing_dashboard = self.dashboards_api.get_dashboard(dashboard_id)
            
            # Update fields if provided
            if new_title:
                existing_dashboard.title = new_title
            if new_description:
                existing_dashboard.description = new_description
            
            # Add additional widgets if provided
            if additional_widgets:
                new_widgets = []
                for i, widget_config in enumerate(additional_widgets):
                    widget = self._create_widget(widget_config, len(existing_dashboard.widgets) + i)
                    if widget:
                        new_widgets.append(widget)
                
                existing_dashboard.widgets.extend(new_widgets)
            
            # Update dashboard via API
            self.dashboards_api.update_dashboard(dashboard_id, body=existing_dashboard)
            
            self.logger.info(f"Dashboard updated successfully: {dashboard_id}")
            
            # Send success metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "dashboard.update.success",
                    tags=[f"dashboard_id:{dashboard_id}", f"service:{self.service_name}"]
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update dashboard {dashboard_id}: {str(e)}")
            
            # Send failure metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "dashboard.update.failure",
                    tags=[
                        f"dashboard_id:{dashboard_id}",
                        f"service:{self.service_name}",
                        f"error_type:{type(e).__name__}"
                    ]
                )
            
            return False
    
    async def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a dashboard."""
        
        if not self.api_client:
            self.logger.error("DataDog API client not available")
            return False
        
        try:
            self.dashboards_api.delete_dashboard(dashboard_id)
            
            # Remove from created dashboards tracking
            for dashboard_type, info in list(self.created_dashboards.items()):
                if info["id"] == dashboard_id:
                    del self.created_dashboards[dashboard_type]
                    break
            
            self.logger.info(f"Dashboard deleted successfully: {dashboard_id}")
            
            # Send success metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "dashboard.deletion.success",
                    tags=[f"dashboard_id:{dashboard_id}", f"service:{self.service_name}"]
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete dashboard {dashboard_id}: {str(e)}")
            
            # Send failure metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "dashboard.deletion.failure",
                    tags=[
                        f"dashboard_id:{dashboard_id}",
                        f"service:{self.service_name}",
                        f"error_type:{type(e).__name__}"
                    ]
                )
            
            return False
    
    # Custom Metrics Management
    
    def register_custom_metric(self, metric_name: str, metric_type: str, 
                             description: str, unit: str,
                             tags: Optional[List[str]] = None) -> bool:
        """Register a custom metric definition."""
        
        try:
            metric_definition = {
                "name": metric_name,
                "type": metric_type,  # gauge, counter, histogram, distribution
                "description": description,
                "unit": unit,
                "tags": tags or [],
                "registered_at": datetime.utcnow().isoformat()
            }
            
            self.custom_metrics[metric_name] = metric_definition
            
            self.logger.info(f"Custom metric registered: {metric_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register custom metric {metric_name}: {str(e)}")
            return False
    
    async def submit_custom_metric(self, metric_name: str, value: Union[int, float],
                                 timestamp: Optional[datetime] = None,
                                 tags: Optional[Dict[str, str]] = None) -> bool:
        """Submit a custom metric value."""
        
        if not self.datadog_monitoring:
            self.logger.error("DataDog monitoring client not available")
            return False
        
        try:
            if metric_name not in self.custom_metrics:
                self.logger.warning(f"Custom metric not registered: {metric_name}")
                return False
            
            metric_def = self.custom_metrics[metric_name]
            
            # Convert tags to list format
            tag_list = []
            if tags:
                tag_list = [f"{k}:{v}" for k, v in tags.items()]
            
            # Submit metric based on type
            if metric_def["type"] == "gauge":
                await self.datadog_monitoring.gauge(metric_name, value, tags=tag_list)
            elif metric_def["type"] == "counter":
                await self.datadog_monitoring.counter(metric_name, value, tags=tag_list)
            elif metric_def["type"] == "histogram":
                await self.datadog_monitoring.histogram(metric_name, value, tags=tag_list)
            else:
                self.logger.error(f"Unsupported metric type: {metric_def['type']}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to submit custom metric {metric_name}: {str(e)}")
            return False
    
    # Dashboard Management
    
    def get_dashboard_status(self) -> Dict[str, Any]:
        """Get status of created dashboards."""
        
        status = {
            "service_name": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "api_client_available": self.api_client is not None,
            "total_dashboards_created": len(self.created_dashboards),
            "dashboard_types_available": list(self.dashboard_templates.keys()),
            "custom_metrics_registered": len(self.custom_metrics),
            "dashboards": self.created_dashboards.copy(),
            "custom_metrics": self.custom_metrics.copy()
        }
        
        return status
    
    async def validate_dashboards(self) -> Dict[str, Any]:
        """Validate that created dashboards are accessible."""
        
        validation_results = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_dashboards": len(self.created_dashboards),
            "validation_results": {},
            "summary": {
                "accessible": 0,
                "inaccessible": 0,
                "errors": []
            }
        }
        
        if not self.api_client:
            validation_results["summary"]["errors"].append("DataDog API client not available")
            return validation_results
        
        for dashboard_type, dashboard_info in self.created_dashboards.items():
            dashboard_id = dashboard_info["id"]
            
            try:
                # Try to access the dashboard
                dashboard = self.dashboards_api.get_dashboard(dashboard_id)
                
                validation_results["validation_results"][dashboard_type] = {
                    "accessible": True,
                    "title": dashboard.title,
                    "widget_count": len(dashboard.widgets) if dashboard.widgets else 0,
                    "last_modified": getattr(dashboard, 'modified_at', None)
                }
                
                validation_results["summary"]["accessible"] += 1
                
            except Exception as e:
                validation_results["validation_results"][dashboard_type] = {
                    "accessible": False,
                    "error": str(e)
                }
                
                validation_results["summary"]["inaccessible"] += 1
                validation_results["summary"]["errors"].append(f"{dashboard_type}: {str(e)}")
        
        return validation_results
    
    def export_dashboard_definitions(self) -> Dict[str, Any]:
        """Export all dashboard definitions for backup or sharing."""
        
        return {
            "export_timestamp": datetime.utcnow().isoformat(),
            "service_name": self.service_name,
            "dashboard_templates": self.dashboard_templates,
            "created_dashboards": self.created_dashboards,
            "custom_metrics": self.custom_metrics
        }
    
    async def import_dashboard_definitions(self, definitions: Dict[str, Any]) -> bool:
        """Import dashboard definitions from backup."""
        
        try:
            if "dashboard_templates" in definitions:
                self.dashboard_templates.update(definitions["dashboard_templates"])
            
            if "custom_metrics" in definitions:
                self.custom_metrics.update(definitions["custom_metrics"])
            
            self.logger.info("Dashboard definitions imported successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to import dashboard definitions: {str(e)}")
            return False


# Global dashboard manager instance
_dashboard_manager: Optional[DataDogDashboardManager] = None


def get_dashboard_manager(service_name: str = "dashboard-manager",
                         datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogDashboardManager:
    """Get or create dashboard manager."""
    global _dashboard_manager
    
    if _dashboard_manager is None:
        _dashboard_manager = DataDogDashboardManager(service_name, datadog_monitoring)
    
    return _dashboard_manager


# Convenience functions

async def create_enterprise_dashboards() -> Dict[str, Optional[str]]:
    """Create all enterprise data platform dashboards."""
    manager = get_dashboard_manager()
    return await manager.create_all_dashboards()


async def setup_custom_metrics() -> bool:
    """Setup standard custom metrics for the enterprise platform."""
    manager = get_dashboard_manager()
    
    # Business metrics
    manager.register_custom_metric(
        "enterprise.business.revenue.daily",
        "gauge",
        "Daily revenue in USD",
        "dollars",
        ["currency", "region"]
    )
    
    manager.register_custom_metric(
        "enterprise.data.quality.score",
        "gauge", 
        "Overall data quality score (0-100)",
        "score",
        ["dataset", "layer"]
    )
    
    # Performance metrics
    manager.register_custom_metric(
        "enterprise.pipeline.execution.time",
        "histogram",
        "Pipeline execution time in seconds",
        "seconds",
        ["pipeline", "layer", "environment"]
    )
    
    manager.register_custom_metric(
        "enterprise.ml.model.accuracy",
        "gauge",
        "ML model accuracy percentage",
        "percent",
        ["model", "version", "environment"]
    )
    
    # Infrastructure metrics
    manager.register_custom_metric(
        "enterprise.system.resource.utilization",
        "gauge",
        "System resource utilization percentage", 
        "percent",
        ["resource_type", "service", "environment"]
    )
    
    return True


def get_dashboard_urls() -> Dict[str, str]:
    """Get URLs for all created dashboards."""
    manager = get_dashboard_manager()
    
    urls = {}
    for dashboard_type, info in manager.created_dashboards.items():
        urls[dashboard_type] = info["url"]
    
    return urls


async def validate_dashboard_health() -> Dict[str, Any]:
    """Validate health of all created dashboards."""
    manager = get_dashboard_manager()
    return await manager.validate_dashboards()