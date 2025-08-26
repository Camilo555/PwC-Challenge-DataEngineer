"""
Grafana Dashboard Configuration Generator
Creates comprehensive Grafana dashboards for enterprise monitoring.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from core.logging import get_logger

logger = get_logger(__name__)


class DashboardCategory(Enum):
    """Dashboard categories for organization."""
    EXECUTIVE = "executive"
    OPERATIONS = "operations"
    INFRASTRUCTURE = "infrastructure"
    BUSINESS = "business"
    SECURITY = "security"
    DEVELOPMENT = "development"


class PanelType(Enum):
    """Grafana panel types."""
    GRAPH = "graph"
    STAT = "stat"
    GAUGE = "gauge"
    BAR_GAUGE = "bargauge"
    TABLE = "table"
    HEATMAP = "heatmap"
    ALERT_LIST = "alertlist"
    TEXT = "text"
    LOGS = "logs"
    NODE_GRAPH = "nodeGraph"


class VisualizationType(Enum):
    """Visualization types for panels."""
    TIME_SERIES = "timeseries"
    STATE_TIMELINE = "state-timeline"
    STATUS_HISTORY = "status-history"
    BAR_CHART = "barchart"
    PIE_CHART = "piechart"
    SCATTER = "scattergraph"


@dataclass
class Target:
    """Prometheus query target for panels."""
    expr: str
    interval: str = ""
    legendFormat: str = ""
    refId: str = "A"
    datasource: str = "Prometheus"
    
    
@dataclass
class Panel:
    """Grafana panel configuration."""
    id: int
    title: str
    type: str
    gridPos: Dict[str, int]
    targets: List[Target]
    description: str = ""
    unit: str = ""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    thresholds: List[Dict[str, Any]] = None
    fieldConfig: Dict[str, Any] = None
    options: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.thresholds is None:
            self.thresholds = []
        if self.fieldConfig is None:
            self.fieldConfig = {"defaults": {"unit": self.unit}}
        if self.options is None:
            self.options = {}


@dataclass
class Dashboard:
    """Grafana dashboard configuration."""
    id: Optional[int]
    uid: str
    title: str
    description: str
    category: DashboardCategory
    panels: List[Panel]
    time_from: str = "now-1h"
    time_to: str = "now"
    refresh: str = "30s"
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = [self.category.value]


class GrafanaDashboardGenerator:
    """Generator for comprehensive Grafana dashboards."""
    
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        
    def create_executive_dashboard(self) -> Dashboard:
        """Create executive-level dashboard with high-level KPIs."""
        panels = [
            Panel(
                id=1,
                title="Revenue Today",
                type=PanelType.STAT.value,
                gridPos={"h": 8, "w": 6, "x": 0, "y": 0},
                targets=[
                    Target(
                        expr='sum(increase(business_sales_revenue_total[24h]))',
                        legendFormat="Revenue"
                    )
                ],
                description="Total revenue for the current day",
                unit="currencyUSD",
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": None},
                                {"color": "yellow", "value": 50000},
                                {"color": "green", "value": 100000}
                            ]
                        }
                    }
                }
            ),
            Panel(
                id=2,
                title="Active Customers",
                type=PanelType.STAT.value,
                gridPos={"h": 8, "w": 6, "x": 6, "y": 0},
                targets=[
                    Target(
                        expr='sum(business_active_customers)',
                        legendFormat="Active Customers"
                    )
                ],
                description="Current number of active customers",
                unit="short"
            ),
            Panel(
                id=3,
                title="System Health",
                type=PanelType.GAUGE.value,
                gridPos={"h": 8, "w": 6, "x": 12, "y": 0},
                targets=[
                    Target(
                        expr='(sum(rate(app_requests_total{status!~"5.."}[5m])) / sum(rate(app_requests_total[5m]))) * 100',
                        legendFormat="Success Rate"
                    )
                ],
                description="Overall system health percentage",
                unit="percent",
                min_value=0,
                max_value=100,
                thresholds=[
                    {"color": "red", "value": 0},
                    {"color": "yellow", "value": 95},
                    {"color": "green", "value": 99}
                ]
            ),
            Panel(
                id=4,
                title="Error Rate",
                type=PanelType.GAUGE.value,
                gridPos={"h": 8, "w": 6, "x": 18, "y": 0},
                targets=[
                    Target(
                        expr='sum(rate(app_errors_total[5m])) * 100',
                        legendFormat="Error Rate"
                    )
                ],
                description="Application error rate percentage",
                unit="percent",
                max_value=10,
                thresholds=[
                    {"color": "green", "value": 0},
                    {"color": "yellow", "value": 1},
                    {"color": "red", "value": 5}
                ]
            ),
            Panel(
                id=5,
                title="Revenue Trend",
                type=PanelType.GRAPH.value,
                gridPos={"h": 9, "w": 12, "x": 0, "y": 8},
                targets=[
                    Target(
                        expr='sum(rate(business_sales_revenue_total[1h]))',
                        legendFormat="Hourly Revenue",
                        interval="1h"
                    )
                ],
                description="Revenue trend over time",
                unit="currencyUSD"
            ),
            Panel(
                id=6,
                title="Top Products by Revenue",
                type=PanelType.BAR_GAUGE.value,
                gridPos={"h": 9, "w": 12, "x": 12, "y": 8},
                targets=[
                    Target(
                        expr='topk(10, sum by (product_category) (increase(business_sales_revenue_total[24h])))',
                        legendFormat="{{product_category}}"
                    )
                ],
                description="Top 10 products by revenue today",
                unit="currencyUSD"
            )
        ]
        
        return Dashboard(
            id=1,
            uid="executive-dashboard",
            title="Executive Dashboard",
            description="High-level business metrics and KPIs for executives",
            category=DashboardCategory.EXECUTIVE,
            panels=panels,
            time_from="now-24h",
            refresh="5m",
            tags=["executive", "kpi", "business"]
        )
        
    def create_operations_dashboard(self) -> Dashboard:
        """Create operations dashboard for system monitoring."""
        panels = [
            Panel(
                id=1,
                title="Request Rate",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 0, "y": 0},
                targets=[
                    Target(
                        expr='sum(rate(app_requests_total[5m])) by (service)',
                        legendFormat="{{service}}"
                    )
                ],
                description="Request rate per service",
                unit="reqps"
            ),
            Panel(
                id=2,
                title="Response Time (95th percentile)",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 12, "y": 0},
                targets=[
                    Target(
                        expr='histogram_quantile(0.95, sum(rate(app_request_duration_seconds_bucket[5m])) by (service, le))',
                        legendFormat="{{service}}"
                    )
                ],
                description="95th percentile response time",
                unit="s"
            ),
            Panel(
                id=3,
                title="Error Rate by Service",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 0, "y": 8},
                targets=[
                    Target(
                        expr='sum(rate(app_errors_total[5m])) by (service)',
                        legendFormat="{{service}}"
                    )
                ],
                description="Error rate breakdown by service",
                unit="eps"
            ),
            Panel(
                id=4,
                title="Active Connections",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 12, "y": 8},
                targets=[
                    Target(
                        expr='sum(app_active_connections) by (service)',
                        legendFormat="{{service}}"
                    )
                ],
                description="Number of active connections per service",
                unit="short"
            ),
            Panel(
                id=5,
                title="Top Endpoints by Traffic",
                type=PanelType.TABLE.value,
                gridPos={"h": 8, "w": 24, "x": 0, "y": 16},
                targets=[
                    Target(
                        expr='topk(20, sum(rate(app_requests_total[1h])) by (endpoint, method))',
                        legendFormat="{{method}} {{endpoint}}"
                    )
                ],
                description="Top 20 endpoints by request volume"
            )
        ]
        
        return Dashboard(
            id=2,
            uid="operations-dashboard",
            title="Operations Dashboard",
            description="Operational metrics for system monitoring and troubleshooting",
            category=DashboardCategory.OPERATIONS,
            panels=panels,
            refresh="10s",
            tags=["operations", "monitoring", "performance"]
        )
        
    def create_infrastructure_dashboard(self) -> Dashboard:
        """Create infrastructure monitoring dashboard."""
        panels = [
            Panel(
                id=1,
                title="CPU Usage",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 8, "x": 0, "y": 0},
                targets=[
                    Target(
                        expr='avg(system_cpu_usage_percent) by (host)',
                        legendFormat="{{host}}"
                    )
                ],
                description="CPU usage per host",
                unit="percent",
                max_value=100
            ),
            Panel(
                id=2,
                title="Memory Usage",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 8, "x": 8, "y": 0},
                targets=[
                    Target(
                        expr='(system_memory_usage_bytes{memory_type="used"} / system_memory_usage_bytes{memory_type="total"}) * 100',
                        legendFormat="{{host}}"
                    )
                ],
                description="Memory usage percentage per host",
                unit="percent",
                max_value=100
            ),
            Panel(
                id=3,
                title="Database Performance",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 8, "x": 16, "y": 0},
                targets=[
                    Target(
                        expr='histogram_quantile(0.95, sum(rate(database_query_duration_seconds_bucket[5m])) by (database, le))',
                        legendFormat="{{database}}"
                    )
                ],
                description="Database query performance (95th percentile)",
                unit="s"
            ),
            Panel(
                id=4,
                title="ETL Pipeline Status",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 0, "y": 8},
                targets=[
                    Target(
                        expr='sum(rate(etl_records_processed_total[10m])) by (stage, status)',
                        legendFormat="{{stage}} - {{status}}"
                    )
                ],
                description="ETL pipeline processing rate by stage",
                unit="rps"
            ),
            Panel(
                id=5,
                title="Cache Hit Rate",
                type=PanelType.GAUGE.value,
                gridPos={"h": 8, "w": 12, "x": 12, "y": 8},
                targets=[
                    Target(
                        expr='avg(cache_hit_ratio)',
                        legendFormat="Hit Rate"
                    )
                ],
                description="Overall cache hit rate",
                unit="percentunit",
                min_value=0,
                max_value=1
            )
        ]
        
        return Dashboard(
            id=3,
            uid="infrastructure-dashboard",
            title="Infrastructure Dashboard",
            description="Infrastructure and system resource monitoring",
            category=DashboardCategory.INFRASTRUCTURE,
            panels=panels,
            tags=["infrastructure", "resources", "performance"]
        )
        
    def create_business_dashboard(self) -> Dashboard:
        """Create business metrics dashboard."""
        panels = [
            Panel(
                id=1,
                title="Sales by Region",
                type=PanelType.BAR_GAUGE.value,
                gridPos={"h": 8, "w": 12, "x": 0, "y": 0},
                targets=[
                    Target(
                        expr='sum(rate(business_sales_revenue_total[1h])) by (region)',
                        legendFormat="{{region}}"
                    )
                ],
                description="Sales performance by geographic region",
                unit="currencyUSD"
            ),
            Panel(
                id=2,
                title="Customer Segments",
                type=PanelType.BAR_GAUGE.value,
                gridPos={"h": 8, "w": 12, "x": 12, "y": 0},
                targets=[
                    Target(
                        expr='sum(business_active_customers) by (segment)',
                        legendFormat="{{segment}}"
                    )
                ],
                description="Active customers by segment",
                unit="short"
            ),
            Panel(
                id=3,
                title="Order Value Distribution",
                type=PanelType.HEATMAP.value,
                gridPos={"h": 8, "w": 24, "x": 0, "y": 8},
                targets=[
                    Target(
                        expr='sum(rate(business_order_value_bucket[1h])) by (le)',
                        legendFormat="{{le}}"
                    )
                ],
                description="Distribution of order values over time",
                unit="currencyUSD"
            ),
            Panel(
                id=4,
                title="Conversion Rate by Channel",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 0, "y": 16},
                targets=[
                    Target(
                        expr='avg(business_conversion_rate) by (channel)',
                        legendFormat="{{channel}}"
                    )
                ],
                description="Conversion rates across different channels",
                unit="percent"
            ),
            Panel(
                id=5,
                title="Inventory Levels",
                type=PanelType.TABLE.value,
                gridPos={"h": 8, "w": 12, "x": 12, "y": 16},
                targets=[
                    Target(
                        expr='sum(business_inventory_levels) by (category, warehouse)',
                        legendFormat="{{category}} - {{warehouse}}"
                    )
                ],
                description="Current inventory levels by category and warehouse",
                unit="short"
            )
        ]
        
        return Dashboard(
            id=4,
            uid="business-dashboard",
            title="Business Metrics Dashboard",
            description="Business KPIs and performance metrics",
            category=DashboardCategory.BUSINESS,
            panels=panels,
            time_from="now-7d",
            refresh="5m",
            tags=["business", "kpi", "sales", "customers"]
        )
        
    def create_security_dashboard(self) -> Dashboard:
        """Create security monitoring dashboard."""
        panels = [
            Panel(
                id=1,
                title="Authentication Failures",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 0, "y": 0},
                targets=[
                    Target(
                        expr='sum(rate(app_errors_total{error_type="authentication_error"}[5m]))',
                        legendFormat="Auth Failures"
                    )
                ],
                description="Rate of authentication failures",
                unit="fps"
            ),
            Panel(
                id=2,
                title="Security Events by Type",
                type=PanelType.BAR_GAUGE.value,
                gridPos={"h": 8, "w": 12, "x": 12, "y": 0},
                targets=[
                    Target(
                        expr='sum(rate(app_errors_total{severity="security"}[1h])) by (error_type)',
                        legendFormat="{{error_type}}"
                    )
                ],
                description="Security events breakdown by type",
                unit="eps"
            ),
            Panel(
                id=3,
                title="Failed Login Attempts",
                type=PanelType.STAT.value,
                gridPos={"h": 4, "w": 6, "x": 0, "y": 8},
                targets=[
                    Target(
                        expr='sum(increase(app_errors_total{error_type="login_failure"}[1h]))',
                        legendFormat="Failed Logins"
                    )
                ],
                description="Failed login attempts in the last hour",
                unit="short"
            ),
            Panel(
                id=4,
                title="Suspicious Activity Alert",
                type=PanelType.ALERT_LIST.value,
                gridPos={"h": 8, "w": 18, "x": 6, "y": 8},
                targets=[],
                description="Active security alerts and suspicious activities"
            )
        ]
        
        return Dashboard(
            id=5,
            uid="security-dashboard",
            title="Security Monitoring Dashboard",
            description="Security events and threat monitoring",
            category=DashboardCategory.SECURITY,
            panels=panels,
            refresh="30s",
            tags=["security", "alerts", "threats"]
        )
        
    def create_development_dashboard(self) -> Dashboard:
        """Create development and deployment dashboard."""
        panels = [
            Panel(
                id=1,
                title="Deployment Frequency",
                type=PanelType.GRAPH.value,
                gridPos={"h": 8, "w": 12, "x": 0, "y": 0},
                targets=[
                    Target(
                        expr='sum(rate(app_deployments_total[24h]))',
                        legendFormat="Deployments"
                    )
                ],
                description="Deployment frequency over time",
                unit="short"
            ),
            Panel(
                id=2,
                title="Build Success Rate",
                type=PanelType.GAUGE.value,
                gridPos={"h": 8, "w": 12, "x": 12, "y": 0},
                targets=[
                    Target(
                        expr='(sum(rate(app_builds_total{status="success"}[24h])) / sum(rate(app_builds_total[24h]))) * 100',
                        legendFormat="Success Rate"
                    )
                ],
                description="Build success rate percentage",
                unit="percent",
                max_value=100
            ),
            Panel(
                id=3,
                title="Test Coverage",
                type=PanelType.STAT.value,
                gridPos={"h": 4, "w": 6, "x": 0, "y": 8},
                targets=[
                    Target(
                        expr='avg(test_coverage_percentage)',
                        legendFormat="Coverage"
                    )
                ],
                description="Current test coverage percentage",
                unit="percent"
            ),
            Panel(
                id=4,
                title="Code Quality Metrics",
                type=PanelType.TABLE.value,
                gridPos={"h": 8, "w": 18, "x": 6, "y": 8},
                targets=[
                    Target(
                        expr='avg_over_time(code_quality_score[24h])',
                        legendFormat="Quality Score"
                    )
                ],
                description="Code quality metrics and scores"
            )
        ]
        
        return Dashboard(
            id=6,
            uid="development-dashboard",
            title="Development Dashboard",
            description="Development metrics, builds, and deployments",
            category=DashboardCategory.DEVELOPMENT,
            panels=panels,
            refresh="1m",
            tags=["development", "builds", "deployments", "quality"]
        )
        
    def generate_all_dashboards(self) -> List[Dashboard]:
        """Generate all predefined dashboards."""
        dashboards = [
            self.create_executive_dashboard(),
            self.create_operations_dashboard(),
            self.create_infrastructure_dashboard(),
            self.create_business_dashboard(),
            self.create_security_dashboard(),
            self.create_development_dashboard()
        ]
        
        self.logger.info(f"Generated {len(dashboards)} Grafana dashboards")
        return dashboards
        
    def export_dashboard_json(self, dashboard: Dashboard) -> str:
        """Export dashboard as Grafana JSON format."""
        try:
            # Convert to Grafana format
            grafana_dashboard = {
                "dashboard": {
                    "id": dashboard.id,
                    "uid": dashboard.uid,
                    "title": dashboard.title,
                    "description": dashboard.description,
                    "tags": dashboard.tags,
                    "timezone": "browser",
                    "panels": [
                        {
                            "id": panel.id,
                            "title": panel.title,
                            "type": panel.type,
                            "gridPos": panel.gridPos,
                            "targets": [asdict(target) for target in panel.targets],
                            "description": panel.description,
                            "fieldConfig": panel.fieldConfig,
                            "options": panel.options,
                            "thresholds": panel.thresholds
                        }
                        for panel in dashboard.panels
                    ],
                    "time": {
                        "from": dashboard.time_from,
                        "to": dashboard.time_to
                    },
                    "refresh": dashboard.refresh,
                    "schemaVersion": 30,
                    "version": 1,
                    "editable": True
                },
                "folderId": None,
                "overwrite": True
            }
            
            return json.dumps(grafana_dashboard, indent=2)
            
        except Exception as e:
            self.logger.error(f"Failed to export dashboard {dashboard.title}: {e}")
            return "{}"
            
    def save_dashboards_to_files(self, output_dir: str = "dashboards"):
        """Save all dashboards to JSON files."""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        dashboards = self.generate_all_dashboards()
        
        for dashboard in dashboards:
            filename = f"{dashboard.uid}.json"
            filepath = os.path.join(output_dir, filename)
            
            try:
                dashboard_json = self.export_dashboard_json(dashboard)
                with open(filepath, 'w') as f:
                    f.write(dashboard_json)
                    
                self.logger.info(f"Saved dashboard: {filepath}")
                
            except Exception as e:
                self.logger.error(f"Failed to save dashboard {dashboard.title}: {e}")


# Global generator instance
_dashboard_generator: Optional[GrafanaDashboardGenerator] = None


def get_dashboard_generator() -> GrafanaDashboardGenerator:
    """Get global Grafana dashboard generator instance."""
    global _dashboard_generator
    if _dashboard_generator is None:
        _dashboard_generator = GrafanaDashboardGenerator()
    return _dashboard_generator


def generate_enterprise_dashboards() -> List[Dashboard]:
    """Generate all enterprise dashboards."""
    generator = get_dashboard_generator()
    return generator.generate_all_dashboards()


def export_dashboards_to_json(output_dir: str = "dashboards"):
    """Export all dashboards to JSON files."""
    generator = get_dashboard_generator()
    generator.save_dashboards_to_files(output_dir)