"""
Comprehensive Grafana Dashboard Configuration
Provides production-ready Grafana dashboard definitions for complete
observability of ETL pipelines, API services, and data quality monitoring.
"""

import json
from dataclasses import dataclass, field
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class GrafanaPanel:
    """Grafana panel configuration."""

    title: str
    type: str
    targets: list[dict[str, Any]]
    gridPos: dict[str, int]
    options: dict[str, Any] = field(default_factory=dict)
    fieldConfig: dict[str, Any] = field(default_factory=dict)
    transformations: list[dict[str, Any]] = field(default_factory=list)
    alert: dict[str, Any] | None = None


@dataclass
class GrafanaDashboard:
    """Grafana dashboard configuration."""

    title: str
    description: str
    tags: list[str]
    panels: list[GrafanaPanel]
    variables: list[dict[str, Any]] = field(default_factory=list)
    time_range: dict[str, str] = field(default_factory=lambda: {"from": "now-6h", "to": "now"})
    refresh: str = "30s"
    uid: str | None = None


class GrafanaDashboardGenerator:
    """Generates comprehensive Grafana dashboards for monitoring."""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.datasource = "DataDog"  # Default datasource

    def generate_etl_pipeline_dashboard(self) -> dict[str, Any]:
        """Generate ETL Pipeline monitoring dashboard."""

        # Dashboard variables
        variables = [
            {
                "name": "pipeline",
                "type": "query",
                "query": "datadog.custom.etl_pipeline_duration_ms{*} by {pipeline}",
                "label": "Pipeline",
                "multi": True,
                "includeAll": True,
            },
            {
                "name": "environment",
                "type": "query",
                "query": "datadog.custom.etl_pipeline_duration_ms{*} by {environment}",
                "label": "Environment",
                "multi": False,
                "includeAll": False,
            },
        ]

        panels = [
            # Row 1: Pipeline Overview
            GrafanaPanel(
                title="Pipeline Execution Status",
                type="stat",
                gridPos={"h": 8, "w": 6, "x": 0, "y": 0},
                targets=[
                    {
                        "expr": 'sum(rate(datadog.custom.etl_pipeline_duration_ms{pipeline=~"$pipeline",environment="$environment"}[5m]))',
                        "legendFormat": "Executions/min",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "textMode": "auto",
                    "colorMode": "value",
                    "graphMode": "area",
                    "justifyMode": "auto",
                },
                fieldConfig={
                    "defaults": {"color": {"mode": "palette-classic"}, "unit": "reqps", "min": 0}
                },
            ),
            GrafanaPanel(
                title="Pipeline Success Rate",
                type="stat",
                gridPos={"h": 8, "w": 6, "x": 6, "y": 0},
                targets=[
                    {
                        "expr": '(sum(rate(datadog.custom.etl_records_processed{pipeline=~"$pipeline",environment="$environment"}[5m])) / (sum(rate(datadog.custom.etl_records_processed{pipeline=~"$pipeline",environment="$environment"}[5m])) + sum(rate(datadog.custom.etl_records_failed{pipeline=~"$pipeline",environment="$environment"}[5m])))) * 100',
                        "legendFormat": "Success Rate %",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "textMode": "auto",
                    "colorMode": "value",
                    "graphMode": "area",
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "unit": "percent",
                        "min": 0,
                        "max": 100,
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": 0},
                                {"color": "yellow", "value": 95},
                                {"color": "green", "value": 99},
                            ]
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="Average Processing Time",
                type="stat",
                gridPos={"h": 8, "w": 6, "x": 12, "y": 0},
                targets=[
                    {
                        "expr": 'avg(datadog.custom.etl_pipeline_duration_ms{pipeline=~"$pipeline",environment="$environment"})',
                        "legendFormat": "Avg Duration",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "textMode": "auto",
                    "colorMode": "value",
                    "graphMode": "area",
                },
                fieldConfig={
                    "defaults": {"color": {"mode": "palette-classic"}, "unit": "ms", "min": 0}
                },
            ),
            GrafanaPanel(
                title="Active Pipelines",
                type="stat",
                gridPos={"h": 8, "w": 6, "x": 18, "y": 0},
                targets=[
                    {
                        "expr": 'count(count by (pipeline)(datadog.custom.etl_pipeline_duration_ms{pipeline=~"$pipeline",environment="$environment"} offset 1m))',
                        "legendFormat": "Active Pipelines",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "textMode": "auto",
                    "colorMode": "value",
                    "graphMode": "area",
                },
                fieldConfig={
                    "defaults": {"color": {"mode": "palette-classic"}, "unit": "short", "min": 0}
                },
            ),
            # Row 2: Pipeline Performance Trends
            GrafanaPanel(
                title="Pipeline Duration Trends",
                type="timeseries",
                gridPos={"h": 8, "w": 12, "x": 0, "y": 8},
                targets=[
                    {
                        "expr": 'avg by (pipeline) (datadog.custom.etl_pipeline_duration_ms{pipeline=~"$pipeline",environment="$environment"})',
                        "legendFormat": "{{pipeline}} - Duration",
                        "refId": "A",
                    },
                    {
                        "expr": 'quantile by (pipeline) (0.95, datadog.custom.etl_pipeline_duration_ms{pipeline=~"$pipeline",environment="$environment"})',
                        "legendFormat": "{{pipeline}} - P95",
                        "refId": "B",
                    },
                ],
                options={
                    "tooltip": {"mode": "multi"},
                    "legend": {"displayMode": "table", "placement": "bottom"},
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": "ms",
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 1,
                            "fillOpacity": 0.1,
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="Records Processed Over Time",
                type="timeseries",
                gridPos={"h": 8, "w": 12, "x": 12, "y": 8},
                targets=[
                    {
                        "expr": 'sum by (pipeline) (rate(datadog.custom.etl_records_processed{pipeline=~"$pipeline",environment="$environment"}[5m]))',
                        "legendFormat": "{{pipeline}} - Records/sec",
                        "refId": "A",
                    },
                    {
                        "expr": 'sum by (pipeline) (rate(datadog.custom.etl_records_failed{pipeline=~"$pipeline",environment="$environment"}[5m]))',
                        "legendFormat": "{{pipeline}} - Failures/sec",
                        "refId": "B",
                    },
                ],
                options={
                    "tooltip": {"mode": "multi"},
                    "legend": {"displayMode": "table", "placement": "bottom"},
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": "reqps",
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 1,
                            "fillOpacity": 0.1,
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byRegexp", "options": ".*Failures.*"},
                            "properties": [
                                {"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}
                            ],
                        }
                    ],
                },
            ),
            # Row 3: Resource Utilization
            GrafanaPanel(
                title="Memory Usage by Pipeline",
                type="timeseries",
                gridPos={"h": 8, "w": 12, "x": 0, "y": 16},
                targets=[
                    {
                        "expr": 'avg by (pipeline) (datadog.custom.etl_memory_peak_mb{pipeline=~"$pipeline",environment="$environment"})',
                        "legendFormat": "{{pipeline}} - Memory",
                        "refId": "A",
                    }
                ],
                options={
                    "tooltip": {"mode": "multi"},
                    "legend": {"displayMode": "table", "placement": "bottom"},
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": "mbytes",
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 2,
                            "fillOpacity": 0.2,
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="Pipeline Error Distribution",
                type="piechart",
                gridPos={"h": 8, "w": 12, "x": 12, "y": 16},
                targets=[
                    {
                        "expr": 'sum by (pipeline) (datadog.custom.etl_pipeline_errors{pipeline=~"$pipeline",environment="$environment"})',
                        "legendFormat": "{{pipeline}}",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "pieType": "pie",
                    "tooltip": {"mode": "single"},
                    "legend": {"displayMode": "table", "placement": "bottom"},
                },
                fieldConfig={"defaults": {"color": {"mode": "palette-classic"}, "unit": "short"}},
            ),
        ]

        dashboard = GrafanaDashboard(
            title="ETL Pipeline Monitoring",
            description="Comprehensive monitoring for ETL pipelines including performance, quality, and resource utilization",
            tags=["etl", "data-engineering", "pipelines", "monitoring"],
            panels=panels,
            variables=variables,
            uid="etl-pipeline-monitoring",
        )

        return self._dashboard_to_json(dashboard)

    def generate_data_quality_dashboard(self) -> dict[str, Any]:
        """Generate Data Quality monitoring dashboard."""

        variables = [
            {
                "name": "dataset",
                "type": "query",
                "query": "datadog.custom.data_quality_completeness_score{*} by {dataset}",
                "label": "Dataset",
                "multi": True,
                "includeAll": True,
            }
        ]

        panels = [
            # Row 1: Quality Overview
            GrafanaPanel(
                title="Overall Data Quality Score",
                type="gauge",
                gridPos={"h": 8, "w": 8, "x": 0, "y": 0},
                targets=[
                    {
                        "expr": 'avg(datadog.custom.data_quality_completeness_score{dataset=~"$dataset"}) * 100',
                        "legendFormat": "Quality Score",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "showThresholdLabels": True,
                    "showThresholdMarkers": True,
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "unit": "percent",
                        "min": 0,
                        "max": 100,
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": 0},
                                {"color": "yellow", "value": 80},
                                {"color": "green", "value": 95},
                            ]
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="Quality Dimensions Heatmap",
                type="heatmap",
                gridPos={"h": 8, "w": 16, "x": 8, "y": 0},
                targets=[
                    {
                        "expr": 'avg by (dataset, dimension) (datadog.custom.data_quality_score{dataset=~"$dataset"})',
                        "legendFormat": "{{dataset}} - {{dimension}}",
                        "refId": "A",
                    }
                ],
                options={
                    "calculate": True,
                    "calculation": {"xBuckets": {"mode": "count", "value": "10"}},
                    "cellGap": 2,
                    "color": {"mode": "spectrum", "scale": "exponential", "steps": 64},
                    "yAxis": {"min": 0, "max": 1, "unit": "short"},
                },
            ),
            # Row 2: Quality Trends
            GrafanaPanel(
                title="Data Quality Trends",
                type="timeseries",
                gridPos={"h": 8, "w": 24, "x": 0, "y": 8},
                targets=[
                    {
                        "expr": 'avg by (dimension) (datadog.custom.data_quality_completeness_score{dataset=~"$dataset"})',
                        "legendFormat": "Completeness",
                        "refId": "A",
                    },
                    {
                        "expr": 'avg by (dimension) (datadog.custom.data_quality_accuracy_score{dataset=~"$dataset"})',
                        "legendFormat": "Accuracy",
                        "refId": "B",
                    },
                    {
                        "expr": 'avg by (dimension) (datadog.custom.data_quality_uniqueness_score{dataset=~"$dataset"})',
                        "legendFormat": "Uniqueness",
                        "refId": "C",
                    },
                ],
                options={
                    "tooltip": {"mode": "multi"},
                    "legend": {"displayMode": "table", "placement": "bottom"},
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": "percentunit",
                        "min": 0,
                        "max": 1,
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 2,
                            "fillOpacity": 0.1,
                        },
                    }
                },
            ),
            # Row 3: Quality Alerts and SLA
            GrafanaPanel(
                title="Quality SLA Compliance",
                type="stat",
                gridPos={"h": 8, "w": 12, "x": 0, "y": 16},
                targets=[
                    {
                        "expr": '(count(datadog.custom.slo.compliance{metric=~".*quality.*"} == 1) / count(datadog.custom.slo.compliance{metric=~".*quality.*"})) * 100',
                        "legendFormat": "SLA Compliance %",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "textMode": "auto",
                    "colorMode": "value",
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "unit": "percent",
                        "min": 0,
                        "max": 100,
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": 0},
                                {"color": "yellow", "value": 95},
                                {"color": "green", "value": 99},
                            ]
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="Data Quality Alerts",
                type="table",
                gridPos={"h": 8, "w": 12, "x": 12, "y": 16},
                targets=[
                    {
                        "expr": "increase(datadog.custom.validation.errors_rate[1h])",
                        "format": "table",
                        "instant": True,
                        "refId": "A",
                    }
                ],
                options={"showHeader": True},
                fieldConfig={
                    "defaults": {"custom": {"align": "auto", "displayMode": "auto"}},
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Value"},
                            "properties": [
                                {"id": "displayName", "value": "Error Count"},
                                {"id": "unit", "value": "short"},
                            ],
                        }
                    ],
                },
            ),
        ]

        dashboard = GrafanaDashboard(
            title="Data Quality Monitoring",
            description="Comprehensive data quality monitoring across all datasets and dimensions",
            tags=["data-quality", "monitoring", "sla", "validation"],
            panels=panels,
            variables=variables,
            uid="data-quality-monitoring",
        )

        return self._dashboard_to_json(dashboard)

    def generate_api_performance_dashboard(self) -> dict[str, Any]:
        """Generate API Performance monitoring dashboard."""

        variables = [
            {
                "name": "service",
                "type": "query",
                "query": "api.response_time{*} by {service}",
                "label": "Service",
                "multi": True,
                "includeAll": True,
            },
            {
                "name": "endpoint",
                "type": "query",
                "query": 'api.response_time{service=~"$service"} by {endpoint}',
                "label": "Endpoint",
                "multi": True,
                "includeAll": True,
            },
        ]

        panels = [
            # Row 1: API Overview
            GrafanaPanel(
                title="Request Rate",
                type="stat",
                gridPos={"h": 6, "w": 4, "x": 0, "y": 0},
                targets=[
                    {
                        "expr": 'sum(rate(api.request_count{service=~"$service",endpoint=~"$endpoint"}[5m]))',
                        "legendFormat": "Requests/sec",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "textMode": "auto",
                    "colorMode": "value",
                },
                fieldConfig={"defaults": {"color": {"mode": "palette-classic"}, "unit": "reqps"}},
            ),
            GrafanaPanel(
                title="Average Response Time",
                type="stat",
                gridPos={"h": 6, "w": 4, "x": 4, "y": 0},
                targets=[
                    {
                        "expr": 'avg(api.response_time{service=~"$service",endpoint=~"$endpoint"})',
                        "legendFormat": "Avg Response",
                        "refId": "A",
                    }
                ],
                fieldConfig={"defaults": {"color": {"mode": "palette-classic"}, "unit": "ms"}},
            ),
            GrafanaPanel(
                title="Error Rate",
                type="stat",
                gridPos={"h": 6, "w": 4, "x": 8, "y": 0},
                targets=[
                    {
                        "expr": '(sum(rate(api.request_count{service=~"$service",endpoint=~"$endpoint",status_code=~"[45].*"}[5m])) / sum(rate(api.request_count{service=~"$service",endpoint=~"$endpoint"}[5m]))) * 100',
                        "legendFormat": "Error Rate %",
                        "refId": "A",
                    }
                ],
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "unit": "percent",
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": 0},
                                {"color": "yellow", "value": 1},
                                {"color": "red", "value": 5},
                            ]
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="P95 Response Time",
                type="stat",
                gridPos={"h": 6, "w": 4, "x": 12, "y": 0},
                targets=[
                    {
                        "expr": 'quantile(0.95, api.response_time{service=~"$service",endpoint=~"$endpoint"})',
                        "legendFormat": "P95 Response",
                        "refId": "A",
                    }
                ],
                fieldConfig={"defaults": {"color": {"mode": "palette-classic"}, "unit": "ms"}},
            ),
            GrafanaPanel(
                title="Active Connections",
                type="stat",
                gridPos={"h": 6, "w": 4, "x": 16, "y": 0},
                targets=[
                    {
                        "expr": 'sum(api.active_connections{service=~"$service"})',
                        "legendFormat": "Connections",
                        "refId": "A",
                    }
                ],
                fieldConfig={"defaults": {"color": {"mode": "palette-classic"}, "unit": "short"}},
            ),
            GrafanaPanel(
                title="Uptime",
                type="stat",
                gridPos={"h": 6, "w": 4, "x": 20, "y": 0},
                targets=[
                    {
                        "expr": '(up{service=~"$service"}) * 100',
                        "legendFormat": "Uptime %",
                        "refId": "A",
                    }
                ],
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "unit": "percent",
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": 0},
                                {"color": "yellow", "value": 99},
                                {"color": "green", "value": 99.9},
                            ]
                        },
                    }
                },
            ),
            # Row 2: Performance Trends
            GrafanaPanel(
                title="Request Rate and Response Time",
                type="timeseries",
                gridPos={"h": 8, "w": 12, "x": 0, "y": 6},
                targets=[
                    {
                        "expr": 'sum by (service) (rate(api.request_count{service=~"$service",endpoint=~"$endpoint"}[5m]))',
                        "legendFormat": "{{service}} - Requests/sec",
                        "refId": "A",
                    },
                    {
                        "expr": 'avg by (service) (api.response_time{service=~"$service",endpoint=~"$endpoint"})',
                        "legendFormat": "{{service}} - Avg Response Time",
                        "refId": "B",
                    },
                ],
                options={
                    "tooltip": {"mode": "multi"},
                    "legend": {"displayMode": "table", "placement": "bottom"},
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 2,
                            "fillOpacity": 0.1,
                            "axisPlacement": "auto",
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byRegexp", "options": ".*Requests.*"},
                            "properties": [
                                {"id": "unit", "value": "reqps"},
                                {"id": "custom.axisPlacement", "value": "left"},
                            ],
                        },
                        {
                            "matcher": {"id": "byRegexp", "options": ".*Response Time.*"},
                            "properties": [
                                {"id": "unit", "value": "ms"},
                                {"id": "custom.axisPlacement", "value": "right"},
                            ],
                        },
                    ],
                },
            ),
            GrafanaPanel(
                title="Error Rate by Endpoint",
                type="timeseries",
                gridPos={"h": 8, "w": 12, "x": 12, "y": 6},
                targets=[
                    {
                        "expr": '(sum by (endpoint) (rate(api.request_count{service=~"$service",endpoint=~"$endpoint",status_code=~"[45].*"}[5m])) / sum by (endpoint) (rate(api.request_count{service=~"$service",endpoint=~"$endpoint"}[5m]))) * 100',
                        "legendFormat": "{{endpoint}} - Error Rate",
                        "refId": "A",
                    }
                ],
                options={
                    "tooltip": {"mode": "multi"},
                    "legend": {"displayMode": "table", "placement": "bottom"},
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": "percent",
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 2,
                            "fillOpacity": 0.1,
                        },
                    }
                },
            ),
            # Row 3: Detailed Analysis
            GrafanaPanel(
                title="Response Time Distribution",
                type="heatmap",
                gridPos={"h": 8, "w": 12, "x": 0, "y": 14},
                targets=[
                    {
                        "expr": 'sum by (le) (increase(api.response_time_bucket{service=~"$service",endpoint=~"$endpoint"}[5m]))',
                        "legendFormat": "{{le}}ms",
                        "refId": "A",
                    }
                ],
                options={
                    "calculate": True,
                    "calculation": {"xBuckets": {"mode": "count", "value": "20"}},
                    "cellGap": 2,
                    "color": {"mode": "spectrum", "scale": "exponential", "steps": 64},
                },
            ),
            GrafanaPanel(
                title="Top Slowest Endpoints",
                type="table",
                gridPos={"h": 8, "w": 12, "x": 12, "y": 14},
                targets=[
                    {
                        "expr": 'topk(10, avg by (endpoint) (api.response_time{service=~"$service",endpoint=~"$endpoint"}))',
                        "format": "table",
                        "instant": True,
                        "refId": "A",
                    }
                ],
                options={"showHeader": True, "sortBy": [{"desc": True, "displayName": "Value"}]},
                fieldConfig={
                    "defaults": {"custom": {"align": "auto", "displayMode": "auto"}},
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Value"},
                            "properties": [
                                {"id": "displayName", "value": "Response Time (ms)"},
                                {"id": "unit", "value": "ms"},
                                {"id": "custom.displayMode", "value": "color-background"},
                            ],
                        }
                    ],
                },
            ),
        ]

        dashboard = GrafanaDashboard(
            title="API Performance Monitoring",
            description="Comprehensive API performance monitoring including response times, error rates, and throughput",
            tags=["api", "performance", "monitoring", "sla"],
            panels=panels,
            variables=variables,
            uid="api-performance-monitoring",
        )

        return self._dashboard_to_json(dashboard)

    def generate_system_health_dashboard(self) -> dict[str, Any]:
        """Generate System Health and Resource monitoring dashboard."""

        variables = [
            {
                "name": "host",
                "type": "query",
                "query": "system.cpu.usage{*} by {host}",
                "label": "Host",
                "multi": True,
                "includeAll": True,
            }
        ]

        panels = [
            # Row 1: System Overview
            GrafanaPanel(
                title="System Health Score",
                type="gauge",
                gridPos={"h": 8, "w": 8, "x": 0, "y": 0},
                targets=[
                    {
                        "expr": 'avg(datadog.custom.system.health_score{host=~"$host"})',
                        "legendFormat": "Health Score",
                        "refId": "A",
                    }
                ],
                options={
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                    "orientation": "auto",
                    "showThresholdLabels": True,
                    "showThresholdMarkers": True,
                },
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "unit": "percentunit",
                        "min": 0,
                        "max": 1,
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": 0},
                                {"color": "yellow", "value": 0.7},
                                {"color": "green", "value": 0.9},
                            ]
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="CPU Usage",
                type="timeseries",
                gridPos={"h": 8, "w": 8, "x": 8, "y": 0},
                targets=[
                    {
                        "expr": 'avg by (host) (system.cpu.usage{host=~"$host"})',
                        "legendFormat": "{{host}} CPU",
                        "refId": "A",
                    }
                ],
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": "percent",
                        "min": 0,
                        "max": 100,
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 2,
                            "fillOpacity": 0.2,
                        },
                    }
                },
            ),
            GrafanaPanel(
                title="Memory Usage",
                type="timeseries",
                gridPos={"h": 8, "w": 8, "x": 16, "y": 0},
                targets=[
                    {
                        "expr": 'avg by (host) (system.memory.usage{host=~"$host"})',
                        "legendFormat": "{{host}} Memory",
                        "refId": "A",
                    }
                ],
                fieldConfig={
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "unit": "percent",
                        "min": 0,
                        "max": 100,
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "lineWidth": 2,
                            "fillOpacity": 0.2,
                        },
                    }
                },
            ),
        ]

        dashboard = GrafanaDashboard(
            title="System Health Monitoring",
            description="System resource utilization and health monitoring",
            tags=["system", "health", "resources", "monitoring"],
            panels=panels,
            variables=variables,
            uid="system-health-monitoring",
        )

        return self._dashboard_to_json(dashboard)

    def _dashboard_to_json(self, dashboard: GrafanaDashboard) -> dict[str, Any]:
        """Convert dashboard object to Grafana JSON format."""

        dashboard_json = {
            "dashboard": {
                "id": None,
                "uid": dashboard.uid,
                "title": dashboard.title,
                "description": dashboard.description,
                "tags": dashboard.tags,
                "timezone": "browser",
                "panels": [],
                "time": dashboard.time_range,
                "timepicker": {},
                "templating": {"list": dashboard.variables},
                "annotations": {"list": []},
                "refresh": dashboard.refresh,
                "schemaVersion": 30,
                "version": 1,
                "links": [],
                "gnetId": None,
                "graphTooltip": 0,
            },
            "overwrite": True,
            "message": f"Updated {dashboard.title} dashboard",
            "folderId": None,
        }

        # Convert panels
        for i, panel in enumerate(dashboard.panels):
            panel_json = {
                "id": i + 1,
                "title": panel.title,
                "type": panel.type,
                "targets": panel.targets,
                "gridPos": panel.gridPos,
                "options": panel.options,
                "fieldConfig": panel.fieldConfig,
                "transformations": panel.transformations,
                "datasource": {"type": "prometheus", "uid": self.datasource},
            }

            if panel.alert:
                panel_json["alert"] = panel.alert

            dashboard_json["dashboard"]["panels"].append(panel_json)

        return dashboard_json

    def generate_all_dashboards(self) -> dict[str, dict[str, Any]]:
        """Generate all monitoring dashboards."""

        dashboards = {
            "etl_pipeline": self.generate_etl_pipeline_dashboard(),
            "data_quality": self.generate_data_quality_dashboard(),
            "api_performance": self.generate_api_performance_dashboard(),
            "system_health": self.generate_system_health_dashboard(),
        }

        self.logger.info(f"Generated {len(dashboards)} Grafana dashboards")
        return dashboards

    def export_dashboards(self, output_dir: str = "dashboards") -> None:
        """Export all dashboards to JSON files."""
        import os

        os.makedirs(output_dir, exist_ok=True)
        dashboards = self.generate_all_dashboards()

        for name, dashboard_config in dashboards.items():
            file_path = os.path.join(output_dir, f"{name}_dashboard.json")
            with open(file_path, "w") as f:
                json.dump(dashboard_config, f, indent=2)
            self.logger.info(f"Exported dashboard: {file_path}")


# Global dashboard generator
_dashboard_generator = None


def get_dashboard_generator() -> GrafanaDashboardGenerator:
    """Get global dashboard generator instance."""
    global _dashboard_generator
    if _dashboard_generator is None:
        _dashboard_generator = GrafanaDashboardGenerator()
    return _dashboard_generator


if __name__ == "__main__":
    # Generate and export all dashboards
    generator = GrafanaDashboardGenerator()

    # Generate individual dashboards
    etl_dashboard = generator.generate_etl_pipeline_dashboard()
    print("ETL Pipeline Dashboard generated")

    quality_dashboard = generator.generate_data_quality_dashboard()
    print("Data Quality Dashboard generated")

    api_dashboard = generator.generate_api_performance_dashboard()
    print("API Performance Dashboard generated")

    system_dashboard = generator.generate_system_health_dashboard()
    print("System Health Dashboard generated")

    # Export all dashboards
    generator.export_dashboards()
    print("All dashboards exported to JSON files")
