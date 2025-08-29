"""
DataDog Log Visualization and Real-time Monitoring System

This module provides comprehensive log visualization and dashboard capabilities including:
- Real-time log streaming dashboards
- Error rate and pattern visualization
- Performance trend analysis
- Security event monitoring dashboards
- Business process logging analytics
- Custom dashboard creation and management
- Interactive log exploration
- Automated dashboard updates
"""

import json
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from enum import Enum
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import threading
import queue
from pathlib import Path
import uuid

# Data visualization imports
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

# DataDog API imports
from datadog import initialize, api

# Internal imports
from ..core.logging import get_logger
from .enterprise_datadog_log_collection import LogEntry, LogLevel, LogSource
from .enterprise_log_analysis import AnomalyResult, PatternMatch, AnomalyType
from .enterprise_security_audit_logging import SecurityEvent, AuditEvent

logger = get_logger(__name__)


class DashboardType(Enum):
    """Types of dashboards"""
    OVERVIEW = "overview"
    SECURITY = "security"
    PERFORMANCE = "performance"
    ML_PIPELINE = "ml_pipeline"
    DATA_PIPELINE = "data_pipeline"
    API_MONITORING = "api_monitoring"
    ERROR_TRACKING = "error_tracking"
    BUSINESS_ANALYTICS = "business_analytics"
    COMPLIANCE = "compliance"
    REAL_TIME = "real_time"


class VisualizationType(Enum):
    """Types of visualizations"""
    TIME_SERIES = "time_series"
    BAR_CHART = "bar_chart"
    PIE_CHART = "pie_chart"
    HEATMAP = "heatmap"
    TABLE = "table"
    GAUGE = "gauge"
    SCATTER_PLOT = "scatter_plot"
    HISTOGRAM = "histogram"
    TREEMAP = "treemap"
    SANKEY = "sankey"


class AlertCondition(Enum):
    """Alert condition types"""
    THRESHOLD_ABOVE = "threshold_above"
    THRESHOLD_BELOW = "threshold_below"
    RATE_INCREASE = "rate_increase"
    RATE_DECREASE = "rate_decrease"
    ANOMALY_DETECTED = "anomaly_detected"
    PATTERN_MATCH = "pattern_match"


@dataclass
class DashboardWidget:
    """Dashboard widget configuration"""
    widget_id: str
    title: str
    visualization_type: VisualizationType
    data_source: str
    query: str
    time_range: str = "1h"
    refresh_interval_seconds: int = 60
    position: Dict[str, int] = None  # x, y, width, height
    styling: Dict[str, Any] = None
    thresholds: Dict[str, float] = None
    
    def __post_init__(self):
        if self.position is None:
            self.position = {"x": 0, "y": 0, "width": 4, "height": 3}
        if self.styling is None:
            self.styling = {}
        if self.thresholds is None:
            self.thresholds = {}


@dataclass
class Dashboard:
    """Dashboard configuration"""
    dashboard_id: str
    title: str
    description: str
    dashboard_type: DashboardType
    widgets: List[DashboardWidget]
    is_public: bool = False
    auto_refresh: bool = True
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.tags is None:
            self.tags = []


@dataclass
class AlertRule:
    """Alert rule configuration"""
    rule_id: str
    name: str
    description: str
    condition: AlertCondition
    query: str
    threshold_value: Optional[float] = None
    time_window_minutes: int = 15
    evaluation_frequency_minutes: int = 5
    notification_channels: List[str] = None
    severity: str = "medium"
    is_enabled: bool = True
    
    def __post_init__(self):
        if self.notification_channels is None:
            self.notification_channels = []


class DataDogDashboardManager:
    """Manages DataDog dashboard creation and updates"""
    
    def __init__(self, api_key: str, app_key: str):
        self.logger = get_logger(f"{__name__}.DataDogDashboardManager")
        
        # Initialize DataDog API
        initialize(api_key=api_key, app_key=app_key)
        
        # Dashboard storage
        self.dashboards = {}
        self.widgets_data_cache = {}
        
        # Update tracking
        self.last_update_times = {}
        
    def create_dashboard(self, dashboard: Dashboard) -> str:
        """Create a new DataDog dashboard"""
        try:
            # Convert dashboard to DataDog format
            dashboard_payload = self._convert_to_datadog_format(dashboard)
            
            # Create dashboard via DataDog API
            response = api.Dashboard.create(**dashboard_payload)
            
            if response.get('id'):
                dashboard.dashboard_id = response['id']
                self.dashboards[dashboard.dashboard_id] = dashboard
                
                self.logger.info(f"Created DataDog dashboard: {dashboard.title} (ID: {dashboard.dashboard_id})")
                return dashboard.dashboard_id
            else:
                self.logger.error(f"Failed to create dashboard: {response}")
                return ""
                
        except Exception as e:
            self.logger.error(f"Error creating dashboard: {e}")
            return ""
    
    def update_dashboard(self, dashboard_id: str, dashboard: Dashboard) -> bool:
        """Update an existing DataDog dashboard"""
        try:
            # Convert dashboard to DataDog format
            dashboard_payload = self._convert_to_datadog_format(dashboard)
            
            # Update dashboard via DataDog API
            response = api.Dashboard.update(dashboard_id, **dashboard_payload)
            
            if response.get('id'):
                self.dashboards[dashboard_id] = dashboard
                self.last_update_times[dashboard_id] = datetime.now()
                
                self.logger.info(f"Updated DataDog dashboard: {dashboard.title}")
                return True
            else:
                self.logger.error(f"Failed to update dashboard: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error updating dashboard: {e}")
            return False
    
    def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a DataDog dashboard"""
        try:
            response = api.Dashboard.delete(dashboard_id)
            
            if dashboard_id in self.dashboards:
                del self.dashboards[dashboard_id]
            if dashboard_id in self.last_update_times:
                del self.last_update_times[dashboard_id]
                
            self.logger.info(f"Deleted DataDog dashboard: {dashboard_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting dashboard {dashboard_id}: {e}")
            return False
    
    def get_dashboard_data(self, dashboard_id: str) -> Optional[Dict[str, Any]]:
        """Get data for dashboard widgets"""
        try:
            if dashboard_id not in self.dashboards:
                return None
            
            dashboard = self.dashboards[dashboard_id]
            dashboard_data = {
                "dashboard_id": dashboard_id,
                "title": dashboard.title,
                "widgets_data": {},
                "last_updated": datetime.now().isoformat()
            }
            
            # Get data for each widget
            for widget in dashboard.widgets:
                widget_data = self._get_widget_data(widget)
                dashboard_data["widgets_data"][widget.widget_id] = widget_data
            
            return dashboard_data
            
        except Exception as e:
            self.logger.error(f"Error getting dashboard data: {e}")
            return None
    
    def _convert_to_datadog_format(self, dashboard: Dashboard) -> Dict[str, Any]:
        """Convert internal dashboard format to DataDog API format"""
        try:
            # Convert widgets to DataDog format
            widgets = []
            for widget in dashboard.widgets:
                dd_widget = self._convert_widget_to_datadog_format(widget)
                widgets.append(dd_widget)
            
            return {
                "title": dashboard.title,
                "description": dashboard.description,
                "widgets": widgets,
                "layout_type": "ordered",  # or "free"
                "is_read_only": not dashboard.is_public,
                "tags": dashboard.tags
            }
            
        except Exception as e:
            self.logger.error(f"Error converting dashboard to DataDog format: {e}")
            return {}
    
    def _convert_widget_to_datadog_format(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Convert widget to DataDog format"""
        try:
            # Base widget structure
            dd_widget = {
                "id": widget.widget_id,
                "definition": {
                    "title": widget.title,
                    "type": self._map_visualization_type(widget.visualization_type),
                    "requests": [{
                        "q": widget.query,
                        "aggregator": "avg"
                    }]
                }
            }
            
            # Add visualization-specific properties
            if widget.visualization_type == VisualizationType.TIME_SERIES:
                dd_widget["definition"]["show_legend"] = True
                dd_widget["definition"]["legend_size"] = "0"
            elif widget.visualization_type == VisualizationType.GAUGE:
                if widget.thresholds:
                    dd_widget["definition"]["custom_links"] = []
                    for threshold_name, threshold_value in widget.thresholds.items():
                        dd_widget["definition"]["custom_links"].append({
                            "label": threshold_name,
                            "url": f"threshold_{threshold_value}"
                        })
            
            return dd_widget
            
        except Exception as e:
            self.logger.error(f"Error converting widget to DataDog format: {e}")
            return {}
    
    def _map_visualization_type(self, viz_type: VisualizationType) -> str:
        """Map internal visualization type to DataDog type"""
        mapping = {
            VisualizationType.TIME_SERIES: "timeseries",
            VisualizationType.BAR_CHART: "query_value",
            VisualizationType.PIE_CHART: "pie_chart",
            VisualizationType.HEATMAP: "heatmap",
            VisualizationType.TABLE: "query_table",
            VisualizationType.GAUGE: "gauge",
            VisualizationType.SCATTER_PLOT: "scatterplot",
            VisualizationType.HISTOGRAM: "distribution"
        }
        return mapping.get(viz_type, "timeseries")
    
    def _get_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Get data for a specific widget"""
        try:
            # For this implementation, we'll simulate data
            # In a real implementation, this would query DataDog metrics API
            
            current_time = datetime.now()
            time_range_delta = self._parse_time_range(widget.time_range)
            start_time = current_time - time_range_delta
            
            # Generate sample data based on widget type
            if widget.visualization_type == VisualizationType.TIME_SERIES:
                data = self._generate_time_series_data(start_time, current_time, widget.query)
            elif widget.visualization_type == VisualizationType.BAR_CHART:
                data = self._generate_bar_chart_data(widget.query)
            elif widget.visualization_type == VisualizationType.PIE_CHART:
                data = self._generate_pie_chart_data(widget.query)
            elif widget.visualization_type == VisualizationType.TABLE:
                data = self._generate_table_data(widget.query)
            else:
                data = {"message": "Visualization type not yet implemented"}
            
            return {
                "widget_id": widget.widget_id,
                "data": data,
                "last_updated": current_time.isoformat(),
                "query": widget.query
            }
            
        except Exception as e:
            self.logger.error(f"Error getting widget data: {e}")
            return {"error": str(e)}
    
    def _parse_time_range(self, time_range: str) -> timedelta:
        """Parse time range string to timedelta"""
        if time_range.endswith('m'):
            return timedelta(minutes=int(time_range[:-1]))
        elif time_range.endswith('h'):
            return timedelta(hours=int(time_range[:-1]))
        elif time_range.endswith('d'):
            return timedelta(days=int(time_range[:-1]))
        else:
            return timedelta(hours=1)  # Default to 1 hour
    
    def _generate_time_series_data(self, start_time: datetime, end_time: datetime, query: str) -> Dict[str, Any]:
        """Generate sample time series data"""
        timestamps = []
        values = []
        
        current = start_time
        while current <= end_time:
            timestamps.append(current.isoformat())
            # Generate realistic-looking data based on query
            if "error" in query.lower():
                value = max(0, np.random.normal(5, 2))  # Error rate around 5%
            elif "response_time" in query.lower():
                value = max(0, np.random.normal(200, 50))  # Response time around 200ms
            elif "throughput" in query.lower():
                value = max(0, np.random.normal(1000, 100))  # Throughput around 1000 rps
            else:
                value = max(0, np.random.normal(100, 20))  # Generic metric
            
            values.append(round(value, 2))
            current += timedelta(minutes=5)
        
        return {
            "type": "time_series",
            "timestamps": timestamps,
            "values": values,
            "unit": self._infer_unit_from_query(query)
        }
    
    def _generate_bar_chart_data(self, query: str) -> Dict[str, Any]:
        """Generate sample bar chart data"""
        if "service" in query.lower():
            labels = ["api-gateway", "auth-service", "user-service", "order-service", "payment-service"]
        elif "environment" in query.lower():
            labels = ["production", "staging", "development"]
        elif "status" in query.lower():
            labels = ["200", "400", "401", "403", "404", "500", "502", "503"]
        else:
            labels = [f"Category {i}" for i in range(1, 6)]
        
        values = [max(0, np.random.normal(100, 20)) for _ in labels]
        
        return {
            "type": "bar_chart",
            "labels": labels,
            "values": [round(v, 2) for v in values],
            "unit": self._infer_unit_from_query(query)
        }
    
    def _generate_pie_chart_data(self, query: str) -> Dict[str, Any]:
        """Generate sample pie chart data"""
        if "log_level" in query.lower():
            labels = ["INFO", "WARNING", "ERROR", "DEBUG"]
            values = [70, 15, 10, 5]
        elif "source" in query.lower():
            labels = ["API", "Database", "ML Pipeline", "ETL", "Security"]
            values = [30, 25, 20, 15, 10]
        else:
            labels = [f"Segment {i}" for i in range(1, 5)]
            values = [25, 30, 25, 20]
        
        return {
            "type": "pie_chart",
            "labels": labels,
            "values": values,
            "unit": "percent"
        }
    
    def _generate_table_data(self, query: str) -> Dict[str, Any]:
        """Generate sample table data"""
        if "error" in query.lower():
            headers = ["Timestamp", "Service", "Error Type", "Count", "Last Occurrence"]
            rows = [
                [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "api-gateway", "ConnectionError", "15", "2 minutes ago"],
                [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "database", "QueryTimeout", "8", "5 minutes ago"],
                [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "ml-pipeline", "ModelNotFound", "3", "10 minutes ago"]
            ]
        else:
            headers = ["Metric", "Current Value", "Previous Value", "Change", "Status"]
            rows = [
                ["Response Time", "195ms", "180ms", "+8.3%", "Good"],
                ["Error Rate", "2.3%", "1.8%", "+0.5%", "Warning"],
                ["Throughput", "1,250 rps", "1,180 rps", "+5.9%", "Good"]
            ]
        
        return {
            "type": "table",
            "headers": headers,
            "rows": rows
        }
    
    def _infer_unit_from_query(self, query: str) -> str:
        """Infer unit from query"""
        query_lower = query.lower()
        if "time" in query_lower or "latency" in query_lower:
            return "ms"
        elif "rate" in query_lower or "percent" in query_lower:
            return "%"
        elif "count" in query_lower or "number" in query_lower:
            return "count"
        elif "bytes" in query_lower or "size" in query_lower:
            return "bytes"
        else:
            return "unit"


class LogVisualizationEngine:
    """Creates interactive log visualizations"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.LogVisualizationEngine")
        self.color_palette = px.colors.qualitative.Set3
    
    def create_log_volume_chart(self, logs: List[LogEntry], time_bucket_minutes: int = 5) -> go.Figure:
        """Create log volume time series chart"""
        try:
            # Convert logs to DataFrame
            df_data = []
            for log in logs:
                df_data.append({
                    "timestamp": log.timestamp,
                    "service": log.service,
                    "level": log.level.value,
                    "source": log.source.value
                })
            
            df = pd.DataFrame(df_data)
            
            if df.empty:
                return go.Figure().add_annotation(text="No data available")
            
            # Create time buckets
            df['time_bucket'] = df['timestamp'].dt.floor(f'{time_bucket_minutes}T')
            
            # Aggregate by time bucket and service
            agg_df = df.groupby(['time_bucket', 'service']).size().reset_index(name='count')
            
            # Create time series chart
            fig = px.line(
                agg_df, 
                x='time_bucket', 
                y='count', 
                color='service',
                title='Log Volume Over Time',
                labels={'time_bucket': 'Time', 'count': 'Number of Logs'}
            )
            
            fig.update_layout(
                showlegend=True,
                height=400,
                hovermode='x unified'
            )
            
            return fig
            
        except Exception as e:
            self.logger.error(f"Error creating log volume chart: {e}")
            return go.Figure().add_annotation(text=f"Error: {str(e)}")
    
    def create_error_rate_chart(self, logs: List[LogEntry], time_bucket_minutes: int = 5) -> go.Figure:
        """Create error rate chart"""
        try:
            df_data = []
            for log in logs:
                df_data.append({
                    "timestamp": log.timestamp,
                    "service": log.service,
                    "is_error": log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL]
                })
            
            df = pd.DataFrame(df_data)
            
            if df.empty:
                return go.Figure().add_annotation(text="No data available")
            
            # Create time buckets
            df['time_bucket'] = df['timestamp'].dt.floor(f'{time_bucket_minutes}T')
            
            # Calculate error rates
            error_rates = df.groupby(['time_bucket', 'service']).agg({
                'is_error': ['sum', 'count']
            }).reset_index()
            
            error_rates.columns = ['time_bucket', 'service', 'errors', 'total']
            error_rates['error_rate'] = (error_rates['errors'] / error_rates['total']) * 100
            
            # Create line chart
            fig = px.line(
                error_rates,
                x='time_bucket',
                y='error_rate',
                color='service',
                title='Error Rate Over Time',
                labels={'time_bucket': 'Time', 'error_rate': 'Error Rate (%)'}
            )
            
            # Add threshold line
            fig.add_hline(y=5.0, line_dash="dash", line_color="red", 
                         annotation_text="5% Threshold")
            
            fig.update_layout(height=400)
            
            return fig
            
        except Exception as e:
            self.logger.error(f"Error creating error rate chart: {e}")
            return go.Figure().add_annotation(text=f"Error: {str(e)}")
    
    def create_service_heatmap(self, logs: List[LogEntry]) -> go.Figure:
        """Create service activity heatmap"""
        try:
            df_data = []
            for log in logs:
                df_data.append({
                    "timestamp": log.timestamp,
                    "service": log.service,
                    "hour": log.timestamp.hour,
                    "day_of_week": log.timestamp.strftime("%A")
                })
            
            df = pd.DataFrame(df_data)
            
            if df.empty:
                return go.Figure().add_annotation(text="No data available")
            
            # Aggregate by hour and day of week
            heatmap_data = df.groupby(['day_of_week', 'hour']).size().reset_index(name='count')
            
            # Pivot for heatmap
            pivot_df = heatmap_data.pivot(index='day_of_week', columns='hour', values='count').fillna(0)
            
            # Create heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=list(range(24)),
                y=pivot_df.index,
                colorscale='Viridis',
                hoverongaps=False
            ))
            
            fig.update_layout(
                title='Service Activity Heatmap (by Hour and Day)',
                xaxis_title='Hour of Day',
                yaxis_title='Day of Week',
                height=400
            )
            
            return fig
            
        except Exception as e:
            self.logger.error(f"Error creating service heatmap: {e}")
            return go.Figure().add_annotation(text=f"Error: {str(e)}")
    
    def create_log_level_distribution(self, logs: List[LogEntry]) -> go.Figure:
        """Create log level distribution pie chart"""
        try:
            level_counts = defaultdict(int)
            for log in logs:
                level_counts[log.level.value] += 1
            
            if not level_counts:
                return go.Figure().add_annotation(text="No data available")
            
            labels = list(level_counts.keys())
            values = list(level_counts.values())
            
            # Create pie chart with custom colors
            colors = {
                'DEBUG': '#17becf',
                'INFO': '#2ca02c',
                'WARNING': '#ff7f0e',
                'ERROR': '#d62728',
                'CRITICAL': '#8b0000',
                'FATAL': '#4b0000'
            }
            
            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                hole=0.4,
                marker_colors=[colors.get(label, '#1f77b4') for label in labels]
            )])
            
            fig.update_layout(
                title='Log Level Distribution',
                height=400
            )
            
            return fig
            
        except Exception as e:
            self.logger.error(f"Error creating log level distribution: {e}")
            return go.Figure().add_annotation(text=f"Error: {str(e)}")
    
    def create_response_time_distribution(self, logs: List[LogEntry]) -> go.Figure:
        """Create response time distribution histogram"""
        try:
            response_times = [log.duration_ms for log in logs if log.duration_ms is not None and log.duration_ms > 0]
            
            if not response_times:
                return go.Figure().add_annotation(text="No response time data available")
            
            fig = go.Figure(data=[go.Histogram(
                x=response_times,
                nbinsx=50,
                opacity=0.7,
                marker_color='steelblue'
            )])
            
            # Add percentile lines
            p50 = np.percentile(response_times, 50)
            p95 = np.percentile(response_times, 95)
            p99 = np.percentile(response_times, 99)
            
            fig.add_vline(x=p50, line_dash="dash", line_color="green", 
                         annotation_text=f"P50: {p50:.1f}ms")
            fig.add_vline(x=p95, line_dash="dash", line_color="orange", 
                         annotation_text=f"P95: {p95:.1f}ms")
            fig.add_vline(x=p99, line_dash="dash", line_color="red", 
                         annotation_text=f"P99: {p99:.1f}ms")
            
            fig.update_layout(
                title='Response Time Distribution',
                xaxis_title='Response Time (ms)',
                yaxis_title='Frequency',
                height=400
            )
            
            return fig
            
        except Exception as e:
            self.logger.error(f"Error creating response time distribution: {e}")
            return go.Figure().add_annotation(text=f"Error: {str(e)}")
    
    def create_security_events_timeline(self, security_events: List[SecurityEvent]) -> go.Figure:
        """Create security events timeline"""
        try:
            if not security_events:
                return go.Figure().add_annotation(text="No security events")
            
            # Prepare data
            timeline_data = []
            for event in security_events:
                timeline_data.append({
                    "timestamp": event.timestamp,
                    "event_type": event.event_type.value,
                    "risk_level": event.risk_level.value,
                    "description": event.description,
                    "user_id": event.user_id or "Unknown",
                    "ip_address": event.ip_address or "Unknown"
                })
            
            df = pd.DataFrame(timeline_data)
            
            # Create timeline scatter plot
            risk_level_colors = {
                'very_low': '#2ca02c',
                'low': '#17becf',
                'medium': '#ff7f0e',
                'high': '#d62728',
                'critical': '#8b0000'
            }
            
            fig = px.scatter(
                df,
                x='timestamp',
                y='event_type',
                color='risk_level',
                size_max=15,
                hover_data=['description', 'user_id', 'ip_address'],
                title='Security Events Timeline',
                color_discrete_map=risk_level_colors
            )
            
            fig.update_layout(
                height=500,
                showlegend=True,
                yaxis_title='Event Type'
            )
            
            return fig
            
        except Exception as e:
            self.logger.error(f"Error creating security events timeline: {e}")
            return go.Figure().add_annotation(text=f"Error: {str(e)}")
    
    def create_anomaly_detection_chart(self, logs: List[LogEntry], anomalies: List[AnomalyResult]) -> go.Figure:
        """Create anomaly detection visualization"""
        try:
            # Create subplots
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Log Volume with Anomalies', 'Anomaly Types Distribution'),
                specs=[[{"secondary_y": True}], [{"type": "pie"}]]
            )
            
            # Prepare log volume data
            df_data = []
            for log in logs:
                df_data.append({
                    "timestamp": log.timestamp,
                    "count": 1
                })
            
            if df_data:
                df = pd.DataFrame(df_data)
                df['time_bucket'] = df['timestamp'].dt.floor('5T')
                volume_df = df.groupby('time_bucket')['count'].sum().reset_index()
                
                # Add log volume line
                fig.add_trace(
                    go.Scatter(
                        x=volume_df['time_bucket'],
                        y=volume_df['count'],
                        mode='lines',
                        name='Log Volume',
                        line=dict(color='blue')
                    ),
                    row=1, col=1
                )
            
            # Add anomaly markers
            if anomalies:
                anomaly_times = [anomaly.timestamp for anomaly in anomalies]
                anomaly_types = [anomaly.anomaly_type.value for anomaly in anomalies]
                
                fig.add_trace(
                    go.Scatter(
                        x=anomaly_times,
                        y=[100] * len(anomaly_times),  # Fixed height for visibility
                        mode='markers',
                        name='Anomalies',
                        marker=dict(color='red', size=10, symbol='triangle-up'),
                        text=anomaly_types,
                        hovertemplate='<b>Anomaly</b><br>Time: %{x}<br>Type: %{text}<extra></extra>'
                    ),
                    row=1, col=1
                )
                
                # Anomaly type distribution
                anomaly_type_counts = defaultdict(int)
                for anomaly in anomalies:
                    anomaly_type_counts[anomaly.anomaly_type.value] += 1
                
                fig.add_trace(
                    go.Pie(
                        labels=list(anomaly_type_counts.keys()),
                        values=list(anomaly_type_counts.values()),
                        name="Anomaly Types"
                    ),
                    row=2, col=1
                )
            
            fig.update_layout(
                height=800,
                title='Log Volume and Anomaly Detection',
                showlegend=True
            )
            
            return fig
            
        except Exception as e:
            self.logger.error(f"Error creating anomaly detection chart: {e}")
            return go.Figure().add_annotation(text=f"Error: {str(e)}")


class RealTimeLogMonitor:
    """Real-time log monitoring and streaming"""
    
    def __init__(self, update_interval_seconds: int = 30):
        self.logger = get_logger(f"{__name__}.RealTimeLogMonitor")
        self.update_interval_seconds = update_interval_seconds
        self.is_running = False
        self.monitoring_thread = None
        
        # Data storage
        self.live_logs = deque(maxlen=10000)
        self.live_metrics = defaultdict(list)
        self.subscribers = {}  # Dashboard ID -> callback function
        
        # Alert rules
        self.alert_rules = {}
        self.last_alert_times = defaultdict(datetime.min)
        
    def start_monitoring(self):
        """Start real-time monitoring"""
        try:
            self.is_running = True
            self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitoring_thread.start()
            
            self.logger.info("Real-time log monitoring started")
            
        except Exception as e:
            self.logger.error(f"Error starting real-time monitoring: {e}")
    
    def stop_monitoring(self):
        """Stop real-time monitoring"""
        try:
            self.is_running = False
            if self.monitoring_thread:
                self.monitoring_thread.join(timeout=5)
            
            self.logger.info("Real-time log monitoring stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping real-time monitoring: {e}")
    
    def add_logs(self, logs: List[LogEntry]):
        """Add new logs to real-time monitoring"""
        try:
            for log in logs:
                self.live_logs.append(log)
            
            # Update live metrics
            self._update_live_metrics(logs)
            
            # Check alert rules
            self._check_alert_rules(logs)
            
        except Exception as e:
            self.logger.error(f"Error adding logs to real-time monitor: {e}")
    
    def subscribe_to_updates(self, dashboard_id: str, callback: Callable):
        """Subscribe to real-time updates"""
        self.subscribers[dashboard_id] = callback
        self.logger.info(f"Dashboard {dashboard_id} subscribed to real-time updates")
    
    def unsubscribe_from_updates(self, dashboard_id: str):
        """Unsubscribe from real-time updates"""
        if dashboard_id in self.subscribers:
            del self.subscribers[dashboard_id]
            self.logger.info(f"Dashboard {dashboard_id} unsubscribed from real-time updates")
    
    def add_alert_rule(self, rule: AlertRule):
        """Add alert rule"""
        self.alert_rules[rule.rule_id] = rule
        self.logger.info(f"Added alert rule: {rule.name}")
    
    def remove_alert_rule(self, rule_id: str):
        """Remove alert rule"""
        if rule_id in self.alert_rules:
            del self.alert_rules[rule_id]
            self.logger.info(f"Removed alert rule: {rule_id}")
    
    def get_live_metrics(self) -> Dict[str, Any]:
        """Get current live metrics"""
        try:
            current_time = datetime.now()
            recent_logs = [log for log in self.live_logs 
                          if (current_time - log.timestamp).total_seconds() < 300]  # Last 5 minutes
            
            if not recent_logs:
                return {"message": "No recent logs"}
            
            # Calculate metrics
            total_logs = len(recent_logs)
            error_logs = sum(1 for log in recent_logs 
                           if log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL])
            
            services = set(log.service for log in recent_logs)
            avg_response_time = np.mean([log.duration_ms for log in recent_logs 
                                       if log.duration_ms is not None])
            
            return {
                "timestamp": current_time.isoformat(),
                "total_logs_5min": total_logs,
                "error_count_5min": error_logs,
                "error_rate_5min": (error_logs / total_logs) * 100 if total_logs > 0 else 0,
                "active_services": len(services),
                "avg_response_time_ms": round(avg_response_time, 2) if not np.isnan(avg_response_time) else None,
                "logs_per_second": round(total_logs / 300, 2)  # 5 minutes = 300 seconds
            }
            
        except Exception as e:
            self.logger.error(f"Error getting live metrics: {e}")
            return {"error": str(e)}
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                time.sleep(self.update_interval_seconds)
                
                # Get updated metrics
                live_metrics = self.get_live_metrics()
                
                # Notify subscribers
                for dashboard_id, callback in self.subscribers.items():
                    try:
                        callback(live_metrics)
                    except Exception as e:
                        self.logger.error(f"Error notifying subscriber {dashboard_id}: {e}")
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
    
    def _update_live_metrics(self, logs: List[LogEntry]):
        """Update live metrics with new logs"""
        try:
            current_time = datetime.now()
            
            for log in logs:
                # Update service metrics
                service_key = f"service_{log.service}"
                self.live_metrics[service_key].append({
                    "timestamp": current_time,
                    "log_count": 1,
                    "error": log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL]
                })
                
                # Keep only recent metrics (last hour)
                cutoff_time = current_time - timedelta(hours=1)
                self.live_metrics[service_key] = [
                    m for m in self.live_metrics[service_key] 
                    if m["timestamp"] > cutoff_time
                ]
            
        except Exception as e:
            self.logger.error(f"Error updating live metrics: {e}")
    
    def _check_alert_rules(self, logs: List[LogEntry]):
        """Check alert rules against new logs"""
        try:
            for rule_id, rule in self.alert_rules.items():
                if not rule.is_enabled:
                    continue
                
                # Check if enough time has passed since last alert
                time_since_last_alert = datetime.now() - self.last_alert_times[rule_id]
                if time_since_last_alert.total_seconds() < rule.evaluation_frequency_minutes * 60:
                    continue
                
                # Evaluate rule condition
                alert_triggered = self._evaluate_alert_condition(rule, logs)
                
                if alert_triggered:
                    self._trigger_alert(rule)
                    self.last_alert_times[rule_id] = datetime.now()
            
        except Exception as e:
            self.logger.error(f"Error checking alert rules: {e}")
    
    def _evaluate_alert_condition(self, rule: AlertRule, logs: List[LogEntry]) -> bool:
        """Evaluate if alert condition is met"""
        try:
            current_time = datetime.now()
            time_window_start = current_time - timedelta(minutes=rule.time_window_minutes)
            
            # Get logs in time window
            window_logs = [log for log in self.live_logs 
                          if time_window_start <= log.timestamp <= current_time]
            
            if rule.condition == AlertCondition.THRESHOLD_ABOVE:
                if "error_rate" in rule.query.lower():
                    error_count = sum(1 for log in window_logs 
                                    if log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL])
                    error_rate = (error_count / len(window_logs)) * 100 if window_logs else 0
                    return error_rate > (rule.threshold_value or 0)
                
                elif "response_time" in rule.query.lower():
                    response_times = [log.duration_ms for log in window_logs 
                                    if log.duration_ms is not None]
                    if response_times:
                        avg_response_time = np.mean(response_times)
                        return avg_response_time > (rule.threshold_value or 0)
            
            elif rule.condition == AlertCondition.RATE_INCREASE:
                # Compare current window with previous window
                prev_window_start = time_window_start - timedelta(minutes=rule.time_window_minutes)
                prev_window_logs = [log for log in self.live_logs 
                                   if prev_window_start <= log.timestamp < time_window_start]
                
                current_rate = len(window_logs)
                previous_rate = len(prev_window_logs)
                
                if previous_rate > 0:
                    rate_increase = ((current_rate - previous_rate) / previous_rate) * 100
                    return rate_increase > (rule.threshold_value or 0)
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error evaluating alert condition: {e}")
            return False
    
    def _trigger_alert(self, rule: AlertRule):
        """Trigger an alert"""
        try:
            alert_data = {
                "rule_id": rule.rule_id,
                "rule_name": rule.name,
                "description": rule.description,
                "severity": rule.severity,
                "condition": rule.condition.value,
                "threshold": rule.threshold_value,
                "timestamp": datetime.now().isoformat(),
                "notification_channels": rule.notification_channels
            }
            
            self.logger.warning(f"ALERT TRIGGERED: {rule.name} - {rule.description}")
            
            # In a real implementation, this would send notifications
            # to the specified channels (email, Slack, PagerDuty, etc.)
            
        except Exception as e:
            self.logger.error(f"Error triggering alert: {e}")


# Factory functions
def create_dashboard_manager(api_key: str, app_key: str) -> DataDogDashboardManager:
    """Create DataDog dashboard manager"""
    return DataDogDashboardManager(api_key, app_key)

def create_visualization_engine() -> LogVisualizationEngine:
    """Create log visualization engine"""
    return LogVisualizationEngine()

def create_realtime_monitor(update_interval_seconds: int = 30) -> RealTimeLogMonitor:
    """Create real-time log monitor"""
    return RealTimeLogMonitor(update_interval_seconds)


# Pre-built dashboard templates
def create_overview_dashboard() -> Dashboard:
    """Create overview dashboard"""
    widgets = [
        DashboardWidget(
            widget_id="log_volume",
            title="Log Volume",
            visualization_type=VisualizationType.TIME_SERIES,
            data_source="logs",
            query="sum:logs.count{*} by {service}"
        ),
        DashboardWidget(
            widget_id="error_rate",
            title="Error Rate",
            visualization_type=VisualizationType.TIME_SERIES,
            data_source="logs",
            query="sum:logs.error_rate{*} by {service}",
            thresholds={"warning": 5.0, "critical": 10.0}
        ),
        DashboardWidget(
            widget_id="response_time",
            title="Response Time P95",
            visualization_type=VisualizationType.TIME_SERIES,
            data_source="logs",
            query="p95:logs.response_time{*} by {service}"
        ),
        DashboardWidget(
            widget_id="service_status",
            title="Service Status",
            visualization_type=VisualizationType.TABLE,
            data_source="logs",
            query="avg:logs.success_rate{*} by {service}"
        )
    ]
    
    return Dashboard(
        dashboard_id=str(uuid.uuid4()),
        title="System Overview",
        description="High-level system health and performance metrics",
        dashboard_type=DashboardType.OVERVIEW,
        widgets=widgets,
        tags=["overview", "system", "health"]
    )


def create_security_dashboard() -> Dashboard:
    """Create security monitoring dashboard"""
    widgets = [
        DashboardWidget(
            widget_id="security_events",
            title="Security Events",
            visualization_type=VisualizationType.TIME_SERIES,
            data_source="logs",
            query="sum:security.events{*} by {event_type}"
        ),
        DashboardWidget(
            widget_id="failed_logins",
            title="Failed Login Attempts",
            visualization_type=VisualizationType.BAR_CHART,
            data_source="logs",
            query="sum:auth.failed_logins{*} by {ip_address}"
        ),
        DashboardWidget(
            widget_id="threat_levels",
            title="Threat Level Distribution",
            visualization_type=VisualizationType.PIE_CHART,
            data_source="logs",
            query="count:security.threats{*} by {risk_level}"
        )
    ]
    
    return Dashboard(
        dashboard_id=str(uuid.uuid4()),
        title="Security Monitoring",
        description="Security events and threat monitoring",
        dashboard_type=DashboardType.SECURITY,
        widgets=widgets,
        tags=["security", "threats", "authentication"]
    )


# Example usage
if __name__ == "__main__":
    # Create components
    dashboard_manager = create_dashboard_manager("dummy_api_key", "dummy_app_key")
    viz_engine = create_visualization_engine()
    realtime_monitor = create_realtime_monitor()
    
    # Create sample dashboard
    overview_dashboard = create_overview_dashboard()
    print(f"Created overview dashboard: {overview_dashboard.title}")
    print(f"Dashboard has {len(overview_dashboard.widgets)} widgets")
    
    # Create sample logs for visualization
    sample_logs = [
        LogEntry(
            timestamp=datetime.now() - timedelta(minutes=i),
            level=LogLevel.INFO,
            message=f"Sample log message {i}",
            source=LogSource.API,
            service="api-gateway",
            environment="production",
            duration_ms=200 + i * 10
        )
        for i in range(100)
    ]
    
    # Create visualizations
    log_volume_chart = viz_engine.create_log_volume_chart(sample_logs)
    error_rate_chart = viz_engine.create_error_rate_chart(sample_logs)
    
    print("Created sample visualizations")
    print("DataDog log visualization system ready")