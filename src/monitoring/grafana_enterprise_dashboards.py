"""
Enterprise Grafana Dashboard Configuration System
Provides comprehensive dashboard configurations for all monitoring aspects
with role-based access, automated provisioning, and real-time insights.
"""

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)


class DashboardType(Enum):
    """Types of monitoring dashboards"""

    EXECUTIVE = "executive"
    OPERATIONAL = "operational"
    TECHNICAL = "technical"
    BUSINESS = "business"
    SECURITY = "security"
    COMPLIANCE = "compliance"


class UserRole(Enum):
    """User roles for dashboard access"""

    EXECUTIVE = "executive"
    MANAGER = "manager"
    ENGINEER = "engineer"
    ANALYST = "analyst"
    VIEWER = "viewer"


class RefreshInterval(Enum):
    """Dashboard refresh intervals"""

    REAL_TIME = "5s"
    FREQUENT = "30s"
    STANDARD = "1m"
    SLOW = "5m"
    MANUAL = ""


@dataclass
class GrafanaPanel:
    """Grafana panel configuration"""

    id: int
    title: str
    type: str
    targets: list[dict[str, Any]]
    gridPos: dict[str, int]
    fieldConfig: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)
    transformations: list[dict[str, Any]] = field(default_factory=list)
    alert: dict[str, Any] | None = None


@dataclass
class GrafanaDashboard:
    """Complete Grafana dashboard configuration"""

    uid: str
    title: str
    description: str
    dashboard_type: DashboardType
    tags: list[str]
    panels: list[GrafanaPanel]
    refresh_interval: RefreshInterval = RefreshInterval.STANDARD
    time_range: str = "now-1h"
    auto_refresh: bool = True
    editable: bool = False
    templating: dict[str, Any] = field(default_factory=dict)
    annotations: list[dict[str, Any]] = field(default_factory=list)
    links: list[dict[str, Any]] = field(default_factory=list)
    variables: list[dict[str, Any]] = field(default_factory=list)


class GrafanaEnterpriseConfigurator:
    """Enterprise Grafana dashboard configurator with comprehensive monitoring coverage"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.dashboards: dict[str, GrafanaDashboard] = {}

        # Initialize all dashboard configurations
        self._initialize_executive_dashboards()
        self._initialize_operational_dashboards()
        self._initialize_technical_dashboards()
        self._initialize_business_dashboards()
        self._initialize_security_dashboards()
        self._initialize_compliance_dashboards()

        self.logger.info(f"Initialized {len(self.dashboards)} enterprise Grafana dashboards")

    def _initialize_executive_dashboards(self):
        """Initialize executive-level dashboards"""

        # Executive Overview Dashboard
        executive_panels = [
            self._create_single_stat_panel(
                id=1,
                title="System Health Score",
                targets=[{"expr": "system_health_score", "legendFormat": "Health Score"}],
                gridPos={"h": 6, "w": 6, "x": 0, "y": 0},
                unit="percent",
                thresholds=[80, 95],
                colors=["red", "yellow", "green"],
            ),
            self._create_single_stat_panel(
                id=2,
                title="SLA Compliance",
                targets=[{"expr": "avg(sla_compliance_score)", "legendFormat": "SLA Compliance"}],
                gridPos={"h": 6, "w": 6, "x": 6, "y": 0},
                unit="percent",
                thresholds=[95, 99],
                colors=["red", "yellow", "green"],
            ),
            self._create_single_stat_panel(
                id=3,
                title="Active Incidents",
                targets=[{"expr": "sum(active_incidents)", "legendFormat": "Incidents"}],
                gridPos={"h": 6, "w": 6, "x": 12, "y": 0},
                unit="short",
                thresholds=[1, 5],
                colors=["green", "yellow", "red"],
            ),
            self._create_single_stat_panel(
                id=4,
                title="Monthly Revenue Impact",
                targets=[{"expr": "sum(revenue_impact_usd)", "legendFormat": "Revenue Impact"}],
                gridPos={"h": 6, "w": 6, "x": 18, "y": 0},
                unit="currencyUSD",
                thresholds=[10000, 50000],
                colors=["green", "yellow", "red"],
            ),
            self._create_time_series_panel(
                id=5,
                title="Business Metrics Trend",
                targets=[
                    {"expr": "rate(total_transactions[5m])", "legendFormat": "Transaction Rate"},
                    {
                        "expr": "avg(customer_satisfaction_score)",
                        "legendFormat": "Customer Satisfaction",
                    },
                    {"expr": "rate(revenue_usd[1h])", "legendFormat": "Revenue Rate"},
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 6},
            ),
            self._create_bar_chart_panel(
                id=6,
                title="Service Performance Summary",
                targets=[
                    {
                        "expr": "avg_over_time(api_response_time_p95[1h])",
                        "legendFormat": "{{service}} Response Time",
                    },
                    {
                        "expr": "rate(service_errors_total[1h])",
                        "legendFormat": "{{service}} Error Rate",
                    },
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 6},
            ),
            self._create_heatmap_panel(
                id=7,
                title="System Load Distribution",
                targets=[{"expr": "system_load_buckets", "legendFormat": "Load"}],
                gridPos={"h": 8, "w": 24, "x": 0, "y": 14},
            ),
            self._create_table_panel(
                id=8,
                title="Critical Alerts Summary",
                targets=[{"expr": "topk(10, critical_alerts)", "legendFormat": "Alert"}],
                gridPos={"h": 6, "w": 24, "x": 0, "y": 22},
                columns=["Time", "Service", "Alert", "Severity", "Impact", "Status"],
            ),
        ]

        executive_dashboard = GrafanaDashboard(
            uid="executive-overview",
            title="Executive Overview - System Health & Business Impact",
            description="High-level executive dashboard showing system health, business impact, and key performance indicators",
            dashboard_type=DashboardType.EXECUTIVE,
            tags=["executive", "overview", "business", "health"],
            panels=executive_panels,
            refresh_interval=RefreshInterval.FREQUENT,
            time_range="now-24h",
            variables=[
                self._create_variable(
                    "environment", "Environment", "query", "label_values(environment)"
                ),
                self._create_variable("service", "Service", "query", "label_values(service)"),
            ],
            links=[
                {"title": "Operational Dashboard", "url": "/d/operational-overview"},
                {"title": "Incident Management", "url": "/d/incident-management"},
            ],
        )

        self.dashboards["executive-overview"] = executive_dashboard

        # Executive SLA Dashboard
        sla_panels = [
            self._create_gauge_panel(
                id=1,
                title="Overall SLA Compliance",
                targets=[{"expr": "avg(sla_compliance_percentage)", "legendFormat": "Compliance"}],
                gridPos={"h": 8, "w": 8, "x": 0, "y": 0},
                min_value=95,
                max_value=100,
                thresholds=[98, 99.5],
            ),
            self._create_time_series_panel(
                id=2,
                title="SLA Trends by Service",
                targets=[
                    {
                        "expr": "sla_compliance_percentage by (service)",
                        "legendFormat": "{{service}}",
                    }
                ],
                gridPos={"h": 8, "w": 16, "x": 8, "y": 0},
            ),
            self._create_table_panel(
                id=3,
                title="SLA Violations",
                targets=[{"expr": "sla_violations", "legendFormat": "Violation"}],
                gridPos={"h": 8, "w": 24, "x": 0, "y": 8},
                columns=["Time", "Service", "SLA Type", "Target", "Actual", "Impact", "Duration"],
            ),
        ]

        self.dashboards["executive-sla"] = GrafanaDashboard(
            uid="executive-sla",
            title="Executive SLA Dashboard",
            description="Executive view of SLA compliance and violations",
            dashboard_type=DashboardType.EXECUTIVE,
            tags=["executive", "sla", "compliance"],
            panels=sla_panels,
            refresh_interval=RefreshInterval.STANDARD,
        )

    def _initialize_operational_dashboards(self):
        """Initialize operational dashboards"""

        # System Operations Dashboard
        ops_panels = [
            self._create_single_stat_panel(
                id=1,
                title="System Uptime",
                targets=[{"expr": "avg(up)", "legendFormat": "Uptime"}],
                gridPos={"h": 4, "w": 6, "x": 0, "y": 0},
                unit="percent",
                thresholds=[95, 99],
            ),
            self._create_single_stat_panel(
                id=2,
                title="Active Services",
                targets=[{"expr": "count(up == 1)", "legendFormat": "Active"}],
                gridPos={"h": 4, "w": 6, "x": 6, "y": 0},
                unit="short",
            ),
            self._create_single_stat_panel(
                id=3,
                title="Avg Response Time",
                targets=[
                    {"expr": "avg(http_request_duration_seconds)", "legendFormat": "Response Time"}
                ],
                gridPos={"h": 4, "w": 6, "x": 12, "y": 0},
                unit="ms",
                thresholds=[200, 500],
            ),
            self._create_single_stat_panel(
                id=4,
                title="Error Rate",
                targets=[
                    {
                        "expr": 'rate(http_requests_total{status=~"5.."}[5m])',
                        "legendFormat": "Error Rate",
                    }
                ],
                gridPos={"h": 4, "w": 6, "x": 18, "y": 0},
                unit="percent",
                thresholds=[1, 5],
            ),
            self._create_time_series_panel(
                id=5,
                title="Request Rate",
                targets=[
                    {
                        "expr": "rate(http_requests_total[5m])",
                        "legendFormat": "{{service}} - {{method}}",
                    }
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 4},
            ),
            self._create_time_series_panel(
                id=6,
                title="Response Time Distribution",
                targets=[
                    {
                        "expr": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
                        "legendFormat": "95th percentile",
                    }
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 4},
            ),
            self._create_alert_list_panel(
                id=7, title="Active Alerts", gridPos={"h": 8, "w": 24, "x": 0, "y": 12}
            ),
        ]

        self.dashboards["operational-overview"] = GrafanaDashboard(
            uid="operational-overview",
            title="Operational Overview",
            description="Real-time operational monitoring and alerting dashboard",
            dashboard_type=DashboardType.OPERATIONAL,
            tags=["operational", "monitoring", "alerts"],
            panels=ops_panels,
            refresh_interval=RefreshInterval.REAL_TIME,
        )

        # Infrastructure Monitoring Dashboard
        infra_panels = [
            self._create_time_series_panel(
                id=1,
                title="CPU Usage",
                targets=[
                    {
                        "expr": '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
                        "legendFormat": "{{instance}}",
                    }
                ],
                gridPos={"h": 8, "w": 8, "x": 0, "y": 0},
            ),
            self._create_time_series_panel(
                id=2,
                title="Memory Usage",
                targets=[
                    {
                        "expr": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",
                        "legendFormat": "{{instance}}",
                    }
                ],
                gridPos={"h": 8, "w": 8, "x": 8, "y": 0},
            ),
            self._create_time_series_panel(
                id=3,
                title="Disk Usage",
                targets=[
                    {
                        "expr": "100 - ((node_filesystem_avail_bytes / node_filesystem_size_bytes) * 100)",
                        "legendFormat": "{{instance}} - {{mountpoint}}",
                    }
                ],
                gridPos={"h": 8, "w": 8, "x": 16, "y": 0},
            ),
            self._create_time_series_panel(
                id=4,
                title="Network I/O",
                targets=[
                    {
                        "expr": "rate(node_network_receive_bytes_total[5m])",
                        "legendFormat": "{{instance}} RX",
                    },
                    {
                        "expr": "rate(node_network_transmit_bytes_total[5m])",
                        "legendFormat": "{{instance}} TX",
                    },
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 8},
            ),
            self._create_time_series_panel(
                id=5,
                title="Load Average",
                targets=[{"expr": "node_load1", "legendFormat": "{{instance}} 1m"}],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 8},
            ),
        ]

        self.dashboards["infrastructure-monitoring"] = GrafanaDashboard(
            uid="infrastructure-monitoring",
            title="Infrastructure Monitoring",
            description="System infrastructure monitoring including CPU, memory, disk, and network",
            dashboard_type=DashboardType.OPERATIONAL,
            tags=["infrastructure", "system", "resources"],
            panels=infra_panels,
            refresh_interval=RefreshInterval.FREQUENT,
        )

    def _initialize_technical_dashboards(self):
        """Initialize technical dashboards for engineers"""

        # Application Performance Monitoring
        apm_panels = [
            self._create_time_series_panel(
                id=1,
                title="Application Response Times",
                targets=[
                    {
                        "expr": "histogram_quantile(0.50, rate(http_request_duration_seconds_bucket[5m]))",
                        "legendFormat": "50th percentile",
                    },
                    {
                        "expr": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
                        "legendFormat": "95th percentile",
                    },
                    {
                        "expr": "histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
                        "legendFormat": "99th percentile",
                    },
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 0},
            ),
            self._create_time_series_panel(
                id=2,
                title="Database Query Performance",
                targets=[
                    {
                        "expr": "avg(db_query_duration_seconds) by (query_type)",
                        "legendFormat": "{{query_type}}",
                    },
                    {"expr": "rate(db_query_errors_total[5m])", "legendFormat": "Query Errors"},
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 0},
            ),
            self._create_time_series_panel(
                id=3,
                title="Cache Performance",
                targets=[
                    {"expr": "rate(cache_hits_total[5m])", "legendFormat": "Cache Hits"},
                    {"expr": "rate(cache_misses_total[5m])", "legendFormat": "Cache Misses"},
                    {"expr": "cache_hit_ratio", "legendFormat": "Hit Ratio"},
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 8},
            ),
            self._create_time_series_panel(
                id=4,
                title="Error Rates by Service",
                targets=[
                    {
                        "expr": 'rate(http_requests_total{status=~"[45].."}[5m]) by (service)',
                        "legendFormat": "{{service}}",
                    }
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 8},
            ),
            self._create_logs_panel(
                id=5,
                title="Recent Error Logs",
                targets=[{"expr": '{level="error"}', "legendFormat": ""}],
                gridPos={"h": 8, "w": 24, "x": 0, "y": 16},
            ),
        ]

        self.dashboards["application-performance"] = GrafanaDashboard(
            uid="application-performance",
            title="Application Performance Monitoring",
            description="Detailed application performance metrics and error tracking",
            dashboard_type=DashboardType.TECHNICAL,
            tags=["apm", "performance", "errors"],
            panels=apm_panels,
            refresh_interval=RefreshInterval.FREQUENT,
        )

        # Database Monitoring Dashboard
        db_panels = [
            self._create_time_series_panel(
                id=1,
                title="Database Connections",
                targets=[
                    {"expr": "db_connections_active", "legendFormat": "Active Connections"},
                    {"expr": "db_connections_total", "legendFormat": "Total Connections"},
                    {"expr": "db_connections_max", "legendFormat": "Max Connections"},
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 0},
            ),
            self._create_time_series_panel(
                id=2,
                title="Query Performance",
                targets=[
                    {"expr": "avg(db_query_duration_seconds)", "legendFormat": "Avg Query Time"},
                    {"expr": "rate(db_queries_total[5m])", "legendFormat": "Query Rate"},
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 0},
            ),
            self._create_time_series_panel(
                id=3,
                title="Database Size",
                targets=[{"expr": "db_size_bytes by (database)", "legendFormat": "{{database}}"}],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 8},
            ),
            self._create_time_series_panel(
                id=4,
                title="Slow Queries",
                targets=[
                    {"expr": "rate(db_slow_queries_total[5m])", "legendFormat": "Slow Queries"}
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 8},
            ),
        ]

        self.dashboards["database-monitoring"] = GrafanaDashboard(
            uid="database-monitoring",
            title="Database Monitoring",
            description="Comprehensive database performance and health monitoring",
            dashboard_type=DashboardType.TECHNICAL,
            tags=["database", "performance", "sql"],
            panels=db_panels,
            refresh_interval=RefreshInterval.STANDARD,
        )

    def _initialize_business_dashboards(self):
        """Initialize business-focused dashboards"""

        # Business Metrics Dashboard
        business_panels = [
            self._create_single_stat_panel(
                id=1,
                title="Daily Active Users",
                targets=[{"expr": "daily_active_users", "legendFormat": "DAU"}],
                gridPos={"h": 6, "w": 6, "x": 0, "y": 0},
                unit="short",
            ),
            self._create_single_stat_panel(
                id=2,
                title="Total Revenue",
                targets=[{"expr": "sum(revenue_usd)", "legendFormat": "Revenue"}],
                gridPos={"h": 6, "w": 6, "x": 6, "y": 0},
                unit="currencyUSD",
            ),
            self._create_single_stat_panel(
                id=3,
                title="Conversion Rate",
                targets=[{"expr": "conversion_rate_percentage", "legendFormat": "Conversion"}],
                gridPos={"h": 6, "w": 6, "x": 12, "y": 0},
                unit="percent",
            ),
            self._create_single_stat_panel(
                id=4,
                title="Customer Satisfaction",
                targets=[{"expr": "avg(customer_satisfaction_score)", "legendFormat": "CSAT"}],
                gridPos={"h": 6, "w": 6, "x": 18, "y": 0},
                unit="percent",
            ),
            self._create_time_series_panel(
                id=5,
                title="User Engagement Trends",
                targets=[
                    {"expr": "rate(page_views_total[1h])", "legendFormat": "Page Views"},
                    {"expr": "rate(user_sessions_total[1h])", "legendFormat": "Sessions"},
                    {"expr": "avg(session_duration_seconds)", "legendFormat": "Session Duration"},
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 6},
            ),
            self._create_time_series_panel(
                id=6,
                title="Revenue by Channel",
                targets=[
                    {"expr": "rate(revenue_usd[1h]) by (channel)", "legendFormat": "{{channel}}"}
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 6},
            ),
            self._create_pie_chart_panel(
                id=7,
                title="Traffic Sources",
                targets=[
                    {"expr": "sum(user_sessions_total) by (source)", "legendFormat": "{{source}}"}
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 14},
            ),
            self._create_table_panel(
                id=8,
                title="Top Products",
                targets=[
                    {
                        "expr": "topk(10, sum(product_purchases) by (product))",
                        "legendFormat": "{{product}}",
                    }
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 14},
                columns=["Product", "Purchases", "Revenue", "Conversion Rate"],
            ),
        ]

        self.dashboards["business-metrics"] = GrafanaDashboard(
            uid="business-metrics",
            title="Business Metrics Dashboard",
            description="Key business metrics including user engagement, revenue, and conversion rates",
            dashboard_type=DashboardType.BUSINESS,
            tags=["business", "kpi", "revenue", "users"],
            panels=business_panels,
            refresh_interval=RefreshInterval.STANDARD,
            time_range="now-7d",
        )

    def _initialize_security_dashboards(self):
        """Initialize security monitoring dashboards"""

        # Security Overview Dashboard
        security_panels = [
            self._create_single_stat_panel(
                id=1,
                title="Security Incidents",
                targets=[{"expr": "sum(security_incidents_total)", "legendFormat": "Incidents"}],
                gridPos={"h": 6, "w": 6, "x": 0, "y": 0},
                unit="short",
                thresholds=[1, 5],
                colors=["green", "yellow", "red"],
            ),
            self._create_single_stat_panel(
                id=2,
                title="Failed Login Attempts",
                targets=[
                    {
                        "expr": "rate(failed_login_attempts_total[1h])",
                        "legendFormat": "Failed Logins",
                    }
                ],
                gridPos={"h": 6, "w": 6, "x": 6, "y": 0},
                unit="reqps",
            ),
            self._create_single_stat_panel(
                id=3,
                title="Blocked Requests",
                targets=[{"expr": "rate(blocked_requests_total[1h])", "legendFormat": "Blocked"}],
                gridPos={"h": 6, "w": 6, "x": 12, "y": 0},
                unit="reqps",
            ),
            self._create_single_stat_panel(
                id=4,
                title="Vulnerability Score",
                targets=[{"expr": "security_vulnerability_score", "legendFormat": "Score"}],
                gridPos={"h": 6, "w": 6, "x": 18, "y": 0},
                unit="short",
                thresholds=[5, 8],
                colors=["green", "yellow", "red"],
            ),
            self._create_time_series_panel(
                id=5,
                title="Authentication Events",
                targets=[
                    {
                        "expr": "rate(successful_logins_total[5m])",
                        "legendFormat": "Successful Logins",
                    },
                    {"expr": "rate(failed_logins_total[5m])", "legendFormat": "Failed Logins"},
                    {
                        "expr": "rate(suspicious_activity_total[5m])",
                        "legendFormat": "Suspicious Activity",
                    },
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 6},
            ),
            self._create_world_map_panel(
                id=6,
                title="Attack Origins",
                targets=[
                    {
                        "expr": "sum(attack_attempts_total) by (country)",
                        "legendFormat": "{{country}}",
                    }
                ],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 6},
            ),
            self._create_table_panel(
                id=7,
                title="Top Threats",
                targets=[
                    {
                        "expr": "topk(10, sum(threat_events_total) by (threat_type))",
                        "legendFormat": "{{threat_type}}",
                    }
                ],
                gridPos={"h": 8, "w": 24, "x": 0, "y": 14},
                columns=["Threat Type", "Count", "Severity", "Source", "Last Seen"],
            ),
        ]

        self.dashboards["security-overview"] = GrafanaDashboard(
            uid="security-overview",
            title="Security Monitoring Dashboard",
            description="Comprehensive security monitoring including threats, vulnerabilities, and incidents",
            dashboard_type=DashboardType.SECURITY,
            tags=["security", "threats", "vulnerabilities"],
            panels=security_panels,
            refresh_interval=RefreshInterval.FREQUENT,
        )

    def _initialize_compliance_dashboards(self):
        """Initialize compliance monitoring dashboards"""

        # Compliance Overview Dashboard
        compliance_panels = [
            self._create_gauge_panel(
                id=1,
                title="GDPR Compliance Score",
                targets=[{"expr": "gdpr_compliance_score", "legendFormat": "GDPR"}],
                gridPos={"h": 8, "w": 8, "x": 0, "y": 0},
                min_value=80,
                max_value=100,
                thresholds=[90, 95],
            ),
            self._create_gauge_panel(
                id=2,
                title="SOX Compliance Score",
                targets=[{"expr": "sox_compliance_score", "legendFormat": "SOX"}],
                gridPos={"h": 8, "w": 8, "x": 8, "y": 0},
                min_value=80,
                max_value=100,
                thresholds=[95, 98],
            ),
            self._create_gauge_panel(
                id=3,
                title="PCI DSS Compliance",
                targets=[{"expr": "pci_compliance_score", "legendFormat": "PCI DSS"}],
                gridPos={"h": 8, "w": 8, "x": 16, "y": 0},
                min_value=80,
                max_value=100,
                thresholds=[95, 99],
            ),
            self._create_table_panel(
                id=4,
                title="Compliance Violations",
                targets=[{"expr": "compliance_violations", "legendFormat": "Violation"}],
                gridPos={"h": 8, "w": 24, "x": 0, "y": 8},
                columns=[
                    "Time",
                    "Regulation",
                    "Violation Type",
                    "Severity",
                    "Status",
                    "Remediation",
                ],
            ),
            self._create_time_series_panel(
                id=5,
                title="Audit Trail Activity",
                targets=[
                    {
                        "expr": "rate(audit_events_total[1h]) by (event_type)",
                        "legendFormat": "{{event_type}}",
                    }
                ],
                gridPos={"h": 8, "w": 12, "x": 0, "y": 16},
            ),
            self._create_heatmap_panel(
                id=6,
                title="Data Access Patterns",
                targets=[{"expr": "data_access_frequency", "legendFormat": "Access"}],
                gridPos={"h": 8, "w": 12, "x": 12, "y": 16},
            ),
        ]

        self.dashboards["compliance-monitoring"] = GrafanaDashboard(
            uid="compliance-monitoring",
            title="Compliance Monitoring Dashboard",
            description="Regulatory compliance monitoring and audit trail visualization",
            dashboard_type=DashboardType.COMPLIANCE,
            tags=["compliance", "audit", "regulations"],
            panels=compliance_panels,
            refresh_interval=RefreshInterval.STANDARD,
        )

    # Panel creation helper methods
    def _create_single_stat_panel(
        self,
        id: int,
        title: str,
        targets: list[dict],
        gridPos: dict,
        unit: str = "short",
        thresholds: list[float] = None,
        colors: list[str] = None,
    ) -> GrafanaPanel:
        """Create a single stat panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="stat",
            targets=targets,
            gridPos=gridPos,
            fieldConfig={
                "defaults": {
                    "unit": unit,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": colors[0] if colors else "green", "value": None},
                            {
                                "color": colors[1] if colors and len(colors) > 1 else "yellow",
                                "value": thresholds[0] if thresholds else 50,
                            },
                            {
                                "color": colors[2] if colors and len(colors) > 2 else "red",
                                "value": thresholds[1]
                                if thresholds and len(thresholds) > 1
                                else 80,
                            },
                        ],
                    },
                }
            },
            options={
                "reduceOptions": {"values": False, "calcs": ["lastNotNull"]},
                "orientation": "auto",
                "textMode": "auto",
                "colorMode": "value",
            },
        )

    def _create_time_series_panel(
        self, id: int, title: str, targets: list[dict], gridPos: dict, unit: str = "short"
    ) -> GrafanaPanel:
        """Create a time series panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="timeseries",
            targets=targets,
            gridPos=gridPos,
            fieldConfig={
                "defaults": {
                    "unit": unit,
                    "custom": {
                        "drawStyle": "line",
                        "lineInterpolation": "linear",
                        "barAlignment": 0,
                        "lineWidth": 1,
                        "fillOpacity": 10,
                        "gradientMode": "none",
                        "spanNulls": False,
                        "insertNulls": False,
                        "showPoints": "never",
                        "pointSize": 5,
                    },
                }
            },
            options={
                "tooltip": {"mode": "single", "sort": "none"},
                "legend": {"displayMode": "list", "placement": "bottom", "calcs": []},
                "displayMode": "line",
            },
        )

    def _create_gauge_panel(
        self,
        id: int,
        title: str,
        targets: list[dict],
        gridPos: dict,
        min_value: float = 0,
        max_value: float = 100,
        thresholds: list[float] = None,
    ) -> GrafanaPanel:
        """Create a gauge panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="gauge",
            targets=targets,
            gridPos=gridPos,
            fieldConfig={
                "defaults": {
                    "min": min_value,
                    "max": max_value,
                    "unit": "percent",
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "yellow", "value": thresholds[0] if thresholds else 80},
                            {
                                "color": "green",
                                "value": thresholds[1]
                                if thresholds and len(thresholds) > 1
                                else 90,
                            },
                        ],
                    },
                }
            },
            options={
                "reduceOptions": {"values": False, "calcs": ["lastNotNull"]},
                "orientation": "auto",
                "showThresholdLabels": False,
                "showThresholdMarkers": True,
            },
        )

    def _create_table_panel(
        self, id: int, title: str, targets: list[dict], gridPos: dict, columns: list[str] = None
    ) -> GrafanaPanel:
        """Create a table panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="table",
            targets=targets,
            gridPos=gridPos,
            options={"showHeader": True, "columns": columns or []},
            transformations=[
                {
                    "id": "organize",
                    "options": {"excludeByName": {}, "indexByName": {}, "renameByName": {}},
                }
            ],
        )

    def _create_bar_chart_panel(
        self, id: int, title: str, targets: list[dict], gridPos: dict
    ) -> GrafanaPanel:
        """Create a bar chart panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="barchart",
            targets=targets,
            gridPos=gridPos,
            options={
                "orientation": "horizontal",
                "xField": "Time",
                "displayMode": "basic",
                "legend": {"displayMode": "visible", "placement": "right"},
            },
        )

    def _create_pie_chart_panel(
        self, id: int, title: str, targets: list[dict], gridPos: dict
    ) -> GrafanaPanel:
        """Create a pie chart panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="piechart",
            targets=targets,
            gridPos=gridPos,
            options={
                "reduceOptions": {"values": False, "calcs": ["lastNotNull"]},
                "pieType": "pie",
                "tooltip": {"mode": "single", "sort": "none"},
                "legend": {"displayMode": "list", "placement": "right", "calcs": []},
                "displayLabels": ["name", "percent"],
            },
        )

    def _create_heatmap_panel(
        self, id: int, title: str, targets: list[dict], gridPos: dict
    ) -> GrafanaPanel:
        """Create a heatmap panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="heatmap",
            targets=targets,
            gridPos=gridPos,
            options={
                "calculate": True,
                "calculation": {"xBuckets": {"mode": "count", "value": "20"}},
                "cellGap": 1,
                "color": {"mode": "spectrum"},
                "exponent": 0.5,
                "hideZeroBuckets": False,
            },
        )

    def _create_logs_panel(
        self, id: int, title: str, targets: list[dict], gridPos: dict
    ) -> GrafanaPanel:
        """Create a logs panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="logs",
            targets=targets,
            gridPos=gridPos,
            options={
                "showTime": True,
                "showLabels": True,
                "showCommonLabels": False,
                "wrapLogMessage": True,
                "prettifyLogMessage": False,
                "enableLogDetails": True,
                "dedupStrategy": "none",
                "sortOrder": "Descending",
            },
        )

    def _create_alert_list_panel(self, id: int, title: str, gridPos: dict) -> GrafanaPanel:
        """Create an alert list panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="alertlist",
            targets=[],
            gridPos=gridPos,
            options={
                "showOptions": "current",
                "maxItems": 20,
                "sortOrder": 1,
                "dashboardAlerts": False,
                "alertName": "",
                "dashboardTitle": "",
                "folderId": None,
                "tags": [],
            },
        )

    def _create_world_map_panel(
        self, id: int, title: str, targets: list[dict], gridPos: dict
    ) -> GrafanaPanel:
        """Create a world map panel"""
        return GrafanaPanel(
            id=id,
            title=title,
            type="geomap",
            targets=targets,
            gridPos=gridPos,
            options={
                "view": {"id": "coords", "lat": 0, "lon": 0, "zoom": 2},
                "controls": {"mouseWheelZoom": True, "showZoom": True, "showAttribution": True},
                "layers": [
                    {
                        "type": "markers",
                        "name": "Markers",
                        "config": {
                            "style": {
                                "size": {"fixed": 5, "min": 2, "max": 15},
                                "color": {"fixed": "dark-green"},
                                "fillOpacity": 0.4,
                                "strokeWidth": 1,
                                "strokeColor": "green",
                            }
                        },
                    }
                ],
            },
        )

    def _create_variable(self, name: str, label: str, type: str, query: str) -> dict[str, Any]:
        """Create a dashboard variable"""
        return {
            "name": name,
            "label": label,
            "type": type,
            "query": query,
            "refresh": 1,
            "regex": "",
            "sort": 0,
            "multi": True,
            "includeAll": True,
            "allValue": None,
            "datasource": "prometheus",
        }

    def get_dashboard_json(self, dashboard_uid: str) -> dict[str, Any]:
        """Get complete Grafana dashboard JSON"""
        if dashboard_uid not in self.dashboards:
            raise ValueError(f"Dashboard {dashboard_uid} not found")

        dashboard = self.dashboards[dashboard_uid]

        # Convert dashboard to Grafana JSON format
        dashboard_json = {
            "id": None,
            "uid": dashboard.uid,
            "title": dashboard.title,
            "description": dashboard.description,
            "tags": dashboard.tags,
            "style": "dark",
            "timezone": "browser",
            "editable": dashboard.editable,
            "hideControls": False,
            "graphTooltip": 1,
            "time": {
                "from": dashboard.time_range.split("now-")[1]
                if "now-" in dashboard.time_range
                else "now-1h",
                "to": "now",
            },
            "timepicker": {
                "refresh_intervals": [
                    "5s",
                    "10s",
                    "30s",
                    "1m",
                    "5m",
                    "15m",
                    "30m",
                    "1h",
                    "2h",
                    "1d",
                ],
                "time_options": ["5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"],
            },
            "refresh": dashboard.refresh_interval.value,
            "schemaVersion": 30,
            "version": 1,
            "panels": [self._panel_to_json(panel) for panel in dashboard.panels],
            "templating": {"list": dashboard.variables},
            "annotations": {"list": dashboard.annotations},
            "links": dashboard.links,
        }

        return dashboard_json

    def _panel_to_json(self, panel: GrafanaPanel) -> dict[str, Any]:
        """Convert panel to Grafana JSON format"""
        panel_json = {
            "id": panel.id,
            "title": panel.title,
            "type": panel.type,
            "targets": panel.targets,
            "gridPos": panel.gridPos,
            "fieldConfig": panel.fieldConfig,
            "options": panel.options,
            "transformations": panel.transformations,
        }

        if panel.alert:
            panel_json["alert"] = panel.alert

        return panel_json

    def export_all_dashboards(self, output_dir: str = "dashboards") -> dict[str, str]:
        """Export all dashboards to JSON files"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        exported_files = {}

        for dashboard_uid, dashboard in self.dashboards.items():
            try:
                dashboard_json = self.get_dashboard_json(dashboard_uid)

                filename = f"{dashboard_uid}.json"
                filepath = output_path / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(dashboard_json, f, indent=2, ensure_ascii=False)

                exported_files[dashboard_uid] = str(filepath)
                self.logger.info(f"Exported dashboard: {dashboard.title} -> {filepath}")

            except Exception as e:
                self.logger.error(f"Failed to export dashboard {dashboard_uid}: {str(e)}")

        self.logger.info(f"Exported {len(exported_files)} Grafana dashboards to {output_path}")
        return exported_files

    def get_dashboard_list(self) -> list[dict[str, Any]]:
        """Get list of all available dashboards"""
        return [
            {
                "uid": dashboard.uid,
                "title": dashboard.title,
                "description": dashboard.description,
                "type": dashboard.dashboard_type.value,
                "tags": dashboard.tags,
                "refresh_interval": dashboard.refresh_interval.value,
                "panel_count": len(dashboard.panels),
                "time_range": dashboard.time_range,
            }
            for dashboard in self.dashboards.values()
        ]

    def get_dashboards_by_role(self, role: UserRole) -> list[str]:
        """Get dashboard UIDs accessible by user role"""
        role_dashboard_mapping = {
            UserRole.EXECUTIVE: ["executive-overview", "executive-sla", "business-metrics"],
            UserRole.MANAGER: [
                "operational-overview",
                "business-metrics",
                "compliance-monitoring",
                "infrastructure-monitoring",
            ],
            UserRole.ENGINEER: [
                "application-performance",
                "database-monitoring",
                "infrastructure-monitoring",
                "operational-overview",
            ],
            UserRole.ANALYST: ["business-metrics", "compliance-monitoring", "security-overview"],
            UserRole.VIEWER: ["operational-overview", "business-metrics"],
        }

        return role_dashboard_mapping.get(role, [])


# Global configurator instance
_grafana_configurator: GrafanaEnterpriseConfigurator | None = None


def get_grafana_configurator() -> GrafanaEnterpriseConfigurator:
    """Get global Grafana configurator instance"""
    global _grafana_configurator
    if _grafana_configurator is None:
        _grafana_configurator = GrafanaEnterpriseConfigurator()
    return _grafana_configurator


# Convenience functions
def export_grafana_dashboards(output_dir: str = "grafana_dashboards") -> dict[str, str]:
    """Export all Grafana dashboards"""
    configurator = get_grafana_configurator()
    return configurator.export_all_dashboards(output_dir)


def get_dashboard_json(dashboard_uid: str) -> dict[str, Any]:
    """Get specific dashboard JSON"""
    configurator = get_grafana_configurator()
    return configurator.get_dashboard_json(dashboard_uid)


def get_available_dashboards() -> list[dict[str, Any]]:
    """Get list of available dashboards"""
    configurator = get_grafana_configurator()
    return configurator.get_dashboard_list()


if __name__ == "__main__":
    # Export all dashboards for testing
    configurator = GrafanaEnterpriseConfigurator()

    # Export all dashboards
    exported = configurator.export_all_dashboards("test_dashboards")
    print(f"Exported {len(exported)} dashboards:")
    for uid, path in exported.items():
        print(f"  {uid} -> {path}")

    # List available dashboards
    dashboards = configurator.get_dashboard_list()
    print(f"\nAvailable dashboards: {len(dashboards)}")
    for dashboard in dashboards:
        print(f"  {dashboard['title']} ({dashboard['type']}) - {dashboard['panel_count']} panels")
