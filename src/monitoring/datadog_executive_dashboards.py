"""
Comprehensive DataDog Executive Dashboards
Provides executive-level business intelligence dashboards with real-time KPIs, 
system health overviews, SLA tracking, and business impact visualization
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_dashboard_integration import DataDogDashboardManager
from monitoring.datadog_business_metrics import DataDogBusinessMetricsTracker, KPICategory

logger = get_logger(__name__)


class ExecutiveDashboardType(Enum):
    """Types of executive dashboards"""
    CEO_OVERVIEW = "ceo_overview"
    CFO_FINANCIAL = "cfo_financial"
    CTO_TECHNICAL = "cto_technical"
    COO_OPERATIONS = "coo_operations"
    BOARD_SUMMARY = "board_summary"
    DATA_STRATEGY = "data_strategy"


class MetricTier(Enum):
    """Priority tiers for executive metrics"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    INFORMATIONAL = "informational"


@dataclass
class ExecutiveKPI:
    """Executive-level KPI definition"""
    name: str
    title: str
    description: str
    category: KPICategory
    tier: MetricTier
    target_value: Optional[float] = None
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None
    unit: str = "count"
    format_type: str = "number"  # number, percentage, currency, duration
    trend_analysis: bool = True
    board_visible: bool = False


@dataclass
class SLADefinition:
    """Service Level Agreement definition"""
    name: str
    description: str
    target_percentage: float
    measurement_window: str  # hourly, daily, weekly, monthly
    critical_threshold: float
    warning_threshold: float
    penalty_cost: Optional[float] = None
    business_impact: str = "high"


class DataDogExecutiveDashboards:
    """
    Executive-level DataDog dashboards for C-suite and board reporting
    
    Features:
    - CEO overview dashboard with key business metrics
    - CFO financial dashboard with revenue and cost optimization
    - CTO technical dashboard with system health and innovation metrics
    - COO operations dashboard with efficiency and performance metrics
    - Board summary dashboard with strategic KPIs
    - Data strategy dashboard with data-driven insights
    - Real-time SLA monitoring and compliance tracking
    - ROI and business impact visualization
    - Automated executive reports and insights
    """
    
    def __init__(self, service_name: str = "executive-dashboards",
                 datadog_monitoring: Optional[DatadogMonitoring] = None,
                 dashboard_manager: Optional[DataDogDashboardManager] = None,
                 business_metrics: Optional[DataDogBusinessMetricsTracker] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.dashboard_manager = dashboard_manager
        self.business_metrics = business_metrics
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Executive KPI registry
        self.executive_kpis: Dict[str, ExecutiveKPI] = {}
        self.sla_definitions: Dict[str, SLADefinition] = {}
        self.dashboard_definitions: Dict[ExecutiveDashboardType, Dict[str, Any]] = {}
        
        # Performance tracking
        self.dashboard_access_count = 0
        self.last_refresh_time = datetime.utcnow()
        self.alert_counts = {"critical": 0, "warning": 0, "info": 0}
        
        # Initialize executive KPIs and dashboards
        self._initialize_executive_kpis()
        self._initialize_sla_definitions()
        self._initialize_dashboard_definitions()
        
        self.logger.info(f"Executive dashboards initialized for {service_name}")
    
    def _initialize_executive_kpis(self):
        """Initialize executive-level KPIs"""
        
        executive_kpis = [
            # CEO Overview KPIs
            ExecutiveKPI(
                name="total_revenue",
                title="Total Revenue",
                description="Total revenue generated across all channels",
                category=KPICategory.FINANCIAL,
                tier=MetricTier.CRITICAL,
                target_value=1000000.0,
                warning_threshold=800000.0,
                critical_threshold=600000.0,
                unit="currency",
                format_type="currency",
                board_visible=True
            ),
            ExecutiveKPI(
                name="customer_satisfaction_nps",
                title="Net Promoter Score",
                description="Customer satisfaction and loyalty metric",
                category=KPICategory.CUSTOMER,
                tier=MetricTier.CRITICAL,
                target_value=50.0,
                warning_threshold=30.0,
                critical_threshold=10.0,
                unit="score",
                format_type="number",
                board_visible=True
            ),
            ExecutiveKPI(
                name="system_availability",
                title="System Availability",
                description="Overall system uptime and availability",
                category=KPICategory.TECHNICAL,
                tier=MetricTier.CRITICAL,
                target_value=99.9,
                warning_threshold=99.0,
                critical_threshold=95.0,
                unit="percentage",
                format_type="percentage",
                board_visible=True
            ),
            
            # CFO Financial KPIs
            ExecutiveKPI(
                name="gross_profit_margin",
                title="Gross Profit Margin",
                description="Gross profit as percentage of total revenue",
                category=KPICategory.FINANCIAL,
                tier=MetricTier.CRITICAL,
                target_value=40.0,
                warning_threshold=30.0,
                critical_threshold=20.0,
                unit="percentage",
                format_type="percentage",
                board_visible=True
            ),
            ExecutiveKPI(
                name="operating_cash_flow",
                title="Operating Cash Flow",
                description="Cash generated from operating activities",
                category=KPICategory.FINANCIAL,
                tier=MetricTier.HIGH,
                target_value=500000.0,
                warning_threshold=300000.0,
                critical_threshold=100000.0,
                unit="currency",
                format_type="currency",
                board_visible=True
            ),
            ExecutiveKPI(
                name="cost_per_transaction",
                title="Cost per Transaction",
                description="Average cost to process each transaction",
                category=KPICategory.FINANCIAL,
                tier=MetricTier.HIGH,
                target_value=0.50,
                warning_threshold=0.75,
                critical_threshold=1.00,
                unit="currency",
                format_type="currency"
            ),
            
            # CTO Technical KPIs
            ExecutiveKPI(
                name="innovation_index",
                title="Innovation Index",
                description="Measure of technical innovation and R&D progress",
                category=KPICategory.TECHNICAL,
                tier=MetricTier.HIGH,
                target_value=8.0,
                warning_threshold=6.0,
                critical_threshold=4.0,
                unit="score",
                format_type="number"
            ),
            ExecutiveKPI(
                name="technical_debt_ratio",
                title="Technical Debt Ratio",
                description="Ratio of technical debt to total development effort",
                category=KPICategory.TECHNICAL,
                tier=MetricTier.MEDIUM,
                target_value=20.0,
                warning_threshold=35.0,
                critical_threshold=50.0,
                unit="percentage",
                format_type="percentage"
            ),
            ExecutiveKPI(
                name="security_incidents",
                title="Security Incidents",
                description="Number of security incidents per month",
                category=KPICategory.TECHNICAL,
                tier=MetricTier.CRITICAL,
                target_value=0.0,
                warning_threshold=1.0,
                critical_threshold=3.0,
                unit="count",
                format_type="number"
            ),
            
            # COO Operations KPIs
            ExecutiveKPI(
                name="operational_efficiency",
                title="Operational Efficiency",
                description="Overall operational efficiency percentage",
                category=KPICategory.OPERATIONAL,
                tier=MetricTier.CRITICAL,
                target_value=90.0,
                warning_threshold=80.0,
                critical_threshold=70.0,
                unit="percentage",
                format_type="percentage"
            ),
            ExecutiveKPI(
                name="process_automation_rate",
                title="Process Automation Rate",
                description="Percentage of processes that are automated",
                category=KPICategory.OPERATIONAL,
                tier=MetricTier.HIGH,
                target_value=75.0,
                warning_threshold=60.0,
                critical_threshold=40.0,
                unit="percentage",
                format_type="percentage"
            ),
            
            # Data Strategy KPIs
            ExecutiveKPI(
                name="data_quality_index",
                title="Data Quality Index",
                description="Overall data quality across all systems",
                category=KPICategory.DATA_QUALITY,
                tier=MetricTier.CRITICAL,
                target_value=95.0,
                warning_threshold=85.0,
                critical_threshold=75.0,
                unit="percentage",
                format_type="percentage",
                board_visible=True
            ),
            ExecutiveKPI(
                name="data_driven_decisions",
                title="Data-Driven Decisions",
                description="Percentage of business decisions based on data analytics",
                category=KPICategory.DATA_QUALITY,
                tier=MetricTier.HIGH,
                target_value=80.0,
                warning_threshold=60.0,
                critical_threshold=40.0,
                unit="percentage",
                format_type="percentage"
            ),
            ExecutiveKPI(
                name="ml_model_roi",
                title="ML Model ROI",
                description="Return on investment from machine learning initiatives",
                category=KPICategory.FINANCIAL,
                tier=MetricTier.HIGH,
                target_value=300.0,
                warning_threshold=200.0,
                critical_threshold=100.0,
                unit="percentage",
                format_type="percentage"
            )
        ]
        
        for kpi in executive_kpis:
            self.executive_kpis[kpi.name] = kpi
        
        self.logger.info(f"Initialized {len(executive_kpis)} executive KPIs")
    
    def _initialize_sla_definitions(self):
        """Initialize SLA definitions"""
        
        slas = [
            SLADefinition(
                name="system_uptime",
                description="System must maintain 99.9% uptime",
                target_percentage=99.9,
                measurement_window="daily",
                critical_threshold=95.0,
                warning_threshold=99.0,
                penalty_cost=10000.0,
                business_impact="critical"
            ),
            SLADefinition(
                name="api_response_time",
                description="95% of API calls must respond within 200ms",
                target_percentage=95.0,
                measurement_window="hourly",
                critical_threshold=80.0,
                warning_threshold=90.0,
                penalty_cost=1000.0,
                business_impact="high"
            ),
            SLADefinition(
                name="data_processing_sla",
                description="ETL pipelines must complete within 30 minutes",
                target_percentage=98.0,
                measurement_window="daily",
                critical_threshold=85.0,
                warning_threshold=95.0,
                penalty_cost=5000.0,
                business_impact="high"
            ),
            SLADefinition(
                name="customer_support_response",
                description="Customer support must respond within 2 hours",
                target_percentage=95.0,
                measurement_window="daily",
                critical_threshold=75.0,
                warning_threshold=90.0,
                penalty_cost=500.0,
                business_impact="medium"
            )
        ]
        
        for sla in slas:
            self.sla_definitions[sla.name] = sla
        
        self.logger.info(f"Initialized {len(slas)} SLA definitions")
    
    def _initialize_dashboard_definitions(self):
        """Initialize executive dashboard definitions"""
        
        # CEO Overview Dashboard
        self.dashboard_definitions[ExecutiveDashboardType.CEO_OVERVIEW] = {
            "title": "CEO Executive Dashboard - Business Overview",
            "description": "High-level business performance and strategic KPIs for executive decision making",
            "refresh_interval": "1m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Total Revenue (MTD)",
                    "query": "sum:business.kpi.total_revenue{*}",
                    "precision": 0,
                    "format": "currency",
                    "comparison": "previous_month"
                },
                {
                    "type": "query_value",
                    "title": "Net Promoter Score",
                    "query": "avg:business.kpi.customer_satisfaction_nps{*}",
                    "precision": 1,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "System Availability",
                    "query": "avg:business.kpi.system_availability{*}",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Active Customers",
                    "query": "sum:business.kpi.active_customers{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "timeseries",
                    "title": "Revenue Trend (90 days)",
                    "query": "sum:business.kpi.total_revenue{*}.rollup(sum, 86400)",
                    "timeframe": "90d"
                },
                {
                    "type": "timeseries",
                    "title": "Customer Satisfaction Trend",
                    "query": "avg:business.kpi.customer_satisfaction_nps{*}",
                    "timeframe": "30d"
                },
                {
                    "type": "heatmap",
                    "title": "Business KPI Health Matrix",
                    "query": "avg:business.kpis.health_score{*} by {category}"
                },
                {
                    "type": "toplist",
                    "title": "Top Revenue Sources",
                    "query": "sum:business.revenue.by_source{*} by {source}",
                    "limit": 10
                }
            ]
        }
        
        # CFO Financial Dashboard
        self.dashboard_definitions[ExecutiveDashboardType.CFO_FINANCIAL] = {
            "title": "CFO Financial Dashboard - Revenue & Cost Optimization",
            "description": "Financial performance metrics and cost optimization insights",
            "refresh_interval": "5m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Gross Profit Margin",
                    "query": "avg:business.kpi.gross_profit_margin{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Operating Cash Flow",
                    "query": "sum:business.kpi.operating_cash_flow{*}",
                    "precision": 0,
                    "format": "currency"
                },
                {
                    "type": "query_value",
                    "title": "Cost per Transaction",
                    "query": "avg:business.kpi.cost_per_transaction{*}",
                    "precision": 2,
                    "format": "currency"
                },
                {
                    "type": "query_value",
                    "title": "Infrastructure Costs",
                    "query": "sum:infrastructure.costs.total{*}",
                    "precision": 0,
                    "format": "currency"
                },
                {
                    "type": "timeseries",
                    "title": "Profit & Loss Trend",
                    "query": "sum:business.financial.profit{*}, sum:business.financial.loss{*}",
                    "timeframe": "60d"
                },
                {
                    "type": "timeseries",
                    "title": "Cost Breakdown by Category",
                    "query": "sum:business.costs.total{*} by {category}",
                    "timeframe": "30d"
                },
                {
                    "type": "pie_chart",
                    "title": "Revenue by Business Unit",
                    "query": "sum:business.revenue.total{*} by {business_unit}"
                },
                {
                    "type": "change",
                    "title": "Month-over-Month Growth",
                    "query": "sum:business.revenue.total{*}",
                    "change_type": "relative",
                    "compare_to": "month_before"
                }
            ]
        }
        
        # CTO Technical Dashboard
        self.dashboard_definitions[ExecutiveDashboardType.CTO_TECHNICAL] = {
            "title": "CTO Technical Dashboard - Innovation & System Health",
            "description": "Technical innovation metrics and system performance overview",
            "refresh_interval": "2m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Innovation Index",
                    "query": "avg:business.kpi.innovation_index{*}",
                    "precision": 1,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Technical Debt Ratio",
                    "query": "avg:business.kpi.technical_debt_ratio{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Security Incidents",
                    "query": "sum:business.kpi.security_incidents{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "System Performance Score",
                    "query": "avg:system.performance.score{*}",
                    "precision": 1,
                    "format": "number"
                },
                {
                    "type": "timeseries",
                    "title": "API Performance Trends",
                    "query": "percentile:api.response_time{*}:95",
                    "timeframe": "24h"
                },
                {
                    "type": "timeseries",
                    "title": "Infrastructure Utilization",
                    "query": "avg:system.cpu.utilization{*}, avg:system.memory.utilization{*}",
                    "timeframe": "24h"
                },
                {
                    "type": "service_map",
                    "title": "Service Dependencies",
                    "services": ["api", "database", "cache", "messaging"]
                },
                {
                    "type": "hostmap",
                    "title": "Infrastructure Health Map",
                    "query": "avg:system.cpu.utilization{*} by {host}"
                }
            ]
        }
        
        # COO Operations Dashboard  
        self.dashboard_definitions[ExecutiveDashboardType.COO_OPERATIONS] = {
            "title": "COO Operations Dashboard - Efficiency & Performance",
            "description": "Operational efficiency and process performance metrics",
            "refresh_interval": "3m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Operational Efficiency",
                    "query": "avg:business.kpi.operational_efficiency{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Process Automation Rate",
                    "query": "avg:business.kpi.process_automation_rate{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "ETL Success Rate",
                    "query": "avg:etl.success_rate{*}",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Average Processing Time",
                    "query": "avg:etl.processing_time{*}",
                    "precision": 1,
                    "format": "duration"
                },
                {
                    "type": "timeseries",
                    "title": "Data Pipeline Performance",
                    "query": "avg:etl.processing_time{*} by {pipeline}",
                    "timeframe": "48h"
                },
                {
                    "type": "timeseries",
                    "title": "Error Rate Trends",
                    "query": "sum:errors.total{*}.as_rate() by {service}",
                    "timeframe": "24h"
                },
                {
                    "type": "distribution",
                    "title": "Transaction Volume Distribution",
                    "query": "avg:transactions.volume{*} by {service}"
                },
                {
                    "type": "table",
                    "title": "SLA Compliance Summary",
                    "query": "avg:sla.compliance{*} by {sla_name}",
                    "columns": ["SLA", "Current", "Target", "Status"]
                }
            ]
        }
        
        # Board Summary Dashboard
        self.dashboard_definitions[ExecutiveDashboardType.BOARD_SUMMARY] = {
            "title": "Board Summary Dashboard - Strategic KPIs",
            "description": "Strategic KPIs and business impact metrics for board reporting",
            "refresh_interval": "10m",
            "widgets": [
                {
                    "type": "iframe",
                    "title": "Business Performance Summary",
                    "url": "/api/v1/reports/executive-summary"
                },
                {
                    "type": "query_value",
                    "title": "YTD Revenue",
                    "query": "sum:business.revenue.ytd{*}",
                    "precision": 0,
                    "format": "currency"
                },
                {
                    "type": "query_value",
                    "title": "Market Share",
                    "query": "avg:business.market_share{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Customer Growth Rate",
                    "query": "avg:business.customer_growth_rate{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "sunburst",
                    "title": "Business Unit Performance",
                    "query": "sum:business.performance{*} by {unit,category}"
                },
                {
                    "type": "treemap",
                    "title": "Risk Assessment Matrix",
                    "query": "avg:business.risk_score{*} by {risk_category}"
                }
            ]
        }
        
        # Data Strategy Dashboard
        self.dashboard_definitions[ExecutiveDashboardType.DATA_STRATEGY] = {
            "title": "Data Strategy Dashboard - Analytics & Insights",
            "description": "Data-driven insights and analytics strategic overview",
            "refresh_interval": "5m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Data Quality Index",
                    "query": "avg:business.kpi.data_quality_index{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Data-Driven Decisions",
                    "query": "avg:business.kpi.data_driven_decisions{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "ML Model ROI",
                    "query": "avg:business.kpi.ml_model_roi{*}",
                    "precision": 0,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Data Processing Volume",
                    "query": "sum:data.processing.volume{*}",
                    "precision": 0,
                    "format": "bytes"
                },
                {
                    "type": "timeseries",
                    "title": "ML Model Performance Trends",
                    "query": "avg:ml.model.accuracy{*} by {model_name}",
                    "timeframe": "7d"
                },
                {
                    "type": "timeseries",
                    "title": "Data Quality Trends",
                    "query": "avg:data.quality.score{*} by {dataset}",
                    "timeframe": "30d"
                },
                {
                    "type": "scatterplot",
                    "title": "Data Value vs Quality Matrix",
                    "x_axis": "avg:data.business_value{*}",
                    "y_axis": "avg:data.quality.score{*}"
                },
                {
                    "type": "funnel",
                    "title": "Data Pipeline Funnel",
                    "steps": ["Raw", "Bronze", "Silver", "Gold", "Insights"]
                }
            ]
        }
        
        self.logger.info(f"Initialized {len(self.dashboard_definitions)} executive dashboard definitions")
    
    async def create_executive_dashboard(self, dashboard_type: ExecutiveDashboardType,
                                       custom_title: Optional[str] = None,
                                       custom_widgets: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
        """Create an executive dashboard"""
        
        try:
            if dashboard_type not in self.dashboard_definitions:
                self.logger.error(f"Unknown dashboard type: {dashboard_type}")
                return None
            
            definition = self.dashboard_definitions[dashboard_type]
            
            # Use dashboard manager to create the dashboard
            if self.dashboard_manager:
                dashboard_id = await self.dashboard_manager.create_dashboard(
                    dashboard_type.value,
                    custom_title or definition["title"],
                    definition.get("description", ""),
                    custom_widgets or definition["widgets"]
                )
                
                if dashboard_id and self.datadog_monitoring:
                    # Track dashboard creation
                    self.datadog_monitoring.counter(
                        "executive.dashboard.created",
                        tags=[
                            f"dashboard_type:{dashboard_type.value}",
                            f"service:{self.service_name}"
                        ]
                    )
                
                return dashboard_id
            else:
                self.logger.error("Dashboard manager not available")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to create executive dashboard {dashboard_type}: {str(e)}")
            return None
    
    async def create_all_executive_dashboards(self) -> Dict[ExecutiveDashboardType, Optional[str]]:
        """Create all executive dashboards"""
        
        results = {}
        
        for dashboard_type in ExecutiveDashboardType:
            try:
                dashboard_id = await self.create_executive_dashboard(dashboard_type)
                results[dashboard_type] = dashboard_id
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Failed to create dashboard {dashboard_type}: {str(e)}")
                results[dashboard_type] = None
        
        success_count = sum(1 for result in results.values() if result is not None)
        total_count = len(results)
        
        self.logger.info(f"Executive dashboard creation completed: {success_count}/{total_count} successful")
        
        # Track overall success
        if self.datadog_monitoring:
            self.datadog_monitoring.gauge(
                "executive.dashboards.created_total",
                success_count,
                tags=[f"service:{self.service_name}"]
            )
        
        return results
    
    async def track_executive_kpi(self, kpi_name: str, value: float,
                                tags: Optional[Dict[str, str]] = None) -> bool:
        """Track an executive KPI value"""
        
        try:
            if kpi_name not in self.executive_kpis:
                self.logger.warning(f"Unknown executive KPI: {kpi_name}")
                return False
            
            kpi = self.executive_kpis[kpi_name]
            
            # Track with business metrics tracker
            if self.business_metrics:
                await self.business_metrics.track_kpi(kpi_name, value, tags)
            
            # Send directly to DataDog with executive tags
            if self.datadog_monitoring:
                dd_tags = [
                    f"kpi:{kpi_name}",
                    f"tier:{kpi.tier.value}",
                    f"category:{kpi.category.value}",
                    f"board_visible:{kpi.board_visible}",
                    f"service:{self.service_name}"
                ]
                
                if tags:
                    dd_tags.extend([f"{k}:{v}" for k, v in tags.items()])
                
                self.datadog_monitoring.gauge(
                    f"executive.kpi.{kpi_name}",
                    value,
                    tags=dd_tags
                )
                
                # Check thresholds and send alerts if needed
                await self._check_executive_threshold(kpi, value)
            
            self.logger.debug(f"Tracked executive KPI {kpi_name}: {value}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track executive KPI {kpi_name}: {str(e)}")
            return False
    
    async def _check_executive_threshold(self, kpi: ExecutiveKPI, value: float):
        """Check executive KPI thresholds and generate alerts"""
        
        try:
            alert_level = None
            
            # Determine if higher or lower is better based on KPI type
            if kpi.name in ["cost_per_transaction", "technical_debt_ratio", "security_incidents"]:
                # Lower is better
                if kpi.critical_threshold and value >= kpi.critical_threshold:
                    alert_level = "critical"
                elif kpi.warning_threshold and value >= kpi.warning_threshold:
                    alert_level = "warning"
            else:
                # Higher is better
                if kpi.critical_threshold and value <= kpi.critical_threshold:
                    alert_level = "critical"
                elif kpi.warning_threshold and value <= kpi.warning_threshold:
                    alert_level = "warning"
            
            if alert_level and self.datadog_monitoring:
                self.alert_counts[alert_level] += 1
                
                self.datadog_monitoring.counter(
                    "executive.kpi.threshold_breach",
                    tags=[
                        f"kpi:{kpi.name}",
                        f"alert_level:{alert_level}",
                        f"tier:{kpi.tier.value}",
                        f"board_visible:{kpi.board_visible}",
                        f"service:{self.service_name}"
                    ]
                )
                
                self.logger.warning(
                    f"Executive KPI threshold breach: {kpi.name} = {value} "
                    f"(threshold: {kpi.critical_threshold if alert_level == 'critical' else kpi.warning_threshold})"
                )
                
        except Exception as e:
            self.logger.error(f"Failed to check executive threshold: {str(e)}")
    
    async def track_sla_compliance(self, sla_name: str, compliance_percentage: float,
                                 measurement_window: Optional[str] = None) -> bool:
        """Track SLA compliance"""
        
        try:
            if sla_name not in self.sla_definitions:
                self.logger.warning(f"Unknown SLA: {sla_name}")
                return False
            
            sla = self.sla_definitions[sla_name]
            
            # Determine compliance status
            if compliance_percentage >= sla.target_percentage:
                status = "compliant"
            elif compliance_percentage >= sla.warning_threshold:
                status = "warning"
            else:
                status = "breach"
            
            if self.datadog_monitoring:
                tags = [
                    f"sla:{sla_name}",
                    f"status:{status}",
                    f"business_impact:{sla.business_impact}",
                    f"window:{sla.measurement_window}",
                    f"service:{self.service_name}"
                ]
                
                # Track compliance percentage
                self.datadog_monitoring.gauge(
                    f"executive.sla.compliance.{sla_name}",
                    compliance_percentage,
                    tags=tags
                )
                
                # Track compliance status
                self.datadog_monitoring.counter(
                    f"executive.sla.status.{status}",
                    tags=tags
                )
                
                # Track potential penalty cost for breaches
                if status == "breach" and sla.penalty_cost:
                    self.datadog_monitoring.gauge(
                        "executive.sla.penalty_cost",
                        sla.penalty_cost,
                        tags=tags
                    )
            
            self.logger.info(f"SLA compliance tracked: {sla_name} = {compliance_percentage}% ({status})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track SLA compliance: {str(e)}")
            return False
    
    async def generate_executive_summary(self) -> Dict[str, Any]:
        """Generate executive summary report"""
        
        try:
            current_time = datetime.utcnow()
            
            # Calculate KPI summaries
            kpi_summary = {}
            critical_issues = []
            
            for kpi_name, kpi in self.executive_kpis.items():
                # This would normally get actual values from DataDog or metrics store
                # For now, we'll create a placeholder structure
                kpi_summary[kpi_name] = {
                    "title": kpi.title,
                    "category": kpi.category.value,
                    "tier": kpi.tier.value,
                    "target_value": kpi.target_value,
                    "current_value": None,  # Would be populated from actual metrics
                    "trend": "stable",  # Would be calculated from historical data
                    "status": "unknown",
                    "board_visible": kpi.board_visible
                }
            
            # Calculate SLA summary
            sla_summary = {}
            for sla_name, sla in self.sla_definitions.items():
                sla_summary[sla_name] = {
                    "description": sla.description,
                    "target": sla.target_percentage,
                    "current_compliance": None,  # Would be populated from actual metrics
                    "business_impact": sla.business_impact,
                    "penalty_cost": sla.penalty_cost
                }
            
            executive_summary = {
                "generated_at": current_time.isoformat(),
                "service_name": self.service_name,
                "summary_period": "last_24_hours",
                "kpis": kpi_summary,
                "slas": sla_summary,
                "alerts": {
                    "critical_count": self.alert_counts["critical"],
                    "warning_count": self.alert_counts["warning"],
                    "info_count": self.alert_counts["info"]
                },
                "dashboard_access_count": self.dashboard_access_count,
                "last_refresh": self.last_refresh_time.isoformat(),
                "critical_issues": critical_issues,
                "recommendations": [
                    "Monitor critical KPI thresholds closely",
                    "Review SLA compliance for potential improvements",
                    "Consider automated alerting for board-visible KPIs"
                ]
            }
            
            # Track summary generation
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "executive.summary.generated",
                    tags=[f"service:{self.service_name}"]
                )
            
            return executive_summary
            
        except Exception as e:
            self.logger.error(f"Failed to generate executive summary: {str(e)}")
            return {}
    
    def get_dashboard_status(self) -> Dict[str, Any]:
        """Get status of executive dashboards"""
        
        status = {
            "service_name": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "executive_kpis": {
                "total_defined": len(self.executive_kpis),
                "critical_tier": len([k for k in self.executive_kpis.values() if k.tier == MetricTier.CRITICAL]),
                "board_visible": len([k for k in self.executive_kpis.values() if k.board_visible]),
                "by_category": {}
            },
            "sla_definitions": {
                "total_defined": len(self.sla_definitions),
                "critical_impact": len([s for s in self.sla_definitions.values() if s.business_impact == "critical"]),
                "with_penalties": len([s for s in self.sla_definitions.values() if s.penalty_cost is not None])
            },
            "dashboards": {
                "types_available": [dt.value for dt in ExecutiveDashboardType],
                "total_types": len(ExecutiveDashboardType)
            },
            "performance": {
                "dashboard_access_count": self.dashboard_access_count,
                "last_refresh_time": self.last_refresh_time.isoformat(),
                "alert_counts": self.alert_counts.copy()
            }
        }
        
        # Count KPIs by category
        for kpi in self.executive_kpis.values():
            category = kpi.category.value
            if category not in status["executive_kpis"]["by_category"]:
                status["executive_kpis"]["by_category"][category] = 0
            status["executive_kpis"]["by_category"][category] += 1
        
        return status


# Global executive dashboards instance
_executive_dashboards: Optional[DataDogExecutiveDashboards] = None


def get_executive_dashboards(service_name: str = "executive-dashboards",
                           datadog_monitoring: Optional[DatadogMonitoring] = None,
                           dashboard_manager: Optional[DataDogDashboardManager] = None,
                           business_metrics: Optional[DataDogBusinessMetricsTracker] = None) -> DataDogExecutiveDashboards:
    """Get or create executive dashboards instance"""
    global _executive_dashboards
    
    if _executive_dashboards is None:
        _executive_dashboards = DataDogExecutiveDashboards(
            service_name, datadog_monitoring, dashboard_manager, business_metrics
        )
    
    return _executive_dashboards


# Convenience functions

async def track_executive_kpi(kpi_name: str, value: float, tags: Optional[Dict[str, str]] = None) -> bool:
    """Convenience function for tracking executive KPIs"""
    executive_dashboards = get_executive_dashboards()
    return await executive_dashboards.track_executive_kpi(kpi_name, value, tags)


async def track_sla_compliance(sla_name: str, compliance_percentage: float) -> bool:
    """Convenience function for tracking SLA compliance"""
    executive_dashboards = get_executive_dashboards()
    return await executive_dashboards.track_sla_compliance(sla_name, compliance_percentage)


async def create_all_executive_dashboards() -> Dict[ExecutiveDashboardType, Optional[str]]:
    """Convenience function for creating all executive dashboards"""
    executive_dashboards = get_executive_dashboards()
    return await executive_dashboards.create_all_executive_dashboards()


async def generate_executive_summary() -> Dict[str, Any]:
    """Convenience function for generating executive summary"""
    executive_dashboards = get_executive_dashboards()
    return await executive_dashboards.generate_executive_summary()