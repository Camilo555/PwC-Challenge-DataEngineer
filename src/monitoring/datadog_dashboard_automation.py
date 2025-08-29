"""
DataDog Dashboard Automation System
Provides comprehensive automated dashboard creation, updates, and role-based access management
with dynamic configuration and scheduled insights reporting
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path
import yaml

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_dashboard_integration import DataDogDashboardManager
from monitoring.datadog_executive_dashboards import DataDogExecutiveDashboards, ExecutiveDashboardType
from monitoring.datadog_technical_operations_dashboards import DataDogTechnicalOperationsDashboards
from monitoring.datadog_ml_operations_dashboards import DataDogMLOperationsDashboards

logger = get_logger(__name__)


class DashboardScope(Enum):
    """Dashboard scope levels"""
    EXECUTIVE = "executive"
    TECHNICAL = "technical"
    OPERATIONAL = "operational"
    ML_OPS = "ml_ops"
    BUSINESS = "business"
    SECURITY = "security"
    COMPLIANCE = "compliance"


class UserRole(Enum):
    """User roles for dashboard access"""
    VIEWER = "viewer"
    EDITOR = "editor"
    ADMIN = "admin"
    EXECUTIVE = "executive"
    TECHNICAL_LEAD = "technical_lead"
    DATA_SCIENTIST = "data_scientist"
    OPERATIONS_MANAGER = "operations_manager"


class RefreshInterval(Enum):
    """Dashboard refresh intervals"""
    REAL_TIME = "30s"
    HIGH_FREQUENCY = "1m"
    STANDARD = "5m"
    LOW_FREQUENCY = "15m"
    PERIODIC = "1h"
    DAILY = "24h"


@dataclass
class DashboardTemplate:
    """Dashboard template definition"""
    template_id: str
    name: str
    description: str
    scope: DashboardScope
    widgets: List[Dict[str, Any]]
    default_timeframe: str = "4h"
    refresh_interval: RefreshInterval = RefreshInterval.STANDARD
    required_roles: List[UserRole] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    filters: Dict[str, str] = field(default_factory=dict)


@dataclass
class DashboardConfig:
    """Dashboard configuration"""
    config_id: str
    dashboard_id: Optional[str]
    template_id: str
    title: str
    description: str
    owner: str
    team: str
    environment: str
    custom_variables: Dict[str, Any] = field(default_factory=dict)
    notification_settings: Dict[str, Any] = field(default_factory=dict)
    access_controls: Dict[str, List[str]] = field(default_factory=dict)
    auto_update: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class ScheduledReport:
    """Scheduled dashboard report configuration"""
    report_id: str
    dashboard_ids: List[str]
    schedule: str  # cron format
    recipients: List[str]
    format: str = "pdf"  # pdf, png, html
    title: str = ""
    description: str = ""
    filters: Dict[str, str] = field(default_factory=dict)
    last_sent: Optional[datetime] = None
    enabled: bool = True


class DataDogDashboardAutomation:
    """
    Comprehensive Dashboard Automation System
    
    Features:
    - Automated dashboard creation from templates
    - Role-based access control and permissions
    - Dynamic dashboard configuration and updates
    - Scheduled report generation and distribution
    - Dashboard lifecycle management
    - Custom widget and metric integration
    - Multi-environment dashboard deployment
    - Dashboard performance monitoring
    - Automated insights and recommendations
    """
    
    def __init__(self, service_name: str = "dashboard-automation",
                 datadog_monitoring: Optional[DatadogMonitoring] = None,
                 dashboard_manager: Optional[DataDogDashboardManager] = None,
                 config_path: Optional[str] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.dashboard_manager = dashboard_manager
        self.config_path = config_path or "./config/datadog/dashboards"
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Core data structures
        self.dashboard_templates: Dict[str, DashboardTemplate] = {}
        self.dashboard_configs: Dict[str, DashboardConfig] = {}
        self.scheduled_reports: Dict[str, ScheduledReport] = {}
        self.user_permissions: Dict[str, Dict[str, List[UserRole]]] = {}
        
        # Dashboard managers
        self.executive_dashboards = None
        self.technical_dashboards = None
        self.ml_dashboards = None
        
        # Performance tracking
        self.dashboard_creation_count = 0
        self.dashboard_update_count = 0
        self.report_generation_count = 0
        self.last_automation_run = None
        
        # Background tasks
        self.automation_tasks: List[asyncio.Task] = []
        
        # Initialize system
        self._initialize_dashboard_templates()
        self._initialize_user_permissions()
        self._load_dashboard_configurations()
        
        # Start automation services
        asyncio.create_task(self._start_automation_services())
        
        self.logger.info(f"Dashboard automation system initialized for {service_name}")
    
    def _initialize_dashboard_templates(self):
        """Initialize dashboard templates"""
        
        # Executive Dashboard Templates
        executive_templates = [
            DashboardTemplate(
                template_id="ceo_overview",
                name="CEO Executive Overview",
                description="High-level business performance and strategic KPIs",
                scope=DashboardScope.EXECUTIVE,
                widgets=[
                    {
                        "type": "query_value",
                        "title": "Total Revenue (Current Month)",
                        "query": "sum:business.kpi.total_revenue{env:{{environment}}}",
                        "format": "currency",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Net Promoter Score",
                        "query": "avg:business.kpi.customer_satisfaction_nps{env:{{environment}}}",
                        "format": "number",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "System Availability",
                        "query": "avg:business.kpi.system_availability{env:{{environment}}}",
                        "format": "percentage",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Active Customers",
                        "query": "sum:business.kpi.active_customers{env:{{environment}}}",
                        "format": "number",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "timeseries",
                        "title": "Revenue Trend (90 Days)",
                        "query": "sum:business.kpi.total_revenue{env:{{environment}}}.rollup(sum, 86400)",
                        "timeframe": "90d",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "timeseries",
                        "title": "Customer Growth",
                        "query": "sum:business.kpi.customer_growth{env:{{environment}}}",
                        "timeframe": "30d",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "toplist",
                        "title": "Top Revenue Sources",
                        "query": "sum:business.revenue.by_source{env:{{environment}}} by {source}",
                        "limit": 10,
                        "size": {"width": 4, "height": 4}
                    },
                    {
                        "type": "sunburst",
                        "title": "Business Unit Performance",
                        "query": "sum:business.performance{env:{{environment}}} by {unit,category}",
                        "size": {"width": 4, "height": 4}
                    },
                    {
                        "type": "alert_graph",
                        "title": "Critical Alerts Status",
                        "query": "alerts.status{priority:critical,env:{{environment}}}",
                        "size": {"width": 4, "height": 4}
                    }
                ],
                default_timeframe="24h",
                refresh_interval=RefreshInterval.HIGH_FREQUENCY,
                required_roles=[UserRole.EXECUTIVE, UserRole.ADMIN],
                tags=["executive", "business", "kpi"]
            ),
            
            DashboardTemplate(
                template_id="cfo_financial",
                name="CFO Financial Dashboard",
                description="Financial performance and cost optimization metrics",
                scope=DashboardScope.EXECUTIVE,
                widgets=[
                    {
                        "type": "query_value",
                        "title": "Gross Profit Margin",
                        "query": "avg:business.kpi.gross_profit_margin{env:{{environment}}}",
                        "format": "percentage",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Operating Cash Flow",
                        "query": "sum:business.kpi.operating_cash_flow{env:{{environment}}}",
                        "format": "currency",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Cost per Transaction",
                        "query": "avg:business.kpi.cost_per_transaction{env:{{environment}}}",
                        "format": "currency",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Infrastructure Costs",
                        "query": "sum:infrastructure.costs.total{env:{{environment}}}",
                        "format": "currency",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "timeseries",
                        "title": "Revenue vs Costs",
                        "query": "sum:business.revenue.total{env:{{environment}}}, sum:business.costs.total{env:{{environment}}}",
                        "timeframe": "30d",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "pie_chart",
                        "title": "Cost Breakdown by Category",
                        "query": "sum:business.costs.total{env:{{environment}}} by {category}",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "table",
                        "title": "Financial KPIs Summary",
                        "query": "avg:business.financial.kpis{env:{{environment}}} by {kpi_name}",
                        "size": {"width": 12, "height": 6}
                    }
                ],
                required_roles=[UserRole.EXECUTIVE, UserRole.ADMIN],
                tags=["executive", "financial", "costs", "revenue"]
            )
        ]
        
        # Technical Dashboard Templates
        technical_templates = [
            DashboardTemplate(
                template_id="infrastructure_health",
                name="Infrastructure Health Dashboard",
                description="System infrastructure performance and health monitoring",
                scope=DashboardScope.TECHNICAL,
                widgets=[
                    {
                        "type": "hostmap",
                        "title": "Host Infrastructure Map",
                        "query": "avg:system.cpu.utilization{env:{{environment}}} by {host}",
                        "size": {"width": 12, "height": 6}
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Utilization",
                        "query": "avg:system.cpu.utilization{env:{{environment}}} by {host}",
                        "timeframe": "4h",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage",
                        "query": "avg:system.memory.utilization{env:{{environment}}} by {host}",
                        "timeframe": "4h",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "timeseries",
                        "title": "Network I/O",
                        "query": "avg:system.network.bytes_sent{env:{{environment}}}, avg:system.network.bytes_received{env:{{environment}}}",
                        "timeframe": "4h",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "timeseries",
                        "title": "Disk I/O",
                        "query": "avg:system.disk.read{env:{{environment}}}, avg:system.disk.write{env:{{environment}}}",
                        "timeframe": "4h",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "service_map",
                        "title": "Service Dependencies",
                        "services": ["api", "database", "cache", "messaging"],
                        "size": {"width": 12, "height": 6}
                    }
                ],
                required_roles=[UserRole.TECHNICAL_LEAD, UserRole.OPERATIONS_MANAGER, UserRole.ADMIN],
                tags=["infrastructure", "performance", "monitoring"]
            ),
            
            DashboardTemplate(
                template_id="api_performance",
                name="API Performance Dashboard",
                description="API response times, error rates, and throughput monitoring",
                scope=DashboardScope.TECHNICAL,
                widgets=[
                    {
                        "type": "query_value",
                        "title": "Average Response Time",
                        "query": "avg:api.response_time{env:{{environment}}}",
                        "format": "duration",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Error Rate",
                        "query": "sum:api.errors.total{env:{{environment}}}.as_rate() / sum:api.requests.total{env:{{environment}}}.as_rate() * 100",
                        "format": "percentage",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Requests per Second",
                        "query": "sum:api.requests.total{env:{{environment}}}.as_rate()",
                        "format": "number",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "P95 Response Time",
                        "query": "percentile:api.response_time{env:{{environment}}}:95",
                        "format": "duration",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "timeseries",
                        "title": "API Response Times",
                        "query": "avg:api.response_time{env:{{environment}}} by {endpoint}, percentile:api.response_time{env:{{environment}}}:95 by {endpoint}",
                        "timeframe": "4h",
                        "size": {"width": 12, "height": 4}
                    },
                    {
                        "type": "timeseries",
                        "title": "API Error Rates",
                        "query": "sum:api.errors.total{env:{{environment}}}.as_rate() by {endpoint}",
                        "timeframe": "4h",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "toplist",
                        "title": "Slowest Endpoints",
                        "query": "avg:api.response_time{env:{{environment}}} by {endpoint}",
                        "limit": 10,
                        "size": {"width": 6, "height": 4}
                    }
                ],
                required_roles=[UserRole.TECHNICAL_LEAD, UserRole.ADMIN],
                tags=["api", "performance", "monitoring"]
            )
        ]
        
        # ML Operations Dashboard Templates
        ml_templates = [
            DashboardTemplate(
                template_id="ml_model_performance",
                name="ML Model Performance Dashboard",
                description="Machine learning model performance, accuracy, and inference monitoring",
                scope=DashboardScope.ML_OPS,
                widgets=[
                    {
                        "type": "query_value",
                        "title": "Average Model Accuracy",
                        "query": "avg:ml.model.accuracy{env:{{environment}},stage:production}",
                        "format": "percentage",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Inference Latency (P95)",
                        "query": "percentile:ml.inference.latency{env:{{environment}}}:95",
                        "format": "duration",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Model Drift Score",
                        "query": "avg:ml.model.drift_score{env:{{environment}}}",
                        "format": "number",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "query_value",
                        "title": "Active Models",
                        "query": "count:ml.model.active{env:{{environment}}}",
                        "format": "number",
                        "size": {"width": 3, "height": 2}
                    },
                    {
                        "type": "timeseries",
                        "title": "Model Accuracy Trends",
                        "query": "avg:ml.model.accuracy{env:{{environment}}} by {model_name}",
                        "timeframe": "7d",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "timeseries",
                        "title": "Inference Volume and Latency",
                        "query": "sum:ml.inference.requests{env:{{environment}}}.as_count(), avg:ml.inference.latency{env:{{environment}}}",
                        "timeframe": "4h",
                        "size": {"width": 6, "height": 4}
                    },
                    {
                        "type": "heatmap",
                        "title": "Feature Importance Heatmap",
                        "query": "avg:ml.feature.importance{env:{{environment}}} by {feature,model}",
                        "size": {"width": 8, "height": 6}
                    },
                    {
                        "type": "table",
                        "title": "Model Performance Summary",
                        "query": "avg:ml.model.metrics{env:{{environment}}} by {model_name,metric}",
                        "size": {"width": 4, "height": 6}
                    }
                ],
                required_roles=[UserRole.DATA_SCIENTIST, UserRole.TECHNICAL_LEAD, UserRole.ADMIN],
                tags=["ml", "models", "performance", "monitoring"]
            )
        ]
        
        # Combine all templates
        all_templates = executive_templates + technical_templates + ml_templates
        
        for template in all_templates:
            self.dashboard_templates[template.template_id] = template
        
        self.logger.info(f"Initialized {len(all_templates)} dashboard templates")
    
    def _initialize_user_permissions(self):
        """Initialize user role permissions"""
        
        self.user_permissions = {
            # Executive access
            "ceo@company.com": {
                "dashboards": [UserRole.EXECUTIVE, UserRole.VIEWER],
                "scopes": [DashboardScope.EXECUTIVE, DashboardScope.BUSINESS]
            },
            "cfo@company.com": {
                "dashboards": [UserRole.EXECUTIVE, UserRole.VIEWER],
                "scopes": [DashboardScope.EXECUTIVE, DashboardScope.BUSINESS]
            },
            "cto@company.com": {
                "dashboards": [UserRole.EXECUTIVE, UserRole.TECHNICAL_LEAD, UserRole.ADMIN],
                "scopes": [DashboardScope.EXECUTIVE, DashboardScope.TECHNICAL, DashboardScope.OPERATIONAL]
            },
            
            # Technical team access
            "tech-lead@company.com": {
                "dashboards": [UserRole.TECHNICAL_LEAD, UserRole.EDITOR],
                "scopes": [DashboardScope.TECHNICAL, DashboardScope.OPERATIONAL]
            },
            "ops-manager@company.com": {
                "dashboards": [UserRole.OPERATIONS_MANAGER, UserRole.EDITOR],
                "scopes": [DashboardScope.OPERATIONAL, DashboardScope.TECHNICAL]
            },
            
            # Data Science team access
            "data-scientist@company.com": {
                "dashboards": [UserRole.DATA_SCIENTIST, UserRole.EDITOR],
                "scopes": [DashboardScope.ML_OPS, DashboardScope.TECHNICAL]
            },
            
            # General team access
            "team@company.com": {
                "dashboards": [UserRole.VIEWER],
                "scopes": [DashboardScope.OPERATIONAL, DashboardScope.BUSINESS]
            }
        }
        
        self.logger.info(f"Initialized permissions for {len(self.user_permissions)} user groups")
    
    def _load_dashboard_configurations(self):
        """Load dashboard configurations from files"""
        
        try:
            config_dir = Path(self.config_path)
            if config_dir.exists():
                for config_file in config_dir.glob("*.yaml"):
                    try:
                        with open(config_file, 'r') as f:
                            config_data = yaml.safe_load(f)
                            
                        config = DashboardConfig(**config_data)
                        self.dashboard_configs[config.config_id] = config
                        
                    except Exception as e:
                        self.logger.error(f"Failed to load config {config_file}: {str(e)}")
                
                self.logger.info(f"Loaded {len(self.dashboard_configs)} dashboard configurations")
            else:
                self.logger.warning(f"Dashboard config directory not found: {config_dir}")
                
        except Exception as e:
            self.logger.error(f"Failed to load dashboard configurations: {str(e)}")
    
    async def _start_automation_services(self):
        """Start background automation services"""
        
        try:
            # Start dashboard sync service
            self.automation_tasks.append(
                asyncio.create_task(self._dashboard_sync_service())
            )
            
            # Start scheduled reports service
            self.automation_tasks.append(
                asyncio.create_task(self._scheduled_reports_service())
            )
            
            # Start dashboard health monitoring
            self.automation_tasks.append(
                asyncio.create_task(self._dashboard_health_monitoring())
            )
            
            # Start automated insights generation
            self.automation_tasks.append(
                asyncio.create_task(self._automated_insights_service())
            )
            
            self.logger.info("Dashboard automation services started")
            
        except Exception as e:
            self.logger.error(f"Failed to start automation services: {str(e)}")
    
    async def create_dashboard_from_template(self, template_id: str, config: DashboardConfig,
                                           custom_variables: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Create dashboard from template with custom configuration"""
        
        try:
            if template_id not in self.dashboard_templates:
                self.logger.error(f"Unknown template: {template_id}")
                return None
            
            template = self.dashboard_templates[template_id]
            
            # Check user permissions
            if not await self._check_user_permissions(config.owner, template.scope, UserRole.EDITOR):
                self.logger.error(f"User {config.owner} lacks permissions for {template.scope}")
                return None
            
            # Prepare variables for template substitution
            variables = {
                "environment": config.environment,
                "team": config.team,
                **template.variables,
                **config.custom_variables,
                **(custom_variables or {})
            }
            
            # Process template widgets with variable substitution
            processed_widgets = []
            for widget in template.widgets:
                processed_widget = await self._process_widget_template(widget, variables)
                if processed_widget:
                    processed_widgets.append(processed_widget)
            
            # Create dashboard using dashboard manager
            if self.dashboard_manager:
                dashboard_id = await self.dashboard_manager.create_dashboard(
                    config.config_id,
                    config.title,
                    config.description,
                    processed_widgets,
                    template.default_timeframe
                )
                
                if dashboard_id:
                    # Update config with dashboard ID
                    config.dashboard_id = dashboard_id
                    config.created_at = datetime.utcnow()
                    config.updated_at = datetime.utcnow()
                    
                    # Store configuration
                    self.dashboard_configs[config.config_id] = config
                    await self._save_dashboard_configuration(config)
                    
                    # Track creation
                    self.dashboard_creation_count += 1
                    
                    if self.datadog_monitoring:
                        self.datadog_monitoring.counter(
                            "dashboard.automation.created",
                            tags=[
                                f"template:{template_id}",
                                f"scope:{template.scope.value}",
                                f"environment:{config.environment}",
                                f"team:{config.team}"
                            ]
                        )
                    
                    self.logger.info(f"Created dashboard {config.title} with ID {dashboard_id}")
                    return dashboard_id
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to create dashboard from template: {str(e)}")
            return None
    
    async def _process_widget_template(self, widget: Dict[str, Any], 
                                     variables: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process widget template with variable substitution"""
        
        try:
            # Create a copy of the widget
            processed_widget = json.loads(json.dumps(widget))
            
            # Recursive function to substitute variables in nested dictionaries
            def substitute_variables(obj):
                if isinstance(obj, dict):
                    return {k: substitute_variables(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [substitute_variables(item) for item in obj]
                elif isinstance(obj, str):
                    # Simple template variable substitution
                    for var_name, var_value in variables.items():
                        placeholder = f"{{{{{var_name}}}}}"
                        obj = obj.replace(placeholder, str(var_value))
                    return obj
                else:
                    return obj
            
            return substitute_variables(processed_widget)
            
        except Exception as e:
            self.logger.error(f"Failed to process widget template: {str(e)}")
            return None
    
    async def _check_user_permissions(self, user_email: str, scope: DashboardScope, 
                                    required_role: UserRole) -> bool:
        """Check if user has required permissions"""
        
        try:
            if user_email not in self.user_permissions:
                return False
            
            user_perms = self.user_permissions[user_email]
            
            # Check scope access
            if scope not in user_perms.get("scopes", []):
                return False
            
            # Check role access
            user_roles = user_perms.get("dashboards", [])
            if required_role not in user_roles and UserRole.ADMIN not in user_roles:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to check permissions: {str(e)}")
            return False
    
    async def _save_dashboard_configuration(self, config: DashboardConfig):
        """Save dashboard configuration to file"""
        
        try:
            config_dir = Path(self.config_path)
            config_dir.mkdir(parents=True, exist_ok=True)
            
            config_file = config_dir / f"{config.config_id}.yaml"
            config_data = asdict(config)
            
            # Convert datetime objects to strings
            if config_data.get("created_at"):
                config_data["created_at"] = config_data["created_at"].isoformat()
            if config_data.get("updated_at"):
                config_data["updated_at"] = config_data["updated_at"].isoformat()
            
            with open(config_file, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False)
            
        except Exception as e:
            self.logger.error(f"Failed to save dashboard configuration: {str(e)}")
    
    async def update_dashboard(self, config_id: str, 
                             updates: Optional[Dict[str, Any]] = None) -> bool:
        """Update existing dashboard with new configuration"""
        
        try:
            if config_id not in self.dashboard_configs:
                self.logger.error(f"Dashboard config not found: {config_id}")
                return False
            
            config = self.dashboard_configs[config_id]
            
            if not config.dashboard_id:
                self.logger.error(f"No dashboard ID for config: {config_id}")
                return False
            
            # Apply updates
            if updates:
                for key, value in updates.items():
                    if hasattr(config, key):
                        setattr(config, key, value)
            
            # Get template for re-processing
            if config.template_id in self.dashboard_templates:
                template = self.dashboard_templates[config.template_id]
                
                # Prepare variables
                variables = {
                    "environment": config.environment,
                    "team": config.team,
                    **template.variables,
                    **config.custom_variables
                }
                
                # Process widgets
                processed_widgets = []
                for widget in template.widgets:
                    processed_widget = await self._process_widget_template(widget, variables)
                    if processed_widget:
                        processed_widgets.append(processed_widget)
                
                # Update dashboard
                if self.dashboard_manager:
                    success = await self.dashboard_manager.update_dashboard(
                        config.dashboard_id,
                        config.title,
                        config.description,
                        processed_widgets
                    )
                    
                    if success:
                        config.updated_at = datetime.utcnow()
                        await self._save_dashboard_configuration(config)
                        
                        self.dashboard_update_count += 1
                        
                        if self.datadog_monitoring:
                            self.datadog_monitoring.counter(
                                "dashboard.automation.updated",
                                tags=[
                                    f"config_id:{config_id}",
                                    f"environment:{config.environment}"
                                ]
                            )
                        
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to update dashboard: {str(e)}")
            return False
    
    async def _dashboard_sync_service(self):
        """Background service for syncing dashboard configurations"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                sync_count = 0
                
                for config in self.dashboard_configs.values():
                    if config.auto_update and config.dashboard_id:
                        # Check if dashboard needs update
                        if await self._should_update_dashboard(config):
                            success = await self.update_dashboard(config.config_id)
                            if success:
                                sync_count += 1
                
                self.last_automation_run = datetime.utcnow()
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        "dashboard.automation.sync_count",
                        sync_count,
                        tags=[f"service:{self.service_name}"]
                    )
                
                self.logger.info(f"Dashboard sync completed: {sync_count} dashboards updated")
                
            except Exception as e:
                self.logger.error(f"Error in dashboard sync service: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _should_update_dashboard(self, config: DashboardConfig) -> bool:
        """Check if dashboard should be updated"""
        
        try:
            if not config.updated_at:
                return True
            
            # Update if more than 24 hours old
            age = datetime.utcnow() - config.updated_at
            return age.total_seconds() > 86400
            
        except Exception as e:
            self.logger.error(f"Failed to check update requirement: {str(e)}")
            return False
    
    async def _scheduled_reports_service(self):
        """Background service for generating scheduled reports"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                current_time = datetime.utcnow()
                
                for report_id, report in self.scheduled_reports.items():
                    if not report.enabled:
                        continue
                    
                    # Simple schedule check (in production, use proper cron parsing)
                    should_generate = await self._should_generate_report(report, current_time)
                    
                    if should_generate:
                        success = await self._generate_scheduled_report(report)
                        if success:
                            report.last_sent = current_time
                            self.report_generation_count += 1
                
            except Exception as e:
                self.logger.error(f"Error in scheduled reports service: {str(e)}")
                await asyncio.sleep(300)
    
    async def _should_generate_report(self, report: ScheduledReport, 
                                    current_time: datetime) -> bool:
        """Check if report should be generated based on schedule"""
        
        try:
            if not report.last_sent:
                return True
            
            # Simple hourly/daily schedule check
            if "hourly" in report.schedule:
                return (current_time - report.last_sent).total_seconds() >= 3600
            elif "daily" in report.schedule:
                return (current_time - report.last_sent).total_seconds() >= 86400
            elif "weekly" in report.schedule:
                return (current_time - report.last_sent).total_seconds() >= 604800
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to check report schedule: {str(e)}")
            return False
    
    async def _generate_scheduled_report(self, report: ScheduledReport) -> bool:
        """Generate and send scheduled report"""
        
        try:
            # This is a placeholder for report generation
            # In production, you'd integrate with DataDog's reporting API
            
            self.logger.info(f"Generating report {report.report_id} for {len(report.dashboard_ids)} dashboards")
            
            # Simulate report generation
            report_data = {
                "report_id": report.report_id,
                "generated_at": datetime.utcnow().isoformat(),
                "dashboard_count": len(report.dashboard_ids),
                "recipients": report.recipients,
                "format": report.format
            }
            
            # Send report (placeholder)
            for recipient in report.recipients:
                self.logger.info(f"Sending report to {recipient}")
            
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "dashboard.automation.report_generated",
                    tags=[
                        f"report_id:{report.report_id}",
                        f"format:{report.format}",
                        f"recipients:{len(report.recipients)}"
                    ]
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to generate report: {str(e)}")
            return False
    
    async def _dashboard_health_monitoring(self):
        """Background service for monitoring dashboard health"""
        
        while True:
            try:
                await asyncio.sleep(1800)  # Check every 30 minutes
                
                health_issues = []
                
                for config in self.dashboard_configs.values():
                    if config.dashboard_id:
                        health_status = await self._check_dashboard_health(config)
                        if not health_status["healthy"]:
                            health_issues.append({
                                "config_id": config.config_id,
                                "dashboard_id": config.dashboard_id,
                                "issues": health_status["issues"]
                            })
                
                if health_issues and self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        "dashboard.automation.health_issues",
                        len(health_issues),
                        tags=[f"service:{self.service_name}"]
                    )
                
                self.logger.info(f"Dashboard health check completed: {len(health_issues)} issues found")
                
            except Exception as e:
                self.logger.error(f"Error in dashboard health monitoring: {str(e)}")
                await asyncio.sleep(1800)
    
    async def _check_dashboard_health(self, config: DashboardConfig) -> Dict[str, Any]:
        """Check health of a specific dashboard"""
        
        try:
            issues = []
            
            # Check if dashboard exists
            if not config.dashboard_id:
                issues.append("missing_dashboard_id")
            
            # Check last update time
            if config.updated_at:
                age = datetime.utcnow() - config.updated_at
                if age.total_seconds() > 604800:  # 1 week
                    issues.append("outdated_dashboard")
            
            # Check template availability
            if config.template_id not in self.dashboard_templates:
                issues.append("missing_template")
            
            return {
                "healthy": len(issues) == 0,
                "issues": issues
            }
            
        except Exception as e:
            self.logger.error(f"Failed to check dashboard health: {str(e)}")
            return {"healthy": False, "issues": ["health_check_failed"]}
    
    async def _automated_insights_service(self):
        """Background service for generating automated insights"""
        
        while True:
            try:
                await asyncio.sleep(7200)  # Generate insights every 2 hours
                
                insights = await self._generate_automated_insights()
                
                if insights and self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        "dashboard.automation.insights_generated",
                        len(insights),
                        tags=[f"service:{self.service_name}"]
                    )
                
                self.logger.info(f"Generated {len(insights)} automated insights")
                
            except Exception as e:
                self.logger.error(f"Error in automated insights service: {str(e)}")
                await asyncio.sleep(7200)
    
    async def _generate_automated_insights(self) -> List[Dict[str, Any]]:
        """Generate automated insights from dashboard data"""
        
        try:
            insights = []
            
            # Dashboard usage insights
            if self.dashboard_creation_count > 0:
                insights.append({
                    "type": "usage",
                    "title": "Dashboard Creation Activity",
                    "description": f"{self.dashboard_creation_count} dashboards created",
                    "metrics": {"count": self.dashboard_creation_count}
                })
            
            # Template popularity insights
            template_usage = {}
            for config in self.dashboard_configs.values():
                template_id = config.template_id
                template_usage[template_id] = template_usage.get(template_id, 0) + 1
            
            if template_usage:
                most_used = max(template_usage.items(), key=lambda x: x[1])
                insights.append({
                    "type": "template_popularity",
                    "title": "Most Popular Template",
                    "description": f"Template '{most_used[0]}' used {most_used[1]} times",
                    "metrics": {"template": most_used[0], "usage": most_used[1]}
                })
            
            # Environment distribution
            env_distribution = {}
            for config in self.dashboard_configs.values():
                env = config.environment
                env_distribution[env] = env_distribution.get(env, 0) + 1
            
            if env_distribution:
                insights.append({
                    "type": "environment_distribution",
                    "title": "Dashboard Distribution by Environment",
                    "description": f"Dashboards across {len(env_distribution)} environments",
                    "metrics": env_distribution
                })
            
            return insights
            
        except Exception as e:
            self.logger.error(f"Failed to generate insights: {str(e)}")
            return []
    
    async def create_scheduled_report(self, report: ScheduledReport) -> bool:
        """Create a new scheduled report"""
        
        try:
            # Validate dashboard IDs
            for dashboard_id in report.dashboard_ids:
                config_exists = any(
                    config.dashboard_id == dashboard_id 
                    for config in self.dashboard_configs.values()
                )
                if not config_exists:
                    self.logger.warning(f"Dashboard ID {dashboard_id} not found in configurations")
            
            # Store report
            self.scheduled_reports[report.report_id] = report
            
            self.logger.info(f"Created scheduled report {report.report_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create scheduled report: {str(e)}")
            return False
    
    def get_automation_summary(self) -> Dict[str, Any]:
        """Get automation system summary"""
        
        try:
            current_time = datetime.utcnow()
            
            # Template statistics
            template_stats = {}
            for scope in DashboardScope:
                template_stats[scope.value] = len([
                    t for t in self.dashboard_templates.values() 
                    if t.scope == scope
                ])
            
            # Configuration statistics
            config_stats = {
                "total": len(self.dashboard_configs),
                "with_dashboard_id": len([c for c in self.dashboard_configs.values() if c.dashboard_id]),
                "auto_update_enabled": len([c for c in self.dashboard_configs.values() if c.auto_update])
            }
            
            # Environment distribution
            env_distribution = {}
            for config in self.dashboard_configs.values():
                env = config.environment
                env_distribution[env] = env_distribution.get(env, 0) + 1
            
            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "templates": {
                    "total": len(self.dashboard_templates),
                    "by_scope": template_stats
                },
                "configurations": config_stats,
                "scheduled_reports": {
                    "total": len(self.scheduled_reports),
                    "enabled": len([r for r in self.scheduled_reports.values() if r.enabled])
                },
                "performance": {
                    "dashboards_created": self.dashboard_creation_count,
                    "dashboards_updated": self.dashboard_update_count,
                    "reports_generated": self.report_generation_count,
                    "last_automation_run": self.last_automation_run.isoformat() if self.last_automation_run else None
                },
                "environment_distribution": env_distribution,
                "user_permissions": len(self.user_permissions),
                "automation_tasks_running": len([t for t in self.automation_tasks if not t.done()])
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate automation summary: {str(e)}")
            return {}


# Global dashboard automation instance
_dashboard_automation: Optional[DataDogDashboardAutomation] = None


def get_dashboard_automation(service_name: str = "dashboard-automation",
                           datadog_monitoring: Optional[DatadogMonitoring] = None,
                           dashboard_manager: Optional[DataDogDashboardManager] = None,
                           config_path: Optional[str] = None) -> DataDogDashboardAutomation:
    """Get or create dashboard automation instance"""
    global _dashboard_automation
    
    if _dashboard_automation is None:
        _dashboard_automation = DataDogDashboardAutomation(
            service_name, datadog_monitoring, dashboard_manager, config_path
        )
    
    return _dashboard_automation


# Convenience functions

async def create_executive_dashboard(template_id: str, title: str, environment: str,
                                   owner: str, team: str) -> Optional[str]:
    """Convenience function for creating executive dashboard"""
    automation = get_dashboard_automation()
    
    config = DashboardConfig(
        config_id=f"exec_{template_id}_{environment}",
        dashboard_id=None,
        template_id=template_id,
        title=title,
        description=f"Executive dashboard: {title}",
        owner=owner,
        team=team,
        environment=environment
    )
    
    return await automation.create_dashboard_from_template(template_id, config)


async def create_technical_dashboard(template_id: str, title: str, environment: str,
                                   owner: str, team: str) -> Optional[str]:
    """Convenience function for creating technical dashboard"""
    automation = get_dashboard_automation()
    
    config = DashboardConfig(
        config_id=f"tech_{template_id}_{environment}",
        dashboard_id=None,
        template_id=template_id,
        title=title,
        description=f"Technical dashboard: {title}",
        owner=owner,
        team=team,
        environment=environment
    )
    
    return await automation.create_dashboard_from_template(template_id, config)


def get_automation_summary() -> Dict[str, Any]:
    """Convenience function for getting automation summary"""
    automation = get_dashboard_automation()
    return automation.get_automation_summary()