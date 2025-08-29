"""
DataDog Enterprise Platform Integration
Main orchestration layer that integrates all DataDog monitoring components
providing a unified interface for comprehensive enterprise data platform monitoring
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from contextlib import asynccontextmanager

from core.logging import get_logger

# Import all DataDog monitoring components
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_dashboard_integration import DataDogDashboardManager
from monitoring.datadog_executive_dashboards import DataDogExecutiveDashboards, ExecutiveDashboardType
from monitoring.datadog_technical_operations_dashboards import DataDogTechnicalOperationsDashboards, TechnicalDashboardType
from monitoring.datadog_ml_operations_dashboards import DataDogMLOperationsDashboards, MLDashboardType
from monitoring.datadog_custom_metrics_advanced import DataDogCustomMetrics
from monitoring.datadog_intelligent_alerting import DataDogIntelligentAlerting
from monitoring.datadog_business_metrics import DataDogBusinessMetricsTracker
from monitoring.datadog_dashboard_automation import DataDogDashboardAutomation
from monitoring.datadog_enterprise_alerting import DataDogEnterpriseAlerting
from monitoring.datadog_role_based_access import DataDogRoleBasedAccess, Role, Permission

logger = get_logger(__name__)


class PlatformComponent(Enum):
    """Enterprise platform components"""
    CORE_MONITORING = "core_monitoring"
    DASHBOARD_MANAGER = "dashboard_manager"
    EXECUTIVE_DASHBOARDS = "executive_dashboards"
    TECHNICAL_DASHBOARDS = "technical_dashboards"
    ML_DASHBOARDS = "ml_dashboards"
    CUSTOM_METRICS = "custom_metrics"
    INTELLIGENT_ALERTING = "intelligent_alerting"
    BUSINESS_METRICS = "business_metrics"
    DASHBOARD_AUTOMATION = "dashboard_automation"
    ENTERPRISE_ALERTING = "enterprise_alerting"
    RBAC_SYSTEM = "rbac_system"


class DeploymentEnvironment(Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    DISASTER_RECOVERY = "disaster_recovery"


@dataclass
class PlatformConfiguration:
    """Enterprise platform configuration"""
    environment: DeploymentEnvironment
    datadog_api_key: str
    datadog_app_key: str
    service_name: str
    version: str
    region: str = "us1"
    
    # Component-specific configurations
    enable_executive_dashboards: bool = True
    enable_technical_dashboards: bool = True
    enable_ml_dashboards: bool = True
    enable_enterprise_alerting: bool = True
    enable_dashboard_automation: bool = True
    enable_rbac: bool = True
    
    # Performance settings
    metrics_batch_size: int = 100
    dashboard_refresh_interval: int = 300  # seconds
    alert_evaluation_interval: int = 60   # seconds
    
    # Compliance settings
    audit_retention_days: int = 365
    enable_sox_compliance: bool = True
    enable_gdpr_compliance: bool = True
    enable_pci_compliance: bool = False


@dataclass
class ComponentStatus:
    """Status of a platform component"""
    component: PlatformComponent
    status: str  # healthy, degraded, unhealthy, disabled
    last_check: datetime
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = None


@dataclass
class PlatformHealth:
    """Overall platform health status"""
    overall_status: str  # healthy, degraded, unhealthy
    component_statuses: Dict[PlatformComponent, ComponentStatus]
    last_updated: datetime
    uptime_percentage: float
    total_dashboards: int
    total_metrics: int
    total_alerts: int
    active_users: int


class DataDogEnterprisePlatform:
    """
    Enterprise DataDog Platform Integration
    
    This is the main orchestration layer that provides:
    - Unified initialization and configuration of all monitoring components
    - Health monitoring and status reporting
    - Cross-component coordination and data flow
    - Enterprise-grade security and access control
    - Automated deployment and scaling
    - Comprehensive reporting and analytics
    - Disaster recovery and backup coordination
    - Performance optimization and resource management
    """
    
    def __init__(self, config: PlatformConfiguration):
        self.config = config
        self.service_name = f"datadog-enterprise-platform-{config.environment.value}"
        self.logger = get_logger(f"{__name__}.{self.service_name}")
        
        # Component instances
        self.components: Dict[PlatformComponent, Any] = {}
        self.component_health: Dict[PlatformComponent, ComponentStatus] = {}
        
        # Platform state
        self.initialization_complete = False
        self.startup_time = datetime.utcnow()
        self.last_health_check = None
        self.performance_metrics = {
            "total_requests": 0,
            "total_dashboards_created": 0,
            "total_alerts_triggered": 0,
            "average_response_time": 0.0,
            "error_rate": 0.0
        }
        
        # Background tasks
        self.background_tasks: List[asyncio.Task] = []
        
        self.logger.info(f"Enterprise DataDog Platform initialized for {config.environment.value}")
    
    async def initialize(self) -> bool:
        """Initialize all platform components"""
        
        try:
            self.logger.info("Starting DataDog Enterprise Platform initialization...")
            
            # Initialize core monitoring first
            await self._initialize_core_monitoring()
            
            # Initialize dashboard manager
            await self._initialize_dashboard_manager()
            
            # Initialize business metrics tracker
            await self._initialize_business_metrics()
            
            # Initialize custom metrics system
            await self._initialize_custom_metrics()
            
            # Initialize RBAC system if enabled
            if self.config.enable_rbac:
                await self._initialize_rbac_system()
            
            # Initialize alerting systems
            await self._initialize_intelligent_alerting()
            
            if self.config.enable_enterprise_alerting:
                await self._initialize_enterprise_alerting()
            
            # Initialize dashboard systems
            if self.config.enable_executive_dashboards:
                await self._initialize_executive_dashboards()
            
            if self.config.enable_technical_dashboards:
                await self._initialize_technical_dashboards()
            
            if self.config.enable_ml_dashboards:
                await self._initialize_ml_dashboards()
            
            # Initialize dashboard automation
            if self.config.enable_dashboard_automation:
                await self._initialize_dashboard_automation()
            
            # Start background services
            await self._start_background_services()
            
            # Perform initial health check
            await self._perform_health_check()
            
            self.initialization_complete = True
            
            # Create initial dashboards and alerts
            await self._deploy_initial_configuration()
            
            self.logger.info("DataDog Enterprise Platform initialization completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize DataDog Enterprise Platform: {str(e)}")
            return False
    
    async def _initialize_core_monitoring(self):
        """Initialize core DataDog monitoring"""
        
        try:
            self.components[PlatformComponent.CORE_MONITORING] = DatadogMonitoring(
                api_key=self.config.datadog_api_key,
                app_key=self.config.datadog_app_key,
                service_name=self.service_name,
                environment=self.config.environment.value,
                version=self.config.version
            )
            
            self._update_component_status(
                PlatformComponent.CORE_MONITORING,
                "healthy",
                {"api_key_configured": bool(self.config.datadog_api_key)}
            )
            
            self.logger.info("Core monitoring initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.CORE_MONITORING,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_dashboard_manager(self):
        """Initialize dashboard manager"""
        
        try:
            self.components[PlatformComponent.DASHBOARD_MANAGER] = DataDogDashboardManager(
                service_name=f"{self.service_name}-dashboard-manager",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING]
            )
            
            self._update_component_status(PlatformComponent.DASHBOARD_MANAGER, "healthy")
            self.logger.info("Dashboard manager initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.DASHBOARD_MANAGER,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_business_metrics(self):
        """Initialize business metrics tracker"""
        
        try:
            self.components[PlatformComponent.BUSINESS_METRICS] = DataDogBusinessMetricsTracker(
                service_name=f"{self.service_name}-business-metrics",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING]
            )
            
            self._update_component_status(PlatformComponent.BUSINESS_METRICS, "healthy")
            self.logger.info("Business metrics tracker initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.BUSINESS_METRICS,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_custom_metrics(self):
        """Initialize custom metrics system"""
        
        try:
            self.components[PlatformComponent.CUSTOM_METRICS] = DataDogCustomMetrics(
                service_name=f"{self.service_name}-custom-metrics",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING],
                business_metrics=self.components[PlatformComponent.BUSINESS_METRICS]
            )
            
            self._update_component_status(PlatformComponent.CUSTOM_METRICS, "healthy")
            self.logger.info("Custom metrics system initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.CUSTOM_METRICS,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_rbac_system(self):
        """Initialize RBAC system"""
        
        try:
            self.components[PlatformComponent.RBAC_SYSTEM] = DataDogRoleBasedAccess(
                service_name=f"{self.service_name}-rbac",
                config_path=f"./config/rbac/{self.config.environment.value}"
            )
            
            self._update_component_status(PlatformComponent.RBAC_SYSTEM, "healthy")
            self.logger.info("RBAC system initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.RBAC_SYSTEM,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_intelligent_alerting(self):
        """Initialize intelligent alerting system"""
        
        try:
            self.components[PlatformComponent.INTELLIGENT_ALERTING] = DataDogIntelligentAlerting(
                service_name=f"{self.service_name}-intelligent-alerting",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING],
                business_metrics=self.components[PlatformComponent.BUSINESS_METRICS]
            )
            
            self._update_component_status(PlatformComponent.INTELLIGENT_ALERTING, "healthy")
            self.logger.info("Intelligent alerting system initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.INTELLIGENT_ALERTING,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_enterprise_alerting(self):
        """Initialize enterprise alerting system"""
        
        try:
            self.components[PlatformComponent.ENTERPRISE_ALERTING] = DataDogEnterpriseAlerting(
                service_name=f"{self.service_name}-enterprise-alerting",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING],
                base_alerting=self.components[PlatformComponent.INTELLIGENT_ALERTING]
            )
            
            self._update_component_status(PlatformComponent.ENTERPRISE_ALERTING, "healthy")
            self.logger.info("Enterprise alerting system initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.ENTERPRISE_ALERTING,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_executive_dashboards(self):
        """Initialize executive dashboards"""
        
        try:
            self.components[PlatformComponent.EXECUTIVE_DASHBOARDS] = DataDogExecutiveDashboards(
                service_name=f"{self.service_name}-executive-dashboards",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING],
                dashboard_manager=self.components[PlatformComponent.DASHBOARD_MANAGER],
                business_metrics=self.components[PlatformComponent.BUSINESS_METRICS]
            )
            
            self._update_component_status(PlatformComponent.EXECUTIVE_DASHBOARDS, "healthy")
            self.logger.info("Executive dashboards initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.EXECUTIVE_DASHBOARDS,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_technical_dashboards(self):
        """Initialize technical operations dashboards"""
        
        try:
            self.components[PlatformComponent.TECHNICAL_DASHBOARDS] = DataDogTechnicalOperationsDashboards(
                service_name=f"{self.service_name}-technical-dashboards",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING],
                dashboard_manager=self.components[PlatformComponent.DASHBOARD_MANAGER]
            )
            
            self._update_component_status(PlatformComponent.TECHNICAL_DASHBOARDS, "healthy")
            self.logger.info("Technical operations dashboards initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.TECHNICAL_DASHBOARDS,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_ml_dashboards(self):
        """Initialize ML operations dashboards"""
        
        try:
            self.components[PlatformComponent.ML_DASHBOARDS] = DataDogMLOperationsDashboards(
                service_name=f"{self.service_name}-ml-dashboards",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING],
                dashboard_manager=self.components[PlatformComponent.DASHBOARD_MANAGER],
                business_metrics=self.components[PlatformComponent.BUSINESS_METRICS]
            )
            
            self._update_component_status(PlatformComponent.ML_DASHBOARDS, "healthy")
            self.logger.info("ML operations dashboards initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.ML_DASHBOARDS,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    async def _initialize_dashboard_automation(self):
        """Initialize dashboard automation system"""
        
        try:
            self.components[PlatformComponent.DASHBOARD_AUTOMATION] = DataDogDashboardAutomation(
                service_name=f"{self.service_name}-dashboard-automation",
                datadog_monitoring=self.components[PlatformComponent.CORE_MONITORING],
                dashboard_manager=self.components[PlatformComponent.DASHBOARD_MANAGER],
                config_path=f"./config/dashboards/{self.config.environment.value}"
            )
            
            self._update_component_status(PlatformComponent.DASHBOARD_AUTOMATION, "healthy")
            self.logger.info("Dashboard automation system initialized")
            
        except Exception as e:
            self._update_component_status(
                PlatformComponent.DASHBOARD_AUTOMATION,
                "unhealthy",
                error_message=str(e)
            )
            raise
    
    def _update_component_status(self, component: PlatformComponent, status: str,
                               metrics: Optional[Dict[str, Any]] = None,
                               error_message: Optional[str] = None):
        """Update component status"""
        
        self.component_health[component] = ComponentStatus(
            component=component,
            status=status,
            last_check=datetime.utcnow(),
            error_message=error_message,
            metrics=metrics
        )
    
    async def _start_background_services(self):
        """Start all background services"""
        
        try:
            # Health monitoring service
            self.background_tasks.append(
                asyncio.create_task(self._health_monitoring_service())
            )
            
            # Performance monitoring service
            self.background_tasks.append(
                asyncio.create_task(self._performance_monitoring_service())
            )
            
            # Metrics collection service
            self.background_tasks.append(
                asyncio.create_task(self._metrics_collection_service())
            )
            
            # Compliance monitoring service
            if self.config.enable_sox_compliance or self.config.enable_gdpr_compliance:
                self.background_tasks.append(
                    asyncio.create_task(self._compliance_monitoring_service())
                )
            
            self.logger.info(f"Started {len(self.background_tasks)} background services")
            
        except Exception as e:
            self.logger.error(f"Failed to start background services: {str(e)}")
            raise
    
    async def _deploy_initial_configuration(self):
        """Deploy initial dashboards, alerts, and configurations"""
        
        try:
            self.logger.info("Deploying initial configuration...")
            
            # Create executive dashboards
            if PlatformComponent.EXECUTIVE_DASHBOARDS in self.components:
                executive_dashboard = self.components[PlatformComponent.EXECUTIVE_DASHBOARDS]
                
                # Create CEO dashboard
                await executive_dashboard.create_executive_dashboard(
                    ExecutiveDashboardType.CEO_OVERVIEW,
                    custom_title=f"CEO Overview - {self.config.environment.value.upper()}"
                )
                
                # Create CFO dashboard  
                await executive_dashboard.create_executive_dashboard(
                    ExecutiveDashboardType.FINANCIAL_OVERVIEW,
                    custom_title=f"Financial Overview - {self.config.environment.value.upper()}"
                )
                
                await asyncio.sleep(2)  # Rate limiting
            
            # Create technical dashboards
            if PlatformComponent.TECHNICAL_DASHBOARDS in self.components:
                technical_dashboard = self.components[PlatformComponent.TECHNICAL_DASHBOARDS]
                
                # Create infrastructure overview
                await technical_dashboard.create_technical_dashboard(
                    TechnicalDashboardType.INFRASTRUCTURE_OVERVIEW,
                    custom_title=f"Infrastructure Overview - {self.config.environment.value.upper()}"
                )
                
                # Create API performance dashboard
                await technical_dashboard.create_technical_dashboard(
                    TechnicalDashboardType.APPLICATION_PERFORMANCE,
                    custom_title=f"API Performance - {self.config.environment.value.upper()}"
                )
                
                await asyncio.sleep(2)  # Rate limiting
            
            # Create ML dashboards
            if PlatformComponent.ML_DASHBOARDS in self.components:
                ml_dashboard = self.components[PlatformComponent.ML_DASHBOARDS]
                
                # Create model performance dashboard
                await ml_dashboard.create_ml_dashboard(
                    MLDashboardType.MODEL_PERFORMANCE,
                    custom_title=f"ML Model Performance - {self.config.environment.value.upper()}"
                )
                
                await asyncio.sleep(2)  # Rate limiting
            
            # Set up initial business metrics
            if PlatformComponent.BUSINESS_METRICS in self.components:
                business_metrics = self.components[PlatformComponent.BUSINESS_METRICS]
                
                # Track platform startup
                await business_metrics.track_kpi(
                    "platform_startup",
                    1.0,
                    tags={"environment": self.config.environment.value}
                )
            
            self.logger.info("Initial configuration deployment completed")
            
        except Exception as e:
            self.logger.error(f"Failed to deploy initial configuration: {str(e)}")
    
    async def _health_monitoring_service(self):
        """Background service for monitoring component health"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                await self._perform_health_check()
                
            except Exception as e:
                self.logger.error(f"Error in health monitoring service: {str(e)}")
                await asyncio.sleep(300)
    
    async def _perform_health_check(self):
        """Perform comprehensive health check of all components"""
        
        try:
            self.logger.debug("Performing platform health check...")
            
            healthy_components = 0
            total_components = len(self.components)
            
            for component_type, component_instance in self.components.items():
                try:
                    # Basic health check - check if component has required methods
                    if hasattr(component_instance, 'get_status'):
                        status_info = component_instance.get_status()
                        self._update_component_status(
                            component_type,
                            "healthy",
                            metrics=status_info
                        )
                    else:
                        self._update_component_status(component_type, "healthy")
                    
                    healthy_components += 1
                    
                except Exception as e:
                    self._update_component_status(
                        component_type,
                        "unhealthy",
                        error_message=str(e)
                    )
            
            # Update platform-wide health
            uptime = (datetime.utcnow() - self.startup_time).total_seconds()
            uptime_percentage = min(99.99, (healthy_components / total_components) * 100)
            
            overall_status = "healthy"
            if uptime_percentage < 90:
                overall_status = "unhealthy"
            elif uptime_percentage < 95:
                overall_status = "degraded"
            
            self.last_health_check = datetime.utcnow()
            
            # Send health metrics to DataDog
            if PlatformComponent.CORE_MONITORING in self.components:
                monitoring = self.components[PlatformComponent.CORE_MONITORING]
                
                monitoring.gauge(
                    "platform.health.uptime_percentage",
                    uptime_percentage,
                    tags=[
                        f"environment:{self.config.environment.value}",
                        f"overall_status:{overall_status}"
                    ]
                )
                
                monitoring.gauge(
                    "platform.components.healthy_count",
                    healthy_components,
                    tags=[f"environment:{self.config.environment.value}"]
                )
                
                monitoring.gauge(
                    "platform.components.total_count",
                    total_components,
                    tags=[f"environment:{self.config.environment.value}"]
                )
            
        except Exception as e:
            self.logger.error(f"Failed to perform health check: {str(e)}")
    
    async def _performance_monitoring_service(self):
        """Background service for monitoring platform performance"""
        
        while True:
            try:
                await asyncio.sleep(60)  # Monitor every minute
                
                # Calculate performance metrics
                current_time = datetime.utcnow()
                uptime_seconds = (current_time - self.startup_time).total_seconds()
                
                # Send performance metrics
                if PlatformComponent.CORE_MONITORING in self.components:
                    monitoring = self.components[PlatformComponent.CORE_MONITORING]
                    
                    monitoring.gauge(
                        "platform.performance.uptime_seconds",
                        uptime_seconds,
                        tags=[f"environment:{self.config.environment.value}"]
                    )
                    
                    for metric_name, value in self.performance_metrics.items():
                        monitoring.gauge(
                            f"platform.performance.{metric_name}",
                            value,
                            tags=[f"environment:{self.config.environment.value}"]
                        )
                
            except Exception as e:
                self.logger.error(f"Error in performance monitoring service: {str(e)}")
                await asyncio.sleep(60)
    
    async def _metrics_collection_service(self):
        """Background service for collecting platform metrics"""
        
        while True:
            try:
                await asyncio.sleep(self.config.dashboard_refresh_interval)
                
                # Collect metrics from all components
                total_dashboards = 0
                total_metrics = 0
                total_alerts = 0
                
                # Count dashboards
                if PlatformComponent.DASHBOARD_MANAGER in self.components:
                    dashboard_manager = self.components[PlatformComponent.DASHBOARD_MANAGER]
                    if hasattr(dashboard_manager, 'dashboard_count'):
                        total_dashboards = dashboard_manager.dashboard_count
                
                # Count metrics
                if PlatformComponent.CUSTOM_METRICS in self.components:
                    custom_metrics = self.components[PlatformComponent.CUSTOM_METRICS]
                    if hasattr(custom_metrics, 'get_metrics_summary'):
                        summary = custom_metrics.get_metrics_summary()
                        total_metrics = summary.get('total_metrics', 0)
                
                # Count alerts
                if PlatformComponent.INTELLIGENT_ALERTING in self.components:
                    alerting = self.components[PlatformComponent.INTELLIGENT_ALERTING]
                    if hasattr(alerting, 'get_alert_summary'):
                        summary = alerting.get_alert_summary()
                        total_alerts = summary.get('total_rules', 0)
                
                # Update performance metrics
                self.performance_metrics.update({
                    "total_dashboards": total_dashboards,
                    "total_metrics": total_metrics,
                    "total_alerts": total_alerts
                })
                
            except Exception as e:
                self.logger.error(f"Error in metrics collection service: {str(e)}")
                await asyncio.sleep(self.config.dashboard_refresh_interval)
    
    async def _compliance_monitoring_service(self):
        """Background service for compliance monitoring"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Check hourly
                
                compliance_metrics = {}
                
                # SOX Compliance
                if self.config.enable_sox_compliance:
                    compliance_metrics["sox_compliant"] = 1.0  # Simplified for demo
                
                # GDPR Compliance
                if self.config.enable_gdpr_compliance:
                    compliance_metrics["gdpr_compliant"] = 1.0  # Simplified for demo
                
                # PCI Compliance
                if self.config.enable_pci_compliance:
                    compliance_metrics["pci_compliant"] = 1.0  # Simplified for demo
                
                # Send compliance metrics
                if PlatformComponent.CORE_MONITORING in self.components:
                    monitoring = self.components[PlatformComponent.CORE_MONITORING]
                    
                    for metric_name, value in compliance_metrics.items():
                        monitoring.gauge(
                            f"platform.compliance.{metric_name}",
                            value,
                            tags=[f"environment:{self.config.environment.value}"]
                        )
                
            except Exception as e:
                self.logger.error(f"Error in compliance monitoring service: {str(e)}")
                await asyncio.sleep(3600)
    
    async def get_platform_health(self) -> PlatformHealth:
        """Get comprehensive platform health status"""
        
        try:
            # Calculate overall status
            healthy_count = sum(
                1 for status in self.component_health.values()
                if status.status == "healthy"
            )
            total_count = len(self.component_health)
            
            if total_count == 0:
                uptime_percentage = 0.0
                overall_status = "unhealthy"
            else:
                uptime_percentage = (healthy_count / total_count) * 100
                
                if uptime_percentage >= 95:
                    overall_status = "healthy"
                elif uptime_percentage >= 90:
                    overall_status = "degraded"
                else:
                    overall_status = "unhealthy"
            
            # Get active users count
            active_users = 0
            if PlatformComponent.RBAC_SYSTEM in self.components:
                rbac = self.components[PlatformComponent.RBAC_SYSTEM]
                if hasattr(rbac, 'get_rbac_summary'):
                    rbac_summary = rbac.get_rbac_summary()
                    active_users = rbac_summary.get('users', {}).get('active_users', 0)
            
            return PlatformHealth(
                overall_status=overall_status,
                component_statuses=self.component_health.copy(),
                last_updated=datetime.utcnow(),
                uptime_percentage=uptime_percentage,
                total_dashboards=self.performance_metrics.get("total_dashboards", 0),
                total_metrics=self.performance_metrics.get("total_metrics", 0),
                total_alerts=self.performance_metrics.get("total_alerts", 0),
                active_users=active_users
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get platform health: {str(e)}")
            return PlatformHealth(
                overall_status="unhealthy",
                component_statuses={},
                last_updated=datetime.utcnow(),
                uptime_percentage=0.0,
                total_dashboards=0,
                total_metrics=0,
                total_alerts=0,
                active_users=0
            )
    
    async def shutdown(self):
        """Gracefully shutdown the platform"""
        
        try:
            self.logger.info("Shutting down DataDog Enterprise Platform...")
            
            # Cancel all background tasks
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)
            
            # Shutdown individual components if they have shutdown methods
            for component_type, component_instance in self.components.items():
                try:
                    if hasattr(component_instance, 'shutdown'):
                        await component_instance.shutdown()
                    self.logger.debug(f"Shutdown {component_type.value}")
                except Exception as e:
                    self.logger.error(f"Error shutting down {component_type.value}: {str(e)}")
            
            # Send final metrics
            if PlatformComponent.CORE_MONITORING in self.components:
                monitoring = self.components[PlatformComponent.CORE_MONITORING]
                monitoring.gauge(
                    "platform.shutdown",
                    1.0,
                    tags=[f"environment:{self.config.environment.value}"]
                )
            
            self.logger.info("DataDog Enterprise Platform shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during platform shutdown: {str(e)}")
    
    def get_platform_summary(self) -> Dict[str, Any]:
        """Get comprehensive platform summary"""
        
        try:
            current_time = datetime.utcnow()
            uptime = current_time - self.startup_time
            
            # Component status summary
            status_counts = {}
            for status in self.component_health.values():
                status_counts[status.status] = status_counts.get(status.status, 0) + 1
            
            return {
                "timestamp": current_time.isoformat(),
                "environment": self.config.environment.value,
                "service_name": self.service_name,
                "version": self.config.version,
                "uptime": {
                    "seconds": uptime.total_seconds(),
                    "formatted": str(uptime)
                },
                "initialization": {
                    "complete": self.initialization_complete,
                    "startup_time": self.startup_time.isoformat()
                },
                "components": {
                    "total_components": len(self.components),
                    "enabled_components": list(self.components.keys()),
                    "status_distribution": status_counts,
                    "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None
                },
                "performance": self.performance_metrics.copy(),
                "configuration": {
                    "environment": self.config.environment.value,
                    "region": self.config.region,
                    "executive_dashboards_enabled": self.config.enable_executive_dashboards,
                    "technical_dashboards_enabled": self.config.enable_technical_dashboards,
                    "ml_dashboards_enabled": self.config.enable_ml_dashboards,
                    "enterprise_alerting_enabled": self.config.enable_enterprise_alerting,
                    "dashboard_automation_enabled": self.config.enable_dashboard_automation,
                    "rbac_enabled": self.config.enable_rbac
                },
                "compliance": {
                    "sox_compliance": self.config.enable_sox_compliance,
                    "gdpr_compliance": self.config.enable_gdpr_compliance,
                    "pci_compliance": self.config.enable_pci_compliance,
                    "audit_retention_days": self.config.audit_retention_days
                },
                "background_services": {
                    "active_tasks": len([t for t in self.background_tasks if not t.done()]),
                    "total_tasks": len(self.background_tasks)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate platform summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


@asynccontextmanager
async def create_enterprise_platform(config: PlatformConfiguration):
    """Context manager for creating and managing DataDog Enterprise Platform"""
    
    platform = DataDogEnterprisePlatform(config)
    
    try:
        # Initialize platform
        success = await platform.initialize()
        if not success:
            raise RuntimeError("Failed to initialize DataDog Enterprise Platform")
        
        yield platform
        
    finally:
        # Always shutdown gracefully
        await platform.shutdown()


# Convenience functions for quick setup

def create_development_config(api_key: str, app_key: str) -> PlatformConfiguration:
    """Create development environment configuration"""
    return PlatformConfiguration(
        environment=DeploymentEnvironment.DEVELOPMENT,
        datadog_api_key=api_key,
        datadog_app_key=app_key,
        service_name="datadog-enterprise-dev",
        version="1.0.0-dev",
        enable_rbac=False,  # Simplified for development
        audit_retention_days=30,
        enable_sox_compliance=False,
        enable_gdpr_compliance=False
    )


def create_production_config(api_key: str, app_key: str, region: str = "us1") -> PlatformConfiguration:
    """Create production environment configuration"""
    return PlatformConfiguration(
        environment=DeploymentEnvironment.PRODUCTION,
        datadog_api_key=api_key,
        datadog_app_key=app_key,
        service_name="datadog-enterprise-prod",
        version="1.0.0",
        region=region,
        enable_executive_dashboards=True,
        enable_technical_dashboards=True,
        enable_ml_dashboards=True,
        enable_enterprise_alerting=True,
        enable_dashboard_automation=True,
        enable_rbac=True,
        audit_retention_days=365,
        enable_sox_compliance=True,
        enable_gdpr_compliance=True,
        enable_pci_compliance=True
    )