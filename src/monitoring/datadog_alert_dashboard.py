"""
DataDog Alert Management Dashboard
Real-time alert monitoring, management, and analytics dashboard
with comprehensive incident tracking and team performance metrics
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Set, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, deque
import statistics
from concurrent.futures import ThreadPoolExecutor

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_comprehensive_alerting import DataDogComprehensiveAlerting
from monitoring.datadog_enterprise_alerting import DataDogEnterpriseAlerting
from monitoring.datadog_incident_management import DataDogIncidentManagement

logger = get_logger(__name__)


class DashboardView(Enum):
    """Dashboard view types"""
    REAL_TIME = "real_time"
    HISTORICAL = "historical"
    ANALYTICS = "analytics"
    TEAM_PERFORMANCE = "team_performance"
    SLA_TRACKING = "sla_tracking"
    BUSINESS_IMPACT = "business_impact"


class AlertStatus(Enum):
    """Alert status for dashboard display"""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"
    ESCALATED = "escalated"


class MetricTimeRange(Enum):
    """Time ranges for metrics"""
    LAST_HOUR = "1h"
    LAST_4_HOURS = "4h"
    LAST_24_HOURS = "24h"
    LAST_7_DAYS = "7d"
    LAST_30_DAYS = "30d"


@dataclass
class DashboardAlert:
    """Alert data structure for dashboard display"""
    alert_id: str
    rule_id: str
    rule_name: str
    severity: str
    status: AlertStatus
    current_value: float
    threshold: float
    created_at: datetime
    updated_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    assigned_to: Optional[str] = None
    business_impact: Optional[str] = None
    revenue_impact: Optional[float] = None
    customer_impact: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    related_incidents: List[str] = field(default_factory=list)
    remediation_actions: List[str] = field(default_factory=list)
    time_to_acknowledge: Optional[float] = None
    time_to_resolve: Optional[float] = None


@dataclass
class DashboardMetrics:
    """Metrics for dashboard display"""
    timestamp: datetime
    time_range: MetricTimeRange
    
    # Alert metrics
    total_alerts: int = 0
    active_alerts: int = 0
    critical_alerts: int = 0
    warning_alerts: int = 0
    resolved_alerts: int = 0
    
    # Performance metrics
    mean_time_to_acknowledge: float = 0.0
    mean_time_to_resolve: float = 0.0
    alert_resolution_rate: float = 0.0
    false_positive_rate: float = 0.0
    escalation_rate: float = 0.0
    
    # Business metrics
    total_business_impact: float = 0.0
    prevented_revenue_loss: float = 0.0
    affected_customers: int = 0
    sla_breach_count: int = 0
    
    # Team metrics
    alerts_by_team: Dict[str, int] = field(default_factory=dict)
    response_time_by_team: Dict[str, float] = field(default_factory=dict)
    resolution_rate_by_team: Dict[str, float] = field(default_factory=dict)


@dataclass
class DashboardConfig:
    """Dashboard configuration"""
    refresh_interval_seconds: int = 30
    max_alerts_displayed: int = 100
    default_time_range: MetricTimeRange = MetricTimeRange.LAST_24_HOURS
    auto_refresh_enabled: bool = True
    show_resolved_alerts: bool = True
    alert_grouping_enabled: bool = True
    real_time_updates: bool = True
    notification_sound_enabled: bool = False
    theme: str = "dark"  # dark, light
    timezone: str = "UTC"


class DataDogAlertDashboard:
    """
    Comprehensive DataDog Alert Management Dashboard
    
    Features:
    - Real-time alert status monitoring
    - Interactive alert management (acknowledge, resolve, escalate)
    - Alert history and trend analysis
    - MTTR and resolution time tracking
    - Team performance metrics
    - Alert fatigue analysis and optimization
    - Business impact visualization
    - SLA breach tracking and prediction
    - Automated alert grouping and correlation
    - Customizable views and filters
    - Export capabilities for reports
    """
    
    def __init__(self, service_name: str = "alert-dashboard",
                 datadog_monitoring: Optional[DatadogMonitoring] = None,
                 comprehensive_alerting: Optional[DataDogComprehensiveAlerting] = None,
                 enterprise_alerting: Optional[DataDogEnterpriseAlerting] = None,
                 incident_management: Optional[DataDogIncidentManagement] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.comprehensive_alerting = comprehensive_alerting
        self.enterprise_alerting = enterprise_alerting
        self.incident_management = incident_management
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Dashboard data
        self.alerts: Dict[str, DashboardAlert] = {}
        self.metrics_history: deque = deque(maxlen=1440)  # Store 24h of minute-level data
        self.current_metrics: Optional[DashboardMetrics] = None
        
        # Configuration
        self.config = DashboardConfig()
        
        # Real-time data streams
        self.alert_stream: deque = deque(maxlen=1000)
        self.metric_stream: deque = deque(maxlen=500)
        
        # Dashboard state
        self.active_filters: Dict[str, Any] = {}
        self.selected_alerts: Set[str] = set()
        self.current_view: DashboardView = DashboardView.REAL_TIME
        self.user_preferences: Dict[str, Any] = {}
        
        # Performance tracking
        self.dashboard_metrics = {
            "page_load_time_ms": 0.0,
            "data_refresh_time_ms": 0.0,
            "active_users": 0,
            "api_response_time_ms": 0.0
        }
        
        # Team and user tracking
        self.team_assignments: Dict[str, str] = {}  # alert_id -> team
        self.user_sessions: Dict[str, Dict[str, Any]] = {}
        
        # Initialize dashboard
        self._initialize_dashboard()
        
        # Start background services
        asyncio.create_task(self._start_dashboard_services())
        
        self.logger.info(f"Alert dashboard initialized for {service_name}")
    
    def _initialize_dashboard(self):
        """Initialize dashboard with default data and configuration"""
        
        try:
            # Initialize current metrics
            self.current_metrics = DashboardMetrics(
                timestamp=datetime.utcnow(),
                time_range=self.config.default_time_range
            )
            
            # Set default filters
            self.active_filters = {
                "severity": [],
                "status": [],
                "teams": [],
                "time_range": self.config.default_time_range.value,
                "search_query": ""
            }
            
            # Initialize team assignments (in production, this would come from configuration)
            self.team_assignments = {
                "infrastructure": ["database", "cache", "network"],
                "application": ["api", "frontend", "backend"],
                "security": ["auth", "firewall", "compliance"],
                "data": ["etl", "analytics", "ml"]
            }
            
            self.logger.info("Dashboard initialized with default configuration")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize dashboard: {str(e)}")
    
    async def _start_dashboard_services(self):
        """Start dashboard background services"""
        
        try:
            # Real-time data collection
            asyncio.create_task(self._real_time_data_service())
            
            # Metrics calculation
            asyncio.create_task(self._metrics_calculation_service())
            
            # Alert processing
            asyncio.create_task(self._alert_processing_service())
            
            # Performance monitoring
            asyncio.create_task(self._performance_monitoring_service())
            
            # User session tracking
            asyncio.create_task(self._user_session_service())
            
            self.logger.info("Dashboard services started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start dashboard services: {str(e)}")
    
    async def _real_time_data_service(self):
        """Real-time data collection service"""
        
        while True:
            try:
                await asyncio.sleep(self.config.refresh_interval_seconds)
                
                # Collect real-time alert data
                await self._collect_alert_data()
                
                # Update metrics
                await self._update_current_metrics()
                
                # Broadcast updates to connected clients
                await self._broadcast_updates()
                
            except Exception as e:
                self.logger.error(f"Error in real-time data service: {str(e)}")
                await asyncio.sleep(self.config.refresh_interval_seconds)
    
    async def _collect_alert_data(self):
        """Collect current alert data from alerting systems"""
        
        try:
            start_time = time.time()
            
            collected_alerts = {}
            
            # Collect from comprehensive alerting system
            if self.comprehensive_alerting:
                comprehensive_data = await self._get_comprehensive_alerts()
                collected_alerts.update(comprehensive_data)
            
            # Collect from enterprise alerting system
            if self.enterprise_alerting:
                enterprise_data = await self._get_enterprise_alerts()
                collected_alerts.update(enterprise_data)
            
            # Collect from incident management system
            if self.incident_management:
                incident_data = await self._get_incident_alerts()
                collected_alerts.update(incident_data)
            
            # Update alerts collection
            self.alerts.update(collected_alerts)
            
            # Remove old resolved alerts (older than 24 hours)
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            alerts_to_remove = [
                alert_id for alert_id, alert in self.alerts.items()
                if (alert.status == AlertStatus.RESOLVED and 
                    alert.resolved_at and alert.resolved_at < cutoff_time)
            ]
            
            for alert_id in alerts_to_remove:
                del self.alerts[alert_id]
            
            # Update collection time metric
            collection_time = (time.time() - start_time) * 1000
            self.dashboard_metrics["data_refresh_time_ms"] = collection_time
            
            # Add to alert stream for real-time updates
            self.alert_stream.append({
                "timestamp": datetime.utcnow().isoformat(),
                "alert_count": len(collected_alerts),
                "collection_time_ms": collection_time
            })
            
        except Exception as e:
            self.logger.error(f"Failed to collect alert data: {str(e)}")
    
    async def _get_comprehensive_alerts(self) -> Dict[str, DashboardAlert]:
        """Get alerts from comprehensive alerting system"""
        
        try:
            alerts = {}
            
            # In production, this would query the actual comprehensive alerting system
            # For demonstration, we'll simulate some alerts
            
            import random
            
            for i in range(random.randint(5, 20)):
                alert_id = f"comp_alert_{i}_{int(time.time())}"
                
                alert = DashboardAlert(
                    alert_id=alert_id,
                    rule_id=f"rule_{i % 5}",
                    rule_name=f"Comprehensive Rule {i % 5}",
                    severity=random.choice(["info", "warning", "critical", "emergency"]),
                    status=random.choice(list(AlertStatus)),
                    current_value=random.uniform(50, 100),
                    threshold=random.uniform(70, 90),
                    created_at=datetime.utcnow() - timedelta(minutes=random.randint(1, 120)),
                    updated_at=datetime.utcnow(),
                    business_impact=random.choice(["low", "medium", "high", "critical"]),
                    revenue_impact=random.uniform(0, 5000),
                    customer_impact=random.randint(0, 1000),
                    tags=[f"service:api_{i % 3}", f"env:prod", f"team:infrastructure"],
                    remediation_actions=[random.choice(["auto_scale", "restart_service", "cache_clear"])]
                )
                
                # Set acknowledged/resolved times based on status
                if alert.status in [AlertStatus.ACKNOWLEDGED, AlertStatus.RESOLVED]:
                    alert.acknowledged_at = alert.created_at + timedelta(minutes=random.randint(1, 30))
                    if alert.acknowledged_at:
                        alert.time_to_acknowledge = (alert.acknowledged_at - alert.created_at).total_seconds() / 60
                
                if alert.status == AlertStatus.RESOLVED:
                    alert.resolved_at = alert.acknowledged_at + timedelta(minutes=random.randint(5, 60))
                    if alert.resolved_at:
                        alert.time_to_resolve = (alert.resolved_at - alert.created_at).total_seconds() / 60
                
                alerts[alert_id] = alert
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Failed to get comprehensive alerts: {str(e)}")
            return {}
    
    async def _get_enterprise_alerts(self) -> Dict[str, DashboardAlert]:
        """Get alerts from enterprise alerting system"""
        
        try:
            alerts = {}
            
            # Simulate enterprise alerts
            import random
            
            for i in range(random.randint(3, 10)):
                alert_id = f"ent_alert_{i}_{int(time.time())}"
                
                alert = DashboardAlert(
                    alert_id=alert_id,
                    rule_id=f"enterprise_rule_{i % 3}",
                    rule_name=f"Enterprise Business Rule {i % 3}",
                    severity=random.choice(["warning", "critical", "emergency"]),
                    status=random.choice(list(AlertStatus)),
                    current_value=random.uniform(1000, 10000),
                    threshold=random.uniform(5000, 8000),
                    created_at=datetime.utcnow() - timedelta(minutes=random.randint(1, 60)),
                    updated_at=datetime.utcnow(),
                    business_impact=random.choice(["high", "critical", "catastrophic"]),
                    revenue_impact=random.uniform(1000, 10000),
                    customer_impact=random.randint(100, 5000),
                    tags=[f"business:revenue", f"env:prod", f"team:business"],
                    assigned_to=random.choice(["team_lead", "manager", "executive"])
                )
                
                alerts[alert_id] = alert
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Failed to get enterprise alerts: {str(e)}")
            return {}
    
    async def _get_incident_alerts(self) -> Dict[str, DashboardAlert]:
        """Get alerts associated with incidents"""
        
        try:
            alerts = {}
            
            if not self.incident_management:
                return alerts
            
            # Get active incidents and their associated alerts
            active_incidents = self.incident_management.list_active_incidents()
            
            for incident in active_incidents:
                incident_id = incident.get("incident_id")
                incident_alerts = incident.get("alerts", [])
                
                for alert_id in incident_alerts:
                    if alert_id not in self.alerts:
                        # Create dashboard alert from incident data
                        alert = DashboardAlert(
                            alert_id=alert_id,
                            rule_id=f"incident_{incident_id}",
                            rule_name=incident.get("title", "Incident Alert"),
                            severity=incident.get("severity", "warning"),
                            status=AlertStatus.ESCALATED,
                            current_value=0.0,
                            threshold=0.0,
                            created_at=datetime.fromisoformat(incident.get("created_at")),
                            updated_at=datetime.fromisoformat(incident.get("updated_at")),
                            business_impact=incident.get("priority", "medium"),
                            related_incidents=[incident_id],
                            assigned_to=incident.get("assigned_to"),
                            tags=[f"incident:{incident_id}"]
                        )
                        
                        alerts[alert_id] = alert
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Failed to get incident alerts: {str(e)}")
            return {}
    
    async def _update_current_metrics(self):
        """Update current dashboard metrics"""
        
        try:
            current_time = datetime.utcnow()
            time_range = MetricTimeRange(self.active_filters.get("time_range", "24h"))
            
            # Calculate time range for metrics
            time_cutoff = self._get_time_cutoff(time_range)
            
            # Filter alerts by time range
            filtered_alerts = [
                alert for alert in self.alerts.values()
                if alert.created_at >= time_cutoff
            ]
            
            # Calculate basic metrics
            total_alerts = len(filtered_alerts)
            active_alerts = len([a for a in filtered_alerts if a.status == AlertStatus.ACTIVE])
            critical_alerts = len([a for a in filtered_alerts if a.severity in ["critical", "emergency"]])
            warning_alerts = len([a for a in filtered_alerts if a.severity == "warning"])
            resolved_alerts = len([a for a in filtered_alerts if a.status == AlertStatus.RESOLVED])
            
            # Calculate performance metrics
            acknowledged_alerts = [a for a in filtered_alerts if a.time_to_acknowledge is not None]
            resolved_with_time = [a for a in filtered_alerts if a.time_to_resolve is not None]
            
            mean_time_to_acknowledge = statistics.mean([a.time_to_acknowledge for a in acknowledged_alerts]) if acknowledged_alerts else 0.0
            mean_time_to_resolve = statistics.mean([a.time_to_resolve for a in resolved_with_time]) if resolved_with_time else 0.0
            
            alert_resolution_rate = (resolved_alerts / total_alerts) if total_alerts > 0 else 0.0
            escalation_rate = len([a for a in filtered_alerts if a.status == AlertStatus.ESCALATED]) / total_alerts if total_alerts > 0 else 0.0
            
            # Calculate business metrics
            total_business_impact = sum(a.revenue_impact or 0 for a in filtered_alerts)
            prevented_revenue_loss = sum(
                a.revenue_impact or 0 for a in filtered_alerts
                if a.status == AlertStatus.RESOLVED and a.time_to_resolve and a.time_to_resolve < 30
            )
            affected_customers = sum(a.customer_impact or 0 for a in filtered_alerts if a.status == AlertStatus.ACTIVE)
            
            # Calculate team metrics
            alerts_by_team = defaultdict(int)
            response_times_by_team = defaultdict(list)
            resolution_rates_by_team = defaultdict(list)
            
            for alert in filtered_alerts:
                team = self._get_alert_team(alert)
                alerts_by_team[team] += 1
                
                if alert.time_to_acknowledge:
                    response_times_by_team[team].append(alert.time_to_acknowledge)
                
                if alert.status == AlertStatus.RESOLVED:
                    resolution_rates_by_team[team].append(1)
                else:
                    resolution_rates_by_team[team].append(0)
            
            response_time_by_team = {
                team: statistics.mean(times)
                for team, times in response_times_by_team.items()
                if times
            }
            
            resolution_rate_by_team = {
                team: statistics.mean(rates)
                for team, rates in resolution_rates_by_team.items()
                if rates
            }
            
            # Update current metrics
            self.current_metrics = DashboardMetrics(
                timestamp=current_time,
                time_range=time_range,
                total_alerts=total_alerts,
                active_alerts=active_alerts,
                critical_alerts=critical_alerts,
                warning_alerts=warning_alerts,
                resolved_alerts=resolved_alerts,
                mean_time_to_acknowledge=mean_time_to_acknowledge,
                mean_time_to_resolve=mean_time_to_resolve,
                alert_resolution_rate=alert_resolution_rate,
                escalation_rate=escalation_rate,
                total_business_impact=total_business_impact,
                prevented_revenue_loss=prevented_revenue_loss,
                affected_customers=affected_customers,
                alerts_by_team=dict(alerts_by_team),
                response_time_by_team=response_time_by_team,
                resolution_rate_by_team=resolution_rate_by_team
            )
            
            # Add to metrics history
            self.metrics_history.append(self.current_metrics)
            
            # Add to metric stream
            self.metric_stream.append({
                "timestamp": current_time.isoformat(),
                "active_alerts": active_alerts,
                "critical_alerts": critical_alerts,
                "mean_time_to_resolve": mean_time_to_resolve,
                "business_impact": total_business_impact
            })
            
        except Exception as e:
            self.logger.error(f"Failed to update current metrics: {str(e)}")
    
    def _get_time_cutoff(self, time_range: MetricTimeRange) -> datetime:
        """Get time cutoff for given time range"""
        
        current_time = datetime.utcnow()
        
        if time_range == MetricTimeRange.LAST_HOUR:
            return current_time - timedelta(hours=1)
        elif time_range == MetricTimeRange.LAST_4_HOURS:
            return current_time - timedelta(hours=4)
        elif time_range == MetricTimeRange.LAST_24_HOURS:
            return current_time - timedelta(hours=24)
        elif time_range == MetricTimeRange.LAST_7_DAYS:
            return current_time - timedelta(days=7)
        elif time_range == MetricTimeRange.LAST_30_DAYS:
            return current_time - timedelta(days=30)
        else:
            return current_time - timedelta(hours=24)
    
    def _get_alert_team(self, alert: DashboardAlert) -> str:
        """Determine team assignment for alert"""
        
        try:
            # Check tags for team information
            for tag in alert.tags:
                if tag.startswith("team:"):
                    return tag.split(":")[1]
            
            # Check service tags
            for tag in alert.tags:
                if tag.startswith("service:"):
                    service = tag.split(":")[1]
                    for team, services in self.team_assignments.items():
                        if any(svc in service for svc in services):
                            return team
            
            # Default team
            return "unassigned"
            
        except Exception as e:
            self.logger.error(f"Failed to get alert team: {str(e)}")
            return "unassigned"
    
    async def _broadcast_updates(self):
        """Broadcast real-time updates to connected clients"""
        
        try:
            # In production, this would use WebSockets or Server-Sent Events
            # to broadcast updates to connected dashboard clients
            
            update_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "alerts_count": len(self.alerts),
                "active_alerts": len([a for a in self.alerts.values() if a.status == AlertStatus.ACTIVE]),
                "critical_alerts": len([a for a in self.alerts.values() if a.severity in ["critical", "emergency"]]),
                "latest_metrics": asdict(self.current_metrics) if self.current_metrics else None
            }
            
            self.logger.debug(f"Broadcasting update: {json.dumps(update_data, default=str)[:200]}...")
            
        except Exception as e:
            self.logger.error(f"Failed to broadcast updates: {str(e)}")
    
    async def _metrics_calculation_service(self):
        """Background service for advanced metrics calculation"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Calculate every 5 minutes
                
                await self._calculate_advanced_metrics()
                await self._calculate_trend_analysis()
                await self._calculate_alert_fatigue_metrics()
                
            except Exception as e:
                self.logger.error(f"Error in metrics calculation service: {str(e)}")
                await asyncio.sleep(300)
    
    async def _calculate_advanced_metrics(self):
        """Calculate advanced analytics metrics"""
        
        try:
            if not self.current_metrics:
                return
            
            # Calculate alert velocity (alerts per hour)
            recent_alerts = [
                alert for alert in self.alerts.values()
                if alert.created_at >= datetime.utcnow() - timedelta(hours=1)
            ]
            alert_velocity = len(recent_alerts)
            
            # Calculate noise ratio (unresolved low-priority alerts)
            low_priority_alerts = [
                alert for alert in self.alerts.values()
                if alert.severity == "info" and alert.status == AlertStatus.ACTIVE
            ]
            noise_ratio = len(low_priority_alerts) / len(self.alerts) if self.alerts else 0
            
            # Update metrics
            if self.datadog_monitoring:
                self.datadog_monitoring.gauge("dashboard.alert_velocity", alert_velocity)
                self.datadog_monitoring.gauge("dashboard.noise_ratio", noise_ratio)
                self.datadog_monitoring.gauge("dashboard.active_alerts", self.current_metrics.active_alerts)
                self.datadog_monitoring.gauge("dashboard.mean_time_to_resolve", self.current_metrics.mean_time_to_resolve)
            
        except Exception as e:
            self.logger.error(f"Failed to calculate advanced metrics: {str(e)}")
    
    async def _calculate_trend_analysis(self):
        """Calculate trend analysis for alerts"""
        
        try:
            if len(self.metrics_history) < 10:
                return
            
            # Get recent metrics for trend analysis
            recent_metrics = list(self.metrics_history)[-10:]
            
            # Calculate trends
            alert_counts = [m.active_alerts for m in recent_metrics]
            response_times = [m.mean_time_to_acknowledge for m in recent_metrics if m.mean_time_to_acknowledge > 0]
            resolution_times = [m.mean_time_to_resolve for m in recent_metrics if m.mean_time_to_resolve > 0]
            
            # Simple trend calculation (positive = increasing, negative = decreasing)
            alert_trend = (alert_counts[-1] - alert_counts[0]) / len(alert_counts) if alert_counts else 0
            response_trend = (response_times[-1] - response_times[0]) / len(response_times) if len(response_times) > 1 else 0
            resolution_trend = (resolution_times[-1] - resolution_times[0]) / len(resolution_times) if len(resolution_times) > 1 else 0
            
            # Store trends for dashboard display
            self.dashboard_metrics.update({
                "alert_trend": alert_trend,
                "response_time_trend": response_trend,
                "resolution_time_trend": resolution_trend
            })
            
        except Exception as e:
            self.logger.error(f"Failed to calculate trend analysis: {str(e)}")
    
    async def _calculate_alert_fatigue_metrics(self):
        """Calculate alert fatigue metrics"""
        
        try:
            # Calculate repetitive alerts (same rule triggering frequently)
            rule_counts = defaultdict(int)
            for alert in self.alerts.values():
                if alert.created_at >= datetime.utcnow() - timedelta(hours=24):
                    rule_counts[alert.rule_id] += 1
            
            # Find noisy rules (more than 10 alerts in 24h)
            noisy_rules = {rule_id: count for rule_id, count in rule_counts.items() if count > 10}
            
            # Calculate alert-to-incident ratio
            total_alerts_24h = sum(rule_counts.values())
            total_incidents = len([
                alert for alert in self.alerts.values()
                if alert.related_incidents and alert.created_at >= datetime.utcnow() - timedelta(hours=24)
            ])
            
            alert_to_incident_ratio = total_alerts_24h / max(total_incidents, 1)
            
            # Store fatigue metrics
            fatigue_metrics = {
                "noisy_rules_count": len(noisy_rules),
                "noisiest_rule": max(rule_counts.items(), key=lambda x: x[1]) if rule_counts else ("none", 0),
                "alert_to_incident_ratio": alert_to_incident_ratio,
                "repetitive_alert_percentage": len(noisy_rules) / len(rule_counts) if rule_counts else 0
            }
            
            self.dashboard_metrics.update(fatigue_metrics)
            
        except Exception as e:
            self.logger.error(f"Failed to calculate alert fatigue metrics: {str(e)}")
    
    async def _alert_processing_service(self):
        """Background service for alert processing"""
        
        while True:
            try:
                await asyncio.sleep(60)  # Process every minute
                
                await self._process_alert_actions()
                await self._check_sla_breaches()
                await self._update_alert_priorities()
                
            except Exception as e:
                self.logger.error(f"Error in alert processing service: {str(e)}")
                await asyncio.sleep(60)
    
    async def _process_alert_actions(self):
        """Process pending alert actions"""
        
        try:
            # Process automated actions for active alerts
            active_alerts = [alert for alert in self.alerts.values() if alert.status == AlertStatus.ACTIVE]
            
            for alert in active_alerts:
                # Check if alert should be auto-escalated
                if alert.severity in ["critical", "emergency"]:
                    time_since_created = (datetime.utcnow() - alert.created_at).total_seconds() / 60
                    if time_since_created > 15 and not alert.acknowledged_at:  # 15 minutes without acknowledgment
                        await self._escalate_alert(alert.alert_id)
                
                # Check for auto-remediation
                if alert.remediation_actions and alert.severity == "critical":
                    await self._trigger_auto_remediation(alert)
            
        except Exception as e:
            self.logger.error(f"Failed to process alert actions: {str(e)}")
    
    async def _check_sla_breaches(self):
        """Check for SLA breaches"""
        
        try:
            current_time = datetime.utcnow()
            
            for alert in self.alerts.values():
                if alert.status not in [AlertStatus.RESOLVED]:
                    # Define SLA targets based on severity
                    sla_targets = {
                        "emergency": timedelta(minutes=5),
                        "critical": timedelta(minutes=15),
                        "warning": timedelta(hours=4),
                        "info": timedelta(hours=24)
                    }
                    
                    sla_target = sla_targets.get(alert.severity, timedelta(hours=24))
                    time_since_created = current_time - alert.created_at
                    
                    if time_since_created > sla_target and alert.status != AlertStatus.ESCALATED:
                        await self._handle_sla_breach(alert)
            
        except Exception as e:
            self.logger.error(f"Failed to check SLA breaches: {str(e)}")
    
    async def _handle_sla_breach(self, alert: DashboardAlert):
        """Handle SLA breach for alert"""
        
        try:
            self.logger.warning(f"SLA breach detected for alert {alert.alert_id}")
            
            # Update alert status
            alert.status = AlertStatus.ESCALATED
            alert.updated_at = datetime.utcnow()
            
            # Send escalation notification (in production, this would trigger actual notifications)
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "dashboard.sla_breach",
                    tags=[
                        f"alert_id:{alert.alert_id}",
                        f"severity:{alert.severity}",
                        f"rule_id:{alert.rule_id}"
                    ]
                )
            
            # Update metrics
            if self.current_metrics:
                self.current_metrics.sla_breach_count += 1
            
        except Exception as e:
            self.logger.error(f"Failed to handle SLA breach: {str(e)}")
    
    async def _update_alert_priorities(self):
        """Update alert priorities based on business impact"""
        
        try:
            for alert in self.alerts.values():
                if alert.status == AlertStatus.ACTIVE:
                    # Calculate dynamic priority based on business impact
                    priority_score = 0
                    
                    # Severity impact
                    severity_scores = {"emergency": 100, "critical": 75, "warning": 50, "info": 25}
                    priority_score += severity_scores.get(alert.severity, 25)
                    
                    # Business impact
                    if alert.revenue_impact:
                        if alert.revenue_impact > 10000:
                            priority_score += 50
                        elif alert.revenue_impact > 5000:
                            priority_score += 30
                        elif alert.revenue_impact > 1000:
                            priority_score += 15
                    
                    # Customer impact
                    if alert.customer_impact:
                        if alert.customer_impact > 1000:
                            priority_score += 30
                        elif alert.customer_impact > 500:
                            priority_score += 20
                        elif alert.customer_impact > 100:
                            priority_score += 10
                    
                    # Time factor (older alerts get higher priority)
                    hours_old = (datetime.utcnow() - alert.created_at).total_seconds() / 3600
                    priority_score += min(hours_old * 5, 25)  # Max 25 points for time
                    
                    # Store priority score for sorting/display
                    alert.tags = [tag for tag in alert.tags if not tag.startswith("priority_score:")]
                    alert.tags.append(f"priority_score:{int(priority_score)}")
            
        except Exception as e:
            self.logger.error(f"Failed to update alert priorities: {str(e)}")
    
    async def _escalate_alert(self, alert_id: str):
        """Escalate alert to higher tier"""
        
        try:
            alert = self.alerts.get(alert_id)
            if not alert:
                return
            
            alert.status = AlertStatus.ESCALATED
            alert.updated_at = datetime.utcnow()
            
            self.logger.warning(f"Alert escalated: {alert_id}")
            
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "dashboard.alert_escalated",
                    tags=[f"alert_id:{alert_id}", f"severity:{alert.severity}"]
                )
            
        except Exception as e:
            self.logger.error(f"Failed to escalate alert: {str(e)}")
    
    async def _trigger_auto_remediation(self, alert: DashboardAlert):
        """Trigger auto-remediation for alert"""
        
        try:
            for action in alert.remediation_actions:
                self.logger.info(f"Triggering auto-remediation: {action} for alert {alert.alert_id}")
                
                # In production, this would trigger actual remediation
                await asyncio.sleep(1)  # Simulate action time
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "dashboard.auto_remediation_triggered",
                        tags=[
                            f"alert_id:{alert.alert_id}",
                            f"action:{action}"
                        ]
                    )
            
        except Exception as e:
            self.logger.error(f"Failed to trigger auto-remediation: {str(e)}")
    
    async def _performance_monitoring_service(self):
        """Background service for dashboard performance monitoring"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Monitor every 5 minutes
                
                # Monitor dashboard performance
                await self._monitor_dashboard_performance()
                
            except Exception as e:
                self.logger.error(f"Error in performance monitoring service: {str(e)}")
                await asyncio.sleep(300)
    
    async def _monitor_dashboard_performance(self):
        """Monitor dashboard performance metrics"""
        
        try:
            # Simulate performance metrics
            import random
            
            self.dashboard_metrics.update({
                "page_load_time_ms": random.uniform(100, 500),
                "api_response_time_ms": random.uniform(50, 200),
                "memory_usage_mb": random.uniform(100, 300),
                "cpu_usage_percent": random.uniform(5, 25)
            })
            
            if self.datadog_monitoring:
                for metric, value in self.dashboard_metrics.items():
                    if isinstance(value, (int, float)):
                        self.datadog_monitoring.gauge(f"dashboard.{metric}", value)
            
        except Exception as e:
            self.logger.error(f"Failed to monitor dashboard performance: {str(e)}")
    
    async def _user_session_service(self):
        """Background service for user session tracking"""
        
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Clean up inactive user sessions
                current_time = datetime.utcnow()
                inactive_sessions = []
                
                for user_id, session in self.user_sessions.items():
                    last_activity = datetime.fromisoformat(session.get("last_activity", current_time.isoformat()))
                    if current_time - last_activity > timedelta(minutes=30):  # 30 minutes inactive
                        inactive_sessions.append(user_id)
                
                for user_id in inactive_sessions:
                    del self.user_sessions[user_id]
                
                # Update active users count
                self.dashboard_metrics["active_users"] = len(self.user_sessions)
                
            except Exception as e:
                self.logger.error(f"Error in user session service: {str(e)}")
                await asyncio.sleep(60)
    
    # Public API methods for dashboard interaction
    
    def get_dashboard_data(self, user_id: str = "anonymous") -> Dict[str, Any]:
        """Get current dashboard data for display"""
        
        try:
            # Update user session
            self.user_sessions[user_id] = {
                "last_activity": datetime.utcnow().isoformat(),
                "view": self.current_view.value
            }
            
            # Apply filters to alerts
            filtered_alerts = self._apply_filters(list(self.alerts.values()))
            
            # Sort alerts by priority
            filtered_alerts.sort(key=lambda a: self._get_alert_priority_score(a), reverse=True)
            
            # Limit number of alerts displayed
            if len(filtered_alerts) > self.config.max_alerts_displayed:
                filtered_alerts = filtered_alerts[:self.config.max_alerts_displayed]
            
            dashboard_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "current_view": self.current_view.value,
                "alerts": [asdict(alert) for alert in filtered_alerts],
                "metrics": asdict(self.current_metrics) if self.current_metrics else None,
                "filters": self.active_filters,
                "config": asdict(self.config),
                "performance": self.dashboard_metrics,
                "real_time_stream": list(self.alert_stream)[-20:],  # Last 20 updates
                "metric_stream": list(self.metric_stream)[-20:],
                "trends": {
                    "alert_trend": self.dashboard_metrics.get("alert_trend", 0),
                    "response_time_trend": self.dashboard_metrics.get("response_time_trend", 0),
                    "resolution_time_trend": self.dashboard_metrics.get("resolution_time_trend", 0)
                }
            }
            
            return dashboard_data
            
        except Exception as e:
            self.logger.error(f"Failed to get dashboard data: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}
    
    def _apply_filters(self, alerts: List[DashboardAlert]) -> List[DashboardAlert]:
        """Apply active filters to alert list"""
        
        try:
            filtered_alerts = alerts
            
            # Severity filter
            if self.active_filters.get("severity"):
                filtered_alerts = [
                    a for a in filtered_alerts
                    if a.severity in self.active_filters["severity"]
                ]
            
            # Status filter
            if self.active_filters.get("status"):
                filtered_alerts = [
                    a for a in filtered_alerts
                    if a.status.value in self.active_filters["status"]
                ]
            
            # Team filter
            if self.active_filters.get("teams"):
                filtered_alerts = [
                    a for a in filtered_alerts
                    if self._get_alert_team(a) in self.active_filters["teams"]
                ]
            
            # Search query
            if self.active_filters.get("search_query"):
                query = self.active_filters["search_query"].lower()
                filtered_alerts = [
                    a for a in filtered_alerts
                    if (query in a.rule_name.lower() or
                        query in a.alert_id.lower() or
                        any(query in tag.lower() for tag in a.tags))
                ]
            
            # Time range filter
            time_range = MetricTimeRange(self.active_filters.get("time_range", "24h"))
            time_cutoff = self._get_time_cutoff(time_range)
            filtered_alerts = [
                a for a in filtered_alerts
                if a.created_at >= time_cutoff
            ]
            
            return filtered_alerts
            
        except Exception as e:
            self.logger.error(f"Failed to apply filters: {str(e)}")
            return alerts
    
    def _get_alert_priority_score(self, alert: DashboardAlert) -> float:
        """Get alert priority score for sorting"""
        
        try:
            for tag in alert.tags:
                if tag.startswith("priority_score:"):
                    return float(tag.split(":")[1])
            
            # Default priority based on severity
            severity_scores = {"emergency": 100, "critical": 75, "warning": 50, "info": 25}
            return severity_scores.get(alert.severity, 25)
            
        except Exception as e:
            self.logger.error(f"Failed to get alert priority score: {str(e)}")
            return 0.0
    
    def update_filters(self, filters: Dict[str, Any]) -> bool:
        """Update active filters"""
        
        try:
            self.active_filters.update(filters)
            self.logger.info(f"Updated filters: {json.dumps(filters)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update filters: {str(e)}")
            return False
    
    def acknowledge_alert(self, alert_id: str, user_id: str) -> bool:
        """Acknowledge alert"""
        
        try:
            alert = self.alerts.get(alert_id)
            if not alert:
                return False
            
            if alert.status == AlertStatus.ACTIVE:
                alert.status = AlertStatus.ACKNOWLEDGED
                alert.acknowledged_at = datetime.utcnow()
                alert.updated_at = datetime.utcnow()
                alert.assigned_to = user_id
                
                # Calculate time to acknowledge
                alert.time_to_acknowledge = (
                    alert.acknowledged_at - alert.created_at
                ).total_seconds() / 60
                
                self.logger.info(f"Alert acknowledged: {alert_id} by {user_id}")
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "dashboard.alert_acknowledged",
                        tags=[f"alert_id:{alert_id}", f"user_id:{user_id}"]
                    )
                
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to acknowledge alert: {str(e)}")
            return False
    
    def resolve_alert(self, alert_id: str, user_id: str, resolution_note: str = "") -> bool:
        """Resolve alert"""
        
        try:
            alert = self.alerts.get(alert_id)
            if not alert:
                return False
            
            if alert.status in [AlertStatus.ACTIVE, AlertStatus.ACKNOWLEDGED, AlertStatus.ESCALATED]:
                alert.status = AlertStatus.RESOLVED
                alert.resolved_at = datetime.utcnow()
                alert.updated_at = datetime.utcnow()
                alert.assigned_to = user_id
                
                # Calculate time to resolve
                alert.time_to_resolve = (
                    alert.resolved_at - alert.created_at
                ).total_seconds() / 60
                
                if not alert.acknowledged_at:
                    alert.acknowledged_at = alert.resolved_at
                    alert.time_to_acknowledge = alert.time_to_resolve
                
                self.logger.info(f"Alert resolved: {alert_id} by {user_id}")
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "dashboard.alert_resolved",
                        tags=[f"alert_id:{alert_id}", f"user_id:{user_id}"]
                    )
                    
                    self.datadog_monitoring.histogram(
                        "dashboard.time_to_resolve",
                        alert.time_to_resolve,
                        tags=[f"severity:{alert.severity}"]
                    )
                
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to resolve alert: {str(e)}")
            return False
    
    def get_alert_history(self, time_range: MetricTimeRange = MetricTimeRange.LAST_7_DAYS) -> Dict[str, Any]:
        """Get alert history for analytics"""
        
        try:
            time_cutoff = self._get_time_cutoff(time_range)
            
            historical_alerts = [
                alert for alert in self.alerts.values()
                if alert.created_at >= time_cutoff
            ]
            
            # Group by time periods
            time_series_data = defaultdict(lambda: {
                "total": 0, "critical": 0, "resolved": 0, "avg_resolution_time": 0
            })
            
            for alert in historical_alerts:
                # Group by hour for recent data, by day for older data
                if time_range in [MetricTimeRange.LAST_HOUR, MetricTimeRange.LAST_4_HOURS]:
                    time_key = alert.created_at.strftime("%Y-%m-%d %H:00")
                else:
                    time_key = alert.created_at.strftime("%Y-%m-%d")
                
                time_series_data[time_key]["total"] += 1
                
                if alert.severity in ["critical", "emergency"]:
                    time_series_data[time_key]["critical"] += 1
                
                if alert.status == AlertStatus.RESOLVED:
                    time_series_data[time_key]["resolved"] += 1
                    if alert.time_to_resolve:
                        current_avg = time_series_data[time_key]["avg_resolution_time"]
                        current_count = time_series_data[time_key]["resolved"]
                        time_series_data[time_key]["avg_resolution_time"] = (
                            (current_avg * (current_count - 1) + alert.time_to_resolve) / current_count
                        )
            
            return {
                "time_range": time_range.value,
                "total_alerts": len(historical_alerts),
                "time_series": dict(time_series_data),
                "summary": {
                    "critical_alerts": len([a for a in historical_alerts if a.severity in ["critical", "emergency"]]),
                    "resolved_alerts": len([a for a in historical_alerts if a.status == AlertStatus.RESOLVED]),
                    "average_resolution_time": statistics.mean([
                        a.time_to_resolve for a in historical_alerts
                        if a.time_to_resolve is not None
                    ]) if historical_alerts else 0
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get alert history: {str(e)}")
            return {"error": str(e)}
    
    def get_team_performance(self, time_range: MetricTimeRange = MetricTimeRange.LAST_7_DAYS) -> Dict[str, Any]:
        """Get team performance metrics"""
        
        try:
            time_cutoff = self._get_time_cutoff(time_range)
            
            team_alerts = defaultdict(list)
            
            for alert in self.alerts.values():
                if alert.created_at >= time_cutoff:
                    team = self._get_alert_team(alert)
                    team_alerts[team].append(alert)
            
            team_performance = {}
            
            for team, alerts in team_alerts.items():
                acknowledged_alerts = [a for a in alerts if a.time_to_acknowledge is not None]
                resolved_alerts = [a for a in alerts if a.time_to_resolve is not None]
                
                team_performance[team] = {
                    "total_alerts": len(alerts),
                    "critical_alerts": len([a for a in alerts if a.severity in ["critical", "emergency"]]),
                    "resolved_alerts": len(resolved_alerts),
                    "resolution_rate": len(resolved_alerts) / len(alerts) if alerts else 0,
                    "avg_response_time": statistics.mean([a.time_to_acknowledge for a in acknowledged_alerts]) if acknowledged_alerts else 0,
                    "avg_resolution_time": statistics.mean([a.time_to_resolve for a in resolved_alerts]) if resolved_alerts else 0,
                    "escalation_rate": len([a for a in alerts if a.status == AlertStatus.ESCALATED]) / len(alerts) if alerts else 0
                }
            
            return {
                "time_range": time_range.value,
                "teams": dict(team_performance),
                "overall": {
                    "total_teams": len(team_performance),
                    "best_response_time": min([perf["avg_response_time"] for perf in team_performance.values() if perf["avg_response_time"] > 0], default=0),
                    "best_resolution_rate": max([perf["resolution_rate"] for perf in team_performance.values()], default=0)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get team performance: {str(e)}")
            return {"error": str(e)}
    
    def export_dashboard_data(self, format_type: str = "json") -> str:
        """Export dashboard data for reporting"""
        
        try:
            export_data = {
                "export_timestamp": datetime.utcnow().isoformat(),
                "dashboard_summary": self.get_dashboard_data(),
                "alert_history": self.get_alert_history(),
                "team_performance": self.get_team_performance(),
                "system_metrics": self.dashboard_metrics
            }
            
            if format_type.lower() == "json":
                return json.dumps(export_data, indent=2, default=str)
            elif format_type.lower() == "csv":
                # In production, this would generate proper CSV
                return "CSV export not implemented in this example"
            else:
                return json.dumps(export_data, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to export dashboard data: {str(e)}")
            return json.dumps({"error": str(e)})


# Global instance
_alert_dashboard: Optional[DataDogAlertDashboard] = None


def get_alert_dashboard(
    service_name: str = "alert-dashboard",
    datadog_monitoring: Optional[DatadogMonitoring] = None,
    comprehensive_alerting: Optional[DataDogComprehensiveAlerting] = None,
    enterprise_alerting: Optional[DataDogEnterpriseAlerting] = None,
    incident_management: Optional[DataDogIncidentManagement] = None
) -> DataDogAlertDashboard:
    """Get or create alert dashboard instance"""
    global _alert_dashboard
    
    if _alert_dashboard is None:
        _alert_dashboard = DataDogAlertDashboard(
            service_name, datadog_monitoring, comprehensive_alerting,
            enterprise_alerting, incident_management
        )
    
    return _alert_dashboard


def get_dashboard_summary() -> Dict[str, Any]:
    """Get dashboard summary"""
    dashboard = get_alert_dashboard()
    return dashboard.get_dashboard_data()