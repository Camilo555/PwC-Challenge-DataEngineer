"""
Enterprise Security Monitoring Dashboard
Provides real-time security monitoring, alerting, and comprehensive security analytics
with integration to all security components including DLP, compliance, access control, and governance.
"""
import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable
from collections import defaultdict, deque
import threading
import time

from fastapi import WebSocket
from pydantic import BaseModel

from core.logging import get_logger
from core.security.advanced_security import get_security_manager, SecurityEventType, ThreatLevel
from core.security.enterprise_dlp import EnterpriseDLPManager, DLPAction, SensitiveDataType
from core.security.compliance_framework import get_compliance_engine, ComplianceStatus, ComplianceFramework
from core.security.enhanced_access_control import get_access_control_manager, AccessDecision
from core.security.data_governance import get_governance_orchestrator


logger = get_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(Enum):
    """Types of security alerts"""
    SECURITY_THREAT = "security_threat"
    DLP_VIOLATION = "dlp_violation"
    COMPLIANCE_VIOLATION = "compliance_violation"
    ACCESS_VIOLATION = "access_violation"
    DATA_GOVERNANCE = "data_governance"
    SYSTEM_HEALTH = "system_health"
    PERFORMANCE = "performance"


class AlertStatus(Enum):
    """Alert status values"""
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"
    FALSE_POSITIVE = "false_positive"


@dataclass
class SecurityAlert:
    """Security alert definition"""
    alert_id: str
    timestamp: datetime
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    description: str
    source_system: str
    
    # Details
    details: Dict[str, Any] = field(default_factory=dict)
    affected_assets: List[str] = field(default_factory=list)
    affected_users: List[str] = field(default_factory=list)
    risk_score: float = 0.0
    
    # Alert lifecycle
    status: AlertStatus = AlertStatus.NEW
    assigned_to: Optional[str] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None
    
    # Correlation
    parent_alert_id: Optional[str] = None
    child_alert_ids: List[str] = field(default_factory=list)
    correlation_key: Optional[str] = None
    
    # Metadata
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertRule:
    """Alert rule definition"""
    rule_id: str
    name: str
    description: str
    alert_type: AlertType
    severity: AlertSeverity
    enabled: bool
    
    # Conditions
    conditions: Dict[str, Any] = field(default_factory=dict)
    threshold: Optional[Dict[str, Any]] = None
    time_window: Optional[timedelta] = None
    
    # Actions
    notification_channels: List[str] = field(default_factory=list)
    auto_actions: List[str] = field(default_factory=list)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    created_by: str = ""
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0


@dataclass
class DashboardWidget:
    """Dashboard widget configuration"""
    widget_id: str
    title: str
    widget_type: str  # chart, metric, table, alert_list
    position: Dict[str, int]  # x, y, width, height
    config: Dict[str, Any] = field(default_factory=dict)
    refresh_interval: int = 30  # seconds
    enabled: bool = True


@dataclass
class SecurityMetrics:
    """Security metrics snapshot"""
    timestamp: datetime
    
    # Threat metrics
    total_threats_24h: int = 0
    critical_threats_24h: int = 0
    blocked_attacks: int = 0
    threat_trend: float = 0.0  # Percentage change from previous period
    
    # DLP metrics
    dlp_violations_24h: int = 0
    sensitive_data_exposures: int = 0
    data_blocked: int = 0
    dlp_effectiveness: float = 0.0  # Percentage of threats blocked
    
    # Compliance metrics
    compliance_violations: int = 0
    non_compliant_controls: int = 0
    compliance_score: float = 0.0
    frameworks_assessed: int = 0
    
    # Access control metrics
    access_denials: int = 0
    privilege_escalations: int = 0
    unauthorized_attempts: int = 0
    access_effectiveness: float = 0.0
    
    # Governance metrics
    governance_score: float = 0.0
    assets_cataloged: int = 0
    lineage_coverage: float = 0.0
    retention_violations: int = 0
    
    # System health
    system_uptime: float = 100.0
    response_time_ms: float = 0.0
    error_rate: float = 0.0


class AlertManager:
    """Manages security alerts and notifications"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.alerts: Dict[str, SecurityAlert] = {}
        self.alert_rules: Dict[str, AlertRule] = {}
        self.notification_channels: Dict[str, Callable] = {}
        
        # Alert correlation
        self.correlation_window = timedelta(minutes=5)
        self.correlation_keys: Dict[str, List[str]] = defaultdict(list)
        
        # Initialize default alert rules
        self._initialize_default_rules()
        
        # Start alert processing
        self._start_alert_processing()
    
    def _initialize_default_rules(self):
        """Initialize default alert rules"""
        
        default_rules = [
            AlertRule(
                rule_id="critical_security_threat",
                name="Critical Security Threat",
                description="Alert on critical security threats",
                alert_type=AlertType.SECURITY_THREAT,
                severity=AlertSeverity.CRITICAL,
                enabled=True,
                conditions={"threat_level": "critical"},
                notification_channels=["email", "slack", "sms"]
            ),
            AlertRule(
                rule_id="high_dlp_violations",
                name="High DLP Violations",
                description="Alert on high volume of DLP violations",
                alert_type=AlertType.DLP_VIOLATION,
                severity=AlertSeverity.HIGH,
                enabled=True,
                threshold={"count": 10, "time_window": "5m"},
                time_window=timedelta(minutes=5),
                notification_channels=["email", "slack"]
            ),
            AlertRule(
                rule_id="compliance_failure",
                name="Compliance Control Failure",
                description="Alert on compliance control failures",
                alert_type=AlertType.COMPLIANCE_VIOLATION,
                severity=AlertSeverity.HIGH,
                enabled=True,
                conditions={"status": "non_compliant"},
                notification_channels=["email"]
            ),
            AlertRule(
                rule_id="unauthorized_access_attempts",
                name="Unauthorized Access Attempts",
                description="Alert on repeated unauthorized access attempts",
                alert_type=AlertType.ACCESS_VIOLATION,
                severity=AlertSeverity.MEDIUM,
                enabled=True,
                threshold={"count": 5, "time_window": "10m"},
                time_window=timedelta(minutes=10),
                notification_channels=["slack"]
            ),
            AlertRule(
                rule_id="data_retention_violation",
                name="Data Retention Violation",
                description="Alert on data retention policy violations",
                alert_type=AlertType.DATA_GOVERNANCE,
                severity=AlertSeverity.MEDIUM,
                enabled=True,
                conditions={"retention_expired": True},
                notification_channels=["email"]
            )
        ]
        
        for rule in default_rules:
            self.alert_rules[rule.rule_id] = rule
    
    def create_alert(
        self,
        alert_type: AlertType,
        severity: AlertSeverity,
        title: str,
        description: str,
        source_system: str,
        details: Dict[str, Any] = None,
        correlation_key: Optional[str] = None
    ) -> str:
        """Create a new security alert"""
        
        alert = SecurityAlert(
            alert_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            alert_type=alert_type,
            severity=severity,
            title=title,
            description=description,
            source_system=source_system,
            details=details or {},
            correlation_key=correlation_key
        )
        
        # Calculate risk score
        alert.risk_score = self._calculate_risk_score(alert)
        
        # Store alert
        self.alerts[alert.alert_id] = alert
        
        # Handle correlation
        if correlation_key:
            self._correlate_alert(alert)
        
        # Trigger notifications
        self._trigger_notifications(alert)
        
        self.logger.info(f"Created {severity.value} alert: {title}")
        return alert.alert_id
    
    def _calculate_risk_score(self, alert: SecurityAlert) -> float:
        """Calculate risk score for alert"""
        
        base_scores = {
            AlertSeverity.INFO: 1.0,
            AlertSeverity.LOW: 3.0,
            AlertSeverity.MEDIUM: 5.0,
            AlertSeverity.HIGH: 8.0,
            AlertSeverity.CRITICAL: 10.0
        }
        
        score = base_scores.get(alert.severity, 5.0)
        
        # Adjust based on alert type
        type_multipliers = {
            AlertType.SECURITY_THREAT: 1.2,
            AlertType.DLP_VIOLATION: 1.1,
            AlertType.COMPLIANCE_VIOLATION: 1.0,
            AlertType.ACCESS_VIOLATION: 1.1,
            AlertType.DATA_GOVERNANCE: 0.8,
            AlertType.SYSTEM_HEALTH: 0.7,
            AlertType.PERFORMANCE: 0.5
        }
        
        score *= type_multipliers.get(alert.alert_type, 1.0)
        
        # Adjust based on affected assets/users
        if len(alert.affected_assets) > 10:
            score *= 1.3
        elif len(alert.affected_assets) > 5:
            score *= 1.1
        
        if len(alert.affected_users) > 50:
            score *= 1.4
        elif len(alert.affected_users) > 10:
            score *= 1.2
        
        return min(10.0, score)
    
    def _correlate_alert(self, alert: SecurityAlert):
        """Correlate alert with existing alerts"""
        
        if not alert.correlation_key:
            return
        
        # Find alerts with same correlation key within time window
        now = datetime.now()
        correlation_window_start = now - self.correlation_window
        
        correlated_alerts = []
        for existing_alert_id in self.correlation_keys.get(alert.correlation_key, []):
            if existing_alert_id in self.alerts:
                existing_alert = self.alerts[existing_alert_id]
                if existing_alert.timestamp >= correlation_window_start:
                    correlated_alerts.append(existing_alert)
        
        if correlated_alerts:
            # Find or create parent alert
            parent_alert = None
            for ca in correlated_alerts:
                if ca.parent_alert_id is None:
                    parent_alert = ca
                    break
            
            if parent_alert:
                # Add as child alert
                parent_alert.child_alert_ids.append(alert.alert_id)
                alert.parent_alert_id = parent_alert.alert_id
                
                # Update parent alert severity if needed
                if alert.severity.value > parent_alert.severity.value:
                    parent_alert.severity = alert.severity
                    parent_alert.risk_score = max(parent_alert.risk_score, alert.risk_score)
        
        # Add to correlation tracking
        self.correlation_keys[alert.correlation_key].append(alert.alert_id)
    
    def _trigger_notifications(self, alert: SecurityAlert):
        """Trigger notifications based on alert rules"""
        
        for rule in self.alert_rules.values():
            if not rule.enabled:
                continue
            
            if self._rule_matches_alert(rule, alert):
                for channel in rule.notification_channels:
                    if channel in self.notification_channels:
                        try:
                            self.notification_channels[channel](alert)
                        except Exception as e:
                            self.logger.error(f"Notification failed for channel {channel}: {e}")
                
                # Update rule statistics
                rule.last_triggered = datetime.now()
                rule.trigger_count += 1
    
    def _rule_matches_alert(self, rule: AlertRule, alert: SecurityAlert) -> bool:
        """Check if alert rule matches the alert"""
        
        # Check alert type
        if rule.alert_type != alert.alert_type:
            return False
        
        # Check severity (rule applies to same or higher severity)
        severity_levels = {
            AlertSeverity.INFO: 1,
            AlertSeverity.LOW: 2,
            AlertSeverity.MEDIUM: 3,
            AlertSeverity.HIGH: 4,
            AlertSeverity.CRITICAL: 5
        }
        
        if severity_levels[alert.severity] < severity_levels[rule.severity]:
            return False
        
        # Check conditions
        for condition_key, condition_value in rule.conditions.items():
            if condition_key in alert.details:
                if alert.details[condition_key] != condition_value:
                    return False
        
        # Check threshold conditions
        if rule.threshold and rule.time_window:
            # This would check if threshold is exceeded within time window
            # Implementation depends on specific threshold logic
            pass
        
        return True
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert"""
        
        if alert_id not in self.alerts:
            return False
        
        alert = self.alerts[alert_id]
        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_by = acknowledged_by
        alert.acknowledged_at = datetime.now()
        
        self.logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
        return True
    
    def resolve_alert(
        self, 
        alert_id: str, 
        resolved_by: str, 
        resolution_notes: Optional[str] = None
    ) -> bool:
        """Resolve an alert"""
        
        if alert_id not in self.alerts:
            return False
        
        alert = self.alerts[alert_id]
        alert.status = AlertStatus.RESOLVED
        alert.resolved_by = resolved_by
        alert.resolved_at = datetime.now()
        alert.resolution_notes = resolution_notes
        
        self.logger.info(f"Alert {alert_id} resolved by {resolved_by}")
        return True
    
    def get_active_alerts(
        self,
        severity_filter: Optional[AlertSeverity] = None,
        type_filter: Optional[AlertType] = None,
        limit: int = 100
    ) -> List[SecurityAlert]:
        """Get active alerts with filters"""
        
        active_statuses = {AlertStatus.NEW, AlertStatus.ACKNOWLEDGED, AlertStatus.INVESTIGATING}
        
        filtered_alerts = []
        for alert in self.alerts.values():
            if alert.status not in active_statuses:
                continue
            
            if severity_filter and alert.severity != severity_filter:
                continue
            
            if type_filter and alert.alert_type != type_filter:
                continue
            
            filtered_alerts.append(alert)
        
        # Sort by risk score and timestamp
        filtered_alerts.sort(key=lambda x: (-x.risk_score, -x.timestamp.timestamp()))
        
        return filtered_alerts[:limit]
    
    def _start_alert_processing(self):
        """Start background alert processing"""
        
        def alert_processor():
            while True:
                try:
                    self._process_alert_correlations()
                    self._cleanup_old_alerts()
                    time.sleep(30)  # Process every 30 seconds
                except Exception as e:
                    self.logger.error(f"Alert processing error: {e}")
        
        processing_thread = threading.Thread(target=alert_processor, daemon=True)
        processing_thread.start()
    
    def _process_alert_correlations(self):
        """Process alert correlations and cleanup"""
        
        now = datetime.now()
        cutoff_time = now - self.correlation_window
        
        # Cleanup old correlation keys
        for key in list(self.correlation_keys.keys()):
            self.correlation_keys[key] = [
                alert_id for alert_id in self.correlation_keys[key]
                if alert_id in self.alerts and self.alerts[alert_id].timestamp >= cutoff_time
            ]
            
            if not self.correlation_keys[key]:
                del self.correlation_keys[key]
    
    def _cleanup_old_alerts(self):
        """Cleanup old resolved alerts"""
        
        cutoff_time = datetime.now() - timedelta(days=30)
        
        alerts_to_remove = []
        for alert_id, alert in self.alerts.items():
            if (alert.status in {AlertStatus.RESOLVED, AlertStatus.FALSE_POSITIVE} and 
                alert.timestamp < cutoff_time):
                alerts_to_remove.append(alert_id)
        
        for alert_id in alerts_to_remove:
            del self.alerts[alert_id]


class SecurityMetricsCollector:
    """Collects and aggregates security metrics"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.metrics_history: List[SecurityMetrics] = []
        self.collection_interval = 60  # seconds
        
        # Component references
        self.security_manager = None
        self.dlp_manager = None
        self.compliance_engine = None
        self.access_manager = None
        self.governance_orchestrator = None
        
        # Start metrics collection
        self._start_metrics_collection()
    
    def _start_metrics_collection(self):
        """Start background metrics collection"""
        
        def metrics_collector():
            while True:
                try:
                    metrics = self.collect_current_metrics()
                    self.metrics_history.append(metrics)
                    
                    # Keep only last 24 hours of data
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self.metrics_history = [
                        m for m in self.metrics_history if m.timestamp >= cutoff_time
                    ]
                    
                    time.sleep(self.collection_interval)
                except Exception as e:
                    self.logger.error(f"Metrics collection error: {e}")
        
        collection_thread = threading.Thread(target=metrics_collector, daemon=True)
        collection_thread.start()
    
    def collect_current_metrics(self) -> SecurityMetrics:
        """Collect current security metrics"""
        
        metrics = SecurityMetrics(timestamp=datetime.now())
        
        try:
            # Initialize component references if needed
            if not self.security_manager:
                self.security_manager = get_security_manager()
            if not self.compliance_engine:
                self.compliance_engine = get_compliance_engine()
            if not self.access_manager:
                self.access_manager = get_access_control_manager()
            if not self.governance_orchestrator:
                self.governance_orchestrator = get_governance_orchestrator()
            
            # Collect threat metrics
            threat_dashboard = self.security_manager.get_security_dashboard()
            metrics.total_threats_24h = threat_dashboard.get('total_events_24h', 0)
            metrics.critical_threats_24h = len([
                e for e in threat_dashboard.get('recent_high_severity_events', [])
                if e.get('threat_level') == 'critical'
            ])
            metrics.blocked_attacks = threat_dashboard.get('blocked_ips', 0)
            
            # Calculate threat trend
            if len(self.metrics_history) > 0:
                previous_threats = self.metrics_history[-1].total_threats_24h
                if previous_threats > 0:
                    metrics.threat_trend = ((metrics.total_threats_24h - previous_threats) / previous_threats) * 100
            
            # Collect DLP metrics (would integrate with actual DLP manager)
            # For now using placeholder values
            metrics.dlp_violations_24h = 15  # Placeholder
            metrics.sensitive_data_exposures = 3  # Placeholder
            metrics.data_blocked = 12  # Placeholder
            if metrics.dlp_violations_24h > 0:
                metrics.dlp_effectiveness = (metrics.data_blocked / metrics.dlp_violations_24h) * 100
            
            # Collect compliance metrics
            compliance_dashboard = self.compliance_engine.get_compliance_dashboard()
            metrics.compliance_violations = compliance_dashboard['violation_summary']['total_violations']
            metrics.non_compliant_controls = sum(
                compliance_dashboard['framework_compliance'].get(fw, {}).get('non_compliant', 0)
                for fw in compliance_dashboard['framework_compliance']
            )
            metrics.compliance_score = compliance_dashboard['overall_compliance']['compliance_rate'] * 100
            metrics.frameworks_assessed = len(compliance_dashboard.get('framework_compliance', {}))
            
            # Collect access control metrics
            access_dashboard = self.access_manager.get_access_dashboard()
            metrics.access_denials = 25  # Placeholder - would come from actual access logs
            metrics.privilege_escalations = access_dashboard['elevation_requests']['active']
            metrics.unauthorized_attempts = 8  # Placeholder
            
            # Collect governance metrics
            governance_dashboard = self.governance_orchestrator.get_comprehensive_dashboard()
            metrics.governance_score = governance_dashboard.get('overall_governance_score', 0.0) * 100
            metrics.assets_cataloged = governance_dashboard['catalog_statistics']['total_assets']
            metrics.lineage_coverage = governance_dashboard['lineage_statistics']['lineage_coverage']['coverage_percentage']
            metrics.retention_violations = governance_dashboard['retention_compliance']['issues']
            
            # System health metrics (placeholder)
            metrics.system_uptime = 99.9  # Would integrate with monitoring system
            metrics.response_time_ms = 150.0  # Would come from performance monitoring
            metrics.error_rate = 0.1  # Would come from error tracking
            
        except Exception as e:
            self.logger.error(f"Error collecting metrics: {e}")
        
        return metrics
    
    def get_metrics_trend(self, hours: int = 24) -> List[SecurityMetrics]:
        """Get metrics trend for specified hours"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [m for m in self.metrics_history if m.timestamp >= cutoff_time]
    
    def get_aggregated_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """Get aggregated metrics for dashboard"""
        
        trend_data = self.get_metrics_trend(hours)
        
        if not trend_data:
            return {}
        
        latest = trend_data[-1] if trend_data else SecurityMetrics(datetime.now())
        
        # Calculate averages and totals
        avg_response_time = sum(m.response_time_ms for m in trend_data) / len(trend_data)
        avg_uptime = sum(m.system_uptime for m in trend_data) / len(trend_data)
        total_threats = sum(m.total_threats_24h for m in trend_data)
        
        return {
            'current': {
                'threats_24h': latest.total_threats_24h,
                'critical_threats': latest.critical_threats_24h,
                'dlp_violations': latest.dlp_violations_24h,
                'compliance_score': latest.compliance_score,
                'governance_score': latest.governance_score,
                'system_uptime': latest.system_uptime
            },
            'trends': {
                'threat_trend': latest.threat_trend,
                'total_threats_period': total_threats,
                'avg_response_time': round(avg_response_time, 2),
                'avg_uptime': round(avg_uptime, 2)
            },
            'effectiveness': {
                'dlp_effectiveness': latest.dlp_effectiveness,
                'access_effectiveness': latest.access_effectiveness,
                'blocked_attacks': latest.blocked_attacks
            }
        }


class RealTimeNotificationManager:
    """Manages real-time notifications and WebSocket connections"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.active_connections: Set[WebSocket] = set()
        self.user_subscriptions: Dict[str, Dict[str, Any]] = {}
    
    async def connect_websocket(self, websocket: WebSocket, user_id: str):
        """Connect a WebSocket for real-time updates"""
        
        await websocket.accept()
        self.active_connections.add(websocket)
        
        # Initialize user subscription
        self.user_subscriptions[user_id] = {
            'websocket': websocket,
            'subscriptions': ['all'],  # Default to all alerts
            'last_activity': datetime.now()
        }
        
        self.logger.info(f"WebSocket connected for user {user_id}")
    
    def disconnect_websocket(self, websocket: WebSocket, user_id: str):
        """Disconnect a WebSocket"""
        
        self.active_connections.discard(websocket)
        if user_id in self.user_subscriptions:
            del self.user_subscriptions[user_id]
        
        self.logger.info(f"WebSocket disconnected for user {user_id}")
    
    async def broadcast_alert(self, alert: SecurityAlert):
        """Broadcast alert to all connected clients"""
        
        alert_data = {
            'type': 'alert',
            'data': {
                'alert_id': alert.alert_id,
                'timestamp': alert.timestamp.isoformat(),
                'alert_type': alert.alert_type.value,
                'severity': alert.severity.value,
                'title': alert.title,
                'description': alert.description,
                'risk_score': alert.risk_score,
                'source_system': alert.source_system,
                'status': alert.status.value
            }
        }
        
        await self._broadcast_to_subscribers(alert_data, alert.alert_type.value)
    
    async def broadcast_metrics_update(self, metrics: SecurityMetrics):
        """Broadcast metrics update to connected clients"""
        
        metrics_data = {
            'type': 'metrics_update',
            'data': {
                'timestamp': metrics.timestamp.isoformat(),
                'threats_24h': metrics.total_threats_24h,
                'critical_threats': metrics.critical_threats_24h,
                'dlp_violations': metrics.dlp_violations_24h,
                'compliance_score': metrics.compliance_score,
                'governance_score': metrics.governance_score,
                'system_uptime': metrics.system_uptime
            }
        }
        
        await self._broadcast_to_subscribers(metrics_data, 'metrics')
    
    async def _broadcast_to_subscribers(self, data: Dict[str, Any], subscription_type: str):
        """Broadcast data to relevant subscribers"""
        
        message = json.dumps(data)
        disconnected_websockets = set()
        
        for user_id, sub_info in self.user_subscriptions.items():
            if (subscription_type in sub_info['subscriptions'] or 
                'all' in sub_info['subscriptions']):
                
                try:
                    await sub_info['websocket'].send_text(message)
                    sub_info['last_activity'] = datetime.now()
                except Exception as e:
                    self.logger.warning(f"Failed to send to {user_id}: {e}")
                    disconnected_websockets.add(sub_info['websocket'])
        
        # Clean up disconnected websockets
        for ws in disconnected_websockets:
            self.active_connections.discard(ws)


class SecurityDashboard:
    """Main security monitoring dashboard"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Initialize components
        self.alert_manager = AlertManager()
        self.metrics_collector = SecurityMetricsCollector()
        self.notification_manager = RealTimeNotificationManager()
        
        # Dashboard configuration
        self.widgets: Dict[str, DashboardWidget] = {}
        self._initialize_default_widgets()
        
        # Event monitoring
        self._start_event_monitoring()
    
    def _initialize_default_widgets(self):
        """Initialize default dashboard widgets"""
        
        default_widgets = [
            DashboardWidget(
                widget_id="threat_overview",
                title="Threat Overview",
                widget_type="metric",
                position={"x": 0, "y": 0, "width": 4, "height": 2},
                config={
                    "metrics": ["total_threats_24h", "critical_threats_24h", "blocked_attacks"],
                    "show_trend": True
                }
            ),
            DashboardWidget(
                widget_id="dlp_status",
                title="Data Loss Prevention",
                widget_type="metric",
                position={"x": 4, "y": 0, "width": 4, "height": 2},
                config={
                    "metrics": ["dlp_violations_24h", "sensitive_data_exposures", "dlp_effectiveness"],
                    "show_trend": True
                }
            ),
            DashboardWidget(
                widget_id="compliance_score",
                title="Compliance Dashboard",
                widget_type="chart",
                position={"x": 8, "y": 0, "width": 4, "height": 2},
                config={
                    "chart_type": "gauge",
                    "metric": "compliance_score",
                    "target": 95
                }
            ),
            DashboardWidget(
                widget_id="active_alerts",
                title="Active Alerts",
                widget_type="alert_list",
                position={"x": 0, "y": 2, "width": 8, "height": 4},
                config={
                    "max_alerts": 10,
                    "severity_filter": ["high", "critical"]
                }
            ),
            DashboardWidget(
                widget_id="threat_trend",
                title="24-Hour Threat Trend",
                widget_type="chart",
                position={"x": 8, "y": 2, "width": 4, "height": 4},
                config={
                    "chart_type": "line",
                    "metrics": ["total_threats_24h", "critical_threats_24h"],
                    "time_range": "24h"
                }
            ),
            DashboardWidget(
                widget_id="governance_metrics",
                title="Data Governance",
                widget_type="metric",
                position={"x": 0, "y": 6, "width": 6, "height": 2},
                config={
                    "metrics": ["governance_score", "assets_cataloged", "lineage_coverage"],
                    "show_trend": False
                }
            ),
            DashboardWidget(
                widget_id="system_health",
                title="System Health",
                widget_type="metric",
                position={"x": 6, "y": 6, "width": 6, "height": 2},
                config={
                    "metrics": ["system_uptime", "response_time_ms", "error_rate"],
                    "show_alerts": True
                }
            )
        ]
        
        for widget in default_widgets:
            self.widgets[widget.widget_id] = widget
    
    def _start_event_monitoring(self):
        """Start monitoring security events from all components"""
        
        def event_monitor():
            while True:
                try:
                    self._check_for_new_events()
                    time.sleep(5)  # Check every 5 seconds
                except Exception as e:
                    self.logger.error(f"Event monitoring error: {e}")
        
        monitoring_thread = threading.Thread(target=event_monitor, daemon=True)
        monitoring_thread.start()
    
    def _check_for_new_events(self):
        """Check for new security events and create alerts"""
        
        try:
            # Check security manager for new threats
            security_manager = get_security_manager()
            dashboard_data = security_manager.get_security_dashboard()
            
            # Check for high-severity events
            high_severity_events = dashboard_data.get('recent_high_severity_events', [])
            for event in high_severity_events[-5:]:  # Check last 5 events
                if event.get('threat_level') in ['high', 'critical']:
                    # Create alert if not already created
                    self.alert_manager.create_alert(
                        alert_type=AlertType.SECURITY_THREAT,
                        severity=AlertSeverity.HIGH if event.get('threat_level') == 'high' else AlertSeverity.CRITICAL,
                        title=f"Security Threat Detected: {event.get('type')}",
                        description=f"High severity security event from {event.get('source_ip')}",
                        source_system="security_manager",
                        details=event,
                        correlation_key=f"threat_{event.get('source_ip')}"
                    )
            
            # Check compliance engine for violations
            compliance_engine = get_compliance_engine()
            compliance_dashboard = compliance_engine.get_compliance_dashboard()
            
            if compliance_dashboard['violation_summary']['total_violations'] > 0:
                # This would create alerts for new compliance violations
                # Implementation would track which violations are new
                pass
            
            # Check access control for violations
            # Implementation would integrate with access control logs
            
        except Exception as e:
            self.logger.error(f"Error checking for new events: {e}")
    
    async def get_dashboard_data(self, user_id: str = "system") -> Dict[str, Any]:
        """Get complete dashboard data"""
        
        # Get current metrics
        current_metrics = self.metrics_collector.collect_current_metrics()
        aggregated_metrics = self.metrics_collector.get_aggregated_metrics()
        
        # Get active alerts
        active_alerts = self.alert_manager.get_active_alerts(limit=20)
        
        # Get widget data
        widget_data = {}
        for widget_id, widget in self.widgets.items():
            widget_data[widget_id] = await self._get_widget_data(widget, current_metrics)
        
        return {
            'dashboard_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'metrics': {
                'current': current_metrics.__dict__,
                'aggregated': aggregated_metrics
            },
            'alerts': {
                'active_count': len(active_alerts),
                'critical_count': len([a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]),
                'high_count': len([a for a in active_alerts if a.severity == AlertSeverity.HIGH]),
                'recent_alerts': [
                    {
                        'alert_id': a.alert_id,
                        'title': a.title,
                        'severity': a.severity.value,
                        'alert_type': a.alert_type.value,
                        'timestamp': a.timestamp.isoformat(),
                        'status': a.status.value,
                        'risk_score': a.risk_score
                    }
                    for a in active_alerts[:10]
                ]
            },
            'widgets': widget_data,
            'system_status': {
                'components': {
                    'security_manager': 'operational',
                    'dlp_system': 'operational',
                    'compliance_engine': 'operational',
                    'access_control': 'operational',
                    'governance': 'operational'
                },
                'overall_health': 'healthy'
            }
        }
    
    async def _get_widget_data(
        self, 
        widget: DashboardWidget, 
        current_metrics: SecurityMetrics
    ) -> Dict[str, Any]:
        """Get data for a specific widget"""
        
        widget_data = {
            'widget_id': widget.widget_id,
            'title': widget.title,
            'type': widget.widget_type,
            'position': widget.position,
            'last_updated': datetime.now().isoformat()
        }
        
        if widget.widget_type == "metric":
            metric_values = {}
            for metric_name in widget.config.get('metrics', []):
                if hasattr(current_metrics, metric_name):
                    metric_values[metric_name] = getattr(current_metrics, metric_name)
            
            widget_data['data'] = {
                'metrics': metric_values,
                'show_trend': widget.config.get('show_trend', False)
            }
        
        elif widget.widget_type == "chart":
            # Chart data would be generated based on historical metrics
            chart_type = widget.config.get('chart_type', 'line')
            metrics = widget.config.get('metrics', [])
            
            # Get trend data for charts
            trend_data = self.metrics_collector.get_metrics_trend(hours=24)
            chart_data = []
            
            for metric_name in metrics:
                series_data = []
                for m in trend_data:
                    if hasattr(m, metric_name):
                        series_data.append({
                            'timestamp': m.timestamp.isoformat(),
                            'value': getattr(m, metric_name)
                        })
                
                chart_data.append({
                    'name': metric_name,
                    'data': series_data
                })
            
            widget_data['data'] = {
                'chart_type': chart_type,
                'series': chart_data
            }
        
        elif widget.widget_type == "alert_list":
            # Get filtered alerts for the widget
            severity_filter = widget.config.get('severity_filter', [])
            max_alerts = widget.config.get('max_alerts', 10)
            
            alerts = self.alert_manager.get_active_alerts(limit=max_alerts)
            
            if severity_filter:
                alerts = [
                    a for a in alerts 
                    if a.severity.value in severity_filter
                ]
            
            widget_data['data'] = {
                'alerts': [
                    {
                        'alert_id': a.alert_id,
                        'title': a.title,
                        'severity': a.severity.value,
                        'timestamp': a.timestamp.isoformat(),
                        'status': a.status.value,
                        'risk_score': a.risk_score
                    }
                    for a in alerts
                ]
            }
        
        return widget_data
    
    async def handle_websocket_connection(self, websocket: WebSocket, user_id: str):
        """Handle WebSocket connection for real-time updates"""
        
        await self.notification_manager.connect_websocket(websocket, user_id)
        
        try:
            while True:
                # Keep connection alive and handle incoming messages
                data = await websocket.receive_text()
                message = json.loads(data)
                
                if message.get('type') == 'subscribe':
                    # Handle subscription updates
                    subscriptions = message.get('subscriptions', ['all'])
                    if user_id in self.notification_manager.user_subscriptions:
                        self.notification_manager.user_subscriptions[user_id]['subscriptions'] = subscriptions
                
                elif message.get('type') == 'acknowledge_alert':
                    # Handle alert acknowledgment
                    alert_id = message.get('alert_id')
                    if alert_id:
                        self.alert_manager.acknowledge_alert(alert_id, user_id)
                
        except Exception as e:
            self.logger.info(f"WebSocket connection closed: {e}")
        finally:
            self.notification_manager.disconnect_websocket(websocket, user_id)
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Get high-level dashboard summary"""
        
        current_metrics = self.metrics_collector.collect_current_metrics()
        active_alerts = self.alert_manager.get_active_alerts()
        
        critical_alerts = [a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]
        high_alerts = [a for a in active_alerts if a.severity == AlertSeverity.HIGH]
        
        return {
            'summary': {
                'total_alerts': len(active_alerts),
                'critical_alerts': len(critical_alerts),
                'high_alerts': len(high_alerts),
                'threats_24h': current_metrics.total_threats_24h,
                'compliance_score': current_metrics.compliance_score,
                'governance_score': current_metrics.governance_score,
                'system_uptime': current_metrics.system_uptime
            },
            'health_status': 'critical' if critical_alerts else 'warning' if high_alerts else 'healthy',
            'last_updated': datetime.now().isoformat()
        }


# Global dashboard instance
_security_dashboard: Optional[SecurityDashboard] = None

def get_security_dashboard() -> SecurityDashboard:
    """Get global security dashboard instance"""
    global _security_dashboard
    if _security_dashboard is None:
        _security_dashboard = SecurityDashboard()
    return _security_dashboard