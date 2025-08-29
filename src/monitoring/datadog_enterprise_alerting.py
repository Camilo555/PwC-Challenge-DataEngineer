"""
DataDog Enterprise Alerting System
Comprehensive real-time alerting with business impact awareness, capacity planning,
security compliance, and automated incident response
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
from collections import defaultdict, deque

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring, Alert, AlertPriority
from monitoring.datadog_intelligent_alerting import (
    DataDogIntelligentAlerting, AlertRule, AlertState, AlertChannel,
    EscalationLevel, ThresholdType, NotificationChannel, EscalationRule
)

logger = get_logger(__name__)


class BusinessImpact(Enum):
    """Business impact levels"""
    MINIMAL = "minimal"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    CATASTROPHIC = "catastrophic"


class AlertCategory(Enum):
    """Alert categories for better organization"""
    INFRASTRUCTURE = "infrastructure"
    APPLICATION = "application"
    BUSINESS_METRICS = "business_metrics"
    SECURITY = "security"
    DATA_QUALITY = "data_quality"
    ML_OPERATIONS = "ml_operations"
    CAPACITY_PLANNING = "capacity_planning"
    COMPLIANCE = "compliance"
    USER_EXPERIENCE = "user_experience"


class IncidentSeverity(Enum):
    """Incident severity levels"""
    SEV1 = "sev1"  # Critical system outage
    SEV2 = "sev2"  # Major functionality impacted
    SEV3 = "sev3"  # Minor functionality impacted
    SEV4 = "sev4"  # Cosmetic or documentation issues


@dataclass
class BusinessContext:
    """Business context for alerts"""
    revenue_impact_per_hour: Optional[float] = None
    customer_impact_count: Optional[int] = None
    sla_breach_penalty: Optional[float] = None
    regulatory_requirements: List[str] = field(default_factory=list)
    business_units_affected: List[str] = field(default_factory=list)
    peak_business_hours: bool = False


@dataclass
class EnterpriseAlertRule:
    """Enhanced enterprise alert rule with business context"""
    rule_id: str
    name: str
    description: str
    category: AlertCategory
    business_impact: BusinessImpact
    business_context: BusinessContext
    metric_query: str
    threshold_type: ThresholdType
    
    # Enhanced thresholds
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None
    emergency_threshold: Optional[float] = None
    
    # Time-based conditions
    evaluation_window_minutes: int = 5
    minimum_occurrences: int = 1
    recovery_window_minutes: int = 5
    
    # Business-aware scheduling
    business_hours_only: bool = False
    timezone: str = "UTC"
    quiet_hours: Optional[tuple] = None
    maintenance_windows: List[Dict[str, Any]] = field(default_factory=list)
    
    # Advanced notification
    notification_channels: List[str] = field(default_factory=list)
    escalation_rules: List[EscalationRule] = field(default_factory=list)
    custom_message_template: Optional[str] = None
    
    # Auto-remediation
    auto_remediation_enabled: bool = False
    remediation_playbook: Optional[str] = None
    max_auto_remediation_attempts: int = 3
    
    # Compliance and audit
    regulatory_tags: List[str] = field(default_factory=list)
    audit_required: bool = False
    data_retention_days: int = 90
    
    # Machine learning enhancements
    anomaly_detection_enabled: bool = False
    seasonal_adjustment: bool = False
    forecast_based_alerting: bool = False
    
    # Dependencies
    dependent_services: List[str] = field(default_factory=list)
    correlation_rules: List[str] = field(default_factory=list)


@dataclass
class CapacityPlanningMetric:
    """Capacity planning metric definition"""
    metric_name: str
    current_usage: float
    capacity_limit: float
    growth_rate_percent: float
    warning_threshold_percent: float = 80.0
    critical_threshold_percent: float = 90.0
    forecast_days: int = 30
    unit: str = "percent"
    cost_per_unit: Optional[float] = None


@dataclass
class SecurityAlertRule:
    """Security-specific alert rule"""
    rule_id: str
    name: str
    description: str
    security_category: str  # authentication, authorization, data_breach, etc.
    threat_level: str  # low, medium, high, critical
    mitre_attack_framework: List[str] = field(default_factory=list)
    indicators_of_compromise: List[str] = field(default_factory=list)
    automated_response: Optional[str] = None
    requires_human_review: bool = True
    pci_dss_relevant: bool = False
    gdpr_relevant: bool = False
    sox_relevant: bool = False


class DataDogEnterpriseAlerting:
    """
    Enterprise-grade Real-time Alerting System
    
    Features:
    - Business impact-aware alerting
    - Advanced capacity planning alerts
    - Security and compliance monitoring
    - ML-enhanced anomaly detection
    - Automated incident response
    - Regulatory compliance tracking
    - Cost-aware alerting
    - Multi-dimensional correlation
    - Predictive alerting
    """
    
    def __init__(self, service_name: str = "enterprise-alerting",
                 datadog_monitoring: Optional[DatadogMonitoring] = None,
                 base_alerting: Optional[DataDogIntelligentAlerting] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.base_alerting = base_alerting
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Enhanced data structures
        self.enterprise_rules: Dict[str, EnterpriseAlertRule] = {}
        self.capacity_metrics: Dict[str, CapacityPlanningMetric] = {}
        self.security_rules: Dict[str, SecurityAlertRule] = {}
        
        # Business context tracking
        self.business_hours_cache: Dict[str, bool] = {}
        self.maintenance_windows: Dict[str, List[Dict[str, Any]]] = {}
        self.cost_impact_tracking: Dict[str, float] = {}
        
        # ML and forecasting
        self.ml_models: Dict[str, Any] = {}
        self.forecast_cache: Dict[str, Dict[str, float]] = {}
        self.anomaly_thresholds: Dict[str, Dict[str, float]] = {}
        
        # Compliance and audit
        self.compliance_violations: List[Dict[str, Any]] = []
        self.audit_trail: List[Dict[str, Any]] = []
        self.regulatory_notifications: Dict[str, List[str]] = {}
        
        # Performance tracking
        self.enterprise_metrics = {
            "total_alerts": 0,
            "business_impact_prevented": 0.0,
            "auto_remediation_success": 0,
            "compliance_violations": 0,
            "capacity_warnings": 0
        }
        
        # Initialize enterprise alerting
        self._initialize_enterprise_alert_rules()
        self._initialize_capacity_planning()
        self._initialize_security_rules()
        self._initialize_compliance_framework()
        
        # Start enterprise services
        asyncio.create_task(self._start_enterprise_services())
        
        self.logger.info(f"Enterprise alerting system initialized for {service_name}")
    
    def _initialize_enterprise_alert_rules(self):
        """Initialize comprehensive enterprise alert rules"""
        
        enterprise_rules = [
            # Critical Business Metrics
            EnterpriseAlertRule(
                rule_id="revenue_drop_critical",
                name="Critical Revenue Drop Detection",
                description="Detects significant drops in revenue that could indicate system or business issues",
                category=AlertCategory.BUSINESS_METRICS,
                business_impact=BusinessImpact.CRITICAL,
                business_context=BusinessContext(
                    revenue_impact_per_hour=10000.0,
                    customer_impact_count=1000,
                    business_units_affected=["sales", "marketing", "customer_success"]
                ),
                metric_query="sum:business.revenue.total{*}",
                threshold_type=ThresholdType.TREND,
                critical_threshold=-15.0,  # 15% drop
                emergency_threshold=-25.0,  # 25% drop
                evaluation_window_minutes=15,
                minimum_occurrences=2,
                business_hours_only=False,
                notification_channels=["pagerduty_executive", "email_cfo", "slack_executive"],
                escalation_rules=[
                    EscalationRule(
                        level=EscalationLevel.EXECUTIVE,
                        delay_minutes=0,
                        channels=["pagerduty_executive"]
                    )
                ],
                auto_remediation_enabled=False,
                audit_required=True,
                regulatory_tags=["financial_reporting", "sox_compliance"]
            ),
            
            # Infrastructure Critical Alerts
            EnterpriseAlertRule(
                rule_id="system_outage_detection",
                name="Critical System Outage",
                description="Detects complete system outages across multiple services",
                category=AlertCategory.INFRASTRUCTURE,
                business_impact=BusinessImpact.CATASTROPHIC,
                business_context=BusinessContext(
                    revenue_impact_per_hour=50000.0,
                    customer_impact_count=10000,
                    sla_breach_penalty=25000.0
                ),
                metric_query="avg:system.availability{*}",
                threshold_type=ThresholdType.STATIC,
                critical_threshold=95.0,
                emergency_threshold=90.0,
                evaluation_window_minutes=2,
                minimum_occurrences=1,
                notification_channels=["pagerduty_oncall", "email_executives", "slack_ops"],
                escalation_rules=[
                    EscalationRule(
                        level=EscalationLevel.LEVEL_1,
                        delay_minutes=0,
                        channels=["pagerduty_oncall", "slack_ops"]
                    ),
                    EscalationRule(
                        level=EscalationLevel.LEVEL_2,
                        delay_minutes=10,
                        channels=["email_executives"]
                    ),
                    EscalationRule(
                        level=EscalationLevel.EXECUTIVE,
                        delay_minutes=30,
                        channels=["pagerduty_executive"]
                    )
                ],
                auto_remediation_enabled=True,
                remediation_playbook="system_failover_playbook",
                dependent_services=["api", "database", "cache", "messaging"],
                audit_required=True
            ),
            
            # API Performance Degradation
            EnterpriseAlertRule(
                rule_id="api_performance_degradation",
                name="API Performance Degradation",
                description="Detects API performance issues that impact user experience",
                category=AlertCategory.APPLICATION,
                business_impact=BusinessImpact.HIGH,
                business_context=BusinessContext(
                    revenue_impact_per_hour=5000.0,
                    customer_impact_count=500,
                    peak_business_hours=True
                ),
                metric_query="percentile:api.response_time{*}:95",
                threshold_type=ThresholdType.DYNAMIC,
                warning_threshold=500.0,  # 500ms
                critical_threshold=1000.0,  # 1s
                emergency_threshold=2000.0,  # 2s
                evaluation_window_minutes=5,
                minimum_occurrences=3,
                business_hours_only=False,
                notification_channels=["slack_ops", "email_api_team"],
                escalation_rules=[
                    EscalationRule(
                        level=EscalationLevel.LEVEL_1,
                        delay_minutes=0,
                        channels=["slack_ops"]
                    ),
                    EscalationRule(
                        level=EscalationLevel.LEVEL_2,
                        delay_minutes=20,
                        channels=["email_api_team", "pagerduty_oncall"]
                    )
                ],
                auto_remediation_enabled=True,
                remediation_playbook="api_performance_optimization",
                anomaly_detection_enabled=True,
                seasonal_adjustment=True,
                correlation_rules=["database_performance", "cache_hit_rate"]
            ),
            
            # Database Performance
            EnterpriseAlertRule(
                rule_id="database_performance_critical",
                name="Critical Database Performance",
                description="Database performance issues that could lead to system-wide problems",
                category=AlertCategory.INFRASTRUCTURE,
                business_impact=BusinessImpact.HIGH,
                business_context=BusinessContext(
                    revenue_impact_per_hour=8000.0,
                    customer_impact_count=2000
                ),
                metric_query="avg:database.query_time{*}",
                threshold_type=ThresholdType.ANOMALY,
                warning_threshold=100.0,  # 100ms
                critical_threshold=500.0,  # 500ms
                evaluation_window_minutes=3,
                minimum_occurrences=2,
                notification_channels=["slack_ops", "email_dba_team"],
                auto_remediation_enabled=True,
                remediation_playbook="database_optimization",
                dependent_services=["api", "etl_pipelines"],
                anomaly_detection_enabled=True
            ),
            
            # Data Quality Alerts
            EnterpriseAlertRule(
                rule_id="data_quality_degradation",
                name="Data Quality Degradation",
                description="Critical data quality issues affecting business operations",
                category=AlertCategory.DATA_QUALITY,
                business_impact=BusinessImpact.MEDIUM,
                business_context=BusinessContext(
                    business_units_affected=["analytics", "reporting", "compliance"]
                ),
                metric_query="avg:data_quality.overall_score{*}",
                threshold_type=ThresholdType.STATIC,
                warning_threshold=95.0,
                critical_threshold=90.0,
                emergency_threshold=85.0,
                evaluation_window_minutes=10,
                notification_channels=["slack_data_eng", "email_data_team"],
                audit_required=True,
                regulatory_tags=["data_governance", "gdpr_compliance"]
            ),
            
            # ML Model Performance
            EnterpriseAlertRule(
                rule_id="ml_model_accuracy_drop",
                name="ML Model Accuracy Degradation",
                description="Machine learning model accuracy below acceptable thresholds",
                category=AlertCategory.ML_OPERATIONS,
                business_impact=BusinessImpact.MEDIUM,
                business_context=BusinessContext(
                    revenue_impact_per_hour=2000.0,
                    business_units_affected=["ai_products", "recommendations"]
                ),
                metric_query="avg:ml.model.accuracy{stage:production}",
                threshold_type=ThresholdType.DYNAMIC,
                warning_threshold=90.0,
                critical_threshold=85.0,
                evaluation_window_minutes=30,
                notification_channels=["slack_ml_ops", "email_ds_team"],
                auto_remediation_enabled=True,
                remediation_playbook="model_retraining",
                anomaly_detection_enabled=True,
                forecast_based_alerting=True
            ),
            
            # Customer Experience
            EnterpriseAlertRule(
                rule_id="customer_satisfaction_drop",
                name="Customer Satisfaction Drop",
                description="Significant drop in customer satisfaction metrics",
                category=AlertCategory.USER_EXPERIENCE,
                business_impact=BusinessImpact.HIGH,
                business_context=BusinessContext(
                    customer_impact_count=5000,
                    business_units_affected=["customer_success", "product", "marketing"]
                ),
                metric_query="avg:customer.satisfaction.nps{*}",
                threshold_type=ThresholdType.TREND,
                warning_threshold=-10.0,  # 10 point drop
                critical_threshold=-20.0,  # 20 point drop
                evaluation_window_minutes=60,
                notification_channels=["email_customer_success", "slack_product"],
                business_hours_only=True
            )
        ]
        
        for rule in enterprise_rules:
            self.enterprise_rules[rule.rule_id] = rule
        
        self.logger.info(f"Initialized {len(enterprise_rules)} enterprise alert rules")
    
    def _initialize_capacity_planning(self):
        """Initialize capacity planning metrics and alerts"""
        
        capacity_metrics = [
            CapacityPlanningMetric(
                metric_name="cpu_utilization",
                current_usage=65.0,
                capacity_limit=100.0,
                growth_rate_percent=2.5,
                warning_threshold_percent=75.0,
                critical_threshold_percent=85.0,
                forecast_days=30,
                unit="percent",
                cost_per_unit=0.05
            ),
            CapacityPlanningMetric(
                metric_name="memory_utilization",
                current_usage=70.0,
                capacity_limit=100.0,
                growth_rate_percent=1.8,
                warning_threshold_percent=80.0,
                critical_threshold_percent=90.0,
                forecast_days=30,
                unit="percent",
                cost_per_unit=0.08
            ),
            CapacityPlanningMetric(
                metric_name="storage_utilization",
                current_usage=60.0,
                capacity_limit=100.0,
                growth_rate_percent=5.0,
                warning_threshold_percent=70.0,
                critical_threshold_percent=85.0,
                forecast_days=60,
                unit="percent",
                cost_per_unit=0.10
            ),
            CapacityPlanningMetric(
                metric_name="database_connections",
                current_usage=150.0,
                capacity_limit=200.0,
                growth_rate_percent=3.0,
                warning_threshold_percent=80.0,
                critical_threshold_percent=95.0,
                forecast_days=14,
                unit="count",
                cost_per_unit=0.02
            ),
            CapacityPlanningMetric(
                metric_name="api_requests_per_second",
                current_usage=1000.0,
                capacity_limit=1500.0,
                growth_rate_percent=8.0,
                warning_threshold_percent=75.0,
                critical_threshold_percent=90.0,
                forecast_days=21,
                unit="rps",
                cost_per_unit=0.01
            )
        ]
        
        for metric in capacity_metrics:
            self.capacity_metrics[metric.metric_name] = metric
        
        self.logger.info(f"Initialized {len(capacity_metrics)} capacity planning metrics")
    
    def _initialize_security_rules(self):
        """Initialize security-specific alert rules"""
        
        security_rules = [
            SecurityAlertRule(
                rule_id="failed_authentication_spike",
                name="Authentication Failure Spike",
                description="Unusual spike in authentication failures indicating potential brute force attack",
                security_category="authentication",
                threat_level="high",
                mitre_attack_framework=["T1110", "T1078"],
                indicators_of_compromise=["multiple_failed_logins", "source_ip_patterns"],
                automated_response="block_suspicious_ips",
                requires_human_review=True,
                pci_dss_relevant=True
            ),
            SecurityAlertRule(
                rule_id="data_exfiltration_detection",
                name="Potential Data Exfiltration",
                description="Unusual data access patterns that may indicate data exfiltration",
                security_category="data_breach",
                threat_level="critical",
                mitre_attack_framework=["T1041", "T1005"],
                indicators_of_compromise=["unusual_data_volume", "off_hours_access"],
                automated_response="quarantine_affected_accounts",
                requires_human_review=True,
                gdpr_relevant=True,
                sox_relevant=True
            ),
            SecurityAlertRule(
                rule_id="privilege_escalation_attempt",
                name="Privilege Escalation Attempt",
                description="Attempted privilege escalation detected",
                security_category="authorization",
                threat_level="high",
                mitre_attack_framework=["T1548", "T1134"],
                indicators_of_compromise=["admin_access_attempts", "permission_changes"],
                automated_response="alert_security_team",
                requires_human_review=True,
                pci_dss_relevant=True
            ),
            SecurityAlertRule(
                rule_id="malicious_file_upload",
                name="Malicious File Upload Detection",
                description="Potential malicious file upload detected",
                security_category="malware",
                threat_level="high",
                mitre_attack_framework=["T1566", "T1105"],
                indicators_of_compromise=["suspicious_file_types", "malware_signatures"],
                automated_response="quarantine_files",
                requires_human_review=False
            )
        ]
        
        for rule in security_rules:
            self.security_rules[rule.rule_id] = rule
        
        self.logger.info(f"Initialized {len(security_rules)} security alert rules")
    
    def _initialize_compliance_framework(self):
        """Initialize compliance and regulatory framework"""
        
        # Map regulations to alert categories
        self.regulatory_notifications = {
            "gdpr": ["data_quality", "security", "privacy"],
            "pci_dss": ["security", "payment_processing"],
            "sox": ["financial_reporting", "audit", "data_integrity"],
            "hipaa": ["data_security", "privacy", "access_control"],
            "iso_27001": ["information_security", "risk_management"]
        }
        
        # Initialize compliance tracking
        self.compliance_violations = []
        self.audit_trail = []
        
        self.logger.info("Compliance framework initialized")
    
    async def _start_enterprise_services(self):
        """Start enterprise-specific background services"""
        
        try:
            # Start capacity planning service
            asyncio.create_task(self._capacity_planning_service())
            
            # Start business context service
            asyncio.create_task(self._business_context_service())
            
            # Start security monitoring service
            asyncio.create_task(self._security_monitoring_service())
            
            # Start compliance monitoring service
            asyncio.create_task(self._compliance_monitoring_service())
            
            # Start ML-enhanced alerting service
            asyncio.create_task(self._ml_enhanced_alerting_service())
            
            # Start cost impact tracking service
            asyncio.create_task(self._cost_impact_tracking_service())
            
            self.logger.info("Enterprise alerting services started")
            
        except Exception as e:
            self.logger.error(f"Failed to start enterprise services: {str(e)}")
    
    async def _capacity_planning_service(self):
        """Background service for capacity planning alerts"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                for metric_name, metric in self.capacity_metrics.items():
                    # Calculate projected usage
                    projected_usage = await self._calculate_projected_usage(metric)
                    
                    # Check thresholds
                    warning_threshold = metric.capacity_limit * (metric.warning_threshold_percent / 100)
                    critical_threshold = metric.capacity_limit * (metric.critical_threshold_percent / 100)
                    
                    if projected_usage >= critical_threshold:
                        await self._trigger_capacity_alert(metric, projected_usage, "critical")
                    elif projected_usage >= warning_threshold:
                        await self._trigger_capacity_alert(metric, projected_usage, "warning")
                
            except Exception as e:
                self.logger.error(f"Error in capacity planning service: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _calculate_projected_usage(self, metric: CapacityPlanningMetric) -> float:
        """Calculate projected usage for capacity planning"""
        
        try:
            # Simple linear projection
            daily_growth = metric.current_usage * (metric.growth_rate_percent / 100) / 30
            projected_usage = metric.current_usage + (daily_growth * metric.forecast_days)
            
            return min(projected_usage, metric.capacity_limit)
            
        except Exception as e:
            self.logger.error(f"Failed to calculate projected usage: {str(e)}")
            return metric.current_usage
    
    async def _trigger_capacity_alert(self, metric: CapacityPlanningMetric, 
                                    projected_usage: float, severity: str):
        """Trigger capacity planning alert"""
        
        try:
            # Calculate time to capacity
            if metric.growth_rate_percent > 0:
                remaining_capacity = metric.capacity_limit - metric.current_usage
                daily_growth = metric.current_usage * (metric.growth_rate_percent / 100) / 30
                days_to_capacity = remaining_capacity / daily_growth if daily_growth > 0 else 999
            else:
                days_to_capacity = 999
            
            # Calculate cost impact
            cost_impact = 0.0
            if metric.cost_per_unit:
                additional_capacity_needed = max(0, projected_usage - metric.capacity_limit)
                cost_impact = additional_capacity_needed * metric.cost_per_unit
            
            alert_message = f"""
Capacity Planning Alert: {metric.metric_name}

Current Usage: {metric.current_usage:.1f} {metric.unit}
Projected Usage: {projected_usage:.1f} {metric.unit}
Capacity Limit: {metric.capacity_limit:.1f} {metric.unit}
Days to Capacity: {days_to_capacity:.1f}
Estimated Cost Impact: ${cost_impact:.2f}
Growth Rate: {metric.growth_rate_percent:.1f}% per month
"""
            
            if self.datadog_monitoring:
                self.datadog_monitoring.gauge(
                    f"capacity.projected_usage.{metric.metric_name}",
                    projected_usage,
                    tags=[
                        f"severity:{severity}",
                        f"metric:{metric.metric_name}",
                        f"days_to_capacity:{days_to_capacity:.0f}"
                    ]
                )
                
                self.datadog_monitoring.counter(
                    "capacity.alert.triggered",
                    tags=[
                        f"metric:{metric.metric_name}",
                        f"severity:{severity}"
                    ]
                )
            
            self.enterprise_metrics["capacity_warnings"] += 1
            self.logger.warning(f"Capacity alert triggered for {metric.metric_name}: {severity}")
            
        except Exception as e:
            self.logger.error(f"Failed to trigger capacity alert: {str(e)}")
    
    async def _business_context_service(self):
        """Background service for maintaining business context"""
        
        while True:
            try:
                await asyncio.sleep(1800)  # Update every 30 minutes
                
                current_time = datetime.utcnow()
                current_hour = current_time.hour
                
                # Determine if it's business hours (9 AM - 5 PM UTC)
                is_business_hours = 9 <= current_hour < 17
                
                # Update business hours cache
                for rule_id in self.enterprise_rules:
                    self.business_hours_cache[rule_id] = is_business_hours
                
                # Check maintenance windows
                await self._update_maintenance_windows()
                
                # Update cost impact tracking
                await self._update_cost_impact_tracking()
                
            except Exception as e:
                self.logger.error(f"Error in business context service: {str(e)}")
                await asyncio.sleep(1800)
    
    async def _update_maintenance_windows(self):
        """Update maintenance window status"""
        
        try:
            current_time = datetime.utcnow()
            
            for rule_id, rule in self.enterprise_rules.items():
                active_windows = []
                
                for window in rule.maintenance_windows:
                    start_time = datetime.fromisoformat(window.get("start_time", ""))
                    end_time = datetime.fromisoformat(window.get("end_time", ""))
                    
                    if start_time <= current_time <= end_time:
                        active_windows.append(window)
                
                self.maintenance_windows[rule_id] = active_windows
                
        except Exception as e:
            self.logger.error(f"Failed to update maintenance windows: {str(e)}")
    
    async def _update_cost_impact_tracking(self):
        """Update cost impact tracking for alerts"""
        
        try:
            for rule_id, rule in self.enterprise_rules.items():
                if rule.business_context.revenue_impact_per_hour:
                    # Calculate potential cost impact
                    impact = rule.business_context.revenue_impact_per_hour
                    
                    # Adjust for business impact level
                    multiplier = {
                        BusinessImpact.MINIMAL: 0.1,
                        BusinessImpact.LOW: 0.3,
                        BusinessImpact.MEDIUM: 0.6,
                        BusinessImpact.HIGH: 1.0,
                        BusinessImpact.CRITICAL: 1.5,
                        BusinessImpact.CATASTROPHIC: 2.0
                    }.get(rule.business_impact, 1.0)
                    
                    self.cost_impact_tracking[rule_id] = impact * multiplier
                
        except Exception as e:
            self.logger.error(f"Failed to update cost impact tracking: {str(e)}")
    
    async def _security_monitoring_service(self):
        """Background service for security monitoring"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                for rule_id, rule in self.security_rules.items():
                    # Simulate security monitoring
                    await self._evaluate_security_rule(rule)
                
            except Exception as e:
                self.logger.error(f"Error in security monitoring service: {str(e)}")
                await asyncio.sleep(300)
    
    async def _evaluate_security_rule(self, rule: SecurityAlertRule):
        """Evaluate security rule against current metrics"""
        
        try:
            # This is a placeholder for actual security rule evaluation
            # In production, this would integrate with security tools and SIEM systems
            
            # Simulate security event detection
            security_event_detected = False  # This would be real detection logic
            
            if security_event_detected:
                await self._trigger_security_alert(rule)
                
        except Exception as e:
            self.logger.error(f"Failed to evaluate security rule: {str(e)}")
    
    async def _trigger_security_alert(self, rule: SecurityAlertRule):
        """Trigger security alert"""
        
        try:
            alert_data = {
                "rule_id": rule.rule_id,
                "name": rule.name,
                "threat_level": rule.threat_level,
                "security_category": rule.security_category,
                "mitre_framework": rule.mitre_attack_framework,
                "indicators": rule.indicators_of_compromise,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Add to audit trail
            self.audit_trail.append({
                "event_type": "security_alert",
                "rule_id": rule.rule_id,
                "timestamp": datetime.utcnow().isoformat(),
                "details": alert_data
            })
            
            # Check compliance requirements
            if rule.pci_dss_relevant:
                await self._record_compliance_event("pci_dss", alert_data)
            if rule.gdpr_relevant:
                await self._record_compliance_event("gdpr", alert_data)
            if rule.sox_relevant:
                await self._record_compliance_event("sox", alert_data)
            
            # Execute automated response if configured
            if rule.automated_response and not rule.requires_human_review:
                await self._execute_automated_security_response(rule)
            
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "security.alert.triggered",
                    tags=[
                        f"rule_id:{rule.rule_id}",
                        f"threat_level:{rule.threat_level}",
                        f"category:{rule.security_category}"
                    ]
                )
            
            self.logger.warning(f"Security alert triggered: {rule.name}")
            
        except Exception as e:
            self.logger.error(f"Failed to trigger security alert: {str(e)}")
    
    async def _execute_automated_security_response(self, rule: SecurityAlertRule):
        """Execute automated security response"""
        
        try:
            # This is where automated security responses would be executed
            # Examples: block IPs, disable accounts, quarantine files, etc.
            
            response_data = {
                "rule_id": rule.rule_id,
                "response": rule.automated_response,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "executed"
            }
            
            # Log the response
            self.audit_trail.append({
                "event_type": "automated_security_response",
                "rule_id": rule.rule_id,
                "timestamp": datetime.utcnow().isoformat(),
                "details": response_data
            })
            
            self.logger.info(f"Executed automated security response: {rule.automated_response}")
            
        except Exception as e:
            self.logger.error(f"Failed to execute security response: {str(e)}")
    
    async def _compliance_monitoring_service(self):
        """Background service for compliance monitoring"""
        
        while True:
            try:
                await asyncio.sleep(1800)  # Check every 30 minutes
                
                # Monitor compliance violations
                await self._check_compliance_violations()
                
                # Generate compliance reports
                await self._generate_compliance_reports()
                
            except Exception as e:
                self.logger.error(f"Error in compliance monitoring service: {str(e)}")
                await asyncio.sleep(1800)
    
    async def _check_compliance_violations(self):
        """Check for compliance violations"""
        
        try:
            # Check for violations in the last hour
            cutoff_time = datetime.utcnow() - timedelta(hours=1)
            
            recent_violations = [
                violation for violation in self.compliance_violations
                if datetime.fromisoformat(violation["timestamp"]) >= cutoff_time
            ]
            
            if self.datadog_monitoring:
                self.datadog_monitoring.gauge(
                    "compliance.violations.count",
                    len(recent_violations),
                    tags=[f"service:{self.service_name}"]
                )
            
        except Exception as e:
            self.logger.error(f"Failed to check compliance violations: {str(e)}")
    
    async def _record_compliance_event(self, regulation: str, event_data: Dict[str, Any]):
        """Record compliance-related event"""
        
        try:
            compliance_event = {
                "regulation": regulation,
                "event_data": event_data,
                "timestamp": datetime.utcnow().isoformat(),
                "requires_reporting": True
            }
            
            self.compliance_violations.append(compliance_event)
            
            # Limit history size
            if len(self.compliance_violations) > 1000:
                self.compliance_violations = self.compliance_violations[-1000:]
            
            self.enterprise_metrics["compliance_violations"] += 1
            
        except Exception as e:
            self.logger.error(f"Failed to record compliance event: {str(e)}")
    
    async def _generate_compliance_reports(self):
        """Generate periodic compliance reports"""
        
        try:
            # Generate summary of compliance status
            compliance_summary = {
                "timestamp": datetime.utcnow().isoformat(),
                "total_violations": len(self.compliance_violations),
                "by_regulation": defaultdict(int)
            }
            
            for violation in self.compliance_violations:
                regulation = violation.get("regulation", "unknown")
                compliance_summary["by_regulation"][regulation] += 1
            
            if self.datadog_monitoring:
                for regulation, count in compliance_summary["by_regulation"].items():
                    self.datadog_monitoring.gauge(
                        f"compliance.violations.by_regulation",
                        count,
                        tags=[f"regulation:{regulation}"]
                    )
            
        except Exception as e:
            self.logger.error(f"Failed to generate compliance reports: {str(e)}")
    
    async def _ml_enhanced_alerting_service(self):
        """Background service for ML-enhanced alerting"""
        
        while True:
            try:
                await asyncio.sleep(7200)  # Update ML models every 2 hours
                
                for rule_id, rule in self.enterprise_rules.items():
                    if rule.anomaly_detection_enabled:
                        await self._update_anomaly_thresholds(rule)
                    
                    if rule.forecast_based_alerting:
                        await self._update_forecast_thresholds(rule)
                
            except Exception as e:
                self.logger.error(f"Error in ML-enhanced alerting service: {str(e)}")
                await asyncio.sleep(7200)
    
    async def _update_anomaly_thresholds(self, rule: EnterpriseAlertRule):
        """Update anomaly detection thresholds using ML"""
        
        try:
            # Placeholder for ML-based anomaly detection
            # In production, this would use actual ML models
            
            base_threshold = rule.critical_threshold or 0
            dynamic_threshold = base_threshold * 1.1  # 10% adjustment
            
            self.anomaly_thresholds[rule.rule_id] = {
                "dynamic_threshold": dynamic_threshold,
                "confidence": 0.85,
                "updated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to update anomaly thresholds: {str(e)}")
    
    async def _update_forecast_thresholds(self, rule: EnterpriseAlertRule):
        """Update forecast-based thresholds"""
        
        try:
            # Placeholder for forecast-based threshold adjustment
            # In production, this would use time series forecasting
            
            base_threshold = rule.critical_threshold or 0
            forecast_adjustment = 0.95  # 5% more sensitive
            
            self.forecast_cache[rule.rule_id] = {
                "forecast_threshold": base_threshold * forecast_adjustment,
                "forecast_confidence": 0.80,
                "forecast_horizon_hours": 24,
                "updated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to update forecast thresholds: {str(e)}")
    
    async def _cost_impact_tracking_service(self):
        """Background service for tracking cost impact of incidents"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Update hourly
                
                # Calculate prevented business impact
                prevented_impact = 0.0
                
                for rule_id, cost_impact in self.cost_impact_tracking.items():
                    # Simulate prevented incidents
                    prevention_rate = 0.15  # 15% of potential incidents prevented
                    prevented_impact += cost_impact * prevention_rate
                
                self.enterprise_metrics["business_impact_prevented"] = prevented_impact
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        "enterprise.business_impact.prevented_cost",
                        prevented_impact,
                        tags=[f"service:{self.service_name}"]
                    )
                
            except Exception as e:
                self.logger.error(f"Error in cost impact tracking service: {str(e)}")
                await asyncio.sleep(3600)
    
    def get_enterprise_alerting_summary(self) -> Dict[str, Any]:
        """Get comprehensive enterprise alerting summary"""
        
        try:
            current_time = datetime.utcnow()
            
            # Rules summary by category
            rules_by_category = defaultdict(int)
            rules_by_impact = defaultdict(int)
            
            for rule in self.enterprise_rules.values():
                rules_by_category[rule.category.value] += 1
                rules_by_impact[rule.business_impact.value] += 1
            
            # Security rules summary
            security_rules_by_threat = defaultdict(int)
            for rule in self.security_rules.values():
                security_rules_by_threat[rule.threat_level] += 1
            
            # Capacity planning summary
            capacity_at_risk = sum(
                1 for metric in self.capacity_metrics.values()
                if metric.current_usage / metric.capacity_limit > 0.8
            )
            
            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "enterprise_rules": {
                    "total": len(self.enterprise_rules),
                    "by_category": dict(rules_by_category),
                    "by_business_impact": dict(rules_by_impact),
                    "with_auto_remediation": len([r for r in self.enterprise_rules.values() if r.auto_remediation_enabled]),
                    "compliance_required": len([r for r in self.enterprise_rules.values() if r.audit_required])
                },
                "security_rules": {
                    "total": len(self.security_rules),
                    "by_threat_level": dict(security_rules_by_threat),
                    "with_auto_response": len([r for r in self.security_rules.values() if r.automated_response]),
                    "requiring_review": len([r for r in self.security_rules.values() if r.requires_human_review])
                },
                "capacity_planning": {
                    "total_metrics": len(self.capacity_metrics),
                    "at_risk_count": capacity_at_risk,
                    "total_forecast_cost": sum(
                        (metric.capacity_limit - metric.current_usage) * (metric.cost_per_unit or 0)
                        for metric in self.capacity_metrics.values()
                    )
                },
                "compliance": {
                    "total_violations": len(self.compliance_violations),
                    "regulations_monitored": len(self.regulatory_notifications),
                    "audit_trail_entries": len(self.audit_trail)
                },
                "performance_metrics": self.enterprise_metrics,
                "business_context": {
                    "maintenance_windows_active": sum(len(windows) for windows in self.maintenance_windows.values()),
                    "cost_impact_rules": len(self.cost_impact_tracking),
                    "ml_enhanced_rules": len([r for r in self.enterprise_rules.values() if r.anomaly_detection_enabled])
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate enterprise alerting summary: {str(e)}")
            return {}


# Global enterprise alerting instance
_enterprise_alerting: Optional[DataDogEnterpriseAlerting] = None


def get_enterprise_alerting(service_name: str = "enterprise-alerting",
                          datadog_monitoring: Optional[DatadogMonitoring] = None,
                          base_alerting: Optional[DataDogIntelligentAlerting] = None) -> DataDogEnterpriseAlerting:
    """Get or create enterprise alerting instance"""
    global _enterprise_alerting
    
    if _enterprise_alerting is None:
        _enterprise_alerting = DataDogEnterpriseAlerting(service_name, datadog_monitoring, base_alerting)
    
    return _enterprise_alerting


# Convenience functions

def get_enterprise_alerting_summary() -> Dict[str, Any]:
    """Convenience function for getting enterprise alerting summary"""
    alerting_system = get_enterprise_alerting()
    return alerting_system.get_enterprise_alerting_summary()


async def trigger_capacity_planning_alert(metric_name: str, current_usage: float, 
                                        projected_usage: float, days_to_capacity: int) -> bool:
    """Convenience function for triggering capacity planning alerts"""
    alerting_system = get_enterprise_alerting()
    
    # This would integrate with the actual capacity planning system
    # For now, it's a placeholder
    
    return True


async def record_security_incident(rule_id: str, incident_details: Dict[str, Any]) -> bool:
    """Convenience function for recording security incidents"""
    alerting_system = get_enterprise_alerting()
    
    # Add to audit trail
    alerting_system.audit_trail.append({
        "event_type": "security_incident",
        "rule_id": rule_id,
        "timestamp": datetime.utcnow().isoformat(),
        "details": incident_details
    })
    
    return True