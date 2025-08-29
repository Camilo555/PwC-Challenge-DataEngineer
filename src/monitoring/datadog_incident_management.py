"""
DataDog Comprehensive Incident Management System
Provides enterprise-grade incident management with automated response,
escalation policies, and post-incident analysis
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable, Set
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, deque
import hashlib

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring, Alert, AlertPriority

logger = get_logger(__name__)


class IncidentStatus(Enum):
    """Incident status levels"""
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    INVESTIGATING = "investigating"
    IDENTIFIED = "identified"
    MONITORING = "monitoring"
    RESOLVED = "resolved"
    CLOSED = "closed"


class IncidentSeverity(Enum):
    """Incident severity levels aligned with business impact"""
    SEV1 = "sev1"  # Critical - Complete system outage
    SEV2 = "sev2"  # High - Major functionality impacted
    SEV3 = "sev3"  # Medium - Minor functionality impacted
    SEV4 = "sev4"  # Low - Cosmetic or documentation issues


class IncidentPriority(Enum):
    """Incident priority levels"""
    P1 = "p1"  # Critical - Fix immediately
    P2 = "p2"  # High - Fix within 4 hours
    P3 = "p3"  # Medium - Fix within 24 hours
    P4 = "p4"  # Low - Fix when convenient


class AlertCorrelationMethod(Enum):
    """Methods for correlating alerts"""
    TIME_BASED = "time_based"
    METRIC_SIMILARITY = "metric_similarity"
    SERVICE_DEPENDENCY = "service_dependency"
    ROOT_CAUSE = "root_cause"
    PATTERN_MATCHING = "pattern_matching"


@dataclass
class IncidentContext:
    """Business context for incidents"""
    affected_services: List[str] = field(default_factory=list)
    affected_customers: Optional[int] = None
    estimated_revenue_impact: Optional[float] = None
    sla_breach_risk: bool = False
    compliance_implications: List[str] = field(default_factory=list)
    business_units_affected: List[str] = field(default_factory=list)
    geographic_impact: List[str] = field(default_factory=list)


@dataclass
class ResponseAction:
    """Automated response action"""
    action_id: str
    name: str
    description: str
    action_type: str  # script, webhook, api_call, etc.
    config: Dict[str, Any]
    conditions: List[str] = field(default_factory=list)
    timeout_seconds: int = 300
    retry_attempts: int = 3
    requires_approval: bool = False


@dataclass
class EscalationPolicy:
    """Incident escalation policy"""
    policy_id: str
    name: str
    description: str
    severity_filter: List[IncidentSeverity] = field(default_factory=list)
    escalation_steps: List[Dict[str, Any]] = field(default_factory=list)
    business_hours_only: bool = False
    timezone: str = "UTC"


@dataclass
class Incident:
    """Comprehensive incident definition"""
    incident_id: str
    title: str
    description: str
    status: IncidentStatus
    severity: IncidentSeverity
    priority: IncidentPriority
    context: IncidentContext
    
    # Timing
    created_at: datetime
    updated_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    
    # Assignment
    assigned_to: Optional[str] = None
    incident_commander: Optional[str] = None
    response_team: List[str] = field(default_factory=list)
    
    # Alerts and correlation
    alerts: List[str] = field(default_factory=list)  # Alert IDs
    correlated_incidents: List[str] = field(default_factory=list)
    root_cause_analysis: Optional[Dict[str, Any]] = None
    
    # Response and resolution
    response_actions_taken: List[Dict[str, Any]] = field(default_factory=list)
    timeline: List[Dict[str, Any]] = field(default_factory=list)
    resolution_summary: Optional[str] = None
    
    # Metrics and tracking
    time_to_acknowledge: Optional[float] = None
    time_to_resolve: Optional[float] = None
    escalation_count: int = 0
    notification_count: int = 0
    
    # Tags and metadata
    tags: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert incident to dictionary"""
        result = asdict(self)
        
        # Convert datetime objects to ISO strings
        for field_name in ["created_at", "updated_at", "acknowledged_at", "resolved_at", "closed_at"]:
            if result.get(field_name):
                result[field_name] = result[field_name].isoformat()
        
        # Convert enums to strings
        result["status"] = self.status.value
        result["severity"] = self.severity.value
        result["priority"] = self.priority.value
        
        return result


@dataclass
class AlertCorrelationRule:
    """Rule for correlating alerts into incidents"""
    rule_id: str
    name: str
    description: str
    method: AlertCorrelationMethod
    conditions: Dict[str, Any]
    time_window_minutes: int = 15
    minimum_alerts: int = 2
    confidence_threshold: float = 0.8
    enabled: bool = True


class DataDogIncidentManagement:
    """
    Comprehensive DataDog Incident Management System
    
    Features:
    - Intelligent alert correlation and deduplication
    - Automated incident creation and lifecycle management
    - Multi-tier escalation policies with business awareness
    - Automated response and remediation workflows
    - Post-incident analysis and reporting
    - Integration with PagerDuty, Slack, Teams
    - SLA tracking and breach prevention
    """
    
    def __init__(self, service_name: str = "incident-management",
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Core data structures
        self.incidents: Dict[str, Incident] = {}
        self.correlation_rules: Dict[str, AlertCorrelationRule] = {}
        self.escalation_policies: Dict[str, EscalationPolicy] = {}
        self.response_actions: Dict[str, ResponseAction] = {}
        
        # Alert processing
        self.alert_queue: deque = deque()
        self.correlation_cache: Dict[str, List[str]] = {}  # Alert hash -> incident IDs
        self.alert_fingerprints: Dict[str, str] = {}  # Alert ID -> fingerprint
        
        # Incident tracking
        self.incident_metrics = {
            "total_incidents": 0,
            "active_incidents": 0,
            "resolved_incidents": 0,
            "mean_time_to_acknowledge": 0.0,
            "mean_time_to_resolve": 0.0,
            "escalation_rate": 0.0,
            "auto_resolution_rate": 0.0,
            "sla_breach_count": 0
        }
        
        # Initialize system
        self._initialize_correlation_rules()
        self._initialize_escalation_policies()
        self._initialize_response_actions()
        
        # Start background services
        asyncio.create_task(self._start_incident_services())
        
        self.logger.info(f"Incident management system initialized for {service_name}")
    
    def _initialize_correlation_rules(self):
        """Initialize alert correlation rules"""
        
        correlation_rules = [
            AlertCorrelationRule(
                rule_id="service_dependency_correlation",
                name="Service Dependency Correlation",
                description="Correlate alerts based on service dependencies",
                method=AlertCorrelationMethod.SERVICE_DEPENDENCY,
                conditions={
                    "dependency_graph": {
                        "api": ["database", "cache", "messaging"],
                        "frontend": ["api", "cdn"],
                        "etl": ["database", "messaging", "storage"]
                    }
                },
                time_window_minutes=10,
                minimum_alerts=2,
                confidence_threshold=0.9
            ),
            AlertCorrelationRule(
                rule_id="time_based_correlation",
                name="Time-Based Alert Correlation",
                description="Correlate alerts that occur within a short time window",
                method=AlertCorrelationMethod.TIME_BASED,
                conditions={
                    "time_window_seconds": 300,
                    "max_alerts": 10,
                    "service_filter": ["api", "database", "cache"]
                },
                time_window_minutes=5,
                minimum_alerts=3,
                confidence_threshold=0.7
            ),
            AlertCorrelationRule(
                rule_id="metric_similarity_correlation",
                name="Metric Similarity Correlation",
                description="Correlate alerts with similar metric patterns",
                method=AlertCorrelationMethod.METRIC_SIMILARITY,
                conditions={
                    "similarity_threshold": 0.8,
                    "metric_types": ["cpu", "memory", "response_time"],
                    "correlation_window": "15m"
                },
                time_window_minutes=15,
                minimum_alerts=2,
                confidence_threshold=0.8
            ),
            AlertCorrelationRule(
                rule_id="pattern_matching_correlation",
                name="Pattern Matching Correlation",
                description="Correlate alerts based on known failure patterns",
                method=AlertCorrelationMethod.PATTERN_MATCHING,
                conditions={
                    "patterns": [
                        {
                            "name": "database_cascade_failure",
                            "sequence": ["database.connection_error", "api.response_time_high", "queue.backlog_high"],
                            "time_window": "10m"
                        },
                        {
                            "name": "memory_leak_pattern",
                            "sequence": ["memory.usage_high", "gc.collection_time_high", "api.timeout"],
                            "time_window": "30m"
                        }
                    ]
                },
                time_window_minutes=30,
                minimum_alerts=2,
                confidence_threshold=0.85
            )
        ]
        
        for rule in correlation_rules:
            self.correlation_rules[rule.rule_id] = rule
        
        self.logger.info(f"Initialized {len(correlation_rules)} correlation rules")
    
    def _initialize_escalation_policies(self):
        """Initialize incident escalation policies"""
        
        escalation_policies = [
            EscalationPolicy(
                policy_id="critical_infrastructure",
                name="Critical Infrastructure Escalation",
                description="Escalation policy for SEV1/SEV2 infrastructure incidents",
                severity_filter=[IncidentSeverity.SEV1, IncidentSeverity.SEV2],
                escalation_steps=[
                    {
                        "step": 1,
                        "delay_minutes": 0,
                        "targets": ["oncall_engineer"],
                        "channels": ["pagerduty", "slack"]
                    },
                    {
                        "step": 2,
                        "delay_minutes": 15,
                        "targets": ["team_lead"],
                        "channels": ["pagerduty", "slack", "email"]
                    },
                    {
                        "step": 3,
                        "delay_minutes": 30,
                        "targets": ["engineering_manager"],
                        "channels": ["pagerduty", "email"]
                    },
                    {
                        "step": 4,
                        "delay_minutes": 60,
                        "targets": ["director_engineering"],
                        "channels": ["pagerduty", "email", "sms"]
                    }
                ],
                business_hours_only=False
            ),
            EscalationPolicy(
                policy_id="business_critical",
                name="Business Critical Escalation",
                description="Escalation policy for business-critical incidents",
                severity_filter=[IncidentSeverity.SEV1],
                escalation_steps=[
                    {
                        "step": 1,
                        "delay_minutes": 0,
                        "targets": ["incident_commander", "oncall_engineer"],
                        "channels": ["pagerduty", "slack", "teams"]
                    },
                    {
                        "step": 2,
                        "delay_minutes": 10,
                        "targets": ["engineering_manager", "product_manager"],
                        "channels": ["pagerduty", "email"]
                    },
                    {
                        "step": 3,
                        "delay_minutes": 30,
                        "targets": ["cto", "ceo"],
                        "channels": ["pagerduty", "email", "sms"]
                    }
                ],
                business_hours_only=False
            ),
            EscalationPolicy(
                policy_id="standard_operations",
                name="Standard Operations Escalation",
                description="Standard escalation policy for SEV3/SEV4 incidents",
                severity_filter=[IncidentSeverity.SEV3, IncidentSeverity.SEV4],
                escalation_steps=[
                    {
                        "step": 1,
                        "delay_minutes": 0,
                        "targets": ["team_member"],
                        "channels": ["slack", "email"]
                    },
                    {
                        "step": 2,
                        "delay_minutes": 60,
                        "targets": ["team_lead"],
                        "channels": ["slack", "email"]
                    },
                    {
                        "step": 3,
                        "delay_minutes": 240,
                        "targets": ["engineering_manager"],
                        "channels": ["email"]
                    }
                ],
                business_hours_only=True
            )
        ]
        
        for policy in escalation_policies:
            self.escalation_policies[policy.policy_id] = policy
        
        self.logger.info(f"Initialized {len(escalation_policies)} escalation policies")
    
    def _initialize_response_actions(self):
        """Initialize automated response actions"""
        
        response_actions = [
            ResponseAction(
                action_id="auto_scale_infrastructure",
                name="Auto Scale Infrastructure",
                description="Automatically scale infrastructure resources",
                action_type="api_call",
                config={
                    "endpoint": "/api/v1/infrastructure/scale",
                    "method": "POST",
                    "parameters": {
                        "resource_type": "compute",
                        "scale_factor": 1.5,
                        "max_instances": 20
                    }
                },
                conditions=["cpu_utilization > 80", "memory_utilization > 85"],
                timeout_seconds=180,
                retry_attempts=2
            ),
            ResponseAction(
                action_id="restart_unhealthy_services",
                name="Restart Unhealthy Services",
                description="Restart services showing health check failures",
                action_type="script",
                config={
                    "script_path": "/opt/scripts/restart_service.sh",
                    "parameters": {
                        "service_name": "${service}",
                        "graceful": True,
                        "timeout": 60
                    }
                },
                conditions=["health_check_failed", "response_time > threshold"],
                timeout_seconds=300,
                retry_attempts=3
            ),
            ResponseAction(
                action_id="enable_circuit_breaker",
                name="Enable Circuit Breaker",
                description="Enable circuit breaker to prevent cascade failures",
                action_type="api_call",
                config={
                    "endpoint": "/api/v1/circuit-breaker/enable",
                    "method": "POST",
                    "parameters": {
                        "service": "${service}",
                        "failure_threshold": 50,
                        "timeout": 30
                    }
                },
                conditions=["error_rate > 10", "dependency_failure"],
                timeout_seconds=60,
                retry_attempts=1
            ),
            ResponseAction(
                action_id="failover_to_backup",
                name="Failover to Backup",
                description="Failover to backup systems during outages",
                action_type="webhook",
                config={
                    "url": "https://disaster-recovery.company.com/failover",
                    "method": "POST",
                    "headers": {
                        "Authorization": "Bearer ${DR_TOKEN}",
                        "Content-Type": "application/json"
                    },
                    "payload": {
                        "service": "${service}",
                        "region": "${region}",
                        "backup_type": "hot_standby"
                    }
                },
                conditions=["service_unavailable", "region_failure"],
                timeout_seconds=600,
                retry_attempts=2,
                requires_approval=True
            ),
            ResponseAction(
                action_id="clear_cache",
                name="Clear Application Cache",
                description="Clear application cache to resolve data inconsistencies",
                action_type="api_call",
                config={
                    "endpoint": "/api/v1/cache/clear",
                    "method": "DELETE",
                    "parameters": {
                        "cache_type": "${cache_type}",
                        "pattern": "${cache_pattern}"
                    }
                },
                conditions=["cache_hit_rate_low", "data_inconsistency"],
                timeout_seconds=120,
                retry_attempts=2
            )
        ]
        
        for action in response_actions:
            self.response_actions[action.action_id] = action
        
        self.logger.info(f"Initialized {len(response_actions)} response actions")
    
    async def _start_incident_services(self):
        """Start incident management background services"""
        
        try:
            # Start alert correlation service
            asyncio.create_task(self._alert_correlation_service())
            
            # Start incident escalation service
            asyncio.create_task(self._incident_escalation_service())
            
            # Start automated response service
            asyncio.create_task(self._automated_response_service())
            
            # Start incident metrics service
            asyncio.create_task(self._incident_metrics_service())
            
            # Start incident cleanup service
            asyncio.create_task(self._incident_cleanup_service())
            
            self.logger.info("Incident management services started")
            
        except Exception as e:
            self.logger.error(f"Failed to start incident services: {str(e)}")
    
    async def process_alert(self, alert_data: Dict[str, Any]) -> Optional[str]:
        """Process incoming alert and potentially create or correlate with incident"""
        
        try:
            # Generate alert fingerprint for deduplication
            alert_fingerprint = self._generate_alert_fingerprint(alert_data)
            alert_id = alert_data.get("id", str(uuid.uuid4()))
            
            # Check for duplicate alerts
            if alert_fingerprint in self.alert_fingerprints.values():
                existing_alert_id = next(
                    alert_id for alert_id, fingerprint in self.alert_fingerprints.items()
                    if fingerprint == alert_fingerprint
                )
                self.logger.info(f"Deduplicated alert {alert_id} (matches {existing_alert_id})")
                return None
            
            self.alert_fingerprints[alert_id] = alert_fingerprint
            
            # Add alert to processing queue
            self.alert_queue.append({
                "alert_id": alert_id,
                "alert_data": alert_data,
                "fingerprint": alert_fingerprint,
                "timestamp": datetime.utcnow()
            })
            
            # Try to correlate with existing incidents
            incident_id = await self._correlate_alert_with_incidents(alert_id, alert_data)
            
            if incident_id:
                await self._add_alert_to_incident(incident_id, alert_id, alert_data)
                self.logger.info(f"Alert {alert_id} correlated with incident {incident_id}")
                return incident_id
            else:
                # Create new incident if alert meets criteria
                incident_id = await self._create_incident_from_alert(alert_id, alert_data)
                if incident_id:
                    self.logger.info(f"Created new incident {incident_id} from alert {alert_id}")
                return incident_id
        
        except Exception as e:
            self.logger.error(f"Failed to process alert {alert_data.get('id')}: {str(e)}")
            return None
    
    def _generate_alert_fingerprint(self, alert_data: Dict[str, Any]) -> str:
        """Generate unique fingerprint for alert deduplication"""
        
        try:
            # Key fields for fingerprint generation
            fingerprint_fields = [
                alert_data.get("metric_name", ""),
                alert_data.get("service", ""),
                alert_data.get("host", ""),
                alert_data.get("threshold_type", ""),
                str(alert_data.get("threshold_value", "")),
                alert_data.get("environment", "")
            ]
            
            fingerprint_string = "|".join(fingerprint_fields)
            return hashlib.md5(fingerprint_string.encode()).hexdigest()
            
        except Exception as e:
            self.logger.error(f"Failed to generate alert fingerprint: {str(e)}")
            return str(uuid.uuid4())
    
    async def _correlate_alert_with_incidents(self, alert_id: str, alert_data: Dict[str, Any]) -> Optional[str]:
        """Correlate alert with existing incidents"""
        
        try:
            current_time = datetime.utcnow()
            
            for rule_id, rule in self.correlation_rules.items():
                if not rule.enabled:
                    continue
                
                # Check time window
                time_window = timedelta(minutes=rule.time_window_minutes)
                
                for incident_id, incident in self.incidents.items():
                    # Skip resolved incidents
                    if incident.status in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]:
                        continue
                    
                    # Check if incident is within time window
                    if current_time - incident.created_at > time_window:
                        continue
                    
                    # Apply correlation method
                    confidence = await self._calculate_correlation_confidence(
                        rule, alert_data, incident
                    )
                    
                    if confidence >= rule.confidence_threshold:
                        return incident_id
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to correlate alert: {str(e)}")
            return None
    
    async def _calculate_correlation_confidence(self, rule: AlertCorrelationRule,
                                             alert_data: Dict[str, Any],
                                             incident: Incident) -> float:
        """Calculate confidence score for alert-incident correlation"""
        
        try:
            if rule.method == AlertCorrelationMethod.SERVICE_DEPENDENCY:
                return await self._calculate_service_dependency_confidence(
                    rule, alert_data, incident
                )
            elif rule.method == AlertCorrelationMethod.TIME_BASED:
                return await self._calculate_time_based_confidence(
                    rule, alert_data, incident
                )
            elif rule.method == AlertCorrelationMethod.METRIC_SIMILARITY:
                return await self._calculate_metric_similarity_confidence(
                    rule, alert_data, incident
                )
            elif rule.method == AlertCorrelationMethod.PATTERN_MATCHING:
                return await self._calculate_pattern_matching_confidence(
                    rule, alert_data, incident
                )
            else:
                return 0.0
                
        except Exception as e:
            self.logger.error(f"Failed to calculate correlation confidence: {str(e)}")
            return 0.0
    
    async def _calculate_service_dependency_confidence(self, rule: AlertCorrelationRule,
                                                     alert_data: Dict[str, Any],
                                                     incident: Incident) -> float:
        """Calculate confidence based on service dependencies"""
        
        try:
            alert_service = alert_data.get("service", "")
            incident_services = incident.context.affected_services
            
            dependency_graph = rule.conditions.get("dependency_graph", {})
            
            # Check if alert service is in dependency chain of incident services
            confidence = 0.0
            
            for incident_service in incident_services:
                if alert_service == incident_service:
                    confidence = 1.0
                    break
                elif alert_service in dependency_graph.get(incident_service, []):
                    confidence = max(confidence, 0.8)
                elif incident_service in dependency_graph.get(alert_service, []):
                    confidence = max(confidence, 0.7)
            
            return confidence
            
        except Exception as e:
            self.logger.error(f"Failed to calculate service dependency confidence: {str(e)}")
            return 0.0
    
    async def _calculate_time_based_confidence(self, rule: AlertCorrelationRule,
                                             alert_data: Dict[str, Any],
                                             incident: Incident) -> float:
        """Calculate confidence based on temporal proximity"""
        
        try:
            alert_time = datetime.utcnow()
            incident_time = incident.created_at
            
            time_diff_seconds = (alert_time - incident_time).total_seconds()
            max_time_window = rule.conditions.get("time_window_seconds", 300)
            
            if time_diff_seconds <= max_time_window:
                # Confidence decreases linearly with time
                confidence = 1.0 - (time_diff_seconds / max_time_window)
                return max(confidence, 0.0)
            
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Failed to calculate time-based confidence: {str(e)}")
            return 0.0
    
    async def _calculate_metric_similarity_confidence(self, rule: AlertCorrelationRule,
                                                    alert_data: Dict[str, Any],
                                                    incident: Incident) -> float:
        """Calculate confidence based on metric similarity"""
        
        try:
            # Placeholder for metric similarity calculation
            # In production, this would analyze metric patterns and values
            
            alert_metric = alert_data.get("metric_name", "")
            alert_value = alert_data.get("value", 0)
            
            # Simple similarity based on metric type
            similarity_threshold = rule.conditions.get("similarity_threshold", 0.8)
            metric_types = rule.conditions.get("metric_types", [])
            
            for metric_type in metric_types:
                if metric_type in alert_metric.lower():
                    return similarity_threshold
            
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Failed to calculate metric similarity confidence: {str(e)}")
            return 0.0
    
    async def _calculate_pattern_matching_confidence(self, rule: AlertCorrelationRule,
                                                   alert_data: Dict[str, Any],
                                                   incident: Incident) -> float:
        """Calculate confidence based on pattern matching"""
        
        try:
            patterns = rule.conditions.get("patterns", [])
            alert_metric = alert_data.get("metric_name", "")
            
            for pattern in patterns:
                sequence = pattern.get("sequence", [])
                
                # Check if alert metric matches any step in the pattern
                if any(step in alert_metric for step in sequence):
                    return 0.8
            
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Failed to calculate pattern matching confidence: {str(e)}")
            return 0.0
    
    async def _create_incident_from_alert(self, alert_id: str, alert_data: Dict[str, Any]) -> Optional[str]:
        """Create new incident from alert"""
        
        try:
            # Determine severity based on alert data
            severity = self._determine_incident_severity(alert_data)
            priority = self._determine_incident_priority(severity, alert_data)
            
            # Extract context from alert
            context = self._extract_incident_context(alert_data)
            
            # Generate incident ID
            incident_id = f"INC-{int(time.time())}-{str(uuid.uuid4())[:8].upper()}"
            
            # Create incident
            incident = Incident(
                incident_id=incident_id,
                title=self._generate_incident_title(alert_data),
                description=self._generate_incident_description(alert_data),
                status=IncidentStatus.OPEN,
                severity=severity,
                priority=priority,
                context=context,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                alerts=[alert_id],
                tags=self._extract_incident_tags(alert_data),
                labels=self._extract_incident_labels(alert_data)
            )
            
            # Store incident
            self.incidents[incident_id] = incident
            
            # Update metrics
            self.incident_metrics["total_incidents"] += 1
            self.incident_metrics["active_incidents"] += 1
            
            # Record in timeline
            await self._add_incident_timeline_entry(
                incident_id,
                "incident_created",
                f"Incident created from alert {alert_id}",
                {"alert_id": alert_id, "severity": severity.value}
            )
            
            # Trigger initial response
            await self._trigger_incident_response(incident_id)
            
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "incident.created",
                    tags=[
                        f"severity:{severity.value}",
                        f"priority:{priority.value}",
                        f"service:{context.affected_services[0] if context.affected_services else 'unknown'}"
                    ]
                )
            
            return incident_id
            
        except Exception as e:
            self.logger.error(f"Failed to create incident from alert: {str(e)}")
            return None
    
    def _determine_incident_severity(self, alert_data: Dict[str, Any]) -> IncidentSeverity:
        """Determine incident severity from alert data"""
        
        try:
            alert_priority = alert_data.get("priority", "").lower()
            metric_name = alert_data.get("metric_name", "").lower()
            value = alert_data.get("value", 0)
            
            # Business-critical metrics
            if any(keyword in metric_name for keyword in ["availability", "uptime", "outage"]):
                if value <= 90:  # Less than 90% availability
                    return IncidentSeverity.SEV1
                elif value <= 95:
                    return IncidentSeverity.SEV2
            
            # Performance-critical metrics
            if any(keyword in metric_name for keyword in ["response_time", "latency"]):
                if value >= 5000:  # > 5 seconds
                    return IncidentSeverity.SEV1
                elif value >= 2000:  # > 2 seconds
                    return IncidentSeverity.SEV2
                elif value >= 1000:  # > 1 second
                    return IncidentSeverity.SEV3
            
            # Error rate metrics
            if any(keyword in metric_name for keyword in ["error_rate", "failure_rate"]):
                if value >= 10:  # > 10% error rate
                    return IncidentSeverity.SEV1
                elif value >= 5:  # > 5% error rate
                    return IncidentSeverity.SEV2
                elif value >= 1:  # > 1% error rate
                    return IncidentSeverity.SEV3
            
            # Resource utilization
            if any(keyword in metric_name for keyword in ["cpu", "memory", "disk"]):
                if value >= 95:  # > 95% utilization
                    return IncidentSeverity.SEV2
                elif value >= 85:  # > 85% utilization
                    return IncidentSeverity.SEV3
            
            # Default based on alert priority
            if alert_priority in ["critical", "high"]:
                return IncidentSeverity.SEV2
            elif alert_priority == "medium":
                return IncidentSeverity.SEV3
            else:
                return IncidentSeverity.SEV4
                
        except Exception as e:
            self.logger.error(f"Failed to determine incident severity: {str(e)}")
            return IncidentSeverity.SEV3
    
    def _determine_incident_priority(self, severity: IncidentSeverity, alert_data: Dict[str, Any]) -> IncidentPriority:
        """Determine incident priority based on severity and business context"""
        
        try:
            # Check business hours and service criticality
            current_hour = datetime.utcnow().hour
            is_business_hours = 9 <= current_hour <= 17
            
            service = alert_data.get("service", "").lower()
            critical_services = ["api", "database", "payment", "auth"]
            
            if severity == IncidentSeverity.SEV1:
                return IncidentPriority.P1
            elif severity == IncidentSeverity.SEV2:
                if service in critical_services or is_business_hours:
                    return IncidentPriority.P1
                else:
                    return IncidentPriority.P2
            elif severity == IncidentSeverity.SEV3:
                if is_business_hours:
                    return IncidentPriority.P2
                else:
                    return IncidentPriority.P3
            else:
                return IncidentPriority.P4
                
        except Exception as e:
            self.logger.error(f"Failed to determine incident priority: {str(e)}")
            return IncidentPriority.P3
    
    def _extract_incident_context(self, alert_data: Dict[str, Any]) -> IncidentContext:
        """Extract incident context from alert data"""
        
        try:
            return IncidentContext(
                affected_services=[alert_data.get("service", "unknown")],
                sla_breach_risk=alert_data.get("sla_breach_risk", False),
                business_units_affected=alert_data.get("business_units", []),
                geographic_impact=alert_data.get("regions", [])
            )
            
        except Exception as e:
            self.logger.error(f"Failed to extract incident context: {str(e)}")
            return IncidentContext()
    
    def _generate_incident_title(self, alert_data: Dict[str, Any]) -> str:
        """Generate incident title from alert data"""
        
        try:
            service = alert_data.get("service", "Unknown Service")
            metric = alert_data.get("metric_name", "metric")
            
            return f"{service} - {metric.replace('_', ' ').title()} Alert"
            
        except Exception as e:
            self.logger.error(f"Failed to generate incident title: {str(e)}")
            return "Incident - Alert Triggered"
    
    def _generate_incident_description(self, alert_data: Dict[str, Any]) -> str:
        """Generate incident description from alert data"""
        
        try:
            service = alert_data.get("service", "unknown")
            metric = alert_data.get("metric_name", "metric")
            value = alert_data.get("value", "N/A")
            threshold = alert_data.get("threshold", "N/A")
            
            return f"""
Alert triggered for {service}:
- Metric: {metric}
- Current Value: {value}
- Threshold: {threshold}
- Time: {datetime.utcnow().isoformat()}

This incident was automatically created from alert monitoring.
"""
            
        except Exception as e:
            self.logger.error(f"Failed to generate incident description: {str(e)}")
            return "Incident automatically created from alert"
    
    def _extract_incident_tags(self, alert_data: Dict[str, Any]) -> List[str]:
        """Extract tags from alert data"""
        
        try:
            tags = []
            
            if service := alert_data.get("service"):
                tags.append(f"service:{service}")
            
            if environment := alert_data.get("environment"):
                tags.append(f"env:{environment}")
            
            if region := alert_data.get("region"):
                tags.append(f"region:{region}")
            
            if alert_data.get("auto_created"):
                tags.append("auto_created")
            
            return tags
            
        except Exception as e:
            self.logger.error(f"Failed to extract incident tags: {str(e)}")
            return []
    
    def _extract_incident_labels(self, alert_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract labels from alert data"""
        
        try:
            labels = {}
            
            if team := alert_data.get("team"):
                labels["team"] = team
            
            if owner := alert_data.get("owner"):
                labels["owner"] = owner
            
            if runbook := alert_data.get("runbook"):
                labels["runbook"] = runbook
            
            return labels
            
        except Exception as e:
            self.logger.error(f"Failed to extract incident labels: {str(e)}")
            return {}
    
    async def _add_alert_to_incident(self, incident_id: str, alert_id: str, alert_data: Dict[str, Any]):
        """Add alert to existing incident"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                self.logger.error(f"Incident {incident_id} not found")
                return
            
            # Add alert to incident
            if alert_id not in incident.alerts:
                incident.alerts.append(alert_id)
                incident.updated_at = datetime.utcnow()
                
                # Update incident context if needed
                if service := alert_data.get("service"):
                    if service not in incident.context.affected_services:
                        incident.context.affected_services.append(service)
                
                # Add to timeline
                await self._add_incident_timeline_entry(
                    incident_id,
                    "alert_correlated",
                    f"Alert {alert_id} correlated with incident",
                    {"alert_id": alert_id, "service": alert_data.get("service")}
                )
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "incident.alert_correlated",
                        tags=[f"incident_id:{incident_id}", f"severity:{incident.severity.value}"]
                    )
            
        except Exception as e:
            self.logger.error(f"Failed to add alert to incident: {str(e)}")
    
    async def _add_incident_timeline_entry(self, incident_id: str, event_type: str, 
                                         description: str, metadata: Optional[Dict[str, Any]] = None):
        """Add entry to incident timeline"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            timeline_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "event_type": event_type,
                "description": description,
                "metadata": metadata or {}
            }
            
            incident.timeline.append(timeline_entry)
            
        except Exception as e:
            self.logger.error(f"Failed to add timeline entry: {str(e)}")
    
    async def _trigger_incident_response(self, incident_id: str):
        """Trigger initial incident response"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            # Find appropriate escalation policy
            policy = self._find_escalation_policy(incident)
            if policy:
                await self._start_escalation_process(incident_id, policy)
            
            # Trigger automated responses
            await self._trigger_automated_responses(incident_id)
            
            # Send initial notifications
            await self._send_incident_notifications(incident_id, "incident_created")
            
        except Exception as e:
            self.logger.error(f"Failed to trigger incident response: {str(e)}")
    
    def _find_escalation_policy(self, incident: Incident) -> Optional[EscalationPolicy]:
        """Find appropriate escalation policy for incident"""
        
        try:
            for policy in self.escalation_policies.values():
                if incident.severity in policy.severity_filter:
                    return policy
            
            # Default policy for incidents without specific policy
            return next(iter(self.escalation_policies.values()), None)
            
        except Exception as e:
            self.logger.error(f"Failed to find escalation policy: {str(e)}")
            return None
    
    async def _start_escalation_process(self, incident_id: str, policy: EscalationPolicy):
        """Start escalation process for incident"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            for step in policy.escalation_steps:
                delay_minutes = step.get("delay_minutes", 0)
                
                # Schedule escalation step
                asyncio.create_task(
                    self._execute_escalation_step(incident_id, step, delay_minutes)
                )
            
            await self._add_incident_timeline_entry(
                incident_id,
                "escalation_started",
                f"Escalation process started using policy: {policy.name}",
                {"policy_id": policy.policy_id}
            )
            
        except Exception as e:
            self.logger.error(f"Failed to start escalation process: {str(e)}")
    
    async def _execute_escalation_step(self, incident_id: str, step: Dict[str, Any], delay_minutes: int):
        """Execute escalation step after delay"""
        
        try:
            if delay_minutes > 0:
                await asyncio.sleep(delay_minutes * 60)
            
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            # Check if incident is still active
            if incident.status in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]:
                return
            
            # Execute escalation
            targets = step.get("targets", [])
            channels = step.get("channels", [])
            
            # Send notifications to escalation targets
            for target in targets:
                for channel in channels:
                    await self._send_escalation_notification(incident_id, target, channel, step)
            
            # Update incident escalation count
            incident.escalation_count += 1
            
            await self._add_incident_timeline_entry(
                incident_id,
                "escalation_step_executed",
                f"Escalation step {step.get('step')} executed",
                {"step": step, "targets": targets, "channels": channels}
            )
            
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "incident.escalation_step",
                    tags=[f"incident_id:{incident_id}", f"step:{step.get('step')}"]
                )
            
        except Exception as e:
            self.logger.error(f"Failed to execute escalation step: {str(e)}")
    
    async def _send_escalation_notification(self, incident_id: str, target: str, 
                                          channel: str, step: Dict[str, Any]):
        """Send escalation notification"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            notification_data = {
                "incident_id": incident_id,
                "title": incident.title,
                "severity": incident.severity.value,
                "priority": incident.priority.value,
                "target": target,
                "channel": channel,
                "escalation_step": step.get("step"),
                "created_at": incident.created_at.isoformat(),
                "affected_services": incident.context.affected_services
            }
            
            # Log escalation notification
            self.logger.warning(
                f"Escalation notification sent for incident {incident_id}: "
                f"target={target}, channel={channel}, step={step.get('step')}"
            )
            
            # Update notification count
            incident.notification_count += 1
            
            # Here you would integrate with actual notification systems
            # (PagerDuty, Slack, email, etc.)
            
        except Exception as e:
            self.logger.error(f"Failed to send escalation notification: {str(e)}")
    
    async def _trigger_automated_responses(self, incident_id: str):
        """Trigger automated response actions for incident"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            # Find applicable response actions
            applicable_actions = []
            
            for action in self.response_actions.values():
                if await self._check_action_conditions(action, incident):
                    applicable_actions.append(action)
            
            # Execute actions
            for action in applicable_actions:
                asyncio.create_task(self._execute_response_action(incident_id, action))
            
            if applicable_actions:
                await self._add_incident_timeline_entry(
                    incident_id,
                    "automated_responses_triggered",
                    f"Triggered {len(applicable_actions)} automated response actions",
                    {"actions": [action.action_id for action in applicable_actions]}
                )
            
        except Exception as e:
            self.logger.error(f"Failed to trigger automated responses: {str(e)}")
    
    async def _check_action_conditions(self, action: ResponseAction, incident: Incident) -> bool:
        """Check if response action conditions are met"""
        
        try:
            # Placeholder for condition checking logic
            # In production, this would evaluate the actual conditions
            
            for condition in action.conditions:
                # Simple condition checking
                if "cpu_utilization" in condition and incident.severity in [IncidentSeverity.SEV1, IncidentSeverity.SEV2]:
                    return True
                if "health_check_failed" in condition:
                    return True
                if "error_rate" in condition and incident.severity == IncidentSeverity.SEV1:
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to check action conditions: {str(e)}")
            return False
    
    async def _execute_response_action(self, incident_id: str, action: ResponseAction):
        """Execute automated response action"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            # Check if action requires approval
            if action.requires_approval:
                await self._request_action_approval(incident_id, action)
                return
            
            # Execute action based on type
            action_result = None
            
            if action.action_type == "api_call":
                action_result = await self._execute_api_call_action(action, incident)
            elif action.action_type == "script":
                action_result = await self._execute_script_action(action, incident)
            elif action.action_type == "webhook":
                action_result = await self._execute_webhook_action(action, incident)
            
            # Record action execution
            action_record = {
                "action_id": action.action_id,
                "action_name": action.name,
                "executed_at": datetime.utcnow().isoformat(),
                "result": action_result,
                "success": action_result is not None
            }
            
            incident.response_actions_taken.append(action_record)
            
            await self._add_incident_timeline_entry(
                incident_id,
                "response_action_executed",
                f"Executed response action: {action.name}",
                {"action": action_record}
            )
            
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "incident.response_action_executed",
                    tags=[
                        f"incident_id:{incident_id}",
                        f"action_id:{action.action_id}",
                        f"success:{action_result is not None}"
                    ]
                )
            
        except Exception as e:
            self.logger.error(f"Failed to execute response action: {str(e)}")
    
    async def _execute_api_call_action(self, action: ResponseAction, incident: Incident) -> Optional[Dict[str, Any]]:
        """Execute API call response action"""
        
        try:
            # Placeholder for API call execution
            # In production, this would make actual API calls
            
            endpoint = action.config.get("endpoint", "")
            method = action.config.get("method", "GET")
            parameters = action.config.get("parameters", {})
            
            self.logger.info(f"Executing API call action: {method} {endpoint}")
            
            # Simulate successful API call
            return {
                "status": "success",
                "endpoint": endpoint,
                "method": method,
                "response_code": 200
            }
            
        except Exception as e:
            self.logger.error(f"Failed to execute API call action: {str(e)}")
            return None
    
    async def _execute_script_action(self, action: ResponseAction, incident: Incident) -> Optional[Dict[str, Any]]:
        """Execute script response action"""
        
        try:
            # Placeholder for script execution
            # In production, this would execute actual scripts
            
            script_path = action.config.get("script_path", "")
            parameters = action.config.get("parameters", {})
            
            self.logger.info(f"Executing script action: {script_path}")
            
            # Simulate successful script execution
            return {
                "status": "success",
                "script_path": script_path,
                "exit_code": 0
            }
            
        except Exception as e:
            self.logger.error(f"Failed to execute script action: {str(e)}")
            return None
    
    async def _execute_webhook_action(self, action: ResponseAction, incident: Incident) -> Optional[Dict[str, Any]]:
        """Execute webhook response action"""
        
        try:
            # Placeholder for webhook execution
            # In production, this would make actual webhook calls
            
            url = action.config.get("url", "")
            method = action.config.get("method", "POST")
            payload = action.config.get("payload", {})
            
            self.logger.info(f"Executing webhook action: {method} {url}")
            
            # Simulate successful webhook call
            return {
                "status": "success",
                "url": url,
                "method": method,
                "response_code": 200
            }
            
        except Exception as e:
            self.logger.error(f"Failed to execute webhook action: {str(e)}")
            return None
    
    async def _request_action_approval(self, incident_id: str, action: ResponseAction):
        """Request approval for action execution"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            approval_request = {
                "incident_id": incident_id,
                "action_id": action.action_id,
                "action_name": action.name,
                "action_description": action.description,
                "requested_at": datetime.utcnow().isoformat(),
                "status": "pending"
            }
            
            await self._add_incident_timeline_entry(
                incident_id,
                "action_approval_requested",
                f"Approval requested for action: {action.name}",
                {"approval_request": approval_request}
            )
            
            # Here you would integrate with approval workflow systems
            
        except Exception as e:
            self.logger.error(f"Failed to request action approval: {str(e)}")
    
    async def _send_incident_notifications(self, incident_id: str, notification_type: str):
        """Send incident notifications"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            notification_data = {
                "incident_id": incident_id,
                "title": incident.title,
                "description": incident.description,
                "severity": incident.severity.value,
                "priority": incident.priority.value,
                "status": incident.status.value,
                "created_at": incident.created_at.isoformat(),
                "affected_services": incident.context.affected_services,
                "notification_type": notification_type
            }
            
            # Log notification
            self.logger.info(f"Sending incident notification: {notification_type} for {incident_id}")
            
            # Here you would integrate with actual notification systems
            
        except Exception as e:
            self.logger.error(f"Failed to send incident notifications: {str(e)}")
    
    async def _alert_correlation_service(self):
        """Background service for alert correlation"""
        
        while True:
            try:
                await asyncio.sleep(60)  # Run every minute
                
                # Process pending alerts
                while self.alert_queue:
                    alert_data = self.alert_queue.popleft()
                    
                    # Try to correlate with existing incidents
                    await self._process_queued_alert(alert_data)
                
            except Exception as e:
                self.logger.error(f"Error in alert correlation service: {str(e)}")
                await asyncio.sleep(60)
    
    async def _process_queued_alert(self, alert_data: Dict[str, Any]):
        """Process queued alert for correlation"""
        
        try:
            alert_id = alert_data["alert_id"]
            alert_info = alert_data["alert_data"]
            
            # Try correlation again (in case new incidents were created)
            incident_id = await self._correlate_alert_with_incidents(alert_id, alert_info)
            
            if incident_id:
                await self._add_alert_to_incident(incident_id, alert_id, alert_info)
            
        except Exception as e:
            self.logger.error(f"Failed to process queued alert: {str(e)}")
    
    async def _incident_escalation_service(self):
        """Background service for incident escalation monitoring"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                current_time = datetime.utcnow()
                
                for incident_id, incident in self.incidents.items():
                    # Skip resolved incidents
                    if incident.status in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]:
                        continue
                    
                    # Check for SLA breaches
                    await self._check_sla_breach(incident_id, incident, current_time)
                    
                    # Check for stale incidents
                    await self._check_stale_incident(incident_id, incident, current_time)
                
            except Exception as e:
                self.logger.error(f"Error in incident escalation service: {str(e)}")
                await asyncio.sleep(300)
    
    async def _check_sla_breach(self, incident_id: str, incident: Incident, current_time: datetime):
        """Check for SLA breaches"""
        
        try:
            # Define SLA targets based on priority
            sla_targets = {
                IncidentPriority.P1: timedelta(hours=1),
                IncidentPriority.P2: timedelta(hours=4),
                IncidentPriority.P3: timedelta(hours=24),
                IncidentPriority.P4: timedelta(days=3)
            }
            
            sla_target = sla_targets.get(incident.priority, timedelta(hours=24))
            time_since_created = current_time - incident.created_at
            
            if time_since_created > sla_target and incident.status != IncidentStatus.RESOLVED:
                # SLA breach detected
                incident.context.sla_breach_risk = True
                
                await self._add_incident_timeline_entry(
                    incident_id,
                    "sla_breach_detected",
                    f"SLA breach detected - target: {sla_target}, actual: {time_since_created}",
                    {"sla_target_hours": sla_target.total_seconds() / 3600}
                )
                
                # Trigger additional escalation
                await self._trigger_sla_breach_escalation(incident_id)
                
                self.incident_metrics["sla_breach_count"] += 1
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "incident.sla_breach",
                        tags=[f"incident_id:{incident_id}", f"priority:{incident.priority.value}"]
                    )
            
        except Exception as e:
            self.logger.error(f"Failed to check SLA breach: {str(e)}")
    
    async def _trigger_sla_breach_escalation(self, incident_id: str):
        """Trigger escalation for SLA breach"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return
            
            # Send urgent notifications
            await self._send_incident_notifications(incident_id, "sla_breach")
            
            # Auto-assign incident commander if not already assigned
            if not incident.incident_commander:
                incident.incident_commander = "sla_breach_commander"
            
        except Exception as e:
            self.logger.error(f"Failed to trigger SLA breach escalation: {str(e)}")
    
    async def _check_stale_incident(self, incident_id: str, incident: Incident, current_time: datetime):
        """Check for stale incidents"""
        
        try:
            # Consider incident stale if no updates in 2 hours
            stale_threshold = timedelta(hours=2)
            time_since_update = current_time - incident.updated_at
            
            if time_since_update > stale_threshold:
                await self._add_incident_timeline_entry(
                    incident_id,
                    "stale_incident_detected",
                    f"Incident appears stale - no updates in {time_since_update}",
                    {"hours_since_update": time_since_update.total_seconds() / 3600}
                )
                
                # Send reminder notifications
                await self._send_incident_notifications(incident_id, "stale_reminder")
            
        except Exception as e:
            self.logger.error(f"Failed to check stale incident: {str(e)}")
    
    async def _automated_response_service(self):
        """Background service for monitoring automated response effectiveness"""
        
        while True:
            try:
                await asyncio.sleep(1800)  # Check every 30 minutes
                
                # Track automated response success rates
                await self._update_response_metrics()
                
                # Check for failed automated responses
                await self._check_failed_responses()
                
            except Exception as e:
                self.logger.error(f"Error in automated response service: {str(e)}")
                await asyncio.sleep(1800)
    
    async def _update_response_metrics(self):
        """Update automated response metrics"""
        
        try:
            successful_responses = 0
            total_responses = 0
            
            for incident in self.incidents.values():
                for action in incident.response_actions_taken:
                    total_responses += 1
                    if action.get("success", False):
                        successful_responses += 1
            
            if total_responses > 0:
                success_rate = successful_responses / total_responses
                self.incident_metrics["auto_resolution_rate"] = success_rate
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        "incident.auto_response.success_rate",
                        success_rate,
                        tags=[f"service:{self.service_name}"]
                    )
            
        except Exception as e:
            self.logger.error(f"Failed to update response metrics: {str(e)}")
    
    async def _check_failed_responses(self):
        """Check for failed automated responses"""
        
        try:
            for incident in self.incidents.values():
                for action in incident.response_actions_taken:
                    if not action.get("success", False):
                        # Log failed response for investigation
                        self.logger.warning(
                            f"Failed automated response in incident {incident.incident_id}: "
                            f"action={action.get('action_name')}"
                        )
            
        except Exception as e:
            self.logger.error(f"Failed to check failed responses: {str(e)}")
    
    async def _incident_metrics_service(self):
        """Background service for incident metrics collection"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Update every 5 minutes
                
                await self._calculate_incident_metrics()
                await self._publish_incident_metrics()
                
            except Exception as e:
                self.logger.error(f"Error in incident metrics service: {str(e)}")
                await asyncio.sleep(300)
    
    async def _calculate_incident_metrics(self):
        """Calculate incident metrics"""
        
        try:
            active_incidents = len([
                i for i in self.incidents.values() 
                if i.status not in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]
            ])
            
            resolved_incidents = len([
                i for i in self.incidents.values() 
                if i.status == IncidentStatus.RESOLVED
            ])
            
            # Calculate MTTA (Mean Time To Acknowledge)
            acknowledged_incidents = [
                i for i in self.incidents.values() 
                if i.acknowledged_at
            ]
            
            if acknowledged_incidents:
                total_tta = sum(
                    (i.acknowledged_at - i.created_at).total_seconds()
                    for i in acknowledged_incidents
                )
                mtta_seconds = total_tta / len(acknowledged_incidents)
                self.incident_metrics["mean_time_to_acknowledge"] = mtta_seconds / 60  # Convert to minutes
            
            # Calculate MTTR (Mean Time To Resolve)
            resolved_with_time = [
                i for i in self.incidents.values() 
                if i.resolved_at
            ]
            
            if resolved_with_time:
                total_ttr = sum(
                    (i.resolved_at - i.created_at).total_seconds()
                    for i in resolved_with_time
                )
                mttr_seconds = total_ttr / len(resolved_with_time)
                self.incident_metrics["mean_time_to_resolve"] = mttr_seconds / 60  # Convert to minutes
            
            # Calculate escalation rate
            total_incidents = len(self.incidents)
            escalated_incidents = len([
                i for i in self.incidents.values() 
                if i.escalation_count > 0
            ])
            
            if total_incidents > 0:
                self.incident_metrics["escalation_rate"] = escalated_incidents / total_incidents
            
            # Update current counts
            self.incident_metrics["active_incidents"] = active_incidents
            self.incident_metrics["resolved_incidents"] = resolved_incidents
            
        except Exception as e:
            self.logger.error(f"Failed to calculate incident metrics: {str(e)}")
    
    async def _publish_incident_metrics(self):
        """Publish incident metrics to DataDog"""
        
        try:
            if self.datadog_monitoring:
                for metric_name, value in self.incident_metrics.items():
                    self.datadog_monitoring.gauge(
                        f"incident.{metric_name}",
                        value,
                        tags=[f"service:{self.service_name}"]
                    )
            
        except Exception as e:
            self.logger.error(f"Failed to publish incident metrics: {str(e)}")
    
    async def _incident_cleanup_service(self):
        """Background service for incident cleanup"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                # Archive old resolved incidents
                await self._archive_old_incidents()
                
                # Clean up alert fingerprints
                await self._cleanup_alert_fingerprints()
                
            except Exception as e:
                self.logger.error(f"Error in incident cleanup service: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _archive_old_incidents(self):
        """Archive old resolved incidents"""
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=30)
            incidents_to_archive = []
            
            for incident_id, incident in self.incidents.items():
                if (incident.status in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED] and
                    incident.resolved_at and incident.resolved_at < cutoff_date):
                    incidents_to_archive.append(incident_id)
            
            # Archive incidents (in production, move to archive storage)
            for incident_id in incidents_to_archive:
                archived_incident = self.incidents.pop(incident_id)
                
                # Log archival
                self.logger.info(f"Archived incident {incident_id}")
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "incident.archived",
                        tags=[f"incident_id:{incident_id}"]
                    )
            
            if incidents_to_archive:
                self.logger.info(f"Archived {len(incidents_to_archive)} old incidents")
            
        except Exception as e:
            self.logger.error(f"Failed to archive old incidents: {str(e)}")
    
    async def _cleanup_alert_fingerprints(self):
        """Clean up old alert fingerprints"""
        
        try:
            # Keep fingerprints for 24 hours to prevent duplicate alert processing
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            
            # In production, you would track alert timestamps
            # For now, limit the size of the fingerprint cache
            if len(self.alert_fingerprints) > 10000:
                # Remove oldest 20% of fingerprints
                items_to_remove = len(self.alert_fingerprints) // 5
                alert_ids_to_remove = list(self.alert_fingerprints.keys())[:items_to_remove]
                
                for alert_id in alert_ids_to_remove:
                    del self.alert_fingerprints[alert_id]
                
                self.logger.info(f"Cleaned up {items_to_remove} alert fingerprints")
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup alert fingerprints: {str(e)}")
    
    def acknowledge_incident(self, incident_id: str, acknowledged_by: str) -> bool:
        """Acknowledge incident"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return False
            
            if incident.status == IncidentStatus.OPEN:
                incident.status = IncidentStatus.ACKNOWLEDGED
                incident.acknowledged_at = datetime.utcnow()
                incident.updated_at = datetime.utcnow()
                
                # Calculate time to acknowledge
                incident.time_to_acknowledge = (
                    incident.acknowledged_at - incident.created_at
                ).total_seconds()
                
                # Add to timeline
                asyncio.create_task(
                    self._add_incident_timeline_entry(
                        incident_id,
                        "incident_acknowledged",
                        f"Incident acknowledged by {acknowledged_by}",
                        {"acknowledged_by": acknowledged_by}
                    )
                )
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "incident.acknowledged",
                        tags=[f"incident_id:{incident_id}", f"severity:{incident.severity.value}"]
                    )
                
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to acknowledge incident: {str(e)}")
            return False
    
    def resolve_incident(self, incident_id: str, resolved_by: str, resolution_summary: str) -> bool:
        """Resolve incident"""
        
        try:
            incident = self.incidents.get(incident_id)
            if not incident:
                return False
            
            if incident.status not in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]:
                incident.status = IncidentStatus.RESOLVED
                incident.resolved_at = datetime.utcnow()
                incident.updated_at = datetime.utcnow()
                incident.resolution_summary = resolution_summary
                
                # Calculate time to resolve
                incident.time_to_resolve = (
                    incident.resolved_at - incident.created_at
                ).total_seconds()
                
                # Update metrics
                self.incident_metrics["active_incidents"] -= 1
                self.incident_metrics["resolved_incidents"] += 1
                
                # Add to timeline
                asyncio.create_task(
                    self._add_incident_timeline_entry(
                        incident_id,
                        "incident_resolved",
                        f"Incident resolved by {resolved_by}: {resolution_summary}",
                        {"resolved_by": resolved_by, "resolution_summary": resolution_summary}
                    )
                )
                
                # Send resolution notifications
                asyncio.create_task(
                    self._send_incident_notifications(incident_id, "incident_resolved")
                )
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.counter(
                        "incident.resolved",
                        tags=[f"incident_id:{incident_id}", f"severity:{incident.severity.value}"]
                    )
                    
                    self.datadog_monitoring.histogram(
                        "incident.time_to_resolve",
                        incident.time_to_resolve,
                        tags=[f"severity:{incident.severity.value}"]
                    )
                
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to resolve incident: {str(e)}")
            return False
    
    def get_incident(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Get incident details"""
        
        try:
            incident = self.incidents.get(incident_id)
            if incident:
                return incident.to_dict()
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get incident: {str(e)}")
            return None
    
    def list_active_incidents(self) -> List[Dict[str, Any]]:
        """List all active incidents"""
        
        try:
            active_incidents = []
            
            for incident in self.incidents.values():
                if incident.status not in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]:
                    active_incidents.append(incident.to_dict())
            
            # Sort by priority and creation time
            priority_order = {
                IncidentPriority.P1: 1,
                IncidentPriority.P2: 2,
                IncidentPriority.P3: 3,
                IncidentPriority.P4: 4
            }
            
            active_incidents.sort(
                key=lambda x: (
                    priority_order.get(IncidentPriority(x["priority"]), 5),
                    x["created_at"]
                )
            )
            
            return active_incidents
            
        except Exception as e:
            self.logger.error(f"Failed to list active incidents: {str(e)}")
            return []
    
    def get_incident_metrics(self) -> Dict[str, Any]:
        """Get incident management metrics"""
        
        try:
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "service_name": self.service_name,
                "metrics": self.incident_metrics.copy(),
                "system_health": {
                    "correlation_rules_active": len([r for r in self.correlation_rules.values() if r.enabled]),
                    "escalation_policies": len(self.escalation_policies),
                    "response_actions": len(self.response_actions),
                    "alert_queue_size": len(self.alert_queue),
                    "alert_fingerprints_cached": len(self.alert_fingerprints)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get incident metrics: {str(e)}")
            return {}
    
    def get_incident_management_summary(self) -> Dict[str, Any]:
        """Get comprehensive incident management summary"""
        
        try:
            current_time = datetime.utcnow()
            
            # Incident statistics
            total_incidents = len(self.incidents)
            active_incidents = len([
                i for i in self.incidents.values() 
                if i.status not in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]
            ])
            
            # Severity breakdown
            severity_breakdown = defaultdict(int)
            for incident in self.incidents.values():
                severity_breakdown[incident.severity.value] += 1
            
            # Priority breakdown
            priority_breakdown = defaultdict(int)
            for incident in self.incidents.values():
                priority_breakdown[incident.priority.value] += 1
            
            # Recent incidents (last 24 hours)
            recent_cutoff = current_time - timedelta(hours=24)
            recent_incidents = len([
                i for i in self.incidents.values() 
                if i.created_at >= recent_cutoff
            ])
            
            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "incident_statistics": {
                    "total_incidents": total_incidents,
                    "active_incidents": active_incidents,
                    "recent_incidents_24h": recent_incidents,
                    "by_severity": dict(severity_breakdown),
                    "by_priority": dict(priority_breakdown)
                },
                "performance_metrics": self.incident_metrics,
                "system_configuration": {
                    "correlation_rules": len(self.correlation_rules),
                    "escalation_policies": len(self.escalation_policies),
                    "response_actions": len(self.response_actions)
                },
                "operational_status": {
                    "alert_queue_size": len(self.alert_queue),
                    "correlation_cache_entries": len(self.correlation_cache),
                    "alert_fingerprints_cached": len(self.alert_fingerprints)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate incident management summary: {str(e)}")
            return {}


# Global incident management instance
_incident_management: Optional[DataDogIncidentManagement] = None


def get_incident_management(service_name: str = "incident-management",
                          datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogIncidentManagement:
    """Get or create incident management instance"""
    global _incident_management
    
    if _incident_management is None:
        _incident_management = DataDogIncidentManagement(service_name, datadog_monitoring)
    
    return _incident_management


# Convenience functions

async def process_alert_for_incident(alert_data: Dict[str, Any]) -> Optional[str]:
    """Convenience function for processing alerts"""
    incident_mgr = get_incident_management()
    return await incident_mgr.process_alert(alert_data)


def acknowledge_incident_by_id(incident_id: str, acknowledged_by: str) -> bool:
    """Convenience function for acknowledging incidents"""
    incident_mgr = get_incident_management()
    return incident_mgr.acknowledge_incident(incident_id, acknowledged_by)


def resolve_incident_by_id(incident_id: str, resolved_by: str, resolution_summary: str) -> bool:
    """Convenience function for resolving incidents"""
    incident_mgr = get_incident_management()
    return incident_mgr.resolve_incident(incident_id, resolved_by, resolution_summary)


def get_active_incidents() -> List[Dict[str, Any]]:
    """Convenience function for getting active incidents"""
    incident_mgr = get_incident_management()
    return incident_mgr.list_active_incidents()


def get_incident_management_summary() -> Dict[str, Any]:
    """Convenience function for getting incident management summary"""
    incident_mgr = get_incident_management()
    return incident_mgr.get_incident_management_summary()