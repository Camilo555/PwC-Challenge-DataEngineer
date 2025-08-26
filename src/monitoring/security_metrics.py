"""
Advanced Security Metrics Collection System
Integrates with the enterprise security framework to provide comprehensive
security event tracking, threat analytics, and compliance monitoring.
"""
import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
import threading

from core.logging import get_logger
from core.monitoring.metrics import MetricsCollector, MetricPoint, MetricType
from core.security.advanced_security import SecurityEventType, ThreatLevel, ActionType
from core.security.enterprise_dlp import SensitiveDataType, DLPAction, DataClassification
from core.security.compliance_framework import ComplianceFramework
from monitoring.advanced_observability import ObservabilityCollector

logger = get_logger(__name__)


class SecurityMetricType(Enum):
    """Types of security metrics"""
    THREAT_DETECTION = "threat_detection"
    ACCESS_CONTROL = "access_control"
    DLP_VIOLATION = "dlp_violation"
    COMPLIANCE_CHECK = "compliance_check"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_GOVERNANCE = "data_governance"
    INCIDENT_RESPONSE = "incident_response"
    SECURITY_SCANNING = "security_scanning"
    VULNERABILITY = "vulnerability"


class SecurityEventSeverity(Enum):
    """Security event severity levels"""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class SecurityMetricEvent:
    """Individual security metric event"""
    event_id: str
    metric_type: SecurityMetricType
    severity: SecurityEventSeverity
    user_id: Optional[str]
    source_ip: Optional[str]
    resource_id: str
    action: str
    outcome: str  # success, failure, blocked, etc.
    details: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    session_id: Optional[str] = None
    threat_indicators: List[str] = field(default_factory=list)
    compliance_frameworks: List[ComplianceFramework] = field(default_factory=list)


@dataclass
class ThreatDetectionMetrics:
    """Threat detection metrics aggregation"""
    total_threats_detected: int = 0
    threats_by_type: Dict[str, int] = field(default_factory=dict)
    threats_by_severity: Dict[SecurityEventSeverity, int] = field(default_factory=dict)
    false_positive_rate: float = 0.0
    detection_accuracy: float = 0.0
    avg_response_time_seconds: float = 0.0
    blocked_attempts: int = 0
    active_investigations: int = 0


@dataclass
class DLPMetrics:
    """DLP metrics aggregation"""
    total_scans: int = 0
    violations_detected: int = 0
    data_types_detected: Dict[SensitiveDataType, int] = field(default_factory=dict)
    actions_taken: Dict[DLPAction, int] = field(default_factory=dict)
    policy_effectiveness: Dict[str, float] = field(default_factory=dict)
    false_positive_rate: float = 0.0
    redactions_performed: int = 0
    encryption_operations: int = 0


@dataclass
class ComplianceMetrics:
    """Compliance metrics aggregation"""
    framework_scores: Dict[ComplianceFramework, float] = field(default_factory=dict)
    total_violations: int = 0
    violations_by_framework: Dict[ComplianceFramework, int] = field(default_factory=dict)
    remediation_time_avg: float = 0.0
    overdue_remediations: int = 0
    compliance_drift: float = 0.0
    audit_readiness_score: float = 0.0


@dataclass
class AccessControlMetrics:
    """Access control metrics aggregation"""
    total_requests: int = 0
    approved_requests: int = 0
    denied_requests: int = 0
    conditional_approvals: int = 0
    approval_rate: float = 0.0
    avg_decision_time_ms: float = 0.0
    privilege_escalations: int = 0
    suspicious_patterns: int = 0
    risk_score_distribution: Dict[str, int] = field(default_factory=dict)


class SecurityMetricsCollector:
    """
    Advanced security metrics collection system that integrates with
    existing monitoring infrastructure and security components.
    """
    
    def __init__(self, 
                 metrics_collector: Optional[MetricsCollector] = None,
                 observability_collector: Optional[ObservabilityCollector] = None):
        self.logger = get_logger(__name__)
        self.metrics_collector = metrics_collector or MetricsCollector()
        self.observability_collector = observability_collector or ObservabilityCollector()
        
        # Security-specific storage
        self.security_events: deque = deque(maxlen=10000)
        self.threat_metrics: ThreatDetectionMetrics = ThreatDetectionMetrics()
        self.dlp_metrics: DLPMetrics = DLPMetrics()
        self.compliance_metrics: ComplianceMetrics = ComplianceMetrics()
        self.access_metrics: AccessControlMetrics = AccessControlMetrics()
        
        # Time-based aggregations
        self.hourly_metrics: Dict[str, Dict] = defaultdict(dict)
        self.daily_metrics: Dict[str, Dict] = defaultdict(dict)
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Background processing
        self._background_task: Optional[asyncio.Task] = None
        self._processing_enabled = True
        
        self.logger.info("Security metrics collector initialized")
        
    def record_security_event(self, event: SecurityMetricEvent):
        """Record a security event and update metrics"""
        with self._lock:
            # Store the event
            self.security_events.append(event)
            
            # Update aggregated metrics based on event type
            self._update_metrics(event)
            
            # Send to observability system
            self._send_to_observability(event)
            
            # Send to core metrics collector
            self._send_to_core_metrics(event)
            
        self.logger.debug(f"Recorded security event: {event.event_id}")
    
    def _update_metrics(self, event: SecurityMetricEvent):
        """Update aggregated metrics based on event"""
        timestamp_hour = event.timestamp.replace(minute=0, second=0, microsecond=0)
        timestamp_day = event.timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if event.metric_type == SecurityMetricType.THREAT_DETECTION:
            self._update_threat_metrics(event)
            
        elif event.metric_type == SecurityMetricType.DLP_VIOLATION:
            self._update_dlp_metrics(event)
            
        elif event.metric_type == SecurityMetricType.COMPLIANCE_CHECK:
            self._update_compliance_metrics(event)
            
        elif event.metric_type == SecurityMetricType.ACCESS_CONTROL:
            self._update_access_control_metrics(event)
        
        # Update time-based aggregations
        hour_key = timestamp_hour.isoformat()
        day_key = timestamp_day.isoformat()
        
        if hour_key not in self.hourly_metrics:
            self.hourly_metrics[hour_key] = defaultdict(int)
        if day_key not in self.daily_metrics:
            self.daily_metrics[day_key] = defaultdict(int)
            
        self.hourly_metrics[hour_key][event.metric_type.value] += 1
        self.daily_metrics[day_key][event.metric_type.value] += 1
    
    def _update_threat_metrics(self, event: SecurityMetricEvent):
        """Update threat detection metrics"""
        self.threat_metrics.total_threats_detected += 1
        
        threat_type = event.details.get('threat_type', 'unknown')
        self.threat_metrics.threats_by_type[threat_type] = \
            self.threat_metrics.threats_by_type.get(threat_type, 0) + 1
            
        self.threat_metrics.threats_by_severity[event.severity] = \
            self.threat_metrics.threats_by_severity.get(event.severity, 0) + 1
            
        if event.outcome == 'blocked':
            self.threat_metrics.blocked_attempts += 1
            
        # Update response time if available
        if 'response_time_seconds' in event.details:
            response_time = event.details['response_time_seconds']
            current_avg = self.threat_metrics.avg_response_time_seconds
            total_threats = self.threat_metrics.total_threats_detected
            self.threat_metrics.avg_response_time_seconds = \
                ((current_avg * (total_threats - 1)) + response_time) / total_threats
    
    def _update_dlp_metrics(self, event: SecurityMetricEvent):
        """Update DLP metrics"""
        self.dlp_metrics.total_scans += 1
        
        if event.outcome == 'violation':
            self.dlp_metrics.violations_detected += 1
            
        # Update data type tracking
        data_type_str = event.details.get('data_type')
        if data_type_str:
            try:
                data_type = SensitiveDataType(data_type_str)
                self.dlp_metrics.data_types_detected[data_type] = \
                    self.dlp_metrics.data_types_detected.get(data_type, 0) + 1
            except ValueError:
                pass
        
        # Update action tracking
        action_str = event.details.get('action')
        if action_str:
            try:
                action = DLPAction(action_str)
                self.dlp_metrics.actions_taken[action] = \
                    self.dlp_metrics.actions_taken.get(action, 0) + 1
                    
                if action == DLPAction.REDACT:
                    self.dlp_metrics.redactions_performed += 1
                elif action == DLPAction.ENCRYPT:
                    self.dlp_metrics.encryption_operations += 1
            except ValueError:
                pass
    
    def _update_compliance_metrics(self, event: SecurityMetricEvent):
        """Update compliance metrics"""
        if event.outcome == 'violation':
            self.compliance_metrics.total_violations += 1
            
            # Track violations by framework
            for framework in event.compliance_frameworks:
                self.compliance_metrics.violations_by_framework[framework] = \
                    self.compliance_metrics.violations_by_framework.get(framework, 0) + 1
        
        # Update framework scores if provided
        if 'compliance_score' in event.details and event.compliance_frameworks:
            score = event.details['compliance_score']
            for framework in event.compliance_frameworks:
                self.compliance_metrics.framework_scores[framework] = score
    
    def _update_access_control_metrics(self, event: SecurityMetricEvent):
        """Update access control metrics"""
        self.access_metrics.total_requests += 1
        
        if event.outcome == 'approved':
            self.access_metrics.approved_requests += 1
        elif event.outcome == 'denied':
            self.access_metrics.denied_requests += 1
        elif event.outcome == 'conditional':
            self.access_metrics.conditional_approvals += 1
            
        # Update approval rate
        if self.access_metrics.total_requests > 0:
            self.access_metrics.approval_rate = \
                self.access_metrics.approved_requests / self.access_metrics.total_requests
        
        # Update decision time
        if 'decision_time_ms' in event.details:
            decision_time = event.details['decision_time_ms']
            current_avg = self.access_metrics.avg_decision_time_ms
            total_requests = self.access_metrics.total_requests
            self.access_metrics.avg_decision_time_ms = \
                ((current_avg * (total_requests - 1)) + decision_time) / total_requests
        
        # Track risk score distribution
        if 'risk_score' in event.details:
            risk_score = event.details['risk_score']
            if risk_score <= 3:
                risk_level = 'low'
            elif risk_score <= 6:
                risk_level = 'medium'
            elif risk_score <= 8:
                risk_level = 'high'
            else:
                risk_level = 'critical'
                
            self.access_metrics.risk_score_distribution[risk_level] = \
                self.access_metrics.risk_score_distribution.get(risk_level, 0) + 1
    
    def _send_to_observability(self, event: SecurityMetricEvent):
        """Send security event to observability system"""
        try:
            # Convert to metric point for observability collector
            metric_point = MetricPoint(
                name=f"security.{event.metric_type.value}",
                value=1.0,  # Count metric
                timestamp=event.timestamp,
                labels={
                    'severity': event.severity.value,
                    'outcome': event.outcome,
                    'user_id': event.user_id or 'anonymous',
                    'resource_type': event.details.get('resource_type', 'unknown')
                },
                metric_type=MetricType.COUNTER
            )
            
            # Use async if available, otherwise skip
            if hasattr(self.observability_collector, 'record_metric'):
                asyncio.create_task(self.observability_collector.record_metric(metric_point))
                
        except Exception as e:
            self.logger.error(f"Failed to send security event to observability: {e}")
    
    def _send_to_core_metrics(self, event: SecurityMetricEvent):
        """Send security metrics to core metrics collector"""
        try:
            # Record basic security metrics
            labels = {
                'metric_type': event.metric_type.value,
                'severity': event.severity.value,
                'outcome': event.outcome
            }
            
            # Security event counter
            self.metrics_collector.increment_counter(
                "security_events_total",
                labels=labels
            )
            
            # Severity-specific metrics
            self.metrics_collector.increment_counter(
                f"security_events_{event.severity.value}_total",
                labels={'metric_type': event.metric_type.value}
            )
            
            # Response time histogram if available
            if 'response_time_seconds' in event.details:
                self.metrics_collector.record_histogram(
                    "security_response_time_seconds",
                    event.details['response_time_seconds'],
                    labels={'metric_type': event.metric_type.value}
                )
                
        except Exception as e:
            self.logger.error(f"Failed to send security metrics to core collector: {e}")
    
    def get_threat_detection_metrics(self, 
                                   time_window_hours: int = 24) -> Dict[str, Any]:
        """Get threat detection metrics"""
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        recent_events = [e for e in self.security_events 
                        if e.timestamp >= cutoff_time and 
                        e.metric_type == SecurityMetricType.THREAT_DETECTION]
        
        # Calculate metrics for the time window
        total_threats = len(recent_events)
        threats_by_type = defaultdict(int)
        threats_by_severity = defaultdict(int)
        blocked_count = 0
        
        for event in recent_events:
            threat_type = event.details.get('threat_type', 'unknown')
            threats_by_type[threat_type] += 1
            threats_by_severity[event.severity.value] += 1
            
            if event.outcome == 'blocked':
                blocked_count += 1
        
        return {
            'time_window_hours': time_window_hours,
            'total_threats_detected': total_threats,
            'threats_by_type': dict(threats_by_type),
            'threats_by_severity': dict(threats_by_severity),
            'threats_blocked': blocked_count,
            'block_rate': blocked_count / total_threats if total_threats > 0 else 0,
            'detection_rate_per_hour': total_threats / time_window_hours,
            'top_threat_types': sorted(threats_by_type.items(), 
                                     key=lambda x: x[1], reverse=True)[:5]
        }
    
    def get_dlp_effectiveness_metrics(self, 
                                     time_window_hours: int = 24) -> Dict[str, Any]:
        """Get DLP effectiveness metrics"""
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        dlp_events = [e for e in self.security_events 
                     if e.timestamp >= cutoff_time and 
                     e.metric_type == SecurityMetricType.DLP_VIOLATION]
        
        violations = len([e for e in dlp_events if e.outcome == 'violation'])
        total_scans = len([e for e in dlp_events if e.outcome in ['clean', 'violation']])
        
        # Data type distribution
        data_types = defaultdict(int)
        actions_taken = defaultdict(int)
        
        for event in dlp_events:
            if 'data_type' in event.details:
                data_types[event.details['data_type']] += 1
            if 'action' in event.details:
                actions_taken[event.details['action']] += 1
        
        return {
            'time_window_hours': time_window_hours,
            'total_scans': total_scans,
            'violations_detected': violations,
            'violation_rate': violations / total_scans if total_scans > 0 else 0,
            'data_types_detected': dict(data_types),
            'actions_distribution': dict(actions_taken),
            'scans_per_hour': total_scans / time_window_hours,
            'most_detected_types': sorted(data_types.items(), 
                                        key=lambda x: x[1], reverse=True)[:5]
        }
    
    def get_compliance_adherence_metrics(self, 
                                       time_window_hours: int = 24) -> Dict[str, Any]:
        """Get compliance adherence metrics"""
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        compliance_events = [e for e in self.security_events 
                           if e.timestamp >= cutoff_time and 
                           e.metric_type == SecurityMetricType.COMPLIANCE_CHECK]
        
        total_checks = len(compliance_events)
        violations = len([e for e in compliance_events if e.outcome == 'violation'])
        
        # Framework-specific metrics
        framework_violations = defaultdict(int)
        framework_checks = defaultdict(int)
        
        for event in compliance_events:
            for framework in event.compliance_frameworks:
                framework_checks[framework.value] += 1
                if event.outcome == 'violation':
                    framework_violations[framework.value] += 1
        
        # Calculate compliance rates by framework
        framework_rates = {}
        for framework, checks in framework_checks.items():
            violations_count = framework_violations.get(framework, 0)
            framework_rates[framework] = (checks - violations_count) / checks if checks > 0 else 0
        
        return {
            'time_window_hours': time_window_hours,
            'total_compliance_checks': total_checks,
            'total_violations': violations,
            'overall_compliance_rate': (total_checks - violations) / total_checks if total_checks > 0 else 1.0,
            'framework_compliance_rates': framework_rates,
            'violations_by_framework': dict(framework_violations),
            'checks_per_hour': total_checks / time_window_hours,
            'critical_frameworks': [fw for fw, rate in framework_rates.items() if rate < 0.9]
        }
    
    def get_access_control_analytics(self, 
                                   time_window_hours: int = 24) -> Dict[str, Any]:
        """Get access control analytics"""
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        access_events = [e for e in self.security_events 
                        if e.timestamp >= cutoff_time and 
                        e.metric_type == SecurityMetricType.ACCESS_CONTROL]
        
        total_requests = len(access_events)
        approved = len([e for e in access_events if e.outcome == 'approved'])
        denied = len([e for e in access_events if e.outcome == 'denied'])
        
        # User behavior analysis
        user_requests = defaultdict(int)
        suspicious_users = defaultdict(int)
        
        for event in access_events:
            if event.user_id:
                user_requests[event.user_id] += 1
                if event.outcome == 'denied' or event.severity in [SecurityEventSeverity.HIGH, SecurityEventSeverity.CRITICAL]:
                    suspicious_users[event.user_id] += 1
        
        # Risk score analysis
        risk_distribution = defaultdict(int)
        for event in access_events:
            risk_score = event.details.get('risk_score', 0)
            if risk_score <= 3:
                risk_distribution['low'] += 1
            elif risk_score <= 6:
                risk_distribution['medium'] += 1
            elif risk_score <= 8:
                risk_distribution['high'] += 1
            else:
                risk_distribution['critical'] += 1
        
        return {
            'time_window_hours': time_window_hours,
            'total_access_requests': total_requests,
            'approved_requests': approved,
            'denied_requests': denied,
            'approval_rate': approved / total_requests if total_requests > 0 else 0,
            'requests_per_hour': total_requests / time_window_hours,
            'risk_score_distribution': dict(risk_distribution),
            'top_requesting_users': sorted(user_requests.items(), 
                                         key=lambda x: x[1], reverse=True)[:10],
            'suspicious_users': dict(suspicious_users),
            'high_risk_requests': len([e for e in access_events 
                                     if event.details.get('risk_score', 0) > 8])
        }
    
    def get_security_posture_score(self) -> Dict[str, Any]:
        """Calculate overall security posture score"""
        
        # Get recent metrics (last 24 hours)
        threat_metrics = self.get_threat_detection_metrics(24)
        dlp_metrics = self.get_dlp_effectiveness_metrics(24)
        compliance_metrics = self.get_compliance_adherence_metrics(24)
        access_metrics = self.get_access_control_analytics(24)
        
        # Calculate component scores (0-100)
        threat_score = min(100, max(0, 100 - (threat_metrics['total_threats_detected'] * 5)))
        dlp_score = min(100, max(0, 100 - (dlp_metrics['violation_rate'] * 100)))
        compliance_score = compliance_metrics['overall_compliance_rate'] * 100
        access_score = access_metrics['approval_rate'] * 100
        
        # Weight the scores
        weights = {
            'threat_detection': 0.3,
            'dlp_effectiveness': 0.25,
            'compliance_adherence': 0.25,
            'access_control': 0.2
        }
        
        overall_score = (
            threat_score * weights['threat_detection'] +
            dlp_score * weights['dlp_effectiveness'] +
            compliance_score * weights['compliance_adherence'] +
            access_score * weights['access_control']
        )
        
        # Determine security level
        if overall_score >= 90:
            security_level = 'excellent'
        elif overall_score >= 80:
            security_level = 'good'
        elif overall_score >= 70:
            security_level = 'acceptable'
        elif overall_score >= 60:
            security_level = 'needs_improvement'
        else:
            security_level = 'critical'
        
        return {
            'overall_score': round(overall_score, 1),
            'security_level': security_level,
            'component_scores': {
                'threat_detection': round(threat_score, 1),
                'dlp_effectiveness': round(dlp_score, 1),
                'compliance_adherence': round(compliance_score, 1),
                'access_control': round(access_score, 1)
            },
            'weights': weights,
            'timestamp': datetime.now().isoformat(),
            'recommendations': self._generate_security_recommendations(
                threat_score, dlp_score, compliance_score, access_score
            )
        }
    
    def _generate_security_recommendations(self, 
                                         threat_score: float,
                                         dlp_score: float, 
                                         compliance_score: float,
                                         access_score: float) -> List[str]:
        """Generate security recommendations based on scores"""
        recommendations = []
        
        if threat_score < 70:
            recommendations.append("Enhance threat detection capabilities and response procedures")
        if dlp_score < 80:
            recommendations.append("Review and strengthen DLP policies to reduce violations")
        if compliance_score < 90:
            recommendations.append("Address compliance violations to improve adherence")
        if access_score < 85:
            recommendations.append("Review access control policies for potential improvements")
        
        if not recommendations:
            recommendations.append("Security posture is strong - maintain current practices")
        
        return recommendations
    
    def export_prometheus_metrics(self) -> str:
        """Export security metrics in Prometheus format"""
        timestamp = int(time.time() * 1000)
        lines = []
        
        # Threat detection metrics
        lines.extend([
            "# HELP security_threats_total Total security threats detected",
            "# TYPE security_threats_total counter",
            f"security_threats_total {self.threat_metrics.total_threats_detected} {timestamp}",
            "",
            "# HELP security_threats_blocked_total Total threats blocked",
            "# TYPE security_threats_blocked_total counter", 
            f"security_threats_blocked_total {self.threat_metrics.blocked_attempts} {timestamp}",
            ""
        ])
        
        # DLP metrics
        lines.extend([
            "# HELP security_dlp_scans_total Total DLP scans performed",
            "# TYPE security_dlp_scans_total counter",
            f"security_dlp_scans_total {self.dlp_metrics.total_scans} {timestamp}",
            "",
            "# HELP security_dlp_violations_total Total DLP violations detected",
            "# TYPE security_dlp_violations_total counter",
            f"security_dlp_violations_total {self.dlp_metrics.violations_detected} {timestamp}",
            ""
        ])
        
        # Access control metrics
        lines.extend([
            "# HELP security_access_requests_total Total access control requests",
            "# TYPE security_access_requests_total counter",
            f"security_access_requests_total {self.access_metrics.total_requests} {timestamp}",
            "",
            "# HELP security_access_approval_rate Access approval rate",
            "# TYPE security_access_approval_rate gauge",
            f"security_access_approval_rate {self.access_metrics.approval_rate:.3f} {timestamp}",
            ""
        ])
        
        # Compliance metrics
        lines.extend([
            "# HELP security_compliance_violations_total Total compliance violations",
            "# TYPE security_compliance_violations_total counter",
            f"security_compliance_violations_total {self.compliance_metrics.total_violations} {timestamp}",
            ""
        ])
        
        return '\n'.join(lines)
    
    async def start_background_processing(self):
        """Start background processing for metrics aggregation"""
        self._processing_enabled = True
        self._background_task = asyncio.create_task(self._background_processor())
        self.logger.info("Started security metrics background processing")
    
    async def stop_background_processing(self):
        """Stop background processing"""
        self._processing_enabled = False
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped security metrics background processing")
    
    async def _background_processor(self):
        """Background task for metrics processing and cleanup"""
        while self._processing_enabled:
            try:
                # Clean up old events (keep only last 7 days)
                cutoff_time = datetime.now() - timedelta(days=7)
                with self._lock:
                    # Convert deque to list, filter, and create new deque
                    recent_events = [e for e in self.security_events if e.timestamp >= cutoff_time]
                    self.security_events = deque(recent_events, maxlen=10000)
                
                # Clean up old hourly/daily metrics (keep only last 30 days)
                cutoff_day = datetime.now() - timedelta(days=30)
                cutoff_day_key = cutoff_day.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                
                with self._lock:
                    # Remove old daily metrics
                    old_day_keys = [k for k in self.daily_metrics.keys() if k < cutoff_day_key]
                    for key in old_day_keys:
                        del self.daily_metrics[key]
                    
                    # Remove old hourly metrics (keep only last 7 days)
                    cutoff_hour = datetime.now() - timedelta(days=7)
                    cutoff_hour_key = cutoff_hour.replace(minute=0, second=0, microsecond=0).isoformat()
                    old_hour_keys = [k for k in self.hourly_metrics.keys() if k < cutoff_hour_key]
                    for key in old_hour_keys:
                        del self.hourly_metrics[key]
                
                await asyncio.sleep(3600)  # Run cleanup every hour
                
            except Exception as e:
                self.logger.error(f"Background processing error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error


# Global security metrics collector instance
_security_metrics_collector: Optional[SecurityMetricsCollector] = None

def get_security_metrics_collector(
    metrics_collector: Optional[MetricsCollector] = None,
    observability_collector: Optional[ObservabilityCollector] = None
) -> SecurityMetricsCollector:
    """Get global security metrics collector instance"""
    global _security_metrics_collector
    if _security_metrics_collector is None:
        _security_metrics_collector = SecurityMetricsCollector(
            metrics_collector, observability_collector
        )
    return _security_metrics_collector


# Convenience functions for recording common security events
def record_threat_detection(
    event_id: str,
    threat_type: str,
    severity: SecurityEventSeverity,
    outcome: str,
    user_id: Optional[str] = None,
    source_ip: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
):
    """Record a threat detection event"""
    collector = get_security_metrics_collector()
    event = SecurityMetricEvent(
        event_id=event_id,
        metric_type=SecurityMetricType.THREAT_DETECTION,
        severity=severity,
        user_id=user_id,
        source_ip=source_ip,
        resource_id="threat_detection_system",
        action="detect",
        outcome=outcome,
        details=dict(details or {}, threat_type=threat_type)
    )
    collector.record_security_event(event)


def record_dlp_violation(
    event_id: str,
    data_type: SensitiveDataType,
    action: DLPAction,
    severity: SecurityEventSeverity,
    user_id: Optional[str] = None,
    resource_id: str = "unknown",
    details: Optional[Dict[str, Any]] = None
):
    """Record a DLP violation event"""
    collector = get_security_metrics_collector()
    event = SecurityMetricEvent(
        event_id=event_id,
        metric_type=SecurityMetricType.DLP_VIOLATION,
        severity=severity,
        user_id=user_id,
        source_ip=None,
        resource_id=resource_id,
        action=action.value,
        outcome="violation",
        details=dict(details or {}, data_type=data_type.value, action=action.value)
    )
    collector.record_security_event(event)


def record_compliance_check(
    event_id: str,
    frameworks: List[ComplianceFramework],
    outcome: str,
    resource_id: str,
    details: Optional[Dict[str, Any]] = None
):
    """Record a compliance check event"""
    collector = get_security_metrics_collector()
    severity = SecurityEventSeverity.CRITICAL if outcome == 'violation' else SecurityEventSeverity.INFO
    event = SecurityMetricEvent(
        event_id=event_id,
        metric_type=SecurityMetricType.COMPLIANCE_CHECK,
        severity=severity,
        user_id=None,
        source_ip=None,
        resource_id=resource_id,
        action="check",
        outcome=outcome,
        details=details or {},
        compliance_frameworks=frameworks
    )
    collector.record_security_event(event)


def record_access_control_decision(
    event_id: str,
    user_id: str,
    resource_id: str,
    action: str,
    outcome: str,
    risk_score: float,
    decision_time_ms: float,
    source_ip: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
):
    """Record an access control decision event"""
    collector = get_security_metrics_collector()
    
    # Determine severity based on risk score and outcome
    if outcome == 'denied' or risk_score > 8:
        severity = SecurityEventSeverity.HIGH
    elif risk_score > 6:
        severity = SecurityEventSeverity.MEDIUM
    else:
        severity = SecurityEventSeverity.INFO
    
    event = SecurityMetricEvent(
        event_id=event_id,
        metric_type=SecurityMetricType.ACCESS_CONTROL,
        severity=severity,
        user_id=user_id,
        source_ip=source_ip,
        resource_id=resource_id,
        action=action,
        outcome=outcome,
        details=dict(details or {}, risk_score=risk_score, decision_time_ms=decision_time_ms)
    )
    collector.record_security_event(event)