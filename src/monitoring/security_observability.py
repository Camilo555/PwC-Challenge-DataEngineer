"""
Security-Focused Observability Framework
Provides comprehensive distributed tracing, logging, and monitoring
specifically designed for security events and threat detection.
"""
import asyncio
import json
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import threading
import time

from opentelemetry import trace
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.trace import Status, StatusCode

from core.logging import get_logger
from core.tracing.tracer import get_tracer
from monitoring.security_metrics import SecurityMetricEvent, SecurityEventSeverity, SecurityMetricType
from monitoring.advanced_observability import DistributedTracer, MetricPoint, MetricType


logger = get_logger(__name__)


class SecurityTraceType(Enum):
    """Types of security traces"""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    THREAT_DETECTION = "threat_detection"
    DLP_SCAN = "dlp_scan"
    COMPLIANCE_CHECK = "compliance_check"
    INCIDENT_RESPONSE = "incident_response"
    AUDIT_TRAIL = "audit_trail"


class SecurityContextField(Enum):
    """Security context fields for tracing"""
    USER_ID = "security.user_id"
    SESSION_ID = "security.session_id"
    SOURCE_IP = "security.source_ip"
    USER_AGENT = "security.user_agent"
    RISK_SCORE = "security.risk_score"
    THREAT_LEVEL = "security.threat_level"
    DATA_CLASSIFICATION = "security.data_classification"
    COMPLIANCE_FRAMEWORKS = "security.compliance_frameworks"
    SENSITIVE_DATA_TYPES = "security.sensitive_data_types"
    DLP_POLICIES = "security.dlp_policies"
    ACCESS_DECISION = "security.access_decision"
    SECURITY_EVENTS = "security.events"


@dataclass
class SecurityTraceContext:
    """Security-specific context for tracing"""
    trace_id: str
    span_id: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    source_ip: Optional[str] = None
    user_agent: Optional[str] = None
    risk_score: Optional[float] = None
    threat_level: Optional[str] = None
    data_classification: Optional[str] = None
    compliance_frameworks: List[str] = field(default_factory=list)
    sensitive_data_detected: List[str] = field(default_factory=list)
    security_events: List[Dict[str, Any]] = field(default_factory=list)
    custom_attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SecurityLogEntry:
    """Structured security log entry"""
    timestamp: datetime
    trace_id: str
    span_id: str
    log_level: str
    message: str
    security_context: SecurityTraceContext
    event_type: SecurityTraceType
    component: str
    details: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class SecuritySpanEvent:
    """Security event within a trace span"""
    name: str
    timestamp: datetime
    attributes: Dict[str, Any]
    security_impact: SecurityEventSeverity
    event_type: SecurityTraceType


class SecurityDistributedTracer:
    """Enhanced distributed tracer with security-specific functionality"""
    
    def __init__(self, service_name: str = "security-monitoring"):
        self.service_name = service_name
        self.base_tracer = get_tracer()
        self.security_spans: Dict[str, SecurityTraceContext] = {}
        self.security_logs: deque = deque(maxlen=10000)
        self.correlation_map: Dict[str, List[str]] = defaultdict(list)
        self._lock = threading.RLock()
        self.logger = get_logger(__name__)
        
        # Initialize OpenTelemetry logging instrumentation
        LoggingInstrumentor().instrument()
        
    @contextmanager 
    def trace_security_operation(
        self,
        operation_name: str,
        trace_type: SecurityTraceType,
        security_context: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        """Context manager for tracing security operations"""
        
        with self.base_tracer.start_as_current_span(
            f"security.{trace_type.value}.{operation_name}"
        ) as span:
            
            # Get trace context
            span_context = span.get_span_context()
            trace_id = format(span_context.trace_id, '032x')
            span_id = format(span_context.span_id, '016x')
            
            # Create security context
            sec_context = SecurityTraceContext(
                trace_id=trace_id,
                span_id=span_id,
                **(security_context or {})
            )
            
            # Set security attributes on span
            self._set_security_attributes(span, sec_context, trace_type)
            
            # Store security context
            with self._lock:
                self.security_spans[span_id] = sec_context
                if correlation_id:
                    self.correlation_map[correlation_id].append(span_id)
            
            try:
                yield sec_context
                
                # Mark span as successful
                span.set_status(Status(StatusCode.OK))
                
            except Exception as e:
                # Record exception and security impact
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                
                # Log security event
                self._log_security_event(
                    trace_id, span_id, "ERROR", 
                    f"Security operation failed: {str(e)}",
                    sec_context, trace_type, operation_name,
                    {'exception': str(e), 'exception_type': type(e).__name__}
                )
                
                raise
                
            finally:
                # Clean up context
                with self._lock:
                    if span_id in self.security_spans:
                        del self.security_spans[span_id]
    
    def _set_security_attributes(
        self,
        span,
        context: SecurityTraceContext,
        trace_type: SecurityTraceType
    ):
        """Set security-specific attributes on the span"""
        
        span.set_attribute("security.trace_type", trace_type.value)
        
        if context.user_id:
            span.set_attribute(SecurityContextField.USER_ID.value, context.user_id)
        if context.session_id:
            span.set_attribute(SecurityContextField.SESSION_ID.value, context.session_id)
        if context.source_ip:
            span.set_attribute(SecurityContextField.SOURCE_IP.value, context.source_ip)
        if context.user_agent:
            span.set_attribute(SecurityContextField.USER_AGENT.value, context.user_agent)
        if context.risk_score is not None:
            span.set_attribute(SecurityContextField.RISK_SCORE.value, context.risk_score)
        if context.threat_level:
            span.set_attribute(SecurityContextField.THREAT_LEVEL.value, context.threat_level)
        if context.data_classification:
            span.set_attribute(SecurityContextField.DATA_CLASSIFICATION.value, context.data_classification)
        if context.compliance_frameworks:
            span.set_attribute(SecurityContextField.COMPLIANCE_FRAMEWORKS.value, ','.join(context.compliance_frameworks))
        if context.sensitive_data_detected:
            span.set_attribute(SecurityContextField.SENSITIVE_DATA_TYPES.value, ','.join(context.sensitive_data_detected))
        
        # Add custom attributes
        for key, value in context.custom_attributes.items():
            span.set_attribute(f"security.custom.{key}", str(value))
    
    def add_security_event(
        self,
        span_context: SecurityTraceContext,
        event_name: str,
        event_type: SecurityTraceType,
        severity: SecurityEventSeverity,
        details: Optional[Dict[str, Any]] = None
    ):
        """Add a security event to the current span"""
        
        current_span = trace.get_current_span()
        if current_span:
            # Add event to span
            current_span.add_event(
                event_name,
                attributes=dict(details or {}, 
                               security_severity=severity.value,
                               security_event_type=event_type.value)
            )
            
            # Record in security context
            event_data = {
                'name': event_name,
                'type': event_type.value,
                'severity': severity.value,
                'timestamp': datetime.now().isoformat(),
                'details': details or {}
            }
            
            with self._lock:
                if span_context.span_id in self.security_spans:
                    self.security_spans[span_context.span_id].security_events.append(event_data)
    
    def _log_security_event(
        self,
        trace_id: str,
        span_id: str,
        level: str,
        message: str,
        context: SecurityTraceContext,
        event_type: SecurityTraceType,
        component: str,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ):
        """Log a structured security event"""
        
        log_entry = SecurityLogEntry(
            timestamp=datetime.now(),
            trace_id=trace_id,
            span_id=span_id,
            log_level=level,
            message=message,
            security_context=context,
            event_type=event_type,
            component=component,
            details=details or {},
            correlation_id=correlation_id
        )
        
        with self._lock:
            self.security_logs.append(log_entry)
        
        # Also log through standard logger with trace context
        log_data = {
            'trace_id': trace_id,
            'span_id': span_id,
            'security_event_type': event_type.value,
            'user_id': context.user_id,
            'source_ip': context.source_ip,
            'risk_score': context.risk_score,
            **details or {}
        }
        
        if level == "ERROR":
            self.logger.error(f"[SECURITY] {message}", extra=log_data)
        elif level == "WARNING":
            self.logger.warning(f"[SECURITY] {message}", extra=log_data)
        else:
            self.logger.info(f"[SECURITY] {message}", extra=log_data)
    
    def correlate_security_events(
        self,
        correlation_id: str,
        time_window_minutes: int = 60
    ) -> List[SecurityLogEntry]:
        """Correlate security events by correlation ID and time window"""
        
        with self._lock:
            span_ids = self.correlation_map.get(correlation_id, [])
            
            # Find all logs related to these spans
            cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
            
            correlated_logs = []
            for log_entry in self.security_logs:
                if (log_entry.span_id in span_ids or 
                    log_entry.correlation_id == correlation_id) and \
                   log_entry.timestamp >= cutoff_time:
                    correlated_logs.append(log_entry)
            
            # Sort by timestamp
            correlated_logs.sort(key=lambda x: x.timestamp)
            return correlated_logs
    
    def get_security_trace_analytics(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Get analytics on security traces"""
        
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        
        with self._lock:
            recent_logs = [log for log in self.security_logs if log.timestamp >= cutoff_time]
        
        # Analyze by trace type
        trace_types = defaultdict(int)
        severity_distribution = defaultdict(int)
        user_activity = defaultdict(int)
        source_ip_activity = defaultdict(int)
        component_activity = defaultdict(int)
        
        for log in recent_logs:
            trace_types[log.event_type.value] += 1
            
            # Map log level to severity
            if log.log_level in ['ERROR', 'CRITICAL']:
                severity = 'high'
            elif log.log_level == 'WARNING':
                severity = 'medium'
            else:
                severity = 'low'
            severity_distribution[severity] += 1
            
            if log.security_context.user_id:
                user_activity[log.security_context.user_id] += 1
            if log.security_context.source_ip:
                source_ip_activity[log.security_context.source_ip] += 1
            component_activity[log.component] += 1
        
        return {
            'time_window_hours': time_window_hours,
            'total_security_traces': len(recent_logs),
            'trace_types_distribution': dict(trace_types),
            'severity_distribution': dict(severity_distribution),
            'most_active_users': sorted(user_activity.items(), key=lambda x: x[1], reverse=True)[:10],
            'most_active_ips': sorted(source_ip_activity.items(), key=lambda x: x[1], reverse=True)[:10],
            'component_activity': dict(component_activity),
            'unique_traces': len(set(log.trace_id for log in recent_logs)),
            'avg_events_per_trace': len(recent_logs) / max(len(set(log.trace_id for log in recent_logs)), 1)
        }


class SecurityLoggingFormatter:
    """Custom formatter for security logs with structured context"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def format_security_log(
        self,
        message: str,
        context: SecurityTraceContext,
        event_type: SecurityTraceType,
        severity: SecurityEventSeverity,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Format security log with structured context"""
        
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'message': message,
            'event_type': event_type.value,
            'severity': severity.value,
            'trace_id': context.trace_id,
            'span_id': context.span_id,
            'security_context': {
                'user_id': context.user_id,
                'session_id': context.session_id,
                'source_ip': context.source_ip,
                'user_agent': context.user_agent,
                'risk_score': context.risk_score,
                'threat_level': context.threat_level,
                'data_classification': context.data_classification,
                'compliance_frameworks': context.compliance_frameworks,
                'sensitive_data_detected': context.sensitive_data_detected
            },
            'security_events_count': len(context.security_events),
            'additional_data': additional_data or {}
        }
        
        # Remove None values for cleaner logs
        log_data['security_context'] = {
            k: v for k, v in log_data['security_context'].items() if v is not None
        }
        
        return log_data


class SecurityAnomalyDetector:
    """Detect anomalies in security traces and logs"""
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.trace_patterns: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.user_baselines: Dict[str, Dict[str, float]] = {}
        self.ip_baselines: Dict[str, Dict[str, float]] = {}
        self.logger = get_logger(__name__)
        self._lock = threading.RLock()
    
    def analyze_trace_pattern(
        self,
        context: SecurityTraceContext,
        event_type: SecurityTraceType
    ) -> Dict[str, Any]:
        """Analyze trace pattern for anomalies"""
        
        pattern_key = f"{context.user_id}:{event_type.value}"
        current_time = datetime.now()
        
        with self._lock:
            # Record the pattern
            self.trace_patterns[pattern_key].append({
                'timestamp': current_time,
                'risk_score': context.risk_score or 0,
                'source_ip': context.source_ip,
                'data_classification': context.data_classification,
                'sensitive_data_count': len(context.sensitive_data_detected)
            })
            
            # Analyze for anomalies
            patterns = list(self.trace_patterns[pattern_key])
            
            if len(patterns) < 10:  # Need minimum data for analysis
                return {'anomaly_detected': False, 'confidence': 0.0}
            
            # Check for unusual patterns
            anomalies = []
            
            # 1. Unusual time patterns
            hours = [p['timestamp'].hour for p in patterns[-20:]]
            current_hour = current_time.hour
            if current_hour not in hours[-10:] and len(set(hours)) > 1:
                anomalies.append('unusual_time_pattern')
            
            # 2. Risk score anomaly
            risk_scores = [p['risk_score'] for p in patterns if p['risk_score'] > 0]
            if risk_scores and len(risk_scores) > 5:
                avg_risk = sum(risk_scores) / len(risk_scores)
                current_risk = context.risk_score or 0
                if current_risk > avg_risk * 2:
                    anomalies.append('high_risk_anomaly')
            
            # 3. New source IP
            recent_ips = [p['source_ip'] for p in patterns[-10:] if p['source_ip']]
            if context.source_ip and context.source_ip not in recent_ips and len(recent_ips) > 0:
                anomalies.append('new_source_ip')
            
            # 4. Unusual data access pattern
            recent_classifications = [p['data_classification'] for p in patterns[-10:] if p['data_classification']]
            if (context.data_classification and 
                context.data_classification == 'restricted' and 
                'restricted' not in recent_classifications):
                anomalies.append('unusual_data_access')
            
            confidence = min(len(anomalies) * 0.3, 1.0)
            
            return {
                'anomaly_detected': len(anomalies) > 0,
                'anomalies': anomalies,
                'confidence': confidence,
                'pattern_analysis': {
                    'total_patterns': len(patterns),
                    'avg_risk_score': sum(risk_scores) / len(risk_scores) if risk_scores else 0,
                    'unique_ips': len(set(p['source_ip'] for p in patterns if p['source_ip'])),
                    'time_spread_hours': len(set(p['timestamp'].hour for p in patterns))
                }
            }


class SecurityObservabilityOrchestrator:
    """Main orchestrator for security observability"""
    
    def __init__(self, service_name: str = "security-platform"):
        self.service_name = service_name
        self.tracer = SecurityDistributedTracer(service_name)
        self.formatter = SecurityLoggingFormatter()
        self.anomaly_detector = SecurityAnomalyDetector()
        self.logger = get_logger(__name__)
        
        # Metrics and events
        self.security_metrics: deque = deque(maxlen=5000)
        self.active_traces: Dict[str, SecurityTraceContext] = {}
        self._lock = threading.RLock()
        
        self.logger.info(f"Security observability orchestrator initialized for {service_name}")
    
    @asynccontextmanager
    async def trace_security_workflow(
        self,
        workflow_name: str,
        trace_type: SecurityTraceType,
        user_id: Optional[str] = None,
        source_ip: Optional[str] = None,
        session_id: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ):
        """Async context manager for tracing complete security workflows"""
        
        correlation_id = str(uuid.uuid4())
        security_context = {
            'user_id': user_id,
            'source_ip': source_ip,
            'session_id': session_id,
            **(additional_context or {})
        }
        
        with self.tracer.trace_security_operation(
            workflow_name, trace_type, security_context, correlation_id
        ) as context:
            
            # Store active trace
            with self._lock:
                self.active_traces[context.trace_id] = context
            
            try:
                # Analyze for anomalies
                anomaly_result = self.anomaly_detector.analyze_trace_pattern(context, trace_type)
                
                if anomaly_result['anomaly_detected']:
                    self.tracer.add_security_event(
                        context, 
                        "anomaly_detected",
                        SecurityTraceType.THREAT_DETECTION,
                        SecurityEventSeverity.MEDIUM,
                        anomaly_result
                    )
                
                yield context
                
                # Log successful completion
                self._log_workflow_completion(context, workflow_name, trace_type, "success")
                
            except Exception as e:
                # Log failure
                self._log_workflow_completion(context, workflow_name, trace_type, "failure", str(e))
                raise
                
            finally:
                # Clean up active trace
                with self._lock:
                    if context.trace_id in self.active_traces:
                        del self.active_traces[context.trace_id]
    
    def _log_workflow_completion(
        self,
        context: SecurityTraceContext,
        workflow_name: str,
        trace_type: SecurityTraceType,
        status: str,
        error_message: Optional[str] = None
    ):
        """Log workflow completion"""
        
        severity = SecurityEventSeverity.CRITICAL if status == "failure" else SecurityEventSeverity.INFO
        message = f"Security workflow '{workflow_name}' {status}"
        
        if error_message:
            message += f": {error_message}"
        
        log_data = self.formatter.format_security_log(
            message, context, trace_type, severity,
            {'workflow': workflow_name, 'status': status, 'error': error_message}
        )
        
        self.logger.info(json.dumps(log_data))
    
    def create_security_metric_from_trace(
        self,
        context: SecurityTraceContext,
        metric_type: SecurityMetricType,
        additional_details: Optional[Dict[str, Any]] = None
    ) -> SecurityMetricEvent:
        """Create security metric event from trace context"""
        
        # Determine severity based on context
        if context.risk_score and context.risk_score > 8:
            severity = SecurityEventSeverity.CRITICAL
        elif context.risk_score and context.risk_score > 6:
            severity = SecurityEventSeverity.HIGH
        elif context.risk_score and context.risk_score > 3:
            severity = SecurityEventSeverity.MEDIUM
        else:
            severity = SecurityEventSeverity.INFO
        
        event = SecurityMetricEvent(
            event_id=f"{context.trace_id}_{int(time.time())}",
            metric_type=metric_type,
            severity=severity,
            user_id=context.user_id,
            source_ip=context.source_ip,
            resource_id=context.custom_attributes.get('resource_id', 'unknown'),
            action=context.custom_attributes.get('action', 'unknown'),
            outcome=context.custom_attributes.get('outcome', 'processed'),
            details=dict(
                trace_id=context.trace_id,
                span_id=context.span_id,
                session_id=context.session_id,
                risk_score=context.risk_score,
                threat_level=context.threat_level,
                data_classification=context.data_classification,
                compliance_frameworks=context.compliance_frameworks,
                sensitive_data_types=context.sensitive_data_detected,
                security_events_count=len(context.security_events),
                **(additional_details or {})
            ),
            session_id=context.session_id
        )
        
        with self._lock:
            self.security_metrics.append(event)
        
        return event
    
    def get_trace_correlation_analysis(
        self,
        time_window_hours: int = 24
    ) -> Dict[str, Any]:
        """Analyze trace correlations for security patterns"""
        
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        
        # Get recent security logs
        recent_logs = self.tracer.correlate_security_events("", time_window_hours * 60)
        
        # Analyze patterns
        user_traces = defaultdict(list)
        ip_traces = defaultdict(list)
        session_traces = defaultdict(list)
        
        for log in recent_logs:
            if log.timestamp >= cutoff_time:
                if log.security_context.user_id:
                    user_traces[log.security_context.user_id].append(log)
                if log.security_context.source_ip:
                    ip_traces[log.security_context.source_ip].append(log)
                if log.security_context.session_id:
                    session_traces[log.security_context.session_id].append(log)
        
        # Find suspicious patterns
        suspicious_users = []
        suspicious_ips = []
        
        for user_id, logs in user_traces.items():
            if len(logs) > 50:  # High activity
                error_rate = len([l for l in logs if l.log_level in ['ERROR', 'WARNING']]) / len(logs)
                if error_rate > 0.3:  # High error rate
                    suspicious_users.append({
                        'user_id': user_id,
                        'total_events': len(logs),
                        'error_rate': error_rate,
                        'unique_ips': len(set(l.security_context.source_ip for l in logs if l.security_context.source_ip))
                    })
        
        for ip, logs in ip_traces.items():
            if len(logs) > 100:  # Very high activity from single IP
                unique_users = len(set(l.security_context.user_id for l in logs if l.security_context.user_id))
                if unique_users > 10:  # Many users from same IP
                    suspicious_ips.append({
                        'source_ip': ip,
                        'total_events': len(logs),
                        'unique_users': unique_users,
                        'events_per_user': len(logs) / unique_users
                    })
        
        return {
            'time_window_hours': time_window_hours,
            'total_traces_analyzed': len(recent_logs),
            'unique_users': len(user_traces),
            'unique_ips': len(ip_traces),
            'unique_sessions': len(session_traces),
            'suspicious_users': suspicious_users,
            'suspicious_ips': suspicious_ips,
            'avg_events_per_user': sum(len(logs) for logs in user_traces.values()) / len(user_traces) if user_traces else 0,
            'avg_events_per_ip': sum(len(logs) for logs in ip_traces.values()) / len(ip_traces) if ip_traces else 0
        }
    
    def export_security_traces(
        self,
        time_window_hours: int = 24,
        format_type: str = "json"
    ) -> str:
        """Export security traces for external analysis"""
        
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        
        with self.tracer._lock:
            recent_logs = [
                log for log in self.tracer.security_logs 
                if log.timestamp >= cutoff_time
            ]
        
        if format_type == "json":
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'time_window_hours': time_window_hours,
                'total_traces': len(recent_logs),
                'traces': [
                    {
                        'timestamp': log.timestamp.isoformat(),
                        'trace_id': log.trace_id,
                        'span_id': log.span_id,
                        'level': log.log_level,
                        'message': log.message,
                        'event_type': log.event_type.value,
                        'component': log.component,
                        'security_context': asdict(log.security_context),
                        'details': log.details,
                        'correlation_id': log.correlation_id,
                        'tags': log.tags
                    }
                    for log in recent_logs
                ]
            }
            return json.dumps(export_data, indent=2, default=str)
        
        else:
            # CSV format
            import csv
            import io
            
            output = io.StringIO()
            fieldnames = [
                'timestamp', 'trace_id', 'span_id', 'level', 'message', 
                'event_type', 'component', 'user_id', 'source_ip', 'risk_score'
            ]
            
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            
            for log in recent_logs:
                writer.writerow({
                    'timestamp': log.timestamp.isoformat(),
                    'trace_id': log.trace_id,
                    'span_id': log.span_id,
                    'level': log.log_level,
                    'message': log.message,
                    'event_type': log.event_type.value,
                    'component': log.component,
                    'user_id': log.security_context.user_id or '',
                    'source_ip': log.security_context.source_ip or '',
                    'risk_score': log.security_context.risk_score or 0
                })
            
            return output.getvalue()
    
    async def cleanup_old_traces(self, retention_hours: int = 168):  # 7 days default
        """Clean up old trace data"""
        cutoff_time = datetime.now() - timedelta(hours=retention_hours)
        
        with self.tracer._lock:
            # Filter security logs
            self.tracer.security_logs = deque(
                [log for log in self.tracer.security_logs if log.timestamp >= cutoff_time],
                maxlen=10000
            )
            
            # Clean up correlation map (remove old entries)
            for correlation_id in list(self.tracer.correlation_map.keys()):
                if not self.tracer.correlation_map[correlation_id]:  # Empty list
                    del self.tracer.correlation_map[correlation_id]
        
        with self._lock:
            # Clean up security metrics
            self.security_metrics = deque(
                [m for m in self.security_metrics if m.timestamp >= cutoff_time],
                maxlen=5000
            )
        
        self.logger.info(f"Cleaned up security traces older than {retention_hours} hours")


# Global security observability orchestrator
_security_observability: Optional[SecurityObservabilityOrchestrator] = None

def get_security_observability(service_name: str = "security-platform") -> SecurityObservabilityOrchestrator:
    """Get global security observability orchestrator"""
    global _security_observability
    if _security_observability is None:
        _security_observability = SecurityObservabilityOrchestrator(service_name)
    return _security_observability


# Convenience decorators and context managers for security tracing
def trace_security_operation(
    operation_name: str,
    trace_type: SecurityTraceType,
    extract_context: bool = True
):
    """Decorator for tracing security operations"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            orchestrator = get_security_observability()
            
            # Extract security context from function arguments if requested
            security_context = {}
            if extract_context:
                # Look for common security parameters
                for arg_name in ['user_id', 'source_ip', 'session_id', 'resource_id']:
                    if arg_name in kwargs:
                        security_context[arg_name] = kwargs[arg_name]
            
            with orchestrator.tracer.trace_security_operation(
                operation_name, trace_type, security_context
            ) as context:
                
                # Add operation result to context
                try:
                    result = func(*args, **kwargs)
                    context.custom_attributes['outcome'] = 'success'
                    return result
                except Exception as e:
                    context.custom_attributes['outcome'] = 'failure'
                    context.custom_attributes['error'] = str(e)
                    raise
        
        return wrapper
    return decorator


@asynccontextmanager
async def security_workflow_context(
    workflow_name: str,
    trace_type: SecurityTraceType,
    user_id: Optional[str] = None,
    source_ip: Optional[str] = None,
    **context_kwargs
):
    """Async context manager for security workflows"""
    orchestrator = get_security_observability()
    
    async with orchestrator.trace_security_workflow(
        workflow_name, trace_type, user_id, source_ip, 
        additional_context=context_kwargs
    ) as context:
        yield context