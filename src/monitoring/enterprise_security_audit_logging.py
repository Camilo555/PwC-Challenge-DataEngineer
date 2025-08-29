"""
Enterprise Security and Audit Logging Framework

This module provides comprehensive security and audit logging capabilities including:
- Authentication and authorization event logging
- Data access and modification tracking
- Compliance logging (SOX, GDPR, HIPAA, PCI-DSS)
- Security incident detection and response
- Audit trail reconstruction and reporting
- Regulatory compliance reporting
- Data loss prevention (DLP) logging
"""

import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Union
from enum import Enum
from dataclasses import dataclass, asdict, field
from pathlib import Path
from collections import defaultdict, deque
import threading
import queue
import ipaddress
import re
import base64
from cryptography.fernet import Fernet
import bcrypt

# Internal imports
from ..core.logging import get_logger
from .enterprise_datadog_log_collection import LogEntry, LogLevel, LogSource

logger = get_logger(__name__)


class SecurityEventType(Enum):
    """Types of security events"""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    SECURITY_VIOLATION = "security_violation"
    CONFIGURATION_CHANGE = "configuration_change"
    SYSTEM_COMPROMISE = "system_compromise"
    DATA_EXFILTRATION = "data_exfiltration"
    MALWARE_DETECTION = "malware_detection"
    NETWORK_INTRUSION = "network_intrusion"


class ComplianceFramework(Enum):
    """Supported compliance frameworks"""
    SOX = "sox"  # Sarbanes-Oxley Act
    GDPR = "gdpr"  # General Data Protection Regulation
    HIPAA = "hipaa"  # Health Insurance Portability and Accountability Act
    PCI_DSS = "pci_dss"  # Payment Card Industry Data Security Standard
    ISO27001 = "iso27001"  # ISO 27001
    NIST = "nist"  # NIST Cybersecurity Framework
    CCPA = "ccpa"  # California Consumer Privacy Act
    FISMA = "fisma"  # Federal Information Security Management Act


class AuditEventType(Enum):
    """Types of audit events"""
    USER_ACTIVITY = "user_activity"
    SYSTEM_ACTIVITY = "system_activity"
    DATA_ACTIVITY = "data_activity"
    CONFIGURATION_CHANGE = "configuration_change"
    FINANCIAL_TRANSACTION = "financial_transaction"
    COMPLIANCE_EVENT = "compliance_event"
    ADMINISTRATIVE_ACTION = "administrative_action"


class RiskLevel(Enum):
    """Risk levels for security events"""
    VERY_LOW = "very_low"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DataClassification(Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


@dataclass
class SecurityEvent:
    """Security event data structure"""
    event_id: str
    timestamp: datetime
    event_type: SecurityEventType
    risk_level: RiskLevel
    user_id: Optional[str]
    session_id: Optional[str]
    ip_address: Optional[str]
    user_agent: Optional[str]
    resource: str
    action: str
    result: str  # success, failure, blocked
    details: Dict[str, Any]
    compliance_frameworks: List[ComplianceFramework] = field(default_factory=list)
    data_classification: Optional[DataClassification] = None
    geolocation: Optional[Dict[str, str]] = None
    device_info: Optional[Dict[str, str]] = None
    threat_indicators: List[str] = field(default_factory=list)
    
    def to_log_entry(self) -> LogEntry:
        """Convert to standard log entry"""
        return LogEntry(
            timestamp=self.timestamp,
            level=self._get_log_level(),
            message=self._format_message(),
            source=LogSource.SECURITY,
            service="security-monitor",
            environment="production",
            user_id=self.user_id,
            session_id=self.session_id,
            ip_address=self.ip_address,
            tags={
                "event_type": self.event_type.value,
                "risk_level": self.risk_level.value,
                "result": self.result,
                "resource": self.resource,
                "action": self.action
            },
            metadata={
                "event_id": self.event_id,
                "compliance_frameworks": [f.value for f in self.compliance_frameworks],
                "data_classification": self.data_classification.value if self.data_classification else None,
                "details": self.details,
                "geolocation": self.geolocation,
                "device_info": self.device_info,
                "threat_indicators": self.threat_indicators
            }
        )
    
    def _get_log_level(self) -> LogLevel:
        """Get appropriate log level based on risk"""
        if self.risk_level == RiskLevel.CRITICAL:
            return LogLevel.CRITICAL
        elif self.risk_level == RiskLevel.HIGH:
            return LogLevel.ERROR
        elif self.risk_level == RiskLevel.MEDIUM:
            return LogLevel.WARNING
        else:
            return LogLevel.INFO
    
    def _format_message(self) -> str:
        """Format security event message"""
        return f"Security Event: {self.event_type.value} - {self.action} on {self.resource} by {self.user_id or 'unknown'} - {self.result}"


@dataclass
class AuditEvent:
    """Audit event data structure"""
    audit_id: str
    timestamp: datetime
    event_type: AuditEventType
    user_id: Optional[str]
    session_id: Optional[str]
    ip_address: Optional[str]
    resource_type: str
    resource_id: str
    action: str
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    compliance_frameworks: List[ComplianceFramework] = field(default_factory=list)
    retention_period_days: int = 2555  # 7 years default
    is_sensitive: bool = False
    business_context: Optional[str] = None
    
    def to_log_entry(self) -> LogEntry:
        """Convert to standard log entry"""
        return LogEntry(
            timestamp=self.timestamp,
            level=LogLevel.INFO,
            message=self._format_message(),
            source=LogSource.AUDIT,
            service="audit-monitor",
            environment="production",
            user_id=self.user_id,
            session_id=self.session_id,
            ip_address=self.ip_address,
            tags={
                "event_type": self.event_type.value,
                "resource_type": self.resource_type,
                "resource_id": self.resource_id,
                "action": self.action,
                "is_sensitive": str(self.is_sensitive)
            },
            metadata={
                "audit_id": self.audit_id,
                "old_values": self.old_values,
                "new_values": self.new_values,
                "compliance_frameworks": [f.value for f in self.compliance_frameworks],
                "retention_period_days": self.retention_period_days,
                "business_context": self.business_context
            }
        )
    
    def _format_message(self) -> str:
        """Format audit event message"""
        return f"Audit: {self.event_type.value} - {self.action} on {self.resource_type}:{self.resource_id} by {self.user_id or 'system'}"


@dataclass 
class ComplianceRule:
    """Compliance rule definition"""
    rule_id: str
    framework: ComplianceFramework
    requirement: str
    description: str
    log_retention_days: int
    required_fields: List[str]
    monitoring_patterns: List[str]
    alert_on_violation: bool = True
    auto_remediation: bool = False
    
    
class SecurityEventDetector:
    """Detects security events from various sources"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.SecurityEventDetector")
        self.detection_rules = self._initialize_detection_rules()
        self.threat_intelligence = self._initialize_threat_intelligence()
        self.geo_ip_cache = {}
        
    def _initialize_detection_rules(self) -> Dict[str, Any]:
        """Initialize security detection rules"""
        return {
            "brute_force_login": {
                "pattern": r"(?i)login.*failed",
                "threshold": 5,
                "time_window_minutes": 15,
                "risk_level": RiskLevel.HIGH,
                "event_type": SecurityEventType.SUSPICIOUS_ACTIVITY
            },
            "privilege_escalation": {
                "pattern": r"(?i)privilege.*escalat|admin.*access|root.*access",
                "threshold": 1,
                "time_window_minutes": 5,
                "risk_level": RiskLevel.CRITICAL,
                "event_type": SecurityEventType.PRIVILEGE_ESCALATION
            },
            "data_exfiltration": {
                "pattern": r"(?i)large.*download|bulk.*export|mass.*data",
                "threshold": 1,
                "time_window_minutes": 10,
                "risk_level": RiskLevel.CRITICAL,
                "event_type": SecurityEventType.DATA_EXFILTRATION
            },
            "sql_injection": {
                "pattern": r"(?i)union.*select|drop.*table|insert.*into.*values",
                "threshold": 1,
                "time_window_minutes": 1,
                "risk_level": RiskLevel.HIGH,
                "event_type": SecurityEventType.SECURITY_VIOLATION
            },
            "unauthorized_access": {
                "pattern": r"(?i)access.*denied|unauthorized|forbidden",
                "threshold": 10,
                "time_window_minutes": 5,
                "risk_level": RiskLevel.MEDIUM,
                "event_type": SecurityEventType.AUTHORIZATION
            }
        }
    
    def _initialize_threat_intelligence(self) -> Dict[str, Any]:
        """Initialize threat intelligence data"""
        return {
            "known_malicious_ips": [
                "192.168.1.100",  # Example malicious IP
                "10.0.0.50"       # Example malicious IP
            ],
            "suspicious_user_agents": [
                "sqlmap",
                "nikto",
                "nmap",
                "masscan"
            ],
            "malicious_patterns": [
                r"(?i)<script.*?>.*?</script>",  # XSS
                r"(?i)eval\s*\(",                # Code injection
                r"(?i)system\s*\(",             # Command injection
                r"(?i)exec\s*\("               # Command execution
            ]
        }
    
    def detect_security_events(self, logs: List[LogEntry]) -> List[SecurityEvent]:
        """Detect security events from logs"""
        try:
            events = []
            
            # Group logs by time window for pattern detection
            time_windows = self._group_logs_by_time_window(logs, 15)  # 15-minute windows
            
            for time_window in time_windows:
                # Apply detection rules
                for rule_name, rule_config in self.detection_rules.items():
                    rule_events = self._apply_detection_rule(rule_name, rule_config, time_window)
                    events.extend(rule_events)
                
                # Detect anomalous behavior
                anomaly_events = self._detect_security_anomalies(time_window)
                events.extend(anomaly_events)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error detecting security events: {e}")
            return []
    
    def _group_logs_by_time_window(self, logs: List[LogEntry], minutes: int) -> List[List[LogEntry]]:
        """Group logs into time windows"""
        if not logs:
            return []
        
        sorted_logs = sorted(logs, key=lambda x: x.timestamp)
        windows = []
        current_window = []
        window_start = None
        
        for log in sorted_logs:
            if window_start is None:
                window_start = log.timestamp
                current_window = [log]
            elif (log.timestamp - window_start).total_seconds() <= minutes * 60:
                current_window.append(log)
            else:
                if current_window:
                    windows.append(current_window)
                window_start = log.timestamp
                current_window = [log]
        
        if current_window:
            windows.append(current_window)
        
        return windows
    
    def _apply_detection_rule(self, rule_name: str, rule_config: Dict[str, Any], logs: List[LogEntry]) -> List[SecurityEvent]:
        """Apply a specific detection rule"""
        try:
            events = []
            pattern = rule_config["pattern"]
            threshold = rule_config["threshold"]
            risk_level = rule_config["risk_level"]
            event_type = rule_config["event_type"]
            
            # Find matching logs
            matching_logs = []
            for log in logs:
                if re.search(pattern, log.message, re.IGNORECASE):
                    matching_logs.append(log)
            
            # Check if threshold is met
            if len(matching_logs) >= threshold:
                # Create security event
                event = SecurityEvent(
                    event_id=self._generate_event_id(),
                    timestamp=datetime.now(),
                    event_type=event_type,
                    risk_level=risk_level,
                    user_id=matching_logs[0].user_id,
                    session_id=matching_logs[0].session_id,
                    ip_address=matching_logs[0].ip_address,
                    resource=rule_name,
                    action="pattern_detected",
                    result="detected",
                    details={
                        "rule_name": rule_name,
                        "matches_count": len(matching_logs),
                        "threshold": threshold,
                        "sample_messages": [log.message for log in matching_logs[:3]]
                    },
                    threat_indicators=[rule_name]
                )
                
                # Add threat intelligence context
                self._enrich_with_threat_intelligence(event, matching_logs)
                
                events.append(event)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error applying detection rule {rule_name}: {e}")
            return []
    
    def _detect_security_anomalies(self, logs: List[LogEntry]) -> List[SecurityEvent]:
        """Detect security anomalies"""
        try:
            events = []
            
            # Detect IP-based anomalies
            ip_events = self._detect_ip_anomalies(logs)
            events.extend(ip_events)
            
            # Detect user behavior anomalies
            user_events = self._detect_user_anomalies(logs)
            events.extend(user_events)
            
            # Detect time-based anomalies
            time_events = self._detect_time_anomalies(logs)
            events.extend(time_events)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error detecting security anomalies: {e}")
            return []
    
    def _detect_ip_anomalies(self, logs: List[LogEntry]) -> List[SecurityEvent]:
        """Detect IP-based security anomalies"""
        try:
            events = []
            ip_activities = defaultdict(list)
            
            # Group activities by IP
            for log in logs:
                if log.ip_address:
                    ip_activities[log.ip_address].append(log)
            
            for ip_address, ip_logs in ip_activities.items():
                # Check for known malicious IPs
                if ip_address in self.threat_intelligence["known_malicious_ips"]:
                    event = SecurityEvent(
                        event_id=self._generate_event_id(),
                        timestamp=datetime.now(),
                        event_type=SecurityEventType.NETWORK_INTRUSION,
                        risk_level=RiskLevel.CRITICAL,
                        user_id=None,
                        session_id=None,
                        ip_address=ip_address,
                        resource="network",
                        action="malicious_ip_detected",
                        result="blocked",
                        details={
                            "ip_address": ip_address,
                            "activity_count": len(ip_logs),
                            "threat_source": "threat_intelligence"
                        },
                        threat_indicators=["known_malicious_ip"]
                    )
                    events.append(event)
                
                # Check for excessive activity from single IP
                if len(ip_logs) > 100:  # High activity threshold
                    event = SecurityEvent(
                        event_id=self._generate_event_id(),
                        timestamp=datetime.now(),
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        risk_level=RiskLevel.MEDIUM,
                        user_id=None,
                        session_id=None,
                        ip_address=ip_address,
                        resource="network",
                        action="high_activity_detected",
                        result="monitored",
                        details={
                            "ip_address": ip_address,
                            "activity_count": len(ip_logs),
                            "threshold": 100
                        },
                        threat_indicators=["high_activity_ip"]
                    )
                    events.append(event)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error detecting IP anomalies: {e}")
            return []
    
    def _detect_user_anomalies(self, logs: List[LogEntry]) -> List[SecurityEvent]:
        """Detect user behavior anomalies"""
        try:
            events = []
            user_activities = defaultdict(list)
            
            # Group activities by user
            for log in logs:
                if log.user_id:
                    user_activities[log.user_id].append(log)
            
            for user_id, user_logs in user_activities.items():
                # Check for unusual activity patterns
                unique_ips = set(log.ip_address for log in user_logs if log.ip_address)
                
                # Multiple IP addresses for same user (potential account compromise)
                if len(unique_ips) > 3:
                    event = SecurityEvent(
                        event_id=self._generate_event_id(),
                        timestamp=datetime.now(),
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        risk_level=RiskLevel.MEDIUM,
                        user_id=user_id,
                        session_id=None,
                        ip_address=list(unique_ips)[0] if unique_ips else None,
                        resource="user_account",
                        action="multiple_ip_access",
                        result="flagged",
                        details={
                            "user_id": user_id,
                            "unique_ips": list(unique_ips),
                            "ip_count": len(unique_ips),
                            "activity_count": len(user_logs)
                        },
                        threat_indicators=["multiple_ip_access"]
                    )
                    events.append(event)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error detecting user anomalies: {e}")
            return []
    
    def _detect_time_anomalies(self, logs: List[LogEntry]) -> List[SecurityEvent]:
        """Detect time-based anomalies"""
        try:
            events = []
            
            # Check for after-hours activity
            after_hours_logs = []
            for log in logs:
                if log.timestamp.hour < 6 or log.timestamp.hour > 22:  # Outside business hours
                    if log.user_id:  # Only for authenticated users
                        after_hours_logs.append(log)
            
            if len(after_hours_logs) > 10:  # Threshold for after-hours activity
                users = set(log.user_id for log in after_hours_logs)
                event = SecurityEvent(
                    event_id=self._generate_event_id(),
                    timestamp=datetime.now(),
                    event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                    risk_level=RiskLevel.MEDIUM,
                    user_id=list(users)[0] if users else None,
                    session_id=None,
                    ip_address=None,
                    resource="system",
                    action="after_hours_activity",
                    result="monitored",
                    details={
                        "activity_count": len(after_hours_logs),
                        "users_involved": list(users),
                        "time_range": "after_hours"
                    },
                    threat_indicators=["after_hours_activity"]
                )
                events.append(event)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error detecting time anomalies: {e}")
            return []
    
    def _enrich_with_threat_intelligence(self, event: SecurityEvent, logs: List[LogEntry]):
        """Enrich security event with threat intelligence"""
        try:
            # Check for malicious user agents
            for log in logs:
                if log.user_agent:
                    for suspicious_agent in self.threat_intelligence["suspicious_user_agents"]:
                        if suspicious_agent.lower() in log.user_agent.lower():
                            event.threat_indicators.append(f"suspicious_user_agent:{suspicious_agent}")
            
            # Check for malicious patterns
            for log in logs:
                for pattern in self.threat_intelligence["malicious_patterns"]:
                    if re.search(pattern, log.message, re.IGNORECASE):
                        event.threat_indicators.append("malicious_pattern_detected")
                        break
            
            # Add geolocation context if IP available
            if event.ip_address:
                event.geolocation = self._get_geolocation(event.ip_address)
                
        except Exception as e:
            self.logger.error(f"Error enriching with threat intelligence: {e}")
    
    def _get_geolocation(self, ip_address: str) -> Optional[Dict[str, str]]:
        """Get geolocation for IP address (simplified)"""
        try:
            # In a real implementation, this would use a GeoIP service
            if ip_address in self.geo_ip_cache:
                return self.geo_ip_cache[ip_address]
            
            # Simplified geolocation based on IP ranges
            if ip_address.startswith("192.168.") or ip_address.startswith("10."):
                geo_info = {"country": "Internal", "city": "Private Network"}
            else:
                geo_info = {"country": "Unknown", "city": "Unknown"}
            
            self.geo_ip_cache[ip_address] = geo_info
            return geo_info
            
        except Exception as e:
            self.logger.error(f"Error getting geolocation for {ip_address}: {e}")
            return None
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID"""
        return f"SEC_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"


class ComplianceMonitor:
    """Monitors compliance with various regulatory frameworks"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.ComplianceMonitor")
        self.compliance_rules = self._initialize_compliance_rules()
        self.violation_history = defaultdict(list)
        
    def _initialize_compliance_rules(self) -> Dict[ComplianceFramework, List[ComplianceRule]]:
        """Initialize compliance rules for different frameworks"""
        return {
            ComplianceFramework.GDPR: [
                ComplianceRule(
                    rule_id="GDPR_001",
                    framework=ComplianceFramework.GDPR,
                    requirement="Article 30 - Records of processing activities",
                    description="All personal data processing must be logged",
                    log_retention_days=2555,  # 7 years
                    required_fields=["user_id", "data_type", "purpose", "legal_basis"],
                    monitoring_patterns=[r"(?i)personal.*data", r"(?i)gdpr.*processing"]
                ),
                ComplianceRule(
                    rule_id="GDPR_002", 
                    framework=ComplianceFramework.GDPR,
                    requirement="Article 32 - Security of processing",
                    description="Security incidents must be logged and monitored",
                    log_retention_days=2555,
                    required_fields=["incident_type", "affected_data", "containment_actions"],
                    monitoring_patterns=[r"(?i)data.*breach", r"(?i)security.*incident"]
                )
            ],
            ComplianceFramework.HIPAA: [
                ComplianceRule(
                    rule_id="HIPAA_001",
                    framework=ComplianceFramework.HIPAA,
                    requirement="§164.312(b) - Audit controls",
                    description="Audit controls must be implemented for PHI access",
                    log_retention_days=2190,  # 6 years
                    required_fields=["user_id", "patient_id", "phi_accessed", "purpose"],
                    monitoring_patterns=[r"(?i)phi.*access", r"(?i)health.*information"]
                )
            ],
            ComplianceFramework.SOX: [
                ComplianceRule(
                    rule_id="SOX_001",
                    framework=ComplianceFramework.SOX,
                    requirement="Section 404 - Management assessment of internal controls",
                    description="Financial data access must be logged and controlled",
                    log_retention_days=2555,  # 7 years
                    required_fields=["user_id", "financial_data", "transaction_id", "approval"],
                    monitoring_patterns=[r"(?i)financial.*data", r"(?i)transaction.*process"]
                )
            ],
            ComplianceFramework.PCI_DSS: [
                ComplianceRule(
                    rule_id="PCI_001",
                    framework=ComplianceFramework.PCI_DSS,
                    requirement="Requirement 10 - Track and monitor all access",
                    description="All cardholder data access must be logged",
                    log_retention_days=365,  # 1 year minimum
                    required_fields=["user_id", "card_data_accessed", "transaction_id"],
                    monitoring_patterns=[r"(?i)card.*data", r"(?i)payment.*process"]
                )
            ]
        }
    
    def monitor_compliance(self, logs: List[LogEntry]) -> Dict[str, Any]:
        """Monitor logs for compliance violations"""
        try:
            compliance_report = {
                "timestamp": datetime.now(),
                "frameworks_checked": [],
                "violations": [],
                "compliant_events": 0,
                "total_events": len(logs),
                "coverage_analysis": {}
            }
            
            # Check each compliance framework
            for framework, rules in self.compliance_rules.items():
                compliance_report["frameworks_checked"].append(framework.value)
                
                framework_violations = self._check_framework_compliance(framework, rules, logs)
                compliance_report["violations"].extend(framework_violations)
                
                # Analyze coverage for this framework
                coverage = self._analyze_compliance_coverage(framework, rules, logs)
                compliance_report["coverage_analysis"][framework.value] = coverage
            
            # Calculate compliance metrics
            compliance_report["violation_count"] = len(compliance_report["violations"])
            compliance_report["compliance_score"] = self._calculate_compliance_score(compliance_report)
            
            return compliance_report
            
        except Exception as e:
            self.logger.error(f"Error monitoring compliance: {e}")
            return {"error": str(e), "timestamp": datetime.now()}
    
    def _check_framework_compliance(self, framework: ComplianceFramework, rules: List[ComplianceRule], 
                                   logs: List[LogEntry]) -> List[Dict[str, Any]]:
        """Check compliance for a specific framework"""
        try:
            violations = []
            
            for rule in rules:
                rule_violations = self._check_rule_compliance(rule, logs)
                violations.extend(rule_violations)
            
            return violations
            
        except Exception as e:
            self.logger.error(f"Error checking {framework.value} compliance: {e}")
            return []
    
    def _check_rule_compliance(self, rule: ComplianceRule, logs: List[LogEntry]) -> List[Dict[str, Any]]:
        """Check compliance for a specific rule"""
        try:
            violations = []
            
            # Find logs that match the rule patterns
            matching_logs = []
            for log in logs:
                for pattern in rule.monitoring_patterns:
                    if re.search(pattern, log.message, re.IGNORECASE):
                        matching_logs.append(log)
                        break
            
            # Check each matching log for required fields
            for log in matching_logs:
                missing_fields = []
                
                for required_field in rule.required_fields:
                    if not self._has_required_field(log, required_field):
                        missing_fields.append(required_field)
                
                if missing_fields:
                    violation = {
                        "rule_id": rule.rule_id,
                        "framework": rule.framework.value,
                        "requirement": rule.requirement,
                        "violation_type": "missing_required_fields",
                        "missing_fields": missing_fields,
                        "log_timestamp": log.timestamp,
                        "log_message": log.message[:200],  # Truncated
                        "severity": "medium"
                    }
                    violations.append(violation)
            
            return violations
            
        except Exception as e:
            self.logger.error(f"Error checking rule {rule.rule_id} compliance: {e}")
            return []
    
    def _has_required_field(self, log: LogEntry, field_name: str) -> bool:
        """Check if log has required field"""
        try:
            # Check standard fields
            if hasattr(log, field_name) and getattr(log, field_name) is not None:
                return True
            
            # Check tags
            if log.tags and field_name in log.tags and log.tags[field_name]:
                return True
            
            # Check metadata
            if log.metadata and field_name in log.metadata and log.metadata[field_name]:
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking required field {field_name}: {e}")
            return False
    
    def _analyze_compliance_coverage(self, framework: ComplianceFramework, rules: List[ComplianceRule], 
                                   logs: List[LogEntry]) -> Dict[str, Any]:
        """Analyze compliance coverage for a framework"""
        try:
            coverage = {
                "framework": framework.value,
                "rules_total": len(rules),
                "rules_covered": 0,
                "coverage_percentage": 0.0,
                "logs_requiring_compliance": 0,
                "compliant_logs": 0
            }
            
            rules_with_matches = 0
            total_requiring_compliance = 0
            total_compliant = 0
            
            for rule in rules:
                # Count logs that require this compliance rule
                requiring_logs = 0
                compliant_logs = 0
                
                for log in logs:
                    for pattern in rule.monitoring_patterns:
                        if re.search(pattern, log.message, re.IGNORECASE):
                            requiring_logs += 1
                            
                            # Check if it's compliant (has all required fields)
                            is_compliant = all(self._has_required_field(log, field) 
                                             for field in rule.required_fields)
                            if is_compliant:
                                compliant_logs += 1
                            break
                
                if requiring_logs > 0:
                    rules_with_matches += 1
                    total_requiring_compliance += requiring_logs
                    total_compliant += compliant_logs
            
            coverage["rules_covered"] = rules_with_matches
            coverage["coverage_percentage"] = (rules_with_matches / len(rules)) * 100 if rules else 0
            coverage["logs_requiring_compliance"] = total_requiring_compliance
            coverage["compliant_logs"] = total_compliant
            coverage["compliance_rate"] = (total_compliant / total_requiring_compliance) * 100 if total_requiring_compliance > 0 else 100
            
            return coverage
            
        except Exception as e:
            self.logger.error(f"Error analyzing compliance coverage: {e}")
            return {}
    
    def _calculate_compliance_score(self, compliance_report: Dict[str, Any]) -> float:
        """Calculate overall compliance score"""
        try:
            total_score = 0.0
            framework_count = len(compliance_report["frameworks_checked"])
            
            if framework_count == 0:
                return 100.0
            
            for framework_name in compliance_report["frameworks_checked"]:
                framework_coverage = compliance_report["coverage_analysis"].get(framework_name, {})
                compliance_rate = framework_coverage.get("compliance_rate", 100.0)
                total_score += compliance_rate
            
            return total_score / framework_count
            
        except Exception as e:
            self.logger.error(f"Error calculating compliance score: {e}")
            return 0.0


class AuditTrailManager:
    """Manages audit trails for compliance and investigation purposes"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.AuditTrailManager")
        self.audit_storage = Path("./audit_trails")
        self.audit_storage.mkdir(exist_ok=True)
        
        # Initialize encryption for sensitive data
        self.encryption_key = self._get_or_create_encryption_key()
        self.cipher_suite = Fernet(self.encryption_key)
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for audit data"""
        key_file = self.audit_storage / "audit.key"
        
        if key_file.exists():
            return key_file.read_bytes()
        else:
            key = Fernet.generate_key()
            key_file.write_bytes(key)
            # Set restrictive permissions (owner read/write only)
            key_file.chmod(0o600)
            return key
    
    def create_audit_trail(self, events: List[AuditEvent]) -> str:
        """Create an audit trail from audit events"""
        try:
            if not events:
                return ""
            
            trail_id = self._generate_trail_id()
            trail_data = {
                "trail_id": trail_id,
                "created_timestamp": datetime.now().isoformat(),
                "events_count": len(events),
                "events": [asdict(event) for event in events],
                "integrity_hash": None
            }
            
            # Calculate integrity hash
            trail_json = json.dumps(trail_data["events"], sort_keys=True)
            trail_data["integrity_hash"] = hashlib.sha256(trail_json.encode()).hexdigest()
            
            # Encrypt sensitive data
            if any(event.is_sensitive for event in events):
                trail_data = self._encrypt_sensitive_data(trail_data)
            
            # Store audit trail
            trail_file = self.audit_storage / f"{trail_id}.json"
            with open(trail_file, 'w') as f:
                json.dump(trail_data, f, indent=2, default=str)
            
            self.logger.info(f"Created audit trail {trail_id} with {len(events)} events")
            return trail_id
            
        except Exception as e:
            self.logger.error(f"Error creating audit trail: {e}")
            return ""
    
    def reconstruct_audit_trail(self, resource_type: str, resource_id: str, 
                              start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Reconstruct audit trail for a specific resource"""
        try:
            trail_data = {
                "resource_type": resource_type,
                "resource_id": resource_id,
                "time_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat()
                },
                "events": [],
                "reconstruction_timestamp": datetime.now().isoformat()
            }
            
            # Search through stored audit trails
            for trail_file in self.audit_storage.glob("*.json"):
                try:
                    with open(trail_file, 'r') as f:
                        stored_trail = json.load(f)
                    
                    # Check each event in the stored trail
                    for event_data in stored_trail.get("events", []):
                        if (event_data.get("resource_type") == resource_type and
                            event_data.get("resource_id") == resource_id):
                            
                            # Parse timestamp
                            event_time = datetime.fromisoformat(event_data["timestamp"])
                            
                            # Check if event is in time range
                            if start_time <= event_time <= end_time:
                                trail_data["events"].append(event_data)
                
                except Exception as e:
                    self.logger.warning(f"Error reading trail file {trail_file}: {e}")
                    continue
            
            # Sort events by timestamp
            trail_data["events"].sort(key=lambda x: x["timestamp"])
            trail_data["events_count"] = len(trail_data["events"])
            
            return trail_data
            
        except Exception as e:
            self.logger.error(f"Error reconstructing audit trail: {e}")
            return {"error": str(e)}
    
    def verify_audit_integrity(self, trail_id: str) -> Dict[str, Any]:
        """Verify the integrity of an audit trail"""
        try:
            trail_file = self.audit_storage / f"{trail_id}.json"
            
            if not trail_file.exists():
                return {"valid": False, "error": "Trail not found"}
            
            with open(trail_file, 'r') as f:
                trail_data = json.load(f)
            
            # Recalculate hash
            events_json = json.dumps(trail_data.get("events", []), sort_keys=True)
            calculated_hash = hashlib.sha256(events_json.encode()).hexdigest()
            stored_hash = trail_data.get("integrity_hash")
            
            is_valid = calculated_hash == stored_hash
            
            return {
                "trail_id": trail_id,
                "valid": is_valid,
                "stored_hash": stored_hash,
                "calculated_hash": calculated_hash,
                "verification_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error verifying audit integrity for {trail_id}: {e}")
            return {"valid": False, "error": str(e)}
    
    def _encrypt_sensitive_data(self, trail_data: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive data in audit trail"""
        try:
            sensitive_fields = ["old_values", "new_values", "user_id", "details"]
            
            for event in trail_data["events"]:
                for field in sensitive_fields:
                    if field in event and event[field]:
                        # Convert to JSON string and encrypt
                        field_json = json.dumps(event[field])
                        encrypted_data = self.cipher_suite.encrypt(field_json.encode())
                        event[field] = base64.b64encode(encrypted_data).decode()
                        event[f"{field}_encrypted"] = True
            
            return trail_data
            
        except Exception as e:
            self.logger.error(f"Error encrypting sensitive data: {e}")
            return trail_data
    
    def _generate_trail_id(self) -> str:
        """Generate unique trail ID"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        random_hash = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        return f"AUDIT_{timestamp}_{random_hash}"


class EnterpriseSecurityAuditSystem:
    """Main enterprise security and audit logging system"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.EnterpriseSecurityAuditSystem")
        
        # Initialize components
        self.security_detector = SecurityEventDetector()
        self.compliance_monitor = ComplianceMonitor()
        self.audit_manager = AuditTrailManager()
        
        # System state
        self.is_running = False
        self.monitoring_thread = None
        self.event_queue = queue.Queue(maxsize=10000)
        self.audit_queue = queue.Queue(maxsize=10000)
        
        # Statistics
        self.stats = {
            "security_events_detected": 0,
            "compliance_violations": 0,
            "audit_trails_created": 0,
            "start_time": None
        }
    
    def start_monitoring(self):
        """Start security and audit monitoring"""
        try:
            self.is_running = True
            self.stats["start_time"] = datetime.now()
            
            # Start monitoring thread
            self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitoring_thread.start()
            
            self.logger.info("Enterprise Security and Audit System started")
            
        except Exception as e:
            self.logger.error(f"Error starting security monitoring: {e}")
    
    def stop_monitoring(self):
        """Stop security and audit monitoring"""
        try:
            self.is_running = False
            
            if self.monitoring_thread:
                self.monitoring_thread.join(timeout=10)
            
            # Process remaining events
            self._process_remaining_events()
            
            self.logger.info("Enterprise Security and Audit System stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping security monitoring: {e}")
    
    def process_logs(self, logs: List[LogEntry]) -> Dict[str, Any]:
        """Process logs for security and audit events"""
        try:
            processing_start = datetime.now()
            
            # Detect security events
            security_events = self.security_detector.detect_security_events(logs)
            self.stats["security_events_detected"] += len(security_events)
            
            # Monitor compliance
            compliance_report = self.compliance_monitor.monitor_compliance(logs)
            self.stats["compliance_violations"] += compliance_report.get("violation_count", 0)
            
            # Create audit events from security events
            audit_events = []
            for security_event in security_events:
                audit_event = AuditEvent(
                    audit_id=f"AUDIT_{security_event.event_id}",
                    timestamp=security_event.timestamp,
                    event_type=AuditEventType.COMPLIANCE_EVENT,
                    user_id=security_event.user_id,
                    session_id=security_event.session_id,
                    ip_address=security_event.ip_address,
                    resource_type="security_event",
                    resource_id=security_event.event_id,
                    action=security_event.action,
                    new_values=security_event.details,
                    compliance_frameworks=security_event.compliance_frameworks,
                    is_sensitive=security_event.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]
                )
                audit_events.append(audit_event)
            
            # Create audit trail
            trail_id = ""
            if audit_events:
                trail_id = self.audit_manager.create_audit_trail(audit_events)
                if trail_id:
                    self.stats["audit_trails_created"] += 1
            
            # Generate summary report
            processing_time = (datetime.now() - processing_start).total_seconds()
            
            return {
                "timestamp": processing_start.isoformat(),
                "processing_time_seconds": processing_time,
                "logs_processed": len(logs),
                "security_events": [asdict(event) for event in security_events],
                "compliance_report": compliance_report,
                "audit_trail_id": trail_id,
                "statistics": self.get_statistics()
            }
            
        except Exception as e:
            self.logger.error(f"Error processing logs for security and audit: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                time.sleep(60)  # Check every minute
                
                # Process queued events
                self._process_queued_events()
                
                # Perform periodic compliance checks
                self._periodic_compliance_check()
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
    
    def _process_queued_events(self):
        """Process events from queues"""
        try:
            # Process security events
            events_to_process = []
            while not self.event_queue.empty() and len(events_to_process) < 100:
                try:
                    event = self.event_queue.get_nowait()
                    events_to_process.append(event)
                except queue.Empty:
                    break
            
            if events_to_process:
                self.logger.debug(f"Processing {len(events_to_process)} queued security events")
            
            # Process audit events
            audit_events = []
            while not self.audit_queue.empty() and len(audit_events) < 100:
                try:
                    audit_event = self.audit_queue.get_nowait()
                    audit_events.append(audit_event)
                except queue.Empty:
                    break
            
            if audit_events:
                trail_id = self.audit_manager.create_audit_trail(audit_events)
                if trail_id:
                    self.stats["audit_trails_created"] += 1
                
        except Exception as e:
            self.logger.error(f"Error processing queued events: {e}")
    
    def _periodic_compliance_check(self):
        """Perform periodic compliance checks"""
        try:
            self.logger.debug("Performing periodic compliance check")
            
            # In a real implementation, this would check for:
            # - Expired audit trails that need archiving
            # - Missing compliance documentation
            # - Overdue security reviews
            # - Compliance reporting requirements
            
        except Exception as e:
            self.logger.error(f"Error in periodic compliance check: {e}")
    
    def _process_remaining_events(self):
        """Process any remaining events before shutdown"""
        try:
            self.logger.info("Processing remaining events before shutdown")
            self._process_queued_events()
            
        except Exception as e:
            self.logger.error(f"Error processing remaining events: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get system statistics"""
        current_stats = self.stats.copy()
        if current_stats["start_time"]:
            current_stats["uptime_seconds"] = (datetime.now() - current_stats["start_time"]).total_seconds()
        current_stats["event_queue_size"] = self.event_queue.qsize()
        current_stats["audit_queue_size"] = self.audit_queue.qsize()
        current_stats["is_running"] = self.is_running
        return current_stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform system health check"""
        stats = self.get_statistics()
        
        health = {
            "status": "healthy",
            "is_running": self.is_running,
            "queue_utilization": {
                "event_queue": stats["event_queue_size"] / 10000,
                "audit_queue": stats["audit_queue_size"] / 10000
            },
            "statistics": stats,
            "timestamp": datetime.now().isoformat()
        }
        
        # Determine health status
        if (health["queue_utilization"]["event_queue"] > 0.8 or 
            health["queue_utilization"]["audit_queue"] > 0.8):
            health["status"] = "degraded"
        
        if not health["is_running"]:
            health["status"] = "unhealthy"
        
        return health


# Factory function
def create_enterprise_security_audit_system() -> EnterpriseSecurityAuditSystem:
    """Create enterprise security and audit system"""
    return EnterpriseSecurityAuditSystem()


# Example usage
if __name__ == "__main__":
    # Create system
    security_system = create_enterprise_security_audit_system()
    
    # Sample logs for testing
    sample_logs = [
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.WARNING,
            message="Login failed for user admin from IP 192.168.1.100",
            source=LogSource.SECURITY,
            service="auth-service",
            environment="production",
            user_id="admin",
            ip_address="192.168.1.100"
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="User alice accessed personal data for patient P12345",
            source=LogSource.APPLICATION,
            service="health-app",
            environment="production",
            user_id="alice",
            tags={"data_type": "phi", "patient_id": "P12345"}
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            message="Potential SQL injection detected: SELECT * FROM users WHERE id=1 OR 1=1",
            source=LogSource.API,
            service="web-api",
            environment="production",
            ip_address="203.0.113.15"
        )
    ]
    
    # Process logs
    result = security_system.process_logs(sample_logs)
    
    # Print results
    print("Security and Audit Processing Results:")
    print(f"Logs processed: {result['logs_processed']}")
    print(f"Security events detected: {len(result['security_events'])}")
    print(f"Compliance violations: {result['compliance_report']['violation_count']}")
    print(f"Audit trail created: {result['audit_trail_id']}")
    
    if result["security_events"]:
        print("\nSecurity Events:")
        for event in result["security_events"]:
            print(f"  - {event['event_type']}: {event['description']} (Risk: {event['risk_level']})")
    
    if result["compliance_report"]["violations"]:
        print("\nCompliance Violations:")
        for violation in result["compliance_report"]["violations"]:
            print(f"  - {violation['framework']}: {violation['requirement']}")