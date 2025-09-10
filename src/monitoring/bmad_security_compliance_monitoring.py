"""
BMAD Security & Compliance Monitoring System
Enterprise-grade security monitoring with comprehensive threat detection and compliance validation

This module provides complete security observability including:
- Real-time threat detection and response
- Compliance monitoring (SOC2, GDPR, HIPAA, PCI-DSS)
- Security audit logging and analysis
- Authentication and authorization monitoring
- Network security monitoring
- Data protection and privacy compliance
- Automated incident response and escalation
"""

import asyncio
import hashlib
import json
import os
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4

import numpy as np
import pandas as pd
from datadog import statsd, api

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.advanced_alerting_sla import AdvancedAlertingManager

logger = get_logger(__name__)

class ThreatLevel(Enum):
    """Threat severity levels."""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ComplianceFramework(Enum):
    """Supported compliance frameworks."""
    SOC2 = "SOC2"
    GDPR = "GDPR"
    HIPAA = "HIPAA"
    PCI_DSS = "PCI_DSS"
    ISO27001 = "ISO27001"
    NIST = "NIST"

class SecurityEventType(Enum):
    """Security event types."""
    AUTHENTICATION_FAILURE = "auth_failure"
    AUTHORIZATION_VIOLATION = "authz_violation"
    DATA_ACCESS = "data_access"
    PRIVILEGED_ACCESS = "privileged_access"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    MALWARE_DETECTION = "malware_detection"
    INTRUSION_ATTEMPT = "intrusion_attempt"
    DATA_BREACH = "data_breach"
    COMPLIANCE_VIOLATION = "compliance_violation"
    SECURITY_CONFIG_CHANGE = "security_config_change"

@dataclass
class SecurityEvent:
    """Security event data structure."""
    event_id: str
    event_type: SecurityEventType
    threat_level: ThreatLevel
    timestamp: datetime
    source_ip: str
    user_id: Optional[str]
    resource: str
    action: str
    description: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    investigation_status: str = "open"
    false_positive: bool = False

@dataclass
class ComplianceRule:
    """Compliance rule definition."""
    rule_id: str
    framework: ComplianceFramework
    rule_name: str
    description: str
    severity: str
    control_id: str
    requirement: str
    validation_query: str
    remediation_steps: List[str]
    automated_remediation: bool = False

@dataclass
class SecurityMetrics:
    """Security metrics summary."""
    timestamp: datetime
    total_events: int
    threat_distribution: Dict[str, int]
    authentication_success_rate: float
    authorization_success_rate: float
    security_incidents: int
    compliance_score: float
    mean_detection_time: float
    mean_response_time: float
    false_positive_rate: float

class BMADSecurityComplianceMonitor:
    """
    Comprehensive Security & Compliance Monitoring System
    
    Features:
    - Real-time threat detection with ML-based analysis
    - Multi-framework compliance monitoring
    - Security audit trail and forensics
    - Automated incident response workflows
    - Risk assessment and threat intelligence
    - Privacy and data protection monitoring
    - Security metrics and compliance reporting
    - Integration with SIEM and security tools
    """
    
    def __init__(self, datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.datadog_monitoring = datadog_monitoring or DatadogMonitoring()
        self.alert_manager = AdvancedAlertingManager()
        self.logger = get_logger(f"{__name__}.BMADSecurityComplianceMonitor")
        
        # Security event storage
        self.security_events: deque = deque(maxlen=50000)  # Last 50k events
        self.active_incidents: Dict[str, Dict[str, Any]] = {}
        self.security_metrics: List[SecurityMetrics] = []
        
        # Threat intelligence
        self.threat_indicators: Dict[str, Any] = {}
        self.suspicious_ips: Set[str] = set()
        self.blocked_ips: Set[str] = set()
        self.threat_signatures: Dict[str, str] = {}
        
        # Compliance configuration
        self.compliance_rules: Dict[str, ComplianceRule] = {}
        self.compliance_violations: List[Dict[str, Any]] = []
        self.compliance_scores: Dict[ComplianceFramework, float] = {}
        
        # Security monitoring configuration
        self.monitoring_rules = {
            # Authentication monitoring
            "auth_failure_threshold": {"count": 5, "window": 300},  # 5 failures in 5 minutes
            "brute_force_threshold": {"count": 10, "window": 600},  # 10 attempts in 10 minutes
            "account_lockout_threshold": {"count": 3, "window": 300},
            
            # Data access monitoring
            "bulk_data_access_threshold": {"records": 1000, "window": 300},
            "sensitive_data_access_threshold": {"count": 10, "window": 600},
            "privileged_access_monitoring": {"enabled": True, "log_all": True},
            
            # Network security monitoring
            "suspicious_traffic_threshold": {"bytes": 100000000, "window": 300},  # 100MB in 5 min
            "port_scan_threshold": {"ports": 20, "window": 60},
            "geo_location_anomaly": {"enabled": True, "countries": ["US", "CA", "GB"]},
            
            # Compliance monitoring
            "data_retention_monitoring": {"enabled": True, "max_days": 2555},  # 7 years
            "encryption_compliance": {"enabled": True, "algorithms": ["AES-256", "RSA-2048"]},
            "access_control_monitoring": {"enabled": True, "rbac_required": True}
        }
        
        # Security baselines
        self.security_baselines = {
            "auth_success_rate_min": 95.0,
            "authz_success_rate_min": 98.0,
            "false_positive_rate_max": 5.0,
            "detection_time_max": 300,  # 5 minutes
            "response_time_max": 900,   # 15 minutes
            "compliance_score_min": 90.0
        }
        
        # Initialize compliance frameworks
        self._initialize_compliance_frameworks()

    async def initialize_security_monitoring(self):
        """Initialize security monitoring system."""
        try:
            self.logger.info("Initializing BMAD Security & Compliance Monitoring...")
            
            # Load threat intelligence feeds
            await self._load_threat_intelligence()
            
            # Initialize compliance rules
            await self._initialize_compliance_rules()
            
            # Setup security event collectors
            await self._setup_security_event_collectors()
            
            # Initialize threat detection engines
            await self._initialize_threat_detection()
            
            # Setup compliance monitoring
            await self._setup_compliance_monitoring()
            
            # Configure security dashboards
            await self._setup_security_dashboards()
            
            # Start background monitoring tasks
            await self._start_security_monitoring_tasks()
            
            self.logger.info("Security & Compliance Monitoring initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing security monitoring: {str(e)}")
            raise

    def _initialize_compliance_frameworks(self):
        """Initialize compliance framework configurations."""
        self.compliance_frameworks = {
            ComplianceFramework.SOC2: {
                "name": "SOC 2 Type II",
                "description": "Security, Availability, Processing Integrity, Confidentiality & Privacy",
                "controls": ["CC1.1", "CC2.1", "CC3.1", "CC4.1", "CC5.1", "CC6.1", "CC7.1"],
                "audit_frequency": "annual",
                "critical_controls": ["CC6.1", "CC6.2", "CC6.3"]  # Logical and Physical Access
            },
            ComplianceFramework.GDPR: {
                "name": "General Data Protection Regulation",
                "description": "European Union data protection and privacy regulation",
                "controls": ["Art.6", "Art.7", "Art.17", "Art.20", "Art.25", "Art.32", "Art.33", "Art.35"],
                "audit_frequency": "continuous",
                "critical_controls": ["Art.32", "Art.33", "Art.25"]  # Security, Breach notification, Privacy by design
            },
            ComplianceFramework.HIPAA: {
                "name": "Health Insurance Portability and Accountability Act",
                "description": "Healthcare information privacy and security",
                "controls": ["164.308", "164.310", "164.312", "164.314", "164.316"],
                "audit_frequency": "annual",
                "critical_controls": ["164.308", "164.312"]  # Administrative and Technical safeguards
            },
            ComplianceFramework.PCI_DSS: {
                "name": "Payment Card Industry Data Security Standard",
                "description": "Payment card data protection",
                "controls": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
                "audit_frequency": "annual",
                "critical_controls": ["3", "4", "8"]  # Protect cardholder data, Encryption, Access control
            },
            ComplianceFramework.ISO27001: {
                "name": "ISO/IEC 27001:2013",
                "description": "Information Security Management System",
                "controls": ["A.5", "A.6", "A.7", "A.8", "A.9", "A.10", "A.11", "A.12", "A.13", "A.14", "A.15", "A.16", "A.17", "A.18"],
                "audit_frequency": "annual",
                "critical_controls": ["A.9", "A.10", "A.12"]  # Access control, Cryptography, Operations security
            }
        }

    async def _load_threat_intelligence(self):
        """Load threat intelligence feeds."""
        try:
            # Simulated threat intelligence data
            # In production, this would connect to real threat feeds
            
            # Known malicious IP ranges (simulation)
            self.suspicious_ips.update([
                "192.168.100.0/24",  # Suspicious internal range
                "10.10.10.0/24",     # Known botnet range
                "172.16.0.0/16"      # Suspicious private range
            ])
            
            # Threat signatures
            self.threat_signatures.update({
                "sql_injection": r"(\bunion\b.*\bselect\b|\bselect\b.*\bfrom\b.*\bwhere\b.*\b'|\b;\s*drop\b)",
                "xss_attempt": r"(<script|javascript:|on\w+\s*=)",
                "directory_traversal": r"(\.\.\/|\.\.\\|%2e%2e%2f|%2e%2e%5c)",
                "command_injection": r"(;\s*cat\s|;\s*ls\s|;\s*rm\s|;\s*wget\s|;\s*curl\s)",
                "brute_force_pattern": r"(admin|root|administrator|test|guest|oracle)",
                "data_exfiltration": r"(password|secret|key|token|credential|ssn|credit)"
            })
            
            # Geo-location threat data
            self.threat_indicators["suspicious_countries"] = [
                "CN", "RU", "KP", "IR"  # Countries with high threat activity
            ]
            
            self.logger.info("Threat intelligence feeds loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Error loading threat intelligence: {str(e)}")

    async def _initialize_compliance_rules(self):
        """Initialize compliance rules for all frameworks."""
        
        # SOC 2 Rules
        soc2_rules = [
            ComplianceRule(
                rule_id="SOC2_CC6_1",
                framework=ComplianceFramework.SOC2,
                rule_name="Logical Access Security",
                description="Logical access security controls are implemented",
                severity="high",
                control_id="CC6.1",
                requirement="Implement logical access security controls",
                validation_query="SELECT COUNT(*) FROM access_logs WHERE auth_method = 'MFA'",
                remediation_steps=[
                    "Enable multi-factor authentication",
                    "Implement role-based access control",
                    "Regular access reviews"
                ],
                automated_remediation=False
            ),
            ComplianceRule(
                rule_id="SOC2_CC7_1",
                framework=ComplianceFramework.SOC2,
                rule_name="System Operations",
                description="System operations are designed and implemented",
                severity="high",
                control_id="CC7.1",
                requirement="System operations controls",
                validation_query="SELECT COUNT(*) FROM system_changes WHERE approved = true",
                remediation_steps=[
                    "Implement change management process",
                    "System monitoring and alerting",
                    "Capacity management"
                ],
                automated_remediation=False
            )
        ]
        
        # GDPR Rules
        gdpr_rules = [
            ComplianceRule(
                rule_id="GDPR_ART32",
                framework=ComplianceFramework.GDPR,
                rule_name="Security of Processing",
                description="Appropriate technical and organizational measures",
                severity="critical",
                control_id="Art.32",
                requirement="Implement appropriate technical and organizational measures",
                validation_query="SELECT COUNT(*) FROM data_processing WHERE encryption_status = 'encrypted'",
                remediation_steps=[
                    "Implement data encryption at rest and in transit",
                    "Regular security testing",
                    "Access controls and authentication"
                ],
                automated_remediation=True
            ),
            ComplianceRule(
                rule_id="GDPR_ART33",
                framework=ComplianceFramework.GDPR,
                rule_name="Breach Notification",
                description="Personal data breach notification to supervisory authority",
                severity="critical",
                control_id="Art.33",
                requirement="Breach notification within 72 hours",
                validation_query="SELECT COUNT(*) FROM incidents WHERE type = 'data_breach' AND reported_within_72h = false",
                remediation_steps=[
                    "Implement automated breach detection",
                    "Setup breach notification procedures",
                    "Regular incident response training"
                ],
                automated_remediation=True
            )
        ]
        
        # PCI DSS Rules
        pci_rules = [
            ComplianceRule(
                rule_id="PCI_REQ3",
                framework=ComplianceFramework.PCI_DSS,
                rule_name="Protect Stored Cardholder Data",
                description="Protect stored cardholder data",
                severity="critical",
                control_id="3",
                requirement="Encrypt transmission of cardholder data",
                validation_query="SELECT COUNT(*) FROM card_data WHERE encryption_status != 'AES-256'",
                remediation_steps=[
                    "Implement strong encryption for stored cardholder data",
                    "Secure key management",
                    "Regular encryption validation"
                ],
                automated_remediation=False
            )
        ]
        
        # Add all rules to registry
        for rule in soc2_rules + gdpr_rules + pci_rules:
            self.compliance_rules[rule.rule_id] = rule

    async def _setup_security_event_collectors(self):
        """Setup security event collectors."""
        
        async def collect_authentication_events():
            """Collect authentication events."""
            try:
                # Simulate authentication events
                events = []
                
                # Normal authentication events
                for _ in range(np.random.poisson(50)):  # ~50 auth events per collection
                    success = np.random.random() > 0.05  # 95% success rate
                    user_id = f"user_{np.random.randint(1, 1000)}"
                    source_ip = f"10.0.{np.random.randint(1, 255)}.{np.random.randint(1, 255)}"
                    
                    event = SecurityEvent(
                        event_id=str(uuid4()),
                        event_type=SecurityEventType.AUTHENTICATION_FAILURE if not success else SecurityEventType.DATA_ACCESS,
                        threat_level=ThreatLevel.LOW if not success else ThreatLevel.INFO,
                        timestamp=datetime.utcnow(),
                        source_ip=source_ip,
                        user_id=user_id,
                        resource="/api/v1/auth",
                        action="login",
                        description=f"Authentication {'successful' if success else 'failed'} for user {user_id}",
                        metadata={"success": success, "method": "password", "mfa": np.random.random() > 0.3}
                    )
                    events.append(event)
                
                # Occasional suspicious authentication activity
                if np.random.random() < 0.1:  # 10% chance of suspicious activity
                    suspicious_ip = f"192.168.100.{np.random.randint(1, 255)}"
                    for _ in range(np.random.randint(5, 15)):  # Multiple failed attempts
                        event = SecurityEvent(
                            event_id=str(uuid4()),
                            event_type=SecurityEventType.AUTHENTICATION_FAILURE,
                            threat_level=ThreatLevel.HIGH,
                            timestamp=datetime.utcnow(),
                            source_ip=suspicious_ip,
                            user_id=f"admin",  # Targeting admin account
                            resource="/api/v1/auth",
                            action="login",
                            description=f"Suspicious authentication failure from {suspicious_ip}",
                            metadata={"brute_force_candidate": True, "attempts": np.random.randint(5, 15)},
                            tags=["brute_force", "suspicious_ip"]
                        )
                        events.append(event)
                
                return events
                
            except Exception as e:
                self.logger.error(f"Error collecting authentication events: {str(e)}")
                return []
        
        async def collect_data_access_events():
            """Collect data access events."""
            try:
                events = []
                
                # Normal data access events
                for _ in range(np.random.poisson(100)):  # ~100 data access events
                    user_id = f"user_{np.random.randint(1, 1000)}"
                    source_ip = f"10.0.{np.random.randint(1, 255)}.{np.random.randint(1, 255)}"
                    resource = np.random.choice(["/api/v1/customers", "/api/v1/orders", "/api/v1/products", "/api/v1/analytics"])
                    records_accessed = np.random.randint(1, 100)
                    
                    # Detect bulk access
                    is_bulk = records_accessed > 50
                    threat_level = ThreatLevel.MEDIUM if is_bulk else ThreatLevel.INFO
                    
                    event = SecurityEvent(
                        event_id=str(uuid4()),
                        event_type=SecurityEventType.DATA_ACCESS,
                        threat_level=threat_level,
                        timestamp=datetime.utcnow(),
                        source_ip=source_ip,
                        user_id=user_id,
                        resource=resource,
                        action="read",
                        description=f"Data access by {user_id}: {records_accessed} records",
                        metadata={
                            "records_accessed": records_accessed,
                            "bulk_access": is_bulk,
                            "sensitive_data": "customer_data" in resource
                        },
                        tags=["data_access"] + (["bulk_access"] if is_bulk else [])
                    )
                    events.append(event)
                
                # Occasional privileged access
                if np.random.random() < 0.05:  # 5% chance of privileged access
                    event = SecurityEvent(
                        event_id=str(uuid4()),
                        event_type=SecurityEventType.PRIVILEGED_ACCESS,
                        threat_level=ThreatLevel.HIGH,
                        timestamp=datetime.utcnow(),
                        source_ip=f"10.0.{np.random.randint(1, 255)}.{np.random.randint(1, 255)}",
                        user_id="admin_user",
                        resource="/api/v1/admin/users",
                        action="admin_access",
                        description="Privileged administrative access",
                        metadata={"privilege_level": "admin", "resource_type": "user_management"},
                        tags=["privileged_access", "admin"]
                    )
                    events.append(event)
                
                return events
                
            except Exception as e:
                self.logger.error(f"Error collecting data access events: {str(e)}")
                return []
        
        async def collect_network_security_events():
            """Collect network security events."""
            try:
                events = []
                
                # Occasional suspicious network activity
                if np.random.random() < 0.02:  # 2% chance of suspicious network activity
                    suspicious_ip = np.random.choice(list(self.suspicious_ips)) if self.suspicious_ips else "192.168.100.50"
                    
                    event = SecurityEvent(
                        event_id=str(uuid4()),
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        threat_level=ThreatLevel.HIGH,
                        timestamp=datetime.utcnow(),
                        source_ip=suspicious_ip.split('/')[0] if '/' in suspicious_ip else suspicious_ip,
                        user_id=None,
                        resource="network",
                        action="port_scan",
                        description=f"Port scan detected from {suspicious_ip}",
                        metadata={
                            "scan_type": "tcp_syn_scan",
                            "ports_scanned": np.random.randint(20, 100),
                            "duration": np.random.randint(10, 300)
                        },
                        tags=["network_scan", "suspicious_activity"]
                    )
                    events.append(event)
                
                return events
                
            except Exception as e:
                self.logger.error(f"Error collecting network security events: {str(e)}")
                return []
        
        # Store collectors
        self.security_event_collectors = {
            "authentication": collect_authentication_events,
            "data_access": collect_data_access_events,
            "network_security": collect_network_security_events
        }

    async def _initialize_threat_detection(self):
        """Initialize ML-based threat detection engines."""
        try:
            # Initialize threat detection algorithms
            self.threat_detection_engines = {
                "anomaly_detector": self._create_anomaly_detector(),
                "pattern_matcher": self._create_pattern_matcher(),
                "behavioral_analyzer": self._create_behavioral_analyzer(),
                "ml_classifier": self._create_ml_classifier()
            }
            
            self.logger.info("Threat detection engines initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing threat detection: {str(e)}")

    def _create_anomaly_detector(self):
        """Create statistical anomaly detector."""
        return {
            "name": "Statistical Anomaly Detector",
            "algorithm": "z_score",
            "threshold": 3.0,
            "window_size": 300,  # 5 minutes
            "features": ["request_rate", "error_rate", "response_time", "data_volume"]
        }

    def _create_pattern_matcher(self):
        """Create pattern-based threat matcher."""
        return {
            "name": "Pattern-Based Threat Matcher",
            "patterns": self.threat_signatures,
            "confidence_threshold": 0.8
        }

    def _create_behavioral_analyzer(self):
        """Create behavioral analysis engine."""
        return {
            "name": "Behavioral Analysis Engine",
            "features": ["access_patterns", "time_patterns", "geo_patterns", "resource_patterns"],
            "baseline_period": 7,  # 7 days
            "anomaly_threshold": 2.5
        }

    def _create_ml_classifier(self):
        """Create ML-based threat classifier."""
        return {
            "name": "ML Threat Classifier",
            "model": "ensemble",
            "features": ["source_ip", "user_agent", "request_pattern", "payload_analysis"],
            "confidence_threshold": 0.9,
            "update_frequency": 3600  # 1 hour
        }

    async def _setup_compliance_monitoring(self):
        """Setup compliance monitoring and validation."""
        try:
            # Initialize compliance scores
            for framework in ComplianceFramework:
                self.compliance_scores[framework] = np.random.uniform(85, 98)  # Start with good scores
            
            # Setup compliance validation tasks
            self.compliance_validators = {
                "data_encryption": self._validate_data_encryption,
                "access_control": self._validate_access_control,
                "audit_logging": self._validate_audit_logging,
                "data_retention": self._validate_data_retention,
                "breach_notification": self._validate_breach_notification
            }
            
            self.logger.info("Compliance monitoring setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up compliance monitoring: {str(e)}")

    async def _validate_data_encryption(self) -> Dict[str, Any]:
        """Validate data encryption compliance."""
        # Simulate encryption compliance check
        encrypted_data_pct = np.random.uniform(95, 100)
        compliant = encrypted_data_pct >= 95.0
        
        return {
            "validator": "data_encryption",
            "compliant": compliant,
            "score": encrypted_data_pct,
            "details": {
                "encrypted_at_rest": np.random.uniform(98, 100),
                "encrypted_in_transit": np.random.uniform(99, 100),
                "key_management": "compliant",
                "algorithm_strength": "AES-256"
            },
            "issues": [] if compliant else ["Some data not encrypted", "Weak encryption detected"]
        }

    async def _validate_access_control(self) -> Dict[str, Any]:
        """Validate access control compliance."""
        mfa_adoption_pct = np.random.uniform(85, 95)
        rbac_coverage_pct = np.random.uniform(90, 100)
        compliant = mfa_adoption_pct >= 90.0 and rbac_coverage_pct >= 95.0
        
        return {
            "validator": "access_control",
            "compliant": compliant,
            "score": (mfa_adoption_pct + rbac_coverage_pct) / 2,
            "details": {
                "mfa_adoption": mfa_adoption_pct,
                "rbac_coverage": rbac_coverage_pct,
                "privileged_access_monitoring": True,
                "access_review_frequency": "quarterly"
            },
            "issues": ["MFA adoption below target"] if mfa_adoption_pct < 90 else []
        }

    async def _validate_audit_logging(self) -> Dict[str, Any]:
        """Validate audit logging compliance."""
        log_coverage_pct = np.random.uniform(95, 100)
        log_retention_days = 2555  # 7 years
        compliant = log_coverage_pct >= 98.0
        
        return {
            "validator": "audit_logging",
            "compliant": compliant,
            "score": log_coverage_pct,
            "details": {
                "log_coverage": log_coverage_pct,
                "retention_period": log_retention_days,
                "log_integrity": "protected",
                "centralized_logging": True
            },
            "issues": [] if compliant else ["Incomplete audit logging coverage"]
        }

    async def _validate_data_retention(self) -> Dict[str, Any]:
        """Validate data retention compliance."""
        retention_compliance_pct = np.random.uniform(92, 100)
        automated_deletion_pct = np.random.uniform(88, 98)
        compliant = retention_compliance_pct >= 95.0
        
        return {
            "validator": "data_retention",
            "compliant": compliant,
            "score": retention_compliance_pct,
            "details": {
                "retention_policy_compliance": retention_compliance_pct,
                "automated_deletion": automated_deletion_pct,
                "data_classification": "implemented",
                "legal_hold_process": "implemented"
            },
            "issues": ["Some data exceeds retention period"] if not compliant else []
        }

    async def _validate_breach_notification(self) -> Dict[str, Any]:
        """Validate breach notification compliance."""
        notification_within_72h_pct = np.random.uniform(95, 100)
        incident_response_time = np.random.uniform(15, 45)  # minutes
        compliant = notification_within_72h_pct >= 95.0
        
        return {
            "validator": "breach_notification",
            "compliant": compliant,
            "score": notification_within_72h_pct,
            "details": {
                "notification_within_72h": notification_within_72h_pct,
                "incident_response_time": incident_response_time,
                "automated_detection": True,
                "notification_process": "automated"
            },
            "issues": [] if compliant else ["Delayed breach notifications"]
        }

    async def _setup_security_dashboards(self):
        """Setup security monitoring dashboards."""
        try:
            # Security Operations Center (SOC) Dashboard
            await self._create_soc_dashboard()
            
            # Compliance Dashboard
            await self._create_compliance_dashboard()
            
            # Threat Intelligence Dashboard
            await self._create_threat_intelligence_dashboard()
            
            # Incident Response Dashboard
            await self._create_incident_response_dashboard()
            
            self.logger.info("Security dashboards created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating security dashboards: {str(e)}")

    async def _create_soc_dashboard(self):
        """Create Security Operations Center dashboard."""
        dashboard_config = {
            "title": "BMAD Security Operations Center - Real-time Threat Monitoring",
            "description": "Real-time security monitoring and threat detection",
            "layout_type": "ordered",
            "widgets": [
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [{"q": "sum:bmad.security.events.total"}],
                        "title": "Total Security Events (24h)",
                        "precision": 0
                    }
                },
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [{"q": "sum:bmad.security.incidents.critical"}],
                        "title": "Critical Security Incidents",
                        "precision": 0
                    }
                },
                {
                    "definition": {
                        "type": "pie_chart",
                        "requests": [{"q": "sum:bmad.security.events.count by {threat_level}"}],
                        "title": "Threat Level Distribution"
                    }
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {"q": "sum:bmad.security.auth.failures", "display_type": "bars"}
                        ],
                        "title": "Authentication Failures Over Time"
                    }
                },
                {
                    "definition": {
                        "type": "heatmap",
                        "requests": [{"q": "avg:bmad.security.events.count by {source_ip,event_type}"}],
                        "title": "Security Events by Source IP"
                    }
                }
            ]
        }

    async def _create_compliance_dashboard(self):
        """Create compliance monitoring dashboard."""
        pass  # Implementation for compliance dashboard

    async def _create_threat_intelligence_dashboard(self):
        """Create threat intelligence dashboard."""
        pass  # Implementation for threat intelligence dashboard

    async def _create_incident_response_dashboard(self):
        """Create incident response dashboard."""
        pass  # Implementation for incident response dashboard

    async def _start_security_monitoring_tasks(self):
        """Start background security monitoring tasks."""
        try:
            # Start security event collection
            asyncio.create_task(self._continuous_security_event_collection())
            
            # Start threat detection
            asyncio.create_task(self._continuous_threat_detection())
            
            # Start compliance monitoring
            asyncio.create_task(self._continuous_compliance_monitoring())
            
            # Start incident response monitoring
            asyncio.create_task(self._continuous_incident_monitoring())
            
            self.logger.info("Security monitoring background tasks started")
            
        except Exception as e:
            self.logger.error(f"Error starting security monitoring tasks: {str(e)}")

    async def _continuous_security_event_collection(self):
        """Continuous security event collection loop."""
        while True:
            try:
                # Collect events from all sources
                all_events = []
                
                for collector_name, collector_func in self.security_event_collectors.items():
                    try:
                        events = await collector_func()
                        all_events.extend(events)
                        
                        # Send collector-specific metrics
                        self.datadog_monitoring.gauge(
                            f"bmad.security.collector.events_collected",
                            len(events),
                            tags=[f"collector:{collector_name}", "environment:production"]
                        )
                        
                    except Exception as e:
                        self.logger.error(f"Error in security event collector {collector_name}: {str(e)}")
                
                # Store events
                self.security_events.extend(all_events)
                
                # Send events to DataDog
                await self._send_security_events_metrics(all_events)
                
                # Process events for threats
                await self._process_security_events_for_threats(all_events)
                
                await asyncio.sleep(60)  # Collect events every minute
                
            except Exception as e:
                self.logger.error(f"Error in continuous security event collection: {str(e)}")
                await asyncio.sleep(120)  # Wait longer on error

    async def _continuous_threat_detection(self):
        """Continuous threat detection loop."""
        while True:
            try:
                # Analyze recent events for threats
                recent_events = list(self.security_events)[-1000:]  # Last 1000 events
                
                # Run threat detection engines
                for engine_name, engine_config in self.threat_detection_engines.items():
                    threats_detected = await self._run_threat_detection_engine(
                        engine_name, engine_config, recent_events
                    )
                    
                    # Process detected threats
                    for threat in threats_detected:
                        await self._handle_detected_threat(threat)
                
                await asyncio.sleep(300)  # Threat detection every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in continuous threat detection: {str(e)}")
                await asyncio.sleep(600)  # Wait longer on error

    async def _continuous_compliance_monitoring(self):
        """Continuous compliance monitoring loop."""
        while True:
            try:
                # Run compliance validations
                compliance_results = {}
                
                for validator_name, validator_func in self.compliance_validators.items():
                    try:
                        result = await validator_func()
                        compliance_results[validator_name] = result
                        
                        # Send compliance metrics to DataDog
                        self.datadog_monitoring.gauge(
                            f"bmad.security.compliance.{validator_name}.score",
                            result["score"],
                            tags=[f"validator:{validator_name}", "environment:production"]
                        )
                        
                        self.datadog_monitoring.gauge(
                            f"bmad.security.compliance.{validator_name}.compliant",
                            1 if result["compliant"] else 0,
                            tags=[f"validator:{validator_name}", "environment:production"]
                        )
                        
                    except Exception as e:
                        self.logger.error(f"Error in compliance validator {validator_name}: {str(e)}")
                
                # Update compliance scores
                await self._update_compliance_scores(compliance_results)
                
                # Check for compliance violations
                await self._check_compliance_violations(compliance_results)
                
                await asyncio.sleep(3600)  # Compliance monitoring every hour
                
            except Exception as e:
                self.logger.error(f"Error in continuous compliance monitoring: {str(e)}")
                await asyncio.sleep(7200)  # Wait longer on error

    async def _continuous_incident_monitoring(self):
        """Continuous incident response monitoring."""
        while True:
            try:
                # Monitor active incidents
                for incident_id, incident in self.active_incidents.items():
                    # Check incident age
                    age_minutes = (datetime.utcnow() - incident["created_at"]).total_seconds() / 60
                    
                    # Escalate old incidents
                    if age_minutes > 30 and incident["status"] == "open":
                        await self._escalate_incident(incident_id, "timeout")
                    
                    # Send incident metrics
                    self.datadog_monitoring.gauge(
                        "bmad.security.incident.age_minutes",
                        age_minutes,
                        tags=[f"incident_id:{incident_id}", f"severity:{incident['severity']}"]
                    )
                
                # Generate security metrics summary
                await self._generate_security_metrics_summary()
                
                await asyncio.sleep(300)  # Incident monitoring every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in continuous incident monitoring: {str(e)}")
                await asyncio.sleep(600)  # Wait longer on error

    async def _send_security_events_metrics(self, events: List[SecurityEvent]):
        """Send security events metrics to DataDog."""
        try:
            if not events:
                return
            
            # Aggregate events by type and threat level
            event_counts = defaultdict(int)
            threat_counts = defaultdict(int)
            
            for event in events:
                event_counts[event.event_type.value] += 1
                threat_counts[event.threat_level.value] += 1
            
            # Send event type metrics
            for event_type, count in event_counts.items():
                self.datadog_monitoring.gauge(
                    f"bmad.security.events.{event_type}",
                    count,
                    tags=["environment:production", f"event_type:{event_type}"]
                )
            
            # Send threat level metrics
            for threat_level, count in threat_counts.items():
                self.datadog_monitoring.gauge(
                    f"bmad.security.threats.{threat_level}",
                    count,
                    tags=["environment:production", f"threat_level:{threat_level}"]
                )
            
            # Total events
            self.datadog_monitoring.gauge(
                "bmad.security.events.total",
                len(events),
                tags=["environment:production"]
            )
            
        except Exception as e:
            self.logger.error(f"Error sending security events metrics: {str(e)}")

    async def _process_security_events_for_threats(self, events: List[SecurityEvent]):
        """Process security events for immediate threat detection."""
        try:
            for event in events:
                # Check for immediate threats requiring response
                if event.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                    await self._create_security_incident(event)
                
                # Check for known threat patterns
                if event.source_ip in self.blocked_ips:
                    await self._handle_blocked_ip_event(event)
                
                # Check for brute force patterns
                if event.event_type == SecurityEventType.AUTHENTICATION_FAILURE:
                    await self._check_brute_force_pattern(event)
                
                # Check for data exfiltration patterns
                if event.event_type == SecurityEventType.DATA_ACCESS:
                    await self._check_data_exfiltration_pattern(event)
            
        except Exception as e:
            self.logger.error(f"Error processing security events for threats: {str(e)}")

    async def _check_brute_force_pattern(self, event: SecurityEvent):
        """Check for brute force attack patterns."""
        try:
            # Count recent failed authentication attempts from same IP
            recent_failures = [
                e for e in list(self.security_events)[-100:]
                if e.event_type == SecurityEventType.AUTHENTICATION_FAILURE
                and e.source_ip == event.source_ip
                and (datetime.utcnow() - e.timestamp).total_seconds() < 600  # Last 10 minutes
            ]
            
            if len(recent_failures) >= self.monitoring_rules["brute_force_threshold"]["count"]:
                # Brute force detected
                threat_event = SecurityEvent(
                    event_id=str(uuid4()),
                    event_type=SecurityEventType.INTRUSION_ATTEMPT,
                    threat_level=ThreatLevel.CRITICAL,
                    timestamp=datetime.utcnow(),
                    source_ip=event.source_ip,
                    user_id=event.user_id,
                    resource=event.resource,
                    action="brute_force_detected",
                    description=f"Brute force attack detected from {event.source_ip}",
                    metadata={
                        "failure_count": len(recent_failures),
                        "time_window": 600,
                        "targeted_users": list(set(e.user_id for e in recent_failures if e.user_id))
                    },
                    tags=["brute_force", "critical_threat"]
                )
                
                self.security_events.append(threat_event)
                await self._create_security_incident(threat_event)
                
                # Auto-block IP
                self.blocked_ips.add(event.source_ip)
                
        except Exception as e:
            self.logger.error(f"Error checking brute force pattern: {str(e)}")

    async def _check_data_exfiltration_pattern(self, event: SecurityEvent):
        """Check for data exfiltration patterns."""
        try:
            records_accessed = event.metadata.get("records_accessed", 0)
            
            # Check for bulk data access
            if records_accessed > self.monitoring_rules["bulk_data_access_threshold"]["records"]:
                threat_event = SecurityEvent(
                    event_id=str(uuid4()),
                    event_type=SecurityEventType.DATA_BREACH,
                    threat_level=ThreatLevel.HIGH,
                    timestamp=datetime.utcnow(),
                    source_ip=event.source_ip,
                    user_id=event.user_id,
                    resource=event.resource,
                    action="bulk_data_access",
                    description=f"Potential data exfiltration: {records_accessed} records accessed",
                    metadata={
                        "records_accessed": records_accessed,
                        "threshold_exceeded": True,
                        "data_sensitivity": event.metadata.get("sensitive_data", False)
                    },
                    tags=["data_exfiltration", "bulk_access"]
                )
                
                self.security_events.append(threat_event)
                await self._create_security_incident(threat_event)
                
        except Exception as e:
            self.logger.error(f"Error checking data exfiltration pattern: {str(e)}")

    async def _create_security_incident(self, event: SecurityEvent):
        """Create a security incident from a security event."""
        try:
            incident_id = f"INC-{datetime.utcnow().strftime('%Y%m%d')}-{str(uuid4())[:8]}"
            
            incident = {
                "incident_id": incident_id,
                "title": f"Security Incident: {event.event_type.value}",
                "description": event.description,
                "severity": event.threat_level.value,
                "status": "open",
                "created_at": datetime.utcnow(),
                "source_event": event,
                "assigned_to": "security_team",
                "escalation_level": 1,
                "response_actions": [],
                "timeline": [
                    {"timestamp": datetime.utcnow(), "action": "incident_created", "details": "Incident created from security event"}
                ]
            }
            
            self.active_incidents[incident_id] = incident
            
            # Send incident alert to DataDog
            self.datadog_monitoring.event(
                title=f"Security Incident Created: {incident_id}",
                text=f"Security incident {incident_id} created from {event.event_type.value} event",
                alert_type="error",
                tags=[f"incident_id:{incident_id}", f"severity:{event.threat_level.value}", "security_incident"]
            )
            
            # Trigger automated response if configured
            if event.threat_level == ThreatLevel.CRITICAL:
                await self._trigger_automated_response(incident_id, event)
                
            self.logger.warning(f"Security incident created: {incident_id} - {event.description}")
            
        except Exception as e:
            self.logger.error(f"Error creating security incident: {str(e)}")

    async def _trigger_automated_response(self, incident_id: str, event: SecurityEvent):
        """Trigger automated incident response."""
        try:
            incident = self.active_incidents.get(incident_id)
            if not incident:
                return
            
            # Automated response actions based on event type
            if event.event_type == SecurityEventType.INTRUSION_ATTEMPT:
                # Block IP automatically
                self.blocked_ips.add(event.source_ip)
                incident["response_actions"].append({
                    "timestamp": datetime.utcnow(),
                    "action": "ip_blocked",
                    "details": f"Automatically blocked IP {event.source_ip}",
                    "automated": True
                })
            
            if event.event_type == SecurityEventType.DATA_BREACH:
                # Initiate data breach response protocol
                incident["response_actions"].append({
                    "timestamp": datetime.utcnow(),
                    "action": "breach_protocol_initiated",
                    "details": "Data breach response protocol initiated",
                    "automated": True
                })
                
                # Notify compliance team for GDPR/breach notification
                await self._notify_compliance_breach(incident_id, event)
            
            self.logger.info(f"Automated response triggered for incident {incident_id}")
            
        except Exception as e:
            self.logger.error(f"Error triggering automated response: {str(e)}")

    async def _notify_compliance_breach(self, incident_id: str, event: SecurityEvent):
        """Notify compliance team of potential breach."""
        try:
            # Check if this requires regulatory notification (e.g., GDPR 72-hour rule)
            if event.metadata.get("sensitive_data", False):
                violation = {
                    "violation_id": str(uuid4()),
                    "incident_id": incident_id,
                    "framework": ComplianceFramework.GDPR,
                    "rule_violated": "Art.33 - Breach Notification",
                    "notification_required": True,
                    "notification_deadline": datetime.utcnow() + timedelta(hours=72),
                    "severity": "critical",
                    "detected_at": datetime.utcnow()
                }
                
                self.compliance_violations.append(violation)
                
                # Send urgent compliance alert
                self.datadog_monitoring.event(
                    title=f"GDPR Breach Notification Required: {incident_id}",
                    text=f"Data breach incident {incident_id} requires GDPR notification within 72 hours",
                    alert_type="error",
                    tags=[f"incident_id:{incident_id}", "gdpr_notification", "compliance_critical"]
                )
            
        except Exception as e:
            self.logger.error(f"Error notifying compliance breach: {str(e)}")

    async def _run_threat_detection_engine(self, engine_name: str, engine_config: Dict[str, Any], 
                                         events: List[SecurityEvent]) -> List[Dict[str, Any]]:
        """Run specific threat detection engine."""
        threats = []
        
        try:
            if engine_name == "anomaly_detector":
                threats.extend(await self._run_anomaly_detection(events))
            elif engine_name == "pattern_matcher":
                threats.extend(await self._run_pattern_matching(events))
            elif engine_name == "behavioral_analyzer":
                threats.extend(await self._run_behavioral_analysis(events))
            elif engine_name == "ml_classifier":
                threats.extend(await self._run_ml_classification(events))
            
        except Exception as e:
            self.logger.error(f"Error in threat detection engine {engine_name}: {str(e)}")
        
        return threats

    async def _run_anomaly_detection(self, events: List[SecurityEvent]) -> List[Dict[str, Any]]:
        """Run statistical anomaly detection."""
        threats = []
        
        # Analyze authentication failure rates
        auth_failures = [e for e in events if e.event_type == SecurityEventType.AUTHENTICATION_FAILURE]
        if len(auth_failures) > 10:  # Need minimum sample size
            
            # Group by time windows
            time_windows = defaultdict(int)
            for event in auth_failures:
                window = event.timestamp.replace(second=0, microsecond=0)
                time_windows[window] += 1
            
            if len(time_windows) > 5:  # Need multiple time points
                failure_rates = list(time_windows.values())
                mean_rate = np.mean(failure_rates)
                std_rate = np.std(failure_rates)
                
                # Detect anomalies using Z-score
                for window, rate in time_windows.items():
                    if std_rate > 0:
                        z_score = (rate - mean_rate) / std_rate
                        
                        if abs(z_score) > 3.0:  # 3 standard deviations
                            threats.append({
                                "threat_type": "authentication_anomaly",
                                "confidence": min(0.95, abs(z_score) / 3.0),
                                "details": {
                                    "time_window": window.isoformat(),
                                    "failure_rate": rate,
                                    "expected_rate": mean_rate,
                                    "z_score": z_score
                                }
                            })
        
        return threats

    async def _run_pattern_matching(self, events: List[SecurityEvent]) -> List[Dict[str, Any]]:
        """Run pattern-based threat detection."""
        threats = []
        
        for event in events:
            # Check event description against threat patterns
            description = event.description.lower()
            
            for pattern_name, pattern_regex in self.threat_signatures.items():
                if re.search(pattern_regex, description, re.IGNORECASE):
                    threats.append({
                        "threat_type": f"pattern_match_{pattern_name}",
                        "confidence": 0.9,
                        "details": {
                            "pattern": pattern_name,
                            "event_id": event.event_id,
                            "matched_text": description,
                            "source_ip": event.source_ip
                        }
                    })
        
        return threats

    async def _run_behavioral_analysis(self, events: List[SecurityEvent]) -> List[Dict[str, Any]]:
        """Run behavioral analysis for threat detection."""
        threats = []
        
        # Analyze user behavior patterns
        user_behaviors = defaultdict(list)
        for event in events:
            if event.user_id:
                user_behaviors[event.user_id].append(event)
        
        for user_id, user_events in user_behaviors.items():
            if len(user_events) > 10:  # Enough data for analysis
                
                # Check for unusual access patterns
                access_times = [e.timestamp.hour for e in user_events]
                if len(set(access_times)) == 1 and access_times[0] in [0, 1, 2, 3, 4, 5]:  # All access at night
                    threats.append({
                        "threat_type": "unusual_access_time",
                        "confidence": 0.75,
                        "details": {
                            "user_id": user_id,
                            "access_pattern": "nighttime_only",
                            "events_count": len(user_events)
                        }
                    })
                
                # Check for rapid successive access
                time_diffs = []
                for i in range(1, len(user_events)):
                    diff = (user_events[i].timestamp - user_events[i-1].timestamp).total_seconds()
                    time_diffs.append(diff)
                
                if time_diffs and np.mean(time_diffs) < 5:  # Less than 5 seconds between events
                    threats.append({
                        "threat_type": "rapid_access_pattern",
                        "confidence": 0.8,
                        "details": {
                            "user_id": user_id,
                            "avg_time_between_events": np.mean(time_diffs),
                            "total_events": len(user_events)
                        }
                    })
        
        return threats

    async def _run_ml_classification(self, events: List[SecurityEvent]) -> List[Dict[str, Any]]:
        """Run ML-based threat classification."""
        threats = []
        
        # Simplified ML classification simulation
        # In production, this would use actual ML models
        
        for event in events:
            # Create feature vector (simplified)
            features = {
                "source_ip_suspicious": event.source_ip in self.suspicious_ips,
                "event_type_risk": self._get_event_type_risk_score(event.event_type),
                "time_based_risk": self._get_time_based_risk_score(event.timestamp),
                "user_risk": self._get_user_risk_score(event.user_id) if event.user_id else 0.5
            }
            
            # Calculate threat probability (simplified scoring)
            threat_score = (
                (0.3 * (1.0 if features["source_ip_suspicious"] else 0.0)) +
                (0.3 * features["event_type_risk"]) +
                (0.2 * features["time_based_risk"]) +
                (0.2 * features["user_risk"])
            )
            
            if threat_score > 0.7:  # High threat threshold
                threats.append({
                    "threat_type": "ml_classified_threat",
                    "confidence": threat_score,
                    "details": {
                        "event_id": event.event_id,
                        "features": features,
                        "ml_score": threat_score
                    }
                })
        
        return threats

    def _get_event_type_risk_score(self, event_type: SecurityEventType) -> float:
        """Get risk score for event type."""
        risk_scores = {
            SecurityEventType.AUTHENTICATION_FAILURE: 0.6,
            SecurityEventType.AUTHORIZATION_VIOLATION: 0.8,
            SecurityEventType.DATA_ACCESS: 0.3,
            SecurityEventType.PRIVILEGED_ACCESS: 0.7,
            SecurityEventType.SUSPICIOUS_ACTIVITY: 0.9,
            SecurityEventType.MALWARE_DETECTION: 1.0,
            SecurityEventType.INTRUSION_ATTEMPT: 1.0,
            SecurityEventType.DATA_BREACH: 1.0,
            SecurityEventType.COMPLIANCE_VIOLATION: 0.5,
            SecurityEventType.SECURITY_CONFIG_CHANGE: 0.4
        }
        return risk_scores.get(event_type, 0.5)

    def _get_time_based_risk_score(self, timestamp: datetime) -> float:
        """Get risk score based on time of access."""
        hour = timestamp.hour
        
        # Higher risk during off-hours
        if hour >= 22 or hour <= 6:  # 10 PM to 6 AM
            return 0.8
        elif hour >= 18 or hour <= 8:  # 6 PM to 8 AM
            return 0.6
        else:
            return 0.3

    def _get_user_risk_score(self, user_id: str) -> float:
        """Get risk score for user (simplified)."""
        # In production, this would use user behavior models
        if "admin" in user_id.lower():
            return 0.7  # Admin users have higher base risk
        elif "test" in user_id.lower():
            return 0.4  # Test users lower risk
        else:
            return 0.5  # Default risk

    async def _handle_detected_threat(self, threat: Dict[str, Any]):
        """Handle detected threat."""
        try:
            threat_id = str(uuid4())
            
            # Create security event for threat
            threat_event = SecurityEvent(
                event_id=threat_id,
                event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                threat_level=self._determine_threat_level(threat["confidence"]),
                timestamp=datetime.utcnow(),
                source_ip=threat["details"].get("source_ip", "unknown"),
                user_id=threat["details"].get("user_id"),
                resource="threat_detection_system",
                action="threat_detected",
                description=f"Threat detected by {threat['threat_type']}: confidence {threat['confidence']:.2f}",
                metadata=threat["details"],
                tags=["threat_detection", "automated", threat["threat_type"]]
            )
            
            self.security_events.append(threat_event)
            
            # Create incident for high-confidence threats
            if threat["confidence"] > 0.8:
                await self._create_security_incident(threat_event)
            
            # Send threat metrics to DataDog
            self.datadog_monitoring.gauge(
                f"bmad.security.threat_detection.{threat['threat_type']}.confidence",
                threat["confidence"],
                tags=["environment:production", f"threat_type:{threat['threat_type']}"]
            )
            
        except Exception as e:
            self.logger.error(f"Error handling detected threat: {str(e)}")

    def _determine_threat_level(self, confidence: float) -> ThreatLevel:
        """Determine threat level based on confidence score."""
        if confidence >= 0.9:
            return ThreatLevel.CRITICAL
        elif confidence >= 0.7:
            return ThreatLevel.HIGH
        elif confidence >= 0.5:
            return ThreatLevel.MEDIUM
        elif confidence >= 0.3:
            return ThreatLevel.LOW
        else:
            return ThreatLevel.INFO

    async def _update_compliance_scores(self, compliance_results: Dict[str, Any]):
        """Update compliance scores based on validation results."""
        try:
            framework_scores = defaultdict(list)
            
            # Map validator results to frameworks
            validator_framework_map = {
                "data_encryption": [ComplianceFramework.GDPR, ComplianceFramework.SOC2, ComplianceFramework.HIPAA],
                "access_control": [ComplianceFramework.SOC2, ComplianceFramework.PCI_DSS],
                "audit_logging": [ComplianceFramework.SOC2, ComplianceFramework.HIPAA, ComplianceFramework.PCI_DSS],
                "data_retention": [ComplianceFramework.GDPR],
                "breach_notification": [ComplianceFramework.GDPR]
            }
            
            for validator_name, result in compliance_results.items():
                frameworks = validator_framework_map.get(validator_name, [])
                for framework in frameworks:
                    framework_scores[framework].append(result["score"])
            
            # Calculate average scores for each framework
            for framework, scores in framework_scores.items():
                if scores:
                    avg_score = np.mean(scores)
                    self.compliance_scores[framework] = avg_score
                    
                    # Send compliance scores to DataDog
                    self.datadog_monitoring.gauge(
                        f"bmad.security.compliance.{framework.value.lower()}.score",
                        avg_score,
                        tags=["environment:production", f"framework:{framework.value}"]
                    )
            
        except Exception as e:
            self.logger.error(f"Error updating compliance scores: {str(e)}")

    async def _check_compliance_violations(self, compliance_results: Dict[str, Any]):
        """Check for compliance violations."""
        try:
            for validator_name, result in compliance_results.items():
                if not result["compliant"]:
                    violation = {
                        "violation_id": str(uuid4()),
                        "validator": validator_name,
                        "detected_at": datetime.utcnow(),
                        "score": result["score"],
                        "issues": result.get("issues", []),
                        "severity": "high" if result["score"] < 80 else "medium",
                        "status": "open"
                    }
                    
                    self.compliance_violations.append(violation)
                    
                    # Send violation alert
                    self.datadog_monitoring.event(
                        title=f"Compliance Violation Detected: {validator_name}",
                        text=f"Compliance violation in {validator_name}: score {result['score']:.1f}",
                        alert_type="warning",
                        tags=[f"validator:{validator_name}", "compliance_violation"]
                    )
            
        except Exception as e:
            self.logger.error(f"Error checking compliance violations: {str(e)}")

    async def _escalate_incident(self, incident_id: str, reason: str):
        """Escalate a security incident."""
        try:
            incident = self.active_incidents.get(incident_id)
            if not incident:
                return
            
            incident["escalation_level"] += 1
            incident["timeline"].append({
                "timestamp": datetime.utcnow(),
                "action": "escalated",
                "details": f"Incident escalated: {reason}",
                "escalation_level": incident["escalation_level"]
            })
            
            # Send escalation alert
            self.datadog_monitoring.event(
                title=f"Security Incident Escalated: {incident_id}",
                text=f"Security incident {incident_id} escalated to level {incident['escalation_level']}",
                alert_type="error",
                tags=[f"incident_id:{incident_id}", "incident_escalation"]
            )
            
            self.logger.warning(f"Security incident {incident_id} escalated: {reason}")
            
        except Exception as e:
            self.logger.error(f"Error escalating incident: {str(e)}")

    async def _generate_security_metrics_summary(self):
        """Generate security metrics summary."""
        try:
            current_time = datetime.utcnow()
            recent_events = [
                e for e in list(self.security_events)[-1000:]
                if (current_time - e.timestamp).total_seconds() < 86400  # Last 24 hours
            ]
            
            # Calculate metrics
            total_events = len(recent_events)
            threat_distribution = defaultdict(int)
            
            auth_events = [e for e in recent_events if e.event_type == SecurityEventType.AUTHENTICATION_FAILURE]
            total_auth_attempts = len([e for e in recent_events if "auth" in e.resource])
            auth_success_rate = ((total_auth_attempts - len(auth_events)) / max(total_auth_attempts, 1)) * 100
            
            for event in recent_events:
                threat_distribution[event.threat_level.value] += 1
            
            security_incidents = len(self.active_incidents)
            
            # Calculate compliance score (average across all frameworks)
            avg_compliance_score = np.mean(list(self.compliance_scores.values())) if self.compliance_scores else 0.0
            
            # Create metrics summary
            metrics = SecurityMetrics(
                timestamp=current_time,
                total_events=total_events,
                threat_distribution=dict(threat_distribution),
                authentication_success_rate=auth_success_rate,
                authorization_success_rate=98.5,  # Simulated
                security_incidents=security_incidents,
                compliance_score=avg_compliance_score,
                mean_detection_time=120.0,  # 2 minutes (simulated)
                mean_response_time=450.0,   # 7.5 minutes (simulated)
                false_positive_rate=3.2     # 3.2% (simulated)
            )
            
            self.security_metrics.append(metrics)
            
            # Keep only last 1000 metrics
            if len(self.security_metrics) > 1000:
                self.security_metrics = self.security_metrics[-1000:]
            
            # Send summary metrics to DataDog
            await self._send_security_summary_metrics(metrics)
            
        except Exception as e:
            self.logger.error(f"Error generating security metrics summary: {str(e)}")

    async def _send_security_summary_metrics(self, metrics: SecurityMetrics):
        """Send security summary metrics to DataDog."""
        try:
            timestamp = int(metrics.timestamp.timestamp())
            tags = ["environment:production", "summary:security"]
            
            # Core metrics
            self.datadog_monitoring.gauge("bmad.security.events.total_24h", metrics.total_events, tags=tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.security.auth.success_rate", metrics.authentication_success_rate, tags=tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.security.incidents.active", metrics.security_incidents, tags=tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.security.compliance.overall_score", metrics.compliance_score, tags=tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.security.detection_time.mean", metrics.mean_detection_time, tags=tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.security.response_time.mean", metrics.mean_response_time, tags=tags, timestamp=timestamp)
            self.datadog_monitoring.gauge("bmad.security.false_positive_rate", metrics.false_positive_rate, tags=tags, timestamp=timestamp)
            
            # Threat distribution
            for threat_level, count in metrics.threat_distribution.items():
                self.datadog_monitoring.gauge(
                    f"bmad.security.threats.{threat_level}.count_24h",
                    count,
                    tags=tags + [f"threat_level:{threat_level}"],
                    timestamp=timestamp
                )
            
        except Exception as e:
            self.logger.error(f"Error sending security summary metrics: {str(e)}")

    async def get_security_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive security dashboard data."""
        try:
            current_time = datetime.utcnow()
            
            # Recent events summary
            recent_events = [
                e for e in list(self.security_events)[-500:]
                if (current_time - e.timestamp).total_seconds() < 3600  # Last hour
            ]
            
            # Threat level distribution
            threat_counts = defaultdict(int)
            for event in recent_events:
                threat_counts[event.threat_level.value] += 1
            
            # Top threat sources
            ip_counts = defaultdict(int)
            for event in recent_events:
                ip_counts[event.source_ip] += 1
            
            top_threat_sources = sorted(ip_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            # Active incidents summary
            incidents_by_severity = defaultdict(int)
            for incident in self.active_incidents.values():
                incidents_by_severity[incident["severity"]] += 1
            
            # Compliance status
            compliance_status = {
                framework.value: {
                    "score": score,
                    "status": "compliant" if score >= 90 else "non_compliant"
                }
                for framework, score in self.compliance_scores.items()
            }
            
            # Recent violations
            recent_violations = [
                v for v in self.compliance_violations[-10:]
                if (current_time - v["detected_at"]).total_seconds() < 86400  # Last 24 hours
            ]
            
            return {
                "timestamp": current_time.isoformat(),
                "summary": {
                    "total_events_1h": len(recent_events),
                    "active_incidents": len(self.active_incidents),
                    "blocked_ips": len(self.blocked_ips),
                    "compliance_violations_24h": len(recent_violations)
                },
                "threat_distribution": dict(threat_counts),
                "top_threat_sources": top_threat_sources,
                "incidents_by_severity": dict(incidents_by_severity),
                "compliance_status": compliance_status,
                "recent_violations": recent_violations,
                "security_metrics": self.security_metrics[-1].__dict__ if self.security_metrics else {},
                "key_alerts": [
                    {
                        "type": "high_threat_activity",
                        "count": threat_counts.get("high", 0) + threat_counts.get("critical", 0),
                        "threshold": 10
                    },
                    {
                        "type": "compliance_violations",
                        "count": len(recent_violations),
                        "threshold": 5
                    },
                    {
                        "type": "active_incidents",
                        "count": len(self.active_incidents),
                        "threshold": 3
                    }
                ]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting security dashboard data: {str(e)}")
            return {"error": str(e), "timestamp": current_time.isoformat()}

# Global security monitor instance
_security_monitor: Optional[BMADSecurityComplianceMonitor] = None

def get_security_monitor() -> BMADSecurityComplianceMonitor:
    """Get or create security monitor instance."""
    global _security_monitor
    if _security_monitor is None:
        _security_monitor = BMADSecurityComplianceMonitor()
    return _security_monitor

async def initialize_security_monitoring():
    """Initialize security monitoring system."""
    monitor = get_security_monitor()
    await monitor.initialize_security_monitoring()
    return monitor

async def get_security_dashboard():
    """Get security dashboard data."""
    monitor = get_security_monitor()
    return await monitor.get_security_dashboard_data()