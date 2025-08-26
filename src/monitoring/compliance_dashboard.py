"""
Compliance Monitoring Dashboard
Real-time compliance tracking, violation monitoring, and regulatory adherence
dashboard with support for GDPR, HIPAA, PCI-DSS, SOX, and other frameworks.
"""
import asyncio
import json
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
import threading
from urllib.parse import quote

from fastapi import WebSocket

from core.logging import get_logger
from core.security.compliance_framework import ComplianceFramework
from monitoring.security_metrics import SecurityMetricsCollector, get_security_metrics_collector
from monitoring.security_observability import get_security_observability, SecurityTraceType


logger = get_logger(__name__)


class ComplianceStatus(Enum):
    """Compliance status levels"""
    COMPLIANT = "compliant"
    WARNING = "warning"
    VIOLATION = "violation"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class ComplianceMetricType(Enum):
    """Types of compliance metrics"""
    ADHERENCE_SCORE = "adherence_score"
    VIOLATION_COUNT = "violation_count"
    REMEDIATION_TIME = "remediation_time"
    AUDIT_READINESS = "audit_readiness"
    DATA_GOVERNANCE = "data_governance"
    PRIVACY_IMPACT = "privacy_impact"


@dataclass
class ComplianceViolation:
    """Compliance violation record"""
    violation_id: str
    framework: ComplianceFramework
    violation_type: str
    severity: str
    description: str
    resource_id: str
    user_id: Optional[str]
    detected_at: datetime
    status: ComplianceStatus
    remediation_deadline: Optional[datetime]
    remediation_notes: Optional[str] = None
    resolved_at: Optional[datetime] = None
    assigned_to: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComplianceFrameworkStatus:
    """Status for a specific compliance framework"""
    framework: ComplianceFramework
    overall_score: float
    status: ComplianceStatus
    total_checks: int
    passed_checks: int
    failed_checks: int
    active_violations: int
    last_assessment: datetime
    next_assessment: Optional[datetime] = None
    requirements_met: int = 0
    total_requirements: int = 0
    critical_issues: List[str] = field(default_factory=list)


@dataclass
class PrivacyImpactAssessment:
    """Privacy impact assessment for data processing"""
    assessment_id: str
    data_processing_activity: str
    data_types: List[str]
    legal_basis: str
    data_subjects: List[str]
    retention_period: int  # days
    privacy_risk_score: float
    mitigation_measures: List[str]
    created_at: datetime
    reviewed_at: Optional[datetime] = None
    approved_by: Optional[str] = None
    status: str = "pending"


@dataclass
class DataGovernanceMetrics:
    """Data governance metrics"""
    total_data_assets: int = 0
    cataloged_assets: int = 0
    classified_assets: int = 0
    lineage_tracked_assets: int = 0
    quality_monitored_assets: int = 0
    access_controlled_assets: int = 0
    retention_policy_applied: int = 0
    governance_score: float = 0.0
    last_updated: datetime = field(default_factory=datetime.now)


class ComplianceDashboard:
    """Real-time compliance monitoring dashboard"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.security_metrics = get_security_metrics_collector()
        self.security_observability = get_security_observability()
        
        # Compliance data storage
        self.violations: List[ComplianceViolation] = []
        self.framework_status: Dict[ComplianceFramework, ComplianceFrameworkStatus] = {}
        self.privacy_assessments: List[PrivacyImpactAssessment] = []
        self.governance_metrics = DataGovernanceMetrics()
        
        # Real-time metrics
        self.compliance_scores: deque = deque(maxlen=1000)  # Historical scores
        self.violation_trends: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.remediation_metrics: Dict[str, List[float]] = defaultdict(list)
        
        # WebSocket connections for real-time updates
        self.active_connections: Set[WebSocket] = set()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._assessment_task: Optional[asyncio.Task] = None
        
        # Initialize framework statuses
        self._initialize_framework_status()
        
        self.logger.info("Compliance dashboard initialized")
    
    def _initialize_framework_status(self):
        """Initialize compliance framework statuses"""
        frameworks = [
            ComplianceFramework.GDPR,
            ComplianceFramework.HIPAA,
            ComplianceFramework.PCI_DSS,
            ComplianceFramework.SOX,
            ComplianceFramework.CCPA,
            ComplianceFramework.ISO_27001,
            ComplianceFramework.NIST
        ]
        
        for framework in frameworks:
            self.framework_status[framework] = ComplianceFrameworkStatus(
                framework=framework,
                overall_score=85.0,  # Default baseline
                status=ComplianceStatus.COMPLIANT,
                total_checks=0,
                passed_checks=0,
                failed_checks=0,
                active_violations=0,
                last_assessment=datetime.now(),
                total_requirements=self._get_framework_requirements_count(framework)
            )
    
    def _get_framework_requirements_count(self, framework: ComplianceFramework) -> int:
        """Get total requirements count for framework"""
        requirements_count = {
            ComplianceFramework.GDPR: 99,  # GDPR has 99 articles
            ComplianceFramework.HIPAA: 18,  # HIPAA has 18 sections
            ComplianceFramework.PCI_DSS: 12,  # PCI-DSS has 12 requirements
            ComplianceFramework.SOX: 11,  # SOX has 11 sections
            ComplianceFramework.CCPA: 7,  # CCPA has 7 main provisions
            ComplianceFramework.ISO_27001: 14,  # ISO 27001 has 14 control clauses
            ComplianceFramework.NIST: 23   # NIST has 23 control families
        }
        return requirements_count.get(framework, 10)
    
    async def record_compliance_violation(
        self,
        framework: ComplianceFramework,
        violation_type: str,
        severity: str,
        description: str,
        resource_id: str,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Record a compliance violation"""
        
        violation_id = f"violation_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{framework.value}"
        
        # Calculate remediation deadline based on severity
        deadline_hours = {
            'critical': 24,
            'high': 72,
            'medium': 168,  # 1 week
            'low': 720      # 1 month
        }
        
        deadline = datetime.now() + timedelta(hours=deadline_hours.get(severity, 168))
        
        violation = ComplianceViolation(
            violation_id=violation_id,
            framework=framework,
            violation_type=violation_type,
            severity=severity,
            description=description,
            resource_id=resource_id,
            user_id=user_id,
            detected_at=datetime.now(),
            status=ComplianceStatus.VIOLATION,
            remediation_deadline=deadline,
            metadata=metadata or {}
        )
        
        with self._lock:
            self.violations.append(violation)
            
            # Update framework status
            if framework in self.framework_status:
                self.framework_status[framework].active_violations += 1
                self.framework_status[framework].failed_checks += 1
                
                # Recalculate overall score
                await self._recalculate_framework_score(framework)
        
        # Record in security metrics
        self.security_metrics.record_security_event({
            'event_id': violation_id,
            'metric_type': 'compliance_check',
            'severity': severity,
            'user_id': user_id,
            'resource_id': resource_id,
            'outcome': 'violation',
            'details': {
                'framework': framework.value,
                'violation_type': violation_type,
                'description': description
            },
            'compliance_frameworks': [framework]
        })
        
        # Notify connected clients
        await self._broadcast_violation_alert(violation)
        
        self.logger.warning(f"Compliance violation recorded: {violation_id} ({framework.value})")
        
        return violation_id
    
    async def _recalculate_framework_score(self, framework: ComplianceFramework):
        """Recalculate compliance score for a framework"""
        
        framework_status = self.framework_status[framework]
        
        # Calculate based on violations and checks
        total_checks = framework_status.total_checks
        if total_checks > 0:
            pass_rate = framework_status.passed_checks / total_checks
        else:
            pass_rate = 1.0
        
        # Factor in active violations
        violation_penalty = min(framework_status.active_violations * 5, 50)  # Max 50% penalty
        
        # Calculate score (0-100)
        base_score = pass_rate * 100
        final_score = max(0, base_score - violation_penalty)
        
        framework_status.overall_score = final_score
        
        # Update status based on score
        if final_score >= 95:
            framework_status.status = ComplianceStatus.COMPLIANT
        elif final_score >= 85:
            framework_status.status = ComplianceStatus.WARNING
        elif final_score >= 70:
            framework_status.status = ComplianceStatus.VIOLATION
        else:
            framework_status.status = ComplianceStatus.CRITICAL
        
        # Record historical score
        self.compliance_scores.append({
            'timestamp': datetime.now(),
            'framework': framework.value,
            'score': final_score
        })
    
    async def resolve_violation(
        self,
        violation_id: str,
        resolution_notes: str,
        resolved_by: str
    ) -> bool:
        """Mark a violation as resolved"""
        
        with self._lock:
            violation = next((v for v in self.violations if v.violation_id == violation_id), None)
            
            if not violation:
                return False
            
            violation.status = ComplianceStatus.COMPLIANT
            violation.resolved_at = datetime.now()
            violation.remediation_notes = resolution_notes
            violation.assigned_to = resolved_by
            
            # Update framework status
            framework = violation.framework
            if framework in self.framework_status:
                self.framework_status[framework].active_violations -= 1
                self.framework_status[framework].passed_checks += 1
                
                # Record remediation time
                if violation.detected_at:
                    remediation_time = (datetime.now() - violation.detected_at).total_seconds() / 3600
                    self.remediation_metrics[framework.value].append(remediation_time)
                
                # Recalculate score
                await self._recalculate_framework_score(framework)
        
        # Notify connected clients
        await self._broadcast_resolution_update(violation_id, resolution_notes)
        
        self.logger.info(f"Violation resolved: {violation_id} by {resolved_by}")
        return True
    
    async def create_privacy_impact_assessment(
        self,
        data_processing_activity: str,
        data_types: List[str],
        legal_basis: str,
        data_subjects: List[str],
        retention_period: int,
        mitigation_measures: List[str]
    ) -> str:
        """Create a privacy impact assessment"""
        
        assessment_id = f"pia_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Calculate privacy risk score
        risk_score = self._calculate_privacy_risk_score(
            data_types, legal_basis, data_subjects, retention_period
        )
        
        assessment = PrivacyImpactAssessment(
            assessment_id=assessment_id,
            data_processing_activity=data_processing_activity,
            data_types=data_types,
            legal_basis=legal_basis,
            data_subjects=data_subjects,
            retention_period=retention_period,
            privacy_risk_score=risk_score,
            mitigation_measures=mitigation_measures,
            created_at=datetime.now()
        )
        
        with self._lock:
            self.privacy_assessments.append(assessment)
        
        self.logger.info(f"Privacy impact assessment created: {assessment_id}")
        return assessment_id
    
    def _calculate_privacy_risk_score(
        self,
        data_types: List[str],
        legal_basis: str,
        data_subjects: List[str],
        retention_period: int
    ) -> float:
        """Calculate privacy risk score (0-10)"""
        
        risk_score = 0.0
        
        # Risk from data types
        high_risk_data = ['health', 'financial', 'biometric', 'genetic', 'location']
        medium_risk_data = ['email', 'phone', 'name', 'address']
        
        for data_type in data_types:
            if any(risk_type in data_type.lower() for risk_type in high_risk_data):
                risk_score += 2.0
            elif any(risk_type in data_type.lower() for risk_type in medium_risk_data):
                risk_score += 1.0
        
        # Risk from legal basis
        if legal_basis.lower() in ['consent', 'legitimate_interest']:
            risk_score += 0.5
        elif legal_basis.lower() in ['legal_obligation', 'vital_interests']:
            risk_score += 0.2
        
        # Risk from data subjects
        vulnerable_subjects = ['children', 'patients', 'employees']
        if any(subject.lower() in vulnerable_subjects for subject in data_subjects):
            risk_score += 1.5
        
        # Risk from retention period
        if retention_period > 2555:  # > 7 years
            risk_score += 2.0
        elif retention_period > 1095:  # > 3 years
            risk_score += 1.0
        elif retention_period > 365:  # > 1 year
            risk_score += 0.5
        
        return min(risk_score, 10.0)
    
    def update_governance_metrics(
        self,
        cataloged_assets: Optional[int] = None,
        classified_assets: Optional[int] = None,
        lineage_tracked: Optional[int] = None,
        quality_monitored: Optional[int] = None,
        access_controlled: Optional[int] = None,
        retention_applied: Optional[int] = None
    ):
        """Update data governance metrics"""
        
        with self._lock:
            if cataloged_assets is not None:
                self.governance_metrics.cataloged_assets = cataloged_assets
            if classified_assets is not None:
                self.governance_metrics.classified_assets = classified_assets
            if lineage_tracked is not None:
                self.governance_metrics.lineage_tracked_assets = lineage_tracked
            if quality_monitored is not None:
                self.governance_metrics.quality_monitored_assets = quality_monitored
            if access_controlled is not None:
                self.governance_metrics.access_controlled_assets = access_controlled
            if retention_applied is not None:
                self.governance_metrics.retention_policy_applied = retention_applied
            
            # Recalculate governance score
            self._recalculate_governance_score()
            self.governance_metrics.last_updated = datetime.now()
    
    def _recalculate_governance_score(self):
        """Recalculate data governance score"""
        
        metrics = self.governance_metrics
        if metrics.total_data_assets == 0:
            metrics.governance_score = 100.0
            return
        
        # Calculate component scores
        cataloged_rate = metrics.cataloged_assets / metrics.total_data_assets
        classified_rate = metrics.classified_assets / metrics.total_data_assets
        lineage_rate = metrics.lineage_tracked_assets / metrics.total_data_assets
        quality_rate = metrics.quality_monitored_assets / metrics.total_data_assets
        access_rate = metrics.access_controlled_assets / metrics.total_data_assets
        retention_rate = metrics.retention_policy_applied / metrics.total_data_assets
        
        # Weighted score
        weights = {
            'cataloged': 0.2,
            'classified': 0.2,
            'lineage': 0.15,
            'quality': 0.15,
            'access': 0.15,
            'retention': 0.15
        }
        
        metrics.governance_score = (
            cataloged_rate * weights['cataloged'] +
            classified_rate * weights['classified'] +
            lineage_rate * weights['lineage'] +
            quality_rate * weights['quality'] +
            access_rate * weights['access'] +
            retention_rate * weights['retention']
        ) * 100
    
    async def get_dashboard_data(self, user_id: str = "system") -> Dict[str, Any]:
        """Get complete compliance dashboard data"""
        
        with self._lock:
            # Calculate overall compliance metrics
            total_violations = len([v for v in self.violations if v.status == ComplianceStatus.VIOLATION])
            overdue_violations = len([
                v for v in self.violations 
                if v.status == ComplianceStatus.VIOLATION and 
                v.remediation_deadline and v.remediation_deadline < datetime.now()
            ])
            
            # Framework-specific metrics
            framework_data = {}
            for framework, status in self.framework_status.items():
                framework_data[framework.value] = {
                    'overall_score': status.overall_score,
                    'status': status.status.value,
                    'active_violations': status.active_violations,
                    'total_checks': status.total_checks,
                    'passed_checks': status.passed_checks,
                    'failed_checks': status.failed_checks,
                    'requirements_met': status.requirements_met,
                    'total_requirements': status.total_requirements,
                    'compliance_percentage': (status.requirements_met / status.total_requirements * 100) 
                                           if status.total_requirements > 0 else 100,
                    'last_assessment': status.last_assessment.isoformat(),
                    'critical_issues': status.critical_issues
                }
            
            # Recent violations
            recent_violations = sorted(
                [v for v in self.violations if v.detected_at >= datetime.now() - timedelta(days=30)],
                key=lambda x: x.detected_at,
                reverse=True
            )[:10]
            
            # Privacy assessments summary
            pia_summary = {
                'total_assessments': len(self.privacy_assessments),
                'pending_assessments': len([p for p in self.privacy_assessments if p.status == 'pending']),
                'high_risk_assessments': len([p for p in self.privacy_assessments if p.privacy_risk_score > 7]),
                'avg_risk_score': sum(p.privacy_risk_score for p in self.privacy_assessments) / len(self.privacy_assessments)
                                 if self.privacy_assessments else 0
            }
            
            # Remediation metrics
            remediation_stats = {}
            for framework, times in self.remediation_metrics.items():
                if times:
                    remediation_stats[framework] = {
                        'avg_hours': sum(times) / len(times),
                        'min_hours': min(times),
                        'max_hours': max(times),
                        'total_resolved': len(times)
                    }
            
            # Governance metrics
            governance_data = asdict(self.governance_metrics)
            
        return {
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'overview': {
                'overall_compliance_score': sum(s.overall_score for s in self.framework_status.values()) 
                                          / len(self.framework_status),
                'total_violations': total_violations,
                'overdue_violations': overdue_violations,
                'frameworks_monitored': len(self.framework_status),
                'compliant_frameworks': len([s for s in self.framework_status.values() 
                                           if s.status == ComplianceStatus.COMPLIANT])
            },
            'frameworks': framework_data,
            'recent_violations': [
                {
                    'violation_id': v.violation_id,
                    'framework': v.framework.value,
                    'violation_type': v.violation_type,
                    'severity': v.severity,
                    'description': v.description,
                    'detected_at': v.detected_at.isoformat(),
                    'status': v.status.value,
                    'days_overdue': (datetime.now() - v.remediation_deadline).days 
                                  if v.remediation_deadline and v.remediation_deadline < datetime.now() else 0
                }
                for v in recent_violations
            ],
            'privacy_impact_assessments': pia_summary,
            'remediation_metrics': remediation_stats,
            'data_governance': governance_data,
            'compliance_trends': list(self.compliance_scores)[-50:],  # Last 50 data points
            'alerts': self._get_active_alerts()
        }
    
    def _get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get active compliance alerts"""
        
        alerts = []
        
        # Framework alerts
        for framework, status in self.framework_status.items():
            if status.status in [ComplianceStatus.CRITICAL, ComplianceStatus.VIOLATION]:
                alerts.append({
                    'type': 'framework_status',
                    'severity': status.status.value,
                    'message': f"{framework.value} compliance status is {status.status.value}",
                    'framework': framework.value,
                    'score': status.overall_score
                })
        
        # Overdue violations
        overdue = [
            v for v in self.violations 
            if v.status == ComplianceStatus.VIOLATION and 
            v.remediation_deadline and v.remediation_deadline < datetime.now()
        ]
        
        if overdue:
            alerts.append({
                'type': 'overdue_violations',
                'severity': 'critical',
                'message': f"{len(overdue)} violations are overdue for remediation",
                'count': len(overdue)
            })
        
        # High-risk privacy assessments
        high_risk_pias = [p for p in self.privacy_assessments if p.privacy_risk_score > 7]
        if high_risk_pias:
            alerts.append({
                'type': 'high_risk_privacy',
                'severity': 'high',
                'message': f"{len(high_risk_pias)} high-risk privacy impact assessments require attention",
                'count': len(high_risk_pias)
            })
        
        return alerts
    
    async def handle_websocket_connection(self, websocket: WebSocket):
        """Handle WebSocket connection for real-time updates"""
        
        await websocket.accept()
        self.active_connections.add(websocket)
        
        try:
            # Send initial dashboard data
            dashboard_data = await self.get_dashboard_data()
            await websocket.send_json({
                'type': 'dashboard_data',
                'data': dashboard_data
            })
            
            # Keep connection alive and handle messages
            while True:
                try:
                    # Wait for messages from client
                    message = await websocket.receive_json()
                    
                    if message.get('type') == 'request_update':
                        # Send updated dashboard data
                        dashboard_data = await self.get_dashboard_data(
                            message.get('user_id', 'system')
                        )
                        await websocket.send_json({
                            'type': 'dashboard_update',
                            'data': dashboard_data
                        })
                
                except Exception as e:
                    self.logger.error(f"WebSocket message handling error: {e}")
                    break
                    
        except Exception as e:
            self.logger.error(f"WebSocket connection error: {e}")
        finally:
            self.active_connections.discard(websocket)
    
    async def _broadcast_violation_alert(self, violation: ComplianceViolation):
        """Broadcast violation alert to connected clients"""
        
        alert_data = {
            'type': 'violation_alert',
            'data': {
                'violation_id': violation.violation_id,
                'framework': violation.framework.value,
                'violation_type': violation.violation_type,
                'severity': violation.severity,
                'description': violation.description,
                'detected_at': violation.detected_at.isoformat()
            }
        }
        
        # Send to all connected clients
        disconnected = set()
        for websocket in self.active_connections:
            try:
                await websocket.send_json(alert_data)
            except:
                disconnected.add(websocket)
        
        # Clean up disconnected clients
        self.active_connections -= disconnected
    
    async def _broadcast_resolution_update(self, violation_id: str, resolution_notes: str):
        """Broadcast violation resolution update"""
        
        update_data = {
            'type': 'violation_resolved',
            'data': {
                'violation_id': violation_id,
                'resolved_at': datetime.now().isoformat(),
                'resolution_notes': resolution_notes
            }
        }
        
        disconnected = set()
        for websocket in self.active_connections:
            try:
                await websocket.send_json(update_data)
            except:
                disconnected.add(websocket)
        
        self.active_connections -= disconnected
    
    def generate_compliance_report(
        self,
        framework: ComplianceFramework,
        start_date: datetime,
        end_date: datetime,
        report_format: str = "json"
    ) -> str:
        """Generate compliance report for a specific framework and time period"""
        
        # Filter violations for the framework and time period
        framework_violations = [
            v for v in self.violations
            if v.framework == framework and 
            start_date <= v.detected_at <= end_date
        ]
        
        # Calculate metrics
        total_violations = len(framework_violations)
        resolved_violations = len([v for v in framework_violations if v.resolved_at])
        overdue_violations = len([
            v for v in framework_violations
            if v.status == ComplianceStatus.VIOLATION and 
            v.remediation_deadline and v.remediation_deadline < datetime.now()
        ])
        
        # Violation breakdown by type and severity
        violation_by_type = defaultdict(int)
        violation_by_severity = defaultdict(int)
        
        for violation in framework_violations:
            violation_by_type[violation.violation_type] += 1
            violation_by_severity[violation.severity] += 1
        
        framework_status = self.framework_status.get(framework)
        
        report_data = {
            'report_generated': datetime.now().isoformat(),
            'framework': framework.value,
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            },
            'summary': {
                'overall_score': framework_status.overall_score if framework_status else 0,
                'compliance_status': framework_status.status.value if framework_status else 'unknown',
                'total_violations': total_violations,
                'resolved_violations': resolved_violations,
                'overdue_violations': overdue_violations,
                'resolution_rate': resolved_violations / total_violations if total_violations > 0 else 1.0
            },
            'violations_by_type': dict(violation_by_type),
            'violations_by_severity': dict(violation_by_severity),
            'detailed_violations': [
                {
                    'violation_id': v.violation_id,
                    'violation_type': v.violation_type,
                    'severity': v.severity,
                    'description': v.description,
                    'resource_id': v.resource_id,
                    'user_id': v.user_id,
                    'detected_at': v.detected_at.isoformat(),
                    'status': v.status.value,
                    'remediation_deadline': v.remediation_deadline.isoformat() if v.remediation_deadline else None,
                    'resolved_at': v.resolved_at.isoformat() if v.resolved_at else None,
                    'days_to_resolve': (v.resolved_at - v.detected_at).days if v.resolved_at else None
                }
                for v in framework_violations
            ]
        }
        
        if report_format == "json":
            return json.dumps(report_data, indent=2, default=str)
        else:
            # CSV format
            import csv
            import io
            
            output = io.StringIO()
            
            # Write summary
            output.write(f"Compliance Report - {framework.value}\n")
            output.write(f"Period: {start_date.date()} to {end_date.date()}\n")
            output.write(f"Overall Score: {report_data['summary']['overall_score']:.1f}\n\n")
            
            # Write violations table
            fieldnames = [
                'violation_id', 'violation_type', 'severity', 'description',
                'resource_id', 'user_id', 'detected_at', 'status', 'days_to_resolve'
            ]
            
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            
            for violation in report_data['detailed_violations']:
                writer.writerow({
                    'violation_id': violation['violation_id'],
                    'violation_type': violation['violation_type'],
                    'severity': violation['severity'],
                    'description': violation['description'][:50] + '...' if len(violation['description']) > 50 else violation['description'],
                    'resource_id': violation['resource_id'],
                    'user_id': violation['user_id'] or '',
                    'detected_at': violation['detected_at'],
                    'status': violation['status'],
                    'days_to_resolve': violation['days_to_resolve'] or 'N/A'
                })
            
            return output.getvalue()
    
    async def start_monitoring(self):
        """Start background monitoring tasks"""
        self._monitoring_task = asyncio.create_task(self._compliance_monitoring_loop())
        self._assessment_task = asyncio.create_task(self._periodic_assessment_loop())
        self.logger.info("Compliance monitoring started")
    
    async def stop_monitoring(self):
        """Stop background monitoring tasks"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._assessment_task:
            self._assessment_task.cancel()
        self.logger.info("Compliance monitoring stopped")
    
    async def _compliance_monitoring_loop(self):
        """Background loop for continuous compliance monitoring"""
        while True:
            try:
                # Send periodic updates to connected clients
                if self.active_connections:
                    dashboard_data = await self.get_dashboard_data()
                    update_data = {
                        'type': 'periodic_update',
                        'data': dashboard_data
                    }
                    
                    disconnected = set()
                    for websocket in self.active_connections:
                        try:
                            await websocket.send_json(update_data)
                        except:
                            disconnected.add(websocket)
                    
                    self.active_connections -= disconnected
                
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Compliance monitoring loop error: {e}")
                await asyncio.sleep(60)
    
    async def _periodic_assessment_loop(self):
        """Background loop for periodic compliance assessments"""
        while True:
            try:
                # Run daily compliance assessments
                for framework in self.framework_status.keys():
                    await self._run_framework_assessment(framework)
                
                # Clean up old data
                await self._cleanup_old_data()
                
                await asyncio.sleep(86400)  # Run daily
                
            except Exception as e:
                self.logger.error(f"Periodic assessment loop error: {e}")
                await asyncio.sleep(3600)  # Retry in 1 hour
    
    async def _run_framework_assessment(self, framework: ComplianceFramework):
        """Run compliance assessment for a specific framework"""
        
        # This would integrate with actual compliance checking systems
        # For now, we'll update the assessment timestamp
        
        with self._lock:
            if framework in self.framework_status:
                self.framework_status[framework].last_assessment = datetime.now()
                self.framework_status[framework].next_assessment = \
                    datetime.now() + timedelta(days=1)
        
        self.logger.debug(f"Completed compliance assessment for {framework.value}")
    
    async def _cleanup_old_data(self):
        """Clean up old compliance data"""
        
        cutoff_date = datetime.now() - timedelta(days=365)  # Keep 1 year of data
        
        with self._lock:
            # Clean up old violations (keep only if not resolved or recent)
            self.violations = [
                v for v in self.violations
                if v.detected_at >= cutoff_date or v.status != ComplianceStatus.COMPLIANT
            ]
            
            # Clean up old privacy assessments
            self.privacy_assessments = [
                p for p in self.privacy_assessments
                if p.created_at >= cutoff_date
            ]
        
        self.logger.debug("Cleaned up old compliance data")


# Global compliance dashboard instance
_compliance_dashboard: Optional[ComplianceDashboard] = None

def get_compliance_dashboard() -> ComplianceDashboard:
    """Get global compliance dashboard instance"""
    global _compliance_dashboard
    if _compliance_dashboard is None:
        _compliance_dashboard = ComplianceDashboard()
    return _compliance_dashboard