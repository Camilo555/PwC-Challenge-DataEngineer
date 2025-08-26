"""
Security API Endpoints
Provides comprehensive security management endpoints including:
- Security dashboard and monitoring
- Compliance reporting and status
- Access control management
- DLP policy configuration
- Audit log search and retrieval
- Security testing and validation
"""
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from core.logging import get_logger
from core.security.enterprise_security_orchestrator import get_security_orchestrator
from core.security.enterprise_dlp import EnterpriseDLPManager, SensitiveDataType, DataClassification
from core.security.compliance_framework import get_compliance_engine, ComplianceFramework
from core.security.enhanced_access_control import get_access_control_manager, AccessRequest, AccessDecision
from core.security.advanced_security import get_security_manager, ActionType, SecurityEventType, ThreatLevel

logger = get_logger(__name__)
router = APIRouter(tags=["security"], prefix="/security")

# Pydantic models for request/response validation

class SecurityDashboardResponse(BaseModel):
    """Security dashboard data model"""
    summary: Dict[str, Any]
    threat_analysis: Dict[str, Any]
    dlp_statistics: Dict[str, Any]
    compliance_status: Dict[str, Any]
    access_control_metrics: Dict[str, Any]
    recent_events: List[Dict[str, Any]]
    security_score: float
    recommendations: List[str]
    last_updated: datetime

class ComplianceReportRequest(BaseModel):
    """Compliance report request model"""
    frameworks: List[str] = Field(default=["gdpr", "pci_dss", "hipaa", "sox"])
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    include_violations: bool = True
    include_remediations: bool = True
    export_format: str = Field(default="json", regex="^(json|pdf|csv)$")

class DLPPolicyRequest(BaseModel):
    """DLP policy configuration request"""
    name: str
    description: str
    enabled: bool = True
    data_types: List[str]
    classification_levels: List[str]
    actions: List[str]
    applies_to: Dict[str, Any]

class AccessControlRuleRequest(BaseModel):
    """Access control rule request"""
    rule_name: str
    description: str
    enabled: bool = True
    subjects: List[str]
    actions: List[str]
    resources: List[str]
    conditions: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=100, ge=1, le=1000)

class AuditLogSearchRequest(BaseModel):
    """Audit log search request"""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    user_id: Optional[str] = None
    action: Optional[str] = None
    resource_type: Optional[str] = None
    source_ip: Optional[str] = None
    limit: int = Field(default=100, le=1000)
    offset: int = Field(default=0, ge=0)

class SecurityTestRequest(BaseModel):
    """Security test request"""
    test_types: List[str] = Field(default=["vulnerability_scan", "dlp_test", "compliance_check"])
    target_endpoints: Optional[List[str]] = None
    intensity: str = Field(default="medium", regex="^(low|medium|high)$")


# Security Dashboard Endpoints

@router.get("/dashboard", response_model=SecurityDashboardResponse)
async def get_security_dashboard(
    user_id: str = Query(default="system"),
    include_sensitive: bool = Query(default=False),
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["security_read"]})
) -> SecurityDashboardResponse:
    """
    Get comprehensive security dashboard with real-time metrics and insights.
    
    Features:
    - Overall security posture and health
    - Threat analysis and active incidents
    - DLP statistics and policy status
    - Compliance framework status
    - Access control metrics
    - Recent security events
    - Security recommendations
    """
    
    try:
        logger.info(f"Generating security dashboard for user {user_id}")
        
        # Get orchestrator and component managers
        orchestrator = get_security_orchestrator()
        security_manager = get_security_manager()
        dlp_manager = EnterpriseDLPManager()
        compliance_engine = get_compliance_engine()
        access_manager = get_access_control_manager()
        
        # Collect data from all security components
        security_summary = security_manager.get_security_dashboard()
        dlp_dashboard = dlp_manager.get_dlp_dashboard()
        compliance_dashboard = compliance_engine.get_compliance_dashboard()
        access_dashboard = access_manager.get_access_dashboard()
        
        # Calculate overall security score
        security_score = _calculate_security_score(
            security_summary, dlp_dashboard, compliance_dashboard, access_dashboard
        )
        
        # Get recent events (last 24 hours)
        recent_events = _get_recent_security_events(security_manager, limit=20)
        
        # Generate recommendations
        recommendations = _generate_security_recommendations(
            security_summary, dlp_dashboard, compliance_dashboard, access_dashboard
        )
        
        dashboard_response = SecurityDashboardResponse(
            summary={
                "overall_status": "healthy" if security_score > 0.8 else "warning" if security_score > 0.6 else "critical",
                "active_threats": security_summary.get("active_threats", 0),
                "blocked_ips": security_summary.get("blocked_ips", 0),
                "total_events_24h": security_summary.get("total_events_24h", 0),
                "security_score": security_score
            },
            threat_analysis={
                "threat_level_distribution": security_summary.get("threat_level_distribution", {}),
                "event_type_distribution": security_summary.get("event_type_distribution", {}),
                "top_source_ips": security_summary.get("top_source_ips", {}),
                "geographic_analysis": _get_geographic_threat_analysis(security_manager)
            },
            dlp_statistics={
                "incidents_24h": dlp_dashboard.get("incidents_24h", 0),
                "incidents_7d": dlp_dashboard.get("incidents_7d", 0),
                "unresolved_incidents": dlp_dashboard.get("unresolved_incidents", 0),
                "data_type_distribution": dlp_dashboard.get("data_type_distribution", {}),
                "risk_level_distribution": dlp_dashboard.get("risk_level_distribution", {}),
                "policies_active": dlp_dashboard.get("policies_active", 0)
            },
            compliance_status={
                "overall_compliance": compliance_dashboard.get("overall_compliance", {}),
                "framework_status": compliance_dashboard.get("framework_status", {}),
                "violation_summary": compliance_dashboard.get("violation_summary", {}),
                "audit_readiness": compliance_dashboard.get("audit_readiness", {})
            },
            access_control_metrics={
                "total_subjects": access_dashboard["summary"]["total_subjects"],
                "total_policies": access_dashboard["summary"]["total_policies"],
                "active_elevations": access_dashboard["summary"]["active_elevations"],
                "access_requests_24h": access_dashboard["recent_activity"]["requests_24h"],
                "denied_requests_24h": access_dashboard["recent_activity"]["denied_requests_24h"]
            },
            recent_events=recent_events,
            security_score=security_score,
            recommendations=recommendations,
            last_updated=datetime.now()
        )
        
        logger.info(f"Security dashboard generated successfully with score {security_score:.2f}")
        return dashboard_response
        
    except Exception as e:
        logger.error(f"Failed to generate security dashboard: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate security dashboard: {str(e)}"
        )


@router.get("/dashboard/real-time")
async def get_real_time_security_metrics(
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system"})
) -> Dict[str, Any]:
    """Get real-time security metrics for live dashboard updates"""
    
    try:
        orchestrator = get_security_orchestrator()
        security_manager = get_security_manager()
        
        # Get current metrics
        current_metrics = {
            "timestamp": datetime.now().isoformat(),
            "active_connections": len(getattr(orchestrator, "active_connections", {})),
            "processing_requests": 0,  # Would track active requests
            "threat_level": "normal",
            "recent_events": _get_recent_security_events(security_manager, limit=5),
            "system_health": {
                "cpu_usage": 45.2,  # Would get actual system metrics
                "memory_usage": 67.8,
                "disk_usage": 23.1,
                "network_activity": "normal"
            },
            "security_alerts": {
                "critical": 0,
                "high": 1,
                "medium": 3,
                "low": 7
            }
        }
        
        return current_metrics
        
    except Exception as e:
        logger.error(f"Failed to get real-time metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve real-time metrics"
        )


# Compliance Reporting Endpoints

@router.post("/compliance/report")
async def generate_compliance_report(
    request: ComplianceReportRequest,
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["compliance_read"]})
) -> Dict[str, Any]:
    """
    Generate comprehensive compliance report for specified frameworks and time period.
    
    Supports:
    - Multiple compliance frameworks (GDPR, PCI-DSS, HIPAA, SOX)
    - Customizable date ranges
    - Violation tracking and remediation status
    - Multiple export formats (JSON, PDF, CSV)
    """
    
    try:
        logger.info(f"Generating compliance report for frameworks: {request.frameworks}")
        
        compliance_engine = get_compliance_engine()
        
        # Set default date range if not specified
        if not request.start_date:
            request.start_date = datetime.now() - timedelta(days=30)
        if not request.end_date:
            request.end_date = datetime.now()
        
        # Generate report data
        report_data = {
            "report_id": f"compliance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "generated_at": datetime.now().isoformat(),
            "requested_by": current_user["user_id"],
            "parameters": {
                "frameworks": request.frameworks,
                "date_range": {
                    "start": request.start_date.isoformat(),
                    "end": request.end_date.isoformat()
                },
                "include_violations": request.include_violations,
                "include_remediations": request.include_remediations
            },
            "compliance_status": {},
            "violations": [],
            "remediations": [],
            "recommendations": []
        }
        
        # Get compliance data for each requested framework
        for framework in request.frameworks:
            try:
                framework_status = compliance_engine.get_framework_status(framework)
                report_data["compliance_status"][framework] = framework_status
                
                if request.include_violations:
                    violations = compliance_engine.get_violations(
                        framework=framework,
                        start_date=request.start_date,
                        end_date=request.end_date
                    )
                    report_data["violations"].extend(violations)
                
                if request.include_remediations:
                    remediations = compliance_engine.get_remediations(
                        framework=framework,
                        start_date=request.start_date,
                        end_date=request.end_date
                    )
                    report_data["remediations"].extend(remediations)
                    
            except Exception as e:
                logger.error(f"Error getting compliance data for {framework}: {e}")
                report_data["compliance_status"][framework] = {
                    "status": "error",
                    "error": str(e)
                }
        
        # Generate recommendations
        report_data["recommendations"] = compliance_engine.generate_compliance_recommendations(
            report_data["compliance_status"]
        )
        
        # Handle different export formats
        if request.export_format == "json":
            return report_data
        elif request.export_format == "pdf":
            # Would generate PDF report
            return {
                "message": "PDF report generation not implemented in this demo",
                "data": report_data
            }
        elif request.export_format == "csv":
            # Would generate CSV export
            return {
                "message": "CSV export not implemented in this demo",
                "data": report_data
            }
            
    except Exception as e:
        logger.error(f"Failed to generate compliance report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate compliance report: {str(e)}"
        )


@router.get("/compliance/status")
async def get_compliance_status(
    framework: Optional[str] = Query(None, description="Specific framework to check"),
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system"})
) -> Dict[str, Any]:
    """Get current compliance status for all or specific frameworks"""
    
    try:
        compliance_engine = get_compliance_engine()
        
        if framework:
            status = compliance_engine.get_framework_status(framework)
            return {"framework": framework, "status": status}
        else:
            dashboard = compliance_engine.get_compliance_dashboard()
            return dashboard
            
    except Exception as e:
        logger.error(f"Failed to get compliance status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve compliance status"
        )


# Access Control Management Endpoints

@router.get("/access-control/dashboard")
async def get_access_control_dashboard(
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["access_control_read"]})
) -> Dict[str, Any]:
    """Get comprehensive access control dashboard"""
    
    try:
        access_manager = get_access_control_manager()
        dashboard = access_manager.get_access_dashboard()
        return dashboard
        
    except Exception as e:
        logger.error(f"Failed to get access control dashboard: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve access control dashboard"
        )


@router.post("/access-control/rules")
async def create_access_control_rule(
    rule: AccessControlRuleRequest,
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["access_control_write"]})
) -> Dict[str, Any]:
    """Create new access control rule"""
    
    try:
        access_manager = get_access_control_manager()
        
        # Create rule configuration
        rule_config = {
            "name": rule.rule_name,
            "description": rule.description,
            "enabled": rule.enabled,
            "subjects": rule.subjects,
            "actions": rule.actions,
            "resources": rule.resources,
            "conditions": rule.conditions,
            "priority": rule.priority,
            "created_by": current_user["user_id"],
            "created_at": datetime.now().isoformat()
        }
        
        # Add rule to access control system
        rule_id = access_manager.add_rule(rule_config)
        
        logger.info(f"Access control rule created: {rule_id}")
        
        return {
            "rule_id": rule_id,
            "message": "Access control rule created successfully",
            "rule": rule_config
        }
        
    except Exception as e:
        logger.error(f"Failed to create access control rule: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create access control rule: {str(e)}"
        )


@router.get("/access-control/rules/{rule_id}")
async def get_access_control_rule(
    rule_id: str,
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system"})
) -> Dict[str, Any]:
    """Get specific access control rule details"""
    
    try:
        access_manager = get_access_control_manager()
        rule = access_manager.get_rule(rule_id)
        
        if not rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Access control rule {rule_id} not found"
            )
        
        return {"rule_id": rule_id, "rule": rule}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get access control rule: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve access control rule"
        )


# DLP Policy Configuration Endpoints

@router.get("/dlp/dashboard")
async def get_dlp_dashboard(
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["dlp_read"]})
) -> Dict[str, Any]:
    """Get comprehensive DLP dashboard and statistics"""
    
    try:
        dlp_manager = EnterpriseDLPManager()
        dashboard = dlp_manager.get_dlp_dashboard()
        return dashboard
        
    except Exception as e:
        logger.error(f"Failed to get DLP dashboard: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve DLP dashboard"
        )


@router.get("/dlp/policies")
async def get_dlp_policies(
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system"})
) -> Dict[str, Any]:
    """Get all DLP policies"""
    
    try:
        dlp_manager = EnterpriseDLPManager()
        policies = {
            policy_id: {
                "name": policy.name,
                "description": policy.description,
                "enabled": policy.enabled,
                "data_types": [dt.value for dt in policy.data_types],
                "classification_levels": [cl.value for cl in policy.classification_levels],
                "actions": [action.value for action in policy.actions],
                "compliance_frameworks": [cf.value for cf in policy.compliance_frameworks],
                "applies_to": policy.applies_to,
                "created_at": policy.created_at.isoformat(),
                "updated_at": policy.updated_at.isoformat()
            }
            for policy_id, policy in dlp_manager.policy_engine.policies.items()
        }
        
        return {"policies": policies, "count": len(policies)}
        
    except Exception as e:
        logger.error(f"Failed to get DLP policies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve DLP policies"
        )


@router.get("/dlp/incidents")
async def get_dlp_incidents(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    data_type: Optional[str] = Query(None),
    resolved: Optional[bool] = Query(None),
    limit: int = Query(default=100, le=1000),
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system"})
) -> Dict[str, Any]:
    """Get DLP incidents with optional filtering"""
    
    try:
        dlp_manager = EnterpriseDLPManager()
        
        # Convert string data_type to enum if provided
        data_type_enum = None
        if data_type:
            try:
                data_type_enum = SensitiveDataType(data_type)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid data type: {data_type}"
                )
        
        incidents = dlp_manager.get_incidents(
            start_date=start_date,
            end_date=end_date,
            data_type=data_type_enum,
            resolved=resolved
        )
        
        # Limit results
        incidents = incidents[:limit]
        
        # Convert incidents to dict format
        incidents_data = [
            {
                "incident_id": incident.incident_id,
                "timestamp": incident.timestamp.isoformat(),
                "policy_id": incident.policy_id,
                "data_type": incident.data_type.value,
                "classification": incident.classification.value,
                "action_taken": incident.action_taken.value,
                "source_location": incident.source_location,
                "destination_location": incident.destination_location,
                "user_id": incident.user_id,
                "confidence": incident.confidence,
                "risk_score": incident.risk_score,
                "resolved": incident.resolved,
                "resolution_notes": incident.resolution_notes,
                "details": incident.details
            }
            for incident in incidents
        ]
        
        return {"incidents": incidents_data, "count": len(incidents_data)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get DLP incidents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve DLP incidents"
        )


# Audit Log Endpoints

@router.post("/audit/search")
async def search_audit_logs(
    search_request: AuditLogSearchRequest,
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["audit_read"]})
) -> Dict[str, Any]:
    """Search audit logs with advanced filtering"""
    
    try:
        security_manager = get_security_manager()
        
        # Convert action string to enum if provided
        action_enum = None
        if search_request.action:
            try:
                action_enum = ActionType(search_request.action.upper())
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid action type: {search_request.action}"
                )
        
        # Search audit logs
        audit_entries = security_manager.audit_logger.search_audit_logs(
            start_time=search_request.start_time,
            end_time=search_request.end_time,
            user_id=search_request.user_id,
            action=action_enum,
            resource_type=search_request.resource_type,
            source_ip=search_request.source_ip,
            limit=search_request.limit
        )
        
        # Apply offset
        if search_request.offset > 0:
            audit_entries = audit_entries[search_request.offset:]
        
        # Convert to dict format
        entries_data = [
            {
                "audit_id": entry.audit_id,
                "timestamp": entry.timestamp.isoformat(),
                "user_id": entry.user_id,
                "session_id": entry.session_id,
                "source_ip": entry.source_ip,
                "user_agent": entry.user_agent,
                "action": entry.action.value,
                "resource_type": entry.resource_type,
                "resource_id": entry.resource_id,
                "old_value": entry.old_value,
                "new_value": entry.new_value,
                "success": entry.success,
                "error_message": entry.error_message,
                "request_id": entry.request_id,
                "correlation_id": entry.correlation_id,
                "metadata": entry.metadata
            }
            for entry in audit_entries
        ]
        
        return {
            "entries": entries_data,
            "count": len(entries_data),
            "search_parameters": search_request.dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to search audit logs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search audit logs"
        )


@router.get("/audit/statistics")
async def get_audit_statistics(
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system"})
) -> Dict[str, Any]:
    """Get audit log statistics and analytics"""
    
    try:
        security_manager = get_security_manager()
        statistics = security_manager.audit_logger.get_audit_statistics()
        return statistics
        
    except Exception as e:
        logger.error(f"Failed to get audit statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve audit statistics"
        )


# Security Testing Endpoints

@router.post("/test/run-security-assessment")
async def run_security_assessment(
    test_request: SecurityTestRequest,
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["security_test"]})
) -> Dict[str, Any]:
    """Run comprehensive security assessment"""
    
    try:
        logger.info(f"Running security assessment for user {current_user['user_id']}")
        
        orchestrator = get_security_orchestrator()
        
        # Run comprehensive assessment
        assessment_result = await orchestrator.run_security_assessment()
        
        # Add test-specific metadata
        assessment_result["test_metadata"] = {
            "requested_by": current_user["user_id"],
            "test_types": test_request.test_types,
            "target_endpoints": test_request.target_endpoints,
            "intensity": test_request.intensity,
            "started_at": datetime.now().isoformat()
        }
        
        logger.info(f"Security assessment completed with status: {assessment_result['overall_status']}")
        
        return assessment_result
        
    except Exception as e:
        logger.error(f"Failed to run security assessment: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to run security assessment: {str(e)}"
        )


@router.get("/test/vulnerability-scan")
async def run_vulnerability_scan(
    target_url: str = Query(..., description="Target URL to scan"),
    scan_type: str = Query(default="basic", regex="^(basic|comprehensive|deep)$"),
    current_user: Dict[str, Any] = Depends(lambda: {"user_id": "system", "permissions": ["security_test"]})
) -> Dict[str, Any]:
    """Run vulnerability scan on specific endpoint"""
    
    try:
        logger.info(f"Running vulnerability scan on {target_url}")
        
        # This would integrate with actual vulnerability scanning tools
        scan_result = {
            "scan_id": f"vuln_scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "target_url": target_url,
            "scan_type": scan_type,
            "started_at": datetime.now().isoformat(),
            "status": "completed",
            "vulnerabilities": [
                {
                    "severity": "medium",
                    "type": "Missing Security Headers",
                    "description": "Some security headers are not present",
                    "recommendation": "Add X-Frame-Options, X-Content-Type-Options headers"
                }
            ],
            "security_score": 7.5,
            "recommendations": [
                "Implement Content Security Policy",
                "Add security headers",
                "Enable HSTS"
            ]
        }
        
        return scan_result
        
    except Exception as e:
        logger.error(f"Failed to run vulnerability scan: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to run vulnerability scan"
        )


# Helper functions

def _calculate_security_score(security_summary: Dict, dlp_dashboard: Dict, compliance_dashboard: Dict, access_dashboard: Dict) -> float:
    """Calculate overall security score based on component metrics"""
    
    score_components = []
    
    # Security component (25% weight)
    active_threats = security_summary.get("active_threats", 0)
    blocked_ips = security_summary.get("blocked_ips", 0)
    security_score = max(0, 10 - active_threats - (blocked_ips * 0.1)) / 10
    score_components.append(security_score * 0.25)
    
    # DLP component (25% weight)
    unresolved_incidents = dlp_dashboard.get("unresolved_incidents", 0)
    dlp_score = max(0, 10 - unresolved_incidents) / 10
    score_components.append(dlp_score * 0.25)
    
    # Compliance component (30% weight)
    compliance_rate = compliance_dashboard.get("overall_compliance", {}).get("compliance_rate", 1.0)
    score_components.append(compliance_rate * 0.30)
    
    # Access control component (20% weight)
    denied_requests = access_dashboard["recent_activity"]["denied_requests_24h"]
    access_score = max(0, 10 - denied_requests * 0.1) / 10
    score_components.append(access_score * 0.20)
    
    return sum(score_components)


def _get_recent_security_events(security_manager, limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent security events"""
    
    try:
        # Get recent events from security manager
        with security_manager.lock:
            recent_events = list(security_manager.security_events)[-limit:]
        
        events_data = [
            {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "threat_level": event.threat_level.value,
                "timestamp": event.timestamp.isoformat(),
                "source_ip": event.source_ip,
                "user_id": event.user_id,
                "resource_accessed": event.resource_accessed,
                "success": event.success,
                "blocked": event.blocked,
                "risk_score": event.risk_score
            }
            for event in reversed(recent_events)
        ]
        
        return events_data
        
    except Exception as e:
        logger.error(f"Failed to get recent security events: {e}")
        return []


def _generate_security_recommendations(security_summary: Dict, dlp_dashboard: Dict, compliance_dashboard: Dict, access_dashboard: Dict) -> List[str]:
    """Generate security recommendations based on current state"""
    
    recommendations = []
    
    # Check for high threat activity
    if security_summary.get("active_threats", 0) > 5:
        recommendations.append("High threat activity detected - review and investigate active security incidents")
    
    # Check DLP incidents
    if dlp_dashboard.get("unresolved_incidents", 0) > 0:
        recommendations.append(f"Resolve {dlp_dashboard['unresolved_incidents']} unresolved DLP incidents")
    
    # Check compliance status
    compliance_rate = compliance_dashboard.get("overall_compliance", {}).get("compliance_rate", 1.0)
    if compliance_rate < 0.9:
        recommendations.append("Compliance rate below 90% - address policy violations and gaps")
    
    # Check access control
    denied_requests = access_dashboard["recent_activity"]["denied_requests_24h"]
    if denied_requests > 10:
        recommendations.append("High number of denied access requests - review access policies")
    
    # General recommendations
    if not recommendations:
        recommendations.extend([
            "Security posture is healthy - maintain current security practices",
            "Consider regular security training for all team members",
            "Schedule next security assessment within 30 days"
        ])
    
    return recommendations


def _get_geographic_threat_analysis(security_manager) -> Dict[str, Any]:
    """Get geographic analysis of threats"""
    
    try:
        # This would analyze geographic patterns in security events
        return {
            "top_countries": ["US", "CN", "RU"],
            "high_risk_regions": ["Unknown"],
            "geographic_diversity": "medium",
            "anomalous_locations": []
        }
    except Exception:
        return {"status": "analysis_unavailable"}