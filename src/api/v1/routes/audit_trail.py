"""
Audit Trail API Router
Provides comprehensive audit trail management and reporting endpoints.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from core.logging import get_logger
from core.audit.audit_trail_manager import (
    ComprehensiveAuditTrailManager,
    AuditEvent,
    AuditEventType,
    DataClassification,
    AuditConfiguration,
    create_audit_trail_manager
)
from data_access.db import get_async_engine

logger = get_logger(__name__)
router = APIRouter(prefix="/audit", tags=["audit-trail"])


class AuditEventRequest(BaseModel):
    """Request model for logging audit events."""
    event_type: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    resource_type: str
    resource_id: Optional[str] = None
    resource_name: Optional[str] = None
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    operation: str
    request_id: Optional[str] = None
    endpoint: Optional[str] = None
    method: Optional[str] = None
    status_code: Optional[int] = None


class AuditReportRequest(BaseModel):
    """Request model for audit reports."""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    user_id: Optional[str] = None
    resource_type: Optional[str] = None
    event_types: Optional[List[str]] = None
    include_compliance: bool = True
    include_risk_analysis: bool = True
    format: str = "json"  # json, csv, pdf


class ComplianceReportRequest(BaseModel):
    """Request model for compliance reports."""
    compliance_type: str  # GDPR, SOX, PCI
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    severity_filter: Optional[str] = None
    status_filter: Optional[str] = None


# Dependency to get audit manager
async def get_audit_manager() -> ComprehensiveAuditTrailManager:
    """Get audit trail manager instance."""
    engine = await get_async_engine()
    config = AuditConfiguration(
        enable_database_auditing=True,
        enable_api_auditing=True,
        enable_real_time_streaming=True,
        enable_gdpr_compliance=True,
        enable_sox_compliance=True,
        enable_pci_compliance=True
    )
    manager = create_audit_trail_manager(engine, config)
    await manager.initialize()
    return manager


@router.post("/events/log")
async def log_audit_event(
    request: AuditEventRequest,
    manager: ComprehensiveAuditTrailManager = Depends(get_audit_manager)
) -> Dict[str, Any]:
    """
    Log a comprehensive audit event.

    Features:
    - Automatic data classification
    - Risk score calculation
    - Compliance violation detection
    - User attribution and session tracking
    """
    try:
        # Validate event type
        try:
            event_type = AuditEventType(request.event_type.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid event type: {request.event_type}"
            )

        # Create audit event
        audit_event = AuditEvent(
            event_type=event_type,
            user_id=request.user_id,
            session_id=request.session_id,
            ip_address=request.ip_address,
            user_agent=request.user_agent,
            resource_type=request.resource_type,
            resource_id=request.resource_id,
            resource_name=request.resource_name,
            old_values=request.old_values,
            new_values=request.new_values,
            operation=request.operation,
            request_id=request.request_id,
            endpoint=request.endpoint,
            method=request.method,
            status_code=request.status_code
        )

        # Calculate changed fields if both old and new values provided
        if request.old_values and request.new_values:
            changed_fields = []
            for key, new_val in request.new_values.items():
                old_val = request.old_values.get(key)
                if old_val != new_val:
                    changed_fields.append(key)
            audit_event.changed_fields = changed_fields

        # Log the event
        await manager.log_event(audit_event)

        return {
            "status": "success",
            "event_id": audit_event.event_id,
            "message": "Audit event logged successfully",
            "risk_score": audit_event.risk_score,
            "data_classification": audit_event.data_classification.value,
            "compliance_flags": audit_event.compliance_flags,
            "timestamp": audit_event.timestamp.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error logging audit event: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to log audit event: {str(e)}"
        )


@router.post("/reports/comprehensive")
async def generate_audit_report(
    request: AuditReportRequest,
    manager: ComprehensiveAuditTrailManager = Depends(get_audit_manager)
) -> Dict[str, Any]:
    """
    Generate comprehensive audit report with analytics.

    Features:
    - Event statistics and trends
    - User activity analysis
    - Risk assessment
    - Compliance violation summary
    - Data access patterns
    """
    try:
        # Set default date range if not provided
        if not request.start_date:
            request.start_date = datetime.now() - timedelta(days=30)
        if not request.end_date:
            request.end_date = datetime.now()

        # Generate the report
        report = await manager.get_audit_report(
            start_date=request.start_date,
            end_date=request.end_date,
            user_id=request.user_id,
            resource_type=request.resource_type,
            include_compliance=request.include_compliance
        )

        if "error" in report:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=report["error"]
            )

        # Add additional analytics if requested
        if request.include_risk_analysis:
            report["risk_analysis"] = await _generate_risk_analysis(manager, request)

        return {
            "report_type": "comprehensive_audit",
            "parameters": {
                "start_date": request.start_date.isoformat(),
                "end_date": request.end_date.isoformat(),
                "user_id": request.user_id,
                "resource_type": request.resource_type
            },
            "data": report,
            "generated_at": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating audit report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate audit report: {str(e)}"
        )


@router.post("/reports/compliance")
async def generate_compliance_report(
    request: ComplianceReportRequest,
    manager: ComprehensiveAuditTrailManager = Depends(get_audit_manager)
) -> Dict[str, Any]:
    """
    Generate compliance-specific audit report.

    Supports:
    - GDPR compliance reporting
    - SOX compliance reporting
    - PCI DSS compliance reporting
    - Custom compliance frameworks
    """
    try:
        # Set default date range
        if not request.start_date:
            request.start_date = datetime.now() - timedelta(days=90)
        if not request.end_date:
            request.end_date = datetime.now()

        # Generate compliance report
        async with manager.engine.begin() as conn:
            from sqlalchemy import text

            # Get compliance violations
            violations_query = """
                SELECT
                    v.violation_id,
                    v.compliance_type,
                    v.violation_type,
                    v.severity,
                    v.description,
                    v.remediation_status,
                    v.reported_at,
                    v.resolved_at,
                    e.user_id,
                    e.resource_type,
                    e.resource_id,
                    e.timestamp as event_timestamp
                FROM audit_compliance_violations v
                JOIN audit_events e ON v.event_id = e.event_id
                WHERE v.compliance_type = $1
                  AND e.timestamp BETWEEN $2 AND $3
            """

            params = [request.compliance_type.upper(), request.start_date, request.end_date]

            if request.severity_filter:
                violations_query += " AND v.severity = $4"
                params.append(request.severity_filter)

            if request.status_filter:
                param_num = len(params) + 1
                violations_query += f" AND v.remediation_status = ${param_num}"
                params.append(request.status_filter)

            violations_query += " ORDER BY v.reported_at DESC"

            result = await conn.execute(text(violations_query), *params)
            violations = [dict(row._mapping) for row in result]

            # Get summary statistics
            summary_query = """
                SELECT
                    v.severity,
                    v.remediation_status,
                    COUNT(*) as violation_count,
                    COUNT(CASE WHEN v.resolved_at IS NOT NULL THEN 1 END) as resolved_count
                FROM audit_compliance_violations v
                JOIN audit_events e ON v.event_id = e.event_id
                WHERE v.compliance_type = $1
                  AND e.timestamp BETWEEN $2 AND $3
                GROUP BY v.severity, v.remediation_status
                ORDER BY v.severity, v.remediation_status
            """

            summary_result = await conn.execute(text(summary_query),
                                              request.compliance_type.upper(),
                                              request.start_date, request.end_date)
            summary_stats = [dict(row._mapping) for row in summary_result]

        return {
            "report_type": f"{request.compliance_type.lower()}_compliance",
            "compliance_framework": request.compliance_type.upper(),
            "report_period": {
                "start_date": request.start_date.isoformat(),
                "end_date": request.end_date.isoformat()
            },
            "summary": {
                "total_violations": len(violations),
                "resolved_violations": len([v for v in violations if v["resolved_at"]]),
                "pending_violations": len([v for v in violations if not v["resolved_at"]]),
                "critical_violations": len([v for v in violations if v["severity"] == "critical"]),
                "high_violations": len([v for v in violations if v["severity"] == "high"])
            },
            "statistics": summary_stats,
            "violations": violations[:100],  # Limit for API response
            "recommendations": _get_compliance_recommendations(request.compliance_type, violations),
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error generating compliance report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate compliance report: {str(e)}"
        )


@router.get("/events/search")
async def search_audit_events(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    start_date: Optional[datetime] = Query(None, description="Start date filter"),
    end_date: Optional[datetime] = Query(None, description="End date filter"),
    risk_score_min: Optional[float] = Query(None, description="Minimum risk score"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    manager: ComprehensiveAuditTrailManager = Depends(get_audit_manager)
) -> Dict[str, Any]:
    """
    Search and filter audit events with advanced criteria.

    Features:
    - Multi-criteria filtering
    - Risk-based filtering
    - Temporal analysis
    - Pattern detection
    """
    try:
        # Build search query
        search_conditions = []
        params = []

        if user_id:
            search_conditions.append(f"user_id = ${len(params) + 1}")
            params.append(user_id)

        if resource_type:
            search_conditions.append(f"resource_type = ${len(params) + 1}")
            params.append(resource_type)

        if event_type:
            search_conditions.append(f"event_type = ${len(params) + 1}")
            params.append(event_type.lower())

        if start_date:
            search_conditions.append(f"timestamp >= ${len(params) + 1}")
            params.append(start_date)

        if end_date:
            search_conditions.append(f"timestamp <= ${len(params) + 1}")
            params.append(end_date)

        if risk_score_min:
            search_conditions.append(f"risk_score >= ${len(params) + 1}")
            params.append(risk_score_min)

        # Construct final query
        base_query = """
            SELECT
                event_id, event_type, timestamp, user_id, session_id,
                resource_type, resource_id, resource_name, operation,
                risk_score, data_classification, sensitive_data_accessed,
                compliance_flags, endpoint, method, status_code
            FROM audit_events
        """

        if search_conditions:
            base_query += " WHERE " + " AND ".join(search_conditions)

        base_query += f" ORDER BY timestamp DESC LIMIT ${len(params) + 1}"
        params.append(limit)

        # Execute search
        async with manager.engine.begin() as conn:
            from sqlalchemy import text
            result = await conn.execute(text(base_query), *params)
            events = []

            for row in result:
                events.append({
                    "event_id": row.event_id,
                    "event_type": row.event_type,
                    "timestamp": row.timestamp.isoformat(),
                    "user_id": row.user_id,
                    "session_id": row.session_id,
                    "resource_type": row.resource_type,
                    "resource_id": row.resource_id,
                    "resource_name": row.resource_name,
                    "operation": row.operation,
                    "risk_score": float(row.risk_score) if row.risk_score else 0.0,
                    "data_classification": row.data_classification,
                    "sensitive_data_accessed": row.sensitive_data_accessed,
                    "compliance_flags": row.compliance_flags,
                    "endpoint": row.endpoint,
                    "method": row.method,
                    "status_code": row.status_code
                })

        return {
            "search_criteria": {
                "user_id": user_id,
                "resource_type": resource_type,
                "event_type": event_type,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "risk_score_min": risk_score_min
            },
            "results": {
                "count": len(events),
                "limit": limit,
                "events": events
            },
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error searching audit events: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search audit events: {str(e)}"
        )


@router.get("/dashboard/metrics")
async def get_audit_dashboard_metrics(
    time_range: str = Query("24h", description="Time range: 1h, 24h, 7d, 30d"),
    manager: ComprehensiveAuditTrailManager = Depends(get_audit_manager)
) -> Dict[str, Any]:
    """
    Get audit dashboard metrics and KPIs.

    Provides real-time insights for:
    - Event volume and trends
    - Risk distribution
    - Compliance status
    - User activity patterns
    """
    try:
        # Parse time range
        time_ranges = {
            "1h": timedelta(hours=1),
            "24h": timedelta(days=1),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30)
        }

        if time_range not in time_ranges:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid time range: {time_range}"
            )

        start_time = datetime.now() - time_ranges[time_range]
        end_time = datetime.now()

        # Get dashboard metrics
        async with manager.engine.begin() as conn:
            from sqlalchemy import text

            # Event volume metrics
            volume_query = """
                SELECT
                    DATE_TRUNC('hour', timestamp) as hour,
                    COUNT(*) as event_count,
                    COUNT(CASE WHEN risk_score > 0.7 THEN 1 END) as high_risk_events,
                    COUNT(CASE WHEN sensitive_data_accessed THEN 1 END) as sensitive_access_events
                FROM audit_events
                WHERE timestamp BETWEEN $1 AND $2
                GROUP BY DATE_TRUNC('hour', timestamp)
                ORDER BY hour
            """

            volume_result = await conn.execute(text(volume_query), start_time, end_time)
            volume_data = [dict(row._mapping) for row in volume_result]

            # Risk distribution
            risk_query = """
                SELECT
                    CASE
                        WHEN risk_score >= 0.8 THEN 'critical'
                        WHEN risk_score >= 0.6 THEN 'high'
                        WHEN risk_score >= 0.4 THEN 'medium'
                        ELSE 'low'
                    END as risk_level,
                    COUNT(*) as event_count
                FROM audit_events
                WHERE timestamp BETWEEN $1 AND $2
                GROUP BY risk_level
                ORDER BY risk_level
            """

            risk_result = await conn.execute(text(risk_query), start_time, end_time)
            risk_data = [dict(row._mapping) for row in risk_result]

            # Top users by activity
            users_query = """
                SELECT
                    user_id,
                    COUNT(*) as event_count,
                    AVG(risk_score) as avg_risk_score,
                    MAX(timestamp) as last_activity
                FROM audit_events
                WHERE timestamp BETWEEN $1 AND $2 AND user_id IS NOT NULL
                GROUP BY user_id
                ORDER BY event_count DESC
                LIMIT 10
            """

            users_result = await conn.execute(text(users_query), start_time, end_time)
            users_data = [dict(row._mapping) for row in users_result]

            # Compliance violations
            violations_query = """
                SELECT
                    v.compliance_type,
                    v.severity,
                    COUNT(*) as violation_count
                FROM audit_compliance_violations v
                JOIN audit_events e ON v.event_id = e.event_id
                WHERE e.timestamp BETWEEN $1 AND $2
                GROUP BY v.compliance_type, v.severity
                ORDER BY violation_count DESC
            """

            violations_result = await conn.execute(text(violations_query), start_time, end_time)
            violations_data = [dict(row._mapping) for row in violations_result]

        return {
            "time_range": time_range,
            "period": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "metrics": {
                "total_events": sum(row["event_count"] for row in volume_data),
                "high_risk_events": sum(row["high_risk_events"] for row in volume_data),
                "sensitive_access_events": sum(row["sensitive_access_events"] for row in volume_data),
                "total_violations": sum(row["violation_count"] for row in violations_data),
                "active_users": len(users_data)
            },
            "trends": {
                "hourly_volume": volume_data,
                "risk_distribution": risk_data,
                "compliance_violations": violations_data
            },
            "top_users": users_data,
            "system_health": {
                "status": "healthy" if manager.metrics["events_failed"] < manager.metrics["events_processed"] * 0.05 else "warning",
                "processing_metrics": manager.metrics
            },
            "generated_at": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dashboard metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get dashboard metrics: {str(e)}"
        )


async def _generate_risk_analysis(manager: ComprehensiveAuditTrailManager, request: AuditReportRequest) -> Dict[str, Any]:
    """Generate risk analysis for audit report."""
    # Implementation for risk analysis
    return {
        "risk_trends": [],
        "risk_factors": [],
        "recommendations": [
            "Monitor users with high risk scores",
            "Review access patterns for anomalies",
            "Implement additional security controls for sensitive data"
        ]
    }


def _get_compliance_recommendations(compliance_type: str, violations: List[Dict[str, Any]]) -> List[str]:
    """Get compliance-specific recommendations."""
    recommendations = []

    if compliance_type.upper() == "GDPR":
        recommendations.extend([
            "Implement explicit consent mechanisms for PII processing",
            "Establish clear data retention policies",
            "Provide data subject access and deletion capabilities",
            "Conduct regular privacy impact assessments"
        ])
    elif compliance_type.upper() == "SOX":
        recommendations.extend([
            "Implement segregation of duties for financial operations",
            "Establish approval workflows for data modifications",
            "Maintain detailed audit trails for all financial transactions",
            "Regular review of access controls and permissions"
        ])
    elif compliance_type.upper() == "PCI":
        recommendations.extend([
            "Implement strong access controls for cardholder data",
            "Encrypt sensitive payment information",
            "Regular vulnerability scanning and testing",
            "Maintain secure network architecture"
        ])

    # Add violation-specific recommendations
    if any(v["severity"] == "critical" for v in violations):
        recommendations.append("Address critical violations immediately")

    if len(violations) > 10:
        recommendations.append("Consider implementing automated compliance monitoring")

    return recommendations