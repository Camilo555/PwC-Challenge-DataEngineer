"""
Performance SLA API Routes - <15ms Response Time Validation
=========================================================

FastAPI routes for performance SLA monitoring and validation with real-time metrics.

Key Features:
- Real-time performance metric recording and retrieval
- <15ms SLA compliance validation and reporting
- Performance analysis and trending
- SLA violation tracking and alerting
- Business impact analysis for performance issues
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

from core.auth import get_current_user, User
from monitoring.performance_sla_monitor import (
    PerformanceSLAMonitor,
    PerformanceMetric,
    SLAConfiguration,
    SLAStatus,
    AlertSeverity
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/performance-sla", tags=["Performance SLA"])

# Global Performance SLA Monitor instance
performance_monitor = PerformanceSLAMonitor()


# Pydantic Models for API
class RecordPerformanceRequest(BaseModel):
    """Request model for recording performance metrics."""
    endpoint: str = Field(..., description="API endpoint path")
    method: str = Field(..., description="HTTP method")
    response_time_ms: float = Field(..., ge=0, description="Response time in milliseconds")
    status_code: int = Field(..., ge=100, le=599, description="HTTP status code")
    user_id: Optional[str] = Field(None, description="User ID if authenticated")
    region: Optional[str] = Field(None, description="Geographic region")
    cache_hit: bool = Field(False, description="Whether request was served from cache")
    request_size_bytes: int = Field(0, ge=0, description="Request size in bytes")
    response_size_bytes: int = Field(0, ge=0, description="Response size in bytes")

    class Config:
        schema_extra = {
            "example": {
                "endpoint": "/api/v1/sales",
                "method": "GET",
                "response_time_ms": 12.5,
                "status_code": 200,
                "user_id": "user123",
                "region": "us-east-1",
                "cache_hit": False,
                "request_size_bytes": 1024,
                "response_size_bytes": 2048
            }
        }


class SLAComplianceRequest(BaseModel):
    """Request model for SLA compliance validation."""
    endpoint: Optional[str] = Field(None, description="Specific endpoint to validate (optional)")
    time_window_minutes: int = Field(5, ge=1, le=1440, description="Time window in minutes")

    class Config:
        schema_extra = {
            "example": {
                "endpoint": "/api/v1/sales",
                "time_window_minutes": 15
            }
        }


class PerformanceAnalysisRequest(BaseModel):
    """Request model for performance analysis."""
    endpoint: Optional[str] = Field(None, description="Specific endpoint to analyze (optional)")
    time_window_minutes: int = Field(60, ge=1, le=10080, description="Time window in minutes")

    class Config:
        schema_extra = {
            "example": {
                "endpoint": "/api/v1/dashboard",
                "time_window_minutes": 120
            }
        }


class SLAConfigurationRequest(BaseModel):
    """Request model for SLA configuration."""
    endpoint_pattern: str = Field(..., description="Endpoint pattern (e.g., /api/v1/sales)")
    target_response_time_ms: float = Field(15.0, ge=1.0, le=1000.0, description="Target response time in ms")
    warning_threshold_ms: float = Field(12.0, ge=1.0, le=1000.0, description="Warning threshold in ms")
    percentile_target: float = Field(95.0, ge=50.0, le=99.99, description="Percentile target")
    max_violation_rate: float = Field(1.0, ge=0.1, le=10.0, description="Maximum violation rate percentage")

    @validator('warning_threshold_ms')
    def warning_must_be_less_than_target(cls, v, values):
        if 'target_response_time_ms' in values and v >= values['target_response_time_ms']:
            raise ValueError('Warning threshold must be less than target response time')
        return v

    class Config:
        schema_extra = {
            "example": {
                "endpoint_pattern": "/api/v1/analytics",
                "target_response_time_ms": 15.0,
                "warning_threshold_ms": 12.0,
                "percentile_target": 95.0,
                "max_violation_rate": 1.0
            }
        }


# API Endpoints
@router.on_event("startup")
async def initialize_performance_monitor():
    """Initialize the performance SLA monitor on startup."""
    try:
        await performance_monitor.initialize()
        logger.info("Performance SLA Monitor initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Performance SLA Monitor: {e}")


@router.post("/metrics", status_code=status.HTTP_201_CREATED)
async def record_performance_metric(
    request: RecordPerformanceRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user)
) -> Dict[str, str]:
    """
    Record a performance metric for SLA monitoring.

    This endpoint records individual API response time measurements for real-time
    SLA compliance monitoring and violation detection.
    """
    try:
        # Record metric in background to avoid blocking
        background_tasks.add_task(
            performance_monitor.record_performance_metric,
            endpoint=request.endpoint,
            method=request.method,
            response_time_ms=request.response_time_ms,
            status_code=request.status_code,
            user_id=request.user_id or current_user.id,
            region=request.region,
            cache_hit=request.cache_hit,
            request_size_bytes=request.request_size_bytes,
            response_size_bytes=request.response_size_bytes
        )

        # Check for immediate SLA violation
        violation_status = "compliant"
        if request.response_time_ms > 15.0:
            violation_status = "violation"
        elif request.response_time_ms > 12.0:
            violation_status = "warning"

        return {
            "message": "Performance metric recorded successfully",
            "endpoint": request.endpoint,
            "response_time_ms": str(request.response_time_ms),
            "sla_status": violation_status,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to record performance metric: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record performance metric"
        )


@router.post("/validate-compliance", response_model=Dict[str, Any])
async def validate_sla_compliance(
    request: SLAComplianceRequest,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Validate <15ms SLA compliance for API endpoints.

    Returns comprehensive compliance report with violations, trends, and
    performance recommendations for optimization.
    """
    try:
        compliance_report = await performance_monitor.validate_15ms_sla_compliance(
            endpoint=request.endpoint,
            time_window_minutes=request.time_window_minutes
        )

        # Add metadata
        compliance_report["validated_by"] = current_user.id
        compliance_report["validation_type"] = "15ms_sla_compliance"

        # Log critical violations
        if not compliance_report["overall_compliance"]:
            violation_count = compliance_report["compliance_summary"]["violation_endpoints"]
            logger.warning(
                f"SLA compliance validation failed: {violation_count} endpoints in violation "
                f"(validated by {current_user.id})"
            )

        return compliance_report

    except Exception as e:
        logger.error(f"Failed to validate SLA compliance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SLA compliance validation failed"
        )


@router.post("/analysis", response_model=Dict[str, Any])
async def get_performance_analysis(
    request: PerformanceAnalysisRequest,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get comprehensive performance analysis for API endpoints.

    Provides detailed performance statistics, trends, and insights for
    endpoint optimization and capacity planning.
    """
    try:
        analyses = await performance_monitor.get_performance_analysis(
            endpoint=request.endpoint,
            time_window_minutes=request.time_window_minutes
        )

        if not analyses:
            return {
                "message": "No performance data available for the specified criteria",
                "endpoint": request.endpoint,
                "time_window_minutes": request.time_window_minutes,
                "timestamp": datetime.utcnow().isoformat(),
                "analyzed_by": current_user.id
            }

        # Calculate summary statistics
        all_analyses = list(analyses.values())
        summary = {
            "total_endpoints": len(all_analyses),
            "avg_response_time_ms": sum(a.avg_response_time_ms for a in all_analyses) / len(all_analyses),
            "avg_p95_response_time_ms": sum(a.p95_response_time_ms for a in all_analyses) / len(all_analyses),
            "total_requests": sum(a.total_requests for a in all_analyses),
            "total_violations": sum(a.violation_count for a in all_analyses),
            "avg_compliance_rate": sum(a.sla_compliance_rate for a in all_analyses) / len(all_analyses),
            "avg_performance_score": sum(a.performance_score for a in all_analyses) / len(all_analyses)
        }

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "analyzed_by": current_user.id,
            "analysis_period": {
                "time_window_minutes": request.time_window_minutes,
                "endpoint_filter": request.endpoint
            },
            "summary": summary,
            "endpoint_analyses": {
                ep: {
                    "endpoint": analysis.endpoint,
                    "total_requests": analysis.total_requests,
                    "avg_response_time_ms": round(analysis.avg_response_time_ms, 2),
                    "p50_response_time_ms": round(analysis.p50_response_time_ms, 2),
                    "p95_response_time_ms": round(analysis.p95_response_time_ms, 2),
                    "p99_response_time_ms": round(analysis.p99_response_time_ms, 2),
                    "max_response_time_ms": round(analysis.max_response_time_ms, 2),
                    "sla_compliance_rate": round(analysis.sla_compliance_rate, 2),
                    "violation_count": analysis.violation_count,
                    "performance_score": analysis.performance_score,
                    "trend_direction": analysis.trend_direction
                }
                for ep, analysis in analyses.items()
            }
        }

    except Exception as e:
        logger.error(f"Failed to get performance analysis: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Performance analysis failed"
        )


@router.get("/metrics/real-time", response_model=Dict[str, Any])
async def get_real_time_metrics(
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get real-time performance metrics for dashboard display.

    Returns current performance statistics for the last minute including
    request rate, response times, and SLA compliance.
    """
    try:
        metrics = await performance_monitor.get_real_time_performance_metrics()

        # Add user context
        metrics["retrieved_by"] = current_user.id
        metrics["metric_type"] = "real_time_performance"

        # Add status indicators
        if metrics["avg_response_time_ms"] > 15.0:
            metrics["status"] = "violation"
            metrics["status_message"] = f"Average response time ({metrics['avg_response_time_ms']}ms) exceeds 15ms target"
        elif metrics["avg_response_time_ms"] > 12.0:
            metrics["status"] = "warning"
            metrics["status_message"] = f"Average response time ({metrics['avg_response_time_ms']}ms) approaching 15ms target"
        else:
            metrics["status"] = "healthy"
            metrics["status_message"] = "Performance within SLA targets"

        return metrics

    except Exception as e:
        logger.error(f"Failed to get real-time metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve real-time metrics"
        )


@router.get("/violations/active", response_model=Dict[str, Any])
async def get_active_violations(
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get currently active SLA violations.

    Returns list of ongoing performance violations that require attention.
    """
    try:
        active_violations = performance_monitor.active_violations

        violations_data = []
        for violation_id, violation in active_violations.items():
            violations_data.append({
                "violation_id": violation.violation_id,
                "endpoint": violation.endpoint,
                "violation_type": violation.violation_type,
                "severity": violation.severity.value,
                "response_time_ms": violation.response_time_ms,
                "threshold_ms": violation.threshold_ms,
                "timestamp": violation.timestamp.isoformat(),
                "business_impact_score": violation.business_impact_score,
                "resolved": violation.resolved
            })

        # Sort by severity and timestamp
        severity_order = {"emergency": 0, "critical": 1, "warning": 2, "info": 3}
        violations_data.sort(
            key=lambda x: (severity_order.get(x["severity"], 4), x["timestamp"]),
            reverse=True
        )

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "total_active_violations": len(violations_data),
            "violation_summary": {
                "emergency": len([v for v in violations_data if v["severity"] == "emergency"]),
                "critical": len([v for v in violations_data if v["severity"] == "critical"]),
                "warning": len([v for v in violations_data if v["severity"] == "warning"]),
                "info": len([v for v in violations_data if v["severity"] == "info"])
            },
            "violations": violations_data
        }

    except Exception as e:
        logger.error(f"Failed to get active violations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve active violations"
        )


@router.post("/configuration", status_code=status.HTTP_201_CREATED)
async def configure_sla(
    request: SLAConfigurationRequest,
    current_user: User = Depends(get_current_user)
) -> Dict[str, str]:
    """
    Configure SLA parameters for specific endpoints.

    Allows customization of performance targets and thresholds for different
    API endpoints based on business requirements.
    """
    try:
        # Create SLA configuration
        sla_config = SLAConfiguration(
            endpoint_pattern=request.endpoint_pattern,
            target_response_time_ms=request.target_response_time_ms,
            warning_threshold_ms=request.warning_threshold_ms,
            percentile_target=request.percentile_target,
            max_violation_rate=request.max_violation_rate
        )

        # Store configuration
        performance_monitor.sla_configurations[request.endpoint_pattern] = sla_config

        logger.info(
            f"SLA configuration updated for {request.endpoint_pattern} by {current_user.id}: "
            f"target={request.target_response_time_ms}ms, warning={request.warning_threshold_ms}ms"
        )

        return {
            "message": "SLA configuration updated successfully",
            "endpoint_pattern": request.endpoint_pattern,
            "target_response_time_ms": str(request.target_response_time_ms),
            "warning_threshold_ms": str(request.warning_threshold_ms),
            "configured_by": current_user.id,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to configure SLA: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SLA configuration failed"
        )


@router.get("/configuration", response_model=Dict[str, Any])
async def get_sla_configurations(
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current SLA configurations for all endpoints.

    Returns the configured performance targets and thresholds for monitoring.
    """
    try:
        configurations = {}
        for endpoint, config in performance_monitor.sla_configurations.items():
            configurations[endpoint] = {
                "endpoint_pattern": config.endpoint_pattern,
                "target_response_time_ms": config.target_response_time_ms,
                "warning_threshold_ms": config.warning_threshold_ms,
                "percentile_target": config.percentile_target,
                "max_violation_rate": config.max_violation_rate,
                "measurement_window_minutes": config.measurement_window_minutes,
                "alert_escalation_minutes": config.alert_escalation_minutes
            }

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "total_configurations": len(configurations),
            "default_configuration": {
                "target_response_time_ms": performance_monitor.default_sla.target_response_time_ms,
                "warning_threshold_ms": performance_monitor.default_sla.warning_threshold_ms
            },
            "endpoint_configurations": configurations
        }

    except Exception as e:
        logger.error(f"Failed to get SLA configurations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve SLA configurations"
        )


@router.get("/health", response_model=Dict[str, Any])
async def get_monitor_health() -> Dict[str, Any]:
    """
    Get health status of the performance SLA monitor.

    Returns operational status and basic statistics about the monitoring system.
    """
    try:
        current_time = datetime.utcnow()

        # Calculate basic statistics
        total_metrics = len(performance_monitor.metrics_buffer)
        active_violations = len(performance_monitor.active_violations)
        total_violations = len(performance_monitor.violation_history)

        # Check Redis connectivity
        redis_status = "connected" if performance_monitor.redis_client else "disconnected"

        # Recent activity (last 5 minutes)
        five_minutes_ago = current_time - timedelta(minutes=5)
        recent_metrics = [
            m for m in performance_monitor.metrics_buffer
            if m.timestamp >= five_minutes_ago
        ]

        return {
            "timestamp": current_time.isoformat(),
            "status": "healthy",
            "monitor_info": {
                "total_metrics_recorded": total_metrics,
                "metrics_buffer_size": len(performance_monitor.metrics_buffer),
                "active_violations": active_violations,
                "total_violations_recorded": total_violations,
                "configured_endpoints": len(performance_monitor.sla_configurations),
                "redis_status": redis_status
            },
            "recent_activity": {
                "metrics_last_5_minutes": len(recent_metrics),
                "avg_response_time_ms": sum(m.response_time_ms for m in recent_metrics) / len(recent_metrics) if recent_metrics else 0
            },
            "sla_targets": {
                "default_target_ms": performance_monitor.default_sla.target_response_time_ms,
                "default_warning_ms": performance_monitor.default_sla.warning_threshold_ms
            }
        }

    except Exception as e:
        logger.error(f"Failed to get monitor health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve monitor health status"
        )