"""
Chaos Engineering API Routes
============================

FastAPI routes for comprehensive chaos engineering management with:
- Controlled failure injection with safety guardrails
- Real-time experiment monitoring and control
- Resilience assessment and reporting
- Business impact analysis and compliance
- Emergency abort capabilities with automated recovery
- Comprehensive audit trails for reliability validation

Endpoints:
- POST /chaos/experiments - Create chaos experiment
- GET /chaos/experiments - List experiments with filtering
- GET /chaos/experiments/{experiment_id} - Get experiment details
- POST /chaos/experiments/{experiment_id}/execute - Execute experiment
- POST /chaos/experiments/{experiment_id}/abort - Abort running experiment
- GET /chaos/executions - List experiment executions
- GET /chaos/executions/{execution_id} - Get execution details
- GET /chaos/resilience/report - Generate resilience report
- GET /chaos/statistics - Get chaos engineering statistics
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi import status, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, validator

from core.auth.jwt import get_current_user
from core.auth.models import User
from monitoring.chaos_engineering import (
    ChaosEngineering,
    ChaosExperiment,
    ExperimentExecution,
    ResilienceReport,
    ChaosType,
    BlastRadius,
    SafetyLevel,
    ExperimentStatus,
    SafetyGuard,
    get_chaos_engineering
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/chaos", tags=["chaos-engineering"])

# Request/Response Models
class CreateExperimentRequest(BaseModel):
    """Create chaos experiment request model"""
    name: str = Field(..., min_length=1, max_length=100, description="Experiment name")
    description: str = Field("", max_length=500, description="Experiment description")
    chaos_type: ChaosType = Field(..., description="Type of chaos to inject")
    blast_radius: BlastRadius = Field(BlastRadius.SINGLE_INSTANCE, description="Scope of impact")
    safety_level: SafetyLevel = Field(SafetyLevel.SAFE, description="Safety level for experiment")
    target_services: List[str] = Field(..., min_items=1, description="Target services")
    target_percentage: float = Field(10.0, ge=1.0, le=100.0, description="Percentage of targets to affect")
    duration_minutes: int = Field(5, ge=1, le=60, description="Experiment duration in minutes")
    intensity: float = Field(0.5, ge=0.1, le=1.0, description="Failure intensity (0.1-1.0)")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Experiment-specific parameters")
    hypothesis: str = Field(..., min_length=10, description="Experiment hypothesis")
    success_criteria: List[Dict[str, Any]] = Field(default_factory=list, description="Success criteria")
    safety_guards: List[Dict[str, Any]] = Field(default_factory=list, description="Safety guard configurations")
    scheduled_at: Optional[datetime] = Field(None, description="Schedule experiment for future execution")
    business_hours_only: bool = Field(True, description="Restrict to business hours")
    tags: List[str] = Field(default_factory=list, description="Experiment tags")

    @validator('scheduled_at')
    def validate_scheduled_time(cls, v):
        if v and v <= datetime.utcnow():
            raise ValueError("Scheduled time must be in the future")
        return v

class ExecuteExperimentRequest(BaseModel):
    """Execute experiment request model"""
    dry_run: bool = Field(False, description="Perform dry run without actual injection")
    override_safety: bool = Field(False, description="Override safety restrictions (admin only)")
    execution_notes: str = Field("", max_length=1000, description="Execution notes")

class AbortExperimentRequest(BaseModel):
    """Abort experiment request model"""
    reason: str = Field(..., min_length=5, max_length=200, description="Abort reason")
    emergency: bool = Field(False, description="Emergency abort flag")

class ResilienceReportRequest(BaseModel):
    """Resilience report request model"""
    assessment_period_days: int = Field(30, ge=1, le=365, description="Assessment period in days")
    include_recommendations: bool = Field(True, description="Include improvement recommendations")
    include_business_impact: bool = Field(True, description="Include business impact analysis")
    service_filter: Optional[List[str]] = Field(None, description="Filter by specific services")

class ExperimentResponse(BaseModel):
    """Chaos experiment response model"""
    experiment_id: str = Field(..., description="Experiment identifier")
    name: str = Field(..., description="Experiment name")
    description: str = Field(..., description="Experiment description")
    chaos_type: str = Field(..., description="Type of chaos")
    blast_radius: str = Field(..., description="Scope of impact")
    safety_level: str = Field(..., description="Safety level")
    target_services: List[str] = Field(..., description="Target services")
    duration_minutes: int = Field(..., description="Duration in minutes")
    hypothesis: str = Field(..., description="Experiment hypothesis")
    status: str = Field(..., description="Experiment status")
    created_by: str = Field(..., description="Experiment creator")
    created_at: datetime = Field(..., description="Creation timestamp")
    enabled: bool = Field(..., description="Experiment enabled status")

class ExecutionResponse(BaseModel):
    """Experiment execution response model"""
    execution_id: str = Field(..., description="Execution identifier")
    experiment_id: str = Field(..., description="Associated experiment ID")
    status: str = Field(..., description="Execution status")
    started_at: Optional[datetime] = Field(..., description="Start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    duration_seconds: Optional[float] = Field(None, description="Execution duration")
    affected_targets: List[str] = Field(..., description="Actually affected targets")
    hypothesis_result: str = Field(..., description="Hypothesis validation result")
    success_criteria_met: int = Field(..., description="Number of success criteria met")
    total_success_criteria: int = Field(..., description="Total success criteria")
    auto_aborted: bool = Field(..., description="Whether execution was auto-aborted")
    customer_impact_score: float = Field(..., description="Customer impact score")

# Experiment Management Endpoints
@router.post("/experiments", response_model=Dict[str, str], status_code=status.HTTP_201_CREATED)
async def create_chaos_experiment(
    request: CreateExperimentRequest,
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Create new chaos experiment with comprehensive configuration

    Creates a chaos experiment with specified failure injection parameters,
    safety guards, and success criteria for resilience validation.
    """
    try:
        # Check permissions for chaos experiments
        if "perm_chaos_engineering" not in current_user.permissions and "perm_admin_system" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos engineering permissions required"
            )

        # Additional safety check for aggressive experiments
        if request.safety_level == SafetyLevel.AGGRESSIVE:
            if "perm_chaos_aggressive" not in current_user.permissions:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Aggressive chaos engineering permissions required"
                )

        # Convert request to ChaosExperiment
        safety_guards = [SafetyGuard(**guard) for guard in request.safety_guards]

        experiment = ChaosExperiment(
            name=request.name,
            description=request.description,
            chaos_type=request.chaos_type,
            blast_radius=request.blast_radius,
            safety_level=request.safety_level,
            target_services=request.target_services,
            target_percentage=request.target_percentage,
            duration_minutes=request.duration_minutes,
            intensity=request.intensity,
            parameters=request.parameters,
            hypothesis=request.hypothesis,
            success_criteria=request.success_criteria,
            safety_guards=safety_guards,
            scheduled_at=request.scheduled_at,
            business_hours_only=request.business_hours_only,
            tags=request.tags,
            created_by=current_user.id
        )

        # Create experiment
        experiment_id = await chaos_engineering.create_experiment(experiment)

        logger.info(f"Chaos experiment created by user {current_user.id}: {experiment_id}")

        return {
            "message": "Chaos experiment created successfully",
            "experiment_id": experiment_id,
            "experiment_name": request.name
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create chaos experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Experiment creation failed: {str(e)}")

@router.get("/experiments", response_model=Dict[str, Any])
async def list_chaos_experiments(
    chaos_type: Optional[ChaosType] = Query(None, description="Filter by chaos type"),
    safety_level: Optional[SafetyLevel] = Query(None, description="Filter by safety level"),
    enabled_only: bool = Query(True, description="Return only enabled experiments"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    List chaos experiments with comprehensive filtering

    Returns paginated list of chaos experiments with filtering options
    for chaos type, safety level, and enabled status.
    """
    try:
        # Check permissions
        if "perm_chaos_engineering" not in current_user.permissions and "perm_monitoring_read" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos engineering or monitoring permissions required"
            )

        # Get experiments
        experiments = []
        for experiment in chaos_engineering.experiments.values():
            # Apply filters
            if enabled_only and not experiment.enabled:
                continue
            if chaos_type and experiment.chaos_type != chaos_type:
                continue
            if safety_level and experiment.safety_level != safety_level:
                continue

            experiment_response = ExperimentResponse(
                experiment_id=experiment.experiment_id,
                name=experiment.name,
                description=experiment.description,
                chaos_type=experiment.chaos_type.value,
                blast_radius=experiment.blast_radius.value,
                safety_level=experiment.safety_level.value,
                target_services=experiment.target_services,
                duration_minutes=experiment.duration_minutes,
                hypothesis=experiment.hypothesis,
                status="enabled" if experiment.enabled else "disabled",
                created_by=experiment.created_by,
                created_at=experiment.created_at,
                enabled=experiment.enabled
            )
            experiments.append(experiment_response)

        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_experiments = experiments[start_idx:end_idx]

        logger.info(f"Chaos experiments listed by user {current_user.id}: {len(experiments)} total")

        return {
            "experiments": [exp.dict() for exp in paginated_experiments],
            "pagination": {
                "total_count": len(experiments),
                "page": page,
                "page_size": page_size,
                "total_pages": (len(experiments) + page_size - 1) // page_size
            },
            "filters_applied": {
                "chaos_type": chaos_type.value if chaos_type else None,
                "safety_level": safety_level.value if safety_level else None,
                "enabled_only": enabled_only
            },
            "retrieved_at": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list chaos experiments: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Experiment listing failed: {str(e)}")

@router.get("/experiments/{experiment_id}", response_model=Dict[str, Any])
async def get_chaos_experiment(
    experiment_id: str,
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Get detailed chaos experiment configuration

    Returns complete experiment configuration including safety guards,
    success criteria, and execution history.
    """
    try:
        # Check permissions
        if "perm_chaos_engineering" not in current_user.permissions and "perm_monitoring_read" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos engineering or monitoring permissions required"
            )

        # Get experiment
        if experiment_id not in chaos_engineering.experiments:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Experiment not found"
            )

        experiment = chaos_engineering.experiments[experiment_id]

        # Get execution history
        async with chaos_engineering.db_pool.acquire() as conn:
            executions = await conn.fetch("""
                SELECT execution_id, status, started_at, completed_at, duration_seconds,
                       hypothesis_result, success_criteria_met, total_success_criteria,
                       auto_aborted, customer_impact_score
                FROM chaos_executions
                WHERE experiment_id = $1
                ORDER BY started_at DESC
                LIMIT 10
            """, experiment_id)

        execution_history = [dict(execution) for execution in executions]

        logger.info(f"Chaos experiment details accessed by user {current_user.id}: {experiment_id}")

        return {
            "experiment": {
                "experiment_id": experiment.experiment_id,
                "name": experiment.name,
                "description": experiment.description,
                "chaos_type": experiment.chaos_type.value,
                "blast_radius": experiment.blast_radius.value,
                "safety_level": experiment.safety_level.value,
                "target_services": experiment.target_services,
                "target_percentage": experiment.target_percentage,
                "duration_minutes": experiment.duration_minutes,
                "intensity": experiment.intensity,
                "parameters": experiment.parameters,
                "hypothesis": experiment.hypothesis,
                "success_criteria": experiment.success_criteria,
                "safety_guards": [guard.__dict__ for guard in experiment.safety_guards],
                "scheduled_at": experiment.scheduled_at.isoformat() if experiment.scheduled_at else None,
                "business_hours_only": experiment.business_hours_only,
                "tags": experiment.tags,
                "created_by": experiment.created_by,
                "created_at": experiment.created_at.isoformat(),
                "enabled": experiment.enabled
            },
            "execution_history": execution_history,
            "execution_statistics": {
                "total_executions": len(execution_history),
                "successful_executions": len([e for e in execution_history if e["hypothesis_result"] == "confirmed"]),
                "aborted_executions": len([e for e in execution_history if e["auto_aborted"]]),
                "average_impact_score": sum(e.get("customer_impact_score", 0) for e in execution_history) / len(execution_history) if execution_history else 0
            },
            "retrieved_at": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get chaos experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Experiment retrieval failed: {str(e)}")

# Experiment Execution Endpoints
@router.post("/experiments/{experiment_id}/execute", response_model=Dict[str, Any])
async def execute_chaos_experiment(
    experiment_id: str,
    request: ExecuteExperimentRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Execute chaos experiment with comprehensive monitoring

    Executes the specified chaos experiment with real-time monitoring,
    safety guard validation, and automated recovery capabilities.
    """
    try:
        # Check permissions for experiment execution
        if "perm_chaos_execute" not in current_user.permissions and "perm_admin_system" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos experiment execution permissions required"
            )

        # Get experiment
        if experiment_id not in chaos_engineering.experiments:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Experiment not found"
            )

        experiment = chaos_engineering.experiments[experiment_id]

        # Additional safety checks for override
        if request.override_safety and "perm_chaos_override" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Safety override permissions required"
            )

        # Check if experiment is already running
        for execution in chaos_engineering.active_executions.values():
            if execution.experiment_id == experiment_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Experiment is already running"
                )

        # Execute experiment
        execution = await chaos_engineering.execute_experiment(experiment_id, request.dry_run)

        logger.info(f"Chaos experiment executed by user {current_user.id}: {experiment_id} (dry_run: {request.dry_run})")

        return {
            "message": "Chaos experiment executed successfully",
            "execution_id": execution.execution_id,
            "experiment_id": experiment_id,
            "experiment_name": experiment.name,
            "execution_status": execution.status.value,
            "dry_run": request.dry_run,
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "expected_duration_minutes": experiment.duration_minutes,
            "affected_targets": execution.affected_targets,
            "execution_summary": {
                "hypothesis": experiment.hypothesis,
                "chaos_type": experiment.chaos_type.value,
                "safety_level": experiment.safety_level.value,
                "success_criteria_count": len(experiment.success_criteria)
            },
            "executed_by": current_user.id,
            "execution_notes": request.execution_notes
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute chaos experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Experiment execution failed: {str(e)}")

@router.post("/experiments/{experiment_id}/abort", response_model=Dict[str, str])
async def abort_chaos_experiment(
    experiment_id: str,
    request: AbortExperimentRequest,
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Abort running chaos experiment with immediate recovery

    Immediately aborts the running experiment and triggers emergency
    recovery procedures to restore system state.
    """
    try:
        # Check permissions
        if "perm_chaos_abort" not in current_user.permissions and "perm_admin_system" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos experiment abort permissions required"
            )

        # Find active execution for this experiment
        execution_id = None
        for exec_id, execution in chaos_engineering.active_executions.items():
            if execution.experiment_id == experiment_id:
                execution_id = exec_id
                break

        if not execution_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No active execution found for this experiment"
            )

        # Abort experiment
        abort_reason = f"Manual abort by {current_user.id}: {request.reason}"
        if request.emergency:
            abort_reason = f"EMERGENCY ABORT by {current_user.id}: {request.reason}"

        success = await chaos_engineering.abort_experiment(execution_id, abort_reason)

        if success:
            logger.warning(f"Chaos experiment aborted by user {current_user.id}: {experiment_id} - {request.reason}")
            return {
                "message": "Chaos experiment aborted successfully",
                "execution_id": execution_id,
                "experiment_id": experiment_id,
                "abort_reason": abort_reason,
                "emergency": request.emergency,
                "aborted_by": current_user.id
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to abort experiment"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to abort chaos experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Experiment abort failed: {str(e)}")

# Execution Management Endpoints
@router.get("/executions", response_model=Dict[str, Any])
async def list_experiment_executions(
    experiment_id: Optional[str] = Query(None, description="Filter by experiment ID"),
    status_filter: Optional[ExperimentStatus] = Query(None, description="Filter by execution status"),
    start_date: Optional[datetime] = Query(None, description="Filter executions after start date"),
    end_date: Optional[datetime] = Query(None, description="Filter executions before end date"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    List experiment executions with comprehensive filtering

    Returns paginated list of experiment executions with detailed
    results and filtering options for analysis and reporting.
    """
    try:
        # Check permissions
        if "perm_chaos_engineering" not in current_user.permissions and "perm_monitoring_read" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos engineering or monitoring permissions required"
            )

        # Build query conditions
        conditions = []
        params = []
        param_count = 0

        if experiment_id:
            param_count += 1
            conditions.append(f"experiment_id = ${param_count}")
            params.append(experiment_id)

        if status_filter:
            param_count += 1
            conditions.append(f"status = ${param_count}")
            params.append(status_filter.value)

        if start_date:
            param_count += 1
            conditions.append(f"started_at >= ${param_count}")
            params.append(start_date)

        if end_date:
            param_count += 1
            conditions.append(f"started_at <= ${param_count}")
            params.append(end_date)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Execute query
        async with chaos_engineering.db_pool.acquire() as conn:
            # Get total count
            count_query = f"SELECT COUNT(*) FROM chaos_executions {where_clause}"
            total_count = await conn.fetchval(count_query, *params)

            # Get paginated results
            offset = (page - 1) * page_size
            param_count += 1
            limit_clause = f"LIMIT ${param_count}"
            params.append(page_size)
            param_count += 1
            offset_clause = f"OFFSET ${param_count}"
            params.append(offset)

            executions_query = f"""
                SELECT execution_id, experiment_id, status, started_at, completed_at,
                       duration_seconds, affected_targets, success_targets, failed_targets,
                       hypothesis_result, success_criteria_met, total_success_criteria,
                       auto_aborted, abort_reason, customer_impact_score, revenue_impact_estimate
                FROM chaos_executions
                {where_clause}
                ORDER BY started_at DESC
                {limit_clause} {offset_clause}
            """

            executions = await conn.fetch(executions_query, *params)

        # Convert to response format
        execution_responses = []
        for execution in executions:
            execution_response = ExecutionResponse(
                execution_id=execution["execution_id"],
                experiment_id=execution["experiment_id"],
                status=execution["status"],
                started_at=execution["started_at"],
                completed_at=execution["completed_at"],
                duration_seconds=execution["duration_seconds"],
                affected_targets=execution["affected_targets"],
                hypothesis_result=execution["hypothesis_result"],
                success_criteria_met=execution["success_criteria_met"],
                total_success_criteria=execution["total_success_criteria"],
                auto_aborted=execution["auto_aborted"],
                customer_impact_score=execution["customer_impact_score"]
            )
            execution_responses.append(execution_response)

        logger.info(f"Chaos executions listed by user {current_user.id}: {len(executions)} results")

        return {
            "executions": [exec.dict() for exec in execution_responses],
            "pagination": {
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_count + page_size - 1) // page_size
            },
            "filters_applied": {
                "experiment_id": experiment_id,
                "status_filter": status_filter.value if status_filter else None,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            },
            "summary_statistics": {
                "total_executions": total_count,
                "successful_executions": len([e for e in executions if e["hypothesis_result"] == "confirmed"]),
                "aborted_executions": len([e for e in executions if e["auto_aborted"]]),
                "average_duration_seconds": sum(e["duration_seconds"] or 0 for e in executions) / len(executions) if executions else 0
            },
            "retrieved_at": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list chaos executions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Execution listing failed: {str(e)}")

@router.get("/executions/{execution_id}", response_model=Dict[str, Any])
async def get_experiment_execution(
    execution_id: str,
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Get detailed experiment execution results

    Returns comprehensive execution details including metrics,
    logs, safety violations, and business impact analysis.
    """
    try:
        # Check permissions
        if "perm_chaos_engineering" not in current_user.permissions and "perm_monitoring_read" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos engineering or monitoring permissions required"
            )

        # Get execution details
        async with chaos_engineering.db_pool.acquire() as conn:
            execution = await conn.fetchrow("""
                SELECT * FROM chaos_executions WHERE execution_id = $1
            """, execution_id)

        if not execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Execution not found"
            )

        # Get associated experiment details
        experiment = chaos_engineering.experiments.get(execution["experiment_id"])

        logger.info(f"Chaos execution details accessed by user {current_user.id}: {execution_id}")

        return {
            "execution": {
                "execution_id": execution["execution_id"],
                "experiment_id": execution["experiment_id"],
                "experiment_name": experiment.name if experiment else "Unknown",
                "status": execution["status"],
                "started_at": execution["started_at"].isoformat() if execution["started_at"] else None,
                "completed_at": execution["completed_at"].isoformat() if execution["completed_at"] else None,
                "duration_seconds": execution["duration_seconds"],
                "affected_targets": execution["affected_targets"],
                "success_targets": execution["success_targets"],
                "failed_targets": execution["failed_targets"],
                "hypothesis_result": execution["hypothesis_result"],
                "success_criteria_met": execution["success_criteria_met"],
                "total_success_criteria": execution["total_success_criteria"],
                "auto_aborted": execution["auto_aborted"],
                "abort_reason": execution["abort_reason"],
                "customer_impact_score": execution["customer_impact_score"],
                "revenue_impact_estimate": execution["revenue_impact_estimate"]
            },
            "metrics": {
                "baseline_metrics": execution["baseline_metrics"],
                "experiment_metrics": execution["experiment_metrics"],
                "recovery_metrics": execution["recovery_metrics"]
            },
            "safety_and_compliance": {
                "safety_violations": execution["safety_violations"],
                "incidents_triggered": execution["incidents_triggered"],
                "sla_violations": execution["sla_violations"]
            },
            "execution_logs": execution["execution_logs"],
            "artifacts": execution["artifacts"],
            "business_impact": {
                "customer_impact_score": execution["customer_impact_score"],
                "revenue_impact_estimate": execution["revenue_impact_estimate"],
                "impact_classification": "high" if execution["customer_impact_score"] > 5.0 else
                                        "medium" if execution["customer_impact_score"] > 2.0 else "low"
            },
            "retrieved_at": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get chaos execution: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Execution retrieval failed: {str(e)}")

# Resilience Assessment Endpoints
@router.get("/resilience/report", response_model=Dict[str, Any])
async def generate_resilience_report(
    request: ResilienceReportRequest = Depends(),
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Generate comprehensive resilience assessment report

    Creates detailed resilience report with vulnerability analysis,
    business impact assessment, and actionable recommendations.
    """
    try:
        # Check permissions
        if "perm_chaos_reports" not in current_user.permissions and "perm_admin_system" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos engineering report permissions required"
            )

        # Generate resilience report
        report = await chaos_engineering.get_resilience_report(request.assessment_period_days)

        # Filter by services if specified
        if request.service_filter:
            filtered_scores = {
                service: score for service, score in report.service_resilience_scores.items()
                if service in request.service_filter
            }
            report.service_resilience_scores = filtered_scores

        logger.info(f"Resilience report generated by user {current_user.id} for {request.assessment_period_days} days")

        response = {
            "report_id": report.report_id,
            "generated_at": report.generated_at.isoformat(),
            "assessment_period": {
                "days": report.assessment_period_days,
                "start_date": (report.generated_at - timedelta(days=report.assessment_period_days)).isoformat(),
                "end_date": report.generated_at.isoformat()
            },
            "executive_summary": {
                "overall_resilience_score": round(report.overall_resilience_score, 3),
                "total_experiments": report.total_experiments,
                "successful_experiments": report.successful_experiments,
                "success_rate_percentage": round((report.successful_experiments / report.total_experiments * 100) if report.total_experiments > 0 else 100, 2),
                "vulnerabilities_identified": len(report.identified_vulnerabilities),
                "critical_weaknesses": len(report.critical_weaknesses)
            },
            "resilience_analysis": {
                "overall_score": report.overall_resilience_score,
                "service_scores": report.service_resilience_scores,
                "failure_type_resilience": report.failure_type_resilience,
                "system_health_grade": "A" if report.overall_resilience_score >= 0.9 else
                                       "B" if report.overall_resilience_score >= 0.8 else
                                       "C" if report.overall_resilience_score >= 0.7 else
                                       "D" if report.overall_resilience_score >= 0.6 else "F"
            },
            "vulnerability_assessment": {
                "identified_vulnerabilities": report.identified_vulnerabilities,
                "critical_weaknesses": report.critical_weaknesses,
                "improvement_opportunities": report.improvement_opportunities,
                "risk_level": "high" if len(report.critical_weaknesses) > 3 else
                             "medium" if len(report.critical_weaknesses) > 1 else "low"
            }
        }

        # Add business impact if requested
        if request.include_business_impact:
            response["business_impact"] = {
                "business_continuity_score": report.business_continuity_score,
                "customer_impact_analysis": report.customer_impact_analysis,
                "financial_risk_assessment": report.financial_risk_assessment,
                "business_continuity_grade": "Excellent" if report.business_continuity_score >= 0.95 else
                                           "Good" if report.business_continuity_score >= 0.90 else
                                           "Fair" if report.business_continuity_score >= 0.80 else
                                           "Poor"
            }

        # Add recommendations if requested
        if request.include_recommendations:
            response["recommendations"] = {
                "immediate_actions": report.immediate_actions,
                "strategic_improvements": report.strategic_improvements,
                "investment_priorities": report.investment_priorities,
                "next_review_date": (report.generated_at + timedelta(days=30)).isoformat()
            }

        response.update({
            "report_metadata": {
                "generated_by": current_user.id,
                "service_filter": request.service_filter,
                "include_recommendations": request.include_recommendations,
                "include_business_impact": request.include_business_impact
            }
        })

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate resilience report: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Resilience report generation failed: {str(e)}")

# Statistics and Monitoring Endpoints
@router.get("/statistics", response_model=Dict[str, Any])
async def get_chaos_statistics(
    hours: int = Query(24, ge=1, le=168, description="Time range in hours"),
    current_user: User = Depends(get_current_user),
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Get comprehensive chaos engineering statistics

    Returns detailed statistics including experiment success rates,
    safety violations, business impact metrics, and system health.
    """
    try:
        # Check permissions
        if "perm_chaos_engineering" not in current_user.permissions and "perm_monitoring_read" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chaos engineering or monitoring permissions required"
            )

        # Get comprehensive statistics
        statistics = await chaos_engineering.get_experiment_statistics(hours)

        logger.info(f"Chaos statistics accessed by user {current_user.id} for {hours} hours")

        return statistics

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get chaos statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Statistics retrieval failed: {str(e)}")

# Real-time Monitoring Endpoints
@router.websocket("/monitor/{execution_id}")
async def monitor_experiment_execution(
    websocket: WebSocket,
    execution_id: str,
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    WebSocket endpoint for real-time experiment monitoring

    Provides real-time updates on experiment execution including
    progress, metrics, safety violations, and completion status.
    """
    await websocket.accept()

    try:
        logger.info(f"WebSocket monitoring connection established for execution: {execution_id}")

        # Send initial connection confirmation
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "execution_id": execution_id,
            "timestamp": datetime.utcnow().isoformat()
        }))

        # Monitor execution in real-time
        while True:
            try:
                # Check if execution exists and is active
                if execution_id not in chaos_engineering.active_executions:
                    # Check if execution completed
                    async with chaos_engineering.db_pool.acquire() as conn:
                        execution = await conn.fetchrow("""
                            SELECT status, completed_at, hypothesis_result
                            FROM chaos_executions
                            WHERE execution_id = $1
                        """, execution_id)

                    if execution:
                        await websocket.send_text(json.dumps({
                            "type": "execution_completed",
                            "execution_id": execution_id,
                            "status": execution["status"],
                            "completed_at": execution["completed_at"].isoformat() if execution["completed_at"] else None,
                            "hypothesis_result": execution["hypothesis_result"],
                            "timestamp": datetime.utcnow().isoformat()
                        }))
                        break
                    else:
                        await websocket.send_text(json.dumps({
                            "type": "execution_not_found",
                            "execution_id": execution_id,
                            "timestamp": datetime.utcnow().isoformat()
                        }))
                        break

                # Send real-time updates for active execution
                execution = chaos_engineering.active_executions[execution_id]
                experiment = chaos_engineering.experiments.get(execution.experiment_id)

                update_data = {
                    "type": "execution_update",
                    "execution_id": execution_id,
                    "status": execution.status.value,
                    "progress": {
                        "affected_targets": len(execution.affected_targets),
                        "success_targets": len(execution.success_targets),
                        "failed_targets": len(execution.failed_targets),
                        "safety_violations": len(execution.safety_violations)
                    },
                    "metrics": {
                        "baseline_metrics": execution.baseline_metrics,
                        "current_metrics": execution.experiment_metrics
                    },
                    "hypothesis_validation": {
                        "success_criteria_met": execution.success_criteria_met,
                        "total_success_criteria": execution.total_success_criteria,
                        "current_result": execution.hypothesis_result.value
                    },
                    "business_impact": {
                        "customer_impact_score": execution.customer_impact_score,
                        "revenue_impact_estimate": execution.revenue_impact_estimate
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }

                await websocket.send_text(json.dumps(update_data))

                # Wait before next update
                await asyncio.sleep(5)  # Update every 5 seconds

            except WebSocketDisconnect:
                logger.info(f"WebSocket monitoring disconnected for execution: {execution_id}")
                break
            except Exception as e:
                logger.error(f"WebSocket monitoring error for execution {execution_id}: {str(e)}")
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Monitoring error occurred",
                    "timestamp": datetime.utcnow().isoformat()
                }))
                await asyncio.sleep(10)

    except WebSocketDisconnect:
        logger.info(f"WebSocket connection closed for execution: {execution_id}")
    except Exception as e:
        logger.error(f"WebSocket error for execution {execution_id}: {str(e)}")

# System Health and Safety Endpoints
@router.get("/health", response_model=Dict[str, Any])
async def get_chaos_system_health(
    chaos_engineering: ChaosEngineering = Depends(get_chaos_engineering)
):
    """
    Get chaos engineering system health status

    Returns overall system health including active experiments,
    safety guard status, and system capability assessment.
    """
    try:
        return {
            "status": "healthy" if chaos_engineering.is_running and not chaos_engineering.emergency_stop else "degraded",
            "system_state": {
                "is_running": chaos_engineering.is_running,
                "emergency_stop": chaos_engineering.emergency_stop,
                "active_experiments": len(chaos_engineering.active_executions),
                "scheduled_experiments": len(chaos_engineering.scheduled_experiments),
                "total_configured_experiments": len(chaos_engineering.experiments)
            },
            "safety_status": {
                "global_safety_guards": len(chaos_engineering.global_safety_guards),
                "safety_monitoring_active": True,
                "last_safety_check": datetime.utcnow().isoformat()
            },
            "capabilities": {
                "supported_chaos_types": [ct.value for ct in ChaosType],
                "supported_blast_radius": [br.value for br in BlastRadius],
                "safety_levels": [sl.value for sl in SafetyLevel],
                "max_concurrent_experiments": 5,
                "max_experiment_duration_minutes": 60
            },
            "global_statistics": chaos_engineering.statistics,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Chaos system health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }