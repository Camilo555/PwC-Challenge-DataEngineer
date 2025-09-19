"""
APM Monitoring API Routes
=========================

API endpoints for Application Performance Monitoring with comprehensive
performance tracking, distributed tracing, and system health monitoring.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import json
import io

from monitoring.comprehensive_apm_system import (
    get_apm_system,
    ComprehensiveAPMSystem,
    TransactionType,
    PerformanceLevel,
    start_transaction,
    finish_transaction,
    trace_operation,
    finish_trace_operation
)
from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/apm", tags=["apm", "monitoring", "performance"])


# Request/Response Models
class StartTransactionRequest(BaseModel):
    """Request model for starting a business transaction"""
    operation_name: str = Field(..., description="Name of the operation")
    transaction_type: str = Field("api_request", description="Type of transaction: api_request, database_query, external_service, background_task, etl_process, user_session")
    user_id: Optional[str] = Field(None, description="User ID for the transaction")
    session_id: Optional[str] = Field(None, description="Session ID for the transaction")
    business_context: Optional[Dict[str, Any]] = Field(None, description="Business context for the transaction")
    sla_target_ms: Optional[float] = Field(None, description="SLA target in milliseconds")


class StartTraceRequest(BaseModel):
    """Request model for starting a performance trace"""
    operation_name: str = Field(..., description="Name of the operation to trace")
    parent_span_id: Optional[str] = Field(None, description="Parent span ID for nested tracing")
    tags: Optional[Dict[str, Any]] = Field(None, description="Tags to attach to the trace")


class FinishTraceRequest(BaseModel):
    """Request model for finishing a performance trace"""
    status: str = Field("success", description="Status of the trace: success, error, timeout")
    error: Optional[str] = Field(None, description="Error message if status is error")
    tags: Optional[Dict[str, Any]] = Field(None, description="Additional tags to attach")


class PerformanceQueryRequest(BaseModel):
    """Request model for querying performance data"""
    start_time: Optional[datetime] = Field(None, description="Start time for the query")
    end_time: Optional[datetime] = Field(None, description="End time for the query")
    operation_filter: Optional[str] = Field(None, description="Filter by operation name")
    transaction_type: Optional[str] = Field(None, description="Filter by transaction type")
    performance_level: Optional[str] = Field(None, description="Filter by performance level")
    include_traces: bool = Field(False, description="Include detailed trace information")
    limit: int = Field(100, description="Maximum number of results", ge=1, le=1000)


# Transaction Management Endpoints

@router.post("/transactions/start", response_model=dict, status_code=status.HTTP_201_CREATED)
async def start_business_transaction(
    transaction_request: StartTransactionRequest,
    apm_system: ComprehensiveAPMSystem = Depends(get_apm_system),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Start a new business transaction for performance tracking

    Initiates comprehensive performance monitoring for business operations including:
    - End-to-end transaction timing
    - Resource utilization tracking
    - SLA compliance monitoring
    - Business context correlation
    - Distributed tracing support
    """
    try:
        # Validate transaction type
        try:
            transaction_type_enum = TransactionType(transaction_request.transaction_type.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid transaction type. Supported types: {[tt.value for tt in TransactionType]}"
            )

        # Start the transaction
        transaction_id = await apm_system.start_transaction(
            transaction_type=transaction_type_enum,
            operation_name=transaction_request.operation_name,
            user_id=transaction_request.user_id,
            session_id=transaction_request.session_id,
            business_context=transaction_request.business_context,
            sla_target_ms=transaction_request.sla_target_ms
        )

        logger.info(f"Started business transaction: {transaction_id}")

        return {
            "message": "Business transaction started successfully",
            "transaction_id": transaction_id,
            "operation_name": transaction_request.operation_name,
            "transaction_type": transaction_type_enum.value,
            "start_time": datetime.utcnow().isoformat(),
            "sla_target_ms": transaction_request.sla_target_ms,
            "monitoring_features": {
                "performance_tracking": True,
                "resource_monitoring": True,
                "sla_compliance": True,
                "distributed_tracing": True,
                "business_context": bool(transaction_request.business_context)
            },
            "started_by": current_user["sub"],
            "status": "active"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start business transaction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start business transaction"
        )


@router.post("/transactions/{transaction_id}/finish", response_model=dict)
async def finish_business_transaction(
    transaction_id: str,
    apm_system: ComprehensiveAPMSystem = Depends(get_apm_system),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Finish a business transaction and generate performance report

    Completes transaction tracking and provides comprehensive analysis including:
    - Total transaction duration
    - Performance level assessment
    - SLA compliance status
    - Resource utilization summary
    - Recommendation generation
    """
    try:
        if transaction_id not in apm_system.business_transactions:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transaction {transaction_id} not found or already completed"
            )

        # Get transaction data before finishing
        transaction = apm_system.business_transactions[transaction_id]
        start_time = transaction.start_time

        # Finish the transaction
        await apm_system.finish_transaction(transaction_id)

        # Get updated transaction data
        end_time = datetime.utcnow()
        duration_ms = (end_time - start_time).total_seconds() * 1000

        logger.info(f"Finished business transaction: {transaction_id} in {duration_ms:.2f}ms")

        return {
            "message": "Business transaction completed successfully",
            "transaction_id": transaction_id,
            "performance_summary": {
                "duration_ms": duration_ms,
                "performance_level": transaction.performance_level.value if hasattr(transaction, 'performance_level') else "unknown",
                "sla_met": transaction.sla_met if hasattr(transaction, 'sla_met') and transaction.sla_met is not None else None,
                "sla_target_ms": transaction.sla_target_ms,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            },
            "spans_collected": len(transaction.spans),
            "business_context": transaction.business_context,
            "recommendations": _generate_transaction_recommendations(transaction, duration_ms),
            "finished_by": current_user["sub"],
            "status": "completed"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to finish business transaction {transaction_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to finish business transaction: {transaction_id}"
        )


def _generate_transaction_recommendations(transaction, duration_ms: float) -> List[Dict[str, str]]:
    """Generate recommendations based on transaction performance"""
    recommendations = []

    if duration_ms > 1000:  # 1 second
        recommendations.append({
            "type": "performance",
            "priority": "high",
            "title": "Long Transaction Duration",
            "description": f"Transaction took {duration_ms:.0f}ms, which may impact user experience",
            "action": "Consider optimizing database queries or implementing caching"
        })

    if transaction.sla_target_ms and duration_ms > transaction.sla_target_ms * 1.5:
        recommendations.append({
            "type": "sla_compliance",
            "priority": "critical",
            "title": "SLA Target Exceeded",
            "description": f"Transaction exceeded SLA target by {((duration_ms / transaction.sla_target_ms) - 1) * 100:.0f}%",
            "action": "Immediate performance optimization required to meet SLA commitments"
        })

    if len(transaction.spans) > 20:
        recommendations.append({
            "type": "complexity",
            "priority": "medium",
            "title": "High Transaction Complexity",
            "description": f"Transaction contains {len(transaction.spans)} spans, indicating high complexity",
            "action": "Consider breaking down into smaller, more manageable operations"
        })

    return recommendations


# Tracing Endpoints

@router.post("/traces/start", response_model=dict, status_code=status.HTTP_201_CREATED)
async def start_performance_trace(
    trace_request: StartTraceRequest,
    apm_system: ComprehensiveAPMSystem = Depends(get_apm_system),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Start a new performance trace

    Initiates detailed performance tracing for specific operations including:
    - Method-level execution timing
    - Resource usage during operation
    - Distributed tracing across services
    - Custom tagging and context
    """
    try:
        span_id = await apm_system.start_trace(
            operation_name=trace_request.operation_name,
            parent_span_id=trace_request.parent_span_id,
            tags=trace_request.tags
        )

        return {
            "message": "Performance trace started successfully",
            "span_id": span_id,
            "operation_name": trace_request.operation_name,
            "parent_span_id": trace_request.parent_span_id,
            "start_time": datetime.utcnow().isoformat(),
            "tags": trace_request.tags or {},
            "trace_features": {
                "distributed_tracing": True,
                "resource_tracking": True,
                "custom_tagging": True,
                "error_tracking": True
            }
        }

    except Exception as e:
        logger.error(f"Failed to start performance trace: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start performance trace"
        )


@router.post("/traces/{span_id}/finish", response_model=dict)
async def finish_performance_trace(
    span_id: str,
    finish_request: FinishTraceRequest,
    apm_system: ComprehensiveAPMSystem = Depends(get_apm_system),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Finish a performance trace and get execution summary

    Completes trace tracking and provides detailed performance analysis including:
    - Execution duration and timing
    - Resource utilization during trace
    - Error information if applicable
    - Performance optimization suggestions
    """
    try:
        if span_id not in apm_system.active_traces:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Active trace {span_id} not found"
            )

        # Get trace info before finishing
        trace = apm_system.active_traces[span_id]
        start_time = trace.start_time

        # Finish the trace
        await apm_system.finish_trace(
            span_id=span_id,
            status=finish_request.status,
            error=finish_request.error,
            tags=finish_request.tags
        )

        # Calculate duration
        end_time = datetime.utcnow()
        duration_ms = (end_time - start_time).total_seconds() * 1000

        return {
            "message": "Performance trace completed successfully",
            "span_id": span_id,
            "execution_summary": {
                "operation_name": trace.operation_name,
                "duration_ms": duration_ms,
                "status": finish_request.status,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "error": finish_request.error
            },
            "trace_context": {
                "trace_id": trace.trace_id,
                "parent_span_id": trace.parent_span_id,
                "tags": {**trace.tags, **(finish_request.tags or {})}
            },
            "performance_analysis": {
                "performance_level": _assess_trace_performance(duration_ms),
                "optimization_suggestions": _generate_trace_optimization_suggestions(duration_ms, finish_request.status)
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to finish performance trace {span_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to finish performance trace: {span_id}"
        )


def _assess_trace_performance(duration_ms: float) -> str:
    """Assess trace performance level"""
    if duration_ms <= 10:
        return "excellent"
    elif duration_ms <= 50:
        return "good"
    elif duration_ms <= 200:
        return "fair"
    elif duration_ms <= 1000:
        return "poor"
    else:
        return "critical"


def _generate_trace_optimization_suggestions(duration_ms: float, status: str) -> List[str]:
    """Generate optimization suggestions for trace"""
    suggestions = []

    if status == "error":
        suggestions.append("Review error handling and implement proper exception management")

    if duration_ms > 1000:
        suggestions.append("Consider implementing caching or optimizing algorithm complexity")

    if duration_ms > 100:
        suggestions.append("Analyze database queries and consider indexing improvements")

    if duration_ms <= 10:
        suggestions.append("Excellent performance - consider this as a baseline for similar operations")

    return suggestions


# Performance Analytics Endpoints

@router.get("/performance/summary", response_model=dict)
async def get_performance_summary(
    time_range_hours: int = 1,
    include_resource_metrics: bool = True,
    include_database_metrics: bool = True,
    apm_system: ComprehensiveAPMSystem = Depends(get_apm_system),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get comprehensive performance summary

    Provides enterprise-grade performance analytics including:
    - Overall system performance metrics
    - Resource utilization trends
    - Database performance analysis
    - Business transaction insights
    - Performance anomaly detection
    - System health assessment
    """
    try:
        performance_summary = await apm_system.get_performance_summary()

        # Enhance with time-based analysis
        enhanced_summary = {
            **performance_summary,
            "analysis_parameters": {
                "time_range_hours": time_range_hours,
                "include_resource_metrics": include_resource_metrics,
                "include_database_metrics": include_database_metrics,
                "analysis_timestamp": datetime.utcnow().isoformat()
            },
            "performance_insights": {
                "trending": _analyze_performance_trends(apm_system),
                "anomalies": _detect_performance_anomalies(apm_system),
                "recommendations": _generate_system_recommendations(performance_summary),
                "capacity_analysis": _analyze_system_capacity(apm_system)
            }
        }

        # Filter out sections based on parameters
        if not include_resource_metrics:
            enhanced_summary.pop("resource_metrics", None)

        if not include_database_metrics:
            enhanced_summary.pop("database_performance", None)

        return enhanced_summary

    except Exception as e:
        logger.error(f"Failed to get performance summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve performance summary"
        )


def _analyze_performance_trends(apm_system: ComprehensiveAPMSystem) -> Dict[str, Any]:
    """Analyze performance trends"""
    try:
        recent_traces = list(apm_system.completed_traces)[-100:]
        if len(recent_traces) < 10:
            return {"message": "Insufficient data for trend analysis"}

        # Split into two halves for comparison
        mid_point = len(recent_traces) // 2
        first_half = recent_traces[:mid_point]
        second_half = recent_traces[mid_point:]

        first_avg = sum(t.duration_ms for t in first_half if t.duration_ms) / len(first_half)
        second_avg = sum(t.duration_ms for t in second_half if t.duration_ms) / len(second_half)

        trend_direction = "improving" if second_avg < first_avg else "degrading"
        change_percent = abs((second_avg - first_avg) / first_avg * 100) if first_avg > 0 else 0

        return {
            "trend_direction": trend_direction,
            "change_percent": change_percent,
            "first_period_avg_ms": first_avg,
            "second_period_avg_ms": second_avg,
            "confidence": "high" if len(recent_traces) >= 50 else "medium"
        }

    except Exception as e:
        logger.error(f"Failed to analyze performance trends: {e}")
        return {"error": str(e)}


def _detect_performance_anomalies(apm_system: ComprehensiveAPMSystem) -> List[Dict[str, Any]]:
    """Detect performance anomalies"""
    anomalies = []

    try:
        # Resource anomalies
        if apm_system.resource_history and len(apm_system.resource_history) >= 5:
            recent_resources = list(apm_system.resource_history)[-5:]
            avg_cpu = sum(r.cpu_percent for r in recent_resources) / len(recent_resources)
            avg_memory = sum(r.memory_percent for r in recent_resources) / len(recent_resources)

            if avg_cpu > 90:
                anomalies.append({
                    "type": "resource",
                    "severity": "critical",
                    "description": f"Sustained high CPU usage: {avg_cpu:.1f}%",
                    "recommendation": "Investigate CPU-intensive processes"
                })

            if avg_memory > 95:
                anomalies.append({
                    "type": "resource",
                    "severity": "critical",
                    "description": f"Critical memory usage: {avg_memory:.1f}%",
                    "recommendation": "Check for memory leaks or scale resources"
                })

        # Trace performance anomalies
        recent_traces = list(apm_system.completed_traces)[-50:]
        if recent_traces:
            durations = [t.duration_ms for t in recent_traces if t.duration_ms]
            if durations:
                avg_duration = sum(durations) / len(durations)
                max_duration = max(durations)

                if max_duration > avg_duration * 10:  # 10x slower than average
                    anomalies.append({
                        "type": "performance",
                        "severity": "high",
                        "description": f"Extreme performance outlier detected: {max_duration:.0f}ms vs {avg_duration:.0f}ms average",
                        "recommendation": "Investigate slow operations and optimize"
                    })

    except Exception as e:
        logger.error(f"Failed to detect performance anomalies: {e}")

    return anomalies


def _generate_system_recommendations(performance_summary: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate system-level recommendations"""
    recommendations = []

    try:
        # System health recommendations
        health_score = performance_summary.get("system_health", {}).get("overall_health_score", 100)

        if health_score < 70:
            recommendations.append({
                "priority": "critical",
                "category": "system_health",
                "title": "System Health Below Acceptable Level",
                "description": f"Overall system health score: {health_score:.1f}%",
                "action": "Immediate investigation and optimization required"
            })

        # Resource recommendations
        resource_metrics = performance_summary.get("resource_metrics", {})
        if resource_metrics:
            cpu_percent = resource_metrics.get("cpu_percent", 0)
            memory_percent = resource_metrics.get("memory_percent", 0)

            if cpu_percent > 80:
                recommendations.append({
                    "priority": "high",
                    "category": "resources",
                    "title": "High CPU Utilization",
                    "description": f"CPU usage at {cpu_percent:.1f}%",
                    "action": "Consider horizontal scaling or CPU optimization"
                })

            if memory_percent > 85:
                recommendations.append({
                    "priority": "high",
                    "category": "resources",
                    "title": "High Memory Utilization",
                    "description": f"Memory usage at {memory_percent:.1f}%",
                    "action": "Investigate memory usage patterns and optimize"
                })

        # Database recommendations
        db_performance = performance_summary.get("database_performance", {})
        if db_performance:
            slow_queries = db_performance.get("slow_queries_count", 0)
            if slow_queries > 0:
                recommendations.append({
                    "priority": "medium",
                    "category": "database",
                    "title": "Slow Database Queries Detected",
                    "description": f"{slow_queries} slow queries identified",
                    "action": "Review and optimize database queries and indexes"
                })

    except Exception as e:
        logger.error(f"Failed to generate system recommendations: {e}")

    return recommendations


def _analyze_system_capacity(apm_system: ComprehensiveAPMSystem) -> Dict[str, Any]:
    """Analyze system capacity and utilization"""
    try:
        capacity_analysis = {
            "current_utilization": {},
            "capacity_headroom": {},
            "scaling_recommendations": []
        }

        # Resource capacity analysis
        if amp_system.resource_history:
            latest = apm_system.resource_history[-1]

            capacity_analysis["current_utilization"] = {
                "cpu_percent": latest.cpu_percent,
                "memory_percent": latest.memory_percent,
                "active_threads": latest.active_threads
            }

            capacity_analysis["capacity_headroom"] = {
                "cpu_headroom_percent": 100 - latest.cpu_percent,
                "memory_headroom_percent": 100 - latest.memory_percent,
                "estimated_capacity_remaining": "high" if latest.cpu_percent < 60 else "medium" if latest.cpu_percent < 80 else "low"
            }

            # Scaling recommendations
            if latest.cpu_percent > 80 or latest.memory_percent > 80:
                capacity_analysis["scaling_recommendations"].append({
                    "type": "horizontal_scaling",
                    "reason": "High resource utilization detected",
                    "urgency": "high" if latest.cpu_percent > 90 or latest.memory_percent > 90 else "medium"
                })

        # Transaction capacity analysis
        active_transactions = len(apm_system.business_transactions)
        if active_transactions > 50:  # Arbitrary threshold
            capacity_analysis["scaling_recommendations"].append({
                "type": "transaction_processing",
                "reason": f"High number of active transactions: {active_transactions}",
                "urgency": "medium"
            })

        return capacity_analysis

    except Exception as e:
        logger.error(f"Failed to analyze system capacity: {e}")
        return {"error": str(e)}


@router.post("/performance/query", response_model=dict)
async def query_performance_data(
    query_request: PerformanceQueryRequest,
    apm_system: ComprehensiveAPMSystem = Depends(get_apm_system),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Query historical performance data with advanced filtering

    Provides flexible querying of performance data including:
    - Time-based filtering and aggregation
    - Operation and transaction type filtering
    - Performance level classification
    - Detailed trace information
    - Statistical analysis
    """
    try:
        # Set default time range if not provided
        end_time = query_request.end_time or datetime.utcnow()
        start_time = query_request.start_time or (end_time - timedelta(hours=1))

        # Get relevant traces
        all_traces = list(apm_system.completed_traces)

        # Apply time filter
        filtered_traces = [
            trace for trace in all_traces
            if start_time <= trace.start_time <= end_time
        ]

        # Apply operation filter
        if query_request.operation_filter:
            filtered_traces = [
                trace for trace in filtered_traces
                if query_request.operation_filter.lower() in trace.operation_name.lower()
            ]

        # Apply performance level filter
        if query_request.performance_level:
            filtered_traces = [
                trace for trace in filtered_traces
                if _assess_trace_performance(trace.duration_ms or 0) == query_request.performance_level.lower()
            ]

        # Apply limit
        filtered_traces = filtered_traces[-query_request.limit:]

        # Prepare response
        query_results = {
            "query_parameters": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "operation_filter": query_request.operation_filter,
                "performance_level": query_request.performance_level,
                "include_traces": query_request.include_traces,
                "limit": query_request.limit
            },
            "results_summary": {
                "total_traces_found": len(filtered_traces),
                "time_range_hours": (end_time - start_time).total_seconds() / 3600,
                "filters_applied": bool(query_request.operation_filter or query_request.performance_level)
            },
            "statistical_analysis": {},
            "traces": []
        }

        # Calculate statistics
        if filtered_traces:
            durations = [t.duration_ms for t in filtered_traces if t.duration_ms]
            if durations:
                query_results["statistical_analysis"] = {
                    "count": len(durations),
                    "avg_duration_ms": sum(durations) / len(durations),
                    "min_duration_ms": min(durations),
                    "max_duration_ms": max(durations),
                    "p50_duration_ms": sorted(durations)[len(durations) // 2],
                    "p95_duration_ms": sorted(durations)[int(len(durations) * 0.95)],
                    "p99_duration_ms": sorted(durations)[int(len(durations) * 0.99)],
                    "error_count": len([t for t in filtered_traces if t.status == "error"]),
                    "error_rate_percent": len([t for t in filtered_traces if t.status == "error"]) / len(filtered_traces) * 100
                }

        # Include trace details if requested
        if query_request.include_traces:
            query_results["traces"] = [
                {
                    "trace_id": trace.trace_id,
                    "span_id": trace.span_id,
                    "operation_name": trace.operation_name,
                    "duration_ms": trace.duration_ms,
                    "start_time": trace.start_time.isoformat(),
                    "end_time": trace.end_time.isoformat() if trace.end_time else None,
                    "status": trace.status,
                    "error": trace.error,
                    "tags": trace.tags,
                    "performance_level": _assess_trace_performance(trace.duration_ms or 0)
                }
                for trace in filtered_traces
            ]

        return query_results

    except Exception as e:
        logger.error(f"Failed to query performance data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to query performance data"
        )


# System Health Endpoints

@router.get("/health", response_model=dict)
async def get_apm_system_health(
    apm_system: ComprehensiveAPMSystem = Depends(get_apm_system),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get APM system health status

    Returns comprehensive health information about the APM system including:
    - Monitoring system status
    - Data collection health
    - Resource utilization
    - Performance baselines
    - System capacity
    """
    try:
        health_status = {
            "system_status": {
                "monitoring_enabled": apm_system.monitoring_enabled,
                "active_traces": len(apm_system.active_traces),
                "completed_traces": len(amp_system.completed_traces),
                "active_transactions": len(apm_system.business_transactions),
                "database_metrics_tracked": len(apm_system.database_metrics)
            },
            "data_collection": {
                "sampling_rate": apm_system.sampling_rate,
                "trace_timeout_seconds": apm_system.trace_timeout_seconds,
                "resource_collection_interval_seconds": apm_system.resource_collection_interval,
                "redis_distributed_tracing": apm_system.redis_client is not None
            },
            "performance_baselines": apm_system.performance_baselines,
            "resource_health": {},
            "monitoring_tasks": {
                "total_background_tasks": len(apm_system._monitoring_tasks),
                "tasks_running": sum(1 for task in apm_system._monitoring_tasks if not task.done())
            },
            "last_health_check": datetime.utcnow().isoformat()
        }

        # Add resource health if available
        if apm_system.resource_history:
            latest_resource = apm_system.resource_history[-1]
            health_status["resource_health"] = {
                "cpu_percent": latest_resource.cpu_percent,
                "memory_percent": latest_resource.memory_percent,
                "memory_mb": latest_resource.memory_mb,
                "active_threads": latest_resource.active_threads,
                "last_collected": latest_resource.timestamp.isoformat()
            }

        return health_status

    except Exception as e:
        logger.error(f"Failed to get APM system health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve APM system health"
        )