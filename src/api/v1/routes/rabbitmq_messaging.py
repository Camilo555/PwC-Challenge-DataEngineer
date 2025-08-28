"""
RabbitMQ Messaging API Routes

This module provides REST API endpoints for the RabbitMQ messaging system
including ML pipeline orchestration, data quality workflows, and system monitoring.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.messaging import (
    get_rabbitmq_manager,
    get_ml_integration_service,
    get_dq_message_handler,
    get_session_integration_service,
    get_dashboard_integration_service,
    get_metrics_collector,
    get_performance_optimizer,
    get_health_check_service,
    get_reliability_handler,
    EnterpriseMessage,
    MessageMetadata,
    QueueType,
    MessagePriority,
    MLTrainingRequest,
    MLInferenceRequest,
    DataQualityValidationRequest,
    initialize_messaging_system,
    get_system_status
)
from core.logging import get_logger


logger = get_logger(__name__)
router = APIRouter(prefix="/messaging", tags=["RabbitMQ Messaging"])


# Pydantic models for API requests/responses
class MessagePublishRequest(BaseModel):
    """Request model for publishing messages"""
    queue_type: str = Field(..., description="Target queue type")
    message_type: str = Field(..., description="Type of message")
    payload: Dict[str, Any] = Field(..., description="Message payload")
    priority: str = Field(default="normal", description="Message priority")
    expiration_seconds: Optional[int] = Field(None, description="Message expiration in seconds")
    correlation_id: Optional[str] = Field(None, description="Correlation ID")
    headers: Optional[Dict[str, str]] = Field(default_factory=dict, description="Additional headers")


class MessagePublishResponse(BaseModel):
    """Response model for message publishing"""
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


class MLPipelineStartRequest(BaseModel):
    """Request model for starting ML pipeline"""
    pipeline_name: str = Field(..., description="Name of the ML pipeline")
    pipeline_config: Dict[str, Any] = Field(..., description="Pipeline configuration")
    priority: str = Field(default="high", description="Pipeline priority")


class MLPipelineResponse(BaseModel):
    """Response model for ML pipeline operations"""
    pipeline_id: str
    status: str
    message: str


class DataQualityValidationRequestAPI(BaseModel):
    """API request model for data quality validation"""
    dataset_name: str = Field(..., description="Name of the dataset to validate")
    dataset_path: str = Field(..., description="Path to the dataset")
    validation_rules: List[Dict[str, Any]] = Field(..., description="Validation rules to apply")
    validation_scope: str = Field(default="full", description="Validation scope")
    async_execution: bool = Field(default=True, description="Execute validation asynchronously")
    notify_on_completion: bool = Field(default=True, description="Send notification on completion")


class UserSessionRequest(BaseModel):
    """Request model for user session management"""
    user_id: str = Field(..., description="User ID")
    session_data: Dict[str, Any] = Field(default_factory=dict, description="Session data")
    ip_address: Optional[str] = Field(None, description="User IP address")
    user_agent: Optional[str] = Field(None, description="User agent string")


class UserActivityRequest(BaseModel):
    """Request model for user activity tracking"""
    session_id: str = Field(..., description="Session ID")
    activity_type: str = Field(..., description="Type of activity")
    activity_data: Dict[str, Any] = Field(..., description="Activity data")


class DashboardUpdateRequest(BaseModel):
    """Request model for dashboard updates"""
    dashboard_id: str = Field(..., description="Dashboard ID")
    widget_id: Optional[str] = Field(None, description="Specific widget ID")
    update_data: Dict[str, Any] = Field(..., description="Update data")


class MetricPublishRequest(BaseModel):
    """Request model for publishing metrics"""
    metric_name: str = Field(..., description="Metric name")
    metric_value: float = Field(..., description="Metric value")
    tags: Optional[Dict[str, str]] = Field(default_factory=dict, description="Metric tags")


# Dependency injection for messaging components
def get_messaging_components():
    """Get messaging system components"""
    return {
        "rabbitmq_manager": get_rabbitmq_manager(),
        "ml_integration": get_ml_integration_service(),
        "dq_handler": get_dq_message_handler(),
        "session_integration": get_session_integration_service(),
        "dashboard_integration": get_dashboard_integration_service(),
        "metrics_collector": get_metrics_collector(),
        "performance_optimizer": get_performance_optimizer(),
        "health_service": get_health_check_service(),
        "reliability_handler": get_reliability_handler()
    }


# System Management Endpoints
@router.post("/system/initialize")
async def initialize_system(background_tasks: BackgroundTasks):
    """Initialize the RabbitMQ messaging system"""
    try:
        def init_task():
            result = initialize_messaging_system(
                enable_metrics=True,
                enable_health_checks=True,
                enable_ml_integration=True,
                enable_dq_integration=True,
                enable_session_integration=True,
                enable_dashboard_integration=True
            )
            logger.info(f"System initialization result: {result['status']}")
        
        background_tasks.add_task(init_task)
        
        return JSONResponse(
            status_code=202,
            content={
                "message": "System initialization started",
                "status": "processing"
            }
        )
        
    except Exception as e:
        logger.error(f"Error initializing system: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system/status")
async def get_messaging_system_status():
    """Get comprehensive system status"""
    try:
        status = get_system_status()
        return JSONResponse(content=status)
        
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system/health")
async def health_check(components: dict = Depends(get_messaging_components)):
    """Perform health check on messaging system"""
    try:
        health_service = components["health_service"]
        health_result = health_service.perform_health_check()
        
        status_code = 200
        if health_result["overall_status"] == "degraded":
            status_code = 206  # Partial Content
        elif health_result["overall_status"] == "unhealthy":
            status_code = 503  # Service Unavailable
        
        return JSONResponse(
            status_code=status_code,
            content=health_result
        )
        
    except Exception as e:
        logger.error(f"Error in health check: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Message Publishing Endpoints
@router.post("/messages/publish", response_model=MessagePublishResponse)
async def publish_message(
    request: MessagePublishRequest,
    components: dict = Depends(get_messaging_components)
):
    """Publish a message to RabbitMQ"""
    try:
        rabbitmq_manager = components["rabbitmq_manager"]
        reliability_handler = components["reliability_handler"]
        
        # Create message
        try:
            queue_type = QueueType(request.queue_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid queue type: {request.queue_type}")
        
        try:
            priority = MessagePriority[request.priority.upper()]
        except KeyError:
            priority = MessagePriority.NORMAL
        
        message = EnterpriseMessage(
            queue_type=queue_type,
            message_type=request.message_type,
            payload=request.payload,
            metadata=MessageMetadata(
                priority=priority,
                correlation_id=request.correlation_id,
                expiration=request.expiration_seconds,
                headers=request.headers
            )
        )
        
        # Publish with reliability features
        success = reliability_handler.publish_reliable_message(
            message=message,
            enable_deduplication=True,
            enable_circuit_breaker=True
        )
        
        if success:
            return MessagePublishResponse(
                success=True,
                message_id=message.metadata.message_id
            )
        else:
            return MessagePublishResponse(
                success=False,
                error="Failed to publish message"
            )
        
    except Exception as e:
        logger.error(f"Error publishing message: {e}")
        return MessagePublishResponse(
            success=False,
            error=str(e)
        )


@router.get("/queues/{queue_name}/stats")
async def get_queue_stats(
    queue_name: str,
    components: dict = Depends(get_messaging_components)
):
    """Get statistics for a specific queue"""
    try:
        rabbitmq_manager = components["rabbitmq_manager"]
        
        try:
            queue_type = QueueType(queue_name)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid queue name: {queue_name}")
        
        stats = rabbitmq_manager.get_queue_stats(queue_type)
        return JSONResponse(content=stats)
        
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ML Pipeline Endpoints
@router.post("/ml/pipeline/start", response_model=MLPipelineResponse)
async def start_ml_pipeline(
    request: MLPipelineStartRequest,
    components: dict = Depends(get_messaging_components)
):
    """Start an ML pipeline"""
    try:
        ml_integration = components["ml_integration"]
        
        try:
            priority = MessagePriority[request.priority.upper()]
        except KeyError:
            priority = MessagePriority.HIGH
        
        pipeline_id = ml_integration.start_ml_pipeline(
            pipeline_name=request.pipeline_name,
            pipeline_config=request.pipeline_config,
            priority=priority
        )
        
        return MLPipelineResponse(
            pipeline_id=pipeline_id,
            status="started",
            message="ML pipeline started successfully"
        )
        
    except Exception as e:
        logger.error(f"Error starting ML pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ml/pipeline/{pipeline_id}/status")
async def get_ml_pipeline_status(
    pipeline_id: str,
    components: dict = Depends(get_messaging_components)
):
    """Get ML pipeline status"""
    try:
        ml_integration = components["ml_integration"]
        
        # Get pipeline status
        if pipeline_id in ml_integration.active_pipelines:
            pipeline_info = ml_integration.active_pipelines[pipeline_id]
            return JSONResponse(content={
                "pipeline_id": pipeline_id,
                "status": pipeline_info["status"],
                "pipeline_name": pipeline_info["pipeline_name"],
                "started_at": pipeline_info["started_at"].isoformat(),
                "stages": pipeline_info.get("stages", [])
            })
        else:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ml/training/submit")
async def submit_ml_training(
    request: Dict[str, Any],
    components: dict = Depends(get_messaging_components)
):
    """Submit ML training job"""
    try:
        ml_integration = components["ml_integration"]
        ml_handler = ml_integration.ml_handler
        
        # Create training request
        training_request = MLTrainingRequest(**request)
        
        job_id = await ml_handler.submit_training_job_async(training_request)
        
        return JSONResponse(content={
            "job_id": job_id,
            "status": "submitted",
            "message": "Training job submitted successfully"
        })
        
    except Exception as e:
        logger.error(f"Error submitting training job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ml/inference/request")
async def submit_ml_inference(
    request: Dict[str, Any],
    components: dict = Depends(get_messaging_components)
):
    """Submit ML inference request"""
    try:
        ml_integration = components["ml_integration"]
        ml_handler = ml_integration.ml_handler
        
        # Create inference request
        inference_request = MLInferenceRequest(**request)
        
        response = await ml_handler.submit_inference_request_async(inference_request)
        
        return JSONResponse(content=response.to_dict())
        
    except Exception as e:
        logger.error(f"Error submitting inference request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Data Quality Endpoints
@router.post("/data-quality/validation/submit")
async def submit_data_quality_validation(
    request: DataQualityValidationRequestAPI,
    components: dict = Depends(get_messaging_components)
):
    """Submit data quality validation request"""
    try:
        dq_handler = components["dq_handler"]
        
        # Create validation request
        from src.messaging.data_quality_handlers import DataQualityValidationRequest, DataQualityRule
        
        rules = []
        for rule_data in request.validation_rules:
            rule = DataQualityRule(**rule_data)
            rules.append(rule)
        
        validation_request = DataQualityValidationRequest(
            dataset_name=request.dataset_name,
            dataset_path=request.dataset_path,
            validation_rules=rules,
            validation_scope=request.validation_scope,
            async_execution=request.async_execution,
            notify_on_completion=request.notify_on_completion
        )
        
        job_id = await dq_handler.submit_validation_request_async(validation_request)
        
        return JSONResponse(content={
            "job_id": job_id,
            "status": "submitted",
            "message": "Data quality validation submitted successfully"
        })
        
    except Exception as e:
        logger.error(f"Error submitting data quality validation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-quality/validation/{job_id}/status")
async def get_data_quality_validation_status(
    job_id: str,
    components: dict = Depends(get_messaging_components)
):
    """Get data quality validation status"""
    try:
        dq_handler = components["dq_handler"]
        
        job_status = dq_handler.get_validation_job_status(job_id)
        
        if job_status:
            return JSONResponse(content=job_status)
        else:
            raise HTTPException(status_code=404, detail="Validation job not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting validation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# User Session Endpoints
@router.post("/sessions/create")
async def create_user_session(
    request: UserSessionRequest,
    components: dict = Depends(get_messaging_components)
):
    """Create new user session"""
    try:
        session_integration = components["session_integration"]
        
        session_id = session_integration.create_user_session(
            user_id=request.user_id,
            session_data=request.session_data,
            ip_address=request.ip_address,
            user_agent=request.user_agent
        )
        
        return JSONResponse(content={
            "session_id": session_id,
            "status": "created",
            "message": "User session created successfully"
        })
        
    except Exception as e:
        logger.error(f"Error creating user session: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sessions/{session_id}/activity")
async def track_user_activity(
    session_id: str,
    request: UserActivityRequest,
    components: dict = Depends(get_messaging_components)
):
    """Track user activity"""
    try:
        session_integration = components["session_integration"]
        
        success = session_integration.track_user_activity(
            session_id=session_id,
            activity_type=request.activity_type,
            activity_data=request.activity_data
        )
        
        if success:
            return JSONResponse(content={
                "status": "tracked",
                "message": "User activity tracked successfully"
            })
        else:
            raise HTTPException(status_code=404, detail="Session not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error tracking user activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/sessions/{session_id}")
async def expire_user_session(
    session_id: str,
    reason: str = Query(default="manual", description="Reason for session expiration"),
    components: dict = Depends(get_messaging_components)
):
    """Expire user session"""
    try:
        session_integration = components["session_integration"]
        
        success = session_integration.expire_session(session_id, reason)
        
        if success:
            return JSONResponse(content={
                "status": "expired",
                "message": "User session expired successfully"
            })
        else:
            raise HTTPException(status_code=404, detail="Session not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error expiring user session: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Dashboard and Real-time Updates Endpoints
@router.post("/dashboard/update")
async def publish_dashboard_update(
    request: DashboardUpdateRequest,
    components: dict = Depends(get_messaging_components)
):
    """Publish dashboard update"""
    try:
        dashboard_integration = components["dashboard_integration"]
        
        success = dashboard_integration.publish_dashboard_update(
            dashboard_id=request.dashboard_id,
            update_data=request.update_data,
            widget_id=request.widget_id
        )
        
        if success:
            return JSONResponse(content={
                "status": "published",
                "message": "Dashboard update published successfully"
            })
        else:
            raise HTTPException(status_code=500, detail="Failed to publish dashboard update")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error publishing dashboard update: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/metrics/publish")
async def publish_metric(
    request: MetricPublishRequest,
    components: dict = Depends(get_messaging_components)
):
    """Publish real-time metric"""
    try:
        dashboard_integration = components["dashboard_integration"]
        
        success = dashboard_integration.publish_realtime_metric(
            metric_name=request.metric_name,
            metric_value=request.metric_value,
            tags=request.tags
        )
        
        if success:
            return JSONResponse(content={
                "status": "published",
                "message": "Metric published successfully"
            })
        else:
            raise HTTPException(status_code=500, detail="Failed to publish metric")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error publishing metric: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Monitoring and Analytics Endpoints
@router.get("/metrics/current")
async def get_current_metrics(components: dict = Depends(get_messaging_components)):
    """Get current system metrics"""
    try:
        metrics_collector = components["metrics_collector"]
        current_metrics = metrics_collector.get_current_metrics()
        
        return JSONResponse(content=current_metrics)
        
    except Exception as e:
        logger.error(f"Error getting current metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/summary")
async def get_performance_summary(components: dict = Depends(get_messaging_components)):
    """Get performance summary"""
    try:
        metrics_collector = components["metrics_collector"]
        performance_summary = metrics_collector.get_performance_summary()
        
        return JSONResponse(content=performance_summary)
        
    except Exception as e:
        logger.error(f"Error getting performance summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/analysis")
async def get_performance_analysis(components: dict = Depends(get_messaging_components)):
    """Get performance analysis and optimization recommendations"""
    try:
        performance_optimizer = components["performance_optimizer"]
        analysis = performance_optimizer.analyze_performance()
        
        return JSONResponse(content=analysis)
        
    except Exception as e:
        logger.error(f"Error getting performance analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/recommendations")
async def get_optimization_recommendations(components: dict = Depends(get_messaging_components)):
    """Get optimization recommendations"""
    try:
        performance_optimizer = components["performance_optimizer"]
        recommendations = performance_optimizer.get_optimization_recommendations()
        
        return JSONResponse(content={
            "recommendations": recommendations,
            "timestamp": datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting optimization recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reliability/metrics")
async def get_reliability_metrics(components: dict = Depends(get_messaging_components)):
    """Get reliability metrics"""
    try:
        reliability_handler = components["reliability_handler"]
        reliability_metrics = reliability_handler.get_reliability_metrics()
        
        return JSONResponse(content=reliability_metrics)
        
    except Exception as e:
        logger.error(f"Error getting reliability metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))