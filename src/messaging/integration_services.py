"""
RabbitMQ Integration Services

This module provides integration services that connect RabbitMQ messaging
with various enterprise components including:
- ML pipelines and training orchestration
- Data quality validation workflows
- User session management
- Real-time dashboard updates
- Alert and notification systems
"""

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Union
import threading
from concurrent.futures import ThreadPoolExecutor
import schedule

from .enterprise_rabbitmq_manager import (
    EnterpriseRabbitMQManager, EnterpriseMessage, MessageMetadata,
    QueueType, MessagePriority, get_rabbitmq_manager
)
from .message_patterns import WorkQueue, PublisherSubscriber, TopicRouter, RequestResponse
from .ml_message_handlers import MLMessageHandler, get_ml_message_handler
from .data_quality_handlers import DataQualityMessageHandler, get_dq_message_handler
from .reliability_features import MessageReliabilityHandler, get_reliability_handler
from core.logging import get_logger


class IntegrationEventType(Enum):
    """Types of integration events"""
    ML_PIPELINE_STARTED = "ml.pipeline.started"
    ML_PIPELINE_COMPLETED = "ml.pipeline.completed"
    ML_PIPELINE_FAILED = "ml.pipeline.failed"
    
    DATA_QUALITY_ALERT = "dq.alert"
    DATA_QUALITY_THRESHOLD_BREACH = "dq.threshold.breach"
    DATA_QUALITY_ANOMALY = "dq.anomaly.detected"
    
    USER_SESSION_CREATED = "user.session.created"
    USER_SESSION_EXPIRED = "user.session.expired"
    USER_ACTIVITY_TRACKED = "user.activity.tracked"
    
    DASHBOARD_UPDATE_REQUIRED = "dashboard.update.required"
    REAL_TIME_METRIC_UPDATE = "realtime.metric.update"
    
    SYSTEM_ALERT_CRITICAL = "system.alert.critical"
    SYSTEM_ALERT_WARNING = "system.alert.warning"
    
    NOTIFICATION_SEND = "notification.send"
    EMAIL_SEND = "email.send"


@dataclass
class MLPipelineIntegrationEvent:
    """ML pipeline integration event"""
    pipeline_id: str
    pipeline_name: str
    event_type: IntegrationEventType
    status: str
    model_id: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    artifacts: Dict[str, str] = field(default_factory=dict)
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "event_type": self.event_type.value,
            "status": self.status,
            "model_id": self.model_id,
            "metrics": self.metrics,
            "artifacts": self.artifacts,
            "error_message": self.error_message,
            "execution_time_seconds": self.execution_time_seconds,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class UserSessionEvent:
    """User session management event"""
    session_id: str
    user_id: str
    event_type: IntegrationEventType
    session_data: Dict[str, Any] = field(default_factory=dict)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    location: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "event_type": self.event_type.value,
            "session_data": self.session_data,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "location": self.location,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class DashboardUpdateEvent:
    """Dashboard update event"""
    dashboard_id: str
    widget_id: Optional[str] = None
    update_type: str = "metric_update"
    data: Dict[str, Any] = field(default_factory=dict)
    filters: Dict[str, Any] = field(default_factory=dict)
    refresh_required: bool = False
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "dashboard_id": self.dashboard_id,
            "widget_id": self.widget_id,
            "update_type": self.update_type,
            "data": self.data,
            "filters": self.filters,
            "refresh_required": self.refresh_required,
            "timestamp": self.timestamp.isoformat()
        }


class MLPipelineIntegrationService:
    """
    Integration service for ML pipelines with RabbitMQ messaging
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Get handlers
        self.ml_handler = get_ml_message_handler()
        self.reliability_handler = get_reliability_handler()
        
        # Message patterns
        self.publisher = PublisherSubscriber(self.rabbitmq)
        self.work_queue = WorkQueue(self.rabbitmq)
        self.topic_router = TopicRouter(self.rabbitmq)
        
        # Pipeline tracking
        self.active_pipelines: Dict[str, Dict[str, Any]] = {}
        self.pipeline_subscribers: Dict[str, List[Callable]] = {}
        
        # Setup integration handlers
        self._setup_ml_integration()
        
    def _setup_ml_integration(self):
        """Setup ML pipeline integration handlers"""
        # Subscribe to ML events
        self.publisher.subscribe(
            event_types=["ml_training_completed", "ml_inference_completed", "ml_deployment_completed"],
            callback=self._handle_ml_event,
            subscriber_id="ml_pipeline_integration"
        )
        
        # Register workflow handlers
        self.work_queue.register_handler("ml_pipeline_orchestration", self._handle_pipeline_orchestration)
        self.work_queue.register_handler("ml_model_lifecycle", self._handle_model_lifecycle)
        
        self.logger.info("ML pipeline integration service initialized")
    
    def start_ml_pipeline(
        self,
        pipeline_name: str,
        pipeline_config: Dict[str, Any],
        priority: MessagePriority = MessagePriority.HIGH
    ) -> str:
        """Start ML pipeline with orchestration"""
        try:
            pipeline_id = f"pipeline_{uuid.uuid4().hex[:8]}"
            
            # Track pipeline
            self.active_pipelines[pipeline_id] = {
                "pipeline_name": pipeline_name,
                "config": pipeline_config,
                "status": "starting",
                "started_at": datetime.utcnow(),
                "stages": []
            }
            
            # Create orchestration message
            orchestration_data = {
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "config": pipeline_config,
                "stages": self._get_pipeline_stages(pipeline_config)
            }
            
            # Submit pipeline orchestration task
            task_id = self.work_queue.add_task(
                task_type="ml_pipeline_orchestration",
                task_data=orchestration_data,
                priority=priority
            )
            
            # Publish pipeline started event
            event = MLPipelineIntegrationEvent(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name,
                event_type=IntegrationEventType.ML_PIPELINE_STARTED,
                status="started"
            )
            
            self.publisher.publish(
                event_type="ml_pipeline_started",
                data=event.to_dict(),
                priority=priority
            )
            
            self.logger.info(f"Started ML pipeline {pipeline_name} with ID {pipeline_id}")
            return pipeline_id
            
        except Exception as e:
            self.logger.error(f"Error starting ML pipeline: {e}")
            raise
    
    def _get_pipeline_stages(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get pipeline stages from configuration"""
        stages = []
        
        # Data preparation stage
        if config.get("data_preparation", {}).get("enabled", True):
            stages.append({
                "stage_name": "data_preparation",
                "stage_type": "data_processing",
                "config": config.get("data_preparation", {}),
                "dependencies": []
            })
        
        # Feature engineering stage
        if config.get("feature_engineering", {}).get("enabled", True):
            stages.append({
                "stage_name": "feature_engineering",
                "stage_type": "feature_processing",
                "config": config.get("feature_engineering", {}),
                "dependencies": ["data_preparation"]
            })
        
        # Model training stage
        if config.get("model_training", {}).get("enabled", True):
            stages.append({
                "stage_name": "model_training",
                "stage_type": "ml_training",
                "config": config.get("model_training", {}),
                "dependencies": ["feature_engineering"]
            })
        
        # Model validation stage
        if config.get("model_validation", {}).get("enabled", True):
            stages.append({
                "stage_name": "model_validation",
                "stage_type": "validation",
                "config": config.get("model_validation", {}),
                "dependencies": ["model_training"]
            })
        
        # Model deployment stage
        if config.get("model_deployment", {}).get("enabled", False):
            stages.append({
                "stage_name": "model_deployment",
                "stage_type": "deployment",
                "config": config.get("model_deployment", {}),
                "dependencies": ["model_validation"]
            })
        
        return stages
    
    def _handle_pipeline_orchestration(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle ML pipeline orchestration"""
        try:
            orchestration_data = task_data.get("data", {})
            pipeline_id = orchestration_data.get("pipeline_id")
            stages = orchestration_data.get("stages", [])
            
            self.logger.info(f"Orchestrating pipeline {pipeline_id} with {len(stages)} stages")
            
            # Update pipeline status
            if pipeline_id in self.active_pipelines:
                self.active_pipelines[pipeline_id]["status"] = "running"
                self.active_pipelines[pipeline_id]["stages"] = stages
            
            # Execute stages sequentially
            executed_stages = []
            
            for stage in stages:
                try:
                    stage_result = self._execute_pipeline_stage(pipeline_id, stage)
                    executed_stages.append({
                        **stage,
                        "result": stage_result,
                        "status": "completed",
                        "execution_time": stage_result.get("execution_time_seconds", 0)
                    })
                    
                except Exception as stage_error:
                    executed_stages.append({
                        **stage,
                        "status": "failed",
                        "error": str(stage_error)
                    })
                    
                    # Pipeline failed
                    self._handle_pipeline_failure(pipeline_id, str(stage_error))
                    raise stage_error
            
            # Pipeline completed successfully
            self._handle_pipeline_completion(pipeline_id, executed_stages)
            
            return {
                "pipeline_id": pipeline_id,
                "status": "completed",
                "stages_executed": len(executed_stages),
                "total_execution_time": sum(s.get("execution_time", 0) for s in executed_stages)
            }
            
        except Exception as e:
            self.logger.error(f"Error in pipeline orchestration: {e}")
            raise
    
    def _execute_pipeline_stage(self, pipeline_id: str, stage: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single pipeline stage"""
        stage_name = stage.get("stage_name")
        stage_type = stage.get("stage_type")
        
        self.logger.info(f"Executing pipeline stage {stage_name} of type {stage_type}")
        
        start_time = time.time()
        
        if stage_type == "data_processing":
            result = self._execute_data_processing_stage(pipeline_id, stage)
        elif stage_type == "feature_processing":
            result = self._execute_feature_processing_stage(pipeline_id, stage)
        elif stage_type == "ml_training":
            result = self._execute_ml_training_stage(pipeline_id, stage)
        elif stage_type == "validation":
            result = self._execute_validation_stage(pipeline_id, stage)
        elif stage_type == "deployment":
            result = self._execute_deployment_stage(pipeline_id, stage)
        else:
            raise ValueError(f"Unknown stage type: {stage_type}")
        
        execution_time = time.time() - start_time
        result["execution_time_seconds"] = execution_time
        
        return result
    
    def _execute_data_processing_stage(self, pipeline_id: str, stage: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data processing stage"""
        # Simulate data processing
        time.sleep(2)  # Simulate processing time
        
        return {
            "stage_name": stage.get("stage_name"),
            "records_processed": 100000,
            "data_quality_score": 0.95,
            "output_path": f"/data/processed/{pipeline_id}_processed.parquet"
        }
    
    def _execute_feature_processing_stage(self, pipeline_id: str, stage: Dict[str, Any]) -> Dict[str, Any]:
        """Execute feature processing stage"""
        # Simulate feature engineering
        time.sleep(1.5)
        
        return {
            "stage_name": stage.get("stage_name"),
            "features_created": 25,
            "feature_importance_calculated": True,
            "feature_store_updated": True,
            "output_path": f"/features/{pipeline_id}_features.parquet"
        }
    
    def _execute_ml_training_stage(self, pipeline_id: str, stage: Dict[str, Any]) -> Dict[str, Any]:
        """Execute ML training stage"""
        # Use ML handler for training
        config = stage.get("config", {})
        
        # Create training request
        from .ml_message_handlers import MLTrainingRequest, ModelType
        
        training_request = MLTrainingRequest(
            job_id=f"{pipeline_id}_training",
            model_name=config.get("model_name", f"model_{pipeline_id}"),
            model_type=ModelType(config.get("model_type", "sklearn")),
            dataset_path=config.get("dataset_path", f"/data/processed/{pipeline_id}_processed.parquet"),
            training_config=config.get("training_config", {}),
            hyperparameters=config.get("hyperparameters", {}),
            max_training_time_minutes=config.get("max_training_time_minutes", 30)
        )
        
        # Submit training job
        job_id = self.ml_handler.submit_training_job(training_request)
        
        # Wait for completion (simplified - in real implementation, use async)
        timeout = 300  # 5 minutes
        start_wait = time.time()
        
        while time.time() - start_wait < timeout:
            job_status = self.ml_handler.get_job_status(job_id)
            if job_status and job_status.get("status") in ["completed", "failed"]:
                break
            time.sleep(5)
        
        # Get final status
        job_status = self.ml_handler.get_job_status(job_id)
        
        if job_status and job_status.get("status") == "completed":
            response = job_status.get("response", {})
            return {
                "stage_name": stage.get("stage_name"),
                "model_id": response.get("model_id"),
                "model_path": response.get("model_path"),
                "training_metrics": response.get("metrics", {}),
                "training_time_seconds": response.get("training_time_seconds")
            }
        else:
            raise Exception(f"Training stage failed: {job_status.get('error', 'Unknown error')}")
    
    def _execute_validation_stage(self, pipeline_id: str, stage: Dict[str, Any]) -> Dict[str, Any]:
        """Execute validation stage"""
        # Simulate model validation
        time.sleep(1)
        
        return {
            "stage_name": stage.get("stage_name"),
            "validation_score": 0.87,
            "validation_passed": True,
            "validation_metrics": {
                "accuracy": 0.87,
                "precision": 0.85,
                "recall": 0.89,
                "f1_score": 0.87
            }
        }
    
    def _execute_deployment_stage(self, pipeline_id: str, stage: Dict[str, Any]) -> Dict[str, Any]:
        """Execute deployment stage"""
        # Simulate model deployment
        time.sleep(0.5)
        
        return {
            "stage_name": stage.get("stage_name"),
            "deployment_environment": stage.get("config", {}).get("environment", "staging"),
            "deployment_status": "deployed",
            "endpoint_url": f"/api/models/{pipeline_id}/predict"
        }
    
    def _handle_pipeline_completion(self, pipeline_id: str, stages: List[Dict[str, Any]]):
        """Handle successful pipeline completion"""
        if pipeline_id in self.active_pipelines:
            self.active_pipelines[pipeline_id]["status"] = "completed"
            self.active_pipelines[pipeline_id]["completed_at"] = datetime.utcnow()
            self.active_pipelines[pipeline_id]["stages"] = stages
        
        # Create completion event
        event = MLPipelineIntegrationEvent(
            pipeline_id=pipeline_id,
            pipeline_name=self.active_pipelines.get(pipeline_id, {}).get("pipeline_name", "unknown"),
            event_type=IntegrationEventType.ML_PIPELINE_COMPLETED,
            status="completed",
            execution_time_seconds=sum(s.get("execution_time", 0) for s in stages)
        )
        
        # Publish completion event
        self.publisher.publish(
            event_type="ml_pipeline_completed",
            data=event.to_dict(),
            priority=MessagePriority.HIGH
        )
        
        # Notify subscribers
        self._notify_pipeline_subscribers(pipeline_id, "completed", event.to_dict())
        
        self.logger.info(f"Pipeline {pipeline_id} completed successfully")
    
    def _handle_pipeline_failure(self, pipeline_id: str, error_message: str):
        """Handle pipeline failure"""
        if pipeline_id in self.active_pipelines:
            self.active_pipelines[pipeline_id]["status"] = "failed"
            self.active_pipelines[pipeline_id]["failed_at"] = datetime.utcnow()
            self.active_pipelines[pipeline_id]["error_message"] = error_message
        
        # Create failure event
        event = MLPipelineIntegrationEvent(
            pipeline_id=pipeline_id,
            pipeline_name=self.active_pipelines.get(pipeline_id, {}).get("pipeline_name", "unknown"),
            event_type=IntegrationEventType.ML_PIPELINE_FAILED,
            status="failed",
            error_message=error_message
        )
        
        # Publish failure event
        self.publisher.publish(
            event_type="ml_pipeline_failed",
            data=event.to_dict(),
            priority=MessagePriority.CRITICAL
        )
        
        # Send alert
        self._send_pipeline_failure_alert(pipeline_id, error_message)
        
        # Notify subscribers
        self._notify_pipeline_subscribers(pipeline_id, "failed", event.to_dict())
        
        self.logger.error(f"Pipeline {pipeline_id} failed: {error_message}")
    
    def _send_pipeline_failure_alert(self, pipeline_id: str, error_message: str):
        """Send alert for pipeline failure"""
        alert_data = {
            "alert_type": "ml_pipeline_failure",
            "pipeline_id": pipeline_id,
            "error_message": error_message,
            "severity": "critical",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.work_queue.add_task(
            task_type="send_alert",
            task_data=alert_data,
            priority=MessagePriority.CRITICAL
        )
    
    def _handle_ml_event(self, event_type: str, event_data: Dict[str, Any]):
        """Handle ML events"""
        self.logger.debug(f"Handling ML event: {event_type}")
        
        # Process based on event type
        if event_type == "ml_training_completed":
            self._handle_training_completed_event(event_data)
        elif event_type == "ml_inference_completed":
            self._handle_inference_completed_event(event_data)
        elif event_type == "ml_deployment_completed":
            self._handle_deployment_completed_event(event_data)
    
    def _handle_training_completed_event(self, event_data: Dict[str, Any]):
        """Handle training completion event"""
        job_id = event_data.get("job_id")
        model_id = event_data.get("model_id")
        
        if model_id:
            # Trigger automatic model validation
            self.work_queue.add_task(
                task_type="model_validation",
                task_data={"model_id": model_id},
                priority=MessagePriority.HIGH
            )
            
            # Update dashboards
            self._trigger_dashboard_update("ml_models", {
                "new_model": model_id,
                "training_completed": True
            })
    
    def _handle_inference_completed_event(self, event_data: Dict[str, Any]):
        """Handle inference completion event"""
        # Update real-time metrics
        self._trigger_realtime_metric_update("ml_inference", {
            "inference_count": 1,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def _handle_deployment_completed_event(self, event_data: Dict[str, Any]):
        """Handle deployment completion event"""
        model_id = event_data.get("model_id")
        environment = event_data.get("environment", "unknown")
        
        # Send deployment notification
        notification_data = {
            "type": "model_deployment",
            "model_id": model_id,
            "environment": environment,
            "message": f"Model {model_id} deployed to {environment}"
        }
        
        self.work_queue.add_task(
            task_type="send_notification",
            task_data=notification_data,
            priority=MessagePriority.NORMAL
        )
    
    def _handle_model_lifecycle(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle model lifecycle management"""
        lifecycle_data = task_data.get("data", {})
        action = lifecycle_data.get("action")
        model_id = lifecycle_data.get("model_id")
        
        self.logger.info(f"Handling model lifecycle action {action} for model {model_id}")
        
        if action == "validate":
            return self._validate_model(model_id, lifecycle_data)
        elif action == "promote":
            return self._promote_model(model_id, lifecycle_data)
        elif action == "retire":
            return self._retire_model(model_id, lifecycle_data)
        else:
            return {"error": f"Unknown lifecycle action: {action}"}
    
    def _validate_model(self, model_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate model"""
        # Submit validation task
        validation_task = {
            "model_id": model_id,
            "validation_dataset": data.get("validation_dataset"),
            "validation_metrics": data.get("required_metrics", [])
        }
        
        self.work_queue.add_task(
            task_type="model_validation",
            task_data=validation_task,
            priority=MessagePriority.HIGH
        )
        
        return {"model_id": model_id, "validation_status": "submitted"}
    
    def _promote_model(self, model_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Promote model to next environment"""
        target_env = data.get("target_environment", "production")
        
        deployment_task = {
            "model_id": model_id,
            "target_environment": target_env,
            "deployment_strategy": data.get("deployment_strategy", "blue_green")
        }
        
        self.work_queue.add_task(
            task_type="model_deployment",
            task_data=deployment_task,
            priority=MessagePriority.HIGH
        )
        
        return {"model_id": model_id, "promotion_status": "submitted", "target_environment": target_env}
    
    def _retire_model(self, model_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Retire model"""
        # Simulate model retirement
        return {"model_id": model_id, "retirement_status": "completed"}
    
    def subscribe_to_pipeline_events(self, pipeline_id: str, callback: Callable):
        """Subscribe to events for specific pipeline"""
        if pipeline_id not in self.pipeline_subscribers:
            self.pipeline_subscribers[pipeline_id] = []
        
        self.pipeline_subscribers[pipeline_id].append(callback)
        
    def _notify_pipeline_subscribers(self, pipeline_id: str, event_type: str, event_data: Dict[str, Any]):
        """Notify pipeline event subscribers"""
        if pipeline_id in self.pipeline_subscribers:
            for callback in self.pipeline_subscribers[pipeline_id]:
                try:
                    callback(event_type, event_data)
                except Exception as e:
                    self.logger.error(f"Error in pipeline subscriber callback: {e}")
    
    def _trigger_dashboard_update(self, dashboard_id: str, update_data: Dict[str, Any]):
        """Trigger dashboard update"""
        update_event = DashboardUpdateEvent(
            dashboard_id=dashboard_id,
            data=update_data
        )
        
        self.publisher.publish(
            event_type="dashboard_update_required",
            data=update_event.to_dict(),
            priority=MessagePriority.NORMAL
        )
    
    def _trigger_realtime_metric_update(self, metric_name: str, metric_data: Dict[str, Any]):
        """Trigger real-time metric update"""
        self.topic_router.publish(
            routing_key=f"realtime.metric.{metric_name}",
            data=metric_data,
            priority=MessagePriority.HIGH
        )


class UserSessionIntegrationService:
    """
    Integration service for user session management with RabbitMQ messaging
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Message patterns
        self.publisher = PublisherSubscriber(self.rabbitmq)
        self.work_queue = WorkQueue(self.rabbitmq)
        
        # Session tracking
        self.active_sessions: Dict[str, Dict[str, Any]] = {}
        self.session_cache_ttl = 3600  # 1 hour
        
        # Setup integration handlers
        self._setup_session_integration()
        
        # Start session cleanup
        self._start_session_cleanup()
    
    def _setup_session_integration(self):
        """Setup session integration handlers"""
        # Register session handlers
        self.work_queue.register_handler("user_session_management", self._handle_session_management)
        self.work_queue.register_handler("user_activity_tracking", self._handle_activity_tracking)
        
        self.logger.info("User session integration service initialized")
    
    def create_user_session(
        self,
        user_id: str,
        session_data: Dict[str, Any],
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> str:
        """Create new user session"""
        try:
            session_id = str(uuid.uuid4())
            
            session_info = {
                "session_id": session_id,
                "user_id": user_id,
                "session_data": session_data,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "created_at": datetime.utcnow(),
                "last_activity": datetime.utcnow(),
                "status": "active"
            }
            
            # Store session
            self.active_sessions[session_id] = session_info
            
            # Create session event
            event = UserSessionEvent(
                session_id=session_id,
                user_id=user_id,
                event_type=IntegrationEventType.USER_SESSION_CREATED,
                session_data=session_data,
                ip_address=ip_address,
                user_agent=user_agent
            )
            
            # Publish session created event
            self.publisher.publish(
                event_type="user_session_created",
                data=event.to_dict(),
                priority=MessagePriority.NORMAL
            )
            
            # Submit session management task
            self.work_queue.add_task(
                task_type="user_session_management",
                task_data={
                    "action": "create",
                    "session_info": session_info
                },
                priority=MessagePriority.NORMAL
            )
            
            self.logger.info(f"Created session {session_id} for user {user_id}")
            return session_id
            
        except Exception as e:
            self.logger.error(f"Error creating user session: {e}")
            raise
    
    def track_user_activity(
        self,
        session_id: str,
        activity_type: str,
        activity_data: Dict[str, Any]
    ) -> bool:
        """Track user activity"""
        try:
            if session_id not in self.active_sessions:
                self.logger.warning(f"Activity tracked for unknown session: {session_id}")
                return False
            
            # Update session activity
            self.active_sessions[session_id]["last_activity"] = datetime.utcnow()
            
            # Create activity event
            activity_event = {
                "session_id": session_id,
                "user_id": self.active_sessions[session_id]["user_id"],
                "activity_type": activity_type,
                "activity_data": activity_data,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Publish activity event
            self.publisher.publish(
                event_type="user_activity_tracked",
                data=activity_event,
                priority=MessagePriority.LOW
            )
            
            # Submit activity tracking task
            self.work_queue.add_task(
                task_type="user_activity_tracking",
                task_data=activity_event,
                priority=MessagePriority.LOW
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error tracking user activity: {e}")
            return False
    
    def expire_session(self, session_id: str, reason: str = "timeout") -> bool:
        """Expire user session"""
        try:
            if session_id not in self.active_sessions:
                return False
            
            session_info = self.active_sessions[session_id]
            session_info["status"] = "expired"
            session_info["expired_at"] = datetime.utcnow()
            session_info["expiry_reason"] = reason
            
            # Create expiration event
            event = UserSessionEvent(
                session_id=session_id,
                user_id=session_info["user_id"],
                event_type=IntegrationEventType.USER_SESSION_EXPIRED,
                session_data={"expiry_reason": reason}
            )
            
            # Publish session expired event
            self.publisher.publish(
                event_type="user_session_expired",
                data=event.to_dict(),
                priority=MessagePriority.NORMAL
            )
            
            # Remove from active sessions
            del self.active_sessions[session_id]
            
            self.logger.info(f"Expired session {session_id} - {reason}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error expiring session: {e}")
            return False
    
    def _handle_session_management(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle session management tasks"""
        try:
            management_data = task_data.get("data", {})
            action = management_data.get("action")
            
            if action == "create":
                return self._process_session_creation(management_data)
            elif action == "update":
                return self._process_session_update(management_data)
            elif action == "expire":
                return self._process_session_expiration(management_data)
            else:
                return {"error": f"Unknown session action: {action}"}
                
        except Exception as e:
            self.logger.error(f"Error handling session management: {e}")
            raise
    
    def _process_session_creation(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process session creation"""
        session_info = data.get("session_info", {})
        session_id = session_info.get("session_id")
        user_id = session_info.get("user_id")
        
        # Could integrate with external session stores here
        # For now, just log and return success
        
        return {
            "session_id": session_id,
            "user_id": user_id,
            "status": "created",
            "processed_at": datetime.utcnow().isoformat()
        }
    
    def _process_session_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process session update"""
        return {"status": "updated"}
    
    def _process_session_expiration(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process session expiration"""
        return {"status": "expired"}
    
    def _handle_activity_tracking(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle user activity tracking"""
        try:
            activity_data = task_data.get("data", {})
            activity_type = activity_data.get("activity_type")
            user_id = activity_data.get("user_id")
            
            # Process different activity types
            if activity_type == "page_view":
                return self._process_page_view_activity(activity_data)
            elif activity_type == "api_call":
                return self._process_api_call_activity(activity_data)
            elif activity_type == "feature_usage":
                return self._process_feature_usage_activity(activity_data)
            else:
                return self._process_generic_activity(activity_data)
                
        except Exception as e:
            self.logger.error(f"Error handling activity tracking: {e}")
            raise
    
    def _process_page_view_activity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process page view activity"""
        return {"activity_type": "page_view", "processed": True}
    
    def _process_api_call_activity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process API call activity"""
        return {"activity_type": "api_call", "processed": True}
    
    def _process_feature_usage_activity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process feature usage activity"""
        return {"activity_type": "feature_usage", "processed": True}
    
    def _process_generic_activity(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process generic activity"""
        return {"activity_type": "generic", "processed": True}
    
    def _start_session_cleanup(self):
        """Start session cleanup thread"""
        def cleanup_expired_sessions():
            while True:
                try:
                    current_time = datetime.utcnow()
                    expired_sessions = []
                    
                    for session_id, session_info in self.active_sessions.items():
                        last_activity = session_info.get("last_activity")
                        if last_activity:
                            time_since_activity = (current_time - last_activity).total_seconds()
                            if time_since_activity > self.session_cache_ttl:
                                expired_sessions.append(session_id)
                    
                    # Expire old sessions
                    for session_id in expired_sessions:
                        self.expire_session(session_id, "timeout")
                    
                    if expired_sessions:
                        self.logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
                    
                    # Sleep for 5 minutes
                    time.sleep(300)
                    
                except Exception as e:
                    self.logger.error(f"Error in session cleanup: {e}")
                    time.sleep(60)
        
        cleanup_thread = threading.Thread(target=cleanup_expired_sessions, daemon=True)
        cleanup_thread.start()
        
        self.logger.info("Started session cleanup thread")
    
    def get_session_metrics(self) -> Dict[str, Any]:
        """Get session metrics"""
        return {
            "active_sessions": len(self.active_sessions),
            "unique_users": len(set(s.get("user_id") for s in self.active_sessions.values())),
            "average_session_duration": self._calculate_average_session_duration()
        }
    
    def _calculate_average_session_duration(self) -> float:
        """Calculate average session duration"""
        if not self.active_sessions:
            return 0.0
        
        total_duration = 0
        session_count = 0
        
        for session_info in self.active_sessions.values():
            created_at = session_info.get("created_at")
            last_activity = session_info.get("last_activity")
            
            if created_at and last_activity:
                duration = (last_activity - created_at).total_seconds()
                total_duration += duration
                session_count += 1
        
        return total_duration / session_count if session_count > 0 else 0.0


class RealtimeDashboardIntegrationService:
    """
    Integration service for real-time dashboard updates with RabbitMQ messaging
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Message patterns
        self.publisher = PublisherSubscriber(self.rabbitmq)
        self.topic_router = TopicRouter(self.rabbitmq)
        
        # Dashboard tracking
        self.dashboard_subscribers: Dict[str, List[Callable]] = {}
        self.metric_cache: Dict[str, Dict[str, Any]] = {}
        
        # Setup integration handlers
        self._setup_dashboard_integration()
    
    def _setup_dashboard_integration(self):
        """Setup dashboard integration handlers"""
        # Subscribe to relevant events
        self.publisher.subscribe(
            event_types=[
                "ml_training_completed", "dq_validation_completed", 
                "user_session_created", "system_alert"
            ],
            callback=self._handle_dashboard_event,
            subscriber_id="dashboard_integration"
        )
        
        # Subscribe to real-time metrics
        self.topic_router.subscribe(
            routing_patterns=["realtime.metric.*"],
            callback=self._handle_realtime_metric,
            subscriber_id="dashboard_metrics"
        )
        
        self.logger.info("Real-time dashboard integration service initialized")
    
    def register_dashboard_subscriber(self, dashboard_id: str, callback: Callable):
        """Register subscriber for dashboard updates"""
        if dashboard_id not in self.dashboard_subscribers:
            self.dashboard_subscribers[dashboard_id] = []
        
        self.dashboard_subscribers[dashboard_id].append(callback)
        self.logger.info(f"Registered subscriber for dashboard {dashboard_id}")
    
    def publish_dashboard_update(
        self,
        dashboard_id: str,
        update_data: Dict[str, Any],
        widget_id: Optional[str] = None
    ) -> bool:
        """Publish dashboard update"""
        try:
            event = DashboardUpdateEvent(
                dashboard_id=dashboard_id,
                widget_id=widget_id,
                data=update_data
            )
            
            return self.publisher.publish(
                event_type="dashboard_update_required",
                data=event.to_dict(),
                priority=MessagePriority.NORMAL
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing dashboard update: {e}")
            return False
    
    def publish_realtime_metric(
        self,
        metric_name: str,
        metric_value: Union[int, float, Dict[str, Any]],
        tags: Optional[Dict[str, str]] = None
    ) -> bool:
        """Publish real-time metric"""
        try:
            metric_data = {
                "metric_name": metric_name,
                "metric_value": metric_value,
                "tags": tags or {},
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Cache metric
            self.metric_cache[metric_name] = metric_data
            
            return self.topic_router.publish(
                routing_key=f"realtime.metric.{metric_name}",
                data=metric_data,
                priority=MessagePriority.HIGH
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing real-time metric: {e}")
            return False
    
    def _handle_dashboard_event(self, event_type: str, event_data: Dict[str, Any]):
        """Handle events that affect dashboards"""
        self.logger.debug(f"Handling dashboard event: {event_type}")
        
        if event_type == "ml_training_completed":
            self._update_ml_dashboard(event_data)
        elif event_type == "dq_validation_completed":
            self._update_data_quality_dashboard(event_data)
        elif event_type == "user_session_created":
            self._update_user_activity_dashboard(event_data)
        elif event_type == "system_alert":
            self._update_system_monitoring_dashboard(event_data)
    
    def _update_ml_dashboard(self, event_data: Dict[str, Any]):
        """Update ML dashboard with training results"""
        dashboard_data = {
            "model_count": 1,
            "latest_training": event_data,
            "training_metrics": event_data.get("metrics", {})
        }
        
        self.publish_dashboard_update("ml_models", dashboard_data)
        
        # Update real-time metrics
        metrics = event_data.get("metrics", {})
        for metric_name, metric_value in metrics.items():
            self.publish_realtime_metric(f"ml.training.{metric_name}", metric_value)
    
    def _update_data_quality_dashboard(self, event_data: Dict[str, Any]):
        """Update data quality dashboard"""
        dashboard_data = {
            "validation_result": event_data,
            "overall_score": event_data.get("overall_score", 0),
            "dataset_name": event_data.get("dataset_name")
        }
        
        self.publish_dashboard_update("data_quality", dashboard_data)
        
        # Update quality score metric
        self.publish_realtime_metric(
            "dq.overall_score", 
            event_data.get("overall_score", 0),
            {"dataset": event_data.get("dataset_name", "unknown")}
        )
    
    def _update_user_activity_dashboard(self, event_data: Dict[str, Any]):
        """Update user activity dashboard"""
        dashboard_data = {
            "new_session": event_data,
            "user_id": event_data.get("user_id"),
            "session_count": 1
        }
        
        self.publish_dashboard_update("user_activity", dashboard_data)
        
        # Update session count metric
        self.publish_realtime_metric("user.active_sessions", 1)
    
    def _update_system_monitoring_dashboard(self, event_data: Dict[str, Any]):
        """Update system monitoring dashboard"""
        dashboard_data = {
            "alert": event_data,
            "severity": event_data.get("severity", "info")
        }
        
        self.publish_dashboard_update("system_monitoring", dashboard_data)
        
        # Update alert count metric
        self.publish_realtime_metric(
            "system.alerts", 
            1,
            {"severity": event_data.get("severity", "info")}
        )
    
    def _handle_realtime_metric(self, routing_key: str, metric_data: Dict[str, Any]):
        """Handle real-time metric updates"""
        metric_name = metric_data.get("metric_name", routing_key.split(".")[-1])
        
        # Notify dashboard subscribers
        affected_dashboards = self._get_dashboards_for_metric(metric_name)
        
        for dashboard_id in affected_dashboards:
            if dashboard_id in self.dashboard_subscribers:
                for callback in self.dashboard_subscribers[dashboard_id]:
                    try:
                        callback("metric_update", {
                            "metric_name": metric_name,
                            "metric_data": metric_data
                        })
                    except Exception as e:
                        self.logger.error(f"Error in dashboard subscriber callback: {e}")
    
    def _get_dashboards_for_metric(self, metric_name: str) -> List[str]:
        """Get dashboards that should be updated for a metric"""
        dashboard_mapping = {
            "ml.": ["ml_models", "ml_performance"],
            "dq.": ["data_quality"],
            "user.": ["user_activity"],
            "system.": ["system_monitoring"],
            "pipeline.": ["pipeline_status"]
        }
        
        affected_dashboards = []
        for prefix, dashboards in dashboard_mapping.items():
            if metric_name.startswith(prefix):
                affected_dashboards.extend(dashboards)
        
        return affected_dashboards
    
    def get_cached_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get cached metrics"""
        return self.metric_cache.copy()


# Global integration services
_ml_integration_service: Optional[MLPipelineIntegrationService] = None
_session_integration_service: Optional[UserSessionIntegrationService] = None
_dashboard_integration_service: Optional[RealtimeDashboardIntegrationService] = None


def get_ml_integration_service() -> MLPipelineIntegrationService:
    """Get or create ML integration service instance"""
    global _ml_integration_service
    if _ml_integration_service is None:
        _ml_integration_service = MLPipelineIntegrationService()
    return _ml_integration_service


def get_session_integration_service() -> UserSessionIntegrationService:
    """Get or create session integration service instance"""
    global _session_integration_service
    if _session_integration_service is None:
        _session_integration_service = UserSessionIntegrationService()
    return _session_integration_service


def get_dashboard_integration_service() -> RealtimeDashboardIntegrationService:
    """Get or create dashboard integration service instance"""
    global _dashboard_integration_service
    if _dashboard_integration_service is None:
        _dashboard_integration_service = RealtimeDashboardIntegrationService()
    return _dashboard_integration_service