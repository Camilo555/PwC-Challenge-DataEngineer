"""
ML Pipeline Message Handlers

This module provides specialized message handlers for ML operations including:
- ML model training job messages
- ML inference requests and responses  
- Model deployment notifications
- Feature pipeline events
- ML monitoring and alerting
"""

import asyncio
import json
import pickle
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable
from pathlib import Path
import threading

from .enterprise_rabbitmq_manager import (
    EnterpriseRabbitMQManager, EnterpriseMessage, MessageMetadata,
    QueueType, MessagePriority, get_rabbitmq_manager
)
from .message_patterns import WorkQueue, PublisherSubscriber, RequestResponse
from core.logging import get_logger


class MLJobType(Enum):
    """ML job types"""
    TRAINING = "training"
    INFERENCE = "inference"
    BATCH_INFERENCE = "batch_inference"
    FEATURE_EXTRACTION = "feature_extraction"
    MODEL_VALIDATION = "model_validation"
    HYPERPARAMETER_TUNING = "hyperparameter_tuning"
    MODEL_DEPLOYMENT = "model_deployment"
    MODEL_ROLLBACK = "model_rollback"
    A_B_TEST = "ab_test"


class MLJobStatus(Enum):
    """ML job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRY = "retry"


class ModelType(Enum):
    """Supported model types"""
    SCIKIT_LEARN = "sklearn"
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    CATBOOST = "catboost"
    ONNX = "onnx"


@dataclass
class MLTrainingRequest:
    """ML training job request"""
    job_id: str = field(default_factory=lambda: f"train_{uuid.uuid4().hex[:8]}")
    model_name: str = ""
    model_type: ModelType = ModelType.SCIKIT_LEARN
    dataset_path: str = ""
    training_config: Dict[str, Any] = field(default_factory=dict)
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    validation_split: float = 0.2
    target_metric: str = "accuracy"
    max_training_time_minutes: int = 60
    
    # Resource requirements
    cpu_cores: int = 2
    memory_gb: int = 4
    gpu_required: bool = False
    gpu_memory_gb: Optional[int] = None
    
    # Experiment tracking
    experiment_name: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    
    # Notifications
    notify_on_completion: bool = True
    notification_channels: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "model_name": self.model_name,
            "model_type": self.model_type.value,
            "dataset_path": self.dataset_path,
            "training_config": self.training_config,
            "hyperparameters": self.hyperparameters,
            "validation_split": self.validation_split,
            "target_metric": self.target_metric,
            "max_training_time_minutes": self.max_training_time_minutes,
            "cpu_cores": self.cpu_cores,
            "memory_gb": self.memory_gb,
            "gpu_required": self.gpu_required,
            "gpu_memory_gb": self.gpu_memory_gb,
            "experiment_name": self.experiment_name,
            "tags": self.tags,
            "notify_on_completion": self.notify_on_completion,
            "notification_channels": self.notification_channels
        }


@dataclass
class MLTrainingResponse:
    """ML training job response"""
    job_id: str
    status: MLJobStatus
    model_id: Optional[str] = None
    model_path: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    training_time_seconds: Optional[float] = None
    model_size_mb: Optional[float] = None
    error_message: Optional[str] = None
    artifacts: Dict[str, str] = field(default_factory=dict)  # artifact_type -> path
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "model_id": self.model_id,
            "model_path": self.model_path,
            "metrics": self.metrics,
            "training_time_seconds": self.training_time_seconds,
            "model_size_mb": self.model_size_mb,
            "error_message": self.error_message,
            "artifacts": self.artifacts,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class MLInferenceRequest:
    """ML inference request"""
    request_id: str = field(default_factory=lambda: f"infer_{uuid.uuid4().hex[:8]}")
    model_id: str = ""
    model_version: str = "latest"
    input_data: Union[Dict[str, Any], List[Dict[str, Any]]] = field(default_factory=dict)
    input_format: str = "json"  # json, csv, parquet, etc.
    output_format: str = "json"
    
    # Batch inference options
    is_batch: bool = False
    batch_size: Optional[int] = None
    input_path: Optional[str] = None
    output_path: Optional[str] = None
    
    # Performance options
    use_cache: bool = True
    timeout_seconds: int = 30
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "input_data": self.input_data,
            "input_format": self.input_format,
            "output_format": self.output_format,
            "is_batch": self.is_batch,
            "batch_size": self.batch_size,
            "input_path": self.input_path,
            "output_path": self.output_path,
            "use_cache": self.use_cache,
            "timeout_seconds": self.timeout_seconds
        }


@dataclass
class MLInferenceResponse:
    """ML inference response"""
    request_id: str
    predictions: Union[Any, List[Any]]
    confidence_scores: Optional[Union[float, List[float]]] = None
    inference_time_ms: Optional[float] = None
    model_version_used: Optional[str] = None
    cache_hit: bool = False
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "predictions": self.predictions,
            "confidence_scores": self.confidence_scores,
            "inference_time_ms": self.inference_time_ms,
            "model_version_used": self.model_version_used,
            "cache_hit": self.cache_hit,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class FeaturePipelineEvent:
    """Feature pipeline event"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str = ""
    event_type: str = ""  # feature_computed, pipeline_started, pipeline_completed, etc.
    feature_names: List[str] = field(default_factory=list)
    feature_values: Dict[str, Any] = field(default_factory=dict)
    entity_id: Optional[str] = None  # customer_id, product_id, etc.
    feature_store_path: Optional[str] = None
    computation_time_ms: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "pipeline_name": self.pipeline_name,
            "event_type": self.event_type,
            "feature_names": self.feature_names,
            "feature_values": self.feature_values,
            "entity_id": self.entity_id,
            "feature_store_path": self.feature_store_path,
            "computation_time_ms": self.computation_time_ms,
            "timestamp": self.timestamp.isoformat()
        }


class MLMessageHandler:
    """
    Comprehensive ML message handler for training, inference, and feature pipelines
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Message patterns
        self.work_queue = WorkQueue(self.rabbitmq)
        self.publisher = PublisherSubscriber(self.rabbitmq)
        self.rpc = RequestResponse(self.rabbitmq)
        
        # ML job tracking
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        self.job_history: Dict[str, Dict[str, Any]] = {}
        
        # Model registry
        self.model_registry: Dict[str, Dict[str, Any]] = {}
        self.feature_store: Dict[str, Dict[str, Any]] = {}
        
        # Setup handlers
        self._setup_handlers()
        
    def _setup_handlers(self):
        """Setup message handlers for different ML operations"""
        
        # Training handlers
        self.work_queue.register_handler("ml_training_request", self.handle_training_request)
        self.work_queue.register_handler("ml_training_status", self.handle_training_status)
        
        # Inference handlers
        self.rpc.register_handler("ml_inference", self.handle_inference_request)
        self.work_queue.register_handler("ml_batch_inference", self.handle_batch_inference)
        
        # Feature pipeline handlers
        self.work_queue.register_handler("feature_pipeline_event", self.handle_feature_event)
        
        # Model management handlers
        self.work_queue.register_handler("model_deployment", self.handle_model_deployment)
        self.work_queue.register_handler("model_validation", self.handle_model_validation)
        
        self.logger.info("ML message handlers setup completed")
    
    def start_ml_workers(self, num_workers: int = 3):
        """Start ML worker processes"""
        self.work_queue.start_workers(num_workers, QueueType.ML_TRAINING.value)
        self.work_queue.start_workers(num_workers, QueueType.ML_INFERENCE.value)
        self.work_queue.start_workers(num_workers, QueueType.ML_FEATURE_PIPELINE.value)
        
        # Start RPC server for real-time inference
        self.rpc.start_server(max_workers=5)
        
        self.logger.info(f"Started {num_workers} ML workers for each queue type")
    
    # Training Operations
    
    def submit_training_job(
        self,
        training_request: MLTrainingRequest,
        priority: MessagePriority = MessagePriority.HIGH
    ) -> str:
        """Submit ML training job"""
        try:
            job_id = self.work_queue.add_task(
                task_type="ml_training_request",
                task_data=training_request.to_dict(),
                priority=priority
            )
            
            # Track job
            self.active_jobs[job_id] = {
                "type": "training",
                "status": MLJobStatus.PENDING,
                "submitted_at": datetime.utcnow(),
                "request": training_request.to_dict()
            }
            
            self.logger.info(f"Submitted ML training job {job_id}")
            return job_id
            
        except Exception as e:
            self.logger.error(f"Failed to submit training job: {e}")
            raise
    
    async def submit_training_job_async(
        self,
        training_request: MLTrainingRequest,
        priority: MessagePriority = MessagePriority.HIGH
    ) -> str:
        """Submit ML training job asynchronously"""
        try:
            job_id = await self.work_queue.add_task_async(
                task_type="ml_training_request",
                task_data=training_request.to_dict(),
                priority=priority
            )
            
            # Track job
            self.active_jobs[job_id] = {
                "type": "training",
                "status": MLJobStatus.PENDING,
                "submitted_at": datetime.utcnow(),
                "request": training_request.to_dict()
            }
            
            self.logger.info(f"Submitted async ML training job {job_id}")
            return job_id
            
        except Exception as e:
            self.logger.error(f"Failed to submit async training job: {e}")
            raise
    
    def handle_training_request(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle ML training request"""
        try:
            training_data = task_data.get("data", {})
            job_id = training_data.get("job_id")
            
            self.logger.info(f"Processing ML training job {job_id}")
            
            # Update job status
            if job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = MLJobStatus.RUNNING
                self.active_jobs[job_id]["started_at"] = datetime.utcnow()
            
            # Simulate training process (in real implementation, this would call actual ML training)
            start_time = time.time()
            
            # Here you would integrate with your ML training pipeline
            # For example: MLflow, Kubeflow, or custom training infrastructure
            result = self._simulate_training(training_data)
            
            training_time = time.time() - start_time
            
            # Create training response
            response = MLTrainingResponse(
                job_id=job_id,
                status=MLJobStatus.COMPLETED if result["success"] else MLJobStatus.FAILED,
                model_id=result.get("model_id"),
                model_path=result.get("model_path"),
                metrics=result.get("metrics", {}),
                training_time_seconds=training_time,
                model_size_mb=result.get("model_size_mb"),
                error_message=result.get("error") if not result["success"] else None
            )
            
            # Update job tracking
            if job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = response.status
                self.active_jobs[job_id]["completed_at"] = datetime.utcnow()
                self.active_jobs[job_id]["response"] = response.to_dict()
                
                # Move to history
                self.job_history[job_id] = self.active_jobs.pop(job_id)
            
            # Publish training completion event
            self.publisher.publish(
                event_type="ml_training_completed",
                data=response.to_dict(),
                priority=MessagePriority.HIGH
            )
            
            # Send notifications if requested
            if training_data.get("notify_on_completion", False):
                self._send_training_notification(training_data, response)
            
            return response.to_dict()
            
        except Exception as e:
            self.logger.error(f"Error handling training request: {e}")
            
            # Update job status
            job_id = task_data.get("data", {}).get("job_id")
            if job_id and job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = MLJobStatus.FAILED
                self.active_jobs[job_id]["error"] = str(e)
            
            raise
    
    def _simulate_training(self, training_data: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate ML training process"""
        try:
            model_name = training_data.get("model_name", "default_model")
            model_type = training_data.get("model_type", "sklearn")
            
            # Simulate training delay
            time.sleep(2)
            
            # Generate mock results
            model_id = f"{model_name}_{uuid.uuid4().hex[:8]}"
            model_path = f"/models/{model_id}/model.pkl"
            
            metrics = {
                "accuracy": 0.85 + (hash(model_id) % 100) / 1000,  # Random but deterministic
                "precision": 0.83 + (hash(model_id) % 80) / 1000,
                "recall": 0.81 + (hash(model_id) % 90) / 1000,
                "f1_score": 0.82 + (hash(model_id) % 85) / 1000
            }
            
            # Register model
            self.model_registry[model_id] = {
                "model_name": model_name,
                "model_type": model_type,
                "model_path": model_path,
                "metrics": metrics,
                "created_at": datetime.utcnow().isoformat(),
                "version": "1.0.0",
                "status": "active"
            }
            
            return {
                "success": True,
                "model_id": model_id,
                "model_path": model_path,
                "metrics": metrics,
                "model_size_mb": 2.5
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _send_training_notification(
        self,
        training_data: Dict[str, Any],
        response: MLTrainingResponse
    ):
        """Send training completion notification"""
        try:
            notification_data = {
                "type": "ml_training_notification",
                "job_id": response.job_id,
                "model_name": training_data.get("model_name"),
                "status": response.status.value,
                "metrics": response.metrics,
                "error_message": response.error_message,
                "training_time_seconds": response.training_time_seconds
            }
            
            self.work_queue.add_task(
                task_type="send_notification",
                task_data=notification_data,
                priority=MessagePriority.HIGH
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send training notification: {e}")
    
    # Inference Operations
    
    def submit_inference_request(
        self,
        inference_request: MLInferenceRequest,
        timeout: int = 30
    ) -> MLInferenceResponse:
        """Submit ML inference request (synchronous)"""
        try:
            if inference_request.is_batch:
                # Use work queue for batch inference
                job_id = self.work_queue.add_task(
                    task_type="ml_batch_inference",
                    task_data=inference_request.to_dict(),
                    priority=MessagePriority.HIGH
                )
                
                # Return job reference for batch processing
                return MLInferenceResponse(
                    request_id=inference_request.request_id,
                    predictions={"job_id": job_id, "status": "processing"},
                    inference_time_ms=0
                )
            else:
                # Use RPC for real-time inference
                result = self.rpc.call(
                    method="ml_inference",
                    params=inference_request.to_dict(),
                    timeout=timeout,
                    priority=MessagePriority.HIGH
                )
                
                return MLInferenceResponse(**result)
                
        except Exception as e:
            self.logger.error(f"Failed to submit inference request: {e}")
            return MLInferenceResponse(
                request_id=inference_request.request_id,
                predictions=None,
                error_message=str(e)
            )
    
    async def submit_inference_request_async(
        self,
        inference_request: MLInferenceRequest,
        timeout: int = 30
    ) -> MLInferenceResponse:
        """Submit ML inference request (asynchronous)"""
        try:
            if inference_request.is_batch:
                # Use work queue for batch inference
                job_id = await self.work_queue.add_task_async(
                    task_type="ml_batch_inference",
                    task_data=inference_request.to_dict(),
                    priority=MessagePriority.HIGH
                )
                
                return MLInferenceResponse(
                    request_id=inference_request.request_id,
                    predictions={"job_id": job_id, "status": "processing"},
                    inference_time_ms=0
                )
            else:
                # Use RPC for real-time inference
                result = await self.rpc.call_async(
                    method="ml_inference",
                    params=inference_request.to_dict(),
                    timeout=timeout,
                    priority=MessagePriority.HIGH
                )
                
                return MLInferenceResponse(**result)
                
        except Exception as e:
            self.logger.error(f"Failed to submit async inference request: {e}")
            return MLInferenceResponse(
                request_id=inference_request.request_id,
                predictions=None,
                error_message=str(e)
            )
    
    def handle_inference_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle ML inference request"""
        try:
            start_time = time.time()
            
            model_id = params.get("model_id")
            input_data = params.get("input_data")
            request_id = params.get("request_id")
            
            self.logger.debug(f"Processing inference request {request_id} for model {model_id}")
            
            # Check if model exists
            if model_id not in self.model_registry:
                return MLInferenceResponse(
                    request_id=request_id,
                    predictions=None,
                    error_message=f"Model {model_id} not found"
                ).to_dict()
            
            # Simulate inference (in real implementation, load and run model)
            predictions = self._simulate_inference(model_id, input_data)
            
            inference_time = (time.time() - start_time) * 1000
            
            response = MLInferenceResponse(
                request_id=request_id,
                predictions=predictions["predictions"],
                confidence_scores=predictions.get("confidence_scores"),
                inference_time_ms=inference_time,
                model_version_used=self.model_registry[model_id].get("version"),
                cache_hit=False  # Would check cache in real implementation
            )
            
            return response.to_dict()
            
        except Exception as e:
            self.logger.error(f"Error handling inference request: {e}")
            return MLInferenceResponse(
                request_id=params.get("request_id", "unknown"),
                predictions=None,
                error_message=str(e)
            ).to_dict()
    
    def handle_batch_inference(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle batch ML inference request"""
        try:
            inference_data = task_data.get("data", {})
            request_id = inference_data.get("request_id")
            
            self.logger.info(f"Processing batch inference request {request_id}")
            
            # Simulate batch processing
            model_id = inference_data.get("model_id")
            batch_size = inference_data.get("batch_size", 1000)
            
            start_time = time.time()
            
            # In real implementation, process data in batches
            results = self._simulate_batch_inference(model_id, batch_size)
            
            processing_time = time.time() - start_time
            
            # Publish batch completion event
            self.publisher.publish(
                event_type="ml_batch_inference_completed",
                data={
                    "request_id": request_id,
                    "model_id": model_id,
                    "batch_size": batch_size,
                    "processing_time_seconds": processing_time,
                    "output_path": results.get("output_path")
                },
                priority=MessagePriority.NORMAL
            )
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error handling batch inference: {e}")
            raise
    
    def _simulate_inference(self, model_id: str, input_data: Any) -> Dict[str, Any]:
        """Simulate model inference"""
        try:
            # Simulate processing delay
            time.sleep(0.1)
            
            if isinstance(input_data, list):
                predictions = [0.8 + (hash(f"{model_id}_{i}") % 20) / 100 for i in range(len(input_data))]
                confidence_scores = [0.9 + (hash(f"{model_id}_{i}_conf") % 10) / 100 for i in range(len(input_data))]
            else:
                predictions = 0.8 + (hash(f"{model_id}_single") % 20) / 100
                confidence_scores = 0.9 + (hash(f"{model_id}_single_conf") % 10) / 100
            
            return {
                "predictions": predictions,
                "confidence_scores": confidence_scores
            }
            
        except Exception as e:
            raise Exception(f"Inference simulation failed: {e}")
    
    def _simulate_batch_inference(self, model_id: str, batch_size: int) -> Dict[str, Any]:
        """Simulate batch model inference"""
        try:
            # Simulate batch processing
            output_path = f"/batch_results/{model_id}_{uuid.uuid4().hex[:8]}.parquet"
            
            return {
                "output_path": output_path,
                "records_processed": batch_size,
                "success": True
            }
            
        except Exception as e:
            raise Exception(f"Batch inference simulation failed: {e}")
    
    # Feature Pipeline Operations
    
    def publish_feature_event(
        self,
        feature_event: FeaturePipelineEvent,
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> bool:
        """Publish feature pipeline event"""
        try:
            return self.publisher.publish(
                event_type="feature_pipeline_event",
                data=feature_event.to_dict(),
                priority=priority
            )
            
        except Exception as e:
            self.logger.error(f"Failed to publish feature event: {e}")
            return False
    
    def handle_feature_event(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle feature pipeline event"""
        try:
            feature_data = task_data.get("data", {})
            event_type = feature_data.get("event_type")
            pipeline_name = feature_data.get("pipeline_name")
            
            self.logger.info(f"Processing feature event {event_type} for pipeline {pipeline_name}")
            
            # Store feature data in feature store
            if event_type == "feature_computed":
                feature_names = feature_data.get("feature_names", [])
                feature_values = feature_data.get("feature_values", {})
                entity_id = feature_data.get("entity_id")
                
                if entity_id:
                    if entity_id not in self.feature_store:
                        self.feature_store[entity_id] = {}
                    
                    self.feature_store[entity_id].update(feature_values)
                    self.feature_store[entity_id]["last_updated"] = datetime.utcnow().isoformat()
            
            return {"success": True, "event_processed": True}
            
        except Exception as e:
            self.logger.error(f"Error handling feature event: {e}")
            raise
    
    # Model Management Operations
    
    def handle_model_deployment(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle model deployment request"""
        try:
            deployment_data = task_data.get("data", {})
            model_id = deployment_data.get("model_id")
            target_environment = deployment_data.get("target_environment", "staging")
            
            self.logger.info(f"Deploying model {model_id} to {target_environment}")
            
            if model_id in self.model_registry:
                # Update model status
                self.model_registry[model_id]["deployment_status"] = "deployed"
                self.model_registry[model_id]["deployed_at"] = datetime.utcnow().isoformat()
                self.model_registry[model_id]["environment"] = target_environment
                
                # Publish deployment event
                self.publisher.publish(
                    event_type="model_deployed",
                    data={
                        "model_id": model_id,
                        "environment": target_environment,
                        "deployed_at": datetime.utcnow().isoformat()
                    },
                    priority=MessagePriority.HIGH
                )
                
                return {"success": True, "deployment_status": "deployed"}
            else:
                return {"success": False, "error": f"Model {model_id} not found"}
                
        except Exception as e:
            self.logger.error(f"Error handling model deployment: {e}")
            raise
    
    def handle_model_validation(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle model validation request"""
        try:
            validation_data = task_data.get("data", {})
            model_id = validation_data.get("model_id")
            validation_dataset = validation_data.get("validation_dataset")
            
            self.logger.info(f"Validating model {model_id}")
            
            # Simulate model validation
            validation_results = {
                "model_id": model_id,
                "validation_passed": True,
                "validation_metrics": {
                    "accuracy": 0.87,
                    "precision": 0.85,
                    "recall": 0.88,
                    "f1_score": 0.86
                },
                "validation_errors": [],
                "validated_at": datetime.utcnow().isoformat()
            }
            
            # Publish validation results
            self.publisher.publish(
                event_type="model_validated",
                data=validation_results,
                priority=MessagePriority.HIGH
            )
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error handling model validation: {e}")
            raise
    
    def handle_training_status(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle training status update"""
        try:
            status_data = task_data.get("data", {})
            job_id = status_data.get("job_id")
            new_status = status_data.get("status")
            
            if job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = MLJobStatus(new_status)
                self.active_jobs[job_id]["updated_at"] = datetime.utcnow()
            
            return {"success": True, "status_updated": True}
            
        except Exception as e:
            self.logger.error(f"Error handling training status: {e}")
            raise
    
    # Utility Methods
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of ML job"""
        if job_id in self.active_jobs:
            return self.active_jobs[job_id]
        elif job_id in self.job_history:
            return self.job_history[job_id]
        else:
            return None
    
    def list_active_jobs(self) -> List[Dict[str, Any]]:
        """List all active ML jobs"""
        return list(self.active_jobs.values())
    
    def get_model_info(self, model_id: str) -> Optional[Dict[str, Any]]:
        """Get model information"""
        return self.model_registry.get(model_id)
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all registered models"""
        return list(self.model_registry.values())
    
    def get_features(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get features for entity"""
        return self.feature_store.get(entity_id)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get ML handler metrics"""
        return {
            "active_jobs": len(self.active_jobs),
            "completed_jobs": len(self.job_history),
            "registered_models": len(self.model_registry),
            "feature_entities": len(self.feature_store)
        }


# Global ML message handler instance
_ml_message_handler: Optional[MLMessageHandler] = None


def get_ml_message_handler() -> MLMessageHandler:
    """Get or create global ML message handler instance"""
    global _ml_message_handler
    if _ml_message_handler is None:
        _ml_message_handler = MLMessageHandler()
    return _ml_message_handler


def set_ml_message_handler(handler: MLMessageHandler):
    """Set global ML message handler instance"""
    global _ml_message_handler
    _ml_message_handler = handler