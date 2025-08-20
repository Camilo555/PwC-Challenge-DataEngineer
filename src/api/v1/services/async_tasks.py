"""
Async Task Service
Implements the Async Request-Reply pattern for long-running operations.
"""
import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import redis
from celery import Celery

from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)
config = BaseConfig()

# Redis client for storing task results
redis_client = redis.Redis(
    host=getattr(config, 'redis_host', 'localhost'),
    port=getattr(config, 'redis_port', 6379),
    decode_responses=True
)

# Celery app configuration
celery_app = Celery(
    'retail_etl_tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    result_expires=3600,  # 1 hour
    task_routes={
        'api.v1.services.async_tasks.generate_comprehensive_report': {'queue': 'reports'},
        'api.v1.services.async_tasks.process_large_dataset': {'queue': 'etl'},
        'api.v1.services.async_tasks.run_advanced_analytics': {'queue': 'analytics'},
    }
)


class TaskStatus:
    """Task status constants."""
    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    REVOKED = "revoked"


class AsyncTaskService:
    """Service for managing async tasks and request-reply pattern."""
    
    def __init__(self):
        self.redis_client = redis_client
        self.task_ttl = 3600  # 1 hour
    
    async def submit_task(
        self, 
        task_name: str, 
        task_args: Dict[str, Any], 
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Submit a task for async processing."""
        task_id = str(uuid.uuid4())
        
        # Store task metadata
        task_metadata = {
            "task_id": task_id,
            "task_name": task_name,
            "status": TaskStatus.PENDING,
            "submitted_at": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "args": task_args
        }
        
        # Store in Redis
        self.redis_client.setex(
            f"task:{task_id}",
            self.task_ttl,
            json.dumps(task_metadata)
        )
        
        # Submit to Celery
        try:
            if task_name == "generate_comprehensive_report":
                celery_task = generate_comprehensive_report.delay(task_id, **task_args)
            elif task_name == "process_large_dataset":
                celery_task = process_large_dataset.delay(task_id, **task_args)
            elif task_name == "run_advanced_analytics":
                celery_task = run_advanced_analytics.delay(task_id, **task_args)
            else:
                raise ValueError(f"Unknown task type: {task_name}")
            
            # Update with Celery task ID
            task_metadata["celery_task_id"] = celery_task.id
            self.redis_client.setex(
                f"task:{task_id}",
                self.task_ttl,
                json.dumps(task_metadata)
            )
            
            logger.info(f"Task {task_id} submitted successfully")
            
            return {
                "task_id": task_id,
                "status": TaskStatus.PENDING,
                "submitted_at": task_metadata["submitted_at"],
                "estimated_completion": (
                    datetime.utcnow() + timedelta(minutes=10)
                ).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to submit task {task_id}: {str(e)}")
            # Update status to failure
            task_metadata["status"] = TaskStatus.FAILURE
            task_metadata["error"] = str(e)
            self.redis_client.setex(
                f"task:{task_id}",
                self.task_ttl,
                json.dumps(task_metadata)
            )
            raise
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a task."""
        task_data = self.redis_client.get(f"task:{task_id}")
        if not task_data:
            return None
        
        task_metadata = json.loads(task_data)
        
        # Check Celery task status if available
        if "celery_task_id" in task_metadata:
            try:
                celery_task = celery_app.AsyncResult(task_metadata["celery_task_id"])
                task_metadata["celery_status"] = celery_task.status
                
                if celery_task.ready():
                    if celery_task.successful():
                        task_metadata["status"] = TaskStatus.SUCCESS
                        if celery_task.result:
                            task_metadata["result"] = celery_task.result
                    else:
                        task_metadata["status"] = TaskStatus.FAILURE
                        task_metadata["error"] = str(celery_task.info)
                elif celery_task.status == 'STARTED':
                    task_metadata["status"] = TaskStatus.STARTED
                    
            except Exception as e:
                logger.error(f"Error checking Celery task status: {str(e)}")
        
        return task_metadata
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task."""
        task_data = self.redis_client.get(f"task:{task_id}")
        if not task_data:
            return False
        
        task_metadata = json.loads(task_data)
        
        # Revoke Celery task
        if "celery_task_id" in task_metadata:
            try:
                celery_app.control.revoke(task_metadata["celery_task_id"], terminate=True)
                task_metadata["status"] = TaskStatus.REVOKED
                task_metadata["cancelled_at"] = datetime.utcnow().isoformat()
                
                self.redis_client.setex(
                    f"task:{task_id}",
                    self.task_ttl,
                    json.dumps(task_metadata)
                )
                
                logger.info(f"Task {task_id} cancelled successfully")
                return True
                
            except Exception as e:
                logger.error(f"Error cancelling task {task_id}: {str(e)}")
                return False
        
        return False
    
    async def list_user_tasks(
        self, 
        user_id: str, 
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List tasks for a specific user."""
        pattern = "task:*"
        task_keys = self.redis_client.keys(pattern)
        
        user_tasks = []
        for key in task_keys:
            task_data = self.redis_client.get(key)
            if task_data:
                task_metadata = json.loads(task_data)
                if task_metadata.get("user_id") == user_id:
                    if not status or task_metadata.get("status") == status:
                        user_tasks.append(task_metadata)
        
        # Sort by submission time
        user_tasks.sort(key=lambda x: x.get("submitted_at", ""), reverse=True)
        return user_tasks


# Celery task definitions

@celery_app.task(bind=True)
def generate_comprehensive_report(self, task_id: str, report_type: str, filters: Dict[str, Any]):
    """Generate a comprehensive business report."""
    try:
        # Update task status
        _update_task_status(task_id, TaskStatus.STARTED, {"progress": 0})
        
        # Simulate report generation with progress updates
        for progress in [20, 40, 60, 80, 100]:
            self.update_state(
                state='PROGRESS',
                meta={'progress': progress, 'status': f'Processing... {progress}%'}
            )
            # Simulate work
            import time
            time.sleep(2)
        
        # Generate actual report (simplified)
        report_data = {
            "report_type": report_type,
            "filters": filters,
            "generated_at": datetime.utcnow().isoformat(),
            "data": {
                "summary": "Comprehensive business report generated successfully",
                "total_records": 1000000,
                "processing_time_seconds": 10,
                "file_size_mb": 15.7
            },
            "download_url": f"/api/v1/reports/download/{task_id}",
            "expires_at": (datetime.utcnow() + timedelta(hours=24)).isoformat()
        }
        
        # Update final status
        _update_task_status(task_id, TaskStatus.SUCCESS, {"result": report_data})
        
        logger.info(f"Report generation task {task_id} completed successfully")
        return report_data
        
    except Exception as e:
        logger.error(f"Report generation task {task_id} failed: {str(e)}")
        _update_task_status(task_id, TaskStatus.FAILURE, {"error": str(e)})
        raise


@celery_app.task(bind=True)
def process_large_dataset(self, task_id: str, dataset_path: str, processing_options: Dict[str, Any]):
    """Process a large dataset with ETL operations."""
    try:
        _update_task_status(task_id, TaskStatus.STARTED, {"progress": 0})
        
        # Simulate dataset processing
        processing_steps = [
            "Loading dataset",
            "Data validation", 
            "Data cleaning",
            "Feature engineering",
            "Data quality assessment",
            "Saving results"
        ]
        
        for i, step in enumerate(processing_steps):
            progress = int((i + 1) / len(processing_steps) * 100)
            self.update_state(
                state='PROGRESS',
                meta={'progress': progress, 'status': step}
            )
            import time
            time.sleep(3)
        
        result_data = {
            "dataset_path": dataset_path,
            "processing_options": processing_options,
            "processed_at": datetime.utcnow().isoformat(),
            "results": {
                "input_records": 500000,
                "output_records": 485000,
                "quality_score": 94.5,
                "processing_time_minutes": 8.5,
                "output_path": f"/data/processed/{task_id}/results.parquet"
            }
        }
        
        _update_task_status(task_id, TaskStatus.SUCCESS, {"result": result_data})
        
        logger.info(f"Dataset processing task {task_id} completed successfully")
        return result_data
        
    except Exception as e:
        logger.error(f"Dataset processing task {task_id} failed: {str(e)}")
        _update_task_status(task_id, TaskStatus.FAILURE, {"error": str(e)})
        raise


@celery_app.task(bind=True)
def run_advanced_analytics(self, task_id: str, analysis_type: str, parameters: Dict[str, Any]):
    """Run advanced analytics and machine learning models."""
    try:
        _update_task_status(task_id, TaskStatus.STARTED, {"progress": 0})
        
        # Simulate advanced analytics
        analysis_steps = [
            "Data preparation",
            "Feature selection",
            "Model training",
            "Model validation",
            "Results compilation"
        ]
        
        for i, step in enumerate(analysis_steps):
            progress = int((i + 1) / len(analysis_steps) * 100)
            self.update_state(
                state='PROGRESS',
                meta={'progress': progress, 'status': step}
            )
            import time
            time.sleep(4)
        
        analytics_result = {
            "analysis_type": analysis_type,
            "parameters": parameters,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "model_accuracy": 0.87,
                "feature_importance": {
                    "recency": 0.35,
                    "frequency": 0.28,
                    "monetary": 0.25,
                    "seasonality": 0.12
                },
                "insights": [
                    "Customer segmentation shows 6 distinct clusters",
                    "High-value customers prefer premium products",
                    "Seasonal patterns strongly influence purchase behavior"
                ],
                "recommendations": [
                    "Focus retention efforts on at-risk high-value customers",
                    "Increase inventory for seasonal peak periods",
                    "Develop targeted campaigns for each customer segment"
                ]
            }
        }
        
        _update_task_status(task_id, TaskStatus.SUCCESS, {"result": analytics_result})
        
        logger.info(f"Advanced analytics task {task_id} completed successfully")
        return analytics_result
        
    except Exception as e:
        logger.error(f"Advanced analytics task {task_id} failed: {str(e)}")
        _update_task_status(task_id, TaskStatus.FAILURE, {"error": str(e)})
        raise


def _update_task_status(task_id: str, status: str, additional_data: Dict[str, Any] = None):
    """Update task status in Redis."""
    try:
        task_data = redis_client.get(f"task:{task_id}")
        if task_data:
            task_metadata = json.loads(task_data)
            task_metadata["status"] = status
            task_metadata["updated_at"] = datetime.utcnow().isoformat()
            
            if additional_data:
                task_metadata.update(additional_data)
            
            redis_client.setex(
                f"task:{task_id}",
                3600,  # 1 hour TTL
                json.dumps(task_metadata)
            )
    except Exception as e:
        logger.error(f"Failed to update task status for {task_id}: {str(e)}")