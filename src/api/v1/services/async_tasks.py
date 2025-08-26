"""
Async Task Service with RabbitMQ
Implements the Async Request-Reply pattern using RabbitMQ instead of Redis/Celery.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Any, List

from messaging.rabbitmq_manager import (
    RabbitMQManager, QueueType, MessagePriority, 
    TaskMessage, ResultMessage
)
from streaming.kafka_manager import KafkaManager, StreamingTopic
from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)
config = BaseConfig()

# RabbitMQ and Kafka managers for task processing
rabbitmq_manager = RabbitMQManager()
kafka_manager = KafkaManager()


class TaskStatus:
    """Task status constants."""
    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    REVOKED = "revoked"


class AsyncTaskService:
    """Service for managing async tasks using RabbitMQ message queues."""

    def __init__(self):
        self.rabbitmq_manager = rabbitmq_manager
        self.kafka_manager = kafka_manager
        self.task_ttl = 3600  # 1 hour
        
        # In-memory task tracking (in production, this could be a database)
        self.active_tasks: dict[str, dict[str, Any]] = {}
        self.task_results: dict[str, dict[str, Any]] = {}

    async def submit_task(
        self,
        task_name: str,
        task_args: dict[str, Any],
        user_id: str | None = None
    ) -> dict[str, Any]:
        """Submit a task for async processing using RabbitMQ."""
        task_id = str(uuid.uuid4())

        # Store task metadata in memory
        task_metadata = {
            "task_id": task_id,
            "task_name": task_name,
            "status": TaskStatus.PENDING,
            "submitted_at": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "args": task_args
        }
        
        self.active_tasks[task_id] = task_metadata

        # Submit to RabbitMQ
        try:
            # Determine target queue based on task type
            if task_name in ["generate_comprehensive_report"]:
                target_queue = QueueType.RESULT_QUEUE
                priority = MessagePriority.NORMAL
            elif task_name in ["process_large_dataset"]:
                target_queue = QueueType.ETL_QUEUE
                priority = MessagePriority.HIGH
            elif task_name in ["run_advanced_analytics"]:
                target_queue = QueueType.TASK_QUEUE
                priority = MessagePriority.NORMAL
            else:
                target_queue = QueueType.TASK_QUEUE
                priority = MessagePriority.NORMAL

            # Publish task to RabbitMQ
            rabbitmq_task_id = self.rabbitmq_manager.publish_task(
                task_name=task_name,
                payload=task_args,
                queue=target_queue,
                priority=priority,
                expires_in_seconds=self.task_ttl,
                correlation_id=task_id
            )

            # Update task metadata with RabbitMQ task ID
            task_metadata["rabbitmq_task_id"] = rabbitmq_task_id
            self.active_tasks[task_id] = task_metadata
            
            # Publish task event to Kafka for monitoring
            self.kafka_manager.produce_task_event(
                task_id=task_id,
                task_name=task_name,
                status=TaskStatus.PENDING,
                metadata={"user_id": user_id, "submitted_at": task_metadata["submitted_at"]}
            )

            logger.info(f"Task {task_id} submitted to RabbitMQ successfully")

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
            self.active_tasks[task_id] = task_metadata
            
            # Publish failure event to Kafka
            self.kafka_manager.produce_task_event(
                task_id=task_id,
                task_name=task_name,
                status=TaskStatus.FAILURE,
                metadata={"error": str(e)}
            )
            raise

    async def get_task_status(self, task_id: str) -> dict[str, Any] | None:
        """Get the status of a task from RabbitMQ system."""
        # Check in-memory task storage first
        if task_id not in self.active_tasks:
            return None
            
        task_metadata = self.active_tasks[task_id].copy()

        # Check RabbitMQ task result if available
        if "rabbitmq_task_id" in task_metadata:
            try:
                # Get task result from RabbitMQ manager
                result = self.rabbitmq_manager.get_task_result(
                    task_metadata["rabbitmq_task_id"], 
                    timeout=1  # Short timeout for status check
                )
                
                if result:
                    task_metadata["rabbitmq_status"] = result.status
                    
                    if result.status == "success":
                        task_metadata["status"] = TaskStatus.SUCCESS
                        if result.result:
                            task_metadata["result"] = result.result
                        
                        # Store result for future retrieval
                        self.task_results[task_id] = task_metadata
                        
                        # Publish success event to Kafka
                        self.kafka_manager.produce_task_event(
                            task_id=task_id,
                            task_name=task_metadata["task_name"],
                            status=TaskStatus.SUCCESS,
                            metadata={"processing_time_ms": result.processing_time_ms}
                        )
                        
                    elif result.status == "error":
                        task_metadata["status"] = TaskStatus.FAILURE
                        task_metadata["error"] = result.error
                        
                        # Publish failure event to Kafka
                        self.kafka_manager.produce_task_event(
                            task_id=task_id,
                            task_name=task_metadata["task_name"],
                            status=TaskStatus.FAILURE,
                            metadata={"error": result.error}
                        )
                        
                    # Update in-memory storage
                    self.active_tasks[task_id] = task_metadata

            except Exception as e:
                logger.error(f"Error checking RabbitMQ task status: {str(e)}")

        return task_metadata

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task (limited cancellation in RabbitMQ)."""
        if task_id not in self.active_tasks:
            return False

        task_metadata = self.active_tasks[task_id]

        try:
            # Mark task as cancelled in metadata
            task_metadata["status"] = TaskStatus.REVOKED
            task_metadata["cancelled_at"] = datetime.utcnow().isoformat()
            
            # Update in-memory storage
            self.active_tasks[task_id] = task_metadata
            
            # Publish cancellation event to Kafka
            self.kafka_manager.produce_task_event(
                task_id=task_id,
                task_name=task_metadata["task_name"],
                status=TaskStatus.REVOKED,
                metadata={"cancelled_at": task_metadata["cancelled_at"]}
            )

            logger.info(f"Task {task_id} marked as cancelled")
            return True

        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {str(e)}")
            return False

    async def list_user_tasks(
        self,
        user_id: str,
        status: str | None = None
    ) -> List[dict[str, Any]]:
        """List tasks for a specific user from in-memory storage."""
        user_tasks = []
        
        for task_metadata in self.active_tasks.values():
            if task_metadata.get("user_id") == user_id:
                if not status or task_metadata.get("status") == status:
                    user_tasks.append(task_metadata.copy())
        
        # Also check completed tasks
        for task_metadata in self.task_results.values():
            if task_metadata.get("user_id") == user_id:
                if not status or task_metadata.get("status") == status:
                    if task_metadata not in user_tasks:  # Avoid duplicates
                        user_tasks.append(task_metadata.copy())

        # Sort by submission time
        user_tasks.sort(key=lambda x: x.get("submitted_at", ""), reverse=True)
        return user_tasks


# RabbitMQ task processing functions (these would be consumed by separate workers)

def process_generate_comprehensive_report(task_message: TaskMessage) -> ResultMessage:
    """Process comprehensive business report generation task."""
    try:
        task_id = task_message.task_id
        payload = task_message.payload
        
        # Extract parameters
        report_type = payload.get("report_type", "general")
        filters = payload.get("filters", {})

        # Simulate report generation (in real implementation, this would be actual processing)
        import time
        time.sleep(5)  # Simulate processing time

        # Generate actual report (simplified)
        report_data = {
            "report_type": report_type,
            "filters": filters,
            "generated_at": datetime.utcnow().isoformat(),
            "data": {
                "summary": "Comprehensive business report generated successfully",
                "total_records": 1000000,
                "processing_time_seconds": 5,
                "file_size_mb": 15.7
            },
            "download_url": f"/api/v1/reports/download/{task_id}",
            "expires_at": (datetime.utcnow() + timedelta(hours=24)).isoformat()
        }

        logger.info(f"Report generation task {task_id} completed successfully")
        
        return ResultMessage(
            task_id=task_id,
            correlation_id=task_message.correlation_id,
            status="success",
            result=report_data,
            processing_time_ms=5000
        )

    except Exception as e:
        logger.error(f"Report generation task {task_message.task_id} failed: {str(e)}")
        return ResultMessage(
            task_id=task_message.task_id,
            correlation_id=task_message.correlation_id,
            status="error",
            error=str(e)
        )


def process_large_dataset(task_message: TaskMessage) -> ResultMessage:
    """Process large dataset ETL task."""
    try:
        task_id = task_message.task_id
        payload = task_message.payload
        
        # Extract parameters
        dataset_path = payload.get("dataset_path", "")
        processing_options = payload.get("processing_options", {})

        # Simulate dataset processing
        import time
        time.sleep(8)  # Simulate processing time

        result_data = {
            "dataset_path": dataset_path,
            "processing_options": processing_options,
            "processed_at": datetime.utcnow().isoformat(),
            "results": {
                "input_records": 500000,
                "output_records": 485000,
                "quality_score": 94.5,
                "processing_time_minutes": 8.0,
                "output_path": f"/data/processed/{task_id}/results.parquet"
            }
        }

        logger.info(f"Dataset processing task {task_id} completed successfully")
        
        return ResultMessage(
            task_id=task_id,
            correlation_id=task_message.correlation_id,
            status="success",
            result=result_data,
            processing_time_ms=8000
        )

    except Exception as e:
        logger.error(f"Dataset processing task {task_message.task_id} failed: {str(e)}")
        return ResultMessage(
            task_id=task_message.task_id,
            correlation_id=task_message.correlation_id,
            status="error",
            error=str(e)
        )


def run_advanced_analytics(task_message: TaskMessage) -> ResultMessage:
    """Run advanced analytics and machine learning models."""
    try:
        task_id = task_message.task_id
        payload = task_message.payload
        
        # Extract parameters
        analysis_type = payload.get("analysis_type", "general")
        parameters = payload.get("parameters", {})

        # Simulate advanced analytics processing
        import time
        time.sleep(10)  # Simulate processing time

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

        logger.info(f"Advanced analytics task {task_id} completed successfully")
        
        return ResultMessage(
            task_id=task_id,
            correlation_id=task_message.correlation_id,
            status="success",
            result=analytics_result,
            processing_time_ms=10000
        )

    except Exception as e:
        logger.error(f"Advanced analytics task {task_message.task_id} failed: {str(e)}")
        return ResultMessage(
            task_id=task_message.task_id,
            correlation_id=task_message.correlation_id,
            status="error",
            error=str(e)
        )


# Task processor mapping
TASK_PROCESSORS = {
    "generate_comprehensive_report": process_generate_comprehensive_report,
    "process_large_dataset": process_large_dataset,
    "run_advanced_analytics": run_advanced_analytics
}


class TaskWorker:
    """RabbitMQ task worker that processes tasks from queues."""
    
    def __init__(self):
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
        self.logger = get_logger(__name__)
    
    def process_task(self, task_message: TaskMessage) -> ResultMessage:
        """Process a single task message."""
        task_name = task_message.task_name
        
        if task_name in TASK_PROCESSORS:
            processor = TASK_PROCESSORS[task_name]
            return processor(task_message)
        else:
            error_msg = f"Unknown task type: {task_name}"
            self.logger.error(error_msg)
            return ResultMessage(
                task_id=task_message.task_id,
                correlation_id=task_message.correlation_id,
                status="error",
                error=error_msg
            )
    
    def start_worker(self, queue: QueueType = QueueType.TASK_QUEUE):
        """Start consuming tasks from the specified queue."""
        self.logger.info(f"Starting task worker for queue: {queue.value}")
        
        self.rabbitmq_manager.consume_tasks(
            queue=queue,
            callback=self.process_task,
            auto_ack=False,
            max_workers=5
        )


# Create global task service instance
task_service = AsyncTaskService()