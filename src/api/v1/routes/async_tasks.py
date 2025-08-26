"""
Async Tasks Router
Implements the Async Request-Reply pattern for long-running operations.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from api.v1.services.async_tasks import AsyncTaskService, TaskStatus
from core.logging import get_logger

router = APIRouter(prefix="/tasks", tags=["async-tasks"])
logger = get_logger(__name__)


class TaskSubmissionRequest(BaseModel):
    """Request model for task submission."""
    task_name: str
    parameters: dict[str, Any]


class TaskSubmissionResponse(BaseModel):
    """Response model for task submission."""
    task_id: str
    status: str
    submitted_at: str
    estimated_completion: str
    message: str = "Task submitted successfully"


class TaskStatusResponse(BaseModel):
    """Response model for task status."""
    task_id: str
    task_name: str
    status: str
    submitted_at: str
    updated_at: str | None = None
    progress: int | None = None
    result: dict[str, Any] | None = None
    error: str | None = None


class TaskListResponse(BaseModel):
    """Response model for listing tasks."""
    tasks: list[TaskStatusResponse]
    total_count: int


@router.post("/submit", response_model=TaskSubmissionResponse)
async def submit_async_task(
    request: TaskSubmissionRequest,
    user_id: str | None = Query(None, description="User ID for task tracking")
) -> TaskSubmissionResponse:
    """
    Submit a long-running task for async processing.
    
    Supported task types:
    - generate_comprehensive_report: Generate detailed business reports
    - process_large_dataset: Process large datasets with ETL operations
    - run_advanced_analytics: Run advanced analytics and ML models
    """
    service = AsyncTaskService()

    try:
        # Validate task name
        valid_tasks = [
            "generate_comprehensive_report",
            "process_large_dataset",
            "run_advanced_analytics"
        ]

        if request.task_name not in valid_tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid task name. Supported tasks: {', '.join(valid_tasks)}"
            )

        # Submit task
        result = await service.submit_task(
            task_name=request.task_name,
            task_args=request.parameters,
            user_id=user_id
        )

        return TaskSubmissionResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting task: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to submit task"
        )


@router.get("/{task_id}/status", response_model=TaskStatusResponse)
async def get_task_status(task_id: str) -> TaskStatusResponse:
    """Get the status and results of a specific task."""
    service = AsyncTaskService()

    try:
        task_data = await service.get_task_status(task_id)

        if not task_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )

        # Extract progress from Celery meta if available
        progress = None
        if task_data.get("celery_status") == "PROGRESS" and "result" in task_data:
            if isinstance(task_data["result"], dict):
                progress = task_data["result"].get("progress")

        return TaskStatusResponse(
            task_id=task_data["task_id"],
            task_name=task_data["task_name"],
            status=task_data["status"],
            submitted_at=task_data["submitted_at"],
            updated_at=task_data.get("updated_at"),
            progress=progress,
            result=task_data.get("result"),
            error=task_data.get("error")
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve task status"
        )


@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_task(task_id: str):
    """Cancel a running task."""
    service = AsyncTaskService()

    try:
        success = await service.cancel_task(task_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found or cannot be cancelled"
            )

        logger.info(f"Task {task_id} cancelled successfully")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling task: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel task"
        )


@router.get("/user/{user_id}", response_model=TaskListResponse)
async def list_user_tasks(
    user_id: str,
    status_filter: str | None = Query(None, description="Filter by task status"),
    limit: int = Query(50, ge=1, le=100)
) -> TaskListResponse:
    """List all tasks for a specific user."""
    service = AsyncTaskService()

    try:
        tasks = await service.list_user_tasks(user_id, status_filter)

        # Apply limit
        limited_tasks = tasks[:limit]

        task_responses = [
            TaskStatusResponse(
                task_id=task["task_id"],
                task_name=task["task_name"],
                status=task["status"],
                submitted_at=task["submitted_at"],
                updated_at=task.get("updated_at"),
                result=task.get("result"),
                error=task.get("error")
            )
            for task in limited_tasks
        ]

        return TaskListResponse(
            tasks=task_responses,
            total_count=len(tasks)
        )

    except Exception as e:
        logger.error(f"Error listing user tasks: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve user tasks"
        )


@router.get("/statistics", response_model=dict[str, Any])
async def get_task_statistics() -> dict[str, Any]:
    """Get overall task execution statistics."""
    service = AsyncTaskService()

    try:
        # Get all tasks (simplified - in production, use proper pagination)
        all_tasks = []
        pattern = "task:*"
        task_keys = service.redis_client.keys(pattern)

        for key in task_keys:
            task_data = service.redis_client.get(key)
            if task_data:
                import json
                task_metadata = json.loads(task_data)
                all_tasks.append(task_metadata)

        # Calculate statistics
        total_tasks = len(all_tasks)
        status_counts = {}
        task_type_counts = {}

        for task in all_tasks:
            status = task.get("status", "unknown")
            task_name = task.get("task_name", "unknown")

            status_counts[status] = status_counts.get(status, 0) + 1
            task_type_counts[task_name] = task_type_counts.get(task_name, 0) + 1

        # Calculate success rate
        successful_tasks = status_counts.get(TaskStatus.SUCCESS, 0)
        success_rate = (successful_tasks / total_tasks * 100) if total_tasks > 0 else 0

        return {
            "total_tasks": total_tasks,
            "status_distribution": status_counts,
            "task_type_distribution": task_type_counts,
            "success_rate_percentage": round(success_rate, 2),
            "generated_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Error getting task statistics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve task statistics"
        )


# Task-specific endpoints

@router.post("/reports/generate", response_model=TaskSubmissionResponse)
async def generate_report(
    report_type: str = Query(..., description="Type of report to generate"),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    country: str | None = Query(None),
    category: str | None = Query(None),
    user_id: str | None = Query(None)
) -> TaskSubmissionResponse:
    """Generate a comprehensive business report asynchronously."""
    service = AsyncTaskService()

    parameters = {
        "report_type": report_type,
        "filters": {
            "date_from": date_from,
            "date_to": date_to,
            "country": country,
            "category": category
        }
    }

    result = await service.submit_task(
        task_name="generate_comprehensive_report",
        task_args=parameters,
        user_id=user_id
    )

    return TaskSubmissionResponse(**result)


@router.post("/etl/process", response_model=TaskSubmissionResponse)
async def process_dataset(
    dataset_path: str = Query(..., description="Path to dataset to process"),
    engine: str = Query("pandas", description="Processing engine (pandas/spark)"),
    enable_quality_checks: bool = Query(True),
    enable_enrichment: bool = Query(False),
    user_id: str | None = Query(None)
) -> TaskSubmissionResponse:
    """Process a large dataset asynchronously."""
    service = AsyncTaskService()

    parameters = {
        "dataset_path": dataset_path,
        "processing_options": {
            "engine": engine,
            "enable_quality_checks": enable_quality_checks,
            "enable_enrichment": enable_enrichment
        }
    }

    result = await service.submit_task(
        task_name="process_large_dataset",
        task_args=parameters,
        user_id=user_id
    )

    return TaskSubmissionResponse(**result)


@router.post("/analytics/run", response_model=TaskSubmissionResponse)
async def run_analytics(
    analysis_type: str = Query(..., description="Type of analysis (rfm/segmentation/forecasting)"),
    include_ml: bool = Query(False, description="Include machine learning models"),
    confidence_level: float = Query(0.95, ge=0.1, le=0.99),
    user_id: str | None = Query(None)
) -> TaskSubmissionResponse:
    """Run advanced analytics and ML models asynchronously."""
    service = AsyncTaskService()

    parameters = {
        "analysis_type": analysis_type,
        "parameters": {
            "include_ml": include_ml,
            "confidence_level": confidence_level
        }
    }

    result = await service.submit_task(
        task_name="run_advanced_analytics",
        task_args=parameters,
        user_id=user_id
    )

    return TaskSubmissionResponse(**result)
