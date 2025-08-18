"""Dagster sensors for file-triggered data ingestion."""

from pathlib import Path
from typing import Iterator

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from de_challenge.core.config import settings
from de_challenge.core.logging import get_logger

logger = get_logger(__name__)


@sensor(
    name="file_drop_sensor",
    description="Triggers pipeline when new files are dropped in the raw data directory",
    default_status=DefaultSensorStatus.RUNNING,
)
def file_drop_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    File sensor that monitors the raw data directory for new CSV files.
    
    This sensor checks for new files every 30 seconds (configurable) and
    triggers the data pipeline when new files are detected.
    """
    raw_data_path = Path(settings.raw_data_path)
    
    if not raw_data_path.exists():
        context.log.warning(f"Raw data directory does not exist: {raw_data_path}")
        return SensorResult(skip_reason=f"Directory not found: {raw_data_path}")
    
    # Look for CSV files in the raw data directory
    csv_files = list(raw_data_path.glob("*.csv"))
    
    if not csv_files:
        return SensorResult(skip_reason="No CSV files found in raw data directory")
    
    # Get the cursor (last processed file timestamp)
    cursor = context.cursor or "0"
    last_processed_time = float(cursor)
    
    # Find new files since last check
    new_files = []
    latest_timestamp = last_processed_time
    
    for file_path in csv_files:
        file_timestamp = file_path.stat().st_mtime
        
        if file_timestamp > last_processed_time:
            new_files.append((file_path, file_timestamp))
            latest_timestamp = max(latest_timestamp, file_timestamp)
    
    if not new_files:
        return SensorResult(skip_reason="No new files detected")
    
    # Sort files by timestamp to process them in order
    new_files.sort(key=lambda x: x[1])
    
    # Create run requests for new files
    run_requests = []
    
    for file_path, file_timestamp in new_files:
        # Create configuration for the run
        run_config = {
            "ops": {
                "raw_retail_data": {
                    "config": {
                        "input_file": file_path.name,
                        "enable_enrichment": True,
                        "batch_size": 100,
                    }
                }
            }
        }
        
        # Create run request with metadata
        run_request = RunRequest(
            run_key=f"file_drop_{file_path.stem}_{int(file_timestamp)}",
            run_config=run_config,
            tags={
                "source_file": file_path.name,
                "file_size": str(file_path.stat().st_size),
                "trigger": "file_drop",
                "timestamp": str(file_timestamp),
            }
        )
        
        run_requests.append(run_request)
        
        context.log.info(f"Detected new file: {file_path.name} (size: {file_path.stat().st_size} bytes)")
    
    # Update cursor to latest timestamp
    new_cursor = str(latest_timestamp)
    
    context.log.info(f"Triggering {len(run_requests)} pipeline runs for new files")
    
    return SensorResult(
        run_requests=run_requests,
        cursor=new_cursor,
    )


@sensor(
    name="large_file_sensor",
    description="Special handling for large files (>10MB) with batched processing",
    default_status=DefaultSensorStatus.STOPPED,  # Manual activation required
)
def large_file_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    Specialized sensor for handling large files with different processing strategies.
    
    This sensor detects large files and triggers special processing logic
    that includes batching and progress monitoring.
    """
    raw_data_path = Path(settings.raw_data_path)
    large_file_threshold = 10 * 1024 * 1024  # 10MB
    
    if not raw_data_path.exists():
        return SensorResult(skip_reason=f"Directory not found: {raw_data_path}")
    
    # Look for large CSV or Excel files
    large_files = []
    for pattern in ["*.csv", "*.xlsx", "*.xls"]:
        for file_path in raw_data_path.glob(pattern):
            if file_path.stat().st_size > large_file_threshold:
                large_files.append(file_path)
    
    if not large_files:
        return SensorResult(skip_reason="No large files detected")
    
    # Get cursor for tracking processed large files
    cursor = context.cursor or "0"
    last_processed_time = float(cursor)
    
    # Find new large files
    new_large_files = []
    latest_timestamp = last_processed_time
    
    for file_path in large_files:
        file_timestamp = file_path.stat().st_mtime
        
        if file_timestamp > last_processed_time:
            new_large_files.append((file_path, file_timestamp))
            latest_timestamp = max(latest_timestamp, file_timestamp)
    
    if not new_large_files:
        return SensorResult(skip_reason="No new large files detected")
    
    run_requests = []
    
    for file_path, file_timestamp in new_large_files:
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        # Calculate appropriate batch size based on file size
        if file_size_mb > 100:
            batch_size = 50  # Smaller batches for very large files
        elif file_size_mb > 50:
            batch_size = 100
        else:
            batch_size = 200
        
        # Create special configuration for large files
        run_config = {
            "ops": {
                "raw_retail_data": {
                    "config": {
                        "input_file": file_path.name,
                        "enable_enrichment": False,  # Disable enrichment for large files initially
                        "batch_size": batch_size,
                    }
                }
            }
        }
        
        run_request = RunRequest(
            run_key=f"large_file_{file_path.stem}_{int(file_timestamp)}",
            run_config=run_config,
            tags={
                "source_file": file_path.name,
                "file_size_mb": f"{file_size_mb:.1f}",
                "trigger": "large_file",
                "batch_size": str(batch_size),
                "processing_mode": "large_file_optimized",
            }
        )
        
        run_requests.append(run_request)
        
        context.log.info(f"Detected large file: {file_path.name} ({file_size_mb:.1f}MB)")
    
    new_cursor = str(latest_timestamp)
    
    return SensorResult(
        run_requests=run_requests,
        cursor=new_cursor,
    )


@sensor(
    name="error_file_sensor",
    description="Monitors for files that failed processing and need retry",
    default_status=DefaultSensorStatus.STOPPED,
)
def error_file_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    Sensor for handling files that failed processing.
    
    This sensor monitors an error directory where failed files are moved
    and can trigger retry processing with different configurations.
    """
    error_data_path = Path(settings.raw_data_path) / "errors"
    
    if not error_data_path.exists():
        return SensorResult(skip_reason="Error directory does not exist")
    
    # Look for files in error directory
    error_files = list(error_data_path.glob("*.csv"))
    
    if not error_files:
        return SensorResult(skip_reason="No error files found")
    
    cursor = context.cursor or "0"
    last_processed_time = float(cursor)
    
    # Find files to retry
    retry_files = []
    latest_timestamp = last_processed_time
    
    for file_path in error_files:
        file_timestamp = file_path.stat().st_mtime
        
        # Only retry files that are at least 5 minutes old (avoid immediate retry loops)
        import time
        if file_timestamp > last_processed_time and (time.time() - file_timestamp) > 300:
            retry_files.append((file_path, file_timestamp))
            latest_timestamp = max(latest_timestamp, file_timestamp)
    
    if not retry_files:
        return SensorResult(skip_reason="No files ready for retry")
    
    run_requests = []
    
    for file_path, file_timestamp in retry_files:
        # Use conservative settings for retry
        run_config = {
            "ops": {
                "raw_retail_data": {
                    "config": {
                        "input_file": f"errors/{file_path.name}",
                        "enable_enrichment": False,  # Disable enrichment for retry
                        "batch_size": 50,  # Smaller batch size
                    }
                }
            }
        }
        
        run_request = RunRequest(
            run_key=f"retry_{file_path.stem}_{int(file_timestamp)}",
            run_config=run_config,
            tags={
                "source_file": file_path.name,
                "trigger": "error_retry",
                "retry_attempt": "true",
                "processing_mode": "conservative",
            }
        )
        
        run_requests.append(run_request)
        
        context.log.info(f"Retrying failed file: {file_path.name}")
    
    new_cursor = str(latest_timestamp)
    
    return SensorResult(
        run_requests=run_requests,
        cursor=new_cursor,
    )