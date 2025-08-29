"""
Application-Specific Logging System

This module provides specialized logging capabilities for different application components:
- ML pipeline execution logs and metrics
- Data pipeline processing logs and errors  
- API request and response logging
- Database query and performance logs
- Message queue processing logs
- Stream processing logs
- Business process logging and analytics
"""

import json
import time
import traceback
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Union
from enum import Enum
from dataclasses import dataclass, asdict, field
from contextlib import contextmanager
import functools
import threading
import queue
from pathlib import Path
import pickle

# Database and ML imports
import sqlalchemy
from sqlalchemy import event
import pandas as pd
import numpy as np

# Internal imports
from ..core.logging import get_logger
from .enterprise_datadog_log_collection import LogEntry, LogLevel, LogSource

logger = get_logger(__name__)


class ApplicationComponent(Enum):
    """Application component types"""
    ML_PIPELINE = "ml_pipeline"
    DATA_PIPELINE = "data_pipeline"
    API_GATEWAY = "api_gateway"
    DATABASE = "database"
    MESSAGE_QUEUE = "message_queue"
    STREAM_PROCESSOR = "stream_processor"
    BUSINESS_PROCESS = "business_process"
    ETL_PROCESSOR = "etl_processor"
    FEATURE_STORE = "feature_store"
    MODEL_SERVING = "model_serving"


class ProcessingStage(Enum):
    """Processing stage types"""
    INITIALIZATION = "initialization"
    DATA_LOADING = "data_loading"
    PREPROCESSING = "preprocessing"
    FEATURE_ENGINEERING = "feature_engineering"
    MODEL_TRAINING = "model_training"
    MODEL_VALIDATION = "model_validation"
    MODEL_DEPLOYMENT = "model_deployment"
    PREDICTION = "prediction"
    POST_PROCESSING = "post_processing"
    CLEANUP = "cleanup"


class PerformanceMetric(Enum):
    """Performance metric types"""
    EXECUTION_TIME = "execution_time"
    MEMORY_USAGE = "memory_usage"
    CPU_USAGE = "cpu_usage"
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    ERROR_RATE = "error_rate"
    SUCCESS_RATE = "success_rate"
    QUEUE_SIZE = "queue_size"
    CACHE_HIT_RATE = "cache_hit_rate"


@dataclass
class MLPipelineEvent:
    """ML Pipeline specific event"""
    pipeline_id: str
    run_id: str
    stage: ProcessingStage
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    dataset_info: Optional[Dict[str, Any]] = None
    hyperparameters: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, float]] = None
    feature_importance: Optional[Dict[str, float]] = None
    data_drift_score: Optional[float] = None
    model_accuracy: Optional[float] = None
    training_samples: Optional[int] = None
    validation_samples: Optional[int] = None
    experiment_config: Optional[Dict[str, Any]] = None
    artifacts_created: Optional[List[str]] = None
    
    def to_log_entry(self, level: LogLevel, message: str) -> LogEntry:
        """Convert to standard log entry"""
        return LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            source=LogSource.ML_PIPELINE,
            service=f"ml-{self.pipeline_id}",
            environment="production",
            tags={
                "pipeline_id": self.pipeline_id,
                "run_id": self.run_id,
                "stage": self.stage.value,
                "model_name": self.model_name or "unknown",
                "model_version": self.model_version or "unknown"
            },
            metadata={
                "dataset_info": self.dataset_info,
                "hyperparameters": self.hyperparameters,
                "metrics": self.metrics,
                "feature_importance": self.feature_importance,
                "data_drift_score": self.data_drift_score,
                "model_accuracy": self.model_accuracy,
                "training_samples": self.training_samples,
                "validation_samples": self.validation_samples,
                "experiment_config": self.experiment_config,
                "artifacts_created": self.artifacts_created
            }
        )


@dataclass
class DataPipelineEvent:
    """Data Pipeline specific event"""
    pipeline_id: str
    job_id: str
    stage: ProcessingStage
    input_sources: Optional[List[str]] = None
    output_destinations: Optional[List[str]] = None
    records_processed: Optional[int] = None
    records_failed: Optional[int] = None
    data_quality_score: Optional[float] = None
    schema_changes: Optional[List[str]] = None
    partition_info: Optional[Dict[str, Any]] = None
    compression_ratio: Optional[float] = None
    data_freshness: Optional[timedelta] = None
    lineage_info: Optional[Dict[str, Any]] = None
    
    def to_log_entry(self, level: LogLevel, message: str) -> LogEntry:
        """Convert to standard log entry"""
        return LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            source=LogSource.ETL_PIPELINE,
            service=f"data-{self.pipeline_id}",
            environment="production",
            tags={
                "pipeline_id": self.pipeline_id,
                "job_id": self.job_id,
                "stage": self.stage.value,
                "records_processed": str(self.records_processed or 0),
                "records_failed": str(self.records_failed or 0)
            },
            metadata={
                "input_sources": self.input_sources,
                "output_destinations": self.output_destinations,
                "data_quality_score": self.data_quality_score,
                "schema_changes": self.schema_changes,
                "partition_info": self.partition_info,
                "compression_ratio": self.compression_ratio,
                "data_freshness": self.data_freshness.total_seconds() if self.data_freshness else None,
                "lineage_info": self.lineage_info
            }
        )


@dataclass
class APIEvent:
    """API specific event"""
    request_id: str
    endpoint: str
    method: str
    status_code: Optional[int] = None
    response_time_ms: Optional[float] = None
    request_size_bytes: Optional[int] = None
    response_size_bytes: Optional[int] = None
    user_agent: Optional[str] = None
    client_ip: Optional[str] = None
    auth_method: Optional[str] = None
    rate_limit_remaining: Optional[int] = None
    cache_status: Optional[str] = None  # hit, miss, bypass
    upstream_service: Optional[str] = None
    business_context: Optional[Dict[str, Any]] = None
    
    def to_log_entry(self, level: LogLevel, message: str) -> LogEntry:
        """Convert to standard log entry"""
        return LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            source=LogSource.API,
            service="api-gateway",
            environment="production",
            request_id=self.request_id,
            ip_address=self.client_ip,
            user_agent=self.user_agent,
            status_code=self.status_code,
            duration_ms=self.response_time_ms,
            url=self.endpoint,
            tags={
                "method": self.method,
                "endpoint": self.endpoint,
                "status_code": str(self.status_code or 0),
                "auth_method": self.auth_method or "unknown",
                "cache_status": self.cache_status or "unknown"
            },
            metadata={
                "request_size_bytes": self.request_size_bytes,
                "response_size_bytes": self.response_size_bytes,
                "rate_limit_remaining": self.rate_limit_remaining,
                "upstream_service": self.upstream_service,
                "business_context": self.business_context
            }
        )


@dataclass
class DatabaseEvent:
    """Database specific event"""
    query_id: str
    query_type: str  # SELECT, INSERT, UPDATE, DELETE, DDL
    table_name: Optional[str] = None
    query_text: Optional[str] = None
    execution_time_ms: Optional[float] = None
    rows_affected: Optional[int] = None
    rows_examined: Optional[int] = None
    index_usage: Optional[List[str]] = None
    lock_time_ms: Optional[float] = None
    connection_pool_size: Optional[int] = None
    transaction_id: Optional[str] = None
    isolation_level: Optional[str] = None
    query_plan: Optional[Dict[str, Any]] = None
    cache_hit: Optional[bool] = None
    
    def to_log_entry(self, level: LogLevel, message: str) -> LogEntry:
        """Convert to standard log entry"""
        return LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            source=LogSource.DATABASE,
            service="database",
            environment="production",
            duration_ms=self.execution_time_ms,
            tags={
                "query_id": self.query_id,
                "query_type": self.query_type,
                "table_name": self.table_name or "unknown",
                "rows_affected": str(self.rows_affected or 0),
                "cache_hit": str(self.cache_hit) if self.cache_hit is not None else "unknown"
            },
            metadata={
                "query_text": self.query_text[:500] if self.query_text else None,  # Truncate long queries
                "rows_examined": self.rows_examined,
                "index_usage": self.index_usage,
                "lock_time_ms": self.lock_time_ms,
                "connection_pool_size": self.connection_pool_size,
                "transaction_id": self.transaction_id,
                "isolation_level": self.isolation_level,
                "query_plan": self.query_plan
            }
        )


class MLPipelineLogger:
    """Specialized logger for ML pipelines"""
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.logger = get_logger(f"ml_pipeline.{pipeline_id}")
        self.current_run_id = None
        self.run_metrics = {}
        self.stage_timings = {}
        
    def start_run(self, run_id: str, config: Optional[Dict[str, Any]] = None) -> str:
        """Start a new ML pipeline run"""
        self.current_run_id = run_id
        self.run_metrics = {}
        self.stage_timings = {}
        
        event = MLPipelineEvent(
            pipeline_id=self.pipeline_id,
            run_id=run_id,
            stage=ProcessingStage.INITIALIZATION,
            experiment_config=config
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Started ML pipeline run {run_id} for pipeline {self.pipeline_id}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
        return run_id
    
    def log_stage_start(self, stage: ProcessingStage, **kwargs):
        """Log the start of a processing stage"""
        if not self.current_run_id:
            self.current_run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.stage_timings[stage] = {"start_time": datetime.now()}
        
        event = MLPipelineEvent(
            pipeline_id=self.pipeline_id,
            run_id=self.current_run_id,
            stage=stage,
            **kwargs
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Started stage {stage.value} for run {self.current_run_id}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
    
    def log_stage_complete(self, stage: ProcessingStage, metrics: Optional[Dict[str, Any]] = None, **kwargs):
        """Log the completion of a processing stage"""
        if stage in self.stage_timings and "start_time" in self.stage_timings[stage]:
            duration = datetime.now() - self.stage_timings[stage]["start_time"]
            self.stage_timings[stage]["duration"] = duration
        
        if metrics:
            self.run_metrics.update(metrics)
        
        event = MLPipelineEvent(
            pipeline_id=self.pipeline_id,
            run_id=self.current_run_id,
            stage=stage,
            metrics=metrics,
            **kwargs
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Completed stage {stage.value} for run {self.current_run_id}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
    
    def log_model_training(self, model_name: str, model_version: str, 
                          hyperparameters: Dict[str, Any], training_samples: int):
        """Log model training details"""
        event = MLPipelineEvent(
            pipeline_id=self.pipeline_id,
            run_id=self.current_run_id,
            stage=ProcessingStage.MODEL_TRAINING,
            model_name=model_name,
            model_version=model_version,
            hyperparameters=hyperparameters,
            training_samples=training_samples
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Training model {model_name} v{model_version} with {training_samples} samples"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
    
    def log_model_metrics(self, model_name: str, metrics: Dict[str, float], 
                         feature_importance: Optional[Dict[str, float]] = None):
        """Log model performance metrics"""
        event = MLPipelineEvent(
            pipeline_id=self.pipeline_id,
            run_id=self.current_run_id,
            stage=ProcessingStage.MODEL_VALIDATION,
            model_name=model_name,
            metrics=metrics,
            feature_importance=feature_importance,
            model_accuracy=metrics.get("accuracy")
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Model {model_name} metrics: {json.dumps(metrics, indent=2)}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
    
    def log_data_drift(self, drift_score: float, threshold: float = 0.3):
        """Log data drift detection results"""
        level = LogLevel.WARNING if drift_score > threshold else LogLevel.INFO
        
        event = MLPipelineEvent(
            pipeline_id=self.pipeline_id,
            run_id=self.current_run_id,
            stage=ProcessingStage.PREPROCESSING,
            data_drift_score=drift_score
        )
        
        message = f"Data drift detected: score {drift_score:.3f} {'(above threshold)' if drift_score > threshold else '(within threshold)'}"
        log_entry = event.to_log_entry(level, message)
        
        self.logger.log(level.value, log_entry.message, extra={"datadog_event": event})
    
    def log_error(self, stage: ProcessingStage, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log pipeline errors"""
        event = MLPipelineEvent(
            pipeline_id=self.pipeline_id,
            run_id=self.current_run_id,
            stage=stage,
            experiment_config=context
        )
        
        log_entry = event.to_log_entry(
            LogLevel.ERROR,
            f"Error in stage {stage.value}: {str(error)}"
        )
        
        log_entry.exception = {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": traceback.format_exc()
        }
        
        self.logger.error(log_entry.message, extra={"datadog_event": event})
    
    @contextmanager
    def log_stage_context(self, stage: ProcessingStage, **kwargs):
        """Context manager for logging stage execution"""
        try:
            self.log_stage_start(stage, **kwargs)
            start_time = datetime.now()
            yield
            duration = (datetime.now() - start_time).total_seconds()
            self.log_stage_complete(stage, metrics={"duration_seconds": duration}, **kwargs)
        except Exception as e:
            self.log_error(stage, e, kwargs)
            raise


class DataPipelineLogger:
    """Specialized logger for data pipelines"""
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.logger = get_logger(f"data_pipeline.{pipeline_id}")
        self.current_job_id = None
        self.job_metrics = {}
        
    def start_job(self, job_id: str, input_sources: List[str], 
                  output_destinations: List[str]) -> str:
        """Start a new data pipeline job"""
        self.current_job_id = job_id
        self.job_metrics = {
            "start_time": datetime.now(),
            "records_processed": 0,
            "records_failed": 0
        }
        
        event = DataPipelineEvent(
            pipeline_id=self.pipeline_id,
            job_id=job_id,
            stage=ProcessingStage.INITIALIZATION,
            input_sources=input_sources,
            output_destinations=output_destinations
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Started data pipeline job {job_id} for pipeline {self.pipeline_id}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
        return job_id
    
    def log_data_processing(self, stage: ProcessingStage, records_processed: int, 
                           records_failed: int = 0, data_quality_score: Optional[float] = None):
        """Log data processing progress"""
        self.job_metrics["records_processed"] += records_processed
        self.job_metrics["records_failed"] += records_failed
        
        event = DataPipelineEvent(
            pipeline_id=self.pipeline_id,
            job_id=self.current_job_id,
            stage=stage,
            records_processed=records_processed,
            records_failed=records_failed,
            data_quality_score=data_quality_score
        )
        
        success_rate = (records_processed - records_failed) / records_processed if records_processed > 0 else 0
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Processed {records_processed} records in stage {stage.value} (success rate: {success_rate:.2%})"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
    
    def log_schema_change(self, table_name: str, changes: List[str]):
        """Log schema changes"""
        event = DataPipelineEvent(
            pipeline_id=self.pipeline_id,
            job_id=self.current_job_id,
            stage=ProcessingStage.PREPROCESSING,
            schema_changes=changes
        )
        
        log_entry = event.to_log_entry(
            LogLevel.WARNING,
            f"Schema changes detected in {table_name}: {', '.join(changes)}"
        )
        
        self.logger.warning(log_entry.message, extra={"datadog_event": event})
    
    def log_data_quality(self, quality_score: float, threshold: float = 0.95):
        """Log data quality assessment"""
        level = LogLevel.WARNING if quality_score < threshold else LogLevel.INFO
        
        event = DataPipelineEvent(
            pipeline_id=self.pipeline_id,
            job_id=self.current_job_id,
            stage=ProcessingStage.PREPROCESSING,
            data_quality_score=quality_score
        )
        
        message = f"Data quality score: {quality_score:.3f} {'(below threshold)' if quality_score < threshold else '(acceptable)'}"
        log_entry = event.to_log_entry(level, message)
        
        self.logger.log(level.value, log_entry.message, extra={"datadog_event": event})
    
    def log_lineage(self, lineage_info: Dict[str, Any]):
        """Log data lineage information"""
        event = DataPipelineEvent(
            pipeline_id=self.pipeline_id,
            job_id=self.current_job_id,
            stage=ProcessingStage.POST_PROCESSING,
            lineage_info=lineage_info
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Data lineage recorded for job {self.current_job_id}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})


class APILogger:
    """Specialized logger for API requests"""
    
    def __init__(self, service_name: str = "api-gateway"):
        self.service_name = service_name
        self.logger = get_logger(f"api.{service_name}")
    
    def log_request(self, request_id: str, method: str, endpoint: str, 
                   client_ip: str, user_agent: Optional[str] = None,
                   request_size: Optional[int] = None):
        """Log incoming API request"""
        event = APIEvent(
            request_id=request_id,
            endpoint=endpoint,
            method=method,
            client_ip=client_ip,
            user_agent=user_agent,
            request_size_bytes=request_size
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"API Request: {method} {endpoint} from {client_ip}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})
    
    def log_response(self, request_id: str, method: str, endpoint: str,
                    status_code: int, response_time_ms: float,
                    response_size: Optional[int] = None,
                    cache_status: Optional[str] = None):
        """Log API response"""
        level = LogLevel.ERROR if status_code >= 500 else LogLevel.WARNING if status_code >= 400 else LogLevel.INFO
        
        event = APIEvent(
            request_id=request_id,
            endpoint=endpoint,
            method=method,
            status_code=status_code,
            response_time_ms=response_time_ms,
            response_size_bytes=response_size,
            cache_status=cache_status
        )
        
        log_entry = event.to_log_entry(
            level,
            f"API Response: {method} {endpoint} -> {status_code} ({response_time_ms:.1f}ms)"
        )
        
        self.logger.log(level.value, log_entry.message, extra={"datadog_event": event})
    
    def log_rate_limit(self, client_ip: str, endpoint: str, remaining: int, limit: int):
        """Log rate limiting information"""
        level = LogLevel.WARNING if remaining < limit * 0.1 else LogLevel.INFO
        
        event = APIEvent(
            request_id="rate_limit_check",
            endpoint=endpoint,
            method="CHECK",
            client_ip=client_ip,
            rate_limit_remaining=remaining
        )
        
        log_entry = event.to_log_entry(
            level,
            f"Rate limit check for {client_ip} on {endpoint}: {remaining}/{limit} remaining"
        )
        
        self.logger.log(level.value, log_entry.message, extra={"datadog_event": event})
    
    def log_authentication(self, request_id: str, user_id: str, auth_method: str, 
                          success: bool, client_ip: str):
        """Log authentication attempts"""
        level = LogLevel.INFO if success else LogLevel.WARNING
        
        event = APIEvent(
            request_id=request_id,
            endpoint="/auth",
            method="POST",
            client_ip=client_ip,
            auth_method=auth_method,
            status_code=200 if success else 401
        )
        
        result = "successful" if success else "failed"
        log_entry = event.to_log_entry(
            level,
            f"Authentication {result} for user {user_id} via {auth_method} from {client_ip}"
        )
        
        self.logger.log(level.value, log_entry.message, extra={"datadog_event": event})
    
    @contextmanager
    def log_request_context(self, request_id: str, method: str, endpoint: str,
                           client_ip: str, **kwargs):
        """Context manager for API request/response logging"""
        start_time = datetime.now()
        self.log_request(request_id, method, endpoint, client_ip, **kwargs)
        
        try:
            yield
            # If no exception, assume success
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            self.log_response(request_id, method, endpoint, 200, response_time)
        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            status_code = getattr(e, 'status_code', 500)
            self.log_response(request_id, method, endpoint, status_code, response_time)
            raise


class DatabaseLogger:
    """Specialized logger for database operations"""
    
    def __init__(self, database_name: str = "main"):
        self.database_name = database_name
        self.logger = get_logger(f"database.{database_name}")
        self.query_counter = 0
        
    def log_query(self, query_type: str, table_name: str, query_text: str,
                 execution_time_ms: float, rows_affected: int = 0,
                 rows_examined: int = 0, transaction_id: Optional[str] = None):
        """Log database query execution"""
        self.query_counter += 1
        query_id = f"query_{self.query_counter}_{int(time.time())}"
        
        # Determine log level based on performance
        if execution_time_ms > 5000:  # >5 seconds
            level = LogLevel.WARNING
        elif execution_time_ms > 1000:  # >1 second
            level = LogLevel.INFO
        else:
            level = LogLevel.DEBUG
        
        event = DatabaseEvent(
            query_id=query_id,
            query_type=query_type,
            table_name=table_name,
            query_text=query_text,
            execution_time_ms=execution_time_ms,
            rows_affected=rows_affected,
            rows_examined=rows_examined,
            transaction_id=transaction_id
        )
        
        log_entry = event.to_log_entry(
            level,
            f"Query {query_type} on {table_name}: {execution_time_ms:.1f}ms, {rows_affected} rows affected"
        )
        
        self.logger.log(level.value, log_entry.message, extra={"datadog_event": event})
    
    def log_connection_pool(self, pool_size: int, active_connections: int, 
                           idle_connections: int):
        """Log connection pool status"""
        utilization = active_connections / pool_size if pool_size > 0 else 0
        level = LogLevel.WARNING if utilization > 0.8 else LogLevel.INFO
        
        event = DatabaseEvent(
            query_id="pool_status",
            query_type="POOL_STATUS",
            connection_pool_size=pool_size
        )
        
        log_entry = event.to_log_entry(
            level,
            f"Connection pool: {active_connections}/{pool_size} active, {idle_connections} idle ({utilization:.1%} utilization)"
        )
        
        self.logger.log(level.value, log_entry.message, extra={"datadog_event": event})
    
    def log_slow_query(self, query_text: str, execution_time_ms: float, 
                      query_plan: Optional[Dict[str, Any]] = None):
        """Log slow queries for optimization"""
        event = DatabaseEvent(
            query_id=f"slow_query_{int(time.time())}",
            query_type="SLOW_QUERY",
            query_text=query_text,
            execution_time_ms=execution_time_ms,
            query_plan=query_plan
        )
        
        log_entry = event.to_log_entry(
            LogLevel.WARNING,
            f"Slow query detected: {execution_time_ms:.1f}ms - {query_text[:100]}..."
        )
        
        self.logger.warning(log_entry.message, extra={"datadog_event": event})
    
    def log_transaction(self, transaction_id: str, operation: str, 
                       tables_affected: List[str], duration_ms: Optional[float] = None):
        """Log database transactions"""
        event = DatabaseEvent(
            query_id=transaction_id,
            query_type="TRANSACTION",
            transaction_id=transaction_id,
            execution_time_ms=duration_ms
        )
        
        log_entry = event.to_log_entry(
            LogLevel.INFO,
            f"Transaction {transaction_id} {operation}: {', '.join(tables_affected)}"
        )
        
        self.logger.info(log_entry.message, extra={"datadog_event": event})


class BusinessProcessLogger:
    """Specialized logger for business processes"""
    
    def __init__(self, process_name: str):
        self.process_name = process_name
        self.logger = get_logger(f"business.{process_name}")
    
    def log_process_start(self, process_id: str, context: Dict[str, Any]):
        """Log business process start"""
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message=f"Business process {self.process_name} started: {process_id}",
            source=LogSource.APPLICATION,
            service=f"business-{self.process_name}",
            environment="production",
            tags={
                "process_name": self.process_name,
                "process_id": process_id,
                "event_type": "process_start"
            },
            metadata=context
        )
        
        self.logger.info(log_entry.message, extra={"log_entry": log_entry})
    
    def log_business_event(self, event_type: str, event_data: Dict[str, Any], 
                          user_id: Optional[str] = None, amount: Optional[float] = None):
        """Log business events (orders, payments, etc.)"""
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message=f"Business event: {event_type}",
            source=LogSource.APPLICATION,
            service=f"business-{self.process_name}",
            environment="production",
            user_id=user_id,
            tags={
                "process_name": self.process_name,
                "event_type": event_type,
                "has_amount": str(amount is not None),
                "amount_range": self._categorize_amount(amount) if amount else None
            },
            metadata={
                "event_data": event_data,
                "amount": amount
            }
        )
        
        self.logger.info(log_entry.message, extra={"log_entry": log_entry})
    
    def log_compliance_event(self, regulation: str, event_details: Dict[str, Any], 
                           compliance_status: str = "compliant"):
        """Log compliance-related business events"""
        level = LogLevel.WARNING if compliance_status != "compliant" else LogLevel.INFO
        
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=f"Compliance event for {regulation}: {compliance_status}",
            source=LogSource.APPLICATION,
            service=f"business-{self.process_name}",
            environment="production",
            tags={
                "process_name": self.process_name,
                "event_type": "compliance",
                "regulation": regulation,
                "compliance_status": compliance_status
            },
            metadata=event_details
        )
        
        self.logger.log(level.value, log_entry.message, extra={"log_entry": log_entry})
    
    def _categorize_amount(self, amount: float) -> str:
        """Categorize monetary amounts for analysis"""
        if amount < 100:
            return "small"
        elif amount < 1000:
            return "medium"
        elif amount < 10000:
            return "large"
        else:
            return "very_large"


class PerformanceMonitor:
    """Performance monitoring for application components"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.PerformanceMonitor")
        self.metrics_cache = {}
        self.alert_thresholds = {
            PerformanceMetric.EXECUTION_TIME: 5000,  # ms
            PerformanceMetric.MEMORY_USAGE: 0.8,     # 80%
            PerformanceMetric.CPU_USAGE: 0.8,        # 80%
            PerformanceMetric.ERROR_RATE: 0.05       # 5%
        }
    
    def monitor_function_performance(self, component: ApplicationComponent, function_name: str):
        """Decorator for monitoring function performance"""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = datetime.now()
                start_memory = psutil.Process().memory_percent()
                start_cpu = psutil.Process().cpu_percent()
                
                try:
                    result = func(*args, **kwargs)
                    success = True
                    error = None
                except Exception as e:
                    success = False
                    error = e
                    raise
                finally:
                    end_time = datetime.now()
                    execution_time = (end_time - start_time).total_seconds() * 1000
                    end_memory = psutil.Process().memory_percent()
                    end_cpu = psutil.Process().cpu_percent()
                    
                    # Log performance metrics
                    self._log_performance_metrics(
                        component, function_name, execution_time,
                        end_memory - start_memory, end_cpu - start_cpu,
                        success, error
                    )
                
                return result
            return wrapper
        return decorator
    
    def _log_performance_metrics(self, component: ApplicationComponent, 
                                function_name: str, execution_time_ms: float,
                                memory_delta: float, cpu_delta: float,
                                success: bool, error: Optional[Exception]):
        """Log performance metrics"""
        level = LogLevel.ERROR if not success else LogLevel.WARNING if execution_time_ms > self.alert_thresholds[PerformanceMetric.EXECUTION_TIME] else LogLevel.INFO
        
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=f"Performance: {component.value}.{function_name} - {execution_time_ms:.1f}ms",
            source=LogSource.APPLICATION,
            service=f"performance-{component.value}",
            environment="production",
            duration_ms=execution_time_ms,
            tags={
                "component": component.value,
                "function": function_name,
                "success": str(success),
                "performance_category": self._categorize_performance(execution_time_ms)
            },
            metadata={
                "execution_time_ms": execution_time_ms,
                "memory_delta_percent": memory_delta,
                "cpu_delta_percent": cpu_delta,
                "error": str(error) if error else None,
                "thresholds": self.alert_thresholds
            }
        )
        
        if error:
            log_entry.exception = {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": traceback.format_exc()
            }
        
        self.logger.log(level.value, log_entry.message, extra={"log_entry": log_entry})
    
    def _categorize_performance(self, execution_time_ms: float) -> str:
        """Categorize performance"""
        if execution_time_ms < 100:
            return "excellent"
        elif execution_time_ms < 500:
            return "good"
        elif execution_time_ms < 1000:
            return "acceptable"
        elif execution_time_ms < 5000:
            return "slow"
        else:
            return "very_slow"


# Factory functions
def create_ml_pipeline_logger(pipeline_id: str) -> MLPipelineLogger:
    """Create ML pipeline logger"""
    return MLPipelineLogger(pipeline_id)

def create_data_pipeline_logger(pipeline_id: str) -> DataPipelineLogger:
    """Create data pipeline logger"""
    return DataPipelineLogger(pipeline_id)

def create_api_logger(service_name: str = "api-gateway") -> APILogger:
    """Create API logger"""
    return APILogger(service_name)

def create_database_logger(database_name: str = "main") -> DatabaseLogger:
    """Create database logger"""
    return DatabaseLogger(database_name)

def create_business_process_logger(process_name: str) -> BusinessProcessLogger:
    """Create business process logger"""
    return BusinessProcessLogger(process_name)

def create_performance_monitor() -> PerformanceMonitor:
    """Create performance monitor"""
    return PerformanceMonitor()


# Example usage
if __name__ == "__main__":
    # ML Pipeline logging example
    ml_logger = create_ml_pipeline_logger("customer_churn_prediction")
    
    run_id = ml_logger.start_run("run_20241201", {"model": "xgboost", "features": 50})
    
    with ml_logger.log_stage_context(ProcessingStage.DATA_LOADING):
        # Simulate data loading
        time.sleep(1)
    
    ml_logger.log_model_training("xgboost_v1", "1.0.0", {"n_estimators": 100}, 10000)
    ml_logger.log_model_metrics("xgboost_v1", {"accuracy": 0.92, "precision": 0.89, "recall": 0.85})
    ml_logger.log_data_drift(0.15)  # Low drift
    
    # API logging example
    api_logger = create_api_logger()
    
    with api_logger.log_request_context("req_123", "GET", "/api/v1/predictions", "192.168.1.100"):
        # Simulate API processing
        time.sleep(0.1)
    
    # Database logging example
    db_logger = create_database_logger()
    db_logger.log_query("SELECT", "users", "SELECT * FROM users WHERE active = 1", 45.2, rows_examined=1000)
    db_logger.log_connection_pool(20, 15, 5)
    
    # Business process logging example
    business_logger = create_business_process_logger("order_processing")
    business_logger.log_business_event("order_placed", {"order_id": "ORD-123", "customer_id": "CUST-456"}, amount=299.99)
    
    # Performance monitoring example
    perf_monitor = create_performance_monitor()
    
    @perf_monitor.monitor_function_performance(ApplicationComponent.ML_PIPELINE, "predict")
    def predict_customer_churn(customer_data):
        # Simulate prediction
        time.sleep(0.2)
        return {"churn_probability": 0.35}
    
    result = predict_customer_churn({"customer_id": "CUST-789"})
    
    print("Application-specific logging examples completed")
    print(f"Prediction result: {result}")