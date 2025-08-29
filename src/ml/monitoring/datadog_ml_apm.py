"""
DataDog APM Integration for ML Components
Provides comprehensive APM monitoring for ML pipelines, model serving, and feature engineering
"""

import asyncio
import json
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from ddtrace import tracer
from ddtrace.ext import SpanTypes

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_distributed_tracing import get_distributed_tracer, trace_ml_operation, TraceType, CorrelationLevel
from monitoring.datadog_error_tracking import get_error_tracker, capture_exception
from monitoring.datadog_profiling import get_datadog_profiler, profile_operation
from monitoring.datadog_business_metrics import get_business_metrics_tracker, track_kpi_value
from monitoring.datadog_apm_middleware import (
    DataDogTraceContext, trace_function, add_custom_tags, add_custom_metric
)

logger = get_logger(__name__)


class MLAPMTracker:
    """
    Comprehensive APM tracking for ML operations
    
    Features:
    - Model training pipeline tracing
    - Model serving and inference monitoring  
    - Feature pipeline performance tracking
    - A/B testing experiment monitoring
    - Model drift and performance tracking
    """
    
    def __init__(self, service_name: str = "ml-platform", datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Performance tracking
        self.inference_count = 0
        self.training_jobs = {}
        self.feature_pipeline_runs = {}
        self.experiments = {}
        
        # Initialize enhanced APM components
        self.distributed_tracer = get_distributed_tracer(f"{service_name}-ml-tracing", datadog_monitoring)
        self.error_tracker = get_error_tracker(f"{service_name}-ml-errors", datadog_monitoring)
        self.profiler = get_datadog_profiler(f"{service_name}-ml-profiler", datadog_monitoring)
        self.business_metrics = get_business_metrics_tracker(f"{service_name}-ml-business", datadog_monitoring)
        
        self.logger.info(f"Enhanced ML APM tracker initialized for {service_name}")
        
    @asynccontextmanager
    async def trace_ml_operation(self, operation_type: str, operation_name: str, 
                                model_id: Optional[str] = None, experiment_id: Optional[str] = None,
                                metadata: Optional[Dict[str, Any]] = None):
        """Context manager for tracing ML operations."""
        
        span_name = f"ml.{operation_type}"
        resource_name = f"{operation_type}.{operation_name}"
        
        tags = {
            "ml.operation_type": operation_type,
            "ml.operation_name": operation_name,
            "service.component": "ml"
        }
        
        if model_id:
            tags["ml.model_id"] = model_id
        if experiment_id:
            tags["ml.experiment_id"] = experiment_id
        if metadata:
            for key, value in metadata.items():
                tags[f"ml.{key}"] = str(value)
        
        with tracer.trace(
            name=span_name,
            service=self.service_name,
            resource=resource_name,
            span_type=SpanTypes.ML
        ) as span:
            
            # Set tags
            for key, value in tags.items():
                span.set_tag(key, value)
            
            # Set start time
            start_time = time.time()
            span.set_tag("ml.start_time", datetime.utcnow().isoformat())
            
            try:
                yield span
                span.set_tag("ml.success", True)
                
            except Exception as e:
                span.set_error(e)
                span.set_tag("ml.success", False)
                span.set_tag("ml.error_type", type(e).__name__)
                span.set_tag("ml.error_message", str(e))
                self.logger.error(f"ML operation failed: {operation_type}.{operation_name} - {str(e)}")
                raise
                
            finally:
                # Calculate duration
                duration = time.time() - start_time
                span.set_metric("ml.duration_seconds", duration)
                span.set_tag("ml.end_time", datetime.utcnow().isoformat())
                
                # Send custom metrics
                if self.datadog_monitoring:
                    await self._send_operation_metrics(operation_type, operation_name, duration, 
                                                     model_id, experiment_id, span.error)
    
    async def _send_operation_metrics(self, operation_type: str, operation_name: str, 
                                    duration: float, model_id: Optional[str] = None,
                                    experiment_id: Optional[str] = None, has_error: bool = False):
        """Send ML operation metrics to DataDog."""
        try:
            tags = [
                f"operation_type:{operation_type}",
                f"operation_name:{operation_name}",
                f"service:{self.service_name}"
            ]
            
            if model_id:
                tags.append(f"model_id:{model_id}")
            if experiment_id:
                tags.append(f"experiment_id:{experiment_id}")
            
            # Track operation count
            self.datadog_monitoring.counter(
                "ml.operations.total",
                tags=tags
            )
            
            # Track operation duration
            self.datadog_monitoring.histogram(
                "ml.operations.duration",
                duration * 1000,  # Convert to milliseconds
                tags=tags
            )
            
            # Track errors
            if has_error:
                self.datadog_monitoring.counter(
                    "ml.operations.errors",
                    tags=tags
                )
            
        except Exception as e:
            self.logger.warning(f"Failed to send ML operation metrics: {str(e)}")
    
    # Model Training APM
    
    async def trace_model_training(self, model_id: str, model_type: str, 
                                 training_config: Dict[str, Any]):
        """Context manager for tracing model training."""
        
        training_job_id = str(uuid.uuid4())
        self.training_jobs[training_job_id] = {
            "model_id": model_id,
            "model_type": model_type,
            "start_time": datetime.utcnow(),
            "config": training_config
        }
        
        metadata = {
            "model_type": model_type,
            "training_job_id": training_job_id,
            "hyperparameters": json.dumps(training_config.get("hyperparameters", {}))
        }
        
        return self.trace_ml_operation(
            operation_type="training",
            operation_name="model_training",
            model_id=model_id,
            metadata=metadata
        )
    
    @trace_function(operation_name="ml.training.data_preparation", service_name="ml-platform")
    async def trace_data_preparation(self, dataset_size: int, features_count: int,
                                   preprocessing_steps: List[str]) -> Dict[str, Any]:
        """Trace data preparation for training."""
        
        add_custom_tags({
            "ml.dataset_size": dataset_size,
            "ml.features_count": features_count,
            "ml.preprocessing_steps": ",".join(preprocessing_steps)
        })
        
        add_custom_metric("ml.dataset.size", float(dataset_size))
        add_custom_metric("ml.dataset.features", float(features_count))
        
        if self.datadog_monitoring:
            self.datadog_monitoring.gauge(
                "ml.training.dataset_size",
                dataset_size,
                tags=["component:data_preparation"]
            )
        
        return {
            "dataset_size": dataset_size,
            "features_count": features_count,
            "preprocessing_steps": preprocessing_steps
        }
    
    @trace_function(operation_name="ml.training.model_fitting", service_name="ml-platform")
    async def trace_model_fitting(self, model_id: str, training_samples: int,
                                validation_samples: int, epochs: Optional[int] = None) -> Dict[str, Any]:
        """Trace model fitting process."""
        
        add_custom_tags({
            "ml.model_id": model_id,
            "ml.training_samples": training_samples,
            "ml.validation_samples": validation_samples
        })
        
        if epochs:
            add_custom_tags({"ml.epochs": epochs})
            add_custom_metric("ml.training.epochs", float(epochs))
        
        add_custom_metric("ml.training.samples", float(training_samples))
        add_custom_metric("ml.validation.samples", float(validation_samples))
        
        if self.datadog_monitoring:
            tags = [f"model_id:{model_id}"]
            
            self.datadog_monitoring.gauge("ml.training.samples", training_samples, tags=tags)
            self.datadog_monitoring.gauge("ml.validation.samples", validation_samples, tags=tags)
            
            if epochs:
                self.datadog_monitoring.gauge("ml.training.epochs", epochs, tags=tags)
        
        return {
            "model_id": model_id,
            "training_samples": training_samples,
            "validation_samples": validation_samples,
            "epochs": epochs
        }
    
    @trace_function(operation_name="ml.training.evaluation", service_name="ml-platform")
    async def trace_model_evaluation(self, model_id: str, metrics: Dict[str, float],
                                   test_samples: int) -> Dict[str, Any]:
        """Trace model evaluation."""
        
        add_custom_tags({
            "ml.model_id": model_id,
            "ml.test_samples": test_samples
        })
        
        # Add metrics as custom tags and metrics
        for metric_name, metric_value in metrics.items():
            add_custom_tags({f"ml.metric.{metric_name}": metric_value})
            add_custom_metric(f"ml.evaluation.{metric_name}", metric_value)
        
        add_custom_metric("ml.evaluation.test_samples", float(test_samples))
        
        if self.datadog_monitoring:
            tags = [f"model_id:{model_id}"]
            
            # Send evaluation metrics
            for metric_name, metric_value in metrics.items():
                self.datadog_monitoring.gauge(
                    f"ml.model.evaluation.{metric_name}",
                    metric_value,
                    tags=tags
                )
            
            self.datadog_monitoring.gauge("ml.evaluation.test_samples", test_samples, tags=tags)
        
        return {
            "model_id": model_id,
            "metrics": metrics,
            "test_samples": test_samples
        }
    
    # Model Serving APM
    
    async def trace_model_inference(self, model_id: str, model_version: str,
                                  input_features: Union[pd.DataFrame, Dict[str, Any], List]):
        """Context manager for tracing model inference."""
        
        # Calculate input characteristics
        if isinstance(input_features, pd.DataFrame):
            batch_size = len(input_features)
            feature_count = len(input_features.columns)
        elif isinstance(input_features, dict):
            batch_size = 1
            feature_count = len(input_features)
        elif isinstance(input_features, list):
            batch_size = len(input_features)
            feature_count = len(input_features[0]) if input_features and isinstance(input_features[0], (list, dict)) else 0
        else:
            batch_size = 1
            feature_count = 0
        
        metadata = {
            "model_version": model_version,
            "batch_size": batch_size,
            "feature_count": feature_count,
            "inference_id": str(uuid.uuid4())
        }
        
        return self.trace_ml_operation(
            operation_type="inference",
            operation_name="model_prediction",
            model_id=model_id,
            metadata=metadata
        )
    
    @trace_function(operation_name="ml.inference.preprocessing", service_name="ml-platform")
    async def trace_inference_preprocessing(self, model_id: str, input_size: int,
                                          preprocessing_steps: List[str]) -> Dict[str, Any]:
        """Trace inference preprocessing."""
        
        add_custom_tags({
            "ml.model_id": model_id,
            "ml.input_size": input_size,
            "ml.preprocessing_steps": ",".join(preprocessing_steps)
        })
        
        add_custom_metric("ml.inference.input_size", float(input_size))
        
        if self.datadog_monitoring:
            self.datadog_monitoring.histogram(
                "ml.inference.preprocessing.input_size",
                input_size,
                tags=[f"model_id:{model_id}"]
            )
        
        return {
            "model_id": model_id,
            "input_size": input_size,
            "preprocessing_steps": preprocessing_steps
        }
    
    @trace_function(operation_name="ml.inference.prediction", service_name="ml-platform")
    async def trace_prediction_generation(self, model_id: str, prediction_count: int,
                                        prediction_confidence: Optional[float] = None) -> Dict[str, Any]:
        """Trace prediction generation."""
        
        self.inference_count += prediction_count
        
        add_custom_tags({
            "ml.model_id": model_id,
            "ml.prediction_count": prediction_count
        })
        
        add_custom_metric("ml.inference.prediction_count", float(prediction_count))
        
        if prediction_confidence is not None:
            add_custom_tags({"ml.prediction_confidence": prediction_confidence})
            add_custom_metric("ml.inference.confidence", prediction_confidence)
        
        if self.datadog_monitoring:
            tags = [f"model_id:{model_id}"]
            
            self.datadog_monitoring.counter(
                "ml.inference.predictions.total",
                prediction_count,
                tags=tags
            )
            
            if prediction_confidence is not None:
                self.datadog_monitoring.histogram(
                    "ml.inference.prediction_confidence",
                    prediction_confidence,
                    tags=tags
                )
        
        return {
            "model_id": model_id,
            "prediction_count": prediction_count,
            "prediction_confidence": prediction_confidence,
            "total_inferences": self.inference_count
        }
    
    # Feature Pipeline APM
    
    async def trace_feature_pipeline(self, pipeline_name: str, pipeline_version: str,
                                   input_sources: List[str]):
        """Context manager for tracing feature pipelines."""
        
        pipeline_run_id = str(uuid.uuid4())
        self.feature_pipeline_runs[pipeline_run_id] = {
            "pipeline_name": pipeline_name,
            "pipeline_version": pipeline_version,
            "start_time": datetime.utcnow(),
            "input_sources": input_sources
        }
        
        metadata = {
            "pipeline_version": pipeline_version,
            "pipeline_run_id": pipeline_run_id,
            "input_sources": ",".join(input_sources)
        }
        
        return self.trace_ml_operation(
            operation_type="feature_engineering",
            operation_name=pipeline_name,
            metadata=metadata
        )
    
    @trace_function(operation_name="ml.features.extraction", service_name="ml-platform")
    async def trace_feature_extraction(self, pipeline_name: str, raw_records: int,
                                     extracted_features: int) -> Dict[str, Any]:
        """Trace feature extraction."""
        
        add_custom_tags({
            "ml.pipeline_name": pipeline_name,
            "ml.raw_records": raw_records,
            "ml.extracted_features": extracted_features
        })
        
        add_custom_metric("ml.features.raw_records", float(raw_records))
        add_custom_metric("ml.features.extracted", float(extracted_features))
        
        if self.datadog_monitoring:
            tags = [f"pipeline:{pipeline_name}"]
            
            self.datadog_monitoring.gauge("ml.features.raw_records", raw_records, tags=tags)
            self.datadog_monitoring.gauge("ml.features.extracted", extracted_features, tags=tags)
        
        return {
            "pipeline_name": pipeline_name,
            "raw_records": raw_records,
            "extracted_features": extracted_features
        }
    
    @trace_function(operation_name="ml.features.transformation", service_name="ml-platform")
    async def trace_feature_transformation(self, pipeline_name: str, transformation_steps: List[str],
                                         features_before: int, features_after: int) -> Dict[str, Any]:
        """Trace feature transformation."""
        
        add_custom_tags({
            "ml.pipeline_name": pipeline_name,
            "ml.transformation_steps": ",".join(transformation_steps),
            "ml.features_before": features_before,
            "ml.features_after": features_after
        })
        
        add_custom_metric("ml.features.before_transformation", float(features_before))
        add_custom_metric("ml.features.after_transformation", float(features_after))
        
        feature_reduction = ((features_before - features_after) / features_before * 100) if features_before > 0 else 0
        add_custom_metric("ml.features.reduction_percent", feature_reduction)
        
        if self.datadog_monitoring:
            tags = [f"pipeline:{pipeline_name}"]
            
            self.datadog_monitoring.gauge("ml.features.before_transformation", features_before, tags=tags)
            self.datadog_monitoring.gauge("ml.features.after_transformation", features_after, tags=tags)
            self.datadog_monitoring.gauge("ml.features.reduction_percent", feature_reduction, tags=tags)
        
        return {
            "pipeline_name": pipeline_name,
            "transformation_steps": transformation_steps,
            "features_before": features_before,
            "features_after": features_after,
            "reduction_percent": feature_reduction
        }
    
    # A/B Testing APM
    
    async def trace_ab_experiment(self, experiment_id: str, experiment_name: str,
                                model_a_id: str, model_b_id: str, traffic_split: float):
        """Context manager for tracing A/B testing experiments."""
        
        self.experiments[experiment_id] = {
            "experiment_name": experiment_name,
            "model_a_id": model_a_id,
            "model_b_id": model_b_id,
            "traffic_split": traffic_split,
            "start_time": datetime.utcnow(),
            "requests_a": 0,
            "requests_b": 0
        }
        
        metadata = {
            "experiment_name": experiment_name,
            "model_a_id": model_a_id,
            "model_b_id": model_b_id,
            "traffic_split": traffic_split
        }
        
        return self.trace_ml_operation(
            operation_type="ab_testing",
            operation_name=experiment_name,
            experiment_id=experiment_id,
            metadata=metadata
        )
    
    @trace_function(operation_name="ml.ab_test.assignment", service_name="ml-platform")
    async def trace_experiment_assignment(self, experiment_id: str, user_id: str,
                                        assigned_variant: str, assignment_reason: str) -> Dict[str, Any]:
        """Trace A/B experiment assignment."""
        
        add_custom_tags({
            "ml.experiment_id": experiment_id,
            "ml.assigned_variant": assigned_variant,
            "ml.assignment_reason": assignment_reason
        })
        
        # Update experiment counters
        if experiment_id in self.experiments:
            if assigned_variant == "A":
                self.experiments[experiment_id]["requests_a"] += 1
            elif assigned_variant == "B":
                self.experiments[experiment_id]["requests_b"] += 1
        
        if self.datadog_monitoring:
            tags = [
                f"experiment_id:{experiment_id}",
                f"variant:{assigned_variant}",
                f"assignment_reason:{assignment_reason}"
            ]
            
            self.datadog_monitoring.counter("ml.ab_test.assignments", tags=tags)
        
        return {
            "experiment_id": experiment_id,
            "user_id": user_id,
            "assigned_variant": assigned_variant,
            "assignment_reason": assignment_reason
        }
    
    @trace_function(operation_name="ml.ab_test.conversion", service_name="ml-platform")
    async def trace_experiment_conversion(self, experiment_id: str, variant: str,
                                        conversion_type: str, conversion_value: Optional[float] = None) -> Dict[str, Any]:
        """Trace A/B experiment conversion."""
        
        add_custom_tags({
            "ml.experiment_id": experiment_id,
            "ml.variant": variant,
            "ml.conversion_type": conversion_type
        })
        
        if conversion_value is not None:
            add_custom_tags({"ml.conversion_value": conversion_value})
            add_custom_metric("ml.ab_test.conversion_value", conversion_value)
        
        if self.datadog_monitoring:
            tags = [
                f"experiment_id:{experiment_id}",
                f"variant:{variant}",
                f"conversion_type:{conversion_type}"
            ]
            
            self.datadog_monitoring.counter("ml.ab_test.conversions", tags=tags)
            
            if conversion_value is not None:
                self.datadog_monitoring.histogram("ml.ab_test.conversion_value", conversion_value, tags=tags)
        
        return {
            "experiment_id": experiment_id,
            "variant": variant,
            "conversion_type": conversion_type,
            "conversion_value": conversion_value
        }
    
    # Model Monitoring APM
    
    @trace_function(operation_name="ml.monitoring.drift_detection", service_name="ml-platform")
    async def trace_drift_detection(self, model_id: str, feature_name: str,
                                  drift_score: float, drift_threshold: float,
                                  is_drift_detected: bool) -> Dict[str, Any]:
        """Trace model drift detection."""
        
        add_custom_tags({
            "ml.model_id": model_id,
            "ml.feature_name": feature_name,
            "ml.drift_score": drift_score,
            "ml.drift_threshold": drift_threshold,
            "ml.drift_detected": is_drift_detected
        })
        
        add_custom_metric("ml.monitoring.drift_score", drift_score)
        
        if self.datadog_monitoring:
            tags = [
                f"model_id:{model_id}",
                f"feature:{feature_name}",
                f"drift_detected:{is_drift_detected}"
            ]
            
            self.datadog_monitoring.gauge("ml.model.drift_score", drift_score, tags=tags)
            
            if is_drift_detected:
                self.datadog_monitoring.counter("ml.model.drift_alerts", tags=tags)
        
        return {
            "model_id": model_id,
            "feature_name": feature_name,
            "drift_score": drift_score,
            "drift_threshold": drift_threshold,
            "is_drift_detected": is_drift_detected
        }
    
    @trace_function(operation_name="ml.monitoring.performance_check", service_name="ml-platform")
    async def trace_performance_monitoring(self, model_id: str, performance_metrics: Dict[str, float],
                                         baseline_metrics: Dict[str, float]) -> Dict[str, Any]:
        """Trace model performance monitoring."""
        
        add_custom_tags({
            "ml.model_id": model_id
        })
        
        # Calculate performance degradation
        degradation_metrics = {}
        for metric_name, current_value in performance_metrics.items():
            if metric_name in baseline_metrics:
                baseline_value = baseline_metrics[metric_name]
                if baseline_value != 0:
                    degradation_pct = ((baseline_value - current_value) / baseline_value) * 100
                    degradation_metrics[f"{metric_name}_degradation_pct"] = degradation_pct
                    
                    add_custom_tags({f"ml.{metric_name}": current_value})
                    add_custom_tags({f"ml.{metric_name}_baseline": baseline_value})
                    add_custom_tags({f"ml.{metric_name}_degradation_pct": degradation_pct})
                    
                    add_custom_metric(f"ml.performance.{metric_name}", current_value)
                    add_custom_metric(f"ml.performance.{metric_name}_degradation", degradation_pct)
        
        if self.datadog_monitoring:
            tags = [f"model_id:{model_id}"]
            
            for metric_name, metric_value in performance_metrics.items():
                self.datadog_monitoring.gauge(f"ml.model.performance.{metric_name}", metric_value, tags=tags)
            
            for degradation_name, degradation_value in degradation_metrics.items():
                self.datadog_monitoring.gauge(f"ml.model.{degradation_name}", degradation_value, tags=tags)
        
        return {
            "model_id": model_id,
            "performance_metrics": performance_metrics,
            "baseline_metrics": baseline_metrics,
            "degradation_metrics": degradation_metrics
        }
    
    # Reporting and Analytics
    
    def get_ml_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of ML metrics."""
        
        active_training_jobs = len(self.training_jobs)
        active_experiments = len(self.experiments)
        active_pipelines = len(self.feature_pipeline_runs)
        
        # Calculate experiment stats
        experiment_stats = {}
        for exp_id, exp_data in self.experiments.items():
            total_requests = exp_data["requests_a"] + exp_data["requests_b"]
            if total_requests > 0:
                split_ratio = exp_data["requests_a"] / total_requests
            else:
                split_ratio = 0
            
            experiment_stats[exp_id] = {
                "name": exp_data["experiment_name"],
                "total_requests": total_requests,
                "requests_a": exp_data["requests_a"],
                "requests_b": exp_data["requests_b"],
                "actual_split_ratio": split_ratio,
                "target_split": exp_data["traffic_split"],
                "duration_hours": (datetime.utcnow() - exp_data["start_time"]).total_seconds() / 3600
            }
        
        return {
            "service_name": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {
                "total_inferences": self.inference_count,
                "active_training_jobs": active_training_jobs,
                "active_experiments": active_experiments,
                "active_feature_pipelines": active_pipelines
            },
            "training_jobs": list(self.training_jobs.keys()),
            "experiments": experiment_stats,
            "feature_pipelines": list(self.feature_pipeline_runs.keys())
        }


# Global ML APM tracker
_ml_apm_tracker: Optional[MLAPMTracker] = None


def get_ml_apm_tracker(service_name: str = "ml-platform", 
                      datadog_monitoring: Optional[DatadogMonitoring] = None) -> MLAPMTracker:
    """Get or create ML APM tracker."""
    global _ml_apm_tracker
    
    if _ml_apm_tracker is None:
        _ml_apm_tracker = MLAPMTracker(service_name, datadog_monitoring)
    
    return _ml_apm_tracker


# Convenience functions for common ML operations

async def trace_model_training_session(model_id: str, model_type: str, training_config: Dict[str, Any]):
    """Convenience function for tracing model training."""
    tracker = get_ml_apm_tracker()
    return await tracker.trace_model_training(model_id, model_type, training_config)


async def trace_model_inference_session(model_id: str, model_version: str, input_features: Any):
    """Convenience function for tracing model inference."""
    tracker = get_ml_apm_tracker()
    return await tracker.trace_model_inference(model_id, model_version, input_features)


async def trace_feature_pipeline_session(pipeline_name: str, pipeline_version: str, input_sources: List[str]):
    """Convenience function for tracing feature pipelines."""
    tracker = get_ml_apm_tracker()
    return await tracker.trace_feature_pipeline(pipeline_name, pipeline_version, input_sources)


async def trace_ab_experiment_session(experiment_id: str, experiment_name: str,
                                    model_a_id: str, model_b_id: str, traffic_split: float):
    """Convenience function for tracing A/B experiments."""
    tracker = get_ml_apm_tracker()
    return await tracker.trace_ab_experiment(experiment_id, experiment_name, model_a_id, model_b_id, traffic_split)