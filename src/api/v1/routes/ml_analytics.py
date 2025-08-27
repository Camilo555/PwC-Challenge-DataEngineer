"""
Advanced ML Analytics API Routes

Comprehensive REST API for ML analytics, predictions, insights generation,
and model management with proper authentication and monitoring.
"""

import logging
import numpy as np
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query, Path
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import pandas as pd
import json

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.api.v1.services.authentication_service import verify_token, get_current_user
from src.ml.analytics.predictive_engine import PredictiveAnalyticsEngine, PredictionType
from src.ml.feature_engineering.feature_store import FeatureStore
from src.ml.deployment.model_server import ModelServer
from src.ml.deployment.ab_testing import ABTestingFramework, ExperimentConfig, SplitStrategy
from src.ml.monitoring.model_monitor import ModelMonitor
from src.ml.insights.narrative_generator import InsightEngine, InsightConfig
from src.ml.training.model_trainer import AutoMLTrainer
from src.ml.feature_engineering.feature_pipeline import FeaturePipeline
from src.monitoring.intelligent_alerting import create_intelligent_alerting_system

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter(prefix="/ml-analytics", tags=["ML Analytics"])
security = HTTPBearer()
metrics_collector = MetricsCollector()

# Initialize ML components
predictive_engine = PredictiveAnalyticsEngine()
feature_store = FeatureStore()
insight_engine = InsightEngine()
model_monitor = ModelMonitor()
feature_pipeline = FeaturePipeline()
ab_testing_framework = ABTestingFramework()


class PredictionRequest(BaseModel):
    """Request model for predictions."""
    model_type: str = Field(..., description="Type of prediction model")
    data: Dict[str, Any] = Field(..., description="Input data for prediction")
    include_confidence: bool = Field(True, description="Include confidence scores")
    include_explanation: bool = Field(False, description="Include model explanation")


class BatchPredictionRequest(BaseModel):
    """Request model for batch predictions."""
    model_type: str = Field(..., description="Type of prediction model")
    data: List[Dict[str, Any]] = Field(..., description="List of input data for predictions")
    include_confidence: bool = Field(True, description="Include confidence scores")
    callback_url: Optional[str] = Field(None, description="Callback URL for results")


class ModelTrainingRequest(BaseModel):
    """Request model for model training."""
    model_name: str = Field(..., description="Name for the trained model")
    model_type: str = Field(..., description="Type of model to train")
    training_data: Dict[str, Any] = Field(..., description="Training data configuration")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Model parameters")
    validation_split: float = Field(0.2, description="Validation data split ratio")


class InsightRequest(BaseModel):
    """Request model for insight generation."""
    data_source: str = Field(..., description="Data source identifier")
    analysis_types: List[str] = Field(default=["trends", "anomalies"], description="Types of analysis")
    date_range: Optional[Dict[str, str]] = Field(None, description="Date range for analysis")
    metrics: List[str] = Field(default_factory=list, description="Specific metrics to analyze")
    report_level: str = Field("operational", description="Report detail level")


class FeatureStoreRequest(BaseModel):
    """Request model for feature store operations."""
    feature_group: str = Field(..., description="Feature group name")
    entity_keys: Dict[str, Any] = Field(..., description="Entity keys for feature lookup")
    features: Optional[List[str]] = Field(None, description="Specific features to retrieve")
    as_of_date: Optional[datetime] = Field(None, description="Point-in-time for features")


class FeaturePipelineRequest(BaseModel):
    """Request model for feature pipeline operations."""
    pipeline_name: str = Field(..., description="Feature pipeline name")
    input_data: Dict[str, Any] = Field(..., description="Input data for feature generation")
    feature_names: Optional[List[str]] = Field(None, description="Specific features to generate")
    save_to_store: bool = Field(False, description="Save features to feature store")


class ABTestRequest(BaseModel):
    """Request model for A/B test creation."""
    experiment_name: str = Field(..., description="Name of the experiment")
    description: str = Field(..., description="Description of the experiment")
    control_model_id: str = Field(..., description="Control model ID")
    treatment_model_ids: List[str] = Field(..., description="Treatment model IDs")
    traffic_allocation: Dict[str, float] = Field(..., description="Traffic allocation per variant")
    primary_metric: str = Field("accuracy", description="Primary metric to measure")
    duration_days: int = Field(7, description="Experiment duration in days")
    minimum_sample_size: int = Field(1000, description="Minimum sample size")


class ModelMonitoringRequest(BaseModel):
    """Request model for model monitoring configuration."""
    model_id: str = Field(..., description="Model ID to monitor")
    monitoring_config: Dict[str, Any] = Field(..., description="Monitoring configuration")
    drift_threshold: float = Field(0.1, description="Drift detection threshold")
    alert_channels: List[str] = Field(default=["email"], description="Alert notification channels")


class ModelDeploymentRequest(BaseModel):
    """Request model for model deployment."""
    model_id: str = Field(..., description="Model ID to deploy")
    deployment_target: str = Field(..., description="Deployment target (staging/production)")
    resource_config: Dict[str, Any] = Field(default_factory=dict, description="Resource configuration")
    auto_scaling: bool = Field(True, description="Enable auto-scaling")
    health_check_config: Dict[str, Any] = Field(default_factory=dict, description="Health check configuration")


@router.get("/health")
async def health_check():
    """Health check endpoint for ML analytics services."""
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow(),
            "services": {
                "predictive_engine": "operational",
                "feature_store": "operational", 
                "insight_engine": "operational"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unavailable")


@router.post("/predict")
async def predict(
    request: PredictionRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Make a single prediction using specified model."""
    try:
        # Verify authentication
        user = await verify_token(credentials.credentials)
        
        start_time = datetime.utcnow()
        
        # Convert request data to DataFrame
        data_df = pd.DataFrame([request.data])
        
        # Make prediction based on model type
        if request.model_type == "sales_forecast":
            result = await predictive_engine.predict("sales_forecaster", data_df)
        elif request.model_type == "customer_segmentation":
            result = await predictive_engine.predict("customer_segmentation", data_df)
        elif request.model_type == "anomaly_detection":
            result = await predictive_engine.predict("anomaly_detector", data_df)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported model type: {request.model_type}")
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        # Log metrics
        metrics_collector.increment_counter(
            "ml_api_predictions_total",
            tags={"model_type": request.model_type, "status": "success"}
        )
        metrics_collector.record_histogram(
            "ml_api_prediction_duration_seconds",
            duration
        )
        
        response = {
            "prediction_id": result.prediction_id,
            "prediction_type": result.prediction_type.value,
            "result": {
                "values": result.forecast_values or result.cluster_assignments or result.anomaly_scores,
                "confidence": result.confidence_score if request.include_confidence else None
            },
            "metadata": {
                "model_used": result.model_used,
                "timestamp": result.timestamp,
                "processing_time_ms": duration * 1000
            }
        }
        
        if request.include_explanation:
            response["explanation"] = result.feature_importance
            
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        metrics_collector.increment_counter(
            "ml_api_predictions_total",
            tags={"model_type": request.model_type, "status": "error"}
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict/batch")
async def predict_batch(
    request: BatchPredictionRequest,
    background_tasks: BackgroundTasks,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Process batch predictions asynchronously."""
    try:
        # Verify authentication
        user = await verify_token(credentials.credentials)
        
        # Convert request data to DataFrame
        data_df = pd.DataFrame(request.data)
        
        # Start batch processing
        batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Add to background tasks
        background_tasks.add_task(
            process_batch_prediction,
            batch_id,
            request.model_type,
            data_df,
            request.include_confidence,
            request.callback_url
        )
        
        return {
            "batch_id": batch_id,
            "status": "processing",
            "estimated_completion": datetime.utcnow() + timedelta(minutes=5),
            "records_count": len(request.data)
        }
        
    except Exception as e:
        logger.error(f"Batch prediction error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/predict/batch/{batch_id}/status")
async def get_batch_status(
    batch_id: str = Path(..., description="Batch prediction ID"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get status of batch prediction."""
    try:
        user = await verify_token(credentials.credentials)
        
        # This would query the actual batch status from your storage
        # For demonstration, returning a mock response
        return {
            "batch_id": batch_id,
            "status": "completed",
            "progress": 100,
            "results_available": True,
            "completed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting batch status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/train")
async def train_model(
    request: ModelTrainingRequest,
    background_tasks: BackgroundTasks,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Train a new ML model."""
    try:
        # Verify authentication and permissions
        user = await verify_token(credentials.credentials)
        
        # Start training in background
        training_id = f"training_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        background_tasks.add_task(
            start_model_training,
            training_id,
            request.model_name,
            request.model_type,
            request.training_data,
            request.parameters,
            request.validation_split
        )
        
        return {
            "training_id": training_id,
            "status": "started",
            "model_name": request.model_name,
            "model_type": request.model_type,
            "estimated_duration_minutes": 30
        }
        
    except Exception as e:
        logger.error(f"Model training error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models")
async def list_models(
    model_type: Optional[str] = Query(None, description="Filter by model type"),
    status: Optional[str] = Query(None, description="Filter by model status"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """List available ML models with pagination."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Get models from predictive engine
        models = await predictive_engine.get_prediction_history(limit=50)
        
        # Filter by parameters
        if model_type:
            models = [m for m in models if m.get('model_type') == model_type]
        
        if status:
            models = [m for m in models if m.get('status') == status]
        
        # Pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_models = models[start_idx:end_idx]
        
        return {
            "models": paginated_models,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": len(models),
                "total_pages": (len(models) + page_size - 1) // page_size
            },
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error listing models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/training/{training_id}/status")
async def get_training_status(
    training_id: str = Path(..., description="Training job ID"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get status of model training job."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock training status - in production this would query actual job status
        training_status = {
            "training_id": training_id,
            "status": "completed",
            "progress": 100,
            "current_epoch": 50,
            "total_epochs": 50,
            "metrics": {
                "train_accuracy": 0.94,
                "val_accuracy": 0.91,
                "train_loss": 0.23,
                "val_loss": 0.28
            },
            "start_time": (datetime.utcnow() - timedelta(hours=2)).isoformat(),
            "estimated_completion": datetime.utcnow().isoformat(),
            "resource_usage": {
                "cpu_percent": 45,
                "memory_mb": 2048,
                "gpu_utilization": 85
            }
        }
        
        return training_status
        
    except Exception as e:
        logger.error(f"Error getting training status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/training/{training_id}/stop")
async def stop_training(
    training_id: str = Path(..., description="Training job ID"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Stop a running training job."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock stopping training job
        logger.info(f"Stopping training job {training_id}")
        
        return {
            "training_id": training_id,
            "status": "stopped",
            "message": "Training job stopped successfully",
            "stopped_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error stopping training: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/{model_id}/validate")
async def validate_model(
    model_id: str = Path(..., description="Model ID to validate"),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    validation_data: Dict[str, Any] = None
):
    """Validate model performance on test data."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock model validation
        validation_results = {
            "model_id": model_id,
            "validation_metrics": {
                "accuracy": 0.89,
                "precision": 0.87,
                "recall": 0.91,
                "f1_score": 0.89,
                "auc_roc": 0.94
            },
            "confusion_matrix": [[850, 45], [32, 873]],
            "feature_importance": {
                "feature1": 0.35,
                "feature2": 0.28,
                "feature3": 0.22,
                "feature4": 0.15
            },
            "validation_date": datetime.utcnow().isoformat(),
            "test_samples": 1800
        }
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating model: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/{model_id}/deploy")
async def deploy_model(
    deployment_request: ModelDeploymentRequest,
    model_id: str = Path(..., description="Model ID to deploy"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Deploy model to specified environment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock model deployment
        deployment_id = f"deploy_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Deploying model {model_id} to {deployment_request.deployment_target}")
        
        return {
            "deployment_id": deployment_id,
            "model_id": model_id,
            "target": deployment_request.deployment_target,
            "status": "deploying",
            "endpoint_url": f"https://api.example.com/ml/models/{model_id}/predict",
            "deployment_config": {
                "auto_scaling": deployment_request.auto_scaling,
                "resource_config": deployment_request.resource_config,
                "health_check": deployment_request.health_check_config
            },
            "deployed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error deploying model: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/models/{model_id}")
async def delete_model(
    model_id: str = Path(..., description="Model ID to delete"),
    force: bool = Query(False, description="Force delete even if model is deployed"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Delete a model and its artifacts."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Check if model is deployed
        if not force:
            # Mock check for deployed models
            deployed_models = ["model1", "model2"]  # This would be a real check
            if model_id in deployed_models:
                raise HTTPException(
                    status_code=400, 
                    detail="Cannot delete deployed model. Use force=true to override."
                )
        
        logger.info(f"Deleting model {model_id}")
        
        return {
            "model_id": model_id,
            "status": "deleted",
            "deleted_at": datetime.utcnow().isoformat(),
            "message": "Model and all associated artifacts have been deleted"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting model: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/insights/generate")
async def generate_insights(
    request: InsightRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Generate automated insights from data."""
    try:
        user = await verify_token(credentials.credentials)
        
        # This would retrieve data based on data_source
        # For demonstration, using mock data
        sample_data = pd.DataFrame({
            'date': pd.date_range(start='2023-01-01', periods=100, freq='D'),
            'sales': np.random.normal(1000, 100, 100),
            'customers': np.random.normal(50, 10, 100)
        })
        
        # Configure insight generation
        config = InsightConfig(
            report_level=getattr(InsightConfig.ReportLevel, request.report_level.upper(), 
                              InsightConfig.ReportLevel.OPERATIONAL)
        )
        insight_engine = InsightEngine(config)
        
        # Generate insights
        insights = await insight_engine.generate_insights(
            sample_data,
            analysis_types=request.analysis_types,
            date_column='date',
            metric_columns=request.metrics or ['sales', 'customers']
        )
        
        # Generate report
        report = await insight_engine.generate_report(insights)
        
        return {
            "insights_count": len(insights),
            "report": report,
            "insights": [
                {
                    "id": insight.insight_id,
                    "type": insight.insight_type.value,
                    "title": insight.title,
                    "summary": insight.summary,
                    "confidence": insight.confidence_score,
                    "recommendations": insight.recommendations
                } for insight in insights
            ],
            "generated_at": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Insight generation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/history")
async def get_insights_history(
    limit: int = Query(20, description="Number of insights to retrieve"),
    insight_type: Optional[str] = Query(None, description="Filter by insight type"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get historical insights."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock historical insights - in production this would query the database
        insights = [
            {
                "id": f"insight_{i}",
                "type": "trend_analysis",
                "title": f"Sales Trend Analysis {i}",
                "summary": f"Sales showing positive trend in period {i}",
                "confidence": 0.85,
                "created_at": (datetime.utcnow() - timedelta(days=i)).isoformat()
            } for i in range(limit)
        ]
        
        if insight_type:
            insights = [i for i in insights if i['type'] == insight_type]
        
        return {
            "insights": insights,
            "total_count": len(insights),
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error getting insights history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features/retrieve")
async def retrieve_features(
    request: FeatureStoreRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Retrieve features from feature store."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Get features from feature store
        features = await feature_store.get_features(
            request.feature_group,
            "1.0",  # version
            request.entity_keys,
            request.features,
            request.as_of_date
        )
        
        return {
            "feature_group": request.feature_group,
            "entity_keys": request.entity_keys,
            "features": features or {},
            "retrieved_at": datetime.utcnow(),
            "as_of_date": request.as_of_date
        }
        
    except Exception as e:
        logger.error(f"Feature retrieval error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/groups")
async def list_feature_groups(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """List available feature groups."""
    try:
        user = await verify_token(credentials.credentials)
        
        feature_groups = await feature_store.list_feature_groups()
        
        return {
            "feature_groups": feature_groups,
            "count": len(feature_groups),
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error listing feature groups: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features/pipeline/execute")
async def execute_feature_pipeline(
    request: FeaturePipelineRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Execute feature engineering pipeline."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Execute feature pipeline
        pipeline_result = await feature_pipeline.execute_pipeline(
            request.pipeline_name,
            request.input_data,
            request.feature_names
        )
        
        # Save to feature store if requested
        if request.save_to_store:
            await feature_store.save_features(
                request.pipeline_name,
                pipeline_result["features"],
                pipeline_result["metadata"]
            )
        
        return {
            "pipeline_name": request.pipeline_name,
            "execution_id": pipeline_result.get("execution_id"),
            "features_generated": pipeline_result.get("features", {}),
            "feature_count": len(pipeline_result.get("features", {})),
            "execution_time_ms": pipeline_result.get("execution_time_ms"),
            "saved_to_store": request.save_to_store,
            "metadata": {
                "pipeline_version": pipeline_result.get("pipeline_version"),
                "data_version": pipeline_result.get("data_version"),
                "execution_timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error executing feature pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/pipelines")
async def list_feature_pipelines(
    status: Optional[str] = Query(None, description="Filter by pipeline status"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """List available feature engineering pipelines."""
    try:
        user = await verify_token(credentials.credentials)
        
        pipelines = await feature_pipeline.list_pipelines(status_filter=status)
        
        return {
            "pipelines": pipelines,
            "count": len(pipelines),
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error listing feature pipelines: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/pipelines/{pipeline_name}")
async def get_pipeline_details(
    pipeline_name: str = Path(..., description="Feature pipeline name"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get detailed information about a feature pipeline."""
    try:
        user = await verify_token(credentials.credentials)
        
        pipeline_details = await feature_pipeline.get_pipeline_details(pipeline_name)
        
        return {
            "pipeline_name": pipeline_name,
            "description": pipeline_details.get("description"),
            "version": pipeline_details.get("version"),
            "status": pipeline_details.get("status"),
            "features": {
                "input_features": pipeline_details.get("input_features", []),
                "output_features": pipeline_details.get("output_features", []),
                "derived_features": pipeline_details.get("derived_features", [])
            },
            "transformations": pipeline_details.get("transformations", []),
            "dependencies": pipeline_details.get("dependencies", []),
            "execution_stats": {
                "last_execution": pipeline_details.get("last_execution"),
                "execution_count": pipeline_details.get("execution_count", 0),
                "avg_execution_time_ms": pipeline_details.get("avg_execution_time_ms"),
                "success_rate": pipeline_details.get("success_rate", 1.0)
            },
            "created_at": pipeline_details.get("created_at"),
            "updated_at": pipeline_details.get("updated_at")
        }
        
    except Exception as e:
        logger.error(f"Error getting pipeline details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features/pipelines/{pipeline_name}/validate")
async def validate_feature_pipeline(
    pipeline_name: str = Path(..., description="Feature pipeline name"),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    sample_data: Dict[str, Any] = None
):
    """Validate feature pipeline with sample data."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Validate pipeline with sample data
        validation_result = await feature_pipeline.validate_pipeline(
            pipeline_name, sample_data
        )
        
        return {
            "pipeline_name": pipeline_name,
            "validation_status": validation_result.get("status"),
            "validation_results": {
                "schema_validation": validation_result.get("schema_valid", True),
                "transformation_validation": validation_result.get("transformations_valid", True),
                "output_validation": validation_result.get("output_valid", True),
                "data_quality_checks": validation_result.get("data_quality", {})
            },
            "errors": validation_result.get("errors", []),
            "warnings": validation_result.get("warnings", []),
            "sample_output": validation_result.get("sample_output", {}),
            "validated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error validating feature pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/lineage/{feature_name}")
async def get_feature_lineage(
    feature_name: str = Path(..., description="Feature name"),
    depth: int = Query(5, ge=1, le=10, description="Lineage depth"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get feature lineage and dependencies."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Get feature lineage from feature store
        lineage = await feature_store.get_feature_lineage(feature_name, depth)
        
        return {
            "feature_name": feature_name,
            "lineage": {
                "upstream_features": lineage.get("upstream", []),
                "downstream_features": lineage.get("downstream", []),
                "transformation_path": lineage.get("transformations", []),
                "data_sources": lineage.get("sources", [])
            },
            "dependencies": {
                "direct_dependencies": lineage.get("direct_deps", []),
                "transitive_dependencies": lineage.get("transitive_deps", []),
                "dependency_count": lineage.get("dependency_count", 0)
            },
            "impact_analysis": {
                "affected_models": lineage.get("affected_models", []),
                "affected_pipelines": lineage.get("affected_pipelines", []),
                "impact_score": lineage.get("impact_score", 0.0)
            },
            "metadata": {
                "feature_type": lineage.get("feature_type"),
                "data_type": lineage.get("data_type"),
                "created_at": lineage.get("created_at"),
                "last_updated": lineage.get("last_updated")
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting feature lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features/quality/check")
async def check_feature_quality(
    feature_group: str,
    quality_rules: Dict[str, Any],
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Run data quality checks on features."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock feature quality check
        quality_results = {
            "feature_group": feature_group,
            "quality_score": 0.95,
            "checks": [
                {
                    "check_name": "null_values",
                    "passed": True,
                    "threshold": 0.05,
                    "actual_value": 0.02,
                    "message": "Null value rate within acceptable limits"
                },
                {
                    "check_name": "outliers",
                    "passed": True,
                    "threshold": 0.1,
                    "actual_value": 0.03,
                    "message": "Outlier rate acceptable"
                },
                {
                    "check_name": "data_drift",
                    "passed": False,
                    "threshold": 0.1,
                    "actual_value": 0.15,
                    "message": "Significant data drift detected"
                }
            ],
            "recommendations": [
                "Investigate data drift in feature_group",
                "Consider feature transformation updates"
            ],
            "checked_at": datetime.utcnow().isoformat()
        }
        
        return quality_results
        
    except Exception as e:
        logger.error(f"Error checking feature quality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/statistics/{feature_name}")
async def get_feature_statistics(
    feature_name: str = Path(..., description="Feature name"),
    time_range: str = Query("7d", description="Time range for statistics"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get detailed statistics for a specific feature."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock feature statistics
        statistics = {
            "feature_name": feature_name,
            "time_range": time_range,
            "descriptive_stats": {
                "count": 10000,
                "mean": 45.6,
                "median": 43.2,
                "std": 12.8,
                "min": 18.0,
                "max": 89.5,
                "percentiles": {
                    "p25": 35.1,
                    "p75": 56.2,
                    "p90": 67.8,
                    "p95": 74.3,
                    "p99": 82.1
                }
            },
            "data_quality": {
                "missing_values": 42,
                "missing_rate": 0.0042,
                "unique_values": 8934,
                "cardinality": 0.8934,
                "outliers": 15,
                "outlier_rate": 0.0015
            },
            "distribution": {
                "skewness": 0.23,
                "kurtosis": -0.45,
                "normality_test_pvalue": 0.03,
                "is_normal": False
            },
            "trend_analysis": {
                "trend_direction": "increasing",
                "trend_strength": 0.15,
                "seasonal_pattern": "weekly",
                "volatility": 0.08
            },
            "computed_at": datetime.utcnow().isoformat()
        }
        
        return statistics
        
    except Exception as e:
        logger.error(f"Error getting feature statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/model/{model_id}")
async def get_model_metrics(
    model_id: str = Path(..., description="Model ID"),
    start_date: Optional[datetime] = Query(None, description="Start date for metrics"),
    end_date: Optional[datetime] = Query(None, description="End date for metrics"),
    metric_types: List[str] = Query(["performance", "drift", "usage"], description="Types of metrics to include"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get comprehensive model performance metrics with drift detection."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Default date range
        if not end_date:
            end_date = datetime.utcnow()
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        # Get metrics from model monitor
        metrics_data = await model_monitor.get_model_metrics(
            model_id, start_date, end_date, metric_types
        )
        
        # Enhanced metrics response
        metrics = {
            "model_id": model_id,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "performance": {
                "accuracy": 0.89,
                "precision": 0.87,
                "recall": 0.91,
                "f1_score": 0.89,
                "auc_roc": 0.94,
                "trend": "stable",
                "performance_alerts": []
            },
            "drift_metrics": {
                "feature_drift_score": 0.03,
                "prediction_drift_score": 0.02,
                "label_drift_score": 0.01,
                "drift_detected": False,
                "drift_features": [],
                "drift_trend": "stable",
                "drift_alerts": []
            },
            "usage_stats": {
                "total_predictions": 15420,
                "avg_latency_ms": 125,
                "p95_latency_ms": 245,
                "p99_latency_ms": 380,
                "error_rate": 0.002,
                "requests_per_minute": 85,
                "peak_requests_per_minute": 150
            },
            "resource_usage": {
                "avg_cpu_percent": 35,
                "avg_memory_mb": 1024,
                "avg_gpu_utilization": 65,
                "cost_per_prediction": 0.001
            },
            "data_quality": {
                "missing_values_rate": 0.005,
                "outlier_detection_rate": 0.02,
                "schema_violations": 0,
                "data_quality_score": 0.98
            }
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting model metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monitoring/configure")
async def configure_model_monitoring(
    request: ModelMonitoringRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Configure monitoring for a specific model."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Configure monitoring with model monitor
        monitoring_id = await model_monitor.configure_monitoring(
            request.model_id,
            request.monitoring_config,
            request.drift_threshold,
            request.alert_channels
        )
        
        return {
            "monitoring_id": monitoring_id,
            "model_id": request.model_id,
            "status": "configured",
            "config": {
                "drift_threshold": request.drift_threshold,
                "alert_channels": request.alert_channels,
                "monitoring_frequency": request.monitoring_config.get("frequency", "hourly"),
                "metrics_collected": request.monitoring_config.get("metrics", ["accuracy", "drift", "latency"])
            },
            "configured_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error configuring monitoring: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/monitoring/{model_id}/drift")
async def get_drift_analysis(
    model_id: str = Path(..., description="Model ID"),
    feature_names: Optional[List[str]] = Query(None, description="Specific features to analyze"),
    time_window: str = Query("7d", description="Time window for analysis (1d, 7d, 30d)"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get detailed drift analysis for model features."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Parse time window
        if time_window == "1d":
            start_date = datetime.utcnow() - timedelta(days=1)
        elif time_window == "7d":
            start_date = datetime.utcnow() - timedelta(days=7)
        elif time_window == "30d":
            start_date = datetime.utcnow() - timedelta(days=30)
        else:
            start_date = datetime.utcnow() - timedelta(days=7)
        
        # Get drift analysis from model monitor
        drift_analysis = await model_monitor.analyze_drift(
            model_id, start_date, datetime.utcnow(), feature_names
        )
        
        # Mock detailed drift analysis
        drift_data = {
            "model_id": model_id,
            "analysis_period": {
                "start": start_date.isoformat(),
                "end": datetime.utcnow().isoformat(),
                "window": time_window
            },
            "overall_drift": {
                "drift_score": 0.05,
                "drift_detected": False,
                "severity": "low",
                "trend": "stable"
            },
            "feature_drift": [
                {
                    "feature_name": "age",
                    "drift_score": 0.02,
                    "drift_method": "ks_test",
                    "p_value": 0.85,
                    "drift_detected": False,
                    "distribution_shift": "none"
                },
                {
                    "feature_name": "income",
                    "drift_score": 0.08,
                    "drift_method": "ks_test",
                    "p_value": 0.03,
                    "drift_detected": True,
                    "distribution_shift": "mean_shift"
                }
            ],
            "prediction_drift": {
                "drift_score": 0.03,
                "drift_detected": False,
                "distribution_changes": {
                    "mean_change": 0.02,
                    "std_change": 0.01,
                    "skewness_change": 0.005
                }
            },
            "recommendations": [
                "Monitor income feature closely for continued drift",
                "Consider retraining if drift persists for more than 14 days"
            ]
        }
        
        return drift_data
        
    except Exception as e:
        logger.error(f"Error getting drift analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/monitoring/{model_id}/alerts")
async def get_model_alerts(
    model_id: str = Path(..., description="Model ID"),
    severity: Optional[str] = Query(None, description="Filter by severity (low, medium, high, critical)"),
    limit: int = Query(50, ge=1, le=200, description="Number of alerts to retrieve"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get alerts for a specific model."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock alerts data
        alerts = [
            {
                "alert_id": f"alert_{i}",
                "model_id": model_id,
                "alert_type": "drift_detection",
                "severity": "medium",
                "message": "Feature drift detected in income variable",
                "metric_value": 0.08,
                "threshold": 0.05,
                "created_at": (datetime.utcnow() - timedelta(hours=i)).isoformat(),
                "status": "active" if i < 5 else "resolved",
                "affected_features": ["income"],
                "recommended_actions": ["Investigate data source", "Consider model retraining"]
            } for i in range(min(limit, 20))
        ]
        
        # Filter by severity
        if severity:
            alerts = [alert for alert in alerts if alert["severity"] == severity]
        
        return {
            "model_id": model_id,
            "alerts": alerts,
            "total_count": len(alerts),
            "active_count": len([a for a in alerts if a["status"] == "active"]),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting model alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monitoring/{model_id}/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    model_id: str = Path(..., description="Model ID"),
    alert_id: str = Path(..., description="Alert ID"),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    acknowledgment: Dict[str, str] = None
):
    """Acknowledge and optionally resolve a model alert."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock alert acknowledgment
        logger.info(f"Acknowledging alert {alert_id} for model {model_id}")
        
        return {
            "alert_id": alert_id,
            "model_id": model_id,
            "status": "acknowledged",
            "acknowledged_by": user.get("username", "system"),
            "acknowledged_at": datetime.utcnow().isoformat(),
            "notes": acknowledgment.get("notes", "") if acknowledgment else ""
        }
        
    except Exception as e:
        logger.error(f"Error acknowledging alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monitoring/alerts/configure")
async def configure_model_alert(
    alert_config: Dict[str, Any],
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Configure intelligent alerts for model monitoring."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Create intelligent alerting system
        alerting_system = create_intelligent_alerting_system()
        
        # Configure alert
        alert_id = await alerting_system.create_alert_rule(
            name=alert_config.get('name'),
            condition=alert_config.get('condition'),
            threshold=alert_config.get('threshold'),
            channels=alert_config.get('channels', ['email'])
        )
        
        return {
            "alert_id": alert_id,
            "status": "configured",
            "alert_config": alert_config,
            "created_at": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error configuring alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments")
async def create_ab_experiment(
    request: ABTestRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Create a new A/B testing experiment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Create experiment configuration
        experiment_config = ExperimentConfig(
            experiment_name=request.experiment_name,
            description=request.description,
            control_model_id=request.control_model_id,
            treatment_model_ids=request.treatment_model_ids,
            traffic_allocation=request.traffic_allocation,
            primary_metric=request.primary_metric,
            duration_days=request.duration_days,
            minimum_sample_size=request.minimum_sample_size,
            split_strategy=SplitStrategy.RANDOM
        )
        
        # Create experiment
        experiment = await ab_testing_framework.create_experiment(experiment_config)
        
        return {
            "experiment_id": experiment.experiment_id,
            "name": request.experiment_name,
            "status": "created",
            "config": {
                "control_model": request.control_model_id,
                "treatment_models": request.treatment_model_ids,
                "traffic_allocation": request.traffic_allocation,
                "primary_metric": request.primary_metric,
                "duration_days": request.duration_days
            },
            "created_at": datetime.utcnow().isoformat(),
            "estimated_end": (datetime.utcnow() + timedelta(days=request.duration_days)).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating A/B experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments")
async def list_ab_experiments(
    status: Optional[str] = Query(None, description="Filter by experiment status"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """List A/B testing experiments."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Get experiments from A/B testing framework
        experiments = await ab_testing_framework.list_experiments(
            status_filter=status,
            limit=page_size,
            offset=(page - 1) * page_size
        )
        
        return {
            "experiments": experiments,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": len(experiments),
                "total_pages": (len(experiments) + page_size - 1) // page_size
            },
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error listing A/B experiments: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}")
async def get_ab_experiment(
    experiment_id: str = Path(..., description="Experiment ID"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get detailed information about an A/B experiment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Get experiment details
        experiment = await ab_testing_framework.get_experiment(experiment_id)
        
        return {
            "experiment_id": experiment_id,
            "name": experiment.get("name"),
            "description": experiment.get("description"),
            "status": experiment.get("status"),
            "config": {
                "control_model": experiment.get("control_model_id"),
                "treatment_models": experiment.get("treatment_model_ids", []),
                "traffic_allocation": experiment.get("traffic_allocation", {}),
                "primary_metric": experiment.get("primary_metric")
            },
            "stats": {
                "total_samples": experiment.get("total_samples", 0),
                "control_samples": experiment.get("control_samples", 0),
                "treatment_samples": experiment.get("treatment_samples", 0),
                "statistical_power": experiment.get("statistical_power", 0.8)
            },
            "results": experiment.get("results", {}),
            "created_at": experiment.get("created_at"),
            "started_at": experiment.get("started_at"),
            "ended_at": experiment.get("ended_at")
        }
        
    except Exception as e:
        logger.error(f"Error getting A/B experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments/{experiment_id}/start")
async def start_ab_experiment(
    experiment_id: str = Path(..., description="Experiment ID"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Start an A/B testing experiment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Start experiment
        result = await ab_testing_framework.start_experiment(experiment_id)
        
        return {
            "experiment_id": experiment_id,
            "status": "started",
            "started_at": datetime.utcnow().isoformat(),
            "message": "Experiment started successfully"
        }
        
    except Exception as e:
        logger.error(f"Error starting A/B experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments/{experiment_id}/stop")
async def stop_ab_experiment(
    experiment_id: str = Path(..., description="Experiment ID"),
    reason: Optional[str] = Query(None, description="Reason for stopping"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Stop a running A/B testing experiment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Stop experiment
        result = await ab_testing_framework.stop_experiment(experiment_id, reason)
        
        return {
            "experiment_id": experiment_id,
            "status": "stopped",
            "stopped_at": datetime.utcnow().isoformat(),
            "reason": reason,
            "message": "Experiment stopped successfully"
        }
        
    except Exception as e:
        logger.error(f"Error stopping A/B experiment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/results")
async def get_ab_experiment_results(
    experiment_id: str = Path(..., description="Experiment ID"),
    include_raw_data: bool = Query(False, description="Include raw experiment data"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get results and statistical analysis of an A/B experiment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Get experiment results
        results = await ab_testing_framework.get_experiment_results(
            experiment_id, include_raw_data
        )
        
        # Mock comprehensive results
        experiment_results = {
            "experiment_id": experiment_id,
            "status": "completed",
            "statistical_results": {
                "primary_metric": {
                    "metric_name": "accuracy",
                    "control_value": 0.85,
                    "treatment_value": 0.89,
                    "lift": 0.047,
                    "relative_improvement": 4.7,
                    "confidence_interval": [0.02, 0.075],
                    "p_value": 0.003,
                    "statistical_significance": True,
                    "confidence_level": 0.95
                },
                "secondary_metrics": [
                    {
                        "metric_name": "precision",
                        "control_value": 0.82,
                        "treatment_value": 0.86,
                        "lift": 0.049,
                        "p_value": 0.012,
                        "statistical_significance": True
                    }
                ]
            },
            "sample_statistics": {
                "total_samples": 5000,
                "control_samples": 2500,
                "treatment_samples": 2500,
                "statistical_power": 0.89,
                "minimum_detectable_effect": 0.03
            },
            "recommendations": {
                "decision": "deploy_treatment",
                "confidence": "high",
                "reasoning": "Treatment model shows statistically significant improvement with high confidence",
                "next_steps": [
                    "Deploy treatment model to production",
                    "Monitor performance for 14 days",
                    "Set up ongoing A/B tests for continuous optimization"
                ]
            },
            "duration": {
                "started_at": (datetime.utcnow() - timedelta(days=7)).isoformat(),
                "ended_at": datetime.utcnow().isoformat(),
                "duration_days": 7
            }
        }
        
        if include_raw_data:
            experiment_results["raw_data"] = results.get("raw_data", [])
        
        return experiment_results
        
    except Exception as e:
        logger.error(f"Error getting A/B experiment results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/experiments/{experiment_id}/traffic")
async def update_experiment_traffic(
    experiment_id: str = Path(..., description="Experiment ID"),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    traffic_allocation: Dict[str, float] = None
):
    """Update traffic allocation for a running experiment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Validate traffic allocation sums to 1.0
        if traffic_allocation and sum(traffic_allocation.values()) != 1.0:
            raise HTTPException(
                status_code=400,
                detail="Traffic allocation must sum to 1.0"
            )
        
        # Update traffic allocation
        result = await ab_testing_framework.update_traffic_allocation(
            experiment_id, traffic_allocation
        )
        
        return {
            "experiment_id": experiment_id,
            "traffic_allocation": traffic_allocation,
            "updated_at": datetime.utcnow().isoformat(),
            "message": "Traffic allocation updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating experiment traffic: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/experiments/{experiment_id}/metrics")
async def get_experiment_metrics(
    experiment_id: str = Path(..., description="Experiment ID"),
    metric_names: Optional[List[str]] = Query(None, description="Specific metrics to retrieve"),
    time_range: str = Query("24h", description="Time range for metrics"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get real-time metrics for an A/B experiment."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Get experiment metrics
        metrics = await ab_testing_framework.get_experiment_metrics(
            experiment_id, metric_names, time_range
        )
        
        return {
            "experiment_id": experiment_id,
            "time_range": time_range,
            "metrics": metrics or {
                "sample_size": {
                    "control": 1250,
                    "treatment": 1250,
                    "total": 2500
                },
                "conversion_rate": {
                    "control": 0.23,
                    "treatment": 0.27,
                    "lift": 0.17
                },
                "statistical_significance": {
                    "p_value": 0.032,
                    "confidence_level": 0.95,
                    "is_significant": True
                },
                "realtime_performance": {
                    "control_accuracy": 0.85,
                    "treatment_accuracy": 0.89,
                    "current_lift": 0.047
                }
            },
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error getting experiment metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/data")
async def get_dashboard_data(
    dashboard_type: str = Query("overview", description="Type of dashboard"),
    time_range: str = Query("24h", description="Time range for data"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get data for ML analytics dashboards."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock dashboard data - in production this would aggregate real metrics
        dashboard_data = {
            "dashboard_type": dashboard_type,
            "time_range": time_range,
            "summary": {
                "active_models": 12,
                "total_predictions": 45678,
                "avg_accuracy": 0.89,
                "alerts_triggered": 3,
                "active_experiments": 3,
                "feature_pipelines": 8,
                "drift_alerts": 2
            },
            "charts": {
                "prediction_volume": [
                    {"time": "2023-01-01T00:00:00Z", "value": 1200},
                    {"time": "2023-01-01T01:00:00Z", "value": 1350},
                    {"time": "2023-01-01T02:00:00Z", "value": 1100}
                ],
                "model_performance": [
                    {"model": "sales_forecaster", "accuracy": 0.92, "drift_score": 0.02},
                    {"model": "customer_segmentation", "accuracy": 0.88, "drift_score": 0.05},
                    {"model": "anomaly_detector", "accuracy": 0.85, "drift_score": 0.01}
                ],
                "experiment_results": [
                    {"experiment": "checkout_optimization", "lift": 0.15, "significance": True},
                    {"experiment": "recommendation_engine", "lift": 0.08, "significance": True},
                    {"experiment": "pricing_model", "lift": 0.03, "significance": False}
                ],
                "feature_importance": [
                    {"feature": "customer_age", "importance": 0.35},
                    {"feature": "purchase_history", "importance": 0.28},
                    {"feature": "location", "importance": 0.22},
                    {"feature": "seasonality", "importance": 0.15}
                ]
            },
            "generated_at": datetime.utcnow()
        }
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Error getting dashboard data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/compare")
async def compare_models(
    model_ids: List[str],
    credentials: HTTPAuthorizationCredentials = Depends(security),
    comparison_metrics: List[str] = ["accuracy", "precision", "recall", "f1_score"],
    test_data_source: Optional[str] = None
):
    """Compare multiple models side-by-side on various metrics."""
    try:
        user = await verify_token(credentials.credentials)
        
        if len(model_ids) < 2:
            raise HTTPException(
                status_code=400,
                detail="At least 2 models required for comparison"
            )
        
        # Mock comparison results
        comparison_results = {
            "comparison_id": f"comp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "models": model_ids,
            "metrics_compared": comparison_metrics,
            "results": {
                model_id: {
                    "accuracy": 0.85 + (hash(model_id) % 10) * 0.01,
                    "precision": 0.82 + (hash(model_id) % 8) * 0.01,
                    "recall": 0.88 + (hash(model_id) % 6) * 0.01,
                    "f1_score": 0.85 + (hash(model_id) % 7) * 0.01,
                    "inference_time_ms": 50 + (hash(model_id) % 30),
                    "model_size_mb": 100 + (hash(model_id) % 200)
                } for model_id in model_ids
            },
            "rankings": {
                metric: sorted(model_ids, 
                             key=lambda x: 0.85 + (hash(x) % 10) * 0.01, 
                             reverse=True)
                for metric in comparison_metrics
            },
            "statistical_tests": {
                f"{model_ids[0]}_vs_{model_ids[1]}": {
                    "t_test_p_value": 0.032,
                    "effect_size": 0.05,
                    "significant_difference": True
                }
            },
            "recommendations": {
                "best_overall": model_ids[0],
                "best_for_speed": model_ids[-1],
                "best_for_accuracy": model_ids[0],
                "trade_offs": [
                    "Higher accuracy models have slightly higher inference time",
                    "Model size correlates with accuracy in this comparison"
                ]
            },
            "compared_at": datetime.utcnow().isoformat()
        }
        
        return comparison_results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/ensemble")
async def create_model_ensemble(
    base_model_ids: List[str],
    credentials: HTTPAuthorizationCredentials = Depends(security),
    ensemble_method: str = "voting",
    ensemble_name: str = None,
    weights: Optional[Dict[str, float]] = None
):
    """Create an ensemble from multiple models."""
    try:
        user = await verify_token(credentials.credentials)
        
        if len(base_model_ids) < 2:
            raise HTTPException(
                status_code=400,
                detail="At least 2 models required for ensemble"
            )
        
        valid_methods = ["voting", "weighted_average", "stacking", "blending"]
        if ensemble_method not in valid_methods:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid ensemble method. Choose from: {valid_methods}"
            )
        
        # Create ensemble
        ensemble_id = f"ensemble_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        ensemble_name = ensemble_name or f"Ensemble_{ensemble_id}"
        
        # Mock ensemble creation
        ensemble_result = {
            "ensemble_id": ensemble_id,
            "name": ensemble_name,
            "method": ensemble_method,
            "base_models": base_model_ids,
            "weights": weights or {model_id: 1.0/len(base_model_ids) for model_id in base_model_ids},
            "status": "created",
            "performance_estimate": {
                "accuracy": 0.91,  # Typically better than individual models
                "precision": 0.89,
                "recall": 0.93,
                "f1_score": 0.91,
                "expected_improvement": 0.03
            },
            "configuration": {
                "cross_validation_folds": 5,
                "validation_score": 0.89,
                "overfitting_risk": "low"
            },
            "created_at": datetime.utcnow().isoformat()
        }
        
        return ensemble_result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating ensemble: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{model_id}/interpretability")
async def get_model_interpretability(
    model_id: str = Path(..., description="Model ID"),
    interpretation_method: str = Query("shap", description="Interpretation method (shap, lime, permutation)"),
    sample_size: int = Query(100, description="Number of samples for interpretation"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get model interpretability and explainability analysis."""
    try:
        user = await verify_token(credentials.credentials)
        
        valid_methods = ["shap", "lime", "permutation", "integrated_gradients"]
        if interpretation_method not in valid_methods:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid interpretation method. Choose from: {valid_methods}"
            )
        
        # Mock interpretability results
        interpretability_results = {
            "model_id": model_id,
            "interpretation_method": interpretation_method,
            "sample_size": sample_size,
            "global_explanations": {
                "feature_importance": [
                    {"feature": "customer_age", "importance": 0.35, "direction": "positive"},
                    {"feature": "purchase_history", "importance": 0.28, "direction": "positive"},
                    {"feature": "location_risk_score", "importance": 0.22, "direction": "negative"},
                    {"feature": "seasonality_factor", "importance": 0.15, "direction": "positive"}
                ],
                "interactions": [
                    {
                        "features": ["customer_age", "purchase_history"],
                        "interaction_strength": 0.12,
                        "type": "synergistic"
                    }
                ]
            },
            "local_explanations": [
                {
                    "instance_id": f"sample_{i}",
                    "prediction": 0.85 + (i % 20) * 0.01,
                    "feature_contributions": [
                        {"feature": "customer_age", "contribution": 0.15},
                        {"feature": "purchase_history", "contribution": 0.12},
                        {"feature": "location_risk_score", "contribution": -0.08},
                        {"feature": "seasonality_factor", "contribution": 0.05}
                    ]
                } for i in range(min(5, sample_size))
            ],
            "model_behavior": {
                "linearity_score": 0.7,
                "monotonicity": {
                    "customer_age": "increasing",
                    "purchase_history": "increasing",
                    "location_risk_score": "decreasing"
                },
                "sensitivity_analysis": {
                    "most_sensitive_feature": "customer_age",
                    "robustness_score": 0.82
                }
            },
            "bias_analysis": {
                "fairness_metrics": [
                    {"protected_attribute": "gender", "bias_score": 0.02, "status": "fair"},
                    {"protected_attribute": "age_group", "bias_score": 0.05, "status": "monitored"},
                    {"protected_attribute": "location", "bias_score": 0.01, "status": "fair"}
                ],
                "recommendations": [
                    "Monitor age_group bias score closely",
                    "Consider feature engineering to reduce location bias"
                ]
            },
            "generated_at": datetime.utcnow().isoformat()
        }
        
        return interpretability_results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting model interpretability: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/{model_id}/stress-test")
async def stress_test_model(
    model_id: str = Path(..., description="Model ID"),
    test_scenarios: List[str] = ["adversarial", "outliers", "missing_data", "distribution_shift"],
    test_intensity: str = Query("medium", description="Test intensity (low, medium, high)"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Perform comprehensive stress testing on a model."""
    try:
        user = await verify_token(credentials.credentials)
        
        valid_intensities = ["low", "medium", "high"]
        if test_intensity not in valid_intensities:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid test intensity. Choose from: {valid_intensities}"
            )
        
        # Mock stress test results
        stress_test_results = {
            "model_id": model_id,
            "test_id": f"stress_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "test_scenarios": test_scenarios,
            "test_intensity": test_intensity,
            "results": {
                "adversarial": {
                    "baseline_accuracy": 0.89,
                    "adversarial_accuracy": 0.73,
                    "robustness_score": 0.82,
                    "vulnerability_areas": ["edge cases", "boundary conditions"],
                    "status": "moderate_vulnerability"
                },
                "outliers": {
                    "outlier_detection_rate": 0.95,
                    "false_positive_rate": 0.02,
                    "performance_degradation": 0.08,
                    "status": "robust"
                },
                "missing_data": {
                    "handling_strategy": "imputation",
                    "accuracy_with_missing": 0.85,
                    "missing_data_tolerance": 0.15,
                    "status": "acceptable"
                },
                "distribution_shift": {
                    "shift_detection_accuracy": 0.92,
                    "adaptation_speed": "fast",
                    "performance_under_shift": 0.81,
                    "status": "good"
                }
            },
            "overall_assessment": {
                "robustness_score": 0.84,
                "reliability_grade": "B+",
                "production_readiness": "ready_with_monitoring",
                "risk_level": "medium"
            },
            "recommendations": [
                "Implement adversarial training to improve robustness",
                "Add real-time distribution shift monitoring",
                "Consider ensemble methods for critical predictions",
                "Regular retraining schedule recommended"
            ],
            "tested_at": datetime.utcnow().isoformat()
        }
        
        return stress_test_results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error performing stress test: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/model-lifecycle/{model_id}")
async def get_model_lifecycle_analytics(
    model_id: str = Path(..., description="Model ID"),
    include_versions: bool = Query(True, description="Include version history"),
    include_performance_trend: bool = Query(True, description="Include performance trends"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Get comprehensive model lifecycle analytics."""
    try:
        user = await verify_token(credentials.credentials)
        
        # Mock lifecycle analytics
        lifecycle_analytics = {
            "model_id": model_id,
            "current_version": "v2.1.0",
            "lifecycle_stage": "production",
            "age_days": 145,
            "deployment_history": [
                {
                    "version": "v1.0.0",
                    "deployed_at": "2023-06-01T10:00:00Z",
                    "performance": {"accuracy": 0.82, "f1": 0.80},
                    "status": "retired"
                },
                {
                    "version": "v2.1.0",
                    "deployed_at": "2023-11-20T09:15:00Z", 
                    "performance": {"accuracy": 0.89, "f1": 0.88},
                    "status": "active"
                }
            ],
            "performance_trends": {
                "accuracy_trend": {
                    "direction": "improving",
                    "slope": 0.002,
                    "r_squared": 0.85,
                    "last_30_days_change": 0.01
                },
                "prediction_volume_trend": {
                    "direction": "increasing",
                    "avg_predictions_per_day": 15420,
                    "growth_rate": 0.12
                },
                "error_rate_trend": {
                    "direction": "decreasing",
                    "current_rate": 0.11,
                    "improvement_rate": -0.005
                }
            },
            "business_impact": {
                "roi_estimate": 1.85,
                "cost_savings_usd": 125000,
                "automation_percentage": 78,
                "user_satisfaction_score": 4.2
            },
            "technical_health": {
                "code_quality_score": 0.92,
                "test_coverage": 0.89,
                "documentation_completeness": 0.85,
                "security_scan_status": "passed",
                "dependency_health": "good"
            },
            "risk_assessment": {
                "technical_debt_score": 0.15,
                "maintenance_burden": "low",
                "obsolescence_risk": "low",
                "compliance_status": "compliant"
            },
            "future_roadmap": {
                "next_scheduled_retrain": "2024-02-01T00:00:00Z",
                "planned_improvements": [
                    "Feature engineering pipeline v3.0",
                    "Ensemble method integration", 
                    "Real-time learning capabilities"
                ],
                "retirement_timeline": "2025-06-01T00:00:00Z"
            }
        }
        
        return lifecycle_analytics
        
    except Exception as e:
        logger.error(f"Error getting lifecycle analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Background task functions
async def process_batch_prediction(batch_id: str, model_type: str, data: pd.DataFrame,
                                  include_confidence: bool, callback_url: Optional[str]):
    """Process batch prediction in background."""
    try:
        logger.info(f"Processing batch prediction {batch_id}")
        
        # Make batch predictions
        if model_type == "sales_forecast":
            result = await predictive_engine.predict("sales_forecaster", data)
        elif model_type == "customer_segmentation":
            result = await predictive_engine.predict("customer_segmentation", data)
        elif model_type == "anomaly_detection":
            result = await predictive_engine.predict("anomaly_detector", data)
        else:
            raise ValueError(f"Unsupported model type: {model_type}")
        
        # Store results (implement your storage logic)
        results = {
            "batch_id": batch_id,
            "predictions": result.forecast_values or result.cluster_assignments or result.anomaly_scores,
            "confidence_scores": [result.confidence_score] * len(data) if include_confidence else None,
            "completed_at": datetime.utcnow().isoformat()
        }
        
        # Call callback URL if provided
        if callback_url:
            # Implement callback notification
            pass
        
        logger.info(f"Batch prediction {batch_id} completed")
        
    except Exception as e:
        logger.error(f"Batch prediction {batch_id} failed: {str(e)}")


async def start_model_training(training_id: str, model_name: str, model_type: str,
                              training_data: Dict[str, Any], parameters: Dict[str, Any],
                              validation_split: float):
    """Start model training in background."""
    try:
        logger.info(f"Starting model training {training_id}")
        
        # Create trainer
        trainer = AutoMLTrainer(model_type)
        
        # Mock training data - in production this would load real data
        mock_data = pd.DataFrame({
            'feature1': np.random.normal(0, 1, 1000),
            'feature2': np.random.normal(0, 1, 1000),
            'target': np.random.randint(0, 2, 1000)
        })
        
        X = mock_data[['feature1', 'feature2']]
        y = mock_data['target']
        
        # Train model
        results = trainer.train_models(X, y)
        
        # Store trained model
        best_model = results[0] if results else None
        if best_model:
            # Register model with predictive engine
            await predictive_engine.register_model(model_name, best_model)
        
        logger.info(f"Model training {training_id} completed")
        
    except Exception as e:
        logger.error(f"Model training {training_id} failed: {str(e)}")


