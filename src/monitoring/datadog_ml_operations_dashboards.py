"""
DataDog ML Operations Dashboards
Provides comprehensive ML operations dashboards for model performance monitoring,
drift detection, A/B testing, feature engineering, and ML pipeline optimization
"""

import asyncio
import json
import time
import numpy as np
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_dashboard_integration import DataDogDashboardManager
from monitoring.datadog_business_metrics import DataDogBusinessMetricsTracker

logger = get_logger(__name__)


class MLDashboardType(Enum):
    """Types of ML operations dashboards"""
    MODEL_PERFORMANCE = "model_performance"
    MODEL_TRAINING = "model_training"
    FEATURE_ENGINEERING = "feature_engineering"
    AB_TESTING = "ab_testing"
    MODEL_DRIFT = "model_drift"
    ML_PIPELINE_MONITORING = "ml_pipeline_monitoring"
    DATA_QUALITY = "data_quality"
    MODEL_SERVING = "model_serving"
    ML_EXPERIMENTATION = "ml_experimentation"


class ModelStage(Enum):
    """ML model lifecycle stages"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class DriftType(Enum):
    """Types of model drift"""
    DATA_DRIFT = "data_drift"
    CONCEPT_DRIFT = "concept_drift"
    PREDICTION_DRIFT = "prediction_drift"
    PERFORMANCE_DRIFT = "performance_drift"


@dataclass
class MLMetric:
    """ML-specific metric definition"""
    name: str
    description: str
    metric_type: str  # accuracy, precision, recall, f1, auc_roc, etc.
    unit: str
    higher_is_better: bool = True
    critical_threshold: Optional[float] = None
    warning_threshold: Optional[float] = None
    baseline_value: Optional[float] = None
    model_types: Optional[List[str]] = None  # classification, regression, clustering, etc.


@dataclass
class ModelDefinition:
    """ML model definition"""
    model_id: str
    model_name: str
    model_type: str  # classification, regression, etc.
    model_version: str
    stage: ModelStage
    training_date: datetime
    deployment_date: Optional[datetime] = None
    expected_performance: Optional[Dict[str, float]] = None
    drift_thresholds: Optional[Dict[str, float]] = None
    business_impact: str = "medium"  # low, medium, high, critical


@dataclass
class ExperimentDefinition:
    """ML experiment definition"""
    experiment_id: str
    experiment_name: str
    experiment_type: str  # ab_test, multivariate, champion_challenger
    models: List[str]  # model IDs
    traffic_allocation: Dict[str, float]
    start_date: datetime
    planned_end_date: datetime
    success_criteria: Dict[str, float]
    status: str = "running"  # planned, running, paused, completed, failed


class DataDogMLOperationsDashboards:
    """
    ML Operations Dashboards for comprehensive ML monitoring
    
    Features:
    - Model training and deployment metrics
    - Feature pipeline performance tracking
    - Model drift and accuracy monitoring
    - A/B testing results and experiment tracking
    - Data quality and governance metrics
    - ML pipeline performance optimization
    - Model serving latency and throughput
    - Feature store monitoring
    - ML experiment lifecycle management
    """
    
    def __init__(self, service_name: str = "ml-operations",
                 datadog_monitoring: Optional[DatadogMonitoring] = None,
                 dashboard_manager: Optional[DataDogDashboardManager] = None,
                 business_metrics: Optional[DataDogBusinessMetricsTracker] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.dashboard_manager = dashboard_manager
        self.business_metrics = business_metrics
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # ML operations registry
        self.ml_metrics: Dict[str, MLMetric] = {}
        self.model_registry: Dict[str, ModelDefinition] = {}
        self.experiment_registry: Dict[str, ExperimentDefinition] = {}
        self.dashboard_definitions: Dict[MLDashboardType, Dict[str, Any]] = {}
        
        # Performance tracking
        self.model_predictions = {}
        self.drift_detections = []
        self.experiment_results = {}
        self.feature_quality_scores = {}
        
        # Initialize components
        self._initialize_ml_metrics()
        self._initialize_sample_models()
        self._initialize_dashboard_definitions()
        
        self.logger.info(f"ML operations dashboards initialized for {service_name}")
    
    def _initialize_ml_metrics(self):
        """Initialize ML-specific metrics"""
        
        ml_metrics = [
            # Classification Metrics
            MLMetric(
                name="accuracy",
                description="Model accuracy score",
                metric_type="classification",
                unit="percentage",
                higher_is_better=True,
                critical_threshold=0.85,
                warning_threshold=0.90,
                model_types=["classification"]
            ),
            MLMetric(
                name="precision",
                description="Model precision score",
                metric_type="classification",
                unit="percentage",
                higher_is_better=True,
                critical_threshold=0.80,
                warning_threshold=0.85,
                model_types=["classification"]
            ),
            MLMetric(
                name="recall",
                description="Model recall score",
                metric_type="classification",
                unit="percentage",
                higher_is_better=True,
                critical_threshold=0.75,
                warning_threshold=0.85,
                model_types=["classification"]
            ),
            MLMetric(
                name="f1_score",
                description="Model F1 score",
                metric_type="classification",
                unit="percentage",
                higher_is_better=True,
                critical_threshold=0.80,
                warning_threshold=0.85,
                model_types=["classification"]
            ),
            MLMetric(
                name="auc_roc",
                description="Area Under the ROC Curve",
                metric_type="classification",
                unit="percentage",
                higher_is_better=True,
                critical_threshold=0.80,
                warning_threshold=0.85,
                model_types=["classification"]
            ),
            
            # Regression Metrics
            MLMetric(
                name="mse",
                description="Mean Squared Error",
                metric_type="regression",
                unit="squared_error",
                higher_is_better=False,
                critical_threshold=1000.0,
                warning_threshold=500.0,
                model_types=["regression"]
            ),
            MLMetric(
                name="mae",
                description="Mean Absolute Error",
                metric_type="regression",
                unit="absolute_error",
                higher_is_better=False,
                critical_threshold=50.0,
                warning_threshold=25.0,
                model_types=["regression"]
            ),
            MLMetric(
                name="r2_score",
                description="R-squared score",
                metric_type="regression",
                unit="percentage",
                higher_is_better=True,
                critical_threshold=0.80,
                warning_threshold=0.90,
                model_types=["regression"]
            ),
            
            # Performance Metrics
            MLMetric(
                name="inference_latency",
                description="Model inference latency",
                metric_type="performance",
                unit="milliseconds",
                higher_is_better=False,
                critical_threshold=1000.0,
                warning_threshold=500.0
            ),
            MLMetric(
                name="throughput",
                description="Model throughput (predictions per second)",
                metric_type="performance",
                unit="predictions_per_second",
                higher_is_better=True,
                critical_threshold=100.0,
                warning_threshold=500.0
            ),
            
            # Data Quality Metrics
            MLMetric(
                name="feature_completeness",
                description="Percentage of complete features",
                metric_type="data_quality",
                unit="percentage",
                higher_is_better=True,
                critical_threshold=0.95,
                warning_threshold=0.98
            ),
            MLMetric(
                name="data_freshness",
                description="Data freshness in hours",
                metric_type="data_quality",
                unit="hours",
                higher_is_better=False,
                critical_threshold=24.0,
                warning_threshold=12.0
            )
        ]
        
        for metric in ml_metrics:
            self.ml_metrics[metric.name] = metric
        
        self.logger.info(f"Initialized {len(ml_metrics)} ML metrics")
    
    def _initialize_sample_models(self):
        """Initialize sample models for demonstration"""
        
        sample_models = [
            ModelDefinition(
                model_id="customer_churn_v1",
                model_name="Customer Churn Prediction",
                model_type="classification",
                model_version="1.0.0",
                stage=ModelStage.PRODUCTION,
                training_date=datetime.utcnow() - timedelta(days=30),
                deployment_date=datetime.utcnow() - timedelta(days=15),
                expected_performance={"accuracy": 0.92, "precision": 0.89, "recall": 0.87},
                business_impact="high"
            ),
            ModelDefinition(
                model_id="revenue_forecast_v2",
                model_name="Revenue Forecasting",
                model_type="regression",
                model_version="2.1.0",
                stage=ModelStage.PRODUCTION,
                training_date=datetime.utcnow() - timedelta(days=14),
                deployment_date=datetime.utcnow() - timedelta(days=7),
                expected_performance={"mse": 150.0, "mae": 8.5, "r2_score": 0.94},
                business_impact="critical"
            ),
            ModelDefinition(
                model_id="recommendation_engine_v1",
                model_name="Product Recommendation Engine",
                model_type="recommendation",
                model_version="1.2.0",
                stage=ModelStage.STAGING,
                training_date=datetime.utcnow() - timedelta(days=7),
                expected_performance={"precision_at_k": 0.75, "recall_at_k": 0.68},
                business_impact="medium"
            )
        ]
        
        for model in sample_models:
            self.model_registry[model.model_id] = model
        
        self.logger.info(f"Initialized {len(sample_models)} sample models")
    
    def _initialize_dashboard_definitions(self):
        """Initialize ML dashboard definitions"""
        
        # Model Performance Dashboard
        self.dashboard_definitions[MLDashboardType.MODEL_PERFORMANCE] = {
            "title": "ML Model Performance - Production Monitoring",
            "description": "Real-time model performance metrics and accuracy tracking",
            "refresh_interval": "1m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Models in Production",
                    "query": "count:ml.models.production{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Average Model Accuracy",
                    "query": "avg:ml.model.accuracy{stage:production}",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Predictions per Minute",
                    "query": "sum:ml.predictions.total{*}.as_rate()",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Average Inference Latency",
                    "query": "avg:ml.inference.latency{*}",
                    "precision": 1,
                    "format": "duration"
                },
                {
                    "type": "timeseries",
                    "title": "Model Accuracy Trends",
                    "query": "avg:ml.model.accuracy{*} by {model_id}",
                    "timeframe": "7d"
                },
                {
                    "type": "timeseries",
                    "title": "Inference Latency Distribution",
                    "query": "percentile:ml.inference.latency{*}:50, percentile:ml.inference.latency{*}:95, percentile:ml.inference.latency{*}:99",
                    "timeframe": "24h"
                },
                {
                    "type": "heatmap",
                    "title": "Model Performance Heatmap",
                    "query": "avg:ml.model.performance_score{*} by {model_id,metric}"
                },
                {
                    "type": "table",
                    "title": "Model Performance Summary",
                    "query": "avg:ml.model.accuracy{*} by {model_id}",
                    "columns": ["Model", "Accuracy", "Latency", "Throughput", "Status"]
                }
            ]
        }
        
        # Model Training Dashboard
        self.dashboard_definitions[MLDashboardType.MODEL_TRAINING] = {
            "title": "ML Model Training - Pipeline Performance",
            "description": "Model training metrics, pipeline performance, and resource utilization",
            "refresh_interval": "5m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Active Training Jobs",
                    "query": "count:ml.training.active{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Training Success Rate",
                    "query": "sum:ml.training.success{*} / sum:ml.training.total{*} * 100",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Average Training Time",
                    "query": "avg:ml.training.duration{*}",
                    "precision": 1,
                    "format": "duration"
                },
                {
                    "type": "query_value",
                    "title": "GPU Utilization",
                    "query": "avg:ml.gpu.utilization{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "timeseries",
                    "title": "Training Job Queue Length",
                    "query": "avg:ml.training.queue_length{*}",
                    "timeframe": "48h"
                },
                {
                    "type": "timeseries",
                    "title": "Resource Utilization During Training",
                    "query": "avg:ml.gpu.utilization{*}, avg:ml.cpu.utilization{*}, avg:ml.memory.utilization{*}",
                    "timeframe": "24h"
                },
                {
                    "type": "distribution",
                    "title": "Training Duration Distribution",
                    "query": "avg:ml.training.duration{*}"
                },
                {
                    "type": "toplist",
                    "title": "Longest Running Training Jobs",
                    "query": "max:ml.training.duration{*} by {job_id}",
                    "limit": 10
                }
            ]
        }
        
        # Feature Engineering Dashboard
        self.dashboard_definitions[MLDashboardType.FEATURE_ENGINEERING] = {
            "title": "Feature Engineering - Pipeline Performance",
            "description": "Feature pipeline performance, data quality, and feature store metrics",
            "refresh_interval": "3m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Feature Pipelines Active",
                    "query": "count:ml.features.pipelines.active{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Features in Store",
                    "query": "count:ml.features.store.total{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Data Quality Score",
                    "query": "avg:ml.data_quality.overall_score{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Feature Freshness",
                    "query": "avg:ml.features.freshness_hours{*}",
                    "precision": 1,
                    "format": "duration"
                },
                {
                    "type": "timeseries",
                    "title": "Feature Pipeline Processing Time",
                    "query": "avg:ml.features.processing_time{*} by {pipeline}",
                    "timeframe": "24h"
                },
                {
                    "type": "timeseries",
                    "title": "Feature Quality Trends",
                    "query": "avg:ml.features.quality_score{*} by {feature_group}",
                    "timeframe": "7d"
                },
                {
                    "type": "scatterplot",
                    "title": "Feature Importance vs Quality",
                    "x_axis": "avg:ml.features.importance_score{*}",
                    "y_axis": "avg:ml.features.quality_score{*}"
                },
                {
                    "type": "treemap",
                    "title": "Feature Store Usage by Model",
                    "query": "sum:ml.features.usage{*} by {model_id}"
                }
            ]
        }
        
        # A/B Testing Dashboard
        self.dashboard_definitions[MLDashboardType.AB_TESTING] = {
            "title": "A/B Testing - Experiment Results",
            "description": "A/B testing results, statistical significance, and experiment performance",
            "refresh_interval": "5m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Active Experiments",
                    "query": "count:ml.experiments.active{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Statistical Significance Rate",
                    "query": "sum:ml.experiments.significant{*} / sum:ml.experiments.total{*} * 100",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Winner Detection Rate",
                    "query": "avg:ml.experiments.winner_confidence{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Average Experiment Duration",
                    "query": "avg:ml.experiments.duration_days{*}",
                    "precision": 1,
                    "format": "number"
                },
                {
                    "type": "timeseries",
                    "title": "Experiment Conversion Rates",
                    "query": "avg:ml.experiments.conversion_rate{*} by {experiment_id,variant}",
                    "timeframe": "30d"
                },
                {
                    "type": "timeseries",
                    "title": "Statistical Power Over Time",
                    "query": "avg:ml.experiments.statistical_power{*} by {experiment_id}",
                    "timeframe": "30d"
                },
                {
                    "type": "sunburst",
                    "title": "Experiment Results Breakdown",
                    "query": "count:ml.experiments.results{*} by {result_type,significance}"
                },
                {
                    "type": "table",
                    "title": "Experiment Performance Summary",
                    "query": "avg:ml.experiments.lift{*} by {experiment_id}",
                    "columns": ["Experiment", "Lift %", "P-Value", "Confidence", "Status"]
                }
            ]
        }
        
        # Model Drift Dashboard
        self.dashboard_definitions[MLDashboardType.MODEL_DRIFT] = {
            "title": "Model Drift Detection - Data & Concept Drift",
            "description": "Model drift monitoring, data distribution changes, and performance degradation",
            "refresh_interval": "10m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Models with Drift Detected",
                    "query": "count:ml.drift.detected{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Average Drift Score",
                    "query": "avg:ml.drift.score{*}",
                    "precision": 2,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Critical Drift Alerts",
                    "query": "sum:ml.drift.critical{*}",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "Performance Degradation",
                    "query": "avg:ml.performance.degradation_percent{*}",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "timeseries",
                    "title": "Drift Score Trends by Model",
                    "query": "avg:ml.drift.score{*} by {model_id}",
                    "timeframe": "30d"
                },
                {
                    "type": "timeseries",
                    "title": "Feature Drift Over Time",
                    "query": "avg:ml.drift.feature_drift{*} by {feature_name}",
                    "timeframe": "30d"
                },
                {
                    "type": "heatmap",
                    "title": "Feature Drift Correlation Matrix",
                    "query": "avg:ml.drift.correlation{*} by {feature_x,feature_y}"
                },
                {
                    "type": "alert_list",
                    "title": "Drift Alerts",
                    "query": "tag:drift_detected"
                }
            ]
        }
        
        # ML Pipeline Monitoring Dashboard
        self.dashboard_definitions[MLDashboardType.ML_PIPELINE_MONITORING] = {
            "title": "ML Pipeline Monitoring - End-to-End Performance",
            "description": "Complete ML pipeline monitoring from data ingestion to model serving",
            "refresh_interval": "2m",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Pipeline Success Rate",
                    "query": "sum:ml.pipeline.success{*} / sum:ml.pipeline.total{*} * 100",
                    "precision": 1,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "End-to-End Latency",
                    "query": "avg:ml.pipeline.e2e_latency{*}",
                    "precision": 0,
                    "format": "duration"
                },
                {
                    "type": "query_value",
                    "title": "Data Processing Volume",
                    "query": "sum:ml.pipeline.data_volume{*}",
                    "precision": 0,
                    "format": "bytes"
                },
                {
                    "type": "query_value",
                    "title": "Model Deployment Rate",
                    "query": "sum:ml.deployments.total{*}.as_rate()",
                    "precision": 2,
                    "format": "number"
                },
                {
                    "type": "timeseries",
                    "title": "Pipeline Stage Performance",
                    "query": "avg:ml.pipeline.stage_duration{*} by {stage}",
                    "timeframe": "12h"
                },
                {
                    "type": "timeseries",
                    "title": "Resource Utilization by Pipeline",
                    "query": "avg:ml.pipeline.cpu_usage{*}, avg:ml.pipeline.memory_usage{*}",
                    "timeframe": "12h"
                },
                {
                    "type": "sankey",
                    "title": "Pipeline Data Flow",
                    "query": "sum:ml.pipeline.data_flow{*} by {source,destination}"
                },
                {
                    "type": "service_map",
                    "title": "ML Pipeline Dependencies",
                    "services": ["data-ingestion", "feature-store", "training", "serving"]
                }
            ]
        }
        
        # Model Serving Dashboard
        self.dashboard_definitions[MLDashboardType.MODEL_SERVING] = {
            "title": "Model Serving - Real-time Inference",
            "description": "Model serving performance, inference latency, and serving infrastructure",
            "refresh_interval": "30s",
            "widgets": [
                {
                    "type": "query_value",
                    "title": "Inference Requests/sec",
                    "query": "sum:ml.serving.requests{*}.as_rate()",
                    "precision": 0,
                    "format": "number"
                },
                {
                    "type": "query_value",
                    "title": "P95 Inference Latency",
                    "query": "percentile:ml.serving.latency{*}:95",
                    "precision": 0,
                    "format": "duration"
                },
                {
                    "type": "query_value",
                    "title": "Serving Error Rate",
                    "query": "sum:ml.serving.errors{*} / sum:ml.serving.requests{*} * 100",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "query_value",
                    "title": "Model Server Uptime",
                    "query": "avg:ml.serving.uptime{*}",
                    "precision": 2,
                    "format": "percentage"
                },
                {
                    "type": "timeseries",
                    "title": "Inference Throughput",
                    "query": "sum:ml.serving.requests{*}.as_rate() by {model_id}",
                    "timeframe": "6h"
                },
                {
                    "type": "timeseries",
                    "title": "Latency Percentiles",
                    "query": "percentile:ml.serving.latency{*}:50, percentile:ml.serving.latency{*}:95, percentile:ml.serving.latency{*}:99",
                    "timeframe": "6h"
                },
                {
                    "type": "distribution",
                    "title": "Batch Size Distribution",
                    "query": "avg:ml.serving.batch_size{*}"
                },
                {
                    "type": "hostmap",
                    "title": "Model Server Resource Usage",
                    "query": "avg:ml.serving.cpu_usage{*} by {host}"
                }
            ]
        }
        
        self.logger.info(f"Initialized {len(self.dashboard_definitions)} ML dashboard definitions")
    
    async def create_ml_dashboard(self, dashboard_type: MLDashboardType,
                                custom_title: Optional[str] = None,
                                custom_widgets: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
        """Create an ML operations dashboard"""
        
        try:
            if dashboard_type not in self.dashboard_definitions:
                self.logger.error(f"Unknown ML dashboard type: {dashboard_type}")
                return None
            
            definition = self.dashboard_definitions[dashboard_type]
            
            # Use dashboard manager to create the dashboard
            if self.dashboard_manager:
                dashboard_id = await self.dashboard_manager.create_dashboard(
                    dashboard_type.value,
                    custom_title or definition["title"],
                    definition.get("description", ""),
                    custom_widgets or definition["widgets"]
                )
                
                if dashboard_id and self.datadog_monitoring:
                    # Track dashboard creation
                    self.datadog_monitoring.counter(
                        "ml.dashboard.created",
                        tags=[
                            f"dashboard_type:{dashboard_type.value}",
                            f"service:{self.service_name}"
                        ]
                    )
                
                return dashboard_id
            else:
                self.logger.error("Dashboard manager not available")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to create ML dashboard {dashboard_type}: {str(e)}")
            return None
    
    async def create_all_ml_dashboards(self) -> Dict[MLDashboardType, Optional[str]]:
        """Create all ML operations dashboards"""
        
        results = {}
        
        for dashboard_type in MLDashboardType:
            try:
                dashboard_id = await self.create_ml_dashboard(dashboard_type)
                results[dashboard_type] = dashboard_id
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(1.5)
                
            except Exception as e:
                self.logger.error(f"Failed to create ML dashboard {dashboard_type}: {str(e)}")
                results[dashboard_type] = None
        
        success_count = sum(1 for result in results.values() if result is not None)
        total_count = len(results)
        
        self.logger.info(f"ML dashboard creation completed: {success_count}/{total_count} successful")
        
        # Track overall success
        if self.datadog_monitoring:
            self.datadog_monitoring.gauge(
                "ml.dashboards.created_total",
                success_count,
                tags=[f"service:{self.service_name}"]
            )
        
        return results
    
    async def track_model_performance(self, model_id: str, metrics: Dict[str, float],
                                    model_version: Optional[str] = None,
                                    data_source: str = "production") -> bool:
        """Track model performance metrics"""
        
        try:
            if model_id not in self.model_registry:
                self.logger.warning(f"Unknown model: {model_id}")
                return False
            
            model = self.model_registry[model_id]
            timestamp = datetime.utcnow()
            
            # Store prediction record
            if model_id not in self.model_predictions:
                self.model_predictions[model_id] = []
            
            prediction_record = {
                "timestamp": timestamp.isoformat(),
                "model_version": model_version or model.model_version,
                "metrics": metrics,
                "data_source": data_source
            }
            
            self.model_predictions[model_id].append(prediction_record)
            
            # Keep only last 1000 records per model
            if len(self.model_predictions[model_id]) > 1000:
                self.model_predictions[model_id] = self.model_predictions[model_id][-1000:]
            
            # Send metrics to DataDog
            if self.datadog_monitoring:
                tags = [
                    f"model_id:{model_id}",
                    f"model_type:{model.model_type}",
                    f"stage:{model.stage.value}",
                    f"version:{model_version or model.model_version}",
                    f"data_source:{data_source}",
                    f"service:{self.service_name}"
                ]
                
                for metric_name, value in metrics.items():
                    self.datadog_monitoring.gauge(
                        f"ml.model.{metric_name}",
                        value,
                        tags=tags
                    )
                
                # Check performance against expectations
                await self._check_model_performance_thresholds(model, metrics)
            
            # Track with business metrics
            if self.business_metrics:
                for metric_name, value in metrics.items():
                    await self.business_metrics.track_kpi(
                        f"ml_model_{metric_name}",
                        value,
                        tags={"model_id": model_id, "model_type": model.model_type}
                    )
            
            self.logger.debug(f"Tracked model performance for {model_id}: {metrics}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track model performance: {str(e)}")
            return False
    
    async def _check_model_performance_thresholds(self, model: ModelDefinition, metrics: Dict[str, float]):
        """Check model performance against thresholds"""
        
        try:
            for metric_name, value in metrics.items():
                if metric_name in self.ml_metrics:
                    ml_metric = self.ml_metrics[metric_name]
                    
                    # Check against baseline if available
                    if (model.expected_performance and 
                        metric_name in model.expected_performance):
                        expected = model.expected_performance[metric_name]
                        
                        # Calculate performance degradation
                        if ml_metric.higher_is_better:
                            degradation = (expected - value) / expected * 100
                        else:
                            degradation = (value - expected) / expected * 100
                        
                        # Track degradation
                        if self.datadog_monitoring:
                            self.datadog_monitoring.gauge(
                                "ml.performance.degradation_percent",
                                degradation,
                                tags=[
                                    f"model_id:{model.model_id}",
                                    f"metric:{metric_name}",
                                    f"service:{self.service_name}"
                                ]
                            )
                        
                        # Alert on significant degradation
                        if degradation > 10.0:  # 10% degradation threshold
                            self.datadog_monitoring.counter(
                                "ml.performance.degradation_alert",
                                tags=[
                                    f"model_id:{model.model_id}",
                                    f"metric:{metric_name}",
                                    f"degradation_percent:{degradation:.1f}",
                                    f"service:{self.service_name}"
                                ]
                            )
                    
                    # Check against critical/warning thresholds
                    alert_level = None
                    if ml_metric.critical_threshold is not None:
                        if ml_metric.higher_is_better:
                            if value <= ml_metric.critical_threshold:
                                alert_level = "critical"
                        else:
                            if value >= ml_metric.critical_threshold:
                                alert_level = "critical"
                    
                    if alert_level is None and ml_metric.warning_threshold is not None:
                        if ml_metric.higher_is_better:
                            if value <= ml_metric.warning_threshold:
                                alert_level = "warning"
                        else:
                            if value >= ml_metric.warning_threshold:
                                alert_level = "warning"
                    
                    if alert_level and self.datadog_monitoring:
                        self.datadog_monitoring.counter(
                            "ml.metric.threshold_breach",
                            tags=[
                                f"model_id:{model.model_id}",
                                f"metric:{metric_name}",
                                f"alert_level:{alert_level}",
                                f"value:{value}",
                                f"service:{self.service_name}"
                            ]
                        )
                        
        except Exception as e:
            self.logger.error(f"Failed to check model performance thresholds: {str(e)}")
    
    async def track_model_drift(self, model_id: str, drift_type: DriftType,
                              drift_score: float, feature_name: Optional[str] = None,
                              threshold: float = 0.1) -> bool:
        """Track model drift detection"""
        
        try:
            if model_id not in self.model_registry:
                self.logger.warning(f"Unknown model: {model_id}")
                return False
            
            model = self.model_registry[model_id]
            is_drift_detected = drift_score > threshold
            
            drift_record = {
                "timestamp": datetime.utcnow().isoformat(),
                "model_id": model_id,
                "drift_type": drift_type.value,
                "drift_score": drift_score,
                "feature_name": feature_name,
                "threshold": threshold,
                "drift_detected": is_drift_detected
            }
            
            self.drift_detections.append(drift_record)
            
            # Keep only last 10000 drift records
            if len(self.drift_detections) > 10000:
                self.drift_detections = self.drift_detections[-10000:]
            
            # Send to DataDog
            if self.datadog_monitoring:
                tags = [
                    f"model_id:{model_id}",
                    f"drift_type:{drift_type.value}",
                    f"drift_detected:{is_drift_detected}",
                    f"service:{self.service_name}"
                ]
                
                if feature_name:
                    tags.append(f"feature_name:{feature_name}")
                
                # Track drift score
                self.datadog_monitoring.gauge(
                    "ml.drift.score",
                    drift_score,
                    tags=tags
                )
                
                # Track drift detection events
                if is_drift_detected:
                    self.datadog_monitoring.counter(
                        "ml.drift.detected",
                        tags=tags
                    )
                    
                    # Alert on critical drift
                    if drift_score > 0.5:  # Critical drift threshold
                        self.datadog_monitoring.counter(
                            "ml.drift.critical",
                            tags=tags
                        )
            
            self.logger.info(
                f"Tracked drift for model {model_id}: {drift_type.value} = {drift_score:.3f} "
                f"(detected: {is_drift_detected})"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track model drift: {str(e)}")
            return False
    
    async def track_ab_experiment(self, experiment_id: str, variant: str,
                                conversion_type: str, conversion_value: Optional[float] = None,
                                user_count: Optional[int] = None) -> bool:
        """Track A/B testing experiment results"""
        
        try:
            if experiment_id not in self.experiment_results:
                self.experiment_results[experiment_id] = {
                    "variants": {},
                    "total_users": 0,
                    "total_conversions": 0,
                    "start_time": datetime.utcnow().isoformat()
                }
            
            experiment = self.experiment_results[experiment_id]
            
            # Initialize variant if not exists
            if variant not in experiment["variants"]:
                experiment["variants"][variant] = {
                    "users": 0,
                    "conversions": 0,
                    "conversion_value": 0.0,
                    "conversion_rate": 0.0
                }
            
            variant_data = experiment["variants"][variant]
            
            # Update metrics
            if user_count:
                variant_data["users"] += user_count
                experiment["total_users"] += user_count
            
            variant_data["conversions"] += 1
            experiment["total_conversions"] += 1
            
            if conversion_value:
                variant_data["conversion_value"] += conversion_value
            
            # Calculate conversion rate
            if variant_data["users"] > 0:
                variant_data["conversion_rate"] = variant_data["conversions"] / variant_data["users"]
            
            # Send to DataDog
            if self.datadog_monitoring:
                tags = [
                    f"experiment_id:{experiment_id}",
                    f"variant:{variant}",
                    f"conversion_type:{conversion_type}",
                    f"service:{self.service_name}"
                ]
                
                # Track experiment metrics
                self.datadog_monitoring.counter("ml.experiments.conversions", tags=tags)
                self.datadog_monitoring.gauge("ml.experiments.conversion_rate", 
                                            variant_data["conversion_rate"], tags=tags)
                
                if conversion_value:
                    self.datadog_monitoring.histogram("ml.experiments.conversion_value", 
                                                     conversion_value, tags=tags)
                
                # Calculate statistical significance (simplified)
                total_variants = len(experiment["variants"])
                if total_variants >= 2 and experiment["total_users"] >= 100:
                    # This is a simplified significance calculation
                    # In practice, you'd use proper statistical tests
                    significance = min(0.95, experiment["total_conversions"] / 100)
                    self.datadog_monitoring.gauge("ml.experiments.significance", 
                                                significance, tags=tags)
            
            self.logger.debug(f"Tracked A/B experiment {experiment_id}: variant {variant}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track A/B experiment: {str(e)}")
            return False
    
    async def track_feature_quality(self, feature_name: str, quality_metrics: Dict[str, float],
                                  pipeline: Optional[str] = None) -> bool:
        """Track feature quality metrics"""
        
        try:
            # Store feature quality record
            if feature_name not in self.feature_quality_scores:
                self.feature_quality_scores[feature_name] = []
            
            quality_record = {
                "timestamp": datetime.utcnow().isoformat(),
                "feature_name": feature_name,
                "pipeline": pipeline,
                "metrics": quality_metrics
            }
            
            self.feature_quality_scores[feature_name].append(quality_record)
            
            # Keep only last 1000 records per feature
            if len(self.feature_quality_scores[feature_name]) > 1000:
                self.feature_quality_scores[feature_name] = self.feature_quality_scores[feature_name][-1000:]
            
            # Send to DataDog
            if self.datadog_monitoring:
                tags = [
                    f"feature_name:{feature_name}",
                    f"service:{self.service_name}"
                ]
                
                if pipeline:
                    tags.append(f"pipeline:{pipeline}")
                
                # Track all quality metrics
                for metric_name, value in quality_metrics.items():
                    self.datadog_monitoring.gauge(
                        f"ml.features.quality.{metric_name}",
                        value,
                        tags=tags
                    )
                
                # Calculate overall quality score
                overall_score = sum(quality_metrics.values()) / len(quality_metrics)
                self.datadog_monitoring.gauge(
                    "ml.features.quality.overall_score",
                    overall_score,
                    tags=tags
                )
            
            self.logger.debug(f"Tracked feature quality for {feature_name}: {quality_metrics}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track feature quality: {str(e)}")
            return False
    
    def get_ml_operations_summary(self) -> Dict[str, Any]:
        """Get comprehensive ML operations summary"""
        
        try:
            current_time = datetime.utcnow()
            
            # Model registry summary
            models_by_stage = {}
            models_by_type = {}
            for model in self.model_registry.values():
                stage = model.stage.value
                model_type = model.model_type
                
                models_by_stage[stage] = models_by_stage.get(stage, 0) + 1
                models_by_type[model_type] = models_by_type.get(model_type, 0) + 1
            
            # Recent predictions summary
            recent_predictions = 0
            for model_id, predictions in self.model_predictions.items():
                # Count predictions from last 24 hours
                cutoff = current_time - timedelta(hours=24)
                recent = [
                    p for p in predictions 
                    if datetime.fromisoformat(p["timestamp"]) >= cutoff
                ]
                recent_predictions += len(recent)
            
            # Drift detection summary
            recent_drifts = [
                d for d in self.drift_detections
                if datetime.fromisoformat(d["timestamp"]) >= current_time - timedelta(hours=24)
            ]
            
            drift_by_type = {}
            for drift in recent_drifts:
                drift_type = drift["drift_type"]
                drift_by_type[drift_type] = drift_by_type.get(drift_type, 0) + 1
            
            # Experiment summary
            active_experiments = len(self.experiment_results)
            total_experiment_users = sum(
                exp["total_users"] for exp in self.experiment_results.values()
            )
            
            # Feature quality summary
            features_monitored = len(self.feature_quality_scores)
            
            summary = {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "models": {
                    "total_registered": len(self.model_registry),
                    "by_stage": models_by_stage,
                    "by_type": models_by_type,
                    "recent_predictions_24h": recent_predictions
                },
                "drift_monitoring": {
                    "total_detections_24h": len(recent_drifts),
                    "by_type": drift_by_type,
                    "models_with_drift": len(set(d["model_id"] for d in recent_drifts if d["drift_detected"]))
                },
                "experiments": {
                    "active_experiments": active_experiments,
                    "total_users_in_experiments": total_experiment_users
                },
                "feature_quality": {
                    "features_monitored": features_monitored
                },
                "dashboards": {
                    "types_available": [dt.value for dt in MLDashboardType],
                    "total_types": len(MLDashboardType)
                }
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to generate ML operations summary: {str(e)}")
            return {}


# Global ML operations dashboards instance
_ml_operations_dashboards: Optional[DataDogMLOperationsDashboards] = None


def get_ml_operations_dashboards(service_name: str = "ml-operations",
                               datadog_monitoring: Optional[DatadogMonitoring] = None,
                               dashboard_manager: Optional[DataDogDashboardManager] = None,
                               business_metrics: Optional[DataDogBusinessMetricsTracker] = None) -> DataDogMLOperationsDashboards:
    """Get or create ML operations dashboards instance"""
    global _ml_operations_dashboards
    
    if _ml_operations_dashboards is None:
        _ml_operations_dashboards = DataDogMLOperationsDashboards(
            service_name, datadog_monitoring, dashboard_manager, business_metrics
        )
    
    return _ml_operations_dashboards


# Convenience functions

async def track_model_performance(model_id: str, metrics: Dict[str, float], 
                                model_version: Optional[str] = None) -> bool:
    """Convenience function for tracking model performance"""
    ml_dashboards = get_ml_operations_dashboards()
    return await ml_dashboards.track_model_performance(model_id, metrics, model_version)


async def track_model_drift(model_id: str, drift_type: DriftType, drift_score: float,
                          feature_name: Optional[str] = None) -> bool:
    """Convenience function for tracking model drift"""
    ml_dashboards = get_ml_operations_dashboards()
    return await ml_dashboards.track_model_drift(model_id, drift_type, drift_score, feature_name)


async def track_ab_experiment(experiment_id: str, variant: str, conversion_type: str,
                            conversion_value: Optional[float] = None) -> bool:
    """Convenience function for tracking A/B experiments"""
    ml_dashboards = get_ml_operations_dashboards()
    return await ml_dashboards.track_ab_experiment(experiment_id, variant, conversion_type, conversion_value)


async def create_all_ml_dashboards() -> Dict[MLDashboardType, Optional[str]]:
    """Convenience function for creating all ML dashboards"""
    ml_dashboards = get_ml_operations_dashboards()
    return await ml_dashboards.create_all_ml_dashboards()