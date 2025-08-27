"""
ML Deployment Orchestrator

Orchestrates the complete ML deployment pipeline including training,
model serving, A/B testing, and monitoring.
"""

import logging
import asyncio
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
import json
import uuid

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.ml.training.model_trainer import AutoMLTrainer, ModelConfig, TrainingResult
from src.ml.deployment.model_server import ModelRegistry, ModelMetadata, ModelServer
from src.ml.deployment.ab_testing import ABTestingFramework, ExperimentConfig, SplitStrategy
from src.ml.monitoring.model_monitor import ModelMonitor, MonitoringConfig
from src.ml.feature_engineering.feature_pipeline import FeatureEngineeringPipeline
from src.ml.feature_engineering.feature_store import FeatureStore

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class DeploymentConfig:
    """Configuration for ML deployment."""
    
    # Model configuration
    model_name: str
    model_version: str
    problem_type: str = "classification"
    
    # Training configuration
    training_config: Optional[ModelConfig] = None
    
    # A/B testing configuration
    enable_ab_testing: bool = True
    ab_test_duration_days: int = 14
    control_model_id: Optional[str] = None
    traffic_allocation: Optional[Dict[str, float]] = None
    
    # Monitoring configuration
    enable_monitoring: bool = True
    monitoring_config: Optional[MonitoringConfig] = None
    
    # Deployment targets
    deploy_to_staging: bool = True
    deploy_to_production: bool = False
    auto_promote: bool = False
    
    # Quality gates
    min_accuracy_threshold: float = 0.85
    min_sample_size: int = 1000
    max_prediction_latency_ms: float = 500.0
    
    # Metadata
    owner: str = "system"
    tags: List[str] = field(default_factory=list)


class DeploymentPipeline:
    """Complete ML deployment pipeline."""
    
    def __init__(self, 
                 model_registry: ModelRegistry,
                 model_server: ModelServer,
                 ab_testing_framework: ABTestingFramework,
                 model_monitor: Optional[ModelMonitor] = None,
                 feature_store: Optional[FeatureStore] = None):
        
        self.model_registry = model_registry
        self.model_server = model_server
        self.ab_testing_framework = ab_testing_framework
        self.model_monitor = model_monitor
        self.feature_store = feature_store
        self.metrics_collector = MetricsCollector()
        
    async def deploy_model(self, 
                          training_result: TrainingResult, 
                          config: DeploymentConfig,
                          feature_pipeline: Optional[FeatureEngineeringPipeline] = None) -> Dict[str, Any]:
        """Deploy a trained model through the complete pipeline."""
        
        deployment_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting deployment pipeline: {config.model_name} v{config.model_version}")
            
            deployment_result = {
                "deployment_id": deployment_id,
                "model_name": config.model_name,
                "model_version": config.model_version,
                "start_time": start_time,
                "status": "in_progress",
                "stages": {}
            }
            
            # Stage 1: Model Registration
            logger.info("Stage 1: Registering model")
            registration_result = await self._register_model(
                training_result, config, feature_pipeline
            )
            deployment_result["stages"]["registration"] = registration_result
            
            if not registration_result["success"]:
                deployment_result["status"] = "failed"
                deployment_result["error"] = "Model registration failed"
                return deployment_result
            
            model_id = registration_result["model_id"]
            
            # Stage 2: Quality Gates
            logger.info("Stage 2: Running quality gates")
            quality_result = await self._run_quality_gates(model_id, training_result, config)
            deployment_result["stages"]["quality_gates"] = quality_result
            
            if not quality_result["passed"]:
                deployment_result["status"] = "failed"
                deployment_result["error"] = "Quality gates failed"
                return deployment_result
            
            # Stage 3: Staging Deployment
            if config.deploy_to_staging:
                logger.info("Stage 3: Deploying to staging")
                staging_result = await self._deploy_to_staging(model_id, config)
                deployment_result["stages"]["staging"] = staging_result
                
                if not staging_result["success"]:
                    deployment_result["status"] = "failed"
                    deployment_result["error"] = "Staging deployment failed"
                    return deployment_result
            
            # Stage 4: A/B Testing Setup (if enabled)
            if config.enable_ab_testing:
                logger.info("Stage 4: Setting up A/B testing")
                ab_test_result = await self._setup_ab_testing(model_id, config)
                deployment_result["stages"]["ab_testing"] = ab_test_result
                
                if not ab_test_result["success"]:
                    logger.warning("A/B testing setup failed, continuing without it")
            
            # Stage 5: Monitoring Setup (if enabled)
            if config.enable_monitoring and self.model_monitor:
                logger.info("Stage 5: Setting up monitoring")
                monitoring_result = await self._setup_monitoring(model_id, config)
                deployment_result["stages"]["monitoring"] = monitoring_result
            
            # Stage 6: Production Deployment (if enabled)
            if config.deploy_to_production:
                logger.info("Stage 6: Deploying to production")
                production_result = await self._deploy_to_production(model_id, config)
                deployment_result["stages"]["production"] = production_result
                
                if not production_result["success"]:
                    deployment_result["status"] = "failed"
                    deployment_result["error"] = "Production deployment failed"
                    return deployment_result
            
            deployment_result["status"] = "completed"
            deployment_result["end_time"] = datetime.utcnow()
            deployment_result["duration_seconds"] = (
                deployment_result["end_time"] - start_time
            ).total_seconds()
            
            logger.info(f"Deployment pipeline completed: {config.model_name} v{config.model_version}")
            
            # Record metrics
            self.metrics_collector.increment_counter(
                "ml_deployment_completed_total",
                tags={
                    "model_name": config.model_name,
                    "model_type": training_result.model_type
                }
            )
            self.metrics_collector.record_histogram(
                "ml_deployment_duration_seconds",
                deployment_result["duration_seconds"]
            )
            
            return deployment_result
            
        except Exception as e:
            logger.error(f"Deployment pipeline failed: {str(e)}")
            deployment_result["status"] = "failed"
            deployment_result["error"] = str(e)
            deployment_result["end_time"] = datetime.utcnow()
            
            self.metrics_collector.increment_counter(
                "ml_deployment_errors_total",
                tags={
                    "model_name": config.model_name,
                    "error_type": type(e).__name__
                }
            )
            
            return deployment_result
    
    async def _register_model(self, 
                            training_result: TrainingResult, 
                            config: DeploymentConfig,
                            feature_pipeline: Optional[FeatureEngineeringPipeline]) -> Dict[str, Any]:
        """Register model in the model registry."""
        try:
            model_id = f"{config.model_name}_{config.model_version}_{training_result.model_type}"
            
            # Save feature pipeline if provided
            feature_pipeline_path = None
            if feature_pipeline:
                pipeline_dir = Path("models/feature_pipelines")
                pipeline_dir.mkdir(parents=True, exist_ok=True)
                feature_pipeline_path = str(pipeline_dir / f"{model_id}_pipeline.pkl")
                feature_pipeline.save(feature_pipeline_path)
            
            # Create model metadata
            metadata = ModelMetadata(
                model_id=model_id,
                model_name=config.model_name,
                model_type=training_result.model_type,
                version=config.model_version,
                problem_type=config.problem_type,
                input_schema={"features": "dict"},  # Simplified schema
                output_schema={"prediction": "number"},
                model_path=training_result.model_path or "",
                feature_pipeline_path=feature_pipeline_path
            )
            
            success = await self.model_registry.register_model(metadata)
            
            return {
                "success": success,
                "model_id": model_id,
                "metadata": {
                    "model_path": metadata.model_path,
                    "feature_pipeline_path": feature_pipeline_path
                }
            }
            
        except Exception as e:
            logger.error(f"Model registration failed: {str(e)}")
            return {"success": False, "error": str(e)}
    
    async def _run_quality_gates(self, 
                               model_id: str, 
                               training_result: TrainingResult, 
                               config: DeploymentConfig) -> Dict[str, Any]:
        """Run quality gates to ensure model meets deployment criteria."""
        try:
            checks = []
            passed = True
            
            # Check 1: Model accuracy/performance
            accuracy_check = {
                "name": "accuracy_threshold",
                "passed": training_result.best_score >= config.min_accuracy_threshold,
                "value": training_result.best_score,
                "threshold": config.min_accuracy_threshold
            }
            checks.append(accuracy_check)
            if not accuracy_check["passed"]:
                passed = False
            
            # Check 2: Model file exists
            model_file_check = {
                "name": "model_file_exists",
                "passed": Path(training_result.model_path).exists() if training_result.model_path else False,
                "value": training_result.model_path
            }
            checks.append(model_file_check)
            if not model_file_check["passed"]:
                passed = False
            
            # Check 3: Training stability (CV scores)
            if training_result.cv_scores:
                cv_std = pd.Series(training_result.cv_scores).std()
                stability_check = {
                    "name": "training_stability",
                    "passed": cv_std < 0.1,  # Less than 10% std deviation
                    "value": cv_std,
                    "threshold": 0.1
                }
                checks.append(stability_check)
                if not stability_check["passed"]:
                    passed = False
            
            # Check 4: Load model and test prediction
            load_test_result = await self._test_model_loading(model_id)
            checks.append(load_test_result)
            if not load_test_result["passed"]:
                passed = False
            
            return {
                "passed": passed,
                "checks": checks,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Quality gates failed: {str(e)}")
            return {
                "passed": False,
                "error": str(e),
                "checks": []
            }
    
    async def _test_model_loading(self, model_id: str) -> Dict[str, Any]:
        """Test if model can be loaded and make predictions."""
        try:
            # Load model
            load_success = await self.model_registry.load_model(model_id)
            if not load_success:
                return {
                    "name": "model_loading",
                    "passed": False,
                    "error": "Failed to load model"
                }
            
            # Test prediction with dummy data
            model = self.model_registry.get_model(model_id)
            if model is None:
                return {
                    "name": "model_loading",
                    "passed": False,
                    "error": "Model not found after loading"
                }
            
            # Create dummy test data (this would need to be adapted based on model input)
            try:
                import numpy as np
                dummy_features = np.array([[1, 2, 3, 4, 5]])  # Simplified
                prediction = model.predict(dummy_features)
                
                return {
                    "name": "model_loading",
                    "passed": True,
                    "test_prediction": str(prediction)
                }
                
            except Exception as pred_error:
                return {
                    "name": "model_loading",
                    "passed": False,
                    "error": f"Prediction test failed: {str(pred_error)}"
                }
            
        except Exception as e:
            return {
                "name": "model_loading",
                "passed": False,
                "error": str(e)
            }
    
    async def _deploy_to_staging(self, model_id: str, config: DeploymentConfig) -> Dict[str, Any]:
        """Deploy model to staging environment."""
        try:
            # In a real implementation, this would deploy to a staging k8s namespace
            # or separate staging infrastructure
            
            # For now, just ensure the model is loaded in the registry
            load_success = await self.model_registry.load_model(model_id)
            
            return {
                "success": load_success,
                "environment": "staging",
                "endpoint": f"http://staging-ml-api/models/{model_id}/predict",
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Staging deployment failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _setup_ab_testing(self, model_id: str, config: DeploymentConfig) -> Dict[str, Any]:
        """Setup A/B testing for the new model."""
        try:
            if not config.control_model_id:
                # If no control model specified, skip A/B testing
                return {
                    "success": False,
                    "error": "No control model specified for A/B testing"
                }
            
            experiment_name = f"{config.model_name}_v{config.model_version}_test"
            
            experiment_id = await self.ab_testing_framework.create_model_comparison_experiment(
                experiment_name=experiment_name,
                control_model_id=config.control_model_id,
                treatment_model_ids=[model_id],
                primary_metric="accuracy",
                duration_days=config.ab_test_duration_days,
                minimum_sample_size=config.min_sample_size,
                traffic_allocation=config.traffic_allocation
            )
            
            return {
                "success": True,
                "experiment_id": experiment_id,
                "experiment_name": experiment_name,
                "duration_days": config.ab_test_duration_days
            }
            
        except Exception as e:
            logger.error(f"A/B testing setup failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _setup_monitoring(self, model_id: str, config: DeploymentConfig) -> Dict[str, Any]:
        """Setup monitoring for the deployed model."""
        try:
            if not self.model_monitor:
                return {
                    "success": False,
                    "error": "Model monitor not available"
                }
            
            monitoring_config = config.monitoring_config or MonitoringConfig(
                model_id=model_id,
                check_interval_minutes=60,
                alert_thresholds={
                    "accuracy_drop": 0.05,
                    "latency_increase": 2.0,
                    "error_rate": 0.01
                }
            )
            
            success = await self.model_monitor.start_monitoring(model_id, monitoring_config)
            
            return {
                "success": success,
                "monitoring_config": {
                    "check_interval_minutes": monitoring_config.check_interval_minutes,
                    "alert_thresholds": monitoring_config.alert_thresholds
                }
            }
            
        except Exception as e:
            logger.error(f"Monitoring setup failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _deploy_to_production(self, model_id: str, config: DeploymentConfig) -> Dict[str, Any]:
        """Deploy model to production environment."""
        try:
            # In a real implementation, this would:
            # 1. Deploy to production k8s cluster
            # 2. Update load balancer configuration
            # 3. Set up auto-scaling
            # 4. Configure production monitoring
            
            # For now, just mark as production-ready in registry
            await self.model_registry.load_model(model_id)
            
            return {
                "success": True,
                "environment": "production",
                "endpoint": f"http://prod-ml-api/models/{model_id}/predict",
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Production deployment failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def promote_model(self, experiment_id: str) -> Dict[str, Any]:
        """Promote a model from A/B testing to full production."""
        try:
            # Check if experiment has significant results
            early_stopping_check = await self.ab_testing_framework.check_experiment_for_early_stopping(
                experiment_id
            )
            
            if not early_stopping_check.get("should_stop"):
                return {
                    "success": False,
                    "reason": "Experiment not ready for promotion",
                    "details": early_stopping_check
                }
            
            analysis = early_stopping_check.get("analysis", {})
            
            # Find the best performing model
            best_model_id = None
            best_score = -float('inf')
            
            for result_key, result_data in analysis.get("results", {}).items():
                if result_data.get("is_significant") and result_data.get("treatment_mean", 0) > best_score:
                    best_score = result_data.get("treatment_mean", 0)
                    # Extract model info from result key (this would need proper parsing)
                    best_model_id = result_key  # Simplified
            
            if not best_model_id:
                return {
                    "success": False,
                    "reason": "No model showed significant improvement"
                }
            
            # Stop the experiment
            await self.ab_testing_framework.experiment_manager.stop_experiment(experiment_id)
            
            # Promote the winning model (update production traffic routing)
            # In a real system, this would update service mesh or load balancer config
            
            return {
                "success": True,
                "promoted_model_id": best_model_id,
                "experiment_id": experiment_id,
                "performance_improvement": best_score,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Model promotion failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def rollback_deployment(self, model_id: str, reason: str) -> Dict[str, Any]:
        """Rollback a deployment."""
        try:
            # Unload model from serving
            await self.model_registry.unload_model(model_id)
            
            # Stop any active A/B tests
            # (This would need to query for active experiments with this model)
            
            # Alert monitoring systems
            if self.model_monitor:
                # This would send alerts about the rollback
                pass
            
            logger.warning(f"Model {model_id} rolled back: {reason}")
            
            return {
                "success": True,
                "model_id": model_id,
                "reason": reason,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Rollback failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_deployment_status(self, deployment_id: str) -> Dict[str, Any]:
        """Get deployment status and health."""
        try:
            # This would query deployment database/cache
            # For now, return a mock status
            
            return {
                "deployment_id": deployment_id,
                "status": "completed",
                "health": "healthy",
                "last_check": datetime.utcnow(),
                "metrics": {
                    "predictions_per_hour": 1000,
                    "avg_latency_ms": 45.2,
                    "error_rate": 0.001,
                    "accuracy": 0.92
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get deployment status: {str(e)}")
            return {
                "deployment_id": deployment_id,
                "status": "unknown",
                "error": str(e)
            }


class MLOpsOrchestrator:
    """High-level MLOps orchestrator."""
    
    def __init__(self):
        self.model_registry = ModelRegistry()
        self.feature_store = FeatureStore()
        self.model_server = ModelServer(self.model_registry, self.feature_store)
        self.ab_testing_framework = ABTestingFramework()
        self.model_monitor = ModelMonitor()
        
        self.deployment_pipeline = DeploymentPipeline(
            model_registry=self.model_registry,
            model_server=self.model_server,
            ab_testing_framework=self.ab_testing_framework,
            model_monitor=self.model_monitor,
            feature_store=self.feature_store
        )
        
        self.metrics_collector = MetricsCollector()
    
    async def train_and_deploy(self, 
                              data: pd.DataFrame,
                              target_column: str,
                              deployment_config: DeploymentConfig) -> Dict[str, Any]:
        """Complete train and deploy pipeline."""
        try:
            # Prepare training data
            X = data.drop(columns=[target_column])
            y = data[target_column]
            
            # Create trainer
            trainer_config = deployment_config.training_config or ModelConfig(
                problem_type=deployment_config.problem_type,
                model_types=["random_forest", "xgboost", "lightgbm"],
                primary_metric="f1" if deployment_config.problem_type == "classification" else "rmse",
                n_trials=50
            )
            
            trainer = AutoMLTrainer(trainer_config)
            
            # Train models
            logger.info("Starting model training")
            training_results = trainer.train_models(X, y)
            
            if not training_results:
                return {
                    "success": False,
                    "error": "No models were successfully trained"
                }
            
            # Get best model
            best_result = training_results[0]  # Results are sorted by score
            
            logger.info(f"Best model: {best_result.model_type} with score {best_result.best_score}")
            
            # Deploy the best model
            deployment_result = await self.deployment_pipeline.deploy_model(
                training_result=best_result,
                config=deployment_config
            )
            
            return {
                "success": True,
                "training_results": {
                    "best_model_type": best_result.model_type,
                    "best_score": best_result.best_score,
                    "training_time": best_result.training_time,
                    "total_models_trained": len(training_results)
                },
                "deployment_result": deployment_result
            }
            
        except Exception as e:
            logger.error(f"Train and deploy pipeline failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health status."""
        try:
            health_status = {
                "timestamp": datetime.utcnow(),
                "overall_status": "healthy",
                "components": {}
            }
            
            # Check model registry
            registered_models = len(self.model_registry.models)
            loaded_models = len(self.model_registry.loaded_models)
            
            health_status["components"]["model_registry"] = {
                "status": "healthy",
                "registered_models": registered_models,
                "loaded_models": loaded_models
            }
            
            # Check A/B testing
            active_experiments = await self.ab_testing_framework.get_active_experiments()
            
            health_status["components"]["ab_testing"] = {
                "status": "healthy",
                "active_experiments": len(active_experiments)
            }
            
            # Check monitoring
            health_status["components"]["monitoring"] = {
                "status": "healthy" if self.model_monitor else "disabled",
                "monitored_models": 0  # Would query actual monitoring data
            }
            
            return health_status
            
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return {
                "timestamp": datetime.utcnow(),
                "overall_status": "unhealthy",
                "error": str(e)
            }


# Factory functions
def create_deployment_pipeline(model_registry: Optional[ModelRegistry] = None,
                             model_server: Optional[ModelServer] = None,
                             ab_testing_framework: Optional[ABTestingFramework] = None) -> DeploymentPipeline:
    """Create deployment pipeline instance."""
    
    if not model_registry:
        model_registry = ModelRegistry()
    
    if not model_server:
        feature_store = FeatureStore()
        model_server = ModelServer(model_registry, feature_store)
    
    if not ab_testing_framework:
        ab_testing_framework = ABTestingFramework()
    
    model_monitor = ModelMonitor()
    
    return DeploymentPipeline(
        model_registry=model_registry,
        model_server=model_server,
        ab_testing_framework=ab_testing_framework,
        model_monitor=model_monitor
    )


def create_mlops_orchestrator() -> MLOpsOrchestrator:
    """Create MLOps orchestrator instance."""
    return MLOpsOrchestrator()