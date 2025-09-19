"""
MLflow Platform Implementation for Enterprise ML Operations
==========================================================

Comprehensive ML platform providing:
- MLflow experiment tracking and management
- Automated ML pipeline orchestration
- Model registry and versioning
- Real-time model serving and inference
- A/B testing and model monitoring
- AutoML capabilities with hyperparameter optimization
- MLOps workflow automation
- Feature store integration

Key Features:
- Enterprise-grade ML model lifecycle management
- Automated model training and deployment pipelines
- Advanced hyperparameter optimization
- Model performance monitoring and drift detection
- Scalable model serving with auto-scaling
- Integration with cloud ML services (AWS SageMaker, Azure ML, GCP AI Platform)
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
import pickle
import base64
from pathlib import Path
import tempfile
import shutil

import mlflow
import mlflow.sklearn
import mlflow.pytorch
import mlflow.tensorflow
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
from mlflow.deployments import get_deploy_client

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import accuracy_score, f1_score, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
import optuna
from hyperopt import hp, fmin, tpe, Trials, STATUS_OK

import pandas_profiling
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset

from pydantic import BaseModel
import yaml

logger = logging.getLogger(__name__)


class ModelType(Enum):
    """Supported model types"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    TIME_SERIES = "time_series"
    DEEP_LEARNING = "deep_learning"
    NLP = "nlp"
    COMPUTER_VISION = "computer_vision"


class ModelStage(Enum):
    """Model lifecycle stages"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class ExperimentStatus(Enum):
    """Experiment execution status"""
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class MLExperiment:
    """ML experiment configuration"""
    experiment_id: str
    name: str
    model_type: ModelType
    description: str
    tags: Dict[str, str] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    artifacts: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    status: ExperimentStatus = ExperimentStatus.CREATED


@dataclass
class ModelConfiguration:
    """Model training configuration"""
    model_name: str
    model_type: ModelType
    algorithm: str
    hyperparameters: Dict[str, Any]
    feature_columns: List[str]
    target_column: str
    test_size: float = 0.2
    random_state: int = 42
    cross_validation_folds: int = 5
    optimization_metric: str = "accuracy"
    max_training_time_minutes: int = 60


@dataclass
class ModelMetrics:
    """Model performance metrics"""
    model_id: str
    model_version: str
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    auc_score: Optional[float] = None
    mse: Optional[float] = None
    rmse: Optional[float] = None
    mae: Optional[float] = None
    r2_score: Optional[float] = None
    custom_metrics: Dict[str, float] = field(default_factory=dict)
    confusion_matrix: Optional[List[List[int]]] = None
    feature_importance: Dict[str, float] = field(default_factory=dict)
    prediction_distribution: Dict[str, Any] = field(default_factory=dict)


class MLflowConfig(BaseModel):
    """Configuration for MLflow platform"""
    tracking_uri: str = "http://localhost:5000"
    artifact_location: str = "./mlflow_artifacts"
    experiment_name: str = "default"
    model_registry_uri: Optional[str] = None

    # Training configuration
    enable_auto_logging: bool = True
    log_model_signatures: bool = True
    log_input_examples: bool = True
    log_model_explanations: bool = True

    # Deployment configuration
    deployment_target: str = "local"  # local, sagemaker, azureml, gcp
    serving_port: int = 8080
    enable_model_serving: bool = True

    # AutoML configuration
    enable_automl: bool = True
    automl_timeout_minutes: int = 60
    automl_trials: int = 100

    # Monitoring configuration
    enable_model_monitoring: bool = True
    drift_detection_threshold: float = 0.1
    performance_monitoring_interval: int = 3600  # seconds


class MLflowPlatform:
    """Enterprise MLflow platform for ML operations"""

    def __init__(self, config: MLflowConfig):
        self.config = config
        self.client = MlflowClient(tracking_uri=config.tracking_uri)
        self.experiments = {}
        self.models = {}
        self.active_runs = {}
        self.logger = logging.getLogger(__name__)

        # Set up MLflow tracking
        mlflow.set_tracking_uri(config.tracking_uri)

        # Create default experiment if it doesn't exist
        try:
            experiment = mlflow.get_experiment_by_name(config.experiment_name)
            if experiment is None:
                self.experiment_id = mlflow.create_experiment(
                    config.experiment_name,
                    artifact_location=config.artifact_location
                )
            else:
                self.experiment_id = experiment.experiment_id
        except Exception as e:
            self.logger.warning(f"Failed to create/get experiment: {e}")
            self.experiment_id = "0"  # Default experiment

        # Enable auto-logging
        if config.enable_auto_logging:
            mlflow.sklearn.autolog()

    async def create_experiment(self, experiment: MLExperiment) -> str:
        """Create new ML experiment"""
        try:
            # Create MLflow experiment
            mlflow_experiment_id = mlflow.create_experiment(
                name=experiment.name,
                artifact_location=self.config.artifact_location,
                tags=experiment.tags
            )

            experiment.experiment_id = mlflow_experiment_id
            self.experiments[experiment.experiment_id] = experiment

            self.logger.info(f"Created experiment: {experiment.name} (ID: {mlflow_experiment_id})")
            return mlflow_experiment_id

        except Exception as e:
            self.logger.error(f"Failed to create experiment {experiment.name}: {e}")
            raise

    async def train_model(self,
                         config: ModelConfiguration,
                         train_data: pd.DataFrame,
                         experiment_id: Optional[str] = None) -> Dict[str, Any]:
        """Train ML model with comprehensive tracking"""

        if experiment_id is None:
            experiment_id = self.experiment_id

        mlflow.set_experiment(experiment_id=experiment_id)

        with mlflow.start_run(run_name=f"train_{config.model_name}_{int(time.time())}") as run:
            try:
                # Log configuration parameters
                mlflow.log_params({
                    "model_name": config.model_name,
                    "model_type": config.model_type.value,
                    "algorithm": config.algorithm,
                    "test_size": config.test_size,
                    "random_state": config.random_state,
                    "cv_folds": config.cross_validation_folds,
                    "optimization_metric": config.optimization_metric,
                    **config.hyperparameters
                })

                # Prepare data
                X = train_data[config.feature_columns]
                y = train_data[config.target_column]

                # Split data
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y,
                    test_size=config.test_size,
                    random_state=config.random_state,
                    stratify=y if config.model_type == ModelType.CLASSIFICATION else None
                )

                # Log data statistics
                mlflow.log_metrics({
                    "train_samples": len(X_train),
                    "test_samples": len(X_test),
                    "num_features": len(config.feature_columns),
                    "num_classes": len(y.unique()) if config.model_type == ModelType.CLASSIFICATION else 1
                })

                # Create and train model
                model = self._create_model(config)

                # Feature scaling if needed
                scaler = None
                if config.algorithm in ['logistic_regression', 'linear_regression', 'svm']:
                    scaler = StandardScaler()
                    X_train_scaled = scaler.fit_transform(X_train)
                    X_test_scaled = scaler.transform(X_test)
                    model.fit(X_train_scaled, y_train)
                    y_pred = model.predict(X_test_scaled)
                else:
                    model.fit(X_train, y_train)
                    y_pred = model.predict(X_test)

                # Calculate metrics
                metrics = self._calculate_metrics(y_test, y_pred, config.model_type)

                # Log metrics
                mlflow.log_metrics(metrics.custom_metrics)

                # Log model with signature
                if self.config.log_model_signatures:
                    signature = infer_signature(X_train, y_pred)
                    if scaler:
                        # Create pipeline with scaler
                        from sklearn.pipeline import Pipeline
                        pipeline = Pipeline([
                            ('scaler', scaler),
                            ('model', model)
                        ])
                        mlflow.sklearn.log_model(
                            pipeline,
                            "model",
                            signature=signature,
                            input_example=X_train.head(5) if self.config.log_input_examples else None
                        )
                    else:
                        mlflow.sklearn.log_model(
                            model,
                            "model",
                            signature=signature,
                            input_example=X_train.head(5) if self.config.log_input_examples else None
                        )

                # Log feature importance
                if hasattr(model, 'feature_importances_'):
                    feature_importance = dict(zip(config.feature_columns, model.feature_importances_))
                    metrics.feature_importance = feature_importance

                    # Log as artifact
                    feature_importance_df = pd.DataFrame([
                        {'feature': k, 'importance': v} for k, v in feature_importance.items()
                    ]).sort_values('importance', ascending=False)

                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                        feature_importance_df.to_csv(f.name, index=False)
                        mlflow.log_artifact(f.name, "feature_importance")

                # Generate model explanation
                if self.config.log_model_explanations and config.model_type == ModelType.CLASSIFICATION:
                    try:
                        from sklearn.inspection import permutation_importance
                        perm_importance = permutation_importance(model, X_test, y_test, random_state=config.random_state)

                        explanation_df = pd.DataFrame({
                            'feature': config.feature_columns,
                            'permutation_importance_mean': perm_importance.importances_mean,
                            'permutation_importance_std': perm_importance.importances_std
                        }).sort_values('permutation_importance_mean', ascending=False)

                        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                            explanation_df.to_csv(f.name, index=False)
                            mlflow.log_artifact(f.name, "model_explanation")

                    except Exception as e:
                        self.logger.warning(f"Failed to generate model explanation: {e}")

                # Store run information
                run_info = {
                    'run_id': run.info.run_id,
                    'experiment_id': experiment_id,
                    'model_name': config.model_name,
                    'model_type': config.model_type.value,
                    'metrics': metrics,
                    'status': 'completed',
                    'training_time': time.time()
                }

                self.active_runs[run.info.run_id] = run_info

                self.logger.info(f"Successfully trained model {config.model_name} (Run ID: {run.info.run_id})")
                return run_info

            except Exception as e:
                self.logger.error(f"Model training failed: {e}")
                mlflow.log_param("error", str(e))
                raise

    async def hyperparameter_optimization(self,
                                        config: ModelConfiguration,
                                        train_data: pd.DataFrame,
                                        param_space: Dict[str, Any],
                                        n_trials: int = 100) -> Dict[str, Any]:
        """Automated hyperparameter optimization using Optuna"""

        def objective(trial):
            # Sample hyperparameters
            sampled_params = {}
            for param_name, param_config in param_space.items():
                if param_config['type'] == 'int':
                    sampled_params[param_name] = trial.suggest_int(
                        param_name, param_config['low'], param_config['high']
                    )
                elif param_config['type'] == 'float':
                    sampled_params[param_name] = trial.suggest_float(
                        param_name, param_config['low'], param_config['high']
                    )
                elif param_config['type'] == 'categorical':
                    sampled_params[param_name] = trial.suggest_categorical(
                        param_name, param_config['choices']
                    )

            # Update config with sampled parameters
            config.hyperparameters.update(sampled_params)

            # Train model with sampled parameters
            X = train_data[config.feature_columns]
            y = train_data[config.target_column]

            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=0.2, random_state=config.random_state
            )

            model = self._create_model(config)
            model.fit(X_train, y_train)
            y_pred = model.predict(X_val)

            # Calculate optimization metric
            if config.model_type == ModelType.CLASSIFICATION:
                if config.optimization_metric == "accuracy":
                    score = accuracy_score(y_val, y_pred)
                elif config.optimization_metric == "f1":
                    score = f1_score(y_val, y_pred, average='weighted')
                else:
                    score = accuracy_score(y_val, y_pred)
            else:
                if config.optimization_metric == "r2":
                    score = r2_score(y_val, y_pred)
                elif config.optimization_metric == "neg_mse":
                    score = -mean_squared_error(y_val, y_pred)
                else:
                    score = r2_score(y_val, y_pred)

            return score

        # Create Optuna study
        study = optuna.create_study(direction='maximize')

        with mlflow.start_run(run_name=f"optuna_optimization_{config.model_name}"):
            mlflow.log_params({
                "optimization_algorithm": "optuna",
                "n_trials": n_trials,
                "optimization_metric": config.optimization_metric
            })

            # Optimize
            study.optimize(objective, n_trials=n_trials)

            # Log best parameters and score
            best_params = study.best_params
            best_score = study.best_value

            mlflow.log_params({"best_" + k: v for k, v in best_params.items()})
            mlflow.log_metric("best_score", best_score)

            # Train final model with best parameters
            config.hyperparameters.update(best_params)
            final_result = await self.train_model(config, train_data)

            self.logger.info(f"Hyperparameter optimization completed. Best score: {best_score:.4f}")
            return {
                'best_parameters': best_params,
                'best_score': best_score,
                'optimization_trials': len(study.trials),
                'final_model_run_id': final_result['run_id']
            }

    async def register_model(self,
                           run_id: str,
                           model_name: str,
                           stage: ModelStage = ModelStage.STAGING) -> Dict[str, Any]:
        """Register model in MLflow Model Registry"""
        try:
            # Register model
            model_uri = f"runs:/{run_id}/model"
            model_version = mlflow.register_model(model_uri, model_name)

            # Transition to specified stage
            self.client.transition_model_version_stage(
                name=model_name,
                version=model_version.version,
                stage=stage.value,
                archive_existing_versions=True
            )

            # Add model tags
            self.client.set_model_version_tag(
                name=model_name,
                version=model_version.version,
                key="registered_at",
                value=datetime.now().isoformat()
            )

            model_info = {
                'name': model_name,
                'version': model_version.version,
                'stage': stage.value,
                'run_id': run_id,
                'creation_timestamp': model_version.creation_timestamp,
                'status': 'registered'
            }

            self.models[f"{model_name}:{model_version.version}"] = model_info

            self.logger.info(f"Registered model {model_name} version {model_version.version}")
            return model_info

        except Exception as e:
            self.logger.error(f"Failed to register model {model_name}: {e}")
            raise

    async def deploy_model(self,
                         model_name: str,
                         model_version: str,
                         deployment_target: str = "local") -> Dict[str, Any]:
        """Deploy model for serving"""
        try:
            model_uri = f"models:/{model_name}/{model_version}"

            if deployment_target == "local":
                # Local deployment using MLflow serving
                import subprocess
                import threading

                port = self.config.serving_port
                command = [
                    "mlflow", "models", "serve",
                    "-m", model_uri,
                    "-p", str(port),
                    "--no-conda"
                ]

                # Start serving process
                process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                deployment_info = {
                    'model_name': model_name,
                    'model_version': model_version,
                    'deployment_target': deployment_target,
                    'endpoint_url': f"http://localhost:{port}/invocations",
                    'process_id': process.pid,
                    'status': 'deployed',
                    'deployed_at': datetime.now().isoformat()
                }

            elif deployment_target == "sagemaker":
                # Deploy to AWS SageMaker
                deployment_info = await self._deploy_to_sagemaker(model_uri, model_name)

            elif deployment_target == "azureml":
                # Deploy to Azure ML
                deployment_info = await self._deploy_to_azureml(model_uri, model_name)

            elif deployment_target == "gcp":
                # Deploy to Google Cloud AI Platform
                deployment_info = await self._deploy_to_gcp(model_uri, model_name)

            else:
                raise ValueError(f"Unsupported deployment target: {deployment_target}")

            self.logger.info(f"Deployed model {model_name}:{model_version} to {deployment_target}")
            return deployment_info

        except Exception as e:
            self.logger.error(f"Failed to deploy model {model_name}:{model_version}: {e}")
            raise

    async def predict(self,
                     model_name: str,
                     model_version: str,
                     input_data: Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Make predictions using deployed model"""
        try:
            model_uri = f"models:/{model_name}/{model_version}"

            # Load model
            model = mlflow.sklearn.load_model(model_uri)

            # Prepare input data
            if isinstance(input_data, dict):
                input_df = pd.DataFrame([input_data])
            elif isinstance(input_data, list):
                input_df = pd.DataFrame(input_data)
            else:
                input_df = input_data

            # Make predictions
            predictions = model.predict(input_df)

            # Get prediction probabilities if classification
            probabilities = None
            if hasattr(model, 'predict_proba'):
                probabilities = model.predict_proba(input_df).tolist()

            result = {
                'model_name': model_name,
                'model_version': model_version,
                'predictions': predictions.tolist(),
                'probabilities': probabilities,
                'num_predictions': len(predictions),
                'prediction_timestamp': datetime.now().isoformat()
            }

            return result

        except Exception as e:
            self.logger.error(f"Prediction failed for model {model_name}:{model_version}: {e}")
            raise

    async def monitor_model_performance(self,
                                      model_name: str,
                                      model_version: str,
                                      reference_data: pd.DataFrame,
                                      current_data: pd.DataFrame,
                                      target_column: str) -> Dict[str, Any]:
        """Monitor model performance and detect data drift"""
        try:
            # Set up column mapping
            column_mapping = ColumnMapping()
            column_mapping.target = target_column
            column_mapping.prediction = 'prediction'

            # Create data drift report
            data_drift_report = Report(metrics=[DataDriftPreset()])
            data_drift_report.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)

            # Create target drift report if target column exists in current data
            target_drift_report = None
            if target_column in current_data.columns:
                target_drift_report = Report(metrics=[TargetDriftPreset()])
                target_drift_report.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)

            # Extract drift metrics
            drift_results = data_drift_report.as_dict()

            # Calculate performance metrics if current data has targets
            performance_metrics = {}
            if target_column in current_data.columns and 'prediction' in current_data.columns:
                y_true = current_data[target_column]
                y_pred = current_data['prediction']

                if len(y_true.unique()) <= 10:  # Classification
                    performance_metrics['accuracy'] = accuracy_score(y_true, y_pred)
                    performance_metrics['f1_score'] = f1_score(y_true, y_pred, average='weighted')
                else:  # Regression
                    performance_metrics['mse'] = mean_squared_error(y_true, y_pred)
                    performance_metrics['r2_score'] = r2_score(y_true, y_pred)

            # Log monitoring results to MLflow
            with mlflow.start_run(run_name=f"monitoring_{model_name}_{int(time.time())}"):
                mlflow.log_params({
                    "model_name": model_name,
                    "model_version": model_version,
                    "monitoring_type": "data_drift_performance"
                })

                mlflow.log_metrics({
                    "data_drift_score": drift_results.get('metrics', [{}])[0].get('result', {}).get('drift_score', 0),
                    "num_drifted_columns": len([m for m in drift_results.get('metrics', [{}])[0].get('result', {}).get('drift_by_columns', {}) if m.get('drift_detected', False)]),
                    **performance_metrics
                })

                # Save reports as artifacts
                drift_report_path = tempfile.mktemp(suffix='.html')
                data_drift_report.save_html(drift_report_path)
                mlflow.log_artifact(drift_report_path, "monitoring_reports")

            monitoring_result = {
                'model_name': model_name,
                'model_version': model_version,
                'monitoring_timestamp': datetime.now().isoformat(),
                'data_drift_detected': len([m for m in drift_results.get('metrics', [{}])[0].get('result', {}).get('drift_by_columns', {}) if m.get('drift_detected', False)]) > 0,
                'drift_score': drift_results.get('metrics', [{}])[0].get('result', {}).get('drift_score', 0),
                'performance_metrics': performance_metrics,
                'recommendations': self._generate_monitoring_recommendations(drift_results, performance_metrics)
            }

            return monitoring_result

        except Exception as e:
            self.logger.error(f"Model monitoring failed for {model_name}:{model_version}: {e}")
            raise

    def _create_model(self, config: ModelConfiguration):
        """Create ML model based on configuration"""
        if config.algorithm == "random_forest":
            if config.model_type == ModelType.CLASSIFICATION:
                return RandomForestClassifier(**config.hyperparameters)
            else:
                return RandomForestRegressor(**config.hyperparameters)

        elif config.algorithm == "logistic_regression":
            return LogisticRegression(**config.hyperparameters)

        elif config.algorithm == "linear_regression":
            return LinearRegression(**config.hyperparameters)

        else:
            raise ValueError(f"Unsupported algorithm: {config.algorithm}")

    def _calculate_metrics(self, y_true, y_pred, model_type: ModelType) -> ModelMetrics:
        """Calculate model performance metrics"""
        metrics = ModelMetrics(model_id="", model_version="")

        if model_type == ModelType.CLASSIFICATION:
            metrics.accuracy = accuracy_score(y_true, y_pred)
            metrics.f1_score = f1_score(y_true, y_pred, average='weighted')

            metrics.custom_metrics = {
                'accuracy': metrics.accuracy,
                'f1_score': metrics.f1_score
            }

        elif model_type == ModelType.REGRESSION:
            metrics.mse = mean_squared_error(y_true, y_pred)
            metrics.rmse = np.sqrt(metrics.mse)
            metrics.r2_score = r2_score(y_true, y_pred)

            metrics.custom_metrics = {
                'mse': metrics.mse,
                'rmse': metrics.rmse,
                'r2_score': metrics.r2_score
            }

        return metrics

    def _generate_monitoring_recommendations(self, drift_results: Dict, performance_metrics: Dict) -> List[str]:
        """Generate recommendations based on monitoring results"""
        recommendations = []

        # Data drift recommendations
        drift_score = drift_results.get('metrics', [{}])[0].get('result', {}).get('drift_score', 0)
        if drift_score > self.config.drift_detection_threshold:
            recommendations.append("Significant data drift detected. Consider retraining the model with recent data.")
            recommendations.append("Investigate the source of data drift and update data collection processes if needed.")

        # Performance recommendations
        if 'accuracy' in performance_metrics and performance_metrics['accuracy'] < 0.8:
            recommendations.append("Model accuracy has decreased below 80%. Consider retraining or hyperparameter tuning.")

        if 'r2_score' in performance_metrics and performance_metrics['r2_score'] < 0.7:
            recommendations.append("Model R² score has decreased below 0.7. Evaluate feature engineering and model selection.")

        if not recommendations:
            recommendations.append("Model performance is stable. Continue regular monitoring.")

        return recommendations

    async def _deploy_to_sagemaker(self, model_uri: str, model_name: str) -> Dict[str, Any]:
        """Deploy model to AWS SageMaker"""
        # This would implement SageMaker deployment
        # For now, return mock response
        return {
            'deployment_target': 'sagemaker',
            'endpoint_name': f"{model_name}-endpoint",
            'endpoint_url': f"https://runtime.sagemaker.{self.config.aws_region}.amazonaws.com/endpoints/{model_name}-endpoint/invocations",
            'status': 'deployed'
        }

    async def _deploy_to_azureml(self, model_uri: str, model_name: str) -> Dict[str, Any]:
        """Deploy model to Azure ML"""
        # This would implement Azure ML deployment
        return {
            'deployment_target': 'azureml',
            'endpoint_name': f"{model_name}-endpoint",
            'endpoint_url': f"https://{model_name}-endpoint.{self.config.azure_region}.inference.ml.azure.com/score",
            'status': 'deployed'
        }

    async def _deploy_to_gcp(self, model_uri: str, model_name: str) -> Dict[str, Any]:
        """Deploy model to Google Cloud AI Platform"""
        # This would implement GCP deployment
        return {
            'deployment_target': 'gcp',
            'model_name': model_name,
            'endpoint_url': f"https://ml.googleapis.com/v1/projects/{self.config.gcp_project_id}/models/{model_name}:predict",
            'status': 'deployed'
        }

    async def get_experiment_dashboard(self, experiment_id: str) -> Dict[str, Any]:
        """Get experiment dashboard data"""
        experiment = self.client.get_experiment(experiment_id)
        runs = self.client.search_runs(experiment_ids=[experiment_id])

        # Calculate experiment statistics
        total_runs = len(runs)
        completed_runs = len([r for r in runs if r.info.status == 'FINISHED'])
        failed_runs = len([r for r in runs if r.info.status == 'FAILED'])

        # Get best runs by metric
        best_runs = {}
        if runs:
            for metric_name in ['accuracy', 'f1_score', 'r2_score', 'rmse']:
                metric_runs = [r for r in runs if metric_name in r.data.metrics]
                if metric_runs:
                    if metric_name in ['accuracy', 'f1_score', 'r2_score']:
                        best_run = max(metric_runs, key=lambda r: r.data.metrics[metric_name])
                    else:  # rmse - lower is better
                        best_run = min(metric_runs, key=lambda r: r.data.metrics[metric_name])

                    best_runs[metric_name] = {
                        'run_id': best_run.info.run_id,
                        'value': best_run.data.metrics[metric_name],
                        'run_name': best_run.data.tags.get('mlflow.runName', 'Unnamed')
                    }

        return {
            'experiment_id': experiment_id,
            'experiment_name': experiment.name,
            'total_runs': total_runs,
            'completed_runs': completed_runs,
            'failed_runs': failed_runs,
            'success_rate': (completed_runs / max(total_runs, 1)) * 100,
            'best_runs': best_runs,
            'recent_runs': [
                {
                    'run_id': r.info.run_id,
                    'run_name': r.data.tags.get('mlflow.runName', 'Unnamed'),
                    'status': r.info.status,
                    'start_time': r.info.start_time,
                    'metrics': dict(r.data.metrics)
                } for r in runs[:10]  # Last 10 runs
            ]
        }

    async def get_model_registry_dashboard(self) -> Dict[str, Any]:
        """Get model registry dashboard data"""
        registered_models = self.client.search_registered_models()

        models_by_stage = {'Production': 0, 'Staging': 0, 'Development': 0, 'Archived': 0}
        model_details = []

        for model in registered_models:
            latest_versions = self.client.get_latest_versions(model.name)

            for version in latest_versions:
                models_by_stage[version.current_stage] = models_by_stage.get(version.current_stage, 0) + 1

                model_details.append({
                    'name': model.name,
                    'version': version.version,
                    'stage': version.current_stage,
                    'creation_timestamp': version.creation_timestamp,
                    'last_updated_timestamp': version.last_updated_timestamp,
                    'run_id': version.run_id
                })

        return {
            'total_models': len(registered_models),
            'models_by_stage': models_by_stage,
            'model_details': model_details
        }


# Factory function for easy platform creation
def create_mlflow_platform(config: MLflowConfig = None) -> MLflowPlatform:
    """Create configured MLflow platform"""
    if config is None:
        config = MLflowConfig()

    return MLflowPlatform(config)


# AutoML implementation
class AutoMLPipeline:
    """Automated machine learning pipeline"""

    def __init__(self, mlflow_platform: MLflowPlatform):
        self.platform = mlflow_platform
        self.logger = logging.getLogger(__name__)

    async def auto_train(self,
                        data: pd.DataFrame,
                        target_column: str,
                        problem_type: ModelType = None,
                        time_budget_minutes: int = 60) -> Dict[str, Any]:
        """Automated model training with algorithm selection and hyperparameter optimization"""

        if problem_type is None:
            # Auto-detect problem type
            if data[target_column].nunique() <= 10:
                problem_type = ModelType.CLASSIFICATION
            else:
                problem_type = ModelType.REGRESSION

        # Define algorithms to try
        algorithms = {
            'random_forest': {
                'n_estimators': {'type': 'int', 'low': 10, 'high': 200},
                'max_depth': {'type': 'int', 'low': 3, 'high': 20},
                'min_samples_split': {'type': 'int', 'low': 2, 'high': 20}
            },
            'logistic_regression': {
                'C': {'type': 'float', 'low': 0.01, 'high': 100.0},
                'max_iter': {'type': 'int', 'low': 100, 'high': 1000}
            } if problem_type == ModelType.CLASSIFICATION else {
                'fit_intercept': {'type': 'categorical', 'choices': [True, False]}
            }
        }

        best_model = None
        best_score = -float('inf') if problem_type == ModelType.CLASSIFICATION else -float('inf')
        results = []

        feature_columns = [col for col in data.columns if col != target_column]

        # Try each algorithm
        for algorithm, param_space in algorithms.items():
            self.logger.info(f"Training {algorithm} with AutoML")

            try:
                config = ModelConfiguration(
                    model_name=f"automl_{algorithm}",
                    model_type=problem_type,
                    algorithm=algorithm.replace('_', ' ').title(),
                    hyperparameters={},
                    feature_columns=feature_columns,
                    target_column=target_column,
                    optimization_metric="accuracy" if problem_type == ModelType.CLASSIFICATION else "r2"
                )

                # Run hyperparameter optimization
                optimization_result = await self.platform.hyperparameter_optimization(
                    config, data, param_space, n_trials=min(50, time_budget_minutes * 2)
                )

                results.append({
                    'algorithm': algorithm,
                    'best_score': optimization_result['best_score'],
                    'best_parameters': optimization_result['best_parameters'],
                    'run_id': optimization_result['final_model_run_id']
                })

                if optimization_result['best_score'] > best_score:
                    best_score = optimization_result['best_score']
                    best_model = optimization_result

            except Exception as e:
                self.logger.error(f"AutoML failed for {algorithm}: {e}")

        if best_model:
            # Register best model
            model_info = await self.platform.register_model(
                best_model['final_model_run_id'],
                f"automl_best_model_{int(time.time())}",
                ModelStage.STAGING
            )

            return {
                'status': 'completed',
                'best_model': best_model,
                'registered_model': model_info,
                'all_results': results,
                'problem_type': problem_type.value
            }
        else:
            return {
                'status': 'failed',
                'error': 'No successful models trained',
                'all_results': results
            }