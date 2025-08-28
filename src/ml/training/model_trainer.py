"""
Enhanced Automated ML Model Training System

Comprehensive ML training pipeline with automated hyperparameter tuning,
model selection, deployment capabilities, and enterprise infrastructure integration
including Redis caching, RabbitMQ messaging, and Kafka streaming.
"""

import asyncio
import hashlib
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pathlib import Path
import json
import joblib
import mlflow
import mlflow.sklearn
import optuna
from optuna.samplers import TPESampler
from optuna.pruners import MedianPruner

from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold, TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score,
    mean_squared_error, mean_absolute_error, r2_score
)

# ML Models
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression, ElasticNet
from sklearn.svm import SVC, SVR
from xgboost import XGBClassifier, XGBRegressor
from lightgbm import LGBMClassifier, LGBMRegressor
import catboost as cb

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.core.caching.redis_cache_manager import get_cache_manager
from src.messaging.enterprise_rabbitmq_manager import get_rabbitmq_manager, EnterpriseMessage, QueueType, MessagePriority
from src.streaming.kafka_manager import KafkaManager, StreamingTopic
from src.ml.feature_engineering.feature_pipeline import FeatureEngineeringPipeline

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class ModelConfig:
    """Enhanced configuration for model training with infrastructure integration."""
    
    # Model selection
    problem_type: str = "classification"  # "classification", "regression"
    model_types: List[str] = field(default_factory=lambda: ["random_forest", "xgboost", "lightgbm"])
    
    # Training parameters
    test_size: float = 0.2
    validation_size: float = 0.2
    random_state: int = 42
    cv_folds: int = 5
    
    # Hyperparameter tuning
    enable_hyperparameter_tuning: bool = True
    n_trials: int = 100
    timeout_seconds: Optional[int] = 3600  # 1 hour
    
    # Model selection criteria
    primary_metric: str = "f1"  # for classification: "accuracy", "f1", "roc_auc"
    # for regression: "rmse", "mae", "r2"
    
    # Early stopping
    enable_early_stopping: bool = True
    patience: int = 10
    
    # Output configuration
    save_models: bool = True
    model_registry_path: str = "models/registry"
    experiment_name: str = "ml_training"
    
    # Feature selection during training
    feature_selection_during_training: bool = True
    feature_importance_threshold: float = 0.001
    
    # Caching configuration
    enable_caching: bool = True
    cache_training_data: bool = True
    cache_intermediate_results: bool = True
    cache_ttl_seconds: int = 3600  # 1 hour
    
    # Messaging configuration
    enable_messaging: bool = True
    publish_training_progress: bool = True
    publish_model_metrics: bool = True
    
    # Streaming configuration
    enable_streaming: bool = True
    stream_training_metrics: bool = True
    stream_model_evaluation: bool = True
    
    # Distributed training configuration
    distributed_model_registry: bool = True
    model_registry_cache_ttl: int = 7200  # 2 hours


@dataclass
class TrainingResult:
    """Enhanced result of model training with infrastructure integration."""
    
    model_name: str
    model_type: str
    best_score: float
    best_params: Dict[str, Any]
    cv_scores: List[float]
    feature_importance: Optional[Dict[str, float]]
    training_time: float
    model_path: Optional[str] = None
    experiment_id: Optional[str] = None
    run_id: Optional[str] = None
    
    # Infrastructure integration
    cache_key: Optional[str] = None
    cached_at: Optional[datetime] = None
    message_published: bool = False
    metrics_streamed: bool = False
    
    # Distributed registry
    registry_id: Optional[str] = None
    registry_version: str = "1.0"
    
    def to_cache_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for caching."""
        return {
            "model_name": self.model_name,
            "model_type": self.model_type,
            "best_score": self.best_score,
            "best_params": self.best_params,
            "cv_scores": self.cv_scores,
            "feature_importance": self.feature_importance,
            "training_time": self.training_time,
            "model_path": self.model_path,
            "experiment_id": self.experiment_id,
            "run_id": self.run_id,
            "cache_key": self.cache_key,
            "cached_at": self.cached_at.isoformat() if self.cached_at else None,
            "message_published": self.message_published,
            "metrics_streamed": self.metrics_streamed,
            "registry_id": self.registry_id,
            "registry_version": self.registry_version
        }


class BaseModelTrainer(ABC):
    """Base class for model trainers."""
    
    def __init__(self, model_type: str, config: ModelConfig):
        self.model_type = model_type
        self.config = config
        self.model = None
        self.best_params = {}
        
    @abstractmethod
    def get_model(self, **params) -> Any:
        """Get model instance with parameters."""
        pass
        
    @abstractmethod
    def get_param_space(self) -> Dict[str, Any]:
        """Get hyperparameter search space."""
        pass
        
    def objective(self, trial: optuna.Trial, X_train: pd.DataFrame, y_train: pd.Series,
                 X_val: pd.DataFrame, y_val: pd.Series) -> float:
        """Objective function for hyperparameter optimization."""
        # Sample hyperparameters
        params = {}
        param_space = self.get_param_space()
        
        for param_name, param_config in param_space.items():
            if param_config['type'] == 'int':
                params[param_name] = trial.suggest_int(
                    param_name, param_config['low'], param_config['high']
                )
            elif param_config['type'] == 'float':
                params[param_name] = trial.suggest_float(
                    param_name, param_config['low'], param_config['high'],
                    log=param_config.get('log', False)
                )
            elif param_config['type'] == 'categorical':
                params[param_name] = trial.suggest_categorical(
                    param_name, param_config['choices']
                )
        
        # Train model
        model = self.get_model(**params)
        
        try:
            model.fit(X_train, y_train)
            y_pred = model.predict(X_val)
            
            # Calculate score based on problem type
            if self.config.problem_type == "classification":
                if self.config.primary_metric == "accuracy":
                    score = accuracy_score(y_val, y_pred)
                elif self.config.primary_metric == "f1":
                    score = f1_score(y_val, y_pred, average='weighted')
                elif self.config.primary_metric == "roc_auc":
                    if len(np.unique(y_val)) > 2:
                        score = roc_auc_score(y_val, model.predict_proba(X_val), multi_class='ovr')
                    else:
                        score = roc_auc_score(y_val, model.predict_proba(X_val)[:, 1])
                else:
                    score = f1_score(y_val, y_pred, average='weighted')
            else:  # regression
                if self.config.primary_metric == "rmse":
                    score = -np.sqrt(mean_squared_error(y_val, y_pred))
                elif self.config.primary_metric == "mae":
                    score = -mean_absolute_error(y_val, y_pred)
                elif self.config.primary_metric == "r2":
                    score = r2_score(y_val, y_pred)
                else:
                    score = -np.sqrt(mean_squared_error(y_val, y_pred))
            
            return score
            
        except Exception as e:
            logger.warning(f"Trial failed for {self.model_type}: {str(e)}")
            return -np.inf if self.config.problem_type == "regression" else 0.0


class RandomForestTrainer(BaseModelTrainer):
    """Random Forest model trainer."""
    
    def get_model(self, **params) -> Any:
        if self.config.problem_type == "classification":
            return RandomForestClassifier(random_state=self.config.random_state, **params)
        else:
            return RandomForestRegressor(random_state=self.config.random_state, **params)
    
    def get_param_space(self) -> Dict[str, Any]:
        return {
            'n_estimators': {'type': 'int', 'low': 50, 'high': 500},
            'max_depth': {'type': 'int', 'low': 3, 'high': 20},
            'min_samples_split': {'type': 'int', 'low': 2, 'high': 20},
            'min_samples_leaf': {'type': 'int', 'low': 1, 'high': 10},
            'max_features': {'type': 'categorical', 'choices': ['sqrt', 'log2', None]}
        }


class XGBoostTrainer(BaseModelTrainer):
    """XGBoost model trainer."""
    
    def get_model(self, **params) -> Any:
        if self.config.problem_type == "classification":
            return XGBClassifier(random_state=self.config.random_state, **params)
        else:
            return XGBRegressor(random_state=self.config.random_state, **params)
    
    def get_param_space(self) -> Dict[str, Any]:
        return {
            'n_estimators': {'type': 'int', 'low': 50, 'high': 500},
            'max_depth': {'type': 'int', 'low': 3, 'high': 10},
            'learning_rate': {'type': 'float', 'low': 0.01, 'high': 0.3, 'log': True},
            'subsample': {'type': 'float', 'low': 0.6, 'high': 1.0},
            'colsample_bytree': {'type': 'float', 'low': 0.6, 'high': 1.0},
            'reg_alpha': {'type': 'float', 'low': 1e-8, 'high': 10, 'log': True},
            'reg_lambda': {'type': 'float', 'low': 1e-8, 'high': 10, 'log': True}
        }


class LightGBMTrainer(BaseModelTrainer):
    """LightGBM model trainer."""
    
    def get_model(self, **params) -> Any:
        if self.config.problem_type == "classification":
            return LGBMClassifier(random_state=self.config.random_state, verbose=-1, **params)
        else:
            return LGBMRegressor(random_state=self.config.random_state, verbose=-1, **params)
    
    def get_param_space(self) -> Dict[str, Any]:
        return {
            'n_estimators': {'type': 'int', 'low': 50, 'high': 500},
            'max_depth': {'type': 'int', 'low': 3, 'high': 15},
            'learning_rate': {'type': 'float', 'low': 0.01, 'high': 0.3, 'log': True},
            'num_leaves': {'type': 'int', 'low': 10, 'high': 300},
            'subsample': {'type': 'float', 'low': 0.6, 'high': 1.0},
            'colsample_bytree': {'type': 'float', 'low': 0.6, 'high': 1.0},
            'reg_alpha': {'type': 'float', 'low': 1e-8, 'high': 10, 'log': True},
            'reg_lambda': {'type': 'float', 'low': 1e-8, 'high': 10, 'log': True}
        }


class CatBoostTrainer(BaseModelTrainer):
    """CatBoost model trainer."""
    
    def get_model(self, **params) -> Any:
        if self.config.problem_type == "classification":
            return cb.CatBoostClassifier(random_state=self.config.random_state, verbose=False, **params)
        else:
            return cb.CatBoostRegressor(random_state=self.config.random_state, verbose=False, **params)
    
    def get_param_space(self) -> Dict[str, Any]:
        return {
            'iterations': {'type': 'int', 'low': 50, 'high': 500},
            'depth': {'type': 'int', 'low': 3, 'high': 10},
            'learning_rate': {'type': 'float', 'low': 0.01, 'high': 0.3, 'log': True},
            'l2_leaf_reg': {'type': 'float', 'low': 1, 'high': 10},
            'subsample': {'type': 'float', 'low': 0.6, 'high': 1.0}
        }


class AutoMLTrainer:
    """Enhanced Automated ML training system with enterprise infrastructure integration."""
    
    def __init__(self, config: ModelConfig):
        self.config = config
        self.metrics_collector = MetricsCollector()
        self.trainers = self._initialize_trainers()
        self.mlflow_client = mlflow.tracking.MlflowClient()
        self._setup_mlflow()
        
        # Infrastructure integration
        self.cache_manager = None
        self.rabbitmq_manager = None
        self.kafka_manager = None
        
        # Initialize infrastructure if enabled
        if self.config.enable_caching or self.config.enable_messaging or self.config.enable_streaming:
            self._initialize_infrastructure()
    
    def _initialize_infrastructure(self):
        """Initialize infrastructure components asynchronously."""
        asyncio.create_task(self._async_initialize_infrastructure())
    
    async def _async_initialize_infrastructure(self):
        """Async initialization of infrastructure components."""
        try:
            # Initialize cache manager
            if self.config.enable_caching:
                self.cache_manager = await get_cache_manager()
                logger.info("Cache manager initialized for ML training")
            
            # Initialize messaging
            if self.config.enable_messaging:
                self.rabbitmq_manager = get_rabbitmq_manager()
                logger.info("RabbitMQ manager initialized for ML training")
            
            # Initialize streaming
            if self.config.enable_streaming:
                self.kafka_manager = KafkaManager()
                logger.info("Kafka manager initialized for ML training")
            
        except Exception as e:
            logger.error(f"Error initializing infrastructure: {e}")
        
    def _initialize_trainers(self) -> Dict[str, BaseModelTrainer]:
        """Initialize model trainers."""
        trainers = {}
        
        if "random_forest" in self.config.model_types:
            trainers["random_forest"] = RandomForestTrainer("random_forest", self.config)
        
        if "xgboost" in self.config.model_types:
            trainers["xgboost"] = XGBoostTrainer("xgboost", self.config)
        
        if "lightgbm" in self.config.model_types:
            trainers["lightgbm"] = LightGBMTrainer("lightgbm", self.config)
        
        if "catboost" in self.config.model_types:
            trainers["catboost"] = CatBoostTrainer("catboost", self.config)
        
        return trainers
    
    def _setup_mlflow(self):
        """Setup MLflow experiment tracking."""
        try:
            mlflow.set_experiment(self.config.experiment_name)
            logger.info(f"MLflow experiment set to: {self.config.experiment_name}")
        except Exception as e:
            logger.error(f"Error setting up MLflow: {str(e)}")
    
    async def _cache_training_data(self, X: pd.DataFrame, y: pd.Series) -> Optional[str]:
        """Cache training data for reuse."""
        if not self.config.enable_caching or not self.cache_manager:
            return None
        
        try:
            # Create cache key from data hash
            data_hash = hashlib.sha256(str(X.values.tobytes() + y.values.tobytes()).encode()).hexdigest()
            cache_key = f"training_data:{data_hash}"
            
            # Cache the data
            await self.cache_manager.set(
                cache_key,
                {
                    "X": X.to_dict(),
                    "y": y.to_dict(),
                    "cached_at": datetime.utcnow().isoformat(),
                    "shape": X.shape,
                    "columns": X.columns.tolist()
                },
                ttl=self.config.cache_ttl_seconds,
                namespace="ml_training"
            )
            
            logger.info(f"Training data cached with key: {cache_key}")
            return cache_key
            
        except Exception as e:
            logger.error(f"Error caching training data: {e}")
            return None
    
    async def _get_cached_training_data(self, cache_key: str) -> Optional[Tuple[pd.DataFrame, pd.Series]]:
        """Retrieve cached training data."""
        if not self.config.enable_caching or not self.cache_manager or not cache_key:
            return None
        
        try:
            cached_data = await self.cache_manager.get(cache_key, namespace="ml_training")
            if cached_data:
                X = pd.DataFrame.from_dict(cached_data["X"])
                y = pd.Series(cached_data["y"])
                logger.info(f"Retrieved cached training data: {cache_key}")
                return X, y
        except Exception as e:
            logger.error(f"Error retrieving cached training data: {e}")
        
        return None
    
    async def _cache_model_result(self, result: TrainingResult):
        """Cache model training result."""
        if not self.config.enable_caching or not self.cache_manager:
            return
        
        try:
            cache_key = f"model_result:{result.model_name}"
            result.cache_key = cache_key
            result.cached_at = datetime.utcnow()
            
            await self.cache_manager.set(
                cache_key,
                result.to_cache_dict(),
                ttl=self.config.model_registry_cache_ttl,
                namespace="ml_models"
            )
            
            logger.info(f"Model result cached: {cache_key}")
            
        except Exception as e:
            logger.error(f"Error caching model result: {e}")
    
    async def _publish_training_progress(self, job_id: str, stage: str, progress: Dict[str, Any]):
        """Publish training progress to RabbitMQ."""
        if not self.config.enable_messaging or not self.rabbitmq_manager:
            return
        
        try:
            message = EnterpriseMessage(
                queue_type=QueueType.ML_TRAINING,
                message_type="training_progress",
                payload={
                    "job_id": job_id,
                    "stage": stage,
                    "progress": progress,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            message.metadata.priority = MessagePriority.NORMAL
            message.metadata.source_service = "ml_trainer"
            
            await self.rabbitmq_manager.publish_message_async(message)
            logger.debug(f"Training progress published for job {job_id}")
            
        except Exception as e:
            logger.error(f"Error publishing training progress: {e}")
    
    async def _publish_training_completion(self, result: TrainingResult):
        """Publish training completion to RabbitMQ."""
        if not self.config.enable_messaging or not self.rabbitmq_manager:
            return
        
        try:
            message = EnterpriseMessage(
                queue_type=QueueType.ML_TRAINING,
                message_type="training_completed",
                payload={
                    "model_name": result.model_name,
                    "model_type": result.model_type,
                    "best_score": result.best_score,
                    "training_time": result.training_time,
                    "experiment_id": result.experiment_id,
                    "run_id": result.run_id,
                    "model_path": result.model_path,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            message.metadata.priority = MessagePriority.HIGH
            message.metadata.source_service = "ml_trainer"
            
            success = await self.rabbitmq_manager.publish_message_async(message)
            if success:
                result.message_published = True
                logger.info(f"Training completion published for model: {result.model_name}")
            
        except Exception as e:
            logger.error(f"Error publishing training completion: {e}")
    
    async def _stream_training_metrics(self, job_id: str, model_type: str, metrics: Dict[str, Any]):
        """Stream training metrics to Kafka."""
        if not self.config.enable_streaming or not self.kafka_manager:
            return
        
        try:
            success = self.kafka_manager.produce_message(
                topic=StreamingTopic.METRICS,
                message={
                    "job_id": job_id,
                    "model_type": model_type,
                    "metrics": metrics,
                    "event_type": "training_metrics",
                    "timestamp": datetime.utcnow().isoformat()
                },
                key=job_id
            )
            
            if success:
                logger.debug(f"Training metrics streamed for job {job_id}")
            
        except Exception as e:
            logger.error(f"Error streaming training metrics: {e}")
    
    async def _stream_model_evaluation(self, result: TrainingResult):
        """Stream model evaluation results to Kafka."""
        if not self.config.enable_streaming or not self.kafka_manager:
            return
        
        try:
            success = self.kafka_manager.produce_message(
                topic=StreamingTopic.METRICS,
                message={
                    "model_name": result.model_name,
                    "model_type": result.model_type,
                    "best_score": result.best_score,
                    "cv_scores": result.cv_scores,
                    "feature_importance": result.feature_importance,
                    "training_time": result.training_time,
                    "experiment_id": result.experiment_id,
                    "run_id": result.run_id,
                    "event_type": "model_evaluation",
                    "timestamp": datetime.utcnow().isoformat()
                },
                key=result.model_name
            )
            
            if success:
                result.metrics_streamed = True
                logger.info(f"Model evaluation streamed for: {result.model_name}")
            
        except Exception as e:
            logger.error(f"Error streaming model evaluation: {e}")
    
    async def _register_model_distributed(self, result: TrainingResult):
        """Register model in distributed model registry using cache."""
        if not self.config.distributed_model_registry or not self.cache_manager:
            return
        
        try:
            registry_id = f"model_registry:{result.model_name}"
            result.registry_id = registry_id
            
            registry_entry = {
                "model_name": result.model_name,
                "model_type": result.model_type,
                "version": result.registry_version,
                "best_score": result.best_score,
                "model_path": result.model_path,
                "experiment_id": result.experiment_id,
                "run_id": result.run_id,
                "registered_at": datetime.utcnow().isoformat(),
                "status": "registered"
            }
            
            await self.cache_manager.hset(
                "model_registry",
                result.model_name,
                registry_entry,
                namespace="ml_registry"
            )
            
            logger.info(f"Model registered in distributed registry: {registry_id}")
            
        except Exception as e:
            logger.error(f"Error registering model in distributed registry: {e}")
    
    def train_models(self, X: pd.DataFrame, y: pd.Series, 
                    feature_pipeline: Optional[FeatureEngineeringPipeline] = None) -> List[TrainingResult]:
        """Train multiple models and return results with infrastructure integration."""
        return asyncio.run(self._train_models_async(X, y, feature_pipeline))
    
    async def _train_models_async(self, X: pd.DataFrame, y: pd.Series, 
                                 feature_pipeline: Optional[FeatureEngineeringPipeline] = None) -> List[TrainingResult]:
        """Async training with infrastructure integration."""
        start_time = datetime.utcnow()
        job_id = f"training_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{len(X)}"
        
        try:
            logger.info(f"Starting AutoML training with {len(X)} samples - Job ID: {job_id}")
            
            # Publish training start
            await self._publish_training_progress(job_id, "started", {
                "total_samples": len(X),
                "features": len(X.columns),
                "models_to_train": len(self.trainers)
            })
            
            # Cache training data if enabled
            cache_key = await self._cache_training_data(X, y) if self.config.cache_training_data else None
            
            # Feature engineering
            if feature_pipeline:
                logger.info("Applying feature engineering pipeline")
                await self._publish_training_progress(job_id, "feature_engineering", {"status": "started"})
                X = feature_pipeline.fit_transform(X, y)
                await self._publish_training_progress(job_id, "feature_engineering", {"status": "completed"})
            
            # Split data
            await self._publish_training_progress(job_id, "data_splitting", {"status": "started"})
            
            X_temp, X_test, y_temp, y_test = train_test_split(
                X, y, test_size=self.config.test_size, 
                random_state=self.config.random_state,
                stratify=y if self.config.problem_type == "classification" else None
            )
            
            X_train, X_val, y_train, y_val = train_test_split(
                X_temp, y_temp, test_size=self.config.validation_size,
                random_state=self.config.random_state,
                stratify=y_temp if self.config.problem_type == "classification" else None
            )
            
            logger.info(f"Data split - Train: {len(X_train)}, Val: {len(X_val)}, Test: {len(X_test)}")
            
            await self._publish_training_progress(job_id, "data_splitting", {
                "status": "completed",
                "train_size": len(X_train),
                "val_size": len(X_val),
                "test_size": len(X_test)
            })
            
            results = []
            
            # Train each model type
            for i, (model_type, trainer) in enumerate(self.trainers.items()):
                logger.info(f"Training {model_type} model ({i+1}/{len(self.trainers)})")
                
                await self._publish_training_progress(job_id, f"model_training_{model_type}", {
                    "status": "started",
                    "model_index": i + 1,
                    "total_models": len(self.trainers)
                })
                
                try:
                    result = await self._train_single_model_async(
                        job_id, trainer, X_train, y_train, X_val, y_val, X_test, y_test
                    )
                    results.append(result)
                    
                    # Cache model result
                    await self._cache_model_result(result)
                    
                    # Publish training completion for this model
                    await self._publish_training_completion(result)
                    
                    # Stream model evaluation
                    await self._stream_model_evaluation(result)
                    
                    # Register in distributed model registry
                    await self._register_model_distributed(result)
                    
                    logger.info(f"{model_type} training completed - Score: {result.best_score:.4f}")
                    
                    await self._publish_training_progress(job_id, f"model_training_{model_type}", {
                        "status": "completed",
                        "score": result.best_score,
                        "training_time": result.training_time
                    })
                    
                except Exception as e:
                    logger.error(f"Error training {model_type}: {str(e)}")
                    
                    await self._publish_training_progress(job_id, f"model_training_{model_type}", {
                        "status": "failed",
                        "error": str(e)
                    })
                    continue
            
            # Sort results by score
            results.sort(key=lambda x: x.best_score, reverse=True)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"AutoML training completed in {duration:.2f} seconds")
            
            # Final progress update
            await self._publish_training_progress(job_id, "completed", {
                "duration_seconds": duration,
                "models_trained": len(results),
                "best_score": results[0].best_score if results else None,
                "best_model": results[0].model_type if results else None
            })
            
            # Stream final metrics
            if results:
                await self._stream_training_metrics(job_id, "automl", {
                    "total_duration": duration,
                    "models_trained": len(results),
                    "best_score": results[0].best_score,
                    "best_model": results[0].model_type,
                    "scores": [r.best_score for r in results]
                })
            
            # Log metrics
            self.metrics_collector.increment_counter(
                "automl_training_completed_total",
                tags={"models_trained": str(len(results))}
            )
            self.metrics_collector.record_histogram(
                "automl_training_duration_seconds",
                duration
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error in AutoML training: {str(e)}")
            
            # Publish error
            await self._publish_training_progress(job_id, "failed", {
                "error": str(e),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
            })
            
            self.metrics_collector.increment_counter(
                "automl_training_errors_total"
            )
            raise
    
    async def _train_single_model_async(self, job_id: str, trainer: BaseModelTrainer, 
                                       X_train: pd.DataFrame, y_train: pd.Series,
                                       X_val: pd.DataFrame, y_val: pd.Series,
                                       X_test: pd.DataFrame, y_test: pd.Series) -> TrainingResult:
        """Train a single model with hyperparameter optimization and infrastructure integration."""
        model_start_time = datetime.utcnow()
        model_job_id = f"{job_id}_{trainer.model_type}"
        
        with mlflow.start_run(run_name=f"{trainer.model_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"):
            # Log parameters
            mlflow.log_param("model_type", trainer.model_type)
            mlflow.log_param("problem_type", self.config.problem_type)
            mlflow.log_param("train_size", len(X_train))
            mlflow.log_param("val_size", len(X_val))
            mlflow.log_param("test_size", len(X_test))
            mlflow.log_param("job_id", job_id)
            
            best_score = -np.inf if self.config.problem_type == "regression" else 0.0
            best_params = {}
            best_model = None
            
            if self.config.enable_hyperparameter_tuning:
                # Stream hyperparameter tuning start
                await self._stream_training_metrics(model_job_id, trainer.model_type, {
                    "stage": "hyperparameter_tuning_started",
                    "n_trials": self.config.n_trials,
                    "timeout_seconds": self.config.timeout_seconds
                })
                
                # Hyperparameter optimization with Optuna
                study = optuna.create_study(
                    direction='maximize',
                    sampler=TPESampler(seed=self.config.random_state),
                    pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=10)
                )
                
                # Create callback to stream progress
                def optuna_callback(study, trial):
                    if trial.number % 10 == 0:  # Stream every 10 trials
                        asyncio.create_task(self._stream_training_metrics(model_job_id, trainer.model_type, {
                            "stage": "hyperparameter_tuning_progress",
                            "trial_number": trial.number,
                            "current_best_score": study.best_value if study.best_value else None,
                            "current_best_params": study.best_params if study.best_params else None
                        }))
                
                # Optimize
                study.optimize(
                    lambda trial: trainer.objective(trial, X_train, y_train, X_val, y_val),
                    n_trials=self.config.n_trials,
                    timeout=self.config.timeout_seconds,
                    callbacks=[optuna_callback]
                )
                
                best_params = study.best_params
                best_score = study.best_value
                
                # Stream hyperparameter tuning completion
                await self._stream_training_metrics(model_job_id, trainer.model_type, {
                    "stage": "hyperparameter_tuning_completed",
                    "best_score": best_score,
                    "best_params": best_params,
                    "total_trials": len(study.trials)
                })
                
                # Train best model
                best_model = trainer.get_model(**best_params)
                best_model.fit(X_train, y_train)
                
                # Log hyperparameters
                mlflow.log_params(best_params)
                
            else:
                # Train with default parameters
                best_model = trainer.get_model()
                best_model.fit(X_train, y_train)
                
                # Evaluate on validation set
                y_val_pred = best_model.predict(X_val)
                best_score = self._calculate_score(y_val, y_val_pred)
            
            # Cross-validation scores
            cv_scores = []
            if self.config.cv_folds > 1:
                cv_splitter = StratifiedKFold(n_splits=self.config.cv_folds, shuffle=True, 
                                            random_state=self.config.random_state)
                if self.config.problem_type == "regression":
                    cv_splitter = TimeSeriesSplit(n_splits=self.config.cv_folds)
                
                scoring = self.config.primary_metric
                if scoring == "rmse":
                    scoring = "neg_mean_squared_error"
                elif scoring == "mae":
                    scoring = "neg_mean_absolute_error"
                
                cv_scores = cross_val_score(
                    best_model, X_train, y_train, 
                    cv=cv_splitter, scoring=scoring, n_jobs=-1
                )
                cv_scores = cv_scores.tolist()
            
            # Test set evaluation
            y_test_pred = best_model.predict(X_test)
            test_score = self._calculate_score(y_test, y_test_pred)
            
            # Feature importance
            feature_importance = None
            if hasattr(best_model, 'feature_importances_'):
                feature_importance = dict(zip(X_train.columns, best_model.feature_importances_))
            elif hasattr(best_model, 'coef_'):
                feature_importance = dict(zip(X_train.columns, np.abs(best_model.coef_.flatten())))
            
            # Log metrics
            mlflow.log_metric("validation_score", best_score)
            mlflow.log_metric("test_score", test_score)
            if cv_scores:
                mlflow.log_metric("cv_mean_score", np.mean(cv_scores))
                mlflow.log_metric("cv_std_score", np.std(cv_scores))
            
            # Additional metrics for classification
            if self.config.problem_type == "classification":
                accuracy = accuracy_score(y_test, y_test_pred)
                precision = precision_score(y_test, y_test_pred, average='weighted')
                recall = recall_score(y_test, y_test_pred, average='weighted')
                f1 = f1_score(y_test, y_test_pred, average='weighted')
                
                mlflow.log_metric("test_accuracy", accuracy)
                mlflow.log_metric("test_precision", precision)
                mlflow.log_metric("test_recall", recall)
                mlflow.log_metric("test_f1", f1)
            
            # Additional metrics for regression
            if self.config.problem_type == "regression":
                rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
                mae = mean_absolute_error(y_test, y_test_pred)
                r2 = r2_score(y_test, y_test_pred)
                
                mlflow.log_metric("test_rmse", rmse)
                mlflow.log_metric("test_mae", mae)
                mlflow.log_metric("test_r2", r2)
            
            # Save model
            model_path = None
            if self.config.save_models:
                model_path = self._save_model(best_model, trainer.model_type, best_params)
                mlflow.log_artifact(model_path)
            
            training_time = (datetime.utcnow() - model_start_time).total_seconds()
            mlflow.log_metric("training_time", training_time)
            
            # Get MLflow run info
            run = mlflow.active_run()
            experiment_id = run.info.experiment_id
            run_id = run.info.run_id
            
            return TrainingResult(
                model_name=f"{trainer.model_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                model_type=trainer.model_type,
                best_score=best_score,
                best_params=best_params,
                cv_scores=cv_scores,
                feature_importance=feature_importance,
                training_time=training_time,
                model_path=model_path,
                experiment_id=experiment_id,
                run_id=run_id
            )
    
    def _calculate_score(self, y_true: pd.Series, y_pred: np.ndarray) -> float:
        """Calculate score based on problem type and metric."""
        if self.config.problem_type == "classification":
            if self.config.primary_metric == "accuracy":
                return accuracy_score(y_true, y_pred)
            elif self.config.primary_metric == "f1":
                return f1_score(y_true, y_pred, average='weighted')
            elif self.config.primary_metric == "roc_auc":
                # This would need probability predictions
                return f1_score(y_true, y_pred, average='weighted')
            else:
                return f1_score(y_true, y_pred, average='weighted')
        else:  # regression
            if self.config.primary_metric == "rmse":
                return -np.sqrt(mean_squared_error(y_true, y_pred))
            elif self.config.primary_metric == "mae":
                return -mean_absolute_error(y_true, y_pred)
            elif self.config.primary_metric == "r2":
                return r2_score(y_true, y_pred)
            else:
                return -np.sqrt(mean_squared_error(y_true, y_pred))
    
    def _save_model(self, model: Any, model_type: str, params: Dict[str, Any]) -> str:
        """Save trained model to disk."""
        model_dir = Path(self.config.model_registry_path)
        model_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        model_filename = f"{model_type}_{timestamp}.pkl"
        model_path = model_dir / model_filename
        
        # Save model
        joblib.dump(model, model_path)
        
        # Save metadata
        metadata = {
            "model_type": model_type,
            "timestamp": timestamp,
            "parameters": params,
            "config": {
                "problem_type": self.config.problem_type,
                "primary_metric": self.config.primary_metric
            }
        }
        
        metadata_path = model_dir / f"{model_type}_{timestamp}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Model saved to {model_path}")
        return str(model_path)


def create_sales_prediction_trainer() -> AutoMLTrainer:
    """Create AutoML trainer optimized for sales prediction."""
    config = ModelConfig(
        problem_type="regression",
        model_types=["random_forest", "xgboost", "lightgbm", "catboost"],
        primary_metric="rmse",
        n_trials=50,
        cv_folds=5,
        experiment_name="sales_prediction"
    )
    
    return AutoMLTrainer(config)


def create_customer_segmentation_trainer() -> AutoMLTrainer:
    """Create AutoML trainer optimized for customer segmentation."""
    config = ModelConfig(
        problem_type="classification",
        model_types=["random_forest", "xgboost", "lightgbm"],
        primary_metric="f1",
        n_trials=50,
        cv_folds=5,
        experiment_name="customer_segmentation"
    )
    
    return AutoMLTrainer(config)