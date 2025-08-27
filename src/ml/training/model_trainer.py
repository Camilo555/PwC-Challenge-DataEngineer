"""
Automated ML Model Training System

Comprehensive ML training pipeline with automated hyperparameter tuning,
model selection, and deployment capabilities.
"""

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
from src.ml.feature_engineering.feature_pipeline import FeatureEngineeringPipeline

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class ModelConfig:
    """Configuration for model training."""
    
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


@dataclass
class TrainingResult:
    """Result of model training."""
    
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
    """Automated ML training system."""
    
    def __init__(self, config: ModelConfig):
        self.config = config
        self.metrics_collector = MetricsCollector()
        self.trainers = self._initialize_trainers()
        self.mlflow_client = mlflow.tracking.MlflowClient()
        self._setup_mlflow()
        
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
    
    def train_models(self, X: pd.DataFrame, y: pd.Series, 
                    feature_pipeline: Optional[FeatureEngineeringPipeline] = None) -> List[TrainingResult]:
        """Train multiple models and return results."""
        start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting AutoML training with {len(X)} samples")
            
            # Feature engineering
            if feature_pipeline:
                logger.info("Applying feature engineering pipeline")
                X = feature_pipeline.fit_transform(X, y)
            
            # Split data
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
            
            results = []
            
            # Train each model type
            for model_type, trainer in self.trainers.items():
                logger.info(f"Training {model_type} model")
                
                try:
                    result = self._train_single_model(
                        trainer, X_train, y_train, X_val, y_val, X_test, y_test
                    )
                    results.append(result)
                    
                    logger.info(f"{model_type} training completed - Score: {result.best_score:.4f}")
                    
                except Exception as e:
                    logger.error(f"Error training {model_type}: {str(e)}")
                    continue
            
            # Sort results by score
            results.sort(key=lambda x: x.best_score, reverse=True)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"AutoML training completed in {duration:.2f} seconds")
            
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
            self.metrics_collector.increment_counter(
                "automl_training_errors_total"
            )
            raise
    
    def _train_single_model(self, trainer: BaseModelTrainer, 
                          X_train: pd.DataFrame, y_train: pd.Series,
                          X_val: pd.DataFrame, y_val: pd.Series,
                          X_test: pd.DataFrame, y_test: pd.Series) -> TrainingResult:
        """Train a single model with hyperparameter optimization."""
        model_start_time = datetime.utcnow()
        
        with mlflow.start_run(run_name=f"{trainer.model_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"):
            # Log parameters
            mlflow.log_param("model_type", trainer.model_type)
            mlflow.log_param("problem_type", self.config.problem_type)
            mlflow.log_param("train_size", len(X_train))
            mlflow.log_param("val_size", len(X_val))
            mlflow.log_param("test_size", len(X_test))
            
            best_score = -np.inf if self.config.problem_type == "regression" else 0.0
            best_params = {}
            best_model = None
            
            if self.config.enable_hyperparameter_tuning:
                # Hyperparameter optimization with Optuna
                study = optuna.create_study(
                    direction='maximize',
                    sampler=TPESampler(seed=self.config.random_state),
                    pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=10)
                )
                
                # Optimize
                study.optimize(
                    lambda trial: trainer.objective(trial, X_train, y_train, X_val, y_val),
                    n_trials=self.config.n_trials,
                    timeout=self.config.timeout_seconds
                )
                
                best_params = study.best_params
                best_score = study.best_value
                
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