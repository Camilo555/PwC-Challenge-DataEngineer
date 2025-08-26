"""
ML Model Management and MLOps Infrastructure
Enterprise-grade model lifecycle management with versioning, deployment, and monitoring
"""

import hashlib
import json
import pickle
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import logging

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from .base import BaseMLComponent, MLConfig, ModelMetrics, MLExperiment


class ModelStatus(str, Enum):
    """Model status enumeration"""
    TRAINING = "training"
    TRAINED = "trained"
    VALIDATING = "validating"
    VALIDATED = "validated"
    STAGING = "staging"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class ModelType(str, Enum):
    """Model type enumeration"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    ANOMALY_DETECTION = "anomaly_detection"
    RECOMMENDATION = "recommendation"
    TIME_SERIES = "time_series"
    NLP = "nlp"
    COMPUTER_VISION = "computer_vision"


class ModelMetadata(BaseModel):
    """Comprehensive model metadata"""
    model_id: str = Field(..., description="Unique model identifier")
    model_name: str = Field(..., description="Human-readable model name")
    model_type: ModelType = Field(..., description="Type of ML model")
    version: str = Field(..., description="Model version")
    status: ModelStatus = Field(default=ModelStatus.TRAINING, description="Current model status")
    
    # Model information
    algorithm: str = Field(..., description="ML algorithm used")
    framework: str = Field(default="scikit-learn", description="ML framework")
    feature_count: int = Field(..., description="Number of features")
    training_samples: int = Field(..., description="Number of training samples")
    
    # Performance metrics
    metrics: ModelMetrics = Field(..., description="Model performance metrics")
    validation_metrics: Optional[ModelMetrics] = Field(default=None, description="Validation metrics")
    
    # Lifecycle information
    created_at: datetime = Field(default_factory=datetime.utcnow)
    trained_at: Optional[datetime] = Field(default=None)
    deployed_at: Optional[datetime] = Field(default=None)
    last_used: Optional[datetime] = Field(default=None)
    
    # Configuration
    hyperparameters: Dict[str, Any] = Field(default_factory=dict)
    feature_names: List[str] = Field(default_factory=list)
    target_names: Optional[List[str]] = Field(default=None)
    
    # Governance
    owner: str = Field(default="system")
    description: str = Field(default="")
    tags: List[str] = Field(default_factory=list)
    environment: str = Field(default="development")
    
    # Performance tracking
    prediction_count: int = Field(default=0)
    average_prediction_time: Optional[float] = Field(default=None)
    model_size_mb: Optional[float] = Field(default=None)
    
    class Config:
        arbitrary_types_allowed = True


class ModelRegistry:
    """Model registry for tracking and managing ML models"""
    
    def __init__(self, registry_path: str = "models/registry/"):
        self.registry_path = Path(registry_path)
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.registry_file = self.registry_path / "model_registry.json"
        self.models: Dict[str, ModelMetadata] = {}
        self.load_registry()
        
    def load_registry(self) -> None:
        """Load model registry from disk"""
        if self.registry_file.exists():
            try:
                with open(self.registry_file, 'r') as f:
                    registry_data = json.load(f)
                
                self.models = {
                    model_id: ModelMetadata(**metadata)
                    for model_id, metadata in registry_data.items()
                }
            except Exception as e:
                logging.warning(f"Failed to load model registry: {e}")
                self.models = {}
    
    def save_registry(self) -> None:
        """Save model registry to disk"""
        try:
            registry_data = {
                model_id: metadata.dict()
                for model_id, metadata in self.models.items()
            }
            
            with open(self.registry_file, 'w') as f:
                json.dump(registry_data, f, indent=2, default=str)
                
        except Exception as e:
            logging.error(f"Failed to save model registry: {e}")
    
    def register_model(self, metadata: ModelMetadata) -> str:
        """Register a new model"""
        self.models[metadata.model_id] = metadata
        self.save_registry()
        return metadata.model_id
    
    def update_model(self, model_id: str, **updates) -> None:
        """Update model metadata"""
        if model_id in self.models:
            for key, value in updates.items():
                if hasattr(self.models[model_id], key):
                    setattr(self.models[model_id], key, value)
            
            self.save_registry()
    
    def get_model(self, model_id: str) -> Optional[ModelMetadata]:
        """Get model metadata by ID"""
        return self.models.get(model_id)
    
    def list_models(self, 
                   status: Optional[ModelStatus] = None,
                   model_type: Optional[ModelType] = None,
                   environment: Optional[str] = None) -> List[ModelMetadata]:
        """List models with optional filtering"""
        models = list(self.models.values())
        
        if status:
            models = [m for m in models if m.status == status]
        
        if model_type:
            models = [m for m in models if m.model_type == model_type]
        
        if environment:
            models = [m for m in models if m.environment == environment]
        
        return models
    
    def delete_model(self, model_id: str) -> bool:
        """Delete model from registry"""
        if model_id in self.models:
            del self.models[model_id]
            self.save_registry()
            return True
        return False
    
    def get_model_lineage(self, model_id: str) -> Dict[str, Any]:
        """Get model lineage information"""
        if model_id not in self.models:
            return {}
        
        model = self.models[model_id]
        
        # Find related models (same name, different versions)
        related_models = [
            m for m in self.models.values()
            if m.model_name == model.model_name and m.model_id != model_id
        ]
        
        return {
            "model_id": model_id,
            "model_name": model.model_name,
            "current_version": model.version,
            "related_versions": [
                {
                    "model_id": m.model_id,
                    "version": m.version,
                    "status": m.status,
                    "created_at": m.created_at
                }
                for m in sorted(related_models, key=lambda x: x.created_at, reverse=True)
            ]
        }


class ModelManager(BaseMLComponent):
    """Enterprise model management with MLOps capabilities"""
    
    def __init__(self, 
                 storage_path: str = "models/",
                 registry_path: str = "models/registry/",
                 config: Optional[MLConfig] = None):
        super().__init__(config)
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self.registry = ModelRegistry(registry_path)
        self.experiments: Dict[str, MLExperiment] = {}
        self.deployed_models: Dict[str, Any] = {}
        
    def create_experiment(self, experiment_name: str) -> MLExperiment:
        """Create a new ML experiment"""
        experiment = MLExperiment(experiment_name)
        self.experiments[experiment_name] = experiment
        return experiment
    
    def register_model(self,
                      model: Any,
                      model_name: str,
                      model_type: ModelType,
                      algorithm: str,
                      metrics: ModelMetrics,
                      feature_names: List[str],
                      hyperparameters: Optional[Dict[str, Any]] = None,
                      description: str = "",
                      tags: Optional[List[str]] = None,
                      environment: str = "development") -> str:
        """Register a trained model"""
        
        # Generate unique model ID
        model_id = self._generate_model_id(model_name, algorithm)
        
        # Save model to disk
        model_path = self.storage_path / f"{model_id}.pkl"
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        # Calculate model size
        model_size_mb = model_path.stat().st_size / (1024 * 1024)
        
        # Create model metadata
        metadata = ModelMetadata(
            model_id=model_id,
            model_name=model_name,
            model_type=model_type,
            version=self._generate_version(model_name),
            algorithm=algorithm,
            feature_count=len(feature_names),
            training_samples=getattr(model, 'n_samples_', 0),
            metrics=metrics,
            hyperparameters=hyperparameters or {},
            feature_names=feature_names,
            description=description,
            tags=tags or [],
            environment=environment,
            model_size_mb=model_size_mb,
            trained_at=datetime.utcnow(),
            status=ModelStatus.TRAINED
        )
        
        # Register in registry
        self.registry.register_model(metadata)
        
        self.logger.info(f"Registered model {model_name} with ID {model_id}")
        return model_id
    
    def load_model(self, model_id: str) -> Any:
        """Load model from disk"""
        model_path = self.storage_path / f"{model_id}.pkl"
        
        if not model_path.exists():
            raise FileNotFoundError(f"Model {model_id} not found")
        
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        # Update last used timestamp
        self.registry.update_model(model_id, last_used=datetime.utcnow())
        
        return model
    
    def deploy_model(self, model_id: str, environment: str = "production") -> bool:
        """Deploy model to specified environment"""
        try:
            # Load model
            model = self.load_model(model_id)
            
            # Store in deployed models cache
            self.deployed_models[model_id] = model
            
            # Update model status
            self.registry.update_model(
                model_id,
                status=ModelStatus.PRODUCTION if environment == "production" else ModelStatus.STAGING,
                deployed_at=datetime.utcnow(),
                environment=environment
            )
            
            self.logger.info(f"Deployed model {model_id} to {environment}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to deploy model {model_id}: {e}")
            return False
    
    def predict(self, model_id: str, data: Union[pd.DataFrame, np.ndarray]) -> np.ndarray:
        """Make predictions using deployed model"""
        start_time = datetime.utcnow()
        
        # Get model (load if not in cache)
        if model_id not in self.deployed_models:
            if not self.deploy_model(model_id):
                raise RuntimeError(f"Failed to deploy model {model_id}")
        
        model = self.deployed_models[model_id]
        
        # Make prediction
        predictions = model.predict(data)
        
        # Update prediction metrics
        prediction_time = (datetime.utcnow() - start_time).total_seconds()
        metadata = self.registry.get_model(model_id)
        
        if metadata:
            # Update prediction count and average time
            new_count = metadata.prediction_count + 1
            if metadata.average_prediction_time:
                new_avg_time = ((metadata.average_prediction_time * metadata.prediction_count) + prediction_time) / new_count
            else:
                new_avg_time = prediction_time
            
            self.registry.update_model(
                model_id,
                prediction_count=new_count,
                average_prediction_time=new_avg_time,
                last_used=datetime.utcnow()
            )
        
        return predictions
    
    def validate_model(self, 
                      model_id: str, 
                      validation_data: pd.DataFrame,
                      validation_target: pd.Series) -> ModelMetrics:
        """Validate model on new data"""
        model = self.load_model(model_id)
        predictions = model.predict(validation_data)
        
        # Calculate validation metrics
        validation_metrics = self._calculate_metrics(
            validation_target, 
            predictions,
            self.registry.get_model(model_id).model_type
        )
        
        # Update model with validation metrics
        self.registry.update_model(
            model_id,
            validation_metrics=validation_metrics,
            status=ModelStatus.VALIDATED
        )
        
        return validation_metrics
    
    def compare_models(self, model_ids: List[str], metric: str = "accuracy") -> Dict[str, Any]:
        """Compare multiple models by specified metric"""
        comparison_data = []
        
        for model_id in model_ids:
            metadata = self.registry.get_model(model_id)
            if metadata:
                metric_value = getattr(metadata.metrics, metric, None)
                comparison_data.append({
                    "model_id": model_id,
                    "model_name": metadata.model_name,
                    "version": metadata.version,
                    "algorithm": metadata.algorithm,
                    "metric_value": metric_value,
                    "created_at": metadata.created_at
                })
        
        # Sort by metric value
        comparison_data.sort(
            key=lambda x: x["metric_value"] if x["metric_value"] is not None else float('-inf'),
            reverse=True
        )
        
        return {
            "comparison_metric": metric,
            "model_count": len(comparison_data),
            "best_model": comparison_data[0] if comparison_data else None,
            "models": comparison_data
        }
    
    def archive_model(self, model_id: str) -> bool:
        """Archive old model version"""
        try:
            # Update status to archived
            self.registry.update_model(model_id, status=ModelStatus.ARCHIVED)
            
            # Remove from deployed models cache
            if model_id in self.deployed_models:
                del self.deployed_models[model_id]
            
            # Move model file to archive directory
            archive_dir = self.storage_path / "archived"
            archive_dir.mkdir(exist_ok=True)
            
            model_path = self.storage_path / f"{model_id}.pkl"
            archive_path = archive_dir / f"{model_id}.pkl"
            
            if model_path.exists():
                model_path.rename(archive_path)
            
            self.logger.info(f"Archived model {model_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to archive model {model_id}: {e}")
            return False
    
    def get_model_performance_report(self, model_id: str) -> Dict[str, Any]:
        """Generate comprehensive model performance report"""
        metadata = self.registry.get_model(model_id)
        if not metadata:
            return {"error": "Model not found"}
        
        lineage = self.registry.get_model_lineage(model_id)
        
        return {
            "model_info": {
                "model_id": model_id,
                "name": metadata.model_name,
                "version": metadata.version,
                "type": metadata.model_type,
                "algorithm": metadata.algorithm,
                "status": metadata.status
            },
            "performance": {
                "training_metrics": metadata.metrics.to_dict(),
                "validation_metrics": metadata.validation_metrics.to_dict() if metadata.validation_metrics else None,
                "prediction_count": metadata.prediction_count,
                "average_prediction_time": metadata.average_prediction_time
            },
            "resources": {
                "feature_count": metadata.feature_count,
                "training_samples": metadata.training_samples,
                "model_size_mb": metadata.model_size_mb,
                "memory_usage": metadata.metrics.memory_usage
            },
            "lifecycle": {
                "created_at": metadata.created_at,
                "trained_at": metadata.trained_at,
                "deployed_at": metadata.deployed_at,
                "last_used": metadata.last_used,
                "environment": metadata.environment
            },
            "lineage": lineage,
            "configuration": {
                "hyperparameters": metadata.hyperparameters,
                "features": metadata.feature_names[:10] if len(metadata.feature_names) > 10 else metadata.feature_names
            }
        }
    
    def _generate_model_id(self, model_name: str, algorithm: str) -> str:
        """Generate unique model ID"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        hash_input = f"{model_name}_{algorithm}_{timestamp}"
        model_hash = hashlib.md5(hash_input.encode()).hexdigest()[:8]
        return f"{model_name}_{algorithm}_{timestamp}_{model_hash}"
    
    def _generate_version(self, model_name: str) -> str:
        """Generate model version"""
        existing_models = [m for m in self.registry.list_models() if m.model_name == model_name]
        version_number = len(existing_models) + 1
        return f"v{version_number}.0.0"
    
    def _calculate_metrics(self, y_true, y_pred, model_type: ModelType) -> ModelMetrics:
        """Calculate performance metrics based on model type"""
        try:
            from sklearn.metrics import (
                accuracy_score, precision_score, recall_score, f1_score,
                mean_squared_error, mean_absolute_error, r2_score,
                roc_auc_score
            )
        except ImportError:
            # Return basic metrics if sklearn not available
            return ModelMetrics()
        
        metrics = ModelMetrics()
        
        if model_type == ModelType.CLASSIFICATION:
            if len(np.unique(y_true)) == 2:  # Binary classification
                metrics.accuracy = accuracy_score(y_true, y_pred)
                metrics.precision = precision_score(y_true, y_pred, average='binary')
                metrics.recall = recall_score(y_true, y_pred, average='binary')
                metrics.f1_score = f1_score(y_true, y_pred, average='binary')
                
                try:
                    metrics.auc_roc = roc_auc_score(y_true, y_pred)
                except:
                    pass
            else:  # Multiclass
                metrics.accuracy = accuracy_score(y_true, y_pred)
                metrics.precision = precision_score(y_true, y_pred, average='weighted')
                metrics.recall = recall_score(y_true, y_pred, average='weighted')
                metrics.f1_score = f1_score(y_true, y_pred, average='weighted')
        
        elif model_type == ModelType.REGRESSION:
            metrics.mse = mean_squared_error(y_true, y_pred)
            metrics.rmse = np.sqrt(metrics.mse)
            metrics.mae = mean_absolute_error(y_true, y_pred)
            metrics.r2_score = r2_score(y_true, y_pred)
        
        return metrics
    
    def fit(self, data: Union[pd.DataFrame, np.ndarray], **kwargs) -> 'ModelManager':
        """Fit method for BaseMLComponent interface"""
        # ModelManager doesn't fit directly, but manages other models
        self._is_fitted = True
        return self
    
    def transform(self, data: Union[pd.DataFrame, np.ndarray]) -> Union[pd.DataFrame, np.ndarray]:
        """Transform method for BaseMLComponent interface"""
        # ModelManager doesn't transform directly
        return data
    
    def cleanup_old_models(self, days: int = 30) -> int:
        """Clean up old, unused models"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        archived_count = 0
        
        for model_id, metadata in self.registry.models.items():
            if (metadata.status in [ModelStatus.TRAINED, ModelStatus.DEPRECATED] and
                (metadata.last_used is None or metadata.last_used < cutoff_date)):
                
                if self.archive_model(model_id):
                    archived_count += 1
        
        return archived_count