"""
Advanced ML Predictor with Automated Pipeline
Production-ready machine learning predictor with automated feature engineering
"""

import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
import json

import numpy as np
import pandas as pd
from pydantic import BaseModel

from ..core.base import BaseMLComponent, MLConfig, ModelMetrics
from ..core.feature_engineering import FeatureEngineer, FeatureConfig
from ..core.model_manager import ModelManager, ModelType, ModelStatus

# Suppress sklearn warnings
warnings.filterwarnings('ignore', category=UserWarning)


class PredictionConfig(BaseModel):
    """Configuration for ML prediction pipeline"""
    auto_feature_engineering: bool = True
    auto_model_selection: bool = True
    cross_validation_folds: int = 5
    test_size: float = 0.2
    random_state: int = 42
    
    # Model selection parameters
    max_models_to_try: int = 5
    model_timeout_minutes: int = 30
    metric_optimization: str = "auto"  # auto, accuracy, f1, rmse, r2
    
    # Advanced settings
    enable_feature_selection: bool = True
    enable_hyperparameter_tuning: bool = True
    ensemble_models: bool = True
    explain_predictions: bool = True


class PredictionResult(BaseModel):
    """Result of ML prediction"""
    predictions: List[float]
    probabilities: Optional[List[List[float]]] = None
    confidence_scores: Optional[List[float]] = None
    feature_importance: Optional[Dict[str, float]] = None
    model_explanation: Optional[str] = None
    prediction_metadata: Dict[str, Any] = {}
    
    class Config:
        arbitrary_types_allowed = True


class MLPredictor(BaseMLComponent):
    """Advanced ML predictor with automated pipeline"""
    
    def __init__(self, 
                 config: Optional[PredictionConfig] = None,
                 ml_config: Optional[MLConfig] = None):
        super().__init__(ml_config)
        self.config = config or PredictionConfig()
        self.feature_engineer: Optional[FeatureEngineer] = None
        self.model_manager = ModelManager()
        self.trained_models: Dict[str, Any] = {}
        self.best_model_id: Optional[str] = None
        self.model_type: Optional[ModelType] = None
        
    def fit(self, 
            data: pd.DataFrame, 
            target: pd.Series,
            model_name: str = "auto_ml_model",
            problem_type: Optional[str] = None,
            **kwargs) -> 'MLPredictor':
        """Fit the ML predictor with automated pipeline"""
        self.validate_data(data)
        
        if target is None or len(target) == 0:
            raise ValueError("Target variable is required for training")
        
        self.logger.info(f"Starting ML training with {len(data)} samples and {len(data.columns)} features")
        
        # Determine problem type
        if problem_type is None:
            problem_type = self._infer_problem_type(target)
        
        self.model_type = ModelType.CLASSIFICATION if problem_type == "classification" else ModelType.REGRESSION
        
        # Feature engineering
        if self.config.auto_feature_engineering:
            self.logger.info("Starting automated feature engineering")
            self.feature_engineer = FeatureEngineer(
                config=FeatureConfig(
                    auto_feature_selection=self.config.enable_feature_selection
                )
            )
            
            # Fit and transform features
            engineered_data = self.feature_engineer.fit_transform(data, target)
            self.logger.info(f"Feature engineering completed: {engineered_data.shape[1]} features")
        else:
            engineered_data = data.copy()
        
        # Split data for training and validation
        from sklearn.model_selection import train_test_split
        
        X_train, X_test, y_train, y_test = train_test_split(
            engineered_data, target,
            test_size=self.config.test_size,
            random_state=self.config.random_state,
            stratify=target if problem_type == "classification" else None
        )
        
        # Train models
        if self.config.auto_model_selection:
            best_model, best_score = self._train_multiple_models(
                X_train, y_train, X_test, y_test, problem_type
            )
        else:
            best_model, best_score = self._train_single_model(
                X_train, y_train, X_test, y_test, problem_type
            )
        
        # Register best model
        if best_model is not None:
            # Calculate comprehensive metrics
            train_predictions = best_model.predict(X_train)
            test_predictions = best_model.predict(X_test)
            
            train_metrics = self._calculate_metrics(y_train, train_predictions, problem_type)
            test_metrics = self._calculate_metrics(y_test, test_predictions, problem_type)
            
            # Get feature names
            feature_names = list(engineered_data.columns)
            
            # Register model
            self.best_model_id = self.model_manager.register_model(
                model=best_model,
                model_name=model_name,
                model_type=self.model_type,
                algorithm=best_model.__class__.__name__,
                metrics=train_metrics,
                feature_names=feature_names,
                hyperparameters=getattr(best_model, 'get_params', lambda: {})(),
                description=f"Auto-trained {problem_type} model",
                tags=["auto-ml", problem_type],
                environment="training"
            )
            
            # Update with validation metrics
            self.model_manager.registry.update_model(
                self.best_model_id,
                validation_metrics=test_metrics,
                status=ModelStatus.VALIDATED
            )
            
            self.logger.info(f"Best model registered with ID: {self.best_model_id}")
            self.logger.info(f"Best validation score: {best_score:.4f}")
        
        self._is_fitted = True
        return self
    
    def predict(self, data: pd.DataFrame) -> PredictionResult:
        """Make predictions on new data"""
        if not self.is_fitted or self.best_model_id is None:
            raise ValueError("MLPredictor must be fitted before making predictions")
        
        self.validate_data(data)
        
        # Apply feature engineering
        if self.feature_engineer is not None:
            engineered_data = self.feature_engineer.transform(data)
        else:
            engineered_data = data.copy()
        
        # Make predictions using model manager
        predictions = self.model_manager.predict(self.best_model_id, engineered_data)
        
        # Get model for additional information
        model = self.model_manager.load_model(self.best_model_id)
        
        # Calculate prediction probabilities for classification
        probabilities = None
        confidence_scores = None
        
        if self.model_type == ModelType.CLASSIFICATION and hasattr(model, 'predict_proba'):
            try:
                probabilities = model.predict_proba(engineered_data).tolist()
                # Calculate confidence as max probability
                confidence_scores = [max(probs) for probs in probabilities]
            except Exception as e:
                self.logger.warning(f"Could not calculate probabilities: {e}")
        
        # Get feature importance
        feature_importance = None
        if hasattr(model, 'feature_importances_'):
            feature_names = engineered_data.columns
            importance_values = model.feature_importances_
            feature_importance = dict(zip(feature_names, importance_values))
        
        # Generate model explanation
        model_explanation = self._generate_prediction_explanation(
            model, predictions, engineered_data
        )
        
        return PredictionResult(
            predictions=predictions.tolist(),
            probabilities=probabilities,
            confidence_scores=confidence_scores,
            feature_importance=feature_importance,
            model_explanation=model_explanation,
            prediction_metadata={
                "model_id": self.best_model_id,
                "model_type": self.model_type.value,
                "feature_count": len(engineered_data.columns),
                "prediction_timestamp": datetime.utcnow().isoformat()
            }
        )
    
    def _infer_problem_type(self, target: pd.Series) -> str:
        """Infer whether this is a classification or regression problem"""
        if target.dtype == 'object' or target.dtype.name == 'category':
            return "classification"
        
        # Check if target has few unique values (likely classification)
        unique_values = target.nunique()
        total_values = len(target)
        
        if unique_values <= 20 or unique_values / total_values < 0.05:
            return "classification"
        
        return "regression"
    
    def _train_multiple_models(self, 
                             X_train: pd.DataFrame,
                             y_train: pd.Series,
                             X_test: pd.DataFrame,
                             y_test: pd.Series,
                             problem_type: str) -> Tuple[Any, float]:
        """Train multiple models and select the best one"""
        from sklearn.model_selection import cross_val_score
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        from sklearn.linear_model import LogisticRegression, LinearRegression
        from sklearn.svm import SVC, SVR
        from sklearn.naive_bayes import GaussianNB
        from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
        
        models_to_try = []
        
        if problem_type == "classification":
            models_to_try = [
                ("RandomForest", RandomForestClassifier(random_state=self.config.random_state)),
                ("LogisticRegression", LogisticRegression(random_state=self.config.random_state, max_iter=1000)),
                ("SVM", SVC(random_state=self.config.random_state, probability=True)),
                ("NaiveBayes", GaussianNB()),
                ("KNeighbors", KNeighborsClassifier())
            ]
        else:
            models_to_try = [
                ("RandomForest", RandomForestRegressor(random_state=self.config.random_state)),
                ("LinearRegression", LinearRegression()),
                ("SVM", SVR()),
                ("KNeighbors", KNeighborsRegressor())
            ]
        
        # Limit number of models to try
        models_to_try = models_to_try[:self.config.max_models_to_try]
        
        best_model = None
        best_score = float('-inf') if problem_type == "classification" else float('inf')
        
        for model_name, model in models_to_try:
            try:
                self.logger.info(f"Training {model_name}...")
                
                # Cross-validation
                scoring = self._get_scoring_metric(problem_type)
                cv_scores = cross_val_score(
                    model, X_train, y_train, 
                    cv=self.config.cross_validation_folds,
                    scoring=scoring
                )
                
                mean_cv_score = cv_scores.mean()
                
                # Train on full training set
                model.fit(X_train, y_train)
                
                # Evaluate on test set
                test_predictions = model.predict(X_test)
                test_score = self._score_predictions(y_test, test_predictions, problem_type)
                
                self.logger.info(f"{model_name} - CV Score: {mean_cv_score:.4f}, Test Score: {test_score:.4f}")
                
                # Select best model based on test score
                is_better = (test_score > best_score if problem_type == "classification" 
                           else test_score < best_score)
                
                if is_better:
                    best_model = model
                    best_score = test_score
                
                # Store model
                self.trained_models[model_name] = {
                    'model': model,
                    'cv_score': mean_cv_score,
                    'test_score': test_score
                }
                
            except Exception as e:
                self.logger.warning(f"Failed to train {model_name}: {e}")
                continue
        
        return best_model, best_score
    
    def _train_single_model(self,
                          X_train: pd.DataFrame,
                          y_train: pd.Series,
                          X_test: pd.DataFrame,
                          y_test: pd.Series,
                          problem_type: str) -> Tuple[Any, float]:
        """Train a single default model"""
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        
        if problem_type == "classification":
            model = RandomForestClassifier(random_state=self.config.random_state)
        else:
            model = RandomForestRegressor(random_state=self.config.random_state)
        
        model.fit(X_train, y_train)
        test_predictions = model.predict(X_test)
        test_score = self._score_predictions(y_test, test_predictions, problem_type)
        
        return model, test_score
    
    def _get_scoring_metric(self, problem_type: str) -> str:
        """Get appropriate scoring metric for cross-validation"""
        if problem_type == "classification":
            return "accuracy"
        else:
            return "neg_mean_squared_error"
    
    def _score_predictions(self, y_true, y_pred, problem_type: str) -> float:
        """Score predictions based on problem type"""
        if problem_type == "classification":
            from sklearn.metrics import accuracy_score
            return accuracy_score(y_true, y_pred)
        else:
            from sklearn.metrics import mean_squared_error
            return -mean_squared_error(y_true, y_pred)  # Negative because we want to maximize
    
    def _calculate_metrics(self, y_true, y_pred, problem_type: str) -> ModelMetrics:
        """Calculate comprehensive metrics"""
        try:
            from sklearn.metrics import (
                accuracy_score, precision_score, recall_score, f1_score,
                mean_squared_error, mean_absolute_error, r2_score
            )
        except ImportError:
            return ModelMetrics()
        
        metrics = ModelMetrics()
        
        if problem_type == "classification":
            metrics.accuracy = accuracy_score(y_true, y_pred)
            try:
                metrics.precision = precision_score(y_true, y_pred, average='weighted')
                metrics.recall = recall_score(y_true, y_pred, average='weighted')
                metrics.f1_score = f1_score(y_true, y_pred, average='weighted')
            except:
                pass
        else:
            metrics.mse = mean_squared_error(y_true, y_pred)
            metrics.rmse = np.sqrt(metrics.mse)
            metrics.mae = mean_absolute_error(y_true, y_pred)
            metrics.r2_score = r2_score(y_true, y_pred)
        
        return metrics
    
    def _generate_prediction_explanation(self, 
                                       model: Any,
                                       predictions: np.ndarray,
                                       data: pd.DataFrame) -> str:
        """Generate human-readable explanation of predictions"""
        if not self.config.explain_predictions:
            return "Explanations disabled"
        
        try:
            explanation_parts = []
            
            # Model information
            explanation_parts.append(f"Model: {model.__class__.__name__}")
            
            # Feature importance (top 5)
            if hasattr(model, 'feature_importances_'):
                feature_names = list(data.columns)
                importances = model.feature_importances_
                
                top_features = sorted(
                    zip(feature_names, importances),
                    key=lambda x: x[1],
                    reverse=True
                )[:5]
                
                explanation_parts.append("Top influential features:")
                for feature, importance in top_features:
                    explanation_parts.append(f"  - {feature}: {importance:.3f}")
            
            # Prediction summary
            if self.model_type == ModelType.CLASSIFICATION:
                unique_predictions = np.unique(predictions)
                explanation_parts.append(f"Predicted classes: {list(unique_predictions)}")
            else:
                explanation_parts.append(f"Prediction range: {predictions.min():.3f} to {predictions.max():.3f}")
            
            return "\n".join(explanation_parts)
            
        except Exception as e:
            return f"Could not generate explanation: {e}"
    
    def get_model_comparison(self) -> Dict[str, Any]:
        """Get comparison of all trained models"""
        if not self.trained_models:
            return {"message": "No models trained yet"}
        
        comparison = []
        for model_name, model_info in self.trained_models.items():
            comparison.append({
                "model_name": model_name,
                "cv_score": model_info['cv_score'],
                "test_score": model_info['test_score'],
                "is_best": (model_info['model'] == self.model_manager.load_model(self.best_model_id) 
                          if self.best_model_id else False)
            })
        
        # Sort by test score
        comparison.sort(key=lambda x: x['test_score'], reverse=self.model_type == ModelType.CLASSIFICATION)
        
        return {
            "model_comparison": comparison,
            "best_model_id": self.best_model_id,
            "total_models_trained": len(self.trained_models)
        }
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform method for BaseMLComponent interface"""
        # For predictor, transform returns predictions
        result = self.predict(data)
        
        # Convert predictions to DataFrame
        pred_df = pd.DataFrame({
            'predictions': result.predictions
        })
        
        if result.confidence_scores:
            pred_df['confidence'] = result.confidence_scores
        
        return pred_df


class PredictionPipeline:
    """End-to-end prediction pipeline with data preprocessing and model management"""
    
    def __init__(self, pipeline_name: str, config: Optional[PredictionConfig] = None):
        self.pipeline_name = pipeline_name
        self.config = config or PredictionConfig()
        self.predictor = MLPredictor(config)
        self.pipeline_metadata = {
            "created_at": datetime.utcnow(),
            "pipeline_name": pipeline_name,
            "predictions_made": 0
        }
        
    def train_pipeline(self,
                      data: pd.DataFrame,
                      target: pd.Series,
                      validation_data: Optional[pd.DataFrame] = None,
                      validation_target: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Train the complete prediction pipeline"""
        
        # Train predictor
        self.predictor.fit(data, target, model_name=f"{self.pipeline_name}_model")
        
        # Validate on separate dataset if provided
        validation_results = None
        if validation_data is not None and validation_target is not None:
            validation_predictions = self.predictor.predict(validation_data)
            
            # Calculate validation metrics
            if self.predictor.model_type == ModelType.CLASSIFICATION:
                from sklearn.metrics import classification_report
                validation_results = {
                    "classification_report": classification_report(
                        validation_target, validation_predictions.predictions, output_dict=True
                    )
                }
            else:
                from sklearn.metrics import mean_squared_error, r2_score
                validation_results = {
                    "mse": mean_squared_error(validation_target, validation_predictions.predictions),
                    "r2_score": r2_score(validation_target, validation_predictions.predictions)
                }
        
        # Update metadata
        self.pipeline_metadata["trained_at"] = datetime.utcnow()
        self.pipeline_metadata["model_id"] = self.predictor.best_model_id
        
        return {
            "pipeline_name": self.pipeline_name,
            "training_completed": True,
            "model_id": self.predictor.best_model_id,
            "model_comparison": self.predictor.get_model_comparison(),
            "validation_results": validation_results,
            "pipeline_metadata": self.pipeline_metadata
        }
    
    def predict_batch(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Make batch predictions"""
        if not self.predictor.is_fitted:
            raise ValueError("Pipeline must be trained before making predictions")
        
        predictions = self.predictor.predict(data)
        
        # Update metadata
        self.pipeline_metadata["predictions_made"] += len(data)
        self.pipeline_metadata["last_prediction"] = datetime.utcnow()
        
        return {
            "predictions": predictions,
            "batch_size": len(data),
            "pipeline_metadata": self.pipeline_metadata
        }
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status"""
        return {
            "pipeline_name": self.pipeline_name,
            "is_trained": self.predictor.is_fitted,
            "model_id": self.predictor.best_model_id,
            "metadata": self.pipeline_metadata
        }