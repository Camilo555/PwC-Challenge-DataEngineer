"""
Predictive Analytics Engine

Comprehensive predictive analytics with sales forecasting, customer segmentation,
anomaly detection, and automated insights generation.
"""

import logging
import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import json
import joblib
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Time series forecasting
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.exponential_smoothing.ets import ETSModel
from statsmodels.tsa.seasonal import seasonal_decompose
from prophet import Prophet
import pmdarima as pm

# ML models
from sklearn.cluster import KMeans, DBSCAN
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import silhouette_score, adjusted_rand_score
from sklearn.decomposition import PCA
import xgboost as xgb
import lightgbm as lgb

# Anomaly detection
from pyod.models.iforest import IForest
from pyod.models.lof import LOF
from pyod.models.ocsvm import OCSVM
from pyod.models.autoencoder import AutoEncoder

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.data_access.supabase_client import get_supabase_client
from src.ml.feature_engineering.feature_pipeline import FeatureEngineeringPipeline

logger = get_logger(__name__)
settings = get_settings()


class PredictionType(Enum):
    """Types of predictions supported."""
    SALES_FORECAST = "sales_forecast"
    DEMAND_PREDICTION = "demand_prediction"
    CUSTOMER_SEGMENTATION = "customer_segmentation"
    ANOMALY_DETECTION = "anomaly_detection"
    CHURN_PREDICTION = "churn_prediction"
    PRICE_OPTIMIZATION = "price_optimization"


@dataclass
class ForecastConfig:
    """Configuration for forecasting models."""
    
    # Time series parameters
    freq: str = "D"  # Daily frequency
    horizon: int = 30  # Forecast horizon
    seasonal_period: int = 7  # Weekly seasonality
    
    # Model selection
    models: List[str] = field(default_factory=lambda: ["prophet", "arima", "ets"])
    auto_model_selection: bool = True
    
    # Prophet-specific
    prophet_seasonalities: List[Dict[str, Any]] = field(default_factory=lambda: [
        {"name": "weekly", "period": 7, "fourier_order": 3},
        {"name": "monthly", "period": 30.5, "fourier_order": 5}
    ])
    
    # Validation
    validation_method: str = "time_series_split"
    n_splits: int = 3
    
    # Confidence intervals
    confidence_level: float = 0.95


@dataclass
class SegmentationConfig:
    """Configuration for customer segmentation."""
    
    # Clustering parameters
    n_clusters: int = 5
    algorithm: str = "kmeans"  # "kmeans", "dbscan", "hierarchical"
    
    # Feature scaling
    scale_features: bool = True
    scaling_method: str = "standard"
    
    # Dimensionality reduction
    use_pca: bool = False
    pca_components: int = 10
    
    # RFM analysis
    enable_rfm: bool = True
    rfm_quantiles: int = 5


@dataclass
class AnomalyConfig:
    """Configuration for anomaly detection."""
    
    # Model parameters
    algorithm: str = "isolation_forest"  # "isolation_forest", "lof", "ocsvm", "autoencoder"
    contamination: float = 0.1
    
    # Ensemble parameters
    use_ensemble: bool = True
    ensemble_methods: List[str] = field(default_factory=lambda: ["isolation_forest", "lof"])
    
    # Time series anomalies
    enable_temporal_detection: bool = True
    window_size: int = 7
    
    # Thresholds
    anomaly_threshold: float = 0.5
    severity_levels: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.3,
        "medium": 0.5,
        "high": 0.7,
        "critical": 0.9
    })


@dataclass
class PredictionResult:
    """Result of a prediction operation."""
    
    prediction_id: str
    prediction_type: PredictionType
    timestamp: datetime
    model_used: str
    confidence_score: float
    
    # Forecast-specific
    forecast_values: Optional[List[float]] = None
    confidence_intervals: Optional[List[Tuple[float, float]]] = None
    forecast_dates: Optional[List[datetime]] = None
    
    # Segmentation-specific
    cluster_assignments: Optional[List[int]] = None
    cluster_centers: Optional[List[List[float]]] = None
    silhouette_score: Optional[float] = None
    
    # Anomaly-specific
    anomaly_scores: Optional[List[float]] = None
    is_anomaly: Optional[List[bool]] = None
    anomaly_severity: Optional[List[str]] = None
    
    # General
    feature_importance: Optional[Dict[str, float]] = None
    model_performance: Optional[Dict[str, float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class BasePredictionModel(ABC):
    """Base class for prediction models."""
    
    def __init__(self, model_type: str):
        self.model_type = model_type
        self.model = None
        self.is_fitted = False
        
    @abstractmethod
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'BasePredictionModel':
        """Fit the model."""
        pass
        
    @abstractmethod
    def predict(self, X: pd.DataFrame) -> PredictionResult:
        """Make predictions."""
        pass


class SalesForecaster(BasePredictionModel):
    """Sales forecasting model using multiple time series methods."""
    
    def __init__(self, config: ForecastConfig):
        super().__init__("sales_forecaster")
        self.config = config
        self.models = {}
        self.best_model = None
        self.scaler = StandardScaler()
        
    def fit(self, data: pd.DataFrame, target_column: str = "sales",
           date_column: str = "date") -> 'SalesForecaster':
        """Fit forecasting models."""
        
        try:
            # Prepare time series data
            ts_data = data.copy()
            ts_data[date_column] = pd.to_datetime(ts_data[date_column])
            ts_data = ts_data.sort_values(date_column)
            ts_data.set_index(date_column, inplace=True)
            
            # Resample to desired frequency
            ts_data = ts_data.resample(self.config.freq)[target_column].sum()
            
            # Handle missing values
            ts_data = ts_data.interpolate(method='linear')
            
            logger.info(f"Fitting forecasting models on {len(ts_data)} data points")
            
            # Fit Prophet model
            if "prophet" in self.config.models:
                self._fit_prophet(ts_data)
            
            # Fit ARIMA model
            if "arima" in self.config.models:
                self._fit_arima(ts_data)
            
            # Fit ETS model
            if "ets" in self.config.models:
                self._fit_ets(ts_data)
            
            # Fit XGBoost for time series
            if "xgboost" in self.config.models:
                self._fit_xgboost(ts_data)
            
            # Select best model if enabled
            if self.config.auto_model_selection:
                self._select_best_model(ts_data)
            
            self.is_fitted = True
            logger.info("Sales forecasting models fitted successfully")
            
            return self
            
        except Exception as e:
            logger.error(f"Error fitting sales forecaster: {str(e)}")
            raise
    
    def predict(self, horizon: Optional[int] = None) -> PredictionResult:
        """Generate sales forecast."""
        
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        horizon = horizon or self.config.horizon
        model_name = self.best_model or list(self.models.keys())[0]
        model = self.models[model_name]
        
        try:
            if model_name == "prophet":
                forecast_result = self._predict_prophet(model, horizon)
            elif model_name == "arima":
                forecast_result = self._predict_arima(model, horizon)
            elif model_name == "ets":
                forecast_result = self._predict_ets(model, horizon)
            elif model_name == "xgboost":
                forecast_result = self._predict_xgboost(model, horizon)
            else:
                raise ValueError(f"Unknown model: {model_name}")
            
            return PredictionResult(
                prediction_id=f"forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                prediction_type=PredictionType.SALES_FORECAST,
                timestamp=datetime.utcnow(),
                model_used=model_name,
                confidence_score=0.85,  # This would be calculated based on validation
                forecast_values=forecast_result["values"],
                confidence_intervals=forecast_result["intervals"],
                forecast_dates=forecast_result["dates"]
            )
            
        except Exception as e:
            logger.error(f"Error generating forecast: {str(e)}")
            raise
    
    def _fit_prophet(self, ts_data: pd.Series):
        """Fit Prophet model."""
        try:
            # Prepare data for Prophet
            prophet_data = pd.DataFrame({
                'ds': ts_data.index,
                'y': ts_data.values
            })
            
            # Initialize Prophet
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                interval_width=self.config.confidence_level
            )
            
            # Add custom seasonalities
            for seasonality in self.config.prophet_seasonalities:
                model.add_seasonality(**seasonality)
            
            # Fit model
            model.fit(prophet_data)
            self.models["prophet"] = model
            
            logger.info("Prophet model fitted successfully")
            
        except Exception as e:
            logger.error(f"Error fitting Prophet model: {str(e)}")
    
    def _fit_arima(self, ts_data: pd.Series):
        """Fit ARIMA model with auto parameter selection."""
        try:
            # Use pmdarima for automatic ARIMA parameter selection
            model = pm.auto_arima(
                ts_data,
                seasonal=True,
                m=self.config.seasonal_period,
                stepwise=True,
                suppress_warnings=True,
                error_action='ignore'
            )
            
            self.models["arima"] = model
            logger.info(f"ARIMA model fitted: {model.order} x {model.seasonal_order}")
            
        except Exception as e:
            logger.error(f"Error fitting ARIMA model: {str(e)}")
    
    def _fit_ets(self, ts_data: pd.Series):
        """Fit ETS (Error, Trend, Seasonality) model."""
        try:
            model = ETSModel(
                ts_data,
                error='add',
                trend='add',
                seasonal='add',
                seasonal_periods=self.config.seasonal_period
            )
            
            fitted_model = model.fit()
            self.models["ets"] = fitted_model
            logger.info("ETS model fitted successfully")
            
        except Exception as e:
            logger.error(f"Error fitting ETS model: {str(e)}")
    
    def _fit_xgboost(self, ts_data: pd.Series):
        """Fit XGBoost for time series forecasting."""
        try:
            # Create lagged features
            X, y = self._create_lagged_features(ts_data)
            
            # Scale features
            X_scaled = self.scaler.fit_transform(X)
            
            # Fit XGBoost
            model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            )
            
            model.fit(X_scaled, y)
            self.models["xgboost"] = {
                "model": model,
                "last_values": ts_data.tail(10).values  # Store for prediction
            }
            
            logger.info("XGBoost model fitted successfully")
            
        except Exception as e:
            logger.error(f"Error fitting XGBoost model: {str(e)}")
    
    def _create_lagged_features(self, ts_data: pd.Series, lags: int = 10) -> Tuple[np.ndarray, np.ndarray]:
        """Create lagged features for time series prediction."""
        X, y = [], []
        
        for i in range(lags, len(ts_data)):
            X.append(ts_data.iloc[i-lags:i].values)
            y.append(ts_data.iloc[i])
        
        return np.array(X), np.array(y)
    
    def _select_best_model(self, ts_data: pd.Series):
        """Select best model based on validation performance."""
        try:
            best_score = float('inf')
            best_model = None
            
            # Simple validation: use last 20% for testing
            train_size = int(len(ts_data) * 0.8)
            train_data = ts_data[:train_size]
            test_data = ts_data[train_size:]
            
            for model_name, model in self.models.items():
                try:
                    if model_name == "prophet":
                        future = model.make_future_dataframe(periods=len(test_data), freq=self.config.freq)
                        forecast = model.predict(future)
                        predictions = forecast.tail(len(test_data))['yhat'].values
                    
                    elif model_name == "arima":
                        predictions = model.predict(n_periods=len(test_data))
                    
                    elif model_name == "ets":
                        predictions = model.forecast(steps=len(test_data))
                    
                    elif model_name == "xgboost":
                        # This would need proper implementation for XGBoost time series
                        continue
                    
                    # Calculate RMSE
                    rmse = np.sqrt(np.mean((test_data.values - predictions) ** 2))
                    
                    if rmse < best_score:
                        best_score = rmse
                        best_model = model_name
                        
                except Exception as e:
                    logger.warning(f"Error validating {model_name}: {str(e)}")
                    continue
            
            self.best_model = best_model
            logger.info(f"Best model selected: {best_model} (RMSE: {best_score:.2f})")
            
        except Exception as e:
            logger.error(f"Error selecting best model: {str(e)}")
            self.best_model = list(self.models.keys())[0] if self.models else None
    
    def _predict_prophet(self, model, horizon: int) -> Dict[str, Any]:
        """Generate Prophet forecast."""
        future = model.make_future_dataframe(periods=horizon, freq=self.config.freq)
        forecast = model.predict(future)
        
        # Extract forecast results
        forecast_tail = forecast.tail(horizon)
        
        return {
            "values": forecast_tail['yhat'].tolist(),
            "intervals": list(zip(forecast_tail['yhat_lower'].tolist(), 
                                forecast_tail['yhat_upper'].tolist())),
            "dates": forecast_tail['ds'].tolist()
        }
    
    def _predict_arima(self, model, horizon: int) -> Dict[str, Any]:
        """Generate ARIMA forecast."""
        forecast, conf_int = model.predict(n_periods=horizon, return_conf_int=True)
        
        # Generate future dates
        future_dates = pd.date_range(
            start=model.arima_res_.data.dates[-1] + pd.Timedelta(days=1),
            periods=horizon,
            freq=self.config.freq
        )
        
        return {
            "values": forecast.tolist(),
            "intervals": [(conf_int[i, 0], conf_int[i, 1]) for i in range(len(conf_int))],
            "dates": future_dates.tolist()
        }
    
    def _predict_ets(self, model, horizon: int) -> Dict[str, Any]:
        """Generate ETS forecast."""
        forecast = model.forecast(steps=horizon)
        
        # Generate future dates (simplified)
        future_dates = pd.date_range(
            start=datetime.now(),
            periods=horizon,
            freq=self.config.freq
        )
        
        return {
            "values": forecast.tolist(),
            "intervals": [(val * 0.9, val * 1.1) for val in forecast],  # Simplified CI
            "dates": future_dates.tolist()
        }
    
    def _predict_xgboost(self, model_info, horizon: int) -> Dict[str, Any]:
        """Generate XGBoost forecast."""
        model = model_info["model"]
        last_values = model_info["last_values"]
        
        predictions = []
        current_values = last_values.copy()
        
        for _ in range(horizon):
            # Use last 10 values as features
            X = current_values[-10:].reshape(1, -1)
            X_scaled = self.scaler.transform(X)
            
            pred = model.predict(X_scaled)[0]
            predictions.append(pred)
            
            # Update current_values for next prediction
            current_values = np.append(current_values[1:], pred)
        
        # Generate future dates
        future_dates = pd.date_range(
            start=datetime.now(),
            periods=horizon,
            freq=self.config.freq
        )
        
        return {
            "values": predictions,
            "intervals": [(val * 0.95, val * 1.05) for val in predictions],  # Simplified CI
            "dates": future_dates.tolist()
        }


class CustomerSegmentation(BasePredictionModel):
    """Customer segmentation using clustering algorithms."""
    
    def __init__(self, config: SegmentationConfig):
        super().__init__("customer_segmentation")
        self.config = config
        self.scaler = None
        self.pca = None
        self.clustering_model = None
        self.rfm_quantiles = None
        
    def fit(self, customer_data: pd.DataFrame) -> 'CustomerSegmentation':
        """Fit customer segmentation model."""
        
        try:
            logger.info(f"Fitting customer segmentation on {len(customer_data)} customers")
            
            # Prepare features
            features = self._prepare_features(customer_data)
            
            # Scale features if enabled
            if self.config.scale_features:
                if self.config.scaling_method == "standard":
                    self.scaler = StandardScaler()
                else:
                    self.scaler = MinMaxScaler()
                
                features_scaled = self.scaler.fit_transform(features)
                features_df = pd.DataFrame(features_scaled, columns=features.columns)
            else:
                features_df = features
            
            # Apply PCA if enabled
            if self.config.use_pca:
                self.pca = PCA(n_components=self.config.pca_components)
                features_pca = self.pca.fit_transform(features_df)
                features_df = pd.DataFrame(features_pca, 
                                         columns=[f"PC{i+1}" for i in range(self.config.pca_components)])
            
            # Fit clustering model
            if self.config.algorithm == "kmeans":
                self.clustering_model = KMeans(
                    n_clusters=self.config.n_clusters,
                    random_state=42,
                    n_init=10
                )
            elif self.config.algorithm == "dbscan":
                self.clustering_model = DBSCAN(
                    eps=0.5,
                    min_samples=5
                )
            
            self.clustering_model.fit(features_df)
            
            # Calculate RFM quantiles if enabled
            if self.config.enable_rfm and all(col in customer_data.columns for col in ['recency', 'frequency', 'monetary']):
                self._calculate_rfm_quantiles(customer_data)
            
            self.is_fitted = True
            logger.info("Customer segmentation model fitted successfully")
            
            return self
            
        except Exception as e:
            logger.error(f"Error fitting customer segmentation: {str(e)}")
            raise
    
    def predict(self, customer_data: pd.DataFrame) -> PredictionResult:
        """Segment customers."""
        
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        try:
            # Prepare features
            features = self._prepare_features(customer_data)
            
            # Apply same transformations
            if self.scaler:
                features_scaled = self.scaler.transform(features)
                features_df = pd.DataFrame(features_scaled, columns=features.columns)
            else:
                features_df = features
            
            if self.pca:
                features_pca = self.pca.transform(features_df)
                features_df = pd.DataFrame(features_pca, 
                                         columns=[f"PC{i+1}" for i in range(self.config.pca_components)])
            
            # Predict clusters
            cluster_labels = self.clustering_model.predict(features_df)
            
            # Calculate silhouette score
            silhouette = silhouette_score(features_df, cluster_labels)
            
            # Get cluster centers (for KMeans)
            cluster_centers = None
            if hasattr(self.clustering_model, 'cluster_centers_'):
                cluster_centers = self.clustering_model.cluster_centers_.tolist()
            
            return PredictionResult(
                prediction_id=f"segmentation_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                prediction_type=PredictionType.CUSTOMER_SEGMENTATION,
                timestamp=datetime.utcnow(),
                model_used=self.config.algorithm,
                confidence_score=silhouette,
                cluster_assignments=cluster_labels.tolist(),
                cluster_centers=cluster_centers,
                silhouette_score=silhouette
            )
            
        except Exception as e:
            logger.error(f"Error predicting customer segments: {str(e)}")
            raise
    
    def _prepare_features(self, customer_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for clustering."""
        
        # Basic RFM features
        features = pd.DataFrame()
        
        if 'recency' in customer_data.columns:
            features['recency'] = customer_data['recency']
        
        if 'frequency' in customer_data.columns:
            features['frequency'] = customer_data['frequency']
        
        if 'monetary' in customer_data.columns:
            features['monetary'] = customer_data['monetary']
        
        # Additional behavioral features
        if 'avg_order_value' in customer_data.columns:
            features['avg_order_value'] = customer_data['avg_order_value']
        
        if 'days_since_last_purchase' in customer_data.columns:
            features['days_since_last_purchase'] = customer_data['days_since_last_purchase']
        
        if 'total_orders' in customer_data.columns:
            features['total_orders'] = customer_data['total_orders']
        
        # Handle missing values
        features = features.fillna(features.median())
        
        return features
    
    def _calculate_rfm_quantiles(self, customer_data: pd.DataFrame):
        """Calculate RFM quintiles for segmentation."""
        rfm_data = customer_data[['recency', 'frequency', 'monetary']].copy()
        
        # Calculate quantiles
        self.rfm_quantiles = {}
        self.rfm_quantiles['recency'] = rfm_data['recency'].quantile([0.2, 0.4, 0.6, 0.8]).tolist()
        self.rfm_quantiles['frequency'] = rfm_data['frequency'].quantile([0.2, 0.4, 0.6, 0.8]).tolist()
        self.rfm_quantiles['monetary'] = rfm_data['monetary'].quantile([0.2, 0.4, 0.6, 0.8]).tolist()


class AnomalyDetector(BasePredictionModel):
    """Multi-algorithm anomaly detection system."""
    
    def __init__(self, config: AnomalyConfig):
        super().__init__("anomaly_detector")
        self.config = config
        self.models = {}
        self.scaler = StandardScaler()
        
    def fit(self, data: pd.DataFrame) -> 'AnomalyDetector':
        """Fit anomaly detection models."""
        
        try:
            logger.info(f"Fitting anomaly detection on {len(data)} samples")
            
            # Prepare features
            features = self._prepare_features(data)
            
            # Scale features
            features_scaled = self.scaler.fit_transform(features)
            
            # Fit individual models
            if "isolation_forest" in self.config.ensemble_methods or self.config.algorithm == "isolation_forest":
                self.models["isolation_forest"] = IForest(
                    contamination=self.config.contamination,
                    random_state=42
                )
                self.models["isolation_forest"].fit(features_scaled)
            
            if "lof" in self.config.ensemble_methods or self.config.algorithm == "lof":
                self.models["lof"] = LOF(
                    contamination=self.config.contamination
                )
                self.models["lof"].fit(features_scaled)
            
            if "ocsvm" in self.config.ensemble_methods or self.config.algorithm == "ocsvm":
                self.models["ocsvm"] = OCSVM(
                    contamination=self.config.contamination
                )
                self.models["ocsvm"].fit(features_scaled)
            
            if "autoencoder" in self.config.ensemble_methods or self.config.algorithm == "autoencoder":
                try:
                    self.models["autoencoder"] = AutoEncoder(
                        contamination=self.config.contamination,
                        epochs=50,
                        verbose=0
                    )
                    self.models["autoencoder"].fit(features_scaled)
                except Exception as e:
                    logger.warning(f"Could not fit autoencoder: {str(e)}")
            
            self.is_fitted = True
            logger.info("Anomaly detection models fitted successfully")
            
            return self
            
        except Exception as e:
            logger.error(f"Error fitting anomaly detector: {str(e)}")
            raise
    
    def predict(self, data: pd.DataFrame) -> PredictionResult:
        """Detect anomalies in data."""
        
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        try:
            # Prepare features
            features = self._prepare_features(data)
            features_scaled = self.scaler.transform(features)
            
            if self.config.use_ensemble:
                # Ensemble prediction
                all_scores = []
                all_labels = []
                
                for model_name, model in self.models.items():
                    try:
                        scores = model.decision_function(features_scaled)
                        labels = model.predict(features_scaled)
                        
                        all_scores.append(scores)
                        all_labels.append(labels)
                    except Exception as e:
                        logger.warning(f"Error with {model_name}: {str(e)}")
                        continue
                
                if all_scores:
                    # Average ensemble scores
                    ensemble_scores = np.mean(all_scores, axis=0)
                    ensemble_labels = np.mean(all_labels, axis=0)
                    
                    # Convert to binary anomaly detection
                    is_anomaly = ensemble_labels == 1
                else:
                    ensemble_scores = np.zeros(len(features_scaled))
                    is_anomaly = np.zeros(len(features_scaled), dtype=bool)
            
            else:
                # Single model prediction
                model = self.models[self.config.algorithm]
                ensemble_scores = model.decision_function(features_scaled)
                labels = model.predict(features_scaled)
                is_anomaly = labels == 1
            
            # Determine severity levels
            severity_levels = self._determine_severity(ensemble_scores)
            
            return PredictionResult(
                prediction_id=f"anomaly_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                prediction_type=PredictionType.ANOMALY_DETECTION,
                timestamp=datetime.utcnow(),
                model_used="ensemble" if self.config.use_ensemble else self.config.algorithm,
                confidence_score=np.mean(np.abs(ensemble_scores)),
                anomaly_scores=ensemble_scores.tolist(),
                is_anomaly=is_anomaly.tolist(),
                anomaly_severity=severity_levels
            )
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
            raise
    
    def _prepare_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for anomaly detection."""
        # Select numerical columns
        numerical_columns = data.select_dtypes(include=[np.number]).columns
        features = data[numerical_columns].copy()
        
        # Handle missing values
        features = features.fillna(features.median())
        
        return features
    
    def _determine_severity(self, scores: np.ndarray) -> List[str]:
        """Determine severity levels based on anomaly scores."""
        severity_levels = []
        
        for score in scores:
            abs_score = abs(score)
            
            if abs_score >= self.config.severity_levels["critical"]:
                severity_levels.append("critical")
            elif abs_score >= self.config.severity_levels["high"]:
                severity_levels.append("high")
            elif abs_score >= self.config.severity_levels["medium"]:
                severity_levels.append("medium")
            else:
                severity_levels.append("low")
        
        return severity_levels


class PredictiveAnalyticsEngine:
    """Main predictive analytics engine coordinating all models."""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        self.metrics_collector = MetricsCollector()
        self.models: Dict[str, BasePredictionModel] = {}
        
    async def register_model(self, model_id: str, model: BasePredictionModel):
        """Register a prediction model."""
        self.models[model_id] = model
        logger.info(f"Registered model: {model_id}")
    
    async def train_sales_forecaster(self, data: pd.DataFrame, 
                                   config: Optional[ForecastConfig] = None) -> str:
        """Train sales forecasting model."""
        
        try:
            config = config or ForecastConfig()
            model_id = f"sales_forecaster_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            forecaster = SalesForecaster(config)
            forecaster.fit(data)
            
            await self.register_model(model_id, forecaster)
            
            # Store model metadata
            await self._store_model_metadata(model_id, "sales_forecaster", config.__dict__)
            
            logger.info(f"Sales forecaster trained: {model_id}")
            return model_id
            
        except Exception as e:
            logger.error(f"Error training sales forecaster: {str(e)}")
            raise
    
    async def train_customer_segmentation(self, data: pd.DataFrame,
                                        config: Optional[SegmentationConfig] = None) -> str:
        """Train customer segmentation model."""
        
        try:
            config = config or SegmentationConfig()
            model_id = f"customer_segmentation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            segmentation = CustomerSegmentation(config)
            segmentation.fit(data)
            
            await self.register_model(model_id, segmentation)
            
            # Store model metadata
            await self._store_model_metadata(model_id, "customer_segmentation", config.__dict__)
            
            logger.info(f"Customer segmentation trained: {model_id}")
            return model_id
            
        except Exception as e:
            logger.error(f"Error training customer segmentation: {str(e)}")
            raise
    
    async def train_anomaly_detector(self, data: pd.DataFrame,
                                   config: Optional[AnomalyConfig] = None) -> str:
        """Train anomaly detection model."""
        
        try:
            config = config or AnomalyConfig()
            model_id = f"anomaly_detector_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            detector = AnomalyDetector(config)
            detector.fit(data)
            
            await self.register_model(model_id, detector)
            
            # Store model metadata
            await self._store_model_metadata(model_id, "anomaly_detector", config.__dict__)
            
            logger.info(f"Anomaly detector trained: {model_id}")
            return model_id
            
        except Exception as e:
            logger.error(f"Error training anomaly detector: {str(e)}")
            raise
    
    async def predict(self, model_id: str, data: pd.DataFrame, 
                     **kwargs) -> PredictionResult:
        """Make prediction using specified model."""
        
        try:
            if model_id not in self.models:
                raise ValueError(f"Model {model_id} not found")
            
            model = self.models[model_id]
            result = model.predict(data, **kwargs)
            
            # Store prediction result
            await self._store_prediction_result(result)
            
            # Update metrics
            self.metrics_collector.increment_counter(
                "predictive_analytics_predictions_total",
                tags={"model_id": model_id, "prediction_type": result.prediction_type.value}
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error making prediction with {model_id}: {str(e)}")
            raise
    
    async def get_prediction_history(self, model_id: Optional[str] = None,
                                   prediction_type: Optional[PredictionType] = None,
                                   limit: int = 100) -> List[Dict[str, Any]]:
        """Get prediction history."""
        
        try:
            query = self.supabase.table('prediction_results').select('*')
            
            if model_id:
                query = query.eq('model_id', model_id)
            
            if prediction_type:
                query = query.eq('prediction_type', prediction_type.value)
            
            result = query.order('timestamp', desc=True).limit(limit).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error getting prediction history: {str(e)}")
            return []
    
    async def _store_model_metadata(self, model_id: str, model_type: str, config: Dict[str, Any]):
        """Store model metadata."""
        
        try:
            metadata = {
                "model_id": model_id,
                "model_type": model_type,
                "config": config,
                "created_at": datetime.utcnow().isoformat(),
                "status": "active"
            }
            
            self.supabase.table('predictive_models').insert(metadata).execute()
            
        except Exception as e:
            logger.error(f"Error storing model metadata: {str(e)}")
    
    async def _store_prediction_result(self, result: PredictionResult):
        """Store prediction result."""
        
        try:
            result_data = {
                "prediction_id": result.prediction_id,
                "prediction_type": result.prediction_type.value,
                "timestamp": result.timestamp.isoformat(),
                "model_used": result.model_used,
                "confidence_score": result.confidence_score,
                "result_data": {
                    "forecast_values": result.forecast_values,
                    "confidence_intervals": result.confidence_intervals,
                    "forecast_dates": [d.isoformat() if isinstance(d, datetime) else str(d) 
                                     for d in (result.forecast_dates or [])],
                    "cluster_assignments": result.cluster_assignments,
                    "cluster_centers": result.cluster_centers,
                    "silhouette_score": result.silhouette_score,
                    "anomaly_scores": result.anomaly_scores,
                    "is_anomaly": result.is_anomaly,
                    "anomaly_severity": result.anomaly_severity,
                    "feature_importance": result.feature_importance,
                    "model_performance": result.model_performance
                },
                "metadata": result.metadata
            }
            
            self.supabase.table('prediction_results').insert(result_data).execute()
            
        except Exception as e:
            logger.error(f"Error storing prediction result: {str(e)}")


# Factory functions for easy model creation
def create_sales_forecaster(data: pd.DataFrame, **kwargs) -> SalesForecaster:
    """Create and train a sales forecaster."""
    config = ForecastConfig(**kwargs)
    forecaster = SalesForecaster(config)
    forecaster.fit(data)
    return forecaster


def create_customer_segmentation(data: pd.DataFrame, **kwargs) -> CustomerSegmentation:
    """Create and train customer segmentation."""
    config = SegmentationConfig(**kwargs)
    segmentation = CustomerSegmentation(config)
    segmentation.fit(data)
    return segmentation


def create_anomaly_detector(data: pd.DataFrame, **kwargs) -> AnomalyDetector:
    """Create and train anomaly detector."""
    config = AnomalyConfig(**kwargs)
    detector = AnomalyDetector(config)
    detector.fit(data)
    return detector