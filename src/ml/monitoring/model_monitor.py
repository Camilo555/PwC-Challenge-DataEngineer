"""
Enhanced ML Model Monitoring and Drift Detection

Comprehensive monitoring system for ML models in production with
automated drift detection, performance tracking, alerting, and enterprise infrastructure integration
including Redis caching, RabbitMQ messaging, and Kafka streaming.
"""

import hashlib
import logging
import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
from enum import Enum
import json
import joblib
from pathlib import Path
from scipy import stats
from scipy.spatial.distance import jensenshannon
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import plotly.graph_objects as go
import plotly.express as px

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.core.caching.redis_cache_manager import get_cache_manager
from src.messaging.enterprise_rabbitmq_manager import get_rabbitmq_manager, EnterpriseMessage, QueueType, MessagePriority
from src.streaming.kafka_manager import KafkaManager, StreamingTopic
from src.data_access.supabase_client import get_supabase_client

logger = get_logger(__name__)
settings = get_settings()


class DriftType(Enum):
    """Types of drift detection."""
    FEATURE_DRIFT = "feature_drift"
    PREDICTION_DRIFT = "prediction_drift"
    CONCEPT_DRIFT = "concept_drift"
    LABEL_DRIFT = "label_drift"


class AlertSeverity(Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class DriftConfig:
    """Enhanced configuration for drift detection with infrastructure integration."""
    
    # Thresholds
    feature_drift_threshold: float = 0.1
    prediction_drift_threshold: float = 0.1
    concept_drift_threshold: float = 0.05
    label_drift_threshold: float = 0.1
    
    # Detection methods
    statistical_test: str = "ks"  # "ks", "chi2", "js"
    reference_window_size: int = 1000
    detection_window_size: int = 100
    
    # Monitoring frequency
    check_frequency_minutes: int = 60
    
    # Alerting
    enable_alerts: bool = True
    alert_channels: List[str] = field(default_factory=lambda: ["email", "slack"])
    
    # Infrastructure integration
    enable_caching: bool = True
    cache_model_metrics: bool = True
    cache_drift_data: bool = True
    cache_ttl_seconds: int = 3600  # 1 hour
    
    enable_messaging: bool = True
    publish_drift_alerts: bool = True
    publish_performance_alerts: bool = True
    
    enable_streaming: bool = True
    stream_real_time_metrics: bool = True
    stream_drift_detection: bool = True
    
    # Real-time monitoring
    enable_real_time_monitoring: bool = True
    real_time_drift_threshold: float = 0.05  # More sensitive for real-time


@dataclass
class DriftResult:
    """Result of drift detection."""
    
    drift_type: DriftType
    feature_name: Optional[str]
    drift_score: float
    p_value: Optional[float]
    threshold: float
    is_drift_detected: bool
    severity: AlertSeverity
    timestamp: datetime = field(default_factory=datetime.utcnow)
    reference_stats: Optional[Dict[str, float]] = None
    current_stats: Optional[Dict[str, float]] = None
    

@dataclass
class ModelPerformanceMetrics:
    """Model performance metrics."""
    
    model_id: str
    timestamp: datetime
    sample_size: int
    
    # Classification metrics
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    
    # Regression metrics
    rmse: Optional[float] = None
    mae: Optional[float] = None
    r2_score: Optional[float] = None
    
    # Custom metrics
    custom_metrics: Dict[str, float] = field(default_factory=dict)


class BaseDriftDetector(ABC):
    """Base class for drift detectors."""
    
    def __init__(self, config: DriftConfig):
        self.config = config
        
    @abstractmethod
    def detect_drift(self, reference_data: pd.Series, current_data: pd.Series) -> DriftResult:
        """Detect drift between reference and current data."""
        pass


class FeatureDriftDetector(BaseDriftDetector):
    """Feature drift detector using statistical tests."""
    
    def detect_drift(self, reference_data: pd.Series, current_data: pd.Series,
                    feature_name: str) -> DriftResult:
        """Detect feature drift using statistical tests."""
        
        try:
            # Handle missing values
            ref_clean = reference_data.dropna()
            curr_clean = current_data.dropna()
            
            if len(ref_clean) == 0 or len(curr_clean) == 0:
                return DriftResult(
                    drift_type=DriftType.FEATURE_DRIFT,
                    feature_name=feature_name,
                    drift_score=0.0,
                    p_value=1.0,
                    threshold=self.config.feature_drift_threshold,
                    is_drift_detected=False,
                    severity=AlertSeverity.LOW
                )
            
            # Determine data type
            if pd.api.types.is_numeric_dtype(reference_data):
                return self._detect_numerical_drift(ref_clean, curr_clean, feature_name)
            else:
                return self._detect_categorical_drift(ref_clean, curr_clean, feature_name)
                
        except Exception as e:
            logger.error(f"Error detecting drift for feature {feature_name}: {str(e)}")
            return DriftResult(
                drift_type=DriftType.FEATURE_DRIFT,
                feature_name=feature_name,
                drift_score=0.0,
                p_value=1.0,
                threshold=self.config.feature_drift_threshold,
                is_drift_detected=False,
                severity=AlertSeverity.LOW
            )
    
    def _detect_numerical_drift(self, ref_data: pd.Series, curr_data: pd.Series,
                              feature_name: str) -> DriftResult:
        """Detect drift in numerical features."""
        
        # Calculate statistics
        ref_stats = {
            "mean": ref_data.mean(),
            "std": ref_data.std(),
            "min": ref_data.min(),
            "max": ref_data.max(),
            "q25": ref_data.quantile(0.25),
            "q50": ref_data.quantile(0.50),
            "q75": ref_data.quantile(0.75)
        }
        
        curr_stats = {
            "mean": curr_data.mean(),
            "std": curr_data.std(),
            "min": curr_data.min(),
            "max": curr_data.max(),
            "q25": curr_data.quantile(0.25),
            "q50": curr_data.quantile(0.50),
            "q75": curr_data.quantile(0.75)
        }
        
        # Perform statistical test
        if self.config.statistical_test == "ks":
            # Kolmogorov-Smirnov test
            statistic, p_value = stats.ks_2samp(ref_data, curr_data)
            drift_score = statistic
            
        elif self.config.statistical_test == "js":
            # Jensen-Shannon divergence
            # Create histograms
            min_val = min(ref_data.min(), curr_data.min())
            max_val = max(ref_data.max(), curr_data.max())
            bins = np.linspace(min_val, max_val, 50)
            
            ref_hist, _ = np.histogram(ref_data, bins=bins, density=True)
            curr_hist, _ = np.histogram(curr_data, bins=bins, density=True)
            
            # Normalize to probabilities
            ref_hist = ref_hist / ref_hist.sum()
            curr_hist = curr_hist / curr_hist.sum()
            
            # Add small epsilon to avoid zero probabilities
            epsilon = 1e-8
            ref_hist = ref_hist + epsilon
            curr_hist = curr_hist + epsilon
            ref_hist = ref_hist / ref_hist.sum()
            curr_hist = curr_hist / curr_hist.sum()
            
            drift_score = jensenshannon(ref_hist, curr_hist)
            p_value = None
            
        else:  # Default to KS test
            statistic, p_value = stats.ks_2samp(ref_data, curr_data)
            drift_score = statistic
        
        # Determine severity
        if drift_score > self.config.feature_drift_threshold * 2:
            severity = AlertSeverity.CRITICAL
        elif drift_score > self.config.feature_drift_threshold * 1.5:
            severity = AlertSeverity.HIGH
        elif drift_score > self.config.feature_drift_threshold:
            severity = AlertSeverity.MEDIUM
        else:
            severity = AlertSeverity.LOW
        
        is_drift_detected = drift_score > self.config.feature_drift_threshold
        
        return DriftResult(
            drift_type=DriftType.FEATURE_DRIFT,
            feature_name=feature_name,
            drift_score=drift_score,
            p_value=p_value,
            threshold=self.config.feature_drift_threshold,
            is_drift_detected=is_drift_detected,
            severity=severity,
            reference_stats=ref_stats,
            current_stats=curr_stats
        )
    
    def _detect_categorical_drift(self, ref_data: pd.Series, curr_data: pd.Series,
                                feature_name: str) -> DriftResult:
        """Detect drift in categorical features."""
        
        # Calculate distributions
        ref_counts = ref_data.value_counts(normalize=True)
        curr_counts = curr_data.value_counts(normalize=True)
        
        # Get all categories
        all_categories = set(ref_counts.index) | set(curr_counts.index)
        
        # Create aligned probability vectors
        ref_probs = []
        curr_probs = []
        
        for cat in all_categories:
            ref_probs.append(ref_counts.get(cat, 0))
            curr_probs.append(curr_counts.get(cat, 0))
        
        ref_probs = np.array(ref_probs)
        curr_probs = np.array(curr_probs)
        
        # Chi-square test for categorical data
        if self.config.statistical_test == "chi2":
            # Observed frequencies
            ref_freq = ref_data.value_counts()
            curr_freq = curr_data.value_counts()
            
            # Create contingency table
            contingency = []
            for cat in all_categories:
                contingency.append([
                    ref_freq.get(cat, 0),
                    curr_freq.get(cat, 0)
                ])
            
            contingency = np.array(contingency)
            
            try:
                chi2, p_value, _, _ = stats.chi2_contingency(contingency)
                drift_score = chi2 / len(all_categories)  # Normalized chi-square
            except:
                drift_score = 0.0
                p_value = 1.0
        else:
            # Jensen-Shannon divergence for categorical
            # Add small epsilon
            epsilon = 1e-8
            ref_probs = ref_probs + epsilon
            curr_probs = curr_probs + epsilon
            ref_probs = ref_probs / ref_probs.sum()
            curr_probs = curr_probs / curr_probs.sum()
            
            drift_score = jensenshannon(ref_probs, curr_probs)
            p_value = None
        
        # Statistics
        ref_stats = {
            "unique_values": len(ref_counts),
            "most_frequent": ref_counts.index[0] if len(ref_counts) > 0 else None,
            "most_frequent_freq": ref_counts.iloc[0] if len(ref_counts) > 0 else 0
        }
        
        curr_stats = {
            "unique_values": len(curr_counts),
            "most_frequent": curr_counts.index[0] if len(curr_counts) > 0 else None,
            "most_frequent_freq": curr_counts.iloc[0] if len(curr_counts) > 0 else 0
        }
        
        # Determine severity
        if drift_score > self.config.feature_drift_threshold * 2:
            severity = AlertSeverity.CRITICAL
        elif drift_score > self.config.feature_drift_threshold * 1.5:
            severity = AlertSeverity.HIGH
        elif drift_score > self.config.feature_drift_threshold:
            severity = AlertSeverity.MEDIUM
        else:
            severity = AlertSeverity.LOW
        
        is_drift_detected = drift_score > self.config.feature_drift_threshold
        
        return DriftResult(
            drift_type=DriftType.FEATURE_DRIFT,
            feature_name=feature_name,
            drift_score=drift_score,
            p_value=p_value,
            threshold=self.config.feature_drift_threshold,
            is_drift_detected=is_drift_detected,
            severity=severity,
            reference_stats=ref_stats,
            current_stats=curr_stats
        )


class PredictionDriftDetector(BaseDriftDetector):
    """Prediction drift detector."""
    
    def detect_drift(self, reference_predictions: pd.Series, 
                    current_predictions: pd.Series) -> DriftResult:
        """Detect drift in model predictions."""
        
        try:
            # Remove missing values
            ref_clean = reference_predictions.dropna()
            curr_clean = current_predictions.dropna()
            
            if len(ref_clean) == 0 or len(curr_clean) == 0:
                return DriftResult(
                    drift_type=DriftType.PREDICTION_DRIFT,
                    feature_name=None,
                    drift_score=0.0,
                    p_value=1.0,
                    threshold=self.config.prediction_drift_threshold,
                    is_drift_detected=False,
                    severity=AlertSeverity.LOW
                )
            
            # Calculate statistics
            ref_stats = {
                "mean": ref_clean.mean(),
                "std": ref_clean.std(),
                "min": ref_clean.min(),
                "max": ref_clean.max()
            }
            
            curr_stats = {
                "mean": curr_clean.mean(),
                "std": curr_clean.std(),
                "min": curr_clean.min(),
                "max": curr_clean.max()
            }
            
            # Kolmogorov-Smirnov test
            statistic, p_value = stats.ks_2samp(ref_clean, curr_clean)
            drift_score = statistic
            
            # Determine severity
            if drift_score > self.config.prediction_drift_threshold * 2:
                severity = AlertSeverity.CRITICAL
            elif drift_score > self.config.prediction_drift_threshold * 1.5:
                severity = AlertSeverity.HIGH
            elif drift_score > self.config.prediction_drift_threshold:
                severity = AlertSeverity.MEDIUM
            else:
                severity = AlertSeverity.LOW
            
            is_drift_detected = drift_score > self.config.prediction_drift_threshold
            
            return DriftResult(
                drift_type=DriftType.PREDICTION_DRIFT,
                feature_name=None,
                drift_score=drift_score,
                p_value=p_value,
                threshold=self.config.prediction_drift_threshold,
                is_drift_detected=is_drift_detected,
                severity=severity,
                reference_stats=ref_stats,
                current_stats=curr_stats
            )
            
        except Exception as e:
            logger.error(f"Error detecting prediction drift: {str(e)}")
            return DriftResult(
                drift_type=DriftType.PREDICTION_DRIFT,
                feature_name=None,
                drift_score=0.0,
                p_value=1.0,
                threshold=self.config.prediction_drift_threshold,
                is_drift_detected=False,
                severity=AlertSeverity.LOW
            )


class ModelMonitor:
    """Enhanced comprehensive ML model monitoring system with infrastructure integration."""
    
    def __init__(self, model_id: str, config: DriftConfig):
        self.model_id = model_id
        self.config = config
        self.supabase = get_supabase_client()
        self.metrics_collector = MetricsCollector()
        
        # Initialize detectors
        self.feature_drift_detector = FeatureDriftDetector(config)
        self.prediction_drift_detector = PredictionDriftDetector(config)
        
        # Reference data cache
        self.reference_data: Optional[pd.DataFrame] = None
        self.reference_predictions: Optional[pd.Series] = None
        self.reference_labels: Optional[pd.Series] = None
        
        # Infrastructure integration
        self.cache_manager = None
        self.rabbitmq_manager = None
        self.kafka_manager = None
        
        # Initialize infrastructure
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
                logger.info(f"Cache manager initialized for model monitor {self.model_id}")
            
            # Initialize messaging
            if self.config.enable_messaging:
                self.rabbitmq_manager = get_rabbitmq_manager()
                logger.info(f"RabbitMQ manager initialized for model monitor {self.model_id}")
            
            # Initialize streaming
            if self.config.enable_streaming:
                self.kafka_manager = KafkaManager()
                logger.info(f"Kafka manager initialized for model monitor {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error initializing infrastructure for model {self.model_id}: {e}")
    
    async def _publish_drift_alert(self, drift_result: DriftResult):
        """Publish drift alert to RabbitMQ."""
        if not self.rabbitmq_manager or not self.config.enable_messaging or not self.config.publish_drift_alerts:
            return
        
        try:
            message = EnterpriseMessage(
                queue_type=QueueType.ML_MONITORING,
                message_type="drift_detected",
                payload={
                    "model_id": self.model_id,
                    "drift_type": drift_result.drift_type.value,
                    "feature_name": drift_result.feature_name,
                    "drift_score": drift_result.drift_score,
                    "p_value": drift_result.p_value,
                    "threshold": drift_result.threshold,
                    "severity": drift_result.severity.value,
                    "timestamp": drift_result.timestamp.isoformat(),
                    "reference_stats": drift_result.reference_stats,
                    "current_stats": drift_result.current_stats
                }
            )
            
            # Set priority based on severity
            if drift_result.severity == AlertSeverity.CRITICAL:
                message.metadata.priority = MessagePriority.CRITICAL
            elif drift_result.severity == AlertSeverity.HIGH:
                message.metadata.priority = MessagePriority.HIGH
            else:
                message.metadata.priority = MessagePriority.NORMAL
            
            message.metadata.source_service = "model_monitor"
            
            await self.rabbitmq_manager.publish_message_async(message)
            logger.debug(f"Drift alert published for model {self.model_id}: {drift_result.drift_type.value}")
            
        except Exception as e:
            logger.error(f"Error publishing drift alert: {e}")
    
    async def _publish_performance_alert(self, metrics: 'ModelPerformanceMetrics', alert_type: str, alert_data: Dict[str, Any]):
        """Publish performance alert to RabbitMQ."""
        if not self.rabbitmq_manager or not self.config.enable_messaging or not self.config.publish_performance_alerts:
            return
        
        try:
            message = EnterpriseMessage(
                queue_type=QueueType.ML_MONITORING,
                message_type="performance_alert",
                payload={
                    "model_id": self.model_id,
                    "alert_type": alert_type,
                    "alert_data": alert_data,
                    "timestamp": metrics.timestamp.isoformat(),
                    "sample_size": metrics.sample_size,
                    "metrics": {
                        "accuracy": metrics.accuracy,
                        "precision": metrics.precision,
                        "recall": metrics.recall,
                        "f1_score": metrics.f1_score,
                        "rmse": metrics.rmse,
                        "mae": metrics.mae,
                        "r2_score": metrics.r2_score,
                        "custom_metrics": metrics.custom_metrics
                    }
                }
            )
            message.metadata.priority = MessagePriority.HIGH
            message.metadata.source_service = "model_monitor"
            
            await self.rabbitmq_manager.publish_message_async(message)
            logger.debug(f"Performance alert published for model {self.model_id}: {alert_type}")
            
        except Exception as e:
            logger.error(f"Error publishing performance alert: {e}")
    
    async def _stream_drift_detection(self, drift_result: DriftResult):
        """Stream drift detection results to Kafka."""
        if not self.kafka_manager or not self.config.enable_streaming or not self.config.stream_drift_detection:
            return
        
        try:
            success = self.kafka_manager.produce_message(
                topic=StreamingTopic.METRICS,
                message={
                    "event_type": "ml_drift_detected",
                    "model_id": self.model_id,
                    "drift_type": drift_result.drift_type.value,
                    "feature_name": drift_result.feature_name,
                    "drift_score": drift_result.drift_score,
                    "p_value": drift_result.p_value,
                    "threshold": drift_result.threshold,
                    "is_drift_detected": drift_result.is_drift_detected,
                    "severity": drift_result.severity.value,
                    "timestamp": drift_result.timestamp.isoformat(),
                    "reference_stats": drift_result.reference_stats,
                    "current_stats": drift_result.current_stats
                },
                key=f"{self.model_id}_{drift_result.feature_name or 'global'}"
            )
            
            if success:
                logger.debug(f"Drift detection streamed for model {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error streaming drift detection: {e}")
    
    async def _stream_real_time_metrics(self, metrics_data: Dict[str, Any]):
        """Stream real-time metrics to Kafka."""
        if not self.kafka_manager or not self.config.enable_streaming or not self.config.stream_real_time_metrics:
            return
        
        try:
            success = self.kafka_manager.produce_message(
                topic=StreamingTopic.METRICS,
                message={
                    "event_type": "ml_real_time_metrics",
                    "model_id": self.model_id,
                    "metrics": metrics_data,
                    "timestamp": datetime.utcnow().isoformat()
                },
                key=self.model_id
            )
            
            if success:
                logger.debug(f"Real-time metrics streamed for model {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error streaming real-time metrics: {e}")
    
    async def _cache_model_metrics(self, metrics: 'ModelPerformanceMetrics'):
        """Cache model performance metrics."""
        if not self.cache_manager or not self.config.enable_caching or not self.config.cache_model_metrics:
            return
        
        try:
            cache_key = f"model_metrics:{self.model_id}"
            
            metrics_data = {
                "model_id": metrics.model_id,
                "timestamp": metrics.timestamp.isoformat(),
                "sample_size": metrics.sample_size,
                "accuracy": metrics.accuracy,
                "precision": metrics.precision,
                "recall": metrics.recall,
                "f1_score": metrics.f1_score,
                "rmse": metrics.rmse,
                "mae": metrics.mae,
                "r2_score": metrics.r2_score,
                "custom_metrics": metrics.custom_metrics
            }
            
            await self.cache_manager.set(
                cache_key,
                metrics_data,
                ttl=self.config.cache_ttl_seconds,
                namespace="ml_monitoring"
            )
            
            # Also maintain a list of recent metrics
            recent_metrics_key = f"model_metrics_recent:{self.model_id}"
            await self.cache_manager.lpush(recent_metrics_key, metrics_data, namespace="ml_monitoring")
            await self.cache_manager.ltrim(recent_metrics_key, 0, 99, namespace="ml_monitoring")  # Keep last 100
            
            logger.debug(f"Model metrics cached for model {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error caching model metrics: {e}")
    
    async def _cache_drift_data(self, drift_results: List[DriftResult]):
        """Cache drift detection data."""
        if not self.cache_manager or not self.config.enable_caching or not self.config.cache_drift_data:
            return
        
        try:
            cache_key = f"model_drift:{self.model_id}"
            
            drift_data = []
            for result in drift_results:
                drift_data.append({
                    "drift_type": result.drift_type.value,
                    "feature_name": result.feature_name,
                    "drift_score": result.drift_score,
                    "p_value": result.p_value,
                    "threshold": result.threshold,
                    "is_drift_detected": result.is_drift_detected,
                    "severity": result.severity.value,
                    "timestamp": result.timestamp.isoformat(),
                    "reference_stats": result.reference_stats,
                    "current_stats": result.current_stats
                })
            
            await self.cache_manager.set(
                cache_key,
                drift_data,
                ttl=self.config.cache_ttl_seconds,
                namespace="ml_monitoring"
            )
            
            logger.debug(f"Drift data cached for model {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error caching drift data: {e}")
    
    async def _check_performance_degradation(self, current_metrics: 'ModelPerformanceMetrics', problem_type: str):
        """Check for performance degradation and publish alerts."""
        try:
            # Get recent metrics from cache for comparison
            if not self.cache_manager:
                return
            
            recent_metrics_key = f"model_metrics_recent:{self.model_id}"
            recent_metrics_data = await self.cache_manager.lrange(recent_metrics_key, 1, 10, namespace="ml_monitoring")  # Skip current, get last 10
            
            if not recent_metrics_data or len(recent_metrics_data) < 3:
                return  # Need at least 3 historical points
            
            # Calculate baseline performance
            if problem_type == "classification":
                if current_metrics.accuracy is None:
                    return
                
                historical_accuracies = [m.get("accuracy") for m in recent_metrics_data if m.get("accuracy") is not None]
                if not historical_accuracies:
                    return
                
                baseline_accuracy = sum(historical_accuracies) / len(historical_accuracies)
                
                # Check for significant degradation (more than 5% relative decrease)
                if current_metrics.accuracy < baseline_accuracy * 0.95:
                    await self._publish_performance_alert(
                        current_metrics,
                        "performance_degradation",
                        {
                            "metric": "accuracy",
                            "current_value": current_metrics.accuracy,
                            "baseline_value": baseline_accuracy,
                            "degradation_percent": ((baseline_accuracy - current_metrics.accuracy) / baseline_accuracy) * 100
                        }
                    )
            
            else:  # regression
                if current_metrics.r2_score is None:
                    return
                
                historical_r2_scores = [m.get("r2_score") for m in recent_metrics_data if m.get("r2_score") is not None]
                if not historical_r2_scores:
                    return
                
                baseline_r2 = sum(historical_r2_scores) / len(historical_r2_scores)
                
                # Check for significant degradation
                if current_metrics.r2_score < baseline_r2 * 0.95:
                    await self._publish_performance_alert(
                        current_metrics,
                        "performance_degradation",
                        {
                            "metric": "r2_score",
                            "current_value": current_metrics.r2_score,
                            "baseline_value": baseline_r2,
                            "degradation_percent": ((baseline_r2 - current_metrics.r2_score) / baseline_r2) * 100
                        }
                    )
            
        except Exception as e:
            logger.error(f"Error checking performance degradation: {e}")
        
    async def set_reference_data(self, features: pd.DataFrame, 
                               predictions: Optional[pd.Series] = None,
                               labels: Optional[pd.Series] = None):
        """Set reference data for drift detection."""
        
        try:
            self.reference_data = features.copy()
            if predictions is not None:
                self.reference_predictions = predictions.copy()
            if labels is not None:
                self.reference_labels = labels.copy()
            
            # Store reference data
            reference_data = {
                "model_id": self.model_id,
                "timestamp": datetime.utcnow().isoformat(),
                "features_stats": self._calculate_features_stats(features),
                "prediction_stats": self._calculate_prediction_stats(predictions) if predictions is not None else None,
                "label_stats": self._calculate_prediction_stats(labels) if labels is not None else None
            }
            
            # Store in database
            self.supabase.table('model_reference_data').insert(reference_data).execute()
            
            # Cache in Redis
            await self.redis_client.setex(
                f"reference_data:{self.model_id}",
                86400,  # 24 hours
                json.dumps(reference_data, default=str)
            )
            
            logger.info(f"Reference data set for model {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error setting reference data for model {self.model_id}: {str(e)}")
    
    async def monitor_batch(self, features: pd.DataFrame,
                          predictions: Optional[pd.Series] = None,
                          labels: Optional[pd.Series] = None) -> List[DriftResult]:
        """Monitor a batch of data for drift."""
        
        try:
            drift_results = []
            
            # Ensure reference data is available
            if self.reference_data is None:
                await self._load_reference_data()
            
            if self.reference_data is None:
                logger.warning(f"No reference data available for model {self.model_id}")
                return drift_results
            
            # Feature drift detection
            for feature in features.columns:
                if feature in self.reference_data.columns:
                    drift_result = self.feature_drift_detector.detect_drift(
                        self.reference_data[feature],
                        features[feature],
                        feature
                    )
                    drift_results.append(drift_result)
            
            # Prediction drift detection
            if predictions is not None and self.reference_predictions is not None:
                prediction_drift = self.prediction_drift_detector.detect_drift(
                    self.reference_predictions,
                    predictions
                )
                drift_results.append(prediction_drift)
            
            # Store results
            await self._store_drift_results(drift_results)
            
            # Cache drift data
            await self._cache_drift_data(drift_results)
            
            # Generate alerts for significant drift
            await self._process_drift_alerts(drift_results)
            
            # Publish drift alerts and stream detection results
            for drift_result in drift_results:
                if drift_result.is_drift_detected:
                    await self._publish_drift_alert(drift_result)
                    await self._stream_drift_detection(drift_result)
            
            # Stream real-time drift metrics
            drift_metrics = {
                "total_features_checked": len(drift_results),
                "drift_detected_count": sum(1 for r in drift_results if r.is_drift_detected),
                "avg_drift_score": sum(r.drift_score for r in drift_results) / len(drift_results) if drift_results else 0,
                "critical_alerts": sum(1 for r in drift_results if r.severity == AlertSeverity.CRITICAL),
                "high_alerts": sum(1 for r in drift_results if r.severity == AlertSeverity.HIGH)
            }
            await self._stream_real_time_metrics(drift_metrics)
            
            # Update metrics
            await self._update_drift_metrics(drift_results)
            
            return drift_results
            
        except Exception as e:
            logger.error(f"Error monitoring batch for model {self.model_id}: {str(e)}")
            return []
    
    async def calculate_performance_metrics(self, predictions: pd.Series,
                                          labels: pd.Series,
                                          problem_type: str = "classification") -> ModelPerformanceMetrics:
        """Calculate model performance metrics."""
        
        try:
            metrics = ModelPerformanceMetrics(
                model_id=self.model_id,
                timestamp=datetime.utcnow(),
                sample_size=len(predictions)
            )
            
            if problem_type == "classification":
                # Classification metrics
                metrics.accuracy = accuracy_score(labels, predictions)
                metrics.precision = precision_score(labels, predictions, average='weighted', zero_division=0)
                metrics.recall = recall_score(labels, predictions, average='weighted', zero_division=0)
                metrics.f1_score = f1_score(labels, predictions, average='weighted', zero_division=0)
                
            else:  # regression
                # Regression metrics
                metrics.rmse = np.sqrt(mean_squared_error(labels, predictions))
                metrics.mae = mean_absolute_error(labels, predictions)
                metrics.r2_score = r2_score(labels, predictions)
            
            # Store metrics
            metrics_data = {
                "model_id": self.model_id,
                "timestamp": metrics.timestamp.isoformat(),
                "sample_size": metrics.sample_size,
                "accuracy": metrics.accuracy,
                "precision": metrics.precision,
                "recall": metrics.recall,
                "f1_score": metrics.f1_score,
                "rmse": metrics.rmse,
                "mae": metrics.mae,
                "r2_score": metrics.r2_score,
                "custom_metrics": metrics.custom_metrics
            }
            
            self.supabase.table('model_performance_metrics').insert(metrics_data).execute()
            
            # Cache model metrics
            await self._cache_model_metrics(metrics)
            
            # Check for performance degradation and publish alerts
            await self._check_performance_degradation(metrics, problem_type)
            
            # Stream performance metrics
            performance_metrics_data = {
                "sample_size": metrics.sample_size,
                "problem_type": problem_type
            }
            
            if problem_type == "classification":
                performance_metrics_data.update({
                    "accuracy": metrics.accuracy,
                    "precision": metrics.precision,
                    "recall": metrics.recall,
                    "f1_score": metrics.f1_score
                })
            else:
                performance_metrics_data.update({
                    "rmse": metrics.rmse,
                    "mae": metrics.mae,
                    "r2_score": metrics.r2_score
                })
            
            await self._stream_real_time_metrics(performance_metrics_data)
            
            # Update real-time metrics
            await self._update_performance_metrics(metrics, problem_type)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics for model {self.model_id}: {str(e)}")
            return ModelPerformanceMetrics(
                model_id=self.model_id,
                timestamp=datetime.utcnow(),
                sample_size=0
            )
    
    async def generate_monitoring_report(self, start_date: datetime, 
                                       end_date: datetime) -> Dict[str, Any]:
        """Generate comprehensive monitoring report."""
        
        try:
            # Get drift results
            drift_results = await self._get_drift_results(start_date, end_date)
            
            # Get performance metrics
            performance_metrics = await self._get_performance_metrics(start_date, end_date)
            
            # Calculate summary statistics
            summary = {
                "total_drift_checks": len(drift_results),
                "drift_detected_count": sum(1 for r in drift_results if r.get("is_drift_detected", False)),
                "critical_alerts": sum(1 for r in drift_results if r.get("severity") == "critical"),
                "avg_performance": self._calculate_avg_performance(performance_metrics)
            }
            
            # Generate visualizations
            charts = await self._generate_monitoring_charts(drift_results, performance_metrics)
            
            report = {
                "model_id": self.model_id,
                "report_period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "summary": summary,
                "drift_results": drift_results,
                "performance_metrics": performance_metrics,
                "charts": charts,
                "recommendations": self._generate_recommendations(drift_results, performance_metrics)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating monitoring report for model {self.model_id}: {str(e)}")
            return {}
    
    def _calculate_features_stats(self, features: pd.DataFrame) -> Dict[str, Any]:
        """Calculate statistics for features."""
        stats = {}
        
        for column in features.columns:
            if pd.api.types.is_numeric_dtype(features[column]):
                stats[column] = {
                    "type": "numerical",
                    "mean": float(features[column].mean()),
                    "std": float(features[column].std()),
                    "min": float(features[column].min()),
                    "max": float(features[column].max()),
                    "q25": float(features[column].quantile(0.25)),
                    "q50": float(features[column].quantile(0.50)),
                    "q75": float(features[column].quantile(0.75))
                }
            else:
                value_counts = features[column].value_counts()
                stats[column] = {
                    "type": "categorical",
                    "unique_values": int(features[column].nunique()),
                    "most_frequent": value_counts.index[0] if len(value_counts) > 0 else None,
                    "most_frequent_freq": float(value_counts.iloc[0]) if len(value_counts) > 0 else 0
                }
        
        return stats
    
    def _calculate_prediction_stats(self, predictions: pd.Series) -> Dict[str, float]:
        """Calculate statistics for predictions."""
        return {
            "mean": float(predictions.mean()),
            "std": float(predictions.std()),
            "min": float(predictions.min()),
            "max": float(predictions.max()),
            "q25": float(predictions.quantile(0.25)),
            "q50": float(predictions.quantile(0.50)),
            "q75": float(predictions.quantile(0.75))
        }
    
    async def _load_reference_data(self):
        """Load reference data from cache or database."""
        try:
            # Try Redis first
            cached_data = await self.redis_client.get(f"reference_data:{self.model_id}")
            if cached_data:
                # This would need proper reconstruction of DataFrames
                # For now, query from database
                pass
            
            # Query from database
            result = self.supabase.table('model_reference_data').select('*').eq('model_id', self.model_id).order('timestamp', desc=True).limit(1).execute()
            
            if result.data:
                # This would need proper reconstruction of DataFrames from stored stats
                logger.info(f"Loaded reference data for model {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error loading reference data for model {self.model_id}: {str(e)}")
    
    async def _store_drift_results(self, drift_results: List[DriftResult]):
        """Store drift detection results."""
        try:
            for result in drift_results:
                drift_data = {
                    "model_id": self.model_id,
                    "timestamp": result.timestamp.isoformat(),
                    "drift_type": result.drift_type.value,
                    "feature_name": result.feature_name,
                    "drift_score": result.drift_score,
                    "p_value": result.p_value,
                    "threshold": result.threshold,
                    "is_drift_detected": result.is_drift_detected,
                    "severity": result.severity.value,
                    "reference_stats": result.reference_stats,
                    "current_stats": result.current_stats
                }
                
                self.supabase.table('model_drift_results').insert(drift_data).execute()
        
        except Exception as e:
            logger.error(f"Error storing drift results: {str(e)}")
    
    async def _process_drift_alerts(self, drift_results: List[DriftResult]):
        """Process drift alerts."""
        try:
            for result in drift_results:
                if result.is_drift_detected and result.severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
                    alert_message = f"Drift detected in model {self.model_id}"
                    if result.feature_name:
                        alert_message += f" for feature {result.feature_name}"
                    alert_message += f". Drift score: {result.drift_score:.4f}, Severity: {result.severity.value}"
                    
                    await self.alert_manager.send_alert(
                        title="ML Model Drift Alert",
                        message=alert_message,
                        severity=result.severity.value,
                        channels=self.config.alert_channels,
                        metadata={
                            "model_id": self.model_id,
                            "drift_type": result.drift_type.value,
                            "feature_name": result.feature_name,
                            "drift_score": result.drift_score
                        }
                    )
        
        except Exception as e:
            logger.error(f"Error processing drift alerts: {str(e)}")
    
    async def _update_drift_metrics(self, drift_results: List[DriftResult]):
        """Update drift metrics for monitoring."""
        try:
            for result in drift_results:
                self.metrics_collector.record_gauge(
                    f"ml_model_drift_score",
                    result.drift_score,
                    tags={
                        "model_id": self.model_id,
                        "drift_type": result.drift_type.value,
                        "feature_name": result.feature_name or "all"
                    }
                )
                
                if result.is_drift_detected:
                    self.metrics_collector.increment_counter(
                        "ml_model_drift_detected_total",
                        tags={
                            "model_id": self.model_id,
                            "drift_type": result.drift_type.value,
                            "severity": result.severity.value
                        }
                    )
        
        except Exception as e:
            logger.error(f"Error updating drift metrics: {str(e)}")
    
    async def _update_performance_metrics(self, metrics: ModelPerformanceMetrics, problem_type: str):
        """Update performance metrics for monitoring."""
        try:
            if problem_type == "classification":
                if metrics.accuracy is not None:
                    self.metrics_collector.record_gauge(
                        "ml_model_accuracy",
                        metrics.accuracy,
                        tags={"model_id": self.model_id}
                    )
                
                if metrics.f1_score is not None:
                    self.metrics_collector.record_gauge(
                        "ml_model_f1_score",
                        metrics.f1_score,
                        tags={"model_id": self.model_id}
                    )
            
            else:  # regression
                if metrics.rmse is not None:
                    self.metrics_collector.record_gauge(
                        "ml_model_rmse",
                        metrics.rmse,
                        tags={"model_id": self.model_id}
                    )
                
                if metrics.r2_score is not None:
                    self.metrics_collector.record_gauge(
                        "ml_model_r2_score",
                        metrics.r2_score,
                        tags={"model_id": self.model_id}
                    )
        
        except Exception as e:
            logger.error(f"Error updating performance metrics: {str(e)}")
    
    async def _get_drift_results(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get drift results from database."""
        try:
            result = self.supabase.table('model_drift_results').select('*').eq('model_id', self.model_id).gte('timestamp', start_date.isoformat()).lte('timestamp', end_date.isoformat()).execute()
            
            return result.data if result.data else []
        
        except Exception as e:
            logger.error(f"Error getting drift results: {str(e)}")
            return []
    
    async def _get_performance_metrics(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get performance metrics from database."""
        try:
            result = self.supabase.table('model_performance_metrics').select('*').eq('model_id', self.model_id).gte('timestamp', start_date.isoformat()).lte('timestamp', end_date.isoformat()).execute()
            
            return result.data if result.data else []
        
        except Exception as e:
            logger.error(f"Error getting performance metrics: {str(e)}")
            return []
    
    def _calculate_avg_performance(self, performance_metrics: List[Dict[str, Any]]) -> Dict[str, float]:
        """Calculate average performance metrics."""
        if not performance_metrics:
            return {}
        
        avg_metrics = {}
        metric_names = ["accuracy", "precision", "recall", "f1_score", "rmse", "mae", "r2_score"]
        
        for metric_name in metric_names:
            values = [m.get(metric_name) for m in performance_metrics if m.get(metric_name) is not None]
            if values:
                avg_metrics[metric_name] = sum(values) / len(values)
        
        return avg_metrics
    
    async def _generate_monitoring_charts(self, drift_results: List[Dict[str, Any]], 
                                        performance_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate monitoring charts."""
        charts = {}
        
        try:
            # Drift score over time
            if drift_results:
                df_drift = pd.DataFrame(drift_results)
                df_drift['timestamp'] = pd.to_datetime(df_drift['timestamp'])
                
                fig_drift = px.line(df_drift, x='timestamp', y='drift_score', 
                                  color='feature_name', title='Feature Drift Over Time')
                charts['drift_over_time'] = fig_drift.to_json()
            
            # Performance metrics over time
            if performance_metrics:
                df_perf = pd.DataFrame(performance_metrics)
                df_perf['timestamp'] = pd.to_datetime(df_perf['timestamp'])
                
                # Choose appropriate metrics to plot
                if 'accuracy' in df_perf.columns:
                    fig_perf = px.line(df_perf, x='timestamp', y='accuracy', 
                                     title='Model Accuracy Over Time')
                elif 'rmse' in df_perf.columns:
                    fig_perf = px.line(df_perf, x='timestamp', y='rmse', 
                                     title='Model RMSE Over Time')
                else:
                    fig_perf = None
                
                if fig_perf:
                    charts['performance_over_time'] = fig_perf.to_json()
        
        except Exception as e:
            logger.error(f"Error generating charts: {str(e)}")
        
        return charts
    
    def _generate_recommendations(self, drift_results: List[Dict[str, Any]], 
                                performance_metrics: List[Dict[str, Any]]) -> List[str]:
        """Generate monitoring recommendations."""
        recommendations = []
        
        # Drift-based recommendations
        critical_drift_count = sum(1 for r in drift_results if r.get("severity") == "critical")
        if critical_drift_count > 0:
            recommendations.append(
                f"Critical drift detected in {critical_drift_count} features. "
                "Consider retraining the model with recent data."
            )
        
        high_drift_count = sum(1 for r in drift_results if r.get("severity") == "high")
        if high_drift_count > 2:
            recommendations.append(
                f"High drift detected in {high_drift_count} features. "
                "Monitor closely and prepare for model updates."
            )
        
        # Performance-based recommendations
        if performance_metrics:
            latest_perf = performance_metrics[-1] if performance_metrics else {}
            
            if latest_perf.get("accuracy") and latest_perf["accuracy"] < 0.8:
                recommendations.append(
                    "Model accuracy is below 80%. Consider retraining or feature engineering."
                )
            
            if latest_perf.get("f1_score") and latest_perf["f1_score"] < 0.7:
                recommendations.append(
                    "F1 score is below 70%. Review class imbalance and threshold tuning."
                )
        
        if not recommendations:
            recommendations.append("Model is performing within expected parameters.")
        
        return recommendations
    
    async def monitor_model_inference(self, features: pd.DataFrame, predictions: pd.Series,
                                    actual_labels: Optional[pd.Series] = None,
                                    request_id: Optional[str] = None) -> Dict[str, Any]:
        """Monitor single model inference with comprehensive tracking."""
        
        try:
            inference_start_time = datetime.utcnow()
            
            # Use observability platform for distributed tracing
            async with self.observability_platform.trace_operation(
                operation_name="ml_model_inference",
                service_name="ml-monitoring",
                correlation_id=request_id
            ) as trace_context:
                
                # Record inference metrics
                self._record_inference_metrics(features, predictions, trace_context['trace_id'])
                
                # Check for real-time drift
                drift_alerts = []
                if self.reference_data is not None:
                    for feature in features.columns:
                        if feature in self.reference_data.columns:
                            # Quick drift check for single sample
                            drift_result = await self._quick_drift_check(
                                self.reference_data[feature].sample(min(100, len(self.reference_data))),
                                features[feature],
                                feature
                            )
                            if drift_result.is_drift_detected:
                                drift_alerts.append(drift_result)
                
                # Record performance metrics if labels available
                performance_metrics = None
                if actual_labels is not None:
                    performance_metrics = await self.calculate_performance_metrics(
                        predictions, actual_labels
                    )
                
                inference_duration = (datetime.utcnow() - inference_start_time).total_seconds()
                
                # Record comprehensive metrics
                self.prometheus_collector.observe_histogram(
                    "ml_model_inference_duration_seconds",
                    inference_duration,
                    {"model_id": self.model_id, "status": "success"}
                )
                
                self.prometheus_collector.increment_counter(
                    "ml_model_inferences_total",
                    1,
                    {"model_id": self.model_id, "status": "success"}
                )
                
                # Business metrics
                if predictions is not None:
                    avg_prediction = float(predictions.mean())
                    self.observability_platform.business_metrics.update_kpi_value(
                        f"ml_model_{self.model_id}_avg_prediction",
                        avg_prediction
                    )
                
                return {
                    "model_id": self.model_id,
                    "trace_id": trace_context['trace_id'],
                    "inference_duration": inference_duration,
                    "drift_alerts": [asdict(alert) for alert in drift_alerts],
                    "performance_metrics": asdict(performance_metrics) if performance_metrics else None,
                    "prediction_stats": self._calculate_prediction_stats(predictions),
                    "timestamp": inference_start_time.isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error monitoring model inference: {str(e)}")
            
            # Record error metrics
            self.prometheus_collector.increment_counter(
                "ml_model_inferences_total",
                1,
                {"model_id": self.model_id, "status": "error"}
            )
            
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}
    
    def _record_inference_metrics(self, features: pd.DataFrame, predictions: pd.Series, trace_id: str):
        """Record detailed inference metrics."""
        
        # Feature statistics metrics
        for feature in features.columns:
            if pd.api.types.is_numeric_dtype(features[feature]):
                feature_mean = float(features[feature].mean())
                self.prometheus_collector.set_gauge(
                    "ml_model_feature_mean",
                    feature_mean,
                    {"model_id": self.model_id, "feature": feature}
                )
                
                feature_std = float(features[feature].std())
                self.prometheus_collector.set_gauge(
                    "ml_model_feature_std",
                    feature_std,
                    {"model_id": self.model_id, "feature": feature}
                )
        
        # Prediction statistics
        pred_mean = float(predictions.mean())
        pred_std = float(predictions.std())
        
        self.prometheus_collector.set_gauge(
            "ml_model_prediction_mean",
            pred_mean,
            {"model_id": self.model_id}
        )
        
        self.prometheus_collector.set_gauge(
            "ml_model_prediction_std",
            pred_std,
            {"model_id": self.model_id}
        )
        
        # Record business KPI
        self.observability_platform.record_metric(
            "ml_prediction_volume",
            len(predictions),
            self.observability_platform.MetricCategory.BUSINESS,
            {"model_id": self.model_id, "trace_id": trace_id}
        )
    
    async def _quick_drift_check(self, reference_sample: pd.Series, current_data: pd.Series,
                               feature_name: str) -> DriftResult:
        """Perform quick drift check for real-time monitoring."""
        
        try:
            # Simplified drift detection for real-time use
            if pd.api.types.is_numeric_dtype(reference_sample):
                # Simple statistical comparison
                ref_mean = reference_sample.mean()
                curr_mean = current_data.mean()
                ref_std = reference_sample.std()
                
                if ref_std > 0:
                    # Z-score based drift
                    z_score = abs(curr_mean - ref_mean) / ref_std
                    drift_score = z_score / 3.0  # Normalize to 0-1 range roughly
                else:
                    drift_score = 0.0
                
                is_drift = drift_score > self.config.feature_drift_threshold
                
                return DriftResult(
                    drift_type=DriftType.FEATURE_DRIFT,
                    feature_name=feature_name,
                    drift_score=drift_score,
                    p_value=None,
                    threshold=self.config.feature_drift_threshold,
                    is_drift_detected=is_drift,
                    severity=AlertSeverity.HIGH if is_drift else AlertSeverity.LOW,
                    reference_stats={"mean": ref_mean, "std": ref_std},
                    current_stats={"mean": curr_mean}
                )
            else:
                # For categorical, just return no drift for now (quick check)
                return DriftResult(
                    drift_type=DriftType.FEATURE_DRIFT,
                    feature_name=feature_name,
                    drift_score=0.0,
                    p_value=None,
                    threshold=self.config.feature_drift_threshold,
                    is_drift_detected=False,
                    severity=AlertSeverity.LOW
                )
                
        except Exception as e:
            logger.error(f"Error in quick drift check for {feature_name}: {str(e)}")
            return DriftResult(
                drift_type=DriftType.FEATURE_DRIFT,
                feature_name=feature_name,
                drift_score=0.0,
                p_value=None,
                threshold=self.config.feature_drift_threshold,
                is_drift_detected=False,
                severity=AlertSeverity.LOW
            )
    
    async def get_model_health_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive model health dashboard data."""
        
        try:
            now = datetime.utcnow()
            last_24h = now - timedelta(hours=24)
            last_7d = now - timedelta(days=7)
            
            # Get recent drift results
            recent_drift = await self._get_drift_results(last_24h, now)
            weekly_drift = await self._get_drift_results(last_7d, now)
            
            # Get recent performance metrics
            recent_performance = await self._get_performance_metrics(last_24h, now)
            weekly_performance = await self._get_performance_metrics(last_7d, now)
            
            # Calculate health scores
            health_score = self._calculate_model_health_score(recent_drift, recent_performance)
            
            # Get SLO compliance
            slo_compliance = self.observability_platform.slo_manager.calculate_slo_compliance(
                f"ml_model_{self.model_id}_slo"
            )
            
            dashboard_data = {
                "model_id": self.model_id,
                "timestamp": now.isoformat(),
                "health_score": health_score,
                "slo_compliance": slo_compliance,
                "drift_summary": {
                    "total_features_monitored": len(set([r.get("feature_name") for r in weekly_drift if r.get("feature_name")])),
                    "drift_detected_24h": len([r for r in recent_drift if r.get("is_drift_detected")]),
                    "drift_detected_7d": len([r for r in weekly_drift if r.get("is_drift_detected")]),
                    "critical_drift_alerts": len([r for r in recent_drift if r.get("severity") == "critical"]),
                    "avg_drift_score_24h": self._calculate_avg_drift_score(recent_drift)
                },
                "performance_summary": {
                    "total_inferences_24h": len(recent_performance),
                    "avg_performance_24h": self._calculate_avg_performance(recent_performance),
                    "performance_trend": self._calculate_performance_trend(weekly_performance),
                    "last_performance_check": recent_performance[-1]["timestamp"] if recent_performance else None
                },
                "alerts": {
                    "active_alerts": len([r for r in recent_drift if r.get("is_drift_detected")]),
                    "critical_alerts": len([r for r in recent_drift if r.get("severity") == "critical"]),
                    "high_alerts": len([r for r in recent_drift if r.get("severity") == "high"])
                },
                "recommendations": self._generate_health_recommendations(recent_drift, recent_performance, health_score)
            }
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Error generating model health dashboard: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}
    
    def _calculate_model_health_score(self, drift_results: List[Dict], performance_metrics: List[Dict]) -> Dict[str, Any]:
        """Calculate overall model health score (0-100)."""
        
        # Drift health component (0-50 points)
        drift_health = 50
        if drift_results:
            critical_drifts = len([r for r in drift_results if r.get("severity") == "critical"])
            high_drifts = len([r for r in drift_results if r.get("severity") == "high"])
            
            # Penalize for drift
            drift_health -= (critical_drifts * 20 + high_drifts * 10)
            drift_health = max(0, drift_health)
        
        # Performance health component (0-50 points)
        performance_health = 50
        if performance_metrics:
            latest_perf = performance_metrics[-1] if performance_metrics else {}
            
            # Classification model
            if latest_perf.get("accuracy"):
                accuracy = latest_perf["accuracy"]
                if accuracy >= 0.9:
                    performance_health = 50
                elif accuracy >= 0.8:
                    performance_health = 40
                elif accuracy >= 0.7:
                    performance_health = 30
                else:
                    performance_health = 20
            
            # Regression model
            elif latest_perf.get("r2_score"):
                r2 = latest_perf["r2_score"]
                if r2 >= 0.9:
                    performance_health = 50
                elif r2 >= 0.8:
                    performance_health = 40
                elif r2 >= 0.7:
                    performance_health = 30
                else:
                    performance_health = 20
        
        total_health = drift_health + performance_health
        
        # Determine status
        if total_health >= 80:
            status = "excellent"
        elif total_health >= 60:
            status = "good"
        elif total_health >= 40:
            status = "fair"
        elif total_health >= 20:
            status = "poor"
        else:
            status = "critical"
        
        return {
            "total_score": total_health,
            "drift_score": drift_health,
            "performance_score": performance_health,
            "status": status,
            "last_calculated": datetime.utcnow().isoformat()
        }
    
    def _calculate_avg_drift_score(self, drift_results: List[Dict]) -> float:
        """Calculate average drift score."""
        if not drift_results:
            return 0.0
        
        scores = [r.get("drift_score", 0) for r in drift_results if r.get("drift_score") is not None]
        return sum(scores) / len(scores) if scores else 0.0
    
    def _calculate_performance_trend(self, performance_metrics: List[Dict]) -> str:
        """Calculate performance trend direction."""
        if len(performance_metrics) < 2:
            return "stable"
        
        # Use accuracy for classification or r2_score for regression
        key = "accuracy" if performance_metrics[-1].get("accuracy") else "r2_score"
        
        values = [m.get(key, 0) for m in performance_metrics if m.get(key) is not None]
        if len(values) < 2:
            return "stable"
        
        # Simple trend calculation
        recent_avg = sum(values[-3:]) / len(values[-3:]) if len(values) >= 3 else values[-1]
        older_avg = sum(values[-6:-3]) / 3 if len(values) >= 6 else sum(values[:-3]) / len(values[:-3]) if len(values) > 3 else values[0]
        
        if recent_avg > older_avg * 1.05:
            return "improving"
        elif recent_avg < older_avg * 0.95:
            return "declining"
        else:
            return "stable"
    
    def _generate_health_recommendations(self, drift_results: List[Dict], 
                                       performance_metrics: List[Dict], 
                                       health_score: Dict) -> List[str]:
        """Generate health-specific recommendations."""
        recommendations = []
        
        # Health score recommendations
        if health_score["status"] == "critical":
            recommendations.append("URGENT: Model health is critical. Immediate intervention required.")
        elif health_score["status"] == "poor":
            recommendations.append("Model health is poor. Schedule retraining soon.")
        
        # Drift-specific recommendations
        critical_drifts = len([r for r in drift_results if r.get("severity") == "critical"])
        if critical_drifts > 0:
            recommendations.append(f"Critical drift detected in {critical_drifts} features. Consider immediate retraining.")
        
        # Performance-specific recommendations
        if performance_metrics:
            trend = self._calculate_performance_trend(performance_metrics)
            if trend == "declining":
                recommendations.append("Performance is declining. Review recent data quality and consider model refresh.")
            elif trend == "improving":
                recommendations.append("Performance is improving. Current monitoring strategy is effective.")
        
        # SLO recommendations
        if health_score["performance_score"] < 30:
            recommendations.append("Model performance below acceptable thresholds. Review SLO targets.")
        
        if not recommendations:
            recommendations.append("Model health is good. Continue current monitoring practices.")
        
        return recommendations


# Global model monitor registry
_model_monitors: Dict[str, ModelMonitor] = {}


def get_model_monitor(model_id: str, config: Optional[DriftConfig] = None) -> ModelMonitor:
    """Get or create model monitor for given model ID."""
    global _model_monitors
    
    if model_id not in _model_monitors:
        if config is None:
            config = DriftConfig()  # Use default config
        _model_monitors[model_id] = ModelMonitor(model_id, config)
    
    return _model_monitors[model_id]


async def initialize_ml_monitoring() -> Dict[str, Any]:
    """Initialize ML monitoring infrastructure."""
    try:
        logger.info("Initializing ML monitoring infrastructure...")
        
        # Setup ML-specific metrics in Prometheus
        prometheus_collector = get_prometheus_collector()
        
        # Register ML monitoring SLOs
        observability_platform = get_observability_platform()
        
        # ML Model SLO
        from monitoring.enterprise_observability import SLO, SLIMetric, MetricCategory
        from datetime import timedelta
        
        ml_slo = SLO(
            name="ml_model_reliability",
            description="ML model performance and drift SLO",
            sli_metrics=[
                SLIMetric(
                    name="model_accuracy",
                    description="Model prediction accuracy",
                    query="avg(ml_model_accuracy)",
                    unit="percentage",
                    target_value=0.85,  # 85% accuracy target
                    threshold_warning=0.80,
                    threshold_critical=0.75,
                    category=MetricCategory.BUSINESS
                ),
                SLIMetric(
                    name="drift_detection_rate",
                    description="Rate of drift detection alerts",
                    query="rate(ml_model_drift_detected_total[1h])",
                    unit="per_hour",
                    target_value=0.1,  # Max 0.1 drift alerts per hour
                    threshold_warning=0.2,
                    threshold_critical=0.5,
                    category=MetricCategory.APPLICATION
                )
            ],
            target_percentage=95.0,
            time_window=timedelta(hours=24),
            error_budget_percentage=5.0
        )
        
        observability_platform.slo_manager.register_slo(ml_slo)
        
        # Register default business KPIs for ML
        from monitoring.enterprise_observability import BusinessKPI
        
        ml_kpis = [
            BusinessKPI(
                name="ml_predictions_daily",
                description="Daily ML model predictions",
                formula="sum(ml_model_inferences_total)",
                target_value=10000.0,
                category="ml_operations",
                unit="predictions"
            ),
            BusinessKPI(
                name="ml_model_accuracy_avg",
                description="Average ML model accuracy",
                formula="avg(ml_model_accuracy)",
                target_value=0.85,
                category="ml_quality",
                unit="percentage"
            )
        ]
        
        for kpi in ml_kpis:
            observability_platform.business_metrics.register_kpi(kpi)
        
        logger.info("ML monitoring infrastructure initialized successfully")
        
        return {
            "status": "success",
            "message": "ML monitoring initialized",
            "slos_registered": 1,
            "kpis_registered": len(ml_kpis),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to initialize ML monitoring: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


async def get_ml_monitoring_dashboard() -> Dict[str, Any]:
    """Get comprehensive ML monitoring dashboard."""
    try:
        observability_platform = get_observability_platform()
        
        # Get all model monitors
        global _model_monitors
        
        models_health = {}
        for model_id, monitor in _model_monitors.items():
            models_health[model_id] = await monitor.get_model_health_dashboard()
        
        # Get ML SLO status
        slo_status = observability_platform.slo_manager.calculate_slo_compliance("ml_model_reliability")
        
        # Get ML business KPIs
        kpi_data = observability_platform.business_metrics.get_kpi_dashboard_data()
        ml_kpis = {k: v for k, v in kpi_data["kpis"].items() if k.startswith("ml_")}
        
        # Overall ML health
        overall_health = "healthy"
        total_models = len(_model_monitors)
        unhealthy_models = len([h for h in models_health.values() if h.get("health_score", {}).get("status") in ["poor", "critical"]])
        
        if unhealthy_models > total_models * 0.5:
            overall_health = "critical"
        elif unhealthy_models > total_models * 0.2:
            overall_health = "degraded"
        elif unhealthy_models > 0:
            overall_health = "warning"
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_health": overall_health,
            "summary": {
                "total_models_monitored": total_models,
                "healthy_models": total_models - unhealthy_models,
                "unhealthy_models": unhealthy_models,
                "slo_compliance": slo_status.get("overall_compliance_rate", 0) * 100 if "error" not in slo_status else 0
            },
            "models_health": models_health,
            "slo_status": slo_status,
            "business_kpis": ml_kpis,
            "recommendations": _generate_global_ml_recommendations(models_health, slo_status)
        }
        
    except Exception as e:
        logger.error(f"Error generating ML monitoring dashboard: {str(e)}")
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


def _generate_global_ml_recommendations(models_health: Dict, slo_status: Dict) -> List[str]:
    """Generate global ML recommendations."""
    recommendations = []
    
    # Model health recommendations
    critical_models = [m for m, h in models_health.items() if h.get("health_score", {}).get("status") == "critical"]
    if critical_models:
        recommendations.append(f"Critical: {len(critical_models)} models require immediate attention: {', '.join(critical_models)}")
    
    poor_models = [m for m, h in models_health.items() if h.get("health_score", {}).get("status") == "poor"]
    if poor_models:
        recommendations.append(f"Warning: {len(poor_models)} models showing poor health: {', '.join(poor_models)}")
    
    # SLO recommendations
    if "error" not in slo_status:
        compliance_rate = slo_status.get("overall_compliance_rate", 1.0)
        if compliance_rate < 0.95:
            recommendations.append(f"SLO compliance at {compliance_rate*100:.1f}% - below 95% target")
    
    if not recommendations:
        recommendations.append("All ML models are performing within acceptable parameters")
    
    return recommendations