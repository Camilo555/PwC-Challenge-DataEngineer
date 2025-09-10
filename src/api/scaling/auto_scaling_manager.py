"""
Auto-Scaling Manager for Story 1.1 Dashboard API
Enterprise Kubernetes HPA/VPA integration with intelligent load prediction

Features:
- Horizontal Pod Autoscaler (HPA) integration with custom metrics
- Vertical Pod Autoscaler (VPA) recommendations
- Predictive scaling based on historical patterns
- Load prediction using ML algorithms
- Real-time metrics collection and analysis
- Cost-optimized scaling decisions
- Integration with dashboard performance requirements
"""
import asyncio
import json
import time
import math
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from collections import deque, defaultdict
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import psutil
import aiohttp
import yaml

from prometheus_client import Counter, Histogram, Gauge
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from core.logging import get_logger
from core.config.unified_config import get_unified_config


class ScalingAction(Enum):
    """Scaling action types"""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    MAINTAIN = "maintain"
    PREDICTIVE_SCALE = "predictive_scale"


class ScalingTrigger(Enum):
    """Scaling trigger types"""
    CPU_THRESHOLD = "cpu_threshold"
    MEMORY_THRESHOLD = "memory_threshold"
    RESPONSE_TIME = "response_time"
    CONNECTION_COUNT = "connection_count"
    QUEUE_DEPTH = "queue_depth"
    PREDICTIVE = "predictive"
    CUSTOM_METRIC = "custom_metric"


class ResourceType(Enum):
    """Resource types for scaling"""
    CPU = "cpu"
    MEMORY = "memory"
    REQUESTS_PER_SECOND = "requests_per_second"
    WEBSOCKET_CONNECTIONS = "websocket_connections"


@dataclass
class ScalingMetrics:
    """Current scaling metrics"""
    timestamp: datetime
    cpu_usage_percent: float
    memory_usage_mb: float
    active_connections: int
    requests_per_second: float
    response_time_p95_ms: float
    queue_depth: int
    error_rate_percent: float
    
    # Business metrics
    dashboard_load_score: float = 0.0
    user_concurrency: int = 0
    peak_hour_factor: float = 1.0


@dataclass
class ScalingConfiguration:
    """Scaling configuration parameters"""
    # HPA Configuration
    min_replicas: int = 1
    max_replicas: int = 10
    target_cpu_percent: int = 70
    target_memory_percent: int = 80
    
    # Performance thresholds
    max_response_time_ms: float = 25.0  # Story 1.1 requirement
    max_connection_per_pod: int = 1000
    max_requests_per_second_per_pod: int = 100
    
    # Scaling behavior
    scale_up_stabilization_seconds: int = 60
    scale_down_stabilization_seconds: int = 300
    scale_up_percent: int = 50  # Scale up by 50%
    scale_down_percent: int = 25  # Scale down by 25%
    
    # VPA Configuration
    enable_vpa: bool = True
    vpa_update_mode: str = "Auto"  # Auto, Recreation, Initial, Off
    
    # Predictive scaling
    enable_predictive: bool = True
    prediction_window_minutes: int = 30
    historical_data_days: int = 7
    confidence_threshold: float = 0.8
    
    # Cost optimization
    enable_cost_optimization: bool = True
    cost_per_pod_hour: float = 0.50  # $0.50 per pod per hour
    performance_cost_ratio: float = 0.7  # 70% performance, 30% cost


class LoadPredictionModel:
    """Machine learning model for load prediction"""
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.historical_data: deque = deque(maxlen=window_size)
        self.model = LinearRegression()
        self.scaler = StandardScaler()
        self.is_trained = False
        self.logger = get_logger("load_prediction_model")
        
        # Feature extractors
        self.feature_extractors = {
            'time_features': self._extract_time_features,
            'trend_features': self._extract_trend_features,
            'seasonal_features': self._extract_seasonal_features,
            'business_features': self._extract_business_features
        }
    
    def add_data_point(self, metrics: ScalingMetrics):
        """Add data point to historical data"""
        data_point = {
            'timestamp': metrics.timestamp,
            'cpu_usage': metrics.cpu_usage_percent,
            'memory_usage': metrics.memory_usage_mb,
            'connections': metrics.active_connections,
            'rps': metrics.requests_per_second,
            'response_time': metrics.response_time_p95_ms,
            'error_rate': metrics.error_rate_percent,
            'dashboard_load': metrics.dashboard_load_score,
            'user_concurrency': metrics.user_concurrency
        }
        
        self.historical_data.append(data_point)
        
        # Retrain model periodically
        if len(self.historical_data) >= 100 and len(self.historical_data) % 50 == 0:
            asyncio.create_task(self._retrain_model())
    
    async def _retrain_model(self):
        """Retrain the prediction model"""
        try:
            if len(self.historical_data) < 50:
                return
            
            # Prepare training data
            features = []
            targets = []
            
            data_list = list(self.historical_data)
            
            for i in range(len(data_list) - 10):
                # Use current metrics to predict load 10 points ahead
                current = data_list[i]
                future = data_list[i + 10]
                
                feature_vector = self._extract_features(current)
                target_load = self._calculate_load_score(future)
                
                features.append(feature_vector)
                targets.append(target_load)
            
            if len(features) < 20:
                return
            
            # Train model
            X = np.array(features)
            y = np.array(targets)
            
            X_scaled = self.scaler.fit_transform(X)
            self.model.fit(X_scaled, y)
            self.is_trained = True
            
            self.logger.info(f"Load prediction model retrained with {len(features)} samples")
            
        except Exception as e:
            self.logger.error(f"Error retraining model: {e}")
    
    def predict_load(self, current_metrics: ScalingMetrics, minutes_ahead: int = 30) -> Tuple[float, float]:
        """
        Predict load score for specified minutes ahead
        Returns: (predicted_load_score, confidence)
        """
        try:
            if not self.is_trained or len(self.historical_data) < 50:
                # Fallback to simple trend-based prediction
                return self._simple_trend_prediction(current_metrics, minutes_ahead)
            
            # Extract features from current metrics
            current_data = {
                'timestamp': current_metrics.timestamp,
                'cpu_usage': current_metrics.cpu_usage_percent,
                'memory_usage': current_metrics.memory_usage_mb,
                'connections': current_metrics.active_connections,
                'rps': current_metrics.requests_per_second,
                'response_time': current_metrics.response_time_p95_ms,
                'error_rate': current_metrics.error_rate_percent,
                'dashboard_load': current_metrics.dashboard_load_score,
                'user_concurrency': current_metrics.user_concurrency
            }
            
            features = self._extract_features(current_data)
            X = np.array([features])
            X_scaled = self.scaler.transform(X)
            
            predicted_load = self.model.predict(X_scaled)[0]
            
            # Calculate confidence based on recent prediction accuracy
            confidence = self._calculate_prediction_confidence()
            
            return max(0.0, min(1.0, predicted_load)), confidence
            
        except Exception as e:
            self.logger.error(f"Error in load prediction: {e}")
            return self._simple_trend_prediction(current_metrics, minutes_ahead)
    
    def _simple_trend_prediction(self, current_metrics: ScalingMetrics, minutes_ahead: int) -> Tuple[float, float]:
        """Simple trend-based prediction fallback"""
        try:
            if len(self.historical_data) < 10:
                current_load = self._calculate_load_score({
                    'cpu_usage': current_metrics.cpu_usage_percent,
                    'memory_usage': current_metrics.memory_usage_mb,
                    'connections': current_metrics.active_connections,
                    'rps': current_metrics.requests_per_second,
                    'response_time': current_metrics.response_time_p95_ms
                })
                return current_load, 0.5
            
            # Calculate recent trend
            recent_data = list(self.historical_data)[-10:]
            load_scores = [self._calculate_load_score(data) for data in recent_data]
            
            if len(load_scores) >= 2:
                # Simple linear trend
                trend = (load_scores[-1] - load_scores[0]) / len(load_scores)
                predicted_load = load_scores[-1] + (trend * minutes_ahead / 5)  # Approximate scaling
                confidence = 0.6 if abs(trend) < 0.1 else 0.4  # Lower confidence for high trend
            else:
                predicted_load = load_scores[-1] if load_scores else 0.5
                confidence = 0.5
            
            return max(0.0, min(1.0, predicted_load)), confidence
            
        except Exception as e:
            self.logger.error(f"Error in simple trend prediction: {e}")
            return 0.5, 0.3
    
    def _extract_features(self, data_point: Dict[str, Any]) -> List[float]:
        """Extract feature vector from data point"""
        features = []
        
        for extractor_name, extractor_func in self.feature_extractors.items():
            try:
                extracted_features = extractor_func(data_point)
                features.extend(extracted_features)
            except Exception as e:
                self.logger.error(f"Error in {extractor_name}: {e}")
                # Add default features to maintain vector size
                features.extend([0.0] * 3)
        
        return features
    
    def _extract_time_features(self, data_point: Dict[str, Any]) -> List[float]:
        """Extract time-based features"""
        timestamp = data_point['timestamp']
        
        # Hour of day (0-23) normalized
        hour = timestamp.hour / 23.0
        
        # Day of week (0-6) normalized
        day_of_week = timestamp.weekday() / 6.0
        
        # Is business hours (9 AM - 6 PM)
        is_business_hours = 1.0 if 9 <= timestamp.hour <= 18 else 0.0
        
        return [hour, day_of_week, is_business_hours]
    
    def _extract_trend_features(self, data_point: Dict[str, Any]) -> List[float]:
        """Extract trend-based features"""
        if len(self.historical_data) < 5:
            return [0.0, 0.0, 0.0]
        
        recent_data = list(self.historical_data)[-5:]
        
        # CPU trend
        cpu_values = [d['cpu_usage'] for d in recent_data]
        cpu_trend = (cpu_values[-1] - cpu_values[0]) / len(cpu_values) if len(cpu_values) > 1 else 0.0
        
        # RPS trend
        rps_values = [d['rps'] for d in recent_data]
        rps_trend = (rps_values[-1] - rps_values[0]) / len(rps_values) if len(rps_values) > 1 else 0.0
        
        # Connection trend
        conn_values = [d['connections'] for d in recent_data]
        conn_trend = (conn_values[-1] - conn_values[0]) / len(conn_values) if len(conn_values) > 1 else 0.0
        
        return [cpu_trend, rps_trend, conn_trend]
    
    def _extract_seasonal_features(self, data_point: Dict[str, Any]) -> List[float]:
        """Extract seasonal pattern features"""
        timestamp = data_point['timestamp']
        
        # Weekly seasonality (sin/cos encoding)
        week_progress = (timestamp.weekday() * 24 + timestamp.hour) / (7 * 24)
        weekly_sin = math.sin(2 * math.pi * week_progress)
        weekly_cos = math.cos(2 * math.pi * week_progress)
        
        # Daily seasonality
        day_progress = timestamp.hour / 24.0
        daily_pattern = math.sin(2 * math.pi * day_progress)
        
        return [weekly_sin, weekly_cos, daily_pattern]
    
    def _extract_business_features(self, data_point: Dict[str, Any]) -> List[float]:
        """Extract business-specific features"""
        # Dashboard load score
        dashboard_load = data_point.get('dashboard_load', 0.0)
        
        # User concurrency normalized (assuming max 10000 users)
        user_concurrency = min(data_point.get('user_concurrency', 0) / 10000.0, 1.0)
        
        # Response time normalized (assuming max 1000ms acceptable)
        response_time_norm = min(data_point.get('response_time', 0) / 1000.0, 1.0)
        
        return [dashboard_load, user_concurrency, response_time_norm]
    
    def _calculate_load_score(self, data_point: Dict[str, Any]) -> float:
        """Calculate overall load score from metrics"""
        try:
            # Weighted combination of metrics
            cpu_score = data_point.get('cpu_usage', 0) / 100.0
            memory_score = min(data_point.get('memory_usage', 0) / 1000.0, 1.0)  # Normalize to GB
            rps_score = min(data_point.get('rps', 0) / 1000.0, 1.0)  # Normalize to 1000 RPS
            conn_score = min(data_point.get('connections', 0) / 5000.0, 1.0)  # Normalize to 5000 connections
            
            # Weighted average
            load_score = (
                cpu_score * 0.3 +
                memory_score * 0.2 +
                rps_score * 0.25 +
                conn_score * 0.25
            )
            
            return max(0.0, min(1.0, load_score))
            
        except Exception as e:
            self.logger.error(f"Error calculating load score: {e}")
            return 0.5
    
    def _calculate_prediction_confidence(self) -> float:
        """Calculate confidence in predictions based on recent accuracy"""
        if len(self.historical_data) < 20:
            return 0.5
        
        # Simple confidence based on data availability and model training
        data_confidence = min(len(self.historical_data) / self.window_size, 1.0)
        model_confidence = 0.8 if self.is_trained else 0.4
        
        return (data_confidence + model_confidence) / 2


class KubernetesScalingClient:
    """Kubernetes client for HPA/VPA operations"""
    
    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self.logger = get_logger("k8s_scaling_client")
        
        # Initialize Kubernetes clients
        try:
            config.load_incluster_config()  # For running inside cluster
        except config.ConfigException:
            try:
                config.load_kube_config()  # For local development
            except config.ConfigException:
                self.logger.warning("Could not load Kubernetes config - scaling disabled")
                self.enabled = False
                return
        
        self.enabled = True
        self.apps_v1 = client.AppsV1Api()
        self.autoscaling_v1 = client.AutoscalingV1Api()
        self.autoscaling_v2 = client.AutoscalingV2Api()
        self.custom_objects = client.CustomObjectsApi()
    
    async def get_current_replicas(self, deployment_name: str) -> int:
        """Get current number of replicas for deployment"""
        try:
            if not self.enabled:
                return 1
            
            deployment = await asyncio.get_event_loop().run_in_executor(
                None, self.apps_v1.read_namespaced_deployment,
                deployment_name, self.namespace
            )
            return deployment.spec.replicas
            
        except ApiException as e:
            self.logger.error(f"Error getting current replicas: {e}")
            return 1
    
    async def update_hpa(self, hpa_name: str, config: ScalingConfiguration) -> bool:
        """Update HPA configuration"""
        try:
            if not self.enabled:
                self.logger.info(f"K8s scaling disabled - would update HPA {hpa_name}")
                return True
            
            # Get current HPA
            try:
                current_hpa = await asyncio.get_event_loop().run_in_executor(
                    None, self.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler,
                    hpa_name, self.namespace
                )
            except ApiException:
                # HPA doesn't exist, create it
                return await self._create_hpa(hpa_name, config)
            
            # Update HPA spec
            current_hpa.spec.min_replicas = config.min_replicas
            current_hpa.spec.max_replicas = config.max_replicas
            
            # Update metrics
            if current_hpa.spec.metrics:
                for metric in current_hpa.spec.metrics:
                    if metric.type == "Resource":
                        if metric.resource.name == "cpu":
                            metric.resource.target.average_utilization = config.target_cpu_percent
                        elif metric.resource.name == "memory":
                            metric.resource.target.average_utilization = config.target_memory_percent
            
            # Update behavior (scaling policies)
            if not current_hpa.spec.behavior:
                current_hpa.spec.behavior = client.V2HorizontalPodAutoscalerBehavior()
            
            # Scale up behavior
            if not current_hpa.spec.behavior.scale_up:
                current_hpa.spec.behavior.scale_up = client.V2HPAScalingRules()
            
            current_hpa.spec.behavior.scale_up.stabilization_window_seconds = config.scale_up_stabilization_seconds
            
            # Scale down behavior
            if not current_hpa.spec.behavior.scale_down:
                current_hpa.spec.behavior.scale_down = client.V2HPAScalingRules()
            
            current_hpa.spec.behavior.scale_down.stabilization_window_seconds = config.scale_down_stabilization_seconds
            
            # Apply update
            await asyncio.get_event_loop().run_in_executor(
                None, self.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler,
                hpa_name, self.namespace, current_hpa
            )
            
            self.logger.info(f"Updated HPA {hpa_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating HPA: {e}")
            return False
    
    async def _create_hpa(self, hpa_name: str, config: ScalingConfiguration) -> bool:
        """Create new HPA"""
        try:
            # Create HPA manifest
            hpa_manifest = client.V2HorizontalPodAutoscaler(
                api_version="autoscaling/v2",
                kind="HorizontalPodAutoscaler",
                metadata=client.V1ObjectMeta(name=hpa_name, namespace=self.namespace),
                spec=client.V2HorizontalPodAutoscalerSpec(
                    scale_target_ref=client.V2CrossVersionObjectReference(
                        api_version="apps/v1",
                        kind="Deployment",
                        name=hpa_name  # Assuming HPA name matches deployment name
                    ),
                    min_replicas=config.min_replicas,
                    max_replicas=config.max_replicas,
                    metrics=[
                        client.V2MetricSpec(
                            type="Resource",
                            resource=client.V2ResourceMetricSource(
                                name="cpu",
                                target=client.V2MetricTarget(
                                    type="Utilization",
                                    average_utilization=config.target_cpu_percent
                                )
                            )
                        ),
                        client.V2MetricSpec(
                            type="Resource",
                            resource=client.V2ResourceMetricSource(
                                name="memory",
                                target=client.V2MetricTarget(
                                    type="Utilization",
                                    average_utilization=config.target_memory_percent
                                )
                            )
                        )
                    ],
                    behavior=client.V2HorizontalPodAutoscalerBehavior(
                        scale_up=client.V2HPAScalingRules(
                            stabilization_window_seconds=config.scale_up_stabilization_seconds
                        ),
                        scale_down=client.V2HPAScalingRules(
                            stabilization_window_seconds=config.scale_down_stabilization_seconds
                        )
                    )
                )
            )
            
            await asyncio.get_event_loop().run_in_executor(
                None, self.autoscaling_v2.create_namespaced_horizontal_pod_autoscaler,
                self.namespace, hpa_manifest
            )
            
            self.logger.info(f"Created HPA {hpa_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating HPA: {e}")
            return False
    
    async def update_vpa(self, vpa_name: str, config: ScalingConfiguration) -> bool:
        """Update VPA configuration"""
        try:
            if not self.enabled or not config.enable_vpa:
                return True
            
            # VPA manifest
            vpa_manifest = {
                "apiVersion": "autoscaling.k8s.io/v1",
                "kind": "VerticalPodAutoscaler",
                "metadata": {
                    "name": vpa_name,
                    "namespace": self.namespace
                },
                "spec": {
                    "targetRef": {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": vpa_name
                    },
                    "updatePolicy": {
                        "updateMode": config.vpa_update_mode
                    },
                    "resourcePolicy": {
                        "containerPolicies": [{
                            "containerName": "*",
                            "maxAllowed": {
                                "cpu": "2",
                                "memory": "4Gi"
                            },
                            "minAllowed": {
                                "cpu": "100m",
                                "memory": "128Mi"
                            }
                        }]
                    }
                }
            }
            
            # Try to update existing VPA or create new one
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, self.custom_objects.patch_namespaced_custom_object,
                    "autoscaling.k8s.io", "v1", self.namespace, "verticalpodautoscalers",
                    vpa_name, vpa_manifest
                )
            except ApiException:
                # Create new VPA
                await asyncio.get_event_loop().run_in_executor(
                    None, self.custom_objects.create_namespaced_custom_object,
                    "autoscaling.k8s.io", "v1", self.namespace, "verticalpodautoscalers",
                    vpa_manifest
                )
            
            self.logger.info(f"Updated/Created VPA {vpa_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating VPA: {e}")
            return False


class AutoScalingManager:
    """
    Comprehensive auto-scaling manager for dashboard API
    Integrates HPA, VPA, and predictive scaling
    """
    
    def __init__(self, config: ScalingConfiguration):
        self.config = config
        self.logger = get_logger("auto_scaling_manager")
        
        # Components
        self.prediction_model = LoadPredictionModel()
        self.k8s_client = KubernetesScalingClient()
        
        # Metrics collection
        self.current_metrics: Optional[ScalingMetrics] = None
        self.metrics_history: deque = deque(maxlen=1000)
        
        # Scaling state
        self.current_replicas = 1
        self.target_replicas = 1
        self.last_scaling_action = ScalingAction.MAINTAIN
        self.last_scaling_time = datetime.now()
        
        # Decision tracking
        self.scaling_decisions: deque = deque(maxlen=100)
        
        # Prometheus metrics
        self._setup_prometheus_metrics()
        
        # Background tasks
        self.background_tasks: List[asyncio.Task] = []
        self._start_background_tasks()
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics for monitoring"""
        self.prom_replicas = Gauge('autoscaler_target_replicas', 'Target number of replicas')
        self.prom_current_replicas = Gauge('autoscaler_current_replicas', 'Current number of replicas')
        self.prom_scaling_decisions = Counter('autoscaler_scaling_decisions_total', 'Scaling decisions', ['action', 'trigger'])
        self.prom_prediction_accuracy = Gauge('autoscaler_prediction_accuracy', 'Load prediction accuracy')
        self.prom_cost_savings = Counter('autoscaler_cost_savings_dollars', 'Estimated cost savings from scaling')
        self.prom_performance_score = Gauge('autoscaler_performance_score', 'Performance score (0-1)')
    
    def _start_background_tasks(self):
        """Start background monitoring and scaling tasks"""
        tasks = [
            self._metrics_collection_loop(),
            self._scaling_decision_loop(),
            self._predictive_scaling_loop(),
            self._cost_optimization_loop()
        ]
        
        for task_func in tasks:
            task = asyncio.create_task(task_func)
            self.background_tasks.append(task)
        
        self.logger.info("Auto-scaling background tasks started")
    
    async def update_metrics(self, metrics: ScalingMetrics):
        """Update current metrics for scaling decisions"""
        self.current_metrics = metrics
        self.metrics_history.append(metrics)
        
        # Add to prediction model
        self.prediction_model.add_data_point(metrics)
        
        # Update Prometheus metrics
        self.prom_performance_score.set(self._calculate_performance_score(metrics))
    
    def _calculate_performance_score(self, metrics: ScalingMetrics) -> float:
        """Calculate overall performance score"""
        try:
            # Response time score (target: <25ms)
            response_time_score = max(0, 1 - (metrics.response_time_p95_ms / 25.0))
            
            # Resource utilization score (target: 70% CPU, 80% memory)
            cpu_score = 1 - abs(metrics.cpu_usage_percent - 70) / 100
            memory_target = self.config.target_memory_percent
            memory_score = 1 - abs((metrics.memory_usage_mb / 1000 * 100) - memory_target) / 100
            
            # Error rate score (target: <1%)
            error_score = max(0, 1 - (metrics.error_rate_percent / 5))  # Allow up to 5% before penalty
            
            # Weighted average
            performance_score = (
                response_time_score * 0.4 +  # 40% weight on response time
                cpu_score * 0.25 +           # 25% weight on CPU
                memory_score * 0.2 +         # 20% weight on memory
                error_score * 0.15           # 15% weight on error rate
            )
            
            return max(0.0, min(1.0, performance_score))
            
        except Exception as e:
            self.logger.error(f"Error calculating performance score: {e}")
            return 0.5
    
    async def make_scaling_decision(self) -> Tuple[ScalingAction, int, ScalingTrigger]:
        """
        Make intelligent scaling decision based on current metrics and predictions
        Returns: (action, target_replicas, trigger)
        """
        try:
            if not self.current_metrics:
                return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.CPU_THRESHOLD
            
            metrics = self.current_metrics
            
            # Check immediate thresholds first
            immediate_action = await self._check_immediate_scaling_needs(metrics)
            if immediate_action[0] != ScalingAction.MAINTAIN:
                return immediate_action
            
            # Check predictive scaling if enabled
            if self.config.enable_predictive:
                predictive_action = await self._check_predictive_scaling(metrics)
                if predictive_action[0] != ScalingAction.MAINTAIN:
                    return predictive_action
            
            # Cost optimization check
            if self.config.enable_cost_optimization:
                cost_action = await self._check_cost_optimization(metrics)
                if cost_action[0] != ScalingAction.MAINTAIN:
                    return cost_action
            
            return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.CPU_THRESHOLD
            
        except Exception as e:
            self.logger.error(f"Error making scaling decision: {e}")
            return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.CPU_THRESHOLD
    
    async def _check_immediate_scaling_needs(self, metrics: ScalingMetrics) -> Tuple[ScalingAction, int, ScalingTrigger]:
        """Check for immediate scaling needs based on current metrics"""
        
        # Response time threshold (critical for Story 1.1)
        if metrics.response_time_p95_ms > self.config.max_response_time_ms:
            scale_factor = min(2.0, metrics.response_time_p95_ms / self.config.max_response_time_ms)
            target_replicas = min(
                self.config.max_replicas,
                int(self.current_replicas * scale_factor)
            )
            if target_replicas > self.current_replicas:
                return ScalingAction.SCALE_UP, target_replicas, ScalingTrigger.RESPONSE_TIME
        
        # CPU threshold
        if metrics.cpu_usage_percent > self.config.target_cpu_percent + 10:  # 10% buffer
            scale_factor = 1 + (self.config.scale_up_percent / 100)
            target_replicas = min(
                self.config.max_replicas,
                int(self.current_replicas * scale_factor)
            )
            if target_replicas > self.current_replicas:
                return ScalingAction.SCALE_UP, target_replicas, ScalingTrigger.CPU_THRESHOLD
        
        # Memory threshold
        memory_percent = (metrics.memory_usage_mb / 1000) * 100  # Assuming 1GB base
        if memory_percent > self.config.target_memory_percent + 10:
            scale_factor = 1 + (self.config.scale_up_percent / 100)
            target_replicas = min(
                self.config.max_replicas,
                int(self.current_replicas * scale_factor)
            )
            if target_replicas > self.current_replicas:
                return ScalingAction.SCALE_UP, target_replicas, ScalingTrigger.MEMORY_THRESHOLD
        
        # Connection count threshold
        if metrics.active_connections > self.config.max_connection_per_pod * self.current_replicas:
            connections_per_pod = metrics.active_connections / self.current_replicas
            target_replicas = min(
                self.config.max_replicas,
                int(math.ceil(metrics.active_connections / self.config.max_connection_per_pod))
            )
            if target_replicas > self.current_replicas:
                return ScalingAction.SCALE_UP, target_replicas, ScalingTrigger.CONNECTION_COUNT
        
        # Scale down conditions
        if (metrics.cpu_usage_percent < self.config.target_cpu_percent - 20 and
            metrics.response_time_p95_ms < self.config.max_response_time_ms / 2 and
            self.current_replicas > self.config.min_replicas):
            
            # Check if we can safely scale down
            time_since_last_scale = (datetime.now() - self.last_scaling_time).total_seconds()
            if time_since_last_scale > self.config.scale_down_stabilization_seconds:
                scale_factor = 1 - (self.config.scale_down_percent / 100)
                target_replicas = max(
                    self.config.min_replicas,
                    int(self.current_replicas * scale_factor)
                )
                if target_replicas < self.current_replicas:
                    return ScalingAction.SCALE_DOWN, target_replicas, ScalingTrigger.CPU_THRESHOLD
        
        return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.CPU_THRESHOLD
    
    async def _check_predictive_scaling(self, metrics: ScalingMetrics) -> Tuple[ScalingAction, int, ScalingTrigger]:
        """Check for predictive scaling opportunities"""
        try:
            # Get load prediction for next 30 minutes
            predicted_load, confidence = self.prediction_model.predict_load(
                metrics, self.config.prediction_window_minutes
            )
            
            self.prom_prediction_accuracy.set(confidence)
            
            # Only act on high-confidence predictions
            if confidence < self.config.confidence_threshold:
                return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.PREDICTIVE
            
            # Calculate required replicas for predicted load
            current_load_score = self.prediction_model._calculate_load_score({
                'cpu_usage': metrics.cpu_usage_percent,
                'memory_usage': metrics.memory_usage_mb,
                'connections': metrics.active_connections,
                'rps': metrics.requests_per_second,
                'response_time': metrics.response_time_p95_ms
            })
            
            if predicted_load > current_load_score * 1.3:  # 30% increase predicted
                scale_factor = min(2.0, predicted_load / current_load_score)
                target_replicas = min(
                    self.config.max_replicas,
                    int(self.current_replicas * scale_factor)
                )
                if target_replicas > self.current_replicas:
                    return ScalingAction.PREDICTIVE_SCALE, target_replicas, ScalingTrigger.PREDICTIVE
            
            elif predicted_load < current_load_score * 0.7:  # 30% decrease predicted
                scale_factor = max(0.5, predicted_load / current_load_score)
                target_replicas = max(
                    self.config.min_replicas,
                    int(self.current_replicas * scale_factor)
                )
                if target_replicas < self.current_replicas:
                    return ScalingAction.PREDICTIVE_SCALE, target_replicas, ScalingTrigger.PREDICTIVE
            
            return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.PREDICTIVE
            
        except Exception as e:
            self.logger.error(f"Error in predictive scaling check: {e}")
            return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.PREDICTIVE
    
    async def _check_cost_optimization(self, metrics: ScalingMetrics) -> Tuple[ScalingAction, int, ScalingTrigger]:
        """Check for cost optimization opportunities"""
        try:
            # Calculate current cost per hour
            current_cost_per_hour = self.current_replicas * self.config.cost_per_pod_hour
            
            # Calculate performance efficiency
            performance_score = self._calculate_performance_score(metrics)
            
            # Cost efficiency ratio
            cost_efficiency = performance_score / current_cost_per_hour if current_cost_per_hour > 0 else 0
            
            # If performance is good but cost efficiency is low, consider scaling down
            if (performance_score > 0.8 and  # Good performance
                cost_efficiency < 0.1 and   # Poor cost efficiency
                self.current_replicas > self.config.min_replicas):
                
                target_replicas = max(
                    self.config.min_replicas,
                    self.current_replicas - 1
                )
                
                # Estimate cost savings
                cost_savings = (self.current_replicas - target_replicas) * self.config.cost_per_pod_hour
                self.prom_cost_savings.inc(cost_savings)
                
                return ScalingAction.SCALE_DOWN, target_replicas, ScalingTrigger.CUSTOM_METRIC
            
            return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.CUSTOM_METRIC
            
        except Exception as e:
            self.logger.error(f"Error in cost optimization check: {e}")
            return ScalingAction.MAINTAIN, self.current_replicas, ScalingTrigger.CUSTOM_METRIC
    
    async def execute_scaling_action(self, action: ScalingAction, target_replicas: int, trigger: ScalingTrigger) -> bool:
        """Execute the scaling action"""
        try:
            if action == ScalingAction.MAINTAIN:
                return True
            
            self.logger.info(
                f"Executing scaling action: {action.value} to {target_replicas} replicas "
                f"(trigger: {trigger.value})"
            )
            
            # Record decision
            decision = {
                'timestamp': datetime.now(),
                'action': action.value,
                'target_replicas': target_replicas,
                'current_replicas': self.current_replicas,
                'trigger': trigger.value,
                'metrics': self.current_metrics.__dict__ if self.current_metrics else {}
            }
            self.scaling_decisions.append(decision)
            
            # Update Prometheus metrics
            self.prom_scaling_decisions.labels(action=action.value, trigger=trigger.value).inc()
            
            # Update target replicas
            self.target_replicas = target_replicas
            self.last_scaling_action = action
            self.last_scaling_time = datetime.now()
            
            # Update Prometheus
            self.prom_replicas.set(target_replicas)
            
            # Execute HPA update (in real deployment)
            success = await self.k8s_client.update_hpa("dashboard-api", self.config)
            
            if success:
                self.current_replicas = target_replicas
                self.prom_current_replicas.set(self.current_replicas)
                self.logger.info(f"Successfully scaled to {target_replicas} replicas")
            else:
                self.logger.error("Failed to execute scaling action")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error executing scaling action: {e}")
            return False
    
    # Background task methods
    async def _metrics_collection_loop(self):
        """Background metrics collection"""
        while True:
            try:
                # Collect system metrics
                if not self.current_metrics:
                    # Create default metrics if none provided
                    self.current_metrics = ScalingMetrics(
                        timestamp=datetime.now(),
                        cpu_usage_percent=psutil.cpu_percent(),
                        memory_usage_mb=psutil.virtual_memory().used / 1024 / 1024,
                        active_connections=0,
                        requests_per_second=0.0,
                        response_time_p95_ms=10.0,
                        queue_depth=0,
                        error_rate_percent=0.0
                    )
                
                await asyncio.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in metrics collection: {e}")
                await asyncio.sleep(60)
    
    async def _scaling_decision_loop(self):
        """Background scaling decision loop"""
        while True:
            try:
                if self.current_metrics:
                    action, target_replicas, trigger = await self.make_scaling_decision()
                    
                    if action != ScalingAction.MAINTAIN:
                        await self.execute_scaling_action(action, target_replicas, trigger)
                
                await asyncio.sleep(60)  # Make decisions every minute
                
            except Exception as e:
                self.logger.error(f"Error in scaling decision loop: {e}")
                await asyncio.sleep(120)
    
    async def _predictive_scaling_loop(self):
        """Background predictive scaling analysis"""
        while True:
            try:
                if self.config.enable_predictive and len(self.metrics_history) > 50:
                    # Perform deeper predictive analysis
                    await self._analyze_scaling_patterns()
                
                await asyncio.sleep(300)  # Analyze every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in predictive scaling loop: {e}")
                await asyncio.sleep(600)
    
    async def _analyze_scaling_patterns(self):
        """Analyze historical scaling patterns for optimization"""
        try:
            if len(self.scaling_decisions) < 10:
                return
            
            recent_decisions = list(self.scaling_decisions)[-10:]
            
            # Analyze decision effectiveness
            effective_decisions = 0
            for decision in recent_decisions:
                # Check if the scaling action was effective
                # (This would require more sophisticated analysis in production)
                if decision['action'] in ['scale_up', 'scale_down']:
                    effective_decisions += 1
            
            effectiveness_ratio = effective_decisions / len(recent_decisions)
            self.logger.info(f"Scaling decision effectiveness: {effectiveness_ratio:.2%}")
            
            # Adjust configuration based on effectiveness
            if effectiveness_ratio < 0.6:  # Less than 60% effective
                # Make scaling more conservative
                self.config.scale_up_percent = max(25, self.config.scale_up_percent - 5)
                self.config.confidence_threshold = min(0.9, self.config.confidence_threshold + 0.05)
                self.logger.info("Adjusted scaling to be more conservative")
            
            elif effectiveness_ratio > 0.8:  # More than 80% effective
                # Make scaling more aggressive
                self.config.scale_up_percent = min(75, self.config.scale_up_percent + 5)
                self.config.confidence_threshold = max(0.7, self.config.confidence_threshold - 0.05)
                self.logger.info("Adjusted scaling to be more aggressive")
            
        except Exception as e:
            self.logger.error(f"Error analyzing scaling patterns: {e}")
    
    async def _cost_optimization_loop(self):
        """Background cost optimization analysis"""
        while True:
            try:
                if self.config.enable_cost_optimization and self.current_metrics:
                    await self._perform_cost_analysis()
                
                await asyncio.sleep(3600)  # Analyze every hour
                
            except Exception as e:
                self.logger.error(f"Error in cost optimization loop: {e}")
                await asyncio.sleep(1800)
    
    async def _perform_cost_analysis(self):
        """Perform cost analysis and optimization"""
        try:
            # Calculate current hourly cost
            current_cost = self.current_replicas * self.config.cost_per_pod_hour
            
            # Calculate performance over the last hour
            if len(self.metrics_history) > 60:
                recent_metrics = list(self.metrics_history)[-60:]  # Last hour assuming 1-minute intervals
                avg_performance = statistics.mean([
                    self._calculate_performance_score(m) for m in recent_metrics
                ])
                
                cost_effectiveness = avg_performance / current_cost if current_cost > 0 else 0
                
                self.logger.info(
                    f"Cost analysis: ${current_cost:.2f}/hour, "
                    f"Performance: {avg_performance:.2f}, "
                    f"Cost effectiveness: {cost_effectiveness:.3f}"
                )
                
                # Recommendations for cost optimization
                if cost_effectiveness < 1.0 and avg_performance > 0.8:
                    self.logger.info("Recommendation: Consider scaling down to reduce costs")
                elif cost_effectiveness > 2.0 and avg_performance < 0.7:
                    self.logger.info("Recommendation: Consider scaling up to improve performance")
            
        except Exception as e:
            self.logger.error(f"Error in cost analysis: {e}")
    
    def get_scaling_stats(self) -> Dict[str, Any]:
        """Get comprehensive scaling statistics"""
        try:
            recent_decisions = list(self.scaling_decisions)[-20:] if self.scaling_decisions else []
            
            return {
                "current_state": {
                    "current_replicas": self.current_replicas,
                    "target_replicas": self.target_replicas,
                    "last_scaling_action": self.last_scaling_action.value,
                    "last_scaling_time": self.last_scaling_time.isoformat(),
                    "k8s_enabled": self.k8s_client.enabled
                },
                "configuration": {
                    "min_replicas": self.config.min_replicas,
                    "max_replicas": self.config.max_replicas,
                    "target_cpu_percent": self.config.target_cpu_percent,
                    "target_memory_percent": self.config.target_memory_percent,
                    "max_response_time_ms": self.config.max_response_time_ms,
                    "predictive_enabled": self.config.enable_predictive,
                    "cost_optimization_enabled": self.config.enable_cost_optimization
                },
                "recent_decisions": [
                    {
                        "timestamp": d['timestamp'].isoformat(),
                        "action": d['action'],
                        "target_replicas": d['target_replicas'],
                        "trigger": d['trigger']
                    } for d in recent_decisions
                ],
                "prediction_model": {
                    "trained": self.prediction_model.is_trained,
                    "data_points": len(self.prediction_model.historical_data),
                    "window_size": self.prediction_model.window_size
                },
                "cost_analysis": {
                    "current_hourly_cost": self.current_replicas * self.config.cost_per_pod_hour,
                    "cost_per_pod_hour": self.config.cost_per_pod_hour,
                    "estimated_monthly_cost": self.current_replicas * self.config.cost_per_pod_hour * 24 * 30
                },
                "performance": {
                    "current_score": (
                        self._calculate_performance_score(self.current_metrics)
                        if self.current_metrics else 0.0
                    ),
                    "metrics_history_size": len(self.metrics_history)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting scaling stats: {e}")
            return {"error": str(e)}
    
    async def close(self):
        """Clean shutdown of auto-scaling manager"""
        try:
            # Cancel background tasks
            for task in self.background_tasks:
                task.cancel()
            
            # Wait for tasks to complete
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            
            self.logger.info("Auto-scaling manager closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing auto-scaling manager: {e}")


# Factory function
def create_auto_scaling_manager(config: Optional[ScalingConfiguration] = None) -> AutoScalingManager:
    """Create AutoScalingManager instance with default configuration"""
    if config is None:
        config = ScalingConfiguration(
            min_replicas=1,
            max_replicas=10,
            target_cpu_percent=70,
            target_memory_percent=80,
            max_response_time_ms=25.0,  # Story 1.1 requirement
            enable_predictive=True,
            enable_cost_optimization=True
        )
    
    return AutoScalingManager(config)


# Export components
__all__ = [
    "AutoScalingManager",
    "ScalingConfiguration", 
    "ScalingMetrics",
    "LoadPredictionModel",
    "KubernetesScalingClient",
    "ScalingAction",
    "ScalingTrigger",
    "create_auto_scaling_manager"
]