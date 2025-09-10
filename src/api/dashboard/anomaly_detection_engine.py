"""
Advanced Anomaly Detection Engine for Business Intelligence Dashboard
Implements statistical and ML-based anomaly detection with automated alerting
"""
import asyncio
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
from collections import deque

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import redis.asyncio as aioredis

from src.core.database.async_db_manager import get_async_db_session
from src.api.dashboard.dashboard_cache_manager import create_dashboard_cache_manager
from src.streaming.real_time_dashboard_processor import create_real_time_dashboard_processor
from core.config.unified_config import get_unified_config
from core.logging import get_logger


class AnomalySeverity(Enum):
    """Anomaly severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AnomalyType(Enum):
    """Types of anomalies"""
    STATISTICAL = "statistical"      # Statistical outliers (Z-score, IQR)
    TREND = "trend"                  # Trend anomalies
    SEASONAL = "seasonal"            # Seasonal pattern anomalies
    CONTEXTUAL = "contextual"        # Context-specific anomalies
    COLLECTIVE = "collective"        # Group/collective anomalies
    ML_BASED = "ml_based"           # Machine learning based


class AlertChannel(Enum):
    """Alert delivery channels"""
    EMAIL = "email"
    SMS = "sms"
    SLACK = "slack"
    WEBHOOK = "webhook"
    DASHBOARD = "dashboard"
    PAGERDUTY = "pagerduty"


@dataclass
class AnomalyConfig:
    """Configuration for anomaly detection"""
    kpi_name: str
    detection_methods: List[AnomalyType]
    sensitivity: float = 0.95  # Confidence level
    window_size: int = 24  # Hours of data for analysis
    min_data_points: int = 10
    z_score_threshold: float = 2.0
    iqr_multiplier: float = 1.5
    trend_threshold: float = 0.2  # 20% change threshold
    enable_ml_detection: bool = True
    alert_channels: List[AlertChannel] = field(default_factory=lambda: [AlertChannel.DASHBOARD])
    alert_threshold: Optional[float] = None
    business_rules: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Anomaly:
    """Detected anomaly information"""
    id: str
    kpi_name: str
    timestamp: datetime
    value: float
    expected_value: float
    deviation: float
    severity: AnomalySeverity
    anomaly_type: AnomalyType
    confidence_score: float
    z_score: Optional[float] = None
    description: str = ""
    context: Dict[str, Any] = field(default_factory=dict)
    is_resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert anomaly to dictionary"""
        return {
            "id": self.id,
            "kpi_name": self.kpi_name,
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "expected_value": self.expected_value,
            "deviation": self.deviation,
            "severity": self.severity.value,
            "anomaly_type": self.anomaly_type.value,
            "confidence_score": self.confidence_score,
            "z_score": self.z_score,
            "description": self.description,
            "context": self.context,
            "is_resolved": self.is_resolved,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None
        }


@dataclass  
class AlertRule:
    """Alert rule configuration"""
    id: str
    name: str
    kpi_name: str
    condition: str  # "above", "below", "change", "anomaly"
    threshold: Union[float, Dict[str, float]]
    channels: List[AlertChannel]
    severity: AnomalySeverity
    enabled: bool = True
    cooldown_minutes: int = 30
    business_hours_only: bool = False
    recipients: List[str] = field(default_factory=list)
    custom_message: Optional[str] = None


class AnomalyDetectionEngine:
    """
    Advanced anomaly detection engine with multiple detection algorithms
    and intelligent alerting capabilities
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.config = get_unified_config()
        
        # Data storage for historical analysis
        self.historical_data: Dict[str, deque] = {}
        self.max_history_size = 1000  # Keep last 1000 data points
        
        # Anomaly configurations
        self.anomaly_configs: Dict[str, AnomalyConfig] = {}
        
        # Alert rules
        self.alert_rules: Dict[str, AlertRule] = {}
        
        # Recent alerts (for cooldown management)
        self.recent_alerts: Dict[str, datetime] = {}
        
        # ML models for each KPI
        self.ml_models: Dict[str, IsolationForest] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        
        # Components
        self.dashboard_cache = create_dashboard_cache_manager()
        
        # Performance metrics
        self.metrics = {
            "anomalies_detected": 0,
            "alerts_sent": 0,
            "false_positives": 0,
            "detection_accuracy": 0.0,
            "avg_detection_time_ms": 0.0
        }
        
        # Initialize default configurations
        self._initialize_default_configs()
    
    def _initialize_default_configs(self):
        """Initialize default anomaly detection configurations"""
        # Revenue KPIs
        self.anomaly_configs["hourly_revenue"] = AnomalyConfig(
            kpi_name="hourly_revenue",
            detection_methods=[
                AnomalyType.STATISTICAL, 
                AnomalyType.TREND, 
                AnomalyType.ML_BASED
            ],
            sensitivity=0.95,
            window_size=48,  # 48 hours
            z_score_threshold=2.5,
            trend_threshold=0.25,  # 25% change
            alert_channels=[AlertChannel.DASHBOARD, AlertChannel.EMAIL],
            business_rules={
                "min_revenue_threshold": 1000,
                "exclude_weekends": False,
                "seasonal_adjustment": True
            }
        )
        
        # Customer metrics
        self.anomaly_configs["active_customers"] = AnomalyConfig(
            kpi_name="active_customers",
            detection_methods=[AnomalyType.STATISTICAL, AnomalyType.CONTEXTUAL],
            sensitivity=0.90,
            window_size=24,
            z_score_threshold=2.0,
            alert_channels=[AlertChannel.DASHBOARD]
        )
        
        # System performance
        self.anomaly_configs["system_performance"] = AnomalyConfig(
            kpi_name="system_performance",
            detection_methods=[AnomalyType.STATISTICAL, AnomalyType.TREND],
            sensitivity=0.99,
            window_size=12,
            z_score_threshold=1.5,
            trend_threshold=0.10,
            alert_channels=[AlertChannel.DASHBOARD, AlertChannel.SLACK, AlertChannel.PAGERDUTY],
            business_rules={
                "critical_threshold": 0.95,
                "immediate_alert": True
            }
        )
        
        # Initialize default alert rules
        self._initialize_default_alert_rules()
    
    def _initialize_default_alert_rules(self):
        """Initialize default alert rules"""
        self.alert_rules["revenue_drop"] = AlertRule(
            id="revenue_drop",
            name="Revenue Drop Alert",
            kpi_name="hourly_revenue",
            condition="below",
            threshold={"percentage": -20},  # 20% drop
            channels=[AlertChannel.EMAIL, AlertChannel.DASHBOARD],
            severity=AnomalySeverity.HIGH,
            cooldown_minutes=60,
            recipients=["executives@company.com", "analytics@company.com"]
        )
        
        self.alert_rules["performance_degradation"] = AlertRule(
            id="performance_degradation", 
            name="System Performance Degradation",
            kpi_name="system_performance",
            condition="below",
            threshold=0.95,
            channels=[AlertChannel.PAGERDUTY, AlertChannel.SLACK],
            severity=AnomalySeverity.CRITICAL,
            cooldown_minutes=15,
            recipients=["ops-team@company.com"]
        )
    
    async def detect_anomalies(self, kpi_name: str, current_value: float, timestamp: datetime) -> List[Anomaly]:
        """Detect anomalies for a given KPI value"""
        start_time = datetime.now()
        
        try:
            config = self.anomaly_configs.get(kpi_name)
            if not config:
                self.logger.debug(f"No anomaly config found for KPI: {kpi_name}")
                return []
            
            # Get historical data
            historical_data = await self._get_historical_data(kpi_name, config.window_size)
            
            if len(historical_data) < config.min_data_points:
                self.logger.debug(f"Insufficient historical data for {kpi_name}: {len(historical_data)} points")
                return []
            
            anomalies = []
            
            # Run different detection methods
            for method in config.detection_methods:
                anomaly = await self._detect_by_method(
                    method, kpi_name, current_value, timestamp, historical_data, config
                )
                if anomaly:
                    anomalies.append(anomaly)
            
            # Remove duplicates and rank by severity
            unique_anomalies = self._deduplicate_anomalies(anomalies)
            
            # Update metrics
            detection_time = (datetime.now() - start_time).total_seconds() * 1000
            self.metrics["avg_detection_time_ms"] = (
                (self.metrics["avg_detection_time_ms"] + detection_time) / 2
            )
            
            if unique_anomalies:
                self.metrics["anomalies_detected"] += len(unique_anomalies)
                self.logger.info(f"Detected {len(unique_anomalies)} anomalies for {kpi_name}")
            
            return unique_anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting anomalies for {kpi_name}: {e}")
            return []
    
    async def _get_historical_data(self, kpi_name: str, window_hours: int) -> List[Tuple[datetime, float]]:
        """Get historical data for anomaly detection"""
        try:
            # First check in-memory cache
            if kpi_name in self.historical_data:
                return list(self.historical_data[kpi_name])
            
            # Fetch from database
            async with get_async_db_session() as session:
                query = text("""
                    SELECT timestamp, value 
                    FROM kpi_history 
                    WHERE kpi_name = :kpi_name 
                    AND timestamp >= :start_time 
                    ORDER BY timestamp ASC
                """)
                
                start_time = datetime.now() - timedelta(hours=window_hours)
                result = await session.execute(query, {
                    "kpi_name": kpi_name,
                    "start_time": start_time
                })
                
                data = [(row.timestamp, float(row.value)) for row in result.fetchall()]
                
                # Cache the data
                self.historical_data[kpi_name] = deque(data, maxlen=self.max_history_size)
                
                return data
                
        except Exception as e:
            self.logger.error(f"Error getting historical data for {kpi_name}: {e}")
            return []
    
    async def _detect_by_method(
        self, 
        method: AnomalyType, 
        kpi_name: str, 
        current_value: float, 
        timestamp: datetime,
        historical_data: List[Tuple[datetime, float]], 
        config: AnomalyConfig
    ) -> Optional[Anomaly]:
        """Detect anomaly using specific method"""
        try:
            values = [item[1] for item in historical_data]
            
            if method == AnomalyType.STATISTICAL:
                return await self._statistical_detection(
                    kpi_name, current_value, timestamp, values, config
                )
            elif method == AnomalyType.TREND:
                return await self._trend_detection(
                    kpi_name, current_value, timestamp, historical_data, config
                )
            elif method == AnomalyType.ML_BASED and config.enable_ml_detection:
                return await self._ml_detection(
                    kpi_name, current_value, timestamp, values, config
                )
            elif method == AnomalyType.SEASONAL:
                return await self._seasonal_detection(
                    kpi_name, current_value, timestamp, historical_data, config
                )
            elif method == AnomalyType.CONTEXTUAL:
                return await self._contextual_detection(
                    kpi_name, current_value, timestamp, historical_data, config
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in {method.value} detection for {kpi_name}: {e}")
            return None
    
    async def _statistical_detection(
        self, 
        kpi_name: str, 
        current_value: float, 
        timestamp: datetime, 
        values: List[float], 
        config: AnomalyConfig
    ) -> Optional[Anomaly]:
        """Statistical anomaly detection using Z-score and IQR"""
        try:
            if len(values) < 3:
                return None
                
            mean = np.mean(values)
            std = np.std(values)
            
            if std == 0:
                return None
            
            z_score = (current_value - mean) / std
            
            # Z-score based detection
            if abs(z_score) > config.z_score_threshold:
                severity = self._calculate_severity(abs(z_score), config.z_score_threshold)
                
                return Anomaly(
                    id=f"{kpi_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}_statistical",
                    kpi_name=kpi_name,
                    timestamp=timestamp,
                    value=current_value,
                    expected_value=mean,
                    deviation=abs(current_value - mean),
                    severity=severity,
                    anomaly_type=AnomalyType.STATISTICAL,
                    confidence_score=min(abs(z_score) / 5.0, 1.0),  # Normalize to 0-1
                    z_score=z_score,
                    description=f"Statistical outlier: {z_score:.2f} standard deviations from mean",
                    context={
                        "mean": mean,
                        "std": std,
                        "threshold": config.z_score_threshold,
                        "method": "z_score"
                    }
                )
            
            # IQR based detection
            q25, q75 = np.percentile(values, [25, 75])
            iqr = q75 - q25
            
            if iqr > 0:
                lower_bound = q25 - config.iqr_multiplier * iqr
                upper_bound = q75 + config.iqr_multiplier * iqr
                
                if current_value < lower_bound or current_value > upper_bound:
                    deviation_ratio = max(
                        (lower_bound - current_value) / iqr if current_value < lower_bound else 0,
                        (current_value - upper_bound) / iqr if current_value > upper_bound else 0
                    )
                    
                    severity = self._calculate_severity(deviation_ratio, config.iqr_multiplier)
                    
                    return Anomaly(
                        id=f"{kpi_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}_iqr",
                        kpi_name=kpi_name,
                        timestamp=timestamp,
                        value=current_value,
                        expected_value=(q25 + q75) / 2,
                        deviation=min(abs(current_value - lower_bound), abs(current_value - upper_bound)),
                        severity=severity,
                        anomaly_type=AnomalyType.STATISTICAL,
                        confidence_score=min(deviation_ratio / 3.0, 1.0),
                        description=f"IQR outlier: outside [{lower_bound:.2f}, {upper_bound:.2f}] range",
                        context={
                            "q25": q25,
                            "q75": q75,
                            "iqr": iqr,
                            "lower_bound": lower_bound,
                            "upper_bound": upper_bound,
                            "method": "iqr"
                        }
                    )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in statistical detection: {e}")
            return None
    
    async def _trend_detection(
        self, 
        kpi_name: str, 
        current_value: float, 
        timestamp: datetime,
        historical_data: List[Tuple[datetime, float]], 
        config: AnomalyConfig
    ) -> Optional[Anomaly]:
        """Trend-based anomaly detection"""
        try:
            if len(historical_data) < 2:
                return None
            
            # Get recent values for trend analysis
            recent_values = [item[1] for item in historical_data[-5:]]  # Last 5 values
            
            if len(recent_values) < 2:
                return None
            
            # Calculate trend
            previous_value = recent_values[-1]
            if previous_value == 0:
                return None
            
            change_percentage = (current_value - previous_value) / previous_value
            
            if abs(change_percentage) > config.trend_threshold:
                severity = self._calculate_severity(
                    abs(change_percentage), 
                    config.trend_threshold
                )
                
                trend_direction = "increase" if change_percentage > 0 else "decrease"
                
                return Anomaly(
                    id=f"{kpi_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}_trend",
                    kpi_name=kpi_name,
                    timestamp=timestamp,
                    value=current_value,
                    expected_value=previous_value,
                    deviation=abs(current_value - previous_value),
                    severity=severity,
                    anomaly_type=AnomalyType.TREND,
                    confidence_score=min(abs(change_percentage) / 0.5, 1.0),
                    description=f"Significant trend anomaly: {change_percentage*100:.1f}% {trend_direction}",
                    context={
                        "previous_value": previous_value,
                        "change_percentage": change_percentage,
                        "trend_direction": trend_direction,
                        "threshold": config.trend_threshold
                    }
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in trend detection: {e}")
            return None
    
    async def _ml_detection(
        self, 
        kpi_name: str, 
        current_value: float, 
        timestamp: datetime, 
        values: List[float], 
        config: AnomalyConfig
    ) -> Optional[Anomaly]:
        """Machine learning based anomaly detection using Isolation Forest"""
        try:
            if len(values) < 20:  # Need sufficient data for ML
                return None
            
            # Prepare data
            X = np.array(values).reshape(-1, 1)
            
            # Initialize or get existing model
            if kpi_name not in self.ml_models:
                self.ml_models[kpi_name] = IsolationForest(
                    contamination=0.1,  # Expected 10% outliers
                    random_state=42,
                    n_estimators=100
                )
                self.scalers[kpi_name] = StandardScaler()
                
                # Fit the model
                X_scaled = self.scalers[kpi_name].fit_transform(X)
                self.ml_models[kpi_name].fit(X_scaled)
            
            # Predict anomaly for current value
            current_scaled = self.scalers[kpi_name].transform([[current_value]])
            anomaly_score = self.ml_models[kpi_name].decision_function(current_scaled)[0]
            is_anomaly = self.ml_models[kpi_name].predict(current_scaled)[0] == -1
            
            if is_anomaly:
                # Calculate confidence based on decision function score
                confidence = min(abs(anomaly_score) / 0.5, 1.0)
                
                # Estimate expected value using recent normal values
                normal_predictions = self.ml_models[kpi_name].predict(
                    self.scalers[kpi_name].transform(X[-10:])
                )
                normal_values = [values[i] for i, pred in enumerate(normal_predictions[-10:]) if pred == 1]
                expected_value = np.mean(normal_values) if normal_values else np.mean(values)
                
                severity = self._calculate_severity(confidence, 0.5)
                
                return Anomaly(
                    id=f"{kpi_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}_ml",
                    kpi_name=kpi_name,
                    timestamp=timestamp,
                    value=current_value,
                    expected_value=expected_value,
                    deviation=abs(current_value - expected_value),
                    severity=severity,
                    anomaly_type=AnomalyType.ML_BASED,
                    confidence_score=confidence,
                    description=f"ML-detected anomaly (Isolation Forest score: {anomaly_score:.3f})",
                    context={
                        "anomaly_score": anomaly_score,
                        "model_type": "IsolationForest",
                        "training_samples": len(values)
                    }
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in ML detection: {e}")
            return None
    
    async def _seasonal_detection(
        self,
        kpi_name: str,
        current_value: float,
        timestamp: datetime,
        historical_data: List[Tuple[datetime, float]],
        config: AnomalyConfig
    ) -> Optional[Anomaly]:
        """Seasonal pattern anomaly detection"""
        try:
            # Simple seasonal detection based on hour of day
            current_hour = timestamp.hour
            
            # Group historical data by hour
            hourly_data = {}
            for ts, value in historical_data:
                hour = ts.hour
                if hour not in hourly_data:
                    hourly_data[hour] = []
                hourly_data[hour].append(value)
            
            if current_hour not in hourly_data or len(hourly_data[current_hour]) < 3:
                return None
            
            # Calculate seasonal baseline
            seasonal_values = hourly_data[current_hour]
            seasonal_mean = np.mean(seasonal_values)
            seasonal_std = np.std(seasonal_values)
            
            if seasonal_std == 0:
                return None
            
            seasonal_z_score = (current_value - seasonal_mean) / seasonal_std
            
            if abs(seasonal_z_score) > 2.0:  # Seasonal threshold
                severity = self._calculate_severity(abs(seasonal_z_score), 2.0)
                
                return Anomaly(
                    id=f"{kpi_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}_seasonal",
                    kpi_name=kpi_name,
                    timestamp=timestamp,
                    value=current_value,
                    expected_value=seasonal_mean,
                    deviation=abs(current_value - seasonal_mean),
                    severity=severity,
                    anomaly_type=AnomalyType.SEASONAL,
                    confidence_score=min(abs(seasonal_z_score) / 4.0, 1.0),
                    description=f"Seasonal anomaly: {seasonal_z_score:.2f} deviations from hour {current_hour} baseline",
                    context={
                        "hour": current_hour,
                        "seasonal_mean": seasonal_mean,
                        "seasonal_std": seasonal_std,
                        "seasonal_z_score": seasonal_z_score,
                        "seasonal_samples": len(seasonal_values)
                    }
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in seasonal detection: {e}")
            return None
    
    async def _contextual_detection(
        self,
        kpi_name: str,
        current_value: float,
        timestamp: datetime,
        historical_data: List[Tuple[datetime, float]],
        config: AnomalyConfig
    ) -> Optional[Anomaly]:
        """Contextual anomaly detection based on business rules"""
        try:
            business_rules = config.business_rules
            
            # Weekend vs weekday context
            if business_rules.get("exclude_weekends") and timestamp.weekday() >= 5:
                return None  # Skip weekend analysis
            
            # Business hours context
            if business_rules.get("business_hours_only"):
                if timestamp.hour < 9 or timestamp.hour > 17:
                    return None  # Skip non-business hours
            
            # Minimum threshold context
            min_threshold = business_rules.get("min_revenue_threshold")
            if min_threshold and current_value < min_threshold:
                severity = AnomalySeverity.HIGH if current_value < min_threshold * 0.5 else AnomalySeverity.MEDIUM
                
                return Anomaly(
                    id=f"{kpi_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}_contextual",
                    kpi_name=kpi_name,
                    timestamp=timestamp,
                    value=current_value,
                    expected_value=min_threshold,
                    deviation=min_threshold - current_value,
                    severity=severity,
                    anomaly_type=AnomalyType.CONTEXTUAL,
                    confidence_score=0.9,
                    description=f"Below minimum business threshold: {current_value} < {min_threshold}",
                    context={
                        "min_threshold": min_threshold,
                        "business_rule": "minimum_threshold",
                        "context": "business_rules"
                    }
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in contextual detection: {e}")
            return None
    
    def _calculate_severity(self, score: float, threshold: float) -> AnomalySeverity:
        """Calculate anomaly severity based on score and threshold"""
        if score > threshold * 3:
            return AnomalySeverity.CRITICAL
        elif score > threshold * 2:
            return AnomalySeverity.HIGH
        elif score > threshold * 1.5:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW
    
    def _deduplicate_anomalies(self, anomalies: List[Anomaly]) -> List[Anomaly]:
        """Remove duplicate anomalies and keep the highest severity"""
        if not anomalies:
            return []
        
        # Group by KPI and timestamp (same minute)
        groups = {}
        for anomaly in anomalies:
            key = (anomaly.kpi_name, anomaly.timestamp.strftime('%Y%m%d_%H%M'))
            if key not in groups:
                groups[key] = []
            groups[key].append(anomaly)
        
        # Keep the highest severity anomaly from each group
        unique_anomalies = []
        severity_order = {
            AnomalySeverity.CRITICAL: 4,
            AnomalySeverity.HIGH: 3,
            AnomalySeverity.MEDIUM: 2,
            AnomalySeverity.LOW: 1
        }
        
        for group in groups.values():
            best_anomaly = max(group, key=lambda a: severity_order[a.severity])
            unique_anomalies.append(best_anomaly)
        
        return unique_anomalies
    
    async def process_anomalies(self, anomalies: List[Anomaly]) -> Dict[str, Any]:
        """Process detected anomalies and trigger alerts"""
        try:
            if not anomalies:
                return {"processed": 0, "alerts_sent": 0}
            
            alerts_sent = 0
            
            for anomaly in anomalies:
                # Store anomaly in database
                await self._store_anomaly(anomaly)
                
                # Check alert rules and send notifications
                triggered_alerts = await self._check_alert_rules(anomaly)
                alerts_sent += len(triggered_alerts)
                
                # Send to dashboard via WebSocket
                await self._send_dashboard_notification(anomaly)
            
            self.logger.info(f"Processed {len(anomalies)} anomalies, sent {alerts_sent} alerts")
            
            return {
                "processed": len(anomalies),
                "alerts_sent": alerts_sent,
                "anomalies": [a.to_dict() for a in anomalies]
            }
            
        except Exception as e:
            self.logger.error(f"Error processing anomalies: {e}")
            return {"processed": 0, "alerts_sent": 0, "error": str(e)}
    
    async def _store_anomaly(self, anomaly: Anomaly):
        """Store anomaly in database"""
        try:
            async with get_async_db_session() as session:
                insert_sql = text("""
                    INSERT INTO anomalies (
                        id, kpi_name, timestamp, value, expected_value, deviation,
                        severity, anomaly_type, confidence_score, z_score, description, context
                    ) VALUES (
                        :id, :kpi_name, :timestamp, :value, :expected_value, :deviation,
                        :severity, :anomaly_type, :confidence_score, :z_score, :description, :context
                    )
                """)
                
                await session.execute(insert_sql, {
                    "id": anomaly.id,
                    "kpi_name": anomaly.kpi_name,
                    "timestamp": anomaly.timestamp,
                    "value": anomaly.value,
                    "expected_value": anomaly.expected_value,
                    "deviation": anomaly.deviation,
                    "severity": anomaly.severity.value,
                    "anomaly_type": anomaly.anomaly_type.value,
                    "confidence_score": anomaly.confidence_score,
                    "z_score": anomaly.z_score,
                    "description": anomaly.description,
                    "context": json.dumps(anomaly.context)
                })
                
                await session.commit()
                
        except Exception as e:
            self.logger.error(f"Error storing anomaly: {e}")
    
    async def _check_alert_rules(self, anomaly: Anomaly) -> List[str]:
        """Check alert rules and trigger notifications"""
        triggered_alerts = []
        
        try:
            for rule_id, rule in self.alert_rules.items():
                if not rule.enabled or rule.kpi_name != anomaly.kpi_name:
                    continue
                
                # Check cooldown
                last_alert = self.recent_alerts.get(rule_id)
                if last_alert and (datetime.now() - last_alert).total_seconds() < rule.cooldown_minutes * 60:
                    continue
                
                # Check conditions
                if await self._evaluate_alert_condition(rule, anomaly):
                    # Send alert
                    success = await self._send_alert(rule, anomaly)
                    if success:
                        triggered_alerts.append(rule_id)
                        self.recent_alerts[rule_id] = datetime.now()
                        self.metrics["alerts_sent"] += 1
            
            return triggered_alerts
            
        except Exception as e:
            self.logger.error(f"Error checking alert rules: {e}")
            return []
    
    async def _evaluate_alert_condition(self, rule: AlertRule, anomaly: Anomaly) -> bool:
        """Evaluate if alert rule conditions are met"""
        try:
            if rule.condition == "anomaly":
                return True  # Any anomaly triggers the alert
            
            elif rule.condition == "above":
                threshold = rule.threshold if isinstance(rule.threshold, (int, float)) else rule.threshold.get("value", 0)
                return anomaly.value > threshold
            
            elif rule.condition == "below":
                threshold = rule.threshold if isinstance(rule.threshold, (int, float)) else rule.threshold.get("value", 0)
                return anomaly.value < threshold
            
            elif rule.condition == "change":
                change_threshold = rule.threshold.get("percentage", 0.2) if isinstance(rule.threshold, dict) else 0.2
                change_pct = abs(anomaly.deviation) / anomaly.expected_value if anomaly.expected_value > 0 else 0
                return change_pct > change_threshold
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error evaluating alert condition: {e}")
            return False
    
    async def _send_alert(self, rule: AlertRule, anomaly: Anomaly) -> bool:
        """Send alert through configured channels"""
        try:
            success = True
            
            for channel in rule.channels:
                if channel == AlertChannel.EMAIL:
                    success &= await self._send_email_alert(rule, anomaly)
                elif channel == AlertChannel.SLACK:
                    success &= await self._send_slack_alert(rule, anomaly)
                elif channel == AlertChannel.WEBHOOK:
                    success &= await self._send_webhook_alert(rule, anomaly)
                elif channel == AlertChannel.DASHBOARD:
                    success &= await self._send_dashboard_notification(anomaly)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
            return False
    
    async def _send_email_alert(self, rule: AlertRule, anomaly: Anomaly) -> bool:
        """Send email alert (placeholder implementation)"""
        # This would integrate with an email service
        self.logger.info(f"EMAIL ALERT: {rule.name} - {anomaly.description}")
        return True
    
    async def _send_slack_alert(self, rule: AlertRule, anomaly: Anomaly) -> bool:
        """Send Slack alert (placeholder implementation)"""
        # This would integrate with Slack API
        self.logger.info(f"SLACK ALERT: {rule.name} - {anomaly.description}")
        return True
    
    async def _send_webhook_alert(self, rule: AlertRule, anomaly: Anomaly) -> bool:
        """Send webhook alert (placeholder implementation)"""
        # This would send HTTP POST to webhook URL
        self.logger.info(f"WEBHOOK ALERT: {rule.name} - {anomaly.description}")
        return True
    
    async def _send_dashboard_notification(self, anomaly: Anomaly) -> bool:
        """Send real-time dashboard notification"""
        try:
            # This would integrate with the WebSocket manager
            notification_data = {
                "type": "anomaly_alert",
                "data": anomaly.to_dict(),
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"DASHBOARD ALERT: {anomaly.kpi_name} - {anomaly.description}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending dashboard notification: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get anomaly detection metrics"""
        return {
            "anomaly_detection_metrics": self.metrics,
            "configured_kpis": len(self.anomaly_configs),
            "active_alert_rules": len([r for r in self.alert_rules.values() if r.enabled]),
            "ml_models_trained": len(self.ml_models),
            "historical_data_points": sum(len(data) for data in self.historical_data.values())
        }


# Factory function
def create_anomaly_detection_engine() -> AnomalyDetectionEngine:
    """Create AnomalyDetectionEngine instance"""
    return AnomalyDetectionEngine()


# Example usage
async def main():
    """Example usage of anomaly detection engine"""
    engine = create_anomaly_detection_engine()
    
    # Simulate KPI value
    current_time = datetime.now()
    current_value = 1250.0  # Example hourly revenue
    
    # Detect anomalies
    anomalies = await engine.detect_anomalies("hourly_revenue", current_value, current_time)
    
    if anomalies:
        print(f"Detected {len(anomalies)} anomalies:")
        for anomaly in anomalies:
            print(f"  - {anomaly.description} (Severity: {anomaly.severity.value})")
        
        # Process anomalies and send alerts
        result = await engine.process_anomalies(anomalies)
        print(f"Processing result: {result}")
    else:
        print("No anomalies detected")
    
    # Show metrics
    metrics = engine.get_metrics()
    print(f"Metrics: {metrics}")


if __name__ == "__main__":
    asyncio.run(main())