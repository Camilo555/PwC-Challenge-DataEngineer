"""
ML-Powered Anomaly Detection for Data Quality
Advanced machine learning algorithms for detecting data anomalies and quality issues.
"""
from __future__ import annotations

import json
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np
from scipy import stats
from sklearn.cluster import DBSCAN, IsolationForest
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler

from core.logging import get_logger


class AnomalyType(Enum):
    """Types of anomalies that can be detected"""
    STATISTICAL_OUTLIER = "statistical_outlier"
    DISTRIBUTION_SHIFT = "distribution_shift"
    PATTERN_ANOMALY = "pattern_anomaly"
    CORRELATION_BREAK = "correlation_break"
    FREQUENCY_ANOMALY = "frequency_anomaly"
    VALUE_ANOMALY = "value_anomaly"
    TEMPORAL_ANOMALY = "temporal_anomaly"
    CATEGORICAL_ANOMALY = "categorical_anomaly"


class AnomalySeverity(Enum):
    """Severity levels for anomalies"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Anomaly:
    """Anomaly detection result"""
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    confidence_score: float
    description: str
    affected_field: str | None
    affected_records: list[int] = field(default_factory=list)
    detected_at: datetime = field(default_factory=datetime.now)
    details: dict[str, Any] = field(default_factory=dict)
    remediation_suggestion: str | None = None


@dataclass
class AnomalyDetectionConfig:
    """Configuration for anomaly detection"""
    enable_statistical_detection: bool = True
    enable_ml_detection: bool = True
    enable_pattern_detection: bool = True
    statistical_threshold: float = 3.0  # Z-score threshold
    isolation_contamination: float = 0.1
    dbscan_eps: float = 0.5
    dbscan_min_samples: int = 5
    confidence_threshold: float = 0.7
    max_anomalies_per_field: int = 100


class MLAnomalyDetector:
    """
    Machine Learning-powered Anomaly Detector
    
    Uses multiple algorithms to detect various types of data anomalies:
    - Statistical methods (Z-score, IQR, etc.)
    - Isolation Forest for general anomaly detection
    - DBSCAN for density-based clustering
    - Distribution comparison methods
    - Pattern recognition for text data
    """

    def __init__(self, config: AnomalyDetectionConfig | None = None):
        self.config = config or AnomalyDetectionConfig()
        self.logger = get_logger(self.__class__.__name__)
        
        # Initialize models
        self.isolation_forest = None
        self.scaler = StandardScaler()
        self.historical_profiles = {}
        
        # Text analysis components
        self.text_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            lowercase=True
        )

    def detect_anomalies(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any],
        historical_data: list[dict[str, Any]] | None = None
    ) -> list[Anomaly]:
        """
        Comprehensive anomaly detection across multiple dimensions
        
        Args:
            data: Current dataset to analyze
            profile: Data profile with statistics
            historical_data: Optional historical data for comparison
        
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        try:
            if not data:
                return anomalies
            
            # Statistical anomaly detection
            if self.config.enable_statistical_detection:
                statistical_anomalies = self._detect_statistical_anomalies(data, profile)
                anomalies.extend(statistical_anomalies)
            
            # ML-based anomaly detection
            if self.config.enable_ml_detection:
                ml_anomalies = self._detect_ml_anomalies(data, profile)
                anomalies.extend(ml_anomalies)
            
            # Pattern-based anomaly detection
            if self.config.enable_pattern_detection:
                pattern_anomalies = self._detect_pattern_anomalies(data, profile)
                anomalies.extend(pattern_anomalies)
            
            # Temporal anomaly detection
            temporal_anomalies = self._detect_temporal_anomalies(data, profile)
            anomalies.extend(temporal_anomalies)
            
            # Distribution shift detection
            if historical_data:
                distribution_anomalies = self._detect_distribution_shifts(
                    data, historical_data, profile
                )
                anomalies.extend(distribution_anomalies)
            
            # Correlation anomaly detection
            correlation_anomalies = self._detect_correlation_anomalies(data, profile)
            anomalies.extend(correlation_anomalies)
            
            # Filter and rank anomalies
            anomalies = self._filter_and_rank_anomalies(anomalies)
            
            self.logger.info(f"Detected {len(anomalies)} anomalies across all methods")
            
        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {e}", exc_info=True)
        
        return anomalies

    def _detect_statistical_anomalies(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect statistical outliers using various methods"""
        anomalies = []
        field_stats = profile.get('fields', {})
        
        for field_name, stats in field_stats.items():
            try:
                field_type = stats.get('type', 'unknown')
                
                if field_type in ['int', 'float', 'numeric']:
                    # Extract numeric values
                    values = [
                        float(record.get(field_name, 0))
                        for record in data
                        if record.get(field_name) is not None
                        and str(record.get(field_name)).replace('.', '').replace('-', '').isdigit()
                    ]
                    
                    if len(values) < 10:
                        continue
                    
                    # Z-score method
                    z_score_anomalies = self._detect_z_score_anomalies(
                        values, field_name, data
                    )
                    anomalies.extend(z_score_anomalies)
                    
                    # IQR method
                    iqr_anomalies = self._detect_iqr_anomalies(
                        values, field_name, data
                    )
                    anomalies.extend(iqr_anomalies)
                    
                    # Modified Z-score method (more robust)
                    modified_z_anomalies = self._detect_modified_z_score_anomalies(
                        values, field_name, data
                    )
                    anomalies.extend(modified_z_anomalies)
                
                elif field_type in ['string', 'categorical']:
                    # Categorical anomaly detection
                    categorical_anomalies = self._detect_categorical_anomalies(
                        data, field_name, stats
                    )
                    anomalies.extend(categorical_anomalies)
                
            except Exception as e:
                self.logger.error(f"Statistical anomaly detection failed for {field_name}: {e}")
        
        return anomalies

    def _detect_z_score_anomalies(
        self,
        values: list[float],
        field_name: str,
        data: list[dict[str, Any]]
    ) -> list[Anomaly]:
        """Detect anomalies using Z-score method"""
        anomalies = []
        
        if len(values) < 2:
            return anomalies
        
        mean = statistics.mean(values)
        std_dev = statistics.stdev(values)
        
        if std_dev == 0:
            return anomalies
        
        outlier_indices = []
        outlier_values = []
        
        for i, value in enumerate(values):
            z_score = abs((value - mean) / std_dev)
            if z_score > self.config.statistical_threshold:
                outlier_indices.append(i)
                outlier_values.append(value)
        
        if outlier_indices:
            severity = self._calculate_severity(len(outlier_indices), len(values))
            
            anomalies.append(Anomaly(
                anomaly_id=f"zscore_{field_name}_{int(datetime.now().timestamp())}",
                anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                severity=severity,
                confidence_score=min(0.95, (max(abs((v - mean) / std_dev) for v in outlier_values) - 2.0) / 3.0),
                description=f"Z-score outliers detected in {field_name}: {len(outlier_indices)} values",
                affected_field=field_name,
                affected_records=outlier_indices[:self.config.max_anomalies_per_field],
                details={
                    'method': 'z_score',
                    'threshold': self.config.statistical_threshold,
                    'outlier_count': len(outlier_indices),
                    'sample_values': outlier_values[:10],
                    'mean': mean,
                    'std_dev': std_dev
                },
                remediation_suggestion=f"Review extreme values in {field_name}, consider data validation rules"
            ))
        
        return anomalies

    def _detect_iqr_anomalies(
        self,
        values: list[float],
        field_name: str,
        data: list[dict[str, Any]]
    ) -> list[Anomaly]:
        """Detect anomalies using Interquartile Range method"""
        anomalies = []
        
        if len(values) < 4:
            return anomalies
        
        sorted_values = sorted(values)
        q1 = np.percentile(sorted_values, 25)
        q3 = np.percentile(sorted_values, 75)
        iqr = q3 - q1
        
        if iqr == 0:
            return anomalies
        
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outlier_indices = []
        outlier_values = []
        
        for i, value in enumerate(values):
            if value < lower_bound or value > upper_bound:
                outlier_indices.append(i)
                outlier_values.append(value)
        
        if outlier_indices:
            severity = self._calculate_severity(len(outlier_indices), len(values))
            
            anomalies.append(Anomaly(
                anomaly_id=f"iqr_{field_name}_{int(datetime.now().timestamp())}",
                anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                severity=severity,
                confidence_score=0.8,
                description=f"IQR outliers detected in {field_name}: {len(outlier_indices)} values",
                affected_field=field_name,
                affected_records=outlier_indices[:self.config.max_anomalies_per_field],
                details={
                    'method': 'iqr',
                    'q1': q1,
                    'q3': q3,
                    'iqr': iqr,
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound,
                    'outlier_count': len(outlier_indices),
                    'sample_values': outlier_values[:10]
                },
                remediation_suggestion=f"Investigate extreme values in {field_name} outside normal range"
            ))
        
        return anomalies

    def _detect_modified_z_score_anomalies(
        self,
        values: list[float],
        field_name: str,
        data: list[dict[str, Any]]
    ) -> list[Anomaly]:
        """Detect anomalies using Modified Z-score method (more robust)"""
        anomalies = []
        
        if len(values) < 3:
            return anomalies
        
        median = statistics.median(values)
        mad = statistics.median([abs(x - median) for x in values])
        
        if mad == 0:
            return anomalies
        
        threshold = 3.5  # Standard threshold for modified Z-score
        outlier_indices = []
        outlier_values = []
        
        for i, value in enumerate(values):
            modified_z_score = 0.6745 * (value - median) / mad
            if abs(modified_z_score) > threshold:
                outlier_indices.append(i)
                outlier_values.append(value)
        
        if outlier_indices:
            severity = self._calculate_severity(len(outlier_indices), len(values))
            
            anomalies.append(Anomaly(
                anomaly_id=f"modz_{field_name}_{int(datetime.now().timestamp())}",
                anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                severity=severity,
                confidence_score=0.85,
                description=f"Modified Z-score outliers in {field_name}: {len(outlier_indices)} values",
                affected_field=field_name,
                affected_records=outlier_indices[:self.config.max_anomalies_per_field],
                details={
                    'method': 'modified_z_score',
                    'median': median,
                    'mad': mad,
                    'threshold': threshold,
                    'outlier_count': len(outlier_indices),
                    'sample_values': outlier_values[:10]
                },
                remediation_suggestion=f"Review robust outliers in {field_name} using median-based detection"
            ))
        
        return anomalies

    def _detect_categorical_anomalies(
        self,
        data: list[dict[str, Any]],
        field_name: str,
        stats: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect anomalies in categorical data"""
        anomalies = []
        
        try:
            value_counts = stats.get('value_counts', {})
            total_count = sum(value_counts.values())
            
            if total_count < 10:
                return anomalies
            
            # Detect rare categories (frequency < 1% of total)
            rare_threshold = 0.01
            rare_categories = []
            rare_indices = []
            
            for i, record in enumerate(data):
                value = record.get(field_name)
                if value is not None:
                    frequency = value_counts.get(str(value), 0) / total_count
                    if frequency < rare_threshold and frequency > 0:
                        rare_categories.append(str(value))
                        rare_indices.append(i)
            
            if rare_categories:
                unique_rare_categories = list(set(rare_categories))
                
                anomalies.append(Anomaly(
                    anomaly_id=f"rare_cat_{field_name}_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.CATEGORICAL_ANOMALY,
                    severity=AnomalySeverity.LOW,
                    confidence_score=0.7,
                    description=f"Rare categories in {field_name}: {len(unique_rare_categories)} categories",
                    affected_field=field_name,
                    affected_records=rare_indices[:self.config.max_anomalies_per_field],
                    details={
                        'method': 'frequency_analysis',
                        'rare_categories': unique_rare_categories[:20],
                        'threshold': rare_threshold,
                        'total_rare_occurrences': len(rare_categories)
                    },
                    remediation_suggestion=f"Review rare categories in {field_name} for data quality issues"
                ))
        
        except Exception as e:
            self.logger.error(f"Categorical anomaly detection failed for {field_name}: {e}")
        
        return anomalies

    def _detect_ml_anomalies(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect anomalies using machine learning algorithms"""
        anomalies = []
        
        try:
            # Prepare numerical features
            numerical_features = self._extract_numerical_features(data, profile)
            
            if len(numerical_features) < 10:
                return anomalies
            
            # Isolation Forest detection
            isolation_anomalies = self._isolation_forest_detection(
                numerical_features, data
            )
            anomalies.extend(isolation_anomalies)
            
            # DBSCAN clustering-based detection
            dbscan_anomalies = self._dbscan_anomaly_detection(
                numerical_features, data
            )
            anomalies.extend(dbscan_anomalies)
            
        except Exception as e:
            self.logger.error(f"ML anomaly detection failed: {e}")
        
        return anomalies

    def _extract_numerical_features(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> np.ndarray:
        """Extract numerical features for ML algorithms"""
        
        field_stats = profile.get('fields', {})
        numerical_fields = [
            name for name, stats in field_stats.items()
            if stats.get('type') in ['int', 'float', 'numeric']
        ]
        
        if not numerical_fields:
            return np.array([])
        
        features = []
        for record in data:
            feature_vector = []
            for field in numerical_fields:
                value = record.get(field, 0)
                try:
                    feature_vector.append(float(value))
                except (ValueError, TypeError):
                    feature_vector.append(0.0)
            features.append(feature_vector)
        
        return np.array(features)

    def _isolation_forest_detection(
        self,
        features: np.ndarray,
        data: list[dict[str, Any]]
    ) -> list[Anomaly]:
        """Detect anomalies using Isolation Forest"""
        anomalies = []
        
        try:
            if features.shape[0] < 10:
                return anomalies
            
            # Initialize and fit Isolation Forest
            isolation_forest = IsolationForest(
                contamination=self.config.isolation_contamination,
                random_state=42,
                n_estimators=100
            )
            
            # Scale features
            scaled_features = self.scaler.fit_transform(features)
            
            # Detect anomalies
            predictions = isolation_forest.fit_predict(scaled_features)
            anomaly_scores = isolation_forest.decision_function(scaled_features)
            
            # Find anomalous records
            anomalous_indices = np.where(predictions == -1)[0]
            
            if len(anomalous_indices) > 0:
                # Calculate confidence scores
                anomaly_scores_subset = anomaly_scores[anomalous_indices]
                confidence_scores = (anomaly_scores_subset - anomaly_scores.min()) / (
                    anomaly_scores.max() - anomaly_scores.min()
                )
                
                severity = self._calculate_severity(len(anomalous_indices), len(data))
                
                anomalies.append(Anomaly(
                    anomaly_id=f"isolation_forest_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.PATTERN_ANOMALY,
                    severity=severity,
                    confidence_score=float(np.mean(confidence_scores)),
                    description=f"Isolation Forest detected {len(anomalous_indices)} anomalous records",
                    affected_field=None,
                    affected_records=anomalous_indices.tolist()[:self.config.max_anomalies_per_field],
                    details={
                        'method': 'isolation_forest',
                        'contamination': self.config.isolation_contamination,
                        'n_features': features.shape[1],
                        'anomaly_scores': anomaly_scores_subset.tolist()[:10],
                        'mean_anomaly_score': float(np.mean(anomaly_scores_subset))
                    },
                    remediation_suggestion="Review records flagged by ML algorithm for data quality issues"
                ))
        
        except Exception as e:
            self.logger.error(f"Isolation Forest detection failed: {e}")
        
        return anomalies

    def _dbscan_anomaly_detection(
        self,
        features: np.ndarray,
        data: list[dict[str, Any]]
    ) -> list[Anomaly]:
        """Detect anomalies using DBSCAN clustering"""
        anomalies = []
        
        try:
            if features.shape[0] < 20:
                return anomalies
            
            # Scale features
            scaled_features = self.scaler.fit_transform(features)
            
            # Apply DBSCAN
            dbscan = DBSCAN(
                eps=self.config.dbscan_eps,
                min_samples=self.config.dbscan_min_samples
            )
            
            cluster_labels = dbscan.fit_predict(scaled_features)
            
            # Find outliers (labeled as -1)
            outlier_indices = np.where(cluster_labels == -1)[0]
            
            if len(outlier_indices) > 0:
                # Calculate density-based confidence
                core_samples = len(dbscan.core_sample_indices_)
                confidence_score = min(0.9, core_samples / len(data))
                
                severity = self._calculate_severity(len(outlier_indices), len(data))
                
                anomalies.append(Anomaly(
                    anomaly_id=f"dbscan_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.PATTERN_ANOMALY,
                    severity=severity,
                    confidence_score=confidence_score,
                    description=f"DBSCAN clustering identified {len(outlier_indices)} density outliers",
                    affected_field=None,
                    affected_records=outlier_indices.tolist()[:self.config.max_anomalies_per_field],
                    details={
                        'method': 'dbscan',
                        'eps': self.config.dbscan_eps,
                        'min_samples': self.config.dbscan_min_samples,
                        'n_clusters': len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0),
                        'core_samples': core_samples
                    },
                    remediation_suggestion="Investigate density-based outliers for potential data quality issues"
                ))
        
        except Exception as e:
            self.logger.error(f"DBSCAN anomaly detection failed: {e}")
        
        return anomalies

    def _detect_pattern_anomalies(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect pattern anomalies in text and structured data"""
        anomalies = []
        
        field_stats = profile.get('fields', {})
        
        for field_name, stats in field_stats.items():
            if stats.get('type') == 'string':
                try:
                    text_anomalies = self._detect_text_pattern_anomalies(
                        data, field_name, stats
                    )
                    anomalies.extend(text_anomalies)
                except Exception as e:
                    self.logger.error(f"Text pattern detection failed for {field_name}: {e}")
        
        return anomalies

    def _detect_text_pattern_anomalies(
        self,
        data: list[dict[str, Any]],
        field_name: str,
        stats: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect anomalies in text patterns"""
        anomalies = []
        
        # Extract text values
        text_values = [
            str(record.get(field_name, ''))
            for record in data
            if record.get(field_name) is not None
        ]
        
        if len(text_values) < 10:
            return anomalies
        
        # Length anomalies
        lengths = [len(text) for text in text_values]
        if len(set(lengths)) > 1:  # Not all same length
            mean_length = statistics.mean(lengths)
            std_length = statistics.stdev(lengths)
            
            if std_length > 0:
                length_anomalies = []
                for i, length in enumerate(lengths):
                    z_score = abs((length - mean_length) / std_length)
                    if z_score > 2.5:  # More lenient than numerical data
                        length_anomalies.append(i)
                
                if length_anomalies:
                    anomalies.append(Anomaly(
                        anomaly_id=f"text_length_{field_name}_{int(datetime.now().timestamp())}",
                        anomaly_type=AnomalyType.PATTERN_ANOMALY,
                        severity=AnomalySeverity.LOW,
                        confidence_score=0.6,
                        description=f"Unusual text lengths in {field_name}: {len(length_anomalies)} values",
                        affected_field=field_name,
                        affected_records=length_anomalies[:self.config.max_anomalies_per_field],
                        details={
                            'method': 'text_length_analysis',
                            'mean_length': mean_length,
                            'std_length': std_length,
                            'anomalous_lengths': [lengths[i] for i in length_anomalies[:10]]
                        },
                        remediation_suggestion=f"Review unusual text lengths in {field_name}"
                    ))
        
        return anomalies

    def _detect_temporal_anomalies(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect temporal anomalies in date/time fields"""
        anomalies = []
        
        field_stats = profile.get('fields', {})
        
        for field_name, stats in field_stats.items():
            if stats.get('type') in ['datetime', 'date', 'timestamp']:
                try:
                    temporal_anomalies = self._detect_date_anomalies(
                        data, field_name, stats
                    )
                    anomalies.extend(temporal_anomalies)
                except Exception as e:
                    self.logger.error(f"Temporal anomaly detection failed for {field_name}: {e}")
        
        return anomalies

    def _detect_date_anomalies(
        self,
        data: list[dict[str, Any]],
        field_name: str,
        stats: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect anomalies in date/time data"""
        anomalies = []
        
        current_time = datetime.now()
        future_dates = []
        very_old_dates = []
        
        for i, record in enumerate(data):
            date_value = record.get(field_name)
            if date_value:
                try:
                    if isinstance(date_value, str):
                        # Try to parse string dates
                        date_obj = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                    elif isinstance(date_value, datetime):
                        date_obj = date_value
                    else:
                        continue
                    
                    # Check for future dates
                    if date_obj > current_time + timedelta(days=1):
                        future_dates.append(i)
                    
                    # Check for very old dates (more than 50 years)
                    if current_time - date_obj > timedelta(days=50*365):
                        very_old_dates.append(i)
                
                except (ValueError, TypeError, AttributeError):
                    continue
        
        # Report future dates anomaly
        if future_dates:
            anomalies.append(Anomaly(
                anomaly_id=f"future_dates_{field_name}_{int(datetime.now().timestamp())}",
                anomaly_type=AnomalyType.TEMPORAL_ANOMALY,
                severity=AnomalySeverity.HIGH,
                confidence_score=0.9,
                description=f"Future dates detected in {field_name}: {len(future_dates)} records",
                affected_field=field_name,
                affected_records=future_dates[:self.config.max_anomalies_per_field],
                details={
                    'method': 'temporal_validation',
                    'anomaly_type': 'future_dates',
                    'count': len(future_dates)
                },
                remediation_suggestion=f"Review and correct future dates in {field_name}"
            ))
        
        # Report very old dates anomaly
        if very_old_dates:
            anomalies.append(Anomaly(
                anomaly_id=f"old_dates_{field_name}_{int(datetime.now().timestamp())}",
                anomaly_type=AnomalyType.TEMPORAL_ANOMALY,
                severity=AnomalySeverity.MEDIUM,
                confidence_score=0.7,
                description=f"Very old dates in {field_name}: {len(very_old_dates)} records",
                affected_field=field_name,
                affected_records=very_old_dates[:self.config.max_anomalies_per_field],
                details={
                    'method': 'temporal_validation',
                    'anomaly_type': 'very_old_dates',
                    'count': len(very_old_dates)
                },
                remediation_suggestion=f"Verify very old dates in {field_name} are correct"
            ))
        
        return anomalies

    def _detect_distribution_shifts(
        self,
        current_data: list[dict[str, Any]],
        historical_data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect distribution shifts between current and historical data"""
        anomalies = []
        
        field_stats = profile.get('fields', {})
        
        for field_name, stats in field_stats.items():
            if stats.get('type') in ['int', 'float', 'numeric']:
                try:
                    # Extract values from both datasets
                    current_values = [
                        float(record.get(field_name, 0))
                        for record in current_data
                        if record.get(field_name) is not None
                    ]
                    
                    historical_values = [
                        float(record.get(field_name, 0))
                        for record in historical_data
                        if record.get(field_name) is not None
                    ]
                    
                    if len(current_values) < 10 or len(historical_values) < 10:
                        continue
                    
                    # Kolmogorov-Smirnov test for distribution comparison
                    ks_statistic, p_value = stats.ks_2samp(current_values, historical_values)
                    
                    # Significant distribution shift if p-value < 0.05
                    if p_value < 0.05:
                        severity = AnomalySeverity.HIGH if p_value < 0.01 else AnomalySeverity.MEDIUM
                        
                        anomalies.append(Anomaly(
                            anomaly_id=f"dist_shift_{field_name}_{int(datetime.now().timestamp())}",
                            anomaly_type=AnomalyType.DISTRIBUTION_SHIFT,
                            severity=severity,
                            confidence_score=1.0 - p_value,
                            description=f"Distribution shift detected in {field_name}",
                            affected_field=field_name,
                            details={
                                'method': 'kolmogorov_smirnov',
                                'ks_statistic': ks_statistic,
                                'p_value': p_value,
                                'current_mean': statistics.mean(current_values),
                                'historical_mean': statistics.mean(historical_values),
                                'current_std': statistics.stdev(current_values),
                                'historical_std': statistics.stdev(historical_values)
                            },
                            remediation_suggestion=f"Investigate data source changes affecting {field_name} distribution"
                        ))
                
                except Exception as e:
                    self.logger.error(f"Distribution shift detection failed for {field_name}: {e}")
        
        return anomalies

    def _detect_correlation_anomalies(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[Anomaly]:
        """Detect anomalies in field correlations"""
        anomalies = []
        
        try:
            # Extract numerical fields
            field_stats = profile.get('fields', {})
            numerical_fields = [
                name for name, stats in field_stats.items()
                if stats.get('type') in ['int', 'float', 'numeric']
            ]
            
            if len(numerical_fields) < 2:
                return anomalies
            
            # Build correlation matrix
            field_data = {}
            for field in numerical_fields:
                values = []
                for record in data:
                    try:
                        value = float(record.get(field, 0))
                        values.append(value)
                    except (ValueError, TypeError):
                        values.append(0.0)
                field_data[field] = values
            
            # Calculate correlations and detect anomalies
            correlation_anomalies = []
            for i, field1 in enumerate(numerical_fields):
                for j, field2 in enumerate(numerical_fields[i+1:], i+1):
                    try:
                        correlation = np.corrcoef(field_data[field1], field_data[field2])[0, 1]
                        
                        # Flag unusually high correlations (potential data duplication)
                        if abs(correlation) > 0.95 and field1 != field2:
                            correlation_anomalies.append({
                                'field1': field1,
                                'field2': field2,
                                'correlation': correlation,
                                'type': 'high_correlation'
                            })
                    
                    except (ValueError, np.linalg.LinAlgError):
                        continue
            
            if correlation_anomalies:
                anomalies.append(Anomaly(
                    anomaly_id=f"correlation_{int(datetime.now().timestamp())}",
                    anomaly_type=AnomalyType.CORRELATION_BREAK,
                    severity=AnomalySeverity.MEDIUM,
                    confidence_score=0.8,
                    description=f"Unusual correlations detected between {len(correlation_anomalies)} field pairs",
                    affected_field=None,
                    details={
                        'method': 'correlation_analysis',
                        'anomalous_correlations': correlation_anomalies[:10]
                    },
                    remediation_suggestion="Review highly correlated fields for potential data duplication or derivation"
                ))
        
        except Exception as e:
            self.logger.error(f"Correlation anomaly detection failed: {e}")
        
        return anomalies

    def _calculate_severity(self, anomaly_count: int, total_count: int) -> AnomalySeverity:
        """Calculate anomaly severity based on proportion"""
        proportion = anomaly_count / total_count if total_count > 0 else 0
        
        if proportion >= 0.20:
            return AnomalySeverity.CRITICAL
        elif proportion >= 0.10:
            return AnomalySeverity.HIGH
        elif proportion >= 0.05:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW

    def _filter_and_rank_anomalies(self, anomalies: list[Anomaly]) -> list[Anomaly]:
        """Filter and rank anomalies by confidence and severity"""
        
        # Filter by confidence threshold
        filtered_anomalies = [
            a for a in anomalies 
            if a.confidence_score >= self.config.confidence_threshold
        ]
        
        # Remove duplicates based on affected field and type
        seen = set()
        unique_anomalies = []
        
        for anomaly in filtered_anomalies:
            key = (anomaly.affected_field, anomaly.anomaly_type.value)
            if key not in seen:
                seen.add(key)
                unique_anomalies.append(anomaly)
        
        # Sort by severity (critical first) then by confidence
        severity_order = {
            AnomalySeverity.CRITICAL: 0,
            AnomalySeverity.HIGH: 1,
            AnomalySeverity.MEDIUM: 2,
            AnomalySeverity.LOW: 3
        }
        
        sorted_anomalies = sorted(
            unique_anomalies,
            key=lambda a: (severity_order.get(a.severity, 3), -a.confidence_score)
        )
        
        return sorted_anomalies

    def update_historical_profile(self, dataset_id: str, profile: dict[str, Any]):
        """Update historical profile for distribution comparison"""
        self.historical_profiles[dataset_id] = {
            'profile': profile,
            'timestamp': datetime.now()
        }

    def get_anomaly_summary(self, anomalies: list[Anomaly]) -> dict[str, Any]:
        """Generate summary of detected anomalies"""
        if not anomalies:
            return {"message": "No anomalies detected"}
        
        summary = {
            "total_anomalies": len(anomalies),
            "by_severity": {},
            "by_type": {},
            "by_field": {},
            "high_confidence_anomalies": len([a for a in anomalies if a.confidence_score > 0.8]),
            "auto_fixable_anomalies": len([a for a in anomalies if getattr(a, 'auto_fixable', False)])
        }
        
        # Count by severity
        for severity in AnomalySeverity:
            summary["by_severity"][severity.value] = len([
                a for a in anomalies if a.severity == severity
            ])
        
        # Count by type
        for anomaly_type in AnomalyType:
            summary["by_type"][anomaly_type.value] = len([
                a for a in anomalies if a.anomaly_type == anomaly_type
            ])
        
        # Count by field
        field_counts = defaultdict(int)
        for anomaly in anomalies:
            if anomaly.affected_field:
                field_counts[anomaly.affected_field] += 1
        
        summary["by_field"] = dict(sorted(
            field_counts.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:10])
        
        return summary