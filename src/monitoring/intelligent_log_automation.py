"""
Intelligent Log Analysis and Automation System

This module provides automated log analysis capabilities including:
- Intelligent log parsing and categorization
- Automated error detection and classification
- Performance baseline establishment and monitoring
- Anomaly detection with machine learning
- Predictive issue identification
- Automated remediation and alerting
- Smart log correlation and root cause analysis
- Adaptive learning from historical patterns
"""

import json
import time
import asyncio
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Tuple, Set
from enum import Enum
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import threading
import queue
from pathlib import Path
import hashlib
import re

# Machine learning imports
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN, KMeans
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import Pipeline
import joblib

# NLP imports
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

# Statistical analysis
from scipy import stats
import statsmodels.api as sm
from statsmodels.tsa.arima.model import ARIMA

# Internal imports
from ..core.logging import get_logger
from .enterprise_datadog_log_collection import LogEntry, LogLevel, LogSource
from .enterprise_log_analysis import LogParser, PatternDetector, AnomalyDetector
from .enterprise_security_audit_logging import SecurityEvent, AuditEvent

logger = get_logger(__name__)


class AutomationAction(Enum):
    """Types of automation actions"""
    ALERT = "alert"
    AUTO_SCALE = "auto_scale"
    RESTART_SERVICE = "restart_service"
    ROLLBACK_DEPLOYMENT = "rollback_deployment"
    CIRCUIT_BREAKER = "circuit_breaker"
    CACHE_CLEAR = "cache_clear"
    THROTTLE_REQUESTS = "throttle_requests"
    ISOLATE_SERVICE = "isolate_service"
    CREATE_TICKET = "create_ticket"
    NOTIFY_ONCALL = "notify_oncall"


class AnalysisType(Enum):
    """Types of analysis"""
    PATTERN_RECOGNITION = "pattern_recognition"
    ANOMALY_DETECTION = "anomaly_detection"
    PERFORMANCE_ANALYSIS = "performance_analysis"
    ERROR_CLASSIFICATION = "error_classification"
    CORRELATION_ANALYSIS = "correlation_analysis"
    PREDICTIVE_ANALYSIS = "predictive_analysis"
    ROOT_CAUSE_ANALYSIS = "root_cause_analysis"
    SENTIMENT_ANALYSIS = "sentiment_analysis"


class LearningMode(Enum):
    """Machine learning modes"""
    SUPERVISED = "supervised"
    UNSUPERVISED = "unsupervised"
    SEMI_SUPERVISED = "semi_supervised"
    REINFORCEMENT = "reinforcement"
    ONLINE_LEARNING = "online_learning"


@dataclass
class IntelligentRule:
    """Intelligent automation rule"""
    rule_id: str
    name: str
    description: str
    analysis_type: AnalysisType
    condition: str  # Python expression or ML model
    actions: List[AutomationAction]
    confidence_threshold: float = 0.7
    learning_enabled: bool = True
    feedback_weight: float = 1.0
    is_active: bool = True
    created_at: Optional[datetime] = None
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0
    success_rate: float = 1.0
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class AnalysisResult:
    """Result of intelligent analysis"""
    analysis_id: str
    timestamp: datetime
    analysis_type: AnalysisType
    confidence: float
    findings: Dict[str, Any]
    recommendations: List[str]
    triggered_rules: List[str]
    automated_actions: List[str]
    metadata: Dict[str, Any]
    
    
@dataclass
class PredictionResult:
    """Result of predictive analysis"""
    prediction_id: str
    timestamp: datetime
    prediction_type: str
    predicted_value: Any
    confidence: float
    time_horizon_minutes: int
    features_used: List[str]
    model_version: str
    actual_value: Optional[Any] = None
    accuracy: Optional[float] = None


class IntelligentLogParser:
    """AI-powered log parsing and understanding"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.IntelligentLogParser")
        
        # NLP components
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.cluster_model = None
        
        # Pattern learning
        self.known_patterns = {}
        self.pattern_templates = {}
        self.error_signatures = {}
        
        # Parsing models
        self.classification_model = None
        self.severity_model = None
        self.is_trained = False
        
        # Initialize NLTK data
        try:
            nltk.download('vader_lexicon', quiet=True)
        except:
            pass
    
    def train_parsing_models(self, training_logs: List[LogEntry]):
        """Train ML models for intelligent parsing"""
        try:
            self.logger.info(f"Training parsing models on {len(training_logs)} logs")
            
            # Prepare training data
            features, labels, severities = self._prepare_training_data(training_logs)
            
            if len(features) < 10:
                self.logger.warning("Insufficient training data for ML models")
                return False
            
            # Train vectorizer
            self.vectorizer.fit([log.message for log in training_logs])
            
            # Transform features
            X = self.vectorizer.transform([log.message for log in training_logs])
            
            # Train classification model (log category)
            if labels:
                self.classification_model = RandomForestClassifier(n_estimators=100, random_state=42)
                self.classification_model.fit(X, labels)
                
                # Evaluate model
                X_train, X_test, y_train, y_test = train_test_split(X, labels, test_size=0.2, random_state=42)
                self.classification_model.fit(X_train, y_train)
                y_pred = self.classification_model.predict(X_test)
                accuracy = accuracy_score(y_test, y_pred)
                
                self.logger.info(f"Classification model accuracy: {accuracy:.3f}")
            
            # Train severity prediction model
            if severities:
                self.severity_model = RandomForestClassifier(n_estimators=50, random_state=42)
                severity_features = self._extract_severity_features(training_logs)
                
                if len(severity_features) > 0:
                    self.severity_model.fit(severity_features, severities)
                    self.logger.info("Severity prediction model trained")
            
            # Learn common patterns
            self._learn_patterns(training_logs)
            
            # Train clustering for anomaly detection
            if X.shape[0] >= 5:
                self.cluster_model = DBSCAN(eps=0.3, min_samples=2, metric='cosine')
                self.cluster_model.fit(X)
                self.logger.info("Clustering model for anomaly detection trained")
            
            self.is_trained = True
            return True
            
        except Exception as e:
            self.logger.error(f"Error training parsing models: {e}")
            return False
    
    def parse_intelligently(self, log_entry: LogEntry) -> Dict[str, Any]:
        """Perform intelligent parsing on a log entry"""
        try:
            result = {
                "log_id": f"log_{int(time.time() * 1000)}",
                "original_log": log_entry,
                "parsed_data": {},
                "intelligence": {},
                "confidence": 0.0
            }
            
            # Basic parsing
            result["parsed_data"] = self._extract_structured_data(log_entry)
            
            # Intelligent analysis
            if self.is_trained:
                intelligence = self._apply_intelligence(log_entry)
                result["intelligence"] = intelligence
                result["confidence"] = intelligence.get("overall_confidence", 0.0)
            else:
                # Fallback to rule-based analysis
                intelligence = self._rule_based_analysis(log_entry)
                result["intelligence"] = intelligence
                result["confidence"] = 0.5
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in intelligent parsing: {e}")
            return {"error": str(e), "log_id": f"error_{int(time.time())}"}
    
    def _prepare_training_data(self, logs: List[LogEntry]) -> Tuple[List[str], List[str], List[str]]:
        """Prepare training data from logs"""
        try:
            features = []
            labels = []
            severities = []
            
            for log in logs:
                features.append(log.message)
                
                # Infer category from source or service
                if log.source == LogSource.API:
                    labels.append("api")
                elif log.source == LogSource.DATABASE:
                    labels.append("database")
                elif log.source == LogSource.ML_PIPELINE:
                    labels.append("ml_pipeline")
                elif log.source == LogSource.SECURITY:
                    labels.append("security")
                else:
                    labels.append("application")
                
                # Map log level to severity
                severity_mapping = {
                    LogLevel.DEBUG: "low",
                    LogLevel.INFO: "low",
                    LogLevel.WARNING: "medium",
                    LogLevel.ERROR: "high",
                    LogLevel.CRITICAL: "critical",
                    LogLevel.FATAL: "critical"
                }
                severities.append(severity_mapping.get(log.level, "medium"))
            
            return features, labels, severities
            
        except Exception as e:
            self.logger.error(f"Error preparing training data: {e}")
            return [], [], []
    
    def _extract_structured_data(self, log_entry: LogEntry) -> Dict[str, Any]:
        """Extract structured data from log entry"""
        try:
            structured = {
                "timestamp": log_entry.timestamp.isoformat(),
                "level": log_entry.level.value,
                "service": log_entry.service,
                "source": log_entry.source.value,
                "message_length": len(log_entry.message),
                "has_exception": log_entry.exception is not None,
                "has_trace": log_entry.trace_id is not None
            }
            
            # Extract numbers from message
            numbers = re.findall(r'\d+(?:\.\d+)?', log_entry.message)
            structured["numbers_found"] = [float(n) for n in numbers]
            
            # Extract IPs
            ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
            ips = re.findall(ip_pattern, log_entry.message)
            structured["ip_addresses"] = ips
            
            # Extract URLs
            url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
            urls = re.findall(url_pattern, log_entry.message)
            structured["urls"] = urls
            
            # Extract common error keywords
            error_keywords = ['error', 'exception', 'failed', 'timeout', 'denied', 'invalid', 'corrupt']
            found_keywords = [kw for kw in error_keywords if kw.lower() in log_entry.message.lower()]
            structured["error_keywords"] = found_keywords
            
            return structured
            
        except Exception as e:
            self.logger.error(f"Error extracting structured data: {e}")
            return {}
    
    def _apply_intelligence(self, log_entry: LogEntry) -> Dict[str, Any]:
        """Apply trained ML models for intelligence"""
        try:
            intelligence = {}
            
            # Vectorize log message
            message_vector = self.vectorizer.transform([log_entry.message])
            
            # Predict category
            if self.classification_model:
                category_pred = self.classification_model.predict(message_vector)[0]
                category_proba = max(self.classification_model.predict_proba(message_vector)[0])
                intelligence["predicted_category"] = category_pred
                intelligence["category_confidence"] = category_proba
            
            # Predict severity
            if self.severity_model:
                severity_features = self._extract_severity_features([log_entry])
                if severity_features:
                    severity_pred = self.severity_model.predict(severity_features)[0]
                    intelligence["predicted_severity"] = severity_pred
            
            # Anomaly detection
            if self.cluster_model:
                cluster_pred = self.cluster_model.fit_predict(message_vector)
                is_anomaly = cluster_pred[0] == -1  # -1 indicates outlier in DBSCAN
                intelligence["is_anomaly"] = is_anomaly
                intelligence["anomaly_confidence"] = 0.8 if is_anomaly else 0.2
            
            # Sentiment analysis
            sentiment_score = self.sentiment_analyzer.polarity_scores(log_entry.message)
            intelligence["sentiment"] = {
                "compound": sentiment_score['compound'],
                "positive": sentiment_score['pos'],
                "negative": sentiment_score['neg'],
                "neutral": sentiment_score['neu']
            }
            
            # Pattern matching
            matched_patterns = self._match_known_patterns(log_entry.message)
            intelligence["matched_patterns"] = matched_patterns
            
            # Calculate overall confidence
            confidences = [
                intelligence.get("category_confidence", 0.5),
                intelligence.get("anomaly_confidence", 0.5),
                0.7 if matched_patterns else 0.3
            ]
            intelligence["overall_confidence"] = np.mean(confidences)
            
            return intelligence
            
        except Exception as e:
            self.logger.error(f"Error applying intelligence: {e}")
            return {"error": str(e)}
    
    def _extract_severity_features(self, logs: List[LogEntry]) -> Optional[np.ndarray]:
        """Extract features for severity prediction"""
        try:
            features = []
            
            for log in logs:
                feature_vector = [
                    len(log.message),
                    1 if log.exception else 0,
                    1 if 'error' in log.message.lower() else 0,
                    1 if 'fail' in log.message.lower() else 0,
                    1 if 'timeout' in log.message.lower() else 0,
                    len(re.findall(r'\d+', log.message)),  # Number count
                    log.level.value.count('E')  # ERROR, CRITICAL have 'E'
                ]
                features.append(feature_vector)
            
            return np.array(features) if features else None
            
        except Exception as e:
            self.logger.error(f"Error extracting severity features: {e}")
            return None
    
    def _learn_patterns(self, logs: List[LogEntry]):
        """Learn common patterns from logs"""
        try:
            # Group similar messages
            message_groups = defaultdict(list)
            
            for log in logs:
                # Create a template by replacing numbers and variable parts
                template = re.sub(r'\d+', '{number}', log.message)
                template = re.sub(r'\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b', '{uuid}', template)
                template = re.sub(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', '{ip}', template)
                
                message_groups[template].append(log)
            
            # Store patterns that appear frequently
            for template, group_logs in message_groups.items():
                if len(group_logs) >= 3:  # At least 3 occurrences
                    pattern_id = hashlib.md5(template.encode()).hexdigest()[:8]
                    self.pattern_templates[pattern_id] = {
                        "template": template,
                        "count": len(group_logs),
                        "example_logs": group_logs[:3],
                        "severity_distribution": self._analyze_severity_distribution(group_logs)
                    }
            
            self.logger.info(f"Learned {len(self.pattern_templates)} common patterns")
            
        except Exception as e:
            self.logger.error(f"Error learning patterns: {e}")
    
    def _match_known_patterns(self, message: str) -> List[Dict[str, Any]]:
        """Match message against known patterns"""
        try:
            matches = []
            
            # Create template from message
            template = re.sub(r'\d+', '{number}', message)
            template = re.sub(r'\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b', '{uuid}', template)
            template = re.sub(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', '{ip}', template)
            
            # Find matching patterns
            for pattern_id, pattern_data in self.pattern_templates.items():
                similarity = self._calculate_similarity(template, pattern_data["template"])
                if similarity > 0.8:  # High similarity threshold
                    matches.append({
                        "pattern_id": pattern_id,
                        "template": pattern_data["template"],
                        "similarity": similarity,
                        "frequency": pattern_data["count"],
                        "severity_distribution": pattern_data["severity_distribution"]
                    })
            
            return sorted(matches, key=lambda x: x["similarity"], reverse=True)
            
        except Exception as e:
            self.logger.error(f"Error matching patterns: {e}")
            return []
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two strings"""
        try:
            # Simple word-based similarity
            words1 = set(text1.lower().split())
            words2 = set(text2.lower().split())
            
            if not words1 and not words2:
                return 1.0
            if not words1 or not words2:
                return 0.0
            
            intersection = words1.intersection(words2)
            union = words1.union(words2)
            
            return len(intersection) / len(union)
            
        except Exception as e:
            self.logger.error(f"Error calculating similarity: {e}")
            return 0.0
    
    def _analyze_severity_distribution(self, logs: List[LogEntry]) -> Dict[str, float]:
        """Analyze severity distribution in log group"""
        try:
            severity_counts = defaultdict(int)
            for log in logs:
                severity_counts[log.level.value] += 1
            
            total = len(logs)
            return {severity: count / total for severity, count in severity_counts.items()}
            
        except Exception as e:
            self.logger.error(f"Error analyzing severity distribution: {e}")
            return {}
    
    def _rule_based_analysis(self, log_entry: LogEntry) -> Dict[str, Any]:
        """Fallback rule-based analysis when ML models aren't trained"""
        try:
            analysis = {
                "method": "rule_based",
                "overall_confidence": 0.6
            }
            
            # Severity analysis based on keywords
            high_severity_keywords = ['critical', 'fatal', 'emergency', 'panic', 'crash']
            medium_severity_keywords = ['error', 'fail', 'exception', 'timeout', 'denied']
            
            message_lower = log_entry.message.lower()
            
            if any(kw in message_lower for kw in high_severity_keywords):
                analysis["predicted_severity"] = "critical"
                analysis["severity_confidence"] = 0.8
            elif any(kw in message_lower for kw in medium_severity_keywords):
                analysis["predicted_severity"] = "high"
                analysis["severity_confidence"] = 0.7
            else:
                analysis["predicted_severity"] = "medium"
                analysis["severity_confidence"] = 0.5
            
            # Simple anomaly detection based on message length and keywords
            avg_message_length = 100  # Assumed average
            unusual_keywords = ['heap', 'outofmemory', 'stackoverflow', 'segfault']
            
            is_anomaly = (
                len(log_entry.message) > avg_message_length * 3 or  # Very long message
                any(kw in message_lower for kw in unusual_keywords)
            )
            
            analysis["is_anomaly"] = is_anomaly
            analysis["anomaly_confidence"] = 0.7 if is_anomaly else 0.3
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error in rule-based analysis: {e}")
            return {"error": str(e)}


class PerformanceBaselineManager:
    """Manages performance baselines and trend analysis"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.PerformanceBaselineManager")
        self.baselines = {}
        self.time_series_data = defaultdict(deque)
        self.trend_models = {}
        
    def establish_baseline(self, service: str, metric: str, values: List[float], 
                          confidence_level: float = 0.95):
        """Establish performance baseline for a service metric"""
        try:
            if len(values) < 10:
                self.logger.warning(f"Insufficient data to establish baseline for {service}.{metric}")
                return False
            
            # Calculate statistical baseline
            values_array = np.array(values)
            mean = np.mean(values_array)
            std = np.std(values_array)
            
            # Calculate confidence interval
            alpha = 1 - confidence_level
            z_score = stats.norm.ppf(1 - alpha/2)
            margin_error = z_score * (std / np.sqrt(len(values)))
            
            baseline = {
                "service": service,
                "metric": metric,
                "mean": mean,
                "std": std,
                "median": np.median(values_array),
                "percentile_95": np.percentile(values_array, 95),
                "percentile_99": np.percentile(values_array, 99),
                "confidence_interval": {
                    "lower": mean - margin_error,
                    "upper": mean + margin_error,
                    "level": confidence_level
                },
                "established_at": datetime.now(),
                "sample_size": len(values),
                "outliers": self._detect_outliers(values_array)
            }
            
            baseline_key = f"{service}.{metric}"
            self.baselines[baseline_key] = baseline
            
            self.logger.info(f"Established baseline for {baseline_key}: mean={mean:.2f}, std={std:.2f}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error establishing baseline: {e}")
            return False
    
    def check_against_baseline(self, service: str, metric: str, value: float) -> Dict[str, Any]:
        """Check if a value deviates from established baseline"""
        try:
            baseline_key = f"{service}.{metric}"
            
            if baseline_key not in self.baselines:
                return {"status": "no_baseline", "message": "No baseline established"}
            
            baseline = self.baselines[baseline_key]
            
            # Calculate deviation
            mean = baseline["mean"]
            std = baseline["std"]
            z_score = (value - mean) / std if std > 0 else 0
            
            # Determine status
            if abs(z_score) <= 2:
                status = "normal"
                severity = "low"
            elif abs(z_score) <= 3:
                status = "deviation"
                severity = "medium"
            else:
                status = "anomaly"
                severity = "high"
            
            # Additional checks
            is_above_p95 = value > baseline["percentile_95"]
            is_above_p99 = value > baseline["percentile_99"]
            
            result = {
                "status": status,
                "severity": severity,
                "z_score": z_score,
                "deviation_percent": ((value - mean) / mean) * 100 if mean != 0 else 0,
                "is_above_p95": is_above_p95,
                "is_above_p99": is_above_p99,
                "baseline_mean": mean,
                "baseline_std": std,
                "current_value": value,
                "timestamp": datetime.now()
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error checking against baseline: {e}")
            return {"status": "error", "message": str(e)}
    
    def update_baseline(self, service: str, metric: str, new_values: List[float]):
        """Update existing baseline with new data"""
        try:
            baseline_key = f"{service}.{metric}"
            
            if baseline_key not in self.baselines:
                return self.establish_baseline(service, metric, new_values)
            
            # Combine old and new data (weighted update)
            baseline = self.baselines[baseline_key]
            old_mean = baseline["mean"]
            old_count = baseline["sample_size"]
            
            new_values_array = np.array(new_values)
            new_mean = np.mean(new_values_array)
            new_count = len(new_values)
            
            # Calculate updated statistics
            total_count = old_count + new_count
            updated_mean = (old_mean * old_count + new_mean * new_count) / total_count
            
            # Update baseline
            baseline["mean"] = updated_mean
            baseline["sample_size"] = total_count
            baseline["last_updated"] = datetime.now()
            
            self.logger.info(f"Updated baseline for {baseline_key}: new mean={updated_mean:.2f}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating baseline: {e}")
            return False
    
    def predict_trend(self, service: str, metric: str, forecast_minutes: int = 60) -> Optional[Dict[str, Any]]:
        """Predict future trends using time series analysis"""
        try:
            # Get time series data
            ts_key = f"{service}.{metric}"
            if ts_key not in self.time_series_data or len(self.time_series_data[ts_key]) < 10:
                return None
            
            # Prepare data for ARIMA model
            data_points = list(self.time_series_data[ts_key])
            timestamps = [point["timestamp"] for point in data_points]
            values = [point["value"] for point in data_points]
            
            # Create time series
            ts = pd.Series(values, index=pd.to_datetime(timestamps))
            ts = ts.asfreq('5T')  # 5-minute frequency
            
            # Fit ARIMA model
            try:
                model = ARIMA(ts, order=(1, 1, 1))  # Simple ARIMA(1,1,1)
                fitted_model = model.fit()
                
                # Make forecast
                forecast_steps = forecast_minutes // 5  # Convert to 5-minute intervals
                forecast = fitted_model.forecast(steps=forecast_steps)
                
                # Generate future timestamps
                last_timestamp = timestamps[-1]
                future_timestamps = []
                for i in range(1, forecast_steps + 1):
                    future_timestamp = last_timestamp + timedelta(minutes=i * 5)
                    future_timestamps.append(future_timestamp)
                
                return {
                    "service": service,
                    "metric": metric,
                    "forecast_horizon_minutes": forecast_minutes,
                    "predictions": [
                        {"timestamp": ts.isoformat(), "predicted_value": float(val)}
                        for ts, val in zip(future_timestamps, forecast)
                    ],
                    "model_info": {
                        "type": "ARIMA(1,1,1)",
                        "aic": float(fitted_model.aic),
                        "bic": float(fitted_model.bic)
                    },
                    "created_at": datetime.now()
                }
                
            except Exception as model_error:
                # Fallback to simple linear trend
                return self._simple_trend_prediction(values, timestamps, forecast_minutes)
                
        except Exception as e:
            self.logger.error(f"Error predicting trend: {e}")
            return None
    
    def add_time_series_data(self, service: str, metric: str, value: float, timestamp: datetime):
        """Add data point to time series"""
        try:
            ts_key = f"{service}.{metric}"
            
            data_point = {
                "timestamp": timestamp,
                "value": value
            }
            
            self.time_series_data[ts_key].append(data_point)
            
            # Keep only recent data (last 24 hours)
            cutoff_time = datetime.now() - timedelta(hours=24)
            while (self.time_series_data[ts_key] and 
                   self.time_series_data[ts_key][0]["timestamp"] < cutoff_time):
                self.time_series_data[ts_key].popleft()
                
        except Exception as e:
            self.logger.error(f"Error adding time series data: {e}")
    
    def _detect_outliers(self, values: np.ndarray) -> List[float]:
        """Detect outliers in baseline data"""
        try:
            Q1 = np.percentile(values, 25)
            Q3 = np.percentile(values, 75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = values[(values < lower_bound) | (values > upper_bound)]
            return outliers.tolist()
            
        except Exception as e:
            self.logger.error(f"Error detecting outliers: {e}")
            return []
    
    def _simple_trend_prediction(self, values: List[float], timestamps: List[datetime], 
                                forecast_minutes: int) -> Dict[str, Any]:
        """Simple linear trend prediction as fallback"""
        try:
            if len(values) < 2:
                return None
            
            # Simple linear regression
            x = np.arange(len(values))
            y = np.array(values)
            
            slope, intercept = np.polyfit(x, y, 1)
            
            # Predict future values
            forecast_steps = forecast_minutes // 5
            future_x = np.arange(len(values), len(values) + forecast_steps)
            future_y = slope * future_x + intercept
            
            # Generate timestamps
            last_timestamp = timestamps[-1]
            future_timestamps = []
            for i in range(1, forecast_steps + 1):
                future_timestamp = last_timestamp + timedelta(minutes=i * 5)
                future_timestamps.append(future_timestamp)
            
            return {
                "service": "unknown",
                "metric": "unknown", 
                "forecast_horizon_minutes": forecast_minutes,
                "predictions": [
                    {"timestamp": ts.isoformat(), "predicted_value": float(val)}
                    for ts, val in zip(future_timestamps, future_y)
                ],
                "model_info": {
                    "type": "Linear Trend",
                    "slope": float(slope),
                    "intercept": float(intercept)
                },
                "created_at": datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error in simple trend prediction: {e}")
            return None


class AutomatedDecisionEngine:
    """Makes automated decisions based on log analysis"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.AutomatedDecisionEngine")
        self.rules = {}
        self.rule_performance = defaultdict(lambda: {"triggers": 0, "successes": 0, "failures": 0})
        self.decision_history = deque(maxlen=10000)
        
    def add_rule(self, rule: IntelligentRule):
        """Add automation rule"""
        self.rules[rule.rule_id] = rule
        self.logger.info(f"Added automation rule: {rule.name}")
    
    def remove_rule(self, rule_id: str):
        """Remove automation rule"""
        if rule_id in self.rules:
            del self.rules[rule_id]
            self.logger.info(f"Removed automation rule: {rule_id}")
    
    def evaluate_rules(self, analysis_results: List[AnalysisResult]) -> List[Dict[str, Any]]:
        """Evaluate rules against analysis results"""
        try:
            decisions = []
            
            for analysis in analysis_results:
                for rule_id, rule in self.rules.items():
                    if not rule.is_active:
                        continue
                    
                    # Check if rule applies to this analysis type
                    if rule.analysis_type != analysis.analysis_type:
                        continue
                    
                    # Evaluate rule condition
                    should_trigger = self._evaluate_condition(rule, analysis)
                    
                    if should_trigger and analysis.confidence >= rule.confidence_threshold:
                        decision = self._create_decision(rule, analysis)
                        decisions.append(decision)
                        
                        # Update rule statistics
                        self.rule_performance[rule_id]["triggers"] += 1
                        rule.trigger_count += 1
                        rule.last_triggered = datetime.now()
            
            return decisions
            
        except Exception as e:
            self.logger.error(f"Error evaluating rules: {e}")
            return []
    
    def execute_actions(self, decisions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute automated actions"""
        try:
            execution_results = []
            
            for decision in decisions:
                for action in decision["actions"]:
                    result = self._execute_action(action, decision)
                    execution_results.append(result)
                    
                    # Update decision history
                    self.decision_history.append({
                        "decision_id": decision["decision_id"],
                        "action": action,
                        "result": result,
                        "timestamp": datetime.now()
                    })
            
            return execution_results
            
        except Exception as e:
            self.logger.error(f"Error executing actions: {e}")
            return []
    
    def provide_feedback(self, decision_id: str, was_successful: bool, feedback_data: Dict[str, Any]):
        """Provide feedback on decision effectiveness"""
        try:
            # Find the decision in history
            for decision_record in reversed(self.decision_history):
                if decision_record.get("decision_id") == decision_id:
                    rule_id = decision_record.get("rule_id")
                    
                    if rule_id and rule_id in self.rules:
                        rule = self.rules[rule_id]
                        
                        # Update rule performance
                        if was_successful:
                            self.rule_performance[rule_id]["successes"] += 1
                        else:
                            self.rule_performance[rule_id]["failures"] += 1
                        
                        # Calculate new success rate
                        perf = self.rule_performance[rule_id]
                        total_outcomes = perf["successes"] + perf["failures"]
                        rule.success_rate = perf["successes"] / total_outcomes if total_outcomes > 0 else 1.0
                        
                        # Adjust confidence threshold based on feedback
                        if rule.learning_enabled:
                            self._adjust_rule_parameters(rule, was_successful, feedback_data)
                        
                        self.logger.info(f"Feedback processed for rule {rule_id}: success_rate={rule.success_rate:.2f}")
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error providing feedback: {e}")
            return False
    
    def _evaluate_condition(self, rule: IntelligentRule, analysis: AnalysisResult) -> bool:
        """Evaluate if rule condition is met"""
        try:
            # Create evaluation context
            context = {
                "analysis": analysis,
                "findings": analysis.findings,
                "confidence": analysis.confidence,
                "timestamp": analysis.timestamp
            }
            
            # Simple condition evaluation (in production, this would be more sophisticated)
            condition = rule.condition.lower()
            
            if "confidence >" in condition:
                threshold = float(condition.split(">")[1].strip())
                return analysis.confidence > threshold
            elif "error_rate >" in condition:
                error_rate = analysis.findings.get("error_rate", 0)
                threshold = float(condition.split(">")[1].strip())
                return error_rate > threshold
            elif "anomaly" in condition:
                return analysis.findings.get("is_anomaly", False)
            elif "pattern_match" in condition:
                return len(analysis.findings.get("patterns", [])) > 0
            
            # Default to false for unknown conditions
            return False
            
        except Exception as e:
            self.logger.error(f"Error evaluating condition: {e}")
            return False
    
    def _create_decision(self, rule: IntelligentRule, analysis: AnalysisResult) -> Dict[str, Any]:
        """Create decision from rule and analysis"""
        decision_id = f"decision_{int(time.time() * 1000)}"
        
        return {
            "decision_id": decision_id,
            "rule_id": rule.rule_id,
            "rule_name": rule.name,
            "analysis_id": analysis.analysis_id,
            "actions": rule.actions,
            "confidence": analysis.confidence,
            "reasoning": f"Rule '{rule.name}' triggered based on {rule.condition}",
            "created_at": datetime.now(),
            "analysis_findings": analysis.findings
        }
    
    def _execute_action(self, action: AutomationAction, decision: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific action"""
        try:
            result = {
                "action": action.value,
                "decision_id": decision["decision_id"],
                "status": "executed",
                "timestamp": datetime.now(),
                "details": {}
            }
            
            if action == AutomationAction.ALERT:
                result["details"] = self._send_alert(decision)
            elif action == AutomationAction.AUTO_SCALE:
                result["details"] = self._trigger_autoscale(decision)
            elif action == AutomationAction.RESTART_SERVICE:
                result["details"] = self._restart_service(decision)
            elif action == AutomationAction.CIRCUIT_BREAKER:
                result["details"] = self._activate_circuit_breaker(decision)
            elif action == AutomationAction.CREATE_TICKET:
                result["details"] = self._create_incident_ticket(decision)
            else:
                result["status"] = "not_implemented"
                result["details"] = {"message": f"Action {action.value} not yet implemented"}
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error executing action {action.value}: {e}")
            return {
                "action": action.value,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now()
            }
    
    def _send_alert(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        """Send alert (simulated)"""
        return {
            "alert_sent": True,
            "channels": ["email", "slack"],
            "message": f"Automated alert: {decision['reasoning']}"
        }
    
    def _trigger_autoscale(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger autoscaling (simulated)"""
        return {
            "autoscale_triggered": True,
            "action": "scale_up",
            "target_instances": 5
        }
    
    def _restart_service(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        """Restart service (simulated)"""
        return {
            "service_restarted": True,
            "service": "inferred_from_analysis",
            "restart_time": datetime.now().isoformat()
        }
    
    def _activate_circuit_breaker(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        """Activate circuit breaker (simulated)"""
        return {
            "circuit_breaker_activated": True,
            "service": "inferred_from_analysis",
            "duration_minutes": 15
        }
    
    def _create_incident_ticket(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        """Create incident ticket (simulated)"""
        return {
            "ticket_created": True,
            "ticket_id": f"INC-{int(time.time())}",
            "priority": "high",
            "assigned_to": "oncall_engineer"
        }
    
    def _adjust_rule_parameters(self, rule: IntelligentRule, was_successful: bool, feedback_data: Dict[str, Any]):
        """Adjust rule parameters based on feedback"""
        try:
            if not was_successful:
                # Increase confidence threshold to reduce false positives
                rule.confidence_threshold = min(0.95, rule.confidence_threshold + 0.05)
            else:
                # Slightly decrease confidence threshold for more sensitivity
                rule.confidence_threshold = max(0.5, rule.confidence_threshold - 0.01)
            
            self.logger.debug(f"Adjusted confidence threshold for rule {rule.rule_id} to {rule.confidence_threshold}")
            
        except Exception as e:
            self.logger.error(f"Error adjusting rule parameters: {e}")


class IntelligentLogAutomationSystem:
    """Main intelligent log automation system"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.IntelligentLogAutomationSystem")
        
        # Initialize components
        self.parser = IntelligentLogParser()
        self.baseline_manager = PerformanceBaselineManager()
        self.decision_engine = AutomatedDecisionEngine()
        
        # System state
        self.is_running = False
        self.processing_thread = None
        self.log_queue = queue.Queue(maxsize=10000)
        
        # Statistics
        self.stats = {
            "logs_processed": 0,
            "intelligent_analyses": 0,
            "decisions_made": 0,
            "actions_executed": 0,
            "start_time": None
        }
        
        # Initialize with default rules
        self._setup_default_rules()
    
    def start(self):
        """Start the intelligent automation system"""
        try:
            self.is_running = True
            self.stats["start_time"] = datetime.now()
            
            # Start processing thread
            self.processing_thread = threading.Thread(target=self._processing_loop, daemon=True)
            self.processing_thread.start()
            
            self.logger.info("Intelligent Log Automation System started")
            
        except Exception as e:
            self.logger.error(f"Error starting intelligent automation: {e}")
    
    def stop(self):
        """Stop the intelligent automation system"""
        try:
            self.is_running = False
            
            if self.processing_thread:
                self.processing_thread.join(timeout=10)
            
            self.logger.info("Intelligent Log Automation System stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping intelligent automation: {e}")
    
    def process_logs(self, logs: List[LogEntry]) -> Dict[str, Any]:
        """Process logs through intelligent automation pipeline"""
        try:
            processing_start = datetime.now()
            
            # Parse logs intelligently
            parsed_results = []
            for log in logs:
                parsed_result = self.parser.parse_intelligently(log)
                parsed_results.append(parsed_result)
                
                # Add performance data to baseline manager
                if log.duration_ms is not None:
                    self.baseline_manager.add_time_series_data(
                        log.service, "response_time", log.duration_ms, log.timestamp
                    )
            
            # Create analysis results
            analysis_results = []
            for parsed_result in parsed_results:
                if "intelligence" in parsed_result:
                    analysis_result = AnalysisResult(
                        analysis_id=f"analysis_{int(time.time() * 1000)}",
                        timestamp=datetime.now(),
                        analysis_type=AnalysisType.PATTERN_RECOGNITION,
                        confidence=parsed_result["confidence"],
                        findings=parsed_result["intelligence"],
                        recommendations=[],
                        triggered_rules=[],
                        automated_actions=[],
                        metadata=parsed_result.get("parsed_data", {})
                    )
                    analysis_results.append(analysis_result)
            
            # Evaluate automation rules
            decisions = self.decision_engine.evaluate_rules(analysis_results)
            
            # Execute automated actions
            execution_results = []
            if decisions:
                execution_results = self.decision_engine.execute_actions(decisions)
            
            # Update statistics
            self.stats["logs_processed"] += len(logs)
            self.stats["intelligent_analyses"] += len(analysis_results)
            self.stats["decisions_made"] += len(decisions)
            self.stats["actions_executed"] += len(execution_results)
            
            processing_time = (datetime.now() - processing_start).total_seconds()
            
            return {
                "timestamp": processing_start.isoformat(),
                "processing_time_seconds": processing_time,
                "logs_processed": len(logs),
                "parsed_results": len(parsed_results),
                "analysis_results": len(analysis_results),
                "decisions_made": len(decisions),
                "actions_executed": len(execution_results),
                "decisions": decisions,
                "execution_results": execution_results,
                "statistics": self.get_statistics()
            }
            
        except Exception as e:
            self.logger.error(f"Error processing logs through automation: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def train_models(self, training_logs: List[LogEntry]) -> bool:
        """Train intelligent models"""
        try:
            self.logger.info("Training intelligent models...")
            success = self.parser.train_parsing_models(training_logs)
            
            if success:
                self.logger.info("Model training completed successfully")
            else:
                self.logger.warning("Model training failed or incomplete")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error training models: {e}")
            return False
    
    def add_automation_rule(self, rule: IntelligentRule):
        """Add automation rule"""
        self.decision_engine.add_rule(rule)
    
    def provide_feedback(self, decision_id: str, was_successful: bool, feedback_data: Dict[str, Any]):
        """Provide feedback on automation decision"""
        return self.decision_engine.provide_feedback(decision_id, was_successful, feedback_data)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get system statistics"""
        current_stats = self.stats.copy()
        if current_stats["start_time"]:
            current_stats["uptime_seconds"] = (datetime.now() - current_stats["start_time"]).total_seconds()
        current_stats["queue_size"] = self.log_queue.qsize()
        current_stats["is_running"] = self.is_running
        current_stats["models_trained"] = self.parser.is_trained
        return current_stats
    
    def _processing_loop(self):
        """Main processing loop"""
        while self.is_running:
            try:
                time.sleep(10)  # Process every 10 seconds
                
                # Process queued logs
                logs_to_process = []
                while not self.log_queue.empty() and len(logs_to_process) < 100:
                    try:
                        log = self.log_queue.get_nowait()
                        logs_to_process.append(log)
                    except queue.Empty:
                        break
                
                if logs_to_process:
                    self.process_logs(logs_to_process)
                
            except Exception as e:
                self.logger.error(f"Error in processing loop: {e}")
    
    def _setup_default_rules(self):
        """Setup default automation rules"""
        try:
            # High error rate rule
            error_rate_rule = IntelligentRule(
                rule_id="high_error_rate",
                name="High Error Rate Detection",
                description="Trigger when error rate exceeds baseline",
                analysis_type=AnalysisType.ANOMALY_DETECTION,
                condition="error_rate > 10",
                actions=[AutomationAction.ALERT, AutomationAction.CREATE_TICKET],
                confidence_threshold=0.8
            )
            self.add_automation_rule(error_rate_rule)
            
            # Performance anomaly rule
            perf_anomaly_rule = IntelligentRule(
                rule_id="performance_anomaly",
                name="Performance Anomaly Detection",
                description="Trigger when response time anomalies detected",
                analysis_type=AnalysisType.PERFORMANCE_ANALYSIS,
                condition="anomaly",
                actions=[AutomationAction.ALERT, AutomationAction.AUTO_SCALE],
                confidence_threshold=0.75
            )
            self.add_automation_rule(perf_anomaly_rule)
            
            # Security incident rule
            security_rule = IntelligentRule(
                rule_id="security_incident",
                name="Security Incident Detection",
                description="Trigger on security-related anomalies",
                analysis_type=AnalysisType.ANOMALY_DETECTION,
                condition="predicted_category == 'security'",
                actions=[AutomationAction.ALERT, AutomationAction.CREATE_TICKET, AutomationAction.CIRCUIT_BREAKER],
                confidence_threshold=0.9
            )
            self.add_automation_rule(security_rule)
            
            self.logger.info("Default automation rules configured")
            
        except Exception as e:
            self.logger.error(f"Error setting up default rules: {e}")


# Factory function
def create_intelligent_automation_system() -> IntelligentLogAutomationSystem:
    """Create intelligent log automation system"""
    return IntelligentLogAutomationSystem()


# Example usage
if __name__ == "__main__":
    # Create system
    automation_system = create_intelligent_automation_system()
    
    # Start system
    automation_system.start()
    
    # Sample logs for testing
    sample_logs = [
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            message="Database connection timeout after 30 seconds",
            source=LogSource.DATABASE,
            service="user-service",
            environment="production",
            duration_ms=30000
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.WARNING,
            message="High memory usage detected: 85% of heap space used",
            source=LogSource.APPLICATION,
            service="api-gateway", 
            environment="production",
            duration_ms=150
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="User authentication successful for user_id: 12345",
            source=LogSource.SECURITY,
            service="auth-service",
            environment="production",
            duration_ms=50
        )
    ]
    
    # Train models (in production, this would use historical data)
    training_success = automation_system.train_models(sample_logs)
    print(f"Model training success: {training_success}")
    
    # Process logs
    result = automation_system.process_logs(sample_logs)
    
    # Print results
    print("Intelligent Automation Results:")
    print(f"Logs processed: {result['logs_processed']}")
    print(f"Analysis results: {result['analysis_results']}")
    print(f"Decisions made: {result['decisions_made']}")
    print(f"Actions executed: {result['actions_executed']}")
    
    if result.get('decisions'):
        print("\nDecisions Made:")
        for decision in result['decisions']:
            print(f"  - {decision['rule_name']}: {decision['reasoning']}")
    
    if result.get('execution_results'):
        print("\nActions Executed:")
        for action in result['execution_results']:
            print(f"  - {action['action']}: {action['status']}")
    
    # Stop system
    automation_system.stop()
    
    print("Intelligent log automation system test completed")