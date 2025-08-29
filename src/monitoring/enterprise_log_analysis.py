"""
Enterprise Log Analysis and Processing System

This module provides comprehensive log analysis capabilities including:
- Real-time log parsing and normalization
- Advanced pattern recognition and matching
- Anomaly detection using statistical and ML methods
- Log correlation across services and requests
- Performance trend analysis from logs
- Error pattern identification and classification
- Root cause analysis and recommendations
"""

import re
import json
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from enum import Enum
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import hashlib
import threading
import queue
from pathlib import Path
import pickle
import gzip

# Statistical and ML imports
from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from scipy import stats
import networkx as nx

# Internal imports
from ..core.logging import get_logger
from .enterprise_datadog_log_collection import LogEntry, LogLevel, LogSource

logger = get_logger(__name__)


class AnomalyType(Enum):
    """Types of anomalies that can be detected"""
    VOLUME_SPIKE = "volume_spike"
    ERROR_RATE_SPIKE = "error_rate_spike"
    RESPONSE_TIME_ANOMALY = "response_time_anomaly"
    NEW_ERROR_TYPE = "new_error_type"
    UNUSUAL_PATTERN = "unusual_pattern"
    SERVICE_DEGRADATION = "service_degradation"
    SECURITY_THREAT = "security_threat"
    CORRELATION_BREAK = "correlation_break"


class PatternSeverity(Enum):
    """Pattern severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class LogPattern:
    """Definition of a log pattern to detect"""
    name: str
    pattern: str  # Regex pattern
    description: str
    severity: PatternSeverity
    category: str
    action: Optional[str] = None
    threshold: int = 1
    time_window_minutes: int = 5
    tags: Dict[str, str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = {}


@dataclass
class AnomalyResult:
    """Result of anomaly detection"""
    timestamp: datetime
    anomaly_type: AnomalyType
    severity: PatternSeverity
    description: str
    affected_services: List[str]
    confidence_score: float
    evidence: Dict[str, Any]
    recommendations: List[str]
    related_logs: List[str]


@dataclass
class PatternMatch:
    """Result of pattern matching"""
    timestamp: datetime
    pattern_name: str
    matches_count: int
    sample_logs: List[LogEntry]
    severity: PatternSeverity
    confidence: float
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class CorrelationResult:
    """Result of log correlation analysis"""
    timestamp: datetime
    correlation_id: str
    services_involved: List[str]
    request_chain: List[Dict[str, Any]]
    total_duration_ms: float
    error_points: List[Dict[str, Any]]
    performance_issues: List[Dict[str, Any]]
    recommendations: List[str]


class LogParser:
    """Advanced log parser with normalization capabilities"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.LogParser")
        self.parsing_rules = self._initialize_parsing_rules()
        self.normalizers = self._initialize_normalizers()
    
    def _initialize_parsing_rules(self) -> Dict[str, Any]:
        """Initialize log parsing rules"""
        return {
            "error_patterns": [
                r"(?i)exception\s*:?\s*([a-zA-Z0-9_.]+Exception)",
                r"(?i)error\s*:?\s*([A-Z][a-zA-Z0-9_]+Error)",
                r"(?i)fatal\s*:?\s*(.*)",
                r"(?i)failed\s+to\s+(.*)",
                r"(?i)unable\s+to\s+(.*)",
                r"(?i)connection\s+(refused|timeout|failed)"
            ],
            "performance_patterns": [
                r"(?i)(?:response|execution|request)\s+time\s*:?\s*(\d+(?:\.\d+)?)\s*(ms|seconds?|s)",
                r"(?i)took\s+(\d+(?:\.\d+)?)\s*(ms|seconds?|s)",
                r"(?i)duration\s*:?\s*(\d+(?:\.\d+)?)\s*(ms|seconds?|s)",
                r"(?i)latency\s*:?\s*(\d+(?:\.\d+)?)\s*(ms|seconds?|s)"
            ],
            "security_patterns": [
                r"(?i)authentication\s+(failed|denied|rejected)",
                r"(?i)authorization\s+(failed|denied|rejected)",
                r"(?i)access\s+(denied|forbidden)",
                r"(?i)invalid\s+(token|credentials|user)",
                r"(?i)suspicious\s+(activity|request|behavior)",
                r"(?i)potential\s+(attack|intrusion|breach)"
            ],
            "business_patterns": [
                r"(?i)transaction\s+(failed|completed|started)",
                r"(?i)payment\s+(processed|failed|declined)",
                r"(?i)order\s+(placed|cancelled|fulfilled)",
                r"(?i)user\s+(registered|logged|signup)"
            ]
        }
    
    def _initialize_normalizers(self) -> Dict[str, Callable]:
        """Initialize field normalizers"""
        return {
            "timestamp": self._normalize_timestamp,
            "level": self._normalize_log_level,
            "ip_address": self._normalize_ip_address,
            "user_id": self._normalize_user_id,
            "duration": self._normalize_duration,
            "status_code": self._normalize_status_code
        }
    
    def parse_log(self, log_entry: LogEntry) -> Dict[str, Any]:
        """Parse and normalize a log entry"""
        try:
            parsed = {
                "original": log_entry,
                "normalized": {},
                "extracted": {},
                "classification": {}
            }
            
            # Normalize standard fields
            parsed["normalized"] = self._normalize_fields(log_entry)
            
            # Extract information using patterns
            parsed["extracted"] = self._extract_patterns(log_entry.message)
            
            # Classify the log
            parsed["classification"] = self._classify_log(log_entry)
            
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error parsing log entry: {e}")
            return {"error": str(e), "original": log_entry}
    
    def _normalize_fields(self, log_entry: LogEntry) -> Dict[str, Any]:
        """Normalize log entry fields"""
        normalized = {}
        
        # Normalize timestamp
        normalized["timestamp"] = self._normalize_timestamp(log_entry.timestamp)
        
        # Normalize level
        normalized["level"] = self._normalize_log_level(log_entry.level)
        
        # Normalize service name
        normalized["service"] = log_entry.service.lower().replace("-", "_")
        
        # Normalize message
        normalized["message"] = log_entry.message.strip()
        
        # Extract and normalize IP addresses
        if log_entry.ip_address:
            normalized["ip_address"] = self._normalize_ip_address(log_entry.ip_address)
        
        # Normalize duration
        if log_entry.duration_ms:
            normalized["duration_ms"] = self._normalize_duration(log_entry.duration_ms)
        
        # Normalize status code
        if log_entry.status_code:
            normalized["status_code"] = self._normalize_status_code(log_entry.status_code)
        
        return normalized
    
    def _normalize_timestamp(self, timestamp: datetime) -> Dict[str, Any]:
        """Normalize timestamp into multiple formats"""
        return {
            "iso": timestamp.isoformat(),
            "unix": timestamp.timestamp(),
            "hour": timestamp.hour,
            "day_of_week": timestamp.weekday(),
            "is_weekend": timestamp.weekday() >= 5,
            "is_business_hours": 9 <= timestamp.hour <= 17
        }
    
    def _normalize_log_level(self, level: LogLevel) -> Dict[str, Any]:
        """Normalize log level"""
        level_priority = {
            LogLevel.DEBUG: 0,
            LogLevel.INFO: 1,
            LogLevel.WARNING: 2,
            LogLevel.ERROR: 3,
            LogLevel.CRITICAL: 4,
            LogLevel.FATAL: 5
        }
        
        return {
            "name": level.value,
            "priority": level_priority.get(level, 1),
            "is_error": level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL]
        }
    
    def _normalize_ip_address(self, ip_address: str) -> Dict[str, Any]:
        """Normalize IP address"""
        return {
            "address": ip_address,
            "is_private": self._is_private_ip(ip_address),
            "is_local": ip_address in ["127.0.0.1", "::1", "localhost"]
        }
    
    def _is_private_ip(self, ip: str) -> bool:
        """Check if IP address is private"""
        try:
            parts = ip.split('.')
            if len(parts) != 4:
                return False
            
            first_octet = int(parts[0])
            second_octet = int(parts[1])
            
            # RFC 1918 private ranges
            if first_octet == 10:
                return True
            elif first_octet == 172 and 16 <= second_octet <= 31:
                return True
            elif first_octet == 192 and second_octet == 168:
                return True
            
            return False
        except:
            return False
    
    def _normalize_user_id(self, user_id: str) -> Dict[str, Any]:
        """Normalize user ID"""
        return {
            "id": user_id,
            "is_anonymous": user_id.lower() in ["anonymous", "guest", ""],
            "is_system": user_id.lower() in ["system", "admin", "root"]
        }
    
    def _normalize_duration(self, duration_ms: float) -> Dict[str, Any]:
        """Normalize duration"""
        return {
            "ms": duration_ms,
            "seconds": duration_ms / 1000,
            "is_slow": duration_ms > 1000,  # >1 second
            "is_very_slow": duration_ms > 5000,  # >5 seconds
            "category": self._categorize_duration(duration_ms)
        }
    
    def _categorize_duration(self, duration_ms: float) -> str:
        """Categorize duration performance"""
        if duration_ms < 100:
            return "fast"
        elif duration_ms < 500:
            return "normal"
        elif duration_ms < 1000:
            return "slow"
        elif duration_ms < 5000:
            return "very_slow"
        else:
            return "timeout_risk"
    
    def _normalize_status_code(self, status_code: int) -> Dict[str, Any]:
        """Normalize HTTP status code"""
        return {
            "code": status_code,
            "class": status_code // 100,
            "is_success": 200 <= status_code < 300,
            "is_client_error": 400 <= status_code < 500,
            "is_server_error": status_code >= 500,
            "is_error": status_code >= 400,
            "category": self._categorize_status_code(status_code)
        }
    
    def _categorize_status_code(self, status_code: int) -> str:
        """Categorize HTTP status code"""
        if 200 <= status_code < 300:
            return "success"
        elif 300 <= status_code < 400:
            return "redirect"
        elif 400 <= status_code < 500:
            return "client_error"
        elif status_code >= 500:
            return "server_error"
        else:
            return "informational"
    
    def _extract_patterns(self, message: str) -> Dict[str, List[str]]:
        """Extract patterns from log message"""
        extracted = {}
        
        for category, patterns in self.parsing_rules.items():
            matches = []
            for pattern in patterns:
                found = re.findall(pattern, message)
                matches.extend(found)
            extracted[category] = matches
        
        return extracted
    
    def _classify_log(self, log_entry: LogEntry) -> Dict[str, Any]:
        """Classify the log entry"""
        classification = {
            "primary_category": "unknown",
            "subcategories": [],
            "confidence": 0.0,
            "tags": []
        }
        
        message_lower = log_entry.message.lower()
        
        # Classify by source
        if log_entry.source == LogSource.API:
            classification["primary_category"] = "api"
            if any(word in message_lower for word in ["request", "response", "endpoint"]):
                classification["subcategories"].append("http_request")
        elif log_entry.source == LogSource.DATABASE:
            classification["primary_category"] = "database"
            if any(word in message_lower for word in ["query", "connection", "transaction"]):
                classification["subcategories"].append("database_operation")
        elif log_entry.source == LogSource.ML_PIPELINE:
            classification["primary_category"] = "ml_pipeline"
            if any(word in message_lower for word in ["training", "prediction", "model"]):
                classification["subcategories"].append("ml_operation")
        
        # Classify by content
        if any(word in message_lower for word in ["error", "exception", "failed"]):
            classification["subcategories"].append("error")
            classification["tags"].append("error")
        
        if any(word in message_lower for word in ["slow", "timeout", "latency"]):
            classification["subcategories"].append("performance")
            classification["tags"].append("performance")
        
        if any(word in message_lower for word in ["auth", "login", "security"]):
            classification["subcategories"].append("security")
            classification["tags"].append("security")
        
        # Set confidence based on matches
        classification["confidence"] = min(1.0, len(classification["subcategories"]) / 3)
        
        return classification


class PatternDetector:
    """Advanced pattern detection engine"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.PatternDetector")
        self.patterns = self._initialize_default_patterns()
        self.pattern_history = defaultdict(list)
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.pattern_cache = {}
    
    def _initialize_default_patterns(self) -> List[LogPattern]:
        """Initialize default patterns to detect"""
        return [
            LogPattern(
                name="database_connection_failure",
                pattern=r"(?i)database.*connection.*(?:failed|refused|timeout)",
                description="Database connection failures",
                severity=PatternSeverity.HIGH,
                category="database"
            ),
            LogPattern(
                name="memory_leak_indicator",
                pattern=r"(?i)out.*of.*memory|memory.*leak|heap.*space",
                description="Potential memory issues",
                severity=PatternSeverity.CRITICAL,
                category="performance"
            ),
            LogPattern(
                name="authentication_failure",
                pattern=r"(?i)auth(?:entication)?.*(?:failed|denied|invalid)",
                description="Authentication failures",
                severity=PatternSeverity.MEDIUM,
                category="security"
            ),
            LogPattern(
                name="high_response_time",
                pattern=r"(?i)(?:response|request).*time.*(?:[5-9]\d{3,}|\d{5,})",
                description="High response times detected",
                severity=PatternSeverity.MEDIUM,
                category="performance"
            ),
            LogPattern(
                name="service_unavailable",
                pattern=r"(?i)service.*(?:unavailable|down|not.*responding)",
                description="Service availability issues",
                severity=PatternSeverity.HIGH,
                category="availability"
            ),
            LogPattern(
                name="disk_space_warning",
                pattern=r"(?i)disk.*(?:full|space|low|warning)",
                description="Disk space warnings",
                severity=PatternSeverity.MEDIUM,
                category="system"
            ),
            LogPattern(
                name="ssl_certificate_error",
                pattern=r"(?i)ssl.*(?:certificate|cert).*(?:expired|invalid|error)",
                description="SSL certificate issues",
                severity=PatternSeverity.HIGH,
                category="security"
            ),
            LogPattern(
                name="rate_limit_exceeded",
                pattern=r"(?i)rate.*limit.*(?:exceeded|reached)",
                description="Rate limiting activated",
                severity=PatternSeverity.MEDIUM,
                category="performance"
            ),
            LogPattern(
                name="data_corruption",
                pattern=r"(?i)data.*(?:corrupt|corrupted|invalid|checksum)",
                description="Potential data corruption",
                severity=PatternSeverity.CRITICAL,
                category="data_integrity"
            ),
            LogPattern(
                name="suspicious_activity",
                pattern=r"(?i)suspicious.*(?:activity|request|behavior|login)",
                description="Suspicious security activity",
                severity=PatternSeverity.HIGH,
                category="security"
            )
        ]
    
    def add_pattern(self, pattern: LogPattern):
        """Add a custom pattern"""
        self.patterns.append(pattern)
        self.logger.info(f"Added pattern: {pattern.name}")
    
    def detect_patterns(self, logs: List[LogEntry], time_window_minutes: int = 5) -> List[PatternMatch]:
        """Detect patterns in log entries"""
        try:
            matches = []
            current_time = datetime.now()
            
            # Group logs by time window
            time_groups = self._group_logs_by_time(logs, time_window_minutes)
            
            for time_group in time_groups:
                for pattern in self.patterns:
                    pattern_matches = self._find_pattern_matches(pattern, time_group)
                    if pattern_matches:
                        matches.extend(pattern_matches)
            
            return matches
            
        except Exception as e:
            self.logger.error(f"Error detecting patterns: {e}")
            return []
    
    def _group_logs_by_time(self, logs: List[LogEntry], window_minutes: int) -> List[List[LogEntry]]:
        """Group logs into time windows"""
        if not logs:
            return []
        
        # Sort logs by timestamp
        sorted_logs = sorted(logs, key=lambda x: x.timestamp)
        
        groups = []
        current_group = []
        window_start = None
        
        for log_entry in sorted_logs:
            if window_start is None:
                window_start = log_entry.timestamp
                current_group = [log_entry]
            elif (log_entry.timestamp - window_start).total_seconds() <= window_minutes * 60:
                current_group.append(log_entry)
            else:
                # Start new group
                if current_group:
                    groups.append(current_group)
                window_start = log_entry.timestamp
                current_group = [log_entry]
        
        # Add final group
        if current_group:
            groups.append(current_group)
        
        return groups
    
    def _find_pattern_matches(self, pattern: LogPattern, logs: List[LogEntry]) -> List[PatternMatch]:
        """Find matches for a specific pattern"""
        try:
            matches = []
            matched_logs = []
            
            # Compile regex pattern
            regex = re.compile(pattern.pattern, re.IGNORECASE)
            
            # Find matching logs
            for log_entry in logs:
                if regex.search(log_entry.message):
                    matched_logs.append(log_entry)
            
            # Check if threshold is met
            if len(matched_logs) >= pattern.threshold:
                # Calculate confidence score
                confidence = min(1.0, len(matched_logs) / (pattern.threshold * 2))
                
                match = PatternMatch(
                    timestamp=datetime.now(),
                    pattern_name=pattern.name,
                    matches_count=len(matched_logs),
                    sample_logs=matched_logs[:5],  # Sample logs
                    severity=pattern.severity,
                    confidence=confidence,
                    metadata={
                        "pattern_category": pattern.category,
                        "pattern_description": pattern.description,
                        "time_window_minutes": pattern.time_window_minutes,
                        "total_logs_analyzed": len(logs)
                    }
                )
                
                matches.append(match)
                
                # Update pattern history
                self.pattern_history[pattern.name].append({
                    "timestamp": datetime.now(),
                    "matches_count": len(matched_logs),
                    "confidence": confidence
                })
            
            return matches
            
        except Exception as e:
            self.logger.error(f"Error finding matches for pattern {pattern.name}: {e}")
            return []
    
    def get_pattern_trends(self, pattern_name: str, hours: int = 24) -> Dict[str, Any]:
        """Get trending information for a pattern"""
        try:
            history = self.pattern_history.get(pattern_name, [])
            
            # Filter by time window
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_history = [h for h in history if h["timestamp"] > cutoff_time]
            
            if not recent_history:
                return {"pattern_name": pattern_name, "trend": "no_data"}
            
            # Calculate trend
            timestamps = [h["timestamp"] for h in recent_history]
            counts = [h["matches_count"] for h in recent_history]
            
            # Simple linear trend
            if len(counts) > 1:
                trend_slope = np.polyfit(range(len(counts)), counts, 1)[0]
                if trend_slope > 0.1:
                    trend = "increasing"
                elif trend_slope < -0.1:
                    trend = "decreasing"
                else:
                    trend = "stable"
            else:
                trend = "insufficient_data"
            
            return {
                "pattern_name": pattern_name,
                "trend": trend,
                "total_occurrences": len(recent_history),
                "average_matches": np.mean(counts) if counts else 0,
                "max_matches": max(counts) if counts else 0,
                "last_occurrence": max(timestamps) if timestamps else None
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating pattern trends: {e}")
            return {"pattern_name": pattern_name, "trend": "error"}


class AnomalyDetector:
    """Advanced anomaly detection using statistical and ML methods"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.AnomalyDetector")
        self.isolation_forest = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        self.baseline_models = {}
        self.historical_data = defaultdict(list)
        self.is_trained = False
    
    def detect_anomalies(self, logs: List[LogEntry]) -> List[AnomalyResult]:
        """Detect anomalies in log data"""
        try:
            anomalies = []
            
            # Extract features for anomaly detection
            features = self._extract_features(logs)
            
            if not features:
                return anomalies
            
            # Volume-based anomaly detection
            volume_anomalies = self._detect_volume_anomalies(logs)
            anomalies.extend(volume_anomalies)
            
            # Error rate anomaly detection
            error_rate_anomalies = self._detect_error_rate_anomalies(logs)
            anomalies.extend(error_rate_anomalies)
            
            # Response time anomaly detection
            response_time_anomalies = self._detect_response_time_anomalies(logs)
            anomalies.extend(response_time_anomalies)
            
            # Pattern-based anomaly detection
            pattern_anomalies = self._detect_pattern_anomalies(logs)
            anomalies.extend(pattern_anomalies)
            
            # Service correlation anomalies
            correlation_anomalies = self._detect_correlation_anomalies(logs)
            anomalies.extend(correlation_anomalies)
            
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting anomalies: {e}")
            return []
    
    def _extract_features(self, logs: List[LogEntry]) -> Optional[pd.DataFrame]:
        """Extract features for anomaly detection"""
        try:
            if not logs:
                return None
            
            # Convert logs to DataFrame
            log_data = []
            for log in logs:
                log_data.append({
                    "timestamp": log.timestamp,
                    "level": log.level.value,
                    "service": log.service,
                    "source": log.source.value,
                    "message_length": len(log.message),
                    "has_exception": log.exception is not None,
                    "duration_ms": log.duration_ms or 0,
                    "status_code": log.status_code or 0,
                    "hour": log.timestamp.hour,
                    "day_of_week": log.timestamp.weekday()
                })
            
            df = pd.DataFrame(log_data)
            
            # Aggregate by time windows (5-minute intervals)
            df['time_bucket'] = df['timestamp'].dt.floor('5T')
            
            features = df.groupby('time_bucket').agg({
                'level': lambda x: (x == 'ERROR').sum(),  # Error count
                'service': 'count',  # Total log count
                'message_length': ['mean', 'std'],  # Message characteristics
                'has_exception': 'sum',  # Exception count
                'duration_ms': ['mean', 'max'],  # Performance metrics
                'status_code': lambda x: (x >= 400).sum()  # HTTP error count
            }).reset_index()
            
            # Flatten column names
            features.columns = [
                'time_bucket', 'error_count', 'total_count', 'avg_msg_length',
                'std_msg_length', 'exception_count', 'avg_duration',
                'max_duration', 'http_error_count'
            ]
            
            return features
            
        except Exception as e:
            self.logger.error(f"Error extracting features: {e}")
            return None
    
    def _detect_volume_anomalies(self, logs: List[LogEntry]) -> List[AnomalyResult]:
        """Detect volume anomalies (sudden spikes or drops)"""
        try:
            anomalies = []
            
            # Group logs by service and time
            service_counts = defaultdict(list)
            time_buckets = defaultdict(int)
            
            for log in logs:
                time_bucket = log.timestamp.replace(minute=log.timestamp.minute // 5 * 5, second=0, microsecond=0)
                service_counts[log.service].append(time_bucket)
                time_buckets[time_bucket] += 1
            
            # Detect spikes in overall volume
            if len(time_buckets) > 1:
                counts = list(time_buckets.values())
                mean_count = np.mean(counts)
                std_count = np.std(counts)
                
                for time_bucket, count in time_buckets.items():
                    # Z-score threshold for anomaly
                    if std_count > 0 and abs(count - mean_count) / std_count > 3:
                        severity = PatternSeverity.HIGH if count > mean_count else PatternSeverity.MEDIUM
                        
                        anomaly = AnomalyResult(
                            timestamp=time_bucket,
                            anomaly_type=AnomalyType.VOLUME_SPIKE,
                            severity=severity,
                            description=f"Volume {'spike' if count > mean_count else 'drop'} detected: {count} logs vs average {mean_count:.1f}",
                            affected_services=list(service_counts.keys()),
                            confidence_score=min(1.0, abs(count - mean_count) / std_count / 3),
                            evidence={
                                "current_volume": count,
                                "baseline_volume": mean_count,
                                "z_score": (count - mean_count) / std_count if std_count > 0 else 0
                            },
                            recommendations=[
                                "Investigate cause of volume change",
                                "Check for service issues or traffic changes",
                                "Monitor related services for impact"
                            ],
                            related_logs=[log.message for log in logs if 
                                        log.timestamp.replace(minute=log.timestamp.minute // 5 * 5, second=0, microsecond=0) == time_bucket][:5]
                        )
                        
                        anomalies.append(anomaly)
            
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting volume anomalies: {e}")
            return []
    
    def _detect_error_rate_anomalies(self, logs: List[LogEntry]) -> List[AnomalyResult]:
        """Detect error rate anomalies"""
        try:
            anomalies = []
            
            # Calculate error rate over time windows
            time_windows = defaultdict(lambda: {"total": 0, "errors": 0})
            
            for log in logs:
                time_bucket = log.timestamp.replace(minute=log.timestamp.minute // 5 * 5, second=0, microsecond=0)
                time_windows[time_bucket]["total"] += 1
                if log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL]:
                    time_windows[time_bucket]["errors"] += 1
            
            # Calculate error rates
            error_rates = []
            for time_bucket, counts in time_windows.items():
                if counts["total"] > 0:
                    error_rate = counts["errors"] / counts["total"]
                    error_rates.append((time_bucket, error_rate, counts))
            
            if len(error_rates) > 1:
                rates = [rate for _, rate, _ in error_rates]
                mean_rate = np.mean(rates)
                std_rate = np.std(rates)
                
                for time_bucket, error_rate, counts in error_rates:
                    # Detect significant error rate increases
                    if std_rate > 0 and (error_rate - mean_rate) / std_rate > 2 and error_rate > 0.1:
                        anomaly = AnomalyResult(
                            timestamp=time_bucket,
                            anomaly_type=AnomalyType.ERROR_RATE_SPIKE,
                            severity=PatternSeverity.CRITICAL if error_rate > 0.5 else PatternSeverity.HIGH,
                            description=f"Error rate spike: {error_rate:.1%} vs baseline {mean_rate:.1%}",
                            affected_services=[log.service for log in logs if log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL]],
                            confidence_score=min(1.0, (error_rate - mean_rate) / std_rate / 2),
                            evidence={
                                "current_error_rate": error_rate,
                                "baseline_error_rate": mean_rate,
                                "error_count": counts["errors"],
                                "total_count": counts["total"]
                            },
                            recommendations=[
                                "Investigate root cause of errors",
                                "Check for service degradation",
                                "Review recent deployments or changes",
                                "Monitor downstream services"
                            ],
                            related_logs=[log.message for log in logs if 
                                        log.timestamp.replace(minute=log.timestamp.minute // 5 * 5, second=0, microsecond=0) == time_bucket
                                        and log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL]][:5]
                        )
                        
                        anomalies.append(anomaly)
            
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting error rate anomalies: {e}")
            return []
    
    def _detect_response_time_anomalies(self, logs: List[LogEntry]) -> List[AnomalyResult]:
        """Detect response time anomalies"""
        try:
            anomalies = []
            
            # Extract response times
            response_times = [(log.timestamp, log.duration_ms, log.service) 
                            for log in logs if log.duration_ms is not None and log.duration_ms > 0]
            
            if len(response_times) < 10:
                return anomalies
            
            # Group by service
            service_response_times = defaultdict(list)
            for timestamp, duration, service in response_times:
                service_response_times[service].append((timestamp, duration))
            
            # Detect anomalies for each service
            for service, times in service_response_times.items():
                if len(times) < 5:
                    continue
                
                durations = [duration for _, duration in times]
                mean_duration = np.mean(durations)
                std_duration = np.std(durations)
                
                if std_duration == 0:
                    continue
                
                # Find outliers
                for timestamp, duration in times:
                    z_score = abs(duration - mean_duration) / std_duration
                    if z_score > 3 and duration > mean_duration:  # Only flag slow responses
                        anomaly = AnomalyResult(
                            timestamp=timestamp,
                            anomaly_type=AnomalyType.RESPONSE_TIME_ANOMALY,
                            severity=PatternSeverity.HIGH if duration > 5000 else PatternSeverity.MEDIUM,
                            description=f"Slow response time detected for {service}: {duration:.0f}ms vs average {mean_duration:.0f}ms",
                            affected_services=[service],
                            confidence_score=min(1.0, z_score / 3),
                            evidence={
                                "response_time_ms": duration,
                                "baseline_ms": mean_duration,
                                "z_score": z_score,
                                "percentile": (sorted(durations).index(duration) + 1) / len(durations) * 100
                            },
                            recommendations=[
                                "Investigate performance bottlenecks",
                                "Check database query performance",
                                "Review resource utilization",
                                "Check for network issues"
                            ],
                            related_logs=[log.message for log in logs if 
                                        log.service == service and log.timestamp == timestamp]
                        )
                        
                        anomalies.append(anomaly)
            
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting response time anomalies: {e}")
            return []
    
    def _detect_pattern_anomalies(self, logs: List[LogEntry]) -> List[AnomalyResult]:
        """Detect unusual patterns in log messages"""
        try:
            anomalies = []
            
            # Extract messages for clustering
            messages = [log.message for log in logs]
            
            if len(messages) < 10:
                return anomalies
            
            # Vectorize messages
            try:
                tfidf_matrix = self.vectorizer.fit_transform(messages)
                
                # Use DBSCAN clustering to find outliers
                clustering = DBSCAN(eps=0.3, min_samples=2, metric='cosine')
                cluster_labels = clustering.fit_predict(tfidf_matrix)
                
                # Find outlier messages (cluster label -1)
                outlier_indices = [i for i, label in enumerate(cluster_labels) if label == -1]
                
                if outlier_indices:
                    # Group outliers by time proximity
                    outlier_logs = [logs[i] for i in outlier_indices]
                    
                    anomaly = AnomalyResult(
                        timestamp=datetime.now(),
                        anomaly_type=AnomalyType.UNUSUAL_PATTERN,
                        severity=PatternSeverity.MEDIUM,
                        description=f"Unusual log patterns detected: {len(outlier_indices)} unique messages",
                        affected_services=list(set(log.service for log in outlier_logs)),
                        confidence_score=min(1.0, len(outlier_indices) / len(messages)),
                        evidence={
                            "outlier_count": len(outlier_indices),
                            "total_messages": len(messages),
                            "unique_clusters": len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
                        },
                        recommendations=[
                            "Review unusual log messages for potential issues",
                            "Check for new error conditions",
                            "Investigate service behavior changes"
                        ],
                        related_logs=[log.message for log in outlier_logs[:5]]
                    )
                    
                    anomalies.append(anomaly)
                    
            except ValueError as e:
                # Not enough data for vectorization
                self.logger.debug(f"Cannot vectorize messages: {e}")
            
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting pattern anomalies: {e}")
            return []
    
    def _detect_correlation_anomalies(self, logs: List[LogEntry]) -> List[AnomalyResult]:
        """Detect correlation breaks between services"""
        try:
            anomalies = []
            
            # Build service correlation graph
            service_interactions = defaultdict(lambda: defaultdict(int))
            
            # Group logs by correlation ID or time proximity
            correlation_groups = self._group_logs_by_correlation(logs)
            
            for group in correlation_groups:
                services = list(set(log.service for log in group))
                # Create edges between all services in the same correlation group
                for i, service1 in enumerate(services):
                    for service2 in services[i+1:]:
                        service_interactions[service1][service2] += 1
                        service_interactions[service2][service1] += 1
            
            # Detect broken correlations (services that usually interact but don't)
            expected_correlations = self._get_expected_correlations()
            
            for service1, service2 in expected_correlations:
                current_interaction = service_interactions[service1][service2]
                expected_interaction = expected_correlations[(service1, service2)]
                
                if current_interaction < expected_interaction * 0.5:  # 50% drop threshold
                    anomaly = AnomalyResult(
                        timestamp=datetime.now(),
                        anomaly_type=AnomalyType.CORRELATION_BREAK,
                        severity=PatternSeverity.MEDIUM,
                        description=f"Correlation break detected between {service1} and {service2}",
                        affected_services=[service1, service2],
                        confidence_score=1.0 - (current_interaction / expected_interaction),
                        evidence={
                            "current_interactions": current_interaction,
                            "expected_interactions": expected_interaction,
                            "drop_percentage": (1 - current_interaction / expected_interaction) * 100
                        },
                        recommendations=[
                            f"Check connectivity between {service1} and {service2}",
                            "Investigate service availability",
                            "Review configuration changes"
                        ],
                        related_logs=[]
                    )
                    
                    anomalies.append(anomaly)
            
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting correlation anomalies: {e}")
            return []
    
    def _group_logs_by_correlation(self, logs: List[LogEntry]) -> List[List[LogEntry]]:
        """Group logs by correlation ID or time proximity"""
        groups = []
        
        # First, group by correlation ID if available
        correlation_groups = defaultdict(list)
        uncorrelated_logs = []
        
        for log in logs:
            if log.correlation_id:
                correlation_groups[log.correlation_id].append(log)
            else:
                uncorrelated_logs.append(log)
        
        # Add correlation ID groups
        for correlation_id, group_logs in correlation_groups.items():
            if len(group_logs) > 1:  # Only consider groups with multiple logs
                groups.append(group_logs)
        
        # Group uncorrelated logs by time proximity (within 1 minute)
        uncorrelated_logs.sort(key=lambda x: x.timestamp)
        current_group = []
        
        for log in uncorrelated_logs:
            if not current_group:
                current_group = [log]
            elif (log.timestamp - current_group[-1].timestamp).total_seconds() <= 60:
                current_group.append(log)
            else:
                if len(current_group) > 1:
                    groups.append(current_group)
                current_group = [log]
        
        if len(current_group) > 1:
            groups.append(current_group)
        
        return groups
    
    def _get_expected_correlations(self) -> Dict[Tuple[str, str], int]:
        """Get expected service correlations (simplified - in real implementation, this would use historical data)"""
        # This is a simplified example - in practice, this would analyze historical correlation patterns
        return {
            ("api-gateway", "auth-service"): 10,
            ("api-gateway", "user-service"): 8,
            ("api-gateway", "order-service"): 6,
            ("order-service", "payment-service"): 5,
            ("order-service", "inventory-service"): 5,
            ("ml-pipeline", "data-service"): 4
        }


class LogCorrelator:
    """Correlates logs across services and requests"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.LogCorrelator")
        self.correlation_graph = nx.DiGraph()
        self.request_chains = {}
    
    def correlate_logs(self, logs: List[LogEntry]) -> List[CorrelationResult]:
        """Correlate logs across services and requests"""
        try:
            correlations = []
            
            # Group logs by correlation identifiers
            correlation_groups = self._build_correlation_groups(logs)
            
            # Analyze each correlation group
            for correlation_id, group_logs in correlation_groups.items():
                if len(group_logs) > 1:
                    correlation_result = self._analyze_correlation_group(correlation_id, group_logs)
                    if correlation_result:
                        correlations.append(correlation_result)
            
            return correlations
            
        except Exception as e:
            self.logger.error(f"Error correlating logs: {e}")
            return []
    
    def _build_correlation_groups(self, logs: List[LogEntry]) -> Dict[str, List[LogEntry]]:
        """Build correlation groups from logs"""
        groups = defaultdict(list)
        
        for log in logs:
            # Use correlation_id if available
            if log.correlation_id:
                groups[log.correlation_id].append(log)
            # Use trace_id if available
            elif log.trace_id:
                groups[f"trace_{log.trace_id}"].append(log)
            # Use request_id if available
            elif log.request_id:
                groups[f"request_{log.request_id}"].append(log)
            # Use session_id if available
            elif log.session_id:
                groups[f"session_{log.session_id}"].append(log)
        
        return dict(groups)
    
    def _analyze_correlation_group(self, correlation_id: str, logs: List[LogEntry]) -> Optional[CorrelationResult]:
        """Analyze a group of correlated logs"""
        try:
            if len(logs) < 2:
                return None
            
            # Sort logs by timestamp
            sorted_logs = sorted(logs, key=lambda x: x.timestamp)
            
            # Build request chain
            request_chain = []
            services_involved = set()
            error_points = []
            performance_issues = []
            
            for i, log in enumerate(sorted_logs):
                services_involved.add(log.service)
                
                chain_entry = {
                    "step": i + 1,
                    "timestamp": log.timestamp,
                    "service": log.service,
                    "level": log.level.value,
                    "message": log.message[:200],  # Truncate for summary
                    "duration_ms": log.duration_ms,
                    "status_code": log.status_code
                }
                
                # Identify error points
                if log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL]:
                    error_points.append({
                        "step": i + 1,
                        "service": log.service,
                        "error_type": log.exception.get("type") if log.exception else "Unknown",
                        "error_message": log.message,
                        "timestamp": log.timestamp
                    })
                
                # Identify performance issues
                if log.duration_ms and log.duration_ms > 1000:  # >1 second
                    performance_issues.append({
                        "step": i + 1,
                        "service": log.service,
                        "duration_ms": log.duration_ms,
                        "timestamp": log.timestamp
                    })
                
                request_chain.append(chain_entry)
            
            # Calculate total duration
            total_duration_ms = (sorted_logs[-1].timestamp - sorted_logs[0].timestamp).total_seconds() * 1000
            
            # Generate recommendations
            recommendations = self._generate_correlation_recommendations(
                error_points, performance_issues, services_involved, total_duration_ms
            )
            
            return CorrelationResult(
                timestamp=sorted_logs[0].timestamp,
                correlation_id=correlation_id,
                services_involved=list(services_involved),
                request_chain=request_chain,
                total_duration_ms=total_duration_ms,
                error_points=error_points,
                performance_issues=performance_issues,
                recommendations=recommendations
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing correlation group {correlation_id}: {e}")
            return None
    
    def _generate_correlation_recommendations(self, error_points: List[Dict], performance_issues: List[Dict], 
                                           services: Set[str], total_duration_ms: float) -> List[str]:
        """Generate recommendations based on correlation analysis"""
        recommendations = []
        
        # Error-based recommendations
        if error_points:
            recommendations.append(f"Investigate {len(error_points)} error points in the request chain")
            
            # Identify error patterns
            error_services = [ep["service"] for ep in error_points]
            error_service_counts = defaultdict(int)
            for service in error_services:
                error_service_counts[service] += 1
            
            most_error_prone = max(error_service_counts, key=error_service_counts.get)
            recommendations.append(f"Focus on {most_error_prone} service - most errors ({error_service_counts[most_error_prone]})")
        
        # Performance-based recommendations
        if performance_issues:
            recommendations.append(f"Address {len(performance_issues)} performance bottlenecks")
            
            slowest_service = max(performance_issues, key=lambda x: x["duration_ms"])
            recommendations.append(f"Optimize {slowest_service['service']} - slowest step ({slowest_service['duration_ms']:.0f}ms)")
        
        # Overall duration recommendations
        if total_duration_ms > 5000:  # >5 seconds
            recommendations.append("Request chain is taking too long - consider async processing or optimization")
        
        # Service dependency recommendations
        if len(services) > 5:
            recommendations.append("Request involves many services - consider service consolidation or caching")
        
        return recommendations


class EnterpriseLogAnalyzer:
    """Main enterprise log analysis orchestrator"""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.EnterpriseLogAnalyzer")
        
        # Initialize components
        self.parser = LogParser()
        self.pattern_detector = PatternDetector()
        self.anomaly_detector = AnomalyDetector()
        self.correlator = LogCorrelator()
        
        # Analysis state
        self.analysis_history = deque(maxlen=1000)
        self.is_running = False
        self.analysis_thread = None
        
        # Configuration
        self.analysis_interval_seconds = 60
        self.batch_size = 500
    
    def start_analysis(self):
        """Start continuous log analysis"""
        try:
            self.is_running = True
            self.analysis_thread = threading.Thread(target=self._analysis_loop, daemon=True)
            self.analysis_thread.start()
            self.logger.info("Enterprise log analysis started")
            
        except Exception as e:
            self.logger.error(f"Error starting log analysis: {e}")
    
    def stop_analysis(self):
        """Stop continuous log analysis"""
        try:
            self.is_running = False
            if self.analysis_thread:
                self.analysis_thread.join(timeout=10)
            self.logger.info("Enterprise log analysis stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping log analysis: {e}")
    
    def analyze_logs(self, logs: List[LogEntry]) -> Dict[str, Any]:
        """Analyze a batch of logs"""
        try:
            analysis_start = time.time()
            
            # Parse and normalize logs
            parsed_logs = []
            for log in logs:
                parsed_log = self.parser.parse_log(log)
                parsed_logs.append(parsed_log)
            
            # Detect patterns
            pattern_matches = self.pattern_detector.detect_patterns(logs)
            
            # Detect anomalies
            anomalies = self.anomaly_detector.detect_anomalies(logs)
            
            # Correlate logs
            correlations = self.correlator.correlate_logs(logs)
            
            # Generate analysis summary
            analysis_result = {
                "timestamp": datetime.now(),
                "logs_analyzed": len(logs),
                "processing_time_ms": (time.time() - analysis_start) * 1000,
                "parsed_logs": len(parsed_logs),
                "pattern_matches": [asdict(match) for match in pattern_matches],
                "anomalies": [asdict(anomaly) for anomaly in anomalies],
                "correlations": [asdict(correlation) for correlation in correlations],
                "summary": self._generate_analysis_summary(logs, pattern_matches, anomalies, correlations)
            }
            
            # Store in history
            self.analysis_history.append(analysis_result)
            
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"Error analyzing logs: {e}")
            return {"error": str(e), "timestamp": datetime.now()}
    
    def _analysis_loop(self):
        """Continuous analysis loop"""
        while self.is_running:
            try:
                time.sleep(self.analysis_interval_seconds)
                
                # In a real implementation, this would fetch logs from a queue or database
                # For now, we'll just log that analysis is running
                self.logger.debug("Performing periodic log analysis")
                
            except Exception as e:
                self.logger.error(f"Error in analysis loop: {e}")
    
    def _generate_analysis_summary(self, logs: List[LogEntry], pattern_matches: List[PatternMatch], 
                                 anomalies: List[AnomalyResult], correlations: List[CorrelationResult]) -> Dict[str, Any]:
        """Generate analysis summary"""
        try:
            # Calculate basic statistics
            total_logs = len(logs)
            error_logs = sum(1 for log in logs if log.level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.FATAL])
            services = set(log.service for log in logs)
            sources = set(log.source.value for log in logs)
            
            # Calculate time range
            if logs:
                timestamps = [log.timestamp for log in logs]
                time_range = max(timestamps) - min(timestamps)
            else:
                time_range = timedelta(0)
            
            # Categorize findings by severity
            high_severity_findings = []
            medium_severity_findings = []
            low_severity_findings = []
            
            for match in pattern_matches:
                if match.severity == PatternSeverity.CRITICAL:
                    high_severity_findings.append(f"Critical pattern: {match.pattern_name}")
                elif match.severity == PatternSeverity.HIGH:
                    high_severity_findings.append(f"High severity pattern: {match.pattern_name}")
                elif match.severity == PatternSeverity.MEDIUM:
                    medium_severity_findings.append(f"Pattern: {match.pattern_name}")
                else:
                    low_severity_findings.append(f"Pattern: {match.pattern_name}")
            
            for anomaly in anomalies:
                if anomaly.severity == PatternSeverity.CRITICAL:
                    high_severity_findings.append(f"Critical anomaly: {anomaly.anomaly_type.value}")
                elif anomaly.severity == PatternSeverity.HIGH:
                    high_severity_findings.append(f"High severity anomaly: {anomaly.anomaly_type.value}")
                elif anomaly.severity == PatternSeverity.MEDIUM:
                    medium_severity_findings.append(f"Anomaly: {anomaly.anomaly_type.value}")
                else:
                    low_severity_findings.append(f"Anomaly: {anomaly.anomaly_type.value}")
            
            # Generate recommendations
            all_recommendations = set()
            for match in pattern_matches:
                if hasattr(match, 'recommendations'):
                    all_recommendations.update(match.recommendations or [])
            for anomaly in anomalies:
                all_recommendations.update(anomaly.recommendations or [])
            for correlation in correlations:
                all_recommendations.update(correlation.recommendations or [])
            
            return {
                "total_logs": total_logs,
                "error_rate": error_logs / total_logs if total_logs > 0 else 0,
                "services_count": len(services),
                "sources_count": len(sources),
                "time_range_minutes": time_range.total_seconds() / 60,
                "patterns_detected": len(pattern_matches),
                "anomalies_detected": len(anomalies),
                "correlations_found": len(correlations),
                "high_severity_findings": high_severity_findings,
                "medium_severity_findings": medium_severity_findings,
                "low_severity_findings": low_severity_findings,
                "recommendations": list(all_recommendations),
                "health_score": self._calculate_health_score(error_logs, total_logs, len(high_severity_findings))
            }
            
        except Exception as e:
            self.logger.error(f"Error generating analysis summary: {e}")
            return {"error": str(e)}
    
    def _calculate_health_score(self, error_logs: int, total_logs: int, high_severity_findings: int) -> float:
        """Calculate overall health score (0-100)"""
        try:
            base_score = 100.0
            
            # Penalize error rate
            if total_logs > 0:
                error_rate = error_logs / total_logs
                base_score -= error_rate * 30  # Up to 30 points for error rate
            
            # Penalize high severity findings
            base_score -= high_severity_findings * 10  # 10 points per critical finding
            
            # Ensure score stays within bounds
            return max(0.0, min(100.0, base_score))
            
        except Exception as e:
            self.logger.error(f"Error calculating health score: {e}")
            return 50.0  # Default to neutral score
    
    def get_analysis_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent analysis history"""
        return list(self.analysis_history)[-limit:]
    
    def get_pattern_trends(self, pattern_name: str, hours: int = 24) -> Dict[str, Any]:
        """Get trending information for a pattern"""
        return self.pattern_detector.get_pattern_trends(pattern_name, hours)


# Factory function
def create_enterprise_log_analyzer() -> EnterpriseLogAnalyzer:
    """Create an enterprise log analyzer"""
    return EnterpriseLogAnalyzer()


# Example usage
if __name__ == "__main__":
    # Create analyzer
    analyzer = create_enterprise_log_analyzer()
    
    # Sample logs for testing
    sample_logs = [
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            message="Database connection failed: Connection timeout after 30s",
            source=LogSource.DATABASE,
            service="user-service",
            environment="production",
            correlation_id="req-12345"
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Processing user authentication request",
            source=LogSource.API,
            service="auth-service",
            environment="production",
            correlation_id="req-12345",
            duration_ms=250
        ),
        LogEntry(
            timestamp=datetime.now() + timedelta(seconds=5),
            level=LogLevel.WARNING,
            message="High response time detected: 3500ms",
            source=LogSource.API,
            service="user-service",
            environment="production",
            correlation_id="req-12345",
            duration_ms=3500
        )
    ]
    
    # Analyze logs
    result = analyzer.analyze_logs(sample_logs)
    
    # Print results
    print("Analysis Results:")
    print(f"Logs analyzed: {result['logs_analyzed']}")
    print(f"Processing time: {result['processing_time_ms']:.2f}ms")
    print(f"Patterns detected: {len(result['pattern_matches'])}")
    print(f"Anomalies detected: {len(result['anomalies'])}")
    print(f"Correlations found: {len(result['correlations'])}")
    
    if result.get('summary'):
        summary = result['summary']
        print(f"\nHealth Score: {summary.get('health_score', 0):.1f}/100")
        print(f"Error Rate: {summary.get('error_rate', 0):.1%}")
        
        if summary.get('high_severity_findings'):
            print("\nHigh Severity Findings:")
            for finding in summary['high_severity_findings']:
                print(f"  - {finding}")
        
        if summary.get('recommendations'):
            print("\nRecommendations:")
            for rec in summary['recommendations'][:5]:  # Show top 5
                print(f"  - {rec}")