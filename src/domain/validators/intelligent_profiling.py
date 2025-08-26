"""
Intelligent Data Profiling and Anomaly Detection System
Advanced analytics for data discovery, pattern recognition, and anomaly detection
"""
from __future__ import annotations

import json
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from core.logging import get_logger

logger = get_logger(__name__)


class DataType(str, Enum):
    """Enhanced data type classification"""
    NUMERIC_INTEGER = "numeric_integer"
    NUMERIC_FLOAT = "numeric_float"
    CATEGORICAL_LOW = "categorical_low"  # < 50% unique
    CATEGORICAL_HIGH = "categorical_high"  # >= 50% unique
    TEXT_SHORT = "text_short"  # < 100 chars avg
    TEXT_LONG = "text_long"  # >= 100 chars avg
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    JSON = "json"
    IDENTIFIER = "identifier"
    GEOLOCATION = "geolocation"
    CURRENCY = "currency"
    UNKNOWN = "unknown"


class AnomalyType(str, Enum):
    """Types of anomalies detected"""
    STATISTICAL_OUTLIER = "statistical_outlier"
    PATTERN_DEVIATION = "pattern_deviation"
    DATA_DRIFT = "data_drift"
    SEASONAL_ANOMALY = "seasonal_anomaly"
    CORRELATION_BREAK = "correlation_break"
    DUPLICATE_SPIKE = "duplicate_spike"
    NULL_SPIKE = "null_spike"
    CARDINALITY_CHANGE = "cardinality_change"
    FORMAT_INCONSISTENCY = "format_inconsistency"


class ProfileLevel(str, Enum):
    """Profiling depth levels"""
    BASIC = "basic"
    STANDARD = "standard"
    ADVANCED = "advanced"
    COMPREHENSIVE = "comprehensive"


@dataclass
class PatternInfo:
    """Information about detected patterns"""
    pattern_type: str
    pattern: str
    confidence: float
    examples: List[str] = field(default_factory=list)
    frequency: int = 0
    description: str = ""


@dataclass
class AnomalyDetection:
    """Information about detected anomalies"""
    anomaly_type: AnomalyType
    column: str
    description: str
    severity: str  # low, medium, high, critical
    confidence: float
    affected_rows: int
    sample_values: List[Any] = field(default_factory=list)
    statistical_details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DataQualityMetrics:
    """Comprehensive data quality metrics"""
    completeness_score: float = 0.0
    validity_score: float = 0.0
    uniqueness_score: float = 0.0
    consistency_score: float = 0.0
    accuracy_score: float = 0.0
    timeliness_score: float = 0.0
    overall_score: float = 0.0
    
    # Detailed metrics
    null_percentage: float = 0.0
    duplicate_percentage: float = 0.0
    outlier_percentage: float = 0.0
    format_consistency: float = 0.0
    pattern_compliance: float = 0.0


@dataclass
class ColumnProfile:
    """Comprehensive column profile"""
    column_name: str
    inferred_type: DataType
    data_type: str
    
    # Basic statistics
    total_count: int = 0
    null_count: int = 0
    null_percentage: float = 0.0
    unique_count: int = 0
    unique_percentage: float = 0.0
    duplicate_count: int = 0
    
    # Value statistics
    min_value: Any = None
    max_value: Any = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    mode_value: Any = None
    std_dev: Optional[float] = None
    variance: Optional[float] = None
    
    # Distribution information
    quantiles: Dict[str, float] = field(default_factory=dict)
    histogram: Dict[str, int] = field(default_factory=dict)
    value_counts: Dict[str, int] = field(default_factory=dict)
    
    # Pattern analysis
    detected_patterns: List[PatternInfo] = field(default_factory=list)
    format_consistency: float = 0.0
    common_formats: List[str] = field(default_factory=list)
    
    # Text-specific metrics
    avg_length: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    
    # Quality metrics
    quality_metrics: DataQualityMetrics = field(default_factory=DataQualityMetrics)
    
    # Anomalies
    detected_anomalies: List[AnomalyDetection] = field(default_factory=list)
    
    # Metadata
    profiling_timestamp: datetime = field(default_factory=datetime.utcnow)
    sample_values: List[Any] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class DatasetProfile:
    """Comprehensive dataset profile"""
    dataset_name: str
    total_rows: int = 0
    total_columns: int = 0
    
    # Dataset-level metrics
    dataset_quality_score: float = 0.0
    memory_usage_mb: float = 0.0
    
    # Column profiles
    column_profiles: Dict[str, ColumnProfile] = field(default_factory=dict)
    
    # Relationships and correlations
    correlations: Dict[Tuple[str, str], float] = field(default_factory=dict)
    dependencies: List[Tuple[str, str]] = field(default_factory=list)
    
    # Dataset-level anomalies
    dataset_anomalies: List[AnomalyDetection] = field(default_factory=list)
    
    # Schema information
    schema_fingerprint: str = ""
    schema_changes: List[Dict[str, Any]] = field(default_factory=list)
    
    # Profiling metadata
    profiling_level: ProfileLevel = ProfileLevel.STANDARD
    profiling_duration: float = 0.0
    profiling_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Recommendations
    recommendations: List[str] = field(default_factory=list)


class IntelligentDataProfiler:
    """Advanced data profiling system with anomaly detection"""
    
    def __init__(self, profiling_level: ProfileLevel = ProfileLevel.STANDARD):
        """Initialize the profiler
        
        Args:
            profiling_level: Level of profiling detail
        """
        self.logger = get_logger(__name__)
        self.profiling_level = profiling_level
        
        # Pattern libraries
        self.email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.phone_patterns = [
            re.compile(r'^\+?1?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$'),
            re.compile(r'^(\+\d{1,3}[- ]?)?\d{10}$')
        ]
        self.url_pattern = re.compile(r'^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$')
        self.id_patterns = [
            re.compile(r'^[A-Z]{2,5}\d{3,10}$'),  # Standard ID format
            re.compile(r'^\d{8,15}$'),  # Numeric ID
            re.compile(r'^[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$')  # UUID
        ]
        
        # Anomaly detection models
        self.isolation_forest = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        
        self.logger.info(f"IntelligentDataProfiler initialized with {profiling_level.value} level")
    
    def profile_dataset(self, df: pd.DataFrame, dataset_name: str = None) -> DatasetProfile:
        """Profile entire dataset
        
        Args:
            df: DataFrame to profile
            dataset_name: Name of the dataset
            
        Returns:
            Comprehensive dataset profile
        """
        start_time = datetime.utcnow()
        dataset_name = dataset_name or f"dataset_{start_time.strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"Starting dataset profiling: {dataset_name} ({df.shape[0]} rows, {df.shape[1]} columns)")
        
        try:
            # Initialize profile
            profile = DatasetProfile(
                dataset_name=dataset_name,
                total_rows=len(df),
                total_columns=len(df.columns),
                profiling_level=self.profiling_level,
                memory_usage_mb=df.memory_usage(deep=True).sum() / 1024 / 1024
            )
            
            # Profile each column
            for column in df.columns:
                self.logger.debug(f"Profiling column: {column}")
                column_profile = self._profile_column(df[column], column)
                profile.column_profiles[column] = column_profile
            
            # Dataset-level analysis
            if self.profiling_level in [ProfileLevel.ADVANCED, ProfileLevel.COMPREHENSIVE]:
                self._analyze_correlations(df, profile)
                self._detect_dataset_anomalies(df, profile)
                self._analyze_dependencies(df, profile)
            
            # Calculate overall quality score
            profile.dataset_quality_score = self._calculate_dataset_quality_score(profile)
            
            # Generate schema fingerprint
            profile.schema_fingerprint = self._generate_schema_fingerprint(df)
            
            # Generate recommendations
            profile.recommendations = self._generate_dataset_recommendations(profile)
            
            # Record profiling duration
            end_time = datetime.utcnow()
            profile.profiling_duration = (end_time - start_time).total_seconds()
            
            self.logger.info(f"Dataset profiling completed in {profile.profiling_duration:.2f}s, quality score: {profile.dataset_quality_score:.2f}")
            
            return profile
            
        except Exception as e:
            self.logger.error(f"Dataset profiling failed: {e}")
            raise
    
    def _profile_column(self, series: pd.Series, column_name: str) -> ColumnProfile:
        """Profile individual column
        
        Args:
            series: Pandas Series to profile
            column_name: Name of the column
            
        Returns:
            Column profile
        """
        profile = ColumnProfile(
            column_name=column_name,
            data_type=str(series.dtype),
            total_count=len(series)
        )
        
        # Basic statistics
        profile.null_count = series.isnull().sum()
        profile.null_percentage = (profile.null_count / profile.total_count) * 100
        profile.unique_count = series.nunique()
        profile.unique_percentage = (profile.unique_count / profile.total_count) * 100
        profile.duplicate_count = profile.total_count - profile.unique_count
        
        # Infer enhanced data type
        profile.inferred_type = self._infer_data_type(series)
        
        # Sample values (non-null)
        non_null_series = series.dropna()
        if not non_null_series.empty:
            sample_size = min(10, len(non_null_series))
            profile.sample_values = non_null_series.sample(n=sample_size).tolist()
        
        # Type-specific profiling
        if profile.inferred_type in [DataType.NUMERIC_INTEGER, DataType.NUMERIC_FLOAT]:
            self._profile_numeric_column(non_null_series, profile)
        elif profile.inferred_type in [DataType.TEXT_SHORT, DataType.TEXT_LONG]:
            self._profile_text_column(non_null_series, profile)
        elif profile.inferred_type in [DataType.CATEGORICAL_LOW, DataType.CATEGORICAL_HIGH]:
            self._profile_categorical_column(non_null_series, profile)
        elif profile.inferred_type == DataType.DATETIME:
            self._profile_datetime_column(non_null_series, profile)
        
        # Pattern analysis
        if self.profiling_level in [ProfileLevel.STANDARD, ProfileLevel.ADVANCED, ProfileLevel.COMPREHENSIVE]:
            profile.detected_patterns = self._detect_patterns(non_null_series)
            profile.format_consistency = self._calculate_format_consistency(non_null_series)
        
        # Anomaly detection
        if self.profiling_level in [ProfileLevel.ADVANCED, ProfileLevel.COMPREHENSIVE]:
            profile.detected_anomalies = self._detect_column_anomalies(series, profile)
        
        # Quality metrics
        profile.quality_metrics = self._calculate_column_quality_metrics(series, profile)
        
        # Recommendations
        profile.recommendations = self._generate_column_recommendations(profile)
        
        return profile
    
    def _infer_data_type(self, series: pd.Series) -> DataType:
        """Infer enhanced data type"""
        try:
            non_null_series = series.dropna()
            if non_null_series.empty:
                return DataType.UNKNOWN
            
            # Check for boolean
            unique_vals = set(non_null_series.astype(str).str.lower())
            if unique_vals.issubset({'true', 'false', '1', '0', 'yes', 'no', 't', 'f', 'y', 'n'}):
                return DataType.BOOLEAN
            
            # Check for datetime
            if pd.api.types.is_datetime64_any_dtype(series):
                return DataType.DATETIME
            
            # Try to convert to datetime
            if series.dtype == 'object':
                try:
                    pd.to_datetime(non_null_series.head(100), errors='raise')
                    return DataType.DATETIME
                except:
                    pass
            
            # Check for numeric
            if pd.api.types.is_numeric_dtype(series):
                if pd.api.types.is_integer_dtype(series):
                    return DataType.NUMERIC_INTEGER
                else:
                    return DataType.NUMERIC_FLOAT
            
            # Try numeric conversion
            if series.dtype == 'object':
                try:
                    numeric_series = pd.to_numeric(non_null_series.head(100), errors='raise')
                    if (numeric_series % 1 == 0).all():
                        return DataType.NUMERIC_INTEGER
                    else:
                        return DataType.NUMERIC_FLOAT
                except:
                    pass
            
            # Check for specialized text types
            if series.dtype == 'object':
                sample = non_null_series.head(100)
                
                # Email detection
                email_matches = sum(1 for val in sample if self.email_pattern.match(str(val)))
                if email_matches / len(sample) > 0.8:
                    return DataType.EMAIL
                
                # URL detection
                url_matches = sum(1 for val in sample if self.url_pattern.match(str(val)))
                if url_matches / len(sample) > 0.8:
                    return DataType.URL
                
                # Phone detection
                phone_matches = 0
                for val in sample:
                    if any(pattern.match(str(val)) for pattern in self.phone_patterns):
                        phone_matches += 1
                if phone_matches / len(sample) > 0.8:
                    return DataType.PHONE
                
                # ID detection
                id_matches = 0
                for val in sample:
                    if any(pattern.match(str(val)) for pattern in self.id_patterns):
                        id_matches += 1
                if id_matches / len(sample) > 0.8:
                    return DataType.IDENTIFIER
                
                # JSON detection
                try:
                    json_matches = 0
                    for val in sample[:10]:  # Limited sample for performance
                        try:
                            json.loads(str(val))
                            json_matches += 1
                        except:
                            pass
                    if json_matches / min(10, len(sample)) > 0.8:
                        return DataType.JSON
                except:
                    pass
                
                # Text length analysis
                avg_length = non_null_series.astype(str).str.len().mean()
                if avg_length < 100:
                    # Categorical vs short text
                    unique_ratio = non_null_series.nunique() / len(non_null_series)
                    if unique_ratio < 0.5:
                        return DataType.CATEGORICAL_LOW
                    else:
                        return DataType.TEXT_SHORT
                else:
                    return DataType.TEXT_LONG
            
            return DataType.UNKNOWN
            
        except Exception as e:
            self.logger.warning(f"Data type inference failed: {e}")
            return DataType.UNKNOWN
    
    def _profile_numeric_column(self, series: pd.Series, profile: ColumnProfile):
        """Profile numeric column"""
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            numeric_series = numeric_series.dropna()
            
            if not numeric_series.empty:
                profile.min_value = float(numeric_series.min())
                profile.max_value = float(numeric_series.max())
                profile.mean_value = float(numeric_series.mean())
                profile.median_value = float(numeric_series.median())
                profile.std_dev = float(numeric_series.std())
                profile.variance = float(numeric_series.var())
                
                # Mode (most common value)
                mode_result = numeric_series.mode()
                if not mode_result.empty:
                    profile.mode_value = float(mode_result.iloc[0])
                
                # Quantiles
                profile.quantiles = {
                    'q25': float(numeric_series.quantile(0.25)),
                    'q50': float(numeric_series.quantile(0.50)),
                    'q75': float(numeric_series.quantile(0.75)),
                    'q90': float(numeric_series.quantile(0.90)),
                    'q95': float(numeric_series.quantile(0.95)),
                    'q99': float(numeric_series.quantile(0.99))
                }
                
                # Histogram (binned distribution)
                if self.profiling_level in [ProfileLevel.ADVANCED, ProfileLevel.COMPREHENSIVE]:
                    hist, bins = np.histogram(numeric_series, bins=20)
                    profile.histogram = {
                        f"{bins[i]:.2f}-{bins[i+1]:.2f}": int(hist[i])
                        for i in range(len(hist))
                    }
        
        except Exception as e:
            self.logger.warning(f"Numeric profiling failed for {profile.column_name}: {e}")
    
    def _profile_text_column(self, series: pd.Series, profile: ColumnProfile):
        """Profile text column"""
        try:
            text_series = series.astype(str)
            
            # Length statistics
            lengths = text_series.str.len()
            profile.avg_length = float(lengths.mean())
            profile.min_length = int(lengths.min())
            profile.max_length = int(lengths.max())
            
            # Most common values
            value_counts = text_series.value_counts()
            profile.value_counts = dict(value_counts.head(10))
            
            if not value_counts.empty:
                profile.mode_value = value_counts.index[0]
            
        except Exception as e:
            self.logger.warning(f"Text profiling failed for {profile.column_name}: {e}")
    
    def _profile_categorical_column(self, series: pd.Series, profile: ColumnProfile):
        """Profile categorical column"""
        try:
            # Value counts
            value_counts = series.value_counts()
            profile.value_counts = dict(value_counts.head(20))
            
            if not value_counts.empty:
                profile.mode_value = value_counts.index[0]
            
            # For text-like categories, also get length stats
            if series.dtype == 'object':
                text_series = series.astype(str)
                lengths = text_series.str.len()
                profile.avg_length = float(lengths.mean())
                profile.min_length = int(lengths.min())
                profile.max_length = int(lengths.max())
                
        except Exception as e:
            self.logger.warning(f"Categorical profiling failed for {profile.column_name}: {e}")
    
    def _profile_datetime_column(self, series: pd.Series, profile: ColumnProfile):
        """Profile datetime column"""
        try:
            # Try to convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(series):
                dt_series = pd.to_datetime(series, errors='coerce')
            else:
                dt_series = series
            
            dt_series = dt_series.dropna()
            
            if not dt_series.empty:
                profile.min_value = dt_series.min().isoformat()
                profile.max_value = dt_series.max().isoformat()
                
                # Mode (most common date)
                mode_result = dt_series.mode()
                if not mode_result.empty:
                    profile.mode_value = mode_result.iloc[0].isoformat()
                
                # Date range analysis
                date_range = dt_series.max() - dt_series.min()
                profile.quality_metrics.timeliness_score = self._calculate_timeliness_score(dt_series)
                
        except Exception as e:
            self.logger.warning(f"Datetime profiling failed for {profile.column_name}: {e}")
    
    def _detect_patterns(self, series: pd.Series) -> List[PatternInfo]:
        """Detect patterns in data"""
        patterns = []
        
        try:
            if series.dtype == 'object':
                # Convert to string for pattern analysis
                str_series = series.astype(str)
                sample = str_series.head(1000)  # Limit sample for performance
                
                # Length patterns
                lengths = str_series.str.len()
                length_counts = lengths.value_counts()
                most_common_length = length_counts.index[0]
                
                if length_counts.iloc[0] / len(str_series) > 0.8:
                    patterns.append(PatternInfo(
                        pattern_type="fixed_length",
                        pattern=f"length_{most_common_length}",
                        confidence=length_counts.iloc[0] / len(str_series),
                        examples=str_series[lengths == most_common_length].head(3).tolist(),
                        frequency=length_counts.iloc[0],
                        description=f"Most values have fixed length of {most_common_length} characters"
                    ))
                
                # Character patterns
                if len(sample) > 0:
                    # Check for common patterns
                    all_digits = sum(1 for val in sample if val.isdigit()) / len(sample)
                    all_alpha = sum(1 for val in sample if val.isalpha()) / len(sample)
                    all_alnum = sum(1 for val in sample if val.isalnum()) / len(sample)
                    
                    if all_digits > 0.9:
                        patterns.append(PatternInfo(
                            pattern_type="character_class",
                            pattern="all_digits",
                            confidence=all_digits,
                            description="Values are primarily numeric strings"
                        ))
                    elif all_alpha > 0.9:
                        patterns.append(PatternInfo(
                            pattern_type="character_class",
                            pattern="all_alphabetic",
                            confidence=all_alpha,
                            description="Values are primarily alphabetic"
                        ))
                    elif all_alnum > 0.9:
                        patterns.append(PatternInfo(
                            pattern_type="character_class",
                            pattern="alphanumeric",
                            confidence=all_alnum,
                            description="Values are alphanumeric"
                        ))
                
                # Case patterns
                if len(sample) > 0:
                    upper_count = sum(1 for val in sample if val.isupper()) / len(sample)
                    lower_count = sum(1 for val in sample if val.islower()) / len(sample)
                    title_count = sum(1 for val in sample if val.istitle()) / len(sample)
                    
                    if upper_count > 0.8:
                        patterns.append(PatternInfo(
                            pattern_type="case_pattern",
                            pattern="uppercase",
                            confidence=upper_count,
                            description="Values are primarily uppercase"
                        ))
                    elif lower_count > 0.8:
                        patterns.append(PatternInfo(
                            pattern_type="case_pattern",
                            pattern="lowercase",
                            confidence=lower_count,
                            description="Values are primarily lowercase"
                        ))
                    elif title_count > 0.8:
                        patterns.append(PatternInfo(
                            pattern_type="case_pattern",
                            pattern="title_case",
                            confidence=title_count,
                            description="Values are in title case"
                        ))
        
        except Exception as e:
            self.logger.warning(f"Pattern detection failed: {e}")
        
        return patterns
    
    def _calculate_format_consistency(self, series: pd.Series) -> float:
        """Calculate format consistency score"""
        try:
            if series.dtype == 'object':
                str_series = series.astype(str)
                
                # Length consistency
                lengths = str_series.str.len()
                length_consistency = (lengths == lengths.mode().iloc[0]).sum() / len(lengths) if not lengths.mode().empty else 0
                
                # Case consistency  
                upper_ratio = str_series.str.isupper().sum() / len(str_series)
                lower_ratio = str_series.str.islower().sum() / len(str_series)
                case_consistency = max(upper_ratio, lower_ratio)
                
                # Character type consistency
                digit_ratio = str_series.str.isdigit().sum() / len(str_series)
                alpha_ratio = str_series.str.isalpha().sum() / len(str_series)
                alnum_ratio = str_series.str.isalnum().sum() / len(str_series)
                char_consistency = max(digit_ratio, alpha_ratio, alnum_ratio)
                
                # Overall consistency (weighted average)
                consistency = (length_consistency * 0.4 + case_consistency * 0.3 + char_consistency * 0.3)
                return float(consistency)
            
            return 1.0  # Non-string types are considered fully consistent
            
        except Exception as e:
            self.logger.warning(f"Format consistency calculation failed: {e}")
            return 0.0
    
    def _detect_column_anomalies(self, series: pd.Series, profile: ColumnProfile) -> List[AnomalyDetection]:
        """Detect anomalies in individual column"""
        anomalies = []
        
        try:
            # Null value spike detection
            if profile.null_percentage > 30:
                anomalies.append(AnomalyDetection(
                    anomaly_type=AnomalyType.NULL_SPIKE,
                    column=profile.column_name,
                    description=f"High null percentage: {profile.null_percentage:.1f}%",
                    severity="high" if profile.null_percentage > 50 else "medium",
                    confidence=min(profile.null_percentage / 100, 1.0),
                    affected_rows=profile.null_count
                ))
            
            # Duplicate spike detection
            if profile.duplicate_count > 0 and profile.unique_percentage < 50:
                anomalies.append(AnomalyDetection(
                    anomaly_type=AnomalyType.DUPLICATE_SPIKE,
                    column=profile.column_name,
                    description=f"High duplicate rate: {100 - profile.unique_percentage:.1f}%",
                    severity="medium",
                    confidence=(100 - profile.unique_percentage) / 100,
                    affected_rows=profile.duplicate_count
                ))
            
            # Statistical outlier detection for numeric columns
            if profile.inferred_type in [DataType.NUMERIC_INTEGER, DataType.NUMERIC_FLOAT]:
                outliers = self._detect_statistical_outliers(series)
                if len(outliers) > 0:
                    outlier_percentage = len(outliers) / len(series) * 100
                    anomalies.append(AnomalyDetection(
                        anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                        column=profile.column_name,
                        description=f"Statistical outliers detected: {outlier_percentage:.1f}%",
                        severity="high" if outlier_percentage > 10 else "medium",
                        confidence=min(outlier_percentage / 20, 1.0),
                        affected_rows=len(outliers),
                        sample_values=outliers[:10],
                        statistical_details={
                            "outlier_count": len(outliers),
                            "outlier_percentage": outlier_percentage,
                            "detection_method": "iqr_and_isolation_forest"
                        }
                    ))
            
            # Format inconsistency detection
            if profile.format_consistency < 0.8:
                anomalies.append(AnomalyDetection(
                    anomaly_type=AnomalyType.FORMAT_INCONSISTENCY,
                    column=profile.column_name,
                    description=f"Format inconsistency: {profile.format_consistency:.2f} consistency score",
                    severity="medium" if profile.format_consistency > 0.5 else "high",
                    confidence=1.0 - profile.format_consistency,
                    affected_rows=int(len(series) * (1 - profile.format_consistency))
                ))
        
        except Exception as e:
            self.logger.warning(f"Column anomaly detection failed for {profile.column_name}: {e}")
        
        return anomalies
    
    def _detect_statistical_outliers(self, series: pd.Series) -> List[Any]:
        """Detect statistical outliers using multiple methods"""
        outliers = []
        
        try:
            numeric_series = pd.to_numeric(series, errors='coerce').dropna()
            
            if len(numeric_series) < 10:  # Not enough data for outlier detection
                return outliers
            
            # IQR method
            Q1 = numeric_series.quantile(0.25)
            Q3 = numeric_series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            iqr_outliers = numeric_series[(numeric_series < lower_bound) | (numeric_series > upper_bound)]
            
            # Isolation Forest method (if sufficient data)
            if len(numeric_series) >= 100:
                try:
                    # Reshape for sklearn
                    data = numeric_series.values.reshape(-1, 1)
                    
                    # Fit isolation forest
                    isolation_outliers_mask = self.isolation_forest.fit_predict(data) == -1
                    isolation_outliers = numeric_series[isolation_outliers_mask]
                    
                    # Combine results (union of both methods)
                    all_outliers = pd.concat([iqr_outliers, isolation_outliers]).drop_duplicates()
                    outliers = all_outliers.tolist()
                    
                except Exception as e:
                    self.logger.debug(f"Isolation forest failed, using only IQR: {e}")
                    outliers = iqr_outliers.tolist()
            else:
                outliers = iqr_outliers.tolist()
        
        except Exception as e:
            self.logger.warning(f"Statistical outlier detection failed: {e}")
        
        return outliers
    
    def _analyze_correlations(self, df: pd.DataFrame, profile: DatasetProfile):
        """Analyze correlations between columns"""
        try:
            # Get numeric columns for correlation analysis
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            
            if len(numeric_columns) > 1:
                corr_matrix = df[numeric_columns].corr()
                
                # Extract significant correlations
                for i, col1 in enumerate(numeric_columns):
                    for j, col2 in enumerate(numeric_columns):
                        if i < j:  # Avoid duplicates
                            correlation = corr_matrix.loc[col1, col2]
                            if not pd.isna(correlation) and abs(correlation) > 0.5:
                                profile.correlations[(col1, col2)] = float(correlation)
        
        except Exception as e:
            self.logger.warning(f"Correlation analysis failed: {e}")
    
    def _detect_dataset_anomalies(self, df: pd.DataFrame, profile: DatasetProfile):
        """Detect dataset-level anomalies"""
        try:
            # Check for completely empty dataset
            if len(df) == 0:
                profile.dataset_anomalies.append(AnomalyDetection(
                    anomaly_type=AnomalyType.DATA_DRIFT,
                    column="dataset",
                    description="Dataset is completely empty",
                    severity="critical",
                    confidence=1.0,
                    affected_rows=0
                ))
            
            # Check for unusually high null rates across columns
            null_rates = df.isnull().mean()
            high_null_columns = null_rates[null_rates > 0.5].index.tolist()
            
            if len(high_null_columns) > len(df.columns) * 0.3:  # More than 30% of columns
                profile.dataset_anomalies.append(AnomalyDetection(
                    anomaly_type=AnomalyType.NULL_SPIKE,
                    column="dataset",
                    description=f"High null rates across {len(high_null_columns)} columns",
                    severity="high",
                    confidence=len(high_null_columns) / len(df.columns),
                    affected_rows=0,
                    statistical_details={"affected_columns": high_null_columns}
                ))
        
        except Exception as e:
            self.logger.warning(f"Dataset anomaly detection failed: {e}")
    
    def _analyze_dependencies(self, df: pd.DataFrame, profile: DatasetProfile):
        """Analyze functional dependencies between columns"""
        # This is a simplified implementation
        # A full implementation would use more sophisticated dependency detection
        pass
    
    def _calculate_timeliness_score(self, dt_series: pd.Series) -> float:
        """Calculate timeliness score for datetime columns"""
        try:
            now = pd.Timestamp.now()
            max_date = dt_series.max()
            
            # Score based on how recent the most recent data is
            days_old = (now - max_date).days
            
            if days_old <= 1:
                return 1.0
            elif days_old <= 7:
                return 0.9
            elif days_old <= 30:
                return 0.7
            elif days_old <= 90:
                return 0.5
            else:
                return 0.2
        
        except Exception:
            return 0.5  # Default score
    
    def _calculate_column_quality_metrics(self, series: pd.Series, profile: ColumnProfile) -> DataQualityMetrics:
        """Calculate comprehensive quality metrics for column"""
        metrics = DataQualityMetrics()
        
        try:
            # Completeness (1 - null percentage)
            metrics.completeness_score = 1.0 - (profile.null_percentage / 100)
            
            # Uniqueness (for identifier-like columns)
            if profile.inferred_type == DataType.IDENTIFIER:
                metrics.uniqueness_score = profile.unique_percentage / 100
            else:
                metrics.uniqueness_score = 1.0  # Not applicable
            
            # Validity (format consistency for text, range validity for numeric)
            if profile.inferred_type in [DataType.TEXT_SHORT, DataType.TEXT_LONG]:
                metrics.validity_score = profile.format_consistency
            elif profile.inferred_type in [DataType.NUMERIC_INTEGER, DataType.NUMERIC_FLOAT]:
                # Check for reasonable ranges (simplified)
                outlier_count = sum(1 for anomaly in profile.detected_anomalies 
                                  if anomaly.anomaly_type == AnomalyType.STATISTICAL_OUTLIER)
                metrics.validity_score = 1.0 - (outlier_count / len(series))
            else:
                metrics.validity_score = 1.0
            
            # Consistency
            metrics.consistency_score = profile.format_consistency
            
            # Accuracy (pattern compliance for specialized types)
            if profile.inferred_type == DataType.EMAIL:
                email_valid = series.dropna().apply(lambda x: bool(self.email_pattern.match(str(x)))).mean()
                metrics.accuracy_score = float(email_valid)
            else:
                metrics.accuracy_score = 1.0  # Assume accurate for non-specialized types
            
            # Timeliness (for datetime columns)
            if profile.inferred_type == DataType.DATETIME:
                metrics.timeliness_score = self._calculate_timeliness_score(pd.to_datetime(series, errors='coerce'))
            else:
                metrics.timeliness_score = 1.0
            
            # Overall score (weighted average)
            weights = {
                'completeness': 0.25,
                'validity': 0.25,
                'uniqueness': 0.15,
                'consistency': 0.15,
                'accuracy': 0.15,
                'timeliness': 0.05
            }
            
            metrics.overall_score = (
                metrics.completeness_score * weights['completeness'] +
                metrics.validity_score * weights['validity'] +
                metrics.uniqueness_score * weights['uniqueness'] +
                metrics.consistency_score * weights['consistency'] +
                metrics.accuracy_score * weights['accuracy'] +
                metrics.timeliness_score * weights['timeliness']
            )
            
            # Additional metrics
            metrics.null_percentage = profile.null_percentage
            metrics.duplicate_percentage = 100 - profile.unique_percentage
            metrics.outlier_percentage = sum(anomaly.affected_rows for anomaly in profile.detected_anomalies 
                                           if anomaly.anomaly_type == AnomalyType.STATISTICAL_OUTLIER) / len(series) * 100
            metrics.format_consistency = profile.format_consistency
            
        except Exception as e:
            self.logger.warning(f"Quality metrics calculation failed: {e}")
        
        return metrics
    
    def _calculate_dataset_quality_score(self, profile: DatasetProfile) -> float:
        """Calculate overall dataset quality score"""
        try:
            if not profile.column_profiles:
                return 0.0
            
            column_scores = [col.quality_metrics.overall_score for col in profile.column_profiles.values()]
            return sum(column_scores) / len(column_scores)
        
        except Exception as e:
            self.logger.warning(f"Dataset quality score calculation failed: {e}")
            return 0.0
    
    def _generate_schema_fingerprint(self, df: pd.DataFrame) -> str:
        """Generate schema fingerprint for change detection"""
        try:
            schema_info = {
                'columns': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'shape': df.shape
            }
            
            import hashlib
            schema_str = json.dumps(schema_info, sort_keys=True)
            return hashlib.md5(schema_str.encode()).hexdigest()
        
        except Exception as e:
            self.logger.warning(f"Schema fingerprint generation failed: {e}")
            return ""
    
    def _generate_column_recommendations(self, profile: ColumnProfile) -> List[str]:
        """Generate recommendations for column improvement"""
        recommendations = []
        
        try:
            # Completeness recommendations
            if profile.null_percentage > 30:
                recommendations.append(f"Consider null value handling strategy for {profile.column_name}")
            
            # Uniqueness recommendations
            if profile.inferred_type == DataType.IDENTIFIER and profile.unique_percentage < 100:
                recommendations.append(f"Review duplicate identifiers in {profile.column_name}")
            
            # Format consistency recommendations
            if profile.format_consistency < 0.8:
                recommendations.append(f"Improve format consistency in {profile.column_name}")
            
            # Type-specific recommendations
            if profile.inferred_type in [DataType.TEXT_SHORT, DataType.TEXT_LONG]:
                if profile.avg_length and profile.avg_length > 1000:
                    recommendations.append(f"Consider text compression or truncation for {profile.column_name}")
            
            # Anomaly-based recommendations
            for anomaly in profile.detected_anomalies:
                if anomaly.severity in ["high", "critical"]:
                    recommendations.append(f"Address {anomaly.anomaly_type.value} in {profile.column_name}")
        
        except Exception as e:
            self.logger.warning(f"Column recommendations generation failed: {e}")
        
        return recommendations[:5]  # Limit to top 5
    
    def _generate_dataset_recommendations(self, profile: DatasetProfile) -> List[str]:
        """Generate dataset-level recommendations"""
        recommendations = []
        
        try:
            # Overall quality recommendations
            if profile.dataset_quality_score < 0.7:
                recommendations.append("Dataset quality is below acceptable threshold - comprehensive review needed")
            
            # Column-specific issues
            problematic_columns = [name for name, col in profile.column_profiles.items() 
                                 if col.quality_metrics.overall_score < 0.7]
            if problematic_columns:
                recommendations.append(f"Focus on improving quality of columns: {', '.join(problematic_columns[:5])}")
            
            # Correlation insights
            high_correlations = [(cols, corr) for cols, corr in profile.correlations.items() 
                               if abs(corr) > 0.9]
            if high_correlations:
                recommendations.append("Review highly correlated columns for potential redundancy")
            
            # Memory optimization
            if profile.memory_usage_mb > 1000:  # > 1GB
                recommendations.append("Consider data type optimization to reduce memory usage")
            
            # Anomaly insights
            critical_anomalies = [anomaly for anomaly in profile.dataset_anomalies 
                                if anomaly.severity == "critical"]
            if critical_anomalies:
                recommendations.append("Address critical dataset-level anomalies immediately")
        
        except Exception as e:
            self.logger.warning(f"Dataset recommendations generation failed: {e}")
        
        return recommendations[:10]  # Limit to top 10
    
    def export_profile_report(self, profile: DatasetProfile, output_path: str):
        """Export profile report to JSON file"""
        try:
            # Convert profile to serializable format
            profile_dict = asdict(profile)
            
            # Convert datetime objects to ISO format
            def convert_datetime(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return obj
            
            with open(output_path, 'w') as f:
                json.dump(profile_dict, f, indent=2, default=convert_datetime)
            
            self.logger.info(f"Profile report exported to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to export profile report: {e}")


# Example usage and testing
if __name__ == "__main__":
    print("Testing Intelligent Data Profiling...")
    
    try:
        # Create sample DataFrame with various data quality issues
        np.random.seed(42)
        
        sample_data = pd.DataFrame({
            'customer_id': ['CUST001', 'CUST002', 'CUST003', None, 'CUST005', 'CUST001'],  # Null and duplicate
            'email': ['user@example.com', 'invalid-email', 'test@test.com', 'user@example.com', None, 'another@test.org'],
            'age': [25, 30, -5, 45, 150, 35],  # Outliers
            'salary': [50000, 60000, 75000, 80000, 1000000, 55000],  # Outlier
            'city': ['New York', 'london', 'PARIS', 'new york', 'London', 'Tokyo'],  # Format inconsistency
            'purchase_date': pd.to_datetime(['2024-01-15', '2024-01-16', '2023-12-01', '2024-01-17', None, '2024-01-18']),
            'description': ['Short text', 'This is a much longer description with many more characters to test text length analysis', 'Medium length text here', 'Another short one', 'Long text with lots of details about the product and its features and benefits', 'Brief'],
            'phone': ['+1-555-123-4567', '555-987-6543', 'invalid-phone', '+1-555-111-2222', None, '555.444.3333']
        })
        
        print(f"Sample data shape: {sample_data.shape}")
        print(f"Sample data types:\n{sample_data.dtypes}")
        
        # Initialize profiler
        profiler = IntelligentDataProfiler(profiling_level=ProfileLevel.COMPREHENSIVE)
        
        # Profile the dataset
        print("\nProfiling dataset...")
        dataset_profile = profiler.profile_dataset(sample_data, "sample_retail_data")
        
        print(f"\nDataset Profile Results:")
        print(f"- Dataset Quality Score: {dataset_profile.dataset_quality_score:.3f}")
        print(f"- Total Columns: {dataset_profile.total_columns}")
        print(f"- Memory Usage: {dataset_profile.memory_usage_mb:.2f} MB")
        print(f"- Profiling Duration: {dataset_profile.profiling_duration:.2f} seconds")
        
        print(f"\nColumn Profiles:")
        for col_name, col_profile in dataset_profile.column_profiles.items():
            print(f"\n{col_name}:")
            print(f"  - Inferred Type: {col_profile.inferred_type.value}")
            print(f"  - Quality Score: {col_profile.quality_metrics.overall_score:.3f}")
            print(f"  - Null Percentage: {col_profile.null_percentage:.1f}%")
            print(f"  - Unique Percentage: {col_profile.unique_percentage:.1f}%")
            print(f"  - Format Consistency: {col_profile.format_consistency:.3f}")
            
            if col_profile.detected_patterns:
                print(f"  - Patterns: {[p.pattern_type for p in col_profile.detected_patterns]}")
            
            if col_profile.detected_anomalies:
                print(f"  - Anomalies: {len(col_profile.detected_anomalies)} detected")
                for anomaly in col_profile.detected_anomalies:
                    print(f"    * {anomaly.anomaly_type.value}: {anomaly.description}")
        
        print(f"\nCorrelations Found: {len(dataset_profile.correlations)}")
        for (col1, col2), corr in dataset_profile.correlations.items():
            print(f"  - {col1} <-> {col2}: {corr:.3f}")
        
        print(f"\nDataset Recommendations:")
        for i, rec in enumerate(dataset_profile.recommendations, 1):
            print(f"  {i}. {rec}")
        
        # Export profile report
        output_file = "intelligent_profile_report.json"
        profiler.export_profile_report(dataset_profile, output_file)
        print(f"\nProfile report exported to: {output_file}")
        
        print("\n✅ Intelligent data profiling testing completed successfully!")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()