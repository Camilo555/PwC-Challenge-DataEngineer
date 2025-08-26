"""
Advanced Data Profiler
Comprehensive data profiling with automated schema inference and statistical analysis.
"""
from __future__ import annotations

import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

import numpy as np

from core.logging import get_logger


@dataclass
class FieldProfile:
    """Profile for a single data field"""
    name: str
    data_type: str
    null_count: int = 0
    null_percentage: float = 0.0
    unique_count: int = 0
    unique_percentage: float = 0.0
    
    # Statistical measures
    min_value: Any = None
    max_value: Any = None
    mean: float | None = None
    median: float | None = None
    std_dev: float | None = None
    
    # String-specific
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None
    
    # Categorical data
    most_common_values: dict[str, int] = field(default_factory=dict)
    value_counts: dict[str, int] = field(default_factory=dict)
    
    # Data quality indicators
    format_errors: int = 0
    type_errors: int = 0
    range_violations: int = 0
    pattern_matches: dict[str, int] = field(default_factory=dict)
    
    # Advanced statistics
    percentiles: dict[str, float] = field(default_factory=dict)
    distribution_info: dict[str, Any] = field(default_factory=dict)
    quality_score: float = 1.0
    
    # Metadata
    inferred_constraints: list[str] = field(default_factory=list)
    suggested_type: str | None = None
    data_classification: str | None = None


@dataclass 
class DatasetProfile:
    """Complete dataset profile"""
    dataset_name: str
    total_records: int
    total_fields: int
    fields: dict[str, FieldProfile] = field(default_factory=dict)
    
    # Dataset-level statistics
    completeness_score: float = 0.0
    consistency_score: float = 0.0
    uniqueness_score: float = 0.0
    overall_quality_score: float = 0.0
    
    # Relationships and constraints
    field_correlations: dict[str, dict[str, float]] = field(default_factory=dict)
    foreign_key_candidates: list[dict[str, Any]] = field(default_factory=list)
    primary_key_candidates: list[str] = field(default_factory=list)
    
    # Schema information
    inferred_schema: dict[str, Any] = field(default_factory=dict)
    suggested_indexes: list[dict[str, Any]] = field(default_factory=list)
    
    # Quality issues
    data_quality_issues: list[dict[str, Any]] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    
    # Profiling metadata
    profiling_timestamp: datetime = field(default_factory=datetime.now)
    profiling_duration_seconds: float = 0.0
    sample_size: int = 0


class DataProfiler:
    """
    Advanced Data Profiler
    
    Provides comprehensive data profiling capabilities including:
    - Automated data type detection
    - Statistical analysis
    - Data quality assessment
    - Schema inference
    - Pattern recognition
    - Constraint detection
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        
        # Common patterns for data classification
        self.patterns = {
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'phone': re.compile(r'^\+?[\d\s\-\(\)]{10,}$'),
            'url': re.compile(r'^https?://[^\s/$.?#].[^\s]*$'),
            'ip_address': re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'),
            'credit_card': re.compile(r'^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$'),
            'ssn': re.compile(r'^\d{3}-?\d{2}-?\d{4}$'),
            'uuid': re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'),
            'date_iso': re.compile(r'^\d{4}-\d{2}-\d{2}'),
            'currency': re.compile(r'^\$?[\d,]+\.?\d*$')
        }

    def profile_data(
        self,
        data: list[dict[str, Any]],
        schema: dict[str, Any] | None = None,
        sample_size: int | None = None
    ) -> dict[str, Any]:
        """
        Generate comprehensive data profile
        
        Args:
            data: List of records to profile
            schema: Optional schema definition
            sample_size: Optional sample size (None = use all data)
        
        Returns:
            Complete data profile dictionary
        """
        start_time = datetime.now()
        
        try:
            if not data:
                return {"error": "No data to profile"}
            
            # Sample data if requested
            if sample_size and len(data) > sample_size:
                data = data[:sample_size]
            
            # Initialize profile
            profile = DatasetProfile(
                dataset_name="unknown",
                total_records=len(data),
                total_fields=len(data[0].keys()) if data else 0,
                sample_size=len(data)
            )
            
            # Profile each field
            field_names = list(data[0].keys()) if data else []
            for field_name in field_names:
                field_profile = self._profile_field(data, field_name, schema)
                profile.fields[field_name] = field_profile
            
            # Calculate dataset-level metrics
            self._calculate_dataset_metrics(profile, data)
            
            # Detect relationships and constraints
            self._detect_relationships(profile, data)
            
            # Generate recommendations
            self._generate_recommendations(profile)
            
            # Record profiling duration
            profile.profiling_duration_seconds = (datetime.now() - start_time).total_seconds()
            
            # Convert to dictionary for serialization
            return self._profile_to_dict(profile)
            
        except Exception as e:
            self.logger.error(f"Data profiling failed: {e}", exc_info=True)
            return {"error": str(e)}

    def _profile_field(
        self,
        data: list[dict[str, Any]],
        field_name: str,
        schema: dict[str, Any] | None = None
    ) -> FieldProfile:
        """Profile a single field"""
        
        field_profile = FieldProfile(name=field_name, data_type="unknown")
        
        # Extract values
        values = [record.get(field_name) for record in data]
        non_null_values = [v for v in values if v is not None]
        
        # Basic statistics
        field_profile.null_count = len(values) - len(non_null_values)
        field_profile.null_percentage = field_profile.null_count / len(values) if values else 0
        field_profile.unique_count = len(set(str(v) for v in non_null_values))
        field_profile.unique_percentage = (
            field_profile.unique_count / len(non_null_values) if non_null_values else 0
        )
        
        if not non_null_values:
            return field_profile
        
        # Detect data type
        detected_type = self._detect_data_type(non_null_values)
        field_profile.data_type = detected_type
        
        # Type-specific profiling
        if detected_type in ['int', 'float', 'numeric']:
            self._profile_numeric_field(field_profile, non_null_values)
        elif detected_type == 'string':
            self._profile_string_field(field_profile, non_null_values)
        elif detected_type in ['datetime', 'date']:
            self._profile_datetime_field(field_profile, non_null_values)
        elif detected_type == 'boolean':
            self._profile_boolean_field(field_profile, non_null_values)
        
        # Value distribution analysis
        self._analyze_value_distribution(field_profile, non_null_values)
        
        # Data quality assessment
        self._assess_field_quality(field_profile, values, schema)
        
        # Pattern recognition and classification
        self._classify_field_content(field_profile, non_null_values)
        
        return field_profile

    def _detect_data_type(self, values: list[Any]) -> str:
        """Detect the most appropriate data type for a field"""
        
        if not values:
            return "unknown"
        
        type_votes = defaultdict(int)
        
        for value in values[:100]:  # Sample first 100 values
            str_value = str(value).strip()
            
            # Check for numeric types
            if self._is_integer(str_value):
                type_votes['int'] += 1
            elif self._is_float(str_value):
                type_votes['float'] += 1
            elif self._is_datetime(str_value):
                type_votes['datetime'] += 1
            elif self._is_date(str_value):
                type_votes['date'] += 1
            elif self._is_boolean(str_value):
                type_votes['boolean'] += 1
            else:
                type_votes['string'] += 1
        
        # Return the most voted type
        if type_votes:
            return max(type_votes.items(), key=lambda x: x[1])[0]
        
        return "string"

    def _is_integer(self, value: str) -> bool:
        """Check if value is an integer"""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_float(self, value: str) -> bool:
        """Check if value is a float"""
        try:
            float(value)
            return '.' in value or 'e' in value.lower()
        except (ValueError, TypeError):
            return False

    def _is_datetime(self, value: str) -> bool:
        """Check if value is a datetime"""
        datetime_patterns = [
            r'^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            r'^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2}',
            r'^\d{2}-\d{2}-\d{4}\s\d{2}:\d{2}:\d{2}'
        ]
        
        return any(re.match(pattern, value) for pattern in datetime_patterns)

    def _is_date(self, value: str) -> bool:
        """Check if value is a date"""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',
            r'^\d{2}/\d{2}/\d{4}$',
            r'^\d{2}-\d{2}-\d{4}$'
        ]
        
        return any(re.match(pattern, value) for pattern in date_patterns)

    def _is_boolean(self, value: str) -> bool:
        """Check if value is a boolean"""
        boolean_values = {
            'true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 
            't', 'f', 'on', 'off', 'enabled', 'disabled'
        }
        return value.lower() in boolean_values

    def _profile_numeric_field(self, field_profile: FieldProfile, values: list[Any]):
        """Profile numeric field with statistical measures"""
        try:
            numeric_values = []
            for value in values:
                try:
                    if isinstance(value, (int, float, Decimal)):
                        numeric_values.append(float(value))
                    else:
                        numeric_values.append(float(str(value)))
                except (ValueError, TypeError):
                    field_profile.type_errors += 1
            
            if not numeric_values:
                return
            
            # Basic statistics
            field_profile.min_value = min(numeric_values)
            field_profile.max_value = max(numeric_values)
            field_profile.mean = statistics.mean(numeric_values)
            field_profile.median = statistics.median(numeric_values)
            
            if len(numeric_values) > 1:
                field_profile.std_dev = statistics.stdev(numeric_values)
            
            # Percentiles
            field_profile.percentiles = {
                'p10': np.percentile(numeric_values, 10),
                'p25': np.percentile(numeric_values, 25),
                'p50': np.percentile(numeric_values, 50),
                'p75': np.percentile(numeric_values, 75),
                'p90': np.percentile(numeric_values, 90),
                'p99': np.percentile(numeric_values, 99)
            }
            
            # Distribution analysis
            field_profile.distribution_info = self._analyze_numeric_distribution(numeric_values)
            
        except Exception as e:
            self.logger.error(f"Numeric field profiling failed: {e}")

    def _profile_string_field(self, field_profile: FieldProfile, values: list[Any]):
        """Profile string field with text analysis"""
        try:
            string_values = [str(value) for value in values]
            
            if not string_values:
                return
            
            # Length statistics
            lengths = [len(s) for s in string_values]
            field_profile.min_length = min(lengths)
            field_profile.max_length = max(lengths)
            field_profile.avg_length = statistics.mean(lengths)
            
            # Character set analysis
            field_profile.distribution_info = {
                'avg_length': field_profile.avg_length,
                'length_variance': statistics.variance(lengths) if len(lengths) > 1 else 0,
                'empty_strings': sum(1 for s in string_values if s == ''),
                'whitespace_only': sum(1 for s in string_values if s.isspace()),
                'alphanumeric_only': sum(1 for s in string_values if s.isalnum()),
                'contains_special_chars': sum(1 for s in string_values if not s.isalnum() and s != '')
            }
            
        except Exception as e:
            self.logger.error(f"String field profiling failed: {e}")

    def _profile_datetime_field(self, field_profile: FieldProfile, values: list[Any]):
        """Profile datetime field with temporal analysis"""
        try:
            datetime_values = []
            
            for value in values:
                try:
                    if isinstance(value, datetime):
                        datetime_values.append(value)
                    elif isinstance(value, str):
                        # Try parsing common datetime formats
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
                            try:
                                dt = datetime.strptime(value, fmt)
                                datetime_values.append(dt)
                                break
                            except ValueError:
                                continue
                except (ValueError, TypeError):
                    field_profile.format_errors += 1
            
            if datetime_values:
                field_profile.min_value = min(datetime_values)
                field_profile.max_value = max(datetime_values)
                
                # Temporal distribution analysis
                field_profile.distribution_info = {
                    'earliest_date': field_profile.min_value.isoformat(),
                    'latest_date': field_profile.max_value.isoformat(),
                    'date_range_days': (field_profile.max_value - field_profile.min_value).days,
                    'future_dates': sum(1 for dt in datetime_values if dt > datetime.now()),
                    'historical_dates': sum(1 for dt in datetime_values if dt < datetime.now() - timedelta(days=365*10))
                }
            
        except Exception as e:
            self.logger.error(f"Datetime field profiling failed: {e}")

    def _profile_boolean_field(self, field_profile: FieldProfile, values: list[Any]):
        """Profile boolean field"""
        try:
            boolean_values = []
            
            for value in values:
                str_value = str(value).lower().strip()
                if str_value in ['true', '1', 'yes', 'y', 't', 'on', 'enabled']:
                    boolean_values.append(True)
                elif str_value in ['false', '0', 'no', 'n', 'f', 'off', 'disabled']:
                    boolean_values.append(False)
                else:
                    field_profile.format_errors += 1
            
            if boolean_values:
                true_count = sum(boolean_values)
                false_count = len(boolean_values) - true_count
                
                field_profile.distribution_info = {
                    'true_count': true_count,
                    'false_count': false_count,
                    'true_percentage': true_count / len(boolean_values) if boolean_values else 0,
                    'false_percentage': false_count / len(boolean_values) if boolean_values else 0
                }
            
        except Exception as e:
            self.logger.error(f"Boolean field profiling failed: {e}")

    def _analyze_value_distribution(self, field_profile: FieldProfile, values: list[Any]):
        """Analyze value distribution and frequency"""
        try:
            # Count value frequencies
            value_counter = Counter(str(v) for v in values)
            field_profile.value_counts = dict(value_counter.most_common())
            field_profile.most_common_values = dict(value_counter.most_common(10))
            
        except Exception as e:
            self.logger.error(f"Value distribution analysis failed: {e}")

    def _analyze_numeric_distribution(self, values: list[float]) -> dict[str, Any]:
        """Analyze numeric distribution characteristics"""
        try:
            if len(values) < 2:
                return {}
            
            mean = statistics.mean(values)
            median = statistics.median(values)
            std_dev = statistics.stdev(values)
            
            # Skewness approximation
            skewness = 3 * (mean - median) / std_dev if std_dev > 0 else 0
            
            # Detect potential distribution type
            distribution_type = "normal"
            if abs(skewness) > 1:
                distribution_type = "skewed"
            if field_profile.unique_count < 10:
                distribution_type = "discrete"
            
            return {
                'distribution_type': distribution_type,
                'skewness': skewness,
                'coefficient_of_variation': std_dev / abs(mean) if mean != 0 else 0,
                'outlier_candidates': len([v for v in values if abs(v - mean) > 3 * std_dev]),
                'zero_count': sum(1 for v in values if v == 0),
                'negative_count': sum(1 for v in values if v < 0)
            }
            
        except Exception as e:
            self.logger.error(f"Numeric distribution analysis failed: {e}")
            return {}

    def _assess_field_quality(
        self,
        field_profile: FieldProfile,
        values: list[Any],
        schema: dict[str, Any] | None = None
    ):
        """Assess data quality for a field"""
        try:
            quality_score = 1.0
            
            # Penalize for null values
            if field_profile.null_percentage > 0:
                quality_score -= field_profile.null_percentage * 0.3
            
            # Penalize for format errors
            if field_profile.format_errors > 0:
                error_rate = field_profile.format_errors / len(values)
                quality_score -= error_rate * 0.5
            
            # Penalize for type errors
            if field_profile.type_errors > 0:
                error_rate = field_profile.type_errors / len(values)
                quality_score -= error_rate * 0.4
            
            # Bonus for high uniqueness in appropriate fields
            if field_profile.unique_percentage > 0.9:
                quality_score += 0.1
            
            field_profile.quality_score = max(0.0, quality_score)
            
        except Exception as e:
            self.logger.error(f"Field quality assessment failed: {e}")

    def _classify_field_content(self, field_profile: FieldProfile, values: list[Any]):
        """Classify field content using pattern recognition"""
        try:
            string_values = [str(v) for v in values[:100]]  # Sample for performance
            
            # Test against known patterns
            pattern_matches = {}
            for pattern_name, pattern in self.patterns.items():
                matches = sum(1 for v in string_values if pattern.match(v))
                if matches > 0:
                    pattern_matches[pattern_name] = matches
            
            field_profile.pattern_matches = pattern_matches
            
            # Determine primary classification
            if pattern_matches:
                # Find pattern with highest match percentage
                best_pattern = max(pattern_matches.items(), key=lambda x: x[1])
                if best_pattern[1] / len(string_values) > 0.7:
                    field_profile.data_classification = best_pattern[0]
            
            # Additional heuristics
            if not field_profile.data_classification:
                if field_profile.unique_percentage == 1.0:
                    field_profile.data_classification = "identifier"
                elif field_profile.unique_count < 20 and field_profile.unique_percentage < 0.5:
                    field_profile.data_classification = "categorical"
                elif field_profile.data_type in ['int', 'float']:
                    field_profile.data_classification = "measure"
            
        except Exception as e:
            self.logger.error(f"Field content classification failed: {e}")

    def _calculate_dataset_metrics(self, profile: DatasetProfile, data: list[dict[str, Any]]):
        """Calculate dataset-level quality metrics"""
        try:
            if not profile.fields:
                return
            
            # Completeness score
            avg_completeness = statistics.mean([
                1 - field.null_percentage for field in profile.fields.values()
            ])
            profile.completeness_score = avg_completeness
            
            # Uniqueness score (average uniqueness across appropriate fields)
            uniqueness_scores = []
            for field in profile.fields.values():
                if field.data_classification in ["identifier", "measure"]:
                    uniqueness_scores.append(field.unique_percentage)
                elif field.data_classification == "categorical":
                    # For categorical fields, moderate uniqueness is expected
                    ideal_uniqueness = 0.1  # Adjust based on domain
                    uniqueness_scores.append(1.0 - abs(field.unique_percentage - ideal_uniqueness))
            
            if uniqueness_scores:
                profile.uniqueness_score = statistics.mean(uniqueness_scores)
            
            # Consistency score (based on format/type errors)
            consistency_scores = []
            for field in profile.fields.values():
                total_errors = field.format_errors + field.type_errors
                if len(data) > 0:
                    error_rate = total_errors / len(data)
                    consistency_scores.append(1.0 - error_rate)
                else:
                    consistency_scores.append(1.0)
            
            if consistency_scores:
                profile.consistency_score = statistics.mean(consistency_scores)
            
            # Overall quality score
            weights = {'completeness': 0.4, 'uniqueness': 0.3, 'consistency': 0.3}
            profile.overall_quality_score = (
                profile.completeness_score * weights['completeness'] +
                profile.uniqueness_score * weights['uniqueness'] +
                profile.consistency_score * weights['consistency']
            )
            
        except Exception as e:
            self.logger.error(f"Dataset metrics calculation failed: {e}")

    def _detect_relationships(self, profile: DatasetProfile, data: list[dict[str, Any]]):
        """Detect potential relationships and constraints"""
        try:
            # Identify primary key candidates
            for field_name, field in profile.fields.items():
                if (field.unique_percentage == 1.0 and 
                    field.null_percentage == 0.0 and
                    field.data_classification == "identifier"):
                    profile.primary_key_candidates.append(field_name)
            
            # Detect foreign key relationships (simplified)
            for field_name, field in profile.fields.items():
                if (field.data_classification == "identifier" and 
                    field_name not in profile.primary_key_candidates and
                    field.unique_percentage < 1.0):
                    profile.foreign_key_candidates.append({
                        'field': field_name,
                        'unique_percentage': field.unique_percentage,
                        'suggested_reference': 'unknown'
                    })
            
            # Calculate field correlations for numeric fields
            numeric_fields = [
                name for name, field in profile.fields.items()
                if field.data_type in ['int', 'float', 'numeric']
            ]
            
            if len(numeric_fields) > 1:
                correlations = {}
                for field1 in numeric_fields:
                    correlations[field1] = {}
                    values1 = [
                        float(record.get(field1, 0)) for record in data
                        if record.get(field1) is not None
                    ]
                    
                    for field2 in numeric_fields:
                        if field1 != field2:
                            values2 = [
                                float(record.get(field2, 0)) for record in data
                                if record.get(field2) is not None
                            ]
                            
                            if len(values1) == len(values2) and len(values1) > 1:
                                try:
                                    correlation = np.corrcoef(values1, values2)[0, 1]
                                    if not np.isnan(correlation):
                                        correlations[field1][field2] = float(correlation)
                                except:
                                    continue
                
                profile.field_correlations = correlations
            
        except Exception as e:
            self.logger.error(f"Relationship detection failed: {e}")

    def _generate_recommendations(self, profile: DatasetProfile):
        """Generate data quality recommendations"""
        try:
            recommendations = []
            
            # Completeness recommendations
            incomplete_fields = [
                name for name, field in profile.fields.items()
                if field.null_percentage > 0.1
            ]
            if incomplete_fields:
                recommendations.append(
                    f"Address missing values in fields: {', '.join(incomplete_fields[:5])}"
                )
            
            # Data type recommendations
            type_error_fields = [
                name for name, field in profile.fields.items()
                if field.type_errors > 0
            ]
            if type_error_fields:
                recommendations.append(
                    f"Review data types for fields: {', '.join(type_error_fields[:5])}"
                )
            
            # Format error recommendations
            format_error_fields = [
                name for name, field in profile.fields.items()
                if field.format_errors > 0
            ]
            if format_error_fields:
                recommendations.append(
                    f"Implement format validation for fields: {', '.join(format_error_fields[:5])}"
                )
            
            # Primary key recommendation
            if not profile.primary_key_candidates:
                high_unique_fields = [
                    name for name, field in profile.fields.items()
                    if field.unique_percentage > 0.95
                ]
                if high_unique_fields:
                    recommendations.append(
                        f"Consider using {high_unique_fields[0]} as primary key"
                    )
                else:
                    recommendations.append("Consider adding a surrogate primary key")
            
            # Index recommendations
            categorical_fields = [
                name for name, field in profile.fields.items()
                if field.data_classification == "categorical"
            ]
            if categorical_fields:
                recommendations.append(
                    f"Consider indexing categorical fields: {', '.join(categorical_fields[:3])}"
                )
            
            # Data quality threshold recommendations
            low_quality_fields = [
                name for name, field in profile.fields.items()
                if field.quality_score < 0.8
            ]
            if low_quality_fields:
                recommendations.append(
                    f"Improve data quality for fields: {', '.join(low_quality_fields[:5])}"
                )
            
            profile.recommendations = recommendations[:10]  # Limit to top 10
            
        except Exception as e:
            self.logger.error(f"Recommendations generation failed: {e}")

    def _profile_to_dict(self, profile: DatasetProfile) -> dict[str, Any]:
        """Convert profile to dictionary for serialization"""
        try:
            return {
                'summary': {
                    'dataset_name': profile.dataset_name,
                    'total_records': profile.total_records,
                    'total_fields': profile.total_fields,
                    'sample_size': profile.sample_size,
                    'profiling_duration_seconds': profile.profiling_duration_seconds,
                    'overall_quality_score': profile.overall_quality_score,
                    'completeness_score': profile.completeness_score,
                    'consistency_score': profile.consistency_score,
                    'uniqueness_score': profile.uniqueness_score
                },
                'fields': {
                    name: {
                        'name': field.name,
                        'data_type': field.data_type,
                        'null_count': field.null_count,
                        'null_percentage': field.null_percentage,
                        'unique_count': field.unique_count,
                        'unique_percentage': field.unique_percentage,
                        'min_value': field.min_value,
                        'max_value': field.max_value,
                        'mean': field.mean,
                        'median': field.median,
                        'std_dev': field.std_dev,
                        'min_length': field.min_length,
                        'max_length': field.max_length,
                        'avg_length': field.avg_length,
                        'most_common_values': field.most_common_values,
                        'format_errors': field.format_errors,
                        'type_errors': field.type_errors,
                        'percentiles': field.percentiles,
                        'distribution_info': field.distribution_info,
                        'quality_score': field.quality_score,
                        'data_classification': field.data_classification,
                        'pattern_matches': field.pattern_matches,
                        'suggested_type': field.suggested_type
                    } for name, field in profile.fields.items()
                },
                'relationships': {
                    'primary_key_candidates': profile.primary_key_candidates,
                    'foreign_key_candidates': profile.foreign_key_candidates,
                    'field_correlations': profile.field_correlations
                },
                'schema': {
                    'inferred_schema': profile.inferred_schema,
                    'suggested_indexes': profile.suggested_indexes
                },
                'recommendations': profile.recommendations,
                'quality_issues': profile.data_quality_issues,
                'metadata': {
                    'profiling_timestamp': profile.profiling_timestamp.isoformat(),
                    'profiling_duration_seconds': profile.profiling_duration_seconds
                }
            }
            
        except Exception as e:
            self.logger.error(f"Profile serialization failed: {e}")
            return {"error": str(e)}