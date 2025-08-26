"""
Advanced Data Quality Validators and Governance Framework
"""
from __future__ import annotations

import re
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from core.logging import get_logger


class ValidationSeverity(str, Enum):
    """Validation severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DataQualityDimension(str, Enum):
    """Data quality dimensions framework"""
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"
    PROFILING = "profiling"


class ValidationResult(BaseModel):
    """Enhanced result of a validation check"""
    is_valid: bool
    message: str
    severity: ValidationSeverity = Field(default=ValidationSeverity.INFO)
    dimension: DataQualityDimension
    score: float = Field(default=1.0, ge=0.0, le=1.0)
    details: dict[str, Any] | None = None
    remediation_suggestions: list[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    validator_name: str = ""
    
    @property
    def weight(self) -> float:
        """Get weight based on severity"""
        weights = {
            ValidationSeverity.INFO: 0.1,
            ValidationSeverity.WARNING: 0.5,
            ValidationSeverity.ERROR: 1.0,
            ValidationSeverity.CRITICAL: 2.0
        }
        return weights.get(self.severity, 1.0)


class DataLineage(BaseModel):
    """Data lineage tracking"""
    source_system: str
    dataset_name: str
    transformation_steps: list[str] = Field(default_factory=list)
    upstream_dependencies: list[str] = Field(default_factory=list)
    downstream_consumers: list[str] = Field(default_factory=list)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    data_owner: str = ""
    business_glossary_terms: list[str] = Field(default_factory=list)


class DataProfile(BaseModel):
    """Comprehensive data profiling results"""
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float
    min_value: Any = None
    max_value: Any = None
    mean_value: float | None = None
    median_value: float | None = None
    std_dev: float | None = None
    quartiles: dict[str, float] = Field(default_factory=dict)
    mode_values: list[Any] = Field(default_factory=list)
    pattern_analysis: dict[str, Any] = Field(default_factory=dict)
    outliers_count: int = 0
    data_distribution: dict[str, Any] = Field(default_factory=dict)


class DataQualityValidator(ABC):
    """Enhanced base class for data quality validators"""
    
    def __init__(self, name: str, dimension: DataQualityDimension, weight: float = 1.0):
        self.name = name
        self.dimension = dimension
        self.weight = weight
        self.logger = get_logger(self.__class__.__name__)
        self.remediation_rules: dict[str, list[str]] = {}
    
    @abstractmethod
    def validate(self, data: pd.DataFrame, metadata: dict[str, Any] | None = None) -> ValidationResult:
        """Validate the data and return result"""
        pass
    
    def add_remediation_rule(self, condition: str, suggestions: list[str]):
        """Add remediation suggestions for specific conditions"""
        self.remediation_rules[condition] = suggestions


class CompletenessValidator(DataQualityValidator):
    """Enhanced completeness validator with advanced features"""
    
    def __init__(self, column: str, threshold: float = 0.95, 
                 critical_threshold: float = 0.8, check_patterns: bool = True):
        super().__init__("completeness", DataQualityDimension.COMPLETENESS)
        self.column = column
        self.threshold = threshold
        self.critical_threshold = critical_threshold
        self.check_patterns = check_patterns
        
        # Add remediation rules
        self.add_remediation_rule("high_nulls", [
            "Investigate data source for collection issues",
            "Implement default value strategy",
            "Consider data imputation techniques",
            "Review ETL pipeline for data loss"
        ])
    
    def validate(self, data: pd.DataFrame, metadata: dict[str, Any] | None = None) -> ValidationResult:
        if self.column not in data.columns:
            return ValidationResult(
                is_valid=False,
                message=f"Column '{self.column}' not found",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name
            )
        
        total_rows = len(data)
        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                message="Empty dataset - completeness check skipped",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name
            )
        
        # Basic completeness check
        non_null_rows = data[self.column].notna().sum()
        completeness_ratio = non_null_rows / total_rows
        
        # Pattern-based completeness check
        pattern_details = {}
        if self.check_patterns and data[self.column].dtype == 'object':
            # Check for empty strings, whitespace-only values
            empty_strings = (data[self.column] == "").sum()
            whitespace_only = data[self.column].str.strip().eq("").sum()
            
            effective_completeness = (total_rows - data[self.column].isna().sum() - 
                                    empty_strings - whitespace_only) / total_rows
            
            pattern_details = {
                "empty_strings": empty_strings,
                "whitespace_only": whitespace_only,
                "effective_completeness": effective_completeness
            }
            
            completeness_ratio = effective_completeness
        
        # Determine severity and validity
        if completeness_ratio >= self.threshold:
            is_valid = True
            severity = ValidationSeverity.INFO
        elif completeness_ratio >= self.critical_threshold:
            is_valid = False
            severity = ValidationSeverity.WARNING
        else:
            is_valid = False
            severity = ValidationSeverity.CRITICAL
        
        # Generate remediation suggestions
        remediation = []
        if completeness_ratio < self.threshold:
            remediation = self.remediation_rules.get("high_nulls", [])
        
        details = {
            "total_rows": total_rows,
            "non_null_rows": non_null_rows,
            "completeness_ratio": completeness_ratio,
            "threshold": self.threshold,
            "critical_threshold": self.critical_threshold,
            **pattern_details
        }
        
        return ValidationResult(
            is_valid=is_valid,
            message=f"Completeness for '{self.column}': {completeness_ratio:.2%} (threshold: {self.threshold:.2%})",
            severity=severity,
            dimension=self.dimension,
            score=completeness_ratio,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name
        )


class UniquenessValidator(DataQualityValidator):
    """Enhanced uniqueness validator with duplicate analysis"""
    
    def __init__(self, columns: list[str], allow_duplicates: bool = False, 
                 max_duplicate_percentage: float = 0.05):
        super().__init__("uniqueness", DataQualityDimension.UNIQUENESS)
        self.columns = columns
        self.allow_duplicates = allow_duplicates
        self.max_duplicate_percentage = max_duplicate_percentage
        
        self.add_remediation_rule("duplicates_found", [
            "Review data source for duplicate generation",
            "Implement deduplication logic in ETL pipeline",
            "Add unique constraints to database",
            "Consider composite keys for uniqueness"
        ])
    
    def validate(self, data: pd.DataFrame, metadata: dict[str, Any] | None = None) -> ValidationResult:
        missing_cols = [col for col in self.columns if col not in data.columns]
        if missing_cols:
            return ValidationResult(
                is_valid=False,
                message=f"Columns not found: {missing_cols}",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name
            )
        
        total_rows = len(data)
        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                message="Empty dataset - uniqueness check skipped",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name
            )
        
        # Advanced duplicate analysis
        subset_data = data[self.columns].copy()
        
        # Find duplicates and analyze patterns
        duplicate_mask = subset_data.duplicated(keep=False)
        duplicate_count = duplicate_mask.sum()
        unique_rows = total_rows - duplicate_count
        duplicate_percentage = duplicate_count / total_rows if total_rows > 0 else 0
        
        # Analyze duplicate patterns
        duplicate_groups = subset_data[duplicate_mask].groupby(self.columns).size().reset_index(name='count')
        max_duplicate_group = duplicate_groups['count'].max() if not duplicate_groups.empty else 0
        
        # Determine validity and severity
        if self.allow_duplicates:
            is_valid = duplicate_percentage <= self.max_duplicate_percentage
            severity = ValidationSeverity.WARNING if not is_valid else ValidationSeverity.INFO
        else:
            is_valid = duplicate_count == 0
            severity = ValidationSeverity.ERROR if duplicate_count > 0 else ValidationSeverity.INFO
        
        # Calculate score
        score = 1.0 - min(duplicate_percentage, 1.0)
        
        # Generate remediation suggestions
        remediation = []
        if duplicate_count > 0:
            remediation = self.remediation_rules.get("duplicates_found", [])
        
        details = {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": duplicate_percentage,
            "max_duplicate_group": max_duplicate_group,
            "duplicate_groups_count": len(duplicate_groups),
            "allow_duplicates": self.allow_duplicates,
            "max_duplicate_percentage": self.max_duplicate_percentage
        }
        
        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {duplicate_count} duplicate rows ({duplicate_percentage:.2%}) in columns {self.columns}",
            severity=severity,
            dimension=self.dimension,
            score=score,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name
        )


class ValidityValidator(DataQualityValidator):
    """Enhanced validity validator with pattern matching and domain validation"""
    
    def __init__(self, column: str, validation_rules: dict[str, Any]):
        super().__init__("validity", DataQualityDimension.VALIDITY)
        self.column = column
        self.validation_rules = validation_rules
        
        self.add_remediation_rule("invalid_format", [
            "Review data input validation rules",
            "Implement data cleansing transformations",
            "Add format validation at source system",
            "Consider data standardization processes"
        ])
    
    def validate(self, data: pd.DataFrame, metadata: dict[str, Any] | None = None) -> ValidationResult:
        if self.column not in data.columns:
            return ValidationResult(
                is_valid=False,
                message=f"Column '{self.column}' not found",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name
            )
        
        total_rows = len(data)
        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                message="Empty dataset - validity check skipped",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name
            )
        
        invalid_count = 0
        validation_details = {}
        
        # Apply validation rules
        for rule_name, rule_config in self.validation_rules.items():
            rule_invalid_count = self._apply_validation_rule(data, rule_name, rule_config)
            validation_details[rule_name] = rule_invalid_count
            invalid_count += rule_invalid_count
        
        valid_rows = total_rows - invalid_count
        validity_ratio = valid_rows / total_rows if total_rows > 0 else 1.0
        
        # Determine validity and severity
        is_valid = invalid_count == 0
        if invalid_count > 0:
            if invalid_count / total_rows > 0.1:  # > 10% invalid
                severity = ValidationSeverity.CRITICAL
            elif invalid_count / total_rows > 0.05:  # > 5% invalid
                severity = ValidationSeverity.ERROR
            else:
                severity = ValidationSeverity.WARNING
        else:
            severity = ValidationSeverity.INFO
        
        # Generate remediation suggestions
        remediation = []
        if invalid_count > 0:
            remediation = self.remediation_rules.get("invalid_format", [])
        
        details = {
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_count": invalid_count,
            "validity_ratio": validity_ratio,
            "validation_details": validation_details,
            "validation_rules": list(self.validation_rules.keys())
        }
        
        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {invalid_count} invalid values in '{self.column}' ({(invalid_count/total_rows)*100:.1f}%)",
            severity=severity,
            dimension=self.dimension,
            score=validity_ratio,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name
        )
    
    def _apply_validation_rule(self, data: pd.DataFrame, rule_name: str, rule_config: dict[str, Any]) -> int:
        """Apply a specific validation rule"""
        column_data = data[self.column].dropna()
        
        if rule_name == "regex":
            pattern = rule_config.get("pattern", "")
            if pattern:
                invalid_mask = ~column_data.astype(str).str.match(pattern, na=False)
                return invalid_mask.sum()
        
        elif rule_name == "enum":
            allowed_values = rule_config.get("values", [])
            if allowed_values:
                invalid_mask = ~column_data.isin(allowed_values)
                return invalid_mask.sum()
        
        elif rule_name == "range":
            min_val = rule_config.get("min")
            max_val = rule_config.get("max")
            numeric_data = pd.to_numeric(column_data, errors='coerce')
            invalid_count = 0
            
            if min_val is not None:
                invalid_count += (numeric_data < min_val).sum()
            if max_val is not None:
                invalid_count += (numeric_data > max_val).sum()
            
            return invalid_count
        
        elif rule_name == "length":
            min_len = rule_config.get("min", 0)
            max_len = rule_config.get("max", float('inf'))
            lengths = column_data.astype(str).str.len()
            invalid_mask = (lengths < min_len) | (lengths > max_len)
            return invalid_mask.sum()
        
        elif rule_name == "custom":
            custom_func = rule_config.get("function")
            if callable(custom_func):
                try:
                    invalid_mask = ~column_data.apply(custom_func)
                    return invalid_mask.sum()
                except Exception:
                    return 0
        
        return 0


class StatisticalOutlierValidator(DataQualityValidator):
    """Statistical outlier detection validator"""
    
    def __init__(self, column: str, method: str = "iqr", contamination: float = 0.1):
        super().__init__("outlier_detection", DataQualityDimension.VALIDITY)
        self.column = column
        self.method = method  # 'iqr', 'zscore', 'isolation_forest'
        self.contamination = contamination
        
        self.add_remediation_rule("outliers_detected", [
            "Investigate outlier values for data entry errors",
            "Consider data transformation or normalization",
            "Review business rules for valid ranges",
            "Implement outlier handling in ETL pipeline"
        ])
    
    def validate(self, data: pd.DataFrame, metadata: dict[str, Any] | None = None) -> ValidationResult:
        if self.column not in data.columns:
            return ValidationResult(
                is_valid=False,
                message=f"Column '{self.column}' not found",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name
            )
        
        numeric_data = pd.to_numeric(data[self.column], errors='coerce').dropna()
        
        if len(numeric_data) < 10:  # Need minimum data for statistical analysis
            return ValidationResult(
                is_valid=True,
                message="Insufficient data for outlier analysis",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name
            )
        
        outliers_count = 0
        outlier_details = {}
        
        try:
            if self.method == "iqr":
                Q1 = numeric_data.quantile(0.25)
                Q3 = numeric_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers_mask = (numeric_data < lower_bound) | (numeric_data > upper_bound)
                outliers_count = outliers_mask.sum()
                
                outlier_details = {
                    "method": "IQR",
                    "Q1": Q1,
                    "Q3": Q3,
                    "IQR": IQR,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound
                }
            
            elif self.method == "zscore":
                z_scores = np.abs(stats.zscore(numeric_data))
                outliers_mask = z_scores > 3  # Standard threshold
                outliers_count = outliers_mask.sum()
                
                outlier_details = {
                    "method": "Z-Score",
                    "threshold": 3,
                    "mean": numeric_data.mean(),
                    "std": numeric_data.std()
                }
            
            elif self.method == "isolation_forest":
                if len(numeric_data) >= 100:  # Minimum for isolation forest
                    scaler = StandardScaler()
                    scaled_data = scaler.fit_transform(numeric_data.values.reshape(-1, 1))
                    
                    iso_forest = IsolationForest(contamination=self.contamination, random_state=42)
                    outliers_pred = iso_forest.fit_predict(scaled_data)
                    outliers_count = (outliers_pred == -1).sum()
                    
                    outlier_details = {
                        "method": "Isolation Forest",
                        "contamination": self.contamination,
                        "model_score": iso_forest.score_samples(scaled_data).mean()
                    }
                else:
                    # Fall back to IQR for small datasets
                    return self._iqr_fallback(numeric_data)
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                message=f"Outlier detection failed: {str(e)}",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name
            )
        
        total_rows = len(numeric_data)
        outlier_percentage = outliers_count / total_rows if total_rows > 0 else 0
        
        # Determine validity and severity
        is_valid = outlier_percentage <= 0.05  # Allow up to 5% outliers
        if outlier_percentage > 0.15:  # > 15% outliers
            severity = ValidationSeverity.CRITICAL
        elif outlier_percentage > 0.10:  # > 10% outliers
            severity = ValidationSeverity.ERROR
        elif outlier_percentage > 0.05:  # > 5% outliers
            severity = ValidationSeverity.WARNING
        else:
            severity = ValidationSeverity.INFO
        
        score = max(0.0, 1.0 - outlier_percentage * 2)  # Penalize outliers
        
        # Generate remediation suggestions
        remediation = []
        if outliers_count > 0:
            remediation = self.remediation_rules.get("outliers_detected", [])
        
        details = {
            "total_rows": total_rows,
            "outliers_count": outliers_count,
            "outlier_percentage": outlier_percentage,
            **outlier_details
        }
        
        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {outliers_count} outliers ({outlier_percentage:.2%}) using {self.method} method",
            severity=severity,
            dimension=self.dimension,
            score=score,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name
        )
    
    def _iqr_fallback(self, numeric_data: pd.Series) -> ValidationResult:
        """Fallback to IQR method for small datasets"""
        Q1 = numeric_data.quantile(0.25)
        Q3 = numeric_data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers_mask = (numeric_data < lower_bound) | (numeric_data > upper_bound)
        outliers_count = outliers_mask.sum()
        outlier_percentage = outliers_count / len(numeric_data) if len(numeric_data) > 0 else 0
        
        return ValidationResult(
            is_valid=outlier_percentage <= 0.05,
            message=f"Found {outliers_count} outliers ({outlier_percentage:.2%}) using IQR fallback",
            severity=ValidationSeverity.INFO,
            dimension=self.dimension,
            score=max(0.0, 1.0 - outlier_percentage * 2),
            details={
                "method": "IQR (fallback)",
                "outliers_count": outliers_count,
                "outlier_percentage": outlier_percentage
            },
            validator_name=self.name
        )


class ConsistencyValidator(DataQualityValidator):
    """Cross-column consistency validator"""
    
    def __init__(self, consistency_rules: dict[str, dict[str, Any]]):
        super().__init__("consistency", DataQualityDimension.CONSISTENCY)
        self.consistency_rules = consistency_rules
        
        self.add_remediation_rule("inconsistent_data", [
            "Review business rules for data relationships",
            "Implement cross-field validation logic",
            "Add referential integrity constraints",
            "Standardize data entry processes"
        ])
    
    def validate(self, data: pd.DataFrame, metadata: dict[str, Any] | None = None) -> ValidationResult:
        inconsistent_count = 0
        rule_details = {}
        
        for rule_name, rule_config in self.consistency_rules.items():
            rule_violations = self._check_consistency_rule(data, rule_name, rule_config)
            rule_details[rule_name] = rule_violations
            inconsistent_count += rule_violations
        
        total_rows = len(data)
        consistency_ratio = 1.0 - (inconsistent_count / total_rows) if total_rows > 0 else 1.0
        
        is_valid = inconsistent_count == 0
        if inconsistent_count > 0:
            if inconsistent_count / total_rows > 0.1:
                severity = ValidationSeverity.CRITICAL
            elif inconsistent_count / total_rows > 0.05:
                severity = ValidationSeverity.ERROR
            else:
                severity = ValidationSeverity.WARNING
        else:
            severity = ValidationSeverity.INFO
        
        remediation = []
        if inconsistent_count > 0:
            remediation = self.remediation_rules.get("inconsistent_data", [])
        
        details = {
            "total_rows": total_rows,
            "inconsistent_count": inconsistent_count,
            "consistency_ratio": consistency_ratio,
            "rule_details": rule_details
        }
        
        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {inconsistent_count} consistency violations across {len(self.consistency_rules)} rules",
            severity=severity,
            dimension=self.dimension,
            score=consistency_ratio,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name
        )
    
    def _check_consistency_rule(self, data: pd.DataFrame, rule_name: str, rule_config: dict[str, Any]) -> int:
        """Check a specific consistency rule"""
        try:
            rule_type = rule_config.get("type", "")
            
            if rule_type == "conditional":
                condition = rule_config.get("condition", "")
                expected = rule_config.get("expected", "")
                
                if condition and expected:
                    condition_mask = data.eval(condition)
                    expected_mask = data.eval(expected)
                    violations = condition_mask & ~expected_mask
                    return violations.sum()
            
            elif rule_type == "relationship":
                column1 = rule_config.get("column1", "")
                column2 = rule_config.get("column2", "")
                relationship = rule_config.get("relationship", "")  # "greater", "less", "equal"
                
                if column1 in data.columns and column2 in data.columns:
                    if relationship == "greater":
                        violations = data[column1] <= data[column2]
                    elif relationship == "less":
                        violations = data[column1] >= data[column2]
                    elif relationship == "equal":
                        violations = data[column1] != data[column2]
                    else:
                        return 0
                    
                    return violations.sum()
            
            elif rule_type == "custom":
                custom_func = rule_config.get("function")
                if callable(custom_func):
                    violations = data.apply(custom_func, axis=1)
                    return (~violations).sum()
        
        except Exception as e:
            self.logger.warning(f"Consistency rule '{rule_name}' failed: {str(e)}")
        
        return 0


class DataProfiler:
    """Comprehensive data profiling engine"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def profile_dataset(self, data: pd.DataFrame, dataset_name: str = "unknown") -> dict[str, DataProfile]:
        """Generate comprehensive data profile for entire dataset"""
        profiles = {}
        
        for column in data.columns:
            try:
                profile = self._profile_column(data[column], column)
                profiles[column] = profile
            except Exception as e:
                self.logger.error(f"Failed to profile column '{column}': {str(e)}")
                profiles[column] = DataProfile(
                    column_name=column,
                    data_type="unknown",
                    null_count=0,
                    null_percentage=0.0,
                    unique_count=0,
                    unique_percentage=0.0
                )
        
        return profiles
    
    def _profile_column(self, series: pd.Series, column_name: str) -> DataProfile:
        """Profile a single column"""
        total_count = len(series)
        null_count = series.isna().sum()
        non_null_series = series.dropna()
        
        profile = DataProfile(
            column_name=column_name,
            data_type=str(series.dtype),
            null_count=null_count,
            null_percentage=null_count / total_count if total_count > 0 else 0.0,
            unique_count=series.nunique(),
            unique_percentage=series.nunique() / total_count if total_count > 0 else 0.0
        )
        
        if len(non_null_series) == 0:
            return profile
        
        # Basic statistics
        if pd.api.types.is_numeric_dtype(series):
            profile.min_value = float(non_null_series.min())
            profile.max_value = float(non_null_series.max())
            profile.mean_value = float(non_null_series.mean())
            profile.median_value = float(non_null_series.median())
            profile.std_dev = float(non_null_series.std())
            
            # Quartiles
            profile.quartiles = {
                "Q1": float(non_null_series.quantile(0.25)),
                "Q2": float(non_null_series.quantile(0.5)),
                "Q3": float(non_null_series.quantile(0.75))
            }
            
            # Outlier detection
            Q1 = profile.quartiles["Q1"]
            Q3 = profile.quartiles["Q3"]
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = non_null_series[(non_null_series < lower_bound) | 
                                     (non_null_series > upper_bound)]
            profile.outliers_count = len(outliers)
            
            # Distribution analysis
            try:
                # Test for normality
                if len(non_null_series) >= 8:  # Minimum for normality test
                    shapiro_stat, shapiro_p = stats.shapiro(non_null_series.sample(min(5000, len(non_null_series))))
                    profile.data_distribution = {
                        "shapiro_stat": float(shapiro_stat),
                        "shapiro_p_value": float(shapiro_p),
                        "is_normal": shapiro_p > 0.05,
                        "skewness": float(stats.skew(non_null_series)),
                        "kurtosis": float(stats.kurtosis(non_null_series))
                    }
            except Exception:
                profile.data_distribution = {"analysis_failed": True}
        
        else:
            # String/categorical data
            profile.min_value = str(non_null_series.min())
            profile.max_value = str(non_null_series.max())
            
            # Mode analysis
            mode_values = non_null_series.mode()
            profile.mode_values = mode_values.head(5).tolist()
            
            # Pattern analysis for strings
            if series.dtype == 'object':
                profile.pattern_analysis = self._analyze_string_patterns(non_null_series)
        
        return profile
    
    def _analyze_string_patterns(self, series: pd.Series) -> dict[str, Any]:
        """Analyze patterns in string data"""
        patterns = {}
        
        # Length analysis
        lengths = series.astype(str).str.len()
        patterns["length_stats"] = {
            "min_length": int(lengths.min()),
            "max_length": int(lengths.max()),
            "avg_length": float(lengths.mean()),
            "std_length": float(lengths.std())
        }
        
        # Common patterns
        string_series = series.astype(str)
        
        # Email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        email_matches = string_series.str.match(email_pattern, na=False).sum()
        patterns["email_like"] = {
            "count": int(email_matches),
            "percentage": email_matches / len(series)
        }
        
        # Phone pattern (simple)
        phone_pattern = r'^\+?[\d\s\-\(\)]{7,15}$'
        phone_matches = string_series.str.match(phone_pattern, na=False).sum()
        patterns["phone_like"] = {
            "count": int(phone_matches),
            "percentage": phone_matches / len(series)
        }
        
        # URL pattern
        url_pattern = r'^https?://'
        url_matches = string_series.str.match(url_pattern, na=False).sum()
        patterns["url_like"] = {
            "count": int(url_matches),
            "percentage": url_matches / len(series)
        }
        
        # Numeric pattern
        numeric_pattern = r'^\d+$'
        numeric_matches = string_series.str.match(numeric_pattern, na=False).sum()
        patterns["numeric_like"] = {
            "count": int(numeric_matches),
            "percentage": numeric_matches / len(series)
        }
        
        return patterns


class DataQualityReport(BaseModel):
    """Enhanced comprehensive data quality report"""
    dataset_name: str
    total_records: int
    validation_results: list[ValidationResult]
    data_profiles: dict[str, DataProfile] = Field(default_factory=dict)
    lineage: DataLineage | None = None
    overall_score: float
    dimension_scores: dict[DataQualityDimension, float] = Field(default_factory=dict)
    critical_issues_count: int = 0
    recommendations: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    @property
    def passed_validations(self) -> int:
        return sum(1 for result in self.validation_results if result.is_valid)
    
    @property
    def failed_validations(self) -> int:
        return len(self.validation_results) - self.passed_validations
    
    @property
    def critical_issues(self) -> int:
        return sum(1 for result in self.validation_results 
                  if result.severity == ValidationSeverity.CRITICAL and not result.is_valid)
    
    def get_issues_by_severity(self) -> dict[ValidationSeverity, int]:
        """Get count of issues by severity level"""
        severity_counts = defaultdict(int)
        for result in self.validation_results:
            if not result.is_valid:
                severity_counts[result.severity] += 1
        return dict(severity_counts)
    
    def get_recommendations(self) -> list[str]:
        """Get all unique remediation recommendations"""
        all_recommendations = set()
        for result in self.validation_results:
            if not result.is_valid and result.remediation_suggestions:
                all_recommendations.update(result.remediation_suggestions)
        return list(all_recommendations)


class DataQualityGovernanceEngine:
    """Enterprise data quality and governance engine"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.validators: list[DataQualityValidator] = []
        self.profiler = DataProfiler()
        self.quality_history: dict[str, list[DataQualityReport]] = defaultdict(list)
        self.governance_rules: dict[str, Any] = {}
    
    def add_validator(self, validator: DataQualityValidator):
        """Add a validator to the engine"""
        self.validators.append(validator)
        self.logger.info(f"Added validator: {validator.name} ({validator.dimension.value})")
    
    def add_governance_rule(self, rule_name: str, rule_config: dict[str, Any]):
        """Add governance rule"""
        self.governance_rules[rule_name] = rule_config
        self.logger.info(f"Added governance rule: {rule_name}")
    
    def validate(self, data: pd.DataFrame, dataset_name: str = "unknown", 
                lineage: DataLineage | None = None, 
                generate_profile: bool = True) -> DataQualityReport:
        """Run comprehensive data quality validation and profiling"""
        
        self.logger.info(f"Starting data quality validation for dataset: {dataset_name}")
        
        # Run validations
        results = []
        for validator in self.validators:
            try:
                result = validator.validate(data)
                results.append(result)
                self.logger.debug(f"Validation '{validator.name}': {result.message}")
            except Exception as e:
                self.logger.error(f"Validation '{validator.name}' failed: {str(e)}")
                results.append(ValidationResult(
                    is_valid=False,
                    message=f"Validation failed: {str(e)}",
                    severity=ValidationSeverity.CRITICAL,
                    dimension=DataQualityDimension.VALIDITY,
                    score=0.0,
                    validator_name=validator.name
                ))
        
        # Calculate dimension scores
        dimension_scores = {}
        for dimension in DataQualityDimension:
            dimension_results = [r for r in results if r.dimension == dimension]
            if dimension_results:
                # Weighted average of scores
                total_weight = sum(r.weight for r in dimension_results)
                weighted_score = sum(r.score * r.weight for r in dimension_results) / total_weight if total_weight > 0 else 0
                dimension_scores[dimension] = weighted_score
        
        # Calculate overall score
        if results:
            total_weight = sum(r.weight for r in results)
            overall_score = sum(r.score * r.weight for r in results) / total_weight if total_weight > 0 else 0
        else:
            overall_score = 0.0
        
        # Generate data profiles
        data_profiles = {}
        if generate_profile:
            try:
                data_profiles = self.profiler.profile_dataset(data, dataset_name)
                self.logger.info(f"Generated profiles for {len(data_profiles)} columns")
            except Exception as e:
                self.logger.error(f"Data profiling failed: {str(e)}")
        
        # Count critical issues
        critical_issues_count = sum(1 for r in results 
                                  if r.severity == ValidationSeverity.CRITICAL and not r.is_valid)
        
        # Generate recommendations
        recommendations = []
        for result in results:
            if not result.is_valid and result.remediation_suggestions:
                recommendations.extend(result.remediation_suggestions)
        recommendations = list(set(recommendations))  # Remove duplicates
        
        # Create report
        report = DataQualityReport(
            dataset_name=dataset_name,
            total_records=len(data),
            validation_results=results,
            data_profiles=data_profiles,
            lineage=lineage,
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            critical_issues_count=critical_issues_count,
            recommendations=recommendations,
            metadata={
                "validators_count": len(self.validators),
                "governance_rules_count": len(self.governance_rules),
                "validation_timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Store in history
        self.quality_history[dataset_name].append(report)
        
        # Keep only last 100 reports per dataset
        if len(self.quality_history[dataset_name]) > 100:
            self.quality_history[dataset_name] = self.quality_history[dataset_name][-100:]
        
        self.logger.info(f"Data quality validation completed. Overall score: {overall_score:.2%}")
        return report
    
    def get_quality_trend(self, dataset_name: str, days: int = 30) -> dict[str, Any]:
        """Get quality trend for a dataset over specified days"""
        if dataset_name not in self.quality_history:
            return {"error": "No history found for dataset"}
        
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        recent_reports = [
            report for report in self.quality_history[dataset_name]
            if report.timestamp >= cutoff_date
        ]
        
        if not recent_reports:
            return {"error": "No recent data found"}
        
        # Calculate trends
        scores = [r.overall_score for r in recent_reports]
        timestamps = [r.timestamp for r in recent_reports]
        
        trend_data = {
            "dataset_name": dataset_name,
            "period_days": days,
            "reports_count": len(recent_reports),
            "latest_score": scores[-1] if scores else 0,
            "average_score": sum(scores) / len(scores) if scores else 0,
            "min_score": min(scores) if scores else 0,
            "max_score": max(scores) if scores else 0,
            "score_trend": "improving" if len(scores) >= 2 and scores[-1] > scores[0] else "declining" if len(scores) >= 2 and scores[-1] < scores[0] else "stable",
            "critical_issues_trend": [r.critical_issues_count for r in recent_reports],
            "timestamps": [t.isoformat() for t in timestamps],
            "scores": scores
        }
        
        return trend_data
    
    def export_governance_report(self, dataset_name: str | None = None) -> dict[str, Any]:
        """Export comprehensive governance report"""
        if dataset_name and dataset_name in self.quality_history:
            datasets_to_report = [dataset_name]
        else:
            datasets_to_report = list(self.quality_history.keys())
        
        governance_report = {
            "report_timestamp": datetime.utcnow().isoformat(),
            "datasets_monitored": len(datasets_to_report),
            "total_validators": len(self.validators),
            "governance_rules": len(self.governance_rules),
            "dataset_summaries": {}
        }
        
        for dataset in datasets_to_report:
            if self.quality_history[dataset]:
                latest_report = self.quality_history[dataset][-1]
                governance_report["dataset_summaries"][dataset] = {
                    "latest_score": latest_report.overall_score,
                    "total_records": latest_report.total_records,
                    "critical_issues": latest_report.critical_issues_count,
                    "dimension_scores": {dim.value: score for dim, score in latest_report.dimension_scores.items()},
                    "last_validated": latest_report.timestamp.isoformat(),
                    "validation_count": len(self.quality_history[dataset])
                }
        
        return governance_report