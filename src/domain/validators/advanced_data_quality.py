"""
Advanced Data Quality Validators and Governance Framework
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

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
            ValidationSeverity.CRITICAL: 2.0,
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
    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
        """Validate the data and return result"""
        pass

    def add_remediation_rule(self, condition: str, suggestions: list[str]):
        """Add remediation suggestions for specific conditions"""
        self.remediation_rules[condition] = suggestions


class CompletenessValidator(DataQualityValidator):
    """Enhanced completeness validator with advanced features"""

    def __init__(
        self,
        column: str,
        threshold: float = 0.95,
        critical_threshold: float = 0.8,
        check_patterns: bool = True,
    ):
        super().__init__("completeness", DataQualityDimension.COMPLETENESS)
        self.column = column
        self.threshold = threshold
        self.critical_threshold = critical_threshold
        self.check_patterns = check_patterns

        # Add remediation rules
        self.add_remediation_rule(
            "high_nulls",
            [
                "Investigate data source for collection issues",
                "Implement default value strategy",
                "Consider data imputation techniques",
                "Review ETL pipeline for data loss",
            ],
        )

    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
        if self.column not in data.columns:
            return ValidationResult(
                is_valid=False,
                message=f"Column '{self.column}' not found",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name,
            )

        total_rows = len(data)
        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                message="Empty dataset - completeness check skipped",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name,
            )

        # Basic completeness check
        non_null_rows = data[self.column].notna().sum()
        completeness_ratio = non_null_rows / total_rows

        # Pattern-based completeness check
        pattern_details = {}
        if self.check_patterns and data[self.column].dtype == "object":
            # Check for empty strings, whitespace-only values
            empty_strings = (data[self.column] == "").sum()
            whitespace_only = data[self.column].str.strip().eq("").sum()

            effective_completeness = (
                total_rows - data[self.column].isna().sum() - empty_strings - whitespace_only
            ) / total_rows

            pattern_details = {
                "empty_strings": empty_strings,
                "whitespace_only": whitespace_only,
                "effective_completeness": effective_completeness,
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
            **pattern_details,
        }

        return ValidationResult(
            is_valid=is_valid,
            message=f"Completeness for '{self.column}': {completeness_ratio:.2%} (threshold: {self.threshold:.2%})",
            severity=severity,
            dimension=self.dimension,
            score=completeness_ratio,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name,
        )


class UniquenessValidator(DataQualityValidator):
    """Enhanced uniqueness validator with duplicate analysis"""

    def __init__(
        self,
        columns: list[str],
        allow_duplicates: bool = False,
        max_duplicate_percentage: float = 0.05,
    ):
        super().__init__("uniqueness", DataQualityDimension.UNIQUENESS)
        self.columns = columns
        self.allow_duplicates = allow_duplicates
        self.max_duplicate_percentage = max_duplicate_percentage

        self.add_remediation_rule(
            "duplicates_found",
            [
                "Review data source for duplicate generation",
                "Implement deduplication logic in ETL pipeline",
                "Add unique constraints to database",
                "Consider composite keys for uniqueness",
            ],
        )

    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
        missing_cols = [col for col in self.columns if col not in data.columns]
        if missing_cols:
            return ValidationResult(
                is_valid=False,
                message=f"Columns not found: {missing_cols}",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name,
            )

        total_rows = len(data)
        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                message="Empty dataset - uniqueness check skipped",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name,
            )

        # Advanced duplicate analysis
        subset_data = data[self.columns].copy()

        # Find duplicates and analyze patterns
        duplicate_mask = subset_data.duplicated(keep=False)
        duplicate_count = duplicate_mask.sum()
        unique_rows = total_rows - duplicate_count
        duplicate_percentage = duplicate_count / total_rows if total_rows > 0 else 0

        # Analyze duplicate patterns
        duplicate_groups = (
            subset_data[duplicate_mask].groupby(self.columns).size().reset_index(name="count")
        )
        max_duplicate_group = duplicate_groups["count"].max() if not duplicate_groups.empty else 0

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
            "max_duplicate_percentage": self.max_duplicate_percentage,
        }

        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {duplicate_count} duplicate rows ({duplicate_percentage:.2%}) in columns {self.columns}",
            severity=severity,
            dimension=self.dimension,
            score=score,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name,
        )


class ValidityValidator(DataQualityValidator):
    """Enhanced validity validator with pattern matching and domain validation"""

    def __init__(self, column: str, validation_rules: dict[str, Any]):
        super().__init__("validity", DataQualityDimension.VALIDITY)
        self.column = column
        self.validation_rules = validation_rules

        self.add_remediation_rule(
            "invalid_format",
            [
                "Review data input validation rules",
                "Implement data cleansing transformations",
                "Add format validation at source system",
                "Consider data standardization processes",
            ],
        )

    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
        if self.column not in data.columns:
            return ValidationResult(
                is_valid=False,
                message=f"Column '{self.column}' not found",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name,
            )

        total_rows = len(data)
        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                message="Empty dataset - validity check skipped",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name,
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
            "validation_rules": list(self.validation_rules.keys()),
        }

        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {invalid_count} invalid values in '{self.column}' ({(invalid_count / total_rows) * 100:.1f}%)",
            severity=severity,
            dimension=self.dimension,
            score=validity_ratio,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name,
        )

    def _apply_validation_rule(
        self, data: pd.DataFrame, rule_name: str, rule_config: dict[str, Any]
    ) -> int:
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
            numeric_data = pd.to_numeric(column_data, errors="coerce")
            invalid_count = 0

            if min_val is not None:
                invalid_count += (numeric_data < min_val).sum()
            if max_val is not None:
                invalid_count += (numeric_data > max_val).sum()

            return invalid_count

        elif rule_name == "length":
            min_len = rule_config.get("min", 0)
            max_len = rule_config.get("max", float("inf"))
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

        self.add_remediation_rule(
            "outliers_detected",
            [
                "Investigate outlier values for data entry errors",
                "Consider data transformation or normalization",
                "Review business rules for valid ranges",
                "Implement outlier handling in ETL pipeline",
            ],
        )

    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
        if self.column not in data.columns:
            return ValidationResult(
                is_valid=False,
                message=f"Column '{self.column}' not found",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
                score=0.0,
                validator_name=self.name,
            )

        numeric_data = pd.to_numeric(data[self.column], errors="coerce").dropna()

        if len(numeric_data) < 10:  # Need minimum data for statistical analysis
            return ValidationResult(
                is_valid=True,
                message="Insufficient data for outlier analysis",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name,
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
                    "upper_bound": upper_bound,
                }

            elif self.method == "zscore":
                z_scores = np.abs(stats.zscore(numeric_data))
                outliers_mask = z_scores > 3  # Standard threshold
                outliers_count = outliers_mask.sum()

                outlier_details = {
                    "method": "Z-Score",
                    "threshold": 3,
                    "mean": numeric_data.mean(),
                    "std": numeric_data.std(),
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
                        "model_score": iso_forest.score_samples(scaled_data).mean(),
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
                validator_name=self.name,
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
            **outlier_details,
        }

        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {outliers_count} outliers ({outlier_percentage:.2%}) using {self.method} method",
            severity=severity,
            dimension=self.dimension,
            score=score,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name,
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
                "outlier_percentage": outlier_percentage,
            },
            validator_name=self.name,
        )


class ConsistencyValidator(DataQualityValidator):
    """Cross-column consistency validator"""

    def __init__(self, consistency_rules: dict[str, dict[str, Any]]):
        super().__init__("consistency", DataQualityDimension.CONSISTENCY)
        self.consistency_rules = consistency_rules

        self.add_remediation_rule(
            "inconsistent_data",
            [
                "Review business rules for data relationships",
                "Implement cross-field validation logic",
                "Add referential integrity constraints",
                "Standardize data entry processes",
            ],
        )

    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
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
            "rule_details": rule_details,
        }

        return ValidationResult(
            is_valid=is_valid,
            message=f"Found {inconsistent_count} consistency violations across {len(self.consistency_rules)} rules",
            severity=severity,
            dimension=self.dimension,
            score=consistency_ratio,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name,
        )

    def _check_consistency_rule(
        self, data: pd.DataFrame, rule_name: str, rule_config: dict[str, Any]
    ) -> int:
        """Check a specific consistency rule"""
        try:
            rule_type = rule_config.get("type", "")

            if rule_type == "conditional":
                condition = rule_config.get("condition", "")
                expected = rule_config.get("expected", "")

                if condition and expected:
                    # Validate expressions for security
                    if not self._is_safe_expression(condition) or not self._is_safe_expression(
                        expected
                    ):
                        self.logger.warning(f"Unsafe expression detected in rule {rule_name}")
                        return 0

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

    def _is_safe_expression(self, expression: str) -> bool:
        """Validate that pandas eval expression is safe with comprehensive security checks"""
        import ast

        # Basic input validation
        if not expression or len(expression) > 500:  # Limit expression length
            return False

        # Allow only safe characters: alphanumeric, operators, parentheses, spaces, and dots
        safe_pattern = r"^[a-zA-Z0-9_\s\+\-\*\/\(\)\.\>\<\=\&\|\!\~\[\]]+$"

        if not re.match(safe_pattern, expression):
            self.logger.warning(f"Expression contains unsafe characters: {expression}")
            return False

        # Block dangerous functions and keywords
        dangerous_patterns = [
            # Python builtins that could be dangerous
            r"__",
            "import",
            "exec",
            "eval",
            "compile",
            "open",
            "input",
            "raw_input",
            "globals",
            "locals",
            "vars",
            "dir",
            "help",
            "reload",
            "repr",
            "getattr",
            "setattr",
            "delattr",
            "hasattr",
            "callable",
            "isinstance",
            "issubclass",
            # System modules
            "sys",
            "os",
            "subprocess",
            "shutil",
            "tempfile",
            "pickle",
            "marshal",
            # Pandas dangerous functions
            "read_",
            "to_",
            "query",
            "pipe",
            "apply",
            "transform",
            "agg",
            "aggregate",
        ]

        expression_lower = expression.lower()
        for pattern in dangerous_patterns:
            if pattern in expression_lower:
                self.logger.warning(
                    f"Expression contains dangerous pattern '{pattern}': {expression}"
                )
                return False

        # Additional AST-based validation for pandas expressions
        try:
            # Parse expression to ensure it's syntactically valid
            parsed = ast.parse(expression, mode="eval")

            # Check for function calls (only allow basic operators)
            for node in ast.walk(parsed):
                if isinstance(node, ast.Call):
                    # Block all function calls in expressions
                    self.logger.warning(f"Function calls not allowed in expressions: {expression}")
                    return False
                elif isinstance(node, ast.Attribute) and hasattr(node, "attr"):
                    # Allow only safe attribute access (column names with dots)
                    if not re.match(r"^[a-zA-Z0-9_]+$", node.attr):
                        self.logger.warning(f"Unsafe attribute access: {expression}")
                        return False

        except SyntaxError:
            self.logger.warning(f"Invalid expression syntax: {expression}")
            return False
        except Exception as e:
            self.logger.error(f"Expression validation error: {e}")
            return False

        return True


class DataProfiler:
    """Comprehensive data profiling engine"""

    def __init__(self):
        self.logger = get_logger(__name__)

    def profile_dataset(
        self, data: pd.DataFrame, dataset_name: str = "unknown"
    ) -> dict[str, DataProfile]:
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
                    unique_percentage=0.0,
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
            unique_percentage=series.nunique() / total_count if total_count > 0 else 0.0,
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
                "Q3": float(non_null_series.quantile(0.75)),
            }

            # Outlier detection
            Q1 = profile.quartiles["Q1"]
            Q3 = profile.quartiles["Q3"]
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            outliers = non_null_series[
                (non_null_series < lower_bound) | (non_null_series > upper_bound)
            ]
            profile.outliers_count = len(outliers)

            # Distribution analysis
            try:
                # Test for normality
                if len(non_null_series) >= 8:  # Minimum for normality test
                    shapiro_stat, shapiro_p = stats.shapiro(
                        non_null_series.sample(min(5000, len(non_null_series)))
                    )
                    profile.data_distribution = {
                        "shapiro_stat": float(shapiro_stat),
                        "shapiro_p_value": float(shapiro_p),
                        "is_normal": shapiro_p > 0.05,
                        "skewness": float(stats.skew(non_null_series)),
                        "kurtosis": float(stats.kurtosis(non_null_series)),
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
            if series.dtype == "object":
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
            "std_length": float(lengths.std()),
        }

        # Common patterns
        string_series = series.astype(str)

        # Email pattern
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        email_matches = string_series.str.match(email_pattern, na=False).sum()
        patterns["email_like"] = {
            "count": int(email_matches),
            "percentage": email_matches / len(series),
        }

        # Phone pattern (simple)
        phone_pattern = r"^\+?[\d\s\-\(\)]{7,15}$"
        phone_matches = string_series.str.match(phone_pattern, na=False).sum()
        patterns["phone_like"] = {
            "count": int(phone_matches),
            "percentage": phone_matches / len(series),
        }

        # URL pattern
        url_pattern = r"^https?://"
        url_matches = string_series.str.match(url_pattern, na=False).sum()
        patterns["url_like"] = {"count": int(url_matches), "percentage": url_matches / len(series)}

        # Numeric pattern
        numeric_pattern = r"^\d+$"
        numeric_matches = string_series.str.match(numeric_pattern, na=False).sum()
        patterns["numeric_like"] = {
            "count": int(numeric_matches),
            "percentage": numeric_matches / len(series),
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
        return sum(
            1
            for result in self.validation_results
            if result.severity == ValidationSeverity.CRITICAL and not result.is_valid
        )

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

    def validate(
        self,
        data: pd.DataFrame,
        dataset_name: str = "unknown",
        lineage: DataLineage | None = None,
        generate_profile: bool = True,
    ) -> DataQualityReport:
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
                results.append(
                    ValidationResult(
                        is_valid=False,
                        message=f"Validation failed: {str(e)}",
                        severity=ValidationSeverity.CRITICAL,
                        dimension=DataQualityDimension.VALIDITY,
                        score=0.0,
                        validator_name=validator.name,
                    )
                )

        # Calculate dimension scores
        dimension_scores = {}
        for dimension in DataQualityDimension:
            dimension_results = [r for r in results if r.dimension == dimension]
            if dimension_results:
                # Weighted average of scores
                total_weight = sum(r.weight for r in dimension_results)
                weighted_score = (
                    sum(r.score * r.weight for r in dimension_results) / total_weight
                    if total_weight > 0
                    else 0
                )
                dimension_scores[dimension] = weighted_score

        # Calculate overall score
        if results:
            total_weight = sum(r.weight for r in results)
            overall_score = (
                sum(r.score * r.weight for r in results) / total_weight if total_weight > 0 else 0
            )
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
        critical_issues_count = sum(
            1 for r in results if r.severity == ValidationSeverity.CRITICAL and not r.is_valid
        )

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
                "validation_timestamp": datetime.utcnow().isoformat(),
            },
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
            report
            for report in self.quality_history[dataset_name]
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
            "score_trend": "improving"
            if len(scores) >= 2 and scores[-1] > scores[0]
            else "declining"
            if len(scores) >= 2 and scores[-1] < scores[0]
            else "stable",
            "critical_issues_trend": [r.critical_issues_count for r in recent_reports],
            "timestamps": [t.isoformat() for t in timestamps],
            "scores": scores,
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
            "dataset_summaries": {},
        }

        for dataset in datasets_to_report:
            if self.quality_history[dataset]:
                latest_report = self.quality_history[dataset][-1]
                governance_report["dataset_summaries"][dataset] = {
                    "latest_score": latest_report.overall_score,
                    "total_records": latest_report.total_records,
                    "critical_issues": latest_report.critical_issues_count,
                    "dimension_scores": {
                        dim.value: score for dim, score in latest_report.dimension_scores.items()
                    },
                    "last_validated": latest_report.timestamp.isoformat(),
                    "validation_count": len(self.quality_history[dataset]),
                }

        return governance_report


class DataDriftValidator(DataQualityValidator):
    """Advanced data drift detection validator with multiple statistical methods"""

    def __init__(
        self,
        baseline_data: pd.DataFrame,
        columns: list[str],
        drift_threshold: float = 0.1,
        detection_methods: list[str] = None,
    ):
        super().__init__("data_drift", DataQualityDimension.CONSISTENCY)
        self.baseline_data = baseline_data
        self.columns = columns
        self.drift_threshold = drift_threshold
        self.detection_methods = detection_methods or [
            "ks_test",
            "chi2_test",
            "psi",
            "js_divergence",
        ]
        self.baseline_stats = self._compute_baseline_statistics()

        self.add_remediation_rule(
            "significant_drift",
            [
                "Investigate data source changes",
                "Retrain models with new data distribution",
                "Update data validation rules",
                "Implement gradual model updates",
            ],
        )

    def _compute_baseline_statistics(self) -> dict[str, Any]:
        """Compute baseline statistics for drift detection"""
        stats = {}

        for column in self.columns:
            if column not in self.baseline_data.columns:
                continue

            col_data = self.baseline_data[column].dropna()

            if pd.api.types.is_numeric_dtype(col_data):
                stats[column] = {
                    "type": "numeric",
                    "mean": float(col_data.mean()),
                    "std": float(col_data.std()),
                    "min": float(col_data.min()),
                    "max": float(col_data.max()),
                    "quartiles": {
                        "q25": float(col_data.quantile(0.25)),
                        "q50": float(col_data.quantile(0.5)),
                        "q75": float(col_data.quantile(0.75)),
                    },
                    "histogram": np.histogram(col_data, bins=20)[0].tolist(),
                }
            else:
                # Categorical data
                value_counts = col_data.value_counts(normalize=True)
                stats[column] = {
                    "type": "categorical",
                    "categories": value_counts.index.tolist(),
                    "frequencies": value_counts.values.tolist(),
                    "entropy": float(-np.sum(value_counts * np.log2(value_counts + 1e-10))),
                }

        return stats

    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
        """Detect data drift using multiple statistical methods"""
        drift_results = {}
        overall_drift_score = 0.0
        significant_drifts = []

        for column in self.columns:
            if column not in data.columns or column not in self.baseline_stats:
                continue

            column_drift = self._detect_column_drift(data[column], column)
            drift_results[column] = column_drift

            if column_drift["drift_detected"]:
                significant_drifts.append(column)
                overall_drift_score += column_drift["drift_score"]

        avg_drift_score = overall_drift_score / len(self.columns) if self.columns else 0
        has_significant_drift = len(significant_drifts) > 0

        # Determine severity
        if avg_drift_score > 0.5:  # High drift
            severity = ValidationSeverity.CRITICAL
        elif avg_drift_score > 0.3:  # Moderate drift
            severity = ValidationSeverity.ERROR
        elif avg_drift_score > 0.1:  # Low drift
            severity = ValidationSeverity.WARNING
        else:
            severity = ValidationSeverity.INFO

        # Generate remediation suggestions
        remediation = []
        if has_significant_drift:
            remediation = self.remediation_rules.get("significant_drift", [])

        details = {
            "columns_tested": len(self.columns),
            "columns_with_drift": len(significant_drifts),
            "avg_drift_score": avg_drift_score,
            "drift_threshold": self.drift_threshold,
            "detection_methods": self.detection_methods,
            "column_drift_results": drift_results,
            "significant_drift_columns": significant_drifts,
        }

        return ValidationResult(
            is_valid=not has_significant_drift,
            message=f"Data drift detected in {len(significant_drifts)}/{len(self.columns)} columns (avg score: {avg_drift_score:.3f})",
            severity=severity,
            dimension=self.dimension,
            score=max(0.0, 1.0 - avg_drift_score),
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name,
        )

    def _detect_column_drift(self, current_data: pd.Series, column_name: str) -> dict[str, Any]:
        """Detect drift in a single column using multiple methods"""
        baseline_stats = self.baseline_stats[column_name]
        current_data_clean = current_data.dropna()

        drift_results = {"drift_detected": False, "drift_score": 0.0, "method_results": {}}

        if baseline_stats["type"] == "numeric":
            drift_results.update(self._detect_numeric_drift(current_data_clean, baseline_stats))
        else:
            drift_results.update(self._detect_categorical_drift(current_data_clean, baseline_stats))

        return drift_results

    def _detect_numeric_drift(
        self, current_data: pd.Series, baseline_stats: dict[str, Any]
    ) -> dict[str, Any]:
        """Detect drift in numeric columns"""
        from scipy import stats as scipy_stats

        method_results = {}
        drift_scores = []

        # Kolmogorov-Smirnov test
        if "ks_test" in self.detection_methods:
            try:
                # Generate baseline sample from statistics
                baseline_mean = baseline_stats["mean"]
                baseline_std = baseline_stats["std"]
                baseline_sample = np.random.normal(baseline_mean, baseline_std, len(current_data))

                ks_stat, ks_p_value = scipy_stats.ks_2samp(baseline_sample, current_data)
                ks_drift_score = ks_stat

                method_results["ks_test"] = {
                    "statistic": float(ks_stat),
                    "p_value": float(ks_p_value),
                    "drift_score": ks_drift_score,
                    "drift_detected": ks_p_value < 0.05,
                }

                drift_scores.append(ks_drift_score)
            except Exception as e:
                self.logger.warning(f"KS test failed: {e}")

        # Population Stability Index (PSI)
        if "psi" in self.detection_methods:
            try:
                psi_score = self._calculate_psi_numeric(current_data, baseline_stats)
                method_results["psi"] = {
                    "psi_score": psi_score,
                    "drift_score": min(psi_score / 0.2, 1.0),  # Normalize PSI
                    "drift_detected": psi_score > 0.1,
                }

                drift_scores.append(min(psi_score / 0.2, 1.0))
            except Exception as e:
                self.logger.warning(f"PSI calculation failed: {e}")

        # Jensen-Shannon Divergence
        if "js_divergence" in self.detection_methods:
            try:
                js_score = self._calculate_js_divergence_numeric(current_data, baseline_stats)
                method_results["js_divergence"] = {
                    "js_score": js_score,
                    "drift_score": js_score,
                    "drift_detected": js_score > self.drift_threshold,
                }

                drift_scores.append(js_score)
            except Exception as e:
                self.logger.warning(f"JS divergence calculation failed: {e}")

        # Statistical distance measures
        current_mean = current_data.mean()
        current_std = current_data.std()

        mean_drift = abs(current_mean - baseline_stats["mean"]) / (baseline_stats["std"] + 1e-10)
        std_drift = abs(current_std - baseline_stats["std"]) / (baseline_stats["std"] + 1e-10)

        method_results["statistical_distance"] = {
            "mean_drift": float(mean_drift),
            "std_drift": float(std_drift),
            "combined_drift": float((mean_drift + std_drift) / 2),
            "drift_detected": mean_drift > 2.0
            or std_drift > 1.0,  # 2 std devs for mean, 100% for std
        }

        drift_scores.append((mean_drift + std_drift) / 4)  # Normalize

        avg_drift_score = np.mean(drift_scores) if drift_scores else 0.0
        has_drift = any(result.get("drift_detected", False) for result in method_results.values())

        return {
            "drift_detected": has_drift,
            "drift_score": avg_drift_score,
            "method_results": method_results,
        }

    def _detect_categorical_drift(
        self, current_data: pd.Series, baseline_stats: dict[str, Any]
    ) -> dict[str, Any]:
        """Detect drift in categorical columns"""
        method_results = {}
        drift_scores = []

        # Chi-square test
        if "chi2_test" in self.detection_methods:
            try:
                current_counts = current_data.value_counts()
                baseline_categories = baseline_stats["categories"]
                baseline_frequencies = np.array(baseline_stats["frequencies"])

                # Align categories
                aligned_current = []
                aligned_baseline = []

                all_categories = set(baseline_categories + current_counts.index.tolist())

                for category in all_categories:
                    baseline_freq = (
                        baseline_frequencies[baseline_categories.index(category)]
                        if category in baseline_categories
                        else 0
                    )
                    current_freq = current_counts.get(category, 0) / len(current_data)

                    aligned_baseline.append(baseline_freq)
                    aligned_current.append(current_freq)

                aligned_baseline = np.array(aligned_baseline)
                aligned_current = np.array(aligned_current)

                # Chi-square statistic
                expected_counts = aligned_baseline * len(current_data)
                observed_counts = aligned_current * len(current_data)

                # Avoid division by zero
                expected_counts = np.maximum(expected_counts, 1)

                chi2_stat = np.sum((observed_counts - expected_counts) ** 2 / expected_counts)
                chi2_drift_score = min(chi2_stat / len(all_categories), 1.0)  # Normalize

                method_results["chi2_test"] = {
                    "chi2_statistic": float(chi2_stat),
                    "drift_score": chi2_drift_score,
                    "drift_detected": chi2_stat > len(all_categories),  # Simple threshold
                }

                drift_scores.append(chi2_drift_score)
            except Exception as e:
                self.logger.warning(f"Chi-square test failed: {e}")

        # Population Stability Index for categorical
        if "psi" in self.detection_methods:
            try:
                psi_score = self._calculate_psi_categorical(current_data, baseline_stats)
                method_results["psi"] = {
                    "psi_score": psi_score,
                    "drift_score": min(psi_score / 0.2, 1.0),
                    "drift_detected": psi_score > 0.1,
                }

                drift_scores.append(min(psi_score / 0.2, 1.0))
            except Exception as e:
                self.logger.warning(f"PSI calculation failed: {e}")

        avg_drift_score = np.mean(drift_scores) if drift_scores else 0.0
        has_drift = any(result.get("drift_detected", False) for result in method_results.values())

        return {
            "drift_detected": has_drift,
            "drift_score": avg_drift_score,
            "method_results": method_results,
        }

    def _calculate_psi_numeric(
        self, current_data: pd.Series, baseline_stats: dict[str, Any]
    ) -> float:
        """Calculate Population Stability Index for numeric data"""
        # Create bins based on baseline quartiles
        quartiles = baseline_stats["quartiles"]
        bins = [-np.inf, quartiles["q25"], quartiles["q50"], quartiles["q75"], np.inf]

        # Get baseline distribution (assume uniform in each quartile)
        baseline_dist = np.array([0.25, 0.25, 0.25, 0.25])

        # Get current distribution
        current_binned = pd.cut(current_data, bins=bins, include_lowest=True)
        current_dist = current_binned.value_counts(normalize=True, sort=False).values

        # Calculate PSI
        current_dist = np.maximum(current_dist, 1e-10)  # Avoid log(0)
        baseline_dist = np.maximum(baseline_dist, 1e-10)

        psi = np.sum((current_dist - baseline_dist) * np.log(current_dist / baseline_dist))
        return float(psi)

    def _calculate_psi_categorical(
        self, current_data: pd.Series, baseline_stats: dict[str, Any]
    ) -> float:
        """Calculate Population Stability Index for categorical data"""
        baseline_categories = baseline_stats["categories"]
        baseline_frequencies = np.array(baseline_stats["frequencies"])

        current_counts = current_data.value_counts(normalize=True)

        psi = 0.0

        for i, category in enumerate(baseline_categories):
            baseline_freq = baseline_frequencies[i]
            current_freq = current_counts.get(category, 1e-10)

            baseline_freq = max(baseline_freq, 1e-10)
            current_freq = max(current_freq, 1e-10)

            psi += (current_freq - baseline_freq) * np.log(current_freq / baseline_freq)

        return float(psi)

    def _calculate_js_divergence_numeric(
        self, current_data: pd.Series, baseline_stats: dict[str, Any]
    ) -> float:
        """Calculate Jensen-Shannon divergence for numeric data"""
        from scipy.spatial.distance import jensenshannon

        # Create histogram bins
        min_val = min(baseline_stats["min"], current_data.min())
        max_val = max(baseline_stats["max"], current_data.max())

        bins = np.linspace(min_val, max_val, 20)

        # Get baseline histogram (stored during baseline computation)
        baseline_hist = np.array(baseline_stats["histogram"])
        baseline_hist = baseline_hist / baseline_hist.sum()  # Normalize

        # Get current histogram
        current_hist, _ = np.histogram(current_data, bins=bins)
        current_hist = current_hist / current_hist.sum() if current_hist.sum() > 0 else current_hist

        # Calculate JS divergence
        js_distance = jensenshannon(baseline_hist, current_hist)
        return float(js_distance)


class AnomalyDetectionValidator(DataQualityValidator):
    """Advanced ML-based anomaly detection validator"""

    def __init__(
        self,
        columns: list[str],
        contamination: float = 0.1,
        methods: list[str] = None,
        model_params: dict[str, Any] = None,
    ):
        super().__init__("anomaly_detection", DataQualityDimension.VALIDITY)
        self.columns = columns
        self.contamination = contamination
        self.methods = methods or ["isolation_forest", "dbscan", "statistical"]
        self.model_params = model_params or {}

        # Trained models storage
        self.trained_models: dict[str, Any] = {}
        self.scalers: dict[str, StandardScaler] = {}

        self.add_remediation_rule(
            "anomalies_detected",
            [
                "Investigate anomalous records for data quality issues",
                "Review data collection processes",
                "Consider outlier treatment strategies",
                "Update anomaly detection thresholds if needed",
            ],
        )

    def validate(
        self, data: pd.DataFrame, metadata: dict[str, Any] | None = None
    ) -> ValidationResult:
        """Detect anomalies using multiple ML methods"""
        # Prepare features for anomaly detection
        feature_data = self._prepare_features(data)

        if feature_data.empty:
            return ValidationResult(
                is_valid=True,
                message="No suitable features for anomaly detection",
                severity=ValidationSeverity.INFO,
                dimension=self.dimension,
                score=1.0,
                validator_name=self.name,
            )

        anomaly_results = {}
        total_anomalies = 0
        method_scores = []

        # Run each anomaly detection method
        for method in self.methods:
            try:
                method_result = self._detect_anomalies_method(feature_data, method)
                anomaly_results[method] = method_result

                method_anomalies = method_result.get("anomaly_count", 0)
                total_anomalies = max(total_anomalies, method_anomalies)  # Take worst case

                method_score = 1.0 - (method_anomalies / len(feature_data))
                method_scores.append(method_score)

            except Exception as e:
                self.logger.warning(f"Anomaly detection method '{method}' failed: {e}")
                anomaly_results[method] = {"error": str(e)}

        # Calculate overall anomaly score
        anomaly_percentage = total_anomalies / len(feature_data) if len(feature_data) > 0 else 0
        overall_score = np.mean(method_scores) if method_scores else 1.0

        # Determine severity
        if anomaly_percentage > 0.2:  # > 20% anomalies
            severity = ValidationSeverity.CRITICAL
        elif anomaly_percentage > 0.1:  # > 10% anomalies
            severity = ValidationSeverity.ERROR
        elif anomaly_percentage > 0.05:  # > 5% anomalies
            severity = ValidationSeverity.WARNING
        else:
            severity = ValidationSeverity.INFO

        is_valid = anomaly_percentage <= self.contamination

        # Generate remediation suggestions
        remediation = []
        if total_anomalies > 0:
            remediation = self.remediation_rules.get("anomalies_detected", [])

        details = {
            "total_records": len(feature_data),
            "total_anomalies": total_anomalies,
            "anomaly_percentage": anomaly_percentage,
            "contamination_threshold": self.contamination,
            "methods_used": self.methods,
            "method_results": anomaly_results,
            "features_analyzed": list(feature_data.columns),
        }

        return ValidationResult(
            is_valid=is_valid,
            message=f"Detected {total_anomalies} anomalies ({anomaly_percentage:.2%}) using {len(self.methods)} methods",
            severity=severity,
            dimension=self.dimension,
            score=overall_score,
            details=details,
            remediation_suggestions=remediation,
            validator_name=self.name,
        )

    def _prepare_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for anomaly detection"""
        available_columns = [col for col in self.columns if col in data.columns]

        if not available_columns:
            return pd.DataFrame()

        feature_data = data[available_columns].copy()

        # Handle missing values
        feature_data = feature_data.dropna()

        # Convert categorical columns to numeric using encoding
        for col in feature_data.columns:
            if not pd.api.types.is_numeric_dtype(feature_data[col]):
                # Simple label encoding for categorical data
                feature_data[col] = pd.factorize(feature_data[col])[0]

        return feature_data

    def _detect_anomalies_method(self, data: pd.DataFrame, method: str) -> dict[str, Any]:
        """Detect anomalies using specific method"""
        if method == "isolation_forest":
            return self._isolation_forest_detection(data)
        elif method == "dbscan":
            return self._dbscan_detection(data)
        elif method == "statistical":
            return self._statistical_detection(data)
        else:
            raise ValueError(f"Unknown anomaly detection method: {method}")

    def _isolation_forest_detection(self, data: pd.DataFrame) -> dict[str, Any]:
        """Isolation Forest anomaly detection"""
        # Scale data
        scaler_key = "isolation_forest"
        if scaler_key not in self.scalers:
            self.scalers[scaler_key] = StandardScaler()

        scaled_data = self.scalers[scaler_key].fit_transform(data)

        # Train Isolation Forest
        model_params = self.model_params.get("isolation_forest", {})
        model = IsolationForest(contamination=self.contamination, random_state=42, **model_params)

        anomaly_labels = model.fit_predict(scaled_data)
        anomaly_scores = model.decision_function(scaled_data)

        # Count anomalies (-1 indicates anomaly)
        anomaly_count = np.sum(anomaly_labels == -1)

        return {
            "method": "isolation_forest",
            "anomaly_count": int(anomaly_count),
            "anomaly_percentage": anomaly_count / len(data),
            "anomaly_scores_mean": float(np.mean(anomaly_scores)),
            "anomaly_scores_std": float(np.std(anomaly_scores)),
            "model_parameters": model.get_params(),
        }

    def _dbscan_detection(self, data: pd.DataFrame) -> dict[str, Any]:
        """DBSCAN clustering-based anomaly detection"""
        # Scale data
        scaler_key = "dbscan"
        if scaler_key not in self.scalers:
            self.scalers[scaler_key] = StandardScaler()

        scaled_data = self.scalers[scaler_key].fit_transform(data)

        # Apply PCA for dimensionality reduction if needed
        if data.shape[1] > 10:
            pca = PCA(n_components=min(10, data.shape[1]))
            scaled_data = pca.fit_transform(scaled_data)

        # DBSCAN clustering
        model_params = self.model_params.get("dbscan", {})
        dbscan = DBSCAN(
            eps=model_params.get("eps", 0.5),
            min_samples=model_params.get("min_samples", 5),
            **{k: v for k, v in model_params.items() if k not in ["eps", "min_samples"]},
        )

        cluster_labels = dbscan.fit_predict(scaled_data)

        # Points labeled as -1 are considered anomalies
        anomaly_count = np.sum(cluster_labels == -1)
        n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)

        return {
            "method": "dbscan",
            "anomaly_count": int(anomaly_count),
            "anomaly_percentage": anomaly_count / len(data),
            "n_clusters": n_clusters,
            "n_noise_points": int(anomaly_count),
            "model_parameters": dbscan.get_params(),
        }

    def _statistical_detection(self, data: pd.DataFrame) -> dict[str, Any]:
        """Statistical method anomaly detection (Z-score based)"""
        anomaly_count = 0
        column_anomalies = {}

        for column in data.columns:
            col_data = data[column]

            if pd.api.types.is_numeric_dtype(col_data):
                # Z-score method
                z_scores = np.abs(stats.zscore(col_data))
                threshold = self.model_params.get("z_threshold", 3.0)
                col_anomalies = np.sum(z_scores > threshold)

                column_anomalies[column] = {
                    "anomalies": int(col_anomalies),
                    "threshold": threshold,
                    "max_z_score": float(np.max(z_scores)),
                }

                anomaly_count += col_anomalies

        # Remove duplicate counts (a record might be anomalous in multiple columns)
        # For simplicity, take the maximum anomalies across columns
        if column_anomalies:
            max_col_anomalies = max(result["anomalies"] for result in column_anomalies.values())
            anomaly_count = max_col_anomalies

        return {
            "method": "statistical",
            "anomaly_count": int(anomaly_count),
            "anomaly_percentage": anomaly_count / len(data),
            "column_results": column_anomalies,
            "threshold_used": self.model_params.get("z_threshold", 3.0),
        }
