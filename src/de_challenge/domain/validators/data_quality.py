"""Data quality validation and metrics."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Any, Optional, Tuple, Set
from collections import defaultdict
import statistics

from ...core.logging import get_logger
from ...core.exceptions import DataQualityException

logger = get_logger(__name__)


class QualityDimension(str, Enum):
    """Data quality dimensions."""

    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"


@dataclass
class QualityThreshold:
    """Quality thresholds for validation."""

    min_completeness: float = 0.95  # 95% complete
    min_accuracy: float = 0.98      # 98% accurate
    min_consistency: float = 0.99   # 99% consistent
    max_duplicates: float = 0.01    # 1% duplicates allowed
    max_outliers: float = 0.05      # 5% outliers allowed
    max_missing: float = 0.05       # 5% missing allowed


@dataclass
class ValidationResult:
    """Result of a validation check."""

    dimension: QualityDimension
    metric_name: str
    value: float
    threshold: float
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dimension": self.dimension.value,
            "metric": self.metric_name,
            "value": self.value,
            "threshold": self.threshold,
            "passed": self.passed,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class QualityMetrics:
    """Container for quality metrics."""

    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0

    # Completeness
    missing_values: Dict[str, int] = field(default_factory=dict)
    completeness_rate: float = 0.0

    # Accuracy
    format_errors: Dict[str, int] = field(default_factory=dict)
    range_errors: Dict[str, int] = field(default_factory=dict)
    accuracy_rate: float = 0.0

    # Consistency
    inconsistent_records: int = 0
    consistency_rate: float = 0.0

    # Uniqueness
    duplicate_records: int = 0
    duplicate_keys: Set[str] = field(default_factory=set)
    uniqueness_rate: float = 0.0

    # Timeliness
    future_dates: int = 0
    old_dates: int = 0
    timeliness_rate: float = 0.0

    # Statistical
    outliers: Dict[str, List[Any]] = field(default_factory=dict)
    statistics: Dict[str, Dict[str, float]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "completeness_rate": f"{self.completeness_rate:.2%}",
            "accuracy_rate": f"{self.accuracy_rate:.2%}",
            "consistency_rate": f"{self.consistency_rate:.2%}",
            "uniqueness_rate": f"{self.uniqueness_rate:.2%}",
            "timeliness_rate": f"{self.timeliness_rate:.2%}",
            "missing_values": self.missing_values,
            "format_errors": self.format_errors,
            "duplicate_records": self.duplicate_records,
            "outliers_count": {k: len(v) for k, v in self.outliers.items()},
        }


@dataclass
class DataQualityReport:
    """Comprehensive data quality report."""

    dataset_name: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metrics: QualityMetrics = field(default_factory=QualityMetrics)
    validation_results: List[ValidationResult] = field(default_factory=list)
    thresholds: QualityThreshold = field(default_factory=QualityThreshold)
    overall_quality_score: float = 0.0
    passed: bool = False
    recommendations: List[str] = field(default_factory=list)

    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result."""
        self.validation_results.append(result)

    def calculate_overall_score(self) -> float:
        """Calculate overall quality score."""
        if not self.validation_results:
            return 0.0

        scores = []
        weights = {
            QualityDimension.COMPLETENESS: 0.25,
            QualityDimension.ACCURACY: 0.25,
            QualityDimension.CONSISTENCY: 0.20,
            QualityDimension.UNIQUENESS: 0.15,
            QualityDimension.VALIDITY: 0.10,
            QualityDimension.TIMELINESS: 0.05,
        }

        dimension_scores = defaultdict(list)
        for result in self.validation_results:
            dimension_scores[result.dimension].append(
                result.value / result.threshold if result.threshold > 0 else 0
            )

        for dimension, values in dimension_scores.items():
            avg_score = sum(values) / len(values) if values else 0
            weight = weights.get(dimension, 0.1)
            scores.append(min(1.0, avg_score) * weight)

        self.overall_quality_score = sum(scores) / sum(weights.values())
        return self.overall_quality_score

    def generate_recommendations(self) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []

        # Check failed validations
        for result in self.validation_results:
            if not result.passed:
                if result.dimension == QualityDimension.COMPLETENESS:
                    recommendations.append(
                        f"Address missing values in: {', '.join(result.details.get('fields', []))}"
                    )
                elif result.dimension == QualityDimension.ACCURACY:
                    recommendations.append(
                        f"Fix format/range errors in: {result.metric_name}"
                    )
                elif result.dimension == QualityDimension.UNIQUENESS:
                    recommendations.append(
                        f"Remove {result.details.get('duplicate_count', 0)} duplicate records"
                    )
                elif result.dimension == QualityDimension.CONSISTENCY:
                    recommendations.append(
                        "Review and fix data consistency issues"
                    )

        # General recommendations
        if self.metrics.completeness_rate < 0.95:
            recommendations.append(
                "Implement data imputation strategy for missing values"
            )

        if self.metrics.accuracy_rate < 0.98:
            recommendations.append(
                "Review data validation rules and cleansing procedures"
            )

        if self.metrics.duplicate_records > 100:
            recommendations.append(
                "Implement deduplication process in ETL pipeline"
            )

        self.recommendations = recommendations[:10]  # Limit to top 10
        return self.recommendations

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "dataset": self.dataset_name,
            "timestamp": self.timestamp.isoformat(),
            "overall_score": f"{self.overall_quality_score:.2%}",
            "passed": self.passed,
            "metrics": self.metrics.to_dict(),
            "validation_results": [r.to_dict() for r in self.validation_results],
            "recommendations": self.recommendations,
        }


class DataQualityValidator:
    """
    Comprehensive data quality validator.
    Evaluates data across multiple quality dimensions.
    """

    def __init__(self, thresholds: Optional[QualityThreshold] = None) -> None:
        """
        Initialize validator.

        Args:
            thresholds: Quality thresholds for validation
        """
        self.thresholds = thresholds or QualityThreshold()
        self.metrics = QualityMetrics()

    def validate_completeness(
        self,
        data: List[Dict[str, Any]],
        required_fields: List[str],
        optional_fields: Optional[List[str]] = None
    ) -> ValidationResult:
        """
        Validate data completeness.

        Args:
            data: List of records to validate
            required_fields: Fields that must be present
            optional_fields: Fields that are optional

        Returns:
            Validation result
        """
        if not data:
            return ValidationResult(
                dimension=QualityDimension.COMPLETENESS,
                metric_name="completeness",
                value=0.0,
                threshold=self.thresholds.min_completeness,
                passed=False,
                message="No data to validate",
            )

        total_fields = len(required_fields) * len(data)
        missing_count = 0
        missing_by_field = defaultdict(int)

        for record in data:
            for field in required_fields:
                value = record.get(field)
                if value is None or value == "" or value == "?":
                    missing_count += 1
                    missing_by_field[field] += 1

        completeness = 1 - \
            (missing_count / total_fields) if total_fields > 0 else 0

        self.metrics.missing_values = dict(missing_by_field)
        self.metrics.completeness_rate = completeness

        return ValidationResult(
            dimension=QualityDimension.COMPLETENESS,
            metric_name="completeness",
            value=completeness,
            threshold=self.thresholds.min_completeness,
            passed=completeness >= self.thresholds.min_completeness,
            message=f"Completeness: {completeness:.2%}",
            details={
                "missing_count": missing_count,
                "fields": list(missing_by_field.keys()),
            }
        )

    def validate_uniqueness(
        self,
        data: List[Dict[str, Any]],
        key_fields: List[str]
    ) -> ValidationResult:
        """
        Validate data uniqueness.

        Args:
            data: List of records to validate
            key_fields: Fields that form the unique key

        Returns:
            Validation result
        """
        if not data:
            return ValidationResult(
                dimension=QualityDimension.UNIQUENESS,
                metric_name="uniqueness",
                value=1.0,
                threshold=1 - self.thresholds.max_duplicates,
                passed=True,
                message="No data to validate",
            )

        seen_keys = set()
        duplicates = set()

        for record in data:
            key = tuple(record.get(field) for field in key_fields)
            if key in seen_keys:
                duplicates.add(key)
            seen_keys.add(key)

        duplicate_rate = len(duplicates) / len(data) if data else 0
        uniqueness = 1 - duplicate_rate

        self.metrics.duplicate_records = len(duplicates)
        self.metrics.duplicate_keys = {str(k) for k in duplicates}
        self.metrics.uniqueness_rate = uniqueness

        return ValidationResult(
            dimension=QualityDimension.UNIQUENESS,
            metric_name="uniqueness",
            value=uniqueness,
            threshold=1 - self.thresholds.max_duplicates,
            passed=duplicate_rate <= self.thresholds.max_duplicates,
            message=f"Uniqueness: {uniqueness:.2%}",
            details={
                "duplicate_count": len(duplicates),
                "duplicate_keys": list(duplicates)[:10],  # Sample
            }
        )

    def validate_accuracy(
        self,
        data: List[Dict[str, Any]],
        validation_rules: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate data accuracy against rules.

        Args:
            data: List of records to validate
            validation_rules: Dictionary of field -> validation function

        Returns:
            Validation result
        """
        if not data:
            return ValidationResult(
                dimension=QualityDimension.ACCURACY,
                metric_name="accuracy",
                value=1.0,
                threshold=self.thresholds.min_accuracy,
                passed=True,
                message="No data to validate",
            )

        total_checks = len(validation_rules) * len(data)
        errors = 0
        error_by_field = defaultdict(int)

        for record in data:
            for field, rule in validation_rules.items():
                value = record.get(field)
                try:
                    if callable(rule):
                        if not rule(value):
                            errors += 1
                            error_by_field[field] += 1
                except Exception:
                    errors += 1
                    error_by_field[field] += 1

        accuracy = 1 - (errors / total_checks) if total_checks > 0 else 0

        self.metrics.format_errors = dict(error_by_field)
        self.metrics.accuracy_rate = accuracy

        return ValidationResult(
            dimension=QualityDimension.ACCURACY,
            metric_name="accuracy",
            value=accuracy,
            threshold=self.thresholds.min_accuracy,
            passed=accuracy >= self.thresholds.min_accuracy,
            message=f"Accuracy: {accuracy:.2%}",
            details={
                "error_count": errors,
                "fields": list(error_by_field.keys()),
            }
        )

    def validate_consistency(
        self,
        data: List[Dict[str, Any]],
        consistency_checks: List[Tuple[str, str, Any]]
    ) -> ValidationResult:
        """
        Validate data consistency.

        Args:
            data: List of records to validate
            consistency_checks: List of (field1, field2, check_function)

        Returns:
            Validation result
        """
        if not data:
            return ValidationResult(
                dimension=QualityDimension.CONSISTENCY,
                metric_name="consistency",
                value=1.0,
                threshold=self.thresholds.min_consistency,
                passed=True,
                message="No data to validate",
            )

        inconsistent = 0

        for record in data:
            for field1, field2, check_func in consistency_checks:
                val1 = record.get(field1)
                val2 = record.get(field2)

                try:
                    if not check_func(val1, val2):
                        inconsistent += 1
                        break  # One inconsistency per record
                except Exception:
                    inconsistent += 1
                    break

        consistency = 1 - (inconsistent / len(data)) if data else 0

        self.metrics.inconsistent_records = inconsistent
        self.metrics.consistency_rate = consistency

        return ValidationResult(
            dimension=QualityDimension.CONSISTENCY,
            metric_name="consistency",
            value=consistency,
            threshold=self.thresholds.min_consistency,
            passed=consistency >= self.thresholds.min_consistency,
            message=f"Consistency: {consistency:.2%}",
            details={
                "inconsistent_count": inconsistent,
            }
        )

    def validate_timeliness(
        self,
        data: List[Dict[str, Any]],
        date_field: str,
        max_age_days: int = 730  # 2 years
    ) -> ValidationResult:
        """
        Validate data timeliness.

        Args:
            data: List of records to validate
            date_field: Field containing date
            max_age_days: Maximum age in days

        Returns:
            Validation result
        """
        if not data:
            return ValidationResult(
                dimension=QualityDimension.TIMELINESS,
                metric_name="timeliness",
                value=1.0,
                threshold=0.95,
                passed=True,
                message="No data to validate",
            )

        now = datetime.utcnow()
        timely = 0
        future = 0
        old = 0

        for record in data:
            date_val = record.get(date_field)
            if isinstance(date_val, (datetime, str)):
                if isinstance(date_val, str):
                    try:
                        date_val = datetime.fromisoformat(date_val)
                    except:
                        continue

                if date_val > now:
                    future += 1
                elif (now - date_val).days > max_age_days:
                    old += 1
                else:
                    timely += 1

        timeliness = timely / len(data) if data else 0

        self.metrics.future_dates = future
        self.metrics.old_dates = old
        self.metrics.timeliness_rate = timeliness

        return ValidationResult(
            dimension=QualityDimension.TIMELINESS,
            metric_name="timeliness",
            value=timeliness,
            threshold=0.95,
            passed=timeliness >= 0.95,
            message=f"Timeliness: {timeliness:.2%}",
            details={
                "future_dates": future,
                "old_dates": old,
            }
        )

    def detect_outliers(
        self,
        data: List[Dict[str, Any]],
        numeric_fields: List[str],
        z_threshold: float = 3.0
    ) -> Dict[str, List[Any]]:
        """
        Detect outliers using Z-score method.

        Args:
            data: List of records
            numeric_fields: Numeric fields to check
            z_threshold: Z-score threshold for outliers

        Returns:
            Dictionary of field -> list of outlier values
        """
        outliers = defaultdict(list)

        for field in numeric_fields:
            values = []
            for record in data:
                val = record.get(field)
                if isinstance(val, (int, float, Decimal)):
                    values.append(float(val))

            if len(values) > 2:
                mean = statistics.mean(values)
                stdev = statistics.stdev(values)

                if stdev > 0:
                    for val in values:
                        z_score = abs((val - mean) / stdev)
                        if z_score > z_threshold:
                            outliers[field].append(val)

        self.metrics.outliers = dict(outliers)
        return dict(outliers)

    def generate_report(
        self,
        dataset_name: str,
        data: List[Dict[str, Any]],
        required_fields: List[str],
        key_fields: List[str],
        validation_rules: Optional[Dict[str, Any]] = None,
        consistency_checks: Optional[List[Tuple[str, str, Any]]] = None
    ) -> DataQualityReport:
        """
        Generate comprehensive data quality report.

        Args:
            dataset_name: Name of the dataset
            data: Data to validate
            required_fields: Required fields
            key_fields: Key fields for uniqueness
            validation_rules: Accuracy validation rules
            consistency_checks: Consistency checks

        Returns:
            Complete data quality report
        """
        report = DataQualityReport(
            dataset_name=dataset_name,
            thresholds=self.thresholds,
        )

        # Update metrics
        self.metrics.total_records = len(data)

        # Run validations
        results = []

        # Completeness
        results.append(
            self.validate_completeness(data, required_fields)
        )

        # Uniqueness
        results.append(
            self.validate_uniqueness(data, key_fields)
        )

        # Accuracy
        if validation_rules:
            results.append(
                self.validate_accuracy(data, validation_rules)
            )

        # Consistency
        if consistency_checks:
            results.append(
                self.validate_consistency(data, consistency_checks)
            )

        # Add all results
        for result in results:
            report.add_result(result)

        # Calculate metrics
        report.metrics = self.metrics
        report.metrics.valid_records = sum(
            1 for r in results if r.passed
        ) * len(data) // len(results)
        report.metrics.invalid_records = (
            report.metrics.total_records - report.metrics.valid_records
        )

        # Calculate overall score
        report.calculate_overall_score()
        report.passed = report.overall_quality_score >= 0.9

        # Generate recommendations
        report.generate_recommendations()

        return report
