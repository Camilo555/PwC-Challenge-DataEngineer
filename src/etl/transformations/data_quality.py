"""
Advanced Data Quality and Validation Framework
Provides comprehensive data quality checks, profiling, and automated remediation
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

from core.logging import get_logger

logger = get_logger(__name__)


class QualityIssueType(Enum):
    """Types of data quality issues."""
    MISSING_VALUES = "missing_values"
    DUPLICATES = "duplicates"
    OUTLIERS = "outliers"
    INVALID_FORMAT = "invalid_format"
    BUSINESS_RULE_VIOLATION = "business_rule_violation"
    INCONSISTENT_DATA = "inconsistent_data"
    REFERENTIAL_INTEGRITY = "referential_integrity"


@dataclass
class QualityIssue:
    """Represents a data quality issue."""
    type: QualityIssueType
    column: str
    description: str
    severity: str  # 'critical', 'high', 'medium', 'low'
    count: int
    percentage: float
    sample_values: list[Any]
    recommendation: str


@dataclass
class DataProfile:
    """Comprehensive data profile."""
    total_rows: int
    total_columns: int
    missing_values_count: int
    missing_values_percentage: float
    duplicate_rows: int
    duplicate_percentage: float
    column_profiles: dict[str, dict[str, Any]]
    quality_score: float
    issues: list[QualityIssue]


class DataQualityValidator:
    """Advanced data quality validation and profiling."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or self._get_default_config()
        self.quality_issues = []

    def _get_default_config(self) -> dict[str, Any]:
        """Get default quality validation configuration."""
        return {
            'missing_value_threshold': 0.1,  # 10%
            'duplicate_threshold': 0.05,     # 5%
            'outlier_method': 'iqr',         # 'iqr' or 'zscore'
            'outlier_threshold': 1.5,
            'business_rules': {
                'quantity': {'min': 0, 'max': 10000},
                'unit_price': {'min': 0, 'max': 1000},
                'customer_id': {'not_null': True},
                'invoice_no': {'not_null': True, 'pattern': r'^[A-Z0-9]+$'}
            }
        }

    def profile_data(self, df: pd.DataFrame) -> DataProfile:
        """Generate comprehensive data profile."""
        logger.info("Generating data quality profile...")

        # Basic statistics
        total_rows = len(df)
        total_columns = len(df.columns)
        missing_values_count = df.isnull().sum().sum()
        missing_values_percentage = (missing_values_count / (total_rows * total_columns)) * 100

        # Duplicate analysis
        duplicate_rows = df.duplicated().sum()
        duplicate_percentage = (duplicate_rows / total_rows) * 100

        # Column-level profiling
        column_profiles = {}
        for col in df.columns:
            column_profiles[col] = self._profile_column(df[col])

        # Validate data quality
        self.quality_issues = []
        self._validate_missing_values(df)
        self._validate_duplicates(df)
        self._validate_outliers(df)
        self._validate_business_rules(df)
        self._validate_data_consistency(df)

        # Calculate quality score
        quality_score = self._calculate_quality_score(df)

        return DataProfile(
            total_rows=total_rows,
            total_columns=total_columns,
            missing_values_count=missing_values_count,
            missing_values_percentage=missing_values_percentage,
            duplicate_rows=duplicate_rows,
            duplicate_percentage=duplicate_percentage,
            column_profiles=column_profiles,
            quality_score=quality_score,
            issues=self.quality_issues
        )

    def _profile_column(self, series: pd.Series) -> dict[str, Any]:
        """Profile individual column."""
        profile = {
            'dtype': str(series.dtype),
            'missing_count': series.isnull().sum(),
            'missing_percentage': (series.isnull().sum() / len(series)) * 100,
            'unique_count': series.nunique(),
            'unique_percentage': (series.nunique() / len(series)) * 100,
        }

        if series.dtype in ['int64', 'float64']:
            profile.update({
                'min': series.min(),
                'max': series.max(),
                'mean': series.mean(),
                'median': series.median(),
                'std': series.std(),
                'skewness': series.skew(),
                'kurtosis': series.kurtosis()
            })

        if series.dtype == 'object':
            profile.update({
                'avg_length': series.astype(str).str.len().mean(),
                'most_common': series.value_counts().head(5).to_dict()
            })

        if pd.api.types.is_datetime64_any_dtype(series):
            profile.update({
                'min_date': series.min(),
                'max_date': series.max(),
                'date_range_days': (series.max() - series.min()).days
            })

        return profile

    def _validate_missing_values(self, df: pd.DataFrame):
        """Validate missing values."""
        threshold = self.config['missing_value_threshold']

        for col in df.columns:
            missing_count = df[col].isnull().sum()
            missing_percentage = (missing_count / len(df)) * 100

            if missing_percentage > threshold * 100:
                severity = 'critical' if missing_percentage > 50 else 'high'
                self.quality_issues.append(QualityIssue(
                    type=QualityIssueType.MISSING_VALUES,
                    column=col,
                    description=f"High missing value rate: {missing_percentage:.2f}%",
                    severity=severity,
                    count=missing_count,
                    percentage=missing_percentage,
                    sample_values=[],
                    recommendation=f"Consider imputation or removal of column {col}"
                ))

    def _validate_duplicates(self, df: pd.DataFrame):
        """Validate duplicate records."""
        threshold = self.config['duplicate_threshold']

        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df)) * 100

        if duplicate_percentage > threshold * 100:
            severity = 'high' if duplicate_percentage > 10 else 'medium'
            sample_duplicates = df[df.duplicated()].head(3).to_dict('records')

            self.quality_issues.append(QualityIssue(
                type=QualityIssueType.DUPLICATES,
                column='all',
                description=f"High duplicate rate: {duplicate_percentage:.2f}%",
                severity=severity,
                count=duplicate_count,
                percentage=duplicate_percentage,
                sample_values=sample_duplicates,
                recommendation="Review and remove duplicate records"
            ))

    def _validate_outliers(self, df: pd.DataFrame):
        """Validate outliers in numeric columns."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            outliers = self._detect_outliers(df[col])
            outlier_count = len(outliers)
            outlier_percentage = (outlier_count / len(df)) * 100

            if outlier_percentage > 5:  # More than 5% outliers
                severity = 'medium' if outlier_percentage < 15 else 'high'
                sample_outliers = outliers.head(5).tolist()

                self.quality_issues.append(QualityIssue(
                    type=QualityIssueType.OUTLIERS,
                    column=col,
                    description=f"High outlier rate: {outlier_percentage:.2f}%",
                    severity=severity,
                    count=outlier_count,
                    percentage=outlier_percentage,
                    sample_values=sample_outliers,
                    recommendation=f"Review outliers in {col} - may indicate data entry errors"
                ))

    def _detect_outliers(self, series: pd.Series) -> pd.Series:
        """Detect outliers using IQR method."""
        if self.config['outlier_method'] == 'iqr':
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            threshold = self.config['outlier_threshold']

            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR

            return series[(series < lower_bound) | (series > upper_bound)]

        elif self.config['outlier_method'] == 'zscore':
            z_scores = np.abs((series - series.mean()) / series.std())
            threshold = self.config['outlier_threshold']
            return series[z_scores > threshold]

        return pd.Series([])

    def _validate_business_rules(self, df: pd.DataFrame):
        """Validate business rules."""
        business_rules = self.config.get('business_rules', {})

        for col, rules in business_rules.items():
            if col not in df.columns:
                continue

            # Null value validation
            if rules.get('not_null', False):
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    self.quality_issues.append(QualityIssue(
                        type=QualityIssueType.BUSINESS_RULE_VIOLATION,
                        column=col,
                        description=f"Column {col} should not have null values",
                        severity='critical',
                        count=null_count,
                        percentage=(null_count / len(df)) * 100,
                        sample_values=[],
                        recommendation=f"Ensure {col} is always populated"
                    ))

            # Numeric range validation
            if 'min' in rules and col in df.select_dtypes(include=[np.number]).columns:
                invalid_count = (df[col] < rules['min']).sum()
                if invalid_count > 0:
                    self.quality_issues.append(QualityIssue(
                        type=QualityIssueType.BUSINESS_RULE_VIOLATION,
                        column=col,
                        description=f"Values in {col} below minimum {rules['min']}",
                        severity='high',
                        count=invalid_count,
                        percentage=(invalid_count / len(df)) * 100,
                        sample_values=df[df[col] < rules['min']][col].head(5).tolist(),
                        recommendation=f"Review values in {col} below {rules['min']}"
                    ))

            if 'max' in rules and col in df.select_dtypes(include=[np.number]).columns:
                invalid_count = (df[col] > rules['max']).sum()
                if invalid_count > 0:
                    self.quality_issues.append(QualityIssue(
                        type=QualityIssueType.BUSINESS_RULE_VIOLATION,
                        column=col,
                        description=f"Values in {col} above maximum {rules['max']}",
                        severity='high',
                        count=invalid_count,
                        percentage=(invalid_count / len(df)) * 100,
                        sample_values=df[df[col] > rules['max']][col].head(5).tolist(),
                        recommendation=f"Review values in {col} above {rules['max']}"
                    ))

            # Pattern validation
            if 'pattern' in rules and col in df.select_dtypes(include=['object']).columns:
                import re
                pattern = re.compile(rules['pattern'])
                invalid_mask = ~df[col].astype(str).str.match(pattern, na=False)
                invalid_count = invalid_mask.sum()

                if invalid_count > 0:
                    self.quality_issues.append(QualityIssue(
                        type=QualityIssueType.BUSINESS_RULE_VIOLATION,
                        column=col,
                        description=f"Values in {col} don't match pattern {rules['pattern']}",
                        severity='medium',
                        count=invalid_count,
                        percentage=(invalid_count / len(df)) * 100,
                        sample_values=df[invalid_mask][col].head(5).tolist(),
                        recommendation=f"Standardize format for {col}"
                    ))

    def _validate_data_consistency(self, df: pd.DataFrame):
        """Validate data consistency across columns."""
        # Example: Check if invoice_timestamp is consistent with derived date fields
        if 'invoice_timestamp' in df.columns and 'year' in df.columns:
            df_temp = df.copy()
            df_temp['extracted_year'] = pd.to_datetime(df_temp['invoice_timestamp']).dt.year
            inconsistent = df_temp['year'] != df_temp['extracted_year']
            inconsistent_count = inconsistent.sum()

            if inconsistent_count > 0:
                self.quality_issues.append(QualityIssue(
                    type=QualityIssueType.INCONSISTENT_DATA,
                    column='year',
                    description="Year field inconsistent with invoice_timestamp",
                    severity='high',
                    count=inconsistent_count,
                    percentage=(inconsistent_count / len(df)) * 100,
                    sample_values=[],
                    recommendation="Recalculate derived date fields from source timestamp"
                ))

    def _calculate_quality_score(self, df: pd.DataFrame) -> float:
        """Calculate overall data quality score (0-100)."""
        base_score = 100.0

        # Deduct points for each issue type
        deductions = {
            QualityIssueType.MISSING_VALUES: 20,
            QualityIssueType.DUPLICATES: 15,
            QualityIssueType.OUTLIERS: 10,
            QualityIssueType.BUSINESS_RULE_VIOLATION: 25,
            QualityIssueType.INCONSISTENT_DATA: 20,
            QualityIssueType.REFERENTIAL_INTEGRITY: 15
        }

        severity_multipliers = {
            'critical': 1.0,
            'high': 0.7,
            'medium': 0.4,
            'low': 0.2
        }

        for issue in self.quality_issues:
            deduction = deductions.get(issue.type, 5)
            multiplier = severity_multipliers.get(issue.severity, 0.5)

            # Scale deduction by percentage affected
            scaled_deduction = deduction * multiplier * (issue.percentage / 100)
            base_score -= min(scaled_deduction, deduction)  # Cap at max deduction

        return max(0.0, min(100.0, base_score))

    def auto_remediate(self, df: pd.DataFrame, apply_fixes: bool = False) -> pd.DataFrame:
        """Automatically remediate common data quality issues."""
        logger.info("Performing automatic data quality remediation...")

        df_clean = df.copy()
        remediation_log = []

        # Remove exact duplicates
        if df_clean.duplicated().any():
            original_len = len(df_clean)
            df_clean = df_clean.drop_duplicates()
            removed = original_len - len(df_clean)
            remediation_log.append(f"Removed {removed} duplicate rows")

        # Handle missing values
        for col in df_clean.columns:
            missing_count = df_clean[col].isnull().sum()
            if missing_count > 0:
                if df_clean[col].dtype in ['int64', 'float64']:
                    # Fill numeric columns with median
                    df_clean[col] = df_clean[col].fillna(df_clean[col].median())
                    remediation_log.append(f"Filled {missing_count} missing values in {col} with median")
                elif df_clean[col].dtype == 'object':
                    # Fill categorical columns with mode
                    mode_value = df_clean[col].mode()
                    if not mode_value.empty:
                        df_clean[col] = df_clean[col].fillna(mode_value[0])
                        remediation_log.append(f"Filled {missing_count} missing values in {col} with mode")

        # Cap outliers (for numeric columns)
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            Q1 = df_clean[col].quantile(0.01)
            Q99 = df_clean[col].quantile(0.99)

            outliers_low = (df_clean[col] < Q1).sum()
            outliers_high = (df_clean[col] > Q99).sum()

            if outliers_low > 0 or outliers_high > 0:
                df_clean[col] = df_clean[col].clip(lower=Q1, upper=Q99)
                remediation_log.append(f"Capped {outliers_low + outliers_high} outliers in {col}")

        # Log remediation actions
        if remediation_log:
            logger.info("Data quality remediation completed:")
            for action in remediation_log:
                logger.info(f"  - {action}")

        return df_clean if apply_fixes else df

    def generate_quality_report(self, profile: DataProfile) -> dict[str, Any]:
        """Generate comprehensive quality report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_rows': profile.total_rows,
                'total_columns': profile.total_columns,
                'quality_score': profile.quality_score,
                'overall_status': self._get_quality_status(profile.quality_score)
            },
            'missing_data': {
                'total_missing_values': profile.missing_values_count,
                'missing_percentage': profile.missing_values_percentage
            },
            'duplicates': {
                'duplicate_rows': profile.duplicate_rows,
                'duplicate_percentage': profile.duplicate_percentage
            },
            'issues': [
                {
                    'type': issue.type.value,
                    'column': issue.column,
                    'description': issue.description,
                    'severity': issue.severity,
                    'count': issue.count,
                    'percentage': issue.percentage,
                    'recommendation': issue.recommendation
                }
                for issue in profile.issues
            ],
            'column_profiles': profile.column_profiles
        }

        return report

    def _get_quality_status(self, score: float) -> str:
        """Get quality status based on score."""
        if score >= 90:
            return 'Excellent'
        elif score >= 80:
            return 'Good'
        elif score >= 70:
            return 'Fair'
        elif score >= 60:
            return 'Poor'
        else:
            return 'Critical'


def validate_data_quality(df: pd.DataFrame, config: dict[str, Any] | None = None) -> DataProfile:
    """Main function to validate data quality."""
    validator = DataQualityValidator(config)
    return validator.profile_data(df)


def auto_clean_data(df: pd.DataFrame, apply_fixes: bool = True) -> pd.DataFrame:
    """Main function to automatically clean data."""
    validator = DataQualityValidator()
    return validator.auto_remediate(df, apply_fixes)


def main():
    """Test the data quality module."""
    print("Data Quality Validation Module loaded successfully")
    print("Available features:")
    print("- Comprehensive data profiling")
    print("- Missing value detection and remediation")
    print("- Duplicate detection")
    print("- Outlier detection (IQR and Z-score methods)")
    print("- Business rule validation")
    print("- Data consistency checks")
    print("- Automatic data cleaning")
    print("- Quality scoring (0-100)")
    print("- Detailed quality reports")


if __name__ == "__main__":
    main()
