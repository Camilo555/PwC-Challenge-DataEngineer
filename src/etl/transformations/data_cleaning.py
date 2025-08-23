"""
Data Cleaning Transformations
Advanced data cleaning, validation, and quality improvement operations.
"""
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .base import DataFrameTransformationStrategy, TransformationResult


class ValidationRule:
    """Rule for data validation."""

    def __init__(self,
                 name: str,
                 condition: Callable[[Any], bool],
                 message: str,
                 severity: str = 'error'):
        self.name = name
        self.condition = condition
        self.message = message
        self.severity = severity  # 'error', 'warning', 'info'

    def validate(self, value: Any) -> bool:
        """Apply validation rule to a value."""
        try:
            return self.condition(value)
        except Exception:
            return False


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: list[str]
    warnings: list[str]
    info: list[str]


class DataCleaner(DataFrameTransformationStrategy):
    """Comprehensive data cleaning transformation."""

    def __init__(self):
        self.validation_rules: list[ValidationRule] = []

    def add_validation_rule(self, rule: ValidationRule):
        """Add a validation rule."""
        self.validation_rules.append(rule)

    def transform(self, data: pd.DataFrame, **kwargs) -> TransformationResult:
        """Clean data according to configured rules and options."""
        cleaned_data = data.copy()
        metadata = {
            'original_shape': data.shape,
            'cleaning_steps': [],
            'validation_results': {}
        }

        # Step 1: Handle duplicates
        if kwargs.get('remove_duplicates', True):
            duplicates_before = len(cleaned_data)
            cleaned_data = cleaned_data.drop_duplicates()
            duplicates_removed = duplicates_before - len(cleaned_data)
            metadata['cleaning_steps'].append(f'Removed {duplicates_removed} duplicates')

        # Step 2: Handle missing values
        null_strategy = kwargs.get('null_strategy', 'drop')
        if null_strategy == 'drop':
            null_threshold = kwargs.get('null_threshold', 0.5)
            # Drop columns with more than threshold % nulls
            null_ratios = cleaned_data.isnull().sum() / len(cleaned_data)
            columns_to_drop = null_ratios[null_ratios > null_threshold].index.tolist()
            if columns_to_drop:
                cleaned_data = cleaned_data.drop(columns=columns_to_drop)
                metadata['cleaning_steps'].append(f'Dropped columns: {columns_to_drop}')

            # Drop rows with any nulls in remaining columns
            rows_before = len(cleaned_data)
            cleaned_data = cleaned_data.dropna()
            rows_dropped = rows_before - len(cleaned_data)
            metadata['cleaning_steps'].append(f'Dropped {rows_dropped} rows with nulls')

        elif null_strategy == 'fill':
            fill_values = kwargs.get('fill_values', {})
            if fill_values:
                cleaned_data = cleaned_data.fillna(fill_values)
                metadata['cleaning_steps'].append(f'Filled nulls with: {fill_values}')
            else:
                # Use forward fill and backward fill
                cleaned_data = cleaned_data.fillna(method='ffill').fillna(method='bfill')
                metadata['cleaning_steps'].append('Applied forward/backward fill')

        # Step 3: Handle outliers
        if kwargs.get('remove_outliers', False):
            numeric_cols = cleaned_data.select_dtypes(include=[np.number]).columns
            outlier_method = kwargs.get('outlier_method', 'iqr')

            if outlier_method == 'iqr':
                for col in numeric_cols:
                    Q1 = cleaned_data[col].quantile(0.25)
                    Q3 = cleaned_data[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR

                    outliers_before = len(cleaned_data)
                    cleaned_data = cleaned_data[
                        (cleaned_data[col] >= lower_bound) &
                        (cleaned_data[col] <= upper_bound)
                    ]
                    outliers_removed = outliers_before - len(cleaned_data)
                    if outliers_removed > 0:
                        metadata['cleaning_steps'].append(
                            f'Removed {outliers_removed} outliers from {col}'
                        )

        # Step 4: Data type conversions
        dtype_conversions = kwargs.get('dtype_conversions', {})
        for col, dtype in dtype_conversions.items():
            if col in cleaned_data.columns:
                try:
                    cleaned_data[col] = cleaned_data[col].astype(dtype)
                    metadata['cleaning_steps'].append(f'Converted {col} to {dtype}')
                except Exception as e:
                    metadata['cleaning_steps'].append(f'Failed to convert {col}: {str(e)}')

        # Step 5: Apply validation rules
        if self.validation_rules:
            validation_results = self._validate_data(cleaned_data)
            metadata['validation_results'] = validation_results

        metadata['final_shape'] = cleaned_data.shape

        return TransformationResult(cleaned_data, metadata)

    def _validate_data(self, data: pd.DataFrame) -> dict[str, ValidationResult]:
        """Apply all validation rules to the data."""
        results = {}

        for col in data.columns:
            errors = []
            warnings = []
            info = []

            for rule in self.validation_rules:
                valid_values = data[col].apply(rule.validate)
                invalid_count = (~valid_values).sum()

                if invalid_count > 0:
                    message = f"{rule.message} ({invalid_count} violations in {col})"
                    if rule.severity == 'error':
                        errors.append(message)
                    elif rule.severity == 'warning':
                        warnings.append(message)
                    else:
                        info.append(message)

            results[col] = ValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                info=info
            )

        return results


# Common validation rules
def create_common_validation_rules() -> list[ValidationRule]:
    """Create common validation rules."""
    return [
        ValidationRule(
            'not_null',
            lambda x: pd.notna(x),
            'Value cannot be null',
            'error'
        ),
        ValidationRule(
            'positive_number',
            lambda x: pd.notna(x) and (not isinstance(x, (int, float)) or x > 0),
            'Number must be positive',
            'warning'
        ),
        ValidationRule(
            'valid_email',
            lambda x: pd.notna(x) and (not isinstance(x, str) or '@' in str(x)),
            'Invalid email format',
            'warning'
        )
    ]
