"""
Slowly Changing Dimension Type 2 (SCD2) Processor
Handles historical tracking of dimension changes with effective dating.
"""
from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from .base import DataFrameTransformationStrategy, TransformationResult


@dataclass
class SCD2Config:
    """Configuration for SCD2 processing."""
    business_key_columns: list[str]
    tracked_columns: list[str]
    effective_date_column: str = 'effective_date'
    end_date_column: str = 'end_date'
    is_current_column: str = 'is_current'
    default_end_date: str = '9999-12-31'


class SCD2Processor(DataFrameTransformationStrategy):
    """Processor for SCD Type 2 transformations."""

    def __init__(self, config: SCD2Config):
        self.config = config

    def transform(self, data: pd.DataFrame, **kwargs) -> TransformationResult:
        """
        Apply SCD2 transformation to input data.
        
        Args:
            data: New data to process
            existing_data: Optional existing dimension data
            effective_date: Date for new records (defaults to today)
        """
        existing_data = kwargs.get('existing_data')
        effective_date = kwargs.get('effective_date', datetime.now().date())

        if existing_data is None or existing_data.empty:
            # First load - create initial records
            return self._create_initial_records(data, effective_date)
        else:
            # Incremental load - detect changes and apply SCD2
            return self._process_changes(data, existing_data, effective_date)

    def _create_initial_records(self, data: pd.DataFrame, effective_date: date) -> TransformationResult:
        """Create initial SCD2 records from new data."""
        result_data = data.copy()

        # Add SCD2 columns
        result_data[self.config.effective_date_column] = effective_date
        result_data[self.config.end_date_column] = pd.to_datetime(self.config.default_end_date).date()
        result_data[self.config.is_current_column] = True

        metadata = {
            'operation': 'initial_load',
            'records_processed': len(result_data),
            'new_records': len(result_data),
            'changed_records': 0,
            'unchanged_records': 0
        }

        return TransformationResult(result_data, metadata)

    def _process_changes(self,
                        new_data: pd.DataFrame,
                        existing_data: pd.DataFrame,
                        effective_date: date) -> TransformationResult:
        """Process changes using SCD2 logic."""

        # Get current records only
        current_data = existing_data[existing_data[self.config.is_current_column] == True].copy()

        # Merge on business keys to identify changes
        merged = pd.merge(
            new_data,
            current_data,
            on=self.config.business_key_columns,
            how='outer',
            suffixes=('_new', '_existing'),
            indicator=True
        )

        # Identify different types of changes
        new_records = merged[merged['_merge'] == 'left_only'].copy()
        unchanged_records = merged[merged['_merge'] == 'both'].copy()
        deleted_records = merged[merged['_merge'] == 'right_only'].copy()

        # Check for actual changes in tracked columns
        changed_records = []
        truly_unchanged = []

        for _, row in unchanged_records.iterrows():
            has_changes = False
            for col in self.config.tracked_columns:
                new_val = row.get(f'{col}_new')
                existing_val = row.get(f'{col}_existing')

                if pd.isna(new_val) and pd.isna(existing_val):
                    continue
                elif pd.isna(new_val) or pd.isna(existing_val):
                    has_changes = True
                    break
                elif new_val != existing_val:
                    has_changes = True
                    break

            if has_changes:
                changed_records.append(row)
            else:
                truly_unchanged.append(row)

        # Build result dataset
        result_records = []

        # Keep all historical records
        historical_records = existing_data[existing_data[self.config.is_current_column] == False]
        if not historical_records.empty:
            result_records.append(historical_records)

        # Process new records
        if not new_records.empty:
            new_final = self._prepare_new_records(new_records, effective_date)
            result_records.append(new_final)

        # Process unchanged records (keep as current)
        if truly_unchanged:
            unchanged_df = pd.DataFrame(truly_unchanged)
            unchanged_final = self._prepare_unchanged_records(unchanged_df)
            result_records.append(unchanged_final)

        # Process changed records (close old, create new)
        if changed_records:
            changed_df = pd.DataFrame(changed_records)
            old_versions, new_versions = self._process_changed_records(changed_df, effective_date)
            result_records.extend([old_versions, new_versions])

        # Process deleted records (close them)
        if not deleted_records.empty:
            deleted_final = self._process_deleted_records(deleted_records, effective_date)
            result_records.append(deleted_final)

        # Combine all records
        if result_records:
            result_data = pd.concat(result_records, ignore_index=True)
        else:
            result_data = existing_data.copy()

        metadata = {
            'operation': 'scd2_update',
            'records_processed': len(new_data),
            'new_records': len(new_records),
            'changed_records': len(changed_records),
            'unchanged_records': len(truly_unchanged),
            'deleted_records': len(deleted_records),
            'total_result_records': len(result_data)
        }

        return TransformationResult(result_data, metadata)

    def _prepare_new_records(self, new_records: pd.DataFrame, effective_date: date) -> pd.DataFrame:
        """Prepare new records with SCD2 columns."""
        # Remove merge artifacts and _new suffixes
        columns_to_keep = []
        for col in new_records.columns:
            if col.endswith('_new'):
                columns_to_keep.append(col)
            elif not col.endswith('_existing') and col != '_merge':
                columns_to_keep.append(col)

        result = new_records[columns_to_keep].copy()

        # Rename _new columns
        rename_dict = {col: col.replace('_new', '') for col in result.columns if col.endswith('_new')}
        result = result.rename(columns=rename_dict)

        # Add SCD2 columns
        result[self.config.effective_date_column] = effective_date
        result[self.config.end_date_column] = pd.to_datetime(self.config.default_end_date).date()
        result[self.config.is_current_column] = True

        return result

    def _prepare_unchanged_records(self, unchanged_records: pd.DataFrame) -> pd.DataFrame:
        """Prepare unchanged records (keep existing values)."""
        # Use existing values
        result = pd.DataFrame()

        for col in unchanged_records.columns:
            if col.endswith('_existing'):
                base_col = col.replace('_existing', '')
                result[base_col] = unchanged_records[col]
            elif not col.endswith('_new') and col != '_merge':
                result[col] = unchanged_records[col]

        return result

    def _process_changed_records(self,
                                changed_records: pd.DataFrame,
                                effective_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Process changed records: close old versions and create new ones."""

        # Create closed versions of existing records
        old_versions = pd.DataFrame()
        for col in changed_records.columns:
            if col.endswith('_existing'):
                base_col = col.replace('_existing', '')
                old_versions[base_col] = changed_records[col]
            elif not col.endswith('_new') and col != '_merge':
                old_versions[col] = changed_records[col]

        # Close the old versions
        old_versions[self.config.end_date_column] = effective_date
        old_versions[self.config.is_current_column] = False

        # Create new versions
        new_versions = pd.DataFrame()
        for col in changed_records.columns:
            if col.endswith('_new'):
                base_col = col.replace('_new', '')
                new_versions[base_col] = changed_records[col]
            elif col in self.config.business_key_columns:
                new_versions[col] = changed_records[col]

        # Add SCD2 columns to new versions
        new_versions[self.config.effective_date_column] = effective_date
        new_versions[self.config.end_date_column] = pd.to_datetime(self.config.default_end_date).date()
        new_versions[self.config.is_current_column] = True

        return old_versions, new_versions

    def _process_deleted_records(self,
                                deleted_records: pd.DataFrame,
                                effective_date: date) -> pd.DataFrame:
        """Process deleted records by closing them."""
        result = pd.DataFrame()

        for col in deleted_records.columns:
            if col.endswith('_existing'):
                base_col = col.replace('_existing', '')
                result[base_col] = deleted_records[col]
            elif not col.endswith('_new') and col != '_merge':
                result[col] = deleted_records[col]

        # Close the deleted records
        result[self.config.end_date_column] = effective_date
        result[self.config.is_current_column] = False

        return result
