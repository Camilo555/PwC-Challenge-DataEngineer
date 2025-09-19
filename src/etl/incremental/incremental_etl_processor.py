"""
Incremental ETL Processing Framework
Enterprise-grade incremental ETL with change data capture (CDC),
delta processing, and optimized data transfer strategies.
"""
from __future__ import annotations

import hashlib
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd
from pydantic import BaseModel, Field

from core.logging import get_logger
from etl.framework.base_processor import BaseProcessor

logger = get_logger(__name__)


class ChangeType(Enum):
    """Types of changes detected in data."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"


class IncrementalStrategy(Enum):
    """Incremental processing strategies."""
    TIMESTAMP = "timestamp"          # Based on timestamp columns
    WATERMARK = "watermark"         # High-water mark approach
    CHECKSUM = "checksum"           # Content-based checksums
    LOG_BASED = "log_based"         # Transaction log based
    HYBRID = "hybrid"               # Combination of strategies


class ProcessingMode(Enum):
    """Processing modes for incremental ETL."""
    APPEND_ONLY = "append_only"     # Only new records
    MERGE = "merge"                 # Merge updates and inserts
    FULL_MERGE = "full_merge"       # Full merge with deletes
    STREAMING = "streaming"         # Real-time streaming


@dataclass
class IncrementalConfig:
    """Configuration for incremental ETL processing."""
    strategy: IncrementalStrategy = IncrementalStrategy.TIMESTAMP
    processing_mode: ProcessingMode = ProcessingMode.MERGE
    timestamp_column: Optional[str] = None
    watermark_column: Optional[str] = None
    primary_key_columns: List[str] = field(default_factory=list)
    checksum_columns: List[str] = field(default_factory=list)
    batch_size: int = 10000
    max_lookback_days: int = 7
    enable_soft_deletes: bool = True
    enable_deduplication: bool = True
    enable_data_validation: bool = True


@dataclass
class ChangeRecord:
    """Record representing a detected change."""
    change_type: ChangeType
    primary_key: Dict[str, Any]
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessingResult:
    """Result of incremental processing operation."""
    total_records_processed: int
    records_inserted: int
    records_updated: int
    records_deleted: int
    processing_time_seconds: float
    high_water_mark: Optional[Any] = None
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class HighWaterMarkManager:
    """Manages high-water marks for incremental processing."""

    def __init__(self, storage_backend: str = "database"):
        """Initialize high-water mark manager."""
        self.storage_backend = storage_backend
        self.watermarks: Dict[str, Any] = {}

    def get_watermark(self, table_name: str, column: str) -> Optional[Any]:
        """Get the current high-water mark for a table/column."""
        key = f"{table_name}.{column}"
        return self.watermarks.get(key)

    def set_watermark(self, table_name: str, column: str, value: Any):
        """Set the high-water mark for a table/column."""
        key = f"{table_name}.{column}"
        self.watermarks[key] = value
        logger.info(f"Updated watermark for {key}: {value}")

    def save_watermarks(self):
        """Persist watermarks to storage."""
        # In a real implementation, this would save to database, file, etc.
        logger.info("Watermarks saved to storage")

    def load_watermarks(self):
        """Load watermarks from storage."""
        # In a real implementation, this would load from database, file, etc.
        logger.info("Watermarks loaded from storage")


class ChecksumCalculator:
    """Calculates checksums for change detection."""

    @staticmethod
    def calculate_row_checksum(row: Dict[str, Any], columns: Optional[List[str]] = None) -> str:
        """Calculate checksum for a single row."""
        if columns:
            # Only include specified columns
            data_to_hash = {k: v for k, v in row.items() if k in columns}
        else:
            data_to_hash = row

        # Sort keys for consistent hashing
        sorted_data = json.dumps(data_to_hash, sort_keys=True, default=str)
        return hashlib.md5(sorted_data.encode()).hexdigest()

    @staticmethod
    def calculate_dataframe_checksums(
        df: pd.DataFrame,
        columns: Optional[List[str]] = None
    ) -> pd.Series:
        """Calculate checksums for all rows in a DataFrame."""
        if columns:
            subset_df = df[columns]
        else:
            subset_df = df

        return subset_df.apply(
            lambda row: ChecksumCalculator.calculate_row_checksum(row.to_dict()),
            axis=1
        )


class ChangeDetector:
    """Detects changes between source and target datasets."""

    def __init__(self, config: IncrementalConfig):
        """Initialize change detector with configuration."""
        self.config = config
        self.checksum_calculator = ChecksumCalculator()

    def detect_changes(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame
    ) -> List[ChangeRecord]:
        """Detect changes between source and target datasets."""
        logger.info(f"Detecting changes using strategy: {self.config.strategy.value}")

        if self.config.strategy == IncrementalStrategy.TIMESTAMP:
            return self._detect_timestamp_changes(source_df, target_df)
        elif self.config.strategy == IncrementalStrategy.CHECKSUM:
            return self._detect_checksum_changes(source_df, target_df)
        elif self.config.strategy == IncrementalStrategy.HYBRID:
            return self._detect_hybrid_changes(source_df, target_df)
        else:
            return self._detect_basic_changes(source_df, target_df)

    def _detect_timestamp_changes(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame
    ) -> List[ChangeRecord]:
        """Detect changes based on timestamp comparison."""
        changes = []

        if not self.config.timestamp_column:
            raise ValueError("Timestamp column must be specified for timestamp strategy")

        # Ensure timestamp columns are datetime
        if self.config.timestamp_column in source_df.columns:
            source_df[self.config.timestamp_column] = pd.to_datetime(
                source_df[self.config.timestamp_column]
            )

        if self.config.timestamp_column in target_df.columns:
            target_df[self.config.timestamp_column] = pd.to_datetime(
                target_df[self.config.timestamp_column]
            )

        # Create lookup for target records
        if self.config.primary_key_columns:
            target_lookup = target_df.set_index(self.config.primary_key_columns).to_dict('index')
        else:
            target_lookup = {}

        # Process each source record
        for _, source_row in source_df.iterrows():
            primary_key = self._extract_primary_key(source_row)

            if primary_key in target_lookup:
                target_row_data = target_lookup[primary_key]
                source_timestamp = source_row[self.config.timestamp_column]
                target_timestamp = target_row_data.get(self.config.timestamp_column)

                if target_timestamp and source_timestamp > target_timestamp:
                    # Record has been updated
                    changes.append(ChangeRecord(
                        change_type=ChangeType.UPDATE,
                        primary_key=primary_key,
                        old_values=target_row_data,
                        new_values=source_row.to_dict(),
                        timestamp=source_timestamp
                    ))
            else:
                # New record
                changes.append(ChangeRecord(
                    change_type=ChangeType.INSERT,
                    primary_key=primary_key,
                    new_values=source_row.to_dict(),
                    timestamp=source_row[self.config.timestamp_column]
                ))

        return changes

    def _detect_checksum_changes(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame
    ) -> List[ChangeRecord]:
        """Detect changes based on content checksums."""
        changes = []

        # Calculate checksums
        source_checksums = self.checksum_calculator.calculate_dataframe_checksums(
            source_df, self.config.checksum_columns
        )
        target_checksums = self.checksum_calculator.calculate_dataframe_checksums(
            target_df, self.config.checksum_columns
        )

        # Add checksums to dataframes
        source_df = source_df.copy()
        target_df = target_df.copy()
        source_df['_checksum'] = source_checksums
        target_df['_checksum'] = target_checksums

        # Create lookup for target records
        if self.config.primary_key_columns:
            target_lookup = target_df.set_index(self.config.primary_key_columns).to_dict('index')
        else:
            target_lookup = {}

        # Process each source record
        for _, source_row in source_df.iterrows():
            primary_key = self._extract_primary_key(source_row)

            if primary_key in target_lookup:
                target_row_data = target_lookup[primary_key]
                source_checksum = source_row['_checksum']
                target_checksum = target_row_data.get('_checksum')

                if source_checksum != target_checksum:
                    # Record has been updated
                    changes.append(ChangeRecord(
                        change_type=ChangeType.UPDATE,
                        primary_key=primary_key,
                        old_values={k: v for k, v in target_row_data.items() if k != '_checksum'},
                        new_values={k: v for k, v in source_row.to_dict().items() if k != '_checksum'},
                        checksum=source_checksum
                    ))
            else:
                # New record
                changes.append(ChangeRecord(
                    change_type=ChangeType.INSERT,
                    primary_key=primary_key,
                    new_values={k: v for k, v in source_row.to_dict().items() if k != '_checksum'},
                    checksum=source_row['_checksum']
                ))

        return changes

    def _detect_hybrid_changes(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame
    ) -> List[ChangeRecord]:
        """Detect changes using hybrid approach (timestamp + checksum)."""
        # First, filter by timestamp if available
        if self.config.timestamp_column:
            timestamp_changes = self._detect_timestamp_changes(source_df, target_df)

            # Then verify with checksums for updated records
            verified_changes = []
            for change in timestamp_changes:
                if change.change_type == ChangeType.UPDATE:
                    # Verify update with checksum
                    old_checksum = self.checksum_calculator.calculate_row_checksum(
                        change.old_values, self.config.checksum_columns
                    )
                    new_checksum = self.checksum_calculator.calculate_row_checksum(
                        change.new_values, self.config.checksum_columns
                    )

                    if old_checksum != new_checksum:
                        change.checksum = new_checksum
                        verified_changes.append(change)
                else:
                    # Insert records are always included
                    verified_changes.append(change)

            return verified_changes
        else:
            # Fallback to checksum-only detection
            return self._detect_checksum_changes(source_df, target_df)

    def _detect_basic_changes(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame
    ) -> List[ChangeRecord]:
        """Basic change detection using direct comparison."""
        changes = []

        if not self.config.primary_key_columns:
            # Without primary keys, treat all source records as inserts
            for _, source_row in source_df.iterrows():
                changes.append(ChangeRecord(
                    change_type=ChangeType.INSERT,
                    primary_key={},
                    new_values=source_row.to_dict()
                ))
            return changes

        # Create lookup for target records
        target_lookup = target_df.set_index(self.config.primary_key_columns).to_dict('index')

        # Process each source record
        for _, source_row in source_df.iterrows():
            primary_key = self._extract_primary_key(source_row)

            if primary_key in target_lookup:
                # Compare all values to detect updates
                target_row_data = target_lookup[primary_key]
                source_dict = source_row.to_dict()

                # Check if any values have changed
                has_changes = False
                for col, source_value in source_dict.items():
                    if col not in self.config.primary_key_columns:
                        target_value = target_row_data.get(col)
                        if source_value != target_value:
                            has_changes = True
                            break

                if has_changes:
                    changes.append(ChangeRecord(
                        change_type=ChangeType.UPDATE,
                        primary_key=primary_key,
                        old_values=target_row_data,
                        new_values=source_dict
                    ))
            else:
                # New record
                changes.append(ChangeRecord(
                    change_type=ChangeType.INSERT,
                    primary_key=primary_key,
                    new_values=source_row.to_dict()
                ))

        return changes

    def _extract_primary_key(self, row: pd.Series) -> Dict[str, Any]:
        """Extract primary key values from a row."""
        if not self.config.primary_key_columns:
            return {}

        return {col: row[col] for col in self.config.primary_key_columns if col in row.index}


class IncrementalETLProcessor(BaseProcessor):
    """
    Enterprise-grade incremental ETL processor with advanced change detection,
    delta processing, and optimized data transfer capabilities.
    """

    def __init__(self, config: IncrementalConfig):
        """Initialize incremental ETL processor."""
        super().__init__()
        self.config = config
        self.watermark_manager = HighWaterMarkManager()
        self.change_detector = ChangeDetector(config)

        # Processing metrics
        self.metrics = {
            'total_processed': 0,
            'total_changes_detected': 0,
            'total_inserts': 0,
            'total_updates': 0,
            'total_deletes': 0,
            'processing_time_total': 0.0,
            'avg_processing_time': 0.0,
            'last_processing_time': None
        }

    def process_incremental(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        table_name: str
    ) -> ProcessingResult:
        """
        Process incremental changes between source and target datasets.

        Args:
            source_df: Source dataset with potential changes
            target_df: Current target dataset
            table_name: Name of the table being processed

        Returns:
            ProcessingResult with processing statistics
        """
        start_time = time.time()
        logger.info(f"Starting incremental processing for table: {table_name}")

        try:
            # Filter source data based on watermark
            filtered_source = self._filter_by_watermark(source_df, table_name)
            logger.info(f"Filtered source: {len(filtered_source)} records from {len(source_df)}")

            # Detect changes
            changes = self.change_detector.detect_changes(filtered_source, target_df)
            logger.info(f"Detected {len(changes)} changes")

            # Apply changes
            result = self._apply_changes(changes, target_df, table_name)

            # Update watermark
            self._update_watermark(filtered_source, table_name)

            # Update metrics
            processing_time = time.time() - start_time
            self._update_metrics(result, processing_time)

            result.processing_time_seconds = processing_time
            result.metadata['table_name'] = table_name
            result.metadata['source_records'] = len(source_df)
            result.metadata['filtered_records'] = len(filtered_source)

            logger.info(f"Incremental processing completed for {table_name}: {result}")
            return result

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Incremental processing failed for {table_name}: {e}")

            return ProcessingResult(
                total_records_processed=0,
                records_inserted=0,
                records_updated=0,
                records_deleted=0,
                processing_time_seconds=processing_time,
                errors=[str(e)]
            )

    def _filter_by_watermark(self, source_df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Filter source data based on high-water mark."""
        if not self.config.watermark_column:
            return source_df

        # Get current watermark
        current_watermark = self.watermark_manager.get_watermark(
            table_name, self.config.watermark_column
        )

        if current_watermark is None:
            # No watermark, process all data
            return source_df

        # Filter data newer than watermark
        if self.config.watermark_column in source_df.columns:
            # Convert to datetime if needed
            watermark_series = pd.to_datetime(source_df[self.config.watermark_column])
            current_watermark = pd.to_datetime(current_watermark)

            # Include records with watermark > current watermark
            mask = watermark_series > current_watermark
            filtered_df = source_df[mask].copy()

            logger.info(f"Watermark filter: {len(filtered_df)} records after watermark {current_watermark}")
            return filtered_df
        else:
            logger.warning(f"Watermark column {self.config.watermark_column} not found in source data")
            return source_df

    def _apply_changes(
        self,
        changes: List[ChangeRecord],
        target_df: pd.DataFrame,
        table_name: str
    ) -> ProcessingResult:
        """Apply detected changes to target dataset."""
        logger.info(f"Applying {len(changes)} changes to {table_name}")

        # Count changes by type
        inserts = [c for c in changes if c.change_type == ChangeType.INSERT]
        updates = [c for c in changes if c.change_type == ChangeType.UPDATE]
        deletes = [c for c in changes if c.change_type == ChangeType.DELETE]

        logger.info(f"Changes breakdown: {len(inserts)} inserts, {len(updates)} updates, {len(deletes)} deletes")

        # Apply changes based on processing mode
        if self.config.processing_mode == ProcessingMode.APPEND_ONLY:
            result_df = self._apply_append_only(target_df, inserts)
        elif self.config.processing_mode == ProcessingMode.MERGE:
            result_df = self._apply_merge(target_df, inserts + updates)
        elif self.config.processing_mode == ProcessingMode.FULL_MERGE:
            result_df = self._apply_full_merge(target_df, changes)
        else:
            raise ValueError(f"Unsupported processing mode: {self.config.processing_mode}")

        return ProcessingResult(
            total_records_processed=len(changes),
            records_inserted=len(inserts),
            records_updated=len(updates),
            records_deleted=len(deletes),
            processing_time_seconds=0.0  # Will be set by caller
        )

    def _apply_append_only(
        self,
        target_df: pd.DataFrame,
        inserts: List[ChangeRecord]
    ) -> pd.DataFrame:
        """Apply append-only changes (inserts only)."""
        if not inserts:
            return target_df

        # Convert inserts to DataFrame
        insert_rows = [change.new_values for change in inserts]
        insert_df = pd.DataFrame(insert_rows)

        # Append to target
        if not target_df.empty:
            result_df = pd.concat([target_df, insert_df], ignore_index=True)
        else:
            result_df = insert_df

        # Deduplicate if enabled
        if self.config.enable_deduplication and self.config.primary_key_columns:
            result_df = result_df.drop_duplicates(
                subset=self.config.primary_key_columns,
                keep='last'
            )

        return result_df

    def _apply_merge(
        self,
        target_df: pd.DataFrame,
        changes: List[ChangeRecord]
    ) -> pd.DataFrame:
        """Apply merge changes (inserts and updates)."""
        if not changes:
            return target_df

        if not self.config.primary_key_columns:
            # Without primary keys, treat as append-only
            return self._apply_append_only(target_df, changes)

        # Convert changes to DataFrame
        change_rows = []
        for change in changes:
            if change.new_values:
                row = change.new_values.copy()
                row['_change_type'] = change.change_type.value
                change_rows.append(row)

        if not change_rows:
            return target_df

        changes_df = pd.DataFrame(change_rows)

        # Merge with target
        if target_df.empty:
            result_df = changes_df.drop(columns=['_change_type'])
        else:
            # Perform left join to update existing records
            merged_df = target_df.merge(
                changes_df,
                on=self.config.primary_key_columns,
                how='left',
                suffixes=('', '_new')
            )

            # Update existing records with new values
            for col in changes_df.columns:
                if col not in self.config.primary_key_columns and col != '_change_type':
                    new_col = f"{col}_new"
                    if new_col in merged_df.columns:
                        merged_df[col] = merged_df[new_col].fillna(merged_df[col])
                        merged_df = merged_df.drop(columns=[new_col])

            # Add new records
            new_records = changes_df[
                ~changes_df[self.config.primary_key_columns[0]].isin(
                    target_df[self.config.primary_key_columns[0]]
                )
            ].drop(columns=['_change_type'])

            if not new_records.empty:
                result_df = pd.concat([merged_df, new_records], ignore_index=True)
            else:
                result_df = merged_df

        return result_df

    def _apply_full_merge(
        self,
        target_df: pd.DataFrame,
        changes: List[ChangeRecord]
    ) -> pd.DataFrame:
        """Apply full merge including deletes."""
        # First apply inserts and updates
        inserts_updates = [c for c in changes if c.change_type in [ChangeType.INSERT, ChangeType.UPDATE]]
        result_df = self._apply_merge(target_df, inserts_updates)

        # Apply deletes
        deletes = [c for c in changes if c.change_type == ChangeType.DELETE]

        if deletes and self.config.primary_key_columns:
            # Remove deleted records
            delete_keys = []
            for delete_change in deletes:
                key_values = tuple(delete_change.primary_key[col] for col in self.config.primary_key_columns)
                delete_keys.append(key_values)

            if delete_keys:
                # Create mask for records to keep
                target_keys = result_df[self.config.primary_key_columns].apply(tuple, axis=1)
                keep_mask = ~target_keys.isin(delete_keys)
                result_df = result_df[keep_mask].reset_index(drop=True)

        return result_df

    def _update_watermark(self, processed_df: pd.DataFrame, table_name: str):
        """Update high-water mark after successful processing."""
        if not self.config.watermark_column or processed_df.empty:
            return

        if self.config.watermark_column in processed_df.columns:
            # Get maximum watermark value from processed data
            max_watermark = processed_df[self.config.watermark_column].max()

            self.watermark_manager.set_watermark(
                table_name, self.config.watermark_column, max_watermark
            )

            # Persist watermarks
            self.watermark_manager.save_watermarks()

    def _update_metrics(self, result: ProcessingResult, processing_time: float):
        """Update processing metrics."""
        self.metrics['total_processed'] += result.total_records_processed
        self.metrics['total_changes_detected'] += result.total_records_processed
        self.metrics['total_inserts'] += result.records_inserted
        self.metrics['total_updates'] += result.records_updated
        self.metrics['total_deletes'] += result.records_deleted
        self.metrics['processing_time_total'] += processing_time
        self.metrics['last_processing_time'] = datetime.utcnow()

        # Calculate average processing time
        total_runs = self.metrics.get('total_runs', 0) + 1
        self.metrics['total_runs'] = total_runs
        self.metrics['avg_processing_time'] = self.metrics['processing_time_total'] / total_runs

    def get_processing_metrics(self) -> Dict[str, Any]:
        """Get comprehensive processing metrics."""
        return {
            **self.metrics,
            'config': {
                'strategy': self.config.strategy.value,
                'processing_mode': self.config.processing_mode.value,
                'batch_size': self.config.batch_size,
                'enable_deduplication': self.config.enable_deduplication
            }
        }

    def process_streaming_incremental(
        self,
        source_stream,
        target_storage,
        table_name: str,
        batch_size: Optional[int] = None
    ):
        """
        Process streaming incremental data in real-time.

        This method would be implemented for real-time streaming scenarios
        using technologies like Kafka, Pulsar, or Kinesis.
        """
        # Implementation would depend on specific streaming technology
        raise NotImplementedError("Streaming incremental processing not yet implemented")


# Factory function for creating incremental processors
def create_incremental_processor(
    strategy: IncrementalStrategy = IncrementalStrategy.TIMESTAMP,
    processing_mode: ProcessingMode = ProcessingMode.MERGE,
    **config_kwargs
) -> IncrementalETLProcessor:
    """
    Create an incremental ETL processor with specified configuration.

    Args:
        strategy: Incremental processing strategy
        processing_mode: How to apply changes
        **config_kwargs: Additional configuration parameters

    Returns:
        Configured IncrementalETLProcessor
    """
    config = IncrementalConfig(
        strategy=strategy,
        processing_mode=processing_mode,
        **config_kwargs
    )

    return IncrementalETLProcessor(config)


# Export key classes and functions
__all__ = [
    'IncrementalETLProcessor',
    'IncrementalConfig',
    'ChangeDetector',
    'HighWaterMarkManager',
    'ChecksumCalculator',
    'ChangeRecord',
    'ProcessingResult',
    'IncrementalStrategy',
    'ProcessingMode',
    'ChangeType',
    'create_incremental_processor'
]