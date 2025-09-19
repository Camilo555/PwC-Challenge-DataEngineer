"""
Data Warehouse Domain Models
==========================

Entities for data warehouse operations, ETL processes, and data lineage.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4

from pydantic import Field, validator

from ..base import DomainEntity


class DataLayerType(str, Enum):
    """Data warehouse layer types."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    STAGING = "staging"
    ARCHIVE = "archive"


class DataQualityStatus(str, Enum):
    """Data quality status."""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    PENDING = "pending"
    SKIPPED = "skipped"


class ProcessingStatus(str, Enum):
    """Data processing status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ETLJob(DomainEntity):
    """ETL job execution entity."""
    
    id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = Field(max_length=500)
    
    # Job configuration
    source_layer: DataLayerType
    target_layer: DataLayerType
    job_type: str  # e.g., "incremental", "full_load", "delta"
    
    # Execution details
    status: ProcessingStatus = ProcessingStatus.PENDING
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[int]
    
    # Data metrics
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_deleted: int = 0
    records_failed: int = 0
    
    # Resource usage
    memory_peak_mb: Optional[float]
    cpu_usage_percent: Optional[float]
    io_read_mb: Optional[float]
    io_write_mb: Optional[float]
    
    # Configuration and metadata
    parameters: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str]
    log_file_path: Optional[str]
    
    # Dependencies
    upstream_jobs: List[UUID] = Field(default_factory=list)
    downstream_jobs: List[UUID] = Field(default_factory=list)
    
    @validator('duration_seconds')
    def validate_duration(cls, v, values):
        if v is not None and v < 0:
            raise ValueError('Duration cannot be negative')
        return v
    
    @property
    def success_rate(self) -> float:
        """Calculate job success rate."""
        total = self.records_processed
        if total == 0:
            return 0.0
        return (total - self.records_failed) / total


class DataLineage(DomainEntity):
    """Data lineage tracking entity."""
    
    id: UUID = Field(default_factory=uuid4)
    source_table: str
    target_table: str
    transformation_type: str
    
    # Lineage metadata
    job_id: Optional[UUID]
    etl_pipeline: str
    transformation_logic: Optional[str]
    
    # Column-level lineage
    source_columns: List[str] = Field(default_factory=list)
    target_columns: List[str] = Field(default_factory=list)
    column_mappings: Dict[str, str] = Field(default_factory=dict)
    
    # Quality and validation
    data_quality_checks: List[str] = Field(default_factory=list)
    validation_rules: Dict[str, Any] = Field(default_factory=dict)
    
    # Timing and frequency
    last_processed: Optional[datetime]
    processing_frequency: Optional[str]  # cron expression
    
    # Impact analysis
    downstream_dependencies: List[str] = Field(default_factory=list)
    business_impact: Optional[str]
    
    @validator('source_table', 'target_table')
    def validate_table_names(cls, v):
        if not v.strip():
            raise ValueError('Table name cannot be empty')
        return v.strip()


class DataQualityCheck(DomainEntity):
    """Data quality check execution entity."""
    
    id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = Field(max_length=500)
    
    # Check configuration
    table_name: str
    column_name: Optional[str]
    check_type: str  # e.g., "null_check", "range_check", "format_check"
    check_logic: str
    
    # Execution results
    status: DataQualityStatus = DataQualityStatus.PENDING
    executed_at: Optional[datetime]
    execution_time_ms: Optional[int]
    
    # Results
    total_records: int = 0
    failed_records: int = 0
    passed_records: int = 0
    failure_rate: float = Field(ge=0.0, le=1.0, default=0.0)
    
    # Thresholds
    warning_threshold: float = Field(ge=0.0, le=1.0, default=0.05)
    failure_threshold: float = Field(ge=0.0, le=1.0, default=0.10)
    
    # Error details
    error_samples: List[Dict[str, Any]] = Field(default_factory=list)
    error_message: Optional[str]
    
    # Remediation
    auto_fix_enabled: bool = False
    remediation_actions: List[str] = Field(default_factory=list)
    
    @validator('failure_rate')
    def calculate_failure_rate(cls, v, values):
        total = values.get('total_records', 0)
        failed = values.get('failed_records', 0)
        if total > 0:
            return failed / total
        return 0.0
    
    @property
    def is_healthy(self) -> bool:
        """Check if data quality is healthy."""
        return self.status == DataQualityStatus.PASSED and self.failure_rate <= self.warning_threshold


class DataPartition(DomainEntity):
    """Data partition management entity."""
    
    id: UUID = Field(default_factory=uuid4)
    table_name: str
    partition_key: str
    partition_value: str
    
    # Partition metadata
    layer: DataLayerType
    schema_name: str
    partition_type: str  # e.g., "date", "range", "hash"
    
    # Storage information
    file_path: Optional[str]
    file_format: str = "parquet"  # parquet, delta, orc, etc.
    compression_type: Optional[str]
    
    # Size and performance metrics
    record_count: int = 0
    size_bytes: int = 0
    avg_row_size_bytes: Optional[float]
    
    # Partition lifecycle
    created_date: datetime = Field(default_factory=datetime.utcnow)
    last_accessed: Optional[datetime]
    last_modified: Optional[datetime]
    retention_days: Optional[int]
    
    # Optimization metrics
    read_frequency: int = 0
    write_frequency: int = 0
    query_performance_ms: Optional[float]
    
    # Status and maintenance
    is_active: bool = True
    needs_compaction: bool = False
    needs_reindexing: bool = False
    
    @validator('record_count', 'size_bytes')
    def validate_non_negative(cls, v):
        if v < 0:
            raise ValueError('Value must be non-negative')
        return v
    
    @property
    def size_mb(self) -> float:
        """Get partition size in MB."""
        return self.size_bytes / (1024 * 1024) if self.size_bytes > 0 else 0.0


class DataCatalog(DomainEntity):
    """Data catalog entity for metadata management."""
    
    id: UUID = Field(default_factory=uuid4)
    table_name: str
    schema_name: str
    database_name: str
    
    # Table metadata
    description: Optional[str] = Field(max_length=1000)
    table_type: str  # table, view, materialized_view
    layer: DataLayerType
    
    # Schema information
    columns: List[Dict[str, Any]] = Field(default_factory=list)
    primary_keys: List[str] = Field(default_factory=list)
    foreign_keys: List[Dict[str, str]] = Field(default_factory=list)
    indexes: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Business metadata
    business_owner: Optional[str]
    technical_owner: Optional[str]
    domain: Optional[str]
    classification: Optional[str]  # public, internal, confidential, restricted
    
    # Usage statistics
    last_accessed: Optional[datetime]
    access_frequency: int = 0
    dependent_tables: List[str] = Field(default_factory=list)
    
    # Data quality
    data_quality_score: Optional[float] = Field(ge=0.0, le=1.0)
    last_quality_check: Optional[datetime]
    
    # Lifecycle
    is_deprecated: bool = False
    deprecation_date: Optional[datetime]
    replacement_table: Optional[str]
    
    @validator('table_name', 'schema_name', 'database_name')
    def validate_names(cls, v):
        if not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip().lower()
    
    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.database_name}.{self.schema_name}.{self.table_name}"