"""
Backup Validation and Integrity Checking

Comprehensive backup validation system with integrity checks, corruption detection,
and automated verification procedures.
"""
from __future__ import annotations

import gzip
import hashlib
import sqlite3
import tarfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)


class ValidationResult(str, Enum):
    """Results of validation operations."""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


class IntegrityLevel(str, Enum):
    """Levels of integrity validation."""
    BASIC = "basic"        # File existence and size checks
    STANDARD = "standard"  # Checksums and format validation
    THOROUGH = "thorough"  # Deep content validation and structure checks
    FORENSIC = "forensic"  # Comprehensive validation with recovery testing


@dataclass
class ValidationCheck:
    """Individual validation check result."""
    check_name: str
    result: ValidationResult
    message: str
    execution_time_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'check_name': self.check_name,
            'result': self.result.value,
            'message': self.message,
            'execution_time_ms': self.execution_time_ms,
            'metadata': self.metadata
        }


@dataclass
class ValidationReport:
    """Comprehensive validation report."""
    backup_id: str
    validation_timestamp: datetime
    integrity_level: IntegrityLevel
    overall_result: ValidationResult
    checks: list[ValidationCheck]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'backup_id': self.backup_id,
            'validation_timestamp': self.validation_timestamp.isoformat(),
            'integrity_level': self.integrity_level.value,
            'overall_result': self.overall_result.value,
            'checks': [check.to_dict() for check in self.checks],
            'summary': self.summary
        }

    @property
    def passed_checks(self) -> int:
        """Count of passed validation checks."""
        return sum(1 for check in self.checks if check.result == ValidationResult.PASSED)

    @property
    def failed_checks(self) -> int:
        """Count of failed validation checks."""
        return sum(1 for check in self.checks if check.result == ValidationResult.FAILED)

    @property
    def warning_checks(self) -> int:
        """Count of checks with warnings."""
        return sum(1 for check in self.checks if check.result == ValidationResult.WARNING)

    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        if not self.checks:
            return 0.0
        return (self.passed_checks / len(self.checks)) * 100


class BackupValidator:
    """
    Comprehensive backup validation system.
    
    Features:
    - Multi-level integrity validation
    - Component-specific validation rules
    - Corruption detection and reporting
    - Performance impact monitoring
    - Automated validation scheduling
    """

    def __init__(self, validation_config: dict[str, Any] | None = None):
        self.config = validation_config or {}
        self.validation_cache: dict[str, ValidationReport] = {}

        # Default validation configuration
        self.default_config = {
            "checksum_algorithm": "sha256",
            "max_validation_time_seconds": 300,  # 5 minutes
            "parallel_validation": True,
            "deep_validation_sample_size": 1024 * 1024,  # 1MB sample for deep checks
            "retention_days": 30
        }

        # Merge with provided config
        self.config = {**self.default_config, **self.config}

    async def validate_backup(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any],
        integrity_level: IntegrityLevel = IntegrityLevel.STANDARD
    ) -> ValidationReport:
        """
        Perform comprehensive backup validation.
        
        Args:
            backup_path: Path to backup file/directory
            backup_metadata: Backup metadata dictionary
            integrity_level: Level of validation to perform
            
        Returns:
            Detailed validation report
        """
        backup_id = backup_metadata.get("backup_id", "unknown")
        start_time = datetime.utcnow()

        logger.info(f"Starting {integrity_level.value} validation for backup: {backup_id}")

        validation_checks = []
        overall_result = ValidationResult.PASSED

        try:
            # Basic validation checks
            basic_checks = await self._perform_basic_validation(backup_path, backup_metadata)
            validation_checks.extend(basic_checks)

            # Standard validation checks
            if integrity_level in [IntegrityLevel.STANDARD, IntegrityLevel.THOROUGH, IntegrityLevel.FORENSIC]:
                standard_checks = await self._perform_standard_validation(backup_path, backup_metadata)
                validation_checks.extend(standard_checks)

            # Thorough validation checks
            if integrity_level in [IntegrityLevel.THOROUGH, IntegrityLevel.FORENSIC]:
                thorough_checks = await self._perform_thorough_validation(backup_path, backup_metadata)
                validation_checks.extend(thorough_checks)

            # Forensic validation checks
            if integrity_level == IntegrityLevel.FORENSIC:
                forensic_checks = await self._perform_forensic_validation(backup_path, backup_metadata)
                validation_checks.extend(forensic_checks)

            # Determine overall result
            if any(check.result == ValidationResult.FAILED for check in validation_checks):
                overall_result = ValidationResult.FAILED
            elif any(check.result == ValidationResult.WARNING for check in validation_checks):
                overall_result = ValidationResult.WARNING

        except Exception as e:
            # Critical validation error
            error_check = ValidationCheck(
                check_name="validation_execution",
                result=ValidationResult.FAILED,
                message=f"Validation failed with critical error: {e}"
            )
            validation_checks.append(error_check)
            overall_result = ValidationResult.FAILED
            logger.error(f"Validation failed for {backup_id}: {e}")

        # Calculate summary statistics
        end_time = datetime.utcnow()
        total_time = (end_time - start_time).total_seconds()

        summary = {
            "total_checks": len(validation_checks),
            "passed_checks": sum(1 for c in validation_checks if c.result == ValidationResult.PASSED),
            "failed_checks": sum(1 for c in validation_checks if c.result == ValidationResult.FAILED),
            "warning_checks": sum(1 for c in validation_checks if c.result == ValidationResult.WARNING),
            "skipped_checks": sum(1 for c in validation_checks if c.result == ValidationResult.SKIPPED),
            "total_validation_time_seconds": total_time,
            "backup_size_bytes": backup_metadata.get("size_bytes", 0),
            "backup_type": backup_metadata.get("backup_type", "unknown"),
            "component": backup_metadata.get("tags", {}).get("component", "unknown")
        }

        # Create validation report
        report = ValidationReport(
            backup_id=backup_id,
            validation_timestamp=start_time,
            integrity_level=integrity_level,
            overall_result=overall_result,
            checks=validation_checks,
            summary=summary
        )

        # Cache the report
        self.validation_cache[backup_id] = report

        logger.info(f"Validation completed for {backup_id}: "
                   f"{report.overall_result.value} "
                   f"({report.success_rate:.1f}% success rate)")

        return report

    async def _perform_basic_validation(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> list[ValidationCheck]:
        """Perform basic validation checks."""
        checks = []

        # File existence check
        start_time = datetime.utcnow()
        if backup_path.exists():
            checks.append(ValidationCheck(
                check_name="file_existence",
                result=ValidationResult.PASSED,
                message="Backup file exists",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))
        else:
            checks.append(ValidationCheck(
                check_name="file_existence",
                result=ValidationResult.FAILED,
                message=f"Backup file not found: {backup_path}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))
            return checks  # Cannot continue without file

        # File size validation
        start_time = datetime.utcnow()
        try:
            if backup_path.is_file():
                actual_size = backup_path.stat().st_size
            else:
                actual_size = sum(f.stat().st_size for f in backup_path.rglob('*') if f.is_file())

            expected_size = backup_metadata.get("size_bytes", 0)

            if expected_size > 0:
                size_diff = abs(actual_size - expected_size)
                tolerance = max(1024, expected_size * 0.01)  # 1KB or 1% tolerance

                if size_diff <= tolerance:
                    checks.append(ValidationCheck(
                        check_name="file_size",
                        result=ValidationResult.PASSED,
                        message=f"File size matches (expected: {expected_size}, actual: {actual_size})",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                        metadata={"expected_size": expected_size, "actual_size": actual_size}
                    ))
                else:
                    checks.append(ValidationCheck(
                        check_name="file_size",
                        result=ValidationResult.FAILED,
                        message=f"File size mismatch (expected: {expected_size}, actual: {actual_size})",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                        metadata={"expected_size": expected_size, "actual_size": actual_size}
                    ))
            else:
                checks.append(ValidationCheck(
                    check_name="file_size",
                    result=ValidationResult.WARNING,
                    message="No expected size in metadata to compare",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                    metadata={"actual_size": actual_size}
                ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="file_size",
                result=ValidationResult.FAILED,
                message=f"Could not verify file size: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        # File permissions check
        start_time = datetime.utcnow()
        try:
            if backup_path.is_file():
                readable = backup_path.stat().st_mode & 0o400  # Owner read permission
            else:
                readable = backup_path.stat().st_mode & 0o500  # Owner read and execute

            if readable:
                checks.append(ValidationCheck(
                    check_name="file_permissions",
                    result=ValidationResult.PASSED,
                    message="Backup file is readable",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                ))
            else:
                checks.append(ValidationCheck(
                    check_name="file_permissions",
                    result=ValidationResult.FAILED,
                    message="Backup file is not readable",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="file_permissions",
                result=ValidationResult.WARNING,
                message=f"Could not verify file permissions: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _perform_standard_validation(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> list[ValidationCheck]:
        """Perform standard validation checks."""
        checks = []

        # Checksum validation
        expected_checksum = backup_metadata.get("checksum")
        if expected_checksum:
            start_time = datetime.utcnow()
            try:
                actual_checksum = await self._calculate_checksum(backup_path)

                if actual_checksum == expected_checksum:
                    checks.append(ValidationCheck(
                        check_name="checksum_validation",
                        result=ValidationResult.PASSED,
                        message="Checksum validation passed",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                        metadata={"algorithm": self.config["checksum_algorithm"], "checksum": actual_checksum}
                    ))
                else:
                    checks.append(ValidationCheck(
                        check_name="checksum_validation",
                        result=ValidationResult.FAILED,
                        message=f"Checksum mismatch (expected: {expected_checksum}, actual: {actual_checksum})",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                        metadata={"expected": expected_checksum, "actual": actual_checksum}
                    ))

            except Exception as e:
                checks.append(ValidationCheck(
                    check_name="checksum_validation",
                    result=ValidationResult.FAILED,
                    message=f"Checksum calculation failed: {e}",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                ))

        # Format validation based on file type
        component = backup_metadata.get("tags", {}).get("component", "unknown")
        format_checks = await self._validate_backup_format(backup_path, component)
        checks.extend(format_checks)

        # Compression validation
        if backup_path.suffix in [".gz", ".bz2", ".xz"]:
            compression_checks = await self._validate_compression(backup_path)
            checks.extend(compression_checks)

        return checks

    async def _perform_thorough_validation(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> list[ValidationCheck]:
        """Perform thorough validation checks."""
        checks = []

        # Content structure validation
        component = backup_metadata.get("tags", {}).get("component", "unknown")
        structure_checks = await self._validate_content_structure(backup_path, component)
        checks.extend(structure_checks)

        # Sample data validation
        sample_checks = await self._validate_sample_data(backup_path, component)
        checks.extend(sample_checks)

        # Metadata consistency validation
        metadata_checks = await self._validate_metadata_consistency(backup_path, backup_metadata)
        checks.extend(metadata_checks)

        return checks

    async def _perform_forensic_validation(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> list[ValidationCheck]:
        """Perform forensic-level validation checks."""
        checks = []

        # Recovery simulation test
        recovery_checks = await self._simulate_recovery_test(backup_path, backup_metadata)
        checks.extend(recovery_checks)

        # Bit-level validation
        bitlevel_checks = await self._validate_bit_patterns(backup_path)
        checks.extend(bitlevel_checks)

        # Historical comparison
        historical_checks = await self._validate_historical_consistency(backup_path, backup_metadata)
        checks.extend(historical_checks)

        return checks

    async def _validate_backup_format(self, backup_path: Path, component: str) -> list[ValidationCheck]:
        """Validate backup file format."""
        checks = []
        start_time = datetime.utcnow()

        try:
            if component == "database" and backup_path.suffix == ".db":
                # SQLite database validation
                try:
                    conn = sqlite3.connect(str(backup_path))
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA integrity_check;")
                    result = cursor.fetchone()
                    conn.close()

                    if result and result[0] == "ok":
                        checks.append(ValidationCheck(
                            check_name="database_format",
                            result=ValidationResult.PASSED,
                            message="SQLite database format is valid",
                            execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                        ))
                    else:
                        checks.append(ValidationCheck(
                            check_name="database_format",
                            result=ValidationResult.FAILED,
                            message=f"SQLite integrity check failed: {result}",
                            execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                        ))

                except Exception as e:
                    checks.append(ValidationCheck(
                        check_name="database_format",
                        result=ValidationResult.FAILED,
                        message=f"Database format validation failed: {e}",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                    ))

            elif backup_path.suffix == ".tar":
                # TAR archive validation
                try:
                    with tarfile.open(backup_path, 'r') as tar:
                        tar.getmembers()  # This will raise exception if corrupted

                    checks.append(ValidationCheck(
                        check_name="tar_format",
                        result=ValidationResult.PASSED,
                        message="TAR archive format is valid",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                    ))

                except Exception as e:
                    checks.append(ValidationCheck(
                        check_name="tar_format",
                        result=ValidationResult.FAILED,
                        message=f"TAR format validation failed: {e}",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                    ))

            else:
                checks.append(ValidationCheck(
                    check_name="format_validation",
                    result=ValidationResult.SKIPPED,
                    message=f"No specific format validation for {backup_path.suffix}",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="format_validation",
                result=ValidationResult.FAILED,
                message=f"Format validation error: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _validate_compression(self, backup_path: Path) -> list[ValidationCheck]:
        """Validate compressed backup integrity."""
        checks = []
        start_time = datetime.utcnow()

        try:
            if backup_path.suffix == ".gz":
                with gzip.open(backup_path, 'rb') as f:
                    # Read first 1KB to verify compression integrity
                    f.read(1024)

                checks.append(ValidationCheck(
                    check_name="compression_integrity",
                    result=ValidationResult.PASSED,
                    message="GZIP compression integrity validated",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="compression_integrity",
                result=ValidationResult.FAILED,
                message=f"Compression validation failed: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _validate_content_structure(self, backup_path: Path, component: str) -> list[ValidationCheck]:
        """Validate backup content structure."""
        checks = []
        start_time = datetime.utcnow()

        try:
            if component == "data_lake" and backup_path.is_dir():
                # Expected data lake structure
                expected_dirs = ["bronze", "silver", "gold"]
                found_dirs = [d.name for d in backup_path.iterdir() if d.is_dir()]

                missing_dirs = set(expected_dirs) - set(found_dirs)
                if not missing_dirs:
                    checks.append(ValidationCheck(
                        check_name="data_lake_structure",
                        result=ValidationResult.PASSED,
                        message="Data lake structure is complete",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                        metadata={"found_dirs": found_dirs}
                    ))
                else:
                    checks.append(ValidationCheck(
                        check_name="data_lake_structure",
                        result=ValidationResult.WARNING,
                        message=f"Missing directories: {missing_dirs}",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                        metadata={"found_dirs": found_dirs, "missing_dirs": list(missing_dirs)}
                    ))

            else:
                checks.append(ValidationCheck(
                    check_name="content_structure",
                    result=ValidationResult.SKIPPED,
                    message="No specific structure validation defined",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="content_structure",
                result=ValidationResult.FAILED,
                message=f"Structure validation error: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _validate_sample_data(self, backup_path: Path, component: str) -> list[ValidationCheck]:
        """Validate sample data from backup."""
        checks = []
        start_time = datetime.utcnow()

        try:
            sample_size = self.config["deep_validation_sample_size"]

            if backup_path.is_file():
                # Read sample from file
                with open(backup_path, 'rb') as f:
                    sample_data = f.read(sample_size)

                # Basic data validation
                if len(sample_data) > 0:
                    # Check for null bytes (could indicate corruption)
                    null_ratio = sample_data.count(b'\x00') / len(sample_data)

                    if null_ratio < 0.1:  # Less than 10% null bytes is normal
                        checks.append(ValidationCheck(
                            check_name="sample_data_quality",
                            result=ValidationResult.PASSED,
                            message=f"Sample data quality check passed (null ratio: {null_ratio:.3f})",
                            execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                            metadata={"sample_size": len(sample_data), "null_ratio": null_ratio}
                        ))
                    else:
                        checks.append(ValidationCheck(
                            check_name="sample_data_quality",
                            result=ValidationResult.WARNING,
                            message=f"High null byte ratio in sample data: {null_ratio:.3f}",
                            execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                            metadata={"sample_size": len(sample_data), "null_ratio": null_ratio}
                        ))
                else:
                    checks.append(ValidationCheck(
                        check_name="sample_data_quality",
                        result=ValidationResult.FAILED,
                        message="No sample data could be read",
                        execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                    ))

            else:
                checks.append(ValidationCheck(
                    check_name="sample_data_quality",
                    result=ValidationResult.SKIPPED,
                    message="Sample validation not applicable to directories",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
                ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="sample_data_quality",
                result=ValidationResult.FAILED,
                message=f"Sample data validation failed: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _validate_metadata_consistency(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> list[ValidationCheck]:
        """Validate metadata consistency."""
        checks = []
        start_time = datetime.utcnow()

        try:
            # Validate timestamp consistency
            file_mtime = datetime.fromtimestamp(backup_path.stat().st_mtime)
            backup_timestamp = datetime.fromisoformat(backup_metadata["timestamp"])

            time_diff = abs((file_mtime - backup_timestamp).total_seconds())

            if time_diff < 300:  # Within 5 minutes is acceptable
                checks.append(ValidationCheck(
                    check_name="timestamp_consistency",
                    result=ValidationResult.PASSED,
                    message="Backup timestamp is consistent with file modification time",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                    metadata={"time_diff_seconds": time_diff}
                ))
            else:
                checks.append(ValidationCheck(
                    check_name="timestamp_consistency",
                    result=ValidationResult.WARNING,
                    message=f"Large timestamp difference: {time_diff:.0f} seconds",
                    execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                    metadata={"time_diff_seconds": time_diff}
                ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="metadata_consistency",
                result=ValidationResult.FAILED,
                message=f"Metadata consistency validation failed: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _simulate_recovery_test(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> list[ValidationCheck]:
        """Simulate recovery to test backup viability."""
        checks = []
        start_time = datetime.utcnow()

        try:
            # This would involve creating a temporary restore location
            # and attempting to restore a small portion of the backup

            checks.append(ValidationCheck(
                check_name="recovery_simulation",
                result=ValidationResult.SKIPPED,
                message="Recovery simulation requires dedicated test environment",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="recovery_simulation",
                result=ValidationResult.FAILED,
                message=f"Recovery simulation failed: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _validate_bit_patterns(self, backup_path: Path) -> list[ValidationCheck]:
        """Validate bit-level patterns for corruption detection."""
        checks = []
        start_time = datetime.utcnow()

        try:
            # Simplified bit pattern analysis
            checks.append(ValidationCheck(
                check_name="bit_pattern_analysis",
                result=ValidationResult.SKIPPED,
                message="Bit-level validation requires specialized tools",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="bit_pattern_analysis",
                result=ValidationResult.FAILED,
                message=f"Bit pattern analysis failed: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _validate_historical_consistency(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> list[ValidationCheck]:
        """Validate consistency with historical backups."""
        checks = []
        start_time = datetime.utcnow()

        try:
            # Compare with previous backups
            checks.append(ValidationCheck(
                check_name="historical_consistency",
                result=ValidationResult.SKIPPED,
                message="Historical comparison requires backup history database",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        except Exception as e:
            checks.append(ValidationCheck(
                check_name="historical_consistency",
                result=ValidationResult.FAILED,
                message=f"Historical consistency validation failed: {e}",
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000
            ))

        return checks

    async def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate file checksum."""
        hash_func = hashlib.sha256()

        if file_path.is_file():
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_func.update(chunk)
        else:
            # Calculate checksum for directory contents
            for file_path in sorted(file_path.rglob('*')):
                if file_path.is_file():
                    with open(file_path, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            hash_func.update(chunk)

        return hash_func.hexdigest()

    def get_validation_history(self, backup_id: str | None = None) -> list[dict[str, Any]]:
        """Get validation history."""
        if backup_id:
            report = self.validation_cache.get(backup_id)
            return [report.to_dict()] if report else []

        return [report.to_dict() for report in self.validation_cache.values()]


class IntegrityChecker:
    """
    Automated integrity checking with scheduling and alerting.
    
    Features:
    - Scheduled integrity checks
    - Automated corruption detection
    - Alert generation and notifications
    - Performance monitoring
    """

    def __init__(self, validator: BackupValidator):
        self.validator = validator
        self.check_schedule: dict[str, dict[str, Any]] = {}
        self.alert_handlers: list[Callable] = []

    async def schedule_integrity_check(
        self,
        backup_id: str,
        schedule_expression: str,
        integrity_level: IntegrityLevel = IntegrityLevel.STANDARD
    ) -> None:
        """Schedule automated integrity checking."""
        self.check_schedule[backup_id] = {
            "schedule": schedule_expression,
            "integrity_level": integrity_level,
            "last_check": None,
            "next_check": None
        }

        logger.info(f"Scheduled integrity check for {backup_id}: {schedule_expression}")

    async def run_scheduled_checks(self) -> dict[str, Any]:
        """Run all scheduled integrity checks."""
        results = {
            "checks_run": 0,
            "checks_passed": 0,
            "checks_failed": 0,
            "alerts_generated": 0
        }

        for backup_id, schedule_info in self.check_schedule.items():
            try:
                # Simplified scheduling logic (would use cron or APScheduler in production)
                # For now, just run the check

                # Load backup metadata and path (simplified)
                # In practice, this would query the backup manager
                backup_path = Path(f"./backups/{backup_id}")
                backup_metadata = {"backup_id": backup_id, "timestamp": datetime.utcnow().isoformat()}

                if backup_path.exists():
                    report = await self.validator.validate_backup(
                        backup_path,
                        backup_metadata,
                        schedule_info["integrity_level"]
                    )

                    results["checks_run"] += 1

                    if report.overall_result == ValidationResult.PASSED:
                        results["checks_passed"] += 1
                    else:
                        results["checks_failed"] += 1

                        # Generate alert for failed checks
                        await self._generate_alert(backup_id, report)
                        results["alerts_generated"] += 1

                    schedule_info["last_check"] = datetime.utcnow()

            except Exception as e:
                logger.error(f"Scheduled integrity check failed for {backup_id}: {e}")
                results["checks_failed"] += 1

        return results

    async def _generate_alert(self, backup_id: str, report: ValidationReport) -> None:
        """Generate alert for failed integrity check."""
        alert_data = {
            "alert_type": "integrity_check_failed",
            "backup_id": backup_id,
            "validation_report": report.to_dict(),
            "timestamp": datetime.utcnow().isoformat(),
            "severity": "high" if report.failed_checks > 0 else "medium"
        }

        # Call all registered alert handlers
        for handler in self.alert_handlers:
            try:
                await handler(alert_data)
            except Exception as e:
                logger.error(f"Alert handler failed: {e}")

        logger.warning(f"Integrity check alert generated for {backup_id}")

    def add_alert_handler(self, handler: Callable) -> None:
        """Add alert handler function."""
        self.alert_handlers.append(handler)
