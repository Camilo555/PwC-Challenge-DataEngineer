"""
Advanced API Migration Engine
============================

Enterprise-grade API migration engine with automated transformation,
validation, rollback capabilities, and zero-downtime deployments.
"""
from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import yaml

from fastapi import Request, Response
from pydantic import BaseModel, Field

from core.logging import get_logger

logger = get_logger(__name__)


class MigrationStrategy(Enum):
    """Migration deployment strategies."""
    BLUE_GREEN = "blue_green"
    CANARY = "canary"
    ROLLING = "rolling"
    IMMEDIATE = "immediate"
    SHADOW = "shadow"


class MigrationStatus(Enum):
    """Migration execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    PAUSED = "paused"


class ValidationSeverity(Enum):
    """Validation issue severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationIssue:
    """API validation issue."""
    severity: ValidationSeverity
    category: str
    description: str
    field_path: str = ""
    expected_value: Any = None
    actual_value: Any = None
    suggestion: str = ""


@dataclass
class MigrationMetrics:
    """Migration performance and reliability metrics."""
    total_requests: int = 0
    successful_migrations: int = 0
    failed_migrations: int = 0
    validation_errors: int = 0
    avg_response_time: float = 0.0
    error_rate: float = 0.0
    rollback_count: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None


class APITransformer(ABC):
    """Abstract base class for API transformers."""

    @abstractmethod
    async def transform_request(self, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform request data between versions."""
        pass

    @abstractmethod
    async def transform_response(self, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform response data between versions."""
        pass

    @abstractmethod
    async def validate_transformation(self, original: Dict[str, Any], transformed: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate transformation accuracy."""
        pass


class SalesAPITransformer(APITransformer):
    """Transformer for sales API between versions."""

    def __init__(self):
        # Version-specific transformation rules
        self.transformation_rules = {
            ("1.0", "2.0"): {
                "request_mappings": {
                    "invoice_date": "transaction_date",
                    "customer_country": "country_code",
                    "product_id": "stock_code"
                },
                "response_mappings": {
                    "transaction_date": "invoice_date",
                    "country_code": "customer_country",
                    "stock_code": "product_id"
                },
                "field_transformations": {
                    "date_format": lambda x: self._convert_date_format(x, "DD/MM/YYYY", "YYYY-MM-DD"),
                    "currency": lambda x: self._normalize_currency(x)
                }
            },
            ("2.0", "3.0"): {
                "request_mappings": {
                    "customer_id": "customer_key",
                    "total_amount": "net_amount"
                },
                "response_mappings": {
                    "customer_key": "customer_id",
                    "net_amount": "total_amount"
                },
                "field_transformations": {
                    "decimal_precision": lambda x: round(float(x), 2) if isinstance(x, (int, float)) else x
                }
            }
        }

    async def transform_request(self, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform request data between API versions."""
        rule_key = (from_version, to_version)
        if rule_key not in self.transformation_rules:
            logger.warning(f"No transformation rule found for {from_version} -> {to_version}")
            return data

        rules = self.transformation_rules[rule_key]
        transformed = {}

        # Apply field mappings
        for old_field, new_field in rules.get("request_mappings", {}).items():
            if old_field in data:
                transformed[new_field] = data[old_field]
            else:
                # Keep unmapped fields
                for key, value in data.items():
                    if key not in rules["request_mappings"]:
                        transformed[key] = value

        # Apply field transformations
        for field, transformer in rules.get("field_transformations", {}).items():
            if field in transformed:
                try:
                    transformed[field] = transformer(transformed[field])
                except Exception as e:
                    logger.error(f"Field transformation failed for {field}: {e}")
                    # Keep original value on transformation failure
                    pass

        logger.debug(f"Transformed request from {from_version} to {to_version}: {len(data)} -> {len(transformed)} fields")
        return transformed

    async def transform_response(self, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform response data between API versions."""
        rule_key = (to_version, from_version)  # Reverse for response transformation
        if rule_key not in self.transformation_rules:
            return data

        rules = self.transformation_rules[rule_key]
        transformed = {}

        # Apply reverse field mappings for response
        for new_field, old_field in rules.get("response_mappings", {}).items():
            if new_field in data:
                transformed[old_field] = data[new_field]
            else:
                # Keep unmapped fields
                for key, value in data.items():
                    if key not in rules["response_mappings"]:
                        transformed[key] = value

        return transformed

    async def validate_transformation(self, original: Dict[str, Any], transformed: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate transformation accuracy and completeness."""
        issues = []

        # Check for data loss
        if len(transformed) < len(original):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                category="data_loss",
                description=f"Transformed data has fewer fields ({len(transformed)}) than original ({len(original)})",
                suggestion="Review field mappings to ensure all important fields are preserved"
            ))

        # Validate required fields
        required_fields = ["transaction_date", "total_amount", "customer_id"]
        for field in required_fields:
            if field not in transformed:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category="missing_field",
                    description=f"Required field '{field}' missing after transformation",
                    field_path=field,
                    suggestion=f"Add mapping for '{field}' in transformation rules"
                ))

        # Validate data types
        type_validations = {
            "total_amount": (int, float),
            "quantity": (int,),
            "unit_price": (int, float)
        }

        for field, expected_types in type_validations.items():
            if field in transformed and not isinstance(transformed[field], expected_types):
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category="type_mismatch",
                    description=f"Field '{field}' has incorrect type",
                    field_path=field,
                    expected_value=str(expected_types),
                    actual_value=type(transformed[field]).__name__,
                    suggestion=f"Convert '{field}' to one of: {expected_types}"
                ))

        return issues

    def _convert_date_format(self, date_str: str, from_format: str, to_format: str) -> str:
        """Convert date between different formats."""
        if not date_str:
            return date_str

        try:
            # This is a simplified implementation
            # In production, you'd use proper date parsing libraries
            if from_format == "DD/MM/YYYY" and to_format == "YYYY-MM-DD":
                parts = date_str.split("/")
                if len(parts) == 3:
                    return f"{parts[2]}-{parts[1]:0>2}-{parts[0]:0>2}"
            return date_str
        except Exception:
            return date_str

    def _normalize_currency(self, amount: Any) -> float:
        """Normalize currency values to standard format."""
        if isinstance(amount, str):
            # Remove currency symbols and convert
            cleaned = ''.join(c for c in amount if c.isdigit() or c in '.-')
            return float(cleaned) if cleaned else 0.0
        return float(amount) if amount else 0.0


class MigrationValidator:
    """Validates API migrations before and during execution."""

    def __init__(self):
        self.validation_rules = {
            "schema_compatibility": self._validate_schema_compatibility,
            "data_integrity": self._validate_data_integrity,
            "performance_impact": self._validate_performance_impact,
            "security_compliance": self._validate_security_compliance
        }

    async def validate_migration_plan(self, from_version: str, to_version: str,
                                    migration_config: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate migration plan before execution."""
        issues = []

        # Run all validation rules
        for rule_name, validator in self.validation_rules.items():
            try:
                rule_issues = await validator(from_version, to_version, migration_config)
                issues.extend(rule_issues)
            except Exception as e:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.CRITICAL,
                    category="validation_error",
                    description=f"Validation rule '{rule_name}' failed: {str(e)}",
                    suggestion="Check validation rule implementation"
                ))

        return issues

    async def _validate_schema_compatibility(self, from_version: str, to_version: str,
                                           config: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate schema compatibility between versions."""
        issues = []

        # Check for breaking changes
        breaking_changes = config.get("breaking_changes", [])
        if breaking_changes:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                category="schema_compatibility",
                description=f"Migration includes {len(breaking_changes)} breaking changes",
                suggestion="Ensure backward compatibility transformers are in place"
            ))

        return issues

    async def _validate_data_integrity(self, from_version: str, to_version: str,
                                     config: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate data integrity during migration."""
        issues = []

        # Check for data loss scenarios
        field_mappings = config.get("field_mappings", {})
        if not field_mappings:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                category="data_integrity",
                description="No field mappings defined for migration",
                suggestion="Define explicit field mappings to prevent data loss"
            ))

        return issues

    async def _validate_performance_impact(self, from_version: str, to_version: str,
                                         config: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate potential performance impact."""
        issues = []

        # Check transformation complexity
        transformation_count = len(config.get("field_transformations", {}))
        if transformation_count > 10:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.INFO,
                category="performance",
                description=f"High number of field transformations ({transformation_count})",
                suggestion="Consider optimizing transformations or using caching"
            ))

        return issues

    async def _validate_security_compliance(self, from_version: str, to_version: str,
                                          config: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate security compliance during migration."""
        issues = []

        # Check for sensitive field handling
        sensitive_fields = ["password", "token", "api_key", "secret"]
        field_mappings = config.get("field_mappings", {})

        for field in sensitive_fields:
            if field in field_mappings:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.CRITICAL,
                    category="security",
                    description=f"Sensitive field '{field}' included in migration",
                    field_path=field,
                    suggestion="Ensure sensitive data is properly encrypted or excluded"
                ))

        return issues


class MigrationExecutor:
    """Executes API migrations with monitoring and rollback capabilities."""

    def __init__(self, transformer: APITransformer, validator: MigrationValidator):
        self.transformer = transformer
        self.validator = validator
        self.active_migrations: Dict[str, MigrationMetrics] = {}
        self.rollback_snapshots: Dict[str, Dict[str, Any]] = {}

    async def execute_migration(self, migration_id: str, from_version: str, to_version: str,
                              strategy: MigrationStrategy, config: Dict[str, Any]) -> MigrationMetrics:
        """Execute API migration with specified strategy."""
        logger.info(f"Starting migration {migration_id}: {from_version} -> {to_version} using {strategy.value}")

        metrics = MigrationMetrics()
        self.active_migrations[migration_id] = metrics

        try:
            # Validate migration plan
            validation_issues = await self.validator.validate_migration_plan(from_version, to_version, config)
            critical_issues = [issue for issue in validation_issues if issue.severity == ValidationSeverity.CRITICAL]

            if critical_issues:
                raise ValueError(f"Migration blocked by {len(critical_issues)} critical issues")

            # Create rollback snapshot
            self.rollback_snapshots[migration_id] = {
                "version": from_version,
                "config": config.copy(),
                "timestamp": datetime.utcnow()
            }

            # Execute based on strategy
            if strategy == MigrationStrategy.BLUE_GREEN:
                await self._execute_blue_green_migration(migration_id, from_version, to_version, config, metrics)
            elif strategy == MigrationStrategy.CANARY:
                await self._execute_canary_migration(migration_id, from_version, to_version, config, metrics)
            elif strategy == MigrationStrategy.ROLLING:
                await self._execute_rolling_migration(migration_id, from_version, to_version, config, metrics)
            elif strategy == MigrationStrategy.SHADOW:
                await self._execute_shadow_migration(migration_id, from_version, to_version, config, metrics)
            else:
                await self._execute_immediate_migration(migration_id, from_version, to_version, config, metrics)

            metrics.end_time = datetime.utcnow()
            logger.info(f"Migration {migration_id} completed successfully")

        except Exception as e:
            logger.error(f"Migration {migration_id} failed: {str(e)}")
            metrics.end_time = datetime.utcnow()
            await self.rollback_migration(migration_id)
            raise

        return metrics

    async def _execute_blue_green_migration(self, migration_id: str, from_version: str,
                                          to_version: str, config: Dict[str, Any],
                                          metrics: MigrationMetrics):
        """Execute blue-green deployment migration."""
        logger.info(f"Executing blue-green migration for {migration_id}")

        # Deploy new version (green) alongside current version (blue)
        await self._deploy_version(to_version, config)

        # Run validation tests on green deployment
        validation_passed = await self._validate_green_deployment(to_version, config)

        if validation_passed:
            # Switch traffic to green deployment
            await self._switch_traffic(from_version, to_version)
            metrics.successful_migrations += 1
        else:
            # Rollback to blue deployment
            await self._cleanup_green_deployment(to_version)
            metrics.failed_migrations += 1
            raise ValueError("Green deployment validation failed")

    async def _execute_canary_migration(self, migration_id: str, from_version: str,
                                      to_version: str, config: Dict[str, Any],
                                      metrics: MigrationMetrics):
        """Execute canary deployment migration."""
        logger.info(f"Executing canary migration for {migration_id}")

        # Start with 5% traffic to new version
        traffic_percentages = [5, 10, 25, 50, 100]

        for percentage in traffic_percentages:
            await self._route_traffic_percentage(to_version, percentage)

            # Monitor for issues
            await asyncio.sleep(60)  # Wait 1 minute between stages

            if await self._monitor_canary_health(to_version):
                logger.info(f"Canary stage {percentage}% successful")
                metrics.successful_migrations += 1
            else:
                logger.error(f"Canary stage {percentage}% failed")
                await self._route_traffic_percentage(from_version, 100)
                metrics.failed_migrations += 1
                raise ValueError(f"Canary deployment failed at {percentage}%")

    async def _execute_rolling_migration(self, migration_id: str, from_version: str,
                                       to_version: str, config: Dict[str, Any],
                                       metrics: MigrationMetrics):
        """Execute rolling deployment migration."""
        logger.info(f"Executing rolling migration for {migration_id}")

        # Update instances one by one
        instance_count = config.get("instance_count", 3)

        for i in range(instance_count):
            await self._update_instance(i, to_version, config)
            await self._health_check_instance(i)
            await asyncio.sleep(30)  # Wait between instance updates
            metrics.successful_migrations += 1

    async def _execute_shadow_migration(self, migration_id: str, from_version: str,
                                      to_version: str, config: Dict[str, Any],
                                      metrics: MigrationMetrics):
        """Execute shadow deployment migration."""
        logger.info(f"Executing shadow migration for {migration_id}")

        # Deploy shadow version that receives copy of production traffic
        await self._deploy_shadow_version(to_version, config)

        # Mirror production traffic for testing
        duration_minutes = config.get("shadow_duration", 60)
        await self._mirror_traffic(from_version, to_version, duration_minutes)

        # Analyze shadow results
        shadow_metrics = await self._analyze_shadow_results(to_version)

        if shadow_metrics["error_rate"] < 0.01:  # Less than 1% error rate
            await self._promote_shadow_to_production(to_version)
            metrics.successful_migrations += 1
        else:
            await self._cleanup_shadow_deployment(to_version)
            metrics.failed_migrations += 1
            raise ValueError("Shadow deployment showed high error rate")

    async def _execute_immediate_migration(self, migration_id: str, from_version: str,
                                         to_version: str, config: Dict[str, Any],
                                         metrics: MigrationMetrics):
        """Execute immediate migration (for urgent fixes)."""
        logger.info(f"Executing immediate migration for {migration_id}")

        # Stop old version and start new version immediately
        await self._stop_version(from_version)
        await self._start_version(to_version, config)

        # Quick health check
        if await self._quick_health_check(to_version):
            metrics.successful_migrations += 1
        else:
            await self._emergency_rollback(from_version)
            metrics.failed_migrations += 1
            raise ValueError("Immediate migration health check failed")

    async def rollback_migration(self, migration_id: str) -> bool:
        """Rollback a migration to previous version."""
        if migration_id not in self.rollback_snapshots:
            logger.error(f"No rollback snapshot found for migration {migration_id}")
            return False

        snapshot = self.rollback_snapshots[migration_id]
        logger.info(f"Rolling back migration {migration_id} to version {snapshot['version']}")

        try:
            # Restore previous version
            await self._restore_version(snapshot["version"], snapshot["config"])

            # Update metrics
            if migration_id in self.active_migrations:
                self.active_migrations[migration_id].rollback_count += 1

            logger.info(f"Migration {migration_id} rolled back successfully")
            return True

        except Exception as e:
            logger.error(f"Rollback failed for migration {migration_id}: {str(e)}")
            return False

    # Placeholder methods for deployment operations
    # In a real implementation, these would interface with deployment systems

    async def _deploy_version(self, version: str, config: Dict[str, Any]):
        """Deploy a new API version."""
        logger.info(f"Deploying version {version}")
        await asyncio.sleep(1)  # Simulate deployment time

    async def _validate_green_deployment(self, version: str, config: Dict[str, Any]) -> bool:
        """Validate green deployment in blue-green strategy."""
        logger.info(f"Validating green deployment for version {version}")
        await asyncio.sleep(2)  # Simulate validation time
        return True  # Simplified - would run actual tests

    async def _switch_traffic(self, from_version: str, to_version: str):
        """Switch traffic between versions."""
        logger.info(f"Switching traffic from {from_version} to {to_version}")
        await asyncio.sleep(1)

    async def _cleanup_green_deployment(self, version: str):
        """Cleanup failed green deployment."""
        logger.info(f"Cleaning up green deployment for version {version}")

    async def _route_traffic_percentage(self, version: str, percentage: int):
        """Route percentage of traffic to specific version."""
        logger.info(f"Routing {percentage}% traffic to version {version}")

    async def _monitor_canary_health(self, version: str) -> bool:
        """Monitor canary deployment health."""
        logger.info(f"Monitoring canary health for version {version}")
        await asyncio.sleep(1)
        return True  # Simplified health check

    async def _update_instance(self, instance_id: int, version: str, config: Dict[str, Any]):
        """Update specific instance to new version."""
        logger.info(f"Updating instance {instance_id} to version {version}")

    async def _health_check_instance(self, instance_id: int):
        """Health check for specific instance."""
        logger.info(f"Health checking instance {instance_id}")

    async def _deploy_shadow_version(self, version: str, config: Dict[str, Any]):
        """Deploy shadow version for testing."""
        logger.info(f"Deploying shadow version {version}")

    async def _mirror_traffic(self, from_version: str, to_version: str, duration_minutes: int):
        """Mirror traffic to shadow deployment."""
        logger.info(f"Mirroring traffic from {from_version} to {to_version} for {duration_minutes} minutes")
        await asyncio.sleep(duration_minutes * 0.1)  # Simulate shorter duration for demo

    async def _analyze_shadow_results(self, version: str) -> Dict[str, float]:
        """Analyze shadow deployment results."""
        logger.info(f"Analyzing shadow results for version {version}")
        return {"error_rate": 0.005, "response_time": 120}  # Simulated metrics

    async def _promote_shadow_to_production(self, version: str):
        """Promote shadow deployment to production."""
        logger.info(f"Promoting shadow version {version} to production")

    async def _cleanup_shadow_deployment(self, version: str):
        """Cleanup shadow deployment."""
        logger.info(f"Cleaning up shadow deployment for version {version}")

    async def _stop_version(self, version: str):
        """Stop API version."""
        logger.info(f"Stopping version {version}")

    async def _start_version(self, version: str, config: Dict[str, Any]):
        """Start API version."""
        logger.info(f"Starting version {version}")

    async def _quick_health_check(self, version: str) -> bool:
        """Quick health check for immediate migration."""
        logger.info(f"Quick health check for version {version}")
        return True

    async def _emergency_rollback(self, version: str):
        """Emergency rollback for immediate migration."""
        logger.info(f"Emergency rollback to version {version}")

    async def _restore_version(self, version: str, config: Dict[str, Any]):
        """Restore previous version."""
        logger.info(f"Restoring version {version}")


class MigrationOrchestrator:
    """High-level orchestrator for API migrations."""

    def __init__(self):
        self.transformer = SalesAPITransformer()
        self.validator = MigrationValidator()
        self.executor = MigrationExecutor(self.transformer, self.validator)
        self.migration_history: List[Dict[str, Any]] = []

    async def plan_migration(self, from_version: str, to_version: str,
                           strategy: MigrationStrategy = MigrationStrategy.BLUE_GREEN) -> Dict[str, Any]:
        """Plan a migration between API versions."""

        plan = {
            "migration_id": f"migration_{from_version}_to_{to_version}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "from_version": from_version,
            "to_version": to_version,
            "strategy": strategy.value,
            "created_at": datetime.utcnow().isoformat(),
            "config": {
                "field_mappings": {
                    "invoice_date": "transaction_date",
                    "customer_country": "country_code"
                },
                "field_transformations": {
                    "date_format": "iso8601",
                    "currency": "normalized"
                },
                "breaking_changes": [
                    "Date format changed to ISO 8601",
                    "Country field renamed to country_code"
                ],
                "estimated_duration": "30 minutes",
                "rollback_plan": "Automatic rollback on failure"
            }
        }

        # Validate the plan
        validation_issues = await self.validator.validate_migration_plan(
            from_version, to_version, plan["config"]
        )

        plan["validation_issues"] = [
            {
                "severity": issue.severity.value,
                "category": issue.category,
                "description": issue.description,
                "field_path": issue.field_path,
                "suggestion": issue.suggestion
            }
            for issue in validation_issues
        ]

        # Check if migration can proceed
        critical_issues = [issue for issue in validation_issues
                          if issue.severity == ValidationSeverity.CRITICAL]
        plan["can_proceed"] = len(critical_issues) == 0

        return plan

    async def execute_migration_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a migration plan."""
        migration_id = plan["migration_id"]
        from_version = plan["from_version"]
        to_version = plan["to_version"]
        strategy = MigrationStrategy(plan["strategy"])

        logger.info(f"Executing migration plan {migration_id}")

        try:
            metrics = await self.executor.execute_migration(
                migration_id, from_version, to_version, strategy, plan["config"]
            )

            # Record migration in history
            migration_record = {
                "migration_id": migration_id,
                "from_version": from_version,
                "to_version": to_version,
                "strategy": strategy.value,
                "status": MigrationStatus.COMPLETED.value,
                "started_at": metrics.start_time.isoformat(),
                "completed_at": metrics.end_time.isoformat() if metrics.end_time else None,
                "metrics": {
                    "total_requests": metrics.total_requests,
                    "successful_migrations": metrics.successful_migrations,
                    "failed_migrations": metrics.failed_migrations,
                    "error_rate": metrics.error_rate,
                    "avg_response_time": metrics.avg_response_time
                }
            }

            self.migration_history.append(migration_record)

            return {
                "status": "success",
                "migration_id": migration_id,
                "metrics": migration_record["metrics"]
            }

        except Exception as e:
            # Record failed migration
            migration_record = {
                "migration_id": migration_id,
                "from_version": from_version,
                "to_version": to_version,
                "strategy": strategy.value,
                "status": MigrationStatus.FAILED.value,
                "error": str(e),
                "started_at": datetime.utcnow().isoformat()
            }

            self.migration_history.append(migration_record)

            return {
                "status": "failed",
                "migration_id": migration_id,
                "error": str(e)
            }

    async def get_migration_status(self, migration_id: str) -> Dict[str, Any]:
        """Get status of a specific migration."""
        for migration in self.migration_history:
            if migration["migration_id"] == migration_id:
                return migration

        # Check active migrations
        if migration_id in self.executor.active_migrations:
            metrics = self.executor.active_migrations[migration_id]
            return {
                "migration_id": migration_id,
                "status": MigrationStatus.RUNNING.value,
                "started_at": metrics.start_time.isoformat(),
                "metrics": {
                    "total_requests": metrics.total_requests,
                    "successful_migrations": metrics.successful_migrations,
                    "failed_migrations": metrics.failed_migrations,
                    "error_rate": metrics.error_rate
                }
            }

        return {"error": f"Migration {migration_id} not found"}

    async def rollback_migration(self, migration_id: str) -> Dict[str, Any]:
        """Rollback a specific migration."""
        success = await self.executor.rollback_migration(migration_id)

        if success:
            # Update migration history
            for migration in self.migration_history:
                if migration["migration_id"] == migration_id:
                    migration["status"] = MigrationStatus.ROLLED_BACK.value
                    migration["rolled_back_at"] = datetime.utcnow().isoformat()
                    break

            return {
                "status": "success",
                "migration_id": migration_id,
                "message": "Migration rolled back successfully"
            }
        else:
            return {
                "status": "failed",
                "migration_id": migration_id,
                "error": "Rollback failed"
            }

    def get_migration_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get migration history."""
        return sorted(
            self.migration_history,
            key=lambda x: x.get("started_at", ""),
            reverse=True
        )[:limit]


# Global migration orchestrator instance
migration_orchestrator = MigrationOrchestrator()