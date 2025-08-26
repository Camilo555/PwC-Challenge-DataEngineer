"""
Backup Scheduling and Retention Management

Enterprise backup scheduling system with retention policies, automated cleanup,
and compliance monitoring.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

try:
    import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False
    croniter = None

from core.logging import get_logger

logger = get_logger(__name__)


class ScheduleType(str, Enum):
    """Types of backup schedules."""
    CRON = "cron"
    INTERVAL = "interval"
    MANUAL = "manual"


class RetentionUnit(str, Enum):
    """Units for retention policies."""
    DAYS = "days"
    WEEKS = "weeks"
    MONTHS = "months"
    YEARS = "years"


class BackupPriority(str, Enum):
    """Backup priority levels."""
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


@dataclass
class RetentionRule:
    """Individual retention rule."""
    name: str
    count: int
    unit: RetentionUnit
    backup_types: list[str] = field(default_factory=lambda: ["full", "incremental"])
    components: list[str] = field(default_factory=lambda: ["database", "data_lake", "configuration"])

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'count': self.count,
            'unit': self.unit.value,
            'backup_types': self.backup_types,
            'components': self.components
        }

    def calculate_expiry_date(self, backup_date: datetime) -> datetime:
        """Calculate when a backup expires based on this rule."""
        if self.unit == RetentionUnit.DAYS:
            return backup_date + timedelta(days=self.count)
        elif self.unit == RetentionUnit.WEEKS:
            return backup_date + timedelta(weeks=self.count)
        elif self.unit == RetentionUnit.MONTHS:
            return backup_date + timedelta(days=self.count * 30)  # Approximate
        elif self.unit == RetentionUnit.YEARS:
            return backup_date + timedelta(days=self.count * 365)  # Approximate
        else:
            return backup_date + timedelta(days=self.count)


@dataclass
class RetentionPolicy:
    """
    Comprehensive retention policy with multiple rules.
    
    Implements grandfather-father-son (GFS) retention strategy and custom rules.
    """
    policy_id: str
    name: str
    description: str
    rules: list[RetentionRule]
    gfs_enabled: bool = True
    compliance_requirements: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            'policy_id': self.policy_id,
            'name': self.name,
            'description': self.description,
            'rules': [rule.to_dict() for rule in self.rules],
            'gfs_enabled': self.gfs_enabled,
            'compliance_requirements': self.compliance_requirements,
            'created_at': self.created_at.isoformat()
        }

    @classmethod
    def create_standard_policy(cls, policy_id: str, name: str) -> RetentionPolicy:
        """Create standard 3-2-1 backup retention policy."""
        rules = [
            RetentionRule("daily", 7, RetentionUnit.DAYS, ["full", "incremental"]),
            RetentionRule("weekly", 4, RetentionUnit.WEEKS, ["full"]),
            RetentionRule("monthly", 12, RetentionUnit.MONTHS, ["full"]),
            RetentionRule("yearly", 7, RetentionUnit.YEARS, ["full"])
        ]

        return cls(
            policy_id=policy_id,
            name=name,
            description="Standard 3-2-1 backup retention policy",
            rules=rules,
            gfs_enabled=True
        )

    @classmethod
    def create_compliance_policy(cls, policy_id: str, name: str, years: int = 7) -> RetentionPolicy:
        """Create compliance-focused retention policy."""
        rules = [
            RetentionRule("daily", 30, RetentionUnit.DAYS, ["full", "incremental"]),
            RetentionRule("monthly", 12, RetentionUnit.MONTHS, ["full"]),
            RetentionRule("yearly", years, RetentionUnit.YEARS, ["full"])
        ]

        compliance_requirements = {
            "regulatory_framework": "SOX/GDPR",
            "audit_trail_required": True,
            "immutable_storage": True,
            "retention_years": years
        }

        return cls(
            policy_id=policy_id,
            name=name,
            description=f"Compliance retention policy ({years} years)",
            rules=rules,
            gfs_enabled=True,
            compliance_requirements=compliance_requirements
        )


@dataclass
class BackupSchedule:
    """Backup schedule configuration."""
    schedule_id: str
    name: str
    component: str
    backup_type: str
    schedule_type: ScheduleType
    schedule_expression: str  # Cron expression or interval
    retention_policy_id: str
    priority: BackupPriority = BackupPriority.NORMAL
    enabled: bool = True
    next_run: datetime | None = None
    last_run: datetime | None = None
    tags: dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            'schedule_id': self.schedule_id,
            'name': self.name,
            'component': self.component,
            'backup_type': self.backup_type,
            'schedule_type': self.schedule_type.value,
            'schedule_expression': self.schedule_expression,
            'retention_policy_id': self.retention_policy_id,
            'priority': self.priority.value,
            'enabled': self.enabled,
            'next_run': self.next_run.isoformat() if self.next_run else None,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'tags': self.tags,
            'created_at': self.created_at.isoformat()
        }


class BackupScheduler:
    """
    Enterprise backup scheduler with retention management.
    
    Features:
    - Cron-based scheduling
    - Interval-based scheduling
    - Priority-based execution
    - Retention policy enforcement
    - Compliance monitoring
    - Performance optimization
    """

    def __init__(self, backup_manager, config: dict[str, Any] | None = None):
        from .backup_manager import BackupManager
        self.backup_manager: BackupManager = backup_manager
        self.config = config or {}

        # Storage for schedules and policies
        self.schedules: dict[str, BackupSchedule] = {}
        self.retention_policies: dict[str, RetentionPolicy] = {}

        # Runtime state
        self._scheduler_running = False
        self._scheduler_task: asyncio.Task | None = None
        self._next_cleanup: datetime | None = None

        # Configuration
        self.max_concurrent_backups = self.config.get("max_concurrent_backups", 3)
        self.scheduler_interval = self.config.get("scheduler_interval_seconds", 60)
        self.cleanup_interval_hours = self.config.get("cleanup_interval_hours", 24)

        # Initialize default policies
        self._create_default_policies()

        logger.info("Backup scheduler initialized")

    def _create_default_policies(self) -> None:
        """Create default retention policies."""
        # Standard policy
        standard_policy = RetentionPolicy.create_standard_policy(
            "standard_3_2_1",
            "Standard 3-2-1 Policy"
        )
        self.retention_policies[standard_policy.policy_id] = standard_policy

        # Compliance policy
        compliance_policy = RetentionPolicy.create_compliance_policy(
            "compliance_7_year",
            "7-Year Compliance Policy"
        )
        self.retention_policies[compliance_policy.policy_id] = compliance_policy

        # Short-term policy for testing
        test_rules = [
            RetentionRule("hourly", 24, RetentionUnit.DAYS, ["full", "incremental"]),
            RetentionRule("daily", 7, RetentionUnit.DAYS, ["full"])
        ]

        test_policy = RetentionPolicy(
            policy_id="test_short_term",
            name="Test Short-Term Policy",
            description="Short retention for testing and development",
            rules=test_rules,
            gfs_enabled=False
        )
        self.retention_policies[test_policy.policy_id] = test_policy

    def create_schedule(
        self,
        name: str,
        component: str,
        backup_type: str,
        schedule_expression: str,
        retention_policy_id: str,
        priority: BackupPriority = BackupPriority.NORMAL,
        tags: dict[str, str] | None = None
    ) -> BackupSchedule:
        """
        Create a new backup schedule.
        
        Args:
            name: Schedule name
            component: Component to backup (database, data_lake, configuration)
            backup_type: Type of backup (full, incremental, differential)
            schedule_expression: Cron expression or interval string
            retention_policy_id: ID of retention policy to apply
            priority: Backup priority level
            tags: Additional metadata tags
            
        Returns:
            Created backup schedule
        """
        schedule_id = f"schedule_{len(self.schedules) + 1}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        # Determine schedule type
        if schedule_expression.startswith("@") or " " in schedule_expression:
            schedule_type = ScheduleType.CRON
        else:
            schedule_type = ScheduleType.INTERVAL

        # Calculate next run time
        next_run = self._calculate_next_run(schedule_expression, schedule_type)

        schedule = BackupSchedule(
            schedule_id=schedule_id,
            name=name,
            component=component,
            backup_type=backup_type,
            schedule_type=schedule_type,
            schedule_expression=schedule_expression,
            retention_policy_id=retention_policy_id,
            priority=priority,
            next_run=next_run,
            tags=tags or {}
        )

        self.schedules[schedule_id] = schedule

        logger.info(f"Created backup schedule: {name} ({schedule_id})")
        logger.info(f"Next run: {next_run}")

        return schedule

    def create_standard_schedules(self) -> list[BackupSchedule]:
        """Create standard backup schedules for all components."""
        schedules = []

        # Database backups
        schedules.append(self.create_schedule(
            "Database Daily Full Backup",
            "database",
            "full",
            "0 2 * * *",  # Daily at 2 AM
            "standard_3_2_1",
            BackupPriority.HIGH,
            {"frequency": "daily", "time": "02:00"}
        ))

        schedules.append(self.create_schedule(
            "Database Hourly Incremental",
            "database",
            "incremental",
            "0 * * * *",  # Every hour
            "test_short_term",
            BackupPriority.NORMAL,
            {"frequency": "hourly"}
        ))

        # Data lake backups
        schedules.append(self.create_schedule(
            "Data Lake Weekly Full Backup",
            "data_lake",
            "full",
            "0 3 * * 0",  # Weekly on Sunday at 3 AM
            "standard_3_2_1",
            BackupPriority.NORMAL,
            {"frequency": "weekly", "day": "sunday"}
        ))

        # Configuration backups
        schedules.append(self.create_schedule(
            "Configuration Daily Backup",
            "configuration",
            "full",
            "0 1 * * *",  # Daily at 1 AM
            "standard_3_2_1",
            BackupPriority.LOW,
            {"frequency": "daily", "time": "01:00"}
        ))

        logger.info(f"Created {len(schedules)} standard backup schedules")
        return schedules

    async def start_scheduler(self) -> None:
        """Start the backup scheduler."""
        if self._scheduler_running:
            logger.warning("Scheduler is already running")
            return

        self._scheduler_running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._next_cleanup = datetime.utcnow() + timedelta(hours=self.cleanup_interval_hours)

        logger.info("Backup scheduler started")

    async def stop_scheduler(self) -> None:
        """Stop the backup scheduler."""
        if not self._scheduler_running:
            return

        self._scheduler_running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        logger.info("Backup scheduler stopped")

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._scheduler_running:
            try:
                current_time = datetime.utcnow()

                # Check for due backups
                due_schedules = []
                for schedule in self.schedules.values():
                    if (schedule.enabled and
                        schedule.next_run and
                        current_time >= schedule.next_run):
                        due_schedules.append(schedule)

                # Sort by priority and execute
                if due_schedules:
                    due_schedules.sort(key=lambda s: (
                        s.priority.value,
                        s.next_run or datetime.min
                    ))

                    await self._execute_scheduled_backups(due_schedules[:self.max_concurrent_backups])

                # Check for cleanup
                if self._next_cleanup and current_time >= self._next_cleanup:
                    await self._run_retention_cleanup()
                    self._next_cleanup = current_time + timedelta(hours=self.cleanup_interval_hours)

                # Wait for next iteration
                await asyncio.sleep(self.scheduler_interval)

            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(self.scheduler_interval)

    async def _execute_scheduled_backups(self, schedules: list[BackupSchedule]) -> None:
        """Execute scheduled backups."""
        tasks = []

        for schedule in schedules:
            task = asyncio.create_task(self._execute_single_backup(schedule))
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                schedule = schedules[i]

                if isinstance(result, Exception):
                    logger.error(f"Scheduled backup failed: {schedule.name} - {result}")
                else:
                    logger.info(f"Scheduled backup completed: {schedule.name}")

                # Update schedule
                schedule.last_run = datetime.utcnow()
                schedule.next_run = self._calculate_next_run(
                    schedule.schedule_expression,
                    schedule.schedule_type
                )

    async def _execute_single_backup(self, schedule: BackupSchedule) -> Any:
        """Execute a single scheduled backup."""
        logger.info(f"Executing scheduled backup: {schedule.name}")

        try:
            if schedule.component == "database":
                from .backup_manager import BackupType
                backup_type = BackupType(schedule.backup_type)
                result = await self.backup_manager.backup_database(
                    backup_type=backup_type,
                    compress=True,
                    validate=True
                )

            elif schedule.component == "data_lake":
                from .backup_manager import BackupType
                backup_type = BackupType(schedule.backup_type)
                result = await self.backup_manager.backup_data_lake(
                    backup_type=backup_type,
                    compress=True
                )

            elif schedule.component == "configuration":
                result = await self.backup_manager.backup_configuration(compress=True)

            else:
                raise ValueError(f"Unknown component: {schedule.component}")

            # Apply retention policy
            policy = self.retention_policies.get(schedule.retention_policy_id)
            if policy:
                await self._apply_retention_policy(schedule.component, policy)

            return result

        except Exception as e:
            logger.error(f"Backup execution failed: {schedule.name} - {e}")
            raise

    async def _run_retention_cleanup(self) -> None:
        """Run retention policy cleanup."""
        logger.info("Starting retention cleanup")

        cleanup_stats = {
            "policies_processed": 0,
            "backups_evaluated": 0,
            "backups_deleted": 0,
            "storage_freed_bytes": 0
        }

        try:
            for policy in self.retention_policies.values():
                cleanup_stats["policies_processed"] += 1
                policy_stats = await self._apply_retention_policy(None, policy)

                cleanup_stats["backups_evaluated"] += policy_stats.get("backups_evaluated", 0)
                cleanup_stats["backups_deleted"] += policy_stats.get("backups_deleted", 0)
                cleanup_stats["storage_freed_bytes"] += policy_stats.get("storage_freed_bytes", 0)

            logger.info(f"Retention cleanup completed: {cleanup_stats}")

        except Exception as e:
            logger.error(f"Retention cleanup failed: {e}")

    async def _apply_retention_policy(
        self,
        component: str | None,
        policy: RetentionPolicy
    ) -> dict[str, Any]:
        """Apply retention policy to backups."""
        stats = {
            "backups_evaluated": 0,
            "backups_deleted": 0,
            "storage_freed_bytes": 0
        }

        try:
            # Get backup history
            backup_history = self.backup_manager.get_backup_history(
                component=component,
                limit=1000  # Process up to 1000 backups
            )

            current_time = datetime.utcnow()
            backups_to_delete = []

            for backup in backup_history:
                stats["backups_evaluated"] += 1
                backup_date = datetime.fromisoformat(backup["timestamp"])
                backup_component = backup.get("tags", {}).get("component", "unknown")
                backup_type = backup.get("backup_type", "unknown")

                # Check each retention rule
                should_retain = False

                for rule in policy.rules:
                    if (backup_component in rule.components and
                        backup_type in rule.backup_types):

                        expiry_date = rule.calculate_expiry_date(backup_date)
                        if current_time < expiry_date:
                            should_retain = True
                            break

                if not should_retain:
                    backups_to_delete.append(backup)

            # Delete expired backups
            for backup in backups_to_delete:
                try:
                    backup_path = Path(backup["destination_path"])
                    if backup_path.exists():
                        size = backup.get("size_bytes", 0)

                        if backup_path.is_file():
                            backup_path.unlink()
                        else:
                            import shutil
                            shutil.rmtree(backup_path)

                        stats["backups_deleted"] += 1
                        stats["storage_freed_bytes"] += size

                        logger.debug(f"Deleted expired backup: {backup['backup_id']}")

                except Exception as e:
                    logger.warning(f"Could not delete backup {backup['backup_id']}: {e}")

        except Exception as e:
            logger.error(f"Retention policy application failed: {e}")

        return stats

    def _calculate_next_run(self, schedule_expression: str, schedule_type: ScheduleType) -> datetime:
        """Calculate next run time for a schedule."""
        current_time = datetime.utcnow()

        if schedule_type == ScheduleType.CRON:
            if HAS_CRONITER:
                try:
                    cron = croniter.croniter(schedule_expression, current_time)
                    return cron.get_next(datetime)
                except Exception as e:
                    logger.error(f"Invalid cron expression '{schedule_expression}': {e}")
                    # Fallback to daily
                    return current_time + timedelta(days=1)
            else:
                logger.warning("croniter not available, falling back to simple scheduling")
                # Simple fallback: parse basic cron patterns
                if schedule_expression.startswith("0 "):  # Hourly patterns like "0 2 * * *"
                    parts = schedule_expression.split()
                    if len(parts) >= 5:
                        try:
                            hour = int(parts[1]) if parts[1] != "*" else current_time.hour
                            # Schedule for next occurrence at specified hour
                            next_run = current_time.replace(hour=hour, minute=0, second=0, microsecond=0)
                            if next_run <= current_time:
                                next_run += timedelta(days=1)
                            return next_run
                        except ValueError:
                            pass
                # Fallback to daily
                return current_time + timedelta(days=1)

        elif schedule_type == ScheduleType.INTERVAL:
            # Parse interval string like "1h", "30m", "2d"
            try:
                if schedule_expression.endswith('m'):
                    minutes = int(schedule_expression[:-1])
                    return current_time + timedelta(minutes=minutes)
                elif schedule_expression.endswith('h'):
                    hours = int(schedule_expression[:-1])
                    return current_time + timedelta(hours=hours)
                elif schedule_expression.endswith('d'):
                    days = int(schedule_expression[:-1])
                    return current_time + timedelta(days=days)
                else:
                    # Default to minutes
                    minutes = int(schedule_expression)
                    return current_time + timedelta(minutes=minutes)

            except ValueError as e:
                logger.error(f"Invalid interval expression '{schedule_expression}': {e}")
                return current_time + timedelta(hours=1)

        else:
            return current_time + timedelta(days=1)

    def get_schedule_status(self) -> dict[str, Any]:
        """Get scheduler status and statistics."""
        current_time = datetime.utcnow()

        status = {
            "scheduler_running": self._scheduler_running,
            "total_schedules": len(self.schedules),
            "enabled_schedules": sum(1 for s in self.schedules.values() if s.enabled),
            "next_cleanup": self._next_cleanup.isoformat() if self._next_cleanup else None,
            "schedules": [],
            "retention_policies": len(self.retention_policies)
        }

        # Add individual schedule information
        for schedule in self.schedules.values():
            schedule_info = {
                "schedule_id": schedule.schedule_id,
                "name": schedule.name,
                "component": schedule.component,
                "enabled": schedule.enabled,
                "next_run": schedule.next_run.isoformat() if schedule.next_run else None,
                "last_run": schedule.last_run.isoformat() if schedule.last_run else None,
                "overdue": (schedule.next_run and current_time > schedule.next_run) if schedule.enabled else False
            }
            status["schedules"].append(schedule_info)

        return status

    def get_retention_policy(self, policy_id: str) -> RetentionPolicy | None:
        """Get retention policy by ID."""
        return self.retention_policies.get(policy_id)

    def list_retention_policies(self) -> list[dict[str, Any]]:
        """List all retention policies."""
        return [policy.to_dict() for policy in self.retention_policies.values()]

    def create_retention_policy(
        self,
        policy_id: str,
        name: str,
        description: str,
        rules: list[RetentionRule]
    ) -> RetentionPolicy:
        """Create a custom retention policy."""
        policy = RetentionPolicy(
            policy_id=policy_id,
            name=name,
            description=description,
            rules=rules
        )

        self.retention_policies[policy_id] = policy
        logger.info(f"Created retention policy: {name} ({policy_id})")

        return policy

    async def trigger_manual_backup(
        self,
        component: str,
        backup_type: str = "full",
        retention_policy_id: str | None = None
    ) -> Any:
        """Trigger a manual backup outside of scheduling."""
        logger.info(f"Triggering manual backup: {component} ({backup_type})")

        try:
            if component == "database":
                from .backup_manager import BackupType
                backup_type_enum = BackupType(backup_type)
                result = await self.backup_manager.backup_database(
                    backup_type=backup_type_enum,
                    compress=True,
                    validate=True
                )

            elif component == "data_lake":
                from .backup_manager import BackupType
                backup_type_enum = BackupType(backup_type)
                result = await self.backup_manager.backup_data_lake(
                    backup_type=backup_type_enum,
                    compress=True
                )

            elif component == "configuration":
                result = await self.backup_manager.backup_configuration(compress=True)

            else:
                raise ValueError(f"Unknown component: {component}")

            # Apply retention policy if specified
            if retention_policy_id:
                policy = self.retention_policies.get(retention_policy_id)
                if policy:
                    await self._apply_retention_policy(component, policy)

            logger.info(f"Manual backup completed: {component}")
            return result

        except Exception as e:
            logger.error(f"Manual backup failed: {component} - {e}")
            raise
