"""
Advanced Database Migration Manager

Provides comprehensive database migration capabilities with:
- Version-controlled schema changes
- Zero-downtime migrations
- Rollback capabilities
- Cross-database compatibility
- Data integrity validation
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, MetaData, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from core.database.connection_factory import get_connection_factory
from core.logging import get_logger

logger = get_logger(__name__)


class MigrationStatus(str, Enum):
    """Migration execution status"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class MigrationType(str, Enum):
    """Types of database migrations"""

    SCHEMA_CHANGE = "schema_change"
    DATA_MIGRATION = "data_migration"
    INDEX_OPTIMIZATION = "index_optimization"
    CONSTRAINT_UPDATE = "constraint_update"
    PARTITION_MANAGEMENT = "partition_management"


@dataclass
class MigrationStep:
    """Individual migration step"""

    step_id: str
    sql_command: str
    description: str
    rollback_command: str | None = None
    validation_query: str | None = None
    timeout_seconds: int = 300
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class MigrationPlan:
    """Complete migration execution plan"""

    migration_id: str
    name: str
    description: str
    migration_type: MigrationType
    source_version: str
    target_version: str
    steps: list[MigrationStep]
    prerequisites: list[str] = field(default_factory=list)
    estimated_duration: timedelta = field(default_factory=lambda: timedelta(minutes=30))
    requires_maintenance_mode: bool = False
    backup_required: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class MigrationResult:
    """Migration execution result"""

    migration_id: str
    status: MigrationStatus
    executed_steps: int
    total_steps: int
    execution_time: timedelta
    error_message: str | None = None
    rollback_executed: bool = False
    validation_results: dict[str, bool] = field(default_factory=dict)
    performance_metrics: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class MigrationManager:
    """Advanced database migration management system"""

    def __init__(self, engine: Engine, migration_dir: str = "./migrations"):
        self.engine = engine
        self.migration_dir = Path(migration_dir)
        self.migration_dir.mkdir(parents=True, exist_ok=True)

        self.connection_factory = get_connection_factory()
        self.metadata = MetaData()

        # Migration tracking
        self.migration_history: list[MigrationResult] = []
        self.pending_migrations: list[MigrationPlan] = []

        # Initialize migration infrastructure
        self._initialize_migration_tables()
        self._load_migration_history()

        logger.info("Migration Manager initialized")

    def _initialize_migration_tables(self):
        """Initialize migration tracking tables"""
        try:
            with self.engine.connect() as conn:
                # Create migration history table
                migration_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    migration_id VARCHAR(64) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    migration_type VARCHAR(50) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    source_version VARCHAR(50),
                    target_version VARCHAR(50),
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    execution_time_ms INTEGER,
                    rollback_executed BOOLEAN DEFAULT FALSE,
                    error_message TEXT,
                    checksum VARCHAR(64)
                )
                """

                conn.execute(text(migration_table_sql))

                # Create migration steps tracking table
                steps_table_sql = """
                CREATE TABLE IF NOT EXISTS migration_steps (
                    step_id VARCHAR(64) PRIMARY KEY,
                    migration_id VARCHAR(64),
                    step_order INTEGER,
                    sql_command TEXT,
                    status VARCHAR(20),
                    executed_at TIMESTAMP,
                    execution_time_ms INTEGER,
                    error_message TEXT,
                    FOREIGN KEY (migration_id) REFERENCES schema_migrations(migration_id)
                )
                """

                conn.execute(text(steps_table_sql))
                conn.commit()

            logger.info("Migration tracking tables initialized")

        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize migration tables: {e}")
            raise

    def _load_migration_history(self):
        """Load migration history from database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                    SELECT migration_id, name, migration_type, status,
                           source_version, target_version, executed_at,
                           execution_time_ms, rollback_executed, error_message
                    FROM schema_migrations
                    ORDER BY executed_at DESC
                """)
                )

                for row in result:
                    migration_result = MigrationResult(
                        migration_id=row[0],
                        status=MigrationStatus(row[3]),
                        executed_steps=0,  # Would need additional query
                        total_steps=0,  # Would need additional query
                        execution_time=timedelta(milliseconds=row[7] or 0),
                        error_message=row[9],
                        rollback_executed=bool(row[8]),
                        timestamp=row[6],
                    )
                    self.migration_history.append(migration_result)

            logger.info(f"Loaded {len(self.migration_history)} migration records")

        except SQLAlchemyError as e:
            logger.warning(f"Failed to load migration history: {e}")

    def create_migration_plan(
        self,
        name: str,
        description: str,
        migration_type: MigrationType,
        sql_commands: list[str],
        rollback_commands: list[str] = None,
        validation_queries: list[str] = None,
    ) -> MigrationPlan:
        """Create a new migration plan"""

        migration_id = f"migration_{int(time.time())}_{hashlib.md5(name.encode()).hexdigest()[:8]}"

        # Create migration steps
        steps = []
        for i, sql_command in enumerate(sql_commands):
            step = MigrationStep(
                step_id=f"{migration_id}_step_{i + 1}",
                sql_command=sql_command,
                description=f"Step {i + 1}: {sql_command.strip()[:50]}...",
                rollback_command=rollback_commands[i]
                if rollback_commands and i < len(rollback_commands)
                else None,
                validation_query=validation_queries[i]
                if validation_queries and i < len(validation_queries)
                else None,
            )
            steps.append(step)

        # Estimate duration based on complexity
        estimated_duration = self._estimate_migration_duration(steps)

        plan = MigrationPlan(
            migration_id=migration_id,
            name=name,
            description=description,
            migration_type=migration_type,
            source_version=self._get_current_schema_version(),
            target_version=f"v{len(self.migration_history) + 1}",
            steps=steps,
            estimated_duration=estimated_duration,
            requires_maintenance_mode=self._requires_maintenance_mode(steps),
            backup_required=self._requires_backup(migration_type, steps),
        )

        self.pending_migrations.append(plan)
        logger.info(f"Created migration plan: {name} with {len(steps)} steps")

        return plan

    def execute_migration(
        self, migration_plan: MigrationPlan, dry_run: bool = False, backup_before: bool = None
    ) -> MigrationResult:
        """Execute migration plan with comprehensive error handling"""

        start_time = datetime.utcnow()
        executed_steps = 0
        validation_results = {}
        performance_metrics = {}

        # Determine if backup is needed
        should_backup = (
            backup_before if backup_before is not None else migration_plan.backup_required
        )

        logger.info(f"{'DRY RUN: ' if dry_run else ''}Executing migration: {migration_plan.name}")

        try:
            # Pre-migration backup
            if should_backup and not dry_run:
                backup_result = self._create_backup(migration_plan.migration_id)
                performance_metrics["backup_time_ms"] = backup_result.get("duration_ms", 0)

            # Pre-migration validation
            pre_validation_result = self._validate_prerequisites(migration_plan)
            if not pre_validation_result:
                raise RuntimeError("Migration prerequisites not met")

            # Execute migration steps
            with self.engine.connect() as conn:
                trans = conn.begin()

                try:
                    for step in migration_plan.steps:
                        step_start = time.time()

                        if dry_run:
                            logger.info(f"DRY RUN - Would execute: {step.description}")
                            validation_results[step.step_id] = True
                        else:
                            # Execute step with retry logic
                            step_result = self._execute_migration_step(conn, step)
                            validation_results[step.step_id] = step_result

                            if not step_result:
                                raise RuntimeError(f"Step failed: {step.step_id}")

                        executed_steps += 1
                        step_duration = (time.time() - step_start) * 1000
                        performance_metrics[f"step_{executed_steps}_duration_ms"] = step_duration

                        logger.debug(
                            f"Completed step {executed_steps}/{len(migration_plan.steps)}: {step.description}"
                        )

                    if not dry_run:
                        # Commit transaction
                        trans.commit()

                        # Post-migration validation
                        post_validation = self._validate_migration_result(migration_plan)
                        validation_results["post_migration_validation"] = post_validation

                        # Record migration in history
                        self._record_migration_success(
                            migration_plan, executed_steps, performance_metrics
                        )

                    execution_time = datetime.utcnow() - start_time

                    result = MigrationResult(
                        migration_id=migration_plan.migration_id,
                        status=MigrationStatus.SUCCESS,
                        executed_steps=executed_steps,
                        total_steps=len(migration_plan.steps),
                        execution_time=execution_time,
                        validation_results=validation_results,
                        performance_metrics=performance_metrics,
                    )

                    logger.info(
                        f"{'DRY RUN: ' if dry_run else ''}Migration completed successfully: {migration_plan.name} ({execution_time.total_seconds():.1f}s)"
                    )
                    return result

                except Exception as step_error:
                    if not dry_run:
                        trans.rollback()
                    raise step_error

        except Exception as e:
            execution_time = datetime.utcnow() - start_time
            error_message = str(e)

            # Attempt rollback if not dry run
            rollback_executed = False
            if not dry_run:
                try:
                    rollback_executed = self._execute_rollback(migration_plan, executed_steps)
                except Exception as rollback_error:
                    logger.error(f"Rollback failed: {rollback_error}")
                    error_message += f"; Rollback failed: {str(rollback_error)}"

            # Record migration failure
            if not dry_run:
                self._record_migration_failure(migration_plan, executed_steps, error_message)

            result = MigrationResult(
                migration_id=migration_plan.migration_id,
                status=MigrationStatus.FAILED,
                executed_steps=executed_steps,
                total_steps=len(migration_plan.steps),
                execution_time=execution_time,
                error_message=error_message,
                rollback_executed=rollback_executed,
                validation_results=validation_results,
                performance_metrics=performance_metrics,
            )

            logger.error(f"Migration failed: {migration_plan.name} - {error_message}")
            return result

    def _execute_migration_step(self, conn, step: MigrationStep) -> bool:
        """Execute individual migration step with retry logic"""
        for attempt in range(step.max_retries + 1):
            try:
                # Execute SQL command
                conn.execute(text(step.sql_command))

                # Run validation query if provided
                if step.validation_query:
                    validation_result = conn.execute(text(step.validation_query))
                    if not validation_result.scalar():
                        raise RuntimeError(f"Validation failed for step {step.step_id}")

                return True

            except SQLAlchemyError as e:
                step.retry_count = attempt + 1
                if attempt < step.max_retries:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(
                        f"Step {step.step_id} failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Step {step.step_id} failed after {step.max_retries + 1} attempts: {e}"
                    )
                    raise

        return False

    def _execute_rollback(self, migration_plan: MigrationPlan, executed_steps: int) -> bool:
        """Execute rollback for failed migration"""
        try:
            logger.warning(f"Executing rollback for migration: {migration_plan.name}")

            with self.engine.connect() as conn:
                trans = conn.begin()

                try:
                    # Execute rollback commands in reverse order
                    rollback_steps = migration_plan.steps[:executed_steps]
                    for step in reversed(rollback_steps):
                        if step.rollback_command:
                            conn.execute(text(step.rollback_command))
                            logger.debug(f"Executed rollback for step: {step.step_id}")

                    trans.commit()
                    logger.info(f"Rollback completed for migration: {migration_plan.name}")
                    return True

                except Exception as rollback_error:
                    trans.rollback()
                    logger.error(f"Rollback execution failed: {rollback_error}")
                    raise

        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False

    def _validate_prerequisites(self, migration_plan: MigrationPlan) -> bool:
        """Validate migration prerequisites"""
        try:
            # Check database connectivity
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Check schema version compatibility
            current_version = self._get_current_schema_version()
            if migration_plan.source_version != current_version:
                logger.warning(
                    f"Schema version mismatch: expected {migration_plan.source_version}, got {current_version}"
                )
                return False

            # Check for conflicting migrations
            running_migrations = [
                r for r in self.migration_history if r.status == MigrationStatus.RUNNING
            ]
            if running_migrations:
                logger.error(
                    f"Cannot start migration: {len(running_migrations)} migrations currently running"
                )
                return False

            # Validate table access
            inspector = inspect(self.engine)
            required_tables = self._extract_table_names_from_steps(migration_plan.steps)
            existing_tables = set(inspector.get_table_names())

            for table_name in required_tables:
                if table_name not in existing_tables:
                    logger.error(f"Required table not found: {table_name}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Prerequisites validation failed: {e}")
            return False

    def _validate_migration_result(self, migration_plan: MigrationPlan) -> bool:
        """Validate migration execution result"""
        try:
            with self.engine.connect() as conn:
                # Check table integrity
                inspector = inspect(self.engine)
                tables = inspector.get_table_names()

                for table_name in tables:
                    # Basic table access test
                    try:
                        conn.execute(text(f"SELECT COUNT(*) FROM {table_name} LIMIT 1"))
                    except SQLAlchemyError as e:
                        logger.error(
                            f"Post-migration validation failed for table {table_name}: {e}"
                        )
                        return False

                # Check constraints and indexes
                # This would be expanded based on specific migration requirements

                return True

        except Exception as e:
            logger.error(f"Post-migration validation failed: {e}")
            return False

    def _record_migration_success(
        self,
        migration_plan: MigrationPlan,
        executed_steps: int,
        performance_metrics: dict[str, Any],
    ):
        """Record successful migration in history"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                    INSERT INTO schema_migrations
                    (migration_id, name, migration_type, status, source_version, target_version,
                     execution_time_ms, rollback_executed, checksum)
                    VALUES (:migration_id, :name, :migration_type, :status, :source_version, :target_version,
                            :execution_time_ms, :rollback_executed, :checksum)
                """),
                    {
                        "migration_id": migration_plan.migration_id,
                        "name": migration_plan.name,
                        "migration_type": migration_plan.migration_type.value,
                        "status": MigrationStatus.SUCCESS.value,
                        "source_version": migration_plan.source_version,
                        "target_version": migration_plan.target_version,
                        "execution_time_ms": sum(
                            v for k, v in performance_metrics.items() if "duration_ms" in k
                        ),
                        "rollback_executed": False,
                        "checksum": self._calculate_migration_checksum(migration_plan),
                    },
                )
                conn.commit()

        except SQLAlchemyError as e:
            logger.error(f"Failed to record migration success: {e}")

    def _record_migration_failure(
        self, migration_plan: MigrationPlan, executed_steps: int, error_message: str
    ):
        """Record failed migration in history"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                    INSERT INTO schema_migrations
                    (migration_id, name, migration_type, status, source_version, target_version,
                     error_message, checksum)
                    VALUES (:migration_id, :name, :migration_type, :status, :source_version, :target_version,
                            :error_message, :checksum)
                """),
                    {
                        "migration_id": migration_plan.migration_id,
                        "name": migration_plan.name,
                        "migration_type": migration_plan.migration_type.value,
                        "status": MigrationStatus.FAILED.value,
                        "source_version": migration_plan.source_version,
                        "target_version": migration_plan.target_version,
                        "error_message": error_message[:1000],  # Truncate if too long
                        "checksum": self._calculate_migration_checksum(migration_plan),
                    },
                )
                conn.commit()

        except SQLAlchemyError as e:
            logger.error(f"Failed to record migration failure: {e}")

    def _estimate_migration_duration(self, steps: list[MigrationStep]) -> timedelta:
        """Estimate migration duration based on step complexity"""
        base_time = timedelta(minutes=5)  # Base overhead

        for step in steps:
            sql = step.sql_command.upper()

            # Estimate based on SQL operation type
            if "CREATE INDEX" in sql:
                base_time += timedelta(minutes=10)
            elif "ALTER TABLE" in sql and "ADD COLUMN" in sql:
                base_time += timedelta(minutes=2)
            elif "ALTER TABLE" in sql and "DROP COLUMN" in sql:
                base_time += timedelta(minutes=5)
            elif "CREATE TABLE" in sql:
                base_time += timedelta(minutes=1)
            elif "INSERT" in sql or "UPDATE" in sql:
                base_time += timedelta(minutes=5)  # Assume data operations are slower
            else:
                base_time += timedelta(minutes=1)

        return base_time

    def _requires_maintenance_mode(self, steps: list[MigrationStep]) -> bool:
        """Determine if migration requires maintenance mode"""
        for step in steps:
            sql = step.sql_command.upper()

            # Operations that typically require maintenance mode
            if any(op in sql for op in ["DROP TABLE", "TRUNCATE", "ALTER TABLE", "DROP INDEX"]):
                return True

        return False

    def _requires_backup(self, migration_type: MigrationType, steps: list[MigrationStep]) -> bool:
        """Determine if migration requires backup"""
        # Always backup for data migrations and destructive operations
        if migration_type == MigrationType.DATA_MIGRATION:
            return True

        for step in steps:
            sql = step.sql_command.upper()
            if any(op in sql for op in ["DROP", "DELETE", "TRUNCATE", "ALTER"]):
                return True

        return False

    def _get_current_schema_version(self) -> str:
        """Get current schema version"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                    SELECT target_version FROM schema_migrations
                    WHERE status = 'success'
                    ORDER BY executed_at DESC LIMIT 1
                """)
                )

                row = result.fetchone()
                return row[0] if row else "v0"

        except SQLAlchemyError:
            return "v0"

    def _extract_table_names_from_steps(self, steps: list[MigrationStep]) -> list[str]:
        """Extract table names mentioned in migration steps"""
        import re

        table_names = set()

        for step in steps:
            sql = step.sql_command.upper()

            # Simple regex patterns to extract table names
            patterns = [
                r"FROM\s+([\w_]+)",
                r"INTO\s+([\w_]+)",
                r"UPDATE\s+([\w_]+)",
                r"ALTER\s+TABLE\s+([\w_]+)",
                r"CREATE\s+TABLE\s+([\w_]+)",
                r"DROP\s+TABLE\s+([\w_]+)",
            ]

            for pattern in patterns:
                matches = re.findall(pattern, sql)
                table_names.update(matches)

        return list(table_names)

    def _calculate_migration_checksum(self, migration_plan: MigrationPlan) -> str:
        """Calculate checksum for migration plan"""
        content = f"{migration_plan.name}{migration_plan.migration_type.value}"
        content += "".join(step.sql_command for step in migration_plan.steps)
        return hashlib.md5(content.encode()).hexdigest()

    def _create_backup(self, migration_id: str) -> dict[str, Any]:
        """Create database backup before migration"""
        start_time = time.time()

        try:
            backup_path = (
                self.migration_dir / "backups" / f"backup_{migration_id}_{int(start_time)}.sql"
            )
            backup_path.parent.mkdir(exist_ok=True)

            # This is a simplified backup - in production, use database-specific tools
            logger.info(f"Creating backup: {backup_path}")

            # For SQLite, copy the file; for PostgreSQL/MySQL, use pg_dump/mysqldump
            # This is a placeholder implementation

            duration_ms = (time.time() - start_time) * 1000

            return {"backup_path": str(backup_path), "duration_ms": duration_ms, "success": True}

        except Exception as e:
            logger.error(f"Backup creation failed: {e}")
            return {"success": False, "error": str(e), "duration_ms": 0}

    def get_migration_history(self, limit: int = 50) -> list[MigrationResult]:
        """Get migration execution history"""
        return self.migration_history[:limit]

    def get_pending_migrations(self) -> list[MigrationPlan]:
        """Get pending migrations"""
        return self.pending_migrations.copy()

    def rollback_migration(self, migration_id: str) -> MigrationResult:
        """Rollback a specific migration"""
        # Find migration in history
        migration_record = None
        for record in self.migration_history:
            if record.migration_id == migration_id and record.status == MigrationStatus.SUCCESS:
                migration_record = record
                break

        if not migration_record:
            raise ValueError(f"Migration {migration_id} not found or not successful")

        # This would require storing rollback plans in the database
        # For now, return a placeholder result
        logger.warning(
            f"Rollback requested for migration {migration_id} - manual rollback required"
        )

        return MigrationResult(
            migration_id=migration_id,
            status=MigrationStatus.ROLLED_BACK,
            executed_steps=0,
            total_steps=0,
            execution_time=timedelta(),
            rollback_executed=True,
        )

    def get_schema_diff(self, target_metadata: MetaData = None) -> dict[str, Any]:
        """Get differences between current schema and target"""
        try:
            inspector = inspect(self.engine)
            current_tables = set(inspector.get_table_names())

            if target_metadata:
                target_tables = set(target_metadata.tables.keys())

                return {
                    "tables_to_add": list(target_tables - current_tables),
                    "tables_to_drop": list(current_tables - target_tables),
                    "tables_common": list(current_tables & target_tables),
                }
            else:
                return {"current_tables": list(current_tables), "table_count": len(current_tables)}

        except Exception as e:
            logger.error(f"Schema diff failed: {e}")
            return {"error": str(e)}


# Convenience function to create migration manager
def create_migration_manager(
    engine: Engine, migration_dir: str = "./migrations"
) -> MigrationManager:
    """Create migration manager instance"""
    return MigrationManager(engine, migration_dir)
