"""
Disaster Recovery Manager

Enterprise-grade disaster recovery system with automated recovery procedures,
point-in-time restoration, and comprehensive disaster recovery planning.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import shutil
import tarfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from core.config.base_config import BaseConfig
from core.logging import get_logger

from .backup_manager import BackupManager, BackupStatus, BackupType

logger = get_logger(__name__)


class RecoveryType(str, Enum):
    """Types of recovery operations."""
    POINT_IN_TIME = "point_in_time"
    FULL_RESTORE = "full_restore"
    PARTIAL_RESTORE = "partial_restore"
    DISASTER_RECOVERY = "disaster_recovery"


class RecoveryStatus(str, Enum):
    """Status of recovery operations."""
    PLANNED = "planned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    VERIFIED = "verified"


@dataclass
class RecoveryPoint:
    """Represents a point-in-time recovery target."""
    timestamp: datetime
    backup_id: str
    component: str
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'timestamp': self.timestamp.isoformat(),
            'backup_id': self.backup_id,
            'component': self.component,
            'description': self.description,
            'metadata': self.metadata
        }


@dataclass
class DisasterRecoveryPlan:
    """Comprehensive disaster recovery plan."""
    plan_id: str
    name: str
    description: str
    recovery_time_objective_minutes: int  # RTO
    recovery_point_objective_minutes: int  # RPO
    priority_components: list[str]
    recovery_steps: list[dict[str, Any]]
    contact_information: dict[str, str]
    validation_procedures: list[str]
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_tested: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'plan_id': self.plan_id,
            'name': self.name,
            'description': self.description,
            'recovery_time_objective_minutes': self.recovery_time_objective_minutes,
            'recovery_point_objective_minutes': self.recovery_point_objective_minutes,
            'priority_components': self.priority_components,
            'recovery_steps': self.recovery_steps,
            'contact_information': self.contact_information,
            'validation_procedures': self.validation_procedures,
            'created_at': self.created_at.isoformat(),
            'last_tested': self.last_tested.isoformat() if self.last_tested else None
        }


class RecoveryManager:
    """
    Enterprise disaster recovery manager with comprehensive recovery capabilities.
    
    Features:
    - Point-in-time recovery from backups
    - Full system disaster recovery
    - Partial component recovery
    - Recovery plan automation
    - Recovery verification and testing
    - RTO/RPO compliance monitoring
    """

    def __init__(self, config: BaseConfig, backup_manager: BackupManager | None = None):
        self.config = config
        self.backup_manager = backup_manager or BackupManager(config)
        self.recovery_root = config.project_root / "recovery"
        self.plans_dir = self.recovery_root / "plans"
        self.logs_dir = self.recovery_root / "logs"
        self._ensure_recovery_structure()

    def _ensure_recovery_structure(self) -> None:
        """Ensure recovery directory structure exists."""
        directories = [
            self.recovery_root,
            self.plans_dir,
            self.logs_dir,
            self.recovery_root / "temp",
            self.recovery_root / "verification"
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

        logger.info(f"Recovery structure initialized at {self.recovery_root}")

    async def create_recovery_point(
        self,
        component: str,
        description: str = "",
        metadata: dict[str, Any] | None = None
    ) -> RecoveryPoint:
        """
        Create a recovery point by triggering a backup.
        
        Args:
            component: Component to create recovery point for
            description: Human-readable description
            metadata: Additional metadata
            
        Returns:
            Recovery point information
        """
        logger.info(f"Creating recovery point for {component}")

        try:
            # Trigger appropriate backup based on component
            if component == "database":
                backup_metadata = await self.backup_manager.backup_database(
                    backup_type=BackupType.FULL,
                    compress=True,
                    validate=True
                )
            elif component == "data_lake":
                backup_results = await self.backup_manager.backup_data_lake(
                    backup_type=BackupType.FULL,
                    compress=True
                )
                # Use first successful backup
                backup_metadata = next(
                    (b for b in backup_results if b.status == BackupStatus.COMPLETED),
                    backup_results[0] if backup_results else None
                )
            elif component == "configuration":
                backup_metadata = await self.backup_manager.backup_configuration(compress=True)
            else:
                raise ValueError(f"Unknown component: {component}")

            if not backup_metadata:
                raise RuntimeError(f"Failed to create backup for {component}")

            recovery_point = RecoveryPoint(
                timestamp=backup_metadata.timestamp,
                backup_id=backup_metadata.backup_id,
                component=component,
                description=description or f"Recovery point for {component}",
                metadata=metadata or {}
            )

            # Save recovery point metadata
            await self._save_recovery_point(recovery_point)

            logger.info(f"Recovery point created: {recovery_point.backup_id}")
            return recovery_point

        except Exception as e:
            logger.error(f"Failed to create recovery point for {component}: {e}")
            raise

    async def restore_from_backup(
        self,
        backup_id: str,
        target_location: Path | None = None,
        verify_integrity: bool = True
    ) -> dict[str, Any]:
        """
        Restore system from a specific backup.
        
        Args:
            backup_id: ID of backup to restore from
            target_location: Optional custom restore location
            verify_integrity: Whether to verify backup integrity
            
        Returns:
            Recovery operation results
        """
        logger.info(f"Starting recovery from backup: {backup_id}")

        start_time = datetime.utcnow()
        recovery_id = f"recovery_{backup_id}_{start_time.strftime('%Y%m%d_%H%M%S')}"

        recovery_results = {
            "recovery_id": recovery_id,
            "backup_id": backup_id,
            "start_time": start_time.isoformat(),
            "status": RecoveryStatus.IN_PROGRESS.value,
            "steps_completed": [],
            "error_message": None,
            "verification_results": {}
        }

        try:
            # Find backup metadata
            backup_history = self.backup_manager.get_backup_history(limit=500)
            backup_metadata = None

            for backup in backup_history:
                if backup.get("backup_id") == backup_id:
                    backup_metadata = backup
                    break

            if not backup_metadata:
                raise ValueError(f"Backup not found: {backup_id}")

            component = backup_metadata.get("tags", {}).get("component", "unknown")
            backup_path = Path(backup_metadata["destination_path"])

            # Verify backup integrity if requested
            if verify_integrity:
                logger.info("Verifying backup integrity...")
                if not await self._verify_backup_integrity(backup_path, backup_metadata):
                    raise ValueError("Backup integrity verification failed")
                recovery_results["steps_completed"].append("integrity_verified")

            # Determine restore location
            if target_location is None:
                target_location = self._get_default_restore_location(component)

            # Perform component-specific restoration
            if component == "database":
                await self._restore_database(backup_path, backup_metadata, target_location)
            elif component == "data_lake":
                await self._restore_data_lake(backup_path, backup_metadata, target_location)
            elif component == "configuration":
                await self._restore_configuration(backup_path, backup_metadata, target_location)
            else:
                raise ValueError(f"Unknown component type: {component}")

            recovery_results["steps_completed"].append(f"{component}_restored")

            # Verify restoration if possible
            verification_results = await self._verify_restoration(component, target_location)
            recovery_results["verification_results"] = verification_results
            recovery_results["steps_completed"].append("restoration_verified")

            # Calculate duration and mark as completed
            end_time = datetime.utcnow()
            recovery_results["end_time"] = end_time.isoformat()
            recovery_results["duration_seconds"] = (end_time - start_time).total_seconds()
            recovery_results["status"] = RecoveryStatus.COMPLETED.value

            # Save recovery log
            await self._save_recovery_log(recovery_results)

            logger.info(f"Recovery completed successfully: {recovery_id}")

        except Exception as e:
            recovery_results["status"] = RecoveryStatus.FAILED.value
            recovery_results["error_message"] = str(e)
            recovery_results["end_time"] = datetime.utcnow().isoformat()

            await self._save_recovery_log(recovery_results)

            logger.error(f"Recovery failed: {recovery_id} - {e}")
            raise

        return recovery_results

    async def create_disaster_recovery_plan(
        self,
        name: str,
        description: str,
        rto_minutes: int = 240,  # 4 hours default RTO
        rpo_minutes: int = 60,   # 1 hour default RPO
        priority_components: list[str] | None = None
    ) -> DisasterRecoveryPlan:
        """
        Create a comprehensive disaster recovery plan.
        
        Args:
            name: Plan name
            description: Plan description
            rto_minutes: Recovery Time Objective in minutes
            rpo_minutes: Recovery Point Objective in minutes
            priority_components: Components in order of recovery priority
            
        Returns:
            Disaster recovery plan
        """
        plan_id = f"dr_plan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        if priority_components is None:
            priority_components = ["database", "configuration", "data_lake"]

        # Define standard recovery steps
        recovery_steps = [
            {
                "step": 1,
                "name": "Assess Disaster Scope",
                "description": "Determine extent of system failure and components affected",
                "estimated_minutes": 15,
                "required_personnel": ["incident_commander", "technical_lead"]
            },
            {
                "step": 2,
                "name": "Activate DR Team",
                "description": "Notify and mobilize disaster recovery team",
                "estimated_minutes": 10,
                "required_personnel": ["incident_commander"]
            },
            {
                "step": 3,
                "name": "Secure Alternative Infrastructure",
                "description": "Prepare backup systems and infrastructure",
                "estimated_minutes": 30,
                "required_personnel": ["infrastructure_team"]
            },
            {
                "step": 4,
                "name": "Restore Critical Data",
                "description": "Restore database and critical configuration",
                "estimated_minutes": 60,
                "required_personnel": ["database_admin", "technical_lead"],
                "components": ["database", "configuration"]
            },
            {
                "step": 5,
                "name": "Restore Data Lake",
                "description": "Restore analytical data and data lake layers",
                "estimated_minutes": 120,
                "required_personnel": ["data_engineer"],
                "components": ["data_lake"]
            },
            {
                "step": 6,
                "name": "Verify System Integrity",
                "description": "Run comprehensive system verification checks",
                "estimated_minutes": 30,
                "required_personnel": ["technical_lead", "qa_engineer"]
            },
            {
                "step": 7,
                "name": "Resume Operations",
                "description": "Bring systems online and resume normal operations",
                "estimated_minutes": 15,
                "required_personnel": ["incident_commander", "operations_team"]
            }
        ]

        # Standard validation procedures
        validation_procedures = [
            "Verify database connectivity and data integrity",
            "Confirm ETL pipeline operational status",
            "Test API endpoints and service health",
            "Validate data lake accessibility and structure",
            "Check monitoring and alerting systems",
            "Perform end-to-end system functionality test",
            "Confirm backup systems are operational"
        ]

        # Contact information template
        contact_information = {
            "incident_commander": "TBD - Primary decision maker",
            "technical_lead": "TBD - Technical recovery lead",
            "database_admin": "TBD - Database recovery specialist",
            "data_engineer": "TBD - Data systems specialist",
            "infrastructure_team": "TBD - Infrastructure and cloud team",
            "qa_engineer": "TBD - Quality assurance lead",
            "operations_team": "TBD - Operations and monitoring team"
        }

        plan = DisasterRecoveryPlan(
            plan_id=plan_id,
            name=name,
            description=description,
            recovery_time_objective_minutes=rto_minutes,
            recovery_point_objective_minutes=rpo_minutes,
            priority_components=priority_components,
            recovery_steps=recovery_steps,
            contact_information=contact_information,
            validation_procedures=validation_procedures
        )

        # Save the plan
        await self._save_dr_plan(plan)

        logger.info(f"Disaster recovery plan created: {plan_id}")
        return plan

    async def execute_disaster_recovery_plan(
        self,
        plan_id: str,
        simulate: bool = False
    ) -> dict[str, Any]:
        """
        Execute a disaster recovery plan.
        
        Args:
            plan_id: ID of disaster recovery plan to execute
            simulate: Whether to simulate execution (dry run)
            
        Returns:
            Execution results and timeline
        """
        logger.info(f"{'Simulating' if simulate else 'Executing'} DR plan: {plan_id}")

        # Load the plan
        plan = await self._load_dr_plan(plan_id)
        if not plan:
            raise ValueError(f"Disaster recovery plan not found: {plan_id}")

        execution_id = f"dr_exec_{plan_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.utcnow()

        execution_results = {
            "execution_id": execution_id,
            "plan_id": plan_id,
            "plan_name": plan.name,
            "start_time": start_time.isoformat(),
            "simulation": simulate,
            "steps": [],
            "total_estimated_minutes": sum(step.get("estimated_minutes", 0) for step in plan.recovery_steps),
            "actual_duration_minutes": 0,
            "status": "in_progress",
            "components_recovered": []
        }

        try:
            for step_info in plan.recovery_steps:
                step_start = datetime.utcnow()
                step_result = {
                    "step": step_info["step"],
                    "name": step_info["name"],
                    "start_time": step_start.isoformat(),
                    "estimated_minutes": step_info.get("estimated_minutes", 0),
                    "status": "in_progress"
                }

                try:
                    if simulate:
                        # Simulate execution with delay
                        await asyncio.sleep(0.1)  # Brief delay for simulation
                        step_result["status"] = "simulated"
                        step_result["notes"] = f"Simulated execution of {step_info['name']}"
                    else:
                        # Execute actual recovery step
                        if "components" in step_info:
                            await self._execute_recovery_step(step_info, plan.priority_components)
                            execution_results["components_recovered"].extend(step_info["components"])

                        step_result["status"] = "completed"

                except Exception as e:
                    step_result["status"] = "failed"
                    step_result["error"] = str(e)
                    logger.error(f"DR step failed: {step_info['name']} - {e}")

                    if not simulate:
                        # In real execution, failure of critical steps should halt the process
                        execution_results["status"] = "failed"
                        break

                step_end = datetime.utcnow()
                step_result["actual_minutes"] = (step_end - step_start).total_seconds() / 60
                step_result["end_time"] = step_end.isoformat()

                execution_results["steps"].append(step_result)

                logger.info(f"DR step {'simulated' if simulate else 'completed'}: {step_info['name']}")

            # Calculate final results
            end_time = datetime.utcnow()
            execution_results["end_time"] = end_time.isoformat()
            execution_results["actual_duration_minutes"] = (end_time - start_time).total_seconds() / 60

            if execution_results["status"] != "failed":
                execution_results["status"] = "completed"

            # Save execution log
            await self._save_dr_execution_log(execution_results)

            # Update plan last tested time if this was a simulation
            if simulate:
                plan.last_tested = datetime.utcnow()
                await self._save_dr_plan(plan)

            logger.info(f"DR plan {'simulation' if simulate else 'execution'} completed: {execution_id}")

        except Exception as e:
            execution_results["status"] = "failed"
            execution_results["error"] = str(e)
            execution_results["end_time"] = datetime.utcnow().isoformat()

            logger.error(f"DR plan execution failed: {execution_id} - {e}")
            raise

        return execution_results

    def get_recovery_points(self, component: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """
        Get available recovery points.
        
        Args:
            component: Filter by component
            limit: Maximum number of recovery points
            
        Returns:
            List of recovery points
        """
        recovery_points = []

        # Load recovery points from files
        recovery_points_dir = self.recovery_root / "recovery_points"
        if recovery_points_dir.exists():
            for point_file in recovery_points_dir.glob("*.json"):
                try:
                    with open(point_file) as f:
                        point_data = json.load(f)

                    if component is None or point_data.get("component") == component:
                        recovery_points.append(point_data)

                except Exception as e:
                    logger.warning(f"Could not read recovery point {point_file}: {e}")

        # Also get recovery points from backup history
        backup_history = self.backup_manager.get_backup_history(component=component, limit=limit)

        for backup in backup_history:
            if backup.get("status") in ["completed", "validated"]:
                recovery_points.append({
                    "timestamp": backup["timestamp"],
                    "backup_id": backup["backup_id"],
                    "component": backup.get("tags", {}).get("component", "unknown"),
                    "description": f"Backup-based recovery point: {backup['backup_id']}",
                    "metadata": backup
                })

        # Sort by timestamp and limit
        recovery_points.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return recovery_points[:limit]

    async def _restore_database(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any],
        target_location: Path
    ) -> None:
        """Restore database from backup."""
        logger.info(f"Restoring database to {target_location}")

        # Ensure target directory exists
        target_location.parent.mkdir(parents=True, exist_ok=True)

        # Handle compressed backups
        if backup_path.suffix == ".gz":
            with gzip.open(backup_path, 'rb') as f_in:
                with open(target_location, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(backup_path, target_location)

        logger.info("Database restoration completed")

    async def _restore_data_lake(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any],
        target_location: Path
    ) -> None:
        """Restore data lake from backup."""
        logger.info(f"Restoring data lake to {target_location}")

        # Ensure target directory exists
        target_location.mkdir(parents=True, exist_ok=True)

        # Handle compressed archives
        if backup_path.suffix == ".gz" and backup_path.name.endswith(".tar.gz"):
            with tarfile.open(backup_path, "r:gz") as tar:
                tar.extractall(path=target_location.parent)
        else:
            shutil.copytree(backup_path, target_location, dirs_exist_ok=True)

        logger.info("Data lake restoration completed")

    async def _restore_configuration(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any],
        target_location: Path
    ) -> None:
        """Restore configuration from backup."""
        logger.info(f"Restoring configuration to {target_location}")

        # Ensure target directory exists
        target_location.mkdir(parents=True, exist_ok=True)

        # Handle compressed archives
        if backup_path.suffix == ".gz" and backup_path.name.endswith(".tar.gz"):
            with tarfile.open(backup_path, "r:gz") as tar:
                tar.extractall(path=target_location)
        else:
            shutil.copytree(backup_path, target_location, dirs_exist_ok=True)

        logger.info("Configuration restoration completed")

    async def _verify_backup_integrity(
        self,
        backup_path: Path,
        backup_metadata: dict[str, Any]
    ) -> bool:
        """Verify backup file integrity."""
        try:
            # Check file exists and has expected size
            if not backup_path.exists():
                logger.error(f"Backup file not found: {backup_path}")
                return False

            actual_size = backup_path.stat().st_size
            expected_size = backup_metadata.get("size_bytes", 0)

            if expected_size > 0 and abs(actual_size - expected_size) > 1024:  # Allow 1KB variance
                logger.error(f"Backup size mismatch: expected {expected_size}, got {actual_size}")
                return False

            # Verify checksum if available
            expected_checksum = backup_metadata.get("checksum")
            if expected_checksum:
                actual_checksum = await self.backup_manager._calculate_checksum(backup_path)
                if actual_checksum != expected_checksum:
                    logger.error(f"Backup checksum mismatch: expected {expected_checksum}, got {actual_checksum}")
                    return False

            logger.info("Backup integrity verification passed")
            return True

        except Exception as e:
            logger.error(f"Backup integrity verification failed: {e}")
            return False

    async def _verify_restoration(self, component: str, target_location: Path) -> dict[str, Any]:
        """Verify restoration was successful."""
        verification_results = {
            "component": component,
            "target_location": str(target_location),
            "checks": [],
            "overall_success": True
        }

        try:
            # Basic file existence check
            if target_location.exists():
                verification_results["checks"].append({
                    "name": "file_exists",
                    "status": "passed",
                    "message": f"Target location exists: {target_location}"
                })
            else:
                verification_results["checks"].append({
                    "name": "file_exists",
                    "status": "failed",
                    "message": f"Target location missing: {target_location}"
                })
                verification_results["overall_success"] = False

            # Component-specific verification
            if component == "database":
                # Try to read database file
                if target_location.is_file() and target_location.stat().st_size > 0:
                    verification_results["checks"].append({
                        "name": "database_readable",
                        "status": "passed",
                        "message": "Database file appears valid"
                    })
                else:
                    verification_results["checks"].append({
                        "name": "database_readable",
                        "status": "failed",
                        "message": "Database file is missing or empty"
                    })
                    verification_results["overall_success"] = False

            elif component in ["data_lake", "configuration"]:
                # Check directory structure
                if target_location.is_dir() and any(target_location.iterdir()):
                    verification_results["checks"].append({
                        "name": "directory_structure",
                        "status": "passed",
                        "message": "Directory structure appears valid"
                    })
                else:
                    verification_results["checks"].append({
                        "name": "directory_structure",
                        "status": "failed",
                        "message": "Directory is missing or empty"
                    })
                    verification_results["overall_success"] = False

        except Exception as e:
            verification_results["checks"].append({
                "name": "verification_error",
                "status": "failed",
                "message": f"Verification failed with error: {e}"
            })
            verification_results["overall_success"] = False

        return verification_results

    async def _execute_recovery_step(
        self,
        step_info: dict[str, Any],
        priority_components: list[str]
    ) -> None:
        """Execute a specific recovery step."""
        components = step_info.get("components", [])

        for component in components:
            if component in priority_components:
                # Find latest backup for component
                recovery_points = self.get_recovery_points(component=component, limit=1)

                if recovery_points:
                    latest_backup = recovery_points[0]
                    await self.restore_from_backup(
                        backup_id=latest_backup["backup_id"],
                        verify_integrity=True
                    )
                else:
                    logger.warning(f"No recovery points found for component: {component}")

    def _get_default_restore_location(self, component: str) -> Path:
        """Get default restoration location for component."""
        restore_base = self.recovery_root / "restored"

        if component == "database":
            return restore_base / "database" / "restored.db"
        elif component == "data_lake":
            return restore_base / "data_lake"
        elif component == "configuration":
            return restore_base / "configuration"
        else:
            return restore_base / component

    async def _save_recovery_point(self, recovery_point: RecoveryPoint) -> None:
        """Save recovery point metadata."""
        points_dir = self.recovery_root / "recovery_points"
        points_dir.mkdir(exist_ok=True)

        point_file = points_dir / f"{recovery_point.backup_id}.json"
        with open(point_file, 'w') as f:
            json.dump(recovery_point.to_dict(), f, indent=2)

    async def _save_recovery_log(self, recovery_results: dict[str, Any]) -> None:
        """Save recovery operation log."""
        log_file = self.logs_dir / f"{recovery_results['recovery_id']}.json"

        with open(log_file, 'w') as f:
            json.dump(recovery_results, f, indent=2)

    async def _save_dr_plan(self, plan: DisasterRecoveryPlan) -> None:
        """Save disaster recovery plan."""
        plan_file = self.plans_dir / f"{plan.plan_id}.json"

        with open(plan_file, 'w') as f:
            json.dump(plan.to_dict(), f, indent=2)

    async def _load_dr_plan(self, plan_id: str) -> DisasterRecoveryPlan | None:
        """Load disaster recovery plan."""
        plan_file = self.plans_dir / f"{plan_id}.json"

        if not plan_file.exists():
            return None

        try:
            with open(plan_file) as f:
                plan_data = json.load(f)

            return DisasterRecoveryPlan(
                plan_id=plan_data["plan_id"],
                name=plan_data["name"],
                description=plan_data["description"],
                recovery_time_objective_minutes=plan_data["recovery_time_objective_minutes"],
                recovery_point_objective_minutes=plan_data["recovery_point_objective_minutes"],
                priority_components=plan_data["priority_components"],
                recovery_steps=plan_data["recovery_steps"],
                contact_information=plan_data["contact_information"],
                validation_procedures=plan_data["validation_procedures"],
                created_at=datetime.fromisoformat(plan_data["created_at"]),
                last_tested=datetime.fromisoformat(plan_data["last_tested"]) if plan_data.get("last_tested") else None
            )

        except Exception as e:
            logger.error(f"Failed to load DR plan {plan_id}: {e}")
            return None

    async def _save_dr_execution_log(self, execution_results: dict[str, Any]) -> None:
        """Save disaster recovery execution log."""
        log_file = self.logs_dir / f"{execution_results['execution_id']}.json"

        with open(log_file, 'w') as f:
            json.dump(execution_results, f, indent=2)
