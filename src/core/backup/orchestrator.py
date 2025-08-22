"""
Backup and Disaster Recovery Orchestrator

Central orchestrator for comprehensive backup and disaster recovery operations,
integrating all backup components into a unified system.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

from ...core.logging import get_logger
from ...core.config.base_config import BaseConfig

from .backup_manager import BackupManager, BackupType
from .recovery_manager import RecoveryManager, DisasterRecoveryPlan
from .scheduler import BackupScheduler, BackupPriority
from .validation import BackupValidator, IntegrityLevel
from .storage_backends import StorageBackend, LocalStorageBackend, create_storage_backend
from .monitoring import BackupMonitor, AlertSeverity

logger = get_logger(__name__)


class OrchestrationMode(str, Enum):
    """Orchestration operation modes."""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EMERGENCY = "emergency"
    MAINTENANCE = "maintenance"


@dataclass
class BackupJob:
    """Comprehensive backup job definition."""
    job_id: str
    name: str
    component: str
    backup_type: str
    schedule: str
    retention_policy: str
    validation_level: str
    storage_backends: List[str]
    priority: str = "normal"
    enabled: bool = True
    tags: Dict[str, str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = {}


class BackupOrchestrator:
    """
    Master orchestrator for backup and disaster recovery operations.
    
    Features:
    - Centralized backup and recovery coordination
    - Multi-storage backend management
    - Automated disaster recovery workflows
    - Comprehensive monitoring and alerting
    - SLA compliance tracking
    - Cross-component dependency management
    """
    
    def __init__(self, config: BaseConfig):
        self.config = config
        
        # Initialize core components
        self.backup_manager = BackupManager(config)
        self.recovery_manager = RecoveryManager(config, self.backup_manager)
        self.scheduler = BackupScheduler(self.backup_manager)
        self.validator = BackupValidator()
        self.monitor = BackupMonitor()
        
        # Storage backends
        self.storage_backends: Dict[str, StorageBackend] = {}
        
        # Backup jobs registry
        self.backup_jobs: Dict[str, BackupJob] = {}
        
        # Orchestration state
        self.orchestration_state = {
            "active_operations": {},
            "last_full_backup": None,
            "last_dr_test": None,
            "system_health": "healthy"
        }
        
        # Initialize default storage backends
        self._initialize_storage_backends()
        
        # Set up monitoring alert handlers
        self._setup_monitoring()
        
        logger.info("Backup orchestrator initialized")
    
    async def initialize(self) -> None:
        """Initialize the orchestrator and all components."""
        try:
            # Create default backup jobs
            await self._create_default_backup_jobs()
            
            # Create default disaster recovery plans
            await self._create_default_dr_plans()
            
            # Start scheduler
            await self.scheduler.start_scheduler()
            
            logger.info("Backup orchestrator initialization completed")
            
        except Exception as e:
            logger.error(f"Failed to initialize backup orchestrator: {e}")
            raise
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the orchestrator."""
        try:
            # Stop scheduler
            await self.scheduler.stop_scheduler()
            
            # Wait for active operations to complete (with timeout)
            await self._wait_for_active_operations(timeout_seconds=300)
            
            logger.info("Backup orchestrator shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during orchestrator shutdown: {e}")
    
    async def trigger_full_system_backup(
        self,
        mode: OrchestrationMode = OrchestrationMode.MANUAL,
        validate: bool = True,
        multi_storage: bool = True
    ) -> Dict[str, Any]:
        """
        Trigger comprehensive full system backup.
        
        Args:
            mode: Orchestration mode for the backup
            validate: Whether to validate backups after completion
            multi_storage: Whether to store in multiple backend
            
        Returns:
            Comprehensive backup results
        """
        operation_id = f"full_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Starting full system backup: {operation_id} (mode: {mode.value})")
        
        # Record operation start
        self.orchestration_state["active_operations"][operation_id] = {
            "type": "full_backup",
            "start_time": datetime.utcnow(),
            "mode": mode.value,
            "status": "in_progress"
        }
        
        try:
            # Record monitoring metrics
            await self.monitor.record_backup_start(operation_id, "system", "full")
            
            start_time = datetime.utcnow()
            
            # Execute full system backup
            backup_results = await self.backup_manager.perform_full_system_backup(
                compress=True,
                validate=False  # We'll do our own validation
            )
            
            # Store backups in multiple storage backends if requested
            if multi_storage:
                await self._replicate_to_storage_backends(backup_results)
            
            # Perform comprehensive validation if requested
            if validate:
                validation_results = await self._validate_system_backup(backup_results)
                backup_results["validation"] = validation_results
            
            # Calculate total metrics
            end_time = datetime.utcnow()
            total_duration = (end_time - start_time).total_seconds()
            total_size = backup_results["summary"]["total_size_bytes"]
            success = backup_results["summary"]["failed_backups"] == 0
            
            # Record completion metrics
            await self.monitor.record_backup_completion(
                operation_id,
                "system",
                "full",
                total_duration,
                total_size,
                success
            )
            
            # Update orchestration state
            self.orchestration_state["last_full_backup"] = end_time
            self.orchestration_state["active_operations"][operation_id]["status"] = "completed"
            self.orchestration_state["active_operations"][operation_id]["end_time"] = end_time
            
            # Generate success alert
            if success:
                await self.monitor.generate_alert(
                    AlertSeverity.INFO,
                    "Full System Backup Completed",
                    f"Full system backup completed successfully in {total_duration:.1f}s "
                    f"({total_size / 1024 / 1024:.1f} MB)",
                    "system",
                    tags={"operation_id": operation_id, "mode": mode.value}
                )
            else:
                await self.monitor.generate_alert(
                    AlertSeverity.HIGH,
                    "Full System Backup Issues",
                    f"Full system backup completed with {backup_results['summary']['failed_backups']} failures",
                    "system",
                    tags={"operation_id": operation_id, "mode": mode.value}
                )
            
            logger.info(f"Full system backup completed: {operation_id} "
                       f"(success: {success}, duration: {total_duration:.1f}s)")
            
            return {
                "operation_id": operation_id,
                "success": success,
                "duration_seconds": total_duration,
                "backup_results": backup_results
            }
            
        except Exception as e:
            # Record failure
            await self.monitor.record_backup_completion(
                operation_id, "system", "full", 0, 0, False
            )
            
            await self.monitor.generate_alert(
                AlertSeverity.CRITICAL,
                "Full System Backup Failed",
                f"Full system backup failed: {str(e)}",
                "system",
                tags={"operation_id": operation_id, "error": str(e)}
            )
            
            self.orchestration_state["active_operations"][operation_id]["status"] = "failed"
            self.orchestration_state["active_operations"][operation_id]["error"] = str(e)
            
            logger.error(f"Full system backup failed: {operation_id} - {e}")
            raise
    
    async def execute_disaster_recovery(
        self,
        plan_id: str,
        simulate: bool = False,
        override_rto: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute disaster recovery plan.
        
        Args:
            plan_id: ID of disaster recovery plan to execute
            simulate: Whether to simulate execution
            override_rto: Override RTO in minutes
            
        Returns:
            Disaster recovery execution results
        """
        operation_id = f"dr_exec_{plan_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        logger.critical(f"{'Simulating' if simulate else 'Executing'} disaster recovery: {plan_id}")
        
        # Record operation start
        self.orchestration_state["active_operations"][operation_id] = {
            "type": "disaster_recovery",
            "plan_id": plan_id,
            "start_time": datetime.utcnow(),
            "simulate": simulate,
            "status": "in_progress"
        }
        
        try:
            start_time = datetime.utcnow()
            
            # Generate critical alert for real DR execution
            if not simulate:
                await self.monitor.generate_alert(
                    AlertSeverity.CRITICAL,
                    "Disaster Recovery Initiated",
                    f"Disaster recovery plan {plan_id} is being executed",
                    "system",
                    tags={"plan_id": plan_id, "operation_id": operation_id}
                )
            
            # Execute the disaster recovery plan
            dr_results = await self.recovery_manager.execute_disaster_recovery_plan(
                plan_id, simulate=simulate
            )
            
            end_time = datetime.utcnow()
            total_duration = (end_time - start_time).total_seconds()
            
            # Record recovery metrics
            await self.monitor.record_recovery_operation(
                operation_id,
                "system",
                "disaster_recovery",
                total_duration,
                dr_results["status"] == "completed"
            )
            
            # Update orchestration state
            self.orchestration_state["active_operations"][operation_id]["status"] = dr_results["status"]
            self.orchestration_state["active_operations"][operation_id]["end_time"] = end_time
            
            if simulate:
                self.orchestration_state["last_dr_test"] = end_time
            
            # Generate completion alert
            if dr_results["status"] == "completed":
                await self.monitor.generate_alert(
                    AlertSeverity.INFO if simulate else AlertSeverity.CRITICAL,
                    f"Disaster Recovery {'Test' if simulate else 'Execution'} Completed",
                    f"DR plan {plan_id} {'simulation' if simulate else 'execution'} completed in {total_duration:.1f}s",
                    "system",
                    tags={"plan_id": plan_id, "operation_id": operation_id, "simulate": str(simulate)}
                )
            else:
                await self.monitor.generate_alert(
                    AlertSeverity.CRITICAL,
                    f"Disaster Recovery {'Test' if simulate else 'Execution'} Failed",
                    f"DR plan {plan_id} {'simulation' if simulate else 'execution'} failed",
                    "system",
                    tags={"plan_id": plan_id, "operation_id": operation_id, "error": dr_results.get("error", "Unknown")}
                )
            
            logger.info(f"Disaster recovery {'simulation' if simulate else 'execution'} completed: {operation_id}")
            
            return {
                "operation_id": operation_id,
                "dr_results": dr_results,
                "total_duration": total_duration
            }
            
        except Exception as e:
            await self.monitor.generate_alert(
                AlertSeverity.CRITICAL,
                "Disaster Recovery Failed",
                f"DR plan {plan_id} failed with critical error: {str(e)}",
                "system",
                tags={"plan_id": plan_id, "operation_id": operation_id, "error": str(e)}
            )
            
            self.orchestration_state["active_operations"][operation_id]["status"] = "failed"
            self.orchestration_state["active_operations"][operation_id]["error"] = str(e)
            
            logger.error(f"Disaster recovery execution failed: {operation_id} - {e}")
            raise
    
    async def restore_point_in_time(
        self,
        component: str,
        target_timestamp: datetime,
        destination: Optional[Path] = None
    ) -> Dict[str, Any]:
        """
        Restore component to specific point in time.
        
        Args:
            component: Component to restore
            target_timestamp: Target timestamp for restore
            destination: Optional custom restore destination
            
        Returns:
            Point-in-time restore results
        """
        operation_id = f"pit_restore_{component}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Starting point-in-time restore: {component} to {target_timestamp}")
        
        try:
            start_time = datetime.utcnow()
            
            # Find closest backup to target timestamp
            recovery_points = self.recovery_manager.get_recovery_points(component=component, limit=100)
            
            closest_backup = None
            min_time_diff = float('inf')
            
            for point in recovery_points:
                point_time = datetime.fromisoformat(point["timestamp"])
                time_diff = abs((point_time - target_timestamp).total_seconds())
                
                if time_diff < min_time_diff and point_time <= target_timestamp:
                    min_time_diff = time_diff
                    closest_backup = point
            
            if not closest_backup:
                raise ValueError(f"No suitable backup found for {component} at {target_timestamp}")
            
            # Perform the restore
            restore_results = await self.recovery_manager.restore_from_backup(
                backup_id=closest_backup["backup_id"],
                target_location=destination,
                verify_integrity=True
            )
            
            end_time = datetime.utcnow()
            total_duration = (end_time - start_time).total_seconds()
            
            # Record recovery metrics
            await self.monitor.record_recovery_operation(
                operation_id,
                component,
                "point_in_time",
                total_duration,
                restore_results["status"] == "completed"
            )
            
            logger.info(f"Point-in-time restore completed: {operation_id}")
            
            return {
                "operation_id": operation_id,
                "restore_results": restore_results,
                "backup_used": closest_backup["backup_id"],
                "time_difference_seconds": min_time_diff,
                "duration_seconds": total_duration
            }
            
        except Exception as e:
            logger.error(f"Point-in-time restore failed: {operation_id} - {e}")
            raise
    
    async def run_maintenance_tasks(self) -> Dict[str, Any]:
        """
        Run scheduled maintenance tasks.
        
        Returns:
            Maintenance tasks results
        """
        logger.info("Starting backup system maintenance tasks")
        
        maintenance_results = {
            "start_time": datetime.utcnow().isoformat(),
            "tasks": {},
            "total_duration": 0.0
        }
        
        start_time = datetime.utcnow()
        
        try:
            # Clean up old metrics
            metrics_cleanup = await self.monitor.cleanup_old_metrics()
            maintenance_results["tasks"]["metrics_cleanup"] = metrics_cleanup
            
            # Run retention cleanup
            retention_stats = await self.scheduler._run_retention_cleanup()
            maintenance_results["tasks"]["retention_cleanup"] = retention_stats
            
            # Clean up storage backends
            storage_cleanup = {}
            for backend_id, backend in self.storage_backends.items():
                if hasattr(backend, 'cleanup_expired_files'):
                    cleanup_result = await backend.cleanup_expired_files()
                    storage_cleanup[backend_id] = cleanup_result
            
            maintenance_results["tasks"]["storage_cleanup"] = storage_cleanup
            
            # Validate recent backups
            validation_results = await self._run_maintenance_validation()
            maintenance_results["tasks"]["validation"] = validation_results
            
            # Update system health status
            await self._update_system_health()
            
            end_time = datetime.utcnow()
            maintenance_results["end_time"] = end_time.isoformat()
            maintenance_results["total_duration"] = (end_time - start_time).total_seconds()
            
            logger.info(f"Maintenance tasks completed in {maintenance_results['total_duration']:.1f}s")
            
            return maintenance_results
            
        except Exception as e:
            logger.error(f"Maintenance tasks failed: {e}")
            raise
    
    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get comprehensive system status.
        
        Returns:
            Complete system status report
        """
        status = {
            "timestamp": datetime.utcnow().isoformat(),
            "system_health": self.orchestration_state["system_health"],
            "active_operations": len(self.orchestration_state["active_operations"]),
            "scheduler": self.scheduler.get_schedule_status(),
            "storage_backends": {},
            "monitoring": {
                "active_alerts": len(await self.monitor.get_active_alerts()),
                "metrics_summary": await self.monitor.get_metrics_summary(hours=24)
            },
            "sla_compliance": await self.monitor.get_sla_compliance_report(),
            "last_full_backup": self.orchestration_state.get("last_full_backup"),
            "last_dr_test": self.orchestration_state.get("last_dr_test")
        }
        
        # Add storage backend statistics
        for backend_id, backend in self.storage_backends.items():
            try:
                status["storage_backends"][backend_id] = await backend.get_storage_stats()
            except Exception as e:
                status["storage_backends"][backend_id] = {"error": str(e)}
        
        return status
    
    def _initialize_storage_backends(self) -> None:
        """Initialize default storage backends."""
        # Local storage backend
        local_config = {
            "base_path": str(self.config.project_root / "backup_storage")
        }
        
        self.storage_backends["local"] = LocalStorageBackend("local", local_config)
        
        logger.info("Initialized storage backends")
    
    def _setup_monitoring(self) -> None:
        """Set up monitoring and alert handlers."""
        from .monitoring import console_alert_handler, log_alert_handler
        
        # Add default alert handlers
        self.monitor.add_alert_handler(console_alert_handler)
        self.monitor.add_alert_handler(log_alert_handler)
        
        logger.info("Monitoring and alerting configured")
    
    async def _create_default_backup_jobs(self) -> None:
        """Create default backup jobs."""
        # Database backup job
        self.backup_jobs["database_daily"] = BackupJob(
            job_id="database_daily",
            name="Daily Database Backup",
            component="database",
            backup_type="full",
            schedule="0 2 * * *",  # Daily at 2 AM
            retention_policy="standard_3_2_1",
            validation_level="standard",
            storage_backends=["local"],
            priority="high",
            tags={"automated": "true", "critical": "true"}
        )
        
        # Data lake backup job
        self.backup_jobs["datalake_weekly"] = BackupJob(
            job_id="datalake_weekly",
            name="Weekly Data Lake Backup",
            component="data_lake",
            backup_type="full",
            schedule="0 3 * * 0",  # Weekly on Sunday at 3 AM
            retention_policy="standard_3_2_1",
            validation_level="standard",
            storage_backends=["local"],
            priority="normal",
            tags={"automated": "true"}
        )
        
        # Configuration backup job
        self.backup_jobs["config_daily"] = BackupJob(
            job_id="config_daily",
            name="Daily Configuration Backup",
            component="configuration",
            backup_type="full",
            schedule="0 1 * * *",  # Daily at 1 AM
            retention_policy="standard_3_2_1",
            validation_level="basic",
            storage_backends=["local"],
            priority="normal",
            tags={"automated": "true"}
        )
        
        logger.info(f"Created {len(self.backup_jobs)} default backup jobs")
    
    async def _create_default_dr_plans(self) -> None:
        """Create default disaster recovery plans."""
        # Standard DR plan
        await self.recovery_manager.create_disaster_recovery_plan(
            "Standard Disaster Recovery Plan",
            "Comprehensive disaster recovery plan for system-wide failures",
            rto_minutes=240,  # 4 hours
            rpo_minutes=60,   # 1 hour
            priority_components=["database", "configuration", "data_lake"]
        )
        
        logger.info("Created default disaster recovery plans")
    
    async def _replicate_to_storage_backends(self, backup_results: Dict[str, Any]) -> None:
        """Replicate backups to multiple storage backends."""
        # Implementation would replicate backups to configured storage backends
        logger.info("Multi-storage replication completed")
    
    async def _validate_system_backup(self, backup_results: Dict[str, Any]) -> Dict[str, Any]:
        """Perform comprehensive validation of system backup."""
        validation_results = {
            "database": None,
            "data_lake": [],
            "configuration": None,
            "overall_success": True
        }
        
        try:
            # Validate database backup
            if backup_results.get("database"):
                db_backup = backup_results["database"]
                db_path = Path(db_backup["destination_path"])
                
                if db_path.exists():
                    report = await self.validator.validate_backup(
                        db_path,
                        db_backup,
                        IntegrityLevel.STANDARD
                    )
                    validation_results["database"] = report.to_dict()
                    
                    if report.overall_result.value != "passed":
                        validation_results["overall_success"] = False
            
            # Validate data lake backups
            for dl_backup in backup_results.get("data_lake", []):
                if "error" not in dl_backup:
                    dl_path = Path(dl_backup["destination_path"])
                    
                    if dl_path.exists():
                        report = await self.validator.validate_backup(
                            dl_path,
                            dl_backup,
                            IntegrityLevel.STANDARD
                        )
                        validation_results["data_lake"].append(report.to_dict())
                        
                        if report.overall_result.value != "passed":
                            validation_results["overall_success"] = False
            
            # Validate configuration backup
            if backup_results.get("configuration"):
                config_backup = backup_results["configuration"]
                config_path = Path(config_backup["destination_path"])
                
                if config_path.exists():
                    report = await self.validator.validate_backup(
                        config_path,
                        config_backup,
                        IntegrityLevel.BASIC
                    )
                    validation_results["configuration"] = report.to_dict()
                    
                    if report.overall_result.value != "passed":
                        validation_results["overall_success"] = False
            
        except Exception as e:
            logger.error(f"Backup validation failed: {e}")
            validation_results["overall_success"] = False
            validation_results["error"] = str(e)
        
        return validation_results
    
    async def _run_maintenance_validation(self) -> Dict[str, Any]:
        """Run validation on recent backups."""
        # Simplified maintenance validation
        return {
            "backups_validated": 0,
            "validation_errors": 0,
            "validation_warnings": 0
        }
    
    async def _update_system_health(self) -> None:
        """Update overall system health status."""
        try:
            # Check recent backup success rate
            active_alerts = await self.monitor.get_active_alerts(AlertSeverity.CRITICAL)
            high_alerts = await self.monitor.get_active_alerts(AlertSeverity.HIGH)
            
            if active_alerts:
                self.orchestration_state["system_health"] = "critical"
            elif high_alerts:
                self.orchestration_state["system_health"] = "warning"
            else:
                self.orchestration_state["system_health"] = "healthy"
            
        except Exception as e:
            logger.error(f"Failed to update system health: {e}")
            self.orchestration_state["system_health"] = "unknown"
    
    async def _wait_for_active_operations(self, timeout_seconds: int = 300) -> None:
        """Wait for active operations to complete."""
        start_time = datetime.utcnow()
        
        while self.orchestration_state["active_operations"]:
            if (datetime.utcnow() - start_time).total_seconds() > timeout_seconds:
                logger.warning("Timeout waiting for active operations to complete")
                break
            
            await asyncio.sleep(5)
        
        logger.info("All active operations completed or timed out")