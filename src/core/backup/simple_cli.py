"""
Simple Backup and Disaster Recovery Command Line Interface

Simplified CLI for backup and disaster recovery operations without external dependencies.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from pathlib import Path

from core.config.base_config import BaseConfig, Environment
from core.logging import get_logger

from .orchestrator import BackupOrchestrator, OrchestrationMode

logger = get_logger(__name__)


class SimpleBackupCLI:
    """Simplified command line interface for backup operations."""

    def __init__(self, config_path: Path | None = None):
        self.config = BaseConfig(
            environment=Environment.PRODUCTION,
            project_root=config_path or Path.cwd()
        )

        self.orchestrator: BackupOrchestrator | None = None

    async def _ensure_orchestrator(self) -> BackupOrchestrator:
        """Ensure orchestrator is initialized."""
        if self.orchestrator is None:
            self.orchestrator = BackupOrchestrator(self.config)
            await self.orchestrator.initialize()
        return self.orchestrator

    async def backup_full_system(self, validate: bool = True) -> None:
        """Perform full system backup."""
        orchestrator = await self._ensure_orchestrator()

        try:
            print("Starting full system backup...")

            results = await orchestrator.trigger_full_system_backup(
                mode=OrchestrationMode.MANUAL,
                validate=validate,
                multi_storage=False
            )

            if results["success"]:
                print("[SUCCESS] Full system backup completed successfully!")
                print(f"   Operation ID: {results['operation_id']}")
                print(f"   Duration: {results['duration_seconds']:.1f} seconds")
                size_mb = results['backup_results']['summary']['total_size_bytes'] / 1024 / 1024
                print(f"   Size: {size_mb:.1f} MB")
            else:
                print("[FAILED] Full system backup completed with issues!")
                print(f"   Failed backups: {results['backup_results']['summary']['failed_backups']}")

        except Exception as e:
            print(f"[ERROR] Full system backup failed: {e}")
            logger.error(f"Backup failed: {e}")

    async def backup_component(self, component: str, backup_type: str = "full") -> None:
        """Backup specific component."""
        orchestrator = await self._ensure_orchestrator()

        try:
            print(f"Starting {component} backup ({backup_type})...")

            if component == "database":
                from .backup_manager import BackupType
                result = await orchestrator.backup_manager.backup_database(
                    backup_type=BackupType(backup_type),
                    compress=True,
                    validate=True
                )
                print(f"[SUCCESS] Database backup completed: {result.backup_id}")

            elif component == "data_lake":
                from .backup_manager import BackupType
                results = await orchestrator.backup_manager.backup_data_lake(
                    backup_type=BackupType(backup_type),
                    compress=True
                )
                print(f"[SUCCESS] Data lake backup completed: {len(results)} layer(s) backed up")

            elif component == "configuration":
                result = await orchestrator.backup_manager.backup_configuration(compress=True)
                print(f"[SUCCESS] Configuration backup completed: {result.backup_id}")

            else:
                print(f"[ERROR] Unknown component: {component}")

        except Exception as e:
            print(f"[ERROR] Component backup failed: {e}")
            logger.error(f"Component backup failed: {e}")

    async def list_backups(self, component: str | None = None, limit: int = 10) -> None:
        """List available backups."""
        orchestrator = await self._ensure_orchestrator()

        try:
            backups = orchestrator.backup_manager.get_backup_history(
                component=component,
                limit=limit
            )

            if not backups:
                print("No backups found.")
                return

            print(f"\nAvailable Backups ({len(backups)} found):")
            print("-" * 80)
            print("| Backup ID               | Component    | Type      | Created         | Size    | Status    |")
            print("-" * 80)

            for backup in backups:
                size_mb = backup.get("size_bytes", 0) / 1024 / 1024
                component_name = backup.get("tags", {}).get("component", "unknown")
                status_icon = "[OK]" if backup.get("status") in ["completed", "validated"] else "[FAIL]"
                created = backup["timestamp"][:19].replace("T", " ")
                backup_id = backup['backup_id'][:22] + "..." if len(backup['backup_id']) > 25 else backup['backup_id']

                print(f"| {status_icon} {backup_id:<20} | {component_name:<12} | {backup.get('backup_type', 'unknown'):<9} | "
                      f"{created} | {size_mb:>6.1f}MB | {backup.get('status', 'unknown'):<9} |")

            print("-" * 80)

        except Exception as e:
            print(f"[ERROR] Failed to list backups: {e}")
            logger.error(f"List backups failed: {e}")

    async def show_status(self) -> None:
        """Show system status."""
        orchestrator = await self._ensure_orchestrator()

        try:
            status = await orchestrator.get_system_status()

            # System health
            health = status["system_health"]
            health_icon = {"healthy": "[HEALTHY]", "warning": "[WARNING]", "critical": "[CRITICAL]", "unknown": "[UNKNOWN]"}

            print(f"\nSystem Health: {health_icon.get(health, '[UNKNOWN]')} {health.upper()}")
            print(f"Active Operations: {status['active_operations']}")

            # Last operations
            if status.get("last_full_backup"):
                last_backup = datetime.fromisoformat(status["last_full_backup"]).strftime("%Y-%m-%d %H:%M:%S")
                print(f"Last Full Backup: {last_backup}")

            if status.get("last_dr_test"):
                last_test = datetime.fromisoformat(status["last_dr_test"]).strftime("%Y-%m-%d %H:%M:%S")
                print(f"Last DR Test: {last_test}")

            # Scheduler status
            scheduler = status["scheduler"]
            print("\nScheduler:")
            print(f"   Running: {'[YES]' if scheduler['scheduler_running'] else '[NO]'}")
            print(f"   Total Schedules: {scheduler['total_schedules']}")
            print(f"   Enabled Schedules: {scheduler['enabled_schedules']}")

            # Active alerts
            monitoring = status["monitoring"]
            if monitoring["active_alerts"] > 0:
                print(f"\nActive Alerts: {monitoring['active_alerts']}")

            # Storage backends
            if status["storage_backends"]:
                print("\nStorage Backends:")
                for backend_id, backend_stats in status["storage_backends"].items():
                    if "error" in backend_stats:
                        print(f"   {backend_id}: [ERROR] {backend_stats['error']}")
                    else:
                        total_size_mb = backend_stats.get("total_size_bytes", 0) / 1024 / 1024
                        print(f"   {backend_id}: {backend_stats.get('total_files', 0)} files, "
                              f"{total_size_mb:.1f} MB")

        except Exception as e:
            print(f"[ERROR] Failed to get system status: {e}")
            logger.error(f"Status check failed: {e}")

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self.orchestrator:
            await self.orchestrator.shutdown()


def print_help():
    """Print help information."""
    print("""
Backup and Disaster Recovery CLI

Usage: python -m src.core.backup.simple_cli <command> [options]

Commands:
    backup-full                    Perform full system backup
    backup <component>             Backup specific component (database, data_lake, configuration)
    list-backups [component]       List available backups
    status                        Show system status
    test                          Test backup system initialization
    help                          Show this help message

Examples:
    python -m src.core.backup.simple_cli backup-full
    python -m src.core.backup.simple_cli backup database
    python -m src.core.backup.simple_cli list-backups
    python -m src.core.backup.simple_cli status
""")


async def main():
    """Main CLI entry point."""
    args = sys.argv[1:]

    if not args or args[0] in ['help', '-h', '--help']:
        print_help()
        return

    command = args[0]
    backup_cli = SimpleBackupCLI()

    try:
        if command == "test":
            print("Testing backup system initialization...")
            orchestrator = await backup_cli._ensure_orchestrator()
            print("[SUCCESS] Backup system initialized successfully!")

        elif command == "backup-full":
            await backup_cli.backup_full_system()

        elif command == "backup":
            if len(args) < 2:
                print("[ERROR] Component required. Use: backup <database|data_lake|configuration>")
                return
            component = args[1]
            await backup_cli.backup_component(component)

        elif command == "list-backups":
            component = args[1] if len(args) > 1 else None
            await backup_cli.list_backups(component)

        elif command == "status":
            await backup_cli.show_status()

        else:
            print(f"[ERROR] Unknown command: {command}")
            print_help()

    except Exception as e:
        print(f"[ERROR] Command failed: {e}")
        logger.error(f"CLI command failed: {e}")

    finally:
        await backup_cli.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
