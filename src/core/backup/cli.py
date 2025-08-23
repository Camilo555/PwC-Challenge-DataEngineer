"""
Backup and Disaster Recovery Command Line Interface

Comprehensive CLI for backup and disaster recovery operations management.
"""

import asyncio
from datetime import datetime
from pathlib import Path

import click
from tabulate import tabulate

from ...core.config.base_config import BaseConfig, Environment
from ...core.logging import get_logger
from .monitoring import AlertSeverity
from .orchestrator import BackupOrchestrator, OrchestrationMode
from .validation import IntegrityLevel

logger = get_logger(__name__)


class BackupCLI:
    """Command line interface for backup operations."""

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

    async def backup_full_system(
        self,
        validate: bool = True,
        multi_storage: bool = False,
        mode: str = "manual"
    ) -> None:
        """Perform full system backup."""
        orchestrator = await self._ensure_orchestrator()

        try:
            click.echo("🔄 Starting full system backup...")

            results = await orchestrator.trigger_full_system_backup(
                mode=OrchestrationMode(mode),
                validate=validate,
                multi_storage=multi_storage
            )

            if results["success"]:
                click.echo("✅ Full system backup completed successfully!")
                click.echo(f"   Operation ID: {results['operation_id']}")
                click.echo(f"   Duration: {results['duration_seconds']:.1f} seconds")
                click.echo(f"   Size: {results['backup_results']['summary']['total_size_bytes'] / 1024 / 1024:.1f} MB")
            else:
                click.echo("❌ Full system backup completed with issues!")
                click.echo(f"   Failed backups: {results['backup_results']['summary']['failed_backups']}")

        except Exception as e:
            click.echo(f"❌ Full system backup failed: {e}")
            raise click.ClickException(str(e))

    async def backup_component(
        self,
        component: str,
        backup_type: str = "full",
        validate: bool = True
    ) -> None:
        """Backup specific component."""
        orchestrator = await self._ensure_orchestrator()

        try:
            click.echo(f"🔄 Starting {component} backup ({backup_type})...")

            if component == "database":
                from .backup_manager import BackupType
                result = await orchestrator.backup_manager.backup_database(
                    backup_type=BackupType(backup_type),
                    compress=True,
                    validate=validate
                )
                click.echo(f"✅ Database backup completed: {result.backup_id}")

            elif component == "data_lake":
                from .backup_manager import BackupType
                results = await orchestrator.backup_manager.backup_data_lake(
                    backup_type=BackupType(backup_type),
                    compress=True
                )
                click.echo(f"✅ Data lake backup completed: {len(results)} layer(s) backed up")

            elif component == "configuration":
                result = await orchestrator.backup_manager.backup_configuration(compress=True)
                click.echo(f"✅ Configuration backup completed: {result.backup_id}")

            else:
                raise click.ClickException(f"Unknown component: {component}")

        except Exception as e:
            click.echo(f"❌ Component backup failed: {e}")
            raise click.ClickException(str(e))

    async def restore_component(
        self,
        backup_id: str,
        destination: str | None = None,
        verify: bool = True
    ) -> None:
        """Restore from specific backup."""
        orchestrator = await self._ensure_orchestrator()

        try:
            click.echo(f"🔄 Starting restore from backup: {backup_id}")

            dest_path = Path(destination) if destination else None

            results = await orchestrator.recovery_manager.restore_from_backup(
                backup_id=backup_id,
                target_location=dest_path,
                verify_integrity=verify
            )

            if results["status"] == "completed":
                click.echo("✅ Restore completed successfully!")
                click.echo(f"   Recovery ID: {results['recovery_id']}")
                click.echo(f"   Duration: {results['duration_seconds']:.1f} seconds")

                if results["verification_results"]["overall_success"]:
                    click.echo("   Verification: ✅ Passed")
                else:
                    click.echo("   Verification: ⚠️  Issues detected")
            else:
                click.echo("❌ Restore failed!")
                click.echo(f"   Error: {results.get('error_message', 'Unknown error')}")

        except Exception as e:
            click.echo(f"❌ Restore failed: {e}")
            raise click.ClickException(str(e))

    async def restore_point_in_time(
        self,
        component: str,
        timestamp: str,
        destination: str | None = None
    ) -> None:
        """Restore component to point in time."""
        orchestrator = await self._ensure_orchestrator()

        try:
            target_time = datetime.fromisoformat(timestamp.replace('T', ' ').replace('Z', ''))
            dest_path = Path(destination) if destination else None

            click.echo(f"🔄 Starting point-in-time restore: {component} to {target_time}")

            results = await orchestrator.restore_point_in_time(
                component=component,
                target_timestamp=target_time,
                destination=dest_path
            )

            if results["restore_results"]["status"] == "completed":
                click.echo("✅ Point-in-time restore completed!")
                click.echo(f"   Operation ID: {results['operation_id']}")
                click.echo(f"   Backup used: {results['backup_used']}")
                click.echo(f"   Time difference: {results['time_difference_seconds']:.0f} seconds")
                click.echo(f"   Duration: {results['duration_seconds']:.1f} seconds")
            else:
                click.echo("❌ Point-in-time restore failed!")

        except ValueError as e:
            click.echo(f"❌ Invalid timestamp format: {timestamp}")
            click.echo("   Expected format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SSZ")
            raise click.ClickException(str(e))
        except Exception as e:
            click.echo(f"❌ Point-in-time restore failed: {e}")
            raise click.ClickException(str(e))

    async def execute_disaster_recovery(
        self,
        plan_id: str,
        simulate: bool = False
    ) -> None:
        """Execute disaster recovery plan."""
        orchestrator = await self._ensure_orchestrator()

        try:
            if not simulate:
                click.confirm(
                    f"⚠️  This will execute REAL disaster recovery for plan: {plan_id}\n"
                    "   This operation may affect production systems.\n"
                    "   Do you want to continue?",
                    abort=True
                )

            click.echo(f"🚨 {'Simulating' if simulate else 'Executing'} disaster recovery plan: {plan_id}")

            results = await orchestrator.execute_disaster_recovery(
                plan_id=plan_id,
                simulate=simulate
            )

            dr_results = results["dr_results"]

            if dr_results["status"] == "completed":
                click.echo(f"✅ Disaster recovery {'simulation' if simulate else 'execution'} completed!")
                click.echo(f"   Execution ID: {results['operation_id']}")
                click.echo(f"   Duration: {results['total_duration']:.1f} seconds")
                click.echo(f"   Steps completed: {len(dr_results['steps'])}")

                # Show step details
                steps_table = []
                for step in dr_results["steps"]:
                    status_icon = "✅" if step["status"] in ["completed", "simulated"] else "❌"
                    steps_table.append([
                        f"{status_icon} {step['step']}",
                        step["name"],
                        f"{step.get('actual_minutes', 0):.1f}m",
                        step["status"]
                    ])

                click.echo("\nExecution Steps:")
                click.echo(tabulate(
                    steps_table,
                    headers=["Step", "Name", "Duration", "Status"],
                    tablefmt="grid"
                ))

            else:
                click.echo(f"❌ Disaster recovery {'simulation' if simulate else 'execution'} failed!")
                click.echo(f"   Error: {dr_results.get('error', 'Unknown error')}")

        except click.Abort:
            click.echo("Disaster recovery execution aborted.")
        except Exception as e:
            click.echo(f"❌ Disaster recovery failed: {e}")
            raise click.ClickException(str(e))

    async def list_backups(
        self,
        component: str | None = None,
        limit: int = 20
    ) -> None:
        """List available backups."""
        orchestrator = await self._ensure_orchestrator()

        try:
            backups = orchestrator.backup_manager.get_backup_history(
                component=component,
                limit=limit
            )

            if not backups:
                click.echo("No backups found.")
                return

            # Prepare table data
            table_data = []
            for backup in backups:
                size_mb = backup.get("size_bytes", 0) / 1024 / 1024
                component_name = backup.get("tags", {}).get("component", "unknown")
                status_icon = "✅" if backup.get("status") in ["completed", "validated"] else "❌"

                table_data.append([
                    f"{status_icon} {backup['backup_id'][:20]}...",
                    component_name,
                    backup.get("backup_type", "unknown"),
                    backup["timestamp"][:19].replace("T", " "),
                    f"{size_mb:.1f} MB",
                    backup.get("status", "unknown")
                ])

            click.echo(f"\nAvailable Backups ({len(backups)} found):")
            click.echo(tabulate(
                table_data,
                headers=["Backup ID", "Component", "Type", "Created", "Size", "Status"],
                tablefmt="grid"
            ))

        except Exception as e:
            click.echo(f"❌ Failed to list backups: {e}")
            raise click.ClickException(str(e))

    async def validate_backup(
        self,
        backup_id: str,
        integrity_level: str = "standard"
    ) -> None:
        """Validate specific backup."""
        orchestrator = await self._ensure_orchestrator()

        try:
            # Find backup metadata
            backups = orchestrator.backup_manager.get_backup_history(limit=500)
            backup_metadata = None

            for backup in backups:
                if backup.get("backup_id") == backup_id:
                    backup_metadata = backup
                    break

            if not backup_metadata:
                raise click.ClickException(f"Backup not found: {backup_id}")

            backup_path = Path(backup_metadata["destination_path"])

            click.echo(f"🔍 Validating backup: {backup_id}")
            click.echo(f"   Integrity level: {integrity_level}")

            report = await orchestrator.validator.validate_backup(
                backup_path,
                backup_metadata,
                IntegrityLevel(integrity_level)
            )

            # Display results
            if report.overall_result.value == "passed":
                click.echo("✅ Backup validation passed!")
            elif report.overall_result.value == "warning":
                click.echo("⚠️  Backup validation completed with warnings!")
            else:
                click.echo("❌ Backup validation failed!")

            click.echo(f"   Success rate: {report.success_rate:.1f}%")
            click.echo(f"   Checks performed: {len(report.checks)}")
            click.echo(f"   Passed: {report.passed_checks}")
            click.echo(f"   Failed: {report.failed_checks}")
            click.echo(f"   Warnings: {report.warning_checks}")

            # Show failed checks
            if report.failed_checks > 0:
                click.echo("\nFailed Checks:")
                for check in report.checks:
                    if check.result.value == "failed":
                        click.echo(f"   ❌ {check.check_name}: {check.message}")

            # Show warning checks
            if report.warning_checks > 0:
                click.echo("\nWarning Checks:")
                for check in report.checks:
                    if check.result.value == "warning":
                        click.echo(f"   ⚠️  {check.check_name}: {check.message}")

        except Exception as e:
            click.echo(f"❌ Backup validation failed: {e}")
            raise click.ClickException(str(e))

    async def show_status(self) -> None:
        """Show system status."""
        orchestrator = await self._ensure_orchestrator()

        try:
            status = await orchestrator.get_system_status()

            # System health
            health = status["system_health"]
            health_icon = {"healthy": "✅", "warning": "⚠️", "critical": "❌", "unknown": "❓"}

            click.echo(f"\n🏥 System Health: {health_icon.get(health, '❓')} {health.upper()}")

            # Active operations
            click.echo(f"🔄 Active Operations: {status['active_operations']}")

            # Last operations
            if status.get("last_full_backup"):
                last_backup = datetime.fromisoformat(status["last_full_backup"]).strftime("%Y-%m-%d %H:%M:%S")
                click.echo(f"💾 Last Full Backup: {last_backup}")

            if status.get("last_dr_test"):
                last_test = datetime.fromisoformat(status["last_dr_test"]).strftime("%Y-%m-%d %H:%M:%S")
                click.echo(f"🚨 Last DR Test: {last_test}")

            # Scheduler status
            scheduler = status["scheduler"]
            click.echo("\n📅 Scheduler:")
            click.echo(f"   Running: {'✅' if scheduler['scheduler_running'] else '❌'}")
            click.echo(f"   Total Schedules: {scheduler['total_schedules']}")
            click.echo(f"   Enabled Schedules: {scheduler['enabled_schedules']}")

            # Active alerts
            monitoring = status["monitoring"]
            if monitoring["active_alerts"] > 0:
                click.echo(f"\n🚨 Active Alerts: {monitoring['active_alerts']}")

            # SLA compliance
            sla = status["sla_compliance"]
            periods = sla["periods"]

            click.echo("\n📊 SLA Compliance (24h/7d/30d):")
            click.echo(f"   Backup Success: "
                      f"{periods['24_hours']['backup_success_rate']:.1f}% / "
                      f"{periods['7_days']['backup_success_rate']:.1f}% / "
                      f"{periods['30_days']['backup_success_rate']:.1f}%")

            # Storage backends
            if status["storage_backends"]:
                click.echo("\n💽 Storage Backends:")
                for backend_id, backend_stats in status["storage_backends"].items():
                    if "error" in backend_stats:
                        click.echo(f"   {backend_id}: ❌ Error - {backend_stats['error']}")
                    else:
                        total_size_mb = backend_stats.get("total_size_bytes", 0) / 1024 / 1024
                        click.echo(f"   {backend_id}: {backend_stats.get('total_files', 0)} files, "
                                  f"{total_size_mb:.1f} MB")

        except Exception as e:
            click.echo(f"❌ Failed to get system status: {e}")
            raise click.ClickException(str(e))

    async def show_alerts(
        self,
        severity: str | None = None,
        limit: int = 10
    ) -> None:
        """Show active alerts."""
        orchestrator = await self._ensure_orchestrator()

        try:
            severity_filter = AlertSeverity(severity) if severity else None
            alerts = await orchestrator.monitor.get_active_alerts(severity_filter)

            if not alerts:
                click.echo("✅ No active alerts.")
                return

            # Limit results
            alerts = alerts[:limit]

            # Prepare table
            table_data = []
            for alert in alerts:
                severity_icons = {
                    "critical": "🚨",
                    "high": "❗",
                    "medium": "⚠️",
                    "low": "ℹ️",
                    "info": "💡"
                }

                severity_icon = severity_icons.get(alert["severity"], "❓")
                timestamp = alert["timestamp"][:19].replace("T", " ")

                table_data.append([
                    alert["alert_id"][:12] + "...",
                    f"{severity_icon} {alert['severity'].upper()}",
                    alert["component"],
                    alert["title"][:30] + ("..." if len(alert["title"]) > 30 else ""),
                    timestamp
                ])

            click.echo(f"\n🚨 Active Alerts ({len(alerts)} shown):")
            click.echo(tabulate(
                table_data,
                headers=["Alert ID", "Severity", "Component", "Title", "Created"],
                tablefmt="grid"
            ))

        except Exception as e:
            click.echo(f"❌ Failed to show alerts: {e}")
            raise click.ClickException(str(e))

    async def run_maintenance(self) -> None:
        """Run maintenance tasks."""
        orchestrator = await self._ensure_orchestrator()

        try:
            click.echo("🔧 Running backup system maintenance...")

            results = await orchestrator.run_maintenance_tasks()

            click.echo(f"✅ Maintenance completed in {results['total_duration']:.1f} seconds")

            # Show task results
            for task_name, task_result in results["tasks"].items():
                click.echo(f"\n   📋 {task_name.replace('_', ' ').title()}:")

                if isinstance(task_result, dict):
                    for key, value in task_result.items():
                        if isinstance(value, (int, float)):
                            click.echo(f"      {key}: {value}")
                        elif isinstance(value, dict):
                            click.echo(f"      {key}: {len(value)} items")
                        else:
                            click.echo(f"      {key}: {value}")
                else:
                    click.echo(f"      {task_result}")

        except Exception as e:
            click.echo(f"❌ Maintenance failed: {e}")
            raise click.ClickException(str(e))

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self.orchestrator:
            await self.orchestrator.shutdown()


# CLI Commands
@click.group()
@click.option("--config-path", type=click.Path(exists=True), help="Path to configuration directory")
@click.pass_context
def cli(ctx, config_path):
    """Backup and Disaster Recovery CLI Tool"""
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = Path(config_path) if config_path else None


@cli.command()
@click.option("--validate/--no-validate", default=True, help="Validate backups after completion")
@click.option("--multi-storage/--single-storage", default=False, help="Store in multiple backends")
@click.option("--mode", type=click.Choice(["manual", "scheduled", "emergency"]), default="manual")
@click.pass_context
def backup_full(ctx, validate, multi_storage, mode):
    """Perform full system backup"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.backup_full_system(validate, multi_storage, mode)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.argument("component", type=click.Choice(["database", "data_lake", "configuration"]))
@click.option("--type", "backup_type", type=click.Choice(["full", "incremental", "differential"]), default="full")
@click.option("--validate/--no-validate", default=True, help="Validate backup after completion")
@click.pass_context
def backup(ctx, component, backup_type, validate):
    """Backup specific component"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.backup_component(component, backup_type, validate)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.argument("backup_id")
@click.option("--destination", help="Custom restore destination")
@click.option("--verify/--no-verify", default=True, help="Verify backup integrity")
@click.pass_context
def restore(ctx, backup_id, destination, verify):
    """Restore from specific backup"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.restore_component(backup_id, destination, verify)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.argument("component", type=click.Choice(["database", "data_lake", "configuration"]))
@click.argument("timestamp")
@click.option("--destination", help="Custom restore destination")
@click.pass_context
def restore_pit(ctx, component, timestamp, destination):
    """Restore component to point in time (YYYY-MM-DD HH:MM:SS)"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.restore_point_in_time(component, timestamp, destination)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.argument("plan_id")
@click.option("--simulate/--execute", default=True, help="Simulate or execute disaster recovery")
@click.pass_context
def disaster_recovery(ctx, plan_id, simulate):
    """Execute disaster recovery plan"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.execute_disaster_recovery(plan_id, simulate)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.option("--component", help="Filter by component")
@click.option("--limit", default=20, help="Maximum number of backups to show")
@click.pass_context
def list_backups(ctx, component, limit):
    """List available backups"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.list_backups(component, limit)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.argument("backup_id")
@click.option("--level", type=click.Choice(["basic", "standard", "thorough", "forensic"]), default="standard")
@click.pass_context
def validate(ctx, backup_id, level):
    """Validate specific backup"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.validate_backup(backup_id, level)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.pass_context
def status(ctx):
    """Show system status"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.show_status()
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.option("--severity", type=click.Choice(["critical", "high", "medium", "low", "info"]))
@click.option("--limit", default=10, help="Maximum number of alerts to show")
@click.pass_context
def alerts(ctx, severity, limit):
    """Show active alerts"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.show_alerts(severity, limit)
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


@cli.command()
@click.pass_context
def maintenance(ctx):
    """Run maintenance tasks"""
    backup_cli = BackupCLI(ctx.obj['config_path'])

    async def run():
        try:
            await backup_cli.run_maintenance()
        finally:
            await backup_cli.cleanup()

    asyncio.run(run())


if __name__ == "__main__":
    cli()
