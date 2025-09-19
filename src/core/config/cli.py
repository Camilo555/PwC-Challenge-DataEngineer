#!/usr/bin/env python3
"""
Configuration Management CLI
============================

Command-line interface for managing the centralized configuration system.
Provides tools for validation, environment setup, and configuration monitoring.
"""
import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

import click
import yaml

from .config_integration import get_integrated_config


@click.group()
def config_cli():
    """Configuration Management CLI for the centralized configuration system."""
    pass


@config_cli.command()
@click.option('--format', type=click.Choice(['json', 'yaml']), default='yaml', help='Output format')
@click.option('--include-secrets', is_flag=True, help='Include secret values in output')
def show(format: str, include_secrets: bool):
    """Show current configuration summary."""
    try:
        config_manager = get_integrated_config()
        summary = config_manager.export_configuration_summary()

        if format == 'json':
            click.echo(json.dumps(summary, indent=2, default=str))
        else:
            click.echo(yaml.dump(summary, default_flow_style=False, indent=2))

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@config_cli.command()
@click.option('--output-dir', type=click.Path(), help='Output directory for config files')
def validate(output_dir: Optional[str]):
    """Validate all configuration systems."""
    async def run_validation():
        try:
            config_manager = get_integrated_config()
            results = await config_manager.validate_all_configurations()

            # Display results
            click.echo("Configuration Validation Results")
            click.echo("=" * 50)

            overall_status = results.get("overall_status", "unknown")
            status_color = "green" if overall_status == "valid" else "red"
            click.echo(f"Overall Status: {click.style(overall_status.upper(), fg=status_color)}")
            click.echo()

            # Detailed results
            for system, result in results.items():
                if system == "overall_status":
                    continue

                click.echo(f"{system.replace('_', ' ').title()}:")
                status = result.get("status", "unknown")
                status_color = "green" if status == "valid" else "yellow" if status == "disabled" else "red"
                click.echo(f"  Status: {click.style(status.upper(), fg=status_color)}")

                if "details" in result:
                    for detail_name, detail in result["details"].items():
                        detail_status = detail.get("status", "unknown")
                        detail_color = "green" if detail_status == "valid" else "red"
                        click.echo(f"    {detail_name}: {click.style(detail_status, fg=detail_color)}")
                        if detail.get("errors"):
                            for error in detail["errors"]:
                                click.echo(f"      - {error}")

                if "errors" in result and result["errors"]:
                    for error in result["errors"]:
                        click.echo(f"    Error: {error}")

                if "features" in result:
                    click.echo("    Features:")
                    for feature, enabled in result["features"].items():
                        feature_color = "green" if enabled else "yellow"
                        click.echo(f"      {feature}: {click.style(str(enabled), fg=feature_color)}")

                click.echo()

            # Save results if output directory specified
            if output_dir:
                output_path = Path(output_dir)
                output_path.mkdir(parents=True, exist_ok=True)

                results_file = output_path / "validation_results.json"
                with open(results_file, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                click.echo(f"Validation results saved to: {results_file}")

            return overall_status == "valid"

        except Exception as e:
            click.echo(f"Validation failed: {e}", err=True)
            return False

    success = asyncio.run(run_validation())
    if not success:
        sys.exit(1)


@config_cli.command()
@click.argument('environment', type=click.Choice(['development', 'staging', 'production', 'testing']))
@click.option('--output-dir', type=click.Path(), default='config', help='Output directory for config templates')
def create_env(environment: str, output_dir: str):
    """Create configuration templates for an environment."""
    try:
        config_manager = get_integrated_config()
        output_path = Path(output_dir)

        config_manager.create_environment_template(environment, output_path)

        click.echo(f"Configuration templates created for {environment} environment in: {output_path}")
        click.echo("\nGenerated files:")
        for file in output_path.iterdir():
            if file.is_file():
                click.echo(f"  - {file.name}")

    except Exception as e:
        click.echo(f"Error creating environment templates: {e}", err=True)
        sys.exit(1)


@config_cli.command()
@click.argument('key')
@click.argument('value', required=False)
def config(key: str, value: Optional[str]):
    """Get or set configuration values."""
    try:
        config_manager = get_integrated_config()

        if value is None:
            # Get configuration value
            current_value = config_manager.get_config_value(key)
            if current_value is not None:
                click.echo(f"{key}: {current_value}")
            else:
                click.echo(f"Configuration key '{key}' not found")
                sys.exit(1)
        else:
            # Set configuration value
            # Try to parse value as JSON for complex types
            try:
                parsed_value = json.loads(value)
            except json.JSONDecodeError:
                parsed_value = value

            config_manager.set_config_value(key, parsed_value)
            click.echo(f"Set {key} = {parsed_value}")

    except Exception as e:
        click.echo(f"Error accessing configuration: {e}", err=True)
        sys.exit(1)


@config_cli.command()
def features():
    """Show all feature flags and their status."""
    try:
        config_manager = get_integrated_config()
        feature_flags = config_manager.get_feature_flags()

        click.echo("Feature Flags Status")
        click.echo("=" * 30)

        for field_name, field_info in feature_flags.__fields__.items():
            enabled = getattr(feature_flags, field_name)
            status_color = "green" if enabled else "red"
            click.echo(f"{field_name}: {click.style(str(enabled), fg=status_color)}")

    except Exception as e:
        click.echo(f"Error retrieving feature flags: {e}", err=True)
        sys.exit(1)


@config_cli.command()
def performance():
    """Show performance optimization settings."""
    try:
        config_manager = get_integrated_config()
        perf_settings = config_manager.get_performance_settings()

        click.echo("Performance Settings")
        click.echo("=" * 25)

        settings_dict = perf_settings.dict()
        for category in ['Database', 'Caching', 'API', 'Memory', 'Async']:
            category_settings = {k: v for k, v in settings_dict.items()
                               if category.lower() in k.lower()}

            if category_settings:
                click.echo(f"\n{category}:")
                for key, value in category_settings.items():
                    click.echo(f"  {key}: {value}")

    except Exception as e:
        click.echo(f"Error retrieving performance settings: {e}", err=True)
        sys.exit(1)


@config_cli.command()
def resources():
    """Show resource limits and current usage."""
    try:
        config_manager = get_integrated_config()
        resource_limits = config_manager.get_resource_limits()

        click.echo("Resource Limits")
        click.echo("=" * 20)

        limits_dict = resource_limits.dict()
        for key, value in limits_dict.items():
            if value is not None:
                if 'memory' in key.lower():
                    click.echo(f"{key}: {value} MB")
                elif 'percent' in key.lower():
                    click.echo(f"{key}: {value}%")
                elif 'connections' in key.lower():
                    click.echo(f"{key}: {value} connections")
                else:
                    click.echo(f"{key}: {value}")
            else:
                click.echo(f"{key}: No limit set")

    except Exception as e:
        click.echo(f"Error retrieving resource limits: {e}", err=True)
        sys.exit(1)


@config_cli.command()
@click.option('--format', type=click.Choice(['json', 'yaml']), default='yaml', help='Output format')
@click.option('--output-file', type=click.Path(), help='Output file path')
def export(format: str, output_file: Optional[str]):
    """Export complete configuration to file."""
    try:
        config_manager = get_integrated_config()
        summary = config_manager.export_configuration_summary()

        if format == 'json':
            content = json.dumps(summary, indent=2, default=str)
        else:
            content = yaml.dump(summary, default_flow_style=False, indent=2)

        if output_file:
            with open(output_file, 'w') as f:
                f.write(content)
            click.echo(f"Configuration exported to: {output_file}")
        else:
            click.echo(content)

    except Exception as e:
        click.echo(f"Error exporting configuration: {e}", err=True)
        sys.exit(1)


@config_cli.command()
def doctor():
    """Run configuration health check and provide recommendations."""
    async def run_doctor():
        try:
            config_manager = get_integrated_config()

            click.echo("Configuration System Health Check")
            click.echo("=" * 45)

            # Check configuration validity
            validation_results = await config_manager.validate_all_configurations()
            overall_status = validation_results.get("overall_status", "unknown")

            if overall_status == "valid":
                click.echo(click.style("✓ Configuration system is healthy", fg="green"))
            else:
                click.echo(click.style("✗ Configuration system has issues", fg="red"))

            # Check for common issues and recommendations
            recommendations = []

            # Check if enhanced config is available
            if not validation_results.get("enhanced_config", {}).get("status") == "valid":
                recommendations.append(
                    "Consider enabling enhanced configuration for hot reloading and secrets management"
                )

            # Check performance settings
            perf_settings = config_manager.get_performance_settings()
            if perf_settings.db_pool_size < 10:
                recommendations.append(
                    "Database pool size is small - consider increasing for better performance"
                )

            # Check feature flags
            feature_flags = config_manager.get_feature_flags()
            if not feature_flags.enable_query_caching:
                recommendations.append(
                    "Query caching is disabled - enabling it can improve performance"
                )

            # Display recommendations
            if recommendations:
                click.echo("\nRecommendations:")
                for i, rec in enumerate(recommendations, 1):
                    click.echo(f"{i}. {rec}")
            else:
                click.echo(click.style("\n✓ No recommendations - system is optimally configured", fg="green"))

            return overall_status == "valid"

        except Exception as e:
            click.echo(f"Health check failed: {e}", err=True)
            return False

    success = asyncio.run(run_doctor())
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    config_cli()