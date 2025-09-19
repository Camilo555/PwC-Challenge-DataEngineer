#!/usr/bin/env python3
"""
Centralized Configuration System Usage Examples
===============================================

This example demonstrates how to use the centralized configuration management
system with hot reloading, performance optimization, and feature flags.
"""
import asyncio
from pathlib import Path

# Import the centralized configuration system
from src.core.config import (
    config_manager,
    integrated_settings,
    get_config_value,
    is_feature_enabled,
    get_database_config,
    get_api_config
)


async def main():
    """Main example function."""
    print("Centralized Configuration System Examples")
    print("=" * 50)

    # 1. Basic Configuration Access
    print("\n1. Basic Configuration Access")
    print("-" * 30)

    # Get configuration values using the integrated system
    db_url = get_config_value("database.url", "default_db_url")
    api_port = get_config_value("api.port", 8000)
    log_level = get_config_value("logging.level", "INFO")

    print(f"Database URL: {db_url}")
    print(f"API Port: {api_port}")
    print(f"Log Level: {log_level}")

    # 2. Feature Flags
    print("\n2. Feature Flag Usage")
    print("-" * 30)

    # Check if specific features are enabled
    caching_enabled = is_feature_enabled("enable_query_caching")
    monitoring_enabled = is_feature_enabled("enable_performance_monitoring")
    compression_enabled = is_feature_enabled("enable_response_compression")

    print(f"Query Caching: {'✓' if caching_enabled else '✗'}")
    print(f"Performance Monitoring: {'✓' if monitoring_enabled else '✗'}")
    print(f"Response Compression: {'✓' if compression_enabled else '✗'}")

    # 3. Performance Settings
    print("\n3. Performance Configuration")
    print("-" * 30)

    perf_settings = config_manager.get_performance_settings()
    print(f"Database Pool Size: {perf_settings.db_pool_size}")
    print(f"Cache TTL: {perf_settings.cache_default_ttl}s")
    print(f"API Timeout: {perf_settings.api_timeout}s")
    print(f"Max Workers: {perf_settings.max_workers}")

    # 4. Service-Specific Configurations
    print("\n4. Service Configurations")
    print("-" * 30)

    # Get optimized database configuration
    db_config = get_database_config()
    print(f"Database Configuration:")
    print(f"  URL: {db_config['url'][:50]}...")
    print(f"  Pool Size: {db_config['pool_size']}")
    print(f"  Max Overflow: {db_config['max_overflow']}")
    print(f"  Pool Timeout: {db_config['pool_timeout']}s")

    # Get optimized API configuration
    api_config = get_api_config()
    print(f"\nAPI Configuration:")
    print(f"  Host: {api_config['host']}")
    print(f"  Port: {api_config['port']}")
    print(f"  Workers: {api_config['workers']}")
    print(f"  Timeout: {api_config['timeout']}s")

    # 5. Resource Limits
    print("\n5. Resource Limits")
    print("-" * 30)

    resource_limits = config_manager.get_resource_limits()
    if resource_limits.max_memory_mb:
        print(f"Memory Limit: {resource_limits.max_memory_mb} MB")
    if resource_limits.max_cpu_percent:
        print(f"CPU Limit: {resource_limits.max_cpu_percent}%")
    if resource_limits.max_connections:
        print(f"Connection Limit: {resource_limits.max_connections}")

    # 6. Environment Information
    print("\n6. Environment Summary")
    print("-" * 30)

    env_summary = config_manager.unified_config.get_environment_summary()
    print(f"Environment: {env_summary['environment']}")
    print(f"Processing Engine: {env_summary['processing_engine']}")
    print(f"Production Mode: {env_summary['production_mode']}")
    print(f"Debug Mode: {env_summary['debug_mode']}")

    # 7. Configuration Validation
    print("\n7. Configuration Validation")
    print("-" * 30)

    validation_results = await config_manager.validate_all_configurations()
    overall_status = validation_results.get("overall_status", "unknown")
    print(f"Overall Status: {overall_status.upper()}")

    for system, result in validation_results.items():
        if system != "overall_status":
            status = result.get("status", "unknown")
            print(f"  {system.replace('_', ' ').title()}: {status}")

    # 8. Runtime Configuration Changes
    print("\n8. Runtime Configuration Changes")
    print("-" * 30)

    # Demonstrate setting configuration values at runtime
    original_value = get_config_value("api.timeout", 30)
    print(f"Original API timeout: {original_value}")

    # Set new value (this will work if enhanced config is available)
    config_manager.set_config_value("api.timeout", 45)
    new_value = get_config_value("api.timeout", 30)
    print(f"Updated API timeout: {new_value}")

    # 9. Export Configuration Summary
    print("\n9. Configuration Export")
    print("-" * 30)

    summary = config_manager.export_configuration_summary()
    print(f"Configuration categories: {list(summary.keys())}")

    integration_status = summary.get("integration_status", {})
    print(f"Hot reload enabled: {integration_status.get('hot_reload_enabled', False)}")
    print(f"Secrets enabled: {integration_status.get('secrets_enabled', False)}")

    # 10. Feature-Conditional Logic
    print("\n10. Feature-Conditional Logic Example")
    print("-" * 30)

    # Example of using feature flags to conditionally enable functionality
    if is_feature_enabled("enable_query_caching"):
        print("✓ Query caching is enabled - using cached results")
        # In real code, you would use caching here
    else:
        print("✗ Query caching is disabled - querying database directly")

    if is_feature_enabled("enable_performance_monitoring"):
        print("✓ Performance monitoring enabled - collecting metrics")
        # In real code, you would start performance monitoring
    else:
        print("✗ Performance monitoring disabled - no metrics collection")

    print(f"\n{'='*50}")
    print("Configuration system demonstration completed!")


def demonstrate_config_templates():
    """Demonstrate creating configuration templates."""
    print("\nConfiguration Template Generation")
    print("=" * 40)

    # Create configuration templates for different environments
    environments = ["development", "staging", "production"]

    for env in environments:
        output_dir = Path(f"config_templates/{env}")
        try:
            config_manager.create_environment_template(env, output_dir)
            print(f"✓ Created {env} configuration template in {output_dir}")
        except Exception as e:
            print(f"✗ Failed to create {env} template: {e}")


if __name__ == "__main__":
    # Run the main examples
    asyncio.run(main())

    # Demonstrate template generation
    demonstrate_config_templates()

    print("\nTo run the configuration CLI:")
    print("python -m src.core.config.cli --help")