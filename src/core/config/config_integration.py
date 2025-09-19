"""
Configuration Integration Module
================================

Integrates the unified configuration system with the enhanced configuration manager
to provide hot reloading, secrets management, and comprehensive configuration
management across the entire application.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Dict, Optional
from functools import lru_cache

from .unified_config import UnifiedConfig, get_unified_config
from .enhanced_config_manager import (
    EnhancedConfigManager,
    get_config as get_enhanced_config,
    initialize_config,
    PerformanceSettings,
    FeatureFlags,
    ResourceLimits
)
import logging

logger = logging.getLogger(__name__)


class IntegratedConfigManager:
    """
    Integrated configuration manager that combines the unified config system
    with the enhanced configuration manager for complete configuration control.
    """

    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the integrated configuration manager."""
        self.config_dir = config_dir or Path("config")
        self._unified_config: Optional[UnifiedConfig] = None
        self._enhanced_config: Optional[EnhancedConfigManager] = None
        self._is_initialized = False

        # Initialize configurations
        self._initialize()

    def _initialize(self) -> None:
        """Initialize both configuration systems."""
        try:
            # Initialize unified config first
            self._unified_config = get_unified_config()

            # Initialize enhanced config manager
            environment = os.getenv("ENVIRONMENT", "development")
            self._enhanced_config = initialize_config(
                config_dir=self.config_dir,
                environment=environment
            )

            # Setup configuration sync
            self._setup_config_sync()

            self._is_initialized = True
            logger.info("Integrated configuration manager initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize integrated config manager: {e}")
            # Fallback to unified config only
            self._unified_config = get_unified_config()
            self._enhanced_config = None

    def _setup_config_sync(self) -> None:
        """Setup synchronization between configuration systems."""
        if not self._enhanced_config:
            return

        # Register callback to sync enhanced config changes to unified config
        self._enhanced_config.add_reload_callback(self._on_enhanced_config_reload)

    async def _on_enhanced_config_reload(self, enhanced_config: EnhancedConfigManager) -> None:
        """Handle enhanced configuration reload events."""
        try:
            logger.info("Syncing enhanced configuration changes to unified config")

            # Get updated values from enhanced config
            updates = {}

            # Sync database configuration
            if db_url := enhanced_config.get("database.url"):
                updates["database_url"] = db_url

            # Sync API configuration
            if api_host := enhanced_config.get("api.host"):
                updates["api_host"] = api_host
            if api_port := enhanced_config.get("api.port"):
                updates["api_port"] = api_port

            # Sync logging configuration
            if log_level := enhanced_config.get("logging.level"):
                updates["log_level"] = log_level

            # Apply updates to unified config
            if updates and self._unified_config:
                for key, value in updates.items():
                    if hasattr(self._unified_config.base, key):
                        setattr(self._unified_config.base, key, value)

            logger.info(f"Applied {len(updates)} configuration updates from hot reload")

        except Exception as e:
            logger.error(f"Failed to sync configuration changes: {e}")

    @property
    def unified_config(self) -> UnifiedConfig:
        """Get the unified configuration instance."""
        if not self._unified_config:
            self._unified_config = get_unified_config()
        return self._unified_config

    @property
    def enhanced_config(self) -> Optional[EnhancedConfigManager]:
        """Get the enhanced configuration manager."""
        return self._enhanced_config

    def get_performance_settings(self) -> PerformanceSettings:
        """Get performance optimization settings."""
        if self._enhanced_config:
            return self._enhanced_config.performance
        return PerformanceSettings()

    def get_feature_flags(self) -> FeatureFlags:
        """Get feature flags configuration."""
        if self._enhanced_config:
            return self._enhanced_config.feature_flags
        return FeatureFlags()

    def get_resource_limits(self) -> ResourceLimits:
        """Get resource limits configuration."""
        if self._enhanced_config:
            return self._enhanced_config.resource_limits
        return ResourceLimits()

    def is_feature_enabled(self, feature_name: str) -> bool:
        """Check if a specific feature is enabled."""
        flags = self.get_feature_flags()
        return getattr(flags, feature_name, False)

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get configuration value with priority: enhanced -> unified -> default."""
        # Try enhanced config first (supports dot notation)
        if self._enhanced_config:
            value = self._enhanced_config.get(key, None)
            if value is not None:
                return value

        # Try unified config (convert dot notation to attribute access)
        if self._unified_config:
            try:
                keys = key.split('.')

                # Handle special mappings
                if keys[0] == "database":
                    if keys[1] == "url":
                        return self._unified_config.base.database_url
                elif keys[0] == "api":
                    if keys[1] == "host":
                        return getattr(self._unified_config.base, 'api_host', '0.0.0.0')
                    elif keys[1] == "port":
                        return getattr(self._unified_config.base, 'api_port', 8000)
                elif keys[0] == "logging":
                    if keys[1] == "level":
                        return getattr(self._unified_config.base, 'log_level', 'INFO')

                # General attribute access
                obj = self._unified_config.base
                for k in keys:
                    if hasattr(obj, k):
                        obj = getattr(obj, k)
                    else:
                        return default
                return obj

            except (AttributeError, IndexError):
                pass

        return default

    def set_config_value(self, key: str, value: Any) -> None:
        """Set configuration value at runtime."""
        if self._enhanced_config:
            self._enhanced_config.set(key, value)
        else:
            # Fallback to unified config if possible
            logger.warning(f"Hot configuration updates not available, change not persisted: {key}={value}")

    def get_database_config(self) -> Dict[str, Any]:
        """Get comprehensive database configuration."""
        base_config = self.unified_config.base
        perf_settings = self.get_performance_settings()

        return {
            "url": base_config.database_url,
            "async_url": base_config.get_database_url(async_mode=True),
            "pool_size": perf_settings.db_pool_size,
            "max_overflow": perf_settings.db_max_overflow,
            "pool_timeout": perf_settings.db_pool_timeout,
            "pool_recycle": perf_settings.db_pool_recycle,
            "jdbc_url": base_config.jdbc_url,
            "jdbc_properties": base_config.jdbc_properties
        }

    def get_api_config(self) -> Dict[str, Any]:
        """Get comprehensive API configuration."""
        base_config = self.unified_config.base
        perf_settings = self.get_performance_settings()

        return {
            "host": getattr(base_config, 'api_host', '0.0.0.0'),
            "port": getattr(base_config, 'api_port', 8000),
            "workers": getattr(base_config, 'api_workers', 4),
            "reload": getattr(base_config, 'debug', False),
            "timeout": perf_settings.api_timeout,
            "max_retries": perf_settings.api_max_retries,
            "compression_threshold": perf_settings.api_compression_threshold
        }

    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration with performance optimizations."""
        base_spark_config = self.unified_config.spark.get_spark_config()
        perf_settings = self.get_performance_settings()

        # Merge performance settings
        base_spark_config.update({
            "spark.executor.memory": f"{min(perf_settings.memory_limit_mb or 2048, 4096)}m",
            "spark.driver.memory": f"{min(perf_settings.memory_limit_mb or 1024, 2048)}m",
            "spark.sql.adaptive.coalescePartitions.parallelismFirst": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true"
        })

        return base_spark_config

    async def validate_all_configurations(self) -> Dict[str, Any]:
        """Validate all configuration systems and return comprehensive results."""
        results = {
            "unified_config": {},
            "enhanced_config": {},
            "integration": {},
            "overall_status": "unknown"
        }

        # Validate unified config
        try:
            unified_validation = self.unified_config.validate_all()
            results["unified_config"] = {
                "status": "valid" if all(r["status"] == "valid" for r in unified_validation.values()) else "invalid",
                "details": unified_validation
            }
        except Exception as e:
            results["unified_config"] = {"status": "error", "error": str(e)}

        # Validate enhanced config
        if self._enhanced_config:
            try:
                enhanced_errors = await self._enhanced_config.validate_configuration()
                results["enhanced_config"] = {
                    "status": "valid" if not enhanced_errors else "invalid",
                    "errors": enhanced_errors
                }
            except Exception as e:
                results["enhanced_config"] = {"status": "error", "error": str(e)}
        else:
            results["enhanced_config"] = {"status": "disabled", "message": "Enhanced config not available"}

        # Validate integration
        try:
            integration_checks = {
                "hot_reload_available": self._enhanced_config is not None,
                "secrets_management_available": self._enhanced_config is not None and self._enhanced_config.enable_secrets,
                "performance_settings_available": self.get_performance_settings() is not None,
                "feature_flags_available": self.get_feature_flags() is not None
            }
            results["integration"] = {
                "status": "valid",
                "features": integration_checks
            }
        except Exception as e:
            results["integration"] = {"status": "error", "error": str(e)}

        # Determine overall status
        all_valid = (
            results["unified_config"].get("status") == "valid" and
            results["enhanced_config"].get("status") in ["valid", "disabled"] and
            results["integration"].get("status") == "valid"
        )
        results["overall_status"] = "valid" if all_valid else "invalid"

        return results

    def export_configuration_summary(self) -> Dict[str, Any]:
        """Export a comprehensive configuration summary."""
        return {
            "environment": {
                "current": os.getenv("ENVIRONMENT", "development"),
                "summary": self.unified_config.get_environment_summary()
            },
            "services": self.unified_config.get_service_configs(),
            "performance": self.get_performance_settings().dict(),
            "feature_flags": self.get_feature_flags().dict(),
            "resource_limits": self.get_resource_limits().dict(),
            "health_checks": self.unified_config.get_health_check_config(),
            "integration_status": {
                "unified_config_available": self._unified_config is not None,
                "enhanced_config_available": self._enhanced_config is not None,
                "hot_reload_enabled": self._enhanced_config is not None and self._enhanced_config.enable_hot_reload,
                "secrets_enabled": self._enhanced_config is not None and self._enhanced_config.enable_secrets
            }
        }

    def create_environment_template(self, environment: str, output_dir: Path) -> None:
        """Create configuration template files for an environment."""
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create enhanced config template
        if self._enhanced_config:
            try:
                # Create basic config template
                config_template = self._create_enhanced_config_template(environment)
                config_file = output_dir / f"{environment}.yaml"
                config_file.write_text(config_template, encoding='utf-8')

                logger.info(f"Created enhanced config template: {config_file}")
            except Exception as e:
                logger.error(f"Failed to create enhanced config template: {e}")

        # Create unified config template using existing functionality
        self.unified_config.create_deployment_configs(output_dir)

    def _create_enhanced_config_template(self, environment: str) -> str:
        """Create an enhanced configuration template for the environment."""
        template = f"""# Enhanced Configuration Template for {environment.upper()}
# Generated by Integrated Configuration Manager

# Database Configuration
database:
  url: "{'sqlite:///./data/warehouse/retail.db' if environment != 'production' else 'postgresql://user:pass@localhost/db'}"
  pool_size: {20 if environment == 'production' else 5}
  max_overflow: {30 if environment == 'production' else 10}
  pool_timeout: {30 if environment == 'production' else 10}

# API Configuration
api:
  host: "{'0.0.0.0' if environment == 'production' else '127.0.0.1'}"
  port: 8000
  workers: {8 if environment == 'production' else 1}
  reload: {str(environment == 'development').lower()}

# Logging Configuration
logging:
  level: "{'INFO' if environment == 'production' else 'DEBUG'}"
  format: "json"
  file_path: "./logs/{environment}.log"

# Performance Settings
performance:
  db_pool_size: {20 if environment == 'production' else 5}
  db_max_overflow: {30 if environment == 'production' else 10}
  db_pool_timeout: {30 if environment == 'production' else 10}
  db_pool_recycle: {3600 if environment == 'production' else 1800}
  cache_default_ttl: {600 if environment == 'production' else 300}
  cache_max_size: {5000 if environment == 'production' else 1000}
  api_timeout: {60 if environment == 'production' else 30}
  api_max_retries: {5 if environment == 'production' else 3}
  max_workers: {20 if environment == 'production' else 5}
  memory_limit_mb: {8192 if environment == 'production' else 2048}

# Resource Limits
resource_limits:
  max_memory_mb: {8192 if environment == 'production' else 2048}
  max_cpu_percent: {80.0 if environment == 'production' else 50.0}
  max_disk_usage_percent: {85.0 if environment == 'production' else 70.0}
  max_connections: {1000 if environment == 'production' else 100}
  max_requests_per_second: {100.0 if environment == 'production' else 50.0}

# Feature Flags
features:
  enable_query_caching: true
  enable_response_compression: {str(environment == 'production').lower()}
  enable_connection_pooling: true
  enable_async_processing: true
  enable_metrics_collection: {str(environment != 'development').lower()}
  enable_distributed_tracing: {str(environment != 'development').lower()}
  enable_performance_monitoring: true
  enable_health_checks: true
  enable_rate_limiting: {str(environment == 'production').lower()}
  enable_input_validation: true
  enable_incremental_processing: true
  enable_data_quality_checks: true
  enable_automatic_retry: true
  enable_parallel_processing: true

# Secrets (use environment variables or secret management)
secrets:
  # Examples of secret references:
  # secret_key: "${{SECRET:app_secret_key}}"
  # database_password: "${{SECRET:db_password}}"
  # api_keys: "${{SECRET:external_api_keys}}"
"""
        return template


# Global integrated configuration manager
_integrated_config_manager: Optional[IntegratedConfigManager] = None


@lru_cache
def get_integrated_config() -> IntegratedConfigManager:
    """Get the global integrated configuration manager."""
    global _integrated_config_manager
    if not _integrated_config_manager:
        _integrated_config_manager = IntegratedConfigManager()
    return _integrated_config_manager


# Convenience functions for easy access
def get_config_value(key: str, default: Any = None) -> Any:
    """Get configuration value from integrated system."""
    return get_integrated_config().get_config_value(key, default)


def is_feature_enabled(feature_name: str) -> bool:
    """Check if a feature is enabled."""
    return get_integrated_config().is_feature_enabled(feature_name)


def get_database_config() -> Dict[str, Any]:
    """Get database configuration."""
    return get_integrated_config().get_database_config()


def get_api_config() -> Dict[str, Any]:
    """Get API configuration."""
    return get_integrated_config().get_api_config()


# Initialize integrated config on module import
_integrated_config = get_integrated_config()