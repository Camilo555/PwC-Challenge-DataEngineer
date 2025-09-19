"""
Centralized Configuration Management System
==========================================

Provides unified, integrated configuration management with:
- Hot reloading and environment-specific settings
- Performance optimization settings
- Feature flags and resource limits
- Secrets management integration
- Comprehensive validation and monitoring

This module integrates multiple configuration systems:
- Unified configuration for service orchestration
- Enhanced configuration manager for hot reloading
- Performance and feature flag management
- Environment-specific templates and validation
"""
from .airflow_config import AirflowConfig
from .base_config import BaseConfig, DatabaseType, Environment
from .dagster_config import DagsterConfig
from .monitoring_config import MonitoringConfig
from .security_config import SecurityConfig
from .spark_config import SparkConfig

# Import unified configuration system
from .unified_config import get_unified_config, UnifiedConfig

# Import enhanced configuration capabilities
from .enhanced_config_manager import (
    PerformanceSettings,
    ResourceLimits,
    FeatureFlags,
    EnhancedConfigManager
)

# Import integrated configuration system
from .config_integration import (
    IntegratedConfigManager,
    get_integrated_config,
    get_config_value,
    is_feature_enabled,
    get_database_config,
    get_api_config
)

# Main configuration instances
config_manager = get_integrated_config()
unified_config = get_unified_config()

# Backwards compatibility - expose the base config as settings
settings = unified_config.base

# Enhanced settings with integrated capabilities
integrated_settings = config_manager

__all__ = [
    # Core configuration classes
    "BaseConfig",
    "Environment",
    "DatabaseType",
    "SparkConfig",
    "AirflowConfig",
    "DagsterConfig",
    "MonitoringConfig",
    "SecurityConfig",

    # Unified configuration
    "UnifiedConfig",
    "get_unified_config",

    # Enhanced configuration
    "PerformanceSettings",
    "ResourceLimits",
    "FeatureFlags",
    "EnhancedConfigManager",

    # Integrated configuration
    "IntegratedConfigManager",
    "get_integrated_config",
    "get_config_value",
    "is_feature_enabled",
    "get_database_config",
    "get_api_config",

    # Main instances
    "config_manager",
    "unified_config",
    "settings",
    "integrated_settings"
]
