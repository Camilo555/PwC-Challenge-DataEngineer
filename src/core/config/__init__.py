"""
Enhanced Configuration Management System
Provides centralized configuration with environment-specific settings
"""
from .airflow_config import AirflowConfig
from .base_config import BaseConfig, DatabaseType, Environment
from .dagster_config import DagsterConfig
from .monitoring_config import MonitoringConfig
from .security_config import SecurityConfig
from .spark_config import SparkConfig

# Create settings instance using unified configuration
from .unified_config import get_unified_config

# Backwards compatibility - expose the base config as settings
settings = get_unified_config().base

__all__ = [
    "BaseConfig",
    "Environment",
    "DatabaseType",
    "SparkConfig",
    "AirflowConfig",
    "DagsterConfig",
    "MonitoringConfig",
    "SecurityConfig",
    "settings"
]
