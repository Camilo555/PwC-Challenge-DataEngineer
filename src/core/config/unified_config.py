"""
Unified Configuration Management System
Provides centralized configuration for all system components
"""
from __future__ import annotations

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Type, TypeVar, Union

from pydantic import BaseSettings, validator

from .base_config import BaseConfig, Environment, ProcessingEngine, OrchestrationEngine
from .spark_config import SparkConfig
from .airflow_config import AirflowConfig
from .dagster_config import DagsterConfig
from .monitoring_config import MonitoringConfig
from .security_config import SecurityConfig

T = TypeVar('T', bound=BaseSettings)


class UnifiedConfig:
    """Unified configuration manager that orchestrates all configuration components."""
    
    def __init__(self, config_file: Optional[Path] = None, environment: Optional[str] = None):
        """
        Initialize unified configuration.
        
        Args:
            config_file: Optional path to configuration file
            environment: Override environment (dev, test, staging, prod)
        """
        self._config_file = config_file or Path(".env")
        self._environment = Environment(environment) if environment else None
        
        # Initialize configuration components
        self._base_config: Optional[BaseConfig] = None
        self._spark_config: Optional[SparkConfig] = None
        self._airflow_config: Optional[AirflowConfig] = None
        self._dagster_config: Optional[DagsterConfig] = None
        self._monitoring_config: Optional[MonitoringConfig] = None
        self._security_config: Optional[SecurityConfig] = None
        
        # Load configurations
        self._load_configurations()
    
    def _load_configurations(self) -> None:
        """Load all configuration components."""
        # Override environment if specified
        if self._environment:
            os.environ["ENVIRONMENT"] = self._environment.value
        
        # Load base configuration first
        self._base_config = BaseConfig()
        
        # Load specialized configurations based on enabled features
        self._spark_config = SparkConfig()
        self._airflow_config = AirflowConfig()
        self._dagster_config = DagsterConfig()
        self._monitoring_config = MonitoringConfig()
        self._security_config = SecurityConfig()
    
    @property
    def base(self) -> BaseConfig:
        """Get base configuration."""
        if not self._base_config:
            self._base_config = BaseConfig()
        return self._base_config
    
    @property
    def spark(self) -> SparkConfig:
        """Get Spark configuration."""
        if not self._spark_config:
            self._spark_config = SparkConfig()
        return self._spark_config
    
    @property
    def airflow(self) -> AirflowConfig:
        """Get Airflow configuration."""
        if not self._airflow_config:
            self._airflow_config = AirflowConfig()
        return self._airflow_config
    
    @property
    def dagster(self) -> DagsterConfig:
        """Get Dagster configuration."""
        if not self._dagster_config:
            self._dagster_config = DagsterConfig()
        return self._dagster_config
    
    @property
    def monitoring(self) -> MonitoringConfig:
        """Get monitoring configuration."""
        if not self._monitoring_config:
            self._monitoring_config = MonitoringConfig()
        return self._monitoring_config
    
    @property
    def security(self) -> SecurityConfig:
        """Get security configuration."""
        if not self._security_config:
            self._security_config = SecurityConfig()
        return self._security_config
    
    def get_config(self, config_type: Type[T]) -> T:
        """Get configuration by type."""
        if config_type == BaseConfig:
            return self.base
        elif config_type == SparkConfig:
            return self.spark
        elif config_type == AirflowConfig:
            return self.airflow
        elif config_type == DagsterConfig:
            return self.dagster
        elif config_type == MonitoringConfig:
            return self.monitoring
        elif config_type == SecurityConfig:
            return self.security
        else:
            raise ValueError(f"Unknown configuration type: {config_type}")
    
    def validate_all(self) -> Dict[str, Any]:
        """Validate all configurations and return validation results."""
        results = {}
        
        try:
            # Validate base configuration
            self.base.validate_paths()
            results['base'] = {'status': 'valid', 'errors': []}
        except Exception as e:
            results['base'] = {'status': 'invalid', 'errors': [str(e)]}
        
        # Validate other configurations
        config_components = {
            'spark': self.spark,
            'airflow': self.airflow,
            'dagster': self.dagster,
            'monitoring': self.monitoring,
            'security': self.security
        }
        
        for name, config in config_components.items():
            try:
                # Basic validation by accessing all fields
                config.dict()
                results[name] = {'status': 'valid', 'errors': []}
            except Exception as e:
                results[name] = {'status': 'invalid', 'errors': [str(e)]}
        
        return results
    
    def get_environment_summary(self) -> Dict[str, Any]:
        """Get comprehensive environment summary."""
        return {
            'environment': self.base.environment.value,
            'processing_engine': self.base.processing_engine.value,
            'orchestration_engine': self.base.orchestration_engine.value,
            'database_type': getattr(self.base, 'database_type', 'unknown'),
            'debug_mode': self.base.debug,
            'production_mode': self.base.is_production(),
            'features': {
                'external_apis': self.base.enable_external_apis,
                'data_quality_checks': self.base.enable_data_quality_checks,
                'monitoring': self.base.enable_monitoring,
                'caching': self.base.enable_caching,
            },
            'paths': {
                'project_root': str(self.base.project_root),
                'data_path': str(self.base.data_path),
                'raw_data_path': str(self.base.raw_data_path),
                'bronze_path': str(self.base.bronze_path),
                'silver_path': str(self.base.silver_path),
                'gold_path': str(self.base.gold_path),
            }
        }
    
    def get_service_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get configurations for all services."""
        configs = {}
        
        # API configuration
        configs['api'] = {
            'host': getattr(self.base, 'api_host', '0.0.0.0'),
            'port': getattr(self.base, 'api_port', 8000),
            'workers': getattr(self.base, 'api_workers', 4),
            'debug': self.base.debug,
        }
        
        # Database configuration
        if hasattr(self.base, 'database_url'):
            configs['database'] = {
                'url': self.base.database_url,
                'type': getattr(self.base, 'database_type', 'unknown'),
            }
        
        # Spark configuration (if enabled)
        if self.base.processing_engine == ProcessingEngine.SPARK:
            configs['spark'] = {
                'master': self.spark.master,
                'driver_memory': self.spark.driver_memory,
                'executor_memory': self.spark.executor_memory,
                'executor_instances': self.spark.executor_instances,
            }
        
        # Orchestration configuration
        if self.base.orchestration_engine == OrchestrationEngine.AIRFLOW:
            configs['airflow'] = {
                'host': self.airflow.web_server_host,
                'port': self.airflow.web_server_port,
                'executor': self.airflow.executor,
                'database_url': self.airflow.sql_alchemy_conn,
            }
        elif self.base.orchestration_engine == OrchestrationEngine.DAGSTER:
            configs['dagster'] = {
                'host': self.dagster.dagster_host,
                'port': self.dagster.dagster_port,
                'run_launcher': self.dagster.run_launcher,
            }
        
        # Monitoring configuration (if enabled)
        if self.base.enable_monitoring:
            configs['monitoring'] = {
                'prometheus_enabled': getattr(self.monitoring, 'prometheus_enabled', False),
                'grafana_enabled': getattr(self.monitoring, 'grafana_enabled', False),
                'log_level': getattr(self.monitoring, 'log_level', 'INFO'),
            }
        
        return configs
    
    def export_to_yaml(self, output_path: Path) -> None:
        """Export all configurations to a YAML file."""
        config_data = {
            'environment': self.get_environment_summary(),
            'services': self.get_service_configs(),
            'validation': self.validate_all(),
        }
        
        with open(output_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)
    
    def export_env_file(self, output_path: Path) -> None:
        """Export configuration as environment variables file."""
        env_vars = {}
        
        # Base configuration
        base_vars = self.base.dict()
        for key, value in base_vars.items():
            if value is not None:
                env_vars[key.upper()] = str(value)
        
        # Add specialized configurations
        if self.base.processing_engine == ProcessingEngine.SPARK:
            spark_vars = self.spark.dict()
            for key, value in spark_vars.items():
                if value is not None:
                    env_vars[f"SPARK_{key.upper()}"] = str(value)
        
        if self.base.orchestration_engine == OrchestrationEngine.AIRFLOW:
            airflow_vars = self.airflow.dict()
            for key, value in airflow_vars.items():
                if value is not None:
                    env_vars[f"AIRFLOW_{key.upper()}"] = str(value)
        
        # Write to file
        with open(output_path, 'w') as f:
            for key, value in sorted(env_vars.items()):
                f.write(f"{key}={value}\n")
    
    def generate_docker_env(self) -> Dict[str, str]:
        """Generate environment variables for Docker containers."""
        env_vars = {
            'ENVIRONMENT': self.base.environment.value,
            'PROCESSING_ENGINE': self.base.processing_engine.value,
            'ORCHESTRATION_ENGINE': self.base.orchestration_engine.value,
            'PYTHONPATH': '/app/src',
        }
        
        # Add service-specific variables
        if self.base.processing_engine == ProcessingEngine.SPARK:
            env_vars.update({
                'SPARK_MASTER_URL': self.spark.master,
                'SPARK_DRIVER_MEMORY': self.spark.driver_memory,
                'SPARK_EXECUTOR_MEMORY': self.spark.executor_memory,
            })
        
        # Add database configuration
        if hasattr(self.base, 'database_url'):
            env_vars['DATABASE_URL'] = self.base.database_url
        
        # Add security configuration
        if hasattr(self.security, 'secret_key'):
            env_vars['SECRET_KEY'] = self.security.secret_key
        
        return env_vars
    
    def create_deployment_configs(self, output_dir: Path) -> None:
        """Create all deployment configuration files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export environment file
        self.export_env_file(output_dir / ".env")
        
        # Export YAML configuration
        self.export_to_yaml(output_dir / "config.yml")
        
        # Generate Spark configuration (if enabled)
        if self.base.processing_engine == ProcessingEngine.SPARK:
            spark_yaml = self.spark.generate_spark_yaml(self.base)
            with open(output_dir / "spark.yml", 'w') as f:
                f.write(spark_yaml)
        
        # Generate Airflow configuration (if enabled)
        if self.base.orchestration_engine == OrchestrationEngine.AIRFLOW:
            airflow_cfg = self.airflow.generate_airflow_cfg(self.base)
            with open(output_dir / "airflow.cfg", 'w') as f:
                f.write(airflow_cfg)
        
        # Generate Dagster configuration (if enabled)
        if self.base.orchestration_engine == OrchestrationEngine.DAGSTER:
            dagster_yaml = self.dagster.generate_dagster_yaml(self.base)
            with open(output_dir / "dagster.yaml", 'w') as f:
                f.write(dagster_yaml)
    
    def setup_for_environment(self, environment: Environment) -> None:
        """Setup configuration for specific environment."""
        # Update environment
        os.environ["ENVIRONMENT"] = environment.value
        
        # Reload configurations
        self._load_configurations()
        
        # Apply environment-specific overrides
        env_overrides = self.base.get_environment_config()
        
        for key, value in env_overrides.items():
            setattr(self.base, key, value)
        
        # Validate paths for the environment
        self.base.validate_paths()
    
    def get_health_check_config(self) -> Dict[str, Any]:
        """Get health check configuration for all services."""
        return {
            'api': {
                'endpoint': f"http://localhost:{getattr(self.base, 'api_port', 8000)}/api/v1/health",
                'timeout': 10,
                'interval': 30,
            },
            'database': {
                'check_query': 'SELECT 1',
                'timeout': 5,
            },
            'spark': {
                'endpoint': f"http://localhost:8080",
                'timeout': 10,
            } if self.base.processing_engine == ProcessingEngine.SPARK else None,
            'airflow': {
                'endpoint': f"http://localhost:{self.airflow.web_server_port}/health",
                'timeout': 10,
            } if self.base.orchestration_engine == OrchestrationEngine.AIRFLOW else None,
            'dagster': {
                'endpoint': f"http://localhost:{self.dagster.dagster_port}/server_info",
                'timeout': 10,
            } if self.base.orchestration_engine == OrchestrationEngine.DAGSTER else None,
        }


# Global configuration instance
_unified_config: Optional[UnifiedConfig] = None


def get_unified_config() -> UnifiedConfig:
    """Get global unified configuration instance."""
    global _unified_config
    if _unified_config is None:
        _unified_config = UnifiedConfig()
    return _unified_config


def reload_config(config_file: Optional[Path] = None, environment: Optional[str] = None) -> UnifiedConfig:
    """Reload global configuration."""
    global _unified_config
    _unified_config = UnifiedConfig(config_file=config_file, environment=environment)
    return _unified_config


# Convenience functions for backward compatibility
def get_base_config() -> BaseConfig:
    """Get base configuration."""
    return get_unified_config().base


def get_spark_config() -> SparkConfig:
    """Get Spark configuration."""
    return get_unified_config().spark


def get_airflow_config() -> AirflowConfig:
    """Get Airflow configuration."""
    return get_unified_config().airflow


def get_dagster_config() -> DagsterConfig:
    """Get Dagster configuration."""
    return get_unified_config().dagster