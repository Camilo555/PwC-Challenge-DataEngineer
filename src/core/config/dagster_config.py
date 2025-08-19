"""
Advanced Dagster Configuration
Provides comprehensive Dagster settings for modern data orchestration
"""
from __future__ import annotations

from typing import Dict, List, Optional, Any
from pathlib import Path

from pydantic import BaseSettings, Field, validator

from .base_config import BaseConfig, Environment


class DagsterConfig(BaseSettings):
    """Enhanced Dagster configuration with production-ready settings."""
    
    # Core Dagster settings
    dagster_home: Optional[str] = Field(default=None, env="DAGSTER_HOME")
    dagster_host: str = Field(default="127.0.0.1", env="DAGSTER_HOST")
    dagster_port: int = Field(default=3000, env="DAGSTER_PORT")
    
    # Database configuration
    dagster_postgres_db: Optional[str] = Field(default=None, env="DAGSTER_POSTGRES_DB")
    dagster_postgres_user: Optional[str] = Field(default=None, env="DAGSTER_POSTGRES_USER")
    dagster_postgres_password: Optional[str] = Field(default=None, env="DAGSTER_POSTGRES_PASSWORD")
    dagster_postgres_host: str = Field(default="localhost", env="DAGSTER_POSTGRES_HOST")
    dagster_postgres_port: int = Field(default=5432, env="DAGSTER_POSTGRES_PORT")
    
    # Run launcher configuration
    run_launcher: str = Field(default="DefaultRunLauncher", env="DAGSTER_RUN_LAUNCHER")
    max_concurrent_runs: int = Field(default=10, env="DAGSTER_MAX_CONCURRENT_RUNS")
    
    # Compute log manager
    compute_logs_directory: str = Field(default="logs/compute", env="DAGSTER_COMPUTE_LOGS_DIRECTORY")
    
    # Event log storage
    event_log_storage: str = Field(default="sqlite", env="DAGSTER_EVENT_LOG_STORAGE")
    
    # Run storage
    run_storage: str = Field(default="sqlite", env="DAGSTER_RUN_STORAGE")
    
    # Schedule storage
    schedule_storage: str = Field(default="sqlite", env="DAGSTER_SCHEDULE_STORAGE")
    
    # Sensor settings
    sensor_evaluation_interval_seconds: int = Field(default=30, env="DAGSTER_SENSOR_EVALUATION_INTERVAL")
    sensor_minimum_interval_seconds: int = Field(default=30, env="DAGSTER_SENSOR_MINIMUM_INTERVAL")
    
    # Auto-materialize settings
    enable_auto_materialize: bool = Field(default=True, env="DAGSTER_ENABLE_AUTO_MATERIALIZE")
    auto_materialize_evaluation_interval_seconds: int = Field(default=60, env="DAGSTER_AUTO_MATERIALIZE_INTERVAL")
    
    # Resource limits
    op_concurrency_limit: Optional[int] = Field(default=None, env="DAGSTER_OP_CONCURRENCY_LIMIT")
    asset_concurrency_limit: Optional[int] = Field(default=None, env="DAGSTER_ASSET_CONCURRENCY_LIMIT")
    
    # Monitoring and observability
    enable_asset_monitoring: bool = Field(default=True, env="DAGSTER_ENABLE_ASSET_MONITORING")
    enable_op_monitoring: bool = Field(default=True, env="DAGSTER_ENABLE_OP_MONITORING")
    
    # External service integrations
    slack_webhook_url: Optional[str] = Field(default=None, env="DAGSTER_SLACK_WEBHOOK_URL")
    datadog_api_key: Optional[str] = Field(default=None, env="DAGSTER_DATADOG_API_KEY")
    
    # Data quality thresholds
    min_data_quality_score: float = Field(default=0.8, env="DAGSTER_MIN_DATA_QUALITY_SCORE")
    max_null_percentage: float = Field(default=0.1, env="DAGSTER_MAX_NULL_PERCENTAGE")
    max_duplicate_percentage: float = Field(default=0.05, env="DAGSTER_MAX_DUPLICATE_PERCENTAGE")
    
    # External API settings
    enable_external_api_enrichment: bool = Field(default=True, env="DAGSTER_ENABLE_EXTERNAL_API_ENRICHMENT")
    api_request_timeout_seconds: int = Field(default=30, env="DAGSTER_API_REQUEST_TIMEOUT")
    api_retry_attempts: int = Field(default=3, env="DAGSTER_API_RETRY_ATTEMPTS")
    
    # Spark integration
    spark_config_path: Optional[str] = Field(default=None, env="DAGSTER_SPARK_CONFIG_PATH")
    default_spark_conf: Dict[str, str] = Field(
        default_factory=lambda: {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
        env="DAGSTER_DEFAULT_SPARK_CONF"
    )
    
    # I/O managers
    default_io_manager: str = Field(default="fs_io_manager", env="DAGSTER_DEFAULT_IO_MANAGER")
    enable_s3_io_manager: bool = Field(default=False, env="DAGSTER_ENABLE_S3_IO_MANAGER")
    s3_bucket: Optional[str] = Field(default=None, env="DAGSTER_S3_BUCKET")
    
    # Partition settings
    enable_date_partitioning: bool = Field(default=True, env="DAGSTER_ENABLE_DATE_PARTITIONING")
    partition_start_date: str = Field(default="2024-01-01", env="DAGSTER_PARTITION_START_DATE")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    @validator("dagster_home", pre=True)
    def set_dagster_home(cls, v: Optional[str]) -> str:
        """Set DAGSTER_HOME if not provided."""
        if v is None:
            v = str(Path.cwd() / "dagster_home")
        Path(v).mkdir(parents=True, exist_ok=True)
        return v
    
    def get_storage_config(self, base_config: BaseConfig) -> Dict[str, Any]:
        """Get storage configuration based on environment."""
        if self.dagster_postgres_db and base_config.is_production():
            # Production PostgreSQL configuration
            return {
                "postgres_db": {
                    "postgres_db": {
                        "hostname": self.dagster_postgres_host,
                        "username": self.dagster_postgres_user,
                        "password": self.dagster_postgres_password,
                        "db_name": self.dagster_postgres_db,
                        "port": self.dagster_postgres_port,
                    }
                }
            }
        else:
            # Development SQLite configuration
            storage_dir = Path(self.dagster_home) / "storage"
            storage_dir.mkdir(parents=True, exist_ok=True)
            
            return {
                "sqlite_db": {
                    "sqlite_db": {
                        "base_dir": str(storage_dir)
                    }
                }
            }
    
    def get_run_launcher_config(self, base_config: BaseConfig) -> Dict[str, Any]:
        """Get run launcher configuration."""
        if base_config.is_production():
            return {
                "DockerRunLauncher": {
                    "image": "retail-etl-pipeline:latest",
                    "registry": {
                        "url": "your-registry.com",
                        "username": {"env": "DOCKER_REGISTRY_USERNAME"},
                        "password": {"env": "DOCKER_REGISTRY_PASSWORD"},
                    },
                    "network": "dagster_network",
                }
            }
        else:
            return {
                "DefaultRunLauncher": {}
            }
    
    def get_compute_log_manager_config(self) -> Dict[str, Any]:
        """Get compute log manager configuration."""
        log_dir = Path(self.dagster_home) / self.compute_logs_directory
        log_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            "LocalComputeLogManager": {
                "base_dir": str(log_dir)
            }
        }
    
    def get_io_manager_configs(self) -> Dict[str, Any]:
        """Get I/O manager configurations."""
        configs = {
            "fs_io_manager": {
                "config": {
                    "base_dir": str(Path(self.dagster_home) / "storage" / "io_manager")
                }
            }
        }
        
        if self.enable_s3_io_manager and self.s3_bucket:
            configs["s3_io_manager"] = {
                "config": {
                    "s3_bucket": self.s3_bucket,
                    "s3_prefix": "dagster-io"
                }
            }
        
        return configs
    
    def get_resource_configs(self, base_config: BaseConfig) -> Dict[str, Any]:
        """Get resource configurations."""
        configs = {}
        
        # Spark resource
        if base_config.processing_engine.value == "spark":
            spark_config = {
                "spark_session": {
                    "config": {
                        "spark_conf": self.default_spark_conf
                    }
                }
            }
            if self.spark_config_path:
                spark_config["spark_session"]["config"]["config_path"] = self.spark_config_path
            
            configs["spark"] = spark_config
        
        # External API resource
        if self.enable_external_api_enrichment:
            configs["external_api"] = {
                "config": {
                    "timeout_seconds": self.api_request_timeout_seconds,
                    "retry_attempts": self.api_retry_attempts
                }
            }
        
        # Data quality resource
        configs["data_quality"] = {
            "config": {
                "min_quality_score": self.min_data_quality_score,
                "max_null_percentage": self.max_null_percentage,
                "max_duplicate_percentage": self.max_duplicate_percentage
            }
        }
        
        # Notification resources
        if self.slack_webhook_url:
            configs["slack"] = {
                "config": {
                    "webhook_url": self.slack_webhook_url
                }
            }
        
        return configs
    
    def get_sensor_configs(self) -> Dict[str, Any]:
        """Get sensor configurations."""
        return {
            "evaluation_interval": self.sensor_evaluation_interval_seconds,
            "minimum_interval_seconds": self.sensor_minimum_interval_seconds
        }
    
    def get_schedule_configs(self) -> Dict[str, Any]:
        """Get schedule configurations."""
        return {
            "enable_schedules": True,
            "default_schedule": {
                "cron_schedule": "0 2 * * *",  # Daily at 2 AM
                "execution_timezone": "UTC"
            }
        }
    
    def get_auto_materialize_configs(self) -> Dict[str, Any]:
        """Get auto-materialize configurations."""
        return {
            "enabled": self.enable_auto_materialize,
            "evaluation_interval_seconds": self.auto_materialize_evaluation_interval_seconds,
            "use_sensors": True
        }
    
    def get_monitoring_configs(self) -> Dict[str, Any]:
        """Get monitoring and observability configurations."""
        configs = {
            "asset_monitoring": self.enable_asset_monitoring,
            "op_monitoring": self.enable_op_monitoring
        }
        
        if self.datadog_api_key:
            configs["datadog"] = {
                "api_key": self.datadog_api_key
            }
        
        return configs
    
    def get_partition_configs(self) -> Dict[str, Any]:
        """Get partitioning configurations."""
        return {
            "date_partitioning": {
                "enabled": self.enable_date_partitioning,
                "start_date": self.partition_start_date,
                "format": "%Y-%m-%d"
            }
        }
    
    def generate_dagster_yaml(self, base_config: BaseConfig) -> str:
        """Generate dagster.yaml configuration file."""
        storage_config = self.get_storage_config(base_config)
        run_launcher_config = self.get_run_launcher_config(base_config)
        compute_log_config = self.get_compute_log_manager_config()
        
        # Determine storage backend
        if "postgres_db" in storage_config:
            storage_backend = "postgres_db"
        else:
            storage_backend = "sqlite_db"
        
        # Determine run launcher
        launcher_type = list(run_launcher_config.keys())[0]
        
        yaml_content = f"""
# Dagster Configuration
# Generated automatically - do not edit manually

run_launcher:
  module: dagster._core.launcher
  class: {launcher_type}
  config:
"""
        
        # Add launcher config
        launcher_config = run_launcher_config[launcher_type]
        if launcher_config:
            for key, value in launcher_config.items():
                yaml_content += f"    {key}: {value}\n"
        
        yaml_content += f"""
run_storage:
  module: dagster_postgres.run_storage
  class: DagsterPostgresRunStorage
  config:
    {storage_backend}:
      hostname: {storage_config[storage_backend][storage_backend]['hostname']}
      username: {storage_config[storage_backend][storage_backend]['username']}
      password: {storage_config[storage_backend][storage_backend]['password']}
      db_name: {storage_config[storage_backend][storage_backend]['db_name']}
      port: {storage_config[storage_backend][storage_backend]['port']}

event_log_storage:
  module: dagster_postgres.event_log
  class: DagsterPostgresEventLogStorage
  config:
    {storage_backend}:
      hostname: {storage_config[storage_backend][storage_backend]['hostname']}
      username: {storage_config[storage_backend][storage_backend]['username']}
      password: {storage_config[storage_backend][storage_backend]['password']}
      db_name: {storage_config[storage_backend][storage_backend]['db_name']}
      port: {storage_config[storage_backend][storage_backend]['port']}

schedule_storage:
  module: dagster_postgres.schedule_storage
  class: DagsterPostgresScheduleStorage
  config:
    {storage_backend}:
      hostname: {storage_config[storage_backend][storage_backend]['hostname']}
      username: {storage_config[storage_backend][storage_backend]['username']}
      password: {storage_config[storage_backend][storage_backend]['password']}
      db_name: {storage_config[storage_backend][storage_backend]['db_name']}
      port: {storage_config[storage_backend][storage_backend]['port']}

compute_logs:
  module: dagster._core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: {compute_log_config['LocalComputeLogManager']['base_dir']}

local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: {self.dagster_home}/storage

telemetry:
  enabled: false
""" if storage_backend == "postgres_db" else f"""
# Dagster Configuration - SQLite Development Mode
# Generated automatically - do not edit manually

storage:
  sqlite:
    base_dir: {storage_config[storage_backend][storage_backend]['base_dir']}

compute_logs:
  module: dagster._core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: {compute_log_config['LocalComputeLogManager']['base_dir']}

local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: {self.dagster_home}/storage

telemetry:
  enabled: false
"""
        
        return yaml_content.strip()
    
    def get_environment_variables(self, base_config: BaseConfig) -> Dict[str, str]:
        """Get environment variables for Dagster."""
        env_vars = {
            "DAGSTER_HOME": self.dagster_home,
            "DAGSTER_HOST": self.dagster_host,
            "DAGSTER_PORT": str(self.dagster_port),
        }
        
        if self.dagster_postgres_db:
            env_vars.update({
                "DAGSTER_POSTGRES_DB": self.dagster_postgres_db,
                "DAGSTER_POSTGRES_USER": self.dagster_postgres_user,
                "DAGSTER_POSTGRES_PASSWORD": self.dagster_postgres_password,
                "DAGSTER_POSTGRES_HOST": self.dagster_postgres_host,
                "DAGSTER_POSTGRES_PORT": str(self.dagster_postgres_port),
            })
        
        return env_vars