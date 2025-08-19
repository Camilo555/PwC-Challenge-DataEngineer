"""
Base Configuration Classes
Provides foundational configuration management
"""
from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict

from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Environment(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class DatabaseType(str, Enum):
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    SUPABASE = "supabase"


class ProcessingEngine(str, Enum):
    PANDAS = "pandas"
    SPARK = "spark"
    DASK = "dask"


class OrchestrationEngine(str, Enum):
    DAGSTER = "dagster"
    AIRFLOW = "airflow"
    PREFECT = "prefect"


class BaseConfig(BaseSettings):
    """Enhanced base configuration with comprehensive settings."""
    
    # Environment
    environment: Environment = Field(default=Environment.DEVELOPMENT, env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")
    
    # Project paths
    project_root: Path = Field(default_factory=lambda: Path.cwd())
    src_path: Path = Field(default_factory=lambda: Path.cwd() / "src")
    
    # Data paths (medallion architecture)
    data_path: Path = Field(default_factory=lambda: Path.cwd() / "data")
    raw_data_path: Path = Field(default_factory=lambda: Path.cwd() / "data" / "raw")
    bronze_path: Path = Field(default_factory=lambda: Path.cwd() / "data" / "bronze")
    silver_path: Path = Field(default_factory=lambda: Path.cwd() / "data" / "silver")
    gold_path: Path = Field(default_factory=lambda: Path.cwd() / "data" / "gold")
    
    # Processing configuration
    processing_engine: ProcessingEngine = Field(default=ProcessingEngine.PANDAS, env="PROCESSING_ENGINE")
    orchestration_engine: OrchestrationEngine = Field(default=OrchestrationEngine.DAGSTER, env="ORCHESTRATION_ENGINE")
    
    # Performance settings
    max_workers: int = Field(default=4, env="MAX_WORKERS")
    batch_size: int = Field(default=1000, env="BATCH_SIZE")
    memory_limit_gb: float = Field(default=4.0, env="MEMORY_LIMIT_GB")
    
    # Feature flags
    enable_external_apis: bool = Field(default=True, env="ENABLE_EXTERNAL_APIS")
    enable_data_quality_checks: bool = Field(default=True, env="ENABLE_DATA_QUALITY_CHECKS")
    enable_monitoring: bool = Field(default=True, env="ENABLE_MONITORING")
    enable_caching: bool = Field(default=True, env="ENABLE_CACHING")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"
        
    @validator("project_root", "src_path", "data_path", pre=True)
    def resolve_paths(cls, v: Any) -> Path:
        """Resolve paths to absolute paths."""
        if isinstance(v, str):
            path = Path(v)
        elif isinstance(v, Path):
            path = v
        else:
            return v
            
        return path.resolve()
    
    def validate_paths(self) -> None:
        """Validate that required paths exist and are accessible."""
        paths_to_check = [
            self.data_path,
            self.raw_data_path,
            self.bronze_path,
            self.silver_path,
            self.gold_path
        ]
        
        for path in paths_to_check:
            path.mkdir(parents=True, exist_ok=True)
    
    def get_environment_config(self) -> Dict[str, Any]:
        """Get environment-specific configuration overrides."""
        config_overrides = {
            Environment.DEVELOPMENT: {
                "debug": True,
                "batch_size": 500,
                "enable_monitoring": False,
            },
            Environment.TESTING: {
                "debug": True,
                "batch_size": 100,
                "enable_external_apis": False,
            },
            Environment.STAGING: {
                "debug": False,
                "batch_size": 2000,
                "enable_monitoring": True,
            },
            Environment.PRODUCTION: {
                "debug": False,
                "batch_size": 5000,
                "enable_monitoring": True,
                "enable_caching": True,
            }
        }
        
        return config_overrides.get(self.environment, {})
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PRODUCTION
        
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEVELOPMENT
    
    @property
    def spark_config(self) -> dict:
        """Get Spark configuration for backward compatibility."""
        return {
            "spark.app.name": getattr(self, "spark_app_name", "RetailETL"),
            "spark.master": getattr(self, "spark_master", "local[*]"),
            "spark.executor.memory": getattr(self, "spark_executor_memory", "2g"),
            "spark.driver.memory": getattr(self, "spark_driver_memory", "2g"),
            "spark.sql.shuffle.partitions": str(getattr(self, "spark_sql_shuffle_partitions", 200)),
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.jars.packages": ",".join([
                "org.xerial:sqlite-jdbc:3.45.3.0",
                "org.postgresql:postgresql:42.7.3"
            ]),
        }
    
    def get_database_url(self, async_mode: bool = False) -> str:
        """Get database URL for backward compatibility."""
        database_url = getattr(self, "database_url", "sqlite:///./data/warehouse/retail.db")
        if async_mode:
            if database_url.startswith("sqlite://"):
                return database_url.replace("sqlite://", "sqlite+aiosqlite://")
            elif database_url.startswith("postgresql://"):
                return database_url.replace("postgresql://", "postgresql+asyncpg://")
        return database_url