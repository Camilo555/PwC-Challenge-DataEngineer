"""
Centralized configuration management using Pydantic BaseSettings.
Supports multiple environments and future Supabase integration.
"""

from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any
from functools import lru_cache

from pydantic import Field, field_validator, PostgresDsn, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Environment types for the application."""
    
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class DatabaseType(str, Enum):
    """Supported database types."""
    
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class Settings(BaseSettings):
    """
    Application settings with support for multiple environments.
    Prepared for future Supabase (PostgreSQL) integration.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # Environment
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="Current environment (development, staging, production)",
    )
    
    # Database Configuration
    database_type: DatabaseType = Field(
        default=DatabaseType.SQLITE,
        description="Database type (sqlite or postgresql)",
    )
    database_url: str = Field(
        default="sqlite:///./data/warehouse/retail.db",
        description="Database connection URL",
    )
    supabase_url: Optional[str] = Field(
        default=None,
        description="Supabase project URL for future integration",
    )
    supabase_key: Optional[str] = Field(
        default=None,
        description="Supabase API key for future integration",
    )
    
    # Spark Configuration
    spark_master: str = Field(
        default="local[*]",
        description="Spark master URL",
    )
    spark_app_name: str = Field(
        default="RetailETL",
        description="Spark application name",
    )
    spark_memory: str = Field(
        default="4g",
        description="Spark executor memory",
    )
    spark_executor_memory: str = Field(
        default="2g",
        description="Spark executor memory",
    )
    spark_driver_memory: str = Field(
        default="2g",
        description="Spark driver memory",
    )
    spark_sql_shuffle_partitions: int = Field(
        default=200,
        description="Number of partitions for shuffles",
    )
    
    # Delta Lake Configuration
    delta_log_level: str = Field(
        default="INFO",
        description="Delta Lake log level",
    )
    bronze_path: Path = Field(
        default=Path("./data/bronze"),
        description="Path to Bronze layer data",
    )
    silver_path: Path = Field(
        default=Path("./data/silver"),
        description="Path to Silver layer data",
    )
    gold_path: Path = Field(
        default=Path("./data/gold"),
        description="Path to Gold layer data",
    )
    
    # Typesense Configuration
    typesense_api_key: str = Field(
        default="xyz123changeme",
        description="Typesense API key",
    )
    typesense_host: str = Field(
        default="localhost",
        description="Typesense host",
    )
    typesense_port: int = Field(
        default=8108,
        description="Typesense port",
    )
    typesense_protocol: str = Field(
        default="http",
        description="Typesense protocol (http/https)",
    )
    
    # API Configuration
    api_host: str = Field(
        default="0.0.0.0",
        description="API host",
    )
    api_port: int = Field(
        default=8000,
        description="API port",
    )
    api_workers: int = Field(
        default=4,
        description="Number of API workers",
    )
    api_reload: bool = Field(
        default=True,
        description="Enable auto-reload for development",
    )
    api_log_level: str = Field(
        default="info",
        description="API log level",
    )
    
    # Security
    secret_key: str = Field(
        default="your-secret-key-here-change-in-production",
        description="Secret key for encryption",
    )
    basic_auth_username: str = Field(
        default="admin",
        description="Basic auth username",
    )
    basic_auth_password: str = Field(
        default="changeme123",
        description="Basic auth password",
    )
    
    # Logging
    log_level: str = Field(
        default="INFO",
        description="Application log level",
    )
    log_format: str = Field(
        default="json",
        description="Log format (json/text)",
    )
    log_file_path: Optional[Path] = Field(
        default=Path("./logs/app.log"),
        description="Log file path",
    )
    
    # Data Sources
    raw_data_path: Path = Field(
        default=Path("./data/raw"),
        description="Path to raw data files",
    )
    online_retail_file: str = Field(
        default="online_retail_II.xlsx",
        description="Online Retail dataset filename",
    )
    
    # Feature Flags
    enable_vector_search: bool = Field(
        default=True,
        description="Enable vector search functionality",
    )
    enable_caching: bool = Field(
        default=False,
        description="Enable caching",
    )
    enable_monitoring: bool = Field(
        default=False,
        description="Enable monitoring",
    )
    
    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment value."""
        if v not in [e.value for e in Environment]:
            raise ValueError(f"Invalid environment: {v}")
        return v
    
    @field_validator("database_type")
    @classmethod
    def validate_database_type(cls, v: str) -> str:
        """Validate database type."""
        if v not in [d.value for d in DatabaseType]:
            raise ValueError(f"Invalid database type: {v}")
        return v
    
    @computed_field  # type: ignore[misc]
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PRODUCTION
    
    @computed_field  # type: ignore[misc]
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEVELOPMENT
    
    @computed_field  # type: ignore[misc]
    @property
    def is_postgresql(self) -> bool:
        """Check if using PostgreSQL database."""
        return self.database_type == DatabaseType.POSTGRESQL

    @computed_field  # type: ignore[misc]
    @property
    def spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration as dictionary."""
        return {
            "spark.app.name": self.spark_app_name,
            "spark.master": self.spark_master,
            "spark.executor.memory": self.spark_executor_memory,
            "spark.driver.memory": self.spark_driver_memory,
            "spark.sql.shuffle.partitions": str(self.spark_sql_shuffle_partitions),
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            # JDBC drivers for SQLite and Postgres (download from Maven Central at runtime)
            "spark.jars.packages": ",".join([
                "org.xerial:sqlite-jdbc:3.45.3.0",
                "org.postgresql:postgresql:42.7.3",
            ]),
        }

    @computed_field  # type: ignore[misc]
    @property
    def typesense_config(self) -> Dict[str, Any]:
        """Get Typesense configuration as dictionary."""
        return {
            "api_key": self.typesense_api_key,
            "nodes": [
                {
                    "host": self.typesense_host,
                    "port": str(self.typesense_port),
                    "protocol": self.typesense_protocol,
                }
            ],
            "connection_timeout_seconds": 2,
        }

    def get_database_url(self, async_mode: bool = False) -> str:
        """
        Get database URL with support for async drivers.
        Prepared for future Supabase integration.
        """
        if self.database_type == DatabaseType.SQLITE:
            if async_mode:
                return self.database_url.replace("sqlite://", "sqlite+aiosqlite://")
            return self.database_url
        elif self.database_type == DatabaseType.POSTGRESQL:
            if self.supabase_url:
                # Placeholder for Supabase parsing when needed
                pass
            if async_mode:
                return self.database_url.replace("postgresql://", "postgresql+asyncpg://")
            return self.database_url
        return self.database_url

    def validate_paths(self) -> None:
        """Create necessary directories if they don't exist."""
        paths = [
            self.bronze_path,
            self.silver_path,
            self.gold_path,
            self.raw_data_path,
            Path("./data/warehouse"),
            Path("./logs"),
        ]
        for path in paths:
            path.mkdir(parents=True, exist_ok=True)

    @computed_field  # type: ignore[misc]
    @property
    def jdbc_driver(self) -> str:
        """Return JDBC driver class based on database type."""
        if self.database_type == DatabaseType.SQLITE:
            return "org.sqlite.JDBC"
        return "org.postgresql.Driver"

    @computed_field  # type: ignore[misc]
    @property
    def jdbc_url(self) -> str:
        """Return JDBC URL for Spark based on settings.database_url."""
        if self.database_type == DatabaseType.SQLITE:
            # Expect database_url like sqlite:///./data/warehouse/retail.db
            url = self.database_url
            prefix = "sqlite:///"
            if url.startswith(prefix):
                db_path = url[len(prefix):]
            else:
                db_path = url.replace("sqlite://", "")
            p = Path(db_path).resolve()
            return f"jdbc:sqlite:{p.as_posix()}"
        # Postgres
        return self.database_url.replace("postgresql://", "jdbc:postgresql://")

    @computed_field  # type: ignore[misc]
    @property
    def jdbc_properties(self) -> Dict[str, str]:
        """Return JDBC connection properties for Spark."""
        props: Dict[str, str] = {"driver": self.jdbc_driver}
        if self.database_type == DatabaseType.POSTGRESQL:
            # Very simple parse; prefer env vars in production
            import re
            m = re.match(r"postgresql://([^:@]+):([^@]+)@", self.database_url)
            if m:
                props["user"] = m.group(1)
                props["password"] = m.group(2)
        return props

    @computed_field  # type: ignore[misc]
    @property
    def jdbc_driver(self) -> str:
        """Return JDBC driver class based on database type."""
        if self.database_type == DatabaseType.SQLITE:
            return "org.sqlite.JDBC"
        return "org.postgresql.Driver"

    @computed_field  # type: ignore[misc]
    @property
    def jdbc_url(self) -> str:
        """Return JDBC URL for Spark based on settings.database_url."""
        if self.database_type == DatabaseType.SQLITE:
            # Expect database_url like sqlite:///./data/warehouse/retail.db
            # Extract filesystem path component
            url = self.database_url
            prefix = "sqlite:///"
            if url.startswith(prefix):
                db_path = url[len(prefix):]
            else:
                # Fallback: remove leading sqlite://
                db_path = url.replace("sqlite://", "")
            p = Path(db_path).resolve()
            return f"jdbc:sqlite:{p.as_posix()}"
        # Postgres
        # Expect database_url like postgresql://user:pass@host:port/db
        return self.database_url.replace("postgresql://", "jdbc:postgresql://")


@lru_cache
def get_settings() -> "Settings":
    """Uses LRU cache to ensure single instance across application."""
    return Settings()


# Global settings instance
settings = get_settings()

# Validate and create paths on import
settings.validate_paths()