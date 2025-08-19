"""
Advanced Spark Configuration
Provides comprehensive Spark settings for all deployment scenarios
"""
from __future__ import annotations

import os
import platform
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import Field, validator
from pydantic_settings import BaseSettings

from .base_config import BaseConfig, Environment


class SparkConfig(BaseSettings):
    """Enhanced Spark configuration with production-ready settings."""
    
    # Spark application settings
    app_name: str = Field(default="RetailETL-Pipeline", env="SPARK_APP_NAME")
    master: str = Field(default="local[*]", env="SPARK_MASTER_URL")
    deploy_mode: str = Field(default="client", env="SPARK_DEPLOY_MODE")
    
    # Driver settings
    driver_memory: str = Field(default="2g", env="SPARK_DRIVER_MEMORY") 
    driver_cores: int = Field(default=2, env="SPARK_DRIVER_CORES")
    driver_max_result_size: str = Field(default="1g", env="SPARK_DRIVER_MAX_RESULT_SIZE")
    
    # Executor settings
    executor_memory: str = Field(default="2g", env="SPARK_EXECUTOR_MEMORY")
    executor_cores: int = Field(default=2, env="SPARK_EXECUTOR_CORES")
    executor_instances: int = Field(default=2, env="SPARK_EXECUTOR_INSTANCES")
    
    # Dynamic allocation
    dynamic_allocation_enabled: bool = Field(default=True, env="SPARK_DYNAMIC_ALLOCATION_ENABLED")
    dynamic_allocation_min_executors: int = Field(default=1, env="SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS")
    dynamic_allocation_max_executors: int = Field(default=10, env="SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS")
    dynamic_allocation_initial_executors: int = Field(default=2, env="SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS")
    
    # SQL and adaptive query execution
    adaptive_enabled: bool = Field(default=True, env="SPARK_ADAPTIVE_ENABLED")
    adaptive_coalesce_partitions_enabled: bool = Field(default=True, env="SPARK_ADAPTIVE_COALESCE_PARTITIONS_ENABLED")
    adaptive_skewed_join_enabled: bool = Field(default=True, env="SPARK_ADAPTIVE_SKEWED_JOIN_ENABLED")
    
    # Serialization and compression
    serializer: str = Field(default="org.apache.spark.serializer.KryoSerializer", env="SPARK_SERIALIZER")
    compression_codec: str = Field(default="zstd", env="SPARK_COMPRESSION_CODEC")
    
    # Delta Lake settings
    enable_delta: bool = Field(default=True, env="SPARK_ENABLE_DELTA")
    delta_catalog_enabled: bool = Field(default=True, env="SPARK_DELTA_CATALOG_ENABLED")
    
    # Checkpointing and recovery
    checkpoint_dir: Optional[str] = Field(default=None, env="SPARK_CHECKPOINT_DIR")
    recovery_mode: str = Field(default="FILESYSTEM", env="SPARK_RECOVERY_MODE")
    
    # Monitoring and metrics
    metrics_enabled: bool = Field(default=True, env="SPARK_METRICS_ENABLED")
    event_log_enabled: bool = Field(default=True, env="SPARK_EVENT_LOG_ENABLED")
    event_log_dir: Optional[str] = Field(default=None, env="SPARK_EVENT_LOG_DIR")
    
    # UI and history server
    ui_enabled: bool = Field(default=True, env="SPARK_UI_ENABLED")
    ui_port: int = Field(default=4040, env="SPARK_UI_PORT")
    history_server_enabled: bool = Field(default=False, env="SPARK_HISTORY_SERVER_ENABLED")
    
    # Security settings
    authenticate: bool = Field(default=False, env="SPARK_AUTHENTICATE")
    encrypt_enabled: bool = Field(default=False, env="SPARK_ENCRYPT_ENABLED")
    ssl_enabled: bool = Field(default=False, env="SPARK_SSL_ENABLED")
    
    # Custom JARs and packages
    jars_packages: List[str] = Field(
        default_factory=lambda: [
            "io.delta:delta-core_2.12:2.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ],
        env="SPARK_JARS_PACKAGES"
    )
    
    # Environment-specific overrides
    java_home: Optional[str] = Field(default=None, env="JAVA_HOME")
    hadoop_home: Optional[str] = Field(default=None, env="HADOOP_HOME")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"
    
    @validator("checkpoint_dir", "event_log_dir", pre=True)
    def resolve_paths(cls, v: Optional[str]) -> Optional[str]:
        """Resolve checkpoint and log directories."""
        if v is None:
            return v
        path = Path(v)
        path.mkdir(parents=True, exist_ok=True)
        return str(path.resolve())
    
    def get_spark_config(self, base_config: BaseConfig) -> Dict[str, str]:
        """Generate Spark configuration dictionary."""
        config = {
            # Application settings
            "spark.app.name": self.app_name,
            "spark.master": self.master,
            "spark.submit.deployMode": self.deploy_mode,
            
            # Driver configuration
            "spark.driver.memory": self.driver_memory,
            "spark.driver.cores": str(self.driver_cores),
            "spark.driver.maxResultSize": self.driver_max_result_size,
            
            # Executor configuration
            "spark.executor.memory": self.executor_memory,
            "spark.executor.cores": str(self.executor_cores),
            "spark.executor.instances": str(self.executor_instances),
            
            # SQL and adaptive query execution
            "spark.sql.adaptive.enabled": str(self.adaptive_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(self.adaptive_coalesce_partitions_enabled).lower(),
            "spark.sql.adaptive.skewJoin.enabled": str(self.adaptive_skewed_join_enabled).lower(),
            
            # Serialization
            "spark.serializer": self.serializer,
            "spark.io.compression.codec": self.compression_codec,
            
            # Data location
            "spark.sql.warehouse.dir": str(base_config.gold_path),
        }
        
        # Dynamic allocation
        if self.dynamic_allocation_enabled:
            config.update({
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": str(self.dynamic_allocation_min_executors),
                "spark.dynamicAllocation.maxExecutors": str(self.dynamic_allocation_max_executors),
                "spark.dynamicAllocation.initialExecutors": str(self.dynamic_allocation_initial_executors),
            })
        
        # Delta Lake configuration
        if self.enable_delta:
            config.update({
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            })
        
        # Checkpointing
        if self.checkpoint_dir:
            config["spark.sql.streaming.checkpointLocation"] = self.checkpoint_dir
        
        # Event logging
        if self.event_log_enabled and self.event_log_dir:
            config.update({
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": self.event_log_dir,
            })
        
        # UI configuration
        if not self.ui_enabled:
            config["spark.ui.enabled"] = "false"
        else:
            config["spark.ui.port"] = str(self.ui_port)
        
        # Security settings
        if self.authenticate:
            config["spark.authenticate"] = "true"
        
        if self.encrypt_enabled:
            config.update({
                "spark.network.crypto.enabled": "true",
                "spark.io.encryption.enabled": "true",
            })
        
        # Environment-specific settings
        environment_config = self._get_environment_config(base_config.environment)
        config.update(environment_config)
        
        # Platform-specific settings
        platform_config = self._get_platform_config()
        config.update(platform_config)
        
        return config
    
    def _get_environment_config(self, environment: Environment) -> Dict[str, str]:
        """Get environment-specific Spark configuration."""
        config_overrides = {
            Environment.DEVELOPMENT: {
                "spark.executor.instances": "1",
                "spark.executor.memory": "1g",
                "spark.driver.memory": "1g",
                "spark.dynamicAllocation.maxExecutors": "2",
            },
            Environment.TESTING: {
                "spark.executor.instances": "1",
                "spark.executor.memory": "512m",
                "spark.driver.memory": "512m",
                "spark.ui.enabled": "false",
            },
            Environment.STAGING: {
                "spark.executor.instances": "3",
                "spark.executor.memory": "4g",
                "spark.driver.memory": "2g",
                "spark.dynamicAllocation.maxExecutors": "5",
            },
            Environment.PRODUCTION: {
                "spark.executor.instances": "5",
                "spark.executor.memory": "8g",
                "spark.driver.memory": "4g",
                "spark.dynamicAllocation.maxExecutors": "20",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            }
        }
        
        return config_overrides.get(environment, {})
    
    def _get_platform_config(self) -> Dict[str, str]:
        """Get platform-specific Spark configuration."""
        config = {}
        
        # Windows-specific optimizations
        if platform.system() == "Windows":
            config.update({
                "spark.sql.warehouse.dir": str(Path.cwd() / "spark-warehouse").as_posix(),
                "spark.hadoop.fs.defaultFS": "file:///",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
            })
            
            # Try to detect Java automatically on Windows
            if not self.java_home:
                potential_java_paths = [
                    Path("C:/Program Files/Eclipse Adoptium/jdk-17.0.8.101-hotspot"),
                    Path("C:/Program Files/Java/jdk-17"),
                    Path("C:/Program Files/OpenJDK/jdk-17"),
                ]
                
                for java_path in potential_java_paths:
                    if java_path.exists():
                        config["spark.driver.extraJavaOptions"] = f"-Djava.home={java_path}"
                        break
        
        # Linux/Mac optimizations
        else:
            config.update({
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.serializer.objectStreamReset": "100",
            })
        
        return config
    
    def get_submit_args(self) -> List[str]:
        """Generate spark-submit arguments."""
        args = [
            "--packages", ",".join(self.jars_packages),
        ]
        
        if self.master != "local[*]":
            args.extend([
                "--deploy-mode", self.deploy_mode,
                "--executor-memory", self.executor_memory,
                "--executor-cores", str(self.executor_cores),
                "--num-executors", str(self.executor_instances),
            ])
        
        return args