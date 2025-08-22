"""
Advanced Spark Session Management
Provides centralized, optimized Spark session creation and management
"""
from __future__ import annotations

import atexit
import os
import platform
from contextlib import contextmanager
from typing import Dict, Optional, Any
from pathlib import Path

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from core.config.base_config import BaseConfig, Environment
from core.config.spark_config import SparkConfig
from core.logging import get_logger

logger = get_logger(__name__)


class SparkSessionManager:
    """Advanced Spark session manager with production optimizations."""
    
    _instance: Optional['SparkSessionManager'] = None
    _spark_session: Optional[SparkSession] = None
    
    def __new__(cls) -> 'SparkSessionManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._base_config = BaseConfig()
            self._spark_config = SparkConfig()
            self._initialized = True
    
    def get_session(
        self,
        app_name: Optional[str] = None,
        config_overrides: Optional[Dict[str, str]] = None,
        enable_delta: bool = True,
        enable_adaptive_query: bool = True
    ) -> SparkSession:
        """
        Get or create optimized Spark session.
        
        Args:
            app_name: Custom application name
            config_overrides: Additional configuration overrides
            enable_delta: Enable Delta Lake support
            enable_adaptive_query: Enable adaptive query execution
            
        Returns:
            Configured SparkSession instance
        """
        if self._spark_session is None or self._spark_session._sc._jsc is None:
            self._spark_session = self._create_session(
                app_name=app_name,
                config_overrides=config_overrides,
                enable_delta=enable_delta,
                enable_adaptive_query=enable_adaptive_query
            )
        
        return self._spark_session
    
    def _create_session(
        self,
        app_name: Optional[str] = None,
        config_overrides: Optional[Dict[str, str]] = None,
        enable_delta: bool = True,
        enable_adaptive_query: bool = True
    ) -> SparkSession:
        """Create a new Spark session with optimized configuration."""
        
        # Generate base configuration
        spark_config_dict = self._spark_config.get_spark_config(self._base_config)
        
        # Apply overrides
        if config_overrides:
            spark_config_dict.update(config_overrides)
        
        # Custom app name
        if app_name:
            spark_config_dict["spark.app.name"] = app_name
        
        # Platform-specific optimizations
        self._apply_platform_optimizations(spark_config_dict)
        
        # Environment-specific settings
        self._apply_environment_optimizations(spark_config_dict)
        
        logger.info(f"Creating Spark session with {len(spark_config_dict)} configuration parameters")
        logger.debug(f"Spark config: {spark_config_dict}")
        
        # Create Spark configuration
        conf = SparkConf()
        for key, value in spark_config_dict.items():
            conf.set(key, value)
        
        # Create session builder
        builder = SparkSession.builder.config(conf=conf)
        
        # Add Delta Lake support
        if enable_delta and self._spark_config.enable_delta:
            builder = self._configure_delta_lake(builder)
        
        # Create session
        spark = builder.getOrCreate()
        
        # Configure session-level settings
        self._configure_session_settings(spark, enable_adaptive_query)
        
        # Register cleanup
        atexit.register(self._cleanup_session)
        
        logger.info(f"Spark session created successfully: {spark.sparkContext.applicationId}")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark UI: http://localhost:{spark.sparkContext.uiWebUrl.split(':')[-1] if spark.sparkContext.uiWebUrl else 'N/A'}")
        
        return spark
    
    def _apply_platform_optimizations(self, config: Dict[str, str]) -> None:
        """Apply platform-specific optimizations."""
        system = platform.system()
        
        if system == "Windows":
            # Windows-specific settings
            config.update({
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.unsafe": "true",
            })
            
            # Try to auto-detect Java on Windows
            self._detect_java_home(config)
            
        elif system == "Linux":
            # Linux optimizations
            config.update({
                "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions",
                "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
            })
            
        elif system == "Darwin":  # macOS
            # macOS optimizations
            config.update({
                "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
                "spark.driver.extraJavaOptions": "-XX:+UseG1GC",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
            })
    
    def _apply_environment_optimizations(self, config: Dict[str, str]) -> None:
        """Apply environment-specific optimizations."""
        env = self._base_config.environment
        
        if env == Environment.DEVELOPMENT:
            # Development optimizations - faster startup
            config.update({
                "spark.sql.adaptive.localShuffleReader.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "false",  # Disable for small datasets
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
            })
            
        elif env == Environment.PRODUCTION:
            # Production optimizations - performance and stability
            config.update({
                "spark.sql.adaptive.localShuffleReader.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
                "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "128MB",
                "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
                "spark.default.parallelism": str(self._base_config.max_workers * 2),
            })
            
            # Enable metrics in production
            config.update({
                "spark.metrics.conf.executor.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
                "spark.metrics.conf.driver.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
            })
    
    def _detect_java_home(self, config: Dict[str, str]) -> None:
        """Auto-detect Java installation on Windows."""
        if os.environ.get("JAVA_HOME"):
            return
            
        # Common Java installation paths on Windows
        potential_paths = [
            Path("C:/Program Files/Eclipse Adoptium/jdk-17.0.8.101-hotspot"),
            Path("C:/Program Files/Java/jdk-17"),
            Path("C:/Program Files/Java/jdk-11"),
            Path("C:/Program Files/OpenJDK/jdk-17"),
            Path("C:/Program Files/OpenJDK/jdk-11"),
        ]
        
        for java_path in potential_paths:
            if java_path.exists() and (java_path / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(java_path)
                logger.info(f"Auto-detected JAVA_HOME: {java_path}")
                break
        else:
            logger.warning("Could not auto-detect Java installation. Please set JAVA_HOME manually.")
    
    def _configure_delta_lake(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """Configure Delta Lake support."""
        delta_packages = [
            "io.delta:delta-core_2.12:2.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ]
        
        return (builder
                .config("spark.jars.packages", ",".join(delta_packages))
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    
    def _configure_session_settings(self, spark: SparkSession, enable_adaptive_query: bool) -> None:
        """Configure session-level settings."""
        # Set up SQL configurations
        spark.conf.set("spark.sql.adaptive.enabled", str(enable_adaptive_query).lower())
        
        # Configure checkpoint directory
        checkpoint_dir = self._base_config.data_path / "checkpoints"
        checkpoint_dir.mkdir(exist_ok=True)
        spark.sparkContext.setCheckpointDir(str(checkpoint_dir))
        
        # Set log level based on environment
        if self._base_config.environment == Environment.PRODUCTION:
            spark.sparkContext.setLogLevel("WARN")
        else:
            spark.sparkContext.setLogLevel("INFO")
    
    def _cleanup_session(self) -> None:
        """Clean up Spark session on exit."""
        if self._spark_session:
            try:
                self._spark_session.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")
            finally:
                self._spark_session = None
    
    @contextmanager
    def session_context(self, **kwargs):
        """Context manager for Spark session with automatic cleanup."""
        session = self.get_session(**kwargs)
        try:
            yield session
        finally:
            # Session is managed globally, so we don't stop it here
            pass
    
    def stop_session(self) -> None:
        """Manually stop the current Spark session."""
        if self._spark_session:
            self._spark_session.stop()
            self._spark_session = None
            logger.info("Spark session stopped manually")
    
    def restart_session(self, **kwargs) -> SparkSession:
        """Restart the Spark session with new configuration."""
        self.stop_session()
        return self.get_session(**kwargs)
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get information about the current Spark session."""
        if not self._spark_session:
            return {"status": "no_session"}
        
        sc = self._spark_session.sparkContext
        return {
            "status": "active",
            "app_id": sc.applicationId,
            "app_name": sc.appName,
            "version": self._spark_session.version,
            "master": sc.master,
            "ui_url": sc.uiWebUrl,
            "default_parallelism": sc.defaultParallelism,
            "executor_memory": sc._conf.get("spark.executor.memory"),
            "driver_memory": sc._conf.get("spark.driver.memory"),
        }


# Global instance
spark_manager = SparkSessionManager()


def get_spark_session(**kwargs) -> SparkSession:
    """Convenience function to get Spark session."""
    return spark_manager.get_session(**kwargs)


def create_optimized_session(
    app_name: str,
    processing_type: str = "batch",
    data_size: str = "medium"
) -> SparkSession:
    """
    Create an optimized Spark session for specific use cases.
    
    Args:
        app_name: Application name
        processing_type: 'batch', 'streaming', or 'ml'
        data_size: 'small', 'medium', 'large'
    """
    config_overrides = {}
    
    # Optimize based on processing type
    if processing_type == "streaming":
        config_overrides.update({
            "spark.sql.streaming.metricsEnabled": "true",
            "spark.sql.streaming.ui.enabled": "true",
            "spark.sql.adaptive.enabled": "false",  # Not compatible with streaming
        })
    elif processing_type == "ml":
        config_overrides.update({
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrationRequired": "false",
        })
    
    # Optimize based on data size
    if data_size == "small":
        config_overrides.update({
            "spark.executor.instances": "1",
            "spark.executor.memory": "1g",
            "spark.sql.files.maxPartitionBytes": "32MB",
        })
    elif data_size == "large":
        config_overrides.update({
            "spark.executor.instances": "8",
            "spark.executor.memory": "8g",
            "spark.sql.files.maxPartitionBytes": "256MB",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
        })
    
    return spark_manager.get_session(
        app_name=app_name,
        config_overrides=config_overrides
    )


# Add alias method for backward compatibility
def get_or_create_session(app_name: str, **kwargs) -> SparkSession:
    """Alias for get_session for backward compatibility."""
    return spark_manager.get_session(app_name=app_name, **kwargs)


# Add the method to the class as well
SparkSessionManager.get_or_create_session = lambda self, app_name: self.get_session(app_name=app_name)