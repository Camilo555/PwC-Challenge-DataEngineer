import platform
from pathlib import Path

from pyspark.sql import SparkSession

from de_challenge.core.config import settings
from de_challenge.core.logging import get_logger

logger = get_logger(__name__)


def get_spark(app_name: str | None = None) -> SparkSession:
    """Get or create a Spark session with platform-specific optimizations."""
    app = app_name or settings.spark_app_name

    # Use Windows-specific configuration if on Windows
    if platform.system() == "Windows":
        try:
            from de_challenge.etl.utils.windows_spark import create_windows_spark_session
            logger.info("Using Windows-optimized Spark configuration")
            return create_windows_spark_session(app)
        except ImportError:
            logger.warning("Windows Spark utilities not available, using standard configuration")
        except Exception as e:
            logger.warning(f"Windows Spark configuration failed, falling back to standard: {e}")

    # Standard Spark configuration
    logger.info("Using standard Spark configuration")

    builder = (
        SparkSession.builder.appName(app)
        .master(settings.spark_master)
    )

    # Apply base configuration
    spark_config = settings.spark_config
    for k, v in spark_config.items():
        builder = builder.config(k, v)

    # Load extra configs from config/spark-defaults.conf if present
    conf_path = Path("config/spark-defaults.conf")
    if conf_path.exists():
        logger.info(f"Loading additional Spark configuration from {conf_path}")
        for line in conf_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # spark.key value -> key may contain dots and value may contain spaces
            parts = line.split(None, 1)
            if len(parts) == 2:
                key, value = parts
                builder = builder.config(key, value)

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(settings.delta_log_level)
        logger.info(f"Spark session created successfully: {spark.version}")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise
