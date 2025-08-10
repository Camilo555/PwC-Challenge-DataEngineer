from pathlib import Path
from pyspark.sql import SparkSession
from de_challenge.core.config import settings


def get_spark(app_name: str | None = None) -> SparkSession:
    app = app_name or settings.spark_app_name
    builder = (
        SparkSession.builder.appName(app)
        .master(settings.spark_master)
    )
    for k, v in settings.spark_config.items():
        builder = builder.config(k, v)
    # Load extra configs from config/spark-defaults.conf if present
    conf_path = Path("config/spark-defaults.conf")
    if conf_path.exists():
        for line in conf_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # spark.key value -> key may contain dots and value may contain spaces
            parts = line.split(None, 1)
            if len(parts) == 2:
                key, value = parts
                builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(settings.delta_log_level)
    return spark
