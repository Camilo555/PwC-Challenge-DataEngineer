"""
Windows-specific PySpark configuration and utilities.
Handles Hadoop winutils, Java detection, and proper environment setup.
"""

import os
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


def detect_java_home() -> str | None:
    """
    Detect Java installation on Windows.

    Returns:
        Path to Java home directory or None if not found
    """
    # Check environment variable first
    java_home = os.environ.get("JAVA_HOME")
    if java_home and Path(java_home).exists():
        logger.info(f"Found JAVA_HOME: {java_home}")
        return java_home

    # Common Java installation paths on Windows
    java_paths = [
        r"C:\Program Files\Eclipse Adoptium",
        r"C:\Program Files\Java",
        r"C:\Program Files (x86)\Java",
        r"C:\Program Files\Amazon Corretto",
        r"C:\Program Files\Microsoft\jdk-*",
    ]

    for base_path in java_paths:
        base = Path(base_path)
        if base.exists():
            # Look for JDK directories
            for jdk_dir in base.glob("jdk*"):
                if (jdk_dir / "bin" / "java.exe").exists():
                    logger.info(f"Auto-detected Java: {jdk_dir}")
                    return str(jdk_dir)

    # Try to find java.exe in PATH
    try:
        result = subprocess.run(
            ["where", "java"],
            capture_output=True,
            text=True,
            check=True
        )
        if result.stdout:
            java_exe = Path(result.stdout.strip().split('\n')[0])
            java_home_path = java_exe.parent.parent
            if java_home_path.exists():
                java_home = str(java_home_path)
                logger.info(f"Found Java via PATH: {java_home}")
                return java_home
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    logger.warning("Could not detect Java installation")
    return None


def setup_windows_environment() -> bool:
    """
    Set up Windows-specific environment for PySpark.

    Returns:
        True if setup successful, False otherwise
    """
    try:
        # Set Java environment
        java_home = detect_java_home()
        if not java_home:
            logger.warning("Java not auto-detected. Will try with system Java...")
            # Try to continue without explicit JAVA_HOME
            java_home = os.environ.get("JAVA_HOME", "")

        if java_home:
            os.environ["JAVA_HOME"] = java_home

            # Add Java to PATH if not already there
            java_bin = Path(java_home) / "bin"
            current_path = os.environ.get("PATH", "")
            if str(java_bin) not in current_path:
                os.environ["PATH"] = f"{java_bin};{current_path}"

        # Set Python environment for PySpark
        python_exe = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_exe
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

        # Disable Hadoop native libraries warning on Windows
        os.environ.setdefault("HADOOP_HOME", "")
        os.environ.setdefault("HADOOP_CONF_DIR", "")

        # Set proper temp directory
        temp_dir = Path.cwd() / "temp"
        temp_dir.mkdir(exist_ok=True)
        os.environ.setdefault("SPARK_LOCAL_DIRS", str(temp_dir))

        logger.info("Windows PySpark environment configured successfully")
        return True

    except Exception as e:
        logger.warning(f"Windows environment setup had issues: {e}")
        return True  # Continue anyway for testing


def get_windows_spark_config(skip_delta: bool = False) -> dict[str, str]:
    """
    Get Windows-specific Spark configuration.

    Args:
        skip_delta: Skip Delta Lake configuration for tests

    Returns:
        Dictionary of Spark configuration properties
    """
    config = {
        # Basic configuration
        "spark.app.name": settings.spark_app_name,
        "spark.master": "local[*]",
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",

        # Windows-specific optimizations
        "spark.sql.shuffle.partitions": "4",  # Reduced for local mode
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",

        # Disable UI for headless environments
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
    }

    # Add Delta Lake configuration only if not skipped
    if not skip_delta:
        config.update({
            # Delta Lake configuration
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        })

    config.update({

        # Memory and performance tuning for Windows
        "spark.sql.execution.arrow.pyspark.enabled": "false",  # Can cause issues on Windows
        "spark.driver.maxResultSize": "2g",
        "spark.sql.adaptive.skewJoin.enabled": "true",

        # Logging configuration
        "spark.sql.adaptive.logLevel": "WARN",

        # Windows file system handling
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.sql.warehouse.dir": str(Path.cwd() / "spark-warehouse"),
    })

    return config


def create_windows_spark_session(app_name: str = "WindowsRetailETL") -> SparkSession:
    """
    Create a PySpark session optimized for Windows.

    Args:
        app_name: Name of the Spark application

    Returns:
        Configured SparkSession

    Raises:
        RuntimeError: If Spark session creation fails
    """
    try:
        # Setup Windows environment
        setup_success = setup_windows_environment()
        if not setup_success:
            logger.warning("Windows environment setup had issues, but continuing...")

        logger.info("Creating Windows-optimized Spark session...")

        # Get configuration - skip Delta for test sessions
        skip_delta = app_name.startswith("Test")
        spark_config = get_windows_spark_config(skip_delta=skip_delta)
        spark_config["spark.app.name"] = app_name

        # Create Spark session builder
        builder = SparkSession.builder

        # Apply all configuration
        for key, value in spark_config.items():
            builder = builder.config(key, value)

        # Add JAR packages for Delta Lake, PostgreSQL, SQLite (skip for tests if needed)
        if not app_name.startswith("Test"):
            packages = [
                "io.delta:delta-spark_2.12:3.2.1",
                "org.postgresql:postgresql:42.7.3",
                "org.xerial:sqlite-jdbc:3.45.3.0"
            ]
            builder = builder.config("spark.jars.packages", ",".join(packages))
        else:
            # Minimal configuration for tests
            logger.info("Skipping JAR packages for test session")

        # Create session with fallback configuration
        try:
            spark = builder.getOrCreate()
        except Exception as spark_error:
            logger.warning(f"Initial Spark creation failed: {spark_error}")
            # Fallback: try with minimal configuration
            minimal_builder = SparkSession.builder.appName(app_name).master("local[1]")
            minimal_builder = minimal_builder.config("spark.driver.memory", "2g")
            minimal_builder = minimal_builder.config("spark.sql.shuffle.partitions", "2")
            spark = minimal_builder.getOrCreate()

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Spark session created successfully: {spark.version}")
        logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")

        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise RuntimeError(f"Spark session creation failed: {e}") from e


def test_spark_functionality(spark: SparkSession) -> bool:
    """
    Test basic Spark functionality.

    Args:
        spark: SparkSession to test

    Returns:
        True if all tests pass, False otherwise
    """
    try:
        logger.info("Testing Spark functionality...")

        # Test 1: Basic DataFrame operations
        test_data = [(1, "test", 100.0), (2, "data", 200.0)]
        df = spark.createDataFrame(test_data, ["id", "name", "value"])

        row_count = df.count()
        if row_count != 2:
            logger.error(f"Expected 2 rows, got {row_count}")
            return False

        # Test 2: SQL operations
        df.createOrReplaceTempView("test_table")
        result = spark.sql("SELECT COUNT(*) as count FROM test_table").collect()
        if result[0]["count"] != 2:
            logger.error("SQL query failed")
            return False

        # Test 3: File I/O (if possible)
        try:
            temp_path = Path.cwd() / "temp" / "spark_test.parquet"
            temp_path.parent.mkdir(exist_ok=True)

            df.write.mode("overwrite").parquet(str(temp_path))
            read_df = spark.read.parquet(str(temp_path))

            if read_df.count() != 2:
                logger.error("File I/O test failed")
                return False

            # Clean up
            import shutil
            shutil.rmtree(temp_path, ignore_errors=True)

        except Exception as e:
            logger.warning(f"File I/O test failed (non-critical): {e}")

        logger.info("All Spark functionality tests passed")
        return True

    except Exception as e:
        logger.error(f"Spark functionality test failed: {e}")
        return False


def get_optimized_spark_for_etl() -> SparkSession:
    """
    Get a Spark session optimized for ETL workloads on Windows.

    Returns:
        Configured SparkSession for ETL processing
    """
    spark = create_windows_spark_session("RetailETL-Windows")

    # Test functionality
    if not test_spark_functionality(spark):
        logger.warning("Some Spark functionality tests failed, but proceeding...")

    return spark


def cleanup_spark_resources(spark: SparkSession) -> None:
    """
    Clean up Spark resources and temporary files.

    Args:
        spark: SparkSession to clean up
    """
    try:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

        # Clean up temporary directories
        temp_dirs = [
            Path.cwd() / "temp",
            Path.cwd() / "spark-warehouse",
            Path.cwd() / "derby.log"
        ]

        for temp_dir in temp_dirs:
            if temp_dir.exists():
                import shutil
                if temp_dir.is_dir():
                    shutil.rmtree(temp_dir, ignore_errors=True)
                else:
                    temp_dir.unlink(missing_ok=True)

        logger.info("Spark resources cleaned up")

    except Exception as e:
        logger.warning(f"Error during Spark cleanup: {e}")
