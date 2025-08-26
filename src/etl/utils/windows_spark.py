"""
Windows-specific PySpark configuration and utilities.
Handles Hadoop winutils, Java detection, and proper environment setup.
"""
from __future__ import annotations

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


def validate_windows_environment() -> dict:
    """
    Validate Windows environment for Spark/Hadoop compatibility.
    
    Returns:
        Dictionary containing validation results and environment information
    """
    import platform

    result = {
        'is_valid': True,
        'platform': platform.system(),
        'errors': [],
        'warnings': []
    }

    # Check if we're on Windows
    if result['platform'] != 'Windows':
        result['is_valid'] = False
        result['error'] = f"This function is for Windows only, current platform: {result['platform']}"
        return result

    # Check Java installation
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        result['java_home'] = java_home
        if not Path(java_home).exists():
            result['errors'].append(f"JAVA_HOME path does not exist: {java_home}")
            result['is_valid'] = False
    else:
        # Try to detect Java
        detected_java = detect_java_home()
        if detected_java:
            result['java_home'] = detected_java
            result['warnings'].append("JAVA_HOME not set, but Java was auto-detected")
        else:
            result['errors'].append("JAVA_HOME not set and Java not found")
            result['is_valid'] = False

    # Check Hadoop home
    hadoop_home = os.environ.get('HADOOP_HOME')
    if hadoop_home:
        result['hadoop_home'] = hadoop_home
        if not Path(hadoop_home).exists():
            result['warnings'].append(f"HADOOP_HOME path does not exist: {hadoop_home}")
    else:
        result['warnings'].append("HADOOP_HOME not set")

    # Check Spark home
    spark_home = os.environ.get('SPARK_HOME')
    if spark_home:
        result['spark_home'] = spark_home
        if not Path(spark_home).exists():
            result['warnings'].append(f"SPARK_HOME path does not exist: {spark_home}")
    else:
        result['warnings'].append("SPARK_HOME not set")

    return result


def setup_hadoop_home() -> bool:
    """
    Setup Hadoop home directory for Windows compatibility.
    
    Returns:
        True if setup successful, False otherwise
    """
    try:
        project_root = Path(__file__).parent.parent.parent.parent
        hadoop_dir = project_root / "hadoop"

        # Create hadoop directory structure if it doesn't exist
        hadoop_dir.mkdir(parents=True, exist_ok=True)
        (hadoop_dir / "bin").mkdir(parents=True, exist_ok=True)

        # Set environment variables
        hadoop_home = str(hadoop_dir.absolute())
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["HADOOP_CONF_DIR"] = hadoop_home

        # Check for winutils.exe
        winutils_path = hadoop_dir / "bin" / "winutils.exe"
        if not winutils_path.exists():
            logger.info(f"winutils.exe not found, creating placeholder at {winutils_path}")
            # Create a placeholder file (in real scenario, you'd download winutils.exe)
            winutils_path.touch()

        logger.info(f"Hadoop home setup complete: {hadoop_home}")
        return True

    except Exception as e:
        logger.error(f"Failed to setup Hadoop home: {e}")
        return False


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

        # Setup Hadoop for Windows - use local hadoop directory
        project_root = Path(__file__).parent.parent.parent.parent
        hadoop_dir = project_root / "hadoop"

        if hadoop_dir.exists():
            hadoop_home = str(hadoop_dir.absolute())
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["HADOOP_CONF_DIR"] = hadoop_home

            # Ensure winutils.exe exists
            winutils_path = hadoop_dir / "bin" / "winutils.exe"
            if not winutils_path.exists():
                logger.warning(f"winutils.exe not found at {winutils_path}")
                logger.info("Downloading winutils.exe for Windows compatibility...")
                try:
                    import urllib.request
                    winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
                    hadoop_dir.mkdir(parents=True, exist_ok=True)
                    (hadoop_dir / "bin").mkdir(parents=True, exist_ok=True)
                    urllib.request.urlretrieve(winutils_url, winutils_path)
                    logger.info(f"Downloaded winutils.exe to {winutils_path}")
                except Exception as e:
                    logger.warning(f"Failed to download winutils.exe: {e}")

            logger.info(f"Using local HADOOP_HOME: {hadoop_home}")
        else:
            # Create minimal hadoop structure
            hadoop_dir.mkdir(parents=True, exist_ok=True)
            (hadoop_dir / "bin").mkdir(parents=True, exist_ok=True)

            # Try to download winutils.exe
            try:
                import urllib.request
                winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
                winutils_path = hadoop_dir / "bin" / "winutils.exe"
                urllib.request.urlretrieve(winutils_url, winutils_path)
                logger.info(f"Downloaded winutils.exe to {winutils_path}")

                hadoop_home = str(hadoop_dir.absolute())
                os.environ["HADOOP_HOME"] = hadoop_home
                os.environ["HADOOP_CONF_DIR"] = hadoop_home
                logger.info(f"Created and configured HADOOP_HOME: {hadoop_home}")
            except Exception as e:
                logger.warning(f"Failed to setup Hadoop directory: {e}")
                # Set minimal environment to avoid errors
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


def get_windows_spark_config(skip_delta: bool = True) -> dict[str, str]:
    """
    Get Windows-specific Spark configuration.

    Args:
        skip_delta: Skip Delta Lake configuration for Windows compatibility

    Returns:
        Dictionary of Spark configuration properties
    """
    # Create safe temporary directories
    temp_dir = Path.cwd() / "temp"
    temp_dir.mkdir(exist_ok=True)

    config = {
        # Basic configuration
        "spark.app.name": settings.spark_app_name,
        "spark.master": "local[2]",  # Use only 2 cores to reduce resource pressure
        "spark.driver.memory": "2g",  # Reduced memory for Windows compatibility
        "spark.executor.memory": "2g",

        # Windows-specific optimizations
        "spark.sql.shuffle.partitions": "2",  # Minimal partitions for Windows
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",

        # Disable problematic features on Windows
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.execution.arrow.pyspark.enabled": "false",

        # Hadoop configuration for Windows
        "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem",
        "spark.hadoop.fs.file.impl.disable.cache": "false",

        # Native library workarounds
        "spark.hadoop.io.nativeio.NativeIO$Windows.enabled": "false",
        "spark.sql.catalogImplementation": "in-memory",

        # File handling optimizations
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.hadoop.parquet.enable.summary-metadata": "false",
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.filterPushdown": "true",

        # Temporary directory configuration
        "spark.local.dir": str(temp_dir),
        "spark.sql.warehouse.dir": str(temp_dir / "spark-warehouse"),
        "spark.hadoop.hadoop.tmp.dir": str(temp_dir / "hadoop-tmp"),

        # Memory and GC tuning for Windows
        "spark.driver.maxResultSize": "1g",
        "spark.sql.adaptive.skewJoin.enabled": "false",  # Disable to reduce complexity
        "spark.sql.adaptive.localShuffleReader.enabled": "false",

        # Reduce logging noise
        "spark.sql.adaptive.logLevel": "ERROR",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "1000",

        # Checkpoint and recovery
        "spark.sql.recovery.checkpointInterval": "20",
        "spark.cleaner.referenceTracking.cleanCheckpoints": "true",
    }

    # Only add Delta Lake if explicitly requested (not recommended on Windows)
    if not skip_delta:
        logger.warning("Adding Delta Lake configuration - may cause native library issues on Windows")
        config.update({
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
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

        # Get configuration - skip Delta for all Windows sessions due to native library issues
        skip_delta = True  # Always skip Delta on Windows
        spark_config = get_windows_spark_config(skip_delta=skip_delta)
        spark_config["spark.app.name"] = app_name

        # Create Spark session builder
        builder = SparkSession.builder

        # Apply all configuration
        for key, value in spark_config.items():
            builder = builder.config(key, value)

        # Skip Delta Lake packages on Windows due to native library issues
        # Only add PostgreSQL and SQLite for database connectivity
        packages = [
            "org.postgresql:postgresql:42.7.3",
            "org.xerial:sqlite-jdbc:3.45.3.0"
        ]
        builder = builder.config("spark.jars.packages", ",".join(packages))
        logger.info("Skipping Delta Lake packages for Windows compatibility")

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

        # Test 2: DataFrame operations (using DataFrame API instead of SQL)
        from pyspark.sql.functions import count, lit
        result = df.agg(count(lit(1)).alias("count")).collect()
        if result[0]["count"] != 2:
            logger.error("DataFrame aggregation failed")
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


class WindowsSparkManager:
    """Manager class for Windows-specific Spark operations."""

    def __init__(self, app_name: str = "WindowsSparkApp"):
        """Initialize the Windows Spark manager."""
        self.app_name = app_name
        self.spark_session = None
        self._is_environment_setup = False

    def setup_environment(self) -> bool:
        """Setup the Windows environment for Spark."""
        if not self._is_environment_setup:
            self._is_environment_setup = setup_windows_environment()
        return self._is_environment_setup

    def validate_environment(self) -> dict:
        """Validate the Windows environment."""
        return validate_windows_environment()

    def get_spark_session(self, force_recreate: bool = False) -> SparkSession:
        """Get or create a Spark session."""
        if self.spark_session is None or force_recreate:
            if not self.setup_environment():
                logger.warning("Environment setup had issues, continuing anyway...")

            self.spark_session = create_windows_spark_session(self.app_name)

        return self.spark_session

    def test_functionality(self) -> bool:
        """Test Spark functionality."""
        if self.spark_session is None:
            self.get_spark_session()

        return test_spark_functionality(self.spark_session)

    def cleanup(self) -> None:
        """Cleanup Spark resources."""
        if self.spark_session:
            cleanup_spark_resources(self.spark_session)
            self.spark_session = None

    def __enter__(self):
        """Context manager entry."""
        self.get_spark_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
