"""
Spark-based Bronze Layer Implementation
Provides scalable data ingestion using PySpark
"""
from __future__ import annotations

import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)

# Define schema for sales data
SALES_SCHEMA = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])


def _create_spark_session() -> SparkSession:
    """Create Spark session for Bronze layer processing."""
    import platform

    # Use Windows-optimized configuration if on Windows
    if platform.system() == "Windows":
        try:
            from etl.utils.windows_spark import create_windows_spark_session
            logger.info("Using Windows-optimized Spark session for Bronze layer")
            return create_windows_spark_session("RetailETL-BronzeLayer")
        except Exception as e:
            logger.warning(f"Windows Spark session failed, falling back to standard: {e}")

    # Standard configuration for non-Windows systems
    return (
        SparkSession.builder
        .appName("RetailETL-BronzeLayer")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", str(settings.bronze_path))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def _list_raw_csvs(raw_dir: Path) -> list[str]:
    """List CSV files in raw directory."""
    if not raw_dir.exists():
        return []

    csv_files = []
    for file_path in raw_dir.rglob("*.csv"):
        csv_files.append(str(file_path.absolute()))

    return sorted(csv_files)


def _read_and_normalize_csvs(spark: SparkSession, files: list[str]) -> DataFrame:
    """Read and normalize CSV files with chunked processing for memory efficiency."""
    if not files:
        raise FileNotFoundError(
            f"No raw CSV files found under {settings.raw_data_path.resolve()}"
        )

    job_id = str(uuid.uuid4())
    logger.info(f"Processing {len(files)} CSV files with job ID: {job_id}")

    # Check file sizes and enable chunked processing for large files
    large_files = []
    normal_files = []

    for file_path in files:
        file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)
        if file_size_mb > 100:  # Files larger than 100MB
            large_files.append(file_path)
            logger.info(f"Large file detected: {Path(file_path).name} ({file_size_mb:.1f}MB)")
        else:
            normal_files.append(file_path)

    dfs = []

    # Process normal files as before
    for file_path in normal_files:
        try:
            # Read CSV with flexible schema detection
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "M/d/yyyy H:mm")
                .csv(file_path)
            )

            # Add source metadata
            df = df.withColumn("source_file_path", F.lit(file_path))
            df = df.withColumn("source_file_type", F.lit("csv"))
            df = df.withColumn("ingestion_job_id", F.lit(job_id))

            dfs.append(df)
            logger.info(f"Read {df.count()} records from {Path(file_path).name}")

        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            continue

    # Process large files with chunked reading
    for file_path in large_files:
        try:
            # Read large files with additional memory optimizations
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")  # Skip schema inference for large files
                .option("timestampFormat", "M/d/yyyy H:mm")
                .option("multiline", "false")
                .option("maxColumns", "50")  # Limit columns for memory efficiency
                .csv(file_path)
            )

            # Repartition for better memory distribution
            df = df.repartition(8)  # Distribute across more partitions

            # Add source metadata
            df = df.withColumn("source_file_path", F.lit(file_path))
            df = df.withColumn("source_file_type", F.lit("csv"))
            df = df.withColumn("ingestion_job_id", F.lit(job_id))
            df = df.withColumn("is_large_file", F.lit(True))

            dfs.append(df)
            logger.info(f"Read large file {Path(file_path).name} with chunked processing")

        except Exception as e:
            logger.error(f"Failed to read large file {file_path}: {e}")
            continue

    if not dfs:
        raise ValueError("No CSV files could be read successfully")

    # Union all dataframes with memory-efficient approach
    if len(dfs) == 1:
        combined_df = dfs[0]
    else:
        # Use iterative union to avoid memory spikes
        combined_df = dfs[0]
        for i, df in enumerate(dfs[1:], 1):
            try:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)

                # Cache periodically to avoid recomputation
                if i % 3 == 0:
                    combined_df.cache()
                    logger.debug(f"Cached dataframe after unioning {i+1} files")

            except Exception as e:
                logger.error(f"Failed to union dataframe {i}: {e}")
                continue

    # Final count with caching for performance
    combined_df.cache()
    total_count = combined_df.count()
    logger.info(f"Combined {total_count} total records from {len(dfs)} files")

    return combined_df


def _normalize_column_names(df: DataFrame) -> DataFrame:
    """Normalize column names to standard format."""
    logger.info("Normalizing column names...")

    # Column name mapping - avoid conflicts with existing columns
    column_mapping = {
        "InvoiceNo": "invoice_no",
        "invoiceno": "invoice_no",
        "StockCode": "stock_code",
        "stockcode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "UnitPrice": "unit_price",
        "unitprice": "unit_price",
        "CustomerID": "customer_id",
        "customerid": "customer_id",
        "Country": "country"
    }

    # Handle InvoiceDate -> invoice_timestamp mapping carefully
    if "InvoiceDate" in df.columns and "invoice_timestamp" not in df.columns:
        column_mapping["InvoiceDate"] = "invoice_timestamp"
    elif "invoicedate" in df.columns and "invoice_timestamp" not in df.columns:
        column_mapping["invoicedate"] = "invoice_timestamp"

    # Apply column renaming only for columns that exist and won't cause conflicts
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            logger.info(f"Renamed column: {old_name} -> {new_name}")

    return df


def _add_metadata_and_partitioning(df: DataFrame) -> DataFrame:
    """Add metadata and partitioning columns."""
    logger.info("Adding metadata and partitioning columns...")

    # Add ingestion metadata (only if not already present)
    if "ingestion_timestamp" not in df.columns:
        df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    if "schema_version" not in df.columns:
        df = df.withColumn("schema_version", F.lit("1.0"))
    if "row_id" not in df.columns:
        df = df.withColumn("row_id", F.monotonically_increasing_id())

    # Parse invoice timestamp and add partitioning columns
    if "invoice_timestamp" in df.columns:
        # Ensure proper timestamp format
        df = df.withColumn("invoice_timestamp_parsed",
                          F.to_timestamp(F.col("invoice_timestamp"), "M/d/yyyy H:mm"))
        df = df.drop("invoice_timestamp").withColumnRenamed("invoice_timestamp_parsed", "invoice_timestamp")
        df = df.withColumn("invoice_date", F.to_date(F.col("invoice_timestamp")))

    # Add ingestion date for partitioning
    df = df.withColumn("ingestion_date", F.current_date())

    return df


def _ensure_required_columns(df: DataFrame) -> DataFrame:
    """Ensure all required columns exist."""
    required_columns = [
        "invoice_no", "stock_code", "description", "quantity",
        "unit_price", "invoice_timestamp", "customer_id", "country"
    ]

    for col in required_columns:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))
            logger.info(f"Added missing column: {col}")

    return df


def _apply_data_quality_rules(df: DataFrame) -> DataFrame:
    """Apply basic data quality rules."""
    logger.info("Applying data quality rules...")

    original_count = df.count()

    # Add data quality flags instead of filtering
    df = (df
          .withColumn("is_valid_quantity", F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), False).otherwise(True))
          .withColumn("is_valid_price", F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), False).otherwise(True))
          .withColumn("has_invoice_no", F.when(F.col("invoice_no").isNull() | (F.trim(F.col("invoice_no")) == ""), False).otherwise(True))
          .withColumn("data_quality_score",
                     (F.col("is_valid_quantity").cast("int") +
                      F.col("is_valid_price").cast("int") +
                      F.col("has_invoice_no").cast("int")) / 3.0)
    )

    logger.info(f"Applied data quality flags to {original_count} records")
    return df


def ingest_bronze_spark() -> bool:
    """Main Spark-based bronze ingestion function."""
    spark = None
    try:
        logger.info("Starting Spark-based Bronze layer ingestion...")

        # Create Spark session
        spark = _create_spark_session()

        raw_dir = settings.raw_data_path
        out_dir = settings.bronze_path / "sales"

        # List CSV files
        files = _list_raw_csvs(raw_dir)
        logger.info(f"Found {len(files)} CSV files to process")

        if not files:
            logger.warning("No CSV files found in raw directory")
            return False

        # Read and process data
        df = _read_and_normalize_csvs(spark, files)
        df = _normalize_column_names(df)
        df = _ensure_required_columns(df)
        df = _add_metadata_and_partitioning(df)
        df = _apply_data_quality_rules(df)

        # Create output directory
        out_dir.mkdir(parents=True, exist_ok=True)

        # Write data with Windows compatibility
        logger.info(f"Writing {df.count()} records to Bronze layer...")

        import platform
        if platform.system() == "Windows":
            # Use Windows-compatible writing approach
            try:
                # Try to write as single file to avoid partitioning issues on Windows
                df.coalesce(1).write.mode("overwrite").parquet(str(out_dir / "bronze_data.parquet"))
                logger.info(f"Bronze ingest complete -> {out_dir}/bronze_data.parquet (Windows-compatible mode)")
            except Exception as e:
                logger.warning(f"Parquet writing failed on Windows, trying CSV: {e}")
                # Fallback to CSV if Parquet fails
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(out_dir / "bronze_data.csv"))
                logger.info(f"Bronze ingest complete -> {out_dir}/bronze_data.csv (CSV fallback mode)")
        else:
            # Standard partitioned approach for Linux/Unix
            df.write.mode("overwrite").partitionBy("ingestion_date").parquet(str(out_dir))
            logger.info(f"Bronze ingest complete -> {out_dir} (partitioned parquet mode)")

        return True

    except Exception as e:
        logger.error(f"Spark Bronze ingestion failed: {e}")
        return False
    finally:
        if spark:
            spark.stop()


def main() -> None:
    """Entry point for Spark Bronze layer ingestion."""
    logger.info("Starting Spark-based Bronze layer ETL...")
    settings.validate_paths()

    success = ingest_bronze_spark()
    if success:
        print("Bronze ingest completed successfully (Spark-based)")
    else:
        print("Bronze ingest failed")
        exit(1)


if __name__ == "__main__":
    main()
