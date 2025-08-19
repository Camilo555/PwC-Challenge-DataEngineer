"""
Spark-based Silver Layer Implementation
Provides advanced data cleaning and transformation using PySpark
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


def _create_spark_session() -> SparkSession:
    """Create Spark session for Silver layer processing."""
    return (
        SparkSession.builder
        .appName("RetailETL-SilverLayer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", str(settings.silver_path))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def _read_bronze_data(spark: SparkSession) -> DataFrame:
    """Read data from Bronze layer."""
    bronze_path = settings.bronze_path / "sales"
    
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze dataset not found at {bronze_path}")
    
    # Read partitioned Parquet files from Bronze layer
    df = spark.read.parquet(str(bronze_path))
    logger.info(f"Loaded {df.count()} records from Bronze layer")
    
    return df


def _cast_and_clean_types(df: DataFrame) -> DataFrame:
    """Cast columns to appropriate types and clean data."""
    logger.info("Casting and cleaning data types...")
    
    # Cast numeric columns with error handling
    df = (df
          .withColumn("quantity", F.col("quantity").cast(IntegerType()))
          .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    )
    
    # Clean string columns
    string_columns = ["invoice_no", "stock_code", "description", "customer_id", "country"]
    for col in string_columns:
        if col in df.columns:
            df = df.withColumn(col, 
                              F.when(F.trim(F.col(col)).isin("", "nan", "None", "null"), None)
                              .otherwise(F.trim(F.col(col))))
    
    # Ensure timestamp is properly parsed
    if "invoice_timestamp" in df.columns:
        df = df.withColumn("invoice_timestamp", 
                          F.when(F.col("invoice_timestamp").isNull(), None)
                          .otherwise(F.col("invoice_timestamp")))
    
    return df


def _validate_business_rules(df: DataFrame) -> DataFrame:
    """Apply business rules validation and filtering."""
    logger.info("Applying business rules validation...")
    
    original_count = df.count()
    
    # Filter out invalid records based on business rules
    df_filtered = (df
                   .filter(F.col("quantity") > 0)  # Positive quantities only
                   .filter(F.col("unit_price") >= 0)  # Non-negative prices
                   .filter(F.col("invoice_no").isNotNull())  # Must have invoice number
                   .filter(F.trim(F.col("invoice_no")) != "")  # Invoice number not empty
    )
    
    filtered_count = df_filtered.count()
    removed_count = original_count - filtered_count
    
    logger.info(f"Removed {removed_count} records that failed business rules validation")
    logger.info(f"Remaining records: {filtered_count}")
    
    return df_filtered


def _add_derived_columns(df: DataFrame) -> DataFrame:
    """Add derived columns for analysis."""
    logger.info("Adding derived columns...")
    
    # Calculate total amount
    df = df.withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
    
    # Add date components if invoice_timestamp exists
    if "invoice_timestamp" in df.columns:
        df = (df
              .withColumn("invoice_date", F.to_date(F.col("invoice_timestamp")))
              .withColumn("invoice_year", F.year(F.col("invoice_timestamp")))
              .withColumn("invoice_month", F.month(F.col("invoice_timestamp")))
              .withColumn("invoice_quarter", F.quarter(F.col("invoice_timestamp")))
              .withColumn("invoice_day_of_week", F.date_format(F.col("invoice_timestamp"), "EEEE"))
              .withColumn("invoice_hour", F.hour(F.col("invoice_timestamp")))
        )
    
    # Add processing metadata
    df = (df
          .withColumn("processed_timestamp", F.current_timestamp())
          .withColumn("processing_engine", F.lit("pyspark"))
          .withColumn("processing_version", F.lit("3.5.3"))
    )
    
    return df


def _remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate records based on business key."""
    logger.info("Removing duplicates...")
    
    original_count = df.count()
    
    # Define business key columns for deduplication
    business_key_columns = ["invoice_no", "stock_code", "customer_id"]
    available_columns = [col for col in business_key_columns if col in df.columns]
    
    if available_columns:
        # Use window function to identify and remove duplicates
        window_spec = Window.partitionBy(*available_columns).orderBy(F.desc("ingestion_timestamp"))
        
        df_deduped = (df
                      .withColumn("row_number", F.row_number().over(window_spec))
                      .filter(F.col("row_number") == 1)
                      .drop("row_number")
        )
        
        deduped_count = df_deduped.count()
        duplicates_removed = original_count - deduped_count
        logger.info(f"Removed {duplicates_removed} duplicate records")
        
        return df_deduped
    else:
        logger.warning("No business key columns available for deduplication")
        return df


def _add_data_quality_metrics(df: DataFrame) -> DataFrame:
    """Add data quality metrics and flags."""
    logger.info("Adding data quality metrics...")
    
    # Add completeness scores
    df = (df
          .withColumn("completeness_score", 
                     (F.when(F.col("invoice_no").isNotNull(), 1).otherwise(0) +
                      F.when(F.col("stock_code").isNotNull(), 1).otherwise(0) +
                      F.when(F.col("description").isNotNull(), 1).otherwise(0) +
                      F.when(F.col("quantity").isNotNull(), 1).otherwise(0) +
                      F.when(F.col("unit_price").isNotNull(), 1).otherwise(0) +
                      F.when(F.col("invoice_timestamp").isNotNull(), 1).otherwise(0) +
                      F.when(F.col("country").isNotNull(), 1).otherwise(0)) / 7.0)
          .withColumn("is_high_quality", F.when(F.col("completeness_score") >= 0.8, True).otherwise(False))
    )
    
    # Add outlier detection for amounts
    amount_stats = df.select(
        F.mean("total_amount").alias("mean_amount"),
        F.stddev("total_amount").alias("stddev_amount")
    ).collect()[0]
    
    mean_amount = amount_stats["mean_amount"]
    stddev_amount = amount_stats["stddev_amount"]
    
    if mean_amount and stddev_amount:
        upper_bound = mean_amount + (3 * stddev_amount)
        lower_bound = max(0, mean_amount - (3 * stddev_amount))
        
        df = df.withColumn("is_amount_outlier", 
                          F.when((F.col("total_amount") > upper_bound) | 
                                (F.col("total_amount") < lower_bound), True).otherwise(False))
    else:
        df = df.withColumn("is_amount_outlier", F.lit(False))
    
    return df


def _generate_quality_report(df: DataFrame) -> Dict[str, Any]:
    """Generate comprehensive data quality report."""
    logger.info("Generating data quality report...")
    
    # Basic statistics
    total_records = df.count()
    
    # Null counts for each column
    null_counts = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_counts[col] = null_count
    
    # Data type information
    data_types = {field.name: str(field.dataType) for field in df.schema.fields}
    
    # Quality metrics
    quality_stats = df.select(
        F.avg("completeness_score").alias("avg_completeness"),
        F.sum(F.when(F.col("is_high_quality"), 1).otherwise(0)).alias("high_quality_records"),
        F.sum(F.when(F.col("is_amount_outlier"), 1).otherwise(0)).alias("amount_outliers")
    ).collect()[0]
    
    # Numeric statistics
    numeric_stats = df.select(
        F.min("total_amount").alias("min_amount"),
        F.max("total_amount").alias("max_amount"),
        F.avg("total_amount").alias("avg_amount"),
        F.stddev("total_amount").alias("stddev_amount"),
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity")
    ).collect()[0]
    
    report = {
        "total_records": total_records,
        "column_count": len(df.columns),
        "null_counts": null_counts,
        "data_types": data_types,
        "avg_completeness_score": float(quality_stats["avg_completeness"]) if quality_stats["avg_completeness"] else 0.0,
        "high_quality_records": int(quality_stats["high_quality_records"]),
        "high_quality_percentage": (int(quality_stats["high_quality_records"]) / total_records * 100) if total_records > 0 else 0.0,
        "amount_outliers": int(quality_stats["amount_outliers"]),
        "numeric_statistics": {
            "total_amount": {
                "min": float(numeric_stats["min_amount"]) if numeric_stats["min_amount"] else 0.0,
                "max": float(numeric_stats["max_amount"]) if numeric_stats["max_amount"] else 0.0,
                "avg": float(numeric_stats["avg_amount"]) if numeric_stats["avg_amount"] else 0.0,
                "stddev": float(numeric_stats["stddev_amount"]) if numeric_stats["stddev_amount"] else 0.0
            },
            "quantity": {
                "min": int(numeric_stats["min_quantity"]) if numeric_stats["min_quantity"] else 0,
                "max": int(numeric_stats["max_quantity"]) if numeric_stats["max_quantity"] else 0,
                "avg": float(numeric_stats["avg_quantity"]) if numeric_stats["avg_quantity"] else 0.0
            }
        }
    }
    
    return report


def process_silver_layer_spark() -> bool:
    """Main function to process Silver layer data transformation with Spark."""
    spark = None
    try:
        logger.info("Starting Silver layer processing with Spark...")
        
        # Create Spark session
        spark = _create_spark_session()
        
        # Read Bronze data
        df = _read_bronze_data(spark)
        logger.info(f"Loaded {df.count()} records from Bronze layer")
        
        # Data processing pipeline
        df = _cast_and_clean_types(df)
        df = _validate_business_rules(df)
        df = _add_derived_columns(df)
        df = _remove_duplicates(df)
        df = _add_data_quality_metrics(df)
        
        # Generate quality report
        quality_report = _generate_quality_report(df)
        logger.info(f"Processed {quality_report['total_records']} records with "
                   f"{quality_report['high_quality_percentage']:.1f}% high quality")
        
        # Save to Silver layer
        silver_path = settings.silver_path / "sales"
        silver_path.mkdir(parents=True, exist_ok=True)
        
        # Write as partitioned Parquet files (by year if available)
        logger.info(f"Writing {df.count()} records to Silver layer...")
        
        if "invoice_year" in df.columns:
            # Partition by year for better query performance
            df.write.mode("overwrite").partitionBy("invoice_year").parquet(str(silver_path))
        else:
            # Save as single partitioned file
            df.write.mode("overwrite").parquet(str(silver_path))
        
        # Save quality report
        report_file = silver_path / "quality_report.json"
        with open(report_file, 'w') as f:
            json.dump(quality_report, f, indent=2, default=str)
        logger.info(f"Quality report saved to {report_file}")
        
        logger.info(f"Silver layer processing completed successfully - {quality_report['total_records']} records processed")
        return True
        
    except Exception as e:
        logger.error(f"Silver layer processing failed: {e}")
        return False
    finally:
        if spark:
            spark.stop()


def main() -> None:
    """Entry point for Spark Silver layer processing."""
    logger.info("Starting Spark-based Silver layer ETL...")
    
    success = process_silver_layer_spark()
    if success:
        print("Silver layer processing completed successfully (Spark-based)")
    else:
        print("Silver layer processing failed")
        exit(1)


if __name__ == "__main__":
    main()