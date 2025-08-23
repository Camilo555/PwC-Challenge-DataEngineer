"""
Spark-based Gold Layer Implementation
Provides advanced analytics and aggregations using PySpark
"""
from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


def _create_spark_session() -> SparkSession:
    """Create Spark session for Gold layer processing."""
    return (
        SparkSession.builder
        .appName("RetailETL-GoldLayer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", str(settings.gold_path))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def _read_silver_data(spark: SparkSession) -> DataFrame:
    """Read data from Silver layer."""
    silver_path = settings.silver_path / "sales"

    if not silver_path.exists():
        raise FileNotFoundError(f"Silver dataset not found at {silver_path}")

    # Read partitioned Parquet files from Silver layer
    df = spark.read.parquet(str(silver_path))
    logger.info(f"Loaded {df.count()} records from Silver layer")

    return df


def _create_sales_summary(df: DataFrame) -> DataFrame:
    """Create sales summary aggregations."""
    logger.info("Creating sales summary aggregations...")

    sales_summary = (
        df.groupBy("country", "invoice_year", "invoice_month")
        .agg(
            F.count("invoice_no").alias("total_transactions"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("stock_code").alias("unique_products")
        )
        .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("unique_customers"))
    )

    return sales_summary


def _create_product_analysis(df: DataFrame) -> DataFrame:
    """Create product performance analysis."""
    logger.info("Creating product analysis...")

    product_analysis = (
        df.groupBy("stock_code", "description", "country")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.count("invoice_no").alias("transaction_count"),
            F.avg("unit_price").alias("avg_unit_price"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("revenue_per_transaction", F.col("total_revenue") / F.col("transaction_count"))
    )

    # Add product ranking within each country
    window_spec = Window.partitionBy("country").orderBy(F.desc("total_revenue"))
    product_analysis = product_analysis.withColumn(
        "revenue_rank_in_country", F.row_number().over(window_spec)
    )

    return product_analysis


def _create_customer_segmentation(df: DataFrame) -> DataFrame:
    """Create customer segmentation analysis."""
    logger.info("Creating customer segmentation...")

    customer_metrics = (
        df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id", "country")
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.count("invoice_no").alias("transaction_count"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.countDistinct("stock_code").alias("unique_products_bought"),
            F.min("invoice_timestamp").alias("first_purchase"),
            F.max("invoice_timestamp").alias("last_purchase"),
            F.sum("quantity").alias("total_quantity_purchased")
        )
        .withColumn("customer_lifetime_days",
                   F.datediff(F.col("last_purchase"), F.col("first_purchase")))
    )

    # Customer segmentation based on RFM analysis
    recency_window = Window.orderBy(F.desc("last_purchase"))
    frequency_window = Window.orderBy(F.desc("transaction_count"))
    monetary_window = Window.orderBy(F.desc("total_spent"))

    customer_segmentation = (
        customer_metrics
        .withColumn("recency_score", F.ntile(5).over(recency_window))
        .withColumn("frequency_score", F.ntile(5).over(frequency_window))
        .withColumn("monetary_score", F.ntile(5).over(monetary_window))
        .withColumn("rfm_score",
                   F.concat(F.col("recency_score"), F.col("frequency_score"), F.col("monetary_score")))
        .withColumn("customer_segment",
                   F.when(F.col("rfm_score").rlike("^[4-5][4-5][4-5]$"), "Champions")
                   .when(F.col("rfm_score").rlike("^[3-5][1-3][4-5]$"), "Loyal Customers")
                   .when(F.col("rfm_score").rlike("^[4-5][1-2][1-3]$"), "Big Spenders")
                   .when(F.col("rfm_score").rlike("^[4-5][3-5][1-3]$"), "New Customers")
                   .when(F.col("rfm_score").rlike("^[3-4][1-3][1-3]$"), "Potential Loyalists")
                   .when(F.col("rfm_score").rlike("^[2-3][2-3][2-3]$"), "Need Attention")
                   .when(F.col("rfm_score").rlike("^[1-2][4-5][1-2]$"), "Cannot Lose Them")
                   .when(F.col("rfm_score").rlike("^[1-2][1-2][4-5]$"), "At Risk")
                   .otherwise("Others"))
    )

    return customer_segmentation


def _create_time_series_analysis(df: DataFrame) -> DataFrame:
    """Create time series analysis."""
    logger.info("Creating time series analysis...")

    daily_metrics = (
        df.groupBy("invoice_date", "country")
        .agg(
            F.sum("total_amount").alias("daily_revenue"),
            F.count("invoice_no").alias("daily_transactions"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("total_quantity")
        )
    )

    # Add moving averages using window functions
    days_window = Window.partitionBy("country").orderBy("invoice_date").rowsBetween(-6, 0)

    time_series = (
        daily_metrics
        .withColumn("revenue_7day_avg", F.avg("daily_revenue").over(days_window))
        .withColumn("transactions_7day_avg", F.avg("daily_transactions").over(days_window))
        .withColumn("day_of_week", F.date_format("invoice_date", "EEEE"))
        .withColumn("is_weekend", F.when(F.date_format("invoice_date", "u").isin("6", "7"), True).otherwise(False))
    )

    return time_series


def _create_cohort_analysis(df: DataFrame) -> DataFrame:
    """Create customer cohort analysis."""
    logger.info("Creating cohort analysis...")

    # Define customer's first purchase month as cohort
    customer_cohorts = (
        df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(F.min("invoice_timestamp").alias("first_purchase_date"))
        .withColumn("cohort_month", F.date_format("first_purchase_date", "yyyy-MM"))
    )

    # Join back with main data
    df_with_cohorts = df.join(customer_cohorts, "customer_id", "inner")

    # Calculate period number (months since first purchase)
    cohort_analysis = (
        df_with_cohorts
        .withColumn("invoice_month", F.date_format("invoice_timestamp", "yyyy-MM"))
        .withColumn("period_number",
                   F.months_between(F.col("invoice_timestamp"), F.col("first_purchase_date")))
        .groupBy("cohort_month", "period_number")
        .agg(
            F.countDistinct("customer_id").alias("customers"),
            F.sum("total_amount").alias("revenue"),
            F.count("invoice_no").alias("orders")
        )
    )

    return cohort_analysis


def _save_gold_tables(spark: SparkSession, tables: dict[str, DataFrame]) -> None:
    """Save all Gold layer tables."""
    gold_path = settings.gold_path
    gold_path.mkdir(parents=True, exist_ok=True)

    for table_name, df in tables.items():
        output_path = gold_path / table_name

        logger.info(f"Saving {table_name} with {df.count()} records...")

        # Write as partitioned Parquet files
        if table_name == "sales_summary":
            df.write.mode("overwrite").partitionBy("country").parquet(str(output_path))
        elif table_name == "product_analysis":
            df.write.mode("overwrite").partitionBy("country").parquet(str(output_path))
        elif table_name == "time_series_analysis":
            df.write.mode("overwrite").partitionBy("country").parquet(str(output_path))
        else:
            df.write.mode("overwrite").parquet(str(output_path))

        logger.info(f"Successfully saved {table_name} to {output_path}")


def _generate_gold_metrics(tables: dict[str, DataFrame]) -> dict[str, Any]:
    """Generate Gold layer metrics."""
    metrics = {
        "processing_timestamp": F.current_timestamp(),
        "tables_created": list(tables.keys()),
        "table_counts": {}
    }

    for table_name, df in tables.items():
        count = df.count()
        metrics["table_counts"][table_name] = count
        logger.info(f"Gold table {table_name}: {count} records")

    return metrics


def process_gold_layer() -> bool:
    """Main function to process Gold layer analytics."""
    spark = None
    try:
        logger.info("Starting Gold layer processing with Spark...")

        # Create Spark session
        spark = _create_spark_session()

        # Read Silver data
        df = _read_silver_data(spark)

        # Create analytics tables
        tables = {
            "sales_summary": _create_sales_summary(df),
            "product_analysis": _create_product_analysis(df),
            "customer_segmentation": _create_customer_segmentation(df),
            "time_series_analysis": _create_time_series_analysis(df),
            "cohort_analysis": _create_cohort_analysis(df)
        }

        # Save all tables
        _save_gold_tables(spark, tables)

        # Generate and save metrics
        metrics = _generate_gold_metrics(tables)
        metrics_file = settings.gold_path / "processing_metrics.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)

        logger.info("Gold layer processing completed successfully")
        return True

    except Exception as e:
        logger.error(f"Gold layer processing failed: {e}")
        return False
    finally:
        if spark:
            spark.stop()


def main() -> None:
    """Entry point for Gold layer processing."""
    logger.info("Starting Spark-based Gold layer ETL...")

    success = process_gold_layer()
    if success:
        print("Gold layer processing completed successfully (Spark-based)")
    else:
        print("Gold layer processing failed")
        exit(1)


if __name__ == "__main__":
    main()
