"""
Spark Delta Lake ETL Processor
Comprehensive ETL pipeline using Spark DataFrames and Delta Lake for all layers
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    date_format,
    dayofmonth,
    expr,
    hash,
    lit,
    monotonically_increasing_id,
    month,
    regexp_replace,
    to_timestamp,
    trim,
    upper,
    when,
    year,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager


class DeltaETLProcessor:
    """
    Comprehensive ETL processor using Spark DataFrames and Delta Lake
    Implements Bronze -> Silver -> Gold pipeline with data quality and optimization
    """

    def __init__(self, spark: SparkSession | None = None):
        self.config = get_unified_config()
        self.logger = get_logger(__name__)

        # Initialize Delta Lake manager
        self.delta_manager = DeltaLakeManager(spark)
        self.spark = self.delta_manager.spark

        # Data paths
        self.raw_data_path = Path(self.config.base.data_path) / "raw"
        self.processed_data_path = Path(self.config.base.data_path) / "processed"

        self.logger.info("DeltaETLProcessor initialized")

    def process_complete_pipeline(
        self,
        input_file: str | None = None,
        enable_optimizations: bool = True,
        enable_data_quality: bool = True
    ) -> dict[str, Any]:
        """
        Execute complete Bronze -> Silver -> Gold pipeline
        
        Args:
            input_file: Optional input file path
            enable_optimizations: Whether to apply optimizations
            enable_data_quality: Whether to run data quality checks
            
        Returns:
            Dictionary with pipeline execution results
        """
        try:
            pipeline_start = datetime.now()
            self.logger.info("Starting complete Delta Lake ETL pipeline")

            results = {
                "pipeline_id": f"delta_etl_{int(pipeline_start.timestamp())}",
                "start_time": pipeline_start.isoformat(),
                "layers_processed": []
            }

            # 1. Bronze Layer Processing
            self.logger.info("Processing Bronze layer...")
            bronze_result = self.process_bronze_layer(input_file, enable_data_quality)
            results["layers_processed"].append(bronze_result)

            # 2. Silver Layer Processing
            self.logger.info("Processing Silver layer...")
            silver_result = self.process_silver_layer(enable_data_quality)
            results["layers_processed"].append(silver_result)

            # 3. Gold Layer Processing
            self.logger.info("Processing Gold layer...")
            gold_result = self.process_gold_layer(enable_optimizations)
            results["layers_processed"].append(gold_result)

            # Pipeline summary
            pipeline_end = datetime.now()
            total_time = (pipeline_end - pipeline_start).total_seconds()

            results.update({
                "end_time": pipeline_end.isoformat(),
                "total_processing_time_seconds": total_time,
                "status": "success",
                "total_records_processed": sum(layer.get("record_count", 0) for layer in results["layers_processed"]),
                "optimizations_enabled": enable_optimizations,
                "data_quality_enabled": enable_data_quality
            })

            self.logger.info(f"Complete pipeline finished in {total_time:.2f} seconds")
            return results

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            raise

    def process_bronze_layer(
        self,
        input_file: str | None = None,
        enable_data_quality: bool = True
    ) -> dict[str, Any]:
        """
        Process Bronze layer with raw data ingestion and basic validation
        
        Args:
            input_file: Input CSV file path
            enable_data_quality: Whether to run data quality assessment
            
        Returns:
            Dictionary with Bronze processing results
        """
        try:
            start_time = datetime.now()

            # Determine input file
            if not input_file:
                input_file = self._find_latest_input_file()

            if not Path(input_file).exists():
                raise FileNotFoundError(f"Input file not found: {input_file}")

            # Read raw data with Spark
            self.logger.info(f"Reading raw data from: {input_file}")
            raw_df = self._read_raw_data(input_file)

            # Basic data cleansing
            bronze_df = self._apply_bronze_transformations(raw_df)

            # Data quality assessment
            quality_metrics = {}
            if enable_data_quality:
                quality_metrics = self._assess_data_quality(bronze_df, "bronze")

            # Write to Bronze Delta Lake
            write_result = self.delta_manager.write_bronze_layer(
                bronze_df,
                "retail_transactions",
                partition_cols=["__bronze_ingestion_date"],
                mode="overwrite"
            )

            processing_time = (datetime.now() - start_time).total_seconds()

            result = {
                "layer": "bronze",
                "status": "success",
                "input_file": str(input_file),
                "record_count": write_result["record_count"],
                "processing_time_seconds": processing_time,
                "quality_metrics": quality_metrics,
                "table_path": write_result["table_path"]
            }

            self.logger.info(f"Bronze layer processed: {write_result['record_count']:,} records")
            return result

        except Exception as e:
            self.logger.error(f"Bronze layer processing failed: {str(e)}")
            raise

    def process_silver_layer(self, enable_data_quality: bool = True) -> dict[str, Any]:
        """
        Process Silver layer with business rules and data quality validation
        
        Args:
            enable_data_quality: Whether to run data quality assessment
            
        Returns:
            Dictionary with Silver processing results
        """
        try:
            start_time = datetime.now()

            # Read from Bronze layer
            bronze_df = self.delta_manager.read_delta_table("bronze", "retail_transactions")

            # Apply Silver transformations
            silver_df = self._apply_silver_transformations(bronze_df)

            # Business rules validation
            silver_df = self._apply_business_rules(silver_df)

            # Data quality assessment
            quality_metrics = {}
            if enable_data_quality:
                quality_metrics = self._assess_data_quality(silver_df, "silver")

            # Write to Silver Delta Lake with SCD2
            write_result = self.delta_manager.write_silver_layer(
                silver_df,
                "retail_transactions_clean",
                business_key="transaction_id",
                partition_cols=["country", "year_month"],
                enable_scd2=True
            )

            processing_time = (datetime.now() - start_time).total_seconds()

            result = {
                "layer": "silver",
                "status": "success",
                "record_count": write_result["record_count"],
                "processing_time_seconds": processing_time,
                "quality_metrics": quality_metrics,
                "scd2_enabled": True,
                "business_rules_applied": True
            }

            self.logger.info(f"Silver layer processed: {write_result['record_count']:,} records")
            return result

        except Exception as e:
            self.logger.error(f"Silver layer processing failed: {str(e)}")
            raise

    def process_gold_layer(self, enable_optimizations: bool = True) -> dict[str, Any]:
        """
        Process Gold layer with star schema and advanced analytics
        
        Args:
            enable_optimizations: Whether to apply table optimizations
            
        Returns:
            Dictionary with Gold processing results
        """
        try:
            start_time = datetime.now()

            # Read from Silver layer
            silver_df = self.delta_manager.read_delta_table("silver", "retail_transactions_clean")

            # Create dimensional tables
            dim_results = self._create_dimensional_tables(silver_df)

            # Create fact table
            fact_result = self._create_fact_table(silver_df)

            # Create analytics aggregations
            analytics_results = self._create_analytics_aggregations(silver_df)

            processing_time = (datetime.now() - start_time).total_seconds()

            total_records = (
                dim_results.get("total_dim_records", 0) +
                fact_result.get("record_count", 0) +
                analytics_results.get("total_analytics_records", 0)
            )

            result = {
                "layer": "gold",
                "status": "success",
                "record_count": total_records,
                "processing_time_seconds": processing_time,
                "dimensional_tables": dim_results,
                "fact_table": fact_result,
                "analytics_aggregations": analytics_results,
                "optimizations_applied": enable_optimizations
            }

            self.logger.info(f"Gold layer processed: {total_records:,} total records")
            return result

        except Exception as e:
            self.logger.error(f"Gold layer processing failed: {str(e)}")
            raise

    def _find_latest_input_file(self) -> str:
        """Find the latest raw data file"""
        raw_files = list(self.raw_data_path.glob("*.csv"))
        if not raw_files:
            raise FileNotFoundError("No raw CSV files found in data/raw directory")

        # Return the most recent file
        latest_file = max(raw_files, key=os.path.getmtime)
        return str(latest_file)

    def _read_raw_data(self, input_file: str) -> DataFrame:
        """Read raw CSV data with proper schema inference"""

        # Define expected schema for retail data
        schema = StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", StringType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", StringType(), True),
            StructField("Country", StringType(), True)
        ])

        # Read CSV with proper options
        df = (self.spark.read
              .option("header", "true")
              .option("inferSchema", "false")
              .option("multiline", "true")
              .option("escape", "\"")
              .schema(schema)
              .csv(input_file))

        self.logger.info(f"Raw data loaded: {df.count():,} records")
        return df

    def _apply_bronze_transformations(self, df: DataFrame) -> DataFrame:
        """Apply Bronze layer transformations"""

        # Add ingestion metadata
        transformed_df = (df
            .withColumn("__bronze_ingestion_date", date_format(current_timestamp(), "yyyy-MM-dd"))
            .withColumn("__bronze_ingestion_timestamp", current_timestamp())
            .withColumn("__bronze_file_source", lit("csv_import"))

            # Basic cleaning
            .withColumn("InvoiceNo", trim(col("InvoiceNo")))
            .withColumn("StockCode", trim(upper(col("StockCode"))))
            .withColumn("Description", trim(col("Description")))
            .withColumn("Country", trim(col("Country")))
            .withColumn("CustomerID", trim(col("CustomerID")))

            # Convert InvoiceDate to timestamp
            .withColumn("InvoiceTimestamp",
                       to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))

            # Create calculated fields
            .withColumn("TotalAmount",
                       when(col("Quantity").isNotNull() & col("UnitPrice").isNotNull(),
                            col("Quantity") * col("UnitPrice")).otherwise(0.0))

            # Add row ID
            .withColumn("__bronze_row_id", monotonically_increasing_id())
        )

        # Filter out completely null rows
        transformed_df = transformed_df.filter(
            col("InvoiceNo").isNotNull() |
            col("StockCode").isNotNull() |
            col("Description").isNotNull()
        )

        self.logger.info(f"Bronze transformations applied: {transformed_df.count():,} records")
        return transformed_df

    def _apply_silver_transformations(self, bronze_df: DataFrame) -> DataFrame:
        """Apply Silver layer transformations with data cleaning and standardization"""

        silver_df = (bronze_df
            # Create primary key
            .withColumn("transaction_id",
                       concat_ws("_", col("InvoiceNo"), col("StockCode"), col("__bronze_row_id")))

            # Enhanced cleaning
            .withColumn("invoice_no", regexp_replace(col("InvoiceNo"), "[^A-Z0-9]", ""))
            .withColumn("stock_code", upper(trim(col("StockCode"))))
            .withColumn("description",
                       regexp_replace(trim(col("Description")), "\\s+", " "))
            .withColumn("customer_id",
                       when(col("CustomerID").isNotNull() & (col("CustomerID") != ""),
                            col("CustomerID")).otherwise(lit("UNKNOWN")))
            .withColumn("country", trim(upper(col("Country"))))

            # Data type conversions and validations
            .withColumn("quantity",
                       when(col("Quantity").isNull() | (col("Quantity") < 0), 0)
                       .otherwise(col("Quantity")))
            .withColumn("unit_price",
                       when(col("UnitPrice").isNull() | (col("UnitPrice") < 0), 0.0)
                       .otherwise(col("UnitPrice")))
            .withColumn("total_amount",
                       col("quantity") * col("unit_price"))

            # Date standardization
            .withColumn("invoice_timestamp", col("InvoiceTimestamp"))
            .withColumn("invoice_date", col("InvoiceTimestamp").cast(DateType()))
            .withColumn("year_month", date_format(col("InvoiceTimestamp"), "yyyy-MM"))

            # Business categorization
            .withColumn("transaction_type",
                       when(col("quantity") > 0, "SALE")
                       .when(col("quantity") < 0, "RETURN")
                       .otherwise("OTHER"))

            .withColumn("customer_type",
                       when(col("customer_id") == "UNKNOWN", "GUEST")
                       .otherwise("REGISTERED"))

            # Data quality flags
            .withColumn("is_valid_transaction",
                       (col("invoice_no").isNotNull()) &
                       (col("stock_code").isNotNull()) &
                       (col("quantity") != 0) &
                       (col("unit_price") >= 0) &
                       (col("invoice_timestamp").isNotNull()))

            # Select final columns
            .select(
                "transaction_id",
                "invoice_no",
                "stock_code",
                "description",
                "quantity",
                "unit_price",
                "total_amount",
                "customer_id",
                "customer_type",
                "country",
                "invoice_timestamp",
                "invoice_date",
                "year_month",
                "transaction_type",
                "is_valid_transaction",
                "__bronze_ingestion_timestamp"
            )
        )

        self.logger.info(f"Silver transformations applied: {silver_df.count():,} records")
        return silver_df

    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules and validation"""

        # Business rule: Filter out invalid transactions
        validated_df = df.filter(col("is_valid_transaction") == True)

        # Business rule: Reasonable price range (0.01 to 10000)
        validated_df = validated_df.filter(
            (col("unit_price") >= 0.01) & (col("unit_price") <= 10000)
        )

        # Business rule: Reasonable quantity range (-1000 to 10000)
        validated_df = validated_df.filter(
            (col("quantity") >= -1000) & (col("quantity") <= 10000)
        )

        # Business rule: Valid invoice date (last 10 years)
        current_year = datetime.now().year
        validated_df = validated_df.filter(
            year(col("invoice_timestamp")) >= (current_year - 10)
        )

        self.logger.info(f"Business rules applied: {validated_df.count():,} records")
        return validated_df

    def _create_dimensional_tables(self, silver_df: DataFrame) -> dict[str, Any]:
        """Create dimensional tables for star schema"""

        results = {}

        # 1. Customer Dimension with SCD2
        customer_dim = self._create_customer_dimension(silver_df)
        customer_result = self.delta_manager.write_gold_layer(
            customer_dim, "dim_customer",
            partition_cols=["customer_type"],
            optimize_for_queries=True
        )
        results["dim_customer"] = customer_result

        # 2. Product Dimension
        product_dim = self._create_product_dimension(silver_df)
        product_result = self.delta_manager.write_gold_layer(
            product_dim, "dim_product",
            optimize_for_queries=True
        )
        results["dim_product"] = product_result

        # 3. Date Dimension
        date_dim = self._create_date_dimension(silver_df)
        date_result = self.delta_manager.write_gold_layer(
            date_dim, "dim_date",
            optimize_for_queries=True
        )
        results["dim_date"] = date_result

        # 4. Country Dimension
        country_dim = self._create_country_dimension(silver_df)
        country_result = self.delta_manager.write_gold_layer(
            country_dim, "dim_country",
            optimize_for_queries=True
        )
        results["dim_country"] = country_result

        total_records = sum(result["record_count"] for result in results.values())
        results["total_dim_records"] = total_records

        self.logger.info(f"Created {len(results)-1} dimensional tables with {total_records:,} total records")
        return results

    def _create_customer_dimension(self, df: DataFrame) -> DataFrame:
        """Create customer dimension table"""

        customer_stats = (df
            .groupBy("customer_id", "customer_type")
            .agg(
                countDistinct("invoice_no").alias("total_orders"),
                spark_sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_order_value"),
                spark_min("invoice_timestamp").alias("first_purchase_date"),
                spark_max("invoice_timestamp").alias("last_purchase_date"),
                count("transaction_id").alias("total_transactions")
            )
        )

        # Add customer segmentation
        customer_dim = (customer_stats
            .withColumn("customer_key", monotonically_increasing_id())
            .withColumn("customer_segment",
                when(col("total_spent") >= 1000, "HIGH_VALUE")
                .when(col("total_spent") >= 500, "MEDIUM_VALUE")
                .otherwise("LOW_VALUE"))
            .withColumn("is_active_customer",
                col("last_purchase_date") >= expr("current_date() - interval 365 days"))
            .select(
                "customer_key",
                "customer_id",
                "customer_type",
                "customer_segment",
                "total_orders",
                "total_spent",
                "avg_order_value",
                "first_purchase_date",
                "last_purchase_date",
                "total_transactions",
                "is_active_customer"
            )
        )

        return customer_dim

    def _create_product_dimension(self, df: DataFrame) -> DataFrame:
        """Create product dimension table"""

        product_stats = (df
            .groupBy("stock_code", "description")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("unit_price").alias("avg_unit_price"),
                spark_min("unit_price").alias("min_unit_price"),
                spark_max("unit_price").alias("max_unit_price"),
                countDistinct("customer_id").alias("unique_customers")
            )
        )

        product_dim = (product_stats
            .withColumn("product_key", monotonically_increasing_id())
            .withColumn("product_category",
                when(col("description").contains("BAG"), "BAGS")
                .when(col("description").contains("CANDLE"), "CANDLES")
                .when(col("description").contains("CARD"), "CARDS")
                .when(col("description").contains("LIGHT"), "LIGHTING")
                .otherwise("OTHER"))
            .withColumn("price_tier",
                when(col("avg_unit_price") >= 10, "PREMIUM")
                .when(col("avg_unit_price") >= 5, "STANDARD")
                .otherwise("BUDGET"))
            .select(
                "product_key",
                "stock_code",
                "description",
                "product_category",
                "price_tier",
                "total_transactions",
                "total_quantity_sold",
                "total_revenue",
                "avg_unit_price",
                "min_unit_price",
                "max_unit_price",
                "unique_customers"
            )
        )

        return product_dim

    def _create_date_dimension(self, df: DataFrame) -> DataFrame:
        """Create date dimension table"""

        # Get unique dates from transactions
        dates = (df
            .select(col("invoice_date").alias("date"))
            .distinct()
            .filter(col("date").isNotNull())
        )

        date_dim = (dates
            .withColumn("date_key", monotonically_increasing_id())
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("day", dayofmonth(col("date")))
            .withColumn("quarter", expr("quarter(date)"))
            .withColumn("day_of_week", expr("dayofweek(date)"))
            .withColumn("day_name", date_format(col("date"), "EEEE"))
            .withColumn("month_name", date_format(col("date"), "MMMM"))
            .withColumn("is_weekend",
                expr("dayofweek(date) IN (1, 7)"))
            .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
            .select(
                "date_key",
                "date",
                "year",
                "quarter",
                "month",
                "month_name",
                "day",
                "day_of_week",
                "day_name",
                "is_weekend",
                "year_month"
            )
        )

        return date_dim

    def _create_country_dimension(self, df: DataFrame) -> DataFrame:
        """Create country dimension table"""

        country_stats = (df
            .groupBy("country")
            .agg(
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("stock_code").alias("unique_products"),
                count("transaction_id").alias("total_transactions"),
                spark_sum("total_amount").alias("total_revenue")
            )
        )

        country_dim = (country_stats
            .withColumn("country_key", monotonically_increasing_id())
            .withColumn("region",
                when(col("country").isin("United Kingdom", "Ireland"), "UK_IRELAND")
                .when(col("country").isin("Germany", "France", "Netherlands", "Belgium"), "WESTERN_EUROPE")
                .when(col("country").isin("Spain", "Italy", "Portugal"), "SOUTHERN_EUROPE")
                .when(col("country").isin("Norway", "Sweden", "Denmark", "Finland"), "NORDIC")
                .otherwise("OTHER"))
            .withColumn("market_size",
                when(col("total_revenue") >= 100000, "LARGE")
                .when(col("total_revenue") >= 10000, "MEDIUM")
                .otherwise("SMALL"))
            .select(
                "country_key",
                "country",
                "region",
                "market_size",
                "unique_customers",
                "unique_products",
                "total_transactions",
                "total_revenue"
            )
        )

        return country_dim

    def _create_fact_table(self, silver_df: DataFrame) -> dict[str, Any]:
        """Create fact table with foreign keys"""

        # This would typically involve joining with dimension tables to get keys
        # For simplicity, using generated keys based on values

        fact_sales = (silver_df
            .withColumn("fact_key", monotonically_increasing_id())
            .withColumn("customer_key", hash(col("customer_id")))
            .withColumn("product_key", hash(col("stock_code")))
            .withColumn("date_key", hash(col("invoice_date")))
            .withColumn("country_key", hash(col("country")))
            .select(
                "fact_key",
                "transaction_id",
                "customer_key",
                "product_key",
                "date_key",
                "country_key",
                "invoice_no",
                "quantity",
                "unit_price",
                "total_amount",
                "transaction_type",
                "invoice_timestamp"
            )
        )

        fact_result = self.delta_manager.write_gold_layer(
            fact_sales, "fact_sales",
            partition_cols=["transaction_type"],
            optimize_for_queries=True
        )

        return fact_result

    def _create_analytics_aggregations(self, silver_df: DataFrame) -> dict[str, Any]:
        """Create analytics aggregation tables"""

        results = {}

        # 1. Daily sales summary
        daily_sales = (silver_df
            .groupBy("invoice_date", "country")
            .agg(
                count("transaction_id").alias("transaction_count"),
                spark_sum("total_amount").alias("total_revenue"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("stock_code").alias("unique_products"),
                avg("total_amount").alias("avg_transaction_value")
            )
            .withColumn("summary_key", monotonically_increasing_id())
        )

        daily_result = self.delta_manager.write_gold_layer(
            daily_sales, "analytics_daily_sales",
            partition_cols=["country"],
            optimize_for_queries=True
        )
        results["daily_sales"] = daily_result

        # 2. Customer analytics
        customer_analytics = (silver_df
            .groupBy("customer_id", "customer_type", "country")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("total_amount").alias("customer_ltv"),
                avg("total_amount").alias("avg_order_value"),
                countDistinct("stock_code").alias("unique_products_purchased"),
                spark_min("invoice_timestamp").alias("first_purchase"),
                spark_max("invoice_timestamp").alias("last_purchase")
            )
            .withColumn("analytics_key", monotonically_increasing_id())
        )

        customer_result = self.delta_manager.write_gold_layer(
            customer_analytics, "analytics_customer_summary",
            partition_cols=["customer_type"],
            optimize_for_queries=True
        )
        results["customer_analytics"] = customer_result

        total_records = sum(result["record_count"] for result in results.values())
        results["total_analytics_records"] = total_records

        return results

    def _assess_data_quality(self, df: DataFrame, layer: str) -> dict[str, Any]:
        """Assess data quality metrics"""

        total_records = df.count()
        if total_records == 0:
            return {"error": "No records to assess"}

        quality_metrics = {
            "layer": layer,
            "total_records": total_records,
            "assessment_timestamp": datetime.now().isoformat()
        }

        # Column-level quality assessment
        column_metrics = {}
        for column in df.columns:
            if not column.startswith("__"):
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / total_records) * 100

                column_metrics[column] = {
                    "null_count": null_count,
                    "null_percentage": round(null_percentage, 2),
                    "non_null_count": total_records - null_count
                }

        quality_metrics["column_quality"] = column_metrics

        # Overall quality score (0-100)
        avg_null_percentage = sum(
            metric["null_percentage"] for metric in column_metrics.values()
        ) / len(column_metrics)

        quality_score = max(0, 100 - avg_null_percentage)
        quality_metrics["overall_quality_score"] = round(quality_score, 2)

        return quality_metrics

    def get_pipeline_statistics(self) -> dict[str, Any]:
        """Get comprehensive pipeline statistics"""

        stats = {
            "pipeline_type": "Delta Lake Spark ETL",
            "timestamp": datetime.now().isoformat(),
            "layers": {}
        }

        # Get statistics for each layer
        for layer in ["bronze", "silver", "gold"]:
            layer_stats = {}

            # List tables in layer
            layer_path = getattr(self.delta_manager, f"{layer}_path")
            tables = [d.name for d in layer_path.iterdir() if d.is_dir() and not d.name.startswith(".")]

            layer_stats["table_count"] = len(tables)
            layer_stats["tables"] = []

            for table in tables:
                try:
                    table_metrics = self.delta_manager.get_table_metrics(layer, table)
                    layer_stats["tables"].append(table_metrics)
                except Exception as e:
                    self.logger.warning(f"Could not get metrics for {layer}.{table}: {str(e)}")

            stats["layers"][layer] = layer_stats

        return stats

    def close(self):
        """Close resources"""
        if self.delta_manager:
            self.delta_manager.close()


# Factory function
def create_delta_etl_processor(spark: SparkSession | None = None) -> DeltaETLProcessor:
    """Create DeltaETLProcessor instance"""
    return DeltaETLProcessor(spark)


# Example usage and testing
if __name__ == "__main__":
    processor = create_delta_etl_processor()

    try:
        print("Testing Delta Lake ETL Processor...")

        # Run complete pipeline
        results = processor.process_complete_pipeline()

        print("Pipeline Results:")
        print(json.dumps(results, indent=2, default=str))

        # Get statistics
        stats = processor.get_pipeline_statistics()
        print("\nPipeline Statistics:")
        print(json.dumps(stats, indent=2, default=str))

        print("✅ Delta Lake ETL Processor testing completed successfully!")

    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        processor.close()
