"""
Enhanced Spark-based Bronze Layer with Advanced Features
Provides enterprise-grade data ingestion with comprehensive monitoring
"""
from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from core.config.base_config import BaseConfig
from core.logging import get_logger
from etl.spark.data_quality import DataQualityChecker, QualityReport
from etl.spark.schema_evolution import SchemaEvolutionManager
from etl.spark.session_manager import create_optimized_session

logger = get_logger(__name__)


class EnhancedBronzeProcessor:
    """Advanced Bronze layer processor with enterprise features."""

    def __init__(self, config: BaseConfig | None = None):
        self.config = config or BaseConfig()
        self.spark: SparkSession | None = None
        self.quality_checker = DataQualityChecker()
        self.schema_manager = SchemaEvolutionManager()

    def process(self,
                input_path: Path | None = None,
                output_path: Path | None = None,
                enable_schema_evolution: bool = True,
                enable_data_profiling: bool = True) -> dict[str, Any]:
        """
        Process Bronze layer with advanced features.
        
        Args:
            input_path: Input data path (defaults to config.raw_data_path)
            output_path: Output path (defaults to config.bronze_path)
            enable_schema_evolution: Enable automatic schema evolution
            enable_data_profiling: Enable comprehensive data profiling
            
        Returns:
            Processing results and metrics
        """
        try:
            # Initialize Spark session
            self.spark = create_optimized_session(
                app_name="Enhanced-Bronze-Layer",
                processing_type="batch",
                data_size="medium"
            )

            input_path = input_path or self.config.raw_data_path
            output_path = output_path or (self.config.bronze_path / "sales")

            logger.info("Starting enhanced Bronze layer processing...")
            logger.info(f"Input path: {input_path}")
            logger.info(f"Output path: {output_path}")

            # Discover and validate input files
            input_files = self._discover_input_files(input_path)
            if not input_files:
                raise FileNotFoundError(f"No input files found in {input_path}")

            # Read and process data
            raw_df = self._read_raw_data(input_files)
            logger.info(f"Read {raw_df.count()} raw records")

            # Schema evolution (if enabled)
            if enable_schema_evolution:
                raw_df = self._handle_schema_evolution(raw_df, output_path)

            # Data standardization and enrichment
            processed_df = self._standardize_data(raw_df)
            processed_df = self._enrich_metadata(processed_df, input_files)

            # Data quality assessment
            quality_report = None
            if enable_data_profiling:
                quality_report = self._assess_data_quality(processed_df)
                self._log_quality_metrics(quality_report)

            # Partitioning strategy
            partitioned_df = self._apply_partitioning_strategy(processed_df)

            # Write to Bronze layer
            write_stats = self._write_bronze_data(partitioned_df, output_path)

            # Generate processing report
            processing_report = self._generate_processing_report(
                input_files=input_files,
                input_count=raw_df.count(),
                output_count=partitioned_df.count(),
                output_path=output_path,
                quality_report=quality_report,
                write_stats=write_stats
            )

            logger.info("Enhanced Bronze layer processing completed successfully")
            return processing_report

        except Exception as e:
            logger.error(f"Enhanced Bronze layer processing failed: {e}")
            raise
        finally:
            if self.spark:
                # Don't stop the session - let the session manager handle it
                pass

    def _discover_input_files(self, input_path: Path) -> list[dict[str, Any]]:
        """Discover and catalog input files with metadata."""
        file_patterns = ["*.csv", "*.json", "*.parquet", "*.xlsx"]
        discovered_files = []

        for pattern in file_patterns:
            for file_path in input_path.rglob(pattern):
                file_info = {
                    "path": str(file_path.absolute()),
                    "name": file_path.name,
                    "extension": file_path.suffix.lower(),
                    "size_bytes": file_path.stat().st_size,
                    "modified_time": datetime.fromtimestamp(file_path.stat().st_mtime),
                    "relative_path": str(file_path.relative_to(input_path))
                }
                discovered_files.append(file_info)

        logger.info(f"Discovered {len(discovered_files)} files: {[f['name'] for f in discovered_files]}")
        return discovered_files

    def _read_raw_data(self, input_files: list[dict[str, Any]]) -> DataFrame:
        """Read raw data from multiple file formats with error handling."""
        dfs = []
        job_id = str(uuid.uuid4())

        for file_info in input_files:
            file_path = file_info["path"]
            file_ext = file_info["extension"]

            try:
                logger.info(f"Reading file: {file_info['name']} ({file_info['size_bytes']} bytes)")

                # Read based on file type
                if file_ext == ".csv":
                    df = self._read_csv_file(file_path)
                elif file_ext == ".json":
                    df = self._read_json_file(file_path)
                elif file_ext == ".parquet":
                    df = self._read_parquet_file(file_path)
                elif file_ext == ".xlsx":
                    df = self._read_excel_file(file_path)
                else:
                    logger.warning(f"Unsupported file type: {file_ext}")
                    continue

                # Add source metadata
                df = df.withColumn("_source_file", F.lit(file_path))
                df = df.withColumn("_source_type", F.lit(file_ext[1:]))  # Remove dot
                df = df.withColumn("_ingestion_job_id", F.lit(job_id))
                df = df.withColumn("_file_size_bytes", F.lit(file_info["size_bytes"]))
                df = df.withColumn("_file_modified_time", F.lit(file_info["modified_time"]))

                dfs.append(df)
                logger.info(f"Successfully read {df.count()} records from {file_info['name']}")

            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                continue

        if not dfs:
            raise ValueError("No files could be read successfully")

        # Union all DataFrames with schema merge
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        logger.info(f"Combined {combined_df.count()} total records from {len(dfs)} files")
        return combined_df

    def _read_csv_file(self, file_path: str) -> DataFrame:
        """Read CSV file with advanced options."""
        return (self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "M/d/yyyy H:mm")
                .option("dateFormat", "M/d/yyyy")
                .option("multiLine", "true")
                .option("escape", '"')
                .option("ignoreLeadingWhiteSpace", "true")
                .option("ignoreTrailingWhiteSpace", "true")
                .csv(file_path))

    def _read_json_file(self, file_path: str) -> DataFrame:
        """Read JSON file with error handling."""
        return (self.spark.read
                .option("multiLine", "true")
                .option("allowComments", "true")
                .option("allowUnquotedFieldNames", "true")
                .json(file_path))

    def _read_parquet_file(self, file_path: str) -> DataFrame:
        """Read Parquet file."""
        return self.spark.read.parquet(file_path)

    def _read_excel_file(self, file_path: str) -> DataFrame:
        """Read Excel file (requires pandas conversion)."""
        try:
            import pandas as pd
            pandas_df = pd.read_excel(file_path)
            return self.spark.createDataFrame(pandas_df)
        except ImportError:
            logger.error("pandas required for Excel file reading")
            raise

    def _handle_schema_evolution(self, df: DataFrame, output_path: Path) -> DataFrame:
        """Handle schema evolution and compatibility."""
        try:
            # Try to read existing schema
            existing_schema = self.schema_manager.get_latest_schema(output_path)
            if existing_schema:
                logger.info("Applying schema evolution...")
                df = self.schema_manager.evolve_schema(df, existing_schema)

            # Save current schema
            self.schema_manager.save_schema(df.schema, output_path)

        except Exception as e:
            logger.warning(f"Schema evolution failed: {e}")

        return df

    def _standardize_data(self, df: DataFrame) -> DataFrame:
        """Apply data standardization and normalization."""
        logger.info("Applying data standardization...")

        # Normalize column names
        df = self._normalize_column_names(df)

        # Standardize data types
        df = self._standardize_data_types(df)

        # Apply business rules
        df = self._apply_business_rules(df)

        return df

    def _normalize_column_names(self, df: DataFrame) -> DataFrame:
        """Normalize column names to standard format."""
        # Column mapping for common variations
        column_mapping = {
            "InvoiceNo": "invoice_no",
            "invoiceno": "invoice_no",
            "invoice_number": "invoice_no",
            "StockCode": "stock_code",
            "stockcode": "stock_code",
            "product_code": "stock_code",
            "Description": "description",
            "product_name": "description",
            "Quantity": "quantity",
            "qty": "quantity",
            "InvoiceDate": "invoice_timestamp",
            "invoicedate": "invoice_timestamp",
            "date": "invoice_timestamp",
            "UnitPrice": "unit_price",
            "unitprice": "unit_price",
            "price": "unit_price",
            "CustomerID": "customer_id",
            "customerid": "customer_id",
            "customer": "customer_id",
            "Country": "country"
        }

        # Apply mappings
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        # Convert all remaining columns to lowercase with underscores
        for col_name in df.columns:
            if col_name not in column_mapping.values() and not col_name.startswith("_"):
                new_name = col_name.lower().replace(" ", "_").replace("-", "_")
                if new_name != col_name:
                    df = df.withColumnRenamed(col_name, new_name)

        return df

    def _standardize_data_types(self, df: DataFrame) -> DataFrame:
        """Standardize data types with error handling."""
        # Define expected data types
        type_mappings = {
            "invoice_no": StringType(),
            "stock_code": StringType(),
            "description": StringType(),
            "quantity": IntegerType(),
            "unit_price": DoubleType(),
            "customer_id": StringType(),
            "country": StringType(),
        }

        for col_name, target_type in type_mappings.items():
            if col_name in df.columns:
                if isinstance(target_type, (IntegerType, DoubleType)):
                    # Numeric columns with error handling
                    df = df.withColumn(
                        col_name,
                        F.when(F.col(col_name).rlike(r"^\d*\.?\d*$"), F.col(col_name).cast(target_type))
                        .otherwise(None)
                    )
                else:
                    # String columns
                    df = df.withColumn(col_name, F.col(col_name).cast(target_type))

        # Handle timestamp columns
        if "invoice_timestamp" in df.columns:
            df = df.withColumn(
                "invoice_timestamp",
                F.coalesce(
                    F.to_timestamp(F.col("invoice_timestamp"), "M/d/yyyy H:mm"),
                    F.to_timestamp(F.col("invoice_timestamp"), "yyyy-MM-dd HH:mm:ss"),
                    F.to_timestamp(F.col("invoice_timestamp"), "yyyy-MM-dd"),
                    F.to_timestamp(F.col("invoice_timestamp"))
                )
            )

        return df

    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules and create derived columns."""
        # Add data quality flags
        df = df.withColumn("_is_valid_quantity",
                          F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), False).otherwise(True))
        df = df.withColumn("_is_valid_price",
                          F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), False).otherwise(True))
        df = df.withColumn("_has_invoice_no",
                          F.when(F.col("invoice_no").isNull() | (F.trim(F.col("invoice_no")) == ""), False).otherwise(True))

        # Calculate quality score
        df = df.withColumn("_quality_score",
                          (F.col("_is_valid_quantity").cast("int") +
                           F.col("_is_valid_price").cast("int") +
                           F.col("_has_invoice_no").cast("int")) / 3.0)

        # Add transaction type detection
        if "invoice_no" in df.columns:
            df = df.withColumn("_is_return",
                              F.when(F.col("invoice_no").startswith("C"), True).otherwise(False))

        return df

    def _enrich_metadata(self, df: DataFrame, input_files: list[dict[str, Any]]) -> DataFrame:
        """Enrich data with processing metadata."""
        current_timestamp = F.current_timestamp()

        df = df.withColumn("_ingestion_timestamp", current_timestamp)
        df = df.withColumn("_bronze_schema_version", F.lit("2.0"))
        df = df.withColumn("_processing_date", F.current_date())
        df = df.withColumn("_total_source_files", F.lit(len(input_files)))
        df = df.withColumn("_row_hash", F.hash(*[col for col in df.columns if not col.startswith("_")]))

        return df

    def _assess_data_quality(self, df: DataFrame) -> QualityReport:
        """Perform comprehensive data quality assessment."""
        logger.info("Performing data quality assessment...")
        return self.quality_checker.assess_dataframe(df)

    def _log_quality_metrics(self, quality_report: QualityReport) -> None:
        """Log data quality metrics."""
        logger.info("=== Data Quality Report ===")
        logger.info(f"Total records: {quality_report.total_records}")
        logger.info(f"Overall quality score: {quality_report.overall_score:.2%}")
        logger.info(f"High quality records: {quality_report.high_quality_records} ({quality_report.high_quality_percentage:.1f}%)")

        for dimension, score in quality_report.dimension_scores.items():
            logger.info(f"{dimension.title()}: {score:.2%}")

    def _apply_partitioning_strategy(self, df: DataFrame) -> DataFrame:
        """Apply intelligent partitioning strategy."""
        # Add partitioning columns
        if "invoice_timestamp" in df.columns:
            df = df.withColumn("year", F.year("invoice_timestamp"))
            df = df.withColumn("month", F.month("invoice_timestamp"))
        else:
            # Fallback to processing date
            df = df.withColumn("year", F.year("_processing_date"))
            df = df.withColumn("month", F.month("_processing_date"))

        return df

    def _write_bronze_data(self, df: DataFrame, output_path: Path) -> dict[str, Any]:
        """Write data to Bronze layer with optimizations."""
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing {df.count()} records to Bronze layer...")

        # Write with partitioning
        write_start = datetime.now()

        (df.write
         .mode("overwrite")
         .option("compression", "snappy")
         .partitionBy("year", "month")
         .parquet(str(output_path)))

        write_duration = (datetime.now() - write_start).total_seconds()

        # Calculate write statistics
        written_files = list(output_path.rglob("*.parquet"))
        total_size_bytes = sum(f.stat().st_size for f in written_files)

        write_stats = {
            "write_duration_seconds": write_duration,
            "output_files": len(written_files),
            "total_size_bytes": total_size_bytes,
            "total_size_mb": total_size_bytes / (1024 * 1024),
            "average_throughput_mb_per_second": (total_size_bytes / (1024 * 1024)) / write_duration if write_duration > 0 else 0
        }

        logger.info(f"Write completed in {write_duration:.1f}s, {write_stats['total_size_mb']:.1f}MB written")
        return write_stats

    def _generate_processing_report(self, **kwargs) -> dict[str, Any]:
        """Generate comprehensive processing report."""
        return {
            "timestamp": datetime.now().isoformat(),
            "bronze_processor_version": "2.0",
            "spark_version": self.spark.version if self.spark else "unknown",
            "config": {
                "environment": self.config.environment.value,
                "processing_engine": self.config.processing_engine.value,
            },
            **kwargs
        }


# Convenience functions
def process_bronze_enhanced(
    input_path: Path | None = None,
    output_path: Path | None = None,
    **kwargs
) -> dict[str, Any]:
    """Process Bronze layer with enhanced features."""
    processor = EnhancedBronzeProcessor()
    return processor.process(input_path=input_path, output_path=output_path, **kwargs)


def main() -> None:
    """Entry point for enhanced Bronze layer processing."""
    logger.info("Starting Enhanced Bronze Layer ETL...")

    try:
        config = BaseConfig()
        config.validate_paths()

        result = process_bronze_enhanced()

        print("Enhanced Bronze layer processing completed successfully")
        print(f"Processed {result.get('output_count', 0)} records")
        print(f"Overall quality score: {result.get('quality_report', {}).get('overall_score', 0):.2%}")

    except Exception as e:
        print(f"Enhanced Bronze layer processing failed: {e}")
        logger.error(f"Processing failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
