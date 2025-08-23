"""
Delta Lake Manager for Spark DataFrames
Provides unified Delta Lake operations for Bronze, Silver, and Gold layers
"""
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, isnan, isnull, 
    count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, stddev, expr, hash
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta import DeltaTable, configure_spark_with_delta_pip

from core.config.unified_config import get_unified_config
from core.logging import get_logger


class DeltaLakeManager:
    """
    Comprehensive Delta Lake manager for Spark DataFrames
    Supports ACID transactions, time travel, schema evolution, and optimizations
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.config = get_unified_config()
        self.logger = get_logger(__name__)
        
        # Initialize Spark session with Delta Lake
        self.spark = spark or self._create_delta_spark_session()
        
        # Delta Lake paths
        self.bronze_path = Path(self.config.base.data_path) / "bronze_delta"
        self.silver_path = Path(self.config.base.data_path) / "silver_delta"
        self.gold_path = Path(self.config.base.data_path) / "gold_delta"
        
        # Ensure directories exist
        self._create_directories()
        
        self.logger.info("DeltaLakeManager initialized with Delta Lake support")
    
    def _create_delta_spark_session(self) -> SparkSession:
        """Create Spark session optimized for Delta Lake"""
        
        builder = (SparkSession.builder
                  .appName("PwC-Delta-Lake-ETL")
                  .config("spark.sql.adaptive.enabled", "true")
                  .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                  .config("spark.sql.adaptive.skewJoin.enabled", "true")
                  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                  .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                  .config("spark.databricks.delta.vacuum.logging.enabled", "true")
                  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                  .config("spark.sql.shuffle.partitions", "200"))
        
        # Configure memory settings
        if self.config.spark.executor_memory:
            builder = builder.config("spark.executor.memory", self.config.spark.executor_memory)
        if self.config.spark.driver_memory:
            builder = builder.config("spark.driver.memory", self.config.spark.driver_memory)
            
        # Enable Delta Lake
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def _create_directories(self):
        """Create Delta Lake directories if they don't exist"""
        for path in [self.bronze_path, self.silver_path, self.gold_path]:
            path.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Created directory: {path}")
    
    def write_bronze_layer(
        self, 
        df: DataFrame, 
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        mode: str = "append",
        add_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Write data to Bronze Delta Lake layer with metadata enrichment
        
        Args:
            df: Spark DataFrame to write
            table_name: Name of the Delta table
            partition_cols: Optional partition columns
            mode: Write mode (append, overwrite, merge)
            add_metadata: Whether to add ingestion metadata
            
        Returns:
            Dictionary with write operation results
        """
        try:
            start_time = datetime.now()
            
            # Add ingestion metadata
            if add_metadata:
                df = self._add_bronze_metadata(df)
            
            # Prepare Delta table path
            table_path = self.bronze_path / table_name
            
            # Write to Delta Lake
            writer = df.write.format("delta").mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                
            writer.save(str(table_path))
            
            # Generate statistics
            record_count = df.count()
            processing_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                "status": "success",
                "table_name": table_name,
                "record_count": record_count,
                "processing_time_seconds": processing_time,
                "table_path": str(table_path),
                "partition_cols": partition_cols or [],
                "write_mode": mode
            }
            
            self.logger.info(f"Bronze layer write completed: {table_name} ({record_count:,} records)")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to write Bronze layer {table_name}: {str(e)}")
            raise
    
    def write_silver_layer(
        self,
        df: DataFrame,
        table_name: str,
        business_key: str,
        partition_cols: Optional[List[str]] = None,
        enable_scd2: bool = True
    ) -> Dict[str, Any]:
        """
        Write data to Silver Delta Lake layer with SCD2 support
        
        Args:
            df: Spark DataFrame to write
            table_name: Name of the Delta table
            business_key: Business key for SCD2 tracking
            partition_cols: Optional partition columns
            enable_scd2: Whether to enable SCD Type 2 processing
            
        Returns:
            Dictionary with write operation results
        """
        try:
            start_time = datetime.now()
            table_path = self.silver_path / table_name
            
            # Add Silver metadata
            df = self._add_silver_metadata(df)
            
            if enable_scd2 and self._table_exists(table_path):
                # Perform SCD2 merge
                result = self._perform_scd2_merge(df, table_path, business_key)
            else:
                # Initial write or non-SCD2 write
                writer = df.write.format("delta").mode("overwrite")
                
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                    
                writer.save(str(table_path))
                
                record_count = df.count()
                result = {
                    "status": "success",
                    "table_name": table_name,
                    "record_count": record_count,
                    "records_inserted": record_count,
                    "records_updated": 0,
                    "processing_time_seconds": (datetime.now() - start_time).total_seconds(),
                    "table_path": str(table_path),
                    "scd2_enabled": enable_scd2
                }
            
            self.logger.info(f"Silver layer write completed: {table_name}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to write Silver layer {table_name}: {str(e)}")
            raise
    
    def write_gold_layer(
        self,
        df: DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        optimize_for_queries: bool = True
    ) -> Dict[str, Any]:
        """
        Write data to Gold Delta Lake layer with query optimization
        
        Args:
            df: Spark DataFrame to write
            table_name: Name of the Delta table
            partition_cols: Optional partition columns
            optimize_for_queries: Whether to apply query optimizations
            
        Returns:
            Dictionary with write operation results
        """
        try:
            start_time = datetime.now()
            table_path = self.gold_path / table_name
            
            # Add Gold metadata
            df = self._add_gold_metadata(df)
            
            # Write to Delta Lake
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                
            writer.save(str(table_path))
            
            # Apply optimizations
            if optimize_for_queries:
                self._optimize_table(table_path)
            
            record_count = df.count()
            processing_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                "status": "success",
                "table_name": table_name,
                "record_count": record_count,
                "processing_time_seconds": processing_time,
                "table_path": str(table_path),
                "optimized": optimize_for_queries,
                "partition_cols": partition_cols or []
            }
            
            self.logger.info(f"Gold layer write completed: {table_name} ({record_count:,} records)")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to write Gold layer {table_name}: {str(e)}")
            raise
    
    def read_delta_table(
        self,
        layer: str,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        filters: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Read Delta table with time travel support
        
        Args:
            layer: Layer name (bronze, silver, gold)
            table_name: Name of the Delta table
            version: Specific version to read
            timestamp: Specific timestamp to read
            filters: Optional filter conditions
            
        Returns:
            Spark DataFrame
        """
        try:
            # Get table path
            layer_path = getattr(self, f"{layer}_path")
            table_path = layer_path / table_name
            
            if not self._table_exists(table_path):
                raise ValueError(f"Table {table_name} does not exist in {layer} layer")
            
            # Build reader
            reader = self.spark.read.format("delta")
            
            # Apply time travel
            if version is not None:
                reader = reader.option("versionAsOf", version)
            elif timestamp is not None:
                reader = reader.option("timestampAsOf", timestamp)
            
            df = reader.load(str(table_path))
            
            # Apply filters
            if filters:
                for filter_expr in filters:
                    df = df.filter(filter_expr)
            
            self.logger.info(f"Read Delta table: {layer}.{table_name}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read Delta table {layer}.{table_name}: {str(e)}")
            raise
    
    def get_table_history(self, layer: str, table_name: str) -> DataFrame:
        """Get Delta table history"""
        layer_path = getattr(self, f"{layer}_path")
        table_path = layer_path / table_name
        
        if not self._table_exists(table_path):
            raise ValueError(f"Table {table_name} does not exist in {layer} layer")
        
        delta_table = DeltaTable.forPath(self.spark, str(table_path))
        return delta_table.history()
    
    def vacuum_table(self, layer: str, table_name: str, retention_hours: int = 168):
        """
        Vacuum Delta table to remove old files
        Default retention: 7 days (168 hours)
        """
        layer_path = getattr(self, f"{layer}_path")
        table_path = layer_path / table_name
        
        if not self._table_exists(table_path):
            raise ValueError(f"Table {table_name} does not exist in {layer} layer")
        
        delta_table = DeltaTable.forPath(self.spark, str(table_path))
        delta_table.vacuum(retention_hours)
        
        self.logger.info(f"Vacuumed table {layer}.{table_name} (retention: {retention_hours}h)")
    
    def optimize_table(self, layer: str, table_name: str, z_order_cols: Optional[List[str]] = None):
        """Optimize Delta table with optional Z-ordering"""
        layer_path = getattr(self, f"{layer}_path")
        table_path = layer_path / table_name
        
        self._optimize_table(table_path, z_order_cols)
    
    def _add_bronze_metadata(self, df: DataFrame) -> DataFrame:
        """Add Bronze layer metadata"""
        return (df
                .withColumn("__bronze_ingestion_timestamp", current_timestamp())
                .withColumn("__bronze_file_path", lit("spark_dataframe"))
                .withColumn("__bronze_source_system", lit("etl_pipeline"))
                .withColumn("__bronze_schema_version", lit("1.0")))
    
    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """Add Silver layer metadata with SCD2 support"""
        return (df
                .withColumn("__silver_processed_timestamp", current_timestamp())
                .withColumn("__silver_valid_from", current_timestamp())
                .withColumn("__silver_valid_to", lit(None).cast(TimestampType()))
                .withColumn("__silver_is_current", lit(True))
                .withColumn("__silver_version", lit(1))
                .withColumn("__silver_record_hash", 
                           hash(*[col(c) for c in df.columns if not c.startswith("__")])))
    
    def _add_gold_metadata(self, df: DataFrame) -> DataFrame:
        """Add Gold layer metadata"""
        return (df
                .withColumn("__gold_created_timestamp", current_timestamp())
                .withColumn("__gold_last_updated", current_timestamp())
                .withColumn("__gold_aggregation_level", lit("detailed"))
                .withColumn("__gold_data_quality_score", lit(1.0)))
    
    def _perform_scd2_merge(
        self, 
        new_df: DataFrame, 
        table_path: Path, 
        business_key: str
    ) -> Dict[str, Any]:
        """Perform SCD2 merge operation"""
        
        delta_table = DeltaTable.forPath(self.spark, str(table_path))
        
        # Get current timestamp
        current_ts = current_timestamp()
        
        # Prepare merge condition
        merge_condition = f"target.{business_key} = source.{business_key} AND target.__silver_is_current = true"
        
        # Prepare update expressions
        update_when_matched = {
            "__silver_valid_to": current_ts,
            "__silver_is_current": lit(False),
            "__silver_last_updated": current_ts
        }
        
        # Prepare insert expressions for updated records
        insert_expressions = {col: f"source.{col}" for col in new_df.columns}
        insert_expressions.update({
            "__silver_valid_from": current_ts,
            "__silver_valid_to": lit(None).cast(TimestampType()),
            "__silver_is_current": lit(True),
            "__silver_version": expr("COALESCE(target.__silver_version, 0) + 1")
        })
        
        # Execute merge
        merge_result = (delta_table.alias("target")
                       .merge(new_df.alias("source"), merge_condition)
                       .whenMatchedUpdate(set=update_when_matched)
                       .whenNotMatchedInsert(values=insert_expressions)
                       .execute())
        
        return {
            "status": "success",
            "scd2_merge": True,
            "merge_metrics": merge_result
        }
    
    def _optimize_table(self, table_path: Path, z_order_cols: Optional[List[str]] = None):
        """Apply Delta table optimizations"""
        try:
            delta_table = DeltaTable.forPath(self.spark, str(table_path))
            
            if z_order_cols:
                delta_table.optimize().executeZOrderBy(*z_order_cols)
                self.logger.info(f"Optimized table with Z-ORDER BY {z_order_cols}")
            else:
                delta_table.optimize().executeCompaction()
                self.logger.info("Optimized table with compaction")
                
        except Exception as e:
            self.logger.warning(f"Table optimization failed: {str(e)}")
    
    def _table_exists(self, table_path: Path) -> bool:
        """Check if Delta table exists"""
        return (table_path.exists() and 
                (table_path / "_delta_log").exists())
    
    def get_table_metrics(self, layer: str, table_name: str) -> Dict[str, Any]:
        """Get comprehensive table metrics"""
        try:
            df = self.read_delta_table(layer, table_name)
            
            # Basic metrics
            total_records = df.count()
            column_count = len(df.columns)
            
            # Data quality metrics
            null_counts = {}
            for column in df.columns:
                if not column.startswith("__"):
                    null_count = df.filter(col(column).isNull()).count()
                    null_counts[column] = {
                        "null_count": null_count,
                        "null_percentage": (null_count / total_records) * 100 if total_records > 0 else 0
                    }
            
            # Get table history
            history = self.get_table_history(layer, table_name)
            version_count = history.count()
            
            return {
                "table_name": table_name,
                "layer": layer,
                "total_records": total_records,
                "column_count": column_count,
                "version_count": version_count,
                "null_analysis": null_counts,
                "last_updated": history.select("timestamp").orderBy(col("timestamp").desc()).first()["timestamp"]
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table metrics: {str(e)}")
            return {"error": str(e)}
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session closed")


# Factory function for easy instantiation
def create_delta_lake_manager(spark: Optional[SparkSession] = None) -> DeltaLakeManager:
    """Create DeltaLakeManager instance"""
    return DeltaLakeManager(spark)


# Example usage and testing
if __name__ == "__main__":
    # Initialize manager
    delta_manager = create_delta_lake_manager()
    
    try:
        # Create sample data
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True)
        ])
        
        sample_data = [(1, "test1", 100.0), (2, "test2", 200.0), (3, "test3", 300.0)]
        df = delta_manager.spark.createDataFrame(sample_data, schema)
        
        # Test Bronze layer
        print("Testing Bronze layer...")
        bronze_result = delta_manager.write_bronze_layer(df, "test_table")
        print(f"Bronze result: {bronze_result}")
        
        # Test Silver layer
        print("Testing Silver layer...")
        silver_result = delta_manager.write_silver_layer(df, "test_table", "id")
        print(f"Silver result: {silver_result}")
        
        # Test Gold layer
        print("Testing Gold layer...")
        gold_result = delta_manager.write_gold_layer(df, "test_table")
        print(f"Gold result: {gold_result}")
        
        # Test reading
        print("Testing read operations...")
        bronze_df = delta_manager.read_delta_table("bronze", "test_table")
        print(f"Bronze records: {bronze_df.count()}")
        
        # Test metrics
        print("Testing table metrics...")
        metrics = delta_manager.get_table_metrics("bronze", "test_table")
        print(f"Table metrics: {metrics}")
        
        print("✅ Delta Lake Manager testing completed successfully!")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        raise
    finally:
        delta_manager.close()