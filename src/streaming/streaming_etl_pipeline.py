"""
Real-Time Streaming ETL Pipeline with Spark Structured Streaming
Provides comprehensive streaming data processing for Bronze, Silver, and Gold layers
"""
from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, from_json, lit, 
    struct, to_json, udf, window, when
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
    DoubleType, IntegerType, BooleanType
)

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from etl.transformations.data_quality import DataQualityValidator
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingTopic


class StreamingLayer(Enum):
    """Streaming data layers"""
    BRONZE = "bronze"
    SILVER = "silver" 
    GOLD = "gold"


class ProcessingMode(Enum):
    """Stream processing modes"""
    APPEND = "append"
    COMPLETE = "complete"
    UPDATE = "update"


@dataclass
class StreamingConfig:
    """Configuration for streaming ETL pipeline"""
    kafka_bootstrap_servers: List[str]
    checkpoint_location: str
    output_path: str
    trigger_interval: str = "10 seconds"
    max_files_per_trigger: int = 1000
    enable_schema_evolution: bool = True
    enable_data_quality: bool = True
    watermark_delay: str = "10 minutes"
    late_data_tolerance: str = "1 hour"


@dataclass
class StreamingMetrics:
    """Metrics for streaming operations"""
    records_processed: int = 0
    records_failed: int = 0
    batches_processed: int = 0
    processing_rate: float = 0.0
    latency_ms: float = 0.0
    watermark_delay_ms: float = 0.0
    last_update: datetime = field(default_factory=datetime.now)


class StreamingETLProcessor(ABC):
    """Abstract base class for streaming ETL processors"""
    
    def __init__(self, spark: SparkSession, config: StreamingConfig):
        self.spark = spark
        self.config = config
        self.logger = get_logger(f"{self.__class__.__name__}")
        self.metrics_collector = get_metrics_collector()
        self.kafka_manager = KafkaManager()
        self.delta_manager = DeltaLakeManager(spark)
        self.metrics = StreamingMetrics()
        
    @abstractmethod
    def get_input_schema(self) -> StructType:
        """Get input schema for the stream"""
        pass
    
    @abstractmethod
    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Transform the streaming dataframe"""
        pass
    
    @abstractmethod
    def get_output_path(self) -> str:
        """Get output path for the layer"""
        pass
    
    def create_stream_reader(self, topics: List[str]) -> DataFrame:
        """Create Kafka stream reader"""
        return (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(self.config.kafka_bootstrap_servers))
            .option("subscribe", ",".join(topics))
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", self.config.max_files_per_trigger)
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def parse_kafka_message(self, df: DataFrame, schema: StructType) -> DataFrame:
        """Parse Kafka message with schema"""
        return (
            df
            .select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("message_value"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            .withColumn("parsed_value", from_json(col("message_value"), schema))
            .select(
                col("message_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("parsed_value.*")
            )
            .withColumn("processing_timestamp", current_timestamp())
        )
    
    def add_watermark(self, df: DataFrame, timestamp_col: str) -> DataFrame:
        """Add watermark for late data handling"""
        return df.withWatermark(timestamp_col, self.config.watermark_delay)
    
    def write_to_delta(self, df: DataFrame, output_mode: str = "append") -> StreamingQuery:
        """Write streaming dataframe to Delta Lake"""
        output_path = self.get_output_path()
        checkpoint_path = f"{self.config.checkpoint_location}/{self.__class__.__name__.lower()}"
        
        writer = (
            df
            .writeStream
            .format("delta")
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", str(self.config.enable_schema_evolution))
        )
        
        # Add trigger configuration
        if self.config.trigger_interval:
            writer = writer.trigger(processingTime=self.config.trigger_interval)
        
        return writer.start(output_path)
    
    def add_stream_listener(self, query: StreamingQuery):
        """Add listener for stream metrics"""
        class MetricsListener:
            def __init__(self, processor: StreamingETLProcessor):
                self.processor = processor
                
            def on_progress(self, query_progress):
                try:
                    progress = query_progress.progress
                    if progress:
                        self.processor.metrics.records_processed += progress.get("inputRowsPerSecond", 0)
                        self.processor.metrics.processing_rate = progress.get("inputRowsPerSecond", 0)
                        self.processor.metrics.batches_processed += 1
                        self.processor.metrics.last_update = datetime.now()
                        
                        # Send metrics
                        if self.processor.metrics_collector:
                            self.processor.metrics_collector.gauge(
                                "streaming_processing_rate",
                                self.processor.metrics.processing_rate,
                                {"layer": self.processor.__class__.__name__.lower()}
                            )
                            
                except Exception as e:
                    self.processor.logger.error(f"Metrics collection failed: {e}")
        
        # Note: PySpark doesn't have direct listener support like Scala
        # This would need to be implemented using query.progress monitoring
        
    def start_processing(self, topics: List[str]) -> StreamingQuery:
        """Start streaming processing"""
        try:
            # Create stream reader
            raw_stream = self.create_stream_reader(topics)
            
            # Parse messages
            schema = self.get_input_schema()
            parsed_stream = self.parse_kafka_message(raw_stream, schema)
            
            # Add watermark
            watermarked_stream = self.add_watermark(parsed_stream, "processing_timestamp")
            
            # Apply transformations
            transformed_stream = self.transform_stream(watermarked_stream)
            
            # Write to Delta Lake
            query = self.write_to_delta(transformed_stream)
            
            # Add metrics listener
            self.add_stream_listener(query)
            
            self.logger.info(f"Started streaming processing for {topics}")
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to start streaming processing: {e}")
            raise


class StreamingBronzeProcessor(StreamingETLProcessor):
    """Real-time Bronze layer processor for raw data ingestion"""
    
    def get_input_schema(self) -> StructType:
        """Schema for raw retail transactions"""
        return StructType([
            StructField("invoice_no", StringType(), True),
            StructField("stock_code", StringType(), True),
            StructField("description", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("invoice_date", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("customer_id", StringType(), True),
            StructField("country", StringType(), True),
            StructField("message_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("event_timestamp", TimestampType(), True)
        ])
    
    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Transform raw streaming data for Bronze layer"""
        try:
            transformed = (
                df
                .withColumn("bronze_id", expr("uuid()"))
                .withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("partition_date", expr("date(processing_timestamp)"))
                .withColumn("data_source", lit("kafka_stream"))
                .withColumn("is_processed", lit(False))
            )
            
            # Add data quality flags
            if self.config.enable_data_quality:
                transformed = self._add_quality_flags(transformed)
            
            self.logger.debug("Applied Bronze layer transformations")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Bronze transformation failed: {e}")
            raise
    
    def _add_quality_flags(self, df: DataFrame) -> DataFrame:
        """Add data quality flags"""
        return (
            df
            .withColumn("is_valid_invoice", col("invoice_no").isNotNull())
            .withColumn("is_valid_customer", col("customer_id").isNotNull())
            .withColumn("is_valid_amount", (col("quantity") > 0) & (col("unit_price") > 0))
            .withColumn("quality_score", 
                       (col("is_valid_invoice").cast("int") + 
                        col("is_valid_customer").cast("int") + 
                        col("is_valid_amount").cast("int")) / 3.0)
        )
    
    def get_output_path(self) -> str:
        """Get Bronze layer output path"""
        return f"{self.config.output_path}/bronze/streaming_retail_data"


class StreamingSilverProcessor(StreamingETLProcessor):
    """Real-time Silver layer processor for cleaned and enriched data"""
    
    def get_input_schema(self) -> StructType:
        """Schema for Bronze layer data"""
        return StructType([
            StructField("bronze_id", StringType(), False),
            StructField("invoice_no", StringType(), True),
            StructField("stock_code", StringType(), True),
            StructField("description", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("invoice_date", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("customer_id", StringType(), True),
            StructField("country", StringType(), True),
            StructField("quality_score", DoubleType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("processing_timestamp", TimestampType(), True)
        ])
    
    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Transform streaming data for Silver layer"""
        try:
            # Filter quality data
            quality_threshold = 0.7
            cleaned_df = df.filter(col("quality_score") >= quality_threshold)
            
            transformed = (
                cleaned_df
                .withColumn("silver_id", expr("uuid()"))
                .withColumn("total_amount", col("quantity") * col("unit_price"))
                .withColumn("processed_timestamp", current_timestamp())
                .withColumn("invoice_datetime", 
                           expr("to_timestamp(invoice_date, 'MM/dd/yyyy HH:mm')"))
                .withColumn("year", expr("year(invoice_datetime)"))
                .withColumn("month", expr("month(invoice_datetime)"))
                .withColumn("day", expr("day(invoice_datetime)"))
                .withColumn("hour", expr("hour(invoice_datetime)"))
            )
            
            # Add business logic transformations
            transformed = self._apply_business_rules(transformed)
            
            # Add real-time enrichment
            if hasattr(self, 'enrichment_service'):
                transformed = self._enrich_data(transformed)
            
            self.logger.debug("Applied Silver layer transformations")
            return transformed
            
        except Exception as e:
            self.logger.error(f"Silver transformation failed: {e}")
            raise
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business transformation rules"""
        return (
            df
            .withColumn("transaction_type", 
                       when(col("total_amount") < 0, "return")
                       .when(col("total_amount") > 1000, "high_value")
                       .otherwise("standard"))
            .withColumn("customer_segment",
                       when(col("total_amount") > 500, "premium")
                       .when(col("total_amount") > 100, "standard")
                       .otherwise("basic"))
            .withColumn("is_weekend", 
                       expr("dayofweek(invoice_datetime) in (1, 7)"))
        )
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with external sources (placeholder)"""
        # This would integrate with external APIs for real-time enrichment
        return df
    
    def get_output_path(self) -> str:
        """Get Silver layer output path"""
        return f"{self.config.output_path}/silver/streaming_retail_data"


class StreamingGoldProcessor(StreamingETLProcessor):
    """Real-time Gold layer processor for business-ready aggregations"""
    
    def get_input_schema(self) -> StructType:
        """Schema for Silver layer data"""
        return StructType([
            StructField("silver_id", StringType(), False),
            StructField("invoice_no", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("country", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("invoice_datetime", TimestampType(), True),
            StructField("processed_timestamp", TimestampType(), True)
        ])
    
    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Transform streaming data for Gold layer with aggregations"""
        try:
            # Add watermark for aggregations
            watermarked_df = df.withWatermark("invoice_datetime", "10 minutes")
            
            # Real-time aggregations with windowing
            hourly_aggregations = (
                watermarked_df
                .groupBy(
                    window(col("invoice_datetime"), "1 hour"),
                    col("country"),
                    col("customer_segment")
                )
                .agg({
                    "total_amount": "sum",
                    "invoice_no": "countDistinct",
                    "customer_id": "countDistinct"
                })
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("country"),
                    col("customer_segment"),
                    col("sum(total_amount)").alias("total_revenue"),
                    col("count(DISTINCT invoice_no)").alias("transaction_count"),
                    col("count(DISTINCT customer_id)").alias("unique_customers")
                )
                .withColumn("aggregation_type", lit("hourly"))
                .withColumn("gold_id", expr("uuid()"))
                .withColumn("created_timestamp", current_timestamp())
            )
            
            self.logger.debug("Applied Gold layer aggregations")
            return hourly_aggregations
            
        except Exception as e:
            self.logger.error(f"Gold aggregation failed: {e}")
            raise
    
    def get_output_path(self) -> str:
        """Get Gold layer output path"""
        return f"{self.config.output_path}/gold/streaming_aggregations"


class StreamingETLOrchestrator:
    """
    Orchestrates real-time streaming ETL pipeline across all layers
    """
    
    def __init__(self, spark: SparkSession, config: StreamingConfig):
        self.spark = spark
        self.config = config
        self.logger = get_logger(__name__)
        self.kafka_manager = KafkaManager()
        self.metrics_collector = get_metrics_collector()
        
        # Initialize processors
        self.bronze_processor = StreamingBronzeProcessor(spark, config)
        self.silver_processor = StreamingSilverProcessor(spark, config)
        self.gold_processor = StreamingGoldProcessor(spark, config)
        
        # Active queries
        self.active_queries: Dict[str, StreamingQuery] = {}
        
    def start_full_pipeline(self) -> Dict[str, StreamingQuery]:
        """Start complete streaming ETL pipeline"""
        try:
            self.logger.info("Starting full streaming ETL pipeline")
            
            # Start Bronze layer processing
            bronze_query = self.bronze_processor.start_processing([
                StreamingTopic.RETAIL_TRANSACTIONS.value
            ])
            self.active_queries["bronze"] = bronze_query
            
            # Start Silver layer processing (reads from Bronze Delta table)
            silver_query = self._start_silver_from_delta()
            self.active_queries["silver"] = silver_query
            
            # Start Gold layer processing (reads from Silver Delta table)
            gold_query = self._start_gold_from_delta()
            self.active_queries["gold"] = gold_query
            
            self.logger.info("Full streaming ETL pipeline started successfully")
            return self.active_queries
            
        except Exception as e:
            self.logger.error(f"Failed to start full pipeline: {e}")
            self._stop_all_queries()
            raise
    
    def _start_silver_from_delta(self) -> StreamingQuery:
        """Start Silver processing from Bronze Delta table"""
        bronze_path = self.bronze_processor.get_output_path()
        
        # Read stream from Bronze Delta table
        bronze_stream = (
            self.spark
            .readStream
            .format("delta")
            .option("ignoreChanges", "true")
            .load(bronze_path)
        )
        
        # Apply Silver transformations
        silver_stream = self.silver_processor.transform_stream(bronze_stream)
        
        # Write to Silver Delta table
        return self.silver_processor.write_to_delta(silver_stream)
    
    def _start_gold_from_delta(self) -> StreamingQuery:
        """Start Gold processing from Silver Delta table"""
        silver_path = self.silver_processor.get_output_path()
        
        # Read stream from Silver Delta table
        silver_stream = (
            self.spark
            .readStream
            .format("delta")
            .option("ignoreChanges", "true")
            .load(silver_path)
        )
        
        # Apply Gold transformations
        gold_stream = self.gold_processor.transform_stream(silver_stream)
        
        # Write to Gold Delta table with update mode for aggregations
        return self.gold_processor.write_to_delta(gold_stream, output_mode="update")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get status of all pipeline components"""
        status = {}
        
        for layer, query in self.active_queries.items():
            if query and query.isActive:
                last_progress = query.lastProgress
                status[layer] = {
                    "active": True,
                    "id": query.id,
                    "run_id": query.runId,
                    "batch_id": last_progress.get("batchId", -1) if last_progress else -1,
                    "input_rows_per_second": last_progress.get("inputRowsPerSecond", 0) if last_progress else 0,
                    "processed_rows_per_second": last_progress.get("processedRowsPerSecond", 0) if last_progress else 0,
                    "timestamp": last_progress.get("timestamp") if last_progress else None
                }
            else:
                status[layer] = {"active": False}
        
        return {
            "pipeline_status": status,
            "total_active_queries": sum(1 for s in status.values() if s.get("active", False)),
            "timestamp": datetime.now().isoformat()
        }
    
    def stop_pipeline(self):
        """Stop all streaming queries"""
        self.logger.info("Stopping streaming ETL pipeline")
        self._stop_all_queries()
        
    def _stop_all_queries(self):
        """Stop all active streaming queries"""
        for layer, query in self.active_queries.items():
            try:
                if query and query.isActive:
                    query.stop()
                    self.logger.info(f"Stopped {layer} streaming query")
            except Exception as e:
                self.logger.warning(f"Error stopping {layer} query: {e}")
        
        self.active_queries.clear()
    
    def restart_layer(self, layer: str):
        """Restart specific layer processing"""
        if layer not in ["bronze", "silver", "gold"]:
            raise ValueError(f"Invalid layer: {layer}")
        
        # Stop existing query
        if layer in self.active_queries:
            query = self.active_queries[layer]
            if query and query.isActive:
                query.stop()
        
        # Restart layer
        if layer == "bronze":
            query = self.bronze_processor.start_processing([
                StreamingTopic.RETAIL_TRANSACTIONS.value
            ])
        elif layer == "silver":
            query = self._start_silver_from_delta()
        else:  # gold
            query = self._start_gold_from_delta()
        
        self.active_queries[layer] = query
        self.logger.info(f"Restarted {layer} layer processing")


# Factory functions
def create_streaming_config(**kwargs) -> StreamingConfig:
    """Create streaming configuration with defaults"""
    config = get_unified_config()
    
    defaults = {
        "kafka_bootstrap_servers": ["localhost:9092"],
        "checkpoint_location": "/tmp/streaming_checkpoints",
        "output_path": "/tmp/streaming_output",
        "trigger_interval": "10 seconds",
        "max_files_per_trigger": 1000,
        "enable_schema_evolution": True,
        "enable_data_quality": True,
        "watermark_delay": "10 minutes",
        "late_data_tolerance": "1 hour"
    }
    
    # Override with provided kwargs
    defaults.update(kwargs)
    
    return StreamingConfig(**defaults)


def create_streaming_orchestrator(spark: SparkSession, **config_kwargs) -> StreamingETLOrchestrator:
    """Create streaming ETL orchestrator"""
    config = create_streaming_config(**config_kwargs)
    return StreamingETLOrchestrator(spark, config)


# Example usage and testing
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import time
    
    # Create Spark session with Delta Lake support
    spark = (
        SparkSession.builder
        .appName("StreamingETLPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/streaming_checkpoints")
        .getOrCreate()
    )
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Testing Streaming ETL Pipeline...")
        
        # Create configuration
        config = create_streaming_config(
            kafka_bootstrap_servers=["localhost:9092"],
            checkpoint_location="./tmp/streaming_checkpoints",
            output_path="./data/streaming",
            trigger_interval="5 seconds"
        )
        
        # Create orchestrator
        orchestrator = create_streaming_orchestrator(spark, **config.__dict__)
        
        # Start pipeline
        queries = orchestrator.start_full_pipeline()
        print(f"✅ Started {len(queries)} streaming queries")
        
        # Monitor for a short time
        for i in range(6):
            time.sleep(10)
            status = orchestrator.get_pipeline_status()
            print(f"Pipeline Status: {status['total_active_queries']} active queries")
            
            for layer, info in status["pipeline_status"].items():
                if info.get("active"):
                    print(f"  {layer}: {info.get('input_rows_per_second', 0):.1f} rows/sec")
        
        # Stop pipeline
        orchestrator.stop_pipeline()
        print("✅ Streaming ETL Pipeline testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()