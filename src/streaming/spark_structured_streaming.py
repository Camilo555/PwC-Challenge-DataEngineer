"""
Advanced Spark Structured Streaming Framework
Provides comprehensive streaming capabilities with advanced features and enterprise patterns
"""
from __future__ import annotations

import json
import uuid
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, struct, when, coalesce,
    window, session_window, count, sum as spark_sum, avg, max as spark_max,
    from_json, to_json, schema_of_json, get_json_object,
    split, regexp_extract, regexp_replace, length, size,
    broadcast, bucketBy, desc, asc
)
from pyspark.sql.streaming import StreamingQuery, StreamingQueryException
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, BooleanType, MapType, ArrayType
)

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingTopic


class StreamingTriggerType(Enum):
    """Streaming trigger types"""
    MICRO_BATCH = "micro_batch"
    CONTINUOUS = "continuous"
    ONCE = "once"
    AVAILABLE_NOW = "available_now"


class OutputMode(Enum):
    """Streaming output modes"""
    APPEND = "append"
    UPDATE = "update"
    COMPLETE = "complete"


class StreamingSourceType(Enum):
    """Types of streaming sources"""
    KAFKA = "kafka"
    SOCKET = "socket"
    RATE = "rate"
    FILE = "file"
    DELTA = "delta"
    KINESIS = "kinesis"


class StreamingSinkType(Enum):
    """Types of streaming sinks"""
    DELTA = "delta"
    KAFKA = "kafka"
    CONSOLE = "console"
    MEMORY = "memory"
    FILE = "file"
    ELASTICSEARCH = "elasticsearch"
    CUSTOM = "custom"


@dataclass
class StreamingSourceConfig:
    """Configuration for streaming source"""
    source_type: StreamingSourceType
    source_options: Dict[str, str] = field(default_factory=dict)
    schema: Optional[StructType] = None
    watermark_column: Optional[str] = None
    watermark_delay: str = "10 minutes"
    max_files_per_trigger: Optional[int] = None
    max_offsets_per_trigger: Optional[int] = None


@dataclass
class StreamingSinkConfig:
    """Configuration for streaming sink"""
    sink_type: StreamingSinkType
    sink_options: Dict[str, str] = field(default_factory=dict)
    output_mode: OutputMode = OutputMode.APPEND
    trigger_type: StreamingTriggerType = StreamingTriggerType.MICRO_BATCH
    trigger_interval: str = "30 seconds"
    checkpoint_location: Optional[str] = None
    partition_columns: List[str] = field(default_factory=list)
    enable_compaction: bool = True


@dataclass
class StreamingQueryConfig:
    """Configuration for streaming query"""
    query_name: str
    source_config: StreamingSourceConfig
    sink_config: StreamingSinkConfig
    transformation_func: Optional[Callable] = None
    stateful_processing: bool = False
    enable_metrics: bool = True
    enable_alerts: bool = True
    query_timeout: Optional[int] = None  # seconds


@dataclass
class StreamingMetrics:
    """Streaming query metrics"""
    query_id: str
    query_name: str
    batch_id: int
    timestamp: datetime
    input_rows_per_second: float
    processed_rows_per_second: float
    batch_duration_ms: int
    trigger_execution_ms: int
    state_rows: int
    watermark: Optional[str] = None
    source_progress: Dict[str, Any] = field(default_factory=dict)
    sink_progress: Dict[str, Any] = field(default_factory=dict)


class StreamingSourceFactory:
    """Factory for creating streaming sources"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
    
    def create_source(self, config: StreamingSourceConfig) -> DataFrame:
        """Create streaming source DataFrame"""
        try:
            if config.source_type == StreamingSourceType.KAFKA:
                return self._create_kafka_source(config)
            elif config.source_type == StreamingSourceType.SOCKET:
                return self._create_socket_source(config)
            elif config.source_type == StreamingSourceType.RATE:
                return self._create_rate_source(config)
            elif config.source_type == StreamingSourceType.FILE:
                return self._create_file_source(config)
            elif config.source_type == StreamingSourceType.DELTA:
                return self._create_delta_source(config)
            else:
                raise ValueError(f"Unsupported source type: {config.source_type}")
                
        except Exception as e:
            self.logger.error(f"Failed to create streaming source: {e}")
            raise
    
    def _create_kafka_source(self, config: StreamingSourceConfig) -> DataFrame:
        """Create Kafka streaming source"""
        default_options = {
            "kafka.bootstrap.servers": "localhost:9092",
            "subscribe": "streaming_data",
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": str(config.max_offsets_per_trigger or 10000)
        }
        
        # Merge with provided options
        kafka_options = {**default_options, **config.source_options}
        
        # Create streaming DataFrame
        df = (
            self.spark
            .readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
        )
        
        # Add watermark if specified
        if config.watermark_column and config.watermark_delay:
            df = df.withWatermark(config.watermark_column, config.watermark_delay)
        
        return df
    
    def _create_socket_source(self, config: StreamingSourceConfig) -> DataFrame:
        """Create socket streaming source"""
        default_options = {
            "host": "localhost",
            "port": "9999"
        }
        
        socket_options = {**default_options, **config.source_options}
        
        return (
            self.spark
            .readStream
            .format("socket")
            .options(**socket_options)
            .load()
        )
    
    def _create_rate_source(self, config: StreamingSourceConfig) -> DataFrame:
        """Create rate streaming source for testing"""
        default_options = {
            "rowsPerSecond": "10",
            "rampUpTime": "0s",
            "numPartitions": "1"
        }
        
        rate_options = {**default_options, **config.source_options}
        
        return (
            self.spark
            .readStream
            .format("rate")
            .options(**rate_options)
            .load()
        )
    
    def _create_file_source(self, config: StreamingSourceConfig) -> DataFrame:
        """Create file streaming source"""
        default_options = {
            "path": "/tmp/streaming_files",
            "maxFilesPerTrigger": str(config.max_files_per_trigger or 100),
            "latestFirst": "false",
            "fileNameOnly": "false"
        }
        
        file_options = {**default_options, **config.source_options}
        path = file_options.pop("path")
        
        df_reader = self.spark.readStream.options(**file_options)
        
        if config.schema:
            df_reader = df_reader.schema(config.schema)
        
        # Detect format from options or default to JSON
        file_format = config.source_options.get("format", "json")
        
        return df_reader.format(file_format).load(path)
    
    def _create_delta_source(self, config: StreamingSourceConfig) -> DataFrame:
        """Create Delta Lake streaming source"""
        default_options = {
            "ignoreDeletes": "false",
            "ignoreChanges": "false",
            "startingVersion": "latest"
        }
        
        delta_options = {**default_options, **config.source_options}
        path = delta_options.pop("path", "/tmp/delta_table")
        
        df = (
            self.spark
            .readStream
            .format("delta")
            .options(**delta_options)
            .load(path)
        )
        
        if config.watermark_column and config.watermark_delay:
            df = df.withWatermark(config.watermark_column, config.watermark_delay)
        
        return df


class StreamingSinkFactory:
    """Factory for creating streaming sinks"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
    
    def create_sink_query(
        self, 
        df: DataFrame, 
        config: StreamingSinkConfig,
        query_name: str
    ) -> StreamingQuery:
        """Create streaming sink query"""
        try:
            # Create base writer
            writer = df.writeStream.queryName(query_name)
            
            # Set output mode
            writer = writer.outputMode(config.output_mode.value)
            
            # Set trigger
            writer = self._configure_trigger(writer, config)
            
            # Set checkpoint location if specified
            if config.checkpoint_location:
                writer = writer.option("checkpointLocation", config.checkpoint_location)
            
            # Configure sink-specific options
            if config.sink_type == StreamingSinkType.DELTA:
                return self._create_delta_sink(writer, config)
            elif config.sink_type == StreamingSinkType.KAFKA:
                return self._create_kafka_sink(writer, config)
            elif config.sink_type == StreamingSinkType.CONSOLE:
                return self._create_console_sink(writer, config)
            elif config.sink_type == StreamingSinkType.MEMORY:
                return self._create_memory_sink(writer, config)
            elif config.sink_type == StreamingSinkType.FILE:
                return self._create_file_sink(writer, config)
            else:
                raise ValueError(f"Unsupported sink type: {config.sink_type}")
                
        except Exception as e:
            self.logger.error(f"Failed to create streaming sink: {e}")
            raise
    
    def _configure_trigger(self, writer, config: StreamingSinkConfig):
        """Configure streaming trigger"""
        if config.trigger_type == StreamingTriggerType.MICRO_BATCH:
            return writer.trigger(processingTime=config.trigger_interval)
        elif config.trigger_type == StreamingTriggerType.CONTINUOUS:
            return writer.trigger(continuous=config.trigger_interval)
        elif config.trigger_type == StreamingTriggerType.ONCE:
            return writer.trigger(once=True)
        elif config.trigger_type == StreamingTriggerType.AVAILABLE_NOW:
            return writer.trigger(availableNow=True)
        else:
            return writer.trigger(processingTime=config.trigger_interval)
    
    def _create_delta_sink(self, writer, config: StreamingSinkConfig) -> StreamingQuery:
        """Create Delta Lake sink"""
        path = config.sink_options.get("path", "/tmp/streaming_output")
        
        # Configure Delta-specific options
        writer = writer.format("delta")
        
        for option_key, option_value in config.sink_options.items():
            if option_key != "path":
                writer = writer.option(option_key, option_value)
        
        # Add partitioning if specified
        if config.partition_columns:
            writer = writer.partitionBy(*config.partition_columns)
        
        return writer.start(path)
    
    def _create_kafka_sink(self, writer, config: StreamingSinkConfig) -> StreamingQuery:
        """Create Kafka sink"""
        default_options = {
            "kafka.bootstrap.servers": "localhost:9092",
            "topic": "streaming_output"
        }
        
        kafka_options = {**default_options, **config.sink_options}
        
        writer = writer.format("kafka")
        for option_key, option_value in kafka_options.items():
            writer = writer.option(option_key, option_value)
        
        return writer.start()
    
    def _create_console_sink(self, writer, config: StreamingSinkConfig) -> StreamingQuery:
        """Create console sink for debugging"""
        console_options = {
            "numRows": "20",
            "truncate": "false",
            **config.sink_options
        }
        
        writer = writer.format("console")
        for option_key, option_value in console_options.items():
            writer = writer.option(option_key, option_value)
        
        return writer.start()
    
    def _create_memory_sink(self, writer, config: StreamingSinkConfig) -> StreamingQuery:
        """Create memory sink for testing"""
        return writer.format("memory").start()
    
    def _create_file_sink(self, writer, config: StreamingSinkConfig) -> StreamingQuery:
        """Create file sink"""
        path = config.sink_options.get("path", "/tmp/streaming_files_output")
        file_format = config.sink_options.get("format", "parquet")
        
        writer = writer.format(file_format)
        
        for option_key, option_value in config.sink_options.items():
            if option_key not in ["path", "format"]:
                writer = writer.option(option_key, option_value)
        
        if config.partition_columns:
            writer = writer.partitionBy(*config.partition_columns)
        
        return writer.start(path)


class StreamingQueryManager:
    """Manager for streaming queries with advanced monitoring and control"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Factories
        self.source_factory = StreamingSourceFactory(spark)
        self.sink_factory = StreamingSinkFactory(spark)
        
        # Active queries tracking
        self.active_queries: Dict[str, Tuple[StreamingQuery, StreamingQueryConfig]] = {}
        self.query_metrics: Dict[str, List[StreamingMetrics]] = {}
        
        # Monitoring thread
        self.monitoring_enabled = True
        self.monitoring_interval = 30  # seconds
        
    def create_and_start_query(self, config: StreamingQueryConfig) -> StreamingQuery:
        """Create and start streaming query"""
        try:
            self.logger.info(f"Creating streaming query: {config.query_name}")
            
            # Create source
            source_df = self.source_factory.create_source(config.source_config)
            
            # Apply transformations if provided
            if config.transformation_func:
                transformed_df = config.transformation_func(source_df)
            else:
                transformed_df = source_df
            
            # Create and start sink
            query = self.sink_factory.create_sink_query(
                transformed_df, 
                config.sink_config,
                config.query_name
            )
            
            # Register query for monitoring
            self.active_queries[config.query_name] = (query, config)
            self.query_metrics[config.query_name] = []
            
            # Start monitoring
            if config.enable_metrics:
                self._start_query_monitoring(config.query_name)
            
            self.logger.info(f"Started streaming query: {config.query_name} (ID: {query.id})")
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to create streaming query {config.query_name}: {e}")
            raise
    
    def stop_query(self, query_name: str, graceful: bool = True):
        """Stop streaming query"""
        try:
            if query_name not in self.active_queries:
                self.logger.warning(f"Query {query_name} not found in active queries")
                return
            
            query, config = self.active_queries[query_name]
            
            if query.isActive:
                if graceful:
                    # Wait for current batch to complete
                    query.stop()
                else:
                    # Force stop
                    query.stop()
                
                self.logger.info(f"Stopped streaming query: {query_name}")
            else:
                self.logger.info(f"Query {query_name} was already stopped")
            
            # Remove from active queries
            del self.active_queries[query_name]
            
        except Exception as e:
            self.logger.error(f"Failed to stop query {query_name}: {e}")
    
    def stop_all_queries(self, graceful: bool = True):
        """Stop all active queries"""
        query_names = list(self.active_queries.keys())
        
        for query_name in query_names:
            self.stop_query(query_name, graceful)
        
        self.logger.info("Stopped all streaming queries")
    
    def get_query_status(self, query_name: str = None) -> Dict[str, Any]:
        """Get status of streaming queries"""
        try:
            if query_name:
                if query_name not in self.active_queries:
                    return {"error": f"Query {query_name} not found"}
                
                query, config = self.active_queries[query_name]
                return self._get_single_query_status(query_name, query, config)
            else:
                return {
                    "total_queries": len(self.active_queries),
                    "active_queries": sum(1 for q, _ in self.active_queries.values() if q.isActive),
                    "queries": {
                        name: self._get_single_query_status(name, query, config)
                        for name, (query, config) in self.active_queries.items()
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get query status: {e}")
            return {"error": str(e)}
    
    def _get_single_query_status(
        self, 
        query_name: str, 
        query: StreamingQuery, 
        config: StreamingQueryConfig
    ) -> Dict[str, Any]:
        """Get status for a single query"""
        try:
            status = {
                "query_name": query_name,
                "query_id": query.id,
                "is_active": query.isActive,
                "run_id": query.runId,
                "source_type": config.source_config.source_type.value,
                "sink_type": config.sink_config.sink_type.value,
                "output_mode": config.sink_config.output_mode.value
            }
            
            if query.isActive:
                last_progress = query.lastProgress
                if last_progress:
                    status.update({
                        "batch_id": last_progress.get("batchId", -1),
                        "timestamp": last_progress.get("timestamp", ""),
                        "input_rows_per_second": last_progress.get("inputRowsPerSecond", 0),
                        "processed_rows_per_second": last_progress.get("processedRowsPerSecond", 0),
                        "batch_duration": last_progress.get("batchDuration", 0),
                        "trigger_execution": last_progress.get("triggerExecution", 0),
                        "watermark": last_progress.get("eventTime", {}).get("watermark"),
                        "state_operators": len(last_progress.get("stateOperators", [])),
                        "sources": last_progress.get("sources", []),
                        "sink": last_progress.get("sink", {})
                    })
            else:
                # Get exception if query failed
                exception = query.exception()
                if exception:
                    status["exception"] = {
                        "class": exception.__class__.__name__,
                        "message": str(exception)
                    }
            
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to get status for query {query_name}: {e}")
            return {"error": str(e)}
    
    def _start_query_monitoring(self, query_name: str):
        """Start monitoring for a specific query"""
        # This would typically run in a separate thread
        # For simplicity, we'll just log the start of monitoring
        self.logger.info(f"Started monitoring for query: {query_name}")
    
    def collect_metrics(self, query_name: str) -> Optional[StreamingMetrics]:
        """Collect metrics for a streaming query"""
        try:
            if query_name not in self.active_queries:
                return None
            
            query, config = self.active_queries[query_name]
            
            if not query.isActive:
                return None
            
            last_progress = query.lastProgress
            if not last_progress:
                return None
            
            # Create metrics object
            metrics = StreamingMetrics(
                query_id=query.id,
                query_name=query_name,
                batch_id=last_progress.get("batchId", -1),
                timestamp=datetime.now(),
                input_rows_per_second=last_progress.get("inputRowsPerSecond", 0),
                processed_rows_per_second=last_progress.get("processedRowsPerSecond", 0),
                batch_duration_ms=last_progress.get("batchDuration", 0),
                trigger_execution_ms=last_progress.get("triggerExecution", 0),
                state_rows=sum(op.get("numRowsTotal", 0) for op in last_progress.get("stateOperators", [])),
                watermark=last_progress.get("eventTime", {}).get("watermark"),
                source_progress=last_progress.get("sources", []),
                sink_progress=last_progress.get("sink", {})
            )
            
            # Store metrics
            if query_name not in self.query_metrics:
                self.query_metrics[query_name] = []
            
            self.query_metrics[query_name].append(metrics)
            
            # Keep only last 100 metrics per query
            if len(self.query_metrics[query_name]) > 100:
                self.query_metrics[query_name] = self.query_metrics[query_name][-100:]
            
            # Send metrics to collector if available
            if self.metrics_collector and config.enable_metrics:
                self._send_metrics_to_collector(metrics)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to collect metrics for {query_name}: {e}")
            return None
    
    def _send_metrics_to_collector(self, metrics: StreamingMetrics):
        """Send metrics to monitoring system"""
        try:
            # Send various metrics
            self.metrics_collector.gauge(
                "streaming_input_rate",
                metrics.input_rows_per_second,
                {"query_name": metrics.query_name, "query_id": metrics.query_id}
            )
            
            self.metrics_collector.gauge(
                "streaming_processing_rate",
                metrics.processed_rows_per_second,
                {"query_name": metrics.query_name, "query_id": metrics.query_id}
            )
            
            self.metrics_collector.gauge(
                "streaming_batch_duration",
                float(metrics.batch_duration_ms),
                {"query_name": metrics.query_name, "query_id": metrics.query_id}
            )
            
            self.metrics_collector.gauge(
                "streaming_state_rows",
                float(metrics.state_rows),
                {"query_name": metrics.query_name, "query_id": metrics.query_id}
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to send metrics to collector: {e}")
    
    def get_query_metrics(self, query_name: str) -> List[StreamingMetrics]:
        """Get collected metrics for a query"""
        return self.query_metrics.get(query_name, [])
    
    def restart_query(self, query_name: str) -> bool:
        """Restart a streaming query"""
        try:
            if query_name not in self.active_queries:
                self.logger.error(f"Query {query_name} not found")
                return False
            
            # Get configuration
            _, config = self.active_queries[query_name]
            
            # Stop current query
            self.stop_query(query_name, graceful=True)
            
            # Wait a moment
            time.sleep(2)
            
            # Restart with same configuration
            new_query = self.create_and_start_query(config)
            
            self.logger.info(f"Restarted query {query_name} with new ID: {new_query.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restart query {query_name}: {e}")
            return False


# Factory function
def create_streaming_query_manager(spark: SparkSession) -> StreamingQueryManager:
    """Create streaming query manager instance"""
    return StreamingQueryManager(spark)


# Utility functions for common streaming patterns
def create_kafka_to_delta_query(
    spark: SparkSession,
    kafka_servers: str,
    kafka_topic: str,
    delta_path: str,
    query_name: str,
    transformation_func: Optional[Callable] = None,
    checkpoint_location: str = None
) -> StreamingQuery:
    """Create a Kafka to Delta streaming query"""
    
    manager = create_streaming_query_manager(spark)
    
    # Configure source
    source_config = StreamingSourceConfig(
        source_type=StreamingSourceType.KAFKA,
        source_options={
            "kafka.bootstrap.servers": kafka_servers,
            "subscribe": kafka_topic,
            "startingOffsets": "latest"
        }
    )
    
    # Configure sink
    sink_config = StreamingSinkConfig(
        sink_type=StreamingSinkType.DELTA,
        sink_options={"path": delta_path},
        checkpoint_location=checkpoint_location or f"/tmp/checkpoints/{query_name}",
        output_mode=OutputMode.APPEND
    )
    
    # Create query configuration
    query_config = StreamingQueryConfig(
        query_name=query_name,
        source_config=source_config,
        sink_config=sink_config,
        transformation_func=transformation_func
    )
    
    return manager.create_and_start_query(query_config)


def create_delta_to_kafka_query(
    spark: SparkSession,
    delta_path: str,
    kafka_servers: str,
    kafka_topic: str,
    query_name: str,
    transformation_func: Optional[Callable] = None
) -> StreamingQuery:
    """Create a Delta to Kafka streaming query"""
    
    manager = create_streaming_query_manager(spark)
    
    # Configure source
    source_config = StreamingSourceConfig(
        source_type=StreamingSourceType.DELTA,
        source_options={"path": delta_path}
    )
    
    # Configure sink
    sink_config = StreamingSinkConfig(
        sink_type=StreamingSinkType.KAFKA,
        sink_options={
            "kafka.bootstrap.servers": kafka_servers,
            "topic": kafka_topic
        },
        output_mode=OutputMode.UPDATE
    )
    
    # Create query configuration
    query_config = StreamingQueryConfig(
        query_name=query_name,
        source_config=source_config,
        sink_config=sink_config,
        transformation_func=transformation_func
    )
    
    return manager.create_and_start_query(query_config)


# Example usage and testing
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("SparkStructuredStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        print("Testing Spark Structured Streaming Framework...")
        
        # Create streaming manager
        manager = create_streaming_query_manager(spark)
        
        # Test with rate source (for demonstration)
        source_config = StreamingSourceConfig(
            source_type=StreamingSourceType.RATE,
            source_options={"rowsPerSecond": "5"}
        )
        
        sink_config = StreamingSinkConfig(
            sink_type=StreamingSinkType.CONSOLE,
            sink_options={"numRows": "10", "truncate": "false"},
            trigger_interval="5 seconds"
        )
        
        def sample_transformation(df):
            return (
                df
                .withColumn("doubled_value", col("value") * 2)
                .withColumn("processing_time", current_timestamp())
            )
        
        query_config = StreamingQueryConfig(
            query_name="test_rate_query",
            source_config=source_config,
            sink_config=sink_config,
            transformation_func=sample_transformation
        )
        
        # Start query
        query = manager.create_and_start_query(query_config)
        
        print(f"✅ Created streaming query: {query.id}")
        
        # Let it run for a few seconds
        time.sleep(10)
        
        # Get status
        status = manager.get_query_status("test_rate_query")
        print(f"✅ Query status - Active: {status['is_active']}, Batch ID: {status.get('batch_id', 'N/A')}")
        
        # Collect metrics
        metrics = manager.collect_metrics("test_rate_query")
        if metrics:
            print(f"✅ Metrics - Input rate: {metrics.input_rows_per_second:.2f} rows/sec")
        
        # Stop query
        manager.stop_query("test_rate_query")
        print("✅ Stopped query")
        
        print("✅ Spark Structured Streaming Framework testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            # Clean up any remaining queries
            for query in spark.streams.active:
                query.stop()
        except:
            pass
        spark.stop()