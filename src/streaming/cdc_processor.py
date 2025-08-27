"""
Change Data Capture (CDC) Processor for Real-time Database Updates
Provides comprehensive CDC capabilities with multiple source support and conflict resolution
"""
from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable

import pandas as pd
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, from_json, lit, 
    max as spark_max, struct, to_json, when, row_number
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
    DoubleType, IntegerType, BooleanType, LongType
)
from pyspark.sql.window import Window

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingTopic


class CDCOperation(Enum):
    """CDC operation types"""
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"
    SNAPSHOT = "S"
    TRUNCATE = "T"
    

class CDCSource(Enum):
    """Supported CDC sources"""
    DEBEZIUM = "debezium"
    MAXWELL = "maxwell"
    CANAL = "canal"
    CUSTOM = "custom"
    

class ConflictResolution(Enum):
    """Conflict resolution strategies"""
    LAST_WRITER_WINS = "last_writer_wins"
    FIRST_WRITER_WINS = "first_writer_wins"
    MANUAL_RESOLUTION = "manual_resolution"
    MERGE_STRATEGY = "merge_strategy"


@dataclass
class CDCEvent:
    """CDC event structure"""
    operation: CDCOperation
    table_name: str
    database_name: str
    timestamp: datetime
    lsn: Optional[str] = None  # Log Sequence Number
    transaction_id: Optional[str] = None
    before_data: Optional[Dict[str, Any]] = None
    after_data: Optional[Dict[str, Any]] = None
    primary_keys: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CDCConfig:
    """Configuration for CDC processing"""
    source_type: CDCSource
    kafka_topics: List[str]
    target_tables: Dict[str, str]  # source_table -> target_path
    primary_keys: Dict[str, List[str]]  # table -> primary_keys
    conflict_resolution: ConflictResolution = ConflictResolution.LAST_WRITER_WINS
    enable_soft_deletes: bool = True
    batch_size: int = 1000
    checkpoint_interval: str = "10 seconds"
    watermark_delay: str = "5 minutes"
    late_data_tolerance: str = "1 hour"
    enable_schema_evolution: bool = True


class CDCSchemaRegistry:
    """Manages schemas for different CDC sources and tables"""
    
    def __init__(self):
        self.schemas: Dict[str, Dict[str, StructType]] = {}
        self.logger = get_logger(__name__)
    
    def register_debezium_schema(self, table_name: str, value_schema: StructType) -> StructType:
        """Register Debezium CDC schema"""
        # Debezium envelope schema
        debezium_schema = StructType([
            StructField("before", value_schema, True),
            StructField("after", value_schema, True),
            StructField("source", StructType([
                StructField("version", StringType(), True),
                StructField("connector", StringType(), True),
                StructField("name", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("snapshot", StringType(), True),
                StructField("db", StringType(), True),
                StructField("sequence", StringType(), True),
                StructField("table", StringType(), True),
                StructField("server_id", LongType(), True),
                StructField("gtid", StringType(), True),
                StructField("file", StringType(), True),
                StructField("pos", LongType(), True),
                StructField("row", IntegerType(), True),
                StructField("thread", LongType(), True),
                StructField("query", StringType(), True)
            ]), True),
            StructField("op", StringType(), True),  # c=create, u=update, d=delete, r=read
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StructType([
                StructField("id", StringType(), True),
                StructField("total_order", LongType(), True),
                StructField("data_collection_order", LongType(), True)
            ]), True)
        ])
        
        self.schemas[f"debezium_{table_name}"] = {
            "envelope": debezium_schema,
            "value": value_schema
        }
        
        return debezium_schema
    
    def register_maxwell_schema(self, table_name: str, value_schema: StructType) -> StructType:
        """Register Maxwell CDC schema"""
        maxwell_schema = StructType([
            StructField("database", StringType(), True),
            StructField("table", StringType(), True),
            StructField("type", StringType(), True),  # insert, update, delete
            StructField("ts", LongType(), True),
            StructField("xid", LongType(), True),
            StructField("commit", BooleanType(), True),
            StructField("data", value_schema, True),
            StructField("old", value_schema, True)
        ])
        
        self.schemas[f"maxwell_{table_name}"] = {
            "envelope": maxwell_schema,
            "value": value_schema
        }
        
        return maxwell_schema
    
    def get_schema(self, source_type: CDCSource, table_name: str) -> Optional[StructType]:
        """Get schema for source type and table"""
        schema_key = f"{source_type.value}_{table_name}"
        return self.schemas.get(schema_key, {}).get("envelope")


class CDCParser(ABC):
    """Abstract base class for CDC parsers"""
    
    @abstractmethod
    def parse_cdc_event(self, df: DataFrame) -> DataFrame:
        """Parse CDC events from raw stream"""
        pass
    
    @abstractmethod
    def extract_operation(self, df: DataFrame) -> DataFrame:
        """Extract CDC operation type"""
        pass


class DebeziumParser(CDCParser):
    """Debezium CDC event parser"""
    
    def __init__(self, schema_registry: CDCSchemaRegistry):
        self.schema_registry = schema_registry
        self.logger = get_logger(__name__)
    
    def parse_cdc_event(self, df: DataFrame) -> DataFrame:
        """Parse Debezium CDC events"""
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
            .withColumn("parsed_value", from_json(col("message_value"), self._get_debezium_schema()))
            .select(
                col("message_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("parsed_value.*")
            )
        )
    
    def extract_operation(self, df: DataFrame) -> DataFrame:
        """Extract Debezium operation details"""
        return (
            df
            .withColumn("cdc_operation", 
                       when(col("op") == "c", lit("INSERT"))
                       .when(col("op") == "u", lit("UPDATE"))
                       .when(col("op") == "d", lit("DELETE"))
                       .when(col("op") == "r", lit("SNAPSHOT"))
                       .otherwise(lit("UNKNOWN")))
            .withColumn("source_table", col("source.table"))
            .withColumn("source_database", col("source.db"))
            .withColumn("event_timestamp", expr("to_timestamp(ts_ms / 1000)"))
            .withColumn("lsn", col("source.pos").cast("string"))
            .withColumn("transaction_id", col("transaction.id"))
            .withColumn("before_data", col("before"))
            .withColumn("after_data", col("after"))
        )
    
    def _get_debezium_schema(self) -> StructType:
        """Get base Debezium schema (simplified)"""
        return StructType([
            StructField("before", StringType(), True),  # JSON string
            StructField("after", StringType(), True),   # JSON string
            StructField("source", StructType([
                StructField("db", StringType(), True),
                StructField("table", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("pos", LongType(), True)
            ]), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StructType([
                StructField("id", StringType(), True)
            ]), True)
        ])


class MaxwellParser(CDCParser):
    """Maxwell CDC event parser"""
    
    def __init__(self, schema_registry: CDCSchemaRegistry):
        self.schema_registry = schema_registry
        self.logger = get_logger(__name__)
    
    def parse_cdc_event(self, df: DataFrame) -> DataFrame:
        """Parse Maxwell CDC events"""
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
            .withColumn("parsed_value", from_json(col("message_value"), self._get_maxwell_schema()))
            .select(
                col("message_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("parsed_value.*")
            )
        )
    
    def extract_operation(self, df: DataFrame) -> DataFrame:
        """Extract Maxwell operation details"""
        return (
            df
            .withColumn("cdc_operation", 
                       when(col("type") == "insert", lit("INSERT"))
                       .when(col("type") == "update", lit("UPDATE"))
                       .when(col("type") == "delete", lit("DELETE"))
                       .otherwise(lit("UNKNOWN")))
            .withColumn("source_table", col("table"))
            .withColumn("source_database", col("database"))
            .withColumn("event_timestamp", expr("to_timestamp(ts / 1000)"))
            .withColumn("transaction_id", col("xid").cast("string"))
            .withColumn("before_data", col("old"))
            .withColumn("after_data", col("data"))
        )
    
    def _get_maxwell_schema(self) -> StructType:
        """Get base Maxwell schema (simplified)"""
        return StructType([
            StructField("database", StringType(), True),
            StructField("table", StringType(), True),
            StructField("type", StringType(), True),
            StructField("ts", LongType(), True),
            StructField("xid", LongType(), True),
            StructField("data", StringType(), True),    # JSON string
            StructField("old", StringType(), True)      # JSON string
        ])


class CDCProcessor:
    """
    Main CDC processor that handles real-time change data capture
    """
    
    def __init__(self, spark: SparkSession, config: CDCConfig):
        self.spark = spark
        self.config = config
        self.logger = get_logger(__name__)
        self.kafka_manager = KafkaManager()
        self.delta_manager = DeltaLakeManager(spark)
        self.metrics_collector = get_metrics_collector()
        self.schema_registry = CDCSchemaRegistry()
        
        # Initialize parser based on source type
        self.parser = self._create_parser()
        
        # Active queries
        self.active_queries: Dict[str, StreamingQuery] = {}
        
        # Metrics
        self.processed_events = 0
        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0
        
    def _create_parser(self) -> CDCParser:
        """Create CDC parser based on source type"""
        if self.config.source_type == CDCSource.DEBEZIUM:
            return DebeziumParser(self.schema_registry)
        elif self.config.source_type == CDCSource.MAXWELL:
            return MaxwellParser(self.schema_registry)
        else:
            raise ValueError(f"Unsupported CDC source: {self.config.source_type}")
    
    def start_cdc_processing(self) -> Dict[str, StreamingQuery]:
        """Start CDC processing for all configured topics"""
        try:
            self.logger.info(f"Starting CDC processing for {len(self.config.kafka_topics)} topics")
            
            for topic in self.config.kafka_topics:
                query = self._process_cdc_topic(topic)
                self.active_queries[topic] = query
            
            self.logger.info(f"Started CDC processing for {len(self.active_queries)} topics")
            return self.active_queries
            
        except Exception as e:
            self.logger.error(f"Failed to start CDC processing: {e}")
            self.stop_cdc_processing()
            raise
    
    def _process_cdc_topic(self, topic: str) -> StreamingQuery:
        """Process CDC events from a specific topic"""
        # Create Kafka stream reader
        kafka_stream = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(self.kafka_manager.bootstrap_servers))
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", self.config.batch_size)
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Parse CDC events
        parsed_stream = self.parser.parse_cdc_event(kafka_stream)
        extracted_stream = self.parser.extract_operation(parsed_stream)
        
        # Add watermark for late data handling
        watermarked_stream = extracted_stream.withWatermark("event_timestamp", self.config.watermark_delay)
        
        # Process CDC operations
        processed_stream = self._apply_cdc_logic(watermarked_stream)
        
        # Write to Delta Lake with merge logic
        return self._write_cdc_to_delta(processed_stream, topic)
    
    def _apply_cdc_logic(self, df: DataFrame) -> DataFrame:
        """Apply CDC-specific logic and transformations"""
        return (
            df
            .withColumn("cdc_id", expr("uuid()"))
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("is_deleted", 
                       when(col("cdc_operation") == "DELETE", True)
                       .otherwise(False))
            .withColumn("version", 
                       row_number().over(
                           Window.partitionBy("source_table", "message_key")
                           .orderBy(col("event_timestamp").desc())
                       ))
            # Add conflict resolution logic
            .filter(col("version") == 1)  # Keep only the latest version
        )
    
    def _write_cdc_to_delta(self, df: DataFrame, topic: str) -> StreamingQuery:
        """Write CDC changes to Delta Lake with merge operations"""
        
        # Define the merge logic as a function
        def merge_cdc_changes(batch_df: DataFrame, batch_id: int):
            try:
                # Get table mapping
                table_name = self._extract_table_name_from_topic(topic)
                target_path = self.config.target_tables.get(table_name)
                
                if not target_path:
                    self.logger.warning(f"No target path configured for table: {table_name}")
                    return
                
                # Ensure target table exists
                self._ensure_delta_table_exists(target_path, batch_df.schema)
                
                # Load Delta table
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Get primary keys for merge
                primary_keys = self.config.primary_keys.get(table_name, ["id"])
                
                # Build merge condition
                merge_condition = " AND ".join([
                    f"target.{pk} = source.{pk}" for pk in primary_keys
                ])
                
                # Perform merge operation
                merge_builder = (
                    delta_table.alias("target")
                    .merge(batch_df.alias("source"), merge_condition)
                )
                
                # Handle different CDC operations
                if self.config.enable_soft_deletes:
                    # Soft delete: mark as deleted instead of removing
                    merge_builder = (
                        merge_builder
                        .whenMatchedUpdate(
                            condition="source.cdc_operation = 'DELETE'",
                            set={
                                "is_deleted": "true",
                                "deleted_at": "source.processing_timestamp",
                                "updated_at": "source.processing_timestamp"
                            }
                        )
                        .whenMatchedUpdate(
                            condition="source.cdc_operation != 'DELETE'",
                            set={pk: f"source.{pk}" for pk in batch_df.columns if pk not in ["cdc_operation", "processing_timestamp"]}
                        )
                        .whenNotMatchedInsert(
                            condition="source.cdc_operation != 'DELETE'",
                            values={pk: f"source.{pk}" for pk in batch_df.columns}
                        )
                    )
                else:
                    # Hard delete: remove records
                    merge_builder = (
                        merge_builder
                        .whenMatchedDelete(condition="source.cdc_operation = 'DELETE'")
                        .whenMatchedUpdateAll(condition="source.cdc_operation != 'DELETE'")
                        .whenNotMatchedInsertAll(condition="source.cdc_operation != 'DELETE'")
                    )
                
                # Execute merge
                merge_builder.execute()
                
                # Update metrics
                operation_counts = batch_df.groupBy("cdc_operation").count().collect()
                for row in operation_counts:
                    op = row["cdc_operation"]
                    count = row["count"]
                    
                    if op == "INSERT":
                        self.insert_count += count
                    elif op == "UPDATE":
                        self.update_count += count
                    elif op == "DELETE":
                        self.delete_count += count
                
                self.processed_events += batch_df.count()
                
                self.logger.info(f"Processed CDC batch {batch_id} for topic {topic}: "
                               f"{batch_df.count()} events")
                
            except Exception as e:
                self.logger.error(f"CDC merge failed for batch {batch_id}: {e}")
                raise
        
        # Configure checkpoint location
        checkpoint_path = f"./tmp/cdc_checkpoints/{topic}"
        
        # Write stream with foreachBatch for merge operations
        return (
            df
            .writeStream
            .foreachBatch(merge_cdc_changes)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=self.config.checkpoint_interval)
            .start()
        )
    
    def _extract_table_name_from_topic(self, topic: str) -> str:
        """Extract table name from Kafka topic"""
        # Common CDC topic patterns:
        # debezium: server.database.table
        # maxwell: maxwell.table
        parts = topic.split(".")
        return parts[-1]  # Assume table name is the last part
    
    def _ensure_delta_table_exists(self, path: str, schema: StructType):
        """Ensure Delta table exists with proper schema"""
        try:
            # Check if table exists
            DeltaTable.forPath(self.spark, path)
        except Exception:
            # Create empty Delta table with schema
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").mode("overwrite").save(path)
            self.logger.info(f"Created Delta table at {path}")
    
    def stop_cdc_processing(self):
        """Stop all CDC processing queries"""
        self.logger.info("Stopping CDC processing")
        
        for topic, query in self.active_queries.items():
            try:
                if query and query.isActive:
                    query.stop()
                    self.logger.info(f"Stopped CDC processing for topic: {topic}")
            except Exception as e:
                self.logger.warning(f"Error stopping CDC query for {topic}: {e}")
        
        self.active_queries.clear()
    
    def get_cdc_metrics(self) -> Dict[str, Any]:
        """Get CDC processing metrics"""
        active_queries = sum(1 for q in self.active_queries.values() 
                           if q and q.isActive)
        
        return {
            "cdc_metrics": {
                "processed_events": self.processed_events,
                "insert_count": self.insert_count,
                "update_count": self.update_count,
                "delete_count": self.delete_count,
                "active_queries": active_queries,
                "configured_topics": len(self.config.kafka_topics),
                "source_type": self.config.source_type.value
            },
            "query_status": {
                topic: {
                    "active": query.isActive if query else False,
                    "id": query.id if query and query.isActive else None
                }
                for topic, query in self.active_queries.items()
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def restart_topic_processing(self, topic: str):
        """Restart CDC processing for a specific topic"""
        if topic in self.active_queries:
            # Stop existing query
            query = self.active_queries[topic]
            if query and query.isActive:
                query.stop()
        
        # Start new query
        query = self._process_cdc_topic(topic)
        self.active_queries[topic] = query
        
        self.logger.info(f"Restarted CDC processing for topic: {topic}")


# Factory functions
def create_cdc_config(
    source_type: CDCSource,
    kafka_topics: List[str],
    target_tables: Dict[str, str],
    primary_keys: Dict[str, List[str]],
    **kwargs
) -> CDCConfig:
    """Create CDC configuration"""
    
    defaults = {
        "conflict_resolution": ConflictResolution.LAST_WRITER_WINS,
        "enable_soft_deletes": True,
        "batch_size": 1000,
        "checkpoint_interval": "10 seconds",
        "watermark_delay": "5 minutes",
        "late_data_tolerance": "1 hour",
        "enable_schema_evolution": True
    }
    
    defaults.update(kwargs)
    
    return CDCConfig(
        source_type=source_type,
        kafka_topics=kafka_topics,
        target_tables=target_tables,
        primary_keys=primary_keys,
        **defaults
    )


def create_cdc_processor(spark: SparkSession, config: CDCConfig) -> CDCProcessor:
    """Create CDC processor instance"""
    return CDCProcessor(spark, config)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import time
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("CDCProcessor")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        print("Testing CDC Processor...")
        
        # Create configuration
        config = create_cdc_config(
            source_type=CDCSource.DEBEZIUM,
            kafka_topics=["debezium.retail.customers", "debezium.retail.orders"],
            target_tables={
                "customers": "./data/cdc/customers",
                "orders": "./data/cdc/orders"
            },
            primary_keys={
                "customers": ["customer_id"],
                "orders": ["order_id"]
            },
            checkpoint_interval="5 seconds"
        )
        
        # Create processor
        processor = create_cdc_processor(spark, config)
        
        # Start processing
        queries = processor.start_cdc_processing()
        print(f"✅ Started CDC processing for {len(queries)} topics")
        
        # Monitor for a short time
        for i in range(6):
            time.sleep(10)
            metrics = processor.get_cdc_metrics()
            print(f"CDC Metrics: {metrics['cdc_metrics']['processed_events']} events processed")
        
        # Stop processing
        processor.stop_cdc_processing()
        print("✅ CDC Processor testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()