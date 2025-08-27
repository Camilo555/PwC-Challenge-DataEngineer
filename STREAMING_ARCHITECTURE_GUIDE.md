# Streaming Architecture Documentation

## Table of Contents
1. [Streaming Platform Overview](#streaming-platform-overview)
2. [Architecture Components](#architecture-components)
3. [Data Processing Layers](#data-processing-layers)
4. [Real-time Processing Pipeline](#real-time-processing-pipeline)
5. [Stream Processing Engines](#stream-processing-engines)
6. [Kafka Integration](#kafka-integration)
7. [Delta Lake & Storage](#delta-lake--storage)
8. [Schema Evolution Management](#schema-evolution-management)
9. [Change Data Capture (CDC)](#change-data-capture-cdc)
10. [Stream Analytics & ML](#stream-analytics--ml)
11. [Governance & Monitoring](#governance--monitoring)
12. [Performance Optimization](#performance-optimization)
13. [Troubleshooting Guide](#troubleshooting-guide)

## Streaming Platform Overview

### Enterprise Streaming Data Platform
Our enterprise streaming platform is built on a modern, scalable architecture that processes real-time data across multiple layers using industry-standard technologies.

**Key Features:**
- **Hybrid Processing**: Supports both batch and stream processing
- **Multi-Layer Architecture**: Bronze, Silver, Gold data layers
- **Real-time Analytics**: Live dashboards and ML inference
- **Schema Evolution**: Automatic schema management and evolution
- **Fault Tolerance**: Built-in resilience and recovery mechanisms
- **Governance**: Comprehensive data governance and compliance

### Platform Architecture Overview
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│   Kafka Topics   │───▶│  Bronze Layer   │
│  (Applications) │    │  (Event Streams) │    │  (Raw Ingestion)│
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Monitoring &  │◀───│ Stream Processor │───▶│  Silver Layer   │
│   Governance    │    │ (Spark Streaming)│    │ (Cleaned Data)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Real-time ML  │◀───│   Gold Layer     │◀───│   Aggregations  │
│   & Analytics   │    │ (Business Ready) │    │   & Features    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Architecture Components

### Core Components

#### 1. Enterprise Streaming Orchestrator
**File**: `src/streaming/enterprise_streaming_orchestrator.py`

The main orchestrator coordinates all streaming components and manages the platform lifecycle.

**Key Features:**
- Central platform management
- Health monitoring and alerting
- Auto-scaling and resource management
- Pipeline orchestration
- Error handling and recovery

**Configuration:**
```python
from src.streaming.enterprise_streaming_orchestrator import (
    create_platform_config, 
    create_enterprise_streaming_platform
)

config = create_platform_config(
    platform_name="PwC-Enterprise-Streaming-Platform",
    environment="production",
    processing_mode=ProcessingMode.HYBRID,
    kafka_bootstrap_servers=["kafka-1:9092", "kafka-2:9092"],
    enable_bronze_layer=True,
    enable_silver_layer=True,
    enable_gold_layer=True,
    enable_analytics=True,
    enable_governance=True,
    max_concurrent_streams=10,
    trigger_interval="15 seconds"
)

# Create and start platform
with create_enterprise_streaming_platform(config) as orchestrator:
    success = orchestrator.start_platform()
```

#### 2. Kafka Manager
**File**: `src/streaming/kafka_manager.py`

Manages Kafka integration including topic management, producer/consumer operations, and streaming data flow.

**Capabilities:**
- Topic creation and management
- Producer/consumer lifecycle management
- Message serialization/deserialization
- Error handling and retry logic
- Monitoring and metrics collection

#### 3. Spark Structured Streaming
**File**: `src/streaming/spark_structured_streaming.py`

Provides Spark-based stream processing capabilities with Delta Lake integration.

**Features:**
- Real-time data processing
- Fault-tolerant stream processing
- Checkpointing and recovery
- Schema inference and evolution
- Integration with Delta Lake

### Processing Components

#### 1. Bronze Layer Processor
**Purpose**: Raw data ingestion and initial processing

```python
class RealtimeBronzeProcessor:
    """Real-time bronze layer processor for raw data ingestion."""
    
    def __init__(self, spark_session, config, checkpoint_name):
        self.spark = spark_session
        self.config = config
        self.checkpoint_name = checkpoint_name
        
    def start_stream(self, topics):
        """Start streaming from Kafka topics to Bronze layer."""
        
        # Read from Kafka
        stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("subscribe", ",".join(topics))
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
        
        # Process and add metadata
        processed_stream = (
            stream
            .selectExpr(
                "CAST(key AS STRING) as message_key",
                "CAST(value AS STRING) as message_value",
                "topic",
                "partition",
                "offset",
                "timestamp as kafka_timestamp"
            )
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("processing_date", col("kafka_timestamp").cast("date"))
        )
        
        # Write to Delta Lake
        query = (
            processed_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{self.config.checkpoint_location}/bronze")
            .trigger(processingTime=self.config.trigger_interval)
            .start(f"{self.config.output_path}/bronze/realtime_transactions")
        )
        
        return query
```

#### 2. Silver Layer Processor
**File**: `src/streaming/realtime_silver_processor.py`

**Purpose**: Data cleaning, validation, and enrichment

```python
class RealtimeSilverProcessor:
    """Enhanced silver layer processor with comprehensive data quality."""
    
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.data_quality_engine = DataQualityEngine()
        self.enrichment_service = EnrichmentService()
        
    async def process_streaming_data(self, source_stream, output_path, checkpoint_path):
        """Process streaming data through silver layer transformations."""
        
        # Parse JSON messages
        parsed_stream = self.parse_json_messages(source_stream)
        
        # Apply data quality checks
        quality_checked_stream = await self.apply_quality_checks(parsed_stream)
        
        # Data enrichment
        enriched_stream = await self.enrich_data(quality_checked_stream)
        
        # Apply business rules
        business_rules_stream = self.apply_business_rules(enriched_stream)
        
        # Write to Silver layer
        query = (
            business_rules_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=self.config.trigger_interval)
            .start(output_path)
        )
        
        return query
    
    def parse_json_messages(self, stream):
        """Parse JSON messages and extract structured data."""
        
        # Define schema for incoming data
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("transaction_date", TimestampType(), True),
            StructField("country", StringType(), True)
        ])
        
        # Parse JSON and extract fields
        parsed_stream = (
            stream
            .select(
                col("message_key"),
                from_json(col("message_value"), transaction_schema).alias("data"),
                col("kafka_timestamp"),
                col("ingestion_timestamp")
            )
            .select("data.*", "kafka_timestamp", "ingestion_timestamp")
        )
        
        return parsed_stream
```

#### 3. Gold Layer Processor
**File**: `src/streaming/realtime_gold_processor.py`

**Purpose**: Business-ready aggregations and analytics

```python
class RealtimeGoldProcessor:
    """Gold layer processor for business-ready aggregations."""
    
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        
    def start_all_aggregations(self, source_stream, output_base_path, checkpoint_base_path):
        """Start all gold layer aggregations."""
        
        aggregation_queries = {}
        
        # 1. Real-time sales metrics
        sales_metrics_query = self.create_sales_metrics_aggregation(
            source_stream, 
            f"{output_base_path}/sales_metrics",
            f"{checkpoint_base_path}/sales_metrics"
        )
        aggregation_queries['sales_metrics'] = sales_metrics_query
        
        # 2. Customer analytics
        customer_analytics_query = self.create_customer_analytics_aggregation(
            source_stream,
            f"{output_base_path}/customer_analytics", 
            f"{checkpoint_base_path}/customer_analytics"
        )
        aggregation_queries['customer_analytics'] = customer_analytics_query
        
        # 3. Product performance
        product_performance_query = self.create_product_performance_aggregation(
            source_stream,
            f"{output_base_path}/product_performance",
            f"{checkpoint_base_path}/product_performance"
        )
        aggregation_queries['product_performance'] = product_performance_query
        
        return aggregation_queries
    
    def create_sales_metrics_aggregation(self, stream, output_path, checkpoint_path):
        """Create real-time sales metrics aggregation."""
        
        # Define time windows
        windowed_stream = (
            stream
            .withWatermark("transaction_date", "10 minutes")
            .groupBy(
                window(col("transaction_date"), "5 minutes"),
                col("country")
            )
            .agg(
                count("*").alias("transaction_count"),
                sum(col("quantity") * col("unit_price")).alias("total_revenue"),
                avg(col("quantity") * col("unit_price")).alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_customers")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("country"),
                col("transaction_count"),
                col("total_revenue"),
                col("avg_order_value"),
                col("unique_customers"),
                current_timestamp().alias("processing_timestamp")
            )
        )
        
        # Write to Gold layer
        query = (
            windowed_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="30 seconds")
            .start(output_path)
        )
        
        return query
```

## Data Processing Layers

### Medallion Architecture

#### Bronze Layer (Raw Data)
**Purpose**: Store raw, unprocessed data exactly as received from sources

**Characteristics**:
- Schema-on-read approach
- All source data preserved
- Minimal transformations
- Audit trail maintenance

**Data Format**:
```json
{
  "message_key": "transaction_12345",
  "message_value": "{\"transaction_id\":\"12345\",\"customer_id\":\"CUST_789\"}",
  "topic": "retail-transactions",
  "partition": 0,
  "offset": 154234,
  "kafka_timestamp": "2023-12-01T10:30:00.000Z",
  "ingestion_timestamp": "2023-12-01T10:30:05.123Z",
  "processing_date": "2023-12-01"
}
```

#### Silver Layer (Cleaned Data)
**Purpose**: Cleaned, validated, and enriched data ready for analysis

**Transformations Applied**:
- Data type validation and conversion
- Null value handling
- Duplicate detection and removal
- Data standardization (dates, currencies, etc.)
- Data enrichment with reference data
- Business rule application

**Quality Checks**:
```python
def apply_quality_checks(self, stream):
    """Apply comprehensive data quality checks."""
    
    quality_checks = [
        # Completeness checks
        col("customer_id").isNotNull().alias("customer_id_complete"),
        col("transaction_date").isNotNull().alias("transaction_date_complete"),
        col("quantity").isNotNull().alias("quantity_complete"),
        
        # Validity checks
        (col("quantity") > 0).alias("quantity_valid"),
        (col("unit_price") > 0).alias("unit_price_valid"),
        
        # Consistency checks
        (col("total_amount") == col("quantity") * col("unit_price")).alias("amount_consistent")
    ]
    
    # Add quality flags
    quality_stream = stream.select(
        "*",
        *quality_checks,
        (col("customer_id_complete") & 
         col("transaction_date_complete") & 
         col("quantity_complete") & 
         col("quantity_valid") & 
         col("unit_price_valid") & 
         col("amount_consistent")).alias("quality_passed")
    )
    
    return quality_stream
```

#### Gold Layer (Business Ready)
**Purpose**: Aggregated, business-ready data for analytics and reporting

**Aggregation Types**:
- Time-based aggregations (hourly, daily, monthly)
- Customer analytics (RFM, segments, lifetime value)
- Product performance metrics
- Geographic analytics
- Real-time KPIs and dashboards

## Real-time Processing Pipeline

### Stream Processing Flow
```
Kafka Topics → Bronze Layer → Silver Layer → Gold Layer → Analytics/ML
     │              │            │             │              │
     └── Schema ──── Validation ── Enrichment ── Aggregation ── Serving
```

### Processing Configuration
```python
class StreamingConfig:
    """Comprehensive streaming configuration."""
    
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['kafka-1:9092', 'kafka-2:9092'],
            'auto_offset_reset': 'latest',
            'enable_auto_commit': False,
            'max_poll_records': 10000
        }
        
        self.spark_config = {
            'spark.sql.streaming.checkpointLocation': './checkpoints',
            'spark.sql.streaming.metricsEnabled': 'true',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        }
        
        self.processing_config = {
            'trigger_interval': '15 seconds',
            'watermark_delay': '10 minutes',
            'max_offsets_per_trigger': 10000,
            'output_mode': 'append'
        }
```

### Error Handling and Recovery
```python
class StreamingErrorHandler:
    """Comprehensive error handling for streaming pipelines."""
    
    def __init__(self):
        self.dead_letter_queue = DeadLetterQueue()
        self.alerting_system = AlertingSystem()
        
    async def handle_processing_error(self, error, record_batch):
        """Handle processing errors with proper logging and recovery."""
        
        error_info = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'timestamp': datetime.now(),
            'batch_size': len(record_batch),
            'batch_sample': record_batch[:5]  # First 5 records for debugging
        }
        
        # Log error
        logger.error(f"Streaming processing error: {error_info}")
        
        # Send to dead letter queue
        await self.dead_letter_queue.send_batch(record_batch, error_info)
        
        # Send alert for critical errors
        if self.is_critical_error(error):
            await self.alerting_system.send_alert(
                title="Critical Streaming Error",
                message=f"Streaming pipeline error: {error_info['error_message']}",
                severity="high"
            )
        
        # Attempt recovery based on error type
        recovery_action = self.determine_recovery_action(error)
        if recovery_action:
            await recovery_action()
```

## Stream Processing Engines

### Apache Spark Structured Streaming

#### Session Configuration
```python
def create_spark_session(config):
    """Create optimized Spark session for streaming."""
    
    spark = (
        SparkSession.builder
        .appName(config.app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", config.checkpoint_location)
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
```

#### Stream Processing Patterns

##### 1. Stateless Transformations
```python
def apply_stateless_transformations(stream):
    """Apply stateless transformations to streaming data."""
    
    transformed_stream = (
        stream
        # Add derived columns
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        .withColumn("transaction_year", year(col("transaction_date")))
        .withColumn("transaction_month", month(col("transaction_date")))
        
        # Filter invalid records
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        
        # Standardize data
        .withColumn("country_code", upper(col("country")))
        .withColumn("customer_id", upper(col("customer_id")))
    )
    
    return transformed_stream
```

##### 2. Stateful Operations
```python
def create_customer_session_analysis(stream):
    """Create customer session analysis with stateful operations."""
    
    # Define session window (30 minutes of inactivity)
    session_stream = (
        stream
        .withWatermark("transaction_date", "1 hour")
        .groupBy(
            col("customer_id"),
            session_window(col("transaction_date"), "30 minutes")
        )
        .agg(
            count("*").alias("transactions_in_session"),
            sum("total_amount").alias("session_total"),
            min("transaction_date").alias("session_start"),
            max("transaction_date").alias("session_end")
        )
        .select(
            col("customer_id"),
            col("session_window.start").alias("session_start"),
            col("session_window.end").alias("session_end"),
            col("transactions_in_session"),
            col("session_total"),
            ((col("session_window.end").cast("long") - 
              col("session_window.start").cast("long")) / 60).alias("session_duration_minutes")
        )
    )
    
    return session_stream
```

### Complex Event Processing (CEP)
**File**: `src/streaming/advanced_stream_processor.py`

```python
class AdvancedStreamProcessor:
    """Advanced stream processor with CEP capabilities."""
    
    def __init__(self, config):
        self.config = config
        self.pattern_engine = PatternEngine()
        self.window_manager = WindowManager()
        
    def detect_fraud_patterns(self, transaction_stream):
        """Detect fraud patterns in real-time transactions."""
        
        # Define fraud detection patterns
        fraud_patterns = [
            {
                'name': 'rapid_transactions',
                'pattern': 'A+ B+',  # Multiple transactions followed by multiple high-value transactions
                'within': '5 minutes',
                'conditions': {
                    'A': 'transaction_count > 5',
                    'B': 'unit_price > 1000'
                }
            },
            {
                'name': 'geographic_anomaly',
                'pattern': 'A B',  # Transaction in different countries within short time
                'within': '1 hour',
                'conditions': {
                    'A': 'country = "US"',
                    'B': 'country != "US"'
                }
            }
        ]
        
        # Apply pattern matching
        fraud_alerts = self.pattern_engine.detect_patterns(
            transaction_stream, fraud_patterns
        )
        
        return fraud_alerts
```

## Kafka Integration

### Kafka Configuration
```python
class KafkaStreamingConfig:
    """Kafka configuration for streaming applications."""
    
    def __init__(self):
        self.producer_config = {
            'bootstrap.servers': 'kafka-1:9092,kafka-2:9092',
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 5,
            'buffer.memory': 33554432,
            'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
            'value.serializer': 'org.apache.kafka.connect.json.JsonSerializer'
        }
        
        self.consumer_config = {
            'bootstrap.servers': 'kafka-1:9092,kafka-2:9092',
            'group.id': 'streaming-consumer-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'max.poll.records': 10000,
            'fetch.min.bytes': 1024,
            'fetch.max.wait.ms': 500
        }
        
        self.streaming_config = {
            'kafka.bootstrap.servers': 'kafka-1:9092,kafka-2:9092',
            'subscribe': 'retail-transactions,customer-events,system-events',
            'startingOffsets': 'latest',
            'failOnDataLoss': 'false',
            'maxOffsetsPerTrigger': 10000,
            'kafkaConsumer.pollTimeoutMs': 5000
        }
```

### Topic Management
```python
class StreamingTopicManager:
    """Manage Kafka topics for streaming applications."""
    
    def __init__(self, kafka_config):
        self.admin_client = AdminClient(kafka_config)
        
    async def create_streaming_topics(self):
        """Create necessary topics for streaming pipeline."""
        
        topics = [
            {
                'name': 'retail-transactions',
                'num_partitions': 12,
                'replication_factor': 3,
                'config': {
                    'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                    'cleanup.policy': 'delete',
                    'compression.type': 'snappy'
                }
            },
            {
                'name': 'customer-events',
                'num_partitions': 6,
                'replication_factor': 3,
                'config': {
                    'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    'cleanup.policy': 'delete'
                }
            },
            {
                'name': 'streaming-errors',
                'num_partitions': 3,
                'replication_factor': 3,
                'config': {
                    'retention.ms': str(90 * 24 * 60 * 60 * 1000),  # 90 days
                    'cleanup.policy': 'delete'
                }
            }
        ]
        
        # Create topics
        topic_objects = []
        for topic_config in topics:
            topic = NewTopic(
                topic_config['name'],
                num_partitions=topic_config['num_partitions'],
                replication_factor=topic_config['replication_factor'],
                config=topic_config['config']
            )
            topic_objects.append(topic)
        
        # Submit topic creation requests
        futures = self.admin_client.create_topics(topic_objects)
        
        # Wait for completion
        for topic_name, future in futures.items():
            try:
                future.result()
                print(f"Topic '{topic_name}' created successfully")
            except KafkaException as e:
                print(f"Failed to create topic '{topic_name}': {e}")
```

## Delta Lake & Storage

### Delta Lake Integration
**File**: `src/etl/spark/delta_lake_manager.py`

```python
class StreamingDeltaLakeManager:
    """Delta Lake manager for streaming applications."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def create_streaming_delta_table(self, path, schema, partition_columns=None):
        """Create Delta table optimized for streaming workloads."""
        
        # Build Delta table
        delta_table_builder = (
            DeltaTable.create(self.spark)
            .tableName("streaming_table")
            .location(path)
        )
        
        # Add columns from schema
        for field in schema.fields:
            delta_table_builder = delta_table_builder.addColumn(field.name, field.dataType)
        
        # Add partitioning
        if partition_columns:
            for col in partition_columns:
                delta_table_builder = delta_table_builder.partitionedBy(col)
        
        # Set table properties for streaming optimization
        table_properties = {
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true',
            'delta.logRetentionDuration': 'interval 30 days',
            'delta.deletedFileRetentionDuration': 'interval 7 days'
        }
        
        for prop, value in table_properties.items():
            delta_table_builder = delta_table_builder.property(prop, value)
        
        # Execute table creation
        delta_table = delta_table_builder.execute()
        
        return delta_table
    
    def optimize_streaming_table(self, table_path, partition_column=None):
        """Optimize Delta table for better streaming performance."""
        
        # Load Delta table
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Optimize command
        optimize_cmd = delta_table.optimize()
        
        # Add Z-ORDER optimization if partition column specified
        if partition_column:
            optimize_cmd = optimize_cmd.executeZOrderBy(partition_column)
        else:
            optimize_cmd.executeCompaction()
        
        # Vacuum old files
        delta_table.vacuum(retentionHours=168)  # 7 days
        
        print(f"Optimized Delta table at {table_path}")
```

### Storage Optimization
```python
class StreamingStorageOptimizer:
    """Optimize storage for streaming workloads."""
    
    def __init__(self):
        self.optimization_config = {
            'file_size_target_mb': 128,  # Target file size
            'max_files_per_partition': 1000,
            'auto_compact_enabled': True,
            'z_order_columns': ['customer_id', 'transaction_date']
        }
    
    def configure_streaming_write(self, stream_writer):
        """Configure stream writer for optimal performance."""
        
        optimized_writer = (
            stream_writer
            .option('checkpointLocation', self.get_checkpoint_location())
            .option('delta.autoOptimize.optimizeWrite', 'true')
            .option('delta.autoOptimize.autoCompact', 'true')
            .option('maxFilesPerTrigger', 100)
            .option('maxBytesPerTrigger', '1g')
        )
        
        return optimized_writer
```

## Schema Evolution Management

### Schema Evolution Framework
**File**: `src/streaming/schema_evolution_manager.py`

```python
class SchemaEvolutionManager:
    """Manage schema evolution in streaming applications."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.schema_registry = SchemaRegistry()
        
    async def handle_schema_evolution(self, stream, table_path, evolution_mode='permissive'):
        """Handle schema evolution automatically."""
        
        # Get current table schema
        current_schema = self.get_table_schema(table_path)
        
        # Detect schema changes
        schema_changes = await self.detect_schema_changes(stream, current_schema)
        
        if schema_changes:
            # Apply schema evolution based on mode
            if evolution_mode == 'strict':
                raise SchemaEvolutionError(f"Schema changes detected: {schema_changes}")
            elif evolution_mode == 'permissive':
                evolved_stream = await self.apply_schema_evolution(stream, schema_changes)
                await self.update_table_schema(table_path, evolved_stream.schema)
                return evolved_stream
            elif evolution_mode == 'ignore':
                return stream.select(*[col(field.name) for field in current_schema.fields])
        
        return stream
    
    async def detect_schema_changes(self, stream, current_schema):
        """Detect schema changes in incoming stream."""
        
        # Get sample batch to infer schema
        sample_batch = stream.limit(100).collect()
        if not sample_batch:
            return []
        
        # Infer schema from sample
        sample_df = self.spark.createDataFrame(sample_batch)
        incoming_schema = sample_df.schema
        
        # Compare schemas
        changes = []
        
        # Check for new columns
        current_columns = {field.name: field for field in current_schema.fields}
        incoming_columns = {field.name: field for field in incoming_schema.fields}
        
        for col_name, field in incoming_columns.items():
            if col_name not in current_columns:
                changes.append({
                    'type': 'column_added',
                    'column': col_name,
                    'data_type': str(field.dataType)
                })
            elif current_columns[col_name].dataType != field.dataType:
                changes.append({
                    'type': 'type_changed',
                    'column': col_name,
                    'old_type': str(current_columns[col_name].dataType),
                    'new_type': str(field.dataType)
                })
        
        # Check for removed columns
        for col_name in current_columns:
            if col_name not in incoming_columns:
                changes.append({
                    'type': 'column_removed',
                    'column': col_name
                })
        
        return changes
```

## Change Data Capture (CDC)

### CDC Processor
**File**: `src/streaming/cdc_processor.py`

```python
class CDCProcessor:
    """Process Change Data Capture events in real-time."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.supported_operations = ['INSERT', 'UPDATE', 'DELETE']
        
    def process_cdc_stream(self, cdc_stream, target_table_path):
        """Process CDC events and apply changes to target table."""
        
        # Parse CDC events
        parsed_cdc = self.parse_cdc_events(cdc_stream)
        
        # Apply CDC operations
        def apply_cdc_batch(batch_df, batch_id):
            """Apply CDC operations to Delta table."""
            
            if batch_df.count() == 0:
                return
            
            # Load target Delta table
            target_table = DeltaTable.forPath(self.spark, target_table_path)
            
            # Separate operations
            inserts = batch_df.filter(col("operation") == "INSERT")
            updates = batch_df.filter(col("operation") == "UPDATE")
            deletes = batch_df.filter(col("operation") == "DELETE")
            
            # Apply DELETE operations
            if deletes.count() > 0:
                delete_condition = self.build_merge_condition(deletes, "id")
                target_table.delete(delete_condition)
            
            # Apply UPDATE operations  
            if updates.count() > 0:
                update_condition = self.build_merge_condition(updates, "id")
                (target_table
                 .alias("target")
                 .merge(updates.alias("source"), update_condition)
                 .whenMatchedUpdateAll()
                 .execute())
            
            # Apply INSERT operations
            if inserts.count() > 0:
                insert_condition = self.build_merge_condition(inserts, "id")
                (target_table
                 .alias("target")
                 .merge(inserts.alias("source"), insert_condition)
                 .whenNotMatchedInsertAll()
                 .execute())
            
            print(f"Applied CDC batch {batch_id}: {inserts.count()} inserts, "
                  f"{updates.count()} updates, {deletes.count()} deletes")
        
        # Start CDC processing
        query = (
            parsed_cdc.writeStream
            .foreachBatch(apply_cdc_batch)
            .option("checkpointLocation", f"{target_table_path}/_cdc_checkpoint")
            .trigger(processingTime="30 seconds")
            .start()
        )
        
        return query
    
    def parse_cdc_events(self, stream):
        """Parse CDC events from stream."""
        
        # Define CDC schema
        cdc_schema = StructType([
            StructField("operation", StringType(), False),
            StructField("table", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("primary_key", StringType(), False)
        ])
        
        # Parse CDC JSON
        parsed_stream = (
            stream
            .select(from_json(col("value"), cdc_schema).alias("cdc_data"))
            .select("cdc_data.*")
            .filter(col("operation").isin(self.supported_operations))
        )
        
        return parsed_stream
```

## Stream Analytics & ML

### Real-time ML Inference
**File**: `src/streaming/realtime_analytics_ml.py`

```python
class RealtimeAnalyticsEngine:
    """Real-time analytics and ML inference engine."""
    
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.model_server = ModelServer()
        self.feature_store = FeatureStore()
        
    async def process_streaming_analytics(self, stream, output_path, checkpoint_path):
        """Process streaming data with ML analytics."""
        
        # Feature engineering
        feature_stream = await self.engineer_features(stream)
        
        # Real-time predictions
        ml_stream = await self.apply_ml_predictions(feature_stream)
        
        # Anomaly detection
        anomaly_stream = await self.detect_anomalies(ml_stream)
        
        # Real-time scoring
        scored_stream = self.calculate_real_time_scores(anomaly_stream)
        
        # Write results
        query = (
            scored_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="15 seconds")
            .start(output_path)
        )
        
        return query
    
    async def apply_ml_predictions(self, stream):
        """Apply ML model predictions to streaming data."""
        
        # Define UDF for model inference
        def predict_customer_segment(features):
            """Predict customer segment using ML model."""
            
            # Load model (cached)
            model = self.model_server.get_model("customer_segmentation_v2")
            
            # Make prediction
            prediction = model.predict([features])[0]
            confidence = model.predict_proba([features]).max()
            
            return {
                'segment': prediction,
                'confidence': float(confidence)
            }
        
        # Register UDF
        predict_udf = udf(predict_customer_segment, 
                         StructType([
                             StructField("segment", StringType()),
                             StructField("confidence", DoubleType())
                         ]))
        
        # Apply predictions
        ml_stream = (
            stream
            .withColumn("ml_features", 
                       array(col("recency"), col("frequency"), col("monetary")))
            .withColumn("prediction", predict_udf(col("ml_features")))
            .select("*", 
                   col("prediction.segment").alias("predicted_segment"),
                   col("prediction.confidence").alias("prediction_confidence"))
        )
        
        return ml_stream
```

### Real-time Feature Engineering
```python
class StreamingFeatureEngine:
    """Real-time feature engineering for streaming data."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def create_time_based_features(self, stream):
        """Create time-based features from streaming data."""
        
        feature_stream = (
            stream
            .withColumn("hour_of_day", hour(col("transaction_date")))
            .withColumn("day_of_week", dayofweek(col("transaction_date")))
            .withColumn("day_of_month", dayofmonth(col("transaction_date")))
            .withColumn("month", month(col("transaction_date")))
            .withColumn("quarter", quarter(col("transaction_date")))
            .withColumn("is_weekend", 
                       when(dayofweek(col("transaction_date")).isin([1, 7]), 1).otherwise(0))
            .withColumn("is_business_hours",
                       when((hour(col("transaction_date")) >= 9) & 
                            (hour(col("transaction_date")) <= 17), 1).otherwise(0))
        )
        
        return feature_stream
    
    def create_customer_features(self, stream):
        """Create customer behavior features."""
        
        # Customer aggregations with watermarking
        customer_features = (
            stream
            .withWatermark("transaction_date", "1 hour")
            .groupBy(
                col("customer_id"),
                window(col("transaction_date"), "24 hours")
            )
            .agg(
                count("*").alias("daily_transaction_count"),
                sum("total_amount").alias("daily_total_spent"),
                avg("total_amount").alias("daily_avg_order_value"),
                countDistinct("product_id").alias("daily_unique_products")
            )
            .select(
                col("customer_id"),
                col("daily_transaction_count"),
                col("daily_total_spent"),
                col("daily_avg_order_value"),
                col("daily_unique_products"),
                current_timestamp().alias("feature_timestamp")
            )
        )
        
        return customer_features
```

## Governance & Monitoring

### Streaming Governance Framework
**File**: `src/streaming/streaming_governance.py`

```python
class StreamingGovernanceFramework:
    """Comprehensive governance framework for streaming data."""
    
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.data_catalog = DataCatalog()
        self.lineage_tracker = DataLineageTracker()
        self.quality_monitor = StreamingDataQualityMonitor()
        
    async def apply_governance(self, stream, stream_name):
        """Apply comprehensive governance to streaming data."""
        
        # Data lineage tracking
        lineage_stream = await self.track_data_lineage(stream, stream_name)
        
        # Data quality monitoring
        quality_monitored_stream = await self.monitor_data_quality(lineage_stream)
        
        # Compliance checks
        compliant_stream = await self.apply_compliance_rules(quality_monitored_stream)
        
        # Data classification
        classified_stream = await self.classify_data(compliant_stream)
        
        return classified_stream
    
    async def monitor_data_quality(self, stream):
        """Monitor data quality in real-time."""
        
        # Define quality metrics
        quality_metrics = [
            {
                'name': 'completeness',
                'check': lambda df: 1 - (df.filter(col("customer_id").isNull()).count() / df.count()),
                'threshold': 0.95
            },
            {
                'name': 'validity',
                'check': lambda df: df.filter((col("quantity") > 0) & (col("unit_price") > 0)).count() / df.count(),
                'threshold': 0.98
            },
            {
                'name': 'consistency',
                'check': lambda df: df.filter(col("total_amount") == col("quantity") * col("unit_price")).count() / df.count(),
                'threshold': 0.99
            }
        ]
        
        def quality_check_batch(batch_df, batch_id):
            """Apply quality checks to each batch."""
            
            if batch_df.count() == 0:
                return
            
            quality_results = {}
            
            for metric in quality_metrics:
                try:
                    score = metric['check'](batch_df)
                    quality_results[metric['name']] = {
                        'score': score,
                        'passed': score >= metric['threshold'],
                        'threshold': metric['threshold']
                    }
                    
                    # Send alert if quality threshold not met
                    if score < metric['threshold']:
                        await self.send_quality_alert(metric['name'], score, metric['threshold'])
                        
                except Exception as e:
                    quality_results[metric['name']] = {
                        'error': str(e),
                        'passed': False
                    }
            
            # Log quality metrics
            await self.log_quality_metrics(batch_id, quality_results)
        
        # Apply quality monitoring
        quality_stream = (
            stream.writeStream
            .foreachBatch(quality_check_batch)
            .option("checkpointLocation", f"{self.config.checkpoint_location}/quality_monitor")
            .start()
        )
        
        return stream  # Return original stream for further processing
```

### Stream Monitoring Dashboard
```python
class StreamingMonitoringDashboard:
    """Real-time monitoring dashboard for streaming pipelines."""
    
    def __init__(self):
        self.metrics_collector = MetricsCollector()
        self.alerting_system = AlertingSystem()
        
    async def collect_pipeline_metrics(self, streaming_queries):
        """Collect comprehensive pipeline metrics."""
        
        pipeline_metrics = {}
        
        for query_name, query in streaming_queries.items():
            if query.isActive:
                progress = query.lastProgress
                
                metrics = {
                    'query_id': query.id,
                    'batch_id': progress.get('batchId', -1),
                    'input_rows_per_second': progress.get('inputRowsPerSecond', 0),
                    'processed_rows_per_second': progress.get('processedRowsPerSecond', 0),
                    'batch_duration_ms': progress.get('batchDuration', 0),
                    'num_input_rows': progress.get('numInputRows', 0),
                    'status': 'active'
                }
            else:
                metrics = {
                    'status': 'inactive',
                    'exception': query.exception.description if query.exception else None
                }
            
            pipeline_metrics[query_name] = metrics
        
        # Store metrics for dashboard
        await self.metrics_collector.store_pipeline_metrics(pipeline_metrics)
        
        return pipeline_metrics
```

## Performance Optimization

### Stream Processing Optimization
```python
class StreamingOptimizer:
    """Optimize streaming performance and resource utilization."""
    
    def __init__(self):
        self.optimization_strategies = {
            'partitioning': self.optimize_partitioning,
            'caching': self.optimize_caching,
            'checkpointing': self.optimize_checkpointing,
            'resource_allocation': self.optimize_resource_allocation
        }
    
    def optimize_partitioning(self, stream, partition_columns):
        """Optimize stream partitioning for better performance."""
        
        # Repartition stream based on key columns
        if partition_columns:
            optimized_stream = (
                stream
                .repartition(*[col(c) for c in partition_columns])
            )
        else:
            # Use default partitioning based on cluster size
            num_partitions = self.calculate_optimal_partitions()
            optimized_stream = stream.repartition(num_partitions)
        
        return optimized_stream
    
    def optimize_caching(self, stream, cache_level="MEMORY_AND_DISK_SER"):
        """Optimize caching strategy for streaming data."""
        
        from pyspark import StorageLevel
        
        cache_levels = {
            "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
            "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
            "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER,
            "DISK_ONLY": StorageLevel.DISK_ONLY
        }
        
        cached_stream = stream.cache()
        cached_stream.persist(cache_levels.get(cache_level, StorageLevel.MEMORY_AND_DISK_SER))
        
        return cached_stream
    
    def calculate_optimal_batch_size(self, input_rate, processing_capacity):
        """Calculate optimal batch size for streaming workload."""
        
        # Basic formula: batch_size = min(input_rate * trigger_interval, max_batch_size)
        trigger_interval_seconds = 30  # 30 seconds default
        max_batch_size = 1000000  # 1M records max
        
        calculated_batch_size = min(
            int(input_rate * trigger_interval_seconds),
            max_batch_size,
            processing_capacity
        )
        
        return max(calculated_batch_size, 1000)  # Minimum 1K records
```

### Resource Management
```python
class StreamingResourceManager:
    """Manage resources for streaming applications."""
    
    def __init__(self):
        self.resource_config = {
            'executor_memory': '4g',
            'executor_cores': 2,
            'num_executors': 4,
            'driver_memory': '2g',
            'max_result_size': '1g'
        }
    
    def configure_spark_resources(self, spark_session, workload_type='standard'):
        """Configure Spark resources based on workload type."""
        
        resource_configs = {
            'lightweight': {
                'spark.executor.memory': '2g',
                'spark.executor.cores': '2',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '64MB'
            },
            'standard': {
                'spark.executor.memory': '4g',
                'spark.executor.cores': '3',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB'
            },
            'heavy': {
                'spark.executor.memory': '8g',
                'spark.executor.cores': '4',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '256MB'
            }
        }
        
        config = resource_configs.get(workload_type, resource_configs['standard'])
        
        for key, value in config.items():
            spark_session.conf.set(key, value)
```

## Troubleshooting Guide

### Common Issues and Solutions

#### 1. High Processing Latency
**Symptoms**: Increasing batch processing times, falling behind

**Diagnosis**:
```python
def diagnose_processing_latency(query):
    """Diagnose processing latency issues."""
    
    progress = query.lastProgress
    
    issues = []
    
    # Check batch duration vs trigger interval
    batch_duration = progress.get('batchDuration', 0)
    trigger_interval_ms = 30000  # 30 seconds
    
    if batch_duration > trigger_interval_ms:
        issues.append(f"Batch duration ({batch_duration}ms) exceeds trigger interval")
    
    # Check input vs processing rate
    input_rate = progress.get('inputRowsPerSecond', 0)
    processing_rate = progress.get('processedRowsPerSecond', 0)
    
    if input_rate > processing_rate * 1.2:  # 20% buffer
        issues.append(f"Input rate ({input_rate}) exceeds processing capacity ({processing_rate})")
    
    return issues
```

**Solutions**:
- Increase parallelism (more executors/cores)
- Optimize transformations (reduce shuffling)
- Implement micro-batching
- Use data partitioning strategies

#### 2. Memory Issues
**Symptoms**: OutOfMemoryError, frequent garbage collection

**Solutions**:
```python
def optimize_memory_configuration():
    """Memory optimization strategies."""
    
    memory_optimizations = {
        # Increase executor memory
        'spark.executor.memory': '8g',
        
        # Tune memory fractions
        'spark.sql.execution.memory.fraction': '0.8',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        
        # Enable off-heap storage
        'spark.sql.columnVector.offheap.enabled': 'true',
        
        # Optimize shuffle
        'spark.sql.adaptive.shuffle.targetPostShuffleInputSize': '64MB',
        'spark.sql.adaptive.skewJoin.enabled': 'true'
    }
    
    return memory_optimizations
```

#### 3. Data Quality Issues
**Symptoms**: Inconsistent data, processing failures

**Monitoring**:
```python
def setup_data_quality_monitoring():
    """Set up comprehensive data quality monitoring."""
    
    quality_checks = [
        {
            'name': 'null_check',
            'sql': 'SELECT COUNT(*) FROM temp_view WHERE customer_id IS NULL',
            'threshold': 0,
            'operator': 'eq'
        },
        {
            'name': 'range_check',
            'sql': 'SELECT COUNT(*) FROM temp_view WHERE quantity <= 0',
            'threshold': 0,
            'operator': 'eq'
        },
        {
            'name': 'consistency_check',
            'sql': 'SELECT COUNT(*) FROM temp_view WHERE total_amount != quantity * unit_price',
            'threshold': 0,
            'operator': 'eq'
        }
    ]
    
    return quality_checks
```

#### 4. Checkpoint Issues
**Symptoms**: Processing restarts from beginning, data loss

**Solutions**:
- Use reliable checkpoint locations (HDFS, S3)
- Monitor checkpoint directory size
- Clean up old checkpoint files
- Implement checkpoint recovery procedures

### Monitoring and Alerting
```python
class StreamingAlertSystem:
    """Comprehensive alerting system for streaming applications."""
    
    def __init__(self):
        self.alert_rules = self.define_alert_rules()
        
    def define_alert_rules(self):
        """Define comprehensive alert rules."""
        
        return [
            {
                'name': 'processing_delay',
                'condition': 'batch_duration > trigger_interval * 2',
                'severity': 'high',
                'action': 'scale_up_resources'
            },
            {
                'name': 'data_quality_failure',
                'condition': 'quality_score < 0.95',
                'severity': 'medium',
                'action': 'investigate_data_source'
            },
            {
                'name': 'stream_failure',
                'condition': 'query_status == "failed"',
                'severity': 'critical',
                'action': 'restart_stream'
            },
            {
                'name': 'low_throughput',
                'condition': 'processed_rows_per_second < expected_rate * 0.8',
                'severity': 'medium',
                'action': 'optimize_processing'
            }
        ]
```

### Best Practices

#### 1. Stream Design
- Design idempotent operations
- Use appropriate partitioning strategies
- Implement proper error handling
- Plan for schema evolution

#### 2. Performance
- Monitor and optimize regularly
- Use appropriate trigger intervals
- Implement backpressure handling
- Optimize resource allocation

#### 3. Reliability
- Use checkpointing effectively
- Implement circuit breakers
- Plan for failure scenarios
- Monitor data quality continuously

#### 4. Operations
- Implement comprehensive monitoring
- Set up proper alerting
- Document troubleshooting procedures
- Plan for capacity scaling

This streaming architecture guide provides a comprehensive foundation for building and operating enterprise-scale real-time data processing systems with proper governance, monitoring, and optimization strategies.