# Comprehensive ETL Pipeline Documentation

## 📋 Table of Contents

1. [ETL Architecture Overview](#etl-architecture-overview)
2. [Medallion Architecture (Bronze-Silver-Gold)](#medallion-architecture)
3. [Multi-Engine Processing Framework](#multi-engine-processing-framework)
4. [Data Flow Documentation](#data-flow-documentation)
5. [Pipeline Configuration](#pipeline-configuration)
6. [Data Quality Framework](#data-quality-framework)
7. [Monitoring & Observability](#monitoring--observability)
8. [Performance Optimization](#performance-optimization)
9. [Troubleshooting Guide](#troubleshooting-guide)
10. [Best Practices](#best-practices)

## 🏗️ ETL Architecture Overview

The PwC Data Engineering Platform implements a modern, scalable ETL architecture using the Medallion pattern with multi-engine processing support.

### High-Level Architecture
```mermaid
graph TB
    subgraph "Data Sources"
        EXCEL[Excel Files]
        API[External APIs]
        DB[Databases]
        STREAM[Streaming Data]
    end
    
    subgraph "Ingestion Layer"
        BRONZE[Bronze Layer<br/>Raw Data]
        VALIDATION[Data Validation]
        SCHEMA[Schema Detection]
    end
    
    subgraph "Processing Layer"
        PANDAS[Pandas Engine<br/>Small Data]
        SPARK[Spark Engine<br/>Big Data]
        POLARS[Polars Engine<br/>Fast Processing]
    end
    
    subgraph "Transformation Layer"
        SILVER[Silver Layer<br/>Cleansed Data]
        QUALITY[Data Quality Checks]
        ENRICHMENT[Data Enrichment]
    end
    
    subgraph "Analytics Layer"
        GOLD[Gold Layer<br/>Business Ready]
        STAR[Star Schema]
        AGGREGATES[Aggregated Views]
    end
    
    subgraph "Output Layer"
        DWH[Data Warehouse]
        API_OUT[API Endpoints]
        REPORTS[Reports & Dashboards]
    end
    
    EXCEL --> BRONZE
    API --> BRONZE  
    DB --> BRONZE
    STREAM --> BRONZE
    
    BRONZE --> VALIDATION
    VALIDATION --> SCHEMA
    
    SCHEMA --> PANDAS
    SCHEMA --> SPARK
    SCHEMA --> POLARS
    
    PANDAS --> SILVER
    SPARK --> SILVER
    POLARS --> SILVER
    
    SILVER --> QUALITY
    QUALITY --> ENRICHMENT
    ENRICHMENT --> GOLD
    
    GOLD --> STAR
    STAR --> AGGREGATES
    AGGREGATES --> DWH
    
    DWH --> API_OUT
    DWH --> REPORTS
```

### Key Components

#### 1. Data Ingestion Framework
- **Source Connectors**: Excel, CSV, JSON, APIs, databases
- **Schema Detection**: Automatic schema inference and validation
- **Format Handlers**: Multi-format support with unified interface
- **Error Handling**: Graceful failure handling with retry mechanisms

#### 2. Multi-Engine Processing
- **Pandas**: For datasets < 10GB, development, and prototyping
- **Apache Spark**: For datasets > 10GB, distributed processing
- **Polars**: For high-performance single-machine processing

#### 3. Storage Layer
- **Bronze Layer**: Raw data storage with Delta Lake ACID transactions
- **Silver Layer**: Cleansed and validated data
- **Gold Layer**: Business-ready analytics data

## 🥉🥈🥇 Medallion Architecture

### Bronze Layer (Raw Data)
```python
# Location: src/etl/bronze/
# Purpose: Ingest raw data with minimal transformation

from etl.bronze.enhanced_bronze import EnhancedBronzeProcessor

processor = EnhancedBronzeProcessor(
    source_path="data/raw/online_retail_II.xlsx",
    target_path="data/bronze/retail_transactions",
    engine="pandas",  # or "spark", "polars"
    options={
        "preserve_raw_schema": True,
        "add_ingestion_metadata": True,
        "enable_schema_evolution": True
    }
)

result = processor.process()
```

#### Bronze Layer Features
- **Schema Preservation**: Maintains original data structure
- **Metadata Addition**: Adds ingestion timestamp, source info
- **Error Tolerance**: Continues processing with invalid records
- **Audit Trail**: Complete lineage and processing history

### Silver Layer (Cleansed Data)
```python
# Location: src/etl/silver/
# Purpose: Data cleaning, validation, and standardization

from etl.silver.enhanced_silver import EnhancedSilverProcessor

processor = EnhancedSilverProcessor(
    source_path="data/bronze/retail_transactions",
    target_path="data/silver/clean_transactions",
    engine="spark",
    transformations=[
        "standardize_dates",
        "validate_customer_ids", 
        "clean_product_descriptions",
        "normalize_currencies",
        "detect_duplicates"
    ]
)

result = processor.process()
```

#### Silver Layer Transformations
- **Data Cleaning**: Handle missing values, outliers, duplicates
- **Standardization**: Consistent formats, naming conventions
- **Validation**: Business rule validation, data quality checks
- **Enrichment**: External API data enrichment (country codes, currencies)

### Gold Layer (Business Ready)
```python
# Location: src/etl/gold/
# Purpose: Business-ready analytics with star schema

from etl.gold.star_schema_builder import StarSchemaBuilder

builder = StarSchemaBuilder(
    source_path="data/silver/clean_transactions", 
    target_path="data/gold/star_schema",
    schema_type="retail_analytics",
    engine="spark"
)

# Build dimensional model
star_schema = builder.build_star_schema([
    "dim_customer",
    "dim_product", 
    "dim_time",
    "fact_sales"
])
```

#### Gold Layer Features
- **Star Schema**: Optimized for analytics queries
- **Aggregations**: Pre-calculated business metrics
- **Historical Tracking**: SCD Type 2 for dimension changes
- **Performance**: Partitioned and indexed for fast queries

## ⚡ Multi-Engine Processing Framework

### Engine Selection Strategy
```python
from etl.framework.engine_selector import EngineSelector

selector = EngineSelector()

# Automatic engine selection based on data characteristics
engine = selector.select_engine(
    data_size_gb=25.5,
    complexity_score=0.75,
    memory_available_gb=16,
    processing_time_limit_minutes=30
)

print(f"Selected engine: {engine}")  # Output: "spark"
```

### Engine Comparison Matrix

| Feature | Pandas | Polars | Spark |
|---------|--------|--------|-------|
| **Data Size Limit** | < 10GB | < 100GB | Unlimited |
| **Memory Usage** | High | Low | Distributed |
| **Processing Speed** | Medium | Fast | Variable |
| **Learning Curve** | Easy | Medium | Complex |
| **Distributed** | No | No | Yes |
| **Best For** | Prototyping | Single-machine | Big Data |

### Pandas Engine Configuration
```python
from etl.framework.pandas_processor import PandasProcessor

processor = PandasProcessor(
    source="data/bronze/retail_transactions",
    target="data/silver/clean_transactions",
    config={
        "chunk_size": 10000,
        "memory_efficient": True,
        "parallel_processing": True,
        "optimize_dtypes": True
    }
)
```

### Spark Engine Configuration
```python
from etl.framework.spark_processor import SparkProcessor

processor = SparkProcessor(
    source="data/bronze/retail_transactions",
    target="data/silver/clean_transactions", 
    config={
        "master": "local[*]",
        "executor_memory": "4g",
        "driver_memory": "2g",
        "sql_shuffle_partitions": 200,
        "adaptive_query_execution": True,
        "dynamic_partition_pruning": True
    }
)
```

### Polars Engine Configuration
```python
from etl.framework.polars_processor import PolarsProcessor

processor = PolarsProcessor(
    source="data/bronze/retail_transactions",
    target="data/silver/clean_transactions",
    config={
        "lazy_evaluation": True,
        "streaming_mode": True,
        "optimize_predicates": True,
        "parallel_execution": True
    }
)
```

## 📊 Data Flow Documentation

### Complete ETL Pipeline Flow
```mermaid
sequenceDiagram
    participant S as Data Source
    participant B as Bronze Layer
    participant Q as Quality Check
    participant Sv as Silver Layer
    participant E as Enrichment
    participant G as Gold Layer
    participant DW as Data Warehouse
    participant API as API Layer

    S->>B: Raw data ingestion
    Note over B: Schema detection<br/>Metadata addition
    
    B->>Q: Data validation
    Q->>Q: Check completeness<br/>Validate business rules
    
    Q->>Sv: Clean data
    Note over Sv: Standardization<br/>Deduplication<br/>Normalization
    
    Sv->>E: Enrichment process
    Note over E: External API calls<br/>Reference data lookup
    
    E->>G: Business transformation
    Note over G: Star schema<br/>Aggregations<br/>KPIs
    
    G->>DW: Load to warehouse
    DW->>API: Expose via API
```

### Data Lineage Tracking
```python
from core.lineage.data_lineage_tracker import DataLineageTracker

tracker = DataLineageTracker()

# Track data transformation
tracker.track_transformation(
    source_table="bronze.retail_transactions",
    target_table="silver.clean_transactions", 
    transformation_type="cleaning",
    engine="spark",
    columns_mapping={
        "CustomerID": "customer_id",
        "InvoiceDate": "transaction_date"
    },
    business_rules_applied=[
        "remove_cancelled_transactions",
        "standardize_date_format",
        "validate_customer_ids"
    ]
)

# Generate lineage graph
lineage_graph = tracker.generate_lineage_graph("gold.fact_sales")
```

## ⚙️ Pipeline Configuration

### Master Configuration File
```yaml
# config/etl_pipeline.yaml
pipeline:
  name: "retail_etl_pipeline"
  version: "2.0.0"
  description: "Comprehensive retail analytics ETL pipeline"

sources:
  primary:
    type: "excel"
    path: "data/raw/online_retail_II.xlsx" 
    sheet_name: "Online Retail"
    encoding: "utf-8"
  
  enrichment:
    currency_api:
      enabled: true
      endpoint: "https://api.exchangerate-api.com/v4/latest/"
      rate_limit: 1000
    
    country_api:
      enabled: true
      endpoint: "https://restcountries.com/v3.1/"

processing:
  bronze:
    engine: "pandas"  # auto, pandas, spark, polars
    options:
      chunk_size: 50000
      preserve_schema: true
      add_metadata: true
  
  silver:
    engine: "auto"
    transformations:
      - "clean_missing_values"
      - "standardize_formats"
      - "validate_business_rules"
      - "enrich_external_data"
    
    quality_checks:
      completeness_threshold: 0.95
      accuracy_threshold: 0.98
      consistency_checks: true
  
  gold:
    engine: "spark"
    schema_type: "star"
    optimizations:
      - "partition_by_date"
      - "create_indexes"
      - "generate_aggregates"

storage:
  bronze_path: "data/bronze"
  silver_path: "data/silver" 
  gold_path: "data/gold"
  format: "delta"  # delta, parquet, iceberg
  
  retention_policy:
    bronze_days: 365
    silver_days: 730
    gold_days: 2555  # 7 years

monitoring:
  enabled: true
  metrics:
    - "processing_time"
    - "data_quality_scores"
    - "record_counts"
    - "error_rates"
  
  alerts:
    - name: "data_quality_degradation"
      condition: "quality_score < 0.95"
      severity: "warning"
    
    - name: "pipeline_failure"
      condition: "status == 'failed'"
      severity: "critical"
```

### Environment-Specific Configurations
```python
# config/environments.py
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class EnvironmentConfig:
    database_url: str
    spark_config: Dict[str, Any]
    monitoring_enabled: bool
    debug_mode: bool

environments = {
    "development": EnvironmentConfig(
        database_url="sqlite:///data/warehouse/retail_dev.db",
        spark_config={
            "master": "local[2]",
            "executor_memory": "2g"
        },
        monitoring_enabled=False,
        debug_mode=True
    ),
    
    "production": EnvironmentConfig(
        database_url="postgresql://user:pass@prod-db:5432/retail",
        spark_config={
            "master": "spark://cluster:7077",
            "executor_memory": "8g",
            "executor_instances": 10
        },
        monitoring_enabled=True,
        debug_mode=False
    )
}
```

## ✅ Data Quality Framework

### Data Quality Dimensions
```python
from domain.validators.advanced_data_quality import AdvancedDataQualityValidator

validator = AdvancedDataQualityValidator()

# Comprehensive quality assessment
quality_report = validator.assess_quality(
    dataframe=df,
    dimensions=[
        "completeness",    # Missing value analysis
        "accuracy",        # Data type and format validation  
        "consistency",     # Cross-field consistency checks
        "validity",        # Business rule validation
        "uniqueness",      # Duplicate detection
        "timeliness"       # Data freshness checks
    ]
)

print(f"Overall Quality Score: {quality_report.overall_score}")
```

### Quality Checks Implementation
```python
from etl.transformations.data_quality import DataQualityChecker

checker = DataQualityChecker(
    rules_config="config/data_quality_rules.yaml"
)

# Define quality rules
quality_rules = [
    {
        "name": "customer_id_not_null",
        "check_type": "completeness",
        "column": "customer_id",
        "condition": "not_null",
        "threshold": 1.0
    },
    {
        "name": "transaction_amount_positive",
        "check_type": "validity", 
        "column": "unit_price",
        "condition": "greater_than",
        "value": 0,
        "threshold": 0.95
    },
    {
        "name": "invoice_date_recent",
        "check_type": "timeliness",
        "column": "invoice_date",
        "condition": "within_days",
        "value": 730,
        "threshold": 0.90
    }
]

# Execute quality checks
results = checker.run_checks(df, quality_rules)
```

### Great Expectations Integration
```python
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Initialize Great Expectations context
context = gx.get_context()

# Create expectation suite
suite = context.create_expectation_suite(
    expectation_suite_name="retail_data_quality_suite",
    overwrite_existing=True
)

# Add expectations
suite.expect_column_to_exist("customer_id")
suite.expect_column_values_to_not_be_null("customer_id")
suite.expect_column_values_to_be_of_type("unit_price", "float")
suite.expect_column_values_to_be_between("quantity", min_value=1, max_value=1000)

# Validate data
batch_request = RuntimeBatchRequest(
    datasource_name="pandas_datasource",
    data_connector_name="runtime_data_connector",
    data_asset_name="retail_transactions",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "default_identifier"}
)

results = context.run_checkpoint(
    checkpoint_name="retail_data_checkpoint",
    batch_request=batch_request
)
```

## 📊 Monitoring & Observability

### Pipeline Metrics Collection
```python
from monitoring.etl_comprehensive_observability import ETLObservabilityManager

observability = ETLObservabilityManager(
    pipeline_name="retail_etl",
    environment="production"
)

# Track pipeline execution
with observability.track_pipeline_execution() as tracker:
    # Bronze layer processing
    with tracker.track_stage("bronze_processing") as stage:
        stage.record_metric("records_processed", 1000000)
        stage.record_metric("processing_time_seconds", 120)
        stage.record_metric("memory_usage_mb", 2048)
    
    # Silver layer processing  
    with tracker.track_stage("silver_processing") as stage:
        stage.record_metric("records_processed", 995000)
        stage.record_metric("records_rejected", 5000)
        stage.record_metric("data_quality_score", 0.97)
    
    # Gold layer processing
    with tracker.track_stage("gold_processing") as stage:
        stage.record_metric("records_processed", 995000)
        stage.record_metric("aggregations_created", 50)
        stage.record_metric("star_schema_tables", 4)
```

### Real-time Pipeline Dashboard
```python
from monitoring.dashboard import create_etl_dashboard
import streamlit as st

def create_pipeline_dashboard():
    st.title("ETL Pipeline Monitoring Dashboard")
    
    # Pipeline status overview
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Pipeline Status", "✅ Healthy")
    with col2:
        st.metric("Records Processed Today", "2.5M")
    with col3:
        st.metric("Data Quality Score", "97.2%")  
    with col4:
        st.metric("Processing Time", "8.5 min")
    
    # Performance metrics chart
    performance_data = get_performance_metrics()
    st.line_chart(performance_data)
    
    # Data quality trends
    quality_data = get_quality_metrics()
    st.area_chart(quality_data)
    
    # Error logs
    st.subheader("Recent Issues")
    error_logs = get_error_logs(limit=10)
    st.dataframe(error_logs)
```

### DataDog Integration
```python
from monitoring.datadog_custom_metrics_advanced import AdvancedDataDogMetrics

datadog = AdvancedDataDogMetrics()

# Custom ETL metrics
datadog.send_metrics([
    {
        "metric": "etl.pipeline.processing_time",
        "value": processing_time_seconds,
        "tags": ["pipeline:retail", "stage:bronze", "engine:spark"]
    },
    {
        "metric": "etl.data_quality.score", 
        "value": quality_score,
        "tags": ["pipeline:retail", "layer:silver"]
    },
    {
        "metric": "etl.records.processed",
        "value": record_count,
        "tags": ["pipeline:retail", "layer:gold"]
    }
])
```

## 🚀 Performance Optimization

### Spark Performance Tuning
```python
# Spark configuration for optimal performance
spark_config = {
    # Memory management
    "spark.executor.memory": "8g",
    "spark.executor.memoryFraction": "0.8",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    
    # Shuffling optimization
    "spark.sql.shuffle.partitions": "400",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    
    # Caching and storage
    "spark.sql.inMemoryColumnarStorage.compressed": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    
    # Delta Lake optimizations
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true"
}
```

### Polars Optimization Strategies
```python
import polars as pl

# Lazy evaluation for memory efficiency
lazy_df = (
    pl.scan_csv("data/bronze/retail_transactions.csv")
    .filter(pl.col("quantity") > 0)
    .with_columns([
        pl.col("invoice_date").str.to_datetime(),
        (pl.col("quantity") * pl.col("unit_price")).alias("total_amount")
    ])
    .group_by("customer_id")
    .agg([
        pl.col("total_amount").sum().alias("total_spent"),
        pl.col("invoice_date").max().alias("last_purchase")
    ])
)

# Execute only when needed
result = lazy_df.collect()
```

### Partitioning Strategies
```python
from etl.utils.partitioning import PartitioningStrategy

# Date-based partitioning for time-series data
partitioner = PartitioningStrategy(
    partition_columns=["year", "month"],
    partition_strategy="hive_style"
)

# Write partitioned data
partitioner.write_partitioned_data(
    df=transactions_df,
    output_path="data/silver/transactions",
    format="delta"
)

# Optimize partition pruning
optimized_df = (
    spark.read.format("delta")
    .load("data/silver/transactions")
    .filter(
        (col("year") == 2025) & 
        (col("month").isin([8, 9, 10]))
    )
)
```

## 🔧 Troubleshooting Guide

### Common Issues and Solutions

#### 1. Memory Issues with Large Datasets
**Problem**: OutOfMemoryError during processing
**Solution**:
```python
# Increase JVM memory for Spark
spark_config = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "8g", 
    "spark.driver.maxResultSize": "2g"
}

# Use chunked processing for Pandas
def process_in_chunks(file_path, chunk_size=10000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        processed_chunk = transform_data(chunk)
        yield processed_chunk
```

#### 2. Schema Evolution Issues
**Problem**: Schema changes breaking pipeline
**Solution**:
```python
from etl.utils.schema_evolution import SchemaEvolutionHandler

handler = SchemaEvolutionHandler()

# Handle schema changes gracefully
evolved_df = handler.evolve_schema(
    current_df=new_data,
    target_schema=existing_schema,
    evolution_mode="merge"  # add_columns, merge, strict
)
```

#### 3. Data Quality Failures  
**Problem**: Pipeline failing due to quality issues
**Solution**:
```python
# Implement graceful degradation
try:
    quality_score = run_quality_checks(df)
    if quality_score < 0.90:
        # Log warning but continue processing
        logger.warning(f"Quality score {quality_score} below threshold")
        send_alert("data_quality_degraded", quality_score)
    
    # Continue with processing
    result = process_data(df)
    
except DataQualityException as e:
    # Handle critical quality failures
    logger.error(f"Critical quality failure: {e}")
    send_critical_alert("pipeline_failure", str(e))
    raise
```

#### 4. Performance Bottlenecks
**Problem**: Slow pipeline execution
**Solution**:
```python
from monitoring.performance_profiler import ETLProfiler

profiler = ETLProfiler()

with profiler.profile_execution():
    # Identify bottlenecks
    with profiler.profile_stage("data_loading"):
        df = load_data(source_path)
    
    with profiler.profile_stage("transformations"):
        clean_df = transform_data(df)
    
    with profiler.profile_stage("data_writing"):
        save_data(clean_df, target_path)

# Generate performance report
report = profiler.generate_report()
print(report.bottlenecks)
```

### Debugging Tools
```python
# ETL debugging utilities
from etl.utils.debugging import ETLDebugger

debugger = ETLDebugger()

# Data profiling
profile = debugger.profile_dataframe(df)
print(f"Memory usage: {profile['memory_usage_mb']} MB")
print(f"Null percentages: {profile['null_percentages']}")

# Transformation tracing  
debugger.trace_transformations(
    input_df=raw_df,
    output_df=clean_df,
    transformations=["clean_nulls", "standardize_dates"]
)

# Performance analysis
perf_analysis = debugger.analyze_performance(
    processing_stats=stats
)
```

## 📝 Best Practices

### 1. Data Pipeline Design
- **Idempotent Operations**: Ensure pipelines can be re-run safely
- **Incremental Processing**: Process only new/changed data when possible
- **Error Handling**: Implement comprehensive error handling and recovery
- **Monitoring**: Add observability at every pipeline stage

### 2. Code Organization  
```
src/etl/
├── framework/           # Multi-engine processing framework
│   ├── base_processor.py
│   ├── pandas_processor.py
│   ├── spark_processor.py
│   └── polars_processor.py
├── bronze/             # Raw data ingestion
├── silver/             # Data cleaning and validation  
├── gold/               # Business-ready transformations
├── transformations/    # Reusable transformation functions
├── utils/              # Utility functions
└── tests/              # Comprehensive test suite
```

### 3. Configuration Management
- Use environment-specific configurations
- Version control all configuration files
- Implement configuration validation
- Support runtime configuration updates

### 4. Testing Strategy
```python
# Unit tests for transformations
def test_customer_id_cleaning():
    input_df = create_test_dataframe()
    result = clean_customer_ids(input_df) 
    assert result["customer_id"].isnull().sum() == 0

# Integration tests for full pipeline
def test_end_to_end_pipeline():
    pipeline = RetailETLPipeline()
    result = pipeline.run(test_data_path)
    assert result.status == "success"
    assert result.quality_score > 0.95

# Data quality tests
def test_data_quality_constraints():
    df = load_test_data()
    quality_checks = run_quality_checks(df)
    assert all(check.passed for check in quality_checks)
```

### 5. Documentation Standards
- Document all transformation logic
- Maintain data dictionaries  
- Create pipeline flow diagrams
- Write troubleshooting guides
- Keep runbooks updated

---

**Last Updated**: August 31, 2025  
**ETL Framework Version**: 2.0.0  
**Documentation Version**: 1.0.0