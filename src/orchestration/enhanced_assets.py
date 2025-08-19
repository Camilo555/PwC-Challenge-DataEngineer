"""
Enhanced Dagster Assets for Modern Data Orchestration
Provides comprehensive asset definitions with advanced features
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
from dagster import (
    asset, AssetExecutionContext, ConfigurableResource, Config,
    AssetMaterialization, Output, MetadataValue, AssetIn, AssetOut,
    multi_asset, AssetKey, DependsOn, Definitions,
    ScheduleDefinition, SensorDefinition, sensor, schedule,
    DefaultSensorStatus, SkipReason, RunRequest, SensorEvaluationContext,
    AssetSelection, define_asset_job, job, op, In, Out, GraphDefinition,
    resource, get_dagster_logger, DagsterLogManager
)
from dagster._core.definitions.partition import StaticPartitionsDefinition
from dagster._core.definitions.time_window_partitions import DailyPartitionsDefinition
from pydantic import BaseModel, Field

from core.config.base_config import BaseConfig, Environment, ProcessingEngine
from core.config.dagster_config import DagsterConfig
from core.logging import get_logger
from external_apis.enrichment_service import DataEnrichmentService

logger = get_dagster_logger()
app_logger = get_logger(__name__)


class RetailDataConfig(Config):
    """Configuration for retail data processing."""
    
    enable_external_enrichment: bool = Field(default=True, description="Enable external API enrichment")
    enable_data_quality_checks: bool = Field(default=True, description="Enable comprehensive data quality checks")
    enable_spark_processing: bool = Field(default=False, description="Use Spark for large-scale processing")
    batch_size: int = Field(default=1000, description="Batch size for processing")
    quality_threshold: float = Field(default=0.8, description="Minimum data quality score")
    max_null_percentage: float = Field(default=0.1, description="Maximum allowed null percentage")


class DataQualityResource(ConfigurableResource):
    """Resource for data quality assessment."""
    
    min_quality_score: float = Field(default=0.8)
    max_null_percentage: float = Field(default=0.1)
    max_duplicate_percentage: float = Field(default=0.05)
    
    def assess_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data quality of a DataFrame."""
        total_records = len(df)
        
        if total_records == 0:
            return {
                "total_records": 0,
                "quality_score": 0.0,
                "null_percentage": 1.0,
                "duplicate_percentage": 0.0,
                "passes_quality_check": False
            }
        
        # Calculate null percentage
        null_count = df.isnull().sum().sum()
        total_cells = total_records * len(df.columns)
        null_percentage = null_count / total_cells if total_cells > 0 else 0
        
        # Calculate duplicate percentage
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = duplicate_count / total_records
        
        # Calculate overall quality score
        completeness_score = 1 - null_percentage
        uniqueness_score = 1 - duplicate_percentage
        
        # Simple quality scoring (can be enhanced)
        quality_score = (completeness_score + uniqueness_score) / 2
        
        # Determine if passes quality check
        passes_check = (
            quality_score >= self.min_quality_score and
            null_percentage <= self.max_null_percentage and
            duplicate_percentage <= self.max_duplicate_percentage
        )
        
        return {
            "total_records": total_records,
            "quality_score": quality_score,
            "null_percentage": null_percentage,
            "duplicate_percentage": duplicate_percentage,
            "completeness_score": completeness_score,
            "uniqueness_score": uniqueness_score,
            "passes_quality_check": passes_check
        }


class ExternalAPIResource(ConfigurableResource):
    """Resource for external API integration."""
    
    timeout_seconds: int = Field(default=30)
    retry_attempts: int = Field(default=3)
    enable_currency_api: bool = Field(default=True)
    enable_country_api: bool = Field(default=True)
    enable_product_api: bool = Field(default=True)
    
    async def enrich_data(self, df: pd.DataFrame, batch_size: int = 100) -> pd.DataFrame:
        """Enrich data with external APIs."""
        try:
            enrichment_service = DataEnrichmentService()
            
            # Convert DataFrame to transaction objects for enrichment
            transactions = []
            for _, row in df.iterrows():
                # Create transaction-like objects from DataFrame rows
                transaction = {
                    'invoice_no': row.get('invoice_no'),
                    'stock_code': row.get('stock_code'),
                    'description': row.get('description'),
                    'quantity': row.get('quantity'),
                    'unit_price': row.get('unit_price'),
                    'customer_id': row.get('customer_id'),
                    'country': row.get('country')
                }
                transactions.append(transaction)
            
            # Enrich transactions
            enriched_transactions = await enrichment_service.enrich_batch_transactions(
                transactions,
                batch_size=batch_size,
                include_currency=self.enable_currency_api,
                include_country=self.enable_country_api,
                include_product=self.enable_product_api
            )
            
            # Convert back to DataFrame
            enriched_df = pd.DataFrame(enriched_transactions)
            
            app_logger.info(f"Successfully enriched {len(enriched_df)} records")
            return enriched_df
            
        except Exception as e:
            app_logger.warning(f"External API enrichment failed: {e}")
            # Return original DataFrame if enrichment fails
            return df


class SparkResource(ConfigurableResource):
    """Resource for Spark session management."""
    
    app_name: str = Field(default="Dagster-Retail-ETL")
    spark_conf: Dict[str, str] = Field(
        default_factory=lambda: {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    )
    
    def create_spark_session(self):
        """Create and configure Spark session."""
        try:
            from etl.spark.session_manager import create_optimized_session
            return create_optimized_session(
                app_name=self.app_name,
                processing_type="batch",
                data_size="medium"
            )
        except ImportError:
            app_logger.warning("Spark not available, falling back to pandas")
            return None


# Date partitioning
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

# Processing engine partitioning
processing_engine_partitions = StaticPartitionsDefinition(["pandas", "spark"])


@asset(
    description="Raw retail transaction data from CSV files",
    group_name="bronze_layer",
    compute_kind="file_ingestion",
    partitions_def=daily_partitions,
    metadata={
        "data_source": "CSV files",
        "update_frequency": "daily",
        "schema_version": "1.0"
    }
)
def raw_retail_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw retail data from CSV files."""
    config = BaseConfig()
    
    # Get partition date if partitioned
    partition_date_str = context.partition_key if context.has_partition_key else None
    
    raw_data_path = config.raw_data_path
    csv_files = list(raw_data_path.rglob("*.csv"))
    
    if not csv_files:
        context.log.warning(f"No CSV files found in {raw_data_path}")
        return pd.DataFrame()
    
    # Read and combine CSV files
    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            df['_source_file'] = str(csv_file)
            dfs.append(df)
            context.log.info(f"Loaded {len(df)} records from {csv_file.name}")
        except Exception as e:
            context.log.error(f"Failed to read {csv_file}: {e}")
            continue
    
    if not dfs:
        return pd.DataFrame()
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Add metadata
    context.add_output_metadata({
        "total_records": len(combined_df),
        "source_files": len(csv_files),
        "columns": list(combined_df.columns),
        "memory_usage_mb": combined_df.memory_usage(deep=True).sum() / 1024 / 1024,
        "partition_date": partition_date_str or "latest"
    })
    
    return combined_df


@asset(
    description="Bronze layer data with standardization and metadata enrichment",
    group_name="bronze_layer",
    compute_kind="data_transformation",
    partitions_def=daily_partitions,
    deps=[raw_retail_data],
    metadata={
        "layer": "bronze",
        "processing_type": "standardization"
    }
)
async def bronze_retail_data(
    context: AssetExecutionContext,
    config: RetailDataConfig,
    raw_retail_data: pd.DataFrame,
    external_api: ExternalAPIResource,
    data_quality: DataQualityResource,
) -> pd.DataFrame:
    """Process raw data into Bronze layer with standardization."""
    
    if raw_retail_data.empty:
        context.log.warning("No raw data to process")
        return pd.DataFrame()
    
    context.log.info(f"Processing {len(raw_retail_data)} raw records")
    
    # Standardize column names
    column_mapping = {
        'InvoiceNo': 'invoice_no',
        'StockCode': 'stock_code',
        'Description': 'description',
        'Quantity': 'quantity',
        'InvoiceDate': 'invoice_timestamp',
        'UnitPrice': 'unit_price',
        'CustomerID': 'customer_id',
        'Country': 'country'
    }
    
    df = raw_retail_data.rename(columns=column_mapping)
    
    # Data type conversion
    try:
        if 'invoice_timestamp' in df.columns:
            df['invoice_timestamp'] = pd.to_datetime(df['invoice_timestamp'], errors='coerce')
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
        if 'unit_price' in df.columns:
            df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    except Exception as e:
        context.log.warning(f"Data type conversion issues: {e}")
    
    # Add processing metadata
    df['_processed_at'] = pd.Timestamp.now()
    df['_bronze_version'] = '2.0'
    df['_processing_engine'] = 'dagster_pandas'
    
    # External API enrichment (if enabled)
    if config.enable_external_enrichment:
        try:
            df = await external_api.enrich_data(df, batch_size=config.batch_size)
            context.log.info("External API enrichment completed")
        except Exception as e:
            context.log.warning(f"External API enrichment failed: {e}")
    
    # Data quality assessment
    if config.enable_data_quality_checks:
        quality_report = data_quality.assess_dataframe(df)
        
        # Log quality metrics
        context.add_output_metadata({
            "quality_score": quality_report["quality_score"],
            "null_percentage": quality_report["null_percentage"],
            "duplicate_percentage": quality_report["duplicate_percentage"],
            "passes_quality_check": quality_report["passes_quality_check"],
            "total_records": len(df)
        })
        
        context.log.info(
            f"Data quality assessment: "
            f"Score={quality_report['quality_score']:.2%}, "
            f"Nulls={quality_report['null_percentage']:.2%}, "
            f"Duplicates={quality_report['duplicate_percentage']:.2%}"
        )
        
        # Raise warning if quality is low
        if not quality_report["passes_quality_check"]:
            context.log.warning(
                f"Data quality below threshold. "
                f"Score: {quality_report['quality_score']:.2%} "
                f"(required: {config.quality_threshold:.2%})"
            )
    
    return df


@asset(
    description="Silver layer data with business rules and derived columns",
    group_name="silver_layer",
    compute_kind="business_transformation",
    partitions_def=daily_partitions,
    deps=[bronze_retail_data],
    metadata={
        "layer": "silver",
        "processing_type": "business_rules"
    }
)
def silver_retail_data(
    context: AssetExecutionContext,
    config: RetailDataConfig,
    bronze_retail_data: pd.DataFrame,
    data_quality: DataQualityResource
) -> pd.DataFrame:
    """Transform Bronze data into Silver layer with business rules."""
    
    if bronze_retail_data.empty:
        context.log.warning("No bronze data to process")
        return pd.DataFrame()
    
    df = bronze_retail_data.copy()
    original_count = len(df)
    
    # Apply business rules
    # Remove invalid quantities and prices
    df = df[
        (df['quantity'] > 0) & 
        (df['unit_price'] >= 0) &
        (df['invoice_no'].notna())
    ]
    
    # Calculate derived columns
    df['total_amount'] = df['quantity'] * df['unit_price']
    
    # Add date components
    if 'invoice_timestamp' in df.columns:
        df['invoice_date'] = df['invoice_timestamp'].dt.date
        df['invoice_year'] = df['invoice_timestamp'].dt.year
        df['invoice_month'] = df['invoice_timestamp'].dt.month
        df['invoice_quarter'] = df['invoice_timestamp'].dt.quarter
        df['invoice_day_of_week'] = df['invoice_timestamp'].dt.day_name()
    
    # Add business flags
    df['is_return'] = df['invoice_no'].str.startswith('C', na=False)
    df['is_high_value'] = df['total_amount'] > df['total_amount'].quantile(0.95)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['invoice_no', 'stock_code'], keep='first')
    
    # Final quality check
    quality_report = data_quality.assess_dataframe(df)
    
    # Add metadata
    context.add_output_metadata({
        "original_records": original_count,
        "final_records": len(df),
        "records_filtered": original_count - len(df),
        "filter_rate": (original_count - len(df)) / original_count if original_count > 0 else 0,
        "quality_score": quality_report["quality_score"],
        "average_transaction_amount": df['total_amount'].mean() if 'total_amount' in df.columns else 0,
        "total_revenue": df['total_amount'].sum() if 'total_amount' in df.columns else 0,
        "unique_customers": df['customer_id'].nunique() if 'customer_id' in df.columns else 0,
        "unique_products": df['stock_code'].nunique() if 'stock_code' in df.columns else 0
    })
    
    context.log.info(
        f"Silver layer processing completed: "
        f"{len(df)} records (filtered {original_count - len(df)} invalid records)"
    )
    
    return df


@multi_asset(
    outs={
        "sales_summary": AssetOut(
            description="Sales summary by country and time period",
            group_name="gold_layer",
            metadata={"aggregation_level": "country_month"}
        ),
        "customer_metrics": AssetOut(
            description="Customer behavior and segmentation metrics", 
            group_name="gold_layer",
            metadata={"aggregation_level": "customer"}
        ),
        "product_performance": AssetOut(
            description="Product sales performance analytics",
            group_name="gold_layer", 
            metadata={"aggregation_level": "product"}
        )
    },
    partitions_def=daily_partitions,
    deps=[silver_retail_data],
    compute_kind="analytics_aggregation"
)
def gold_analytics_tables(
    context: AssetExecutionContext,
    silver_retail_data: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Generate Gold layer analytics tables."""
    
    if silver_retail_data.empty:
        context.log.warning("No silver data for analytics")
        empty_df = pd.DataFrame()
        return empty_df, empty_df, empty_df
    
    df = silver_retail_data
    
    # Sales Summary
    sales_summary = (df.groupby(['country', 'invoice_year', 'invoice_month'])
                    .agg({
                        'invoice_no': 'nunique',
                        'total_amount': ['sum', 'mean'],
                        'quantity': 'sum',
                        'customer_id': 'nunique',
                        'stock_code': 'nunique'
                    })
                    .round(2))
    
    sales_summary.columns = [
        'total_transactions', 'total_revenue', 'avg_transaction_value',
        'total_quantity', 'unique_customers', 'unique_products'
    ]
    sales_summary = sales_summary.reset_index()
    
    # Customer Metrics
    customer_metrics = (df.groupby('customer_id')
                       .agg({
                           'total_amount': ['sum', 'mean', 'count'],
                           'quantity': 'sum',
                           'stock_code': 'nunique',
                           'invoice_timestamp': ['min', 'max']
                       }))
    
    customer_metrics.columns = [
        'total_spent', 'avg_transaction_value', 'transaction_count',
        'total_quantity', 'unique_products', 'first_purchase', 'last_purchase'
    ]
    customer_metrics = customer_metrics.reset_index()
    
    # Calculate customer lifetime in days
    customer_metrics['customer_lifetime_days'] = (
        customer_metrics['last_purchase'] - customer_metrics['first_purchase']
    ).dt.days
    
    # Product Performance
    product_performance = (df.groupby(['stock_code', 'description'])
                          .agg({
                              'total_amount': 'sum',
                              'quantity': 'sum', 
                              'invoice_no': 'nunique',
                              'customer_id': 'nunique'
                          })
                          .round(2))
    
    product_performance.columns = [
        'total_revenue', 'total_quantity', 'total_orders', 'unique_customers'
    ]
    product_performance = product_performance.reset_index()
    product_performance['revenue_per_order'] = (
        product_performance['total_revenue'] / product_performance['total_orders']
    ).round(2)
    
    # Add metadata for each output
    context.add_output_metadata(
        metadata={
            "sales_summary_records": len(sales_summary),
            "customer_metrics_records": len(customer_metrics), 
            "product_performance_records": len(product_performance),
            "total_revenue_analyzed": df['total_amount'].sum(),
            "analysis_period": f"{df['invoice_timestamp'].min()} to {df['invoice_timestamp'].max()}"
        },
        output_name="sales_summary"
    )
    
    context.log.info(
        f"Gold layer analytics completed: "
        f"Sales summary: {len(sales_summary)}, "
        f"Customer metrics: {len(customer_metrics)}, "
        f"Product performance: {len(product_performance)}"
    )
    
    return sales_summary, customer_metrics, product_performance


# Sensors
@sensor(
    job=define_asset_job("retail_etl_job", selection=AssetSelection.all()),
    default_status=DefaultSensorStatus.RUNNING
)
def file_sensor(context: SensorEvaluationContext):
    """Sensor that triggers on new file arrivals."""
    config = BaseConfig()
    raw_data_path = config.raw_data_path
    
    # Check for new CSV files
    csv_files = list(raw_data_path.glob("*.csv"))
    
    if not csv_files:
        return SkipReason("No CSV files found")
    
    # Check modification times
    recent_files = []
    cutoff_time = datetime.now() - timedelta(minutes=30)
    
    for file_path in csv_files:
        if datetime.fromtimestamp(file_path.stat().st_mtime) > cutoff_time:
            recent_files.append(file_path)
    
    if not recent_files:
        return SkipReason("No recent files found")
    
    context.log.info(f"Found {len(recent_files)} recent files: {[f.name for f in recent_files]}")
    
    return RunRequest(
        run_key=f"file_sensor_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        tags={
            "source": "file_sensor",
            "file_count": str(len(recent_files)),
            "latest_file": recent_files[-1].name
        }
    )


# Schedules
@schedule(
    job=define_asset_job("daily_retail_etl", selection=AssetSelection.all()),
    cron_schedule="0 2 * * *"  # Daily at 2 AM
)
def daily_retail_etl_schedule(context):
    """Daily ETL schedule."""
    return {
        "ops": {
            "enable_external_enrichment": True,
            "enable_data_quality_checks": True,
            "quality_threshold": 0.8
        }
    }


# Job definitions
retail_etl_job = define_asset_job(
    "retail_etl_job",
    selection=AssetSelection.all(),
    partitions_def=daily_partitions,
    description="Complete retail ETL pipeline"
)

data_quality_job = define_asset_job(
    "data_quality_job", 
    selection=AssetSelection.groups("bronze_layer", "silver_layer"),
    description="Data quality focused pipeline"
)

analytics_job = define_asset_job(
    "analytics_job",
    selection=AssetSelection.groups("gold_layer"),
    description="Analytics and aggregation pipeline"
)


# Resources
def get_resources(config: BaseConfig, dagster_config: DagsterConfig):
    """Get configured resources."""
    return {
        "data_quality": DataQualityResource(
            min_quality_score=dagster_config.min_data_quality_score,
            max_null_percentage=dagster_config.max_null_percentage,
            max_duplicate_percentage=dagster_config.max_duplicate_percentage
        ),
        "external_api": ExternalAPIResource(
            timeout_seconds=dagster_config.api_request_timeout_seconds,
            retry_attempts=dagster_config.api_retry_attempts,
            enable_currency_api=dagster_config.enable_external_api_enrichment,
            enable_country_api=dagster_config.enable_external_api_enrichment,
            enable_product_api=dagster_config.enable_external_api_enrichment
        ),
        "spark": SparkResource(
            app_name="Dagster-Enhanced-Retail-ETL",
            spark_conf=dagster_config.default_spark_conf
        ) if config.processing_engine == ProcessingEngine.SPARK else None
    }


# Main definitions
def create_enhanced_definitions():
    """Create enhanced Dagster definitions."""
    config = BaseConfig()
    dagster_config = DagsterConfig()
    
    resources = get_resources(config, dagster_config)
    # Remove None values
    resources = {k: v for k, v in resources.items() if v is not None}
    
    return Definitions(
        assets=[
            raw_retail_data,
            bronze_retail_data,
            silver_retail_data,
            gold_analytics_tables
        ],
        jobs=[
            retail_etl_job,
            data_quality_job,
            analytics_job
        ],
        sensors=[file_sensor],
        schedules=[daily_retail_etl_schedule],
        resources=resources
    )


# Export definitions
defs = create_enhanced_definitions()