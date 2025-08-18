"""Dagster assets for the retail data pipeline."""

import asyncio
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from de_challenge.core.config import settings
from de_challenge.core.logging import get_logger

logger = get_logger(__name__)


class RetailDataConfig(Config):
    """Configuration for retail data processing."""
    
    input_file: str = "retail_transactions.csv"
    enable_enrichment: bool = True
    batch_size: int = 100


@asset(
    description="Raw retail transaction data ingested from files",
    group_name="bronze_layer",
)
def raw_retail_data(context: AssetExecutionContext, config: RetailDataConfig) -> pd.DataFrame:
    """
    Ingest raw retail transaction data from CSV files.
    
    This asset watches for new files in the raw data directory and ingests them
    when they appear.
    """
    input_path = Path(settings.raw_data_path) / config.input_file
    
    if not input_path.exists():
        raise FileNotFoundError(f"Raw data file not found: {input_path}")
    
    # Read raw data
    df = pd.read_csv(input_path)
    
    # Add metadata for tracking
    df['ingestion_timestamp'] = pd.Timestamp.now()
    df['source_file'] = config.input_file
    df['data_source'] = 'online_retail'
    
    # Log ingestion statistics
    context.log.info(f"Ingested {len(df)} records from {input_path}")
    
    return df


@asset(
    description="Bronze layer data with basic transformations and optional external enrichment",
    group_name="bronze_layer",
    deps=[raw_retail_data],
)
async def bronze_retail_data(
    context: AssetExecutionContext, 
    config: RetailDataConfig,
    raw_retail_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform raw data to bronze layer with optional external API enrichment.
    
    This asset applies basic transformations and optionally enriches data
    with external APIs for currency rates, country information, and product categorization.
    """
    context.log.info("Starting bronze layer processing...")
    
    # Basic bronze transformations
    bronze_df = raw_retail_data.copy()
    
    # Data cleaning
    bronze_df = bronze_df.dropna(subset=['InvoiceNo', 'StockCode'])
    bronze_df = bronze_df[bronze_df['Quantity'] > 0]  # Remove negative quantities for now
    bronze_df = bronze_df[bronze_df['UnitPrice'] > 0]  # Remove zero/negative prices
    
    # Standard column naming
    bronze_df = bronze_df.rename(columns={
        'InvoiceNo': 'invoice_no',
        'StockCode': 'stock_code',
        'Description': 'description',
        'Quantity': 'quantity',
        'InvoiceDate': 'invoice_date',
        'UnitPrice': 'unit_price',
        'CustomerID': 'customer_id',
        'Country': 'country'
    })
    
    # Add calculated fields
    bronze_df['total_amount'] = bronze_df['quantity'] * bronze_df['unit_price']
    bronze_df['bronze_processing_timestamp'] = pd.Timestamp.now()
    
    # Apply external enrichment if enabled
    if config.enable_enrichment and settings.enable_external_enrichment:
        context.log.info("Applying external API enrichment...")
        
        try:
            from de_challenge.external_apis.enrichment_service import get_enrichment_service
            
            enrichment_service = get_enrichment_service()
            
            # Convert to list of dictionaries for enrichment
            transactions = bronze_df.to_dict('records')
            
            # Enrich in batches
            enriched_transactions = await enrichment_service.enrich_batch_transactions(
                transactions[:config.batch_size],  # Limit for demo
                batch_size=10,
            )
            
            # Convert back to DataFrame
            if enriched_transactions:
                enriched_df = pd.DataFrame(enriched_transactions)
                context.log.info(f"Enriched {len(enriched_df)} records with external APIs")
                
                # Close API clients
                await enrichment_service.close_all_clients()
                
                return enriched_df
            else:
                context.log.warning("No records were enriched")
                
        except Exception as e:
            context.log.error(f"External enrichment failed: {e}")
            bronze_df['enrichment_error'] = str(e)
    
    bronze_df['enrichment_applied'] = False
    
    context.log.info(f"Bronze processing completed: {len(bronze_df)} records")
    return bronze_df


@asset(
    description="Silver layer data with advanced transformations and data quality checks",
    group_name="silver_layer",
    deps=[bronze_retail_data],
)
def silver_retail_data(
    context: AssetExecutionContext,
    bronze_retail_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform bronze data to silver layer with advanced transformations and quality checks.
    
    This asset applies data quality rules, standardization, and creates
    the foundational clean dataset for analytics.
    """
    context.log.info("Starting silver layer processing...")
    
    silver_df = bronze_retail_data.copy()
    
    # Data quality improvements
    silver_df['description'] = silver_df['description'].fillna('Unknown Product')
    silver_df['customer_id'] = silver_df['customer_id'].fillna('GUEST')
    
    # Standardize country names
    country_mapping = {
        'UK': 'United Kingdom',
        'USA': 'United States',
        'UAE': 'United Arab Emirates',
        'RSA': 'South Africa',
        'Eire': 'Ireland',
    }
    silver_df['country'] = silver_df['country'].replace(country_mapping)
    
    # Add derived dimensions
    silver_df['invoice_date'] = pd.to_datetime(silver_df['invoice_date'])
    silver_df['year'] = silver_df['invoice_date'].dt.year
    silver_df['month'] = silver_df['invoice_date'].dt.month
    silver_df['quarter'] = silver_df['invoice_date'].dt.quarter
    silver_df['day_of_week'] = silver_df['invoice_date'].dt.day_name()
    silver_df['is_weekend'] = silver_df['invoice_date'].dt.weekday >= 5
    
    # Calculate business metrics
    silver_df['revenue'] = silver_df['quantity'] * silver_df['unit_price']
    silver_df['is_high_value'] = silver_df['revenue'] > silver_df['revenue'].quantile(0.9)
    
    # Data quality flags
    silver_df['quality_score'] = 1.0
    silver_df.loc[silver_df['description'] == 'Unknown Product', 'quality_score'] -= 0.2
    silver_df.loc[silver_df['customer_id'] == 'GUEST', 'quality_score'] -= 0.1
    
    silver_df['silver_processing_timestamp'] = pd.Timestamp.now()
    
    context.log.info(f"Silver processing completed: {len(silver_df)} records")
    return silver_df


@asset(
    description="Gold layer aggregated data for analytics and reporting",
    group_name="gold_layer",
    deps=[silver_retail_data],
)
def gold_sales_summary(
    context: AssetExecutionContext,
    silver_retail_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Create gold layer aggregations for sales analytics.
    
    This asset produces business-ready aggregated data for dashboards,
    reports, and analytics applications.
    """
    context.log.info("Starting gold layer aggregations...")
    
    df = silver_retail_data.copy()
    
    # Sales by country
    country_sales = df.groupby('country').agg({
        'revenue': ['sum', 'mean', 'count'],
        'quantity': 'sum',
        'customer_id': 'nunique'
    }).round(2)
    
    # Sales by month
    monthly_sales = df.groupby(['year', 'month']).agg({
        'revenue': 'sum',
        'invoice_no': 'nunique',
        'customer_id': 'nunique'
    }).round(2)
    
    # Top products
    top_products = df.groupby(['stock_code', 'description']).agg({
        'revenue': 'sum',
        'quantity': 'sum'
    }).sort_values('revenue', ascending=False).head(10)
    
    # Customer segments
    customer_summary = df.groupby('customer_id').agg({
        'revenue': 'sum',
        'invoice_no': 'nunique',
        'quantity': 'sum'
    })
    
    # Define customer segments based on RFM-like analysis
    customer_summary['segment'] = 'Regular'
    customer_summary.loc[customer_summary['revenue'] > customer_summary['revenue'].quantile(0.8), 'segment'] = 'VIP'
    customer_summary.loc[customer_summary['revenue'] < customer_summary['revenue'].quantile(0.2), 'segment'] = 'Low Value'
    
    # Overall KPIs
    kpis = {
        'total_revenue': df['revenue'].sum(),
        'total_transactions': df['invoice_no'].nunique(),
        'total_customers': df['customer_id'].nunique(),
        'avg_order_value': df.groupby('invoice_no')['revenue'].sum().mean(),
        'total_products': df['stock_code'].nunique(),
        'date_range': {
            'start': df['invoice_date'].min().isoformat(),
            'end': df['invoice_date'].max().isoformat()
        }
    }
    
    gold_data = {
        'kpis': kpis,
        'country_sales': country_sales.to_dict(),
        'monthly_sales': monthly_sales.to_dict(),
        'top_products': top_products.to_dict(),
        'customer_segments': customer_summary['segment'].value_counts().to_dict(),
        'processing_timestamp': pd.Timestamp.now().isoformat()
    }
    
    context.log.info("Gold layer aggregations completed")
    
    # Add metadata for monitoring
    context.add_output_metadata({
        'total_revenue': MetadataValue.float(kpis['total_revenue']),
        'total_transactions': MetadataValue.int(kpis['total_transactions']),
        'total_customers': MetadataValue.int(kpis['total_customers']),
        'processing_date': MetadataValue.text(pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))
    })
    
    return gold_data


@asset(
    description="Data quality metrics and validation results",
    group_name="monitoring",
    deps=[silver_retail_data],
)
def data_quality_metrics(
    context: AssetExecutionContext,
    silver_retail_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Calculate comprehensive data quality metrics for monitoring.
    
    This asset provides data quality insights and validation results
    for the entire pipeline.
    """
    context.log.info("Calculating data quality metrics...")
    
    df = silver_retail_data.copy()
    
    # Completeness metrics
    completeness = {
        'invoice_no': (df['invoice_no'].notna().sum() / len(df)) * 100,
        'stock_code': (df['stock_code'].notna().sum() / len(df)) * 100,
        'description': (df['description'].notna().sum() / len(df)) * 100,
        'customer_id': (df['customer_id'].notna().sum() / len(df)) * 100,
        'country': (df['country'].notna().sum() / len(df)) * 100,
    }
    
    # Validity metrics
    validity = {
        'positive_quantities': (df['quantity'] > 0).sum() / len(df) * 100,
        'positive_prices': (df['unit_price'] > 0).sum() / len(df) * 100,
        'valid_dates': df['invoice_date'].notna().sum() / len(df) * 100,
    }
    
    # Uniqueness metrics
    uniqueness = {
        'unique_invoices': df['invoice_no'].nunique(),
        'unique_products': df['stock_code'].nunique(),
        'unique_customers': df['customer_id'].nunique(),
        'unique_countries': df['country'].nunique(),
    }
    
    # Consistency metrics
    consistency = {
        'description_consistency': df.groupby('stock_code')['description'].nunique().eq(1).sum() / df['stock_code'].nunique() * 100,
        'price_consistency': df.groupby('stock_code')['unit_price'].std().fillna(0).lt(0.1).sum() / df['stock_code'].nunique() * 100,
    }
    
    # Overall quality score
    avg_completeness = sum(completeness.values()) / len(completeness)
    avg_validity = sum(validity.values()) / len(validity)
    avg_consistency = sum(consistency.values()) / len(consistency)
    
    overall_quality_score = (avg_completeness + avg_validity + avg_consistency) / 3
    
    quality_metrics = {
        'overall_quality_score': overall_quality_score,
        'completeness': completeness,
        'validity': validity,
        'uniqueness': uniqueness,
        'consistency': consistency,
        'record_count': len(df),
        'assessment_timestamp': pd.Timestamp.now().isoformat()
    }
    
    context.log.info(f"Data quality assessment completed. Overall score: {overall_quality_score:.1f}%")
    
    # Add metadata for monitoring
    context.add_output_metadata({
        'quality_score': MetadataValue.float(overall_quality_score),
        'record_count': MetadataValue.int(len(df)),
        'completeness_avg': MetadataValue.float(avg_completeness),
        'validity_avg': MetadataValue.float(avg_validity),
    })
    
    return quality_metrics