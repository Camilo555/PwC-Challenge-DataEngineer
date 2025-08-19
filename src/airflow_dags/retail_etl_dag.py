"""
Airflow DAG for retail ETL pipeline with file sensor and external API enrichment.

This DAG provides an alternative to Dagster for orchestrating the retail data pipeline.
It includes file sensors, external API enrichment, and multi-layer data processing.
"""

from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='Retail ETL pipeline with external API enrichment',
    schedule_interval=None,  # Triggered by file sensor
    max_active_runs=1,
    catchup=False,
    tags=['retail', 'etl', 'external-api', 'file-trigger'],
)


def ingest_raw_data(**context):
    """Ingest raw data from CSV files."""
    import pandas as pd

    file_path = context['params'].get('file_path', 'data/raw/retail_transactions.csv')
    logger.info(f"Ingesting raw data from: {file_path}")

    # Read raw data
    df = pd.read_csv(file_path)

    # Add metadata
    df['ingestion_timestamp'] = pd.Timestamp.now()
    df['source_file'] = Path(file_path).name
    df['data_source'] = 'online_retail'

    # Save to bronze layer
    bronze_path = Path(settings.bronze_path) / "raw_data"
    bronze_path.mkdir(parents=True, exist_ok=True)

    output_file = bronze_path / f"raw_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(output_file, index=False)

    logger.info(f"Ingested {len(df)} records to {output_file}")
    return str(output_file)


async def enrich_with_external_apis(**context):
    """Enrich data with external APIs."""
    import pandas as pd

    raw_file = context['task_instance'].xcom_pull(task_ids='ingest_raw_data')
    logger.info(f"Enriching data from: {raw_file}")

    # Read raw data
    df = pd.read_parquet(raw_file)

    if settings.enable_external_enrichment:
        from external_apis.enrichment_service import get_enrichment_service

        enrichment_service = get_enrichment_service()

        try:
            # Check API health
            health_status = await enrichment_service.health_check_all()
            logger.info(f"API health status: {health_status}")

            # Convert to list for enrichment
            transactions = df.to_dict('records')

            # Enrich in batches
            enriched_transactions = await enrichment_service.enrich_batch_transactions(
                transactions[:settings.enrichment_batch_size],  # Limit for Airflow
                batch_size=10,
                include_currency=health_status.get('currency_api', False),
                include_country=health_status.get('country_api', False),
                include_product=health_status.get('product_api', False),
            )

            if enriched_transactions:
                df = pd.DataFrame(enriched_transactions)
                logger.info(f"Successfully enriched {len(df)} records")
            else:
                logger.warning("No records were enriched")
                df['enrichment_applied'] = False

        except Exception as e:
            logger.error(f"External enrichment failed: {e}")
            df['enrichment_applied'] = False
            df['enrichment_error'] = str(e)
        finally:
            await enrichment_service.close_all_clients()
    else:
        df['enrichment_applied'] = False
        logger.info("External enrichment disabled")

    # Save enriched bronze data
    bronze_path = Path(settings.bronze_path) / "enriched_data"
    bronze_path.mkdir(parents=True, exist_ok=True)

    output_file = bronze_path / f"enriched_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(output_file, index=False)

    logger.info(f"Saved enriched data to {output_file}")
    return str(output_file)


def process_bronze_to_silver(**context):
    """Transform bronze data to silver layer."""
    import pandas as pd

    bronze_file = context['task_instance'].xcom_pull(task_ids='enrich_with_apis')
    logger.info(f"Processing bronze to silver: {bronze_file}")

    # Read bronze data
    df = pd.read_parquet(bronze_file)

    # Data cleaning and standardization
    df = df.dropna(subset=['InvoiceNo', 'StockCode']) if 'InvoiceNo' in df.columns else df.dropna(subset=['invoice_no', 'stock_code'])

    # Standardize column names if needed
    if 'InvoiceNo' in df.columns:
        df = df.rename(columns={
            'InvoiceNo': 'invoice_no',
            'StockCode': 'stock_code',
            'Description': 'description',
            'Quantity': 'quantity',
            'InvoiceDate': 'invoice_date',
            'UnitPrice': 'unit_price',
            'CustomerID': 'customer_id',
            'Country': 'country'
        })

    # Data quality improvements
    df['description'] = df['description'].fillna('Unknown Product')
    df['customer_id'] = df['customer_id'].fillna('GUEST')

    # Standardize country names
    country_mapping = {
        'UK': 'United Kingdom',
        'USA': 'United States',
        'UAE': 'United Arab Emirates',
        'RSA': 'South Africa',
        'Eire': 'Ireland',
    }
    df['country'] = df['country'].replace(country_mapping)

    # Add derived fields
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    df['year'] = df['invoice_date'].dt.year
    df['month'] = df['invoice_date'].dt.month
    df['quarter'] = df['invoice_date'].dt.quarter
    df['day_of_week'] = df['invoice_date'].dt.day_name()
    df['is_weekend'] = df['invoice_date'].dt.weekday >= 5

    # Calculate business metrics
    df['revenue'] = df['quantity'] * df['unit_price']
    df['is_high_value'] = df['revenue'] > df['revenue'].quantile(0.9)

    # Data quality score
    df['quality_score'] = 1.0
    df.loc[df['description'] == 'Unknown Product', 'quality_score'] -= 0.2
    df.loc[df['customer_id'] == 'GUEST', 'quality_score'] -= 0.1

    df['silver_processing_timestamp'] = pd.Timestamp.now()

    # Save silver data
    silver_path = Path(settings.silver_path) / "processed_data"
    silver_path.mkdir(parents=True, exist_ok=True)

    output_file = silver_path / f"silver_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(output_file, index=False)

    logger.info(f"Processed {len(df)} records to silver layer: {output_file}")
    return str(output_file)


def process_silver_to_gold(**context):
    """Transform silver data to gold layer aggregations."""
    import json

    import pandas as pd

    silver_file = context['task_instance'].xcom_pull(task_ids='bronze_to_silver')
    logger.info(f"Processing silver to gold: {silver_file}")

    # Read silver data
    df = pd.read_parquet(silver_file)

    # Create aggregations
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

    # Customer analysis
    customer_summary = df.groupby('customer_id').agg({
        'revenue': 'sum',
        'invoice_no': 'nunique',
        'quantity': 'sum'
    })

    # Customer segmentation
    customer_summary['segment'] = 'Regular'
    customer_summary.loc[customer_summary['revenue'] > customer_summary['revenue'].quantile(0.8), 'segment'] = 'VIP'
    customer_summary.loc[customer_summary['revenue'] < customer_summary['revenue'].quantile(0.2), 'segment'] = 'Low Value'

    # KPIs
    kpis = {
        'total_revenue': float(df['revenue'].sum()),
        'total_transactions': int(df['invoice_no'].nunique()),
        'total_customers': int(df['customer_id'].nunique()),
        'avg_order_value': float(df.groupby('invoice_no')['revenue'].sum().mean()),
        'total_products': int(df['stock_code'].nunique()),
        'date_range': {
            'start': df['invoice_date'].min().isoformat(),
            'end': df['invoice_date'].max().isoformat()
        }
    }

    # Compile gold data
    gold_data = {
        'kpis': kpis,
        'country_sales': country_sales.to_dict(),
        'monthly_sales': monthly_sales.to_dict(),
        'top_products': top_products.to_dict(),
        'customer_segments': customer_summary['segment'].value_counts().to_dict(),
        'processing_timestamp': pd.Timestamp.now().isoformat()
    }

    # Save gold data
    gold_path = Path(settings.gold_path) / "aggregations"
    gold_path.mkdir(parents=True, exist_ok=True)

    output_file = gold_path / f"gold_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(gold_data, f, indent=2, default=str)

    logger.info(f"Generated gold layer aggregations: {output_file}")
    logger.info(f"Total revenue: ${kpis['total_revenue']:,.2f}")
    logger.info(f"Total transactions: {kpis['total_transactions']:,}")

    return str(output_file)


def generate_data_quality_report(**context):
    """Generate data quality metrics and report."""
    import json

    import pandas as pd

    silver_file = context['task_instance'].xcom_pull(task_ids='bronze_to_silver')
    logger.info(f"Generating quality report for: {silver_file}")

    # Read silver data
    df = pd.read_parquet(silver_file)

    # Calculate quality metrics
    completeness = {
        'invoice_no': (df['invoice_no'].notna().sum() / len(df)) * 100,
        'stock_code': (df['stock_code'].notna().sum() / len(df)) * 100,
        'description': (df['description'].notna().sum() / len(df)) * 100,
        'customer_id': (df['customer_id'].notna().sum() / len(df)) * 100,
        'country': (df['country'].notna().sum() / len(df)) * 100,
    }

    validity = {
        'positive_quantities': (df['quantity'] > 0).sum() / len(df) * 100,
        'positive_prices': (df['unit_price'] > 0).sum() / len(df) * 100,
        'valid_dates': df['invoice_date'].notna().sum() / len(df) * 100,
    }

    uniqueness = {
        'unique_invoices': int(df['invoice_no'].nunique()),
        'unique_products': int(df['stock_code'].nunique()),
        'unique_customers': int(df['customer_id'].nunique()),
        'unique_countries': int(df['country'].nunique()),
    }

    # Overall quality score
    avg_completeness = sum(completeness.values()) / len(completeness)
    avg_validity = sum(validity.values()) / len(validity)
    overall_quality_score = (avg_completeness + avg_validity) / 2

    quality_report = {
        'overall_quality_score': overall_quality_score,
        'completeness': completeness,
        'validity': validity,
        'uniqueness': uniqueness,
        'record_count': len(df),
        'assessment_timestamp': pd.Timestamp.now().isoformat(),
        'quality_status': 'PASS' if overall_quality_score >= 85 else 'FAIL'
    }

    # Save quality report
    reports_path = Path("reports") / "data_quality"
    reports_path.mkdir(parents=True, exist_ok=True)

    output_file = reports_path / f"quality_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(quality_report, f, indent=2, default=str)

    logger.info(f"Data quality report generated: {output_file}")
    logger.info(f"Overall quality score: {overall_quality_score:.1f}%")

    if overall_quality_score < 85:
        logger.warning("Data quality below threshold! Review required.")

    return str(output_file)


# File sensor to detect new CSV files
file_sensor = FileSensor(
    task_id='wait_for_file',
    filepath=str(Path(settings.raw_data_path) / "*.csv"),
    fs_conn_id='fs_default',
    poke_interval=30,  # Check every 30 seconds
    timeout=60 * 60 * 24,  # Wait up to 24 hours
    dag=dag,
)

# Data ingestion task
ingest_task = PythonOperator(
    task_id='ingest_raw_data',
    python_callable=ingest_raw_data,
    dag=dag,
)

# External API enrichment task
enrich_task = PythonOperator(
    task_id='enrich_with_apis',
    python_callable=enrich_with_external_apis,
    dag=dag,
)

# Bronze to Silver transformation
bronze_to_silver_task = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=process_bronze_to_silver,
    dag=dag,
)

# Silver to Gold transformation
silver_to_gold_task = PythonOperator(
    task_id='silver_to_gold',
    python_callable=process_silver_to_gold,
    dag=dag,
)

# Data quality assessment
quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=generate_data_quality_report,
    dag=dag,
)

# Define task dependencies
file_sensor >> ingest_task >> enrich_task >> bronze_to_silver_task
bronze_to_silver_task >> [silver_to_gold_task, quality_task]
