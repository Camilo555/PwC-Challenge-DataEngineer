"""Dagster jobs for orchestrating the retail data pipeline."""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    define_asset_job,
    schedule,
)

from .assets import (
    bronze_retail_data,
    data_quality_metrics,
    gold_sales_summary,
    raw_retail_data,
    silver_retail_data,
)

# Define the complete ETL pipeline job
retail_etl_job = define_asset_job(
    name="retail_etl_pipeline",
    description="Complete retail data ETL pipeline from raw to gold layer",
    selection=AssetSelection.assets(
        raw_retail_data,
        bronze_retail_data,
        silver_retail_data,
        gold_sales_summary,
        data_quality_metrics,
    ),
    tags={
        "pipeline": "retail_etl",
        "owner": "data_engineering",
        "environment": "production",
    }
)

# Define incremental processing job (bronze to gold only)
incremental_processing_job = define_asset_job(
    name="incremental_processing",
    description="Incremental processing from bronze to gold layers",
    selection=AssetSelection.assets(
        bronze_retail_data,
        silver_retail_data,
        gold_sales_summary,
    ),
    tags={
        "pipeline": "incremental",
        "processing_type": "incremental",
    }
)

# Define data quality monitoring job
data_quality_job = define_asset_job(
    name="data_quality_monitoring",
    description="Data quality assessment and monitoring",
    selection=AssetSelection.assets(
        data_quality_metrics,
    ),
    tags={
        "pipeline": "monitoring",
        "type": "data_quality",
    }
)

# Define reprocessing job for historical data
reprocessing_job = define_asset_job(
    name="historical_reprocessing",
    description="Reprocess historical data through the entire pipeline",
    selection=AssetSelection.assets(
        raw_retail_data,
        bronze_retail_data,
        silver_retail_data,
        gold_sales_summary,
        data_quality_metrics,
    ),
    tags={
        "pipeline": "reprocessing",
        "processing_type": "historical",
    }
)


# Scheduled jobs
@schedule(
    job=data_quality_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
    default_status=DefaultScheduleStatus.RUNNING,
)
def data_quality_schedule():
    """Schedule data quality monitoring every 6 hours."""
    return {
        "ops": {
            "data_quality_metrics": {
                "config": {
                    "enable_alerts": True,
                    "quality_threshold": 85.0,
                }
            }
        }
    }


@schedule(
    job=incremental_processing_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    default_status=DefaultScheduleStatus.STOPPED,  # Manual activation
)
def daily_incremental_schedule():
    """Schedule daily incremental processing."""
    return {
        "ops": {
            "bronze_retail_data": {
                "config": {
                    "enable_enrichment": True,
                    "batch_size": 200,
                }
            }
        }
    }


@schedule(
    job=reprocessing_job,
    cron_schedule="0 3 * * 0",  # Weekly on Sunday at 3 AM
    default_status=DefaultScheduleStatus.STOPPED,
)
def weekly_reprocessing_schedule():
    """Schedule weekly full reprocessing."""
    return {
        "ops": {
            "raw_retail_data": {
                "config": {
                    "input_file": "retail_transactions.csv",
                    "enable_enrichment": True,
                    "batch_size": 500,
                }
            }
        }
    }