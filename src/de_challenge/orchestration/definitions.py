"""Dagster definitions - main entry point for the orchestration system."""

from dagster import Definitions

from .assets import (
    bronze_retail_data,
    data_quality_metrics,
    gold_sales_summary,
    raw_retail_data,
    silver_retail_data,
)
from .jobs import (
    data_quality_job,
    data_quality_schedule,
    daily_incremental_schedule,
    incremental_processing_job,
    reprocessing_job,
    retail_etl_job,
    weekly_reprocessing_schedule,
)
from .sensors import (
    error_file_sensor,
    file_drop_sensor,
    large_file_sensor,
)
from .resources import (
    database_resource,
    enrichment_service_resource,
    spark_resource,
)

# Main Dagster definitions
defs = Definitions(
    assets=[
        raw_retail_data,
        bronze_retail_data,
        silver_retail_data,
        gold_sales_summary,
        data_quality_metrics,
    ],
    jobs=[
        retail_etl_job,
        incremental_processing_job,
        data_quality_job,
        reprocessing_job,
    ],
    schedules=[
        data_quality_schedule,
        daily_incremental_schedule,
        weekly_reprocessing_schedule,
    ],
    sensors=[
        file_drop_sensor,
        large_file_sensor,
        error_file_sensor,
    ],
    resources={
        "database": database_resource,
        "spark": spark_resource,
        "enrichment_service": enrichment_service_resource,
    },
)