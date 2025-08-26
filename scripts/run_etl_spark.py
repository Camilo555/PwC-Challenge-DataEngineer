#!/usr/bin/env python3
"""
Complete Spark-based ETL Pipeline Runner
Runs Bronze -> Silver -> Gold layers sequentially
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.logging import get_logger
from etl.bronze.spark_bronze import ingest_bronze_spark
from etl.gold.spark_gold import process_gold_layer
from etl.silver.spark_silver import process_silver_layer_spark

logger = get_logger(__name__)


def run_full_etl_pipeline() -> bool:
    """Run the complete ETL pipeline with Spark."""
    try:
        logger.info("Starting full Spark-based ETL pipeline...")

        # Bronze layer
        logger.info("=" * 50)
        logger.info("BRONZE LAYER PROCESSING")
        logger.info("=" * 50)
        if not ingest_bronze_spark():
            logger.error("Bronze layer processing failed")
            return False
        logger.info("Bronze layer completed successfully")

        # Silver layer
        logger.info("=" * 50)
        logger.info("SILVER LAYER PROCESSING")
        logger.info("=" * 50)
        if not process_silver_layer_spark():
            logger.error("Silver layer processing failed")
            return False
        logger.info("Silver layer completed successfully")

        # Gold layer
        logger.info("=" * 50)
        logger.info("GOLD LAYER PROCESSING")
        logger.info("=" * 50)
        if not process_gold_layer():
            logger.error("Gold layer processing failed")
            return False
        logger.info("Gold layer completed successfully")

        logger.info("=" * 50)
        logger.info("FULL ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        return True

    except Exception as e:
        logger.error(f"Full ETL pipeline failed: {e}")
        return False


def main() -> None:
    """Entry point for full ETL pipeline."""
    success = run_full_etl_pipeline()
    if success:
        print("Complete ETL pipeline finished successfully (Spark-based)")
    else:
        print("ETL pipeline failed")
        exit(1)


if __name__ == "__main__":
    main()
