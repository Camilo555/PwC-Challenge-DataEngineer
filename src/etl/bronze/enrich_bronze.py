"""Enhanced bronze layer with external API enrichment."""

import asyncio
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType

from core.config import settings
from core.logging import get_logger
from external_apis.enrichment_service import get_enrichment_service

logger = get_logger(__name__)


class EnrichedBronzeProcessor:
    """Enhanced bronze layer processor with external API enrichment."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the enriched bronze processor."""
        self.spark = spark
        self.enrichment_service = get_enrichment_service()

    async def process_with_enrichment(
        self,
        input_path: str | Path,
        output_path: str | Path,
        enable_enrichment: bool = True,
    ) -> None:
        """
        Process raw data through bronze layer with optional external enrichment.

        Args:
            input_path: Path to raw data files
            output_path: Path to write enriched bronze data
            enable_enrichment: Whether to apply external API enrichment
        """
        logger.info(f"Starting enhanced bronze processing: {input_path} -> {output_path}")

        try:
            # Read raw data
            raw_df = self._read_raw_data(input_path)
            logger.info(f"Read {raw_df.count()} raw records")

            # Apply basic bronze transformations
            bronze_df = self._apply_bronze_transformations(raw_df)

            if enable_enrichment and settings.enable_external_enrichment:
                # Apply external API enrichment
                enriched_df = await self._apply_external_enrichment(bronze_df)
                final_df = enriched_df
            else:
                # Skip enrichment
                final_df = bronze_df.withColumn("enrichment_applied", lit(False))
                logger.info("External enrichment skipped")

            # Write enriched bronze data
            self._write_bronze_data(final_df, output_path)

            # Log statistics
            await self._log_enrichment_stats(final_df)

        except Exception as e:
            logger.error(f"Enhanced bronze processing failed: {e}")
            raise
        finally:
            await self.enrichment_service.close_all_clients()

    def _read_raw_data(self, input_path: str | Path) -> DataFrame:
        """Read raw data from various formats."""
        input_path = Path(input_path)

        if input_path.suffix.lower() == '.csv':
            return self.spark.read.option("header", "true").option("inferSchema", "true").csv(str(input_path))
        elif input_path.suffix.lower() in ['.xlsx', '.xls']:
            # For Excel files, you'd need to convert to CSV first or use a different approach
            raise NotImplementedError("Excel files need to be converted to CSV first")
        else:
            # Try to read as parquet/delta
            return self.spark.read.load(str(input_path))

    def _apply_bronze_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic bronze layer transformations."""
        # Add ingestion metadata
        transformed_df = df.withColumn("ingestion_timestamp", lit(self.spark.sql("SELECT current_timestamp()").collect()[0][0]))

        # Add source information
        transformed_df = transformed_df.withColumn("data_source", lit("online_retail"))
        transformed_df = transformed_df.withColumn("bronze_processing_version", lit("v1.1"))

        return transformed_df

    async def _apply_external_enrichment(self, df: DataFrame) -> DataFrame:
        """Apply external API enrichment to the DataFrame."""
        logger.info("Starting external API enrichment...")

        try:
            # Check API health first
            health_status = await self.enrichment_service.health_check_all()
            logger.info(f"API health status: {health_status}")

            # Convert DataFrame to list of dictionaries for enrichment
            # Note: This is suitable for smaller datasets. For large datasets,
            # consider using Spark's mapPartitions or other distributed approaches
            transactions = [row.asDict() for row in df.collect()]

            if len(transactions) > 1000:
                logger.warning(f"Large dataset ({len(transactions)} records) - enrichment may take time")

            # Enrich transactions in batches
            enriched_transactions = await self.enrichment_service.enrich_batch_transactions(
                transactions,
                batch_size=settings.enrichment_batch_size,
                include_currency=health_status.get("currency_api", False),
                include_country=health_status.get("country_api", False),
                include_product=health_status.get("product_api", False),
            )

            # Convert back to DataFrame
            enriched_df = self._convert_enriched_to_dataframe(enriched_transactions)

            # Generate enrichment statistics
            stats = await self.enrichment_service.get_enrichment_statistics(enriched_transactions)
            logger.info(f"Enrichment statistics: {stats}")

            return enriched_df

        except Exception as e:
            logger.error(f"External enrichment failed: {e}")
            # Return original DataFrame with error marker
            return df.withColumn("enrichment_applied", lit(False)).withColumn("enrichment_error", lit(str(e)))

    def _convert_enriched_to_dataframe(self, enriched_data: list[dict]) -> DataFrame:
        """Convert enriched data back to Spark DataFrame."""
        if not enriched_data:
            return self.spark.createDataFrame([], StructType([]))

        # Create DataFrame from enriched data
        # Spark will infer schema from the dictionaries
        enriched_df = self.spark.createDataFrame(enriched_data)

        # Mark as enriched
        enriched_df = enriched_df.withColumn("enrichment_applied", lit(True))

        return enriched_df

    def _write_bronze_data(self, df: DataFrame, output_path: str | Path) -> None:
        """Write enriched bronze data to storage."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write as Delta format for better performance and ACID properties
        df.write.mode("overwrite").option("overwriteSchema", "true").save(str(output_path))

        logger.info(f"Written enriched bronze data to {output_path}")

    async def _log_enrichment_stats(self, df: DataFrame) -> None:
        """Log enrichment statistics."""
        total_records = df.count()

        # Count enriched records
        enriched_records = df.filter(col("enrichment_applied")).count()

        if total_records > 0:
            enrichment_rate = enriched_records / total_records
            logger.info(f"Enrichment summary: {enriched_records}/{total_records} records enriched ({enrichment_rate:.1%})")
        else:
            logger.warning("No records processed")


async def run_enriched_bronze_processing(
    input_path: str = "data/raw/retail_transactions.csv",
    output_path: str = "data/bronze/enriched_sales",
    enable_enrichment: bool = True,
) -> None:
    """
    Run the enriched bronze processing pipeline.

    Args:
        input_path: Input raw data path
        output_path: Output enriched bronze data path
        enable_enrichment: Whether to enable external API enrichment
    """
    from etl.utils.spark import get_spark

    spark = None
    try:
        # Get Spark session
        spark = get_spark("EnrichedBronzeProcessing")

        # Create processor
        processor = EnrichedBronzeProcessor(spark)

        # Run processing
        await processor.process_with_enrichment(
            input_path=input_path,
            output_path=output_path,
            enable_enrichment=enable_enrichment,
        )

        logger.info("Enriched bronze processing completed successfully")

    except Exception as e:
        logger.error(f"Enriched bronze processing failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    # Run enriched bronze processing
    asyncio.run(run_enriched_bronze_processing())
