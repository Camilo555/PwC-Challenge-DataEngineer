"""
Windows-Compatible Bronze Layer Implementation using Pandas
Provides data ingestion without Spark/Hadoop dependencies
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pandas as pd

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)

REQUIRED_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "unit_price",
    "invoice_timestamp",
    "customer_id",
    "country",
]


def _list_raw_csvs(raw_dir: Path) -> list[Path]:
    """List CSV files in raw directory."""
    if not raw_dir.exists():
        return []
    return sorted(raw_dir.glob("**/*.csv"))


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names."""
    # Convert column names to lowercase
    column_mapping = {col: col.lower() for col in df.columns}
    df = df.rename(columns=column_mapping)

    # Handle common column name variations
    column_variations = {
        'invoiceno': 'invoice_no',
        'stockcode': 'stock_code',
        'invoicedate': 'invoice_timestamp',
        'unitprice': 'unit_price',
        'customerid': 'customer_id'
    }

    df = df.rename(columns=column_variations)
    return df


def _read_raw_csvs(files: list[Path]) -> pd.DataFrame:
    """Read and combine CSV files."""
    if not files:
        raise FileNotFoundError(
            f"No raw CSV files found under {settings.raw_data_path.resolve()}"
        )

    job_id = str(uuid.uuid4())

    # Read all CSV files and normalize columns individually
    dfs = []
    for file_path in files:
        try:
            df = pd.read_csv(file_path, dtype=str)  # Read as strings initially

            # Normalize columns immediately after reading each file
            df = _normalize_columns(df)

            # Add source metadata
            df['source_file_path'] = str(file_path.absolute())
            df['source_file_type'] = 'csv'
            df['ingestion_job_id'] = job_id
            dfs.append(df)
            logger.info(f"Read {len(df)} records from {file_path.name} with columns: {list(df.columns)}")
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            continue

    if not dfs:
        raise ValueError("No CSV files could be read successfully")

    # Now combine dataframes (columns should be consistent)
    combined_df = pd.concat(dfs, ignore_index=True, sort=False)
    logger.info(f"Combined {len(combined_df)} total records from {len(dfs)} files")

    return combined_df


def _add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Add ingestion metadata."""
    current_timestamp = pd.Timestamp.now()

    df['ingestion_timestamp'] = current_timestamp
    df['schema_version'] = '1.0'
    df['row_id'] = range(1, len(df) + 1)

    return df


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure required columns exist."""
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None
            logger.info(f"Added missing column: {col}")

    return df


def _add_partitioning_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add date-based partitioning columns."""
    # Convert invoice_timestamp if possible
    if 'invoice_timestamp' in df.columns:
        df['invoice_timestamp'] = pd.to_datetime(df['invoice_timestamp'], errors='coerce')
        df['invoice_date'] = df['invoice_timestamp'].dt.date

    # Add ingestion_date for partitioning
    df['ingestion_date'] = pd.Timestamp.now().date()

    return df


def ingest_bronze() -> bool:
    """Main bronze ingestion function."""
    try:
        logger.info("Starting Pandas-based Bronze layer ingestion...")

        raw_dir = settings.raw_data_path
        out_dir = settings.bronze_path / "sales"

        # List CSV files
        files = _list_raw_csvs(raw_dir)
        logger.info(f"Found {len(files)} CSV files to process")

        if not files:
            logger.warning("No CSV files found in raw directory")
            return False

        # Read and process data (columns already normalized in _read_raw_csvs)
        df = _read_raw_csvs(files)
        df = _ensure_required_columns(df)
        df = _add_metadata(df)
        df = _add_partitioning_columns(df)

        # Create output directory
        out_dir.mkdir(parents=True, exist_ok=True)

        # Save as single Parquet file for simplicity
        output_file = out_dir / "bronze_sales.parquet"
        df.to_parquet(output_file, index=False)
        logger.info(f"Saved {len(df)} records to {output_file}")

        logger.info(f"Bronze ingest complete -> {out_dir} (format=parquet, Pandas mode)")
        return True

    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}")
        return False


def main() -> None:
    """Entry point for Bronze layer ingestion."""
    logger.info("Starting Windows-compatible Bronze layer ETL...")
    settings.validate_paths()

    success = ingest_bronze()
    if success:
        print("Bronze ingest completed successfully (Pandas-based, Windows-compatible)")
    else:
        print("Bronze ingest failed")
        exit(1)


class PandasBronzeProcessor:
    """
    Class-based Bronze processor for compatibility with the enhanced factory.
    Wraps the functional bronze processing logic.
    """

    def __init__(self, config=None):
        self.config = config
        self.logger = get_logger(__name__)

    def process(self, input_path=None, output_path=None):
        """Process bronze data using the functional approach."""
        if input_path:
            # Override default paths if provided
            original_raw = settings.raw_data_path
            original_bronze = settings.bronze_path

            try:
                settings.raw_data_path = Path(input_path)
                if output_path:
                    settings.bronze_path = Path(output_path)

                return ingest_bronze()
            finally:
                # Restore original paths
                settings.raw_data_path = original_raw
                settings.bronze_path = original_bronze
        else:
            return ingest_bronze()

    def validate_config(self):
        """Validate processor configuration."""
        return True  # Basic validation

    def get_metrics(self):
        """Get processing metrics."""
        return {}


if __name__ == "__main__":
    main()
