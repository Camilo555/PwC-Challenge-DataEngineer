from __future__ import annotations

from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F

from de_challenge.core.config import settings
from de_challenge.etl.utils.spark import get_spark


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


def _list_raw_csvs(raw_dir: Path) -> List[Path]:
    if not raw_dir.exists():
        return []
    return sorted(list(raw_dir.glob("**/*.csv")))


def _read_raw_csvs(spark: SparkSession, files: List[Path]) -> DataFrame:
    if not files:
        raise FileNotFoundError(
            f"No raw CSV files found under {settings.raw_data_path.resolve()}"
        )
    df = None
    for f in files:
        _df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(f.resolve()))
        )
        # normalize columns to lower case
        for c in _df.columns:
            _df = _df.withColumnRenamed(c, c.lower())
        # add metadata
        _df = _df.withColumn("source_file_path", F.lit(str(f)))
        _df = _df.withColumn("ingestion_timestamp", F.current_timestamp())
        df = _df if df is None else df.unionByName(_df, allowMissingColumns=True)
    return df


def ingest_bronze() -> None:
    """Ingest raw CSVs into Bronze Parquet dataset at bronze_path/sales/.

    Expected columns (case-insensitive in raw):
    invoice_no, stock_code, description, quantity, unit_price, invoice_timestamp, customer_id, country
    """
    spark = get_spark("BronzeIngest")
    raw_dir = settings.raw_data_path
    out_dir = (settings.bronze_path / "sales").resolve()

    files = _list_raw_csvs(raw_dir)
    df = _read_raw_csvs(spark, files)

    # Standardize required columns presence (fill missing as null)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))

    # Write as Parquet partitioned by date (derived from invoice_timestamp if available)
    dfw = df
    if "invoice_timestamp" in dfw.columns:
        dfw = dfw.withColumn("invoice_date", F.to_date("invoice_timestamp"))

    out_dir.mkdir(parents=True, exist_ok=True)
    (
        dfw.write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(out_dir))
    )

    print(f"Bronze ingest complete -> {out_dir}")


def main() -> None:
    settings.validate_paths()
    ingest_bronze()


if __name__ == "__main__":
    main()
