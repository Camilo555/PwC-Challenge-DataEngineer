from __future__ import annotations

import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from core.config import settings
from etl.utils.spark import get_spark

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
    if not raw_dir.exists():
        return []
    return sorted(raw_dir.glob("**/*.csv"))


def _read_raw_csvs(spark: SparkSession, files: list[Path]) -> DataFrame:
    if not files:
        raise FileNotFoundError(
            f"No raw CSV files found under {settings.raw_data_path.resolve()}"
        )

    # Read all files at once for better performance
    job_id = str(uuid.uuid4())
    file_paths = [str(f.resolve()) for f in files]

    # Use multiline option for better CSV parsing
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiline", True)
        .option("escape", '"')
        .csv(file_paths)
    )

    # normalize columns to lower case in batch
    column_mapping = {c: c.lower() for c in df.columns}
    for old_name, new_name in column_mapping.items():
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    # Add metadata columns efficiently
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    df = df.withColumn("ingestion_job_id", F.lit(job_id))
    df = df.withColumn("schema_version", F.lit("1"))

    # Add file-specific metadata using input_file_name
    df = df.withColumn("source_file_path", F.input_file_name())

    # Calculate file size and type from path - more efficient than accessing each file
    df = df.withColumn("source_file_type", F.lit("csv"))

    # Generate stable row fingerprint for downstream quarantine matching
    fingerprint_columns = [
        "invoice_no", "stock_code", "description", "quantity",
        "unit_price", "invoice_timestamp", "customer_id", "country"
    ]

    parts = []
    for c in fingerprint_columns:
        if c in df.columns:
            if c == "invoice_timestamp":
                parts.append(F.coalesce(F.date_format(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"), F.lit("")))
            else:
                parts.append(F.coalesce(F.col(c).cast("string"), F.lit("")))
        else:
            parts.append(F.lit(""))

    df = df.withColumn("row_id", F.sha2(F.concat_ws("||", *parts), 256))

    # Cache for better performance during subsequent operations
    df.cache()

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

    # Add ingestion_date for partitioning (fallback to current_date if invoice_timestamp missing)
    dfw = df
    if "invoice_timestamp" in dfw.columns:
        dfw = dfw.withColumn("invoice_date", F.to_date("invoice_timestamp"))
    dfw = dfw.withColumn(
        "ingestion_date",
        F.coalesce(F.to_date("ingestion_timestamp"), F.current_date()),
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    # Use Parquet for Windows compatibility (Delta has native library issues)
    import platform
    if platform.system() == "Windows":
        (
            dfw.write.mode("overwrite")
            .format("parquet")
            .partitionBy("ingestion_date")
            .save(str(out_dir))
        )
        print(f"Bronze ingest complete -> {out_dir} (format=parquet, Windows mode)")
    else:
        (
            dfw.write.mode("overwrite")
            .format("delta")
            .option("mergeSchema", "true")
            .partitionBy("ingestion_date")
            .save(str(out_dir))
        )
        print(f"Bronze ingest complete -> {out_dir} (format=delta)")


def main() -> None:
    settings.validate_paths()
    ingest_bronze()


if __name__ == "__main__":
    main()
