from __future__ import annotations

import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

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


def _list_raw_csvs(raw_dir: Path) -> list[Path]:
    if not raw_dir.exists():
        return []
    return sorted(raw_dir.glob("**/*.csv"))


def _read_raw_csvs(spark: SparkSession, files: list[Path]) -> DataFrame:
    if not files:
        raise FileNotFoundError(
            f"No raw CSV files found under {settings.raw_data_path.resolve()}"
        )
    df = None
    job_id = str(uuid.uuid4())
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
        _df = _df.withColumn("source_file_size", F.lit(int(f.stat().st_size)))
        _df = _df.withColumn("source_file_type", F.lit(f.suffix.lower().lstrip(".")))
        _df = _df.withColumn("ingestion_job_id", F.lit(job_id))
        _df = _df.withColumn("schema_version", F.lit("1"))
        # stable row fingerprint for downstream quarantine matching
        parts = []
        for c in [
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "unit_price",
            "invoice_timestamp",
            "customer_id",
            "country",
        ]:
            if c in _df.columns:
                if c == "invoice_timestamp":
                    parts.append(F.coalesce(F.date_format(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"), F.lit("")))
                else:
                    parts.append(F.coalesce(F.col(c).cast("string"), F.lit("")))
            else:
                parts.append(F.lit(""))
        _df = _df.withColumn("row_id", F.sha2(F.concat_ws("||", *parts), 256))
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

    # Add ingestion_date for partitioning (fallback to current_date if invoice_timestamp missing)
    dfw = df
    if "invoice_timestamp" in dfw.columns:
        dfw = dfw.withColumn("invoice_date", F.to_date("invoice_timestamp"))
    dfw = dfw.withColumn(
        "ingestion_date",
        F.coalesce(F.to_date("ingestion_timestamp"), F.current_date()),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
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
