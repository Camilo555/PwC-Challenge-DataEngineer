from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from de_challenge.core.config import settings
from de_challenge.etl.utils.spark import get_spark
from de_challenge.domain.entities.transaction import TransactionLine


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


def _read_bronze_sales(spark: SparkSession) -> DataFrame:
    src = (settings.bronze_path / "sales").resolve()
    if not src.exists():
        raise FileNotFoundError(f"Bronze dataset not found at {src}. Run Bronze ingest first.")
    # Prefer Delta format (new Bronze writes Delta); fallback to Parquet for legacy data
    try:
        df = spark.read.format("delta").load(str(src))
    except Exception:
        df = spark.read.parquet(str(src))
    # Normalize lowercase columns
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df


def _cast_and_clean(df: DataFrame) -> DataFrame:
    # Ensure all required columns exist
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))

    # Cast types
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
    df = df.withColumn("invoice_timestamp", F.to_timestamp("invoice_timestamp"))
    if "ingestion_timestamp" in df.columns:
        df = df.withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))

    # Basic validations
    df = df.where(F.col("invoice_no").isNotNull())
    df = df.where(F.col("stock_code").isNotNull())
    df = df.where(F.col("invoice_timestamp").isNotNull())
    df = df.where(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
    df = df.where(F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0))
    df = df.where(F.col("country").isNotNull())

    # Normalize and trim string columns (ensure string type first)
    str_cols = ["invoice_no", "stock_code", "description", "customer_id", "country"]
    for c in str_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c).cast("string")))

    # Stable row fingerprint to track validation results and build quarantine set
    # Use a deterministic concatenation of required fields with normalized types
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
        if c == "invoice_timestamp" and c in df.columns:
            parts.append(F.coalesce(F.date_format(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"), F.lit("")))
        else:
            parts.append(F.coalesce(F.col(c).cast("string"), F.lit("")))
    df = df.withColumn("row_id", F.sha2(F.concat_ws("||", *parts), 256))

    return df


def _validate_with_pydantic(df: DataFrame) -> DataFrame:
    """Validate each row using the TransactionLine Pydantic model.
    Invalid rows are dropped. Derived/cleaned fields from the model are applied back.
    """
    # Select only the required columns to feed the model
    cols = [
        "row_id",
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "invoice_timestamp",
        "customer_id",
        "country",
    ]
    select_cols = [c for c in cols if c in df.columns]
    sdf = df.select(*select_cols)

    def validate_partition(iter_rows):
        from decimal import Decimal
        import datetime as dt

        for row in iter_rows:
            d = row.asDict(recursive=True)
            try:
                item = TransactionLine(
                    invoice_no=str(d.get("invoice_no")) if d.get("invoice_no") is not None else "",
                    stock_code=str(d.get("stock_code")) if d.get("stock_code") is not None else "",
                    description=d.get("description"),
                    quantity=int(d.get("quantity")) if d.get("quantity") is not None else None,
                    invoice_date=d.get("invoice_timestamp"),
                    unit_price=Decimal(str(d.get("unit_price"))) if d.get("unit_price") is not None else None,
                    customer_id=str(d.get("customer_id")) if d.get("customer_id") is not None else None,
                    country=d.get("country"),
                )
                item.validate_business_rules()
                if item.has_errors:
                    continue
                yield {
                    "row_id": d.get("row_id"),
                    "invoice_no": item.invoice_no,
                    "stock_code": item.stock_code,
                    "description": item.description,
                    "quantity": int(item.quantity),
                    "unit_price": float(item.unit_price),
                    "invoice_timestamp": item.invoice_date,
                    "customer_id": item.customer_id,
                    "country": item.country,
                }
            except Exception:
                # Drop invalid rows
                continue

    rdd = sdf.rdd.mapPartitions(validate_partition)

    # Define schema explicitly to avoid inference failures on empty RDDs
    schema = T.StructType(
        [
            T.StructField("row_id", T.StringType(), True),
            T.StructField("invoice_no", T.StringType(), True),
            T.StructField("stock_code", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("quantity", T.IntegerType(), True),
            T.StructField("unit_price", T.DoubleType(), True),
            T.StructField("invoice_timestamp", T.TimestampType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
        ]
    )

    # If validation drops all rows, return an empty DataFrame with the expected schema
    first = rdd.take(1)
    if not first:
        return df.sparkSession.createDataFrame([], schema)

    return df.sparkSession.createDataFrame(rdd, schema=schema)


def _deduplicate(df: DataFrame) -> DataFrame:
    # Prefer latest by ingestion_timestamp if present; else by invoice_timestamp
    order_col = F.col("ingestion_timestamp") if "ingestion_timestamp" in df.columns else F.col("invoice_timestamp")
    w = Window.partitionBy("invoice_no", "stock_code", "customer_id").orderBy(order_col.desc())
    df = df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
    return df


def write_silver(df: DataFrame) -> Path:
    out_dir = (settings.silver_path / "sales").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    (
        df.write.mode("overwrite")
        .format("delta")
        .option("mergeSchema", "true")
        .save(str(out_dir))
    )
    # Also write rejected/quarantine records if present
    if "row_id" in df.columns:
        # Derive quarantine by anti-join with validated row_ids
        bronze_dir = (settings.bronze_path / "sales").resolve()
        spark = df.sparkSession
        try:
            bronze = spark.read.format("delta").load(str(bronze_dir))
        except Exception:
            bronze = spark.read.parquet(str(bronze_dir))
        if "row_id" in bronze.columns:
            valid_ids = df.select("row_id").dropDuplicates(["row_id"])
            quarantine = bronze.join(valid_ids, on=["row_id"], how="left_anti")
            qdir = (settings.silver_path / "quarantine" / "sales").resolve()
            qdir.mkdir(parents=True, exist_ok=True)
            quarantine.write.mode("overwrite").format("delta").save(str(qdir))
    return out_dir


def clean_to_silver() -> None:
    spark = get_spark("SilverClean")
    bronze_df = _read_bronze_sales(spark)
    cleaned = _cast_and_clean(bronze_df)
    validated = _validate_with_pydantic(cleaned)
    deduped = _deduplicate(validated)
    out = write_silver(deduped)
    print(f"Silver write complete -> {out}")


def main() -> None:
    settings.validate_paths()
    clean_to_silver()


if __name__ == "__main__":
    main()
