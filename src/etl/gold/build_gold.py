"""
Gold layer builder: loads curated Silver sales into SQLite warehouse star schema.

Input priority:
- Parquet directory: data/silver/sales/
- CSV file:          data/silver/sales.csv

Expected columns (case-insensitive, will normalize):
- invoice_no, stock_code, description, quantity, unit_price, invoice_timestamp, customer_id, country

Usage:
  poetry run python scripts/run_gold.py
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sqlmodel import select

from core.config import settings
from data_access.db import create_all, session_scope
from data_access.models.star_schema import (
    DimCountry,
    DimCustomer,
    DimDate,
    DimInvoice,
    DimProduct,
)
from etl.utils.spark import get_spark


def _date_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _read_silver_sales() -> DataFrame:
    spark = get_spark("GoldBuild")
    sales_dir = (settings.silver_path / "sales").resolve()
    sales_csv = (settings.silver_path / "sales.csv").resolve()

    if sales_dir.exists():
        # Prefer Delta format (Silver writes Delta)
        try:
            df = spark.read.format("delta").load(str(sales_dir))
        except Exception:
            # Fallback to parquet if legacy data exists
            df = spark.read.parquet(str(sales_dir))
    elif sales_csv.exists():
        df = spark.read.option("header", True).csv(str(sales_csv), inferSchema=True)
    else:
        raise FileNotFoundError(
            f"No Silver inputs found at {sales_dir} or {sales_csv}. Provide silver data first."
        )

    # Normalize column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    required = [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "invoice_timestamp",
        "customer_id",
        "country",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Silver data: {missing}")

    # Types
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
    df = df.withColumn("invoice_timestamp", F.to_timestamp("invoice_timestamp"))

    return df


def _upsert_dimension_keys(df: DataFrame) -> tuple[
    dict[str, int], dict[str, int], dict[str | None, int | None], dict[str, int], dict[str, int]
]:
    """Insert dims if missing and return natural->surrogate mappings for
    product(stock_code), country(name), customer(customer_id or None), invoice(invoice_no), date_key(str yyyy-mm-dd) -> date_key int.
    """
    create_all()

    # Collect distincts to driver (Gold build, expected manageable size)
    prod_rows = (
        df.select(F.col("stock_code").alias("stock_code"), F.col("description").alias("description"))
        .dropDuplicates(["stock_code"]).collect()
    )
    country_rows = df.select("country").dropDuplicates(["country"]).collect()
    cust_rows = df.select("customer_id").dropDuplicates(["customer_id"]).collect()
    inv_rows = df.select("invoice_no").dropDuplicates(["invoice_no"]).collect()
    date_rows = df.select(F.to_date("invoice_timestamp").alias("d")).dropDuplicates(["d"]).collect()

    prod_map: dict[str, int] = {}
    country_map: dict[str, int] = {}
    customer_map: dict[str | None, int | None] = {}
    invoice_map: dict[str, int] = {}
    date_map: dict[str, int] = {}

    with session_scope() as s:
        # Products
        for r in prod_rows:
            code = r["stock_code"]
            desc = r["description"]
            obj = s.exec(select(DimProduct).where(DimProduct.stock_code == code)).first()
            if not obj:
                obj = DimProduct(stock_code=code, description=desc)
                s.add(obj)
                s.flush()
                s.refresh(obj)
            prod_map[code] = obj.product_key or 0

        # Countries
        for r in country_rows:
            name = r["country"]
            country_obj = s.exec(select(DimCountry).where(DimCountry.country_name == name)).first()
            if not country_obj:
                country_obj = DimCountry(country_name=name)
                s.add(country_obj)
                s.flush()
                s.refresh(country_obj)
            country_map[name] = country_obj.country_key or 0

        # Customers (nullable)
        for r in cust_rows:
            cid = r["customer_id"]
            if cid is None:
                customer_map[None] = None
                continue
            customer_obj = s.exec(select(DimCustomer).where(DimCustomer.customer_id == cid)).first()
            if not customer_obj:
                customer_obj = DimCustomer(customer_id=cid)
                s.add(customer_obj)
                s.flush()
                s.refresh(customer_obj)
            customer_map[cid] = customer_obj.customer_key or 0

        # Invoices
        for r in inv_rows:
            ino = r["invoice_no"]
            invoice_obj = s.exec(select(DimInvoice).where(DimInvoice.invoice_no == ino)).first()
            if not invoice_obj:
                invoice_obj = DimInvoice(invoice_no=ino, invoice_status="Completed", invoice_type="Sale")
                s.add(invoice_obj)
                s.flush()
                s.refresh(invoice_obj)
            invoice_map[ino] = invoice_obj.invoice_key or 0

        # Dates
        for r in date_rows:
            d = r["d"]
            if d is None:
                # Skip invalid dates
                continue
            dk = _date_key(d)
            date_obj = s.get(DimDate, dk)
            if not date_obj:
                date_obj = DimDate(
                    date_key=dk, date=d, year=d.year, quarter=((d.month - 1) // 3) + 1, month=d.month, day=d.day
                )
                s.add(date_obj)
                s.flush()
            date_map[str(d)] = dk

    return prod_map, country_map, customer_map, invoice_map, date_map


def build_and_load_gold() -> None:
    """Read Silver sales, create dimensions if needed, and load FactSale via JDBC with idempotency."""
    df = _read_silver_sales()

    # Short-circuit if there is nothing to load
    if df.count() == 0:
        print("Gold load skipped. No rows found in Silver sales.")
        return
    prod_map, country_map, cust_map, inv_map, date_map = _upsert_dimension_keys(df)

    spark = df.sparkSession

    # Build mapping DataFrames from dicts for joins (handle empty maps with explicit schemas)
    prod_items = [(k, v) for k, v in prod_map.items()]
    prod_df = (
        spark.createDataFrame(prod_items, ["stock_code", "product_key"]) if prod_items else spark.createDataFrame([], "stock_code string, product_key int")
    )

    country_items = [(k, v) for k, v in country_map.items()]
    country_df = (
        spark.createDataFrame(country_items, ["country", "country_key"]) if country_items else spark.createDataFrame([], "country string, country_key int")
    )

    inv_items = [(k, v) for k, v in inv_map.items()]
    inv_df = (
        spark.createDataFrame(inv_items, ["invoice_no", "invoice_key"]) if inv_items else spark.createDataFrame([], "invoice_no string, invoice_key int")
    )

    # customer_id may contain None
    cust_items = [(k, v) for k, v in cust_map.items() if k is not None]
    cust_df = spark.createDataFrame(cust_items, ["customer_id", "customer_key"]) if cust_items else spark.createDataFrame([], "customer_id string, customer_key int")

    date_items = [(k, v) for k, v in date_map.items()]
    date_df = (
        spark.createDataFrame(date_items, ["dstr", "date_key"]).withColumnRenamed("dstr", "dstr")
        if date_items
        else spark.createDataFrame([], "dstr string, date_key int")
    )

    facts = (
        df
        .withColumn("dstr", F.to_date("invoice_timestamp").cast("string"))
        .join(prod_df, on="stock_code", how="left")
        .join(country_df, on="country", how="left")
        .join(inv_df, on="invoice_no", how="left")
        .join(date_df, on="dstr", how="left")
        .join(cust_df, on="customer_id", how="left")
        .select(
            F.col("date_key"),
            F.col("product_key"),
            F.col("customer_key"),
            F.col("country_key"),
            F.col("invoice_key"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("double").alias("unit_price"),
            (F.col("quantity").cast("double") * F.col("unit_price").cast("double")).alias("total"),
            F.col("invoice_timestamp"),
        )
        .na.drop(subset=["date_key", "product_key", "country_key", "invoice_key", "invoice_timestamp"])  # essential keys
    )

    # Idempotency: remove rows that already exist in fact_sale
    jdbc_url = settings.jdbc_url()
    props = settings.jdbc_properties()
    try:
        existing = spark.read.format("jdbc").options(url=jdbc_url, dbtable="fact_sale", **props).load()
    except Exception:
        # Table may not exist yet; use empty DF with needed columns
        existing = spark.createDataFrame([], "invoice_key int, product_key int, invoice_timestamp timestamp")
    existing_small = existing.select("invoice_key", "product_key", "invoice_timestamp").dropDuplicates(["invoice_key", "product_key", "invoice_timestamp"]) if existing.columns else existing
    to_insert = facts.join(existing_small, on=["invoice_key", "product_key", "invoice_timestamp"], how="left_anti")

    count_new = to_insert.count()
    if count_new == 0:
        print("Gold load complete. Inserted 0 fact rows, skipped all as duplicates.")
        return

    to_insert.write.format("jdbc").options(url=jdbc_url, dbtable="fact_sale", **props).mode("append").save()
    print(f"Gold load complete. Inserted {count_new} fact rows.")


def main() -> None:
    create_all()
    build_and_load_gold()


if __name__ == "__main__":
    main()
