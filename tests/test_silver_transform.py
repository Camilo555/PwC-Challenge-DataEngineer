from __future__ import annotations

from datetime import datetime

from pyspark.sql import Row

from de_challenge.etl.utils.spark import get_spark
from de_challenge.etl.silver.clean_silver import _cast_and_clean, _deduplicate


def test_cast_and_clean_and_dedup():
    spark = get_spark("TestSilver")
    rows = [
        Row(
            invoice_no="INV-1",
            stock_code="10001",
            description="Widget",
            quantity=2,
            unit_price=1.5,
            invoice_timestamp="2024-01-02T12:00:00",
            customer_id="C001",
            country="United Kingdom",
            ingestion_timestamp="2024-01-02T12:00:05",
        ),
        # Duplicate with later ingestion_timestamp should win
        Row(
            invoice_no="INV-1",
            stock_code="10001",
            description="Widget v2",
            quantity=2,
            unit_price=1.5,
            invoice_timestamp="2024-01-02T12:00:00",
            customer_id="C001",
            country="United Kingdom",
            ingestion_timestamp="2024-01-02T12:00:06",
        ),
    ]
    df = spark.createDataFrame(rows)

    cleaned = _cast_and_clean(df)
    assert set([c.lower() for c in cleaned.columns]) >= {
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "invoice_timestamp",
        "customer_id",
        "country",
    }

    deduped = _deduplicate(cleaned)
    assert deduped.count() == 1
    row = deduped.collect()[0]
    assert row["description"].startswith("Widget")
