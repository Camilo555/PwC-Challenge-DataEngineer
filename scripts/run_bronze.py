import os

if __name__ == "__main__":
    use_spark = os.getenv("USE_SPARK", "false").lower() == "true"

    if use_spark:
        print("Using Spark-based Bronze layer processing...")
        from etl.bronze.ingest_bronze import ingest_bronze
        ingest_bronze()
    else:
        print("Using pandas-based Bronze layer processing...")
        from etl.bronze.pandas_bronze import ingest_bronze
        ingest_bronze()
