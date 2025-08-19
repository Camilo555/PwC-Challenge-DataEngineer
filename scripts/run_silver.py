import os

if __name__ == "__main__":
    use_spark = os.getenv("USE_SPARK", "false").lower() == "true"
    
    if use_spark:
        print("Using Spark-based Silver layer processing...")
        from etl.silver.clean_silver import clean_to_silver
        clean_to_silver()
    else:
        print("Using pandas-based Silver layer processing...")
        from etl.silver.pandas_silver import process_silver_layer
        process_silver_layer()
