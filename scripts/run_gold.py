#!/usr/bin/env python3
"""
Run the Gold ETL build to load the SQLite/Postgres warehouse from Silver sales data.

Usage:
  poetry run python scripts/run_gold.py
  
Environment variables:
  USE_SPARK=true   - Use Spark-based Gold layer processing
  USE_SPARK=false  - Use pandas-based Gold layer processing (default)
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    """Run Gold layer processing based on environment configuration."""
    use_spark = os.getenv("USE_SPARK", "false").lower() == "true"

    if use_spark:
        print("Using Spark-based Gold layer processing...")
        from etl.gold.spark_gold import main as spark_main
        spark_main()
    else:
        print("Using pandas-based Gold layer processing...")
        from etl.gold.build_gold import main as pandas_main
        pandas_main()

if __name__ == "__main__":
    main()
