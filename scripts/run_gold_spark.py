#!/usr/bin/env python3
"""
Spark-based Gold Layer ETL Runner
Uses PySpark for advanced analytics and aggregations
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from etl.gold.spark_gold import main

if __name__ == "__main__":
    main()