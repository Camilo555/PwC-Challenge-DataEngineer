#!/usr/bin/env python3
"""
Spark-based Bronze Layer ETL Runner
Uses PySpark for scalable data processing
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from etl.bronze.spark_bronze import main

if __name__ == "__main__":
    main()