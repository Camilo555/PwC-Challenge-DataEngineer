#!/usr/bin/env python3
"""
Windows-Compatible Bronze Layer ETL Runner
Uses Pandas instead of PySpark to avoid Hadoop/winutils dependencies
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from etl.bronze.pandas_bronze import main

if __name__ == "__main__":
    main()
