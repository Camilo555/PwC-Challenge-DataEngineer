"""
Run the Gold ETL build to load the SQLite/Postgres warehouse from Silver sales data.

Usage:
  poetry run python scripts/run_gold.py
"""

from etl.gold.build_gold import main

if __name__ == "__main__":
    main()
