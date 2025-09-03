#!/usr/bin/env python3
"""
Check database schema to understand existing columns.
"""

import sqlite3
from pathlib import Path


def check_schema():
    """Check the schema of all tables."""
    db_path = Path("data/warehouse/retail.db")

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    for table in sorted(tables):
        print(f"\n=== {table.upper()} ===")
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()

        for col in columns:
            cid, name, data_type, notnull, default, pk = col
            print(f"  {name}: {data_type} {'NOT NULL' if notnull else ''} {'PRIMARY KEY' if pk else ''}")

        # Show sample data
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  Rows: {count:,}")

        if count > 0:
            cursor.execute(f"SELECT * FROM {table} LIMIT 1")
            sample = cursor.fetchone()
            if sample:
                print(f"  Sample: {dict(zip([col[1] for col in columns], sample, strict=False))}")

    conn.close()

if __name__ == "__main__":
    check_schema()
