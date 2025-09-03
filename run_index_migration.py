#!/usr/bin/env python3
"""
Simple script to run composite index migration.
"""

import asyncio
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def main():
    """Run the composite index migration."""
    print("=== Composite Index Migration ===")

    # Set environment variables
    os.environ['ENVIRONMENT'] = 'development'
    os.environ['DATABASE_URL'] = 'sqlite:///./data/warehouse/retail.db'

    try:
        from sqlalchemy import create_engine

        # Create engine directly
        engine = create_engine('sqlite:///./data/warehouse/retail.db', echo=False)

        print("Database engine created successfully")

        # Import migration after setting up environment
        from core.database.migration_scripts.add_composite_indexes import (
            run_composite_index_migration,
        )

        print("Starting composite index migration...")
        results = await run_composite_index_migration(engine, dry_run=False)

        print("\n=== Migration Results ===")
        print(f"Success: {results['success']}")
        print(f"Indexes Created: {len(results['indexes_created'])}")
        print(f"Indexes Failed: {len(results['indexes_failed'])}")
        print(f"Total Time: {results['total_time_seconds']:.2f}s")

        if results['indexes_created']:
            print("\nSUCCESS: Created Indexes:")
            for idx in results['indexes_created']:
                print(f"  - {idx['name']}: {idx['description']}")

        if results['indexes_failed']:
            print("\nFAILED: Failed Indexes:")
            for idx in results['indexes_failed']:
                print(f"  - {idx['name']}: {idx['error']}")

        if results.get('performance_improvement'):
            print("\nPERFORMANCE: Performance Improvements:")
            for query, improvement in results['performance_improvement'].items():
                if improvement['improvement_pct'] is not None:
                    print(f"  - {query}: {improvement['improvement_pct']:.1f}% improvement "
                          f"({improvement['before_ms']:.1f}ms -> {improvement['after_ms']:.1f}ms)")
                    if improvement['meets_target']:
                        print("    SUCCESS: Meets <50ms target")
                    else:
                        print("    WARNING: Above 50ms target")

        print("\n=== Migration Complete ===")
        return results['success']

    except Exception as e:
        print(f"ERROR: Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
