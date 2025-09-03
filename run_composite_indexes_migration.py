#!/usr/bin/env python3
"""
Script to run the composite indexes migration for fact table performance optimization.

Usage:
    python run_composite_indexes_migration.py [--dry-run]
"""

import asyncio
import sys
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from sqlalchemy import create_engine

from core.config import settings
from core.database.migration_scripts.add_composite_indexes import CompositeIndexMigration
from core.logging import get_logger

logger = get_logger(__name__)


async def main():
    """Run the composite indexes migration."""
    # Check for dry-run flag
    dry_run = "--dry-run" in sys.argv
    
    if dry_run:
        logger.info("Running in DRY-RUN mode - no indexes will be created")
    else:
        logger.info("Running migration to create composite indexes")
    
    try:
        # Get database connection
        database_url = settings.get_database_url(async_mode=False)
        engine = create_engine(database_url)
        
        # Initialize migration
        migration = CompositeIndexMigration(engine)
        
        # Execute migration
        results = await migration.execute(dry_run=dry_run)
        
        # Report results
        if results["success"]:
            logger.info(f"Migration completed successfully!")
            logger.info(f"Indexes created: {len(results['indexes_created'])}")
            if results["indexes_created"]:
                for index_name in results["indexes_created"]:
                    logger.info(f"  ✓ {index_name}")
            
            if results["indexes_failed"]:
                logger.warning(f"Indexes failed: {len(results['indexes_failed'])}")
                for index_name in results["indexes_failed"]:
                    logger.warning(f"  ✗ {index_name}")
        else:
            logger.error("Migration failed!")
            if "error" in results:
                logger.error(f"Error: {results['error']}")
    
    except Exception as e:
        logger.error(f"Migration script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())