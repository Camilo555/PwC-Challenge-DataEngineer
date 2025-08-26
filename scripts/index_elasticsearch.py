#!/usr/bin/env python3
"""
PwC Retail Data Platform - Elasticsearch Indexing Script
Indexes retail data into Elasticsearch for advanced search and analytics
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from core.logging import get_logger
from core.search import get_indexing_service, test_elasticsearch_connection

logger = get_logger(__name__)


async def main():
    """Main indexing function"""
    logger.info("Starting Elasticsearch indexing process")

    try:
        # Test connection first
        logger.info("Testing Elasticsearch connection...")
        health = await test_elasticsearch_connection()

        if health.get("status") != "healthy":
            logger.error(f"Elasticsearch is not healthy: {health}")
            return False

        logger.info(f"Elasticsearch health check passed: {health.get('cluster_status')}")

        # Get indexing service
        service = get_indexing_service()

        # Perform complete reindexing
        logger.info("Starting complete reindexing...")
        results = await service.reindex_all()

        # Log results
        logger.info("Reindexing completed!")
        logger.info(f"Setup results: {results.get('setup_indexes', {})}")
        logger.info(f"Transactions: {results.get('transactions', {})}")
        logger.info(f"Customers: {results.get('customers', {})}")
        logger.info(f"Products: {results.get('products', {})}")

        # Check for errors
        total_errors = 0
        total_indexed = 0

        for data_type in ['transactions', 'customers', 'products']:
            if data_type in results:
                errors = results[data_type].get('errors', 0)
                indexed = results[data_type].get('indexed', 0)
                total_errors += errors
                total_indexed += indexed

                if errors > 0:
                    logger.warning(f"{data_type}: {errors} errors out of {results[data_type].get('total', 0)} documents")
                    error_details = results[data_type].get('error_details', [])
                    for error in error_details[:3]:  # Show first 3 errors
                        logger.error(f"Error detail: {error}")

        logger.info(f"Total indexed: {total_indexed}, Total errors: {total_errors}")

        if total_errors == 0:
            logger.info("✅ All data indexed successfully!")
            return True
        else:
            logger.warning(f"⚠️  Indexing completed with {total_errors} errors")
            return total_errors < total_indexed * 0.1  # Less than 10% error rate is acceptable

    except Exception as e:
        logger.error(f"Indexing failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
