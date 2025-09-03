#!/usr/bin/env python3
"""
Demo script for enhanced query caching with warming and intelligent invalidation.

This script demonstrates the new caching features:
- Cache warming for dashboard queries
- Cache warming for analytical queries
- Intelligent cascade invalidation
- Smart transaction-based invalidation
"""

import asyncio
import sys
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from sqlalchemy import create_engine

from core.config import settings
from data_access.caching.query_cache_manager import (
    QueryCacheManager,
    add_enhanced_caching_to_query_manager,
    get_query_cache_manager,
)
from core.logging import get_logger

logger = get_logger(__name__)


async def demo_enhanced_caching():
    """Demonstrate enhanced query caching features."""
    logger.info("Starting enhanced query caching demo")
    
    try:
        # Get database connection
        database_url = settings.get_database_url(async_mode=False)
        engine = create_engine(database_url)
        
        # Get query cache manager
        cache_manager = await get_query_cache_manager()
        
        # Add enhanced capabilities
        enhanced_cache_manager = add_enhanced_caching_to_query_manager(cache_manager)
        
        # Demonstrate cache warming
        logger.info("=== Cache Warming Demo ===")
        
        with engine.connect() as connection:
            # Warm dashboard cache
            dashboard_results = await enhanced_cache_manager.warm_dashboard_cache(connection)
            logger.info(f"Dashboard cache warming: {dashboard_results}")
            
            # Warm analytical cache
            analytical_results = await enhanced_cache_manager.warm_analytical_cache(connection, date_range_days=7)
            logger.info(f"Analytical cache warming: {analytical_results}")
        
        # Demonstrate intelligent invalidation
        logger.info("=== Intelligent Invalidation Demo ===")
        
        # Cascade invalidation
        cascade_results = await enhanced_cache_manager.cascade_invalidate("fact_sale", cascade_depth=2)
        logger.info(f"Cascade invalidation: {cascade_results}")
        
        # Smart transaction invalidation
        transaction_results = await enhanced_cache_manager.smart_transaction_invalidate(["dim_product", "fact_sale"])
        logger.info(f"Smart transaction invalidation: {transaction_results}")
        
        # Get cache statistics
        stats = await enhanced_cache_manager.get_cache_statistics()
        logger.info(f"Final cache statistics: {stats}")
        
        logger.info("Enhanced caching demo completed successfully!")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise


async def demo_cache_performance():
    """Demonstrate cache performance improvements."""
    logger.info("=== Cache Performance Demo ===")
    
    try:
        cache_manager = await get_query_cache_manager()
        enhanced_cache_manager = add_enhanced_caching_to_query_manager(cache_manager)
        
        database_url = settings.get_database_url(async_mode=False)
        engine = create_engine(database_url)
        
        sample_query = """
            SELECT 
                date_key, 
                COUNT(*) as transactions,
                SUM(line_amount) as revenue
            FROM fact_sale 
            WHERE date_key >= (SELECT MAX(date_key) - 7 FROM fact_sale)
            GROUP BY date_key
            ORDER BY date_key
        """
        
        with engine.connect() as connection:
            # First execution (cache miss)
            import time
            start_time = time.time()
            result1 = await enhanced_cache_manager.execute_with_cache(
                connection, sample_query, tags=["performance_demo"]
            )
            first_execution_time = time.time() - start_time
            
            # Second execution (cache hit)
            start_time = time.time()
            result2 = await enhanced_cache_manager.execute_with_cache(
                connection, sample_query, tags=["performance_demo"]
            )
            second_execution_time = time.time() - start_time
            
            logger.info(f"First execution (cache miss): {first_execution_time:.3f}s")
            logger.info(f"Second execution (cache hit): {second_execution_time:.3f}s")
            logger.info(f"Performance improvement: {((first_execution_time - second_execution_time) / first_execution_time * 100):.1f}%")
        
    except Exception as e:
        logger.error(f"Performance demo failed: {e}")


async def main():
    """Run all demos."""
    await demo_enhanced_caching()
    await demo_cache_performance()


if __name__ == "__main__":
    asyncio.run(main())