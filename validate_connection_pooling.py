#!/usr/bin/env python3
"""
Validate and test database connection pooling optimizations.
"""

import asyncio
import os
import sys
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_connection_pooling():
    """Test the optimized database connection pooling."""
    print("=== Database Connection Pooling Validation ===")

    # Set environment variables
    os.environ['ENVIRONMENT'] = 'development'
    os.environ['DATABASE_URL'] = 'sqlite:///./data/warehouse/retail.db'

    try:
        from core.config.base_config import BaseConfig, DatabaseType, Environment
        from core.database.connection_manager import DatabaseManager
        from core.database.pool_config import ConnectionPoolConfig, PoolStrategy

        # Test 1: Environment-specific pool configurations
        print("\n1. Testing Environment-specific Pool Configurations...")

        environments = [Environment.DEVELOPMENT, Environment.TESTING, Environment.STAGING, Environment.PRODUCTION]

        for env in environments:
            config = ConnectionPoolConfig.for_environment(env, DatabaseType.SQLITE)
            print(f"  {env.value.title()}:")
            print(f"    Pool Size: {config.pool_size}, Max Overflow: {config.max_overflow}")
            print(f"    Timeout: {config.pool_timeout}s, Recycle: {config.pool_recycle}s")
            print(f"    Async Pool: {config.async_pool_size}, Async Overflow: {config.async_max_overflow}")

        # Test 2: Strategy-based configurations
        print("\n2. Testing Strategy-based Pool Configurations...")

        strategies = [PoolStrategy.CONSERVATIVE, PoolStrategy.BALANCED, PoolStrategy.AGGRESSIVE]

        for strategy in strategies:
            config = ConnectionPoolConfig.for_strategy(strategy, DatabaseType.SQLITE)
            print(f"  {strategy.value.title()}:")
            print(f"    Pool Size: {config.pool_size}, Max Overflow: {config.max_overflow}")
            print(f"    Timeout: {config.pool_timeout}s, Health Check: {config.health_check_interval}s")

        # Test 3: Database Manager with Optimized Pooling
        print("\n3. Testing Database Manager with Optimized Pooling...")

        base_config = BaseConfig()
        base_config.environment = Environment.DEVELOPMENT

        # Create database manager with optimized pool config
        pool_config = ConnectionPoolConfig.for_environment(Environment.DEVELOPMENT, DatabaseType.SQLITE)
        db_manager = DatabaseManager(base_config, pool_config)

        print("  Initializing database manager...")
        await db_manager.initialize()

        # Test concurrent connections
        print("  Testing concurrent database connections...")

        async def test_query(session_id: int):
            """Test query with session ID."""
            start_time = time.time()
            try:
                async with db_manager.get_async_session() as session:
                    from sqlalchemy import text
                    result = await session.execute(text("SELECT COUNT(*) FROM fact_sale"))
                    count = result.scalar()
                    query_time = (time.time() - start_time) * 1000
                    return {'session_id': session_id, 'count': count, 'query_time_ms': query_time, 'success': True}
            except Exception as e:
                query_time = (time.time() - start_time) * 1000
                return {'session_id': session_id, 'error': str(e), 'query_time_ms': query_time, 'success': False}

        # Run concurrent queries to test pooling
        concurrent_queries = 5
        print(f"  Running {concurrent_queries} concurrent queries...")

        start_time = time.time()
        tasks = [test_query(i) for i in range(concurrent_queries)]
        results = await asyncio.gather(*tasks)
        total_time = (time.time() - start_time) * 1000

        successful_queries = [r for r in results if r['success']]
        failed_queries = [r for r in results if not r['success']]

        print("  Results:")
        print(f"    Total Time: {total_time:.2f}ms")
        print(f"    Successful Queries: {len(successful_queries)}/{concurrent_queries}")
        print(f"    Failed Queries: {len(failed_queries)}")
        print(f"    Average Query Time: {sum(r['query_time_ms'] for r in successful_queries)/len(successful_queries):.2f}ms")

        if successful_queries:
            print("    Individual Results:")
            for result in successful_queries:
                print(f"      Session {result['session_id']}: {result['query_time_ms']:.2f}ms - {result['count']} rows")

        # Test 4: Connection Pool Health and Metrics
        print("\n4. Testing Connection Pool Health and Metrics...")

        health_status = db_manager.get_health_status()
        print("  Health Status:")
        print(f"    Healthy: {health_status['healthy']}")
        print(f"    Last Check: {health_status['last_check']}")
        print(f"    Uptime: {health_status['uptime_seconds']:.1f}s")

        metrics = health_status['metrics']
        print("  Pool Metrics:")
        print(f"    Total Connections: {metrics['total_connections']}")
        print(f"    Active Connections: {metrics['active_connections']}")
        print(f"    Pool Hits: {metrics['pool_hits']}")
        print(f"    Pool Misses: {metrics['pool_misses']}")
        print(f"    Connection Errors: {metrics['connection_errors']}")
        print(f"    Avg Connection Time: {metrics['avg_connection_time_ms']:.2f}ms")

        # Test 5: Performance Benchmark
        print("\n5. Running Performance Benchmark...")

        benchmark_queries = [
            "SELECT COUNT(*) FROM fact_sale",
            "SELECT COUNT(*) FROM dim_customer WHERE customer_segment IS NOT NULL",
            "SELECT COUNT(*) FROM dim_product WHERE category IS NOT NULL",
            "SELECT AVG(total_amount) FROM fact_sale",
        ]

        benchmark_start = time.time()
        benchmark_results = []

        for i, query in enumerate(benchmark_queries):
            query_start = time.time()
            try:
                async with db_manager.get_async_session() as session:
                    from sqlalchemy import text
                    result = await session.execute(text(query))
                    value = result.scalar()
                    query_time = (time.time() - query_start) * 1000
                    benchmark_results.append({
                        'query_id': i + 1,
                        'query_time_ms': query_time,
                        'result': value,
                        'success': True
                    })
                    print(f"  Query {i + 1}: {query_time:.2f}ms")
            except Exception as e:
                query_time = (time.time() - query_start) * 1000
                benchmark_results.append({
                    'query_id': i + 1,
                    'query_time_ms': query_time,
                    'error': str(e),
                    'success': False
                })
                print(f"  Query {i + 1}: FAILED in {query_time:.2f}ms - {e}")

        benchmark_time = (time.time() - benchmark_start) * 1000
        successful_benchmarks = [r for r in benchmark_results if r['success']]

        print("  Benchmark Summary:")
        print(f"    Total Benchmark Time: {benchmark_time:.2f}ms")
        print(f"    Successful Queries: {len(successful_benchmarks)}/{len(benchmark_queries)}")
        if successful_benchmarks:
            avg_query_time = sum(r['query_time_ms'] for r in successful_benchmarks) / len(successful_benchmarks)
            print(f"    Average Query Time: {avg_query_time:.2f}ms")

            if avg_query_time < 10:
                print("    Performance: EXCELLENT (< 10ms)")
            elif avg_query_time < 50:
                print("    Performance: GOOD (< 50ms)")
            else:
                print("    Performance: NEEDS IMPROVEMENT (> 50ms)")

        # Cleanup
        await db_manager.shutdown()

        print("\n=== Connection Pooling Validation Complete ===")
        return True

    except Exception as e:
        print(f"ERROR: Connection pooling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_connection_pooling())
    print(f"\nResult: {'SUCCESS' if success else 'FAILED'}")
    sys.exit(0 if success else 1)
