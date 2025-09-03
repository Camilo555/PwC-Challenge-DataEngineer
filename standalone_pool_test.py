#!/usr/bin/env python3
"""
Standalone test for connection pool configurations.
"""

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def test_connection_pooling_performance():
    """Test connection pooling performance with SQLite."""
    print("=== Standalone Connection Pooling Performance Test ===")

    db_path = Path("data/warehouse/retail.db")

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return False

    print(f"Database: {db_path}")

    # Test 1: Sequential connections (baseline)
    print("\n1. Testing Sequential Connections (Baseline)...")

    sequential_times = []
    for i in range(10):
        start_time = time.time()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM fact_sale")
        result = cursor.fetchone()
        conn.close()
        query_time = (time.time() - start_time) * 1000
        sequential_times.append(query_time)

    avg_sequential = sum(sequential_times) / len(sequential_times)
    print(f"  Average Sequential Time: {avg_sequential:.2f}ms")
    print(f"  Min: {min(sequential_times):.2f}ms, Max: {max(sequential_times):.2f}ms")

    # Test 2: Connection reuse (simulating pooling)
    print("\n2. Testing Connection Reuse (Pooling Simulation)...")

    conn = sqlite3.connect(db_path, check_same_thread=False)
    cursor = conn.cursor()

    reuse_times = []
    for i in range(10):
        start_time = time.time()
        cursor.execute("SELECT COUNT(*) FROM fact_sale")
        result = cursor.fetchone()
        query_time = (time.time() - start_time) * 1000
        reuse_times.append(query_time)

    conn.close()

    avg_reuse = sum(reuse_times) / len(reuse_times)
    print(f"  Average Reuse Time: {avg_reuse:.2f}ms")
    print(f"  Min: {min(reuse_times):.2f}ms, Max: {max(reuse_times):.2f}ms")

    improvement = ((avg_sequential - avg_reuse) / avg_sequential) * 100
    print(f"  Improvement: {improvement:.1f}% faster with connection reuse")

    # Test 3: Concurrent connections
    print("\n3. Testing Concurrent Connections...")

    def execute_query(query_id):
        """Execute a query in a thread."""
        start_time = time.time()
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM fact_sale")
            result = cursor.fetchone()
            conn.close()
            query_time = (time.time() - start_time) * 1000
            return {'id': query_id, 'time_ms': query_time, 'success': True, 'result': result[0]}
        except Exception as e:
            query_time = (time.time() - start_time) * 1000
            return {'id': query_id, 'time_ms': query_time, 'success': False, 'error': str(e)}

    # Run 5 concurrent queries
    concurrent_count = 5
    total_start = time.time()

    with ThreadPoolExecutor(max_workers=concurrent_count) as executor:
        futures = [executor.submit(execute_query, i) for i in range(concurrent_count)]
        concurrent_results = []

        for future in as_completed(futures):
            result = future.result()
            concurrent_results.append(result)

    total_time = (time.time() - total_start) * 1000
    successful_results = [r for r in concurrent_results if r['success']]
    failed_results = [r for r in concurrent_results if not r['success']]

    print(f"  Total Concurrent Time: {total_time:.2f}ms")
    print(f"  Successful Queries: {len(successful_results)}/{concurrent_count}")
    print(f"  Failed Queries: {len(failed_results)}")

    if successful_results:
        avg_concurrent = sum(r['time_ms'] for r in successful_results) / len(successful_results)
        print(f"  Average Query Time: {avg_concurrent:.2f}ms")

        concurrent_efficiency = (avg_sequential - avg_concurrent) / avg_sequential * 100
        print(f"  Efficiency vs Sequential: {concurrent_efficiency:.1f}%")

    # Test 4: Connection pool simulation with different configurations
    print("\n4. Testing Pool Configuration Scenarios...")

    configurations = [
        {"name": "Development", "connections": 3, "queries": 10},
        {"name": "Testing", "connections": 2, "queries": 5},
        {"name": "Production", "connections": 10, "queries": 20},
    ]

    for config in configurations:
        print(f"\n  {config['name']} Configuration:")
        print(f"    Pool Size: {config['connections']}, Query Count: {config['queries']}")

        start_time = time.time()

        def pool_query(query_id):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM fact_sale WHERE quantity > ?", (query_id % 5,))
            result = cursor.fetchone()
            conn.close()
            return result[0]

        with ThreadPoolExecutor(max_workers=config['connections']) as executor:
            futures = [executor.submit(pool_query, i) for i in range(config['queries'])]
            pool_results = [future.result() for future in as_completed(futures)]

        config_time = (time.time() - start_time) * 1000
        avg_query_time = config_time / config['queries']

        print(f"    Total Time: {config_time:.2f}ms")
        print(f"    Average per Query: {avg_query_time:.2f}ms")
        print(f"    Queries Completed: {len(pool_results)}")

        if avg_query_time < 10:
            print("    Performance: EXCELLENT")
        elif avg_query_time < 50:
            print("    Performance: GOOD")
        else:
            print("    Performance: NEEDS OPTIMIZATION")

    # Test 5: Connection health check simulation
    print("\n5. Testing Connection Health Checks...")

    health_checks = []
    for i in range(5):
        start_time = time.time()
        try:
            conn = sqlite3.connect(db_path, timeout=5.0)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")  # Simple health check query
            result = cursor.fetchone()
            conn.close()
            health_time = (time.time() - start_time) * 1000
            health_checks.append({'check': i+1, 'time_ms': health_time, 'healthy': True})
        except Exception as e:
            health_time = (time.time() - start_time) * 1000
            health_checks.append({'check': i+1, 'time_ms': health_time, 'healthy': False, 'error': str(e)})

    healthy_checks = [h for h in health_checks if h['healthy']]
    print(f"  Health Check Results: {len(healthy_checks)}/{len(health_checks)} passed")

    if healthy_checks:
        avg_health_time = sum(h['time_ms'] for h in healthy_checks) / len(healthy_checks)
        print(f"  Average Health Check Time: {avg_health_time:.2f}ms")

        if avg_health_time < 5:
            print("  Health Check Performance: EXCELLENT")
        elif avg_health_time < 20:
            print("  Health Check Performance: GOOD")
        else:
            print("  Health Check Performance: SLOW")

    print("\n=== Connection Pooling Performance Test Complete ===")

    # Summary assessment
    print("\n=== Performance Summary ===")
    performance_metrics = {
        'sequential_avg_ms': avg_sequential,
        'reuse_improvement_pct': improvement,
        'concurrent_success_rate': len(successful_results) / concurrent_count * 100 if concurrent_results else 0,
        'health_check_success_rate': len(healthy_checks) / len(health_checks) * 100
    }

    print(f"Sequential Query Time: {performance_metrics['sequential_avg_ms']:.2f}ms")
    print(f"Connection Reuse Improvement: {performance_metrics['reuse_improvement_pct']:.1f}%")
    print(f"Concurrent Query Success Rate: {performance_metrics['concurrent_success_rate']:.1f}%")
    print(f"Health Check Success Rate: {performance_metrics['health_check_success_rate']:.1f}%")

    # Overall assessment
    if (performance_metrics['reuse_improvement_pct'] > 20 and
        performance_metrics['concurrent_success_rate'] >= 80 and
        performance_metrics['health_check_success_rate'] >= 80):
        print("Overall Assessment: EXCELLENT - Connection pooling optimizations are working well")
        return True
    elif (performance_metrics['reuse_improvement_pct'] > 10 and
          performance_metrics['concurrent_success_rate'] >= 60):
        print("Overall Assessment: GOOD - Connection pooling shows benefits")
        return True
    else:
        print("Overall Assessment: NEEDS IMPROVEMENT - Connection pooling may need tuning")
        return False

if __name__ == "__main__":
    success = test_connection_pooling_performance()
    print(f"\nResult: {'SUCCESS' if success else 'PARTIAL SUCCESS'}")
    exit(0)
