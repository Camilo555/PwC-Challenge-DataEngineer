"""
Comprehensive Performance Benchmarking Suite

Advanced performance testing and validation system for API optimization
with detailed metrics collection and SLA validation.
"""

import asyncio
import json
import statistics
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable
from pathlib import Path

import aiohttp
import httpx
import pytest
from locust import HttpUser, task, between
from locust.env import Environment
from locust.stats import stats_printer, stats_history
from locust.log import setup_logging

from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BenchmarkRequest:
    """Individual benchmark request configuration."""
    name: str
    method: str
    path: str
    headers: Optional[Dict[str, str]] = None
    body: Optional[Dict[str, Any]] = None
    query_params: Optional[Dict[str, Any]] = None
    expected_status: int = 200
    timeout: float = 30.0
    weight: int = 1  # Relative frequency
    tags: List[str] = field(default_factory=list)


@dataclass
class BenchmarkResult:
    """Results from a single benchmark request."""
    request_name: str
    success: bool
    response_time_ms: float
    status_code: int
    response_size: int
    timestamp: datetime
    error: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class BenchmarkSummary:
    """Summary of benchmark execution."""
    test_name: str
    start_time: datetime
    end_time: datetime
    total_requests: int
    successful_requests: int
    failed_requests: int
    
    # Response time statistics
    min_response_time: float
    max_response_time: float
    avg_response_time: float
    median_response_time: float
    p90_response_time: float
    p95_response_time: float
    p99_response_time: float
    
    # Throughput statistics
    requests_per_second: float
    requests_per_minute: float
    
    # SLA validation
    sla_target_ms: float
    sla_compliance_rate: float
    sla_violations: int
    
    # Error analysis
    error_rate: float
    status_code_distribution: Dict[int, int]
    error_types: Dict[str, int]
    
    # Additional metrics
    total_data_transferred: int
    avg_response_size: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


class PerformanceBenchmark:
    """Advanced performance benchmarking system."""
    
    def __init__(self, 
                 base_url: str,
                 concurrent_users: int = 10,
                 spawn_rate: int = 2,
                 run_time: int = 60,
                 sla_target_ms: float = 50.0):
        
        self.base_url = base_url.rstrip('/')
        self.concurrent_users = concurrent_users
        self.spawn_rate = spawn_rate
        self.run_time = run_time
        self.sla_target_ms = sla_target_ms
        
        # Test configuration
        self.requests: List[BenchmarkRequest] = []
        self.results: List[BenchmarkResult] = []
        self.custom_headers = {}
        
        # HTTP clients
        self.session: Optional[aiohttp.ClientSession] = None
        self.sync_client: Optional[httpx.Client] = None
        
        # Statistics
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "response_times": [],
            "status_codes": defaultdict(int),
            "errors": defaultdict(int)
        }
    
    def add_request(self, request: BenchmarkRequest):
        """Add a request to the benchmark suite."""
        self.requests.append(request)
        logger.info(f"Added benchmark request: {request.name}")
    
    def set_auth_header(self, token: str):
        """Set authentication header for all requests."""
        self.custom_headers["Authorization"] = f"Bearer {token}"
    
    def set_custom_header(self, name: str, value: str):
        """Set custom header for all requests."""
        self.custom_headers[name] = value
    
    async def run_async_benchmark(self, 
                                 iterations: int = 100) -> BenchmarkSummary:
        """Run asynchronous benchmark test."""
        logger.info(f"Starting async benchmark with {iterations} iterations")
        
        start_time = datetime.utcnow()
        self.results = []
        
        # Create HTTP session
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers=self.custom_headers,
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=50)
        )
        
        try:
            # Execute benchmark requests
            tasks = []
            for i in range(iterations):
                for request in self.requests:
                    # Weight-based selection
                    for _ in range(request.weight):
                        task = self._execute_async_request(request, i)
                        tasks.append(task)
            
            # Execute all requests concurrently
            await asyncio.gather(*tasks, return_exceptions=True)
            
        finally:
            if self.session:
                await self.session.close()
        
        end_time = datetime.utcnow()
        
        return self._create_summary("Async Benchmark", start_time, end_time)
    
    async def _execute_async_request(self, request: BenchmarkRequest, iteration: int) -> BenchmarkResult:
        """Execute a single async request."""
        start_time = time.time()
        timestamp = datetime.utcnow()
        
        try:
            # Prepare request
            url = f"{self.base_url}{request.path}"
            headers = {**self.custom_headers, **(request.headers or {})}
            
            # Add query parameters
            params = request.query_params or {}
            
            # Execute request
            async with self.session.request(
                request.method.upper(),
                url,
                headers=headers,
                params=params,
                json=request.body,
                timeout=aiohttp.ClientTimeout(total=request.timeout)
            ) as response:
                
                response_time = (time.time() - start_time) * 1000
                content = await response.read()
                response_size = len(content)
                
                # Create result
                result = BenchmarkResult(
                    request_name=request.name,
                    success=response.status == request.expected_status,
                    response_time_ms=response_time,
                    status_code=response.status,
                    response_size=response_size,
                    timestamp=timestamp,
                    tags=request.tags
                )
                
                self.results.append(result)
                self._update_stats(result)
                
                return result
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            result = BenchmarkResult(
                request_name=request.name,
                success=False,
                response_time_ms=response_time,
                status_code=0,
                response_size=0,
                timestamp=timestamp,
                error=str(e),
                tags=request.tags
            )
            
            self.results.append(result)
            self._update_stats(result)
            
            return result
    
    def run_load_test(self, output_dir: Optional[str] = None) -> BenchmarkSummary:
        """Run comprehensive load test using Locust."""
        logger.info(f"Starting load test: {self.concurrent_users} users, {self.run_time}s duration")
        
        start_time = datetime.utcnow()
        
        # Setup Locust environment
        setup_logging("INFO", None)
        
        # Create user class dynamically
        user_class = self._create_locust_user_class()
        
        # Setup environment
        env = Environment(user_classes=[user_class])
        env.create_local_runner()
        
        # Start test
        env.runner.start(self.concurrent_users, spawn_rate=self.spawn_rate)
        
        # Run for specified duration
        time.sleep(self.run_time)
        
        # Stop test
        env.runner.stop()
        
        end_time = datetime.utcnow()
        
        # Extract results from Locust stats
        stats_data = env.runner.stats
        
        # Convert Locust stats to our format
        for name, stats_entry in stats_data.entries.items():
            if name != "Aggregated":  # Skip aggregated stats
                for i in range(stats_entry.num_requests):
                    result = BenchmarkResult(
                        request_name=name,
                        success=stats_entry.num_failures == 0,
                        response_time_ms=stats_entry.avg_response_time,
                        status_code=200 if stats_entry.num_failures == 0 else 500,
                        response_size=stats_entry.avg_content_length or 0,
                        timestamp=datetime.utcnow()
                    )
                    self.results.append(result)
        
        summary = self._create_summary("Load Test", start_time, end_time)
        
        # Save results if output directory specified
        if output_dir:
            self._save_results(summary, output_dir)
        
        return summary
    
    def _create_locust_user_class(self):
        """Create Locust user class from benchmark requests."""
        
        class BenchmarkUser(HttpUser):
            wait_time = between(0.1, 2.0)
            host = self.base_url
            
            def on_start(self):
                # Set custom headers
                for name, value in self.custom_headers.items():
                    self.client.headers[name] = value
        
        # Add tasks dynamically
        for request in self.requests:
            def create_task(req):
                def task_func(user_self):
                    with user_self.client.request(
                        req.method.upper(),
                        req.path,
                        headers=req.headers,
                        json=req.body,
                        params=req.query_params,
                        catch_response=True
                    ) as response:
                        if response.status_code != req.expected_status:
                            response.failure(f"Expected {req.expected_status}, got {response.status_code}")
                
                task_func.__name__ = f"task_{req.name.replace(' ', '_').lower()}"
                return task_func
            
            # Add task to class with weight
            task_func = task(weight=request.weight)(create_task(request))
            setattr(BenchmarkUser, f"task_{request.name.replace(' ', '_').lower()}", task_func)
        
        return BenchmarkUser
    
    def run_stress_test(self, 
                       max_users: int = 1000, 
                       step_size: int = 50, 
                       step_duration: int = 30) -> List[BenchmarkSummary]:
        """Run stress test with gradually increasing load."""
        logger.info(f"Starting stress test: 0 to {max_users} users, step size {step_size}")
        
        results = []
        current_users = 0
        
        while current_users <= max_users:
            logger.info(f"Testing with {current_users} concurrent users")
            
            # Temporarily set user count
            original_users = self.concurrent_users
            self.concurrent_users = current_users
            
            # Run load test for this user count
            summary = self.run_load_test()
            summary.test_name = f"Stress Test - {current_users} Users"
            results.append(summary)
            
            # Check if we're hitting critical failure rates
            if summary.error_rate > 0.1:  # 10% error rate
                logger.warning(f"High error rate ({summary.error_rate:.1%}) at {current_users} users")
                break
            
            current_users += step_size
        
        # Restore original user count
        self.concurrent_users = original_users
        
        return results
    
    def _update_stats(self, result: BenchmarkResult):
        """Update internal statistics."""
        self.stats["total_requests"] += 1
        
        if result.success:
            self.stats["successful_requests"] += 1
        else:
            self.stats["failed_requests"] += 1
            if result.error:
                self.stats["errors"][result.error] += 1
        
        self.stats["response_times"].append(result.response_time_ms)
        self.stats["status_codes"][result.status_code] += 1
    
    def _create_summary(self, test_name: str, start_time: datetime, end_time: datetime) -> BenchmarkSummary:
        """Create benchmark summary from results."""
        
        if not self.results:
            logger.warning("No results available for summary creation")
            return BenchmarkSummary(
                test_name=test_name,
                start_time=start_time,
                end_time=end_time,
                total_requests=0,
                successful_requests=0,
                failed_requests=0,
                min_response_time=0.0,
                max_response_time=0.0,
                avg_response_time=0.0,
                median_response_time=0.0,
                p90_response_time=0.0,
                p95_response_time=0.0,
                p99_response_time=0.0,
                requests_per_second=0.0,
                requests_per_minute=0.0,
                sla_target_ms=self.sla_target_ms,
                sla_compliance_rate=0.0,
                sla_violations=0,
                error_rate=0.0,
                status_code_distribution={},
                error_types={},
                total_data_transferred=0,
                avg_response_size=0.0
            )
        
        # Calculate statistics
        response_times = [r.response_time_ms for r in self.results]
        successful_results = [r for r in self.results if r.success]
        failed_results = [r for r in self.results if not r.success]
        
        # Response time statistics
        response_times.sort()
        total_requests = len(self.results)
        
        # Duration and throughput
        duration = (end_time - start_time).total_seconds()
        
        # SLA validation
        sla_violations = sum(1 for rt in response_times if rt > self.sla_target_ms)
        sla_compliance_rate = 1.0 - (sla_violations / total_requests) if total_requests > 0 else 0.0
        
        # Status code distribution
        status_distribution = defaultdict(int)
        for result in self.results:
            status_distribution[result.status_code] += 1
        
        # Error types
        error_types = defaultdict(int)
        for result in failed_results:
            if result.error:
                error_types[result.error] += 1
        
        # Data transfer
        total_data = sum(r.response_size for r in self.results)
        avg_response_size = total_data / total_requests if total_requests > 0 else 0.0
        
        return BenchmarkSummary(
            test_name=test_name,
            start_time=start_time,
            end_time=end_time,
            total_requests=total_requests,
            successful_requests=len(successful_results),
            failed_requests=len(failed_results),
            min_response_time=min(response_times) if response_times else 0.0,
            max_response_time=max(response_times) if response_times else 0.0,
            avg_response_time=statistics.mean(response_times) if response_times else 0.0,
            median_response_time=statistics.median(response_times) if response_times else 0.0,
            p90_response_time=self._percentile(response_times, 90) if response_times else 0.0,
            p95_response_time=self._percentile(response_times, 95) if response_times else 0.0,
            p99_response_time=self._percentile(response_times, 99) if response_times else 0.0,
            requests_per_second=total_requests / duration if duration > 0 else 0.0,
            requests_per_minute=(total_requests / duration) * 60 if duration > 0 else 0.0,
            sla_target_ms=self.sla_target_ms,
            sla_compliance_rate=sla_compliance_rate,
            sla_violations=sla_violations,
            error_rate=len(failed_results) / total_requests if total_requests > 0 else 0.0,
            status_code_distribution=dict(status_distribution),
            error_types=dict(error_types),
            total_data_transferred=total_data,
            avg_response_size=avg_response_size
        )
    
    def _percentile(self, values: List[float], percentile: float) -> float:
        """Calculate percentile from sorted values."""
        if not values:
            return 0.0
        
        k = (len(values) - 1) * (percentile / 100)
        f = int(k)
        c = f + 1
        
        if f == c:
            return values[f]
        
        d0 = values[f] * (c - k)
        d1 = values[min(c, len(values) - 1)] * (k - f)
        
        return d0 + d1
    
    def _save_results(self, summary: BenchmarkSummary, output_dir: str):
        """Save benchmark results to files."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Save summary
        summary_file = output_path / f"benchmark_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary.to_dict(), f, indent=2, default=str)
        
        # Save raw results
        results_file = output_path / f"benchmark_results_{timestamp}.json"
        with open(results_file, 'w') as f:
            results_data = [asdict(r) for r in self.results]
            json.dump(results_data, f, indent=2, default=str)
        
        logger.info(f"Benchmark results saved to {output_dir}")


class APIPipelineBenchmark(PerformanceBenchmark):
    """Specialized benchmark for API pipeline testing."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._setup_api_requests()
    
    def _setup_api_requests(self):
        """Setup comprehensive API endpoint tests."""
        
        # Health check endpoints
        self.add_request(BenchmarkRequest(
            name="Health Check",
            method="GET",
            path="/api/v1/health",
            weight=2,
            tags=["health", "monitoring"]
        ))
        
        # Authentication endpoints
        self.add_request(BenchmarkRequest(
            name="Authentication",
            method="POST",
            path="/api/v1/auth/token",
            body={"username": "test_user", "password": "test_pass"},
            weight=1,
            tags=["auth"]
        ))
        
        # Sales analytics endpoints
        self.add_request(BenchmarkRequest(
            name="Sales Analytics",
            method="GET",
            path="/api/v1/sales/analytics",
            query_params={
                "date_from": "2023-01-01",
                "date_to": "2023-12-31",
                "granularity": "monthly"
            },
            weight=3,
            tags=["analytics", "sales"]
        ))
        
        # DataMart endpoints
        self.add_request(BenchmarkRequest(
            name="DataMart Query",
            method="POST",
            path="/api/v1/datamart/query",
            body={
                "query": "SELECT * FROM fact_sales LIMIT 100",
                "format": "json"
            },
            weight=2,
            tags=["datamart", "query"]
        ))
        
        # GraphQL endpoints
        self.add_request(BenchmarkRequest(
            name="GraphQL Query",
            method="POST",
            path="/api/graphql",
            body={
                "query": """
                query {
                    sales(pagination: {first: 20}) {
                        edges {
                            node {
                                saleId
                                totalAmount
                                product {
                                    description
                                    category
                                }
                                customer {
                                    customerId
                                    customerSegment
                                }
                            }
                        }
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                        }
                    }
                }
                """
            },
            weight=2,
            tags=["graphql", "query"]
        ))
        
        # Batch operations
        self.add_request(BenchmarkRequest(
            name="Batch Operation",
            method="POST",
            path="/api/v1/batch",
            body={
                "requests": [
                    {"id": "req1", "method": "GET", "path": "/api/v1/health"},
                    {"id": "req2", "method": "GET", "path": "/api/v1/sales/summary"},
                    {"id": "req3", "method": "GET", "path": "/api/v1/customer/segments"}
                ],
                "execution_mode": "parallel"
            },
            weight=1,
            tags=["batch", "bulk"]
        ))
    
    async def run_comprehensive_benchmark(self, output_dir: str = "benchmark_results") -> Dict[str, Any]:
        """Run comprehensive API benchmark suite."""
        results = {}
        
        logger.info("Starting comprehensive API benchmark suite")
        
        # 1. Async performance test
        logger.info("Running async performance test...")
        async_result = await self.run_async_benchmark(iterations=50)
        results["async_performance"] = async_result.to_dict()
        
        # 2. Load test
        logger.info("Running load test...")
        load_result = self.run_load_test(output_dir)
        results["load_test"] = load_result.to_dict()
        
        # 3. Stress test (reduced scale for example)
        logger.info("Running stress test...")
        stress_results = self.run_stress_test(max_users=200, step_size=25, step_duration=15)
        results["stress_test"] = [r.to_dict() for r in stress_results]
        
        # 4. Generate comprehensive report
        report = self._generate_comprehensive_report(results)
        
        # Save comprehensive report
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_file = output_path / f"comprehensive_benchmark_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Comprehensive benchmark completed. Report saved to {report_file}")
        
        return report
    
    def _generate_comprehensive_report(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "test_configuration": {
                "base_url": self.base_url,
                "sla_target_ms": self.sla_target_ms,
                "concurrent_users": self.concurrent_users,
                "test_duration_seconds": self.run_time
            },
            "summary": {
                "total_test_scenarios": len(results),
                "overall_sla_compliance": self._calculate_overall_sla_compliance(results),
                "performance_grade": self._calculate_performance_grade(results),
                "recommendations": self._generate_recommendations(results)
            },
            "detailed_results": results,
            "performance_analysis": {
                "bottlenecks": self._identify_bottlenecks(results),
                "optimal_load": self._identify_optimal_load(results),
                "scalability_insights": self._generate_scalability_insights(results)
            }
        }
        
        return report
    
    def _calculate_overall_sla_compliance(self, results: Dict[str, Any]) -> float:
        """Calculate overall SLA compliance across all tests."""
        compliance_rates = []
        
        for test_name, test_results in results.items():
            if isinstance(test_results, dict) and 'sla_compliance_rate' in test_results:
                compliance_rates.append(test_results['sla_compliance_rate'])
            elif isinstance(test_results, list):
                # For stress test results
                for result in test_results:
                    if 'sla_compliance_rate' in result:
                        compliance_rates.append(result['sla_compliance_rate'])
        
        return statistics.mean(compliance_rates) if compliance_rates else 0.0
    
    def _calculate_performance_grade(self, results: Dict[str, Any]) -> str:
        """Calculate overall performance grade."""
        overall_compliance = self._calculate_overall_sla_compliance(results)
        
        if overall_compliance >= 0.99:
            return "A+ (Excellent)"
        elif overall_compliance >= 0.95:
            return "A (Very Good)"
        elif overall_compliance >= 0.90:
            return "B (Good)"
        elif overall_compliance >= 0.80:
            return "C (Average)"
        elif overall_compliance >= 0.70:
            return "D (Below Average)"
        else:
            return "F (Poor)"
    
    def _generate_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate performance optimization recommendations."""
        recommendations = []
        
        # Analyze results and provide recommendations
        overall_compliance = self._calculate_overall_sla_compliance(results)
        
        if overall_compliance < 0.95:
            recommendations.append("Consider implementing additional caching layers")
            recommendations.append("Optimize database queries and indexing")
            recommendations.append("Review connection pool configurations")
        
        # Check for high error rates
        for test_name, test_results in results.items():
            if isinstance(test_results, dict) and 'error_rate' in test_results:
                if test_results['error_rate'] > 0.05:  # 5% error rate
                    recommendations.append(f"High error rate detected in {test_name} - investigate error causes")
        
        # Check response time trends
        for test_name, test_results in results.items():
            if isinstance(test_results, dict) and 'p95_response_time' in test_results:
                if test_results['p95_response_time'] > self.sla_target_ms * 2:
                    recommendations.append(f"P95 response time in {test_name} exceeds target by 2x - consider optimization")
        
        if not recommendations:
            recommendations.append("Performance is within acceptable limits - continue monitoring")
        
        return recommendations
    
    def _identify_bottlenecks(self, results: Dict[str, Any]) -> List[str]:
        """Identify performance bottlenecks."""
        bottlenecks = []
        
        # Analyze for common bottleneck patterns
        for test_name, test_results in results.items():
            if isinstance(test_results, dict):
                if test_results.get('p99_response_time', 0) > self.sla_target_ms * 3:
                    bottlenecks.append(f"P99 response time bottleneck in {test_name}")
                
                if test_results.get('error_rate', 0) > 0.1:
                    bottlenecks.append(f"High error rate bottleneck in {test_name}")
        
        return bottlenecks
    
    def _identify_optimal_load(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Identify optimal load capacity."""
        if 'stress_test' in results and results['stress_test']:
            stress_results = results['stress_test']
            
            # Find the point where performance starts degrading
            optimal_users = 0
            best_compliance = 0
            
            for result in stress_results:
                if result['sla_compliance_rate'] > best_compliance:
                    best_compliance = result['sla_compliance_rate']
                    optimal_users = self._extract_user_count(result['test_name'])
            
            return {
                "optimal_concurrent_users": optimal_users,
                "max_tested_users": self._extract_user_count(stress_results[-1]['test_name']) if stress_results else 0,
                "recommendation": f"Optimal load appears to be around {optimal_users} concurrent users"
            }
        
        return {"message": "Insufficient data to determine optimal load"}
    
    def _extract_user_count(self, test_name: str) -> int:
        """Extract user count from test name."""
        import re
        match = re.search(r'(\d+)\s+Users', test_name)
        return int(match.group(1)) if match else 0
    
    def _generate_scalability_insights(self, results: Dict[str, Any]) -> List[str]:
        """Generate scalability insights."""
        insights = []
        
        if 'stress_test' in results and results['stress_test']:
            stress_results = results['stress_test']
            
            # Analyze scalability trends
            compliance_trend = [r['sla_compliance_rate'] for r in stress_results]
            response_time_trend = [r['p95_response_time'] for r in stress_results]
            
            if len(compliance_trend) > 1:
                # Check if compliance degrades linearly or exponentially
                compliance_degradation = compliance_trend[0] - compliance_trend[-1]
                
                if compliance_degradation > 0.2:
                    insights.append("System shows significant performance degradation under load")
                else:
                    insights.append("System maintains relatively stable performance under load")
            
            # Check for capacity limits
            last_result = stress_results[-1]
            if last_result['error_rate'] > 0.1:
                insights.append("System approaching capacity limits - errors increase significantly")
            
        if not insights:
            insights.append("Additional stress testing needed for comprehensive scalability analysis")
        
        return insights


# Factory functions
def create_api_benchmark(base_url: str, **kwargs) -> APIPipelineBenchmark:
    """Create API benchmark instance."""
    return APIPipelineBenchmark(base_url=base_url, **kwargs)


# Example usage and test runner
async def run_api_performance_tests(base_url: str = "http://localhost:8000",
                                  auth_token: Optional[str] = None,
                                  output_dir: str = "benchmark_results") -> Dict[str, Any]:
    """Run comprehensive API performance tests."""
    
    benchmark = create_api_benchmark(
        base_url=base_url,
        concurrent_users=20,
        spawn_rate=5,
        run_time=60,
        sla_target_ms=50.0
    )
    
    # Set authentication if provided
    if auth_token:
        benchmark.set_auth_header(auth_token)
    
    # Run comprehensive benchmark
    results = await benchmark.run_comprehensive_benchmark(output_dir)
    
    return results


if __name__ == "__main__":
    # Example CLI usage
    import argparse
    
    parser = argparse.ArgumentParser(description="Run API performance benchmarks")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Base URL for API")
    parser.add_argument("--auth-token", help="Authentication token")
    parser.add_argument("--output-dir", default="benchmark_results", help="Output directory")
    parser.add_argument("--users", type=int, default=20, help="Concurrent users")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")
    parser.add_argument("--sla-target", type=float, default=50.0, help="SLA target in milliseconds")
    
    args = parser.parse_args()
    
    async def main():
        benchmark = create_api_benchmark(
            base_url=args.base_url,
            concurrent_users=args.users,
            run_time=args.duration,
            sla_target_ms=args.sla_target
        )
        
        if args.auth_token:
            benchmark.set_auth_header(args.auth_token)
        
        results = await benchmark.run_comprehensive_benchmark(args.output_dir)
        
        print("\n" + "="*80)
        print("PERFORMANCE BENCHMARK RESULTS")
        print("="*80)
        print(f"Overall SLA Compliance: {results['summary']['overall_sla_compliance']:.1%}")
        print(f"Performance Grade: {results['summary']['performance_grade']}")
        print(f"Test Scenarios: {results['summary']['total_test_scenarios']}")
        print("\nRecommendations:")
        for rec in results['summary']['recommendations']:
            print(f"  • {rec}")
        print("="*80)
    
    asyncio.run(main())