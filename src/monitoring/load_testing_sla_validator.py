"""
Load Testing SLA Validator for <15ms Performance Verification
Validates production performance claims under realistic load conditions with comprehensive reporting
"""

import asyncio
import aiohttp
import time
import json
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import defaultdict, deque
import random
import yaml

from core.logging import get_logger
from core.config import get_settings

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class LoadTestConfig:
    """Configuration for load testing scenarios."""
    name: str
    duration_seconds: int
    concurrent_users: int
    requests_per_user: int
    ramp_up_seconds: int
    target_endpoints: List[str]
    request_patterns: Dict[str, Any]
    sla_threshold_ms: float = 15.0
    expected_success_rate: float = 99.0
    cache_warmup: bool = True
    realistic_delays: bool = True


@dataclass
class LoadTestResult:
    """Individual load test result."""
    endpoint: str
    method: str
    status_code: int
    response_time_ms: float
    response_time_us: int
    timestamp: float
    user_id: str
    request_size_bytes: int = 0
    response_size_bytes: int = 0
    error_message: Optional[str] = None
    
    @property
    def is_sla_compliant(self) -> bool:
        return self.response_time_ms < 15.0 and 200 <= self.status_code < 300
    
    @property
    def performance_tier(self) -> str:
        if self.response_time_ms < 5.0:
            return "Excellent"
        elif self.response_time_ms < 10.0:
            return "Good"  
        elif self.response_time_ms < 15.0:
            return "Acceptable"
        else:
            return "Poor"


@dataclass
class LoadTestSummary:
    """Summary statistics for load test execution."""
    config_name: str
    total_requests: int
    successful_requests: int
    failed_requests: int
    sla_compliant_requests: int
    
    # Response time statistics
    avg_response_time_ms: float
    min_response_time_ms: float
    max_response_time_ms: float
    p50_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    
    # Performance metrics
    requests_per_second: float
    success_rate_percentage: float
    sla_compliance_percentage: float
    
    # Business metrics
    business_value_validated: float
    estimated_cost_per_violation: float
    
    # Test execution details
    actual_duration_seconds: float
    concurrent_users: int
    total_data_transferred_mb: float
    
    @property
    def meets_sla_target(self) -> bool:
        return self.sla_compliance_percentage >= 95.0 and self.success_rate_percentage >= 99.0
    
    @property
    def performance_grade(self) -> str:
        if self.sla_compliance_percentage >= 95.0 and self.p95_response_time_ms < 10.0:
            return "A+"
        elif self.sla_compliance_percentage >= 95.0 and self.p95_response_time_ms < 15.0:
            return "A"
        elif self.sla_compliance_percentage >= 90.0:
            return "B"
        elif self.sla_compliance_percentage >= 80.0:
            return "C"
        else:
            return "F"


class LoadTestingSLAValidator:
    """
    Comprehensive load testing validator for <15ms SLA verification.
    
    Features:
    - Realistic production traffic simulation
    - Concurrent user simulation with authentic patterns
    - Cache warming and realistic data scenarios
    - Comprehensive performance reporting
    - SLA compliance validation under load
    - Business impact analysis
    """
    
    def __init__(self, 
                 base_url: str = "http://localhost:8000",
                 auth_token: Optional[str] = None,
                 max_concurrent_requests: int = 1000):
        self.base_url = base_url.rstrip('/')
        self.auth_token = auth_token
        self.max_concurrent_requests = max_concurrent_requests
        
        # Test results storage
        self.test_results: List[LoadTestResult] = []
        self.test_summaries: List[LoadTestSummary] = []
        
        # Real-time monitoring
        self.real_time_stats = {
            'requests_sent': 0,
            'responses_received': 0,
            'current_rps': 0.0,
            'current_p95_ms': 0.0,
            'sla_violations': 0
        }
        
        # Semaphore for rate limiting
        self.request_semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Business story mapping for value calculation
        self.story_values = {
            '/api/v1/sales': 2.5e6,    # BI Dashboards
            '/api/v1/analytics': 2.5e6,
            '/api/v1/datamart': 1.8e6,  # Data Quality
            '/api/v2/analytics': 2.1e6,  # API Performance
            '/api/v1/mobile': 2.8e6,    # Mobile Analytics
            '/api/v1/ai': 3.5e6,        # AI/LLM Analytics
            '/api/v1/ml': 6.4e6,        # ML Operations
            '/api/v1/streaming': 3.4e6   # Real-time Streaming
        }
        
        logger.info(f"Load Testing SLA Validator initialized for {base_url}")
    
    def load_test_scenarios(self, config_file: str) -> List[LoadTestConfig]:
        """Load test scenarios from YAML configuration file."""
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
            
            scenarios = []
            for scenario_data in config_data.get('scenarios', []):
                scenario = LoadTestConfig(**scenario_data)
                scenarios.append(scenario)
            
            logger.info(f"Loaded {len(scenarios)} test scenarios from {config_file}")
            return scenarios
            
        except Exception as e:
            logger.error(f"Failed to load test scenarios: {e}")
            return self._get_default_scenarios()
    
    def _get_default_scenarios(self) -> List[LoadTestConfig]:
        """Get default test scenarios for BMAD stories."""
        return [
            LoadTestConfig(
                name="Ultra-Fast API Response (<5ms target)",
                duration_seconds=300,  # 5 minutes
                concurrent_users=50,
                requests_per_user=100,
                ramp_up_seconds=30,
                target_endpoints=[
                    "/api/v1/sales/quick-stats",
                    "/api/v1/analytics/cached-metrics",
                    "/health"
                ],
                request_patterns={
                    "cache_hit_ratio": 0.8,
                    "data_size": "small",
                    "think_time_ms": 100
                },
                sla_threshold_ms=5.0
            ),
            LoadTestConfig(
                name="Production Load Simulation (<15ms SLA)",
                duration_seconds=600,  # 10 minutes
                concurrent_users=200,
                requests_per_user=50,
                ramp_up_seconds=60,
                target_endpoints=[
                    "/api/v1/sales/analytics",
                    "/api/v2/analytics/advanced-analytics",
                    "/api/v1/datamart/quality-metrics",
                    "/api/v1/mobile/analytics",
                    "/api/v1/ai/conversations"
                ],
                request_patterns={
                    "cache_hit_ratio": 0.7,
                    "data_size": "medium",
                    "think_time_ms": 500
                }
            ),
            LoadTestConfig(
                name="Peak Traffic Stress Test",
                duration_seconds=900,  # 15 minutes
                concurrent_users=500,
                requests_per_user=30,
                ramp_up_seconds=120,
                target_endpoints=[
                    "/api/v1/sales/analytics",
                    "/api/v2/analytics/advanced-analytics",
                    "/api/v1/ml/predictions",
                    "/api/v1/streaming/real-time-data"
                ],
                request_patterns={
                    "cache_hit_ratio": 0.6,
                    "data_size": "large",
                    "think_time_ms": 200
                }
            ),
            LoadTestConfig(
                name="Mobile Performance Validation",
                duration_seconds=300,
                concurrent_users=100,
                requests_per_user=40,
                ramp_up_seconds=30,
                target_endpoints=[
                    "/api/v1/mobile/analytics",
                    "/api/v1/mobile/dashboard",
                    "/api/v1/sales/mobile-optimized"
                ],
                request_patterns={
                    "cache_hit_ratio": 0.9,  # Mobile should have high cache hit
                    "data_size": "small",
                    "think_time_ms": 300
                }
            )
        ]
    
    async def run_load_test_scenario(self, config: LoadTestConfig) -> LoadTestSummary:
        """Execute a single load test scenario."""
        logger.info(f"Starting load test scenario: {config.name}")
        start_time = time.time()
        
        # Reset test results for this scenario
        scenario_results = []
        
        # Cache warmup if enabled
        if config.cache_warmup:
            await self._warmup_cache(config.target_endpoints)
        
        # Create concurrent tasks for users
        user_tasks = []
        users_per_second = config.concurrent_users / config.ramp_up_seconds if config.ramp_up_seconds > 0 else config.concurrent_users
        
        for user_id in range(config.concurrent_users):
            # Stagger user start times for ramp-up
            start_delay = (user_id / users_per_second) if config.ramp_up_seconds > 0 else 0
            
            task = asyncio.create_task(
                self._simulate_user(
                    config=config,
                    user_id=f"user_{user_id}",
                    start_delay=start_delay,
                    results_collector=scenario_results
                )
            )
            user_tasks.append(task)
        
        # Monitor progress
        monitoring_task = asyncio.create_task(
            self._monitor_test_progress(config, scenario_results, start_time)
        )
        
        # Wait for all users to complete
        try:
            await asyncio.gather(*user_tasks)
        except Exception as e:
            logger.error(f"Error during load test execution: {e}")
        finally:
            monitoring_task.cancel()
        
        # Calculate summary statistics
        actual_duration = time.time() - start_time
        summary = self._calculate_test_summary(config, scenario_results, actual_duration)
        
        # Store results
        self.test_results.extend(scenario_results)
        self.test_summaries.append(summary)
        
        logger.info(f"Load test scenario completed: {config.name}")
        logger.info(f"Results: {summary.sla_compliance_percentage:.1f}% SLA compliance, "
                   f"{summary.p95_response_time_ms:.2f}ms P95, Grade: {summary.performance_grade}")
        
        return summary
    
    async def _simulate_user(self, 
                           config: LoadTestConfig, 
                           user_id: str, 
                           start_delay: float,
                           results_collector: List[LoadTestResult]):
        """Simulate a single user's behavior during the load test."""
        
        # Wait for ramp-up delay
        if start_delay > 0:
            await asyncio.sleep(start_delay)
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers=self._get_request_headers()
        ) as session:
            
            user_start_time = time.time()
            requests_made = 0
            
            while (
                requests_made < config.requests_per_user and 
                (time.time() - user_start_time) < config.duration_seconds
            ):
                try:
                    # Select endpoint based on realistic patterns
                    endpoint = self._select_endpoint(config.target_endpoints, config.request_patterns)
                    
                    # Execute request with timing
                    result = await self._execute_request(session, endpoint, user_id, config)
                    results_collector.append(result)
                    
                    requests_made += 1
                    
                    # Realistic think time
                    if config.realistic_delays:
                        think_time = config.request_patterns.get("think_time_ms", 500) / 1000
                        think_time_variation = think_time * random.uniform(0.5, 1.5)  # ±50% variation
                        await asyncio.sleep(think_time_variation)
                
                except Exception as e:
                    logger.warning(f"User {user_id} request failed: {e}")
                    # Record failed request
                    results_collector.append(LoadTestResult(
                        endpoint=endpoint,
                        method="GET",
                        status_code=500,
                        response_time_ms=0.0,
                        response_time_us=0,
                        timestamp=time.time(),
                        user_id=user_id,
                        error_message=str(e)
                    ))
    
    async def _execute_request(self, 
                             session: aiohttp.ClientSession, 
                             endpoint: str, 
                             user_id: str,
                             config: LoadTestConfig) -> LoadTestResult:
        """Execute a single HTTP request with precise timing."""
        
        async with self.request_semaphore:
            url = f"{self.base_url}{endpoint}"
            
            # Start high-precision timing
            start_time = time.perf_counter()
            
            try:
                async with session.get(url) as response:
                    # Read response data
                    response_data = await response.read()
                    
                    # Stop timing
                    end_time = time.perf_counter()
                    
                    # Calculate precise response time
                    response_time_seconds = end_time - start_time
                    response_time_ms = response_time_seconds * 1000
                    response_time_us = int(response_time_seconds * 1_000_000)
                    
                    return LoadTestResult(
                        endpoint=endpoint,
                        method="GET",
                        status_code=response.status,
                        response_time_ms=response_time_ms,
                        response_time_us=response_time_us,
                        timestamp=time.time(),
                        user_id=user_id,
                        request_size_bytes=0,  # Could calculate from request
                        response_size_bytes=len(response_data)
                    )
            
            except Exception as e:
                end_time = time.perf_counter()
                response_time_ms = (end_time - start_time) * 1000
                
                return LoadTestResult(
                    endpoint=endpoint,
                    method="GET",
                    status_code=0,
                    response_time_ms=response_time_ms,
                    response_time_us=int(response_time_ms * 1000),
                    timestamp=time.time(),
                    user_id=user_id,
                    error_message=str(e)
                )
    
    def _select_endpoint(self, endpoints: List[str], patterns: Dict[str, Any]) -> str:
        """Select endpoint based on realistic usage patterns."""
        # Simple weighted selection - could be more sophisticated
        return random.choice(endpoints)
    
    def _get_request_headers(self) -> Dict[str, str]:
        """Get HTTP headers for requests."""
        headers = {
            "User-Agent": "LoadTestValidator/1.0",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate"
        }
        
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        
        return headers
    
    async def _warmup_cache(self, endpoints: List[str]):
        """Warm up cache before load testing."""
        logger.info("Warming up cache before load test...")
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers=self._get_request_headers()
        ) as session:
            
            warmup_tasks = []
            for endpoint in endpoints:
                # Hit each endpoint multiple times to warm cache
                for _ in range(3):
                    task = asyncio.create_task(
                        session.get(f"{self.base_url}{endpoint}")
                    )
                    warmup_tasks.append(task)
            
            try:
                await asyncio.gather(*warmup_tasks, return_exceptions=True)
                logger.info("Cache warmup completed")
            except Exception as e:
                logger.warning(f"Cache warmup partially failed: {e}")
    
    async def _monitor_test_progress(self, 
                                   config: LoadTestConfig, 
                                   results: List[LoadTestResult], 
                                   start_time: float):
        """Monitor test progress in real-time."""
        
        last_count = 0
        last_time = start_time
        
        while True:
            try:
                await asyncio.sleep(5)  # Update every 5 seconds
                
                current_time = time.time()
                current_count = len(results)
                
                if current_count > last_count:
                    # Calculate current RPS
                    time_diff = current_time - last_time
                    requests_diff = current_count - last_count
                    current_rps = requests_diff / time_diff if time_diff > 0 else 0
                    
                    # Calculate recent P95
                    recent_results = results[-min(100, len(results)):]  # Last 100 results
                    if recent_results:
                        recent_times = [r.response_time_ms for r in recent_results if r.status_code > 0]
                        if recent_times:
                            recent_times.sort()
                            p95_index = int(len(recent_times) * 0.95)
                            current_p95 = recent_times[p95_index] if p95_index < len(recent_times) else recent_times[-1]
                        else:
                            current_p95 = 0
                    else:
                        current_p95 = 0
                    
                    # Count SLA violations
                    sla_violations = sum(1 for r in results if not r.is_sla_compliant)
                    
                    # Update real-time stats
                    self.real_time_stats.update({
                        'requests_sent': current_count,
                        'responses_received': sum(1 for r in results if r.status_code > 0),
                        'current_rps': current_rps,
                        'current_p95_ms': current_p95,
                        'sla_violations': sla_violations
                    })
                    
                    elapsed_time = current_time - start_time
                    logger.info(f"Load test progress: {current_count} requests, "
                               f"{current_rps:.1f} RPS, "
                               f"P95: {current_p95:.2f}ms, "
                               f"SLA violations: {sla_violations}, "
                               f"Elapsed: {elapsed_time:.0f}s")
                    
                    last_count = current_count
                    last_time = current_time
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring test progress: {e}")
    
    def _calculate_test_summary(self, 
                              config: LoadTestConfig, 
                              results: List[LoadTestResult], 
                              actual_duration: float) -> LoadTestSummary:
        """Calculate comprehensive test summary statistics."""
        
        if not results:
            return LoadTestSummary(
                config_name=config.name,
                total_requests=0,
                successful_requests=0,
                failed_requests=0,
                sla_compliant_requests=0,
                avg_response_time_ms=0.0,
                min_response_time_ms=0.0,
                max_response_time_ms=0.0,
                p50_response_time_ms=0.0,
                p95_response_time_ms=0.0,
                p99_response_time_ms=0.0,
                requests_per_second=0.0,
                success_rate_percentage=0.0,
                sla_compliance_percentage=0.0,
                business_value_validated=0.0,
                estimated_cost_per_violation=0.0,
                actual_duration_seconds=actual_duration,
                concurrent_users=config.concurrent_users,
                total_data_transferred_mb=0.0
            )
        
        # Basic metrics
        total_requests = len(results)
        successful_requests = sum(1 for r in results if 200 <= r.status_code < 300)
        failed_requests = total_requests - successful_requests
        sla_compliant_requests = sum(1 for r in results if r.is_sla_compliant)
        
        # Response time statistics
        successful_results = [r for r in results if 200 <= r.status_code < 300]
        if successful_results:
            response_times = [r.response_time_ms for r in successful_results]
            response_times.sort()
            
            avg_response_time_ms = statistics.mean(response_times)
            min_response_time_ms = min(response_times)
            max_response_time_ms = max(response_times)
            p50_response_time_ms = statistics.median(response_times)
            p95_response_time_ms = response_times[int(len(response_times) * 0.95)]
            p99_response_time_ms = response_times[int(len(response_times) * 0.99)]
        else:
            avg_response_time_ms = min_response_time_ms = max_response_time_ms = 0.0
            p50_response_time_ms = p95_response_time_ms = p99_response_time_ms = 0.0
        
        # Performance metrics
        requests_per_second = total_requests / actual_duration if actual_duration > 0 else 0
        success_rate_percentage = (successful_requests / total_requests) * 100
        sla_compliance_percentage = (sla_compliant_requests / total_requests) * 100
        
        # Business metrics
        business_value_validated = self._calculate_business_value_validated(config.target_endpoints)
        estimated_cost_per_violation = self._calculate_cost_per_violation(config.target_endpoints)
        
        # Data transfer
        total_data_transferred_mb = sum(r.response_size_bytes for r in results) / (1024 * 1024)
        
        return LoadTestSummary(
            config_name=config.name,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            sla_compliant_requests=sla_compliant_requests,
            avg_response_time_ms=avg_response_time_ms,
            min_response_time_ms=min_response_time_ms,
            max_response_time_ms=max_response_time_ms,
            p50_response_time_ms=p50_response_time_ms,
            p95_response_time_ms=p95_response_time_ms,
            p99_response_time_ms=p99_response_time_ms,
            requests_per_second=requests_per_second,
            success_rate_percentage=success_rate_percentage,
            sla_compliance_percentage=sla_compliance_percentage,
            business_value_validated=business_value_validated,
            estimated_cost_per_violation=estimated_cost_per_violation,
            actual_duration_seconds=actual_duration,
            concurrent_users=config.concurrent_users,
            total_data_transferred_mb=total_data_transferred_mb
        )
    
    def _calculate_business_value_validated(self, endpoints: List[str]) -> float:
        """Calculate total business value being validated by the test."""
        total_value = 0.0
        for endpoint in endpoints:
            for path_prefix, value in self.story_values.items():
                if endpoint.startswith(path_prefix):
                    total_value += value
                    break
        return total_value
    
    def _calculate_cost_per_violation(self, endpoints: List[str]) -> float:
        """Estimate cost per SLA violation based on business value."""
        total_value = self._calculate_business_value_validated(endpoints)
        # Assume 0.01% of story value is at risk per violation
        return total_value * 0.0001
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive load testing report."""
        if not self.test_summaries:
            return {"error": "No test results available"}
        
        # Overall statistics
        overall_stats = {
            "total_scenarios": len(self.test_summaries),
            "total_requests": sum(s.total_requests for s in self.test_summaries),
            "total_sla_violations": sum(s.total_requests - s.sla_compliant_requests for s in self.test_summaries),
            "overall_sla_compliance": (sum(s.sla_compliant_requests for s in self.test_summaries) / 
                                     sum(s.total_requests for s in self.test_summaries)) * 100,
            "scenarios_meeting_sla": sum(1 for s in self.test_summaries if s.meets_sla_target),
            "total_business_value_validated": sum(s.business_value_validated for s in self.test_summaries)
        }
        
        # Performance analysis
        performance_analysis = {
            "best_performing_scenario": max(self.test_summaries, key=lambda s: s.sla_compliance_percentage).config_name,
            "worst_performing_scenario": min(self.test_summaries, key=lambda s: s.sla_compliance_percentage).config_name,
            "avg_p95_response_time": statistics.mean([s.p95_response_time_ms for s in self.test_summaries]),
            "max_concurrent_users_tested": max(s.concurrent_users for s in self.test_summaries),
            "peak_requests_per_second": max(s.requests_per_second for s in self.test_summaries)
        }
        
        # SLA validation results
        sla_validation = {
            "15ms_sla_validated": overall_stats["overall_sla_compliance"] >= 95.0,
            "production_ready": all(s.meets_sla_target for s in self.test_summaries),
            "recommended_max_concurrent_users": self._recommend_max_concurrent_users(),
            "performance_bottlenecks_identified": self._identify_bottlenecks()
        }
        
        return {
            "report_generated": datetime.utcnow().isoformat(),
            "overall_statistics": overall_stats,
            "performance_analysis": performance_analysis,
            "sla_validation": sla_validation,
            "scenario_summaries": [asdict(s) for s in self.test_summaries],
            "recommendations": self._generate_recommendations()
        }
    
    def _recommend_max_concurrent_users(self) -> int:
        """Recommend maximum concurrent users based on test results."""
        compliant_scenarios = [s for s in self.test_summaries if s.meets_sla_target]
        if compliant_scenarios:
            return max(s.concurrent_users for s in compliant_scenarios)
        else:
            return 0
    
    def _identify_bottlenecks(self) -> List[str]:
        """Identify performance bottlenecks from test results."""
        bottlenecks = []
        
        for summary in self.test_summaries:
            if summary.p95_response_time_ms > 15.0:
                bottlenecks.append(f"P95 response time exceeds 15ms in scenario '{summary.config_name}'")
            if summary.success_rate_percentage < 99.0:
                bottlenecks.append(f"Success rate below 99% in scenario '{summary.config_name}'")
            if summary.requests_per_second < 100:
                bottlenecks.append(f"Low throughput in scenario '{summary.config_name}'")
        
        return bottlenecks
    
    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on test results."""
        recommendations = []
        
        overall_sla_compliance = (sum(s.sla_compliant_requests for s in self.test_summaries) / 
                                sum(s.total_requests for s in self.test_summaries)) * 100
        
        if overall_sla_compliance >= 95.0:
            recommendations.append("✅ <15ms SLA is validated under load testing conditions")
            recommendations.append("✅ System is ready for production deployment")
        else:
            recommendations.append("❌ <15ms SLA not consistently met - performance optimization required")
            recommendations.append("🔧 Consider implementing additional caching layers")
            recommendations.append("🔧 Review database query optimization")
            recommendations.append("🔧 Consider horizontal scaling for high-traffic endpoints")
        
        avg_p95 = statistics.mean([s.p95_response_time_ms for s in self.test_summaries])
        if avg_p95 < 10.0:
            recommendations.append("🚀 Exceptional performance - consider marketing sub-10ms response times")
        elif avg_p95 > 15.0:
            recommendations.append("⚠️  P95 response time exceeds SLA - immediate optimization needed")
        
        return recommendations


# CLI interface for load testing
async def run_load_testing_validation(config_file: Optional[str] = None, 
                                    base_url: str = "http://localhost:8000",
                                    auth_token: Optional[str] = None) -> Dict[str, Any]:
    """Run comprehensive load testing validation."""
    
    validator = LoadTestingSLAValidator(base_url=base_url, auth_token=auth_token)
    
    # Load test scenarios
    if config_file:
        scenarios = validator.load_test_scenarios(config_file)
    else:
        scenarios = validator._get_default_scenarios()
    
    logger.info(f"Starting load testing validation with {len(scenarios)} scenarios")
    
    # Run all scenarios
    for scenario in scenarios:
        try:
            await validator.run_load_test_scenario(scenario)
        except Exception as e:
            logger.error(f"Failed to run scenario {scenario.name}: {e}")
    
    # Generate comprehensive report
    report = validator.generate_comprehensive_report()
    
    logger.info("Load testing validation completed")
    logger.info(f"Overall SLA compliance: {report['overall_statistics']['overall_sla_compliance']:.1f}%")
    
    return report


if __name__ == "__main__":
    import sys
    
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    config_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    report = asyncio.run(run_load_testing_validation(config_file, base_url))
    
    # Save report to file
    report_filename = f"load_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_filename, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Load testing report saved to: {report_filename}")
    
    # Print key results
    print(f"\n🎯 Load Testing Results Summary:")
    print(f"Overall SLA Compliance: {report['overall_statistics']['overall_sla_compliance']:.1f}%")
    print(f"Total Requests: {report['overall_statistics']['total_requests']:,}")
    print(f"SLA Violations: {report['overall_statistics']['total_sla_violations']:,}")
    print(f"Production Ready: {'✅ YES' if report['sla_validation']['production_ready'] else '❌ NO'}")