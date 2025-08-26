"""
Enterprise Testing Framework
Comprehensive testing framework for the PwC Enterprise Data Platform
"""
import asyncio
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union
from unittest.mock import AsyncMock, MagicMock

import pytest
import httpx
from fastapi.testclient import TestClient

from core.logging import get_logger
from core.config.base_config import BaseConfig


class TestType(Enum):
    """Test type classifications"""
    UNIT = "unit"
    INTEGRATION = "integration"
    E2E = "e2e"
    PERFORMANCE = "performance"
    SECURITY = "security"
    CONTRACT = "contract"
    SMOKE = "smoke"
    REGRESSION = "regression"


class TestSeverity(Enum):
    """Test severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TestStatus(Enum):
    """Test execution status"""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class TestResult:
    """Test execution result"""
    test_id: str
    test_name: str
    test_type: TestType
    status: TestStatus
    duration_seconds: float
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    assertions_passed: int = 0
    assertions_total: int = 0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.end_time is None and self.status in [TestStatus.PASSED, TestStatus.FAILED, TestStatus.ERROR]:
            self.end_time = datetime.now()


@dataclass
class TestSuite:
    """Test suite configuration"""
    suite_id: str
    name: str
    description: str
    test_type: TestType
    severity: TestSeverity
    tags: List[str]
    setup_hooks: List[Callable] = None
    teardown_hooks: List[Callable] = None
    timeout_seconds: int = 300
    retry_count: int = 0
    parallel: bool = False
    
    def __post_init__(self):
        if self.setup_hooks is None:
            self.setup_hooks = []
        if self.teardown_hooks is None:
            self.teardown_hooks = []


class BaseTestFramework(ABC):
    """Base class for all test frameworks"""
    
    def __init__(self, config: Optional[BaseConfig] = None):
        self.config = config or BaseConfig()
        self.logger = get_logger(__name__)
        self.test_results: List[TestResult] = []
        self.current_test: Optional[TestResult] = None
        
    @abstractmethod
    async def setup(self):
        """Setup test environment"""
        pass
    
    @abstractmethod
    async def teardown(self):
        """Cleanup test environment"""
        pass
    
    @asynccontextmanager
    async def test_context(self, test_name: str, test_type: TestType):
        """Context manager for individual tests"""
        test_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        self.current_test = TestResult(
            test_id=test_id,
            test_name=test_name,
            test_type=test_type,
            status=TestStatus.RUNNING,
            duration_seconds=0.0,
            start_time=start_time
        )
        
        try:
            yield self.current_test
            
            # Test completed successfully
            self.current_test.status = TestStatus.PASSED
            self.current_test.end_time = datetime.now()
            self.current_test.duration_seconds = (
                self.current_test.end_time - start_time
            ).total_seconds()
            
        except AssertionError as e:
            # Test assertion failed
            self.current_test.status = TestStatus.FAILED
            self.current_test.error_message = str(e)
            self.current_test.end_time = datetime.now()
            self.current_test.duration_seconds = (
                self.current_test.end_time - start_time
            ).total_seconds()
            raise
            
        except Exception as e:
            # Test error
            self.current_test.status = TestStatus.ERROR
            self.current_test.error_message = str(e)
            self.current_test.end_time = datetime.now()
            self.current_test.duration_seconds = (
                self.current_test.end_time - start_time
            ).total_seconds()
            raise
            
        finally:
            self.test_results.append(self.current_test)
    
    def assert_equals(self, actual: Any, expected: Any, message: str = ""):
        """Enhanced assertion with metadata tracking"""
        self.current_test.assertions_total += 1
        
        try:
            assert actual == expected, f"{message}. Expected: {expected}, Actual: {actual}"
            self.current_test.assertions_passed += 1
        except AssertionError:
            self.logger.error(f"Assertion failed: {message}. Expected: {expected}, Actual: {actual}")
            raise
    
    def assert_in_range(self, actual: float, min_val: float, max_val: float, message: str = ""):
        """Assert value is within range"""
        self.current_test.assertions_total += 1
        
        try:
            assert min_val <= actual <= max_val, f"{message}. Expected: {min_val} <= {actual} <= {max_val}"
            self.current_test.assertions_passed += 1
        except AssertionError:
            self.logger.error(f"Range assertion failed: {message}")
            raise
    
    def assert_response_time(self, duration_seconds: float, max_seconds: float, message: str = ""):
        """Assert response time is within acceptable limits"""
        self.current_test.assertions_total += 1
        
        try:
            assert duration_seconds <= max_seconds, f"{message}. Response time {duration_seconds}s exceeded {max_seconds}s"
            self.current_test.assertions_passed += 1
        except AssertionError:
            self.logger.error(f"Response time assertion failed: {message}")
            raise
    
    def get_test_summary(self) -> Dict[str, Any]:
        """Get test execution summary"""
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r.status == TestStatus.PASSED)
        failed_tests = sum(1 for r in self.test_results if r.status == TestStatus.FAILED)
        error_tests = sum(1 for r in self.test_results if r.status == TestStatus.ERROR)
        
        total_duration = sum(r.duration_seconds for r in self.test_results)
        avg_duration = total_duration / total_tests if total_tests > 0 else 0
        
        return {
            "total_tests": total_tests,
            "passed": passed_tests,
            "failed": failed_tests,
            "errors": error_tests,
            "success_rate": (passed_tests / total_tests * 100) if total_tests > 0 else 0,
            "total_duration": total_duration,
            "average_duration": avg_duration,
            "test_results": [asdict(r) for r in self.test_results]
        }


class APITestFramework(BaseTestFramework):
    """Framework for API testing"""
    
    def __init__(self, base_url: str, auth_token: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.base_url = base_url
        self.auth_token = auth_token
        self.client: Optional[httpx.AsyncClient] = None
        
    async def setup(self):
        """Setup API test client"""
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=30.0
        )
    
    async def teardown(self):
        """Cleanup API client"""
        if self.client:
            await self.client.aclose()
    
    async def get(self, endpoint: str, params: Optional[Dict] = None) -> httpx.Response:
        """Make GET request with timing"""
        start_time = time.time()
        response = await self.client.get(endpoint, params=params)
        duration = time.time() - start_time
        
        # Add response metadata
        if self.current_test:
            self.current_test.metadata.update({
                "last_request": {
                    "method": "GET",
                    "endpoint": endpoint,
                    "duration": duration,
                    "status_code": response.status_code
                }
            })
        
        return response
    
    async def post(self, endpoint: str, json_data: Optional[Dict] = None, 
                  data: Optional[Dict] = None) -> httpx.Response:
        """Make POST request with timing"""
        start_time = time.time()
        response = await self.client.post(endpoint, json=json_data, data=data)
        duration = time.time() - start_time
        
        # Add response metadata
        if self.current_test:
            self.current_test.metadata.update({
                "last_request": {
                    "method": "POST",
                    "endpoint": endpoint,
                    "duration": duration,
                    "status_code": response.status_code
                }
            })
        
        return response
    
    async def put(self, endpoint: str, json_data: Optional[Dict] = None) -> httpx.Response:
        """Make PUT request with timing"""
        start_time = time.time()
        response = await self.client.put(endpoint, json=json_data)
        duration = time.time() - start_time
        
        # Add response metadata
        if self.current_test:
            self.current_test.metadata.update({
                "last_request": {
                    "method": "PUT",
                    "endpoint": endpoint,
                    "duration": duration,
                    "status_code": response.status_code
                }
            })
        
        return response
    
    async def delete(self, endpoint: str) -> httpx.Response:
        """Make DELETE request with timing"""
        start_time = time.time()
        response = await self.client.delete(endpoint)
        duration = time.time() - start_time
        
        # Add response metadata
        if self.current_test:
            self.current_test.metadata.update({
                "last_request": {
                    "method": "DELETE",
                    "endpoint": endpoint,
                    "duration": duration,
                    "status_code": response.status_code
                }
            })
        
        return response
    
    def assert_status_code(self, response: httpx.Response, expected_code: int, message: str = ""):
        """Assert HTTP status code"""
        self.assert_equals(response.status_code, expected_code, 
                          message or f"HTTP status code should be {expected_code}")
    
    def assert_json_schema(self, response: httpx.Response, schema: Dict, message: str = ""):
        """Assert response matches JSON schema"""
        import jsonschema
        
        self.current_test.assertions_total += 1
        
        try:
            jsonschema.validate(response.json(), schema)
            self.current_test.assertions_passed += 1
        except jsonschema.ValidationError as e:
            self.logger.error(f"JSON schema validation failed: {e}")
            raise AssertionError(f"{message}. Schema validation failed: {e}")
    
    def assert_response_contains(self, response: httpx.Response, key: str, value: Any = None, message: str = ""):
        """Assert response contains key/value"""
        response_data = response.json()
        self.current_test.assertions_total += 1
        
        try:
            assert key in response_data, f"Response should contain key '{key}'"
            if value is not None:
                assert response_data[key] == value, f"Value for key '{key}' should be {value}"
            self.current_test.assertions_passed += 1
        except AssertionError:
            self.logger.error(f"Response assertion failed: {message}")
            raise


class DatabaseTestFramework(BaseTestFramework):
    """Framework for database testing"""
    
    def __init__(self, db_connection_string: str, **kwargs):
        super().__init__(**kwargs)
        self.db_connection_string = db_connection_string
        self.db_connection = None
    
    async def setup(self):
        """Setup database connection"""
        import asyncpg
        self.db_connection = await asyncpg.connect(self.db_connection_string)
    
    async def teardown(self):
        """Cleanup database connection"""
        if self.db_connection:
            await self.db_connection.close()
    
    async def execute_query(self, query: str, *args) -> List[Dict]:
        """Execute SQL query and return results"""
        start_time = time.time()
        
        try:
            results = await self.db_connection.fetch(query, *args)
            duration = time.time() - start_time
            
            # Add query metadata
            if self.current_test:
                self.current_test.metadata.update({
                    "last_query": {
                        "query": query[:100] + "..." if len(query) > 100 else query,
                        "duration": duration,
                        "row_count": len(results)
                    }
                })
            
            return [dict(row) for row in results]
            
        except Exception as e:
            self.logger.error(f"Database query failed: {e}")
            raise
    
    async def count_rows(self, table: str, where_clause: str = "") -> int:
        """Count rows in table"""
        query = f"SELECT COUNT(*) as count FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        result = await self.execute_query(query)
        return result[0]['count']
    
    def assert_row_count(self, actual_count: int, expected_count: int, message: str = ""):
        """Assert row count matches expected"""
        self.assert_equals(actual_count, expected_count, 
                          message or f"Row count should be {expected_count}")
    
    def assert_query_performance(self, duration_seconds: float, max_seconds: float = 1.0, message: str = ""):
        """Assert query performance"""
        self.assert_response_time(duration_seconds, max_seconds, 
                                 message or f"Query should complete within {max_seconds}s")


class PerformanceTestFramework(BaseTestFramework):
    """Framework for performance testing"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.performance_metrics: Dict[str, List[float]] = {}
    
    async def setup(self):
        """Setup performance monitoring"""
        pass
    
    async def teardown(self):
        """Cleanup performance monitoring"""
        pass
    
    async def measure_performance(self, operation_name: str, operation: Callable, *args, **kwargs):
        """Measure operation performance"""
        start_time = time.time()
        
        try:
            result = await operation(*args, **kwargs) if asyncio.iscoroutinefunction(operation) else operation(*args, **kwargs)
            duration = time.time() - start_time
            
            # Track performance metrics
            if operation_name not in self.performance_metrics:
                self.performance_metrics[operation_name] = []
            self.performance_metrics[operation_name].append(duration)
            
            # Add to current test metadata
            if self.current_test:
                if "performance_metrics" not in self.current_test.metadata:
                    self.current_test.metadata["performance_metrics"] = {}
                
                self.current_test.metadata["performance_metrics"][operation_name] = {
                    "duration": duration,
                    "timestamp": time.time()
                }
            
            return result, duration
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Performance measurement failed for {operation_name}: {e}")
            raise
    
    def assert_performance_percentile(self, operation_name: str, percentile: int, max_duration: float, message: str = ""):
        """Assert performance percentile"""
        if operation_name not in self.performance_metrics:
            raise AssertionError(f"No performance data for operation: {operation_name}")
        
        durations = sorted(self.performance_metrics[operation_name])
        index = int((percentile / 100.0) * len(durations))
        percentile_duration = durations[min(index, len(durations) - 1)]
        
        self.assert_response_time(percentile_duration, max_duration, 
                                 message or f"{percentile}th percentile for {operation_name} should be under {max_duration}s")
    
    def assert_average_performance(self, operation_name: str, max_avg_duration: float, message: str = ""):
        """Assert average performance"""
        if operation_name not in self.performance_metrics:
            raise AssertionError(f"No performance data for operation: {operation_name}")
        
        durations = self.performance_metrics[operation_name]
        avg_duration = sum(durations) / len(durations)
        
        self.assert_response_time(avg_duration, max_avg_duration,
                                 message or f"Average duration for {operation_name} should be under {max_avg_duration}s")
    
    def get_performance_summary(self, operation_name: str) -> Dict[str, float]:
        """Get performance summary for operation"""
        if operation_name not in self.performance_metrics:
            return {}
        
        durations = sorted(self.performance_metrics[operation_name])
        
        return {
            "count": len(durations),
            "min": min(durations),
            "max": max(durations),
            "average": sum(durations) / len(durations),
            "p50": durations[int(0.5 * len(durations))],
            "p95": durations[int(0.95 * len(durations))],
            "p99": durations[int(0.99 * len(durations))]
        }


class MockService:
    """Mock service for testing"""
    
    def __init__(self):
        self.call_history: List[Dict[str, Any]] = []
        self.responses: Dict[str, Any] = {}
        self.delays: Dict[str, float] = {}
    
    def set_response(self, method_name: str, response: Any):
        """Set mock response for method"""
        self.responses[method_name] = response
    
    def set_delay(self, method_name: str, delay_seconds: float):
        """Set delay for method call"""
        self.delays[method_name] = delay_seconds
    
    async def mock_call(self, method_name: str, *args, **kwargs):
        """Mock method call"""
        # Record call
        self.call_history.append({
            "method": method_name,
            "args": args,
            "kwargs": kwargs,
            "timestamp": time.time()
        })
        
        # Apply delay if configured
        if method_name in self.delays:
            await asyncio.sleep(self.delays[method_name])
        
        # Return configured response
        return self.responses.get(method_name, None)
    
    def get_call_count(self, method_name: Optional[str] = None) -> int:
        """Get call count for method or all methods"""
        if method_name:
            return sum(1 for call in self.call_history if call["method"] == method_name)
        return len(self.call_history)
    
    def reset(self):
        """Reset mock service state"""
        self.call_history.clear()
        self.responses.clear()
        self.delays.clear()


class TestDataFactory:
    """Factory for generating test data"""
    
    @staticmethod
    def create_test_user(user_id: Optional[str] = None, **overrides) -> Dict[str, Any]:
        """Create test user data"""
        base_user = {
            "user_id": user_id or str(uuid.uuid4()),
            "username": f"testuser_{int(time.time())}",
            "email": f"test_{int(time.time())}@example.com",
            "first_name": "Test",
            "last_name": "User",
            "is_active": True,
            "created_at": datetime.now().isoformat()
        }
        base_user.update(overrides)
        return base_user
    
    @staticmethod
    def create_test_sale(sale_id: Optional[str] = None, **overrides) -> Dict[str, Any]:
        """Create test sale data"""
        base_sale = {
            "sale_id": sale_id or str(uuid.uuid4()),
            "customer_id": str(uuid.uuid4()),
            "product_id": str(uuid.uuid4()),
            "quantity": 1,
            "unit_price": 29.99,
            "total_amount": 29.99,
            "currency": "USD",
            "transaction_date": datetime.now().isoformat(),
            "status": "completed"
        }
        base_sale.update(overrides)
        return base_sale
    
    @staticmethod
    def create_test_product(product_id: Optional[str] = None, **overrides) -> Dict[str, Any]:
        """Create test product data"""
        base_product = {
            "product_id": product_id or str(uuid.uuid4()),
            "name": f"Test Product {int(time.time())}",
            "description": "Test product description",
            "category": "electronics",
            "price": 99.99,
            "currency": "USD",
            "in_stock": True,
            "stock_quantity": 100
        }
        base_product.update(overrides)
        return base_product


class TestReporter:
    """Test result reporter"""
    
    def __init__(self, output_format: str = "json"):
        self.output_format = output_format
    
    def generate_report(self, test_results: List[TestResult], output_file: Optional[str] = None) -> str:
        """Generate test report"""
        if self.output_format == "json":
            return self._generate_json_report(test_results, output_file)
        elif self.output_format == "html":
            return self._generate_html_report(test_results, output_file)
        else:
            return self._generate_text_report(test_results, output_file)
    
    def _generate_json_report(self, test_results: List[TestResult], output_file: Optional[str] = None) -> str:
        """Generate JSON report"""
        report_data = {
            "summary": {
                "total_tests": len(test_results),
                "passed": sum(1 for r in test_results if r.status == TestStatus.PASSED),
                "failed": sum(1 for r in test_results if r.status == TestStatus.FAILED),
                "errors": sum(1 for r in test_results if r.status == TestStatus.ERROR),
                "total_duration": sum(r.duration_seconds for r in test_results),
                "generated_at": datetime.now().isoformat()
            },
            "test_results": [asdict(result) for result in test_results]
        }
        
        report_json = json.dumps(report_data, indent=2, default=str)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_json)
        
        return report_json
    
    def _generate_html_report(self, test_results: List[TestResult], output_file: Optional[str] = None) -> str:
        """Generate HTML report"""
        # Simplified HTML report
        total = len(test_results)
        passed = sum(1 for r in test_results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in test_results if r.status == TestStatus.FAILED)
        
        html_content = f"""
        <html>
        <head><title>Test Report</title></head>
        <body>
            <h1>Test Execution Report</h1>
            <h2>Summary</h2>
            <p>Total Tests: {total}</p>
            <p>Passed: {passed}</p>
            <p>Failed: {failed}</p>
            <p>Success Rate: {(passed/total*100):.1f}%</p>
            <h2>Test Results</h2>
            <table border="1">
                <tr><th>Test Name</th><th>Type</th><th>Status</th><th>Duration</th></tr>
        """
        
        for result in test_results:
            status_color = "green" if result.status == TestStatus.PASSED else "red"
            html_content += f"""
                <tr>
                    <td>{result.test_name}</td>
                    <td>{result.test_type.value}</td>
                    <td style="color: {status_color}">{result.status.value}</td>
                    <td>{result.duration_seconds:.2f}s</td>
                </tr>
            """
        
        html_content += """
            </table>
        </body>
        </html>
        """
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(html_content)
        
        return html_content
    
    def _generate_text_report(self, test_results: List[TestResult], output_file: Optional[str] = None) -> str:
        """Generate text report"""
        total = len(test_results)
        passed = sum(1 for r in test_results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in test_results if r.status == TestStatus.FAILED)
        
        report_text = f"""
TEST EXECUTION REPORT
====================

Summary:
- Total Tests: {total}
- Passed: {passed}
- Failed: {failed}
- Success Rate: {(passed/total*100):.1f}%
- Total Duration: {sum(r.duration_seconds for r in test_results):.2f}s

Test Results:
"""
        
        for result in test_results:
            report_text += f"- {result.test_name} ({result.test_type.value}): {result.status.value} ({result.duration_seconds:.2f}s)\n"
            if result.error_message:
                report_text += f"  Error: {result.error_message}\n"
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_text)
        
        return report_text


# Utility functions for common test operations
async def wait_for_condition(condition: Callable[[], bool], timeout_seconds: int = 30, 
                           check_interval: float = 0.5) -> bool:
    """Wait for a condition to become true"""
    start_time = time.time()
    
    while time.time() - start_time < timeout_seconds:
        try:
            if condition():
                return True
        except Exception:
            pass  # Ignore exceptions during condition check
        
        await asyncio.sleep(check_interval)
    
    return False


def create_test_environment_variables() -> Dict[str, str]:
    """Create test environment variables"""
    return {
        "ENVIRONMENT": "testing",
        "DATABASE_URL": "sqlite:///./test.db",
        "REDIS_URL": "redis://localhost:6379/1",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "LOG_LEVEL": "DEBUG",
        "AUTH_ENABLED": "false",
        "RATE_LIMITING_ENABLED": "false"
    }