"""
Comprehensive API Testing Framework
Advanced testing framework for API contract testing, load testing, and security testing.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import httpx

from api.services.microservices_orchestrator import get_microservices_orchestrator
from core.logging import get_logger

logger = get_logger(__name__)


class TestType(str, Enum):
    """Types of API tests."""

    UNIT = "unit"
    INTEGRATION = "integration"
    CONTRACT = "contract"
    LOAD = "load"
    SECURITY = "security"
    PERFORMANCE = "performance"
    MUTATION = "mutation"


class TestStatus(str, Enum):
    """Test execution status."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TestCase:
    """Individual test case definition."""

    test_id: str
    name: str
    description: str
    test_type: TestType
    endpoint: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    expected_status: int = 200
    expected_response_time_ms: float | None = None
    expected_schema: dict[str, Any] | None = None
    setup_func: Callable | None = None
    teardown_func: Callable | None = None
    tags: list[str] = field(default_factory=list)
    timeout: int = 30
    retries: int = 0


@dataclass
class TestResult:
    """Test execution result."""

    test_case: TestCase
    status: TestStatus
    execution_time_ms: float
    response_status: int | None = None
    response_body: dict[str, Any] | None = None
    response_headers: dict[str, str] | None = None
    error_message: str | None = None
    assertions_passed: int = 0
    assertions_failed: int = 0
    performance_metrics: dict[str, float] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class LoadTestConfig:
    """Load test configuration."""

    concurrent_users: int = 10
    ramp_up_time: int = 30  # seconds
    test_duration: int = 300  # seconds
    requests_per_second: int | None = None
    think_time_min: float = 0.5  # seconds
    think_time_max: float = 2.0  # seconds


@dataclass
class LoadTestResult:
    """Load test execution result."""

    total_requests: int
    successful_requests: int
    failed_requests: int
    avg_response_time: float
    min_response_time: float
    max_response_time: float
    p50_response_time: float
    p95_response_time: float
    p99_response_time: float
    requests_per_second: float
    error_rate: float
    throughput_mb_per_sec: float
    errors_by_status: dict[int, int] = field(default_factory=dict)
    response_time_distribution: list[float] = field(default_factory=list)


class ContractTesting:
    """API contract testing using OpenAPI specifications."""

    def __init__(self, openapi_spec: dict[str, Any]):
        self.openapi_spec = openapi_spec
        self.base_url = "http://localhost:8000"

    def generate_test_cases(self) -> list[TestCase]:
        """Generate test cases from OpenAPI specification."""
        test_cases = []

        if "paths" not in self.openapi_spec:
            return test_cases

        for path, path_spec in self.openapi_spec["paths"].items():
            for method, operation_spec in path_spec.items():
                if method.upper() not in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
                    continue

                # Generate positive test case
                test_case = TestCase(
                    test_id=f"contract_{method}_{path.replace('/', '_')}",
                    name=f"Contract test for {method.upper()} {path}",
                    description=operation_spec.get("description", ""),
                    test_type=TestType.CONTRACT,
                    endpoint=path,
                    method=method.upper(),
                    expected_status=self._get_success_status(operation_spec),
                    expected_schema=self._extract_response_schema(operation_spec),
                    tags=["contract", "openapi"],
                )

                # Add query parameters if defined
                if "parameters" in operation_spec:
                    test_case.query_params = self._generate_query_params(
                        operation_spec["parameters"]
                    )

                # Add request body if defined
                if "requestBody" in operation_spec:
                    test_case.body = self._generate_request_body(operation_spec["requestBody"])

                test_cases.append(test_case)

                # Generate negative test cases
                test_cases.extend(self._generate_negative_test_cases(path, method, operation_spec))

        return test_cases

    def _get_success_status(self, operation_spec: dict[str, Any]) -> int:
        """Extract expected success status code from operation spec."""
        responses = operation_spec.get("responses", {})

        for status_code in ["200", "201", "202", "204"]:
            if status_code in responses:
                return int(status_code)

        return 200  # Default

    def _extract_response_schema(self, operation_spec: dict[str, Any]) -> dict[str, Any] | None:
        """Extract response schema from operation spec."""
        responses = operation_spec.get("responses", {})

        for status_code in ["200", "201", "202"]:
            if status_code in responses:
                response_spec = responses[status_code]
                if "content" in response_spec:
                    content = response_spec["content"]
                    if "application/json" in content:
                        return content["application/json"].get("schema")

        return None

    def _generate_query_params(self, parameters: list[dict[str, Any]]) -> dict[str, Any]:
        """Generate query parameters from parameter specifications."""
        params = {}

        for param_spec in parameters:
            if param_spec.get("in") == "query":
                param_name = param_spec["name"]
                param_schema = param_spec.get("schema", {})

                # Generate value based on type
                if param_schema.get("type") == "string":
                    params[param_name] = "test_value"
                elif param_schema.get("type") == "integer":
                    params[param_name] = 123
                elif param_schema.get("type") == "boolean":
                    params[param_name] = True

        return params

    def _generate_request_body(self, request_body_spec: dict[str, Any]) -> dict[str, Any] | None:
        """Generate request body from specification."""
        if "content" not in request_body_spec:
            return None

        content = request_body_spec["content"]
        if "application/json" not in content:
            return None

        schema = content["application/json"].get("schema", {})
        return self._generate_data_from_schema(schema)

    def _generate_data_from_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Generate test data from JSON schema."""
        if schema.get("type") == "object":
            data = {}
            properties = schema.get("properties", {})

            for prop_name, prop_schema in properties.items():
                if prop_schema.get("type") == "string":
                    data[prop_name] = "test_string"
                elif prop_schema.get("type") == "integer":
                    data[prop_name] = 123
                elif prop_schema.get("type") == "number":
                    data[prop_name] = 123.45
                elif prop_schema.get("type") == "boolean":
                    data[prop_name] = True
                elif prop_schema.get("type") == "array":
                    data[prop_name] = []

            return data

        return {}

    def _generate_negative_test_cases(
        self, path: str, method: str, operation_spec: dict[str, Any]
    ) -> list[TestCase]:
        """Generate negative test cases (invalid inputs, etc.)."""
        negative_cases = []

        # Test with invalid authentication
        auth_test = TestCase(
            test_id=f"contract_{method}_{path.replace('/', '_')}_invalid_auth",
            name=f"Invalid auth test for {method.upper()} {path}",
            description="Test with invalid authentication",
            test_type=TestType.CONTRACT,
            endpoint=path,
            method=method.upper(),
            headers={"Authorization": "Bearer invalid_token"},
            expected_status=401,
            tags=["contract", "negative", "auth"],
        )
        negative_cases.append(auth_test)

        # Test with malformed request body for POST/PUT
        if method.upper() in ["POST", "PUT", "PATCH"] and "requestBody" in operation_spec:
            malformed_body_test = TestCase(
                test_id=f"contract_{method}_{path.replace('/', '_')}_malformed_body",
                name=f"Malformed body test for {method.upper()} {path}",
                description="Test with malformed request body",
                test_type=TestType.CONTRACT,
                endpoint=path,
                method=method.upper(),
                body={"invalid": "malformed data"},
                expected_status=400,
                tags=["contract", "negative", "validation"],
            )
            negative_cases.append(malformed_body_test)

        return negative_cases


class LoadTesting:
    """Load testing framework with advanced metrics collection."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.active_sessions = []

    async def run_load_test(
        self, test_cases: list[TestCase], config: LoadTestConfig
    ) -> LoadTestResult:
        """Run load test with specified configuration."""
        logger.info(
            f"Starting load test with {config.concurrent_users} users for {config.test_duration}s"
        )

        # Metrics collection
        request_times = []
        status_codes = []
        errors_by_status = {}
        total_bytes = 0

        # Create semaphore for concurrent users
        semaphore = asyncio.Semaphore(config.concurrent_users)

        # Start time
        start_time = time.time()
        end_time = start_time + config.test_duration

        # Create user sessions
        tasks = []
        for user_id in range(config.concurrent_users):
            task = asyncio.create_task(
                self._simulate_user_session(
                    user_id,
                    test_cases,
                    config,
                    semaphore,
                    start_time,
                    end_time,
                    request_times,
                    status_codes,
                    errors_by_status,
                )
            )
            tasks.append(task)

        # Wait for all users to complete
        await asyncio.gather(*tasks, return_exceptions=True)

        # Calculate metrics
        total_requests = len(request_times)
        successful_requests = sum(1 for code in status_codes if 200 <= code < 400)
        failed_requests = total_requests - successful_requests

        if request_times:
            sorted_times = sorted(request_times)
            avg_response_time = sum(request_times) / len(request_times)
            min_response_time = min(request_times)
            max_response_time = max(request_times)
            p50_response_time = sorted_times[int(0.5 * len(sorted_times))]
            p95_response_time = sorted_times[int(0.95 * len(sorted_times))]
            p99_response_time = sorted_times[int(0.99 * len(sorted_times))]
        else:
            avg_response_time = min_response_time = max_response_time = 0
            p50_response_time = p95_response_time = p99_response_time = 0

        actual_duration = time.time() - start_time
        requests_per_second = total_requests / actual_duration if actual_duration > 0 else 0
        error_rate = failed_requests / total_requests if total_requests > 0 else 0

        result = LoadTestResult(
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            avg_response_time=avg_response_time,
            min_response_time=min_response_time,
            max_response_time=max_response_time,
            p50_response_time=p50_response_time,
            p95_response_time=p95_response_time,
            p99_response_time=p99_response_time,
            requests_per_second=requests_per_second,
            error_rate=error_rate,
            throughput_mb_per_sec=total_bytes / (1024 * 1024 * actual_duration),
            errors_by_status=errors_by_status,
            response_time_distribution=request_times,
        )

        logger.info(f"Load test completed: {total_requests} requests, {error_rate:.2%} error rate")
        return result

    async def _simulate_user_session(
        self,
        user_id: int,
        test_cases: list[TestCase],
        config: LoadTestConfig,
        semaphore: asyncio.Semaphore,
        start_time: float,
        end_time: float,
        request_times: list[float],
        status_codes: list[int],
        errors_by_status: dict[int, int],
    ):
        """Simulate individual user session."""
        async with semaphore:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Ramp up delay
                ramp_up_delay = (user_id * config.ramp_up_time) / config.concurrent_users
                await asyncio.sleep(ramp_up_delay)

                while time.time() < end_time:
                    # Select random test case
                    import random

                    test_case = random.choice(test_cases)

                    try:
                        # Execute request
                        request_start = time.time()

                        response = await client.request(
                            method=test_case.method,
                            url=f"{self.base_url}{test_case.endpoint}",
                            headers=test_case.headers,
                            params=test_case.query_params,
                            json=test_case.body,
                            timeout=test_case.timeout,
                        )

                        request_time = (time.time() - request_start) * 1000  # ms
                        request_times.append(request_time)
                        status_codes.append(response.status_code)

                        # Track errors
                        if response.status_code >= 400:
                            errors_by_status[response.status_code] = (
                                errors_by_status.get(response.status_code, 0) + 1
                            )

                    except Exception as e:
                        # Request failed completely
                        request_times.append(config.test_duration * 1000)  # Max time
                        status_codes.append(0)  # Connection error
                        errors_by_status[0] = errors_by_status.get(0, 0) + 1
                        logger.debug(f"Request failed for user {user_id}: {e}")

                    # Think time between requests
                    think_time = random.uniform(config.think_time_min, config.think_time_max)
                    await asyncio.sleep(think_time)


class SecurityTesting:
    """Security testing framework for API vulnerabilities."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.vulnerability_patterns = {
            "sql_injection": [
                "' OR '1'='1",
                "'; DROP TABLE users; --",
                "1' UNION SELECT * FROM users--",
            ],
            "xss": [
                "<script>alert('XSS')</script>",
                "javascript:alert('XSS')",
                "<img src=x onerror=alert('XSS')>",
            ],
            "command_injection": ["; cat /etc/passwd", "| ls -la", "`whoami`"],
            "path_traversal": [
                "../../../etc/passwd",
                "..\\..\\..\\windows\\system32\\config\\sam",
                "%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd",
            ],
        }

    def generate_security_test_cases(self, base_test_cases: list[TestCase]) -> list[TestCase]:
        """Generate security test cases from base test cases."""
        security_test_cases = []

        for base_test in base_test_cases:
            # Authentication bypass tests
            security_test_cases.extend(self._generate_auth_bypass_tests(base_test))

            # Input validation tests
            security_test_cases.extend(self._generate_injection_tests(base_test))

            # Authorization tests
            security_test_cases.extend(self._generate_authorization_tests(base_test))

            # Rate limiting tests
            security_test_cases.extend(self._generate_rate_limit_tests(base_test))

        return security_test_cases

    def _generate_auth_bypass_tests(self, base_test: TestCase) -> list[TestCase]:
        """Generate authentication bypass tests."""
        auth_tests = []

        # Test without authentication
        no_auth_test = TestCase(
            test_id=f"{base_test.test_id}_no_auth",
            name=f"No auth bypass test for {base_test.endpoint}",
            description="Test accessing endpoint without authentication",
            test_type=TestType.SECURITY,
            endpoint=base_test.endpoint,
            method=base_test.method,
            query_params=base_test.query_params,
            body=base_test.body,
            expected_status=401,
            tags=["security", "auth", "bypass"],
        )
        auth_tests.append(no_auth_test)

        # Test with invalid tokens
        invalid_tokens = [
            "Bearer invalid_token",
            "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.invalid",
            "Basic invalid_base64",
            "Token malformed_token",
        ]

        for token in invalid_tokens:
            invalid_auth_test = TestCase(
                test_id=f"{base_test.test_id}_invalid_auth_{hash(token) % 1000}",
                name=f"Invalid auth test for {base_test.endpoint}",
                description="Test with invalid authentication token",
                test_type=TestType.SECURITY,
                endpoint=base_test.endpoint,
                method=base_test.method,
                headers={**base_test.headers, "Authorization": token},
                query_params=base_test.query_params,
                body=base_test.body,
                expected_status=401,
                tags=["security", "auth", "invalid"],
            )
            auth_tests.append(invalid_auth_test)

        return auth_tests

    def _generate_injection_tests(self, base_test: TestCase) -> list[TestCase]:
        """Generate injection vulnerability tests."""
        injection_tests = []

        for vuln_type, payloads in self.vulnerability_patterns.items():
            for payload in payloads:
                # Test in query parameters
                if base_test.query_params:
                    for param_name in base_test.query_params.keys():
                        modified_params = base_test.query_params.copy()
                        modified_params[param_name] = payload

                        injection_test = TestCase(
                            test_id=f"{base_test.test_id}_{vuln_type}_param_{param_name}",
                            name=f"{vuln_type.title()} test in {param_name} parameter",
                            description=f"Test {vuln_type} vulnerability in query parameter",
                            test_type=TestType.SECURITY,
                            endpoint=base_test.endpoint,
                            method=base_test.method,
                            headers=base_test.headers,
                            query_params=modified_params,
                            body=base_test.body,
                            expected_status=400,  # Should reject malicious input
                            tags=["security", "injection", vuln_type],
                        )
                        injection_tests.append(injection_test)

                # Test in request body
                if base_test.body and isinstance(base_test.body, dict):
                    for field_name in base_test.body.keys():
                        modified_body = base_test.body.copy()
                        modified_body[field_name] = payload

                        injection_test = TestCase(
                            test_id=f"{base_test.test_id}_{vuln_type}_body_{field_name}",
                            name=f"{vuln_type.title()} test in {field_name} field",
                            description=f"Test {vuln_type} vulnerability in request body",
                            test_type=TestType.SECURITY,
                            endpoint=base_test.endpoint,
                            method=base_test.method,
                            headers=base_test.headers,
                            query_params=base_test.query_params,
                            body=modified_body,
                            expected_status=400,  # Should reject malicious input
                            tags=["security", "injection", vuln_type],
                        )
                        injection_tests.append(injection_test)

        return injection_tests

    def _generate_authorization_tests(self, base_test: TestCase) -> list[TestCase]:
        """Generate authorization tests."""
        authz_tests = []

        # Test with different user roles (if applicable)
        test_roles = ["user", "admin", "guest", "service"]

        for role in test_roles:
            # This would require actual token generation for different roles
            # Placeholder implementation
            role_test = TestCase(
                test_id=f"{base_test.test_id}_role_{role}",
                name=f"Authorization test with {role} role",
                description=f"Test access with {role} role",
                test_type=TestType.SECURITY,
                endpoint=base_test.endpoint,
                method=base_test.method,
                headers={**base_test.headers, "X-User-Role": role},
                query_params=base_test.query_params,
                body=base_test.body,
                expected_status=base_test.expected_status,  # May vary by role
                tags=["security", "authorization", "rbac"],
            )
            authz_tests.append(role_test)

        return authz_tests

    def _generate_rate_limit_tests(self, base_test: TestCase) -> list[TestCase]:
        """Generate rate limiting tests."""
        rate_limit_test = TestCase(
            test_id=f"{base_test.test_id}_rate_limit",
            name=f"Rate limit test for {base_test.endpoint}",
            description="Test rate limiting behavior",
            test_type=TestType.SECURITY,
            endpoint=base_test.endpoint,
            method=base_test.method,
            headers=base_test.headers,
            query_params=base_test.query_params,
            body=base_test.body,
            expected_status=429,  # Too Many Requests
            tags=["security", "rate_limit"],
        )

        return [rate_limit_test]


class ComprehensiveAPITester:
    """
    Main API testing orchestrator that coordinates all testing types.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        openapi_spec_url: str = "http://localhost:8000/openapi.json",
    ):
        self.base_url = base_url
        self.openapi_spec_url = openapi_spec_url
        self.contract_tester = None
        self.load_tester = LoadTesting(base_url)
        self.security_tester = SecurityTesting(base_url)
        self.performance_optimizer = None
        self.orchestrator = get_microservices_orchestrator()

        # Test results storage
        self.test_results: list[TestResult] = []
        self.test_suites: dict[str, list[TestCase]] = {}

    async def initialize(self):
        """Initialize testing framework."""
        try:
            # Load OpenAPI specification
            async with httpx.AsyncClient() as client:
                response = await client.get(self.openapi_spec_url)
                if response.status_code == 200:
                    openapi_spec = response.json()
                    self.contract_tester = ContractTesting(openapi_spec)
                    logger.info("OpenAPI specification loaded successfully")
                else:
                    logger.warning(f"Could not load OpenAPI spec: {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to initialize API tester: {e}")

    async def generate_comprehensive_test_suite(self) -> dict[str, list[TestCase]]:
        """Generate comprehensive test suite for all test types."""
        test_suites = {
            TestType.CONTRACT.value: [],
            TestType.LOAD.value: [],
            TestType.SECURITY.value: [],
            TestType.PERFORMANCE.value: [],
        }

        # Generate contract tests from OpenAPI spec
        if self.contract_tester:
            test_suites[TestType.CONTRACT.value] = self.contract_tester.generate_test_cases()
            logger.info(f"Generated {len(test_suites[TestType.CONTRACT.value])} contract tests")

        # Generate load test cases (subset of contract tests)
        base_load_tests = test_suites[TestType.CONTRACT.value][:10]  # Limit for load testing
        for test_case in base_load_tests:
            load_test = TestCase(
                test_id=f"load_{test_case.test_id}",
                name=f"Load test - {test_case.name}",
                description=f"Load testing for {test_case.endpoint}",
                test_type=TestType.LOAD,
                endpoint=test_case.endpoint,
                method=test_case.method,
                headers=test_case.headers,
                query_params=test_case.query_params,
                body=test_case.body,
                expected_status=test_case.expected_status,
                tags=["load", "performance"],
            )
            test_suites[TestType.LOAD.value].append(load_test)

        # Generate security tests
        test_suites[TestType.SECURITY.value] = self.security_tester.generate_security_test_cases(
            test_suites[TestType.CONTRACT.value]
        )
        logger.info(f"Generated {len(test_suites[TestType.SECURITY.value])} security tests")

        # Store test suites
        self.test_suites = test_suites

        return test_suites

    async def execute_test_case(self, test_case: TestCase) -> TestResult:
        """Execute individual test case."""
        start_time = time.time()

        try:
            # Setup
            if test_case.setup_func:
                await test_case.setup_func()

            # Execute request
            async with httpx.AsyncClient(timeout=test_case.timeout) as client:
                response = await client.request(
                    method=test_case.method,
                    url=f"{self.base_url}{test_case.endpoint}",
                    headers=test_case.headers,
                    params=test_case.query_params,
                    json=test_case.body,
                )

                execution_time = (time.time() - start_time) * 1000  # ms

                # Validate response
                assertions_passed = 0
                assertions_failed = 0
                error_message = None

                # Status code assertion
                if response.status_code == test_case.expected_status:
                    assertions_passed += 1
                else:
                    assertions_failed += 1
                    error_message = (
                        f"Expected status {test_case.expected_status}, got {response.status_code}"
                    )

                # Response time assertion
                if test_case.expected_response_time_ms:
                    if execution_time <= test_case.expected_response_time_ms:
                        assertions_passed += 1
                    else:
                        assertions_failed += 1
                        if error_message:
                            error_message += f"; Response time {execution_time:.2f}ms exceeded limit {test_case.expected_response_time_ms}ms"
                        else:
                            error_message = f"Response time {execution_time:.2f}ms exceeded limit {test_case.expected_response_time_ms}ms"

                # Schema validation (if provided)
                if test_case.expected_schema:
                    try:
                        response.json()
                        # Simplified schema validation - in practice, use jsonschema library
                        assertions_passed += 1
                    except Exception:
                        assertions_failed += 1
                        if error_message:
                            error_message += "; Invalid JSON response"
                        else:
                            error_message = "Invalid JSON response"

                # Determine test status
                if assertions_failed == 0:
                    status = TestStatus.PASSED
                else:
                    status = TestStatus.FAILED

                result = TestResult(
                    test_case=test_case,
                    status=status,
                    execution_time_ms=execution_time,
                    response_status=response.status_code,
                    response_body=response.json()
                    if response.headers.get("content-type", "").startswith("application/json")
                    else None,
                    response_headers=dict(response.headers),
                    error_message=error_message,
                    assertions_passed=assertions_passed,
                    assertions_failed=assertions_failed,
                    performance_metrics={
                        "response_time_ms": execution_time,
                        "response_size_bytes": len(response.content),
                    },
                )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            result = TestResult(
                test_case=test_case,
                status=TestStatus.FAILED,
                execution_time_ms=execution_time,
                error_message=str(e),
                assertions_failed=1,
            )

        finally:
            # Teardown
            if test_case.teardown_func:
                try:
                    await test_case.teardown_func()
                except Exception as e:
                    logger.error(f"Teardown failed for test {test_case.test_id}: {e}")

        return result

    async def execute_test_suite(
        self, test_type: TestType, parallel_execution: bool = True
    ) -> list[TestResult]:
        """Execute all tests of a specific type."""
        test_cases = self.test_suites.get(test_type.value, [])

        if not test_cases:
            logger.warning(f"No test cases found for type: {test_type.value}")
            return []

        logger.info(f"Executing {len(test_cases)} {test_type.value} tests")

        if parallel_execution and test_type != TestType.LOAD:
            # Execute tests in parallel (except load tests)
            semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

            async def execute_with_semaphore(test_case: TestCase) -> TestResult:
                async with semaphore:
                    return await self.execute_test_case(test_case)

            tasks = [execute_with_semaphore(tc) for tc in test_cases]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Filter out exceptions and convert to TestResult
            test_results = []
            for i, result in enumerate(results):
                if isinstance(result, TestResult):
                    test_results.append(result)
                else:
                    # Create failed result for exception
                    failed_result = TestResult(
                        test_case=test_cases[i],
                        status=TestStatus.FAILED,
                        execution_time_ms=0,
                        error_message=str(result),
                        assertions_failed=1,
                    )
                    test_results.append(failed_result)
        else:
            # Execute tests sequentially
            test_results = []
            for test_case in test_cases:
                result = await self.execute_test_case(test_case)
                test_results.append(result)

        # Store results
        self.test_results.extend(test_results)

        # Log summary
        passed = sum(1 for r in test_results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in test_results if r.status == TestStatus.FAILED)
        logger.info(f"{test_type.value} tests completed: {passed} passed, {failed} failed")

        return test_results

    async def execute_load_test(self, config: LoadTestConfig) -> LoadTestResult:
        """Execute load test with specified configuration."""
        load_test_cases = self.test_suites.get(TestType.LOAD.value, [])

        if not load_test_cases:
            logger.error("No load test cases available")
            raise ValueError("No load test cases available")

        return await self.load_tester.run_load_test(load_test_cases, config)

    def generate_test_report(self) -> dict[str, Any]:
        """Generate comprehensive test report."""
        if not self.test_results:
            return {"error": "No test results available"}

        # Overall statistics
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r.status == TestStatus.PASSED)
        failed_tests = sum(1 for r in self.test_results if r.status == TestStatus.FAILED)
        skipped_tests = sum(1 for r in self.test_results if r.status == TestStatus.SKIPPED)

        # Performance statistics
        execution_times = [
            r.execution_time_ms for r in self.test_results if r.execution_time_ms > 0
        ]
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0

        # Test type breakdown
        by_type = {}
        for test_type in TestType:
            type_results = [r for r in self.test_results if r.test_case.test_type == test_type]
            if type_results:
                by_type[test_type.value] = {
                    "total": len(type_results),
                    "passed": sum(1 for r in type_results if r.status == TestStatus.PASSED),
                    "failed": sum(1 for r in type_results if r.status == TestStatus.FAILED),
                    "avg_execution_time_ms": sum(r.execution_time_ms for r in type_results)
                    / len(type_results),
                }

        # Failed tests details
        failed_test_details = []
        for result in self.test_results:
            if result.status == TestStatus.FAILED:
                failed_test_details.append(
                    {
                        "test_id": result.test_case.test_id,
                        "test_name": result.test_case.name,
                        "endpoint": result.test_case.endpoint,
                        "method": result.test_case.method,
                        "error_message": result.error_message,
                        "execution_time_ms": result.execution_time_ms,
                        "response_status": result.response_status,
                    }
                )

        return {
            "summary": {
                "total_tests": total_tests,
                "passed_tests": passed_tests,
                "failed_tests": failed_tests,
                "skipped_tests": skipped_tests,
                "pass_rate": (passed_tests / total_tests * 100) if total_tests > 0 else 0,
                "avg_execution_time_ms": avg_execution_time,
            },
            "by_test_type": by_type,
            "failed_tests": failed_test_details,
            "report_generated_at": datetime.utcnow().isoformat(),
        }

    async def run_comprehensive_test_suite(
        self, include_load_test: bool = False, load_test_config: LoadTestConfig | None = None
    ) -> dict[str, Any]:
        """Run complete test suite with all test types."""
        logger.info("Starting comprehensive API test suite")

        # Initialize if not already done
        if not self.test_suites:
            await self.initialize()
            await self.generate_comprehensive_test_suite()

        results = {}

        # Execute contract tests
        logger.info("Executing contract tests...")
        contract_results = await self.execute_test_suite(TestType.CONTRACT)
        results["contract_tests"] = contract_results

        # Execute security tests
        logger.info("Executing security tests...")
        security_results = await self.execute_test_suite(TestType.SECURITY)
        results["security_tests"] = security_results

        # Execute load tests if requested
        if include_load_test:
            logger.info("Executing load tests...")
            load_config = load_test_config or LoadTestConfig()
            load_result = await self.execute_load_test(load_config)
            results["load_test"] = load_result

        # Generate comprehensive report
        test_report = self.generate_test_report()
        results["test_report"] = test_report

        logger.info("Comprehensive test suite completed")
        return results


# Factory function for easy setup
def create_comprehensive_api_tester(
    base_url: str = "http://localhost:8000",
) -> ComprehensiveAPITester:
    """Create and configure comprehensive API tester."""
    tester = ComprehensiveAPITester(base_url)
    logger.info(f"Comprehensive API tester created for {base_url}")
    return tester
