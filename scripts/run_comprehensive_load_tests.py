"""
Comprehensive Load Testing Suite for FastAPI Endpoints
Advanced performance testing with concurrent users, realistic scenarios,
and comprehensive performance analysis for enterprise-scale APIs.
"""

import asyncio
import aiohttp
import json
import time
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum
import argparse
import sys
import random
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestScenario(Enum):
    """Load test scenarios"""
    SMOKE = "smoke"           # Basic functionality test
    LOAD = "load"             # Normal expected load
    STRESS = "stress"         # High load testing
    SPIKE = "spike"           # Sudden load spikes
    VOLUME = "volume"         # Large data volumes
    ENDURANCE = "endurance"   # Long-running stability

@dataclass
class TestConfiguration:
    """Load test configuration"""
    base_url: str = "http://localhost:8000"
    scenario: TestScenario = TestScenario.LOAD

    # User simulation
    concurrent_users: int = 100
    test_duration_seconds: int = 300  # 5 minutes
    ramp_up_seconds: int = 60
    ramp_down_seconds: int = 30

    # Request patterns
    requests_per_user_per_second: float = 1.0
    think_time_seconds: float = 1.0

    # Performance thresholds
    max_response_time_ms: float = 1000.0
    max_error_rate_percent: float = 1.0
    min_throughput_rps: float = 50.0

    # Test data
    use_real_data: bool = True
    data_variation_percent: float = 20.0

@dataclass
class RequestResult:
    """Result of a single HTTP request"""
    url: str
    method: str
    status_code: int
    response_time_ms: float
    response_size_bytes: int
    success: bool
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass
class UserSession:
    """User session for realistic testing"""
    user_id: str
    session_token: Optional[str] = None
    customer_id: Optional[int] = None
    product_preferences: List[str] = field(default_factory=list)
    request_count: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)

@dataclass
class LoadTestResults:
    """Comprehensive load test results"""
    scenario: TestScenario
    config: TestConfiguration

    # Overall metrics
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_bytes_transferred: int = 0
    test_duration_seconds: float = 0.0

    # Performance metrics
    avg_response_time_ms: float = 0.0
    p95_response_time_ms: float = 0.0
    p99_response_time_ms: float = 0.0
    min_response_time_ms: float = 0.0
    max_response_time_ms: float = 0.0

    # Throughput metrics
    requests_per_second: float = 0.0
    bytes_per_second: float = 0.0

    # Error analysis
    error_rate_percent: float = 0.0
    errors_by_type: Dict[str, int] = field(default_factory=dict)
    errors_by_endpoint: Dict[str, int] = field(default_factory=dict)

    # Endpoint-specific results
    endpoint_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Time-series data
    timeline_data: List[Dict[str, Any]] = field(default_factory=list)

class RealisticDataGenerator:
    """Generate realistic test data for API requests"""

    def __init__(self):
        self.customer_names = [
            "John Smith", "Jane Doe", "Bob Johnson", "Alice Wilson", "Charlie Brown",
            "Diana Prince", "Edward Norton", "Fiona Green", "George Lucas", "Helen Troy"
        ]

        self.product_categories = [
            "Electronics", "Clothing", "Books", "Home & Garden", "Sports",
            "Automotive", "Beauty", "Health", "Toys", "Food & Beverage"
        ]

        self.countries = [
            "United States", "Canada", "United Kingdom", "Germany", "France",
            "Australia", "Japan", "Brazil", "India", "China"
        ]

    def generate_customer_data(self) -> Dict[str, Any]:
        """Generate realistic customer data"""
        return {
            "name": random.choice(self.customer_names),
            "email": f"user{random.randint(1000, 9999)}@example.com",
            "country": random.choice(self.countries),
            "age": random.randint(18, 80),
            "registration_date": (datetime.utcnow() - timedelta(days=random.randint(1, 365))).isoformat()
        }

    def generate_product_data(self) -> Dict[str, Any]:
        """Generate realistic product data"""
        return {
            "name": f"Product {random.randint(1000, 9999)}",
            "category": random.choice(self.product_categories),
            "price": round(random.uniform(10.0, 500.0), 2),
            "description": f"High-quality {random.choice(self.product_categories).lower()} product",
            "in_stock": random.choice([True, False])
        }

    def generate_sales_data(self, customer_id: int, product_id: int) -> Dict[str, Any]:
        """Generate realistic sales data"""
        return {
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(10.0, 500.0), 2),
            "sale_date": (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat(),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "cash"])
        }

class APIEndpointTester:
    """Test specific API endpoints with realistic scenarios"""

    def __init__(self, session: aiohttp.ClientSession, data_generator: RealisticDataGenerator):
        self.session = session
        self.data_generator = data_generator

        # Define endpoint test scenarios
        self.endpoint_scenarios = {
            "/api/v1/health": self._test_health_endpoint,
            "/api/v1/customers": self._test_customers_endpoint,
            "/api/v1/products": self._test_products_endpoint,
            "/api/v1/sales": self._test_sales_endpoint,
            "/api/v1/analytics/sales": self._test_analytics_endpoint,
            "/api/v1/analytics/customers": self._test_customer_analytics_endpoint,
            "/graphql": self._test_graphql_endpoint
        }

    async def test_endpoint(self, endpoint: str, user_session: UserSession) -> RequestResult:
        """Test a specific endpoint with realistic data"""

        if endpoint in self.endpoint_scenarios:
            return await self.endpoint_scenarios[endpoint](user_session)
        else:
            # Generic GET request for unknown endpoints
            return await self._generic_get_test(endpoint, user_session)

    async def _test_health_endpoint(self, user_session: UserSession) -> RequestResult:
        """Test health check endpoint"""
        start_time = time.time()

        try:
            async with self.session.get("/api/v1/health") as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url="/api/v1/health",
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url="/api/v1/health",
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_customers_endpoint(self, user_session: UserSession) -> RequestResult:
        """Test customers endpoint with realistic operations"""

        operations = ["list", "create", "get", "update"]
        operation = random.choice(operations)

        if operation == "list":
            return await self._test_get_customers(user_session)
        elif operation == "create":
            return await self._test_create_customer(user_session)
        elif operation == "get":
            return await self._test_get_customer(user_session)
        else:  # update
            return await self._test_update_customer(user_session)

    async def _test_get_customers(self, user_session: UserSession) -> RequestResult:
        """Test GET /customers with pagination"""
        start_time = time.time()

        # Random pagination parameters
        page = random.randint(1, 10)
        limit = random.choice([10, 25, 50, 100])

        try:
            url = f"/api/v1/customers?page={page}&limit={limit}"
            async with self.session.get(url) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=url,
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=url,
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_create_customer(self, user_session: UserSession) -> RequestResult:
        """Test POST /customers"""
        start_time = time.time()

        customer_data = self.data_generator.generate_customer_data()

        try:
            async with self.session.post("/api/v1/customers", json=customer_data) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url="/api/v1/customers",
                    method="POST",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status in [200, 201]
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url="/api/v1/customers",
                method="POST",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_get_customer(self, user_session: UserSession) -> RequestResult:
        """Test GET /customers/{id}"""
        start_time = time.time()

        # Use session customer ID or random ID
        customer_id = user_session.customer_id or random.randint(1, 1000)

        try:
            url = f"/api/v1/customers/{customer_id}"
            async with self.session.get(url) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=url,
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status in [200, 404]  # 404 is acceptable for non-existent customers
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=url,
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_update_customer(self, user_session: UserSession) -> RequestResult:
        """Test PUT /customers/{id}"""
        start_time = time.time()

        customer_id = user_session.customer_id or random.randint(1, 1000)
        update_data = {"name": f"Updated User {random.randint(1000, 9999)}"}

        try:
            url = f"/api/v1/customers/{customer_id}"
            async with self.session.put(url, json=update_data) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=url,
                    method="PUT",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status in [200, 404]
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=url,
                method="PUT",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_products_endpoint(self, user_session: UserSession) -> RequestResult:
        """Test products endpoint operations"""
        operations = ["list", "get", "search"]
        operation = random.choice(operations)

        if operation == "list":
            return await self._test_get_products(user_session)
        elif operation == "get":
            return await self._test_get_product(user_session)
        else:  # search
            return await self._test_search_products(user_session)

    async def _test_get_products(self, user_session: UserSession) -> RequestResult:
        """Test GET /products"""
        start_time = time.time()

        try:
            async with self.session.get("/api/v1/products") as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url="/api/v1/products",
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url="/api/v1/products",
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_get_product(self, user_session: UserSession) -> RequestResult:
        """Test GET /products/{id}"""
        start_time = time.time()

        product_id = random.randint(1, 1000)

        try:
            url = f"/api/v1/products/{product_id}"
            async with self.session.get(url) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=url,
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status in [200, 404]
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=url,
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_search_products(self, user_session: UserSession) -> RequestResult:
        """Test product search"""
        start_time = time.time()

        search_terms = ["electronics", "clothing", "books", "sports", "home"]
        search_term = random.choice(search_terms)

        try:
            url = f"/api/v1/products/search?q={search_term}"
            async with self.session.get(url) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=url,
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=url,
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_sales_endpoint(self, user_session: UserSession) -> RequestResult:
        """Test sales endpoint operations"""
        operations = ["list", "create", "get"]
        operation = random.choice(operations)

        if operation == "list":
            return await self._test_get_sales(user_session)
        elif operation == "create":
            return await self._test_create_sale(user_session)
        else:  # get
            return await self._test_get_sale(user_session)

    async def _test_get_sales(self, user_session: UserSession) -> RequestResult:
        """Test GET /sales"""
        start_time = time.time()

        try:
            async with self.session.get("/api/v1/sales") as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url="/api/v1/sales",
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url="/api/v1/sales",
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_create_sale(self, user_session: UserSession) -> RequestResult:
        """Test POST /sales"""
        start_time = time.time()

        sale_data = self.data_generator.generate_sales_data(
            random.randint(1, 1000), random.randint(1, 1000)
        )

        try:
            async with self.session.post("/api/v1/sales", json=sale_data) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url="/api/v1/sales",
                    method="POST",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status in [200, 201]
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url="/api/v1/sales",
                method="POST",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_get_sale(self, user_session: UserSession) -> RequestResult:
        """Test GET /sales/{id}"""
        start_time = time.time()

        sale_id = random.randint(1, 1000)

        try:
            url = f"/api/v1/sales/{sale_id}"
            async with self.session.get(url) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=url,
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status in [200, 404]
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=url,
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_analytics_endpoint(self, user_session: UserSession) -> RequestResult:
        """Test analytics endpoint"""
        start_time = time.time()

        # Random date range for analytics
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=random.randint(7, 90))

        try:
            url = f"/api/v1/analytics/sales?start_date={start_date.isoformat()}&end_date={end_date.isoformat()}"
            async with self.session.get(url) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=url,
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=url,
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_customer_analytics_endpoint(self, user_session: UserSession) -> RequestResult:
        """Test customer analytics endpoint"""
        start_time = time.time()

        try:
            async with self.session.get("/api/v1/analytics/customers") as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url="/api/v1/analytics/customers",
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url="/api/v1/analytics/customers",
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _test_graphql_endpoint(self, user_session: UserSession) -> RequestResult:
        """Test GraphQL endpoint with realistic queries"""
        start_time = time.time()

        # Sample GraphQL queries
        queries = [
            {
                "query": "query { customers(limit: 10) { id name email } }"
            },
            {
                "query": "query { products(limit: 10) { id name price category } }"
            },
            {
                "query": "query GetCustomerWithSales($customerId: ID!) { customer(id: $customerId) { id name sales { id total_amount date } } }",
                "variables": {"customerId": str(random.randint(1, 1000))}
            }
        ]

        query_data = random.choice(queries)

        try:
            async with self.session.post("/graphql", json=query_data) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url="/graphql",
                    method="POST",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status == 200
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url="/graphql",
                method="POST",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

    async def _generic_get_test(self, endpoint: str, user_session: UserSession) -> RequestResult:
        """Generic GET test for unknown endpoints"""
        start_time = time.time()

        try:
            async with self.session.get(endpoint) as response:
                response_time = (time.time() - start_time) * 1000
                content = await response.read()

                return RequestResult(
                    url=endpoint,
                    method="GET",
                    status_code=response.status,
                    response_time_ms=response_time,
                    response_size_bytes=len(content),
                    success=response.status < 500  # Accept client errors but not server errors
                )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return RequestResult(
                url=endpoint,
                method="GET",
                status_code=0,
                response_time_ms=response_time,
                response_size_bytes=0,
                success=False,
                error_message=str(e)
            )

class VirtualUser:
    """Simulates a virtual user for load testing"""

    def __init__(self, user_id: str, config: TestConfiguration, session: aiohttp.ClientSession):
        self.user_id = user_id
        self.config = config
        self.session = session
        self.user_session = UserSession(user_id=user_id)
        self.data_generator = RealisticDataGenerator()
        self.endpoint_tester = APIEndpointTester(session, self.data_generator)

        # User behavior patterns
        self.endpoints = list(self.endpoint_tester.endpoint_scenarios.keys())
        self.endpoint_weights = self._get_endpoint_weights()

        # Results tracking
        self.results: List[RequestResult] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    def _get_endpoint_weights(self) -> Dict[str, float]:
        """Get realistic endpoint usage weights"""
        return {
            "/api/v1/health": 0.05,           # 5% - health checks
            "/api/v1/customers": 0.20,        # 20% - customer operations
            "/api/v1/products": 0.25,         # 25% - product browsing
            "/api/v1/sales": 0.15,            # 15% - sales operations
            "/api/v1/analytics/sales": 0.10,  # 10% - sales analytics
            "/api/v1/analytics/customers": 0.10,  # 10% - customer analytics
            "/graphql": 0.15                  # 15% - GraphQL queries
        }

    async def run_user_scenario(self) -> List[RequestResult]:
        """Run the complete user scenario"""
        self.start_time = datetime.utcnow()

        try:
            if self.config.scenario == TestScenario.SMOKE:
                await self._run_smoke_scenario()
            elif self.config.scenario == TestScenario.LOAD:
                await self._run_load_scenario()
            elif self.config.scenario == TestScenario.STRESS:
                await self._run_stress_scenario()
            elif self.config.scenario == TestScenario.SPIKE:
                await self._run_spike_scenario()
            elif self.config.scenario == TestScenario.VOLUME:
                await self._run_volume_scenario()
            elif self.config.scenario == TestScenario.ENDURANCE:
                await self._run_endurance_scenario()

        except Exception as e:
            logger.error(f"User {self.user_id} scenario failed: {e}")
        finally:
            self.end_time = datetime.utcnow()

        return self.results

    async def _run_smoke_scenario(self):
        """Basic smoke test - one request to each endpoint"""
        for endpoint in self.endpoints:
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)
            await asyncio.sleep(1)  # Short delay between requests

    async def _run_load_scenario(self):
        """Normal load scenario with realistic usage patterns"""
        end_time = time.time() + self.config.test_duration_seconds

        while time.time() < end_time:
            # Select endpoint based on weights
            endpoint = self._select_weighted_endpoint()

            # Execute request
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)

            # Think time between requests
            await asyncio.sleep(self.config.think_time_seconds)

    async def _run_stress_scenario(self):
        """High stress scenario with increased request rate"""
        end_time = time.time() + self.config.test_duration_seconds
        reduced_think_time = self.config.think_time_seconds * 0.3  # 30% of normal think time

        while time.time() < end_time:
            endpoint = self._select_weighted_endpoint()
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)
            await asyncio.sleep(reduced_think_time)

    async def _run_spike_scenario(self):
        """Spike scenario with sudden load increases"""
        # Normal load for first part
        normal_duration = self.config.test_duration_seconds * 0.6
        spike_duration = self.config.test_duration_seconds * 0.3
        cooldown_duration = self.config.test_duration_seconds * 0.1

        # Normal phase
        end_normal = time.time() + normal_duration
        while time.time() < end_normal:
            endpoint = self._select_weighted_endpoint()
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)
            await asyncio.sleep(self.config.think_time_seconds)

        # Spike phase
        end_spike = time.time() + spike_duration
        while time.time() < end_spike:
            endpoint = self._select_weighted_endpoint()
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)
            await asyncio.sleep(0.1)  # Very short think time during spike

        # Cooldown phase
        end_cooldown = time.time() + cooldown_duration
        while time.time() < end_cooldown:
            endpoint = self._select_weighted_endpoint()
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)
            await asyncio.sleep(self.config.think_time_seconds * 2)  # Longer think time

    async def _run_volume_scenario(self):
        """Volume scenario focusing on data-heavy endpoints"""
        data_heavy_endpoints = [
            "/api/v1/analytics/sales",
            "/api/v1/analytics/customers",
            "/api/v1/customers",
            "/api/v1/products"
        ]

        end_time = time.time() + self.config.test_duration_seconds

        while time.time() < end_time:
            endpoint = random.choice(data_heavy_endpoints)
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)
            await asyncio.sleep(self.config.think_time_seconds)

    async def _run_endurance_scenario(self):
        """Long-running endurance test"""
        # Run for the full duration with consistent load
        end_time = time.time() + self.config.test_duration_seconds

        while time.time() < end_time:
            endpoint = self._select_weighted_endpoint()
            result = await self.endpoint_tester.test_endpoint(endpoint, self.user_session)
            self.results.append(result)

            # Vary think time slightly to simulate real user behavior
            think_time = self.config.think_time_seconds * random.uniform(0.5, 1.5)
            await asyncio.sleep(think_time)

    def _select_weighted_endpoint(self) -> str:
        """Select endpoint based on usage weights"""
        weights = [self.endpoint_weights.get(ep, 0.1) for ep in self.endpoints]
        return random.choices(self.endpoints, weights=weights)[0]

class ComprehensiveLoadTester:
    """Main load testing orchestrator"""

    def __init__(self, config: TestConfiguration):
        self.config = config
        self.results: List[RequestResult] = []
        self.users: List[VirtualUser] = []

    async def run_load_test(self) -> LoadTestResults:
        """Execute comprehensive load test"""
        logger.info(f"Starting {self.config.scenario.value} load test with {self.config.concurrent_users} users")

        start_time = time.time()

        try:
            # Create HTTP session with connection pooling
            connector = aiohttp.TCPConnector(
                limit=self.config.concurrent_users + 50,
                limit_per_host=self.config.concurrent_users + 50,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )

            timeout = aiohttp.ClientTimeout(total=self.config.max_response_time_ms / 1000 * 2)

            async with aiohttp.ClientSession(
                base_url=self.config.base_url,
                connector=connector,
                timeout=timeout
            ) as session:

                # Create virtual users
                self.users = [
                    VirtualUser(f"user_{i:04d}", self.config, session)
                    for i in range(self.config.concurrent_users)
                ]

                # Execute test with ramp-up
                await self._execute_with_ramp_up()

        except Exception as e:
            logger.error(f"Load test failed: {e}")
            raise

        test_duration = time.time() - start_time

        # Analyze results
        return self._analyze_results(test_duration)

    async def _execute_with_ramp_up(self):
        """Execute load test with gradual ramp-up"""
        # Ramp-up phase
        ramp_up_delay = self.config.ramp_up_seconds / self.config.concurrent_users

        user_tasks = []

        for i, user in enumerate(self.users):
            # Stagger user start times
            delay = i * ramp_up_delay

            async def delayed_user_start(user_instance, start_delay):
                await asyncio.sleep(start_delay)
                return await user_instance.run_user_scenario()

            task = asyncio.create_task(delayed_user_start(user, delay))
            user_tasks.append(task)

        # Wait for all users to complete
        logger.info("All users started, waiting for completion...")
        user_results = await asyncio.gather(*user_tasks, return_exceptions=True)

        # Collect all results
        for result_list in user_results:
            if isinstance(result_list, list):
                self.results.extend(result_list)
            else:
                logger.error(f"User failed with exception: {result_list}")

    def _analyze_results(self, test_duration: float) -> LoadTestResults:
        """Analyze load test results"""
        if not self.results:
            logger.warning("No results to analyze")
            return LoadTestResults(
                scenario=self.config.scenario,
                config=self.config,
                test_duration_seconds=test_duration
            )

        # Basic metrics
        total_requests = len(self.results)
        successful_requests = len([r for r in self.results if r.success])
        failed_requests = total_requests - successful_requests

        # Response time analysis
        response_times = [r.response_time_ms for r in self.results if r.success]

        if response_times:
            avg_response_time = statistics.mean(response_times)
            p95_response_time = statistics.quantiles(response_times, n=20)[18] if len(response_times) > 20 else max(response_times)
            p99_response_time = statistics.quantiles(response_times, n=100)[98] if len(response_times) > 100 else max(response_times)
            min_response_time = min(response_times)
            max_response_time = max(response_times)
        else:
            avg_response_time = p95_response_time = p99_response_time = min_response_time = max_response_time = 0

        # Throughput metrics
        requests_per_second = total_requests / test_duration if test_duration > 0 else 0
        total_bytes = sum(r.response_size_bytes for r in self.results)
        bytes_per_second = total_bytes / test_duration if test_duration > 0 else 0

        # Error analysis
        error_rate = (failed_requests / total_requests * 100) if total_requests > 0 else 0

        errors_by_type = {}
        errors_by_endpoint = {}

        for result in self.results:
            if not result.success:
                error_type = result.error_message or f"HTTP_{result.status_code}"
                errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1
                errors_by_endpoint[result.url] = errors_by_endpoint.get(result.url, 0) + 1

        # Endpoint-specific analysis
        endpoint_results = {}
        endpoint_groups = {}

        for result in self.results:
            endpoint = result.url
            if endpoint not in endpoint_groups:
                endpoint_groups[endpoint] = []
            endpoint_groups[endpoint].append(result)

        for endpoint, results_list in endpoint_groups.items():
            successful = [r for r in results_list if r.success]
            failed = [r for r in results_list if not r.success]

            if successful:
                endpoint_response_times = [r.response_time_ms for r in successful]
                endpoint_results[endpoint] = {
                    'total_requests': len(results_list),
                    'successful_requests': len(successful),
                    'failed_requests': len(failed),
                    'avg_response_time_ms': statistics.mean(endpoint_response_times),
                    'p95_response_time_ms': statistics.quantiles(endpoint_response_times, n=20)[18] if len(endpoint_response_times) > 20 else max(endpoint_response_times),
                    'error_rate_percent': (len(failed) / len(results_list) * 100),
                    'throughput_rps': len(results_list) / test_duration if test_duration > 0 else 0
                }

        # Create timeline data for visualization
        timeline_data = self._create_timeline_data()

        return LoadTestResults(
            scenario=self.config.scenario,
            config=self.config,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            total_bytes_transferred=total_bytes,
            test_duration_seconds=test_duration,
            avg_response_time_ms=avg_response_time,
            p95_response_time_ms=p95_response_time,
            p99_response_time_ms=p99_response_time,
            min_response_time_ms=min_response_time,
            max_response_time_ms=max_response_time,
            requests_per_second=requests_per_second,
            bytes_per_second=bytes_per_second,
            error_rate_percent=error_rate,
            errors_by_type=errors_by_type,
            errors_by_endpoint=errors_by_endpoint,
            endpoint_results=endpoint_results,
            timeline_data=timeline_data
        )

    def _create_timeline_data(self) -> List[Dict[str, Any]]:
        """Create timeline data for performance visualization"""
        if not self.results:
            return []

        # Group results by 10-second intervals
        interval_seconds = 10
        timeline_data = []

        # Find test start time
        start_time = min(r.timestamp for r in self.results)

        # Group results by time intervals
        intervals = {}
        for result in self.results:
            time_offset = (result.timestamp - start_time).total_seconds()
            interval = int(time_offset // interval_seconds)

            if interval not in intervals:
                intervals[interval] = []
            intervals[interval].append(result)

        # Calculate metrics for each interval
        for interval, results_list in sorted(intervals.items()):
            successful = [r for r in results_list if r.success]
            failed = [r for r in results_list if not r.success]

            timeline_data.append({
                'timestamp': interval * interval_seconds,
                'total_requests': len(results_list),
                'successful_requests': len(successful),
                'failed_requests': len(failed),
                'avg_response_time_ms': statistics.mean([r.response_time_ms for r in successful]) if successful else 0,
                'requests_per_second': len(results_list) / interval_seconds,
                'error_rate_percent': (len(failed) / len(results_list) * 100) if results_list else 0
            })

        return timeline_data

class LoadTestReporter:
    """Generate comprehensive load test reports"""

    @staticmethod
    def print_summary_report(results: LoadTestResults):
        """Print a summary report to console"""
        print("\n" + "="*80)
        print(f"LOAD TEST RESULTS - {results.scenario.value.upper()} SCENARIO")
        print("="*80)

        print(f"\nTest Configuration:")
        print(f"  Scenario: {results.scenario.value}")
        print(f"  Concurrent Users: {results.config.concurrent_users}")
        print(f"  Test Duration: {results.test_duration_seconds:.1f} seconds")
        print(f"  Base URL: {results.config.base_url}")

        print(f"\nOverall Performance:")
        print(f"  Total Requests: {results.total_requests:,}")
        print(f"  Successful Requests: {results.successful_requests:,}")
        print(f"  Failed Requests: {results.failed_requests:,}")
        print(f"  Success Rate: {(results.successful_requests/results.total_requests*100):.1f}%")
        print(f"  Error Rate: {results.error_rate_percent:.1f}%")

        print(f"\nThroughput:")
        print(f"  Requests/Second: {results.requests_per_second:.1f}")
        print(f"  MB/Second: {results.bytes_per_second/1024/1024:.2f}")

        print(f"\nResponse Times:")
        print(f"  Average: {results.avg_response_time_ms:.1f} ms")
        print(f"  95th Percentile: {results.p95_response_time_ms:.1f} ms")
        print(f"  99th Percentile: {results.p99_response_time_ms:.1f} ms")
        print(f"  Min: {results.min_response_time_ms:.1f} ms")
        print(f"  Max: {results.max_response_time_ms:.1f} ms")

        # SLA Compliance
        print(f"\nSLA Compliance:")
        sla_compliant = results.avg_response_time_ms <= results.config.max_response_time_ms
        error_compliant = results.error_rate_percent <= results.config.max_error_rate_percent
        throughput_compliant = results.requests_per_second >= results.config.min_throughput_rps

        print(f"  Response Time SLA (<{results.config.max_response_time_ms}ms): {'✓ PASS' if sla_compliant else '✗ FAIL'}")
        print(f"  Error Rate SLA (<{results.config.max_error_rate_percent}%): {'✓ PASS' if error_compliant else '✗ FAIL'}")
        print(f"  Throughput SLA (>{results.config.min_throughput_rps} RPS): {'✓ PASS' if throughput_compliant else '✗ FAIL'}")

        overall_pass = sla_compliant and error_compliant and throughput_compliant
        print(f"  Overall SLA Compliance: {'✓ PASS' if overall_pass else '✗ FAIL'}")

        # Top errors
        if results.errors_by_type:
            print(f"\nTop Errors:")
            for error, count in sorted(results.errors_by_type.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {error}: {count} occurrences")

        # Endpoint performance
        print(f"\nEndpoint Performance (Top 5 by request count):")
        sorted_endpoints = sorted(
            results.endpoint_results.items(),
            key=lambda x: x[1]['total_requests'],
            reverse=True
        )[:5]

        for endpoint, metrics in sorted_endpoints:
            print(f"  {endpoint}:")
            print(f"    Requests: {metrics['total_requests']:,}")
            print(f"    Avg Response: {metrics['avg_response_time_ms']:.1f}ms")
            print(f"    Error Rate: {metrics['error_rate_percent']:.1f}%")
            print(f"    Throughput: {metrics['throughput_rps']:.1f} RPS")

        print("\n" + "="*80)

    @staticmethod
    def save_detailed_report(results: LoadTestResults, filename: str = None):
        """Save detailed results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"load_test_results_{results.scenario.value}_{timestamp}.json"

        # Convert results to JSON-serializable format
        report_data = {
            'test_info': {
                'scenario': results.scenario.value,
                'timestamp': datetime.now().isoformat(),
                'duration_seconds': results.test_duration_seconds,
                'concurrent_users': results.config.concurrent_users,
                'base_url': results.config.base_url
            },
            'performance_summary': {
                'total_requests': results.total_requests,
                'successful_requests': results.successful_requests,
                'failed_requests': results.failed_requests,
                'error_rate_percent': results.error_rate_percent,
                'requests_per_second': results.requests_per_second,
                'bytes_per_second': results.bytes_per_second,
                'avg_response_time_ms': results.avg_response_time_ms,
                'p95_response_time_ms': results.p95_response_time_ms,
                'p99_response_time_ms': results.p99_response_time_ms
            },
            'sla_compliance': {
                'response_time_sla_pass': results.avg_response_time_ms <= results.config.max_response_time_ms,
                'error_rate_sla_pass': results.error_rate_percent <= results.config.max_error_rate_percent,
                'throughput_sla_pass': results.requests_per_second >= results.config.min_throughput_rps
            },
            'errors': {
                'by_type': results.errors_by_type,
                'by_endpoint': results.errors_by_endpoint
            },
            'endpoint_results': results.endpoint_results,
            'timeline_data': results.timeline_data
        }

        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2)

        print(f"\nDetailed report saved to: {filename}")

async def main():
    """Main entry point for load testing"""
    parser = argparse.ArgumentParser(description='Comprehensive Load Testing Suite')
    parser.add_argument('--base-url', default='http://localhost:8000', help='Base URL for API')
    parser.add_argument('--scenario', choices=[s.value for s in TestScenario], default='load', help='Test scenario')
    parser.add_argument('--users', type=int, default=100, help='Number of concurrent users')
    parser.add_argument('--duration', type=int, default=300, help='Test duration in seconds')
    parser.add_argument('--ramp-up', type=int, default=60, help='Ramp-up time in seconds')
    parser.add_argument('--max-response-time', type=float, default=1000.0, help='Max acceptable response time (ms)')
    parser.add_argument('--max-error-rate', type=float, default=1.0, help='Max acceptable error rate (%)')
    parser.add_argument('--min-throughput', type=float, default=50.0, help='Min required throughput (RPS)')
    parser.add_argument('--output', help='Output file for detailed results')

    args = parser.parse_args()

    # Create test configuration
    config = TestConfiguration(
        base_url=args.base_url,
        scenario=TestScenario(args.scenario),
        concurrent_users=args.users,
        test_duration_seconds=args.duration,
        ramp_up_seconds=args.ramp_up,
        max_response_time_ms=args.max_response_time,
        max_error_rate_percent=args.max_error_rate,
        min_throughput_rps=args.min_throughput
    )

    # Run load test
    tester = ComprehensiveLoadTester(config)
    results = await tester.run_load_test()

    # Generate reports
    LoadTestReporter.print_summary_report(results)

    if args.output:
        LoadTestReporter.save_detailed_report(results, args.output)
    else:
        LoadTestReporter.save_detailed_report(results)

    # Exit with error code if SLA not met
    sla_pass = (
        results.avg_response_time_ms <= config.max_response_time_ms and
        results.error_rate_percent <= config.max_error_rate_percent and
        results.requests_per_second >= config.min_throughput_rps
    )

    sys.exit(0 if sla_pass else 1)

if __name__ == "__main__":
    asyncio.run(main())