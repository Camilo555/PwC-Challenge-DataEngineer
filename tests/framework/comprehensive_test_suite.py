"""
Comprehensive Test Suite for Enterprise Data Platform
Multi-level testing strategy with automated execution and reporting
"""
import asyncio
import json
import pytest
import time
from datetime import datetime
from typing import Dict, List, Any

from tests.framework.testing_framework import (
    APITestFramework, DatabaseTestFramework, PerformanceTestFramework,
    TestType, TestSeverity, TestResult, TestReporter, TestDataFactory
)
from core.config.base_config import BaseConfig


class ComprehensiveTestSuite:
    """Main test suite orchestrating all test types"""
    
    def __init__(self):
        self.config = BaseConfig()
        self.test_results: List[TestResult] = []
        self.reporter = TestReporter(output_format="json")
        
        # Initialize test frameworks
        self.api_framework = APITestFramework(
            base_url="http://localhost:8000",
            auth_token=None  # Will be set during setup
        )
        
        self.db_framework = DatabaseTestFramework(
            db_connection_string="postgresql://localhost/test_db"
        )
        
        self.perf_framework = PerformanceTestFramework()
    
    async def setup_test_environment(self):
        """Setup comprehensive test environment"""
        print("🔧 Setting up test environment...")
        
        # Setup all frameworks
        await self.api_framework.setup()
        await self.db_framework.setup()
        await self.perf_framework.setup()
        
        # Create test authentication token
        auth_response = await self.api_framework.post("/api/v1/auth/login", json_data={
            "username": "test_user",
            "password": "test_password"
        })
        
        if auth_response.status_code == 200:
            token = auth_response.json().get("access_token")
            self.api_framework.auth_token = token
            self.api_framework.client.headers["Authorization"] = f"Bearer {token}"
    
    async def teardown_test_environment(self):
        """Cleanup test environment"""
        print("🧹 Cleaning up test environment...")
        
        await self.api_framework.teardown()
        await self.db_framework.teardown()
        await self.perf_framework.teardown()
    
    async def run_unit_tests(self):
        """Execute unit tests"""
        print("🧪 Running Unit Tests...")
        
        async with self.api_framework.test_context("unit_config_validation", TestType.UNIT):
            # Test configuration validation
            config = BaseConfig()
            self.api_framework.assert_equals(config.environment.value, "testing", "Environment should be testing")
            
        async with self.api_framework.test_context("unit_data_factory", TestType.UNIT):
            # Test data factory
            user_data = TestDataFactory.create_test_user()
            self.api_framework.assert_equals(len(user_data["user_id"]), 36, "User ID should be UUID format")
            assert "@example.com" in user_data["email"], "Email should contain test domain"
            
        async with self.api_framework.test_context("unit_sale_validation", TestType.UNIT):
            # Test sale data validation
            sale_data = TestDataFactory.create_test_sale(quantity=2, unit_price=15.99)
            expected_total = sale_data["quantity"] * sale_data["unit_price"]
            self.api_framework.assert_equals(sale_data["total_amount"], expected_total, "Total should equal quantity * unit_price")
    
    async def run_integration_tests(self):
        """Execute integration tests"""
        print("🔗 Running Integration Tests...")
        
        async with self.api_framework.test_context("integration_auth_flow", TestType.INTEGRATION):
            # Test complete authentication flow
            login_response = await self.api_framework.post("/api/v1/auth/login", json_data={
                "username": "test_user",
                "password": "test_password"
            })
            
            self.api_framework.assert_status_code(login_response, 200, "Login should succeed")
            
            token_data = login_response.json()
            assert "access_token" in token_data, "Response should contain access token"
            assert "expires_in" in token_data, "Response should contain expiration"
        
        async with self.api_framework.test_context("integration_sales_api", TestType.INTEGRATION):
            # Test sales API integration
            sales_response = await self.api_framework.get("/api/v2/sales/transactions?limit=10")
            
            self.api_framework.assert_status_code(sales_response, 200, "Sales API should respond")
            
            sales_data = sales_response.json()
            assert "data" in sales_data, "Response should contain data array"
            assert "pagination" in sales_data, "Response should contain pagination info"
        
        async with self.api_framework.test_context("integration_monitoring_api", TestType.INTEGRATION):
            # Test monitoring integration
            health_response = await self.api_framework.get("/api/v1/monitoring/health")
            
            self.api_framework.assert_status_code(health_response, 200, "Health check should succeed")
            
            health_data = health_response.json()
            self.api_framework.assert_equals(health_data["status"], "healthy", "System should be healthy")
    
    async def run_api_contract_tests(self):
        """Execute API contract tests"""
        print("📋 Running API Contract Tests...")
        
        # Sales API contract
        async with self.api_framework.test_context("contract_sales_schema", TestType.CONTRACT):
            response = await self.api_framework.get("/api/v2/sales/transactions?limit=1")
            
            expected_schema = {
                "type": "object",
                "required": ["data", "pagination"],
                "properties": {
                    "data": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["id", "customer_id", "total_amount", "status"],
                            "properties": {
                                "id": {"type": "string"},
                                "customer_id": {"type": "string"},
                                "total_amount": {"type": "number"},
                                "status": {"type": "string", "enum": ["pending", "completed", "cancelled"]}
                            }
                        }
                    },
                    "pagination": {
                        "type": "object",
                        "required": ["page", "limit", "total_items"],
                        "properties": {
                            "page": {"type": "integer"},
                            "limit": {"type": "integer"},
                            "total_items": {"type": "integer"}
                        }
                    }
                }
            }
            
            self.api_framework.assert_json_schema(response, expected_schema, "Sales API should match contract")
        
        # Monitoring API contract
        async with self.api_framework.test_context("contract_monitoring_schema", TestType.CONTRACT):
            response = await self.api_framework.get("/api/v1/monitoring/health")
            
            expected_schema = {
                "type": "object",
                "required": ["status", "timestamp"],
                "properties": {
                    "status": {"type": "string", "enum": ["healthy", "unhealthy", "degraded"]},
                    "timestamp": {"type": "string"},
                    "metrics": {"type": "object"}
                }
            }
            
            self.api_framework.assert_json_schema(response, expected_schema, "Health API should match contract")
    
    async def run_database_tests(self):
        """Execute database tests"""
        print("🗄️ Running Database Tests...")
        
        async with self.db_framework.test_context("database_connection", TestType.INTEGRATION):
            # Test database connectivity
            result = await self.db_framework.execute_query("SELECT 1 as test_value")
            self.db_framework.assert_equals(result[0]["test_value"], 1, "Database should be accessible")
        
        async with self.db_framework.test_context("database_sales_table", TestType.INTEGRATION):
            # Test sales table structure
            table_info = await self.db_framework.execute_query("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'sales' 
                ORDER BY ordinal_position
            """)
            
            expected_columns = {"id", "customer_id", "product_id", "quantity", "unit_price", "total_amount"}
            actual_columns = {row["column_name"] for row in table_info}
            
            assert expected_columns.issubset(actual_columns), f"Sales table should have required columns: {expected_columns}"
        
        async with self.db_framework.test_context("database_performance", TestType.PERFORMANCE):
            # Test query performance
            start_time = time.time()
            
            sales_count = await self.db_framework.count_rows("sales", "created_at > NOW() - INTERVAL '30 days'")
            
            query_duration = time.time() - start_time
            self.db_framework.assert_query_performance(query_duration, 2.0, "Sales count query should be fast")
    
    async def run_performance_tests(self):
        """Execute performance tests"""
        print("⚡ Running Performance Tests...")
        
        # API response time tests
        async with self.perf_framework.test_context("perf_api_response_times", TestType.PERFORMANCE):
            
            # Test multiple API calls for performance analysis
            for i in range(20):
                result, duration = await self.perf_framework.measure_performance(
                    "sales_api_call",
                    self.api_framework.get,
                    "/api/v2/sales/transactions?limit=50"
                )
            
            # Assert performance metrics
            self.perf_framework.assert_average_performance("sales_api_call", 1.0, "Average API response should be under 1s")
            self.perf_framework.assert_performance_percentile("sales_api_call", 95, 2.0, "95th percentile should be under 2s")
        
        # Database query performance
        async with self.perf_framework.test_context("perf_database_queries", TestType.PERFORMANCE):
            
            for i in range(10):
                result, duration = await self.perf_framework.measure_performance(
                    "database_query",
                    self.db_framework.execute_query,
                    "SELECT COUNT(*) FROM sales WHERE status = 'completed'"
                )
            
            self.perf_framework.assert_average_performance("database_query", 0.5, "Database queries should be under 500ms")
    
    async def run_security_tests(self):
        """Execute security tests"""
        print("🔒 Running Security Tests...")
        
        async with self.api_framework.test_context("security_auth_required", TestType.SECURITY):
            # Test unauthorized access
            # Temporarily remove auth token
            old_token = self.api_framework.client.headers.get("Authorization")
            self.api_framework.client.headers.pop("Authorization", None)
            
            response = await self.api_framework.get("/api/v1/sales/transactions")
            self.api_framework.assert_status_code(response, 401, "Protected endpoint should require auth")
            
            # Restore auth token
            if old_token:
                self.api_framework.client.headers["Authorization"] = old_token
        
        async with self.api_framework.test_context("security_input_validation", TestType.SECURITY):
            # Test SQL injection protection
            malicious_payload = {
                "customer_id": "'; DROP TABLE sales; --",
                "quantity": -1,
                "unit_price": "not_a_number"
            }
            
            response = await self.api_framework.post("/api/v2/sales/transactions", json_data=malicious_payload)
            # Should return validation error, not 500
            assert response.status_code in [400, 422], "API should validate input and reject malicious data"
        
        async with self.api_framework.test_context("security_rate_limiting", TestType.SECURITY):
            # Test rate limiting (if enabled)
            responses = []
            
            # Make rapid requests
            for i in range(15):
                response = await self.api_framework.get("/api/v1/monitoring/health")
                responses.append(response.status_code)
            
            # Check if any requests were rate limited (429)
            rate_limited = any(code == 429 for code in responses)
            # This may or may not trigger depending on rate limit configuration
            print(f"Rate limiting test: {rate_limited} (429 responses detected)")
    
    async def run_data_quality_tests(self):
        """Execute data quality tests"""
        print("📊 Running Data Quality Tests...")
        
        async with self.db_framework.test_context("data_quality_completeness", TestType.INTEGRATION):
            # Check for null values in critical fields
            null_customer_ids = await self.db_framework.execute_query(
                "SELECT COUNT(*) as null_count FROM sales WHERE customer_id IS NULL"
            )
            
            self.db_framework.assert_equals(
                null_customer_ids[0]["null_count"], 0, 
                "Sales should not have null customer_id values"
            )
        
        async with self.db_framework.test_context("data_quality_consistency", TestType.INTEGRATION):
            # Check data consistency rules
            inconsistent_totals = await self.db_framework.execute_query("""
                SELECT COUNT(*) as inconsistent_count 
                FROM sales 
                WHERE ABS(total_amount - (quantity * unit_price)) > 0.01
            """)
            
            self.db_framework.assert_equals(
                inconsistent_totals[0]["inconsistent_count"], 0,
                "Sales total_amount should equal quantity * unit_price"
            )
        
        async with self.db_framework.test_context("data_quality_ranges", TestType.INTEGRATION):
            # Check for reasonable data ranges
            invalid_quantities = await self.db_framework.execute_query(
                "SELECT COUNT(*) as invalid_count FROM sales WHERE quantity <= 0 OR quantity > 1000"
            )
            
            # Allow some tolerance for edge cases
            invalid_count = invalid_quantities[0]["invalid_count"]
            total_sales = await self.db_framework.count_rows("sales")
            
            if total_sales > 0:
                invalid_percentage = (invalid_count / total_sales) * 100
                assert invalid_percentage < 5, f"Less than 5% of sales should have invalid quantities (found {invalid_percentage}%)"
    
    async def run_end_to_end_tests(self):
        """Execute end-to-end tests"""
        print("🔄 Running End-to-End Tests...")
        
        async with self.api_framework.test_context("e2e_complete_sale_flow", TestType.E2E):
            # Test complete sales workflow
            
            # 1. Create a test sale
            test_sale = TestDataFactory.create_test_sale()
            
            create_response = await self.api_framework.post("/api/v2/sales/transactions", json_data=test_sale)
            
            if create_response.status_code in [200, 201]:
                sale_id = create_response.json().get("id")
                
                # 2. Retrieve the created sale
                get_response = await self.api_framework.get(f"/api/v2/sales/transactions/{sale_id}")
                self.api_framework.assert_status_code(get_response, 200, "Should be able to retrieve created sale")
                
                # 3. Verify sale data
                retrieved_sale = get_response.json()
                self.api_framework.assert_equals(
                    retrieved_sale["customer_id"], 
                    test_sale["customer_id"], 
                    "Retrieved sale should match created data"
                )
                
                # 4. Check if sale appears in analytics
                await asyncio.sleep(1)  # Allow time for processing
                
                analytics_response = await self.api_framework.get(
                    "/api/v2/analytics/summary?period=1h&group_by=total"
                )
                
                if analytics_response.status_code == 200:
                    analytics_data = analytics_response.json()
                    # Verify the sale contributed to analytics
                    assert "metrics" in analytics_data, "Analytics should include metrics"
    
    async def generate_test_report(self) -> str:
        """Generate comprehensive test report"""
        print("📋 Generating Test Report...")
        
        # Collect results from all frameworks
        all_results = []
        all_results.extend(self.api_framework.test_results)
        all_results.extend(self.db_framework.test_results)
        all_results.extend(self.perf_framework.test_results)
        
        # Add performance summaries
        for framework in [self.perf_framework]:
            if hasattr(framework, 'performance_metrics'):
                for operation, metrics in framework.performance_metrics.items():
                    perf_summary = framework.get_performance_summary(operation)
                    # Add performance summary to metadata of relevant test results
                    for result in all_results:
                        if result.test_type == TestType.PERFORMANCE and operation in result.test_name:
                            result.metadata["performance_summary"] = perf_summary
        
        # Generate report
        report = self.reporter.generate_report(all_results, "test_results.json")
        
        # Print summary
        total = len(all_results)
        passed = sum(1 for r in all_results if r.status.value == "passed")
        failed = sum(1 for r in all_results if r.status.value == "failed")
        errors = sum(1 for r in all_results if r.status.value == "error")
        
        print(f"\n📊 Test Summary:")
        print(f"   Total Tests: {total}")
        print(f"   ✅ Passed: {passed}")
        print(f"   ❌ Failed: {failed}")
        print(f"   🚨 Errors: {errors}")
        print(f"   📈 Success Rate: {(passed/total*100):.1f}%")
        
        if failed > 0 or errors > 0:
            print(f"\n🔍 Failed/Error Tests:")
            for result in all_results:
                if result.status.value in ["failed", "error"]:
                    print(f"   - {result.test_name} ({result.test_type.value}): {result.error_message}")
        
        return report
    
    async def run_all_tests(self) -> str:
        """Execute complete test suite"""
        print("🚀 Starting Comprehensive Test Suite...")
        start_time = time.time()
        
        try:
            await self.setup_test_environment()
            
            # Execute all test categories
            await self.run_unit_tests()
            await self.run_integration_tests()
            await self.run_api_contract_tests()
            await self.run_database_tests()
            await self.run_performance_tests()
            await self.run_security_tests()
            await self.run_data_quality_tests()
            await self.run_end_to_end_tests()
            
            # Generate final report
            report = await self.generate_test_report()
            
            total_duration = time.time() - start_time
            print(f"\n⏱️ Total Test Duration: {total_duration:.2f} seconds")
            print(f"📄 Test report saved to: test_results.json")
            
            return report
            
        except Exception as e:
            print(f"🚨 Test suite execution failed: {e}")
            raise
            
        finally:
            await self.teardown_test_environment()


# Pytest integration
class TestComprehensiveSuite:
    """Pytest-compatible test class"""
    
    @pytest.fixture(scope="class")
    async def test_suite(self):
        """Setup test suite fixture"""
        suite = ComprehensiveTestSuite()
        await suite.setup_test_environment()
        yield suite
        await suite.teardown_test_environment()
    
    @pytest.mark.asyncio
    async def test_unit_tests(self, test_suite):
        """Run unit tests via pytest"""
        await test_suite.run_unit_tests()
        assert len(test_suite.api_framework.test_results) > 0
    
    @pytest.mark.asyncio
    async def test_integration_tests(self, test_suite):
        """Run integration tests via pytest"""
        await test_suite.run_integration_tests()
        
        # Check that integration tests passed
        integration_results = [r for r in test_suite.api_framework.test_results 
                             if r.test_type == TestType.INTEGRATION]
        passed_count = sum(1 for r in integration_results if r.status.value == "passed")
        assert passed_count > 0, "At least one integration test should pass"
    
    @pytest.mark.asyncio
    async def test_performance_benchmarks(self, test_suite):
        """Run performance tests via pytest"""
        await test_suite.run_performance_tests()
        
        # Verify performance metrics were collected
        assert len(test_suite.perf_framework.performance_metrics) > 0
        
        # Check API performance
        if "sales_api_call" in test_suite.perf_framework.performance_metrics:
            perf_summary = test_suite.perf_framework.get_performance_summary("sales_api_call")
            assert perf_summary["average"] < 5.0, "Average API response time should be reasonable"
    
    @pytest.mark.asyncio
    async def test_security_checks(self, test_suite):
        """Run security tests via pytest"""
        await test_suite.run_security_tests()
        
        # Verify security tests were executed
        security_results = [r for r in test_suite.api_framework.test_results 
                           if r.test_type == TestType.SECURITY]
        assert len(security_results) > 0, "Security tests should be executed"


# CLI interface for running tests
if __name__ == "__main__":
    async def main():
        suite = ComprehensiveTestSuite()
        report = await suite.run_all_tests()
        
        # Output report summary
        report_data = json.loads(report)
        success_rate = (report_data["summary"]["passed"] / report_data["summary"]["total_tests"]) * 100
        
        print(f"\n🎯 Final Results:")
        print(f"   Success Rate: {success_rate:.1f}%")
        print(f"   Total Duration: {report_data['summary']['total_duration']:.2f}s")
        
        # Exit with appropriate code
        if report_data["summary"]["failed"] > 0 or report_data["summary"]["errors"] > 0:
            exit(1)
        else:
            exit(0)
    
    asyncio.run(main())