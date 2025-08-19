"""
Load testing for the Retail ETL API using Locust
Tests the API under various load conditions to ensure it can handle production traffic.
"""

import random
import json
import base64
from datetime import datetime, timedelta

from locust import HttpUser, task, between, events
from locust.exception import StopUser


class RetailETLUser(HttpUser):
    """Simulates a user interacting with the Retail ETL API"""
    
    wait_time = between(1, 3)  # Wait 1-3 seconds between requests
    
    def on_start(self):
        """Initialize user session"""
        self.auth_header = self._get_auth_header()
        self.customer_ids = list(range(10000, 50000))
        self.countries = ['United Kingdom', 'France', 'Germany', 'Spain', 'Italy']
        self.product_codes = [f'PROD{i:04d}' for i in range(1000)]
    
    def _get_auth_header(self):
        """Get basic auth header"""
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        return {'Authorization': f'Basic {credentials}'}
    
    @task(10)
    def health_check(self):
        """Test health endpoint - most frequent"""
        with self.client.get("/api/v1/health", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Health check failed: {response.status_code}")
    
    @task(8)
    def get_sales_summary(self):
        """Test sales summary endpoint"""
        with self.client.get(
            "/api/v1/sales/summary",
            headers=self.auth_header,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'total_sales' in data:
                        response.success()
                    else:
                        response.failure("Invalid response format")
                except json.JSONDecodeError:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Sales summary failed: {response.status_code}")
    
    @task(6)
    def get_sales_by_country(self):
        """Test sales by country endpoint"""
        country = random.choice(self.countries)
        with self.client.get(
            f"/api/v1/sales/by-country/{country}",
            headers=self.auth_header,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 404:
                # No data for country is acceptable
                response.success()
            else:
                response.failure(f"Sales by country failed: {response.status_code}")
    
    @task(5)
    def get_customer_details(self):
        """Test customer details endpoint"""
        customer_id = random.choice(self.customer_ids)
        with self.client.get(
            f"/api/v1/customers/{customer_id}",
            headers=self.auth_header,
            catch_response=True
        ) as response:
            if response.status_code in [200, 404]:  # Both are acceptable
                response.success()
            else:
                response.failure(f"Customer details failed: {response.status_code}")
    
    @task(4)
    def get_product_analytics(self):
        """Test product analytics endpoint"""
        product_code = random.choice(self.product_codes)
        with self.client.get(
            f"/api/v1/products/{product_code}/analytics",
            headers=self.auth_header,
            catch_response=True
        ) as response:
            if response.status_code in [200, 404]:  # Both are acceptable
                response.success()
            else:
                response.failure(f"Product analytics failed: {response.status_code}")
    
    @task(3)
    def get_sales_with_filters(self):
        """Test sales endpoint with date filters"""
        start_date = (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
        end_date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'limit': random.choice([10, 25, 50, 100])
        }
        
        with self.client.get(
            "/api/v1/sales",
            headers=self.auth_header,
            params=params,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and 'data' in data:
                        response.success()
                    else:
                        response.failure("Invalid response format")
                except json.JSONDecodeError:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Filtered sales failed: {response.status_code}")
    
    @task(2)
    def search_products(self):
        """Test product search endpoint"""
        search_terms = ['WHITE', 'RED', 'HEART', 'METAL', 'LANTERN', 'CUP']
        search_term = random.choice(search_terms)
        
        params = {'q': search_term, 'limit': 20}
        
        with self.client.get(
            "/api/v1/products/search",
            headers=self.auth_header,
            params=params,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Product search failed: {response.status_code}")
    
    @task(2)
    def get_monitoring_metrics(self):
        """Test monitoring metrics endpoint"""
        with self.client.get(
            "/api/v1/monitoring/metrics",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'metrics' in data or 'system' in data:
                        response.success()
                    else:
                        response.failure("Invalid metrics response")
                except json.JSONDecodeError:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Monitoring metrics failed: {response.status_code}")
    
    @task(1)
    def trigger_etl_job(self):
        """Test ETL job trigger endpoint (lowest frequency)"""
        job_data = {
            'job_type': 'bronze',
            'source': 'sample_data',
            'dry_run': True
        }
        
        with self.client.post(
            "/api/v1/etl/jobs",
            headers=self.auth_header,
            json=job_data,
            catch_response=True
        ) as response:
            if response.status_code in [200, 202]:  # Accepted or OK
                response.success()
            else:
                response.failure(f"ETL job trigger failed: {response.status_code}")


class AdminUser(HttpUser):
    """Simulates an admin user with heavy operations"""
    
    wait_time = between(5, 10)  # Longer wait times for admin operations
    weight = 1  # Less frequent than regular users
    
    def on_start(self):
        """Initialize admin session"""
        self.auth_header = self._get_auth_header()
    
    def _get_auth_header(self):
        """Get basic auth header for admin"""
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        return {'Authorization': f'Basic {credentials}'}
    
    @task(3)
    def get_system_health(self):
        """Test detailed health endpoint"""
        with self.client.get(
            "/api/v1/health/detailed",
            headers=self.auth_header,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Detailed health failed: {response.status_code}")
    
    @task(2)
    def get_full_analytics(self):
        """Test comprehensive analytics endpoint"""
        with self.client.get(
            "/api/v1/analytics/comprehensive",
            headers=self.auth_header,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Comprehensive analytics failed: {response.status_code}")
    
    @task(1)
    def export_data(self):
        """Test data export functionality"""
        export_params = {
            'format': random.choice(['csv', 'json', 'parquet']),
            'table': random.choice(['sales', 'customers', 'products']),
            'limit': 1000
        }
        
        with self.client.get(
            "/api/v1/export/data",
            headers=self.auth_header,
            params=export_params,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Data export failed: {response.status_code}")


class BurstTrafficUser(HttpUser):
    """Simulates burst traffic patterns"""
    
    wait_time = between(0.1, 0.5)  # Very short wait times for burst
    weight = 1  # Rare but intensive
    
    def on_start(self):
        """Initialize burst session"""
        self.auth_header = self._get_auth_header()
        self.request_count = 0
        self.max_requests = random.randint(20, 50)  # Burst for 20-50 requests
    
    def _get_auth_header(self):
        """Get basic auth header"""
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        return {'Authorization': f'Basic {credentials}'}
    
    @task
    def burst_requests(self):
        """Generate burst of requests"""
        self.request_count += 1
        
        if self.request_count > self.max_requests:
            raise StopUser()  # Stop this user after burst
        
        # Mix of quick endpoints
        endpoints = [
            "/api/v1/health",
            "/api/v1/sales/summary",
            "/api/v1/monitoring/metrics"
        ]
        
        endpoint = random.choice(endpoints)
        headers = self.auth_header if endpoint != "/api/v1/health" else {}
        
        with self.client.get(endpoint, headers=headers, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Burst request failed: {response.status_code}")


# Custom event handlers for detailed reporting
@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    """Log detailed request information"""
    if exception:
        print(f"Request failed: {name} - {exception}")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize test environment"""
    print("Load test starting...")
    print(f"Target host: {environment.host}")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Clean up after test"""
    print("Load test completed!")
    
    # Print summary statistics
    stats = environment.stats
    print(f"Total requests: {stats.total.num_requests}")
    print(f"Failed requests: {stats.total.num_failures}")
    print(f"Average response time: {stats.total.avg_response_time:.2f}ms")
    print(f"Max response time: {stats.total.max_response_time:.2f}ms")
    print(f"Requests per second: {stats.total.current_rps:.2f}")


# Custom load test scenarios
class StressTestUser(HttpUser):
    """High-intensity stress testing user"""
    
    wait_time = between(0.1, 0.3)
    weight = 2
    
    def on_start(self):
        self.auth_header = self._get_auth_header()
    
    def _get_auth_header(self):
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        return {'Authorization': f'Basic {credentials}'}
    
    @task(5)
    def rapid_health_checks(self):
        """Rapid health check requests"""
        self.client.get("/api/v1/health")
    
    @task(3)
    def concurrent_sales_queries(self):
        """Multiple sales queries"""
        params = {
            'limit': random.randint(10, 100),
            'offset': random.randint(0, 1000)
        }
        self.client.get("/api/v1/sales", headers=self.auth_header, params=params)
    
    @task(2)
    def database_intensive_queries(self):
        """Database-intensive operations"""
        # This would hit endpoints that perform complex queries
        country = random.choice(['United Kingdom', 'France', 'Germany'])
        self.client.get(f"/api/v1/analytics/country/{country}", headers=self.auth_header)


# Scenario-specific user classes for different load patterns
class PeakHourUser(HttpUser):
    """Simulates peak hour traffic"""
    wait_time = between(0.5, 2)
    
    @task(15)
    def health_check(self):
        self.client.get("/api/v1/health")
    
    @task(10)
    def sales_summary(self):
        self.client.get("/api/v1/sales/summary", headers=self._get_auth_header())
    
    @task(5)
    def customer_queries(self):
        customer_id = random.randint(10000, 50000)
        self.client.get(f"/api/v1/customers/{customer_id}", headers=self._get_auth_header())
    
    def _get_auth_header(self):
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        return {'Authorization': f'Basic {credentials}'}


class OffPeakUser(HttpUser):
    """Simulates off-peak traffic"""
    wait_time = between(3, 8)
    
    @task(8)
    def health_check(self):
        self.client.get("/api/v1/health")
    
    @task(5)
    def sales_queries(self):
        self.client.get("/api/v1/sales", headers=self._get_auth_header())
    
    @task(2)
    def analytics_queries(self):
        self.client.get("/api/v1/analytics/summary", headers=self._get_auth_header())
    
    def _get_auth_header(self):
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        return {'Authorization': f'Basic {credentials}'}


# Configuration for different test scenarios
class LoadTestScenarios:
    """Define different load test scenarios"""
    
    @staticmethod
    def light_load():
        """Light load scenario - 10 users"""
        return {
            'users': 10,
            'spawn_rate': 2,
            'run_time': '5m'
        }
    
    @staticmethod
    def normal_load():
        """Normal load scenario - 50 users"""
        return {
            'users': 50,
            'spawn_rate': 5,
            'run_time': '10m'
        }
    
    @staticmethod
    def peak_load():
        """Peak load scenario - 100 users"""
        return {
            'users': 100,
            'spawn_rate': 10,
            'run_time': '15m'
        }
    
    @staticmethod
    def stress_test():
        """Stress test scenario - 200 users"""
        return {
            'users': 200,
            'spawn_rate': 20,
            'run_time': '20m'
        }
    
    @staticmethod
    def spike_test():
        """Spike test scenario - rapid scaling"""
        return {
            'users': 300,
            'spawn_rate': 50,
            'run_time': '10m'
        }


# Example usage commands:
"""
# Light load test
locust -f locustfile.py --host=http://localhost:8000 --users 10 --spawn-rate 2 --run-time 5m

# Normal load test  
locust -f locustfile.py --host=http://localhost:8000 --users 50 --spawn-rate 5 --run-time 10m

# Peak load test
locust -f locustfile.py --host=http://localhost:8000 --users 100 --spawn-rate 10 --run-time 15m

# Stress test
locust -f locustfile.py --host=http://localhost:8000 --users 200 --spawn-rate 20 --run-time 20m

# With specific user types
locust -f locustfile.py --host=http://localhost:8000 -u 50 -r 5 -t 300s --users PeakHourUser

# Headless mode with HTML report
locust -f locustfile.py --host=http://localhost:8000 --headless --users 50 --spawn-rate 5 --run-time 300s --html report.html
"""