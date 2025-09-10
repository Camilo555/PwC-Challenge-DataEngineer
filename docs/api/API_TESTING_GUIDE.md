# API Testing Guide
## Interactive API Testing and Validation Framework

[![API Coverage](https://img.shields.io/badge/API_coverage-100%25-brightgreen.svg)](https://api.pwc-data.com/docs)
[![Test Status](https://img.shields.io/badge/tests-passing-green.svg)](https://github.com/pwc/platform/actions)
[![Response Time](https://img.shields.io/badge/avg_response-24ms-brightgreen.svg)](https://api.pwc-data.com/health)

## Quick Start Testing

### Interactive API Explorer

Access the live API testing interface at: **[https://api.pwc-data.com/docs](https://api.pwc-data.com/docs)**

#### 1. Authentication Setup (30 seconds)
```bash
# Get your API token
curl -X POST "https://api.pwc-data.com/api/v1/auth/token" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "your.username@pwc.com",
    "password": "your_password"
  }'

# Response:
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "token_type": "bearer",
  "expires_in": 3600
}
```

#### 2. First API Call (1 minute)
```bash
# Test the health endpoint
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://api.pwc-data.com/api/v1/health

# Expected Response:
{
  "status": "healthy",
  "version": "2.0.0",
  "timestamp": "2025-01-25T10:30:00Z",
  "uptime": 99.95,
  "services": {
    "database": "healthy",
    "redis": "healthy",
    "kafka": "healthy"
  }
}
```

## Comprehensive API Testing Framework

### Testing Tools Overview

| Tool | Purpose | Best For | Documentation |
|------|---------|----------|---------------|
| **Swagger UI** | Interactive exploration | Quick testing, documentation | Built-in at `/docs` |
| **Postman Collection** | Team collaboration | Complex workflows | [Collection Link](https://api.pwc-data.com/postman) |
| **curl Commands** | Command-line testing | Automation, scripts | Examples below |
| **Python SDK** | Programmatic testing | Integration testing | [SDK Guide](SDK_GUIDE.md) |
| **Automated Test Suite** | Continuous validation | CI/CD pipelines | [Test Framework](../development/TESTING_FRAMEWORK.md) |

### API Testing Environments

#### Environment URLs
```yaml
Development:
  Base URL: https://api-dev.pwc-data.com
  Auth: Basic authentication (test users)
  Rate Limit: 1000 requests/hour
  
Staging:
  Base URL: https://api-staging.pwc-data.com
  Auth: JWT with test tokens
  Rate Limit: 5000 requests/hour
  
Production:
  Base URL: https://api.pwc-data.com
  Auth: JWT with enterprise SSO
  Rate Limit: 10000 requests/hour per user
```

## Interactive Testing Examples

### 1. Sales API Testing

#### Get Sales Data
```bash
# Test endpoint with query parameters
curl -X GET \
  "https://api.pwc-data.com/api/v1/sales?start_date=2025-01-01&end_date=2025-01-25&region=North America" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Accept: application/json"

# Expected Response Structure:
{
  "data": [
    {
      "id": "sale_12345",
      "amount": 1250.00,
      "currency": "USD",
      "date": "2025-01-25T08:30:00Z",
      "customer_id": "cust_67890",
      "region": "North America",
      "product_category": "Electronics"
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 20,
    "total": 1543,
    "pages": 78
  },
  "metadata": {
    "query_time": "0.024s",
    "cache_hit": true,
    "data_freshness": "2 minutes ago"
  }
}
```

#### Create Sales Record
```bash
# Test POST endpoint with validation
curl -X POST \
  "https://api.pwc-data.com/api/v1/sales" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "cust_67890",
    "amount": 1250.00,
    "currency": "USD",
    "product_id": "prod_abc123",
    "quantity": 2,
    "sale_date": "2025-01-25T08:30:00Z",
    "region": "North America"
  }'

# Success Response (201 Created):
{
  "id": "sale_99999",
  "status": "created",
  "message": "Sale record created successfully",
  "data": {
    "sale_id": "sale_99999",
    "created_at": "2025-01-25T10:35:00Z",
    "validation_status": "passed"
  }
}
```

### 2. Analytics API Testing

#### Real-time Analytics Query
```bash
# Test complex analytics query
curl -X POST \
  "https://api.pwc-data.com/api/v2/analytics/query" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "revenue by region for last 30 days with growth rate",
    "format": "json",
    "include_metadata": true
  }'

# Expected Response:
{
  "results": [
    {
      "region": "North America",
      "revenue": 2500000.00,
      "growth_rate": 0.15,
      "previous_period": 2173913.04
    },
    {
      "region": "Europe",
      "revenue": 1800000.00,
      "growth_rate": 0.08,
      "previous_period": 1666666.67
    }
  ],
  "query_metadata": {
    "execution_time": "0.156s",
    "rows_processed": 125000,
    "cache_utilization": "89%",
    "query_complexity": "medium"
  },
  "ai_insights": [
    "North America shows strongest growth at 15%",
    "European market steady with 8% growth",
    "Recommend focus on APAC expansion opportunities"
  ]
}
```

#### Natural Language Query
```bash
# Test NLP-powered analytics
curl -X POST \
  "https://api.pwc-data.com/api/v2/analytics/nlp" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What were our top 5 products by revenue last month?",
    "context": "sales_analysis",
    "response_format": "detailed"
  }'

# Expected Response:
{
  "answer": "Your top 5 products by revenue last month were...",
  "data": [
    {"product": "MacBook Pro", "revenue": 145000, "units": 125},
    {"product": "iPhone 15", "revenue": 128000, "units": 256}
  ],
  "sql_generated": "SELECT product_name, SUM(revenue)...",
  "confidence": 0.95,
  "data_sources": ["sales_fact", "product_dim"],
  "visualization_suggestion": "bar_chart"
}
```

### 3. Data Quality API Testing

#### Quality Metrics Endpoint
```bash
# Test data quality monitoring
curl -X GET \
  "https://api.pwc-data.com/api/v1/quality/metrics" \
  -H "Authorization: Bearer YOUR_TOKEN"

# Expected Response:
{
  "overall_score": 99.9,
  "last_updated": "2025-01-25T10:30:00Z",
  "metrics": {
    "completeness": {
      "score": 99.8,
      "issues": ["2 null values in customer_phone"],
      "auto_remediated": true
    },
    "accuracy": {
      "score": 99.9,
      "validation_rules_passed": 47,
      "validation_rules_failed": 0
    },
    "consistency": {
      "score": 100.0,
      "cross_table_checks": "passed",
      "referential_integrity": "valid"
    },
    "timeliness": {
      "score": 99.7,
      "data_freshness": "4 minutes",
      "sla_compliance": true
    }
  }
}
```

#### Quality Issue Reporting
```bash
# Report data quality issue
curl -X POST \
  "https://api.pwc-data.com/api/v1/quality/issues" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "sales_fact",
    "column": "amount",
    "issue_type": "outlier",
    "description": "Unusual spike in sales amounts detected",
    "severity": "medium",
    "sample_data": [15000, 18000, 22000]
  }'

# Success Response:
{
  "issue_id": "iq_12345",
  "status": "acknowledged",
  "assigned_to": "data-quality-team",
  "priority": "medium",
  "estimated_resolution": "2 hours",
  "auto_remediation": "investigating"
}
```

## GraphQL API Testing

### GraphQL Playground

Access the GraphQL interface at: **[https://api.pwc-data.com/graphql](https://api.pwc-data.com/graphql)**

#### Sample GraphQL Queries

```graphql
# Query with nested relationships
query GetSalesWithCustomerDetails {
  sales(
    filters: {
      dateRange: { start: "2025-01-01", end: "2025-01-25" }
      region: "North America"
    }
    first: 10
  ) {
    edges {
      node {
        id
        amount
        currency
        saleDate
        customer {
          id
          name
          email
          segment
          lifetimeValue
        }
        products {
          id
          name
          category
          price
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    totalCount
  }
}
```

#### GraphQL Mutations
```graphql
# Create sales record via GraphQL
mutation CreateSale($input: CreateSaleInput!) {
  createSale(input: $input) {
    sale {
      id
      amount
      currency
      createdAt
    }
    errors {
      field
      message
    }
  }
}

# Variables:
{
  "input": {
    "customerId": "cust_67890",
    "amount": 1250.00,
    "currency": "USD",
    "productId": "prod_abc123",
    "quantity": 2
  }
}
```

## API Performance Testing

### Load Testing with Artillery

#### Basic Load Test Configuration
```yaml
# artillery-config.yml
config:
  target: 'https://api.pwc-data.com'
  phases:
    - duration: 60
      arrivalRate: 10
    - duration: 120
      arrivalRate: 50
    - duration: 60
      arrivalRate: 100
  defaults:
    headers:
      Authorization: 'Bearer YOUR_TOKEN'

scenarios:
  - name: "API Health Check"
    requests:
      - get:
          url: "/api/v1/health"
  
  - name: "Sales Data Query"
    requests:
      - get:
          url: "/api/v1/sales"
          qs:
            start_date: "2025-01-01"
            end_date: "2025-01-25"
            limit: 100
```

#### Performance Benchmarks
```bash
# Run performance test
artillery run artillery-config.yml

# Expected Results:
Summary report @ 10:45:32(+0000) 2025-01-25
  Scenarios launched:  1800
  Scenarios completed: 1800
  Requests completed:  1800
  Mean response time:  24ms
  95th percentile:     45ms
  99th percentile:     120ms
  Errors:             0
```

### Response Time Validation

#### Automated Performance Checks
```python
import requests
import time

def test_api_performance():
    url = "https://api.pwc-data.com/api/v1/sales"
    headers = {"Authorization": "Bearer YOUR_TOKEN"}
    
    # Measure response time
    start_time = time.time()
    response = requests.get(url, headers=headers)
    end_time = time.time()
    
    response_time = (end_time - start_time) * 1000  # Convert to ms
    
    assert response.status_code == 200
    assert response_time < 50  # SLA requirement: <50ms
    
    print(f"Response time: {response_time:.2f}ms ✅")
    return response_time

# Run test
test_api_performance()
```

## Error Handling and Validation

### Common Error Responses

#### Authentication Errors
```bash
# Missing token
curl https://api.pwc-data.com/api/v1/sales

# Response (401 Unauthorized):
{
  "error": {
    "code": "AUTHENTICATION_REQUIRED",
    "message": "Authentication token required",
    "details": "Include 'Authorization: Bearer <token>' header",
    "timestamp": "2025-01-25T10:30:00Z",
    "request_id": "req_abc123"
  }
}
```

#### Validation Errors
```bash
# Invalid data format
curl -X POST \
  "https://api.pwc-data.com/api/v1/sales" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "invalid_format",
    "amount": "not_a_number"
  }'

# Response (422 Unprocessable Entity):
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Request validation failed",
    "errors": [
      {
        "field": "customer_id",
        "message": "Invalid customer ID format",
        "code": "INVALID_FORMAT"
      },
      {
        "field": "amount",
        "message": "Amount must be a number",
        "code": "TYPE_ERROR"
      }
    ]
  }
}
```

#### Rate Limiting
```bash
# Rate limit exceeded
curl https://api.pwc-data.com/api/v1/sales

# Response (429 Too Many Requests):
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Request rate limit exceeded",
    "rate_limit": {
      "limit": 1000,
      "remaining": 0,
      "reset_time": "2025-01-25T11:00:00Z"
    },
    "retry_after": 300
  }
}
```

## Postman Collection Testing

### Import Collection

#### Download and Import
1. **Download Collection**: [https://api.pwc-data.com/postman/collection.json](https://api.pwc-data.com/postman/collection.json)
2. **Import to Postman**: File → Import → Select downloaded file
3. **Environment Setup**: Import environment variables
4. **Authentication**: Configure bearer token in collection auth

#### Collection Structure
```
PwC Data Platform API Collection
├── 🔐 Authentication
│   ├── Get Access Token
│   └── Refresh Token
├── 📊 Sales API
│   ├── Get Sales Data
│   ├── Create Sale
│   ├── Update Sale
│   └── Delete Sale
├── 📈 Analytics API
│   ├── Real-time Query
│   ├── Natural Language Query
│   └── Batch Analytics
├── 🔍 Data Quality API
│   ├── Get Quality Metrics
│   ├── Report Issue
│   └── Get Validation Rules
└── 🛠️ Utilities
    ├── Health Check
    ├── API Version
    └── Rate Limit Status
```

#### Pre-request Scripts
```javascript
// Automatic token refresh
if (!pm.environment.get("access_token") || 
    pm.environment.get("token_expires") < Date.now()) {
    
    pm.sendRequest({
        url: pm.environment.get("auth_url") + "/token",
        method: 'POST',
        header: {
            'Content-Type': 'application/json'
        },
        body: {
            mode: 'raw',
            raw: JSON.stringify({
                username: pm.environment.get("username"),
                password: pm.environment.get("password")
            })
        }
    }, function(err, response) {
        if (!err) {
            const token = response.json().access_token;
            const expires = Date.now() + (response.json().expires_in * 1000);
            pm.environment.set("access_token", token);
            pm.environment.set("token_expires", expires);
        }
    });
}
```

#### Test Scripts
```javascript
// Validate response structure and performance
pm.test("Status code is 200", function () {
    pm.response.to.have.status(200);
});

pm.test("Response time is less than 100ms", function () {
    pm.expect(pm.response.responseTime).to.be.below(100);
});

pm.test("Response has required fields", function () {
    const jsonData = pm.response.json();
    pm.expect(jsonData).to.have.property('data');
    pm.expect(jsonData).to.have.property('pagination');
    pm.expect(jsonData).to.have.property('metadata');
});

pm.test("Data quality validation", function () {
    const jsonData = pm.response.json();
    pm.expect(jsonData.data).to.be.an('array');
    
    if (jsonData.data.length > 0) {
        const firstItem = jsonData.data[0];
        pm.expect(firstItem).to.have.property('id');
        pm.expect(firstItem).to.have.property('amount');
        pm.expect(firstItem.amount).to.be.a('number');
    }
});
```

## Automated Testing Framework

### Continuous Integration Testing

#### GitHub Actions Workflow
```yaml
# .github/workflows/api-tests.yml
name: API Integration Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours

jobs:
  api-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -r requirements-test.txt
      
      - name: Run API tests
        env:
          API_BASE_URL: ${{ secrets.API_BASE_URL }}
          API_TOKEN: ${{ secrets.API_TOKEN }}
        run: |
          pytest tests/api/ -v --html=reports/api-test-report.html
      
      - name: Upload test results
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: api-test-results
          path: reports/
```

#### Python Test Framework
```python
# tests/api/test_sales_api.py
import pytest
import requests
from datetime import datetime, timedelta

class TestSalesAPI:
    
    @pytest.fixture(autouse=True)
    def setup(self, api_client):
        self.client = api_client
        self.base_url = "https://api.pwc-data.com/api/v1"
    
    def test_get_sales_data_success(self):
        """Test successful sales data retrieval"""
        response = self.client.get(f"{self.base_url}/sales")
        
        assert response.status_code == 200
        assert response.response_time_ms < 100
        
        data = response.json()
        assert 'data' in data
        assert 'pagination' in data
        assert 'metadata' in data
        
        if data['data']:
            sale = data['data'][0]
            assert 'id' in sale
            assert 'amount' in sale
            assert isinstance(sale['amount'], (int, float))
    
    def test_create_sale_validation(self):
        """Test sale creation with validation"""
        sale_data = {
            "customer_id": "cust_12345",
            "amount": 1250.00,
            "currency": "USD",
            "product_id": "prod_abc123",
            "quantity": 2,
            "sale_date": datetime.now().isoformat()
        }
        
        response = self.client.post(f"{self.base_url}/sales", json=sale_data)
        
        assert response.status_code == 201
        assert response.json()['status'] == 'created'
        
        # Cleanup: delete created sale
        sale_id = response.json()['data']['sale_id']
        self.client.delete(f"{self.base_url}/sales/{sale_id}")
    
    def test_invalid_data_validation(self):
        """Test validation error handling"""
        invalid_data = {
            "customer_id": "",  # Empty customer ID
            "amount": "invalid_amount"  # Invalid amount type
        }
        
        response = self.client.post(f"{self.base_url}/sales", json=invalid_data)
        
        assert response.status_code == 422
        error_data = response.json()
        assert error_data['error']['code'] == 'VALIDATION_ERROR'
        assert len(error_data['error']['errors']) > 0
    
    @pytest.mark.performance
    def test_api_performance_benchmarks(self):
        """Test API performance meets SLA requirements"""
        # Test multiple requests to ensure consistent performance
        response_times = []
        
        for _ in range(10):
            response = self.client.get(f"{self.base_url}/sales?limit=100")
            assert response.status_code == 200
            response_times.append(response.response_time_ms)
        
        avg_response_time = sum(response_times) / len(response_times)
        assert avg_response_time < 50, f"Average response time {avg_response_time}ms exceeds SLA"
        
        # 95th percentile should be under 100ms
        sorted_times = sorted(response_times)
        p95_time = sorted_times[int(0.95 * len(sorted_times))]
        assert p95_time < 100, f"95th percentile {p95_time}ms exceeds SLA"
```

### Test Data Management

#### Test Data Fixtures
```python
# tests/fixtures/test_data.py
import pytest
from datetime import datetime, timedelta

@pytest.fixture
def sample_sale_data():
    """Generate sample sale data for testing"""
    return {
        "customer_id": "test_cust_12345",
        "amount": 1250.00,
        "currency": "USD",
        "product_id": "test_prod_abc123",
        "quantity": 2,
        "sale_date": datetime.now().isoformat(),
        "region": "Test Region",
        "channel": "online"
    }

@pytest.fixture
def sales_data_batch():
    """Generate batch of sales data for bulk testing"""
    base_date = datetime.now() - timedelta(days=30)
    
    return [
        {
            "customer_id": f"test_cust_{i:05d}",
            "amount": 100.0 + (i * 10),
            "currency": "USD",
            "product_id": f"test_prod_{i:03d}",
            "quantity": 1 + (i % 5),
            "sale_date": (base_date + timedelta(days=i)).isoformat(),
            "region": "Test Region",
            "channel": "online" if i % 2 == 0 else "store"
        }
        for i in range(100)
    ]

@pytest.fixture(scope="session")
def api_client(request):
    """Authenticated API client for testing"""
    from tests.utils.api_client import TestAPIClient
    
    client = TestAPIClient(
        base_url=request.config.getoption("--api-url"),
        token=request.config.getoption("--api-token")
    )
    
    yield client
    
    # Cleanup: remove test data
    client.cleanup_test_data()
```

## Security Testing

### Authentication Testing

#### Token Validation
```python
def test_token_expiration():
    """Test token expiration handling"""
    # Use expired token
    expired_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.expired_token"
    
    response = requests.get(
        "https://api.pwc-data.com/api/v1/sales",
        headers={"Authorization": f"Bearer {expired_token}"}
    )
    
    assert response.status_code == 401
    assert response.json()['error']['code'] == 'TOKEN_EXPIRED'

def test_invalid_token_format():
    """Test invalid token format handling"""
    response = requests.get(
        "https://api.pwc-data.com/api/v1/sales",
        headers={"Authorization": "Bearer invalid_token_format"}
    )
    
    assert response.status_code == 401
    assert response.json()['error']['code'] == 'INVALID_TOKEN'
```

#### Authorization Testing
```python
def test_role_based_access():
    """Test role-based access control"""
    # Test with read-only user token
    readonly_token = get_readonly_token()
    
    # GET should work
    response = requests.get(
        "https://api.pwc-data.com/api/v1/sales",
        headers={"Authorization": f"Bearer {readonly_token}"}
    )
    assert response.status_code == 200
    
    # POST should be forbidden
    response = requests.post(
        "https://api.pwc-data.com/api/v1/sales",
        headers={"Authorization": f"Bearer {readonly_token}"},
        json={"customer_id": "test", "amount": 100}
    )
    assert response.status_code == 403
    assert response.json()['error']['code'] == 'INSUFFICIENT_PERMISSIONS'
```

### Data Security Testing

#### Input Sanitization
```python
def test_sql_injection_prevention():
    """Test SQL injection prevention"""
    malicious_input = "'; DROP TABLE sales; --"
    
    response = requests.get(
        f"https://api.pwc-data.com/api/v1/sales?customer_id={malicious_input}",
        headers={"Authorization": f"Bearer {valid_token}"}
    )
    
    # Should return safe error, not execute malicious SQL
    assert response.status_code in [400, 422]
    assert "syntax error" not in response.text.lower()

def test_xss_prevention():
    """Test XSS prevention in API responses"""
    xss_payload = "<script>alert('xss')</script>"
    
    response = requests.post(
        "https://api.pwc-data.com/api/v1/sales",
        headers={"Authorization": f"Bearer {valid_token}"},
        json={"customer_id": xss_payload, "amount": 100}
    )
    
    # Response should sanitize or reject malicious input
    if response.status_code == 201:
        # If accepted, ensure output is sanitized
        created_sale = response.json()['data']
        assert "<script>" not in str(created_sale)
```

## API Monitoring and Alerting

### Performance Monitoring Setup

#### Custom Metrics Collection
```python
# Monitor API performance in real-time
import time
import statistics
from datadog import DogStatsdClient

statsd = DogStatsdClient()

def monitor_api_performance():
    """Monitor key API performance metrics"""
    endpoints = [
        "/api/v1/sales",
        "/api/v1/analytics/query",
        "/api/v1/quality/metrics"
    ]
    
    for endpoint in endpoints:
        response_times = []
        
        for _ in range(10):
            start_time = time.time()
            response = requests.get(f"https://api.pwc-data.com{endpoint}")
            end_time = time.time()
            
            response_time = (end_time - start_time) * 1000
            response_times.append(response_time)
            
            # Send metrics to DataDog
            statsd.histogram(
                'api.response_time',
                response_time,
                tags=[f'endpoint:{endpoint}', f'status:{response.status_code}']
            )
        
        # Calculate and report statistics
        avg_time = statistics.mean(response_times)
        p95_time = statistics.quantiles(response_times, n=20)[18]  # 95th percentile
        
        print(f"{endpoint}: avg={avg_time:.2f}ms, p95={p95_time:.2f}ms")
        
        # Alert if performance degrades
        if avg_time > 100:  # SLA threshold
            statsd.event(
                title="API Performance Alert",
                text=f"{endpoint} response time {avg_time:.2f}ms exceeds SLA",
                alert_type="warning",
                tags=[f'endpoint:{endpoint}']
            )

# Run monitoring
monitor_api_performance()
```

#### Health Check Automation
```bash
#!/bin/bash
# health-check.sh - Automated API health monitoring

API_BASE="https://api.pwc-data.com"
SLACK_WEBHOOK="your-slack-webhook-url"
EMAIL_ALERT="api-alerts@pwc.com"

# Check critical endpoints
check_endpoint() {
    local endpoint=$1
    local max_response_time=$2
    
    start_time=$(date +%s.%N)
    http_code=$(curl -o /dev/null -s -w "%{http_code}" -H "Authorization: Bearer $API_TOKEN" "$API_BASE$endpoint")
    end_time=$(date +%s.%N)
    
    response_time=$(echo "($end_time - $start_time) * 1000" | bc -l)
    response_time_int=${response_time%.*}
    
    if [[ $http_code -ne 200 ]]; then
        send_alert "CRITICAL" "$endpoint returned $http_code"
        return 1
    elif [[ $response_time_int -gt $max_response_time ]]; then
        send_alert "WARNING" "$endpoint response time ${response_time_int}ms exceeds ${max_response_time}ms"
        return 2
    else
        echo "✅ $endpoint: ${http_code} (${response_time_int}ms)"
        return 0
    fi
}

send_alert() {
    local level=$1
    local message=$2
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # Send to Slack
    curl -X POST $SLACK_WEBHOOK \
        -H 'Content-type: application/json' \
        -d "{\"text\":\"[$level] API Alert: $message at $timestamp\"}"
    
    # Send email alert for critical issues
    if [[ $level == "CRITICAL" ]]; then
        echo "Subject: API Critical Alert\n\n$message\n\nTimestamp: $timestamp" | \
            mail -s "API Critical Alert" $EMAIL_ALERT
    fi
}

# Run health checks
echo "Running API health checks..."

check_endpoint "/api/v1/health" 50
check_endpoint "/api/v1/sales" 100
check_endpoint "/api/v2/analytics/query" 200
check_endpoint "/api/v1/quality/metrics" 150

echo "Health check completed at $(date)"
```

## Documentation and Best Practices

### API Testing Best Practices

1. **Test Pyramid Approach**
   - **Unit Tests**: Individual endpoint functionality
   - **Integration Tests**: Cross-service interactions
   - **End-to-End Tests**: Complete user workflows
   - **Performance Tests**: Load and stress testing

2. **Environment Strategy**
   - **Development**: Rapid feedback for developers
   - **Staging**: Production-like environment for validation
   - **Production**: Monitoring and canary testing

3. **Data Management**
   - **Test Data Isolation**: Separate test data from production
   - **Data Cleanup**: Automatic cleanup of test artifacts
   - **Synthetic Data**: Use realistic but non-sensitive data

4. **Security Testing**
   - **Authentication Testing**: Verify all auth scenarios
   - **Authorization Testing**: Test role-based access
   - **Input Validation**: Prevent injection attacks
   - **Data Encryption**: Verify data protection

### Testing Checklist

#### Pre-Testing Setup
- [ ] Environment URLs configured correctly
- [ ] Authentication tokens available and valid
- [ ] Test data prepared and isolated
- [ ] Monitoring and logging enabled
- [ ] Backup plan for data recovery

#### Functional Testing
- [ ] All endpoints return correct status codes
- [ ] Response schemas match API documentation
- [ ] Required fields are properly validated
- [ ] Optional fields behave as expected
- [ ] Error responses include helpful messages

#### Performance Testing
- [ ] Response times meet SLA requirements
- [ ] API handles expected load volumes
- [ ] Rate limiting works correctly
- [ ] Caching improves performance
- [ ] Database queries are optimized

#### Security Testing
- [ ] Authentication is required for protected endpoints
- [ ] Authorization correctly restricts access
- [ ] Input sanitization prevents injection
- [ ] Sensitive data is not exposed
- [ ] HTTPS is enforced

#### Integration Testing
- [ ] Cross-service communication works
- [ ] Database transactions are consistent
- [ ] Message queues process correctly
- [ ] External API integrations function
- [ ] Error handling cascades properly

## Conclusion

This comprehensive API testing guide provides everything needed to validate the PwC Enterprise Data Platform APIs. The combination of interactive testing tools, automated test suites, performance monitoring, and security validation ensures robust API quality and reliability.

### Next Steps

1. **Set up your testing environment** using the authentication guide
2. **Import the Postman collection** for quick interactive testing
3. **Run the automated test suite** to validate API functionality
4. **Implement continuous monitoring** for production API health
5. **Customize the test framework** for your specific use cases

### Resources

- **API Documentation**: [https://api.pwc-data.com/docs](https://api.pwc-data.com/docs)
- **GraphQL Playground**: [https://api.pwc-data.com/graphql](https://api.pwc-data.com/graphql)
- **Postman Collection**: [Download Collection](https://api.pwc-data.com/postman/collection.json)
- **Test Framework Repository**: [GitHub Repository](https://github.com/pwc/api-tests)
- **Support**: api-testing-support@pwc.com

---

**Document Information**
- **Version**: 2.0.0
- **Last Updated**: January 25, 2025
- **Maintained By**: API Testing Team
- **Next Review**: April 25, 2025
- **Feedback**: api-docs@pwc.com