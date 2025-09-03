# Interactive API Guide - PwC Data Engineering Platform

## 🚀 Getting Started with the API

This guide provides hands-on examples and interactive workflows to help you quickly understand and use the PwC Data Engineering Platform API.

## 🔑 Authentication Quick Start

### Step 1: Obtain API Key or JWT Token

**Option A: JWT Authentication (Recommended)**
```bash
# Login to get JWT token
curl -X POST "http://localhost:8000/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "demo_user",
    "password": "demo_password",
    "mfa_token": "123456"
  }'
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 3600,
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user_info": {
    "user_id": "user123",
    "roles": ["data_analyst", "api_user"],
    "permissions": ["data:read", "analytics:access"],
    "clearance_level": "standard"
  }
}
```

**Option B: API Key Authentication**
```bash
# Use API key directly
export API_KEY="pwc_your_api_key_here"
```

### Step 2: Set Up Your Environment

Create a `.env` file for your API interactions:
```bash
# API Configuration
API_BASE_URL=http://localhost:8000
API_VERSION=v1
JWT_TOKEN=your_jwt_token_here
API_KEY=pwc_your_api_key_here

# For production
# API_BASE_URL=https://api.pwc-data.com
```

## 🎯 Interactive API Workflows

### Workflow 1: Sales Data Analysis (10 minutes)

This workflow demonstrates how to retrieve, analyze, and visualize sales data.

#### Step 1: Check API Health
```bash
curl -X GET "${API_BASE_URL}/api/v1/health" \
  -H "Authorization: Bearer ${JWT_TOKEN}"
```

**Expected Response:**
```json
{
  "status": "healthy",
  "version": "3.0.0",
  "timestamp": "2025-08-30T10:00:00Z",
  "services": {
    "database": "healthy",
    "cache": "healthy",
    "message_queue": "healthy"
  },
  "metrics": {
    "uptime_seconds": 86400,
    "requests_per_second": 150.5,
    "active_connections": 42
  }
}
```

#### Step 2: Get Sales Summary
```bash
curl -X GET "${API_BASE_URL}/api/v1/sales/summary" \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -H "Content-Type: application/json" \
  -G -d "date_range=2025-08-01:2025-08-30" \
  -d "group_by=country,product_category"
```

**Expected Response:**
```json
{
  "summary": {
    "total_revenue": 15750000.00,
    "total_transactions": 45230,
    "average_order_value": 348.12,
    "period": "2025-08-01 to 2025-08-30"
  },
  "breakdown": [
    {
      "country": "US",
      "product_category": "Electronics",
      "revenue": 5250000.00,
      "transactions": 12500,
      "avg_order_value": 420.00
    },
    {
      "country": "UK",
      "product_category": "Clothing",
      "revenue": 3200000.00,
      "transactions": 18750,
      "avg_order_value": 170.67
    }
  ],
  "metadata": {
    "query_time_ms": 45,
    "cache_hit": false,
    "data_freshness": "2025-08-30T09:45:00Z"
  }
}
```

#### Step 3: Get Detailed Transactions
```bash
curl -X GET "${API_BASE_URL}/api/v1/sales/transactions" \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -G -d "limit=10" \
  -d "offset=0" \
  -d "filters={\"country\": \"US\", \"amount_gte\": 1000}" \
  -d "sort=amount:desc"
```

**Expected Response:**
```json
{
  "transactions": [
    {
      "transaction_id": "txn_001",
      "customer_id": "cust_12345",
      "amount": 2499.99,
      "currency": "USD",
      "product_category": "Electronics",
      "country": "US",
      "timestamp": "2025-08-29T14:30:00Z",
      "payment_method": "credit_card",
      "status": "completed"
    }
  ],
  "pagination": {
    "total": 1250,
    "limit": 10,
    "offset": 0,
    "has_next": true,
    "has_prev": false
  }
}
```

### Workflow 2: Real-Time Analytics with WebSocket (15 minutes)

This workflow demonstrates real-time data streaming capabilities.

#### Step 1: Connect to WebSocket
```javascript
// JavaScript WebSocket client example
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/analytics');

// Authentication
ws.onopen = function() {
  ws.send(JSON.stringify({
    type: 'auth',
    token: 'your_jwt_token_here'
  }));
};

// Subscribe to real-time metrics
ws.send(JSON.stringify({
  type: 'subscribe',
  channels: ['sales_metrics', 'system_health'],
  filters: {
    country: ['US', 'UK'],
    metric_interval: '1m'
  }
}));

// Handle incoming data
ws.onmessage = function(event) {
  const data = JSON.parse(event.data);
  console.log('Real-time data:', data);
};
```

#### Step 2: Python WebSocket Client
```python
import asyncio
import json
import websockets
import os

async def realtime_analytics():
    uri = f"{os.getenv('API_BASE_URL').replace('http', 'ws')}/api/v1/ws/analytics"
    
    async with websockets.connect(uri) as websocket:
        # Authenticate
        auth_message = {
            "type": "auth",
            "token": os.getenv('JWT_TOKEN')
        }
        await websocket.send(json.dumps(auth_message))
        
        # Subscribe to metrics
        subscribe_message = {
            "type": "subscribe",
            "channels": ["sales_metrics", "data_quality"],
            "filters": {
                "update_interval": "30s",
                "metric_types": ["revenue_rate", "transaction_count"]
            }
        }
        await websocket.send(json.dumps(subscribe_message))
        
        # Listen for updates
        async for message in websocket:
            data = json.loads(message)
            if data["type"] == "metrics_update":
                print(f"Revenue Rate: ${data['metrics']['revenue_rate']}/min")
                print(f"Transaction Count: {data['metrics']['transaction_count']}")

# Run the WebSocket client
asyncio.run(realtime_analytics())
```

### Workflow 3: GraphQL Advanced Queries (20 minutes)

This workflow demonstrates powerful GraphQL queries for complex data relationships.

#### Step 1: Basic GraphQL Query
```graphql
query GetSalesInsights($dateRange: DateRange!, $countries: [String!]) {
  salesAnalytics(dateRange: $dateRange, countries: $countries) {
    summary {
      totalRevenue
      transactionCount
      averageOrderValue
    }
    trends {
      date
      revenue
      transactionCount
    }
    topProducts(limit: 5) {
      productId
      name
      revenue
      salesCount
    }
    customerSegments {
      segment
      customerCount
      totalRevenue
      averageLifetimeValue
    }
  }
}
```

**Variables:**
```json
{
  "dateRange": {
    "start": "2025-08-01",
    "end": "2025-08-30"
  },
  "countries": ["US", "UK", "CA"]
}
```

#### Step 2: Execute GraphQL Query with curl
```bash
curl -X POST "${API_BASE_URL}/api/v1/graphql" \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetSalesInsights($dateRange: DateRange!, $countries: [String!]) { salesAnalytics(dateRange: $dateRange, countries: $countries) { summary { totalRevenue transactionCount averageOrderValue } trends { date revenue transactionCount } topProducts(limit: 5) { productId name revenue salesCount } } }",
    "variables": {
      "dateRange": {"start": "2025-08-01", "end": "2025-08-30"},
      "countries": ["US", "UK", "CA"]
    }
  }'
```

#### Step 3: Complex Nested Query
```graphql
query GetComprehensiveReport($filters: SalesFilters!) {
  salesReport(filters: $filters) {
    summary {
      totalRevenue
      growth {
        monthOverMonth
        yearOverYear
      }
    }
    
    geographicBreakdown {
      country
      revenue
      growth
      marketShare
      customers {
        totalCount
        newCustomers
        returningCustomers
      }
    }
    
    productPerformance {
      category
      products {
        id
        name
        revenue
        margin
        inventory {
          currentStock
          reorderLevel
          projectedDemand
        }
      }
    }
    
    dataQuality {
      completenessScore
      accuracyScore
      timeliness
      issues {
        type
        count
        severity
      }
    }
  }
}
```

## 🔧 API Development Tools

### Postman Collection

Download our comprehensive Postman collection:
```bash
curl -O "${API_BASE_URL}/api/v1/docs/postman-collection.json"
```

Import into Postman and set these environment variables:
- `base_url`: http://localhost:8000 (or production URL)
- `jwt_token`: Your JWT token
- `api_key`: Your API key

### OpenAPI/Swagger Integration

Interactive API documentation is available at:
- **Development**: http://localhost:8000/docs
- **Production**: https://api.pwc-data.com/docs

#### Custom OpenAPI Client Generation

Generate Python client:
```bash
# Install OpenAPI Generator
npm install @openapitools/openapi-generator-cli -g

# Generate Python client
openapi-generator-cli generate \
  -i http://localhost:8000/openapi.json \
  -g python \
  -o ./pwc-api-client \
  --additional-properties=packageName=pwc_api_client
```

Generate TypeScript client:
```bash
openapi-generator-cli generate \
  -i http://localhost:8000/openapi.json \
  -g typescript-fetch \
  -o ./pwc-api-client-ts
```

### Python SDK Example

```python
from pwc_api_client import PWCDataPlatform
import asyncio

async def main():
    # Initialize client
    client = PWCDataPlatform(
        base_url="http://localhost:8000",
        api_key="pwc_your_api_key_here"
    )
    
    # Get sales summary
    sales_summary = await client.sales.get_summary(
        date_range="2025-08-01:2025-08-30",
        group_by=["country", "product_category"]
    )
    
    print(f"Total Revenue: ${sales_summary.total_revenue:,.2f}")
    
    # Real-time analytics subscription
    async for update in client.analytics.subscribe_realtime(
        metrics=["revenue_rate", "transaction_count"],
        interval="30s"
    ):
        print(f"Revenue Rate: ${update.revenue_rate}/min")
        print(f"Transactions: {update.transaction_count}")

asyncio.run(main())
```

## 🧪 Testing Your API Integration

### Unit Tests for API Clients

```python
import pytest
import httpx
from unittest.mock import AsyncMock, patch

class TestPWCAPIClient:
    @pytest.fixture
    def client(self):
        return PWCDataPlatform(
            base_url="http://test.example.com",
            api_key="test_key"
        )
    
    @pytest.mark.asyncio
    async def test_get_sales_summary(self, client):
        # Mock the HTTP response
        mock_response = {
            "summary": {
                "total_revenue": 1000000.00,
                "total_transactions": 5000,
                "average_order_value": 200.00
            }
        }
        
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.return_value.json.return_value = mock_response
            
            result = await client.sales.get_summary(
                date_range="2025-08-01:2025-08-30"
            )
            
            assert result.total_revenue == 1000000.00
            assert result.total_transactions == 5000
    
    @pytest.mark.asyncio
    async def test_authentication_flow(self, client):
        # Test JWT authentication
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value.json.return_value = {
                "access_token": "test_token",
                "token_type": "bearer",
                "expires_in": 3600
            }
            
            token = await client.auth.login(
                username="test_user",
                password="test_password"
            )
            
            assert token.access_token == "test_token"
            assert token.expires_in == 3600
```

### Integration Tests

```python
import pytest
import asyncio
from pwc_api_client import PWCDataPlatform

@pytest.mark.integration
class TestAPIIntegration:
    @pytest.fixture
    async def authenticated_client(self):
        client = PWCDataPlatform(base_url="http://localhost:8000")
        
        # Login and get token
        auth_result = await client.auth.login(
            username="test_user",
            password="test_password"
        )
        
        client.set_auth_token(auth_result.access_token)
        return client
    
    @pytest.mark.asyncio
    async def test_full_sales_workflow(self, authenticated_client):
        # Test complete sales analysis workflow
        client = authenticated_client
        
        # 1. Get health status
        health = await client.system.get_health()
        assert health.status == "healthy"
        
        # 2. Get sales summary
        summary = await client.sales.get_summary(
            date_range="2025-08-01:2025-08-30"
        )
        assert summary.total_revenue > 0
        
        # 3. Get detailed transactions
        transactions = await client.sales.get_transactions(
            limit=10,
            filters={"country": "US"}
        )
        assert len(transactions.data) <= 10
        
        # 4. Test GraphQL query
        graphql_result = await client.graphql.query(
            query="""
            query { 
                salesAnalytics(dateRange: {start: "2025-08-01", end: "2025-08-30"}) {
                    summary { totalRevenue }
                }
            }
            """
        )
        assert graphql_result.data.salesAnalytics.summary.totalRevenue > 0
```

## 📊 Performance and Rate Limiting

### Understanding Rate Limits

```python
import asyncio
import time
from pwc_api_client import PWCDataPlatform, RateLimitException

async def handle_rate_limiting():
    client = PWCDataPlatform(
        base_url="http://localhost:8000",
        api_key="your_key"
    )
    
    try:
        # This might hit rate limits
        tasks = []
        for i in range(100):
            tasks.append(client.sales.get_summary())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, RateLimitException):
                print(f"Rate limited. Retry after: {result.retry_after} seconds")
                await asyncio.sleep(result.retry_after)
    
    except Exception as e:
        print(f"Error: {e}")
```

### Batch Processing Best Practices

```python
async def efficient_batch_processing():
    client = PWCDataPlatform(base_url="http://localhost:8000")
    
    # Use batch endpoints when available
    transaction_ids = [f"txn_{i}" for i in range(1000)]
    
    # Process in batches of 50
    batch_size = 50
    results = []
    
    for i in range(0, len(transaction_ids), batch_size):
        batch = transaction_ids[i:i + batch_size]
        
        # Use batch endpoint
        batch_results = await client.sales.get_transactions_batch(
            transaction_ids=batch
        )
        results.extend(batch_results)
        
        # Respect rate limits
        await asyncio.sleep(0.1)
    
    return results
```

## 🚨 Error Handling and Troubleshooting

### Common Error Scenarios

```python
from pwc_api_client import (
    PWCDataPlatform, 
    AuthenticationError, 
    RateLimitException,
    ValidationError,
    ServerError
)

async def robust_api_client():
    client = PWCDataPlatform(base_url="http://localhost:8000")
    
    try:
        # Attempt operation
        result = await client.sales.get_summary()
        return result
        
    except AuthenticationError as e:
        # Handle authentication failures
        print(f"Authentication failed: {e.message}")
        # Refresh token or re-login
        await client.auth.refresh_token()
        
    except RateLimitException as e:
        # Handle rate limiting
        print(f"Rate limited. Waiting {e.retry_after} seconds")
        await asyncio.sleep(e.retry_after)
        return await client.sales.get_summary()  # Retry
        
    except ValidationError as e:
        # Handle validation errors
        print(f"Validation error: {e.details}")
        for field, messages in e.field_errors.items():
            print(f"  {field}: {', '.join(messages)}")
            
    except ServerError as e:
        # Handle server errors
        print(f"Server error: {e.message}")
        # Implement exponential backoff
        await asyncio.sleep(2 ** attempt)
        
    except Exception as e:
        # Handle unexpected errors
        print(f"Unexpected error: {e}")
        # Log for debugging
        logger.error(f"API call failed", exc_info=True)
```

## 📈 Monitoring Your API Usage

### Usage Analytics Dashboard

```python
async def get_api_usage_stats():
    client = PWCDataPlatform(base_url="http://localhost:8000")
    
    # Get your API usage statistics
    usage_stats = await client.account.get_usage_stats(
        period="last_30_days"
    )
    
    print(f"Total Requests: {usage_stats.total_requests:,}")
    print(f"Rate Limit Usage: {usage_stats.rate_limit_usage:.1%}")
    print(f"Error Rate: {usage_stats.error_rate:.2%}")
    
    # Get quota information
    quota_info = await client.account.get_quota_info()
    print(f"Monthly Quota: {quota_info.monthly_limit:,}")
    print(f"Used This Month: {quota_info.used_this_month:,}")
    print(f"Remaining: {quota_info.remaining:,}")
```

## 🎯 Advanced Use Cases

### Custom Analytics Pipeline

```python
import pandas as pd
from pwc_api_client import PWCDataPlatform

async def build_custom_analytics_pipeline():
    client = PWCDataPlatform(base_url="http://localhost:8000")
    
    # 1. Extract data from multiple sources
    sales_data = await client.sales.get_transactions(
        date_range="2025-08-01:2025-08-30",
        limit=10000
    )
    
    customer_data = await client.customers.get_profiles(
        customer_ids=[t.customer_id for t in sales_data]
    )
    
    product_data = await client.products.get_catalog()
    
    # 2. Transform data using pandas
    sales_df = pd.DataFrame([t.dict() for t in sales_data])
    customer_df = pd.DataFrame([c.dict() for c in customer_data])
    
    # 3. Create enriched dataset
    enriched_data = sales_df.merge(
        customer_df, on='customer_id'
    ).merge(
        product_data, on='product_id'
    )
    
    # 4. Calculate custom metrics
    customer_ltv = enriched_data.groupby('customer_id').agg({
        'amount': ['sum', 'count', 'mean']
    }).round(2)
    
    # 5. Store results back to platform
    await client.analytics.store_custom_metrics(
        metric_name="customer_ltv",
        data=customer_ltv.to_dict('records')
    )
    
    return customer_ltv
```

---

## 🔗 Quick Reference Links

- **[OpenAPI Specification](./OPENAPI_SPECIFICATION.yaml)** - Complete API schema
- **[Postman Collection](./postman/PWC_API_Collection.json)** - Ready-to-use API collection
- **[GraphQL Playground](http://localhost:8000/graphql)** - Interactive GraphQL explorer
- **[API Status Page](https://status.pwc-data.com)** - Real-time API health status
- **[Rate Limits & Quotas](./RATE_LIMITS.md)** - Usage limits and best practices
- **[Changelog](./CHANGELOG.md)** - API version history and breaking changes

**Last Updated**: August 30, 2025  
**API Version**: 3.0.0  
**Guide Version**: 1.0.0