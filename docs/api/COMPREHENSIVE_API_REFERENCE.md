# Comprehensive API Reference - PwC Data Engineering Platform

## 📋 Table of Contents

1. [Quick Start Guide](#quick-start-guide)
2. [Authentication & Security](#authentication--security)
3. [Core API Endpoints](#core-api-endpoints)
4. [GraphQL API](#graphql-api)
5. [WebSocket Real-time APIs](#websocket-real-time-apis)
6. [Error Handling](#error-handling)
7. [Rate Limiting & Quotas](#rate-limiting--quotas)
8. [SDK Examples](#sdk-examples)
9. [Postman Collection](#postman-collection)

## 🚀 Quick Start Guide

### Base URLs
- **Development**: `http://localhost:8000`
- **Production**: `https://api.pwc-data.com`

### API Versions
- **v1**: Production-ready endpoints (current: v1.0.0)
- **v2**: Advanced analytics and features (current: v2.0.0)

### Content Types
- **Request**: `application/json`
- **Response**: `application/json` (optimized with ORJSON)

## 🔑 Authentication & Security

### JWT Authentication (Recommended)

#### Login
```http
POST /api/v1/auth/login
Content-Type: application/json

{
  "username": "demo_user",
  "password": "demo_password",
  "mfa_token": "123456"
}
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

#### Using JWT Token
```http
GET /api/v1/sales/summary
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### API Key Authentication

#### Create API Key
```http
POST /api/v1/auth/api-keys
Authorization: Bearer {jwt_token}
Content-Type: application/json

{
  "name": "My Integration Key",
  "permissions": ["data:read", "analytics:access"],
  "expires_in": 2592000
}
```

#### Using API Key
```http
GET /api/v1/sales/summary
Authorization: Bearer pwc_ak_1234567890abcdef...
```

### OAuth2/OIDC Integration

```http
GET /api/v1/auth/oauth/authorize?client_id={client_id}&response_type=code&redirect_uri={redirect_uri}&scope=read+write
```

## 📊 Core API Endpoints

### Health & System Status

#### System Health Check
```http
GET /api/v1/health
```

**Response:**
```json
{
  "status": "healthy",
  "environment": "production",
  "version": "4.0.0",
  "timestamp": "2025-08-30T10:00:00Z",
  "security": {
    "platform_status": "operational",
    "security_level": "operational",
    "auth_enabled": true,
    "dlp_enabled": true,
    "compliance_monitoring": true
  },
  "metrics": {
    "active_connections": 42,
    "security_events_24h": 0,
    "compliance_score": 1.0
  }
}
```

### Sales Data API (v1)

#### Get Sales Summary
```http
GET /api/v1/sales/summary?date_range=2025-08-01:2025-08-30&group_by=country,product_category
Authorization: Bearer {token}
```

**Query Parameters:**
- `date_range` (string): Date range in format YYYY-MM-DD:YYYY-MM-DD
- `group_by` (string): Comma-separated list of grouping fields
- `currency` (string, optional): Currency code for conversion
- `include_trends` (boolean, optional): Include trend analysis

**Response:**
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
    }
  ],
  "metadata": {
    "query_time_ms": 45,
    "cache_hit": false,
    "data_freshness": "2025-08-30T09:45:00Z"
  }
}
```

#### Get Transaction Details
```http
GET /api/v1/sales/transactions?limit=50&offset=0&filters={"country":"US","amount_gte":1000}&sort=amount:desc
Authorization: Bearer {token}
```

**Query Parameters:**
- `limit` (integer): Number of records to return (max: 1000)
- `offset` (integer): Number of records to skip
- `filters` (string): JSON object with filter conditions
- `sort` (string): Sort field and direction (field:asc|desc)
- `include_customer_data` (boolean): Include customer information

**Response:**
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
      "status": "completed",
      "customer_info": {
        "segment": "premium",
        "lifetime_value": 15000.00
      }
    }
  ],
  "pagination": {
    "total": 1250,
    "limit": 50,
    "offset": 0,
    "has_next": true,
    "has_prev": false
  }
}
```

### Advanced Analytics API (v2)

#### Advanced Sales Analytics
```http
POST /api/v2/analytics/sales-insights
Authorization: Bearer {token}
Content-Type: application/json

{
  "date_range": {
    "start": "2025-08-01",
    "end": "2025-08-30"
  },
  "dimensions": ["country", "product_category", "customer_segment"],
  "metrics": ["revenue", "transaction_count", "avg_order_value"],
  "filters": {
    "country": ["US", "UK", "CA"],
    "product_category": ["Electronics", "Clothing"]
  },
  "advanced_options": {
    "include_forecasting": true,
    "include_anomaly_detection": true,
    "include_cohort_analysis": true
  }
}
```

**Response:**
```json
{
  "insights": {
    "summary": {
      "total_revenue": 15750000.00,
      "growth_rate": 0.125,
      "market_trends": "positive"
    },
    "dimensional_breakdown": [
      {
        "dimensions": {
          "country": "US",
          "product_category": "Electronics",
          "customer_segment": "premium"
        },
        "metrics": {
          "revenue": 5250000.00,
          "transaction_count": 12500,
          "avg_order_value": 420.00
        },
        "insights": {
          "trend": "increasing",
          "anomalies": [],
          "forecast": {
            "next_30_days": 6000000.00,
            "confidence": 0.85
          }
        }
      }
    ],
    "cohort_analysis": {
      "retention_rates": {
        "month_1": 0.75,
        "month_3": 0.45,
        "month_6": 0.32
      }
    }
  }
}
```

### Data Quality API

#### Data Quality Assessment
```http
GET /api/v1/data-quality/assessment?dataset=sales_transactions&date_range=2025-08-01:2025-08-30
Authorization: Bearer {token}
```

**Response:**
```json
{
  "assessment": {
    "overall_score": 0.95,
    "completeness": 0.98,
    "accuracy": 0.94,
    "consistency": 0.96,
    "timeliness": 0.92
  },
  "issues": [
    {
      "type": "missing_values",
      "field": "customer_id",
      "count": 45,
      "severity": "medium"
    }
  ],
  "recommendations": [
    "Implement data validation for customer_id field",
    "Add data freshness monitoring"
  ]
}
```

## 🌐 GraphQL API

### GraphQL Endpoint
```http
POST /api/graphql
Authorization: Bearer {token}
Content-Type: application/json
```

### Comprehensive Sales Query
```graphql
query GetSalesInsights($dateRange: DateRange!, $countries: [String!]) {
  salesAnalytics(dateRange: $dateRange, countries: $countries) {
    summary {
      totalRevenue
      transactionCount
      averageOrderValue
      growthRate
    }
    trends {
      date
      revenue
      transactionCount
      movingAverage
    }
    topProducts(limit: 5) {
      productId
      name
      revenue
      salesCount
      margin
    }
    customerSegments {
      segment
      customerCount
      totalRevenue
      averageLifetimeValue
      retentionRate
    }
    geographicBreakdown {
      country
      revenue
      marketShare
      growth
    }
    dataQuality {
      completenessScore
      accuracyScore
      issues {
        type
        count
        severity
      }
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

### Real-time Subscription
```graphql
subscription RealtimeMetrics($filters: MetricFilters) {
  metricsUpdates(filters: $filters) {
    timestamp
    revenue
    transactionCount
    activeUsers
    alerts {
      type
      severity
      message
    }
  }
}
```

## 🔌 WebSocket Real-time APIs

### Connection Setup
```javascript
const ws = new WebSocket('wss://api.pwc-data.com/api/v1/ws/analytics');

// Authentication
ws.onopen = function() {
  ws.send(JSON.stringify({
    type: 'auth',
    token: 'your_jwt_token_here'
  }));
};
```

### Subscribe to Metrics
```javascript
ws.send(JSON.stringify({
  type: 'subscribe',
  channels: ['sales_metrics', 'system_health'],
  filters: {
    country: ['US', 'UK'],
    metric_interval: '1m'
  }
}));
```

### Real-time Data Stream
```json
{
  "type": "metrics_update",
  "channel": "sales_metrics",
  "timestamp": "2025-08-30T10:05:00Z",
  "data": {
    "revenue_rate": 1250.50,
    "transaction_count": 45,
    "active_users": 1205,
    "conversion_rate": 0.034
  }
}
```

## ⚠️ Error Handling

### Standard Error Response
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid request parameters",
    "details": {
      "field": "date_range",
      "issue": "Invalid date format"
    },
    "request_id": "req_123456789",
    "timestamp": "2025-08-30T10:00:00Z"
  }
}
```

### HTTP Status Codes
- **200 OK**: Successful request
- **201 Created**: Resource created successfully
- **400 Bad Request**: Invalid request parameters
- **401 Unauthorized**: Authentication required
- **403 Forbidden**: Insufficient permissions
- **404 Not Found**: Resource not found
- **429 Too Many Requests**: Rate limit exceeded
- **500 Internal Server Error**: Server error

### Error Codes
- `VALIDATION_ERROR`: Invalid request parameters
- `AUTHENTICATION_ERROR`: Invalid or expired credentials
- `AUTHORIZATION_ERROR`: Insufficient permissions
- `RATE_LIMIT_EXCEEDED`: Too many requests
- `DATA_NOT_FOUND`: Requested data not available
- `SERVICE_UNAVAILABLE`: Service temporarily unavailable

## 🚦 Rate Limiting & Quotas

### Rate Limits
- **Authentication endpoints**: 5 requests per minute
- **Analytics endpoints**: 20 requests per minute (burst: 30)
- **General endpoints**: 100 requests per minute (burst: 150)

### Response Headers
```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1693392000
X-RateLimit-Burst: 150
```

### Handling Rate Limits
```python
import time
import requests

def api_request_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            time.sleep(retry_after)
            continue
            
        return response
    
    raise Exception("Max retries exceeded")
```

## 🔧 SDK Examples

### Python SDK
```python
from pwc_api_client import PWCDataPlatform
import asyncio

async def main():
    # Initialize client
    client = PWCDataPlatform(
        base_url="https://api.pwc-data.com",
        api_key="pwc_your_api_key_here"
    )
    
    # Get sales summary
    sales_summary = await client.sales.get_summary(
        date_range="2025-08-01:2025-08-30",
        group_by=["country", "product_category"]
    )
    
    print(f"Total Revenue: ${sales_summary.total_revenue:,.2f}")
    
    # Real-time analytics
    async for update in client.analytics.subscribe_realtime(
        metrics=["revenue_rate", "transaction_count"],
        interval="30s"
    ):
        print(f"Revenue Rate: ${update.revenue_rate}/min")

asyncio.run(main())
```

### JavaScript/TypeScript SDK
```typescript
import { PWCApiClient } from '@pwc/data-platform-sdk';

const client = new PWCApiClient({
  baseUrl: 'https://api.pwc-data.com',
  apiKey: 'pwc_your_api_key_here'
});

// Get sales summary
const salesSummary = await client.sales.getSummary({
  dateRange: '2025-08-01:2025-08-30',
  groupBy: ['country', 'product_category']
});

console.log(`Total Revenue: $${salesSummary.totalRevenue.toLocaleString()}`);

// WebSocket connection
const ws = client.analytics.createRealtimeConnection();
ws.subscribe(['sales_metrics'], (data) => {
  console.log('Real-time metrics:', data);
});
```

### cURL Examples
```bash
# Get sales summary
curl -X GET "https://api.pwc-data.com/api/v1/sales/summary?date_range=2025-08-01:2025-08-30" \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -H "Content-Type: application/json"

# Advanced analytics
curl -X POST "https://api.pwc-data.com/api/v2/analytics/sales-insights" \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "date_range": {"start": "2025-08-01", "end": "2025-08-30"},
    "dimensions": ["country", "product_category"],
    "metrics": ["revenue", "transaction_count"]
  }'
```

## 📮 Postman Collection

Download our comprehensive Postman collection:
```bash
curl -O "https://api.pwc-data.com/api/v1/docs/postman-collection.json"
```

### Environment Variables
Set these in your Postman environment:
- `base_url`: https://api.pwc-data.com
- `jwt_token`: Your JWT token
- `api_key`: Your API key

### Pre-request Scripts
```javascript
// Auto-refresh JWT token
if (pm.environment.get("jwt_expires_at") && 
    new Date() > new Date(pm.environment.get("jwt_expires_at"))) {
    
    const loginRequest = {
        url: pm.environment.get("base_url") + "/api/v1/auth/refresh",
        method: 'POST',
        header: {
            'Authorization': 'Bearer ' + pm.environment.get("refresh_token")
        }
    };
    
    pm.sendRequest(loginRequest, function(err, response) {
        if (!err) {
            const data = response.json();
            pm.environment.set("jwt_token", data.access_token);
            pm.environment.set("jwt_expires_at", 
                new Date(Date.now() + data.expires_in * 1000).toISOString());
        }
    });
}
```

## 🔍 Testing & Validation

### API Contract Testing
```python
import pytest
import requests

class TestAPiContract:
    def test_sales_summary_response_schema(self):
        response = requests.get(
            "https://api.pwc-data.com/api/v1/sales/summary",
            headers={"Authorization": f"Bearer {JWT_TOKEN}"}
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Validate response schema
        assert "summary" in data
        assert "total_revenue" in data["summary"]
        assert isinstance(data["summary"]["total_revenue"], (int, float))
```

### Load Testing
```python
import asyncio
import aiohttp
import time

async def load_test_api():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(100):
            task = session.get(
                "https://api.pwc-data.com/api/v1/sales/summary",
                headers={"Authorization": f"Bearer {JWT_TOKEN}"}
            )
            tasks.append(task)
        
        start_time = time.time()
        responses = await asyncio.gather(*tasks)
        end_time = time.time()
        
        success_count = sum(1 for r in responses if r.status == 200)
        print(f"Success rate: {success_count/len(responses)*100}%")
        print(f"Total time: {end_time - start_time:.2f}s")
```

---

## 📚 Additional Resources

- **[OpenAPI Specification](./OPENAPI_SPECIFICATION.yaml)** - Complete API schema
- **[GraphQL Schema](./GRAPHQL_SCHEMA.graphql)** - GraphQL type definitions  
- **[Webhook Documentation](./WEBHOOKS.md)** - Event-driven integrations
- **[API Changelog](./CHANGELOG.md)** - Version history and changes
- **[Performance Guide](./PERFORMANCE_OPTIMIZATION.md)** - Optimization best practices
- **[Security Guide](./SECURITY_BEST_PRACTICES.md)** - Security implementation

**Last Updated**: August 31, 2025  
**API Version**: v4.0.0  
**Documentation Version**: 1.0.0