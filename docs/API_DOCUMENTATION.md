# API Documentation

## Overview

The PwC Retail Data Platform provides a comprehensive REST API for accessing and manipulating retail analytics data. The API is built with FastAPI and follows OpenAPI 3.0 specification.

## Base URLs

- **Development**: `http://localhost:8000`
- **Staging**: `https://api-staging.pwc-retail.com`
- **Production**: `https://api.pwc-retail.com`

## Authentication

### JWT Authentication (Recommended)

The API uses JWT (JSON Web Tokens) for authentication. Include the token in the Authorization header:

```http
Authorization: Bearer <your-jwt-token>
```

#### Obtaining a JWT Token

```http
POST /api/v1/auth/login
Content-Type: application/json

{
    "username": "your_username",
    "password": "your_password"
}
```

Response:
```json
{
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 1800,
    "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

#### Refreshing Tokens

```http
POST /api/v1/auth/refresh
Content-Type: application/json
Authorization: Bearer <refresh-token>
```

### Basic Authentication (Fallback)

For development and testing, Basic Authentication is supported:

```http
Authorization: Basic <base64-encoded-username:password>
```

## Rate Limiting

The API implements intelligent rate limiting with the following default limits:

- **Authentication endpoints**: 5 requests per minute
- **Analytics endpoints**: 20 requests per minute (burst: 30)
- **General endpoints**: 100 requests per minute (burst: 150)

Rate limit headers are included in all responses:

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1640995200
```

## API Versioning

The API supports versioning through URL paths:

- **v1**: `/api/v1/` - Stable, production-ready endpoints
- **v2**: `/api/v2/` - Enhanced endpoints with advanced features

## Error Handling

### Standard HTTP Status Codes

- `200` - OK
- `201` - Created
- `400` - Bad Request
- `401` - Unauthorized
- `403` - Forbidden
- `404` - Not Found
- `422` - Validation Error
- `429` - Rate Limit Exceeded
- `500` - Internal Server Error
- `503` - Service Unavailable (Circuit Breaker)

### Error Response Format

```json
{
    "error": {
        "code": "VALIDATION_ERROR",
        "message": "Request validation failed",
        "details": {
            "field": "customer_id",
            "message": "Field is required"
        },
        "timestamp": "2024-01-15T10:30:00Z",
        "request_id": "req_abc123"
    }
}
```

## API Endpoints

### Health & Status

#### GET /health
Check API health status.

**Response:**
```json
{
    "status": "healthy",
    "environment": "production", 
    "version": "2.0.0",
    "timestamp": "2024-01-15T10:30:00Z",
    "security": {
        "auth_enabled": true,
        "rate_limiting": true,
        "https_enabled": true
    }
}
```

### Authentication Endpoints

#### POST /api/v1/auth/login
Authenticate user and obtain JWT token.

**Request:**
```json
{
    "username": "string",
    "password": "string"
}
```

**Response:**
```json
{
    "access_token": "string",
    "token_type": "bearer",
    "expires_in": 1800,
    "refresh_token": "string",
    "user": {
        "id": "string",
        "username": "string",
        "roles": ["analyst", "user"]
    }
}
```

#### POST /api/v1/auth/refresh
Refresh access token using refresh token.

**Headers:**
```http
Authorization: Bearer <refresh-token>
```

**Response:**
```json
{
    "access_token": "string",
    "token_type": "bearer", 
    "expires_in": 1800
}
```

### Sales Data Endpoints

#### GET /api/v1/sales
Retrieve sales data with filtering and pagination.

**Query Parameters:**
- `page` (integer): Page number (default: 1)
- `size` (integer): Page size (default: 100, max: 1000)
- `start_date` (string): Filter by start date (ISO 8601)
- `end_date` (string): Filter by end date (ISO 8601)
- `country` (string): Filter by country
- `customer_id` (string): Filter by customer ID
- `product_code` (string): Filter by product code

**Example Request:**
```http
GET /api/v1/sales?start_date=2024-01-01&end_date=2024-01-31&country=United%20Kingdom&page=1&size=100
Authorization: Bearer <token>
```

**Response:**
```json
{
    "items": [
        {
            "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
            "invoice": "536365",
            "customer_id": "17850",
            "product_code": "85123A",
            "description": "WHITE HANGING HEART T-LIGHT HOLDER",
            "quantity": 6,
            "unit_price": 2.55,
            "total_amount": 15.30,
            "invoice_date": "2024-01-01T08:26:00Z",
            "country": "United Kingdom"
        }
    ],
    "total": 1000,
    "page": 1,
    "size": 100,
    "pages": 10
}
```

#### GET /api/v1/sales/{transaction_id}
Get specific transaction by ID.

**Response:**
```json
{
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
    "invoice": "536365",
    "customer_id": "17850",
    "product_code": "85123A",
    "description": "WHITE HANGING HEART T-LIGHT HOLDER",
    "quantity": 6,
    "unit_price": 2.55,
    "total_amount": 15.30,
    "invoice_date": "2024-01-01T08:26:00Z",
    "country": "United Kingdom",
    "created_at": "2024-01-01T08:30:00Z",
    "updated_at": "2024-01-01T08:30:00Z"
}
```

#### GET /api/v1/sales/analytics
Get sales analytics and aggregations.

**Query Parameters:**
- `metric` (array): Metrics to calculate (revenue, transactions, customers)
- `dimension` (array): Group by dimensions (country, product, time)
- `start_date` (string): Analysis start date
- `end_date` (string): Analysis end date
- `granularity` (string): Time granularity (day, week, month)

**Response:**
```json
{
    "metrics": {
        "total_revenue": 1250000.50,
        "total_transactions": 45230,
        "unique_customers": 8945,
        "avg_order_value": 27.63
    },
    "dimensions": {
        "by_country": [
            {"country": "United Kingdom", "revenue": 650000.25},
            {"country": "Germany", "revenue": 320000.75}
        ],
        "by_time": [
            {"date": "2024-01-01", "revenue": 15000.50},
            {"date": "2024-01-02", "revenue": 18200.75}
        ]
    },
    "period": {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31"
    }
}
```

### Advanced Analytics (v2)

#### POST /api/v2/analytics/advanced-analytics
Create advanced analytics calculation with async processing.

**Request:**
```json
{
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-31T23:59:59Z",
    "metrics": ["revenue", "transactions", "customers"],
    "dimensions": ["country", "product", "time"],
    "filters": {
        "country": ["United Kingdom", "Germany"],
        "min_order_value": 10.0
    }
}
```

**Response:**
```json
{
    "request_id": "req_550e8400-e29b-41d4-a716-446655440000",
    "status": "processing",
    "data": null,
    "metadata": {
        "estimated_duration": "30-60 seconds"
    },
    "created_at": "2024-01-15T10:30:00Z"
}
```

#### GET /api/v2/analytics/analytics-status/{request_id}
Check status of analytics processing.

**Response:**
```json
{
    "request_id": "req_550e8400-e29b-41d4-a716-446655440000",
    "status": "completed",
    "data": {
        "revenue_analysis": {
            "total_revenue": 1250000.50,
            "growth_rate": 15.2,
            "top_products": ["Product A", "Product B"]
        },
        "customer_analysis": {
            "total_customers": 45230,
            "new_customers": 1250,
            "retention_rate": 87.5
        }
    },
    "metadata": {
        "processing_time": "45.2 seconds"
    },
    "created_at": "2024-01-15T10:30:00Z",
    "completed_at": "2024-01-15T10:30:45Z"
}
```

#### POST /api/v2/analytics/cohort-analysis
Perform customer cohort analysis.

**Request:**
```json
{
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-31T23:59:59Z",
    "cohort_period": "month",
    "analysis_period": "month"
}
```

**Response:**
```json
{
    "cohort_periods": ["2024-01", "2024-02", "2024-03"],
    "retention_rates": {
        "month_0": 100.0,
        "month_1": 45.2,
        "month_2": 32.1,
        "month_3": 28.5
    },
    "cohort_sizes": [1200, 1350, 1180],
    "revenue_per_cohort": [
        {"period": "2024-01", "revenue": 45000.0},
        {"period": "2024-02", "revenue": 52000.0}
    ]
}
```

#### GET /api/v2/analytics/predictive-insights
Generate predictive insights and forecasting.

**Query Parameters:**
- `forecast_days` (integer): Days to forecast (1-365, default: 30)
- `confidence_interval` (float): Confidence interval (0.8-0.99, default: 0.95)

**Response:**
```json
{
    "forecast_period": 30,
    "confidence_interval": 0.95,
    "predictions": [
        {
            "date": "2024-02-01T00:00:00Z",
            "predicted_revenue": 12500.50,
            "lower_bound": 11250.45,
            "upper_bound": 13750.55
        }
    ],
    "model_accuracy": 0.85,
    "last_updated": "2024-01-15T10:30:00Z"
}
```

#### GET /api/v2/analytics/real-time-metrics
Get real-time business metrics.

**Query Parameters:**
- `metrics` (array): Metrics to retrieve (active_users, revenue_rate, conversion_rate)

**Response:**
```json
{
    "metrics": {
        "active_users": {
            "value": 542,
            "change_24h": 8.5,
            "unit": "users"
        },
        "revenue_rate": {
            "value": 1050.75,
            "change_24h": 12.3,
            "unit": "$/hour"
        },
        "conversion_rate": {
            "value": 3.8,
            "change_24h": 0.2,
            "unit": "%"
        }
    },
    "timestamp": "2024-01-15T10:30:00Z",
    "refresh_interval": 30
}
```

### Search Endpoints

#### GET /api/v1/search
Search products and transactions.

**Query Parameters:**
- `q` (string): Search query
- `type` (string): Search type (products, transactions, customers)
- `limit` (integer): Number of results (default: 20, max: 100)
- `filters` (object): Additional filters

**Response:**
```json
{
    "results": [
        {
            "id": "85123A",
            "type": "product",
            "title": "WHITE HANGING HEART T-LIGHT HOLDER",
            "description": "Decorative heart-shaped tea light holder in white",
            "price": 2.55,
            "category": "Home Decor",
            "score": 0.95
        }
    ],
    "total": 156,
    "query": "heart holder",
    "facets": {
        "category": [
            {"value": "Home Decor", "count": 89},
            {"value": "Garden", "count": 45}
        ],
        "price_range": [
            {"range": "0-10", "count": 120},
            {"range": "10-50", "count": 36}
        ]
    }
}
```

### Customer Endpoints

#### GET /api/v1/customers/{customer_id}
Get customer profile and analytics.

**Response:**
```json
{
    "customer_id": "17850",
    "country": "United Kingdom",
    "created_date": "2023-12-01T00:00:00Z",
    "total_orders": 15,
    "total_revenue": 425.60,
    "avg_order_value": 28.37,
    "first_order_date": "2023-12-01T08:26:00Z",
    "last_order_date": "2024-01-28T14:15:00Z",
    "customer_segment": "Loyal Customers",
    "value_tier": "Medium Value",
    "customer_status": "Active",
    "rfm_scores": {
        "recency": 5,
        "frequency": 4,
        "monetary": 3
    }
}
```

#### GET /api/v1/customers/{customer_id}/orders
Get customer order history.

**Query Parameters:**
- `page` (integer): Page number
- `size` (integer): Page size
- `start_date` (string): Filter by start date
- `end_date` (string): Filter by end date

**Response:**
```json
{
    "items": [
        {
            "invoice": "536365",
            "invoice_date": "2024-01-01T08:26:00Z",
            "total_amount": 139.12,
            "item_count": 8,
            "products": [
                {
                    "product_code": "85123A",
                    "description": "WHITE HANGING HEART T-LIGHT HOLDER",
                    "quantity": 6,
                    "unit_price": 2.55,
                    "total": 15.30
                }
            ]
        }
    ],
    "total": 15,
    "page": 1,
    "size": 10
}
```

## Webhooks

### Event Types

The API supports webhooks for real-time event notifications:

- `transaction.created` - New transaction processed
- `customer.updated` - Customer profile updated
- `alert.triggered` - System alert triggered
- `data.quality_issue` - Data quality problem detected

### Webhook Configuration

```http
POST /api/v1/webhooks
Content-Type: application/json
Authorization: Bearer <token>

{
    "url": "https://your-app.com/webhooks/pwc-retail",
    "events": ["transaction.created", "alert.triggered"],
    "secret": "your-webhook-secret",
    "active": true
}
```

### Webhook Payload Example

```json
{
    "id": "evt_550e8400-e29b-41d4-a716-446655440000",
    "type": "transaction.created",
    "created": "2024-01-15T10:30:00Z",
    "data": {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": "17850",
        "total_amount": 15.30,
        "country": "United Kingdom"
    }
}
```

## SDK & Libraries

### Python SDK

```python
from pwc_retail_api import RetailAPIClient

client = RetailAPIClient(
    base_url="https://api.pwc-retail.com",
    api_key="your-api-key"
)

# Get sales data
sales = client.sales.list(
    start_date="2024-01-01",
    end_date="2024-01-31",
    country="United Kingdom"
)

# Get analytics
analytics = client.analytics.get_metrics(
    metrics=["revenue", "customers"],
    dimensions=["country", "time"]
)
```

### JavaScript SDK

```javascript
import { RetailAPIClient } from '@pwc/retail-api';

const client = new RetailAPIClient({
    baseURL: 'https://api.pwc-retail.com',
    apiKey: 'your-api-key'
});

// Get sales data
const sales = await client.sales.list({
    startDate: '2024-01-01',
    endDate: '2024-01-31',
    country: 'United Kingdom'
});

// Get analytics
const analytics = await client.analytics.getMetrics({
    metrics: ['revenue', 'customers'],
    dimensions: ['country', 'time']
});
```

## Testing

### Postman Collection

Import the Postman collection for easy API testing:

```json
{
    "info": {
        "name": "PwC Retail Data Platform API",
        "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
    },
    "auth": {
        "type": "bearer",
        "bearer": [
            {
                "key": "token",
                "value": "{{jwt_token}}",
                "type": "string"
            }
        ]
    }
}
```

### Test Environment

- **Base URL**: `https://api-test.pwc-retail.com`
- **Test Credentials**: 
  - Username: `test_user`
  - Password: `test_password_2024`

## Performance Guidelines

### Best Practices

1. **Pagination**: Always use pagination for list endpoints
2. **Filtering**: Use specific filters to reduce response size
3. **Caching**: Cache responses when appropriate
4. **Batch Operations**: Use batch endpoints for multiple operations
5. **Async Processing**: Use async endpoints for heavy computations

### Rate Limit Management

```python
import time
import requests

def api_call_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            time.sleep(retry_after)
            continue
            
        return response
    
    raise Exception("Max retries exceeded")
```

## Support & Resources

### Documentation Links

- **OpenAPI Spec**: `/api/v1/openapi.json`
- **Interactive Docs**: `/docs`
- **ReDoc**: `/redoc`

### Support Channels

- **Technical Support**: support@pwc-retail.com
- **Documentation**: https://docs.pwc-retail.com
- **Status Page**: https://status.pwc-retail.com

### Community

- **GitHub Issues**: https://github.com/pwc/retail-platform/issues
- **Stack Overflow**: Tag with `pwc-retail-api`
- **Slack Community**: #pwc-retail-developers