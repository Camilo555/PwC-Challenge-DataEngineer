# PwC Retail Data Platform - REST API Documentation

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Rate Limiting](#rate-limiting)
4. [API Versioning](#api-versioning)
5. [Error Handling](#error-handling)
6. [API Reference](#api-reference)
7. [SDK and Client Libraries](#sdk-and-client-libraries)
8. [Testing](#testing)
9. [Monitoring](#monitoring)

## Overview

The PwC Retail Data Platform provides a comprehensive RESTful API for accessing retail analytics data, managing ETL pipelines, and performing advanced analytics operations. The API is built using FastAPI and follows OpenAPI 3.0 specifications.

### Base URLs
- **Development**: `http://localhost:8000`
- **Staging**: `https://staging-api.pwc-retail-platform.com`
- **Production**: `https://api.pwc-retail-platform.com`

### API Features
- **Multi-version support**: v1 (stable) and v2 (enhanced)
- **Authentication**: JWT tokens with role-based access control
- **Rate limiting**: Intelligent rate limiting with burst protection
- **Circuit breakers**: Automatic failover for enhanced reliability
- **Comprehensive monitoring**: Full request/response tracing
- **Auto-documentation**: Interactive API documentation with Swagger UI

## Authentication

### JWT Authentication (Primary)

The API uses JWT (JSON Web Tokens) for authentication with role-based access control.

#### Authentication Flow

```mermaid
sequenceDiagram
    participant Client
    participant API as API Gateway
    participant Auth as Auth Service
    participant Vault as HashiCorp Vault
    
    Client->>API: POST /api/v1/auth/login
    API->>Auth: Validate credentials
    Auth->>Vault: Verify user data
    Vault->>Auth: User info + permissions
    Auth->>API: JWT token + refresh token
    API->>Client: 200 OK {access_token, refresh_token}
    
    Client->>API: GET /api/v1/sales (with JWT)
    API->>API: Validate JWT & check permissions
    API->>Client: 200 OK {data}
```

#### Login Endpoint

```bash
POST /api/v1/auth/login
Content-Type: application/json

{
  "username": "data_analyst@company.com",
  "password": "secure_password123"
}
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 1800,
  "user": {
    "id": "user_123",
    "username": "data_analyst@company.com",
    "roles": ["data_analyst"],
    "permissions": ["read:analytics", "read:reports"]
  }
}
```

#### Token Refresh

```bash
POST /api/v1/auth/refresh
Content-Type: application/json

{
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

#### Using JWT Tokens

Include the JWT token in the `Authorization` header:

```bash
curl -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
     https://api.pwc-retail-platform.com/api/v1/sales
```

### Basic Authentication (Fallback)

For testing and legacy system integration, Basic Authentication is supported:

```bash
curl -u "admin:password123" \
     https://api.pwc-retail-platform.com/api/v1/health
```

### Role-Based Access Control (RBAC)

#### Available Roles

| Role | Permissions | Description |
|------|-------------|-------------|
| `admin` | `*:*` | Full system access |
| `data_engineer` | `read:*`, `write:etl`, `manage:pipelines` | ETL and pipeline management |
| `data_analyst` | `read:analytics`, `read:reports`, `read:sales` | Analytics and reporting |
| `business_user` | `read:reports`, `read:dashboard` | Business reporting only |
| `api_consumer` | `read:api`, `write:api` | External system integration |

#### Permission Format

Permissions follow the format: `<action>:<resource>`

- **Actions**: `read`, `write`, `delete`, `manage`, `*`
- **Resources**: `analytics`, `sales`, `reports`, `etl`, `pipelines`, `*`

## Rate Limiting

### Rate Limiting Strategy

The API implements intelligent rate limiting using a sliding window algorithm with Redis backend:

#### Rate Limits by Endpoint

| Endpoint Pattern | Limit | Window | Burst Limit | Description |
|------------------|-------|--------|-------------|-------------|
| `/api/v1/auth/*` | 5 req/min | 60s | 8 | Authentication endpoints |
| `/api/v2/analytics/*` | 20 req/min | 60s | 30 | Advanced analytics |
| `/api/*/search` | 50 req/min | 60s | 75 | Search endpoints |
| `/api/*/sales` | 100 req/min | 60s | 150 | Sales data endpoints |
| Default | 100 req/min | 60s | 150 | All other endpoints |

#### Rate Limit Headers

The API returns rate limiting information in response headers:

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1640995200
X-RateLimit-Window: 60
X-RateLimit-Burst: 150
```

#### Rate Limit Exceeded Response

```json
{
  "error": "rate_limit_exceeded",
  "message": "Rate limit exceeded. Try again in 45 seconds.",
  "details": {
    "limit": 100,
    "window": 60,
    "reset_at": "2024-08-25T14:30:00Z"
  }
}
```

### Circuit Breaker

The API implements circuit breaker pattern for enhanced reliability:

#### Circuit Breaker Configuration

```python
circuit_breakers = {
    "GET:/api/v1/sales/analytics": {
        "failure_threshold": 3,
        "timeout_seconds": 30,
        "recovery_timeout": 60
    },
    "POST:/api/v2/analytics/advanced": {
        "failure_threshold": 5,
        "timeout_seconds": 60,
        "recovery_timeout": 120
    }
}
```

## API Versioning

The API supports multiple versions to ensure backward compatibility while enabling new features.

### Version Strategy

- **v1**: Stable API for production use
- **v2**: Enhanced API with new features and improvements
- **Deprecation**: 12-month notice before removing versions

### Version Headers

Include version in the URL path:
```bash
# v1 API
GET /api/v1/sales

# v2 API  
GET /api/v2/sales
```

## Error Handling

### Standard Error Response Format

All API errors follow a consistent format:

```json
{
  "error": "error_code",
  "message": "Human-readable error description",
  "details": {
    "field": "Additional error context",
    "validation_errors": []
  },
  "timestamp": "2024-08-25T14:30:00Z",
  "request_id": "req_123456789"
}
```

### HTTP Status Codes

| Code | Meaning | Usage |
|------|---------|-------|
| 200 | OK | Successful request |
| 201 | Created | Resource created successfully |
| 204 | No Content | Successful request with no response body |
| 400 | Bad Request | Invalid request parameters |
| 401 | Unauthorized | Authentication required |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource not found |
| 422 | Unprocessable Entity | Validation errors |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Server Error | Server error |
| 503 | Service Unavailable | Circuit breaker open |

### Common Error Examples

#### Validation Error (422)
```json
{
  "error": "validation_error",
  "message": "Request validation failed",
  "details": {
    "validation_errors": [
      {
        "field": "start_date",
        "message": "Date must be in YYYY-MM-DD format",
        "value": "2024/08/25"
      }
    ]
  },
  "timestamp": "2024-08-25T14:30:00Z",
  "request_id": "req_123456789"
}
```

#### Authentication Error (401)
```json
{
  "error": "authentication_required",
  "message": "Valid JWT token required",
  "details": {
    "hint": "Include 'Authorization: Bearer <token>' header"
  },
  "timestamp": "2024-08-25T14:30:00Z",
  "request_id": "req_123456789"
}
```

## API Reference

### Sales Data API (v1)

#### Get Sales Data

Retrieve sales transactions with filtering and pagination.

```bash
GET /api/v1/sales
```

**Query Parameters:**
- `limit` (integer, optional): Number of records to return (default: 100, max: 1000)
- `offset` (integer, optional): Number of records to skip (default: 0)
- `start_date` (string, optional): Start date in YYYY-MM-DD format
- `end_date` (string, optional): End date in YYYY-MM-DD format
- `country` (string, optional): Filter by country
- `customer_id` (string, optional): Filter by customer ID

**Example Request:**
```bash
curl -H "Authorization: Bearer <token>" \
     "https://api.pwc-retail-platform.com/api/v1/sales?limit=50&start_date=2024-01-01&country=United%20Kingdom"
```

**Response:**
```json
{
  "data": [
    {
      "transaction_id": "txn_123",
      "invoice": "536365",
      "stock_code": "85123A",
      "description": "CREAM HANGING HEART T-LIGHT HOLDER",
      "quantity": 6,
      "invoice_date": "2024-01-01",
      "unit_price": 2.55,
      "customer_id": "17850",
      "country": "United Kingdom",
      "total_amount": 15.30
    }
  ],
  "pagination": {
    "total_records": 1500,
    "current_page": 1,
    "per_page": 50,
    "total_pages": 30,
    "has_next": true,
    "has_previous": false
  },
  "filters": {
    "start_date": "2024-01-01",
    "country": "United Kingdom"
  }
}
```

#### Get Sales Analytics

Retrieve aggregated sales analytics.

```bash
GET /api/v1/sales/analytics
```

**Query Parameters:**
- `group_by` (string): Grouping dimension (day, week, month, country, product)
- `metric` (string): Metric to calculate (revenue, quantity, transactions, customers)
- `start_date` (string, optional): Start date for analysis
- `end_date` (string, optional): End date for analysis

**Example Request:**
```bash
curl -H "Authorization: Bearer <token>" \
     "https://api.pwc-retail-platform.com/api/v1/sales/analytics?group_by=month&metric=revenue&start_date=2024-01-01"
```

**Response:**
```json
{
  "analytics": [
    {
      "period": "2024-01",
      "revenue": 125450.75,
      "transactions": 1250,
      "customers": 890,
      "avg_order_value": 100.36
    },
    {
      "period": "2024-02", 
      "revenue": 134567.89,
      "transactions": 1356,
      "customers": 945,
      "avg_order_value": 99.25
    }
  ],
  "summary": {
    "total_revenue": 259018.64,
    "total_transactions": 2606,
    "unique_customers": 1650,
    "period_start": "2024-01-01",
    "period_end": "2024-02-29"
  }
}
```

### Enhanced Sales API (v2)

#### Advanced Sales Analytics

Enhanced sales analytics with predictive insights and advanced segmentation.

```bash
GET /api/v2/sales/advanced-analytics
```

**Query Parameters:**
- `analysis_type` (string): Type of analysis (cohort, rfm, clv, churn_prediction)
- `segment` (string, optional): Customer segment filter
- `include_predictions` (boolean, optional): Include ML predictions (default: false)

**Example Request:**
```bash
curl -H "Authorization: Bearer <token>" \
     "https://api.pwc-retail-platform.com/api/v2/sales/advanced-analytics?analysis_type=cohort&include_predictions=true"
```

**Response:**
```json
{
  "analysis_type": "cohort",
  "cohorts": [
    {
      "cohort_month": "2024-01",
      "customers": 450,
      "retention_rates": {
        "month_0": 100.0,
        "month_1": 32.4,
        "month_2": 18.7,
        "month_3": 12.1
      },
      "revenue_per_cohort": {
        "month_0": 45678.90,
        "month_1": 8932.45,
        "month_2": 4567.23,
        "month_3": 2134.56
      }
    }
  ],
  "predictions": {
    "next_month_retention": 15.2,
    "confidence_interval": [12.8, 17.6],
    "model_accuracy": 0.89
  },
  "insights": [
    "Retention drops significantly after month 1",
    "High-value customers show 23% better retention",
    "Seasonal products drive repeat purchases"
  ]
}
```

### Search API

#### Full-Text Search

Search products and transactions using Elasticsearch.

```bash
GET /api/v1/search/products
```

**Query Parameters:**
- `q` (string): Search query
- `category` (string, optional): Product category filter
- `price_min` (float, optional): Minimum price filter
- `price_max` (float, optional): Maximum price filter
- `limit` (integer, optional): Number of results (default: 20, max: 100)

**Example Request:**
```bash
curl -H "Authorization: Bearer <token>" \
     "https://api.pwc-retail-platform.com/api/v1/search/products?q=hanging%20heart&category=decoration&limit=10"
```

**Response:**
```json
{
  "results": [
    {
      "stock_code": "85123A",
      "description": "CREAM HANGING HEART T-LIGHT HOLDER",
      "category": "decoration",
      "price": 2.55,
      "popularity_score": 0.89,
      "search_score": 1.23,
      "highlighted_description": "CREAM <em>HANGING HEART</em> T-LIGHT HOLDER"
    }
  ],
  "facets": {
    "categories": {
      "decoration": 156,
      "lighting": 45,
      "gift": 23
    },
    "price_ranges": {
      "0-10": 89,
      "10-25": 67,
      "25-50": 23
    }
  },
  "total_results": 224,
  "search_time_ms": 12
}
```

#### Vector Search

Semantic search using vector embeddings with Typesense.

```bash
GET /api/v1/search/semantic
```

**Query Parameters:**
- `q` (string): Natural language query
- `k` (integer, optional): Number of similar results (default: 10, max: 50)
- `include_metadata` (boolean, optional): Include embedding metadata

**Example Request:**
```bash
curl -H "Authorization: Bearer <token>" \
     "https://api.pwc-retail-platform.com/api/v1/search/semantic?q=romantic%20lighting%20for%20home&k=5"
```

**Response:**
```json
{
  "results": [
    {
      "stock_code": "85123A",
      "description": "CREAM HANGING HEART T-LIGHT HOLDER",
      "similarity_score": 0.94,
      "category": "decoration",
      "price": 2.55
    },
    {
      "stock_code": "84406B", 
      "description": "CREAM CUPID HEARTS COAT HANGER",
      "similarity_score": 0.87,
      "category": "decoration",
      "price": 3.25
    }
  ],
  "query_embedding_time_ms": 45,
  "search_time_ms": 8,
  "total_results": 5
}
```

### ETL Management API

#### Pipeline Status

Get status of ETL pipelines.

```bash
GET /api/v1/etl/pipelines
```

**Response:**
```json
{
  "pipelines": [
    {
      "pipeline_id": "retail_bronze_ingestion",
      "name": "Bronze Layer Data Ingestion",
      "status": "running",
      "last_run": "2024-08-25T14:25:00Z",
      "success_rate": 98.5,
      "avg_duration_minutes": 12,
      "next_scheduled_run": "2024-08-25T15:00:00Z"
    },
    {
      "pipeline_id": "silver_data_quality",
      "name": "Silver Layer Data Quality",
      "status": "completed",
      "last_run": "2024-08-25T14:20:00Z",
      "success_rate": 99.2,
      "avg_duration_minutes": 8,
      "records_processed": 50000
    }
  ],
  "overall_health": "healthy",
  "total_pipelines": 12,
  "active_pipelines": 8
}
```

#### Trigger Pipeline

Manually trigger an ETL pipeline.

```bash
POST /api/v1/etl/pipelines/{pipeline_id}/trigger
```

**Request Body:**
```json
{
  "parameters": {
    "force_full_refresh": false,
    "date_range": {
      "start": "2024-08-20",
      "end": "2024-08-25"
    }
  },
  "priority": "high"
}
```

**Response:**
```json
{
  "job_id": "job_789123",
  "pipeline_id": "retail_bronze_ingestion",
  "status": "queued",
  "estimated_duration_minutes": 15,
  "position_in_queue": 2,
  "triggered_by": "user_123",
  "triggered_at": "2024-08-25T14:30:00Z"
}
```

### Analytics API (v2)

#### Customer Segmentation

Advanced customer segmentation using RFM analysis and ML clustering.

```bash
GET /api/v2/analytics/customer-segmentation
```

**Query Parameters:**
- `method` (string): Segmentation method (rfm, clustering, behavioral)
- `segments` (integer, optional): Number of segments (default: 5)
- `include_characteristics` (boolean, optional): Include segment characteristics

**Example Request:**
```bash
curl -H "Authorization: Bearer <token>" \
     "https://api.pwc-retail-platform.com/api/v2/analytics/customer-segmentation?method=rfm&segments=5&include_characteristics=true"
```

**Response:**
```json
{
  "segmentation_method": "rfm",
  "segments": [
    {
      "segment_id": "champions",
      "name": "Champions",
      "customer_count": 1250,
      "percentage": 8.3,
      "characteristics": {
        "avg_recency_days": 15,
        "avg_frequency": 12.5,
        "avg_monetary_value": 875.50,
        "description": "Best customers who buy frequently and recently"
      },
      "recommended_actions": [
        "Reward loyalty",
        "Ask for reviews", 
        "Upsell premium products"
      ]
    },
    {
      "segment_id": "loyal_customers",
      "name": "Loyal Customers",
      "customer_count": 2890,
      "percentage": 19.3,
      "characteristics": {
        "avg_recency_days": 25,
        "avg_frequency": 8.2,
        "avg_monetary_value": 542.30
      }
    }
  ],
  "analysis_date": "2024-08-25",
  "total_customers_analyzed": 15000,
  "model_performance": {
    "silhouette_score": 0.72,
    "explained_variance": 0.89
  }
}
```

#### Predictive Analytics

Machine learning predictions for business metrics.

```bash
GET /api/v2/analytics/predictions
```

**Query Parameters:**
- `prediction_type` (string): Type of prediction (revenue, churn, demand, clv)
- `horizon_days` (integer): Prediction horizon in days
- `confidence_level` (float, optional): Confidence level (default: 0.95)

**Example Request:**
```bash
curl -H "Authorization: Bearer <token>" \
     "https://api.pwc-retail-platform.com/api/v2/analytics/predictions?prediction_type=revenue&horizon_days=30"
```

**Response:**
```json
{
  "prediction_type": "revenue",
  "horizon_days": 30,
  "predictions": [
    {
      "date": "2024-08-26",
      "predicted_revenue": 12567.89,
      "confidence_interval": [11200.50, 13935.28],
      "prediction_factors": [
        "Seasonal trend: +8%",
        "Marketing campaign: +12%",
        "Economic factors: -3%"
      ]
    }
  ],
  "model_info": {
    "algorithm": "XGBoost Regression",
    "training_period": "2023-08-01 to 2024-08-25",
    "accuracy_metrics": {
      "mae": 856.23,
      "rmse": 1234.56,
      "r2_score": 0.87
    },
    "feature_importance": {
      "seasonality": 0.35,
      "marketing_spend": 0.28,
      "customer_acquisition": 0.20,
      "external_factors": 0.17
    }
  }
}
```

## SDK and Client Libraries

### Python SDK

```python
from pwc_retail_sdk import RetailDataClient

# Initialize client
client = RetailDataClient(
    base_url="https://api.pwc-retail-platform.com",
    token="your_jwt_token"
)

# Get sales data
sales_data = client.sales.get(
    start_date="2024-01-01",
    end_date="2024-08-25",
    country="United Kingdom"
)

# Get analytics
analytics = client.analytics.get_cohort_analysis(
    include_predictions=True
)

# Search products
products = client.search.products("hanging heart decoration")
```

### JavaScript SDK

```javascript
import { RetailDataClient } from '@pwc/retail-data-sdk';

const client = new RetailDataClient({
  baseUrl: 'https://api.pwc-retail-platform.com',
  token: 'your_jwt_token'
});

// Get sales analytics
const analytics = await client.sales.getAnalytics({
  groupBy: 'month',
  metric: 'revenue',
  startDate: '2024-01-01'
});

// Customer segmentation
const segments = await client.analytics.getCustomerSegmentation({
  method: 'rfm',
  segments: 5
});
```

## Testing

### API Testing Environment

Test your integration using our sandbox environment:

- **Sandbox URL**: `https://sandbox-api.pwc-retail-platform.com`
- **Test Credentials**: Contact support for test account access
- **Rate Limits**: Relaxed limits for testing
- **Mock Data**: Synthetic data that mirrors production structure

### Postman Collection

Download our comprehensive Postman collection:

```bash
curl -o pwc-retail-api.postman_collection.json \
     https://api.pwc-retail-platform.com/docs/postman-collection
```

The collection includes:
- Pre-configured authentication
- Sample requests for all endpoints
- Environment variables for different stages
- Automated tests for response validation

### Test Data Generation

Generate test data for your integration:

```bash
POST /api/v1/test-data/generate
Content-Type: application/json
Authorization: Bearer <test_token>

{
  "dataset_size": 1000,
  "date_range": {
    "start": "2024-01-01",
    "end": "2024-08-25"
  },
  "include_anomalies": true
}
```

## Monitoring

### API Health Monitoring

Monitor API health and performance:

```bash
GET /api/v1/health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "2.0.0",
  "environment": "production",
  "timestamp": "2024-08-25T14:30:00Z",
  "services": {
    "database": "healthy",
    "elasticsearch": "healthy", 
    "redis": "healthy",
    "external_apis": "healthy"
  },
  "performance": {
    "avg_response_time_ms": 120,
    "requests_per_minute": 850,
    "error_rate_percent": 0.02
  }
}
```

### Performance Metrics

Key performance indicators:

| Metric | Target | Current |
|--------|--------|---------|
| Average Response Time | < 200ms | 120ms |
| 95th Percentile Response Time | < 500ms | 380ms |
| Uptime | > 99.9% | 99.97% |
| Error Rate | < 0.1% | 0.02% |
| Throughput | 1000 req/min | 850 req/min |

### Status Page

Real-time system status and incident reports:
- **Status Page**: `https://status.pwc-retail-platform.com`
- **Incident History**: Historical incident reports and resolutions
- **Maintenance Windows**: Scheduled maintenance notifications
- **Performance Metrics**: Real-time performance dashboards

---

## Support and Resources

### Documentation
- **Interactive API Docs**: `/docs` (Swagger UI)
- **Alternative Docs**: `/redoc` (ReDoc)
- **OpenAPI Spec**: `/openapi.json`

### Support Channels
- **Email**: api-support@pwc-retail-platform.com
- **Slack**: #api-support (for enterprise customers)
- **GitHub Issues**: For SDK and documentation issues

### Rate Limits and Quotas
- **Free Tier**: 1,000 requests/hour
- **Professional**: 10,000 requests/hour
- **Enterprise**: Custom limits based on agreement

### Best Practices
1. **Implement exponential backoff** for retries
2. **Use connection pooling** for high-throughput applications
3. **Cache responses** when appropriate to reduce API calls
4. **Monitor your usage** to avoid hitting rate limits
5. **Handle errors gracefully** with proper error handling
6. **Use webhooks** for real-time updates instead of polling

### Changelog
- **v2.0.0**: Enhanced analytics, ML predictions, improved performance
- **v1.5.0**: Vector search, advanced segmentation
- **v1.0.0**: Initial release with core sales and analytics APIs

For detailed changelog and migration guides, visit our [changelog documentation](https://docs.pwc-retail-platform.com/changelog).