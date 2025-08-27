# Complete API Reference Guide

## Overview
This guide provides comprehensive API documentation for the PwC Enterprise Data Engineering Platform, covering all endpoints, authentication methods, and practical usage examples.

## Table of Contents
1. [Authentication & Security](#authentication--security)
2. [Core Business APIs](#core-business-apis)
3. [ML Analytics APIs](#ml-analytics-apis)
4. [Streaming & Real-time APIs](#streaming--real-time-apis)
5. [Enterprise Features](#enterprise-features)
6. [GraphQL API](#graphql-api)
7. [WebSocket APIs](#websocket-apis)
8. [Error Handling](#error-handling)
9. [Rate Limiting](#rate-limiting)
10. [SDK Integration](#sdk-integration)

## Authentication & Security

### JWT Authentication
Primary authentication method with enhanced security features.

```http
POST /api/v1/auth/login
Content-Type: application/json

{
  "username": "user@example.com",
  "password": "secure_password",
  "mfa_code": "123456"
}
```

Response:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IlJlZnJlc2gifQ...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "user_info": {
    "user_id": "12345",
    "username": "user@example.com",
    "roles": ["data_analyst", "viewer"],
    "permissions": ["perm_sales_read", "perm_analytics_read"],
    "clearance_level": "standard",
    "mfa_verified": true
  }
}
```

### API Key Authentication
For system-to-system integration.

```http
GET /api/v1/sales
Authorization: Bearer pwc_api_key_123456789
```

### OAuth2/OIDC Integration
Enterprise SSO integration.

```http
GET /api/v1/auth/oauth/authorize
  ?client_id=your_client_id
  &response_type=code
  &scope=read:sales write:analytics
  &redirect_uri=https://your-app.com/callback
```

## Core Business APIs

### Sales Data API

#### Get Sales Data
Retrieve paginated sales data with filtering and sorting.

```http
GET /api/v1/sales?date_from=2024-01-01&date_to=2024-12-31&country=UK&page=1&size=20&sort=invoice_date:desc
Authorization: Bearer <token>
```

Response:
```json
{
  "items": [
    {
      "invoice_no": "536365",
      "stock_code": "85123A",
      "description": "WHITE HANGING HEART T-LIGHT HOLDER",
      "quantity": 6,
      "invoice_date": "2024-01-01T08:26:00",
      "unit_price": 2.55,
      "customer_id": "17850",
      "country": "United Kingdom",
      "total_amount": 15.30
    }
  ],
  "total": 541909,
  "page": 1,
  "size": 20,
  "pages": 27096
}
```

#### Sales Analytics
Get aggregated sales analytics with advanced metrics.

```http
GET /api/v1/sales/analytics
Authorization: Bearer <token>
```

Response:
```json
{
  "time_period": {
    "start": "2024-01-01",
    "end": "2024-12-31"
  },
  "summary": {
    "total_sales": 9747748.86,
    "total_orders": 541909,
    "unique_customers": 4372,
    "avg_order_value": 17.99,
    "top_countries": [
      {"country": "United Kingdom", "sales": 8187806.36, "percentage": 84.0},
      {"country": "Germany", "sales": 221698.21, "percentage": 2.3}
    ]
  },
  "trends": {
    "monthly_growth": 15.2,
    "seasonal_patterns": {
      "peak_months": ["November", "December"],
      "lowest_months": ["January", "February"]
    }
  }
}
```

### DataMart API

#### Execute DataMart Queries
Run predefined analytical queries against data marts.

```http
POST /api/v1/datamart/query
Authorization: Bearer <token>
Content-Type: application/json

{
  "query_name": "customer_rfm_analysis",
  "parameters": {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "customer_segments": ["high_value", "frequent_buyer"]
  }
}
```

Response:
```json
{
  "query_id": "qry_123456789",
  "status": "completed",
  "results": {
    "columns": ["customer_id", "rfm_score", "segment", "ltv"],
    "data": [
      ["12345", 555, "Champions", 2450.50],
      ["12346", 344, "Loyal Customers", 1890.25]
    ],
    "metadata": {
      "total_rows": 4372,
      "execution_time_ms": 1250,
      "cache_hit": true
    }
  }
}
```

## ML Analytics APIs

### Predictive Analytics

#### Sales Forecasting
Generate sales forecasts using ML models.

```http
POST /api/v1/ml/forecast
Authorization: Bearer <token>
Content-Type: application/json

{
  "forecast_type": "sales",
  "time_horizon": 30,
  "aggregation_level": "daily",
  "models": ["prophet", "arima", "xgboost"],
  "filters": {
    "product_category": "electronics",
    "country": "UK"
  }
}
```

Response:
```json
{
  "forecast_id": "fcst_123456789",
  "status": "completed",
  "model_performance": {
    "selected_model": "prophet",
    "accuracy_metrics": {
      "mape": 0.12,
      "mae": 150.30,
      "rmse": 220.45
    }
  },
  "forecast": [
    {
      "date": "2024-02-01",
      "predicted_value": 15420.50,
      "confidence_lower": 14200.30,
      "confidence_upper": 16640.70,
      "trend_component": 0.05
    }
  ]
}
```

#### Customer Segmentation
Perform ML-driven customer segmentation.

```http
POST /api/v1/ml/segmentation
Authorization: Bearer <token>
Content-Type: application/json

{
  "algorithm": "kmeans",
  "n_clusters": 5,
  "features": ["recency", "frequency", "monetary"],
  "time_window": "12M"
}
```

Response:
```json
{
  "segmentation_id": "seg_123456789",
  "status": "completed",
  "segments": [
    {
      "segment_id": "champions",
      "size": 892,
      "characteristics": {
        "avg_recency": 15,
        "avg_frequency": 45,
        "avg_monetary": 2450.50
      },
      "recommendations": [
        "VIP treatment",
        "Early access to new products",
        "Exclusive offers"
      ]
    }
  ]
}
```

### Model Management

#### Deploy ML Model
Deploy trained models to production.

```http
POST /api/v1/ml/models/deploy
Authorization: Bearer <token>
Content-Type: application/json

{
  "model_id": "model_123456",
  "deployment_config": {
    "environment": "production",
    "scaling": {
      "min_instances": 2,
      "max_instances": 10,
      "target_utilization": 0.7
    },
    "monitoring": {
      "enable_performance_monitoring": true,
      "enable_data_drift_detection": true,
      "alert_thresholds": {
        "accuracy_drop": 0.05,
        "latency_ms": 1000
      }
    }
  }
}
```

Response:
```json
{
  "deployment_id": "deploy_123456789",
  "status": "deploying",
  "endpoint": "https://api.pwc-data.com/api/v1/ml/models/model_123456/predict",
  "estimated_completion": "2024-01-01T10:05:00Z"
}
```

## Streaming & Real-time APIs

### Stream Processing
Manage and monitor real-time data streams.

#### Start Stream Processing Job
```http
POST /api/v1/streaming/jobs
Authorization: Bearer <token>
Content-Type: application/json

{
  "job_name": "realtime_sales_processing",
  "source": {
    "type": "kafka",
    "topic": "sales_events",
    "bootstrap_servers": ["kafka1:9092", "kafka2:9092"]
  },
  "transformations": [
    "data_validation",
    "enrichment",
    "aggregation"
  ],
  "sink": {
    "type": "delta_lake",
    "path": "s3://data-lake/silver/sales/"
  }
}
```

Response:
```json
{
  "job_id": "job_123456789",
  "status": "starting",
  "estimated_startup_time": "30s",
  "monitoring_url": "https://spark-ui.pwc-data.com/job_123456789"
}
```

#### Get Stream Metrics
```http
GET /api/v1/streaming/jobs/job_123456789/metrics
Authorization: Bearer <token>
```

Response:
```json
{
  "job_id": "job_123456789",
  "status": "running",
  "metrics": {
    "records_processed": 1250000,
    "processing_rate": "5000/sec",
    "latency_p95": "150ms",
    "error_rate": 0.001,
    "checkpoint_location": "s3://checkpoints/job_123456789",
    "last_checkpoint": "2024-01-01T10:30:00Z"
  },
  "health": {
    "overall": "healthy",
    "components": {
      "kafka_consumer": "healthy",
      "transformations": "healthy",
      "delta_writer": "healthy"
    }
  }
}
```

## Enterprise Features

### Security Management

#### Access Control
Manage user permissions and roles.

```http
POST /api/v1/security/access-control/assign
Authorization: Bearer <token>
Content-Type: application/json

{
  "user_id": "user_123",
  "roles": ["data_scientist", "analytics_user"],
  "permissions": ["perm_ml_train", "perm_analytics_advanced"],
  "clearance_level": "confidential",
  "expires_at": "2024-12-31T23:59:59Z"
}
```

#### Data Loss Prevention (DLP)
Configure DLP policies and scan data.

```http
POST /api/v1/security/dlp/scan
Authorization: Bearer <token>
Content-Type: application/json

{
  "data_source": "api_response",
  "content": "Customer John Doe with SSN 123-45-6789 purchased...",
  "scan_policies": ["pii_detection", "phi_detection", "financial_data"]
}
```

Response:
```json
{
  "scan_id": "dlp_scan_123456",
  "status": "completed",
  "findings": [
    {
      "policy": "pii_detection",
      "matches": [
        {
          "type": "ssn",
          "confidence": 0.99,
          "location": {"start": 35, "end": 46},
          "original": "123-45-6789",
          "redacted": "XXX-XX-XXXX"
        }
      ]
    }
  ],
  "redacted_content": "Customer John Doe with SSN XXX-XX-XXXX purchased...",
  "compliance_score": 0.95
}
```

### Monitoring & Observability

#### System Health
```http
GET /api/v1/monitoring/health/detailed
Authorization: Bearer <token>
```

Response:
```json
{
  "overall_status": "healthy",
  "components": {
    "api_gateway": {
      "status": "healthy",
      "response_time": "45ms",
      "success_rate": 0.999
    },
    "database": {
      "status": "healthy",
      "connection_pool": {
        "active": 15,
        "idle": 5,
        "max": 50
      }
    },
    "streaming": {
      "status": "healthy",
      "active_jobs": 12,
      "total_throughput": "50000 records/sec"
    }
  },
  "metrics": {
    "cpu_usage": 0.65,
    "memory_usage": 0.72,
    "disk_usage": 0.45
  }
}
```

## GraphQL API

The platform provides a comprehensive GraphQL API for flexible data querying.

### Endpoint
```
POST /api/graphql
Authorization: Bearer <token>
Content-Type: application/json
```

### Example Queries

#### Sales Data with Customer Information
```graphql
query GetSalesWithCustomers($limit: Int, $dateFrom: String) {
  sales(limit: $limit, dateFrom: $dateFrom) {
    invoiceNo
    invoiceDate
    totalAmount
    customer {
      customerId
      country
      totalPurchases
    }
    items {
      stockCode
      description
      quantity
      unitPrice
    }
  }
}
```

#### ML Model Performance
```graphql
query ModelPerformance($modelId: ID!) {
  mlModel(id: $modelId) {
    name
    version
    status
    metrics {
      accuracy
      precision
      recall
      f1Score
    }
    deployments {
      environment
      endpoint
      instanceCount
      avgLatency
    }
  }
}
```

## WebSocket APIs

Real-time communication for streaming data and live updates.

### Security Dashboard WebSocket
```javascript
// Connect to security dashboard
const ws = new WebSocket('wss://api.pwc-data.com/ws/security/dashboard?token=<jwt_token>');

ws.onmessage = function(event) {
  const data = JSON.parse(event.data);
  
  if (data.type === 'security_alert') {
    console.log('Security Alert:', data.payload);
  }
  
  if (data.type === 'metric_update') {
    updateDashboard(data.payload);
  }
};
```

### Streaming Analytics WebSocket
```javascript
const analyticsWs = new WebSocket('wss://api.pwc-data.com/ws/analytics/realtime');

analyticsWs.onmessage = function(event) {
  const update = JSON.parse(event.data);
  
  // Real-time sales metrics
  if (update.type === 'sales_metric') {
    updateSalesChart(update.data);
  }
  
  // Anomaly detection alerts
  if (update.type === 'anomaly_detected') {
    showAnomalyAlert(update.data);
  }
};
```

## Error Handling

All APIs follow consistent error response format:

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "The request parameters are invalid",
    "details": {
      "field": "date_from",
      "issue": "Invalid date format. Expected YYYY-MM-DD"
    },
    "request_id": "req_123456789",
    "timestamp": "2024-01-01T10:30:00Z"
  }
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `INVALID_REQUEST` | 400 | Request parameters are invalid |
| `UNAUTHORIZED` | 401 | Authentication required or invalid |
| `FORBIDDEN` | 403 | Insufficient permissions |
| `NOT_FOUND` | 404 | Resource not found |
| `RATE_LIMIT_EXCEEDED` | 429 | Too many requests |
| `INTERNAL_ERROR` | 500 | Internal server error |
| `SERVICE_UNAVAILABLE` | 503 | Service temporarily unavailable |

## Rate Limiting

API requests are subject to rate limiting based on authentication method and endpoint:

| Endpoint Pattern | Limit | Window | Burst |
|-----------------|-------|---------|-------|
| `/api/v1/auth/*` | 5 req/min | 60s | - |
| `/api/v1/sales` | 100 req/min | 60s | 150 |
| `/api/v1/ml/*` | 20 req/min | 60s | 30 |
| `/api/v1/streaming/*` | 50 req/min | 60s | 75 |
| Default | 100 req/min | 60s | 150 |

Rate limit information is included in response headers:
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1640995200
```

## SDK Integration

### Python SDK
```python
from pwc_data_platform import PWCDataClient

# Initialize client
client = PWCDataClient(
    base_url="https://api.pwc-data.com",
    api_key="your_api_key"
)

# Get sales data
sales = client.sales.list(
    date_from="2024-01-01",
    date_to="2024-12-31",
    country="UK"
)

# Run ML prediction
forecast = client.ml.forecast(
    forecast_type="sales",
    time_horizon=30,
    model="prophet"
)
```

### JavaScript SDK
```javascript
import { PWCDataClient } from '@pwc/data-platform-sdk';

const client = new PWCDataClient({
  baseURL: 'https://api.pwc-data.com',
  apiKey: 'your_api_key'
});

// Get sales analytics
const analytics = await client.sales.getAnalytics({
  dateFrom: '2024-01-01',
  dateTo: '2024-12-31'
});

// Real-time streaming connection
const stream = client.streaming.connect('sales_events');
stream.on('data', (event) => {
  console.log('New sale:', event);
});
```

## Performance Optimization

### Pagination Best Practices
- Use `size` parameter to limit results (max 1000)
- Implement cursor-based pagination for large datasets
- Cache frequently accessed data

### Filtering and Sorting
- Use specific date ranges to reduce data volume
- Apply filters early in the query pipeline
- Index commonly filtered fields

### Caching
- Implement client-side caching for static data
- Use ETags for conditional requests
- Leverage CDN for static assets

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify token hasn't expired
   - Check token format (Bearer prefix)
   - Ensure sufficient permissions

2. **Rate Limiting**
   - Implement exponential backoff
   - Use burst capacity wisely
   - Consider upgrading API plan

3. **Performance Issues**
   - Reduce page size
   - Use specific date ranges
   - Implement proper caching

### Support Resources
- API Status: https://status.pwc-data.com
- Developer Portal: https://developers.pwc-data.com
- Support: api-support@pwc.com
- Documentation: https://docs.pwc-data.com