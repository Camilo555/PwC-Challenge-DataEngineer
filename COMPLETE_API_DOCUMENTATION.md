# Complete API Documentation

## Table of Contents
1. [API Overview](#api-overview)
2. [Authentication & Authorization](#authentication--authorization)
3. [Core API Endpoints](#core-api-endpoints)
4. [ML Analytics API](#ml-analytics-api)
5. [Streaming & Real-time APIs](#streaming--real-time-apis)
6. [Security APIs](#security-apis)
7. [Administration APIs](#administration-apis)
8. [WebSocket APIs](#websocket-apis)
9. [Error Handling](#error-handling)
10. [Rate Limiting](#rate-limiting)
11. [API Testing](#api-testing)

## API Overview

### Base Information
- **Base URL**: `https://api.pwc-challenge.com`
- **API Version**: v3.0.0
- **Protocol**: HTTPS only
- **Authentication**: JWT, API Key, OAuth2/OIDC
- **Data Format**: JSON
- **Rate Limiting**: Implemented
- **Documentation**: OpenAPI 3.0

### Architecture
The API follows enterprise-grade patterns including:
- RESTful design principles
- Circuit breaker patterns
- Rate limiting and bulkhead isolation
- Comprehensive security middleware
- Real-time capabilities via WebSockets
- GraphQL support for complex queries

## Authentication & Authorization

### Supported Authentication Methods

#### 1. JWT Authentication
**Endpoint**: `POST /api/v1/auth/login`

**Request**:
```json
{
  "username": "user@example.com",
  "password": "secure_password",
  "mfa_code": "123456"
}
```

**Response**:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "user_info": {
    "id": "user123",
    "username": "user@example.com",
    "roles": ["analyst", "viewer"],
    "permissions": ["perm_read_data", "perm_run_analytics"],
    "clearance_level": "confidential",
    "mfa_enabled": true
  }
}
```

#### 2. API Key Authentication
**Header**: `Authorization: Bearer pwc_api_key_here`

**Key Management**:
```bash
# Generate API key
POST /api/v1/auth/api-keys
{
  "name": "Analytics Service",
  "permissions": ["perm_read_data", "perm_ml_predict"],
  "expires_at": "2024-12-31T23:59:59Z"
}
```

#### 3. OAuth2/OIDC Integration
**Authorization URL**: `/api/v1/auth/oauth/authorize`
**Token Exchange**: `/api/v1/auth/oauth/token`

### Authorization Levels
- **Public**: No authentication required
- **Authenticated**: Valid token required
- **Role-based**: Specific roles required
- **Permission-based**: Granular permissions required
- **Clearance-based**: Security clearance levels

## Core API Endpoints

### Health & Status
#### GET /health
**Description**: System health check with security status

**Response**:
```json
{
  "status": "healthy",
  "environment": "production",
  "version": "3.0.0",
  "timestamp": "2023-12-01T12:00:00Z",
  "security": {
    "platform_status": "operational",
    "security_level": "high",
    "dlp_enabled": true,
    "compliance_monitoring": true,
    "auth_enabled": true
  },
  "metrics": {
    "active_connections": 150,
    "security_events_24h": 5,
    "compliance_score": 0.98
  }
}
```

### Sales Data Management

#### GET /api/v1/sales
**Description**: Retrieve sales data with filtering and pagination

**Parameters**:
- `page`: Page number (default: 1)
- `page_size`: Items per page (default: 20, max: 100)
- `start_date`: Filter by start date (ISO 8601)
- `end_date`: Filter by end date (ISO 8601)
- `customer_id`: Filter by customer ID
- `product_id`: Filter by product ID

**Response**:
```json
{
  "data": [
    {
      "id": "sale_123",
      "invoice_no": "INV-2023-001",
      "customer_id": "CUST-456",
      "product_id": "PROD-789",
      "quantity": 5,
      "unit_price": 12.99,
      "total_amount": 64.95,
      "sale_date": "2023-12-01T10:30:00Z",
      "country": "United Kingdom"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_count": 150,
    "total_pages": 8
  }
}
```

#### POST /api/v1/sales
**Description**: Create new sales record

**Request**:
```json
{
  "invoice_no": "INV-2023-002",
  "customer_id": "CUST-456",
  "product_id": "PROD-789",
  "quantity": 3,
  "unit_price": 15.99,
  "sale_date": "2023-12-01T14:30:00Z"
}
```

#### PUT /api/v1/sales/{sale_id}
**Description**: Update existing sales record
**Authentication**: Required
**Permissions**: `perm_modify_sales`

#### DELETE /api/v1/sales/{sale_id}
**Description**: Delete sales record
**Authentication**: Required
**Permissions**: `perm_delete_sales`

### Search & Discovery

#### GET /api/v1/search
**Description**: Full-text search across all data with Elasticsearch integration

**Parameters**:
- `q`: Search query (required)
- `filters`: JSON filters object
- `facets`: Comma-separated facet fields
- `page`: Page number
- `page_size`: Results per page

**Example Request**:
```
GET /api/v1/search?q=electronics&filters={"country":"UK"}&facets=category,brand&page=1&page_size=10
```

**Response**:
```json
{
  "results": [
    {
      "id": "PROD-123",
      "title": "Electronics Device",
      "description": "High-quality electronic device",
      "category": "Electronics",
      "price": 199.99,
      "score": 0.95,
      "highlights": {
        "title": ["<em>Electronics</em> Device"],
        "description": ["High-quality <em>electronic</em> device"]
      }
    }
  ],
  "facets": {
    "category": [
      {"value": "Electronics", "count": 45},
      {"value": "Appliances", "count": 12}
    ]
  },
  "total_results": 57,
  "query_time_ms": 15
}
```

## ML Analytics API

### Model Predictions

#### POST /api/v1/ml-analytics/predict
**Description**: Make single predictions using ML models

**Request**:
```json
{
  "model_type": "sales_forecast",
  "data": {
    "historical_sales": [1000, 1200, 1100],
    "seasonal_factors": [1.1, 1.0, 0.9],
    "external_factors": {
      "holiday": false,
      "promotion": true
    }
  },
  "include_confidence": true,
  "include_explanation": true
}
```

**Response**:
```json
{
  "prediction_id": "pred_20231201_123456",
  "prediction_type": "sales_forecast",
  "result": {
    "values": [1150, 1300, 1250],
    "confidence": 0.87
  },
  "explanation": {
    "feature_importance": {
      "historical_sales": 0.65,
      "seasonal_factors": 0.25,
      "promotion": 0.10
    }
  },
  "metadata": {
    "model_used": "sales_forecaster_v2.1",
    "timestamp": "2023-12-01T12:00:00Z",
    "processing_time_ms": 145
  }
}
```

#### POST /api/v1/ml-analytics/predict/batch
**Description**: Process batch predictions asynchronously

**Request**:
```json
{
  "model_type": "customer_segmentation",
  "data": [
    {"customer_id": "C001", "recency": 30, "frequency": 5, "monetary": 1500},
    {"customer_id": "C002", "recency": 15, "frequency": 12, "monetary": 3200}
  ],
  "callback_url": "https://your-app.com/callback/predictions"
}
```

**Response**:
```json
{
  "batch_id": "batch_20231201_123456",
  "status": "processing",
  "estimated_completion": "2023-12-01T12:05:00Z",
  "records_count": 1000
}
```

### Model Training & Management

#### POST /api/v1/ml-analytics/train
**Description**: Train new ML models

**Request**:
```json
{
  "model_name": "sales_forecaster_v3",
  "model_type": "time_series",
  "training_data": {
    "source": "sales_data_2023",
    "features": ["historical_sales", "seasonal_patterns"],
    "target": "next_month_sales"
  },
  "parameters": {
    "algorithm": "arima",
    "seasonality": "monthly",
    "validation_split": 0.2
  }
}
```

#### GET /api/v1/ml-analytics/models
**Description**: List available models with pagination

**Response**:
```json
{
  "models": [
    {
      "id": "model_123",
      "name": "sales_forecaster_v2.1",
      "type": "time_series",
      "status": "active",
      "accuracy": 0.89,
      "created_at": "2023-11-01T10:00:00Z",
      "last_trained": "2023-11-15T08:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "total_count": 15,
    "total_pages": 2
  }
}
```

### Feature Store Operations

#### POST /api/v1/ml-analytics/features/retrieve
**Description**: Retrieve features from feature store

**Request**:
```json
{
  "feature_group": "customer_features",
  "entity_keys": {
    "customer_id": "CUST-123"
  },
  "features": ["recency", "frequency", "monetary"],
  "as_of_date": "2023-12-01T00:00:00Z"
}
```

**Response**:
```json
{
  "feature_group": "customer_features",
  "entity_keys": {
    "customer_id": "CUST-123"
  },
  "features": {
    "recency": 15.5,
    "frequency": 8,
    "monetary": 2450.75
  },
  "retrieved_at": "2023-12-01T12:00:00Z",
  "feature_versions": {
    "recency": "v1.2",
    "frequency": "v1.1",
    "monetary": "v1.0"
  }
}
```

### Model Monitoring

#### GET /api/v1/ml-analytics/metrics/model/{model_id}
**Description**: Get comprehensive model metrics

**Parameters**:
- `start_date`: Start date for metrics
- `end_date`: End date for metrics
- `metric_types`: Types of metrics (performance, drift, usage)

**Response**:
```json
{
  "model_id": "model_123",
  "date_range": {
    "start": "2023-11-01T00:00:00Z",
    "end": "2023-12-01T00:00:00Z"
  },
  "performance": {
    "accuracy": 0.89,
    "precision": 0.87,
    "recall": 0.91,
    "f1_score": 0.89,
    "trend": "stable"
  },
  "drift_metrics": {
    "feature_drift_score": 0.03,
    "prediction_drift_score": 0.02,
    "drift_detected": false,
    "drift_trend": "stable"
  },
  "usage_stats": {
    "total_predictions": 15420,
    "avg_latency_ms": 125,
    "error_rate": 0.002
  }
}
```

## Streaming & Real-time APIs

### Streaming Data Processing

#### GET /api/v1/streaming/status
**Description**: Get streaming platform status

**Response**:
```json
{
  "platform_name": "PwC-Enterprise-Streaming-Platform",
  "platform_state": "running",
  "uptime_seconds": 86400,
  "pipeline_health": {
    "bronze": {
      "active": true,
      "healthy": true,
      "input_rows_per_second": 1500,
      "processed_rows_per_second": 1485
    },
    "silver": {
      "active": true,
      "healthy": true,
      "input_rows_per_second": 1485,
      "processed_rows_per_second": 1470
    },
    "gold": {
      "active": true,
      "healthy": true,
      "input_rows_per_second": 1470,
      "processed_rows_per_second": 1465
    }
  },
  "configuration": {
    "processing_mode": "hybrid",
    "kafka_topics": ["retail-transactions", "customer-events"],
    "max_concurrent_streams": 10
  }
}
```

#### POST /api/v1/streaming/ingest
**Description**: Ingest real-time data into streaming pipelines

**Request**:
```json
{
  "topic": "retail-transactions",
  "data": {
    "transaction_id": "TXN-123456",
    "customer_id": "CUST-789",
    "amount": 99.99,
    "timestamp": "2023-12-01T12:00:00Z"
  },
  "partition_key": "CUST-789"
}
```

### Real-time Analytics

#### GET /api/v1/streaming/analytics/live
**Description**: Get live analytics data

**Parameters**:
- `metric`: Metric type (sales, customers, revenue)
- `time_window`: Time window (1h, 6h, 24h)
- `aggregation`: Aggregation type (sum, avg, count)

**Response**:
```json
{
  "metric": "sales",
  "time_window": "1h",
  "aggregation": "sum",
  "current_value": 45670.50,
  "previous_value": 42130.25,
  "change_percent": 8.4,
  "trend": "increasing",
  "last_update": "2023-12-01T12:00:00Z"
}
```

## Security APIs

### Security Dashboard

#### GET /api/v1/security/status
**Description**: Get comprehensive security status

**Authentication**: Required
**Permissions**: `perm_security_read`

**Response**:
```json
{
  "platform_status": "operational",
  "security_level": "high",
  "components_status": {
    "dlp_engine": "healthy",
    "access_control": "healthy",
    "compliance_engine": "healthy",
    "audit_system": "healthy"
  },
  "key_metrics": {
    "active_alerts": 2,
    "compliance_score": 0.98,
    "risk_score": 0.15
  },
  "security_features": {
    "dlp_enabled": true,
    "compliance_monitoring": true,
    "enhanced_access_control": true,
    "real_time_monitoring": true
  }
}
```

### DLP (Data Loss Prevention)

#### POST /api/v1/security/dlp/scan
**Description**: Scan data for sensitive information

**Request**:
```json
{
  "data": "Customer John Doe with SSN 123-45-6789 made a purchase",
  "scan_types": ["pii", "phi", "financial"],
  "redaction_level": "standard"
}
```

**Response**:
```json
{
  "scan_id": "dlp_20231201_123456",
  "risk_level": "high",
  "detected_entities": [
    {
      "type": "ssn",
      "value": "123-45-6789",
      "confidence": 0.95,
      "position": {"start": 34, "end": 45}
    }
  ],
  "redacted_data": "Customer John Doe with SSN [REDACTED] made a purchase",
  "compliance_flags": ["pii_detected", "gdpr_applicable"]
}
```

### Access Control

#### POST /api/v1/security/access/check
**Description**: Check access permissions for resources

**Request**:
```json
{
  "user_id": "user_123",
  "resource_id": "sales_data_2023",
  "operation": "read",
  "context": {
    "ip_address": "192.168.1.100",
    "user_agent": "Analytics Dashboard v1.0"
  }
}
```

**Response**:
```json
{
  "access_granted": true,
  "access_level": "read_only",
  "conditions": [
    "data_masking_required",
    "audit_logging_enabled"
  ],
  "expires_at": "2023-12-01T18:00:00Z",
  "decision_factors": {
    "role_based": true,
    "time_based": true,
    "location_based": true
  }
}
```

## Administration APIs

### User Management

#### GET /api/v1/admin/users
**Description**: List users with roles and permissions

**Authentication**: Required
**Permissions**: `perm_admin_users`

**Response**:
```json
{
  "users": [
    {
      "id": "user_123",
      "username": "analyst@company.com",
      "roles": ["data_analyst", "viewer"],
      "permissions": ["perm_read_data", "perm_run_analytics"],
      "status": "active",
      "last_login": "2023-12-01T11:30:00Z",
      "mfa_enabled": true
    }
  ],
  "pagination": {
    "page": 1,
    "total_count": 45
  }
}
```

### System Configuration

#### GET /api/v1/admin/config
**Description**: Get system configuration

**Response**:
```json
{
  "environment": "production",
  "features": {
    "ml_analytics": true,
    "real_time_processing": true,
    "advanced_security": true
  },
  "limits": {
    "api_rate_limit": 1000,
    "max_file_size_mb": 100,
    "max_batch_size": 10000
  }
}
```

## WebSocket APIs

### Real-time Security Dashboard

#### WebSocket: /ws/security/dashboard
**Description**: Real-time security events and metrics

**Authentication**: Token via query parameter or header

**Message Types**:

**Security Alert**:
```json
{
  "type": "security_alert",
  "alert_id": "alert_123",
  "severity": "high",
  "title": "Unusual Access Pattern Detected",
  "details": {
    "user_id": "user_456",
    "resource": "sensitive_data",
    "anomaly_score": 0.85
  },
  "timestamp": "2023-12-01T12:00:00Z"
}
```

**Metric Update**:
```json
{
  "type": "metric_update",
  "metrics": {
    "active_connections": 150,
    "security_score": 0.94,
    "threat_level": "low"
  },
  "timestamp": "2023-12-01T12:00:00Z"
}
```

### Live Analytics Dashboard

#### WebSocket: /ws/analytics/live
**Description**: Real-time analytics data updates

**Sample Message**:
```json
{
  "type": "analytics_update",
  "metric": "sales_revenue",
  "current_value": 156780.50,
  "change": 2340.75,
  "change_percent": 1.5,
  "timestamp": "2023-12-01T12:00:00Z"
}
```

## Error Handling

### Standard Error Response Format
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid input data",
    "details": {
      "field": "email",
      "reason": "Invalid email format"
    },
    "request_id": "req_20231201_123456",
    "timestamp": "2023-12-01T12:00:00Z"
  }
}
```

### HTTP Status Codes
- `200 OK`: Successful request
- `201 Created`: Resource created successfully
- `400 Bad Request`: Invalid request data
- `401 Unauthorized`: Authentication required
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource conflict
- `422 Unprocessable Entity`: Validation error
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server error
- `503 Service Unavailable`: Service temporarily unavailable

### Error Codes
- `AUTHENTICATION_FAILED`: Invalid credentials
- `AUTHORIZATION_DENIED`: Insufficient permissions
- `VALIDATION_ERROR`: Input validation failed
- `RESOURCE_NOT_FOUND`: Requested resource not found
- `RATE_LIMIT_EXCEEDED`: API rate limit exceeded
- `INTERNAL_ERROR`: Internal server error
- `SERVICE_UNAVAILABLE`: Service temporarily unavailable

## Rate Limiting

### Rate Limit Headers
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 945
X-RateLimit-Reset: 1701432000
X-RateLimit-Window: 3600
```

### Rate Limit Rules
- **Authentication endpoints**: 5 requests per minute
- **Analytics endpoints**: 100 requests per hour
- **Standard endpoints**: 1000 requests per hour
- **Admin endpoints**: 100 requests per hour
- **Burst limit**: 150% of standard limit for 5 minutes

### Rate Limit Response
```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Rate limit exceeded. Try again later.",
    "details": {
      "limit": 1000,
      "window": 3600,
      "reset_at": "2023-12-01T13:00:00Z"
    }
  }
}
```

## API Testing

### Test Environment
- **Base URL**: `https://api-test.pwc-challenge.com`
- **Test Data**: Synthetic data available
- **Rate Limits**: Relaxed for testing

### Example Test Requests

#### Using cURL
```bash
# Authentication
curl -X POST https://api.pwc-challenge.com/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test@example.com","password":"testpass"}'

# Get sales data
curl -X GET https://api.pwc-challenge.com/api/v1/sales \
  -H "Authorization: Bearer your_jwt_token_here" \
  -H "Content-Type: application/json"

# Make prediction
curl -X POST https://api.pwc-challenge.com/api/v1/ml-analytics/predict \
  -H "Authorization: Bearer your_jwt_token_here" \
  -H "Content-Type: application/json" \
  -d '{"model_type":"sales_forecast","data":{"historical_sales":[1000,1200,1100]}}'
```

#### Using Python
```python
import requests

# Authentication
auth_response = requests.post(
    'https://api.pwc-challenge.com/api/v1/auth/login',
    json={'username': 'test@example.com', 'password': 'testpass'}
)
token = auth_response.json()['access_token']

# API request
headers = {'Authorization': f'Bearer {token}'}
response = requests.get(
    'https://api.pwc-challenge.com/api/v1/sales',
    headers=headers
)
data = response.json()
```

### Postman Collection
A comprehensive Postman collection is available at:
`https://api.pwc-challenge.com/postman/collection.json`

## API Versioning

### Version Strategy
- **Major versions**: Breaking changes (v1, v2, v3)
- **Minor versions**: New features, backward compatible
- **Patch versions**: Bug fixes

### Version Headers
```
API-Version: 3.0.0
Accept-Version: 3.*
```

### Deprecation Policy
- 6 months notice for breaking changes
- 12 months support for deprecated versions
- Migration guides provided

## Support & Resources

### Documentation
- **API Reference**: `/docs`
- **GraphQL Schema**: `/api/graphql/schema`
- **OpenAPI Spec**: `/openapi.json`

### Support Channels
- **Email**: api-support@pwc-challenge.com
- **Slack**: #api-support
- **Documentation**: https://docs.pwc-challenge.com

### SDKs & Libraries
- **Python**: `pip install pwc-api-client`
- **JavaScript**: `npm install @pwc/api-client`
- **Java**: Maven/Gradle artifacts available

## Changelog

### v3.0.0 (Current)
- Enterprise security framework
- ML analytics endpoints
- Real-time streaming APIs
- WebSocket support
- Enhanced monitoring

### v2.1.0
- GraphQL API introduction
- Advanced search capabilities
- Bulk operations support

### v2.0.0
- API restructuring
- Authentication improvements
- Rate limiting implementation