# API Documentation

## Overview

The PwC Data Engineering Challenge API is an enterprise-grade REST API featuring comprehensive security, compliance monitoring, and advanced data processing capabilities. Built with FastAPI, it provides secure access to retail data analytics, ETL operations, and ML-powered insights with enterprise-level security controls.

## Key Features

- **Enterprise Security Framework**: Advanced DLP, RBAC/ABAC, multi-framework compliance
- **Real-time Processing**: WebSocket support for live data streaming
- **Advanced Analytics**: ML-powered predictions and customer segmentation
- **Self-Healing Systems**: Autonomous recovery and adaptive monitoring
- **Compliance Ready**: GDPR, HIPAA, PCI-DSS, SOX compliant data handling
- **Microservices Architecture**: Service discovery, circuit breakers, distributed tracing

## Base URLs

- **Development**: `http://localhost:8000`
- **Staging**: `https://api-staging.pwc-retail.com`
- **Production**: `https://api.pwc-retail.com`

## Current Version

**API Version**: 3.0.0  
**Security Level**: Enterprise  
**Compliance**: Multi-framework (GDPR, HIPAA, PCI-DSS, SOX)

## Authentication

The API supports multiple authentication methods with enterprise-grade security features:

### JWT Authentication with Enhanced Claims (Primary)

Enhanced JWT tokens with comprehensive security context:

```http
Authorization: Bearer <your-jwt-token>
```

#### Obtaining Enhanced JWT Token

```http
POST /api/v1/auth/login
Content-Type: application/json

{
    "username": "your_username",
    "password": "your_password",
    "mfa_token": "123456",
    "device_fingerprint": "optional_device_id"
}
```

**Enhanced Response:**
```json
{
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 3600,
    "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "user_info": {
        "user_id": "user123",
        "roles": ["analyst", "data_reader"],
        "permissions": ["perm_data_read", "perm_analytics_access"],
        "clearance_level": "standard",
        "session_id": "sess_xyz789",
        "mfa_verified": true,
        "risk_score": 0.15,
        "authentication_method": "password_mfa"
    }
}
```

**JWT Token Claims:**
- `sub`: User identifier
- `roles`: Assigned roles array
- `permissions`: Granular permissions array
- `clearance_level`: Security clearance (standard, elevated, admin)
- `session_id`: Session tracking identifier
- `mfa_verified`: Multi-factor authentication status
- `risk_score`: Calculated security risk (0.0-1.0)
- `source_ip`: Authentication source IP

### API Key Authentication

Secure API key system with usage tracking:

```http
Authorization: Bearer pwc_<api_key>
```

API keys provide:
- **Usage Tracking**: Request count and rate monitoring
- **Security Policies**: Configurable access restrictions
- **Expiration Management**: Time-based key rotation
- **Scope Limitation**: Permission-based access control

### OAuth2/OIDC Integration

Enterprise identity provider integration:

```http
GET /api/v1/auth/oauth/authorize?provider=azure&redirect_uri=https://app.pwc.com/callback
```

## Rate Limiting

The API implements sophisticated rate limiting with burst capacity:

| **Endpoint Category** | **Rate Limit** | **Burst Limit** | **Window** |
|-----------------------|----------------|------------------|------------|
| Authentication        | 5 req/min      | 10 req/min       | 60 seconds |
| Analytics             | 20 req/min     | 30 req/min       | 60 seconds |
| Sales Data            | 100 req/min    | 150 req/min      | 60 seconds |
| Search                | 200 req/min    | 300 req/min      | 60 seconds |

Rate limit headers are included in all responses:
- `X-RateLimit-Limit`: Maximum requests per window
- `X-RateLimit-Remaining`: Remaining requests in current window  
- `X-RateLimit-Reset`: Time when the rate limit resets

## Error Handling

The API uses standard HTTP status codes with detailed error responses:

```json
{
    "detail": "Insufficient permissions",
    "error_code": "PERMISSION_DENIED", 
    "timestamp": "2025-01-25T10:30:00Z",
    "correlation_id": "req_123456789",
    "error_type": "authorization_error",
    "suggested_action": "Contact administrator for elevated permissions"
}
```

### Error Codes

| **HTTP Status** | **Error Code** | **Description** |
|-----------------|----------------|-----------------|
| 400 | VALIDATION_ERROR | Request validation failed |
| 401 | AUTHENTICATION_REQUIRED | Valid authentication required |
| 403 | PERMISSION_DENIED | Insufficient permissions |
| 404 | RESOURCE_NOT_FOUND | Requested resource not found |
| 429 | RATE_LIMIT_EXCEEDED | Rate limit exceeded |
| 500 | INTERNAL_ERROR | Internal server error |

## API Endpoints

### Authentication Endpoints

#### POST /api/v1/auth/login
Enhanced login with MFA support.

**Request:**
```json
{
    "username": "john.doe@pwc.com",
    "password": "secure_password",
    "mfa_token": "123456",
    "device_fingerprint": "device_abc123"
}
```

**Response:**
```json
{
    "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
    "token_type": "bearer",
    "expires_in": 3600,
    "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
    "user_info": {
        "user_id": "user_123",
        "roles": ["analyst", "data_reader"],
        "permissions": ["perm_data_read", "perm_analytics_access"],
        "clearance_level": "standard",
        "mfa_verified": true,
        "risk_score": 0.15
    }
}
```

#### POST /api/v1/auth/refresh
Refresh an access token using a refresh token.

#### POST /api/v1/auth/validate
Validate the current authentication token.

### Sales Analytics Endpoints

#### GET /api/v1/sales
Retrieve paginated sales data with advanced filtering.

**Query Parameters:**
- `date_from` (optional): Start date filter (YYYY-MM-DD)
- `date_to` (optional): End date filter (YYYY-MM-DD)
- `product` (optional): Product name filter
- `country` (optional): Country code filter
- `page` (default: 1): Page number for pagination
- `size` (default: 20, max: 100): Items per page
- `sort` (default: "invoice_date:desc"): Sort criteria

**Example Request:**
```http
GET /api/v1/sales?date_from=2024-01-01&date_to=2024-12-31&country=US&page=1&size=20&sort=amount:desc
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
    "items": [
        {
            "invoice_no": "INV-2024-001",
            "stock_code": "PROD123",
            "description": "Product Description",
            "quantity": 10,
            "invoice_date": "2024-01-15T10:30:00Z",
            "unit_price": 29.99,
            "customer_id": "CUST456",
            "country": "United States",
            "total_amount": 299.90
        }
    ],
    "total": 1500,
    "page": 1,
    "size": 20,
    "pages": 75
}
```

### Analytics Endpoints

#### GET /api/v2/analytics/advanced-analytics
Advanced analytics with ML features and customer segmentation.

**Query Parameters:**
- `analysis_type` (required): Type of analysis ("rfm", "segmentation", "forecasting")
- `date_range` (optional): Analysis date range
- `customer_segment` (optional): Specific customer segment filter
- `include_predictions` (default: false): Include ML predictions

**Response:**
```json
{
    "analysis_type": "rfm",
    "results": {
        "customer_segments": {
            "champions": 245,
            "loyal_customers": 1820,
            "potential_loyalists": 890,
            "new_customers": 450,
            "at_risk": 320,
            "hibernating": 180
        },
        "rfm_scores": {
            "avg_recency": 145.2,
            "avg_frequency": 3.8,
            "avg_monetary": 287.50
        },
        "ml_insights": {
            "churn_probability": 0.23,
            "next_purchase_prediction": "2024-02-15",
            "recommended_actions": [
                "Target at-risk customers with retention campaign",
                "Upsell to loyal customers with premium products"
            ]
        }
    },
    "generated_at": "2024-01-25T10:30:00Z",
    "data_quality_score": 0.97
}
```

### Batch Operations

#### POST /api/v1/batch/create
Create multiple records in a single operation.

**Request:**
```json
{
    "table": "sales",
    "records": [
        {
            "invoice_no": "INV-2024-001",
            "stock_code": "PROD123",
            "quantity": 5,
            "unit_price": 29.99
        },
        {
            "invoice_no": "INV-2024-002", 
            "stock_code": "PROD124",
            "quantity": 2,
            "unit_price": 49.99
        }
    ],
    "options": {
        "batch_size": 1000,
        "continue_on_error": true,
        "validate_before_insert": true
    }
}
```

**Response:**
```json
{
    "batch_id": "batch_789",
    "status": "completed",
    "records_processed": 2,
    "records_success": 2,
    "records_failed": 0,
    "processing_time_ms": 145,
    "errors": [],
    "performance_metrics": {
        "throughput_per_second": 13.8,
        "memory_usage_mb": 45.2
    }
}
```

### Search Endpoints

#### GET /api/v1/search
Advanced search across all datasets with semantic similarity.

**Query Parameters:**
- `q` (required): Search query
- `type` (optional): Search type ("semantic", "keyword", "hybrid")
- `filters` (optional): JSON filters object
- `limit` (default: 20): Maximum results
- `offset` (default: 0): Results offset

**Example:**
```http
GET /api/v1/search?q=premium+electronics&type=semantic&limit=10
```

**Response:**
```json
{
    "query": "premium electronics",
    "results": [
        {
            "id": "prod_123",
            "title": "Premium Electronic Device",
            "description": "High-quality electronic product...",
            "score": 0.95,
            "category": "electronics",
            "metadata": {
                "price_range": "premium",
                "availability": "in_stock"
            }
        }
    ],
    "total_results": 150,
    "search_time_ms": 23,
    "suggestions": ["premium electronics accessories"]
}
```

### Async Task Management

#### POST /api/v1/async-tasks/submit
Submit long-running tasks for background processing.

**Request:**
```json
{
    "task_type": "data_export",
    "parameters": {
        "format": "csv",
        "date_range": {
            "start": "2024-01-01",
            "end": "2024-12-31"
        },
        "filters": {
            "country": ["US", "UK", "CA"]
        }
    },
    "priority": "normal",
    "callback_url": "https://your-app.com/callbacks/export-complete"
}
```

**Response:**
```json
{
    "task_id": "task_abc123",
    "status": "submitted",
    "estimated_completion": "2024-01-25T10:35:00Z",
    "progress_url": "/api/v1/async-tasks/task_abc123/status"
}
```

#### GET /api/v1/async-tasks/{task_id}/status
Get status of background task.

**Response:**
```json
{
    "task_id": "task_abc123",
    "status": "processing",
    "progress": 65.5,
    "created_at": "2024-01-25T10:30:00Z",
    "started_at": "2024-01-25T10:30:15Z",
    "estimated_completion": "2024-01-25T10:35:00Z",
    "result_url": null,
    "error": null,
    "metadata": {
        "records_processed": 150000,
        "records_remaining": 79500
    }
}
```

### Security & Monitoring

#### GET /api/v1/security/status
Comprehensive security system status (requires admin permissions).

**Response:**
```json
{
    "platform_status": "operational",
    "security_level": "high",
    "components_status": {
        "dlp_engine": "healthy",
        "access_control": "healthy", 
        "compliance_engine": "healthy",
        "threat_detection": "healthy"
    },
    "key_metrics": {
        "active_sessions": 1250,
        "security_events_24h": 3,
        "compliance_score": 0.98,
        "threat_level": "low"
    },
    "security_features": {
        "dlp_enabled": true,
        "compliance_monitoring": true,
        "enhanced_access_control": true,
        "real_time_monitoring": true
    }
}
```

#### POST /api/v1/security/assessment/trigger
Trigger comprehensive security assessment (admin only).

### Health & Monitoring

#### GET /api/v1/health
System health check with comprehensive status.

**Response:**
```json
{
    "status": "healthy",
    "environment": "production",
    "version": "3.0.0",
    "timestamp": "2024-01-25T10:30:00Z",
    "security": {
        "platform_status": "operational",
        "auth_enabled": true,
        "dlp_enabled": true,
        "compliance_monitoring": true
    },
    "metrics": {
        "active_connections": 1250,
        "response_time_ms": 45,
        "uptime_seconds": 2592000
    },
    "component_health": {
        "database": "healthy",
        "redis": "healthy",
        "elasticsearch": "healthy",
        "kafka": "healthy"
    }
}
```

## WebSocket Endpoints

### Real-time Security Dashboard

#### WS /ws/security/dashboard
Real-time security monitoring with live updates.

**Connection:**
```javascript
const socket = new WebSocket('wss://api.pwc-retail.com/ws/security/dashboard?token=your_jwt_token');

socket.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log('Security update:', data);
};
```

**Message Types:**
- `security_alert`: Real-time security alerts
- `threat_update`: Threat level changes
- `compliance_event`: Compliance monitoring events
- `metrics_update`: Live security metrics

## SDK Examples

### Python SDK

```python
from pwc_api_client import PWCAPIClient
import asyncio

async def main():
    # Initialize client
    client = PWCAPIClient(
        base_url="https://api.pwc-retail.com",
        api_key="your_api_key"
    )
    
    # Get sales data
    sales = await client.sales.list(
        date_from="2024-01-01",
        date_to="2024-12-31",
        country="US"
    )
    
    # Run analytics
    analytics = await client.analytics.advanced_analytics(
        analysis_type="rfm",
        include_predictions=True
    )
    
    print(f"Found {sales.total} sales records")
    print(f"Customer segments: {analytics.results.customer_segments}")

asyncio.run(main())
```

### JavaScript/Node.js SDK

```javascript
const { PWCAPIClient } = require('@pwc/api-client');

const client = new PWCAPIClient({
    baseUrl: 'https://api.pwc-retail.com',
    apiKey: 'your_api_key'
});

// Get sales data
const sales = await client.sales.list({
    dateFrom: '2024-01-01',
    dateTo: '2024-12-31',
    country: 'US'
});

// Submit async task
const task = await client.asyncTasks.submit({
    taskType: 'data_export',
    parameters: {
        format: 'csv',
        dateRange: { start: '2024-01-01', end: '2024-12-31' }
    }
});

console.log(`Task ${task.taskId} submitted`);
```

## Advanced Features

### Data Loss Prevention (DLP)

The API automatically detects and handles sensitive data:

- **PII Detection**: Automatic detection of personally identifiable information
- **PHI Protection**: Healthcare data protection compliance
- **Data Classification**: Automatic data sensitivity classification
- **Policy Enforcement**: Configurable data handling policies

### Compliance Frameworks

- **GDPR**: Data subject rights, consent management, breach notification
- **HIPAA**: Protected health information safeguards
- **PCI-DSS**: Payment card data security standards  
- **SOX**: Financial data controls and audit trails

### Performance Optimization

- **Intelligent Caching**: Multi-level caching with Redis
- **Connection Pooling**: Optimized database connections
- **Compression**: GZip compression for large responses
- **CDN Integration**: Global content delivery network

### Monitoring & Observability

- **Distributed Tracing**: End-to-end request tracing
- **Custom Metrics**: Business and technical metrics
- **Real-time Dashboards**: Live performance monitoring
- **Automated Alerting**: Intelligent alert management

## Migration Guide

### Upgrading from v2.x to v3.0

**Breaking Changes:**
- Authentication now required for all endpoints except `/health`
- Response format changes for error responses
- New required headers for security compliance

**Migration Steps:**
1. Update authentication implementation
2. Handle new error response format
3. Add required security headers
4. Test with new rate limiting

## Support

- **Documentation**: [https://docs.api.pwc-retail.com](https://docs.api.pwc-retail.com)
- **Support Email**: api-support@pwc.com
- **Emergency**: +1-555-PWC-API-1 (24/7)
- **Status Page**: [https://status.api.pwc-retail.com](https://status.api.pwc-retail.com)

```http
Authorization: Bearer <oauth_access_token>
```

Supported providers:
- Azure Active Directory
- Google Workspace
- Okta
- Auth0
- Custom OIDC providers

### Multi-Factor Authentication (MFA)

Enhanced security with MFA support:
- **TOTP (Time-based One-Time Password)**: Google Authenticator, Authy
- **SMS/Voice**: Phone-based verification
- **Hardware Tokens**: FIDO2/WebAuthn support
- **Biometric**: Device-based biometric authentication

### Basic Authentication (Development Only)

For development and testing environments only:

```http
Authorization: Basic <base64-encoded-username:password>
```

## Enterprise Security Framework

### Data Loss Prevention (DLP)

Automatic PII/PHI detection and redaction:

**Supported Data Types:**
- Email addresses: `user@example.com` → `u***@e***.com`
- Phone numbers: `+1234567890` → `+12*****890`
- Credit card numbers: `1234-5678-9012-3456` → `1234-****-****-3456`
- Social Security Numbers: Automatically redacted
- IBAN/Bank Account Numbers: Partially masked
- Personal Names: Context-aware redaction

**DLP Response Headers:**
```http
X-DLP-Scan-ID: dlp_scan_12345
X-Data-Classification: confidential
X-Security-Processed: true
X-Redaction-Applied: pii_email,pii_phone
```

### Access Control (RBAC/ABAC)

**Role-Based Access Control:**
- `admin`: Full system access
- `data_engineer`: ETL and pipeline management
- `analyst`: Data analysis and reporting
- `viewer`: Read-only access
- `api_user`: Programmatic access

**Attribute-Based Access Control:**
- Time-based access restrictions
- IP address whitelisting/blacklisting
- Device trust levels
- Data classification-based filtering
- Geographic access controls

**Permission System:**
```json
{
  "permissions": [
    "perm_data_read",
    "perm_data_write", 
    "perm_analytics_access",
    "perm_admin_system",
    "perm_security_read",
    "perm_etl_manage"
  ]
}
```

### Compliance Framework Support

**GDPR Compliance:**
- Data subject request handling
- Right to be forgotten implementation
- Data portability support
- Consent management
- Processing activity logging

**HIPAA Compliance:**
- PHI automatic detection and protection
- Access logging and audit trails
- Administrative safeguards
- Physical safeguards implementation
- Technical safeguards

**PCI-DSS Compliance:**
- Payment data protection
- Secure transmission protocols
- Access control measures
- Network security monitoring
- Regular security testing

**SOX Compliance:**
- Financial data controls
- Change management auditing
- Access management
- Data integrity monitoring
- Reporting controls

### Security Monitoring

**Real-time Threat Detection:**
- Anomalous access pattern detection
- Brute force attack prevention
- SQL injection attempt monitoring
- Privilege escalation detection
- Data exfiltration monitoring

**Security Metrics:**
```json
{
  "security_score": 0.95,
  "threat_level": "low",
  "active_alerts": 0,
  "security_events_24h": 125,
  "compliance_score": 1.0
}
```

### Security Headers

All API responses include comprehensive security headers:

```http
X-Security-Processed: true
X-Data-Classification: internal
X-DLP-Scan-ID: dlp_12345
X-Security-Score: 0.95
X-Correlation-ID: req_abcd1234
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
Strict-Transport-Security: max-age=31536000; includeSubDomains
Content-Security-Policy: default-src 'self'
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
Enhanced health endpoint with comprehensive system status including security components.

**Response:**
```json
{
    "status": "healthy",
    "environment": "development",
    "version": "3.0.0",
    "timestamp": "2025-08-29T10:30:00Z",
    "security": {
        "platform_status": "operational",
        "security_level": "operational",
        "auth_enabled": true,
        "dlp_enabled": true,
        "compliance_monitoring": true,
        "rate_limiting": true,
        "https_enabled": true,
        "mfa_supported": true,
        "oauth_enabled": true,
        "api_keys_supported": true
    },
    "component_health": {
        "security_orchestrator": "healthy",
        "dlp_engine": "healthy",
        "access_control": "healthy",
        "compliance_engine": "healthy",
        "auth_service": "healthy"
    },
    "metrics": {
        "active_connections": 45,
        "security_events_24h": 125,
        "compliance_score": 1.0
    }
}
```

#### GET /
Enhanced root endpoint with comprehensive API capabilities overview.

**Response:**
```json
{
    "message": "PwC Data Engineering Challenge - Enterprise Security API",
    "version": "3.0.0",
    "documentation": "/docs",
    "health": "/health",
    "security_dashboard": "/api/v1/security/dashboard",
    "websocket_dashboard": "/ws/security/dashboard",
    "security_features": {
        "dlp_enabled": true,
        "compliance_monitoring": true,
        "enhanced_access_control": true,
        "real_time_monitoring": true,
        "security_level": "operational"
    },
    "supported_compliance": ["GDPR", "HIPAA", "PCI-DSS", "SOX"],
    "api_capabilities": [
        "JWT Authentication with MFA support",
        "OAuth2/OIDC Integration",
        "API Key Management",
        "Real-time PII/PHI Detection and Redaction",
        "Advanced RBAC/ABAC Authorization",
        "Privilege Elevation Management",
        "Automated Security Testing",
        "Real-time Security Monitoring",
        "Compliance Reporting"
    ]
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

### Security Management Endpoints

#### GET /api/v1/security/status
Get comprehensive security system status and metrics.

**Required Permissions**: `perm_security_read` or `perm_admin_system`

**Response:**
```json
{
    "platform_status": "operational",
    "security_level": "operational",
    "components_status": {
        "dlp_engine": "active",
        "access_control": "active",
        "compliance_monitoring": "active"
    },
    "key_metrics": {
        "active_alerts": 0,
        "compliance_score": 1.0,
        "security_events_24h": 125,
        "threat_level": "low"
    },
    "active_websocket_connections": 12,
    "security_features": {
        "dlp_enabled": true,
        "compliance_monitoring": true,
        "enhanced_access_control": true,
        "real_time_monitoring": true,
        "data_governance": true
    },
    "authentication_methods": [
        "JWT with enhanced claims",
        "API Keys with security policies",
        "OAuth2/OIDC integration",
        "Multi-factor authentication"
    ],
    "compliance_frameworks": ["GDPR", "HIPAA", "PCI-DSS", "SOX"],
    "last_security_assessment": "2025-08-29T10:00:00Z"
}
```

#### POST /api/v1/security/assessment/trigger
Trigger comprehensive security assessment.

**Required Permissions**: `perm_admin_system`

**Response:**
```json
{
    "assessment_triggered": true,
    "assessment_id": "assess_12345",
    "status": "running",
    "summary": {
        "total_checks": 150,
        "completed": 0,
        "estimated_duration": "5-10 minutes"
    },
    "triggered_by": "user123"
}
```

#### GET /api/v1/security/test/framework-status
Get security testing framework status (non-production only).

**Response:**
```json
{
    "testing_enabled": true,
    "environment": "development",
    "framework_initialized": true,
    "available_tests": [
        "comprehensive_security_assessment",
        "penetration_testing",
        "vulnerability_assessment",
        "compliance_validation",
        "authentication_testing",
        "authorization_testing",
        "input_validation_testing",
        "data_protection_testing"
    ],
    "supported_compliance_standards": [
        "OWASP Top 10",
        "GDPR",
        "PCI-DSS",
        "HIPAA",
        "SOX",
        "ISO 27001"
    ],
    "framework_version": "3.0.0"
}
```

### WebSocket Endpoints

#### WS /ws/security/dashboard
Real-time security dashboard with live event streaming.

**Authentication**: Include `token` parameter in query string or WebSocket headers.

**Connection Example:**
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/security/dashboard?token=your_jwt_token');

ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log('Security Event:', data);
};
```

**Message Types:**
- `security_alert`: Real-time security alerts
- `system_status`: System status updates
- `compliance_event`: Compliance-related events
- `user_activity`: User activity monitoring
- `threat_detection`: Threat detection alerts

**Example Messages:**
```json
{
    "type": "security_alert",
    "timestamp": "2025-08-29T10:30:00Z",
    "severity": "medium",
    "event_id": "alert_12345",
    "description": "Anomalous access pattern detected",
    "user_id": "user123",
    "source_ip": "192.168.1.100",
    "action_taken": "access_logged"
}
```

#### WS /ws/security/alerts
Dedicated WebSocket endpoint for security alerts only.

**Authentication**: Include `token` parameter in query string.

**Alert Categories:**
- `authentication_failure`
- `privilege_escalation`
- `data_access_anomaly`
- `compliance_violation`
- `system_intrusion`

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