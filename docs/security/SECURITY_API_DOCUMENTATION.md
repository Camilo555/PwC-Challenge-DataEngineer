# Enterprise Security API Documentation

## Overview

This document provides comprehensive API documentation for the enterprise security and compliance framework, including REST endpoints, WebSocket APIs, authentication flows, and practical examples.

## Table of Contents

1. [Authentication & Authorization](#authentication--authorization)
2. [Security APIs](#security-apis)
3. [Compliance APIs](#compliance-apis)
4. [WebSocket APIs](#websocket-apis)
5. [Error Handling](#error-handling)
6. [Code Examples](#code-examples)

---

## Authentication & Authorization

### Authentication Methods

#### 1. JWT Token Authentication
```http
POST /api/v1/auth/login
Content-Type: application/json

{
    "username": "security_admin",
    "password": "secure_password",
    "mfa_token": "123456"
}
```

**Response:**
```json
{
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 3600,
    "permissions": [
        "perm_admin_system",
        "perm_audit_logs",
        "perm_compliance_report"
    ]
}
```

#### 2. OAuth2 Flow
```http
GET /api/v1/auth/oauth/authorize
    ?client_id=security_dashboard
    &response_type=code
    &scope=security:read compliance:write
    &redirect_uri=https://dashboard.company.com/callback
```

#### 3. API Key Authentication
```http
GET /api/v1/security/dashboard
Authorization: Bearer your-api-key-here
X-API-Version: 1.0
```

### Permission System

#### Permission Levels
- `perm_admin_system` - System administration access
- `perm_audit_logs` - Audit log access
- `perm_compliance_report` - Compliance reporting
- `perm_admin_users` - User administration
- `perm_security_scan` - Security scanning capabilities
- `perm_dlp_management` - DLP policy management

---

## Security APIs

### 1. Data Security Scanning

#### Scan Data for Sensitive Content
```http
POST /api/v1/security/scan
Authorization: Bearer {access_token}
Content-Type: application/json

{
    "data": {
        "customer_email": "john.doe@example.com",
        "ssn": "123-45-6789",
        "credit_card": "4111-1111-1111-1111"
    },
    "context": {
        "location": "api_endpoint",
        "operation": "export",
        "user_id": "user123"
    }
}
```

**Response:**
```json
{
    "scan_id": "scan_20241026_143022_abc123",
    "action": "processed",
    "data": {
        "customer_email": "jo**@example.com",
        "ssn": "***-**-6789",
        "credit_card": "****-****-****-1111"
    },
    "detections": 3,
    "sensitive_data_types": ["email", "ssn", "credit_card"],
    "compliance_frameworks": ["gdpr", "pci_dss"],
    "risk_score": 8.5,
    "scan_duration_seconds": 0.125,
    "policy_violations": 1,
    "metadata": {
        "timestamp": "2024-10-26T14:30:22.123Z",
        "context": {
            "location": "api_endpoint",
            "operation": "export",
            "user_id": "user123"
        }
    }
}
```

#### Check Data Classification
```http
POST /api/v1/security/classify
Authorization: Bearer {access_token}
Content-Type: application/json

{
    "text": "Patient John Smith's medical record number is MRN123456",
    "context": "medical_records"
}
```

**Response:**
```json
{
    "classification_results": [
        {
            "data_type": "medical_record",
            "classification": "restricted",
            "confidence": 0.95,
            "matched_text": "MRN123456",
            "start_position": 42,
            "end_position": 51,
            "compliance_frameworks": ["hipaa"]
        }
    ],
    "overall_classification": "restricted",
    "processing_time_ms": 15
}
```

### 2. Security Assessment

#### Run Comprehensive Security Assessment
```http
POST /api/v1/security/assess
Authorization: Bearer {access_token}
Content-Type: application/json

{
    "assessment_type": "comprehensive",
    "include_components": [
        "dlp",
        "compliance",
        "access_control",
        "governance"
    ]
}
```

**Response:**
```json
{
    "assessment_id": "assessment_20241026_143045",
    "timestamp": "2024-10-26T14:30:45.789Z",
    "overall_status": "healthy",
    "components": {
        "security": {
            "status": "healthy",
            "active_threats": 2,
            "blocked_ips": 5,
            "total_events_24h": 1247
        },
        "dlp": {
            "status": "warning",
            "incidents_24h": 3,
            "unresolved_incidents": 1,
            "policies_active": 12
        },
        "compliance": {
            "status": "healthy",
            "compliance_rate": 0.94,
            "total_violations": 2,
            "overdue_remediations": 0
        },
        "access_control": {
            "status": "healthy",
            "total_subjects": 1250,
            "active_elevations": 8,
            "total_policies": 45
        }
    },
    "recommendations": [
        "Review and resolve recent DLP incidents",
        "Security posture is healthy - continue monitoring"
    ]
}
```

#### Get Security Dashboard Data
```http
GET /api/v1/security/dashboard
Authorization: Bearer {access_token}
```

**Response:**
```json
{
    "summary": {
        "health_status": "healthy",
        "total_alerts": 15,
        "critical_alerts": 2,
        "compliance_score": 0.94,
        "system_uptime": 99.8
    },
    "widgets": {
        "threat_detection": {
            "active_threats": 2,
            "threats_blocked_24h": 47,
            "threat_sources": ["malware", "phishing"]
        },
        "dlp_monitoring": {
            "scans_24h": 1520,
            "incidents_created": 3,
            "data_protected_gb": 125.7
        },
        "compliance_status": {
            "gdpr_compliance": 0.96,
            "hipaa_compliance": 0.92,
            "pci_dss_compliance": 0.98
        },
        "access_control": {
            "login_attempts_24h": 3420,
            "failed_logins": 23,
            "privileged_sessions": 12
        }
    },
    "recent_events": [
        {
            "timestamp": "2024-10-26T14:25:00Z",
            "type": "dlp_detection",
            "severity": "medium",
            "message": "PII detected in API response"
        },
        {
            "timestamp": "2024-10-26T14:20:00Z",
            "type": "compliance_violation",
            "severity": "high",
            "message": "GDPR data retention policy violation"
        }
    ]
}
```

### 3. Access Control

#### Check Access Permissions
```http
POST /api/v1/security/access/check
Authorization: Bearer {access_token}
Content-Type: application/json

{
    "subject_id": "user123",
    "action": "read",
    "resource_id": "customer_database",
    "resource_type": "database",
    "context": {
        "location": "office",
        "time": "2024-10-26T14:30:00Z",
        "ip_address": "192.168.1.100"
    }
}
```

**Response:**
```json
{
    "access_decision": {
        "allowed": true,
        "decision": "conditional_allow",
        "reason": "Access granted with MFA requirement",
        "risk_score": 3.2,
        "evaluation_time_ms": 45,
        "conditions": [
            {
                "type": "mfa_required",
                "description": "Multi-factor authentication required",
                "expires_in": 3600
            }
        ]
    },
    "policy_matches": [
        {
            "policy_id": "policy_database_access",
            "match_type": "role_based",
            "confidence": 1.0
        }
    ]
}
```

---

## Compliance APIs

### 1. Compliance Monitoring

#### Run Compliance Assessment
```http
POST /api/v1/compliance/assess
Authorization: Bearer {access_token}
Content-Type: application/json

{
    "framework": "gdpr",
    "control_ids": ["GDPR-7.1", "GDPR-32.1"],
    "assessor_id": "compliance_officer"
}
```

**Response:**
```json
{
    "assessment_results": [
        {
            "assessment_id": "assess_gdpr_20241026",
            "control_id": "GDPR-7.1",
            "framework": "gdpr",
            "timestamp": "2024-10-26T14:30:00Z",
            "assessor_id": "compliance_officer",
            "status": "compliant",
            "score": 0.95,
            "findings": [
                "Lawful basis documented for all processing activities",
                "Data processing register maintained and up-to-date"
            ],
            "evidence": [
                {
                    "type": "documentation",
                    "description": "Data processing register",
                    "location": "/compliance/gdpr/processing-register.pdf"
                }
            ],
            "risk_level": "low"
        }
    ],
    "summary": {
        "total_controls": 2,
        "compliant": 1,
        "non_compliant": 0,
        "partially_compliant": 1
    }
}
```

#### Get Compliance Dashboard
```http
GET /api/v1/compliance/dashboard?framework=all
Authorization: Bearer {access_token}
```

**Response:**
```json
{
    "overall_compliance": {
        "total_controls": 45,
        "assessed_controls": 42,
        "compliant_controls": 38,
        "non_compliant_controls": 2,
        "compliance_rate": 0.90
    },
    "framework_compliance": {
        "gdpr": {
            "compliance_rate": 0.94,
            "total_controls": 15,
            "compliant": 14,
            "non_compliant": 1
        },
        "hipaa": {
            "compliance_rate": 0.88,
            "total_controls": 12,
            "compliant": 10,
            "non_compliant": 2
        },
        "pci_dss": {
            "compliance_rate": 0.96,
            "total_controls": 18,
            "compliant": 17,
            "non_compliant": 1
        }
    },
    "violation_summary": {
        "total_violations": 5,
        "by_severity": {
            "critical": 1,
            "high": 2,
            "medium": 1,
            "low": 1
        },
        "overdue_remediations": 2
    }
}
```

#### Generate Compliance Report
```http
POST /api/v1/compliance/report
Authorization: Bearer {access_token}
Content-Type: application/json

{
    "framework": "gdpr",
    "start_date": "2024-09-01T00:00:00Z",
    "end_date": "2024-10-26T23:59:59Z",
    "format": "detailed"
}
```

**Response:**
```json
{
    "report_id": "gdpr_report_20241026",
    "framework": "gdpr",
    "report_period": {
        "start_date": "2024-09-01T00:00:00Z",
        "end_date": "2024-10-26T23:59:59Z"
    },
    "summary": {
        "total_controls_assessed": 15,
        "compliant": 14,
        "non_compliant": 1,
        "partially_compliant": 0,
        "compliance_rate": 0.93
    },
    "violations": {
        "total": 3,
        "by_severity": {
            "critical": 0,
            "high": 1,
            "medium": 1,
            "low": 1
        },
        "open": 2,
        "resolved": 1
    },
    "control_performance": {
        "GDPR-7.1": {
            "assessments": 4,
            "compliant": 4,
            "average_score": 0.95,
            "latest_status": "compliant"
        }
    },
    "recommendations": [
        "Address data retention policy violations",
        "Implement automated consent management"
    ],
    "download_url": "/api/v1/compliance/reports/gdpr_report_20241026/download"
}
```

### 2. Violation Management

#### Get Compliance Violations
```http
GET /api/v1/compliance/violations
    ?framework=gdpr
    &status=open
    &severity=high
    &limit=10
Authorization: Bearer {access_token}
```

**Response:**
```json
{
    "violations": [
        {
            "violation_id": "violation_gdpr_001",
            "timestamp": "2024-10-26T10:15:00Z",
            "framework": "gdpr",
            "control_id": "GDPR-17.1",
            "severity": "high",
            "description": "Data deletion request not processed within required timeframe",
            "affected_systems": ["customer_database", "analytics_platform"],
            "affected_data_types": ["personal_data"],
            "user_id": "system",
            "remediation_required": true,
            "remediation_deadline": "2024-11-02T00:00:00Z",
            "status": "open",
            "assignee": "data_protection_officer",
            "remediation_actions": [
                "Process pending deletion requests",
                "Implement automated deletion workflow"
            ]
        }
    ],
    "pagination": {
        "page": 1,
        "per_page": 10,
        "total": 1,
        "total_pages": 1
    }
}
```

#### Create Remediation Action
```http
POST /api/v1/compliance/violations/{violation_id}/remediate
Authorization: Bearer {access_token}
Content-Type: application/json

{
    "title": "Implement automated data deletion",
    "description": "Develop and deploy automated workflow for processing data deletion requests",
    "priority": "high",
    "assignee": "engineering_team",
    "due_date": "2024-11-01T00:00:00Z"
}
```

**Response:**
```json
{
    "action_id": "remediation_001",
    "violation_id": "violation_gdpr_001",
    "title": "Implement automated data deletion",
    "status": "pending",
    "priority": "high",
    "assignee": "engineering_team",
    "due_date": "2024-11-01T00:00:00Z",
    "created_at": "2024-10-26T14:30:00Z"
}
```

---

## WebSocket APIs

### 1. Security Dashboard WebSocket

#### Connection Endpoint
```
wss://api.company.com/ws/security/dashboard?token={access_token}
```

#### Authentication Message
```json
{
    "type": "auth",
    "data": {
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
    }
}
```

#### Welcome Message (Server → Client)
```json
{
    "type": "welcome",
    "data": {
        "connection_id": "conn_20241026_143045_abc123",
        "user_id": "user123",
        "permissions": ["perm_admin_system", "perm_audit_logs"],
        "server_time": "2024-10-26T14:30:45Z",
        "available_topics": [
            "security_alerts",
            "compliance_violations",
            "dlp_incidents",
            "access_control_events",
            "system_health",
            "dashboard_metrics"
        ]
    },
    "metadata": {
        "connection_established": true,
        "heartbeat_interval": 30
    }
}
```

#### Subscribe to Topic
```json
{
    "type": "subscribe",
    "data": {
        "topic": "security_alerts"
    }
}
```

#### Subscription Confirmation (Server → Client)
```json
{
    "type": "subscription_confirmed",
    "data": {
        "topic": "security_alerts"
    },
    "metadata": {
        "subscription_count": 1
    }
}
```

#### Real-time Security Alert (Server → Client)
```json
{
    "type": "security_alert",
    "data": {
        "alert_id": "alert_20241026_143100",
        "severity": "high",
        "category": "threat_detection",
        "title": "Suspicious API Access Pattern",
        "description": "Multiple failed authentication attempts from IP 192.168.1.50",
        "affected_resources": ["api_gateway"],
        "source_ip": "192.168.1.50",
        "user_id": "unknown",
        "timestamp": "2024-10-26T14:31:00Z",
        "remediation_suggestions": [
            "Block suspicious IP address",
            "Review authentication logs"
        ]
    },
    "metadata": {
        "real_time": true,
        "priority": "immediate"
    }
}
```

### 2. WebSocket Message Types

#### Client → Server Messages
```json
// Subscribe to topic
{
    "type": "subscribe",
    "data": {"topic": "dlp_incidents"}
}

// Unsubscribe from topic
{
    "type": "unsubscribe", 
    "data": {"topic": "dlp_incidents"}
}

// Get dashboard data
{
    "type": "get_dashboard_data",
    "data": {}
}

// Run security scan
{
    "type": "run_security_scan",
    "data": {"scan_type": "comprehensive"}
}

// Ping
{
    "type": "ping",
    "data": {}
}
```

#### Server → Client Messages
```json
// DLP Incident Alert
{
    "type": "dlp_incident",
    "data": {
        "incident_id": "dlp_20241026_143200",
        "data_type": "credit_card",
        "severity": "high",
        "action_taken": "blocked",
        "location": "api_endpoint",
        "timestamp": "2024-10-26T14:32:00Z"
    }
}

// Compliance Violation Alert
{
    "type": "compliance_violation",
    "data": {
        "violation_id": "violation_20241026_143300",
        "framework": "gdpr",
        "control": "GDPR-32.1",
        "severity": "medium",
        "description": "Encryption not enabled for data export"
    }
}

// Dashboard Metrics Update
{
    "type": "metrics_update",
    "data": {
        "active_threats": 3,
        "dlp_incidents_24h": 7,
        "compliance_score": 0.94
    }
}

// Heartbeat
{
    "type": "heartbeat",
    "data": {
        "server_time": "2024-10-26T14:35:00Z",
        "connection_duration": 300
    }
}
```

---

## Error Handling

### Error Response Format
```json
{
    "error": {
        "code": "SECURITY_SCAN_FAILED",
        "message": "Data scanning failed due to invalid input",
        "details": {
            "field": "data",
            "reason": "Invalid JSON format",
            "suggestion": "Ensure data is valid JSON"
        },
        "timestamp": "2024-10-26T14:30:00Z",
        "request_id": "req_20241026_143000_xyz789"
    }
}
```

### Common Error Codes

#### Authentication Errors (401)
- `AUTH_TOKEN_INVALID` - Invalid authentication token
- `AUTH_TOKEN_EXPIRED` - Authentication token has expired
- `MFA_REQUIRED` - Multi-factor authentication required

#### Authorization Errors (403)
- `INSUFFICIENT_PERMISSIONS` - User lacks required permissions
- `RESOURCE_ACCESS_DENIED` - Access to specific resource denied
- `OPERATION_NOT_ALLOWED` - Operation not permitted for user

#### Validation Errors (400)
- `INVALID_REQUEST_FORMAT` - Request format is invalid
- `MISSING_REQUIRED_FIELD` - Required field is missing
- `INVALID_FIELD_VALUE` - Field contains invalid value

#### Rate Limiting (429)
- `RATE_LIMIT_EXCEEDED` - API rate limit exceeded
- `CONNECTION_LIMIT_EXCEEDED` - WebSocket connection limit exceeded

#### Server Errors (500)
- `SECURITY_SCAN_FAILED` - Security scanning service error
- `COMPLIANCE_CHECK_FAILED` - Compliance checking service error
- `INTERNAL_SERVICE_ERROR` - Internal service unavailable

---

## Code Examples

### Python Client Example

```python
import requests
import websocket
import json
from typing import Dict, Any

class SecurityAPIClient:
    def __init__(self, base_url: str, access_token: str):
        self.base_url = base_url
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
    
    def scan_data(self, data: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Scan data for sensitive content"""
        payload = {
            'data': data,
            'context': context or {}
        }
        
        response = requests.post(
            f'{self.base_url}/api/v1/security/scan',
            headers=self.headers,
            json=payload
        )
        
        response.raise_for_status()
        return response.json()
    
    def run_security_assessment(self) -> Dict[str, Any]:
        """Run comprehensive security assessment"""
        payload = {
            'assessment_type': 'comprehensive',
            'include_components': ['dlp', 'compliance', 'access_control']
        }
        
        response = requests.post(
            f'{self.base_url}/api/v1/security/assess',
            headers=self.headers,
            json=payload
        )
        
        response.raise_for_status()
        return response.json()
    
    def check_compliance(self, framework: str) -> Dict[str, Any]:
        """Check compliance for specific framework"""
        response = requests.get(
            f'{self.base_url}/api/v1/compliance/dashboard',
            headers=self.headers,
            params={'framework': framework}
        )
        
        response.raise_for_status()
        return response.json()

# Usage example
client = SecurityAPIClient(
    base_url='https://api.company.com',
    access_token='your_access_token_here'
)

# Scan sensitive data
scan_result = client.scan_data({
    'customer_email': 'john@example.com',
    'phone': '555-123-4567'
})

print(f"Scan completed: {scan_result['detections']} detections found")
print(f"Risk score: {scan_result['risk_score']}")

# Run security assessment
assessment = client.run_security_assessment()
print(f"Security status: {assessment['overall_status']}")

# Check GDPR compliance
gdpr_status = client.check_compliance('gdpr')
print(f"GDPR compliance rate: {gdpr_status['framework_compliance']['gdpr']['compliance_rate']}")
```

### JavaScript WebSocket Client Example

```javascript
class SecurityWebSocketClient {
    constructor(wsUrl, accessToken) {
        this.wsUrl = wsUrl;
        this.accessToken = accessToken;
        this.ws = null;
        this.subscriptions = new Set();
    }
    
    connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket(`${this.wsUrl}?token=${this.accessToken}`);
            
            this.ws.onopen = () => {
                console.log('WebSocket connected');
                resolve();
            };
            
            this.ws.onmessage = (event) => {
                const message = JSON.parse(event.data);
                this.handleMessage(message);
            };
            
            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
                reject(error);
            };
            
            this.ws.onclose = () => {
                console.log('WebSocket disconnected');
                this.reconnect();
            };
        });
    }
    
    subscribe(topic) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                type: 'subscribe',
                data: { topic }
            }));
            this.subscriptions.add(topic);
        }
    }
    
    unsubscribe(topic) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                type: 'unsubscribe',
                data: { topic }
            }));
            this.subscriptions.delete(topic);
        }
    }
    
    handleMessage(message) {
        switch (message.type) {
            case 'welcome':
                console.log('Connected as:', message.data.user_id);
                // Re-subscribe to previous topics
                this.subscriptions.forEach(topic => this.subscribe(topic));
                break;
                
            case 'security_alert':
                this.onSecurityAlert(message.data);
                break;
                
            case 'dlp_incident':
                this.onDLPIncident(message.data);
                break;
                
            case 'compliance_violation':
                this.onComplianceViolation(message.data);
                break;
                
            case 'metrics_update':
                this.onMetricsUpdate(message.data);
                break;
                
            case 'heartbeat':
                // Handle heartbeat
                break;
                
            case 'error':
                console.error('WebSocket error:', message.data);
                break;
        }
    }
    
    onSecurityAlert(alert) {
        console.warn('Security Alert:', alert);
        // Update UI, show notification, etc.
    }
    
    onDLPIncident(incident) {
        console.warn('DLP Incident:', incident);
        // Handle DLP incident
    }
    
    onComplianceViolation(violation) {
        console.error('Compliance Violation:', violation);
        // Handle compliance violation
    }
    
    onMetricsUpdate(metrics) {
        console.log('Metrics updated:', metrics);
        // Update dashboard widgets
    }
    
    reconnect() {
        setTimeout(() => {
            console.log('Attempting to reconnect...');
            this.connect();
        }, 5000);
    }
}

// Usage
const wsClient = new SecurityWebSocketClient(
    'wss://api.company.com/ws/security/dashboard',
    'your_access_token_here'
);

wsClient.connect().then(() => {
    // Subscribe to security events
    wsClient.subscribe('security_alerts');
    wsClient.subscribe('dlp_incidents');
    wsClient.subscribe('dashboard_metrics');
});
```

### cURL Examples

#### Scan Data for Sensitive Content
```bash
curl -X POST "https://api.company.com/api/v1/security/scan" \
  -H "Authorization: Bearer your_access_token" \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "customer_email": "john.doe@example.com",
      "ssn": "123-45-6789"
    },
    "context": {
      "location": "api_endpoint",
      "operation": "export"
    }
  }'
```

#### Run Security Assessment
```bash
curl -X POST "https://api.company.com/api/v1/security/assess" \
  -H "Authorization: Bearer your_access_token" \
  -H "Content-Type: application/json" \
  -d '{
    "assessment_type": "comprehensive"
  }'
```

#### Get Compliance Dashboard
```bash
curl -X GET "https://api.company.com/api/v1/compliance/dashboard?framework=gdpr" \
  -H "Authorization: Bearer your_access_token"
```

---

## Advanced Usage Patterns

### 1. Batch Processing with Security Framework

#### Bulk Data Scanning
```python
import asyncio
import aiohttp
from typing import List, Dict, Any

class BulkSecurityProcessor:
    def __init__(self, base_url: str, access_token: str):
        self.base_url = base_url
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
    
    async def bulk_scan_data(self, data_items: List[Dict[str, Any]], batch_size: int = 10) -> List[Dict[str, Any]]:
        """Process data items in batches for DLP scanning"""
        results = []
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(data_items), batch_size):
                batch = data_items[i:i + batch_size]
                batch_results = await asyncio.gather(
                    *[self._scan_single_item(session, item) for item in batch],
                    return_exceptions=True
                )
                results.extend(batch_results)
        
        return results
    
    async def _scan_single_item(self, session: aiohttp.ClientSession, item: Dict[str, Any]) -> Dict[str, Any]:
        """Scan a single data item"""
        async with session.post(
            f'{self.base_url}/api/v1/security/scan',
            headers=self.headers,
            json={'data': item, 'context': {'batch_processing': True}}
        ) as response:
            return await response.json()

# Usage example
processor = BulkSecurityProcessor('https://api.company.com', 'your_token')
data_items = [
    {'customer_email': 'user1@example.com', 'phone': '555-0001'},
    {'customer_email': 'user2@example.com', 'phone': '555-0002'},
    # ... more items
]

results = await processor.bulk_scan_data(data_items)
```

#### Batch Compliance Assessment
```python
class ComplianceBatchProcessor:
    def __init__(self, base_url: str, access_token: str):
        self.base_url = base_url
        self.headers = {'Authorization': f'Bearer {access_token}'}
    
    async def assess_multiple_controls(self, framework: str, control_ids: List[str]) -> Dict[str, Any]:
        """Assess multiple compliance controls simultaneously"""
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for control_id in control_ids:
                task = self._assess_single_control(session, framework, control_id)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Aggregate results
            summary = {
                'framework': framework,
                'total_controls': len(control_ids),
                'compliant': sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'compliant'),
                'non_compliant': sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'non_compliant'),
                'errors': sum(1 for r in results if isinstance(r, Exception)),
                'details': results
            }
            
            return summary
    
    async def _assess_single_control(self, session: aiohttp.ClientSession, framework: str, control_id: str):
        """Assess a single compliance control"""
        try:
            async with session.post(
                f'{self.base_url}/api/v1/compliance/assess',
                headers=self.headers,
                json={
                    'framework': framework,
                    'control_ids': [control_id]
                }
            ) as response:
                result = await response.json()
                return result.get('assessment_results', [{}])[0]
        except Exception as e:
            return {'control_id': control_id, 'error': str(e)}
```

### 2. Event-Driven Security Integration

#### WebSocket Event Handler
```javascript
class SecurityEventProcessor {
    constructor(wsUrl, accessToken) {
        this.wsUrl = wsUrl;
        this.accessToken = accessToken;
        this.ws = null;
        this.eventHandlers = new Map();
        this.messageQueue = [];
        this.isConnected = false;
    }
    
    // Register event handlers
    on(eventType, handler) {
        if (!this.eventHandlers.has(eventType)) {
            this.eventHandlers.set(eventType, []);
        }
        this.eventHandlers.get(eventType).push(handler);
    }
    
    // Connect and set up event processing
    async connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket(`${this.wsUrl}?token=${this.accessToken}`);
            
            this.ws.onopen = () => {
                this.isConnected = true;
                this.processQueuedMessages();
                resolve();
            };
            
            this.ws.onmessage = (event) => {
                const message = JSON.parse(event.data);
                this.processSecurityEvent(message);
            };
            
            this.ws.onerror = reject;
            this.ws.onclose = () => {
                this.isConnected = false;
                this.reconnect();
            };
        });
    }
    
    processSecurityEvent(event) {
        const handlers = this.eventHandlers.get(event.type) || [];
        handlers.forEach(handler => {
            try {
                handler(event.data, event.metadata);
            } catch (error) {
                console.error(`Error processing ${event.type}:`, error);
            }
        });
    }
    
    // Automatic security response integration
    setupAutoResponse() {
        // High-severity threat detection
        this.on('security_alert', (alert, metadata) => {
            if (alert.severity === 'critical') {
                this.executeEmergencyResponse(alert);
            }
        });
        
        // DLP incident handling
        this.on('dlp_incident', (incident, metadata) => {
            this.handleDLPIncident(incident);
        });
        
        // Compliance violation response
        this.on('compliance_violation', (violation, metadata) => {
            this.createRemediationTask(violation);
        });
    }
    
    async executeEmergencyResponse(alert) {
        // Implement automated threat response
        console.log('Executing emergency response for:', alert);
        
        // Block IP if applicable
        if (alert.source_ip) {
            await this.blockIP(alert.source_ip);
        }
        
        // Disable user if account compromise suspected
        if (alert.user_id && alert.category === 'account_compromise') {
            await this.disableUser(alert.user_id);
        }
        
        // Alert security team
        await this.alertSecurityTeam(alert);
    }
}

// Usage example
const processor = new SecurityEventProcessor(
    'wss://api.company.com/ws/security/dashboard',
    'your_access_token'
);

// Set up event handlers
processor.on('security_alert', (alert) => {
    console.log('Security Alert:', alert);
    updateSecurityDashboard(alert);
});

processor.on('dlp_incident', (incident) => {
    console.log('DLP Incident:', incident);
    logDataProtectionEvent(incident);
});

// Connect and enable auto-response
await processor.connect();
processor.setupAutoResponse();
```

### 3. SDK Integration Examples

#### Python Security SDK
```python
class SecurityFrameworkSDK:
    def __init__(self, base_url: str, access_token: str):
        self.client = SecurityAPIClient(base_url, access_token)
        self.websocket = SecurityWebSocketClient(
            base_url.replace('http', 'ws') + '/ws/security/dashboard',
            access_token
        )
    
    async def __aenter__(self):
        await self.websocket.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.websocket.ws:
            await self.websocket.ws.close()
    
    # High-level security operations
    async def secure_data_operation(self, operation_type: str, data: dict, user_context: dict) -> dict:
        """Perform a data operation with full security pipeline"""
        
        # 1. Pre-operation security check
        access_check = await self.client.check_access(
            user_context['user_id'],
            operation_type,
            user_context.get('resource_id'),
            user_context
        )
        
        if not access_check['access_decision']['allowed']:
            return {
                'status': 'denied',
                'reason': access_check['access_decision']['reason'],
                'operation': operation_type
            }
        
        # 2. DLP scanning
        if operation_type in ['export', 'download', 'send']:
            scan_result = await self.client.scan_data(data, user_context)
            
            if scan_result['action'] == 'blocked':
                return {
                    'status': 'blocked',
                    'reason': 'Data protection policy violation',
                    'scan_result': scan_result
                }
            
            # Use redacted data if available
            if scan_result['action'] == 'redacted':
                data = scan_result['data']
        
        # 3. Compliance validation
        if user_context.get('compliance_required'):
            compliance_check = await self.client.check_compliance(
                user_context['compliance_framework']
            )
            
            if compliance_check['compliance_rate'] < 0.9:
                return {
                    'status': 'compliance_warning',
                    'message': 'Compliance score below threshold',
                    'compliance_status': compliance_check
                }
        
        # 4. Execute operation (placeholder)
        result = await self._execute_operation(operation_type, data, user_context)
        
        # 5. Audit logging
        await self._log_operation(operation_type, user_context, result)
        
        return {
            'status': 'success',
            'result': result,
            'security_metadata': {
                'access_granted': True,
                'dlp_scanned': True,
                'compliance_checked': user_context.get('compliance_required', False)
            }
        }
    
    async def _execute_operation(self, operation_type: str, data: dict, context: dict):
        """Placeholder for actual business operation"""
        # Implement your business logic here
        return {'operation': operation_type, 'processed': True}
    
    async def _log_operation(self, operation_type: str, context: dict, result: dict):
        """Log the operation for audit purposes"""
        # Implement audit logging
        pass

# Usage example
async def process_customer_export(customer_data, user_id):
    async with SecurityFrameworkSDK('https://api.company.com', access_token) as security:
        result = await security.secure_data_operation(
            operation_type='export',
            data=customer_data,
            user_context={
                'user_id': user_id,
                'resource_id': 'customer_database',
                'compliance_framework': 'gdpr',
                'compliance_required': True
            }
        )
        
        if result['status'] == 'success':
            print("Export completed successfully")
            return result['result']
        else:
            print(f"Export failed: {result['reason']}")
            return None
```

#### Node.js Security Middleware
```javascript
const express = require('express');
const { SecurityFrameworkClient } = require('./security-sdk');

class SecurityMiddleware {
    constructor(baseUrl, accessToken) {
        this.client = new SecurityFrameworkClient(baseUrl, accessToken);
    }
    
    // Express middleware for automatic security scanning
    scanRequestData() {
        return async (req, res, next) => {
            try {
                // Skip scanning for GET requests
                if (req.method === 'GET') {
                    return next();
                }
                
                // Scan request body for sensitive data
                if (req.body && Object.keys(req.body).length > 0) {
                    const scanResult = await this.client.scanData(req.body, {
                        location: 'api_request',
                        endpoint: req.path,
                        user_id: req.user?.id,
                        operation: req.method.toLowerCase()
                    });
                    
                    if (scanResult.action === 'blocked') {
                        return res.status(403).json({
                            error: 'Request blocked by data protection policy',
                            scan_id: scanResult.scan_id,
                            message: 'Your request contains sensitive data that violates our data protection policies'
                        });
                    }
                    
                    // Store scan result for potential response filtering
                    req.securityScan = scanResult;
                }
                
                next();
            } catch (error) {
                console.error('Security scan error:', error);
                // In case of security service failure, you might want to fail closed
                res.status(503).json({
                    error: 'Security service unavailable',
                    message: 'Please try again later'
                });
            }
        };
    }
    
    // Middleware for access control
    checkAccess(resource, action) {
        return async (req, res, next) => {
            try {
                const accessResult = await this.client.checkAccess(
                    req.user.id,
                    action,
                    resource,
                    {
                        ip_address: req.ip,
                        user_agent: req.get('User-Agent'),
                        time: new Date().toISOString()
                    }
                );
                
                if (!accessResult.access_decision.allowed) {
                    return res.status(403).json({
                        error: 'Access denied',
                        reason: accessResult.access_decision.reason,
                        message: 'You do not have permission to perform this action'
                    });
                }
                
                req.accessDecision = accessResult;
                next();
            } catch (error) {
                console.error('Access control error:', error);
                res.status(503).json({
                    error: 'Access control service unavailable'
                });
            }
        };
    }
    
    // Response filtering middleware
    filterResponseData() {
        return (req, res, next) => {
            const originalJson = res.json;
            
            res.json = async function(data) {
                try {
                    // Apply DLP filtering to response if scan indicated redaction needed
                    if (req.securityScan && req.securityScan.action === 'processed') {
                        // Apply same redaction patterns to response data
                        const responseFilter = await this.client.scanData(data, {
                            location: 'api_response',
                            endpoint: req.path,
                            user_id: req.user?.id,
                            operation: 'response'
                        });
                        
                        if (responseFilter.action === 'redacted') {
                            data = responseFilter.data;
                        }
                    }
                    
                    originalJson.call(this, data);
                } catch (error) {
                    console.error('Response filtering error:', error);
                    originalJson.call(this, data);
                }
            }.bind(this);
            
            next();
        };
    }
}

// Usage in Express app
const app = express();
const security = new SecurityMiddleware('https://api.company.com', process.env.SECURITY_TOKEN);

// Apply security middleware globally
app.use(security.scanRequestData());
app.use(security.filterResponseData());

// Apply access control to specific routes
app.get('/api/customers', 
    security.checkAccess('customer_data', 'read'),
    (req, res) => {
        // Your route handler
        res.json({ customers: [] });
    }
);

app.post('/api/customers/export',
    security.checkAccess('customer_data', 'export'),
    (req, res) => {
        // Your export handler
        res.json({ export_id: 'exp123' });
    }
);
```

### 4. Custom Integration Patterns

#### Message Queue Integration
```python
import asyncio
import aioredis
import json
from typing import Dict, Any

class SecurityMessageProcessor:
    def __init__(self, redis_url: str, security_api_client):
        self.redis_url = redis_url
        self.security_client = security_api_client
        
    async def process_security_queue(self, queue_name: str = 'security_events'):
        """Process security events from Redis queue"""
        
        redis = await aioredis.from_url(self.redis_url)
        
        while True:
            try:
                # Pop message from queue
                message = await redis.blpop(queue_name, timeout=5)
                
                if message:
                    _, event_data = message
                    event = json.loads(event_data.decode('utf-8'))
                    
                    await self.process_security_event(event)
                    
            except Exception as e:
                print(f"Error processing security queue: {e}")
                await asyncio.sleep(5)
    
    async def process_security_event(self, event: Dict[str, Any]):
        """Process individual security event"""
        
        event_type = event.get('type')
        
        if event_type == 'data_access':
            await self.handle_data_access_event(event)
        elif event_type == 'user_login':
            await self.handle_login_event(event)
        elif event_type == 'compliance_check':
            await self.handle_compliance_event(event)
    
    async def handle_data_access_event(self, event):
        """Handle data access security checking"""
        
        # Perform DLP scan on accessed data
        if event.get('data_content'):
            scan_result = await self.security_client.scan_data(
                event['data_content'],
                {'user_id': event.get('user_id'), 'location': 'message_queue'}
            )
            
            if scan_result['policy_violations'] > 0:
                await self.alert_dlp_violation(event, scan_result)
    
    async def alert_dlp_violation(self, event, scan_result):
        """Alert on DLP policy violations"""
        print(f"DLP violation detected: {scan_result}")
        # Implement your alerting logic
```

### 5. Performance Optimization Patterns

#### Caching Security Decisions
```python
import asyncio
from functools import wraps
import hashlib
import json
import time
from typing import Dict, Any, Callable

class SecurityCacheManager:
    def __init__(self, redis_client, default_ttl: int = 300):
        self.redis = redis_client
        self.default_ttl = default_ttl
    
    def cache_security_decision(self, ttl: int = None):
        """Decorator to cache security API responses"""
        
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Generate cache key from function name and arguments
                cache_key = self._generate_cache_key(func.__name__, args, kwargs)
                
                # Try to get from cache
                cached_result = await self.redis.get(cache_key)
                if cached_result:
                    return json.loads(cached_result.decode('utf-8'))
                
                # Execute function and cache result
                result = await func(*args, **kwargs)
                
                await self.redis.setex(
                    cache_key,
                    ttl or self.default_ttl,
                    json.dumps(result, default=str)
                )
                
                return result
                
            return wrapper
        return decorator
    
    def _generate_cache_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Generate cache key from function call parameters"""
        
        # Create deterministic string representation
        key_data = {
            'function': func_name,
            'args': args,
            'kwargs': sorted(kwargs.items())  # Sort for deterministic key
        }
        
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        return f"security_cache:{hashlib.sha256(key_string.encode()).hexdigest()}"

# Usage with security client
class CachedSecurityClient(SecurityAPIClient):
    def __init__(self, base_url: str, access_token: str, redis_client):
        super().__init__(base_url, access_token)
        self.cache_manager = SecurityCacheManager(redis_client)
    
    @SecurityCacheManager.cache_security_decision(ttl=600)  # Cache for 10 minutes
    async def check_user_permissions_cached(self, user_id: str) -> Dict[str, Any]:
        """Cached version of user permission check"""
        return await self.check_access(user_id, 'read', 'system')
    
    @SecurityCacheManager.cache_security_decision(ttl=1800)  # Cache for 30 minutes
    async def get_compliance_status_cached(self, framework: str) -> Dict[str, Any]:
        """Cached version of compliance status"""
        return await self.check_compliance(framework)
```

---

This API documentation provides comprehensive coverage of the security and compliance framework APIs with practical examples for integration and development.