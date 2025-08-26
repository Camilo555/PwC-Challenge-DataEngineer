# Enterprise Security Developer Guide

## Overview

This guide provides comprehensive instructions for developers integrating with the enterprise security and compliance framework. It covers security middleware integration, API security best practices, testing framework usage, and custom security policy development.

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Security Middleware Integration](#security-middleware-integration)
3. [API Security Best Practices](#api-security-best-practices)
4. [Security Testing Framework](#security-testing-framework)
5. [Custom Security Policy Development](#custom-security-policy-development)
6. [DLP Integration Guide](#dlp-integration-guide)
7. [Compliance Framework Extension](#compliance-framework-extension)

---

## Development Environment Setup

### Local Development Environment

#### Prerequisites
```bash
# Install Python 3.11+
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip

# Install development tools
sudo apt install -y git curl postgresql-client redis-tools

# Install Docker for containerized development
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
```

#### Project Setup
```bash
# Clone the repository
git clone https://github.com/company/security-platform.git
cd security-platform

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

#### Environment Configuration
```bash
# Create local environment file
cat > .env.local << 'EOF'
# Database Configuration
DATABASE_URL=postgresql://security_user:password@localhost:5432/security_dev

# Redis Configuration  
REDIS_URL=redis://localhost:6379/0

# Security Configuration
JWT_SECRET_KEY=dev-jwt-secret-key-change-in-production
ENCRYPTION_KEY=dev-encryption-key-change-in-production

# Development Settings
DEBUG=true
LOG_LEVEL=DEBUG
ENVIRONMENT=development

# External Services
PROMETHEUS_URL=http://localhost:9090
GRAFANA_URL=http://localhost:3000
EOF
```

#### Docker Development Setup
```yaml
# docker-compose.dev.yml
version: '3.8'
services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: security_dev
      POSTGRES_USER: security_user
      POSTGRES_PASSWORD: dev_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  security-api:
    build: .
    environment:
      - DATABASE_URL=postgresql://security_user:dev_password@postgres:5432/security_dev
      - REDIS_URL=redis://redis:6379/0
    ports:
      - "8000:8000"
    depends_on:
      - postgres
      - redis
    volumes:
      - .:/app
      - /app/venv

volumes:
  postgres_data:
```

### IDE Configuration

#### VS Code Settings
```json
{
  "python.defaultInterpreterPath": "./venv/bin/python",
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": false,
  "python.linting.flake8Enabled": true,
  "python.linting.mypyEnabled": true,
  "python.formatting.provider": "black",
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": [
    "tests"
  ],
  "files.exclude": {
    "**/__pycache__": true,
    "**/*.pyc": true,
    ".pytest_cache": true,
    ".mypy_cache": true
  }
}
```

#### PyCharm Configuration
```python
# In PyCharm settings.py for the project
import os
import sys

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Security framework configuration
SECURITY_CONFIG = {
    'development': True,
    'testing': False,
    'debug_mode': True
}
```

---

## Security Middleware Integration

### FastAPI Security Middleware

#### Basic Integration
```python
# src/api/middleware/security_integration.py
from fastapi import FastAPI, Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable, Awaitable

from core.security.enterprise_security_orchestrator import get_security_orchestrator
from core.logging import get_logger

logger = get_logger(__name__)

class SecurityMiddleware(BaseHTTPMiddleware):
    """Enterprise security middleware for FastAPI applications"""
    
    def __init__(self, app: FastAPI, excluded_paths: list = None):
        super().__init__(app)
        self.security_orchestrator = get_security_orchestrator()
        self.excluded_paths = excluded_paths or ['/health', '/metrics', '/docs']
    
    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]):
        """Process request through security pipeline"""
        
        # Skip security checks for excluded paths
        if request.url.path in self.excluded_paths:
            return await call_next(request)
        
        # Extract request context
        context = await self._extract_request_context(request)
        
        try:
            # Process request through security orchestrator
            if request.method in ['POST', 'PUT', 'PATCH']:
                # Get request body for security scanning
                body = await self._get_request_body(request)
                
                if body:
                    # Scan request data for security issues
                    security_result = await self.security_orchestrator.process_data_request(
                        user_id=context.get('user_id', 'anonymous'),
                        data=body,
                        operation=request.method.lower(),
                        resource_id=request.url.path,
                        context=context
                    )
                    
                    # Handle security violations
                    if security_result['status'] in ['denied', 'blocked']:
                        raise HTTPException(
                            status_code=403,
                            detail={
                                'error': 'Security policy violation',
                                'reason': security_result['reason'],
                                'request_id': security_result['request_id']
                            }
                        )
            
            # Process request
            response = await call_next(request)
            
            # Scan response data if needed
            if hasattr(response, 'body') and response.headers.get('content-type', '').startswith('application/json'):
                response = await self._scan_response(response, context)
            
            return response
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Security middleware error: {e}")
            raise HTTPException(status_code=500, detail="Internal security error")
    
    async def _extract_request_context(self, request: Request) -> dict:
        """Extract security context from request"""
        return {
            'user_id': getattr(request.state, 'user_id', None),
            'session_id': getattr(request.state, 'session_id', None),
            'ip_address': request.client.host,
            'user_agent': request.headers.get('user-agent'),
            'location': 'api',
            'timestamp': datetime.now().isoformat()
        }
    
    async def _get_request_body(self, request: Request) -> dict:
        """Safely extract request body"""
        try:
            if request.headers.get('content-type', '').startswith('application/json'):
                body = await request.json()
                return body
        except:
            pass
        return None
    
    async def _scan_response(self, response, context: dict):
        """Scan response data for sensitive information"""
        # Implementation for response scanning
        return response

# Integration with FastAPI app
def setup_security_middleware(app: FastAPI):
    """Setup security middleware for FastAPI application"""
    
    app.add_middleware(
        SecurityMiddleware,
        excluded_paths=['/health', '/metrics', '/docs', '/openapi.json']
    )
    
    return app
```

#### Advanced Security Middleware
```python
# src/api/middleware/advanced_security.py
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
import time
import hashlib
from collections import defaultdict, deque

class AdvancedSecurityMiddleware(BaseHTTPMiddleware):
    """Advanced security middleware with rate limiting, CSRF protection, and more"""
    
    def __init__(self, app, config: dict = None):
        super().__init__(app)
        self.config = config or self._get_default_config()
        self.rate_limiter = self._init_rate_limiter()
        self.csrf_tokens = {}
    
    def _get_default_config(self) -> dict:
        return {
            'rate_limiting': {
                'requests_per_minute': 100,
                'burst_size': 20
            },
            'csrf_protection': {
                'enabled': True,
                'token_header': 'X-CSRF-Token'
            },
            'security_headers': {
                'strict_transport_security': 'max-age=31536000; includeSubDomains',
                'x_content_type_options': 'nosniff',
                'x_frame_options': 'DENY',
                'x_xss_protection': '1; mode=block'
            },
            'content_security_policy': "default-src 'self'; script-src 'self' 'unsafe-inline'"
        }
    
    async def dispatch(self, request: Request, call_next):
        """Advanced security processing"""
        
        # Rate limiting
        if not await self._check_rate_limit(request):
            return Response(
                content='{"error": "Rate limit exceeded"}',
                status_code=429,
                media_type='application/json'
            )
        
        # CSRF protection for state-changing requests
        if request.method in ['POST', 'PUT', 'DELETE', 'PATCH']:
            if not await self._verify_csrf_token(request):
                return Response(
                    content='{"error": "CSRF token validation failed"}',
                    status_code=403,
                    media_type='application/json'
                )
        
        # Process request
        response = await call_next(request)
        
        # Add security headers
        response = self._add_security_headers(response)
        
        return response
    
    async def _check_rate_limit(self, request: Request) -> bool:
        """Check rate limiting for client"""
        
        client_id = self._get_client_identifier(request)
        current_time = time.time()
        
        # Get client's request history
        if client_id not in self.rate_limiter:
            self.rate_limiter[client_id] = deque()
        
        client_requests = self.rate_limiter[client_id]
        
        # Remove old requests (outside time window)
        while client_requests and current_time - client_requests[0] > 60:
            client_requests.popleft()
        
        # Check if limit exceeded
        if len(client_requests) >= self.config['rate_limiting']['requests_per_minute']:
            return False
        
        # Add current request
        client_requests.append(current_time)
        return True
    
    def _get_client_identifier(self, request: Request) -> str:
        """Get unique client identifier for rate limiting"""
        
        # Try to get user ID first
        if hasattr(request.state, 'user_id') and request.state.user_id:
            return f"user:{request.state.user_id}"
        
        # Fall back to IP address
        return f"ip:{request.client.host}"
    
    async def _verify_csrf_token(self, request: Request) -> bool:
        """Verify CSRF token for state-changing requests"""
        
        if not self.config['csrf_protection']['enabled']:
            return True
        
        # Get token from header
        token_header = self.config['csrf_protection']['token_header']
        provided_token = request.headers.get(token_header)
        
        if not provided_token:
            return False
        
        # Get session token (this would integrate with your session management)
        session_id = getattr(request.state, 'session_id', None)
        if not session_id:
            return False
        
        expected_token = self.csrf_tokens.get(session_id)
        return provided_token == expected_token
    
    def _add_security_headers(self, response: Response) -> Response:
        """Add security headers to response"""
        
        headers = self.config['security_headers']
        
        for header_name, header_value in headers.items():
            response.headers[header_name.replace('_', '-')] = header_value
        
        # Add Content Security Policy
        if 'content_security_policy' in self.config:
            response.headers['Content-Security-Policy'] = self.config['content_security_policy']
        
        return response
```

### Authentication Integration

#### JWT Authentication
```python
# src/api/middleware/auth_integration.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timedelta

from api.v1.services.enhanced_auth_service import get_auth_service

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Extract and validate current user from JWT token"""
    
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Decode JWT token
        auth_service = get_auth_service()
        payload = auth_service.decode_access_token(credentials.credentials)
        
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
            
        # Get user from database
        user = await auth_service.get_user_by_id(user_id)
        if user is None:
            raise credentials_exception
            
        return user
        
    except JWTError:
        raise credentials_exception

async def require_permissions(*required_permissions: str):
    """Dependency factory for permission-based authorization"""
    
    def permission_dependency(current_user = Depends(get_current_user)):
        """Check if user has required permissions"""
        
        user_permissions = set(current_user.permissions)
        missing_permissions = set(required_permissions) - user_permissions
        
        if missing_permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions. Required: {', '.join(missing_permissions)}"
            )
        
        return current_user
    
    return permission_dependency

# Usage in API endpoints
from fastapi import APIRouter

router = APIRouter()

@router.get("/security/dashboard")
async def get_security_dashboard(
    current_user = Depends(require_permissions("perm_admin_system"))
):
    """Get security dashboard (requires admin permissions)"""
    # Implementation here
    pass

@router.post("/security/scan")
async def scan_data(
    data: dict,
    current_user = Depends(require_permissions("perm_security_scan"))
):
    """Scan data for security issues"""
    # Implementation here
    pass
```

### WebSocket Security Integration

#### Secure WebSocket Authentication
```python
# src/api/websocket/secure_websocket.py
from fastapi import WebSocket, WebSocketDisconnect, HTTPException
from jose import jwt, JWTError
from typing import Optional

from api.websocket.security_dashboard import get_websocket_manager
from core.logging import get_logger

logger = get_logger(__name__)

class SecureWebSocketHandler:
    """Secure WebSocket handler with authentication and authorization"""
    
    def __init__(self):
        self.websocket_manager = get_websocket_manager()
    
    async def handle_websocket_connection(
        self, 
        websocket: WebSocket, 
        token: Optional[str] = None
    ):
        """Handle secure WebSocket connection"""
        
        try:
            # Accept connection first
            await websocket.accept()
            
            # Authenticate connection
            if not token:
                # Try to get token from query parameters or initial message
                token = await self._get_token_from_websocket(websocket)
            
            if not token:
                await self._send_auth_error(websocket, "Authentication token required")
                await websocket.close(code=1008, reason="Authentication required")
                return
            
            # Validate token
            user_info = await self._validate_websocket_token(token)
            if not user_info:
                await self._send_auth_error(websocket, "Invalid authentication token")
                await websocket.close(code=1008, reason="Invalid token")
                return
            
            # Establish secure connection
            connection = await self.websocket_manager.connect_client(
                websocket, token
            )
            
            # Handle messages
            await self._handle_websocket_messages(connection)
            
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            try:
                await websocket.close(code=1011, reason="Server error")
            except:
                pass
    
    async def _get_token_from_websocket(self, websocket: WebSocket) -> Optional[str]:
        """Get authentication token from WebSocket"""
        
        try:
            # Wait for authentication message
            message = await websocket.receive_json()
            
            if message.get('type') == 'auth':
                return message.get('data', {}).get('token')
                
        except Exception as e:
            logger.error(f"Error getting token from WebSocket: {e}")
        
        return None
    
    async def _validate_websocket_token(self, token: str) -> Optional[dict]:
        """Validate WebSocket authentication token"""
        
        try:
            from api.v1.services.enhanced_auth_service import get_auth_service
            
            auth_service = get_auth_service()
            payload = auth_service.decode_access_token(token)
            
            return {
                'user_id': payload.get('sub'),
                'permissions': payload.get('permissions', [])
            }
            
        except JWTError:
            return None
    
    async def _send_auth_error(self, websocket: WebSocket, message: str):
        """Send authentication error to WebSocket client"""
        
        try:
            await websocket.send_json({
                'type': 'auth_error',
                'message': message,
                'timestamp': datetime.now().isoformat()
            })
        except:
            pass

# FastAPI WebSocket endpoint
@app.websocket("/ws/security/dashboard")
async def websocket_security_dashboard(
    websocket: WebSocket,
    token: Optional[str] = None
):
    """Secure WebSocket endpoint for security dashboard"""
    
    handler = SecureWebSocketHandler()
    await handler.handle_websocket_connection(websocket, token)
```

---

## API Security Best Practices

### Input Validation and Sanitization

#### Pydantic Model Validation
```python
# src/api/v1/schemas/secure_models.py
from pydantic import BaseModel, Field, validator
from typing import Optional, List
import re

class SecureDataRequest(BaseModel):
    """Secure data request model with validation"""
    
    data: dict = Field(..., description="Data to be processed")
    context: Optional[dict] = Field(default_factory=dict, description="Request context")
    
    @validator('data')
    def validate_data_structure(cls, v):
        """Validate data structure"""
        if not isinstance(v, dict):
            raise ValueError('Data must be a dictionary')
        
        # Check for potentially malicious keys
        malicious_patterns = [
            r'__.*__',  # Python dunder methods
            r'eval|exec|import|subprocess',  # Dangerous functions
            r'<script|javascript:|data:',  # XSS patterns
        ]
        
        for key in v.keys():
            if any(re.search(pattern, str(key), re.IGNORECASE) for pattern in malicious_patterns):
                raise ValueError(f'Potentially unsafe key detected: {key}')
        
        return v
    
    @validator('context')
    def validate_context(cls, v):
        """Validate context data"""
        if v is None:
            return {}
        
        # Limit context size
        if len(str(v)) > 10000:
            raise ValueError('Context data too large')
        
        return v

class SecureUserRequest(BaseModel):
    """Secure user management request"""
    
    username: str = Field(..., min_length=3, max_length=50, regex=r'^[a-zA-Z0-9_.-]+$')
    email: str = Field(..., regex=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    password: str = Field(..., min_length=12)
    permissions: List[str] = Field(default_factory=list)
    
    @validator('password')
    def validate_password_strength(cls, v):
        """Validate password strength"""
        if len(v) < 12:
            raise ValueError('Password must be at least 12 characters long')
        
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one digit')
        
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        
        return v
    
    @validator('permissions')
    def validate_permissions(cls, v):
        """Validate permission list"""
        valid_permissions = [
            'perm_admin_system',
            'perm_audit_logs',
            'perm_compliance_report',
            'perm_admin_users',
            'perm_security_scan',
            'perm_dlp_management'
        ]
        
        for perm in v:
            if perm not in valid_permissions:
                raise ValueError(f'Invalid permission: {perm}')
        
        return v
```

#### SQL Injection Prevention
```python
# src/data_access/secure_queries.py
from sqlalchemy import text
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class SecureQueryBuilder:
    """Secure query builder to prevent SQL injection"""
    
    def __init__(self, db_session):
        self.db_session = db_session
    
    def build_secure_query(
        self, 
        base_query: str, 
        filters: Dict[str, Any],
        allowed_columns: List[str]
    ) -> str:
        """Build secure parameterized query"""
        
        # Validate column names against whitelist
        for column in filters.keys():
            if column not in allowed_columns:
                raise ValueError(f"Column '{column}' not allowed")
        
        # Build parameterized query
        where_clauses = []
        params = {}
        
        for column, value in filters.items():
            if value is not None:
                param_name = f"param_{column}"
                where_clauses.append(f"{column} = :{param_name}")
                params[param_name] = value
        
        if where_clauses:
            query = f"{base_query} WHERE {' AND '.join(where_clauses)}"
        else:
            query = base_query
        
        return text(query), params
    
    async def execute_secure_query(
        self, 
        query: str, 
        params: Dict[str, Any]
    ) -> List[Dict]:
        """Execute secure parameterized query"""
        
        try:
            result = await self.db_session.execute(query, params)
            return [dict(row) for row in result]
        
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise

# Usage example
async def get_security_events(
    db_session, 
    user_id: str = None, 
    event_type: str = None,
    start_date: datetime = None
):
    """Get security events with secure filtering"""
    
    query_builder = SecureQueryBuilder(db_session)
    
    base_query = """
        SELECT event_id, user_id, event_type, timestamp, description
        FROM security_events
        ORDER BY timestamp DESC
        LIMIT 100
    """
    
    filters = {}
    if user_id:
        filters['user_id'] = user_id
    if event_type:
        filters['event_type'] = event_type
    if start_date:
        filters['timestamp'] = start_date
    
    allowed_columns = ['user_id', 'event_type', 'timestamp']
    
    query, params = query_builder.build_secure_query(
        base_query, filters, allowed_columns
    )
    
    return await query_builder.execute_secure_query(query, params)
```

### API Response Security

#### Response Data Sanitization
```python
# src/api/middleware/response_security.py
from fastapi import Response
from typing import Any, Dict
import json
import re

class ResponseSecurityProcessor:
    """Process API responses for security"""
    
    def __init__(self):
        self.sensitive_patterns = [
            r'password',
            r'secret',
            r'token',
            r'key',
            r'credential'
        ]
    
    def sanitize_response_data(self, data: Any) -> Any:
        """Sanitize response data to remove sensitive information"""
        
        if isinstance(data, dict):
            return self._sanitize_dict(data)
        elif isinstance(data, list):
            return [self.sanitize_response_data(item) for item in data]
        else:
            return data
    
    def _sanitize_dict(self, data: Dict) -> Dict:
        """Sanitize dictionary data"""
        
        sanitized = {}
        
        for key, value in data.items():
            # Check if key contains sensitive information
            if self._is_sensitive_key(key):
                sanitized[key] = "[REDACTED]"
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_dict(value)
            elif isinstance(value, list):
                sanitized[key] = [self.sanitize_response_data(item) for item in value]
            else:
                sanitized[key] = value
        
        return sanitized
    
    def _is_sensitive_key(self, key: str) -> bool:
        """Check if key contains sensitive information"""
        
        key_lower = key.lower()
        return any(pattern in key_lower for pattern in self.sensitive_patterns)
    
    def add_security_headers(self, response: Response) -> Response:
        """Add security headers to response"""
        
        # Remove server information
        response.headers.pop('server', None)
        
        # Add security headers
        security_headers = {
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block',
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
            'Referrer-Policy': 'strict-origin-when-cross-origin'
        }
        
        for header, value in security_headers.items():
            response.headers[header] = value
        
        return response

# Middleware integration
class ResponseSecurityMiddleware:
    """Middleware to process response security"""
    
    def __init__(self, app):
        self.app = app
        self.processor = ResponseSecurityProcessor()
    
    async def __call__(self, scope, receive, send):
        """Process response through security pipeline"""
        
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        async def send_wrapper(message):
            if message["type"] == "http.response.body":
                # Process response body
                body = message.get("body", b"")
                if body:
                    try:
                        # Parse JSON response
                        data = json.loads(body.decode())
                        
                        # Sanitize data
                        sanitized_data = self.processor.sanitize_response_data(data)
                        
                        # Update response
                        message["body"] = json.dumps(sanitized_data).encode()
                        
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        # Not JSON or can't decode, leave as is
                        pass
            
            await send(message)
        
        await self.app(scope, receive, send_wrapper)
```

---

## Security Testing Framework

### Unit Testing for Security Components

#### Security Service Testing
```python
# tests/security/test_security_components.py
import pytest
import asyncio
from unittest.mock import Mock, patch

from src.core.security.enterprise_security_orchestrator import EnterpriseSecurityOrchestrator
from src.core.security.enterprise_dlp import EnterpriseDLPManager
from src.core.security.compliance_framework import ComplianceEngine

class TestSecurityOrchestrator:
    """Test cases for Security Orchestrator"""
    
    @pytest.fixture
    def security_orchestrator(self):
        """Create security orchestrator for testing"""
        return EnterpriseSecurityOrchestrator()
    
    @pytest.mark.asyncio
    async def test_process_data_request_success(self, security_orchestrator):
        """Test successful data request processing"""
        
        test_data = {"customer_email": "test@example.com"}
        context = {"location": "api", "user_id": "test_user"}
        
        result = await security_orchestrator.process_data_request(
            user_id="test_user",
            data=test_data,
            operation="read",
            resource_id="customer_data",
            context=context
        )
        
        assert result['status'] in ['approved', 'processed']
        assert 'request_id' in result
        assert 'timestamp' in result
    
    @pytest.mark.asyncio
    async def test_process_data_request_dlp_violation(self, security_orchestrator):
        """Test data request with DLP violation"""
        
        # Mock DLP to return violation
        with patch.object(security_orchestrator, 'dlp_manager') as mock_dlp:
            mock_dlp.scan_data.return_value = {
                'action': 'blocked',
                'detections': 1,
                'risk_score': 9.0,
                'sensitive_data_types': ['ssn']
            }
            
            test_data = {"ssn": "123-45-6789"}
            context = {"location": "api", "user_id": "test_user"}
            
            result = await security_orchestrator.process_data_request(
                user_id="test_user",
                data=test_data,
                operation="export",
                resource_id="customer_data",
                context=context
            )
            
            assert result['status'] == 'blocked'
            assert result['reason'] == 'DLP policy violation'
    
    @pytest.mark.asyncio
    async def test_security_assessment(self, security_orchestrator):
        """Test security assessment execution"""
        
        assessment = await security_orchestrator.run_security_assessment()
        
        assert 'assessment_id' in assessment
        assert 'overall_status' in assessment
        assert 'components' in assessment
        assert 'recommendations' in assessment

class TestDLPManager:
    """Test cases for DLP Manager"""
    
    @pytest.fixture
    def dlp_manager(self):
        """Create DLP manager for testing"""
        return EnterpriseDLPManager()
    
    def test_sensitive_data_detection(self, dlp_manager):
        """Test sensitive data detection"""
        
        test_data = {
            "customer_email": "john.doe@example.com",
            "ssn": "123-45-6789",
            "credit_card": "4111-1111-1111-1111"
        }
        
        result = dlp_manager.scan_data(test_data)
        
        assert result['detections'] > 0
        assert 'sensitive_data_types' in result
        assert 'email' in result['sensitive_data_types']
        assert result['risk_score'] > 0
    
    def test_data_redaction(self, dlp_manager):
        """Test data redaction functionality"""
        
        test_text = "Customer email is john.doe@example.com and SSN is 123-45-6789"
        detection_results = dlp_manager.detector.detect_sensitive_data(test_text)
        
        redacted_text, redaction_info = dlp_manager.redaction_engine.redact_data(
            test_text, detection_results
        )
        
        assert "john.doe@example.com" not in redacted_text
        assert "123-45-6789" not in redacted_text
        assert redaction_info['redactions']

class TestComplianceEngine:
    """Test cases for Compliance Engine"""
    
    @pytest.fixture
    def compliance_engine(self):
        """Create compliance engine for testing"""
        return ComplianceEngine()
    
    @pytest.mark.asyncio
    async def test_gdpr_assessment(self, compliance_engine):
        """Test GDPR compliance assessment"""
        
        results = await compliance_engine.run_compliance_assessment(
            framework=compliance_engine.ComplianceFramework.GDPR
        )
        
        assert len(results) > 0
        for result in results:
            assert result.framework == compliance_engine.ComplianceFramework.GDPR
            assert result.status in [
                compliance_engine.ComplianceStatus.COMPLIANT,
                compliance_engine.ComplianceStatus.NON_COMPLIANT,
                compliance_engine.ComplianceStatus.NOT_ASSESSED
            ]
    
    def test_compliance_dashboard(self, compliance_engine):
        """Test compliance dashboard generation"""
        
        dashboard = compliance_engine.get_compliance_dashboard()
        
        assert 'overall_compliance' in dashboard
        assert 'framework_compliance' in dashboard
        assert 'violation_summary' in dashboard
        assert isinstance(dashboard['overall_compliance']['compliance_rate'], float)
```

#### API Security Testing
```python
# tests/security/test_api_security.py
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from src.api.main import app

class TestAPISecurity:
    """Test API security features"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_authentication_required(self, client):
        """Test that protected endpoints require authentication"""
        
        response = client.get("/api/v1/security/dashboard")
        assert response.status_code == 401
    
    def test_invalid_token_rejected(self, client):
        """Test that invalid tokens are rejected"""
        
        headers = {"Authorization": "Bearer invalid_token"}
        response = client.get("/api/v1/security/dashboard", headers=headers)
        assert response.status_code == 401
    
    def test_insufficient_permissions(self, client):
        """Test that insufficient permissions are handled"""
        
        # Mock a valid token with limited permissions
        with patch('src.api.middleware.auth_integration.get_current_user') as mock_user:
            mock_user.return_value.permissions = ['perm_basic_access']
            
            headers = {"Authorization": "Bearer valid_token"}
            response = client.get("/api/v1/security/admin/users", headers=headers)
            assert response.status_code == 403
    
    def test_rate_limiting(self, client):
        """Test API rate limiting"""
        
        headers = {"Authorization": "Bearer valid_token"}
        
        # Make multiple requests quickly
        responses = []
        for _ in range(150):  # Exceed rate limit
            response = client.get("/api/v1/security/dashboard", headers=headers)
            responses.append(response)
        
        # Check that some requests were rate limited
        rate_limited = [r for r in responses if r.status_code == 429]
        assert len(rate_limited) > 0
    
    def test_sql_injection_protection(self, client):
        """Test SQL injection protection"""
        
        # Attempt SQL injection in query parameters
        malicious_queries = [
            "'; DROP TABLE users; --",
            "1' OR '1'='1",
            "1'; SELECT * FROM sensitive_data; --"
        ]
        
        headers = {"Authorization": "Bearer valid_token"}
        
        for query in malicious_queries:
            response = client.get(
                f"/api/v1/security/events?user_id={query}",
                headers=headers
            )
            # Should not return 500 error (SQL injection successful)
            assert response.status_code != 500
    
    def test_xss_protection(self, client):
        """Test XSS protection in API responses"""
        
        # Submit data with potential XSS
        xss_payload = "<script>alert('xss')</script>"
        
        headers = {"Authorization": "Bearer valid_token"}
        response = client.post(
            "/api/v1/security/scan",
            headers=headers,
            json={"data": {"comment": xss_payload}}
        )
        
        # Check that response doesn't contain unescaped script tags
        assert "<script>" not in response.text
        assert "alert('xss')" not in response.text
```

### Integration Testing

#### End-to-End Security Testing
```python
# tests/security/test_e2e_security.py
import pytest
import asyncio
from fastapi.testclient import TestClient

class TestE2ESecurityWorkflow:
    """End-to-end security workflow testing"""
    
    @pytest.fixture
    def authenticated_client(self):
        """Create authenticated test client"""
        client = TestClient(app)
        
        # Get authentication token
        login_response = client.post("/api/v1/auth/login", json={
            "username": "test_security_admin",
            "password": "TestPassword123!"
        })
        
        token = login_response.json()["access_token"]
        client.headers = {"Authorization": f"Bearer {token}"}
        
        return client
    
    def test_complete_security_workflow(self, authenticated_client):
        """Test complete security workflow from data ingestion to compliance reporting"""
        
        # 1. Submit data for security scanning
        test_data = {
            "customer_data": {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "ssn": "123-45-6789"
            }
        }
        
        scan_response = authenticated_client.post(
            "/api/v1/security/scan",
            json={"data": test_data}
        )
        
        assert scan_response.status_code == 200
        scan_result = scan_response.json()
        
        # 2. Verify DLP detection
        assert scan_result["detections"] > 0
        assert "email" in scan_result["sensitive_data_types"]
        assert "ssn" in scan_result["sensitive_data_types"]
        
        # 3. Check compliance status
        compliance_response = authenticated_client.get("/api/v1/compliance/dashboard")
        assert compliance_response.status_code == 200
        
        # 4. Run security assessment
        assessment_response = authenticated_client.post("/api/v1/security/assess")
        assert assessment_response.status_code == 200
        
        assessment = assessment_response.json()
        assert "overall_status" in assessment
        assert "components" in assessment
    
    def test_incident_response_workflow(self, authenticated_client):
        """Test incident response workflow"""
        
        # 1. Simulate security incident
        incident_data = {
            "type": "suspicious_access",
            "severity": "high",
            "description": "Multiple failed login attempts from unusual location",
            "affected_user": "test_user",
            "source_ip": "192.168.1.100"
        }
        
        incident_response = authenticated_client.post(
            "/api/v1/security/incidents",
            json=incident_data
        )
        
        assert incident_response.status_code == 201
        incident = incident_response.json()
        
        # 2. Verify incident was logged
        incidents_response = authenticated_client.get("/api/v1/security/incidents")
        incidents = incidents_response.json()
        
        created_incident = next(
            (i for i in incidents["incidents"] if i["incident_id"] == incident["incident_id"]), 
            None
        )
        assert created_incident is not None
        
        # 3. Update incident status
        update_response = authenticated_client.patch(
            f"/api/v1/security/incidents/{incident['incident_id']}",
            json={"status": "investigating"}
        )
        
        assert update_response.status_code == 200
```

### Security Performance Testing

#### Load Testing with Security Considerations
```python
# tests/security/test_security_performance.py
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from statistics import mean, median

class SecurityPerformanceTests:
    """Performance testing for security components"""
    
    def test_dlp_scanning_performance(self):
        """Test DLP scanning performance under load"""
        
        from src.core.security.enterprise_dlp import EnterpriseDLPManager
        
        dlp_manager = EnterpriseDLPManager()
        test_data = {
            "large_text": "This is a test document containing email john@example.com " * 1000,
            "sensitive_data": {
                "ssn": "123-45-6789",
                "credit_card": "4111-1111-1111-1111",
                "phone": "555-123-4567"
            }
        }
        
        # Measure performance
        scan_times = []
        
        for _ in range(100):
            start_time = time.time()
            result = dlp_manager.scan_data(test_data)
            end_time = time.time()
            
            scan_times.append(end_time - start_time)
        
        # Performance assertions
        avg_time = mean(scan_times)
        median_time = median(scan_times)
        max_time = max(scan_times)
        
        print(f"DLP Scan Performance:")
        print(f"Average: {avg_time:.3f}s")
        print(f"Median: {median_time:.3f}s")
        print(f"Max: {max_time:.3f}s")
        
        # Assert reasonable performance
        assert avg_time < 1.0  # Average scan should be under 1 second
        assert max_time < 5.0  # No scan should take more than 5 seconds
    
    def test_concurrent_authentication(self):
        """Test authentication performance under concurrent load"""
        
        from src.api.v1.services.enhanced_auth_service import get_auth_service
        
        auth_service = get_auth_service()
        
        async def authenticate_user():
            """Simulate user authentication"""
            token = await auth_service.create_access_token(
                user_id="test_user",
                permissions=["perm_basic_access"]
            )
            
            # Verify token
            payload = auth_service.decode_access_token(token)
            return payload is not None
        
        async def run_concurrent_auth_test():
            """Run concurrent authentication test"""
            
            tasks = [authenticate_user() for _ in range(100)]
            
            start_time = time.time()
            results = await asyncio.gather(*tasks)
            end_time = time.time()
            
            # All authentications should succeed
            assert all(results)
            
            # Performance should be reasonable
            total_time = end_time - start_time
            avg_time_per_auth = total_time / len(tasks)
            
            print(f"Concurrent Authentication Performance:")
            print(f"Total time: {total_time:.3f}s")
            print(f"Average per auth: {avg_time_per_auth:.3f}s")
            
            assert avg_time_per_auth < 0.1  # Each auth should be under 100ms
        
        # Run the test
        asyncio.run(run_concurrent_auth_test())
```

---

## Custom Security Policy Development

### Creating Custom DLP Policies

#### Custom Pattern Development
```python
# src/security/custom_policies/dlp_patterns.py
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Any
import re

from core.security.enterprise_dlp import SensitiveDataType, DataClassification, ComplianceFramework

class CustomDataType(Enum):
    """Custom sensitive data types for organization"""
    EMPLOYEE_ID = "employee_id"
    PROJECT_CODE = "project_code"
    CONTRACT_NUMBER = "contract_number"
    INTERNAL_DOCUMENT_ID = "internal_doc_id"
    VENDOR_CODE = "vendor_code"

@dataclass
class CustomDLPPattern:
    """Custom DLP pattern definition"""
    
    name: str
    data_type: CustomDataType
    pattern: str
    confidence_threshold: float
    context_keywords: List[str] = field(default_factory=list)
    exclusion_patterns: List[str] = field(default_factory=list)
    classification: DataClassification = DataClassification.INTERNAL
    business_impact: str = "medium"
    compliance_frameworks: List[ComplianceFramework] = field(default_factory=list)

class CustomDLPPolicyManager:
    """Manager for custom DLP policies"""
    
    def __init__(self):
        self.custom_patterns = self._initialize_custom_patterns()
    
    def _initialize_custom_patterns(self) -> List[CustomDLPPattern]:
        """Initialize custom DLP patterns"""
        
        return [
            CustomDLPPattern(
                name="Employee ID Pattern",
                data_type=CustomDataType.EMPLOYEE_ID,
                pattern=r'EMP-\d{6}',
                confidence_threshold=0.9,
                context_keywords=['employee', 'staff', 'personnel', 'worker'],
                classification=DataClassification.INTERNAL,
                business_impact="high"
            ),
            
            CustomDLPPattern(
                name="Project Code Pattern",
                data_type=CustomDataType.PROJECT_CODE,
                pattern=r'PROJ-[A-Z]{3}-\d{4}',
                confidence_threshold=0.95,
                context_keywords=['project', 'initiative', 'program'],
                classification=DataClassification.CONFIDENTIAL,
                business_impact="critical"
            ),
            
            CustomDLPPattern(
                name="Contract Number Pattern", 
                data_type=CustomDataType.CONTRACT_NUMBER,
                pattern=r'CNT-\d{8}',
                confidence_threshold=0.9,
                context_keywords=['contract', 'agreement', 'deal'],
                classification=DataClassification.CONFIDENTIAL,
                business_impact="high"
            ),
            
            CustomDLPPattern(
                name="Internal Document ID",
                data_type=CustomDataType.INTERNAL_DOCUMENT_ID,
                pattern=r'DOC-[A-Z]{2}\d{6}',
                confidence_threshold=0.85,
                context_keywords=['document', 'file', 'report'],
                classification=DataClassification.INTERNAL,
                business_impact="medium"
            )
        ]
    
    def register_custom_patterns(self, dlp_manager):
        """Register custom patterns with DLP manager"""
        
        for pattern in self.custom_patterns:
            # Convert to standard DLP pattern format
            dlp_pattern = self._convert_to_dlp_pattern(pattern)
            dlp_manager.detector.add_custom_pattern(pattern.name, dlp_pattern)
    
    def _convert_to_dlp_pattern(self, custom_pattern: CustomDLPPattern):
        """Convert custom pattern to standard DLP pattern"""
        
        from core.security.enterprise_dlp import SensitiveDataPattern
        
        return SensitiveDataPattern(
            data_type=SensitiveDataType.CUSTOM_PATTERN,  # Use custom type
            pattern=custom_pattern.pattern,
            confidence_threshold=custom_pattern.confidence_threshold,
            context_keywords=custom_pattern.context_keywords,
            exclusion_patterns=custom_pattern.exclusion_patterns,
            compliance_frameworks=custom_pattern.compliance_frameworks,
            description=custom_pattern.name
        )

# Usage example
def setup_custom_dlp_policies():
    """Setup custom DLP policies for organization"""
    
    from core.security.enterprise_dlp import EnterpriseDLPManager
    
    dlp_manager = EnterpriseDLPManager()
    custom_policy_manager = CustomDLPPolicyManager()
    
    # Register custom patterns
    custom_policy_manager.register_custom_patterns(dlp_manager)
    
    return dlp_manager
```

#### Industry-Specific Compliance Policies
```python
# src/security/custom_policies/industry_compliance.py
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any

class IndustryType(Enum):
    """Industry types for specialized compliance"""
    FINANCIAL_SERVICES = "financial_services"
    HEALTHCARE = "healthcare"
    GOVERNMENT = "government"
    EDUCATION = "education"
    TECHNOLOGY = "technology"

@dataclass
class IndustryComplianceRule:
    """Industry-specific compliance rule"""
    
    rule_id: str
    industry: IndustryType
    regulation: str
    description: str
    data_types: List[str]
    required_controls: List[str]
    audit_frequency: str
    penalty_severity: str

class IndustryComplianceManager:
    """Manager for industry-specific compliance policies"""
    
    def __init__(self, industry: IndustryType):
        self.industry = industry
        self.compliance_rules = self._get_industry_rules()
    
    def _get_industry_rules(self) -> List[IndustryComplianceRule]:
        """Get compliance rules for specific industry"""
        
        if self.industry == IndustryType.FINANCIAL_SERVICES:
            return self._get_financial_services_rules()
        elif self.industry == IndustryType.HEALTHCARE:
            return self._get_healthcare_rules()
        elif self.industry == IndustryType.GOVERNMENT:
            return self._get_government_rules()
        else:
            return self._get_general_rules()
    
    def _get_financial_services_rules(self) -> List[IndustryComplianceRule]:
        """Financial services compliance rules"""
        
        return [
            IndustryComplianceRule(
                rule_id="FS-001",
                industry=IndustryType.FINANCIAL_SERVICES,
                regulation="PCI-DSS",
                description="Payment card data must be encrypted at rest and in transit",
                data_types=["credit_card", "bank_account"],
                required_controls=["encryption", "access_logging", "network_segmentation"],
                audit_frequency="quarterly",
                penalty_severity="critical"
            ),
            
            IndustryComplianceRule(
                rule_id="FS-002", 
                industry=IndustryType.FINANCIAL_SERVICES,
                regulation="SOX",
                description="Financial reporting data requires segregation of duties",
                data_types=["financial_data", "accounting_records"],
                required_controls=["dual_authorization", "audit_trail", "backup"],
                audit_frequency="annual",
                penalty_severity="high"
            )
        ]
    
    def _get_healthcare_rules(self) -> List[IndustryComplianceRule]:
        """Healthcare compliance rules"""
        
        return [
            IndustryComplianceRule(
                rule_id="HC-001",
                industry=IndustryType.HEALTHCARE,
                regulation="HIPAA",
                description="Protected health information requires comprehensive safeguards",
                data_types=["medical_record", "health_id", "prescription"],
                required_controls=["encryption", "access_control", "audit_logging"],
                audit_frequency="continuous",
                penalty_severity="critical"
            )
        ]
    
    def generate_compliance_policy(self) -> Dict[str, Any]:
        """Generate comprehensive compliance policy for industry"""
        
        policy = {
            "industry": self.industry.value,
            "policy_version": "1.0",
            "rules": [],
            "controls": set(),
            "audit_schedule": {}
        }
        
        for rule in self.compliance_rules:
            policy["rules"].append({
                "rule_id": rule.rule_id,
                "regulation": rule.regulation,
                "description": rule.description,
                "data_types": rule.data_types,
                "required_controls": rule.required_controls,
                "penalty_severity": rule.penalty_severity
            })
            
            # Collect all required controls
            policy["controls"].update(rule.required_controls)
            
            # Set audit schedule
            policy["audit_schedule"][rule.rule_id] = rule.audit_frequency
        
        policy["controls"] = list(policy["controls"])
        
        return policy
```

### Custom Access Control Policies

#### Dynamic Policy Engine
```python
# src/security/custom_policies/dynamic_access.py
from dataclasses import dataclass
from typing import Dict, Any, List, Callable
from datetime import datetime, time
import json

@dataclass
class DynamicAccessRule:
    """Dynamic access control rule"""
    
    rule_id: str
    name: str
    description: str
    condition: str  # Python expression string
    action: str  # allow, deny, require_mfa, etc.
    priority: int = 100
    enabled: bool = True
    metadata: Dict[str, Any] = None

class DynamicAccessPolicyEngine:
    """Dynamic access control policy engine"""
    
    def __init__(self):
        self.rules: List[DynamicAccessRule] = []
        self.context_providers: Dict[str, Callable] = {}
        self._register_built_in_providers()
    
    def _register_built_in_providers(self):
        """Register built-in context providers"""
        
        self.context_providers.update({
            'current_time': lambda: datetime.now(),
            'current_hour': lambda: datetime.now().hour,
            'current_day': lambda: datetime.now().strftime('%A'),
            'business_hours': lambda: 8 <= datetime.now().hour <= 17,
            'weekend': lambda: datetime.now().weekday() >= 5
        })
    
    def add_rule(self, rule: DynamicAccessRule):
        """Add dynamic access rule"""
        self.rules.append(rule)
        self.rules.sort(key=lambda r: r.priority)
    
    def register_context_provider(self, name: str, provider: Callable):
        """Register custom context provider"""
        self.context_providers[name] = provider
    
    async def evaluate_access(
        self, 
        user: Dict[str, Any], 
        resource: Dict[str, Any], 
        action: str,
        environment: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Evaluate access based on dynamic rules"""
        
        # Build evaluation context
        context = {
            'user': user,
            'resource': resource,
            'action': action,
            'environment': environment or {},
            **{name: provider() for name, provider in self.context_providers.items()}
        }
        
        # Evaluate rules in priority order
        for rule in self.rules:
            if not rule.enabled:
                continue
            
            try:
                # Evaluate rule condition
                if self._evaluate_condition(rule.condition, context):
                    return {
                        'decision': rule.action,
                        'rule_id': rule.rule_id,
                        'rule_name': rule.name,
                        'evaluation_context': context,
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                # Log evaluation error but continue
                print(f"Error evaluating rule {rule.rule_id}: {e}")
                continue
        
        # Default deny if no rules match
        return {
            'decision': 'deny',
            'rule_id': 'default',
            'rule_name': 'Default Deny',
            'reason': 'No matching access rules',
            'timestamp': datetime.now().isoformat()
        }
    
    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Safely evaluate rule condition"""
        
        # Create safe evaluation environment
        safe_globals = {
            '__builtins__': {},
            # Safe functions
            'len': len,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'any': any,
            'all': all,
            'min': min,
            'max': max,
            # Context variables
            **context
        }
        
        try:
            return eval(condition, safe_globals, {})
        except Exception:
            return False

# Example usage and rule definitions
def setup_dynamic_access_policies():
    """Setup dynamic access control policies"""
    
    engine = DynamicAccessPolicyEngine()
    
    # Time-based access rules
    engine.add_rule(DynamicAccessRule(
        rule_id="business_hours_admin",
        name="Business Hours Admin Access",
        description="Allow admin access during business hours",
        condition="user['role'] == 'admin' and business_hours",
        action="allow",
        priority=10
    ))
    
    engine.add_rule(DynamicAccessRule(
        rule_id="after_hours_mfa",
        name="After Hours MFA Required",
        description="Require MFA for after-hours access",
        condition="not business_hours and user['role'] in ['admin', 'manager']",
        action="require_mfa",
        priority=20
    ))
    
    # Location-based access
    engine.add_rule(DynamicAccessRule(
        rule_id="remote_access_restriction",
        name="Remote Access Restriction",
        description="Restrict sensitive data access from remote locations",
        condition="environment.get('location_type') == 'remote' and resource['classification'] == 'restricted'",
        action="deny",
        priority=5
    ))
    
    # Data classification-based access
    engine.add_rule(DynamicAccessRule(
        rule_id="data_classification_access",
        name="Data Classification Access Control",
        description="Control access based on data classification and user clearance",
        condition="user.get('clearance_level', 0) >= resource.get('classification_level', 0)",
        action="allow",
        priority=30
    ))
    
    return engine

# Integration with access control system
async def integrate_dynamic_policies():
    """Integrate dynamic policies with access control system"""
    
    from core.security.enhanced_access_control import get_access_control_manager
    
    access_manager = get_access_control_manager()
    policy_engine = setup_dynamic_access_policies()
    
    # Register policy engine as custom evaluator
    async def dynamic_policy_evaluator(access_request):
        """Custom policy evaluator using dynamic rules"""
        
        user_data = {
            'id': access_request.subject_id,
            'role': getattr(access_request, 'subject_role', 'user'),
            'clearance_level': getattr(access_request, 'clearance_level', 1)
        }
        
        resource_data = {
            'id': access_request.resource_id,
            'type': access_request.resource_type,
            'classification': getattr(access_request, 'classification', 'internal'),
            'classification_level': getattr(access_request, 'classification_level', 1)
        }
        
        environment_data = {
            'location_type': access_request.context.get('location_type', 'office'),
            'ip_address': access_request.context.get('ip_address'),
            'device_type': access_request.context.get('device_type', 'desktop')
        }
        
        result = await policy_engine.evaluate_access(
            user=user_data,
            resource=resource_data,
            action=access_request.action,
            environment=environment_data
        )
        
        return result
    
    # Register with access control manager
    access_manager.register_custom_evaluator(dynamic_policy_evaluator)
```

---

## DLP Integration Guide

### Custom Data Type Detection

#### Implementing Custom Detectors
```python
# src/security/dlp/custom_detectors.py
from typing import List, Dict, Any, Tuple
import re
from abc import ABC, abstractmethod

from core.security.enterprise_dlp import DataDetectionResult, SensitiveDataType, DataClassification

class CustomDataDetector(ABC):
    """Base class for custom data detectors"""
    
    @abstractmethod
    def detect(self, text: str, context: Dict[str, Any]) -> List[DataDetectionResult]:
        """Detect sensitive data in text"""
        pass
    
    @abstractmethod
    def get_data_type(self) -> str:
        """Get the data type this detector handles"""
        pass

class OrganizationalDataDetector(CustomDataDetector):
    """Detector for organization-specific sensitive data"""
    
    def __init__(self):
        self.patterns = {
            'internal_id': {
                'pattern': r'ID-\d{8}',
                'confidence': 0.9,
                'classification': DataClassification.INTERNAL
            },
            'project_name': {
                'pattern': r'PROJECT-[A-Z]{4}-\d{4}',
                'confidence': 0.95,
                'classification': DataClassification.CONFIDENTIAL
            },
            'budget_code': {
                'pattern': r'BUDGET-[A-Z0-9]{6}',
                'confidence': 0.85,
                'classification': DataClassification.CONFIDENTIAL
            }
        }
    
    def detect(self, text: str, context: Dict[str, Any]) -> List[DataDetectionResult]:
        """Detect organizational sensitive data"""
        
        results = []
        
        for data_type, pattern_info in self.patterns.items():
            matches = re.finditer(pattern_info['pattern'], text, re.IGNORECASE)
            
            for match in matches:
                result = DataDetectionResult(
                    data_type=SensitiveDataType.CUSTOM_PATTERN,
                    classification=pattern_info['classification'],
                    confidence=pattern_info['confidence'],
                    matched_text=match.group(0),
                    redacted_text=self._redact_match(match.group(0), data_type),
                    start_position=match.start(),
                    end_position=match.end(),
                    context=context.get('description', ''),
                    compliance_frameworks=[],
                    metadata={
                        'custom_type': data_type,
                        'detector': 'organizational'
                    }
                )
                results.append(result)
        
        return results
    
    def _redact_match(self, text: str, data_type: str) -> str:
        """Redact matched text based on type"""
        
        if data_type == 'internal_id':
            return f"ID-{text[-4:]}"
        elif data_type in ['project_name', 'budget_code']:
            return f"[REDACTED_{data_type.upper()}]"
        else:
            return "[REDACTED]"
    
    def get_data_type(self) -> str:
        return "organizational_data"

class FinancialDataDetector(CustomDataDetector):
    """Detector for financial data beyond credit cards"""
    
    def __init__(self):
        self.patterns = {
            'iban': {
                'pattern': r'[A-Z]{2}\d{2}[A-Z0-9]{4,30}',
                'confidence': 0.9,
                'validation': self._validate_iban
            },
            'swift_code': {
                'pattern': r'[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?',
                'confidence': 0.85,
                'validation': self._validate_swift
            },
            'tax_id': {
                'pattern': r'\d{2}-\d{7}',
                'confidence': 0.8,
                'validation': None
            }
        }
    
    def detect(self, text: str, context: Dict[str, Any]) -> List[DataDetectionResult]:
        """Detect financial data"""
        
        results = []
        
        for data_type, pattern_info in self.patterns.items():
            matches = re.finditer(pattern_info['pattern'], text, re.IGNORECASE)
            
            for match in matches:
                matched_text = match.group(0)
                
                # Additional validation if available
                if pattern_info['validation']:
                    if not pattern_info['validation'](matched_text):
                        continue
                
                result = DataDetectionResult(
                    data_type=SensitiveDataType.CUSTOM_PATTERN,
                    classification=DataClassification.RESTRICTED,
                    confidence=pattern_info['confidence'],
                    matched_text=matched_text,
                    redacted_text=self._redact_financial_data(matched_text, data_type),
                    start_position=match.start(),
                    end_position=match.end(),
                    context=context.get('description', ''),
                    compliance_frameworks=[],
                    metadata={
                        'custom_type': data_type,
                        'detector': 'financial'
                    }
                )
                results.append(result)
        
        return results
    
    def _validate_iban(self, iban: str) -> bool:
        """Validate IBAN using mod-97 algorithm"""
        
        # Remove spaces and convert to uppercase
        iban = re.sub(r'\s', '', iban.upper())
        
        # Check length
        if len(iban) < 15 or len(iban) > 34:
            return False
        
        # Move first 4 characters to end
        rearranged = iban[4:] + iban[:4]
        
        # Replace letters with numbers
        numeric_string = ''
        for char in rearranged:
            if char.isalpha():
                numeric_string += str(ord(char) - ord('A') + 10)
            else:
                numeric_string += char
        
        # Check mod 97
        return int(numeric_string) % 97 == 1
    
    def _validate_swift(self, swift: str) -> bool:
        """Basic SWIFT code validation"""
        
        swift = swift.upper()
        
        # Length check
        if len(swift) not in [8, 11]:
            return False
        
        # Format check
        if not re.match(r'^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$', swift):
            return False
        
        return True
    
    def _redact_financial_data(self, text: str, data_type: str) -> str:
        """Redact financial data"""
        
        if data_type == 'iban':
            return f"{text[:4]}****{text[-4:]}"
        elif data_type == 'swift_code':
            return f"{text[:4]}****"
        elif data_type == 'tax_id':
            return "**-*******"
        else:
            return "[REDACTED_FINANCIAL]"
    
    def get_data_type(self) -> str:
        return "financial_data"

class CustomDLPManager:
    """Manager for custom DLP detectors"""
    
    def __init__(self):
        self.custom_detectors: List[CustomDataDetector] = []
    
    def register_detector(self, detector: CustomDataDetector):
        """Register a custom detector"""
        self.custom_detectors.append(detector)
    
    def scan_with_custom_detectors(
        self, 
        text: str, 
        context: Dict[str, Any] = None
    ) -> List[DataDetectionResult]:
        """Scan text using all registered custom detectors"""
        
        context = context or {}
        all_results = []
        
        for detector in self.custom_detectors:
            try:
                results = detector.detect(text, context)
                all_results.extend(results)
            except Exception as e:
                print(f"Error in detector {detector.get_data_type()}: {e}")
                continue
        
        return all_results

# Integration with main DLP system
def setup_custom_dlp_detectors():
    """Setup custom DLP detectors"""
    
    from core.security.enterprise_dlp import EnterpriseDLPManager
    
    # Get main DLP manager
    dlp_manager = EnterpriseDLPManager()
    
    # Create custom detector manager
    custom_manager = CustomDLPManager()
    
    # Register custom detectors
    custom_manager.register_detector(OrganizationalDataDetector())
    custom_manager.register_detector(FinancialDataDetector())
    
    # Extend DLP manager with custom scanning
    original_scan = dlp_manager.scan_data
    
    def enhanced_scan_data(data, context=None):
        """Enhanced scan with custom detectors"""
        
        # Run original scan
        original_result = original_scan(data, context)
        
        # Run custom detectors
        if isinstance(data, dict):
            text_data = str(data)
        else:
            text_data = str(data)
        
        custom_results = custom_manager.scan_with_custom_detectors(text_data, context)
        
        # Merge results
        if custom_results:
            original_result['detections'] += len(custom_results)
            original_result['sensitive_data_types'].extend([
                r.metadata.get('custom_type', 'unknown') for r in custom_results
            ])
        
        return original_result
    
    # Replace scan method
    dlp_manager.scan_data = enhanced_scan_data
    
    return dlp_manager
```

---

## Compliance Framework Extension

### Custom Compliance Frameworks

#### Framework Definition
```python
# src/security/compliance/custom_frameworks.py
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Any, Callable
from datetime import datetime

class CustomComplianceFramework(Enum):
    """Custom compliance frameworks"""
    COMPANY_SECURITY_POLICY = "company_security_policy"
    DATA_GOVERNANCE_STANDARD = "data_governance_standard"
    VENDOR_SECURITY_REQUIREMENTS = "vendor_security_requirements"
    INDUSTRY_BEST_PRACTICES = "industry_best_practices"

@dataclass
class CustomComplianceControl:
    """Custom compliance control definition"""
    
    control_id: str
    framework: CustomComplianceFramework
    title: str
    description: str
    category: str
    implementation_guidance: str
    testing_procedure: str
    evidence_requirements: List[str] = field(default_factory=list)
    automation_possible: bool = False
    risk_level: str = "medium"
    frequency: str = "quarterly"
    business_justification: str = ""
    custom_metadata: Dict[str, Any] = field(default_factory=dict)

class CustomComplianceFrameworkManager:
    """Manager for custom compliance frameworks"""
    
    def __init__(self):
        self.frameworks = {}
        self.automated_checks = {}
        self._initialize_custom_frameworks()
    
    def _initialize_custom_frameworks(self):
        """Initialize custom compliance frameworks"""
        
        # Company Security Policy Framework
        self.frameworks[CustomComplianceFramework.COMPANY_SECURITY_POLICY] = [
            CustomComplianceControl(
                control_id="CSP-001",
                framework=CustomComplianceFramework.COMPANY_SECURITY_POLICY,
                title="Password Complexity Requirements",
                description="All user passwords must meet minimum complexity requirements",
                category="Access Control",
                implementation_guidance="Implement password policy with minimum 12 characters, mixed case, numbers, and symbols",
                testing_procedure="Verify password policy settings and test with sample passwords",
                evidence_requirements=["Password policy configuration", "Test results"],
                automation_possible=True,
                risk_level="high",
                business_justification="Protect against brute force attacks and unauthorized access"
            ),
            
            CustomComplianceControl(
                control_id="CSP-002",
                framework=CustomComplianceFramework.COMPANY_SECURITY_POLICY,
                title="Data Backup and Recovery",
                description="Critical data must be backed up regularly and recovery tested",
                category="Data Protection",
                implementation_guidance="Implement automated daily backups with monthly recovery testing",
                testing_procedure="Verify backup completion and test recovery procedures",
                evidence_requirements=["Backup logs", "Recovery test reports"],
                automation_possible=True,
                risk_level="critical",
                business_justification="Ensure business continuity and data availability"
            )
        ]
        
        # Data Governance Standard
        self.frameworks[CustomComplianceFramework.DATA_GOVERNANCE_STANDARD] = [
            CustomComplianceControl(
                control_id="DGS-001",
                framework=CustomComplianceFramework.DATA_GOVERNANCE_STANDARD,
                title="Data Classification and Labeling",
                description="All data must be classified and labeled according to sensitivity",
                category="Data Governance",
                implementation_guidance="Implement automated data classification tools and labeling procedures",
                testing_procedure="Verify data classification accuracy and label consistency",
                evidence_requirements=["Classification rules", "Labeling reports"],
                automation_possible=True,
                risk_level="high",
                business_justification="Ensure appropriate data handling and protection"
            )
        ]
        
        # Register automated checks
        self.automated_checks.update({
            "CSP-001": self._check_password_policy,
            "CSP-002": self._check_backup_procedures,
            "DGS-001": self._check_data_classification
        })
    
    async def _check_password_policy(self) -> Dict[str, Any]:
        """Check password policy compliance"""
        
        findings = []
        evidence = []
        score = 0.0
        
        # Mock password policy check
        # In real implementation, this would check actual password policies
        policy_settings = {
            'min_length': 12,
            'require_uppercase': True,
            'require_lowercase': True,
            'require_numbers': True,
            'require_symbols': True,
            'history_count': 24
        }
        
        # Check each requirement
        if policy_settings['min_length'] >= 12:
            findings.append("Password minimum length requirement met")
            score += 0.2
        
        if policy_settings['require_uppercase']:
            findings.append("Uppercase letter requirement enabled")
            score += 0.2
        
        if policy_settings['require_lowercase']:
            findings.append("Lowercase letter requirement enabled")
            score += 0.2
        
        if policy_settings['require_numbers']:
            findings.append("Number requirement enabled")
            score += 0.2
        
        if policy_settings['require_symbols']:
            findings.append("Symbol requirement enabled")
            score += 0.2
        
        evidence.append({
            'type': 'configuration',
            'description': 'Password policy settings',
            'data': policy_settings
        })
        
        status = 'compliant' if score >= 1.0 else 'non_compliant'
        
        return {
            'status': status,
            'score': score,
            'findings': findings,
            'evidence': evidence,
            'risk_level': 'low' if status == 'compliant' else 'high'
        }
    
    async def _check_backup_procedures(self) -> Dict[str, Any]:
        """Check backup procedures compliance"""
        
        findings = []
        evidence = []
        score = 0.0
        
        # Mock backup check
        backup_status = {
            'daily_backups_enabled': True,
            'last_backup_date': datetime.now().date(),
            'backup_success_rate': 0.98,
            'recovery_test_date': datetime(2024, 10, 1).date(),
            'retention_period_days': 90
        }
        
        if backup_status['daily_backups_enabled']:
            findings.append("Daily backups are enabled")
            score += 0.3
        
        if backup_status['backup_success_rate'] >= 0.95:
            findings.append(f"Backup success rate is acceptable ({backup_status['backup_success_rate']:.1%})")
            score += 0.3
        
        # Check if recovery test is recent (within 30 days)
        days_since_test = (datetime.now().date() - backup_status['recovery_test_date']).days
        if days_since_test <= 30:
            findings.append("Recovery testing is current")
            score += 0.4
        else:
            findings.append(f"Recovery testing is overdue ({days_since_test} days)")
        
        evidence.append({
            'type': 'monitoring_data',
            'description': 'Backup system status',
            'data': backup_status
        })
        
        status = 'compliant' if score >= 0.8 else 'non_compliant'
        
        return {
            'status': status,
            'score': score,
            'findings': findings,
            'evidence': evidence
        }
    
    async def _check_data_classification(self) -> Dict[str, Any]:
        """Check data classification compliance"""
        
        findings = []
        evidence = []
        score = 0.7  # Base score
        
        # Mock data classification check
        classification_stats = {
            'total_datasets': 150,
            'classified_datasets': 135,
            'labeled_datasets': 120,
            'classification_accuracy': 0.92
        }
        
        classification_rate = classification_stats['classified_datasets'] / classification_stats['total_datasets']
        labeling_rate = classification_stats['labeled_datasets'] / classification_stats['total_datasets']
        
        findings.append(f"Data classification rate: {classification_rate:.1%}")
        findings.append(f"Data labeling rate: {labeling_rate:.1%}")
        findings.append(f"Classification accuracy: {classification_stats['classification_accuracy']:.1%}")
        
        if classification_rate >= 0.9:
            score += 0.15
        if labeling_rate >= 0.8:
            score += 0.15
        
        evidence.append({
            'type': 'system_report',
            'description': 'Data classification statistics',
            'data': classification_stats
        })
        
        status = 'compliant' if score >= 0.8 else 'partially_compliant'
        
        return {
            'status': status,
            'score': score,
            'findings': findings,
            'evidence': evidence
        }
    
    def get_framework_controls(self, framework: CustomComplianceFramework) -> List[CustomComplianceControl]:
        """Get controls for specific framework"""
        return self.frameworks.get(framework, [])
    
    def get_all_controls(self) -> List[CustomComplianceControl]:
        """Get all custom compliance controls"""
        
        all_controls = []
        for controls in self.frameworks.values():
            all_controls.extend(controls)
        return all_controls
    
    async def run_automated_check(self, control_id: str) -> Dict[str, Any]:
        """Run automated check for specific control"""
        
        if control_id in self.automated_checks:
            return await self.automated_checks[control_id]()
        else:
            return {
                'status': 'not_assessed',
                'score': 0.0,
                'findings': ['Manual assessment required'],
                'evidence': []
            }

# Integration with main compliance engine
def integrate_custom_compliance_frameworks():
    """Integrate custom frameworks with main compliance engine"""
    
    from core.security.compliance_framework import get_compliance_engine
    
    compliance_engine = get_compliance_engine()
    custom_manager = CustomComplianceFrameworkManager()
    
    # Add custom controls to main engine
    for control in custom_manager.get_all_controls():
        # Convert custom control to standard format
        standard_control = convert_custom_to_standard_control(control)
        compliance_engine.controls[control.control_id] = standard_control
    
    # Register automated checks
    for control_id, check_func in custom_manager.automated_checks.items():
        compliance_engine.automated_checks[control_id] = check_func
    
    return compliance_engine

def convert_custom_to_standard_control(custom_control: CustomComplianceControl):
    """Convert custom control to standard compliance control format"""
    
    from core.security.compliance_framework import ComplianceControl, RiskLevel
    
    # Map risk levels
    risk_mapping = {
        'low': RiskLevel.LOW,
        'medium': RiskLevel.MEDIUM,
        'high': RiskLevel.HIGH,
        'critical': RiskLevel.CRITICAL
    }
    
    return ComplianceControl(
        control_id=custom_control.control_id,
        framework=None,  # Custom frameworks not in enum
        title=custom_control.title,
        description=custom_control.description,
        category=custom_control.category,
        implementation_guidance=custom_control.implementation_guidance,
        testing_procedure=custom_control.testing_procedure,
        evidence_requirements=custom_control.evidence_requirements,
        automation_possible=custom_control.automation_possible,
        risk_level=risk_mapping.get(custom_control.risk_level, RiskLevel.MEDIUM),
        frequency=custom_control.frequency,
        metadata=custom_control.custom_metadata
    )
```

---

This developer guide provides comprehensive coverage of security framework development, integration patterns, testing methodologies, and extensibility features. Developers can use this guide to build secure applications that leverage the enterprise security platform effectively.