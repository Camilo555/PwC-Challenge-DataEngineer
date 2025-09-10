# Story 2.1: Zero-Trust Security Architecture with Advanced Threat Detection (Technical Implementation)

## **Business (B) Context:**
As a **security officer and compliance manager**
I want **comprehensive security monitoring with automated threat detection and response**
So that **we achieve 100% compliance with enterprise security standards and prevent data breaches**

**Business Value:** $5M+ risk mitigation through advanced security and compliance automation

## **Market (M) Validation:**
- **Regulatory Pressure**: 95% increase in compliance requirements (SOX, GDPR, CCPA)
- **Security Threats**: 300% increase in data platform security attacks in 2024
- **Market Standard**: Zero-trust architecture becoming mandatory for enterprise platforms
- **Competitive Advantage**: Advanced security capabilities as market differentiator

## **Architecture (A) - Advanced Security Implementation:**

### **Microservices Security with Service Mesh**
```python
# Advanced JWT authentication with refresh token rotation
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import hashlib
import secrets

class AdvancedJWTManager:
    def __init__(self):
        self.algorithm = "RS256"  # RSA256 for better security
        self.access_token_expire = 15  # 15 minutes
        self.refresh_token_expire = 7  # 7 days
        self.private_key = self.load_private_key()
        self.public_key = self.load_public_key()
        self.revoked_tokens = set()  # Redis-backed in production
        
    async def create_access_token(self, subject: str, permissions: List[str], additional_claims: Dict[str, Any] = None) -> Dict[str, str]:
        """Create access token with custom claims and permissions"""
        now = datetime.utcnow()
        expire = now + timedelta(minutes=self.access_token_expire)
        
        # Standard claims
        payload = {
            "sub": subject,
            "iat": now,
            "exp": expire,
            "nbf": now,
            "iss": "pwc-data-platform",
            "aud": "api-consumers",
            "jti": secrets.token_urlsafe(32),  # Unique token ID for revocation
            "token_type": "access"
        }
        
        # Custom claims
        payload.update({
            "permissions": permissions,
            "scope": " ".join(permissions),
            "tenant_id": additional_claims.get("tenant_id") if additional_claims else None,
            "user_role": additional_claims.get("user_role") if additional_claims else None,
            "security_level": self.calculate_security_level(permissions)
        })
        
        if additional_claims:
            payload.update(additional_claims)
        
        access_token = jwt.encode(payload, self.private_key, algorithm=self.algorithm)
        
        # Create refresh token
        refresh_payload = {
            "sub": subject,
            "iat": now,
            "exp": now + timedelta(days=self.refresh_token_expire),
            "jti": secrets.token_urlsafe(32),
            "token_type": "refresh",
            "access_token_jti": payload["jti"]  # Link to access token
        }
        refresh_token = jwt.encode(refresh_payload, self.private_key, algorithm=self.algorithm)
        
        # Store token metadata for security monitoring
        await self.store_token_metadata(payload["jti"], {
            "user_id": subject,
            "issued_at": now,
            "permissions": permissions,
            "ip_address": additional_claims.get("ip_address"),
            "user_agent": additional_claims.get("user_agent")
        })
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "Bearer",
            "expires_in": self.access_token_expire * 60,
            "scope": " ".join(permissions)
        }
    
    def calculate_security_level(self, permissions: List[str]) -> str:
        """Calculate security level based on permissions"""
        high_risk_permissions = {"admin", "delete", "export", "system"}
        
        if any(perm in high_risk_permissions for perm in permissions):
            return "HIGH"
        elif len(permissions) > 5:
            return "MEDIUM"
        else:
            return "LOW"

# Advanced authorization with RBAC and ABAC
class AuthorizationEngine:
    def __init__(self):
        self.role_permissions = self.load_role_permissions()
        self.resource_policies = self.load_resource_policies()
        
    async def authorize_request(
        self, 
        user_id: str, 
        resource: str, 
        action: str, 
        context: Dict[str, Any] = None
    ) -> bool:
        """Advanced authorization with role-based and attribute-based access control"""
        
        # Get user roles and attributes
        user_context = await self.get_user_context(user_id)
        
        # Check role-based permissions (RBAC)
        rbac_authorized = await self.check_rbac_authorization(
            user_context["roles"], resource, action
        )
        
        if not rbac_authorized:
            return False
        
        # Check attribute-based policies (ABAC)
        abac_authorized = await self.check_abac_authorization(
            user_context, resource, action, context
        )
        
        # Log authorization decision for audit
        await self.log_authorization_decision(
            user_id, resource, action, rbac_authorized and abac_authorized, context
        )
        
        return rbac_authorized and abac_authorized
    
    async def check_abac_authorization(
        self, 
        user_context: Dict[str, Any], 
        resource: str, 
        action: str, 
        request_context: Dict[str, Any] = None
    ) -> bool:
        """Attribute-based access control with dynamic policies"""
        
        # Time-based restrictions
        current_hour = datetime.now().hour
        if user_context.get("time_restricted") and not (9 <= current_hour <= 17):
            return False
        
        # Location-based restrictions
        if request_context and user_context.get("location_restricted"):
            allowed_ips = user_context.get("allowed_ip_ranges", [])
            client_ip = request_context.get("client_ip")
            if client_ip and not self.ip_in_ranges(client_ip, allowed_ips):
                return False
        
        # Data sensitivity restrictions
        if resource.startswith("sensitive_") and user_context.get("clearance_level", 0) < 3:
            return False
        
        # Resource ownership
        if action in ["update", "delete"] and resource.startswith("user_data"):
            resource_owner = await self.get_resource_owner(resource)
            if resource_owner != user_context["user_id"] and "admin" not in user_context["roles"]:
                return False
        
        return True
```

### **Advanced API Rate Limiting**
```python
# API Rate limiting with intelligent throttling
class AdvancedRateLimiter:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.rate_limits = {
            "default": {"requests": 1000, "window": 3600},  # 1000 per hour
            "premium": {"requests": 5000, "window": 3600},  # 5000 per hour
            "admin": {"requests": 10000, "window": 3600},   # 10000 per hour
        }
    
    async def check_rate_limit(
        self, 
        user_id: str, 
        endpoint: str, 
        user_tier: str = "default"
    ) -> Dict[str, Any]:
        """Advanced rate limiting with sliding window and burst handling"""
        
        current_time = int(time.time())
        window_size = self.rate_limits[user_tier]["window"]
        max_requests = self.rate_limits[user_tier]["requests"]
        
        # Sliding window key
        window_key = f"rate_limit:{user_id}:{endpoint}:{current_time // window_size}"
        
        # Get current request count
        current_requests = await self.redis_client.get(window_key)
        current_requests = int(current_requests) if current_requests else 0
        
        # Check if limit exceeded
        if current_requests >= max_requests:
            # Check for burst allowance (20% extra for short periods)
            burst_key = f"burst:{user_id}:{endpoint}"
            burst_count = await self.redis_client.get(burst_key)
            burst_count = int(burst_count) if burst_count else 0
            
            if burst_count >= int(max_requests * 0.2):
                return {
                    "allowed": False,
                    "current_requests": current_requests,
                    "max_requests": max_requests,
                    "reset_time": (current_time // window_size + 1) * window_size,
                    "retry_after": window_size - (current_time % window_size)
                }
            else:
                # Allow burst request but track it
                await self.redis_client.incr(burst_key)
                await self.redis_client.expire(burst_key, window_size)
        
        # Increment request count
        await self.redis_client.incr(window_key)
        await self.redis_client.expire(window_key, window_size)
        
        return {
            "allowed": True,
            "current_requests": current_requests + 1,
            "max_requests": max_requests,
            "reset_time": (current_time // window_size + 1) * window_size,
            "remaining_requests": max_requests - current_requests - 1
        }
```

### **Comprehensive Security Middleware**
```python
# Comprehensive security middleware
class SecurityMiddleware:
    def __init__(self, app):
        self.app = app
        self.security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
            "Referrer-Policy": "strict-origin-when-cross-origin"
        }
        self.threat_detector = ThreatDetectionEngine()
    
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Security analysis
            request_analysis = await self.analyze_request_security(scope)
            
            # Threat detection
            if request_analysis["threat_level"] == "HIGH":
                response = PlainTextResponse(
                    "Request blocked due to security policy",
                    status_code=403
                )
                await response(scope, receive, send)
                return
            
            # Add security headers to response
            async def send_with_security_headers(message):
                if message["type"] == "http.response.start":
                    headers = dict(message.get("headers", []))
                    for header_name, header_value in self.security_headers.items():
                        headers[header_name.encode()] = header_value.encode()
                    message["headers"] = list(headers.items())
                await send(message)
            
            await self.app(scope, receive, send_with_security_headers)
        else:
            await self.app(scope, receive, send)
    
    async def analyze_request_security(self, scope) -> Dict[str, Any]:
        """Analyze request for security threats"""
        headers = dict(scope.get("headers", []))
        
        analysis = {
            "threat_level": "LOW",
            "security_flags": [],
            "recommendations": []
        }
        
        # Check for common attack patterns
        user_agent = headers.get(b"user-agent", b"").decode()
        if any(pattern in user_agent.lower() for pattern in ["sqlmap", "nmap", "nikto"]):
            analysis["threat_level"] = "HIGH"
            analysis["security_flags"].append("MALICIOUS_USER_AGENT")
        
        # Check for SQL injection patterns in query parameters
        query_string = scope.get("query_string", b"").decode()
        if any(pattern in query_string.lower() for pattern in ["union select", "drop table", "'; --"]):
            analysis["threat_level"] = "HIGH"
            analysis["security_flags"].append("SQL_INJECTION_ATTEMPT")
        
        # Rate limiting check
        client_ip = self.get_client_ip(scope)
        if await self.is_rate_limited(client_ip):
            analysis["threat_level"] = "MEDIUM"
            analysis["security_flags"].append("RATE_LIMIT_EXCEEDED")
        
        return analysis
```

### **Intelligent Circuit Breaker Implementation**
```python
# Intelligent circuit breaker with ML-based failure prediction
import asyncio
from enum import Enum
from typing import Dict, Any, Callable, Optional
import time
import statistics
from dataclasses import dataclass

class CircuitBreakerState(Enum):
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Blocking requests
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered

@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    timeout_duration: int = 60
    half_open_max_calls: int = 3
    success_threshold: int = 2
    slow_call_duration_threshold: float = 1.0
    slow_call_rate_threshold: float = 0.5

class IntelligentCircuitBreaker:
    def __init__(self, service_name: str, config: CircuitBreakerConfig):
        self.service_name = service_name
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.call_history = []  # For ML-based prediction
        self.half_open_calls = 0
        
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function call with circuit breaker protection"""
        
        if self.state == CircuitBreakerState.OPEN:
            if time.time() - self.last_failure_time >= self.config.timeout_duration:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
            else:
                raise CircuitBreakerError(f"Circuit breaker OPEN for {self.service_name}")
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            if self.half_open_calls >= self.config.half_open_max_calls:
                raise CircuitBreakerError(f"Half-open call limit reached for {self.service_name}")
            self.half_open_calls += 1
        
        # Execute the function call
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Record successful call
            await self.record_success(execution_time)
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            await self.record_failure(execution_time, str(e))
            raise
    
    async def record_success(self, execution_time: float):
        """Record successful call and update circuit breaker state"""
        self.call_history.append({
            'timestamp': time.time(),
            'success': True,
            'execution_time': execution_time
        })
        
        # Keep only recent history (last 100 calls)
        self.call_history = self.call_history[-100:]
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                logger.info(f"Circuit breaker CLOSED for {self.service_name}")
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)  # Reduce failure count on success
    
    def should_open_circuit(self) -> bool:
        """Intelligent decision to open circuit based on multiple factors"""
        
        # Traditional failure threshold
        if self.failure_count >= self.config.failure_threshold:
            return True
        
        # Slow call rate analysis
        recent_calls = [call for call in self.call_history 
                       if time.time() - call['timestamp'] < 60]  # Last minute
        
        if len(recent_calls) >= 10:  # Need minimum calls for analysis
            slow_calls = [call for call in recent_calls 
                         if call['execution_time'] > self.config.slow_call_duration_threshold]
            slow_call_rate = len(slow_calls) / len(recent_calls)
            
            if slow_call_rate > self.config.slow_call_rate_threshold:
                return True
        
        # ML-based prediction (simplified example)
        if len(self.call_history) >= 20:
            recent_success_rate = sum(1 for call in recent_calls if call['success']) / len(recent_calls)
            if recent_success_rate < 0.5:  # Less than 50% success rate
                return True
        
        return False
```

## **Development (D) Implementation:**

### **Sprint Planning:**
```
Sprint 1 (2 weeks): Security Foundation
- [ ] Implement zero-trust network policies with micro-segmentation
- [ ] Deploy advanced authentication with MFA and risk-based access
- [ ] Create comprehensive audit logging with immutable trails
- [ ] Set up security monitoring with SIEM integration

Sprint 2 (2 weeks): Threat Detection & Response
- [ ] Implement behavioral analysis with ML-powered anomaly detection
- [ ] Create automated incident response workflows with containment
- [ ] Deploy real-time security dashboards with threat visualization
- [ ] Add vulnerability scanning with automated remediation

Sprint 3 (2 weeks): Compliance Automation
- [ ] Implement automated compliance validation with regulatory frameworks
- [ ] Create compliance reporting with audit-ready documentation
- [ ] Deploy policy enforcement with automated governance rules
- [ ] Add privacy controls with data classification and masking
```

## **Acceptance Criteria:**
- **Given** suspicious activity detected, **when** threat threshold exceeded, **then** automated response triggers within 30 seconds
- **Given** compliance audit request, **when** reports generated, **then** 100% regulatory requirements satisfied
- **Given** security policy violation, **when** detected, **then** access automatically revoked and incident logged

## **Success Metrics:**
- Security response time: <30 seconds for automated threat containment
- Compliance score: 100% automated compliance validation
- Vulnerability detection: 99% of security issues identified before exploitation
- Audit preparation: 90% faster compliance audit preparation