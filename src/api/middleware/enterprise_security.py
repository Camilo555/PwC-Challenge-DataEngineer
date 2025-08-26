"""
Enterprise Security Middleware
Integrates all security components into FastAPI middleware for comprehensive request processing.
"""
import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import HTTPException, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from core.logging import get_logger
from core.security.enterprise_security_orchestrator import get_security_orchestrator, SecurityOrchestrationConfig
from core.security.enhanced_access_control import AccessRequest, AccessDecision, get_access_control_manager
from core.security.enterprise_dlp import EnterpriseDLPManager
from core.security.compliance_framework import get_compliance_engine
from core.tracing.correlation import get_correlation_id, set_correlation_id


logger = get_logger(__name__)


class EnterpriseSecurityMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive enterprise security middleware that processes all requests through:
    - Authentication and authorization
    - DLP scanning and redaction
    - Access control policies
    - Compliance verification
    - Security event logging
    """

    def __init__(
        self,
        app,
        config: Optional[SecurityOrchestrationConfig] = None,
        exclude_paths: Optional[List[str]] = None
    ):
        super().__init__(app)
        self.logger = get_logger(__name__)
        self.config = config or SecurityOrchestrationConfig()
        self.security_orchestrator = get_security_orchestrator(config)
        self.access_manager = get_access_control_manager()
        self.compliance_engine = get_compliance_engine()
        
        # Paths to exclude from security processing
        self.exclude_paths = exclude_paths or [
            "/health",
            "/metrics",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/favicon.ico"
        ]
        
        # Security headers to add to all responses
        self.security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Permissions-Policy": "geolocation=(), microphone=(), camera=()"
        }

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Main middleware dispatch method"""
        
        # Skip security processing for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            response = await call_next(request)
            self._add_security_headers(response)
            return response
        
        # Generate correlation ID for request tracking
        correlation_id = get_correlation_id() or str(uuid.uuid4())
        set_correlation_id(correlation_id)
        
        # Record request start time
        start_time = time.time()
        
        try:
            # Phase 1: Pre-request security processing
            security_context = await self._pre_request_security_check(request, correlation_id)
            
            if security_context.get("blocked"):
                return self._create_security_response(
                    status_code=security_context.get("status_code", 403),
                    message=security_context.get("message", "Request blocked by security policy"),
                    details=security_context,
                    correlation_id=correlation_id
                )
            
            # Phase 2: Process the actual request
            response = await call_next(request)
            
            # Phase 3: Post-request security processing
            processed_response = await self._post_request_security_processing(
                request, response, security_context, correlation_id
            )
            
            # Phase 4: Add security headers and metadata
            self._add_security_headers(processed_response)
            self._add_security_metadata(processed_response, security_context, correlation_id)
            
            # Log successful request processing
            processing_time = (time.time() - start_time) * 1000
            self.logger.info(
                f"Request processed successfully",
                extra={
                    "correlation_id": correlation_id,
                    "path": request.url.path,
                    "method": request.method,
                    "processing_time_ms": processing_time,
                    "security_context": security_context
                }
            )
            
            return processed_response
            
        except HTTPException as e:
            # Handle FastAPI HTTP exceptions
            response = JSONResponse(
                status_code=e.status_code,
                content={
                    "error": e.detail,
                    "correlation_id": correlation_id,
                    "timestamp": datetime.now().isoformat()
                }
            )
            self._add_security_headers(response)
            return response
            
        except Exception as e:
            # Handle unexpected errors
            self.logger.error(
                f"Security middleware error: {e}",
                extra={"correlation_id": correlation_id, "path": request.url.path},
                exc_info=True
            )
            
            response = JSONResponse(
                status_code=500,
                content={
                    "error": "Internal security processing error",
                    "correlation_id": correlation_id,
                    "timestamp": datetime.now().isoformat()
                }
            )
            self._add_security_headers(response)
            return response

    async def _pre_request_security_check(self, request: Request, correlation_id: str) -> Dict[str, Any]:
        """Comprehensive pre-request security checks"""
        
        context = {
            "correlation_id": correlation_id,
            "request_id": str(uuid.uuid4()),
            "timestamp": datetime.now(),
            "source_ip": self._get_client_ip(request),
            "user_agent": request.headers.get("user-agent", "unknown"),
            "path": request.url.path,
            "method": request.method,
            "blocked": False
        }
        
        try:
            # 1. Extract user identity
            user_id = await self._extract_user_identity(request)
            context["user_id"] = user_id
            
            # 2. Rate limiting check (handled by existing middleware)
            # This could be enhanced with threat-aware scaling
            
            # 3. Access control check
            if user_id:
                access_result = await self._check_access_control(request, user_id, context)
                context["access_control"] = access_result
                
                if access_result["decision"] == AccessDecision.DENY.value:
                    context.update({
                        "blocked": True,
                        "status_code": 403,
                        "message": f"Access denied: {access_result['reason']}"
                    })
                    return context
                elif access_result["decision"] == AccessDecision.REQUIRES_ELEVATION.value:
                    context.update({
                        "blocked": True,
                        "status_code": 403,
                        "message": "Privilege elevation required for this operation"
                    })
                    return context
            
            # 4. Threat detection (basic IP-based checks)
            threat_check = await self._perform_threat_detection(request, context)
            context["threat_analysis"] = threat_check
            
            if threat_check.get("high_risk", False):
                context.update({
                    "blocked": True,
                    "status_code": 429,
                    "message": "Request blocked due to security threat detection"
                })
                return context
            
            # 5. Compliance pre-check
            compliance_check = await self._pre_compliance_check(request, context)
            context["compliance"] = compliance_check
            
            return context
            
        except Exception as e:
            self.logger.error(f"Pre-request security check failed: {e}", exc_info=True)
            context.update({
                "blocked": True,
                "status_code": 500,
                "message": "Security check failed"
            })
            return context

    async def _post_request_security_processing(
        self, 
        request: Request, 
        response: Response, 
        security_context: Dict[str, Any],
        correlation_id: str
    ) -> Response:
        """Post-request security processing including DLP and response filtering"""
        
        try:
            # Skip processing for non-JSON responses or specific status codes
            if (response.status_code >= 400 or 
                not response.headers.get("content-type", "").startswith("application/json")):
                return response
            
            # Read response body
            response_body = b""
            async for chunk in response.body_iterator:
                response_body += chunk
            
            if not response_body:
                return response
            
            try:
                response_data = json.loads(response_body.decode())
            except json.JSONDecodeError:
                # If not JSON, return as-is
                return Response(
                    content=response_body,
                    status_code=response.status_code,
                    headers=dict(response.headers)
                )
            
            # Apply DLP scanning and redaction
            if self.config.enable_dlp:
                dlp_result = await self._apply_dlp_processing(
                    response_data, request, security_context
                )
                
                if dlp_result.get("action") == "blocked":
                    return JSONResponse(
                        status_code=403,
                        content={
                            "error": "Response blocked by DLP policy",
                            "correlation_id": correlation_id,
                            "dlp_scan_id": dlp_result.get("scan_id"),
                            "timestamp": datetime.now().isoformat()
                        }
                    )
                
                # Use processed data if DLP made changes
                if "data" in dlp_result:
                    response_data = dlp_result["data"]
                
                # Add DLP metadata to security context
                security_context["dlp_analysis"] = {
                    "scan_id": dlp_result.get("scan_id"),
                    "detections": dlp_result.get("detections", 0),
                    "risk_score": dlp_result.get("risk_score", 0),
                    "sensitive_data_types": dlp_result.get("sensitive_data_types", [])
                }
            
            # Apply data classification headers
            classification = self._determine_data_classification(response_data)
            security_context["data_classification"] = classification
            
            # Create processed response
            processed_response = JSONResponse(
                content=response_data,
                status_code=response.status_code,
                headers=dict(response.headers)
            )
            
            # Add data classification header
            processed_response.headers["X-Data-Classification"] = classification
            
            return processed_response
            
        except Exception as e:
            self.logger.error(f"Post-request security processing failed: {e}", exc_info=True)
            # Return original response if processing fails
            return response

    async def _extract_user_identity(self, request: Request) -> Optional[str]:
        """Extract user identity from request"""
        
        try:
            # Try to get user from JWT token
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header[7:]
                # This would typically decode and validate the JWT
                # For now, we'll extract a simple user ID
                try:
                    import base64
                    import json
                    
                    # Decode JWT payload (without validation for demo)
                    parts = token.split('.')
                    if len(parts) >= 2:
                        payload = parts[1]
                        # Add padding if needed
                        padding = 4 - (len(payload) % 4)
                        if padding != 4:
                            payload += '=' * padding
                        
                        decoded = base64.b64decode(payload)
                        token_data = json.loads(decoded)
                        return token_data.get("sub") or token_data.get("user_id")
                        
                except Exception:
                    pass
            
            # Try to get user from basic auth or other sources
            # This could be extended for different auth mechanisms
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to extract user identity: {e}")
            return None

    async def _check_access_control(
        self, 
        request: Request, 
        user_id: str, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform comprehensive access control check"""
        
        try:
            # Create access request
            access_request = AccessRequest(
                request_id=context["request_id"],
                subject_id=user_id,
                action=self._map_http_method_to_action(request.method),
                resource_id=self._extract_resource_id(request),
                resource_type=self._determine_resource_type(request),
                context={
                    "source_ip": context["source_ip"],
                    "user_agent": context["user_agent"],
                    "path": request.url.path,
                    "query_params": dict(request.query_params),
                    "session_id": context.get("session_id")
                },
                session_id=context.get("session_id"),
                source_ip=context["source_ip"]
            )
            
            # Evaluate access using enhanced access control
            evaluation = self.access_manager.check_access(access_request)
            
            return {
                "decision": evaluation.decision.value,
                "reason": evaluation.reason,
                "risk_score": evaluation.risk_score,
                "evaluation_time_ms": evaluation.evaluation_time_ms,
                "applicable_rules": evaluation.applicable_rules,
                "required_conditions": evaluation.required_conditions
            }
            
        except Exception as e:
            self.logger.error(f"Access control check failed: {e}")
            return {
                "decision": AccessDecision.DENY.value,
                "reason": f"Access control error: {str(e)}",
                "risk_score": 10.0
            }

    async def _perform_threat_detection(self, request: Request, context: Dict[str, Any]) -> Dict[str, Any]:
        """Basic threat detection analysis"""
        
        threat_indicators = {
            "high_risk": False,
            "risk_score": 0.0,
            "indicators": []
        }
        
        try:
            source_ip = context["source_ip"]
            user_agent = context["user_agent"]
            
            # Check for suspicious patterns
            suspicious_patterns = [
                "sqlmap", "nmap", "burp", "owasp", "nikto", 
                "script", "alert", "javascript:", "vbscript:",
                "../", "..\\", "etc/passwd", "cmd.exe"
            ]
            
            request_str = f"{request.url} {user_agent}".lower()
            
            for pattern in suspicious_patterns:
                if pattern in request_str:
                    threat_indicators["indicators"].append(f"Suspicious pattern: {pattern}")
                    threat_indicators["risk_score"] += 2.0
            
            # Check request frequency (basic rate limiting awareness)
            # This would be enhanced with actual rate limiting data
            
            # Check for known bad IPs (would integrate with threat intelligence)
            if source_ip and (source_ip.startswith("10.") or source_ip.startswith("192.168.")):
                # Internal IPs are generally safe
                pass
            elif source_ip == "127.0.0.1":
                pass  # Localhost
            else:
                # External IP - could check against threat feeds
                pass
            
            # Determine if high risk
            if threat_indicators["risk_score"] >= 5.0:
                threat_indicators["high_risk"] = True
            
            return threat_indicators
            
        except Exception as e:
            self.logger.error(f"Threat detection failed: {e}")
            return {"high_risk": False, "risk_score": 0.0, "error": str(e)}

    async def _pre_compliance_check(self, request: Request, context: Dict[str, Any]) -> Dict[str, Any]:
        """Pre-request compliance verification"""
        
        try:
            # Check if operation requires compliance verification
            sensitive_operations = ["delete", "export", "bulk_download"]
            operation = self._extract_operation_type(request)
            
            compliance_result = {
                "requires_verification": operation in sensitive_operations,
                "frameworks_applicable": [],
                "status": "compliant"
            }
            
            if compliance_result["requires_verification"]:
                # Determine applicable compliance frameworks
                if "gdpr" in request.url.path.lower() or "personal" in request.url.path.lower():
                    compliance_result["frameworks_applicable"].append("GDPR")
                
                if "payment" in request.url.path.lower() or "card" in request.url.path.lower():
                    compliance_result["frameworks_applicable"].append("PCI-DSS")
                
                if "health" in request.url.path.lower() or "medical" in request.url.path.lower():
                    compliance_result["frameworks_applicable"].append("HIPAA")
            
            return compliance_result
            
        except Exception as e:
            self.logger.error(f"Pre-compliance check failed: {e}")
            return {"status": "error", "error": str(e)}

    async def _apply_dlp_processing(
        self, 
        response_data: Any, 
        request: Request, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply DLP scanning and processing to response data"""
        
        try:
            # Prepare DLP context
            dlp_context = {
                "user_id": context.get("user_id"),
                "operation": self._map_http_method_to_action(request.method),
                "location": "api_response",
                "session_id": context.get("session_id"),
                "source_ip": context.get("source_ip"),
                "endpoint": request.url.path
            }
            
            # Process through security orchestrator
            result = await self.security_orchestrator.process_data_request(
                user_id=context.get("user_id", "anonymous"),
                data=response_data,
                operation="read",  # Response is being read
                resource_id=self._extract_resource_id(request),
                context=dlp_context
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"DLP processing failed: {e}")
            return {
                "action": "error",
                "error": str(e),
                "data": response_data  # Return original data on error
            }

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address"""
        # Check for forwarded headers first
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip
        
        return getattr(request.client, "host", "unknown")

    def _map_http_method_to_action(self, method: str) -> str:
        """Map HTTP method to security action"""
        mapping = {
            "GET": "read",
            "POST": "create",
            "PUT": "update",
            "PATCH": "update",
            "DELETE": "delete",
            "HEAD": "read",
            "OPTIONS": "read"
        }
        return mapping.get(method.upper(), "unknown")

    def _extract_resource_id(self, request: Request) -> str:
        """Extract resource identifier from request"""
        # Simple resource ID extraction from URL path
        path_parts = request.url.path.strip("/").split("/")
        
        # Look for UUID-like patterns or numeric IDs
        for part in reversed(path_parts):
            if part.isdigit() or len(part) == 36:  # UUID length
                return part
        
        # Fall back to the last path component
        return path_parts[-1] if path_parts else "unknown"

    def _determine_resource_type(self, request: Request) -> str:
        """Determine resource type from request path"""
        path_parts = request.url.path.strip("/").split("/")
        
        # Look for API version and resource type
        if len(path_parts) >= 3 and path_parts[0] == "api":
            return path_parts[2]  # e.g., /api/v1/sales -> sales
        elif len(path_parts) >= 1:
            return path_parts[0]
        
        return "unknown"

    def _extract_operation_type(self, request: Request) -> str:
        """Extract operation type from request"""
        path = request.url.path.lower()
        
        if "export" in path or "download" in path:
            return "export"
        elif "delete" in path or request.method == "DELETE":
            return "delete"
        elif "bulk" in path:
            return "bulk_operation"
        
        return self._map_http_method_to_action(request.method)

    def _determine_data_classification(self, data: Any) -> str:
        """Determine data classification level"""
        # Simple classification logic
        if isinstance(data, dict):
            data_str = json.dumps(data).lower()
            
            if any(term in data_str for term in ["ssn", "credit_card", "medical", "health"]):
                return "restricted"
            elif any(term in data_str for term in ["email", "phone", "personal"]):
                return "confidential"
            elif any(term in data_str for term in ["internal", "employee"]):
                return "internal"
        
        return "public"

    def _add_security_headers(self, response: Response) -> None:
        """Add security headers to response"""
        for header, value in self.security_headers.items():
            response.headers[header] = value

    def _add_security_metadata(
        self, 
        response: Response, 
        security_context: Dict[str, Any], 
        correlation_id: str
    ) -> None:
        """Add security metadata headers"""
        response.headers["X-Correlation-ID"] = correlation_id
        response.headers["X-Security-Processed"] = "true"
        
        if "dlp_analysis" in security_context:
            dlp = security_context["dlp_analysis"]
            response.headers["X-DLP-Scan-ID"] = dlp.get("scan_id", "")
            response.headers["X-DLP-Risk-Score"] = str(dlp.get("risk_score", 0))

    def _create_security_response(
        self, 
        status_code: int, 
        message: str, 
        details: Dict[str, Any], 
        correlation_id: str
    ) -> JSONResponse:
        """Create standardized security response"""
        
        response_content = {
            "error": message,
            "correlation_id": correlation_id,
            "timestamp": datetime.now().isoformat(),
            "security_details": {
                "blocked_by": "enterprise_security_middleware",
                "reason": details.get("message", message),
                "risk_score": details.get("risk_score", 0.0)
            }
        }
        
        # Add relevant context without sensitive data
        if "access_control" in details:
            response_content["security_details"]["access_control"] = {
                "decision": details["access_control"]["decision"],
                "reason": details["access_control"]["reason"]
            }
        
        response = JSONResponse(
            status_code=status_code,
            content=response_content
        )
        
        self._add_security_headers(response)
        response.headers["X-Correlation-ID"] = correlation_id
        
        return response


class WebSocketSecurityMiddleware:
    """Security middleware for WebSocket connections"""
    
    def __init__(self, config: Optional[SecurityOrchestrationConfig] = None):
        self.logger = get_logger(__name__)
        self.config = config or SecurityOrchestrationConfig()
        self.access_manager = get_access_control_manager()
        self.active_connections: Dict[str, Dict[str, Any]] = {}

    async def authenticate_websocket(self, websocket, token: Optional[str] = None) -> Dict[str, Any]:
        """Authenticate WebSocket connection"""
        
        try:
            if not token:
                # Try to get token from query parameters
                token = websocket.query_params.get("token")
            
            if not token:
                return {"authenticated": False, "reason": "No authentication token provided"}
            
            # Validate token (similar to JWT validation in HTTP middleware)
            user_id = await self._validate_websocket_token(token)
            
            if not user_id:
                return {"authenticated": False, "reason": "Invalid authentication token"}
            
            # Check access permissions for WebSocket endpoints
            access_check = await self._check_websocket_access(user_id, websocket)
            
            if not access_check["allowed"]:
                return {"authenticated": False, "reason": access_check["reason"]}
            
            # Track active connection
            connection_id = str(uuid.uuid4())
            self.active_connections[connection_id] = {
                "user_id": user_id,
                "connected_at": datetime.now(),
                "websocket": websocket,
                "last_activity": datetime.now()
            }
            
            return {
                "authenticated": True,
                "user_id": user_id,
                "connection_id": connection_id,
                "permissions": access_check.get("permissions", [])
            }
            
        except Exception as e:
            self.logger.error(f"WebSocket authentication failed: {e}")
            return {"authenticated": False, "reason": "Authentication error"}

    async def _validate_websocket_token(self, token: str) -> Optional[str]:
        """Validate WebSocket authentication token"""
        try:
            # This would use the same JWT validation logic as HTTP requests
            # For demo purposes, we'll do a simple validation
            import base64
            import json
            
            parts = token.split('.')
            if len(parts) >= 2:
                payload = parts[1]
                padding = 4 - (len(payload) % 4)
                if padding != 4:
                    payload += '=' * padding
                
                decoded = base64.b64decode(payload)
                token_data = json.loads(decoded)
                return token_data.get("sub") or token_data.get("user_id")
                
        except Exception as e:
            self.logger.error(f"Token validation failed: {e}")
        
        return None

    async def _check_websocket_access(self, user_id: str, websocket) -> Dict[str, Any]:
        """Check WebSocket access permissions"""
        
        try:
            # Create access request for WebSocket
            access_request = AccessRequest(
                request_id=str(uuid.uuid4()),
                subject_id=user_id,
                action="websocket_connect",
                resource_id="security_dashboard",
                resource_type="websocket",
                context={
                    "connection_type": "websocket",
                    "endpoint": str(websocket.url)
                }
            )
            
            # Evaluate access
            evaluation = self.access_manager.check_access(access_request)
            
            return {
                "allowed": evaluation.decision in [AccessDecision.ALLOW, AccessDecision.CONDITIONAL_ALLOW],
                "reason": evaluation.reason,
                "permissions": ["security_dashboard"] if evaluation.decision == AccessDecision.ALLOW else []
            }
            
        except Exception as e:
            self.logger.error(f"WebSocket access check failed: {e}")
            return {"allowed": False, "reason": "Access check error"}

    async def cleanup_inactive_connections(self):
        """Clean up inactive WebSocket connections"""
        
        current_time = datetime.now()
        inactive_threshold = current_time.replace(minute=current_time.minute - 30)  # 30 minutes
        
        inactive_connections = [
            conn_id for conn_id, conn_info in self.active_connections.items()
            if conn_info["last_activity"] < inactive_threshold
        ]
        
        for conn_id in inactive_connections:
            try:
                conn_info = self.active_connections[conn_id]
                await conn_info["websocket"].close(code=1000, reason="Connection timeout")
                del self.active_connections[conn_id]
                
                self.logger.info(f"Cleaned up inactive WebSocket connection: {conn_id}")
                
            except Exception as e:
                self.logger.error(f"Error cleaning up WebSocket connection {conn_id}: {e}")

    def update_connection_activity(self, connection_id: str):
        """Update last activity timestamp for connection"""
        if connection_id in self.active_connections:
            self.active_connections[connection_id]["last_activity"] = datetime.now()


# Global middleware instances
_enterprise_security_middleware: Optional[EnterpriseSecurityMiddleware] = None
_websocket_security_middleware: Optional[WebSocketSecurityMiddleware] = None

def get_enterprise_security_middleware(
    config: Optional[SecurityOrchestrationConfig] = None,
    exclude_paths: Optional[List[str]] = None
) -> EnterpriseSecurityMiddleware:
    """Get global enterprise security middleware instance"""
    global _enterprise_security_middleware
    if _enterprise_security_middleware is None:
        _enterprise_security_middleware = EnterpriseSecurityMiddleware(
            app=None,  # Will be set when added to app
            config=config,
            exclude_paths=exclude_paths
        )
    return _enterprise_security_middleware

def get_websocket_security_middleware(
    config: Optional[SecurityOrchestrationConfig] = None
) -> WebSocketSecurityMiddleware:
    """Get global WebSocket security middleware instance"""
    global _websocket_security_middleware
    if _websocket_security_middleware is None:
        _websocket_security_middleware = WebSocketSecurityMiddleware(config)
    return _websocket_security_middleware