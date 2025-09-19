"""
Advanced Input Validation Middleware
Provides comprehensive input validation, sanitization, and security checks.
"""
from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional, Pattern, Set, Union
from datetime import datetime
from enum import Enum
from functools import lru_cache

from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel, Field, validator
import bleach

from core.logging import get_logger
from core.exceptions import ValidationException
from core.config.base_config import BaseConfig

logger = get_logger(__name__)
config = BaseConfig()


class ValidationLevel(Enum):
    """Validation strictness levels."""
    BASIC = "basic"
    STANDARD = "standard"
    STRICT = "strict"
    ENTERPRISE = "enterprise"


class InputType(Enum):
    """Types of input validation."""
    JSON = "json"
    QUERY_PARAMS = "query_params"
    PATH_PARAMS = "path_params"
    HEADERS = "headers"
    FORM_DATA = "form_data"


class SecurityThreat(Enum):
    """Types of security threats to validate against."""
    SQL_INJECTION = "sql_injection"
    XSS = "xss"
    COMMAND_INJECTION = "command_injection"
    PATH_TRAVERSAL = "path_traversal"
    LDAP_INJECTION = "ldap_injection"
    XML_INJECTION = "xml_injection"
    NOSQL_INJECTION = "nosql_injection"
    PROTOTYPE_POLLUTION = "prototype_pollution"


class ValidationConfig(BaseModel):
    """Configuration for input validation."""
    
    validation_level: ValidationLevel = ValidationLevel.STANDARD
    max_request_size: int = Field(default=10 * 1024 * 1024, description="10MB default")
    max_json_depth: int = Field(default=10, ge=1, le=50)
    max_array_length: int = Field(default=1000, ge=1, le=10000)
    max_string_length: int = Field(default=10000, ge=1, le=100000)
    max_query_params: int = Field(default=50, ge=1, le=200)
    
    # Security settings
    enable_xss_protection: bool = True
    enable_sql_injection_protection: bool = True
    enable_command_injection_protection: bool = True
    enable_path_traversal_protection: bool = True
    
    # Content filtering
    allowed_content_types: Set[str] = {
        "application/json",
        "application/x-www-form-urlencoded",
        "multipart/form-data",
        "text/plain"
    }
    
    # Rate limiting for validation
    max_validation_time_ms: int = 1000
    
    # Custom validation rules
    custom_patterns: Dict[str, str] = {}
    blocked_patterns: List[str] = []
    
    class Config:
        use_enum_values = True


@lru_cache(maxsize=100)
def compile_security_patterns() -> Dict[SecurityThreat, List[Pattern]]:
    """Compile security validation patterns with caching."""
    patterns = {
        SecurityThreat.SQL_INJECTION: [
            re.compile(r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|EXECUTE)\b)", re.IGNORECASE),
            re.compile(r"(\b(UNION|JOIN|FROM|WHERE|ORDER BY|GROUP BY)\b)", re.IGNORECASE),
            re.compile(r"(\b(OR|AND)\s+\w+\s*=\s*\w+)", re.IGNORECASE),
            re.compile(r"(\b(OR|AND)\s+\d+\s*=\s*\d+)", re.IGNORECASE),
            re.compile(r"([\'\"];?\s*(OR|AND)\s+[\'\"]?\w+[\'\"]?\s*=\s*[\'\"]?\w+[\'\"]?)", re.IGNORECASE),
            re.compile(r"(\b(CHAR|NCHAR|VARCHAR|NVARCHAR|CONCAT)\s*\()", re.IGNORECASE),
            re.compile(r"(WAITFOR\s+DELAY)", re.IGNORECASE),
        ],
        
        SecurityThreat.XSS: [
            re.compile(r"<\s*script[^>]*>", re.IGNORECASE),
            re.compile(r"javascript:", re.IGNORECASE),
            re.compile(r"on\w+\s*=", re.IGNORECASE),
            re.compile(r"<\s*iframe[^>]*>", re.IGNORECASE),
            re.compile(r"<\s*object[^>]*>", re.IGNORECASE),
            re.compile(r"<\s*embed[^>]*>", re.IGNORECASE),
            re.compile(r"vbscript:", re.IGNORECASE),
            re.compile(r"expression\s*\(", re.IGNORECASE),
        ],
        
        SecurityThreat.COMMAND_INJECTION: [
            re.compile(r"(\||&|;|`|\$\(|\${)", re.IGNORECASE),
            re.compile(r"(nc|netcat|telnet|wget|curl)\s+", re.IGNORECASE),
            re.compile(r"(rm|mv|cp|chmod|chown)\s+", re.IGNORECASE),
            re.compile(r"(cat|less|more|head|tail)\s+", re.IGNORECASE),
            re.compile(r"(ps|kill|killall|pkill)\s+", re.IGNORECASE),
        ],
        
        SecurityThreat.PATH_TRAVERSAL: [
            re.compile(r"\.\./"),
            re.compile(r"\.\.\\"),
            re.compile(r"%2e%2e%2f", re.IGNORECASE),
            re.compile(r"%2e%2e\\", re.IGNORECASE),
            re.compile(r"\.\.%2f", re.IGNORECASE),
            re.compile(r"\.\.%5c", re.IGNORECASE),
        ],
        
        SecurityThreat.LDAP_INJECTION: [
            re.compile(r"(\*|\(|\)|&|\|)", re.IGNORECASE),
            re.compile(r"(\b(objectClass|cn|uid|sn|mail)\s*=)", re.IGNORECASE),
        ],
        
        SecurityThreat.XML_INJECTION: [
            re.compile(r"<!\[CDATA\[", re.IGNORECASE),
            re.compile(r"<!DOCTYPE", re.IGNORECASE),
            re.compile(r"<!ENTITY", re.IGNORECASE),
            re.compile(r"<\?xml", re.IGNORECASE),
        ],
        
        SecurityThreat.NOSQL_INJECTION: [
            re.compile(r"(\$where|\$ne|\$gt|\$lt|\$gte|\$lte|\$in|\$nin|\$regex)", re.IGNORECASE),
            re.compile(r"({.*\$.*:.*})", re.IGNORECASE),
            re.compile(r"(function\s*\(.*\)\s*{)", re.IGNORECASE),
        ]
    }
    
    return patterns


class ValidationResult:
    """Result of input validation."""
    
    def __init__(self):
        self.is_valid = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.sanitized_data: Optional[Any] = None
        self.threats_detected: List[SecurityThreat] = []
        self.processing_time_ms: float = 0
    
    def add_error(self, field: str, message: str, threat_type: Optional[SecurityThreat] = None):
        """Add validation error."""
        self.is_valid = False
        error = {
            "field": field,
            "message": message,
            "type": "validation_error"
        }
        if threat_type:
            error["threat_type"] = threat_type.value
            if threat_type not in self.threats_detected:
                self.threats_detected.append(threat_type)
        self.errors.append(error)
    
    def add_warning(self, field: str, message: str):
        """Add validation warning."""
        self.warnings.append({
            "field": field,
            "message": message,
            "type": "validation_warning"
        })


class InputValidator:
    """Comprehensive input validator with security checks."""
    
    def __init__(self, config: ValidationConfig):
        self.config = config
        self.security_patterns = compile_security_patterns()
    
    def validate_request(self, request: Request, body: bytes = None) -> ValidationResult:
        """Validate entire request."""
        start_time = time.time()
        result = ValidationResult()
        
        try:
            # Validate request size
            self._validate_request_size(request, body, result)
            
            # Validate content type
            self._validate_content_type(request, result)
            
            # Validate headers
            self._validate_headers(request, result)
            
            # Validate query parameters
            self._validate_query_params(request, result)
            
            # Validate path parameters
            self._validate_path_params(request, result)
            
            # Validate request body if present
            if body:
                self._validate_request_body(body, request.headers.get("content-type", ""), result)
            
        except Exception as e:
            result.add_error("request", f"Validation failed: {str(e)}")
            logger.error(f"Request validation failed: {e}")
        
        result.processing_time_ms = (time.time() - start_time) * 1000
        return result
    
    def _validate_request_size(self, request: Request, body: bytes, result: ValidationResult):
        """Validate request size limits."""
        if body and len(body) > self.config.max_request_size:
            result.add_error(
                "request_body",
                f"Request body too large: {len(body)} bytes (max: {self.config.max_request_size})"
            )
    
    def _validate_content_type(self, request: Request, result: ValidationResult):
        """Validate content type."""
        content_type = request.headers.get("content-type", "")
        
        # Extract base content type (without charset, etc.)
        base_content_type = content_type.split(";")[0].strip().lower()
        
        if base_content_type and base_content_type not in self.config.allowed_content_types:
            result.add_error(
                "content_type",
                f"Content type not allowed: {base_content_type}"
            )
    
    def _validate_headers(self, request: Request, result: ValidationResult):
        """Validate request headers."""
        for name, value in request.headers.items():
            # Check header name
            if not re.match(r'^[a-zA-Z0-9\-_]+$', name):
                result.add_error("headers", f"Invalid header name: {name}")
            
            # Check header value for security threats
            self._check_security_threats(f"header_{name}", value, result)
            
            # Validate specific security headers
            if name.lower() == "user-agent":
                if len(value) > 1000:  # Reasonable user agent length
                    result.add_warning("user_agent", "User agent string is unusually long")
    
    def _validate_query_params(self, request: Request, result: ValidationResult):
        """Validate query parameters."""
        query_params = dict(request.query_params)
        
        # Check number of parameters
        if len(query_params) > self.config.max_query_params:
            result.add_error(
                "query_params",
                f"Too many query parameters: {len(query_params)} (max: {self.config.max_query_params})"
            )
        
        # Validate each parameter
        for key, value in query_params.items():
            # Check parameter name
            if not re.match(r'^[a-zA-Z0-9\-_.]+$', key):
                result.add_error("query_params", f"Invalid parameter name: {key}")
            
            # Check parameter value
            if isinstance(value, str):
                if len(value) > self.config.max_string_length:
                    result.add_error(
                        f"query_param_{key}",
                        f"Parameter value too long: {len(value)} chars (max: {self.config.max_string_length})"
                    )
                
                # Check for security threats
                self._check_security_threats(f"query_param_{key}", value, result)
    
    def _validate_path_params(self, request: Request, result: ValidationResult):
        """Validate path parameters."""
        path_params = getattr(request, "path_params", {})
        
        for key, value in path_params.items():
            if isinstance(value, str):
                # Check for path traversal
                self._check_security_threats(f"path_param_{key}", value, result)
                
                # Basic format validation
                if len(value) > 255:  # Reasonable path parameter length
                    result.add_error(
                        f"path_param_{key}",
                        f"Path parameter too long: {len(value)} chars"
                    )
    
    def _validate_request_body(self, body: bytes, content_type: str, result: ValidationResult):
        """Validate request body content."""
        try:
            body_str = body.decode('utf-8')
        except UnicodeDecodeError:
            result.add_error("request_body", "Request body contains invalid UTF-8")
            return
        
        # Validate JSON content
        if "application/json" in content_type:
            self._validate_json_body(body_str, result)
        
        # General security checks on body content
        self._check_security_threats("request_body", body_str, result)
        
        # Check for suspicious patterns
        self._check_custom_patterns(body_str, result)
    
    def _validate_json_body(self, body_str: str, result: ValidationResult):
        """Validate JSON request body."""
        try:
            json_data = json.loads(body_str)
            
            # Check JSON depth
            depth = self._get_json_depth(json_data)
            if depth > self.config.max_json_depth:
                result.add_error(
                    "json_body",
                    f"JSON too deeply nested: {depth} levels (max: {self.config.max_json_depth})"
                )
            
            # Check array lengths
            self._validate_json_arrays(json_data, result)
            
            # Check string lengths in JSON
            self._validate_json_strings(json_data, result)
            
        except json.JSONDecodeError as e:
            result.add_error("json_body", f"Invalid JSON: {str(e)}")
    
    def _get_json_depth(self, obj: Any, current_depth: int = 1) -> int:
        """Calculate maximum depth of JSON object."""
        if isinstance(obj, dict):
            if not obj:
                return current_depth
            return max(self._get_json_depth(v, current_depth + 1) for v in obj.values())
        elif isinstance(obj, list):
            if not obj:
                return current_depth
            return max(self._get_json_depth(item, current_depth + 1) for item in obj)
        else:
            return current_depth
    
    def _validate_json_arrays(self, obj: Any, result: ValidationResult, path: str = ""):
        """Validate array lengths in JSON."""
        if isinstance(obj, list):
            if len(obj) > self.config.max_array_length:
                result.add_error(
                    f"json_array{path}",
                    f"Array too large: {len(obj)} items (max: {self.config.max_array_length})"
                )
            for i, item in enumerate(obj):
                self._validate_json_arrays(item, result, f"{path}[{i}]")
        elif isinstance(obj, dict):
            for key, value in obj.items():
                self._validate_json_arrays(value, result, f"{path}.{key}")
    
    def _validate_json_strings(self, obj: Any, result: ValidationResult, path: str = ""):
        """Validate string lengths in JSON."""
        if isinstance(obj, str):
            if len(obj) > self.config.max_string_length:
                result.add_error(
                    f"json_string{path}",
                    f"String too long: {len(obj)} chars (max: {self.config.max_string_length})"
                )
            # Check for security threats in string values
            self._check_security_threats(f"json_string{path}", obj, result)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                self._validate_json_strings(value, result, f"{path}.{key}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                self._validate_json_strings(item, result, f"{path}[{i}]")
    
    def _check_security_threats(self, field: str, value: str, result: ValidationResult):
        """Check for various security threats in input."""
        if not isinstance(value, str) or not value:
            return
        
        # Check each security threat type
        for threat_type, patterns in self.security_patterns.items():
            # Skip disabled checks
            if threat_type == SecurityThreat.XSS and not self.config.enable_xss_protection:
                continue
            if threat_type == SecurityThreat.SQL_INJECTION and not self.config.enable_sql_injection_protection:
                continue
            if threat_type == SecurityThreat.COMMAND_INJECTION and not self.config.enable_command_injection_protection:
                continue
            if threat_type == SecurityThreat.PATH_TRAVERSAL and not self.config.enable_path_traversal_protection:
                continue
            
            # Check patterns
            for pattern in patterns:
                if pattern.search(value):
                    result.add_error(
                        field,
                        f"Potential {threat_type.value.replace('_', ' ')} detected",
                        threat_type
                    )
                    break  # One match per threat type is enough
    
    def _check_custom_patterns(self, value: str, result: ValidationResult):
        """Check custom blocked patterns."""
        for pattern_str in self.config.blocked_patterns:
            try:
                pattern = re.compile(pattern_str, re.IGNORECASE)
                if pattern.search(value):
                    result.add_error(
                        "custom_pattern",
                        f"Input matches blocked pattern: {pattern_str}"
                    )
            except re.error as e:
                logger.warning(f"Invalid custom pattern '{pattern_str}': {e}")
    
    def sanitize_input(self, value: Any) -> Any:
        """Sanitize input to remove/escape dangerous content."""
        if isinstance(value, str):
            # HTML sanitization for XSS protection
            if self.config.enable_xss_protection:
                value = bleach.clean(
                    value,
                    tags=[],  # No HTML tags allowed
                    attributes={},  # No attributes allowed
                    strip=True
                )
            
            # Basic escaping for other injection types
            # Note: This is basic sanitization. Consider using more specialized libraries
            # for production use cases
            value = value.replace("'", "&#39;")
            value = value.replace('"', "&#34;")
            value = value.replace("<", "&lt;")
            value = value.replace(">", "&gt;")
            
        elif isinstance(value, dict):
            return {k: self.sanitize_input(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self.sanitize_input(item) for item in value]
        
        return value


class InputValidationMiddleware(BaseHTTPMiddleware):
    """Middleware for comprehensive input validation."""
    
    def __init__(self, app, config: Optional[ValidationConfig] = None):
        super().__init__(app)
        self.config = config or ValidationConfig()
        self.validator = InputValidator(self.config)
        self.excluded_paths = {
            "/health",
            "/metrics",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/favicon.ico"
        }
    
    async def dispatch(self, request: Request, call_next):
        """Process request with input validation."""
        # Skip validation for excluded paths
        if request.url.path in self.excluded_paths:
            return await call_next(request)
        
        # Read request body if present
        body = None
        if request.method in ["POST", "PUT", "PATCH"]:
            body = await request.body()
        
        # Validate input
        start_time = time.time()
        
        # Check validation timeout
        if self.config.max_validation_time_ms > 0:
            timeout = self.config.max_validation_time_ms / 1000.0
            try:
                import asyncio
                validation_result = await asyncio.wait_for(
                    self._validate_async(request, body),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"Input validation timeout for {request.url.path}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Request validation timeout"
                )
        else:
            validation_result = self.validator.validate_request(request, body)
        
        # Handle validation results
        if not validation_result.is_valid:
            validation_time = (time.time() - start_time) * 1000
            
            # Log validation failure
            logger.warning(
                f"Input validation failed for {request.method} {request.url.path}",
                extra={
                    "validation_errors": validation_result.errors,
                    "threats_detected": [t.value for t in validation_result.threats_detected],
                    "validation_time_ms": validation_time,
                    "client_ip": request.client.host if request.client else "unknown"
                }
            )
            
            # Return validation error
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "Input validation failed",
                    "errors": validation_result.errors,
                    "warnings": validation_result.warnings,
                    "threats_detected": [t.value for t in validation_result.threats_detected]
                }
            )
        
        # Log warnings if any
        if validation_result.warnings:
            logger.info(
                f"Input validation warnings for {request.method} {request.url.path}",
                extra={
                    "validation_warnings": validation_result.warnings,
                    "validation_time_ms": validation_result.processing_time_ms
                }
            )
        
        # Proceed with request
        return await call_next(request)
    
    async def _validate_async(self, request: Request, body: bytes) -> ValidationResult:
        """Async wrapper for validation."""
        return self.validator.validate_request(request, body)


# Factory functions
def create_input_validation_middleware(
    validation_level: ValidationLevel = ValidationLevel.STANDARD,
    max_request_size: int = 10 * 1024 * 1024,
    **kwargs
) -> InputValidationMiddleware:
    """Create input validation middleware with custom configuration."""
    config = ValidationConfig(
        validation_level=validation_level,
        max_request_size=max_request_size,
        **kwargs
    )
    return InputValidationMiddleware(None, config)


# Global middleware instance
_validation_middleware: Optional[InputValidationMiddleware] = None


def get_input_validation_middleware() -> InputValidationMiddleware:
    """Get or create global input validation middleware instance."""
    global _validation_middleware
    if _validation_middleware is None:
        _validation_middleware = create_input_validation_middleware()
    return _validation_middleware