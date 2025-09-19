"""
Comprehensive Input Validation Middleware with Pydantic v2 Integration
Enterprise-grade input validation, sanitization, security scanning,
and advanced data quality checks with complete Pydantic v2 support.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Set, Type, Union

import bleach
from fastapi import HTTPException, Request, Response
from pydantic import BaseModel, Field, ValidationError, validator
from pydantic.v1 import BaseModel as BaseModelV1  # For backward compatibility
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger

logger = get_logger(__name__)


class ValidationSeverity(Enum):
    """Validation severity levels for different types of violations."""
    INFO = "info"           # Informational, log only
    WARNING = "warning"     # Warning, allow but log
    ERROR = "error"         # Error, reject request
    CRITICAL = "critical"   # Critical security issue, reject and alert


class InputType(Enum):
    """Types of input to validate."""
    JSON_BODY = "json_body"
    QUERY_PARAMS = "query_params"
    PATH_PARAMS = "path_params"
    HEADERS = "headers"
    FORM_DATA = "form_data"
    FILES = "files"


class SecurityThreat(Enum):
    """Security threats to detect and prevent."""
    SQL_INJECTION = "sql_injection"
    XSS_ATTACK = "xss_attack"
    COMMAND_INJECTION = "command_injection"
    PATH_TRAVERSAL = "path_traversal"
    LDAP_INJECTION = "ldap_injection"
    XML_INJECTION = "xml_injection"
    SCRIPT_INJECTION = "script_injection"
    HTML_INJECTION = "html_injection"
    NOSQL_INJECTION = "nosql_injection"


@dataclass
class ValidationRule:
    """Configuration for a specific validation rule."""
    name: str
    pattern: Optional[Pattern] = None
    threat_type: Optional[SecurityThreat] = None
    severity: ValidationSeverity = ValidationSeverity.ERROR
    custom_validator: Optional[Callable] = None
    sanitizer: Optional[Callable] = None
    error_message: str = "Validation failed"
    enabled: bool = True


@dataclass
class ValidationResult:
    """Result of validation process."""
    is_valid: bool
    violations: List[Dict[str, Any]] = field(default_factory=list)
    sanitized_data: Optional[Dict[str, Any]] = None
    security_threats: List[SecurityThreat] = field(default_factory=list)
    processing_time_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class SecurityScanner:
    """Advanced security scanner for detecting various attack patterns."""

    def __init__(self):
        """Initialize security scanner with threat patterns."""
        self.threat_patterns = {
            SecurityThreat.SQL_INJECTION: [
                re.compile(r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|UNION)\b)", re.IGNORECASE),
                re.compile(r"(';|--|\|\||&&|\bOR\b|\bAND\b).*(\b(SELECT|INSERT|UPDATE|DELETE)\b)", re.IGNORECASE),
                re.compile(r"(\bUNION\b.*\bSELECT\b)", re.IGNORECASE),
                re.compile(r"(\b(WAITFOR|DELAY)\b.*\d+)", re.IGNORECASE),
            ],
            SecurityThreat.XSS_ATTACK: [
                re.compile(r"<script[^>]*>.*?</script>", re.IGNORECASE | re.DOTALL),
                re.compile(r"javascript:", re.IGNORECASE),
                re.compile(r"on\w+\s*=", re.IGNORECASE),
                re.compile(r"<iframe[^>]*>.*?</iframe>", re.IGNORECASE | re.DOTALL),
                re.compile(r"<object[^>]*>.*?</object>", re.IGNORECASE | re.DOTALL),
                re.compile(r"<embed[^>]*>", re.IGNORECASE),
            ],
            SecurityThreat.COMMAND_INJECTION: [
                re.compile(r"(\||&|;|\$\(|\`|>|<)", re.IGNORECASE),
                re.compile(r"(\b(cat|ls|pwd|whoami|id|uname|ps|netstat|wget|curl)\b)", re.IGNORECASE),
                re.compile(r"(\.\./|\.\.\\\)", re.IGNORECASE),
            ],
            SecurityThreat.PATH_TRAVERSAL: [
                re.compile(r"(\.\./|\.\.\\\|%2e%2e%2f|%2e%2e%5c)", re.IGNORECASE),
                re.compile(r"(\\\.\\\.\\|/\.\./)", re.IGNORECASE),
                re.compile(r"(\.\.[/\\])", re.IGNORECASE),
            ],
            SecurityThreat.LDAP_INJECTION: [
                re.compile(r"(\*|\)|\(|\||&)", re.IGNORECASE),
                re.compile(r"(\bAND\b|\bOR\b|\bNOT\b)", re.IGNORECASE),
            ],
            SecurityThreat.XML_INJECTION: [
                re.compile(r"(<\?xml|<!DOCTYPE|<!ENTITY)", re.IGNORECASE),
                re.compile(r"(&\w+;|&#\d+;|&#x[0-9a-f]+;)", re.IGNORECASE),
            ],
            SecurityThreat.NOSQL_INJECTION: [
                re.compile(r"(\$where|\$ne|\$gt|\$lt|\$regex|\$exists)", re.IGNORECASE),
                re.compile(r"(this\.|eval\(|function\()", re.IGNORECASE),
            ]
        }

    def scan_for_threats(self, data: Union[str, Dict, List], input_type: InputType) -> List[SecurityThreat]:
        """Scan data for security threats."""
        threats = []

        if isinstance(data, str):
            threats.extend(self._scan_string(data))
        elif isinstance(data, dict):
            threats.extend(self._scan_dict(data))
        elif isinstance(data, list):
            threats.extend(self._scan_list(data))

        return list(set(threats))  # Remove duplicates

    def _scan_string(self, text: str) -> List[SecurityThreat]:
        """Scan string for security threats."""
        threats = []

        for threat_type, patterns in self.threat_patterns.items():
            for pattern in patterns:
                if pattern.search(text):
                    threats.append(threat_type)
                    break  # Found threat, no need to check other patterns for this type

        return threats

    def _scan_dict(self, data: Dict[str, Any]) -> List[SecurityThreat]:
        """Recursively scan dictionary for threats."""
        threats = []

        for key, value in data.items():
            # Scan key
            if isinstance(key, str):
                threats.extend(self._scan_string(key))

            # Scan value
            if isinstance(value, str):
                threats.extend(self._scan_string(value))
            elif isinstance(value, dict):
                threats.extend(self._scan_dict(value))
            elif isinstance(value, list):
                threats.extend(self._scan_list(value))

        return threats

    def _scan_list(self, data: List[Any]) -> List[SecurityThreat]:
        """Recursively scan list for threats."""
        threats = []

        for item in data:
            if isinstance(item, str):
                threats.extend(self._scan_string(item))
            elif isinstance(item, dict):
                threats.extend(self._scan_dict(item))
            elif isinstance(item, list):
                threats.extend(self._scan_list(item))

        return threats


class DataSanitizer:
    """Advanced data sanitizer for cleaning potentially malicious input."""

    def __init__(self):
        """Initialize data sanitizer."""
        self.html_tags_allowed = [
            'p', 'br', 'strong', 'em', 'u', 'ol', 'ul', 'li',
            'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'blockquote'
        ]
        self.html_attributes_allowed = {
            '*': ['class', 'id'],
            'a': ['href', 'title'],
            'img': ['src', 'alt', 'width', 'height']
        }

    def sanitize_html(self, text: str) -> str:
        """Sanitize HTML content to remove malicious elements."""
        return bleach.clean(
            text,
            tags=self.html_tags_allowed,
            attributes=self.html_attributes_allowed,
            strip=True
        )

    def sanitize_sql(self, text: str) -> str:
        """Sanitize text to prevent SQL injection."""
        # Escape single quotes
        text = text.replace("'", "''")

        # Remove or escape dangerous SQL keywords
        dangerous_patterns = [
            (r"\bDROP\b", ""),
            (r"\bDELETE\b", ""),
            (r"\bEXEC\b", ""),
            (r"\bEXECUTE\b", ""),
            (r"--", ""),
            (r"/\*.*?\*/", ""),
        ]

        for pattern, replacement in dangerous_patterns:
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

        return text

    def sanitize_path(self, path: str) -> str:
        """Sanitize file paths to prevent traversal attacks."""
        # Remove path traversal sequences
        path = re.sub(r'\.\.[\\/]', '', path)
        path = re.sub(r'[\\/]\.\.', '', path)

        # Remove null bytes
        path = path.replace('\x00', '')

        # Normalize path separators
        path = path.replace('\\', '/')

        return path

    def sanitize_command(self, text: str) -> str:
        """Sanitize text to prevent command injection."""
        # Remove dangerous command characters
        dangerous_chars = ['|', '&', ';', '$', '`', '>', '<', '\n', '\r']
        for char in dangerous_chars:
            text = text.replace(char, '')

        return text

    def sanitize_data(self, data: Any, threat_types: List[SecurityThreat]) -> Any:
        """Sanitize data based on detected threat types."""
        if not isinstance(data, (str, dict, list)):
            return data

        if isinstance(data, str):
            sanitized = data

            if SecurityThreat.XSS_ATTACK in threat_types:
                sanitized = self.sanitize_html(sanitized)

            if SecurityThreat.SQL_INJECTION in threat_types:
                sanitized = self.sanitize_sql(sanitized)

            if SecurityThreat.PATH_TRAVERSAL in threat_types:
                sanitized = self.sanitize_path(sanitized)

            if SecurityThreat.COMMAND_INJECTION in threat_types:
                sanitized = self.sanitize_command(sanitized)

            return sanitized

        elif isinstance(data, dict):
            return {
                key: self.sanitize_data(value, threat_types)
                for key, value in data.items()
            }

        elif isinstance(data, list):
            return [
                self.sanitize_data(item, threat_types)
                for item in data
            ]

        return data


class PydanticValidator:
    """Enhanced Pydantic validator with v2 support and advanced features."""

    def __init__(self):
        """Initialize Pydantic validator."""
        self.model_cache: Dict[str, Type[BaseModel]] = {}
        self.validation_stats = defaultdict(int)

    def validate_with_model(
        self,
        data: Dict[str, Any],
        model_class: Type[BaseModel],
        strict: bool = False
    ) -> ValidationResult:
        """Validate data using Pydantic model."""
        start_time = time.time()

        try:
            # Pydantic v2 validation
            if hasattr(model_class, 'model_validate'):
                # Pydantic v2
                validated_data = model_class.model_validate(data, strict=strict)
                sanitized = validated_data.model_dump()
            else:
                # Pydantic v1 fallback
                validated_data = model_class.parse_obj(data)
                sanitized = validated_data.dict()

            processing_time = (time.time() - start_time) * 1000
            self.validation_stats['successful_validations'] += 1

            return ValidationResult(
                is_valid=True,
                sanitized_data=sanitized,
                processing_time_ms=processing_time,
                metadata={'model': model_class.__name__}
            )

        except ValidationError as e:
            processing_time = (time.time() - start_time) * 1000
            self.validation_stats['failed_validations'] += 1

            violations = []
            for error in e.errors():
                violations.append({
                    'field': '.'.join(str(loc) for loc in error['loc']),
                    'message': error['msg'],
                    'type': error['type'],
                    'input': error.get('input'),
                    'severity': ValidationSeverity.ERROR.value
                })

            return ValidationResult(
                is_valid=False,
                violations=violations,
                processing_time_ms=processing_time,
                metadata={'model': model_class.__name__, 'error_count': len(violations)}
            )

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            self.validation_stats['validation_errors'] += 1

            return ValidationResult(
                is_valid=False,
                violations=[{
                    'field': 'root',
                    'message': f"Validation error: {str(e)}",
                    'type': 'validation_error',
                    'severity': ValidationSeverity.CRITICAL.value
                }],
                processing_time_ms=processing_time,
                metadata={'error': str(e)}
            )

    def create_dynamic_model(
        self,
        model_name: str,
        field_definitions: Dict[str, Tuple[Type, Any]]
    ) -> Type[BaseModel]:
        """Create dynamic Pydantic model for validation."""
        if model_name in self.model_cache:
            return self.model_cache[model_name]

        # Create model fields
        annotations = {}
        defaults = {}

        for field_name, (field_type, field_default) in field_definitions.items():
            annotations[field_name] = field_type
            if field_default is not ...:  # Ellipsis indicates required field
                defaults[field_name] = field_default

        # Create dynamic model class
        model_class = type(
            model_name,
            (BaseModel,),
            {
                '__annotations__': annotations,
                **defaults
            }
        )

        self.model_cache[model_name] = model_class
        return model_class


class ComprehensiveInputValidator:
    """
    Comprehensive input validator with enterprise-grade features:
    - Pydantic v2 integration
    - Security threat detection
    - Data sanitization
    - Performance monitoring
    - Adaptive validation rules
    """

    def __init__(
        self,
        enable_security_scanning: bool = True,
        enable_data_sanitization: bool = True,
        strict_validation: bool = False,
        max_payload_size: int = 10 * 1024 * 1024,  # 10MB
        enable_rate_limiting: bool = True
    ):
        """Initialize comprehensive input validator."""
        self.enable_security_scanning = enable_security_scanning
        self.enable_data_sanitization = enable_data_sanitization
        self.strict_validation = strict_validation
        self.max_payload_size = max_payload_size
        self.enable_rate_limiting = enable_rate_limiting

        # Initialize components
        self.security_scanner = SecurityScanner()
        self.data_sanitizer = DataSanitizer()
        self.pydantic_validator = PydanticValidator()

        # Validation rules registry
        self.validation_rules: Dict[str, List[ValidationRule]] = defaultdict(list)

        # Performance and monitoring
        self.validation_metrics = {
            'total_validations': 0,
            'security_threats_detected': 0,
            'sanitizations_performed': 0,
            'validation_failures': 0,
            'avg_processing_time_ms': 0.0,
            'threat_breakdown': defaultdict(int)
        }

        # Rate limiting for validation requests
        self.validation_requests: Dict[str, List[float]] = defaultdict(list)
        self.rate_limit_window = 60  # seconds
        self.rate_limit_max_requests = 1000

        # Setup default validation rules
        self._setup_default_rules()

    def _setup_default_rules(self):
        """Setup default validation rules for common scenarios."""
        # Email validation
        email_rule = ValidationRule(
            name="email_format",
            pattern=re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            severity=ValidationSeverity.ERROR,
            error_message="Invalid email format"
        )
        self.add_validation_rule(InputType.JSON_BODY, email_rule)

        # Phone number validation
        phone_rule = ValidationRule(
            name="phone_format",
            pattern=re.compile(r'^\+?[\d\s\-\(\)]{10,}$'),
            severity=ValidationSeverity.WARNING,
            error_message="Invalid phone number format"
        )
        self.add_validation_rule(InputType.JSON_BODY, phone_rule)

        # URL validation
        url_rule = ValidationRule(
            name="url_format",
            pattern=re.compile(r'^https?://[^\s/$.?#].[^\s]*$', re.IGNORECASE),
            severity=ValidationSeverity.ERROR,
            error_message="Invalid URL format"
        )
        self.add_validation_rule(InputType.JSON_BODY, url_rule)

    def add_validation_rule(self, input_type: InputType, rule: ValidationRule):
        """Add a validation rule for specific input type."""
        self.validation_rules[input_type.value].append(rule)
        logger.info(f"Added validation rule '{rule.name}' for {input_type.value}")

    def _check_rate_limit(self, client_id: str) -> bool:
        """Check if client has exceeded validation rate limit."""
        if not self.enable_rate_limiting:
            return True

        current_time = time.time()
        client_requests = self.validation_requests[client_id]

        # Remove requests outside the window
        client_requests[:] = [
            req_time for req_time in client_requests
            if current_time - req_time < self.rate_limit_window
        ]

        # Check if under limit
        if len(client_requests) >= self.rate_limit_max_requests:
            return False

        # Add current request
        client_requests.append(current_time)
        return True

    async def validate_request(
        self,
        request: Request,
        model_class: Optional[Type[BaseModel]] = None,
        custom_rules: Optional[List[ValidationRule]] = None
    ) -> ValidationResult:
        """
        Comprehensive request validation with security scanning and sanitization.

        Args:
            request: FastAPI request object
            model_class: Optional Pydantic model for validation
            custom_rules: Optional custom validation rules

        Returns:
            ValidationResult with validation outcome and sanitized data
        """
        start_time = time.time()
        client_id = request.client.host

        # Check rate limiting
        if not self._check_rate_limit(client_id):
            return ValidationResult(
                is_valid=False,
                violations=[{
                    'field': 'rate_limit',
                    'message': 'Validation rate limit exceeded',
                    'type': 'rate_limit_exceeded',
                    'severity': ValidationSeverity.ERROR.value
                }],
                processing_time_ms=(time.time() - start_time) * 1000
            )

        # Update metrics
        self.validation_metrics['total_validations'] += 1

        # Extract data from request
        validation_data = await self._extract_request_data(request)

        # Validate payload size
        payload_size = len(json.dumps(validation_data).encode('utf-8'))
        if payload_size > self.max_payload_size:
            return ValidationResult(
                is_valid=False,
                violations=[{
                    'field': 'payload_size',
                    'message': f'Payload size {payload_size} exceeds maximum {self.max_payload_size}',
                    'type': 'payload_too_large',
                    'severity': ValidationSeverity.ERROR.value
                }],
                processing_time_ms=(time.time() - start_time) * 1000
            )

        # Security scanning
        security_threats = []
        if self.enable_security_scanning:
            security_threats = self._perform_security_scan(validation_data)

        # Data sanitization
        sanitized_data = validation_data
        if security_threats and self.enable_data_sanitization:
            sanitized_data = self.data_sanitizer.sanitize_data(validation_data, security_threats)
            self.validation_metrics['sanitizations_performed'] += 1

        # Pydantic validation
        validation_result = None
        if model_class:
            validation_result = self.pydantic_validator.validate_with_model(
                sanitized_data.get('json_body', {}),
                model_class,
                self.strict_validation
            )

        # Custom rule validation
        custom_violations = []
        if custom_rules:
            custom_violations = self._apply_custom_rules(sanitized_data, custom_rules)

        # Aggregate results
        all_violations = []
        is_valid = True

        if validation_result and not validation_result.is_valid:
            all_violations.extend(validation_result.violations)
            is_valid = False

        if custom_violations:
            all_violations.extend(custom_violations)
            # Check if any custom violations are errors or critical
            if any(v.get('severity') in [ValidationSeverity.ERROR.value, ValidationSeverity.CRITICAL.value]
                   for v in custom_violations):
                is_valid = False

        if security_threats:
            self.validation_metrics['security_threats_detected'] += 1
            for threat in security_threats:
                self.validation_metrics['threat_breakdown'][threat.value] += 1

            # Add security threat violations
            threat_violations = [{
                'field': 'security',
                'message': f'Security threat detected: {threat.value}',
                'type': 'security_threat',
                'threat_type': threat.value,
                'severity': ValidationSeverity.CRITICAL.value
            } for threat in security_threats]

            all_violations.extend(threat_violations)
            is_valid = False

        if not is_valid:
            self.validation_metrics['validation_failures'] += 1

        # Calculate processing time
        processing_time = (time.time() - start_time) * 1000

        # Update average processing time
        total_validations = self.validation_metrics['total_validations']
        current_avg = self.validation_metrics['avg_processing_time_ms']
        self.validation_metrics['avg_processing_time_ms'] = (
            (current_avg * (total_validations - 1) + processing_time) / total_validations
        )

        return ValidationResult(
            is_valid=is_valid,
            violations=all_violations,
            sanitized_data=sanitized_data,
            security_threats=security_threats,
            processing_time_ms=processing_time,
            metadata={
                'client_id': client_id,
                'payload_size': payload_size,
                'pydantic_validation': validation_result is not None,
                'custom_rules_applied': len(custom_rules) if custom_rules else 0
            }
        )

    async def _extract_request_data(self, request: Request) -> Dict[str, Any]:
        """Extract all data from request for validation."""
        data = {
            'method': request.method,
            'path': request.url.path,
            'query_params': dict(request.query_params),
            'headers': dict(request.headers),
            'path_params': getattr(request, 'path_params', {}),
        }

        # Extract body data
        try:
            if request.headers.get('content-type', '').startswith('application/json'):
                body = await request.body()
                if body:
                    data['json_body'] = json.loads(body.decode('utf-8'))
        except Exception as e:
            logger.warning(f"Failed to parse request body: {e}")
            data['json_body'] = {}

        return data

    def _perform_security_scan(self, data: Dict[str, Any]) -> List[SecurityThreat]:
        """Perform comprehensive security scan on request data."""
        all_threats = []

        for input_type, input_data in data.items():
            if input_data:
                threats = self.security_scanner.scan_for_threats(
                    input_data,
                    InputType(input_type) if input_type in [e.value for e in InputType] else InputType.JSON_BODY
                )
                all_threats.extend(threats)

        return list(set(all_threats))  # Remove duplicates

    def _apply_custom_rules(
        self,
        data: Dict[str, Any],
        rules: List[ValidationRule]
    ) -> List[Dict[str, Any]]:
        """Apply custom validation rules to data."""
        violations = []

        for rule in rules:
            if not rule.enabled:
                continue

            try:
                if rule.custom_validator:
                    # Use custom validator function
                    result = rule.custom_validator(data)
                    if not result:
                        violations.append({
                            'field': rule.name,
                            'message': rule.error_message,
                            'type': 'custom_validation',
                            'severity': rule.severity.value
                        })

                elif rule.pattern:
                    # Use regex pattern validation
                    violations.extend(self._validate_with_pattern(data, rule))

            except Exception as e:
                logger.error(f"Error applying validation rule '{rule.name}': {e}")
                violations.append({
                    'field': rule.name,
                    'message': f"Validation rule error: {str(e)}",
                    'type': 'rule_execution_error',
                    'severity': ValidationSeverity.ERROR.value
                })

        return violations

    def _validate_with_pattern(self, data: Dict[str, Any], rule: ValidationRule) -> List[Dict[str, Any]]:
        """Validate data using regex pattern."""
        violations = []

        def check_value(value, field_path=""):
            if isinstance(value, str):
                if not rule.pattern.match(value):
                    violations.append({
                        'field': field_path or rule.name,
                        'message': rule.error_message,
                        'type': 'pattern_validation',
                        'pattern': rule.pattern.pattern,
                        'value': value[:100],  # Truncate long values
                        'severity': rule.severity.value
                    })
            elif isinstance(value, dict):
                for k, v in value.items():
                    check_value(v, f"{field_path}.{k}" if field_path else k)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    check_value(item, f"{field_path}[{i}]" if field_path else f"[{i}]")

        # Check all data
        for input_type, input_data in data.items():
            if input_data:
                check_value(input_data, input_type)

        return violations

    def get_validation_metrics(self) -> Dict[str, Any]:
        """Get comprehensive validation metrics."""
        return {
            **self.validation_metrics,
            'rules_registered': sum(len(rules) for rules in self.validation_rules.values()),
            'cached_models': len(self.pydantic_validator.model_cache),
            'rate_limit_config': {
                'enabled': self.enable_rate_limiting,
                'window_seconds': self.rate_limit_window,
                'max_requests': self.rate_limit_max_requests
            },
            'configuration': {
                'security_scanning': self.enable_security_scanning,
                'data_sanitization': self.enable_data_sanitization,
                'strict_validation': self.strict_validation,
                'max_payload_size': self.max_payload_size
            }
        }


class ComprehensiveValidationMiddleware(BaseHTTPMiddleware):
    """
    Middleware for comprehensive input validation with Pydantic v2 integration.
    Provides enterprise-grade validation, security scanning, and data sanitization.
    """

    def __init__(
        self,
        app,
        validator: Optional[ComprehensiveInputValidator] = None,
        endpoint_models: Optional[Dict[str, Type[BaseModel]]] = None,
        exclude_paths: Optional[Set[str]] = None
    ):
        """Initialize comprehensive validation middleware."""
        super().__init__(app)
        self.validator = validator or ComprehensiveInputValidator()
        self.endpoint_models = endpoint_models or {}
        self.exclude_paths = exclude_paths or {'/health', '/metrics', '/docs', '/redoc', '/openapi.json'}

    async def dispatch(self, request: Request, call_next):
        """Process request with comprehensive validation."""
        # Skip validation for excluded paths
        if request.url.path in self.exclude_paths:
            return await call_next(request)

        # Skip validation for GET requests without query parameters
        if request.method == "GET" and not request.query_params:
            return await call_next(request)

        try:
            # Get model for endpoint if available
            endpoint_key = f"{request.method}:{request.url.path}"
            model_class = self.endpoint_models.get(endpoint_key)

            # Perform validation
            validation_result = await self.validator.validate_request(
                request, model_class
            )

            # If validation failed with critical violations, reject request
            critical_violations = [
                v for v in validation_result.violations
                if v.get('severity') in [ValidationSeverity.ERROR.value, ValidationSeverity.CRITICAL.value]
            ]

            if critical_violations:
                logger.warning(
                    f"Request validation failed for {request.method} {request.url.path}",
                    extra={
                        'client_id': request.client.host,
                        'violations': critical_violations,
                        'security_threats': [t.value for t in validation_result.security_threats]
                    }
                )

                raise HTTPException(
                    status_code=400,
                    detail={
                        'message': 'Request validation failed',
                        'violations': critical_violations,
                        'validation_id': hashlib.md5(
                            f"{request.client.host}:{time.time()}".encode()
                        ).hexdigest()[:12]
                    }
                )

            # Add validation context to request
            request.state.validation_result = validation_result

            # Process request
            response = await call_next(request)

            # Add validation headers
            response.headers["X-Validation-Processed"] = "true"
            response.headers["X-Validation-Time"] = f"{validation_result.processing_time_ms:.2f}ms"

            if validation_result.security_threats:
                response.headers["X-Security-Threats-Detected"] = str(len(validation_result.security_threats))

            return response

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Validation middleware error: {e}")
            # If validation fails due to system error, allow request through
            return await call_next(request)


# Factory function for easy setup
def create_comprehensive_validation_middleware(
    app,
    enable_security_scanning: bool = True,
    enable_data_sanitization: bool = True,
    strict_validation: bool = False,
    endpoint_models: Optional[Dict[str, Type[BaseModel]]] = None
) -> ComprehensiveValidationMiddleware:
    """
    Create comprehensive validation middleware with sensible defaults.

    Args:
        app: FastAPI application
        enable_security_scanning: Enable security threat detection
        enable_data_sanitization: Enable automatic data sanitization
        strict_validation: Use strict Pydantic validation
        endpoint_models: Mapping of endpoints to Pydantic models

    Returns:
        Configured validation middleware
    """
    validator = ComprehensiveInputValidator(
        enable_security_scanning=enable_security_scanning,
        enable_data_sanitization=enable_data_sanitization,
        strict_validation=strict_validation
    )

    return ComprehensiveValidationMiddleware(
        app=app,
        validator=validator,
        endpoint_models=endpoint_models
    )


# Export key classes and functions
__all__ = [
    'ComprehensiveInputValidator',
    'ComprehensiveValidationMiddleware',
    'SecurityScanner',
    'DataSanitizer',
    'PydanticValidator',
    'ValidationRule',
    'ValidationResult',
    'ValidationSeverity',
    'InputType',
    'SecurityThreat',
    'create_comprehensive_validation_middleware'
]