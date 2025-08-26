"""
API Response Security Middleware
Provides comprehensive response security including PII/PHI redaction, data classification,
compliance filtering, and security headers.
"""
import json
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, StreamingResponse
from starlette.types import ASGIApp

from core.logging import get_logger
from core.security.enterprise_dlp import EnterpriseDLPManager, DataClassification, SensitiveDataType
from core.security.compliance_framework import get_compliance_engine, ComplianceFramework
from core.security.enhanced_access_control import get_access_control_manager


logger = get_logger(__name__)


class DataClassificationLevel:
    """Data classification levels for response filtering"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class ComplianceFilter:
    """Filters data based on compliance requirements"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.compliance_engine = get_compliance_engine()
    
    def filter_for_gdpr(self, data: Any, user_consent: Dict[str, bool] = None) -> Tuple[Any, List[str]]:
        """Filter data for GDPR compliance"""
        
        user_consent = user_consent or {}
        filtered_data = data
        redactions = []
        
        if isinstance(data, dict):
            filtered_data = {}
            for key, value in data.items():
                # Check if field requires consent
                if key.lower() in ['email', 'phone', 'address', 'location']:
                    if not user_consent.get(f'process_{key}', False):
                        # Redact if no consent
                        filtered_data[key] = f"[REDACTED - No consent for {key}]"
                        redactions.append(f"GDPR consent required for {key}")
                        continue
                
                # Check for personal data patterns
                if isinstance(value, str):
                    # Email patterns
                    if re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', value):
                        if not user_consent.get('process_email', False):
                            filtered_data[key] = re.sub(
                                r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                                '[EMAIL_REDACTED]',
                                value
                            )
                            redactions.append(f"Email redacted in {key}")
                            continue
                    
                    # Phone patterns
                    if re.search(r'\b\+?[\d\s\-\(\)]{10,}\b', value):
                        if not user_consent.get('process_phone', False):
                            filtered_data[key] = re.sub(
                                r'\b\+?[\d\s\-\(\)]{10,}\b',
                                '[PHONE_REDACTED]',
                                value
                            )
                            redactions.append(f"Phone redacted in {key}")
                            continue
                
                filtered_data[key] = value
        
        elif isinstance(data, list):
            filtered_data = []
            for item in data:
                filtered_item, item_redactions = self.filter_for_gdpr(item, user_consent)
                filtered_data.append(filtered_item)
                redactions.extend(item_redactions)
        
        return filtered_data, redactions
    
    def filter_for_hipaa(self, data: Any) -> Tuple[Any, List[str]]:
        """Filter data for HIPAA compliance (PHI protection)"""
        
        filtered_data = data
        redactions = []
        
        # HIPAA PHI identifiers
        phi_patterns = {
            'mrn': r'\b(?:mrn|medical record|patient id)[\s:]*([A-Z0-9]{6,12})\b',
            'ssn': r'\b\d{3}-?\d{2}-?\d{4}\b',
            'dob': r'\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b',
            'diagnosis_code': r'\b[A-Z]\d{2}(\.\d{1,3})?\b',  # ICD codes
            'prescription': r'\b(prescription|rx|medication)[\s:]*([A-Za-z0-9\s]+)\b'
        }
        
        if isinstance(data, dict):
            filtered_data = {}
            for key, value in data.items():
                # Check for PHI fields
                if key.lower() in ['ssn', 'dob', 'diagnosis', 'medical_record', 'prescription']:
                    filtered_data[key] = "[PHI_REDACTED]"
                    redactions.append(f"HIPAA PHI field redacted: {key}")
                    continue
                
                if isinstance(value, str):
                    filtered_value = value
                    for phi_type, pattern in phi_patterns.items():
                        if re.search(pattern, value, re.IGNORECASE):
                            filtered_value = re.sub(pattern, f'[{phi_type.upper()}_REDACTED]', filtered_value, flags=re.IGNORECASE)
                            redactions.append(f"HIPAA {phi_type} pattern redacted in {key}")
                    
                    filtered_data[key] = filtered_value
                else:
                    filtered_data[key] = value
        
        elif isinstance(data, list):
            filtered_data = []
            for item in data:
                filtered_item, item_redactions = self.filter_for_hipaa(item)
                filtered_data.append(filtered_item)
                redactions.extend(item_redactions)
        
        return filtered_data, redactions
    
    def filter_for_pci_dss(self, data: Any) -> Tuple[Any, List[str]]:
        """Filter data for PCI-DSS compliance (cardholder data protection)"""
        
        filtered_data = data
        redactions = []
        
        # PCI-DSS patterns
        pci_patterns = {
            'credit_card': r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9]{2})[0-9]{12}|3[47][0-9]{13}|3[0-9]{13}|2[0-9]{15})\b',
            'cvv': r'\b\d{3,4}\b(?=.*(?:cvv|cvc|security code))',
            'track_data': r'[%\^][0-9A-Z\s]{1,79}[\?\*]'
        }
        
        if isinstance(data, dict):
            filtered_data = {}
            for key, value in data.items():
                # Check for cardholder data fields
                if key.lower() in ['card_number', 'credit_card', 'cvv', 'expiry', 'track_data']:
                    if key.lower() == 'card_number' and isinstance(value, str) and len(value) >= 13:
                        # Show only last 4 digits
                        filtered_data[key] = f"****-****-****-{value[-4:]}"
                    else:
                        filtered_data[key] = "[PCI_REDACTED]"
                    redactions.append(f"PCI-DSS cardholder data redacted: {key}")
                    continue
                
                if isinstance(value, str):
                    filtered_value = value
                    for pci_type, pattern in pci_patterns.items():
                        matches = re.finditer(pattern, value)
                        for match in matches:
                            if pci_type == 'credit_card':
                                # Show only last 4 digits of card
                                card_num = match.group(0)
                                masked = f"****-****-****-{card_num[-4:]}"
                                filtered_value = filtered_value.replace(match.group(0), masked)
                            else:
                                filtered_value = re.sub(pattern, f'[{pci_type.upper()}_REDACTED]', filtered_value)
                            
                            redactions.append(f"PCI-DSS {pci_type} pattern redacted in {key}")
                    
                    filtered_data[key] = filtered_value
                else:
                    filtered_data[key] = value
        
        elif isinstance(data, list):
            filtered_data = []
            for item in data:
                filtered_item, item_redactions = self.filter_for_pci_dss(item)
                filtered_data.append(filtered_item)
                redactions.extend(item_redactions)
        
        return filtered_data, redactions
    
    def apply_compliance_filtering(
        self,
        data: Any,
        frameworks: List[str],
        user_context: Dict[str, Any] = None
    ) -> Tuple[Any, List[str], Dict[str, Any]]:
        """Apply compliance filtering based on specified frameworks"""
        
        user_context = user_context or {}
        filtered_data = data
        all_redactions = []
        compliance_metadata = {
            'frameworks_applied': frameworks,
            'redaction_count': 0,
            'compliance_score': 1.0
        }
        
        try:
            for framework in frameworks:
                if framework.lower() == 'gdpr':
                    filtered_data, redactions = self.filter_for_gdpr(
                        filtered_data, 
                        user_context.get('gdpr_consent', {})
                    )
                    all_redactions.extend(redactions)
                    
                elif framework.lower() == 'hipaa':
                    filtered_data, redactions = self.filter_for_hipaa(filtered_data)
                    all_redactions.extend(redactions)
                    
                elif framework.lower() == 'pci_dss':
                    filtered_data, redactions = self.filter_for_pci_dss(filtered_data)
                    all_redactions.extend(redactions)
            
            compliance_metadata['redaction_count'] = len(all_redactions)
            
            # Calculate compliance score based on redactions
            if len(all_redactions) > 0:
                # Lower score if redactions were necessary (indicates sensitive data exposure)
                compliance_metadata['compliance_score'] = max(0.5, 1.0 - (len(all_redactions) * 0.1))
            
        except Exception as e:
            self.logger.error(f"Compliance filtering error: {e}")
            compliance_metadata['error'] = str(e)
        
        return filtered_data, all_redactions, compliance_metadata


class PIIRedactionEngine:
    """Advanced PII/PHI redaction engine"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.dlp_manager = EnterpriseDLPManager()
        
        # Configurable redaction strategies
        self.redaction_strategies = {
            SensitiveDataType.SSN: self._redact_ssn,
            SensitiveDataType.EMAIL: self._redact_email,
            SensitiveDataType.PHONE: self._redact_phone,
            SensitiveDataType.CREDIT_CARD: self._redact_credit_card,
            SensitiveDataType.MEDICAL_RECORD: self._redact_medical_record,
            SensitiveDataType.API_KEY: self._redact_api_key,
            SensitiveDataType.IP_ADDRESS: self._redact_ip_address
        }
    
    def redact_response_data(
        self,
        data: Any,
        redaction_level: str = "standard",
        preserve_format: bool = True,
        user_permissions: List[str] = None
    ) -> Tuple[Any, Dict[str, Any]]:
        """
        Redact sensitive data from response based on redaction level and user permissions
        
        Args:
            data: Response data to redact
            redaction_level: 'minimal', 'standard', 'strict', 'maximum'
            preserve_format: Whether to preserve original data format
            user_permissions: User permissions to determine redaction scope
        """
        
        user_permissions = user_permissions or []
        redaction_metadata = {
            'redaction_level': redaction_level,
            'redactions_applied': [],
            'sensitive_data_detected': [],
            'total_redactions': 0,
            'preservation_score': 1.0  # How much data was preserved
        }
        
        try:
            # Convert data to JSON string for scanning
            if isinstance(data, dict) or isinstance(data, list):
                data_text = json.dumps(data, ensure_ascii=False)
            else:
                data_text = str(data)
            
            # Scan for sensitive data using DLP
            scan_result = self.dlp_manager.scan_data(
                data_text,
                context={
                    'operation': 'response_redaction',
                    'redaction_level': redaction_level,
                    'user_permissions': user_permissions
                }
            )
            
            # Apply redactions based on detections
            redacted_data = data
            
            if scan_result.get('detections', 0) > 0:
                redacted_data = self._apply_intelligent_redaction(
                    data,
                    scan_result,
                    redaction_level,
                    user_permissions,
                    preserve_format
                )
                
                redaction_metadata.update({
                    'sensitive_data_detected': scan_result.get('sensitive_data_types', []),
                    'dlp_scan_id': scan_result.get('scan_id'),
                    'risk_score': scan_result.get('risk_score', 0.0)
                })
            
            return redacted_data, redaction_metadata
            
        except Exception as e:
            self.logger.error(f"PII redaction failed: {e}")
            redaction_metadata['error'] = str(e)
            return data, redaction_metadata
    
    def _apply_intelligent_redaction(
        self,
        data: Any,
        scan_result: Dict[str, Any],
        redaction_level: str,
        user_permissions: List[str],
        preserve_format: bool
    ) -> Any:
        """Apply intelligent redaction based on context and permissions"""
        
        # Permission-based redaction bypass
        bypass_permissions = {
            'admin': ['perm_admin_system', 'perm_view_sensitive_data'],
            'audit': ['perm_audit_logs', 'perm_compliance_report'],
            'security': ['perm_admin_system', 'perm_security_analysis']
        }
        
        # Check if user has bypass permissions
        can_bypass_redaction = any(
            perm in user_permissions
            for perm_list in bypass_permissions.values()
            for perm in perm_list
        )
        
        if can_bypass_redaction and redaction_level != 'maximum':
            # Allow privileged users to see more data
            return self._apply_minimal_redaction(data)
        
        # Apply redaction based on level
        if redaction_level == 'minimal':
            return self._apply_minimal_redaction(data)
        elif redaction_level == 'standard':
            return self._apply_standard_redaction(data)
        elif redaction_level == 'strict':
            return self._apply_strict_redaction(data)
        elif redaction_level == 'maximum':
            return self._apply_maximum_redaction(data)
        
        return self._apply_standard_redaction(data)
    
    def _apply_minimal_redaction(self, data: Any) -> Any:
        """Minimal redaction - only hide extremely sensitive data"""
        return self._recursive_redact(
            data,
            patterns={
                SensitiveDataType.CREDIT_CARD: r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14})\b',
                SensitiveDataType.SSN: r'\b\d{3}-?\d{2}-?\d{4}\b',
                SensitiveDataType.API_KEY: r'\b[A-Za-z0-9+/]{20,}={0,2}\b'
            },
            show_partial=True
        )
    
    def _apply_standard_redaction(self, data: Any) -> Any:
        """Standard redaction - balance between security and usability"""
        return self._recursive_redact(
            data,
            patterns={
                SensitiveDataType.CREDIT_CARD: r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9]{2})[0-9]{12})\b',
                SensitiveDataType.SSN: r'\b\d{3}-?\d{2}-?\d{4}\b',
                SensitiveDataType.EMAIL: r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                SensitiveDataType.PHONE: r'\b\+?[\d\s\-\(\)]{10,}\b',
                SensitiveDataType.API_KEY: r'\b[A-Za-z0-9+/]{20,}={0,2}\b',
                SensitiveDataType.MEDICAL_RECORD: r'\b(?:mrn|medical record)[\s:]*([A-Z0-9]{6,12})\b'
            },
            show_partial=True
        )
    
    def _apply_strict_redaction(self, data: Any) -> Any:
        """Strict redaction - high security, limited data exposure"""
        return self._recursive_redact(
            data,
            patterns={
                SensitiveDataType.CREDIT_CARD: r'\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b',
                SensitiveDataType.SSN: r'\b\d{3}-?\d{2}-?\d{4}\b',
                SensitiveDataType.EMAIL: r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                SensitiveDataType.PHONE: r'\b\+?[\d\s\-\(\)]{7,}\b',
                SensitiveDataType.API_KEY: r'\b[A-Za-z0-9+/]{15,}={0,2}\b',
                SensitiveDataType.IP_ADDRESS: r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
                'personal_name': r'\b[A-Z][a-z]+ [A-Z][a-z]+\b'
            },
            show_partial=False
        )
    
    def _apply_maximum_redaction(self, data: Any) -> Any:
        """Maximum redaction - minimal data exposure"""
        return self._recursive_redact(
            data,
            patterns={
                SensitiveDataType.CREDIT_CARD: r'\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b',
                SensitiveDataType.SSN: r'\b\d{3}-?\d{2}-?\d{4}\b',
                SensitiveDataType.EMAIL: r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                SensitiveDataType.PHONE: r'\b\+?[\d\s\-\(\)]{7,}\b',
                SensitiveDataType.API_KEY: r'\b[A-Za-z0-9+/]{10,}={0,2}\b',
                SensitiveDataType.IP_ADDRESS: r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
                'personal_name': r'\b[A-Z][a-z]+ [A-Z][a-z]+\b',
                'address': r'\b\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd)\b',
                'date': r'\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b'
            },
            show_partial=False
        )
    
    def _recursive_redact(
        self,
        data: Any,
        patterns: Dict[Union[SensitiveDataType, str], str],
        show_partial: bool = True
    ) -> Any:
        """Recursively redact data using patterns"""
        
        if isinstance(data, dict):
            redacted = {}
            for key, value in data.items():
                # Check if key itself contains sensitive information
                key_redacted = False
                for data_type, pattern in patterns.items():
                    if isinstance(key, str) and re.search(pattern, key, re.IGNORECASE):
                        redacted[f"[REDACTED_{data_type.value.upper() if hasattr(data_type, 'value') else str(data_type).upper()}]"] = "[REDACTED]"
                        key_redacted = True
                        break
                
                if not key_redacted:
                    redacted[key] = self._recursive_redact(value, patterns, show_partial)
            return redacted
            
        elif isinstance(data, list):
            return [self._recursive_redact(item, patterns, show_partial) for item in data]
            
        elif isinstance(data, str):
            redacted_str = data
            for data_type, pattern in patterns.items():
                matches = list(re.finditer(pattern, redacted_str, re.IGNORECASE))
                for match in reversed(matches):  # Reverse to maintain string positions
                    matched_text = match.group(0)
                    
                    if show_partial and isinstance(data_type, SensitiveDataType):
                        # Use specific redaction strategy
                        redactor = self.redaction_strategies.get(data_type)
                        if redactor:
                            replacement = redactor(matched_text)
                        else:
                            replacement = self._generic_partial_redact(matched_text)
                    else:
                        replacement = f"[REDACTED_{data_type.value.upper() if hasattr(data_type, 'value') else str(data_type).upper()}]"
                    
                    redacted_str = redacted_str[:match.start()] + replacement + redacted_str[match.end():]
            
            return redacted_str
        
        return data
    
    # Specific redaction strategies
    def _redact_ssn(self, ssn: str) -> str:
        """Redact SSN showing only last 4 digits"""
        clean_ssn = re.sub(r'\D', '', ssn)
        return f"***-**-{clean_ssn[-4:]}" if len(clean_ssn) >= 4 else "***-**-****"
    
    def _redact_email(self, email: str) -> str:
        """Redact email showing only domain"""
        parts = email.split('@')
        if len(parts) == 2:
            username = parts[0]
            domain = parts[1]
            redacted_username = username[:2] + '*' * max(1, len(username) - 2) if len(username) > 2 else '***'
            return f"{redacted_username}@{domain}"
        return "[EMAIL_REDACTED]"
    
    def _redact_phone(self, phone: str) -> str:
        """Redact phone showing only last 4 digits"""
        clean_phone = re.sub(r'\D', '', phone)
        return f"***-***-{clean_phone[-4:]}" if len(clean_phone) >= 4 else "***-***-****"
    
    def _redact_credit_card(self, card: str) -> str:
        """Redact credit card showing only last 4 digits"""
        clean_card = re.sub(r'\D', '', card)
        return f"****-****-****-{clean_card[-4:]}" if len(clean_card) >= 4 else "****-****-****-****"
    
    def _redact_medical_record(self, mrn: str) -> str:
        """Redact medical record number"""
        return "[MRN_REDACTED]"
    
    def _redact_api_key(self, key: str) -> str:
        """Redact API key showing only prefix"""
        return f"{key[:6]}..." if len(key) > 6 else "[API_KEY_REDACTED]"
    
    def _redact_ip_address(self, ip: str) -> str:
        """Redact IP address showing only first octet"""
        parts = ip.split('.')
        return f"{parts[0]}.***.***.***" if len(parts) >= 4 else "[IP_REDACTED]"
    
    def _generic_partial_redact(self, text: str) -> str:
        """Generic partial redaction for unknown patterns"""
        if len(text) <= 4:
            return '*' * len(text)
        else:
            return text[:2] + '*' * (len(text) - 4) + text[-2:]


class ResponseSecurityMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive response security middleware that applies:
    - PII/PHI redaction based on user permissions
    - Compliance-based data filtering
    - Data classification headers
    - Security headers and metadata
    """
    
    def __init__(
        self,
        app: ASGIApp,
        default_redaction_level: str = "standard",
        enable_compliance_filtering: bool = True,
        compliance_frameworks: List[str] = None
    ):
        super().__init__(app)
        self.logger = get_logger(__name__)
        self.default_redaction_level = default_redaction_level
        self.enable_compliance_filtering = enable_compliance_filtering
        self.compliance_frameworks = compliance_frameworks or ['gdpr', 'pci_dss', 'hipaa']
        
        # Initialize components
        self.pii_redactor = PIIRedactionEngine()
        self.compliance_filter = ComplianceFilter()
        self.access_manager = get_access_control_manager()
        
        # Paths to exclude from response security processing
        self.exclude_paths = [
            "/health",
            "/metrics",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/security/health"
        ]
    
    async def dispatch(self, request, call_next):
        """Process response through security pipeline"""
        
        # Skip processing for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)
        
        response = await call_next(request)
        
        # Only process JSON responses
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("application/json"):
            return response
        
        # Skip processing for error responses (4xx, 5xx)
        if response.status_code >= 400:
            return response
        
        try:
            # Extract response body
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            
            if not body:
                return response
            
            # Parse JSON
            try:
                response_data = json.loads(body.decode())
            except json.JSONDecodeError:
                return response
            
            # Get user context from request
            user_context = await self._extract_user_context(request)
            
            # Apply security processing
            secured_data, security_metadata = await self._apply_response_security(
                response_data,
                user_context,
                request
            )
            
            # Create new response
            secured_response = JSONResponse(
                content=secured_data,
                status_code=response.status_code,
                headers=dict(response.headers)
            )
            
            # Add security metadata headers
            self._add_security_headers(secured_response, security_metadata, user_context)
            
            return secured_response
            
        except Exception as e:
            self.logger.error(f"Response security processing failed: {e}")
            return response
    
    async def _extract_user_context(self, request) -> Dict[str, Any]:
        """Extract user context from request for security processing"""
        
        context = {
            'user_id': None,
            'permissions': [],
            'roles': [],
            'clearance_level': 0,
            'gdpr_consent': {},
            'ip_address': self._get_client_ip(request)
        }
        
        try:
            # Try to extract user info from token
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header[7:]
                
                # This would normally use the enhanced auth service
                # For demo, we'll extract basic info from JWT payload
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
                    
                    context.update({
                        'user_id': token_data.get('sub'),
                        'permissions': token_data.get('permissions', []),
                        'roles': token_data.get('roles', []),
                        'clearance_level': token_data.get('clearance_level', 0)
                    })
        
        except Exception as e:
            self.logger.warning(f"Failed to extract user context: {e}")
        
        return context
    
    async def _apply_response_security(
        self,
        response_data: Any,
        user_context: Dict[str, Any],
        request
    ) -> Tuple[Any, Dict[str, Any]]:
        """Apply comprehensive response security processing"""
        
        security_metadata = {
            'processing_timestamp': datetime.now().isoformat(),
            'security_applied': [],
            'user_context': {
                'user_id': user_context.get('user_id'),
                'clearance_level': user_context.get('clearance_level', 0),
                'permission_count': len(user_context.get('permissions', []))
            }
        }
        
        processed_data = response_data
        
        try:
            # 1. Determine redaction level based on user clearance and data classification
            redaction_level = await self._determine_redaction_level(
                processed_data,
                user_context,
                request
            )
            
            # 2. Apply PII/PHI redaction
            processed_data, redaction_metadata = self.pii_redactor.redact_response_data(
                processed_data,
                redaction_level=redaction_level,
                preserve_format=True,
                user_permissions=user_context.get('permissions', [])
            )
            
            security_metadata['pii_redaction'] = redaction_metadata
            security_metadata['security_applied'].append('pii_redaction')
            
            # 3. Apply compliance filtering if enabled
            if self.enable_compliance_filtering:
                processed_data, compliance_redactions, compliance_metadata = self.compliance_filter.apply_compliance_filtering(
                    processed_data,
                    self.compliance_frameworks,
                    user_context
                )
                
                security_metadata['compliance_filtering'] = compliance_metadata
                security_metadata['compliance_filtering']['redactions'] = compliance_redactions
                security_metadata['security_applied'].append('compliance_filtering')
            
            # 4. Data classification
            classification = self._classify_response_data(processed_data)
            security_metadata['data_classification'] = classification
            
            # 5. Calculate overall security score
            security_score = await self._calculate_security_score(
                redaction_metadata,
                security_metadata.get('compliance_filtering', {}),
                user_context
            )
            
            security_metadata['security_score'] = security_score
            
        except Exception as e:
            self.logger.error(f"Security processing error: {e}")
            security_metadata['error'] = str(e)
            security_metadata['security_applied'].append('error_handling')
        
        return processed_data, security_metadata
    
    async def _determine_redaction_level(
        self,
        data: Any,
        user_context: Dict[str, Any],
        request
    ) -> str:
        """Determine appropriate redaction level based on context"""
        
        clearance_level = user_context.get('clearance_level', 0)
        permissions = user_context.get('permissions', [])
        
        # High clearance users get minimal redaction
        if clearance_level >= 5 and 'perm_admin_system' in permissions:
            return 'minimal'
        
        # Medium clearance users get standard redaction
        elif clearance_level >= 3:
            return 'standard'
        
        # Check for specific permissions that allow reduced redaction
        elif any(perm in permissions for perm in ['perm_audit_logs', 'perm_compliance_report']):
            return 'standard'
        
        # External or low clearance users get strict redaction
        elif user_context.get('ip_address', '').startswith(('10.', '192.168.', '172.')):
            return 'standard'  # Internal network
        else:
            return 'strict'  # External access
    
    def _classify_response_data(self, data: Any) -> str:
        """Classify response data based on content"""
        
        if isinstance(data, dict):
            data_str = json.dumps(data).lower()
            
            # Check for highly sensitive data
            if any(term in data_str for term in ['ssn', 'credit_card', 'medical', 'prescription']):
                return DataClassificationLevel.RESTRICTED
            
            # Check for sensitive data
            elif any(term in data_str for term in ['email', 'phone', 'personal', 'address']):
                return DataClassificationLevel.CONFIDENTIAL
            
            # Check for internal data
            elif any(term in data_str for term in ['internal', 'employee', 'salary']):
                return DataClassificationLevel.INTERNAL
        
        return DataClassificationLevel.PUBLIC
    
    async def _calculate_security_score(
        self,
        redaction_metadata: Dict[str, Any],
        compliance_metadata: Dict[str, Any],
        user_context: Dict[str, Any]
    ) -> float:
        """Calculate overall security score for response"""
        
        base_score = 1.0
        
        # Factor in redaction effectiveness
        redaction_count = redaction_metadata.get('total_redactions', 0)
        if redaction_count > 0:
            base_score *= 0.9  # Slight penalty for needing redactions
        
        # Factor in compliance score
        compliance_score = compliance_metadata.get('compliance_score', 1.0)
        base_score *= compliance_score
        
        # Factor in user clearance
        clearance_level = user_context.get('clearance_level', 0)
        if clearance_level >= 4:
            base_score *= 1.1  # Bonus for high clearance users
        
        return min(1.0, base_score)
    
    def _add_security_headers(
        self,
        response: Response,
        security_metadata: Dict[str, Any],
        user_context: Dict[str, Any]
    ):
        """Add security-related headers to response"""
        
        # Data classification header
        classification = security_metadata.get('data_classification', 'public')
        response.headers['X-Data-Classification'] = classification
        
        # Security processing indicators
        response.headers['X-Security-Processed'] = 'true'
        
        if 'pii_redaction' in security_metadata:
            redaction_meta = security_metadata['pii_redaction']
            response.headers['X-PII-Redacted'] = str(redaction_meta.get('total_redactions', 0) > 0).lower()
            response.headers['X-Redaction-Level'] = redaction_meta.get('redaction_level', 'standard')
        
        if 'compliance_filtering' in security_metadata:
            compliance_meta = security_metadata['compliance_filtering']
            response.headers['X-Compliance-Applied'] = ','.join(compliance_meta.get('frameworks_applied', []))
            response.headers['X-Compliance-Score'] = str(compliance_meta.get('compliance_score', 1.0))
        
        # Security score
        security_score = security_metadata.get('security_score', 1.0)
        response.headers['X-Security-Score'] = str(security_score)
        
        # User context headers (non-sensitive info only)
        if user_context.get('clearance_level'):
            response.headers['X-User-Clearance'] = str(user_context['clearance_level'])
    
    def _get_client_ip(self, request) -> str:
        """Extract client IP from request"""
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip
        
        return getattr(request.client, "host", "unknown")


# Global middleware instance
_response_security_middleware: Optional[ResponseSecurityMiddleware] = None

def get_response_security_middleware(
    default_redaction_level: str = "standard",
    enable_compliance_filtering: bool = True,
    compliance_frameworks: List[str] = None
) -> ResponseSecurityMiddleware:
    """Get global response security middleware instance"""
    global _response_security_middleware
    if _response_security_middleware is None:
        _response_security_middleware = ResponseSecurityMiddleware(
            app=None,  # Will be set when added to app
            default_redaction_level=default_redaction_level,
            enable_compliance_filtering=enable_compliance_filtering,
            compliance_frameworks=compliance_frameworks
        )
    return _response_security_middleware