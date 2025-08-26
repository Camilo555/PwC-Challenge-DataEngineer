"""
Enterprise Data Loss Prevention (DLP) Framework
Provides comprehensive data classification, protection, and monitoring capabilities.
"""
import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import phonenumbers
from cryptography.fernet import Fernet

from core.logging import get_logger
from core.security.advanced_security import AuditLogger, ActionType, SecurityEventType, ThreatLevel


logger = get_logger(__name__)


class DataClassification(Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class SensitiveDataType(Enum):
    """Types of sensitive data"""
    # Personal Information
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    EMAIL = "email"
    PHONE = "phone"
    PASSPORT = "passport"
    DRIVERS_LICENSE = "drivers_license"
    
    # Health Information (HIPAA)
    MEDICAL_RECORD = "medical_record"
    HEALTH_ID = "health_id"
    PRESCRIPTION = "prescription"
    
    # Financial Information (PCI-DSS)
    BANK_ACCOUNT = "bank_account"
    ROUTING_NUMBER = "routing_number"
    IBAN = "iban"
    SWIFT_CODE = "swift_code"
    
    # Business Information
    API_KEY = "api_key"
    PASSWORD = "password"
    ENCRYPTION_KEY = "encryption_key"
    DATABASE_CONNECTION = "database_connection"
    
    # Geographic Information
    IP_ADDRESS = "ip_address"
    GPS_COORDINATES = "gps_coordinates"
    
    # Custom Patterns
    CUSTOM_PATTERN = "custom_pattern"


class DLPAction(Enum):
    """Actions that can be taken by DLP"""
    ALLOW = "allow"
    BLOCK = "block"
    REDACT = "redact"
    ENCRYPT = "encrypt"
    QUARANTINE = "quarantine"
    ALERT = "alert"
    LOG = "log"


class ComplianceFramework(Enum):
    """Compliance frameworks"""
    GDPR = "gdpr"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    SOX = "sox"
    CCPA = "ccpa"
    ISO_27001 = "iso_27001"
    NIST = "nist"


@dataclass
class SensitiveDataPattern:
    """Pattern definition for sensitive data detection"""
    data_type: SensitiveDataType
    pattern: str
    regex_flags: int = re.IGNORECASE
    confidence_threshold: float = 0.8
    context_keywords: List[str] = field(default_factory=list)
    exclusion_patterns: List[str] = field(default_factory=list)
    compliance_frameworks: List[ComplianceFramework] = field(default_factory=list)
    description: str = ""


@dataclass
class DataDetectionResult:
    """Result of data detection scan"""
    data_type: SensitiveDataType
    classification: DataClassification
    confidence: float
    matched_text: str
    redacted_text: str
    start_position: int
    end_position: int
    context: str
    compliance_frameworks: List[ComplianceFramework]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DLPPolicy:
    """DLP policy configuration"""
    policy_id: str
    name: str
    description: str
    enabled: bool
    data_types: List[SensitiveDataType]
    classification_levels: List[DataClassification]
    actions: List[DLPAction]
    compliance_frameworks: List[ComplianceFramework]
    applies_to: Dict[str, Any]  # locations, users, systems
    exceptions: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class DLPIncident:
    """DLP incident record"""
    incident_id: str
    timestamp: datetime
    policy_id: str
    data_type: SensitiveDataType
    classification: DataClassification
    action_taken: DLPAction
    source_location: str
    destination_location: Optional[str]
    user_id: Optional[str]
    session_id: Optional[str]
    confidence: float
    risk_score: float
    details: Dict[str, Any]
    resolved: bool = False
    resolution_notes: Optional[str] = None


class SensitiveDataDetector:
    """Advanced sensitive data detection engine"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.patterns: Dict[SensitiveDataType, List[SensitiveDataPattern]] = {}
        self.custom_patterns: Dict[str, SensitiveDataPattern] = {}
        self._initialize_builtin_patterns()
    
    def _initialize_builtin_patterns(self):
        """Initialize built-in sensitive data patterns"""
        
        # SSN patterns
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.SSN,
            pattern=r'\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b',
            confidence_threshold=0.9,
            context_keywords=['ssn', 'social security', 'social security number'],
            compliance_frameworks=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
            description="US Social Security Number"
        ))
        
        # Credit card patterns
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.CREDIT_CARD,
            pattern=r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9]{2})[0-9]{12}|3[47][0-9]{13}|3[0-9]{13}|2[0-9]{15})\b',
            confidence_threshold=0.95,
            context_keywords=['credit card', 'card number', 'cc', 'visa', 'mastercard', 'amex'],
            compliance_frameworks=[ComplianceFramework.PCI_DSS],
            description="Credit card numbers"
        ))
        
        # Email patterns
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.EMAIL,
            pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            confidence_threshold=0.85,
            context_keywords=['email', 'e-mail', 'contact'],
            compliance_frameworks=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
            description="Email addresses"
        ))
        
        # Phone number patterns
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.PHONE,
            pattern=r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b',
            confidence_threshold=0.8,
            context_keywords=['phone', 'telephone', 'mobile', 'cell'],
            compliance_frameworks=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
            description="Phone numbers"
        ))
        
        # API keys and tokens
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.API_KEY,
            pattern=r'\b(?:api[_-]?key|access[_-]?token|secret[_-]?key)["\'\s:=]*([A-Za-z0-9+/]{20,}={0,2})\b',
            confidence_threshold=0.9,
            context_keywords=['api', 'key', 'token', 'secret', 'access'],
            compliance_frameworks=[ComplianceFramework.ISO_27001, ComplianceFramework.NIST],
            description="API keys and access tokens"
        ))
        
        # Bank account numbers
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.BANK_ACCOUNT,
            pattern=r'\b\d{8,17}\b',
            confidence_threshold=0.7,
            context_keywords=['account', 'bank', 'routing', 'aba'],
            compliance_frameworks=[ComplianceFramework.PCI_DSS, ComplianceFramework.SOX],
            description="Bank account numbers"
        ))
        
        # IP addresses
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.IP_ADDRESS,
            pattern=r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b',
            confidence_threshold=0.9,
            context_keywords=['ip', 'address', 'server', 'host'],
            compliance_frameworks=[ComplianceFramework.GDPR],
            description="IPv4 addresses"
        ))
        
        # Medical record numbers
        self.add_pattern(SensitiveDataPattern(
            data_type=SensitiveDataType.MEDICAL_RECORD,
            pattern=r'\b(?:mrn|medical record|patient id)[\s:=]*([A-Z0-9]{6,12})\b',
            confidence_threshold=0.85,
            context_keywords=['medical', 'patient', 'mrn', 'health', 'hospital'],
            compliance_frameworks=[ComplianceFramework.HIPAA],
            description="Medical record numbers"
        ))
        
    def add_pattern(self, pattern: SensitiveDataPattern):
        """Add a sensitive data pattern"""
        if pattern.data_type not in self.patterns:
            self.patterns[pattern.data_type] = []
        self.patterns[pattern.data_type].append(pattern)
        
    def add_custom_pattern(self, name: str, pattern: SensitiveDataPattern):
        """Add a custom pattern"""
        self.custom_patterns[name] = pattern
        
    def detect_sensitive_data(self, text: str, context: str = "") -> List[DataDetectionResult]:
        """Detect sensitive data in text"""
        results = []
        
        # Check built-in patterns
        for data_type, patterns in self.patterns.items():
            for pattern in patterns:
                matches = self._find_pattern_matches(text, pattern, context)
                results.extend(matches)
        
        # Check custom patterns
        for name, pattern in self.custom_patterns.items():
            matches = self._find_pattern_matches(text, pattern, context)
            results.extend(matches)
            
        # Sort by confidence and position
        results.sort(key=lambda x: (-x.confidence, x.start_position))
        
        return results
    
    def _find_pattern_matches(
        self, 
        text: str, 
        pattern: SensitiveDataPattern, 
        context: str
    ) -> List[DataDetectionResult]:
        """Find matches for a specific pattern"""
        results = []
        
        try:
            matches = re.finditer(pattern.pattern, text, pattern.regex_flags)
            
            for match in matches:
                matched_text = match.group(0)
                start_pos = match.start()
                end_pos = match.end()
                
                # Skip if matches exclusion patterns
                if self._matches_exclusion(matched_text, pattern.exclusion_patterns):
                    continue
                
                # Calculate confidence based on context
                confidence = self._calculate_confidence(
                    matched_text, context, pattern.context_keywords, pattern.confidence_threshold
                )
                
                if confidence >= pattern.confidence_threshold:
                    # Determine classification level
                    classification = self._determine_classification(pattern.data_type)
                    
                    # Create redacted version
                    redacted_text = self._redact_text(matched_text, pattern.data_type)
                    
                    result = DataDetectionResult(
                        data_type=pattern.data_type,
                        classification=classification,
                        confidence=confidence,
                        matched_text=matched_text,
                        redacted_text=redacted_text,
                        start_position=start_pos,
                        end_position=end_pos,
                        context=context,
                        compliance_frameworks=pattern.compliance_frameworks,
                        metadata={
                            'pattern_description': pattern.description,
                            'match_length': len(matched_text)
                        }
                    )
                    
                    results.append(result)
                    
        except re.error as e:
            self.logger.error(f"Regex error in pattern {pattern.data_type}: {e}")
            
        return results
    
    def _matches_exclusion(self, text: str, exclusion_patterns: List[str]) -> bool:
        """Check if text matches any exclusion patterns"""
        for exclusion in exclusion_patterns:
            if re.search(exclusion, text, re.IGNORECASE):
                return True
        return False
    
    def _calculate_confidence(
        self, 
        matched_text: str, 
        context: str, 
        keywords: List[str], 
        base_confidence: float
    ) -> float:
        """Calculate confidence score based on context"""
        confidence = base_confidence
        
        # Boost confidence if context keywords are found
        context_lower = context.lower()
        matched_keywords = sum(1 for keyword in keywords if keyword.lower() in context_lower)
        
        if matched_keywords > 0:
            confidence = min(1.0, confidence + (matched_keywords * 0.1))
        
        # Additional validation for specific data types
        confidence = self._validate_data_format(matched_text, confidence)
        
        return confidence
    
    def _validate_data_format(self, text: str, base_confidence: float) -> float:
        """Validate data format for specific types"""
        # Credit card validation using Luhn algorithm
        if re.match(r'^\d{13,19}$', text.replace(' ', '').replace('-', '')):
            if self._luhn_checksum(text.replace(' ', '').replace('-', '')):
                return min(1.0, base_confidence + 0.2)
        
        # Phone number validation
        try:
            parsed = phonenumbers.parse(text, None)
            if phonenumbers.is_valid_number(parsed):
                return min(1.0, base_confidence + 0.15)
        except:
            pass
            
        return base_confidence
    
    def _luhn_checksum(self, card_num: str) -> bool:
        """Validate credit card using Luhn algorithm"""
        def digits_of(n):
            return [int(d) for d in str(n)]
        
        digits = digits_of(card_num)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        checksum = sum(odd_digits)
        for d in even_digits:
            checksum += sum(digits_of(d*2))
        return checksum % 10 == 0
    
    def _determine_classification(self, data_type: SensitiveDataType) -> DataClassification:
        """Determine data classification level"""
        high_risk_types = {
            SensitiveDataType.SSN, SensitiveDataType.CREDIT_CARD, 
            SensitiveDataType.MEDICAL_RECORD, SensitiveDataType.PASSPORT
        }
        
        medium_risk_types = {
            SensitiveDataType.EMAIL, SensitiveDataType.PHONE,
            SensitiveDataType.BANK_ACCOUNT, SensitiveDataType.API_KEY
        }
        
        if data_type in high_risk_types:
            return DataClassification.RESTRICTED
        elif data_type in medium_risk_types:
            return DataClassification.CONFIDENTIAL
        else:
            return DataClassification.INTERNAL
    
    def _redact_text(self, text: str, data_type: SensitiveDataType) -> str:
        """Redact sensitive text appropriately"""
        if data_type == SensitiveDataType.EMAIL:
            parts = text.split('@')
            if len(parts) == 2:
                username = parts[0]
                domain = parts[1]
                redacted_username = username[:2] + '*' * (len(username) - 2) if len(username) > 2 else '*' * len(username)
                return f"{redacted_username}@{domain}"
        
        elif data_type == SensitiveDataType.CREDIT_CARD:
            clean_num = re.sub(r'\D', '', text)
            return f"****-****-****-{clean_num[-4:]}" if len(clean_num) >= 4 else "****-****-****-****"
        
        elif data_type == SensitiveDataType.SSN:
            clean_ssn = re.sub(r'\D', '', text)
            return f"***-**-{clean_ssn[-4:]}" if len(clean_ssn) >= 4 else "***-**-****"
        
        elif data_type == SensitiveDataType.PHONE:
            clean_phone = re.sub(r'\D', '', text)
            return f"***-***-{clean_phone[-4:]}" if len(clean_phone) >= 4 else "***-***-****"
        
        else:
            # Generic redaction
            if len(text) <= 4:
                return '*' * len(text)
            else:
                return text[:2] + '*' * (len(text) - 4) + text[-2:]


class DataRedactionEngine:
    """Engine for redacting and masking sensitive data"""
    
    def __init__(self, encryption_key: Optional[bytes] = None):
        self.logger = get_logger(__name__)
        self.encryption_key = encryption_key or Fernet.generate_key()
        self.fernet = Fernet(self.encryption_key)
    
    def redact_data(
        self, 
        text: str, 
        detection_results: List[DataDetectionResult],
        redaction_method: str = "mask"
    ) -> Tuple[str, Dict[str, Any]]:
        """Redact sensitive data from text"""
        
        redacted_text = text
        redaction_map = {}
        offset = 0
        
        # Sort by position to handle replacements correctly
        sorted_results = sorted(detection_results, key=lambda x: x.start_position)
        
        for result in sorted_results:
            start = result.start_position + offset
            end = result.end_position + offset
            original = result.matched_text
            
            if redaction_method == "mask":
                replacement = result.redacted_text
            elif redaction_method == "encrypt":
                replacement = self._encrypt_data(original)
            elif redaction_method == "tokenize":
                replacement = self._tokenize_data(original, result.data_type)
            elif redaction_method == "remove":
                replacement = ""
            else:
                replacement = result.redacted_text
            
            # Store redaction mapping
            redaction_id = str(uuid.uuid4())
            redaction_map[redaction_id] = {
                'original': original,
                'replacement': replacement,
                'data_type': result.data_type.value,
                'classification': result.classification.value,
                'confidence': result.confidence,
                'method': redaction_method
            }
            
            # Replace in text
            redacted_text = redacted_text[:start] + replacement + redacted_text[end:]
            offset += len(replacement) - len(original)
        
        metadata = {
            'redaction_count': len(redaction_map),
            'redaction_method': redaction_method,
            'timestamp': datetime.now().isoformat()
        }
        
        return redacted_text, {'redactions': redaction_map, 'metadata': metadata}
    
    def _encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data"""
        encrypted = self.fernet.encrypt(data.encode())
        return f"[ENCRYPTED:{encrypted.decode()}]"
    
    def _tokenize_data(self, data: str, data_type: SensitiveDataType) -> str:
        """Create a token for sensitive data"""
        token_hash = hashlib.sha256(data.encode()).hexdigest()[:8]
        return f"[TOKEN:{data_type.value.upper()}:{token_hash}]"
    
    def decrypt_data(self, encrypted_text: str) -> str:
        """Decrypt encrypted data"""
        try:
            if encrypted_text.startswith("[ENCRYPTED:") and encrypted_text.endswith("]"):
                encrypted_data = encrypted_text[11:-1]
                decrypted = self.fernet.decrypt(encrypted_data.encode())
                return decrypted.decode()
        except Exception as e:
            self.logger.error(f"Failed to decrypt data: {e}")
            return encrypted_text
        
        return encrypted_text


class DLPPolicyEngine:
    """Policy engine for DLP rules and enforcement"""
    
    def __init__(self, audit_logger: Optional[AuditLogger] = None):
        self.logger = get_logger(__name__)
        self.policies: Dict[str, DLPPolicy] = {}
        self.audit_logger = audit_logger or AuditLogger()
        self._initialize_default_policies()
    
    def _initialize_default_policies(self):
        """Initialize default DLP policies"""
        
        # GDPR compliance policy
        gdpr_policy = DLPPolicy(
            policy_id="gdpr_001",
            name="GDPR Personal Data Protection",
            description="Protect personal data according to GDPR requirements",
            enabled=True,
            data_types=[
                SensitiveDataType.EMAIL, SensitiveDataType.PHONE,
                SensitiveDataType.SSN, SensitiveDataType.IP_ADDRESS
            ],
            classification_levels=[DataClassification.CONFIDENTIAL, DataClassification.RESTRICTED],
            actions=[DLPAction.REDACT, DLPAction.LOG, DLPAction.ALERT],
            compliance_frameworks=[ComplianceFramework.GDPR],
            applies_to={
                "locations": ["database", "api", "exports"],
                "data_flows": ["external", "cross_border"]
            }
        )
        self.add_policy(gdpr_policy)
        
        # PCI-DSS compliance policy
        pci_policy = DLPPolicy(
            policy_id="pci_001",
            name="PCI-DSS Card Data Protection",
            description="Protect payment card data according to PCI-DSS",
            enabled=True,
            data_types=[SensitiveDataType.CREDIT_CARD, SensitiveDataType.BANK_ACCOUNT],
            classification_levels=[DataClassification.RESTRICTED],
            actions=[DLPAction.BLOCK, DLPAction.ENCRYPT, DLPAction.ALERT],
            compliance_frameworks=[ComplianceFramework.PCI_DSS],
            applies_to={
                "locations": ["all"],
                "operations": ["create", "read", "update", "export"]
            }
        )
        self.add_policy(pci_policy)
        
        # HIPAA compliance policy
        hipaa_policy = DLPPolicy(
            policy_id="hipaa_001",
            name="HIPAA PHI Protection",
            description="Protect protected health information according to HIPAA",
            enabled=True,
            data_types=[SensitiveDataType.MEDICAL_RECORD, SensitiveDataType.HEALTH_ID],
            classification_levels=[DataClassification.RESTRICTED],
            actions=[DLPAction.ENCRYPT, DLPAction.LOG, DLPAction.ALERT],
            compliance_frameworks=[ComplianceFramework.HIPAA],
            applies_to={
                "locations": ["all"],
                "users": ["healthcare_workers", "administrators"]
            }
        )
        self.add_policy(hipaa_policy)
    
    def add_policy(self, policy: DLPPolicy):
        """Add a DLP policy"""
        self.policies[policy.policy_id] = policy
        self.logger.info(f"Added DLP policy: {policy.name} ({policy.policy_id})")
    
    def remove_policy(self, policy_id: str):
        """Remove a DLP policy"""
        if policy_id in self.policies:
            policy_name = self.policies[policy_id].name
            del self.policies[policy_id]
            self.logger.info(f"Removed DLP policy: {policy_name} ({policy_id})")
    
    def evaluate_policies(
        self, 
        detection_results: List[DataDetectionResult],
        context: Dict[str, Any]
    ) -> List[Tuple[DLPPolicy, DLPAction]]:
        """Evaluate DLP policies against detection results"""
        
        policy_actions = []
        
        for result in detection_results:
            for policy in self.policies.values():
                if not policy.enabled:
                    continue
                
                if self._policy_applies(policy, result, context):
                    # Determine appropriate action
                    action = self._determine_action(policy, result, context)
                    policy_actions.append((policy, action))
                    
                    # Log policy evaluation
                    self.audit_logger.log_audit_event(
                        user_id=context.get('user_id'),
                        action=ActionType.READ,
                        resource_type="dlp_policy",
                        resource_id=policy.policy_id,
                        metadata={
                            'data_type': result.data_type.value,
                            'classification': result.classification.value,
                            'confidence': result.confidence,
                            'action': action.value,
                            'context': context
                        }
                    )
        
        return policy_actions
    
    def _policy_applies(
        self, 
        policy: DLPPolicy, 
        result: DataDetectionResult, 
        context: Dict[str, Any]
    ) -> bool:
        """Check if policy applies to the detection result"""
        
        # Check data type
        if result.data_type not in policy.data_types:
            return False
        
        # Check classification level
        if result.classification not in policy.classification_levels:
            return False
        
        # Check location/context
        location = context.get('location', 'unknown')
        if 'locations' in policy.applies_to:
            allowed_locations = policy.applies_to['locations']
            if 'all' not in allowed_locations and location not in allowed_locations:
                return False
        
        # Check user context
        user_id = context.get('user_id')
        if 'users' in policy.applies_to and user_id:
            allowed_users = policy.applies_to['users']
            if user_id not in allowed_users:
                return False
        
        return True
    
    def _determine_action(
        self, 
        policy: DLPPolicy, 
        result: DataDetectionResult, 
        context: Dict[str, Any]
    ) -> DLPAction:
        """Determine the appropriate action based on policy and context"""
        
        # High confidence and restricted data typically gets blocked
        if result.confidence >= 0.9 and result.classification == DataClassification.RESTRICTED:
            if DLPAction.BLOCK in policy.actions:
                return DLPAction.BLOCK
            elif DLPAction.ENCRYPT in policy.actions:
                return DLPAction.ENCRYPT
        
        # Medium confidence gets redacted
        if result.confidence >= 0.7:
            if DLPAction.REDACT in policy.actions:
                return DLPAction.REDACT
        
        # Low confidence gets logged
        if DLPAction.LOG in policy.actions:
            return DLPAction.LOG
        
        # Default to allow if no specific action
        return DLPAction.ALLOW


class EnterpriseDLPManager:
    """Main DLP manager orchestrating all components"""
    
    def __init__(self, encryption_key: Optional[bytes] = None):
        self.logger = get_logger(__name__)
        self.detector = SensitiveDataDetector()
        self.redaction_engine = DataRedactionEngine(encryption_key)
        self.policy_engine = DLPPolicyEngine()
        self.incidents: List[DLPIncident] = []
        
    def scan_data(
        self, 
        data: Union[str, Dict[str, Any]], 
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Comprehensive data scan and protection"""
        
        context = context or {}
        scan_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        self.logger.info(f"Starting DLP scan {scan_id}")
        
        # Convert data to text for scanning
        if isinstance(data, dict):
            text_data = json.dumps(data, ensure_ascii=False)
        else:
            text_data = str(data)
        
        # Detect sensitive data
        detection_results = self.detector.detect_sensitive_data(
            text_data, 
            context.get('description', '')
        )
        
        # Evaluate policies
        policy_actions = self.policy_engine.evaluate_policies(detection_results, context)
        
        # Apply actions
        final_data = data
        incidents = []
        
        if detection_results:
            # Determine overall action
            actions_to_take = [action for _, action in policy_actions]
            
            if DLPAction.BLOCK in actions_to_take:
                # Block the entire operation
                self._create_incident(scan_id, detection_results, DLPAction.BLOCK, context)
                return {
                    'scan_id': scan_id,
                    'action': 'blocked',
                    'message': 'Data access blocked due to DLP policy',
                    'detections': len(detection_results),
                    'risk_score': max(r.confidence * 10 for r in detection_results)
                }
            
            elif DLPAction.REDACT in actions_to_take or DLPAction.ENCRYPT in actions_to_take:
                # Redact/encrypt sensitive data
                redaction_method = 'encrypt' if DLPAction.ENCRYPT in actions_to_take else 'mask'
                redacted_text, redaction_info = self.redaction_engine.redact_data(
                    text_data, detection_results, redaction_method
                )
                
                # Convert back to original format
                if isinstance(data, dict):
                    try:
                        final_data = json.loads(redacted_text)
                    except json.JSONDecodeError:
                        final_data = {'redacted_content': redacted_text}
                else:
                    final_data = redacted_text
                
                self._create_incident(scan_id, detection_results, DLPAction.REDACT, context)
        
        # Create scan report
        scan_duration = (datetime.now() - start_time).total_seconds()
        
        report = {
            'scan_id': scan_id,
            'action': 'processed',
            'data': final_data,
            'detections': len(detection_results),
            'sensitive_data_types': list(set(r.data_type.value for r in detection_results)),
            'compliance_frameworks': list(set(
                fw.value for r in detection_results for fw in r.compliance_frameworks
            )),
            'risk_score': max((r.confidence * 10 for r in detection_results), default=0),
            'scan_duration_seconds': scan_duration,
            'policy_violations': len(policy_actions),
            'metadata': {
                'timestamp': start_time.isoformat(),
                'context': context
            }
        }
        
        self.logger.info(f"DLP scan {scan_id} completed in {scan_duration:.2f}s")
        
        return report
    
    def _create_incident(
        self, 
        scan_id: str, 
        detection_results: List[DataDetectionResult], 
        action: DLPAction,
        context: Dict[str, Any]
    ):
        """Create DLP incident record"""
        
        for result in detection_results:
            incident = DLPIncident(
                incident_id=str(uuid.uuid4()),
                timestamp=datetime.now(),
                policy_id="auto_detected",  # This would be set by policy engine
                data_type=result.data_type,
                classification=result.classification,
                action_taken=action,
                source_location=context.get('location', 'unknown'),
                destination_location=context.get('destination'),
                user_id=context.get('user_id'),
                session_id=context.get('session_id'),
                confidence=result.confidence,
                risk_score=result.confidence * 10,
                details={
                    'scan_id': scan_id,
                    'matched_text_length': len(result.matched_text),
                    'context': result.context,
                    'compliance_frameworks': [fw.value for fw in result.compliance_frameworks],
                    'metadata': result.metadata
                }
            )
            
            self.incidents.append(incident)
            self.logger.warning(f"DLP incident created: {incident.incident_id}")
    
    def get_incidents(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        data_type: Optional[SensitiveDataType] = None,
        resolved: Optional[bool] = None
    ) -> List[DLPIncident]:
        """Get DLP incidents with filters"""
        
        filtered_incidents = self.incidents
        
        if start_date:
            filtered_incidents = [i for i in filtered_incidents if i.timestamp >= start_date]
        
        if end_date:
            filtered_incidents = [i for i in filtered_incidents if i.timestamp <= end_date]
        
        if data_type:
            filtered_incidents = [i for i in filtered_incidents if i.data_type == data_type]
        
        if resolved is not None:
            filtered_incidents = [i for i in filtered_incidents if i.resolved == resolved]
        
        return filtered_incidents
    
    def get_dlp_dashboard(self) -> Dict[str, Any]:
        """Get DLP dashboard metrics"""
        
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)
        
        recent_incidents = [i for i in self.incidents if i.timestamp >= last_24h]
        weekly_incidents = [i for i in self.incidents if i.timestamp >= last_7d]
        
        # Data type distribution
        data_types = {}
        for incident in weekly_incidents:
            dt = incident.data_type.value
            data_types[dt] = data_types.get(dt, 0) + 1
        
        # Risk score distribution
        risk_levels = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        for incident in weekly_incidents:
            if incident.risk_score <= 3:
                risk_levels['low'] += 1
            elif incident.risk_score <= 6:
                risk_levels['medium'] += 1
            elif incident.risk_score <= 8:
                risk_levels['high'] += 1
            else:
                risk_levels['critical'] += 1
        
        return {
            'incidents_24h': len(recent_incidents),
            'incidents_7d': len(weekly_incidents),
            'total_incidents': len(self.incidents),
            'unresolved_incidents': len([i for i in self.incidents if not i.resolved]),
            'data_type_distribution': data_types,
            'risk_level_distribution': risk_levels,
            'top_data_types': sorted(data_types.items(), key=lambda x: x[1], reverse=True)[:5],
            'policies_active': len([p for p in self.policy_engine.policies.values() if p.enabled]),
            'compliance_coverage': list(set(
                fw.value for p in self.policy_engine.policies.values() for fw in p.compliance_frameworks
            ))
        }