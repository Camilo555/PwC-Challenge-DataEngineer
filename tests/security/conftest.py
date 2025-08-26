"""
Security Testing Configuration and Fixtures
Provides common fixtures, utilities, and test data for security testing
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import tempfile
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from src.core.security.enterprise_dlp import (
    EnterpriseDLPManager, SensitiveDataType, DataClassification, DLPAction
)
from src.core.security.compliance_framework import (
    ComplianceEngine, ComplianceFramework, ComplianceStatus, RiskLevel
)
from src.core.security.rbac_abac import (
    AuthenticationManager, User, Role, Permission, PermissionType, AuthenticationMethod
)
from src.core.security.advanced_security import AuditLogger, SecurityEvent, ThreatLevel
from src.api.main import app


# Test Data Classes and Constants
class SecurityTestData:
    """Test data for security tests"""
    
    # Sample PII data for DLP testing
    SAMPLE_PII_DATA = {
        "ssn": "123-45-6789",
        "credit_card": "4532-1234-5678-9012",
        "email": "john.doe@example.com",
        "phone": "+1-555-123-4567",
        "ip_address": "192.168.1.100",
        "medical_record": "MRN-12345678",
        "bank_account": "1234567890123456"
    }
    
    # Sample sensitive data for classification testing
    SAMPLE_CLASSIFIED_DATA = {
        "public": "This is public information",
        "internal": "Internal company data",
        "confidential": "Confidential customer information: john.doe@example.com",
        "restricted": "Restricted data with SSN: 123-45-6789 and CC: 4532-1234-5678-9012",
        "top_secret": "Top secret data with multiple PII elements"
    }
    
    # Attack payloads for penetration testing
    SQL_INJECTION_PAYLOADS = [
        "' OR '1'='1",
        "'; DROP TABLE users; --",
        "' UNION SELECT * FROM users --",
        "1' OR 1=1 /*",
        "admin'--",
        "'; EXEC xp_cmdshell('dir'); --"
    ]
    
    XSS_PAYLOADS = [
        "<script>alert('XSS')</script>",
        "<img src=x onerror=alert('XSS')>",
        "javascript:alert('XSS')",
        "<svg onload=alert('XSS')>",
        "'><script>alert('XSS')</script>",
        "\"><img src=x onerror=alert('XSS')>"
    ]
    
    # Common weak passwords for testing
    WEAK_PASSWORDS = [
        "password",
        "123456",
        "password123",
        "admin",
        "qwerty",
        "letmein",
        "welcome",
        "monkey"
    ]
    
    # Strong passwords for testing
    STRONG_PASSWORDS = [
        "MySecureP@ssw0rd!2024",
        "Tr0ub4dor&3",
        "C0rr3ct-H0rs3-B@tt3ry-St@pl3",
        "9aF$mK7#nP2qR*5tY",
        "SecureTestPassword123!"
    ]


class SecurityTestHelpers:
    """Helper utilities for security testing"""
    
    @staticmethod
    def generate_test_user(roles: List[str] = None, mfa_enabled: bool = False) -> User:
        """Generate a test user with specified roles"""
        user_id = f"test_user_{secrets.token_hex(4)}"
        username = f"testuser_{secrets.token_hex(4)}"
        email = f"{username}@testdomain.com"
        
        user = User(
            id=user_id,
            username=username,
            email=email,
            password_hash="$2b$12$test_hash",
            roles=set(roles) if roles else {"role_viewer"},
            mfa_enabled=mfa_enabled,
            is_active=True,
            is_verified=True
        )
        
        if mfa_enabled:
            user.mfa_secret = secrets.token_urlsafe(32)
            user.mfa_methods.add(AuthenticationMethod.MFA_TOTP)
        
        return user
    
    @staticmethod
    def generate_test_data_with_pii(pii_types: List[SensitiveDataType] = None) -> str:
        """Generate test data containing specified PII types"""
        if pii_types is None:
            pii_types = [SensitiveDataType.EMAIL, SensitiveDataType.PHONE]
        
        data_parts = ["This is a sample document containing:"]
        
        for pii_type in pii_types:
            if pii_type == SensitiveDataType.SSN:
                data_parts.append(f"SSN: {SecurityTestData.SAMPLE_PII_DATA['ssn']}")
            elif pii_type == SensitiveDataType.CREDIT_CARD:
                data_parts.append(f"Credit Card: {SecurityTestData.SAMPLE_PII_DATA['credit_card']}")
            elif pii_type == SensitiveDataType.EMAIL:
                data_parts.append(f"Contact: {SecurityTestData.SAMPLE_PII_DATA['email']}")
            elif pii_type == SensitiveDataType.PHONE:
                data_parts.append(f"Phone: {SecurityTestData.SAMPLE_PII_DATA['phone']}")
            elif pii_type == SensitiveDataType.IP_ADDRESS:
                data_parts.append(f"Server IP: {SecurityTestData.SAMPLE_PII_DATA['ip_address']}")
        
        return " ".join(data_parts)
    
    @staticmethod
    def create_test_jwt_token(user_id: str, roles: List[str] = None, 
                            expired: bool = False) -> str:
        """Create a test JWT token"""
        import jwt
        
        payload = {
            "user_id": user_id,
            "username": f"testuser_{user_id}",
            "roles": roles or ["role_viewer"],
            "iat": datetime.utcnow(),
            "exp": datetime.utcnow() - timedelta(hours=1) if expired else datetime.utcnow() + timedelta(hours=1)
        }
        
        return jwt.encode(payload, "test_secret", algorithm="HS256")
    
    @staticmethod
    def create_malicious_payload(attack_type: str, payload_data: str = None) -> str:
        """Create a malicious payload for testing"""
        if attack_type == "sql_injection":
            return payload_data or SecurityTestData.SQL_INJECTION_PAYLOADS[0]
        elif attack_type == "xss":
            return payload_data or SecurityTestData.XSS_PAYLOADS[0]
        elif attack_type == "path_traversal":
            return "../../../etc/passwd"
        elif attack_type == "command_injection":
            return "test; ls -la"
        else:
            return payload_data or "malicious_input"
    
    @staticmethod
    def simulate_attack_context(attack_type: str, user_agent: str = None, 
                              source_ip: str = None) -> Dict[str, Any]:
        """Simulate attack context"""
        return {
            "attack_type": attack_type,
            "user_agent": user_agent or "AttackBot/1.0",
            "source_ip": source_ip or "10.0.0.1",
            "timestamp": datetime.now().isoformat(),
            "session_id": f"attack_session_{uuid.uuid4()}",
            "suspicious_patterns": ["rapid_requests", "unusual_payload"]
        }


# Core Fixtures
@pytest.fixture
def security_test_data():
    """Provide security test data"""
    return SecurityTestData()


@pytest.fixture
def security_helpers():
    """Provide security test helpers"""
    return SecurityTestHelpers()


@pytest.fixture
def test_encryption_key():
    """Provide test encryption key for DLP"""
    return Fernet.generate_key()


@pytest.fixture
def temp_test_dir():
    """Provide temporary directory for test files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


# DLP Fixtures
@pytest.fixture
def dlp_manager(test_encryption_key):
    """Provide DLP manager instance for testing"""
    return EnterpriseDLPManager(encryption_key=test_encryption_key)


@pytest.fixture
def sample_pii_document(security_helpers):
    """Provide sample document with PII for testing"""
    return security_helpers.generate_test_data_with_pii([
        SensitiveDataType.SSN,
        SensitiveDataType.EMAIL,
        SensitiveDataType.CREDIT_CARD,
        SensitiveDataType.PHONE
    ])


# Compliance Fixtures
@pytest.fixture
def compliance_engine():
    """Provide compliance engine for testing"""
    return ComplianceEngine()


@pytest_asyncio.fixture
async def sample_compliance_assessment(compliance_engine):
    """Provide sample compliance assessment"""
    return await compliance_engine.run_compliance_assessment(
        framework=ComplianceFramework.GDPR,
        assessor_id="test_assessor"
    )


# RBAC/ABAC Fixtures
@pytest.fixture
def auth_manager():
    """Provide authentication manager for testing"""
    return AuthenticationManager()


@pytest.fixture
def test_user_admin(security_helpers):
    """Provide test admin user"""
    return security_helpers.generate_test_user(
        roles=["role_admin"],
        mfa_enabled=True
    )


@pytest.fixture
def test_user_analyst(security_helpers):
    """Provide test data analyst user"""
    return security_helpers.generate_test_user(
        roles=["role_data_analyst"],
        mfa_enabled=False
    )


@pytest.fixture
def test_user_viewer(security_helpers):
    """Provide test viewer user"""
    return security_helpers.generate_test_user(
        roles=["role_viewer"],
        mfa_enabled=False
    )


@pytest.fixture
def test_users(test_user_admin, test_user_analyst, test_user_viewer):
    """Provide collection of test users"""
    return {
        "admin": test_user_admin,
        "analyst": test_user_analyst,
        "viewer": test_user_viewer
    }


# API Testing Fixtures
@pytest.fixture
def test_client():
    """Provide FastAPI test client"""
    return TestClient(app)


@pytest.fixture
def authenticated_client(test_client, security_helpers, test_user_admin):
    """Provide authenticated test client"""
    token = security_helpers.create_test_jwt_token(
        test_user_admin.id, 
        list(test_user_admin.roles)
    )
    test_client.headers = {"Authorization": f"Bearer {token}"}
    return test_client


# Security Event Fixtures
@pytest.fixture
def audit_logger():
    """Provide audit logger for testing"""
    return AuditLogger()


@pytest.fixture
def sample_security_events():
    """Provide sample security events for testing"""
    events = []
    
    # Failed login event
    events.append(SecurityEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(),
        event_type="authentication_failure",
        severity="medium",
        user_id="test_user_001",
        source_ip="192.168.1.100",
        user_agent="Mozilla/5.0",
        details={
            "username": "test_user",
            "failure_reason": "invalid_password",
            "attempt_count": 3
        }
    ))
    
    # Suspicious data access event
    events.append(SecurityEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(),
        event_type="data_access_anomaly",
        severity="high",
        user_id="test_user_002",
        source_ip="10.0.0.1",
        user_agent="AttackBot/1.0",
        details={
            "resource": "sensitive_customer_data",
            "access_pattern": "bulk_download",
            "data_volume": "10GB"
        }
    ))
    
    return events


# Performance Testing Fixtures
@pytest.fixture
def performance_test_config():
    """Provide performance test configuration"""
    return {
        "concurrent_users": [1, 5, 10, 25, 50],
        "test_duration": 60,  # seconds
        "ramp_up_time": 30,   # seconds
        "think_time": 1,      # seconds between requests
        "timeout": 30,        # request timeout
        "success_rate_threshold": 0.95,
        "response_time_p95_threshold": 2.0,  # seconds
        "error_rate_threshold": 0.05
    }


# Penetration Testing Fixtures
@pytest.fixture
def penetration_test_payloads():
    """Provide penetration test payloads"""
    return {
        "sql_injection": SecurityTestData.SQL_INJECTION_PAYLOADS,
        "xss": SecurityTestData.XSS_PAYLOADS,
        "path_traversal": [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "....//....//....//etc/passwd",
            "%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd"
        ],
        "command_injection": [
            "test; ls -la",
            "test && dir",
            "test | whoami",
            "test`whoami`",
            "test$(whoami)"
        ],
        "ldap_injection": [
            "*)(&)",
            "*)(uid=*",
            "*)(|(uid=*))",
            "admin)(&(password=*))"
        ]
    }


# Mock Fixtures
@pytest.fixture
def mock_external_api():
    """Mock external API calls for security testing"""
    with patch('requests.get') as mock_get, \
         patch('requests.post') as mock_post, \
         patch('requests.put') as mock_put, \
         patch('requests.delete') as mock_delete:
        
        # Configure default responses
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"status": "ok"}
        
        mock_post.return_value.status_code = 201
        mock_post.return_value.json.return_value = {"id": "12345"}
        
        mock_put.return_value.status_code = 200
        mock_put.return_value.json.return_value = {"updated": True}
        
        mock_delete.return_value.status_code = 204
        
        yield {
            "get": mock_get,
            "post": mock_post,
            "put": mock_put,
            "delete": mock_delete
        }


@pytest.fixture
def mock_database():
    """Mock database operations for security testing"""
    with patch('sqlalchemy.create_engine') as mock_engine, \
         patch('sqlalchemy.orm.sessionmaker') as mock_sessionmaker:
        
        mock_session = Mock()
        mock_sessionmaker.return_value = mock_session
        
        yield mock_session


# Utility Fixtures
@pytest.fixture(autouse=True)
def setup_test_logging():
    """Setup test logging configuration"""
    import logging
    
    # Configure test logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Test markers
def pytest_configure(config):
    """Configure pytest markers"""
    config.addinivalue_line(
        "markers", "security: mark test as security test"
    )
    config.addinivalue_line(
        "markers", "dlp: mark test as DLP test"
    )
    config.addinivalue_line(
        "markers", "compliance: mark test as compliance test"
    )
    config.addinivalue_line(
        "markers", "access_control: mark test as access control test"
    )
    config.addinivalue_line(
        "markers", "api_security: mark test as API security test"
    )
    config.addinivalue_line(
        "markers", "websocket_security: mark test as WebSocket security test"
    )
    config.addinivalue_line(
        "markers", "penetration: mark test as penetration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


# Test collection filters
def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers"""
    if config.getoption("--security-only"):
        security_items = []
        for item in items:
            if "security" in [marker.name for marker in item.iter_markers()]:
                security_items.append(item)
        items[:] = security_items


def pytest_addoption(parser):
    """Add custom command line options"""
    parser.addoption(
        "--security-only",
        action="store_true",
        default=False,
        help="run only security tests"
    )
    parser.addoption(
        "--skip-slow",
        action="store_true",
        default=False,
        help="skip slow running tests"
    )
    parser.addoption(
        "--penetration-tests",
        action="store_true",
        default=False,
        help="run penetration tests (use with caution)"
    )