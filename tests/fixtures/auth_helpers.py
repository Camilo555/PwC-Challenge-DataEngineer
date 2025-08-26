"""
Authentication helpers for testing.
Provides mock JWT tokens and authentication utilities for test cases.
"""
import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import Any

from jose import jwt


class MockAuthService:
    """Mock authentication service for testing"""

    def __init__(self):
        self.secret_key = "test-secret-key-for-jwt-signing"
        self.algorithm = "HS256"
        self.users = {
            "test_user": "test_password",
            "admin_user": "admin_password",
            "load_test_user": "load_test_password",
            "performance_user": "performance_password"
        }

    def create_jwt_token(self, username: str, expires_in_minutes: int = 60) -> str:
        """Create a JWT token for testing"""
        payload = {
            "sub": username,
            "username": username,
            "iat": datetime.utcnow(),
            "exp": datetime.utcnow() + timedelta(minutes=expires_in_minutes),
            "roles": ["user"] if username != "admin_user" else ["admin", "user"],
            "test_token": True
        }

        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def validate_credentials(self, username: str, password: str) -> bool:
        """Validate user credentials"""
        return self.users.get(username) == password

    def get_user_permissions(self, username: str) -> dict[str, Any]:
        """Get user permissions for testing"""
        permissions = {
            "can_read": True,
            "can_write": username in ["admin_user", "test_user"],
            "can_delete": username == "admin_user",
            "rate_limit": 1000 if username == "admin_user" else 100,
            "scopes": ["read", "write"] if username != "admin_user" else ["read", "write", "admin"]
        }
        return permissions


# Global mock auth service instance
_mock_auth_service = MockAuthService()


async def get_test_jwt_token(username: str = "test_user", password: str = "test_password") -> str:
    """
    Get a JWT token for testing purposes.
    
    Args:
        username: Username for authentication
        password: Password for authentication
    
    Returns:
        JWT token string for use in test requests
    """
    # Simulate async authentication process
    await asyncio.sleep(0.01)

    if not _mock_auth_service.validate_credentials(username, password):
        raise ValueError(f"Invalid credentials for user: {username}")

    return _mock_auth_service.create_jwt_token(username)


async def get_admin_jwt_token() -> str:
    """Get an admin JWT token for testing admin endpoints"""
    return await get_test_jwt_token("admin_user", "admin_password")


def create_test_user_token(
    username: str = "test_user",
    roles: list | None = None,
    expires_in_minutes: int = 60,
    custom_claims: dict | None = None
) -> str:
    """
    Create a test JWT token with custom parameters.
    
    Args:
        username: Username for the token
        roles: List of roles for the user
        expires_in_minutes: Token expiration time
        custom_claims: Additional claims to include
    
    Returns:
        JWT token string
    """
    if roles is None:
        roles = ["user"]

    payload = {
        "sub": username,
        "username": username,
        "iat": datetime.utcnow(),
        "exp": datetime.utcnow() + timedelta(minutes=expires_in_minutes),
        "roles": roles,
        "test_token": True
    }

    if custom_claims:
        payload.update(custom_claims)

    return jwt.encode(payload, _mock_auth_service.secret_key, algorithm=_mock_auth_service.algorithm)


def decode_test_token(token: str) -> dict[str, Any]:
    """Decode a test JWT token for inspection"""
    try:
        return jwt.decode(token, _mock_auth_service.secret_key, algorithms=[_mock_auth_service.algorithm])
    except jwt.InvalidTokenError:
        raise ValueError("Invalid test token")


def create_expired_token(username: str = "test_user") -> str:
    """Create an expired JWT token for testing expired token scenarios"""
    payload = {
        "sub": username,
        "username": username,
        "iat": datetime.utcnow() - timedelta(minutes=120),
        "exp": datetime.utcnow() - timedelta(minutes=60),  # Expired 1 hour ago
        "roles": ["user"],
        "test_token": True
    }

    return jwt.encode(payload, _mock_auth_service.secret_key, algorithm=_mock_auth_service.algorithm)


def create_malformed_token() -> str:
    """Create a malformed JWT token for testing error handling"""
    return "invalid.jwt.token"


async def authenticate_test_request(username: str, password: str) -> dict[str, Any]:
    """
    Simulate full authentication flow for testing.
    
    Returns authentication response similar to real API.
    """
    await asyncio.sleep(0.05)  # Simulate network delay

    if not _mock_auth_service.validate_credentials(username, password):
        raise ValueError("Authentication failed")

    token = _mock_auth_service.create_jwt_token(username)
    permissions = _mock_auth_service.get_user_permissions(username)

    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": 3600,
        "username": username,
        "permissions": permissions,
        "authenticated": True
    }


def get_test_headers(username: str = "test_user", password: str = "test_password") -> dict[str, str]:
    """Get test headers with authentication for synchronous tests"""
    token = _mock_auth_service.create_jwt_token(username)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


async def get_test_headers_async(username: str = "test_user", password: str = "test_password") -> dict[str, str]:
    """Get test headers with authentication for async tests"""
    token = await get_test_jwt_token(username, password)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


def create_api_key_token(api_key: str = "test-api-key") -> str:
    """Create an API key based token for testing API key authentication"""
    # Simulate API key hashing
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    payload = {
        "api_key_hash": key_hash,
        "key_type": "api_key",
        "iat": datetime.utcnow(),
        "exp": datetime.utcnow() + timedelta(days=30),
        "permissions": ["read", "write"],
        "test_token": True
    }

    return jwt.encode(payload, _mock_auth_service.secret_key, algorithm=_mock_auth_service.algorithm)


# Convenience functions for common test scenarios
async def get_performance_test_token() -> str:
    """Get a token optimized for performance testing"""
    return await get_test_jwt_token("performance_user", "performance_password")


async def get_load_test_credentials() -> dict[str, str]:
    """Get credentials specifically for load testing"""
    token = await get_test_jwt_token("load_test_user", "load_test_password")
    return {
        "Authorization": f"Bearer {token}",
        "User-Agent": "LoadTestClient/1.0",
        "Accept": "application/json"
    }


def validate_test_token_structure(token: str) -> bool:
    """Validate that a token has the expected structure for tests"""
    try:
        decoded = decode_test_token(token)
        required_fields = ["sub", "username", "iat", "exp", "roles"]
        return all(field in decoded for field in required_fields) and decoded.get("test_token") is True
    except Exception:
        return False
