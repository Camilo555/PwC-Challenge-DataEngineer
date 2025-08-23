"""
Security Configuration
Provides comprehensive security settings for production deployment
"""
from __future__ import annotations

import secrets
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .base_config import Environment


class SecurityConfig(BaseSettings):
    """Configuration for security and authentication."""

    # API Security
    secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    api_key_header: str = Field(default="X-API-Key")
    api_keys: list[str] = Field(default_factory=list)

    # Authentication - NO DEFAULT PASSWORDS IN PRODUCTION
    auth_enabled: bool = Field(default=True)
    auth_type: str = Field(default="jwt")  # basic, oauth2, jwt
    basic_auth_username: str | None = Field(default=None)  # Must be set via environment
    basic_auth_password: str | None = Field(default=None)  # Must be set via environment - NEVER USE DEFAULTS
    admin_username: str | None = Field(default=None)  # Must be set via environment
    hashed_password: str | None = Field(default=None)  # Must be generated with strong password

    # JWT Configuration
    jwt_auth_enabled: bool = Field(default=True)
    jwt_secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    jwt_algorithm: str = Field(default="HS256")
    jwt_expiration_hours: int = Field(default=24)
    jwt_refresh_expiration_days: int = Field(default=30)

    # OAuth2 Configuration
    oauth2_client_id: str | None = Field(default=None)
    oauth2_client_secret: str | None = Field(default=None)
    oauth2_authorization_url: str | None = Field(default=None)
    oauth2_token_url: str | None = Field(default=None)
    oauth2_scopes: list[str] = Field(default_factory=lambda: ["read", "write"])

    # CORS Configuration
    cors_enabled: bool = Field(default=True)
    cors_allowed_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:3000", "http://localhost:8000"]
    )
    cors_allowed_methods: list[str] = Field(
        default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    cors_allowed_headers: list[str] = Field(
        default_factory=lambda: ["*"]
    )
    cors_allow_credentials: bool = Field(default=True)

    # Rate Limiting
    rate_limiting_enabled: bool = Field(default=True)
    rate_limit_requests_per_minute: int = Field(default=60)
    rate_limit_burst: int = Field(default=10)

    # HTTPS/TLS Configuration
    https_enabled: bool = Field(default=False)
    ssl_cert_path: str | None = Field(default=None)
    ssl_key_path: str | None = Field(default=None)
    ssl_ca_cert_path: str | None = Field(default=None)
    tls_version: str = Field(default="TLSv1.2")

    # Database Security
    db_ssl_enabled: bool = Field(default=False)
    db_ssl_cert_path: str | None = Field(default=None)
    db_ssl_key_path: str | None = Field(default=None)
    db_ssl_ca_cert_path: str | None = Field(default=None)

    # Encryption
    encryption_enabled: bool = Field(default=False)
    encryption_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    encryption_algorithm: str = Field(default="AES-256-GCM")

    # Password Policy
    min_password_length: int = Field(default=8)
    require_uppercase: bool = Field(default=True)
    require_lowercase: bool = Field(default=True)
    require_numbers: bool = Field(default=True)
    require_special_chars: bool = Field(default=True)
    password_expiry_days: int | None = Field(default=None)

    # Session Management
    session_timeout_minutes: int = Field(default=60)
    session_cookie_secure: bool = Field(default=True)
    session_cookie_httponly: bool = Field(default=True)
    session_cookie_samesite: str = Field(default="Lax")

    # Security Headers
    security_headers_enabled: bool = Field(default=True)
    hsts_enabled: bool = Field(default=True)
    hsts_max_age: int = Field(default=31536000)  # 1 year
    content_type_nosniff: bool = Field(default=True)
    frame_options: str = Field(default="DENY")
    csp_enabled: bool = Field(default=True)
    csp_policy: str = Field(
        default="default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline';"
    )

    # Input Validation
    input_validation_enabled: bool = Field(default=True)
    max_request_size_mb: int = Field(default=10)
    sql_injection_protection: bool = Field(default=True)
    xss_protection: bool = Field(default=True)

    # Logging Security Events
    security_audit_log_enabled: bool = Field(default=True)
    log_failed_auth_attempts: bool = Field(default=True)
    log_suspicious_activity: bool = Field(default=True)

    # IP Allowlisting/Blocklisting
    ip_filtering_enabled: bool = Field(default=False)
    allowed_ips: list[str] = Field(default_factory=list)
    blocked_ips: list[str] = Field(default_factory=list)

    # Secrets Management
    vault_enabled: bool = Field(default=False)
    vault_url: str | None = Field(default=None)
    vault_token: str | None = Field(default=None)
    vault_mount_point: str = Field(default="secret")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow"
    )

    @field_validator("basic_auth_password")
    @classmethod
    def validate_password_strength(cls, v: str | None) -> str | None:
        """Validate password strength."""
        import os

        if v is None:
            return None

        # Skip validation in development/testing environments
        env = os.getenv("ENVIRONMENT", "development").lower()
        if env in ["development", "testing"]:
            return v

        # Production password requirements
        if len(v) < 12:  # Increased minimum length
            raise ValueError("Production password must be at least 12 characters long")

        has_upper = any(c.isupper() for c in v)
        has_lower = any(c.islower() for c in v)
        has_digit = any(c.isdigit() for c in v)
        has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in v)

        if not (has_upper and has_lower and has_digit and has_special):
            raise ValueError(
                "Production password must contain uppercase, lowercase, number, and special character"
            )

        # Check for common patterns
        common_patterns = ["password", "123456", "admin", "user", "test"]
        if any(pattern in v.lower() for pattern in common_patterns):
            raise ValueError("Password cannot contain common patterns")

        return v

    def validate_production_security(self) -> None:
        """Validate security configuration for production deployment."""
        import os
        
        env = os.getenv("ENVIRONMENT", "development").lower()
        if env != "production":
            return
        
        errors = []
        
        # Validate authentication credentials are set
        if self.auth_enabled and self.auth_type == "basic":
            if not self.basic_auth_username:
                errors.append("BASIC_AUTH_USERNAME must be set in production environment")
            if not self.basic_auth_password:
                errors.append("BASIC_AUTH_PASSWORD must be set in production environment")
        
        # Validate HTTPS is enabled in production
        if not self.https_enabled:
            errors.append("HTTPS must be enabled in production environment")
        
        # Validate secrets are properly configured
        if not self.secret_key or len(self.secret_key) < 32:
            errors.append("SECRET_KEY must be at least 32 characters in production")
        
        if not self.jwt_secret_key or len(self.jwt_secret_key) < 32:
            errors.append("JWT_SECRET_KEY must be at least 32 characters in production")
        
        # Validate CORS is properly configured
        if "*" in self.cors_allowed_origins:
            errors.append("CORS wildcard (*) origins not allowed in production")
        
        if errors:
            raise ValueError(f"Production security validation failed: {'; '.join(errors)}")

    def get_environment_overrides(self, environment: Environment) -> dict[str, Any]:
        """Get environment-specific security overrides."""
        overrides = {
            Environment.DEVELOPMENT: {
                "auth_enabled": False,
                "rate_limiting_enabled": False,
                "https_enabled": False,
                "security_headers_enabled": False,
                "cors_allowed_origins": ["*"],
                "security_audit_log_enabled": False,
            },
            Environment.TESTING: {
                "auth_enabled": False,
                "rate_limiting_enabled": False,
                "https_enabled": False,
                "security_headers_enabled": False,
                "security_audit_log_enabled": False,
            },
            Environment.STAGING: {
                "auth_enabled": True,
                "rate_limiting_enabled": True,
                "https_enabled": True,
                "security_headers_enabled": True,
                "security_audit_log_enabled": True,
            },
            Environment.PRODUCTION: {
                "auth_enabled": True,
                "rate_limiting_enabled": True,
                "https_enabled": True,
                "security_headers_enabled": True,
                "hsts_enabled": True,
                "csp_enabled": True,
                "db_ssl_enabled": True,
                "encryption_enabled": True,
                "security_audit_log_enabled": True,
                "ip_filtering_enabled": True,
            }
        }

        result = overrides.get(environment, {})
        return result if isinstance(result, dict) else {}

    def get_security_headers(self) -> dict[str, str]:
        """Get security headers for HTTP responses."""
        headers = {}

        if self.security_headers_enabled:
            if self.hsts_enabled:
                headers["Strict-Transport-Security"] = f"max-age={self.hsts_max_age}; includeSubDomains"

            if self.content_type_nosniff:
                headers["X-Content-Type-Options"] = "nosniff"

            headers["X-Frame-Options"] = self.frame_options
            headers["X-XSS-Protection"] = "1; mode=block"

            if self.csp_enabled:
                headers["Content-Security-Policy"] = self.csp_policy

            headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"

        return headers

    def get_cors_config(self) -> dict[str, Any]:
        """Get CORS configuration."""
        if not self.cors_enabled:
            return {"allow_origins": False}

        return {
            "allow_origins": self.cors_allowed_origins,
            "allow_methods": self.cors_allowed_methods,
            "allow_headers": self.cors_allowed_headers,
            "allow_credentials": self.cors_allow_credentials,
        }

    def get_rate_limit_config(self) -> dict[str, Any]:
        """Get rate limiting configuration."""
        if not self.rate_limiting_enabled:
            return {"enabled": False}

        return {
            "enabled": True,
            "requests_per_minute": self.rate_limit_requests_per_minute,
            "burst": self.rate_limit_burst,
        }

    def validate_password(self, password: str) -> bool:
        """Validate password against policy."""
        if len(password) < self.min_password_length:
            return False

        if self.require_uppercase and not any(c.isupper() for c in password):
            return False

        if self.require_lowercase and not any(c.islower() for c in password):
            return False

        if self.require_numbers and not any(c.isdigit() for c in password):
            return False

        if self.require_special_chars and not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            return False

        return True

    def generate_api_key(self) -> str:
        """Generate a new API key."""
        return secrets.token_urlsafe(32)

    def get_ssl_context(self) -> dict[str, str]:
        """Get SSL context configuration."""
        if not self.https_enabled:
            return {}

        ssl_config = {
            "ssl_version": self.tls_version,
        }

        if self.ssl_cert_path:
            ssl_config["ssl_cert"] = self.ssl_cert_path

        if self.ssl_key_path:
            ssl_config["ssl_key"] = self.ssl_key_path

        if self.ssl_ca_cert_path:
            ssl_config["ssl_ca_cert"] = self.ssl_ca_cert_path

        return ssl_config
