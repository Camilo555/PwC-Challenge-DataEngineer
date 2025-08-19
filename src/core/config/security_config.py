"""
Security Configuration
Provides comprehensive security settings for production deployment
"""
from __future__ import annotations

import secrets
from typing import List, Dict, Optional
from pydantic import BaseSettings, Field, validator

from .base_config import Environment


class SecurityConfig(BaseSettings):
    """Configuration for security and authentication."""
    
    # API Security
    secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32), env="SECRET_KEY")
    api_key_header: str = Field(default="X-API-Key", env="API_KEY_HEADER")
    api_keys: List[str] = Field(default_factory=list, env="API_KEYS")
    
    # Authentication
    auth_enabled: bool = Field(default=True, env="AUTH_ENABLED")
    auth_type: str = Field(default="basic", env="AUTH_TYPE")  # basic, oauth2, jwt
    basic_auth_username: str = Field(default="admin", env="BASIC_AUTH_USERNAME")
    basic_auth_password: str = Field(default="changeme123", env="BASIC_AUTH_PASSWORD")
    
    # JWT Configuration
    jwt_secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32), env="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    jwt_expiration_hours: int = Field(default=24, env="JWT_EXPIRATION_HOURS")
    jwt_refresh_expiration_days: int = Field(default=30, env="JWT_REFRESH_EXPIRATION_DAYS")
    
    # OAuth2 Configuration
    oauth2_client_id: Optional[str] = Field(default=None, env="OAUTH2_CLIENT_ID")
    oauth2_client_secret: Optional[str] = Field(default=None, env="OAUTH2_CLIENT_SECRET")
    oauth2_authorization_url: Optional[str] = Field(default=None, env="OAUTH2_AUTHORIZATION_URL")
    oauth2_token_url: Optional[str] = Field(default=None, env="OAUTH2_TOKEN_URL")
    oauth2_scopes: List[str] = Field(default_factory=lambda: ["read", "write"], env="OAUTH2_SCOPES")
    
    # CORS Configuration
    cors_enabled: bool = Field(default=True, env="CORS_ENABLED")
    cors_allowed_origins: List[str] = Field(
        default_factory=lambda: ["http://localhost:3000", "http://localhost:8000"],
        env="CORS_ALLOWED_ORIGINS"
    )
    cors_allowed_methods: List[str] = Field(
        default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        env="CORS_ALLOWED_METHODS"
    )
    cors_allowed_headers: List[str] = Field(
        default_factory=lambda: ["*"],
        env="CORS_ALLOWED_HEADERS"
    )
    cors_allow_credentials: bool = Field(default=True, env="CORS_ALLOW_CREDENTIALS")
    
    # Rate Limiting
    rate_limiting_enabled: bool = Field(default=True, env="RATE_LIMITING_ENABLED")
    rate_limit_requests_per_minute: int = Field(default=60, env="RATE_LIMIT_REQUESTS_PER_MINUTE")
    rate_limit_burst: int = Field(default=10, env="RATE_LIMIT_BURST")
    
    # HTTPS/TLS Configuration
    https_enabled: bool = Field(default=False, env="HTTPS_ENABLED")
    ssl_cert_path: Optional[str] = Field(default=None, env="SSL_CERT_PATH")
    ssl_key_path: Optional[str] = Field(default=None, env="SSL_KEY_PATH")
    ssl_ca_cert_path: Optional[str] = Field(default=None, env="SSL_CA_CERT_PATH")
    tls_version: str = Field(default="TLSv1.2", env="TLS_VERSION")
    
    # Database Security
    db_ssl_enabled: bool = Field(default=False, env="DB_SSL_ENABLED")
    db_ssl_cert_path: Optional[str] = Field(default=None, env="DB_SSL_CERT_PATH")
    db_ssl_key_path: Optional[str] = Field(default=None, env="DB_SSL_KEY_PATH")
    db_ssl_ca_cert_path: Optional[str] = Field(default=None, env="DB_SSL_CA_CERT_PATH")
    
    # Encryption
    encryption_enabled: bool = Field(default=False, env="ENCRYPTION_ENABLED")
    encryption_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32), env="ENCRYPTION_KEY")
    encryption_algorithm: str = Field(default="AES-256-GCM", env="ENCRYPTION_ALGORITHM")
    
    # Password Policy
    min_password_length: int = Field(default=8, env="MIN_PASSWORD_LENGTH")
    require_uppercase: bool = Field(default=True, env="REQUIRE_UPPERCASE")
    require_lowercase: bool = Field(default=True, env="REQUIRE_LOWERCASE")
    require_numbers: bool = Field(default=True, env="REQUIRE_NUMBERS")
    require_special_chars: bool = Field(default=True, env="REQUIRE_SPECIAL_CHARS")
    password_expiry_days: Optional[int] = Field(default=None, env="PASSWORD_EXPIRY_DAYS")
    
    # Session Management
    session_timeout_minutes: int = Field(default=60, env="SESSION_TIMEOUT_MINUTES")
    session_cookie_secure: bool = Field(default=True, env="SESSION_COOKIE_SECURE")
    session_cookie_httponly: bool = Field(default=True, env="SESSION_COOKIE_HTTPONLY")
    session_cookie_samesite: str = Field(default="Lax", env="SESSION_COOKIE_SAMESITE")
    
    # Security Headers
    security_headers_enabled: bool = Field(default=True, env="SECURITY_HEADERS_ENABLED")
    hsts_enabled: bool = Field(default=True, env="HSTS_ENABLED")
    hsts_max_age: int = Field(default=31536000, env="HSTS_MAX_AGE")  # 1 year
    content_type_nosniff: bool = Field(default=True, env="CONTENT_TYPE_NOSNIFF")
    frame_options: str = Field(default="DENY", env="FRAME_OPTIONS")
    csp_enabled: bool = Field(default=True, env="CSP_ENABLED")
    csp_policy: str = Field(
        default="default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline';",
        env="CSP_POLICY"
    )
    
    # Input Validation
    input_validation_enabled: bool = Field(default=True, env="INPUT_VALIDATION_ENABLED")
    max_request_size_mb: int = Field(default=10, env="MAX_REQUEST_SIZE_MB")
    sql_injection_protection: bool = Field(default=True, env="SQL_INJECTION_PROTECTION")
    xss_protection: bool = Field(default=True, env="XSS_PROTECTION")
    
    # Logging Security Events
    security_audit_log_enabled: bool = Field(default=True, env="SECURITY_AUDIT_LOG_ENABLED")
    log_failed_auth_attempts: bool = Field(default=True, env="LOG_FAILED_AUTH_ATTEMPTS")
    log_suspicious_activity: bool = Field(default=True, env="LOG_SUSPICIOUS_ACTIVITY")
    
    # IP Allowlisting/Blocklisting
    ip_filtering_enabled: bool = Field(default=False, env="IP_FILTERING_ENABLED")
    allowed_ips: List[str] = Field(default_factory=list, env="ALLOWED_IPS")
    blocked_ips: List[str] = Field(default_factory=list, env="BLOCKED_IPS")
    
    # Secrets Management
    vault_enabled: bool = Field(default=False, env="VAULT_ENABLED")
    vault_url: Optional[str] = Field(default=None, env="VAULT_URL")
    vault_token: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    vault_mount_point: str = Field(default="secret", env="VAULT_MOUNT_POINT")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    @validator("basic_auth_password")
    def validate_password_strength(cls, v: str) -> str:
        """Validate password strength."""
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters long")
        
        has_upper = any(c.isupper() for c in v)
        has_lower = any(c.islower() for c in v)
        has_digit = any(c.isdigit() for c in v)
        has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in v)
        
        if not (has_upper and has_lower and has_digit and has_special):
            raise ValueError(
                "Password must contain uppercase, lowercase, number, and special character"
            )
        
        return v
    
    def get_environment_overrides(self, environment: Environment) -> Dict[str, any]:
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
        
        return overrides.get(environment, {})
    
    def get_security_headers(self) -> Dict[str, str]:
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
    
    def get_cors_config(self) -> Dict[str, any]:
        """Get CORS configuration."""
        if not self.cors_enabled:
            return {"allow_origins": False}
        
        return {
            "allow_origins": self.cors_allowed_origins,
            "allow_methods": self.cors_allowed_methods,
            "allow_headers": self.cors_allowed_headers,
            "allow_credentials": self.cors_allow_credentials,
        }
    
    def get_rate_limit_config(self) -> Dict[str, any]:
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
    
    def get_ssl_context(self) -> Dict[str, str]:
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