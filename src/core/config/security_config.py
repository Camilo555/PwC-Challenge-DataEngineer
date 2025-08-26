"""
Security Configuration
Provides comprehensive security settings for production deployment
"""
from __future__ import annotations

import json
import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Any

import bcrypt
from jose import jwt
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .base_config import Environment

logger = logging.getLogger(__name__)


class EncryptionConfig(BaseSettings):
    """Configuration for encryption settings."""

    algorithm: str = Field(default=os.getenv("ENCRYPTION_ALGORITHM", "AES"))
    key_size: int = Field(default=int(os.getenv("ENCRYPTION_KEY_SIZE", "256")))
    mode: str = Field(default=os.getenv("ENCRYPTION_MODE", "GCM"))
    key_derivation: str = Field(default=os.getenv("ENCRYPTION_KEY_DERIVATION", "PBKDF2"))
    iterations: int = Field(default=int(os.getenv("ENCRYPTION_ITERATIONS", "100000")))
    salt_length: int = Field(default=int(os.getenv("ENCRYPTION_SALT_LENGTH", "32")))

    @field_validator("key_size")
    @classmethod
    def validate_key_size(cls, v: int) -> int:
        if v < 128:
            raise ValueError("Key size must be at least 128 bits")
        return v

    @field_validator("iterations")
    @classmethod
    def validate_iterations(cls, v: int) -> int:
        if v < 10000:
            raise ValueError("Iterations must be at least 10000")
        return v

    @field_validator("salt_length")
    @classmethod
    def validate_salt_length(cls, v: int) -> int:
        if v < 16:
            raise ValueError("Salt length must be at least 16 bytes")
        return v


class SecretManager(BaseSettings):
    """Configuration for secrets management."""

    provider: str = Field(default=os.getenv("SECRET_PROVIDER", "local"))
    vault_url: str | None = Field(default=os.getenv("SECRET_VAULT_URL"))
    aws_region: str = Field(default=os.getenv("SECRET_AWS_REGION", "us-east-1"))
    azure_vault_url: str | None = Field(default=os.getenv("SECRET_AZURE_VAULT_URL"))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._local_secrets_file = os.path.join(os.getcwd(), ".secrets.json")
        self._login_attempts: dict[str, list] = {}

    def store_secret(self, key: str, value: str) -> None:
        """Store a secret based on the configured provider."""
        if self.provider == "local":
            self._store_local_secret(key, value)
        elif self.provider == "aws":
            self._store_aws_secret(key, value)
        elif self.provider == "vault":
            self._store_vault_secret(key, value)
        elif self.provider == "azure":
            self._store_azure_secret(key, value)
        else:
            raise ValueError(f"Unsupported secret provider: {self.provider}")

    def get_secret(self, key: str) -> str:
        """Retrieve a secret based on the configured provider."""
        if self.provider == "local":
            return self._get_local_secret(key)
        elif self.provider == "aws":
            return self._get_aws_secret(key)
        elif self.provider == "vault":
            return self._get_vault_secret(key)
        elif self.provider == "azure":
            return self._get_azure_secret(key)
        else:
            raise ValueError(f"Unsupported secret provider: {self.provider}")

    def _store_local_secret(self, key: str, value: str) -> None:
        """Store secret in local JSON file."""
        secrets_data = {}
        if os.path.exists(self._local_secrets_file):
            with open(self._local_secrets_file) as f:
                secrets_data = json.load(f)

        secrets_data[key] = value

        with open(self._local_secrets_file, 'w') as f:
            json.dump(secrets_data, f, indent=2)

    def _get_local_secret(self, key: str) -> str:
        """Retrieve secret from local JSON file."""
        if not os.path.exists(self._local_secrets_file):
            raise ValueError(f"Secret '{key}' not found")

        with open(self._local_secrets_file) as f:
            secrets_data = json.load(f)

        if key not in secrets_data:
            raise ValueError(f"Secret '{key}' not found")

        return secrets_data[key]

    def _get_aws_secret(self, key: str) -> str:
        """Retrieve secret from AWS Secrets Manager."""
        try:
            import boto3
            client = boto3.client('secretsmanager', region_name=self.aws_region)
            response = client.get_secret_value(SecretId=key)
            return response['SecretString']
        except ImportError:
            raise ValueError("boto3 is required for AWS secrets manager")
        except Exception as e:
            raise ValueError(f"Failed to retrieve AWS secret '{key}': {e}")

    def _store_aws_secret(self, key: str, value: str) -> None:
        """Store secret in AWS Secrets Manager."""
        try:
            import boto3
            client = boto3.client('secretsmanager', region_name=self.aws_region)
            client.create_secret(Name=key, SecretString=value)
        except ImportError:
            raise ValueError("boto3 is required for AWS secrets manager")
        except Exception as e:
            raise ValueError(f"Failed to store AWS secret '{key}': {e}")

    def _get_vault_secret(self, key: str) -> str:
        """Retrieve secret from HashiCorp Vault."""
        try:
            import hvac
            client = hvac.Client(url=self.vault_url)
            response = client.secrets.kv.v2.read_secret_version(path=key)
            return response['data']['data'][key]
        except ImportError:
            raise ValueError("hvac is required for HashiCorp Vault")
        except Exception as e:
            raise ValueError(f"Failed to retrieve Vault secret '{key}': {e}")

    def _store_vault_secret(self, key: str, value: str) -> None:
        """Store secret in HashiCorp Vault."""
        try:
            import hvac
            client = hvac.Client(url=self.vault_url)
            client.secrets.kv.v2.create_or_update_secret(path=key, secret={key: value})
        except ImportError:
            raise ValueError("hvac is required for HashiCorp Vault")
        except Exception as e:
            raise ValueError(f"Failed to store Vault secret '{key}': {e}")

    def _get_azure_secret(self, key: str) -> str:
        """Retrieve secret from Azure Key Vault."""
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=self.azure_vault_url, credential=credential)
            secret = client.get_secret(key)
            return secret.value
        except ImportError:
            raise ValueError("azure-keyvault-secrets is required for Azure Key Vault")
        except Exception as e:
            raise ValueError(f"Failed to retrieve Azure secret '{key}': {e}")

    def _store_azure_secret(self, key: str, value: str) -> None:
        """Store secret in Azure Key Vault."""
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=self.azure_vault_url, credential=credential)
            client.set_secret(key, value)
        except ImportError:
            raise ValueError("azure-keyvault-secrets is required for Azure Key Vault")
        except Exception as e:
            raise ValueError(f"Failed to store Azure secret '{key}': {e}")


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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialize components expected by tests
        self.encryption = EncryptionConfig()
        self.secret_manager = SecretManager()
        self._login_attempts: dict[str, list] = {}

        # Add test-expected attributes
        self.jwt_secret_key = kwargs.get('jwt_secret_key') or self.jwt_secret_key
        self.jwt_algorithm = kwargs.get('jwt_algorithm', self.jwt_algorithm)
        self.jwt_expire_minutes = kwargs.get('jwt_expire_minutes', 30)
        self.api_key_length = kwargs.get('api_key_length', 32)
        self.password_min_length = kwargs.get('password_min_length', 8)
        self.password_require_special_chars = kwargs.get('password_require_special_chars', True)
        self.session_timeout_minutes = kwargs.get('session_timeout_minutes', 60)
        self.max_login_attempts = kwargs.get('max_login_attempts', 5)
        self.lockout_duration_minutes = kwargs.get('lockout_duration_minutes', 15)

        # Validate test parameters
        if hasattr(self, 'jwt_expire_minutes') and self.jwt_expire_minutes <= 0:
            raise ValueError("JWT expire minutes must be positive")
        if hasattr(self, 'api_key_length') and self.api_key_length < 16:
            raise ValueError("API key length must be at least 16")
        if hasattr(self, 'password_min_length') and self.password_min_length < 4:
            raise ValueError("Password minimum length must be at least 4")
        if hasattr(self, 'session_timeout_minutes') and self.session_timeout_minutes <= 0:
            raise ValueError("Session timeout must be positive")
        if hasattr(self, 'max_login_attempts') and self.max_login_attempts <= 0:
            raise ValueError("Max login attempts must be positive")
        if hasattr(self, 'lockout_duration_minutes') and self.lockout_duration_minutes <= 0:
            raise ValueError("Lockout duration must be positive")

    def generate_api_key(self) -> str:
        """Generate a new API key."""
        return secrets.token_hex(self.api_key_length)

    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt."""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify a password against its hash."""
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

    def create_jwt_token(self, payload: dict[str, Any]) -> str:
        """Create a JWT token."""
        # Add standard claims
        now = datetime.utcnow()
        payload.update({
            'iat': now,
            'exp': now + timedelta(minutes=self.jwt_expire_minutes)
        })

        return jwt.encode(payload, self.jwt_secret_key, algorithm=self.jwt_algorithm)

    def verify_jwt_token(self, token: str) -> dict[str, Any]:
        """Verify and decode a JWT token."""
        try:
            return jwt.decode(token, self.jwt_secret_key, algorithms=[self.jwt_algorithm])
        except jwt.ExpiredSignatureError:
            raise Exception("JWT token has expired")
        except jwt.InvalidTokenError:
            raise Exception("Invalid JWT token")

    def record_failed_login(self, user_id: str) -> None:
        """Record a failed login attempt."""
        now = datetime.utcnow()

        if user_id not in self._login_attempts:
            self._login_attempts[user_id] = []

        # Add the failed attempt
        self._login_attempts[user_id].append(now)

        # Keep only recent attempts (within lockout duration)
        cutoff = now - timedelta(minutes=self.lockout_duration_minutes)
        self._login_attempts[user_id] = [
            attempt for attempt in self._login_attempts[user_id]
            if attempt > cutoff
        ]

    def is_user_locked(self, user_id: str) -> bool:
        """Check if a user is currently locked out."""
        if user_id not in self._login_attempts:
            return False

        now = datetime.utcnow()
        cutoff = now - timedelta(minutes=self.lockout_duration_minutes)

        # Count recent failed attempts
        recent_attempts = [
            attempt for attempt in self._login_attempts[user_id]
            if attempt > cutoff
        ]

        return len(recent_attempts) >= self.max_login_attempts

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary (excluding sensitive data)."""
        return {
            'encryption': {
                'algorithm': self.encryption.algorithm,
                'key_size': self.encryption.key_size,
                'mode': self.encryption.mode,
                'key_derivation': self.encryption.key_derivation,
                'iterations': self.encryption.iterations,
                'salt_length': self.encryption.salt_length
            },
            'secret_manager': {
                'provider': self.secret_manager.provider,
                'vault_url': self.secret_manager.vault_url,
                'aws_region': self.secret_manager.aws_region,
                'azure_vault_url': self.secret_manager.azure_vault_url
            },
            'jwt_algorithm': self.jwt_algorithm,
            'api_key_length': self.api_key_length,
            'password_min_length': self.password_min_length,
            'password_require_special_chars': self.password_require_special_chars,
            'session_timeout_minutes': self.session_timeout_minutes,
            'max_login_attempts': self.max_login_attempts,
            'lockout_duration_minutes': self.lockout_duration_minutes,
            'auth_enabled': self.auth_enabled,
            'https_enabled': self.https_enabled,
            'rate_limiting_enabled': self.rate_limiting_enabled
        }

    def log_security_event(self, event_type: str, details: dict[str, Any]) -> None:
        """Log security events."""
        log_message = f"Security event: {event_type} - {details}"

        if event_type in ['login_failure', 'unauthorized_access']:
            if event_type == 'unauthorized_access':
                logger.error(log_message)
            else:
                logger.warning(log_message)
        else:
            logger.info(log_message)
