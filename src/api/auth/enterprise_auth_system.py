"""Enterprise Authentication & Authorization System
Advanced OAuth2/OIDC, JWT management, and RBAC implementation
"""

from __future__ import annotations

import asyncio
import json
import secrets
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from urllib.parse import urlencode

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from passlib.context import CryptContext

from core.caching.cache_patterns import get_cache_manager
from core.config import get_settings
from core.logging import get_logger
from core.structured_logging import get_structured_logger
from messaging.rabbitmq_manager import RabbitMQManager

logger = get_logger(__name__)
structured_logger = get_structured_logger(__name__)
settings = get_settings()


class AuthenticationMethod(Enum):
    """Supported authentication methods"""

    PASSWORD = "password"
    API_KEY = "api_key"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    OIDC = "oidc"
    SAML = "saml"
    LDAP = "ldap"
    MFA_TOTP = "mfa_totp"
    MFA_SMS = "mfa_sms"
    BIOMETRIC = "biometric"


class AuthorizationLevel(Enum):
    """Authorization levels"""

    PUBLIC = 0
    AUTHENTICATED = 1
    VERIFIED = 2
    PRIVILEGED = 3
    ADMINISTRATIVE = 4
    SYSTEM = 5


class TokenType(Enum):
    """Token types"""

    ACCESS = "access"
    REFRESH = "refresh"
    ID = "id"
    AUTHORIZATION_CODE = "authorization_code"
    DEVICE_CODE = "device_code"
    API_KEY = "api_key"


@dataclass
class Permission:
    """Permission definition"""

    name: str
    resource: str
    action: str
    conditions: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        return f"{self.resource}:{self.action}"

    def check_conditions(self, context: dict[str, Any]) -> bool:
        """Check if permission conditions are met"""
        if not self.conditions:
            return True

        for condition, expected_value in self.conditions.items():
            if context.get(condition) != expected_value:
                return False

        return True


@dataclass
class Role:
    """Role definition with hierarchical support"""

    name: str
    description: str
    permissions: list[Permission]
    parent_roles: list[str] = field(default_factory=list)
    authorization_level: AuthorizationLevel = AuthorizationLevel.AUTHENTICATED
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def get_all_permissions(self, role_resolver: Callable[[str], Role | None] = None) -> set[str]:
        """Get all permissions including inherited from parent roles"""
        all_permissions = {perm.full_name for perm in self.permissions}

        if role_resolver:
            for parent_role_name in self.parent_roles:
                parent_role = role_resolver(parent_role_name)
                if parent_role:
                    all_permissions.update(parent_role.get_all_permissions(role_resolver))

        return all_permissions


@dataclass
class User:
    """User entity with comprehensive profile"""

    user_id: str
    username: str
    email: str
    roles: list[str]
    is_active: bool = True
    is_verified: bool = False
    mfa_enabled: bool = False
    mfa_secret: str | None = None
    password_hash: str | None = None
    failed_login_attempts: int = 0
    locked_until: datetime | None = None
    last_login: datetime | None = None
    session_data: dict[str, Any] = field(default_factory=dict)
    profile: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_locked(self) -> bool:
        """Check if user account is locked"""
        return self.locked_until and self.locked_until > datetime.utcnow()

    def increment_failed_login(self, max_attempts: int = 5, lockout_duration: int = 900):
        """Increment failed login attempts and lock if necessary"""
        self.failed_login_attempts += 1

        if self.failed_login_attempts >= max_attempts:
            self.locked_until = datetime.utcnow() + timedelta(seconds=lockout_duration)

    def reset_failed_login(self):
        """Reset failed login attempts"""
        self.failed_login_attempts = 0
        self.locked_until = None


@dataclass
class AuthenticationResult:
    """Result of authentication attempt"""

    success: bool
    user: User | None = None
    tokens: dict[str, str] | None = None
    mfa_required: bool = False
    mfa_methods: list[str] = field(default_factory=list)
    error_message: str | None = None
    additional_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthorizationResult:
    """Result of authorization check"""

    authorized: bool
    required_permissions: list[str]
    granted_permissions: list[str]
    missing_permissions: list[str] = field(default_factory=list)
    context_data: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None


class PasswordManager:
    """Advanced password management with multiple hashing algorithms"""

    def __init__(self):
        self.pwd_context = CryptContext(
            schemes=["bcrypt", "argon2", "scrypt"],
            default="bcrypt",
            bcrypt__rounds=12,
            argon2__memory_cost=65536,
            argon2__time_cost=3,
            argon2__parallelism=1,
        )

    def hash_password(self, password: str) -> str:
        """Hash password using the default scheme"""
        return self.pwd_context.hash(password)

    def verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash"""
        try:
            return self.pwd_context.verify(password, password_hash)
        except Exception as e:
            logger.error(f"Password verification error: {e}")
            return False

    def needs_rehash(self, password_hash: str) -> bool:
        """Check if password hash needs to be updated"""
        return self.pwd_context.needs_update(password_hash)

    def generate_secure_password(self, length: int = 16) -> str:
        """Generate cryptographically secure password"""
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def check_password_strength(self, password: str) -> dict[str, Any]:
        """Check password strength and provide recommendations"""
        score = 0
        feedback = []

        if len(password) >= 8:
            score += 1
        else:
            feedback.append("Password should be at least 8 characters long")

        if any(c.islower() for c in password):
            score += 1
        else:
            feedback.append("Password should contain lowercase letters")

        if any(c.isupper() for c in password):
            score += 1
        else:
            feedback.append("Password should contain uppercase letters")

        if any(c.isdigit() for c in password):
            score += 1
        else:
            feedback.append("Password should contain numbers")

        if any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            score += 1
        else:
            feedback.append("Password should contain special characters")

        strength_levels = {
            0: "Very Weak",
            1: "Weak",
            2: "Fair",
            3: "Good",
            4: "Strong",
            5: "Very Strong",
        }

        return {
            "score": score,
            "max_score": 5,
            "strength": strength_levels.get(score, "Unknown"),
            "feedback": feedback,
        }


class JWTManager:
    """Advanced JWT token management with key rotation"""

    def __init__(self, secret_key: str | None = None):
        self.secret_key = secret_key or settings.secret_key or secrets.token_urlsafe(32)
        self.algorithm = "HS256"
        self.access_token_expire_minutes = 30
        self.refresh_token_expire_days = 30
        self.key_rotation_interval = timedelta(days=30)
        self.last_key_rotation = datetime.utcnow()

        # For RS256 support
        self.private_key = None
        self.public_key = None
        self._init_rsa_keys()

    def _init_rsa_keys(self):
        """Initialize RSA key pair for RS256 signing"""
        try:
            private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

            self.private_key = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            public_key = private_key.public_key()
            self.public_key = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )

        except Exception as e:
            logger.warning(f"Failed to generate RSA keys: {e}")

    def create_access_token(
        self, user_id: str, user_data: dict[str, Any], expires_delta: timedelta | None = None
    ) -> str:
        """Create JWT access token"""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)

        to_encode = {
            "sub": user_id,
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": TokenType.ACCESS.value,
            "jti": secrets.token_urlsafe(16),  # JWT ID for tracking
            **user_data,
        }

        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)

    def create_refresh_token(
        self, user_id: str, access_token_jti: str, expires_delta: timedelta | None = None
    ) -> str:
        """Create JWT refresh token"""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)

        to_encode = {
            "sub": user_id,
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": TokenType.REFRESH.value,
            "jti": secrets.token_urlsafe(16),
            "access_jti": access_token_jti,  # Link to access token
        }

        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)

    def verify_token(self, token: str, token_type: TokenType = None) -> dict[str, Any] | None:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])

            # Check token type if specified
            if token_type and payload.get("type") != token_type.value:
                return None

            # Check expiration
            if datetime.fromtimestamp(payload["exp"]) < datetime.utcnow():
                return None

            return payload

        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None
        except Exception as e:
            logger.error(f"Token verification error: {e}")
            return None

    def refresh_access_token(self, refresh_token: str) -> dict[str, str] | None:
        """Create new access token from refresh token"""
        refresh_payload = self.verify_token(refresh_token, TokenType.REFRESH)

        if not refresh_payload:
            return None

        user_id = refresh_payload["sub"]

        # Create new access token
        access_token = self.create_access_token(user_id, {})

        # Decode to get JTI for new refresh token
        access_payload = self.verify_token(access_token)
        if not access_payload:
            return None

        # Create new refresh token
        new_refresh_token = self.create_refresh_token(user_id, access_payload["jti"])

        return {
            "access_token": access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer",
        }

    def revoke_token(self, jti: str) -> bool:
        """Add token to revocation list"""
        # In production, store revoked tokens in Redis/database
        # For now, we'll use cache manager
        cache_manager = get_cache_manager()
        asyncio.create_task(cache_manager.set(f"revoked_token:{jti}", "true", ttl=86400))
        return True

    async def is_token_revoked(self, jti: str) -> bool:
        """Check if token is revoked"""
        cache_manager = get_cache_manager()
        revoked = await cache_manager.get(f"revoked_token:{jti}")
        return revoked == "true"

    def needs_key_rotation(self) -> bool:
        """Check if keys need rotation"""
        return datetime.utcnow() - self.last_key_rotation > self.key_rotation_interval

    def rotate_keys(self):
        """Rotate signing keys"""
        self.secret_key = secrets.token_urlsafe(32)
        self._init_rsa_keys()
        self.last_key_rotation = datetime.utcnow()
        logger.info("JWT signing keys rotated")


class APIKeyManager:
    """API key management system"""

    def __init__(self):
        self.cache_manager = get_cache_manager()
        self.key_prefix = "pwc_"

    def generate_api_key(
        self, user_id: str, name: str, permissions: list[str], expires_at: datetime | None = None
    ) -> dict[str, Any]:
        """Generate new API key"""
        key_id = secrets.token_urlsafe(16)
        api_key = f"{self.key_prefix}{secrets.token_urlsafe(32)}"

        key_data = {
            "key_id": key_id,
            "user_id": user_id,
            "name": name,
            "permissions": permissions,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": expires_at.isoformat() if expires_at else None,
            "is_active": True,
            "usage_count": 0,
            "last_used": None,
        }

        # Store API key data (in production, use database)
        asyncio.create_task(
            self.cache_manager.set(
                f"api_key:{api_key}",
                json.dumps(key_data),
                ttl=86400 * 365 if not expires_at else None,  # 1 year or until expiry
            )
        )

        return {"api_key": api_key, "key_id": key_id, "created_at": key_data["created_at"]}

    async def validate_api_key(self, api_key: str) -> dict[str, Any] | None:
        """Validate API key and return key data"""
        if not api_key.startswith(self.key_prefix):
            return None

        key_data_str = await self.cache_manager.get(f"api_key:{api_key}")
        if not key_data_str:
            return None

        try:
            key_data = json.loads(key_data_str)

            # Check if key is active
            if not key_data.get("is_active", False):
                return None

            # Check expiration
            if key_data.get("expires_at"):
                expires_at = datetime.fromisoformat(key_data["expires_at"])
                if expires_at < datetime.utcnow():
                    return None

            # Update usage statistics
            key_data["usage_count"] = key_data.get("usage_count", 0) + 1
            key_data["last_used"] = datetime.utcnow().isoformat()

            # Update cached data
            await self.cache_manager.set(f"api_key:{api_key}", json.dumps(key_data))

            return key_data

        except Exception as e:
            logger.error(f"API key validation error: {e}")
            return None

    async def revoke_api_key(self, api_key: str) -> bool:
        """Revoke API key"""
        key_data_str = await self.cache_manager.get(f"api_key:{api_key}")
        if not key_data_str:
            return False

        try:
            key_data = json.loads(key_data_str)
            key_data["is_active"] = False
            key_data["revoked_at"] = datetime.utcnow().isoformat()

            await self.cache_manager.set(f"api_key:{api_key}", json.dumps(key_data))

            return True

        except Exception as e:
            logger.error(f"API key revocation error: {e}")
            return False


class MFAManager:
    """Multi-Factor Authentication manager"""

    def __init__(self):
        self.cache_manager = get_cache_manager()

    def generate_totp_secret(self, user_id: str) -> str:
        """Generate TOTP secret for user"""
        secret = secrets.token_urlsafe(20)

        # Store secret (in production, encrypt and store in database)
        asyncio.create_task(
            self.cache_manager.set(
                f"mfa_secret:{user_id}",
                secret,
                ttl=3600,  # 1 hour to set up
            )
        )

        return secret

    def generate_totp_qr_code(self, user_id: str, secret: str, issuer: str = "PwC API") -> str:
        """Generate TOTP QR code URL"""
        import urllib.parse

        params = {
            "secret": secret,
            "issuer": issuer,
            "algorithm": "SHA1",
            "digits": "6",
            "period": "30",
        }

        query_string = urllib.parse.urlencode(params)
        return f"otpauth://totp/{issuer}:{user_id}?{query_string}"

    def verify_totp_code(self, secret: str, code: str, window: int = 1) -> bool:
        """Verify TOTP code with time window"""
        import pyotp

        try:
            totp = pyotp.TOTP(secret)
            return totp.verify(code, valid_window=window)
        except Exception as e:
            logger.error(f"TOTP verification error: {e}")
            return False

    async def generate_backup_codes(self, user_id: str, count: int = 10) -> list[str]:
        """Generate backup codes for MFA"""
        codes = [secrets.token_urlsafe(8) for _ in range(count)]

        # Hash codes before storing
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        hashed_codes = [pwd_context.hash(code) for code in codes]

        await self.cache_manager.set(
            f"backup_codes:{user_id}",
            json.dumps(hashed_codes),
            ttl=86400 * 365,  # 1 year
        )

        return codes

    async def verify_backup_code(self, user_id: str, code: str) -> bool:
        """Verify backup code and mark as used"""
        codes_str = await self.cache_manager.get(f"backup_codes:{user_id}")
        if not codes_str:
            return False

        try:
            hashed_codes = json.loads(codes_str)
            pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

            for i, hashed_code in enumerate(hashed_codes):
                if hashed_code and pwd_context.verify(code, hashed_code):
                    # Mark code as used
                    hashed_codes[i] = None

                    await self.cache_manager.set(
                        f"backup_codes:{user_id}", json.dumps(hashed_codes)
                    )

                    return True

            return False

        except Exception as e:
            logger.error(f"Backup code verification error: {e}")
            return False


class RBACManager:
    """Role-Based Access Control manager"""

    def __init__(self):
        self.roles: dict[str, Role] = {}
        self.permissions: dict[str, Permission] = {}
        self.cache_manager = get_cache_manager()

        # Initialize default roles and permissions
        self._initialize_default_roles()

    def _initialize_default_roles(self):
        """Initialize default roles and permissions"""
        # Define default permissions
        default_permissions = [
            Permission("read_sales", "sales", "read"),
            Permission("write_sales", "sales", "write"),
            Permission("read_analytics", "analytics", "read"),
            Permission("write_analytics", "analytics", "write"),
            Permission("admin_users", "users", "admin"),
            Permission("admin_system", "system", "admin"),
        ]

        for perm in default_permissions:
            self.permissions[perm.full_name] = perm

        # Define default roles
        self.roles["viewer"] = Role(
            name="viewer",
            description="Read-only access to sales data",
            permissions=[self.permissions["sales:read"]],
            authorization_level=AuthorizationLevel.AUTHENTICATED,
        )

        self.roles["analyst"] = Role(
            name="analyst",
            description="Access to sales and analytics data",
            permissions=[self.permissions["sales:read"], self.permissions["analytics:read"]],
            authorization_level=AuthorizationLevel.VERIFIED,
        )

        self.roles["manager"] = Role(
            name="manager",
            description="Full access to sales and analytics",
            permissions=[
                self.permissions["sales:read"],
                self.permissions["sales:write"],
                self.permissions["analytics:read"],
                self.permissions["analytics:write"],
            ],
            parent_roles=["analyst"],
            authorization_level=AuthorizationLevel.PRIVILEGED,
        )

        self.roles["admin"] = Role(
            name="admin",
            description="Administrative access",
            permissions=list(self.permissions.values()),
            parent_roles=["manager"],
            authorization_level=AuthorizationLevel.ADMINISTRATIVE,
        )

    def create_role(self, role: Role) -> bool:
        """Create new role"""
        try:
            self.roles[role.name] = role
            logger.info(f"Created role: {role.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create role {role.name}: {e}")
            return False

    def create_permission(self, permission: Permission) -> bool:
        """Create new permission"""
        try:
            self.permissions[permission.full_name] = permission
            logger.info(f"Created permission: {permission.full_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create permission {permission.full_name}: {e}")
            return False

    def get_role(self, role_name: str) -> Role | None:
        """Get role by name"""
        return self.roles.get(role_name)

    def get_user_permissions(self, user_roles: list[str]) -> set[str]:
        """Get all permissions for user based on roles"""
        all_permissions = set()

        for role_name in user_roles:
            role = self.roles.get(role_name)
            if role:
                role_permissions = role.get_all_permissions(self.get_role)
                all_permissions.update(role_permissions)

        return all_permissions

    def check_permission(
        self, user_roles: list[str], required_permission: str, context: dict[str, Any] = None
    ) -> bool:
        """Check if user has required permission"""
        user_permissions = self.get_user_permissions(user_roles)

        if required_permission not in user_permissions:
            return False

        # Check permission conditions if context provided
        if context:
            permission = self.permissions.get(required_permission)
            if permission and not permission.check_conditions(context):
                return False

        return True

    async def authorize_request(
        self, user_roles: list[str], required_permissions: list[str], context: dict[str, Any] = None
    ) -> AuthorizationResult:
        """Comprehensive authorization check"""
        context = context or {}
        self.get_user_permissions(user_roles)
        granted_permissions = []
        missing_permissions = []

        for perm in required_permissions:
            if self.check_permission(user_roles, perm, context):
                granted_permissions.append(perm)
            else:
                missing_permissions.append(perm)

        authorized = len(missing_permissions) == 0

        return AuthorizationResult(
            authorized=authorized,
            required_permissions=required_permissions,
            granted_permissions=granted_permissions,
            missing_permissions=missing_permissions,
            context_data=context,
        )


class OAuth2Manager:
    """OAuth2/OIDC implementation"""

    def __init__(self):
        self.authorization_codes: dict[str, dict[str, Any]] = {}
        self.jwt_manager = JWTManager()
        self.cache_manager = get_cache_manager()

    def generate_authorization_url(
        self,
        client_id: str,
        redirect_uri: str,
        scope: str = "openid profile",
        state: str | None = None,
    ) -> str:
        """Generate OAuth2 authorization URL"""
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scope,
        }

        if state:
            params["state"] = state

        auth_url = f"/oauth/authorize?{urlencode(params)}"
        return auth_url

    def generate_authorization_code(
        self, client_id: str, user_id: str, redirect_uri: str, scope: str
    ) -> str:
        """Generate authorization code"""
        code = secrets.token_urlsafe(32)

        code_data = {
            "client_id": client_id,
            "user_id": user_id,
            "redirect_uri": redirect_uri,
            "scope": scope,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(minutes=10)).isoformat(),
            "used": False,
        }

        self.authorization_codes[code] = code_data

        # Store in cache with TTL
        asyncio.create_task(
            self.cache_manager.set(
                f"auth_code:{code}",
                json.dumps(code_data),
                ttl=600,  # 10 minutes
            )
        )

        return code

    async def exchange_code_for_tokens(
        self, code: str, client_id: str, redirect_uri: str
    ) -> dict[str, str] | None:
        """Exchange authorization code for tokens"""
        code_data_str = await self.cache_manager.get(f"auth_code:{code}")
        if not code_data_str:
            return None

        try:
            code_data = json.loads(code_data_str)

            # Validate code
            if (
                code_data.get("used")
                or code_data.get("client_id") != client_id
                or code_data.get("redirect_uri") != redirect_uri
            ):
                return None

            # Check expiration
            expires_at = datetime.fromisoformat(code_data["expires_at"])
            if expires_at < datetime.utcnow():
                return None

            # Mark code as used
            code_data["used"] = True
            await self.cache_manager.set(f"auth_code:{code}", json.dumps(code_data))

            # Generate tokens
            user_id = code_data["user_id"]
            scope = code_data["scope"]

            access_token = self.jwt_manager.create_access_token(
                user_id, {"scope": scope, "client_id": client_id}
            )

            access_payload = self.jwt_manager.verify_token(access_token)
            refresh_token = self.jwt_manager.create_refresh_token(user_id, access_payload["jti"])

            tokens = {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "Bearer",
                "expires_in": 1800,  # 30 minutes
                "scope": scope,
            }

            # Generate ID token if OpenID Connect
            if "openid" in scope:
                id_token = self._generate_id_token(user_id, client_id)
                tokens["id_token"] = id_token

            return tokens

        except Exception as e:
            logger.error(f"Token exchange error: {e}")
            return None

    def _generate_id_token(self, user_id: str, client_id: str) -> str:
        """Generate OpenID Connect ID token"""
        claims = {
            "iss": "https://api.pwc.com",  # Issuer
            "sub": user_id,
            "aud": client_id,
            "exp": int((datetime.utcnow() + timedelta(hours=1)).timestamp()),
            "iat": int(datetime.utcnow().timestamp()),
            "auth_time": int(datetime.utcnow().timestamp()),
        }

        return jwt.encode(claims, self.jwt_manager.secret_key, algorithm="HS256")


class EnterpriseAuthSystem:
    """Main enterprise authentication and authorization system"""

    def __init__(self):
        self.password_manager = PasswordManager()
        self.jwt_manager = JWTManager()
        self.api_key_manager = APIKeyManager()
        self.mfa_manager = MFAManager()
        self.rbac_manager = RBACManager()
        self.oauth2_manager = OAuth2Manager()
        self.cache_manager = get_cache_manager()
        self.rabbitmq_manager = RabbitMQManager()

        # User storage (in production, use database)
        self.users: dict[str, User] = {}

        # Rate limiting and security monitoring
        self.login_attempts: dict[str, list[datetime]] = {}
        self.suspicious_activities: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.blocked_ips: set[str] = set()
        self.device_fingerprints: dict[str, dict[str, Any]] = {}

        # Session management
        self.active_sessions: dict[str, dict[str, Any]] = {}
        self.session_timeout = timedelta(hours=24)

        # Security policies
        self.password_policy = {
            "min_length": 12,
            "require_uppercase": True,
            "require_lowercase": True,
            "require_numbers": True,
            "require_special": True,
            "password_history": 5,
            "max_age_days": 90,
        }

        self.account_lockout_policy = {
            "max_failed_attempts": 5,
            "lockout_duration_minutes": 30,
            "reset_attempts_after_minutes": 60,
        }

        # Audit logging
        self.audit_events: list[dict[str, Any]] = []

        # Initialize test users and standard roles
        self._create_test_users()

        # Start background security tasks
        asyncio.create_task(self._start_security_background_tasks())

    def _create_test_users(self):
        """Create test users for development and testing"""
        # Admin user
        admin_user = User(
            user_id="admin_001",
            username="admin",
            email="admin@pwc.com",
            roles=["admin"],
            is_active=True,
            is_verified=True,
            password_hash=self.password_manager.hash_password("SecureAdmin123!"),
        )

        # Analyst user
        analyst_user = User(
            user_id="analyst_001",
            username="analyst",
            email="analyst@pwc.com",
            roles=["analyst"],
            is_active=True,
            is_verified=True,
            mfa_enabled=True,
            password_hash=self.password_manager.hash_password("SecureAnalyst123!"),
        )

        # Manager user
        manager_user = User(
            user_id="manager_001",
            username="manager",
            email="manager@pwc.com",
            roles=["manager"],
            is_active=True,
            is_verified=True,
            password_hash=self.password_manager.hash_password("SecureManager123!"),
        )

        # Viewer user
        viewer_user = User(
            user_id="viewer_001",
            username="viewer",
            email="viewer@pwc.com",
            roles=["viewer"],
            is_active=True,
            is_verified=True,
            password_hash=self.password_manager.hash_password("SecureViewer123!"),
        )

        self.users[admin_user.user_id] = admin_user
        self.users[analyst_user.user_id] = analyst_user
        self.users[manager_user.user_id] = manager_user
        self.users[viewer_user.user_id] = viewer_user

        logger.info("Test users created with enhanced security")

    async def _start_security_background_tasks(self):
        """Start background security monitoring tasks"""
        await asyncio.gather(
            self._cleanup_expired_sessions(),
            self._monitor_suspicious_activity(),
            self._rotate_jwt_keys(),
            self._audit_cleanup(),
            return_exceptions=True,
        )

    async def _cleanup_expired_sessions(self):
        """Clean up expired sessions periodically"""
        while True:
            try:
                current_time = datetime.utcnow()
                expired_sessions = []

                for session_id, session_data in self.active_sessions.items():
                    session_created = session_data.get("created_at")
                    if isinstance(session_created, str):
                        session_created = datetime.fromisoformat(session_created)

                    if current_time - session_created > self.session_timeout:
                        expired_sessions.append(session_id)

                for session_id in expired_sessions:
                    del self.active_sessions[session_id]
                    await self._log_security_event(
                        "session_expired",
                        {"session_id": session_id, "cleanup_time": current_time.isoformat()},
                    )

                if expired_sessions:
                    logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")

                await asyncio.sleep(3600)  # Run every hour

            except Exception as e:
                logger.error(f"Session cleanup error: {e}")
                await asyncio.sleep(3600)

    async def _monitor_suspicious_activity(self):
        """Monitor and respond to suspicious activities"""
        while True:
            try:
                current_time = datetime.utcnow()

                # Check for brute force attacks
                for ip, activities in self.suspicious_activities.items():
                    recent_failures = [
                        activity
                        for activity in activities
                        if current_time - datetime.fromisoformat(activity["timestamp"])
                        < timedelta(minutes=15)
                        and activity["type"] == "failed_login"
                    ]

                    if len(recent_failures) >= 10:  # 10 failures in 15 minutes
                        self.blocked_ips.add(ip)
                        await self._log_security_event(
                            "ip_blocked",
                            {
                                "ip_address": ip,
                                "failure_count": len(recent_failures),
                                "blocked_at": current_time.isoformat(),
                            },
                        )
                        logger.warning(f"Blocked suspicious IP: {ip}")

                await asyncio.sleep(300)  # Check every 5 minutes

            except Exception as e:
                logger.error(f"Security monitoring error: {e}")
                await asyncio.sleep(300)

    async def _rotate_jwt_keys(self):
        """Periodically rotate JWT signing keys"""
        while True:
            try:
                if self.jwt_manager.needs_key_rotation():
                    old_key_id = id(self.jwt_manager.secret_key)
                    self.jwt_manager.rotate_keys()

                    await self._log_security_event(
                        "jwt_key_rotation",
                        {
                            "old_key_id": str(old_key_id),
                            "new_key_id": str(id(self.jwt_manager.secret_key)),
                            "rotated_at": datetime.utcnow().isoformat(),
                        },
                    )

                    logger.info("JWT signing keys rotated for security")

                await asyncio.sleep(86400)  # Check daily

            except Exception as e:
                logger.error(f"JWT key rotation error: {e}")
                await asyncio.sleep(86400)

    async def _audit_cleanup(self):
        """Clean up old audit events"""
        while True:
            try:
                cutoff_time = datetime.utcnow() - timedelta(days=90)  # Keep 90 days

                original_count = len(self.audit_events)
                self.audit_events = [
                    event
                    for event in self.audit_events
                    if datetime.fromisoformat(event["timestamp"]) > cutoff_time
                ]

                cleaned_count = original_count - len(self.audit_events)
                if cleaned_count > 0:
                    logger.info(f"Cleaned up {cleaned_count} old audit events")

                await asyncio.sleep(86400)  # Run daily

            except Exception as e:
                logger.error(f"Audit cleanup error: {e}")
                await asyncio.sleep(86400)

    async def _log_security_event(self, event_type: str, event_data: dict[str, Any]):
        """Log security event for audit trail"""
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "data": event_data,
        }

        self.audit_events.append(event)

        # Also publish to message queue for external monitoring
        try:
            await self._publish_auth_event(f"security.{event_type}", event)
        except Exception as e:
            logger.error(f"Failed to publish security event: {e}")

    def check_password_policy(self, password: str, user_id: str | None = None) -> dict[str, Any]:
        """Check password against security policy"""
        result = self.password_manager.check_password_strength(password)
        policy_violations = []

        # Check minimum length
        if len(password) < self.password_policy["min_length"]:
            policy_violations.append(
                f"Password must be at least {self.password_policy['min_length']} characters"
            )

        # Check character requirements
        if self.password_policy["require_uppercase"] and not any(c.isupper() for c in password):
            policy_violations.append("Password must contain uppercase letters")

        if self.password_policy["require_lowercase"] and not any(c.islower() for c in password):
            policy_violations.append("Password must contain lowercase letters")

        if self.password_policy["require_numbers"] and not any(c.isdigit() for c in password):
            policy_violations.append("Password must contain numbers")

        if self.password_policy["require_special"] and not any(
            c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password
        ):
            policy_violations.append("Password must contain special characters")

        return {
            "compliant": len(policy_violations) == 0,
            "violations": policy_violations,
            "strength_score": result["score"],
            "strength_level": result["strength"],
        }

    def is_ip_blocked(self, ip_address: str) -> bool:
        """Check if IP address is blocked"""
        return ip_address in self.blocked_ips

    def record_suspicious_activity(
        self, ip_address: str, activity_type: str, details: dict[str, Any]
    ):
        """Record suspicious activity"""
        activity = {
            "type": activity_type,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details,
        }

        self.suspicious_activities[ip_address].append(activity)

        # Keep only recent activities (last 24 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        self.suspicious_activities[ip_address] = [
            act
            for act in self.suspicious_activities[ip_address]
            if datetime.fromisoformat(act["timestamp"]) > cutoff_time
        ]

    async def create_user_session(
        self, user_id: str, device_fingerprint: str | None = None, ip_address: str | None = None
    ) -> str:
        """Create secure user session"""
        session_id = str(uuid.uuid4())

        session_data = {
            "session_id": session_id,
            "user_id": user_id,
            "created_at": datetime.utcnow().isoformat(),
            "last_accessed": datetime.utcnow().isoformat(),
            "device_fingerprint": device_fingerprint,
            "ip_address": ip_address,
            "is_active": True,
        }

        self.active_sessions[session_id] = session_data

        # Track device if provided
        if device_fingerprint:
            if device_fingerprint not in self.device_fingerprints:
                self.device_fingerprints[device_fingerprint] = {
                    "first_seen": datetime.utcnow().isoformat(),
                    "user_ids": set(),
                    "session_count": 0,
                }

            self.device_fingerprints[device_fingerprint]["user_ids"].add(user_id)
            self.device_fingerprints[device_fingerprint]["session_count"] += 1
            self.device_fingerprints[device_fingerprint]["last_seen"] = (
                datetime.utcnow().isoformat()
            )

        await self._log_security_event(
            "session_created",
            {
                "session_id": session_id,
                "user_id": user_id,
                "ip_address": ip_address,
                "device_fingerprint": device_fingerprint,
            },
        )

        return session_id

    async def validate_session(self, session_id: str) -> dict[str, Any] | None:
        """Validate and refresh user session"""
        if session_id not in self.active_sessions:
            return None

        session = self.active_sessions[session_id]
        created_at = datetime.fromisoformat(session["created_at"])

        # Check if session has expired
        if datetime.utcnow() - created_at > self.session_timeout:
            del self.active_sessions[session_id]
            await self._log_security_event("session_expired", {"session_id": session_id})
            return None

        # Update last accessed time
        session["last_accessed"] = datetime.utcnow().isoformat()

        return session

    async def revoke_session(self, session_id: str) -> bool:
        """Revoke user session"""
        if session_id in self.active_sessions:
            user_id = self.active_sessions[session_id].get("user_id")
            del self.active_sessions[session_id]

            await self._log_security_event(
                "session_revoked", {"session_id": session_id, "user_id": user_id}
            )

            return True

        return False

    async def revoke_all_user_sessions(self, user_id: str) -> int:
        """Revoke all sessions for a user"""
        revoked_count = 0
        sessions_to_revoke = []

        for session_id, session_data in self.active_sessions.items():
            if session_data.get("user_id") == user_id:
                sessions_to_revoke.append(session_id)

        for session_id in sessions_to_revoke:
            await self.revoke_session(session_id)
            revoked_count += 1

        if revoked_count > 0:
            await self._log_security_event(
                "all_user_sessions_revoked", {"user_id": user_id, "revoked_count": revoked_count}
            )

        return revoked_count

    async def authenticate_user(
        self,
        username: str,
        password: str,
        method: AuthenticationMethod = AuthenticationMethod.PASSWORD,
        mfa_code: str | None = None,
    ) -> AuthenticationResult:
        """Authenticate user with various methods"""

        # Find user
        user = None
        for u in self.users.values():
            if u.username == username or u.email == username:
                user = u
                break

        if not user:
            return AuthenticationResult(success=False, error_message="Invalid credentials")

        # Check if user is locked
        if user.is_locked:
            return AuthenticationResult(
                success=False,
                error_message="Account is temporarily locked due to failed login attempts",
            )

        # Check if user is active
        if not user.is_active:
            return AuthenticationResult(success=False, error_message="Account is disabled")

        # Verify password
        if method == AuthenticationMethod.PASSWORD:
            if not user.password_hash or not self.password_manager.verify_password(
                password, user.password_hash
            ):
                user.increment_failed_login()
                return AuthenticationResult(success=False, error_message="Invalid credentials")

        # Check MFA if enabled
        if user.mfa_enabled:
            if not mfa_code:
                return AuthenticationResult(
                    success=False, mfa_required=True, mfa_methods=["totp", "backup_code"]
                )

            # Verify MFA code
            mfa_valid = False
            if user.mfa_secret:
                mfa_valid = self.mfa_manager.verify_totp_code(user.mfa_secret, mfa_code)

            if not mfa_valid:
                mfa_valid = await self.mfa_manager.verify_backup_code(user.user_id, mfa_code)

            if not mfa_valid:
                user.increment_failed_login()
                return AuthenticationResult(success=False, error_message="Invalid MFA code")

        # Successful authentication
        user.reset_failed_login()
        user.last_login = datetime.utcnow()

        # Generate tokens
        user_data = {
            "username": user.username,
            "email": user.email,
            "roles": user.roles,
            "is_verified": user.is_verified,
        }

        access_token = self.jwt_manager.create_access_token(user.user_id, user_data)
        access_payload = self.jwt_manager.verify_token(access_token)
        refresh_token = self.jwt_manager.create_refresh_token(user.user_id, access_payload["jti"])

        tokens = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
        }

        # Publish authentication event
        await self._publish_auth_event(
            "user_authenticated",
            {
                "user_id": user.user_id,
                "username": user.username,
                "method": method.value,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

        return AuthenticationResult(success=True, user=user, tokens=tokens)

    async def authenticate_api_key(self, api_key: str) -> AuthenticationResult:
        """Authenticate using API key"""
        key_data = await self.api_key_manager.validate_api_key(api_key)

        if not key_data:
            return AuthenticationResult(success=False, error_message="Invalid API key")

        user_id = key_data["user_id"]
        user = self.users.get(user_id)

        if not user or not user.is_active:
            return AuthenticationResult(
                success=False, error_message="User account not found or inactive"
            )

        return AuthenticationResult(
            success=True,
            user=user,
            additional_data={
                "api_key_info": {
                    "key_id": key_data["key_id"],
                    "name": key_data["name"],
                    "permissions": key_data["permissions"],
                    "usage_count": key_data["usage_count"],
                }
            },
        )

    async def authenticate_jwt(self, token: str) -> AuthenticationResult:
        """Authenticate using JWT token"""
        payload = self.jwt_manager.verify_token(token, TokenType.ACCESS)

        if not payload:
            return AuthenticationResult(success=False, error_message="Invalid or expired token")

        # Check if token is revoked
        if await self.jwt_manager.is_token_revoked(payload["jti"]):
            return AuthenticationResult(success=False, error_message="Token has been revoked")

        user_id = payload["sub"]
        user = self.users.get(user_id)

        if not user or not user.is_active:
            return AuthenticationResult(
                success=False, error_message="User account not found or inactive"
            )

        return AuthenticationResult(
            success=True, user=user, additional_data={"token_claims": payload}
        )

    async def authorize_request(
        self, user: User, required_permissions: list[str], context: dict[str, Any] = None
    ) -> AuthorizationResult:
        """Authorize user request"""
        return await self.rbac_manager.authorize_request(user.roles, required_permissions, context)

    async def create_user(
        self, username: str, email: str, password: str, roles: list[str] = None
    ) -> User:
        """Create new user"""
        user_id = f"user_{secrets.token_urlsafe(12)}"

        user = User(
            user_id=user_id,
            username=username,
            email=email,
            roles=roles or ["viewer"],
            password_hash=self.password_manager.hash_password(password),
        )

        self.users[user_id] = user

        # Publish user creation event
        await self._publish_auth_event(
            "user_created",
            {"user_id": user_id, "username": username, "email": email, "roles": user.roles},
        )

        return user

    async def _publish_auth_event(self, event_type: str, data: dict[str, Any]):
        """Publish authentication event to message queue"""
        try:
            self.rabbitmq_manager.publish_event(
                event_type=event_type, data=data, routing_key=f"auth.{event_type}"
            )
        except Exception as e:
            logger.error(f"Failed to publish auth event: {e}")

    async def get_system_stats(self) -> dict[str, Any]:
        """Get authentication system statistics"""
        return {
            "total_users": len(self.users),
            "active_users": len([u for u in self.users.values() if u.is_active]),
            "verified_users": len([u for u in self.users.values() if u.is_verified]),
            "mfa_enabled_users": len([u for u in self.users.values() if u.mfa_enabled]),
            "total_roles": len(self.rbac_manager.roles),
            "total_permissions": len(self.rbac_manager.permissions),
            "jwt_key_rotation_needed": self.jwt_manager.needs_key_rotation(),
            "timestamp": datetime.utcnow().isoformat(),
        }


# Global auth system instance
_auth_system: EnterpriseAuthSystem | None = None


def get_auth_system() -> EnterpriseAuthSystem:
    """Get or create global auth system instance"""
    global _auth_system
    if _auth_system is None:
        _auth_system = EnterpriseAuthSystem()
    return _auth_system
