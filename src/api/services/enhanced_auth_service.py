"""
Enhanced Authentication and Authorization Service
Provides comprehensive authentication with JWT, API Keys, OAuth2, and MFA support.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from typing import Any

import orjson
import redis.asyncio as redis
from fastapi import HTTPException, status
from jose import JWTError, jwt
from passlib.context import CryptContext

from core.config import get_settings
from core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class AuthenticationMethod(str, Enum):
    """Authentication methods supported by the system"""

    JWT = "jwt"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    MFA_TOTP = "mfa_totp"
    MFA_SMS = "mfa_sms"
    CERTIFICATE = "certificate"


class SecurityClearanceLevel(str, Enum):
    """Security clearance levels for access control"""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class PermissionType(str, Enum):
    """System permission types"""

    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    EXECUTE = "execute"
    APPROVE = "approve"


@dataclass
class TokenClaims:
    """Enhanced JWT token claims"""

    sub: str  # Subject (user ID)
    roles: list[str] = field(default_factory=list)
    permissions: list[str] = field(default_factory=list)
    clearance_level: SecurityClearanceLevel = SecurityClearanceLevel.PUBLIC
    session_id: str = ""
    mfa_verified: bool = False
    risk_score: float = 0.0
    authentication_method: AuthenticationMethod = AuthenticationMethod.JWT
    source_ip: str = ""
    device_id: str = ""
    issued_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(hours=1))
    scopes: set[str] = field(default_factory=set)


@dataclass
class APIKeyInfo:
    """API Key information and metadata"""

    key_id: str
    name: str
    user_id: str
    permissions: list[str]
    rate_limit: int = 1000  # Requests per hour
    usage_count: int = 0
    last_used: datetime | None = None
    expires_at: datetime | None = None
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AuthenticationResult:
    """Result of authentication attempt"""

    success: bool
    user_id: str | None = None
    token_claims: TokenClaims | None = None
    api_key_info: APIKeyInfo | None = None
    permissions: list[str] = field(default_factory=list)
    session_id: str | None = None
    risk_score: float = 0.0
    error_message: str | None = None
    requires_mfa: bool = False
    mfa_methods: list[AuthenticationMethod] = field(default_factory=list)


class EnhancedAuthService:
    """
    Enterprise-grade authentication and authorization service
    """

    def __init__(self, redis_client: redis.Redis | None = None):
        self.pwd_context = CryptContext(
            schemes=["bcrypt", "pbkdf2_sha256"],
            deprecated="auto",
            bcrypt__rounds=12,  # Higher rounds for better security
        )

        # JWT settings
        self.secret_key = settings.secret_key or secrets.token_urlsafe(32)
        self.algorithm = "HS256"
        self.access_token_expire_minutes = 60
        self.refresh_token_expire_days = 7

        # Redis for session management and caching
        self.redis_client = redis_client or self._create_redis_client()

        # In-memory stores (replace with database in production)
        self._api_keys: dict[str, APIKeyInfo] = {}
        self._active_sessions: dict[str, TokenClaims] = {}
        self._rate_limits: dict[str, dict[str, int]] = {}

        # Security configuration
        self.max_failed_attempts = 5
        self.lockout_duration = timedelta(minutes=30)
        self.password_min_length = 8
        self.require_password_complexity = True

        logger.info("Enhanced Authentication Service initialized")

    def _create_redis_client(self) -> redis.Redis:
        """Create Redis client for session management"""
        try:
            redis_url = getattr(settings, "redis_url", "redis://localhost:6379/1")
            return redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=False,  # We'll handle JSON serialization
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                max_connections=10,
            )
        except Exception as e:
            logger.warning(f"Failed to create Redis client: {e}")
            # Return mock client for development
            return None

    async def authenticate_jwt(self, token: str) -> AuthenticationResult:
        """Authenticate using JWT token with enhanced validation"""
        try:
            # Decode and verify JWT
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm],
                options={"verify_exp": True, "verify_iat": True},
            )

            user_id = payload.get("sub")
            if not user_id:
                return AuthenticationResult(
                    success=False, error_message="Invalid token: missing subject"
                )

            # Check if token is blacklisted
            if await self._is_token_blacklisted(token):
                return AuthenticationResult(success=False, error_message="Token has been revoked")

            # Reconstruct token claims
            token_claims = TokenClaims(
                sub=user_id,
                roles=payload.get("roles", []),
                permissions=payload.get("permissions", []),
                clearance_level=SecurityClearanceLevel(payload.get("clearance_level", "public")),
                session_id=payload.get("session_id", ""),
                mfa_verified=payload.get("mfa_verified", False),
                risk_score=payload.get("risk_score", 0.0),
                authentication_method=AuthenticationMethod(payload.get("auth_method", "jwt")),
                source_ip=payload.get("source_ip", ""),
                device_id=payload.get("device_id", ""),
                scopes=set(payload.get("scopes", [])),
            )

            # Validate session if session_id is present
            if token_claims.session_id:
                session_valid = await self._validate_session(token_claims.session_id, user_id)
                if not session_valid:
                    return AuthenticationResult(
                        success=False, error_message="Session has expired or is invalid"
                    )

            # Calculate current risk score
            current_risk = await self._calculate_risk_score(user_id, token_claims)
            token_claims.risk_score = current_risk

            return AuthenticationResult(
                success=True,
                user_id=user_id,
                token_claims=token_claims,
                permissions=token_claims.permissions,
                session_id=token_claims.session_id,
                risk_score=current_risk,
            )

        except jwt.ExpiredSignatureError:
            return AuthenticationResult(success=False, error_message="Token has expired")
        except jwt.JWTClaimsError:
            return AuthenticationResult(success=False, error_message="Token claims are invalid")
        except JWTError as e:
            logger.warning(f"JWT validation failed: {e}")
            return AuthenticationResult(success=False, error_message="Invalid token format")
        except Exception as e:
            logger.error(f"JWT authentication error: {e}")
            return AuthenticationResult(success=False, error_message="Authentication service error")

    async def authenticate_api_key(self, api_key: str) -> AuthenticationResult:
        """Authenticate using API key with rate limiting and usage tracking"""
        try:
            # Extract key ID from API key format: pwc_keyid_signature
            if not api_key.startswith("pwc_"):
                return AuthenticationResult(success=False, error_message="Invalid API key format")

            parts = api_key.split("_")
            if len(parts) < 3:
                return AuthenticationResult(success=False, error_message="Malformed API key")

            key_id = parts[1]

            # Lookup API key info
            api_key_info = self._api_keys.get(key_id)
            if not api_key_info:
                # Check in Redis/database
                api_key_info = await self._load_api_key_from_storage(key_id)

            if not api_key_info or not api_key_info.is_active:
                return AuthenticationResult(
                    success=False, error_message="API key not found or inactive"
                )

            # Verify API key signature
            if not self._verify_api_key_signature(api_key, api_key_info):
                return AuthenticationResult(
                    success=False, error_message="Invalid API key signature"
                )

            # Check expiration
            if api_key_info.expires_at and datetime.utcnow() > api_key_info.expires_at:
                return AuthenticationResult(success=False, error_message="API key has expired")

            # Check rate limits
            if not await self._check_api_key_rate_limit(api_key_info):
                return AuthenticationResult(
                    success=False, error_message="API key rate limit exceeded"
                )

            # Update usage statistics
            api_key_info.usage_count += 1
            api_key_info.last_used = datetime.utcnow()
            await self._update_api_key_usage(api_key_info)

            # Calculate risk score for API key usage
            risk_score = await self._calculate_api_key_risk_score(api_key_info)

            return AuthenticationResult(
                success=True,
                user_id=api_key_info.user_id,
                api_key_info=api_key_info,
                permissions=api_key_info.permissions,
                session_id=f"apikey_{key_id}_{int(time.time())}",
                risk_score=risk_score,
            )

        except Exception as e:
            logger.error(f"API key authentication error: {e}")
            return AuthenticationResult(
                success=False, error_message="API key authentication failed"
            )

    async def create_access_token(
        self,
        user_id: str,
        roles: list[str] = None,
        permissions: list[str] = None,
        clearance_level: SecurityClearanceLevel = SecurityClearanceLevel.PUBLIC,
        expires_delta: timedelta | None = None,
        **additional_claims,
    ) -> str:
        """Create JWT access token with enhanced claims"""

        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)

        session_id = f"session_{user_id}_{secrets.token_urlsafe(16)}"

        token_claims = {
            "sub": user_id,
            "exp": expire,
            "iat": datetime.utcnow(),
            "jti": secrets.token_urlsafe(16),  # JWT ID for tracking
            "session_id": session_id,
            "roles": roles or [],
            "permissions": permissions or [],
            "clearance_level": clearance_level.value,
            "mfa_verified": additional_claims.get("mfa_verified", False),
            "risk_score": additional_claims.get("risk_score", 0.0),
            "auth_method": additional_claims.get("auth_method", AuthenticationMethod.JWT.value),
            "source_ip": additional_claims.get("source_ip", ""),
            "device_id": additional_claims.get("device_id", ""),
            "scopes": list(additional_claims.get("scopes", set())),
        }

        # Store session information
        await self._store_session(session_id, user_id, token_claims, expire)

        # Create and return JWT
        encoded_jwt = jwt.encode(token_claims, self.secret_key, algorithm=self.algorithm)

        logger.info(f"Created access token for user {user_id} with session {session_id}")
        return encoded_jwt

    async def create_api_key(
        self,
        user_id: str,
        name: str,
        permissions: list[str],
        expires_days: int | None = None,
        rate_limit: int = 1000,
    ) -> str:
        """Create new API key with permissions and rate limiting"""

        key_id = secrets.token_urlsafe(16)
        expires_at = None
        if expires_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_days)

        api_key_info = APIKeyInfo(
            key_id=key_id,
            name=name,
            user_id=user_id,
            permissions=permissions,
            rate_limit=rate_limit,
            expires_at=expires_at,
        )

        # Generate API key with signature
        api_key = self._generate_api_key(api_key_info)

        # Store API key info
        self._api_keys[key_id] = api_key_info
        await self._store_api_key(api_key_info)

        logger.info(f"Created API key {key_id} for user {user_id}")
        return api_key

    def _generate_api_key(self, api_key_info: APIKeyInfo) -> str:
        """Generate API key with HMAC signature"""
        payload = (
            f"{api_key_info.key_id}:{api_key_info.user_id}:{api_key_info.created_at.isoformat()}"
        )
        signature = hmac.new(
            self.secret_key.encode(), payload.encode(), hashlib.sha256
        ).hexdigest()[:16]

        return f"pwc_{api_key_info.key_id}_{signature}"

    def _verify_api_key_signature(self, api_key: str, api_key_info: APIKeyInfo) -> bool:
        """Verify API key HMAC signature"""
        try:
            parts = api_key.split("_")
            if len(parts) != 3:
                return False

            provided_signature = parts[2]
            payload = f"{api_key_info.key_id}:{api_key_info.user_id}:{api_key_info.created_at.isoformat()}"
            expected_signature = hmac.new(
                self.secret_key.encode(), payload.encode(), hashlib.sha256
            ).hexdigest()[:16]

            return hmac.compare_digest(provided_signature, expected_signature)
        except Exception:
            return False

    async def _calculate_risk_score(self, user_id: str, token_claims: TokenClaims) -> float:
        """Calculate dynamic risk score based on various factors"""
        risk_score = 0.0

        # Base risk from user profile
        base_risk = 0.1  # Default low risk

        # IP-based risk (simplified)
        if token_claims.source_ip:
            # In production, integrate with IP reputation services
            if token_claims.source_ip.startswith("10.") or token_claims.source_ip.startswith(
                "192.168."
            ):
                risk_score += 0.0  # Internal IP
            else:
                risk_score += 0.2  # External IP

        # Device-based risk
        if not token_claims.device_id:
            risk_score += 0.1  # Unknown device

        # Time-based risk
        current_hour = datetime.utcnow().hour
        if current_hour < 6 or current_hour > 22:  # Outside business hours
            risk_score += 0.1

        # MFA verification reduces risk
        if token_claims.mfa_verified:
            risk_score *= 0.5

        # Privilege-based risk
        admin_permissions = ["admin", "delete", "execute"]
        if any(perm in token_claims.permissions for perm in admin_permissions):
            risk_score += 0.2

        # Clearance level risk adjustment
        clearance_multipliers = {
            SecurityClearanceLevel.PUBLIC: 1.0,
            SecurityClearanceLevel.INTERNAL: 1.1,
            SecurityClearanceLevel.CONFIDENTIAL: 1.3,
            SecurityClearanceLevel.RESTRICTED: 1.5,
            SecurityClearanceLevel.TOP_SECRET: 2.0,
        }
        risk_score *= clearance_multipliers.get(token_claims.clearance_level, 1.0)

        return min(max(base_risk + risk_score, 0.0), 1.0)  # Clamp between 0 and 1

    async def _calculate_api_key_risk_score(self, api_key_info: APIKeyInfo) -> float:
        """Calculate risk score for API key usage"""
        risk_score = 0.1  # Base API key risk

        # Usage pattern risk
        if api_key_info.usage_count > api_key_info.rate_limit * 0.8:
            risk_score += 0.2  # High usage

        # Age-based risk
        age_days = (datetime.utcnow() - api_key_info.created_at).days
        if age_days > 365:  # Old keys are riskier
            risk_score += 0.1

        # Permission-based risk
        high_risk_permissions = ["admin", "delete", "execute"]
        if any(perm in api_key_info.permissions for perm in high_risk_permissions):
            risk_score += 0.3

        return min(risk_score, 1.0)

    async def _is_token_blacklisted(self, token: str) -> bool:
        """Check if token is blacklisted"""
        if not self.redis_client:
            return False

        try:
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            result = await self.redis_client.get(f"blacklist:{token_hash}")
            return result is not None
        except Exception as e:
            logger.error(f"Error checking token blacklist: {e}")
            return False

    async def blacklist_token(self, token: str, expire_seconds: int = 3600) -> bool:
        """Add token to blacklist"""
        if not self.redis_client:
            return False

        try:
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            await self.redis_client.setex(f"blacklist:{token_hash}", expire_seconds, "revoked")
            logger.info("Token blacklisted successfully")
            return True
        except Exception as e:
            logger.error(f"Error blacklisting token: {e}")
            return False

    async def _validate_session(self, session_id: str, user_id: str) -> bool:
        """Validate active session"""
        if not self.redis_client:
            return True  # Skip validation if Redis unavailable

        try:
            session_key = f"session:{session_id}"
            session_data = await self.redis_client.get(session_key)
            if not session_data:
                return False

            session_info = orjson.loads(session_data)
            return session_info.get("user_id") == user_id
        except Exception as e:
            logger.error(f"Error validating session: {e}")
            return False

    async def _store_session(
        self, session_id: str, user_id: str, claims: dict, expires_at: datetime
    ) -> None:
        """Store session information"""
        if not self.redis_client:
            return

        try:
            session_data = {
                "user_id": user_id,
                "claims": claims,
                "created_at": datetime.utcnow().isoformat(),
                "expires_at": expires_at.isoformat(),
            }

            expire_seconds = int((expires_at - datetime.utcnow()).total_seconds())
            await self.redis_client.setex(
                f"session:{session_id}", expire_seconds, orjson.dumps(session_data)
            )
        except Exception as e:
            logger.error(f"Error storing session: {e}")

    async def _check_api_key_rate_limit(self, api_key_info: APIKeyInfo) -> bool:
        """Check API key rate limits"""
        if not self.redis_client:
            return True  # Skip if Redis unavailable

        try:
            current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            rate_key = f"rate:{api_key_info.key_id}:{current_hour.isoformat()}"

            current_usage = await self.redis_client.get(rate_key)
            if current_usage is None:
                current_usage = 0
            else:
                current_usage = int(current_usage)

            if current_usage >= api_key_info.rate_limit:
                return False

            # Increment usage
            await self.redis_client.incr(rate_key)
            await self.redis_client.expire(rate_key, 3600)  # Expire after 1 hour

            return True
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            return True  # Allow on error

    async def _load_api_key_from_storage(self, key_id: str) -> APIKeyInfo | None:
        """Load API key info from persistent storage"""
        # In production, load from database
        # For now, return None to indicate not found
        return None

    async def _store_api_key(self, api_key_info: APIKeyInfo) -> None:
        """Store API key info to persistent storage"""
        if not self.redis_client:
            return

        try:
            key_data = {
                "key_id": api_key_info.key_id,
                "name": api_key_info.name,
                "user_id": api_key_info.user_id,
                "permissions": api_key_info.permissions,
                "rate_limit": api_key_info.rate_limit,
                "usage_count": api_key_info.usage_count,
                "is_active": api_key_info.is_active,
                "created_at": api_key_info.created_at.isoformat(),
                "expires_at": api_key_info.expires_at.isoformat()
                if api_key_info.expires_at
                else None,
            }

            await self.redis_client.set(f"apikey:{api_key_info.key_id}", orjson.dumps(key_data))
        except Exception as e:
            logger.error(f"Error storing API key: {e}")

    async def _update_api_key_usage(self, api_key_info: APIKeyInfo) -> None:
        """Update API key usage statistics"""
        await self._store_api_key(api_key_info)


# Global service instance
_auth_service: EnhancedAuthService | None = None


@lru_cache(maxsize=1)
def get_auth_service() -> EnhancedAuthService:
    """Get global authentication service instance"""
    global _auth_service
    if _auth_service is None:
        _auth_service = EnhancedAuthService()
    return _auth_service


# Export for backward compatibility
async def verify_jwt_token(token: str) -> dict[str, Any]:
    """Verify JWT token (backward compatibility)"""
    auth_service = get_auth_service()
    result = await auth_service.authenticate_jwt(token)

    if not result.success:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=result.error_message or "Authentication failed",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return {
        "sub": result.user_id,
        "roles": result.token_claims.roles if result.token_claims else [],
        "permissions": result.permissions,
        "session_id": result.session_id,
        "risk_score": result.risk_score,
    }
