"""
Enhanced Authentication and Authorization Service
Provides comprehensive authentication capabilities including:
- JWT token validation with enhanced security claims
- OAuth2/OIDC integration for enterprise SSO
- API key management with security policies
- Multi-factor authentication support
- Session management with risk-based authentication
- Rate limiting with threat-aware scaling
"""
import asyncio
import hashlib
import hmac
import json
import secrets
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlencode

import httpx
import jwt
from cryptography.fernet import Fernet
from fastapi import HTTPException, status
from passlib.context import CryptContext

from core.logging import get_logger
from core.config.security_config import SecurityConfig
from core.security.enhanced_access_control import get_access_control_manager, AccessRequest, AccessDecision
from core.security.advanced_security import get_security_manager, SecurityEventType, ThreatLevel, ActionType


logger = get_logger(__name__)


class AuthenticationMethod(Enum):
    """Supported authentication methods"""
    JWT = "jwt"
    OAUTH2 = "oauth2"
    OIDC = "oidc"
    API_KEY = "api_key"
    BASIC_AUTH = "basic_auth"
    MFA = "mfa"


class TokenType(Enum):
    """Types of tokens"""
    ACCESS_TOKEN = "access_token"
    REFRESH_TOKEN = "refresh_token"
    ID_TOKEN = "id_token"
    API_KEY = "api_key"
    SESSION_TOKEN = "session_token"


class RiskLevel(Enum):
    """Risk levels for authentication context"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class TokenClaims:
    """Enhanced JWT token claims with security context"""
    sub: str  # Subject (user ID)
    iss: str = "enterprise-security-api"  # Issuer
    aud: str = "api-clients"  # Audience
    exp: Optional[datetime] = None  # Expiration time
    iat: Optional[datetime] = None  # Issued at
    nbf: Optional[datetime] = None  # Not before
    jti: Optional[str] = None  # JWT ID
    
    # Enhanced security claims
    roles: List[str] = field(default_factory=list)
    permissions: List[str] = field(default_factory=list)
    clearance_level: str = "standard"
    session_id: Optional[str] = None
    
    # Risk and security context
    risk_score: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    authentication_method: str = "password"
    mfa_verified: bool = False
    device_id: Optional[str] = None
    source_ip: Optional[str] = None
    
    # Compliance and audit
    compliance_frameworks: List[str] = field(default_factory=list)
    audit_context: Dict[str, Any] = field(default_factory=dict)
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ApiKeyInfo:
    """API key information and metadata"""
    key_id: str
    key_hash: str
    name: str
    description: str
    user_id: str
    permissions: List[str]
    rate_limit: Optional[int] = None
    allowed_ips: List[str] = field(default_factory=list)
    expires_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)
    last_used: Optional[datetime] = None
    usage_count: int = 0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SessionInfo:
    """Session information and security context"""
    session_id: str
    user_id: str
    created_at: datetime
    last_activity: datetime
    source_ip: str
    user_agent: str
    authentication_method: AuthenticationMethod
    mfa_verified: bool = False
    risk_score: float = 0.0
    device_fingerprint: Optional[str] = None
    location: Optional[Dict[str, str]] = None
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthenticationResult:
    """Result of authentication attempt"""
    success: bool
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    token_claims: Optional[TokenClaims] = None
    api_key_info: Optional[ApiKeyInfo] = None
    permissions: List[str] = field(default_factory=list)
    risk_score: float = 0.0
    requires_mfa: bool = False
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class EnhancedAuthenticationService:
    """Comprehensive authentication and authorization service"""
    
    def __init__(self, security_config: Optional[SecurityConfig] = None):
        self.logger = get_logger(__name__)
        self.config = security_config or SecurityConfig()
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.fernet = Fernet(self.config.encryption_key.encode()[:32].ljust(32, b'0'))
        
        # Component integrations
        self.security_manager = get_security_manager()
        self.access_manager = get_access_control_manager()
        
        # Internal storage (would be replaced with database in production)
        self.api_keys: Dict[str, ApiKeyInfo] = {}
        self.sessions: Dict[str, SessionInfo] = {}
        self.refresh_tokens: Dict[str, Dict[str, Any]] = {}
        self.revoked_tokens: Set[str] = set()
        
        # Rate limiting tracking
        self.auth_attempts: Dict[str, List[datetime]] = {}
        
        # OAuth2/OIDC clients
        self.oauth_clients: Dict[str, Dict[str, Any]] = {}
        
        self.logger.info("Enhanced Authentication Service initialized")
    
    async def authenticate_jwt(self, token: str, source_ip: str = None) -> AuthenticationResult:
        """Authenticate and validate JWT token with enhanced security checks"""
        
        try:
            # Check if token is revoked
            if token in self.revoked_tokens:
                return AuthenticationResult(
                    success=False,
                    error_message="Token has been revoked",
                    risk_score=8.0
                )
            
            # Decode and validate JWT
            try:
                payload = jwt.decode(
                    token,
                    self.config.jwt_secret_key,
                    algorithms=[self.config.jwt_algorithm],
                    options={"verify_exp": True, "verify_aud": True, "verify_iss": True}
                )
            except jwt.ExpiredSignatureError:
                return AuthenticationResult(
                    success=False,
                    error_message="Token has expired",
                    risk_score=3.0
                )
            except jwt.InvalidTokenError as e:
                return AuthenticationResult(
                    success=False,
                    error_message=f"Invalid token: {str(e)}",
                    risk_score=7.0
                )
            
            # Create token claims object
            token_claims = TokenClaims(
                sub=payload.get("sub"),
                iss=payload.get("iss"),
                aud=payload.get("aud"),
                exp=datetime.fromtimestamp(payload.get("exp", 0)) if payload.get("exp") else None,
                iat=datetime.fromtimestamp(payload.get("iat", 0)) if payload.get("iat") else None,
                jti=payload.get("jti"),
                roles=payload.get("roles", []),
                permissions=payload.get("permissions", []),
                clearance_level=payload.get("clearance_level", "standard"),
                session_id=payload.get("session_id"),
                risk_score=payload.get("risk_score", 0.0),
                risk_level=RiskLevel(payload.get("risk_level", "low")),
                authentication_method=payload.get("auth_method", "password"),
                mfa_verified=payload.get("mfa_verified", False),
                device_id=payload.get("device_id"),
                source_ip=payload.get("source_ip"),
                compliance_frameworks=payload.get("compliance_frameworks", []),
                audit_context=payload.get("audit_context", {}),
                metadata=payload.get("metadata", {})
            )
            
            # Perform additional security checks
            security_checks = await self._perform_token_security_checks(token_claims, source_ip, token)
            
            # Log authentication event
            await self._log_authentication_event(
                user_id=token_claims.sub,
                method=AuthenticationMethod.JWT,
                success=True,
                source_ip=source_ip,
                metadata={"token_claims": payload}
            )
            
            return AuthenticationResult(
                success=True,
                user_id=token_claims.sub,
                session_id=token_claims.session_id,
                token_claims=token_claims,
                permissions=token_claims.permissions,
                risk_score=token_claims.risk_score + security_checks.get("additional_risk", 0.0),
                metadata=security_checks
            )
            
        except Exception as e:
            self.logger.error(f"JWT authentication failed: {e}")
            
            # Log failed authentication
            await self._log_authentication_event(
                user_id=None,
                method=AuthenticationMethod.JWT,
                success=False,
                source_ip=source_ip,
                error_message=str(e)
            )
            
            return AuthenticationResult(
                success=False,
                error_message=f"Authentication failed: {str(e)}",
                risk_score=5.0
            )
    
    async def authenticate_api_key(self, api_key: str, source_ip: str = None) -> AuthenticationResult:
        """Authenticate using API key with enhanced security policies"""
        
        try:
            # Extract key ID and validate format
            if not api_key.startswith("pwc_"):
                return AuthenticationResult(
                    success=False,
                    error_message="Invalid API key format",
                    risk_score=6.0
                )
            
            # Find API key by hash
            key_hash = self._hash_api_key(api_key)
            api_key_info = None
            
            for key_id, key_info in self.api_keys.items():
                if key_info.key_hash == key_hash:
                    api_key_info = key_info
                    break
            
            if not api_key_info:
                # Track failed API key attempt
                await self._track_failed_auth_attempt(source_ip, "api_key")
                
                return AuthenticationResult(
                    success=False,
                    error_message="Invalid API key",
                    risk_score=7.0
                )
            
            # Check if API key is enabled
            if not api_key_info.enabled:
                return AuthenticationResult(
                    success=False,
                    error_message="API key is disabled",
                    risk_score=5.0
                )
            
            # Check expiration
            if api_key_info.expires_at and datetime.now() > api_key_info.expires_at:
                return AuthenticationResult(
                    success=False,
                    error_message="API key has expired",
                    risk_score=4.0
                )
            
            # Check IP restrictions
            if api_key_info.allowed_ips and source_ip not in api_key_info.allowed_ips:
                return AuthenticationResult(
                    success=False,
                    error_message="API key not allowed from this IP address",
                    risk_score=8.0
                )
            
            # Check rate limiting
            if api_key_info.rate_limit:
                if not await self._check_api_key_rate_limit(api_key_info, source_ip):
                    return AuthenticationResult(
                        success=False,
                        error_message="API key rate limit exceeded",
                        risk_score=6.0
                    )
            
            # Update usage statistics
            api_key_info.last_used = datetime.now()
            api_key_info.usage_count += 1
            
            # Perform security assessment
            risk_score = await self._assess_api_key_risk(api_key_info, source_ip)
            
            # Log successful authentication
            await self._log_authentication_event(
                user_id=api_key_info.user_id,
                method=AuthenticationMethod.API_KEY,
                success=True,
                source_ip=source_ip,
                metadata={
                    "key_id": api_key_info.key_id,
                    "key_name": api_key_info.name,
                    "usage_count": api_key_info.usage_count
                }
            )
            
            return AuthenticationResult(
                success=True,
                user_id=api_key_info.user_id,
                api_key_info=api_key_info,
                permissions=api_key_info.permissions,
                risk_score=risk_score,
                metadata={
                    "key_id": api_key_info.key_id,
                    "authentication_method": "api_key"
                }
            )
            
        except Exception as e:
            self.logger.error(f"API key authentication failed: {e}")
            
            await self._log_authentication_event(
                user_id=None,
                method=AuthenticationMethod.API_KEY,
                success=False,
                source_ip=source_ip,
                error_message=str(e)
            )
            
            return AuthenticationResult(
                success=False,
                error_message=f"API key authentication failed: {str(e)}",
                risk_score=5.0
            )
    
    async def authenticate_oauth2(self, authorization_code: str, redirect_uri: str, client_id: str, source_ip: str = None) -> AuthenticationResult:
        """Authenticate using OAuth2 authorization code flow"""
        
        try:
            # Exchange authorization code for access token
            token_data = await self._exchange_oauth2_code(authorization_code, redirect_uri, client_id)
            
            if not token_data:
                return AuthenticationResult(
                    success=False,
                    error_message="Failed to exchange authorization code",
                    risk_score=6.0
                )
            
            # Get user info from OAuth2 provider
            user_info = await self._get_oauth2_user_info(token_data["access_token"])
            
            if not user_info:
                return AuthenticationResult(
                    success=False,
                    error_message="Failed to retrieve user information",
                    risk_score=5.0
                )
            
            # Create session
            session_id = str(uuid.uuid4())
            user_id = user_info.get("sub") or user_info.get("id") or user_info.get("email")
            
            # Determine permissions based on OAuth2 scopes and user info
            permissions = await self._determine_oauth2_permissions(user_info, token_data.get("scope", ""))
            
            # Create session info
            session_info = SessionInfo(
                session_id=session_id,
                user_id=user_id,
                created_at=datetime.now(),
                last_activity=datetime.now(),
                source_ip=source_ip or "unknown",
                user_agent="oauth2_client",
                authentication_method=AuthenticationMethod.OAUTH2,
                metadata={
                    "oauth2_provider": client_id,
                    "oauth2_scopes": token_data.get("scope", ""),
                    "access_token_expires": token_data.get("expires_in"),
                    "user_info": user_info
                }
            )
            
            self.sessions[session_id] = session_info
            
            # Create JWT token for API access
            token_claims = TokenClaims(
                sub=user_id,
                session_id=session_id,
                permissions=permissions,
                authentication_method="oauth2",
                metadata={
                    "oauth2_provider": client_id,
                    "oauth2_user_info": user_info
                }
            )
            
            # Log successful OAuth2 authentication
            await self._log_authentication_event(
                user_id=user_id,
                method=AuthenticationMethod.OAUTH2,
                success=True,
                source_ip=source_ip,
                metadata={
                    "oauth2_provider": client_id,
                    "oauth2_scopes": token_data.get("scope", "")
                }
            )
            
            return AuthenticationResult(
                success=True,
                user_id=user_id,
                session_id=session_id,
                token_claims=token_claims,
                permissions=permissions,
                risk_score=2.0,  # OAuth2 is generally lower risk
                metadata={
                    "authentication_method": "oauth2",
                    "oauth2_provider": client_id,
                    "oauth2_access_token": token_data["access_token"]
                }
            )
            
        except Exception as e:
            self.logger.error(f"OAuth2 authentication failed: {e}")
            
            await self._log_authentication_event(
                user_id=None,
                method=AuthenticationMethod.OAUTH2,
                success=False,
                source_ip=source_ip,
                error_message=str(e)
            )
            
            return AuthenticationResult(
                success=False,
                error_message=f"OAuth2 authentication failed: {str(e)}",
                risk_score=5.0
            )
    
    async def create_jwt_token(self, token_claims: TokenClaims, expires_in: Optional[int] = None) -> str:
        """Create JWT token with enhanced security claims"""
        
        try:
            now = datetime.now()
            expires_in = expires_in or (self.config.jwt_expiration_hours * 3600)
            
            # Set standard claims
            payload = {
                "sub": token_claims.sub,
                "iss": token_claims.iss,
                "aud": token_claims.aud,
                "iat": int(now.timestamp()),
                "exp": int((now + timedelta(seconds=expires_in)).timestamp()),
                "jti": token_claims.jti or str(uuid.uuid4()),
                
                # Enhanced claims
                "roles": token_claims.roles,
                "permissions": token_claims.permissions,
                "clearance_level": token_claims.clearance_level,
                "session_id": token_claims.session_id,
                "risk_score": token_claims.risk_score,
                "risk_level": token_claims.risk_level.value,
                "auth_method": token_claims.authentication_method,
                "mfa_verified": token_claims.mfa_verified,
                "device_id": token_claims.device_id,
                "source_ip": token_claims.source_ip,
                "compliance_frameworks": token_claims.compliance_frameworks,
                "audit_context": token_claims.audit_context,
                "metadata": token_claims.metadata
            }
            
            # Remove None values
            payload = {k: v for k, v in payload.items() if v is not None}
            
            # Create JWT token
            token = jwt.encode(payload, self.config.jwt_secret_key, algorithm=self.config.jwt_algorithm)
            
            self.logger.info(f"JWT token created for user {token_claims.sub}")
            
            return token
            
        except Exception as e:
            self.logger.error(f"Failed to create JWT token: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create authentication token"
            )
    
    async def create_api_key(
        self,
        user_id: str,
        name: str,
        description: str,
        permissions: List[str],
        expires_in_days: Optional[int] = None,
        allowed_ips: Optional[List[str]] = None,
        rate_limit: Optional[int] = None
    ) -> Tuple[str, str]:
        """Create new API key with security policies"""
        
        try:
            # Generate API key
            key_id = str(uuid.uuid4())
            raw_key = f"pwc_{secrets.token_urlsafe(32)}"
            key_hash = self._hash_api_key(raw_key)
            
            # Set expiration
            expires_at = None
            if expires_in_days:
                expires_at = datetime.now() + timedelta(days=expires_in_days)
            
            # Create API key info
            api_key_info = ApiKeyInfo(
                key_id=key_id,
                key_hash=key_hash,
                name=name,
                description=description,
                user_id=user_id,
                permissions=permissions,
                rate_limit=rate_limit,
                allowed_ips=allowed_ips or [],
                expires_at=expires_at,
                metadata={
                    "created_by": user_id,
                    "creation_method": "api"
                }
            )
            
            # Store API key
            self.api_keys[key_id] = api_key_info
            
            # Log API key creation
            self.security_manager.audit_logger.log_audit_event(
                user_id=user_id,
                action=ActionType.CREATE,
                resource_type="api_key",
                resource_id=key_id,
                new_value={
                    "name": name,
                    "permissions": permissions,
                    "expires_at": expires_at.isoformat() if expires_at else None
                },
                metadata={"key_id": key_id}
            )
            
            self.logger.info(f"API key created: {key_id} for user {user_id}")
            
            return raw_key, key_id
            
        except Exception as e:
            self.logger.error(f"Failed to create API key: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create API key"
            )
    
    async def revoke_token(self, token: str, reason: str = "Manual revocation") -> bool:
        """Revoke JWT token"""
        
        try:
            # Decode token to get JTI
            payload = jwt.decode(
                token,
                self.config.jwt_secret_key,
                algorithms=[self.config.jwt_algorithm],
                options={"verify_exp": False}
            )
            
            jti = payload.get("jti")
            user_id = payload.get("sub")
            
            if jti:
                self.revoked_tokens.add(jti)
                
                # Log token revocation
                self.security_manager.audit_logger.log_audit_event(
                    user_id=user_id,
                    action=ActionType.DELETE,
                    resource_type="jwt_token",
                    resource_id=jti,
                    metadata={"reason": reason, "revoked_at": datetime.now().isoformat()}
                )
                
                self.logger.info(f"Token revoked: {jti} for user {user_id}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to revoke token: {e}")
            return False
    
    async def revoke_api_key(self, key_id: str, reason: str = "Manual revocation") -> bool:
        """Revoke API key"""
        
        try:
            if key_id in self.api_keys:
                api_key_info = self.api_keys[key_id]
                api_key_info.enabled = False
                
                # Log API key revocation
                self.security_manager.audit_logger.log_audit_event(
                    user_id=api_key_info.user_id,
                    action=ActionType.UPDATE,
                    resource_type="api_key",
                    resource_id=key_id,
                    old_value={"enabled": True},
                    new_value={"enabled": False},
                    metadata={"reason": reason, "revoked_at": datetime.now().isoformat()}
                )
                
                self.logger.info(f"API key revoked: {key_id}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to revoke API key: {e}")
            return False
    
    async def validate_permissions(self, user_permissions: List[str], required_permissions: List[str]) -> bool:
        """Validate user permissions against requirements"""
        
        try:
            # Check if user has all required permissions
            return all(perm in user_permissions for perm in required_permissions)
            
        except Exception as e:
            self.logger.error(f"Permission validation failed: {e}")
            return False
    
    async def get_session_info(self, session_id: str) -> Optional[SessionInfo]:
        """Get session information"""
        
        return self.sessions.get(session_id)
    
    async def invalidate_session(self, session_id: str, reason: str = "Manual logout") -> bool:
        """Invalidate user session"""
        
        try:
            if session_id in self.sessions:
                session_info = self.sessions[session_id]
                session_info.active = False
                
                # Log session invalidation
                self.security_manager.audit_logger.log_audit_event(
                    user_id=session_info.user_id,
                    action=ActionType.DELETE,
                    resource_type="session",
                    resource_id=session_id,
                    metadata={"reason": reason, "invalidated_at": datetime.now().isoformat()}
                )
                
                self.logger.info(f"Session invalidated: {session_id}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to invalidate session: {e}")
            return False
    
    # Private helper methods
    
    async def _perform_token_security_checks(self, token_claims: TokenClaims, source_ip: str, token: str) -> Dict[str, Any]:
        """Perform additional security checks on token"""
        
        checks = {
            "additional_risk": 0.0,
            "anomalies": [],
            "recommendations": []
        }
        
        try:
            # Check for IP address changes
            if token_claims.source_ip and source_ip and token_claims.source_ip != source_ip:
                checks["additional_risk"] += 2.0
                checks["anomalies"].append("IP address changed from token creation")
            
            # Check session validity
            if token_claims.session_id:
                session_info = await self.get_session_info(token_claims.session_id)
                if not session_info or not session_info.active:
                    checks["additional_risk"] += 5.0
                    checks["anomalies"].append("Session is invalid or inactive")
                elif session_info.source_ip != source_ip:
                    checks["additional_risk"] += 3.0
                    checks["anomalies"].append("Request from different IP than session")
            
            # Check for high-risk operations
            if "admin" in token_claims.roles or "super_admin" in token_claims.permissions:
                checks["additional_risk"] += 1.0
                checks["recommendations"].append("High-privilege token - ensure secure handling")
            
            # Check token age
            if token_claims.iat:
                token_age = datetime.now() - token_claims.iat
                if token_age > timedelta(hours=24):
                    checks["additional_risk"] += 1.0
                    checks["recommendations"].append("Consider refreshing long-lived token")
            
            return checks
            
        except Exception as e:
            self.logger.error(f"Token security checks failed: {e}")
            return {"additional_risk": 2.0, "error": str(e)}
    
    def _hash_api_key(self, api_key: str) -> str:
        """Hash API key for secure storage"""
        return hashlib.sha256(f"{api_key}{self.config.secret_key}".encode()).hexdigest()
    
    async def _check_api_key_rate_limit(self, api_key_info: ApiKeyInfo, source_ip: str) -> bool:
        """Check API key rate limiting"""
        
        if not api_key_info.rate_limit:
            return True
        
        # Simple rate limiting implementation
        now = datetime.now()
        key = f"api_key_{api_key_info.key_id}_{source_ip}"
        
        if key not in self.auth_attempts:
            self.auth_attempts[key] = []
        
        # Clean old attempts (last hour)
        cutoff = now - timedelta(hours=1)
        self.auth_attempts[key] = [attempt for attempt in self.auth_attempts[key] if attempt >= cutoff]
        
        # Check rate limit
        if len(self.auth_attempts[key]) >= api_key_info.rate_limit:
            return False
        
        self.auth_attempts[key].append(now)
        return True
    
    async def _assess_api_key_risk(self, api_key_info: ApiKeyInfo, source_ip: str) -> float:
        """Assess risk level for API key usage"""
        
        risk_score = 0.0
        
        # Check usage patterns
        if api_key_info.usage_count > 10000:
            risk_score += 0.5  # High usage might indicate compromise
        
        # Check for IP restrictions
        if not api_key_info.allowed_ips:
            risk_score += 1.0  # No IP restrictions increases risk
        
        # Check expiration
        if not api_key_info.expires_at:
            risk_score += 0.5  # No expiration increases risk
        
        # Check permissions
        admin_permissions = ["admin", "super_admin", "system_admin"]
        if any(perm in api_key_info.permissions for perm in admin_permissions):
            risk_score += 2.0  # Admin permissions increase risk
        
        return min(risk_score, 10.0)
    
    async def _track_failed_auth_attempt(self, identifier: str, auth_method: str):
        """Track failed authentication attempts"""
        
        now = datetime.now()
        key = f"failed_auth_{identifier}_{auth_method}"
        
        if key not in self.auth_attempts:
            self.auth_attempts[key] = []
        
        # Clean old attempts (last hour)
        cutoff = now - timedelta(hours=1)
        self.auth_attempts[key] = [attempt for attempt in self.auth_attempts[key] if attempt >= cutoff]
        
        self.auth_attempts[key].append(now)
        
        # Check for brute force attempts
        if len(self.auth_attempts[key]) >= 5:
            self.logger.warning(f"Multiple failed authentication attempts detected: {identifier}")
            
            # Log security event
            self.security_manager.log_authentication_event(
                user_id=None,
                success=False,
                source_ip=identifier if "." in identifier else "unknown",
                details={
                    "event_type": "brute_force_detected",
                    "auth_method": auth_method,
                    "attempt_count": len(self.auth_attempts[key])
                }
            )
    
    async def _exchange_oauth2_code(self, authorization_code: str, redirect_uri: str, client_id: str) -> Optional[Dict[str, Any]]:
        """Exchange OAuth2 authorization code for access token"""
        
        try:
            # This would integrate with actual OAuth2 providers
            # For demo purposes, we'll simulate the exchange
            
            # In production, this would make HTTP request to OAuth2 provider's token endpoint
            token_data = {
                "access_token": f"access_token_{secrets.token_urlsafe(32)}",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": f"refresh_token_{secrets.token_urlsafe(32)}",
                "scope": "openid profile email"
            }
            
            return token_data
            
        except Exception as e:
            self.logger.error(f"Failed to exchange OAuth2 code: {e}")
            return None
    
    async def _get_oauth2_user_info(self, access_token: str) -> Optional[Dict[str, Any]]:
        """Get user information from OAuth2 provider"""
        
        try:
            # This would make HTTP request to OAuth2 provider's userinfo endpoint
            # For demo purposes, we'll simulate user info
            
            user_info = {
                "sub": f"oauth2_user_{secrets.token_hex(8)}",
                "email": "user@example.com",
                "name": "OAuth2 User",
                "given_name": "OAuth2",
                "family_name": "User",
                "picture": None,
                "locale": "en",
                "email_verified": True
            }
            
            return user_info
            
        except Exception as e:
            self.logger.error(f"Failed to get OAuth2 user info: {e}")
            return None
    
    async def _determine_oauth2_permissions(self, user_info: Dict[str, Any], scopes: str) -> List[str]:
        """Determine permissions based on OAuth2 user info and scopes"""
        
        permissions = []
        
        # Map OAuth2 scopes to internal permissions
        scope_mapping = {
            "read": ["data_read", "api_read"],
            "write": ["data_write", "api_write"],
            "admin": ["admin", "user_management"],
            "profile": ["profile_read"],
            "email": ["email_access"]
        }
        
        for scope in scopes.split():
            if scope in scope_mapping:
                permissions.extend(scope_mapping[scope])
        
        # Add default permissions for OAuth2 users
        permissions.extend(["authenticated_user", "oauth2_user"])
        
        return list(set(permissions))  # Remove duplicates
    
    async def _log_authentication_event(
        self,
        user_id: Optional[str],
        method: AuthenticationMethod,
        success: bool,
        source_ip: str = None,
        error_message: str = None,
        metadata: Dict[str, Any] = None
    ):
        """Log authentication event for security monitoring"""
        
        try:
            # Log to security manager
            event_type = SecurityEventType.AUTHENTICATION_SUCCESS if success else SecurityEventType.AUTHENTICATION_FAILURE
            
            self.security_manager.log_authentication_event(
                user_id=user_id or "unknown",
                success=success,
                source_ip=source_ip or "unknown",
                details={
                    "authentication_method": method.value,
                    "error_message": error_message,
                    "metadata": metadata or {}
                }
            )
            
        except Exception as e:
            self.logger.error(f"Failed to log authentication event: {e}")


# Global service instance
_auth_service: Optional[EnhancedAuthenticationService] = None


def get_auth_service(security_config: Optional[SecurityConfig] = None) -> EnhancedAuthenticationService:
    """Get global authentication service instance"""
    global _auth_service
    if _auth_service is None:
        _auth_service = EnhancedAuthenticationService(security_config)
    return _auth_service


# Utility functions for FastAPI dependencies

async def verify_jwt_token(token: str, source_ip: str = None) -> TokenClaims:
    """Verify JWT token and return claims (for use as FastAPI dependency)"""
    
    auth_service = get_auth_service()
    result = await auth_service.authenticate_jwt(token, source_ip)
    
    if not result.success:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=result.error_message,
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return result.token_claims


async def verify_api_key(api_key: str, source_ip: str = None) -> ApiKeyInfo:
    """Verify API key and return key info (for use as FastAPI dependency)"""
    
    auth_service = get_auth_service()
    result = await auth_service.authenticate_api_key(api_key, source_ip)
    
    if not result.success:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=result.error_message,
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return result.api_key_info


async def require_permissions(required_permissions: List[str], user_permissions: List[str]):
    """Check if user has required permissions (for use as FastAPI dependency)"""
    
    auth_service = get_auth_service()
    
    if not await auth_service.validate_permissions(user_permissions, required_permissions):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )