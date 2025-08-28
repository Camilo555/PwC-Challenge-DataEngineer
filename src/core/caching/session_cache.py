"""
Redis-based Session Management and User Data Caching

Comprehensive session management with user data caching, rate limiting,
and security features.
"""

import asyncio
import hashlib
import json
import secrets
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.redis_streams import publish_cache_invalidation

logger = get_logger(__name__)


class SessionStatus(Enum):
    """Session status enumeration."""
    ACTIVE = "active"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"
    SUSPENDED = "suspended"


class UserRole(Enum):
    """User role enumeration."""
    ADMIN = "admin"
    USER = "user"
    ANALYST = "analyst"
    VIEWER = "viewer"
    SERVICE_ACCOUNT = "service_account"


@dataclass
class SessionData:
    """Session data structure."""
    session_id: str
    user_id: str
    username: str
    email: str
    role: UserRole
    status: SessionStatus
    created_at: datetime
    last_accessed: datetime
    expires_at: datetime
    ip_address: str
    user_agent: str
    permissions: List[str]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Redis storage."""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "role": self.role.value,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "permissions": json.dumps(self.permissions),
            "metadata": json.dumps(self.metadata, default=str)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SessionData':
        """Create from dictionary."""
        return cls(
            session_id=data["session_id"],
            user_id=data["user_id"],
            username=data["username"],
            email=data["email"],
            role=UserRole(data["role"]),
            status=SessionStatus(data["status"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            last_accessed=datetime.fromisoformat(data["last_accessed"]),
            expires_at=datetime.fromisoformat(data["expires_at"]),
            ip_address=data["ip_address"],
            user_agent=data["user_agent"],
            permissions=json.loads(data.get("permissions", "[]")),
            metadata=json.loads(data.get("metadata", "{}"))
        )
    
    def is_expired(self) -> bool:
        """Check if session is expired."""
        return datetime.utcnow() > self.expires_at or self.status == SessionStatus.EXPIRED
    
    def is_valid(self) -> bool:
        """Check if session is valid."""
        return (
            self.status == SessionStatus.ACTIVE and 
            not self.is_expired()
        )
    
    def extend_expiry(self, duration: timedelta):
        """Extend session expiry."""
        self.expires_at = datetime.utcnow() + duration
        self.last_accessed = datetime.utcnow()


@dataclass
class UserProfile:
    """User profile data structure."""
    user_id: str
    username: str
    email: str
    full_name: str
    role: UserRole
    department: str
    permissions: List[str]
    preferences: Dict[str, Any]
    last_login: Optional[datetime]
    login_count: int
    is_active: bool
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Redis storage."""
        return {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "full_name": self.full_name,
            "role": self.role.value,
            "department": self.department,
            "permissions": json.dumps(self.permissions),
            "preferences": json.dumps(self.preferences, default=str),
            "last_login": self.last_login.isoformat() if self.last_login else "",
            "login_count": str(self.login_count),
            "is_active": str(self.is_active).lower(),
            "metadata": json.dumps(self.metadata, default=str)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserProfile':
        """Create from dictionary."""
        return cls(
            user_id=data["user_id"],
            username=data["username"],
            email=data["email"],
            full_name=data["full_name"],
            role=UserRole(data["role"]),
            department=data["department"],
            permissions=json.loads(data.get("permissions", "[]")),
            preferences=json.loads(data.get("preferences", "{}")),
            last_login=datetime.fromisoformat(data["last_login"]) if data["last_login"] else None,
            login_count=int(data.get("login_count", 0)),
            is_active=data.get("is_active", "true").lower() == "true",
            metadata=json.loads(data.get("metadata", "{}"))
        )


class RateLimiter:
    """Redis-based rate limiter."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        self.cache_manager = cache_manager
        self.namespace = "rate_limit"
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def _get_rate_limit_key(self, identifier: str, action: str) -> str:
        """Generate rate limit key."""
        return f"{identifier}:{action}"
    
    async def is_allowed(self, identifier: str, action: str, 
                        limit: int, window: int) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if action is allowed within rate limit.
        
        Args:
            identifier: User ID, IP address, etc.
            action: Action being performed
            limit: Maximum allowed requests
            window: Time window in seconds
            
        Returns:
            Tuple of (is_allowed, rate_limit_info)
        """
        cache_manager = await self._get_cache_manager()
        key = self._get_rate_limit_key(identifier, action)
        
        current_time = int(time.time())
        window_start = current_time - window
        
        # Use sliding window with Redis sorted sets
        pipe = cache_manager.async_redis_client.pipeline()
        
        # Remove old entries
        pipe.zremrangebyscore(
            cache_manager._build_key(key, self.namespace),
            0, window_start
        )
        
        # Count current entries
        pipe.zcard(cache_manager._build_key(key, self.namespace))
        
        # Add current request
        pipe.zadd(
            cache_manager._build_key(key, self.namespace),
            {str(uuid.uuid4()): current_time}
        )
        
        # Set expiry
        pipe.expire(cache_manager._build_key(key, self.namespace), window + 10)
        
        results = await pipe.execute()
        current_count = results[1]
        
        is_allowed = current_count < limit
        
        # If not allowed, remove the request we just added
        if not is_allowed:
            await cache_manager.async_redis_client.zpopmax(
                cache_manager._build_key(key, self.namespace)
            )
        
        rate_limit_info = {
            "limit": limit,
            "remaining": max(0, limit - current_count - (1 if is_allowed else 0)),
            "reset_time": current_time + window,
            "window_size": window
        }
        
        return is_allowed, rate_limit_info
    
    async def get_rate_limit_info(self, identifier: str, action: str,
                                 limit: int, window: int) -> Dict[str, Any]:
        """Get current rate limit information."""
        cache_manager = await self._get_cache_manager()
        key = self._get_rate_limit_key(identifier, action)
        
        current_time = int(time.time())
        window_start = current_time - window
        
        # Clean old entries and count current
        await cache_manager.async_redis_client.zremrangebyscore(
            cache_manager._build_key(key, self.namespace),
            0, window_start
        )
        
        current_count = await cache_manager.async_redis_client.zcard(
            cache_manager._build_key(key, self.namespace)
        )
        
        return {
            "limit": limit,
            "remaining": max(0, limit - current_count),
            "reset_time": current_time + window,
            "window_size": window,
            "current_count": current_count
        }


class SessionManager:
    """Comprehensive session management with Redis."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 session_ttl: int = 3600,  # 1 hour
                 refresh_threshold: float = 0.5):  # Refresh when 50% of TTL remaining
        self.cache_manager = cache_manager
        self.session_ttl = session_ttl
        self.refresh_threshold = refresh_threshold
        self.session_namespace = "sessions"
        self.user_namespace = "users"
        self.rate_limiter = RateLimiter(cache_manager)
        
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def _generate_session_id(self) -> str:
        """Generate secure session ID."""
        return secrets.token_urlsafe(32)
    
    async def create_session(self, user_id: str, username: str, email: str,
                           role: UserRole, permissions: List[str],
                           ip_address: str, user_agent: str,
                           metadata: Optional[Dict[str, Any]] = None) -> SessionData:
        """Create a new session."""
        cache_manager = await self._get_cache_manager()
        
        session_id = self._generate_session_id()
        current_time = datetime.utcnow()
        expires_at = current_time + timedelta(seconds=self.session_ttl)
        
        session_data = SessionData(
            session_id=session_id,
            user_id=user_id,
            username=username,
            email=email,
            role=role,
            status=SessionStatus.ACTIVE,
            created_at=current_time,
            last_accessed=current_time,
            expires_at=expires_at,
            ip_address=ip_address,
            user_agent=user_agent,
            permissions=permissions,
            metadata=metadata or {}
        )
        
        # Store session data
        session_key = f"session:{session_id}"
        await cache_manager.hset(
            session_key, "data", session_data.to_dict(), 
            self.session_namespace
        )
        await cache_manager.expire(session_key, self.session_ttl, self.session_namespace)
        
        # Add to user's active sessions
        user_sessions_key = f"user_sessions:{user_id}"
        await cache_manager.sadd(
            user_sessions_key, session_id, self.session_namespace
        )
        await cache_manager.expire(user_sessions_key, self.session_ttl * 2, self.session_namespace)
        
        # Track session by IP (for security)
        ip_sessions_key = f"ip_sessions:{ip_address}"
        await cache_manager.sadd(
            ip_sessions_key, session_id, self.session_namespace
        )
        await cache_manager.expire(ip_sessions_key, self.session_ttl * 2, self.session_namespace)
        
        logger.info(f"Created session {session_id} for user {user_id}")
        return session_data
    
    async def get_session(self, session_id: str) -> Optional[SessionData]:
        """Get session data."""
        cache_manager = await self._get_cache_manager()
        
        session_key = f"session:{session_id}"
        session_dict = await cache_manager.hgetall(session_key, self.session_namespace)
        
        if not session_dict:
            return None
        
        try:
            session_data = SessionData.from_dict(session_dict["data"])
            
            # Check if session is valid
            if not session_data.is_valid():
                await self.invalidate_session(session_id)
                return None
            
            # Update last accessed time if needed
            await self._update_session_access(session_data)
            
            return session_data
            
        except Exception as e:
            logger.error(f"Error parsing session data for {session_id}: {e}")
            return None
    
    async def _update_session_access(self, session_data: SessionData):
        """Update session last accessed time."""
        current_time = datetime.utcnow()
        
        # Check if we should refresh the session
        time_remaining = (session_data.expires_at - current_time).total_seconds()
        should_refresh = time_remaining < (self.session_ttl * self.refresh_threshold)
        
        if should_refresh:
            session_data.extend_expiry(timedelta(seconds=self.session_ttl))
            
            cache_manager = await self._get_cache_manager()
            session_key = f"session:{session_data.session_id}"
            
            # Update session data
            await cache_manager.hset(
                session_key, "data", session_data.to_dict(),
                self.session_namespace
            )
            await cache_manager.expire(session_key, self.session_ttl, self.session_namespace)
            
            logger.debug(f"Refreshed session {session_data.session_id}")
    
    async def invalidate_session(self, session_id: str) -> bool:
        """Invalidate a specific session."""
        cache_manager = await self._get_cache_manager()
        
        # Get session data first
        session_data = await self.get_session(session_id)
        if not session_data:
            return False
        
        # Remove from user's active sessions
        user_sessions_key = f"user_sessions:{session_data.user_id}"
        await cache_manager.async_redis_client.srem(
            cache_manager._build_key(user_sessions_key, self.session_namespace),
            session_id
        )
        
        # Remove from IP sessions
        ip_sessions_key = f"ip_sessions:{session_data.ip_address}"
        await cache_manager.async_redis_client.srem(
            cache_manager._build_key(ip_sessions_key, self.session_namespace),
            session_id
        )
        
        # Delete session data
        session_key = f"session:{session_id}"
        await cache_manager.delete(session_key, self.session_namespace)
        
        # Publish invalidation event
        await publish_cache_invalidation(
            f"session:{session_id}",
            self.session_namespace,
            source="session_manager"
        )
        
        logger.info(f"Invalidated session {session_id}")
        return True
    
    async def invalidate_all_user_sessions(self, user_id: str) -> int:
        """Invalidate all sessions for a user."""
        cache_manager = await self._get_cache_manager()
        
        user_sessions_key = f"user_sessions:{user_id}"
        session_ids = await cache_manager.smembers(user_sessions_key, self.session_namespace)
        
        count = 0
        for session_id in session_ids:
            if await self.invalidate_session(session_id):
                count += 1
        
        logger.info(f"Invalidated {count} sessions for user {user_id}")
        return count
    
    async def get_user_sessions(self, user_id: str) -> List[SessionData]:
        """Get all active sessions for a user."""
        cache_manager = await self._get_cache_manager()
        
        user_sessions_key = f"user_sessions:{user_id}"
        session_ids = await cache_manager.smembers(user_sessions_key, self.session_namespace)
        
        sessions = []
        for session_id in session_ids:
            session_data = await self.get_session(session_id)
            if session_data:
                sessions.append(session_data)
        
        return sessions
    
    async def cleanup_expired_sessions(self) -> int:
        """Clean up expired sessions."""
        cache_manager = await self._get_cache_manager()
        
        # This would typically be run as a background task
        # For now, we rely on Redis TTL for automatic cleanup
        
        # Could implement: scan for expired sessions and clean up related data
        return 0
    
    async def get_session_stats(self) -> Dict[str, Any]:
        """Get session statistics."""
        cache_manager = await self._get_cache_manager()
        
        # Count active sessions by scanning keys
        # In production, you might want to maintain counters
        
        try:
            session_pattern = cache_manager._build_key("session:*", self.session_namespace)
            
            if cache_manager.is_cluster:
                # For cluster, scan each node
                total_sessions = 0
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(session_pattern)
                    total_sessions += len(keys) if keys else 0
            else:
                keys = await cache_manager.async_redis_client.keys(session_pattern)
                total_sessions = len(keys) if keys else 0
            
            return {
                "active_sessions": total_sessions,
                "session_ttl": self.session_ttl,
                "refresh_threshold": self.refresh_threshold
            }
            
        except Exception as e:
            logger.error(f"Error getting session stats: {e}")
            return {"error": str(e)}


class UserDataCache:
    """User profile and preference caching."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 profile_ttl: int = 1800):  # 30 minutes
        self.cache_manager = cache_manager
        self.profile_ttl = profile_ttl
        self.namespace = "user_data"
        
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    async def cache_user_profile(self, profile: UserProfile) -> bool:
        """Cache user profile data."""
        cache_manager = await self._get_cache_manager()
        
        profile_key = f"profile:{profile.user_id}"
        
        # Store profile data as hash
        profile_dict = profile.to_dict()
        for field, value in profile_dict.items():
            await cache_manager.hset(profile_key, field, value, self.namespace)
        
        # Set expiry
        await cache_manager.expire(profile_key, self.profile_ttl, self.namespace)
        
        logger.debug(f"Cached profile for user {profile.user_id}")
        return True
    
    async def get_user_profile(self, user_id: str) -> Optional[UserProfile]:
        """Get cached user profile."""
        cache_manager = await self._get_cache_manager()
        
        profile_key = f"profile:{user_id}"
        profile_dict = await cache_manager.hgetall(profile_key, self.namespace)
        
        if not profile_dict:
            return None
        
        try:
            return UserProfile.from_dict(profile_dict)
        except Exception as e:
            logger.error(f"Error parsing user profile for {user_id}: {e}")
            return None
    
    async def update_user_preferences(self, user_id: str, 
                                    preferences: Dict[str, Any]) -> bool:
        """Update user preferences."""
        cache_manager = await self._get_cache_manager()
        
        profile_key = f"profile:{user_id}"
        
        # Update preferences field
        await cache_manager.hset(
            profile_key, "preferences", 
            json.dumps(preferences, default=str),
            self.namespace
        )
        
        # Extend expiry
        await cache_manager.expire(profile_key, self.profile_ttl, self.namespace)
        
        logger.debug(f"Updated preferences for user {user_id}")
        return True
    
    async def cache_user_activity(self, user_id: str, activity: str,
                                 metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Cache recent user activity."""
        cache_manager = await self._get_cache_manager()
        
        activity_key = f"activity:{user_id}"
        timestamp = int(time.time())
        
        activity_data = {
            "activity": activity,
            "timestamp": timestamp,
            "metadata": metadata or {}
        }
        
        # Add to sorted set (recent activities)
        await cache_manager.async_redis_client.zadd(
            cache_manager._build_key(activity_key, self.namespace),
            {json.dumps(activity_data, default=str): timestamp}
        )
        
        # Keep only last 100 activities
        await cache_manager.async_redis_client.zremrangebyrank(
            cache_manager._build_key(activity_key, self.namespace),
            0, -101
        )
        
        # Set expiry
        await cache_manager.expire(activity_key, 86400, self.namespace)  # 1 day
        
        return True
    
    async def get_user_activities(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent user activities."""
        cache_manager = await self._get_cache_manager()
        
        activity_key = f"activity:{user_id}"
        
        # Get recent activities (highest scores first)
        activities = await cache_manager.async_redis_client.zrevrange(
            cache_manager._build_key(activity_key, self.namespace),
            0, limit - 1,
            withscores=True
        )
        
        result = []
        for activity_json, timestamp in activities:
            try:
                activity_data = json.loads(activity_json)
                activity_data["timestamp"] = int(timestamp)
                result.append(activity_data)
            except Exception as e:
                logger.error(f"Error parsing activity data: {e}")
        
        return result
    
    async def invalidate_user_cache(self, user_id: str) -> bool:
        """Invalidate all cached data for a user."""
        cache_manager = await self._get_cache_manager()
        
        # Delete profile
        profile_key = f"profile:{user_id}"
        await cache_manager.delete(profile_key, self.namespace)
        
        # Delete activities
        activity_key = f"activity:{user_id}"
        await cache_manager.delete(activity_key, self.namespace)
        
        # Publish invalidation event
        await publish_cache_invalidation(
            f"user_data:{user_id}:*",
            self.namespace,
            pattern=True,
            source="user_cache"
        )
        
        logger.info(f"Invalidated cache for user {user_id}")
        return True


# Global instances
_session_manager: Optional[SessionManager] = None
_user_data_cache: Optional[UserDataCache] = None
_rate_limiter: Optional[RateLimiter] = None


async def get_session_manager() -> SessionManager:
    """Get or create global session manager instance."""
    global _session_manager
    if _session_manager is None:
        _session_manager = SessionManager()
    return _session_manager


async def get_user_data_cache() -> UserDataCache:
    """Get or create global user data cache instance."""
    global _user_data_cache
    if _user_data_cache is None:
        _user_data_cache = UserDataCache()
    return _user_data_cache


async def get_rate_limiter() -> RateLimiter:
    """Get or create global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter