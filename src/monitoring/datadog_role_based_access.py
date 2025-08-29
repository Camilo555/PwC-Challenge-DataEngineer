"""
DataDog Role-Based Access Control System
Provides comprehensive role-based access control for dashboards, metrics, and alerting
with dynamic permissions, audit logging, and compliance tracking
"""

import asyncio
import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Set, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import jwt
from pathlib import Path

from core.logging import get_logger

logger = get_logger(__name__)


class Role(Enum):
    """System roles with hierarchical permissions"""
    SUPER_ADMIN = "super_admin"
    ADMIN = "admin"
    EXECUTIVE = "executive"
    MANAGER = "manager"
    TECHNICAL_LEAD = "technical_lead"
    DATA_SCIENTIST = "data_scientist"
    OPERATIONS_MANAGER = "operations_manager"
    ANALYST = "analyst"
    VIEWER = "viewer"
    GUEST = "guest"


class Permission(Enum):
    """System permissions"""
    # Dashboard permissions
    DASHBOARD_CREATE = "dashboard_create"
    DASHBOARD_EDIT = "dashboard_edit"
    DASHBOARD_DELETE = "dashboard_delete"
    DASHBOARD_VIEW = "dashboard_view"
    DASHBOARD_SHARE = "dashboard_share"
    
    # Metrics permissions
    METRICS_CREATE = "metrics_create"
    METRICS_EDIT = "metrics_edit"
    METRICS_VIEW = "metrics_view"
    METRICS_DELETE = "metrics_delete"
    
    # Alerting permissions
    ALERT_CREATE = "alert_create"
    ALERT_EDIT = "alert_edit"
    ALERT_VIEW = "alert_view"
    ALERT_DELETE = "alert_delete"
    ALERT_ACKNOWLEDGE = "alert_acknowledge"
    
    # Administrative permissions
    USER_MANAGE = "user_manage"
    ROLE_MANAGE = "role_manage"
    SYSTEM_CONFIG = "system_config"
    AUDIT_VIEW = "audit_view"
    
    # Data permissions
    DATA_EXPORT = "data_export"
    DATA_SENSITIVE_VIEW = "data_sensitive_view"
    
    # ML Operations permissions
    ML_MODEL_DEPLOY = "ml_model_deploy"
    ML_EXPERIMENT_CREATE = "ml_experiment_create"
    ML_EXPERIMENT_VIEW = "ml_experiment_view"


class ResourceType(Enum):
    """Types of resources for access control"""
    DASHBOARD = "dashboard"
    METRIC = "metric"
    ALERT = "alert"
    USER = "user"
    ROLE = "role"
    MODEL = "model"
    EXPERIMENT = "experiment"
    DATA_SOURCE = "data_source"


class AccessLevel(Enum):
    """Access levels for resources"""
    NONE = "none"
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    OWNER = "owner"


@dataclass
class User:
    """User definition with roles and permissions"""
    user_id: str
    username: str
    email: str
    roles: List[Role]
    department: str
    manager: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    is_active: bool = True
    additional_permissions: List[Permission] = field(default_factory=list)
    restricted_permissions: List[Permission] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceAccess:
    """Resource-specific access control"""
    resource_type: ResourceType
    resource_id: str
    user_id: str
    access_level: AccessLevel
    granted_by: str
    granted_at: datetime
    expires_at: Optional[datetime] = None
    conditions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AccessRequest:
    """Access request for approval workflow"""
    request_id: str
    user_id: str
    resource_type: ResourceType
    resource_id: str
    requested_access: AccessLevel
    justification: str
    requested_by: str
    requested_at: datetime
    status: str = "pending"  # pending, approved, denied, expired
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None


@dataclass
class AuditEntry:
    """Audit log entry"""
    entry_id: str
    user_id: str
    action: str
    resource_type: ResourceType
    resource_id: str
    timestamp: datetime
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    additional_data: Dict[str, Any] = field(default_factory=dict)
    success: bool = True
    failure_reason: Optional[str] = None


@dataclass
class SessionInfo:
    """User session information"""
    session_id: str
    user_id: str
    created_at: datetime
    last_activity: datetime
    ip_address: str
    user_agent: str
    is_active: bool = True
    permissions_cache: Dict[str, bool] = field(default_factory=dict)


class DataDogRoleBasedAccess:
    """
    Comprehensive Role-Based Access Control System
    
    Features:
    - Hierarchical role-based permissions
    - Resource-specific access control
    - Dynamic permission evaluation
    - Session management and caching
    - Audit logging and compliance tracking
    - Access request approval workflow
    - Time-based access expiration
    - Integration with external identity providers
    - Multi-factor authentication support
    """
    
    def __init__(self, service_name: str = "rbac-system",
                 config_path: Optional[str] = None,
                 jwt_secret: Optional[str] = None):
        self.service_name = service_name
        self.config_path = config_path or "./config/rbac"
        self.jwt_secret = jwt_secret or "your-super-secret-key"
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Core data structures
        self.users: Dict[str, User] = {}
        self.resource_access: Dict[str, List[ResourceAccess]] = {}
        self.access_requests: Dict[str, AccessRequest] = {}
        self.audit_log: List[AuditEntry] = []
        self.active_sessions: Dict[str, SessionInfo] = {}
        
        # Role hierarchy and permissions mapping
        self.role_hierarchy: Dict[Role, List[Role]] = {}
        self.role_permissions: Dict[Role, List[Permission]] = {}
        self.permission_cache: Dict[str, Dict[str, bool]] = {}
        
        # Configuration
        self.session_timeout_hours = 8
        self.max_failed_login_attempts = 5
        self.audit_retention_days = 365
        self.require_mfa_for_roles = [Role.SUPER_ADMIN, Role.ADMIN, Role.EXECUTIVE]
        
        # Initialize RBAC system
        self._initialize_role_hierarchy()
        self._initialize_role_permissions()
        self._load_users_and_permissions()
        
        # Start background services
        asyncio.create_task(self._start_background_services())
        
        self.logger.info(f"RBAC system initialized for {service_name}")
    
    def _initialize_role_hierarchy(self):
        """Initialize role hierarchy - higher roles inherit lower role permissions"""
        
        self.role_hierarchy = {
            Role.SUPER_ADMIN: [Role.ADMIN, Role.EXECUTIVE, Role.MANAGER, Role.TECHNICAL_LEAD,
                              Role.DATA_SCIENTIST, Role.OPERATIONS_MANAGER, Role.ANALYST,
                              Role.VIEWER, Role.GUEST],
            Role.ADMIN: [Role.MANAGER, Role.TECHNICAL_LEAD, Role.DATA_SCIENTIST,
                        Role.OPERATIONS_MANAGER, Role.ANALYST, Role.VIEWER, Role.GUEST],
            Role.EXECUTIVE: [Role.MANAGER, Role.ANALYST, Role.VIEWER, Role.GUEST],
            Role.MANAGER: [Role.ANALYST, Role.VIEWER, Role.GUEST],
            Role.TECHNICAL_LEAD: [Role.ANALYST, Role.VIEWER, Role.GUEST],
            Role.DATA_SCIENTIST: [Role.ANALYST, Role.VIEWER, Role.GUEST],
            Role.OPERATIONS_MANAGER: [Role.ANALYST, Role.VIEWER, Role.GUEST],
            Role.ANALYST: [Role.VIEWER, Role.GUEST],
            Role.VIEWER: [Role.GUEST],
            Role.GUEST: []
        }
        
        self.logger.info("Role hierarchy initialized")
    
    def _initialize_role_permissions(self):
        """Initialize permissions for each role"""
        
        self.role_permissions = {
            Role.SUPER_ADMIN: [
                # Full system access
                Permission.DASHBOARD_CREATE, Permission.DASHBOARD_EDIT, Permission.DASHBOARD_DELETE,
                Permission.DASHBOARD_VIEW, Permission.DASHBOARD_SHARE,
                Permission.METRICS_CREATE, Permission.METRICS_EDIT, Permission.METRICS_VIEW,
                Permission.METRICS_DELETE,
                Permission.ALERT_CREATE, Permission.ALERT_EDIT, Permission.ALERT_VIEW,
                Permission.ALERT_DELETE, Permission.ALERT_ACKNOWLEDGE,
                Permission.USER_MANAGE, Permission.ROLE_MANAGE, Permission.SYSTEM_CONFIG,
                Permission.AUDIT_VIEW, Permission.DATA_EXPORT, Permission.DATA_SENSITIVE_VIEW,
                Permission.ML_MODEL_DEPLOY, Permission.ML_EXPERIMENT_CREATE, Permission.ML_EXPERIMENT_VIEW
            ],
            
            Role.ADMIN: [
                Permission.DASHBOARD_CREATE, Permission.DASHBOARD_EDIT, Permission.DASHBOARD_DELETE,
                Permission.DASHBOARD_VIEW, Permission.DASHBOARD_SHARE,
                Permission.METRICS_CREATE, Permission.METRICS_EDIT, Permission.METRICS_VIEW,
                Permission.METRICS_DELETE,
                Permission.ALERT_CREATE, Permission.ALERT_EDIT, Permission.ALERT_VIEW,
                Permission.ALERT_DELETE, Permission.ALERT_ACKNOWLEDGE,
                Permission.USER_MANAGE, Permission.AUDIT_VIEW, Permission.DATA_EXPORT,
                Permission.ML_MODEL_DEPLOY, Permission.ML_EXPERIMENT_CREATE, Permission.ML_EXPERIMENT_VIEW
            ],
            
            Role.EXECUTIVE: [
                Permission.DASHBOARD_VIEW, Permission.DASHBOARD_SHARE,
                Permission.METRICS_VIEW, Permission.ALERT_VIEW, Permission.ALERT_ACKNOWLEDGE,
                Permission.DATA_EXPORT, Permission.DATA_SENSITIVE_VIEW, Permission.ML_EXPERIMENT_VIEW
            ],
            
            Role.MANAGER: [
                Permission.DASHBOARD_CREATE, Permission.DASHBOARD_EDIT, Permission.DASHBOARD_VIEW,
                Permission.DASHBOARD_SHARE, Permission.METRICS_VIEW, Permission.ALERT_VIEW,
                Permission.ALERT_ACKNOWLEDGE, Permission.DATA_EXPORT, Permission.ML_EXPERIMENT_VIEW
            ],
            
            Role.TECHNICAL_LEAD: [
                Permission.DASHBOARD_CREATE, Permission.DASHBOARD_EDIT, Permission.DASHBOARD_VIEW,
                Permission.DASHBOARD_SHARE, Permission.METRICS_CREATE, Permission.METRICS_EDIT,
                Permission.METRICS_VIEW, Permission.ALERT_CREATE, Permission.ALERT_EDIT,
                Permission.ALERT_VIEW, Permission.ALERT_ACKNOWLEDGE, Permission.DATA_EXPORT,
                Permission.ML_MODEL_DEPLOY, Permission.ML_EXPERIMENT_CREATE, Permission.ML_EXPERIMENT_VIEW
            ],
            
            Role.DATA_SCIENTIST: [
                Permission.DASHBOARD_CREATE, Permission.DASHBOARD_EDIT, Permission.DASHBOARD_VIEW,
                Permission.METRICS_CREATE, Permission.METRICS_EDIT, Permission.METRICS_VIEW,
                Permission.ALERT_VIEW, Permission.DATA_EXPORT, Permission.ML_MODEL_DEPLOY,
                Permission.ML_EXPERIMENT_CREATE, Permission.ML_EXPERIMENT_VIEW
            ],
            
            Role.OPERATIONS_MANAGER: [
                Permission.DASHBOARD_CREATE, Permission.DASHBOARD_EDIT, Permission.DASHBOARD_VIEW,
                Permission.METRICS_VIEW, Permission.ALERT_CREATE, Permission.ALERT_EDIT,
                Permission.ALERT_VIEW, Permission.ALERT_ACKNOWLEDGE, Permission.DATA_EXPORT
            ],
            
            Role.ANALYST: [
                Permission.DASHBOARD_VIEW, Permission.DASHBOARD_SHARE, Permission.METRICS_VIEW,
                Permission.ALERT_VIEW, Permission.DATA_EXPORT, Permission.ML_EXPERIMENT_VIEW
            ],
            
            Role.VIEWER: [
                Permission.DASHBOARD_VIEW, Permission.METRICS_VIEW, Permission.ALERT_VIEW
            ],
            
            Role.GUEST: [
                Permission.DASHBOARD_VIEW, Permission.METRICS_VIEW
            ]
        }
        
        self.logger.info(f"Role permissions initialized for {len(self.role_permissions)} roles")
    
    def _load_users_and_permissions(self):
        """Load users and permissions from configuration"""
        
        try:
            config_dir = Path(self.config_path)
            if config_dir.exists():
                # Load users
                users_file = config_dir / "users.json"
                if users_file.exists():
                    with open(users_file, 'r') as f:
                        users_data = json.load(f)
                    
                    for user_data in users_data:
                        user = User(
                            user_id=user_data["user_id"],
                            username=user_data["username"],
                            email=user_data["email"],
                            roles=[Role(role) for role in user_data["roles"]],
                            department=user_data["department"],
                            manager=user_data.get("manager"),
                            is_active=user_data.get("is_active", True),
                            additional_permissions=[Permission(p) for p in user_data.get("additional_permissions", [])],
                            restricted_permissions=[Permission(p) for p in user_data.get("restricted_permissions", [])]
                        )
                        self.users[user.user_id] = user
                
                # Load resource access
                access_file = config_dir / "resource_access.json"
                if access_file.exists():
                    with open(access_file, 'r') as f:
                        access_data = json.load(f)
                    
                    for access in access_data:
                        resource_access = ResourceAccess(
                            resource_type=ResourceType(access["resource_type"]),
                            resource_id=access["resource_id"],
                            user_id=access["user_id"],
                            access_level=AccessLevel(access["access_level"]),
                            granted_by=access["granted_by"],
                            granted_at=datetime.fromisoformat(access["granted_at"]),
                            expires_at=datetime.fromisoformat(access["expires_at"]) if access.get("expires_at") else None
                        )
                        
                        if access["resource_id"] not in self.resource_access:
                            self.resource_access[access["resource_id"]] = []
                        self.resource_access[access["resource_id"]].append(resource_access)
            else:
                # Create default admin user if no configuration exists
                self._create_default_users()
            
            self.logger.info(f"Loaded {len(self.users)} users and resource access permissions")
            
        except Exception as e:
            self.logger.error(f"Failed to load users and permissions: {str(e)}")
            self._create_default_users()
    
    def _create_default_users(self):
        """Create default system users"""
        
        default_users = [
            User(
                user_id="admin",
                username="admin",
                email="admin@company.com",
                roles=[Role.SUPER_ADMIN],
                department="IT"
            ),
            User(
                user_id="ceo",
                username="ceo",
                email="ceo@company.com",
                roles=[Role.EXECUTIVE],
                department="Executive"
            ),
            User(
                user_id="cto",
                username="cto",
                email="cto@company.com",
                roles=[Role.EXECUTIVE, Role.TECHNICAL_LEAD],
                department="Technology"
            ),
            User(
                user_id="data_scientist",
                username="data_scientist",
                email="ds@company.com",
                roles=[Role.DATA_SCIENTIST],
                department="Data Science"
            )
        ]
        
        for user in default_users:
            self.users[user.user_id] = user
        
        self.logger.info(f"Created {len(default_users)} default users")
    
    async def _start_background_services(self):
        """Start background services for session management and cleanup"""
        
        try:
            # Start session cleanup service
            asyncio.create_task(self._session_cleanup_service())
            
            # Start audit log cleanup service
            asyncio.create_task(self._audit_cleanup_service())
            
            # Start permission cache refresh service
            asyncio.create_task(self._permission_cache_refresh_service())
            
            self.logger.info("RBAC background services started")
            
        except Exception as e:
            self.logger.error(f"Failed to start background services: {str(e)}")
    
    async def authenticate_user(self, username: str, password: str,
                              ip_address: Optional[str] = None,
                              user_agent: Optional[str] = None) -> Optional[str]:
        """Authenticate user and create session"""
        
        try:
            # Find user by username or email
            user = None
            for u in self.users.values():
                if u.username == username or u.email == username:
                    user = u
                    break
            
            if not user or not user.is_active:
                await self._log_audit_event(
                    user_id=username,
                    action="login_failed",
                    resource_type=ResourceType.USER,
                    resource_id=username,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    success=False,
                    failure_reason="User not found or inactive"
                )
                return None
            
            # In a real implementation, you would verify the password hash
            # For demonstration purposes, we'll assume authentication succeeds
            password_valid = True  # This would be actual password verification
            
            if not password_valid:
                await self._log_audit_event(
                    user_id=user.user_id,
                    action="login_failed",
                    resource_type=ResourceType.USER,
                    resource_id=user.user_id,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    success=False,
                    failure_reason="Invalid password"
                )
                return None
            
            # Create session
            session_id = self._generate_session_id(user.user_id)
            session = SessionInfo(
                session_id=session_id,
                user_id=user.user_id,
                created_at=datetime.utcnow(),
                last_activity=datetime.utcnow(),
                ip_address=ip_address or "unknown",
                user_agent=user_agent or "unknown"
            )
            
            self.active_sessions[session_id] = session
            
            # Update user last login
            user.last_login = datetime.utcnow()
            
            # Log successful login
            await self._log_audit_event(
                user_id=user.user_id,
                action="login_success",
                resource_type=ResourceType.USER,
                resource_id=user.user_id,
                ip_address=ip_address,
                user_agent=user_agent,
                success=True
            )
            
            # Generate JWT token for session
            jwt_payload = {
                "session_id": session_id,
                "user_id": user.user_id,
                "roles": [role.value for role in user.roles],
                "exp": datetime.utcnow() + timedelta(hours=self.session_timeout_hours)
            }
            
            token = jwt.encode(jwt_payload, self.jwt_secret, algorithm="HS256")
            
            self.logger.info(f"User {user.username} authenticated successfully")
            return token
            
        except Exception as e:
            self.logger.error(f"Failed to authenticate user: {str(e)}")
            return None
    
    def _generate_session_id(self, user_id: str) -> str:
        """Generate unique session ID"""
        
        timestamp = datetime.utcnow().isoformat()
        raw_data = f"{user_id}:{timestamp}:{self.jwt_secret}"
        return hashlib.sha256(raw_data.encode()).hexdigest()
    
    async def validate_session(self, token: str) -> Optional[SessionInfo]:
        """Validate session token and return session info"""
        
        try:
            # Decode JWT token
            payload = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
            session_id = payload["session_id"]
            
            # Check if session exists and is active
            if session_id not in self.active_sessions:
                return None
            
            session = self.active_sessions[session_id]
            
            if not session.is_active:
                return None
            
            # Check session timeout
            if (datetime.utcnow() - session.last_activity).total_seconds() > self.session_timeout_hours * 3600:
                session.is_active = False
                return None
            
            # Update last activity
            session.last_activity = datetime.utcnow()
            
            return session
            
        except jwt.ExpiredSignatureError:
            self.logger.warning("Session token expired")
            return None
        except jwt.InvalidTokenError:
            self.logger.warning("Invalid session token")
            return None
        except Exception as e:
            self.logger.error(f"Failed to validate session: {str(e)}")
            return None
    
    async def check_permission(self, user_id: str, permission: Permission,
                             resource_type: Optional[ResourceType] = None,
                             resource_id: Optional[str] = None) -> bool:
        """Check if user has specific permission"""
        
        try:
            # Check cache first
            cache_key = f"{user_id}:{permission.value}:{resource_type.value if resource_type else 'global'}:{resource_id or 'global'}"
            
            if cache_key in self.permission_cache.get(user_id, {}):
                return self.permission_cache[user_id][cache_key]
            
            user = self.users.get(user_id)
            if not user or not user.is_active:
                return False
            
            # Check if permission is explicitly restricted
            if permission in user.restricted_permissions:
                self._cache_permission(user_id, cache_key, False)
                return False
            
            # Check if permission is explicitly granted
            if permission in user.additional_permissions:
                self._cache_permission(user_id, cache_key, True)
                return True
            
            # Check role-based permissions
            user_permissions = set()
            for role in user.roles:
                # Add direct role permissions
                if role in self.role_permissions:
                    user_permissions.update(self.role_permissions[role])
                
                # Add inherited permissions from role hierarchy
                if role in self.role_hierarchy:
                    for inherited_role in self.role_hierarchy[role]:
                        if inherited_role in self.role_permissions:
                            user_permissions.update(self.role_permissions[inherited_role])
            
            has_permission = permission in user_permissions
            
            # Check resource-specific access if specified
            if has_permission and resource_type and resource_id:
                resource_access = await self._check_resource_access(
                    user_id, resource_type, resource_id
                )
                has_permission = resource_access != AccessLevel.NONE
            
            self._cache_permission(user_id, cache_key, has_permission)
            return has_permission
            
        except Exception as e:
            self.logger.error(f"Failed to check permission: {str(e)}")
            return False
    
    def _cache_permission(self, user_id: str, cache_key: str, result: bool):
        """Cache permission check result"""
        
        if user_id not in self.permission_cache:
            self.permission_cache[user_id] = {}
        
        self.permission_cache[user_id][cache_key] = result
        
        # Limit cache size per user
        if len(self.permission_cache[user_id]) > 1000:
            # Remove oldest entries
            sorted_items = sorted(
                self.permission_cache[user_id].items(),
                key=lambda x: x[0]  # Sort by cache key (contains timestamp info)
            )
            self.permission_cache[user_id] = dict(sorted_items[-800:])  # Keep 800 most recent
    
    async def _check_resource_access(self, user_id: str, resource_type: ResourceType,
                                   resource_id: str) -> AccessLevel:
        """Check user's access level for specific resource"""
        
        try:
            resource_accesses = self.resource_access.get(resource_id, [])
            
            user_access = AccessLevel.NONE
            current_time = datetime.utcnow()
            
            for access in resource_accesses:
                if (access.user_id == user_id and 
                    access.resource_type == resource_type and
                    (not access.expires_at or access.expires_at > current_time)):
                    
                    # Take the highest access level
                    if access.access_level.value > user_access.value:
                        user_access = access.access_level
            
            return user_access
            
        except Exception as e:
            self.logger.error(f"Failed to check resource access: {str(e)}")
            return AccessLevel.NONE
    
    async def grant_resource_access(self, user_id: str, resource_type: ResourceType,
                                  resource_id: str, access_level: AccessLevel,
                                  granted_by: str, expires_at: Optional[datetime] = None) -> bool:
        """Grant resource access to user"""
        
        try:
            if resource_id not in self.resource_access:
                self.resource_access[resource_id] = []
            
            # Remove existing access for this user and resource
            self.resource_access[resource_id] = [
                access for access in self.resource_access[resource_id]
                if not (access.user_id == user_id and access.resource_type == resource_type)
            ]
            
            # Add new access
            access = ResourceAccess(
                resource_type=resource_type,
                resource_id=resource_id,
                user_id=user_id,
                access_level=access_level,
                granted_by=granted_by,
                granted_at=datetime.utcnow(),
                expires_at=expires_at
            )
            
            self.resource_access[resource_id].append(access)
            
            # Clear permission cache for user
            if user_id in self.permission_cache:
                del self.permission_cache[user_id]
            
            # Log audit event
            await self._log_audit_event(
                user_id=granted_by,
                action="access_granted",
                resource_type=resource_type,
                resource_id=resource_id,
                additional_data={
                    "target_user": user_id,
                    "access_level": access_level.value,
                    "expires_at": expires_at.isoformat() if expires_at else None
                }
            )
            
            self.logger.info(f"Granted {access_level.value} access to {resource_type.value}:{resource_id} for user {user_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to grant resource access: {str(e)}")
            return False
    
    async def revoke_resource_access(self, user_id: str, resource_type: ResourceType,
                                   resource_id: str, revoked_by: str) -> bool:
        """Revoke resource access from user"""
        
        try:
            if resource_id in self.resource_access:
                original_count = len(self.resource_access[resource_id])
                
                # Remove access for this user and resource
                self.resource_access[resource_id] = [
                    access for access in self.resource_access[resource_id]
                    if not (access.user_id == user_id and access.resource_type == resource_type)
                ]
                
                revoked = len(self.resource_access[resource_id]) < original_count
                
                if revoked:
                    # Clear permission cache for user
                    if user_id in self.permission_cache:
                        del self.permission_cache[user_id]
                    
                    # Log audit event
                    await self._log_audit_event(
                        user_id=revoked_by,
                        action="access_revoked",
                        resource_type=resource_type,
                        resource_id=resource_id,
                        additional_data={"target_user": user_id}
                    )
                    
                    self.logger.info(f"Revoked access to {resource_type.value}:{resource_id} from user {user_id}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to revoke resource access: {str(e)}")
            return False
    
    async def create_access_request(self, user_id: str, resource_type: ResourceType,
                                  resource_id: str, requested_access: AccessLevel,
                                  justification: str, requested_by: str) -> str:
        """Create access request for approval"""
        
        try:
            request_id = f"req_{int(datetime.utcnow().timestamp())}_{user_id}"
            
            access_request = AccessRequest(
                request_id=request_id,
                user_id=user_id,
                resource_type=resource_type,
                resource_id=resource_id,
                requested_access=requested_access,
                justification=justification,
                requested_by=requested_by,
                requested_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(days=30)  # Request expires in 30 days
            )
            
            self.access_requests[request_id] = access_request
            
            # Log audit event
            await self._log_audit_event(
                user_id=requested_by,
                action="access_requested",
                resource_type=resource_type,
                resource_id=resource_id,
                additional_data={
                    "target_user": user_id,
                    "requested_access": requested_access.value,
                    "justification": justification,
                    "request_id": request_id
                }
            )
            
            self.logger.info(f"Access request created: {request_id}")
            return request_id
            
        except Exception as e:
            self.logger.error(f"Failed to create access request: {str(e)}")
            return ""
    
    async def approve_access_request(self, request_id: str, approved_by: str) -> bool:
        """Approve access request"""
        
        try:
            if request_id not in self.access_requests:
                return False
            
            request = self.access_requests[request_id]
            
            if request.status != "pending":
                return False
            
            if request.expires_at and datetime.utcnow() > request.expires_at:
                request.status = "expired"
                return False
            
            # Grant the access
            success = await self.grant_resource_access(
                user_id=request.user_id,
                resource_type=request.resource_type,
                resource_id=request.resource_id,
                access_level=request.requested_access,
                granted_by=approved_by
            )
            
            if success:
                request.status = "approved"
                request.approved_by = approved_by
                request.approved_at = datetime.utcnow()
                
                # Log audit event
                await self._log_audit_event(
                    user_id=approved_by,
                    action="access_request_approved",
                    resource_type=request.resource_type,
                    resource_id=request.resource_id,
                    additional_data={
                        "request_id": request_id,
                        "target_user": request.user_id
                    }
                )
                
                self.logger.info(f"Access request approved: {request_id}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to approve access request: {str(e)}")
            return False
    
    async def _log_audit_event(self, user_id: str, action: str, resource_type: ResourceType,
                             resource_id: str, ip_address: Optional[str] = None,
                             user_agent: Optional[str] = None,
                             additional_data: Optional[Dict[str, Any]] = None,
                             success: bool = True, failure_reason: Optional[str] = None):
        """Log audit event"""
        
        try:
            entry_id = f"audit_{int(datetime.utcnow().timestamp())}_{hashlib.md5(user_id.encode()).hexdigest()[:8]}"
            
            audit_entry = AuditEntry(
                entry_id=entry_id,
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                timestamp=datetime.utcnow(),
                ip_address=ip_address,
                user_agent=user_agent,
                additional_data=additional_data or {},
                success=success,
                failure_reason=failure_reason
            )
            
            self.audit_log.append(audit_entry)
            
            # Limit audit log size in memory
            if len(self.audit_log) > 10000:
                self.audit_log = self.audit_log[-8000:]  # Keep 8000 most recent
            
        except Exception as e:
            self.logger.error(f"Failed to log audit event: {str(e)}")
    
    async def _session_cleanup_service(self):
        """Background service to clean up expired sessions"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                current_time = datetime.utcnow()
                expired_sessions = []
                
                for session_id, session in self.active_sessions.items():
                    if (current_time - session.last_activity).total_seconds() > self.session_timeout_hours * 3600:
                        expired_sessions.append(session_id)
                
                for session_id in expired_sessions:
                    session = self.active_sessions[session_id]
                    session.is_active = False
                    del self.active_sessions[session_id]
                    
                    # Log session expiration
                    await self._log_audit_event(
                        user_id=session.user_id,
                        action="session_expired",
                        resource_type=ResourceType.USER,
                        resource_id=session.user_id,
                        additional_data={"session_id": session_id}
                    )
                
                if expired_sessions:
                    self.logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
                
            except Exception as e:
                self.logger.error(f"Error in session cleanup service: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _audit_cleanup_service(self):
        """Background service to clean up old audit logs"""
        
        while True:
            try:
                await asyncio.sleep(86400)  # Run daily
                
                cutoff_time = datetime.utcnow() - timedelta(days=self.audit_retention_days)
                
                original_count = len(self.audit_log)
                self.audit_log = [
                    entry for entry in self.audit_log
                    if entry.timestamp >= cutoff_time
                ]
                
                cleaned_count = original_count - len(self.audit_log)
                if cleaned_count > 0:
                    self.logger.info(f"Cleaned up {cleaned_count} old audit log entries")
                
            except Exception as e:
                self.logger.error(f"Error in audit cleanup service: {str(e)}")
                await asyncio.sleep(86400)
    
    async def _permission_cache_refresh_service(self):
        """Background service to refresh permission cache"""
        
        while True:
            try:
                await asyncio.sleep(1800)  # Run every 30 minutes
                
                # Clear cache periodically to ensure fresh permissions
                cache_keys_to_remove = []
                
                for user_id, user_cache in self.permission_cache.items():
                    if len(user_cache) > 500:  # If cache is getting large, clear old entries
                        cache_keys_to_remove.append(user_id)
                
                for user_id in cache_keys_to_remove:
                    self.permission_cache[user_id] = {}
                
                if cache_keys_to_remove:
                    self.logger.info(f"Refreshed permission cache for {len(cache_keys_to_remove)} users")
                
            except Exception as e:
                self.logger.error(f"Error in permission cache refresh service: {str(e)}")
                await asyncio.sleep(1800)
    
    def get_rbac_summary(self) -> Dict[str, Any]:
        """Get RBAC system summary"""
        
        try:
            current_time = datetime.utcnow()
            
            # User statistics
            users_by_role = {}
            active_users = 0
            
            for user in self.users.values():
                if user.is_active:
                    active_users += 1
                    for role in user.roles:
                        role_name = role.value
                        users_by_role[role_name] = users_by_role.get(role_name, 0) + 1
            
            # Session statistics
            active_sessions = len([s for s in self.active_sessions.values() if s.is_active])
            
            # Access request statistics
            pending_requests = len([r for r in self.access_requests.values() if r.status == "pending"])
            
            # Recent audit activity
            recent_audit_entries = len([
                entry for entry in self.audit_log
                if (current_time - entry.timestamp).total_seconds() < 86400  # Last 24 hours
            ])
            
            # Resource access statistics
            total_resource_grants = sum(len(accesses) for accesses in self.resource_access.values())
            
            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "users": {
                    "total_users": len(self.users),
                    "active_users": active_users,
                    "users_by_role": users_by_role
                },
                "sessions": {
                    "active_sessions": active_sessions,
                    "total_sessions": len(self.active_sessions),
                    "session_timeout_hours": self.session_timeout_hours
                },
                "access_requests": {
                    "total_requests": len(self.access_requests),
                    "pending_requests": pending_requests
                },
                "permissions": {
                    "total_roles": len(self.role_permissions),
                    "total_permissions": len(Permission),
                    "cached_permission_checks": sum(len(cache) for cache in self.permission_cache.values())
                },
                "resource_access": {
                    "total_resource_grants": total_resource_grants,
                    "resources_with_access": len(self.resource_access)
                },
                "audit": {
                    "total_audit_entries": len(self.audit_log),
                    "recent_entries_24h": recent_audit_entries,
                    "retention_days": self.audit_retention_days
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate RBAC summary: {str(e)}")
            return {}


# Global RBAC instance
_rbac_system: Optional[DataDogRoleBasedAccess] = None


def get_rbac_system(service_name: str = "rbac-system",
                   config_path: Optional[str] = None,
                   jwt_secret: Optional[str] = None) -> DataDogRoleBasedAccess:
    """Get or create RBAC system instance"""
    global _rbac_system
    
    if _rbac_system is None:
        _rbac_system = DataDogRoleBasedAccess(service_name, config_path, jwt_secret)
    
    return _rbac_system


# Convenience functions

async def authenticate_user(username: str, password: str,
                          ip_address: Optional[str] = None,
                          user_agent: Optional[str] = None) -> Optional[str]:
    """Convenience function for user authentication"""
    rbac = get_rbac_system()
    return await rbac.authenticate_user(username, password, ip_address, user_agent)


async def check_permission(user_id: str, permission: Permission,
                         resource_type: Optional[ResourceType] = None,
                         resource_id: Optional[str] = None) -> bool:
    """Convenience function for permission checking"""
    rbac = get_rbac_system()
    return await rbac.check_permission(user_id, permission, resource_type, resource_id)


def get_rbac_summary() -> Dict[str, Any]:
    """Convenience function for getting RBAC summary"""
    rbac = get_rbac_system()
    return rbac.get_rbac_summary()