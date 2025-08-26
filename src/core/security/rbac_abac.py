"""
Advanced Role-Based and Attribute-Based Access Control (RBAC/ABAC)
Provides enterprise-grade authentication and authorization with MFA support.
"""
import hashlib
import json
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

import bcrypt
import pyotp
from jose import JWTError, jwt
from pydantic import BaseModel, Field

from core.logging import get_logger
from core.security.advanced_security import AuditLogger, ActionType

logger = get_logger(__name__)


class AuthenticationMethod(Enum):
    """Supported authentication methods"""
    PASSWORD = "password"
    MFA_TOTP = "mfa_totp"
    MFA_SMS = "mfa_sms"
    MFA_EMAIL = "mfa_email"
    CERTIFICATE = "certificate"
    BIOMETRIC = "biometric"
    HARDWARE_TOKEN = "hardware_token"


class PermissionType(Enum):
    """Types of permissions"""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"
    MANAGE = "manage"
    AUDIT = "audit"
    ADMIN = "admin"


class AttributeType(Enum):
    """Types of attributes for ABAC"""
    USER = "user"
    RESOURCE = "resource"
    ENVIRONMENT = "environment"
    ACTION = "action"


@dataclass
class Permission:
    """Represents a permission"""
    id: str
    name: str
    description: str
    resource_type: str
    action: PermissionType
    conditions: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class Role:
    """Represents a role with permissions"""
    id: str
    name: str
    description: str
    permissions: Set[str] = field(default_factory=set)  # Permission IDs
    parent_roles: Set[str] = field(default_factory=set)  # Inheritance
    attributes: Dict[str, Any] = field(default_factory=dict)
    is_system_role: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class User:
    """User entity with authentication and authorization data"""
    id: str
    username: str
    email: str
    password_hash: str
    roles: Set[str] = field(default_factory=set)  # Role IDs
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    # MFA settings
    mfa_enabled: bool = False
    mfa_secret: Optional[str] = None
    mfa_backup_codes: List[str] = field(default_factory=list)
    mfa_methods: Set[AuthenticationMethod] = field(default_factory=set)
    
    # Security settings
    password_expires_at: Optional[datetime] = None
    locked_until: Optional[datetime] = None
    failed_login_attempts: int = 0
    last_login: Optional[datetime] = None
    last_password_change: datetime = field(default_factory=datetime.now)
    
    # Account status
    is_active: bool = True
    is_verified: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class AccessRequest:
    """Represents an access request for ABAC evaluation"""
    user_id: str
    resource: str
    action: PermissionType
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AuthenticationResult:
    """Result of authentication attempt"""
    success: bool
    user_id: Optional[str] = None
    token: Optional[str] = None
    mfa_required: bool = False
    mfa_methods: Set[AuthenticationMethod] = field(default_factory=set)
    error_message: Optional[str] = None
    next_step: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthorizationResult:
    """Result of authorization check"""
    granted: bool
    reason: str
    matched_permissions: List[str] = field(default_factory=list)
    required_attributes: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class MFAManager:
    """Multi-Factor Authentication manager"""
    
    def __init__(self):
        self.issuer_name = "Enterprise Data Platform"
        self.backup_code_length = 8
        self.backup_code_count = 10
    
    def generate_mfa_secret(self) -> str:
        """Generate TOTP secret for user"""
        return pyotp.random_base32()
    
    def generate_qr_code_url(self, user_email: str, secret: str) -> str:
        """Generate QR code URL for TOTP setup"""
        totp = pyotp.TOTP(secret)
        return totp.provisioning_uri(
            user_email,
            issuer_name=self.issuer_name
        )
    
    def verify_totp_token(self, secret: str, token: str, window: int = 1) -> bool:
        """Verify TOTP token with time window tolerance"""
        totp = pyotp.TOTP(secret)
        return totp.verify(token, valid_window=window)
    
    def generate_backup_codes(self) -> List[str]:
        """Generate backup codes for MFA recovery"""
        codes = []
        for _ in range(self.backup_code_count):
            code = ''.join(secrets.choice('0123456789') for _ in range(self.backup_code_length))
            codes.append(code)
        return codes
    
    def hash_backup_codes(self, codes: List[str]) -> List[str]:
        """Hash backup codes for secure storage"""
        hashed_codes = []
        for code in codes:
            hashed = bcrypt.hashpw(code.encode('utf-8'), bcrypt.gensalt())
            hashed_codes.append(hashed.decode('utf-8'))
        return hashed_codes
    
    def verify_backup_code(self, code: str, hashed_codes: List[str]) -> bool:
        """Verify backup code against hashed codes"""
        for hashed_code in hashed_codes:
            if bcrypt.checkpw(code.encode('utf-8'), hashed_code.encode('utf-8')):
                return True
        return False


class RBACManager:
    """Role-Based Access Control manager"""
    
    def __init__(self):
        self.permissions: Dict[str, Permission] = {}
        self.roles: Dict[str, Role] = {}
        self.users: Dict[str, User] = {}
        
        # Initialize default system permissions and roles
        self._initialize_system_rbac()
    
    def _initialize_system_rbac(self) -> None:
        """Initialize default system permissions and roles"""
        
        # System permissions
        system_permissions = [
            Permission("perm_user_read", "Read Users", "Read user information", "user", PermissionType.READ),
            Permission("perm_user_create", "Create Users", "Create new users", "user", PermissionType.CREATE),
            Permission("perm_user_update", "Update Users", "Update user information", "user", PermissionType.UPDATE),
            Permission("perm_user_delete", "Delete Users", "Delete users", "user", PermissionType.DELETE),
            Permission("perm_user_manage", "Manage Users", "Full user management", "user", PermissionType.MANAGE),
            
            Permission("perm_role_read", "Read Roles", "Read role information", "role", PermissionType.READ),
            Permission("perm_role_create", "Create Roles", "Create new roles", "role", PermissionType.CREATE),
            Permission("perm_role_update", "Update Roles", "Update role information", "role", PermissionType.UPDATE),
            Permission("perm_role_delete", "Delete Roles", "Delete roles", "role", PermissionType.DELETE),
            Permission("perm_role_manage", "Manage Roles", "Full role management", "role", PermissionType.MANAGE),
            
            Permission("perm_data_read", "Read Data", "Read data resources", "data", PermissionType.READ),
            Permission("perm_data_create", "Create Data", "Create data resources", "data", PermissionType.CREATE),
            Permission("perm_data_update", "Update Data", "Update data resources", "data", PermissionType.UPDATE),
            Permission("perm_data_delete", "Delete Data", "Delete data resources", "data", PermissionType.DELETE),
            
            Permission("perm_system_admin", "System Admin", "Full system administration", "system", PermissionType.ADMIN),
            Permission("perm_audit_view", "View Audits", "View audit logs", "audit", PermissionType.AUDIT),
        ]
        
        for perm in system_permissions:
            self.permissions[perm.id] = perm
        
        # System roles
        admin_role = Role(
            id="role_admin",
            name="Administrator",
            description="Full system administrator",
            permissions={
                "perm_user_manage", "perm_role_manage", "perm_data_read", 
                "perm_data_create", "perm_data_update", "perm_data_delete", 
                "perm_system_admin", "perm_audit_view"
            },
            is_system_role=True
        )
        
        user_manager_role = Role(
            id="role_user_manager",
            name="User Manager",
            description="Manages users and basic roles",
            permissions={
                "perm_user_read", "perm_user_create", "perm_user_update",
                "perm_role_read", "perm_data_read"
            },
            is_system_role=True
        )
        
        data_analyst_role = Role(
            id="role_data_analyst",
            name="Data Analyst",
            description="Analyzes data with read access",
            permissions={"perm_data_read", "perm_audit_view"},
            is_system_role=True
        )
        
        data_engineer_role = Role(
            id="role_data_engineer",
            name="Data Engineer",
            description="Manages data pipelines and transformations",
            permissions={
                "perm_data_read", "perm_data_create", "perm_data_update",
                "perm_audit_view"
            },
            is_system_role=True
        )
        
        viewer_role = Role(
            id="role_viewer",
            name="Viewer",
            description="Read-only access to data",
            permissions={"perm_data_read"},
            is_system_role=True
        )
        
        for role in [admin_role, user_manager_role, data_analyst_role, data_engineer_role, viewer_role]:
            self.roles[role.id] = role
    
    def create_permission(self, permission: Permission) -> bool:
        """Create a new permission"""
        if permission.id in self.permissions:
            return False
        
        self.permissions[permission.id] = permission
        logger.info(f"Created permission: {permission.name} ({permission.id})")
        return True
    
    def create_role(self, role: Role) -> bool:
        """Create a new role"""
        if role.id in self.roles:
            return False
        
        # Validate permissions exist
        for perm_id in role.permissions:
            if perm_id not in self.permissions:
                raise ValueError(f"Permission {perm_id} does not exist")
        
        role.updated_at = datetime.now()
        self.roles[role.id] = role
        logger.info(f"Created role: {role.name} ({role.id})")
        return True
    
    def assign_role_to_user(self, user_id: str, role_id: str) -> bool:
        """Assign role to user"""
        if user_id not in self.users or role_id not in self.roles:
            return False
        
        self.users[user_id].roles.add(role_id)
        self.users[user_id].updated_at = datetime.now()
        logger.info(f"Assigned role {role_id} to user {user_id}")
        return True
    
    def revoke_role_from_user(self, user_id: str, role_id: str) -> bool:
        """Revoke role from user"""
        if user_id not in self.users:
            return False
        
        if role_id in self.users[user_id].roles:
            self.users[user_id].roles.remove(role_id)
            self.users[user_id].updated_at = datetime.now()
            logger.info(f"Revoked role {role_id} from user {user_id}")
            return True
        
        return False
    
    def get_user_permissions(self, user_id: str) -> Set[str]:
        """Get all permissions for a user (including inherited)"""
        if user_id not in self.users:
            return set()
        
        user = self.users[user_id]
        permissions = set()
        
        # Get direct role permissions
        for role_id in user.roles:
            if role_id in self.roles:
                role = self.roles[role_id]
                permissions.update(role.permissions)
                
                # Get inherited permissions from parent roles
                permissions.update(self._get_inherited_permissions(role_id))
        
        return permissions
    
    def _get_inherited_permissions(self, role_id: str, visited: Set[str] = None) -> Set[str]:
        """Get permissions inherited from parent roles (recursive)"""
        if visited is None:
            visited = set()
        
        if role_id in visited or role_id not in self.roles:
            return set()
        
        visited.add(role_id)
        role = self.roles[role_id]
        permissions = set()
        
        for parent_role_id in role.parent_roles:
            if parent_role_id in self.roles:
                parent_role = self.roles[parent_role_id]
                permissions.update(parent_role.permissions)
                permissions.update(self._get_inherited_permissions(parent_role_id, visited))
        
        return permissions
    
    def check_permission(self, user_id: str, permission_id: str) -> bool:
        """Check if user has specific permission"""
        user_permissions = self.get_user_permissions(user_id)
        return permission_id in user_permissions
    
    def check_resource_access(self, user_id: str, resource_type: str, action: PermissionType) -> bool:
        """Check if user can perform action on resource type"""
        user_permissions = self.get_user_permissions(user_id)
        
        # Check for specific resource-action permission
        for perm_id in user_permissions:
            if perm_id in self.permissions:
                perm = self.permissions[perm_id]
                if perm.resource_type == resource_type and perm.action == action:
                    return True
                
                # Check for broader permissions (MANAGE, ADMIN)
                if (perm.resource_type == resource_type and 
                    perm.action in [PermissionType.MANAGE, PermissionType.ADMIN]):
                    return True
        
        return False


class ABACEngine:
    """Attribute-Based Access Control engine"""
    
    def __init__(self, rbac_manager: RBACManager):
        self.rbac_manager = rbac_manager
        self.attribute_policies: List[Dict[str, Any]] = []
        self._initialize_default_policies()
    
    def _initialize_default_policies(self) -> None:
        """Initialize default ABAC policies"""
        
        # Time-based access policy
        time_policy = {
            "id": "policy_business_hours",
            "name": "Business Hours Access",
            "description": "Restrict sensitive operations to business hours",
            "condition": {
                "and": [
                    {
                        "attribute": "environment.time.hour",
                        "operator": "between",
                        "value": [9, 17]
                    },
                    {
                        "attribute": "resource.sensitivity",
                        "operator": "in",
                        "value": ["confidential", "restricted"]
                    }
                ]
            },
            "effect": "allow",
            "priority": 10
        }
        
        # Location-based access policy
        location_policy = {
            "id": "policy_geo_restriction",
            "name": "Geographic Access Control",
            "description": "Restrict access based on geographic location",
            "condition": {
                "and": [
                    {
                        "attribute": "user.location.country",
                        "operator": "in",
                        "value": ["US", "CA", "GB"]
                    },
                    {
                        "attribute": "resource.type",
                        "operator": "equals",
                        "value": "sensitive_data"
                    }
                ]
            },
            "effect": "allow",
            "priority": 20
        }
        
        # Device compliance policy
        device_policy = {
            "id": "policy_device_compliance",
            "name": "Device Compliance Check",
            "description": "Require compliant devices for administrative access",
            "condition": {
                "and": [
                    {
                        "attribute": "user.device.compliant",
                        "operator": "equals",
                        "value": True
                    },
                    {
                        "attribute": "action.type",
                        "operator": "in",
                        "value": ["admin", "manage"]
                    }
                ]
            },
            "effect": "require",
            "priority": 5
        }
        
        self.attribute_policies.extend([time_policy, location_policy, device_policy])
    
    def evaluate_access(self, request: AccessRequest, attributes: Dict[str, Any]) -> AuthorizationResult:
        """Evaluate access request using ABAC policies"""
        
        # First check RBAC permissions
        rbac_result = self._check_rbac_access(request)
        if not rbac_result.granted:
            return rbac_result
        
        # Then evaluate ABAC policies
        abac_result = self._evaluate_abac_policies(request, attributes)
        
        # Combine results
        final_granted = rbac_result.granted and abac_result.granted
        
        return AuthorizationResult(
            granted=final_granted,
            reason=f"RBAC: {rbac_result.reason}, ABAC: {abac_result.reason}",
            matched_permissions=rbac_result.matched_permissions,
            required_attributes=abac_result.required_attributes,
            metadata={
                "rbac_result": rbac_result.metadata,
                "abac_result": abac_result.metadata
            }
        )
    
    def _check_rbac_access(self, request: AccessRequest) -> AuthorizationResult:
        """Check RBAC permissions"""
        
        # Get resource type from request
        resource_type = request.context.get("resource_type", "unknown")
        
        # Check if user has permission
        has_access = self.rbac_manager.check_resource_access(
            request.user_id, resource_type, request.action
        )
        
        if has_access:
            # Get matching permissions
            user_permissions = self.rbac_manager.get_user_permissions(request.user_id)
            matched = []
            for perm_id in user_permissions:
                if perm_id in self.rbac_manager.permissions:
                    perm = self.rbac_manager.permissions[perm_id]
                    if perm.resource_type == resource_type and perm.action == request.action:
                        matched.append(perm_id)
            
            return AuthorizationResult(
                granted=True,
                reason="RBAC permissions granted",
                matched_permissions=matched,
                metadata={"user_permissions": len(user_permissions)}
            )
        else:
            return AuthorizationResult(
                granted=False,
                reason="Insufficient RBAC permissions",
                metadata={"required_action": request.action.value, "resource_type": resource_type}
            )
    
    def _evaluate_abac_policies(self, request: AccessRequest, attributes: Dict[str, Any]) -> AuthorizationResult:
        """Evaluate ABAC policies"""
        
        applicable_policies = []
        policy_results = []
        
        # Sort policies by priority (lower number = higher priority)
        sorted_policies = sorted(self.attribute_policies, key=lambda p: p.get("priority", 100))
        
        for policy in sorted_policies:
            if self._evaluate_policy_condition(policy["condition"], attributes):
                applicable_policies.append(policy)
                policy_results.append({
                    "policy_id": policy["id"],
                    "effect": policy["effect"],
                    "matched": True
                })
        
        # Determine final decision based on policy effects
        if not applicable_policies:
            return AuthorizationResult(
                granted=True,
                reason="No applicable ABAC policies",
                metadata={"evaluated_policies": len(self.attribute_policies)}
            )
        
        # Check for deny policies (highest priority)
        deny_policies = [p for p in applicable_policies if p["effect"] == "deny"]
        if deny_policies:
            return AuthorizationResult(
                granted=False,
                reason=f"Denied by policy: {deny_policies[0]['name']}",
                metadata={
                    "policy_results": policy_results,
                    "deny_policy": deny_policies[0]["id"]
                }
            )
        
        # Check for require policies
        require_policies = [p for p in applicable_policies if p["effect"] == "require"]
        if require_policies:
            # Check if requirements are met
            for policy in require_policies:
                if not self._check_policy_requirements(policy, attributes):
                    return AuthorizationResult(
                        granted=False,
                        reason=f"Requirements not met for policy: {policy['name']}",
                        required_attributes=self._extract_required_attributes(policy),
                        metadata={
                            "policy_results": policy_results,
                            "failed_requirement": policy["id"]
                        }
                    )
        
        # Allow policies
        allow_policies = [p for p in applicable_policies if p["effect"] == "allow"]
        if allow_policies:
            return AuthorizationResult(
                granted=True,
                reason="Allowed by ABAC policies",
                metadata={
                    "policy_results": policy_results,
                    "allow_policies": [p["id"] for p in allow_policies]
                }
            )
        
        # Default deny if no explicit allow
        return AuthorizationResult(
            granted=False,
            reason="No explicit allow policy matched",
            metadata={"policy_results": policy_results}
        )
    
    def _evaluate_policy_condition(self, condition: Dict[str, Any], attributes: Dict[str, Any]) -> bool:
        """Evaluate policy condition against attributes"""
        
        if "and" in condition:
            return all(self._evaluate_policy_condition(sub_cond, attributes) 
                      for sub_cond in condition["and"])
        
        if "or" in condition:
            return any(self._evaluate_policy_condition(sub_cond, attributes) 
                      for sub_cond in condition["or"])
        
        if "not" in condition:
            return not self._evaluate_policy_condition(condition["not"], attributes)
        
        # Simple attribute condition
        if "attribute" in condition:
            attr_path = condition["attribute"]
            operator = condition["operator"]
            expected_value = condition["value"]
            
            actual_value = self._get_nested_attribute(attributes, attr_path)
            
            return self._apply_operator(actual_value, operator, expected_value)
        
        return False
    
    def _get_nested_attribute(self, attributes: Dict[str, Any], path: str) -> Any:
        """Get nested attribute value using dot notation"""
        keys = path.split('.')
        value = attributes
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _apply_operator(self, actual: Any, operator: str, expected: Any) -> bool:
        """Apply comparison operator"""
        
        if operator == "equals":
            return actual == expected
        elif operator == "not_equals":
            return actual != expected
        elif operator == "in":
            return actual in expected if isinstance(expected, (list, set)) else False
        elif operator == "not_in":
            return actual not in expected if isinstance(expected, (list, set)) else True
        elif operator == "greater_than":
            return actual > expected
        elif operator == "less_than":
            return actual < expected
        elif operator == "greater_equal":
            return actual >= expected
        elif operator == "less_equal":
            return actual <= expected
        elif operator == "between":
            return expected[0] <= actual <= expected[1] if len(expected) == 2 else False
        elif operator == "contains":
            return expected in str(actual)
        elif operator == "regex":
            import re
            return bool(re.match(expected, str(actual)))
        
        return False
    
    def _check_policy_requirements(self, policy: Dict[str, Any], attributes: Dict[str, Any]) -> bool:
        """Check if policy requirements are satisfied"""
        # For require policies, the condition itself defines the requirement
        return self._evaluate_policy_condition(policy["condition"], attributes)
    
    def _extract_required_attributes(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        """Extract required attributes from policy"""
        required = {}
        
        def extract_from_condition(condition):
            if "attribute" in condition:
                attr_path = condition["attribute"]
                required[attr_path] = condition["value"]
            
            for key in ["and", "or"]:
                if key in condition:
                    for sub_cond in condition[key]:
                        extract_from_condition(sub_cond)
        
        extract_from_condition(policy["condition"])
        return required


class AuthenticationManager:
    """Comprehensive authentication manager with MFA support"""
    
    def __init__(self):
        self.rbac_manager = RBACManager()
        self.mfa_manager = MFAManager()
        self.abac_engine = ABACEngine(self.rbac_manager)
        self.audit_logger = AuditLogger()
        
        # JWT settings
        self.jwt_secret = secrets.token_urlsafe(32)
        self.jwt_algorithm = "HS256"
        self.jwt_expiration = timedelta(hours=1)
        
        # Security settings
        self.max_failed_attempts = 5
        self.lockout_duration = timedelta(minutes=30)
        self.password_min_length = 12
    
    def create_user(self, username: str, email: str, password: str, 
                   roles: List[str] = None) -> str:
        """Create a new user"""
        
        # Validate password strength
        if not self._validate_password_strength(password):
            raise ValueError("Password does not meet security requirements")
        
        # Generate user ID
        user_id = f"user_{secrets.token_hex(8)}"
        
        # Hash password
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Create user
        user = User(
            id=user_id,
            username=username,
            email=email,
            password_hash=password_hash,
            roles=set(roles) if roles else {"role_viewer"},  # Default viewer role
            password_expires_at=datetime.now() + timedelta(days=90)
        )
        
        self.rbac_manager.users[user_id] = user
        
        # Log user creation
        self.audit_logger.log_audit_event(
            user_id="system",
            action=ActionType.CREATE,
            resource_type="user",
            resource_id=user_id,
            new_value={"username": username, "email": email, "roles": list(user.roles)},
            success=True
        )
        
        logger.info(f"Created user: {username} ({user_id})")
        return user_id
    
    def authenticate(self, username: str, password: str, 
                    context: Dict[str, Any] = None) -> AuthenticationResult:
        """Authenticate user with username and password"""
        
        context = context or {}
        
        # Find user
        user = self._find_user_by_username(username)
        if not user:
            self._log_failed_authentication(username, "user_not_found", context)
            return AuthenticationResult(
                success=False,
                error_message="Invalid credentials"
            )
        
        # Check if user is locked
        if user.locked_until and datetime.now() < user.locked_until:
            self._log_failed_authentication(username, "account_locked", context)
            return AuthenticationResult(
                success=False,
                error_message="Account is locked"
            )
        
        # Check if user is active
        if not user.is_active:
            self._log_failed_authentication(username, "account_inactive", context)
            return AuthenticationResult(
                success=False,
                error_message="Account is inactive"
            )
        
        # Verify password
        if not bcrypt.checkpw(password.encode('utf-8'), user.password_hash.encode('utf-8')):
            user.failed_login_attempts += 1
            
            # Lock account if too many failed attempts
            if user.failed_login_attempts >= self.max_failed_attempts:
                user.locked_until = datetime.now() + self.lockout_duration
                logger.warning(f"Account locked due to failed attempts: {username}")
            
            self._log_failed_authentication(username, "invalid_password", context)
            return AuthenticationResult(
                success=False,
                error_message="Invalid credentials"
            )
        
        # Check password expiration
        if user.password_expires_at and datetime.now() > user.password_expires_at:
            return AuthenticationResult(
                success=False,
                error_message="Password has expired",
                next_step="password_reset"
            )
        
        # Reset failed attempts on successful password verification
        user.failed_login_attempts = 0
        user.locked_until = None
        
        # Check if MFA is required
        if user.mfa_enabled:
            return AuthenticationResult(
                success=False,
                user_id=user.id,
                mfa_required=True,
                mfa_methods=user.mfa_methods,
                next_step="mfa_verification"
            )
        
        # Generate JWT token
        token = self._generate_jwt_token(user)
        
        # Update login timestamp
        user.last_login = datetime.now()
        
        # Log successful authentication
        self.audit_logger.log_audit_event(
            user_id=user.id,
            action=ActionType.LOGIN,
            resource_type="authentication",
            success=True,
            metadata=context
        )
        
        logger.info(f"User authenticated successfully: {username}")
        return AuthenticationResult(
            success=True,
            user_id=user.id,
            token=token
        )
    
    def verify_mfa(self, user_id: str, mfa_code: str, 
                  method: AuthenticationMethod = AuthenticationMethod.MFA_TOTP) -> AuthenticationResult:
        """Verify MFA code"""
        
        user = self.rbac_manager.users.get(user_id)
        if not user:
            return AuthenticationResult(
                success=False,
                error_message="User not found"
            )
        
        if method == AuthenticationMethod.MFA_TOTP:
            if not user.mfa_secret:
                return AuthenticationResult(
                    success=False,
                    error_message="TOTP not configured"
                )
            
            if not self.mfa_manager.verify_totp_token(user.mfa_secret, mfa_code):
                self._log_failed_authentication(user.username, "invalid_mfa", {"method": method.value})
                return AuthenticationResult(
                    success=False,
                    error_message="Invalid MFA code"
                )
        
        elif method == AuthenticationMethod.MFA_SMS:
            # Implement SMS verification logic
            pass
        
        elif method == AuthenticationMethod.MFA_EMAIL:
            # Implement email verification logic
            pass
        
        # Generate JWT token
        token = self._generate_jwt_token(user)
        
        # Update login timestamp
        user.last_login = datetime.now()
        
        # Log successful MFA verification
        self.audit_logger.log_audit_event(
            user_id=user.id,
            action=ActionType.LOGIN,
            resource_type="mfa_verification",
            success=True,
            metadata={"method": method.value}
        )
        
        logger.info(f"MFA verification successful for user: {user.username}")
        return AuthenticationResult(
            success=True,
            user_id=user.id,
            token=token
        )
    
    def setup_mfa(self, user_id: str, method: AuthenticationMethod = AuthenticationMethod.MFA_TOTP) -> Dict[str, Any]:
        """Setup MFA for user"""
        
        user = self.rbac_manager.users.get(user_id)
        if not user:
            raise ValueError("User not found")
        
        if method == AuthenticationMethod.MFA_TOTP:
            # Generate TOTP secret
            secret = self.mfa_manager.generate_mfa_secret()
            qr_url = self.mfa_manager.generate_qr_code_url(user.email, secret)
            
            # Generate backup codes
            backup_codes = self.mfa_manager.generate_backup_codes()
            hashed_codes = self.mfa_manager.hash_backup_codes(backup_codes)
            
            # Update user
            user.mfa_secret = secret
            user.mfa_backup_codes = hashed_codes
            user.mfa_methods.add(method)
            user.updated_at = datetime.now()
            
            logger.info(f"TOTP MFA setup initiated for user: {user.username}")
            
            return {
                "method": method.value,
                "secret": secret,
                "qr_code_url": qr_url,
                "backup_codes": backup_codes  # Return unhashed codes to user
            }
        
        # Add other MFA methods as needed
        raise ValueError(f"MFA method {method.value} not implemented")
    
    def enable_mfa(self, user_id: str, verification_code: str, 
                  method: AuthenticationMethod = AuthenticationMethod.MFA_TOTP) -> bool:
        """Enable MFA after successful verification"""
        
        user = self.rbac_manager.users.get(user_id)
        if not user:
            return False
        
        # Verify the code
        if method == AuthenticationMethod.MFA_TOTP:
            if not user.mfa_secret:
                return False
            
            if not self.mfa_manager.verify_totp_token(user.mfa_secret, verification_code):
                return False
        
        # Enable MFA
        user.mfa_enabled = True
        user.updated_at = datetime.now()
        
        # Log MFA enablement
        self.audit_logger.log_audit_event(
            user_id=user.id,
            action=ActionType.UPDATE,
            resource_type="user_security",
            resource_id=user.id,
            new_value={"mfa_enabled": True, "mfa_method": method.value},
            success=True
        )
        
        logger.info(f"MFA enabled for user: {user.username}")
        return True
    
    def authorize(self, user_id: str, resource: str, action: PermissionType, 
                 context: Dict[str, Any] = None) -> AuthorizationResult:
        """Authorize user access to resource"""
        
        context = context or {}
        
        # Create access request
        request = AccessRequest(
            user_id=user_id,
            resource=resource,
            action=action,
            context=context
        )
        
        # Prepare attributes for ABAC evaluation
        user = self.rbac_manager.users.get(user_id)
        if not user:
            return AuthorizationResult(
                granted=False,
                reason="User not found"
            )
        
        attributes = {
            "user": {
                "id": user.id,
                "username": user.username,
                "roles": list(user.roles),
                "attributes": user.attributes,
                "mfa_enabled": user.mfa_enabled,
                "location": context.get("user_location", {}),
                "device": context.get("device_info", {})
            },
            "resource": {
                "name": resource,
                "type": context.get("resource_type", "unknown"),
                "sensitivity": context.get("resource_sensitivity", "internal")
            },
            "environment": {
                "time": {
                    "hour": datetime.now().hour,
                    "day_of_week": datetime.now().weekday()
                },
                "ip": context.get("source_ip", "unknown")
            },
            "action": {
                "type": action.value
            }
        }
        
        # Evaluate access using ABAC engine
        result = self.abac_engine.evaluate_access(request, attributes)
        
        # Log authorization decision
        self.audit_logger.log_audit_event(
            user_id=user.id,
            action=ActionType.READ,  # Authorization check is a read operation
            resource_type="authorization",
            resource_id=resource,
            success=result.granted,
            metadata={
                "requested_action": action.value,
                "decision_reason": result.reason,
                "matched_permissions": result.matched_permissions
            }
        )
        
        return result
    
    def _find_user_by_username(self, username: str) -> Optional[User]:
        """Find user by username"""
        for user in self.rbac_manager.users.values():
            if user.username == username:
                return user
        return None
    
    def _validate_password_strength(self, password: str) -> bool:
        """Validate password meets security requirements"""
        if len(password) < self.password_min_length:
            return False
        
        # Check for required character types
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password)
        
        return has_upper and has_lower and has_digit and has_special
    
    def _generate_jwt_token(self, user: User) -> str:
        """Generate JWT token for user"""
        payload = {
            "user_id": user.id,
            "username": user.username,
            "roles": list(user.roles),
            "iat": datetime.utcnow(),
            "exp": datetime.utcnow() + self.jwt_expiration
        }
        
        return jwt.encode(payload, self.jwt_secret, algorithm=self.jwt_algorithm)
    
    def verify_jwt_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, self.jwt_secret, algorithms=[self.jwt_algorithm])
            return payload
        except JWTError as e:
            logger.warning(f"JWT verification failed: {e}")
            raise ValueError("Invalid token")
    
    def _log_failed_authentication(self, username: str, reason: str, context: Dict[str, Any]) -> None:
        """Log failed authentication attempt"""
        self.audit_logger.log_audit_event(
            user_id=username,  # Use username since we don't have user ID
            action=ActionType.LOGIN,
            resource_type="authentication",
            success=False,
            error_message=reason,
            metadata=context
        )
        
        logger.warning(f"Failed authentication attempt for {username}: {reason}")


# Global authentication manager instance
_auth_manager: Optional[AuthenticationManager] = None


def get_authentication_manager() -> AuthenticationManager:
    """Get global authentication manager instance"""
    global _auth_manager
    if _auth_manager is None:
        _auth_manager = AuthenticationManager()
    return _auth_manager
