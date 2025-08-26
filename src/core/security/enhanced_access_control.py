"""
Enhanced Access Control System
Provides fine-grained Role-Based Access Control (RBAC) and Attribute-Based Access Control (ABAC)
with dynamic policy evaluation, privileged access management, and comprehensive audit trails.
"""
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union, Callable
from collections import defaultdict

from pydantic import BaseModel, Field

from core.logging import get_logger
from core.security.advanced_security import AuditLogger, ActionType


logger = get_logger(__name__)


class AccessDecision(Enum):
    """Access control decisions"""
    ALLOW = "allow"
    DENY = "deny"
    CONDITIONAL_ALLOW = "conditional_allow"
    REQUIRES_ELEVATION = "requires_elevation"


class AttributeType(Enum):
    """Types of attributes for ABAC"""
    SUBJECT = "subject"  # User attributes
    RESOURCE = "resource"  # Resource attributes
    ACTION = "action"  # Action attributes
    ENVIRONMENT = "environment"  # Environment attributes


class PermissionScope(Enum):
    """Permission scopes"""
    GLOBAL = "global"
    ORGANIZATION = "organization" 
    DEPARTMENT = "department"
    PROJECT = "project"
    RESOURCE = "resource"


class ElevationReason(Enum):
    """Reasons for privilege elevation"""
    ADMIN_TASK = "admin_task"
    EMERGENCY_ACCESS = "emergency_access"
    MAINTENANCE = "maintenance"
    COMPLIANCE_AUDIT = "compliance_audit"
    DATA_RECOVERY = "data_recovery"


@dataclass
class Permission:
    """Individual permission definition"""
    permission_id: str
    name: str
    description: str
    action: str
    resource_type: str
    scope: PermissionScope
    conditions: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Role:
    """Role definition with permissions"""
    role_id: str
    name: str
    description: str
    permissions: Set[str] = field(default_factory=set)
    parent_roles: Set[str] = field(default_factory=set)  # Role hierarchy
    conditions: Dict[str, Any] = field(default_factory=dict)
    is_privileged: bool = False
    max_elevation_duration: Optional[timedelta] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Subject:
    """Subject (user/service) with attributes"""
    subject_id: str
    subject_type: str  # user, service, group
    attributes: Dict[str, Any] = field(default_factory=dict)
    roles: Set[str] = field(default_factory=set)
    temporary_roles: Dict[str, datetime] = field(default_factory=dict)  # Role -> expiration
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    last_access: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Resource:
    """Resource with attributes"""
    resource_id: str
    resource_type: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    owner_id: Optional[str] = None
    access_classification: str = "internal"  # public, internal, confidential, restricted
    inherited_permissions: Dict[str, Set[str]] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyRule:
    """ABAC policy rule"""
    rule_id: str
    name: str
    description: str
    priority: int  # Higher number = higher priority
    effect: AccessDecision
    subject_conditions: Dict[str, Any] = field(default_factory=dict)
    resource_conditions: Dict[str, Any] = field(default_factory=dict)
    action_conditions: Dict[str, Any] = field(default_factory=dict)
    environment_conditions: Dict[str, Any] = field(default_factory=dict)
    time_constraints: Optional[Dict[str, Any]] = None
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AccessRequest:
    """Access request for evaluation"""
    request_id: str
    subject_id: str
    action: str
    resource_id: str
    resource_type: str
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    session_id: Optional[str] = None
    source_ip: Optional[str] = None


@dataclass
class AccessEvaluation:
    """Result of access evaluation"""
    request_id: str
    decision: AccessDecision
    reason: str
    applicable_rules: List[str] = field(default_factory=list)
    required_conditions: List[str] = field(default_factory=list)
    evaluation_time_ms: float = 0.0
    risk_score: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PrivilegeElevation:
    """Privilege elevation request"""
    elevation_id: str
    subject_id: str
    requested_role: str
    reason: ElevationReason
    justification: str
    requested_duration: timedelta
    approved: bool = False
    approver_id: Optional[str] = None
    approval_time: Optional[datetime] = None
    expiration_time: Optional[datetime] = None
    is_active: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConditionEvaluator:
    """Evaluates conditions for ABAC policies"""
    
    def __init__(self):
        self.operators = {
            'eq': self._op_equals,
            'ne': self._op_not_equals,
            'gt': self._op_greater_than,
            'gte': self._op_greater_equal,
            'lt': self._op_less_than,
            'lte': self._op_less_equal,
            'in': self._op_in,
            'not_in': self._op_not_in,
            'contains': self._op_contains,
            'not_contains': self._op_not_contains,
            'starts_with': self._op_starts_with,
            'ends_with': self._op_ends_with,
            'regex': self._op_regex,
            'exists': self._op_exists,
            'not_exists': self._op_not_exists,
            'time_between': self._op_time_between,
            'day_of_week': self._op_day_of_week,
            'ip_range': self._op_ip_range
        }
    
    def evaluate_conditions(
        self, 
        conditions: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> bool:
        """Evaluate a set of conditions against context"""
        
        if not conditions:
            return True
        
        # Handle logical operators
        if 'and' in conditions:
            return all(self.evaluate_conditions(cond, context) for cond in conditions['and'])
        
        if 'or' in conditions:
            return any(self.evaluate_conditions(cond, context) for cond in conditions['or'])
        
        if 'not' in conditions:
            return not self.evaluate_conditions(conditions['not'], context)
        
        # Evaluate individual conditions
        for attr_path, condition_spec in conditions.items():
            if attr_path in ['and', 'or', 'not']:
                continue
            
            if not self._evaluate_single_condition(attr_path, condition_spec, context):
                return False
        
        return True
    
    def _evaluate_single_condition(
        self, 
        attr_path: str, 
        condition_spec: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> bool:
        """Evaluate a single condition"""
        
        # Get actual value from context
        actual_value = self._get_nested_value(context, attr_path)
        
        # Handle different condition formats
        if isinstance(condition_spec, dict):
            for operator, expected_value in condition_spec.items():
                if operator in self.operators:
                    if not self.operators[operator](actual_value, expected_value):
                        return False
        else:
            # Direct value comparison
            return actual_value == condition_spec
        
        return True
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested value from dictionary using dot notation"""
        keys = path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _op_equals(self, actual: Any, expected: Any) -> bool:
        return actual == expected
    
    def _op_not_equals(self, actual: Any, expected: Any) -> bool:
        return actual != expected
    
    def _op_greater_than(self, actual: Any, expected: Any) -> bool:
        return actual is not None and actual > expected
    
    def _op_greater_equal(self, actual: Any, expected: Any) -> bool:
        return actual is not None and actual >= expected
    
    def _op_less_than(self, actual: Any, expected: Any) -> bool:
        return actual is not None and actual < expected
    
    def _op_less_equal(self, actual: Any, expected: Any) -> bool:
        return actual is not None and actual <= expected
    
    def _op_in(self, actual: Any, expected: List[Any]) -> bool:
        return actual in expected
    
    def _op_not_in(self, actual: Any, expected: List[Any]) -> bool:
        return actual not in expected
    
    def _op_contains(self, actual: Any, expected: Any) -> bool:
        return isinstance(actual, (str, list)) and expected in actual
    
    def _op_not_contains(self, actual: Any, expected: Any) -> bool:
        return not (isinstance(actual, (str, list)) and expected in actual)
    
    def _op_starts_with(self, actual: Any, expected: Any) -> bool:
        return isinstance(actual, str) and actual.startswith(expected)
    
    def _op_ends_with(self, actual: Any, expected: Any) -> bool:
        return isinstance(actual, str) and actual.endswith(expected)
    
    def _op_regex(self, actual: Any, expected: str) -> bool:
        import re
        return isinstance(actual, str) and re.match(expected, actual) is not None
    
    def _op_exists(self, actual: Any, expected: bool) -> bool:
        return (actual is not None) == expected
    
    def _op_not_exists(self, actual: Any, expected: bool) -> bool:
        return (actual is None) == expected
    
    def _op_time_between(self, actual: Any, expected: Dict[str, str]) -> bool:
        if not isinstance(actual, datetime):
            return False
        
        start_time = datetime.fromisoformat(expected['start'])
        end_time = datetime.fromisoformat(expected['end'])
        return start_time <= actual <= end_time
    
    def _op_day_of_week(self, actual: Any, expected: List[int]) -> bool:
        if not isinstance(actual, datetime):
            actual = datetime.now()
        return actual.weekday() in expected
    
    def _op_ip_range(self, actual: Any, expected: str) -> bool:
        import ipaddress
        try:
            network = ipaddress.ip_network(expected)
            ip = ipaddress.ip_address(actual)
            return ip in network
        except:
            return False


class RBACEngine:
    """Role-Based Access Control engine"""
    
    def __init__(self, audit_logger: Optional[AuditLogger] = None):
        self.logger = get_logger(__name__)
        self.audit_logger = audit_logger or AuditLogger()
        
        self.permissions: Dict[str, Permission] = {}
        self.roles: Dict[str, Role] = {}
        self.subjects: Dict[str, Subject] = {}
        self.resources: Dict[str, Resource] = {}
        
        self._initialize_default_permissions_and_roles()
    
    def _initialize_default_permissions_and_roles(self):
        """Initialize default permissions and roles"""
        
        # Define core permissions
        core_permissions = [
            Permission("perm_read_data", "Read Data", "Read access to data", "read", "data", PermissionScope.RESOURCE),
            Permission("perm_write_data", "Write Data", "Write access to data", "write", "data", PermissionScope.RESOURCE),
            Permission("perm_delete_data", "Delete Data", "Delete access to data", "delete", "data", PermissionScope.RESOURCE),
            Permission("perm_admin_users", "Administer Users", "Manage user accounts", "admin", "user", PermissionScope.GLOBAL),
            Permission("perm_admin_system", "System Administration", "System administration", "admin", "system", PermissionScope.GLOBAL),
            Permission("perm_audit_logs", "Access Audit Logs", "View audit logs", "read", "audit_log", PermissionScope.GLOBAL),
            Permission("perm_compliance_report", "Compliance Reporting", "Generate compliance reports", "read", "compliance", PermissionScope.ORGANIZATION)
        ]
        
        for perm in core_permissions:
            self.permissions[perm.permission_id] = perm
        
        # Define default roles
        default_roles = [
            Role(
                role_id="role_viewer",
                name="Viewer",
                description="Read-only access to data",
                permissions={"perm_read_data"}
            ),
            Role(
                role_id="role_analyst",
                name="Data Analyst",
                description="Read and analyze data",
                permissions={"perm_read_data"}
            ),
            Role(
                role_id="role_engineer",
                name="Data Engineer",
                description="Full data access for engineering tasks",
                permissions={"perm_read_data", "perm_write_data"},
                parent_roles={"role_analyst"}
            ),
            Role(
                role_id="role_admin",
                name="Administrator",
                description="Full system administration",
                permissions={"perm_admin_users", "perm_admin_system", "perm_audit_logs"},
                is_privileged=True,
                max_elevation_duration=timedelta(hours=8)
            ),
            Role(
                role_id="role_compliance_officer",
                name="Compliance Officer",
                description="Compliance monitoring and reporting",
                permissions={"perm_read_data", "perm_audit_logs", "perm_compliance_report"}
            )
        ]
        
        for role in default_roles:
            self.roles[role.role_id] = role
    
    def add_permission(self, permission: Permission) -> None:
        """Add a permission"""
        self.permissions[permission.permission_id] = permission
        self.logger.info(f"Added permission: {permission.name}")
    
    def add_role(self, role: Role) -> None:
        """Add a role"""
        self.roles[role.role_id] = role
        role.updated_at = datetime.now()
        self.logger.info(f"Added role: {role.name}")
    
    def add_subject(self, subject: Subject) -> None:
        """Add a subject"""
        self.subjects[subject.subject_id] = subject
        self.logger.info(f"Added subject: {subject.subject_id}")
    
    def assign_role_to_subject(
        self, 
        subject_id: str, 
        role_id: str, 
        temporary: bool = False,
        duration: Optional[timedelta] = None
    ) -> bool:
        """Assign role to subject"""
        
        if subject_id not in self.subjects or role_id not in self.roles:
            return False
        
        subject = self.subjects[subject_id]
        
        if temporary and duration:
            expiration = datetime.now() + duration
            subject.temporary_roles[role_id] = expiration
        else:
            subject.roles.add(role_id)
        
        # Audit log
        self.audit_logger.log_audit_event(
            user_id="system",
            action=ActionType.UPDATE,
            resource_type="subject_role",
            resource_id=subject_id,
            new_value={"role_assigned": role_id, "temporary": temporary},
            metadata={"duration_hours": duration.total_seconds() / 3600 if duration else None}
        )
        
        self.logger.info(f"Assigned role {role_id} to subject {subject_id}")
        return True
    
    def revoke_role_from_subject(self, subject_id: str, role_id: str) -> bool:
        """Revoke role from subject"""
        
        if subject_id not in self.subjects:
            return False
        
        subject = self.subjects[subject_id]
        
        # Remove from permanent roles
        subject.roles.discard(role_id)
        
        # Remove from temporary roles
        if role_id in subject.temporary_roles:
            del subject.temporary_roles[role_id]
        
        # Audit log
        self.audit_logger.log_audit_event(
            user_id="system",
            action=ActionType.UPDATE,
            resource_type="subject_role",
            resource_id=subject_id,
            old_value={"role_assigned": role_id},
            new_value={"role_assigned": None}
        )
        
        self.logger.info(f"Revoked role {role_id} from subject {subject_id}")
        return True
    
    def get_subject_permissions(self, subject_id: str) -> Set[str]:
        """Get all permissions for a subject"""
        
        if subject_id not in self.subjects:
            return set()
        
        subject = self.subjects[subject_id]
        all_permissions = set()
        
        # Get permissions from permanent roles
        for role_id in subject.roles:
            all_permissions.update(self._get_role_permissions(role_id))
        
        # Get permissions from active temporary roles
        now = datetime.now()
        for role_id, expiration in subject.temporary_roles.items():
            if expiration > now:
                all_permissions.update(self._get_role_permissions(role_id))
            else:
                # Clean up expired temporary role
                del subject.temporary_roles[role_id]
        
        return all_permissions
    
    def _get_role_permissions(self, role_id: str) -> Set[str]:
        """Get all permissions for a role including inherited"""
        
        if role_id not in self.roles:
            return set()
        
        role = self.roles[role_id]
        permissions = set(role.permissions)
        
        # Add permissions from parent roles
        for parent_role_id in role.parent_roles:
            permissions.update(self._get_role_permissions(parent_role_id))
        
        return permissions
    
    def check_permission(
        self, 
        subject_id: str, 
        permission_id: str,
        resource_id: Optional[str] = None
    ) -> bool:
        """Check if subject has specific permission"""
        
        subject_permissions = self.get_subject_permissions(subject_id)
        
        if permission_id in subject_permissions:
            # Additional checks for resource-scoped permissions
            if resource_id and permission_id in self.permissions:
                permission = self.permissions[permission_id]
                if permission.scope == PermissionScope.RESOURCE:
                    return self._check_resource_permission(subject_id, resource_id, permission)
            
            return True
        
        return False
    
    def _check_resource_permission(
        self, 
        subject_id: str, 
        resource_id: str, 
        permission: Permission
    ) -> bool:
        """Check resource-specific permission"""
        
        if resource_id not in self.resources:
            return False
        
        resource = self.resources[resource_id]
        
        # Check if subject is resource owner
        if resource.owner_id == subject_id:
            return True
        
        # Check inherited permissions
        if subject_id in resource.inherited_permissions:
            return permission.permission_id in resource.inherited_permissions[subject_id]
        
        return True  # Default allow for now


class ABACEngine:
    """Attribute-Based Access Control engine"""
    
    def __init__(self, rbac_engine: RBACEngine):
        self.logger = get_logger(__name__)
        self.rbac_engine = rbac_engine
        self.condition_evaluator = ConditionEvaluator()
        self.policy_rules: Dict[str, PolicyRule] = {}
        
        self._initialize_default_policies()
    
    def _initialize_default_policies(self):
        """Initialize default ABAC policies"""
        
        default_policies = [
            PolicyRule(
                rule_id="policy_business_hours",
                name="Business Hours Access",
                description="Allow access only during business hours",
                priority=100,
                effect=AccessDecision.CONDITIONAL_ALLOW,
                environment_conditions={
                    "time.hour": {"gte": 8, "lte": 18},
                    "time.day_of_week": {"in": [0, 1, 2, 3, 4]}  # Mon-Fri
                }
            ),
            PolicyRule(
                rule_id="policy_high_risk_data",
                name="High Risk Data Access",
                description="Restrict access to high-risk data",
                priority=200,
                effect=AccessDecision.REQUIRES_ELEVATION,
                resource_conditions={
                    "classification": {"in": ["confidential", "restricted"]}
                },
                subject_conditions={
                    "clearance_level": {"gte": 3}
                }
            ),
            PolicyRule(
                rule_id="policy_admin_access",
                name="Administrative Access",
                description="Require elevation for admin operations",
                priority=300,
                effect=AccessDecision.REQUIRES_ELEVATION,
                action_conditions={
                    "action": {"in": ["admin", "delete", "configure"]}
                }
            ),
            PolicyRule(
                rule_id="policy_geo_restriction",
                name="Geographic Restriction",
                description="Restrict access from certain locations",
                priority=150,
                effect=AccessDecision.DENY,
                environment_conditions={
                    "source_ip": {"ip_range": "192.168.0.0/16"}
                }
            )
        ]
        
        for policy in default_policies:
            self.policy_rules[policy.rule_id] = policy
    
    def add_policy(self, policy: PolicyRule) -> None:
        """Add an ABAC policy"""
        self.policy_rules[policy.rule_id] = policy
        self.logger.info(f"Added ABAC policy: {policy.name}")
    
    def evaluate_access(self, request: AccessRequest) -> AccessEvaluation:
        """Evaluate access request against ABAC policies"""
        
        start_time = datetime.now()
        
        # Build context for evaluation
        context = self._build_evaluation_context(request)
        
        # Apply policies in priority order
        applicable_rules = []
        final_decision = AccessDecision.DENY
        reason = "Default deny"
        required_conditions = []
        risk_score = 0.0
        
        # Get sorted policies by priority (higher first)
        sorted_policies = sorted(
            [p for p in self.policy_rules.values() if p.is_active],
            key=lambda x: x.priority,
            reverse=True
        )
        
        for policy in sorted_policies:
            if self._policy_matches(policy, context):
                applicable_rules.append(policy.rule_id)
                
                if policy.effect == AccessDecision.ALLOW:
                    final_decision = AccessDecision.ALLOW
                    reason = f"Allowed by policy: {policy.name}"
                    break
                elif policy.effect == AccessDecision.DENY:
                    final_decision = AccessDecision.DENY
                    reason = f"Denied by policy: {policy.name}"
                    risk_score += 5.0
                    break
                elif policy.effect == AccessDecision.CONDITIONAL_ALLOW:
                    final_decision = AccessDecision.CONDITIONAL_ALLOW
                    reason = f"Conditional allow by policy: {policy.name}"
                    required_conditions.extend(self._extract_conditions(policy))
                elif policy.effect == AccessDecision.REQUIRES_ELEVATION:
                    final_decision = AccessDecision.REQUIRES_ELEVATION
                    reason = f"Elevation required by policy: {policy.name}"
                    risk_score += 2.0
        
        # If no explicit policy matches, check RBAC
        if not applicable_rules:
            if self.rbac_engine.check_permission(
                request.subject_id, 
                f"perm_{request.action}_{request.resource_type}",
                request.resource_id
            ):
                final_decision = AccessDecision.ALLOW
                reason = "Allowed by RBAC"
            else:
                final_decision = AccessDecision.DENY
                reason = "No matching policy or RBAC permission"
                risk_score += 3.0
        
        evaluation_time = (datetime.now() - start_time).total_seconds() * 1000
        
        evaluation = AccessEvaluation(
            request_id=request.request_id,
            decision=final_decision,
            reason=reason,
            applicable_rules=applicable_rules,
            required_conditions=required_conditions,
            evaluation_time_ms=evaluation_time,
            risk_score=min(10.0, risk_score),
            metadata={
                "context": context,
                "policies_evaluated": len(sorted_policies)
            }
        )
        
        self.logger.info(f"Access evaluation completed: {final_decision.value} for {request.subject_id}")
        
        return evaluation
    
    def _build_evaluation_context(self, request: AccessRequest) -> Dict[str, Any]:
        """Build evaluation context from request"""
        
        context = {
            "subject": {},
            "resource": {},
            "action": {"action": request.action},
            "environment": {
                "timestamp": request.timestamp,
                "time": {
                    "hour": request.timestamp.hour,
                    "day_of_week": request.timestamp.weekday()
                },
                "source_ip": request.source_ip,
                "session_id": request.session_id
            }
        }
        
        # Add subject attributes
        if request.subject_id in self.rbac_engine.subjects:
            subject = self.rbac_engine.subjects[request.subject_id]
            context["subject"] = {
                "subject_id": subject.subject_id,
                "subject_type": subject.subject_type,
                "roles": list(subject.roles),
                **subject.attributes
            }
        
        # Add resource attributes
        if request.resource_id in self.rbac_engine.resources:
            resource = self.rbac_engine.resources[request.resource_id]
            context["resource"] = {
                "resource_id": resource.resource_id,
                "resource_type": resource.resource_type,
                "classification": resource.access_classification,
                "owner_id": resource.owner_id,
                **resource.attributes
            }
        else:
            context["resource"] = {
                "resource_id": request.resource_id,
                "resource_type": request.resource_type
            }
        
        # Add request context
        context.update(request.context)
        
        return context
    
    def _policy_matches(self, policy: PolicyRule, context: Dict[str, Any]) -> bool:
        """Check if policy conditions match the context"""
        
        # Evaluate all condition types
        conditions_to_check = [
            (policy.subject_conditions, context.get("subject", {})),
            (policy.resource_conditions, context.get("resource", {})),
            (policy.action_conditions, context.get("action", {})),
            (policy.environment_conditions, context.get("environment", {}))
        ]
        
        for conditions, context_section in conditions_to_check:
            if conditions and not self.condition_evaluator.evaluate_conditions(conditions, context_section):
                return False
        
        # Check time constraints
        if policy.time_constraints:
            if not self._check_time_constraints(policy.time_constraints, context["environment"]["timestamp"]):
                return False
        
        return True
    
    def _check_time_constraints(self, constraints: Dict[str, Any], timestamp: datetime) -> bool:
        """Check time-based constraints"""
        
        if "allowed_hours" in constraints:
            allowed_hours = constraints["allowed_hours"]
            if timestamp.hour not in allowed_hours:
                return False
        
        if "allowed_days" in constraints:
            allowed_days = constraints["allowed_days"]
            if timestamp.weekday() not in allowed_days:
                return False
        
        return True
    
    def _extract_conditions(self, policy: PolicyRule) -> List[str]:
        """Extract human-readable conditions from policy"""
        conditions = []
        
        if policy.environment_conditions:
            for key, value in policy.environment_conditions.items():
                conditions.append(f"Environment condition: {key} must satisfy {value}")
        
        return conditions


class PrivilegedAccessManager:
    """Privileged Access Management (PAM) system"""
    
    def __init__(self, rbac_engine: RBACEngine, audit_logger: Optional[AuditLogger] = None):
        self.logger = get_logger(__name__)
        self.rbac_engine = rbac_engine
        self.audit_logger = audit_logger or AuditLogger()
        self.elevations: Dict[str, PrivilegeElevation] = {}
        self.approval_workflows: Dict[str, Callable] = {}
        
        # Default approval workflow
        self.approval_workflows["default"] = self._auto_approve_low_risk
    
    def request_elevation(
        self,
        subject_id: str,
        role_id: str,
        reason: ElevationReason,
        justification: str,
        duration: timedelta,
        approver_id: Optional[str] = None
    ) -> str:
        """Request privilege elevation"""
        
        elevation = PrivilegeElevation(
            elevation_id=str(uuid.uuid4()),
            subject_id=subject_id,
            requested_role=role_id,
            reason=reason,
            justification=justification,
            requested_duration=duration
        )
        
        self.elevations[elevation.elevation_id] = elevation
        
        # Determine approval workflow
        workflow = self._get_approval_workflow(role_id, reason)
        
        # Process approval
        approved = workflow(elevation)
        
        if approved:
            elevation.approved = True
            elevation.approver_id = approver_id or "system"
            elevation.approval_time = datetime.now()
            elevation.expiration_time = datetime.now() + duration
            elevation.is_active = True
            
            # Grant temporary role
            self.rbac_engine.assign_role_to_subject(
                subject_id, role_id, temporary=True, duration=duration
            )
        
        # Audit log
        self.audit_logger.log_audit_event(
            user_id=subject_id,
            action=ActionType.CREATE,
            resource_type="privilege_elevation",
            resource_id=elevation.elevation_id,
            new_value={
                "role": role_id,
                "reason": reason.value,
                "approved": approved,
                "duration_hours": duration.total_seconds() / 3600
            },
            metadata={"justification": justification}
        )
        
        self.logger.info(f"Privilege elevation {'approved' if approved else 'denied'}: {elevation.elevation_id}")
        
        return elevation.elevation_id
    
    def revoke_elevation(self, elevation_id: str, revoked_by: str = "system") -> bool:
        """Revoke active privilege elevation"""
        
        if elevation_id not in self.elevations:
            return False
        
        elevation = self.elevations[elevation_id]
        
        if not elevation.is_active:
            return False
        
        # Revoke temporary role
        self.rbac_engine.revoke_role_from_subject(elevation.subject_id, elevation.requested_role)
        
        # Update elevation record
        elevation.is_active = False
        
        # Audit log
        self.audit_logger.log_audit_event(
            user_id=revoked_by,
            action=ActionType.UPDATE,
            resource_type="privilege_elevation",
            resource_id=elevation_id,
            old_value={"is_active": True},
            new_value={"is_active": False},
            metadata={"revoked_by": revoked_by}
        )
        
        self.logger.info(f"Privilege elevation revoked: {elevation_id}")
        
        return True
    
    def cleanup_expired_elevations(self) -> int:
        """Clean up expired privilege elevations"""
        
        now = datetime.now()
        expired_count = 0
        
        for elevation in self.elevations.values():
            if (elevation.is_active and 
                elevation.expiration_time and 
                elevation.expiration_time <= now):
                
                self.revoke_elevation(elevation.elevation_id, "system_cleanup")
                expired_count += 1
        
        if expired_count > 0:
            self.logger.info(f"Cleaned up {expired_count} expired privilege elevations")
        
        return expired_count
    
    def _get_approval_workflow(self, role_id: str, reason: ElevationReason) -> Callable:
        """Get approval workflow for elevation request"""
        
        # High-risk roles or emergency access might need manual approval
        if role_id == "role_admin" or reason == ElevationReason.EMERGENCY_ACCESS:
            return self.approval_workflows.get("manual_approval", self.approval_workflows["default"])
        
        return self.approval_workflows["default"]
    
    def _auto_approve_low_risk(self, elevation: PrivilegeElevation) -> bool:
        """Auto-approve low-risk elevation requests"""
        
        # Simple auto-approval logic
        if elevation.requested_duration <= timedelta(hours=4):
            return True
        
        if elevation.reason in [ElevationReason.MAINTENANCE, ElevationReason.ADMIN_TASK]:
            return True
        
        return False


class EnhancedAccessControlManager:
    """Main access control manager integrating RBAC, ABAC, and PAM"""
    
    def __init__(self, audit_logger: Optional[AuditLogger] = None):
        self.logger = get_logger(__name__)
        self.audit_logger = audit_logger or AuditLogger()
        
        self.rbac_engine = RBACEngine(audit_logger)
        self.abac_engine = ABACEngine(self.rbac_engine)
        self.pam = PrivilegedAccessManager(self.rbac_engine, audit_logger)
        
        # Start background tasks
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        import threading
        import time
        
        def cleanup_task():
            while True:
                try:
                    # Clean up expired temporary roles and elevations
                    self.pam.cleanup_expired_elevations()
                    time.sleep(300)  # Run every 5 minutes
                except Exception as e:
                    self.logger.error(f"Cleanup task error: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
        cleanup_thread.start()
    
    def check_access(self, request: AccessRequest) -> AccessEvaluation:
        """Comprehensive access check using RBAC and ABAC"""
        
        # Log access attempt
        self.audit_logger.log_audit_event(
            user_id=request.subject_id,
            action=ActionType.READ,
            resource_type="access_check",
            metadata={
                "resource_id": request.resource_id,
                "action": request.action,
                "source_ip": request.source_ip
            }
        )
        
        # Evaluate access using ABAC engine
        evaluation = self.abac_engine.evaluate_access(request)
        
        # Handle elevation requirements
        if evaluation.decision == AccessDecision.REQUIRES_ELEVATION:
            # Check if user already has elevated privileges
            subject = self.rbac_engine.subjects.get(request.subject_id)
            if subject:
                # Check for active temporary roles that might grant access
                now = datetime.now()
                for role_id, expiration in subject.temporary_roles.items():
                    if expiration > now:
                        # Re-evaluate with elevated role
                        temp_evaluation = self.abac_engine.evaluate_access(request)
                        if temp_evaluation.decision == AccessDecision.ALLOW:
                            evaluation = temp_evaluation
                            evaluation.metadata["elevated_access"] = True
                            break
        
        return evaluation
    
    def get_access_dashboard(self) -> Dict[str, Any]:
        """Get access control dashboard data"""
        
        total_subjects = len(self.rbac_engine.subjects)
        total_roles = len(self.rbac_engine.roles)
        total_permissions = len(self.rbac_engine.permissions)
        total_policies = len(self.abac_engine.policy_rules)
        
        # Active elevations
        active_elevations = [e for e in self.pam.elevations.values() if e.is_active]
        
        # Role distribution
        role_distribution = defaultdict(int)
        for subject in self.rbac_engine.subjects.values():
            for role_id in subject.roles:
                role_distribution[role_id] += 1
        
        return {
            'summary': {
                'total_subjects': total_subjects,
                'total_roles': total_roles,
                'total_permissions': total_permissions,
                'total_policies': total_policies,
                'active_elevations': len(active_elevations)
            },
            'role_distribution': dict(role_distribution),
            'elevation_requests': {
                'total': len(self.pam.elevations),
                'active': len(active_elevations),
                'pending': len([e for e in self.pam.elevations.values() if not e.approved and not e.is_active])
            },
            'policy_status': {
                'active_policies': len([p for p in self.abac_engine.policy_rules.values() if p.is_active]),
                'total_policies': total_policies
            }
        }


# Global access control manager
_access_control_manager: Optional[EnhancedAccessControlManager] = None

def get_access_control_manager() -> EnhancedAccessControlManager:
    """Get global access control manager instance"""
    global _access_control_manager
    if _access_control_manager is None:
        _access_control_manager = EnhancedAccessControlManager()
    return _access_control_manager