"""
Data Policy Engine
Configurable policy management and automated enforcement for data governance.
"""
from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from core.logging import get_logger


class PolicyType(Enum):
    """Types of data policies"""
    DATA_QUALITY = "data_quality"
    DATA_RETENTION = "data_retention"
    DATA_ACCESS = "data_access"
    DATA_PRIVACY = "data_privacy"
    DATA_CLASSIFICATION = "data_classification"
    DATA_LINEAGE = "data_lineage"
    SCHEMA_EVOLUTION = "schema_evolution"
    COMPLIANCE = "compliance"


class EnforcementLevel(Enum):
    """Policy enforcement levels"""
    ADVISORY = "advisory"        # Log only
    WARNING = "warning"          # Log and warn
    BLOCKING = "blocking"        # Prevent action
    AUTOMATIC = "automatic"      # Auto-remediate


class PolicyStatus(Enum):
    """Policy status"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    DRAFT = "draft"
    DEPRECATED = "deprecated"


class ViolationSeverity(Enum):
    """Policy violation severity"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class PolicyCondition:
    """Policy condition definition"""
    condition_id: str
    name: str
    description: str
    condition_type: str  # field_value, data_age, record_count, etc.
    
    # Condition parameters
    field_name: str | None = None
    operator: str = "equals"  # equals, not_equals, greater_than, less_than, contains, regex
    expected_value: Any = None
    threshold: float | None = None
    
    # Advanced conditions
    custom_function: str | None = None  # Reference to custom validation function
    sql_expression: str | None = None   # SQL expression for complex conditions
    
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyAction:
    """Policy enforcement action"""
    action_id: str
    name: str
    action_type: str  # block, warn, log, remediate, notify
    description: str
    
    # Action parameters
    notification_channels: list[str] = field(default_factory=list)
    remediation_script: str | None = None
    auto_approve: bool = False
    escalation_rules: dict[str, Any] = field(default_factory=dict)
    
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataPolicy:
    """Complete data policy definition"""
    policy_id: str
    name: str
    description: str
    policy_type: PolicyType
    
    # Target specification
    target_datasets: list[str] = field(default_factory=list)  # Dataset patterns
    target_tables: list[str] = field(default_factory=list)    # Table patterns
    target_fields: list[str] = field(default_factory=list)    # Field patterns
    
    # Policy logic
    conditions: list[PolicyCondition] = field(default_factory=list)
    actions: list[PolicyAction] = field(default_factory=list)
    
    # Configuration
    enforcement_level: EnforcementLevel = EnforcementLevel.WARNING
    evaluation_schedule: str | None = None  # Cron expression
    evaluation_frequency_minutes: int | None = None
    
    # Lifecycle
    status: PolicyStatus = PolicyStatus.ACTIVE
    version: int = 1
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    created_by: str | None = None
    approved_by: str | None = None
    approved_at: datetime | None = None
    
    # Dependencies
    depends_on_policies: list[str] = field(default_factory=list)
    conflicts_with_policies: list[str] = field(default_factory=list)
    
    # Metadata
    business_owner: str | None = None
    technical_owner: str | None = None
    compliance_framework: str | None = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyViolation:
    """Policy violation record"""
    violation_id: str
    policy_id: str
    dataset_id: str | None
    table_name: str | None
    field_name: str | None
    
    # Violation details
    severity: ViolationSeverity
    violation_type: str
    description: str
    actual_value: Any = None
    expected_value: Any = None
    
    # Context
    detected_at: datetime = field(default_factory=datetime.now)
    detection_method: str = "policy_evaluation"
    affected_records: int = 0
    
    # Resolution
    resolved: bool = False
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    resolution_notes: str | None = None
    
    # Actions taken
    actions_taken: list[dict[str, Any]] = field(default_factory=list)
    remediation_applied: bool = False
    
    # Metadata
    execution_context: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyEvaluationResult:
    """Result of policy evaluation"""
    evaluation_id: str
    policy_id: str
    target_entity: str
    
    # Results
    passed: bool
    violations: list[PolicyViolation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    
    # Execution details
    evaluation_time: datetime = field(default_factory=datetime.now)
    execution_duration_ms: float = 0
    records_evaluated: int = 0
    
    # Actions
    actions_executed: list[dict[str, Any]] = field(default_factory=list)
    blocked_operations: list[str] = field(default_factory=list)
    
    metadata: dict[str, Any] = field(default_factory=dict)


class PolicyEngine:
    """
    Data Policy Engine
    
    Provides comprehensive policy management and enforcement including:
    - Configurable policy definitions
    - Automated policy evaluation
    - Multi-level enforcement (advisory, warning, blocking, automatic)
    - Policy violation tracking and remediation
    - Compliance reporting and audit trails
    - Integration with data pipelines and workflows
    """

    def __init__(self, storage_path: Path | None = None):
        self.logger = get_logger(self.__class__.__name__)
        
        # Storage
        self.storage_path = storage_path or Path("./data_policies")
        self.policies_path = self.storage_path / "policies"
        self.violations_path = self.storage_path / "violations"
        self.evaluations_path = self.storage_path / "evaluations"
        
        self._ensure_directories()
        
        # In-memory storage
        self.policies: dict[str, DataPolicy] = {}
        self.active_policies: dict[str, DataPolicy] = {}
        self.violation_history: list[PolicyViolation] = []
        
        # Evaluation state
        self.evaluation_queue: list[dict[str, Any]] = []
        self.evaluation_results: dict[str, PolicyEvaluationResult] = {}
        
        # Custom functions registry
        self.custom_functions: dict[str, Callable] = {}
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Load existing policies
        self._load_policies()
        
        # Start evaluation scheduler
        self._start_evaluation_scheduler()

    def _ensure_directories(self):
        """Ensure policy directories exist"""
        for path in [self.storage_path, self.policies_path, 
                    self.violations_path, self.evaluations_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _load_policies(self):
        """Load existing policies from storage"""
        try:
            for policy_file in self.policies_path.glob("*.json"):
                try:
                    with open(policy_file) as f:
                        policy_data = json.load(f)
                        policy = self._dict_to_policy(policy_data)
                        self.policies[policy.policy_id] = policy
                        
                        if policy.status == PolicyStatus.ACTIVE:
                            self.active_policies[policy.policy_id] = policy
                            
                except Exception as e:
                    self.logger.error(f"Failed to load policy {policy_file}: {e}")
            
            # Load recent violations
            cutoff_time = datetime.now() - timedelta(days=30)
            for violation_file in self.violations_path.glob("*.json"):
                try:
                    file_time = datetime.fromtimestamp(violation_file.stat().st_mtime)
                    if file_time < cutoff_time:
                        continue
                        
                    with open(violation_file) as f:
                        violations_data = json.load(f)
                        for violation_data in violations_data.get('violations', []):
                            violation = self._dict_to_violation(violation_data)
                            if violation.detected_at >= cutoff_time:
                                self.violation_history.append(violation)
                                
                except Exception as e:
                    self.logger.error(f"Failed to load violations {violation_file}: {e}")
            
            self.logger.info(
                f"Loaded {len(self.policies)} policies "
                f"({len(self.active_policies)} active), "
                f"{len(self.violation_history)} recent violations"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load policies: {e}")

    def _start_evaluation_scheduler(self):
        """Start background policy evaluation scheduler"""
        def evaluation_loop():
            while True:
                try:
                    import time
                    time.sleep(60)  # Check every minute
                    
                    # Schedule policy evaluations
                    self._schedule_policy_evaluations()
                    
                    # Process evaluation queue
                    self._process_evaluation_queue()
                    
                except Exception as e:
                    self.logger.error(f"Evaluation scheduler error: {e}")
        
        scheduler_thread = threading.Thread(target=evaluation_loop, daemon=True)
        scheduler_thread.start()
        self.logger.info("Started policy evaluation scheduler")

    def create_policy(
        self,
        name: str,
        description: str,
        policy_type: PolicyType,
        **kwargs
    ) -> str:
        """Create a new data policy"""
        try:
            with self._lock:
                policy_id = f"policy_{int(datetime.now().timestamp())}"
                
                policy = DataPolicy(
                    policy_id=policy_id,
                    name=name,
                    description=description,
                    policy_type=policy_type,
                    **{k: v for k, v in kwargs.items() if hasattr(DataPolicy, k)}
                )
                
                # Validate policy
                validation_result = self._validate_policy(policy)
                if not validation_result['valid']:
                    raise ValueError(f"Policy validation failed: {validation_result['errors']}")
                
                # Store policy
                self.policies[policy_id] = policy
                
                if policy.status == PolicyStatus.ACTIVE:
                    self.active_policies[policy_id] = policy
                
                # Persist policy
                self._persist_policy(policy)
                
                self.logger.info(f"Created policy: {name} ({policy_id})")
                return policy_id
                
        except Exception as e:
            self.logger.error(f"Failed to create policy: {e}")
            raise

    def update_policy(
        self,
        policy_id: str,
        updates: dict[str, Any],
        increment_version: bool = True
    ) -> bool:
        """Update an existing policy"""
        try:
            with self._lock:
                if policy_id not in self.policies:
                    raise ValueError(f"Policy {policy_id} not found")
                
                policy = self.policies[policy_id]
                old_status = policy.status
                
                # Apply updates
                for field, value in updates.items():
                    if hasattr(policy, field):
                        setattr(policy, field, value)
                
                # Update metadata
                if increment_version:
                    policy.version += 1
                policy.updated_at = datetime.now()
                
                # Validate updated policy
                validation_result = self._validate_policy(policy)
                if not validation_result['valid']:
                    raise ValueError(f"Policy validation failed: {validation_result['errors']}")
                
                # Update active policies cache
                if old_status != policy.status:
                    if policy.status == PolicyStatus.ACTIVE:
                        self.active_policies[policy_id] = policy
                    elif policy_id in self.active_policies:
                        del self.active_policies[policy_id]
                
                # Persist changes
                self._persist_policy(policy)
                
                self.logger.info(f"Updated policy: {policy_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update policy {policy_id}: {e}")
            return False

    def evaluate_policy(
        self,
        policy_id: str,
        target_entity: str,
        context: dict[str, Any] | None = None
    ) -> PolicyEvaluationResult:
        """Evaluate a single policy against a target entity"""
        try:
            start_time = datetime.now()
            
            if policy_id not in self.policies:
                raise ValueError(f"Policy {policy_id} not found")
            
            policy = self.policies[policy_id]
            
            if policy.status != PolicyStatus.ACTIVE:
                raise ValueError(f"Policy {policy_id} is not active")
            
            evaluation_id = f"eval_{int(start_time.timestamp())}"
            
            result = PolicyEvaluationResult(
                evaluation_id=evaluation_id,
                policy_id=policy_id,
                target_entity=target_entity
            )
            
            # Evaluate conditions
            violations = []
            for condition in policy.conditions:
                violation = self._evaluate_condition(
                    condition, target_entity, policy, context or {}
                )
                if violation:
                    violations.append(violation)
            
            result.violations = violations
            result.passed = len(violations) == 0
            
            # Execute actions if violations found
            if violations and policy.enforcement_level != EnforcementLevel.ADVISORY:
                actions_executed = self._execute_policy_actions(
                    policy, violations, target_entity, context or {}
                )
                result.actions_executed = actions_executed
            
            # Calculate execution time
            end_time = datetime.now()
            result.execution_duration_ms = (end_time - start_time).total_seconds() * 1000
            
            # Store evaluation result
            self.evaluation_results[evaluation_id] = result
            
            # Persist violations
            if violations:
                self._persist_violations(violations)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Policy evaluation failed: {e}")
            # Return failed evaluation result
            return PolicyEvaluationResult(
                evaluation_id=f"eval_error_{int(datetime.now().timestamp())}",
                policy_id=policy_id,
                target_entity=target_entity,
                passed=False,
                warnings=[str(e)]
            )

    def evaluate_all_policies(
        self,
        target_entity: str,
        context: dict[str, Any] | None = None
    ) -> dict[str, PolicyEvaluationResult]:
        """Evaluate all active policies against a target entity"""
        try:
            results = {}
            
            with self._lock:
                applicable_policies = self._get_applicable_policies(target_entity)
            
            for policy_id in applicable_policies:
                try:
                    result = self.evaluate_policy(policy_id, target_entity, context)
                    results[policy_id] = result
                except Exception as e:
                    self.logger.error(f"Failed to evaluate policy {policy_id}: {e}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Batch policy evaluation failed: {e}")
            return {}

    def register_custom_function(self, name: str, function: Callable):
        """Register a custom validation function"""
        self.custom_functions[name] = function
        self.logger.info(f"Registered custom function: {name}")

    def get_policy_violations(
        self,
        policy_id: str | None = None,
        dataset_id: str | None = None,
        severity: ViolationSeverity | None = None,
        unresolved_only: bool = False,
        limit: int = 100
    ) -> list[PolicyViolation]:
        """Get policy violations with filtering"""
        try:
            filtered_violations = []
            
            for violation in self.violation_history:
                # Apply filters
                if policy_id and violation.policy_id != policy_id:
                    continue
                if dataset_id and violation.dataset_id != dataset_id:
                    continue
                if severity and violation.severity != severity:
                    continue
                if unresolved_only and violation.resolved:
                    continue
                
                filtered_violations.append(violation)
                
                if len(filtered_violations) >= limit:
                    break
            
            return filtered_violations
            
        except Exception as e:
            self.logger.error(f"Failed to get policy violations: {e}")
            return []

    def resolve_violation(
        self,
        violation_id: str,
        resolved_by: str,
        resolution_notes: str | None = None
    ) -> bool:
        """Mark a policy violation as resolved"""
        try:
            for violation in self.violation_history:
                if violation.violation_id == violation_id:
                    violation.resolved = True
                    violation.resolved_at = datetime.now()
                    violation.resolved_by = resolved_by
                    violation.resolution_notes = resolution_notes
                    
                    self.logger.info(f"Resolved violation {violation_id}")
                    return True
            
            raise ValueError(f"Violation {violation_id} not found")
            
        except Exception as e:
            self.logger.error(f"Failed to resolve violation {violation_id}: {e}")
            return False

    def get_compliance_report(
        self,
        policy_type: PolicyType | None = None,
        time_range_days: int = 30
    ) -> dict[str, Any]:
        """Generate compliance report"""
        try:
            cutoff_time = datetime.now() - timedelta(days=time_range_days)
            
            # Filter violations by time and policy type
            relevant_violations = [
                v for v in self.violation_history
                if v.detected_at >= cutoff_time
                and (not policy_type or 
                     self.policies.get(v.policy_id, {}).policy_type == policy_type)
            ]
            
            # Calculate statistics
            total_violations = len(relevant_violations)
            resolved_violations = len([v for v in relevant_violations if v.resolved])
            unresolved_violations = total_violations - resolved_violations
            
            # Group by severity
            by_severity = {}
            for severity in ViolationSeverity:
                count = len([v for v in relevant_violations if v.severity == severity])
                by_severity[severity.value] = count
            
            # Group by policy
            by_policy = {}
            for violation in relevant_violations:
                policy = self.policies.get(violation.policy_id)
                policy_name = policy.name if policy else violation.policy_id
                by_policy[policy_name] = by_policy.get(policy_name, 0) + 1
            
            # Calculate resolution rate
            resolution_rate = (resolved_violations / total_violations * 100) if total_violations > 0 else 100
            
            # Recent trend (compare with previous period)
            previous_cutoff = cutoff_time - timedelta(days=time_range_days)
            previous_violations = [
                v for v in self.violation_history
                if previous_cutoff <= v.detected_at < cutoff_time
                and (not policy_type or 
                     self.policies.get(v.policy_id, {}).policy_type == policy_type)
            ]
            
            trend = "stable"
            if len(previous_violations) > 0:
                change_rate = (total_violations - len(previous_violations)) / len(previous_violations)
                if change_rate > 0.1:
                    trend = "increasing"
                elif change_rate < -0.1:
                    trend = "decreasing"
            
            return {
                'report_generated': datetime.now().isoformat(),
                'time_range_days': time_range_days,
                'policy_type': policy_type.value if policy_type else 'all',
                'summary': {
                    'total_violations': total_violations,
                    'resolved_violations': resolved_violations,
                    'unresolved_violations': unresolved_violations,
                    'resolution_rate_percent': round(resolution_rate, 2),
                    'trend': trend
                },
                'by_severity': by_severity,
                'by_policy': dict(sorted(by_policy.items(), key=lambda x: x[1], reverse=True)[:10]),
                'critical_violations': [
                    {
                        'violation_id': v.violation_id,
                        'policy_id': v.policy_id,
                        'dataset_id': v.dataset_id,
                        'description': v.description,
                        'detected_at': v.detected_at.isoformat(),
                        'resolved': v.resolved
                    }
                    for v in relevant_violations
                    if v.severity == ViolationSeverity.CRITICAL
                ][:20]
            }
            
        except Exception as e:
            self.logger.error(f"Compliance report generation failed: {e}")
            return {"error": str(e)}

    def _validate_policy(self, policy: DataPolicy) -> dict[str, Any]:
        """Validate policy configuration"""
        errors = []
        warnings = []
        
        try:
            # Basic validation
            if not policy.name:
                errors.append("Policy name is required")
            
            if not policy.conditions:
                warnings.append("Policy has no conditions")
            
            if not policy.actions:
                warnings.append("Policy has no actions")
            
            # Validate conditions
            for condition in policy.conditions:
                if condition.custom_function and condition.custom_function not in self.custom_functions:
                    errors.append(f"Custom function '{condition.custom_function}' not registered")
            
            # Validate target patterns
            for pattern in policy.target_datasets + policy.target_tables + policy.target_fields:
                try:
                    re.compile(pattern)
                except re.error as e:
                    errors.append(f"Invalid regex pattern '{pattern}': {e}")
            
            # Check for policy conflicts
            for conflict_policy_id in policy.conflicts_with_policies:
                if (conflict_policy_id in self.active_policies and 
                    conflict_policy_id != policy.policy_id):
                    warnings.append(f"Conflicts with active policy: {conflict_policy_id}")
            
            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings
            }
            
        except Exception as e:
            return {
                'valid': False,
                'errors': [f"Validation error: {str(e)}"],
                'warnings': []
            }

    def _get_applicable_policies(self, target_entity: str) -> list[str]:
        """Get policies applicable to a target entity"""
        applicable_policies = []
        
        try:
            for policy_id, policy in self.active_policies.items():
                if self._policy_applies_to_entity(policy, target_entity):
                    applicable_policies.append(policy_id)
            
            # Sort by dependency order
            return self._sort_policies_by_dependencies(applicable_policies)
            
        except Exception as e:
            self.logger.error(f"Failed to get applicable policies: {e}")
            return []

    def _policy_applies_to_entity(self, policy: DataPolicy, target_entity: str) -> bool:
        """Check if policy applies to target entity"""
        try:
            # Check dataset patterns
            for pattern in policy.target_datasets:
                if re.match(pattern, target_entity):
                    return True
            
            # Check table patterns  
            for pattern in policy.target_tables:
                if re.match(pattern, target_entity):
                    return True
            
            # If no patterns specified, apply to all
            if not policy.target_datasets and not policy.target_tables and not policy.target_fields:
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Policy applicability check failed: {e}")
            return False

    def _sort_policies_by_dependencies(self, policy_ids: list[str]) -> list[str]:
        """Sort policies by dependency order"""
        # Simple topological sort
        try:
            sorted_policies = []
            remaining_policies = set(policy_ids)
            
            while remaining_policies:
                # Find policies with no unresolved dependencies
                ready_policies = []
                for policy_id in remaining_policies:
                    policy = self.policies[policy_id]
                    dependencies = set(policy.depends_on_policies) & remaining_policies
                    if not dependencies:
                        ready_policies.append(policy_id)
                
                if not ready_policies:
                    # Circular dependency - just add remaining policies
                    ready_policies = list(remaining_policies)
                
                sorted_policies.extend(ready_policies)
                remaining_policies -= set(ready_policies)
            
            return sorted_policies
            
        except Exception as e:
            self.logger.error(f"Policy dependency sorting failed: {e}")
            return policy_ids

    def _evaluate_condition(
        self,
        condition: PolicyCondition,
        target_entity: str,
        policy: DataPolicy,
        context: dict[str, Any]
    ) -> PolicyViolation | None:
        """Evaluate a single policy condition"""
        try:
            # Custom function evaluation
            if condition.custom_function:
                custom_func = self.custom_functions.get(condition.custom_function)
                if custom_func:
                    result = custom_func(target_entity, condition, context)
                    if not result:
                        return self._create_violation(condition, policy, target_entity, context)
                return None
            
            # SQL expression evaluation
            if condition.sql_expression:
                # This would require database connection - placeholder for now
                self.logger.warning("SQL expression evaluation not implemented")
                return None
            
            # Field value evaluation
            if condition.field_name and condition.condition_type == "field_value":
                field_value = context.get('data', {}).get(condition.field_name)
                
                if not self._evaluate_field_condition(condition, field_value):
                    return self._create_violation(
                        condition, policy, target_entity, context, 
                        actual_value=field_value
                    )
            
            # Data age evaluation
            elif condition.condition_type == "data_age":
                data_timestamp = context.get('data_timestamp')
                if data_timestamp:
                    data_age_hours = (datetime.now() - data_timestamp).total_seconds() / 3600
                    
                    if condition.threshold and data_age_hours > condition.threshold:
                        return self._create_violation(
                            condition, policy, target_entity, context,
                            actual_value=data_age_hours
                        )
            
            # Record count evaluation
            elif condition.condition_type == "record_count":
                record_count = context.get('record_count', 0)
                
                if not self._evaluate_threshold_condition(condition, record_count):
                    return self._create_violation(
                        condition, policy, target_entity, context,
                        actual_value=record_count
                    )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Condition evaluation failed: {e}")
            # Create violation for evaluation error
            return PolicyViolation(
                violation_id=f"eval_error_{int(datetime.now().timestamp())}",
                policy_id=policy.policy_id,
                dataset_id=target_entity,
                severity=ViolationSeverity.MEDIUM,
                violation_type="evaluation_error",
                description=f"Condition evaluation failed: {str(e)}",
                metadata={'error': str(e)}
            )

    def _evaluate_field_condition(self, condition: PolicyCondition, field_value: Any) -> bool:
        """Evaluate field-based condition"""
        try:
            if field_value is None:
                return condition.operator in ["is_null", "not_required"]
            
            if condition.operator == "equals":
                return field_value == condition.expected_value
            elif condition.operator == "not_equals":
                return field_value != condition.expected_value
            elif condition.operator == "greater_than":
                return float(field_value) > float(condition.expected_value)
            elif condition.operator == "less_than":
                return float(field_value) < float(condition.expected_value)
            elif condition.operator == "contains":
                return str(condition.expected_value) in str(field_value)
            elif condition.operator == "regex":
                return bool(re.search(str(condition.expected_value), str(field_value)))
            elif condition.operator == "is_null":
                return field_value is None
            elif condition.operator == "is_not_null":
                return field_value is not None
            
            return False
            
        except Exception as e:
            self.logger.error(f"Field condition evaluation failed: {e}")
            return False

    def _evaluate_threshold_condition(self, condition: PolicyCondition, value: float) -> bool:
        """Evaluate threshold-based condition"""
        try:
            if condition.threshold is None:
                return True
            
            if condition.operator == "greater_than":
                return value > condition.threshold
            elif condition.operator == "less_than":
                return value < condition.threshold
            elif condition.operator == "equals":
                return abs(value - condition.threshold) < 0.001
            elif condition.operator == "between":
                # Expecting expected_value to be a range [min, max]
                if isinstance(condition.expected_value, (list, tuple)) and len(condition.expected_value) == 2:
                    return condition.expected_value[0] <= value <= condition.expected_value[1]
            
            return False
            
        except Exception as e:
            self.logger.error(f"Threshold condition evaluation failed: {e}")
            return False

    def _create_violation(
        self,
        condition: PolicyCondition,
        policy: DataPolicy,
        target_entity: str,
        context: dict[str, Any],
        actual_value: Any = None
    ) -> PolicyViolation:
        """Create policy violation record"""
        
        # Determine severity based on policy type and condition
        severity = ViolationSeverity.MEDIUM
        if policy.policy_type == PolicyType.DATA_PRIVACY:
            severity = ViolationSeverity.HIGH
        elif policy.policy_type == PolicyType.COMPLIANCE:
            severity = ViolationSeverity.CRITICAL
        elif condition.condition_type in ["data_quality", "schema_validation"]:
            severity = ViolationSeverity.HIGH
        
        violation_id = f"violation_{int(datetime.now().timestamp())}"
        
        return PolicyViolation(
            violation_id=violation_id,
            policy_id=policy.policy_id,
            dataset_id=target_entity,
            table_name=context.get('table_name'),
            field_name=condition.field_name,
            severity=severity,
            violation_type=condition.condition_type,
            description=f"Policy '{policy.name}' condition '{condition.name}' failed",
            actual_value=actual_value,
            expected_value=condition.expected_value,
            affected_records=context.get('affected_records', 0),
            execution_context=context,
            metadata={
                'condition_id': condition.condition_id,
                'operator': condition.operator,
                'threshold': condition.threshold
            }
        )

    def _execute_policy_actions(
        self,
        policy: DataPolicy,
        violations: list[PolicyViolation],
        target_entity: str,
        context: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Execute policy enforcement actions"""
        executed_actions = []
        
        try:
            for action in policy.actions:
                action_result = self._execute_single_action(
                    action, policy, violations, target_entity, context
                )
                executed_actions.append(action_result)
                
                # Update violations with actions taken
                for violation in violations:
                    violation.actions_taken.append(action_result)
            
        except Exception as e:
            self.logger.error(f"Policy action execution failed: {e}")
        
        return executed_actions

    def _execute_single_action(
        self,
        action: PolicyAction,
        policy: DataPolicy,
        violations: list[PolicyViolation],
        target_entity: str,
        context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute a single policy action"""
        
        action_result = {
            'action_id': action.action_id,
            'action_type': action.action_type,
            'executed_at': datetime.now().isoformat(),
            'success': False,
            'details': {}
        }
        
        try:
            if action.action_type == "block":
                # Block the operation
                action_result['success'] = True
                action_result['details'] = {
                    'blocked_operation': context.get('operation', 'unknown'),
                    'message': f"Operation blocked by policy: {policy.name}"
                }
                self.logger.warning(f"Blocked operation by policy {policy.policy_id}")
                
            elif action.action_type == "warn":
                # Issue warning
                action_result['success'] = True
                action_result['details'] = {
                    'warning_message': f"Policy violation warning: {policy.name}",
                    'violations_count': len(violations)
                }
                self.logger.warning(f"Policy violation warning: {policy.name}")
                
            elif action.action_type == "log":
                # Log violation
                action_result['success'] = True
                action_result['details'] = {
                    'log_message': f"Policy violation logged: {policy.name}",
                    'violations_count': len(violations)
                }
                self.logger.info(f"Policy violation logged: {policy.name}")
                
            elif action.action_type == "notify":
                # Send notifications
                notification_result = self._send_policy_notifications(
                    action, policy, violations, target_entity
                )
                action_result['success'] = notification_result['success']
                action_result['details'] = notification_result
                
            elif action.action_type == "remediate":
                # Execute remediation
                if action.remediation_script:
                    remediation_result = self._execute_remediation(
                        action, policy, violations, target_entity, context
                    )
                    action_result['success'] = remediation_result['success']
                    action_result['details'] = remediation_result
            
        except Exception as e:
            action_result['success'] = False
            action_result['details'] = {'error': str(e)}
            self.logger.error(f"Action execution failed: {e}")
        
        return action_result

    def _send_policy_notifications(
        self,
        action: PolicyAction,
        policy: DataPolicy,
        violations: list[PolicyViolation],
        target_entity: str
    ) -> dict[str, Any]:
        """Send policy violation notifications"""
        try:
            # Placeholder for notification integration
            # In practice, integrate with email, Slack, etc.
            
            notification_payload = {
                'policy_name': policy.name,
                'policy_id': policy.policy_id,
                'target_entity': target_entity,
                'violations_count': len(violations),
                'severity': max(v.severity.value for v in violations) if violations else 'low',
                'detected_at': datetime.now().isoformat()
            }
            
            sent_channels = []
            for channel in action.notification_channels:
                # Send to each channel
                self.logger.info(f"Sending policy notification to {channel}")
                sent_channels.append(channel)
            
            return {
                'success': True,
                'channels_notified': sent_channels,
                'payload': notification_payload
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def _execute_remediation(
        self,
        action: PolicyAction,
        policy: DataPolicy,
        violations: list[PolicyViolation],
        target_entity: str,
        context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute automated remediation"""
        try:
            # Placeholder for remediation execution
            # In practice, this would execute scripts, update data, etc.
            
            self.logger.info(f"Executing remediation for policy {policy.policy_id}")
            
            # Mark violations as remediated
            for violation in violations:
                violation.remediation_applied = True
            
            return {
                'success': True,
                'remediation_script': action.remediation_script,
                'violations_remediated': len(violations)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def _schedule_policy_evaluations(self):
        """Schedule policy evaluations based on frequency settings"""
        try:
            current_time = datetime.now()
            
            for policy_id, policy in self.active_policies.items():
                if not policy.evaluation_frequency_minutes:
                    continue
                
                # Check if it's time to evaluate
                last_evaluation_time = self._get_last_evaluation_time(policy_id)
                
                if (not last_evaluation_time or
                    (current_time - last_evaluation_time).total_seconds() / 60 
                    >= policy.evaluation_frequency_minutes):
                    
                    # Add to evaluation queue
                    self.evaluation_queue.append({
                        'policy_id': policy_id,
                        'scheduled_at': current_time,
                        'evaluation_type': 'scheduled'
                    })
                    
        except Exception as e:
            self.logger.error(f"Policy evaluation scheduling failed: {e}")

    def _process_evaluation_queue(self):
        """Process queued policy evaluations"""
        try:
            while self.evaluation_queue:
                evaluation_task = self.evaluation_queue.pop(0)
                
                policy_id = evaluation_task['policy_id']
                policy = self.active_policies.get(policy_id)
                
                if not policy:
                    continue
                
                # Determine target entities for evaluation
                # This is a placeholder - in practice, you'd query your data catalog
                target_entities = self._get_evaluation_targets(policy)
                
                for target_entity in target_entities:
                    try:
                        self.evaluate_policy(policy_id, target_entity)
                    except Exception as e:
                        self.logger.error(f"Scheduled evaluation failed for {policy_id}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Evaluation queue processing failed: {e}")

    def _get_last_evaluation_time(self, policy_id: str) -> datetime | None:
        """Get the last evaluation time for a policy"""
        try:
            # Find most recent evaluation result for this policy
            latest_time = None
            for result in self.evaluation_results.values():
                if result.policy_id == policy_id:
                    if not latest_time or result.evaluation_time > latest_time:
                        latest_time = result.evaluation_time
            return latest_time
        except Exception as e:
            self.logger.error(f"Failed to get last evaluation time: {e}")
            return None

    def _get_evaluation_targets(self, policy: DataPolicy) -> list[str]:
        """Get target entities for policy evaluation"""
        # Placeholder - in practice, query data catalog for matching entities
        return ['sample_dataset_1', 'sample_dataset_2']

    # Persistence methods
    def _persist_policy(self, policy: DataPolicy):
        """Persist policy to storage"""
        try:
            policy_file = self.policies_path / f"{policy.policy_id}.json"
            with open(policy_file, 'w') as f:
                json.dump(self._policy_to_dict(policy), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist policy {policy.policy_id}: {e}")

    def _persist_violations(self, violations: list[PolicyViolation]):
        """Persist violations to storage"""
        try:
            if not violations:
                return
            
            # Group by date
            violations_by_date = {}
            for violation in violations:
                date_str = violation.detected_at.strftime("%Y%m%d")
                if date_str not in violations_by_date:
                    violations_by_date[date_str] = []
                violations_by_date[date_str].append(violation)
            
            # Persist each date group
            for date_str, date_violations in violations_by_date.items():
                violations_file = self.violations_path / f"violations_{date_str}.json"
                
                # Load existing or create new
                if violations_file.exists():
                    with open(violations_file) as f:
                        data = json.load(f)
                else:
                    data = {'date': date_str, 'violations': []}
                
                # Add violations
                for violation in date_violations:
                    data['violations'].append(self._violation_to_dict(violation))
                
                # Save
                with open(violations_file, 'w') as f:
                    json.dump(data, f, default=str)
                    
        except Exception as e:
            self.logger.error(f"Failed to persist violations: {e}")

    # Serialization helpers
    def _policy_to_dict(self, policy: DataPolicy) -> dict[str, Any]:
        """Convert policy to dictionary"""
        return {
            'policy_id': policy.policy_id,
            'name': policy.name,
            'description': policy.description,
            'policy_type': policy.policy_type.value,
            'target_datasets': policy.target_datasets,
            'target_tables': policy.target_tables,
            'target_fields': policy.target_fields,
            'conditions': [self._condition_to_dict(c) for c in policy.conditions],
            'actions': [self._action_to_dict(a) for a in policy.actions],
            'enforcement_level': policy.enforcement_level.value,
            'evaluation_schedule': policy.evaluation_schedule,
            'evaluation_frequency_minutes': policy.evaluation_frequency_minutes,
            'status': policy.status.value,
            'version': policy.version,
            'created_at': policy.created_at.isoformat(),
            'updated_at': policy.updated_at.isoformat(),
            'created_by': policy.created_by,
            'approved_by': policy.approved_by,
            'approved_at': policy.approved_at.isoformat() if policy.approved_at else None,
            'depends_on_policies': policy.depends_on_policies,
            'conflicts_with_policies': policy.conflicts_with_policies,
            'business_owner': policy.business_owner,
            'technical_owner': policy.technical_owner,
            'compliance_framework': policy.compliance_framework,
            'tags': policy.tags,
            'metadata': policy.metadata
        }

    def _dict_to_policy(self, data: dict[str, Any]) -> DataPolicy:
        """Convert dictionary to policy"""
        return DataPolicy(
            policy_id=data['policy_id'],
            name=data['name'],
            description=data['description'],
            policy_type=PolicyType(data['policy_type']),
            target_datasets=data.get('target_datasets', []),
            target_tables=data.get('target_tables', []),
            target_fields=data.get('target_fields', []),
            conditions=[self._dict_to_condition(c) for c in data.get('conditions', [])],
            actions=[self._dict_to_action(a) for a in data.get('actions', [])],
            enforcement_level=EnforcementLevel(data.get('enforcement_level', 'warning')),
            evaluation_schedule=data.get('evaluation_schedule'),
            evaluation_frequency_minutes=data.get('evaluation_frequency_minutes'),
            status=PolicyStatus(data.get('status', 'active')),
            version=data.get('version', 1),
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at']),
            created_by=data.get('created_by'),
            approved_by=data.get('approved_by'),
            approved_at=datetime.fromisoformat(data['approved_at']) if data.get('approved_at') else None,
            depends_on_policies=data.get('depends_on_policies', []),
            conflicts_with_policies=data.get('conflicts_with_policies', []),
            business_owner=data.get('business_owner'),
            technical_owner=data.get('technical_owner'),
            compliance_framework=data.get('compliance_framework'),
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )

    def _condition_to_dict(self, condition: PolicyCondition) -> dict[str, Any]:
        """Convert condition to dictionary"""
        return {
            'condition_id': condition.condition_id,
            'name': condition.name,
            'description': condition.description,
            'condition_type': condition.condition_type,
            'field_name': condition.field_name,
            'operator': condition.operator,
            'expected_value': condition.expected_value,
            'threshold': condition.threshold,
            'custom_function': condition.custom_function,
            'sql_expression': condition.sql_expression,
            'metadata': condition.metadata
        }

    def _dict_to_condition(self, data: dict[str, Any]) -> PolicyCondition:
        """Convert dictionary to condition"""
        return PolicyCondition(
            condition_id=data['condition_id'],
            name=data['name'],
            description=data['description'],
            condition_type=data['condition_type'],
            field_name=data.get('field_name'),
            operator=data.get('operator', 'equals'),
            expected_value=data.get('expected_value'),
            threshold=data.get('threshold'),
            custom_function=data.get('custom_function'),
            sql_expression=data.get('sql_expression'),
            metadata=data.get('metadata', {})
        )

    def _action_to_dict(self, action: PolicyAction) -> dict[str, Any]:
        """Convert action to dictionary"""
        return {
            'action_id': action.action_id,
            'name': action.name,
            'action_type': action.action_type,
            'description': action.description,
            'notification_channels': action.notification_channels,
            'remediation_script': action.remediation_script,
            'auto_approve': action.auto_approve,
            'escalation_rules': action.escalation_rules,
            'metadata': action.metadata
        }

    def _dict_to_action(self, data: dict[str, Any]) -> PolicyAction:
        """Convert dictionary to action"""
        return PolicyAction(
            action_id=data['action_id'],
            name=data['name'],
            action_type=data['action_type'],
            description=data['description'],
            notification_channels=data.get('notification_channels', []),
            remediation_script=data.get('remediation_script'),
            auto_approve=data.get('auto_approve', False),
            escalation_rules=data.get('escalation_rules', {}),
            metadata=data.get('metadata', {})
        )

    def _violation_to_dict(self, violation: PolicyViolation) -> dict[str, Any]:
        """Convert violation to dictionary"""
        return {
            'violation_id': violation.violation_id,
            'policy_id': violation.policy_id,
            'dataset_id': violation.dataset_id,
            'table_name': violation.table_name,
            'field_name': violation.field_name,
            'severity': violation.severity.value,
            'violation_type': violation.violation_type,
            'description': violation.description,
            'actual_value': violation.actual_value,
            'expected_value': violation.expected_value,
            'detected_at': violation.detected_at.isoformat(),
            'detection_method': violation.detection_method,
            'affected_records': violation.affected_records,
            'resolved': violation.resolved,
            'resolved_at': violation.resolved_at.isoformat() if violation.resolved_at else None,
            'resolved_by': violation.resolved_by,
            'resolution_notes': violation.resolution_notes,
            'actions_taken': violation.actions_taken,
            'remediation_applied': violation.remediation_applied,
            'execution_context': violation.execution_context,
            'metadata': violation.metadata
        }

    def _dict_to_violation(self, data: dict[str, Any]) -> PolicyViolation:
        """Convert dictionary to violation"""
        return PolicyViolation(
            violation_id=data['violation_id'],
            policy_id=data['policy_id'],
            dataset_id=data.get('dataset_id'),
            table_name=data.get('table_name'),
            field_name=data.get('field_name'),
            severity=ViolationSeverity(data['severity']),
            violation_type=data['violation_type'],
            description=data['description'],
            actual_value=data.get('actual_value'),
            expected_value=data.get('expected_value'),
            detected_at=datetime.fromisoformat(data['detected_at']),
            detection_method=data.get('detection_method', 'policy_evaluation'),
            affected_records=data.get('affected_records', 0),
            resolved=data.get('resolved', False),
            resolved_at=datetime.fromisoformat(data['resolved_at']) if data.get('resolved_at') else None,
            resolved_by=data.get('resolved_by'),
            resolution_notes=data.get('resolution_notes'),
            actions_taken=data.get('actions_taken', []),
            remediation_applied=data.get('remediation_applied', False),
            execution_context=data.get('execution_context', {}),
            metadata=data.get('metadata', {})
        )