"""
Automated Incident Response System
Provides comprehensive automated incident detection, classification, response
orchestration, and integration with self-healing systems for production resilience.
"""

import asyncio
import hashlib
import json
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from core.logging import get_logger
from core.self_healing.autonomous_recovery import get_recovery_system
from core.structured_logging import (
    AuditEvent,
    LogContext,
    StructuredLogger,
    log_context,
)
from monitoring.datadog_custom_metrics_advanced import get_custom_metrics_advanced
from monitoring.performance_monitoring_realtime import get_performance_monitor

logger = get_logger(__name__)
structured_logger = StructuredLogger.get_logger(__name__)


class IncidentSeverity(Enum):
    """Incident severity levels."""

    P1_CRITICAL = "P1_CRITICAL"  # Service down, data loss
    P2_HIGH = "P2_HIGH"  # Major functionality impaired
    P3_MEDIUM = "P3_MEDIUM"  # Minor functionality impaired
    P4_LOW = "P4_LOW"  # Cosmetic issues, warnings
    P5_INFO = "P5_INFO"  # Informational, monitoring


class IncidentCategory(Enum):
    """Incident categories."""

    SYSTEM_FAILURE = "system_failure"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    DATA_QUALITY = "data_quality"
    SECURITY_BREACH = "security_breach"
    CAPACITY_EXHAUSTION = "capacity_exhaustion"
    EXTERNAL_DEPENDENCY = "external_dependency"
    CONFIGURATION_ERROR = "configuration_error"
    NETWORK_ISSUE = "network_issue"
    DATABASE_ISSUE = "database_issue"
    APPLICATION_ERROR = "application_error"


class IncidentStatus(Enum):
    """Incident status values."""

    OPEN = "open"
    INVESTIGATING = "investigating"
    MITIGATING = "mitigating"
    RESOLVED = "resolved"
    CLOSED = "closed"
    CANCELLED = "cancelled"


class ResponseStatus(Enum):
    """Response execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class AutomationLevel(Enum):
    """Levels of automation for incident response."""

    MANUAL_ONLY = "manual_only"  # No automation, alerts only
    SEMI_AUTOMATED = "semi_automated"  # Automated with approval required
    FULLY_AUTOMATED = "fully_automated"  # Full automation
    EMERGENCY_ONLY = "emergency_only"  # Automated for P1 incidents only


@dataclass
class IncidentMetric:
    """Metrics associated with an incident."""

    metric_name: str
    current_value: float
    expected_value: float
    threshold_exceeded: bool
    impact_score: float = 0.0
    trend_direction: str = "stable"  # increasing, decreasing, stable
    anomaly_confidence: float = 0.0


@dataclass
class IncidentContext:
    """Context information for an incident."""

    service_name: str
    environment: str = "production"
    region: str = "default"
    affected_users: int = 0
    business_impact: str = "low"  # low, medium, high, critical
    upstream_dependencies: list[str] = field(default_factory=list)
    downstream_dependencies: list[str] = field(default_factory=list)
    recent_deployments: list[dict[str, Any]] = field(default_factory=list)
    related_alerts: list[str] = field(default_factory=list)


@dataclass
class ResponseAction:
    """Automated response action definition."""

    action_id: str
    action_type: str  # restart, scale, failover, rollback, etc.
    description: str
    parameters: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 300
    retry_count: int = 0
    requires_approval: bool = False
    safety_checks: list[str] = field(default_factory=list)
    rollback_actions: list[str] = field(default_factory=list)


@dataclass
class ResponsePlan:
    """Automated response plan for incidents."""

    plan_id: str
    name: str
    description: str
    incident_patterns: list[dict[str, Any]]  # Conditions that trigger this plan
    response_actions: list[ResponseAction]
    automation_level: AutomationLevel
    max_execution_time_minutes: int = 30
    success_criteria: list[str] = field(default_factory=list)
    escalation_triggers: list[str] = field(default_factory=list)
    enabled: bool = True
    priority: int = 100  # Lower numbers = higher priority


@dataclass
class ActionExecution:
    """Record of action execution."""

    execution_id: str
    action_id: str
    started_at: datetime
    completed_at: datetime | None = None
    status: ResponseStatus = ResponseStatus.PENDING
    result: dict[str, Any] | None = None
    error_message: str | None = None
    duration_seconds: float | None = None
    retries_attempted: int = 0


@dataclass
class Incident:
    """Incident record with full context and response tracking."""

    incident_id: str
    title: str
    description: str
    severity: IncidentSeverity
    category: IncidentCategory
    status: IncidentStatus = IncidentStatus.OPEN
    context: IncidentContext = field(default_factory=lambda: IncidentContext("unknown"))
    metrics: list[IncidentMetric] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: datetime | None = None
    assigned_plan: str | None = None
    action_executions: list[ActionExecution] = field(default_factory=list)
    escalated: bool = False
    manual_override: bool = False
    tags: dict[str, str] = field(default_factory=dict)
    related_incidents: list[str] = field(default_factory=list)


class IncidentDetector:
    """Advanced incident detection with pattern recognition."""

    def __init__(self):
        self.logger = get_logger(f"{__name__}.incident_detector")
        self.detection_patterns = self._initialize_detection_patterns()
        self.incident_history: deque = deque(maxlen=1000)
        self.correlation_window_minutes = 15

    def _initialize_detection_patterns(self) -> list[dict[str, Any]]:
        """Initialize incident detection patterns."""
        return [
            # API Service Down Pattern
            {
                "pattern_id": "api_service_down",
                "name": "API Service Unavailable",
                "conditions": [
                    {"metric": "api_success_rate_percent", "operator": "<", "value": 50},
                    {"metric": "api_response_time_ms", "operator": ">", "value": 5000},
                ],
                "severity": IncidentSeverity.P1_CRITICAL,
                "category": IncidentCategory.SYSTEM_FAILURE,
                "confidence_threshold": 0.9,
            },
            # Performance Degradation Pattern
            {
                "pattern_id": "performance_degradation",
                "name": "Performance Degradation Detected",
                "conditions": [
                    {"metric": "api_response_time_ms", "operator": ">", "value": 1000},
                    {"metric": "system_cpu_percent", "operator": ">", "value": 80},
                    {"metric": "system_memory_percent", "operator": ">", "value": 85},
                ],
                "severity": IncidentSeverity.P2_HIGH,
                "category": IncidentCategory.PERFORMANCE_DEGRADATION,
                "confidence_threshold": 0.8,
            },
            # Data Quality Issues
            {
                "pattern_id": "data_quality_degradation",
                "name": "Data Quality Below Threshold",
                "conditions": [
                    {"metric": "data_quality_completeness_score", "operator": "<", "value": 0.8},
                    {"metric": "etl_records_failed_rate", "operator": ">", "value": 0.05},
                ],
                "severity": IncidentSeverity.P2_HIGH,
                "category": IncidentCategory.DATA_QUALITY,
                "confidence_threshold": 0.85,
            },
            # Capacity Exhaustion
            {
                "pattern_id": "capacity_exhaustion",
                "name": "System Capacity Exhausted",
                "conditions": [
                    {"metric": "system_memory_percent", "operator": ">", "value": 95},
                    {"metric": "system_disk_percent", "operator": ">", "value": 90},
                ],
                "severity": IncidentSeverity.P1_CRITICAL,
                "category": IncidentCategory.CAPACITY_EXHAUSTION,
                "confidence_threshold": 0.95,
            },
            # Database Issues
            {
                "pattern_id": "database_performance_issue",
                "name": "Database Performance Issue",
                "conditions": [
                    {"metric": "db_query_response_time_ms", "operator": ">", "value": 2000},
                    {"metric": "db_connection_pool_usage_percent", "operator": ">", "value": 90},
                ],
                "severity": IncidentSeverity.P2_HIGH,
                "category": IncidentCategory.DATABASE_ISSUE,
                "confidence_threshold": 0.8,
            },
            # Security Anomaly
            {
                "pattern_id": "security_anomaly",
                "name": "Security Anomaly Detected",
                "conditions": [
                    {"metric": "api_error_rate_percent", "operator": ">", "value": 20},
                    {"metric": "api_4xx_rate", "operator": ">", "value": 15},
                ],
                "severity": IncidentSeverity.P1_CRITICAL,
                "category": IncidentCategory.SECURITY_BREACH,
                "confidence_threshold": 0.7,
            },
            # Cache Performance Issues
            {
                "pattern_id": "cache_performance_issue",
                "name": "Cache Performance Degraded",
                "conditions": [{"metric": "cache_hit_rate_percent", "operator": "<", "value": 60}],
                "severity": IncidentSeverity.P3_MEDIUM,
                "category": IncidentCategory.PERFORMANCE_DEGRADATION,
                "confidence_threshold": 0.75,
            },
            # ETL Pipeline Failure
            {
                "pattern_id": "etl_pipeline_failure",
                "name": "ETL Pipeline Failure",
                "conditions": [
                    {"metric": "etl_pipeline_success_rate", "operator": "<", "value": 0.8},
                    {"metric": "etl_processing_time_minutes", "operator": ">", "value": 120},
                ],
                "severity": IncidentSeverity.P2_HIGH,
                "category": IncidentCategory.APPLICATION_ERROR,
                "confidence_threshold": 0.85,
            },
        ]

    async def evaluate_metrics(self, metrics: dict[str, float]) -> list[Incident]:
        """Evaluate metrics against detection patterns to identify incidents."""
        detected_incidents = []

        for pattern in self.detection_patterns:
            incident = await self._evaluate_pattern(pattern, metrics)
            if incident:
                detected_incidents.append(incident)

        return detected_incidents

    async def _evaluate_pattern(
        self, pattern: dict[str, Any], metrics: dict[str, float]
    ) -> Incident | None:
        """Evaluate a single pattern against metrics."""
        conditions_met = 0
        total_conditions = len(pattern["conditions"])
        incident_metrics = []

        for condition in pattern["conditions"]:
            metric_name = condition["metric"]
            operator = condition["operator"]
            threshold_value = condition["value"]

            if metric_name not in metrics:
                continue

            current_value = metrics[metric_name]
            condition_met = False

            if operator == ">" and current_value > threshold_value:
                condition_met = True
            elif operator == "<" and current_value < threshold_value:
                condition_met = True
            elif operator == "==" and current_value == threshold_value:
                condition_met = True
            elif operator == ">=" and current_value >= threshold_value:
                condition_met = True
            elif operator == "<=" and current_value <= threshold_value:
                condition_met = True

            if condition_met:
                conditions_met += 1

                # Calculate impact score based on how far from threshold
                if operator in [">", ">="]:
                    impact_score = min(1.0, (current_value - threshold_value) / threshold_value)
                else:
                    impact_score = min(1.0, (threshold_value - current_value) / threshold_value)

                incident_metric = IncidentMetric(
                    metric_name=metric_name,
                    current_value=current_value,
                    expected_value=threshold_value,
                    threshold_exceeded=True,
                    impact_score=impact_score,
                )
                incident_metrics.append(incident_metric)

        # Calculate confidence based on conditions met
        confidence = conditions_met / total_conditions if total_conditions > 0 else 0

        if confidence >= pattern.get("confidence_threshold", 0.8):
            # Generate incident
            incident_id = self._generate_incident_id(pattern["pattern_id"], metrics)

            # Check if we've already created this incident recently
            if self._is_duplicate_incident(incident_id):
                return None

            incident = Incident(
                incident_id=incident_id,
                title=pattern["name"],
                description=f"Automated detection: {pattern['name']}. Confidence: {confidence:.2f}",
                severity=pattern["severity"],
                category=pattern["category"],
                metrics=incident_metrics,
                tags={"pattern_id": pattern["pattern_id"], "confidence": str(confidence)},
            )

            # Add to history for duplicate detection
            self.incident_history.append(
                {
                    "incident_id": incident_id,
                    "created_at": incident.created_at,
                    "pattern_id": pattern["pattern_id"],
                }
            )

            return incident

        return None

    def _generate_incident_id(self, pattern_id: str, metrics: dict[str, float]) -> str:
        """Generate a consistent incident ID for pattern and metrics."""
        # Create hash of pattern and key metrics to ensure consistency
        metric_signature = json.dumps(sorted(metrics.items()), sort_keys=True)
        signature = f"{pattern_id}_{metric_signature}_{datetime.utcnow().strftime('%Y%m%d%H')}"
        return hashlib.md5(signature.encode()).hexdigest()[:12]

    def _is_duplicate_incident(self, incident_id: str) -> bool:
        """Check if incident was recently created to avoid duplicates."""
        cutoff_time = datetime.utcnow() - timedelta(minutes=self.correlation_window_minutes)

        for historical_incident in self.incident_history:
            if (
                historical_incident["incident_id"] == incident_id
                and historical_incident["created_at"] >= cutoff_time
            ):
                return True

        return False


class ResponseOrchestrator:
    """Orchestrates automated incident response plans."""

    def __init__(self):
        self.logger = get_logger(f"{__name__}.response_orchestrator")
        self.structured_logger = StructuredLogger.get_logger(__name__)
        self.recovery_system = get_recovery_system()
        self.performance_monitor = get_performance_monitor()

        self.response_plans = self._initialize_response_plans()
        self.active_responses: dict[str, dict[str, Any]] = {}
        self.execution_history: deque = deque(maxlen=1000)

    def _initialize_response_plans(self) -> dict[str, ResponsePlan]:
        """Initialize automated response plans."""
        plans = {}

        # API Service Down Response Plan
        api_service_down = ResponsePlan(
            plan_id="api_service_down_response",
            name="API Service Recovery",
            description="Automated response for API service failures",
            incident_patterns=[
                {"pattern_id": "api_service_down"},
                {"category": IncidentCategory.SYSTEM_FAILURE, "service": "api"},
            ],
            response_actions=[
                ResponseAction(
                    action_id="check_service_health",
                    action_type="health_check",
                    description="Verify service health status",
                    timeout_seconds=60,
                    safety_checks=["verify_monitoring_connectivity"],
                ),
                ResponseAction(
                    action_id="restart_api_service",
                    action_type="service_restart",
                    description="Restart API service containers",
                    parameters={"service_name": "api_service", "graceful": True},
                    timeout_seconds=120,
                    requires_approval=False,
                    safety_checks=["check_backup_service", "verify_load_balancer"],
                ),
                ResponseAction(
                    action_id="scale_api_service",
                    action_type="horizontal_scale",
                    description="Scale up API service instances",
                    parameters={"service_name": "api_service", "scale_factor": 2.0},
                    timeout_seconds=180,
                    safety_checks=["check_resource_availability"],
                ),
                ResponseAction(
                    action_id="enable_circuit_breaker",
                    action_type="circuit_breaker",
                    description="Enable circuit breaker for failing endpoints",
                    parameters={"duration_minutes": 10},
                    timeout_seconds=30,
                ),
            ],
            automation_level=AutomationLevel.FULLY_AUTOMATED,
            success_criteria=["api_success_rate > 95", "api_response_time < 500"],
            priority=10,
        )
        plans[api_service_down.plan_id] = api_service_down

        # Performance Degradation Response Plan
        performance_degradation = ResponsePlan(
            plan_id="performance_degradation_response",
            name="Performance Recovery",
            description="Automated response for performance issues",
            incident_patterns=[
                {"pattern_id": "performance_degradation"},
                {"category": IncidentCategory.PERFORMANCE_DEGRADATION},
            ],
            response_actions=[
                ResponseAction(
                    action_id="analyze_resource_usage",
                    action_type="analysis",
                    description="Analyze current resource utilization",
                    timeout_seconds=60,
                ),
                ResponseAction(
                    action_id="clear_cache",
                    action_type="cache_clear",
                    description="Clear application caches",
                    parameters={"cache_types": ["application", "query"]},
                    timeout_seconds=60,
                ),
                ResponseAction(
                    action_id="scale_resources",
                    action_type="vertical_scale",
                    description="Increase CPU and memory allocation",
                    parameters={"cpu_increase": 50, "memory_increase": 25},
                    timeout_seconds=300,
                    requires_approval=True,
                ),
                ResponseAction(
                    action_id="enable_throttling",
                    action_type="rate_limit",
                    description="Enable request throttling",
                    parameters={"rate_limit": 1000, "duration_minutes": 15},
                    timeout_seconds=30,
                ),
            ],
            automation_level=AutomationLevel.SEMI_AUTOMATED,
            success_criteria=["api_response_time < 200", "system_cpu < 70"],
            priority=20,
        )
        plans[performance_degradation.plan_id] = performance_degradation

        # Data Quality Response Plan
        data_quality_response = ResponsePlan(
            plan_id="data_quality_response",
            name="Data Quality Recovery",
            description="Automated response for data quality issues",
            incident_patterns=[
                {"pattern_id": "data_quality_degradation"},
                {"category": IncidentCategory.DATA_QUALITY},
            ],
            response_actions=[
                ResponseAction(
                    action_id="pause_etl_pipeline",
                    action_type="pipeline_control",
                    description="Pause ETL pipeline to prevent bad data propagation",
                    parameters={"action": "pause", "pipeline": "data_processing"},
                    timeout_seconds=60,
                ),
                ResponseAction(
                    action_id="validate_data_sources",
                    action_type="data_validation",
                    description="Validate upstream data sources",
                    timeout_seconds=180,
                ),
                ResponseAction(
                    action_id="rollback_data_changes",
                    action_type="data_rollback",
                    description="Rollback recent data changes if needed",
                    parameters={"backup_retention_hours": 24},
                    timeout_seconds=600,
                    requires_approval=True,
                ),
                ResponseAction(
                    action_id="resume_etl_pipeline",
                    action_type="pipeline_control",
                    description="Resume ETL pipeline after fixes",
                    parameters={"action": "resume", "pipeline": "data_processing"},
                    timeout_seconds=60,
                ),
            ],
            automation_level=AutomationLevel.SEMI_AUTOMATED,
            success_criteria=["data_quality_score > 0.9", "etl_success_rate > 0.95"],
            priority=25,
        )
        plans[data_quality_response.plan_id] = data_quality_response

        # Capacity Exhaustion Response Plan
        capacity_response = ResponsePlan(
            plan_id="capacity_exhaustion_response",
            name="Capacity Management",
            description="Automated response for resource exhaustion",
            incident_patterns=[
                {"pattern_id": "capacity_exhaustion"},
                {"category": IncidentCategory.CAPACITY_EXHAUSTION},
            ],
            response_actions=[
                ResponseAction(
                    action_id="cleanup_temp_files",
                    action_type="filesystem_cleanup",
                    description="Clean up temporary files and logs",
                    timeout_seconds=120,
                ),
                ResponseAction(
                    action_id="emergency_scale_up",
                    action_type="emergency_scale",
                    description="Emergency resource scaling",
                    parameters={"scale_factor": 1.5, "max_instances": 20},
                    timeout_seconds=300,
                    requires_approval=False,  # Emergency scaling is automated
                ),
                ResponseAction(
                    action_id="enable_emergency_throttling",
                    action_type="emergency_throttle",
                    description="Enable aggressive request throttling",
                    parameters={"rate_limit": 100, "duration_minutes": 30},
                    timeout_seconds=30,
                ),
            ],
            automation_level=AutomationLevel.FULLY_AUTOMATED,
            success_criteria=["system_memory < 80", "system_disk < 80"],
            priority=5,  # Highest priority
        )
        plans[capacity_response.plan_id] = capacity_response

        # Database Issue Response Plan
        database_response = ResponsePlan(
            plan_id="database_issue_response",
            name="Database Recovery",
            description="Automated response for database issues",
            incident_patterns=[
                {"pattern_id": "database_performance_issue"},
                {"category": IncidentCategory.DATABASE_ISSUE},
            ],
            response_actions=[
                ResponseAction(
                    action_id="analyze_db_performance",
                    action_type="db_analysis",
                    description="Analyze database performance metrics",
                    timeout_seconds=60,
                ),
                ResponseAction(
                    action_id="reset_connection_pool",
                    action_type="connection_reset",
                    description="Reset database connection pool",
                    timeout_seconds=60,
                ),
                ResponseAction(
                    action_id="scale_db_connections",
                    action_type="db_scale",
                    description="Increase database connection pool size",
                    parameters={"connection_increase": 20},
                    timeout_seconds=60,
                ),
                ResponseAction(
                    action_id="enable_read_replicas",
                    action_type="db_failover",
                    description="Route read queries to replicas",
                    parameters={"replica_weight": 70},
                    timeout_seconds=120,
                    requires_approval=False,
                ),
            ],
            automation_level=AutomationLevel.FULLY_AUTOMATED,
            success_criteria=["db_response_time < 100", "db_pool_usage < 70"],
            priority=15,
        )
        plans[database_response.plan_id] = database_response

        return plans

    async def execute_response_plan(self, incident: Incident) -> bool:
        """Execute the best matching response plan for an incident."""
        try:
            # Find matching response plan
            response_plan = self._find_matching_plan(incident)
            if not response_plan:
                self.logger.warning(
                    f"No matching response plan found for incident {incident.incident_id}"
                )
                return False

            incident.assigned_plan = response_plan.plan_id

            context = LogContext(
                operation="incident_response",
                component="response_orchestrator",
                correlation_id=incident.incident_id,
            )

            with log_context(context):
                self.structured_logger.info(
                    f"Executing response plan {response_plan.name} for incident {incident.incident_id}",
                    extra={
                        "category": "incident_response",
                        "event_type": "plan_execution_started",
                        "incident_id": incident.incident_id,
                        "plan_id": response_plan.plan_id,
                        "automation_level": response_plan.automation_level.value,
                    },
                )

            # Check automation level and requirements
            if response_plan.automation_level == AutomationLevel.MANUAL_ONLY:
                await self._create_manual_alert(incident, response_plan)
                return True

            # Execute response actions
            success = await self._execute_response_actions(incident, response_plan)

            # Update incident status
            if success:
                incident.status = IncidentStatus.MITIGATING
                self.structured_logger.info(
                    f"Response plan executed successfully for incident {incident.incident_id}",
                    extra={
                        "category": "incident_response",
                        "event_type": "plan_execution_success",
                        "incident_id": incident.incident_id,
                    },
                )
            else:
                self.structured_logger.error(
                    f"Response plan execution failed for incident {incident.incident_id}",
                    extra={
                        "category": "incident_response",
                        "event_type": "plan_execution_failed",
                        "incident_id": incident.incident_id,
                    },
                )

            return success

        except Exception as e:
            self.logger.error(
                f"Error executing response plan for incident {incident.incident_id}: {str(e)}"
            )
            return False

    def _find_matching_plan(self, incident: Incident) -> ResponsePlan | None:
        """Find the best matching response plan for an incident."""
        matching_plans = []

        for plan in self.response_plans.values():
            if not plan.enabled:
                continue

            for pattern in plan.incident_patterns:
                if self._matches_pattern(incident, pattern):
                    matching_plans.append((plan, plan.priority))
                    break

        if not matching_plans:
            return None

        # Sort by priority (lower number = higher priority) and return best match
        matching_plans.sort(key=lambda x: x[1])
        return matching_plans[0][0]

    def _matches_pattern(self, incident: Incident, pattern: dict[str, Any]) -> bool:
        """Check if incident matches a response plan pattern."""
        # Check pattern ID match
        if "pattern_id" in pattern:
            if incident.tags.get("pattern_id") == pattern["pattern_id"]:
                return True

        # Check category match
        if "category" in pattern:
            if incident.category.value == pattern["category"]:
                return True

        # Check severity match
        if "severity" in pattern:
            if incident.severity.value == pattern["severity"]:
                return True

        # Check service match
        if "service" in pattern:
            if incident.context.service_name == pattern["service"]:
                return True

        return False

    async def _execute_response_actions(
        self, incident: Incident, response_plan: ResponsePlan
    ) -> bool:
        """Execute all response actions in a plan."""
        plan_start_time = datetime.utcnow()
        all_actions_successful = True

        self.active_responses[incident.incident_id] = {
            "plan": response_plan,
            "started_at": plan_start_time,
            "actions_completed": 0,
            "actions_total": len(response_plan.response_actions),
        }

        try:
            for action in response_plan.response_actions:
                # Check if we should continue based on previous failures
                if not all_actions_successful and not action.action_type.startswith("cleanup"):
                    self.logger.warning(
                        f"Skipping action {action.action_id} due to previous failures"
                    )
                    continue

                # Execute action
                execution_success = await self._execute_single_action(incident, action)

                if not execution_success:
                    all_actions_successful = False

                    # Check if this is a critical action
                    if action.action_type in ["emergency_scale", "service_restart", "failover"]:
                        self.logger.error(
                            f"Critical action {action.action_id} failed, stopping plan execution"
                        )
                        break

                self.active_responses[incident.incident_id]["actions_completed"] += 1

                # Check success criteria after each action
                if await self._check_success_criteria(incident, response_plan):
                    self.logger.info(f"Success criteria met after action {action.action_id}")
                    all_actions_successful = True
                    break

                # Add delay between actions for system stabilization
                await asyncio.sleep(2)

            # Final success criteria check
            final_success = await self._check_success_criteria(incident, response_plan)

            return final_success and all_actions_successful

        finally:
            # Clean up active response tracking
            if incident.incident_id in self.active_responses:
                del self.active_responses[incident.incident_id]

    async def _execute_single_action(self, incident: Incident, action: ResponseAction) -> bool:
        """Execute a single response action."""
        execution = ActionExecution(
            execution_id=str(uuid.uuid4()), action_id=action.action_id, started_at=datetime.utcnow()
        )

        incident.action_executions.append(execution)

        try:
            self.logger.info(f"Executing action {action.action_id}: {action.description}")

            # Check safety conditions before execution
            if not await self._check_safety_conditions(action):
                execution.status = ResponseStatus.CANCELLED
                execution.error_message = "Safety conditions not met"
                return False

            execution.status = ResponseStatus.RUNNING

            # Execute the action based on type
            result = await self._dispatch_action(action, incident)

            execution.completed_at = datetime.utcnow()
            execution.duration_seconds = (
                execution.completed_at - execution.started_at
            ).total_seconds()
            execution.result = result
            execution.status = (
                ResponseStatus.SUCCESS if result.get("success", False) else ResponseStatus.FAILED
            )

            if execution.status == ResponseStatus.SUCCESS:
                self.logger.info(f"Action {action.action_id} completed successfully")
                return True
            else:
                self.logger.error(
                    f"Action {action.action_id} failed: {result.get('error', 'Unknown error')}"
                )
                execution.error_message = result.get("error", "Unknown error")
                return False

        except asyncio.TimeoutError:
            execution.status = ResponseStatus.TIMEOUT
            execution.error_message = "Action timed out"
            self.logger.error(f"Action {action.action_id} timed out")
            return False
        except Exception as e:
            execution.status = ResponseStatus.FAILED
            execution.error_message = str(e)
            self.logger.error(f"Action {action.action_id} failed with exception: {str(e)}")
            return False

    async def _dispatch_action(self, action: ResponseAction, incident: Incident) -> dict[str, Any]:
        """Dispatch action to appropriate handler."""

        # Integration with recovery system
        if action.action_type in ["service_restart", "horizontal_scale", "vertical_scale"]:
            return await self.recovery_system.execute_recovery_action(
                action.action_type, action.parameters
            )

        # Simulate other action types for now
        action_handlers = {
            "health_check": self._simulate_health_check,
            "cache_clear": self._simulate_cache_clear,
            "circuit_breaker": self._simulate_circuit_breaker,
            "rate_limit": self._simulate_rate_limit,
            "pipeline_control": self._simulate_pipeline_control,
            "data_validation": self._simulate_data_validation,
            "data_rollback": self._simulate_data_rollback,
            "filesystem_cleanup": self._simulate_filesystem_cleanup,
            "emergency_scale": self._simulate_emergency_scale,
            "emergency_throttle": self._simulate_emergency_throttle,
            "db_analysis": self._simulate_db_analysis,
            "connection_reset": self._simulate_connection_reset,
            "db_scale": self._simulate_db_scale,
            "db_failover": self._simulate_db_failover,
            "analysis": self._simulate_analysis,
        }

        handler = action_handlers.get(action.action_type, self._simulate_generic_action)
        return await handler(action, incident)

    async def _check_safety_conditions(self, action: ResponseAction) -> bool:
        """Check safety conditions before executing an action."""
        for safety_check in action.safety_checks:
            # Implement safety checks - for now, simulate
            self.logger.info(f"Checking safety condition: {safety_check}")
            # In production, implement actual safety checks
            await asyncio.sleep(0.1)  # Simulate check time

        return True

    async def _check_success_criteria(
        self, incident: Incident, response_plan: ResponsePlan
    ) -> bool:
        """Check if response plan success criteria are met."""
        if not response_plan.success_criteria:
            return False

        # Get current metrics from performance monitor
        try:
            summary = self.performance_monitor.get_performance_summary(hours=0.1)  # Last 6 minutes
            current_metrics = summary.get("metrics_summary", {})

            for criterion in response_plan.success_criteria:
                # Parse criterion (e.g., "api_response_time < 500")
                if not await self._evaluate_criterion(criterion, current_metrics):
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error checking success criteria: {str(e)}")
            return False

    async def _evaluate_criterion(self, criterion: str, metrics: dict[str, Any]) -> bool:
        """Evaluate a success criterion against current metrics."""
        # Simple parser for criteria like "api_response_time < 500"
        try:
            parts = criterion.strip().split()
            if len(parts) != 3:
                return False

            metric_name, operator, threshold_str = parts
            threshold = float(threshold_str)

            if metric_name not in metrics:
                return False

            current_value = metrics[metric_name].get("current", 0)

            if operator == "<":
                return current_value < threshold
            elif operator == ">":
                return current_value > threshold
            elif operator == "<=":
                return current_value <= threshold
            elif operator == ">=":
                return current_value >= threshold
            elif operator == "==":
                return current_value == threshold

            return False

        except (ValueError, KeyError):
            return False

    # Simulation methods for different action types
    async def _simulate_health_check(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate health check action."""
        await asyncio.sleep(1)  # Simulate check time
        return {"success": True, "status": "healthy", "checks": ["connectivity", "response_time"]}

    async def _simulate_cache_clear(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate cache clear action."""
        await asyncio.sleep(2)
        cache_types = action.parameters.get("cache_types", ["application"])
        return {"success": True, "caches_cleared": cache_types, "keys_removed": 1500}

    async def _simulate_circuit_breaker(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate circuit breaker activation."""
        await asyncio.sleep(0.5)
        duration = action.parameters.get("duration_minutes", 5)
        return {"success": True, "circuit_breaker_enabled": True, "duration_minutes": duration}

    async def _simulate_rate_limit(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate rate limiting activation."""
        await asyncio.sleep(0.5)
        rate_limit = action.parameters.get("rate_limit", 1000)
        return {"success": True, "rate_limit_enabled": True, "limit": rate_limit}

    async def _simulate_pipeline_control(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate ETL pipeline control."""
        await asyncio.sleep(3)
        action_type = action.parameters.get("action", "pause")
        pipeline = action.parameters.get("pipeline", "default")
        return {"success": True, "pipeline": pipeline, "action": action_type}

    async def _simulate_data_validation(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate data validation."""
        await asyncio.sleep(5)
        return {"success": True, "validation_passed": True, "issues_found": 2, "issues_fixed": 2}

    async def _simulate_data_rollback(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate data rollback."""
        await asyncio.sleep(10)
        return {"success": True, "rollback_completed": True, "records_restored": 10000}

    async def _simulate_filesystem_cleanup(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate filesystem cleanup."""
        await asyncio.sleep(3)
        return {"success": True, "space_freed_mb": 2048, "files_cleaned": 150}

    async def _simulate_emergency_scale(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate emergency scaling."""
        await asyncio.sleep(8)
        scale_factor = action.parameters.get("scale_factor", 1.5)
        return {"success": True, "scaled": True, "scale_factor": scale_factor, "new_instances": 6}

    async def _simulate_emergency_throttle(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate emergency throttling."""
        await asyncio.sleep(1)
        rate_limit = action.parameters.get("rate_limit", 100)
        return {"success": True, "throttling_enabled": True, "rate_limit": rate_limit}

    async def _simulate_db_analysis(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate database analysis."""
        await asyncio.sleep(4)
        return {
            "success": True,
            "slow_queries": 3,
            "blocking_queries": 1,
            "recommendations": ["add_index"],
        }

    async def _simulate_connection_reset(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate connection pool reset."""
        await asyncio.sleep(2)
        return {"success": True, "connections_reset": 50, "pool_reinitialized": True}

    async def _simulate_db_scale(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate database scaling."""
        await asyncio.sleep(3)
        return {"success": True, "connection_pool_scaled": True, "new_max_connections": 100}

    async def _simulate_db_failover(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate database failover to replicas."""
        await asyncio.sleep(4)
        return {"success": True, "failover_completed": True, "read_replicas_enabled": True}

    async def _simulate_analysis(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate general analysis action."""
        await asyncio.sleep(3)
        return {
            "success": True,
            "analysis_completed": True,
            "findings": ["high_cpu", "memory_pressure"],
        }

    async def _simulate_generic_action(
        self, action: ResponseAction, incident: Incident
    ) -> dict[str, Any]:
        """Simulate generic action."""
        await asyncio.sleep(2)
        return {"success": True, "action_type": action.action_type, "simulated": True}

    async def _create_manual_alert(self, incident: Incident, response_plan: ResponsePlan):
        """Create manual alert for incidents requiring human intervention."""
        self.structured_logger.warning(
            f"Manual intervention required for incident {incident.incident_id}",
            extra={
                "category": "incident_response",
                "event_type": "manual_intervention_required",
                "incident_id": incident.incident_id,
                "severity": incident.severity.value,
                "response_plan": response_plan.name,
            },
        )


class AutomatedIncidentResponseSystem:
    """Main automated incident response system coordinator."""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.structured_logger = StructuredLogger.get_logger(__name__)

        # Components
        self.detector = IncidentDetector()
        self.orchestrator = ResponseOrchestrator()
        self.custom_metrics = get_custom_metrics_advanced()
        self.performance_monitor = get_performance_monitor()

        # State management
        self.active_incidents: dict[str, Incident] = {}
        self.incident_history: deque = deque(maxlen=10000)
        self.system_active = False

        # Configuration
        self.evaluation_interval_seconds = 30
        self.max_concurrent_responses = 5

        self.logger.info("Automated incident response system initialized")

    async def start_system(self):
        """Start the automated incident response system."""
        self.system_active = True

        # Start monitoring loop
        asyncio.create_task(self._monitoring_loop())

        self.logger.info("Automated incident response system started")

        self.structured_logger.info(
            "Incident response system activated",
            extra={
                "category": "incident_response",
                "event_type": "system_started",
                "evaluation_interval": self.evaluation_interval_seconds,
            },
        )

    async def stop_system(self):
        """Stop the automated incident response system."""
        self.system_active = False
        self.logger.info("Automated incident response system stopped")

    async def _monitoring_loop(self):
        """Main monitoring loop for incident detection and response."""
        while self.system_active:
            try:
                await asyncio.sleep(self.evaluation_interval_seconds)

                # Get current metrics from performance monitor
                summary = self.performance_monitor.get_performance_summary(hours=0.1)
                current_metrics = {}

                # Extract current metric values
                for metric_name, metric_data in summary.get("metrics_summary", {}).items():
                    current_metrics[metric_name] = metric_data.get("current", 0)

                # Detect incidents
                detected_incidents = await self.detector.evaluate_metrics(current_metrics)

                # Process new incidents
                for incident in detected_incidents:
                    await self._handle_new_incident(incident)

                # Check status of active incidents
                await self._check_active_incidents()

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {str(e)}")
                await asyncio.sleep(self.evaluation_interval_seconds)

    async def _handle_new_incident(self, incident: Incident):
        """Handle a newly detected incident."""
        if incident.incident_id in self.active_incidents:
            return  # Already handling this incident

        self.active_incidents[incident.incident_id] = incident
        self.incident_history.append(incident)

        context = LogContext(
            operation="incident_detected",
            component="incident_response_system",
            correlation_id=incident.incident_id,
        )

        with log_context(context):
            self.structured_logger.warning(
                f"New incident detected: {incident.title}",
                extra={
                    "category": "incident_response",
                    "event_type": "incident_detected",
                    "incident_id": incident.incident_id,
                    "severity": incident.severity.value,
                    "category": incident.category.value,
                    "affected_service": incident.context.service_name,
                },
            )

        # Submit incident metrics
        await self.custom_metrics.submit_metric(
            "incidents_detected_total",
            1,
            tags={
                "severity": incident.severity.value,
                "category": incident.category.value,
                "service": incident.context.service_name,
            },
        )

        # Execute response plan
        if (
            len(
                [
                    i
                    for i in self.active_incidents.values()
                    if i.status in [IncidentStatus.INVESTIGATING, IncidentStatus.MITIGATING]
                ]
            )
            < self.max_concurrent_responses
        ):
            incident.status = IncidentStatus.INVESTIGATING
            response_success = await self.orchestrator.execute_response_plan(incident)

            if response_success:
                await self.custom_metrics.submit_metric(
                    "incident_responses_executed_total",
                    1,
                    tags={"incident_id": incident.incident_id},
                )
        else:
            self.logger.warning(
                f"Max concurrent responses reached, queueing incident {incident.incident_id}"
            )

    async def _check_active_incidents(self):
        """Check status of active incidents and update as needed."""
        completed_incidents = []

        for incident_id, incident in self.active_incidents.items():
            if incident.status in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]:
                continue

            # Check if incident should be resolved based on metrics
            if await self._should_resolve_incident(incident):
                await self._resolve_incident(incident)
                completed_incidents.append(incident_id)

            # Check for escalation conditions
            elif await self._should_escalate_incident(incident):
                await self._escalate_incident(incident)

        # Clean up completed incidents
        for incident_id in completed_incidents:
            if incident_id in self.active_incidents:
                del self.active_incidents[incident_id]

    async def _should_resolve_incident(self, incident: Incident) -> bool:
        """Check if incident should be automatically resolved."""
        if incident.status != IncidentStatus.MITIGATING:
            return False

        # Check if success criteria are met for sufficient time
        incident_duration = datetime.utcnow() - incident.created_at

        if incident_duration < timedelta(minutes=5):
            return False  # Need minimum time for resolution

        # Check metrics are back to normal
        try:
            summary = self.performance_monitor.get_performance_summary(hours=0.1)
            current_metrics = {}

            for metric_name, metric_data in summary.get("metrics_summary", {}).items():
                current_metrics[metric_name] = metric_data.get("current", 0)

            # Check if incident conditions are no longer met
            for incident_metric in incident.metrics:
                if incident_metric.metric_name in current_metrics:
                    current_value = current_metrics[incident_metric.metric_name]

                    # If metric is still problematic, don't resolve
                    if incident_metric.threshold_exceeded:
                        if (
                            current_value >= incident_metric.expected_value
                            and incident_metric.current_value >= incident_metric.expected_value
                        ):
                            return False
                        elif (
                            current_value <= incident_metric.expected_value
                            and incident_metric.current_value <= incident_metric.expected_value
                        ):
                            return False

            return True

        except Exception as e:
            self.logger.error(
                f"Error checking incident resolution for {incident.incident_id}: {str(e)}"
            )
            return False

    async def _should_escalate_incident(self, incident: Incident) -> bool:
        """Check if incident should be escalated."""
        if incident.escalated:
            return False

        # Escalate P1 incidents that aren't resolved within 15 minutes
        if (
            incident.severity == IncidentSeverity.P1_CRITICAL
            and datetime.utcnow() - incident.created_at > timedelta(minutes=15)
        ):
            return True

        # Escalate any incident that's been open for more than 1 hour
        if datetime.utcnow() - incident.created_at > timedelta(hours=1):
            return True

        # Escalate if multiple failed response attempts
        failed_actions = [
            action
            for action in incident.action_executions
            if action.status == ResponseStatus.FAILED
        ]
        if len(failed_actions) >= 3:
            return True

        return False

    async def _resolve_incident(self, incident: Incident):
        """Mark incident as resolved."""
        incident.status = IncidentStatus.RESOLVED
        incident.resolved_at = datetime.utcnow()
        incident.updated_at = datetime.utcnow()

        resolution_time = incident.resolved_at - incident.created_at

        self.structured_logger.info(
            f"Incident resolved: {incident.title}",
            extra={
                "category": "incident_response",
                "event_type": "incident_resolved",
                "incident_id": incident.incident_id,
                "resolution_time_minutes": resolution_time.total_seconds() / 60,
                "actions_executed": len(incident.action_executions),
            },
        )

        # Submit resolution metrics
        await self.custom_metrics.submit_metric(
            "incidents_resolved_total",
            1,
            tags={"severity": incident.severity.value, "category": incident.category.value},
        )

        await self.custom_metrics.submit_metric(
            "incident_resolution_time_minutes",
            resolution_time.total_seconds() / 60,
            tags={"severity": incident.severity.value, "category": incident.category.value},
        )

    async def _escalate_incident(self, incident: Incident):
        """Escalate incident for human intervention."""
        incident.escalated = True
        incident.updated_at = datetime.utcnow()

        self.structured_logger.critical(
            f"Incident escalated: {incident.title}",
            extra={
                "category": "incident_response",
                "event_type": "incident_escalated",
                "incident_id": incident.incident_id,
                "time_since_creation_minutes": (
                    datetime.utcnow() - incident.created_at
                ).total_seconds()
                / 60,
                "failed_actions": len(
                    [a for a in incident.action_executions if a.status == ResponseStatus.FAILED]
                ),
            },
        )

        # Create audit event for escalation
        audit_event = AuditEvent(
            action="incident_escalated",
            resource=f"incident_{incident.incident_id}",
            result="escalated",
            metadata={
                "severity": incident.severity.value,
                "category": incident.category.value,
                "duration_minutes": (datetime.utcnow() - incident.created_at).total_seconds() / 60,
            },
        )

        self.structured_logger.log_audit(audit_event)

        # Submit escalation metrics
        await self.custom_metrics.submit_metric(
            "incidents_escalated_total",
            1,
            tags={"severity": incident.severity.value, "category": incident.category.value},
        )

    def get_system_status(self) -> dict[str, Any]:
        """Get current system status and statistics."""
        try:
            current_time = datetime.utcnow()

            # Active incidents summary
            active_by_severity = defaultdict(int)
            active_by_category = defaultdict(int)

            for incident in self.active_incidents.values():
                active_by_severity[incident.severity.value] += 1
                active_by_category[incident.category.value] += 1

            # Recent incident statistics (last 24 hours)
            recent_cutoff = current_time - timedelta(hours=24)
            recent_incidents = [
                incident
                for incident in self.incident_history
                if incident.created_at >= recent_cutoff
            ]

            total_recent = len(recent_incidents)
            resolved_recent = len(
                [i for i in recent_incidents if i.status == IncidentStatus.RESOLVED]
            )
            escalated_recent = len([i for i in recent_incidents if i.escalated])

            # Response plan effectiveness
            response_plans_used = defaultdict(int)
            successful_responses = defaultdict(int)

            for incident in recent_incidents:
                if incident.assigned_plan:
                    response_plans_used[incident.assigned_plan] += 1
                    if incident.status == IncidentStatus.RESOLVED:
                        successful_responses[incident.assigned_plan] += 1

            return {
                "system_active": self.system_active,
                "timestamp": current_time.isoformat(),
                "active_incidents": {
                    "total": len(self.active_incidents),
                    "by_severity": dict(active_by_severity),
                    "by_category": dict(active_by_category),
                },
                "recent_24h": {
                    "total_incidents": total_recent,
                    "resolved": resolved_recent,
                    "escalated": escalated_recent,
                    "resolution_rate": (resolved_recent / total_recent * 100)
                    if total_recent > 0
                    else 0,
                },
                "response_plans": {
                    "total_available": len(self.orchestrator.response_plans),
                    "usage_stats": dict(response_plans_used),
                    "success_rates": {
                        plan_id: (
                            successful_responses[plan_id] / response_plans_used[plan_id] * 100
                        )
                        if response_plans_used[plan_id] > 0
                        else 0
                        for plan_id in response_plans_used.keys()
                    },
                },
                "configuration": {
                    "evaluation_interval_seconds": self.evaluation_interval_seconds,
                    "max_concurrent_responses": self.max_concurrent_responses,
                    "detection_patterns": len(self.detector.detection_patterns),
                },
            }

        except Exception as e:
            self.logger.error(f"Error generating system status: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


# Global incident response system
_incident_response_system = None


def get_incident_response_system() -> AutomatedIncidentResponseSystem:
    """Get global incident response system instance."""
    global _incident_response_system
    if _incident_response_system is None:
        _incident_response_system = AutomatedIncidentResponseSystem()
    return _incident_response_system


if __name__ == "__main__":
    # Example usage
    import asyncio

    async def test_incident_response():
        system = get_incident_response_system()

        # Start the system
        await system.start_system()

        # Simulate some metrics that would trigger incidents

        # Wait for detection and response
        await asyncio.sleep(35)  # Wait for one evaluation cycle

        # Get system status
        status = system.get_system_status()
        print("System Status:")
        print(json.dumps(status, indent=2))

        # Wait a bit more to see resolution
        await asyncio.sleep(60)

        final_status = system.get_system_status()
        print("\nFinal Status:")
        print(json.dumps(final_status, indent=2))

        await system.stop_system()

    # Run test
    asyncio.run(test_incident_response())
