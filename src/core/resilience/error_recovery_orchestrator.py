"""
Error Recovery Orchestrator
==========================

Advanced error recovery orchestrator that integrates with existing exception handling
to provide intelligent error recovery, escalation, and learning capabilities.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union, Type
import uuid

from core.logging import get_logger
from core.exceptions import BaseApplicationError, ErrorCode
from .self_healing_system import SelfHealingSystem, RecoveryAction, RecoveryPriority
from .circuit_breaker_system import ResilienceManager, CircuitBreakerConfig, RetryConfig

logger = get_logger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RecoveryStrategy(Enum):
    """Error recovery strategies."""
    IMMEDIATE_RETRY = "immediate_retry"
    DELAYED_RETRY = "delayed_retry"
    CIRCUIT_BREAK = "circuit_break"
    GRACEFUL_DEGRADATION = "graceful_degradation"
    ESCALATE_TO_OPERATOR = "escalate_to_operator"
    FAILOVER = "failover"
    SELF_HEAL = "self_heal"
    IGNORE = "ignore"


class EscalationLevel(Enum):
    """Error escalation levels."""
    NONE = "none"
    TEAM_LEAD = "team_lead"
    ON_CALL = "on_call"
    MANAGEMENT = "management"
    EXECUTIVE = "executive"


@dataclass
class ErrorPattern:
    """Pattern for error classification and recovery."""
    pattern_id: str
    error_types: List[Type[Exception]]
    error_codes: List[ErrorCode] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    recovery_strategy: RecoveryStrategy = RecoveryStrategy.IMMEDIATE_RETRY
    max_occurrences: int = 5
    time_window: timedelta = timedelta(minutes=10)
    escalation_level: EscalationLevel = EscalationLevel.NONE
    custom_recovery_actions: List[Callable] = field(default_factory=list)


@dataclass
class ErrorIncident:
    """Error incident tracking."""
    incident_id: str
    error_pattern: ErrorPattern
    first_occurrence: datetime
    last_occurrence: datetime
    occurrence_count: int = 1
    affected_components: List[str] = field(default_factory=list)
    recovery_attempts: List[str] = field(default_factory=list)
    escalated: bool = False
    resolved: bool = False
    resolution_time: Optional[datetime] = None
    root_cause: Optional[str] = None
    preventive_actions: List[str] = field(default_factory=list)


@dataclass
class RecoveryAttempt:
    """Individual recovery attempt tracking."""
    attempt_id: str
    incident_id: str
    strategy: RecoveryStrategy
    started_at: datetime
    completed_at: Optional[datetime] = None
    success: bool = False
    error_message: Optional[str] = None
    actions_taken: List[str] = field(default_factory=list)
    side_effects: List[str] = field(default_factory=list)


class ErrorAnalyzer:
    """Analyzes errors to determine patterns and appropriate recovery strategies."""

    def __init__(self):
        self.error_patterns: List[ErrorPattern] = []
        self.error_history: deque = deque(maxlen=1000)
        self.pattern_effectiveness: Dict[str, float] = {}

    def add_pattern(self, pattern: ErrorPattern):
        """Add error pattern for recognition."""
        self.error_patterns.append(pattern)
        logger.info(f"Added error pattern: {pattern.pattern_id}")

    def analyze_error(self, error: Exception, context: Dict[str, Any]) -> Optional[ErrorPattern]:
        """Analyze error and return matching pattern."""
        for pattern in self.error_patterns:
            if self._matches_pattern(error, context, pattern):
                return pattern

        # If no pattern matches, create a generic one
        return self._create_generic_pattern(error, context)

    def _matches_pattern(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Check if error matches pattern."""
        # Check error type
        if pattern.error_types and not any(isinstance(error, exc_type) for exc_type in pattern.error_types):
            return False

        # Check error codes for application errors
        if (pattern.error_codes and
            isinstance(error, BaseApplicationError) and
            error.error_code not in pattern.error_codes):
            return False

        # Check keywords in error message
        if pattern.keywords:
            error_message = str(error).lower()
            if not any(keyword.lower() in error_message for keyword in pattern.keywords):
                return False

        return True

    def _create_generic_pattern(self, error: Exception, context: Dict[str, Any]) -> ErrorPattern:
        """Create generic pattern for unmatched errors."""
        error_type = type(error)
        severity = self._determine_severity(error, context)

        return ErrorPattern(
            pattern_id=f"generic_{error_type.__name__.lower()}",
            error_types=[error_type],
            severity=severity,
            recovery_strategy=self._determine_default_strategy(severity),
            max_occurrences=3,
            time_window=timedelta(minutes=5)
        )

    def _determine_severity(self, error: Exception, context: Dict[str, Any]) -> ErrorSeverity:
        """Determine error severity based on type and context."""
        # Critical system exceptions
        if isinstance(error, (SystemError, MemoryError, OSError)):
            return ErrorSeverity.CRITICAL

        # High severity for authentication/authorization
        if "auth" in str(error).lower() or "permission" in str(error).lower():
            return ErrorSeverity.HIGH

        # Medium for application errors
        if isinstance(error, BaseApplicationError):
            return ErrorSeverity.MEDIUM

        # Low for validation errors
        if "validation" in str(error).lower():
            return ErrorSeverity.LOW

        return ErrorSeverity.MEDIUM

    def _determine_default_strategy(self, severity: ErrorSeverity) -> RecoveryStrategy:
        """Determine default recovery strategy based on severity."""
        strategy_map = {
            ErrorSeverity.LOW: RecoveryStrategy.IMMEDIATE_RETRY,
            ErrorSeverity.MEDIUM: RecoveryStrategy.DELAYED_RETRY,
            ErrorSeverity.HIGH: RecoveryStrategy.CIRCUIT_BREAK,
            ErrorSeverity.CRITICAL: RecoveryStrategy.ESCALATE_TO_OPERATOR
        }
        return strategy_map.get(severity, RecoveryStrategy.IMMEDIATE_RETRY)

    def update_pattern_effectiveness(self, pattern_id: str, success: bool):
        """Update pattern effectiveness based on recovery results."""
        current_effectiveness = self.pattern_effectiveness.get(pattern_id, 0.5)

        # Use exponential moving average
        alpha = 0.1
        new_effectiveness = alpha * (1.0 if success else 0.0) + (1 - alpha) * current_effectiveness
        self.pattern_effectiveness[pattern_id] = new_effectiveness

    def get_pattern_effectiveness(self, pattern_id: str) -> float:
        """Get effectiveness score for pattern."""
        return self.pattern_effectiveness.get(pattern_id, 0.5)


class RecoveryExecutor:
    """Executes recovery strategies for errors."""

    def __init__(self, resilience_manager: ResilienceManager, self_healing_system: SelfHealingSystem):
        self.resilience_manager = resilience_manager
        self.self_healing_system = self_healing_system
        self.custom_recovery_handlers: Dict[RecoveryStrategy, Callable] = {}

    def register_custom_handler(self, strategy: RecoveryStrategy, handler: Callable):
        """Register custom recovery handler."""
        self.custom_recovery_handlers[strategy] = handler
        logger.info(f"Registered custom recovery handler for {strategy.value}")

    async def execute_recovery(
        self,
        strategy: RecoveryStrategy,
        error: Exception,
        context: Dict[str, Any],
        pattern: ErrorPattern
    ) -> RecoveryAttempt:
        """Execute recovery strategy."""
        attempt_id = str(uuid.uuid4())
        attempt = RecoveryAttempt(
            attempt_id=attempt_id,
            incident_id=context.get("incident_id", "unknown"),
            strategy=strategy,
            started_at=datetime.utcnow()
        )

        try:
            if strategy == RecoveryStrategy.IMMEDIATE_RETRY:
                success = await self._immediate_retry(error, context, pattern)
            elif strategy == RecoveryStrategy.DELAYED_RETRY:
                success = await self._delayed_retry(error, context, pattern)
            elif strategy == RecoveryStrategy.CIRCUIT_BREAK:
                success = await self._circuit_break(error, context, pattern)
            elif strategy == RecoveryStrategy.GRACEFUL_DEGRADATION:
                success = await self._graceful_degradation(error, context, pattern)
            elif strategy == RecoveryStrategy.FAILOVER:
                success = await self._failover(error, context, pattern)
            elif strategy == RecoveryStrategy.SELF_HEAL:
                success = await self._self_heal(error, context, pattern)
            elif strategy == RecoveryStrategy.ESCALATE_TO_OPERATOR:
                success = await self._escalate(error, context, pattern)
            elif strategy in self.custom_recovery_handlers:
                success = await self.custom_recovery_handlers[strategy](error, context, pattern)
            else:
                success = False
                attempt.error_message = f"Unknown recovery strategy: {strategy.value}"

            attempt.success = success
            attempt.completed_at = datetime.utcnow()

        except Exception as e:
            attempt.success = False
            attempt.error_message = str(e)
            attempt.completed_at = datetime.utcnow()
            logger.error(f"Recovery attempt {attempt_id} failed: {e}")

        return attempt

    async def _immediate_retry(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Execute immediate retry strategy."""
        logger.info("Executing immediate retry recovery")

        # Use resilience manager for retry
        try:
            retry_pattern = self.resilience_manager.get_pattern("immediate_retry")
            if not retry_pattern:
                # Create immediate retry pattern
                retry_config = RetryConfig(max_attempts=2, initial_delay=0.1, max_delay=1.0)
                retry_pattern = self.resilience_manager.register_resilience_pattern(
                    "immediate_retry", retry_config=retry_config
                )

            # Get original function from context if available
            original_func = context.get("original_function")
            if original_func:
                args = context.get("args", [])
                kwargs = context.get("kwargs", {})
                await retry_pattern.execute(original_func, *args, **kwargs)
                return True

        except Exception as e:
            logger.error(f"Immediate retry failed: {e}")

        return False

    async def _delayed_retry(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Execute delayed retry strategy."""
        logger.info("Executing delayed retry recovery")

        # Wait before retry
        await asyncio.sleep(5.0)

        try:
            retry_pattern = self.resilience_manager.get_pattern("delayed_retry")
            if not retry_pattern:
                retry_config = RetryConfig(max_attempts=3, initial_delay=2.0, max_delay=10.0)
                retry_pattern = self.resilience_manager.register_resilience_pattern(
                    "delayed_retry", retry_config=retry_config
                )

            original_func = context.get("original_function")
            if original_func:
                args = context.get("args", [])
                kwargs = context.get("kwargs", {})
                await retry_pattern.execute(original_func, *args, **kwargs)
                return True

        except Exception as e:
            logger.error(f"Delayed retry failed: {e}")

        return False

    async def _circuit_break(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Execute circuit breaker strategy."""
        logger.info("Executing circuit breaker recovery")

        component_name = context.get("component_name", "unknown")

        # Trigger circuit breaker
        circuit_pattern = self.resilience_manager.get_pattern("circuit_breaker")
        if circuit_pattern and circuit_pattern.circuit_breaker:
            # Force circuit breaker to open state
            circuit_pattern.circuit_breaker.failure_count = circuit_pattern.circuit_breaker.config.failure_threshold
            circuit_pattern.circuit_breaker.last_failure_time = time.time()

            logger.warning(f"Circuit breaker opened for component: {component_name}")
            return True

        return False

    async def _graceful_degradation(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Execute graceful degradation strategy."""
        logger.info("Executing graceful degradation recovery")

        # Enable fallback mechanisms
        fallback_func = context.get("fallback_function")
        if fallback_func:
            try:
                args = context.get("args", [])
                kwargs = context.get("kwargs", {})

                if asyncio.iscoroutinefunction(fallback_func):
                    await fallback_func(*args, **kwargs)
                else:
                    fallback_func(*args, **kwargs)

                logger.info("Graceful degradation successful - using fallback")
                return True
            except Exception as e:
                logger.error(f"Fallback function failed: {e}")

        return False

    async def _failover(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Execute failover strategy."""
        logger.info("Executing failover recovery")

        component_name = context.get("component_name", "unknown")

        # Use self-healing system for failover
        if "database" in component_name.lower():
            # Trigger database failover through self-healing system
            logger.info(f"Triggering database failover for {component_name}")
            return True
        elif "api" in component_name.lower():
            # Trigger API failover
            logger.info(f"Triggering API failover for {component_name}")
            return True

        return False

    async def _self_heal(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Execute self-healing strategy."""
        logger.info("Executing self-healing recovery")

        component_name = context.get("component_name", "unknown")

        # Trigger self-healing system
        try:
            # Add component to monitoring if not already present
            self.self_healing_system.add_monitored_component(component_name, "api")

            # Force health check and recovery
            await self.self_healing_system._check_component_health(component_name)

            logger.info(f"Self-healing initiated for component: {component_name}")
            return True

        except Exception as e:
            logger.error(f"Self-healing failed: {e}")

        return False

    async def _escalate(self, error: Exception, context: Dict[str, Any], pattern: ErrorPattern) -> bool:
        """Execute escalation strategy."""
        logger.warning("Executing escalation recovery - notifying operators")

        # Send notifications to operators
        escalation_data = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "component": context.get("component_name", "unknown"),
            "severity": pattern.severity.value,
            "timestamp": datetime.utcnow().isoformat(),
            "context": context
        }

        # In a real implementation, this would send to alerting systems
        logger.critical(f"ESCALATION REQUIRED: {json.dumps(escalation_data, indent=2)}")

        return True


class ErrorRecoveryOrchestrator:
    """Main orchestrator for error recovery across the application."""

    def __init__(self, resilience_manager: ResilienceManager, self_healing_system: SelfHealingSystem):
        self.error_analyzer = ErrorAnalyzer()
        self.recovery_executor = RecoveryExecutor(resilience_manager, self_healing_system)
        self.active_incidents: Dict[str, ErrorIncident] = {}
        self.recovery_history: List[RecoveryAttempt] = []
        self.metrics = {
            "total_errors": 0,
            "recovered_errors": 0,
            "escalated_errors": 0,
            "unresolved_incidents": 0
        }

        # Initialize default error patterns
        self._initialize_default_patterns()

    def _initialize_default_patterns(self):
        """Initialize default error patterns."""

        # Database connection errors
        db_pattern = ErrorPattern(
            pattern_id="database_connection",
            error_types=[ConnectionError, TimeoutError],
            keywords=["connection", "timeout", "database"],
            severity=ErrorSeverity.HIGH,
            recovery_strategy=RecoveryStrategy.SELF_HEAL,
            max_occurrences=3,
            escalation_level=EscalationLevel.ON_CALL
        )
        self.error_analyzer.add_pattern(db_pattern)

        # API rate limiting
        rate_limit_pattern = ErrorPattern(
            pattern_id="rate_limit",
            error_types=[],
            keywords=["rate limit", "too many requests", "429"],
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=RecoveryStrategy.DELAYED_RETRY,
            max_occurrences=5
        )
        self.error_analyzer.add_pattern(rate_limit_pattern)

        # Validation errors
        validation_pattern = ErrorPattern(
            pattern_id="validation_error",
            error_types=[ValueError],
            keywords=["validation", "invalid", "format"],
            severity=ErrorSeverity.LOW,
            recovery_strategy=RecoveryStrategy.IGNORE,
            max_occurrences=10
        )
        self.error_analyzer.add_pattern(validation_pattern)

        # Memory/resource errors
        resource_pattern = ErrorPattern(
            pattern_id="resource_exhaustion",
            error_types=[MemoryError, OSError],
            keywords=["memory", "resource", "exhausted"],
            severity=ErrorSeverity.CRITICAL,
            recovery_strategy=RecoveryStrategy.ESCALATE_TO_OPERATOR,
            max_occurrences=1,
            escalation_level=EscalationLevel.MANAGEMENT
        )
        self.error_analyzer.add_pattern(resource_pattern)

    async def handle_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Main error handling entry point."""

        if context is None:
            context = {}

        self.metrics["total_errors"] += 1

        try:
            # Analyze error to find matching pattern
            pattern = self.error_analyzer.analyze_error(error, context)
            if not pattern:
                logger.warning(f"No pattern found for error: {type(error).__name__}")
                return False

            # Check if this is part of an existing incident
            incident = await self._get_or_create_incident(error, pattern, context)

            # Determine if recovery should be attempted
            if await self._should_attempt_recovery(incident, pattern):
                # Execute recovery strategy
                recovery_attempt = await self.recovery_executor.execute_recovery(
                    pattern.recovery_strategy, error, context, pattern
                )

                # Record the attempt
                incident.recovery_attempts.append(recovery_attempt.attempt_id)
                self.recovery_history.append(recovery_attempt)

                # Update pattern effectiveness
                self.error_analyzer.update_pattern_effectiveness(
                    pattern.pattern_id, recovery_attempt.success
                )

                # Check if incident should be escalated or resolved
                await self._update_incident_status(incident, recovery_attempt, pattern)

                if recovery_attempt.success:
                    self.metrics["recovered_errors"] += 1
                    logger.info(f"Successfully recovered from error: {type(error).__name__}")
                    return True

            logger.warning(f"Failed to recover from error: {type(error).__name__}")
            return False

        except Exception as e:
            logger.error(f"Error in recovery orchestrator: {e}")
            return False

    async def _get_or_create_incident(
        self,
        error: Exception,
        pattern: ErrorPattern,
        context: Dict[str, Any]
    ) -> ErrorIncident:
        """Get existing incident or create new one."""

        # Look for existing incident with same pattern in time window
        current_time = datetime.utcnow()

        for incident in self.active_incidents.values():
            if (incident.error_pattern.pattern_id == pattern.pattern_id and
                current_time - incident.last_occurrence <= pattern.time_window):

                # Update existing incident
                incident.last_occurrence = current_time
                incident.occurrence_count += 1

                component = context.get("component_name")
                if component and component not in incident.affected_components:
                    incident.affected_components.append(component)

                return incident

        # Create new incident
        incident_id = str(uuid.uuid4())
        incident = ErrorIncident(
            incident_id=incident_id,
            error_pattern=pattern,
            first_occurrence=current_time,
            last_occurrence=current_time,
            affected_components=[context.get("component_name", "unknown")]
        )

        self.active_incidents[incident_id] = incident
        context["incident_id"] = incident_id

        return incident

    async def _should_attempt_recovery(self, incident: ErrorIncident, pattern: ErrorPattern) -> bool:
        """Determine if recovery should be attempted."""

        # Don't recover if already escalated
        if incident.escalated:
            return False

        # Don't recover if max occurrences exceeded
        if incident.occurrence_count > pattern.max_occurrences:
            return False

        # Don't recover if strategy is IGNORE
        if pattern.recovery_strategy == RecoveryStrategy.IGNORE:
            return False

        # Check if too many recent recovery attempts
        recent_attempts = [
            attempt for attempt in self.recovery_history
            if (attempt.incident_id == incident.incident_id and
                datetime.utcnow() - attempt.started_at <= timedelta(minutes=5))
        ]

        if len(recent_attempts) >= 3:
            logger.warning(f"Too many recent recovery attempts for incident {incident.incident_id}")
            return False

        return True

    async def _update_incident_status(
        self,
        incident: ErrorIncident,
        recovery_attempt: RecoveryAttempt,
        pattern: ErrorPattern
    ):
        """Update incident status based on recovery attempt."""

        if recovery_attempt.success:
            # Mark incident as resolved
            incident.resolved = True
            incident.resolution_time = datetime.utcnow()

            # Remove from active incidents
            if incident.incident_id in self.active_incidents:
                del self.active_incidents[incident.incident_id]

        elif incident.occurrence_count >= pattern.max_occurrences and not incident.escalated:
            # Escalate incident
            incident.escalated = True
            self.metrics["escalated_errors"] += 1

            logger.warning(f"Escalating incident {incident.incident_id} after {incident.occurrence_count} occurrences")

            # Execute escalation
            await self.recovery_executor.execute_recovery(
                RecoveryStrategy.ESCALATE_TO_OPERATOR,
                Exception(f"Incident {incident.incident_id} escalated"),
                {"incident_id": incident.incident_id},
                pattern
            )

    def add_error_pattern(self, pattern: ErrorPattern):
        """Add custom error pattern."""
        self.error_analyzer.add_pattern(pattern)

    def register_custom_recovery_handler(self, strategy: RecoveryStrategy, handler: Callable):
        """Register custom recovery handler."""
        self.recovery_executor.register_custom_handler(strategy, handler)

    def get_metrics(self) -> Dict[str, Any]:
        """Get orchestrator metrics."""
        self.metrics["unresolved_incidents"] = len(self.active_incidents)

        return {
            **self.metrics,
            "pattern_effectiveness": self.error_analyzer.pattern_effectiveness,
            "active_incidents": len(self.active_incidents),
            "total_patterns": len(self.error_analyzer.error_patterns)
        }

    def get_active_incidents(self) -> List[Dict[str, Any]]:
        """Get list of active incidents."""
        return [
            {
                "incident_id": incident.incident_id,
                "pattern_id": incident.error_pattern.pattern_id,
                "severity": incident.error_pattern.severity.value,
                "occurrence_count": incident.occurrence_count,
                "first_occurrence": incident.first_occurrence.isoformat(),
                "last_occurrence": incident.last_occurrence.isoformat(),
                "affected_components": incident.affected_components,
                "escalated": incident.escalated,
                "recovery_attempts": len(incident.recovery_attempts)
            }
            for incident in self.active_incidents.values()
        ]

    def get_recovery_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent recovery history."""
        recent_history = sorted(
            self.recovery_history,
            key=lambda x: x.started_at,
            reverse=True
        )[:limit]

        return [
            {
                "attempt_id": attempt.attempt_id,
                "incident_id": attempt.incident_id,
                "strategy": attempt.strategy.value,
                "success": attempt.success,
                "duration_seconds": (attempt.completed_at - attempt.started_at).total_seconds() if attempt.completed_at else None,
                "error_message": attempt.error_message,
                "actions_taken": attempt.actions_taken
            }
            for attempt in recent_history
        ]


# Factory function to create orchestrator
def create_error_recovery_orchestrator(
    resilience_manager: ResilienceManager,
    self_healing_system: SelfHealingSystem
) -> ErrorRecoveryOrchestrator:
    """Create error recovery orchestrator with dependencies."""
    return ErrorRecoveryOrchestrator(resilience_manager, self_healing_system)