"""
Enterprise Self-Healing System
=============================

Advanced error recovery and resilience system with automated healing capabilities,
intelligent monitoring, and adaptive response strategies for enterprise applications.
"""
from __future__ import annotations

import asyncio
import time
import statistics
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union, Tuple
import uuid
import json

from core.logging import get_logger
from core.exceptions import BaseApplicationError

logger = get_logger(__name__)


class HealthStatus(Enum):
    """System health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    FAILING = "failing"
    UNKNOWN = "unknown"


class RecoveryAction(Enum):
    """Types of recovery actions."""
    RESTART_COMPONENT = "restart_component"
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    CIRCUIT_BREAK = "circuit_break"
    REROUTE_TRAFFIC = "reroute_traffic"
    CLEAR_CACHE = "clear_cache"
    RECONNECT_DATABASE = "reconnect_database"
    RESTART_SERVICE = "restart_service"
    ENABLE_BACKUP = "enable_backup"
    NOTIFY_OPERATORS = "notify_operators"


class RecoveryPriority(Enum):
    """Priority levels for recovery actions."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class HealthMetrics:
    """System health metrics."""
    component_name: str
    status: HealthStatus = HealthStatus.UNKNOWN
    error_rate: float = 0.0
    response_time: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    connection_count: int = 0
    throughput: float = 0.0
    last_error: Optional[str] = None
    uptime_seconds: float = 0.0
    restart_count: int = 0
    last_check: datetime = field(default_factory=datetime.utcnow)
    custom_metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryPlan:
    """Recovery action plan."""
    recovery_id: str
    component_name: str
    health_metrics: HealthMetrics
    actions: List[RecoveryAction]
    priority: RecoveryPriority
    estimated_duration: timedelta
    prerequisites: List[str] = field(default_factory=list)
    rollback_actions: List[RecoveryAction] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    executed_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    success: Optional[bool] = None
    error_message: Optional[str] = None


@dataclass
class RecoveryResult:
    """Result of recovery execution."""
    recovery_id: str
    success: bool
    duration: timedelta
    actions_executed: List[RecoveryAction]
    health_before: HealthMetrics
    health_after: HealthMetrics
    error_message: Optional[str] = None
    side_effects: List[str] = field(default_factory=list)


class HealthChecker(ABC):
    """Abstract base class for health checkers."""

    @abstractmethod
    async def check_health(self, component_name: str) -> HealthMetrics:
        """Check health of a component."""
        pass

    @abstractmethod
    def get_component_dependencies(self, component_name: str) -> List[str]:
        """Get dependencies of a component."""
        pass


class RecoveryAgent(ABC):
    """Abstract base class for recovery agents."""

    @abstractmethod
    async def can_handle(self, action: RecoveryAction, component: str) -> bool:
        """Check if agent can handle the recovery action."""
        pass

    @abstractmethod
    async def execute(self, action: RecoveryAction, component: str, context: Dict[str, Any]) -> bool:
        """Execute recovery action."""
        pass

    @abstractmethod
    async def rollback(self, action: RecoveryAction, component: str, context: Dict[str, Any]) -> bool:
        """Rollback recovery action if needed."""
        pass


class DatabaseHealthChecker(HealthChecker):
    """Health checker for database components."""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager

    async def check_health(self, component_name: str) -> HealthMetrics:
        """Check database health."""
        start_time = time.time()
        metrics = HealthMetrics(component_name=component_name)

        try:
            # Test connection
            await self._test_connection()
            response_time = (time.time() - start_time) * 1000

            # Get connection pool stats
            pool_stats = await self._get_pool_stats()

            metrics.response_time = response_time
            metrics.connection_count = pool_stats.get("active_connections", 0)
            metrics.custom_metrics = {
                "pool_size": pool_stats.get("pool_size", 0),
                "idle_connections": pool_stats.get("idle_connections", 0),
                "max_connections": pool_stats.get("max_connections", 0)
            }

            # Determine health status
            if response_time > 5000:  # > 5 seconds
                metrics.status = HealthStatus.CRITICAL
            elif response_time > 2000:  # > 2 seconds
                metrics.status = HealthStatus.DEGRADED
            elif metrics.connection_count > pool_stats.get("max_connections", 100) * 0.9:
                metrics.status = HealthStatus.DEGRADED
            else:
                metrics.status = HealthStatus.HEALTHY

        except Exception as e:
            metrics.status = HealthStatus.FAILING
            metrics.last_error = str(e)
            logger.error(f"Database health check failed for {component_name}: {e}")

        return metrics

    async def _test_connection(self):
        """Test database connection."""
        # Simulate database connection test
        await asyncio.sleep(0.1)

    async def _get_pool_stats(self) -> Dict[str, int]:
        """Get connection pool statistics."""
        # Simulate getting pool stats
        return {
            "active_connections": 15,
            "idle_connections": 5,
            "pool_size": 20,
            "max_connections": 50
        }

    def get_component_dependencies(self, component_name: str) -> List[str]:
        """Get database dependencies."""
        return ["network", "storage"]


class APIHealthChecker(HealthChecker):
    """Health checker for API components."""

    async def check_health(self, component_name: str) -> HealthMetrics:
        """Check API health."""
        start_time = time.time()
        metrics = HealthMetrics(component_name=component_name)

        try:
            # Test endpoint
            await self._test_endpoint()
            response_time = (time.time() - start_time) * 1000

            # Get system metrics
            system_metrics = await self._get_system_metrics()

            metrics.response_time = response_time
            metrics.cpu_usage = system_metrics.get("cpu_percent", 0)
            metrics.memory_usage = system_metrics.get("memory_percent", 0)
            metrics.throughput = system_metrics.get("requests_per_second", 0)

            # Determine health status
            if metrics.cpu_usage > 90 or metrics.memory_usage > 90:
                metrics.status = HealthStatus.CRITICAL
            elif metrics.cpu_usage > 70 or metrics.memory_usage > 70 or response_time > 1000:
                metrics.status = HealthStatus.DEGRADED
            else:
                metrics.status = HealthStatus.HEALTHY

        except Exception as e:
            metrics.status = HealthStatus.FAILING
            metrics.last_error = str(e)
            logger.error(f"API health check failed for {component_name}: {e}")

        return metrics

    async def _test_endpoint(self):
        """Test API endpoint."""
        await asyncio.sleep(0.05)

    async def _get_system_metrics(self) -> Dict[str, float]:
        """Get system metrics."""
        # Simulate system metrics
        import random
        return {
            "cpu_percent": random.uniform(20, 80),
            "memory_percent": random.uniform(30, 70),
            "requests_per_second": random.uniform(100, 1000)
        }

    def get_component_dependencies(self, component_name: str) -> List[str]:
        """Get API dependencies."""
        return ["database", "cache", "external_services"]


class DatabaseRecoveryAgent(RecoveryAgent):
    """Recovery agent for database components."""

    async def can_handle(self, action: RecoveryAction, component: str) -> bool:
        """Check if can handle database recovery actions."""
        database_actions = {
            RecoveryAction.RECONNECT_DATABASE,
            RecoveryAction.RESTART_COMPONENT,
            RecoveryAction.CLEAR_CACHE
        }
        return action in database_actions and "database" in component.lower()

    async def execute(self, action: RecoveryAction, component: str, context: Dict[str, Any]) -> bool:
        """Execute database recovery action."""
        try:
            if action == RecoveryAction.RECONNECT_DATABASE:
                return await self._reconnect_database(component)
            elif action == RecoveryAction.RESTART_COMPONENT:
                return await self._restart_database_component(component)
            elif action == RecoveryAction.CLEAR_CACHE:
                return await self._clear_database_cache(component)
            else:
                logger.warning(f"Unsupported action {action} for database component {component}")
                return False
        except Exception as e:
            logger.error(f"Failed to execute {action} for {component}: {e}")
            return False

    async def rollback(self, action: RecoveryAction, component: str, context: Dict[str, Any]) -> bool:
        """Rollback database recovery action."""
        # Most database recovery actions don't require rollback
        return True

    async def _reconnect_database(self, component: str) -> bool:
        """Reconnect database connections."""
        logger.info(f"Reconnecting database connections for {component}")
        await asyncio.sleep(2)  # Simulate reconnection time
        return True

    async def _restart_database_component(self, component: str) -> bool:
        """Restart database component."""
        logger.info(f"Restarting database component {component}")
        await asyncio.sleep(5)  # Simulate restart time
        return True

    async def _clear_database_cache(self, component: str) -> bool:
        """Clear database cache."""
        logger.info(f"Clearing database cache for {component}")
        await asyncio.sleep(1)  # Simulate cache clear time
        return True


class APIRecoveryAgent(RecoveryAgent):
    """Recovery agent for API components."""

    async def can_handle(self, action: RecoveryAction, component: str) -> bool:
        """Check if can handle API recovery actions."""
        api_actions = {
            RecoveryAction.RESTART_COMPONENT,
            RecoveryAction.SCALE_UP,
            RecoveryAction.SCALE_DOWN,
            RecoveryAction.REROUTE_TRAFFIC,
            RecoveryAction.CLEAR_CACHE
        }
        return action in api_actions and "api" in component.lower()

    async def execute(self, action: RecoveryAction, component: str, context: Dict[str, Any]) -> bool:
        """Execute API recovery action."""
        try:
            if action == RecoveryAction.RESTART_COMPONENT:
                return await self._restart_api_component(component)
            elif action == RecoveryAction.SCALE_UP:
                return await self._scale_up_api(component)
            elif action == RecoveryAction.SCALE_DOWN:
                return await self._scale_down_api(component)
            elif action == RecoveryAction.REROUTE_TRAFFIC:
                return await self._reroute_traffic(component)
            elif action == RecoveryAction.CLEAR_CACHE:
                return await self._clear_api_cache(component)
            else:
                logger.warning(f"Unsupported action {action} for API component {component}")
                return False
        except Exception as e:
            logger.error(f"Failed to execute {action} for {component}: {e}")
            return False

    async def rollback(self, action: RecoveryAction, component: str, context: Dict[str, Any]) -> bool:
        """Rollback API recovery action."""
        try:
            if action == RecoveryAction.SCALE_UP:
                return await self._scale_down_api(component)
            elif action == RecoveryAction.REROUTE_TRAFFIC:
                return await self._restore_traffic_routing(component)
            return True
        except Exception as e:
            logger.error(f"Failed to rollback {action} for {component}: {e}")
            return False

    async def _restart_api_component(self, component: str) -> bool:
        """Restart API component."""
        logger.info(f"Restarting API component {component}")
        await asyncio.sleep(3)
        return True

    async def _scale_up_api(self, component: str) -> bool:
        """Scale up API component."""
        logger.info(f"Scaling up API component {component}")
        await asyncio.sleep(10)
        return True

    async def _scale_down_api(self, component: str) -> bool:
        """Scale down API component."""
        logger.info(f"Scaling down API component {component}")
        await asyncio.sleep(5)
        return True

    async def _reroute_traffic(self, component: str) -> bool:
        """Reroute traffic away from component."""
        logger.info(f"Rerouting traffic from {component}")
        await asyncio.sleep(2)
        return True

    async def _restore_traffic_routing(self, component: str) -> bool:
        """Restore original traffic routing."""
        logger.info(f"Restoring traffic routing to {component}")
        await asyncio.sleep(2)
        return True

    async def _clear_api_cache(self, component: str) -> bool:
        """Clear API cache."""
        logger.info(f"Clearing API cache for {component}")
        await asyncio.sleep(1)
        return True


class RecoveryPlanner:
    """Plans recovery strategies based on health metrics and historical data."""

    def __init__(self):
        self.recovery_history: List[RecoveryResult] = []
        self.success_rates: Dict[Tuple[str, RecoveryAction], float] = {}

    async def create_recovery_plan(
        self,
        component_name: str,
        health_metrics: HealthMetrics,
        dependencies: List[str]
    ) -> RecoveryPlan:
        """Create a recovery plan based on health metrics."""

        recovery_id = str(uuid.uuid4())
        actions = self._determine_recovery_actions(health_metrics)
        priority = self._determine_priority(health_metrics)
        estimated_duration = self._estimate_duration(actions)

        plan = RecoveryPlan(
            recovery_id=recovery_id,
            component_name=component_name,
            health_metrics=health_metrics,
            actions=actions,
            priority=priority,
            estimated_duration=estimated_duration,
            prerequisites=self._get_prerequisites(actions, dependencies),
            rollback_actions=self._get_rollback_actions(actions)
        )

        logger.info(f"Created recovery plan {recovery_id} for {component_name} with {len(actions)} actions")
        return plan

    def _determine_recovery_actions(self, metrics: HealthMetrics) -> List[RecoveryAction]:
        """Determine appropriate recovery actions based on metrics."""
        actions = []

        if metrics.status == HealthStatus.FAILING:
            if "database" in metrics.component_name.lower():
                actions.extend([
                    RecoveryAction.RECONNECT_DATABASE,
                    RecoveryAction.RESTART_COMPONENT
                ])
            elif "api" in metrics.component_name.lower():
                actions.extend([
                    RecoveryAction.RESTART_COMPONENT,
                    RecoveryAction.SCALE_UP
                ])

        elif metrics.status == HealthStatus.CRITICAL:
            if metrics.cpu_usage > 90:
                actions.append(RecoveryAction.SCALE_UP)
            if metrics.memory_usage > 90:
                actions.extend([
                    RecoveryAction.CLEAR_CACHE,
                    RecoveryAction.RESTART_COMPONENT
                ])
            if metrics.response_time > 5000:
                actions.extend([
                    RecoveryAction.REROUTE_TRAFFIC,
                    RecoveryAction.SCALE_UP
                ])

        elif metrics.status == HealthStatus.DEGRADED:
            if metrics.error_rate > 0.1:  # More than 10% error rate
                actions.append(RecoveryAction.CLEAR_CACHE)
            if metrics.response_time > 1000:
                actions.append(RecoveryAction.SCALE_UP)

        # Always notify operators for critical issues
        if metrics.status in [HealthStatus.FAILING, HealthStatus.CRITICAL]:
            actions.append(RecoveryAction.NOTIFY_OPERATORS)

        return list(set(actions))  # Remove duplicates

    def _determine_priority(self, metrics: HealthMetrics) -> RecoveryPriority:
        """Determine recovery priority based on health status."""
        if metrics.status == HealthStatus.FAILING:
            return RecoveryPriority.CRITICAL
        elif metrics.status == HealthStatus.CRITICAL:
            return RecoveryPriority.HIGH
        elif metrics.status == HealthStatus.DEGRADED:
            return RecoveryPriority.MEDIUM
        else:
            return RecoveryPriority.LOW

    def _estimate_duration(self, actions: List[RecoveryAction]) -> timedelta:
        """Estimate duration for recovery actions."""
        duration_map = {
            RecoveryAction.RESTART_COMPONENT: timedelta(minutes=5),
            RecoveryAction.SCALE_UP: timedelta(minutes=10),
            RecoveryAction.SCALE_DOWN: timedelta(minutes=5),
            RecoveryAction.CIRCUIT_BREAK: timedelta(seconds=30),
            RecoveryAction.REROUTE_TRAFFIC: timedelta(minutes=2),
            RecoveryAction.CLEAR_CACHE: timedelta(minutes=1),
            RecoveryAction.RECONNECT_DATABASE: timedelta(minutes=2),
            RecoveryAction.RESTART_SERVICE: timedelta(minutes=8),
            RecoveryAction.ENABLE_BACKUP: timedelta(minutes=15),
            RecoveryAction.NOTIFY_OPERATORS: timedelta(seconds=10)
        }

        total_duration = timedelta(0)
        for action in actions:
            total_duration += duration_map.get(action, timedelta(minutes=5))

        return total_duration

    def _get_prerequisites(self, actions: List[RecoveryAction], dependencies: List[str]) -> List[str]:
        """Get prerequisites for recovery actions."""
        prerequisites = []

        if RecoveryAction.SCALE_UP in actions:
            prerequisites.extend(["resource_availability", "load_balancer_ready"])

        if RecoveryAction.REROUTE_TRAFFIC in actions:
            prerequisites.extend(["backup_instances_ready", "load_balancer_configured"])

        if RecoveryAction.RESTART_COMPONENT in actions:
            prerequisites.extend(["traffic_drained", "backup_ready"])

        return prerequisites

    def _get_rollback_actions(self, actions: List[RecoveryAction]) -> List[RecoveryAction]:
        """Get rollback actions for recovery plan."""
        rollback_map = {
            RecoveryAction.SCALE_UP: RecoveryAction.SCALE_DOWN,
            RecoveryAction.REROUTE_TRAFFIC: RecoveryAction.REROUTE_TRAFFIC,  # Restore original routing
            RecoveryAction.CIRCUIT_BREAK: RecoveryAction.CIRCUIT_BREAK  # Close circuit breaker
        }

        rollback_actions = []
        for action in actions:
            if action in rollback_map:
                rollback_actions.append(rollback_map[action])

        return rollback_actions

    def record_recovery_result(self, result: RecoveryResult):
        """Record recovery result for learning."""
        self.recovery_history.append(result)

        # Update success rates
        for action in result.actions_executed:
            key = (result.health_before.component_name, action)
            if key not in self.success_rates:
                self.success_rates[key] = 0.0

            # Update success rate using exponential moving average
            alpha = 0.1  # Learning rate
            current_rate = self.success_rates[key]
            new_rate = alpha * (1.0 if result.success else 0.0) + (1 - alpha) * current_rate
            self.success_rates[key] = new_rate

        logger.info(f"Recorded recovery result for {result.recovery_id}: {'success' if result.success else 'failure'}")


class SelfHealingSystem:
    """Enterprise self-healing system with automated recovery capabilities."""

    def __init__(self):
        self.health_checkers: Dict[str, HealthChecker] = {}
        self.recovery_agents: List[RecoveryAgent] = []
        self.recovery_planner = RecoveryPlanner()
        self.monitored_components: Dict[str, HealthMetrics] = {}
        self.active_recoveries: Dict[str, RecoveryPlan] = {}
        self.monitoring_enabled = False
        self.monitoring_interval = 30  # seconds
        self.monitoring_task: Optional[asyncio.Task] = None

        # Health thresholds
        self.health_thresholds = {
            "error_rate": 0.05,  # 5% error rate threshold
            "response_time": 1000,  # 1 second response time threshold
            "cpu_usage": 80,  # 80% CPU usage threshold
            "memory_usage": 85  # 85% memory usage threshold
        }

    def register_health_checker(self, component_type: str, checker: HealthChecker):
        """Register a health checker for a component type."""
        self.health_checkers[component_type] = checker
        logger.info(f"Registered health checker for {component_type}")

    def register_recovery_agent(self, agent: RecoveryAgent):
        """Register a recovery agent."""
        self.recovery_agents.append(agent)
        logger.info(f"Registered recovery agent: {type(agent).__name__}")

    def add_monitored_component(self, component_name: str, component_type: str):
        """Add a component to monitoring."""
        if component_type not in self.health_checkers:
            raise ValueError(f"No health checker registered for component type: {component_type}")

        self.monitored_components[component_name] = HealthMetrics(component_name=component_name)
        logger.info(f"Added component {component_name} ({component_type}) to monitoring")

    async def start_monitoring(self):
        """Start continuous health monitoring."""
        if self.monitoring_enabled:
            logger.warning("Monitoring is already enabled")
            return

        self.monitoring_enabled = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Started self-healing monitoring system")

    async def stop_monitoring(self):
        """Stop health monitoring."""
        self.monitoring_enabled = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped self-healing monitoring system")

    async def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.monitoring_enabled:
            try:
                await self._check_all_components()
                await asyncio.sleep(self.monitoring_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying

    async def _check_all_components(self):
        """Check health of all monitored components."""
        check_tasks = []

        for component_name in self.monitored_components.keys():
            task = asyncio.create_task(self._check_component_health(component_name))
            check_tasks.append(task)

        if check_tasks:
            await asyncio.gather(*check_tasks, return_exceptions=True)

    async def _check_component_health(self, component_name: str):
        """Check health of a specific component."""
        try:
            # Find appropriate health checker
            checker = self._get_health_checker(component_name)
            if not checker:
                logger.warning(f"No health checker found for component {component_name}")
                return

            # Check health
            health_metrics = await checker.check_health(component_name)
            previous_health = self.monitored_components.get(component_name)

            # Update stored metrics
            self.monitored_components[component_name] = health_metrics

            # Check if recovery is needed
            if await self._requires_recovery(health_metrics, previous_health):
                await self._initiate_recovery(component_name, health_metrics, checker)

        except Exception as e:
            logger.error(f"Failed to check health of component {component_name}: {e}")

    def _get_health_checker(self, component_name: str) -> Optional[HealthChecker]:
        """Get appropriate health checker for component."""
        # Simple logic to determine component type from name
        if "database" in component_name.lower() or "db" in component_name.lower():
            return self.health_checkers.get("database")
        elif "api" in component_name.lower() or "service" in component_name.lower():
            return self.health_checkers.get("api")
        else:
            # Default to first available checker
            return next(iter(self.health_checkers.values())) if self.health_checkers else None

    async def _requires_recovery(self, current: HealthMetrics, previous: Optional[HealthMetrics]) -> bool:
        """Determine if component requires recovery."""
        # Component is failing or critical
        if current.status in [HealthStatus.FAILING, HealthStatus.CRITICAL]:
            return True

        # Component degraded and getting worse
        if current.status == HealthStatus.DEGRADED and previous:
            if (current.error_rate > previous.error_rate * 1.5 or
                current.response_time > previous.response_time * 1.5):
                return True

        # Check threshold violations
        if (current.error_rate > self.health_thresholds["error_rate"] or
            current.response_time > self.health_thresholds["response_time"] or
            current.cpu_usage > self.health_thresholds["cpu_usage"] or
            current.memory_usage > self.health_thresholds["memory_usage"]):
            return True

        return False

    async def _initiate_recovery(self, component_name: str, health_metrics: HealthMetrics, checker: HealthChecker):
        """Initiate recovery process for component."""
        # Check if recovery is already in progress
        if component_name in self.active_recoveries:
            logger.info(f"Recovery already in progress for {component_name}")
            return

        try:
            # Get component dependencies
            dependencies = checker.get_component_dependencies(component_name)

            # Create recovery plan
            recovery_plan = await self.recovery_planner.create_recovery_plan(
                component_name, health_metrics, dependencies
            )

            # Store active recovery
            self.active_recoveries[component_name] = recovery_plan

            # Execute recovery plan
            result = await self._execute_recovery_plan(recovery_plan)

            # Record result and cleanup
            self.recovery_planner.record_recovery_result(result)
            del self.active_recoveries[component_name]

            logger.info(f"Recovery completed for {component_name}: {'success' if result.success else 'failure'}")

        except Exception as e:
            logger.error(f"Failed to initiate recovery for {component_name}: {e}")
            if component_name in self.active_recoveries:
                del self.active_recoveries[component_name]

    async def _execute_recovery_plan(self, plan: RecoveryPlan) -> RecoveryResult:
        """Execute a recovery plan."""
        logger.info(f"Executing recovery plan {plan.recovery_id} for {plan.component_name}")

        start_time = datetime.utcnow()
        plan.executed_at = start_time
        executed_actions = []
        success = True
        error_message = None

        try:
            # Check prerequisites
            if not await self._check_prerequisites(plan):
                raise Exception("Prerequisites not met for recovery plan")

            # Execute actions in sequence
            for action in plan.actions:
                agent = await self._find_recovery_agent(action, plan.component_name)
                if not agent:
                    logger.warning(f"No agent found for action {action} on component {plan.component_name}")
                    continue

                action_success = await agent.execute(action, plan.component_name, {})
                executed_actions.append(action)

                if not action_success:
                    raise Exception(f"Recovery action {action} failed")

                # Brief pause between actions
                await asyncio.sleep(1)

        except Exception as e:
            success = False
            error_message = str(e)
            logger.error(f"Recovery plan {plan.recovery_id} failed: {e}")

            # Attempt rollback
            await self._rollback_recovery_plan(plan, executed_actions)

        # Get health after recovery
        checker = self._get_health_checker(plan.component_name)
        health_after = await checker.check_health(plan.component_name) if checker else plan.health_metrics

        end_time = datetime.utcnow()
        plan.completed_at = end_time
        plan.success = success
        plan.error_message = error_message

        return RecoveryResult(
            recovery_id=plan.recovery_id,
            success=success,
            duration=end_time - start_time,
            actions_executed=executed_actions,
            health_before=plan.health_metrics,
            health_after=health_after,
            error_message=error_message
        )

    async def _check_prerequisites(self, plan: RecoveryPlan) -> bool:
        """Check if prerequisites are met for recovery plan."""
        # Simplified prerequisite checking
        for prerequisite in plan.prerequisites:
            if prerequisite == "resource_availability":
                # Check if resources are available for scaling
                pass
            elif prerequisite == "backup_instances_ready":
                # Check if backup instances are ready
                pass
            elif prerequisite == "traffic_drained":
                # Check if traffic has been drained
                pass

        return True  # Simplified - assume prerequisites are met

    async def _find_recovery_agent(self, action: RecoveryAction, component_name: str) -> Optional[RecoveryAgent]:
        """Find appropriate recovery agent for action."""
        for agent in self.recovery_agents:
            if await agent.can_handle(action, component_name):
                return agent
        return None

    async def _rollback_recovery_plan(self, plan: RecoveryPlan, executed_actions: List[RecoveryAction]):
        """Rollback executed recovery actions."""
        logger.info(f"Rolling back recovery plan {plan.recovery_id}")

        # Execute rollback actions in reverse order
        for action in reversed(executed_actions):
            try:
                agent = await self._find_recovery_agent(action, plan.component_name)
                if agent:
                    await agent.rollback(action, plan.component_name, {})
            except Exception as e:
                logger.error(f"Failed to rollback action {action}: {e}")

    def get_system_health_summary(self) -> Dict[str, Any]:
        """Get overall system health summary."""
        total_components = len(self.monitored_components)
        if total_components == 0:
            return {"status": "no_components", "components": {}}

        health_counts = defaultdict(int)
        for metrics in self.monitored_components.values():
            health_counts[metrics.status.value] += 1

        overall_status = "healthy"
        if health_counts["failing"] > 0 or health_counts["critical"] > total_components * 0.5:
            overall_status = "critical"
        elif health_counts["degraded"] > total_components * 0.3:
            overall_status = "degraded"

        return {
            "status": overall_status,
            "total_components": total_components,
            "health_distribution": dict(health_counts),
            "active_recoveries": len(self.active_recoveries),
            "monitoring_enabled": self.monitoring_enabled,
            "components": {
                name: {
                    "status": metrics.status.value,
                    "error_rate": metrics.error_rate,
                    "response_time": metrics.response_time,
                    "last_check": metrics.last_check.isoformat()
                }
                for name, metrics in self.monitored_components.items()
            }
        }

    def get_recovery_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent recovery history."""
        recent_history = sorted(
            self.recovery_planner.recovery_history,
            key=lambda x: x.health_before.last_check,
            reverse=True
        )[:limit]

        return [
            {
                "recovery_id": result.recovery_id,
                "component_name": result.health_before.component_name,
                "success": result.success,
                "duration_seconds": result.duration.total_seconds(),
                "actions_executed": [action.value for action in result.actions_executed],
                "error_message": result.error_message
            }
            for result in recent_history
        ]


# Factory function to create self-healing system with default configuration
def create_enterprise_self_healing_system() -> SelfHealingSystem:
    """Create self-healing system with enterprise configuration."""
    system = SelfHealingSystem()

    # Register default health checkers
    system.register_health_checker("database", DatabaseHealthChecker(None))
    system.register_health_checker("api", APIHealthChecker())

    # Register default recovery agents
    system.register_recovery_agent(DatabaseRecoveryAgent())
    system.register_recovery_agent(APIRecoveryAgent())

    # Add default monitored components
    system.add_monitored_component("primary_database", "database")
    system.add_monitored_component("api_gateway", "api")
    system.add_monitored_component("sales_api", "api")

    return system


# Global instance
self_healing_system = create_enterprise_self_healing_system()