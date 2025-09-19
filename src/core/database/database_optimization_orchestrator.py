"""
Database Optimization Orchestrator
==================================

Enterprise orchestrator that coordinates intelligent connection pooling,
performance monitoring, and automated optimization across the database layer.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union
import uuid

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy import event

from .intelligent_connection_pool import (
    IntelligentConnectionPool,
    WorkloadPattern,
    OptimizationStrategy,
    ConnectionPriority,
    create_intelligent_connection_pool
)
from .advanced_performance_monitor import (
    AdvancedPerformanceMonitor,
    PerformanceAlert,
    DatabaseHealthScore,
    QueryMetrics,
    PerformanceAlertLevel,
    create_performance_monitor,
    setup_performance_monitoring
)
from core.logging import get_logger
from core.resilience import get_resilience_system

logger = get_logger(__name__)


class OptimizationMode(Enum):
    """Database optimization modes."""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"
    COST_OPTIMIZED = "cost_optimized"
    PERFORMANCE_FIRST = "performance_first"


class AutomationLevel(Enum):
    """Levels of automation for optimization."""
    MANUAL = "manual"
    ASSISTED = "assisted"
    SEMI_AUTOMATIC = "semi_automatic"
    FULLY_AUTOMATIC = "fully_automatic"


@dataclass
class OptimizationPlan:
    """Database optimization execution plan."""
    plan_id: str
    optimization_mode: OptimizationMode
    automation_level: AutomationLevel
    estimated_duration: timedelta
    estimated_impact: Dict[str, float]
    actions: List[Dict[str, Any]] = field(default_factory=list)
    prerequisites: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    rollback_plan: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    approved: bool = False
    executed: bool = False


@dataclass
class OptimizationResult:
    """Result of optimization execution."""
    plan_id: str
    success: bool
    execution_time: timedelta
    performance_improvement: Dict[str, float]
    cost_impact: Dict[str, float]
    issues_encountered: List[str] = field(default_factory=list)
    rollback_performed: bool = False
    final_state: Dict[str, Any] = field(default_factory=dict)


class DatabaseOptimizationOrchestrator:
    """
    Enterprise database optimization orchestrator that coordinates
    connection pooling, performance monitoring, and automated optimization.
    """

    def __init__(
        self,
        database_url: str,
        optimization_mode: OptimizationMode = OptimizationMode.BALANCED,
        automation_level: AutomationLevel = AutomationLevel.SEMI_AUTOMATIC,
        alert_callback: Optional[Callable] = None
    ):
        self.database_url = database_url
        self.optimization_mode = optimization_mode
        self.automation_level = automation_level
        self.alert_callback = alert_callback

        # Core components
        self.connection_pool: Optional[IntelligentConnectionPool] = None
        self.performance_monitor: Optional[AdvancedPerformanceMonitor] = None
        self.resilience_system = get_resilience_system()

        # Optimization state
        self.optimization_plans: List[OptimizationPlan] = []
        self.optimization_history: List[OptimizationResult] = []
        self.current_optimization: Optional[OptimizationPlan] = None

        # Configuration
        self.optimization_interval = 900  # 15 minutes
        self.health_check_interval = 300  # 5 minutes
        self.auto_approval_threshold = 0.8  # Confidence threshold for auto-approval

        # Tasks
        self.optimization_task: Optional[asyncio.Task] = None
        self.health_monitoring_task: Optional[asyncio.Task] = None
        self.metrics_collection_task: Optional[asyncio.Task] = None

        # Metrics
        self.orchestrator_metrics = {
            "total_optimizations": 0,
            "successful_optimizations": 0,
            "failed_optimizations": 0,
            "auto_optimizations": 0,
            "manual_optimizations": 0,
            "rollbacks_performed": 0,
            "avg_performance_improvement": 0.0
        }

    async def initialize(self):
        """Initialize the database optimization orchestrator."""
        try:
            logger.info("Initializing database optimization orchestrator")

            # Initialize performance monitor
            self.performance_monitor = create_performance_monitor(
                alert_callback=self._handle_performance_alert
            )

            # Initialize intelligent connection pool
            strategy = self._get_pool_strategy_from_mode()
            self.connection_pool = create_intelligent_connection_pool(
                database_url=self.database_url,
                optimization_strategy=strategy,
                auto_optimize=True
            )

            await self.connection_pool.initialize()

            # Set up performance monitoring on the engine
            setup_performance_monitoring(
                self.connection_pool.engine.sync_engine,
                self.performance_monitor
            )

            # Start background tasks
            await self._start_background_tasks()

            logger.info("Database optimization orchestrator initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database optimization orchestrator: {e}")
            raise

    def _get_pool_strategy_from_mode(self) -> OptimizationStrategy:
        """Convert optimization mode to pool strategy."""
        mapping = {
            OptimizationMode.CONSERVATIVE: OptimizationStrategy.CONSERVATIVE,
            OptimizationMode.BALANCED: OptimizationStrategy.BALANCED,
            OptimizationMode.AGGRESSIVE: OptimizationStrategy.AGGRESSIVE,
            OptimizationMode.COST_OPTIMIZED: OptimizationStrategy.COST_OPTIMIZED,
            OptimizationMode.PERFORMANCE_FIRST: OptimizationStrategy.PREDICTIVE
        }
        return mapping.get(self.optimization_mode, OptimizationStrategy.BALANCED)

    async def _start_background_tasks(self):
        """Start background monitoring and optimization tasks."""
        # Start optimization task if automation is enabled
        if self.automation_level in [AutomationLevel.SEMI_AUTOMATIC, AutomationLevel.FULLY_AUTOMATIC]:
            self.optimization_task = asyncio.create_task(self._optimization_loop())

        # Start health monitoring
        self.health_monitoring_task = asyncio.create_task(self._health_monitoring_loop())

        # Start metrics collection
        self.metrics_collection_task = asyncio.create_task(self._metrics_collection_loop())

    async def _optimization_loop(self):
        """Background optimization loop."""
        while True:
            try:
                await asyncio.sleep(self.optimization_interval)

                # Check if optimization is needed
                if await self._should_perform_optimization():
                    await self._perform_automated_optimization()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in optimization loop: {e}")

    async def _health_monitoring_loop(self):
        """Background health monitoring loop."""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)

                # Calculate health score
                if self.performance_monitor:
                    health_score = self.performance_monitor.calculate_health_score()

                    # Check if intervention is needed
                    if health_score.overall_score < 60:
                        await self._handle_poor_health(health_score)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")

    async def _metrics_collection_loop(self):
        """Background metrics collection loop."""
        while True:
            try:
                await asyncio.sleep(60)  # Collect metrics every minute

                # Update orchestrator metrics
                await self._update_orchestrator_metrics()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")

    async def get_database_session(
        self,
        priority: ConnectionPriority = ConnectionPriority.NORMAL,
        timeout: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> AsyncSession:
        """
        Get database session through the intelligent connection pool.

        Args:
            priority: Connection priority level
            timeout: Connection timeout
            context: Additional context for the request

        Returns:
            AsyncSession: Database session
        """
        if not self.connection_pool:
            raise RuntimeError("Connection pool not initialized")

        try:
            # Add resilience wrapper for database operations
            @self.resilience_system.create_resilient_decorator(
                "database_session",
                auto_recovery=True
            ).execute
            async def get_session_with_resilience():
                return await self.connection_pool.get_session(
                    priority=priority,
                    timeout=timeout,
                    context=context
                )

            return await get_session_with_resilience()

        except Exception as e:
            logger.error(f"Failed to get database session: {e}")
            raise

    async def create_optimization_plan(
        self,
        target_improvements: Optional[Dict[str, float]] = None,
        constraints: Optional[Dict[str, Any]] = None
    ) -> OptimizationPlan:
        """
        Create a comprehensive optimization plan.

        Args:
            target_improvements: Target improvements (e.g., {"latency": 0.3, "throughput": 0.2})
            constraints: Optimization constraints (e.g., {"max_cost_increase": 0.1})

        Returns:
            OptimizationPlan: Generated optimization plan
        """
        plan_id = str(uuid.uuid4())

        logger.info(f"Creating optimization plan {plan_id}")

        try:
            # Analyze current state
            current_state = await self._analyze_current_state()

            # Generate optimization actions
            actions = await self._generate_optimization_actions(
                current_state, target_improvements, constraints
            )

            # Estimate impact and duration
            impact_estimate = await self._estimate_optimization_impact(actions)
            duration_estimate = self._estimate_optimization_duration(actions)

            # Identify risks and prerequisites
            risks = self._identify_optimization_risks(actions)
            prerequisites = self._identify_prerequisites(actions)

            # Create rollback plan
            rollback_plan = self._create_rollback_plan(actions)

            plan = OptimizationPlan(
                plan_id=plan_id,
                optimization_mode=self.optimization_mode,
                automation_level=self.automation_level,
                estimated_duration=duration_estimate,
                estimated_impact=impact_estimate,
                actions=actions,
                prerequisites=prerequisites,
                risks=risks,
                rollback_plan=rollback_plan
            )

            self.optimization_plans.append(plan)

            logger.info(f"Created optimization plan {plan_id} with {len(actions)} actions")
            return plan

        except Exception as e:
            logger.error(f"Failed to create optimization plan: {e}")
            raise

    async def execute_optimization_plan(
        self,
        plan_id: str,
        force_approval: bool = False
    ) -> OptimizationResult:
        """
        Execute an optimization plan.

        Args:
            plan_id: ID of the plan to execute
            force_approval: Force execution without approval checks

        Returns:
            OptimizationResult: Execution result
        """
        plan = next((p for p in self.optimization_plans if p.plan_id == plan_id), None)
        if not plan:
            raise ValueError(f"Optimization plan {plan_id} not found")

        if not plan.approved and not force_approval:
            if self.automation_level != AutomationLevel.FULLY_AUTOMATIC:
                raise ValueError("Plan requires approval before execution")

        logger.info(f"Executing optimization plan {plan_id}")

        start_time = datetime.utcnow()
        issues_encountered = []
        rollback_performed = False

        try:
            # Mark as current optimization
            self.current_optimization = plan

            # Record baseline metrics
            baseline_metrics = await self._capture_baseline_metrics()

            # Execute optimization actions
            for i, action in enumerate(plan.actions):
                try:
                    await self._execute_optimization_action(action)
                    logger.info(f"Completed action {i+1}/{len(plan.actions)}: {action['type']}")

                except Exception as e:
                    error_msg = f"Failed to execute action {action['type']}: {str(e)}"
                    issues_encountered.append(error_msg)
                    logger.error(error_msg)

                    # Check if we should abort and rollback
                    if action.get("critical", False):
                        logger.warning("Critical action failed, initiating rollback")
                        await self._execute_rollback_plan(plan.rollback_plan)
                        rollback_performed = True
                        break

            # Capture post-optimization metrics
            post_metrics = await self._capture_baseline_metrics()

            # Calculate performance improvement
            performance_improvement = self._calculate_performance_improvement(
                baseline_metrics, post_metrics
            )

            # Calculate cost impact
            cost_impact = self._calculate_cost_impact(baseline_metrics, post_metrics)

            execution_time = datetime.utcnow() - start_time
            success = len(issues_encountered) == 0 and not rollback_performed

            result = OptimizationResult(
                plan_id=plan_id,
                success=success,
                execution_time=execution_time,
                performance_improvement=performance_improvement,
                cost_impact=cost_impact,
                issues_encountered=issues_encountered,
                rollback_performed=rollback_performed,
                final_state=post_metrics
            )

            # Update metrics
            self.orchestrator_metrics["total_optimizations"] += 1
            if success:
                self.orchestrator_metrics["successful_optimizations"] += 1
            else:
                self.orchestrator_metrics["failed_optimizations"] += 1

            if rollback_performed:
                self.orchestrator_metrics["rollbacks_performed"] += 1

            # Mark plan as executed
            plan.executed = True

            # Store result
            self.optimization_history.append(result)
            self.current_optimization = None

            logger.info(f"Optimization plan {plan_id} completed with success={success}")
            return result

        except Exception as e:
            logger.error(f"Failed to execute optimization plan {plan_id}: {e}")
            self.current_optimization = None
            raise

    async def _should_perform_optimization(self) -> bool:
        """Determine if automated optimization should be performed."""
        if not self.performance_monitor:
            return False

        # Check health score
        health_score = self.performance_monitor.calculate_health_score()

        # Perform optimization if health is poor or declining
        if health_score.overall_score < 70:
            return True

        # Check for performance degradation trends
        if len(self.performance_monitor.health_history) >= 5:
            recent_scores = [h.overall_score for h in list(self.performance_monitor.health_history)[-5:]]
            if self._is_declining_trend(recent_scores):
                return True

        # Check connection pool optimization opportunities
        if self.connection_pool:
            pool_status = await self.connection_pool.get_optimization_status()
            latest_opt = pool_status.get("latest_optimization")

            if latest_opt and latest_opt.get("confidence_score", 0) > 0.7:
                return True

        return False

    async def _perform_automated_optimization(self):
        """Perform automated optimization based on current state."""
        try:
            logger.info("Starting automated optimization")

            # Create optimization plan
            plan = await self.create_optimization_plan()

            # Auto-approve if confidence is high enough
            confidence = self._calculate_plan_confidence(plan)
            if confidence >= self.auto_approval_threshold:
                plan.approved = True
                self.orchestrator_metrics["auto_optimizations"] += 1

                # Execute the plan
                result = await self.execute_optimization_plan(plan.plan_id)

                if result.success:
                    logger.info(f"Automated optimization completed successfully")
                else:
                    logger.warning(f"Automated optimization failed: {result.issues_encountered}")
            else:
                logger.info(f"Automated optimization plan requires manual approval (confidence: {confidence:.2f})")

        except Exception as e:
            logger.error(f"Automated optimization failed: {e}")

    def _is_declining_trend(self, values: List[float]) -> bool:
        """Check if values show a declining trend."""
        if len(values) < 3:
            return False

        # Simple linear regression to detect trend
        n = len(values)
        x_mean = (n - 1) / 2
        y_mean = sum(values) / n

        numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return False

        slope = numerator / denominator
        return slope < -1.0  # Declining trend threshold

    async def _handle_performance_alert(self, alert: PerformanceAlert):
        """Handle performance alerts from the monitor."""
        logger.warning(f"Performance alert: {alert.title} - {alert.description}")

        # Create immediate optimization plan for critical alerts
        if alert.level == PerformanceAlertLevel.CRITICAL and self.automation_level != AutomationLevel.MANUAL:
            try:
                # Create targeted optimization plan
                plan = await self.create_optimization_plan(
                    target_improvements={"latency": 0.5},  # Aggressive improvement target
                    constraints={"immediate": True}
                )

                # Auto-approve for critical issues
                if self.automation_level == AutomationLevel.FULLY_AUTOMATIC:
                    plan.approved = True
                    await self.execute_optimization_plan(plan.plan_id)

            except Exception as e:
                logger.error(f"Failed to create emergency optimization plan: {e}")

        # Call external alert callback if configured
        if self.alert_callback:
            try:
                await self.alert_callback(alert)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")

    async def _handle_poor_health(self, health_score: DatabaseHealthScore):
        """Handle poor database health situations."""
        logger.warning(f"Poor database health detected: {health_score.overall_score:.1f}")

        # Create optimization plan focused on health improvement
        if self.automation_level != AutomationLevel.MANUAL:
            plan = await self.create_optimization_plan(
                target_improvements={
                    "health_score": 0.3,
                    "availability": 0.2
                },
                constraints={"urgent": True}
            )

            # Auto-approve for severe health issues
            if health_score.overall_score < 40 and self.automation_level == AutomationLevel.FULLY_AUTOMATIC:
                plan.approved = True
                await self.execute_optimization_plan(plan.plan_id)

    async def _analyze_current_state(self) -> Dict[str, Any]:
        """Analyze current database state for optimization planning."""
        current_state = {
            "timestamp": datetime.utcnow().isoformat(),
            "connection_pool": {},
            "performance": {},
            "health": {}
        }

        # Get connection pool state
        if self.connection_pool:
            pool_status = await self.connection_pool.get_optimization_status()
            current_state["connection_pool"] = pool_status

        # Get performance metrics
        if self.performance_monitor:
            performance_summary = self.performance_monitor.get_performance_summary(hours=1)
            health_score = self.performance_monitor.calculate_health_score()

            current_state["performance"] = performance_summary
            current_state["health"] = {
                "overall_score": health_score.overall_score,
                "performance_score": health_score.performance_score,
                "availability_score": health_score.availability_score,
                "efficiency_score": health_score.efficiency_score
            }

        return current_state

    async def _generate_optimization_actions(
        self,
        current_state: Dict[str, Any],
        target_improvements: Optional[Dict[str, float]],
        constraints: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Generate optimization actions based on current state and targets."""
        actions = []

        # Pool optimization actions
        pool_state = current_state.get("connection_pool", {})
        if pool_state.get("latest_optimization"):
            latest_opt = pool_state["latest_optimization"]
            if latest_opt.get("confidence_score", 0) > 0.6:
                actions.append({
                    "type": "optimize_connection_pool",
                    "description": "Apply connection pool optimization recommendations",
                    "parameters": {
                        "new_pool_size": latest_opt.get("recommended_pool_size"),
                        "new_max_overflow": latest_opt.get("recommended_max_overflow")
                    },
                    "estimated_duration_minutes": 2,
                    "critical": False
                })

        # Performance-based actions
        performance_state = current_state.get("performance", {})
        health_state = current_state.get("health", {})

        if performance_state.get("slow_query_count", 0) > 10:
            actions.append({
                "type": "analyze_slow_queries",
                "description": "Analyze and optimize slow queries",
                "parameters": {
                    "threshold": 5.0,
                    "limit": 10
                },
                "estimated_duration_minutes": 10,
                "critical": False
            })

        if health_state.get("efficiency_score", 100) < 60:
            actions.append({
                "type": "optimize_query_efficiency",
                "description": "Optimize query efficiency and resource usage",
                "parameters": {
                    "target_efficiency": 80
                },
                "estimated_duration_minutes": 15,
                "critical": False
            })

        # Add emergency actions for critical situations
        if constraints and constraints.get("urgent"):
            actions.insert(0, {
                "type": "emergency_cleanup",
                "description": "Perform emergency database cleanup and optimization",
                "parameters": {},
                "estimated_duration_minutes": 5,
                "critical": True
            })

        return actions

    async def _execute_optimization_action(self, action: Dict[str, Any]):
        """Execute a single optimization action."""
        action_type = action["type"]
        parameters = action.get("parameters", {})

        if action_type == "optimize_connection_pool":
            # Apply connection pool optimization
            if self.connection_pool:
                # The connection pool handles its own optimization
                logger.info("Connection pool optimization triggered")

        elif action_type == "analyze_slow_queries":
            # Analyze slow queries
            if self.performance_monitor:
                threshold = parameters.get("threshold", 5.0)
                slow_queries = [q for q in self.performance_monitor.slow_queries if q.execution_time > threshold]

                for query in slow_queries[:parameters.get("limit", 10)]:
                    recommendations = self.performance_monitor.get_query_recommendations(query.query_hash)
                    logger.info(f"Slow query optimization recommendations for {query.query_id}: {recommendations}")

        elif action_type == "optimize_query_efficiency":
            # Query efficiency optimization (would implement specific optimizations)
            logger.info("Executing query efficiency optimization")

        elif action_type == "emergency_cleanup":
            # Emergency cleanup procedures
            logger.info("Executing emergency database cleanup")

        else:
            logger.warning(f"Unknown optimization action type: {action_type}")

    def get_optimization_status(self) -> Dict[str, Any]:
        """Get current optimization status and metrics."""
        return {
            "orchestrator_metrics": self.orchestrator_metrics,
            "current_optimization": {
                "plan_id": self.current_optimization.plan_id if self.current_optimization else None,
                "started_at": self.current_optimization.created_at.isoformat() if self.current_optimization else None
            },
            "optimization_mode": self.optimization_mode.value,
            "automation_level": self.automation_level.value,
            "pending_plans": len([p for p in self.optimization_plans if not p.executed]),
            "recent_optimizations": [
                {
                    "plan_id": result.plan_id,
                    "success": result.success,
                    "execution_time_seconds": result.execution_time.total_seconds(),
                    "performance_improvement": result.performance_improvement
                }
                for result in self.optimization_history[-5:]  # Last 5 optimizations
            ]
        }

    async def close(self):
        """Close the orchestrator and cleanup resources."""
        try:
            logger.info("Closing database optimization orchestrator")

            # Cancel background tasks
            for task in [self.optimization_task, self.health_monitoring_task, self.metrics_collection_task]:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            # Close connection pool
            if self.connection_pool:
                await self.connection_pool.close()

            logger.info("Database optimization orchestrator closed successfully")

        except Exception as e:
            logger.error(f"Error closing database optimization orchestrator: {e}")

    # Additional helper methods for optimization execution
    async def _estimate_optimization_impact(self, actions: List[Dict[str, Any]]) -> Dict[str, float]:
        """Estimate the impact of optimization actions."""
        # Simplified impact estimation
        total_impact = {"performance": 0.0, "efficiency": 0.0, "cost": 0.0}

        for action in actions:
            if action["type"] == "optimize_connection_pool":
                total_impact["performance"] += 0.1
                total_impact["efficiency"] += 0.15
            elif action["type"] == "analyze_slow_queries":
                total_impact["performance"] += 0.2
                total_impact["efficiency"] += 0.1
            elif action["type"] == "emergency_cleanup":
                total_impact["performance"] += 0.3
                total_impact["efficiency"] += 0.2

        return total_impact

    def _estimate_optimization_duration(self, actions: List[Dict[str, Any]]) -> timedelta:
        """Estimate total duration for optimization actions."""
        total_minutes = sum(action.get("estimated_duration_minutes", 5) for action in actions)
        return timedelta(minutes=total_minutes)

    def _identify_optimization_risks(self, actions: List[Dict[str, Any]]) -> List[str]:
        """Identify risks associated with optimization actions."""
        risks = []

        for action in actions:
            if action["type"] == "optimize_connection_pool":
                risks.append("Temporary connection disruption during pool reconfiguration")
            elif action["type"] == "emergency_cleanup":
                risks.append("Potential service interruption during emergency procedures")

        if not risks:
            risks.append("Minimal risk - safe optimization actions")

        return risks

    def _identify_prerequisites(self, actions: List[Dict[str, Any]]) -> List[str]:
        """Identify prerequisites for optimization actions."""
        prerequisites = []

        has_critical_actions = any(action.get("critical", False) for action in actions)
        if has_critical_actions:
            prerequisites.append("Backup database state before execution")

        if any(action["type"] == "optimize_connection_pool" for action in actions):
            prerequisites.append("Ensure no critical operations are running")

        return prerequisites

    def _create_rollback_plan(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create rollback plan for optimization actions."""
        rollback_actions = []

        for action in reversed(actions):  # Reverse order for rollback
            if action["type"] == "optimize_connection_pool":
                rollback_actions.append({
                    "type": "restore_connection_pool",
                    "description": "Restore previous connection pool configuration"
                })
            elif action["type"] == "emergency_cleanup":
                rollback_actions.append({
                    "type": "restore_from_backup",
                    "description": "Restore database state from backup"
                })

        return rollback_actions

    async def _execute_rollback_plan(self, rollback_plan: List[Dict[str, Any]]):
        """Execute rollback plan."""
        logger.warning("Executing rollback plan")

        for action in rollback_plan:
            try:
                await self._execute_optimization_action(action)
                logger.info(f"Rollback action completed: {action['type']}")
            except Exception as e:
                logger.error(f"Rollback action failed: {action['type']} - {e}")

    def _calculate_plan_confidence(self, plan: OptimizationPlan) -> float:
        """Calculate confidence score for optimization plan."""
        # Simplified confidence calculation
        base_confidence = 0.7

        # Reduce confidence for critical actions
        critical_actions = sum(1 for action in plan.actions if action.get("critical", False))
        confidence_penalty = critical_actions * 0.1

        # Increase confidence for safe actions
        safe_actions = len(plan.actions) - critical_actions
        confidence_bonus = safe_actions * 0.05

        final_confidence = base_confidence - confidence_penalty + confidence_bonus
        return max(0.0, min(1.0, final_confidence))

    async def _capture_baseline_metrics(self) -> Dict[str, Any]:
        """Capture baseline metrics for comparison."""
        metrics = {}

        if self.performance_monitor:
            metrics["performance"] = self.performance_monitor.get_performance_summary(hours=1)
            metrics["health"] = self.performance_monitor.calculate_health_score().__dict__

        if self.connection_pool:
            metrics["pool"] = await self.connection_pool.get_optimization_status()

        return metrics

    def _calculate_performance_improvement(
        self,
        baseline: Dict[str, Any],
        current: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate performance improvement between baseline and current state."""
        improvements = {}

        # Compare performance metrics
        baseline_perf = baseline.get("performance", {})
        current_perf = current.get("performance", {})

        if baseline_perf.get("avg_execution_time") and current_perf.get("avg_execution_time"):
            baseline_time = baseline_perf["avg_execution_time"]
            current_time = current_perf["avg_execution_time"]
            improvements["execution_time"] = (baseline_time - current_time) / baseline_time

        # Compare health scores
        baseline_health = baseline.get("health", {}).get("overall_score", 0)
        current_health = current.get("health", {}).get("overall_score", 0)

        if baseline_health > 0:
            improvements["health_score"] = (current_health - baseline_health) / baseline_health

        return improvements

    def _calculate_cost_impact(
        self,
        baseline: Dict[str, Any],
        current: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate cost impact of optimization."""
        # Simplified cost calculation based on pool size changes
        cost_impact = {"connection_pool": 0.0}

        baseline_pool = baseline.get("pool", {}).get("current_configuration", {})
        current_pool = current.get("pool", {}).get("current_configuration", {})

        baseline_size = baseline_pool.get("pool_size", 0)
        current_size = current_pool.get("pool_size", 0)

        if baseline_size > 0:
            size_change = (current_size - baseline_size) / baseline_size
            cost_impact["connection_pool"] = size_change * 0.1  # Assume 10% cost per size unit

        return cost_impact

    async def _update_orchestrator_metrics(self):
        """Update orchestrator-level metrics."""
        if self.optimization_history:
            successful_optimizations = [r for r in self.optimization_history if r.success]
            if successful_optimizations:
                avg_improvement = sum(
                    r.performance_improvement.get("execution_time", 0)
                    for r in successful_optimizations
                ) / len(successful_optimizations)
                self.orchestrator_metrics["avg_performance_improvement"] = avg_improvement


# Factory function
def create_database_optimization_orchestrator(
    database_url: str,
    optimization_mode: OptimizationMode = OptimizationMode.BALANCED,
    automation_level: AutomationLevel = AutomationLevel.SEMI_AUTOMATIC,
    alert_callback: Optional[Callable] = None
) -> DatabaseOptimizationOrchestrator:
    """Create database optimization orchestrator with enterprise configuration."""

    return DatabaseOptimizationOrchestrator(
        database_url=database_url,
        optimization_mode=optimization_mode,
        automation_level=automation_level,
        alert_callback=alert_callback
    )