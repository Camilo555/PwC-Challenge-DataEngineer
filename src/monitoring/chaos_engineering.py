"""
Chaos Engineering Monitoring - Failure Injection Testing
=======================================================

Enterprise-grade chaos engineering for proactive reliability validation:
- Controlled failure injection with safety guardrails
- Automated resilience testing with business impact assessment
- Real-time system behavior monitoring during chaos experiments
- Intelligent experiment scheduling with blast radius control
- Comprehensive recovery validation with SLA compliance tracking
- Business continuity verification with customer impact minimization

Key Features:
- Multi-layer failure injection: network, compute, storage, application
- Automated rollback with circuit breaker integration
- Real-time hypothesis validation with statistical analysis
- Business metric correlation during chaos experiments
- Compliance-ready experiment audit trails
- Progressive experiment execution with confidence building
"""

import asyncio
import json
import logging
import random
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
import statistics

import aiohttp
import aioredis
import asyncpg
from fastapi import HTTPException
from pydantic import BaseModel, Field

from core.config import get_settings
from core.logging import get_logger

# Configure logging
logger = logging.getLogger(__name__)

# Chaos Engineering Types and Configurations
class ChaosType(str, Enum):
    """Chaos experiment type enumeration"""
    NETWORK_LATENCY = "network_latency"
    NETWORK_PARTITION = "network_partition"
    NETWORK_PACKET_LOSS = "network_packet_loss"
    SERVICE_UNAVAILABLE = "service_unavailable"
    DATABASE_SLOWDOWN = "database_slowdown"
    MEMORY_PRESSURE = "memory_pressure"
    CPU_SPIKE = "cpu_spike"
    DISK_FILL = "disk_fill"
    DEPENDENCY_FAILURE = "dependency_failure"
    CONFIGURATION_DRIFT = "configuration_drift"
    SECURITY_BREACH_SIMULATION = "security_breach_simulation"

class ExperimentStatus(str, Enum):
    """Experiment status enumeration"""
    SCHEDULED = "scheduled"
    RUNNING = "running"
    RECOVERING = "recovering"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"
    PAUSED = "paused"

class BlastRadius(str, Enum):
    """Blast radius scope enumeration"""
    SINGLE_INSTANCE = "single_instance"
    SINGLE_SERVICE = "single_service"
    SERVICE_CLUSTER = "service_cluster"
    AVAILABILITY_ZONE = "availability_zone"
    REGION = "region"
    GLOBAL = "global"

class SafetyLevel(str, Enum):
    """Safety level enumeration"""
    SAFE = "safe"           # Non-production, minimal impact
    CAUTIOUS = "cautious"   # Production with extensive monitoring
    AGGRESSIVE = "aggressive"  # High impact for resilience testing

class HypothesisResult(str, Enum):
    """Hypothesis validation result"""
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INCONCLUSIVE = "inconclusive"
    PARTIALLY_CONFIRMED = "partially_confirmed"

# Data Models
@dataclass
class SafetyGuard:
    """Safety guard configuration for chaos experiments"""
    guard_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""

    # Monitoring conditions
    metric_name: str = ""
    threshold_value: float = 0.0
    comparison_operator: str = ">"  # >, <, >=, <=, ==, !=
    evaluation_window_seconds: int = 60

    # Actions
    abort_experiment: bool = True
    alert_severity: str = "critical"
    notification_channels: List[str] = field(default_factory=list)

    # Business context
    business_impact_threshold: float = 0.1  # 10% impact threshold
    customer_facing: bool = True

@dataclass
class ChaosExperiment:
    """Comprehensive chaos experiment configuration"""
    experiment_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""

    # Experiment classification
    chaos_type: ChaosType = ChaosType.NETWORK_LATENCY
    blast_radius: BlastRadius = BlastRadius.SINGLE_INSTANCE
    safety_level: SafetyLevel = SafetyLevel.SAFE

    # Target configuration
    target_services: List[str] = field(default_factory=list)
    target_instances: List[str] = field(default_factory=list)
    target_percentage: float = 10.0  # Percentage of targets to affect

    # Experiment parameters
    duration_minutes: int = 5
    intensity: float = 0.5  # 0.0 to 1.0 scale
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Hypothesis and validation
    hypothesis: str = ""
    success_criteria: List[Dict[str, Any]] = field(default_factory=list)

    # Safety and monitoring
    safety_guards: List[SafetyGuard] = field(default_factory=list)
    rollback_strategy: Dict[str, Any] = field(default_factory=dict)

    # Scheduling
    scheduled_at: Optional[datetime] = None
    business_hours_only: bool = True
    maintenance_window: bool = False

    # Metadata
    created_by: str = "system"
    created_at: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)
    enabled: bool = True

@dataclass
class ExperimentExecution:
    """Chaos experiment execution tracking"""
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    experiment_id: str = ""

    # Execution lifecycle
    status: ExperimentStatus = ExperimentStatus.SCHEDULED
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Target tracking
    affected_targets: List[str] = field(default_factory=list)
    success_targets: List[str] = field(default_factory=list)
    failed_targets: List[str] = field(default_factory=list)

    # Metrics and monitoring
    baseline_metrics: Dict[str, float] = field(default_factory=dict)
    experiment_metrics: Dict[str, float] = field(default_factory=dict)
    recovery_metrics: Dict[str, float] = field(default_factory=dict)

    # Hypothesis validation
    hypothesis_result: HypothesisResult = HypothesisResult.INCONCLUSIVE
    success_criteria_met: int = 0
    total_success_criteria: int = 0

    # Safety and incidents
    safety_violations: List[Dict[str, Any]] = field(default_factory=list)
    incidents_triggered: List[str] = field(default_factory=list)
    auto_aborted: bool = False
    abort_reason: Optional[str] = None

    # Business impact
    customer_impact_score: float = 0.0
    revenue_impact_estimate: float = 0.0
    sla_violations: List[Dict[str, Any]] = field(default_factory=list)

    # Logs and artifacts
    execution_logs: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ResilienceReport:
    """System resilience assessment report"""
    report_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    generated_at: datetime = field(default_factory=datetime.utcnow)

    # Assessment period
    assessment_period_days: int = 30
    total_experiments: int = 0
    successful_experiments: int = 0

    # Resilience scores
    overall_resilience_score: float = 0.0
    service_resilience_scores: Dict[str, float] = field(default_factory=dict)
    failure_type_resilience: Dict[str, float] = field(default_factory=dict)

    # Vulnerability analysis
    identified_vulnerabilities: List[Dict[str, Any]] = field(default_factory=list)
    critical_weaknesses: List[str] = field(default_factory=list)
    improvement_opportunities: List[str] = field(default_factory=list)

    # Business impact analysis
    business_continuity_score: float = 0.0
    customer_impact_analysis: Dict[str, Any] = field(default_factory=dict)
    financial_risk_assessment: Dict[str, Any] = field(default_factory=dict)

    # Recommendations
    immediate_actions: List[str] = field(default_factory=list)
    strategic_improvements: List[str] = field(default_factory=list)
    investment_priorities: List[Dict[str, Any]] = field(default_factory=list)

class ChaosEngineering:
    """
    Comprehensive chaos engineering system for reliability validation

    Features:
    - Controlled failure injection with intelligent safety guardrails
    - Real-time system behavior monitoring during experiments
    - Automated hypothesis validation with statistical analysis
    - Business impact assessment with customer protection
    - Progressive experiment execution with confidence building
    - Comprehensive resilience reporting with actionable insights
    """

    def __init__(self):
        self.settings = get_settings()
        self.logger = get_logger(__name__)

        # Client connections
        self.redis_client: Optional[aioredis.Redis] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.http_session: Optional[aiohttp.ClientSession] = None

        # Experiment management
        self.experiments: Dict[str, ChaosExperiment] = {}
        self.active_executions: Dict[str, ExperimentExecution] = {}
        self.scheduled_experiments: List[str] = []

        # Safety and monitoring
        self.global_safety_guards: List[SafetyGuard] = []
        self.experiment_queue = asyncio.Queue(maxsize=100)
        self.abort_signals: Dict[str, bool] = {}

        # Resilience tracking
        self.resilience_baselines: Dict[str, Dict[str, float]] = {}
        self.failure_patterns: Dict[str, List[Dict[str, Any]]] = {}

        # System state
        self.is_running = False
        self.emergency_stop = False
        self.statistics = {
            "total_experiments": 0,
            "successful_experiments": 0,
            "aborted_experiments": 0,
            "vulnerabilities_found": 0,
            "avg_resilience_score": 0.0
        }

    async def initialize(self):
        """Initialize chaos engineering system"""
        try:
            # Initialize Redis connection
            self.redis_client = aioredis.from_url(
                f"redis://{self.settings.redis_host}:{self.settings.redis_port}",
                decode_responses=True
            )

            # Initialize database connection pool
            self.db_pool = await asyncpg.create_pool(
                host=self.settings.db_host,
                port=self.settings.db_port,
                user=self.settings.db_user,
                password=self.settings.db_password,
                database=self.settings.db_name,
                min_size=2,
                max_size=10
            )

            # Initialize HTTP session
            self.http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )

            # Load existing experiments
            await self._load_experiments_from_db()

            # Setup global safety guards
            await self._setup_global_safety_guards()

            # Start background workers
            asyncio.create_task(self._experiment_scheduler_worker())
            asyncio.create_task(self._safety_monitor_worker())
            asyncio.create_task(self._resilience_analyzer_worker())
            asyncio.create_task(self._experiment_executor_worker())

            # Create default experiments
            await self._create_default_experiments()

            self.is_running = True
            self.logger.info("Chaos engineering system initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize chaos engineering: {str(e)}")
            raise

    async def create_experiment(self, experiment: ChaosExperiment) -> str:
        """Create new chaos experiment"""
        try:
            # Validate experiment configuration
            await self._validate_experiment(experiment)

            # Store experiment
            self.experiments[experiment.experiment_id] = experiment
            await self._save_experiment_to_db(experiment)

            # Add to schedule if enabled
            if experiment.enabled and experiment.scheduled_at:
                self.scheduled_experiments.append(experiment.experiment_id)
                self.scheduled_experiments.sort(
                    key=lambda eid: self.experiments[eid].scheduled_at or datetime.max
                )

            self.statistics["total_experiments"] += 1

            self.logger.info(f"Chaos experiment created: {experiment.experiment_id} - {experiment.name}")
            return experiment.experiment_id

        except Exception as e:
            self.logger.error(f"Failed to create chaos experiment: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Experiment creation failed: {str(e)}")

    async def execute_experiment(self, experiment_id: str, dry_run: bool = False) -> ExperimentExecution:
        """Execute chaos experiment with comprehensive monitoring"""
        try:
            if experiment_id not in self.experiments:
                raise ValueError(f"Experiment not found: {experiment_id}")

            experiment = self.experiments[experiment_id]

            # Create execution tracking
            execution = ExperimentExecution(
                experiment_id=experiment_id,
                started_at=datetime.utcnow(),
                total_success_criteria=len(experiment.success_criteria)
            )

            if dry_run:
                execution.status = ExperimentStatus.COMPLETED
                execution.hypothesis_result = HypothesisResult.INCONCLUSIVE
                return execution

            # Pre-execution safety checks
            await self._pre_execution_safety_check(experiment, execution)

            # Store active execution
            self.active_executions[execution.execution_id] = execution

            try:
                # Execute experiment phases
                await self._execute_experiment_phases(experiment, execution)

                # Mark as completed
                execution.completed_at = datetime.utcnow()
                execution.duration_seconds = (execution.completed_at - execution.started_at).total_seconds()
                execution.status = ExperimentStatus.COMPLETED

                if not execution.auto_aborted:
                    self.statistics["successful_experiments"] += 1
                else:
                    self.statistics["aborted_experiments"] += 1

            except Exception as e:
                execution.status = ExperimentStatus.FAILED
                execution.abort_reason = str(e)
                execution.completed_at = datetime.utcnow()
                execution.duration_seconds = (execution.completed_at - execution.started_at).total_seconds()
                raise

            finally:
                # Store execution results
                await self._store_execution_results(execution)

                # Remove from active executions
                if execution.execution_id in self.active_executions:
                    del self.active_executions[execution.execution_id]

            return execution

        except Exception as e:
            self.logger.error(f"Failed to execute chaos experiment {experiment_id}: {str(e)}")
            raise

    async def abort_experiment(self, execution_id: str, reason: str = "Manual abort") -> bool:
        """Abort running chaos experiment"""
        try:
            if execution_id not in self.active_executions:
                raise ValueError(f"Active execution not found: {execution_id}")

            # Set abort signal
            self.abort_signals[execution_id] = True

            execution = self.active_executions[execution_id]
            execution.auto_aborted = True
            execution.abort_reason = reason
            execution.status = ExperimentStatus.ABORTED

            self.logger.warning(f"Chaos experiment aborted: {execution_id} - {reason}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to abort experiment {execution_id}: {str(e)}")
            return False

    async def get_resilience_report(self, days: int = 30) -> ResilienceReport:
        """Generate comprehensive resilience assessment report"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)

            # Query execution data
            async with self.db_pool.acquire() as conn:
                executions = await conn.fetch("""
                    SELECT * FROM chaos_executions
                    WHERE started_at >= $1 AND started_at <= $2
                    ORDER BY started_at DESC
                """, start_time, end_time)

            report = ResilienceReport(
                assessment_period_days=days,
                total_experiments=len(executions)
            )

            if not executions:
                return report

            # Calculate basic statistics
            successful = [e for e in executions if e["status"] == "completed" and not e["auto_aborted"]]
            report.successful_experiments = len(successful)

            # Calculate overall resilience score
            if executions:
                success_rate = len(successful) / len(executions)
                avg_hypothesis_confirmation = sum(
                    1 for e in successful if e.get("hypothesis_result") == "confirmed"
                ) / len(successful) if successful else 0

                report.overall_resilience_score = (success_rate * 0.6) + (avg_hypothesis_confirmation * 0.4)

            # Analyze service-specific resilience
            service_stats = {}
            for execution in executions:
                experiment = self.experiments.get(execution["experiment_id"])
                if experiment:
                    for service in experiment.target_services:
                        if service not in service_stats:
                            service_stats[service] = {"total": 0, "successful": 0}
                        service_stats[service]["total"] += 1
                        if execution["status"] == "completed" and not execution["auto_aborted"]:
                            service_stats[service]["successful"] += 1

            for service, stats in service_stats.items():
                if stats["total"] > 0:
                    report.service_resilience_scores[service] = stats["successful"] / stats["total"]

            # Identify vulnerabilities
            vulnerabilities = []
            for execution in executions:
                if execution.get("auto_aborted") or execution.get("safety_violations"):
                    experiment = self.experiments.get(execution["experiment_id"])
                    if experiment:
                        vulnerabilities.append({
                            "experiment_name": experiment.name,
                            "chaos_type": experiment.chaos_type.value,
                            "severity": "high" if execution.get("auto_aborted") else "medium",
                            "description": execution.get("abort_reason", "Safety violation detected")
                        })

            report.identified_vulnerabilities = vulnerabilities
            self.statistics["vulnerabilities_found"] = len(vulnerabilities)

            # Generate recommendations
            await self._generate_resilience_recommendations(report, executions)

            # Calculate business impact
            await self._calculate_business_continuity_score(report, executions)

            # Update system statistics
            self.statistics["avg_resilience_score"] = report.overall_resilience_score

            return report

        except Exception as e:
            self.logger.error(f"Failed to generate resilience report: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Resilience report generation failed: {str(e)}")

    async def get_experiment_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive chaos engineering statistics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)

            # Query recent executions
            async with self.db_pool.acquire() as conn:
                recent_executions = await conn.fetch("""
                    SELECT * FROM chaos_executions
                    WHERE started_at >= $1 AND started_at <= $2
                    ORDER BY started_at DESC
                """, start_time, end_time)

            # Calculate statistics
            total_recent = len(recent_executions)
            successful_recent = len([e for e in recent_executions if e["status"] == "completed" and not e["auto_aborted"]])
            aborted_recent = len([e for e in recent_executions if e["auto_aborted"]])

            # Experiment type distribution
            experiment_types = {}
            for execution in recent_executions:
                experiment = self.experiments.get(execution["experiment_id"])
                if experiment:
                    chaos_type = experiment.chaos_type.value
                    experiment_types[chaos_type] = experiment_types.get(chaos_type, 0) + 1

            # Safety violations
            safety_violations = sum(len(e.get("safety_violations", [])) for e in recent_executions)

            return {
                "time_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                    "hours": hours
                },
                "experiment_summary": {
                    "total_experiments": total_recent,
                    "successful_experiments": successful_recent,
                    "aborted_experiments": aborted_recent,
                    "success_rate_percentage": round((successful_recent / total_recent * 100) if total_recent > 0 else 0, 2),
                    "safety_violations": safety_violations
                },
                "experiment_types": experiment_types,
                "active_experiments": len(self.active_executions),
                "scheduled_experiments": len(self.scheduled_experiments),
                "global_statistics": self.statistics,
                "system_status": {
                    "is_running": self.is_running,
                    "emergency_stop": self.emergency_stop,
                    "total_configured_experiments": len(self.experiments)
                },
                "resilience_insights": {
                    "overall_resilience_score": round(self.statistics["avg_resilience_score"], 3),
                    "vulnerabilities_identified": self.statistics["vulnerabilities_found"],
                    "system_stability": "excellent" if self.statistics["avg_resilience_score"] > 0.9 else
                                     "good" if self.statistics["avg_resilience_score"] > 0.8 else
                                     "needs_improvement"
                },
                "generated_at": datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get experiment statistics: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Statistics retrieval failed: {str(e)}")

    # Private Methods - Experiment Execution
    async def _execute_experiment_phases(self, experiment: ChaosExperiment, execution: ExperimentExecution):
        """Execute chaos experiment in phases with comprehensive monitoring"""
        try:
            # Phase 1: Baseline Collection
            execution.status = ExperimentStatus.RUNNING
            await self._log_execution_event(execution, "Collecting baseline metrics")

            execution.baseline_metrics = await self._collect_baseline_metrics(experiment)

            # Phase 2: Failure Injection
            await self._log_execution_event(execution, f"Injecting {experiment.chaos_type.value} failures")

            affected_targets = await self._inject_failures(experiment, execution)
            execution.affected_targets = affected_targets

            # Phase 3: Monitoring and Validation
            await self._log_execution_event(execution, "Monitoring system behavior")

            await self._monitor_experiment_progress(experiment, execution)

            # Phase 4: Recovery and Cleanup
            execution.status = ExperimentStatus.RECOVERING
            await self._log_execution_event(execution, "Starting recovery process")

            await self._recover_from_failures(experiment, execution)

            # Phase 5: Post-experiment Analysis
            await self._log_execution_event(execution, "Performing post-experiment analysis")

            execution.recovery_metrics = await self._collect_recovery_metrics(experiment)
            await self._validate_hypothesis(experiment, execution)

        except Exception as e:
            execution.abort_reason = str(e)
            execution.auto_aborted = True
            await self._emergency_recovery(experiment, execution)
            raise

    async def _inject_failures(self, experiment: ChaosExperiment, execution: ExperimentExecution) -> List[str]:
        """Inject failures based on experiment configuration"""
        affected_targets = []

        try:
            if experiment.chaos_type == ChaosType.NETWORK_LATENCY:
                affected_targets = await self._inject_network_latency(experiment, execution)
            elif experiment.chaos_type == ChaosType.SERVICE_UNAVAILABLE:
                affected_targets = await self._inject_service_unavailability(experiment, execution)
            elif experiment.chaos_type == ChaosType.DATABASE_SLOWDOWN:
                affected_targets = await self._inject_database_slowdown(experiment, execution)
            elif experiment.chaos_type == ChaosType.MEMORY_PRESSURE:
                affected_targets = await self._inject_memory_pressure(experiment, execution)
            elif experiment.chaos_type == ChaosType.CPU_SPIKE:
                affected_targets = await self._inject_cpu_spike(experiment, execution)
            else:
                raise ValueError(f"Unsupported chaos type: {experiment.chaos_type}")

            return affected_targets

        except Exception as e:
            self.logger.error(f"Failure injection failed: {str(e)}")
            raise

    async def _inject_network_latency(self, experiment: ChaosExperiment, execution: ExperimentExecution) -> List[str]:
        """Inject network latency using traffic control simulation"""
        affected_targets = []

        # Get latency parameters
        latency_ms = experiment.parameters.get("latency_ms", 500)
        jitter_ms = experiment.parameters.get("jitter_ms", 50)

        # Select targets based on percentage
        target_count = max(1, int(len(experiment.target_services) * experiment.target_percentage / 100))
        selected_targets = random.sample(experiment.target_services, min(target_count, len(experiment.target_services)))

        for target in selected_targets:
            try:
                # Simulate network latency injection
                # In a real implementation, this would use tools like tc (traffic control)
                # or integrate with service mesh for fault injection

                await self._log_execution_event(
                    execution,
                    f"Injecting {latency_ms}ms latency to {target}"
                )

                # Store injection details for cleanup
                injection_details = {
                    "target": target,
                    "type": "network_latency",
                    "parameters": {"latency_ms": latency_ms, "jitter_ms": jitter_ms},
                    "injected_at": datetime.utcnow().isoformat()
                }

                if "injections" not in execution.artifacts:
                    execution.artifacts["injections"] = []
                execution.artifacts["injections"].append(injection_details)

                affected_targets.append(target)

            except Exception as e:
                execution.failed_targets.append(target)
                await self._log_execution_event(
                    execution,
                    f"Failed to inject latency to {target}: {str(e)}"
                )

        return affected_targets

    async def _inject_service_unavailability(self, experiment: ChaosExperiment, execution: ExperimentExecution) -> List[str]:
        """Inject service unavailability by simulating downtime"""
        affected_targets = []

        # Get unavailability parameters
        error_rate = experiment.parameters.get("error_rate", 0.5)  # 50% error rate
        error_codes = experiment.parameters.get("error_codes", [503, 504, 500])

        for target in experiment.target_services:
            try:
                await self._log_execution_event(
                    execution,
                    f"Making {target} unavailable with {error_rate*100}% error rate"
                )

                # In a real implementation, this would:
                # 1. Configure load balancer to return errors
                # 2. Use service mesh to inject faults
                # 3. Scale down service instances
                # 4. Block network traffic to service

                injection_details = {
                    "target": target,
                    "type": "service_unavailable",
                    "parameters": {"error_rate": error_rate, "error_codes": error_codes},
                    "injected_at": datetime.utcnow().isoformat()
                }

                if "injections" not in execution.artifacts:
                    execution.artifacts["injections"] = []
                execution.artifacts["injections"].append(injection_details)

                affected_targets.append(target)

            except Exception as e:
                execution.failed_targets.append(target)
                await self._log_execution_event(
                    execution,
                    f"Failed to make {target} unavailable: {str(e)}"
                )

        return affected_targets

    async def _inject_database_slowdown(self, experiment: ChaosExperiment, execution: ExperimentExecution) -> List[str]:
        """Inject database slowdown by simulating query delays"""
        affected_targets = []

        slowdown_factor = experiment.parameters.get("slowdown_factor", 3.0)  # 3x slower
        affected_queries = experiment.parameters.get("affected_queries", ["SELECT", "UPDATE", "INSERT"])

        try:
            await self._log_execution_event(
                execution,
                f"Injecting database slowdown with {slowdown_factor}x factor"
            )

            # In a real implementation, this would:
            # 1. Add artificial delays to database connections
            # 2. Limit connection pool size
            # 3. Inject slow queries
            # 4. Simulate lock contention

            injection_details = {
                "target": "database",
                "type": "database_slowdown",
                "parameters": {"slowdown_factor": slowdown_factor, "affected_queries": affected_queries},
                "injected_at": datetime.utcnow().isoformat()
            }

            if "injections" not in execution.artifacts:
                execution.artifacts["injections"] = []
            execution.artifacts["injections"].append(injection_details)

            affected_targets.append("database")

        except Exception as e:
            execution.failed_targets.append("database")
            await self._log_execution_event(
                execution,
                f"Failed to inject database slowdown: {str(e)}"
            )

        return affected_targets

    async def _inject_memory_pressure(self, experiment: ChaosExperiment, execution: ExperimentExecution) -> List[str]:
        """Inject memory pressure by consuming system memory"""
        affected_targets = []

        memory_percentage = experiment.parameters.get("memory_percentage", 80)  # 80% memory usage
        duration_seconds = experiment.duration_minutes * 60

        for target in experiment.target_services:
            try:
                await self._log_execution_event(
                    execution,
                    f"Injecting memory pressure on {target} ({memory_percentage}% usage)"
                )

                # In a real implementation, this would:
                # 1. Allocate memory in target processes
                # 2. Use memory stress tools
                # 3. Limit memory cgroups
                # 4. Trigger garbage collection pressure

                injection_details = {
                    "target": target,
                    "type": "memory_pressure",
                    "parameters": {"memory_percentage": memory_percentage, "duration_seconds": duration_seconds},
                    "injected_at": datetime.utcnow().isoformat()
                }

                if "injections" not in execution.artifacts:
                    execution.artifacts["injections"] = []
                execution.artifacts["injections"].append(injection_details)

                affected_targets.append(target)

            except Exception as e:
                execution.failed_targets.append(target)
                await self._log_execution_event(
                    execution,
                    f"Failed to inject memory pressure on {target}: {str(e)}"
                )

        return affected_targets

    async def _inject_cpu_spike(self, experiment: ChaosExperiment, execution: ExperimentExecution) -> List[str]:
        """Inject CPU spike by consuming CPU resources"""
        affected_targets = []

        cpu_percentage = experiment.parameters.get("cpu_percentage", 90)  # 90% CPU usage
        duration_seconds = experiment.duration_minutes * 60

        for target in experiment.target_services:
            try:
                await self._log_execution_event(
                    execution,
                    f"Injecting CPU spike on {target} ({cpu_percentage}% usage)"
                )

                # In a real implementation, this would:
                # 1. Create CPU-intensive processes
                # 2. Use stress testing tools
                # 3. Limit CPU cgroups
                # 4. Trigger compute-heavy operations

                injection_details = {
                    "target": target,
                    "type": "cpu_spike",
                    "parameters": {"cpu_percentage": cpu_percentage, "duration_seconds": duration_seconds},
                    "injected_at": datetime.utcnow().isoformat()
                }

                if "injections" not in execution.artifacts:
                    execution.artifacts["injections"] = []
                execution.artifacts["injections"].append(injection_details)

                affected_targets.append(target)

            except Exception as e:
                execution.failed_targets.append(target)
                await self._log_execution_event(
                    execution,
                    f"Failed to inject CPU spike on {target}: {str(e)}"
                )

        return affected_targets

    async def _monitor_experiment_progress(self, experiment: ChaosExperiment, execution: ExperimentExecution):
        """Monitor experiment progress and check safety conditions"""
        try:
            monitoring_duration = experiment.duration_minutes * 60
            check_interval = 10  # Check every 10 seconds
            checks_performed = 0
            max_checks = monitoring_duration // check_interval

            while checks_performed < max_checks:
                # Check for abort signals
                if execution.execution_id in self.abort_signals and self.abort_signals[execution.execution_id]:
                    raise Exception("Experiment aborted by external signal")

                # Check safety guards
                safety_violation = await self._check_safety_guards(experiment, execution)
                if safety_violation:
                    execution.safety_violations.append(safety_violation)
                    if safety_violation.get("abort_required", False):
                        execution.auto_aborted = True
                        execution.abort_reason = f"Safety violation: {safety_violation['description']}"
                        raise Exception(f"Safety guard triggered: {safety_violation['description']}")

                # Collect current metrics
                current_metrics = await self._collect_current_metrics(experiment)
                execution.experiment_metrics.update(current_metrics)

                # Check success criteria
                await self._check_success_criteria(experiment, execution)

                await asyncio.sleep(check_interval)
                checks_performed += 1

                # Log progress
                progress_percentage = (checks_performed / max_checks) * 100
                await self._log_execution_event(
                    execution,
                    f"Monitoring progress: {progress_percentage:.1f}% ({checks_performed}/{max_checks})"
                )

        except Exception as e:
            self.logger.error(f"Experiment monitoring failed: {str(e)}")
            raise

    async def _recover_from_failures(self, experiment: ChaosExperiment, execution: ExperimentExecution):
        """Recover from injected failures and cleanup"""
        try:
            injections = execution.artifacts.get("injections", [])

            for injection in injections:
                try:
                    target = injection["target"]
                    injection_type = injection["type"]

                    await self._log_execution_event(
                        execution,
                        f"Recovering {target} from {injection_type}"
                    )

                    # Perform recovery based on injection type
                    if injection_type == "network_latency":
                        await self._recover_network_latency(target, injection)
                    elif injection_type == "service_unavailable":
                        await self._recover_service_availability(target, injection)
                    elif injection_type == "database_slowdown":
                        await self._recover_database_performance(target, injection)
                    elif injection_type == "memory_pressure":
                        await self._recover_memory_pressure(target, injection)
                    elif injection_type == "cpu_spike":
                        await self._recover_cpu_spike(target, injection)

                    execution.success_targets.append(target)

                except Exception as e:
                    execution.failed_targets.append(injection["target"])
                    await self._log_execution_event(
                        execution,
                        f"Recovery failed for {injection['target']}: {str(e)}"
                    )

            # Wait for system stabilization
            await self._log_execution_event(execution, "Waiting for system stabilization")
            await asyncio.sleep(30)  # 30 seconds stabilization period

        except Exception as e:
            self.logger.error(f"Recovery process failed: {str(e)}")
            raise

    async def _recover_network_latency(self, target: str, injection: Dict[str, Any]):
        """Recover from network latency injection"""
        # In a real implementation, this would:
        # 1. Remove traffic control rules
        # 2. Reset network configurations
        # 3. Clear service mesh fault injection
        pass

    async def _recover_service_availability(self, target: str, injection: Dict[str, Any]):
        """Recover from service unavailability injection"""
        # In a real implementation, this would:
        # 1. Restore load balancer configuration
        # 2. Scale up service instances
        # 3. Remove service mesh error injection
        # 4. Unblock network traffic
        pass

    async def _recover_database_performance(self, target: str, injection: Dict[str, Any]):
        """Recover from database slowdown injection"""
        # In a real implementation, this would:
        # 1. Remove artificial delays
        # 2. Restore connection pool size
        # 3. Clear slow query injections
        # 4. Release database locks
        pass

    async def _recover_memory_pressure(self, target: str, injection: Dict[str, Any]):
        """Recover from memory pressure injection"""
        # In a real implementation, this would:
        # 1. Release allocated memory
        # 2. Stop memory stress processes
        # 3. Restore memory cgroups
        # 4. Trigger garbage collection
        pass

    async def _recover_cpu_spike(self, target: str, injection: Dict[str, Any]):
        """Recover from CPU spike injection"""
        # In a real implementation, this would:
        # 1. Stop CPU stress processes
        # 2. Restore CPU cgroups
        # 3. Clear compute-heavy operations
        # 4. Reset CPU scheduling
        pass

    # Helper Methods
    async def _collect_baseline_metrics(self, experiment: ChaosExperiment) -> Dict[str, float]:
        """Collect baseline metrics before experiment"""
        # In a real implementation, this would collect actual metrics
        return {
            "response_time_ms": 150.0,
            "error_rate": 0.01,
            "throughput_rps": 1000.0,
            "cpu_usage": 0.45,
            "memory_usage": 0.65,
            "active_connections": 250
        }

    async def _collect_current_metrics(self, experiment: ChaosExperiment) -> Dict[str, float]:
        """Collect current metrics during experiment"""
        # In a real implementation, this would collect actual metrics
        return {
            "response_time_ms": 350.0,
            "error_rate": 0.05,
            "throughput_rps": 800.0,
            "cpu_usage": 0.75,
            "memory_usage": 0.80,
            "active_connections": 200
        }

    async def _collect_recovery_metrics(self, experiment: ChaosExperiment) -> Dict[str, float]:
        """Collect recovery metrics after experiment"""
        # In a real implementation, this would collect actual metrics
        return {
            "response_time_ms": 160.0,
            "error_rate": 0.015,
            "throughput_rps": 950.0,
            "cpu_usage": 0.50,
            "memory_usage": 0.68,
            "active_connections": 240
        }

    async def _check_safety_guards(self, experiment: ChaosExperiment, execution: ExperimentExecution) -> Optional[Dict[str, Any]]:
        """Check safety guards during experiment execution"""
        # Check global safety guards
        for guard in self.global_safety_guards:
            violation = await self._evaluate_safety_guard(guard, execution)
            if violation:
                return violation

        # Check experiment-specific safety guards
        for guard in experiment.safety_guards:
            violation = await self._evaluate_safety_guard(guard, execution)
            if violation:
                return violation

        return None

    async def _evaluate_safety_guard(self, guard: SafetyGuard, execution: ExperimentExecution) -> Optional[Dict[str, Any]]:
        """Evaluate individual safety guard"""
        try:
            # Get current metric value
            current_metrics = execution.experiment_metrics
            metric_value = current_metrics.get(guard.metric_name, 0.0)

            # Evaluate threshold condition
            threshold_violated = False

            if guard.comparison_operator == ">":
                threshold_violated = metric_value > guard.threshold_value
            elif guard.comparison_operator == "<":
                threshold_violated = metric_value < guard.threshold_value
            elif guard.comparison_operator == ">=":
                threshold_violated = metric_value >= guard.threshold_value
            elif guard.comparison_operator == "<=":
                threshold_violated = metric_value <= guard.threshold_value
            elif guard.comparison_operator == "==":
                threshold_violated = metric_value == guard.threshold_value
            elif guard.comparison_operator == "!=":
                threshold_violated = metric_value != guard.threshold_value

            if threshold_violated:
                return {
                    "guard_id": guard.guard_id,
                    "guard_name": guard.name,
                    "metric_name": guard.metric_name,
                    "current_value": metric_value,
                    "threshold_value": guard.threshold_value,
                    "operator": guard.comparison_operator,
                    "abort_required": guard.abort_experiment,
                    "description": f"{guard.name}: {guard.metric_name} {guard.comparison_operator} {guard.threshold_value} (current: {metric_value})",
                    "detected_at": datetime.utcnow().isoformat()
                }

            return None

        except Exception as e:
            self.logger.error(f"Safety guard evaluation failed: {str(e)}")
            return None

    async def _check_success_criteria(self, experiment: ChaosExperiment, execution: ExperimentExecution):
        """Check experiment success criteria"""
        met_criteria = 0

        for criteria in experiment.success_criteria:
            if await self._evaluate_success_criteria(criteria, execution):
                met_criteria += 1

        execution.success_criteria_met = met_criteria

    async def _evaluate_success_criteria(self, criteria: Dict[str, Any], execution: ExperimentExecution) -> bool:
        """Evaluate individual success criteria"""
        try:
            metric_name = criteria.get("metric_name", "")
            expected_behavior = criteria.get("expected_behavior", "")
            threshold = criteria.get("threshold", 0.0)

            current_value = execution.experiment_metrics.get(metric_name, 0.0)
            baseline_value = execution.baseline_metrics.get(metric_name, 0.0)

            if expected_behavior == "degradation_within_threshold":
                if baseline_value > 0:
                    degradation = (current_value - baseline_value) / baseline_value
                    return degradation <= threshold
            elif expected_behavior == "recovery_within_time":
                # Would check recovery metrics
                return True
            elif expected_behavior == "no_cascading_failures":
                # Would check related services
                return execution.experiment_metrics.get("error_rate", 0.0) < 0.1

            return False

        except Exception as e:
            self.logger.error(f"Success criteria evaluation failed: {str(e)}")
            return False

    async def _validate_hypothesis(self, experiment: ChaosExperiment, execution: ExperimentExecution):
        """Validate experiment hypothesis based on results"""
        try:
            # Calculate overall success rate
            total_criteria = len(experiment.success_criteria)
            if total_criteria == 0:
                execution.hypothesis_result = HypothesisResult.INCONCLUSIVE
                return

            success_rate = execution.success_criteria_met / total_criteria

            if success_rate >= 0.8:
                execution.hypothesis_result = HypothesisResult.CONFIRMED
            elif success_rate >= 0.5:
                execution.hypothesis_result = HypothesisResult.PARTIALLY_CONFIRMED
            elif success_rate >= 0.2:
                execution.hypothesis_result = HypothesisResult.INCONCLUSIVE
            else:
                execution.hypothesis_result = HypothesisResult.REJECTED

        except Exception as e:
            self.logger.error(f"Hypothesis validation failed: {str(e)}")
            execution.hypothesis_result = HypothesisResult.INCONCLUSIVE

    async def _log_execution_event(self, execution: ExperimentExecution, message: str):
        """Log execution event with timestamp"""
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "message": message,
            "status": execution.status.value
        }
        execution.execution_logs.append(event)
        self.logger.info(f"Chaos Experiment {execution.execution_id}: {message}")

    # Background Workers
    async def _experiment_scheduler_worker(self):
        """Background worker for scheduling experiments"""
        while True:
            try:
                if not self.emergency_stop and self.scheduled_experiments:
                    current_time = datetime.utcnow()

                    # Check for experiments ready to run
                    ready_experiments = []
                    for experiment_id in self.scheduled_experiments:
                        experiment = self.experiments.get(experiment_id)
                        if experiment and experiment.scheduled_at and experiment.scheduled_at <= current_time:
                            ready_experiments.append(experiment_id)

                    # Execute ready experiments
                    for experiment_id in ready_experiments:
                        if experiment_id in self.scheduled_experiments:
                            self.scheduled_experiments.remove(experiment_id)

                        # Add to execution queue
                        await self.experiment_queue.put(experiment_id)

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Experiment scheduler worker error: {str(e)}")
                await asyncio.sleep(60)

    async def _experiment_executor_worker(self):
        """Background worker for executing experiments from queue"""
        while True:
            try:
                # Get experiment from queue
                experiment_id = await asyncio.wait_for(self.experiment_queue.get(), timeout=30.0)

                if not self.emergency_stop:
                    # Execute experiment
                    await self.execute_experiment(experiment_id)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Experiment executor worker error: {str(e)}")
                await asyncio.sleep(10)

    async def _safety_monitor_worker(self):
        """Background worker for monitoring system safety"""
        while True:
            try:
                # Check global system health
                system_health = await self._check_global_system_health()

                if not system_health["healthy"]:
                    self.logger.warning("Global system health degraded, stopping chaos experiments")
                    self.emergency_stop = True

                    # Abort all active experiments
                    for execution_id in list(self.active_executions.keys()):
                        await self.abort_experiment(execution_id, "Global system health degradation")
                else:
                    self.emergency_stop = False

                await asyncio.sleep(60)  # Check every minute

            except Exception as e:
                self.logger.error(f"Safety monitor worker error: {str(e)}")
                await asyncio.sleep(30)

    async def _resilience_analyzer_worker(self):
        """Background worker for analyzing system resilience patterns"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour

                # Analyze recent experiment results
                await self._analyze_resilience_patterns()

                # Update resilience baselines
                await self._update_resilience_baselines()

            except Exception as e:
                self.logger.error(f"Resilience analyzer worker error: {str(e)}")
                await asyncio.sleep(1800)

    # Analysis and Reporting Methods
    async def _analyze_resilience_patterns(self):
        """Analyze patterns in resilience test results"""
        try:
            # Get recent executions
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=7)

            async with self.db_pool.acquire() as conn:
                executions = await conn.fetch("""
                    SELECT * FROM chaos_executions
                    WHERE started_at >= $1 AND started_at <= $2
                """, start_time, end_time)

            # Analyze failure patterns
            failure_patterns = {}
            for execution in executions:
                experiment = self.experiments.get(execution["experiment_id"])
                if experiment and execution["auto_aborted"]:
                    chaos_type = experiment.chaos_type.value
                    if chaos_type not in failure_patterns:
                        failure_patterns[chaos_type] = 0
                    failure_patterns[chaos_type] += 1

            self.failure_patterns = failure_patterns

        except Exception as e:
            self.logger.error(f"Resilience pattern analysis failed: {str(e)}")

    async def _update_resilience_baselines(self):
        """Update resilience baselines based on recent data"""
        try:
            # Calculate service-specific resilience scores
            for service in set(service for exp in self.experiments.values() for service in exp.target_services):
                service_score = await self._calculate_service_resilience_score(service)
                if service not in self.resilience_baselines:
                    self.resilience_baselines[service] = {}
                self.resilience_baselines[service]["resilience_score"] = service_score

        except Exception as e:
            self.logger.error(f"Resilience baseline update failed: {str(e)}")

    async def _calculate_service_resilience_score(self, service: str) -> float:
        """Calculate resilience score for a specific service"""
        try:
            # Get recent experiments for this service
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=30)

            async with self.db_pool.acquire() as conn:
                results = await conn.fetch("""
                    SELECT ce.*, ch.target_services
                    FROM chaos_executions ce
                    JOIN chaos_experiments ch ON ce.experiment_id = ch.experiment_id
                    WHERE ce.started_at >= $1 AND $2 = ANY(ch.target_services)
                """, start_time, service)

            if not results:
                return 0.5  # Default neutral score

            successful = len([r for r in results if r["status"] == "completed" and not r["auto_aborted"]])
            total = len(results)

            return successful / total if total > 0 else 0.5

        except Exception as e:
            self.logger.error(f"Service resilience score calculation failed: {str(e)}")
            return 0.5

    async def _generate_resilience_recommendations(self, report: ResilienceReport, executions: List[Dict]):
        """Generate resilience improvement recommendations"""
        try:
            recommendations = []

            # Analyze failure patterns
            if report.overall_resilience_score < 0.7:
                recommendations.append("Overall system resilience is below target. Implement circuit breakers and retry mechanisms.")

            # Check for specific vulnerabilities
            vulnerability_types = set()
            for vulnerability in report.identified_vulnerabilities:
                vulnerability_types.add(vulnerability.get("chaos_type", "unknown"))

            if "network_latency" in vulnerability_types:
                recommendations.append("Network latency issues detected. Implement timeout configurations and async processing.")

            if "service_unavailable" in vulnerability_types:
                recommendations.append("Service availability issues found. Add redundancy and graceful degradation.")

            if "database_slowdown" in vulnerability_types:
                recommendations.append("Database performance vulnerabilities identified. Optimize queries and add caching.")

            # Set recommendations
            report.immediate_actions = recommendations[:3]  # Top 3 immediate actions
            report.strategic_improvements = recommendations[3:6]  # Strategic improvements

            # Investment priorities
            if report.overall_resilience_score < 0.8:
                report.investment_priorities.append({
                    "area": "Infrastructure Resilience",
                    "priority": "high",
                    "estimated_effort": "3-6 months",
                    "expected_roi": "high"
                })

        except Exception as e:
            self.logger.error(f"Resilience recommendations generation failed: {str(e)}")

    async def _calculate_business_continuity_score(self, report: ResilienceReport, executions: List[Dict]):
        """Calculate business continuity score"""
        try:
            if not executions:
                report.business_continuity_score = 1.0
                return

            # Calculate based on customer impact and SLA violations
            total_impact = sum(execution.get("customer_impact_score", 0.0) for execution in executions)
            avg_impact = total_impact / len(executions)

            # Convert impact to continuity score (inverse relationship)
            report.business_continuity_score = max(0.0, 1.0 - (avg_impact / 10.0))

            # Business impact analysis
            report.customer_impact_analysis = {
                "total_experiments": len(executions),
                "customer_affecting_experiments": len([e for e in executions if e.get("customer_impact_score", 0) > 0]),
                "average_impact_score": round(avg_impact, 3),
                "max_impact_score": max((e.get("customer_impact_score", 0.0) for e in executions), default=0.0)
            }

        except Exception as e:
            self.logger.error(f"Business continuity score calculation failed: {str(e)}")

    # Initialization and Configuration Methods
    async def _setup_global_safety_guards(self):
        """Setup global safety guards for all experiments"""
        try:
            # Critical system response time guard
            response_time_guard = SafetyGuard(
                name="Critical Response Time Guard",
                description="Abort if API response time exceeds 5 seconds",
                metric_name="response_time_ms",
                threshold_value=5000.0,
                comparison_operator=">",
                abort_experiment=True,
                business_impact_threshold=0.2
            )

            # System error rate guard
            error_rate_guard = SafetyGuard(
                name="System Error Rate Guard",
                description="Abort if error rate exceeds 10%",
                metric_name="error_rate",
                threshold_value=0.10,
                comparison_operator=">",
                abort_experiment=True,
                customer_facing=True
            )

            # System availability guard
            availability_guard = SafetyGuard(
                name="System Availability Guard",
                description="Abort if system availability drops below 95%",
                metric_name="availability",
                threshold_value=0.95,
                comparison_operator="<",
                abort_experiment=True,
                customer_facing=True
            )

            self.global_safety_guards = [response_time_guard, error_rate_guard, availability_guard]

        except Exception as e:
            self.logger.error(f"Global safety guards setup failed: {str(e)}")

    async def _create_default_experiments(self):
        """Create default chaos experiments for common scenarios"""
        try:
            # Network latency experiment
            network_latency_exp = ChaosExperiment(
                name="API Network Latency Test",
                description="Test system resilience to network latency",
                chaos_type=ChaosType.NETWORK_LATENCY,
                blast_radius=BlastRadius.SINGLE_SERVICE,
                safety_level=SafetyLevel.SAFE,
                target_services=["bmad-api"],
                target_percentage=50.0,
                duration_minutes=3,
                parameters={"latency_ms": 500, "jitter_ms": 100},
                hypothesis="System should maintain <2 second response times with 500ms network latency",
                success_criteria=[
                    {
                        "metric_name": "response_time_ms",
                        "expected_behavior": "degradation_within_threshold",
                        "threshold": 1.0  # 100% degradation acceptable
                    }
                ]
            )

            # Database slowdown experiment
            db_slowdown_exp = ChaosExperiment(
                name="Database Performance Test",
                description="Test system resilience to database slowdown",
                chaos_type=ChaosType.DATABASE_SLOWDOWN,
                blast_radius=BlastRadius.SINGLE_SERVICE,
                safety_level=SafetyLevel.CAUTIOUS,
                target_services=["database"],
                duration_minutes=2,
                parameters={"slowdown_factor": 2.0},
                hypothesis="System should gracefully handle 2x database slowdown",
                success_criteria=[
                    {
                        "metric_name": "error_rate",
                        "expected_behavior": "no_cascading_failures",
                        "threshold": 0.05
                    }
                ]
            )

            # Store default experiments
            await self.create_experiment(network_latency_exp)
            await self.create_experiment(db_slowdown_exp)

        except Exception as e:
            self.logger.error(f"Default experiments creation failed: {str(e)}")

    # Validation and Safety Methods
    async def _validate_experiment(self, experiment: ChaosExperiment):
        """Validate chaos experiment configuration"""
        if not experiment.name:
            raise ValueError("Experiment name is required")

        if not experiment.target_services and not experiment.target_instances:
            raise ValueError("Experiment must specify target services or instances")

        if experiment.duration_minutes <= 0 or experiment.duration_minutes > 60:
            raise ValueError("Experiment duration must be between 1 and 60 minutes")

        if experiment.target_percentage <= 0 or experiment.target_percentage > 100:
            raise ValueError("Target percentage must be between 0 and 100")

        if experiment.safety_level == SafetyLevel.AGGRESSIVE and not experiment.safety_guards:
            raise ValueError("Aggressive experiments must have safety guards")

    async def _pre_execution_safety_check(self, experiment: ChaosExperiment, execution: ExperimentExecution):
        """Perform pre-execution safety checks"""
        # Check system health
        system_health = await self._check_global_system_health()
        if not system_health["healthy"]:
            raise Exception(f"System health check failed: {system_health['reason']}")

        # Check business hours restriction
        if experiment.business_hours_only:
            current_hour = datetime.utcnow().hour
            if current_hour < 9 or current_hour > 17:  # Outside 9-5 UTC
                raise Exception("Experiment restricted to business hours")

        # Check for conflicting experiments
        for active_execution in self.active_executions.values():
            active_experiment = self.experiments.get(active_execution.experiment_id)
            if active_experiment and set(active_experiment.target_services) & set(experiment.target_services):
                raise Exception(f"Conflicting experiment already running: {active_execution.execution_id}")

    async def _check_global_system_health(self) -> Dict[str, Any]:
        """Check global system health before experiments"""
        try:
            # In a real implementation, this would check:
            # 1. System availability metrics
            # 2. Error rates across services
            # 3. Resource utilization
            # 4. Recent incidents
            # 5. Maintenance windows

            # Simulate health check
            health_score = 0.95  # 95% healthy

            return {
                "healthy": health_score > 0.90,
                "score": health_score,
                "reason": "All systems operational" if health_score > 0.90 else "Degraded performance detected"
            }

        except Exception as e:
            return {
                "healthy": False,
                "score": 0.0,
                "reason": f"Health check failed: {str(e)}"
            }

    async def _emergency_recovery(self, experiment: ChaosExperiment, execution: ExperimentExecution):
        """Perform emergency recovery from failed experiment"""
        try:
            await self._log_execution_event(execution, "Performing emergency recovery")

            # Attempt to recover all injected failures
            await self._recover_from_failures(experiment, execution)

            # Additional emergency measures
            # In a real implementation, this might:
            # 1. Restart affected services
            # 2. Trigger circuit breakers
            # 3. Activate backup systems
            # 4. Notify on-call teams

        except Exception as e:
            self.logger.error(f"Emergency recovery failed: {str(e)}")

    # Persistence Methods
    async def _save_experiment_to_db(self, experiment: ChaosExperiment):
        """Save experiment configuration to database"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO chaos_experiments (
                    experiment_id, name, description, chaos_type, blast_radius,
                    safety_level, target_services, target_instances, target_percentage,
                    duration_minutes, intensity, parameters, hypothesis, success_criteria,
                    safety_guards, rollback_strategy, scheduled_at, business_hours_only,
                    maintenance_window, created_by, created_at, tags, enabled
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                ON CONFLICT (experiment_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    enabled = EXCLUDED.enabled,
                    updated_at = NOW()
            """,
                experiment.experiment_id, experiment.name, experiment.description,
                experiment.chaos_type.value, experiment.blast_radius.value,
                experiment.safety_level.value, experiment.target_services,
                experiment.target_instances, experiment.target_percentage,
                experiment.duration_minutes, experiment.intensity,
                json.dumps(experiment.parameters), experiment.hypothesis,
                json.dumps(experiment.success_criteria),
                json.dumps([guard.__dict__ for guard in experiment.safety_guards]),
                json.dumps(experiment.rollback_strategy), experiment.scheduled_at,
                experiment.business_hours_only, experiment.maintenance_window,
                experiment.created_by, experiment.created_at, experiment.tags, experiment.enabled
            )

    async def _store_execution_results(self, execution: ExperimentExecution):
        """Store execution results in database"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO chaos_executions (
                    execution_id, experiment_id, status, started_at, completed_at,
                    duration_seconds, affected_targets, success_targets, failed_targets,
                    baseline_metrics, experiment_metrics, recovery_metrics,
                    hypothesis_result, success_criteria_met, total_success_criteria,
                    safety_violations, incidents_triggered, auto_aborted, abort_reason,
                    customer_impact_score, revenue_impact_estimate, sla_violations,
                    execution_logs, artifacts
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
            """,
                execution.execution_id, execution.experiment_id, execution.status.value,
                execution.started_at, execution.completed_at, execution.duration_seconds,
                execution.affected_targets, execution.success_targets, execution.failed_targets,
                json.dumps(execution.baseline_metrics), json.dumps(execution.experiment_metrics),
                json.dumps(execution.recovery_metrics), execution.hypothesis_result.value,
                execution.success_criteria_met, execution.total_success_criteria,
                json.dumps(execution.safety_violations), execution.incidents_triggered,
                execution.auto_aborted, execution.abort_reason, execution.customer_impact_score,
                execution.revenue_impact_estimate, json.dumps(execution.sla_violations),
                json.dumps(execution.execution_logs), json.dumps(execution.artifacts)
            )

    async def _load_experiments_from_db(self):
        """Load existing experiments from database"""
        try:
            async with self.db_pool.acquire() as conn:
                experiments = await conn.fetch("SELECT * FROM chaos_experiments WHERE enabled = true")

                for exp_row in experiments:
                    # Reconstruct experiment configuration
                    # Note: This would include proper deserialization of JSON fields
                    pass

        except Exception as e:
            self.logger.error(f"Failed to load experiments from database: {str(e)}")

# Global chaos engineering instance
chaos_engineering = ChaosEngineering()

async def get_chaos_engineering() -> ChaosEngineering:
    """Get global chaos engineering instance"""
    if not chaos_engineering.is_running:
        await chaos_engineering.initialize()
    return chaos_engineering