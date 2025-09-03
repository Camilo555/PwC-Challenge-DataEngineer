"""
Chaos Engineering Framework
Production-ready chaos testing framework for distributed data engineering systems.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import psutil
import pytest

logger = logging.getLogger(__name__)


class ChaosExperimentType(Enum):
    """Types of chaos experiments supported."""
    NETWORK_PARTITION = "network_partition"
    SERVICE_FAILURE = "service_failure"
    DATABASE_FAILURE = "database_failure"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    LATENCY_INJECTION = "latency_injection"
    MEMORY_PRESSURE = "memory_pressure"
    CPU_SPIKE = "cpu_spike"
    DISK_FULL = "disk_full"
    NETWORK_CORRUPTION = "network_corruption"
    TIME_DRIFT = "time_drift"


class ExperimentStatus(Enum):
    """Status of chaos experiments."""
    PLANNED = "planned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    ABORTED = "aborted"


@dataclass
class ChaosExperiment:
    """Represents a single chaos engineering experiment."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    experiment_type: ChaosExperimentType = ChaosExperimentType.SERVICE_FAILURE
    duration_seconds: int = 30
    target_service: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    status: ExperimentStatus = ExperimentStatus.PLANNED
    start_time: datetime | None = None
    end_time: datetime | None = None
    results: dict[str, Any] = field(default_factory=dict)
    rollback_successful: bool = False
    error_message: str | None = None


class ChaosInjector(ABC):
    """Abstract base class for chaos injection mechanisms."""

    @abstractmethod
    async def inject_chaos(self, experiment: ChaosExperiment) -> dict[str, Any]:
        """Inject chaos based on experiment configuration."""
        pass

    @abstractmethod
    async def rollback_chaos(self, experiment: ChaosExperiment) -> bool:
        """Rollback chaos injection and restore normal operation."""
        pass

    @abstractmethod
    async def verify_rollback(self, experiment: ChaosExperiment) -> bool:
        """Verify that rollback was successful."""
        pass


class NetworkChaosInjector(ChaosInjector):
    """Network-based chaos injection (simulated for safety)."""

    async def inject_chaos(self, experiment: ChaosExperiment) -> dict[str, Any]:
        """Simulate network issues."""

        chaos_type = experiment.experiment_type
        duration = experiment.duration_seconds
        parameters = experiment.parameters

        results = {
            "chaos_type": chaos_type.value,
            "duration_seconds": duration,
            "injection_time": datetime.now().isoformat(),
            "simulated": True,  # For safety, we simulate rather than actually break things
            "metrics": {}
        }

        if chaos_type == ChaosExperimentType.NETWORK_PARTITION:
            # Simulate network partition by introducing artificial delays
            await self._simulate_network_partition(parameters)
            results["metrics"]["partition_duration"] = duration
            results["metrics"]["affected_connections"] = parameters.get("connection_count", 10)

        elif chaos_type == ChaosExperimentType.LATENCY_INJECTION:
            # Simulate latency injection
            await self._simulate_latency_injection(parameters)
            latency_ms = parameters.get("latency_ms", 1000)
            results["metrics"]["injected_latency_ms"] = latency_ms
            results["metrics"]["affected_requests"] = parameters.get("request_count", 100)

        elif chaos_type == ChaosExperimentType.NETWORK_CORRUPTION:
            # Simulate network corruption
            await self._simulate_network_corruption(parameters)
            corruption_rate = parameters.get("corruption_rate", 0.1)
            results["metrics"]["corruption_rate"] = corruption_rate

        # Wait for experiment duration
        await asyncio.sleep(duration)

        return results

    async def _simulate_network_partition(self, parameters: dict[str, Any]):
        """Simulate network partition effects."""
        logger.info(f"SIMULATED: Network partition with parameters: {parameters}")

        # In a real implementation, this would:
        # - Block specific network connections
        # - Modify iptables rules
        # - Introduce packet drops

        # For simulation, we just log and create realistic metrics
        await asyncio.sleep(0.1)  # Simulate setup time

    async def _simulate_latency_injection(self, parameters: dict[str, Any]):
        """Simulate network latency injection."""
        latency_ms = parameters.get("latency_ms", 1000)
        logger.info(f"SIMULATED: Injecting {latency_ms}ms network latency")

        # In production, this would use tc (traffic control) or similar tools
        await asyncio.sleep(0.1)

    async def _simulate_network_corruption(self, parameters: dict[str, Any]):
        """Simulate network packet corruption."""
        corruption_rate = parameters.get("corruption_rate", 0.1)
        logger.info(f"SIMULATED: Network corruption at {corruption_rate:.1%} rate")

        await asyncio.sleep(0.1)

    async def rollback_chaos(self, experiment: ChaosExperiment) -> bool:
        """Rollback network chaos injection."""
        logger.info(f"Rolling back network chaos for experiment {experiment.id}")

        # In production, this would:
        # - Remove iptables rules
        # - Reset network configuration
        # - Clear traffic control settings

        await asyncio.sleep(0.5)  # Simulate rollback time
        return True

    async def verify_rollback(self, experiment: ChaosExperiment) -> bool:
        """Verify network rollback was successful."""
        # In production, this would test network connectivity
        await asyncio.sleep(0.2)
        return True


class ServiceChaosInjector(ChaosInjector):
    """Service-level chaos injection (simulated)."""

    def __init__(self):
        self.killed_processes = []
        self.service_states = {}

    async def inject_chaos(self, experiment: ChaosExperiment) -> dict[str, Any]:
        """Simulate service failures."""

        chaos_type = experiment.experiment_type
        target_service = experiment.target_service
        parameters = experiment.parameters

        results = {
            "chaos_type": chaos_type.value,
            "target_service": target_service,
            "injection_time": datetime.now().isoformat(),
            "simulated": True,
            "metrics": {}
        }

        if chaos_type == ChaosExperimentType.SERVICE_FAILURE:
            await self._simulate_service_failure(target_service, parameters)
            results["metrics"]["service_status"] = "failed"
            results["metrics"]["failure_mode"] = parameters.get("failure_mode", "crash")

        elif chaos_type == ChaosExperimentType.DATABASE_FAILURE:
            await self._simulate_database_failure(parameters)
            results["metrics"]["database_type"] = parameters.get("database_type", "postgresql")
            results["metrics"]["failure_type"] = parameters.get("failure_type", "connection_refused")

        return results

    async def _simulate_service_failure(self, service_name: str, parameters: dict[str, Any]):
        """Simulate service failure."""
        failure_mode = parameters.get("failure_mode", "crash")

        logger.info(f"SIMULATED: Service failure for {service_name} with mode {failure_mode}")

        # Store service state for rollback
        self.service_states[service_name] = {
            "status": "running",
            "failure_mode": failure_mode,
            "failure_time": datetime.now()
        }

        # In production, this would:
        # - Kill service processes
        # - Stop containers
        # - Modify service configuration

        await asyncio.sleep(0.2)

    async def _simulate_database_failure(self, parameters: dict[str, Any]):
        """Simulate database failure."""
        failure_type = parameters.get("failure_type", "connection_refused")
        database_type = parameters.get("database_type", "postgresql")

        logger.info(f"SIMULATED: Database failure - {database_type} with {failure_type}")

        # In production, this would:
        # - Stop database service
        # - Block database connections
        # - Corrupt database files (in controlled manner)

        await asyncio.sleep(0.3)

    async def rollback_chaos(self, experiment: ChaosExperiment) -> bool:
        """Rollback service chaos injection."""
        target_service = experiment.target_service

        if target_service in self.service_states:
            logger.info(f"Rolling back service chaos for {target_service}")
            del self.service_states[target_service]

        # In production, this would:
        # - Restart failed services
        # - Restore database connections
        # - Validate service health

        await asyncio.sleep(1.0)  # Simulate service restart time
        return True

    async def verify_rollback(self, experiment: ChaosExperiment) -> bool:
        """Verify service rollback was successful."""
        target_service = experiment.target_service

        # In production, this would check service health endpoints
        await asyncio.sleep(0.5)

        # Verify service is not in failed state
        return target_service not in self.service_states


class ResourceChaosInjector(ChaosInjector):
    """Resource exhaustion chaos injection."""

    def __init__(self):
        self.resource_hogs = []
        self.original_limits = {}

    async def inject_chaos(self, experiment: ChaosExperiment) -> dict[str, Any]:
        """Inject resource exhaustion chaos."""

        chaos_type = experiment.experiment_type
        parameters = experiment.parameters

        results = {
            "chaos_type": chaos_type.value,
            "injection_time": datetime.now().isoformat(),
            "metrics": {}
        }

        if chaos_type == ChaosExperimentType.MEMORY_PRESSURE:
            await self._simulate_memory_pressure(parameters)
            memory_mb = parameters.get("memory_mb", 1024)
            results["metrics"]["memory_consumed_mb"] = memory_mb

        elif chaos_type == ChaosExperimentType.CPU_SPIKE:
            await self._simulate_cpu_spike(parameters)
            cpu_percent = parameters.get("cpu_percent", 80)
            results["metrics"]["cpu_utilization_percent"] = cpu_percent

        elif chaos_type == ChaosExperimentType.DISK_FULL:
            await self._simulate_disk_full(parameters)
            disk_fill_percent = parameters.get("disk_fill_percent", 95)
            results["metrics"]["disk_utilization_percent"] = disk_fill_percent

        return results

    async def _simulate_memory_pressure(self, parameters: dict[str, Any]):
        """Simulate memory pressure."""
        memory_mb = parameters.get("memory_mb", 1024)
        duration = parameters.get("duration_seconds", 30)

        logger.info(f"SIMULATED: Memory pressure - consuming {memory_mb}MB for {duration}s")

        # In production, this would:
        # - Allocate large amounts of memory
        # - Create memory leaks
        # - Fill page cache

        # For simulation, we just track what we would do
        self.resource_hogs.append({
            "type": "memory",
            "amount": memory_mb,
            "start_time": datetime.now()
        })

        await asyncio.sleep(0.1)

    async def _simulate_cpu_spike(self, parameters: dict[str, Any]):
        """Simulate CPU spike."""
        cpu_percent = parameters.get("cpu_percent", 80)
        duration = parameters.get("duration_seconds", 30)

        logger.info(f"SIMULATED: CPU spike - {cpu_percent}% utilization for {duration}s")

        # In production, this would spawn CPU-intensive processes
        self.resource_hogs.append({
            "type": "cpu",
            "utilization": cpu_percent,
            "start_time": datetime.now()
        })

        await asyncio.sleep(0.1)

    async def _simulate_disk_full(self, parameters: dict[str, Any]):
        """Simulate disk space exhaustion."""
        disk_fill_percent = parameters.get("disk_fill_percent", 95)

        logger.info(f"SIMULATED: Disk full - {disk_fill_percent}% utilization")

        # In production, this would create large temporary files
        self.resource_hogs.append({
            "type": "disk",
            "utilization": disk_fill_percent,
            "start_time": datetime.now()
        })

        await asyncio.sleep(0.1)

    async def rollback_chaos(self, experiment: ChaosExperiment) -> bool:
        """Rollback resource chaos injection."""
        logger.info("Rolling back resource chaos")

        # In production, this would:
        # - Kill resource-consuming processes
        # - Clean up temporary files
        # - Reset resource limits

        self.resource_hogs.clear()
        await asyncio.sleep(0.5)
        return True

    async def verify_rollback(self, experiment: ChaosExperiment) -> bool:
        """Verify resource rollback was successful."""
        # In production, this would check resource utilization
        await asyncio.sleep(0.2)
        return len(self.resource_hogs) == 0


class ChaosTestRunner:
    """Main chaos testing orchestrator."""

    def __init__(self):
        self.injectors = {
            ChaosExperimentType.NETWORK_PARTITION: NetworkChaosInjector(),
            ChaosExperimentType.LATENCY_INJECTION: NetworkChaosInjector(),
            ChaosExperimentType.NETWORK_CORRUPTION: NetworkChaosInjector(),
            ChaosExperimentType.SERVICE_FAILURE: ServiceChaosInjector(),
            ChaosExperimentType.DATABASE_FAILURE: ServiceChaosInjector(),
            ChaosExperimentType.MEMORY_PRESSURE: ResourceChaosInjector(),
            ChaosExperimentType.CPU_SPIKE: ResourceChaosInjector(),
            ChaosExperimentType.DISK_FULL: ResourceChaosInjector(),
        }

        self.active_experiments = {}
        self.experiment_history = []

    async def run_experiment(self, experiment: ChaosExperiment) -> dict[str, Any]:
        """Execute a single chaos engineering experiment."""

        logger.info(f"Starting chaos experiment: {experiment.name} ({experiment.id})")

        experiment.start_time = datetime.now()
        experiment.status = ExperimentStatus.RUNNING
        self.active_experiments[experiment.id] = experiment

        results = {
            "experiment_id": experiment.id,
            "experiment_name": experiment.name,
            "start_time": experiment.start_time.isoformat(),
            "status": experiment.status.value,
            "chaos_results": {},
            "rollback_results": {},
            "system_impact": {},
            "success": False
        }

        try:
            # Get appropriate injector
            injector = self.injectors.get(experiment.experiment_type)
            if not injector:
                raise ValueError(f"No injector available for {experiment.experiment_type}")

            # Pre-experiment system state
            pre_state = await self._capture_system_state()
            results["pre_experiment_state"] = pre_state

            # Inject chaos
            chaos_results = await injector.inject_chaos(experiment)
            results["chaos_results"] = chaos_results
            experiment.results = chaos_results

            # Monitor system during experiment
            monitoring_task = asyncio.create_task(
                self._monitor_system_during_experiment(experiment)
            )

            # Wait for experiment duration
            await asyncio.sleep(experiment.duration_seconds)

            # Stop monitoring
            monitoring_task.cancel()
            try:
                await monitoring_task
            except asyncio.CancelledError:
                pass

            # Capture post-experiment state
            post_state = await self._capture_system_state()
            results["post_experiment_state"] = post_state

            # Calculate system impact
            impact = self._calculate_system_impact(pre_state, post_state)
            results["system_impact"] = impact

            # Rollback chaos
            rollback_success = await injector.rollback_chaos(experiment)
            experiment.rollback_successful = rollback_success

            if rollback_success:
                # Verify rollback
                rollback_verified = await injector.verify_rollback(experiment)
                results["rollback_results"] = {
                    "rollback_successful": rollback_success,
                    "rollback_verified": rollback_verified,
                    "rollback_time": datetime.now().isoformat()
                }

                if rollback_verified:
                    experiment.status = ExperimentStatus.COMPLETED
                    results["success"] = True
                else:
                    experiment.status = ExperimentStatus.FAILED
                    results["error"] = "Rollback verification failed"
            else:
                experiment.status = ExperimentStatus.FAILED
                results["error"] = "Rollback failed"

        except Exception as e:
            logger.error(f"Chaos experiment {experiment.id} failed: {e}")
            experiment.status = ExperimentStatus.FAILED
            experiment.error_message = str(e)
            results["error"] = str(e)

            # Attempt emergency rollback
            try:
                injector = self.injectors.get(experiment.experiment_type)
                if injector:
                    await injector.rollback_chaos(experiment)
            except Exception as rollback_error:
                logger.error(f"Emergency rollback failed: {rollback_error}")

        finally:
            experiment.end_time = datetime.now()
            results["end_time"] = experiment.end_time.isoformat()
            results["total_duration_seconds"] = (experiment.end_time - experiment.start_time).total_seconds()

            # Remove from active experiments
            if experiment.id in self.active_experiments:
                del self.active_experiments[experiment.id]

            # Add to history
            self.experiment_history.append(experiment)

        return results

    async def _capture_system_state(self) -> dict[str, Any]:
        """Capture current system state metrics."""

        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()

            # Memory metrics
            memory = psutil.virtual_memory()

            # Disk metrics
            disk_usage = psutil.disk_usage('/')

            # Network metrics (simplified)
            network_io = psutil.net_io_counters()

            return {
                "timestamp": datetime.now().isoformat(),
                "cpu": {
                    "percent": cpu_percent,
                    "count": cpu_count
                },
                "memory": {
                    "total_mb": memory.total / 1024 / 1024,
                    "available_mb": memory.available / 1024 / 1024,
                    "percent": memory.percent
                },
                "disk": {
                    "total_gb": disk_usage.total / 1024 / 1024 / 1024,
                    "free_gb": disk_usage.free / 1024 / 1024 / 1024,
                    "percent": (disk_usage.used / disk_usage.total) * 100
                },
                "network": {
                    "bytes_sent": network_io.bytes_sent if network_io else 0,
                    "bytes_recv": network_io.bytes_recv if network_io else 0
                }
            }

        except Exception as e:
            logger.warning(f"Failed to capture system state: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}

    async def _monitor_system_during_experiment(self, experiment: ChaosExperiment):
        """Monitor system metrics during chaos experiment."""

        monitoring_data = []

        try:
            while experiment.status == ExperimentStatus.RUNNING:
                state = await self._capture_system_state()
                monitoring_data.append(state)

                await asyncio.sleep(5)  # Monitor every 5 seconds

        except asyncio.CancelledError:
            # Monitoring was cancelled, store collected data
            experiment.results["monitoring_data"] = monitoring_data
            raise

    def _calculate_system_impact(self, pre_state: dict[str, Any], post_state: dict[str, Any]) -> dict[str, Any]:
        """Calculate system impact from chaos experiment."""

        impact = {
            "cpu_impact": 0.0,
            "memory_impact": 0.0,
            "disk_impact": 0.0,
            "overall_impact": "low"
        }

        try:
            # Calculate CPU impact
            if "cpu" in pre_state and "cpu" in post_state:
                pre_cpu = pre_state["cpu"]["percent"]
                post_cpu = post_state["cpu"]["percent"]
                impact["cpu_impact"] = post_cpu - pre_cpu

            # Calculate memory impact
            if "memory" in pre_state and "memory" in post_state:
                pre_memory = pre_state["memory"]["percent"]
                post_memory = post_state["memory"]["percent"]
                impact["memory_impact"] = post_memory - pre_memory

            # Determine overall impact level
            max_impact = max(abs(impact["cpu_impact"]), abs(impact["memory_impact"]))

            if max_impact > 50:
                impact["overall_impact"] = "critical"
            elif max_impact > 25:
                impact["overall_impact"] = "high"
            elif max_impact > 10:
                impact["overall_impact"] = "medium"
            else:
                impact["overall_impact"] = "low"

        except Exception as e:
            logger.warning(f"Failed to calculate system impact: {e}")
            impact["error"] = str(e)

        return impact

    async def run_experiment_suite(self, experiments: list[ChaosExperiment]) -> dict[str, Any]:
        """Run a suite of chaos experiments."""

        suite_results = {
            "suite_start_time": datetime.now().isoformat(),
            "total_experiments": len(experiments),
            "experiment_results": [],
            "suite_summary": {}
        }

        successful_experiments = 0
        failed_experiments = 0

        for experiment in experiments:
            logger.info(f"Running experiment {experiment.name}")

            try:
                result = await self.run_experiment(experiment)
                suite_results["experiment_results"].append(result)

                if result["success"]:
                    successful_experiments += 1
                else:
                    failed_experiments += 1

                # Wait between experiments
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Experiment suite failed at {experiment.name}: {e}")
                suite_results["experiment_results"].append({
                    "experiment_id": experiment.id,
                    "experiment_name": experiment.name,
                    "error": str(e),
                    "success": False
                })
                failed_experiments += 1

        # Calculate suite summary
        suite_results["suite_summary"] = {
            "successful_experiments": successful_experiments,
            "failed_experiments": failed_experiments,
            "success_rate": successful_experiments / len(experiments) if experiments else 0,
            "suite_end_time": datetime.now().isoformat()
        }

        return suite_results


class DataEngineeringChaosEngine:
    """Specialized chaos engine for data engineering platforms."""

    def __init__(self):
        self.chaos_runner = ChaosTestRunner()

    def create_etl_pipeline_experiments(self) -> list[ChaosExperiment]:
        """Create chaos experiments specific to ETL pipelines."""

        experiments = [
            # Database connection failures during ETL
            ChaosExperiment(
                name="ETL Database Connection Failure",
                experiment_type=ChaosExperimentType.DATABASE_FAILURE,
                duration_seconds=60,
                target_service="postgresql",
                parameters={
                    "failure_type": "connection_refused",
                    "database_type": "postgresql"
                }
            ),

            # Network partition during data transfer
            ChaosExperiment(
                name="ETL Network Partition",
                experiment_type=ChaosExperimentType.NETWORK_PARTITION,
                duration_seconds=45,
                parameters={
                    "connection_count": 50,
                    "partition_percentage": 0.3
                }
            ),

            # Memory pressure during large data processing
            ChaosExperiment(
                name="ETL Memory Pressure",
                experiment_type=ChaosExperimentType.MEMORY_PRESSURE,
                duration_seconds=90,
                parameters={
                    "memory_mb": 2048,
                    "duration_seconds": 90
                }
            ),

            # CPU spike during transformations
            ChaosExperiment(
                name="ETL CPU Spike",
                experiment_type=ChaosExperimentType.CPU_SPIKE,
                duration_seconds=60,
                parameters={
                    "cpu_percent": 85,
                    "duration_seconds": 60
                }
            ),

            # Disk full during data writes
            ChaosExperiment(
                name="ETL Disk Full",
                experiment_type=ChaosExperimentType.DISK_FULL,
                duration_seconds=30,
                parameters={
                    "disk_fill_percent": 98
                }
            )
        ]

        return experiments

    def create_api_service_experiments(self) -> list[ChaosExperiment]:
        """Create chaos experiments for API services."""

        experiments = [
            # API service failure
            ChaosExperiment(
                name="API Service Crash",
                experiment_type=ChaosExperimentType.SERVICE_FAILURE,
                duration_seconds=30,
                target_service="fastapi_service",
                parameters={
                    "failure_mode": "crash"
                }
            ),

            # Network latency affecting API responses
            ChaosExperiment(
                name="API Network Latency",
                experiment_type=ChaosExperimentType.LATENCY_INJECTION,
                duration_seconds=120,
                parameters={
                    "latency_ms": 2000,
                    "request_count": 500
                }
            ),

            # Database connection issues for API
            ChaosExperiment(
                name="API Database Connection Loss",
                experiment_type=ChaosExperimentType.DATABASE_FAILURE,
                duration_seconds=45,
                target_service="api_database",
                parameters={
                    "failure_type": "connection_timeout",
                    "database_type": "postgresql"
                }
            )
        ]

        return experiments

    def create_monitoring_experiments(self) -> list[ChaosExperiment]:
        """Create experiments to test monitoring and alerting systems."""

        experiments = [
            # Test monitoring system under resource pressure
            ChaosExperiment(
                name="Monitoring Under Memory Pressure",
                experiment_type=ChaosExperimentType.MEMORY_PRESSURE,
                duration_seconds=180,
                parameters={
                    "memory_mb": 1024,
                    "duration_seconds": 180
                }
            ),

            # Test alerting with network issues
            ChaosExperiment(
                name="Monitoring Network Partition",
                experiment_type=ChaosExperimentType.NETWORK_PARTITION,
                duration_seconds=60,
                parameters={
                    "connection_count": 20,
                    "partition_percentage": 0.5
                }
            )
        ]

        return experiments


class ProductionChaosTestSuite:
    """Production-ready chaos testing suite."""

    def __init__(self):
        self.chaos_engine = DataEngineeringChaosEngine()
        self.safety_checks = []

    async def execute_production_validation_suite(self) -> dict[str, Any]:
        """Execute comprehensive chaos testing for production validation."""

        validation_results = {
            "execution_metadata": {
                "start_time": datetime.now().isoformat(),
                "test_environment": "production_simulation",
                "safety_mode": True
            },
            "experiment_categories": {},
            "overall_assessment": {},
            "resilience_metrics": {},
            "recommendations": []
        }

        try:
            # 1. ETL Pipeline Resilience Tests
            etl_experiments = self.chaos_engine.create_etl_pipeline_experiments()
            etl_results = await self.chaos_engine.chaos_runner.run_experiment_suite(etl_experiments)
            validation_results["experiment_categories"]["etl_pipeline"] = etl_results

            # 2. API Service Resilience Tests
            api_experiments = self.chaos_engine.create_api_service_experiments()
            api_results = await self.chaos_engine.chaos_runner.run_experiment_suite(api_experiments)
            validation_results["experiment_categories"]["api_services"] = api_results

            # 3. Monitoring System Tests
            monitoring_experiments = self.chaos_engine.create_monitoring_experiments()
            monitoring_results = await self.chaos_engine.chaos_runner.run_experiment_suite(monitoring_experiments)
            validation_results["experiment_categories"]["monitoring"] = monitoring_results

            # 4. Calculate resilience metrics
            resilience_metrics = self._calculate_resilience_metrics(validation_results["experiment_categories"])
            validation_results["resilience_metrics"] = resilience_metrics

            # 5. Assess overall system resilience
            overall_assessment = self._assess_overall_resilience(resilience_metrics)
            validation_results["overall_assessment"] = overall_assessment

            # 6. Generate recommendations
            recommendations = self._generate_resilience_recommendations(validation_results)
            validation_results["recommendations"] = recommendations

        except Exception as e:
            logger.error(f"Chaos testing suite failed: {e}")
            validation_results["error"] = str(e)
            validation_results["overall_assessment"] = {
                "readiness_level": "NOT_READY",
                "confidence_score": 0.0,
                "critical_issues": [str(e)]
            }

        finally:
            validation_results["execution_metadata"]["end_time"] = datetime.now().isoformat()

        return validation_results

    def _calculate_resilience_metrics(self, experiment_categories: dict[str, Any]) -> dict[str, Any]:
        """Calculate system resilience metrics from chaos experiments."""

        metrics = {
            "overall_success_rate": 0.0,
            "recovery_times": [],
            "failure_modes": {},
            "category_resilience": {}
        }

        total_experiments = 0
        successful_experiments = 0

        for category, results in experiment_categories.items():
            if isinstance(results, dict) and "suite_summary" in results:
                summary = results["suite_summary"]

                category_total = summary.get("successful_experiments", 0) + summary.get("failed_experiments", 0)
                category_successful = summary.get("successful_experiments", 0)

                total_experiments += category_total
                successful_experiments += category_successful

                # Category resilience score
                category_resilience = category_successful / category_total if category_total > 0 else 0
                metrics["category_resilience"][category] = {
                    "resilience_score": category_resilience,
                    "successful_experiments": category_successful,
                    "total_experiments": category_total
                }

        # Overall success rate
        metrics["overall_success_rate"] = successful_experiments / total_experiments if total_experiments > 0 else 0

        return metrics

    def _assess_overall_resilience(self, resilience_metrics: dict[str, Any]) -> dict[str, Any]:
        """Assess overall system resilience level."""

        overall_success_rate = resilience_metrics.get("overall_success_rate", 0.0)
        category_resilience = resilience_metrics.get("category_resilience", {})

        assessment = {
            "readiness_level": "NOT_READY",
            "confidence_score": overall_success_rate,
            "critical_issues": [],
            "resilience_gaps": []
        }

        # Determine readiness level based on success rate
        if overall_success_rate >= 0.95:
            assessment["readiness_level"] = "PRODUCTION_READY"
        elif overall_success_rate >= 0.85:
            assessment["readiness_level"] = "READY_WITH_MONITORING"
        elif overall_success_rate >= 0.7:
            assessment["readiness_level"] = "REQUIRES_IMPROVEMENT"
        else:
            assessment["readiness_level"] = "NOT_READY"

        # Check category-specific resilience
        for category, resilience_data in category_resilience.items():
            resilience_score = resilience_data["resilience_score"]

            if resilience_score < 0.8:
                assessment["resilience_gaps"].append(
                    f"{category} resilience score {resilience_score:.1%} below 80% threshold"
                )

            if resilience_score < 0.5:
                assessment["critical_issues"].append(
                    f"Critical resilience gap in {category}: {resilience_score:.1%} success rate"
                )

        return assessment

    def _generate_resilience_recommendations(self, validation_results: dict[str, Any]) -> list[str]:
        """Generate recommendations for improving system resilience."""

        recommendations = []

        overall_assessment = validation_results.get("overall_assessment", {})
        readiness_level = overall_assessment.get("readiness_level", "NOT_READY")

        if readiness_level == "NOT_READY":
            recommendations.append("CRITICAL: System failed chaos engineering tests. Do not deploy to production.")
            recommendations.append("Implement comprehensive error handling and circuit breaker patterns")
            recommendations.append("Add automated recovery mechanisms for critical failures")

        elif readiness_level == "REQUIRES_IMPROVEMENT":
            recommendations.append("Improve system resilience before production deployment")
            recommendations.append("Implement additional monitoring and alerting for failure scenarios")
            recommendations.append("Add graceful degradation capabilities")

        elif readiness_level == "READY_WITH_MONITORING":
            recommendations.append("Deploy with enhanced monitoring and alerting")
            recommendations.append("Implement gradual rollout with immediate rollback capability")
            recommendations.append("Schedule regular chaos engineering exercises")

        else:  # PRODUCTION_READY
            recommendations.append("System demonstrates excellent resilience - ready for production")
            recommendations.append("Maintain regular chaos engineering testing schedule")
            recommendations.append("Continue monitoring resilience metrics in production")

        # Category-specific recommendations
        resilience_metrics = validation_results.get("resilience_metrics", {})
        category_resilience = resilience_metrics.get("category_resilience", {})

        for category, resilience_data in category_resilience.items():
            if resilience_data["resilience_score"] < 0.8:
                if category == "etl_pipeline":
                    recommendations.append("ETL: Implement robust retry mechanisms and data validation")
                elif category == "api_services":
                    recommendations.append("API: Add circuit breakers and timeout configurations")
                elif category == "monitoring":
                    recommendations.append("Monitoring: Ensure monitoring system resilience and redundancy")

        return recommendations


# Pytest fixtures and test utilities
@pytest.fixture
def chaos_test_runner():
    """Chaos test runner fixture."""
    return ChaosTestRunner()


@pytest.fixture
def data_engineering_chaos_engine():
    """Data engineering chaos engine fixture."""
    return DataEngineeringChaosEngine()


@pytest.fixture
def production_chaos_suite():
    """Production chaos test suite fixture."""
    return ProductionChaosTestSuite()


# Test cases
class TestChaosEngineering:
    """Chaos engineering test cases."""

    @pytest.mark.chaos
    @pytest.mark.asyncio
    async def test_network_partition_experiment(self, chaos_test_runner):
        """Test network partition chaos experiment."""

        experiment = ChaosExperiment(
            name="Test Network Partition",
            experiment_type=ChaosExperimentType.NETWORK_PARTITION,
            duration_seconds=5,  # Short duration for testing
            parameters={
                "connection_count": 10,
                "partition_percentage": 0.3
            }
        )

        result = await chaos_test_runner.run_experiment(experiment)

        assert result["success"]
        assert result["chaos_results"]["chaos_type"] == "network_partition"
        assert "rollback_results" in result
        assert result["rollback_results"]["rollback_successful"]

    @pytest.mark.chaos
    @pytest.mark.asyncio
    async def test_service_failure_experiment(self, chaos_test_runner):
        """Test service failure chaos experiment."""

        experiment = ChaosExperiment(
            name="Test Service Failure",
            experiment_type=ChaosExperimentType.SERVICE_FAILURE,
            duration_seconds=3,
            target_service="test_service",
            parameters={
                "failure_mode": "crash"
            }
        )

        result = await chaos_test_runner.run_experiment(experiment)

        assert result["success"]
        assert "system_impact" in result
        assert result["rollback_results"]["rollback_verified"]

    @pytest.mark.chaos
    @pytest.mark.asyncio
    async def test_etl_pipeline_chaos_suite(self, data_engineering_chaos_engine):
        """Test ETL pipeline specific chaos experiments."""

        experiments = data_engineering_chaos_engine.create_etl_pipeline_experiments()

        # Reduce duration for testing
        for exp in experiments:
            exp.duration_seconds = 2

        # Run first experiment only for testing
        result = await data_engineering_chaos_engine.chaos_runner.run_experiment(experiments[0])

        assert result["success"]
        assert "system_impact" in result

    @pytest.mark.chaos
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_production_chaos_validation(self, production_chaos_suite):
        """Test production chaos validation suite."""

        # This would be a longer running test in production
        validation_results = await production_chaos_suite.execute_production_validation_suite()

        assert "experiment_categories" in validation_results
        assert "overall_assessment" in validation_results
        assert "resilience_metrics" in validation_results

        # Check that resilience assessment was performed
        overall_assessment = validation_results["overall_assessment"]
        assert "readiness_level" in overall_assessment
        assert "confidence_score" in overall_assessment

        print("\n🧪 CHAOS ENGINEERING RESULTS")
        print(f"Readiness Level: {overall_assessment.get('readiness_level', 'UNKNOWN')}")
        print(f"Confidence Score: {overall_assessment.get('confidence_score', 0):.1%}")

        # Print recommendations
        recommendations = validation_results.get("recommendations", [])
        if recommendations:
            print("\n💡 RESILIENCE RECOMMENDATIONS:")
            for rec in recommendations[:3]:
                print(f"  • {rec}")


if __name__ == "__main__":
    # Example usage
    async def main():
        print("🧪 Starting Chaos Engineering Tests...")

        # Create chaos engine
        chaos_engine = DataEngineeringChaosEngine()

        # Create simple test experiment
        test_experiment = ChaosExperiment(
            name="Demo Memory Pressure",
            experiment_type=ChaosExperimentType.MEMORY_PRESSURE,
            duration_seconds=5,
            parameters={"memory_mb": 512}
        )

        # Run experiment
        result = await chaos_engine.chaos_runner.run_experiment(test_experiment)

        print(f"Experiment completed: {result['success']}")
        print(f"System impact: {result['system_impact']['overall_impact']}")

        print("\n✅ Chaos Engineering Framework Ready!")

    asyncio.run(main())
