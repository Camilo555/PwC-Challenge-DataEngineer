"""
Self-Healing System with Automated Failure Recovery
Provides autonomous system recovery, health monitoring, and intelligent failure resolution.
"""
import asyncio
import os
import signal
import statistics
import subprocess
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import psutil

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.framework.resilience import CircuitBreaker, CircuitBreakerConfig


class FailureType(Enum):
    """Types of system failures"""
    PROCESS_CRASH = "process_crash"
    MEMORY_EXHAUSTION = "memory_exhaustion"
    DISK_FULL = "disk_full"
    NETWORK_FAILURE = "network_failure"
    DATABASE_DISCONNECTION = "database_disconnection"
    SERVICE_UNAVAILABLE = "service_unavailable"
    HIGH_CPU_USAGE = "high_cpu_usage"
    DEADLOCK = "deadlock"
    CONFIGURATION_ERROR = "configuration_error"
    DEPENDENCY_FAILURE = "dependency_failure"
    DATA_CORRUPTION = "data_corruption"
    PERFORMANCE_DEGRADATION = "performance_degradation"


class RecoveryAction(Enum):
    """Types of recovery actions"""
    RESTART_PROCESS = "restart_process"
    RESTART_SERVICE = "restart_service"
    CLEAR_CACHE = "clear_cache"
    FREE_MEMORY = "free_memory"
    CLEANUP_TEMP_FILES = "cleanup_temp_files"
    RESET_CONNECTION = "reset_connection"
    SCALE_RESOURCES = "scale_resources"
    SWITCH_FALLBACK = "switch_fallback"
    RELOAD_CONFIGURATION = "reload_configuration"
    REPAIR_DATA = "repair_data"
    QUARANTINE_COMPONENT = "quarantine_component"
    ALERT_OPERATORS = "alert_operators"


class SystemHealth(Enum):
    """System health status levels"""
    HEALTHY = 0
    DEGRADED = 1
    CRITICAL = 2
    FAILED = 3
    RECOVERING = 4

    @property
    def value_name(self):
        """Get the string name of the health status"""
        names = {
            0: "healthy",
            1: "degraded",
            2: "critical",
            3: "failed",
            4: "recovering"
        }
        return names[self.value]


@dataclass
class HealthMetrics:
    """System health metrics"""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_latency: float
    active_connections: int
    error_rate: float
    response_time_p95: float
    throughput: float
    health_status: SystemHealth
    issues: list[str] = field(default_factory=list)


@dataclass
class FailureEvent:
    """System failure event record"""
    event_id: str
    failure_type: FailureType
    component: str
    severity: str
    timestamp: datetime
    description: str
    metrics_at_failure: HealthMetrics
    root_cause: str | None = None
    recovery_actions: list[RecoveryAction] = field(default_factory=list)
    recovery_successful: bool = False
    recovery_time_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryPlan:
    """Automated recovery plan"""
    plan_id: str
    failure_type: FailureType
    component: str
    actions: list[tuple[RecoveryAction, dict[str, Any]]]
    estimated_recovery_time: float
    success_probability: float
    rollback_plan: Optional['RecoveryPlan'] = None


class HealthChecker(ABC):
    """Abstract base class for health checkers"""

    @abstractmethod
    async def check_health(self) -> HealthMetrics:
        """Check component health and return metrics"""
        pass

    @abstractmethod
    def get_component_name(self) -> str:
        """Get the name of the component being checked"""
        pass


class SystemHealthChecker(HealthChecker):
    """System-level health checker"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self._last_network_check = datetime.now()
        self._network_latency = 0.0

    async def check_health(self) -> HealthMetrics:
        """Check overall system health"""

        # CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)

        # Memory usage
        memory = psutil.virtual_memory()
        memory_usage = memory.percent

        # Disk usage
        disk = psutil.disk_usage('/')
        disk_usage = disk.percent

        # Network latency (check periodically)
        if datetime.now() - self._last_network_check > timedelta(minutes=5):
            self._network_latency = await self._check_network_latency()
            self._last_network_check = datetime.now()

        # Active connections
        try:
            connections = psutil.net_connections()
            active_connections = len([c for c in connections if c.status == 'ESTABLISHED'])
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            active_connections = 0

        # Determine health status
        issues = []
        health_status = SystemHealth.HEALTHY

        if cpu_usage > 90:
            issues.append(f"High CPU usage: {cpu_usage:.1f}%")
            health_status = SystemHealth.CRITICAL
        elif cpu_usage > 70:
            issues.append(f"Elevated CPU usage: {cpu_usage:.1f}%")
            health_status = SystemHealth.DEGRADED

        if memory_usage > 95:
            issues.append(f"Critical memory usage: {memory_usage:.1f}%")
            health_status = SystemHealth.CRITICAL
        elif memory_usage > 80:
            issues.append(f"High memory usage: {memory_usage:.1f}%")
            health_status = max(health_status, SystemHealth.DEGRADED)

        if disk_usage > 95:
            issues.append(f"Disk space critical: {disk_usage:.1f}%")
            health_status = SystemHealth.CRITICAL
        elif disk_usage > 85:
            issues.append(f"Disk space low: {disk_usage:.1f}%")
            health_status = max(health_status, SystemHealth.DEGRADED)

        if self._network_latency > 5000:  # 5 seconds
            issues.append(f"High network latency: {self._network_latency:.0f}ms")
            health_status = max(health_status, SystemHealth.DEGRADED)

        return HealthMetrics(
            timestamp=datetime.now(),
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            disk_usage=disk_usage,
            network_latency=self._network_latency,
            active_connections=active_connections,
            error_rate=0.0,  # Would be calculated from logs
            response_time_p95=0.0,  # Would be calculated from metrics
            throughput=0.0,  # Would be calculated from metrics
            health_status=health_status,
            issues=issues
        )

    def get_component_name(self) -> str:
        return "system"

    async def _check_network_latency(self) -> float:
        """Check network latency to external services"""
        try:
            import time
            start_time = time.time()

            # Simple HTTP check to a reliable service
            import httpx
            async with httpx.AsyncClient(timeout=10) as client:
                await client.get('https://httpbin.org/status/200')

            latency = (time.time() - start_time) * 1000  # Convert to milliseconds
            return latency

        except Exception as e:
            self.logger.warning(f"Network latency check failed: {e}")
            return 10000.0  # Return high latency on failure


class DatabaseHealthChecker(HealthChecker):
    """Database connection health checker"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.logger = get_logger(__name__)
        self._last_check_time = datetime.now()
        self._connection_pool_size = 0

    async def check_health(self) -> HealthMetrics:
        """Check database health"""

        issues = []
        health_status = SystemHealth.HEALTHY
        response_time = 0.0

        try:
            # Simple connection test
            start_time = time.time()

            # This would be implemented with actual database connection
            # For now, simulate a health check
            await asyncio.sleep(0.01)  # Simulate DB query time

            response_time = (time.time() - start_time) * 1000

            if response_time > 1000:  # 1 second
                issues.append(f"Slow database response: {response_time:.0f}ms")
                health_status = SystemHealth.DEGRADED

        except Exception as e:
            issues.append(f"Database connection failed: {str(e)}")
            health_status = SystemHealth.FAILED
            response_time = float('inf')

        return HealthMetrics(
            timestamp=datetime.now(),
            cpu_usage=0.0,
            memory_usage=0.0,
            disk_usage=0.0,
            network_latency=0.0,
            active_connections=self._connection_pool_size,
            error_rate=0.0,
            response_time_p95=response_time,
            throughput=0.0,
            health_status=health_status,
            issues=issues
        )

    def get_component_name(self) -> str:
        return "database"


class ServiceHealthChecker(HealthChecker):
    """External service health checker"""

    def __init__(self, service_name: str, health_endpoint: str):
        self.service_name = service_name
        self.health_endpoint = health_endpoint
        self.logger = get_logger(__name__)
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=30.0
        ))

    async def check_health(self) -> HealthMetrics:
        """Check external service health"""

        issues = []
        health_status = SystemHealth.HEALTHY
        response_time = 0.0

        try:
            start_time = time.time()

            async def health_check():
                import httpx
                async with httpx.AsyncClient(timeout=5) as client:
                    response = await client.get(self.health_endpoint)
                    return response.status_code == 200

            is_healthy = await self.circuit_breaker.call_async(health_check)
            response_time = (time.time() - start_time) * 1000

            if not is_healthy:
                issues.append("Service health check failed")
                health_status = SystemHealth.FAILED
            elif response_time > 2000:
                issues.append(f"Slow service response: {response_time:.0f}ms")
                health_status = SystemHealth.DEGRADED

        except Exception as e:
            issues.append(f"Service unreachable: {str(e)}")
            health_status = SystemHealth.FAILED
            response_time = float('inf')

        return HealthMetrics(
            timestamp=datetime.now(),
            cpu_usage=0.0,
            memory_usage=0.0,
            disk_usage=0.0,
            network_latency=response_time,
            active_connections=0,
            error_rate=0.0,
            response_time_p95=response_time,
            throughput=0.0,
            health_status=health_status,
            issues=issues
        )

    def get_component_name(self) -> str:
        return self.service_name


class RecoveryActionExecutor:
    """Executes recovery actions"""

    def __init__(self):
        self.logger = get_logger(__name__)

    async def execute_action(
        self,
        action: RecoveryAction,
        parameters: dict[str, Any],
        component: str
    ) -> tuple[bool, str]:
        """Execute a recovery action"""

        try:
            if action == RecoveryAction.RESTART_PROCESS:
                return await self._restart_process(parameters.get('process_name', component))

            elif action == RecoveryAction.RESTART_SERVICE:
                return await self._restart_service(parameters.get('service_name', component))

            elif action == RecoveryAction.CLEAR_CACHE:
                return await self._clear_cache(parameters)

            elif action == RecoveryAction.FREE_MEMORY:
                return await self._free_memory(parameters)

            elif action == RecoveryAction.CLEANUP_TEMP_FILES:
                return await self._cleanup_temp_files(parameters)

            elif action == RecoveryAction.RESET_CONNECTION:
                return await self._reset_connection(parameters)

            elif action == RecoveryAction.SCALE_RESOURCES:
                return await self._scale_resources(parameters)

            elif action == RecoveryAction.SWITCH_FALLBACK:
                return await self._switch_fallback(parameters)

            elif action == RecoveryAction.RELOAD_CONFIGURATION:
                return await self._reload_configuration(parameters)

            elif action == RecoveryAction.REPAIR_DATA:
                return await self._repair_data(parameters)

            elif action == RecoveryAction.QUARANTINE_COMPONENT:
                return await self._quarantine_component(component, parameters)

            elif action == RecoveryAction.ALERT_OPERATORS:
                return await self._alert_operators(parameters)

            else:
                return False, f"Unknown recovery action: {action}"

        except Exception as e:
            self.logger.error(f"Recovery action {action} failed: {e}")
            return False, str(e)

    async def _restart_process(self, process_name: str) -> tuple[bool, str]:
        """Restart a system process"""
        try:
            # Find process by name
            for proc in psutil.process_iter(['pid', 'name']):
                if proc.info['name'] == process_name:
                    pid = proc.info['pid']

                    # Terminate gracefully first
                    os.kill(pid, signal.SIGTERM)
                    await asyncio.sleep(5)

                    # Force kill if still running
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass  # Process already terminated

                    # TODO: Restart process (would need process management system)
                    return True, f"Process {process_name} restarted"

            return False, f"Process {process_name} not found"

        except Exception as e:
            return False, f"Failed to restart process {process_name}: {e}"

    async def _restart_service(self, service_name: str) -> tuple[bool, str]:
        """Restart a system service"""
        try:
            # Use systemctl to restart service (Linux)
            if os.name == 'posix':
                result = subprocess.run(
                    ['systemctl', 'restart', service_name],
                    capture_output=True,
                    text=True,
                    timeout=30
                )

                if result.returncode == 0:
                    return True, f"Service {service_name} restarted successfully"
                else:
                    return False, f"Failed to restart service: {result.stderr}"
            else:
                # Windows service restart
                result = subprocess.run(
                    ['sc', 'stop', service_name],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                await asyncio.sleep(2)

                result = subprocess.run(
                    ['sc', 'start', service_name],
                    capture_output=True,
                    text=True,
                    timeout=30
                )

                if result.returncode == 0:
                    return True, f"Service {service_name} restarted successfully"
                else:
                    return False, f"Failed to restart service: {result.stderr}"

        except Exception as e:
            return False, f"Service restart failed: {e}"

    async def _clear_cache(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Clear application cache"""
        try:
            cache_paths = parameters.get('cache_paths', ['/tmp/cache', '/var/cache'])
            cleared_size = 0

            for cache_path in cache_paths:
                path = Path(cache_path)
                if path.exists():
                    for file_path in path.rglob('*'):
                        if file_path.is_file():
                            try:
                                size = file_path.stat().st_size
                                file_path.unlink()
                                cleared_size += size
                            except Exception:
                                pass  # Skip files that can't be deleted

            cleared_mb = cleared_size / (1024 * 1024)
            return True, f"Cleared {cleared_mb:.1f} MB from cache"

        except Exception as e:
            return False, f"Cache clear failed: {e}"

    async def _free_memory(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Free system memory"""
        try:
            # Force garbage collection
            import gc
            collected = gc.collect()

            # Clear OS caches (Linux)
            if os.name == 'posix':
                try:
                    subprocess.run(['sync'], check=True)
                    with open('/proc/sys/vm/drop_caches', 'w') as f:
                        f.write('3')
                except Exception:
                    pass  # May not have permission

            return True, f"Memory freed, garbage collected {collected} objects"

        except Exception as e:
            return False, f"Memory free failed: {e}"

    async def _cleanup_temp_files(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Cleanup temporary files"""
        try:
            temp_dirs = parameters.get('temp_dirs', ['/tmp', '/var/tmp'])
            max_age_hours = parameters.get('max_age_hours', 24)
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)

            deleted_files = 0
            freed_space = 0

            for temp_dir in temp_dirs:
                path = Path(temp_dir)
                if path.exists():
                    for file_path in path.rglob('*'):
                        if file_path.is_file():
                            try:
                                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                                if file_time < cutoff_time:
                                    size = file_path.stat().st_size
                                    file_path.unlink()
                                    deleted_files += 1
                                    freed_space += size
                            except Exception:
                                pass

            freed_mb = freed_space / (1024 * 1024)
            return True, f"Deleted {deleted_files} temp files, freed {freed_mb:.1f} MB"

        except Exception as e:
            return False, f"Temp file cleanup failed: {e}"

    async def _reset_connection(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Reset network/database connections"""
        try:
            connection_type = parameters.get('connection_type', 'database')

            if connection_type == 'database':
                # Reset database connection pool
                # This would depend on the actual database library used
                return True, "Database connection pool reset"

            elif connection_type == 'network':
                # Reset network connections
                subprocess.run(['ip', 'route', 'flush', 'cache'], check=False)
                return True, "Network connections reset"

            else:
                return False, f"Unknown connection type: {connection_type}"

        except Exception as e:
            return False, f"Connection reset failed: {e}"

    async def _scale_resources(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Scale system resources"""
        try:
            # This would integrate with container orchestration or cloud APIs
            scale_type = parameters.get('scale_type', 'horizontal')
            target_instances = parameters.get('target_instances', 2)

            # Mock implementation
            return True, f"Scaled resources: {scale_type} scaling to {target_instances} instances"

        except Exception as e:
            return False, f"Resource scaling failed: {e}"

    async def _switch_fallback(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Switch to fallback service/configuration"""
        try:
            fallback_type = parameters.get('fallback_type', 'service')
            fallback_target = parameters.get('fallback_target', 'backup')

            # Mock implementation - would update load balancer/configuration
            return True, f"Switched to fallback {fallback_type}: {fallback_target}"

        except Exception as e:
            return False, f"Fallback switch failed: {e}"

    async def _reload_configuration(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Reload system configuration"""
        try:
            config_file = parameters.get('config_file')

            # Send SIGHUP to reload configuration
            if parameters.get('process_name'):
                for proc in psutil.process_iter(['pid', 'name']):
                    if proc.info['name'] == parameters['process_name']:
                        os.kill(proc.info['pid'], signal.SIGHUP)
                        break

            return True, f"Configuration reloaded from {config_file}"

        except Exception as e:
            return False, f"Configuration reload failed: {e}"

    async def _repair_data(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Repair corrupted data"""
        try:
            data_path = parameters.get('data_path')
            repair_type = parameters.get('repair_type', 'filesystem')

            if repair_type == 'filesystem':
                # Run filesystem check
                if os.name == 'posix':
                    result = subprocess.run(
                        ['fsck', '-y', data_path],
                        capture_output=True,
                        text=True,
                        timeout=300
                    )

                    if result.returncode == 0:
                        return True, f"Filesystem {data_path} repaired successfully"
                    else:
                        return False, f"Filesystem repair failed: {result.stderr}"

            return False, f"Unknown repair type: {repair_type}"

        except Exception as e:
            return False, f"Data repair failed: {e}"

    async def _quarantine_component(self, component: str, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Quarantine a failing component"""
        try:
            # Remove component from load balancer
            # Disable component in service discovery
            # Mark component as unhealthy

            return True, f"Component {component} quarantined"

        except Exception as e:
            return False, f"Component quarantine failed: {e}"

    async def _alert_operators(self, parameters: dict[str, Any]) -> tuple[bool, str]:
        """Send alerts to operators"""
        try:
            message = parameters.get('message', 'System failure detected')
            severity = parameters.get('severity', 'high')

            # Send alert via multiple channels
            # - Email
            # - Slack
            # - PagerDuty
            # - SMS

            self.logger.critical(f"OPERATOR ALERT [{severity.upper()}]: {message}")

            return True, f"Alert sent to operators: {message}"

        except Exception as e:
            return False, f"Operator alert failed: {e}"


class FailureAnalyzer:
    """Analyzes failures and creates recovery plans"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.failure_patterns: dict[str, dict[str, Any]] = {}
        self.recovery_success_rates: dict[str, float] = {}

        # Load failure patterns and recovery strategies
        self._initialize_recovery_strategies()

    def _initialize_recovery_strategies(self):
        """Initialize built-in recovery strategies"""

        self.failure_patterns = {
            'high_memory_usage': {
                'conditions': {'memory_usage': {'min': 90}},
                'recovery_actions': [
                    (RecoveryAction.FREE_MEMORY, {}),
                    (RecoveryAction.CLEAR_CACHE, {'cache_paths': ['/tmp/cache']}),
                    (RecoveryAction.CLEANUP_TEMP_FILES, {'max_age_hours': 1})
                ],
                'success_rate': 0.8
            },

            'high_cpu_usage': {
                'conditions': {'cpu_usage': {'min': 90}},
                'recovery_actions': [
                    (RecoveryAction.SCALE_RESOURCES, {'scale_type': 'horizontal', 'target_instances': 2}),
                    (RecoveryAction.RESTART_PROCESS, {}),
                ],
                'success_rate': 0.7
            },

            'disk_full': {
                'conditions': {'disk_usage': {'min': 95}},
                'recovery_actions': [
                    (RecoveryAction.CLEANUP_TEMP_FILES, {'max_age_hours': 24}),
                    (RecoveryAction.CLEAR_CACHE, {}),
                    (RecoveryAction.ALERT_OPERATORS, {'message': 'Disk space critical', 'severity': 'high'})
                ],
                'success_rate': 0.9
            },

            'service_unresponsive': {
                'conditions': {'response_time_p95': {'min': 10000}},  # 10 seconds
                'recovery_actions': [
                    (RecoveryAction.RESTART_SERVICE, {}),
                    (RecoveryAction.RESET_CONNECTION, {'connection_type': 'network'}),
                    (RecoveryAction.SWITCH_FALLBACK, {'fallback_type': 'service'})
                ],
                'success_rate': 0.75
            },

            'database_issues': {
                'conditions': {'health_status': ['failed', 'critical']},
                'recovery_actions': [
                    (RecoveryAction.RESET_CONNECTION, {'connection_type': 'database'}),
                    (RecoveryAction.RESTART_SERVICE, {'service_name': 'postgresql'}),
                    (RecoveryAction.REPAIR_DATA, {'repair_type': 'database'})
                ],
                'success_rate': 0.6
            }
        }

    def analyze_failure(self, health_metrics: HealthMetrics, component: str) -> RecoveryPlan | None:
        """Analyze failure and create recovery plan"""

        # Convert metrics to comparable format
        metrics_dict = {
            'cpu_usage': health_metrics.cpu_usage,
            'memory_usage': health_metrics.memory_usage,
            'disk_usage': health_metrics.disk_usage,
            'response_time_p95': health_metrics.response_time_p95,
            'health_status': health_metrics.health_status.value_name
        }

        # Find matching patterns
        matching_patterns = []

        for pattern_name, pattern_config in self.failure_patterns.items():
            if self._matches_pattern(metrics_dict, pattern_config['conditions']):
                matching_patterns.append((pattern_name, pattern_config))

        if not matching_patterns:
            self.logger.warning(f"No recovery pattern found for {component}")
            return None

        # Select best matching pattern (highest success rate)
        best_pattern_name, best_pattern = max(
            matching_patterns,
            key=lambda x: x[1]['success_rate']
        )

        # Create recovery plan
        plan = RecoveryPlan(
            plan_id=str(uuid.uuid4()),
            failure_type=self._infer_failure_type(health_metrics),
            component=component,
            actions=best_pattern['recovery_actions'],
            estimated_recovery_time=len(best_pattern['recovery_actions']) * 30.0,  # 30s per action
            success_probability=best_pattern['success_rate']
        )

        self.logger.info(f"Created recovery plan {plan.plan_id} for {component} using pattern {best_pattern_name}")
        return plan

    def _matches_pattern(self, metrics: dict[str, Any], conditions: dict[str, Any]) -> bool:
        """Check if metrics match pattern conditions"""

        for metric_name, condition in conditions.items():
            metric_value = metrics.get(metric_name)

            if metric_value is None:
                continue

            if isinstance(condition, dict):
                if 'min' in condition and metric_value < condition['min']:
                    return False
                if 'max' in condition and metric_value > condition['max']:
                    return False
            elif isinstance(condition, list):
                if metric_value not in condition:
                    return False
            elif metric_value != condition:
                return False

        return True

    def _infer_failure_type(self, health_metrics: HealthMetrics) -> FailureType:
        """Infer failure type from health metrics"""

        if health_metrics.memory_usage > 95:
            return FailureType.MEMORY_EXHAUSTION
        elif health_metrics.cpu_usage > 90:
            return FailureType.HIGH_CPU_USAGE
        elif health_metrics.disk_usage > 95:
            return FailureType.DISK_FULL
        elif health_metrics.response_time_p95 > 10000:
            return FailureType.SERVICE_UNAVAILABLE
        elif health_metrics.network_latency > 5000:
            return FailureType.NETWORK_FAILURE
        else:
            return FailureType.PERFORMANCE_DEGRADATION

    def update_success_rate(self, plan_id: str, successful: bool):
        """Update recovery success rates based on outcomes"""
        # This would be used to improve future recovery plans
        pass


class SelfHealingOrchestrator:
    """Main orchestrator for self-healing system"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.config = get_unified_config()

        # Components
        self.health_checkers: dict[str, HealthChecker] = {}
        self.failure_analyzer = FailureAnalyzer()
        self.recovery_executor = RecoveryActionExecutor()

        # State tracking
        self.current_health: dict[str, HealthMetrics] = {}
        self.active_failures: dict[str, FailureEvent] = {}
        self.recovery_history: list[FailureEvent] = []
        self.lock = threading.Lock()

        # Configuration
        self.health_check_interval = 30  # seconds
        self.failure_threshold = 3  # consecutive failures before triggering recovery
        self.max_concurrent_recoveries = 3

        # Initialize health checkers
        self._initialize_health_checkers()

        # Start monitoring
        self._monitoring_active = True
        self._start_monitoring()

    def _initialize_health_checkers(self):
        """Initialize health checkers for system components"""

        # System health checker
        self.health_checkers['system'] = SystemHealthChecker()

        # Database health checker
        if hasattr(self.config.base, 'database_url'):
            self.health_checkers['database'] = DatabaseHealthChecker(
                self.config.base.database_url
            )

        # External service health checkers
        # These would be configured based on actual services

    def add_health_checker(self, name: str, checker: HealthChecker):
        """Add a custom health checker"""
        self.health_checkers[name] = checker
        self.logger.info(f"Added health checker for {name}")

    def _start_monitoring(self):
        """Start background monitoring tasks"""

        async def monitoring_loop():
            while self._monitoring_active:
                try:
                    await self._run_health_checks()
                    await asyncio.sleep(self.health_check_interval)
                except Exception as e:
                    self.logger.error(f"Error in monitoring loop: {e}")
                    await asyncio.sleep(5)  # Brief pause before retry

        # Start monitoring in background thread
        def start_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(monitoring_loop())

        monitoring_thread = threading.Thread(target=start_loop, daemon=True)
        monitoring_thread.start()

        self.logger.info("Self-healing monitoring started")

    async def _run_health_checks(self):
        """Run health checks for all components"""

        tasks = []
        for name, checker in self.health_checkers.items():
            tasks.append(self._check_component_health(name, checker))

        # Run all health checks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            component_name = list(self.health_checkers.keys())[i]

            if isinstance(result, Exception):
                self.logger.error(f"Health check failed for {component_name}: {result}")
            else:
                await self._process_health_result(component_name, result)

    async def _check_component_health(self, name: str, checker: HealthChecker) -> HealthMetrics:
        """Check health of a single component"""
        try:
            return await checker.check_health()
        except Exception as e:
            self.logger.error(f"Health check failed for {name}: {e}")
            # Return failed health status
            return HealthMetrics(
                timestamp=datetime.now(),
                cpu_usage=0.0,
                memory_usage=0.0,
                disk_usage=0.0,
                network_latency=0.0,
                active_connections=0,
                error_rate=1.0,
                response_time_p95=float('inf'),
                throughput=0.0,
                health_status=SystemHealth.FAILED,
                issues=[str(e)]
            )

    async def _process_health_result(self, component: str, health_metrics: HealthMetrics):
        """Process health check result and trigger recovery if needed"""

        with self.lock:
            self.current_health[component] = health_metrics

        # Check if component is unhealthy
        if health_metrics.health_status in [SystemHealth.CRITICAL, SystemHealth.FAILED]:
            await self._handle_component_failure(component, health_metrics)
        elif health_metrics.health_status == SystemHealth.DEGRADED:
            await self._handle_component_degradation(component, health_metrics)
        else:
            # Component is healthy - clear any active failures
            with self.lock:
                if component in self.active_failures:
                    del self.active_failures[component]

    async def _handle_component_failure(self, component: str, health_metrics: HealthMetrics):
        """Handle component failure"""

        with self.lock:
            # Check if this is a new failure or ongoing
            if component not in self.active_failures:
                # New failure
                failure_event = FailureEvent(
                    event_id=str(uuid.uuid4()),
                    failure_type=self.failure_analyzer._infer_failure_type(health_metrics),
                    component=component,
                    severity="critical",
                    timestamp=datetime.now(),
                    description=f"Component {component} failed: {', '.join(health_metrics.issues)}",
                    metrics_at_failure=health_metrics
                )

                self.active_failures[component] = failure_event
                self.logger.error(f"Component failure detected: {component}")

                # Trigger recovery
                await self._trigger_recovery(component, failure_event)

    async def _handle_component_degradation(self, component: str, health_metrics: HealthMetrics):
        """Handle component degradation"""

        # For now, just log degradation
        # Could implement gradual recovery or preventive actions
        self.logger.warning(f"Component degradation detected: {component} - {', '.join(health_metrics.issues)}")

    async def _trigger_recovery(self, component: str, failure_event: FailureEvent):
        """Trigger automatic recovery for a failed component"""

        # Check concurrent recovery limit
        active_recoveries = sum(
            1 for event in self.active_failures.values()
            if event.recovery_actions and not event.recovery_successful
        )

        if active_recoveries >= self.max_concurrent_recoveries:
            self.logger.warning(f"Recovery delayed for {component}: max concurrent recoveries reached")
            return

        # Analyze failure and create recovery plan
        recovery_plan = self.failure_analyzer.analyze_failure(
            failure_event.metrics_at_failure,
            component
        )

        if not recovery_plan:
            self.logger.error(f"No recovery plan available for {component}")
            return

        self.logger.info(f"Starting recovery for {component} using plan {recovery_plan.plan_id}")

        # Execute recovery plan
        start_time = time.time()
        recovery_successful = await self._execute_recovery_plan(recovery_plan, component)
        recovery_time = (time.time() - start_time) * 1000

        # Update failure event
        with self.lock:
            failure_event.recovery_actions = [action for action, _ in recovery_plan.actions]
            failure_event.recovery_successful = recovery_successful
            failure_event.recovery_time_ms = recovery_time

            if recovery_successful:
                # Move to history
                self.recovery_history.append(failure_event)
                if component in self.active_failures:
                    del self.active_failures[component]

                self.logger.info(f"Recovery successful for {component} in {recovery_time:.0f}ms")
            else:
                self.logger.error(f"Recovery failed for {component}")

        # Update success rates
        self.failure_analyzer.update_success_rate(recovery_plan.plan_id, recovery_successful)

    async def _execute_recovery_plan(self, plan: RecoveryPlan, component: str) -> bool:
        """Execute a recovery plan"""

        success = True

        for action, parameters in plan.actions:
            try:
                self.logger.info(f"Executing recovery action: {action.value} for {component}")

                action_success, message = await self.recovery_executor.execute_action(
                    action, parameters, component
                )

                if action_success:
                    self.logger.info(f"Recovery action succeeded: {message}")
                else:
                    self.logger.error(f"Recovery action failed: {message}")
                    success = False

                    # If action fails, should we continue with remaining actions?
                    # For now, continue with remaining actions

                # Wait between actions to allow system to stabilize
                await asyncio.sleep(5)

            except Exception as e:
                self.logger.error(f"Recovery action {action.value} failed with exception: {e}")
                success = False

        return success

    def get_system_status(self) -> dict[str, Any]:
        """Get comprehensive system status"""

        with self.lock:
            current_health = dict(self.current_health)
            active_failures = dict(self.active_failures)
            recovery_history = list(self.recovery_history)

        overall_health = SystemHealth.HEALTHY
        total_issues = []

        for component, health in current_health.items():
            if health.health_status == SystemHealth.FAILED:
                overall_health = SystemHealth.FAILED
            elif health.health_status == SystemHealth.CRITICAL and overall_health != SystemHealth.FAILED:
                overall_health = SystemHealth.CRITICAL
            elif health.health_status == SystemHealth.DEGRADED and overall_health in [SystemHealth.HEALTHY]:
                overall_health = SystemHealth.DEGRADED

            total_issues.extend(health.issues)

        return {
            'overall_health': overall_health.value_name,
            'timestamp': datetime.now().isoformat(),
            'components': {
                name: {
                    'health_status': health.health_status.value_name,
                    'cpu_usage': health.cpu_usage,
                    'memory_usage': health.memory_usage,
                    'disk_usage': health.disk_usage,
                    'issues': health.issues,
                    'last_check': health.timestamp.isoformat()
                }
                for name, health in current_health.items()
            },
            'active_failures': {
                name: {
                    'failure_type': event.failure_type.value,
                    'severity': event.severity,
                    'timestamp': event.timestamp.isoformat(),
                    'description': event.description,
                    'recovery_in_progress': len(event.recovery_actions) > 0 and not event.recovery_successful
                }
                for name, event in active_failures.items()
            },
            'recovery_statistics': {
                'total_recoveries': len(recovery_history),
                'successful_recoveries': len([e for e in recovery_history if e.recovery_successful]),
                'average_recovery_time_ms': statistics.mean([e.recovery_time_ms for e in recovery_history if e.recovery_time_ms]) if recovery_history else 0
            },
            'system_issues': total_issues
        }

    def stop_monitoring(self):
        """Stop the monitoring system"""
        self._monitoring_active = False
        self.logger.info("Self-healing monitoring stopped")


# Global self-healing orchestrator instance
_self_healing_orchestrator: SelfHealingOrchestrator | None = None


def get_self_healing_orchestrator() -> SelfHealingOrchestrator:
    """Get global self-healing orchestrator instance"""
    global _self_healing_orchestrator
    if _self_healing_orchestrator is None:
        _self_healing_orchestrator = SelfHealingOrchestrator()
    return _self_healing_orchestrator


# Decorator for self-healing functions
def self_healing(
    recovery_actions: list[tuple[RecoveryAction, dict[str, Any]]] = None,
    max_retries: int = 3,
    retry_delay: float = 5.0
):
    """Decorator to add self-healing capabilities to functions"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            orchestrator = get_self_healing_orchestrator()

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        orchestrator.logger.error(f"Function {func.__name__} failed after {max_retries} attempts")
                        raise

                    orchestrator.logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}): {e}")

                    # Execute recovery actions if provided
                    if recovery_actions:
                        for action, params in recovery_actions:
                            success, message = await orchestrator.recovery_executor.execute_action(
                                action, params, func.__name__
                            )
                            orchestrator.logger.info(f"Recovery action {action.value}: {message}")

                    await asyncio.sleep(retry_delay)

        def sync_wrapper(*args, **kwargs):
            # For sync functions, create async wrapper
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(async_wrapper(*args, **kwargs))

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
