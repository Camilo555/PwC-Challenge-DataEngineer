"""
Advanced Health Checks and Circuit Breakers System
Provides comprehensive system health monitoring, dependency checks, and resilience patterns
"""

import asyncio
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
import psutil

from core.config import settings
from core.logging import get_logger
from core.structured_logging import LogContext, PerformanceEvent, StructuredLogger, log_context

logger = get_logger(__name__)
structured_logger = StructuredLogger.get_logger(__name__)


class HealthStatus(Enum):
    """Health check status levels"""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class CircuitBreakerState(Enum):
    """Circuit breaker states"""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


class DependencyType(Enum):
    """Types of system dependencies"""

    DATABASE = "database"
    CACHE = "cache"
    EXTERNAL_API = "external_api"
    MESSAGE_QUEUE = "message_queue"
    FILE_SYSTEM = "file_system"
    NETWORK = "network"
    SERVICE = "service"
    INFRASTRUCTURE = "infrastructure"


@dataclass
class HealthCheckResult:
    """Result of a health check"""

    name: str
    status: HealthStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    dependency_type: DependencyType | None = None
    critical: bool = False
    remediation_suggestions: list[str] = field(default_factory=list)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""

    failure_threshold: int = 5
    recovery_timeout: int = 60  # seconds
    success_threshold: int = 2  # successes needed to close circuit
    timeout: float = 10.0  # request timeout
    monitor_failures: bool = True
    exponential_backoff: bool = True
    max_backoff: int = 300  # max backoff in seconds


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics"""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    state_changes: int = 0
    last_failure_time: datetime | None = None
    last_success_time: datetime | None = None
    current_failure_streak: int = 0
    total_time_open: float = 0.0  # seconds


class BaseHealthCheck(ABC):
    """Base class for all health checks"""

    def __init__(
        self,
        name: str,
        critical: bool = False,
        timeout: float = 10.0,
        dependency_type: DependencyType = DependencyType.SERVICE,
    ):
        self.name = name
        self.critical = critical
        self.timeout = timeout
        self.dependency_type = dependency_type
        self.logger = get_logger(f"{__name__}.{name}")

    @abstractmethod
    async def check(self) -> HealthCheckResult:
        """Perform the health check"""
        pass

    async def execute_with_timeout(self) -> HealthCheckResult:
        """Execute health check with timeout"""
        start_time = time.time()

        try:
            result = await asyncio.wait_for(self.check(), timeout=self.timeout)
            result.duration_ms = (time.time() - start_time) * 1000
            return result

        except asyncio.TimeoutError:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check timed out after {self.timeout}s",
                duration_ms=duration_ms,
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check if service is responsive",
                    "Increase timeout if appropriate",
                    "Investigate service performance",
                ],
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.CRITICAL,
                message=f"Health check failed: {str(e)}",
                duration_ms=duration_ms,
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check service configuration",
                    "Verify network connectivity",
                    "Review service logs",
                ],
            )


class DatabaseHealthCheck(BaseHealthCheck):
    """Database connectivity health check"""

    def __init__(self, name: str = "database", critical: bool = True):
        super().__init__(name, critical, dependency_type=DependencyType.DATABASE)

    async def check(self) -> HealthCheckResult:
        """Check database connectivity and performance"""
        try:
            from data_access.db import get_database_manager

            db_manager = get_database_manager()
            start_time = time.time()

            # Test basic connectivity
            async with db_manager.get_session() as session:
                # Simple query to test connectivity
                await session.execute("SELECT 1")

            query_time = (time.time() - start_time) * 1000

            # Get additional database metrics
            details = {
                "query_time_ms": round(query_time, 2),
                "connection_pool_size": getattr(db_manager.engine.pool, "size", "unknown"),
                "database_type": settings.database_type.value,
            }

            # Determine status based on query performance
            if query_time < 100:
                status = HealthStatus.HEALTHY
                message = "Database is responsive"
            elif query_time < 500:
                status = HealthStatus.DEGRADED
                message = "Database response is slow"
            else:
                status = HealthStatus.UNHEALTHY
                message = "Database response is very slow"

            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                details=details,
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check database server resources",
                    "Optimize slow queries",
                    "Review connection pool settings",
                ]
                if status != HealthStatus.HEALTHY
                else [],
            )

        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.CRITICAL,
                message=f"Database connectivity failed: {str(e)}",
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check database server status",
                    "Verify database credentials",
                    "Check network connectivity",
                ],
            )


class RedisHealthCheck(BaseHealthCheck):
    """Redis cache health check"""

    def __init__(self, name: str = "redis_cache", critical: bool = False):
        super().__init__(name, critical, dependency_type=DependencyType.CACHE)

    async def check(self) -> HealthCheckResult:
        """Check Redis connectivity and performance"""
        try:
            import redis.asyncio as redis

            redis_client = redis.Redis.from_url(
                getattr(settings, "redis_url", "redis://localhost:6379/0"), decode_responses=True
            )

            start_time = time.time()

            # Test basic connectivity with PING
            await redis_client.ping()

            # Test set/get operations
            test_key = f"health_check_{int(time.time())}"
            await redis_client.set(test_key, "test_value", ex=60)  # Expire in 60 seconds
            value = await redis_client.get(test_key)
            await redis_client.delete(test_key)

            operation_time = (time.time() - start_time) * 1000

            # Get Redis info
            info = await redis_client.info()

            details = {
                "operation_time_ms": round(operation_time, 2),
                "redis_version": info.get("redis_version", "unknown"),
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": info.get("used_memory_human", "unknown"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
            }

            # Calculate hit rate
            hits = info.get("keyspace_hits", 0)
            misses = info.get("keyspace_misses", 0)
            if hits + misses > 0:
                hit_rate = hits / (hits + misses)
                details["hit_rate"] = round(hit_rate, 3)

            await redis_client.close()

            # Determine status
            if operation_time < 50 and value == "test_value":
                status = HealthStatus.HEALTHY
                message = "Redis cache is responsive"
            elif operation_time < 200:
                status = HealthStatus.DEGRADED
                message = "Redis cache response is slow"
            else:
                status = HealthStatus.UNHEALTHY
                message = "Redis cache is unresponsive"

            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                details=details,
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check Redis server resources",
                    "Review Redis configuration",
                    "Monitor memory usage",
                ]
                if status != HealthStatus.HEALTHY
                else [],
            )

        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Redis connectivity failed: {str(e)}",
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check Redis server status",
                    "Verify Redis configuration",
                    "Check network connectivity",
                ],
            )


class FileSystemHealthCheck(BaseHealthCheck):
    """File system health check"""

    def __init__(self, name: str = "file_system", paths: list[Path] = None, critical: bool = True):
        super().__init__(name, critical, dependency_type=DependencyType.FILE_SYSTEM)
        self.paths = paths or [
            settings.bronze_path,
            settings.silver_path,
            settings.gold_path,
            Path("./logs"),
        ]

    async def check(self) -> HealthCheckResult:
        """Check file system accessibility and disk space"""
        try:
            details = {}
            issues = []

            # Check disk space for each path
            for path in self.paths:
                try:
                    if not path.exists():
                        path.mkdir(parents=True, exist_ok=True)

                    # Get disk usage
                    usage = psutil.disk_usage(str(path))
                    free_gb = usage.free / (1024**3)
                    total_gb = usage.total / (1024**3)
                    used_percent = (usage.used / usage.total) * 100

                    path_info = {
                        "free_gb": round(free_gb, 2),
                        "total_gb": round(total_gb, 2),
                        "used_percent": round(used_percent, 2),
                    }

                    # Test write permissions
                    test_file = path / f"health_check_{int(time.time())}.tmp"
                    try:
                        test_file.write_text("test")
                        test_file.unlink()
                        path_info["writable"] = True
                    except Exception:
                        path_info["writable"] = False
                        issues.append(f"Path {path} is not writable")

                    details[str(path)] = path_info

                    # Check disk space thresholds
                    if used_percent > 90:
                        issues.append(
                            f"Disk usage for {path} is critically high: {used_percent:.1f}%"
                        )
                    elif used_percent > 80:
                        issues.append(f"Disk usage for {path} is high: {used_percent:.1f}%")

                    if free_gb < 1.0:
                        issues.append(f"Low free space for {path}: {free_gb:.1f} GB")

                except Exception as e:
                    issues.append(f"Failed to check path {path}: {str(e)}")

            # Determine overall status
            if not issues:
                status = HealthStatus.HEALTHY
                message = "All file system paths are healthy"
            elif any("critically high" in issue or "not writable" in issue for issue in issues):
                status = HealthStatus.CRITICAL
                message = f"Critical file system issues: {len(issues)} problems"
            elif len(issues) > 2:
                status = HealthStatus.UNHEALTHY
                message = f"Multiple file system issues: {len(issues)} problems"
            else:
                status = HealthStatus.DEGRADED
                message = f"Minor file system issues: {len(issues)} problems"

            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                details={"paths": details, "issues": issues},
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Free up disk space",
                    "Check file permissions",
                    "Monitor disk usage trends",
                ]
                if issues
                else [],
            )

        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.CRITICAL,
                message=f"File system check failed: {str(e)}",
                dependency_type=self.dependency_type,
                critical=self.critical,
            )


class ExternalAPIHealthCheck(BaseHealthCheck):
    """External API health check"""

    def __init__(
        self,
        name: str,
        url: str,
        expected_status: int = 200,
        critical: bool = False,
        headers: dict[str, str] = None,
    ):
        super().__init__(name, critical, dependency_type=DependencyType.EXTERNAL_API)
        self.url = url
        self.expected_status = expected_status
        self.headers = headers or {}

    async def check(self) -> HealthCheckResult:
        """Check external API availability and response time"""
        try:
            start_time = time.time()

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(self.url, headers=self.headers)

            response_time = (time.time() - start_time) * 1000

            details = {
                "url": self.url,
                "status_code": response.status_code,
                "response_time_ms": round(response_time, 2),
                "expected_status": self.expected_status,
            }

            # Check status and response time
            if response.status_code == self.expected_status:
                if response_time < 1000:  # < 1 second
                    status = HealthStatus.HEALTHY
                    message = f"API is responsive ({response_time:.0f}ms)"
                elif response_time < 5000:  # < 5 seconds
                    status = HealthStatus.DEGRADED
                    message = f"API is slow ({response_time:.0f}ms)"
                else:
                    status = HealthStatus.UNHEALTHY
                    message = f"API is very slow ({response_time:.0f}ms)"
            else:
                status = HealthStatus.UNHEALTHY
                message = f"API returned unexpected status: {response.status_code}"

            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                details=details,
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check API service status",
                    "Verify API credentials",
                    "Check network connectivity",
                ]
                if status != HealthStatus.HEALTHY
                else [],
            )

        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.CRITICAL,
                message=f"API check failed: {str(e)}",
                details={"url": self.url, "error": str(e)},
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Check API endpoint availability",
                    "Verify network connectivity",
                    "Check API service status",
                ],
            )


class SystemResourcesHealthCheck(BaseHealthCheck):
    """System resources health check"""

    def __init__(self, name: str = "system_resources", critical: bool = True):
        super().__init__(name, critical, dependency_type=DependencyType.INFRASTRUCTURE)

    async def check(self) -> HealthCheckResult:
        """Check system CPU, memory, and disk usage"""
        try:
            # Get CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)

            # Get memory usage
            memory = psutil.virtual_memory()

            # Get disk usage for root filesystem
            disk = psutil.disk_usage("/")

            # Get load average (Unix-like systems)
            load_avg = None
            try:
                load_avg = psutil.getloadavg()
            except AttributeError:
                pass  # Windows doesn't have load average

            details = {
                "cpu_percent": round(cpu_percent, 1),
                "memory_percent": round(memory.percent, 1),
                "memory_available_gb": round(memory.available / (1024**3), 2),
                "memory_total_gb": round(memory.total / (1024**3), 2),
                "disk_percent": round((disk.used / disk.total) * 100, 1),
                "disk_free_gb": round(disk.free / (1024**3), 2),
                "disk_total_gb": round(disk.total / (1024**3), 2),
            }

            if load_avg:
                details["load_average"] = [round(x, 2) for x in load_avg]

            # Assess health based on resource usage
            issues = []

            if cpu_percent > 90:
                issues.append(f"CPU usage is critically high: {cpu_percent}%")
            elif cpu_percent > 80:
                issues.append(f"CPU usage is high: {cpu_percent}%")

            if memory.percent > 90:
                issues.append(f"Memory usage is critically high: {memory.percent}%")
            elif memory.percent > 80:
                issues.append(f"Memory usage is high: {memory.percent}%")

            disk_percent = (disk.used / disk.total) * 100
            if disk_percent > 90:
                issues.append(f"Disk usage is critically high: {disk_percent:.1f}%")
            elif disk_percent > 85:
                issues.append(f"Disk usage is high: {disk_percent:.1f}%")

            if load_avg and load_avg[0] > psutil.cpu_count() * 2:
                issues.append(f"System load is very high: {load_avg[0]}")

            # Determine status
            if not issues:
                status = HealthStatus.HEALTHY
                message = "System resources are healthy"
            elif any("critically high" in issue or "very high" in issue for issue in issues):
                status = HealthStatus.CRITICAL
                message = f"Critical resource issues: {len(issues)} problems"
            elif len(issues) > 1:
                status = HealthStatus.UNHEALTHY
                message = f"Multiple resource issues: {len(issues)} problems"
            else:
                status = HealthStatus.DEGRADED
                message = f"Resource usage is elevated: {issues[0]}"

            details["issues"] = issues

            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                details=details,
                dependency_type=self.dependency_type,
                critical=self.critical,
                remediation_suggestions=[
                    "Monitor resource usage trends",
                    "Identify resource-intensive processes",
                    "Consider scaling resources",
                ]
                if issues
                else [],
            )

        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.CRITICAL,
                message=f"System resources check failed: {str(e)}",
                dependency_type=self.dependency_type,
                critical=self.critical,
            )


class CircuitBreaker:
    """Advanced circuit breaker implementation with exponential backoff"""

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.stats = CircuitBreakerStats()
        self.last_state_change = datetime.utcnow()
        self.consecutive_successes = 0
        self.backoff_multiplier = 1
        self.logger = get_logger(f"{__name__}.circuit_breaker.{name}")

    def _should_trip(self) -> bool:
        """Check if circuit breaker should trip"""
        return (
            self.stats.current_failure_streak >= self.config.failure_threshold
            and self.state == CircuitBreakerState.CLOSED
        )

    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt reset"""
        if self.state != CircuitBreakerState.OPEN:
            return False

        time_since_open = (datetime.utcnow() - self.last_state_change).total_seconds()

        if self.config.exponential_backoff:
            required_wait = min(
                self.config.recovery_timeout * self.backoff_multiplier, self.config.max_backoff
            )
        else:
            required_wait = self.config.recovery_timeout

        return time_since_open >= required_wait

    def _transition_to_state(self, new_state: CircuitBreakerState):
        """Transition to new state"""
        old_state = self.state
        self.state = new_state
        self.last_state_change = datetime.utcnow()
        self.stats.state_changes += 1

        if new_state == CircuitBreakerState.OPEN:
            self.backoff_multiplier = min(self.backoff_multiplier * 2, 8)  # Cap at 8x
        elif new_state == CircuitBreakerState.CLOSED:
            self.backoff_multiplier = 1  # Reset backoff
            self.consecutive_successes = 0

        self.logger.info(
            f"Circuit breaker '{self.name}' transitioned from {old_state.value} to {new_state.value}"
        )

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        # Check if we should allow the call
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_state(CircuitBreakerState.HALF_OPEN)
            else:
                raise CircuitBreakerOpenException(f"Circuit breaker '{self.name}' is OPEN")

        self.stats.total_requests += 1
        time.time()

        try:
            # Execute the function with timeout
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.config.timeout)
            else:
                # Run sync function in thread pool
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    result = await loop.run_in_executor(executor, func, *args, **kwargs)

            # Success case
            self.stats.successful_requests += 1
            self.stats.last_success_time = datetime.utcnow()
            self.stats.current_failure_streak = 0
            self.consecutive_successes += 1

            # If we're in half-open state and have enough successes, close the circuit
            if (
                self.state == CircuitBreakerState.HALF_OPEN
                and self.consecutive_successes >= self.config.success_threshold
            ):
                self._transition_to_state(CircuitBreakerState.CLOSED)

            return result

        except Exception:
            # Failure case
            self.stats.failed_requests += 1
            self.stats.last_failure_time = datetime.utcnow()
            self.stats.current_failure_streak += 1
            self.consecutive_successes = 0

            # If in half-open state, go back to open
            if self.state == CircuitBreakerState.HALF_OPEN:
                self._transition_to_state(CircuitBreakerState.OPEN)
            # If in closed state and should trip, open the circuit
            elif self._should_trip():
                self._transition_to_state(CircuitBreakerState.OPEN)

            raise

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics"""
        total_requests = self.stats.total_requests
        success_rate = (
            (self.stats.successful_requests / total_requests) if total_requests > 0 else 0
        )

        return {
            "name": self.name,
            "state": self.state.value,
            "stats": {
                "total_requests": total_requests,
                "successful_requests": self.stats.successful_requests,
                "failed_requests": self.stats.failed_requests,
                "success_rate": round(success_rate, 3),
                "current_failure_streak": self.stats.current_failure_streak,
                "state_changes": self.stats.state_changes,
            },
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
                "success_threshold": self.config.success_threshold,
                "timeout": self.config.timeout,
            },
            "timing": {
                "last_state_change": self.last_state_change.isoformat(),
                "last_success": self.stats.last_success_time.isoformat()
                if self.stats.last_success_time
                else None,
                "last_failure": self.stats.last_failure_time.isoformat()
                if self.stats.last_failure_time
                else None,
            },
        }


class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open"""

    pass


class HealthCheckManager:
    """
    Enhanced health check manager with SLA monitoring, automated recovery,
    and comprehensive system observability.
    """

    def __init__(self):
        self.health_checks: dict[str, BaseHealthCheck] = {}
        self.circuit_breakers: dict[str, CircuitBreaker] = {}
        self.last_check_results: dict[str, HealthCheckResult] = {}
        self.check_history: defaultdict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.logger = get_logger(__name__)

        # Enhanced monitoring features
        self.sla_targets: dict[str, dict[str, Any]] = {}
        self.sla_violations: list[dict[str, Any]] = []
        self.recovery_actions: dict[str, list[Callable]] = {}
        self.monitoring_active = False
        self.monitoring_task: asyncio.Task | None = None

        # Performance tracking
        self.check_metrics: defaultdict = defaultdict(
            lambda: {
                "total_runs": 0,
                "total_duration": 0.0,
                "success_count": 0,
                "failure_count": 0,
                "average_duration": 0.0,
                "success_rate": 0.0,
            }
        )

        # Alerting integration
        self.alerting_enabled = True
        self.alert_cooldowns: dict[str, datetime] = {}

        # Initialize default health checks and SLA targets
        self._initialize_default_checks()
        self._initialize_circuit_breakers()
        self._initialize_sla_targets()
        self._initialize_recovery_actions()

    def _initialize_default_checks(self):
        """Initialize default health checks"""
        # Database check
        self.add_health_check(DatabaseHealthCheck())

        # Redis check (if configured)
        if hasattr(settings, "redis_url"):
            self.add_health_check(RedisHealthCheck())

        # File system check
        self.add_health_check(FileSystemHealthCheck())

        # System resources check
        self.add_health_check(SystemResourcesHealthCheck())

        # External API checks (if configured)
        if hasattr(settings, "currency_api_key") and settings.currency_api_key:
            self.add_health_check(
                ExternalAPIHealthCheck(
                    name="currency_api",
                    url="https://api.exchangerate-api.com/v4/latest/USD",
                    critical=False,
                )
            )

    def _initialize_circuit_breakers(self):
        """Initialize circuit breakers for critical operations"""
        # Database operations circuit breaker
        db_config = CircuitBreakerConfig(
            failure_threshold=3, recovery_timeout=30, success_threshold=2, timeout=10.0
        )
        self.add_circuit_breaker("database_operations", db_config)

        # External API circuit breaker
        api_config = CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=60,
            success_threshold=3,
            timeout=15.0,
            exponential_backoff=True,
        )
        self.add_circuit_breaker("external_api", api_config)

        # ETL processing circuit breaker
        etl_config = CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=120,
            success_threshold=1,
            timeout=300.0,  # 5 minutes for ETL operations
        )
        self.add_circuit_breaker("etl_processing", etl_config)

    def _initialize_sla_targets(self):
        """Initialize SLA targets for health checks."""
        self.sla_targets = {
            "database": {
                "availability": 99.9,  # 99.9% uptime
                "response_time_p95": 100.0,  # 95th percentile < 100ms
                "error_rate": 0.1,  # < 0.1% error rate
                "evaluation_window": 3600,  # 1 hour
                "critical": True,
            },
            "redis_cache": {
                "availability": 99.5,
                "response_time_p95": 50.0,
                "error_rate": 0.5,
                "evaluation_window": 3600,
                "critical": False,
            },
            "file_system": {
                "availability": 99.95,
                "response_time_p95": 200.0,
                "error_rate": 0.05,
                "evaluation_window": 3600,
                "critical": True,
            },
            "system_resources": {
                "availability": 99.9,
                "response_time_p95": 500.0,
                "error_rate": 0.1,
                "evaluation_window": 3600,
                "critical": True,
            },
            "currency_api": {
                "availability": 99.0,
                "response_time_p95": 2000.0,
                "error_rate": 1.0,
                "evaluation_window": 3600,
                "critical": False,
            },
        }

    def _initialize_recovery_actions(self):
        """Initialize automated recovery actions for failing health checks."""
        self.recovery_actions = {
            "database": [
                self._restart_database_connections,
                self._clear_database_cache,
                self._optimize_database_queries,
            ],
            "redis_cache": [
                self._flush_redis_cache,
                self._restart_redis_connection,
                self._cleanup_redis_keys,
            ],
            "file_system": [
                self._cleanup_temp_files,
                self._free_disk_space,
                self._check_file_permissions,
            ],
            "system_resources": [
                self._cleanup_system_resources,
                self._restart_heavy_processes,
                self._optimize_system_performance,
            ],
        }

    async def start_monitoring(self):
        """Start continuous health monitoring with SLA tracking."""
        if self.monitoring_active:
            self.logger.warning("Health monitoring is already active")
            return

        self.monitoring_active = True
        self.monitoring_task = asyncio.create_task(self._continuous_monitoring_loop())
        self.logger.info("Started continuous health monitoring with SLA tracking")

    async def stop_monitoring(self):
        """Stop continuous health monitoring."""
        self.monitoring_active = False

        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            self.monitoring_task = None

        self.logger.info("Stopped continuous health monitoring")

    async def _continuous_monitoring_loop(self):
        """Continuous monitoring loop with automated recovery."""
        while self.monitoring_active:
            try:
                # Run all health checks
                results = await self.run_all_health_checks()

                # Check SLA compliance
                for check_name, result in results.items():
                    await self._check_sla_compliance(check_name, result)
                    await self._update_check_metrics(check_name, result)

                # Trigger recovery actions for failing checks
                await self._trigger_recovery_actions(results)

                # Clean up old violations and metrics
                self._cleanup_old_data()

                # Wait before next check cycle (30 seconds)
                await asyncio.sleep(30)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in continuous monitoring loop: {str(e)}")
                await asyncio.sleep(30)

    async def _check_sla_compliance(self, check_name: str, result: HealthCheckResult):
        """Check SLA compliance for a health check result."""
        if check_name not in self.sla_targets:
            return

        sla = self.sla_targets[check_name]
        current_time = datetime.utcnow()
        evaluation_window = timedelta(seconds=sla["evaluation_window"])

        # Get historical results for SLA evaluation
        history = list(self.check_history[check_name])
        recent_results = [r for r in history if r.timestamp >= current_time - evaluation_window]

        if len(recent_results) < 3:  # Need minimum data points
            return

        # Calculate SLA metrics
        total_checks = len(recent_results)
        successful_checks = len([r for r in recent_results if r.status == HealthStatus.HEALTHY])
        availability = (successful_checks / total_checks) * 100

        response_times = [r.duration_ms for r in recent_results]
        response_times.sort()
        p95_response_time = response_times[int(len(response_times) * 0.95)] if response_times else 0

        error_rate = ((total_checks - successful_checks) / total_checks) * 100

        # Check for SLA violations
        violations = []

        if availability < sla["availability"]:
            violation = {
                "check_name": check_name,
                "violation_type": "availability",
                "target": sla["availability"],
                "actual": availability,
                "severity": "critical" if sla["critical"] else "warning",
                "timestamp": current_time,
                "window_hours": sla["evaluation_window"] / 3600,
            }
            violations.append(violation)

        if p95_response_time > sla["response_time_p95"]:
            violation = {
                "check_name": check_name,
                "violation_type": "response_time",
                "target": sla["response_time_p95"],
                "actual": p95_response_time,
                "severity": "warning",
                "timestamp": current_time,
                "window_hours": sla["evaluation_window"] / 3600,
            }
            violations.append(violation)

        if error_rate > sla["error_rate"]:
            violation = {
                "check_name": check_name,
                "violation_type": "error_rate",
                "target": sla["error_rate"],
                "actual": error_rate,
                "severity": "critical" if sla["critical"] else "warning",
                "timestamp": current_time,
                "window_hours": sla["evaluation_window"] / 3600,
            }
            violations.append(violation)

        # Record violations and trigger alerts
        for violation in violations:
            self.sla_violations.append(violation)
            await self._handle_sla_violation(violation)

            # Log structured violation
            structured_logger.error(
                f"SLA violation: {violation['violation_type']} for {check_name}",
                extra={
                    "category": "sla_violation",
                    "check_name": check_name,
                    "violation_type": violation["violation_type"],
                    "target": violation["target"],
                    "actual": violation["actual"],
                    "severity": violation["severity"],
                },
            )

    async def _update_check_metrics(self, check_name: str, result: HealthCheckResult):
        """Update performance metrics for a health check."""
        metrics = self.check_metrics[check_name]

        metrics["total_runs"] += 1
        metrics["total_duration"] += result.duration_ms

        if result.status == HealthStatus.HEALTHY:
            metrics["success_count"] += 1
        else:
            metrics["failure_count"] += 1

        metrics["average_duration"] = metrics["total_duration"] / metrics["total_runs"]
        metrics["success_rate"] = (metrics["success_count"] / metrics["total_runs"]) * 100

    async def _trigger_recovery_actions(self, results: dict[str, HealthCheckResult]):
        """Trigger automated recovery actions for failing health checks."""
        for check_name, result in results.items():
            if (
                result.status in [HealthStatus.CRITICAL, HealthStatus.UNHEALTHY]
                and check_name in self.recovery_actions
            ):
                # Check if we should trigger recovery (avoid too frequent attempts)
                last_recovery = self.alert_cooldowns.get(f"recovery_{check_name}")
                if (
                    last_recovery and (datetime.utcnow() - last_recovery).total_seconds() < 300
                ):  # 5 min cooldown
                    continue

                self.logger.info(f"Triggering recovery actions for failing check: {check_name}")

                # Execute recovery actions
                for action in self.recovery_actions[check_name]:
                    try:
                        await action(check_name, result)
                        self.logger.info(
                            f"Executed recovery action {action.__name__} for {check_name}"
                        )
                    except Exception as e:
                        self.logger.error(
                            f"Recovery action {action.__name__} failed for {check_name}: {str(e)}"
                        )

                # Set cooldown to prevent rapid recovery attempts
                self.alert_cooldowns[f"recovery_{check_name}"] = datetime.utcnow()

    async def _handle_sla_violation(self, violation: dict[str, Any]):
        """Handle SLA violation with appropriate alerting and escalation."""
        if not self.alerting_enabled:
            return

        # Check alert cooldown to prevent spam
        cooldown_key = f"sla_{violation['check_name']}_{violation['violation_type']}"
        last_alert = self.alert_cooldowns.get(cooldown_key)

        # Cooldown period based on severity
        cooldown_minutes = 15 if violation["severity"] == "critical" else 30

        if last_alert and (datetime.utcnow() - last_alert).total_seconds() < cooldown_minutes * 60:
            return

        # Create alert message
        alert_message = (
            f"SLA VIOLATION: {violation['violation_type']} for {violation['check_name']} - "
            f"Target: {violation['target']}, Actual: {violation['actual']:.2f}"
        )

        # Log alert
        if violation["severity"] == "critical":
            self.logger.critical(alert_message)
        else:
            self.logger.warning(alert_message)

        # Set cooldown
        self.alert_cooldowns[cooldown_key] = datetime.utcnow()

    def _cleanup_old_data(self):
        """Clean up old SLA violations and metrics."""
        # Remove violations older than 24 hours
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        self.sla_violations = [v for v in self.sla_violations if v["timestamp"] >= cutoff_time]

        # Clean up old alert cooldowns
        expired_cooldowns = [
            key
            for key, timestamp in self.alert_cooldowns.items()
            if (datetime.utcnow() - timestamp).total_seconds() > 3600  # 1 hour
        ]
        for key in expired_cooldowns:
            del self.alert_cooldowns[key]

    # Recovery action implementations
    async def _restart_database_connections(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Restart database connection pool."""
        try:
            from data_access.db import get_database_manager

            get_database_manager()

            # This would typically restart the connection pool
            self.logger.info("Simulating database connection pool restart")
            await asyncio.sleep(0.1)  # Simulate restart time

        except Exception as e:
            self.logger.error(f"Failed to restart database connections: {str(e)}")

    async def _clear_database_cache(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Clear database query cache."""
        try:
            self.logger.info("Simulating database cache clear")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to clear database cache: {str(e)}")

    async def _optimize_database_queries(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Optimize slow database queries."""
        try:
            self.logger.info("Simulating database query optimization")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to optimize database queries: {str(e)}")

    async def _flush_redis_cache(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Flush Redis cache."""
        try:
            self.logger.info("Simulating Redis cache flush")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to flush Redis cache: {str(e)}")

    async def _restart_redis_connection(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Restart Redis connection."""
        try:
            self.logger.info("Simulating Redis connection restart")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to restart Redis connection: {str(e)}")

    async def _cleanup_redis_keys(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Clean up expired Redis keys."""
        try:
            self.logger.info("Simulating Redis key cleanup")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to cleanup Redis keys: {str(e)}")

    async def _cleanup_temp_files(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Clean up temporary files."""
        try:
            self.logger.info("Simulating temporary file cleanup")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to cleanup temporary files: {str(e)}")

    async def _free_disk_space(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Free up disk space."""
        try:
            self.logger.info("Simulating disk space cleanup")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to free disk space: {str(e)}")

    async def _check_file_permissions(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Check and fix file permissions."""
        try:
            self.logger.info("Simulating file permission check")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to check file permissions: {str(e)}")

    async def _cleanup_system_resources(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Clean up system resources."""
        try:
            self.logger.info("Simulating system resource cleanup")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to cleanup system resources: {str(e)}")

    async def _restart_heavy_processes(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Restart resource-intensive processes."""
        try:
            self.logger.info("Simulating heavy process restart")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to restart heavy processes: {str(e)}")

    async def _optimize_system_performance(self, check_name: str, result: HealthCheckResult):
        """Recovery action: Optimize system performance."""
        try:
            self.logger.info("Simulating system performance optimization")
            await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Failed to optimize system performance: {str(e)}")

    def get_sla_compliance_report(self, hours: int = 24) -> dict[str, Any]:
        """Generate SLA compliance report."""
        try:
            current_time = datetime.utcnow()
            cutoff_time = current_time - timedelta(hours=hours)

            # Filter recent violations
            recent_violations = [v for v in self.sla_violations if v["timestamp"] >= cutoff_time]

            # Calculate compliance for each check
            compliance_data = {}

            for check_name, sla in self.sla_targets.items():
                history = list(self.check_history[check_name])
                recent_results = [r for r in history if r.timestamp >= cutoff_time]

                if not recent_results:
                    compliance_data[check_name] = {
                        "status": "no_data",
                        "availability": 0.0,
                        "avg_response_time": 0.0,
                        "error_rate": 0.0,
                        "sla_met": False,
                    }
                    continue

                # Calculate metrics
                total_checks = len(recent_results)
                successful_checks = len(
                    [r for r in recent_results if r.status == HealthStatus.HEALTHY]
                )
                availability = (successful_checks / total_checks) * 100

                avg_response_time = sum(r.duration_ms for r in recent_results) / total_checks
                error_rate = ((total_checks - successful_checks) / total_checks) * 100

                # Determine SLA compliance
                sla_met = (
                    availability >= sla["availability"]
                    and avg_response_time <= sla["response_time_p95"]
                    and error_rate <= sla["error_rate"]
                )

                compliance_data[check_name] = {
                    "status": "compliant" if sla_met else "violated",
                    "availability": round(availability, 2),
                    "avg_response_time": round(avg_response_time, 2),
                    "error_rate": round(error_rate, 2),
                    "sla_met": sla_met,
                    "target_availability": sla["availability"],
                    "target_response_time": sla["response_time_p95"],
                    "target_error_rate": sla["error_rate"],
                    "total_checks": total_checks,
                    "critical": sla["critical"],
                }

            # Calculate overall compliance
            total_slas = len(compliance_data)
            met_slas = len(
                [data for data in compliance_data.values() if data.get("sla_met", False)]
            )
            overall_compliance = (met_slas / total_slas * 100) if total_slas > 0 else 0

            return {
                "report_timestamp": current_time.isoformat(),
                "evaluation_period_hours": hours,
                "overall_compliance_percentage": round(overall_compliance, 2),
                "total_slas": total_slas,
                "compliant_slas": met_slas,
                "violated_slas": total_slas - met_slas,
                "recent_violations": len(recent_violations),
                "compliance_by_check": compliance_data,
                "violation_summary": self._summarize_violations(recent_violations),
                "performance_metrics": dict(self.check_metrics),
            }

        except Exception as e:
            self.logger.error(f"Failed to generate SLA compliance report: {str(e)}")
            return {"error": str(e), "report_timestamp": datetime.utcnow().isoformat()}

    def _summarize_violations(self, violations: list[dict[str, Any]]) -> dict[str, Any]:
        """Summarize SLA violations by type and severity."""
        if not violations:
            return {"total": 0, "by_type": {}, "by_severity": {}, "by_check": {}}

        by_type = {}
        by_severity = {}
        by_check = {}

        for violation in violations:
            # By type
            vtype = violation["violation_type"]
            by_type[vtype] = by_type.get(vtype, 0) + 1

            # By severity
            severity = violation["severity"]
            by_severity[severity] = by_severity.get(severity, 0) + 1

            # By check
            check = violation["check_name"]
            by_check[check] = by_check.get(check, 0) + 1

        return {
            "total": len(violations),
            "by_type": by_type,
            "by_severity": by_severity,
            "by_check": by_check,
        }

    def add_health_check(self, health_check: BaseHealthCheck):
        """Add a health check"""
        self.health_checks[health_check.name] = health_check
        self.logger.info(f"Added health check: {health_check.name}")

    def add_circuit_breaker(self, name: str, config: CircuitBreakerConfig):
        """Add a circuit breaker"""
        self.circuit_breakers[name] = CircuitBreaker(name, config)
        self.logger.info(f"Added circuit breaker: {name}")

    def get_circuit_breaker(self, name: str) -> CircuitBreaker | None:
        """Get circuit breaker by name"""
        return self.circuit_breakers.get(name)

    async def run_health_check(self, check_name: str) -> HealthCheckResult:
        """Run a specific health check"""
        if check_name not in self.health_checks:
            return HealthCheckResult(
                name=check_name,
                status=HealthStatus.UNKNOWN,
                message=f"Health check '{check_name}' not found",
            )

        health_check = self.health_checks[check_name]

        # Create context for structured logging
        context = LogContext(operation=f"health_check_{check_name}", component="health_monitoring")

        with log_context(context):
            start_time = time.time()

            try:
                result = await health_check.execute_with_timeout()

                # Log performance
                perf_event = PerformanceEvent(
                    operation=f"health_check_{check_name}",
                    duration_ms=result.duration_ms,
                    component="health_monitoring",
                    metadata={
                        "status": result.status.value,
                        "critical": result.critical,
                        "dependency_type": result.dependency_type.value
                        if result.dependency_type
                        else None,
                    },
                )
                structured_logger.log_performance(perf_event)

                # Store result and history
                self.last_check_results[check_name] = result
                self.check_history[check_name].append(result)

                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                error_result = HealthCheckResult(
                    name=check_name,
                    status=HealthStatus.CRITICAL,
                    message=f"Health check execution failed: {str(e)}",
                    duration_ms=duration_ms,
                )

                self.last_check_results[check_name] = error_result
                self.check_history[check_name].append(error_result)

                structured_logger.error(
                    f"Health check {check_name} failed: {str(e)}", exc_info=True
                )

                return error_result

    async def run_all_health_checks(self) -> dict[str, HealthCheckResult]:
        """Run all health checks concurrently"""
        tasks = []
        for check_name in self.health_checks.keys():
            tasks.append(self.run_health_check(check_name))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        check_results = {}
        for i, result in enumerate(results):
            check_name = list(self.health_checks.keys())[i]

            if isinstance(result, Exception):
                check_results[check_name] = HealthCheckResult(
                    name=check_name,
                    status=HealthStatus.CRITICAL,
                    message=f"Health check failed with exception: {str(result)}",
                )
            else:
                check_results[check_name] = result

        return check_results

    def get_overall_health_status(self) -> tuple[HealthStatus, dict[str, Any]]:
        """Get overall system health status"""
        if not self.last_check_results:
            return HealthStatus.UNKNOWN, {"message": "No health checks have been run"}

        critical_failures = []
        unhealthy_checks = []
        degraded_checks = []
        healthy_checks = []

        for name, result in self.last_check_results.items():
            if result.status == HealthStatus.CRITICAL:
                critical_failures.append(name)
            elif result.status == HealthStatus.UNHEALTHY:
                unhealthy_checks.append(name)
            elif result.status == HealthStatus.DEGRADED:
                degraded_checks.append(name)
            else:
                healthy_checks.append(name)

        # Determine overall status
        if critical_failures:
            overall_status = HealthStatus.CRITICAL
            message = f"Critical failures in {len(critical_failures)} checks"
        elif unhealthy_checks:
            overall_status = HealthStatus.UNHEALTHY
            message = f"Unhealthy status in {len(unhealthy_checks)} checks"
        elif degraded_checks:
            overall_status = HealthStatus.DEGRADED
            message = f"Degraded status in {len(degraded_checks)} checks"
        else:
            overall_status = HealthStatus.HEALTHY
            message = f"All {len(healthy_checks)} checks are healthy"

        summary = {
            "overall_status": overall_status.value,
            "message": message,
            "summary": {
                "total_checks": len(self.last_check_results),
                "healthy": len(healthy_checks),
                "degraded": len(degraded_checks),
                "unhealthy": len(unhealthy_checks),
                "critical": len(critical_failures),
            },
            "failed_checks": {
                "critical": critical_failures,
                "unhealthy": unhealthy_checks,
                "degraded": degraded_checks,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

        return overall_status, summary

    def get_health_trends(self, check_name: str, hours: int = 24) -> dict[str, Any]:
        """Get health trends for a specific check"""
        if check_name not in self.check_history:
            return {"error": f"No history found for check: {check_name}"}

        history = list(self.check_history[check_name])
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        # Filter to specified time window
        recent_history = [r for r in history if r.timestamp >= cutoff_time]

        if not recent_history:
            return {"error": f"No recent history found for check: {check_name}"}

        # Calculate trends
        status_counts = defaultdict(int)
        durations = []

        for result in recent_history:
            status_counts[result.status.value] += 1
            durations.append(result.duration_ms)

        avg_duration = sum(durations) / len(durations) if durations else 0

        return {
            "check_name": check_name,
            "time_window_hours": hours,
            "total_checks": len(recent_history),
            "status_distribution": dict(status_counts),
            "average_duration_ms": round(avg_duration, 2),
            "min_duration_ms": min(durations) if durations else 0,
            "max_duration_ms": max(durations) if durations else 0,
            "latest_status": recent_history[-1].status.value,
            "latest_message": recent_history[-1].message,
        }

    def get_circuit_breaker_status(self) -> dict[str, Any]:
        """Get status of all circuit breakers"""
        status = {}
        for name, breaker in self.circuit_breakers.items():
            status[name] = breaker.get_stats()

        return {
            "circuit_breakers": status,
            "summary": {
                "total_breakers": len(self.circuit_breakers),
                "open_breakers": len(
                    [
                        b
                        for b in self.circuit_breakers.values()
                        if b.state == CircuitBreakerState.OPEN
                    ]
                ),
                "half_open_breakers": len(
                    [
                        b
                        for b in self.circuit_breakers.values()
                        if b.state == CircuitBreakerState.HALF_OPEN
                    ]
                ),
            },
        }


# Global health check manager instance
_health_check_manager: HealthCheckManager | None = None


def get_health_check_manager() -> HealthCheckManager:
    """Get global health check manager instance"""
    global _health_check_manager
    if _health_check_manager is None:
        _health_check_manager = HealthCheckManager()
    return _health_check_manager


# Decorator for circuit breaker protection
def circuit_breaker(breaker_name: str):
    """Decorator to protect functions with circuit breaker"""

    def decorator(func):
        @asyncio.coroutine
        def async_wrapper(*args, **kwargs):
            manager = get_health_check_manager()
            breaker = manager.get_circuit_breaker(breaker_name)

            if breaker is None:
                # If no circuit breaker configured, execute normally
                if asyncio.iscoroutinefunction(func):
                    return func(*args, **kwargs)
                else:
                    loop = asyncio.get_event_loop()
                    return loop.run_in_executor(None, func, *args, **kwargs)

            return breaker.call(func, *args, **kwargs)

        def sync_wrapper(*args, **kwargs):
            # For synchronous functions, we need to run in event loop
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(async_wrapper(*args, **kwargs))

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


@asynccontextmanager
async def health_check_context():
    """Context manager for health check operations"""
    manager = get_health_check_manager()

    try:
        # Run initial health checks
        await manager.run_all_health_checks()

        yield manager

    except Exception as e:
        structured_logger.error(f"Health check context error: {str(e)}", exc_info=True)
        raise

    finally:
        # Cleanup if needed
        pass


if __name__ == "__main__":
    # Example usage and testing
    async def test_health_checks():
        manager = get_health_check_manager()

        # Run all health checks
        results = await manager.run_all_health_checks()

        for name, result in results.items():
            print(f"{name}: {result.status.value} - {result.message}")

        # Get overall status
        status, summary = manager.get_overall_health_status()
        print(f"\nOverall Status: {status.value}")
        print(f"Summary: {summary}")

        # Test circuit breaker
        breaker = manager.get_circuit_breaker("database_operations")
        if breaker:
            print(f"\nCircuit breaker stats: {breaker.get_stats()}")

    # Run the test
    asyncio.run(test_health_checks())
