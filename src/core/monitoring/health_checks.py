"""
Health Check System
Provides comprehensive health monitoring for system components.
"""
import asyncio
import platform
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import psutil

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from core.logging import get_logger

logger = get_logger(__name__)


class HealthStatus(Enum):
    """Health check status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    component: str
    status: HealthStatus
    message: str
    timestamp: datetime
    response_time_ms: float = 0.0
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "component": self.component,
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "response_time_ms": self.response_time_ms,
            "details": self.details or {}
        }


class BaseHealthCheck:
    """Base class for health checks."""

    def __init__(self, name: str, timeout: float = 10.0):
        self.name = name
        self.timeout = timeout

    async def check(self) -> HealthCheckResult:
        """Perform health check with timeout."""
        start_time = time.time()

        try:
            result = await asyncio.wait_for(self._check_health(), timeout=self.timeout)
            response_time = (time.time() - start_time) * 1000
            result.response_time_ms = response_time
            return result

        except asyncio.TimeoutError:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check timed out after {self.timeout}s",
                timestamp=datetime.utcnow(),
                response_time_ms=response_time
            )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                timestamp=datetime.utcnow(),
                response_time_ms=response_time
            )

    async def _check_health(self) -> HealthCheckResult:
        """Override this method to implement specific health check."""
        raise NotImplementedError


class DatabaseHealthCheck(BaseHealthCheck):
    """Health check for database connectivity."""

    def __init__(self, name: str, connection_string: str, timeout: float = 10.0):
        super().__init__(name, timeout)
        self.connection_string = connection_string

    async def _check_health(self) -> HealthCheckResult:
        """Check database connectivity."""
        if not SQLALCHEMY_AVAILABLE:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNKNOWN,
                message="SQLAlchemy not available",
                timestamp=datetime.utcnow()
            )

        try:
            engine = create_async_engine(self.connection_string)

            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT 1"))
                row = result.fetchone()

                if row and row[0] == 1:
                    return HealthCheckResult(
                        component=self.name,
                        status=HealthStatus.HEALTHY,
                        message="Database connection successful",
                        timestamp=datetime.utcnow(),
                        details={
                            "query_result": row[0],
                            "connection_pool_size": engine.pool.size()
                        }
                    )
                else:
                    return HealthCheckResult(
                        component=self.name,
                        status=HealthStatus.UNHEALTHY,
                        message="Database query returned unexpected result",
                        timestamp=datetime.utcnow()
                    )

            await engine.dispose()

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                timestamp=datetime.utcnow()
            )


class RedisHealthCheck(BaseHealthCheck):
    """Health check for Redis connectivity."""

    def __init__(self, name: str, redis_url: str = "redis://localhost:6379", timeout: float = 5.0):
        super().__init__(name, timeout)
        self.redis_url = redis_url

    async def _check_health(self) -> HealthCheckResult:
        """Check Redis connectivity."""
        if not REDIS_AVAILABLE:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNKNOWN,
                message="Redis client not available",
                timestamp=datetime.utcnow()
            )

        try:
            redis_client = redis.from_url(self.redis_url)

            # Test ping
            pong = await redis_client.ping()

            # Get server info
            info = await redis_client.info()

            # Test set/get operation
            test_key = f"health_check_{int(time.time())}"
            await redis_client.set(test_key, "test_value", ex=60)
            test_value = await redis_client.get(test_key)
            await redis_client.delete(test_key)

            await redis_client.close()

            if pong and test_value == b"test_value":
                return HealthCheckResult(
                    component=self.name,
                    status=HealthStatus.HEALTHY,
                    message="Redis connection and operations successful",
                    timestamp=datetime.utcnow(),
                    details={
                        "ping_successful": pong,
                        "redis_version": info.get("redis_version"),
                        "used_memory": info.get("used_memory_human"),
                        "connected_clients": info.get("connected_clients")
                    }
                )
            else:
                return HealthCheckResult(
                    component=self.name,
                    status=HealthStatus.DEGRADED,
                    message="Redis connection issues detected",
                    timestamp=datetime.utcnow()
                )

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Redis connection failed: {str(e)}",
                timestamp=datetime.utcnow()
            )


class SystemResourcesHealthCheck(BaseHealthCheck):
    """Health check for system resources (CPU, Memory, Disk)."""

    def __init__(self, name: str = "system_resources",
                 cpu_threshold: float = 90.0,
                 memory_threshold: float = 90.0,
                 disk_threshold: float = 90.0):
        super().__init__(name)
        self.cpu_threshold = cpu_threshold
        self.memory_threshold = memory_threshold
        self.disk_threshold = disk_threshold

    async def _check_health(self) -> HealthCheckResult:
        """Check system resource usage."""
        try:
            # Get CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)

            # Get memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent

            # Get disk usage for root partition
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent

            # Determine overall status
            issues = []
            status = HealthStatus.HEALTHY

            if cpu_percent > self.cpu_threshold:
                issues.append(f"High CPU usage: {cpu_percent:.1f}%")
                status = HealthStatus.DEGRADED if cpu_percent < 95 else HealthStatus.UNHEALTHY

            if memory_percent > self.memory_threshold:
                issues.append(f"High memory usage: {memory_percent:.1f}%")
                if status != HealthStatus.UNHEALTHY:
                    status = HealthStatus.DEGRADED if memory_percent < 95 else HealthStatus.UNHEALTHY

            if disk_percent > self.disk_threshold:
                issues.append(f"High disk usage: {disk_percent:.1f}%")
                if status != HealthStatus.UNHEALTHY:
                    status = HealthStatus.DEGRADED if disk_percent < 95 else HealthStatus.UNHEALTHY

            message = "System resources within normal limits"
            if issues:
                message = f"Resource issues detected: {', '.join(issues)}"

            return HealthCheckResult(
                component=self.name,
                status=status,
                message=message,
                timestamp=datetime.utcnow(),
                details={
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory_percent,
                    "memory_available_gb": memory.available / (1024**3),
                    "memory_total_gb": memory.total / (1024**3),
                    "disk_percent": disk_percent,
                    "disk_free_gb": disk.free / (1024**3),
                    "disk_total_gb": disk.total / (1024**3),
                    "platform": platform.platform(),
                    "cpu_count": psutil.cpu_count()
                }
            )

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNKNOWN,
                message=f"Failed to check system resources: {str(e)}",
                timestamp=datetime.utcnow()
            )


class FileSystemHealthCheck(BaseHealthCheck):
    """Health check for file system accessibility."""

    def __init__(self, name: str, paths: list[str], check_write: bool = True):
        super().__init__(name)
        self.paths = paths
        self.check_write = check_write

    async def _check_health(self) -> HealthCheckResult:
        """Check file system paths accessibility."""
        try:
            import os
            import tempfile

            issues = []
            details = {}

            for path in self.paths:
                path_details = {"exists": False, "readable": False, "writable": False}

                # Check existence
                if os.path.exists(path):
                    path_details["exists"] = True

                    # Check readability
                    if os.access(path, os.R_OK):
                        path_details["readable"] = True
                    else:
                        issues.append(f"Path not readable: {path}")

                    # Check writability if requested
                    if self.check_write:
                        if os.path.isdir(path):
                            try:
                                with tempfile.NamedTemporaryFile(dir=path, delete=True) as tmp:
                                    tmp.write(b"test")
                                path_details["writable"] = True
                            except Exception:
                                issues.append(f"Path not writable: {path}")
                        elif os.access(path, os.W_OK):
                            path_details["writable"] = True
                        else:
                            issues.append(f"Path not writable: {path}")
                else:
                    issues.append(f"Path does not exist: {path}")

                details[path] = path_details

            status = HealthStatus.HEALTHY if not issues else HealthStatus.UNHEALTHY
            message = "All file system paths accessible" if not issues else f"Issues found: {', '.join(issues)}"

            return HealthCheckResult(
                component=self.name,
                status=status,
                message=message,
                timestamp=datetime.utcnow(),
                details=details
            )

        except Exception as e:
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNKNOWN,
                message=f"Failed to check file system: {str(e)}",
                timestamp=datetime.utcnow()
            )


class CustomHealthCheck(BaseHealthCheck):
    """Custom health check using provided function."""

    def __init__(self, name: str, check_function: Callable[[], Awaitable[HealthCheckResult]]):
        super().__init__(name)
        self.check_function = check_function

    async def _check_health(self) -> HealthCheckResult:
        """Execute custom health check function."""
        return await self.check_function()


class HealthCheckManager:
    """Manages and orchestrates multiple health checks."""

    def __init__(self):
        self.health_checks: dict[str, BaseHealthCheck] = {}
        self.last_results: dict[str, HealthCheckResult] = {}
        self.check_history: dict[str, list[HealthCheckResult]] = {}

    def register_health_check(self, health_check: BaseHealthCheck):
        """Register a health check."""
        self.health_checks[health_check.name] = health_check
        self.check_history[health_check.name] = []
        logger.info(f"Registered health check: {health_check.name}")

    def unregister_health_check(self, name: str):
        """Unregister a health check."""
        if name in self.health_checks:
            del self.health_checks[name]
            del self.last_results[name]
            del self.check_history[name]
            logger.info(f"Unregistered health check: {name}")

    async def check_single(self, name: str) -> HealthCheckResult | None:
        """Run a single health check by name."""
        if name not in self.health_checks:
            logger.warning(f"Health check not found: {name}")
            return None

        health_check = self.health_checks[name]
        result = await health_check.check()

        # Store result and history
        self.last_results[name] = result
        self.check_history[name].append(result)

        # Keep only last 100 results per check
        if len(self.check_history[name]) > 100:
            self.check_history[name] = self.check_history[name][-100:]

        logger.debug(f"Health check completed: {name} - {result.status.value}")
        return result

    async def check_all(self) -> dict[str, HealthCheckResult]:
        """Run all registered health checks."""
        if not self.health_checks:
            logger.warning("No health checks registered")
            return {}

        logger.info(f"Running {len(self.health_checks)} health checks")

        # Run all checks concurrently
        tasks = []
        check_names = []

        for name, health_check in self.health_checks.items():
            task = health_check.check()
            tasks.append(task)
            check_names.append(name)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        all_results = {}
        for i, result in enumerate(results):
            name = check_names[i]

            if isinstance(result, Exception):
                # Handle exceptions from individual checks
                result = HealthCheckResult(
                    component=name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Health check raised exception: {str(result)}",
                    timestamp=datetime.utcnow()
                )

            # Store result and history
            all_results[name] = result
            self.last_results[name] = result
            self.check_history[name].append(result)

            # Keep only last 100 results per check
            if len(self.check_history[name]) > 100:
                self.check_history[name] = self.check_history[name][-100:]

        return all_results

    def get_overall_status(self) -> HealthStatus:
        """Get overall system health status."""
        if not self.last_results:
            return HealthStatus.UNKNOWN

        statuses = [result.status for result in self.last_results.values()]

        if any(status == HealthStatus.UNHEALTHY for status in statuses):
            return HealthStatus.UNHEALTHY
        elif any(status == HealthStatus.DEGRADED for status in statuses):
            return HealthStatus.DEGRADED
        elif all(status == HealthStatus.HEALTHY for status in statuses):
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.UNKNOWN

    def get_health_summary(self) -> dict[str, Any]:
        """Get comprehensive health summary."""
        overall_status = self.get_overall_status()

        component_statuses = {
            name: result.status.value
            for name, result in self.last_results.items()
        }

        status_counts = {}
        for status in HealthStatus:
            count = sum(1 for result in self.last_results.values() if result.status == status)
            status_counts[status.value] = count

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": overall_status.value,
            "total_checks": len(self.health_checks),
            "status_counts": status_counts,
            "components": component_statuses,
            "last_check_times": {
                name: result.timestamp.isoformat()
                for name, result in self.last_results.items()
            }
        }

    def get_detailed_report(self) -> dict[str, Any]:
        """Get detailed health report with all check results."""
        return {
            "summary": self.get_health_summary(),
            "details": {
                name: result.to_dict()
                for name, result in self.last_results.items()
            }
        }

    def get_check_history(self, name: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get history for specific health check."""
        if name not in self.check_history:
            return []

        history = self.check_history[name][-limit:]
        return [result.to_dict() for result in history]


# Global health check manager
health_manager = HealthCheckManager()


# Utility functions for common health check setups

def setup_basic_health_checks(database_url: str | None = None,
                            redis_url: str | None = None,
                            file_paths: list[str] | None = None):
    """Setup basic health checks for common components."""

    # System resources check
    system_check = SystemResourcesHealthCheck()
    health_manager.register_health_check(system_check)

    # Database check
    if database_url:
        db_check = DatabaseHealthCheck("database", database_url)
        health_manager.register_health_check(db_check)

    # Redis check
    if redis_url:
        redis_check = RedisHealthCheck("redis", redis_url)
        health_manager.register_health_check(redis_check)

    # File system check
    if file_paths:
        fs_check = FileSystemHealthCheck("filesystem", file_paths)
        health_manager.register_health_check(fs_check)

    logger.info("Basic health checks configured")


async def run_health_check_loop(interval: int = 60, max_failures: int = 5):
    """Run health checks continuously in a loop."""
    consecutive_failures = 0

    while True:
        try:
            results = await health_manager.check_all()

            # Check for any unhealthy components
            unhealthy_components = [
                name for name, result in results.items()
                if result.status == HealthStatus.UNHEALTHY
            ]

            if unhealthy_components:
                logger.warning(f"Unhealthy components detected: {unhealthy_components}")
                consecutive_failures += 1

                if consecutive_failures >= max_failures:
                    logger.critical(f"System unhealthy for {consecutive_failures} consecutive checks")
            else:
                consecutive_failures = 0
                logger.debug("All health checks passed")

            await asyncio.sleep(interval)

        except Exception as e:
            logger.error(f"Error in health check loop: {e}")
            await asyncio.sleep(interval)
